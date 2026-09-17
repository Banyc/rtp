//! Fresh interactive-tail armour: how a single-symbol interactive message is
//! duplicated so a lone loss is covered on the same round trip, and the
//! loss-adaptive copy ladder that only ever shrinks that redundancy as the
//! link degrades.  Only the interactive lane (`fec_instream_flush`) consults
//! this; stock/bulk tuning never forces in-stream flushing.

/// Armor duplicate copies emitted for a fresh interactive single-symbol tail
/// (in addition to the primary datagram) at the burst-cover tier when the FEC
/// loss gate is OPEN and a *message-sized* parity symbol will therefore trail
/// the same burst as the sixth wire slot.  Primary + four copies + the small
/// parity is six back-to-back datagrams, so a five-packet burst always leaves
/// a survivor; the parity is a ~256 B symbol, not the 8 KB full-MSS symbol a
/// stock flush would emit, so the sixth slot is cheap.
const FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY: usize = 4;

/// Armor duplicate copies at the burst-cover tier when the FEC loss gate is
/// CLOSED, so no parity will trail the burst.  Five copies fill the sixth
/// wire slot the parity would have occupied: primary + five copies is still
/// six back-to-back 256-byte datagrams, covering a five-packet burst.  The
/// copy count is monotone non-increasing with loss, so the closed-gate tier
/// never grows redundancy as the link degrades.
const FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_NO_PARITY: usize = 5;

/// Armor duplicate copies retained once the measured loss passes
/// [`FRESH_INTERACTIVE_TAIL_ARMOR_MODERATE_LOSS`]: the two-copy coverage this
/// lane shipped with, kept for the mid-loss band where a single loss is still
/// the common event.
const FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BASE: usize = 2;

/// Armor duplicate copies retained once the measured loss passes
/// [`FRESH_INTERACTIVE_TAIL_ARMOR_HOSTILE_LOSS`].  On a hostile link the extra
/// packets amplify queue pressure instead of helping, so the fresh tail backs
/// off to the primary datagram alone and leaves repair to FEC/ARQ.
const FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN: usize = 0;

/// Measured effective loss ratio above which the fresh interactive tail drops
/// from the burst-cover copy count to the historical two-copy base.
const FRESH_INTERACTIVE_TAIL_ARMOR_MODERATE_LOSS: f64 = 0.15;

/// Measured effective loss ratio above which the fresh interactive tail backs
/// off to the primary datagram alone: a hostile link must never pay extra
/// redundancy that amplifies congestion.
const FRESH_INTERACTIVE_TAIL_ARMOR_HOSTILE_LOSS: f64 = 0.30;

/// Whether `data_written` is a whole single-symbol frame: the packet carries
/// the frame's declared length.  A multi-symbol bulk frame's first packet
/// declares a larger `frame_len` and never qualifies, so bulk traffic gains no
/// redundancy.
pub(crate) fn is_single_symbol_frame(frame_len: Option<u32>, data_written: usize) -> bool {
    u32::try_from(data_written)
        .ok()
        .is_some_and(|written| frame_len == Some(written))
}

/// Whether a fresh (non-recovery) send is the interactive single-symbol tail
/// that gets the armour duplicates: a whole single-symbol frame, or the first
/// data symbol of an open group (which recognises the interactive message even
/// when the open FEC group already holds preceding bulk data symbols).
/// `open_group_data_count` is `None` when FEC is disabled.  Only the
/// interactive lane (`instream_flush`) opts in.
pub(crate) fn is_fresh_interactive_tail(
    is_recovery: bool,
    instream_flush: bool,
    single_symbol_frame: bool,
    open_group_data_count: Option<usize>,
) -> bool {
    !is_recovery && instream_flush && (single_symbol_frame || open_group_data_count == Some(1))
}

/// Armor duplicate copies for a fresh interactive single-symbol tail as a
/// function of the measured effective loss ratio and whether a parity
/// datagram will trail the same burst (the FEC loss gate is open).  The
/// mapping is **monotone non-increasing** in loss: it may only ever shrink
/// the per-message packet count as the wire loss rate rises, so a hostile
/// link never sees more redundancy than a clean one.  `None` (no loss
/// evidence yet) is treated as the low-loss tier.  At the burst-cover tier
/// the copy count compensates for the parity gate: with a trailing
/// message-sized parity four copies suffice (six datagrams total), without it
/// a fifth copy fills the same sixth slot so a five-packet burst still leaves
/// a survivor.  The per-message datagram budget is therefore six either way
/// and only ever shrinks with loss.  Only the interactive lane consults this:
/// stock/bulk tuning never forces `fec_instream_flush`.
pub(crate) fn fresh_tail_armor_copies(
    effective_loss: Option<f64>,
    parity_covers_burst: bool,
) -> usize {
    match effective_loss {
        Some(loss) if loss >= FRESH_INTERACTIVE_TAIL_ARMOR_HOSTILE_LOSS => {
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN
        }
        Some(loss) if loss >= FRESH_INTERACTIVE_TAIL_ARMOR_MODERATE_LOSS => {
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BASE
        }
        _ if parity_covers_burst => FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY,
        _ => FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_NO_PARITY,
    }
}

/// The number of armour duplicate copies an eligible send gets: a recovery
/// send carries exactly one, a fresh interactive tail uses the test override
/// when set, else the loss-adaptive ladder.  Keeping the override beside the
/// ladder means a forced count never leaks into the recovery path.
pub(crate) fn fresh_tail_armor_copy_count(
    fresh_interactive_tail: bool,
    override_copies: Option<usize>,
    effective_loss: Option<f64>,
    parity_covers_burst: bool,
) -> usize {
    if fresh_interactive_tail {
        override_copies
            .unwrap_or_else(|| fresh_tail_armor_copies(effective_loss, parity_covers_burst))
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    /// The fresh interactive tail's armor copy count is monotone
    /// non-increasing in the measured loss ratio: a hostile link can never
    /// emit more redundancy per message than a clean one.  The low/unmeasured
    /// tier pays the burst-cover copy, the mid band keeps the historical base,
    /// and the hostile tier backs off to the primary datagram alone.  At the
    /// burst-cover tier the count compensates for the parity gate: with a
    /// trailing message-sized parity four copies, without it five, so the
    /// total per-message datagram budget stays at six either way.
    #[test]
    fn fresh_tail_armor_copies_are_monotone_non_increasing_in_loss() {
        use super::{
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BASE,
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_NO_PARITY,
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY,
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN, fresh_tail_armor_copies,
        };
        for parity in [true, false] {
            assert_eq!(
                fresh_tail_armor_copies(None, parity),
                if parity {
                    FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY
                } else {
                    FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_NO_PARITY
                },
                "no loss evidence yet must use the burst-cover tier (parity={parity})"
            );
            assert_eq!(
                fresh_tail_armor_copies(Some(0.0), parity),
                fresh_tail_armor_copies(None, parity),
                "a clean link must use the burst-cover tier (parity={parity})"
            );
            assert_eq!(
                fresh_tail_armor_copies(Some(0.14), parity),
                fresh_tail_armor_copies(None, parity),
                "just below the moderate threshold keeps the burst-cover tier (parity={parity})"
            );
        }
        assert_eq!(
            fresh_tail_armor_copies(
                Some(0.14),
                false // parity irrelevant below the moderate threshold
            ),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_NO_PARITY,
            "the no-parity burst-cover tier pays the fifth copy"
        );
        assert_eq!(
            fresh_tail_armor_copies(
                Some(0.14),
                true // parity irrelevant below the moderate threshold
            ),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY,
            "the with-parity burst-cover tier pays four copies"
        );
        assert_eq!(
            fresh_tail_armor_copies(Some(0.15), false),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BASE,
            "the moderate threshold drops to the two-copy base"
        );
        assert_eq!(
            fresh_tail_armor_copies(Some(0.30), true),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN,
            "the hostile threshold backs off to the primary alone"
        );
        // Non-increasing in loss for either gate state: an open gate (parity
        // trails the burst) must never emit more copies than a closed one, and
        // neither may grow with loss.
        for parity in [true, false] {
            let mut previous = usize::MAX;
            for step in 0..=100 {
                let loss = Some(step as f64 / 100.0);
                let copies = fresh_tail_armor_copies(loss, parity);
                assert!(
                    copies <= previous,
                    "loss {loss:?} (parity={parity}) emitted {copies} copies, more than a lower loss ({previous})"
                );
                previous = copies;
            }
        }
    }

    /// The interactive fresh tail's per-message wire is bounded by six
    /// back-to-back datagrams at every loss tier and never grows with loss.
    /// The primary datagram plus the armor copies plus the (at most one)
    /// trailing message-sized parity is the whole budget; the closed-gate
    /// tier pays one more 256-byte copy in place of the parity, so both
    /// low-loss compositions land on six slots and the byte cost stays
    /// bounded far below a single full-MSS parity symbol.
    #[test]
    fn fresh_tail_burst_cover_stays_within_the_six_datagram_budget() {
        use super::fresh_tail_armor_copies;
        const PRIMARY: usize = 1;
        const PARITY_SLOT: usize = 1;
        const MESSAGE_WIRE_BYTES: usize = 256;
        const BUDGET_DATAGRAMS: usize = 6;
        const BUDGET_WIRE_BYTES: usize = BUDGET_DATAGRAMS * MESSAGE_WIRE_BYTES;
        for parity in [true, false] {
            let mut previous = usize::MAX;
            for step in 0..=100 {
                let loss = Some(step as f64 / 100.0);
                let copies = fresh_tail_armor_copies(loss, parity);
                let total = PRIMARY + copies + usize::from(parity) * PARITY_SLOT;
                assert!(
                    total <= BUDGET_DATAGRAMS,
                    "loss {loss:?} (parity={parity}) spent {total} datagrams, over the {BUDGET_DATAGRAMS}-slot budget"
                );
                assert!(
                    total <= previous,
                    "loss {loss:?} (parity={parity}) spent {total} datagrams, more than a lower loss ({previous})"
                );
                previous = total;
                assert!(
                    total * MESSAGE_WIRE_BYTES <= BUDGET_WIRE_BYTES,
                    "the per-message wire must stay under the {BUDGET_WIRE_BYTES}-byte ceiling"
                );
            }
        }
        // The two low-loss compositions are exactly six slots: five copies
        // when no parity trails, four copies plus the small parity when one
        // does.
        assert_eq!(
            PRIMARY + fresh_tail_armor_copies(None, false),
            BUDGET_DATAGRAMS
        );
        assert_eq!(
            PRIMARY + fresh_tail_armor_copies(None, true) + PARITY_SLOT,
            BUDGET_DATAGRAMS
        );
    }
}
