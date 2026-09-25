//! Fresh interactive-tail armour: how a single-symbol interactive message is
//! duplicated so a lone loss is covered on the same round trip, and the
//! loss-adaptive copy ladder that only ever shrinks that redundancy as the
//! link degrades.  Only the interactive lane (`fec_instream_flush`) consults
//! this; stock/bulk tuning never forces in-stream flushing.

/// Armor duplicate copies emitted for a fresh interactive single-symbol tail
/// (in addition to the primary datagram) while the FEC loss gate is OPEN and a
/// *message-sized* parity symbol will therefore trail the same burst as the
/// sixth wire slot.  Primary + four copies + the small parity is six
/// back-to-back datagrams, so a five-packet burst always leaves a survivor;
/// the parity is a ~256 B symbol, not the 8 KB full-MSS symbol a stock flush
/// would emit, so the sixth slot is cheap.  Paid only when the tail is *lone*
/// (see [`is_lone_tail`]).
const FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY: usize = 4;

/// Armor duplicate copies at the burst-cover tier when the FEC loss gate is
/// CLOSED, so no parity will trail the burst.  Five copies fill the sixth
/// wire slot the parity would have occupied: primary + five copies is still
/// six back-to-back 256-byte datagrams, covering a five-packet burst.  The
/// copy count is monotone non-increasing with loss, so the closed-gate tier
/// never grows redundancy as the link degrades.  Paid only when the tail is
/// *lone* (see [`is_lone_tail`]).
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

/// Armor duplicate copies emitted for a *pipelined* fresh interactive
/// single-symbol tail (in addition to the primary datagram) while the FEC
/// loss gate is closed: the **lone-loss cover**, the minimum that recovers a
/// single lost datagram on the same round trip.
///
/// A pipelined tail — one with another data packet already unacked on the
/// connection, so the application is offering more than one packet per round
/// trip — is the regime where the six-slot burst cover is *not* load-bearing.
/// The peer's next ACK covers a newer packet and therefore SACKs this tail's
/// hole, so the one-reorder-window ARQ repair lands within a round trip even
/// when a multi-packet burst wipes every copy of *this* message.  Measured on
/// the 2 %-loss dual-lane constitution arm, the interactive fresh tails are
/// almost all pipelined (1202 of 1211 in one 30 s run, two to four packets in
/// flight), while an interactive request/response pair is almost all lone
/// (3734 of 4007 in the burst-loss repair probe); the historical
/// burst-cover ladder therefore spent its premium on a regime that never
/// needed it: armor was the interactive lane's dominant wire cost (2.5x the
/// offered payload in byte-identical re-sends) while the gated parity path
/// emitted nothing and every ARQ counter stayed at zero.
const FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_LONE_LOSS: usize = 1;

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

/// Whether a fresh interactive tail is *lone*: it is the only unacked data
/// packet on the connection, so no newer packet is in flight for the peer to
/// SACK its hole with and the application is offering at most one packet per
/// round trip.  `in_flight_pkts` is the sender's live in-flight count read
/// after the tail's primary datagram was minted, so the tail itself is
/// already counted: `1` means nothing else is outstanding, `2` or more means
/// the stream is pipelined.
///
/// The distinction is the load-bearing condition for the burst cover, not the
/// measured loss ratio: a burst-cover ladder protects a *lone* tail whose
/// whole redundancy group can be wiped by one burst with nothing newer behind
/// it, while a pipelined tail's hole is SACKed by the peer's next ACK and the
/// lone-loss cover carries it.  Because it reads the dynamic window rather
/// than a loss estimate, gating on it cannot make redundancy grow with loss.
pub(crate) fn is_lone_tail(in_flight_pkts: usize) -> bool {
    in_flight_pkts <= 1
}

/// Armor duplicate copies for a fresh interactive single-symbol tail, from the
/// measured effective loss ratio, whether the FEC loss gate reports that
/// measured loss warrants recovery, and whether the tail is a lone tail (see
/// [`is_lone_tail`]).
///
/// A lone tail pays the **burst-cover ladder**: sized so primary + copies +
/// the (at most one) parity symbol fill six back-to-back wire slots, so a
/// five-packet burst always leaves a survivor.  It is monotone
/// non-increasing in loss, so a hostile link never sees more redundancy than
/// a clean one.
///
/// A pipelined tail pays the **lone-loss cover** while the gate is closed and
/// nothing at all once it opens: one small datagram recovers a lone loss on
/// the same round trip, and the ARQ fall-through repairs what one copy cannot
/// because the peer's next ACK SACKs the hole.  Either way the pipelined
/// tail's per-message budget (primary plus one repair slot) does not grow as
/// measured loss crosses the gate's enable threshold.
///
/// Only the interactive lane consults this: stock/bulk tuning never forces
/// `fec_instream_flush`.
pub(crate) fn fresh_tail_armor_copies(
    effective_loss: Option<f64>,
    loss_gate_open: bool,
    lone_tail: bool,
) -> usize {
    if effective_loss.is_some_and(|loss| loss >= FRESH_INTERACTIVE_TAIL_ARMOR_HOSTILE_LOSS) {
        // A hostile link must not amplify congestion with extra packets: the
        // defensive floor withdraws armor from either tail and leaves repair
        // to the gated parity path and ARQ.
        return FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN;
    }
    if !lone_tail {
        return if loss_gate_open {
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN
        } else {
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_LONE_LOSS
        };
    }
    match effective_loss {
        Some(loss) if loss >= FRESH_INTERACTIVE_TAIL_ARMOR_MODERATE_LOSS => {
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BASE
        }
        _ if loss_gate_open => FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY,
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
    loss_gate_open: bool,
    lone_tail: bool,
) -> usize {
    if fresh_interactive_tail {
        override_copies
            .unwrap_or_else(|| fresh_tail_armor_copies(effective_loss, loss_gate_open, lone_tail))
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    /// The fresh interactive tail's armor copy count is monotone
    /// non-increasing in the measured loss ratio, for either load-bearing
    /// condition: a hostile link can never emit more redundancy per message
    /// than a clean one.  A lone tail's low/unmeasured tier pays the
    /// burst-cover copy, its mid band keeps the historical base, and its
    /// hostile tier backs off to the primary datagram alone; a pipelined tail
    /// pays the flat lone-loss cover that the gated parity path then replaces.
    #[test]
    fn fresh_tail_armor_copies_are_monotone_non_increasing_in_loss() {
        use super::{
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BASE,
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_NO_PARITY,
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY,
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_LONE_LOSS, FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN,
            fresh_tail_armor_copies,
        };
        for loss_gate_open in [true, false] {
            assert_eq!(
                fresh_tail_armor_copies(None, loss_gate_open, true),
                if loss_gate_open {
                    FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY
                } else {
                    FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_NO_PARITY
                },
                "a lone tail with no loss evidence yet must use the burst-cover tier (gate={loss_gate_open})"
            );
            assert_eq!(
                fresh_tail_armor_copies(Some(0.0), loss_gate_open, true),
                fresh_tail_armor_copies(None, loss_gate_open, true),
                "a clean link's lone tail must use the burst-cover tier (gate={loss_gate_open})"
            );
            assert_eq!(
                fresh_tail_armor_copies(Some(0.14), loss_gate_open, true),
                fresh_tail_armor_copies(None, loss_gate_open, true),
                "just below the moderate threshold keeps the lone tail's burst-cover tier (gate={loss_gate_open})"
            );
        }
        assert_eq!(
            fresh_tail_armor_copies(Some(0.14), false, true),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_NO_PARITY,
            "the closed-gate lone tail pays the fifth copy (the parity's slot)"
        );
        assert_eq!(
            fresh_tail_armor_copies(Some(0.14), true, true),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_WITH_PARITY,
            "the open-gate lone tail pays four copies, the parity taking the sixth slot"
        );
        assert_eq!(
            fresh_tail_armor_copies(Some(0.15), false, true),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BASE,
            "the moderate threshold drops the lone tail to the two-copy base"
        );
        assert_eq!(
            fresh_tail_armor_copies(Some(0.30), true, true),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN,
            "the hostile threshold backs the lone tail off to the primary alone"
        );
        assert_eq!(
            fresh_tail_armor_copies(Some(0.02), false, false),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_LONE_LOSS,
            "a pipelined tail under the constitution arm's 2% loss pays the lone-loss cover"
        );
        assert_eq!(
            fresh_tail_armor_copies(None, true, false),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN,
            "an open gate means the parity path owns a pipelined tail's repair"
        );
        // Non-increasing in loss for each load-bearing condition and gate
        // state: neither twin may grow with loss, and the pipelined twin may
        // never spend more than the lone one at the same loss.
        for lone_tail in [true, false] {
            for loss_gate_open in [true, false] {
                let mut previous = usize::MAX;
                for step in 0..=100 {
                    let loss = Some(step as f64 / 100.0);
                    let copies = fresh_tail_armor_copies(loss, loss_gate_open, lone_tail);
                    assert!(
                        copies <= previous,
                        "loss {loss:?} (gate={loss_gate_open}, lone={lone_tail}) emitted {copies} copies, more than a lower loss ({previous})"
                    );
                    assert!(
                        copies <= fresh_tail_armor_copies(loss, loss_gate_open, true),
                        "loss {loss:?} (gate={loss_gate_open}) let a pipelined tail spend more than a lone one"
                    );
                    previous = copies;
                }
            }
        }
    }

    /// The load-bearing condition for the burst cover is the live send window,
    /// not the measured loss: a tail with nothing else unacked on the
    /// connection is *lone* (no newer packet the peer's ACK could SACK the
    /// hole with) and pays the six-slot cover, while a tail with another
    /// unacked packet is *pipelined* and pays only the lone-loss cover while
    /// the gate is closed, nothing once it opens.  The pipelined composition is
    /// exactly two datagrams (primary + lone-loss copy) and the lone
    /// composition exactly six, so the two regimes differ by 3x of wire.
    #[test]
    fn the_send_window_sparsity_decides_the_burst_cover() {
        use super::{
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_NO_PARITY,
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_LONE_LOSS, FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN,
            fresh_tail_armor_copies, is_lone_tail,
        };
        // The primary datagram is minted before the count is read, so an
        // unopposed tail reads 1; anything above that has another data packet
        // (and therefore a future SACK source) in flight.
        assert!(is_lone_tail(0), "a tail with an empty window is alone");
        assert!(is_lone_tail(1), "the tail's own primary is the only flight");
        assert!(!is_lone_tail(2), "one more unacked packet is a pipeline");
        assert!(!is_lone_tail(64), "a deep pipeline is never a lone tail");
        const PRIMARY: usize = 1;
        let lone = fresh_tail_armor_copies(Some(0.02), false, true);
        let pipelined = fresh_tail_armor_copies(Some(0.02), false, false);
        assert_eq!(lone, FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_NO_PARITY);
        assert_eq!(pipelined, FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_LONE_LOSS);
        assert_eq!(PRIMARY + lone, 6, "the lone tail spends the six-slot cover");
        assert_eq!(
            PRIMARY + pipelined,
            2,
            "the pipelined tail spends the lone-loss cover"
        );
        assert_eq!(
            fresh_tail_armor_copies(None, true, false),
            FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_MIN
        );
    }

    /// Only a whole single-symbol frame qualifies for the interactive tail's
    /// armor: the packet must carry the frame's declared length.  A
    /// multi-symbol bulk frame's first packet declares a larger `frame_len`
    /// and must NEVER qualify, and an unframed send must not either — this is
    /// what keeps bulk traffic from gaining any redundancy (no wire
    /// inflation).
    #[test]
    fn only_a_whole_single_symbol_frame_qualifies_for_armor() {
        use super::is_single_symbol_frame;
        assert!(
            is_single_symbol_frame(Some(2048), 2048),
            "a whole single-symbol frame must qualify"
        );
        assert!(
            !is_single_symbol_frame(Some(8192), 2048),
            "a multi-symbol bulk frame must not qualify"
        );
        assert!(
            !is_single_symbol_frame(None, 2048),
            "an unframed send must not qualify"
        );
        assert!(
            !is_single_symbol_frame(Some(1024), 2048),
            "a frame shorter than the written data must not qualify"
        );
        assert!(
            !is_single_symbol_frame(Some(2048), 0),
            "a zero-length send must not qualify"
        );
    }

    /// The interactive tail is only a fresh (non-recovery) send on the
    /// force-flush lane that is either a whole single-symbol frame or the
    /// first data symbol of an open group.  A recovery send, the stock lane,
    /// and a non-first multi-symbol group all must not qualify, so armor never
    /// reaches bulk or the recovery path.
    #[test]
    fn only_a_fresh_interactive_tail_qualifies_for_armor() {
        use super::is_fresh_interactive_tail;
        // The two qualifying shapes.
        assert!(is_fresh_interactive_tail(false, true, true, None));
        assert!(is_fresh_interactive_tail(false, true, true, Some(5)));
        assert!(is_fresh_interactive_tail(false, true, false, Some(1)));
        // A recovery send never qualifies, even as a single-symbol frame.
        assert!(!is_fresh_interactive_tail(true, true, true, Some(1)));
        // The stock lane (no force-flush) never qualifies.
        assert!(!is_fresh_interactive_tail(false, false, true, Some(1)));
        // A non-first symbol of a multi-symbol group never qualifies.
        assert!(!is_fresh_interactive_tail(false, true, false, Some(2)));
        assert!(!is_fresh_interactive_tail(false, true, false, None));
    }

    /// A non-tail (recovery) send carries exactly one armor copy regardless of
    /// the measured loss, the send-window sparsity, or a test override, and a
    /// fresh interactive tail uses the override when set, else the
    /// loss-adaptive ladder.  Keeping the override beside the ladder means a
    /// forced count can never leak into the recovery path and a hostile loss
    /// tier can never inflate a recovery send's copies.
    #[test]
    fn armor_copy_count_never_leaks_the_override_into_recovery() {
        use super::{fresh_tail_armor_copies, fresh_tail_armor_copy_count};
        for loss in [None, Some(0.0), Some(0.15), Some(0.30), Some(1.0)] {
            for lone_tail in [true, false] {
                for override_copies in [None, Some(0usize), Some(7)] {
                    assert_eq!(
                        fresh_tail_armor_copy_count(false, override_copies, loss, false, lone_tail),
                        1,
                        "a recovery send must carry exactly one copy (loss={loss:?}, lone={lone_tail}, override={override_copies:?})"
                    );
                }
            }
        }
        // A fresh interactive tail: the override wins, else the ladder.
        assert_eq!(
            fresh_tail_armor_copy_count(true, Some(7), Some(0.5), false, false),
            7,
            "the test override must force the fresh tail's copy count"
        );
        assert_eq!(
            fresh_tail_armor_copy_count(true, None, Some(0.30), true, true),
            fresh_tail_armor_copies(Some(0.30), true, true),
            "without an override the fresh tail must use the loss-adaptive ladder"
        );
        assert_eq!(
            fresh_tail_armor_copy_count(true, None, Some(0.0), false, true),
            fresh_tail_armor_copies(Some(0.0), false, true),
            "without an override a clean-link lone tail must use the burst-cover tier"
        );
        assert_eq!(
            fresh_tail_armor_copy_count(true, None, Some(0.0), false, false),
            fresh_tail_armor_copies(Some(0.0), false, false),
            "without an override a clean-link pipelined tail must use the lone-loss cover"
        );
    }

    /// The interactive fresh tail's per-message wire is bounded by six
    /// back-to-back datagrams at every loss tier, for either load-bearing
    /// condition, and never grows with loss.  The primary datagram plus the
    /// armor copies plus the (at most one) trailing message-sized parity is the
    /// whole budget; the closed-gate lone tier pays one more 256-byte copy in
    /// place of the parity, so both lone low-loss compositions land on six
    /// slots and the byte cost stays bounded far below a single full-MSS parity
    /// symbol, while the pipelined tier spends two.
    #[test]
    fn fresh_tail_burst_cover_stays_within_the_six_datagram_budget() {
        use super::fresh_tail_armor_copies;
        const PRIMARY: usize = 1;
        const PARITY_SLOT: usize = 1;
        const MESSAGE_WIRE_BYTES: usize = 256;
        const BUDGET_DATAGRAMS: usize = 6;
        const BUDGET_WIRE_BYTES: usize = BUDGET_DATAGRAMS * MESSAGE_WIRE_BYTES;
        for lone_tail in [true, false] {
            for loss_gate_open in [true, false] {
                let mut previous = usize::MAX;
                for step in 0..=100 {
                    let loss = Some(step as f64 / 100.0);
                    let copies = fresh_tail_armor_copies(loss, loss_gate_open, lone_tail);
                    let total = PRIMARY + copies + usize::from(loss_gate_open) * PARITY_SLOT;
                    assert!(
                        total <= BUDGET_DATAGRAMS,
                        "loss {loss:?} (gate={loss_gate_open}, lone={lone_tail}) spent {total} datagrams, over the {BUDGET_DATAGRAMS}-slot budget"
                    );
                    assert!(
                        total <= previous,
                        "loss {loss:?} (gate={loss_gate_open}, lone={lone_tail}) spent {total} datagrams, more than a lower loss ({previous})"
                    );
                    previous = total;
                    assert!(
                        total * MESSAGE_WIRE_BYTES <= BUDGET_WIRE_BYTES,
                        "the per-message wire must stay under the {BUDGET_WIRE_BYTES}-byte ceiling"
                    );
                }
            }
        }
        // The two lone low-loss compositions are exactly six slots: five
        // copies when no parity trails, four copies plus the small parity when
        // one does.  The pipelined low-loss composition is exactly two: the
        // primary plus the lone-loss copy.
        assert_eq!(
            PRIMARY + fresh_tail_armor_copies(None, false, true),
            BUDGET_DATAGRAMS
        );
        assert_eq!(
            PRIMARY + fresh_tail_armor_copies(None, true, true) + PARITY_SLOT,
            BUDGET_DATAGRAMS
        );
        assert_eq!(PRIMARY + fresh_tail_armor_copies(None, false, false), 2);
    }
}
