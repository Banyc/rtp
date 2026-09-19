//! In-stream group parity sizing and budget policy.
//!
//! When the interactive in-stream group FEC path is live, an open group
//! accumulates data symbols and emits parity while the burst is still in
//! flight (the flush gate and capacity predicate live beside this module).
//! This module owns the group's *size* policy: how many data symbols make a
//! group full, how many parity symbols a flush emits, and how much of the
//! available send budget parity may consume.  The encoder/decoder underlay
//! that actually encodes the symbols and the token bucket it is paced by stay
//! in [`super::super`]; only the numeric decision is made here.
//!
//! The decision is budget-adaptive and non-destructive: emit as many parity
//! symbols as the spare-budget share allows (one is enough to recover a
//! single lost data symbol), and HOLD the group open when none is affordable
//! rather than destroying its accumulated data symbols.  The cap-forced stash
//! carries a group that reached [`MAX_DATA_PER_GROUP`] to the next budgeted
//! flush through the same gate.

use std::collections::VecDeque;

/// In-stream group FEC: a data group accumulates up to this many data symbols
/// before a full-group inline parity flush is emitted mid-burst.  Stock
/// (toggle off) force-skips at the stock threshold instead, so groups never
/// reach this size.
pub(crate) const INSTREAM_DATA_PER_GROUP: usize = 8;

/// Parity symbols emitted for a full in-stream group (`INSTREAM_DATA_PER_GROUP`
/// data symbols).  8+4 = 12 fits the stock decoder group-size cap and
/// `WINDOW_SIZE` without bumping either constant.
pub(crate) const INSTREAM_PARITY_PER_GROUP: usize = 4;

/// Maximum data symbols accumulated before a group is forcibly flushed, so
/// `symbol_id` never climbs past the peer's decoder max.
pub(crate) const MAX_DATA_PER_GROUP: usize = 20;

/// The budget-adaptive parity decision for an open in-stream multi-symbol
/// group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InStreamParity {
    /// Emit this many parity symbols (`1..=INSTREAM_PARITY_PER_GROUP`).  The
    /// flush closes the group.
    Emit(u8),
    /// The spare-budget share affords no parity this pass: HOLD the group
    /// open so its accumulated data symbols keep their parity chance at the
    /// next flush instead of being destroyed.
    Hold,
}

/// Decide the parity count for an open in-stream multi-symbol group from the
/// parity budget the 1/3 spare-bandwidth share affords.
///
/// The decision is budget-adaptive and non-destructive: emit as many parity
/// symbols as the budget allows (one is enough to recover a single lost data
/// symbol), and [`InStreamParity::Hold`] the group open when none is
/// affordable rather than destroying it.  An all-or-nothing gate would destroy
/// the whole group's parity the moment the pacer was momentarily drained — on
/// a batched interactive lane that is the common case, and the group would
/// then have no parity at all, so its loss fell through to RTO/reorder ARQ.
/// Holding preserves the accumulated data symbols so the next pass (after the
/// token refill) still emits their parity.
pub(crate) fn decide_in_stream_parity(parity_budget: usize) -> InStreamParity {
    if parity_budget == 0 {
        return InStreamParity::Hold;
    }
    InStreamParity::Emit(u8::try_from(INSTREAM_PARITY_PER_GROUP.min(parity_budget)).unwrap())
}

/// Whether the open group is a full in-stream group ready for an inline
/// mid-burst parity flush: only when `instream` is `true` (the toggle is on)
/// AND the group has reached [`INSTREAM_DATA_PER_GROUP`] data symbols.  The
/// stock path passes `false` and always gets `false`, so the inline flush
/// never fires.
pub(crate) fn group_is_full(instream: bool, data_count: usize) -> bool {
    instream && data_count >= INSTREAM_DATA_PER_GROUP
}

/// Whether the open group has reached the hard [`MAX_DATA_PER_GROUP`] cap and
/// must be force-flushed before the next data symbol is encoded.
pub(crate) fn cap_reached(data_count: usize) -> bool {
    data_count >= MAX_DATA_PER_GROUP
}

/// Cap-forced full-group parity waiting for the next budgeted flush.
///
/// A group that reaches [`MAX_DATA_PER_GROUP`] is force-flushed by
/// `encode_data` and its encoded parity is held here (bounded: at most the
/// stock per-group parity count) until the token-budget gate admits it.  A
/// tight budget HOLDS the stash rather than dropping it, because the group is
/// already closed and this is its only chance to be protected.
#[derive(Debug)]
pub(crate) struct CapParityStash {
    pending: VecDeque<Vec<u8>>,
}

impl CapParityStash {
    pub(crate) fn new() -> Self {
        Self {
            pending: VecDeque::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.pending.len()
    }

    /// Whether the parity budget can afford the whole stash this pass.
    pub(crate) fn affordable(&self, parity_budget: usize) -> bool {
        self.pending.len() <= parity_budget
    }

    /// Hold a freshly encoded cap-forced parity burst.
    pub(crate) fn hold(&mut self, packets: impl IntoIterator<Item = Vec<u8>>) {
        self.pending.extend(packets);
    }

    /// Take the whole stash for delivery, in emission order.
    pub(crate) fn take(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.pending).into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth(decision: InStreamParity) -> usize {
        match decision {
            InStreamParity::Hold => 0,
            InStreamParity::Emit(count) => usize::from(count),
        }
    }

    /// The open-group parity decision is budget-adaptive and bounded: a zero
    /// budget HOLDS the group open (never destroys its accumulated data
    /// symbols), the depth never exceeds the one-parity-per-data-symbol-group
    /// cap, and more budget never emits fewer parity symbols.  The last is the
    /// direction the redundancy constraint needs: as the spare-budget share is
    /// cut, the parity depth is cut, never raised.
    #[test]
    fn parity_depth_is_budget_adaptive_bounded_and_monotone() {
        assert_eq!(
            decide_in_stream_parity(0),
            InStreamParity::Hold,
            "a zero budget must hold the group open, not emit parity"
        );
        // Below the cap the depth tracks the budget exactly (one parity per
        // unit of budget).
        for budget in 1..=INSTREAM_PARITY_PER_GROUP {
            assert_eq!(
                decide_in_stream_parity(budget),
                InStreamParity::Emit(u8::try_from(budget).unwrap()),
                "budget {budget} must emit exactly {budget} parity symbols"
            );
        }
        // Above the cap the depth saturates at the group parity count.
        for budget in INSTREAM_PARITY_PER_GROUP..=1024 {
            assert_eq!(
                decide_in_stream_parity(budget),
                InStreamParity::Emit(u8::try_from(INSTREAM_PARITY_PER_GROUP).unwrap()),
                "budget {budget} must saturate at the {INSTREAM_PARITY_PER_GROUP}-parity cap"
            );
        }
        // Monotone non-decreasing in budget across the whole range, including
        // the Hold->Emit floor at zero.
        let mut previous = 0usize;
        for budget in 0..=64 {
            let depth = depth(decide_in_stream_parity(budget));
            assert!(
                depth >= previous,
                "budget {budget} emitted {depth} parity, fewer than a lower budget ({previous})"
            );
            previous = depth;
        }
    }

    /// The full-group predicate fires only for the interactive toggle once the
    /// group reaches the group size; the stock path always gets `false`.
    #[test]
    fn group_is_full_only_when_instream_and_at_the_group_size() {
        assert!(!group_is_full(false, INSTREAM_DATA_PER_GROUP));
        assert!(!group_is_full(false, INSTREAM_DATA_PER_GROUP * 10));
        assert!(!group_is_full(true, 0));
        assert!(!group_is_full(true, INSTREAM_DATA_PER_GROUP - 1));
        assert!(group_is_full(true, INSTREAM_DATA_PER_GROUP));
        assert!(group_is_full(true, INSTREAM_DATA_PER_GROUP + 1));
    }

    /// The hard group cap is reached exactly at `MAX_DATA_PER_GROUP`, so a
    /// group can never carry more data symbols than the peer's decoder accepts.
    #[test]
    fn cap_is_reached_at_max_data_per_group() {
        assert!(!cap_reached(0));
        assert!(!cap_reached(MAX_DATA_PER_GROUP - 1));
        assert!(cap_reached(MAX_DATA_PER_GROUP));
        assert!(cap_reached(MAX_DATA_PER_GROUP + 1));
    }

    /// A cap-forced parity stash is non-destructive and FIFO: on a budget too
    /// tight for the whole stash `affordable` is false (the caller HOLDS rather
    /// than drops it, then retries), and once taken the packets come back in
    /// emission order so a retry can neither reorder nor drop a symbol.  The
    /// stash is drained to empty, so a later hold starts a fresh burst.
    #[test]
    fn cap_parity_stash_holds_on_a_tight_budget_and_takes_fifo() {
        let mut stash = CapParityStash::new();
        assert!(stash.is_empty());
        assert_eq!(stash.len(), 0);
        assert!(
            stash.affordable(0),
            "an empty stash is affordable at any budget"
        );

        stash.hold(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(stash.len(), 3);
        assert!(
            !stash.affordable(2),
            "a budget below the stash size must hold, not drop"
        );
        assert!(stash.affordable(3));
        assert!(stash.affordable(4));

        let taken = stash.take();
        assert_eq!(
            taken,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            "the stash must replay in emission order"
        );
        assert!(stash.is_empty());
        assert_eq!(stash.len(), 0);
        assert!(
            stash.take().is_empty(),
            "taking an empty stash must yield nothing"
        );

        // A second hold starts a fresh burst rather than appending forever.
        stash.hold(vec![b"x".to_vec()]);
        assert_eq!(stash.len(), 1);
        assert_eq!(stash.take(), vec![b"x".to_vec()]);
    }
}
