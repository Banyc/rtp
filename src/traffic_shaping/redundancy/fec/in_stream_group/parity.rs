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
