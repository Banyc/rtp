//! Wrap-safe eligibility and deadline index for retransmission.
//!
//! Replaces the per-poll send-window scans of `has_rtx` / `rtx` /
//! `next_poll_time` with one coherent index.  Only the exact
//! `min(num_in_flight, cwnd)` prefix of the send window is tracked as
//! *active* entries, each carrying its RTO deadline (`rto_at`) and its
//! original send time (`sent_at`).  The RTO deadline is indexed directly;
//! the reorder-window deadline is derived at query time as
//! `sent_at + reorder_window` from the `reorder_sent` send-time keys (the
//! window tracks sRTT and changes over a packet's life).
//!
//! Time-based reasons (RTO, reorder) are promoted into the *ready* set once
//! their deadlines elapse ([`RetransmissionIndex::promote_due`]);
//! evidence- and outage-based reasons (fast loss, pre-outage) are set by
//! the caller when the corresponding evidence arrives
//! ([`RetransmissionIndex::update_ready`]).  All four reasons are
//! independent per packet: a packet can be ready for RTO, reorder, fast
//! loss, and pre-outage at once.  Deadline ties break by wrap-aware
//! sequence serial order, which is a total order only within a live window
//! smaller than half the sequence space — the invariant every caller
//! maintains.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::sequence::{HALF_SEQUENCE_SPACE, SequenceMap, SequenceNumber, lt};

/// Deadline for an active packet: the RTO retransmission deadline (send
/// time + the packet's RTO) and the original send time (used to derive the
/// reorder-window deadline at query time).
#[derive(Debug, Clone, Copy)]
struct ActiveEntry {
    rto_at: Instant,
    sent_at: Instant,
}

/// A deadline key: ordered by the `Instant` first, then by wrap-aware
/// sequence serial order within the live window.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct DeadlineKey {
    at: Instant,
    seq: SequenceNumber,
}

impl Ord for DeadlineKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at.cmp(&other.at).then_with(|| {
            if self.seq == other.seq {
                Ordering::Equal
            } else if lt(self.seq, other.seq) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
    }
}

impl PartialOrd for DeadlineKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The independent retransmission reasons that can make a packet ready,
/// each with the deadline at which it became (or becomes) due.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ReadyReasons {
    /// The packet's RTO deadline once it has expired.
    rto: Option<Instant>,
    /// The reorder-window deadline once elapsed on an out-of-order-passed
    /// packet.
    reorder: Option<Instant>,
    /// The moment the evidence-gated fast-loss path declared the packet
    /// lost.
    fast_loss: Option<Instant>,
    /// The moment the outage epoch declared the packet a pre-outage loss.
    pre_outage: Option<Instant>,
}

impl ReadyReasons {
    /// The earliest deadline among the reasons set so far, if any.
    fn earliest(&self) -> Option<Instant> {
        [self.rto, self.reorder, self.fast_loss, self.pre_outage]
            .into_iter()
            .flatten()
            .min()
    }

    /// Whether the evidence-gated fast-loss path declared the packet lost.
    pub(super) fn fast_loss_at(&self) -> Option<Instant> {
        self.fast_loss
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ReadyReason {
    Reorder,
    FastLoss,
    PreOutage,
}

/// The wrap-safe retransmission index.
#[derive(Debug)]
pub(super) struct RetransmissionIndex {
    active: SequenceMap<ActiveEntry>,
    rto_deadlines: BTreeSet<DeadlineKey>,
    reorder_sent: BTreeSet<DeadlineKey>,
    ready: SequenceMap<ReadyReasons>,
    ready_deadlines: BTreeSet<DeadlineKey>,
}

impl RetransmissionIndex {
    pub(super) fn new(anchor: SequenceNumber) -> Self {
        Self {
            active: SequenceMap::new(anchor, HALF_SEQUENCE_SPACE - 1),
            rto_deadlines: BTreeSet::new(),
            reorder_sent: BTreeSet::new(),
            ready: SequenceMap::new(anchor, HALF_SEQUENCE_SPACE - 1),
            ready_deadlines: BTreeSet::new(),
        }
    }

    /// Activate a packet: insert it into the active set, atomically adding
    /// its RTO deadline key and — when the packet is out-of-order-passed —
    /// its reorder send-time key.  Ready reasons are managed independently
    /// by [`Self::update_ready`] / [`Self::promote_due`].
    pub(super) fn activate(
        &mut self,
        seq: SequenceNumber,
        rto_at: Instant,
        sent_at: Instant,
        out_of_order: bool,
    ) {
        let previous = self.active.insert(seq, ActiveEntry { rto_at, sent_at });
        debug_assert!(
            previous.is_none(),
            "activate must not replace an already-active packet"
        );
        self.rto_deadlines.insert(DeadlineKey { at: rto_at, seq });
        if out_of_order {
            self.reorder_sent.insert(DeadlineKey { at: sent_at, seq });
        }
    }

    /// Refresh an active packet's RTO deadline and reorder-boundary
    /// membership (e.g. after a tail-loss probe refreshes the packet, or
    /// the reorder boundary moved).  Ready reasons are preserved.
    pub(super) fn refresh(
        &mut self,
        seq: SequenceNumber,
        rto_at: Instant,
        sent_at: Instant,
        out_of_order: bool,
    ) {
        let Some(&old) = self.active.get(&seq) else {
            debug_assert!(false, "only active packets can enter the reorder index");
            return;
        };
        self.rto_deadlines.remove(&DeadlineKey {
            at: old.rto_at,
            seq,
        });
        self.rto_deadlines.insert(DeadlineKey { at: rto_at, seq });
        self.reorder_sent.remove(&DeadlineKey {
            at: old.sent_at,
            seq,
        });
        if out_of_order {
            self.reorder_sent.insert(DeadlineKey { at: sent_at, seq });
        }
        self.active.insert(seq, ActiveEntry { rto_at, sent_at });
    }

    /// Remove a packet from the index entirely: its RTO deadline key, its
    /// reorder send-time key (when present), and any ready reasons (with
    /// their `ready_deadlines` key).
    pub(super) fn deactivate(&mut self, seq: &SequenceNumber) {
        let Some(entry) = self.active.remove(seq) else {
            return;
        };
        self.rto_deadlines.remove(&DeadlineKey {
            at: entry.rto_at,
            seq: *seq,
        });
        self.reorder_sent.remove(&DeadlineKey {
            at: entry.sent_at,
            seq: *seq,
        });
        if let Some(reasons) = self.ready.remove(seq) {
            if let Some(earliest) = reasons.earliest() {
                self.ready_deadlines.remove(&DeadlineKey {
                    at: earliest,
                    seq: *seq,
                });
            }
        }
    }

    /// Set a ready reason on an active packet.  The previous earliest
    /// `ready_deadlines` key for the packet is removed before the mutation
    /// and the new earliest is stored afterward.  Only active packets may
    /// become ready.  `at` is the deadline at which the reason became due;
    /// `None` clears the reason (e.g. re-syncing a packet that is no longer
    /// reorder- or fast-loss-eligible).
    pub(super) fn update_ready(
        &mut self,
        seq: SequenceNumber,
        reason: ReadyReason,
        at: Option<Instant>,
    ) {
        self.mutate_ready(seq, |reasons| match reason {
            ReadyReason::Reorder => reasons.reorder = at,
            ReadyReason::FastLoss => reasons.fast_loss = at,
            ReadyReason::PreOutage => reasons.pre_outage = at,
        });
    }

    /// Mutate a ready packet's reasons, maintaining `ready_deadlines`:
    /// remove the previous earliest key before the mutation and store the
    /// new earliest afterward.
    fn mutate_ready(&mut self, seq: SequenceNumber, mutate: impl FnOnce(&mut ReadyReasons)) {
        debug_assert!(
            self.active.get(&seq).is_some(),
            "only active packets can become ready"
        );
        let mut reasons = self.ready.remove(&seq).unwrap_or_default();
        if let Some(earliest) = reasons.earliest() {
            self.ready_deadlines
                .remove(&DeadlineKey { at: earliest, seq });
        }
        mutate(&mut reasons);
        match reasons.earliest() {
            Some(earliest) => {
                self.ready.insert(seq, reasons);
                self.ready_deadlines
                    .insert(DeadlineKey { at: earliest, seq });
            }
            None => {
                // Every reason was cleared (e.g. a re-sync with `None`):
                // drop the entry so `ready.is_empty()` stays meaningful for
                // `has_due`.
            }
        }
    }

    /// Promote active packets whose time-based deadlines have elapsed:
    /// first every RTO deadline with `at <= now` (setting the RTO reason),
    /// then every reorder send time with `sent_at + reorder_window <= now`
    /// (setting the reorder reason).  Each reason is set independently, so
    /// a packet can become ready for both.
    pub(super) fn promote_due(&mut self, now: Instant, reorder_window: Duration) {
        while let Some(&deadline) = self.rto_deadlines.first() {
            if deadline.at > now {
                break;
            }
            self.rto_deadlines.remove(&deadline);
            self.mutate_ready(deadline.seq, |reasons| {
                reasons.rto = Some(deadline.at);
            });
        }
        while let Some(&sent) = self.reorder_sent.first() {
            if sent.at + reorder_window > now {
                break;
            }
            self.reorder_sent.remove(&sent);
            self.mutate_ready(sent.seq, |reasons| {
                reasons.reorder = Some(sent.at + reorder_window);
            });
        }
    }

    /// The logically-first ready packet (wrap-aware), if any.
    pub(super) fn first_ready(&self) -> Option<(SequenceNumber, &ReadyReasons)> {
        self.ready.first()
    }

    pub(super) fn has_due(&self, now: Instant, reorder_window: std::time::Duration) -> bool {
        !self.ready.is_empty()
            || self
                .rto_deadlines
                .first()
                .is_some_and(|deadline| deadline.at <= now)
            || self
                .reorder_sent
                .first()
                .is_some_and(|sent| sent.at + reorder_window <= now)
    }

    pub(super) fn next_deadline(&self, reorder_window: std::time::Duration) -> Option<Instant> {
        [
            self.ready_deadlines.first().map(|entry| entry.at),
            self.rto_deadlines.first().map(|entry| entry.at),
            self.reorder_sent
                .first()
                .map(|entry| entry.at + reorder_window),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    pub(super) fn is_active(&self, seq: &SequenceNumber) -> bool {
        self.active.get(seq).is_some()
    }

    pub(super) fn active_entries(&self) -> impl Iterator<Item = SequenceNumber> + '_ {
        self.active.iter().map(|(seq, _)| seq)
    }

    /// Advance the anchor of every wrapped map to the new send-window front.
    /// The caller must have removed (deactivated) every entry the advance
    /// would leave stale.
    pub(super) fn advance_anchor(&mut self, new_anchor: SequenceNumber) {
        self.active.advance_anchor(new_anchor);
        self.ready.advance_anchor(new_anchor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sq(n: u64) -> SequenceNumber {
        SequenceNumber::from_wire(n)
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn retransmit_index_and_deadline_order_cross_u64_wrap() {
        let t0 = Instant::now();
        // Index anchored just before the u64 wrap; sequences straddle
        // u64::MAX -> 0.
        let mut index = RetransmissionIndex::new(sq(u64::MAX - 1));
        index.activate(sq(u64::MAX - 1), t0 + ms(100), t0, false);
        index.activate(sq(u64::MAX), t0 + ms(100), t0 + ms(1), false);
        index.activate(sq(0), t0 + ms(200), t0 + ms(2), false);
        index.activate(sq(1), t0 + ms(50), t0 + ms(3), false);
        // The rto_deadlines set orders equal instants by wrap-aware serial
        // order — never by raw u64 value.
        let first = index.rto_deadlines.first().unwrap();
        assert_eq!(first.at, t0 + ms(50));
        assert_eq!(first.seq, sq(1));
        let mut deadline_iter = index.rto_deadlines.iter();
        assert_eq!(deadline_iter.next().unwrap().seq, sq(1));
        assert_eq!(deadline_iter.next().unwrap().seq, sq(u64::MAX - 1));
        assert_eq!(deadline_iter.next().unwrap().seq, sq(u64::MAX));
        assert_eq!(deadline_iter.next().unwrap().seq, sq(0));
        // All four deadlines remain individually removable across the wrap.
        index.deactivate(&sq(u64::MAX));
        assert!(!index.rto_deadlines.contains(&DeadlineKey {
            at: t0 + ms(100),
            seq: sq(u64::MAX)
        }));
        index.deactivate(&sq(1));
        index.deactivate(&sq(u64::MAX - 1));
        index.deactivate(&sq(0));
        assert!(index.rto_deadlines.is_empty());
        assert!(index.active_entries().next().is_none());
    }

    #[test]
    fn equal_deadlines_remain_removable_across_sequence_wrap() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(u64::MAX - 1));
        // Three packets share one deadline; their keys straddle the wrap.
        index.activate(sq(u64::MAX - 1), t0 + ms(100), t0, false);
        index.activate(sq(u64::MAX), t0 + ms(100), t0 + ms(1), false);
        index.activate(sq(0), t0 + ms(100), t0 + ms(2), false);
        assert_eq!(index.rto_deadlines.len(), 3);
        let order: Vec<u64> = index
            .rto_deadlines
            .iter()
            .map(|k| k.seq.to_wire())
            .collect();
        assert_eq!(
            order,
            vec![u64::MAX - 1, u64::MAX, 0],
            "equal deadlines tie by wrap-aware serial order"
        );
        // Removal uses the same total order as insertion, so each equal
        // deadline key stays findable across the wrap.
        index.deactivate(&sq(u64::MAX - 1));
        index.deactivate(&sq(u64::MAX));
        index.deactivate(&sq(0));
        assert!(index.rto_deadlines.is_empty());
        assert!(index.active_entries().next().is_none());
    }

    #[test]
    #[should_panic(expected = "only active packets can become ready")]
    fn inactive_packets_cannot_enter_the_ready_index() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        // A packet that was never activated cannot be made ready: the
        // ready index's active-set guard fires.
        index.update_ready(sq(5), ReadyReason::FastLoss, Some(t0 + ms(1)));
    }

    #[test]
    #[should_panic(expected = "only active packets can enter the reorder index")]
    fn inactive_packets_cannot_enter_the_reorder_index() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        // A packet that was never activated cannot be given a reorder
        // send-time entry: the reorder index's active-set guard fires.
        index.refresh(sq(7), t0 + ms(100), t0, true);
    }
}
