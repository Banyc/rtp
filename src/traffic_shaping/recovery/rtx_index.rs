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
//! ([`RetransmissionIndex::set_reason`]).  All four reasons are independent
//! per packet: a packet can be ready for RTO, reorder, fast loss, and
//! pre-outage at once.  A non-tail-probe RTO whose stored deadline is below
//! the current live RTO estimator is *lazily* floored: `promote_due`
//! postpones it to `sent_at + live_rto` instead of promoting a stale
//! deadline.  Deadline ties break by wrap-aware sequence serial order,
//! which is a total order only within a live window smaller than half the
//! sequence space — the invariant every caller maintains.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::Instant;

use crate::sequence::{HALF_SEQUENCE_SPACE, SequenceMap, SequenceNumber, lt};

/// Deadline for an active packet: the RTO retransmission deadline (send
/// time + the packet's RTO), the original send time (used to derive the
/// reorder-window deadline at query time), and whether the packet's RTO is
/// eligible for the lazy live-estimator floor in [`RetransmissionIndex::promote_due`].
#[derive(Debug, Clone, Copy)]
struct ActiveEntry {
    rto_at: Instant,
    sent_at: Instant,
    apply_live_rto_floor: bool,
}

/// Everything needed to add a packet to the active set.  Carried as one
/// value so the caller states the packet's full eligibility in a single
/// place: the RTO/send deadlines, whether the live-RTO floor applies (never
/// for tail-probe-derived RTOs), and which evidence- and outage-based
/// reasons are already true for the packet.
#[derive(Debug, Clone, Copy)]
pub(super) struct RetransmissionActivation {
    pub(super) seq: SequenceNumber,
    pub(super) rto_at: Instant,
    pub(super) sent_at: Instant,
    pub(super) apply_live_rto_floor: bool,
    pub(super) reorder_eligible: bool,
    pub(super) fast_loss_eligible: bool,
    pub(super) pre_outage_eligible: bool,
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
    fn earliest(self) -> Option<Instant> {
        [self.rto, self.reorder, self.fast_loss, self.pre_outage]
            .into_iter()
            .flatten()
            .min()
    }

    /// Whether the evidence-gated fast-loss path declared the packet lost.
    pub(super) fn fast_loss_at(self) -> Option<Instant> {
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
    /// its reorder send-time key.  Evidence- and outage-based ready reasons
    /// named in the activation are set independently; time-based reasons
    /// are promoted later by [`Self::promote_due`].  A packet already
    /// active is left untouched (the caller re-syncs by deactivating
    /// first).
    pub(super) fn activate(&mut self, activation: RetransmissionActivation) {
        let RetransmissionActivation {
            seq,
            rto_at,
            sent_at,
            apply_live_rto_floor,
            reorder_eligible,
            fast_loss_eligible,
            pre_outage_eligible,
        } = activation;
        if self.active.contains_key(&seq) {
            return;
        }
        self.active
            .insert_vacant(
                seq,
                ActiveEntry {
                    rto_at,
                    sent_at,
                    apply_live_rto_floor,
                },
            )
            .expect("active retransmission sequence must be vacant and inside the live window");
        self.rto_deadlines.insert(DeadlineKey { at: rto_at, seq });
        if reorder_eligible {
            self.reorder_sent.insert(DeadlineKey { at: sent_at, seq });
        }
        if fast_loss_eligible {
            self.set_reason(seq, ReadyReason::FastLoss, Some(sent_at));
        }
        if pre_outage_eligible {
            self.set_reason(seq, ReadyReason::PreOutage, Some(sent_at));
        }
    }

    /// Remove a packet from the index entirely: its RTO deadline key, its
    /// reorder send-time key (when present), and any ready reasons (with
    /// their `ready_deadlines` key).  Returns whether the packet was
    /// active.
    pub(super) fn deactivate(&mut self, seq: SequenceNumber) -> bool {
        let Some(entry) = self.active.remove(&seq) else {
            return false;
        };
        self.rto_deadlines.remove(&DeadlineKey {
            at: entry.rto_at,
            seq,
        });
        self.reorder_sent.remove(&DeadlineKey {
            at: entry.sent_at,
            seq,
        });
        self.clear_ready(seq);
        true
    }

    /// Promote active packets whose time-based deadlines have elapsed:
    /// first every RTO deadline with `at <= now` (setting the RTO reason —
    /// but lazily postponing stale non-tail-probe deadlines below the live
    /// estimator floor to `sent_at + live_rto`), then every reorder send
    /// time with `sent_at + reorder_window <= now` (setting the reorder
    /// reason).  Each reason is set independently, so a packet can become
    /// ready for both.  Returns the number of RTO deadlines postponed by
    /// the live floor.
    pub(super) fn promote_due(
        &mut self,
        now: Instant,
        reorder_window: std::time::Duration,
        live_rto: std::time::Duration,
    ) -> usize {
        let mut rto_deadline_postponements = 0;
        while let Some(&deadline) = self.rto_deadlines.first() {
            if now < deadline.at {
                break;
            }
            let entry = self
                .active
                .get(&deadline.seq)
                .copied()
                .expect("RTO deadline must belong to an active packet");
            let live_deadline = entry.sent_at + live_rto;
            if entry.apply_live_rto_floor && deadline.at < live_deadline {
                self.rto_deadlines.remove(&deadline);
                self.active
                    .get_mut(&deadline.seq)
                    .expect("RTO deadline must belong to an active packet")
                    .rto_at = live_deadline;
                self.rto_deadlines.insert(DeadlineKey {
                    at: live_deadline,
                    seq: deadline.seq,
                });
                rto_deadline_postponements += 1;
                continue;
            }
            self.rto_deadlines.remove(&deadline);
            self.update_ready(deadline.seq, |reasons| reasons.rto = Some(deadline.at));
        }
        while let Some(&sent) = self.reorder_sent.first() {
            let deadline = sent.at + reorder_window;
            if now < deadline {
                break;
            }
            self.reorder_sent.pop_first();
            self.update_ready(sent.seq, |reasons| reasons.reorder = Some(deadline));
        }
        rto_deadline_postponements
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

    /// Set a ready reason on an active packet.  `since` is the deadline at
    /// which the reason became due; `None` clears the reason (e.g.
    /// re-syncing a packet that is no longer eligible).
    pub(super) fn set_reason(
        &mut self,
        seq: SequenceNumber,
        reason: ReadyReason,
        since: Option<Instant>,
    ) {
        self.update_ready(seq, |reasons| match reason {
            ReadyReason::Reorder => reasons.reorder = since,
            ReadyReason::FastLoss => reasons.fast_loss = since,
            ReadyReason::PreOutage => reasons.pre_outage = since,
        });
    }

    /// Mutate a ready packet's reasons, maintaining `ready_deadlines`:
    /// remove the previous earliest key before the mutation and store the
    /// new earliest afterward.  Only active packets may become ready.
    fn update_ready(&mut self, seq: SequenceNumber, update: impl FnOnce(&mut ReadyReasons)) {
        assert!(
            self.active.contains_key(&seq),
            "only active packets may become retransmission-ready"
        );
        let mut reasons = self.ready.remove(&seq).unwrap_or_default();
        if let Some(previous) = reasons.earliest() {
            self.ready_deadlines
                .remove(&DeadlineKey { at: previous, seq });
        }
        update(&mut reasons);
        self.store_ready(seq, reasons);
    }

    /// Store a packet's ready reasons: insert the `ready_deadlines` key for
    /// the new earliest reason, or drop the entry entirely when every
    /// reason was cleared (`ready.is_empty()` stays meaningful for
    /// `has_due`).
    fn store_ready(&mut self, seq: SequenceNumber, reasons: ReadyReasons) {
        if let Some(next) = reasons.earliest() {
            self.ready
                .insert_vacant(seq, reasons)
                .expect("ready retransmission sequence must be vacant and inside the live window");
            self.ready_deadlines.insert(DeadlineKey { at: next, seq });
        }
    }

    /// Remove a packet's ready reasons and their `ready_deadlines` key.
    fn clear_ready(&mut self, seq: SequenceNumber) {
        if let Some(reasons) = self.ready.remove(&seq) {
            let since = reasons.earliest().expect("stored ready entry has a reason");
            self.ready_deadlines.remove(&DeadlineKey { at: since, seq });
        }
    }

    /// Give an active packet a reorder send-time key so the reorder-window
    /// deadline (`sent_at + reorder_window`) is tracked for it.  Used when
    /// the reorder boundary advances past an already-active packet without
    /// a full re-activation.
    pub(super) fn add_reorder_candidate(&mut self, seq: SequenceNumber, sent_at: Instant) {
        assert!(
            self.active.contains_key(&seq),
            "only active packets may enter the reorder deadline index"
        );
        self.reorder_sent.insert(DeadlineKey { at: sent_at, seq });
    }

    /// The logically-first ready packet (wrap-aware), if any.
    pub(super) fn first_ready(&self) -> Option<(SequenceNumber, &ReadyReasons)> {
        self.ready.first()
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
    use std::time::Duration;

    fn sq(n: u64) -> SequenceNumber {
        SequenceNumber::from_wire(n)
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn activation(
        seq: SequenceNumber,
        rto_at: Instant,
        sent_at: Instant,
    ) -> RetransmissionActivation {
        RetransmissionActivation {
            seq,
            rto_at,
            sent_at,
            apply_live_rto_floor: false,
            reorder_eligible: false,
            fast_loss_eligible: false,
            pre_outage_eligible: false,
        }
    }

    #[test]
    fn retransmit_index_and_deadline_order_cross_u64_wrap() {
        let t0 = Instant::now();
        // Index anchored just before the u64 wrap; sequences straddle
        // u64::MAX -> 0.
        let mut index = RetransmissionIndex::new(sq(u64::MAX - 1));
        index.activate(activation(sq(u64::MAX - 1), t0 + ms(100), t0));
        index.activate(activation(sq(u64::MAX), t0 + ms(100), t0 + ms(1)));
        index.activate(activation(sq(0), t0 + ms(200), t0 + ms(2)));
        index.activate(activation(sq(1), t0 + ms(50), t0 + ms(3)));
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
        index.deactivate(sq(u64::MAX));
        assert!(!index.rto_deadlines.contains(&DeadlineKey {
            at: t0 + ms(100),
            seq: sq(u64::MAX)
        }));
        index.deactivate(sq(1));
        index.deactivate(sq(u64::MAX - 1));
        index.deactivate(sq(0));
        assert!(index.rto_deadlines.is_empty());
        assert!(index.active_entries().next().is_none());
    }

    #[test]
    fn equal_deadlines_remain_removable_across_sequence_wrap() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(u64::MAX - 1));
        // Three packets share one deadline; their keys straddle the wrap.
        index.activate(activation(sq(u64::MAX - 1), t0 + ms(100), t0));
        index.activate(activation(sq(u64::MAX), t0 + ms(100), t0 + ms(1)));
        index.activate(activation(sq(0), t0 + ms(100), t0 + ms(2)));
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
        index.deactivate(sq(u64::MAX - 1));
        index.deactivate(sq(u64::MAX));
        index.deactivate(sq(0));
        assert!(index.rto_deadlines.is_empty());
        assert!(index.active_entries().next().is_none());
    }

    #[test]
    fn live_rto_postpones_a_stale_packet_deadline_lazily() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        // A plain RTO deadline (500 ms) below the current live estimator
        // (900 ms) is floored lazily: promote_due postpones it to
        // sent_at + live_rto instead of promoting a stale deadline.
        index.activate(RetransmissionActivation {
            seq: sq(0),
            rto_at: t0 + ms(500),
            sent_at: t0,
            apply_live_rto_floor: true,
            reorder_eligible: false,
            fast_loss_eligible: false,
            pre_outage_eligible: false,
        });
        assert_eq!(index.promote_due(t0 + ms(500), ms(100), ms(900)), 1);
        assert!(!index.has_due(t0 + ms(500), ms(100)));
        assert_eq!(index.next_deadline(ms(100)), Some(t0 + ms(900)));
        // A second promote before the floored deadline is a no-op.
        assert_eq!(index.promote_due(t0 + ms(800), ms(100), ms(900)), 0);
        assert!(!index.has_due(t0 + ms(800), ms(100)));
        // At the floored deadline the packet becomes RTO-ready.
        assert_eq!(index.promote_due(t0 + ms(900), ms(100), ms(900)), 0);
        assert!(index.has_due(t0 + ms(900), ms(100)));
        let (seq, _) = index.first_ready().unwrap();
        assert_eq!(seq, sq(0));

        // A tail-probe-derived RTO (apply_live_rto_floor = false) is never
        // floored again: its tightened 300 ms deadline fires as stored even
        // though the live estimator is larger.
        let mut tail = RetransmissionIndex::new(sq(0));
        tail.activate(RetransmissionActivation {
            seq: sq(0),
            rto_at: t0 + ms(300),
            sent_at: t0,
            apply_live_rto_floor: false,
            reorder_eligible: false,
            fast_loss_eligible: false,
            pre_outage_eligible: false,
        });
        assert_eq!(tail.promote_due(t0 + ms(300), ms(100), ms(900)), 0);
        assert!(tail.has_due(t0 + ms(300), ms(100)));
        let (seq, _) = tail.first_ready().unwrap();
        assert_eq!(seq, sq(0));
    }

    #[test]
    #[should_panic(expected = "only active packets may become retransmission-ready")]
    fn inactive_packets_cannot_enter_the_ready_index() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        // A packet that was never activated cannot be made ready: the
        // ready index's active-set guard fires.
        index.set_reason(sq(5), ReadyReason::FastLoss, Some(t0 + ms(1)));
    }

    #[test]
    #[should_panic(expected = "only active packets may enter the reorder deadline index")]
    fn inactive_packets_cannot_enter_the_reorder_index() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        // A packet that was never activated cannot be given a reorder
        // send-time entry: the reorder index's active-set guard fires.
        index.add_reorder_candidate(sq(7), t0);
    }
}
