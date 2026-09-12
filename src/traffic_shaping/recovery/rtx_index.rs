//! Wrap-safe eligibility and deadline index for retransmission.
//!
//! Replaces the per-poll send-window scans of `has_rtx` / `rtx` /
//! `next_poll_time` with one coherent index.  Every in-flight packet of the
//! send window is tracked as an *active* entry, each carrying its RTO
//! deadline (`rto_at`) and its original send time (`sent_at`).  The repair
//! index is deliberately decoupled from the congestion window: the cwnd
//! gates NEW packets, while retransmission eligibility spans the whole
//! in-flight window so a cwnd shrink below the in-flight count can never
//! strand the tail outside the index.  The RTO deadline is indexed directly;
//! the reorder-window deadline is derived at query time as
//! `sent_at + reorder_window` from the `reorder_sent` send-time keys (the
//! window tracks SRTT and changes over a packet's life).
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
//! sequence space - the invariant every caller maintains.
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::Instant;

use crate::sequence::{HALF_SEQUENCE_SPACE, SequenceMap, SequenceNumber, SequenceVacancyError, lt};
/// Deadline for an active packet: the RTO retransmission deadline (send
/// time + the packet's RTO), the original send time (used to derive the
/// reorder-window deadline at query time), whether the packet's RTO is
/// eligible for the lazy live-estimator floor in [`RetransmissionIndex::promote_due`],
/// and whether the packet currently has a reorder send-time key in
/// `reorder_sent` (so `deactivate` can remove it without rescanning).
#[derive(Debug, Clone, Copy)]
struct ActiveEntry {
    /// The stored/latched RTO deadline: the RTO deadline as of the last
    /// activation or postpone.  For floor-eligible packets this is only a
    /// *scheduling hint* — readiness is re-validated live against the current
    /// estimator via [`ActiveEntry::effective_rto_deadline`] — while for
    /// tail-probe-derived RTOs it is the exact deadline.
    rto_at: Instant,
    /// The packet's own RTO deadline offset at activation (`rto_at -
    /// sent_at`), never latched up by the live-estimator floor: the true
    /// dynamic deadline for a floor-eligible packet is
    /// `sent_at + max(packet_rto, live_rto)`, recomputed at query time so a
    /// stale latched instant cannot diverge from the live estimator.
    packet_rto: std::time::Duration,
    sent_at: Instant,
    apply_live_rto_floor: bool,
    reorder_indexed: bool,
}

impl ActiveEntry {
    /// The effective RTO deadline of an active packet, evaluated against the
    /// *current* live estimator: for a floor-eligible packet the true
    /// dynamic deadline `sent_at + max(packet_rto, live_rto)` (never a stale
    /// latched instant); for a tail-probe-derived RTO the exact stored
    /// deadline.
    fn effective_rto_deadline(&self, live_rto: std::time::Duration) -> Instant {
        if self.apply_live_rto_floor {
            self.sent_at + self.packet_rto.max(live_rto)
        } else {
            self.rto_at
        }
    }
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

/// Pending deferred CC loss-event recordings for the jitter-tolerant fast-
/// retransmit path: each entry is a packet whose loss event was deferred to
/// the stock reorder-window deadline and is recorded (if still unacked) once
/// that deadline pops.  Keyed by sequence for O(1) cancellation on ACK, with
/// an independent deadline-ordered set so the next due deadline is selected
/// without rescanning the pending set.
#[derive(Debug)]
pub(super) struct DeferredLossIndex {
    by_sequence: SequenceMap<Instant>,
    deadlines: BTreeSet<DeadlineKey>,
}

impl DeferredLossIndex {
    pub(super) fn new(anchor: SequenceNumber) -> Self {
        Self {
            by_sequence: SequenceMap::new(anchor, HALF_SEQUENCE_SPACE - 1),
            deadlines: BTreeSet::new(),
        }
    }

    pub(super) fn insert(&mut self, seq: SequenceNumber, deadline: Instant) {
        self.by_sequence
            .insert_vacant(seq, deadline)
            .expect("deferred-loss sequence must be vacant and inside the live window");
        assert!(
            self.deadlines.insert(DeadlineKey { at: deadline, seq }),
            "deferred-loss deadline must be unique"
        );
    }

    pub(super) fn cancel(&mut self, seq: SequenceNumber) -> bool {
        let Some(deadline) = self.by_sequence.remove(&seq) else {
            return false;
        };
        assert!(
            self.deadlines.remove(&DeadlineKey { at: deadline, seq }),
            "cancelled deferred-loss deadline must be present"
        );
        true
    }

    pub(super) fn pop_due(&mut self, now: Instant) -> Option<SequenceNumber> {
        let first = self.deadlines.first().copied()?;
        if first.at > now {
            return None;
        }
        self.deadlines.pop_first();
        let removed = self.by_sequence.remove(&first.seq);
        assert_eq!(
            removed,
            Some(first.at),
            "deferred-loss sequence and deadline indexes must agree"
        );
        Some(first.seq)
    }

    pub(super) fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.first().map(|entry| entry.at)
    }

    pub(super) fn advance_anchor(&mut self, anchor: SequenceNumber) {
        self.by_sequence.advance_anchor(anchor);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.by_sequence.is_empty()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.by_sequence.len()
    }

    #[cfg(test)]
    pub(super) fn deadline(&self, seq: SequenceNumber) -> Option<Instant> {
        self.by_sequence.get(&seq).copied()
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

    pub(super) fn has_pre_outage(self) -> bool {
        self.pre_outage.is_some()
    }

    pub(super) fn has_fast_loss(self) -> bool {
        self.fast_loss.is_some()
    }

    pub(super) fn has_reorder(self) -> bool {
        self.reorder.is_some()
    }

    pub(super) fn has_rto(self) -> bool {
        self.rto.is_some()
    }

    /// The `since` deadline of one independent reason, if set.
    fn get(self, reason: ReadyReason) -> Option<Instant> {
        match reason {
            ReadyReason::FastLoss => self.fast_loss,
            ReadyReason::PreOutage => self.pre_outage,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ReadyReason {
    FastLoss,
    PreOutage,
}

/// The wrap-safe retransmission index.
#[derive(Debug)]
pub(super) struct RetransmissionIndex {
    active: SequenceMap<ActiveEntry>,
    rto_deadlines: BTreeSet<DeadlineKey>,
    /// Active floor-eligible (`apply_live_rto_floor`) entries keyed by their
    /// original send time, so the live-estimator bound
    /// `sent_at + live_rto <= now` — the shrunk-estimator case where a
    /// latched-up stored key is still in the future — can be found without
    /// rescanning the deadline set.
    floor_sent: BTreeSet<DeadlineKey>,
    /// Scratch buffer for [`Self::promote_due`]'s due-prefix copy. Reused
    /// across calls so the per-tick promotion pass never allocates: the loop
    /// body mutates the index, so the due keys cannot be iterated in place
    /// out of the set.
    floor_sent_scratch: Vec<DeadlineKey>,
    reorder_sent: BTreeSet<DeadlineKey>,
    ready: SequenceMap<ReadyReasons>,
    ready_deadlines: BTreeSet<DeadlineKey>,
    /// Number of active packets whose RTO reason is currently armed in
    /// `ready`.  Maintained incrementally so `has_rto_due` can answer
    /// "is any RTO due" in O(1) once any RTO has been promoted.
    rto_ready_count: usize,
}

impl RetransmissionIndex {
    pub(super) fn new(anchor: SequenceNumber) -> Self {
        Self {
            active: SequenceMap::new(anchor, HALF_SEQUENCE_SPACE - 1),
            rto_deadlines: BTreeSet::new(),
            floor_sent: BTreeSet::new(),
            floor_sent_scratch: Vec::new(),
            reorder_sent: BTreeSet::new(),
            ready: SequenceMap::new(anchor, HALF_SEQUENCE_SPACE - 1),
            ready_deadlines: BTreeSet::new(),
            rto_ready_count: 0,
        }
    }

    /// Activate a packet: insert it into the active set, atomically adding
    /// its RTO deadline key and - when the packet is out-of-order-passed -
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
        match self.active.insert_vacant_with(seq, || ActiveEntry {
            rto_at,
            packet_rto: rto_at - sent_at,
            sent_at,
            apply_live_rto_floor,
            reorder_indexed: reorder_eligible,
        }) {
            Ok(()) => {}
            Err(SequenceVacancyError::Occupied) => return,
            Err(SequenceVacancyError::Outside(position)) => {
                panic!(
                    "active retransmission sequence must be inside the live window: {position:?}"
                );
            }
        }
        self.rto_deadlines.insert(DeadlineKey { at: rto_at, seq });
        if apply_live_rto_floor {
            self.floor_sent.insert(DeadlineKey { at: sent_at, seq });
        }
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
        if entry.apply_live_rto_floor {
            self.floor_sent.remove(&DeadlineKey {
                at: entry.sent_at,
                seq,
            });
        }
        if entry.reorder_indexed {
            self.reorder_sent.remove(&DeadlineKey {
                at: entry.sent_at,
                seq,
            });
        }
        self.clear_ready(seq);
        true
    }

    /// Promote active packets whose time-based deadlines have elapsed:
    /// first every RTO deadline with `at <= now` (setting the RTO reason,
    /// lazily postponing stale non-tail-probe deadlines below the live
    /// estimator floor to the current effective deadline `sent_at +
    /// max(packet_rto, live_rto)`), then every reorder send time with
    /// `sent_at + reorder_window <= now` (setting the reorder reason).
    /// Each reason is set independently, so a packet can become ready for
    /// both.
    ///
    /// The stored RTO deadline is only a *scheduling hint*: every promotion
    /// decision is re-validated against the current live estimator.  A
    /// floor-eligible packet whose live deadline passed while a latched-up
    /// stored key (an earlier RTT spike postponed it) is still in the future
    /// is promoted here too — at its true dynamic deadline — so the index
    /// never withholds retransmission past what the estimator says is lost.
    /// Returns the number of RTO deadlines postponed by the live floor.
    pub(super) fn promote_due(
        &mut self,
        now: Instant,
        reorder_window: std::time::Duration,
        live_rto: std::time::Duration,
    ) -> usize {
        let mut rto_deadline_postponements = 0;
        // Stored-deadline prefix: a key that elapses is ready at its
        // *effective* deadline — re-validated now against the live estimator —
        // or postponed (lazily) when the estimator grew past it.
        while let Some(&deadline) = self.rto_deadlines.first() {
            if now < deadline.at {
                break;
            }
            let entry = self
                .active
                .get_mut(&deadline.seq)
                .expect("RTO deadline must belong to an active packet");
            let effective = entry.effective_rto_deadline(live_rto);
            if effective > now {
                self.rto_deadlines.remove(&deadline);
                entry.rto_at = effective;
                self.rto_deadlines.insert(DeadlineKey {
                    at: effective,
                    seq: deadline.seq,
                });
                rto_deadline_postponements += 1;
                continue;
            }
            self.rto_deadlines.remove(&deadline);
            self.update_ready(deadline.seq, |reasons| reasons.rto = Some(effective));
        }
        // Shrunk-estimator floor entries: a latched-up stored key is still in
        // the future, but the *live* deadline has passed (the estimator
        // cleared after an RTT spike).  The send-window scans already treat
        // these as lost; promote them at the true deadline so repair is not
        // withheld until the stale latched instant.  Iterated in send-time
        // order (FIFO by sequence), preserving the index's selection order.
        let mut floor_due = std::mem::take(&mut self.floor_sent_scratch);
        floor_due.clear();
        floor_due.extend(
            self.floor_sent
                .iter()
                .take_while(|key| key.at <= now && live_rto <= now.duration_since(key.at))
                .copied(),
        );
        for key in floor_due.iter().copied() {
            let seq = key.seq;
            if self
                .ready
                .get(&seq)
                .is_some_and(|reasons| reasons.rto.is_some())
            {
                continue;
            }
            let entry = self
                .active
                .get(&seq)
                .expect("floor deadline must belong to an active packet");
            let effective = entry.effective_rto_deadline(live_rto);
            if effective <= now {
                self.rto_deadlines.remove(&DeadlineKey {
                    at: entry.rto_at,
                    seq,
                });
                self.update_ready(seq, |reasons| reasons.rto = Some(effective));
            }
        }
        self.floor_sent_scratch = floor_due;
        while let Some(&sent) = self.reorder_sent.first() {
            let deadline = sent.at + reorder_window;
            if now < deadline {
                break;
            }
            self.reorder_sent.pop_first();
            self.active
                .get_mut(&sent.seq)
                .expect("reorder deadline must belong to an active packet")
                .reorder_indexed = false;
            self.update_ready(sent.seq, |reasons| reasons.reorder = Some(deadline));
        }
        rto_deadline_postponements
    }

    pub(super) fn has_due(
        &self,
        now: Instant,
        reorder_window: impl FnOnce() -> std::time::Duration,
        live_rto: std::time::Duration,
    ) -> bool {
        !self.ready.is_empty()
            || self
                .rto_deadlines
                .first()
                .is_some_and(|deadline| deadline.at <= now)
            || self
                .reorder_sent
                .first()
                .is_some_and(|sent| sent.at + reorder_window() <= now)
            || self.has_stuck_rto_due(now, live_rto)
    }

    /// Whether any active floor-eligible packet is RTO-due at the *live*
    /// deadline while its latched stored key is still in the future (the
    /// estimator cleared after an RTT spike postponed the key).  These are
    /// exactly the entries whose `sent_at + live_rto <= now`.
    fn has_stuck_rto_due(&self, now: Instant, live_rto: std::time::Duration) -> bool {
        self.floor_sent
            .iter()
            .take_while(|key| key.at <= now && live_rto <= now.duration_since(key.at))
            .any(|key| {
                let entry = self
                    .active
                    .get(&key.seq)
                    .expect("floor deadline must belong to an active packet");
                entry.effective_rto_deadline(live_rto) <= now
            })
    }

    /// Deadline used only to schedule the next send-loop wake. If one source
    /// is already due, later sources cannot make that wake more immediate.
    ///
    /// The stored RTO deadline is a scheduling hint: for a floor-eligible
    /// packet the *effective* deadline `sent_at + max(packet_rto, live_rto)`
    /// is evaluated against the current estimator, and a floor packet already
    /// due at that effective deadline while its latched stored key is still
    /// in the future wakes the poll immediately (never waiting out a stale
    /// latched instant).  Every returned wake time is a moment at which
    /// `promote_due` promotes (or postpones) a real deadline, so the poll
    /// can never spin on a not-yet-due instant.
    pub(super) fn next_deadline(
        &self,
        now: Instant,
        reorder_window: impl FnOnce() -> std::time::Duration,
        live_rto: std::time::Duration,
    ) -> Option<Instant> {
        let mut next = self.ready_deadlines.first().map(|entry| entry.at);
        if next.is_some_and(|deadline| deadline <= now) {
            return next;
        }
        // Earliest-sent floor entry: the earliest live-estimator bound is
        // `sent_at + live_rto`, so its effective deadline is the earliest
        // wake at which `promote_due` can act on a floor packet — exact when
        // per-packet RTOs are uniform, a safe (never-early) bound otherwise.
        if let Some(key) = self.floor_sent.first()
            && key.at <= now
        {
            let entry = self
                .active
                .get(&key.seq)
                .expect("floor deadline must belong to an active packet");
            let effective = entry.effective_rto_deadline(live_rto);
            if effective <= now
                && self
                    .ready
                    .get(&key.seq)
                    .is_none_or(|reasons| reasons.rto.is_none())
            {
                return Some(now);
            }
            next = Some(next.map_or(effective, |current| current.min(effective)));
        }
        if let Some(deadline) = self.rto_deadlines.first().map(|entry| entry.at) {
            next = Some(next.map_or(deadline, |current| current.min(deadline)));
        }
        if next.is_some_and(|deadline| deadline <= now) {
            return next;
        }
        if let Some(deadline) = self
            .reorder_sent
            .first()
            .map(|entry| entry.at + reorder_window())
        {
            next = Some(next.map_or(deadline, |current| current.min(deadline)));
        }
        next
    }

    /// Set a ready reason on an active packet.  `since` is the deadline at
    /// which the reason became due; `None` clears the reason (e.g.
    /// re-syncing a packet that is no longer eligible).  Setting the same
    /// reason to the same value is a no-op that preserves the index.
    pub(super) fn set_reason(
        &mut self,
        seq: SequenceNumber,
        reason: ReadyReason,
        since: Option<Instant>,
    ) {
        assert!(
            self.active.contains_key(&seq),
            "only active packets may become retransmission-ready"
        );
        if self
            .ready
            .get(&seq)
            .copied()
            .unwrap_or_default()
            .get(reason)
            == since
        {
            return;
        }
        self.update_ready_active(seq, |reasons| match reason {
            ReadyReason::FastLoss => reasons.fast_loss = since,
            ReadyReason::PreOutage => reasons.pre_outage = since,
        });
    }

    /// Re-sync an active packet's evidence-based reasons on a send-window
    /// sync: clear the transient reorder reason (re-armed by the caller via
    /// [`Self::add_reorder_candidate`] when the packet stays eligible),
    /// drop the fast-loss reason unless the caller says the evidence still
    /// holds, and refresh the pre-outage reason from the packet's own send
    /// time.  The RTO reason is never touched here.  Returns whether the
    /// reorder reason was previously set — the caller re-adds the reorder
    /// deadline key in that case so the boundary keeps tracking.
    pub(super) fn sync_active_evidence_reasons(
        &mut self,
        seq: SequenceNumber,
        preserve_fast_loss: bool,
        pre_outage: Option<Instant>,
    ) -> bool {
        debug_assert!(
            self.active.contains_key(&seq),
            "only active packets may become retransmission-ready"
        );
        let current = self.ready.get(&seq).copied().unwrap_or_default();
        let rearm_reorder = current.reorder.is_some();
        if current.reorder.is_none()
            && (preserve_fast_loss || current.fast_loss.is_none())
            && current.pre_outage == pre_outage
        {
            return false;
        }
        self.update_ready_active(seq, |reasons| {
            reasons.reorder = None;
            if !preserve_fast_loss {
                reasons.fast_loss = None;
            }
            reasons.pre_outage = pre_outage;
        });
        rearm_reorder
    }

    /// Mutate a ready packet's reasons, maintaining `ready_deadlines`:
    /// remove the previous earliest key before the mutation and store the
    /// new earliest afterward.  Only active packets may become ready.
    fn update_ready(&mut self, seq: SequenceNumber, update: impl FnOnce(&mut ReadyReasons)) {
        assert!(
            self.active.contains_key(&seq),
            "only active packets may become retransmission-ready"
        );
        self.update_ready_active(seq, update);
    }

    /// The `update_ready` core without the active-set guard, also
    /// maintaining the incremental RTO-ready count: a packet whose RTO
    /// reason transitions from unset to set increments the count, and one
    /// whose RTO reason is cleared decrements it.
    fn update_ready_active(&mut self, seq: SequenceNumber, update: impl FnOnce(&mut ReadyReasons)) {
        let mut reasons = self.ready.remove(&seq).unwrap_or_default();
        let rto_was_ready = reasons.rto.is_some();
        if let Some(previous) = reasons.earliest() {
            self.ready_deadlines
                .remove(&DeadlineKey { at: previous, seq });
        }
        update(&mut reasons);
        match (rto_was_ready, reasons.rto.is_some()) {
            (false, true) => self.rto_ready_count += 1,
            (true, false) => {
                self.rto_ready_count = self
                    .rto_ready_count
                    .checked_sub(1)
                    .expect("RTO-ready count must cover every stored RTO reason");
            }
            _ => {}
        }
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
            if reasons.rto.is_some() {
                self.rto_ready_count = self
                    .rto_ready_count
                    .checked_sub(1)
                    .expect("RTO-ready count must cover every removed RTO reason");
            }
            let since = reasons.earliest().expect("stored ready entry has a reason");
            self.ready_deadlines.remove(&DeadlineKey { at: since, seq });
        }
    }

    /// Give an active packet a reorder send-time key so the reorder-window
    /// deadline (`sent_at + reorder_window`) is tracked for it.  Used when
    /// the reorder boundary advances past an already-active packet without
    /// a full re-activation; idempotent while the key is present.
    pub(super) fn add_reorder_candidate(&mut self, seq: SequenceNumber, sent_at: Instant) {
        let entry = self
            .active
            .get_mut(&seq)
            .expect("only active packets may enter the reorder deadline index");
        assert_eq!(
            entry.sent_at, sent_at,
            "reorder candidate send time must match its active entry"
        );
        if entry.reorder_indexed {
            return;
        }
        entry.reorder_indexed = true;
        assert!(
            self.reorder_sent.insert(DeadlineKey { at: sent_at, seq }),
            "newly indexed reorder candidate must have a vacant deadline key"
        );
    }

    pub(super) fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Whether any active packet is RTO-ready (promoted) or has an RTO
    /// deadline that is due now.  A floor-eligible packet is due iff its
    /// *effective* deadline `sent_at + max(packet_rto, live_rto)` has passed
    /// — evaluated live, so a latched-up stored key can never make the index
    /// disagree with the estimator-driven send-window scans.
    pub(super) fn has_rto_due(&self, now: Instant, live_rto: std::time::Duration) -> bool {
        if self.rto_ready_count > 0 {
            return true;
        }
        self.rto_deadlines
            .iter()
            .take_while(|deadline| deadline.at <= now)
            .any(|deadline| {
                let entry = self
                    .active
                    .get(&deadline.seq)
                    .expect("RTO deadline must belong to an active packet");
                entry.effective_rto_deadline(live_rto) <= now
            })
            || self.has_stuck_rto_due(now, live_rto)
    }

    pub(super) fn last_active(&self) -> Option<SequenceNumber> {
        self.active.last().map(|(seq, _)| seq)
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

    /// Number of packets currently retransmission-ready (any reason armed).
    pub(super) fn ready_count(&self) -> usize {
        self.ready.len()
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

    /// `promote_due` runs on every send-loop tick. It must not allocate a
    /// fresh `Vec` per call: the due-prefix scratch buffer is reused, so a
    /// second pass with the same due prefix keeps the buffer's capacity (and
    /// therefore its allocation) unchanged.
    #[test]
    fn promote_due_reuses_its_scratch_buffer_across_calls() {
        let t0 = Instant::now();
        let live = ms(50);
        let mut index = RetransmissionIndex::new(sq(0));
        for i in 0..8u64 {
            index.activate(RetransmissionActivation {
                seq: sq(i),
                rto_at: t0 + ms(30),
                sent_at: t0,
                apply_live_rto_floor: true,
                reorder_eligible: false,
                fast_loss_eligible: false,
                pre_outage_eligible: false,
            });
        }
        let now = t0 + live + ms(1);
        let _ = index.promote_due(now, ms(100), live);
        let capacity_after_first = index.floor_sent_scratch.capacity();
        assert!(
            capacity_after_first >= 8,
            "the first pass must materialize all 8 due floor keys, got capacity {capacity_after_first}"
        );
        let _ = index.promote_due(now, ms(100), live);
        assert_eq!(
            index.floor_sent_scratch.capacity(),
            capacity_after_first,
            "a second promote_due pass with the same due prefix must reuse the scratch buffer, not allocate"
        );
    }

    #[test]
    fn next_deadline_selects_each_earliest_source() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        index.activate(RetransmissionActivation {
            seq: sq(0),
            rto_at: t0 + ms(40),
            sent_at: t0,
            apply_live_rto_floor: false,
            reorder_eligible: true,
            fast_loss_eligible: false,
            pre_outage_eligible: false,
        });
        index.set_reason(sq(0), ReadyReason::FastLoss, Some(t0 + ms(50)));
        assert_eq!(
            index.next_deadline(t0, || ms(100), ms(100)),
            Some(t0 + ms(40))
        );
        index.deactivate(sq(0));
        index.activate(RetransmissionActivation {
            seq: sq(1),
            rto_at: t0 + ms(300),
            sent_at: t0,
            apply_live_rto_floor: false,
            reorder_eligible: true,
            fast_loss_eligible: false,
            pre_outage_eligible: false,
        });
        index.set_reason(sq(1), ReadyReason::FastLoss, Some(t0 + ms(50)));
        assert_eq!(
            index.next_deadline(t0, || ms(100), ms(100)),
            Some(t0 + ms(50))
        );
        index.set_reason(sq(1), ReadyReason::FastLoss, None);
        assert_eq!(
            index.next_deadline(t0, || ms(100), ms(100)),
            Some(t0 + ms(100))
        );
    }

    #[test]
    fn duplicate_activation_leaves_every_existing_index_untouched() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        index.activate(activation(sq(0), t0 + ms(100), t0));
        index.activate(RetransmissionActivation {
            seq: sq(0),
            rto_at: t0 + ms(1),
            sent_at: t0 + ms(1),
            apply_live_rto_floor: false,
            reorder_eligible: true,
            fast_loss_eligible: true,
            pre_outage_eligible: true,
        });

        let active = index.active.get(&sq(0)).unwrap();
        assert_eq!(active.rto_at, t0 + ms(100));
        assert_eq!(active.sent_at, t0);
        assert!(!active.apply_live_rto_floor);
        assert!(!active.reorder_indexed);
        assert!(index.ready.is_empty());
        assert!(index.reorder_sent.is_empty());
        assert_eq!(index.rto_deadlines.first().unwrap().at, t0 + ms(100));
    }

    #[test]
    fn next_deadline_stops_after_the_first_due_wake_source() {
        use std::cell::Cell;

        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        index.activate(RetransmissionActivation {
            seq: sq(0),
            rto_at: t0 + ms(40),
            sent_at: t0,
            apply_live_rto_floor: false,
            reorder_eligible: true,
            fast_loss_eligible: false,
            pre_outage_eligible: false,
        });
        index.set_reason(sq(0), ReadyReason::FastLoss, Some(t0 + ms(50)));
        let reorder_window_read = Cell::new(false);
        assert_eq!(
            index.next_deadline(
                t0 + ms(60),
                || {
                    reorder_window_read.set(true);
                    ms(100)
                },
                ms(100)
            ),
            Some(t0 + ms(50))
        );
        assert!(!reorder_window_read.get());

        assert!(index.has_due(
            t0 + ms(60),
            || {
                reorder_window_read.set(true);
                ms(100)
            },
            ms(100)
        ));
        assert!(!reorder_window_read.get());
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
        assert_eq!(index.active_count(), 4);
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
        assert_eq!(index.active_count(), 0);
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
        assert!(!index.has_rto_due(t0 + ms(500), ms(900)));
        assert!(!index.has_rto_due(t0 + ms(899), ms(900)));
        assert!(index.has_rto_due(t0 + ms(900), ms(900)));
        assert_eq!(index.promote_due(t0 + ms(500), ms(100), ms(900)), 1);
        assert_eq!(index.rto_ready_count, 0);
        assert!(!index.has_due(t0 + ms(500), || ms(100), ms(900)));
        assert_eq!(
            index.next_deadline(t0 + ms(500), || ms(100), ms(900)),
            Some(t0 + ms(900))
        );
        // A second promote before the floored deadline is a no-op.
        assert_eq!(index.promote_due(t0 + ms(800), ms(100), ms(900)), 0);
        assert!(!index.has_due(t0 + ms(800), || ms(100), ms(900)));
        // At the floored deadline the packet becomes RTO-ready.
        assert_eq!(index.promote_due(t0 + ms(900), ms(100), ms(900)), 0);
        assert_eq!(index.rto_ready_count, 1);
        assert!(index.has_rto_due(t0 + ms(900), ms(900)));
        assert!(index.has_due(t0 + ms(900), || ms(100), ms(900)));
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
        assert!(!tail.has_rto_due(t0 + ms(299), ms(900)));
        assert!(tail.has_rto_due(t0 + ms(300), ms(900)));
        assert_eq!(tail.promote_due(t0 + ms(300), ms(100), ms(900)), 0);
        assert_eq!(tail.rto_ready_count, 1);
        assert!(tail.has_due(t0 + ms(300), || ms(100), ms(900)));
        let (seq, _) = tail.first_ready().unwrap();
        assert_eq!(seq, sq(0));
        tail.deactivate(sq(0));
        assert_eq!(tail.rto_ready_count, 0);
        assert!(!tail.has_rto_due(t0 + ms(300), ms(900)));
    }

    #[test]
    fn a_cleared_live_rto_repromotes_a_latched_stale_deadline_at_the_true_deadline() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        // A floor-eligible packet with a 500 ms RTO deadline at activation.
        index.activate(RetransmissionActivation {
            seq: sq(0),
            rto_at: t0 + ms(500),
            sent_at: t0,
            apply_live_rto_floor: true,
            reorder_eligible: false,
            fast_loss_eligible: false,
            pre_outage_eligible: false,
        });
        // Live estimator grows to 900 ms: the stored deadline is latched up.
        assert_eq!(index.promote_due(t0 + ms(500), ms(100), ms(900)), 1);
        assert_eq!(
            index.next_deadline(t0 + ms(500), || ms(100), ms(900)),
            Some(t0 + ms(900))
        );
        // Now the estimator clears to 200 ms (still below the packet's own
        // 500 ms RTO): the true deadline is sent_at + max(500, 200) = 500 ms,
        // already passed, while the latched stored key is at 900 ms.
        let due_now = t0 + ms(600);
        assert!(due_now < t0 + ms(900), "test requires now < latched key");
        // Scans compute max(packet_rto, live_rto) live and declare the packet
        // lost; the index must agree instead of withholding repair until the
        // stale latched instant.
        assert!(index.has_rto_due(due_now, ms(200)));
        assert!(index.has_due(due_now, || ms(100), ms(200)));
        // The wake must be immediate, not the stale latched instant.
        assert_eq!(
            index.next_deadline(due_now, || ms(100), ms(200)),
            Some(due_now)
        );
        // promote_due at the true deadline arms the RTO reason (no postpone).
        assert_eq!(index.promote_due(due_now, ms(100), ms(200)), 0);
        assert_eq!(index.rto_ready_count, 1);
        let (seq, _) = index.first_ready().unwrap();
        assert_eq!(seq, sq(0));
    }

    #[test]
    fn rto_ready_count_ignores_evidence_reasons_and_tracks_rto_lifetime() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        index.activate(activation(sq(0), t0 + ms(100), t0));
        index.set_reason(sq(0), ReadyReason::FastLoss, Some(t0 + ms(1)));
        assert_eq!(index.rto_ready_count, 0);
        assert!(!index.has_rto_due(t0 + ms(1), ms(100)));
        assert_eq!(index.promote_due(t0 + ms(100), ms(10), ms(100)), 0);
        assert_eq!(index.rto_ready_count, 1);
        assert!(index.has_rto_due(t0 + ms(100), ms(100)));
        assert!(!index.sync_active_evidence_reasons(sq(0), false, None));
        assert_eq!(index.rto_ready_count, 1);
        assert!(index.ready.get(&sq(0)).unwrap().rto.is_some());
        index.deactivate(sq(0));
        assert_eq!(index.rto_ready_count, 0);
        assert!(!index.has_rto_due(t0 + ms(100), ms(100)));
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
    fn identical_ready_reason_updates_preserve_the_index() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        index.activate(activation(sq(0), t0 + ms(100), t0));
        let fast_loss_at = t0 + ms(1);
        index.set_reason(sq(0), ReadyReason::FastLoss, Some(fast_loss_at));
        let first_deadline = index.ready_deadlines.first().copied();
        index.set_reason(sq(0), ReadyReason::FastLoss, Some(fast_loss_at));
        assert_eq!(index.ready.len(), 1);
        assert_eq!(index.ready_deadlines.len(), 1);
        assert_eq!(index.ready_deadlines.first().copied(), first_deadline);
        index.set_reason(sq(0), ReadyReason::FastLoss, None);
        index.set_reason(sq(0), ReadyReason::FastLoss, None);
        assert!(index.ready.is_empty());
        assert!(index.ready_deadlines.is_empty());
    }

    #[test]
    fn evidence_sync_clears_transient_reasons_and_preserves_rto() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        index.activate(activation(sq(0), t0 + ms(10), t0));
        index.promote_due(t0 + ms(10), ms(100), ms(10));
        index.update_ready(sq(0), |reasons| {
            reasons.reorder = Some(t0 + ms(2));
            reasons.fast_loss = Some(t0 + ms(3));
        });
        let pre_outage = t0 + ms(4);
        assert!(index.sync_active_evidence_reasons(sq(0), false, Some(pre_outage)));
        let reasons = *index.ready.get(&sq(0)).unwrap();
        assert_eq!(reasons.rto, Some(t0 + ms(10)));
        assert_eq!(reasons.reorder, None);
        assert_eq!(reasons.fast_loss, None);
        assert_eq!(reasons.pre_outage, Some(pre_outage));
        assert_eq!(index.ready_deadlines.len(), 1);
        assert_eq!(index.ready_deadlines.first().unwrap().at, pre_outage);
    }

    #[test]
    fn evidence_sync_preserves_still_eligible_fast_loss() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        index.activate(activation(sq(0), t0 + ms(100), t0));
        let fast_loss = t0 + ms(3);
        index.set_reason(sq(0), ReadyReason::FastLoss, Some(fast_loss));
        let first_deadline = index.ready_deadlines.first().copied();
        assert!(!index.sync_active_evidence_reasons(sq(0), true, None));
        let reasons = *index.ready.get(&sq(0)).unwrap();
        assert_eq!(reasons.fast_loss, Some(fast_loss));
        assert_eq!(index.ready_deadlines.len(), 1);
        assert_eq!(index.ready_deadlines.first().copied(), first_deadline);
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

    #[test]
    fn reorder_candidate_is_inserted_once_until_its_deadline_is_popped() {
        let t0 = Instant::now();
        let mut index = RetransmissionIndex::new(sq(0));
        index.activate(RetransmissionActivation {
            seq: sq(0),
            rto_at: t0 + ms(100),
            sent_at: t0,
            apply_live_rto_floor: false,
            reorder_eligible: true,
            fast_loss_eligible: false,
            pre_outage_eligible: false,
        });
        index.add_reorder_candidate(sq(0), t0);
        assert_eq!(index.reorder_sent.len(), 1);
        assert!(index.active.get(&sq(0)).unwrap().reorder_indexed);
        index.promote_due(t0 + ms(10), ms(10), ms(100));
        assert!(index.reorder_sent.is_empty());
        assert!(!index.active.get(&sq(0)).unwrap().reorder_indexed);
        index.add_reorder_candidate(sq(0), t0);
        assert_eq!(index.reorder_sent.len(), 1);
        assert!(index.active.get(&sq(0)).unwrap().reorder_indexed);
    }

    #[test]
    fn deferred_losses_cancel_and_pop_by_deadline_across_sequence_wrap() {
        let now = Instant::now();
        let mut index = DeferredLossIndex::new(sq(u64::MAX - 1));
        let first = now + ms(10);
        let cancelled = now + ms(20);
        let last = now + ms(30);
        index.insert(sq(u64::MAX), last);
        index.insert(sq(u64::MAX - 1), cancelled);
        index.insert(sq(0), first);
        assert_eq!(index.next_deadline(), Some(first));
        assert!(index.cancel(sq(u64::MAX - 1)));
        assert!(!index.cancel(sq(u64::MAX - 1)));
        assert_eq!(index.pop_due(first - ms(1)), None);
        assert_eq!(index.pop_due(first), Some(sq(0)));
        index.advance_anchor(sq(u64::MAX));
        assert_eq!(index.next_deadline(), Some(last));
        assert_eq!(index.pop_due(last), Some(sq(u64::MAX)));
        assert!(index.is_empty());
        assert_eq!(index.next_deadline(), None);
    }

    fn deferred_loss_cancel_cost(outstanding: u64) -> f64 {
        let mut best = f64::MAX;
        for _ in 0..3 {
            let now = Instant::now();
            let mut index = DeferredLossIndex::new(sq(0));
            for raw in 0..outstanding {
                index.insert(sq(raw), now + Duration::from_nanos(raw + 1));
            }
            let start = Instant::now();
            for raw in 0..outstanding {
                std::hint::black_box(index.next_deadline());
                assert!(index.cancel(sq(raw)));
            }
            assert!(index.is_empty());
            best = best.min(start.elapsed().as_secs_f64() / outstanding as f64 * 1e9);
        }
        best
    }

    fn linear_deferred_loss_cancel_cost(outstanding: u64) -> f64 {
        let mut best = f64::MAX;
        for _ in 0..3 {
            let now = Instant::now();
            let mut pending = (0..outstanding)
                .map(|raw| (sq(raw), now + Duration::from_nanos(raw + 1)))
                .collect::<Vec<_>>();
            let start = Instant::now();
            for raw in 0..outstanding {
                std::hint::black_box(pending.iter().map(|(_, deadline)| *deadline).min());
                let cancelled = sq(raw);
                pending.retain(|(sequence, _)| *sequence != cancelled);
            }
            assert!(pending.is_empty());
            best = best.min(start.elapsed().as_secs_f64() / outstanding as f64 * 1e9);
        }
        best
    }

    #[test]
    #[ignore = "perf lane: wall-clock ns/cancel ratio; run with cargo test --release -- --ignored"]
    fn deferred_loss_cancellation_does_not_rescan_the_pending_set() {
        let few = deferred_loss_cancel_cost(64);
        let many = deferred_loss_cancel_cost(4096);
        let linear_many = linear_deferred_loss_cancel_cost(4096);
        eprintln!(
            "deferred-loss cancellation plus next deadline: indexed {few:.1} ns/op at 64 pending and {many:.1} ns/op at 4096; linear reference {linear_many:.1} ns/op at 4096"
        );
        assert!(
            many < few * 8.0,
            "{many:.1} ns/op at 4096 pending against {few:.1} ns/op at 64: cancellation or deadline selection rescans the pending set"
        );
        assert!(
            many * 4.0 < linear_many,
            "indexed {many:.1} ns/op is not materially below the linear reference at {linear_many:.1} ns/op"
        );
    }
}
