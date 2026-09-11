//! Sender-side condition gate deciding whether parity may be emitted for a
//! FEC data group.
//!
//! FEC eligibility has three independent conditions:
//!
//! 1. **Loss/recovery evidence** — the loss gate.  No congestion feedback
//!    and fewer than [`MIN_RECOVERY_SAMPLES`] primary-send recovery samples
//!    keep the gate closed.  It enables at [`ENABLE_LOSS`] (5%), stays
//!    enabled through [`DISABLE_LOSS`] (3%), and disables below that
//!    (hysteresis).
//! 2. **Genuinely spare capacity** — decided by the reliable layer
//!    (`can_send_tail_fec` plus zero application write waiters and no
//!    queue-building signal), not merely pacer tokens.  A closed capacity
//!    gate DEFERS the open group (holds it open, never destroys it) so a
//!    pending tail probe cannot wipe the tail group's parity; the deferred
//!    flush fires once capacity is spare again.
//! 3. **A tail/full-group policy request** — the caller (data-burst tail,
//!    in-stream full group, ACK/kill tail) must actually be asking for a
//!    flush.
//!
//! The recovery ratio is recovery sends divided by fresh sends over the most
//! recent [`RECOVERY_WINDOW`] (64) successfully emitted primary data
//! datagrams; armor duplicates and parity never enter that window.

/// Loss ratio at which a closed loss gate opens (5%).
const ENABLE_LOSS: f64 = 0.05;
/// Loss ratio below which an open loss gate closes again (3%); the gate stays
/// enabled between the two thresholds (hysteresis).
const DISABLE_LOSS: f64 = 0.03;
/// Number of primary data sends over which the recovery ratio is evaluated.
const RECOVERY_WINDOW: usize = 64;
/// Minimum primary-send recovery samples before the recovery ratio is
/// considered measured at all.
const MIN_RECOVERY_SAMPLES: usize = 16;

/// Outcome of evaluating the condition gate for one FEC data group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FecGateDecision {
    /// Every condition holds: flush parity for the open group.
    Flush,
    /// The loss gate is closed: no congestion loss feedback and/or too few
    /// recovery samples, or the effective loss ratio is below the enable
    /// threshold.
    LossNotWarranted,
    /// Loss warrants recovery but the capacity is not genuinely spare
    /// (queued/waiting application work, queue growth, cwnd pressure,
    /// retransmission, or a pending tail probe).  The caller DEFERS the open
    /// group — holds it open so its parity survives the capacity-closed
    /// window and flushes once capacity is spare again — rather than
    /// destroying it.
    NoSpareCapacity,
    /// Loss and capacity both hold, but this burst did not request a
    /// tail/full-group flush.
    TailNotRequested,
}

/// Sender-side FEC condition gate: loss hysteresis plus the recovery-sample
/// ring and the three-way decision.  Every observation counter here is
/// connection-lifetime cumulative.
#[derive(Debug)]
pub(crate) struct FecConditionGate {
    loss_active: bool,
    recovery_samples: [bool; RECOVERY_WINDOW],
    recovery_sample_len: usize,
    recovery_sample_next: usize,
    recovery_sends: usize,
}

impl Default for FecConditionGate {
    fn default() -> Self {
        Self {
            loss_active: false,
            recovery_samples: [false; RECOVERY_WINDOW],
            recovery_sample_len: 0,
            recovery_sample_next: 0,
            recovery_sends: 0,
        }
    }
}

impl FecConditionGate {
    /// Record one successfully emitted primary data datagram.  `recovery`
    /// marks a retransmission/repair send (`true`) versus a fresh send
    /// (`false`).  The window covers the most recent
    /// [`RECOVERY_WINDOW`] primary sends.
    pub(crate) fn record_data_send(&mut self, recovery: bool) {
        if self.recovery_sample_len == RECOVERY_WINDOW {
            self.recovery_sends -= usize::from(self.recovery_samples[self.recovery_sample_next]);
        } else {
            self.recovery_sample_len += 1;
        }
        self.recovery_samples[self.recovery_sample_next] = recovery;
        self.recovery_sends += usize::from(recovery);
        self.recovery_sample_next = (self.recovery_sample_next + 1) % RECOVERY_WINDOW;
    }

    /// Refresh the loss gate from the latest congestion feedback and the
    /// sender-side recovery samples.  `configured` is whether FEC is enabled
    /// at all: a disabled FEC keeps the gate closed regardless of evidence.
    pub(crate) fn refresh_loss(&mut self, configured: bool, congestion_loss: Option<f64>) {
        if !configured {
            self.loss_active = false;
            return;
        }
        let recovery_loss = self.recovery_loss_ratio();
        let effective_loss = match (congestion_loss, recovery_loss) {
            (Some(congestion), Some(recovery)) => Some(congestion.max(recovery)),
            (Some(loss), None) | (None, Some(loss)) => Some(loss),
            (None, None) => None,
        };
        self.loss_active = match effective_loss {
            None => false,
            Some(loss) if self.loss_active => loss >= DISABLE_LOSS,
            Some(loss) => loss >= ENABLE_LOSS,
        };
    }

    /// Decide for one open group: the loss gate first, then spare capacity,
    /// then the tail/full-group policy request.
    pub(crate) fn decide(&self, spare_capacity: bool, tail_requested: bool) -> FecGateDecision {
        if !self.loss_active {
            FecGateDecision::LossNotWarranted
        } else if !spare_capacity {
            FecGateDecision::NoSpareCapacity
        } else if !tail_requested {
            FecGateDecision::TailNotRequested
        } else {
            FecGateDecision::Flush
        }
    }

    pub(crate) fn loss_active(&self) -> bool {
        self.loss_active
    }

    /// Recovery ratio over the ring: recovery sends divided by fresh sends.
    /// `None` until [`MIN_RECOVERY_SAMPLES`] primary sends have been
    /// recorded; `Some(1.0)` when every sample is a recovery send.
    fn recovery_loss_ratio(&self) -> Option<f64> {
        if self.recovery_sample_len < MIN_RECOVERY_SAMPLES {
            return None;
        }
        let fresh_sends = self.recovery_sample_len - self.recovery_sends;
        if fresh_sends == 0 {
            return Some(1.0);
        }
        Some((self.recovery_sends as f64 / fresh_sends as f64).min(1.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh gate emits no parity even with spare capacity and a tail
    /// request: no congestion feedback and no recovery samples keep the loss
    /// gate closed.  Once feedback arrives, the enable/disable thresholds
    /// have hysteresis (open at 5%, stay open through 3%, close below).
    #[test]
    fn waits_for_feedback_and_uses_hysteresis() {
        let mut gate = FecConditionGate::default();
        assert!(
            !gate.loss_active(),
            "a fresh gate must be closed: startup without measured loss must emit no parity"
        );
        assert_eq!(
            gate.decide(true, true),
            FecGateDecision::LossNotWarranted,
            "spare capacity and a tail request alone must not open the gate"
        );

        // No congestion feedback at all stays closed.
        gate.refresh_loss(true, None);
        assert!(!gate.loss_active());

        // 6% congestion loss: opens (>= 5%).
        gate.refresh_loss(true, Some(0.06));
        assert!(gate.loss_active(), "6% loss must enable the gate");

        // 4% while active: stays open (>= 3%).
        gate.refresh_loss(true, Some(0.04));
        assert!(gate.loss_active(), "4% loss must stay enabled once active");

        // 2% while active: closes (< 3%).
        gate.refresh_loss(true, Some(0.02));
        assert!(!gate.loss_active(), "2% loss must disable the gate");

        // 4% while closed: stays closed (< 5% enable threshold) — hysteresis.
        gate.refresh_loss(true, Some(0.04));
        assert!(
            !gate.loss_active(),
            "4% loss after a disable must not re-enable below the 5% threshold"
        );

        // FEC disabled closes the gate regardless of evidence.
        gate.refresh_loss(false, Some(0.5));
        assert!(
            !gate.loss_active(),
            "disabled FEC must keep the gate closed"
        );
    }

    /// A burst whose recent sends are all retransmissions (a sparse tail that
    /// is mostly recovery) opens the loss gate purely from sender-side
    /// evidence, without any measured congestion feedback.
    #[test]
    fn sparse_tail_recovery_opens_the_loss_gate() {
        let mut gate = FecConditionGate::default();
        // 16 recovery samples (the minimum) with no fresh sends: the recovery
        // ratio is 1.0, so the gate opens without congestion feedback.
        for _ in 0..MIN_RECOVERY_SAMPLES {
            gate.record_data_send(true);
        }
        assert_eq!(
            gate.recovery_loss_ratio(),
            Some(1.0),
            "a fully-recovery window must report a 1.0 recovery ratio"
        );
        gate.refresh_loss(true, None);
        assert!(
            gate.loss_active(),
            "sparse-tail recovery evidence alone must open the loss gate"
        );
        assert_eq!(
            gate.decide(true, true),
            FecGateDecision::Flush,
            "an open gate with spare capacity and a tail request must flush"
        );

        // A window under the minimum sample count never opens the gate.
        let mut sparse = FecConditionGate::default();
        for _ in 0..(MIN_RECOVERY_SAMPLES - 1) {
            sparse.record_data_send(true);
        }
        sparse.refresh_loss(true, None);
        assert!(
            !sparse.loss_active(),
            "fewer than MIN_RECOVERY_SAMPLES samples must not open the gate"
        );
    }

    /// Spare capacity and the tail policy are independent conditions: with an
    /// open loss gate, each combination resolves to the first failing
    /// condition in the decide order.
    #[test]
    fn spare_capacity_and_tail_policy_are_independent_conditions() {
        let mut gate = FecConditionGate::default();
        gate.refresh_loss(true, Some(0.08));
        assert!(gate.loss_active());
        assert_eq!(
            gate.decide(false, false),
            FecGateDecision::NoSpareCapacity,
            "no capacity must win before the tail policy is consulted"
        );
        assert_eq!(
            gate.decide(false, true),
            FecGateDecision::NoSpareCapacity,
            "a tail request cannot override missing spare capacity"
        );
        assert_eq!(
            gate.decide(true, false),
            FecGateDecision::TailNotRequested,
            "spare capacity without a tail request must not flush"
        );
        assert_eq!(
            gate.decide(true, true),
            FecGateDecision::Flush,
            "open loss gate + spare capacity + tail request must flush"
        );
    }
}
