//! The consuming policy for the cross-lane congestion payload.
//!
//! The interactive lane measures its egress path and [`crate::cc`] aggregates
//! the fresh observations of every lane sharing that `(src, dst)` path. This
//! module owns the *policy* that reads that aggregate: whether a bulk lane may
//! claim a path the interactive lane is not using (R1), and whether the
//! interactive lane's own drain gate has fired so the bulk must reclaim the
//! queue it built (R2). R3 -- the attributed floor fed to the queue gate -- is
//! a parameter of `QueueGrowth::observe_with_floor`, owned with the rest of the
//! gate's arithmetic.
//!
//! Every predicate here is a pure function of the payload plus the bulk's own
//! loss sample, so each can be unit tested and each has a named failure. The
//! caller represents "no payload" as `None`, and every rule is skipped then, so
//! a connection with no interactive lane on its path runs the shipped policy
//! byte-for-byte.
//!
//! Two families of fields are deliberately **not** carried here, so a claim
//! rule cannot read them: the capacity/delivery-peak fields (a sparse
//! interactive lane's peak is its offered rate, not capacity), and the
//! liveness/stall/outage fields (a latency spike degrades the lane's sampling,
//! not its liveness; treating the absence of a sample as "the lane is gone"
//! would let the bulk compete during exactly the spike that costs the
//! interactive lane its latency budget).

use std::time::Duration;

use crate::cc::PathState;

use super::CC_DATA_LOSS_RATE;

/// The cross-lane facts a bulk controller consumes, derived from a fresh
/// [`PathState`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SharedPath {
    /// Data waiting on the interactive lane's send path.
    pub(crate) pending_bytes: usize,
    /// Applications blocked on the interactive lane's send path.
    pub(crate) write_waiters: usize,
    /// The interactive lane's offered packet rate.
    pub(crate) offered_pps: f64,
    /// The control interval that dates the payload.
    pub(crate) control_rtt: Option<Duration>,
    /// The interactive lane's own persistent-queue latch.
    pub(crate) reclaiming: bool,
    /// The cross-lane queue-free floor, when the payload carries one.
    pub(crate) floor: Option<Duration>,
    /// The interactive lane's own ordinary drain margin.
    pub(crate) tolerance: Option<Duration>,
    /// The least-queued lane's standing queue.
    pub(crate) queue_delay: Option<Duration>,
}

impl SharedPath {
    /// Derive the consumed policy view from a fresh group aggregate.
    pub(crate) fn from_state(state: &PathState) -> Self {
        Self {
            pending_bytes: state.pending_bytes,
            write_waiters: state.write_waiters,
            offered_pps: state.offered_pps,
            control_rtt: state.control_rtt,
            reclaiming: state.persistent_for.is_some(),
            floor: state.floor,
            tolerance: state.tolerance,
            queue_delay: state.queue_delay,
        }
    }

    /// R1's activity test: the interactive lane is genuinely idle only when it
    /// has nothing pending, no blocked writer, and offers below one datagram per
    /// control RTT. `Little's law` gives the threshold: a rate that low leaves
    /// `rate x RTT < 1` datagram in flight, so the lane cannot build a standing
    /// queue and cannot hold a share. A lane with no control RTT cannot be
    /// dated and is treated as busy.
    pub(crate) fn lane_idle(&self) -> bool {
        let Some(rtt) = self.control_rtt else {
            return false;
        };
        let rtt = rtt.as_secs_f64();
        if rtt <= 0.0 {
            return false;
        }
        self.pending_bytes == 0 && self.write_waiters == 0 && self.offered_pps < 1.0 / rtt
    }
}

/// R1: whether a fresh payload authorizes the bulk lane to claim the path.
/// Loss never triggers a claim; it may only veto one.
pub(crate) fn claim_armed(shared: &SharedPath, loss_event_rate: Option<f64>) -> bool {
    shared.lane_idle() && !loss_blocks(loss_event_rate)
}

/// R2: whether the interactive lane's own gate has fired, so the bulk must
/// drain the queue it built. The target is the interactive lane's own latch or
/// its `queue_delay > tolerance` gate -- closed loop, with no picked constant.
pub(crate) fn reclaim_armed(shared: &SharedPath) -> bool {
    shared.reclaiming
        || shared
            .queue_delay
            .zip(shared.tolerance)
            .is_some_and(|(queue, tolerance)| queue > tolerance)
}

/// Whether a sampled loss is significant enough to veto an idle claim.
fn loss_blocks(loss_event_rate: Option<f64>) -> bool {
    loss_event_rate.is_some_and(|loss| loss >= CC_DATA_LOSS_RATE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idle() -> SharedPath {
        SharedPath {
            pending_bytes: 0,
            write_waiters: 0,
            offered_pps: 0.0,
            control_rtt: Some(Duration::from_millis(50)),
            reclaiming: false,
            floor: Some(Duration::from_millis(50)),
            tolerance: Some(Duration::from_millis(6)),
            queue_delay: Some(Duration::from_millis(3)),
        }
    }

    #[test]
    fn a_genuinely_idle_lane_arms_the_claim() {
        assert!(claim_armed(&idle(), Some(0.0)));
        assert!(claim_armed(&idle(), None));
    }

    /// Each of the three activity conjuncts is load-bearing on its own: any one
    /// of them saying the lane is busy must block the claim, and a lane that
    /// cannot be dated must not read as idle.
    #[test]
    fn any_busy_activity_field_blocks_the_claim() {
        let mut pending = idle();
        pending.pending_bytes = 1;
        assert!(
            !claim_armed(&pending, Some(0.0)),
            "pending send bytes must block the claim"
        );
        let mut waiters = idle();
        waiters.write_waiters = 1;
        assert!(
            !claim_armed(&waiters, Some(0.0)),
            "application write waiters must block the claim"
        );
        let mut rate = idle();
        rate.offered_pps = 1.0 / 0.05; // exactly the threshold is not below it
        assert!(
            !claim_armed(&rate, Some(0.0)),
            "an at-threshold offer must block the claim"
        );
        let mut undateable = idle();
        undateable.control_rtt = None;
        assert!(
            !claim_armed(&undateable, Some(0.0)),
            "a lane with no control RTT cannot be dated and must not read as idle"
        );
    }

    /// Loss is a veto, never a trigger: a busy lane is not armed whatever the
    /// loss sample; an idle lane is vetoed at the significance threshold and
    /// armed just below it.
    #[test]
    fn loss_vetoes_the_claim_and_never_triggers_it() {
        let mut busy = idle();
        busy.pending_bytes = 1;
        assert!(!claim_armed(&busy, None));
        assert!(!claim_armed(&idle(), Some(CC_DATA_LOSS_RATE)));
        assert!(claim_armed(&idle(), Some(CC_DATA_LOSS_RATE - 0.01)));
    }

    #[test]
    fn reclaim_is_armed_by_either_the_latch_or_the_queue_gate() {
        assert!(!reclaim_armed(&idle()));
        let mut latched = idle();
        latched.reclaiming = true;
        assert!(reclaim_armed(&latched));
        let mut queued = idle();
        queued.queue_delay = Some(Duration::from_millis(7));
        queued.tolerance = Some(Duration::from_millis(6));
        assert!(reclaim_armed(&queued));
        // Equal queue delay and tolerance is not yet a breach.
        let mut equal = idle();
        equal.queue_delay = Some(Duration::from_millis(6));
        equal.tolerance = Some(Duration::from_millis(6));
        assert!(!reclaim_armed(&equal));
    }
}
