//! Fast-loss arming policy.
//!
//! The evidence-gated fast-loss path only declares loss when the path is
//! structurally low-jitter.  The srtt-relative half of that gate lives in the
//! RTO filter
//! ([`RtxTimer`](crate::traffic_shaping::recovery::rto::RtxTimer)); this module
//! owns the queue-independent complement: arming from the lifetime minimum RTT.

use std::time::Duration;

use crate::traffic_shaping::core::queue_growth::{
    QUEUE_RTT_FACTOR, QUEUE_RTT_FLOOR, QUEUE_TOL_RTT_FRACTION,
};

/// The jitter tolerance the queue-gate machinery uses to tell a standing
/// queue from the path's own jitter: `2 * rttvar`, never below 5 ms, and
/// never below an eighth of the propagation floor (the floor-scaled term the
/// delay gate applies too).  Reused here so the fast-loss rescue and the
/// congestion controller's queue verdict share one definition of "srtt is
/// elevated by queueing, not by jitter": the rescue must stay reachable at
/// the queue the delay gate's drain actually permits.
fn queue_jitter_margin(floor: Duration, smooth_rtt_var: Duration) -> Duration {
    smooth_rtt_var
        .mul_f64(QUEUE_RTT_FACTOR)
        .max(QUEUE_RTT_FLOOR)
        .max(floor.mul_f64(QUEUE_TOL_RTT_FRACTION))
}

/// Queue-premised fast-loss arming: the smoothed RTT variation is below the
/// lifetime minimum RTT — the propagation floor recorded while the path was
/// still uncongested — *and* the smoothed RTT is elevated above that floor by
/// more than the path's own jitter tolerance, i.e. a standing queue exists.
///
/// The srtt-relative gate (`K*rttvar < srtt/4`) is disarmed exactly when a
/// bulk sender fills the bottleneck queue AND when the path is genuinely
/// jittery.  The gate's disarm must only be overridden in the first world,
/// where a SACK gap is loss evidence: queueing inflates `srtt` and `rttvar`
/// together, so `srtt` clears the propagation floor by at least the jitter
/// tolerance.  In the second world — jitter with no queue — `srtt` sits
/// within the jitter tolerance of the floor, and a disarmed srtt-relative
/// gate is reordering evidence, not queueing: the SACK gap is more plausibly
/// a late arrival than a drop, so fast loss stays off and the reorder-window
/// repair owns the gap.  The lifetime `min_rtt` does not inflate with queue
/// depth (it is a minimum over samples, including the uncongested ones), so
/// the premises keep the evidence-gated fast-loss path armed under the bulk +
/// loss conditions where repair latency matters most.  `false` before any
/// sample exists (the gate abstains; the caller keeps the structural gate).
pub(crate) fn armed_against_min_rtt(
    min_rtt: Option<Duration>,
    smooth_rtt: Duration,
    smooth_rtt_var: Duration,
) -> bool {
    let Some(min_rtt) = min_rtt else {
        return false;
    };
    smooth_rtt_var < min_rtt
        && smooth_rtt > min_rtt.saturating_add(queue_jitter_margin(min_rtt, smooth_rtt_var))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::armed_against_min_rtt;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn abstains_before_any_min_rtt_sample() {
        assert!(!armed_against_min_rtt(None, ms(50), ms(1)));
    }

    #[test]
    fn arms_below_the_propagation_floor_and_disarms_above_it() {
        // Queue-free jittery path: srtt sits within the jitter tolerance of
        // the floor, so a SACK gap is reordering evidence and fast loss stays
        // off even though the variation is under the floor.
        assert!(!armed_against_min_rtt(Some(ms(30)), ms(40), ms(5)));
        // Queue-inflated path: srtt clears the floor by the jitter tolerance
        // and beyond, so the rescue fires.
        assert!(armed_against_min_rtt(Some(ms(30)), ms(75), ms(16)));
        // Variation at or above the propagation floor stays disarmed.
        assert!(!armed_against_min_rtt(Some(ms(30)), ms(75), ms(30)));
        assert!(!armed_against_min_rtt(Some(ms(30)), ms(75), ms(40)));
    }

    #[test]
    fn no_queue_no_rescue_even_when_the_srtt_gate_would_have_disarmed_for_jitter() {
        // The production spurious-fast-loss shape: 5 ms one-way jitter under
        // the 50 ms propagation floor, srtt at the jitter's own mean (the
        // floor is set by the minimum sample).  The srtt-relative gate disarms
        // (K*rttvar >= srtt/4 at rttvar 4 ms), but the min-RTT rescue must
        // abstain because there is no queue above the jitter margin.
        assert!(!armed_against_min_rtt(Some(ms(41)), ms(46), ms(4)));
        // The margin is the jitter term here (2 * 4 ms), not the floor-scaled
        // one, so the boundary sits one millisecond above it.
        assert!(!armed_against_min_rtt(Some(ms(41)), ms(49), ms(4)));
        assert!(armed_against_min_rtt(Some(ms(41)), ms(50), ms(4)));
        // The same link with a 15 ms standing queue: rescue fires.
        assert!(armed_against_min_rtt(Some(ms(41)), ms(66), ms(4)));
    }
}
