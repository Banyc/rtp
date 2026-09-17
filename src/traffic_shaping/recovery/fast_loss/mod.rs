//! Fast-loss arming policy.
//!
//! The evidence-gated fast-loss path only declares loss when the path is
//! structurally low-jitter.  The srtt-relative half of that gate lives in the
//! RTO filter
//! ([`RtxTimer`](crate::traffic_shaping::recovery::rto::RtxTimer)); this module
//! owns the queue-independent complement: arming from the lifetime minimum RTT.

use std::time::Duration;

/// Queue-independent fast-loss arming: the smoothed RTT variation is below the
/// lifetime minimum RTT — the propagation floor recorded while the path was
/// still uncongested — so a SACK gap is evidence of loss rather than
/// jitter-driven reordering.
///
/// The srtt-relative gate (`K*rttvar < srtt/4`) is disarmed exactly when a bulk
/// sender fills the bottleneck queue, because queueing inflates both `srtt` and
/// `rttvar`.  The lifetime `min_rtt` does not inflate with queue depth (it is a
/// minimum over samples, including the uncongested ones), so this gate keeps
/// the evidence-gated fast-loss path armed under the bulk + loss conditions
/// where repair latency matters most.  `false` before any sample exists (the
/// gate abstains; the caller keeps the structural gate).
pub(crate) fn armed_against_min_rtt(min_rtt: Option<Duration>, smooth_rtt_var: Duration) -> bool {
    min_rtt.is_some_and(|min_rtt| smooth_rtt_var < min_rtt)
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
        assert!(!armed_against_min_rtt(None, ms(1)));
    }

    #[test]
    fn arms_below_the_propagation_floor_and_disarms_above_it() {
        assert!(armed_against_min_rtt(Some(ms(30)), ms(16)));
        assert!(!armed_against_min_rtt(Some(ms(30)), ms(30)));
        assert!(!armed_against_min_rtt(Some(ms(30)), ms(40)));
    }
}
