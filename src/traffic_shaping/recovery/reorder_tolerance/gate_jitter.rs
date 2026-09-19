//! Lane-aware jitter evidence for the delay gate.

use crate::traffic_shaping::recovery::pkt_send_space::PktSendSpace;
use crate::traffic_shaping::recovery::rtt_stats::GateJitter;

/// Select the jitter evidence that feeds the delay gate for this lane.
///
/// The delay gate's jitter margin discounts the downward half of the RTT
/// variance on the reorder-tolerant lane, and tracks the windowed
/// steady-state jitter floor rather than the queue-inflated trending variance.
/// The stock/bulk lane keeps the two-sided variance unchanged (both estimates
/// equal).
pub(crate) fn select_gate_jitter(
    pkt_send_space: &PktSendSpace,
    reorder_tolerant: bool,
) -> GateJitter {
    if reorder_tolerant {
        pkt_send_space.gate_jitter()
    } else {
        GateJitter::uniform(pkt_send_space.smooth_rtt_var())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::select_gate_jitter;
    use crate::traffic_shaping::recovery::pkt_send_space::PktSendSpace;
    use crate::traffic_shaping::recovery::rtt_stats::GATE_VAR_MATURE_SAMPLES;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// A space whose lane-specific jitter pair diverges from the raw two-sided
    /// variance: a high-RTT plateau followed by a long downward drift inflates
    /// the two-sided `smooth_rtt_var` while the one-sided upward EWMA stays
    /// near zero, so `trending` (and the windowed `steady`) fall below it.
    fn space_with_diverging_estimates() -> PktSendSpace {
        let mut space = PktSendSpace::new();
        let mut now = Instant::now();
        for _ in 0..20 {
            space.sample_rtt(ms(200), now);
            now += ms(1);
        }
        for step in 0..GATE_VAR_MATURE_SAMPLES + 32 {
            let rtt = 200u64.saturating_sub(step as u64 * 4).max(20);
            space.sample_rtt(ms(rtt), now);
            now += ms(1);
        }
        space
    }

    /// The selector must hand the reorder-tolerant lane the lane-specific
    /// jitter pair (the queue-immune `steady` and trending estimates) and every
    /// other lane the uniform raw-variance margin.  The two are only
    /// indistinguishable when the estimates happen to coincide, so a test that
    /// exercises `QueueGrowth` with `GateJitter::uniform` cannot pin the
    /// lane-to-estimator mapping; this does, at the real selector.
    #[test]
    fn the_reorder_lane_selects_the_lane_specific_jitter_and_stock_the_uniform_margin() {
        let space = space_with_diverging_estimates();
        let lane = space.gate_jitter();
        let var = space.smooth_rtt_var();
        assert!(
            lane.steady != var || lane.trending != var,
            "the scenario must make the lane-specific estimates differ from the raw variance"
        );

        let reorder = select_gate_jitter(&space, true);
        assert_eq!(
            reorder.steady, lane.steady,
            "the reorder lane must use the queue-immune steady estimate"
        );
        assert_eq!(
            reorder.trending, lane.trending,
            "the reorder lane must use the trending estimate"
        );

        let stock = select_gate_jitter(&space, false);
        assert_eq!(
            stock.steady, var,
            "the stock lane must use the uniform raw-variance margin"
        );
        assert_eq!(
            stock.trending, var,
            "the stock lane must use the uniform raw-variance margin"
        );
    }
}
