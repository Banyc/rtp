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
