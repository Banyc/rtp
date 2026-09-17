//! Reorder-lane-only policy over the RTT estimators.
//!
//! When reordering is expected, an isolated low-RTT echo (a reordered packet
//! carrying a stale send timestamp) must not be read as the path baseline, and
//! a self-inflicted queue must not inflate the gate's own jitter tolerance.
//! The raw estimator state stays in
//! [`RttStats`](crate::traffic_shaping::recovery::rtt_stats::RttStats) — the
//! same filter feeds the RTO — and in
//! [`PktSendSpace`](crate::traffic_shaping::recovery::pkt_send_space::PktSendSpace);
//! this module owns only the *policy* each estimator accessor is selected
//! through.
//!
//! Four lane-dependent decisions live here, each a sibling policy over those
//! estimators: the delay gate's jitter evidence
//! ([`gate_variance`], [`select_gate_jitter`]), the RTT-floor bucket
//! ([`reorder_floor`]), and the probe-target cap ([`probe_cap`]).  The filter
//! that feeds them, and the lane-agnostic
//! [`GateJitter::uniform`](crate::traffic_shaping::recovery::rtt_stats::GateJitter::uniform),
//! stay with the estimator.

mod gate_jitter;
pub(crate) mod gate_variance;
mod probe_cap;
mod reorder_floor;

pub(crate) use gate_jitter::select_gate_jitter;
pub(crate) use probe_cap::cap_probe_target;
pub(crate) use reorder_floor::{
    FloorBucket, RTT_MIN_BUCKET, RTT_MIN_BUCKET_RTT_SCALE, floor_bucket,
};
