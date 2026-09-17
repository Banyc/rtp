//! Reorder-lane-only delay-gate policy.
//!
//! When reordering is expected, an isolated low-RTT echo (a reordered packet
//! carrying a stale send timestamp) must not be read as the path baseline, and
//! a self-inflicted queue must not inflate the gate's own jitter tolerance.
//! The raw estimator state stays in
//! [`RttStats`](crate::traffic_shaping::recovery::rtt_stats::RttStats) — the
//! same filter feeds the RTO — and this module owns only the *policy* over
//! that state.

pub(crate) mod gate_variance;
