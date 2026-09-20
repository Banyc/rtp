//! rtp layer-testing kit: the echo/connect/sink scaffolding, frame-delivery
//! plumbing, and the metrics perf-trace builder used by the rtp scenario
//! suites, compiled behind the `testing` feature.
//!
//! This is the owning crate's half of the shared scenario scaffolding: the
//! generic helpers (payload, task scopes, reporting) live in the
//! `netem-test` harness kit (`netem_test::kit`, behind its `test-kit`
//! feature) and are consumed here; the mux-layer helpers live in the `mux`
//! kit. Imports only ever go downward (kit → harness), so no layer depends
//! on a sibling's kit.

pub mod frame;
pub mod perf_trace;
pub mod rtp;
