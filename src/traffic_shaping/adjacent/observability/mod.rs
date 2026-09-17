//! Observability: the typed observation schema and the per-connection sink
//! that stamps and delivers it.
//!
//! [`schema`] owns the observation contract — the event/snapshot types, the
//! FEC and retransmission counters, and the schema version with its
//! append-only compatibility rule.  [`sink`] owns delivery: the callback and
//! the fixed-width CSV trace row, both stamped with the schema version.
pub mod schema;
pub(crate) mod sink;
