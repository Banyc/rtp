//! Compatibility shim: per-connection FEC tuning now lives in
//! [`super::gate::tuning`].  Kept so the existing
//! `redundancy::fec_tuning::…` paths keep resolving.

pub use super::gate::{FecTuning, fec_tuning_from_env};
