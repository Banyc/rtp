//! Compatibility shim: the sender-side FEC condition gate now lives in
//! [`super::gate::loss_gate`].  Kept so the existing
//! `redundancy::fec_gate::…` paths keep resolving.

pub(crate) use super::gate::{FecConditionGate, FecGateDecision, FecLossGateThresholds};
