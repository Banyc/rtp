//! Compatibility shim: the sender-side FEC condition gate now lives in
//! [`super::fec::gate::loss_gate`].  Kept so the existing
//! `redundancy::fec_gate::…` paths keep resolving.

pub(crate) use super::fec::gate::{FecConditionGate, FecGateDecision, FecLossGateThresholds};
