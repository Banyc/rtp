//! Sender-side FEC capacity gate and its per-connection tuning.
//!
//! This is the single home for the capacity-gate policy: whether recent
//! loss/recovery evidence warrants parity at all (the stock and interactive
//! loss-gate presets), whether there is genuinely spare capacity to spend on
//! parity (the no-spare-capacity deferral decision), and the per-connection
//! `FecTuning` that selects a preset — including its env-derived default,
//! sampled once per process.
//!
//! The gate *decision* lives here; the deferral enactment and the encoder's
//! parity budget live with the FEC state they mutate.

mod loss_gate;
mod tuning;

pub(crate) use loss_gate::{FecConditionGate, FecGateDecision, FecLossGateThresholds};
pub use tuning::{FecTuning, fec_tuning_from_env};
