mod config;
mod policy;

pub use config::RetransmissionArmorConfig;
pub(crate) use policy::{ArmorDecision, RetransmissionArmor};
