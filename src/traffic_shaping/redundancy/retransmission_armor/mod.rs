mod config;
pub(crate) mod fresh_tail;
mod policy;

pub use config::{RETRANSMISSION_ARMOR_ENV, RetransmissionArmorConfig};
pub(crate) use policy::{ArmorDecision, RetransmissionArmor};
