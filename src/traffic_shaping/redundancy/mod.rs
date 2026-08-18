pub(crate) mod fec;
pub mod fec_tuning;
mod retransmission_armor;
pub use fec_tuning::{FecTuning, fec_tuning_from_env};
pub(crate) use retransmission_armor::{ArmorDecision, RetransmissionArmor};
pub use retransmission_armor::{RETRANSMISSION_ARMOR_ENV, RetransmissionArmorConfig};
pub(crate) fn instream_group_fec_from_env() -> bool {
    match std::env::var("RTP_INSTREAM_GROUP_FEC") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
}
