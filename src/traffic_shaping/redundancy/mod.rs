pub(crate) mod fec;
pub(crate) mod retransmission_armor;
pub use fec::gate::{FecTuning, fec_tuning_from_env};
pub(crate) use retransmission_armor::{ArmorDecision, RetransmissionArmor};
pub use retransmission_armor::{RETRANSMISSION_ARMOR_ENV, RetransmissionArmorConfig};
pub(crate) fn instream_group_fec_from_env() -> bool {
    *ENV_INSTREAM_GROUP_FEC
}

/// `RTP_INSTREAM_GROUP_FEC` sampled once per process: the connect/accept config
/// `Default` reads it through this cache so a later construction cannot observe
/// a mid-run environment mutation.
static ENV_INSTREAM_GROUP_FEC: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| match std::env::var("RTP_INSTREAM_GROUP_FEC") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    });
