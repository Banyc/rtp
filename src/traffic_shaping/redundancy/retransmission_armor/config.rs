//! Immutable per-session retransmission-armor configuration.
//!
//! The duplicate-copy toggle is sampled once per process from `RTP_RTX_DUP`
//! (the connect/accept config `Default` reads it through a process-wide cache)
//! and is fixed for the life of each session.  It gates the duplicate of a
//! *recovery* send only; a fresh interactive single-symbol tail is duplicated
//! independently, driven by the connection's force-flush FEC tuning (see the
//! retransmission-armor policy).

use std::sync::LazyLock;

/// Environment variable that enables retransmission-armor duplicate copies.
pub const RETRANSMISSION_ARMOR_ENV: &str = "RTP_RTX_DUP";

/// Immutable retransmission-armor configuration for one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetransmissionArmorConfig {
    enabled: bool,
}

impl RetransmissionArmorConfig {
    pub const fn enabled() -> Self {
        Self { enabled: true }
    }

    pub const fn disabled() -> Self {
        Self { enabled: false }
    }

    pub const fn is_enabled(self) -> bool {
        self.enabled
    }
}

/// `RTP_RTX_DUP` sampled once per process: the connect/accept config `Default`
/// reads it through this cache, so a config constructed later cannot observe a
/// mid-run environment mutation.
static ENV_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    std::env::var(RETRANSMISSION_ARMOR_ENV)
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
});

impl Default for RetransmissionArmorConfig {
    fn default() -> Self {
        Self {
            enabled: *ENV_ENABLED,
        }
    }
}

impl From<bool> for RetransmissionArmorConfig {
    fn from(enabled: bool) -> Self {
        Self { enabled }
    }
}
