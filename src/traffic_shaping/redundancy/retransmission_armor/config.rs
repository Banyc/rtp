//! Immutable per-session retransmission-armor configuration.
//!
//! The duplicate-copy toggle is fixed once at connection construction (the
//! connect/accept config `Default` reads `RTP_RTX_DUP` exactly once) and is
//! never changed for the life of the session.

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

impl Default for RetransmissionArmorConfig {
    fn default() -> Self {
        let enabled = std::env::var(RETRANSMISSION_ARMOR_ENV)
            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
        Self { enabled }
    }
}

impl From<bool> for RetransmissionArmorConfig {
    fn from(enabled: bool) -> Self {
        Self { enabled }
    }
}
