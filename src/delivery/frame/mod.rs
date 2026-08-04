//! Per-connection frame-delivery mode (`FrameMode`).
//!
//! Stock rtp exposes a strict in-order byte stream.  When
//! `FrameMode { enabled: true }` is passed to both peers, the
//! connection switches to a *frame-aware* mode:
//!
//! - The sender marks application frame boundaries on the wire (first
//!   packet of a frame carries a `FRAME_DATA_TS` codec command with the
//!   total frame length; continuation packets use `DATA_TS`).
//! - ARQ / SACK / congestion control are untouched.
//! - The receiver may deliver any *complete* frame out of order, past
//!   sequence holes, by scanning the receive queue.
//!
//! # Both peers must enable together
//!
//! There is no in-band negotiation — same coupling as the FEC flag.  Both
//! peers must pass `FrameMode { enabled: true }` to the matching
//! `*_with_mss_fec_tuning_and_frame_delivery` constructor; a mismatch
//! produces a framing desync.

pub(crate) mod recv;
pub(crate) mod send;
pub(crate) mod wire;

/// Per-connection frame-delivery configuration.
///
/// `Default` is `enabled: false` — stock byte-stream behaviour.
/// Pass `FrameMode { enabled: true }` to both peers via the
/// `*_with_mss_fec_tuning_and_frame_delivery` constructor family to
/// switch the connection into frame-delivery mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FrameMode {
    pub enabled: bool,
}

impl FrameMode {
    pub const fn enabled() -> Self {
        Self { enabled: true }
    }
}

/// Read `RTP_FRAME_DELIVERY` once at process startup for the *default*
/// frame-delivery mode.  `1`/`true` selects `FrameMode::enabled()`;
/// anything else selects `FrameMode::default()`.
pub fn frame_delivery_from_env() -> FrameMode {
    match std::env::var("RTP_FRAME_DELIVERY") {
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true") => FrameMode::enabled(),
        _ => FrameMode::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_disabled() {
        let f = FrameMode::default();
        assert!(!f.enabled);
    }

    #[test]
    fn enabled_preset() {
        let f = FrameMode::enabled();
        assert!(f.enabled);
    }
}
