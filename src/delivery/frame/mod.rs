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
//! - The receiver reassembles frames and, by default, delivers them in order,
//!   gap-free, at the in-order cursor: a complete frame past an unrepaired
//!   sequence hole is withheld until the hole fills (retransmission or
//!   reordering), preserving the strict ordering guarantee of stock rtp. A
//!   frame whose next continuation is permanently captured (a hostile peer's
//!   overlapping frame) is abandoned so the cursor never wedges on it.
//! - Opt-in ([`FrameMode::allow_reorder`]) receiver-side fast-forward: a
//!   complete frame that starts past an unrepaired hole may be delivered
//!   immediately while the in-order cursor (`next`) stays pinned at the hole.
//!   The frame's sequence numbers are tombstoned exactly as in the strict
//!   path, so when the hole finally fills the cursor collapses the tombstones
//!   and advances without redelivering. The flag is OFF by default; enabling
//!   it only changes which complete frames are handed up, never the ACKs sent
//!   or the wire format, so liveness is unaffected.
//!
//! # Both peers must enable together
//!
//! There is no in-band negotiation — same coupling as the FEC flag.  Both
//! peers must pass `FrameMode { enabled: true }` to the matching
//! `*_with_mss_fec_tuning_and_frame_delivery` constructor; a mismatch
//! produces a framing desync.

use std::sync::LazyLock;

pub(crate) mod recv;
pub(crate) mod send;
pub(crate) mod wire;

/// Per-connection frame-delivery configuration.
///
/// `Default` is `enabled: false` — stock byte-stream behaviour.
/// Pass `FrameMode { enabled: true }` to both peers via the
/// `*_with_mss_fec_tuning_and_frame_delivery` constructor family to
/// switch the connection into frame-delivery mode.
///
/// `allow_reorder` is a receiver-side, opt-in fast-forward: when set, a
/// complete frame starting past an unrepaired in-order hole is delivered
/// immediately instead of being withheld behind the hole. `next` still pins
/// at the hole, so ordering and liveness are unchanged; only strict-ordered
/// consumers (the default) should leave it `false`. Both the strict and the
/// fast-forward path tombstone a delivered frame's sequence numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FrameMode {
    pub enabled: bool,
    /// Opt-in receiver-side fast-forward past an unrepaired hole. Requires
    /// `enabled`; has no effect otherwise. Defaults to `false`.
    pub allow_reorder: bool,
}

impl FrameMode {
    pub const fn enabled() -> Self {
        Self {
            enabled: true,
            allow_reorder: false,
        }
    }

    /// Frame mode with the receiver-side fast-forward enabled. Use only when
    /// the consumer restores ordering itself (e.g. a per-stream reassembler).
    pub const fn enabled_reordering() -> Self {
        Self {
            enabled: true,
            allow_reorder: true,
        }
    }

    /// Set the fast-forward flag, preserving `enabled`.
    pub const fn with_reorder(self, allow_reorder: bool) -> Self {
        Self {
            allow_reorder,
            ..self
        }
    }
}

/// `RTP_FRAME_DELIVERY` sampled once per process for the *default*
/// frame-delivery mode.  `1`/`true` selects `FrameMode::enabled()`; anything
/// else selects `FrameMode::default()`.  The connect/accept config `Default`
/// reads it through this cache so a later construction cannot observe a
/// mid-run environment mutation.
static ENV_FRAME_DELIVERY: LazyLock<FrameMode> =
    LazyLock::new(|| match std::env::var("RTP_FRAME_DELIVERY") {
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true") => FrameMode::enabled(),
        _ => FrameMode::default(),
    });

/// The *default* frame-delivery mode derived from `RTP_FRAME_DELIVERY`,
/// sampled once per process (see `ENV_FRAME_DELIVERY`).
pub fn frame_delivery_from_env() -> FrameMode {
    *ENV_FRAME_DELIVERY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_disabled() {
        let f = FrameMode::default();
        assert!(!f.enabled);
        assert!(!f.allow_reorder, "fast-forward must be opt-in");
    }

    #[test]
    fn enabled_preset() {
        let f = FrameMode::enabled();
        assert!(f.enabled);
        assert!(
            !f.allow_reorder,
            "FrameMode::enabled must preserve the strict ordering contract"
        );
    }

    #[test]
    fn enabled_reordering_is_opt_in() {
        let f = FrameMode::enabled_reordering();
        assert!(f.enabled);
        assert!(f.allow_reorder);
        assert!(FrameMode::enabled().with_reorder(true).allow_reorder);
        assert!(
            !FrameMode::enabled_reordering()
                .with_reorder(false)
                .allow_reorder
        );
    }
}
