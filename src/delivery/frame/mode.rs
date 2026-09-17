//! Per-connection frame-delivery configuration (`FrameMode`).
//!
//! Owns the `enabled` / `allow_reorder` flags, their presets, and the
//! `RTP_FRAME_DELIVERY` default.  `allow_reorder` is the receiver-side input
//! to the delivery gate in [`super::capture`]; the accept/connect entry points
//! that force frame mode on must go through [`FrameMode::with_enabled`] so
//! the receiver's opt-in survives.

use std::sync::LazyLock;

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

    /// Return this mode with `enabled` set, preserving `allow_reorder`.
    ///
    /// The accept/connect entry points (`accept_frame_delivery`,
    /// `FrameDeliveryIo::connect`) select frame delivery regardless of the
    /// config's `enabled` flag but must honour the receiver's `allow_reorder`
    /// opt-in.  They call this rather than `FrameMode::enabled()` or a struct
    /// literal, either of which silently drops the flag.
    pub const fn with_enabled(self, enabled: bool) -> Self {
        Self { enabled, ..self }
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

    #[test]
    fn with_enabled_preserves_reorder() {
        let reordering = FrameMode::enabled_reordering();
        assert!(reordering.with_enabled(true).allow_reorder);
        assert!(reordering.with_enabled(false).allow_reorder);
        assert!(!reordering.with_enabled(false).enabled);

        let strict = FrameMode::enabled();
        assert!(strict.with_enabled(true).enabled);
        assert!(!strict.with_enabled(true).allow_reorder);
    }
}
