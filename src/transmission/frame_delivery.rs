//! Per-connection frame-delivery mode (`FrameDelivery`).
//!
//! Stock rtp exposes a strict in-order byte stream: the receive queue is a
//! `SeqQueue` keyed by packet sequence, and a single lost packet stalls every
//! later packet behind it (head-of-line blocking).  On a lossy link an
//! interactive stream multiplexed over the connection with bulk traffic pays
//! the bulk's reassembly latency on every one of its own frames.
//!
//! When `FrameDelivery { enabled: true }` is passed to both peers, the
//! connection switches to a *frame-aware* mode:
//!
//! - The sender marks application frame boundaries on the wire (only the
//!   first packet of a frame carries a `FRAME_DATA_TS` codec command with the
//!   total frame length; continuation packets use the stock `DATA_TS`).
//! - ARQ / SACK / congestion control are completely untouched — every packet
//!   is still repaired, and the ACK/SACK queue is independent of the early-
//!   delivery path.
//! - The receiver may deliver any *complete* frame out of order, past
//!   sequence holes, by scanning the receive queue for a frame-start slot and
//!   walking contiguous data slots until the frame length is satisfied.  Slots
//!   consumed by an early-delivered frame become `Tombstone`s that still count
//!   toward the receive window so a persistent hole cannot unbounded memory.
//!
//! Default off is byte-for-byte stock: the wire bytes, delivery order, and hot-
//! path allocations are unchanged, and the `FRAME_DATA_TS` command is never
//! emitted.
//!
//! # Both peers must enable together
//!
//! There is no in-band negotiation — same coupling as the FEC flag.  Both
//! peers must pass `FrameDelivery { enabled: true }` to the matching
//! `*_with_mss_fec_tuning_and_frame_delivery` constructor; a mismatch produces
//! a framing desync.  The env var `RTP_FRAME_DELIVERY=1` only feeds the default
//! for A/B comparison — it is never read as the live setting, so it cannot
//! silently apply to every connection in the process.

/// Per-connection frame-delivery configuration.
///
/// `Default` is `enabled: false` — stock byte-stream behaviour, byte-for-byte.
/// Pass `FrameDelivery { enabled: true }` to both peers via the
/// `*_with_mss_fec_tuning_and_frame_delivery` constructor family to switch the
/// connection into frame-delivery mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FrameDelivery {
    pub enabled: bool,
}

impl FrameDelivery {
    pub const fn enabled() -> Self {
        Self { enabled: true }
    }
}

/// Read `RTP_FRAME_DELIVERY` once at process startup to feed the *default*
/// frame-delivery mode for A/B comparison.  `1`/`true` selects
/// `FrameDelivery::enabled()`; anything else (including unset) selects
/// `FrameDelivery::default()`.  This is **not** the live configuration — the
/// real setting is the per-connection `FrameDelivery` argument threaded
/// through the `*_with_mss_fec_tuning_and_frame_delivery` APIs, so env-var
/// state can never silently apply to every connection in the process.
pub fn frame_delivery_from_env() -> FrameDelivery {
    match std::env::var("RTP_FRAME_DELIVERY") {
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true") => FrameDelivery::enabled(),
        _ => FrameDelivery::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_disabled() {
        let f = FrameDelivery::default();
        assert!(!f.enabled);
    }

    #[test]
    fn enabled_preset() {
        let f = FrameDelivery::enabled();
        assert!(f.enabled);
    }
}
