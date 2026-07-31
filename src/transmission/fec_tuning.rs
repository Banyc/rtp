//! Per-connection FEC tuning ('FecTuning').
//! `interactive_parity_depth_for_message` helper.
//!
//! Stock rtp FEC uses a fixed 1:4 data-to-parity ratio and is gated by the
//! spare-token budget so parity never competes with data bandwidth.  On a
//! wide-MSS connection (e.g. MSS 8192 over loopback / jumbo frames) a small
//! interactive message encodes as a **single** data symbol, so the stock
//! depth-1 parity is literally one extra independent loss/delay draw for the
//! whole message — recovery is no better than a retransmit.  Allowing a
//! deeper parity (depth 3) for those single-symbol groups buys tail-latency
//! resilience for the interactive path without touching bulk traffic, which
//! always spans many symbols and keeps the budget gate.
//!
//! The real configuration is the per-connection `FecTuning` argument threaded
//! through the `*_with_mss_and_fec_tuning` connect/accept APIs.  The env var
//! `RTP_MINDIV=1` only feeds the default for A/B comparison — it is never
//! read as the live setting, so it cannot silently apply to every connection
//! in the process.
//!
//! # Both peers must agree
//!
//! MSS and the FEC flag are negotiated out-of-band today; the FEC tuning is
//! the same — there is no in-band negotiation.  The large-MSS recipe targets
//! loopback / jumbo / fragmentation-tolerant paths.  Real WANs IP-fragment an
//! 8 KiB UDP datagram, and one lost fragment kills the whole symbol — which
//! inverts the benefit.  Use the default MSS for WAN paths.

/// Per-connection FEC tuning.
///
/// - `instream_flush`: when `true`, the transmission layer force-flushes the
///   open FEC data group at the end of every data send burst (after the last
///   data symbol) instead of waiting for the stock `can_send_tail_fec` gate.
///   ACK/kill bursts keep the stock gate regardless — only data bursts are
///   force-flushed.  This is what lets a single-symbol interactive message
///   emit its parity promptly rather than being skipped at the burst end.
/// - `interactive_parity_depth`: the parity depth requested for groups that
///   encode as exactly one data symbol.  Multi-symbol groups always keep the
///   stock budget gate regardless of this value (ungated depth > 1 on bulk
///   would add ~75% overhead and defeat the point).
///
/// `Default` is `(false, 1)` — stock behaviour, byte-for-byte.  The `mindiv`
/// preset is `(true, 3)` — the recommended setting for interactive traffic on
/// a large-MSS path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FecTuning {
    pub instream_flush: bool,
    pub interactive_parity_depth: u8,
}

impl Default for FecTuning {
    fn default() -> Self {
        Self {
            instream_flush: false,
            interactive_parity_depth: 1,
        }
    }
}

impl FecTuning {
    /// Maximum-diversity preset for interactive traffic on a large-MSS path:
    /// force-flush each data burst and emit 3 parity copies for the trailing
    /// single-symbol group.  Use this only when both peers agree on MSS and
    /// the path tolerates the datagram size (loopback / jumbo / no IP
    /// fragmentation).
    pub const fn mindiv() -> Self {
        Self {
            instream_flush: true,
            interactive_parity_depth: 3,
        }
    }
}

/// Read `RTP_MINDIV` once at process startup to feed the *default* FEC
/// tuning for A/B comparison.  `1`/`true` selects `FecTuning::mindiv()`;
/// anything else (including unset) selects `FecTuning::default()`.  This is
/// **not** the live configuration — the real setting is the per-connection
/// `FecTuning` argument threaded through the `*_with_mss_and_fec_tuning`
/// APIs, so env-var state can never silently apply to every connection in
/// the process.
pub fn fec_tuning_from_env() -> FecTuning {
    match std::env::var("RTP_MINDIV") {
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true") => FecTuning::mindiv(),
        _ => FecTuning::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_stock() {
        let t = FecTuning::default();
        assert!(!t.instream_flush);
        assert_eq!(t.interactive_parity_depth, 1);
    }

    #[test]
    fn mindiv_preset() {
        let t = FecTuning::mindiv();
        assert!(t.instream_flush);
        assert_eq!(t.interactive_parity_depth, 3);
    }
}
