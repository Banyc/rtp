//! Per-connection FEC tuning (`FecTuning`) and the
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
//! `RTP_F5_MINDIV=1` only feeds the default for A/B comparison — it is never
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

use fec::proto::{data_mss as fec_data_mss, symbol_size as fec_symbol_size};

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

/// Decide the effective parity depth for a single outgoing application
/// message of `message_len` bytes given the configured `requested_depth` and
/// the connection MSS.
///
/// Returns `requested_depth` when the message encodes as a single FEC data
/// symbol (i.e. fits in one codec payload after the codec and FEC header
/// overhead), and `1` otherwise.  Multi-symbol messages always keep the
/// stock depth — ungated depth > 1 on bulk would add ~75% overhead and
/// defeat the point of the single-symbol special case.
///
/// `mss` is the **raw** connection MSS (the value passed to `*_with_mss`),
/// not the post-FEC data MSS.  The codec payload a single symbol can carry is
/// `data_mss(mss) - codec::data_overhead()`.
///
/// Documented at the connect/accept call sites; both peers must agree on the
/// depth they pass.
pub fn interactive_parity_depth_for_message(
    mss: usize,
    message_len: usize,
    requested_depth: u8,
) -> u8 {
    if requested_depth <= 1 {
        return 1;
    }
    // The codec payload that fits in one FEC data symbol at this MSS.
    let single_symbol_payload = match single_symbol_payload(mss) {
        Some(p) => p,
        None => return 1,
    };
    if message_len <= single_symbol_payload {
        requested_depth
    } else {
        1
    }
}

/// Maximum application bytes that fit in a single FEC data symbol at `mss`,
/// or `None` if `mss` is too small for the FEC + codec headers.  A message of
/// at most this many bytes encodes as one data symbol and so qualifies for the
/// interactive parity depth.  Exposed for tests and the call-site helper.
pub fn single_symbol_payload(mss: usize) -> Option<usize> {
    // FEC header (11) + data-symbol header (2) are stripped by `data_mss`;
    // the codec payload header (`data_overhead`) is stripped here.
    let _ = fec_symbol_size(mss)?;
    let data_mss = fec_data_mss(mss)?;
    Some(data_mss.saturating_sub(crate::codec::data_overhead()))
}

/// Read `RTP_F5_MINDIV` once at process startup to feed the *default* FEC
/// tuning for A/B comparison.  `1`/`true` selects `FecTuning::mindiv()`;
/// anything else (including unset) selects `FecTuning::default()`.  This is
/// **not** the live configuration — the real setting is the per-connection
/// `FecTuning` argument threaded through the `*_with_mss_and_fec_tuning`
/// APIs, so env-var state can never silently apply to every connection in
/// the process.
pub fn fec_tuning_from_env() -> FecTuning {
    match std::env::var("RTP_F5_MINDIV") {
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

    #[test]
    fn depth_for_message_returns_requested_for_single_symbol() {
        // MSS 8192 → single-symbol payload is much larger than a 4 KiB
        // interactive message, so the requested depth is honoured.
        let mss = 8192;
        let p = single_symbol_payload(mss).unwrap();
        assert!(p >= 4096, "single-symbol payload at mss 8192 is {p}");
        assert_eq!(
            interactive_parity_depth_for_message(mss, 4096, 3),
            3,
            "a 4 KiB message at MSS 8192 is a single symbol → depth 3"
        );
    }

    #[test]
    fn depth_for_message_collapses_to_one_for_multi_symbol() {
        // At MSS 8192 the single-symbol payload is ~8175 bytes; a 16 KiB
        // message spans multiple symbols and must keep depth 1.
        let mss = 8192;
        let p = single_symbol_payload(mss).unwrap();
        let multi = p + 1;
        assert_eq!(
            interactive_parity_depth_for_message(mss, multi, 3),
            1,
            "a multi-symbol message must keep the stock depth 1"
        );
    }

    #[test]
    fn depth_for_message_collapses_to_one_when_requested_is_one() {
        // requested_depth == 1 is the stock path; never inflates.
        assert_eq!(interactive_parity_depth_for_message(8192, 1, 1), 1);
        assert_eq!(interactive_parity_depth_for_message(8192, 4096, 1), 1);
    }

    #[test]
    fn depth_for_message_at_default_mss_classifies_correctly() {
        // At the default NO_FEC_MSS (1424) a 4 KiB message is many symbols.
        let mss = crate::udp::NO_FEC_MSS;
        let p = single_symbol_payload(mss).unwrap();
        assert!(p < 4096, "single-symbol payload at default MSS is {p}");
        assert_eq!(
            interactive_parity_depth_for_message(mss, 4096, 3),
            1,
            "4 KiB at default MSS is multi-symbol → depth 1"
        );
        // A tiny message at the default MSS is a single symbol → depth 3.
        assert_eq!(
            interactive_parity_depth_for_message(mss, 16, 3),
            3,
            "16 bytes at default MSS is a single symbol → depth 3"
        );
    }

    #[test]
    fn depth_for_message_tiny_mss_returns_one() {
        // An MSS too small for the FEC header yields depth 1 regardless.
        assert_eq!(interactive_parity_depth_for_message(8, 1, 3), 1);
    }

    #[test]
    fn depth_for_message_boundary_exact_payload_is_single_symbol() {
        // A message whose length is exactly the single-symbol payload still
        // encodes as one symbol (the comparison is inclusive), so the
        // requested depth is honoured.  Misclassifying this boundary (using
        // `<` instead of `<=`) returns depth 1 — a regression.
        let mss = 8192;
        let p = single_symbol_payload(mss).unwrap();
        assert_eq!(
            interactive_parity_depth_for_message(mss, p, 3),
            3,
            "a message of exactly the single-symbol payload must be a single symbol → depth 3"
        );
        // One byte over the boundary must be multi-symbol → depth 1.
        assert_eq!(
            interactive_parity_depth_for_message(mss, p + 1, 3),
            1,
            "one byte over the single-symbol payload must be multi-symbol → depth 1"
        );
    }
}