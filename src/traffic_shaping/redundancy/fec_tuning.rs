//! Per-connection FEC tuning ('FecTuning').
//! `small_group_parity_count_for_message` helper.
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
//! `RTP_MAX_DIVERSITY=1` only feeds the default for A/B comparison — it is
//! never read as the live setting, so it cannot silently apply to every
//! connection in the process.
//!
//! # Both peers must agree
//!
//! MSS and the FEC flag are negotiated out-of-band today; the FEC tuning is
//! the same — there is no in-band negotiation.  The large-MSS recipe targets
//! loopback / jumbo / fragmentation-tolerant paths.  Real WANs IP-fragment an
//! 8 KiB UDP datagram, and one lost fragment kills the whole symbol — which
//! inverts the benefit.  Use the default MSS for WAN paths.

use super::fec_gate::FecLossGateThresholds;

/// Per-connection FEC tuning.
///
/// - `instream_flush`: when `true`, the transmission layer requests a prompt
///   data-burst tail flush for the open FEC data group at the end of every
///   data send burst (after the last data symbol) instead of waiting for the
///   stock `can_send_tail_fec` gate.  This is a *policy request* only — it
///   cannot override the sender-side condition gate: parity is still emitted
///   only when recent loss/recovery evidence warrants recovery (the loss gate
///   is open) and the capacity is genuinely spare (no queued/waiting
///   application work, no queue growth, no cwnd pressure, no retransmission,
///   no pending tail probe).  ACK/kill bursts retain the stock tail request
///   regardless of this flag — only data bursts are force-requested.
/// - `small_group_parity_count`: the parity depth requested for groups that
///   encode as exactly one data symbol.  Multi-symbol groups always keep the
///   stock budget gate regardless of this value (ungated depth > 1 on bulk
///   would add ~75% overhead and defeat the point).  This tuning bypasses
///   only the encoder's token-share check — and only after the
///   transmission-layer condition gate (loss, spare capacity, tail request)
///   has already succeeded — so a single-symbol interactive message emits its
///   deeper parity promptly without ever competing with data traffic.
///
/// `Default` is `(false, 1)` — stock behaviour, byte-for-byte.  The
/// `interactive_prompt` preset is `(true, 1)` and `max_diversity` is
/// `(true, 3)` — the recommended settings for interactive traffic on a
/// large-MSS path.  Both force-flush presets also select the permissive
/// loss gate (see [`FecTuning::loss_gate_thresholds`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FecTuning {
    pub instream_flush: bool,
    pub small_group_parity_count: u8,
}

impl Default for FecTuning {
    fn default() -> Self {
        Self {
            instream_flush: false,
            small_group_parity_count: 1,
        }
    }
}

impl FecTuning {
    /// Maximum-diversity preset for interactive traffic on a large-MSS path:
    /// force-flush each data burst and emit 3 parity copies for the trailing
    /// single-symbol group.  Use this only when both peers agree on MSS and
    /// the path tolerates the datagram size (loopback / jumbo / no IP
    /// fragmentation).
    pub const fn max_diversity() -> Self {
        Self {
            instream_flush: true,
            small_group_parity_count: 3,
        }
    }

    /// Prompt-parity preset for interactive traffic: force-flush each data
    /// burst's open FEC group at the burst tail, with a single parity symbol,
    /// and use the permissive loss gate so realistic WAN loss (~1–2%) opens
    /// parity.  This is the recommended tuning for a low-rate ping lane; it
    /// does not change the stock `Default` gate for bulk/agnostic traffic.
    pub const fn interactive_prompt() -> Self {
        Self {
            instream_flush: true,
            small_group_parity_count: 1,
        }
    }

    /// Loss-gate sensitivity for this tuning.
    ///
    /// The interactive presets force-flush each burst tail
    /// ([`Self::instream_flush`] is `true`) and use the permissive gate, so a
    /// low-rate interactive lane opens parity at 1% loss with only 8 primary
    /// sends.  The stock `Default` keeps the 5% gate and the 16-sample
    /// minimum, so bulk/agnostic traffic is byte-for-byte unchanged.
    pub(crate) const fn loss_gate_thresholds(self) -> FecLossGateThresholds {
        if self.instream_flush {
            FecLossGateThresholds::INTERACTIVE
        } else {
            FecLossGateThresholds::STOCK
        }
    }
}

/// Read `RTP_MAX_DIVERSITY` (falling back to the legacy `RTP_MINDIV` for one
/// release) once at process startup to feed the *default* FEC tuning for A/B
/// comparison.  `1`/`true` selects `FecTuning::max_diversity()`; anything
/// else (including unset) selects `FecTuning::default()`.  This is
/// **not** the live configuration — the real setting is the per-connection
/// `FecTuning` argument threaded through the `*_with_mss_and_fec_tuning`
/// APIs, so env-var state can never silently apply to every connection in
/// the process.
pub fn fec_tuning_from_env() -> FecTuning {
    match std::env::var("RTP_MAX_DIVERSITY").or_else(|_| std::env::var("RTP_MINDIV")) {
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true") => FecTuning::max_diversity(),
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
        assert_eq!(t.small_group_parity_count, 1);
    }

    #[test]
    fn max_diversity_preset() {
        let t = FecTuning::max_diversity();
        assert!(t.instream_flush);
        assert_eq!(t.small_group_parity_count, 3);
    }

    /// The prompt-parity preset is exactly the interactive lane's tuning: it
    /// force-flushes the burst tail with a single parity symbol and selects
    /// the permissive loss gate.  The stock default keeps the 5% gate.
    #[test]
    fn interactive_prompt_selects_the_permissive_gate() {
        let t = FecTuning::interactive_prompt();
        assert!(t.instream_flush);
        assert_eq!(t.small_group_parity_count, 1);
        assert_eq!(
            t.loss_gate_thresholds(),
            FecLossGateThresholds::INTERACTIVE,
            "the interactive preset must use the permissive loss gate"
        );
        assert_eq!(
            FecTuning::max_diversity().loss_gate_thresholds(),
            FecLossGateThresholds::INTERACTIVE,
            "every force-flush preset is interactive and uses the permissive gate"
        );
        assert_eq!(
            FecTuning::default().loss_gate_thresholds(),
            FecLossGateThresholds::STOCK,
            "the stock default must keep the 5% loss gate"
        );
    }
}
