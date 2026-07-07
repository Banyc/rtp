use std::{collections::VecDeque, fmt, num::NonZeroU64, time::Instant};

use fec::{de::FecDecoder, en::FecEncoder};
use primitive::io::token_bucket::TokenBucket;

const FEC_DEBUG: bool = false;

const WINDOW_SIZE: NonZeroU64 = NonZeroU64::new(32).unwrap();
const MAX_GROUP_SIZE: usize = MAX_DATA_PER_GROUP + MAX_PARITY_PER_GROUP;
/// Maximum data symbols accumulated before a group is forcibly flushed.
const MAX_DATA_PER_GROUP: usize = 20;
/// Parity overhead target: ~25% (1 parity per 4 data), at least 1 per group.
const PARITY_RATIO_NUM: usize = 1;
const PARITY_RATIO_DEN: usize = 4;
const MAX_PARITY_PER_GROUP: usize =
    (MAX_DATA_PER_GROUP * PARITY_RATIO_NUM).div_ceil(PARITY_RATIO_DEN);
/// Groups with at most this many data symbols get parity protection.
/// Larger groups skip parity to avoid impacting throughput of big traffic.
const PARITY_DATA_THRESHOLD: usize = 4;
/// Parity must consume at most this fraction of the currently-available send
/// budget. Parity is spare-bandwidth-only: it must never compete with data
/// traffic, so a parity burst is only flushed when it fits within 1/3 of the
/// tokens the bucket holds at flush time — the remaining 2/3 are left for
/// subsequent data packets. The group is proactively trimmed to
/// `PARITY_DATA_THRESHOLD` data symbols in `encode_data`, so the parity burst
/// is at most `MAX_PARITY_PER_GROUP` packets, a tiny, bounded cost.
const PARITY_BUDGET_DEN: usize = 3;
const GROUP_SIZE_HIST_LEN: usize = MAX_DATA_PER_GROUP + 1;

#[derive(Debug, Clone)]
pub struct FecConfig {
    pub symbol_size: usize,
    /// Parity depth requested for groups that encode as exactly one data
    /// symbol.  Multi-symbol groups always keep the stock 1:4 ratio and the
    /// spare-token budget gate regardless of this value.  `1` is stock
    /// behaviour.  See `FecTuning::interactive_parity_depth`.
    pub interactive_parity_depth: u8,
}

/// Encapsulated FEC state owned by the transmission layer. The transmission
/// layer calls `encode_data` on each outgoing packet and `decode` on each
/// incoming raw packet, then `maybe_flush_parities` after the send burst.
///
/// Parity is tail-only and burst-scoped: a group is closed (flushed or
/// skipped) at the end of every send burst and after every ACK/kill packet,
/// so no stale group carries over into the next burst. Parity is fixed-rate
/// (1:4 data-to-parity, clamped) and spare-token-only — it never competes
/// with data for send bandwidth.
#[derive(Debug)]
pub struct FecState {
    encoder: FecEncoder,
    decoder: FecDecoder,
    /// Codec payloads recovered by parity, waiting to be fed to the reliable
    /// layer's `recv_pkts` path.
    recovered: VecDeque<Vec<u8>>,
    enc_buf: Vec<u8>,
    /// Set by the transmission layer when the reliable layer reports that a
    /// tail FEC flush is permitted (send buffer empty, cwnd has room, no RTO
    /// pending). Cleared otherwise. When blocked, `close_fec_burst` skips the
    /// open group instead of flushing it.
    tail_flush_allowed: bool,
    /// Per-connection interactive parity depth for single-symbol groups (from
    /// `FecTuning`).  `1` is stock; a deeper value makes
    /// `maybe_flush_parities` emit up to that many parity copies for a group
    /// with exactly one data symbol, bypassing the spare-token budget gate.
    /// Multi-symbol groups always keep the stock gate and ratio.
    interactive_parity_depth: u8,
    stats: Stats,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct FecStats {
    pub parity_sent: usize,
    pub groups_flushed: usize,
    pub groups_skipped_no_surplus_tokens: usize,
    pub groups_skipped_burst_end: usize,
    pub recovered_symbols: usize,
    pub group_size_skipped_burst_end: [u64; GROUP_SIZE_HIST_LEN],
    pub group_size_skipped_no_surplus_tokens: [u64; GROUP_SIZE_HIST_LEN],
}

#[derive(Debug, Default)]
struct Stats {
    pub parity_sent: usize,
    pub groups_flushed: usize,
    pub groups_skipped_no_surplus_tokens: usize,
    pub parity_groups_skipped_burst_end: usize,
    pub recovered_symbols: usize,
    pub group_size_skipped_burst_end: [u64; GROUP_SIZE_HIST_LEN],
    pub group_size_skipped_no_surplus_tokens: [u64; GROUP_SIZE_HIST_LEN],
}

impl Stats {
    fn snapshot(&self) -> FecStats {
        FecStats {
            parity_sent: self.parity_sent,
            groups_flushed: self.groups_flushed,
            groups_skipped_no_surplus_tokens: self.groups_skipped_no_surplus_tokens,
            groups_skipped_burst_end: self.parity_groups_skipped_burst_end,
            recovered_symbols: self.recovered_symbols,
            group_size_skipped_burst_end: self.group_size_skipped_burst_end,
            group_size_skipped_no_surplus_tokens: self.group_size_skipped_no_surplus_tokens,
        }
    }
}

impl fmt::Display for FecStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FecStats")
            .field("parity_sent", &self.parity_sent)
            .field("groups_flushed", &self.groups_flushed)
            .field(
                "groups_skipped_no_surplus",
                &self.groups_skipped_no_surplus_tokens,
            )
            .field("groups_skipped_burst_end", &self.groups_skipped_burst_end)
            .field("recovered_symbols", &self.recovered_symbols)
            .field(
                "group_size_skipped_burst_end",
                &fmt_hist(&self.group_size_skipped_burst_end),
            )
            .field(
                "group_size_skipped_no_surplus_tokens",
                &fmt_hist(&self.group_size_skipped_no_surplus_tokens),
            )
            .finish()
    }
}

fn fmt_hist(hist: &[u64]) -> String {
    let entries: Vec<String> = (0..hist.len())
        .filter(|&i| hist[i] > 0)
        .map(|i| format!("{}:{}", i, hist[i]))
        .collect();
    if entries.is_empty() {
        "(empty)".to_string()
    } else {
        format!("[{}]", entries.join(", "))
    }
}

impl FecState {
    pub fn new(config: FecConfig) -> Self {
        let encoder = FecEncoder::builder()
            .symbol_size(config.symbol_size)
            .build();
        let decoder = FecDecoder::builder()
            .max_group_size(MAX_GROUP_SIZE)
            .symbol_size(config.symbol_size)
            .window_size(WINDOW_SIZE)
            .build();
        Self {
            encoder,
            decoder,
            recovered: VecDeque::new(),
            enc_buf: vec![0; config.symbol_size * 2],
            tail_flush_allowed: false,
            interactive_parity_depth: config.interactive_parity_depth.max(1),
            stats: Stats::default(),
        }
    }

    /// Mark the open group as eligible for a tail FEC flush at the next
    /// `close_fec_burst`. Called by the transmission layer when the reliable
    /// layer reports `can_send_tail_fec`.
    pub fn note_tail_flush_allowed(&mut self) {
        self.tail_flush_allowed = true;
    }

    /// Mark the open group as not eligible for a tail FEC flush. The next
    /// `close_fec_burst` will skip the group instead of flushing it.
    pub fn note_tail_flush_blocked(&mut self) {
        self.tail_flush_allowed = false;
    }

    /// Skip the currently-open FEC group, recording it in the burst-end skip
    /// stats. No-op when no group is open. Called at burst boundaries where a
    /// tail flush is not permitted (more data/RTX pending), so no stale group
    /// leaks into the next burst.
    pub fn skip_open_group(&mut self) {
        let data_count = self.encoder.group_data_count();
        if data_count == 0 {
            return;
        }
        self.stats.parity_groups_skipped_burst_end += 1;
        inc_hist(&mut self.stats.group_size_skipped_burst_end, data_count);
        self.encoder.skip_group();
    }

    /// Wrap an outgoing codec packet with a FEC data-symbol header and return
    /// the wire bytes to send via `utp_write`. Also accumulates the symbol into
    /// the current FEC group; call `maybe_flush_parities` after the send burst
    /// to emit parity for the group.
    ///
    /// To keep parity overhead bounded and protect only the recent tail group,
    /// the open group is force-skipped once it reaches `PARITY_DATA_THRESHOLD`
    /// data symbols *before* encoding the next symbol. Larger groups would be
    /// skipped by `maybe_flush_parities` anyway (parity is only emitted for
    /// groups with at most `PARITY_DATA_THRESHOLD` data symbols), so skipping
    /// early avoids encoding/decoding parity for symbols that can never be
    /// recovered. Only the most recent `<= PARITY_DATA_THRESHOLD` symbols of a
    /// burst are kept and protected.
    pub fn encode_data(&mut self, data: &[u8], out: &mut [u8]) -> usize {
        if self.encoder.group_data_count() >= PARITY_DATA_THRESHOLD {
            self.encoder.skip_group();
        }
        self.encoder.encode_data(data, out)
    }

    /// Attempt to flush parities for the current group, rate-limited by the
    /// token bucket. Returns `(parity_pkts, total_bytes)` where each entry is
    /// a ready-to-send wire packet. If the parity burst would exceed 1/3 of
    /// the available send budget, the group is skipped — parity is
    /// spare-bandwidth-only and must not compete with data traffic. Groups
    /// larger than `PARITY_DATA_THRESHOLD` are also skipped.
    ///
    /// **Single-symbol interactive exception:** when the open group has
    /// exactly one data symbol and `interactive_parity_depth > 1`, the group
    /// emits up to `interactive_parity_depth` parity copies **bypassing the
    /// spare-token budget gate**.  Multi-symbol groups always keep the stock
    /// 1:4 ratio and the budget gate regardless of the configured depth —
    /// ungated depth > 1 on bulk would add ~75% overhead and defeat the
    /// point.  The single-symbol group is exactly the case where the stock
    /// depth-1 parity is no better than a retransmit (one independent loss
    /// draw for the whole message), so the deeper parity buys tail latency
    /// for negligible bytes on a large-MSS path.
    ///
    /// Reed-Solomon needs the complete parity set to reconstruct, so the full
    /// `parity_count` tokens are reserved atomically before encoding any
    /// (the stock path only; the single-symbol bypass skips the budget
    /// check).  Parity must fit within 1/3 of the currently-available send
    /// budget (`PARITY_BUDGET_DEN`), leaving the rest for data traffic.
    pub fn maybe_flush_parities(
        &mut self,
        send_rate_limiter: &mut TokenBucket,
        now: Instant,
    ) -> Vec<Vec<u8>> {
        let data_count = self.encoder.group_data_count();
        if data_count == 0 {
            return vec![];
        }
        // Defensive: `encode_data` force-skips at PARITY_DATA_THRESHOLD, so a
        // group reaching here should already be <= threshold. Skip if a stale
        // larger group somehow survived (e.g. a flush was permitted at a burst
        // end that the early-skip did not catch).
        if data_count > PARITY_DATA_THRESHOLD {
            self.encoder.skip_group();
            return vec![];
        }
        let parity_count = parity_for(data_count);

        // Single-symbol interactive exception: a group with exactly one data
        // symbol and a configured depth > 1 bypasses the spare-token budget
        // gate and emits up to `interactive_parity_depth` parity copies.
        // Multi-symbol groups always keep the stock gate regardless of depth.
        if data_count == 1 && self.interactive_parity_depth > 1 {
            // Bypass the budget gate for the single-symbol interactive case.
            // The parity set is tiny (one data symbol → at most a handful of
            // parity symbols) and the whole point is tail-latency insurance
            // for an interactive message; gating it on spare tokens would
            // re-introduce the stock retransmit-equivalent behaviour.
            let depth = self.interactive_parity_depth;
            if FEC_DEBUG {
                eprintln!(
                    "FEC: flushing {depth} parities for single-symbol group (interactive, budget bypassed)"
                );
            }
            self.stats.groups_flushed += 1;
            let mut parity_encoder = self.encoder.flush_parities(depth);
            let mut pkts = vec![];
            while let Some(n) = parity_encoder.encode_parity(&mut self.enc_buf) {
                pkts.push(self.enc_buf[..n].to_vec());
            }
            self.stats.parity_sent += pkts.len();
            return pkts;
        }

        let available_tokens = send_rate_limiter.gen_tokens(now);
        let parity_budget = available_tokens / PARITY_BUDGET_DEN;
        if usize::from(parity_count) > parity_budget {
            self.stats.groups_skipped_no_surplus_tokens += 1;
            inc_hist(
                &mut self.stats.group_size_skipped_no_surplus_tokens,
                data_count,
            );
            self.encoder.skip_group();
            return vec![];
        }
        assert!(send_rate_limiter.take_exact_tokens(usize::from(parity_count), now));
        if FEC_DEBUG {
            eprintln!("FEC: flushing {parity_count} parities for group of {data_count}");
        }
        self.stats.groups_flushed += 1;
        let mut parity_encoder = self.encoder.flush_parities(parity_count);
        let mut pkts = vec![];
        while let Some(n) = parity_encoder.encode_parity(&mut self.enc_buf) {
            pkts.push(self.enc_buf[..n].to_vec());
        }
        self.stats.parity_sent += pkts.len();
        pkts
    }

    /// Feed an incoming raw UDP packet through the FEC decoder. Returns:
    /// - `Some(payload)` if the packet is a FEC data symbol — the payload is
    ///   the codec packet to pass to `decode()`.
    /// - `None` if the packet is a parity symbol (or undecodable) — recovered
    ///   data symbols are queued in `self.recovered` and should be drained via
    ///   `pop_recovered` before reading the next raw packet.
    pub fn decode(&mut self, pkt: &[u8]) -> Option<Vec<u8>> {
        let recovered_before = self.recovered.len();
        let hdr_len = self.decoder.decode(pkt, |data| {
            self.recovered.push_back(data.to_vec());
        });
        self.stats.recovered_symbols += self.recovered.len() - recovered_before;
        if FEC_DEBUG {
            let kind = if hdr_len.is_some() {
                "data"
            } else {
                "parity/none"
            };
            eprintln!(
                "FEC decode: kind={kind} pkt_len={} hdr_len={hdr_len:?} recovered={}",
                pkt.len(),
                self.recovered.len()
            );
        }
        hdr_len.map(|hl| pkt[hl..].to_vec())
    }

    /// Pop a codec payload recovered by parity.
    pub fn pop_recovered(&mut self) -> Option<Vec<u8>> {
        self.recovered.pop_front()
    }

    /// Whether a tail flush is currently allowed. Consumed by
    /// `close_fec_burst` to decide between flushing and skipping.
    pub fn tail_flush_allowed(&self) -> bool {
        self.tail_flush_allowed
    }

    /// Number of codec payloads recovered by parity so far. Returns `None`
    /// only conceptually (always `Some(0)` when FEC is on); used by tests to
    /// assert that parity actually reconstructed lost data.
    pub(crate) fn recovered_symbols(&self) -> usize {
        self.stats.recovered_symbols
    }

    /// Test-only accessor for the configured single-symbol interactive
    /// parity depth.
    #[cfg(test)]
    pub(crate) fn interactive_parity_depth(&self) -> u8 {
        self.interactive_parity_depth
    }

    /// Test-only accessor for the running parity-sent counter.
    #[cfg(test)]
    pub(crate) fn parity_sent(&self) -> usize {
        self.stats.parity_sent
    }

    /// Print the basic FEC counters to stderr. Only active when `FEC_DEBUG` is
    /// enabled — flip that flag to debug FEC behavior. Called by the
    /// transmission layer when the read stream reaches EOF so the snapshot is
    /// guaranteed to be visible before the process tears down its spawned
    /// tasks.
    pub fn debug_print_stats(&self) {
        if FEC_DEBUG {
            eprintln!("FEC stats: {}", self.stats.snapshot());
        }
    }
}

/// Parity count for a group of `data_count` data symbols, using the static
/// 1:4 data-to-parity ratio, clamped to `[1, MAX_PARITY_PER_GROUP]`. Fixed
/// rate — no loss scaling.
fn parity_for(data_count: usize) -> u8 {
    let base = (data_count * PARITY_RATIO_NUM).div_ceil(PARITY_RATIO_DEN);
    base.clamp(1, MAX_PARITY_PER_GROUP).try_into().unwrap()
}

/// Increment a histogram bucket: push a count if no bucket for this size yet,
/// otherwise leave the existing one. Kept simple — sizes are small and
/// infrequent.
fn inc_hist(hist: &mut [u64], idx: usize) {
    if let Some(count) = hist.get_mut(idx) {
        *count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    /// A fresh `FecState` with a given interactive parity depth, sized for a
    /// large-MSS loopback path so single-symbol groups dominate.
    fn fec_state(symbol_size: usize, interactive_parity_depth: u8) -> FecState {
        FecState::new(FecConfig {
            symbol_size,
            interactive_parity_depth,
        })
    }

    /// A `TokenBucket` with effectively unlimited tokens so the stock budget
    /// gate never trims a parity burst (the multi-symbol gate is exercised by
    /// a separate test that drains the bucket).  Returns `(bucket, now)` so
    /// the caller uses the same `now` the bucket was filled at.
    fn unlimited_bucket(now: Instant) -> (TokenBucket, Instant) {
        use core::num::NonZeroUsize;
        use core::time::Duration;
        use primitive::ops::float::PosR;
        let tb = TokenBucket::new(
            PosR::new(1e9).unwrap(),
            NonZeroUsize::new(usize::MAX).unwrap(),
            now,
        );
        // Pre-fill by advancing time; return the advanced timestamp so
        // callers query the bucket at the same instant.
        let later = now + Duration::from_secs(1000);
        let mut tb = tb;
        let _ = tb.gen_tokens(later);
        (tb, later)
    }

    /// A `TokenBucket` drained to zero so the stock budget gate trims any
    /// multi-symbol parity burst (single-symbol interactive bypass still
    /// applies).  Rate is 1 token/sec so no tokens regenerate during the
    /// test.
    fn empty_bucket(now: Instant) -> TokenBucket {
        use core::num::NonZeroUsize;
        use core::time::Duration;
        use primitive::ops::float::PosR;
        let mut tb = TokenBucket::new(PosR::new(1.0).unwrap(), NonZeroUsize::new(usize::MAX).unwrap(), now);
        // Force-fill then drain all tokens.
        let later = now + Duration::from_secs(1000);
        let _ = tb.gen_tokens(later);
        let drained = tb.take_at_most_tokens(usize::MAX, later);
        assert!(drained > 0, "bucket should have tokens to drain");
        // Now the bucket is empty; rate=1/s so it stays ~empty for the test.
        tb
    }

    /// A single-symbol group with `interactive_parity_depth = 3` must emit
    /// exactly 3 parity copies, bypassing the spare-token budget gate even
    /// when the bucket is empty.
    #[test]
    fn single_symbol_group_emits_depth_parities_bypassing_budget() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 3);
        let mut tb = empty_bucket(now);

        // Encode one data symbol (single-symbol group).
        let data = b"hello interactive world";
        let mut sym_buf = vec![0u8; 8192];
        let _n = fec.encode_data(data, &mut sym_buf);
        assert_eq!(fec.encoder.group_data_count(), 1);

        let pkts = fec.maybe_flush_parities(&mut tb, now);
        assert_eq!(
            pkts.len(),
            3,
            "single-symbol group at depth 3 must emit 3 parity copies, got {}",
            pkts.len()
        );
        assert_eq!(fec.parity_sent(), 3);
    }

    /// A multi-symbol group must keep the stock budget gate regardless of the
    /// configured interactive depth: when the bucket is empty, a
    /// multi-symbol group is skipped (0 parity) even with depth 3.
    #[test]
    fn multi_symbol_group_keeps_budget_gate_even_with_depth() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 3);
        let mut tb = empty_bucket(now);

        // Encode two data symbols (multi-symbol group). Note
        // PARITY_DATA_THRESHOLD is 4, so a 2-symbol group is not force-skipped.
        let data = b"first symbol payload";
        let mut sym_buf = vec![0u8; 8192];
        fec.encode_data(data, &mut sym_buf);
        fec.encode_data(data, &mut sym_buf);
        assert_eq!(fec.encoder.group_data_count(), 2);

        let pkts = fec.maybe_flush_parities(&mut tb, now);
        assert_eq!(
            pkts.len(),
            0,
            "multi-symbol group with empty bucket must be skipped (0 parity), got {}",
            pkts.len()
        );
    }

    /// A multi-symbol group with a full bucket emits the stock 1:4 parity
    /// (1 parity for 2-4 data symbols), NOT the interactive depth — proving
    /// the depth is single-symbol-only.
    #[test]
    fn multi_symbol_group_with_full_bucket_emits_stock_parity_not_depth() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 3);
        let (mut tb, now) = unlimited_bucket(now);

        // Two data symbols → stock parity_for(2) = 1.
        let data = b"first symbol payload";
        let mut sym_buf = vec![0u8; 8192];
        fec.encode_data(data, &mut sym_buf);
        fec.encode_data(data, &mut sym_buf);
        assert_eq!(fec.encoder.group_data_count(), 2);

        let pkts = fec.maybe_flush_parities(&mut tb, now);
        assert_eq!(
            pkts.len(),
            1,
            "multi-symbol group must emit stock parity_for(2)=1, not depth 3; got {}",
            pkts.len()
        );
    }

    /// A single-symbol group with depth 1 (stock) emits 1 parity and respects
    /// the budget gate — proving the bypass only fires when depth > 1.
    #[test]
    fn single_symbol_group_at_depth_1_respects_budget_gate() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 1);
        let mut tb = empty_bucket(now);

        let data = b"hello";
        let mut sym_buf = vec![0u8; 8192];
        fec.encode_data(data, &mut sym_buf);

        let pkts = fec.maybe_flush_parities(&mut tb, now);
        assert_eq!(
            pkts.len(),
            0,
            "single-symbol group at depth 1 with empty bucket must be skipped, got {}",
            pkts.len()
        );
    }

    /// `FecState::new` clamps a misconfigured `interactive_parity_depth = 0`
    /// to 1 so the stock path always emits at least 1 parity.
    #[test]
    fn depth_zero_is_clamped_to_one() {
        let fec = fec_state(8192 - 11, 0);
        assert_eq!(fec.interactive_parity_depth(), 1);
    }
}
