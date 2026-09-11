use std::{
    cell::Cell,
    collections::VecDeque,
    fmt,
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use fec::{de::FecDecoder, en::FecEncoder};
use primitive::io::token_bucket::TokenBucket;

const FEC_DEBUG: bool = false;

fn fec_hdr_size() -> usize {
    let probe = crate::udp::MAX_MSS;
    probe - fec::proto::symbol_size(probe).unwrap()
}

const WINDOW_SIZE: NonZeroU64 = NonZeroU64::new(32).unwrap();
const MAX_GROUP_SIZE: usize = MAX_DATA_PER_GROUP + MAX_PARITY_PER_GROUP;
/// Maximum data symbols accumulated before a group is forcibly flushed.
const MAX_DATA_PER_GROUP: usize = 20;
/// Parity overhead target: ~25% (1 parity per 4 data), at least 1 per group.
const PARITY_RATIO_NUM: usize = 1;
const PARITY_RATIO_DEN: usize = 4;
const MAX_PARITY_PER_GROUP: usize =
    (MAX_DATA_PER_GROUP * PARITY_RATIO_NUM).div_ceil(PARITY_RATIO_DEN);
const MAX_INTERACTIVE_PARITY_DEPTH: u8 = (MAX_GROUP_SIZE - 1) as u8;
/// Groups with at most this many data symbols get parity protection.
/// Larger groups skip parity to avoid impacting throughput of big traffic.
const PARITY_DATA_THRESHOLD: usize = 4;
/// In-stream group FEC: a data group accumulates up to this many data symbols
/// before a full-group inline parity flush is emitted mid-burst.  Stock
/// (toggle off) force-skips at `PARITY_DATA_THRESHOLD` instead, so groups never
/// reach this size.
const INSTREAM_DATA_PER_GROUP: usize = 8;
/// Parity symbols emitted for a full in-stream group (`INSTREAM_DATA_PER_GROUP`
/// data symbols).  8+4 = 12 fits the stock decoder `MAX_GROUP_SIZE` (25) and
/// `WINDOW_SIZE` (32) without bumping either constant.
const INSTREAM_PARITY_PER_GROUP: usize = 4;
/// Parity must consume at most this fraction of the currently-available send
/// budget. Parity is spare-bandwidth-only: it must never compete with data
/// traffic, so a parity burst is only flushed when it fits within 1/3 of the
/// tokens the bucket holds at flush time — the remaining 2/3 are left for
/// subsequent data packets. The group is proactively trimmed to
/// `PARITY_DATA_THRESHOLD` data symbols in `encode_data`, so the parity burst
/// is at most `MAX_PARITY_PER_GROUP` packets, a tiny, bounded cost.
const PARITY_BUDGET_DEN: usize = 3;
const GROUP_SIZE_HIST_LEN: usize = MAX_DATA_PER_GROUP + 1;

thread_local! {
    static IN_FEC_DECODE: Cell<bool> = const { Cell::new(false) };
}

struct FecDecodeGuard;
impl Drop for FecDecodeGuard {
    fn drop(&mut self) {
        IN_FEC_DECODE.with(|flag| flag.set(false));
    }
}

fn suppress_fec_decoder_panic_hook() {
    use std::panic::{set_hook, take_hook};
    use std::sync::OnceLock;
    static HOOK: OnceLock<()> = OnceLock::new();
    HOOK.get_or_init(|| {
        let default = take_hook();
        set_hook(Box::new(move |info| {
            if !IN_FEC_DECODE.with(Cell::get) {
                default(info);
            }
        }));
    });
}

#[derive(Debug, Clone)]
pub struct FecConfig {
    pub symbol_size: usize,
    /// Parity depth requested for groups that encode as exactly one data
    /// symbol.  Multi-symbol groups always keep the stock 1:4 ratio and the
    /// spare-token budget gate regardless of this value.  `1` is stock
    /// behaviour.  See `FecTuning::small_group_parity_count`.
    pub small_group_parity_count: u8,
}

/// Split FEC ownership for the read/write actor halves.  The encoder half is
/// owned by the write driver (encode/flush/parity), the decoder half by the
/// read driver (decode/recover); both share one `Arc<Stats>` so the session
/// can observe aggregate counters without touching either actor's state.
#[derive(Debug)]
pub struct FecState {
    encoder: FecEncoderState,
    decoder: FecDecoderState,
}

/// Write-actor FEC state: encodes outgoing symbols, holds the group parity
/// buffer, and mutates the shared counters.  The sole mutator is the write
/// driver.
#[derive(Debug)]
pub(crate) struct FecEncoderState {
    encoder: FecEncoder,
    enc_buf: Vec<u8>,
    small_group_parity_count: u8,
    stats: Arc<Stats>,
    /// Whether the open group's parity flush is currently deferred by the
    /// no-spare-capacity gate (queued work, cwnd pressure, or a pending tail
    /// probe).  The group is HELD OPEN — never destroyed — so the parity
    /// survives the gate-closed window and the next budgeted flush emits it.
    /// Guards against re-recording the same group's wait on repeated
    /// gate-closed passes.
    deferred_no_spare_capacity: bool,
    /// Parity packets already encoded by a forced flush at
    /// `MAX_DATA_PER_GROUP` (see `encode_data`), waiting for the next
    /// budgeted `maybe_flush_parities`.  Bounded: at most
    /// `MAX_PARITY_PER_GROUP` packets per capped group.
    pending_cap_parity: VecDeque<Vec<u8>>,
}

/// Read-actor FEC state: decodes incoming symbols and drains recovered
/// payloads.  The sole mutator is the read driver.
#[derive(Debug)]
pub(crate) struct FecDecoderState {
    decoder: FecDecoder,
    recovered: VecDeque<Vec<u8>>,
    symbol_size: usize,
    stats: Arc<Stats>,
}

/// Cloneable handle over the shared FEC counters, handed to the session so
/// observability (e.g. recovered-symbol totals) needs no actor state.
#[derive(Clone, Debug)]
pub(crate) struct FecStatsHandle(Arc<Stats>);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct FecStats {
    parity_sent: usize,
    groups_flushed: usize,
    groups_skipped_no_surplus_tokens: usize,
    groups_skipped_burst_end: usize,
    groups_skipped_loss_gate: usize,
    groups_skipped_no_spare_capacity: usize,
    recovered_symbols: usize,
    dropped_malformed_pkts: usize,
    dropped_fec_decoder_panics: usize,
    group_size_flushed: [u64; GROUP_SIZE_HIST_LEN],
    group_size_skipped_burst_end: [u64; GROUP_SIZE_HIST_LEN],
    group_size_skipped_no_surplus_tokens: [u64; GROUP_SIZE_HIST_LEN],
    group_size_skipped_loss_gate: [u64; GROUP_SIZE_HIST_LEN],
    group_size_skipped_no_spare_capacity: [u64; GROUP_SIZE_HIST_LEN],
}

/// Shared counters mutated by both actor halves.  All fields are atomics so
/// the single `Arc<Stats>` can be updated from the read and write drivers
/// without a lock.
#[derive(Debug)]
struct Stats {
    parity_sent: AtomicUsize,
    groups_flushed: AtomicUsize,
    groups_skipped_no_surplus_tokens: AtomicUsize,
    parity_groups_skipped_burst_end: AtomicUsize,
    groups_skipped_loss_gate: AtomicUsize,
    groups_skipped_no_spare_capacity: AtomicUsize,
    recovered_symbols: AtomicUsize,
    dropped_malformed_pkts: AtomicUsize,
    dropped_fec_decoder_panics: AtomicUsize,
    group_size_flushed: [AtomicU64; GROUP_SIZE_HIST_LEN],
    group_size_skipped_burst_end: [AtomicU64; GROUP_SIZE_HIST_LEN],
    group_size_skipped_no_surplus_tokens: [AtomicU64; GROUP_SIZE_HIST_LEN],
    group_size_skipped_loss_gate: [AtomicU64; GROUP_SIZE_HIST_LEN],
    group_size_skipped_no_spare_capacity: [AtomicU64; GROUP_SIZE_HIST_LEN],
}

impl Default for Stats {
    fn default() -> Self {
        let hist = || std::array::from_fn(|_| AtomicU64::new(0));
        Self {
            parity_sent: AtomicUsize::new(0),
            groups_flushed: AtomicUsize::new(0),
            groups_skipped_no_surplus_tokens: AtomicUsize::new(0),
            parity_groups_skipped_burst_end: AtomicUsize::new(0),
            groups_skipped_loss_gate: AtomicUsize::new(0),
            groups_skipped_no_spare_capacity: AtomicUsize::new(0),
            recovered_symbols: AtomicUsize::new(0),
            dropped_malformed_pkts: AtomicUsize::new(0),
            dropped_fec_decoder_panics: AtomicUsize::new(0),
            group_size_flushed: hist(),
            group_size_skipped_burst_end: hist(),
            group_size_skipped_no_surplus_tokens: hist(),
            group_size_skipped_loss_gate: hist(),
            group_size_skipped_no_spare_capacity: hist(),
        }
    }
}

impl Stats {
    fn snapshot(&self) -> FecStats {
        FecStats {
            parity_sent: self.parity_sent.load(Ordering::Relaxed),
            groups_flushed: self.groups_flushed.load(Ordering::Relaxed),
            groups_skipped_no_surplus_tokens: self
                .groups_skipped_no_surplus_tokens
                .load(Ordering::Relaxed),
            groups_skipped_burst_end: self.parity_groups_skipped_burst_end.load(Ordering::Relaxed),
            groups_skipped_loss_gate: self.groups_skipped_loss_gate.load(Ordering::Relaxed),
            groups_skipped_no_spare_capacity: self
                .groups_skipped_no_spare_capacity
                .load(Ordering::Relaxed),
            recovered_symbols: self.recovered_symbols.load(Ordering::Relaxed),
            dropped_malformed_pkts: self.dropped_malformed_pkts.load(Ordering::Relaxed),
            dropped_fec_decoder_panics: self.dropped_fec_decoder_panics.load(Ordering::Relaxed),
            group_size_flushed: std::array::from_fn(|index| {
                self.group_size_flushed[index].load(Ordering::Relaxed)
            }),
            group_size_skipped_burst_end: std::array::from_fn(|index| {
                self.group_size_skipped_burst_end[index].load(Ordering::Relaxed)
            }),
            group_size_skipped_no_surplus_tokens: std::array::from_fn(|index| {
                self.group_size_skipped_no_surplus_tokens[index].load(Ordering::Relaxed)
            }),
            group_size_skipped_loss_gate: std::array::from_fn(|index| {
                self.group_size_skipped_loss_gate[index].load(Ordering::Relaxed)
            }),
            group_size_skipped_no_spare_capacity: std::array::from_fn(|index| {
                self.group_size_skipped_no_spare_capacity[index].load(Ordering::Relaxed)
            }),
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
            .field("groups_skipped_loss_gate", &self.groups_skipped_loss_gate)
            .field(
                "groups_skipped_no_spare_capacity",
                &self.groups_skipped_no_spare_capacity,
            )
            .field("recovered_symbols", &self.recovered_symbols)
            .field("dropped_malformed_pkts", &self.dropped_malformed_pkts)
            .field(
                "dropped_fec_decoder_panics",
                &self.dropped_fec_decoder_panics,
            )
            .field("group_size_flushed", &fmt_hist(&self.group_size_flushed))
            .field(
                "group_size_skipped_burst_end",
                &fmt_hist(&self.group_size_skipped_burst_end),
            )
            .field(
                "group_size_skipped_no_surplus_tokens",
                &fmt_hist(&self.group_size_skipped_no_surplus_tokens),
            )
            .field(
                "group_size_skipped_loss_gate",
                &fmt_hist(&self.group_size_skipped_loss_gate),
            )
            .field(
                "group_size_skipped_no_spare_capacity",
                &fmt_hist(&self.group_size_skipped_no_spare_capacity),
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

fn encodable_wire_pkt(pkt: &[u8], symbol_size: usize) -> bool {
    if pkt.len() > symbol_size + fec_hdr_size() {
        return false;
    }
    let Some(&data_count) = pkt.get(9) else {
        return false;
    };
    if data_count == 0 {
        return true;
    }
    let Some(&parity_count) = pkt.get(10) else {
        return false;
    };
    parity_count != 0 && usize::from(data_count) + usize::from(parity_count) <= MAX_GROUP_SIZE
}

impl FecState {
    pub fn new(config: FecConfig) -> Self {
        let stats = Arc::new(Stats::default());
        Self {
            encoder: FecEncoderState {
                encoder: FecEncoder::builder()
                    .symbol_size(config.symbol_size)
                    .build(),
                enc_buf: vec![0; config.symbol_size * 2],
                small_group_parity_count: config
                    .small_group_parity_count
                    .clamp(1, MAX_INTERACTIVE_PARITY_DEPTH),
                stats: Arc::clone(&stats),
                deferred_no_spare_capacity: false,
                pending_cap_parity: VecDeque::new(),
            },
            decoder: FecDecoderState {
                decoder: FecDecoder::builder()
                    .max_group_size(MAX_GROUP_SIZE)
                    .symbol_size(config.symbol_size)
                    .window_size(WINDOW_SIZE)
                    .build(),
                recovered: VecDeque::new(),
                symbol_size: config.symbol_size,
                stats,
            },
        }
    }

    /// Split into the write-actor encoder state, the read-actor decoder
    /// state, and the shared counters handle.  Each actor becomes the sole
    /// mutator of its half; the handle is observation-only.
    pub(crate) fn into_actor_parts(self) -> (FecEncoderState, FecDecoderState, FecStatsHandle) {
        let stats = FecStatsHandle(Arc::clone(&self.encoder.stats));
        (self.encoder, self.decoder, stats)
    }

    #[cfg(test)]
    pub fn max_wire_pkt_size(&self) -> usize {
        self.decoder.symbol_size + fec_hdr_size()
    }
}

impl FecEncoderState {
    /// Skip the currently-open FEC group, recording it in the burst-end skip
    /// stats. No-op when no group is open. Called at burst boundaries where a
    /// tail flush is not permitted (more data/RTX pending), so no stale group
    /// leaks into the next burst.
    pub fn skip_open_group(&mut self) {
        let data_count = self.encoder.group_data_count();
        if data_count == 0 {
            return;
        }
        self.stats
            .parity_groups_skipped_burst_end
            .fetch_add(1, Ordering::Relaxed);
        inc_hist(&self.stats.group_size_skipped_burst_end, data_count);
        self.deferred_no_spare_capacity = false;
        self.encoder.skip_group();
    }

    /// Skip the currently-open FEC group, recording it in the loss-gate skip
    /// stats. No-op when no group is open. Called at burst boundaries where
    /// the condition gate is closed on loss/recovery evidence.
    pub fn skip_open_group_loss_gate(&mut self) {
        let data_count = self.encoder.group_data_count();
        if data_count == 0 {
            return;
        }
        self.stats
            .groups_skipped_loss_gate
            .fetch_add(1, Ordering::Relaxed);
        inc_hist(&self.stats.group_size_skipped_loss_gate, data_count);
        self.deferred_no_spare_capacity = false;
        self.encoder.skip_group();
    }

    /// Hold the currently-open FEC group open when the condition gate is
    /// closed on capacity evidence (queued/waiting application work, queue
    /// growth, cwnd pressure, retransmission, or a pending tail probe),
    /// recording the deferral in the no-spare-capacity stats. No-op when no
    /// group is open.
    ///
    /// Unlike the loss-gate and burst-end skips this is a DEFERRAL, not a
    /// destruction: the group's already-accumulated data must not lose its
    /// parity the moment a tail-loss probe becomes due.  `can_send_tail_fec`
    /// closes while `has_tail_probe` is true, and clearing the group here
    /// would permanently cost the old tail group its parity after the probe
    /// outcome — they would recover at RTO speed despite a healthy spare
    /// budget.  Instead the group stays open and the next budgeted flush
    /// (after the probe is acked or its timeout elapses and capacity is
    /// genuinely spare again) emits its parity.  A group that is genuinely
    /// superseded by more data is closed by `encode_data`'s force-skip at
    /// `PARITY_DATA_THRESHOLD` / the `MAX_DATA_PER_GROUP` cap flush.
    pub fn skip_open_group_no_spare_capacity(&mut self) {
        let data_count = self.encoder.group_data_count();
        if data_count == 0 || self.deferred_no_spare_capacity {
            // No group, or a group already held from a previous gate-closed
            // pass (the tail probe is still pending): keep holding without
            // re-recording the same group's wait.
            return;
        }
        self.stats
            .groups_skipped_no_spare_capacity
            .fetch_add(1, Ordering::Relaxed);
        inc_hist(&self.stats.group_size_skipped_no_spare_capacity, data_count);
        // Leave the group open: the next flush pass (after the probe resolves
        // or capacity frees) emits its parity.
        self.deferred_no_spare_capacity = true;
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
    ///
    /// **In-stream group FEC exception:** when `instream` is `true` (the toggle
    /// is enabled for this connection), the `PARITY_DATA_THRESHOLD` force-skip
    /// is suppressed so a group may accumulate up to
    /// `INSTREAM_DATA_PER_GROUP` data symbols.  The transmission layer flushes
    /// `INSTREAM_PARITY_PER_GROUP` parities inline once the group is full (see
    /// `group_data_full`), instead of waiting for the burst tail.  Stock path
    /// passes `false` and keeps the force-skip, so behaviour is byte-identical
    /// when the toggle is off.
    ///
    /// **Hard cap on the open group:** `MAX_DATA_PER_GROUP` (documented as a
    /// forced-flush point but previously unenforced) is enforced HERE.  A
    /// group that already holds `MAX_DATA_PER_GROUP` data symbols when the
    /// next symbol arrives — e.g. the transmission layer's per-pass close
    /// discipline failed to flush it, or an error path leaked the open group
    /// — is force-flushed (its stock-ratio parity is encoded now and queued
    /// for the budgeted delivery via `force_flush_capped_group`) and the new
    /// symbol starts a fresh group.  `symbol_id` therefore never climbs past
    /// the peer's decoder max, and no error path can leak an oversized open
    /// group.
    pub fn encode_data(&mut self, data: &[u8], out: &mut [u8], instream: bool) -> usize {
        if !instream && self.encoder.group_data_count() >= PARITY_DATA_THRESHOLD {
            self.encoder.skip_group();
            self.deferred_no_spare_capacity = false;
        }
        if self.encoder.group_data_count() >= MAX_DATA_PER_GROUP {
            self.force_flush_capped_group();
        }
        self.encoder.encode_data(data, out)
    }

    /// Encode and queue parity for a group that reached `MAX_DATA_PER_GROUP`
    /// data symbols, then close it.  `flush_parities` computes the stock-ratio
    /// parity (`MAX_PARITY_PER_GROUP` packets — 20 data + 5 parity fits the
    /// shared decoder `MAX_GROUP_SIZE`) and clears the group in one call, the
    /// clean reset the encoder API offers, so the encoded packets are queued
    /// here for the next budgeted `maybe_flush_parities` instead of being
    /// dropped.  Any pending no-spare-capacity deferral is superseded by this
    /// close.
    fn force_flush_capped_group(&mut self) {
        let data_count = self.encoder.group_data_count();
        debug_assert_eq!(data_count, MAX_DATA_PER_GROUP);
        self.deferred_no_spare_capacity = false;
        let parity_count = MAX_PARITY_PER_GROUP as u8;
        let mut parity_encoder = self.encoder.flush_parities(parity_count);
        let mut pkts = vec![];
        while let Some(n) = parity_encoder.encode_parity(&mut self.enc_buf) {
            pkts.push(self.enc_buf[..n].to_vec());
        }
        self.pending_cap_parity.extend(pkts);
    }

    /// Whether the open FEC group is a full in-stream group ready for an inline
    /// mid-burst parity flush.  Only `true` when `instream` is `true` (the
    /// toggle is on) AND the group has reached `INSTREAM_DATA_PER_GROUP` data
    /// symbols.  The caller invokes this after each `encode_data` push, so the
    /// first time the count equals the threshold the group flushes inline
    /// (8 data symbols → 4 parity symbols).  Stock path passes `false` and
    /// always gets `false`, so the inline flush never fires.
    pub fn group_data_full(&self, instream: bool) -> bool {
        instream && self.encoder.group_data_count() >= INSTREAM_DATA_PER_GROUP
    }

    /// Attempt to flush parities for the current group, rate-limited by the
    /// token bucket. Returns `(parity_pkts, total_bytes)` where each entry is
    /// a ready-to-send wire packet. If the parity burst would exceed 1/3 of
    /// the available send budget, the group is skipped — parity is
    /// spare-bandwidth-only and must not compete with data traffic. Groups
    /// larger than `PARITY_DATA_THRESHOLD` are also skipped (stock path).
    ///
    /// A cap-forced full-group parity stash (see `encode_data` and
    /// `MAX_DATA_PER_GROUP`) is drained first through the same budget gate;
    /// on a tight budget it HOLDS for the next pass instead of being dropped
    /// (the group is already closed, so this is its only chance to be
    /// protected).
    ///
    /// **In-stream group FEC:** when `instream` is `true`, any multi-symbol
    /// group (`data_count >= 2`) emits `INSTREAM_PARITY_PER_GROUP` parity
    /// symbols, gated on the spare-token budget (ungated parity on a bulk
    /// path collapses throughput from ~0.34 to ~0.06 MiB/s in prototyping).
    /// Depth-4 parity on partial multi-symbol groups (2..7 data symbols) is
    /// the measured win for interactive messages — a 2048-byte message at
    /// default MSS is 2 data symbols, and stock `parity_for(2)=1` parity
    /// makes message p50 *worse* with the toggle on.  The transmission layer
    /// force-flushes partial groups at burst end via `flush_fec_parities`.
    /// Stock path passes `false` and keeps the `PARITY_DATA_THRESHOLD` skip.
    ///
    /// **Single-symbol interactive exception:** when the open group has
    /// exactly one data symbol and `small_group_parity_count > 1`, the group
    /// emits up to `small_group_parity_count` parity copies, bypassing the
    /// stock skip-on-tight-budget gate.  Multi-symbol groups always keep the
    /// stock 1:4 ratio and the budget gate regardless of the configured depth
    /// — ungated depth > 1 on bulk would add ~75% overhead and defeat the
    /// point.  The single-symbol group is exactly the case where the stock
    /// depth-1 parity is no better than a retransmit (one independent loss
    /// draw for the whole message), so the deeper parity buys tail latency
    /// for negligible bytes on a large-MSS path.  The burst is still paced:
    /// the depth is capped at the same 1/3 spare-budget share the stock path
    /// uses (`PARITY_BUDGET_DEN`), so a large configured depth never drains
    /// the whole bucket in one unpaced multi-token burst, and on a
    /// near-empty bucket the group is HELD open (never destroyed) so the next
    /// send pass after the token refill emits the interactive floor parity.
    ///
    /// Note: "interactive" here is defined purely by symbol count at the
    /// FEC Layer. It is intentionally independent of any upper-layer
    /// byte-size traffic classification - a message an upper mux classifies
    /// as interactive (e.g. 2048 bytes) may still be a multi-symbol group
    /// here at default MSS. The two notions are not supposed to align.
    ///
    /// Reed-Solomon needs the complete parity set to reconstruct, so the full
    /// `parity_count` tokens are reserved atomically before encoding any
    /// (the stock and in-stream paths; the single-symbol bypass is capped by
    /// the same budget share rather than skipped).  Parity must fit within
    /// 1/3 of the currently-available send budget (`PARITY_BUDGET_DEN`),
    /// leaving the rest for data traffic.
    pub fn maybe_flush_parities(
        &mut self,
        send_rate_limiter: &mut TokenBucket,
        now: Instant,
        instream: bool,
    ) -> Vec<Vec<u8>> {
        // A cap-forced full-group parity stash (see `encode_data` and
        // `MAX_DATA_PER_GROUP`) is delivered first, through the same
        // token-budget gate as any other parity burst: on a tight budget the
        // stash HOLDS for the next pass instead of being dropped (the group
        // is already closed, so this is its only chance to be protected).
        if !self.pending_cap_parity.is_empty() {
            let need = self.pending_cap_parity.len();
            let available_tokens = send_rate_limiter.gen_tokens(now);
            let parity_budget = available_tokens / PARITY_BUDGET_DEN;
            if need > parity_budget {
                return vec![];
            }
            assert!(send_rate_limiter.take_exact_tokens(need, now));
            self.stats.groups_flushed.fetch_add(1, Ordering::Relaxed);
            inc_hist(&self.stats.group_size_flushed, MAX_DATA_PER_GROUP);
            let pkts: Vec<Vec<u8>> = std::mem::take(&mut self.pending_cap_parity)
                .into_iter()
                .collect();
            self.stats
                .parity_sent
                .fetch_add(pkts.len(), Ordering::Relaxed);
            return pkts;
        }
        // Any no-spare-capacity deferral is resolved by this flush attempt:
        // the group is flushed below or closed by one of the skip paths, so
        // the next deferral records a fresh group.
        self.deferred_no_spare_capacity = false;
        let data_count = self.encoder.group_data_count();
        if data_count == 0 {
            return vec![];
        }
        // Single-symbol interactive exception first: it bypasses the stock
        // skip-on-tight-budget gate (a single symbol's parity is negligible),
        // but the burst must still be paced — cap the depth by the same 1/3
        // spare-budget share the stock path uses, so a large configured depth
        // cannot emit an unpaced multi-token burst many times the data size.
        // On a near-empty bucket the group is HELD open (never destroyed), so
        // the interactive tail-latency benefit survives the drained pacer: it
        // is emitted on the next send pass after the token refill.
        if data_count == 1 && self.small_group_parity_count > 1 {
            let configured_depth = usize::from(self.small_group_parity_count);
            let available_tokens = send_rate_limiter.gen_tokens(now);
            let parity_budget = available_tokens / PARITY_BUDGET_DEN;
            if parity_budget == 0 {
                // The pacer is typically drained right after the data burst —
                // exactly the moment the interactive tail needs protection.
                // Hold the group open for the next pass (never skip it).
                return vec![];
            }
            // With at least `PARITY_BUDGET_DEN` tokens the floor of one
            // parity is always affordable; depth is capped at the 1/3 share
            // so the remaining 2/3 of tokens stay for data.
            let depth = configured_depth.min(parity_budget);
            if !send_rate_limiter.take_exact_tokens(depth, now) {
                // Defensive: the token accounting changed between the gen and
                // the take.  Never destroy the group — hold for the next pass.
                return vec![];
            }
            if FEC_DEBUG {
                eprintln!(
                    "FEC: flushing {depth} parities for single-symbol group (interactive, budget-capped)"
                );
            }
            self.stats.groups_flushed.fetch_add(1, Ordering::Relaxed);
            inc_hist(&self.stats.group_size_flushed, data_count);
            let mut parity_encoder = self.encoder.flush_parities(depth as u8);
            let mut pkts = vec![];
            while let Some(n) = parity_encoder.encode_parity(&mut self.enc_buf) {
                pkts.push(self.enc_buf[..n].to_vec());
            }
            self.stats
                .parity_sent
                .fetch_add(pkts.len(), Ordering::Relaxed);
            return pkts;
        }
        // In-stream group FEC path: any multi-symbol group (data_count >= 2)
        // emits `INSTREAM_PARITY_PER_GROUP` parity symbols, budget-gated.
        // This fires inline mid-burst (a full 8-symbol group triggers the
        // transmission layer's `maybe_flush_full_fec_group`) and at burst end
        // for partial groups (2..7 symbols) via the data-path force-flush.
        // Single-symbol groups (data_count == 1) are handled by the
        // interactive exception above when `small_group_parity_count > 1`,
        // or fall through to the stock path below.
        if instream && data_count >= 2 {
            let parity_count = INSTREAM_PARITY_PER_GROUP as u8;
            let available_tokens = send_rate_limiter.gen_tokens(now);
            let parity_budget = available_tokens / PARITY_BUDGET_DEN;
            if usize::from(parity_count) > parity_budget {
                self.stats
                    .groups_skipped_no_surplus_tokens
                    .fetch_add(1, Ordering::Relaxed);
                inc_hist(&self.stats.group_size_skipped_no_surplus_tokens, data_count);
                self.encoder.skip_group();
                return vec![];
            }
            assert!(send_rate_limiter.take_exact_tokens(usize::from(parity_count), now));
            if FEC_DEBUG {
                eprintln!(
                    "FEC: flushing {parity_count} parities for in-stream group of {data_count}"
                );
            }
            self.stats.groups_flushed.fetch_add(1, Ordering::Relaxed);
            inc_hist(&self.stats.group_size_flushed, data_count);
            let mut parity_encoder = self.encoder.flush_parities(parity_count);
            let mut pkts = vec![];
            while let Some(n) = parity_encoder.encode_parity(&mut self.enc_buf) {
                pkts.push(self.enc_buf[..n].to_vec());
            }
            self.stats
                .parity_sent
                .fetch_add(pkts.len(), Ordering::Relaxed);
            return pkts;
        }
        // Stock path: groups above `PARITY_DATA_THRESHOLD` are skipped so
        // parity never impacts throughput of big traffic.  When `instream` is
        // `true`, multi-symbol groups (>= 2) are already handled by the
        // instream branch above; the only instream group that reaches here is
        // a single-symbol group with `small_group_parity_count <= 1`, which
        // falls through to the stock 1:4 parity.  A group above
        // `INSTREAM_DATA_PER_GROUP` should never reach here (the inline flush
        // resets it at 8), but defensively skip it.
        if (!instream && data_count > PARITY_DATA_THRESHOLD)
            || (instream && data_count > INSTREAM_DATA_PER_GROUP)
        {
            self.encoder.skip_group();
            return vec![];
        }
        let parity_count = parity_for(data_count);

        let available_tokens = send_rate_limiter.gen_tokens(now);
        let parity_budget = available_tokens / PARITY_BUDGET_DEN;
        if usize::from(parity_count) > parity_budget {
            self.stats
                .groups_skipped_no_surplus_tokens
                .fetch_add(1, Ordering::Relaxed);
            inc_hist(&self.stats.group_size_skipped_no_surplus_tokens, data_count);
            self.encoder.skip_group();
            return vec![];
        }
        assert!(send_rate_limiter.take_exact_tokens(usize::from(parity_count), now));
        if FEC_DEBUG {
            eprintln!("FEC: flushing {parity_count} parities for group of {data_count}");
        }
        self.stats.groups_flushed.fetch_add(1, Ordering::Relaxed);
        inc_hist(&self.stats.group_size_flushed, data_count);
        let mut parity_encoder = self.encoder.flush_parities(parity_count);
        let mut pkts = vec![];
        while let Some(n) = parity_encoder.encode_parity(&mut self.enc_buf) {
            pkts.push(self.enc_buf[..n].to_vec());
        }
        self.stats
            .parity_sent
            .fetch_add(pkts.len(), Ordering::Relaxed);
        pkts
    }

    /// Test-only accessor for the configured single-symbol interactive
    /// parity depth.
    #[cfg(test)]
    pub(crate) fn small_group_parity_count(&self) -> u8 {
        self.small_group_parity_count
    }

    /// Test-only accessor for the running parity-sent counter.
    #[cfg(test)]
    pub(crate) fn parity_sent(&self) -> usize {
        self.stats.parity_sent.load(Ordering::Relaxed)
    }
}

impl FecDecoderState {
    /// Feed an incoming raw UDP packet through the FEC decoder. Returns:
    /// - `Some(payload)` if the packet is a FEC data symbol — the payload is
    ///   the codec packet to pass to `decode()`.
    /// - `None` if the packet is a parity symbol (or undecodable) — recovered
    ///   data symbols are queued in `self.recovered` and should be drained via
    ///   `pop_recovered` before reading the next raw packet.
    pub fn decode(&mut self, pkt: &[u8]) -> Option<Vec<u8>> {
        if crate::debug::debug_send() {
            eprintln!(
                "[fec-decode] pkt_len={} encodable={}",
                pkt.len(),
                encodable_wire_pkt(pkt, self.symbol_size)
            );
        }
        if !encodable_wire_pkt(pkt, self.symbol_size) {
            self.stats
                .dropped_malformed_pkts
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let recovered_before = self.recovered.len();
        let decoder = &mut self.decoder;
        let recovered = &mut self.recovered;
        let unwound = {
            suppress_fec_decoder_panic_hook();
            IN_FEC_DECODE.with(|flag| flag.set(true));
            let _guard = FecDecodeGuard;
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                decoder.decode(pkt, |data| {
                    recovered.push_back(data.to_vec());
                })
            }))
        };
        let hdr_len = match unwound {
            Ok(hdr_len) => hdr_len,
            Err(_) => {
                self.stats
                    .dropped_fec_decoder_panics
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
        };
        let recovered = self.recovered.len() - recovered_before;
        if recovered != 0 {
            self.stats
                .recovered_symbols
                .fetch_add(recovered, Ordering::Relaxed);
        }
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

    #[cfg(test)]
    pub(crate) fn dropped_malformed_pkts(&self) -> usize {
        self.stats.dropped_malformed_pkts.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn dropped_fec_decoder_panics(&self) -> usize {
        self.stats
            .dropped_fec_decoder_panics
            .load(Ordering::Relaxed)
    }
}

impl FecStatsHandle {
    /// Number of codec payloads recovered by parity so far across both actor
    /// halves (shared counter).
    pub(crate) fn recovered_symbols(&self) -> usize {
        self.0.recovered_symbols.load(Ordering::Relaxed)
    }

    /// Typed cumulative FEC counters for one connection's metrics snapshot.
    pub(crate) fn metrics_counters(&self) -> crate::metrics::MetricsFecCounters {
        crate::metrics::MetricsFecCounters {
            parity_sent: self.0.parity_sent.load(Ordering::Relaxed) as u64,
            groups_flushed: self.0.groups_flushed.load(Ordering::Relaxed) as u64,
            flushed_group_sizes: group_size_buckets(&self.0.group_size_flushed),
            groups_skipped_no_surplus_tokens: self
                .0
                .groups_skipped_no_surplus_tokens
                .load(Ordering::Relaxed) as u64,
            no_surplus_group_sizes: group_size_buckets(
                &self.0.group_size_skipped_no_surplus_tokens,
            ),
            groups_skipped_burst_end: self
                .0
                .parity_groups_skipped_burst_end
                .load(Ordering::Relaxed) as u64,
            burst_end_group_sizes: group_size_buckets(&self.0.group_size_skipped_burst_end),
            groups_skipped_loss_gate: self.0.groups_skipped_loss_gate.load(Ordering::Relaxed)
                as u64,
            loss_gate_group_sizes: group_size_buckets(&self.0.group_size_skipped_loss_gate),
            groups_skipped_no_spare_capacity: self
                .0
                .groups_skipped_no_spare_capacity
                .load(Ordering::Relaxed) as u64,
            no_spare_capacity_group_sizes: group_size_buckets(
                &self.0.group_size_skipped_no_spare_capacity,
            ),
            recovered_symbols: self.0.recovered_symbols.load(Ordering::Relaxed) as u64,
            dropped_malformed_packets: self.0.dropped_malformed_pkts.load(Ordering::Relaxed) as u64,
            dropped_decoder_panics: self.0.dropped_fec_decoder_panics.load(Ordering::Relaxed)
                as u64,
        }
    }

    pub(crate) fn debug_print(&self) {
        if FEC_DEBUG {
            eprintln!("FEC stats: {}", self.0.snapshot());
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
fn inc_hist(hist: &[AtomicU64], idx: usize) {
    if let Some(count) = hist.get(idx) {
        count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Bucket a raw group-size histogram into the typed metrics buckets: exactly
/// one, two-to-four, five-to-seven, and the full in-stream group size.
fn group_size_buckets(hist: &[AtomicU64]) -> crate::metrics::MetricsFecGroupSizeBuckets {
    let count = |index| {
        hist.get(index)
            .map(|counter: &AtomicU64| counter.load(Ordering::Relaxed))
            .unwrap_or_default()
    };
    crate::metrics::MetricsFecGroupSizeBuckets {
        one: count(1),
        two_to_four: (2..=4).map(count).sum(),
        five_to_seven: (5..=7).map(count).sum(),
        full_eight: count(INSTREAM_DATA_PER_GROUP),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    /// A fresh `FecState` with a given interactive parity depth, sized for a
    /// large-MSS loopback path so single-symbol groups dominate.
    fn fec_state(symbol_size: usize, small_group_parity_count: u8) -> FecState {
        FecState::new(FecConfig {
            symbol_size,
            small_group_parity_count,
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
        let mut tb = TokenBucket::new(
            PosR::new(1.0).unwrap(),
            NonZeroUsize::new(usize::MAX).unwrap(),
            now,
        );
        // Force-fill then drain all tokens.
        let later = now + Duration::from_secs(1000);
        let _ = tb.gen_tokens(later);
        let drained = tb.take_at_most_tokens(usize::MAX, later);
        assert!(drained > 0, "bucket should have tokens to drain");
        // Now the bucket is empty; rate=1/s so it stays ~empty for the test.
        tb
    }

    /// A `TokenBucket` pre-filled with `rate` tokens (1 second at the given
    /// rate), for exercising the interactive-depth budget cap against a
    /// small-but-nonzero bucket.  Returns `(bucket, now)` so the caller
    /// queries the bucket at the same instant it was filled at.
    fn bucket_with_tokens(rate: f64, max_tokens: usize, now: Instant) -> (TokenBucket, Instant) {
        use core::num::NonZeroUsize;
        use core::time::Duration;
        use primitive::ops::float::PosR;
        let tb = TokenBucket::new(
            PosR::new(rate).unwrap(),
            NonZeroUsize::new(max_tokens).unwrap(),
            now,
        );
        let later = now + Duration::from_secs(1);
        let mut tb = tb;
        let _ = tb.gen_tokens(later);
        (tb, later)
    }

    /// The three condition-gate skip reasons must each be observable with
    /// the group size that was affected: the loss gate and the burst-end tail
    /// gate record destructive skips, the no-spare-capacity gate records a
    /// DEFERRAL (the group is held open so the parity survives a pending tail
    /// probe and flushes once capacity is spare); a successful flush records
    /// the flushed histogram.
    #[test]
    fn condition_gate_skip_reasons_are_observable_with_group_sizes() {
        let mut fec = fec_state(8192 - 11, 1);
        let data = b"skip me";
        let mut sym_buf = vec![0u8; 8192];

        // A single-symbol group skipped by the loss gate.
        fec.encoder.encode_data(data, &mut sym_buf, false);
        fec.encoder.skip_open_group_loss_gate();
        let snapshot = fec.encoder.stats.snapshot();
        assert_eq!(snapshot.groups_skipped_loss_gate, 1);
        assert_eq!(
            snapshot.group_size_skipped_loss_gate[1], 1,
            "the loss-gate skip must record the 1-symbol group size"
        );

        // A two-symbol group deferred by the no-spare-capacity gate: the
        // deferral is recorded, but the group is NOT destroyed — it stays
        // open so the budgeted flush emits its parity once capacity is
        // genuinely spare (e.g. a pending tail probe resolves).
        fec.encoder.encode_data(data, &mut sym_buf, false);
        fec.encoder.encode_data(data, &mut sym_buf, false);
        fec.encoder.skip_open_group_no_spare_capacity();
        let snapshot = fec.encoder.stats.snapshot();
        assert_eq!(snapshot.groups_skipped_no_spare_capacity, 1);
        assert_eq!(
            snapshot.group_size_skipped_no_spare_capacity[2], 1,
            "the no-spare-capacity deferral must record the 2-symbol group size"
        );
        assert_eq!(
            fec.encoder.encoder.group_data_count(),
            2,
            "the no-spare-capacity gate must defer (hold) the group, not skip it"
        );

        // The deferred group is flushed by the budgeted path once capacity is
        // available; a three-symbol group is then skipped by the burst-end
        // tail gate.
        let (mut tb, now) = unlimited_bucket(Instant::now());
        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, false);
        assert_eq!(
            pkts.len(),
            1,
            "the deferred 2-symbol group must flush 1 parity"
        );
        for _ in 0..3 {
            fec.encoder.encode_data(data, &mut sym_buf, false);
        }
        fec.encoder.skip_open_group();
        let snapshot = fec.encoder.stats.snapshot();
        assert_eq!(snapshot.groups_skipped_burst_end, 1);
        assert_eq!(
            snapshot.group_size_skipped_burst_end[3], 1,
            "the burst-end skip must record the 3-symbol group size"
        );

        // A successful single-symbol flush records the flushed histogram.
        fec.encoder.encode_data(data, &mut sym_buf, false);
        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, false);
        assert_eq!(pkts.len(), 1, "a 1-symbol group must emit 1 parity");
        let snapshot = fec.encoder.stats.snapshot();
        assert_eq!(snapshot.groups_flushed, 2);
        assert_eq!(
            snapshot.group_size_flushed[1], 1,
            "the flush must record the 1-symbol group size"
        );
    }

    /// A single-symbol group with `small_group_parity_count = 3` must emit
    /// exactly 3 parity copies once the token budget allows, and must never
    /// be destroyed by an empty bucket: on a near-empty bucket the group is
    /// HELD open (0 parity this pass) so the next send pass after the token
    /// refill emits the interactive parity; a plentiful bucket caps the depth
    /// at the stock 1/3 spare-budget share so a large configured depth never
    /// drains the bucket in one unpaced burst.
    #[test]
    fn single_symbol_group_parity_is_paced_by_the_token_budget() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 3);
        let mut tb = empty_bucket(now);

        // Encode one data symbol (single-symbol group).
        let data = b"hello interactive world";
        let mut sym_buf = vec![0u8; 8192];
        let _n = fec.encoder.encode_data(data, &mut sym_buf, false);
        assert_eq!(fec.encoder.encoder.group_data_count(), 1);

        // An empty bucket must HOLD the group, not destroy it: the floor
        // parity cannot be taken, so the group stays open and the next send
        // pass (after the token refill) emits it.
        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, false);
        assert_eq!(
            pkts.len(),
            0,
            "single-symbol group with an empty bucket must emit no parity this pass, got {}",
            pkts.len()
        );
        assert_eq!(fec.encoder.parity_sent(), 0);
        assert_eq!(
            fec.encoder.encoder.group_data_count(),
            1,
            "an empty bucket must hold the single-symbol group open, not skip it"
        );

        // Token refill (rate is 1 token/sec): the held group now flushes the
        // configured depth (capped by the 1/3 spare-budget share).
        let later = now + core::time::Duration::from_secs(1000);
        let pkts = fec.encoder.maybe_flush_parities(&mut tb, later, false);
        assert_eq!(
            pkts.len(),
            3,
            "single-symbol group at depth 3 must emit 3 parity copies after the refill, got {}",
            pkts.len()
        );
        assert_eq!(fec.encoder.parity_sent(), 3);

        // A large configured depth is capped by the 1/3 spare-budget share:
        // with only 9 available tokens the burst is 9/3 = 3 parities, never
        // the full depth and never the whole bucket (6 tokens stay for data).
        let mut fec = fec_state(8192 - 11, MAX_INTERACTIVE_PARITY_DEPTH);
        let (mut tb, now) = bucket_with_tokens(9.0, usize::MAX, now);
        let _n = fec.encoder.encode_data(data, &mut sym_buf, false);
        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, false);
        assert_eq!(
            pkts.len(),
            3,
            "depth must be capped at available/3 = 3 on a 9-token bucket, got {}",
            pkts.len()
        );
        assert_eq!(
            tb.gen_tokens(now),
            6,
            "the capped burst must leave the remaining 2/3 of tokens for data"
        );
    }

    /// A gate-closed no-spare-capacity flush (e.g. a pending tail-loss probe
    /// blocking `can_send_tail_fec`) must DEFER the open group's parity, not
    /// destroy it: after the gate reopens (the probe is acked or its timeout
    /// elapses), the same group's parity is still emitted by the budgeted
    /// flush path.
    #[test]
    fn a_pending_tail_probe_defers_instead_of_destroying_the_group_parity() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 1);
        let data = b"tail members";
        let mut sym_buf = vec![0u8; 8192];
        fec.encoder.encode_data(data, &mut sym_buf, false);
        fec.encoder.encode_data(data, &mut sym_buf, false);
        assert_eq!(fec.encoder.encoder.group_data_count(), 2);

        // The gate is closed (pending tail probe): the group must stay open.
        fec.encoder.skip_open_group_no_spare_capacity();
        assert_eq!(
            fec.encoder.encoder.group_data_count(),
            2,
            "the pending probe must defer the group, not destroy it"
        );
        assert_eq!(
            fec.encoder.parity_sent(),
            0,
            "nothing must be emitted while the probe is pending"
        );
        let snapshot = fec.encoder.stats.snapshot();
        assert_eq!(
            snapshot.groups_skipped_no_spare_capacity, 1,
            "the deferral must be recorded once"
        );

        // The probe resolves: capacity is spare again, the next flush attempt
        // emits the still-open group's parity.
        let (mut tb, later) = unlimited_bucket(now);
        let pkts = fec.encoder.maybe_flush_parities(&mut tb, later, false);
        assert_eq!(
            pkts.len(),
            1,
            "the deferred 2-symbol group must flush its stock parity after the probe resolves, got {}",
            pkts.len()
        );
        assert_eq!(fec.encoder.parity_sent(), 1);
    }

    /// The open group's data-symbol cap (`MAX_DATA_PER_GROUP`, documented as
    /// a forced-flush point but previously unenforced) is enforced inside
    /// `encode_data`: once a group reaches the cap (the transmission layer's
    /// per-pass close discipline failed to flush it), the group is
    /// force-flushed — its stock-ratio parity is queued for the budgeted
    /// delivery — and the next symbol starts a fresh group, so `symbol_id`
    /// never climbs past the peer's decoder max.
    #[test]
    fn encode_data_force_flushes_at_max_data_per_group() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 1);
        let data = b"payload";
        let mut sym_buf = vec![0u8; 8192];

        // instream=true so the PARITY_DATA_THRESHOLD force-skip is suppressed
        // and the group can legitimately accumulate to the cap.
        for _ in 0..MAX_DATA_PER_GROUP {
            fec.encoder.encode_data(data, &mut sym_buf, true);
        }
        assert_eq!(
            fec.encoder.encoder.group_data_count(),
            MAX_DATA_PER_GROUP,
            "the group must reach the cap before the forced flush"
        );

        // The next symbol triggers the forced flush: its parity is queued for
        // the budgeted delivery and the new symbol starts a fresh group.
        fec.encoder.encode_data(data, &mut sym_buf, true);
        assert_eq!(
            fec.encoder.encoder.group_data_count(),
            1,
            "the symbol after the cap must start a fresh group, got {}",
            fec.encoder.encoder.group_data_count()
        );

        // The capped group's parity (stock ratio: MAX_PARITY_PER_GROUP) is
        // delivered by the normal budgeted flush path.
        let (mut tb, later) = unlimited_bucket(now);
        let pkts = fec.encoder.maybe_flush_parities(&mut tb, later, true);
        assert_eq!(
            pkts.len(),
            MAX_PARITY_PER_GROUP,
            "the capped {MAX_DATA_PER_GROUP}-symbol group must flush {MAX_PARITY_PER_GROUP} parities, got {}",
            pkts.len()
        );
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
        fec.encoder.encode_data(data, &mut sym_buf, false);
        fec.encoder.encode_data(data, &mut sym_buf, false);
        assert_eq!(fec.encoder.encoder.group_data_count(), 2);

        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, false);
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
        fec.encoder.encode_data(data, &mut sym_buf, false);
        fec.encoder.encode_data(data, &mut sym_buf, false);
        assert_eq!(fec.encoder.encoder.group_data_count(), 2);

        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, false);
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
        fec.encoder.encode_data(data, &mut sym_buf, false);

        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, false);
        assert_eq!(
            pkts.len(),
            0,
            "single-symbol group at depth 1 with empty bucket must be skipped, got {}",
            pkts.len()
        );
    }

    /// `FecState::new` clamps a misconfigured `small_group_parity_count = 0`
    /// to 1 so the stock path always emits at least 1 parity.
    #[test]
    fn depth_zero_is_clamped_to_one() {
        let fec = fec_state(8192 - 11, 0);
        assert_eq!(fec.encoder.small_group_parity_count(), 1);
    }

    /// The flushed group-size histogram preserves which in-stream decision
    /// bucket each flush came from: a partial 5-symbol group lands in
    /// five-to-seven, a full 8-symbol group lands in full_eight, and the
    /// typed buckets collapse the raw histogram correctly.
    #[test]
    fn group_size_metrics_preserve_the_instream_decision_buckets() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 1);
        let (mut tb, now) = unlimited_bucket(now);
        let data = b"payload";
        let mut sym_buf = vec![0u8; 8192];

        // Partial in-stream group (5 symbols) flushed at burst end.
        for _ in 0..5 {
            fec.encoder.encode_data(data, &mut sym_buf, true);
        }
        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, true);
        assert_eq!(pkts.len(), INSTREAM_PARITY_PER_GROUP);
        let snapshot = fec.encoder.stats.snapshot();
        assert_eq!(snapshot.groups_flushed, 1);
        assert_eq!(
            snapshot.group_size_flushed[5], 1,
            "a 5-symbol in-stream flush must land in the five-to-seven bucket"
        );

        // Full in-stream group (8 symbols) flushed inline.
        for _ in 0..INSTREAM_DATA_PER_GROUP {
            fec.encoder.encode_data(data, &mut sym_buf, true);
        }
        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, true);
        assert_eq!(pkts.len(), INSTREAM_PARITY_PER_GROUP);
        let snapshot = fec.encoder.stats.snapshot();
        assert_eq!(snapshot.groups_flushed, 2);
        assert_eq!(
            snapshot.group_size_flushed[INSTREAM_DATA_PER_GROUP], 1,
            "a full in-stream flush must land in the full_eight bucket"
        );

        let buckets = group_size_buckets(&fec.encoder.stats.group_size_flushed);
        assert_eq!(buckets.one, 0);
        assert_eq!(buckets.two_to_four, 0);
        assert_eq!(
            buckets.five_to_seven, 1,
            "the 5-symbol flush must collapse into five_to_seven"
        );
        assert_eq!(
            buckets.full_eight, 1,
            "the 8-symbol flush must collapse into full_eight"
        );
    }

    #[test]
    fn depth_above_the_decoder_group_size_is_clamped_so_recovery_still_works() {
        use fec::de::FecDecoder;
        let symbol_size = 8192 - 11;
        let mut fec = fec_state(symbol_size, 40);
        assert_eq!(
            fec.encoder.small_group_parity_count(),
            MAX_INTERACTIVE_PARITY_DEPTH,
            "an over-deep request must be clamped to what the decoder accepts"
        );
        let (mut tb, now) = unlimited_bucket(Instant::now());
        let payload = vec![7u8; 32];
        let mut sym_buf = vec![0u8; 8192];
        fec.encoder.encode_data(&payload, &mut sym_buf, false);
        assert_eq!(fec.encoder.encoder.group_data_count(), 1);
        let parity_pkts = fec.encoder.maybe_flush_parities(&mut tb, now, false);
        assert_eq!(
            parity_pkts.len(),
            usize::from(MAX_INTERACTIVE_PARITY_DEPTH),
            "the flush must emit only parity the decoder will accept"
        );
        let mut decoder = FecDecoder::builder()
            .window_size(WINDOW_SIZE)
            .symbol_size(symbol_size)
            .max_group_size(MAX_GROUP_SIZE)
            .build();
        let mut recovered = vec![];
        for pkt in &parity_pkts {
            decoder.decode(pkt, |data| recovered.push(data.to_vec()));
        }
        assert!(
            !recovered.is_empty(),
            "an over-deep parity burst recovered nothing at all"
        );
        assert_eq!(
            recovered[0], payload,
            "the recovered symbol must be the dropped data symbol"
        );
    }

    // ---- In-stream group FEC tests ----

    /// A full in-stream group (8 data symbols) with a full bucket must emit
    /// exactly 4 parity symbols inline.  Mutation target: if the force-skip
    /// at `PARITY_DATA_THRESHOLD` is kept on the instream path (i.e.
    /// `encode_data` ignores the `instream` flag), the group never reaches 8
    /// symbols and this test fails (0 parity instead of 4).
    #[test]
    fn full_group_flushes_four_parities_inline_mid_burst() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 1);
        let (mut tb, now) = unlimited_bucket(now);

        // Encode 8 data symbols with instream=true (suppresses the
        // PARITY_DATA_THRESHOLD force-skip).
        let data = b"payload";
        let mut sym_buf = vec![0u8; 8192];
        for _ in 0..INSTREAM_DATA_PER_GROUP {
            fec.encoder.encode_data(data, &mut sym_buf, true);
        }
        assert_eq!(
            fec.encoder.encoder.group_data_count(),
            INSTREAM_DATA_PER_GROUP
        );

        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, true);
        assert_eq!(
            pkts.len(),
            INSTREAM_PARITY_PER_GROUP,
            "full in-stream group of {} data symbols must emit {} parities, got {}",
            INSTREAM_DATA_PER_GROUP,
            INSTREAM_PARITY_PER_GROUP,
            pkts.len()
        );
    }

    /// A partial multi-symbol in-stream group (5 data symbols) must flush
    /// `INSTREAM_PARITY_PER_GROUP` (4) parities at burst end, not the stock
    /// `parity_for(5)=2`.  Depth-4 parity on partial multi-symbol groups is
    /// the measured win for interactive messages (a 2048-byte message at
    /// default MSS is 2 data symbols; stock parity_for(2)=1 makes p50 worse).
    /// Mutation target: if the instream branch condition is reverted to
    /// `data_count == INSTREAM_DATA_PER_GROUP`, a 5-symbol group falls through
    /// to the stock path and emits `parity_for(5)=2` instead of 4.
    #[test]
    fn partial_instream_group_flushes_stock_parity_at_burst_end() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 1);
        let (mut tb, now) = unlimited_bucket(now);

        // Encode 5 data symbols with instream=true.  A stock path would
        // force-skip at 4, but instream suppresses that, so the group
        // reaches 5.  At burst end, `maybe_flush_parities(instream=true)`
        // must emit INSTREAM_PARITY_PER_GROUP=4 (not stock parity_for(5)=2).
        let data = b"payload";
        let mut sym_buf = vec![0u8; 8192];
        for _ in 0..5 {
            fec.encoder.encode_data(data, &mut sym_buf, true);
        }
        assert_eq!(fec.encoder.encoder.group_data_count(), 5);

        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, true);
        assert_eq!(
            pkts.len(),
            INSTREAM_PARITY_PER_GROUP,
            "partial in-stream group of 5 must flush {INSTREAM_PARITY_PER_GROUP} parities, got {}",
            pkts.len()
        );
    }

    /// A full in-stream group with an empty bucket must be skipped (0
    /// parity) — the budget gate is NOT bypassed for multi-symbol groups.
    /// Mutation target: if the budget check is skipped for instream groups,
    /// this test fails (4 parity instead of 0).
    #[test]
    fn full_group_budget_exhaustion_suppresses_parity() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 1);
        let mut tb = empty_bucket(now);

        let data = b"payload";
        let mut sym_buf = vec![0u8; 8192];
        for _ in 0..INSTREAM_DATA_PER_GROUP {
            fec.encoder.encode_data(data, &mut sym_buf, true);
        }
        assert_eq!(
            fec.encoder.encoder.group_data_count(),
            INSTREAM_DATA_PER_GROUP
        );

        let pkts = fec.encoder.maybe_flush_parities(&mut tb, now, true);
        assert_eq!(
            pkts.len(),
            0,
            "full in-stream group with empty bucket must be skipped (0 parity), got {}",
            pkts.len()
        );
    }

    /// With instream=false (toggle off), the force-skip at
    /// `PARITY_DATA_THRESHOLD` fires, so a group never exceeds 4 data
    /// symbols.  Encoding 8 symbols with instream=false produces a group of
    /// at most 4 (the rest are force-skipped into new groups).  This proves
    /// the toggle-off path is byte-identical to stock.
    #[test]
    fn toggle_off_keeps_threshold_force_skip() {
        let now = Instant::now();
        let mut fec = fec_state(8192 - 11, 1);
        let (_tb, _now) = unlimited_bucket(now);

        let data = b"payload";
        let mut sym_buf = vec![0u8; 8192];
        for _ in 0..8 {
            fec.encoder.encode_data(data, &mut sym_buf, false);
        }
        // Stock force-skip at PARITY_DATA_THRESHOLD=4 means the group never
        // exceeds 4.  After 8 encode_data calls, the open group has at most
        // 4 symbols (the first 4 were force-skipped into a closed group when
        // the 5th was encoded).
        assert!(
            fec.encoder.encoder.group_data_count() <= PARITY_DATA_THRESHOLD,
            "toggle off must keep the force-skip; group_data_count={} > {}",
            fec.encoder.encoder.group_data_count(),
            PARITY_DATA_THRESHOLD
        );
    }

    /// `group_data_full` returns true only when instream is true AND the
    /// group has reached `INSTREAM_DATA_PER_GROUP`.  Stock path (instream=
    /// false) always returns false.
    #[test]
    fn group_data_full_only_when_instream_and_full() {
        let mut fec = fec_state(8192 - 11, 1);
        let data = b"payload";
        let mut sym_buf = vec![0u8; 8192];

        // Empty group: never full.
        assert!(!fec.encoder.group_data_full(true));
        assert!(!fec.encoder.group_data_full(false));

        // Partial group (4 symbols): not full even with instream.
        for _ in 0..4 {
            fec.encoder.encode_data(data, &mut sym_buf, true);
        }
        assert!(!fec.encoder.group_data_full(true), "4 < 8 must not be full");
        assert!(!fec.encoder.group_data_full(false));

        // Full group (8 symbols): full only with instream.
        for _ in 0..4 {
            fec.encoder.encode_data(data, &mut sym_buf, true);
        }
        assert!(
            fec.encoder.group_data_full(true),
            "8 == 8 must be full (instream)"
        );
        assert!(
            !fec.encoder.group_data_full(false),
            "toggle off must never be full"
        );
    }

    /// Parity emitted by a full in-stream group (8 data + 4 parity) must
    /// recover a lost data symbol at the decoder.  This proves the 8+4 group
    /// fits the stock decoder (`MAX_GROUP_SIZE=25`, `WINDOW_SIZE=32`) without
    /// bumping either constant, and that the parity is wire-correct.
    #[test]
    fn parity_recovers_lost_data_symbol_in_group() {
        use fec::de::FecDecoder;
        use std::num::NonZeroU64;

        let symbol_size = 8192 - 11;
        let mut fec = fec_state(symbol_size, 1);
        let (mut tb, now) = unlimited_bucket(Instant::now());

        // Encode 8 distinct data symbols so we can identify which one was
        // recovered.  Each codec packet is a small unique payload.
        let payloads: Vec<Vec<u8>> = (0..INSTREAM_DATA_PER_GROUP)
            .map(|i| vec![i as u8; 32])
            .collect();
        let mut sym_buf = vec![0u8; 8192];
        let mut wire_data_pkts = vec![];
        for p in &payloads {
            let n = fec.encoder.encode_data(p, &mut sym_buf, true);
            wire_data_pkts.push(sym_buf[..n].to_vec());
        }
        assert_eq!(
            fec.encoder.encoder.group_data_count(),
            INSTREAM_DATA_PER_GROUP
        );

        // Flush 4 parities for the full group.
        let parity_pkts = fec.encoder.maybe_flush_parities(&mut tb, now, true);
        assert_eq!(parity_pkts.len(), INSTREAM_PARITY_PER_GROUP);

        // Feed 7 of 8 data symbols + all 4 parities to a stock decoder,
        // dropping data symbol #3 (simulating a loss mid-burst).
        let mut decoder = FecDecoder::builder()
            .window_size(NonZeroU64::new(WINDOW_SIZE.get()).unwrap())
            .symbol_size(symbol_size)
            .max_group_size(MAX_GROUP_SIZE)
            .build();
        let mut recovered = vec![];
        for (i, pkt) in wire_data_pkts.iter().enumerate() {
            if i == 3 {
                continue; // drop this one
            }
            decoder.decode(pkt, |data| recovered.push(data.to_vec()));
        }
        for pkt in &parity_pkts {
            decoder.decode(pkt, |data| recovered.push(data.to_vec()));
        }
        // The decoder must recover the missing data symbol (#3, all bytes
        // = 3).  With 4 parity packets, the decoder fires recovery once per
        // parity after enough symbols arrive, so we expect >= 1 recovery;
        // each recovery returns the same missing symbol.
        assert!(
            !recovered.is_empty(),
            "decoder must recover the lost data symbol from 8+4 group, got 0 recoveries"
        );
        assert!(
            recovered[0].iter().all(|&b| b == 3),
            "recovered symbol must be the dropped one (all bytes == 3), got {:?}",
            recovered[0]
        );
    }

    fn wire_pkt(
        group_id: u64,
        symbol_id: u8,
        data_count: u8,
        parity_count: u8,
        body: &[u8],
    ) -> Vec<u8> {
        let mut pkt = Vec::new();
        pkt.extend_from_slice(&group_id.to_be_bytes());
        pkt.push(symbol_id);
        pkt.push(data_count);
        if data_count != 0 {
            pkt.push(parity_count);
        }
        pkt.extend_from_slice(body);
        pkt
    }

    #[test]
    fn a_parity_header_no_encoder_could_emit_is_dropped() {
        let symbol_size = 1424 - 11;
        let body = vec![0u8; symbol_size];
        for (data_count, parity_count) in [(1u8, 0u8), (200, 200), (255, 255), (20, 6)] {
            let mut fec = fec_state(symbol_size, 1);
            let pkt = wire_pkt(0, 0, data_count, parity_count, &body);
            assert!(
                fec.decoder.decode(&pkt).is_none(),
                "parity header {data_count}+{parity_count} must be dropped"
            );
            assert_eq!(
                fec.decoder.dropped_malformed_pkts(),
                1,
                "parity header {data_count}+{parity_count} must be refused before the decoder"
            );
        }
        let mut fec = fec_state(symbol_size, 1);
        assert!(fec.decoder.decode(&wire_pkt(0, 20, 20, 5, &body)).is_none());
        assert_eq!(fec.decoder.dropped_malformed_pkts(), 0);
    }

    #[test]
    fn a_recovered_symbol_claiming_more_than_it_holds_is_dropped() {
        let symbol_size = 1424 - 11;
        let mut fec = fec_state(symbol_size, 1);
        let data0 = vec![0u8; symbol_size - fec::proto::DATA_SYMBOL_HDR_SIZE];
        let parity = vec![0xFFu8; symbol_size];
        assert!(fec.decoder.decode(&wire_pkt(0, 0, 0, 0, &data0)).is_some());
        assert!(fec.decoder.decode(&wire_pkt(0, 2, 2, 1, &parity)).is_none());
        assert_eq!(fec.decoder.dropped_fec_decoder_panics(), 1);
        while fec.decoder.pop_recovered().is_some() {}
    }

    use crate::testing::SplitMix64;

    /// Drives the production guarded `FecState::decode` with 50,000
    /// deterministic hostile datagrams (out-of-range group/symbol IDs, random
    /// data/parity counts, and truncated bodies) and asserts every decoded
    /// payload and recovered symbol stays within its bound. A third-party
    /// decoder panic is caught by the unwind guard and counted separately as
    /// `dropped_fec_decoder_panics` (asserted by the sibling
    /// recovered-symbol test), not folded into `dropped_malformed_pkts`. The
    /// guard still runs the process panic hook before returning, so a
    /// remotely reachable decoder panic can amplify stderr.
    #[test]
    fn a_hostile_datagram_never_escapes_the_fec_decoder() {
        const ROUNDS: usize = 50_000;
        let symbol_size = 1424 - fec_hdr_size();
        let mut rng = SplitMix64::new(0x5eed_0fec);
        let mut fec = fec_state(symbol_size, 1);
        let mut decoded = 0_usize;
        for round in 0..ROUNDS {
            let group_id = rng.below(4) as u64;
            let symbol_id = rng.below(MAX_GROUP_SIZE + 2) as u8;
            let (data_count, parity_count) = match rng.below(4) {
                0 => (0, 0),
                1 => (
                    1,
                    rng.below(MAX_INTERACTIVE_PARITY_DEPTH as usize) as u8 + 1,
                ),
                2 => (
                    INSTREAM_DATA_PER_GROUP as u8,
                    INSTREAM_PARITY_PER_GROUP as u8,
                ),
                _ => (rng.byte(), rng.byte()),
            };
            let body_len = match rng.below(4) {
                0 => symbol_size,
                _ => rng.below(symbol_size + 1),
            };
            let body: Vec<u8> = (0..body_len).map(|_| rng.byte()).collect();
            let mut pkt = wire_pkt(group_id, symbol_id, data_count, parity_count, &body);
            if rng.below(8) == 0 && !pkt.is_empty() {
                let n = rng.below(pkt.len());
                pkt.truncate(n);
            }
            if let Some(payload) = fec.decoder.decode(&pkt) {
                decoded += 1;
                assert!(
                    payload.len() <= pkt.len(),
                    "round {round}: a {}-byte packet yielded {} payload bytes",
                    pkt.len(),
                    payload.len()
                );
            }
            while let Some(recovered) = fec.decoder.pop_recovered() {
                assert!(
                    recovered.len() <= symbol_size,
                    "round {round}: recovered {} bytes from a {symbol_size}-byte symbol",
                    recovered.len()
                );
            }
        }
        assert!(
            decoded > 0,
            "no packet decoded in {ROUNDS} rounds; the generator went stale"
        );
    }

    /// Direct decoder hostile test: bypasses `FecState::decode`'s
    /// `catch_unwind` guard and drives the third-party `FecDecoder` directly,
    /// so any reachable decoder panic fails the test instead of being folded
    /// into a malformed-packet count.  `encodable_wire_pkt` is applied first —
    /// the production precondition — so failures are attributed only to inputs
    /// that can actually reach the decoder.
    #[test]
    fn a_guarded_hostile_datagram_never_panics_the_fec_decoder() {
        const ROUNDS: usize = 50_000;
        let symbol_size = 1424 - fec_hdr_size();
        let mut rng = SplitMix64::new(0xfec_5eed);
        let mut fec = fec_state(symbol_size, 1);
        let mut decodable = 0_usize;
        for round in 0..ROUNDS {
            let group_id = rng.below(4) as u64;
            let symbol_id = rng.below(MAX_GROUP_SIZE + 2) as u8;
            let (data_count, parity_count) = match rng.below(4) {
                0 => (0, 0),
                1 => (
                    1,
                    rng.below(MAX_INTERACTIVE_PARITY_DEPTH as usize) as u8 + 1,
                ),
                2 => (
                    INSTREAM_DATA_PER_GROUP as u8,
                    INSTREAM_PARITY_PER_GROUP as u8,
                ),
                _ => (rng.byte(), rng.byte()),
            };
            let body_len = match rng.below(4) {
                0 => symbol_size,
                _ => rng.below(symbol_size + 1),
            };
            let body: Vec<u8> = (0..body_len).map(|_| rng.byte()).collect();
            let mut pkt = wire_pkt(group_id, symbol_id, data_count, parity_count, &body);
            if rng.below(8) == 0 && !pkt.is_empty() {
                let n = rng.below(pkt.len());
                pkt.truncate(n);
            }
            if !encodable_wire_pkt(&pkt, symbol_size) {
                continue;
            }
            decodable += 1;
            // No catch_unwind here: a third-party decoder panic fails the test.
            if let Some(hdr_len) = fec.decoder.decoder.decode(&pkt, |recovered| {
                assert!(
                    recovered.len() <= symbol_size,
                    "round {round}: decoder recovered {} bytes from a {symbol_size}-byte symbol",
                    recovered.len()
                );
            }) {
                assert!(
                    hdr_len <= pkt.len(),
                    "round {round}: a {}-byte packet yielded a {hdr_len}-byte header",
                    pkt.len()
                );
            }
        }
        assert!(
            decodable * 4 > ROUNDS,
            "only {decodable}/{ROUNDS} generated packets passed the production guard; the generator went stale"
        );
    }
}
