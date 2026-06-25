use std::{collections::VecDeque, num::NonZeroU64, time::Instant};

use fec::{de::FecDecoder, en::FecEncoder};
use primitive::io::token_bucket::TokenBucket;

use crate::reliable_layer::CC_DATA_LOSS_RATE;

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

#[derive(Debug, Clone)]
pub struct FecConfig {
    pub symbol_size: usize,
}

/// Encapsulated FEC state owned by the transmission layer. The transmission
/// layer calls `encode_data` on each outgoing packet and `decode` on each
/// incoming raw packet, then `maybe_flush_parities` after the send burst.
///
/// No `Arc`, no spawned task, no async channel: the token bucket and loss rate
/// are read directly from the reliable layer the transmission layer already
/// holds, so FEC parity is rate-limited and loss-adaptive without any
/// cross-task coordination.
#[derive(Debug)]
pub struct FecState {
    encoder: FecEncoder,
    decoder: FecDecoder,
    /// Codec payloads recovered by parity, waiting to be fed to the reliable
    /// layer's `recv_pkts` path.
    recovered: VecDeque<Vec<u8>>,
    enc_buf: Vec<u8>,
    /// Most recent loss rate sampled from the reliable layer, in `[0, 1]`.
    loss_rate: Option<f64>,
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
            loss_rate: None,
        }
    }

    pub fn set_loss_rate(&mut self, loss_rate: Option<f64>) {
        self.loss_rate = loss_rate;
    }

    /// Wrap an outgoing codec packet with a FEC data-symbol header and return
    /// the wire bytes to send via `utp_write`. Also accumulates the symbol into
    /// the current FEC group; call `maybe_flush_parities` after the send burst
    /// to emit parity for the group.
    pub fn encode_data(&mut self, data: &[u8], out: &mut [u8]) -> usize {
        // Prevent the group from overflowing the u8 symbol_id space. The
        // encoder panics if group_data_count() exceeds u8::MAX, and the
        // decoder rejects symbols with symbol_id >= MAX_GROUP_SIZE. A
        // force-skip here is safe: groups this large are way past
        // PARITY_DATA_THRESHOLD and would be skipped by maybe_flush_parities
        // anyway.
        if self.encoder.group_data_count() >= MAX_DATA_PER_GROUP {
            self.encoder.skip_group();
        }
        self.encoder.encode_data(data, out)
    }

    /// Attempt to flush parities for the current group, rate-limited by the
    /// token bucket. Returns `(parity_pkts, total_bytes)` where each entry is
    /// a ready-to-send wire packet. If the bucket can't afford the full burst,
    /// the group is left untouched (deferred to the next call); if the group
    /// exceeds the threshold it is skipped.
    ///
    /// Reed-Solomon needs the complete parity set to reconstruct, so the full
    /// `parity_count` tokens are reserved atomically before encoding any.
    pub fn maybe_flush_parities(
        &mut self,
        send_rate_limiter: &mut TokenBucket,
        now: Instant,
    ) -> Vec<Vec<u8>> {
        let data_count = self.encoder.group_data_count();
        if data_count == 0 {
            return vec![];
        }
        if data_count > PARITY_DATA_THRESHOLD {
            self.encoder.skip_group();
            return vec![];
        }
        let parity_count = parity_for(data_count, self.loss_rate);
        if !send_rate_limiter.take_exact_tokens(parity_count as usize, now) {
            if FEC_DEBUG {
                eprintln!(
                    "FEC: deferring group of {data_count} ({parity_count} parities), bucket insufficient"
                );
            }
            return vec![];
        }
        if FEC_DEBUG {
            eprintln!("FEC: flushing {parity_count} parities for group of {data_count}");
        }
        let mut parity_encoder = self.encoder.flush_parities(parity_count);
        let mut pkts = vec![];
        while let Some(n) = parity_encoder.encode_parity(&mut self.enc_buf) {
            pkts.push(self.enc_buf[..n].to_vec());
        }
        pkts
    }

    /// Feed an incoming raw UDP packet through the FEC decoder. Returns:
    /// - `Some(payload)` if the packet is a FEC data symbol — the payload is
    ///   the codec packet to pass to `decode()`.
    /// - `None` if the packet is a parity symbol (or undecodable) — recovered
    ///   data symbols are queued in `self.recovered` and should be drained via
    ///   `pop_recovered` before reading the next raw packet.
    pub fn decode(&mut self, pkt: &[u8]) -> Option<Vec<u8>> {
        let hdr_len = self.decoder.decode(pkt, |data| {
            self.recovered.push_back(data.to_vec());
        });
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
}

/// Parity count for a group of `data_count` data symbols, scaled by the
/// observed loss rate. With no loss signal we fall back to the static 1:4
/// ratio; when the reliable layer reports high loss we add proportionally
/// more parity so FEC can recover losses before retransmission kicks in.
fn parity_for(data_count: usize, loss_rate: Option<f64>) -> u8 {
    let base = (data_count * PARITY_RATIO_NUM).div_ceil(PARITY_RATIO_DEN);
    let scale = loss_rate.map(parity_scale).unwrap_or(1.0);
    let p = (base as f64 * scale).round() as usize;
    p.clamp(1, MAX_PARITY_PER_GROUP).try_into().unwrap()
}

/// Multiplier applied to the base parity count as a function of the observed
/// loss rate. Stays at 1x while the channel is healthy and grows up to 4x as
/// loss approaches the reliable layer's backoff threshold, capping out to
/// avoid runaway overhead.
fn parity_scale(loss_rate: f64) -> f64 {
    if loss_rate <= 0.02 {
        return 1.0;
    }
    const MAX_SCALE: f64 = 4.0;
    let t = (loss_rate / CC_DATA_LOSS_RATE).clamp(0.0, 1.0);
    1.0 + (MAX_SCALE - 1.0) * t
}
