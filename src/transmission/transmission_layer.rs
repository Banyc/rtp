use core::{num::NonZeroUsize, time::Duration};
use std::{io::IoSlice, path::PathBuf, sync::Mutex, time::Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{fec::FecState, fec_tuning::FecTuning, ts_echo::TsEcho};
use crate::delivery::frame::FrameDelivery;
use crate::sack::AckBall;

pub use crate::send_queue::liveness::PeerStall;

pub(crate) const PRINT_DEBUG_MSGS: bool = false;
pub(crate) const FEC_DEBUG: bool = false;
pub(crate) const MAX_NUM_ACK: usize = 64;
pub(crate) const ACK_FLUSH_COUNT: usize = 8;
pub(crate) const ACK_FLUSH_AGE: Duration = Duration::from_millis(1);
pub(crate) const BUF_SIZE: usize = 1024 * 64;

/// Whether retransmission armor (`RTP_RTX_DUP`) is enabled at process
/// startup.  Reads `RTP_RTX_DUP` once; `1`/`true` enables it, anything
/// else preserves stock single-datagram behaviour byte-for-byte.
///
/// When enabled, the transmission layer emits a second identical copy of
/// every retransmit and tail-loss-probe datagram — reusing the exact
/// already-encoded symbol bytes (encode once, send twice).  The primary
/// repair datagram always sends (it bypasses the pacing token bucket as
/// today); the duplicate is skipped when the token bucket lacks tokens and
/// is charged to the bucket when sent, and is suppressed whenever the
/// delivery-rate congestion controller reports the bottleneck queue is
/// building.  Duplicating ordinary data packets is never done — the win is
/// specific to rare recovery packets.
pub(crate) fn rtx_dup_from_env() -> bool {
    match std::env::var("RTP_RTX_DUP") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
}

/// Whether in-stream group FEC (`RTP_INSTREAM_GROUP_FEC`) is enabled at
/// process startup.  Reads the env var once; `1`/`true` enables it, anything
/// else preserves stock behaviour byte-for-byte (parity is tail-only and
/// force-skipped at `PARITY_DATA_THRESHOLD`).
///
/// When enabled, the transmission layer suppresses the
/// `PARITY_DATA_THRESHOLD` force-skip in `encode_data` (passing
/// `instream = true`), so a data group may accumulate up to
/// `INSTREAM_DATA_PER_GROUP` (8) data symbols.  Right after each successful
/// data send, `maybe_flush_full_fec_group` emits
/// `INSTREAM_PARITY_PER_GROUP` (4) parity symbols inline mid-burst when the
/// group is full, gated on the spare-token budget.  At the data-path burst
/// close, a partial DATA group is force-flushed (regardless of the stock
/// `can_send_tail_fec` gate) so a burst ending mid-group still emits its
/// stock 1:4 parity.  ACK/kill bursts keep the stock tail gate untouched
/// (force-flushing ACK bursts tripled reverse-path packets for zero gain).
pub(crate) fn instream_group_fec_from_env() -> bool {
    match std::env::var("RTP_INSTREAM_GROUP_FEC") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
}

pub(crate) type ReliableLayerLogger = Mutex<csv::Writer<std::fs::File>>;

/// Reusable buffers for the send path. Allocated once and passed by `&mut`
/// to avoid per-call allocation.
#[derive(Debug)]
pub struct SendBufs {
    pub data: Vec<u8>,
    pub utp: Vec<u8>,
    pub fec: Vec<u8>,
}

impl SendBufs {
    pub fn new() -> Self {
        Self {
            data: vec![0; BUF_SIZE],
            utp: vec![0; BUF_SIZE],
            fec: vec![0; BUF_SIZE],
        }
    }
}

impl Default for SendBufs {
    fn default() -> Self {
        Self::new()
    }
}

/// Reusable buffers for the recv path. Allocated once and passed by `&mut`
/// to avoid per-call allocation.
#[derive(Debug)]
pub struct RecvBufs {
    pub utp: Vec<u8>,
    pub ack_from_peer: Vec<AckBall>,
    pub ack_to_peer: Vec<u64>,
    pub codec_pkts: Vec<Vec<u8>>,
}

impl RecvBufs {
    pub fn new() -> Self {
        Self {
            utp: vec![0; BUF_SIZE],
            ack_from_peer: vec![],
            ack_to_peer: vec![],
            codec_pkts: vec![],
        }
    }
}

impl Default for RecvBufs {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared ACK-flush state, accessed from both the recv path (records ACK
/// work) and the send path (flushes ACKs to the wire).  Protected by a
/// `Mutex` so the recv and send tasks can safely concurrent access it.
#[derive(Debug)]
pub(crate) struct AckFlushState {
    pub(crate) ts_echo: TsEcho,
    pub(crate) pending_acks: usize,
    pub(crate) fin_pending: bool,
    pub(crate) last_ack_flush: Option<Instant>,
    /// Resume offset for deep ack-history pages. Each flush sends cumulative
    /// page 0 plus one deep page starting here. Wrapped back to MAX_NUM_ACK on
    /// reset and when the cursor reaches the end of the history.
    pub(crate) ack_page_cursor: usize,
}

impl AckFlushState {
    pub(crate) fn new() -> Self {
        Self {
            ts_echo: TsEcho::new(),
            pending_acks: 0,
            fin_pending: false,
            last_ack_flush: None,
            ack_page_cursor: MAX_NUM_ACK,
        }
    }

    /// Subtract-claimed: decrement `pending_acks` by the number actually
    /// sent (clamped), and clear `fin_pending` only if the FIN was claimed
    /// and sent.  Never wholesale-clear so a WouldBlock/cancel leaves the
    /// remaining work intact for the next flush.
    pub(crate) fn complete_claim(&mut self, claimed_acks: usize, claimed_fin: bool) {
        self.pending_acks -= claimed_acks.min(self.pending_acks);
        if claimed_fin {
            self.fin_pending = false;
        }
    }
}
#[derive(Debug)]
pub struct UnreliableLayer {
    pub utp_read: Box<dyn UnreliableRead>,
    pub utp_write: Box<dyn UnreliableWrite>,
    #[doc(hidden)]
    pub post_open_handshake: Option<crate::handshake::PostOpenHandshake>,
    pub mss: NonZeroUsize,
    pub fec: Option<FecState>,
    pub fec_tuning: FecTuning,
    pub frame_delivery: FrameDelivery,
}

#[derive(Debug, Clone)]
pub struct RecvPkts {
    pub num_ack_segments: usize,
    pub num_payload_segments: usize,
    pub num_fin_segments: usize,
}

#[derive(Debug, Clone)]
pub enum SendKillPkt {
    Yes,
    No,
}

#[async_trait]
pub trait UnreliableRead: core::fmt::Debug + Sync + Send + 'static {
    /// Attempt to receive one datagram without creating a pending asynchronous
    /// I/O operation. `WouldBlock` means that no datagram is currently ready.
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind>;
    /// Receive one datagram.
    ///
    /// # Cancellation semantics
    ///
    /// Dropping a pending `recv` future must leave this reader valid for a
    /// subsequent `recv` and must not leave an operation referencing `buf`
    /// after the future is dropped. Cancellation may consume or lose a
    /// datagram: this is an unreliable transport, so packet preservation and
    /// gap-free delivery are deliberately not part of the contract.
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind>;
}

#[async_trait]
pub trait UnreliableWrite: core::fmt::Debug + Send + 'static {
    /// Send one datagram.
    ///
    /// This future is not required to be cancellation-safe. A caller that
    /// cancels it must drop this writer instead of using it for another send.
    async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind>;

    /// Send one datagram from a set of buffers (vectored / scatter-gather I/O).
    ///
    /// The default concatenates all non-empty buffers and delegates to
    /// [`send`](Self::send). Implementations that can issue a true
    /// vectored send (e.g. `sendmsg`) SHOULD override this to avoid the
    /// intermediate copy.
    async fn send_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize, std::io::ErrorKind> {
        match bufs.len() {
            0 => Ok(0),
            1 => self.send(&bufs[0]).await,
            _ => {
                let total: usize = bufs.iter().map(|b| b.len()).sum();
                let mut buf = Vec::with_capacity(total);
                for b in bufs {
                    buf.extend_from_slice(b);
                }
                self.send(&buf).await
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProactiveTerminationContext {
    pub(crate) reason: &'static str,
    pub(crate) no_response_for_ms: Option<u128>,
    pub(crate) no_progress_for_ms: Option<u128>,
    pub(crate) snapshot: String,
}

impl std::fmt::Display for ProactiveTerminationContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rtp_session_terminated trigger=proactive_stall reason={} no_response_for_ms={:?} no_progress_for_ms={:?} snapshot={}",
            self.reason, self.no_response_for_ms, self.no_progress_for_ms, self.snapshot
        )
    }
}

impl std::error::Error for ProactiveTerminationContext {}

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub reliable_layer_log_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log<'a> {
    pub time: u128,
    pub op: &'a str,

    pub tokens: f64,
    pub send_rate: f64,
    pub loss_rate: Option<f64>,
    pub num_tx_pkts: usize,
    pub num_pkts_in_pipe: usize,
    pub num_rt_pkts: usize,
    pub send_seq: u64,
    pub min_rtt: Option<u128>,
    pub rtt: u128,
    pub cwnd: usize,
    pub num_rx_pkts: usize,
    pub recv_seq: Option<u64>,
    pub delivery_rate: Option<f64>,
    pub app_limited: Option<bool>,
}
