use core::num::NonZeroUsize;
use std::{io::IoSlice, path::PathBuf, sync::Mutex, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::ack::AckInterval;
use crate::delivery::frame::FrameMode;
use crate::io_err::IoErr;
use crate::sequence::InitialSequences;
use crate::traffic_shaping::redundancy::{fec::FecState, fec_tuning::FecTuning};

pub(crate) const PRINT_DEBUG_MSGS: bool = false;
pub(crate) const FEC_DEBUG: bool = false;
pub(crate) const BUF_SIZE: usize = 1024 * 64;

pub(crate) use crate::traffic_shaping::control::ack_flush::MAX_NUM_ACK;

pub(crate) type ReliableLayerLogger = Mutex<csv::Writer<std::fs::File>>;

/// Reusable buffers for the send path. Allocated once and passed by `&mut`
/// to avoid per-call allocation.
#[derive(Debug)]
pub struct SendBufs {
    payload: Vec<u8>,
    codec_pkt: Vec<u8>,
    wire_pkt: Vec<u8>,
}

impl SendBufs {
    pub fn new() -> Self {
        Self {
            payload: vec![0; BUF_SIZE],
            codec_pkt: vec![0; BUF_SIZE],
            wire_pkt: vec![0; BUF_SIZE],
        }
    }

    pub(crate) fn parts_mut(&mut self) -> (&mut Vec<u8>, &mut Vec<u8>, &mut Vec<u8>) {
        (&mut self.payload, &mut self.codec_pkt, &mut self.wire_pkt)
    }

    pub(crate) fn wire_pkt_mut(&mut self) -> &mut Vec<u8> {
        &mut self.wire_pkt
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
    pub codec_pkt: Vec<u8>,
    pub ack_from_peer: Vec<AckInterval>,
    pub codec_pkts: Vec<Vec<u8>>,
}

impl RecvBufs {
    pub fn new() -> Self {
        Self {
            codec_pkt: vec![0; BUF_SIZE],
            ack_from_peer: vec![],
            codec_pkts: vec![],
        }
    }
}

impl Default for RecvBufs {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Debug)]
pub struct UnreliableLayer {
    pub(crate) utp_read: Box<dyn UnreliableRead>,
    pub(crate) utp_write: Box<dyn UnreliableWrite>,
    #[doc(hidden)]
    pub(crate) post_open_handshake:
        Option<crate::traffic_shaping::control::handshake::PostOpenHandshake>,
    /// Per-connection session tag that authenticates codec control-plane
    /// datagrams after the opening handshake.  `None` for connections opened
    /// without a handshake (no secret exists; the control plane stays
    /// unauthenticated).  Seeded from here into the shared [`Connection`].
    pub(crate) session_tag: Option<u64>,
    /// Handshake-derived directional initial sequences (`ZERO` for
    /// connections opened without the handshake).  Seeded from here into the
    /// shared [`Connection`] and from there into both `ReliableLayer`
    /// constructors.
    pub(crate) initial_sequences: InitialSequences,
    /// Opening-handshake RTT sample, measured only when the measured request
    /// succeeded on its first transmission (`None` after a retransmission or
    /// when the connection was opened without the handshake).  Seeds the
    /// reliable sender's recovery timing via `ReliableLayer::sample_rtt` at
    /// connection construction.
    pub(crate) initial_rtt: Option<Duration>,
    /// Optional typed transport-observation callback installed by the caller.
    pub(crate) metrics_observer: Option<crate::metrics::MetricsObserver>,
    pub(crate) mss: NonZeroUsize,
    pub(crate) fec: Option<FecState>,
    pub(crate) fec_tuning: FecTuning,
    pub(crate) frame_delivery: FrameMode,
    /// Retransmission-armor duplicate-copy toggle.  Set once at construction
    /// from the connect/accept config (which reads `RTP_RTX_DUP` in
    /// `Default`); the shared session state is seeded from here.
    pub(crate) rtx_dup: bool,
    /// In-stream group FEC toggle.  Set once at construction from the
    /// connect/accept config (which reads `RTP_INSTREAM_GROUP_FEC` in
    /// `Default`); the shared session state is seeded from here.
    pub(crate) instream_group_fec: bool,
}

#[derive(Debug, Clone)]
pub struct RecvPkts {
    pub num_ack_segments: usize,
    pub num_payload_segments: usize,
    pub num_fin_segments: usize,
}

#[derive(Debug, Clone)]
pub enum SendKillPkt {
    No,
}

#[async_trait]
pub trait UnreliableRead: core::fmt::Debug + Sync + Send + 'static {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr>;
}

#[async_trait]
pub trait UnreliableWrite: core::fmt::Debug + Send + 'static {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr>;
    async fn send_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize, IoErr> {
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
pub struct MetricsRow<'a> {
    pub schema_version: u16,
    pub event_index: u64,
    pub time: u128,
    pub elapsed_micros: u128,
    pub op: &'a str,
    pub raw_rtt_micros: Option<u128>,

    pub tokens: f64,
    pub send_rate: f64,
    pub loss_rate: Option<f64>,
    pub congestion_loss_rate: Option<f64>,
    pub congestion_action: Option<&'a str>,
    pub num_in_flight_pkts: usize,
    pub num_pkts_in_pipe: usize,
    pub num_rtx_active_pkts: usize,
    pub num_rtx_ready_pkts: usize,
    pub num_rtx_pkts: usize,
    pub send_seq: u64,
    pub min_rtt: Option<u128>,
    pub rtt: u128,
    pub retransmission_timeout_micros: u128,
    pub oldest_pipe_packet_age_micros: Option<u128>,
    pub maximum_packet_rto_overdue_micros: Option<u128>,
    pub rto_deadline_postponements: u64,
    pub cwnd: usize,
    pub num_rx_pkts: usize,
    pub recv_seq: Option<u64>,
    pub delivery_rate: Option<f64>,
    pub delivery_sample_app_limited: Option<bool>,
    pub application_write_waiters: usize,
    pub application_limited_detections: u64,
    pub application_limited_detections_suppressed_by_waiting_writer: u64,
    pub congestion_control_rtt_micros: Option<u128>,
    pub congestion_rtt_floor_micros: Option<u128>,
    pub congestion_queue_tolerance_micros: Option<u128>,
    pub congestion_persistent_queue_for_micros: Option<u128>,
    pub congestion_persistent_queue_resets: u64,
    pub congestion_delivery_peak_packets_per_second: Option<f64>,
    pub congestion_drain_floor_packets_per_second: Option<f64>,
    pub congestion_drain_target_packets_per_second: Option<f64>,
    pub congestion_rate_samples: u64,
    pub congestion_bandwidth_probe_decisions: u64,
    pub congestion_bandwidth_probe_increases: u64,
    pub congestion_bandwidth_probe_before_feedback: u64,
    pub congestion_last_bandwidth_probe_interval_micros: Option<u128>,
    pub congestion_delay_drains: u64,
    pub pending_send_bytes: usize,
    pub send_stage_capacity_bytes: usize,
    pub accepts_new_packet: bool,
    pub slow_start: bool,
    pub gentle_mode: bool,
    pub gentle_draining: bool,
    pub queue_building: bool,
    pub drain_floor_binding: bool,
    pub outage_recovery: bool,
    pub no_response_for_micros: Option<u128>,
    pub no_progress_for_micros: Option<u128>,
    pub stall_reason: Option<&'a str>,
}
