use core::num::NonZeroUsize;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use dre::{ConnectionState, PacketState};
use primitive::{
    ops::{
        clear::Clear,
        float::{PosR, UnitR},
    },
    time::timer::Timer,
};
use serde::{Deserialize, Serialize};

use crate::io_err::IoErr;
use crate::metrics::MetricsGentleExitCause;
use crate::sequence::{InitialSequences, SequenceNumber};
use crate::{
    ack::AckBlocks,
    codec::data_overhead,
    delivery::{
        byte_stream::{recv::StockRecvStage, send::StockSendStage},
        frame::{
            FrameMode,
            send::{FrameSendStage, MAX_FRAME_LEN},
        },
    },
    recv_queue::pkt_recv_space::PktRecvSpace,
    traffic_shaping::core::{
        CongestionDecision, CongestionInput, CongestionResponse, GentleExitCause, ProbeKind,
        SendPacer, linear_backoff_step,
    },
    traffic_shaping::recovery::pkt_send_space::{CWND_SEND_RATE_SCALE, PktSendSpace},
    transmission::watchdog_tuning::WatchdogTuning,
};

const MAX_SEND_DATA_BUF_LEN: usize = 64 * 1024;
/// The frame-delivery [`MAX_FRAME_LEN`] is defined in
/// [`crate::delivery::frame::send`] and must stay equal to the stock
/// `MAX_SEND_DATA_BUF_LEN` so a frame can occupy the whole staging buffer.
const _: () = assert!(MAX_FRAME_LEN == MAX_SEND_DATA_BUF_LEN);

/// Cap on newly accepted staging bytes, in units of a pacing window.
///
/// Staged bytes are FIFO-committed via `batch_dequeue_extend`: once accepted
/// they cannot be reordered, the mux's LatencyControl cannot preempt them, and
/// the FIFO's latency cost scales inversely with the pace rate.  Capping
/// acceptance to a small pacing window keeps the latency of a small interactive
/// frame bounded even when the preceding bulk leaves the stage nearly full at a
/// low send rate.  After a rate spike occupancy may sit above the cap until it
/// drains; this is intentional and is why we gate acceptance, not eviction.
const STAGE_WINDOW_SECS: f64 = 0.005;
const SMOOTH_SEND_RATE_ALPHA: f64 = 0.4;
const MIN_SEND_RATE: f64 = 1.;
pub(crate) const INIT_SEND_RATE: f64 = 128.;

const MAX_DATA_LOSS_RATE: f64 = 0.9;
/// Pacing interval for a healthy huge-data-loss scan. A negative result is
/// cached for this long; a positive result must be recomputed on every call
/// because an ACK can repair the window between asynchronous sends.
const HUGE_DATA_LOSS_CHECK_INTERVAL: Duration = Duration::from_millis(10);
const PRINT_DEBUG_MSGS: bool = false;
const LINEAR_BACKOFF: bool = true;
fn metrics_gentle_exit_cause(cause: GentleExitCause) -> MetricsGentleExitCause {
    match cause {
        GentleExitCause::Loss => MetricsGentleExitCause::Loss,
        GentleExitCause::GateOpen => MetricsGentleExitCause::GateOpen,
        GentleExitCause::DrainGuard => MetricsGentleExitCause::DrainGuard,
        GentleExitCause::OutageReset => MetricsGentleExitCause::OutageReset,
    }
}

#[cfg(test)]
use crate::traffic_shaping::core::{
    DRAIN_FLOOR_PEAK_FRACTION, GENTLE_DRAIN_GAP_SHRINK, GENTLE_ENTER_RTTS, GENTLE_REENTRY_COOLDOWN,
    GENTLE_REENTRY_COOLDOWN_RTTS, PERSISTENT_QUEUE_RTTVAR_FACTOR, QUEUE_RTT_FACTOR,
    QUEUE_RTT_FLOOR, QUEUE_TOL_RTT_FRACTION, RTT_MIN_BUCKET, RTT_MIN_BUCKET_RTT_SCALE,
    WindowedRttMin,
};

#[derive(Debug, Clone)]
enum FinState {
    None,
    Pending,
    PendingBlocked,
}

/// Interval-state snapshot of the delivery-rate congestion controller, taken
/// only when `congestion_metrics_enabled` is set (an observer or logger
/// exists).  Everything here is derived from the controller's own inputs and
/// decisions - no extra timers or scans are run to produce it.
#[derive(Debug, Clone, Copy, Default)]
struct CongestionMetrics {
    application_limited_detections: u64,
    application_limited_detections_suppressed_by_waiting_writer: u64,
    control_rtt: Option<Duration>,
    rtt_floor: Option<Duration>,
    queue_tolerance: Option<Duration>,
    persistent_queue_for: Option<Duration>,
    persistent_queue_was_armed: bool,
    persistent_queue_resets: u64,
    delivery_peak_packets_per_second: Option<f64>,
    drain_floor_packets_per_second: Option<f64>,
    drain_target_packets_per_second: Option<f64>,
    loss_backoff_floor_packets_per_second: Option<f64>,
    loss_backoff_raw_target_packets_per_second: Option<f64>,
    loss_backoff_target_packets_per_second: Option<f64>,
    loss_backoffs: u64,
    loss_backoff_floor_bindings: u64,
    bandwidth_probe_decisions: u64,
    bandwidth_probe_increases: u64,
    rate_samples: u64,
    bandwidth_probe_before_feedback: u64,
    last_bandwidth_probe_increase_at: Option<Instant>,
    last_bandwidth_probe_interval: Option<Duration>,
    delay_drains: u64,
}

impl CongestionMetrics {
    fn clear_decision_gauges(&mut self) {
        self.control_rtt = None;
        self.rtt_floor = None;
        self.queue_tolerance = None;
        self.persistent_queue_for = None;
        self.delivery_peak_packets_per_second = None;
        self.drain_floor_packets_per_second = None;
        self.drain_target_packets_per_second = None;
        self.loss_backoff_floor_packets_per_second = None;
        self.loss_backoff_raw_target_packets_per_second = None;
        self.loss_backoff_target_packets_per_second = None;
    }

    fn start_congestion_epoch(&mut self) {
        self.clear_decision_gauges();
        self.persistent_queue_was_armed = false;
        self.last_bandwidth_probe_increase_at = None;
        self.last_bandwidth_probe_interval = None;
    }
}

/// A drop-scoped registration of one application writer blocked on send
/// capacity.  Holding one of these raises the exported
/// `application_write_waiters` counter; dropping it decrements the counter
/// (cancellation-safe: no writer ever suppresses application-limited
/// classification while waiting).
#[derive(Debug)]
pub(crate) struct ApplicationWriteWaiter {
    waiters: Arc<AtomicUsize>,
}

impl Drop for ApplicationWriteWaiter {
    fn drop(&mut self) {
        let previous = self.waiters.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "application write waiter counter underflow");
    }
}

#[derive(Debug)]
pub struct ReliableLayer {
    mss: NonZeroUsize,
    /// Cached `mss - data_overhead()`: the maximum payload bytes per data
    /// packet, computed once at construction instead of on every packetize.
    max_data_size_per_pkt: usize,
    send_data_buf: StockSendStage,
    send_fin_buf: FinState,
    recv_data_buf: StockRecvStage,
    recv_fin_buf: bool,
    send_rate_limiter: Arc<Mutex<SendPacer>>,
    /// Number of application writers currently blocked waiting for send
    /// capacity.  Registration is drop-scoped (`ApplicationWriteWaiter`), so
    /// a cancelled writer always releases its slot.
    application_write_waiters: Arc<AtomicUsize>,
    connection_stats: ConnectionState,
    pkt_send_space: PktSendSpace,
    pkt_recv_space: PktRecvSpace,
    send_rate: PosR<f64>,
    prev_sample_rate: Option<dre::RateSample>,
    huge_data_loss_timer: Timer,
    huge_data_loss_check_after: Instant,
    /// Single atomic owner of queue detection, delivered-peak tracking,
    /// ordinary probing, and drain-floor hysteresis (see
    /// [`CongestionResponse`]).
    congestion_response: CongestionResponse,
    slow_start: bool,
    slow_start_acked_pkts: usize,
    last_congestion_loss_ratio: Option<f64>,
    last_congestion_action: Option<crate::metrics::MetricsCongestionAction>,
    /// Whether congestion-controller interval accounting is on.  Set once at
    /// connection construction to `metrics_observer.is_some() ||
    /// log_config.is_some()`; when false the accounting hooks return before
    /// any timestamp arithmetic or counter mutation, so an absent observer
    /// and logger costs exactly one predictable branch per decision point.
    pub(crate) congestion_metrics_enabled: bool,
    /// One-shot transition signal consumed by the ACK owner while it still
    /// holds this layer's lock.  Kept out of the public snapshot so rare exit
    /// events do not enlarge every observation.
    gentle_exit_pending: Option<MetricsGentleExitCause>,
    congestion_metrics: CongestionMetrics,
    /// Set when `PktSendSpace::sample_rtt` closes an outage epoch; consumed by
    /// the next `recv_ack_pkt` so the same datagram's fresh retransmit sample
    /// is not mistaken for the start of a new outage epoch.
    outage_epoch_closed_at: Option<Instant>,
    frame_delivery: FrameMode,
    frame_send_stage: FrameSendStage,
    pkt_stats_buf: Vec<PacketState>,
    pkt_buf: Vec<dre::Packet>,
}

impl ReliableLayer {
    #[cfg(test)]
    pub fn new(
        mss: NonZeroUsize,
        frame_delivery: FrameMode,
        now: Instant,
    ) -> (Self, Arc<Mutex<SendPacer>>) {
        Self::new_at(mss, frame_delivery, now, InitialSequences::ZERO)
    }

    /// Construct with handshake-derived directional initial sequences:
    /// `PktSendSpace` starts at `initial_sequences.send` and `PktRecvSpace`
    /// at `initial_sequences.recv`.  The zero-seeded `new()` keeps skipped-
    /// handshake peers zero-compatible.
    pub fn new_at(
        mss: NonZeroUsize,
        frame_delivery: FrameMode,
        now: Instant,
        initial_sequences: InitialSequences,
    ) -> (Self, Arc<Mutex<SendPacer>>) {
        let send_rate = PosR::new(INIT_SEND_RATE).unwrap();
        let send_rate_limiter = Arc::new(Mutex::new(SendPacer::new_prefilled(send_rate, now)));
        let max_data_size_per_pkt = mss.get().checked_sub(data_overhead()).unwrap();
        let this = Self {
            mss,
            max_data_size_per_pkt,
            send_data_buf: StockSendStage::new(mss),
            send_fin_buf: FinState::None,
            recv_data_buf: StockRecvStage::new(),
            recv_fin_buf: false,
            send_rate_limiter: send_rate_limiter.clone(),
            application_write_waiters: Arc::new(AtomicUsize::new(0)),
            connection_stats: ConnectionState::new(now),
            pkt_send_space: PktSendSpace::new_at(initial_sequences.send),
            pkt_recv_space: PktRecvSpace::new_at(initial_sequences.recv),
            send_rate,
            prev_sample_rate: None,
            huge_data_loss_timer: Timer::new(),
            huge_data_loss_check_after: now,
            congestion_response: CongestionResponse::new(now),
            slow_start: true,
            slow_start_acked_pkts: 0,
            last_congestion_loss_ratio: None,
            last_congestion_action: None,
            congestion_metrics_enabled: false,
            gentle_exit_pending: None,
            congestion_metrics: CongestionMetrics::default(),
            outage_epoch_closed_at: None,
            frame_delivery,
            frame_send_stage: FrameSendStage::new(),
            pkt_stats_buf: Vec::new(),
            pkt_buf: Vec::new(),
        };
        (this, send_rate_limiter)
    }

    pub fn new_with_watchdog_tuning_at(
        mss: NonZeroUsize,
        frame_delivery: FrameMode,
        now: Instant,
        initial_sequences: InitialSequences,
        tuning: WatchdogTuning,
    ) -> (Self, Arc<Mutex<SendPacer>>) {
        let send_rate = PosR::new(INIT_SEND_RATE).unwrap();
        let send_rate_limiter = Arc::new(Mutex::new(SendPacer::new_prefilled(send_rate, now)));
        let max_data_size_per_pkt = mss.get().checked_sub(data_overhead()).unwrap();
        let this = Self {
            mss,
            max_data_size_per_pkt,
            send_data_buf: StockSendStage::new(mss),
            send_fin_buf: FinState::None,
            recv_data_buf: StockRecvStage::new(),
            recv_fin_buf: false,
            send_rate_limiter: send_rate_limiter.clone(),
            application_write_waiters: Arc::new(AtomicUsize::new(0)),
            connection_stats: ConnectionState::new(now),
            pkt_send_space: PktSendSpace::new_at(initial_sequences.send)
                .with_watchdog_tuning(tuning),
            pkt_recv_space: PktRecvSpace::new_at(initial_sequences.recv),
            send_rate,
            prev_sample_rate: None,
            huge_data_loss_timer: Timer::new(),
            huge_data_loss_check_after: now,
            congestion_response: CongestionResponse::new(now),
            slow_start: true,
            slow_start_acked_pkts: 0,
            last_congestion_loss_ratio: None,
            last_congestion_action: None,
            congestion_metrics_enabled: false,
            gentle_exit_pending: None,
            congestion_metrics: CongestionMetrics::default(),
            outage_epoch_closed_at: None,
            frame_delivery,
            frame_send_stage: FrameSendStage::new(),
            pkt_stats_buf: Vec::new(),
            pkt_buf: Vec::new(),
        };
        (this, send_rate_limiter)
    }

    /// Register one blocked application writer: raises the exported
    /// `application_write_waiters` counter for as long as the returned guard
    /// is alive.  The guard is drop-scoped, so a cancelled writer always
    /// releases its slot (see [`ApplicationWriteWaiter`]).
    pub(crate) fn application_write_waiter(&self) -> ApplicationWriteWaiter {
        self.application_write_waiters
            .fetch_add(1, Ordering::Relaxed);
        ApplicationWriteWaiter {
            waiters: Arc::clone(&self.application_write_waiters),
        }
    }

    /// Whether frame-delivery mode is enabled on this connection.
    pub fn frame_delivery_enabled(&self) -> bool {
        self.frame_delivery.enabled
    }

    pub fn is_no_data_to_send(&self) -> bool {
        self.is_send_buf_empty() && self.pkt_send_space.num_in_flight_pkts() == 0
    }

    pub fn is_send_buf_empty(&self) -> bool {
        let stock_empty = self.send_data_buf.is_empty()
            && matches!(self.send_fin_buf, FinState::None | FinState::PendingBlocked);
        if self.frame_delivery.enabled {
            // In frame mode the staging buffer is unused; the frame send
            // stage is the source of truth for unsent application bytes.
            stock_empty && !self.frame_send_stage.has_pending_frames()
        } else {
            stock_empty
        }
    }

    pub(crate) fn next_pacing_deadline(&self, now: Instant) -> Option<Instant> {
        let max_sendable_packets =
            if self.pkt_send_space.in_outage_recovery() && self.pkt_send_space.has_rtx(now) {
                self.pkt_send_space.num_in_flight_pkts().max(1)
            } else {
                let cwnd_headroom = self
                    .pkt_send_space
                    .cwnd()
                    .get()
                    .saturating_sub(self.pkt_send_space.num_in_flight_pkts());
                if cwnd_headroom == 0 {
                    0
                } else {
                    self.pending_new_packets().min(cwnd_headroom)
                }
            };
        if max_sendable_packets == 0 {
            return None;
        }
        Some(
            self.send_rate_limiter
                .lock()
                .unwrap()
                .next_batch_time(now, max_sendable_packets),
        )
    }

    fn pending_new_packets(&self) -> usize {
        let pending_bytes = if self.frame_delivery.enabled {
            self.frame_send_stage.pending_bytes()
        } else {
            self.send_data_buf.len()
        };
        let data_packets = pending_bytes.div_ceil(self.max_data_size_per_pkt());
        let pending_fin = usize::from(matches!(self.send_fin_buf, FinState::Pending));
        data_packets.saturating_add(pending_fin)
    }

    pub fn can_send_tail_fec(&self, now: Instant) -> bool {
        self.is_send_buf_empty()
            && self.pkt_send_space.accepts_new_pkt()
            && !self.pkt_send_space.has_rtx(now)
            // A due tail-loss probe is a pending (re)transmission, so the tail
            // is not settled and the tail parity must wait.
            && !self.pkt_send_space.has_tail_probe(now)
    }

    pub fn pkt_send_space(&self) -> &PktSendSpace {
        &self.pkt_send_space
    }

    pub fn pkt_recv_space(&self) -> &PktRecvSpace {
        &self.pkt_recv_space
    }

    /// Whether the delivery-rate congestion controller currently considers the
    /// bottleneck queue to be building (smooth RTT above the floor plus the
    /// gate tolerance).  Used by the transmission layer to suppress
    /// retransmission-armor duplicate copies under congestion.
    pub fn queue_building(&self) -> bool {
        self.congestion_response.queue_building()
    }

    /// Test-only: force the queue_building flag so the retransmission-armor
    /// duplicate-copy suppression gate can be exercised deterministically without
    /// having to drive a full delivery-rate sample sequence.
    #[cfg(test)]
    pub(crate) fn set_queue_building_for_test(&mut self, v: bool) {
        self.congestion_response.set_queue_building_for_test(v);
    }

    /// Test-only: directly enqueue `buf` into the send buffer, bypassing the
    /// staging-cap gate in `send_data_buf`.  Used by in-stream group FEC tests
    /// to stage many packets worth of data so a single `send_pkts` call emits
    /// a full 8-symbol group.
    #[cfg(test)]
    pub(crate) fn enqueue_send_data_for_test(&mut self, buf: &[u8]) {
        self.send_data_buf.stage(buf, usize::MAX);
    }

    /// Test-only: shrink the congestion window so the send loop stops after a
    /// fixed number of new packets, leaving the rest of the staged data in the
    /// send buffer.  This makes `is_send_buf_empty` false (and thus the stock
    /// `can_send_tail_fec` gate closed) without draining the token bucket, so
    /// the FEC parity flush still has spare tokens.  Used by in-stream group
    /// FEC tests that need the stock tail gate genuinely closed.  This is the
    /// single injectable cwnd seam; see `PktSendSpace::set_cwnd`.
    #[cfg(test)]
    pub(crate) fn set_cwnd_for_test(&mut self, cwnd: NonZeroUsize) {
        self.pkt_send_space.set_cwnd(cwnd);
    }

    pub fn sample_rtt(&mut self, rtt: Duration, now: Instant) {
        if self.pkt_send_space.sample_rtt(rtt, now) {
            self.outage_epoch_closed_at = Some(now);
        }
    }

    pub fn send_fin_buf(&mut self) {
        if matches!(self.send_fin_buf, FinState::PendingBlocked) {
            return;
        }
        self.send_fin_buf = FinState::Pending;
    }

    pub(crate) fn ensure_write_open(&self) -> Result<(), IoErr> {
        if matches!(self.send_fin_buf, FinState::None) {
            Ok(())
        } else {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
    }

    /// Store data in the inner data buffer
    #[cfg(test)]
    pub fn send_data_buf_capacity(&self) -> usize {
        self.send_data_buf.capacity()
    }

    /// Maximum application bytes in a single frame in frame-delivery mode.
    /// In stock (non-frame) mode this returns the byte-stream staging
    /// capacity (`send_data_buf_capacity`), preserving the existing
    /// `AsyncWriteAdapter` behavior.  In frame-delivery mode, the AsyncWrite
    /// adapter must cap each write at this value so one write = one frame
    /// (capping at the byte-stream stage cap would split a large frame
    /// into multiple wire frames, breaking the one-write-one-frame
    /// invariant).
    pub fn write_unit_capacity(&self) -> usize {
        if self.frame_delivery.enabled {
            MAX_FRAME_LEN
        } else {
            self.send_data_buf.capacity()
        }
    }

    pub fn send_data_buf(&mut self, buf: &[u8], now: Instant) -> Result<usize, IoErr> {
        self.ensure_write_open()?;
        self.detect_application_limited_phases(now);
        let stage_pkts = (self.send_rate.get() * STAGE_WINDOW_SECS).ceil() as usize;
        let cap =
            (stage_pkts.max(2) * self.max_data_size_per_pkt()).min(self.send_data_buf.capacity());
        Ok(self.send_data_buf.stage(buf, cap))
    }

    pub fn send_frame_buf(&mut self, frame: &[u8], now: Instant) -> Result<(), IoErr> {
        if !self.frame_delivery.enabled {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        self.ensure_write_open()?;
        crate::delivery::frame::send::validate_frame(frame)?;
        self.detect_application_limited_phases(now);
        let stage_pkts = (self.send_rate.get() * STAGE_WINDOW_SECS).ceil() as usize;
        let cap = (stage_pkts.max(2) * self.max_data_size_per_pkt()).min(MAX_FRAME_LEN);
        self.frame_send_stage.stage_frame(frame, cap)
    }

    /// Total bytes of pending (not yet fully packetized) frames in
    /// frame-delivery mode.  Zero when the mode is off.
    pub fn pending_frame_bytes(&self) -> usize {
        self.frame_send_stage.pending_bytes()
    }

    /// Move data from inner data buffer to inner packet space and return one of the packets if possible
    pub fn send_data_pkt(&mut self, pkt: &mut [u8], now: Instant) -> Option<DataPkt> {
        self.detect_application_limited_phases(now);

        if LINEAR_BACKOFF {
            self.backoff_on_huge_data_loss_linear(now);
        } else {
            self.backoff_on_huge_data_loss_exponential(now);
        }

        // Reconcile any deferred CC loss-events whose stock reorder-window
        // deadline has now elapsed (jitter-tolerant fast-retransmit path).
        // Runs every send tick so deadlines are honoured promptly.
        self.pkt_send_space.poll_deferred_loss(now);

        // During an outage-recovery epoch the whole pre-outage window is
        // immediately eligible for retransmission.  We must pace these
        // retransmits through the token bucket so a just-restored link is not
        // flooded, but they still bypass the new-packet cwnd gate because they
        // are already in flight.
        if self.pkt_send_space.in_outage_recovery()
            && self.pkt_send_space.has_rtx(now)
            && !self
                .send_rate_limiter
                .lock()
                .unwrap()
                .take_exact_tokens(1, now)
        {
            return None;
        }

        let no_packets_in_flight = self.pkt_send_space.no_pkts_in_flight();
        if let Some(p) = self.pkt_send_space.rtx_with_state(now, || {
            self.connection_stats
                .send_packet_2(now, no_packets_in_flight)
        }) {
            pkt[..p.data.len()].copy_from_slice(p.data);

            let data_written = NonZeroUsize::new(p.data.len())
                .map(DataPktPayload::Data)
                .unwrap_or(DataPktPayload::Fin);
            return Some(DataPkt {
                seq: p.seq,
                data_written,
                frame_len: p.frame_len,
                is_recovery: true,
            });
        }

        // Tail-loss probes also bypass the cwnd gate and token bucket: like
        // regular retransmits, they resend an already-in-flight packet and
        // must fire during tail silence to avoid waiting the full RTO.
        if self.is_send_buf_empty()
            && let Some(p) = self.pkt_send_space.tail_probe_with_state(now, || {
                self.connection_stats
                    .send_packet_2(now, no_packets_in_flight)
            })
        {
            pkt[..p.data.len()].copy_from_slice(p.data);

            let data_written = NonZeroUsize::new(p.data.len())
                .map(DataPktPayload::Data)
                .unwrap_or(DataPktPayload::Fin);
            return Some(DataPkt {
                seq: p.seq,
                data_written,
                frame_len: p.frame_len,
                is_recovery: true,
            });
        }

        // No retransmit or tail probe to send. From here on we are sending a
        // *new* packet, so the cwnd gate and the send-rate token bucket both
        // apply. Charge a token only when there is actually a new packet (or
        // a FIN) to send; an idle/cwnd-full call must not drain tokens,
        // otherwise we steal bandwidth from a future send and skew the rate
        // limiter.
        if !self.pkt_send_space.accepts_new_pkt() {
            return None;
        }

        if self.frame_delivery.enabled {
            return self.send_data_pkt_frame(pkt, now, no_packets_in_flight);
        }

        let pkt_bytes = pkt
            .len()
            .min(self.max_data_size_per_pkt())
            .min(self.send_data_buf.len());
        let pkt_bytes = match (NonZeroUsize::new(pkt_bytes), &self.send_fin_buf) {
            (Some(x), _) => x.get(),
            (None, FinState::Pending) => {
                self.send_fin_buf = FinState::PendingBlocked;
                0
            }
            (None, _) => return None,
        };

        // There is a new packet (or FIN) to send: take a token now.
        if !self
            .send_rate_limiter
            .lock()
            .unwrap()
            .take_exact_tokens(1, now)
        {
            // We have data/FIN to send but the rate limiter says not yet.
            // Restore the FIN buffer state so the FIN is retried later
            // instead of being permanently consumed.
            if pkt_bytes == 0 {
                self.send_fin_buf = FinState::Pending;
            }
            return None;
        }

        let stats = self
            .connection_stats
            .send_packet_2(now, no_packets_in_flight);

        let mut buf = self.pkt_send_space.reused_buf().take();
        self.send_data_buf.dequeue_extend(pkt_bytes, &mut buf);
        let data = buf;

        pkt[..data.len()].copy_from_slice(&data);
        let p = self.pkt_send_space.send(data, stats, None, now);

        let data_written = NonZeroUsize::new(pkt_bytes)
            .map(DataPktPayload::Data)
            .unwrap_or(DataPktPayload::Fin);
        Some(DataPkt {
            seq: p.seq,
            data_written,
            frame_len: None,
            is_recovery: false,
        })
    }

    /// Frame-delivery-mode new-packet path.  Staging is via the frame send
    /// stage ([`crate::delivery::frame::send`]); packetization is
    /// frame-aligned (a packet never carries bytes of two frames).  The
    /// first packet of a frame carries `Some(frame_len)`; the continuation
    /// packets carry `None`.  The `pkt` scratch buffer receives the payload
    /// bytes; the returned `DataPkt.frame_len` tells the transmission layer
    /// which codec command to emit.  `no_packets_in_flight` is the pre-send
    /// in-flight snapshot taken once by [`Self::send_data_pkt`] and reused
    /// for the DRE packet stats instead of re-scanning the send window.
    fn send_data_pkt_frame(
        &mut self,
        pkt: &mut [u8],
        now: Instant,
        no_packets_in_flight: bool,
    ) -> Option<DataPkt> {
        let normal_max_payload = self.max_data_size_per_pkt();
        let first_pkt_max_payload = self
            .mss
            .get()
            .checked_sub(crate::delivery::frame::wire::frame_data_overhead())
            .unwrap();

        let chunk = match self
            .frame_send_stage
            .next_chunk(first_pkt_max_payload, normal_max_payload)
        {
            Some(chunk) => Some(chunk),
            None => {
                if !matches!(self.send_fin_buf, FinState::Pending) {
                    return None;
                }
                None
            }
        };

        if !self
            .send_rate_limiter
            .lock()
            .unwrap()
            .take_exact_tokens(1, now)
        {
            return None;
        }

        let Some(chunk) = chunk else {
            self.send_fin_buf = FinState::PendingBlocked;
            let stats = self
                .connection_stats
                .send_packet_2(now, no_packets_in_flight);
            let buf = self.pkt_send_space.reused_buf().take();
            let p = self.pkt_send_space.send(buf, stats, None, now);
            return Some(DataPkt {
                seq: p.seq,
                data_written: DataPktPayload::Fin,
                frame_len: None,
                is_recovery: false,
            });
        };

        let stats = self
            .connection_stats
            .send_packet_2(now, no_packets_in_flight);

        let mut buf = self.pkt_send_space.reused_buf().take();
        let take_bytes = chunk.take_bytes;
        let frame_len = self.frame_send_stage.pop_chunk(chunk, &mut buf);

        pkt[..buf.len()].copy_from_slice(&buf);
        let p = self.pkt_send_space.send(buf, stats, frame_len, now);

        let data_written = NonZeroUsize::new(take_bytes)
            .map(DataPktPayload::Data)
            .unwrap_or(DataPktPayload::Fin);
        Some(DataPkt {
            seq: p.seq,
            data_written,
            frame_len,
            is_recovery: false,
        })
    }

    /// Take ACKs from the unreliable layer
    pub fn recv_ack_pkt(&mut self, recved: AckBlocks<'_>, now: Instant) -> Option<dre::RateSample> {
        if self.gentle_exit_pending.is_some() {
            self.gentle_exit_pending = None;
        }
        self.detect_application_limited_phases(now);

        // An ACK datagram can both close a freshly-closed outage epoch (its
        // RTT sample already ran) and open a new one.  The epoch-close
        // bookkeeping is consumed here so the fresh retransmit sample from
        // this same datagram is not mistaken for the start of a new outage
        // epoch.
        let closed_epoch_on_this_datagram = self.outage_epoch_closed_at.take() == Some(now);
        let entered_recovery =
            !closed_epoch_on_this_datagram && self.pkt_send_space.detect_outage_recovery(now);

        self.pkt_send_space
            .ack(recved, &mut self.pkt_stats_buf, now);

        // Reconcile deferred CC loss-events after the ack: any whose seq was
        // just acked has been cancelled, and any whose stock deadline has
        // elapsed should now be recorded (genuine loss).
        self.pkt_send_space.poll_deferred_loss(now);

        if entered_recovery {
            if let Some(cause) = self.congestion_response.reset(now) {
                self.gentle_exit_pending = Some(metrics_gentle_exit_cause(cause));
            }
            self.slow_start = false;
            self.slow_start_acked_pkts = 0;
            self.last_congestion_loss_ratio = None;
            self.set_send_rate(PosR::new(INIT_SEND_RATE).unwrap(), now);
            self.last_congestion_action =
                Some(crate::metrics::MetricsCongestionAction::OutageReset);
            // Congestion-epoch boundary: clear the controller interval state
            // so the post-outage path starts fresh (rate samples, probe
            // timing, and drain counters all restart at zero).
            if self.congestion_metrics_enabled {
                self.congestion_metrics.start_congestion_epoch();
            }
        }

        // ACK-clocked slow start must count *every* ACK, including those that
        // produce no usable rate sample (e.g. zero-RTT first echoes or sparse
        // bursts). The per-burst accumulator lives here, before the rate sample
        // is computed and cleared.
        if self.slow_start {
            if self.congestion_metrics_enabled {
                self.congestion_metrics.clear_decision_gauges();
            }
            self.slow_start_acked_pkts += self.pkt_stats_buf.len();
            let ss_rate = self.slow_start_acked_pkts as f64 / self.control_rtt().as_secs_f64();
            let ss_rate = PosR::new(ss_rate.max(self.send_rate.get())).unwrap();
            self.set_send_rate(ss_rate, now);
            self.last_congestion_action =
                Some(crate::metrics::MetricsCongestionAction::SlowStartAck);
        }

        // Per-episode accumulator: once the pipe drains, reset for the next
        // burst so slow-start cannot grow without bound on sparse flows.  This
        // runs on every ACK, independent of slow-start state.
        if self.pkt_send_space.no_pkts_in_flight() {
            self.slow_start_acked_pkts = 0;
        }

        self.update_rate_sample_on_ack(now)
    }
    pub(crate) fn take_gentle_mode_exit(&mut self) -> Option<MetricsGentleExitCause> {
        let pending = self.gentle_exit_pending;
        if pending.is_some() {
            self.gentle_exit_pending = None;
        }
        pending
    }

    fn update_rate_sample_on_ack(&mut self, now: Instant) -> Option<dre::RateSample> {
        while let Some(p) = self.pkt_stats_buf.pop() {
            self.pkt_buf.push(dre::Packet {
                state: p,
                data_length: 1,
            })
        }
        let Some(min_rtt) = self.pkt_send_space.min_rtt() else {
            return None;
        };
        let sr = self
            .connection_stats
            .sample_rate(&self.pkt_buf, now, min_rtt);
        self.pkt_stats_buf.clear();
        self.pkt_buf.clear();

        let Some(sr) = sr else {
            return None;
        };
        if PRINT_DEBUG_MSGS {
            println!("{sr:?}");
        }
        self.prev_sample_rate = Some(sr.clone());

        if let Some(cause) = self.on_rate_sample(&sr, now) {
            self.gentle_exit_pending = Some(metrics_gentle_exit_cause(cause));
        }
        Some(sr)
    }

    fn on_rate_sample(&mut self, sr: &dre::RateSample, now: Instant) -> Option<GentleExitCause> {
        if self.congestion_metrics_enabled {
            self.congestion_metrics.rate_samples =
                self.congestion_metrics.rate_samples.saturating_add(1);
        }
        // While an outage-recovery epoch is open, ignore rate samples whose prior
        // time predates the outage cut.  A blackout-spanning sample can report a
        // bogus delivery rate (~acked/outage-length) that would collapse the just-
        // restarted INIT_SEND_RATE back toward zero in the same ACK handler.
        if self
            .pkt_send_space
            .should_censor_rate_sample(sr.prior_time())
        {
            if self.congestion_metrics_enabled {
                self.congestion_metrics.clear_decision_gauges();
            }
            self.last_congestion_action =
                Some(crate::metrics::MetricsCongestionAction::CensoredOutageSample);
            return None;
        }

        let smooth = self.pkt_send_space.smooth_rtt();
        let loss_event_rate = self.pkt_send_space.loss_event_rate(now);
        self.last_congestion_loss_ratio = loss_event_rate;
        let control_rtt = self.control_rtt();
        let observation = self.congestion_response.observe(
            smooth,
            self.pkt_send_space.smooth_rtt_var(),
            loss_event_rate,
            sr.delivery_rate(),
            now,
            control_rtt,
        );
        self.record_congestion_interval_state(
            observation.floor,
            observation.tolerance,
            observation.persistent_for,
            observation.peak_delivery,
        );
        if self.slow_start {
            let probed = CongestionResponse::proposed_probe_rate(sr.delivery_rate());
            let caught_up = self.send_rate.get() <= probed;
            if observation.loss_blocks_delay_control
                || observation.queue_building
                || caught_up
                || sr.is_app_limited()
            {
                self.slow_start = false;
            }
        }
        let current = self.send_rate.get();
        let outcome = self.congestion_response.decide(
            observation,
            CongestionInput {
                delivery_rate: sr.delivery_rate(),
                current_rate: current,
                smooth_rtt: smooth,
                control_rtt,
                loss_event_rate,
                minimum_rate: MIN_SEND_RATE,
                initial_rate: INIT_SEND_RATE,
                now,
            },
        );
        match outcome.decision() {
            CongestionDecision::Hold => {
                self.last_congestion_action =
                    Some(crate::metrics::MetricsCongestionAction::QueueHold);
            }
            CongestionDecision::Probe { target } => match outcome.probe_kind() {
                ProbeKind::Gentle => {
                    self.last_congestion_action =
                        Some(crate::metrics::MetricsCongestionAction::GentleProbe);
                    self.set_smooth_send_rate(target, now);
                }
                ProbeKind::Bandwidth => {
                    self.last_congestion_action =
                        Some(crate::metrics::MetricsCongestionAction::BandwidthProbe);
                    self.record_bandwidth_probe(now, current, target);
                    self.set_smooth_send_rate(target, now);
                }
            },
            CongestionDecision::Drain { floor, target } => {
                self.last_congestion_action =
                    Some(crate::metrics::MetricsCongestionAction::DelayDrain);
                self.record_delay_drain(floor, target);
                match linear_backoff_step(
                    current,
                    target,
                    sr.interval(),
                    control_rtt,
                    CWND_SEND_RATE_SCALE,
                ) {
                    Some(new_rate) => self.set_send_rate(PosR::new(new_rate).unwrap(), now),
                    None => {
                        let send_rate = PosR::new(self.send_rate.get()).unwrap();
                        self.set_send_rate(send_rate, now);
                    }
                }
            }
            CongestionDecision::LossBackoff { raw, floor, target } => {
                if LINEAR_BACKOFF {
                    self.last_congestion_action =
                        Some(crate::metrics::MetricsCongestionAction::LossBackoff);
                    self.record_loss_backoff(raw, floor, target);
                    if let Some(new_rate) = linear_backoff_step(
                        current,
                        target,
                        sr.interval(),
                        control_rtt,
                        CWND_SEND_RATE_SCALE,
                    ) {
                        self.set_send_rate(PosR::new(new_rate).unwrap(), now);
                    }
                } else {
                    self.slow_start = false;
                    self.set_smooth_send_rate(sr.delivery_rate(), now);
                }
            }
        }
        outcome.gentle_exit()
    }

    fn set_smooth_send_rate(&mut self, target_send_rate: f64, now: Instant) {
        let smooth_send_rate = self.send_rate.get() * (1. - SMOOTH_SEND_RATE_ALPHA)
            + target_send_rate * SMOOTH_SEND_RATE_ALPHA;
        let send_rate = PosR::new(smooth_send_rate).unwrap();
        self.set_send_rate(send_rate, now);
    }

    /// Linear backoff on unrecovered huge data loss.
    fn backoff_on_huge_data_loss_linear(&mut self, now: Instant) {
        let Some(elapsed) = self.huge_data_loss_gate(now) else {
            return;
        };
        let control_rtt = self.control_rtt();
        let current = self.send_rate.get();
        let new_rate = linear_backoff_step(
            current,
            MIN_SEND_RATE,
            elapsed,
            control_rtt,
            CWND_SEND_RATE_SCALE,
        );
        let Some(new_rate) = new_rate else {
            return;
        };
        self.last_congestion_action =
            Some(crate::metrics::MetricsCongestionAction::HugeLossBackoff);
        self.set_send_rate(PosR::new(new_rate).unwrap(), now);
    }

    /// Original exponential backoff on unrecovered huge data loss.
    fn backoff_on_huge_data_loss_exponential(&mut self, now: Instant) {
        let Some(_) = self.huge_data_loss_gate(now) else {
            return;
        };
        self.last_congestion_action =
            Some(crate::metrics::MetricsCongestionAction::HugeLossBackoff);
        let send_rate = PosR::new(self.send_rate.get() / 2.).unwrap();
        self.set_send_rate(send_rate, now);
    }

    /// Shared gate for huge-data-loss backoff. Returns the elapsed time the
    /// loss has persisted once the `2 * RTO` threshold is reached.
    fn huge_data_loss_gate(&mut self, now: Instant) -> Option<Duration> {
        // Pace scans: a negative result is cached for the check interval,
        // and a positive result is also gated by the interval so the scan
        // cannot re-fire on every processing pass (an ACK can repair the
        // window by the next scan).
        if now < self.huge_data_loss_check_after {
            return None;
        }
        let huge_data_loss = self
            .pkt_send_space
            .huge_data_loss(UnitR::new(MAX_DATA_LOSS_RATE).unwrap(), now);
        if !huge_data_loss {
            self.huge_data_loss_timer.clear();
            self.huge_data_loss_check_after = now + HUGE_DATA_LOSS_CHECK_INTERVAL;
            return None;
        }
        self.huge_data_loss_check_after = now + HUGE_DATA_LOSS_CHECK_INTERVAL;
        let at_least_for = self.pkt_send_space.rto_duration().mul_f64(2.);
        let (set_off, elapsed) = self
            .huge_data_loss_timer
            .ensure_started_and_check(at_least_for, now);
        if !set_off {
            return None;
        }
        self.huge_data_loss_timer.clear();
        Some(elapsed)
    }

    /// Return `true` iff received FIN
    pub fn recv_fin_buf(&self) -> bool {
        self.recv_fin_buf
    }

    /// In frame-delivery mode, EOF is when a FIN is at the in-order head
    /// of the receive space (all preceding data has been delivered). In
    /// stock mode, EOF is when a FIN has been latched AND the byte receive
    /// buffer is empty.
    pub(crate) fn recv_eof_ready(&self) -> bool {
        if self.frame_delivery.enabled {
            self.recv_fin_buf() || self.pkt_recv_space.fin_at_head()
        } else {
            self.recv_fin_buf() && self.recv_data_buf.is_empty()
        }
    }

    /// Return data from the inner data buffer and inner packet space
    ///
    /// Return `0` does not mean it is FIN/EOF; you have to ask [`Self::recv_fin_buf()`].
    pub fn recv_data_buf(&mut self, buf: &mut [u8]) -> usize {
        let read_bytes = self.recv_data_buf.read(buf);
        self.move_recv_data();
        read_bytes
    }

    /// Take a pkt from the unreliable layer.
    ///
    /// Returns both ACK eligibility and whether this packet was newly inserted.
    /// Duplicate and stale packets remain ACKable without becoming new data.
    pub(crate) fn recv_data_pkt(
        &mut self,
        seq: SequenceNumber,
        frame_len: Option<u32>,
        pkt: &[u8],
    ) -> crate::recv_queue::pkt_recv_space::RecvDisposition {
        let disposition = self.pkt_recv_space.recv_bytes(seq, pkt, frame_len);
        if !disposition.should_ack() {
            return disposition;
        }
        if self.frame_delivery.enabled {
            return disposition;
        }
        self.move_recv_data();
        disposition
    }

    pub fn recv_frame_buf(&mut self) -> Result<Option<Vec<u8>>, IoErr> {
        if !self.frame_delivery.enabled {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        if let Some(frame) = self.pkt_recv_space.pop_complete_frame() {
            return Ok(Some(frame));
        }
        if self.pkt_recv_space.fin_at_head() {
            self.recv_fin_buf = true;
            return Ok(None);
        }
        if self.recv_fin_buf {
            return Ok(None);
        }
        Err(std::io::ErrorKind::WouldBlock.into())
    }

    /// Move data from pkt space to data buffer
    fn move_recv_data(&mut self) {
        if self.recv_fin_buf {
            return;
        }
        while let Some(p) = self.pkt_recv_space.peek() {
            if self.recv_data_buf.capacity() - self.recv_data_buf.len() < p.len() {
                return;
            }
            let p = self.pkt_recv_space.pop().unwrap();
            if p.is_empty() {
                self.recv_fin_buf = true;
                self.pkt_recv_space.reused_buf().put(p);
                return;
            }
            self.recv_data_buf.enqueue(&p);
            self.pkt_recv_space.reused_buf().put(p);
        }
    }

    fn detect_application_limited_phases(&mut self, now: Instant) {
        // DRE's application-limited predicate is conjunctive: if staged data
        // already fills a packet or the congestion window cannot accept a
        // packet, the outcome cannot change, so skip the send-window scan.
        let staged_bytes = if self.frame_delivery.enabled {
            self.pending_frame_bytes()
        } else {
            self.send_data_buf.len()
        };
        if staged_bytes >= self.max_data_size_per_pkt() || !self.pkt_send_space.accepts_new_pkt() {
            return;
        }
        let cwnd_stats = self.pkt_send_space.cwnd_stats(now);
        if self.congestion_metrics_enabled {
            self.congestion_metrics.application_limited_detections = self
                .congestion_metrics
                .application_limited_detections
                .saturating_add(1);
        }
        self.connection_stats.detect_application_limited_phases_2(
            dre::DetectAppLimitedPhaseParams {
                few_data_to_send: true,
                not_transmitting_a_packet: true,
                cwnd_not_full: true,
                all_lost_packets_retransmitted: cwnd_stats.all_lost_pkts_rtxed,
                pipe: cwnd_stats.num_not_lost_in_flight_pkts as u64,
            },
        );
    }

    fn set_send_rate(&mut self, send_rate: PosR<f64>, now: Instant) {
        // While an outage-recovery epoch is open every rate writer is clamped
        // to INIT_SEND_RATE until a fresh post-outage sample closes the
        // epoch: the pre-outage backlog must not be released at a stale high
        // rate onto a just-restored link.
        let send_rate = if self.pkt_send_space.in_outage_recovery() {
            PosR::new(INIT_SEND_RATE).unwrap()
        } else {
            send_rate
        };
        // Reapply the rate to the send space even when its numeric value did
        // not change: cwnd also depends on the latest RTT and outage state.
        // The pacer, however, needs no rebuild for an unchanged effective
        // rate, leaving it alone so it preserves its accumulated token state.
        self.pkt_send_space.set_send_rate(send_rate);
        let send_rate = PosR::new(MIN_SEND_RATE).unwrap().max(send_rate);
        if send_rate == self.send_rate {
            return;
        }
        self.send_rate = send_rate;
        let mut limiter = self.send_rate_limiter.lock().unwrap();
        limiter.set_rate(send_rate, now);
    }
}

/// Send staging buffer size for a given MSS.
///
/// For the default MSS we keep the historical 8 KiB staging buffer. For larger
/// MSS values we scale the buffer to whole packets so the send-data path never
/// refills with a sub-MSS remainder that would sit in the buffer indefinitely.
impl ReliableLayer {
    fn control_rtt(&self) -> Duration {
        self.pkt_send_space
            .smooth_rtt()
            .max(Duration::from_millis(5))
    }

    fn max_data_size_per_pkt(&self) -> usize {
        self.max_data_size_per_pkt
    }

    /// Record the controller's interval state (control RTT, RTT floor, queue
    /// gate tolerance, persistent-queue duration, delivery peak) plus one
    /// evaluated rate sample.  Costs one predictable branch - no timestamp
    /// arithmetic and no counter mutation - when no observer or logger exists.
    fn record_congestion_interval_state(
        &mut self,
        floor: Duration,
        tol: Duration,
        persistent_queue_for: Option<Duration>,
        peak_delivery: f64,
    ) {
        if !self.congestion_metrics_enabled {
            return;
        }
        let control_rtt = self.control_rtt();
        let metrics = &mut self.congestion_metrics;
        metrics.control_rtt = Some(control_rtt);
        metrics.rtt_floor = Some(floor);
        metrics.queue_tolerance = Some(tol);
        let persistent_queue_is_armed = persistent_queue_for.is_some();
        if metrics.persistent_queue_was_armed && !persistent_queue_is_armed {
            metrics.persistent_queue_resets = metrics.persistent_queue_resets.saturating_add(1);
        }
        metrics.persistent_queue_was_armed = persistent_queue_is_armed;
        metrics.persistent_queue_for = persistent_queue_for;
        metrics.delivery_peak_packets_per_second = Some(peak_delivery);
        metrics.drain_floor_packets_per_second = None;
        metrics.drain_target_packets_per_second = None;
        metrics.loss_backoff_floor_packets_per_second = None;
        metrics.loss_backoff_raw_target_packets_per_second = None;
        metrics.loss_backoff_target_packets_per_second = None;
    }

    /// Account a bandwidth-probe decision, saturating every counter.  An
    /// increase applied before the previous applied increase's feedback
    /// (one control RTT) elapsed is classified as before-feedback; the
    /// interval between the two increases is recorded either way.  Costs one
    /// predictable branch when no observer or logger exists.
    fn record_bandwidth_probe(&mut self, now: Instant, current: f64, target: f64) {
        if !self.congestion_metrics_enabled {
            return;
        }
        let control_rtt = self.control_rtt();
        let metrics = &mut self.congestion_metrics;
        metrics.bandwidth_probe_decisions = metrics.bandwidth_probe_decisions.saturating_add(1);
        if target <= current {
            return;
        }
        metrics.bandwidth_probe_increases = metrics.bandwidth_probe_increases.saturating_add(1);
        if let Some(previous) = metrics.last_bandwidth_probe_increase_at {
            let since = now.saturating_duration_since(previous);
            metrics.last_bandwidth_probe_interval = Some(since);
            if since < control_rtt {
                metrics.bandwidth_probe_before_feedback =
                    metrics.bandwidth_probe_before_feedback.saturating_add(1);
            }
        }
        metrics.last_bandwidth_probe_increase_at = Some(now);
    }

    /// Account a delay-drain decision with the drain floor and target that
    /// were applied.  Costs one predictable branch when no observer or
    /// logger exists.
    fn record_delay_drain(&mut self, drain_floor: f64, drain_target: f64) {
        if !self.congestion_metrics_enabled {
            return;
        }
        let metrics = &mut self.congestion_metrics;
        metrics.drain_floor_packets_per_second = Some(drain_floor);
        metrics.drain_target_packets_per_second = Some(drain_target);
        metrics.delay_drains = metrics.delay_drains.saturating_add(1);
    }

    /// Account a loss-backoff decision with the raw target, floor, and target
    /// that were applied.  Floor bindings (floor > raw) are counted separately
    /// so a loss response never reports a stale loss floor as evidence of
    /// capacity.  Costs one predictable branch when no observer or logger
    /// exists.
    fn record_loss_backoff(&mut self, raw: f64, floor: f64, target: f64) {
        if !self.congestion_metrics_enabled {
            return;
        }
        let metrics = &mut self.congestion_metrics;
        metrics.loss_backoff_floor_packets_per_second = Some(floor);
        metrics.loss_backoff_raw_target_packets_per_second = Some(raw);
        metrics.loss_backoff_target_packets_per_second = Some(target);
        metrics.loss_backoffs = metrics.loss_backoffs.saturating_add(1);
        if floor > raw {
            metrics.loss_backoff_floor_bindings =
                metrics.loss_backoff_floor_bindings.saturating_add(1);
        }
    }

    pub(crate) fn metrics_at(&self, now: Instant) -> crate::metrics::MetricsSnapshot {
        let send_window = self.pkt_send_space.send_window_observation(now);
        let retransmission_counters = self.pkt_send_space.retransmission_counters();
        let stall_reason = self
            .pkt_send_space
            .stall_reason(now)
            .map(|reason| match reason {
                crate::traffic_shaping::recovery::liveness::PeerStall::NoResponse => {
                    crate::metrics::MetricsStallReason::NoResponse
                }
                crate::traffic_shaping::recovery::liveness::PeerStall::NoProgress => {
                    crate::metrics::MetricsStallReason::NoProgress
                }
            });
        crate::metrics::MetricsSnapshot {
            pacer_tokens_packets: self.send_rate_limiter.lock().unwrap().outdated_tokens(),
            send_rate_packets_per_second: self.send_rate.get(),
            loss_ratio: send_window.loss_ratio,
            congestion_loss_ratio: self.last_congestion_loss_ratio,
            congestion_action: self.last_congestion_action,
            in_flight_packets: self.pkt_send_space.num_in_flight_pkts(),
            packets_in_pipe: send_window.packets_in_pipe,
            retransmission_active_packets: self.pkt_send_space.num_rtx_active_pkts(),
            retransmission_ready_packets: self.pkt_send_space.num_rtx_ready_pkts(),
            retransmitted_packets: send_window.retransmitted_packets,
            retransmission_counters: crate::metrics::MetricsRetransmissionCounters {
                attempts: retransmission_counters.attempts,
                first_attempts: retransmission_counters.first_attempts,
                repeat_attempts: retransmission_counters.repeat_attempts,
                rto_reason: retransmission_counters.rto_reason,
                reorder_reason: retransmission_counters.reorder_reason,
                fast_loss_reason: retransmission_counters.fast_loss_reason,
                pre_outage_reason: retransmission_counters.pre_outage_reason,
                tail_probes: retransmission_counters.tail_probes,
            },
            next_send_sequence: self.pkt_send_space.next_seq().to_wire(),
            minimum_rtt: self.pkt_send_space.min_rtt(),
            smoothed_rtt: self.pkt_send_space.smooth_rtt(),
            retransmission_timeout: self.pkt_send_space.rto_duration(),
            oldest_pipe_packet_age: send_window.oldest_pipe_packet_age,
            maximum_packet_rto_overdue: send_window.maximum_packet_rto_overdue,
            rto_deadline_postponements: self.pkt_send_space.rto_deadline_postponements(),
            congestion_window_packets: self.pkt_send_space.cwnd().get(),
            received_packets: self.pkt_recv_space.num_recved_pkts(),
            next_receive_sequence: self.pkt_recv_space.next_seq().map(|s| s.to_wire()),
            delivery_rate_packets_per_second: self
                .prev_sample_rate
                .as_ref()
                .map(|sr| sr.delivery_rate()),
            delivery_sample_app_limited: self
                .prev_sample_rate
                .as_ref()
                .map(|sample| sample.is_app_limited()),
            application_write_waiters: self.application_write_waiters.load(Ordering::Relaxed),
            application_limited_detections: self.congestion_metrics.application_limited_detections,
            application_limited_detections_suppressed_by_waiting_writer: self
                .congestion_metrics
                .application_limited_detections_suppressed_by_waiting_writer,
            congestion_control_rtt: self.congestion_metrics.control_rtt,
            congestion_rtt_floor: self.congestion_metrics.rtt_floor,
            congestion_queue_tolerance: self.congestion_metrics.queue_tolerance,
            congestion_persistent_queue_for: self.congestion_metrics.persistent_queue_for,
            congestion_persistent_queue_resets: self.congestion_metrics.persistent_queue_resets,
            congestion_delivery_peak_packets_per_second: self
                .congestion_metrics
                .delivery_peak_packets_per_second,
            congestion_drain_floor_packets_per_second: self
                .congestion_metrics
                .drain_floor_packets_per_second,
            congestion_drain_target_packets_per_second: self
                .congestion_metrics
                .drain_target_packets_per_second,
            congestion_loss_backoff_floor_packets_per_second: self
                .congestion_metrics
                .loss_backoff_floor_packets_per_second,
            congestion_loss_backoff_raw_target_packets_per_second: self
                .congestion_metrics
                .loss_backoff_raw_target_packets_per_second,
            congestion_loss_backoff_target_packets_per_second: self
                .congestion_metrics
                .loss_backoff_target_packets_per_second,
            congestion_loss_backoffs: self.congestion_metrics.loss_backoffs,
            congestion_loss_backoff_floor_bindings: self
                .congestion_metrics
                .loss_backoff_floor_bindings,
            congestion_rate_samples: self.congestion_metrics.rate_samples,
            congestion_bandwidth_probe_decisions: self.congestion_metrics.bandwidth_probe_decisions,
            congestion_bandwidth_probe_increases: self.congestion_metrics.bandwidth_probe_increases,
            congestion_bandwidth_probe_before_feedback: self
                .congestion_metrics
                .bandwidth_probe_before_feedback,
            congestion_last_bandwidth_probe_interval: self
                .congestion_metrics
                .last_bandwidth_probe_interval,
            congestion_delay_drains: self.congestion_metrics.delay_drains,
            pending_send_bytes: if self.frame_delivery.enabled {
                self.pending_frame_bytes()
            } else {
                self.send_data_buf.len()
            },
            send_stage_capacity_bytes: self.write_unit_capacity(),
            accepts_new_packet: self.pkt_send_space.accepts_new_pkt(),
            slow_start: self.slow_start,
            gentle_mode: self.congestion_response.gentle_mode(),
            gentle_draining: self.congestion_response.draining(),
            queue_building: self.congestion_response.queue_building(),
            drain_floor_binding: self.congestion_response.drain_floor_binding(),
            outage_recovery: self.pkt_send_space.in_outage_recovery(),
            no_response_for: self.pkt_send_space.no_resp_for(now),
            no_progress_for: self.pkt_send_space.no_progress_for(now),
            stall_reason,
        }
    }

    pub fn log(&self) -> MetricsRow {
        let now = Instant::now();
        let metrics = self.metrics_at(now);
        MetricsRow {
            tokens: metrics.pacer_tokens_packets,
            send_rate: metrics.send_rate_packets_per_second,
            delivery_rate: metrics.delivery_rate_packets_per_second,
            loss_rate: metrics.loss_ratio,
            congestion_loss_rate: metrics.congestion_loss_ratio,
            congestion_action: metrics
                .congestion_action
                .map(|action| action.as_str().to_owned()),
            num_in_flight_pkts: metrics.in_flight_packets,
            num_pkts_in_pipe: metrics.packets_in_pipe,
            num_rtx_active_pkts: metrics.retransmission_active_packets,
            num_rtx_ready_pkts: metrics.retransmission_ready_packets,
            num_rtx_pkts: metrics.retransmitted_packets,
            send_seq: metrics.next_send_sequence,
            min_rtt: metrics.minimum_rtt.map(|t| t.as_millis()),
            rtt: metrics.smoothed_rtt.as_millis(),
            retransmission_timeout: metrics.retransmission_timeout.as_millis(),
            oldest_pipe_packet_age: metrics.oldest_pipe_packet_age.map(|t| t.as_millis()),
            maximum_packet_rto_overdue: metrics.maximum_packet_rto_overdue.map(|t| t.as_millis()),
            rto_deadline_postponements: metrics.rto_deadline_postponements,
            cwnd: metrics.congestion_window_packets,
            num_rx_pkts: metrics.received_packets,
            recv_seq: metrics.next_receive_sequence,
            delivery_sample_app_limited: metrics.delivery_sample_app_limited,
            application_write_waiters: metrics.application_write_waiters,
            application_limited_detections: metrics.application_limited_detections,
            application_limited_detections_suppressed_by_waiting_writer: metrics
                .application_limited_detections_suppressed_by_waiting_writer,
            congestion_control_rtt: metrics
                .congestion_control_rtt
                .map(|value| value.as_micros()),
            congestion_rtt_floor: metrics.congestion_rtt_floor.map(|value| value.as_micros()),
            congestion_queue_tolerance: metrics
                .congestion_queue_tolerance
                .map(|value| value.as_micros()),
            congestion_persistent_queue_for: metrics
                .congestion_persistent_queue_for
                .map(|value| value.as_micros()),
            congestion_persistent_queue_resets: metrics.congestion_persistent_queue_resets,
            congestion_delivery_peak_packets_per_second: metrics
                .congestion_delivery_peak_packets_per_second,
            congestion_drain_floor_packets_per_second: metrics
                .congestion_drain_floor_packets_per_second,
            congestion_drain_target_packets_per_second: metrics
                .congestion_drain_target_packets_per_second,
            congestion_loss_backoff_floor_packets_per_second: metrics
                .congestion_loss_backoff_floor_packets_per_second,
            congestion_loss_backoff_raw_target_packets_per_second: metrics
                .congestion_loss_backoff_raw_target_packets_per_second,
            congestion_loss_backoff_target_packets_per_second: metrics
                .congestion_loss_backoff_target_packets_per_second,
            congestion_loss_backoffs: metrics.congestion_loss_backoffs,
            congestion_loss_backoff_floor_bindings: metrics.congestion_loss_backoff_floor_bindings,
            congestion_rate_samples: metrics.congestion_rate_samples,
            congestion_bandwidth_probe_decisions: metrics.congestion_bandwidth_probe_decisions,
            congestion_bandwidth_probe_increases: metrics.congestion_bandwidth_probe_increases,
            congestion_bandwidth_probe_before_feedback: metrics
                .congestion_bandwidth_probe_before_feedback,
            congestion_last_bandwidth_probe_interval: metrics
                .congestion_last_bandwidth_probe_interval
                .map(|value| value.as_micros()),
            congestion_delay_drains: metrics.congestion_delay_drains,
            pending_send_bytes: metrics.pending_send_bytes,
            send_stage_capacity_bytes: metrics.send_stage_capacity_bytes,
            accepts_new_packet: metrics.accepts_new_packet,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataPkt {
    pub seq: SequenceNumber,
    pub data_written: DataPktPayload,
    /// Application frame length this packet belongs to.  `Some` only for the
    /// first packet of a frame in frame-delivery mode; the transmission layer
    /// uses this to choose `FRAME_DATA_TS` vs `DATA_TS` on the wire.  `None`
    /// for continuation packets, all stock packets, FIN, and repairs of
    /// continuation packets.  Retransmits/tail-probes inherit the stored
    /// `InFlightPkt.frame_len` so the framing is preserved across repairs.
    pub frame_len: Option<u32>,
    /// Whether this packet is a retransmission or a tail-loss probe (a
    /// recovery packet, not new data).  Used by the transmission layer to
    /// decide whether to emit a retransmission-armor duplicate copy
    /// (`RTP_RTX_DUP`).
    pub is_recovery: bool,
}
#[derive(Debug, Clone)]
pub enum DataPktPayload {
    Data(NonZeroUsize),
    Fin,
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroU64;
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    use super::{
        DRAIN_FLOOR_PEAK_FRACTION, GENTLE_DRAIN_GAP_SHRINK, GENTLE_ENTER_RTTS,
        GENTLE_REENTRY_COOLDOWN, GENTLE_REENTRY_COOLDOWN_RTTS, HUGE_DATA_LOSS_CHECK_INTERVAL,
        INIT_SEND_RATE, MAX_SEND_DATA_BUF_LEN, MetricsGentleExitCause,
        PERSISTENT_QUEUE_RTTVAR_FACTOR, QUEUE_RTT_FACTOR, QUEUE_RTT_FLOOR, QUEUE_TOL_RTT_FRACTION,
        RTT_MIN_BUCKET, RTT_MIN_BUCKET_RTT_SCALE, WindowedRttMin,
    };
    use crate::delivery::byte_stream::send::send_data_buf_len;
    use primitive::ops::float::PosR;

    const SEND_DATA_BUF_LEN: usize = 8 * 1024;

    const TEST_MSS: usize = 1200;
    use crate::{
        ack::{AckBlocks, AckInterval},
        codec::data_overhead,
        udp::NO_FEC_MSS,
    };

    #[test]
    fn windowed_rtt_min_slides_and_forgets() {
        let now = std::time::Instant::now();
        let mut w = WindowedRttMin::new(now);

        // Bucket is max(5 s, 10 * rtt).  Use a 40 ms sample so bucket = 5 s.
        assert_eq!(
            w.update(now, Duration::from_millis(40)),
            Duration::from_millis(40)
        );
        assert_eq!(
            w.update(now, Duration::from_millis(100)),
            Duration::from_millis(40)
        );

        // First rotation: the previous bucket's 40 ms floor is still visible.
        let t1 = now + RTT_MIN_BUCKET + Duration::from_millis(1);
        assert_eq!(
            w.update(t1, Duration::from_millis(90)),
            Duration::from_millis(40)
        );
        assert_eq!(
            w.update(t1, Duration::from_millis(95)),
            Duration::from_millis(40)
        );

        // Second rotation: the stale floor has aged out.
        let t2 = t1 + RTT_MIN_BUCKET + Duration::from_millis(1);
        assert_eq!(
            w.update(t2, Duration::from_millis(95)),
            Duration::from_millis(90)
        );
        assert_eq!(
            w.update(t2, Duration::from_millis(110)),
            Duration::from_millis(90)
        );
    }

    #[test]
    fn windowed_rtt_min_bucket_scales_with_long_rtt() {
        let now = std::time::Instant::now();
        let mut w = WindowedRttMin::new(now);

        // With a 1 s sample the bucket should be 10 s, not the fixed 5 s floor.
        assert_eq!(
            w.update(now, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
        let t1 = now + Duration::from_secs(5) + Duration::from_millis(1);
        // Inside the 10 s bucket, the floor is still the first sample.
        assert_eq!(
            w.update(t1, Duration::from_millis(900)),
            Duration::from_millis(900)
        );

        // After >10 s of staleness both buckets clear, mirroring LossEventWindow.
        let t2 = now + Duration::from_secs(11);
        assert_eq!(
            w.update(t2, Duration::from_millis(800)),
            Duration::from_millis(800)
        );
    }

    #[test]
    fn windowed_rtt_min_clears_after_idle_gap() {
        let now = std::time::Instant::now();
        let mut w = WindowedRttMin::new(now);

        assert_eq!(
            w.update(now, Duration::from_millis(40)),
            Duration::from_millis(40)
        );
        // Idle for more than twice the 5 s bucket.
        let t1 = now + RTT_MIN_BUCKET * 2 + Duration::from_millis(1);
        assert_eq!(
            w.update(t1, Duration::from_millis(100)),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn send_data_buf_len_keeps_default_at_8_kib() {
        let mss = std::num::NonZeroUsize::new(NO_FEC_MSS).unwrap();
        assert_eq!(send_data_buf_len(mss), SEND_DATA_BUF_LEN);
    }

    #[test]
    fn send_data_buf_len_scales_to_whole_packets_above_default() {
        let mss = std::num::NonZeroUsize::new(8192).unwrap();
        let len = send_data_buf_len(mss);
        let payload = mss.get() - data_overhead();
        let expected = (MAX_SEND_DATA_BUF_LEN / payload) * payload;
        assert_eq!(len, expected);
        assert!(len > SEND_DATA_BUF_LEN);
        assert!(len <= MAX_SEND_DATA_BUF_LEN);

        // Spot checks for the larger-overhead wire format.
        let payload_2015 = 2015 - data_overhead();
        assert_eq!(
            send_data_buf_len(nz(2015)),
            (MAX_SEND_DATA_BUF_LEN / payload_2015) * payload_2015
        );
        let payload_9000 = 9000 - data_overhead();
        assert_eq!(
            send_data_buf_len(nz(9000)),
            (MAX_SEND_DATA_BUF_LEN / payload_9000) * payload_9000
        );

        // Sanity check for the default-MSS path.
        let default_mss = std::num::NonZeroUsize::new(NO_FEC_MSS).unwrap();
        assert_eq!(send_data_buf_len(default_mss), SEND_DATA_BUF_LEN);
    }

    fn nz(n: usize) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(n).unwrap()
    }

    fn test_layer(now: Instant) -> super::ReliableLayer {
        super::ReliableLayer::new(
            NonZeroUsize::new(TEST_MSS).unwrap(),
            crate::delivery::frame::FrameMode::default(),
            now,
        )
        .0
    }

    #[test]
    fn application_write_waiter_registration_is_drop_scoped() {
        let now = Instant::now();
        let layer = test_layer(now);
        assert_eq!(layer.metrics_at(now).application_write_waiters, 0);
        {
            let _first = layer.application_write_waiter();
            assert_eq!(layer.metrics_at(now).application_write_waiters, 1);
            {
                let _second = layer.application_write_waiter();
                assert_eq!(layer.metrics_at(now).application_write_waiters, 2);
            }
            assert_eq!(layer.metrics_at(now).application_write_waiters, 1);
        }
        assert_eq!(layer.metrics_at(now).application_write_waiters, 0);
    }

    #[test]
    fn huge_loss_checks_are_paced_including_positive_results() {
        let now = Instant::now();
        let mut layer = test_layer(now);
        assert!(layer.huge_data_loss_gate(now).is_none());
        assert_eq!(
            layer.huge_data_loss_check_after,
            now + HUGE_DATA_LOSS_CHECK_INTERVAL
        );
        layer.recv_ack_pkt(
            AckBlocks::new(crate::sequence::SequenceNumber::ZERO, &[]),
            now + Duration::from_millis(1),
        );
        assert_eq!(
            layer.huge_data_loss_check_after,
            now + HUGE_DATA_LOSS_CHECK_INTERVAL,
            "an ACK must not bypass the bounded huge-loss scan cadence"
        );
        assert!(
            layer
                .huge_data_loss_gate(now + HUGE_DATA_LOSS_CHECK_INTERVAL / 2)
                .is_none()
        );
        send_burst(&mut layer, 20, now);
        let lost_at = now + Duration::from_secs(2);
        assert!(layer.huge_data_loss_gate(lost_at).is_none());
        assert_eq!(
            layer.huge_data_loss_check_after,
            lost_at + HUGE_DATA_LOSS_CHECK_INTERVAL
        );
        let recheck_at = lost_at + Duration::from_millis(1);
        assert!(layer.huge_data_loss_gate(recheck_at).is_none());
        assert_eq!(
            layer.huge_data_loss_check_after,
            lost_at + HUGE_DATA_LOSS_CHECK_INTERVAL
        );
    }

    fn send_burst(rl: &mut super::ReliableLayer, n: usize, now: Instant) {
        let payload = vec![0u8; 100];
        let mut pkt = vec![0u8; TEST_MSS];
        for _ in 0..n {
            assert_eq!(
                rl.send_data_buf(&payload, now).unwrap(),
                payload.len(),
                "send_data_buf must accept the 100-byte payload"
            );
            assert!(
                rl.send_data_pkt(&mut pkt, now).is_some(),
                "send_data_pkt must send a packet"
            );
        }
    }

    fn send_max(rl: &mut super::ReliableLayer, now: Instant) -> usize {
        let payload_len = rl.max_data_size_per_pkt();
        let payload = vec![0u8; payload_len];
        let mut pkt = vec![0u8; TEST_MSS];
        let mut sent = 0;
        for _ in 0..20_000 {
            let free = rl.send_data_buf.capacity() - rl.send_data_buf.len();
            if free >= payload_len && rl.send_data_buf(&payload, now).unwrap() < payload.len() {
                break;
            }
            if rl.send_data_pkt(&mut pkt, now).is_none() {
                break;
            }
            sent += 1;
        }
        sent
    }

    fn ack_all(rl: &mut super::ReliableLayer, rtt: Option<Duration>, now: Instant) {
        let next_seq = rl.pkt_send_space().next_seq();
        if next_seq == crate::sequence::SequenceNumber::ZERO {
            return;
        }
        if let Some(rtt) = rtt {
            rl.sample_rtt(rtt, now);
        }
        let acks = [AckInterval {
            start: crate::sequence::SequenceNumber::ZERO,
            size: NonZeroU64::new(next_seq.to_wire()).unwrap(),
        }];
        rl.recv_ack_pkt(AckBlocks::new(next_seq, &acks), now);
    }

    fn send_one(rl: &mut super::ReliableLayer, now: Instant) -> crate::sequence::SequenceNumber {
        let payload = vec![0u8; 100];
        let mut pkt = vec![0u8; TEST_MSS];
        assert_eq!(
            rl.send_data_buf(&payload, now).unwrap(),
            payload.len(),
            "send_data_buf must accept the 100-byte payload"
        );
        let p = rl
            .send_data_pkt(&mut pkt, now)
            .expect("send_data_pkt must send");
        match p.data_written {
            super::DataPktPayload::Data(_) => p.seq,
            _ => panic!("expected data packet"),
        }
    }

    #[test]
    fn fin_latches_stock_and_frame_staging_closed() {
        let now = Instant::now();
        let mut stock = test_layer(now);
        stock.send_fin_buf();
        assert_eq!(
            stock.send_data_buf(b"after FIN", now),
            Err(std::io::ErrorKind::BrokenPipe.into())
        );
        let (mut frame, _) = super::ReliableLayer::new(
            NonZeroUsize::new(TEST_MSS).unwrap(),
            crate::delivery::frame::FrameMode::enabled(),
            now,
        );
        frame.send_fin_buf();
        assert_eq!(
            frame.send_frame_buf(b"after FIN", now),
            Err(std::io::ErrorKind::BrokenPipe.into())
        );
    }

    fn ack_seq(rl: &mut super::ReliableLayer, seq: u64, rtt: Duration, now: Instant) {
        let _ = ack_seq_observed(rl, seq, rtt, now);
    }
    fn ack_seq_observed(
        rl: &mut super::ReliableLayer,
        seq: u64,
        rtt: Duration,
        now: Instant,
    ) -> Option<MetricsGentleExitCause> {
        rl.sample_rtt(rtt, now);
        let acks = [AckInterval {
            start: crate::sequence::SequenceNumber::from_wire(seq),
            size: NonZeroU64::new(1).unwrap(),
        }];
        rl.recv_ack_pkt(
            AckBlocks::new(
                crate::sequence::SequenceNumber::from_wire(seq).advance(1),
                &acks,
            ),
            now,
        );
        rl.take_gentle_mode_exit()
    }

    /// Feed `count` identical RTT samples in rapid succession to converge the
    /// SRTT filter.  This lets tests establish a stable high RTT while the
    /// WindowedRttMin bucket is still the warm-up floor.
    fn feed_rtt(rl: &mut super::ReliableLayer, count: usize, rtt: Duration, start: Instant) {
        for i in 0..count {
            let t = start + Duration::from_micros(i as u64 * 100);
            rl.sample_rtt(rtt, t);
        }
    }

    fn try_send(rl: &mut super::ReliableLayer, n: usize, now: Instant) -> usize {
        let payload_len = rl.max_data_size_per_pkt();
        let payload = vec![0u8; payload_len];
        let mut pkt = vec![0u8; TEST_MSS];
        let mut sent = 0;
        for _ in 0..n {
            let free = rl.send_data_buf.capacity() - rl.send_data_buf.len();
            if free >= payload_len {
                rl.send_data_buf(&payload, now).unwrap();
            }
            if rl.send_data_pkt(&mut pkt, now).is_none() {
                break;
            }
            sent += 1;
        }
        sent
    }

    fn ack_prefix(rl: &mut super::ReliableLayer, hi: u64, rtt: Duration, now: Instant) -> bool {
        if hi == 0 {
            return false;
        }
        rl.sample_rtt(rtt, now);
        let acks = [AckInterval {
            start: crate::sequence::SequenceNumber::ZERO,
            size: NonZeroU64::new(hi).unwrap(),
        }];
        rl.recv_ack_pkt(
            AckBlocks::new(crate::sequence::SequenceNumber::from_wire(hi), &acks),
            now,
        )
        .is_some()
    }

    #[test]
    fn slow_start_survives_zero_rtt_first_echo() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);

        send_burst(&mut rl, 2, t0);
        ack_all(&mut rl, Some(Duration::ZERO), t0 + Duration::from_micros(1));

        assert!(
            rl.log().send_rate.is_finite(),
            "send_rate must not be inf/nan after zero-rtt first echo"
        );
    }

    #[test]
    fn slow_start_accumulator_is_per_episode() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);

        // First burst: 8 packets, acked after 40 ms.
        send_burst(&mut rl, 8, t0);
        let t1 = t0 + Duration::from_millis(40);
        ack_all(&mut rl, Some(Duration::from_millis(40)), t1);

        // Wait long enough to create a 1960 ms idle gap before the next episode.
        let mut t = t1 + Duration::from_millis(1960);

        // 20 rounds of 4 packets, each acked after 40 ms, then idle for 1960 ms.
        for _ in 0..20 {
            send_burst(&mut rl, 4, t);
            let ack_time = t + Duration::from_millis(40);
            ack_all(&mut rl, Some(Duration::from_millis(40)), ack_time);
            t = ack_time + Duration::from_millis(1960);
        }

        // A lifetime accumulator over all ~88 acked packets with a 40 ms control
        // RTT would read ~88/0.04 = 2200 packets/second.  With per-episode reset
        // it should stay bounded, around 2 * INIT_SEND_RATE.
        assert!(
            rl.log().send_rate <= 2.0 * INIT_SEND_RATE,
            "per-episode accumulator should keep slow-start bounded, got {}",
            rl.log().send_rate
        );
    }

    #[test]
    fn outage_restore_restarts_at_init_rate() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        // 50 rounds of filling cwnd and acking after 40 ms to warm up past
        // 2 * INIT_SEND_RATE.
        for _ in 0..50 {
            send_max(&mut rl, t);
            t += Duration::from_millis(40);
            ack_all(&mut rl, Some(Duration::from_millis(40)), t);
            t += Duration::from_nanos(1);
        }
        assert!(
            rl.log().send_rate > 2.0 * INIT_SEND_RATE,
            "warm-up rate must exceed 2 * INIT_SEND_RATE, got {}",
            rl.log().send_rate
        );

        // Send a new flight, then let it sit unacked for 10 s so the next ACK
        // triggers outage recovery without a fresh RTT echo.
        let _final_sent = send_max(&mut rl, t);
        let restore_time = t + Duration::from_secs(10);
        ack_all(&mut rl, None, restore_time);

        let final_rate = rl.log().send_rate;
        assert!(
            (final_rate - INIT_SEND_RATE).abs() < 1e-9,
            "outage restore should restart at exactly INIT_SEND_RATE, got {final_rate}"
        );
    }

    #[test]
    fn post_outage_retransmit_refreshes_delivery_rate_state() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        feed_rtt(&mut rl, 20, Duration::from_millis(100), t0);
        send_burst(&mut rl, 3, t0 + Duration::from_millis(10));
        ack_seq(
            &mut rl,
            0,
            Duration::from_millis(100),
            t0 + Duration::from_millis(110),
        );
        let outage_at = t0 + Duration::from_secs(3);
        rl.sample_rtt(Duration::from_millis(100), outage_at);
        let selective = [AckInterval {
            start: crate::sequence::SequenceNumber::from_wire(2),
            size: NonZeroU64::new(1).unwrap(),
        }];
        rl.recv_ack_pkt(
            AckBlocks::new(crate::sequence::SequenceNumber::from_wire(1), &selective),
            outage_at,
        );
        assert_eq!(
            rl.last_congestion_action,
            Some(crate::metrics::MetricsCongestionAction::OutageReset)
        );
        let retransmit_at = outage_at + Duration::from_millis(20);
        let mut packet = vec![0u8; TEST_MSS];
        let retransmit = rl
            .send_data_pkt(&mut packet, retransmit_at)
            .expect("pre-outage packet must retransmit immediately");
        assert!(retransmit.is_recovery);
        assert_eq!(retransmit.seq.to_wire(), 1);
        ack_seq(
            &mut rl,
            1,
            Duration::from_millis(100),
            retransmit_at + Duration::from_millis(100),
        );
        assert!(!rl.pkt_send_space.in_outage_recovery());
        assert_eq!(
            rl.last_congestion_action,
            Some(crate::metrics::MetricsCongestionAction::BandwidthProbe),
            "the fresh retransmit sample must reach congestion control"
        );
    }

    #[test]
    fn tail_probe_refreshes_delivery_rate_state() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        feed_rtt(&mut rl, 20, Duration::from_millis(100), t0);
        send_burst(&mut rl, 2, t0 + Duration::from_millis(10));
        ack_seq(
            &mut rl,
            0,
            Duration::from_millis(100),
            t0 + Duration::from_millis(110),
        );
        let probe_at = t0 + Duration::from_millis(260);
        let mut packet = vec![0u8; TEST_MSS];
        let probe = rl
            .send_data_pkt(&mut packet, probe_at)
            .expect("tail-loss probe must fire before the full RTO");
        assert!(probe.is_recovery);
        assert_eq!(probe.seq.to_wire(), 1);
        let ack_at = probe_at + Duration::from_millis(100);
        rl.sample_rtt(Duration::from_millis(100), ack_at);
        let selective = [AckInterval {
            start: crate::sequence::SequenceNumber::from_wire(1),
            size: NonZeroU64::new(1).unwrap(),
        }];
        let sample = rl
            .recv_ack_pkt(
                AckBlocks::new(crate::sequence::SequenceNumber::from_wire(2), &selective),
                ack_at,
            )
            .expect("fresh tail-probe state must yield a delivery-rate sample");
        assert_eq!(sample.prior_time(), t0 + Duration::from_millis(110));
    }

    #[test]
    fn capacity_drop_drain_follows_delivery_past_stale_peak_floor() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        // Ramp delivery at a brisk 10 ms RTT so the slow-start rate climbs
        // quickly.  send_max respects the cwnd limit, sending as many packets
        // as the cwnd and token bucket allow.  After one round the ACK-clocked
        // slow start pushes the rate well above 4× INIT_SEND_RATE; subsequent
        // rounds produce DRE rate samples that exit slow start and let the CC
        // probe branch take over.
        let ramp_rtt = Duration::from_millis(10);
        for _ in 0..40 {
            send_max(&mut rl, t);
            t += ramp_rtt;
            ack_all(&mut rl, Some(ramp_rtt), t);
        }
        let peak_rate = rl.log().send_rate;
        assert!(
            peak_rate > INIT_SEND_RATE * 4.0,
            "peak send rate {peak_rate} must exceed 4× INIT_SEND_RATE ({})",
            INIT_SEND_RATE * 4.0
        );
        assert!(
            rl.congestion_response.delivery_peak().update(t, 0.0) * DRAIN_FLOOR_PEAK_FRACTION
                > INIT_SEND_RATE,
            "DRE delivery peak * DRAIN_FLOOR_PEAK_FRACTION must exceed INIT_SEND_RATE, got {}",
            rl.congestion_response.delivery_peak().update(t, 0.0)
        );
        assert!(
            !rl.slow_start,
            "slow start must have exited during the ramp"
        );

        // Feed the drop-path RTT through `sample_rtt` so the SRTT and rttvar
        // filters settle before the first real send-ack cycle.
        let drop_rtt = Duration::from_millis(1000);
        feed_rtt(&mut rl, 30, drop_rtt, t);
        t += Duration::from_millis(1);

        // Capacity drops to a deep-buffered ~60 pkt/s path.  Inject a single
        // synthetic loss event so loss_event_rate returns a value and the
        // decay-grace clock can start; without this the blind-flow rule keeps
        // the floor at the full cap.  Keep flight shallow with spread-out
        // try_send so the DRE produces a meaningful interval for linear
        // backoff.  The inject_loss_event now uses smooth_rtt so the loss
        // entry persists long enough for deliveries to accumulate.
        rl.pkt_send_space.inject_loss_event(t);
        for _ in 0..10 {
            let round_start = t;
            for _ in 0..8 {
                try_send(&mut rl, 1, t);
                t += Duration::from_millis(100);
            }
            t = round_start + drop_rtt;
            let next_seq = rl.pkt_send_space().next_seq();
            ack_prefix(&mut rl, next_seq.to_wire(), drop_rtt, t);
        }

        // After ~10 s (3 s grace + 7 excess RTTs) the floor has decayed well
        // below 100 pkt/s and the send rate must follow.  The original
        // delivery peak is at most 10 s old (DELIVERY_PEAK_BUCKET = 10 s),
        // so it is still in the window and contributes a genuine cap — the
        // decay comes from the clocked grace path, not from the peak aging out.
        let final_rate = rl.log().send_rate;
        assert!(
            final_rate < 100.0,
            "send rate must decay below 100 pkt/s within 10 s, got {final_rate}"
        );
    }

    #[test]
    fn gentle_drain_guard_survives_rtt_floor_ratchet_and_scales_cooldown() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);

        // Settle the RTT floor around 600 ms by feeding long-RTT samples.
        let mut t = t0;
        for _ in 0..8 {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(700);
            ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(600), t);
        }
        assert!(
            rl.pkt_send_space.smooth_rtt() >= Duration::from_millis(500),
            "sRTT must settle high, got {:?}",
            rl.pkt_send_space.smooth_rtt()
        );

        // Quickly converge SRTT to 1 s while the snapshot window is still the
        // warm-up 600 ms floor, then keep the queue building at a normal pace
        // until gentle mode enters.  The floor will not ratchet for at least 5 s.
        feed_rtt(&mut rl, 20, Duration::from_millis(1000), t);
        t += Duration::from_millis(1);
        let enter_start = t;
        loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(1100);
            ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(1000), t);
            if rl.congestion_response.queue_growth().gentle_mode() {
                break;
            }
            assert!(
                t < enter_start + Duration::from_secs(6),
                "gentle mode must enter within a few seconds"
            );
        }
        let episode = rl
            .congestion_response
            .queue_growth()
            .drain_episode()
            .unwrap()
            .clone();
        assert!(
            episode.floor0 <= Duration::from_millis(650),
            "episode floor snapshot must be taken before the ratchet, got {:?}",
            episode.floor0
        );
        let gap0 = episode.gap0;
        assert!(gap0 > Duration::ZERO, "initial gap must be positive");

        // Mid-episode: simulate the WindowedRttMin bucket rotating and
        // ratcheting the live floor upward.  Replace the QueueGrowth floor with a
        // fresh window pre-fed ~750 ms while the drain episode holds ~1000 ms.
        // The live gap (smooth - new_floor) genuinely shrinks below the
        // GENTLE_DRAIN_GAP_SHRINK threshold, so the live-floor variant WOULD
        // suppress the guard.  The episode's floor0 snapshot keeps the guard
        // alive because it is compared against the pre-ratchet floor.
        let mut new_floor = WindowedRttMin::new(t);
        for i in 0..6 {
            let feed_time = t + Duration::from_millis(750) * i;
            new_floor.update(feed_time, Duration::from_millis(750));
        }
        rl.congestion_response
            .queue_growth()
            .replace_floor(new_floor);
        t += Duration::from_millis(1);
        let shrink_threshold = gap0.mul_f64(GENTLE_DRAIN_GAP_SHRINK);

        // The guard must still fire because it compares against the episode's
        // floor0 snapshot, not the live floor.  Drain at ~1000 ms RTT so
        // smooth stays ~1000 ms and the live gap remains below the threshold
        // for the entire guard window - a live-floor mutant that suppresses
        // the drain guard would fail here.
        let guard_start = t;
        let mut guard_fired = false;
        for _ in 0..20 {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(1100);
            let guard_exit =
                ack_seq_observed(&mut rl, seq.to_wire(), Duration::from_millis(1000), t);

            let smooth = rl.pkt_send_space.smooth_rtt();
            let live_floor = rl
                .congestion_response
                .queue_growth()
                .update_floor(t, smooth);
            let live_gap = smooth.saturating_sub(live_floor);
            assert!(
                live_gap < shrink_threshold,
                "live gap {live_gap:?} must stay below {shrink_threshold:?} (GENTLE_DRAIN_GAP_SHRINK × gap0 = {gap0:?} × {GENTLE_DRAIN_GAP_SHRINK}); \
             the live-floor variant would suppress the guard"
            );
            if let Some(cause) = guard_exit {
                assert_eq!(cause, MetricsGentleExitCause::DrainGuard);
                assert!(
                    !rl.congestion_response.queue_growth().gentle_mode(),
                    "the exit signal must coincide with leaving gentle mode"
                );
                guard_fired = true;
                break;
            }
            assert!(
                t < guard_start + Duration::from_secs(40),
                "guard must fire before the test times out"
            );
        }
        assert!(
            guard_fired,
            "gentle drain guard must fire despite rtt_floor ratchet"
        );

        // The scaled cooldown must dominate the fixed 15 s floor on this path.
        let control_rtt = rl.control_rtt();
        let expected_cooldown =
            GENTLE_REENTRY_COOLDOWN.max(control_rtt.mul_f64(GENTLE_REENTRY_COOLDOWN_RTTS));
        let block_until = rl
            .congestion_response
            .queue_growth()
            .gentle_block_until()
            .expect("cooldown must be set");
        let cooldown = block_until.saturating_duration_since(t);
        assert!(
            cooldown >= expected_cooldown - Duration::from_millis(50),
            "cooldown must scale with RTT, got {:?} expected {:?}",
            cooldown,
            expected_cooldown
        );
    }

    #[test]
    fn gentle_mode_exits_after_full_floor_window_on_clean_link() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        // Warm up and enter gentle mode via a sustained queue-building stretch.
        // Converge SRTT quickly with a burst of identical high-RTT samples while
        // the floor is still the warm-up 200 ms value, then sustain the queue.
        for _ in 0..6 {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(250);
            ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(200), t);
        }
        feed_rtt(&mut rl, 20, Duration::from_millis(800), t);
        t += Duration::from_millis(1);
        let enter_start = t;
        loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(900);
            ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(800), t);
            if rl.congestion_response.queue_growth().gentle_mode() {
                break;
            }
            assert!(
                t < enter_start + Duration::from_secs(6),
                "gentle mode must enter within a few seconds"
            );
        }

        // The link is now clean: RTT drops back to the floor and the gate opens.
        // Wait until the gate actually opens, then keep it open for the full floor
        // window (max(5 s, 10 * smooth)) before exiting gentle mode.
        let clean_start = t;
        let threshold =
            RTT_MIN_BUCKET.max(Duration::from_millis(200).saturating_mul(RTT_MIN_BUCKET_RTT_SCALE));

        // Wait for the gate to open (queue building becomes false).
        let gate_open_start = loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(250);
            ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(200), t);
            if let Some(open_since) = rl
                .congestion_response
                .queue_growth()
                .gentle_gate_open_since()
            {
                break open_since;
            }
            assert!(
                t < clean_start + Duration::from_secs(3),
                "gate must open after RTT drops"
            );
        };

        // Continue on the clean link until gentle mode exits.
        loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(250);
            let gentle_exit =
                ack_seq_observed(&mut rl, seq.to_wire(), Duration::from_millis(200), t);
            if !rl.congestion_response.queue_growth().gentle_mode() {
                assert_eq!(gentle_exit, Some(MetricsGentleExitCause::GateOpen));
                break;
            }
            assert_eq!(gentle_exit, None);
            assert!(
                t < gate_open_start + threshold + Duration::from_secs(2),
                "gentle mode must exit after the floor window"
            );
        }
        let open_for = t.saturating_duration_since(gate_open_start);
        assert!(
            open_for >= threshold - Duration::from_millis(100),
            "gentle mode must stay active until the open threshold elapses, got {:?}",
            open_for
        );
    }

    #[test]
    fn outage_reset_clears_gentle_reentry_cooldown() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        // Trigger the gentle-mode drain guard to set a re-entry cooldown.
        // Converge SRTT quickly with a burst of identical high-RTT samples
        // while the floor is still the warm-up 200 ms value.
        for _ in 0..6 {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(250);
            ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(200), t);
        }
        feed_rtt(&mut rl, 20, Duration::from_millis(800), t);
        t += Duration::from_millis(1);
        let enter_start = t;
        loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(900);
            ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(800), t);
            if rl.congestion_response.queue_growth().gentle_mode() {
                break;
            }
            assert!(
                t < enter_start + Duration::from_secs(6),
                "gentle mode must enter"
            );
        }
        let guard_start = t;
        loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(900);
            ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(800), t);
            if !rl.congestion_response.queue_growth().gentle_mode() {
                break;
            }
            assert!(t < guard_start + Duration::from_secs(14), "guard must fire");
        }
        assert!(
            rl.congestion_response
                .queue_growth()
                .gentle_block_until()
                .is_some_and(|u| u > t + Duration::from_secs(10)),
            "guard must set a long cooldown"
        );

        // Make forward progress so outage detection is eligible, then let the
        // next packet stall for two RTOs and trigger an outage reset.
        let progress_seq = send_one(&mut rl, t);
        t += Duration::from_millis(50);
        ack_seq(
            &mut rl,
            progress_seq.to_wire(),
            Duration::from_millis(50),
            t,
        );

        let stall_seq = send_one(&mut rl, t);
        let _ = stall_seq;
        let rto = rl.pkt_send_space.rto_duration();
        let detect_t = t + rto * 2 + Duration::from_millis(1);
        let acks = [AckInterval {
            start: crate::sequence::SequenceNumber::ZERO,
            size: NonZeroU64::new(progress_seq.to_wire() + 1).unwrap(),
        }];
        rl.recv_ack_pkt(
            AckBlocks::new(
                crate::sequence::SequenceNumber::from_wire(progress_seq.to_wire() + 1),
                &acks,
            ),
            detect_t,
        );

        // Outage recovery must clear the gentle re-entry cooldown.
        assert!(
            rl.congestion_response
                .queue_growth()
                .gentle_block_until()
                .is_none(),
            "outage recovery must clear gentle re-entry cooldown"
        );
    }

    #[test]
    fn high_jitter_without_standing_queue_stays_out_of_gentle_mode() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        let prime_rtt = Duration::from_millis(100);
        for _ in 0..4 {
            let seq = send_one(&mut rl, t);
            t += prime_rtt;
            ack_seq(&mut rl, seq.to_wire(), prime_rtt, t);
        }
        assert!(
            rl.pkt_send_space().smooth_rtt() <= Duration::from_millis(120),
            "primed smooth RTT must be ~100 ms, got {:?}",
            rl.pkt_send_space().smooth_rtt()
        );

        let rtt_lo = Duration::from_millis(200);
        let rtt_hi = Duration::from_millis(300);
        let target_smooth = Duration::from_millis(250);
        for i in 0..48 {
            let rtt = if i % 2 == 0 { rtt_lo } else { rtt_hi };
            rl.sample_rtt(rtt, t);
            t += Duration::from_micros(100);
        }

        let seq = send_one(&mut rl, t);
        t += rtt_hi;
        ack_seq(&mut rl, seq.to_wire(), rtt_hi, t);

        let smooth = rl.pkt_send_space().smooth_rtt();
        let rttvar = rl.pkt_send_space().smooth_rtt_var();
        let floor_est = prime_rtt;
        let tol = rttvar
            .mul_f64(QUEUE_RTT_FACTOR)
            .max(floor_est.mul_f64(QUEUE_TOL_RTT_FRACTION))
            .max(QUEUE_RTT_FLOOR);
        let enter_tol = rttvar
            .mul_f64(QUEUE_RTT_FACTOR * PERSISTENT_QUEUE_RTTVAR_FACTOR)
            .max(floor_est.mul_f64(QUEUE_TOL_RTT_FRACTION))
            .max(QUEUE_RTT_FLOOR);
        let gap = smooth.saturating_sub(floor_est);

        let margin_above_tol = gap.saturating_sub(tol);
        let margin_below_enter = enter_tol.saturating_sub(gap);
        assert!(
            margin_above_tol >= Duration::from_millis(40),
            "gap {gap:?} must exceed tol {tol:?} by >= 40 ms, margin {margin_above_tol:?}"
        );
        assert!(
            margin_below_enter >= Duration::from_millis(40),
            "enter_tol {enter_tol:?} must exceed gap {gap:?} by >= 40 ms, margin {margin_below_enter:?}"
        );

        let sustain_rtts = GENTLE_ENTER_RTTS * 2.0 + 1.0;
        let sustain_until = t + target_smooth.mul_f64(sustain_rtts);
        let rtts = [rtt_lo, rtt_hi];
        let mut sample_idx = 0;
        while t < sustain_until {
            let rtt = rtts[sample_idx % 2];
            sample_idx += 1;
            let seq = send_one(&mut rl, t);
            t += rtt;
            ack_seq(&mut rl, seq.to_wire(), rtt, t);
            assert!(
                !rl.congestion_response.queue_growth().gentle_mode(),
                "gentle mode must stay false at sample {} (t={:?})",
                sample_idx,
                t.saturating_duration_since(t0)
            );
        }
    }

    /// `first_frame_pkt_within_fec_mss` — a 1-byte frame at a legal small MSS
    /// yields an on-wire first packet <= the FEC-reduced MSS.  Before the fix,
    /// the first-packet cap used `mss - data_overhead()` (not
    /// `frame_data_overhead()`), so the first packet was ~4 bytes over budget.
    #[test]
    fn first_frame_pkt_within_fec_mss() {
        let now = Instant::now();
        let mss = NO_FEC_MSS;
        let mut rl = super::ReliableLayer::new(
            NonZeroUsize::new(mss).unwrap(),
            crate::delivery::frame::FrameMode::enabled(),
            now,
        )
        .0;

        // A 1-byte frame at a legal small MSS.
        let frame = vec![0u8; 1];
        rl.send_frame_buf(&frame, now).unwrap();

        // Send the first (and only) packet.
        let mut pkt = vec![0u8; mss];
        let p = rl
            .send_data_pkt(&mut pkt, now)
            .expect("must send first pkt");
        assert!(p.frame_len.is_some(), "first packet must carry frame_len");

        let payload_len = match p.data_written {
            super::DataPktPayload::Data(n) => n.get(),
            _ => panic!("expected data packet"),
        };

        // The on-wire size of the first packet = frame_data_overhead + payload.
        let on_wire_first = crate::delivery::frame::wire::frame_data_overhead() + payload_len;
        assert!(
            on_wire_first <= mss,
            "first frame packet on-wire size {on_wire_first} must be <= MSS {mss}"
        );
    }

    /// Fix #11: `large_legal_frame_admitted_to_empty_stage` — a frame
    /// larger than the ~2-packet initial stage cap is admitted into an
    /// EMPTY stage regardless of the soft cap.  Before the fix, it was
    /// rejected and waited forever for a send notification that never
    /// came (hang).
    #[test]
    fn large_legal_frame_admitted_to_empty_stage() {
        let now = Instant::now();
        let mss = NO_FEC_MSS;
        let mut rl = super::ReliableLayer::new(
            NonZeroUsize::new(mss).unwrap(),
            crate::delivery::frame::FrameMode::enabled(),
            now,
        )
        .0;

        // The initial stage cap is ~2 packets worth of payload.
        let normal_payload = rl.max_data_size_per_pkt();
        let stage_cap = (2 * normal_payload).min(super::MAX_FRAME_LEN);

        // A frame larger than the stage cap but within MAX_FRAME_LEN.
        let frame_len = stage_cap + normal_payload;
        assert!(
            frame_len <= super::MAX_FRAME_LEN,
            "test frame must be a legal size"
        );
        let frame = vec![0u8; frame_len];

        // This must succeed (not return WouldBlock), proving the empty-stage
        // bypass works.
        let result = rl.send_frame_buf(&frame, now);
        assert!(
            result.is_ok(),
            "large legal frame must be admitted to an empty stage, got {result:?}"
        );
    }

    #[test]
    fn outage_recovery_clamps_subsequent_rate_updates_to_init_rate() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        // Warm up well above INIT_SEND_RATE.
        for _ in 0..50 {
            send_max(&mut rl, t);
            t += Duration::from_millis(40);
            ack_all(&mut rl, Some(Duration::from_millis(40)), t);
            t += Duration::from_nanos(1);
        }
        let warm_rate = rl.log().send_rate;
        assert!(
            warm_rate > 2.0 * INIT_SEND_RATE,
            "warm-up rate must exceed 2 * INIT_SEND_RATE, got {}",
            warm_rate
        );

        // Stall the flight to trigger outage recovery, then keep writing
        // rates: every writer must clamp to INIT_SEND_RATE until a fresh
        // post-outage sample closes the epoch.
        let _final_sent = send_max(&mut rl, t);
        let restore_time = t + Duration::from_secs(10);
        ack_all(&mut rl, None, restore_time);
        assert!(rl.pkt_send_space.in_outage_recovery());
        assert_eq!(
            rl.send_rate.get(),
            INIT_SEND_RATE,
            "outage recovery must restart at exactly INIT_SEND_RATE"
        );

        // A subsequent explicit rate writer is clamped too: the pre-outage
        // backlog must not be released at a stale high rate.
        rl.set_send_rate(
            PosR::new(10_000.0).unwrap(),
            restore_time + Duration::from_millis(1),
        );
        assert_eq!(
            rl.send_rate.get(),
            INIT_SEND_RATE,
            "a rate update during outage recovery must clamp to INIT_SEND_RATE"
        );
    }

    #[test]
    fn huge_loss_backoff_reports_its_congestion_action() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);

        // Send a flight, then let it sit unacked: every pipe packet becomes
        // a huge-data-loss sample once the sample-count gate opens.
        send_burst(&mut rl, 20, t0);
        assert!(rl.huge_data_loss_gate(t0).is_none());
        let lost_at = t0 + Duration::from_secs(2);

        // Revalidate the positive huge-loss result until the 2 * RTO
        // persistence threshold fires the backoff, then check the action.
        let mut t = lost_at + Duration::from_millis(1);
        let mut fired = false;
        for _ in 0..600 {
            let before = rl.send_rate.get();
            rl.backoff_on_huge_data_loss_linear(t);
            if rl.last_congestion_action
                == Some(crate::metrics::MetricsCongestionAction::HugeLossBackoff)
            {
                fired = true;
                assert!(
                    rl.send_rate.get() < before,
                    "the huge-loss backoff must lower the send rate"
                );
                break;
            }
            t += Duration::from_millis(10);
        }
        assert!(fired, "huge-data-loss backoff must fire after 2 * RTO");
    }
    #[test]
    fn ordinary_bandwidth_probe_waits_for_previous_feedback() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        rl.congestion_metrics_enabled = true;
        let mut t = t0;

        // Ramp at a brisk 10 ms RTT so slow start exits and the ordinary
        // bandwidth-probe branch takes over with the delivery rate tracking
        // the send rate.  The controller decisions must be counted.
        let ramp_rtt = Duration::from_millis(10);
        for _ in 0..40 {
            send_max(&mut rl, t);
            t += ramp_rtt;
            ack_all(&mut rl, Some(ramp_rtt), t);
        }
        assert!(!rl.slow_start, "slow start must exit during the ramp");
        assert!(
            rl.congestion_metrics.bandwidth_probe_decisions > 0,
            "the ramp must take probe decisions"
        );
        assert_eq!(
            rl.congestion_metrics.bandwidth_probe_before_feedback, 0,
            "the 10 ms ramp cadence equals the control RTT, so nothing is before-feedback"
        );
        let increases_before = rl.congestion_metrics.bandwidth_probe_increases;

        // Grow rttvar (alternating 10/40 ms samples) so the queue gate stays
        // open while the control RTT rises to ~25 ms, keeping the floor at the
        // ramp's 10 ms minimum.  Then offer probe opportunities at the same
        // 10 ms cadence: decisions continue, but increases must wait until the
        // previous increase has had one current control RTT of feedback.
        for i in 0..20 {
            let ft = t + Duration::from_micros(i as u64 * 100);
            rl.sample_rtt(Duration::from_millis(10), ft);
            rl.sample_rtt(Duration::from_millis(40), ft + Duration::from_micros(50));
        }
        t += Duration::from_millis(1);
        let control_rtt = rl.control_rtt();
        assert!(
            control_rtt > ramp_rtt,
            "the jittered feed must raise the control RTT above the cadence, got {control_rtt:?}"
        );
        let before_feedback_before = rl.congestion_metrics.bandwidth_probe_before_feedback;
        let decisions_before = rl.congestion_metrics.bandwidth_probe_decisions;
        for i in 0..6 {
            send_max(&mut rl, t);
            t += Duration::from_millis(10);
            let rtt = if i % 2 == 0 {
                Duration::from_millis(10)
            } else {
                Duration::from_millis(40)
            };
            ack_all(&mut rl, Some(rtt), t);
        }
        assert!(
            rl.congestion_metrics.bandwidth_probe_increases > increases_before,
            "the probe rounds must eventually apply another increase"
        );
        assert!(
            rl.congestion_metrics.bandwidth_probe_decisions >= decisions_before + 6,
            "every rate sample must still evaluate the probe branch"
        );
        assert_eq!(
            rl.congestion_metrics.bandwidth_probe_before_feedback, before_feedback_before,
            "no applied increase may precede one control RTT of feedback"
        );
        let interval = rl
            .congestion_metrics
            .last_bandwidth_probe_interval
            .expect("two increases must record an interval");
        assert!(
            interval >= control_rtt,
            "the interval {interval:?} must cover the control RTT {control_rtt:?}"
        );
    }

    #[test]
    fn unchanged_smooth_rate_refreshes_send_space_without_touching_the_pacer() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let pacer = rl.send_rate_limiter.clone();
        let current = rl.send_rate.get();
        let tokens_before = {
            let mut pacer = pacer.lock().unwrap();
            assert!(pacer.take_at_most_tokens(usize::MAX, t0) > 0);
            pacer.outdated_tokens()
        };
        let later = t0 + Duration::from_millis(10);
        rl.set_smooth_send_rate(current, later);
        assert_eq!(
            pacer.lock().unwrap().outdated_tokens(),
            tokens_before,
            "a no-op smoothing decision must not refresh or rebuild the pacer"
        );
        rl.set_smooth_send_rate(current * 2.0, later);
        assert!(rl.send_rate.get() > current);
        assert!(
            pacer.lock().unwrap().outdated_tokens() > tokens_before,
            "a real rate change must still credit elapsed pacer tokens"
        );
    }

    #[test]
    fn unchanged_rate_still_applies_the_outage_cwnd_clamp() {
        use crate::traffic_shaping::recovery::pkt_send_space::OUTAGE_RECOVERY_CWND;

        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;
        for _ in 0..5 {
            assert!(send_max(&mut rl, t) > 0);
            t += Duration::from_millis(40);
            ack_all(&mut rl, Some(Duration::from_millis(40)), t);
            t += Duration::from_nanos(1);
        }
        // Start the flight while the warmed-up rate still has tokens, then
        // re-establish the initial numeric rate before the blackout so the
        // send-space policy update remains observable even though the outage
        // clamp will make the rate itself compare equal when recovery starts.
        let stalled_at = t + Duration::from_millis(40);
        assert!(send_max(&mut rl, stalled_at) > 0);
        rl.sample_rtt(Duration::from_secs(1), stalled_at);
        rl.set_send_rate(PosR::new(INIT_SEND_RATE).unwrap(), stalled_at);
        assert_eq!(rl.send_rate.get(), INIT_SEND_RATE);
        assert!(
            rl.metrics_at(stalled_at).congestion_window_packets > OUTAGE_RECOVERY_CWND,
            "the pre-outage cwnd must make the clamp observable"
        );
        let restore_time = stalled_at + Duration::from_secs(10);
        ack_all(&mut rl, None, restore_time);

        assert!(rl.pkt_send_space.in_outage_recovery());
        assert_eq!(rl.send_rate.get(), INIT_SEND_RATE);
        assert_eq!(
            rl.metrics_at(restore_time).congestion_window_packets,
            OUTAGE_RECOVERY_CWND,
            "an unchanged numeric rate must still refresh send-space policy"
        );
    }

    #[test]
    fn congestion_metrics_track_persistent_queue_resets_on_signal_loss() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        rl.congestion_metrics_enabled = true;
        let floor = Duration::from_millis(100);
        let tolerance = Duration::from_millis(25);
        let armed_for = Duration::from_millis(400);

        rl.record_congestion_interval_state(floor, tolerance, None, 100.0);
        assert_eq!(rl.congestion_metrics.persistent_queue_resets, 0);
        rl.record_congestion_interval_state(floor, tolerance, Some(armed_for), 100.0);
        assert_eq!(rl.congestion_metrics.persistent_queue_for, Some(armed_for));
        rl.congestion_metrics.clear_decision_gauges();
        rl.record_congestion_interval_state(floor, tolerance, None, 100.0);
        assert_eq!(rl.congestion_metrics.persistent_queue_for, None);
        assert_eq!(rl.congestion_metrics.persistent_queue_resets, 1);
        rl.record_congestion_interval_state(floor, tolerance, None, 100.0);
        assert_eq!(rl.congestion_metrics.persistent_queue_resets, 1);
    }

    #[test]
    fn disabled_congestion_metrics_do_not_account_probe_decisions() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        assert!(
            !rl.congestion_metrics_enabled,
            "test_layer must start with controller accounting disabled"
        );
        let mut t = t0;

        // Ramp hard: the controller takes probe decisions and drives the rate
        // up, but with no observer/logger the accounting must not mutate any
        // counter or timestamp.
        let ramp_rtt = Duration::from_millis(10);
        for _ in 0..40 {
            send_max(&mut rl, t);
            t += ramp_rtt;
            ack_all(&mut rl, Some(ramp_rtt), t);
        }
        assert!(!rl.slow_start, "slow start must exit during the ramp");
        assert!(
            rl.log().send_rate > 2.0 * INIT_SEND_RATE,
            "the ramp must actually probe"
        );
        let m = &rl.congestion_metrics;
        assert_eq!(m.rate_samples, 0);
        assert_eq!(m.bandwidth_probe_decisions, 0);
        assert_eq!(m.bandwidth_probe_increases, 0);
        assert_eq!(m.bandwidth_probe_before_feedback, 0);
        assert_eq!(m.delay_drains, 0);
        assert_eq!(m.persistent_queue_for, None);
        assert!(!m.persistent_queue_was_armed);
        assert_eq!(m.persistent_queue_resets, 0);
        assert_eq!(m.last_bandwidth_probe_increase_at, None);
        assert_eq!(m.last_bandwidth_probe_interval, None);
    }

    #[test]
    fn transient_jitter_holds_rate_until_standing_queue_drains() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        // Ramp on a brisk 10 ms RTT so slow start exits and the send rate
        // climbs well above the drain-floor cap.
        let ramp_rtt = Duration::from_millis(10);
        for _ in 0..40 {
            send_max(&mut rl, t);
            t += ramp_rtt;
            ack_all(&mut rl, Some(ramp_rtt), t);
        }
        assert!(!rl.slow_start, "slow start must exit during the ramp");
        let held_rate = rl.send_rate.get();
        assert!(
            held_rate > 2.0 * INIT_SEND_RATE,
            "ramp must lift the send rate above the drain-floor cap, got {held_rate}"
        );

        // Transient (non-persistent) queue: raise smooth RTT ~100 ms above the
        // ~10 ms floor with ±30 ms jitter.  The gap exceeds the ordinary queue
        // gate (2 * rttvar) but stays under the wider persistent-queue margin
        // (4 * rttvar), so the controller must Hold, not Drain.
        for i in 0..48 {
            let rtt = if i % 2 == 0 {
                Duration::from_millis(80)
            } else {
                Duration::from_millis(140)
            };
            rl.sample_rtt(rtt, t);
            t += Duration::from_micros(100);
        }
        let seq = send_one(&mut rl, t);
        t += Duration::from_millis(140);
        ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(140), t);

        let smooth = rl.pkt_send_space().smooth_rtt();
        let rttvar = rl.pkt_send_space().smooth_rtt_var();
        let tol = rttvar.mul_f64(QUEUE_RTT_FACTOR).max(QUEUE_RTT_FLOOR);
        let persistent_tol = rttvar
            .mul_f64(QUEUE_RTT_FACTOR * PERSISTENT_QUEUE_RTTVAR_FACTOR)
            .max(QUEUE_RTT_FLOOR);
        let gap = smooth.saturating_sub(Duration::from_millis(10));
        assert!(
            gap > tol,
            "gap {gap:?} must exceed the queue gate {tol:?} (smooth {smooth:?}, rttvar {rttvar:?})"
        );
        assert!(
            gap <= persistent_tol,
            "gap {gap:?} must stay under the persistent margin {persistent_tol:?}"
        );
        assert_eq!(
            rl.last_congestion_action,
            Some(crate::metrics::MetricsCongestionAction::QueueHold),
            "a transient queue must Hold, not drain or back off"
        );
        assert_eq!(
            rl.send_rate.get(),
            held_rate,
            "a transient queue must hold the send rate unchanged (no reduction)"
        );

        // Standing queue: sustain a ~260 ms smooth RTT so the gap blows past
        // the persistent-queue margin.  The controller must switch to Drain
        // and lower the rate toward the drain target.
        for _ in 0..30 {
            rl.sample_rtt(Duration::from_millis(260), t);
            t += Duration::from_micros(100);
        }
        let seq = send_one(&mut rl, t);
        t += Duration::from_millis(260);
        ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(260), t);

        let smooth = rl.pkt_send_space().smooth_rtt();
        let rttvar = rl.pkt_send_space().smooth_rtt_var();
        let persistent_tol = rttvar
            .mul_f64(QUEUE_RTT_FACTOR * PERSISTENT_QUEUE_RTTVAR_FACTOR)
            .max(QUEUE_RTT_FLOOR);
        assert!(
            smooth.saturating_sub(Duration::from_millis(10)) > persistent_tol,
            "smooth {smooth:?} must cross the persistent margin {persistent_tol:?}"
        );
        assert_eq!(
            rl.last_congestion_action,
            Some(crate::metrics::MetricsCongestionAction::DelayDrain),
            "a standing queue must drain"
        );
        assert!(
            rl.send_rate.get() < held_rate,
            "a standing queue must reduce the rate below the held rate {held_rate}, got {}",
            rl.send_rate.get()
        );
    }

    #[test]
    fn congestion_metrics_count_loss_floor_bindings_only_when_enabled() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        assert!(
            !rl.congestion_metrics_enabled,
            "test_layer must start with controller accounting disabled"
        );

        // Disabled: no loss-backoff counter or gauge may move.
        rl.record_loss_backoff(10.0, 40.0, 40.0);
        let m = &rl.congestion_metrics;
        assert_eq!(m.loss_backoffs, 0);
        assert_eq!(m.loss_backoff_floor_bindings, 0);
        assert_eq!(m.loss_backoff_floor_packets_per_second, None);
        assert_eq!(m.loss_backoff_raw_target_packets_per_second, None);
        assert_eq!(m.loss_backoff_target_packets_per_second, None);

        // Enabled with floor > raw: gauges set, both counters saturate-add.
        rl.congestion_metrics_enabled = true;
        rl.record_loss_backoff(10.0, 40.0, 40.0);
        let m = &rl.congestion_metrics;
        assert_eq!(m.loss_backoff_floor_packets_per_second, Some(40.0));
        assert_eq!(m.loss_backoff_raw_target_packets_per_second, Some(10.0));
        assert_eq!(m.loss_backoff_target_packets_per_second, Some(40.0));
        assert_eq!(m.loss_backoffs, 1);
        assert_eq!(m.loss_backoff_floor_bindings, 1);

        // A second binding increments both counters.
        rl.record_loss_backoff(20.0, 40.0, 40.0);
        let m = &rl.congestion_metrics;
        assert_eq!(m.loss_backoffs, 2);
        assert_eq!(m.loss_backoff_floor_bindings, 2);

        // Floor at or below raw still counts the backoff but never a binding.
        rl.record_loss_backoff(40.0, 40.0, 40.0);
        let m = &rl.congestion_metrics;
        assert_eq!(m.loss_backoffs, 3);
        assert_eq!(m.loss_backoff_floor_bindings, 2);
        rl.record_loss_backoff(50.0, 40.0, 50.0);
        let m = &rl.congestion_metrics;
        assert_eq!(m.loss_backoffs, 4);
        assert_eq!(m.loss_backoff_floor_bindings, 2);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsRow {
    pub tokens: f64,
    pub send_rate: f64,
    pub delivery_rate: Option<f64>,
    pub loss_rate: Option<f64>,
    pub congestion_loss_rate: Option<f64>,
    pub congestion_action: Option<String>,
    pub num_in_flight_pkts: usize,
    pub num_pkts_in_pipe: usize,
    pub num_rtx_active_pkts: usize,
    pub num_rtx_ready_pkts: usize,
    pub num_rtx_pkts: usize,
    pub send_seq: u64,
    pub min_rtt: Option<u128>,
    pub rtt: u128,
    pub retransmission_timeout: u128,
    pub oldest_pipe_packet_age: Option<u128>,
    pub maximum_packet_rto_overdue: Option<u128>,
    pub rto_deadline_postponements: u64,
    pub cwnd: usize,
    pub num_rx_pkts: usize,
    pub recv_seq: Option<u64>,
    pub delivery_sample_app_limited: Option<bool>,
    pub application_write_waiters: usize,
    pub application_limited_detections: u64,
    pub application_limited_detections_suppressed_by_waiting_writer: u64,
    pub congestion_control_rtt: Option<u128>,
    pub congestion_rtt_floor: Option<u128>,
    pub congestion_queue_tolerance: Option<u128>,
    pub congestion_persistent_queue_for: Option<u128>,
    pub congestion_persistent_queue_resets: u64,
    pub congestion_delivery_peak_packets_per_second: Option<f64>,
    pub congestion_drain_floor_packets_per_second: Option<f64>,
    pub congestion_drain_target_packets_per_second: Option<f64>,
    pub congestion_loss_backoff_floor_packets_per_second: Option<f64>,
    pub congestion_loss_backoff_raw_target_packets_per_second: Option<f64>,
    pub congestion_loss_backoff_target_packets_per_second: Option<f64>,
    pub congestion_loss_backoffs: u64,
    pub congestion_loss_backoff_floor_bindings: u64,
    pub congestion_rate_samples: u64,
    pub congestion_bandwidth_probe_decisions: u64,
    pub congestion_bandwidth_probe_increases: u64,
    pub congestion_bandwidth_probe_before_feedback: u64,
    pub congestion_last_bandwidth_probe_interval: Option<u128>,
    pub congestion_delay_drains: u64,
    pub pending_send_bytes: usize,
    pub send_stage_capacity_bytes: usize,
    pub accepts_new_packet: bool,
}
