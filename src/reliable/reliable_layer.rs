use core::num::NonZeroUsize;
use std::{
    sync::{
        Arc,
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
    delivery::{
        byte_stream::{
            recv::StockRecvStage,
            send::{MAX_SEND_DATA_BUF_LEN, StockSendStage},
        },
        frame::{
            mode::FrameMode,
            send::{FrameSendStage, MAX_FRAME_LEN},
        },
    },
    recv_queue::pkt_recv_space::PktRecvSpace,
    traffic_shaping::core::congestion_response::lane::CongestionLane,
    traffic_shaping::core::fast_start::should_exit_slow_start,
    traffic_shaping::core::{
        CongestionDecision, CongestionInput, CongestionResponse, FastStartEpisode, FastStartStep,
        GentleExitCause, ProbeKind, SendPacer, linear_backoff_step, settle_computed_rate,
    },
    traffic_shaping::recovery::pkt_send_space::{
        CWND_BDP_CAP_ENGAGE_RTT_FACTOR, CWND_BDP_CAP_SCALE, CWND_SEND_RATE_SCALE, INIT_CWND,
        PktSendSpace,
    },
    traffic_shaping::recovery::reorder_tolerance::select_gate_jitter,
    transmission::watchdog_tuning::WatchdogTuning,
};

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
    GENTLE_REENTRY_COOLDOWN_RTTS, ORDINARY_PROBE_MAX_GAIN, PERSISTENT_QUEUE_RTTVAR_FACTOR,
    QUEUE_RTT_FACTOR, QUEUE_RTT_FLOOR, QUEUE_TOL_RTT_FRACTION, WindowedRttMin,
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
    mss: crate::mss::Mss,
    /// Cached `mss - data_overhead()`: the maximum payload bytes per data
    /// packet, computed once at construction instead of on every packetize.
    max_data_size_per_pkt: usize,
    send_data_buf: StockSendStage,
    send_fin_buf: FinState,
    recv_data_buf: StockRecvStage,
    recv_fin_buf: bool,
    send_rate_limiter: SendPacer,
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
    /// Windowed ACK-clock ramp for the dedicated lane's bounded fast start.
    fast_start: FastStartEpisode,
    last_congestion_loss_ratio: Option<f64>,
    last_congestion_action: Option<crate::metrics::MetricsCongestionAction>,
    /// Whether congestion-controller interval accounting is on.  Set once at
    /// connection construction to `metrics_observer.is_some() ||
    /// log_config.is_some()`; when false the accounting hooks return before
    /// any timestamp arithmetic or counter mutation, so an absent observer
    /// and logger costs exactly one predictable branch per decision point.
    congestion_metrics_enabled: bool,
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
    pub fn new(mss: crate::mss::Mss, frame_delivery: FrameMode, now: Instant) -> (Self, SendPacer) {
        Self::new_at(
            mss,
            frame_delivery,
            CongestionLane::default(),
            now,
            InitialSequences::ZERO,
        )
    }

    /// Construct with handshake-derived directional initial sequences:
    /// `PktSendSpace` starts at `initial_sequences.send` and `PktRecvSpace`
    /// at `initial_sequences.recv`.  The zero-seeded `new()` keeps skipped-
    /// handshake peers zero-compatible.  `congestion_lane` is the owner's
    /// declared congestion intent (see [`CongestionLane`]); callers that do not
    /// declare one get the conservative [`CongestionLane::Shared`].
    pub fn new_at(
        mss: crate::mss::Mss,
        frame_delivery: FrameMode,
        congestion_lane: CongestionLane,
        now: Instant,
        initial_sequences: InitialSequences,
    ) -> (Self, SendPacer) {
        let send_rate = PosR::new(INIT_SEND_RATE).unwrap();
        let send_rate_limiter = SendPacer::new_prefilled(send_rate, now);
        let max_data_size_per_pkt = mss.max_data_size_per_pkt();
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
            congestion_response: CongestionResponse::new(
                now,
                frame_delivery.allow_reorder,
                congestion_lane,
            ),
            slow_start: true,
            slow_start_acked_pkts: 0,
            fast_start: FastStartEpisode::new(now),
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
        mss: crate::mss::Mss,
        frame_delivery: FrameMode,
        congestion_lane: CongestionLane,
        now: Instant,
        initial_sequences: InitialSequences,
        tuning: WatchdogTuning,
    ) -> (Self, SendPacer) {
        let send_rate = PosR::new(INIT_SEND_RATE).unwrap();
        let send_rate_limiter = SendPacer::new_prefilled(send_rate, now);
        let max_data_size_per_pkt = mss.max_data_size_per_pkt();
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
            congestion_response: CongestionResponse::new(
                now,
                frame_delivery.allow_reorder,
                congestion_lane,
            ),
            slow_start: true,
            slow_start_acked_pkts: 0,
            fast_start: FastStartEpisode::new(now),
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

    /// Opt controller interval accounting in/out.  When false (no observer
    /// and no logger) the accounting hooks return before any timestamp
    /// arithmetic or counter mutation, so an absent observer and logger cost
    /// exactly one predictable branch per decision point.
    pub(crate) fn set_congestion_metrics_enabled(&mut self, enabled: bool) {
        self.congestion_metrics_enabled = enabled;
    }

    #[cfg(test)]
    pub(crate) fn congestion_metrics_enabled(&self) -> bool {
        self.congestion_metrics_enabled
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

    /// Whether an application writer is currently blocked waiting for send
    /// stage space.  Used by the send driver's wake computation so it never
    /// parks indefinitely while a writer is blocked on it.
    pub(crate) fn has_application_write_waiters(&self) -> bool {
        self.application_write_waiters.load(Ordering::Relaxed) > 0
    }

    /// Number of application writers currently blocked waiting for send stage
    /// space (the raw input to the FEC spare-capacity gates).
    pub(crate) fn application_write_waiters(&self) -> usize {
        self.application_write_waiters.load(Ordering::Relaxed)
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

    /// Whether the bounded receive window is at capacity (every in-window
    /// sequence slot is occupied), so a freshly-arriving payload can no
    /// longer be buffered. The read-closed session half treats this as the
    /// memory-saturation point: it keeps receiving and ACKing into the
    /// bounded window (so the peer does not retransmit-storm) until this
    /// flips, and only then terminates.
    pub fn recv_window_full(&self) -> bool {
        self.pkt_recv_space.is_full()
    }

    /// Whether the delivery-rate congestion controller currently considers the
    /// bottleneck queue to be building (smooth RTT above the floor plus the
    /// gate tolerance).  Used by the transmission layer to suppress
    /// retransmission-armor duplicate copies under congestion.
    pub fn queue_building(&self) -> bool {
        self.congestion_response.queue_building()
    }

    /// The most recent congestion-loss ratio measured by the delivery-rate
    /// controller (`None` before the first rate sample).  Feeds the FEC
    /// condition gate's loss evidence.
    pub(crate) fn congestion_loss_ratio(&self) -> Option<f64> {
        self.last_congestion_loss_ratio
    }

    /// Test-only: force the congestion-loss ratio so the FEC condition gate
    /// can be exercised deterministically without driving a full delivery-rate
    /// sample sequence.
    #[cfg(test)]
    pub(crate) fn set_congestion_loss_ratio_for_test(&mut self, loss: Option<f64>) {
        self.last_congestion_loss_ratio = loss;
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

    /// Test-only: pin the send pacer's burst floor to the legacy 64-packet
    /// value and refill it, so tests of the send/recovery/FEC algorithms keep
    /// the working burst they were written against.  The pacer's own unit tests
    /// cover the smaller production floor.
    #[cfg(test)]
    pub(crate) fn pin_legacy_pacer_burst_for_test(&self, now: Instant) {
        self.send_rate_limiter.set_min_burst_for_test(64, now);
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
        // The MSS must leave room for the first-frame header (FRAME_DATA_TS)
        // plus the truncated-datagram detection headroom plus at least one
        // payload byte; otherwise no first packet can be produced and the
        // frame would stall forever (or, before the saturating sizing, panic).
        if self.mss.get()
            < crate::delivery::frame::wire::frame_data_overhead()
                + crate::mss::TRUNCATION_DETECTION_BYTES
                + 1
        {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
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
        self.send_data_pkt_bounded(pkt, now, 0)
    }

    /// Like [`Self::send_data_pkt`], but reserves `reserved_bytes` of the
    /// per-packet budget for a piggybacked ACK: the combined datagram (ACK +
    /// data) must stay within the FEC symbol / MSS. Retransmissions and tail
    /// probes keep their original size (copied into the full `pkt` buffer);
    /// only NEW packets are sized down. The caller uses this to leave room
    /// for the ACK it will prepend to the data packet.
    pub fn send_data_pkt_bounded(
        &mut self,
        pkt: &mut [u8],
        now: Instant,
        reserved_bytes: usize,
    ) -> Option<DataPkt> {
        self.detect_application_limited_phases(now);
        if crate::debug::debug_send() {
            eprintln!(
                "[sdp] in_flight={} cwnd={} send_buf={} rtx_due={} rate={:.1}",
                self.pkt_send_space.num_in_flight_pkts(),
                self.pkt_send_space.cwnd(),
                self.send_data_buf.len(),
                self.pkt_send_space.has_rtx(now),
                self.send_rate.get()
            );
        }

        self.backoff_on_huge_data_loss_linear(now);

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
            && !self.send_rate_limiter.take_exact_tokens(1, now)
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
            return self.send_data_pkt_frame(pkt, now, no_packets_in_flight, reserved_bytes);
        }

        let pkt_bytes = pkt
            .len()
            .min(self.max_data_size_per_pkt().saturating_sub(reserved_bytes))
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
        if !self.send_rate_limiter.take_exact_tokens(1, now) {
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
        reserved_bytes: usize,
    ) -> Option<DataPkt> {
        let normal_max_payload = self.max_data_size_per_pkt().saturating_sub(reserved_bytes);
        // The first packet of a frame carries the 4-byte frame-length header
        // on top of the stock overhead, and every datagram must stay one byte
        // below the MSS (the truncated-datagram detection headroom), so the
        // first-packet payload is `mss - frame_data_overhead - headroom`.
        // `saturating_sub` (not `checked_sub().unwrap()`) so a too-small MSS
        // degrades to "no packet" instead of panicking.
        let first_pkt_max_payload = self
            .mss
            .get()
            .saturating_sub(crate::delivery::frame::wire::frame_data_overhead())
            .saturating_sub(crate::mss::TRUNCATION_DETECTION_BYTES)
            .saturating_sub(reserved_bytes);
        if first_pkt_max_payload == 0 {
            // The MSS cannot carry a first-frame packet (frame header +
            // headroom + the reserved ACK): no packet can be produced.
            return None;
        }

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

        if !self.send_rate_limiter.take_exact_tokens(1, now) {
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
            self.fast_start.reset(now);
            self.last_congestion_loss_ratio = None;
            self.set_send_rate(INIT_SEND_RATE, now);
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
        //
        // The dedicated lane uses a windowed ACK-clock instead: the historical
        // lifetime accumulator keeps growing on a backlogged flow (the pipe
        // never drains), so reviving slow start with it would run the pacer
        // past capacity without bound. The windowed ramp is bounded by the
        // recent delivered rate and leaves fast start as soon as that rate
        // plateaus or the paced rate outruns it. The shared lane keeps the
        // stock accumulator/exit.
        if self.slow_start {
            if self.congestion_metrics_enabled {
                self.congestion_metrics.clear_decision_gauges();
            }
            if self.congestion_response.lane().owns_fast_start() {
                let fresh = self.pkt_send_space.fresh_acked_count();
                let control_rtt = self.control_rtt();
                let current = self.send_rate.get();
                match self.fast_start.on_ack(
                    fresh,
                    self.pkt_send_space.min_rtt(),
                    now,
                    control_rtt,
                    current,
                ) {
                    FastStartStep::Hold => {}
                    FastStartStep::Ramp(target) => {
                        self.set_send_rate(target, now);
                        self.last_congestion_action =
                            Some(crate::metrics::MetricsCongestionAction::SlowStartAck);
                    }
                    FastStartStep::Plateau(delivered) => {
                        self.slow_start = false;
                        // A zero-delivery window (idle gap, all-lost flight,
                        // duplicate/coalesced ACK) and any non-finite
                        // computation are not rates to settle at; the
                        // setter substitutes the live rate in that case.
                        self.set_send_rate(delivered, now);
                        // A burst of ACKs during the ramp can inflate the
                        // tracked delivery peak; drop it so the gentle probe
                        // cannot use it as a base and creep back over capacity.
                        self.congestion_response.clear_delivery_peak(now);
                        self.last_congestion_action =
                            Some(crate::metrics::MetricsCongestionAction::SlowStartAck);
                    }
                }
            } else {
                self.slow_start_acked_pkts += self.pkt_send_space.fresh_acked_count();
                let ss_rate = self.slow_start_acked_pkts as f64 / self.control_rtt().as_secs_f64();
                self.set_send_rate(ss_rate.max(self.send_rate.get()), now);
                self.last_congestion_action =
                    Some(crate::metrics::MetricsCongestionAction::SlowStartAck);
            }
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
        let min_rtt = self.pkt_send_space.min_rtt()?;
        let sr = self
            .connection_stats
            .sample_rate(&self.pkt_buf, now, min_rtt);
        self.pkt_stats_buf.clear();
        self.pkt_buf.clear();

        let sr = sr?;
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
        let gate_jitter = select_gate_jitter(
            &self.pkt_send_space,
            self.congestion_response.reorder_tolerant(),
        );
        let observation = self.congestion_response.observe(
            smooth,
            gate_jitter,
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
            if self.congestion_response.lane().owns_fast_start() {
                if FastStartEpisode::exits_on_rate_sample(
                    observation.loss_blocks_delay_control,
                    observation.queue_building,
                ) {
                    self.slow_start = false;
                    self.congestion_response.clear_delivery_peak(now);
                }
            } else {
                // The exit threshold is the ordinary *multiplicative* probe
                // ceiling (`1.5 * delivery`), not the shared lane's additive
                // step.  The additive target is always `current + step`, so it
                // is higher than the ceiling for a backlogged flow; using it
                // would exit slow start more aggressively than the historical
                // stock exit.  The shared lane deliberately keeps the lower
                // ceiling, and the ordinary additive probe takes over once slow
                // start ends.  This coupling is deliberate; see
                // `shared_lane_slow_start_exits_on_the_multiplicative_ceiling_not_the_additive_target`.
                let probed =
                    CongestionResponse::proposed_probe_rate(sr.delivery_rate(), loss_event_rate);
                if should_exit_slow_start(
                    self.send_rate.get(),
                    probed,
                    sr.is_app_limited(),
                    observation.loss_blocks_delay_control,
                    observation.queue_building,
                ) {
                    self.slow_start = false;
                }
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
                app_limited: sr.is_app_limited(),
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
                    Some(new_rate) => self.set_send_rate(new_rate, now),
                    None => self.set_send_rate(self.send_rate.get(), now),
                }
            }
            CongestionDecision::LossBackoff { raw, floor, target } => {
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
                    self.set_send_rate(new_rate, now);
                }
            }
        }
        outcome.gentle_exit()
    }

    fn set_smooth_send_rate(&mut self, target_send_rate: f64, now: Instant) {
        let smooth_send_rate = self.send_rate.get() * (1. - SMOOTH_SEND_RATE_ALPHA)
            + target_send_rate * SMOOTH_SEND_RATE_ALPHA;
        self.set_send_rate(smooth_send_rate, now);
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
        self.set_send_rate(new_rate, now);
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
        self.recycle_recv_stage_bufs();
        self.move_recv_data();
        read_bytes
    }

    /// Return fully consumed recv-stage chunks to the packet-space buffer pool.
    /// The recv stage holds each payload chunk by ownership, so a drained chunk
    /// is the same allocation the packet space handed out and can be reused
    /// instead of dropped.
    fn recycle_recv_stage_bufs(&mut self) {
        let Self {
            recv_data_buf,
            pkt_recv_space,
            ..
        } = self;
        let pool = pkt_recv_space.reused_buf();
        recv_data_buf.drain_recycled(|buf| pool.put(buf));
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
        // Strict frame delivery keeps the original no-arg entry point; only
        // the opt-in fast-forward reaches the reorder-parameterised one.
        let frame = if self.frame_delivery.allow_reorder {
            self.pkt_recv_space.pop_complete_frame_with_reorder(true)
        } else {
            self.pkt_recv_space.pop_complete_frame()
        };
        if let Some(frame) = frame {
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
            // The recv stage takes ownership of the pooled chunk; it is
            // returned to the pool after the application has read past it.
            self.recv_data_buf.enqueue_owned(p);
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

    /// Ceiling on the in-flight window, in packets, derived from the
    /// peer-echoed lifetime minimum RTT and the windowed delivery peak.
    ///
    /// The rate-based window scales the *smoothed* RTT, which a standing
    /// queue or a starved ACK path inflates; this ceiling instead bounds
    /// in-flight against the path's observed minimum-RTT bandwidth-delay
    /// product.  The peak is a maximum over a multi-second window, so a
    /// momentary drain of the controller cannot depress it — and the ceiling
    /// itself cannot reduce the peak within that window, so it cannot
    /// self-limit the window it bounds.  It engages only once the smoothed
    /// RTT has left its propagation floor, so a quiet deterministic lane is
    /// never capped.
    fn cwnd_bdp_cap(&self) -> Option<usize> {
        // The interactive reorder-tolerant lane keeps its own tuned gate and
        // is deliberately excluded so this bulk-path safety cannot change its
        // latency floor.
        if self.congestion_response.reorder_tolerant() {
            return None;
        }
        let min_rtt = self.pkt_send_space.min_rtt()?;
        let peak = self.congestion_response.delivery_peak_rate()?;
        let smooth_rtt = self.pkt_send_space.smooth_rtt();
        if smooth_rtt < min_rtt.mul_f64(CWND_BDP_CAP_ENGAGE_RTT_FACTOR) {
            return None;
        }
        let bdp = min_rtt.as_secs_f64() * peak;
        if !bdp.is_finite() || bdp <= 0.0 {
            return None;
        }
        let cap = (bdp * CWND_BDP_CAP_SCALE as f64).round() as usize;
        Some(cap.max(INIT_CWND))
    }

    /// Apply a computed sender rate.
    ///
    /// This is the single bridge from a computed `f64` into the validated
    /// positive rate type.  A non-positive or non-finite computation (a
    /// zero-delivery ACK window, a division by a degenerate interval) is not a
    /// rate to settle at, so it degrades to the live rate instead of panicking
    /// the transport worker.  Every computed-rate site hands its raw `f64`
    /// here, so no call site can carry an unchecked `PosR::new(...).unwrap()`.
    fn set_send_rate(&mut self, rate: f64, now: Instant) {
        // While an outage-recovery epoch is open every rate writer is clamped
        // to INIT_SEND_RATE until a fresh post-outage sample closes the
        // epoch: the pre-outage backlog must not be released at a stale high
        // rate onto a just-restored link.
        let send_rate = if self.pkt_send_space.in_outage_recovery() {
            PosR::new(INIT_SEND_RATE).unwrap()
        } else {
            settle_computed_rate(rate, self.send_rate)
        };
        // The in-flight window is re-derived from the smoothed RTT, which a
        // starved or slow ACK path inflates.  Install a minimum-RTT
        // bandwidth-delay-product ceiling before the rate write so the ACK
        // path cannot raise the allowed in-flight without bound.  The ceiling
        // is derived from the peer-echoed lifetime minimum RTT and the
        // windowed delivery peak — neither of which the ceiling controls —
        // and engages only once the RTT has left its propagation floor.
        let cwnd_cap = self.cwnd_bdp_cap();
        self.pkt_send_space.set_cwnd_bdp_cap(cwnd_cap);
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
        self.send_rate_limiter.set_rate(send_rate, now);
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

    pub(crate) fn max_data_size_per_pkt(&self) -> usize {
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
            pacer_tokens_packets: self.send_rate_limiter.outdated_tokens(),
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
            // FEC actor state is owned by `Connection`, which overlays its
            // `FecStatsHandle` here via `Connection::metrics_snapshot`.
            fec_counters: None,
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
    use std::time::{Duration, Instant};

    use super::{
        DRAIN_FLOOR_PEAK_FRACTION, GENTLE_DRAIN_GAP_SHRINK, GENTLE_ENTER_RTTS,
        GENTLE_REENTRY_COOLDOWN, GENTLE_REENTRY_COOLDOWN_RTTS, HUGE_DATA_LOSS_CHECK_INTERVAL,
        INIT_SEND_RATE, MAX_SEND_DATA_BUF_LEN, MetricsGentleExitCause, ORDINARY_PROBE_MAX_GAIN,
        PERSISTENT_QUEUE_RTTVAR_FACTOR, QUEUE_RTT_FACTOR, QUEUE_RTT_FLOOR, QUEUE_TOL_RTT_FRACTION,
        WindowedRttMin, should_exit_slow_start,
    };
    use crate::delivery::byte_stream::send::send_data_buf_len;
    use crate::traffic_shaping::core::{
        CongestionResponse, has_spare_capacity, has_spare_capacity_interactive,
    };
    use crate::traffic_shaping::recovery::reorder_tolerance::{
        RTT_MIN_BUCKET, RTT_MIN_BUCKET_RTT_SCALE, cap_probe_target,
    };

    const TEST_MSS: usize = 1200;
    use crate::{
        ack::{AckBlocks, AckInterval},
        delivery::byte_stream::send::SEND_DATA_BUF_LEN,
        udp::NO_FEC_MSS,
    };

    /// The reorder-tolerant interactive lane bounds the per-probe rate increase
    /// so a reorder-inflated delivery-rate sample cannot spike the rate
    /// several-fold in one control RTT; the stock/bulk lane keeps the unbounded
    /// probe.
    #[test]
    fn reorder_probe_target_is_capped_only_on_the_reorder_lane() {
        let now = Instant::now();
        let (reorder, _) = super::ReliableLayer::new(
            crate::mss::Mss::try_new(NO_FEC_MSS).unwrap(),
            crate::delivery::frame::mode::FrameMode::enabled_reordering(),
            now,
        );
        let (stock, _) = super::ReliableLayer::new(
            crate::mss::Mss::try_new(NO_FEC_MSS).unwrap(),
            crate::delivery::frame::mode::FrameMode::enabled(),
            now,
        );
        let current = 400.0;
        let spurious = 6000.0;
        assert_eq!(
            cap_probe_target(
                reorder.congestion_response.reorder_tolerant(),
                current,
                spurious,
                0.0,
            ),
            current * ORDINARY_PROBE_MAX_GAIN,
            "the reorder lane must cap a spurious per-probe rate jump",
        );
        assert_eq!(
            cap_probe_target(
                stock.congestion_response.reorder_tolerant(),
                current,
                spurious,
                0.0,
            ),
            spurious,
            "the stock/bulk lane must keep the unbounded probe",
        );
        assert_eq!(
            cap_probe_target(
                reorder.congestion_response.reorder_tolerant(),
                current,
                500.0,
                0.0,
            ),
            500.0,
            "a legitimate target below the cap is unchanged",
        );
        assert_eq!(
            cap_probe_target(
                reorder.congestion_response.reorder_tolerant(),
                current,
                current * ORDINARY_PROBE_MAX_GAIN,
                0.0,
            ),
            current * ORDINARY_PROBE_MAX_GAIN,
            "the cap must not clip the ordinary probe's own legitimate maximum",
        );
        // The additive step is not delivery-scaled, so it is added after the
        // bound rather than clipped by it: a starved backlogged flow must keep
        // its absolute headroom even on the reorder lane.
        let additive = 300.0;
        assert_eq!(
            cap_probe_target(
                reorder.congestion_response.reorder_tolerant(),
                current,
                current * 5.0 + additive,
                additive,
            ),
            current * ORDINARY_PROBE_MAX_GAIN + additive,
            "the reorder cap must bound the delivery-scaled part but let the additive step through",
        );
    }

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
        assert_eq!(
            send_data_buf_len(crate::mss::Mss::try_new(NO_FEC_MSS).unwrap()),
            SEND_DATA_BUF_LEN
        );
    }

    #[test]
    fn send_data_buf_len_scales_to_whole_packets_above_default() {
        let mss = crate::mss::Mss::try_new(8192).unwrap();
        let len = send_data_buf_len(mss);
        let payload = mss.max_data_size_per_pkt();
        let expected = (MAX_SEND_DATA_BUF_LEN / payload) * payload;
        assert_eq!(len, expected);
        assert!(len > SEND_DATA_BUF_LEN);
        assert!(len <= MAX_SEND_DATA_BUF_LEN);

        // Spot checks for the larger-overhead wire format.
        let mss_2015 = crate::mss::Mss::try_new(2015).unwrap();
        let payload_2015 = mss_2015.max_data_size_per_pkt();
        assert_eq!(
            send_data_buf_len(mss_2015),
            (MAX_SEND_DATA_BUF_LEN / payload_2015) * payload_2015
        );
        let mss_9000 = crate::mss::Mss::try_new(9000).unwrap();
        let payload_9000 = mss_9000.max_data_size_per_pkt();
        assert_eq!(
            send_data_buf_len(mss_9000),
            (MAX_SEND_DATA_BUF_LEN / payload_9000) * payload_9000
        );

        // Sanity check for the default-MSS path.
        assert_eq!(
            send_data_buf_len(crate::mss::Mss::try_new(NO_FEC_MSS).unwrap()),
            SEND_DATA_BUF_LEN
        );
    }

    fn test_layer(now: Instant) -> super::ReliableLayer {
        let (layer, pacer) = super::ReliableLayer::new(
            crate::mss::Mss::try_new(TEST_MSS).unwrap(),
            crate::delivery::frame::mode::FrameMode::default(),
            now,
        );
        pacer.set_min_burst_for_test(64, now);
        layer
    }

    fn test_layer_reorder(now: Instant) -> super::ReliableLayer {
        // The congestion floor keys off `allow_reorder` only; keep the stock
        // byte-stream send path so `send_max`/`ack_all` drive it directly.
        let (layer, pacer) = super::ReliableLayer::new(
            crate::mss::Mss::try_new(TEST_MSS).unwrap(),
            crate::delivery::frame::mode::FrameMode {
                enabled: false,
                allow_reorder: true,
            },
            now,
        );
        pacer.set_min_burst_for_test(64, now);
        layer
    }

    fn test_layer_dedicated(now: Instant) -> super::ReliableLayer {
        let (layer, pacer) = super::ReliableLayer::new_at(
            crate::mss::Mss::try_new(TEST_MSS).unwrap(),
            crate::delivery::frame::mode::FrameMode::default(),
            crate::CongestionLane::Dedicated,
            now,
            crate::sequence::InitialSequences::ZERO,
        );
        pacer.set_min_burst_for_test(64, now);
        layer
    }

    /// The sender-rate setter is the single total bridge from a computed
    /// `f64`: a non-positive or non-finite rate must degrade to the live rate
    /// instead of panicking the transport worker.  This covers every computed
    /// ACK/congestion/recovery site, which now hand their raw f64 here.
    #[test]
    fn computed_rate_inputs_settle_at_the_live_rate() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let live = 4096.0;
        rl.set_send_rate(live, t0);
        assert_eq!(rl.send_rate.get(), live);

        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            rl.set_send_rate(bad, t0);
            assert_eq!(
                rl.send_rate.get(),
                live,
                "a {bad:?} computed rate must settle at the live rate"
            );
        }
    }

    /// The smoothed-probe site is a concrete computed-rate caller: its blend is
    /// non-finite when the target is, and the setter must still settle at the
    /// live rate rather than panicking.
    #[test]
    fn non_finite_smoothed_target_settles_at_the_live_rate() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        rl.set_send_rate(4096.0, t0);
        rl.set_smooth_send_rate(f64::NAN, t0);
        assert_eq!(rl.send_rate.get(), 4096.0);
        rl.set_smooth_send_rate(f64::INFINITY, t0);
        assert_eq!(rl.send_rate.get(), 4096.0);
    }

    /// The smoothed-probe bridge weights the *current* rate by `1 - alpha` and
    /// the probe target by `alpha` (`alpha = 0.4`), so one applied probe moves
    /// the live rate 40% of the way to the target.  A reciprocal swap of the
    /// two weights is a plausible off-by-one that no existing test catches;
    /// pin the exact blend.
    #[test]
    fn smooth_send_rate_blends_the_current_rate_with_the_configured_alpha() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let current = 1000.0;
        let target = 2000.0;
        rl.set_send_rate(current, t0);
        rl.set_smooth_send_rate(target, t0);
        // alpha = 0.4: 1000 * 0.6 + 2000 * 0.4 = 1400.
        let expected = 1400.0;
        let blended = rl.send_rate.get();
        assert!(
            (blended - expected).abs() < 1e-6,
            "the smoothed rate must be current * 0.6 + target * 0.4 = {expected}, got {blended}"
        );
    }

    /// A genuine *standing queue* on the reorder-tolerant lane must still be
    /// flagged and drained.  This guards the floor-window change against
    /// trading away queue detection for path-shift recovery.
    #[test]
    fn reorder_lane_still_drains_a_standing_queue() {
        let t0 = Instant::now();
        let mut rl = test_layer_reorder(t0);
        let mut t = t0;

        let ramp_rtt = Duration::from_millis(10);
        for _ in 0..40 {
            send_max(&mut rl, t);
            t += ramp_rtt;
            ack_all(&mut rl, Some(ramp_rtt), t);
        }
        let held_rate = rl.send_rate.get();

        // Mirror the default-lane standing-queue test: transient jitter first
        // to settle rttvar, then a sustained ~260 ms queue.  The mirror keeps
        // the two lanes comparable and only the floor bucket differs.
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

        for _ in 0..30 {
            rl.sample_rtt(Duration::from_millis(260), t);
            t += Duration::from_micros(100);
        }
        let seq = send_one(&mut rl, t);
        t += Duration::from_millis(260);
        ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(260), t);

        assert_eq!(
            rl.last_congestion_action,
            Some(crate::metrics::MetricsCongestionAction::DelayDrain),
            "a standing queue must still trigger a drain"
        );
        assert!(
            rl.send_rate.get() < held_rate,
            "a standing queue must reduce the rate below {held_rate}, got {}",
            rl.send_rate.get()
        );
    }

    /// The other direction of windowed-floor staleness: a genuine latency STEP
    /// (a path shift with no queue).  The floor is a windowed minimum of the
    /// smoothed RTT; the reorder-tolerant lane's short bucket must scale with
    /// the *established* floor, not the step-inflated incoming sample, or the
    /// stale pre-shift floor stays alive for seconds and the delay controller
    /// drains the interactive send rate on a path with no queue at all.
    #[test]
    fn latency_step_up_does_not_drain_the_reorder_lane() {
        let t0 = Instant::now();
        let mut rl = test_layer_reorder(t0);
        let mut t = t0;

        // Ramp on a brisk 10 ms RTT so slow start exits and the rate climbs.
        for _ in 0..60 {
            send_max(&mut rl, t);
            t += Duration::from_millis(10);
            ack_all(&mut rl, Some(Duration::from_millis(10)), t);
            t += Duration::from_nanos(1);
        }
        let ramp_rate = rl.send_rate.get();
        assert!(ramp_rate > 2.0 * INIT_SEND_RATE, "ramp_rate={ramp_rate}");

        // Step the path RTT up to 200 ms and hold it.  Every rate sample must
        // stay on the non-drain path: the step is not a queue.
        let mut min_rate = ramp_rate;
        for _ in 0..25 {
            send_max(&mut rl, t);
            t += Duration::from_millis(200);
            ack_all(&mut rl, Some(Duration::from_millis(200)), t);
            t += Duration::from_nanos(1);
            assert_ne!(
                rl.last_congestion_action,
                Some(crate::metrics::MetricsCongestionAction::DelayDrain),
                "a bare latency step (no queue) must not drain the send rate"
            );
            min_rate = min_rate.min(rl.send_rate.get());
        }
        assert!(
            min_rate > ramp_rate / 2.0,
            "the bare latency step must not collapse the send rate: {min_rate} vs {ramp_rate}"
        );
    }

    #[test]
    fn application_write_waiter_registration_is_drop_scoped() {
        let now = Instant::now();
        let layer = test_layer(now);
        let stock = |layer: &super::ReliableLayer| {
            has_spare_capacity(
                layer.can_send_tail_fec(now),
                layer.application_write_waiters(),
                layer.queue_building(),
            )
        };
        assert_eq!(layer.metrics_at(now).application_write_waiters, 0);
        assert!(stock(&layer));
        {
            let _first = layer.application_write_waiter();
            assert_eq!(layer.metrics_at(now).application_write_waiters, 1);
            assert!(
                !stock(&layer),
                "a waiting application writer means the empty staging queue is not spare capacity"
            );
            {
                let _second = layer.application_write_waiter();
                assert_eq!(layer.metrics_at(now).application_write_waiters, 2);
            }
            assert_eq!(layer.metrics_at(now).application_write_waiters, 1);
        }
        assert_eq!(layer.metrics_at(now).application_write_waiters, 0);
    }

    /// The interactive FEC capacity gate keeps the genuinely-spare conditions
    /// (a sendable window, no waiting application writer, no queue growth) but
    /// deliberately does NOT require the staging queue to be empty or the tail
    /// to be settled: the in-stream group parity is designed to flush
    /// mid-burst with later application symbols still staged, and on a lossy
    /// interactive lane a pending retransmit/tail probe is the norm rather
    /// than the exception.  Application backpressure still closes it.
    #[test]
    fn interactive_spare_capacity_ignores_a_nonempty_stage_but_not_waiters() {
        let now = Instant::now();
        let mut layer = test_layer(now);
        let stock = |layer: &super::ReliableLayer| {
            has_spare_capacity(
                layer.can_send_tail_fec(now),
                layer.application_write_waiters(),
                layer.queue_building(),
            )
        };
        let interactive = |layer: &super::ReliableLayer| {
            has_spare_capacity_interactive(
                layer.pkt_send_space().accepts_new_pkt(),
                layer.application_write_waiters(),
                layer.queue_building(),
            )
        };
        assert!(stock(&layer));
        assert!(interactive(&layer));

        let payload = vec![0u8; 64];
        assert!(layer.send_data_buf(&payload, now).unwrap() > 0);
        assert!(
            !stock(&layer),
            "a non-empty stage must close the stock tail gate"
        );
        assert!(
            interactive(&layer),
            "the interactive gate must stay open with a non-empty stage"
        );

        let _waiter = layer.application_write_waiter();
        assert!(
            !interactive(&layer),
            "application backpressure must close the interactive gate too"
        );
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

    /// The huge-loss persistence timer is condition-clocked, not a one-shot
    /// deadline: a flight that is repaired before the timer matures clears it,
    /// so a *fresh* huge-loss episode must re-earn the full `2 * RTO`
    /// persistence instead of inheriting the earlier episode's elapsed time.
    /// If the clear were dropped, the second episode's first positive sample
    /// would fire the backoff instantly, stalling a send path whose earlier
    /// loss had already been repaired.
    #[test]
    fn huge_loss_persistence_timer_restarts_after_the_flight_is_repaired() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);

        // Episode A: a stalled flight becomes a huge-loss sample once its RTO
        // elapses, arming the `2 * RTO` persistence timer. The first positive
        // check must arm, not fire.
        send_burst(&mut rl, 20, t0);
        let rto = rl.pkt_send_space().rto_duration();
        let lost_a = t0 + rto + Duration::from_millis(1);
        assert!(
            rl.huge_data_loss_gate(lost_a).is_none(),
            "the first positive huge-loss check must arm the persistence timer, not fire"
        );

        // The flight is repaired before the timer matures. That is forward
        // progress, so the next check must CLEAR the persistence timer rather
        // than let a later positive sample inherit the elapsed time.
        ack_all(&mut rl, None, lost_a + Duration::from_millis(1));
        let cleared_at = lost_a + HUGE_DATA_LOSS_CHECK_INTERVAL + Duration::from_millis(1);
        assert!(
            rl.huge_data_loss_gate(cleared_at).is_none(),
            "a repaired flight must not fire the huge-loss gate"
        );

        // Episode B: a fresh stalled flight observed after the original timer
        // would have matured (`lost_a + 2 * rto`). If the clear above were
        // missing, this very first positive sample would find the stale start
        // and fire immediately; with the clear it must re-earn the full
        // `2 * RTO` persistence.
        let send_b = lost_a + 3 * rto;
        send_burst(&mut rl, 20, send_b);
        let lost_b = send_b + rto + Duration::from_millis(1);
        assert!(
            rl.huge_data_loss_gate(lost_b).is_none(),
            "a fresh huge-loss episode must re-earn the full 2 * RTO persistence"
        );
        assert!(
            rl.huge_data_loss_gate(lost_b + rto + rto - Duration::from_millis(1))
                .is_none(),
            "the fresh episode must not fire before 2 * RTO"
        );
        assert!(
            rl.huge_data_loss_gate(
                lost_b + rto + rto + HUGE_DATA_LOSS_CHECK_INTERVAL + Duration::from_millis(1),
            )
            .is_some(),
            "the fresh episode must fire once 2 * RTO persist"
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

    /// Like [`send_one`] but keeps at least one full packet staged after the
    /// send, so the lane is never application-limited at the following ACK.
    ///
    /// The delay controller only drains a lane that actually has a
    /// self-inflicted queue: an application-limited sender has less queued
    /// than the pipe can carry, so a standing delay belongs to cross-traffic
    /// and the drain path is (correctly) skipped.  Gentle-mode drain tests
    /// therefore must drive a *backlogged* lane, which is the only kind that
    /// can build its own queue.  One packet is sent per call so the per-round
    /// timing the tests rely on is preserved.
    fn send_queued(rl: &mut super::ReliableLayer, now: Instant) -> crate::sequence::SequenceNumber {
        let payload_len = rl.max_data_size_per_pkt();
        while rl.send_data_buf.len() < 2 * payload_len {
            let free = rl.send_data_buf.capacity() - rl.send_data_buf.len();
            if free < payload_len {
                break;
            }
            let payload = vec![0u8; payload_len];
            assert_eq!(rl.send_data_buf(&payload, now).unwrap(), payload_len);
        }
        let mut pkt = vec![0u8; TEST_MSS];
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
            crate::mss::Mss::try_new(TEST_MSS).unwrap(),
            crate::delivery::frame::mode::FrameMode::enabled(),
            now,
        );
        frame.send_fin_buf();
        assert_eq!(
            frame.send_frame_buf(b"after FIN", now),
            Err(std::io::ErrorKind::BrokenPipe.into())
        );
    }

    /// A staged frame is unsent application data, and the frame staging buffer
    /// is its only source of truth: the empty-stage predicate must report
    /// `false` between the frame write and the packetization that drains the
    /// stage.  Reporting empty there lets a `send_buf_empty` /
    /// `all_sent_data_acked` barrier return with the frame still staged.
    #[test]
    fn a_staged_frame_is_not_an_empty_send_stage() {
        let now = Instant::now();
        let (mut frame, pacer) = super::ReliableLayer::new(
            crate::mss::Mss::try_new(TEST_MSS).unwrap(),
            crate::delivery::frame::mode::FrameMode::enabled(),
            now,
        );
        pacer.set_min_burst_for_test(64, now);
        assert!(
            frame.is_send_buf_empty(),
            "a fresh frame lane starts with an empty send stage"
        );
        frame.send_frame_buf(b"frame", now).unwrap();
        assert!(
            !frame.is_send_buf_empty(),
            "a frame staged and not yet packetized is not an empty send stage"
        );
        assert!(!frame.is_no_data_to_send());
        let mut pkt = vec![0u8; TEST_MSS];
        let packetized = frame
            .send_data_pkt(&mut pkt, now)
            .expect("the staged frame must packetize");
        assert!(
            packetized.frame_len.is_some(),
            "the frame's first packet carries the frame length"
        );
        assert_eq!(frame.pending_frame_bytes(), 0, "the frame stage drained");
        assert!(
            frame.is_send_buf_empty(),
            "once packetized, the frame stage is empty again"
        );
        assert!(
            !frame.is_no_data_to_send(),
            "the packetized frame is still in flight"
        );
    }

    /// A write close is idempotent once its FIN packet is in flight: both the
    /// application's `shutdown` and the session's write-close arm call
    /// `send_fin_buf`, and a repeated close while the FIN is in flight must not
    /// mint a second FIN packet (a fresh sequence number carrying no data).
    #[test]
    fn a_repeated_write_close_does_not_mint_a_second_fin_packet() {
        let now = Instant::now();
        let mut rl = test_layer(now);
        rl.send_fin_buf();
        let mut pkt = vec![0u8; TEST_MSS];
        let fin = rl
            .send_data_pkt(&mut pkt, now)
            .expect("the requested FIN must be packetized");
        assert!(matches!(fin.data_written, super::DataPktPayload::Fin));
        assert_eq!(rl.pkt_send_space().num_in_flight_pkts(), 1);
        // The write half may close again while that FIN is still in flight:
        // the in-flight FIN already is the close, so nothing new is minted.
        rl.send_fin_buf();
        let mut pkt = vec![0u8; TEST_MSS];
        assert!(
            rl.send_data_pkt(&mut pkt, now).is_none(),
            "a repeated write close minted a second FIN packet"
        );
        assert_eq!(
            rl.pkt_send_space().num_in_flight_pkts(),
            1,
            "a repeated write close grew the in-flight send window"
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

    /// A cold-start delivery sample on a long-RTT, high-BDP path. The shared
    /// lane uses the stock exit: a single app-limited sample below the probe
    /// target leaves slow start. Loss and queue growth also leave slow start.
    #[test]
    fn shared_lane_uses_the_stock_slow_start_exit() {
        let send = 140.0;
        let probed = 156.0; // 1.5 * delivery(104) while the pipe is still filling

        assert!(
            should_exit_slow_start(send, probed, true, false, false),
            "an app-limited cold-start sample must leave slow start"
        );
        assert!(
            should_exit_slow_start(send, probed, false, false, false),
            "send below the probe target must leave slow start"
        );
        assert!(
            !should_exit_slow_start(160.0, probed, false, false, false),
            "send above the probe target may stay in slow start"
        );
        assert!(
            should_exit_slow_start(send, probed, false, true, false),
            "loss must exit slow start"
        );
        assert!(
            should_exit_slow_start(send, probed, false, false, true),
            "queue growth must exit slow start"
        );
    }

    /// The shared lane's slow-start exit threshold is the ordinary
    /// *multiplicative* probe ceiling, not the additive step.  The additive
    /// target is always `current + step`, so it is higher than the ceiling for
    /// a backlogged flow; using it would exit slow start more aggressively than
    /// the historical stock exit.  The lane deliberately keeps the lower
    /// ceiling.
    #[test]
    fn shared_lane_slow_start_exits_on_the_multiplicative_ceiling_not_the_additive_target() {
        let delivery = 100.0;
        let proposed = CongestionResponse::proposed_probe_rate(delivery, Some(0.0));
        assert_eq!(
            proposed, 150.0,
            "the multiplicative ceiling is 1.5x delivery"
        );
        // A 100 ms control RTT gives a 300 pkt/s absolute additive step, so the
        // shared lane's additive target (400) is far above the multiplicative
        // ceiling (150).
        let additive_target = delivery + 300.0;
        assert!(additive_target > proposed);
        // A send rate between the two thresholds: the ceiling keeps slow start
        // while send is above it, whereas the additive target would exit.  The
        // lane uses the ceiling, i.e. the historical stock exit.
        assert!(
            !should_exit_slow_start(200.0, proposed, false, false, false),
            "the multiplicative ceiling must keep slow start while send is above it"
        );
        assert!(
            should_exit_slow_start(200.0, additive_target, false, false, false),
            "the additive target would exit slow start for the same sample"
        );
        // Below the ceiling both thresholds exit.
        assert!(should_exit_slow_start(140.0, proposed, false, false, false));
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

    /// The dedicated lane's windowed fast start ramps up to the delivered rate
    /// and then leaves slow start once the capped delivery plateaus, instead of
    /// growing the pacer rate without bound.  A bottleneck of `cap` packets per
    /// control RTT is modelled by only ever acknowledging a fixed prefix.
    #[test]
    fn dedicated_fast_start_plateaus_at_a_capped_delivery() {
        let t0 = Instant::now();
        let mut rl = test_layer_dedicated(t0);
        let rtt = Duration::from_millis(100);
        let cap = 40u64; // 40 packets / 100 ms = 400 packets/second
        let mut acked = 0u64;
        let mut t = t0;

        for _ in 0..40 {
            send_max(&mut rl, t);
            t += rtt;
            let next = rl.pkt_send_space().next_seq().to_wire();
            acked = (acked + cap).min(next);
            if acked > 0 {
                ack_prefix(&mut rl, acked, rtt, t);
            }
            t += Duration::from_nanos(1);
        }

        assert!(
            !rl.slow_start,
            "a flat capped delivery must end the fast start"
        );
        assert!(
            rl.send_rate.get() < 4.0 * cap as f64 / rtt.as_secs_f64(),
            "the settled rate must stay bounded near the capped delivery, got {}",
            rl.send_rate.get()
        );
    }

    /// A stalled delivery ends the dedicated fast start rather than growing
    /// the pacer rate without bound.
    #[test]
    fn dedicated_fast_start_plateaus_before_an_unbounded_rate() {
        let t0 = Instant::now();
        let mut rl = test_layer_dedicated(t0);
        let rtt = Duration::from_millis(100);
        let mut t = t0;
        // Fill the pipe for two RTTs so the first measurement window ramps
        // (the very first ack only seeds the window).
        send_max(&mut rl, t);
        t += rtt;
        ack_all(&mut rl, Some(rtt), t);
        send_max(&mut rl, t);
        t += rtt;
        ack_all(&mut rl, Some(rtt), t);
        let ramped = rl.send_rate.get();
        assert!(ramped > INIT_SEND_RATE, "the first window must ramp");

        // Acknowledge only one additional packet per window: the windowed
        // delivery collapses, so the next window must plateau and exit.
        let mut acked = rl.pkt_send_space().next_seq().to_wire();
        for _ in 0..8 {
            send_max(&mut rl, t);
            t += rtt;
            let next = rl.pkt_send_space().next_seq().to_wire();
            acked = (acked + 1).min(next);
            if acked > 0 {
                ack_prefix(&mut rl, acked, rtt, t);
            }
            t += Duration::from_nanos(1);
            if !rl.slow_start {
                break;
            }
        }
        assert!(
            !rl.slow_start,
            "a stalled delivery must end the dedicated fast start"
        );
        assert!(
            rl.send_rate.get() <= ramped,
            "the plateau must not raise the rate on a stalled delivery"
        );
    }

    #[test]
    fn dedicated_fast_start_survives_a_zero_fresh_ack_window() {
        let t0 = Instant::now();
        let mut rl = test_layer_dedicated(t0);
        let rtt = Duration::from_millis(100);
        let mut t = t0;
        // Seed the window and close two real delivery windows so the
        // two-windows-past delivery baseline exists.
        for _ in 0..3 {
            send_max(&mut rl, t);
            t += rtt;
            ack_all(&mut rl, Some(rtt), t);
            t += Duration::from_nanos(1);
        }
        assert!(rl.slow_start, "the ramp must still be running");
        let ramped = rl.send_rate.get();

        // A full control-RTT window later, replay the already-acknowledged
        // prefix: the window carries no fresh delivery.
        let acked = rl.pkt_send_space().next_seq().to_wire();
        assert!(acked > 0, "the ramp must have sent packets");
        t += rtt;
        ack_prefix(&mut rl, acked, rtt, t);

        assert!(
            !rl.slow_start,
            "a zero-delivery window must leave the dedicated fast start"
        );
        assert!(
            rl.send_rate.get() > 0.0 && rl.send_rate.get().is_finite(),
            "the settled rate must stay positive and finite, got {}",
            rl.send_rate.get()
        );
        assert!(
            rl.send_rate.get() <= ramped,
            "a zero-delivery settle must not raise the rate"
        );
    }

    /// A lone non-congestion (iid) loss must not abort the dedicated fast
    /// start. A single loss among many deliveries is a loss rate far below the
    /// 20% congestion threshold: the ramp settles at the windowed delivered
    /// rate and keeps running, so it reaches the delivered plateau instead of
    /// handing off to the ordinary probe's overshoot/drain.
    #[test]
    fn dedicated_fast_start_survives_a_lone_iid_loss() {
        let t0 = Instant::now();
        let mut rl = test_layer_dedicated(t0);
        let rtt = Duration::from_millis(100);
        let mut t = t0;
        // Seed the window, close one full-delivery window so the ramp is
        // running, then record a lone iid loss and close the next window.
        // The two-window-past baseline is still unset, so the delivery
        // plateau cannot fire and the loss branch is what is exercised.
        for i in 0..3 {
            send_max(&mut rl, t);
            t += rtt;
            if i == 2 {
                // One iid loss among many deliveries is a rate far below the
                // 20% congestion threshold.
                rl.pkt_send_space.inject_loss_event(t);
            }
            ack_all(&mut rl, Some(rtt), t);
            t += Duration::from_nanos(1);
        }

        assert!(
            rl.slow_start,
            "a lone iid loss must not abort the dedicated fast start"
        );
        assert!(
            rl.send_rate.get() > 0.0 && rl.send_rate.get().is_finite(),
            "the settled rate must stay positive and finite, got {}",
            rl.send_rate.get()
        );
        assert!(
            rl.congestion_loss_ratio().is_some_and(|loss| loss < 0.2),
            "the injected loss must be classified as non-congestion, got {:?}",
            rl.congestion_loss_ratio()
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
            let seq = send_queued(&mut rl, t);
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
            let seq = send_queued(&mut rl, t);
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
            let seq = send_queued(&mut rl, t);
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
            let seq = send_queued(&mut rl, t);
            t += Duration::from_millis(250);
            ack_seq(&mut rl, seq.to_wire(), Duration::from_millis(200), t);
        }
        feed_rtt(&mut rl, 20, Duration::from_millis(800), t);
        t += Duration::from_millis(1);
        let enter_start = t;
        loop {
            let seq = send_queued(&mut rl, t);
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
            let seq = send_queued(&mut rl, t);
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
        let progress_seq = send_queued(&mut rl, t);
        t += Duration::from_millis(50);
        ack_seq(
            &mut rl,
            progress_seq.to_wire(),
            Duration::from_millis(50),
            t,
        );

        let stall_seq = send_queued(&mut rl, t);
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
            crate::mss::Mss::try_new(mss).unwrap(),
            crate::delivery::frame::mode::FrameMode::enabled(),
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
            on_wire_first <= mss - crate::mss::TRUNCATION_DETECTION_BYTES,
            "first frame packet on-wire size {on_wire_first} must stay below the MSS (truncated-datagram detection headroom), mss={mss}"
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
            crate::mss::Mss::try_new(mss).unwrap(),
            crate::delivery::frame::mode::FrameMode::enabled(),
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

    /// Ordered, gap-free frame delivery: a complete later frame must be
    /// withheld behind an earlier frame's repairable hole — NOT handed up out
    /// of order — and both frames must surface in sequence order once the
    /// hole fills (retransmission or reordering). Before the fix, the
    /// reassembly scan delivered whatever complete frame it found, so frame B
    /// was handed up past frame A's missing interior packet: mux's
    /// reassembly then saw an out-of-order byte range and tore the lane down.
    #[test]
    fn frame_delivery_withholds_a_complete_later_frame_behind_a_hole_and_delivers_in_order() {
        let now = Instant::now();
        let mut rl = super::ReliableLayer::new(
            crate::mss::Mss::try_new(NO_FEC_MSS).unwrap(),
            crate::delivery::frame::mode::FrameMode::enabled(),
            now,
        )
        .0;
        let seq = crate::sequence::SequenceNumber::from_wire;

        // Frame A: four 4-byte packets at seqs 0..=3, declared frame_len 16.
        // seq 1 is lost, so A cannot reassemble yet.
        assert!(rl.recv_data_pkt(seq(0), Some(16), b"AAAA").is_new());
        assert!(rl.recv_data_pkt(seq(2), None, b"CCCC").is_new());
        assert!(rl.recv_data_pkt(seq(3), None, b"DDDD").is_new());

        // Frame B: two 4-byte packets at seqs 4..=5, declared frame_len 8,
        // fully received.
        assert!(rl.recv_data_pkt(seq(4), Some(8), b"EEEE").is_new());
        assert!(rl.recv_data_pkt(seq(5), None, b"FFFF").is_new());

        // A is incomplete, B is complete — but B starts past A's hole at seq
        // 1. Frame delivery is in-order and gap-free: B must be WITHHELD
        // until A repairs.
        let err = rl
            .recv_frame_buf()
            .expect_err("a frame past a hole must not be delivered out of order");
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);

        // A's missing packet arrives (retransmission or reordering): A
        // reassembles and delivers FIRST ...
        assert!(rl.recv_data_pkt(seq(1), None, b"BBBB").is_new());
        let frame = rl
            .recv_frame_buf()
            .expect("frame delivery must not error")
            .expect("the earlier frame must reassemble once its hole is filled");
        assert_eq!(frame, b"AAAABBBBCCCCDDDD");

        // ... then B, in order.
        let frame = rl
            .recv_frame_buf()
            .expect("frame delivery must not error")
            .expect("the withheld later frame must deliver after the earlier frame");
        assert_eq!(frame, b"EEEEFFFF");
    }

    /// Opt-in receiver-side fast-forward: with `allow_reorder` set, a complete
    /// later frame is handed up immediately even though an earlier frame's
    /// hole is unrepaired, while the in-order cursor stays pinned. Filling the
    /// hole delivers the earlier frame and never redelivers the fast-forwarded
    /// one. This is the interactive-lane mode where mux's per-stream
    /// reassembly restores ordering; the strict default is asserted by the
    /// test above.
    #[test]
    fn frame_delivery_reordering_delivers_a_complete_later_frame_past_a_hole() {
        let now = Instant::now();
        let mut rl = super::ReliableLayer::new(
            crate::mss::Mss::try_new(NO_FEC_MSS).unwrap(),
            crate::delivery::frame::mode::FrameMode::enabled_reordering(),
            now,
        )
        .0;
        let seq = crate::sequence::SequenceNumber::from_wire;

        // Frame A: four 4-byte packets at seqs 0..=3, declared frame_len 16.
        // seq 1 is lost, so A cannot reassemble yet.
        assert!(rl.recv_data_pkt(seq(0), Some(16), b"AAAA").is_new());
        assert!(rl.recv_data_pkt(seq(2), None, b"CCCC").is_new());
        assert!(rl.recv_data_pkt(seq(3), None, b"DDDD").is_new());

        // Frame B: two 4-byte packets at seqs 4..=5, declared frame_len 8,
        // fully received.
        assert!(rl.recv_data_pkt(seq(4), Some(8), b"EEEE").is_new());
        assert!(rl.recv_data_pkt(seq(5), None, b"FFFF").is_new());

        // B is delivered immediately despite A's unrepaired hole.
        let frame = rl
            .recv_frame_buf()
            .expect("frame delivery must not error")
            .expect("the complete later frame must be fast-forwarded");
        assert_eq!(frame, b"EEEEFFFF");

        // The in-order cursor is still pinned at A's undelivered front.
        assert_eq!(
            rl.pkt_recv_space().next_seq(),
            Some(seq(0)),
            "fast-forward must not advance the in-order cursor"
        );

        // A's missing packet arrives: A reassembles and delivers, the cursor
        // collapses A's and B's tombstones, and B is not redelivered.
        assert!(rl.recv_data_pkt(seq(1), None, b"BBBB").is_new());
        let frame = rl
            .recv_frame_buf()
            .expect("frame delivery must not error")
            .expect("the earlier frame must reassemble once its hole is filled");
        assert_eq!(frame, b"AAAABBBBCCCCDDDD");
        assert_eq!(
            rl.pkt_recv_space().next_seq(),
            Some(seq(6)),
            "the cursor must collapse the tombstones after the hole fills"
        );
        assert_eq!(
            rl.recv_frame_buf().expect_err("no frame remains").kind(),
            std::io::ErrorKind::WouldBlock
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
        rl.set_send_rate(10_000.0, restore_time + Duration::from_millis(1));
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

    /// The send path must apply the *linear* huge-loss backoff, not the
    /// exponential halving that the removed `LINEAR_BACKOFF` const used to
    /// select.  A plain `< before` check cannot tell the two apart when the
    /// linear step lands far below `before / 2`; this drives the real
    /// `send_data_pkt` call site and asserts the rate collapses all the way to
    /// `MIN_SEND_RATE` (the linear step pins to the target once the loss has
    /// persisted for an RTT), which the halving branch does not.
    #[test]
    fn huge_loss_backoff_on_the_send_path_is_linear_not_exponential() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        send_burst(&mut rl, 20, t0);
        assert!(rl.huge_data_loss_gate(t0).is_none());
        let mut pkt = vec![0u8; TEST_MSS];
        let mut t = t0 + Duration::from_secs(2) + Duration::from_millis(1);
        let mut fired = false;
        for _ in 0..600 {
            let before = rl.send_rate.get();
            // The send path's first action is the huge-loss backoff.
            let _ = rl.send_data_pkt(&mut pkt, t);
            if rl.last_congestion_action
                == Some(crate::metrics::MetricsCongestionAction::HugeLossBackoff)
            {
                fired = true;
                let after = rl.send_rate.get();
                assert!(
                    after < before,
                    "the huge-loss backoff must lower the send rate"
                );
                assert_eq!(
                    after,
                    super::MIN_SEND_RATE,
                    "the send path must apply the linear step (which reaches MIN_SEND_RATE \
                     once the loss has persisted for an RTT), not the exponential halving: \
                     before={before} after={after}"
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
        rl.set_congestion_metrics_enabled(true);
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

    /// The additive step must reach the applied *send rate* on the
    /// reorder-tolerant `Shared` lane (the production mux interactive lane's
    /// congestion mode, `allow_reorder`).  The reorder probe cap bounds only
    /// the delivery-scaled part of a probe, so it must not clip the lane's
    /// absolute additive headroom on the wire.
    ///
    /// The lane is ramped out of slow start, its control RTT is lifted to
    /// ~100 ms (additive step ~= `SHARED_ADDITIVE_PROBE_STEP`), then a probe
    /// from a deliberately depressed current rate (100 pkt/s) with a delivery
    /// sample below that rate is applied.  A multiplicative-only probe, or the
    /// old delivery-scaled cap that dropped the additive step, cannot raise the
    /// rate at all here; the additive step raises the smoothed rate to
    /// `0.6 * current + 0.4 * (current + additive)`, which exceeds the 1.5x
    /// delivery-scaled cap that used to clip it.
    #[test]
    fn reorder_tolerant_shared_lane_applies_the_additive_step_to_the_send_rate() {
        let t0 = Instant::now();
        let mut rl = test_layer_reorder(t0);
        assert!(rl.congestion_response.reorder_tolerant());
        let mut t = t0;
        // Ramp out of slow start so the ordinary bandwidth-probe branch is
        // active.
        for _ in 0..40 {
            send_max(&mut rl, t);
            t += Duration::from_millis(10);
            ack_all(&mut rl, Some(Duration::from_millis(10)), t);
        }
        assert!(!rl.slow_start, "slow start must exit during the ramp");
        // Lift the control RTT (and, on the reorder lane, the floor) to 100 ms
        // so the absolute additive step is a large fraction of the depressed
        // current rate and therefore exceeds the 1.5x delivery-scaled cap.
        for _ in 0..60 {
            rl.sample_rtt(Duration::from_millis(100), t);
            t += Duration::from_millis(10);
        }
        for _ in 0..10 {
            send_max(&mut rl, t);
            t += Duration::from_millis(100);
            ack_all(&mut rl, Some(Duration::from_millis(100)), t);
            t += Duration::from_nanos(1);
        }
        assert_eq!(
            rl.last_congestion_action,
            Some(crate::metrics::MetricsCongestionAction::BandwidthProbe),
            "the clean reorder lane must take an ordinary bandwidth probe"
        );

        let current = 100.0;
        rl.set_send_rate(current, t);
        send_max(&mut rl, t);
        let rtt = Duration::from_millis(100);
        t += rtt;
        ack_all(&mut rl, Some(rtt), t);

        let additive = crate::CongestionLane::Shared.ordinary_additive_probe_step(rl.control_rtt());
        assert!(additive > 0.0, "the shared lane carries an additive step");
        let rate = rl.send_rate.get();
        assert!(
            rate > current + 0.2 * additive,
            "the additive step must reach the send rate: {rate} vs current {current} + 0.2 * {additive}"
        );
        assert!(
            rate < current + additive,
            "the applied rate is the smoothed probe target, below the full target: {rate}"
        );
        assert!(
            rate > current * ORDINARY_PROBE_MAX_GAIN,
            "the applied rate {rate} must exceed the 1.5x delivery-scaled cap {}: the reorder cap must not clip the additive step",
            current * ORDINARY_PROBE_MAX_GAIN,
        );
    }

    #[test]
    fn unchanged_smooth_rate_refreshes_send_space_without_touching_the_pacer() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let pacer = rl.send_rate_limiter.clone();
        let current = rl.send_rate.get();
        let tokens_before = {
            assert!(pacer.take_at_most_tokens(usize::MAX, t0) > 0);
            pacer.outdated_tokens()
        };
        let later = t0 + Duration::from_millis(10);
        rl.set_smooth_send_rate(current, later);
        assert_eq!(
            pacer.outdated_tokens(),
            tokens_before,
            "a no-op smoothing decision must not refresh or rebuild the pacer"
        );
        rl.set_smooth_send_rate(current * 2.0, later);
        assert!(rl.send_rate.get() > current);
        assert!(
            pacer.outdated_tokens() > tokens_before,
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
        rl.set_send_rate(INIT_SEND_RATE, stalled_at);
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

    /// The minimum-RTT bandwidth-delay-product ceiling bounds the in-flight
    /// window once the smoothed RTT has left its propagation floor, and it is
    /// deliberately excluded on the interactive reorder-tolerant lane so the
    /// bulk-path safety cannot change that lane's latency floor.  Drive the
    /// real rate-write path (`set_send_rate`) and read the window back.
    ///
    /// `min_rtt` is a lifetime minimum, so seeding one low sample and then a
    /// high-RTT steady state leaves the engage check (smooth_rtt >= 3 *
    /// min_rtt) satisfied while the propagation floor stays low.  The expected
    /// ceiling is written literally so a changed scale cannot move both sides.
    #[test]
    fn bdp_cap_bounds_the_window_off_the_floor_and_excludes_the_reorder_lane() {
        use crate::traffic_shaping::recovery::pkt_send_space::{
            CWND_BDP_CAP_SCALE, CWND_SEND_RATE_SCALE, INIT_CWND,
        };

        let t0 = Instant::now();
        let min_rtt = Duration::from_millis(10);
        let high_rtt = Duration::from_millis(100);
        let peak = 1000.0;
        let rate = 10_000.0;

        // bdp = 10 ms * 1000 pkt/s = 10 pkts; the ceiling is 6 * bdp = 60,
        // above the 32-packet initial-window floor.  The raw rate-based window
        // is sRTT * rate * 8 = 8000, so the ceiling must bind.
        let expected_cap = 60usize;
        assert_eq!(
            ((min_rtt.as_secs_f64() * peak * CWND_BDP_CAP_SCALE as f64).round() as usize)
                .max(INIT_CWND),
            expected_cap,
            "the test's expected ceiling must match the scenario's arithmetic"
        );
        let uncapped = (high_rtt.as_secs_f64() * rate).round() as usize * CWND_SEND_RATE_SCALE;
        assert!(
            expected_cap < uncapped,
            "the scenario must make the ceiling bind"
        );

        // One low sample seeds the propagation floor; a high-RTT steady state
        // then leaves the floor behind.
        fn drive(
            rl: &mut super::ReliableLayer,
            t0: Instant,
            min_rtt: Duration,
            high_rtt: Duration,
        ) -> Instant {
            let mut t = t0;
            rl.sample_rtt(min_rtt, t);
            for _ in 0..60 {
                t += Duration::from_millis(1);
                rl.sample_rtt(high_rtt, t);
            }
            t
        }

        // Stock lane: the ceiling binds the window.
        let mut stock = test_layer(t0);
        let t = drive(&mut stock, t0, min_rtt, high_rtt);
        stock.congestion_response.delivery_peak().update(t, peak);
        stock.set_send_rate(rate, t);
        assert_eq!(
            stock.pkt_send_space().cwnd().get(),
            expected_cap,
            "the stock lane's window must be bounded by the minimum-RTT BDP ceiling"
        );

        // Below the engage factor (sRTT still at the floor) the ceiling must
        // not bind: the window is the raw rate-based estimate.
        let mut quiet = test_layer(t0);
        let mut tq = t0;
        for _ in 0..60 {
            tq += Duration::from_millis(1);
            quiet.sample_rtt(min_rtt, tq);
        }
        quiet.congestion_response.delivery_peak().update(tq, peak);
        quiet.set_send_rate(rate, tq);
        assert!(
            quiet.pkt_send_space().cwnd().get() > expected_cap,
            "a window still at its propagation floor must not be capped"
        );

        // The interactive reorder-tolerant lane is excluded: the ceiling must
        // not bind even with the same high-RTT, low-floor observation.
        let mut reorder = test_layer_reorder(t0);
        assert!(reorder.congestion_response.reorder_tolerant());
        let tr = drive(&mut reorder, t0, min_rtt, high_rtt);
        reorder.congestion_response.delivery_peak().update(tr, peak);
        reorder.set_send_rate(rate, tr);
        assert!(
            reorder.pkt_send_space().cwnd().get() > expected_cap,
            "the reorder-tolerant lane must be excluded from the bulk BDP ceiling"
        );
    }

    /// The BDP ceiling engages *at* the engage factor: the abstain branch is
    /// the strict `smooth_rtt < factor * min_rtt`, so a smoothed RTT exactly
    /// at `factor * min_rtt` is capped rather than treated as still at its
    /// propagation floor.  62.5 ms and 1.0625 s are dyadic, so the SRTT EWMA
    /// lands exactly on `3 * min_rtt = 187.5 ms` (`0.875 * 0.0625 + 0.125 *
    /// 1.0625`).
    #[test]
    fn bdp_cap_engages_at_exactly_the_engage_factor() {
        use crate::traffic_shaping::recovery::pkt_send_space::{
            CWND_BDP_CAP_ENGAGE_RTT_FACTOR, CWND_BDP_CAP_SCALE, INIT_CWND,
        };

        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let min_rtt = Duration::from_secs_f64(0.0625);
        let high_rtt = Duration::from_secs_f64(1.0625);
        rl.sample_rtt(min_rtt, t0);
        rl.sample_rtt(high_rtt, t0 + Duration::from_millis(1));
        assert_eq!(rl.pkt_send_space().min_rtt(), Some(min_rtt));
        assert_eq!(
            rl.pkt_send_space().smooth_rtt(),
            min_rtt.mul_f64(CWND_BDP_CAP_ENGAGE_RTT_FACTOR),
            "the sample must land exactly on the engage boundary"
        );
        let peak = 1000.0;
        rl.congestion_response
            .delivery_peak()
            .update(t0 + Duration::from_millis(2), peak);
        let bdp = min_rtt.as_secs_f64() * peak;
        let expected_cap = ((bdp * CWND_BDP_CAP_SCALE as f64).round() as usize).max(INIT_CWND);
        assert_eq!(
            rl.cwnd_bdp_cap(),
            Some(expected_cap),
            "a smoothed RTT exactly at the engage factor must engage the ceiling"
        );
    }

    #[test]
    fn congestion_metrics_track_persistent_queue_resets_on_signal_loss() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        rl.set_congestion_metrics_enabled(true);
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
            !rl.congestion_metrics_enabled(),
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
            !rl.congestion_metrics_enabled(),
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
        rl.set_congestion_metrics_enabled(true);
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
