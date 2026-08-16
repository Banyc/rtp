//! Opt-in observations of an RTP connection's transport state.
//!
//! Observers run synchronously on the thread that produced the event. They
//! should copy or aggregate the observation quickly and must not block. Calls
//! can overlap across the connection's read and write tasks; 'event_index'
//! supplies a connection-local total order for delivered observations.
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Version of the typed observation schema.
pub const SCHEMA_VERSION: u16 = 21;

/// Why the session reached its first terminal error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsTerminationCause {
    LocalAbort,
    UnreadPayloadAfterReadClose,
    PeerKill,
    UnreliableRead,
    DataWrite,
    AckWrite,
    FecParityWrite,
    HandshakeWrite,
    ProactiveStall,
}

impl MetricsTerminationCause {
    /// Stable snake-case label used by text and CSV exporters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalAbort => "local_abort",
            Self::UnreadPayloadAfterReadClose => "unread_payload_after_read_close",
            Self::PeerKill => "peer_kill",
            Self::UnreliableRead => "unreliable_read",
            Self::DataWrite => "data_write",
            Self::AckWrite => "ack_write",
            Self::FecParityWrite => "fec_parity_write",
            Self::HandshakeWrite => "handshake_write",
            Self::ProactiveStall => "proactive_stall",
        }
    }
}

/// The typed first terminal error recorded for a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsTermination {
    pub cause: MetricsTerminationCause,
    pub error_kind: std::io::ErrorKind,
    pub raw_os_error: Option<i32>,
}

impl MetricsTermination {
    /// Stable snake-case label used by text and CSV exporters.
    pub const fn error_kind_str(self) -> &'static str {
        match self.error_kind {
            std::io::ErrorKind::BrokenPipe => "broken_pipe",
            std::io::ErrorKind::ConnectionReset => "connection_reset",
            std::io::ErrorKind::ConnectionAborted => "connection_aborted",
            std::io::ErrorKind::NotConnected => "not_connected",
            std::io::ErrorKind::TimedOut => "timed_out",
            std::io::ErrorKind::UnexpectedEof => "unexpected_eof",
            std::io::ErrorKind::WouldBlock => "would_block",
            _ => "other",
        }
    }
}

/// Most recent congestion-controller branch evaluated for the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsCongestionAction {
    OutageReset,
    CensoredOutageSample,
    BandwidthProbe,
    SlowStartAck,
    GentleProbe,
    DelayDrain,
    HugeLossBackoff,
    LossBackoff,
}
/// Why one delay-gated gentle-mode episode ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsGentleExitCause {
    Loss,
    GateOpen,
    DrainGuard,
    OutageReset,
}

impl MetricsGentleExitCause {
    pub const ALL: [Self; 4] = [
        Self::Loss,
        Self::GateOpen,
        Self::DrainGuard,
        Self::OutageReset,
    ];

    /// Stable suffix used by aggregate export labels.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Loss => "loss",
            Self::GateOpen => "gate_open",
            Self::DrainGuard => "drain_guard",
            Self::OutageReset => "outage_reset",
        }
    }

    const fn event_str(self) -> &'static str {
        match self {
            Self::Loss => "gentle_mode_exit_loss",
            Self::GateOpen => "gentle_mode_exit_gate_open",
            Self::DrainGuard => "gentle_mode_exit_drain_guard",
            Self::OutageReset => "gentle_mode_exit_outage_reset",
        }
    }
}

impl MetricsCongestionAction {
    /// Stable snake-case label used by text and CSV exporters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OutageReset => "outage_reset",
            Self::CensoredOutageSample => "censored_outage_sample",
            Self::SlowStartAck => "slow_start_ack",
            Self::BandwidthProbe => "bandwidth_probe",
            Self::GentleProbe => "gentle_probe",
            Self::DelayDrain => "delay_drain",
            Self::HugeLossBackoff => "huge_loss_backoff",
            Self::LossBackoff => "loss_backoff",
        }
    }
}

/// Amount of work requested for one metrics event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsInterest {
    /// Do not notify the callback.
    Skip,
    /// Notify with event metadata only, avoiding a transport-state scan.
    EventOnly,
    /// Capture and attach the full transport-state snapshot.
    Snapshot,
}

/// Why the proactive peer-liveness watchdog currently considers the session
/// stalled. This is distinct from congestion-controller outage recovery:
/// recovery may be active well before the watchdog's termination deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsStallReason {
    NoResponse,
    NoProgress,
}
/// What resumed the send driver after a completed send pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsSendDriverWake {
    ResumeSignal,
    PacingTimer,
    ProtocolTimer,
    KillRequested,
}

impl MetricsSendDriverWake {
    /// Stable snake-case label used by text and CSV exporters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResumeSignal => "send_driver_resume_signal",
            Self::PacingTimer => "send_driver_pacing_timer",
            Self::ProtocolTimer => "send_driver_protocol_timer",
            Self::KillRequested => "send_driver_kill_requested",
        }
    }
}

impl MetricsStallReason {
    /// Stable snake-case label used by text and CSV exporters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoResponse => "no_response",
            Self::NoProgress => "no_progress",
        }
    }
}

/// The operation after which a transport-state snapshot was captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsEvent {
    SendDataBuffer,
    SendFrameBuffer,
    ReceiveDataBuffer,
    ReceiveFrameBuffer,
    ReceiveAckPacket,
    ReceiveDataPacket,
    /// A send-loop packet attempt, including attempts that find no sendable
    /// packet because pacing, congestion control, or the queue blocks them.
    SendDataPacketAttempt,
    /// The event that resumed the send driver after its previous send pass.
    SendDriverWake(MetricsSendDriverWake),
    /// A request to resume the send driver, before Notify coalescing.
    SendDriverResumeRequest(MetricsSendDriverResumeSource),
    /// An exact transition that ended a delay-gated gentle-mode episode.
    GentleModeExit(MetricsGentleExitCause),
    /// A raw timestamp-echo RTT sample was accepted by the estimator.
    RttSample,
    /// The first terminal error that owns the session failure.
    SessionTermination(MetricsTermination),
}
/// Producer that requested a resume-signal wake for the RTP send driver.
///
/// Requests and consumed wakes are deliberately separate observations:
/// [`tokio::sync::Notify`] may coalesce several requests into one
/// [`MetricsSendDriverWake::ResumeSignal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsSendDriverResumeSource {
    ApplicationData,
    ApplicationFrame,
    ApplicationFinish,
    PeerAck,
    AckFlush,
    PostOpenHandshake,
    ReceiveOpportunity,
}

impl MetricsSendDriverResumeSource {
    /// Stable snake-case label used by text and CSV exporters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApplicationData => "send_driver_resume_request_application_data",
            Self::ApplicationFrame => "send_driver_resume_request_application_frame",
            Self::ApplicationFinish => "send_driver_resume_request_application_finish",
            Self::PeerAck => "send_driver_resume_request_peer_ack",
            Self::AckFlush => "send_driver_resume_request_ack_flush",
            Self::PostOpenHandshake => "send_driver_resume_request_post_open_handshake",
            Self::ReceiveOpportunity => "send_driver_resume_request_receive_opportunity",
        }
    }
}

/// Cumulative repair activity for one connection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsRetransmissionCounters {
    /// Calls that selected and emitted a retransmission-ready packet.
    pub attempts: u64,
    /// Retransmission attempts for packets that had not previously been repaired.
    pub first_attempts: u64,
    /// Retransmission attempts for packets already repaired at least once.
    pub repeat_attempts: u64,
    /// Attempts where an RTO reason was armed.
    pub rto_reason: u64,
    /// Attempts where the time-based reordering deadline was armed.
    pub reorder_reason: u64,
    /// Attempts where SACK-count fast-loss evidence was armed.
    pub fast_loss_reason: u64,
    /// Attempts where outage recovery marked the packet pre-outage.
    pub pre_outage_reason: u64,
    /// Tail-loss probes emitted outside the retransmission-ready path.
    pub tail_probes: u64,
}

impl MetricsEvent {
    /// Stable snake-case label used by text and CSV exporters.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SendDataBuffer => "send_data_buf",
            Self::SendFrameBuffer => "send_frame_buf",
            Self::ReceiveDataBuffer => "recv_data_buf",
            Self::ReceiveFrameBuffer => "recv_frame_buf",
            Self::ReceiveAckPacket => "recv_ack_pkt",
            Self::ReceiveDataPacket => "recv_data_pkt",
            Self::SendDataPacketAttempt => "send_data_pkt",
            Self::SendDriverWake(wake) => wake.as_str(),
            Self::SendDriverResumeRequest(source) => source.as_str(),
            Self::GentleModeExit(cause) => cause.event_str(),
            Self::RttSample => "rtt_sample",
            Self::SessionTermination(_) => "session_termination",
        }
    }
}

/// A point-in-time snapshot of the reliable transport.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricsSnapshot {
    /// Currently available pacer tokens, in packets.
    pub pacer_tokens_packets: f64,
    /// Configured sender pacing rate, in packets per second.
    pub send_rate_packets_per_second: f64,
    /// Estimated recent packet-loss fraction in [0, 1].
    pub loss_ratio: Option<f64>,
    /// Loss-event ratio most recently evaluated by congestion control.
    pub congestion_loss_ratio: Option<f64>,
    /// Most recent congestion-control branch evaluated for this connection.
    pub congestion_action: Option<MetricsCongestionAction>,
    pub in_flight_packets: usize,
    pub packets_in_pipe: usize,
    /// Packets currently tracked by the retransmission scheduler.
    pub retransmission_active_packets: usize,
    /// Packets currently retransmission-ready (due or evidence-armed).
    pub retransmission_ready_packets: usize,
    pub retransmitted_packets: usize,
    /// Cumulative repair activity.  Scheduler-reason counters are
    /// non-exclusive because one attempt may have multiple reasons armed.
    pub retransmission_counters: MetricsRetransmissionCounters,
    pub next_send_sequence: u64,
    pub minimum_rtt: Option<Duration>,
    pub smoothed_rtt: Duration,
    /// Current retransmission timeout estimate.
    pub retransmission_timeout: Duration,
    /// Age of the oldest packet still in the pipe.
    pub oldest_pipe_packet_age: Option<Duration>,
    /// How far past its RTO deadline the most overdue pipe packet is.
    pub maximum_packet_rto_overdue: Option<Duration>,
    /// RTO deadlines postponed by the lazy live-estimator floor.
    pub rto_deadline_postponements: u64,
    pub congestion_window_packets: usize,
    pub received_packets: usize,
    pub next_receive_sequence: Option<u64>,
    /// Most recent delivery-rate estimate, in packets per second.
    pub delivery_rate_packets_per_second: Option<f64>,
    /// Whether the packet behind the most recent delivery-rate sample was
    /// transmitted while DRE considered the connection application-limited.
    pub delivery_sample_app_limited: Option<bool>,
    /// Application writers currently blocked waiting for send capacity.
    pub application_write_waiters: usize,
    /// Application-limited phases detected by DRE's conjunctive predicate.
    pub application_limited_detections: u64,
    /// Detections suppressed because an application writer was waiting.
    pub application_limited_detections_suppressed_by_waiting_writer: u64,
    /// Control RTT last used by the congestion controller.
    pub congestion_control_rtt: Option<Duration>,
    /// RTT floor last used by the congestion controller's queue gate.
    pub congestion_rtt_floor: Option<Duration>,
    /// Queue-gate tolerance last computed by the congestion controller.
    pub congestion_queue_tolerance: Option<Duration>,
    /// Continuous time for which the wider persistent-queue signal has been
    /// armed.  `None` means the signal is currently clear.
    pub congestion_persistent_queue_for: Option<Duration>,
    /// Observed armed-to-clear transitions of the persistent-queue signal.
    /// Congestion-epoch resets clear the private continuity latch and therefore
    /// do not increment this cumulative diagnostic counter.
    pub congestion_persistent_queue_resets: u64,
    /// Recent delivery peak feeding the drain floor.
    pub congestion_delivery_peak_packets_per_second: Option<f64>,
    /// Drain floor last applied by the congestion controller.
    pub congestion_drain_floor_packets_per_second: Option<f64>,
    /// Drain target last applied by the congestion controller.
    pub congestion_drain_target_packets_per_second: Option<f64>,
    /// Delivery-rate samples evaluated by the congestion controller.
    pub congestion_rate_samples: u64,
    /// Bandwidth-probe decisions taken by the congestion controller.
    pub congestion_bandwidth_probe_decisions: u64,
    /// Probe decisions that increased the send rate.
    pub congestion_bandwidth_probe_increases: u64,
    /// Increases applied before the previous one got feedback.
    pub congestion_bandwidth_probe_before_feedback: u64,
    /// Interval between the last two applied probe increases.
    pub congestion_last_bandwidth_probe_interval: Option<Duration>,
    /// Delay-drain decisions taken by the congestion controller.
    pub congestion_delay_drains: u64,
    /// Application bytes staged inside RTP but not yet packetized.
    pub pending_send_bytes: usize,
    /// Maximum application bytes the current send stage can retain.
    pub send_stage_capacity_bytes: usize,
    /// Whether the congestion window currently permits a new data packet.
    pub accepts_new_packet: bool,
    /// The controller is still in its initial exponential-growth phase.
    pub slow_start: bool,
    /// The delay controller is using its conservative high-queue mode.
    pub gentle_mode: bool,
    /// Gentle mode is actively reducing its send-rate target.
    pub gentle_draining: bool,
    /// Smoothed RTT is currently above the controller's queue gate.
    pub queue_building: bool,
    /// The stale-peak protection floor is currently limiting a drain.
    pub drain_floor_binding: bool,
    /// A loss/no-progress outage epoch is open and constraining recovery.
    pub outage_recovery: bool,
    /// Time since the peer last provided any response while a wait is active.
    pub no_response_for: Option<Duration>,
    /// Time since cumulative send progress while a wait is active.
    pub no_progress_for: Option<Duration>,
    /// Present only once the proactive termination deadline has expired.
    pub stall_reason: Option<MetricsStallReason>,
}

/// One connection observation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricsObservation {
    pub schema_version: u16,
    /// Monotonic index assigned while the connection state lock is held.
    pub event_index: u64,
    /// Monotonic time since this connection was constructed.
    pub elapsed: Duration,
    pub event: MetricsEvent,
    /// Present only for [`MetricsEvent::RttSample`]; this is the raw sample,
    /// while 'snapshot.smoothed_rtt' is the estimator output after sampling.
    pub raw_rtt_sample: Option<Duration>,
    /// Full transport state when requested by the observer filter.
    pub snapshot: Option<MetricsSnapshot>,
}

/// A cheap-to-clone synchronous callback for typed connection observations.
///
/// No callback or observer-side synchronization is performed when this value
/// is absent from the connection config. If a callback forwards observations
/// across an asynchronous boundary, it should use bounded delivery.
#[derive(Clone)]
pub struct MetricsObserver {
    filter: Arc<dyn Fn(MetricsEvent, Duration) -> MetricsInterest + Send + Sync + 'static>,
    callback: Arc<dyn Fn(MetricsObservation) + Send + Sync + 'static>,
}

impl MetricsObserver {
    /// Observe every metrics event.
    pub fn new(callback: impl Fn(MetricsObservation) + Send + Sync + 'static) -> Self {
        Self {
            filter: Arc::new(|_, _| MetricsInterest::Snapshot),
            callback: Arc::new(callback),
        }
    }

    /// Construct an observer that decides whether a state snapshot is needed.
    ///
    /// 'filter' receives the event and monotonic elapsed time before RTP locks
    /// or scans reliable state. It can therefore downsample hot events without
    /// paying snapshot cost. It may be called concurrently and should be
    /// constant-time and non-blocking.
    pub fn filtered(
        filter: impl Fn(MetricsEvent, Duration) -> bool + Send + Sync + 'static,
        callback: impl Fn(MetricsObservation) + Send + Sync + 'static,
    ) -> Self {
        Self {
            filter: Arc::new(move |event, elapsed| {
                if filter(event, elapsed) {
                    MetricsInterest::Snapshot
                } else {
                    MetricsInterest::Skip
                }
            }),
            callback: Arc::new(callback),
        }
    }

    /// Construct an observer that independently chooses event-only or full
    /// snapshot capture. Event-only capture is appropriate for high-rate raw
    /// RTT samples: it preserves the sample without scanning send/receive
    /// windows merely to attach unrelated state.
    pub fn selective(
        filter: impl Fn(MetricsEvent, Duration) -> MetricsInterest + Send + Sync + 'static,
        callback: impl Fn(MetricsObservation) + Send + Sync + 'static,
    ) -> Self {
        Self {
            filter: Arc::new(filter),
            callback: Arc::new(callback),
        }
    }

    pub(crate) fn interest(&self, event: MetricsEvent, elapsed: Duration) -> MetricsInterest {
        (self.filter)(event, elapsed)
    }

    pub(crate) fn observe(&self, observation: MetricsObservation) {
        (self.callback)(observation);
    }
}

impl fmt::Debug for MetricsObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsObserver").finish_non_exhaustive()
    }
}
