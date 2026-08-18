use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::metrics::{
    MetricsEvent, MetricsInterest, MetricsObservation, MetricsObserver, MetricsSnapshot,
    SCHEMA_VERSION,
};
use crate::transmission::transmission_layer::{LogConfig, MetricsRow, ReliableLayerLogger};

/// Session observability: the metrics callback and the reliable-layer CSV
/// logger plus the monotonic event index.  Owned by the shared `Connection`
/// and mutated only through complete synchronous calls, so no guard survives
/// an await.
#[derive(Debug)]
pub(super) struct ConnectionObservability {
    clock_epoch: Instant,
    logger: Option<ReliableLayerLogger>,
    observer: Option<MetricsObserver>,
    next_event_index: AtomicU64,
}

impl ConnectionObservability {
    pub(super) fn new(
        clock_epoch: Instant,
        log_config: Option<LogConfig>,
        observer: Option<MetricsObserver>,
    ) -> Self {
        let logger = log_config.map(|config| {
            let file = std::fs::File::options()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&config.reliable_layer_log_path)
                .expect("open log file");
            std::sync::Mutex::new(csv::WriterBuilder::new().from_writer(file))
        });
        Self {
            clock_epoch,
            logger,
            observer,
            next_event_index: AtomicU64::new(0),
        }
    }

    /// Monotonic elapsed time from the connection epoch at `now`.
    pub(super) fn elapsed_since(&self, now: Instant) -> std::time::Duration {
        now.saturating_duration_since(self.clock_epoch)
    }

    /// Whether any observation sink exists (metrics callback or CSV logger).
    pub(super) fn enabled(&self) -> bool {
        self.observer.is_some() || self.logger.is_some()
    }

    pub(super) fn has_logger(&self) -> bool {
        self.logger.is_some()
    }

    /// Ask the observer what work one event needs; `elapsed` must be the
    /// caller's monotonic elapsed time from the connection epoch.
    pub(super) fn interest(
        &self,
        event: MetricsEvent,
        elapsed: std::time::Duration,
    ) -> MetricsInterest {
        self.observer
            .as_ref()
            .map(|observer| observer.interest(event, elapsed))
            .unwrap_or(MetricsInterest::Skip)
    }

    /// Allocate the next monotonic event index.
    pub(super) fn next_event_index(&self) -> u64 {
        self.next_event_index.fetch_add(1, Ordering::Relaxed)
    }

    /// Deliver one observation to the observer and the CSV logger.  Callbacks
    /// run after any transport locks are released.
    pub(super) fn publish(
        &self,
        event_index: u64,
        event: MetricsEvent,
        raw_rtt_sample: Option<std::time::Duration>,
        elapsed: std::time::Duration,
        snapshot: Option<MetricsSnapshot>,
        observer_interest: MetricsInterest,
    ) {
        let observation = MetricsObservation {
            schema_version: SCHEMA_VERSION,
            event_index,
            elapsed,
            event,
            raw_rtt_sample,
            snapshot,
        };
        if observer_interest != MetricsInterest::Skip
            && let Some(observer) = &self.observer
        {
            observer.observe(observation);
        }
        let Some(logger) = &self.logger else {
            return;
        };
        let snapshot = snapshot.expect("logger capture includes a snapshot");
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let log = MetricsRow {
            schema_version: SCHEMA_VERSION,
            event_index,
            op: event.as_str(),
            time: time.as_micros(),
            elapsed_micros: elapsed.as_micros(),
            raw_rtt_micros: raw_rtt_sample.map(|sample| sample.as_micros()),
            tokens: snapshot.pacer_tokens_packets,
            send_rate: snapshot.send_rate_packets_per_second,
            loss_rate: snapshot.loss_ratio,
            congestion_loss_rate: snapshot.congestion_loss_ratio,
            congestion_action: snapshot.congestion_action.map(|action| action.as_str()),
            num_in_flight_pkts: snapshot.in_flight_packets,
            num_pkts_in_pipe: snapshot.packets_in_pipe,
            num_rtx_active_pkts: snapshot.retransmission_active_packets,
            num_rtx_ready_pkts: snapshot.retransmission_ready_packets,
            num_rtx_pkts: snapshot.retransmitted_packets,
            send_seq: snapshot.next_send_sequence,
            min_rtt: snapshot.minimum_rtt.map(|rtt| rtt.as_millis()),
            rtt: snapshot.smoothed_rtt.as_millis(),
            retransmission_timeout_micros: snapshot.retransmission_timeout.as_micros(),
            oldest_pipe_packet_age_micros: snapshot
                .oldest_pipe_packet_age
                .map(|value| value.as_micros()),
            maximum_packet_rto_overdue_micros: snapshot
                .maximum_packet_rto_overdue
                .map(|value| value.as_micros()),
            rto_deadline_postponements: snapshot.rto_deadline_postponements,
            cwnd: snapshot.congestion_window_packets,
            num_rx_pkts: snapshot.received_packets,
            recv_seq: snapshot.next_receive_sequence,
            delivery_rate: snapshot.delivery_rate_packets_per_second,
            delivery_sample_app_limited: snapshot.delivery_sample_app_limited,
            application_write_waiters: snapshot.application_write_waiters,
            application_limited_detections: snapshot.application_limited_detections,
            application_limited_detections_suppressed_by_waiting_writer: snapshot
                .application_limited_detections_suppressed_by_waiting_writer,
            congestion_control_rtt_micros: snapshot
                .congestion_control_rtt
                .map(|value| value.as_micros()),
            congestion_rtt_floor_micros: snapshot
                .congestion_rtt_floor
                .map(|value| value.as_micros()),
            congestion_queue_tolerance_micros: snapshot
                .congestion_queue_tolerance
                .map(|value| value.as_micros()),
            congestion_persistent_queue_for_micros: snapshot
                .congestion_persistent_queue_for
                .map(|value| value.as_micros()),
            congestion_persistent_queue_resets: snapshot.congestion_persistent_queue_resets,
            congestion_delivery_peak_packets_per_second: snapshot
                .congestion_delivery_peak_packets_per_second,
            congestion_drain_floor_packets_per_second: snapshot
                .congestion_drain_floor_packets_per_second,
            congestion_drain_target_packets_per_second: snapshot
                .congestion_drain_target_packets_per_second,
            congestion_loss_backoff_floor_packets_per_second: snapshot
                .congestion_loss_backoff_floor_packets_per_second,
            congestion_loss_backoff_raw_target_packets_per_second: snapshot
                .congestion_loss_backoff_raw_target_packets_per_second,
            congestion_loss_backoff_target_packets_per_second: snapshot
                .congestion_loss_backoff_target_packets_per_second,
            congestion_loss_backoffs: snapshot.congestion_loss_backoffs,
            congestion_loss_backoff_floor_bindings: snapshot.congestion_loss_backoff_floor_bindings,
            congestion_rate_samples: snapshot.congestion_rate_samples,
            congestion_bandwidth_probe_decisions: snapshot.congestion_bandwidth_probe_decisions,
            congestion_bandwidth_probe_increases: snapshot.congestion_bandwidth_probe_increases,
            congestion_bandwidth_probe_before_feedback: snapshot
                .congestion_bandwidth_probe_before_feedback,
            congestion_last_bandwidth_probe_interval_micros: snapshot
                .congestion_last_bandwidth_probe_interval
                .map(|value| value.as_micros()),
            congestion_delay_drains: snapshot.congestion_delay_drains,
            pending_send_bytes: snapshot.pending_send_bytes,
            send_stage_capacity_bytes: snapshot.send_stage_capacity_bytes,
            accepts_new_packet: snapshot.accepts_new_packet,
            slow_start: snapshot.slow_start,
            gentle_mode: snapshot.gentle_mode,
            gentle_draining: snapshot.gentle_draining,
            queue_building: snapshot.queue_building,
            drain_floor_binding: snapshot.drain_floor_binding,
            outage_recovery: snapshot.outage_recovery,
            no_response_for_micros: snapshot.no_response_for.map(|value| value.as_micros()),
            no_progress_for_micros: snapshot.no_progress_for.map(|value| value.as_micros()),
            stall_reason: snapshot.stall_reason.map(|reason| reason.as_str()),
        };
        logger
            .lock()
            .unwrap()
            .serialize(&log)
            .expect("write CSV log");
    }
}
