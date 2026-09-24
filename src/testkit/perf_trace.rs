use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::metrics::{
    MetricsAckFlushReason, MetricsEvent, MetricsFecCounters, MetricsFecGroupSizeBuckets,
    MetricsGentleExitCause, MetricsInterest, MetricsObservation, MetricsObserver,
    MetricsRetransmissionCounters, MetricsSendDriverResumeSource, MetricsSendDriverWake,
    MetricsSnapshot, MetricsTermination,
};
use netem_test::{Counters, CountersSnapshot};

/// Trace schema 32: RTP rows carry the complete congestion-controller and
/// retransmission-scheduler snapshot plus the typed FEC work/recovery columns
/// and `trace_elapsed_us`, so endpoint, netem, and progress samples share one
/// clock (`PerfTrace::trace_start`). Event-only rows leave every snapshot
/// column empty; the retransmission-active/ready, RTO timing,
/// controller-decision, and FEC evidence is present only on snapshot rows.
/// [`RTP_TRACE_COLUMNS`] is the single authority for the column set.
const TRACE_SCHEMA_VERSION: u16 = 32;
const DEFAULT_CAPACITY: usize = 100_000;
const STATE_SAMPLE_INTERVAL: Duration = Duration::from_millis(50);
/// Raw RTT samples are time-decimated to one retained row per interval on
/// their own clock, finer than [`STATE_SAMPLE_INTERVAL`] so the retained RTT
/// series stays dense enough for p90/p99 tail evidence. Retention is then a
/// fixed rows-per-second constant per endpoint regardless of lane speed, so a
/// fast lane cannot exhaust the bounded storage and long runs degrade
/// evidence resolution uniformly rather than losing their tail.
const RTT_SAMPLE_INTERVAL: Duration = Duration::from_millis(10);
/// One RTP trace column: `(header_name, value_accessor)`, in wire order.
///
/// `header_name` is the CSV header spelling a consumer reads the column by and
/// `value_accessor` renders that same column for one observation. The header
/// and every row are both derived from [`RTP_TRACE_COLUMNS`], so a name can
/// never be emitted against another column's value: adding, removing, or
/// reordering a column is a single edit here, and both sides move together.
type RtpTraceColumn = (&'static str, fn(&MetricsObservation, Duration) -> String);

/// The single authority for the RTP trace schema: one entry per column, in
/// wire order, pairing the header name with the accessor that produces its
/// value. [`rtp_trace_header`] and [`rtp_fields`] are both rendered from this
/// table, so they cannot disagree.
const RTP_TRACE_COLUMNS: &[RtpTraceColumn] = &[
    ("schema_version", |o, _| o.schema_version.to_string()),
    ("event_index", |o, _| o.event_index.to_string()),
    ("elapsed_us", |o, _| o.elapsed.as_micros().to_string()),
    ("event", |o, _| o.event.as_str().to_owned()),
    ("termination_cause", |o, _| {
        termination(o)
            .map(|termination| termination.cause.as_str().to_owned())
            .unwrap_or_default()
    }),
    ("termination_error_kind", |o, _| {
        termination(o)
            .map(|termination| termination.error_kind_str().to_owned())
            .unwrap_or_default()
    }),
    ("termination_raw_os_error", |o, _| {
        termination(o)
            .and_then(|termination| termination.raw_os_error)
            .map(|error| error.to_string())
            .unwrap_or_default()
    }),
    ("raw_rtt_us", |o, _| {
        optional_u128(o.raw_rtt_sample.map(|value| value.as_micros()))
    }),
    ("pacer_tokens_packets", |o, _| {
        snapshot_value(o, |s| s.pacer_tokens_packets.to_string())
    }),
    ("send_rate_packets_per_second", |o, _| {
        snapshot_value(o, |s| s.send_rate_packets_per_second.to_string())
    }),
    ("loss_ratio", |o, _| {
        snapshot_value(o, |s| optional_f64(s.loss_ratio))
    }),
    ("in_flight_packets", |o, _| {
        snapshot_value(o, |s| s.in_flight_packets.to_string())
    }),
    ("packets_in_pipe", |o, _| {
        snapshot_value(o, |s| s.packets_in_pipe.to_string())
    }),
    ("retransmission_active_packets", |o, _| {
        snapshot_value(o, |s| s.retransmission_active_packets.to_string())
    }),
    ("retransmission_ready_packets", |o, _| {
        snapshot_value(o, |s| s.retransmission_ready_packets.to_string())
    }),
    ("retransmitted_packets", |o, _| {
        snapshot_value(o, |s| s.retransmitted_packets.to_string())
    }),
    ("retransmission_attempts", |o, _| {
        snapshot_value(o, |s| s.retransmission_counters.attempts.to_string())
    }),
    ("retransmission_first_attempts", |o, _| {
        snapshot_value(o, |s| s.retransmission_counters.first_attempts.to_string())
    }),
    ("retransmission_repeat_attempts", |o, _| {
        snapshot_value(o, |s| s.retransmission_counters.repeat_attempts.to_string())
    }),
    ("retransmission_rto_reason", |o, _| {
        snapshot_value(o, |s| s.retransmission_counters.rto_reason.to_string())
    }),
    ("retransmission_reorder_reason", |o, _| {
        snapshot_value(o, |s| s.retransmission_counters.reorder_reason.to_string())
    }),
    ("retransmission_fast_loss_reason", |o, _| {
        snapshot_value(o, |s| {
            s.retransmission_counters.fast_loss_reason.to_string()
        })
    }),
    ("retransmission_pre_outage_reason", |o, _| {
        snapshot_value(o, |s| {
            s.retransmission_counters.pre_outage_reason.to_string()
        })
    }),
    ("tail_probe_attempts", |o, _| {
        snapshot_value(o, |s| s.retransmission_counters.tail_probes.to_string())
    }),
    ("fec_parity_sent", |o, _| {
        fec_value(o, |f| f.parity_sent.to_string())
    }),
    ("fec_groups_flushed", |o, _| {
        fec_value(o, |f| f.groups_flushed.to_string())
    }),
    ("fec_flushed_groups_1", |o, _| {
        fec_value(o, |f| f.flushed_group_sizes.one.to_string())
    }),
    ("fec_flushed_groups_2_to_4", |o, _| {
        fec_value(o, |f| f.flushed_group_sizes.two_to_four.to_string())
    }),
    ("fec_flushed_groups_5_to_7", |o, _| {
        fec_value(o, |f| f.flushed_group_sizes.five_to_seven.to_string())
    }),
    ("fec_flushed_groups_8", |o, _| {
        fec_value(o, |f| f.flushed_group_sizes.full_eight.to_string())
    }),
    ("fec_groups_skipped_no_surplus_tokens", |o, _| {
        fec_value(o, |f| f.groups_skipped_no_surplus_tokens.to_string())
    }),
    ("fec_no_surplus_groups_1", |o, _| {
        fec_value(o, |f| f.no_surplus_group_sizes.one.to_string())
    }),
    ("fec_no_surplus_groups_2_to_4", |o, _| {
        fec_value(o, |f| f.no_surplus_group_sizes.two_to_four.to_string())
    }),
    ("fec_no_surplus_groups_5_to_7", |o, _| {
        fec_value(o, |f| f.no_surplus_group_sizes.five_to_seven.to_string())
    }),
    ("fec_no_surplus_groups_8", |o, _| {
        fec_value(o, |f| f.no_surplus_group_sizes.full_eight.to_string())
    }),
    ("fec_groups_skipped_burst_end", |o, _| {
        fec_value(o, |f| f.groups_skipped_burst_end.to_string())
    }),
    ("fec_burst_end_groups_1", |o, _| {
        fec_value(o, |f| f.burst_end_group_sizes.one.to_string())
    }),
    ("fec_burst_end_groups_2_to_4", |o, _| {
        fec_value(o, |f| f.burst_end_group_sizes.two_to_four.to_string())
    }),
    ("fec_burst_end_groups_5_to_7", |o, _| {
        fec_value(o, |f| f.burst_end_group_sizes.five_to_seven.to_string())
    }),
    ("fec_burst_end_groups_8", |o, _| {
        fec_value(o, |f| f.burst_end_group_sizes.full_eight.to_string())
    }),
    ("fec_groups_skipped_loss_gate", |o, _| {
        fec_value(o, |f| f.groups_skipped_loss_gate.to_string())
    }),
    ("fec_loss_gate_groups_1", |o, _| {
        fec_value(o, |f| f.loss_gate_group_sizes.one.to_string())
    }),
    ("fec_loss_gate_groups_2_to_4", |o, _| {
        fec_value(o, |f| f.loss_gate_group_sizes.two_to_four.to_string())
    }),
    ("fec_loss_gate_groups_5_to_7", |o, _| {
        fec_value(o, |f| f.loss_gate_group_sizes.five_to_seven.to_string())
    }),
    ("fec_loss_gate_groups_8", |o, _| {
        fec_value(o, |f| f.loss_gate_group_sizes.full_eight.to_string())
    }),
    ("fec_groups_skipped_no_spare_capacity", |o, _| {
        fec_value(o, |f| f.groups_skipped_no_spare_capacity.to_string())
    }),
    ("fec_no_spare_capacity_groups_1", |o, _| {
        fec_value(o, |f| f.no_spare_capacity_group_sizes.one.to_string())
    }),
    ("fec_no_spare_capacity_groups_2_to_4", |o, _| {
        fec_value(o, |f| {
            f.no_spare_capacity_group_sizes.two_to_four.to_string()
        })
    }),
    ("fec_no_spare_capacity_groups_5_to_7", |o, _| {
        fec_value(o, |f| {
            f.no_spare_capacity_group_sizes.five_to_seven.to_string()
        })
    }),
    ("fec_no_spare_capacity_groups_8", |o, _| {
        fec_value(o, |f| {
            f.no_spare_capacity_group_sizes.full_eight.to_string()
        })
    }),
    ("fec_recovered_symbols", |o, _| {
        fec_value(o, |f| f.recovered_symbols.to_string())
    }),
    ("fec_dropped_malformed_packets", |o, _| {
        fec_value(o, |f| f.dropped_malformed_packets.to_string())
    }),
    ("fec_dropped_decoder_panics", |o, _| {
        fec_value(o, |f| f.dropped_decoder_panics.to_string())
    }),
    ("fec_rejected_recovered_symbols", |o, _| {
        fec_value(o, |f| f.rejected_recovered_symbols.to_string())
    }),
    ("next_send_sequence", |o, _| {
        snapshot_value(o, |s| s.next_send_sequence.to_string())
    }),
    ("minimum_rtt_us", |o, _| {
        snapshot_value(o, |s| {
            optional_u128(s.minimum_rtt.map(|value| value.as_micros()))
        })
    }),
    ("smoothed_rtt_us", |o, _| {
        snapshot_value(o, |s| s.smoothed_rtt.as_micros().to_string())
    }),
    ("retransmission_timeout_us", |o, _| {
        snapshot_value(o, |s| s.retransmission_timeout.as_micros().to_string())
    }),
    ("oldest_pipe_packet_age_us", |o, _| {
        snapshot_value(o, |s| {
            optional_u128(s.oldest_pipe_packet_age.map(|value| value.as_micros()))
        })
    }),
    ("maximum_packet_rto_overdue_us", |o, _| {
        snapshot_value(o, |s| {
            optional_u128(s.maximum_packet_rto_overdue.map(|value| value.as_micros()))
        })
    }),
    ("rto_deadline_postponements", |o, _| {
        snapshot_value(o, |s| s.rto_deadline_postponements.to_string())
    }),
    ("congestion_window_packets", |o, _| {
        snapshot_value(o, |s| s.congestion_window_packets.to_string())
    }),
    ("received_packets", |o, _| {
        snapshot_value(o, |s| s.received_packets.to_string())
    }),
    ("next_receive_sequence", |o, _| {
        snapshot_value(o, |s| optional_u64(s.next_receive_sequence))
    }),
    ("delivery_rate_packets_per_second", |o, _| {
        snapshot_value(o, |s| optional_f64(s.delivery_rate_packets_per_second))
    }),
    ("delivery_sample_app_limited", |o, _| {
        snapshot_value(o, |s| optional_bool(s.delivery_sample_app_limited))
    }),
    ("application_write_waiters", |o, _| {
        snapshot_value(o, |s| s.application_write_waiters.to_string())
    }),
    ("application_limited_detections", |o, _| {
        snapshot_value(o, |s| s.application_limited_detections.to_string())
    }),
    (
        "application_limited_detections_suppressed_by_waiting_writer",
        |o, _| {
            snapshot_value(o, |s| {
                s.application_limited_detections_suppressed_by_waiting_writer
                    .to_string()
            })
        },
    ),
    ("congestion_control_rtt_us", |o, _| {
        snapshot_value(o, |s| {
            optional_u128(s.congestion_control_rtt.map(|value| value.as_micros()))
        })
    }),
    ("congestion_rtt_floor_us", |o, _| {
        snapshot_value(o, |s| {
            optional_u128(s.congestion_rtt_floor.map(|value| value.as_micros()))
        })
    }),
    ("congestion_queue_tolerance_us", |o, _| {
        snapshot_value(o, |s| {
            optional_u128(s.congestion_queue_tolerance.map(|value| value.as_micros()))
        })
    }),
    ("congestion_persistent_queue_for_us", |o, _| {
        snapshot_value(o, |s| {
            optional_u128(
                s.congestion_persistent_queue_for
                    .map(|value| value.as_micros()),
            )
        })
    }),
    ("congestion_persistent_queue_resets", |o, _| {
        snapshot_value(o, |s| s.congestion_persistent_queue_resets.to_string())
    }),
    ("congestion_delivery_peak_packets_per_second", |o, _| {
        snapshot_value(o, |s| {
            optional_f64(s.congestion_delivery_peak_packets_per_second)
        })
    }),
    ("congestion_drain_floor_packets_per_second", |o, _| {
        snapshot_value(o, |s| {
            optional_f64(s.congestion_drain_floor_packets_per_second)
        })
    }),
    ("congestion_drain_target_packets_per_second", |o, _| {
        snapshot_value(o, |s| {
            optional_f64(s.congestion_drain_target_packets_per_second)
        })
    }),
    (
        "congestion_loss_backoff_floor_packets_per_second",
        |o, _| {
            snapshot_value(o, |s| {
                optional_f64(s.congestion_loss_backoff_floor_packets_per_second)
            })
        },
    ),
    (
        "congestion_loss_backoff_raw_target_packets_per_second",
        |o, _| {
            snapshot_value(o, |s| {
                optional_f64(s.congestion_loss_backoff_raw_target_packets_per_second)
            })
        },
    ),
    (
        "congestion_loss_backoff_target_packets_per_second",
        |o, _| {
            snapshot_value(o, |s| {
                optional_f64(s.congestion_loss_backoff_target_packets_per_second)
            })
        },
    ),
    ("congestion_loss_backoffs", |o, _| {
        snapshot_value(o, |s| s.congestion_loss_backoffs.to_string())
    }),
    ("congestion_loss_backoff_floor_bindings", |o, _| {
        snapshot_value(o, |s| s.congestion_loss_backoff_floor_bindings.to_string())
    }),
    ("congestion_rate_samples", |o, _| {
        snapshot_value(o, |s| s.congestion_rate_samples.to_string())
    }),
    ("congestion_bandwidth_probe_decisions", |o, _| {
        snapshot_value(o, |s| s.congestion_bandwidth_probe_decisions.to_string())
    }),
    ("congestion_bandwidth_probe_increases", |o, _| {
        snapshot_value(o, |s| s.congestion_bandwidth_probe_increases.to_string())
    }),
    ("congestion_bandwidth_probe_before_feedback", |o, _| {
        snapshot_value(o, |s| {
            s.congestion_bandwidth_probe_before_feedback.to_string()
        })
    }),
    ("congestion_last_bandwidth_probe_interval_us", |o, _| {
        snapshot_value(o, |s| {
            optional_u128(
                s.congestion_last_bandwidth_probe_interval
                    .map(|value| value.as_micros()),
            )
        })
    }),
    ("congestion_delay_drains", |o, _| {
        snapshot_value(o, |s| s.congestion_delay_drains.to_string())
    }),
    ("pending_send_bytes", |o, _| {
        snapshot_value(o, |s| s.pending_send_bytes.to_string())
    }),
    ("send_stage_capacity_bytes", |o, _| {
        snapshot_value(o, |s| s.send_stage_capacity_bytes.to_string())
    }),
    ("accepts_new_packet", |o, _| {
        snapshot_value(o, |s| s.accepts_new_packet.to_string())
    }),
    ("slow_start", |o, _| {
        snapshot_value(o, |s| s.slow_start.to_string())
    }),
    ("gentle_mode", |o, _| {
        snapshot_value(o, |s| s.gentle_mode.to_string())
    }),
    ("gentle_draining", |o, _| {
        snapshot_value(o, |s| s.gentle_draining.to_string())
    }),
    ("queue_building", |o, _| {
        snapshot_value(o, |s| s.queue_building.to_string())
    }),
    ("drain_floor_binding", |o, _| {
        snapshot_value(o, |s| s.drain_floor_binding.to_string())
    }),
    ("outage_recovery", |o, _| {
        snapshot_value(o, |s| s.outage_recovery.to_string())
    }),
    ("no_response_for_us", |o, _| {
        snapshot_value(o, |s| {
            optional_u128(s.no_response_for.map(|value| value.as_micros()))
        })
    }),
    ("no_progress_for_us", |o, _| {
        snapshot_value(o, |s| {
            optional_u128(s.no_progress_for.map(|value| value.as_micros()))
        })
    }),
    ("stall_reason", |o, _| {
        snapshot_value(o, |s| {
            s.stall_reason
                .map(|reason| reason.as_str())
                .unwrap_or_default()
                .to_owned()
        })
    }),
    ("congestion_loss_ratio", |o, _| {
        snapshot_value(o, |s| optional_f64(s.congestion_loss_ratio))
    }),
    ("congestion_action", |o, _| {
        snapshot_value(o, |s| {
            s.congestion_action
                .map(|action| action.as_str())
                .unwrap_or_default()
                .to_owned()
        })
    }),
    ("trace_elapsed_us", |_, trace_elapsed| {
        trace_elapsed.as_micros().to_string()
    }),
];
/// The RTP trace header as emitted, frozen. The production schema is
/// [`RTP_TRACE_COLUMNS`]; this literal is a test fixture proving that the
/// rendered header still matches it byte for byte, so a rename, reorder,
/// insertion, or removal is loud instead of silently relabelling a metric.
#[cfg(test)]
const FROZEN_RTP_TRACE_HEADER: &str = "schema_version,event_index,elapsed_us,event,termination_cause,termination_error_kind,termination_raw_os_error,raw_rtt_us,pacer_tokens_packets,send_rate_packets_per_second,loss_ratio,in_flight_packets,packets_in_pipe,retransmission_active_packets,retransmission_ready_packets,retransmitted_packets,retransmission_attempts,retransmission_first_attempts,retransmission_repeat_attempts,retransmission_rto_reason,retransmission_reorder_reason,retransmission_fast_loss_reason,retransmission_pre_outage_reason,tail_probe_attempts,fec_parity_sent,fec_groups_flushed,fec_flushed_groups_1,fec_flushed_groups_2_to_4,fec_flushed_groups_5_to_7,fec_flushed_groups_8,fec_groups_skipped_no_surplus_tokens,fec_no_surplus_groups_1,fec_no_surplus_groups_2_to_4,fec_no_surplus_groups_5_to_7,fec_no_surplus_groups_8,fec_groups_skipped_burst_end,fec_burst_end_groups_1,fec_burst_end_groups_2_to_4,fec_burst_end_groups_5_to_7,fec_burst_end_groups_8,fec_groups_skipped_loss_gate,fec_loss_gate_groups_1,fec_loss_gate_groups_2_to_4,fec_loss_gate_groups_5_to_7,fec_loss_gate_groups_8,fec_groups_skipped_no_spare_capacity,fec_no_spare_capacity_groups_1,fec_no_spare_capacity_groups_2_to_4,fec_no_spare_capacity_groups_5_to_7,fec_no_spare_capacity_groups_8,fec_recovered_symbols,fec_dropped_malformed_packets,fec_dropped_decoder_panics,fec_rejected_recovered_symbols,next_send_sequence,minimum_rtt_us,smoothed_rtt_us,retransmission_timeout_us,oldest_pipe_packet_age_us,maximum_packet_rto_overdue_us,rto_deadline_postponements,congestion_window_packets,received_packets,next_receive_sequence,delivery_rate_packets_per_second,delivery_sample_app_limited,application_write_waiters,application_limited_detections,application_limited_detections_suppressed_by_waiting_writer,congestion_control_rtt_us,congestion_rtt_floor_us,congestion_queue_tolerance_us,congestion_persistent_queue_for_us,congestion_persistent_queue_resets,congestion_delivery_peak_packets_per_second,congestion_drain_floor_packets_per_second,congestion_drain_target_packets_per_second,congestion_loss_backoff_floor_packets_per_second,congestion_loss_backoff_raw_target_packets_per_second,congestion_loss_backoff_target_packets_per_second,congestion_loss_backoffs,congestion_loss_backoff_floor_bindings,congestion_rate_samples,congestion_bandwidth_probe_decisions,congestion_bandwidth_probe_increases,congestion_bandwidth_probe_before_feedback,congestion_last_bandwidth_probe_interval_us,congestion_delay_drains,pending_send_bytes,send_stage_capacity_bytes,accepts_new_packet,slow_start,gentle_mode,gentle_draining,queue_building,drain_floor_binding,outage_recovery,no_response_for_us,no_progress_for_us,stall_reason,congestion_loss_ratio,congestion_action,trace_elapsed_us";

/// Exact per-cause gentle-mode exit counters. Rare transitions are aggregated
/// atomically and never consume bounded state-row capacity.
#[derive(Debug, Default)]
struct GentleExitCounters {
    loss: AtomicU64,
    gate_open: AtomicU64,
    drain_guard: AtomicU64,
    outage_reset: AtomicU64,
}

impl GentleExitCounters {
    fn counter(&self, cause: MetricsGentleExitCause) -> &AtomicU64 {
        match cause {
            MetricsGentleExitCause::Loss => &self.loss,
            MetricsGentleExitCause::GateOpen => &self.gate_open,
            MetricsGentleExitCause::DrainGuard => &self.drain_guard,
            MetricsGentleExitCause::OutageReset => &self.outage_reset,
        }
    }

    fn increment(&self, cause: MetricsGentleExitCause) {
        self.counter(cause).fetch_add(1, Ordering::Relaxed);
    }

    fn reset(&self) {
        for cause in MetricsGentleExitCause::ALL {
            self.counter(cause).store(0, Ordering::Relaxed);
        }
    }

    fn load(&self, cause: MetricsGentleExitCause) -> u64 {
        self.counter(cause).load(Ordering::Relaxed)
    }
}

/// Exact per-reason ACK-flush claim counters. Only successful transactional
/// claims are counted (never resume wake requests), and the aggregate atomics
/// never consume bounded state-row capacity.
#[derive(Debug, Default)]
struct AckFlushCounters {
    initial: AtomicU64,
    age: AtomicU64,
    count: AtomicU64,
    fin: AtomicU64,
    explicit: AtomicU64,
}

impl AckFlushCounters {
    fn counter(&self, reason: MetricsAckFlushReason) -> &AtomicU64 {
        match reason {
            MetricsAckFlushReason::Initial => &self.initial,
            MetricsAckFlushReason::Age => &self.age,
            MetricsAckFlushReason::Count => &self.count,
            MetricsAckFlushReason::Fin => &self.fin,
            MetricsAckFlushReason::Explicit => &self.explicit,
        }
    }

    fn increment(&self, reason: MetricsAckFlushReason) {
        self.counter(reason).fetch_add(1, Ordering::Relaxed);
    }

    fn reset(&self) {
        for reason in MetricsAckFlushReason::ALL {
            self.counter(reason).store(0, Ordering::Relaxed);
        }
    }

    fn load(&self, reason: MetricsAckFlushReason) -> u64 {
        self.counter(reason).load(Ordering::Relaxed)
    }
}

/// Connection-lifetime cumulative counters rebased at the measurement
/// boundary so warmup delivery is excluded from the retained evidence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CumulativeCounters {
    retransmission: MetricsRetransmissionCounters,
    fec: Option<MetricsFecCounters>,
    rto_deadline_postponements: u64,
    application_limited_detections: u64,
    application_limited_suppressions: u64,
    congestion_rate_samples: u64,
    congestion_probe_decisions: u64,
    congestion_probe_increases: u64,
    congestion_probe_before_feedback: u64,
    congestion_persistent_queue_resets: u64,
    congestion_delay_drains: u64,
    congestion_loss_backoffs: u64,
    congestion_loss_backoff_floor_bindings: u64,
}

impl CumulativeCounters {
    fn from_snapshot(snapshot: MetricsSnapshot) -> Self {
        Self {
            retransmission: snapshot.retransmission_counters,
            fec: snapshot.fec_counters,
            rto_deadline_postponements: snapshot.rto_deadline_postponements,
            application_limited_detections: snapshot.application_limited_detections,
            application_limited_suppressions: snapshot
                .application_limited_detections_suppressed_by_waiting_writer,
            congestion_rate_samples: snapshot.congestion_rate_samples,
            congestion_probe_decisions: snapshot.congestion_bandwidth_probe_decisions,
            congestion_probe_increases: snapshot.congestion_bandwidth_probe_increases,
            congestion_probe_before_feedback: snapshot.congestion_bandwidth_probe_before_feedback,
            congestion_persistent_queue_resets: snapshot.congestion_persistent_queue_resets,
            congestion_delay_drains: snapshot.congestion_delay_drains,
            congestion_loss_backoffs: snapshot.congestion_loss_backoffs,
            congestion_loss_backoff_floor_bindings: snapshot.congestion_loss_backoff_floor_bindings,
        }
    }

    fn apply_to(self, snapshot: &mut MetricsSnapshot) {
        snapshot.retransmission_counters = self.retransmission;
        snapshot.fec_counters = self.fec;
        snapshot.rto_deadline_postponements = self.rto_deadline_postponements;
        snapshot.application_limited_detections = self.application_limited_detections;
        snapshot.application_limited_detections_suppressed_by_waiting_writer =
            self.application_limited_suppressions;
        snapshot.congestion_rate_samples = self.congestion_rate_samples;
        snapshot.congestion_bandwidth_probe_decisions = self.congestion_probe_decisions;
        snapshot.congestion_bandwidth_probe_increases = self.congestion_probe_increases;
        snapshot.congestion_bandwidth_probe_before_feedback = self.congestion_probe_before_feedback;
        snapshot.congestion_persistent_queue_resets = self.congestion_persistent_queue_resets;
        snapshot.congestion_delay_drains = self.congestion_delay_drains;
        snapshot.congestion_loss_backoffs = self.congestion_loss_backoffs;
        snapshot.congestion_loss_backoff_floor_bindings =
            self.congestion_loss_backoff_floor_bindings;
    }

    fn since(self, baseline: Self) -> Self {
        let since = |current: u64, previous: u64| current.saturating_sub(previous);
        let group_sizes_since =
            |current: MetricsFecGroupSizeBuckets, previous: MetricsFecGroupSizeBuckets| {
                MetricsFecGroupSizeBuckets {
                    one: since(current.one, previous.one),
                    two_to_four: since(current.two_to_four, previous.two_to_four),
                    five_to_seven: since(current.five_to_seven, previous.five_to_seven),
                    full_eight: since(current.full_eight, previous.full_eight),
                }
            };
        let fec = self.fec.map(|current| {
            let previous = baseline.fec.unwrap_or_default();
            MetricsFecCounters {
                parity_sent: since(current.parity_sent, previous.parity_sent),
                groups_flushed: since(current.groups_flushed, previous.groups_flushed),
                flushed_group_sizes: group_sizes_since(
                    current.flushed_group_sizes,
                    previous.flushed_group_sizes,
                ),
                groups_skipped_no_surplus_tokens: since(
                    current.groups_skipped_no_surplus_tokens,
                    previous.groups_skipped_no_surplus_tokens,
                ),
                no_surplus_group_sizes: group_sizes_since(
                    current.no_surplus_group_sizes,
                    previous.no_surplus_group_sizes,
                ),
                groups_skipped_burst_end: since(
                    current.groups_skipped_burst_end,
                    previous.groups_skipped_burst_end,
                ),
                burst_end_group_sizes: group_sizes_since(
                    current.burst_end_group_sizes,
                    previous.burst_end_group_sizes,
                ),
                groups_skipped_loss_gate: since(
                    current.groups_skipped_loss_gate,
                    previous.groups_skipped_loss_gate,
                ),
                loss_gate_group_sizes: group_sizes_since(
                    current.loss_gate_group_sizes,
                    previous.loss_gate_group_sizes,
                ),
                groups_skipped_no_spare_capacity: since(
                    current.groups_skipped_no_spare_capacity,
                    previous.groups_skipped_no_spare_capacity,
                ),
                no_spare_capacity_group_sizes: group_sizes_since(
                    current.no_spare_capacity_group_sizes,
                    previous.no_spare_capacity_group_sizes,
                ),
                recovered_symbols: since(current.recovered_symbols, previous.recovered_symbols),
                dropped_malformed_packets: since(
                    current.dropped_malformed_packets,
                    previous.dropped_malformed_packets,
                ),
                dropped_decoder_panics: since(
                    current.dropped_decoder_panics,
                    previous.dropped_decoder_panics,
                ),
                rejected_recovered_symbols: since(
                    current.rejected_recovered_symbols,
                    previous.rejected_recovered_symbols,
                ),
            }
        });
        Self {
            retransmission: MetricsRetransmissionCounters {
                attempts: since(
                    self.retransmission.attempts,
                    baseline.retransmission.attempts,
                ),
                first_attempts: since(
                    self.retransmission.first_attempts,
                    baseline.retransmission.first_attempts,
                ),
                repeat_attempts: since(
                    self.retransmission.repeat_attempts,
                    baseline.retransmission.repeat_attempts,
                ),
                rto_reason: since(
                    self.retransmission.rto_reason,
                    baseline.retransmission.rto_reason,
                ),
                reorder_reason: since(
                    self.retransmission.reorder_reason,
                    baseline.retransmission.reorder_reason,
                ),
                fast_loss_reason: since(
                    self.retransmission.fast_loss_reason,
                    baseline.retransmission.fast_loss_reason,
                ),
                pre_outage_reason: since(
                    self.retransmission.pre_outage_reason,
                    baseline.retransmission.pre_outage_reason,
                ),
                tail_probes: since(
                    self.retransmission.tail_probes,
                    baseline.retransmission.tail_probes,
                ),
            },
            fec,
            rto_deadline_postponements: since(
                self.rto_deadline_postponements,
                baseline.rto_deadline_postponements,
            ),
            application_limited_detections: since(
                self.application_limited_detections,
                baseline.application_limited_detections,
            ),
            application_limited_suppressions: since(
                self.application_limited_suppressions,
                baseline.application_limited_suppressions,
            ),
            congestion_rate_samples: since(
                self.congestion_rate_samples,
                baseline.congestion_rate_samples,
            ),
            congestion_probe_decisions: since(
                self.congestion_probe_decisions,
                baseline.congestion_probe_decisions,
            ),
            congestion_probe_increases: since(
                self.congestion_probe_increases,
                baseline.congestion_probe_increases,
            ),
            congestion_probe_before_feedback: since(
                self.congestion_probe_before_feedback,
                baseline.congestion_probe_before_feedback,
            ),
            congestion_persistent_queue_resets: since(
                self.congestion_persistent_queue_resets,
                baseline.congestion_persistent_queue_resets,
            ),
            congestion_delay_drains: since(
                self.congestion_delay_drains,
                baseline.congestion_delay_drains,
            ),
            congestion_loss_backoffs: since(
                self.congestion_loss_backoffs,
                baseline.congestion_loss_backoffs,
            ),
            congestion_loss_backoff_floor_bindings: since(
                self.congestion_loss_backoff_floor_bindings,
                baseline.congestion_loss_backoff_floor_bindings,
            ),
        }
    }
}

/// One captured RTP observation plus its position on the shared trace clock.
#[derive(Debug, Clone, Copy)]
struct CapturedRtpObservation {
    observation: MetricsObservation,
    trace_elapsed: Duration,
}

/// One netem/progress sample: the scenario-relative `elapsed` and its shared
/// trace-clock position.
#[derive(Debug, Clone, Copy)]
struct NetemObservation {
    elapsed: Duration,
    trace_elapsed: Duration,
    c2s: CountersSnapshot,
    s2c: CountersSnapshot,
    delivered_bytes: u64,
}

/// A bounded, sealable capture of RTP observations for one endpoint. State
/// samples are throttled to [`STATE_SAMPLE_INTERVAL`] and raw RTT samples are
/// time-decimated to [`RTT_SAMPLE_INTERVAL`] on an independent clock;
/// termination rows bypass both throttles. Callbacks are synchronous and the
/// storage is bounded at `capacity`; the retained row rate is a fixed const
/// per endpoint (state + claimed RTT + terminations), so arbitrarily fast
/// lanes cannot exhaust capacity and long runs degrade evidence resolution
/// (fewer samples per second) rather than losing the run tail. First-N
/// truncation would bias per-unit-time evidence on fast lanes; uniform
/// time-decimation keeps the retained RTT series an unbiased sample of the
/// RTT process on the same grid for every lane. Rare scheduler/recovery
/// events are aggregate atomics and never consume bounded row capacity.
#[derive(Debug)]
struct RtpCapture {
    trace_start: Instant,
    observations: Mutex<Vec<CapturedRtpObservation>>,
    counter_baseline: Mutex<Option<CumulativeCounters>>,
    measurement_start_micros: AtomicU64,
    last_state_sample_micros: AtomicU64,
    last_rtt_sample_micros: AtomicU64,
    dropped_capacity: AtomicU64,
    send_driver_resume_signal_wakes: AtomicU64,
    send_driver_ack_schedule_signal_wakes: AtomicU64,
    send_driver_pacing_timer_wakes: AtomicU64,
    send_driver_protocol_timer_wakes: AtomicU64,
    send_driver_kill_requested_wakes: AtomicU64,
    send_driver_resume_application_data_requests: AtomicU64,
    send_driver_resume_application_frame_requests: AtomicU64,
    send_driver_resume_application_finish_requests: AtomicU64,
    send_driver_resume_peer_ack_requests: AtomicU64,
    send_driver_resume_ack_flush_requests: AtomicU64,
    send_driver_resume_post_open_handshake_requests: AtomicU64,
    send_driver_resume_receive_opportunity_requests: AtomicU64,
    retransmission_armor_duplicates: AtomicU64,
    data_send_would_blocks: AtomicU64,
    gentle_exits: GentleExitCounters,
    ack_flushes: AckFlushCounters,
    sealed: AtomicBool,
    capacity: usize,
}

impl RtpCapture {
    fn new(trace_start: Instant, capacity: usize) -> Self {
        Self {
            trace_start,
            observations: Mutex::new(Vec::with_capacity(capacity)),
            counter_baseline: Mutex::new(None),
            measurement_start_micros: AtomicU64::new(0),
            last_state_sample_micros: AtomicU64::new(u64::MAX),
            last_rtt_sample_micros: AtomicU64::new(u64::MAX),
            dropped_capacity: AtomicU64::new(0),
            send_driver_resume_signal_wakes: AtomicU64::new(0),
            send_driver_ack_schedule_signal_wakes: AtomicU64::new(0),
            send_driver_pacing_timer_wakes: AtomicU64::new(0),
            send_driver_protocol_timer_wakes: AtomicU64::new(0),
            send_driver_kill_requested_wakes: AtomicU64::new(0),
            send_driver_resume_application_data_requests: AtomicU64::new(0),
            send_driver_resume_application_frame_requests: AtomicU64::new(0),
            send_driver_resume_application_finish_requests: AtomicU64::new(0),
            send_driver_resume_peer_ack_requests: AtomicU64::new(0),
            send_driver_resume_ack_flush_requests: AtomicU64::new(0),
            send_driver_resume_post_open_handshake_requests: AtomicU64::new(0),
            send_driver_resume_receive_opportunity_requests: AtomicU64::new(0),
            retransmission_armor_duplicates: AtomicU64::new(0),
            data_send_would_blocks: AtomicU64::new(0),
            gentle_exits: GentleExitCounters::default(),
            ack_flushes: AckFlushCounters::default(),
            sealed: AtomicBool::new(false),
            capacity,
        }
    }

    fn record(&self, observation: MetricsObservation) {
        if self.sealed.load(Ordering::Acquire) {
            return;
        }
        let captured = CapturedRtpObservation {
            observation,
            trace_elapsed: self.trace_start.elapsed(),
        };
        let mut observations = self.observations.lock().unwrap();
        if self.sealed.load(Ordering::Acquire) {
            return;
        }
        let measurement_start = self.measurement_start_micros.load(Ordering::Acquire);
        if measurement_start != 0 && captured.trace_elapsed.as_micros() < measurement_start.into() {
            return;
        }
        if observations.len() >= self.capacity {
            self.dropped_capacity.fetch_add(1, Ordering::Relaxed);
        } else {
            observations.push(captured);
        }
    }

    /// Stop accepting callbacks. Runs before any output file is written so a
    /// callback that raced past the finish boundary cannot corrupt the rows.
    fn seal(&self) {
        self.sealed.store(true, Ordering::Release);
        drop(self.observations.lock().unwrap());
    }

    /// Anchor the measurement boundary and rebase connection-lifetime
    /// counters. Warmup rows are discarded, capacity state is reset, and one
    /// pre-boundary baseline snapshot is captured for later `since`
    /// subtraction while serializing sealed observations.
    fn begin_measurement(&self, trace_elapsed: Duration) {
        let boundary = u64::try_from(trace_elapsed.as_micros()).unwrap_or(u64::MAX);
        self.measurement_start_micros
            .store(boundary, Ordering::Release);
        let mut observations = self.observations.lock().unwrap();
        let counter_baseline = observations
            .iter()
            .filter(|captured| captured.trace_elapsed < trace_elapsed)
            .filter_map(|captured| {
                captured
                    .observation
                    .snapshot
                    .map(|snapshot| (captured.observation.event_index, snapshot))
            })
            .max_by_key(|(event_index, _)| *event_index)
            .map(|(_, snapshot)| CumulativeCounters::from_snapshot(snapshot));
        *self.counter_baseline.lock().unwrap() = counter_baseline;
        observations.retain(|captured| captured.trace_elapsed >= trace_elapsed);
        drop(observations);
        self.last_state_sample_micros
            .store(u64::MAX, Ordering::Relaxed);
        self.last_rtt_sample_micros
            .store(u64::MAX, Ordering::Relaxed);
        self.dropped_capacity.store(0, Ordering::Relaxed);
        self.send_driver_resume_signal_wakes
            .store(0, Ordering::Relaxed);
        self.send_driver_ack_schedule_signal_wakes
            .store(0, Ordering::Relaxed);
        self.send_driver_pacing_timer_wakes
            .store(0, Ordering::Relaxed);
        self.send_driver_protocol_timer_wakes
            .store(0, Ordering::Relaxed);
        self.send_driver_kill_requested_wakes
            .store(0, Ordering::Relaxed);
        self.send_driver_resume_application_data_requests
            .store(0, Ordering::Relaxed);
        self.send_driver_resume_application_frame_requests
            .store(0, Ordering::Relaxed);
        self.send_driver_resume_application_finish_requests
            .store(0, Ordering::Relaxed);
        self.send_driver_resume_peer_ack_requests
            .store(0, Ordering::Relaxed);
        self.send_driver_resume_ack_flush_requests
            .store(0, Ordering::Relaxed);
        self.send_driver_resume_post_open_handshake_requests
            .store(0, Ordering::Relaxed);
        self.send_driver_resume_receive_opportunity_requests
            .store(0, Ordering::Relaxed);
        self.retransmission_armor_duplicates
            .store(0, Ordering::Relaxed);
        self.data_send_would_blocks.store(0, Ordering::Relaxed);
        self.gentle_exits.reset();
        self.ack_flushes.reset();
    }

    /// Claim the next raw RTT sample on the RTT decimation clock: at most one
    /// sample per [`RTT_SAMPLE_INTERVAL`], with the first sample after the
    /// measurement boundary always claiming. Independent of the state clock
    /// so RTT retention never competes with state-row cadence.
    fn claim_rtt_sample_at(&self, elapsed: Duration) -> bool {
        let now = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let interval = RTT_SAMPLE_INTERVAL.as_micros() as u64;
        let mut previous = self.last_rtt_sample_micros.load(Ordering::Relaxed);
        loop {
            if previous != u64::MAX && now.saturating_sub(previous) < interval {
                return false;
            }
            match self.last_rtt_sample_micros.compare_exchange_weak(
                previous,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => previous = actual,
            }
        }
    }

    fn claim_state_sample_at(&self, elapsed: Duration) -> bool {
        let now = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let interval = STATE_SAMPLE_INTERVAL.as_micros() as u64;
        let mut previous = self.last_state_sample_micros.load(Ordering::Relaxed);
        loop {
            if previous != u64::MAX && now.saturating_sub(previous) < interval {
                return false;
            }
            match self.last_state_sample_micros.compare_exchange_weak(
                previous,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => previous = actual,
            }
        }
    }

    fn interest(&self, event: MetricsEvent, elapsed: Duration) -> MetricsInterest {
        if self.sealed.load(Ordering::Acquire) {
            return MetricsInterest::Skip;
        }
        match event {
            MetricsEvent::SendDriverWake(wake) => {
                let counter = match wake {
                    MetricsSendDriverWake::ResumeSignal => &self.send_driver_resume_signal_wakes,
                    MetricsSendDriverWake::AckScheduleSignal => {
                        &self.send_driver_ack_schedule_signal_wakes
                    }
                    MetricsSendDriverWake::PacingTimer => &self.send_driver_pacing_timer_wakes,
                    MetricsSendDriverWake::ProtocolTimer => &self.send_driver_protocol_timer_wakes,
                    MetricsSendDriverWake::KillRequested => &self.send_driver_kill_requested_wakes,
                };
                counter.fetch_add(1, Ordering::Relaxed);
                return MetricsInterest::Skip;
            }
            MetricsEvent::SendDriverResumeRequest(source) => {
                let counter = match source {
                    MetricsSendDriverResumeSource::ApplicationData => {
                        &self.send_driver_resume_application_data_requests
                    }
                    MetricsSendDriverResumeSource::ApplicationFrame => {
                        &self.send_driver_resume_application_frame_requests
                    }
                    MetricsSendDriverResumeSource::ApplicationFinish => {
                        &self.send_driver_resume_application_finish_requests
                    }
                    MetricsSendDriverResumeSource::PeerAck => {
                        &self.send_driver_resume_peer_ack_requests
                    }
                    MetricsSendDriverResumeSource::AckFlush => {
                        &self.send_driver_resume_ack_flush_requests
                    }
                    MetricsSendDriverResumeSource::PostOpenHandshake => {
                        &self.send_driver_resume_post_open_handshake_requests
                    }
                    MetricsSendDriverResumeSource::ReceiveOpportunity => {
                        &self.send_driver_resume_receive_opportunity_requests
                    }
                };
                counter.fetch_add(1, Ordering::Relaxed);
                return MetricsInterest::Skip;
            }
            MetricsEvent::GentleModeExit(cause) => {
                self.gentle_exits.increment(cause);
                return MetricsInterest::Skip;
            }
            MetricsEvent::RetransmissionArmorDuplicate => {
                self.retransmission_armor_duplicates
                    .fetch_add(1, Ordering::Relaxed);
                return MetricsInterest::Skip;
            }
            MetricsEvent::DataSendWouldBlock => {
                self.data_send_would_blocks.fetch_add(1, Ordering::Relaxed);
                return MetricsInterest::Skip;
            }
            MetricsEvent::AckFlush(reason) => {
                // A successful transactional claim, not a resume wake request.
                self.ack_flushes.increment(reason);
                return MetricsInterest::Skip;
            }
            _ => {}
        }
        if matches!(event, MetricsEvent::SessionTermination(_))
            || self.claim_state_sample_at(elapsed)
        {
            MetricsInterest::Snapshot
        } else if event == MetricsEvent::RttSample && self.claim_rtt_sample_at(elapsed) {
            MetricsInterest::EventOnly
        } else {
            MetricsInterest::Skip
        }
    }
}

/// Opt-in capture for performance probes. Set 'NETEM_PERF_TRACE_DIR' to an
/// empty output directory to enable it. Client and accepted-peer RTP state
/// are captured independently at 50 ms while raw RTT samples are
/// time-decimated at a finer 10 ms cadence on an independent clock, so
/// retention is a fixed rows-per-second constant per endpoint regardless of
/// lane speed. Storage is bounded and callback execution is synchronous; no
/// async channel or detached task is involved.
///
/// Set 'NETEM_PERF_TRACE_RTP=0' to retain only netem and application-progress
/// samples for an observer-free control run with the same output artifacts.
#[derive(Debug)]
pub struct PerfTrace {
    output_dir: PathBuf,
    capture_rtp: bool,
    trace_start: Instant,
    measurement_start_trace_elapsed: Option<Duration>,
    rtp: Arc<RtpCapture>,
    rtp_peer: Arc<RtpCapture>,
    netem: Vec<NetemObservation>,
    netem_baseline: Option<(CountersSnapshot, CountersSnapshot)>,
}

impl PerfTrace {
    fn new(output_dir: PathBuf, capture_rtp: bool) -> Self {
        let trace_start = Instant::now();
        Self {
            output_dir,
            capture_rtp,
            trace_start,
            measurement_start_trace_elapsed: None,
            rtp: Self::new_rtp_capture(trace_start),
            rtp_peer: Self::new_rtp_capture(trace_start),
            netem: Vec::new(),
            netem_baseline: None,
        }
    }

    fn new_rtp_capture(trace_start: Instant) -> Arc<RtpCapture> {
        Arc::new(RtpCapture::new(trace_start, DEFAULT_CAPACITY))
    }

    pub fn from_env() -> Option<Self> {
        let output_dir = std::env::var_os("NETEM_PERF_TRACE_DIR").map(PathBuf::from)?;
        let capture_rtp = std::env::var_os("NETEM_PERF_TRACE_RTP").is_none_or(|value| value != "0");
        Some(Self::new(output_dir, capture_rtp))
    }

    /// Anchor the measurement boundary on the shared trace clock. Netem and
    /// progress samples recorded after this call carry
    /// `measurement_start_trace_elapsed + scenario_elapsed`; RTP captures
    /// discard warmup rows and rebase their cumulative counters from the
    /// last pre-boundary snapshot. The netem counters are cumulative over the
    /// instrument's whole lifetime too, so [`PerfTrace::record_netem`] rebases
    /// them the same way, from the first sample inside the window.
    pub fn mark_measurement_start(&mut self, start: Instant) {
        let trace_elapsed = start.saturating_duration_since(self.trace_start);
        self.measurement_start_trace_elapsed = Some(trace_elapsed);
        self.rtp.begin_measurement(trace_elapsed);
        self.rtp_peer.begin_measurement(trace_elapsed);
    }

    pub fn rtp_observer(&self) -> Option<MetricsObserver> {
        self.observer_for(&self.rtp)
    }

    pub fn rtp_peer_observer(&self) -> Option<MetricsObserver> {
        self.observer_for(&self.rtp_peer)
    }

    fn observer_for(&self, rtp: &Arc<RtpCapture>) -> Option<MetricsObserver> {
        if !self.capture_rtp {
            return None;
        }
        let filter_capture = Arc::clone(rtp);
        let capture = Arc::clone(rtp);
        Some(MetricsObserver::selective(
            move |event, elapsed| filter_capture.interest(event, elapsed),
            move |observation| capture.record(observation),
        ))
    }

    /// Record one in-window netem/progress sample.
    ///
    /// `c2s`/`s2c` are the instrument's connection-lifetime counters, while
    /// `delivered_bytes` is already relative to the measurement boundary. The
    /// first sample recorded here is therefore retained as the window baseline
    /// and every row is written relative to it, so a trace's final netem row
    /// totals exactly the measurement window that `delivered_bytes` covers.
    /// Without the rebase the numerator of a bytes-per-delivered-byte ratio
    /// would span the whole process — the unmeasured warmup included — while
    /// the denominator spans only the window.
    pub fn record_netem(
        &mut self,
        elapsed: Duration,
        c2s: CountersSnapshot,
        s2c: CountersSnapshot,
        delivered_bytes: u64,
    ) {
        let trace_elapsed = self
            .measurement_start_trace_elapsed
            .map(|start| start + elapsed)
            .unwrap_or_else(|| self.trace_start.elapsed());
        self.netem_baseline.get_or_insert((c2s, s2c));
        self.netem.push(NetemObservation {
            elapsed,
            trace_elapsed,
            c2s,
            s2c,
            delivered_bytes,
        });
    }

    pub fn finish(self, metadata: &[(&str, String)]) -> io::Result<PathBuf> {
        self.rtp.seal();
        self.rtp_peer.seal();
        std::fs::create_dir_all(&self.output_dir)?;
        self.write_manifest(metadata)?;
        self.write_rtp(&self.rtp, "rtp.csv")?;
        self.write_rtp(&self.rtp_peer, "rtp_peer.csv")?;
        self.write_netem()?;
        self.write_progress()?;
        Ok(self.output_dir)
    }

    fn write_manifest(&self, metadata: &[(&str, String)]) -> io::Result<()> {
        let mut out = csv_writer(self.output_dir.join("manifest.csv"))?;
        writeln!(out, "key,value")?;
        write_csv_row(
            &mut out,
            &["trace_schema_version", &TRACE_SCHEMA_VERSION.to_string()],
        )?;
        write_csv_row(
            &mut out,
            &[
                "rtp_metrics_schema_version",
                &crate::metrics::SCHEMA_VERSION.to_string(),
            ],
        )?;
        write_csv_row(
            &mut out,
            &[
                "trace_finish_elapsed_us",
                &self.trace_start.elapsed().as_micros().to_string(),
            ],
        )?;
        write_csv_row(
            &mut out,
            &[
                "measurement_start_trace_elapsed_us",
                &self
                    .measurement_start_trace_elapsed
                    .map(|elapsed| elapsed.as_micros().to_string())
                    .unwrap_or_default(),
            ],
        )?;
        write_csv_row(&mut out, &["rtp_observer", &self.capture_rtp.to_string()])?;
        write_csv_row(
            &mut out,
            &[
                "rtp_state_sample_interval_micros",
                &STATE_SAMPLE_INTERVAL.as_micros().to_string(),
            ],
        )?;
        write_csv_row(
            &mut out,
            &[
                "rtp_rtt_sample_interval_micros",
                &RTT_SAMPLE_INTERVAL.as_micros().to_string(),
            ],
        )?;
        write_csv_row(&mut out, &["rtp_capacity", &self.rtp.capacity.to_string()])?;
        write_capture_health(&mut out, "rtp", &self.rtp)?;
        write_capture_health(&mut out, "rtp_peer", &self.rtp_peer)?;
        write_csv_row(&mut out, &["netem_samples", &self.netem.len().to_string()])?;
        write_csv_row(
            &mut out,
            &["progress_samples", &self.netem.len().to_string()],
        )?;
        for (key, value) in metadata {
            write_csv_row(&mut out, &[key, value])?;
        }
        Ok(())
    }

    fn write_rtp(&self, capture: &RtpCapture, filename: &str) -> io::Result<()> {
        let mut observations = capture.observations.lock().unwrap().clone();
        observations.sort_unstable_by_key(|captured| captured.observation.event_index);
        let counter_baseline = *capture.counter_baseline.lock().unwrap();
        let mut out = csv_writer(self.output_dir.join(filename))?;
        writeln!(out, "{}", rtp_trace_header())?;
        for captured in observations {
            let mut observation = captured.observation;
            if let (Some(baseline), Some(mut snapshot)) = (counter_baseline, observation.snapshot) {
                CumulativeCounters::from_snapshot(snapshot)
                    .since(baseline)
                    .apply_to(&mut snapshot);
                observation.snapshot = Some(snapshot);
            }
            writeln!(
                out,
                "{}",
                rtp_fields(observation, captured.trace_elapsed).join(",")
            )?;
        }
        Ok(())
    }

    fn write_netem(&self) -> io::Result<()> {
        let mut out = csv_writer(self.output_dir.join("netem.csv"))?;
        writeln!(
            out,
            "elapsed_us,trace_elapsed_us,direction,delayed,dropped,duplicated,reordered,rate_limited,forwarded,received,forwarded_bytes,received_bytes,overflow_dropped,scheduled_drain_batches,scheduled_drain_packets,scheduled_drain_max_packets,queue_len"
        )?;
        let (c2s_baseline, s2c_baseline) = self.netem_baseline.unwrap_or_default();
        for observation in &self.netem {
            write_netem_row(
                &mut out,
                observation.elapsed,
                observation.trace_elapsed,
                "c2s",
                netem_since(observation.c2s, c2s_baseline),
            )?;
            write_netem_row(
                &mut out,
                observation.elapsed,
                observation.trace_elapsed,
                "s2c",
                netem_since(observation.s2c, s2c_baseline),
            )?;
        }
        Ok(())
    }

    fn write_progress(&self) -> io::Result<()> {
        let mut out = csv_writer(self.output_dir.join("progress.csv"))?;
        writeln!(out, "elapsed_us,trace_elapsed_us,delivered_bytes")?;
        for observation in &self.netem {
            writeln!(
                out,
                "{},{},{}",
                observation.elapsed.as_micros(),
                observation.trace_elapsed.as_micros(),
                observation.delivered_bytes,
            )?;
        }
        Ok(())
    }
}

fn write_capture_health(
    out: &mut impl Write,
    prefix: &str,
    capture: &RtpCapture,
) -> io::Result<()> {
    write_csv_row(
        out,
        &[
            &format!("{prefix}_captured"),
            &capture.observations.lock().unwrap().len().to_string(),
        ],
    )?;
    write_csv_row(
        out,
        &[
            &format!("{prefix}_dropped_capacity"),
            &capture.dropped_capacity.load(Ordering::Relaxed).to_string(),
        ],
    )?;
    write_csv_row(
        out,
        &[
            &format!("{prefix}_counter_baseline_present"),
            &capture
                .counter_baseline
                .lock()
                .unwrap()
                .is_some()
                .to_string(),
        ],
    )?;
    for cause in MetricsGentleExitCause::ALL {
        write_csv_row(
            out,
            &[
                &format!("{prefix}_gentle_mode_exit_{}", cause.as_str()),
                &capture.gentle_exits.load(cause).to_string(),
            ],
        )?;
    }
    write_csv_row(
        out,
        &[
            &format!("{prefix}_retransmission_armor_duplicates"),
            &capture
                .retransmission_armor_duplicates
                .load(Ordering::Relaxed)
                .to_string(),
        ],
    )?;
    write_csv_row(
        out,
        &[
            &format!("{prefix}_data_send_would_blocks"),
            &capture
                .data_send_would_blocks
                .load(Ordering::Relaxed)
                .to_string(),
        ],
    )?;
    for reason in MetricsAckFlushReason::ALL {
        write_csv_row(
            out,
            &[
                &format!("{prefix}_ack_flush_{}_claims", reason.as_str()),
                &capture.ack_flushes.load(reason).to_string(),
            ],
        )?;
    }
    for (wake, count) in [
        (
            "resume_signal",
            capture
                .send_driver_resume_signal_wakes
                .load(Ordering::Relaxed),
        ),
        (
            "ack_schedule_signal",
            capture
                .send_driver_ack_schedule_signal_wakes
                .load(Ordering::Relaxed),
        ),
        (
            "pacing_timer",
            capture
                .send_driver_pacing_timer_wakes
                .load(Ordering::Relaxed),
        ),
        (
            "protocol_timer",
            capture
                .send_driver_protocol_timer_wakes
                .load(Ordering::Relaxed),
        ),
        (
            "kill_requested",
            capture
                .send_driver_kill_requested_wakes
                .load(Ordering::Relaxed),
        ),
    ] {
        write_csv_row(
            out,
            &[
                &format!("{prefix}_send_driver_{wake}_wakes"),
                &count.to_string(),
            ],
        )?;
    }
    for (source, count) in [
        (
            "application_data",
            capture
                .send_driver_resume_application_data_requests
                .load(Ordering::Relaxed),
        ),
        (
            "application_frame",
            capture
                .send_driver_resume_application_frame_requests
                .load(Ordering::Relaxed),
        ),
        (
            "application_finish",
            capture
                .send_driver_resume_application_finish_requests
                .load(Ordering::Relaxed),
        ),
        (
            "peer_ack",
            capture
                .send_driver_resume_peer_ack_requests
                .load(Ordering::Relaxed),
        ),
        (
            "ack_flush",
            capture
                .send_driver_resume_ack_flush_requests
                .load(Ordering::Relaxed),
        ),
        (
            "post_open_handshake",
            capture
                .send_driver_resume_post_open_handshake_requests
                .load(Ordering::Relaxed),
        ),
        (
            "receive_opportunity",
            capture
                .send_driver_resume_receive_opportunity_requests
                .load(Ordering::Relaxed),
        ),
    ] {
        write_csv_row(
            out,
            &[
                &format!("{prefix}_send_driver_resume_{source}_requests"),
                &count.to_string(),
            ],
        )?;
    }
    Ok(())
}

fn csv_writer(path: impl AsRef<Path>) -> io::Result<BufWriter<File>> {
    Ok(BufWriter::new(File::create(path)?))
}

/// The CSV header for the RTP trace, rendered from the single schema table so
/// that it can never disagree with the row [`rtp_fields`] produces.
fn rtp_trace_header() -> String {
    RTP_TRACE_COLUMNS
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(",")
}

/// The termination carried by a session-termination observation, if any.
fn termination(observation: &MetricsObservation) -> Option<MetricsTermination> {
    match observation.event {
        MetricsEvent::SessionTermination(termination) => Some(termination),
        _ => None,
    }
}

/// Render a snapshot column, empty when the observation carries no snapshot:
/// event-only rows keep every snapshot column empty rather than fabricating a
/// value.
fn snapshot_value(
    observation: &MetricsObservation,
    accessor: fn(&MetricsSnapshot) -> String,
) -> String {
    observation
        .snapshot
        .as_ref()
        .map(accessor)
        .unwrap_or_default()
}

/// Render a FEC column, empty when the observation carries no snapshot or the
/// connection ran with FEC disabled.
fn fec_value(
    observation: &MetricsObservation,
    accessor: fn(&MetricsFecCounters) -> String,
) -> String {
    observation
        .snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.fec_counters.as_ref())
        .map(accessor)
        .unwrap_or_default()
}

/// One RTP trace row, in the exact column order of [`RTP_TRACE_COLUMNS`].
fn rtp_fields(observation: MetricsObservation, trace_elapsed: Duration) -> Vec<String> {
    RTP_TRACE_COLUMNS
        .iter()
        .map(|(_, value)| value(&observation, trace_elapsed))
        .collect()
}

/// Express a netem counter snapshot relative to the measurement-window
/// baseline: every event counter becomes the number of events the window saw,
/// which is what a per-window price (bytes on the wire per byte delivered) has
/// to be divided by. `scheduled_drain_max_packets` is a running maximum rather
/// than an event count and `queue_len` is an instantaneous gauge, so both are
/// passed through unchanged — subtracting a baseline from either would be
/// meaningless. Each direction has a single writer, so its event counters are
/// monotone and the subtraction is exact rather than a clamp.
fn netem_since(snapshot: CountersSnapshot, baseline: CountersSnapshot) -> CountersSnapshot {
    let stats = snapshot.stats;
    let base = baseline.stats;
    CountersSnapshot {
        stats: Counters {
            delayed: stats.delayed.saturating_sub(base.delayed),
            dropped: stats.dropped.saturating_sub(base.dropped),
            duplicated: stats.duplicated.saturating_sub(base.duplicated),
            reordered: stats.reordered.saturating_sub(base.reordered),
            rate_limited: stats.rate_limited.saturating_sub(base.rate_limited),
            forwarded: stats.forwarded.saturating_sub(base.forwarded),
            received: stats.received.saturating_sub(base.received),
            forwarded_bytes: stats.forwarded_bytes.saturating_sub(base.forwarded_bytes),
            received_bytes: stats.received_bytes.saturating_sub(base.received_bytes),
            overflow_dropped: stats.overflow_dropped.saturating_sub(base.overflow_dropped),
            scheduled_drain_batches: stats
                .scheduled_drain_batches
                .saturating_sub(base.scheduled_drain_batches),
            scheduled_drain_packets: stats
                .scheduled_drain_packets
                .saturating_sub(base.scheduled_drain_packets),
            scheduled_drain_max_packets: stats.scheduled_drain_max_packets,
        },
        queue_len: snapshot.queue_len,
    }
}

fn write_netem_row(
    out: &mut impl Write,
    elapsed: Duration,
    trace_elapsed: Duration,
    direction: &str,
    snapshot: CountersSnapshot,
) -> io::Result<()> {
    let stats = snapshot.stats;
    writeln!(
        out,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        elapsed.as_micros(),
        trace_elapsed.as_micros(),
        direction,
        stats.delayed,
        stats.dropped,
        stats.duplicated,
        stats.reordered,
        stats.rate_limited,
        stats.forwarded,
        stats.received,
        stats.forwarded_bytes,
        stats.received_bytes,
        stats.overflow_dropped,
        stats.scheduled_drain_batches,
        stats.scheduled_drain_packets,
        stats.scheduled_drain_max_packets,
        snapshot.queue_len,
    )
}

fn write_csv_row(out: &mut impl Write, values: &[&str]) -> io::Result<()> {
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            write!(out, ",")?;
        }
        write!(out, "\"{}\"", value.replace('"', "\"\""))?;
    }
    writeln!(out)
}

fn optional_u128(value: Option<u128>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn optional_u64(value: Option<u64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn optional_f64(value: Option<f64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn optional_bool(value: Option<bool>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{
        MetricsSendDriverResumeSource, MetricsSendDriverWake, MetricsSnapshot, MetricsTermination,
        MetricsTerminationCause, SCHEMA_VERSION,
    };

    fn observation(event_index: u64, elapsed_ms: u64, event: MetricsEvent) -> MetricsObservation {
        MetricsObservation {
            schema_version: SCHEMA_VERSION,
            event_index,
            elapsed: Duration::from_millis(elapsed_ms),
            event,
            raw_rtt_sample: (event == MetricsEvent::RttSample).then(|| Duration::from_millis(20)),
            snapshot: Some(MetricsSnapshot {
                pacer_tokens_packets: 0.0,
                send_rate_packets_per_second: 1.0,
                loss_ratio: None,
                congestion_loss_ratio: None,
                congestion_action: None,
                in_flight_packets: 0,
                packets_in_pipe: 0,
                retransmission_active_packets: 0,
                retransmission_ready_packets: 0,
                retransmitted_packets: 0,
                next_send_sequence: 0,
                minimum_rtt: None,
                smoothed_rtt: Duration::from_millis(20),
                retransmission_timeout: Duration::from_millis(1000),
                oldest_pipe_packet_age: None,
                maximum_packet_rto_overdue: None,
                rto_deadline_postponements: 0,
                congestion_window_packets: 1,
                received_packets: 0,
                next_receive_sequence: None,
                delivery_rate_packets_per_second: None,
                delivery_sample_app_limited: None,
                retransmission_counters: MetricsRetransmissionCounters::default(),
                fec_counters: None,
                application_write_waiters: 0,
                application_limited_detections: 0,
                application_limited_detections_suppressed_by_waiting_writer: 0,
                congestion_control_rtt: None,
                congestion_rtt_floor: None,
                congestion_queue_tolerance: None,
                congestion_persistent_queue_for: None,
                congestion_persistent_queue_resets: 0,
                congestion_delivery_peak_packets_per_second: None,
                congestion_drain_floor_packets_per_second: None,
                congestion_drain_target_packets_per_second: None,
                congestion_loss_backoff_floor_packets_per_second: None,
                congestion_loss_backoff_raw_target_packets_per_second: None,
                congestion_loss_backoff_target_packets_per_second: None,
                congestion_loss_backoffs: 0,
                congestion_loss_backoff_floor_bindings: 0,
                congestion_rate_samples: 0,
                congestion_bandwidth_probe_decisions: 0,
                congestion_bandwidth_probe_increases: 0,
                congestion_bandwidth_probe_before_feedback: 0,
                congestion_last_bandwidth_probe_interval: None,
                congestion_delay_drains: 0,
                pending_send_bytes: 0,
                send_stage_capacity_bytes: 8192,
                accepts_new_packet: true,
                slow_start: true,
                gentle_mode: false,
                gentle_draining: false,
                queue_building: false,
                drain_floor_binding: false,
                outage_recovery: false,
                no_response_for: None,
                no_progress_for: None,
                stall_reason: None,
            }),
        }
    }

    fn rich_observation() -> MetricsObservation {
        MetricsObservation {
            schema_version: SCHEMA_VERSION,
            event_index: 7,
            elapsed: Duration::from_micros(100),
            event: MetricsEvent::SessionTermination(MetricsTermination {
                cause: MetricsTerminationCause::PeerKill,
                error_kind: std::io::ErrorKind::BrokenPipe,
                raw_os_error: Some(1234),
            }),
            raw_rtt_sample: Some(Duration::from_micros(1001)),
            snapshot: Some(MetricsSnapshot {
                pacer_tokens_packets: 8.0,
                send_rate_packets_per_second: 9.0,
                loss_ratio: Some(10.0),
                congestion_loss_ratio: Some(98.0),
                congestion_action: Some(crate::metrics::MetricsCongestionAction::DelayDrain),
                in_flight_packets: 11,
                packets_in_pipe: 12,
                retransmission_active_packets: 13,
                retransmission_ready_packets: 14,
                retransmitted_packets: 15,
                retransmission_counters: MetricsRetransmissionCounters {
                    attempts: 16,
                    first_attempts: 17,
                    repeat_attempts: 18,
                    rto_reason: 19,
                    reorder_reason: 20,
                    fast_loss_reason: 21,
                    pre_outage_reason: 22,
                    tail_probes: 23,
                },
                fec_counters: Some(MetricsFecCounters {
                    parity_sent: 24,
                    groups_flushed: 25,
                    flushed_group_sizes: MetricsFecGroupSizeBuckets {
                        one: 26,
                        two_to_four: 27,
                        five_to_seven: 28,
                        full_eight: 29,
                    },
                    groups_skipped_no_surplus_tokens: 30,
                    no_surplus_group_sizes: MetricsFecGroupSizeBuckets {
                        one: 31,
                        two_to_four: 32,
                        five_to_seven: 33,
                        full_eight: 34,
                    },
                    groups_skipped_burst_end: 35,
                    burst_end_group_sizes: MetricsFecGroupSizeBuckets {
                        one: 36,
                        two_to_four: 37,
                        five_to_seven: 38,
                        full_eight: 39,
                    },
                    groups_skipped_loss_gate: 40,
                    loss_gate_group_sizes: MetricsFecGroupSizeBuckets {
                        one: 41,
                        two_to_four: 42,
                        five_to_seven: 43,
                        full_eight: 44,
                    },
                    groups_skipped_no_spare_capacity: 45,
                    no_spare_capacity_group_sizes: MetricsFecGroupSizeBuckets {
                        one: 46,
                        two_to_four: 47,
                        five_to_seven: 48,
                        full_eight: 49,
                    },
                    recovered_symbols: 50,
                    dropped_malformed_packets: 51,
                    dropped_decoder_panics: 52,
                    rejected_recovered_symbols: 53,
                }),
                next_send_sequence: 54,
                minimum_rtt: Some(Duration::from_micros(55)),
                smoothed_rtt: Duration::from_micros(56),
                retransmission_timeout: Duration::from_micros(57),
                oldest_pipe_packet_age: Some(Duration::from_micros(58)),
                maximum_packet_rto_overdue: Some(Duration::from_micros(59)),
                rto_deadline_postponements: 60,
                congestion_window_packets: 61,
                received_packets: 62,
                next_receive_sequence: Some(63),
                delivery_rate_packets_per_second: Some(64.0),
                delivery_sample_app_limited: Some(true),
                application_write_waiters: 66,
                application_limited_detections: 67,
                application_limited_detections_suppressed_by_waiting_writer: 68,
                congestion_control_rtt: Some(Duration::from_micros(69)),
                congestion_rtt_floor: Some(Duration::from_micros(70)),
                congestion_queue_tolerance: Some(Duration::from_micros(71)),
                congestion_persistent_queue_for: Some(Duration::from_micros(72)),
                congestion_persistent_queue_resets: 73,
                congestion_delivery_peak_packets_per_second: Some(74.0),
                congestion_drain_floor_packets_per_second: Some(75.0),
                congestion_drain_target_packets_per_second: Some(76.0),
                congestion_loss_backoff_floor_packets_per_second: Some(77.0),
                congestion_loss_backoff_raw_target_packets_per_second: Some(78.0),
                congestion_loss_backoff_target_packets_per_second: Some(79.0),
                congestion_loss_backoffs: 80,
                congestion_loss_backoff_floor_bindings: 81,
                congestion_rate_samples: 82,
                congestion_bandwidth_probe_decisions: 83,
                congestion_bandwidth_probe_increases: 84,
                congestion_bandwidth_probe_before_feedback: 85,
                congestion_last_bandwidth_probe_interval: Some(Duration::from_micros(86)),
                congestion_delay_drains: 87,
                pending_send_bytes: 88,
                send_stage_capacity_bytes: 89,
                accepts_new_packet: true,
                slow_start: false,
                gentle_mode: true,
                gentle_draining: false,
                queue_building: true,
                drain_floor_binding: false,
                outage_recovery: true,
                no_response_for: Some(Duration::from_micros(95)),
                no_progress_for: Some(Duration::from_micros(96)),
                stall_reason: Some(crate::metrics::MetricsStallReason::NoProgress),
            }),
        }
    }

    /// The emitted RTP trace bytes are frozen. The header and the row are both
    /// rendered from [`RTP_TRACE_COLUMNS`], so this pins the schema end to end:
    /// a renamed, reordered, inserted, or removed column — or a name paired
    /// with another column's accessor — fails here instead of silently
    /// relabelling a metric in the evidence perf verdicts are read from.
    #[test]
    fn rtp_trace_schema_matches_the_frozen_wire_bytes() {
        const FROZEN_RTP_TRACE_ROW: &str = "29,7,100,session_termination,peer_kill,broken_pipe,1234,1001,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,true,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,true,false,true,false,true,false,true,95,96,no_progress,98,delay_drain,4242424";

        let header = rtp_trace_header();
        assert_eq!(
            header, FROZEN_RTP_TRACE_HEADER,
            "the column names and their order are frozen"
        );
        let columns: Vec<&str> = header.split(',').collect();
        let unique: std::collections::BTreeSet<&str> = columns.iter().copied().collect();
        assert_eq!(
            unique.len(),
            columns.len(),
            "a duplicate column name would make a name-keyed reader keep only the last one"
        );
        let row = rtp_fields(rich_observation(), Duration::from_micros(4_242_424));
        assert_eq!(row.len(), columns.len(), "one value per column");
        assert_eq!(
            row.join(","),
            FROZEN_RTP_TRACE_ROW,
            "each column must still carry its own value in wire order"
        );

        // A snapshot row with FEC disabled keeps the 30 FEC columns empty
        // without shifting any other column.
        let mut no_fec = rich_observation();
        no_fec.snapshot.as_mut().unwrap().fec_counters = None;
        let no_fec_row = rtp_fields(no_fec, Duration::from_micros(4_242_424));
        assert_eq!(no_fec_row.len(), columns.len());
        let fec_start = columns
            .iter()
            .position(|column| *column == "tail_probe_attempts")
            .unwrap()
            + 1;
        let fec_end = columns
            .iter()
            .position(|column| *column == "next_send_sequence")
            .unwrap();
        assert!(no_fec_row[fec_start..fec_end].iter().all(String::is_empty));
        for index in (0..fec_start).chain(fec_end..columns.len() - 1) {
            assert_eq!(
                no_fec_row[index], row[index],
                "disabling FEC must not shift column {index}"
            );
        }

        // The writer must emit those same bytes through the real path, not just
        // the renderers in isolation.
        let output_dir = std::env::temp_dir().join(format!(
            "rtp-trace-frozen-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&output_dir);
        let trace = PerfTrace::new(output_dir.clone(), false);
        trace.rtp.record(rich_observation());
        trace.finish(&[]).unwrap();
        let emitted = std::fs::read_to_string(output_dir.join("rtp.csv")).unwrap();
        let mut lines = emitted.lines();
        assert_eq!(lines.next().unwrap(), FROZEN_RTP_TRACE_HEADER);
        let emitted_row: Vec<&str> = lines.next().unwrap().split(',').collect();
        assert_eq!(emitted_row.len(), columns.len());
        // The last column is wall-clock trace elapsed; everything else is the
        // frozen observation and must match byte for byte.
        assert_eq!(&emitted_row[..columns.len() - 1], &row[..columns.len() - 1]);
        let _ = std::fs::remove_dir_all(&output_dir);
    }

    fn capture(trace_start: Instant, capacity: usize) -> RtpCapture {
        RtpCapture::new(trace_start, capacity)
    }

    #[test]
    fn state_and_rtt_samples_are_each_time_decimated_on_independent_clocks() {
        let capture = capture(Instant::now(), 8);
        // The first event claims the state clock.
        assert_eq!(
            capture.interest(MetricsEvent::SendDataPacketAttempt, Duration::ZERO),
            MetricsInterest::Snapshot
        );
        capture.record(observation(0, 0, MetricsEvent::SendDataPacketAttempt));
        // A second state event inside the state interval is throttled even
        // though the RTT clock is still unclaimed: the clocks are independent.
        assert_eq!(
            capture.interest(
                MetricsEvent::SendDataPacketAttempt,
                Duration::from_millis(1)
            ),
            MetricsInterest::Skip
        );
        // The first raw RTT sample claims the RTT clock and is retained.
        assert_eq!(
            capture.interest(MetricsEvent::RttSample, Duration::from_millis(2)),
            MetricsInterest::EventOnly
        );
        capture.record(observation(2, 2, MetricsEvent::RttSample));
        // Raw RTT samples are time-decimated on their own clock, mirroring
        // the state throttle: a sample inside the RTT interval is not
        // retained, so arbitrarily fast lanes cannot exhaust storage.
        assert_eq!(
            capture.interest(MetricsEvent::RttSample, Duration::from_millis(3)),
            MetricsInterest::Skip,
            "raw RTT samples inside {RTT_SAMPLE_INTERVAL:?} of the previous \
             claim must be decimated"
        );
        // The state clock is due again at 50 ms and claims independently of
        // the RTT clock.
        assert_eq!(
            capture.interest(MetricsEvent::ReceiveAckPacket, Duration::from_millis(50)),
            MetricsInterest::Snapshot
        );
        capture.record(observation(4, 50, MetricsEvent::ReceiveAckPacket));
        // One RTT interval after its own prior claim the RTT clock is due
        // again: the sample is retained even though it is inside the state
        // interval.
        let rtt_due = Duration::from_millis(2) + RTT_SAMPLE_INTERVAL;
        assert_eq!(
            capture.interest(MetricsEvent::RttSample, rtt_due),
            MetricsInterest::EventOnly
        );
        capture.record(observation(5, 52, MetricsEvent::RttSample));
        assert_eq!(
            capture.interest(MetricsEvent::RttSample, rtt_due + Duration::from_millis(1)),
            MetricsInterest::Skip
        );
        // Termination bypasses both throttles.
        assert_eq!(
            capture.interest(
                MetricsEvent::SessionTermination(MetricsTermination {
                    cause: MetricsTerminationCause::ProactiveStall,
                    error_kind: std::io::ErrorKind::BrokenPipe,
                    raw_os_error: None,
                }),
                Duration::from_millis(60)
            ),
            MetricsInterest::Snapshot,
            "termination must bypass the periodic state and RTT throttles"
        );

        let observations = capture.observations.lock().unwrap();
        assert_eq!(observations.len(), 4);
        assert_eq!(observations[0].observation.event_index, 0);
        assert_eq!(observations[1].observation.event_index, 2);
        assert_eq!(observations[2].observation.event_index, 4);
        assert_eq!(observations[3].observation.event_index, 5);
        // Every captured row carries its shared trace-clock position.
        for captured in observations.iter() {
            assert!(!captured.trace_elapsed.is_zero());
        }
    }

    #[test]
    fn rtt_decimation_clock_resets_at_the_measurement_boundary() {
        let trace_start = Instant::now();
        let capture = capture(trace_start, 8);
        // Warmup: the first event claims the state clock; the next RTT sample
        // claims the RTT clock; the one after is decimated.
        assert_eq!(
            capture.interest(MetricsEvent::SendDataPacketAttempt, Duration::ZERO),
            MetricsInterest::Snapshot
        );
        capture.record(observation(0, 0, MetricsEvent::SendDataPacketAttempt));
        assert_eq!(
            capture.interest(MetricsEvent::RttSample, Duration::from_millis(1)),
            MetricsInterest::EventOnly
        );
        capture.record(observation(1, 1, MetricsEvent::RttSample));
        assert_eq!(
            capture.interest(MetricsEvent::RttSample, Duration::from_millis(2)),
            MetricsInterest::Skip
        );
        std::thread::sleep(Duration::from_millis(2));
        let boundary = trace_start.elapsed();
        capture.begin_measurement(boundary);
        // Both decimation clocks reset with the capacity state: warmup rows
        // are discarded and the first post-boundary event claims the state
        // clock again ...
        assert_eq!(
            capture.interest(
                MetricsEvent::SendDataPacketAttempt,
                Duration::from_millis(100)
            ),
            MetricsInterest::Snapshot
        );
        capture.record(observation(100, 100, MetricsEvent::SendDataPacketAttempt));
        // ... while the first post-boundary RTT sample claims the RTT clock
        // again, even though it arrives inside the state interval.
        assert_eq!(
            capture.interest(MetricsEvent::RttSample, Duration::from_millis(101)),
            MetricsInterest::EventOnly
        );
        capture.record(observation(101, 101, MetricsEvent::RttSample));
        assert_eq!(
            capture.interest(MetricsEvent::RttSample, Duration::from_millis(102)),
            MetricsInterest::Skip
        );
        let observations = capture.observations.lock().unwrap();
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].observation.event_index, 100);
        assert_eq!(observations[1].observation.event_index, 101);
        assert_eq!(capture.dropped_capacity.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn event_only_and_snapshot_rows_match_the_schema_width() {
        let header = rtp_trace_header();
        assert_eq!(header.split(',').count(), RTP_TRACE_COLUMNS.len());
        let mut snapshot = observation(0, 0, MetricsEvent::SendDataPacketAttempt);
        snapshot.snapshot.as_mut().unwrap().fec_counters = Some(MetricsFecCounters {
            parity_sent: 1,
            groups_flushed: 2,
            flushed_group_sizes: MetricsFecGroupSizeBuckets {
                one: 3,
                two_to_four: 4,
                five_to_seven: 5,
                full_eight: 6,
            },
            groups_skipped_no_surplus_tokens: 7,
            no_surplus_group_sizes: MetricsFecGroupSizeBuckets {
                one: 8,
                two_to_four: 9,
                five_to_seven: 10,
                full_eight: 11,
            },
            groups_skipped_burst_end: 12,
            burst_end_group_sizes: MetricsFecGroupSizeBuckets {
                one: 13,
                two_to_four: 14,
                five_to_seven: 15,
                full_eight: 16,
            },
            groups_skipped_loss_gate: 17,
            loss_gate_group_sizes: MetricsFecGroupSizeBuckets {
                one: 18,
                two_to_four: 19,
                five_to_seven: 20,
                full_eight: 21,
            },
            groups_skipped_no_spare_capacity: 22,
            no_spare_capacity_group_sizes: MetricsFecGroupSizeBuckets {
                one: 23,
                two_to_four: 24,
                five_to_seven: 25,
                full_eight: 26,
            },
            recovered_symbols: 27,
            dropped_malformed_packets: 28,
            dropped_decoder_panics: 29,
            rejected_recovered_symbols: 30,
        });
        let mut event_only = observation(1, 1, MetricsEvent::RttSample);
        event_only.snapshot = None;
        let trace_elapsed = Duration::from_micros(123);
        let snapshot_fields = rtp_fields(snapshot, trace_elapsed);
        assert_eq!(snapshot_fields.len(), RTP_TRACE_COLUMNS.len());
        let event_only_fields = rtp_fields(event_only, trace_elapsed);
        assert_eq!(event_only_fields.len(), RTP_TRACE_COLUMNS.len());
        assert_eq!(event_only_fields[7], "20000");
        assert!(
            event_only_fields[8..RTP_TRACE_COLUMNS.len() - 1]
                .iter()
                .all(String::is_empty)
        );
        assert_eq!(event_only_fields[RTP_TRACE_COLUMNS.len() - 1], "123");

        // The 30 typed FEC columns sit immediately after `tail_probe_attempts`.
        let columns: Vec<&str> = header.split(',').collect();
        let fec_start = columns
            .iter()
            .position(|column| *column == "tail_probe_attempts")
            .unwrap()
            + 1;
        let fec_end = columns
            .iter()
            .position(|column| *column == "next_send_sequence")
            .unwrap();
        assert_eq!(fec_end - fec_start, 30);
        for (index, column) in columns[fec_start..fec_end].iter().enumerate() {
            assert!(
                column.starts_with("fec_"),
                "column {} must be an FEC column: {column}",
                index
            );
        }
        // Snapshot rows serialize every FEC member in header order ...
        let expected_fec: Vec<&str> = vec![
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
            "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
        ];
        assert_eq!(&snapshot_fields[fec_start..fec_end], &expected_fec);
        // ... while event-only rows leave them empty, preserving the width.
        assert!(
            event_only_fields[fec_start..fec_end]
                .iter()
                .all(String::is_empty)
        );
    }

    #[test]
    fn sealed_capture_rejects_callbacks_after_finish_starts() {
        let trace_start = Instant::now();
        let capture = capture(trace_start, 2);
        capture.record(observation(0, 0, MetricsEvent::SendDataPacketAttempt));
        capture.seal();
        // A callback that raced past the finish boundary must be rejected:
        // the captured set is frozen and the count does not grow.
        capture.record(observation(1, 1, MetricsEvent::SendDataPacketAttempt));
        capture.record(observation(2, 2, MetricsEvent::RttSample));
        let observations = capture.observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].observation.event_index, 0);
        assert_eq!(capture.dropped_capacity.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn observer_free_capture_has_no_rtp_callback() {
        let trace = PerfTrace::new(PathBuf::from("unused"), false);
        assert!(trace.rtp_observer().is_none());
        assert!(trace.rtp_peer_observer().is_none());
    }

    /// The netem instrument counts for its whole lifetime, so every warmup
    /// byte of the unmeasured run is already in the counters when the first
    /// in-window sample is taken, while `delivered_bytes` counts from the
    /// measurement boundary. The trace must rebase, because the ratio a trace
    /// is read for — `wire_bytes_per_delivered_byte`, which the harness's
    /// `rtp_trace_compare.py` computes as each direction's final netem row's
    /// `forwarded_bytes`, summed, over `delivered_bytes` — is a window price
    /// and is meaningless if its numerator spans the process.
    #[test]
    fn netem_counters_are_rebased_to_the_measurement_window() {
        let output_dir = std::env::temp_dir().join(format!(
            "rtp-netem-rebase-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&output_dir);
        let mut trace = PerfTrace::new(output_dir.clone(), false);
        // The baseline sample already carries the whole warmup.
        let warmup = CountersSnapshot {
            stats: Counters {
                delayed: 900,
                dropped: 9,
                duplicated: 1,
                reordered: 2,
                rate_limited: 900,
                forwarded: 1_000,
                received: 1_010,
                forwarded_bytes: 8_000_000,
                received_bytes: 8_080_000,
                overflow_dropped: 3,
                scheduled_drain_batches: 990,
                scheduled_drain_packets: 1_000,
                scheduled_drain_max_packets: 12,
            },
            queue_len: 7,
        };
        trace.mark_measurement_start(Instant::now());
        trace.record_netem(Duration::from_micros(500), warmup, warmup, 0);
        trace.record_netem(
            Duration::from_millis(50),
            CountersSnapshot {
                stats: Counters {
                    delayed: 930,
                    dropped: 10,
                    duplicated: 1,
                    reordered: 2,
                    rate_limited: 930,
                    forwarded: 1_100,
                    received: 1_111,
                    forwarded_bytes: 8_800_000,
                    received_bytes: 8_888_000,
                    overflow_dropped: 3,
                    scheduled_drain_batches: 1_090,
                    scheduled_drain_packets: 1_100,
                    scheduled_drain_max_packets: 12,
                },
                queue_len: 3,
            },
            CountersSnapshot {
                stats: Counters {
                    forwarded: 1_010,
                    received: 1_010,
                    forwarded_bytes: 8_088_888,
                    received_bytes: 8_080_000,
                    scheduled_drain_max_packets: 12,
                    ..warmup.stats
                },
                queue_len: 1,
            },
            4_096,
        );
        trace.finish(&[]).unwrap();

        let netem = std::fs::read_to_string(output_dir.join("netem.csv")).unwrap();
        let rows: Vec<Vec<&str>> = netem
            .lines()
            .skip(1)
            .map(|line| line.split(',').collect())
            .collect();
        assert_eq!(rows.len(), 4, "one c2s and one s2c row per sample");
        assert_eq!(rows[0][2], "c2s");
        assert_eq!(rows[1][2], "s2c");
        assert_eq!(rows[2][2], "c2s");
        assert_eq!(rows[3][2], "s2c");
        // The baseline sample is the window origin: no traffic yet, and the
        // gauge is the live queue length rather than a difference.
        assert_eq!(
            &rows[0][3..16],
            &[
                "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "12"
            ],
            "the first in-window sample must be the zeroed baseline, with the \
             running-maximum drain counter passed through"
        );
        assert_eq!(rows[0][16], "7");
        // The last row per direction is what a comparison prices: the c2s
        // excess over the baseline is 800_000 bytes, and the s2c excess is
        // 88_888 bytes. Under the un-rebased counters the same rows would read
        // 8_800_000 and 8_088_888, i.e. the warmup would be charged to the
        // window.
        assert_eq!(rows[2][8], "100");
        assert_eq!(rows[2][9], "101");
        assert_eq!(rows[2][10], "800000");
        assert_eq!(rows[2][11], "808000");
        assert_eq!(rows[3][8], "10");
        assert_eq!(rows[3][10], "88888");
        // The denominator really is the window: the progress row that a
        // comparison reads is the same cumulative-with-warmup-subtracted
        // number.
        let progress = std::fs::read_to_string(output_dir.join("progress.csv")).unwrap();
        assert_eq!(
            progress.lines().nth(1).unwrap().rsplit(',').next(),
            Some("0")
        );
        assert_eq!(
            progress.lines().nth(2).unwrap().rsplit(',').next(),
            Some("4096")
        );
        // The price a comparison derives from this trace, spelled the way
        // `rtp_trace_compare.py` spells it: the final netem row of each
        // direction over the measurement-window delivery. Every term is a
        // window quantity, so the price is what the transfer cost rather than
        // what the process spent.
        let delivered: f64 = progress
            .lines()
            .nth(2)
            .unwrap()
            .rsplit(',')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        let c2s_window: f64 = rows[2][10].parse().unwrap();
        let s2c_window: f64 = rows[3][10].parse().unwrap();
        assert_eq!(c2s_window, 800_000.0);
        assert_eq!(s2c_window, 88_888.0);
        assert_eq!(delivered, 4_096.0);
        assert_eq!((c2s_window + s2c_window) / delivered, 217.013671875);
        let _ = std::fs::remove_dir_all(&output_dir);
    }

    #[test]
    fn measurement_boundary_discards_warmup_rows_and_resets_capacity_state() {
        let trace_start = Instant::now();
        let capture = capture(trace_start, 2);
        for index in 0..3 {
            capture.record(observation(
                index,
                index,
                MetricsEvent::SendDataPacketAttempt,
            ));
        }
        // The third row overflows the bounded capacity while still warmup.
        assert_eq!(capture.dropped_capacity.load(Ordering::Relaxed), 1);
        std::thread::sleep(Duration::from_millis(2));
        let boundary = trace_start.elapsed();
        capture.begin_measurement(boundary);
        // Warmup rows are discarded and the capacity state is reset, but the
        // last pre-boundary snapshot is retained as the counter baseline.
        assert_eq!(capture.observations.lock().unwrap().len(), 0);
        assert_eq!(capture.dropped_capacity.load(Ordering::Relaxed), 0);
        assert!(
            capture.counter_baseline.lock().unwrap().is_some(),
            "begin_measurement must retain the last pre-boundary snapshot as the counter baseline"
        );
        capture.record(observation(3, 3, MetricsEvent::SendDataPacketAttempt));
        let observations = capture.observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].observation.event_index, 3);
    }

    #[test]
    fn ack_flush_reasons_are_counted_without_consuming_trace_rows() {
        use crate::metrics::MetricsAckFlushReason;
        let capture = capture(Instant::now(), 8);
        for (index, reason) in [
            MetricsAckFlushReason::Initial,
            MetricsAckFlushReason::Age,
            MetricsAckFlushReason::Count,
            MetricsAckFlushReason::Fin,
            MetricsAckFlushReason::Explicit,
            MetricsAckFlushReason::Initial,
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                capture.interest(
                    MetricsEvent::AckFlush(reason),
                    Duration::from_millis(index as u64)
                ),
                MetricsInterest::Skip,
                "a claim event is a counted aggregate, never a state row"
            );
        }
        assert_eq!(
            capture.observations.lock().unwrap().len(),
            0,
            "claim counters must not consume bounded state-row capacity"
        );
        assert_eq!(capture.ack_flushes.load(MetricsAckFlushReason::Initial), 2);
        assert_eq!(capture.ack_flushes.load(MetricsAckFlushReason::Age), 1);
        assert_eq!(capture.ack_flushes.load(MetricsAckFlushReason::Count), 1);
        assert_eq!(capture.ack_flushes.load(MetricsAckFlushReason::Fin), 1);
        assert_eq!(capture.ack_flushes.load(MetricsAckFlushReason::Explicit), 1);
        // The measurement boundary rebases the claim counters alongside the
        // other aggregates.
        capture.begin_measurement(capture.trace_start.elapsed());
        assert_eq!(
            capture.ack_flushes.load(MetricsAckFlushReason::Initial),
            0,
            "begin_measurement must reset the claim counters"
        );
    }

    #[test]
    fn cumulative_counters_start_at_the_measurement_boundary() {
        fn fec(base: u64, malformed: u64) -> MetricsFecCounters {
            MetricsFecCounters {
                parity_sent: base,
                groups_flushed: base,
                flushed_group_sizes: MetricsFecGroupSizeBuckets {
                    one: base,
                    two_to_four: base,
                    five_to_seven: base,
                    full_eight: base,
                },
                groups_skipped_no_surplus_tokens: base,
                no_surplus_group_sizes: MetricsFecGroupSizeBuckets {
                    one: base,
                    two_to_four: base,
                    five_to_seven: base,
                    full_eight: base,
                },
                groups_skipped_burst_end: base,
                burst_end_group_sizes: MetricsFecGroupSizeBuckets {
                    one: base,
                    two_to_four: base,
                    five_to_seven: base,
                    full_eight: base,
                },
                groups_skipped_loss_gate: base,
                loss_gate_group_sizes: MetricsFecGroupSizeBuckets {
                    one: base,
                    two_to_four: base,
                    five_to_seven: base,
                    full_eight: base,
                },
                groups_skipped_no_spare_capacity: base,
                no_spare_capacity_group_sizes: MetricsFecGroupSizeBuckets {
                    one: base,
                    two_to_four: base,
                    five_to_seven: base,
                    full_eight: base,
                },
                recovered_symbols: base,
                dropped_malformed_packets: malformed,
                dropped_decoder_panics: base,
                rejected_recovered_symbols: base,
            }
        }
        // Stamp every cumulative retransmission and FEC member onto a
        // snapshot; `malformed` lets the malformed-packet input decrease so
        // the saturating subtraction is exercised.
        fn stamp(snapshot: &mut MetricsSnapshot, base: u64, malformed: u64) {
            snapshot.retransmission_counters = MetricsRetransmissionCounters {
                attempts: base,
                first_attempts: base,
                repeat_attempts: base,
                rto_reason: base,
                reorder_reason: base,
                fast_loss_reason: base,
                pre_outage_reason: base,
                tail_probes: base,
            };
            snapshot.fec_counters = Some(fec(base, malformed));
            snapshot.rto_deadline_postponements = base;
            snapshot.application_limited_detections = base;
            snapshot.application_limited_detections_suppressed_by_waiting_writer = base;
            snapshot.congestion_rate_samples = base;
            snapshot.congestion_bandwidth_probe_decisions = base;
            snapshot.congestion_bandwidth_probe_increases = base;
            snapshot.congestion_bandwidth_probe_before_feedback = base;
            snapshot.congestion_persistent_queue_resets = base;
            snapshot.congestion_delay_drains = base;
            snapshot.congestion_loss_backoffs = base;
            snapshot.congestion_loss_backoff_floor_bindings = base;
        }

        let trace_start = Instant::now();
        let capture = capture(trace_start, 8);
        let mut warmup = observation(0, 0, MetricsEvent::SendDataPacketAttempt);
        stamp(warmup.snapshot.as_mut().unwrap(), 10, 10);
        capture.record(warmup);
        std::thread::sleep(Duration::from_millis(2));
        let mut boundary_snapshot = observation(1, 1, MetricsEvent::ReceiveAckPacket);
        stamp(boundary_snapshot.snapshot.as_mut().unwrap(), 17, 17);
        capture.record(boundary_snapshot);
        std::thread::sleep(Duration::from_millis(2));
        let boundary = trace_start.elapsed();
        capture.begin_measurement(boundary);
        // The baseline is the last pre-boundary snapshot.
        let baseline = capture.counter_baseline.lock().unwrap().unwrap();
        assert_eq!(baseline.rto_deadline_postponements, 17);
        let mut after = observation(2, 2, MetricsEvent::SendDataPacketAttempt);
        // The malformed-packet input decreases from 17 to 12: saturating
        // subtraction must rebase it to zero, never wrapping.
        stamp(after.snapshot.as_mut().unwrap(), 23, 12);
        capture.record(after);
        let retained = capture.observations.lock().unwrap();
        assert_eq!(retained.len(), 1);
        let snapshot = retained[0].observation.snapshot.unwrap();
        drop(retained);
        let rebased = CumulativeCounters::from_snapshot(snapshot).since(baseline);
        // Every retransmission member rebases 23 - 17.
        assert_eq!(
            rebased.retransmission,
            MetricsRetransmissionCounters {
                attempts: 6,
                first_attempts: 6,
                repeat_attempts: 6,
                rto_reason: 6,
                reorder_reason: 6,
                fast_loss_reason: 6,
                pre_outage_reason: 6,
                tail_probes: 6,
            }
        );
        // Every FEC member rebases 23 - 17 except the decreasing malformed
        // input, which saturates to zero; the lane stays `Some` throughout.
        assert_eq!(rebased.fec, Some(fec(6, 0)));
        assert_eq!(rebased.rto_deadline_postponements, 6);
        assert_eq!(rebased.application_limited_detections, 6);
        assert_eq!(rebased.application_limited_suppressions, 6);
        assert_eq!(rebased.congestion_rate_samples, 6);
        assert_eq!(rebased.congestion_probe_decisions, 6);
        assert_eq!(rebased.congestion_probe_increases, 6);
        assert_eq!(rebased.congestion_probe_before_feedback, 6);
        assert_eq!(rebased.congestion_persistent_queue_resets, 6);
        assert_eq!(rebased.congestion_delay_drains, 6);
        assert_eq!(rebased.congestion_loss_backoffs, 6);
        assert_eq!(rebased.congestion_loss_backoff_floor_bindings, 6);
        // apply_to restores the typed FEC counters onto the snapshot.
        let mut applied = snapshot;
        rebased.apply_to(&mut applied);
        assert_eq!(applied.fec_counters, Some(fec(6, 0)));
        assert_eq!(applied.retransmission_counters.attempts, 6);
        // A non-FEC lane stays `None`, never fabricated zero.
        let mut no_fec = observation(3, 3, MetricsEvent::SendDataPacketAttempt);
        stamp(no_fec.snapshot.as_mut().unwrap(), 23, 12);
        no_fec.snapshot.as_mut().unwrap().fec_counters = None;
        let mut no_fec_baseline = observation(4, 4, MetricsEvent::SendDataPacketAttempt);
        stamp(no_fec_baseline.snapshot.as_mut().unwrap(), 17, 17);
        no_fec_baseline.snapshot.as_mut().unwrap().fec_counters = None;
        let rebased_none = CumulativeCounters::from_snapshot(no_fec.snapshot.unwrap()).since(
            CumulativeCounters::from_snapshot(no_fec_baseline.snapshot.unwrap()),
        );
        assert_eq!(rebased_none.fec, None);
    }

    #[test]
    fn send_driver_wakes_are_counted_without_consuming_trace_rows() {
        let capture = capture(Instant::now(), 8);
        capture.record(observation(0, 0, MetricsEvent::SendDataPacketAttempt));
        let rows_before = capture.observations.lock().unwrap().len();
        for wake in [
            MetricsSendDriverWake::ResumeSignal,
            MetricsSendDriverWake::AckScheduleSignal,
            MetricsSendDriverWake::PacingTimer,
            MetricsSendDriverWake::ProtocolTimer,
            MetricsSendDriverWake::KillRequested,
        ] {
            assert_eq!(
                capture.interest(MetricsEvent::SendDriverWake(wake), Duration::from_millis(1)),
                MetricsInterest::Skip,
            );
        }
        assert_eq!(
            capture
                .send_driver_resume_signal_wakes
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            capture
                .send_driver_ack_schedule_signal_wakes
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            capture
                .send_driver_pacing_timer_wakes
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            capture
                .send_driver_protocol_timer_wakes
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            capture
                .send_driver_kill_requested_wakes
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(capture.observations.lock().unwrap().len(), rows_before);
    }

    #[test]
    fn gentle_mode_exit_causes_are_counted_without_consuming_trace_rows() {
        let capture = capture(Instant::now(), 8);
        capture.record(observation(0, 0, MetricsEvent::SendDataPacketAttempt));
        let rows_before = capture.observations.lock().unwrap().len();
        for cause in MetricsGentleExitCause::ALL {
            assert_eq!(
                capture.interest(
                    MetricsEvent::GentleModeExit(cause),
                    Duration::from_millis(1)
                ),
                MetricsInterest::Skip,
            );
            assert_eq!(capture.gentle_exits.load(cause), 1);
        }
        assert_eq!(capture.observations.lock().unwrap().len(), rows_before);
    }

    #[test]
    fn retransmission_armor_duplicates_are_counted_without_consuming_trace_rows() {
        let capture = capture(Instant::now(), 8);
        capture.record(observation(0, 0, MetricsEvent::SendDataPacketAttempt));
        let rows_before = capture.observations.lock().unwrap().len();
        assert_eq!(
            capture.interest(
                MetricsEvent::RetransmissionArmorDuplicate,
                Duration::from_millis(1)
            ),
            MetricsInterest::Skip,
        );
        assert_eq!(
            capture
                .retransmission_armor_duplicates
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(capture.observations.lock().unwrap().len(), rows_before);
    }

    #[test]
    fn data_send_would_blocks_are_counted_without_consuming_trace_rows() {
        let capture = capture(Instant::now(), 8);
        capture.record(observation(0, 0, MetricsEvent::SendDataPacketAttempt));
        let rows_before = capture.observations.lock().unwrap().len();
        assert_eq!(
            capture.interest(MetricsEvent::DataSendWouldBlock, Duration::from_millis(1)),
            MetricsInterest::Skip,
        );
        assert_eq!(capture.data_send_would_blocks.load(Ordering::Relaxed), 1);
        assert_eq!(capture.observations.lock().unwrap().len(), rows_before);
    }

    #[test]
    fn send_driver_resume_requests_are_counted_without_consuming_trace_rows() {
        let capture = capture(Instant::now(), 8);
        capture.record(observation(0, 0, MetricsEvent::SendDataPacketAttempt));
        let rows_before = capture.observations.lock().unwrap().len();
        for source in [
            MetricsSendDriverResumeSource::ApplicationData,
            MetricsSendDriverResumeSource::ApplicationFrame,
            MetricsSendDriverResumeSource::ApplicationFinish,
            MetricsSendDriverResumeSource::PeerAck,
            MetricsSendDriverResumeSource::AckFlush,
            MetricsSendDriverResumeSource::PostOpenHandshake,
            MetricsSendDriverResumeSource::ReceiveOpportunity,
        ] {
            assert_eq!(
                capture.interest(
                    MetricsEvent::SendDriverResumeRequest(source),
                    Duration::from_millis(1)
                ),
                MetricsInterest::Skip,
            );
        }
        assert_eq!(
            capture
                .send_driver_resume_application_data_requests
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            capture
                .send_driver_resume_application_frame_requests
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            capture
                .send_driver_resume_application_finish_requests
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            capture
                .send_driver_resume_peer_ack_requests
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            capture
                .send_driver_resume_ack_flush_requests
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            capture
                .send_driver_resume_post_open_handshake_requests
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            capture
                .send_driver_resume_receive_opportunity_requests
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(capture.observations.lock().unwrap().len(), rows_before);
    }
}
