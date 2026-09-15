//! Encapsulated congestion-response policy.
//!
//! One owner (`CongestionResponse`) holds queue detection, delivered-peak
//! tracking, ordinary probing, and drain-floor hysteresis together so their
//! resets and episode continuity stay atomic at the policy level.  Precedence
//! is probe, transient queue hold, persistent delay drain, then loss backoff;
//! a loss sample blocks every delay-control branch.
use std::time::{Duration, Instant};

use super::gentle::{DRAIN_RATE_FRACTION, GentleExitCause, GentleProbeOutcome};
use super::{OrdinaryBandwidthProbe, QueueGrowth, WindowedDeliveryMax};
use crate::traffic_shaping::recovery::rtt_stats::GateJitter;
use decision::{ResponsePath, select_path};
use loss_backoff::{LossBackoff, LossBackoffInput};
use queue_response::{DrainInput, QueueResponse};

mod decision;
mod loss_backoff;
mod queue_response;

pub(crate) use decision::{CongestionDecision, CongestionOutcome, ProbeKind};
pub(crate) use loss_backoff::linear_backoff_step;
#[cfg(test)]
pub(crate) use queue_response::DRAIN_FLOOR_PEAK_FRACTION;

pub(crate) const CC_DATA_LOSS_RATE: f64 = 0.2;

/// What the controller observed about the path during one sample.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CongestionObservation {
    pub(crate) floor: Duration,
    pub(crate) tolerance: Duration,
    pub(crate) queue_building: bool,
    pub(crate) persistent_for: Option<Duration>,
    pub(crate) peak_delivery: f64,
    pub(crate) loss_blocks_delay_control: bool,
    gentle_exit: Option<GentleExitCause>,
}

/// Transport inputs available to one congestion-response decision.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CongestionInput {
    pub(crate) delivery_rate: f64,
    pub(crate) current_rate: f64,
    pub(crate) smooth_rtt: Duration,
    pub(crate) control_rtt: Duration,
    pub(crate) loss_event_rate: Option<f64>,
    pub(crate) minimum_rate: f64,
    pub(crate) initial_rate: f64,
    pub(crate) now: Instant,
}

/// Single atomic owner of the congestion-response policy state.
#[derive(Debug)]
pub(crate) struct CongestionResponse {
    queue_growth: QueueGrowth,
    delivery_peak: WindowedDeliveryMax,
    bandwidth_probe: OrdinaryBandwidthProbe,
    queue_response: QueueResponse,
    loss_backoff: LossBackoff,
    /// `true` for a stock byte-stream connection (no frame delivery).  Gentle
    /// mode's deeper drain exists to keep a bulk flow from driving a shared
    /// droptail so hard that interactive cross-traffic is tail-dropped.  On the
    /// dedicated byte-stream bulk lane there is no other consumer of this
    /// connection's queue, and the deep cut drains a fat pipe below the link
    /// rate long enough to idle it, so the bulk lane drains at the ordinary
    /// fraction instead.  Frame-delivery lanes keep the conservative fraction.
    byte_stream: bool,
}

impl CongestionResponse {
    pub(crate) fn new(now: Instant, reorder_tolerant: bool, byte_stream: bool) -> Self {
        Self {
            queue_growth: QueueGrowth::new(now, reorder_tolerant),
            delivery_peak: WindowedDeliveryMax::new(now),
            bandwidth_probe: OrdinaryBandwidthProbe::new(),
            queue_response: QueueResponse::default(),
            loss_backoff: LossBackoff::default(),
            byte_stream,
        }
    }

    pub(crate) fn reset(&mut self, now: Instant) -> Option<GentleExitCause> {
        let gentle_exit = self.queue_growth.reset(now);
        self.delivery_peak = WindowedDeliveryMax::new(now);
        self.bandwidth_probe.reset();
        self.queue_response.reset();
        self.loss_backoff.reset();
        gentle_exit
    }

    pub(crate) fn observe(
        &mut self,
        smooth_rtt: Duration,
        jitter: GateJitter,
        loss_event_rate: Option<f64>,
        delivery_rate: f64,
        now: Instant,
        control_rtt: Duration,
    ) -> CongestionObservation {
        let peak_delivery = self.delivery_peak.update(now, delivery_rate);
        let queue =
            self.queue_growth
                .observe(smooth_rtt, jitter, loss_event_rate, now, control_rtt);
        CongestionObservation {
            floor: queue.floor,
            tolerance: queue.tolerance,
            queue_building: queue.building,
            persistent_for: queue.persistent_for,
            peak_delivery,
            loss_blocks_delay_control: loss_event_rate.map(|loss| loss < CC_DATA_LOSS_RATE)
                == Some(false),
            gentle_exit: queue.gentle_exit,
        }
    }

    pub(crate) fn decide(
        &mut self,
        observation: CongestionObservation,
        input: CongestionInput,
    ) -> CongestionOutcome {
        let path = select_path(
            observation.queue_building,
            observation.persistent_for.is_some(),
            observation.loss_blocks_delay_control,
        );
        if path != ResponsePath::Probe {
            self.queue_growth.clear_gate_open();
        }
        if path != ResponsePath::LossBackoff {
            self.loss_backoff.reset();
        }
        let mut gentle_exit = observation.gentle_exit;
        match path {
            ResponsePath::Probe => {
                self.queue_response.reset();
                match self.queue_growth.probe(
                    input.delivery_rate,
                    input.current_rate,
                    input.control_rtt,
                    input.smooth_rtt,
                    input.now,
                    input.loss_event_rate,
                ) {
                    GentleProbeOutcome::Apply(target) => CongestionOutcome::new(
                        CongestionDecision::Probe { target },
                        Some(ProbeKind::Gentle),
                        gentle_exit,
                    ),
                    GentleProbeOutcome::Exit(cause) => {
                        gentle_exit = gentle_exit.or(Some(cause));
                        self.ordinary_probe(input, gentle_exit)
                    }
                    GentleProbeOutcome::Inactive => self.ordinary_probe(input, gentle_exit),
                }
            }
            ResponsePath::Hold => {
                CongestionOutcome::new(CongestionDecision::Hold, None, gentle_exit)
            }
            ResponsePath::Drain => {
                let decision = self.queue_response.decide_drain(DrainInput {
                    delivery_rate: input.delivery_rate,
                    drain_fraction: self.drain_fraction(),
                    peak_delivery: observation.peak_delivery,
                    current_rate: input.current_rate,
                    minimum_rate: input.minimum_rate,
                    initial_rate: input.initial_rate,
                    loss_event_rate: input.loss_event_rate,
                    persistent_for: observation.persistent_for,
                    control_rtt: input.control_rtt,
                    now: input.now,
                });
                let guard_exit = self.queue_growth.drain_episode_guard(
                    input.smooth_rtt,
                    observation.floor,
                    input.control_rtt,
                    input.now,
                );
                CongestionOutcome::new(decision, None, guard_exit.or(gentle_exit))
            }
            ResponsePath::LossBackoff => CongestionOutcome::new(
                self.loss_backoff.decide(LossBackoffInput {
                    current_rate: input.current_rate,
                    delivery_rate: input.delivery_rate,
                    peak_delivery: observation.peak_delivery,
                    minimum_rate: input.minimum_rate,
                    initial_rate: input.initial_rate,
                    control_rtt: input.control_rtt,
                    now: input.now,
                }),
                None,
                gentle_exit,
            ),
        }
    }

    /// Whether the delay controller is using its conservative high-queue mode.
    pub(crate) fn gentle_mode(&self) -> bool {
        self.queue_growth.gentle_mode()
    }

    /// Gentle mode is actively reducing its send-rate target.
    pub(crate) fn draining(&self) -> bool {
        self.queue_growth.draining()
    }

    /// Smoothed RTT is currently above the controller's queue gate.
    pub(crate) fn queue_building(&self) -> bool {
        self.queue_growth.building()
    }

    /// Whether this connection is the reorder-tolerant interactive lane.
    pub(crate) fn reorder_tolerant(&self) -> bool {
        self.queue_growth.reorder_tolerant()
    }

    /// The stale-peak protection floor is currently limiting a drain.
    pub(crate) fn drain_floor_binding(&self) -> bool {
        self.queue_response.floor_binding()
    }

    /// Ordinary (non-gentle) probe target for a delivery-rate sample.
    pub(crate) fn proposed_probe_rate(delivery_rate: f64, loss_event_rate: Option<f64>) -> f64 {
        OrdinaryBandwidthProbe::proposed_rate(delivery_rate, loss_event_rate)
    }

    /// Ordinary bandwidth-probe decision after the gentle sub-controller
    /// declined or handed control back to normal probing.
    fn ordinary_probe(
        &mut self,
        input: CongestionInput,
        gentle_exit: Option<GentleExitCause>,
    ) -> CongestionOutcome {
        let target = self.bandwidth_probe.target(
            input.current_rate,
            input.delivery_rate,
            input.loss_event_rate,
            input.control_rtt,
            input.now,
        );
        CongestionOutcome::new(
            CongestionDecision::Probe { target },
            Some(ProbeKind::Bandwidth),
            gentle_exit,
        )
    }

    /// The drain fraction for the current mode.  A byte-stream lane always
    /// drains at the ordinary fraction; a frame-delivery lane uses gentle
    /// mode's deeper fraction while gentle mode is active.
    fn drain_fraction(&self) -> f64 {
        if self.byte_stream {
            DRAIN_RATE_FRACTION
        } else {
            self.queue_growth.drain_frac()
        }
    }

    #[cfg(test)]
    pub(crate) fn queue_growth(&mut self) -> &mut QueueGrowth {
        &mut self.queue_growth
    }

    #[cfg(test)]
    pub(crate) fn delivery_peak(&mut self) -> &mut WindowedDeliveryMax {
        &mut self.delivery_peak
    }

    /// Test-only: force the queue_building flag so the retransmission-armor
    /// duplicate-copy suppression gate can be exercised deterministically.
    #[cfg(test)]
    pub(crate) fn set_queue_building_for_test(&mut self, v: bool) {
        self.queue_growth.set_building(v);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::super::gentle::{DRAIN_RATE_FRACTION, GENTLE_DRAIN_FRAC};
    use super::*;
    use crate::traffic_shaping::recovery::rtt_stats::GateJitter;

    /// Drive a sustained low-loss queue so gentle mode enters, then run one
    /// drain decision.  A byte-stream lane must drain at the ordinary fraction
    /// (there is no shared cross-traffic to protect) while a frame-delivery
    /// lane keeps gentle mode's deeper fraction.
    #[test]
    fn byte_stream_lane_drains_at_the_ordinary_fraction_in_gentle_mode() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let floor_smooth = Duration::from_millis(100);
        let queue_smooth = Duration::from_millis(300);
        let jitter = GateJitter::uniform(Duration::from_millis(5));
        let delivery_rate = 1000.0;
        let queue_start = t0 + Duration::from_millis(1);
        let enter_at = t0 + Duration::from_secs(1) + Duration::from_millis(2);

        let mut byte_stream = CongestionResponse::new(t0, false, true);
        let mut frame = CongestionResponse::new(t0, false, false);
        // Establish a low floor, then hold a standing queue above it for the
        // one-second gentle-entry stretch.
        let _ = byte_stream.observe(
            floor_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            t0,
            control_rtt,
        );
        let _ = frame.observe(
            floor_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            t0,
            control_rtt,
        );
        let _ = byte_stream.observe(
            queue_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            queue_start,
            control_rtt,
        );
        let _ = frame.observe(
            queue_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            queue_start,
            control_rtt,
        );
        let byte_stream_obs = byte_stream.observe(
            queue_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            enter_at,
            control_rtt,
        );
        let frame_obs = frame.observe(
            queue_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            enter_at,
            control_rtt,
        );
        assert!(
            byte_stream.gentle_mode(),
            "byte-stream must enter gentle mode"
        );
        assert!(frame.gentle_mode(), "frame must enter gentle mode");

        let input = |now| CongestionInput {
            delivery_rate,
            current_rate: delivery_rate,
            smooth_rtt: queue_smooth,
            control_rtt,
            loss_event_rate: Some(0.0),
            minimum_rate: 1.0,
            initial_rate: 128.0,
            now,
        };
        let byte_stream_out = byte_stream.decide(byte_stream_obs, input(enter_at));
        let frame_out = frame.decide(frame_obs, input(enter_at));
        let (
            CongestionDecision::Drain {
                target: byte_stream_target,
                ..
            },
            CongestionDecision::Drain {
                target: frame_target,
                ..
            },
        ) = (byte_stream_out.decision(), frame_out.decision())
        else {
            panic!("both lanes must take the drain path");
        };
        assert_eq!(byte_stream_target, delivery_rate * DRAIN_RATE_FRACTION);
        assert_eq!(frame_target, delivery_rate * GENTLE_DRAIN_FRAC);
        assert!(
            byte_stream_target > frame_target,
            "the byte-stream drain must be shallower than the frame drain"
        );
    }
}
