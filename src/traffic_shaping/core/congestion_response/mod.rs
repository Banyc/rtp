//! Encapsulated congestion-response policy.
//!
//! One owner (`CongestionResponse`) holds queue detection, delivered-peak
//! tracking, ordinary probing, and drain-floor hysteresis together so their
//! resets and episode continuity stay atomic at the policy level.  Precedence
//! is probe, transient queue hold, persistent delay drain, then loss backoff;
//! a loss sample blocks every delay-control branch.
use std::time::{Duration, Instant};

use super::gentle::{GentleExitCause, GentleProbeOutcome};
use super::{OrdinaryBandwidthProbe, QueueGrowth, WindowedDeliveryMax};
use decision::{ResponsePath, select_path};
use queue_response::{DrainInput, QueueResponse};

mod decision;
mod loss_backoff;
pub(crate) mod queue_response;

pub(crate) use decision::{CongestionDecision, CongestionOutcome, ProbeKind};
pub(crate) use loss_backoff::linear_backoff_step;

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
}

impl CongestionResponse {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            queue_growth: QueueGrowth::new(now),
            delivery_peak: WindowedDeliveryMax::new(now),
            bandwidth_probe: OrdinaryBandwidthProbe::new(),
            queue_response: QueueResponse::default(),
        }
    }

    pub(crate) fn reset(&mut self, now: Instant) -> Option<GentleExitCause> {
        let gentle_exit = self.queue_growth.reset(now);
        self.delivery_peak = WindowedDeliveryMax::new(now);
        self.bandwidth_probe.reset();
        self.queue_response.reset();
        gentle_exit
    }

    pub(crate) fn observe(
        &mut self,
        smooth_rtt: Duration,
        rtt_var: Duration,
        loss_event_rate: Option<f64>,
        delivery_rate: f64,
        now: Instant,
        control_rtt: Duration,
    ) -> CongestionObservation {
        let peak_delivery = self.delivery_peak.update(now, delivery_rate);
        let queue =
            self.queue_growth
                .observe(smooth_rtt, rtt_var, loss_event_rate, now, control_rtt);
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
                    drain_fraction: self.queue_growth.drain_frac(),
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
                loss_backoff::decide(
                    input.current_rate,
                    input.delivery_rate,
                    observation.peak_delivery,
                    input.minimum_rate,
                    input.initial_rate,
                ),
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

    /// The stale-peak protection floor is currently limiting a drain.
    pub(crate) fn drain_floor_binding(&self) -> bool {
        self.queue_response.floor_binding()
    }

    /// Ordinary (non-gentle) probe target for a delivery-rate sample.
    pub(crate) fn proposed_probe_rate(delivery_rate: f64) -> f64 {
        OrdinaryBandwidthProbe::proposed_rate(delivery_rate)
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
            input.control_rtt,
            input.now,
        );
        CongestionOutcome::new(
            CongestionDecision::Probe { target },
            Some(ProbeKind::Bandwidth),
            gentle_exit,
        )
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
