//! Encapsulated congestion-response policy.
//!
//! One owner (`CongestionResponse`) holds queue detection, delivered-peak
//! tracking, ordinary probing, and drain-floor hysteresis together so their
//! resets and episode continuity stay atomic at the policy level.  Precedence
//! is probe, transient queue hold, persistent delay drain, then loss backoff;
//! a loss sample blocks every delay-control branch.
use std::time::{Duration, Instant};

use super::gentle::{GentleExitCause, GentleMode, GentleProbeOutcome};
use super::{OrdinaryBandwidthProbe, ProbeIncrease, QueueGrowth, WindowedDeliveryMax};
use crate::traffic_shaping::recovery::reorder_tolerance::cap_probe_target;
use crate::traffic_shaping::recovery::rtt_stats::GateJitter;
use decision::{ResponsePath, select_path};
use lane::{CongestionLane, peak_scaled_probe_base};
use loss_backoff::{LossBackoff, LossBackoffInput};
use queue_response::{DrainInput, QueueResponse};

mod decision;
pub(crate) mod lane;
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
    /// Whether the transport flagged this rate sample as application-limited:
    /// the sender had less queued than the pipe could carry.  Consulted only
    /// for a shared lane, where it means a standing delay cannot be attributed
    /// to this lane's own queue.
    pub(crate) app_limited: bool,
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
    /// The connection's declared congestion lane.  A [`CongestionLane::Dedicated`]
    /// lane has no competing traffic over this connection's queue, so it drains
    /// at the ordinary fraction and creeps toward capacity at the gentler probe
    /// gain; a [`CongestionLane::Shared`] lane keeps the conservative
    /// cross-traffic-protecting tuning.  Declared by the owner (e.g. `rtp_mux`'s
    /// lane class), never inferred from the delivery mode.
    lane: CongestionLane,
}

impl CongestionResponse {
    pub(crate) fn new(now: Instant, reorder_tolerant: bool, lane: CongestionLane) -> Self {
        let mut queue_growth = QueueGrowth::new(now, reorder_tolerant);
        queue_growth.set_lane(lane);
        Self {
            queue_growth,
            delivery_peak: WindowedDeliveryMax::new(now),
            bandwidth_probe: OrdinaryBandwidthProbe::new(),
            queue_response: QueueResponse::default(),
            loss_backoff: LossBackoff::default(),
            lane,
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

    /// Discard the tracked recent delivery peak without touching the rest of
    /// the controller.  Used when a dedicated fast-start episode ends: a burst
    /// of ACKs during the ramp can report a delivery rate above the path's
    /// sustained capacity, and the gentle probe would otherwise use that
    /// inflated peak as its creep base.
    pub(crate) fn clear_delivery_peak(&mut self, now: Instant) {
        self.delivery_peak = WindowedDeliveryMax::new(now);
    }

    /// The recent windowed delivery peak, if any sample has been observed.
    /// Unlike the live send rate this is a windowed maximum of measured
    /// delivery, so it tracks the path's own timescale and is not depressed
    /// by the controller's own momentary drain.
    pub(crate) fn delivery_peak_rate(&self) -> Option<f64> {
        self.delivery_peak.peek()
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
        // An application-limited sample can only be *another* flow's queue on
        // a shared lane: a dedicated lane has no competing traffic over its
        // queue, so a standing delay there is its own queue and the ordinary
        // drain applies even when the sender is briefly starved.
        let shared_app_limited = self.lane.app_limited_means_cross_traffic(input.app_limited);
        let path = select_path(
            observation.queue_building,
            observation.persistent_for.is_some(),
            observation.loss_blocks_delay_control,
            shared_app_limited,
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
                let probe_base =
                    peak_scaled_probe_base(input.delivery_rate, observation.peak_delivery);
                match self.queue_growth.probe(
                    probe_base,
                    input.current_rate,
                    input.control_rtt,
                    input.smooth_rtt,
                    input.now,
                    input.loss_event_rate,
                ) {
                    GentleProbeOutcome::Apply(target) => {
                        let target = self.cap_reorder_probe(
                            input.current_rate,
                            target,
                            GentleMode::probe_additive(input.control_rtt),
                        );
                        CongestionOutcome::new(
                            CongestionDecision::Probe { target },
                            Some(ProbeKind::Gentle),
                            gentle_exit,
                        )
                    }
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

    /// Bound a probe target on the reorder-tolerant lane.
    ///
    /// The cap exists because a reorder-inflated delivery sample could
    /// otherwise spike the probe several-fold in one control RTT.  It bounds
    /// only the *delivery-scaled* part of the target; a lane's additive step is
    /// a fixed RTT-scaled rate, not derived from the delivery sample, so it is
    /// added after the bound instead of being clipped by it.  On a
    /// non-reorder-tolerant lane the target is returned unchanged.
    fn cap_reorder_probe(&self, current: f64, target: f64, additive: f64) -> f64 {
        cap_probe_target(self.reorder_tolerant(), current, target, additive)
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

    /// The connection's declared congestion lane.
    pub(crate) fn lane(&self) -> CongestionLane {
        self.lane
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
        // A sparse, application-limited flow has no competing flow to converge
        // against and its delivery sample reflects the application's sending,
        // not the path's capacity; adding headroom on such a sample would
        // ratchet a periodic flow's rate without bound.  Only a genuinely
        // backlogged shared flow gets the additive step.
        let additive = if input.app_limited {
            0.0
        } else {
            self.lane.ordinary_additive_probe_step(input.control_rtt)
        };
        // A lane with an active additive step probes additively: the step is
        // an absolute rate, so two flows contending for one bottleneck converge
        // to equal rates instead of keeping whatever ratio the multiplicative
        // probe first gave them.  Every other case (a dedicated lane, whose step
        // is zero, or an application-limited sample, whose delivery reflects the
        // application) keeps the historical delivery-scaled multiplicative
        // probe.
        let increase = if additive > 0.0 {
            ProbeIncrease::Additive
        } else {
            ProbeIncrease::Multiplicative
        };
        let target = self.bandwidth_probe.target(
            input.current_rate,
            input.delivery_rate,
            input.loss_event_rate,
            input.control_rtt,
            increase,
            additive,
            input.now,
        );
        let target = self.cap_reorder_probe(input.current_rate, target, additive);
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

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::lane::{
        DRAIN_RATE_FRACTION, GENTLE_DRAIN_FRAC, SHARED_ADDITIVE_PROBE_REFERENCE_RTT,
        SHARED_ADDITIVE_PROBE_STEP,
    };
    use super::*;
    use crate::traffic_shaping::recovery::rtt_stats::GateJitter;

    /// Drive a sustained low-loss queue so gentle mode enters, then run one
    /// drain decision.  A `Dedicated` lane must drain at the ordinary fraction
    /// (there is no shared cross-traffic to protect) while a `Shared` lane
    /// keeps gentle mode's deeper fraction.
    #[test]
    fn dedicated_lane_drains_at_the_ordinary_fraction_in_gentle_mode() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let floor_smooth = Duration::from_millis(100);
        let queue_smooth = Duration::from_millis(300);
        let jitter = GateJitter::uniform(Duration::from_millis(5));
        let delivery_rate = 1000.0;
        let queue_start = t0 + Duration::from_millis(1);
        let enter_at = t0 + Duration::from_secs(1) + Duration::from_millis(2);

        let mut dedicated = CongestionResponse::new(t0, false, CongestionLane::Dedicated);
        let mut shared = CongestionResponse::new(t0, false, CongestionLane::Shared);
        // Establish a low floor, then hold a standing queue above it for the
        // one-second gentle-entry stretch.
        let _ = dedicated.observe(
            floor_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            t0,
            control_rtt,
        );
        let _ = shared.observe(
            floor_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            t0,
            control_rtt,
        );
        // A standing queue produces a rate sample every control RTT, not one
        // observation straddling the whole entry stretch: feed the queue at
        // the cadence a backlogged lane would, so the persistent-queue timer
        // accumulates continuously (an observation gap as long as the entry
        // stretch legitimately breaks the episode and restarts the timer).
        let mut t = queue_start;
        while t < enter_at {
            let _ = dedicated.observe(
                queue_smooth,
                jitter,
                Some(0.0),
                delivery_rate,
                t,
                control_rtt,
            );
            let _ = shared.observe(
                queue_smooth,
                jitter,
                Some(0.0),
                delivery_rate,
                t,
                control_rtt,
            );
            t += control_rtt;
        }
        let dedicated_obs = dedicated.observe(
            queue_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            enter_at,
            control_rtt,
        );
        let shared_obs = shared.observe(
            queue_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            enter_at,
            control_rtt,
        );
        assert!(
            dedicated.gentle_mode(),
            "the dedicated lane must enter gentle mode"
        );
        assert!(
            shared.gentle_mode(),
            "the shared lane must enter gentle mode"
        );

        let input = |now| CongestionInput {
            delivery_rate,
            current_rate: delivery_rate,
            smooth_rtt: queue_smooth,
            control_rtt,
            loss_event_rate: Some(0.0),
            app_limited: false,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            now,
        };
        let dedicated_out = dedicated.decide(dedicated_obs, input(enter_at));
        let shared_out = shared.decide(shared_obs, input(enter_at));
        let (
            CongestionDecision::Drain {
                target: dedicated_target,
                ..
            },
            CongestionDecision::Drain {
                target: shared_target,
                ..
            },
        ) = (dedicated_out.decision(), shared_out.decision())
        else {
            panic!("both lanes must take the drain path");
        };
        assert_eq!(dedicated_target, delivery_rate * DRAIN_RATE_FRACTION);
        assert_eq!(shared_target, delivery_rate * GENTLE_DRAIN_FRAC);
        assert!(
            dedicated_target > shared_target,
            "the dedicated drain must be shallower than the shared drain"
        );
    }

    /// After a drain the instantaneous delivery sample is depressed by the
    /// controller's own drain, but the recent delivery peak still remembers the
    /// established capacity.  The gentle probe scales from the peak on EVERY
    /// lane (queue-depth-neutral), while the gain stays lane-specific: the
    /// `Dedicated` lane creeps at `1.02x`, the `Shared` lane keeps the
    /// historical `1.2x`.
    #[test]
    fn gentle_probe_scales_from_the_peak_with_a_lane_specific_gain() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let floor = Duration::from_millis(100);
        let queued = Duration::from_millis(300);
        let jitter = GateJitter::uniform(Duration::from_millis(5));
        let enter_at = t0 + Duration::from_secs(1) + Duration::from_millis(2);
        let probe_at = enter_at + Duration::from_millis(10);
        let established_peak = 1000.0;
        let depressed = 200.0;

        let mut dedicated = CongestionResponse::new(t0, false, CongestionLane::Dedicated);
        let mut shared = CongestionResponse::new(t0, false, CongestionLane::Shared);
        for c in [&mut dedicated, &mut shared] {
            let _ = c.observe(floor, jitter, Some(0.0), established_peak, t0, control_rtt);
            let _ = c.observe(
                queued,
                jitter,
                Some(0.0),
                established_peak,
                t0 + Duration::from_millis(1),
                control_rtt,
            );
            // Backlogged cadence: one observation per control RTT (see the
            // drain-fraction test), so the persistent-queue timer accumulates
            // continuously and gentle mode enters before `enter_at`.
            let mut t = t0 + Duration::from_millis(1) + control_rtt;
            while t < enter_at {
                let _ = c.observe(queued, jitter, Some(0.0), established_peak, t, control_rtt);
                t += control_rtt;
            }
            let _ = c.observe(
                queued,
                jitter,
                Some(0.0),
                established_peak,
                enter_at,
                control_rtt,
            );
        }
        assert!(
            dedicated.gentle_mode() && shared.gentle_mode(),
            "both lanes must enter gentle mode on the standing queue"
        );

        // The queue has drained (gate open) and the latest delivery sample is
        // far below the established peak.
        let dedicated_obs =
            dedicated.observe(floor, jitter, Some(0.0), depressed, probe_at, control_rtt);
        let shared_obs = shared.observe(floor, jitter, Some(0.0), depressed, probe_at, control_rtt);
        let input = CongestionInput {
            delivery_rate: depressed,
            current_rate: 100.0,
            smooth_rtt: floor,
            control_rtt,
            loss_event_rate: Some(0.0),
            app_limited: false,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            now: probe_at,
        };
        let CongestionDecision::Probe {
            target: dedicated_target,
        } = dedicated.decide(dedicated_obs, input).decision()
        else {
            panic!("the dedicated lane must take the probe path");
        };
        let CongestionDecision::Probe {
            target: shared_target,
        } = shared.decide(shared_obs, input).decision()
        else {
            panic!("the shared lane must take the probe path");
        };
        assert!(
            dedicated_target >= established_peak * 1.02,
            "the dedicated probe must scale from the peak: {dedicated_target}"
        );
        assert!(
            dedicated_target < established_peak * 1.2,
            "the dedicated probe must use the gentler gain: {dedicated_target}"
        );
        assert!(
            shared_target >= established_peak * 1.2,
            "the shared probe must also scale from the peak, with the historical gain: {shared_target}"
        );
    }

    /// A gentle-mode drain episode's "ineffective drain" timer must not
    /// accumulate across an idle gap.  The guard leaves gentle mode (and
    /// blocks re-entry for the cooldown) after twelve control RTTs of a drain
    /// that has not shrunk the queue gap, but a lane that went quiet during
    /// the episode was not draining at all: the first post-idle drain must
    /// restart the measurement instead of minting a spurious exit and
    /// cooldown on the strength of the idle stretch.
    #[test]
    fn gentle_drain_episode_does_not_span_an_idle_gap() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let floor_smooth = Duration::from_millis(100);
        let queue_smooth = Duration::from_millis(300);
        let jitter = GateJitter::uniform(Duration::from_millis(5));
        let delivery_rate = 1000.0;
        let input = |now| CongestionInput {
            delivery_rate,
            current_rate: delivery_rate,
            smooth_rtt: queue_smooth,
            control_rtt,
            loss_event_rate: Some(0.0),
            app_limited: false,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            now,
        };

        let mut c = CongestionResponse::new(t0, false, CongestionLane::Shared);
        let _ = c.observe(
            floor_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            t0,
            control_rtt,
        );
        // Backlogged cadence: one queue sample per control RTT so gentle mode
        // enters on the sustained standing queue.
        let enter_at = t0 + Duration::from_secs(1) + Duration::from_millis(2);
        let mut t = t0 + Duration::from_millis(1);
        while t < enter_at {
            let obs = c.observe(
                queue_smooth,
                jitter,
                Some(0.0),
                delivery_rate,
                t,
                control_rtt,
            );
            let _ = c.decide(obs, input(t));
            t += control_rtt;
        }
        let obs = c.observe(
            queue_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            enter_at,
            control_rtt,
        );
        let _ = c.decide(obs, input(enter_at));
        assert!(c.gentle_mode(), "the standing queue must enter gentle mode");
        assert!(
            c.queue_growth().drain_episode().is_some(),
            "the gentle drain must record an episode"
        );

        // The lane goes quiet far past the twelve-control-RTT drain check, then
        // resumes with the same standing queue.
        let resumed = enter_at + Duration::from_secs(8);
        let obs = c.observe(
            queue_smooth,
            jitter,
            Some(0.0),
            delivery_rate,
            resumed,
            control_rtt,
        );
        let decision = c.decide(obs, input(resumed)).decision();
        assert!(
            matches!(decision, CongestionDecision::Drain { .. }),
            "the post-idle standing queue must still drain"
        );
        assert!(
            c.gentle_mode(),
            "a drain episode that merely idled must not exit gentle mode"
        );
        assert!(
            c.queue_growth().gentle_block_until().is_none(),
            "no ineffective-drain cooldown may be minted across an idle gap"
        );
    }

    /// The shared lane adds an absolute additive step to an accepted ordinary
    /// probe.  The step is scaled by the square root of the path's control RTT:
    /// the shared queue, not each flow's own RTT, gates the accepted-probe
    /// cadence, so a full linear compensation over-rewards the high-RTT flow.
    /// A dedicated lane keeps the purely multiplicative probe.
    #[test]
    fn shared_lane_ordinary_probe_adds_a_sqrt_rtt_scaled_absolute_step() {
        let reference = SHARED_ADDITIVE_PROBE_REFERENCE_RTT;
        assert_eq!(
            CongestionLane::Shared.ordinary_additive_probe_step(reference),
            SHARED_ADDITIVE_PROBE_STEP,
            "the step is calibrated at the reference RTT"
        );
        let double = reference * 2;
        assert_eq!(
            CongestionLane::Shared.ordinary_additive_probe_step(double),
            SHARED_ADDITIVE_PROBE_STEP * 2f64.sqrt(),
            "the RTT compensation is sub-linear"
        );
        assert_eq!(
            CongestionLane::Dedicated.ordinary_additive_probe_step(reference),
            0.0,
            "the dedicated lane must keep a purely multiplicative probe"
        );
    }

    /// A reorder-tolerant shared lane still gets the additive step: the
    /// reorder probe cap bounds only the delivery-scaled part of the target, so
    /// it must not clip the lane's absolute additive headroom.
    #[test]
    fn reorder_probe_cap_does_not_clip_the_lane_additive_step() {
        use crate::traffic_shaping::recovery::reorder_tolerance::cap_probe_target;
        let current = 100.0;
        let additive = 400.0;
        let target = current + additive;
        assert_eq!(
            cap_probe_target(true, current, target, additive),
            target,
            "the additive step must survive the reorder cap"
        );
        assert_eq!(
            cap_probe_target(false, current, target, additive),
            target,
            "the stock/bulk lane is uncapped"
        );
    }

    /// A shared non-reorder lane and a shared reorder lane differ only in the
    /// cap: both keep the additive step.  This pins the fix for the conflict
    /// where the reorder cap used to cancel the additive contribution entirely.
    #[test]
    fn shared_additive_step_survives_on_a_reorder_tolerant_lane() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(50);
        let jitter = GateJitter::uniform(Duration::from_millis(1));
        let current = 100.0;
        let delivery = 90.0;
        let input = CongestionInput {
            delivery_rate: delivery,
            current_rate: current,
            smooth_rtt: control_rtt,
            control_rtt,
            loss_event_rate: Some(0.0),
            app_limited: false,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            now: t0,
        };
        let mut stock = CongestionResponse::new(t0, false, CongestionLane::Shared);
        let mut reorder = CongestionResponse::new(t0, true, CongestionLane::Shared);
        let stock_obs = stock.observe(control_rtt, jitter, Some(0.0), delivery, t0, control_rtt);
        let reorder_obs =
            reorder.observe(control_rtt, jitter, Some(0.0), delivery, t0, control_rtt);
        let CongestionDecision::Probe {
            target: stock_target,
        } = stock.decide(stock_obs, input).decision()
        else {
            panic!("the clean shared lane must probe");
        };
        let CongestionDecision::Probe {
            target: reorder_target,
        } = reorder.decide(reorder_obs, input).decision()
        else {
            panic!("the clean reorder shared lane must probe");
        };
        // Both lanes add the same square-root-RTT-scaled absolute step, so both
        // target the same value; the reorder cap (1.5 * current, plus the
        // additive step) does not bind here because the additive target already
        // dominates.
        let expected = current + CongestionLane::Shared.ordinary_additive_probe_step(control_rtt);
        assert!((stock_target - expected).abs() < 1e-9);
        assert!((reorder_target - expected).abs() < 1e-9);
    }

    /// An application-limited sample can only be attributed to a *competing*
    /// flow's standing queue on a shared lane; a dedicated lane's queue is its
    /// own, so the ordinary drain still applies.  The path selector is tested
    /// with the flag pre-computed (`response_path_encodes_the_controller_
    /// precedence`), which leaves the lane->flag mapping itself unpinned;
    /// this drives the real `decide` call site with a standing queue and an
    /// application-limited sample on each lane.
    #[test]
    fn app_limited_attribution_is_shared_lane_only_at_the_decide_call_site() {
        let now = Instant::now();
        // A standing, persistent queue with no loss block.
        let observation = CongestionObservation {
            floor: Duration::from_millis(100),
            tolerance: Duration::from_millis(20),
            queue_building: true,
            persistent_for: Some(Duration::from_secs(1)),
            peak_delivery: 1000.0,
            loss_blocks_delay_control: false,
            gentle_exit: None,
        };
        let input = CongestionInput {
            delivery_rate: 1000.0,
            current_rate: 1000.0,
            smooth_rtt: Duration::from_millis(300),
            control_rtt: Duration::from_millis(100),
            loss_event_rate: Some(0.0),
            app_limited: true,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            now,
        };

        let mut shared = CongestionResponse::new(now, false, CongestionLane::Shared);
        assert!(
            matches!(
                shared.decide(observation, input).decision(),
                CongestionDecision::Probe { .. }
            ),
            "an application-limited sample on a shared lane must probe: the standing delay belongs to a competing flow, not this (empty) queue"
        );

        let mut dedicated = CongestionResponse::new(now, false, CongestionLane::Dedicated);
        assert!(
            matches!(
                dedicated.decide(observation, input).decision(),
                CongestionDecision::Drain { .. }
            ),
            "the same application-limited sample on a dedicated lane must drain: the standing queue is its own"
        );
    }
}
