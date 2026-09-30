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
use lane::{CongestionLane, GENTLE_DRAIN_FRAC, peak_scaled_probe_base};
use loss_backoff::{LossBackoff, LossBackoffInput};
use queue_response::{DrainInput, QueueResponse};
use shared_path::{claim_armed, reclaim_armed};

mod decision;
pub(crate) mod lane;
mod loss_backoff;
mod queue_response;
mod shared_path;

pub(crate) use decision::{CongestionDecision, CongestionOutcome, ProbeKind};
pub(crate) use loss_backoff::linear_backoff_step;
#[cfg(test)]
pub(crate) use queue_response::DRAIN_FLOOR_PEAK_FRACTION;
pub(crate) use shared_path::SharedPath;

pub(crate) const CC_DATA_LOSS_RATE: f64 = 0.2;

/// Multiplicative-decrease factor of the AIMD law: any sampled loss halves
/// the current send rate.  One authority for the test-only AIMD reference law
/// and the bulk lane's stand-off competing response, so the two cannot drift.
const AIMD_DECREASE_FACTOR: f64 = 0.5;

/// Minimum control-RTT interval between two multiplicative decreases of the
/// AIMD law.
///
/// The sampled loss rate is *windowed*, so one loss event keeps it non-zero
/// for up to two control RTTs; a trigger that fired on every rate sample while
/// the window is non-zero halved the rate dozens of times per event and pinned
/// the reference at its floor (measured: two identical references together
/// delivered 61 % of a shared bottleneck's capacity).  TCP halves about once
/// per loss event — roughly one event per RTT at steady state — so both the
/// reference and the stand-off competing response mirror that cadence.
const AIMD_DECREASE_COOLDOWN_RTTS: u32 = 1;

/// Fraction of its own send rate the bulk lane drains toward while it stands
/// off after the interactive lane resumes.  Half the competing rate is below
/// the bottleneck's serialization rate (so the queue the bulk filled drains
/// instead of standing) while still leaving the lane a live probe rather than
/// yielding the link outright, which would cost M3 goodput on every resume.
const STANDOFF_HOLD_RATE_FRACTION: f64 = 0.5;

/// How long the bulk lane drains below its competing rate after the
/// interactive lane resumes.
///
/// The transition is the mechanism's whole risk: without a hold, the first
/// interactive packets after a pause queue behind the standing queue the bulk
/// built while competing, and the tail spikes.  One control RTT is the latency
/// floor; the drop-tail bottleneck the harness models drains its 128 KiB buffer
/// at 1 MiB/s in ~128 ms, so 300 ms clears a full queue with margin for one
/// scheduling RTT before the lane returns to the delay-first policy.
const STANDOFF_HOLD: Duration = Duration::from_millis(300);

/// One additive-increase / multiplicative-decrease law: an absolute additive
/// increase per control RTT with a multiplicative decrease on a sampled loss.
/// Shared by the test-only AIMD reference and the bulk stand-off competing
/// response so their decrease cadence cannot drift.
#[derive(Debug, Default)]
struct AimdDecrease {
    last_decrease_at: Option<Instant>,
}

impl AimdDecrease {
    /// Whether a rate sample must apply the multiplicative decrease now: loss
    /// is present in the windowed sample and at least one control RTT has
    /// elapsed since the last decrease.  A `None` windowed rate (too few
    /// samples to measure) is not a loss event.
    fn decrease_due(
        &self,
        loss_event_rate: Option<f64>,
        now: Instant,
        control_rtt: Duration,
    ) -> bool {
        if !loss_event_rate.is_some_and(|loss| loss > 0.0) {
            return false;
        }
        let cooldown = control_rtt * AIMD_DECREASE_COOLDOWN_RTTS;
        self.last_decrease_at
            .is_none_or(|last| now.saturating_duration_since(last) >= cooldown)
    }

    /// One AIMD decision: a multiplicative decrease on a sampled loss (paced to
    /// at most one per control RTT), otherwise the absolute additive increase.
    /// The additive step is the shared lane's [`lane::additive_probe_step`] —
    /// an absolute rate, independent of this flow's current share — so two RTP
    /// flows contending for one bottleneck converge to equal rates.  The
    /// decisions reuse the ordinary probe/backoff channels, so the sender
    /// applies them exactly as it applies production delay-first ones.
    fn outcome(
        &mut self,
        probe: &mut OrdinaryBandwidthProbe,
        input: CongestionInput,
    ) -> CongestionOutcome {
        if self.decrease_due(input.loss_event_rate, input.now, input.control_rtt) {
            self.last_decrease_at = Some(input.now);
            let target = (input.current_rate * AIMD_DECREASE_FACTOR).max(input.minimum_rate);
            return CongestionOutcome::new(
                CongestionDecision::LossBackoff {
                    raw: target,
                    floor: target,
                    target,
                },
                None,
                None,
            );
        }
        let step = lane::additive_probe_step(input.control_rtt);
        let target =
            probe.aimd_additive_target(input.current_rate, step, input.control_rtt, input.now);
        CongestionOutcome::new(
            CongestionDecision::Probe { target },
            Some(ProbeKind::Bandwidth),
            None,
        )
    }
}

/// The bulk lane's interactive stand-off: compete with an external loss-based
/// flow only while the interactive lane has been quiet for at least
/// [`crate::cc::STANDOFF_WINDOW`], and drain below the competing rate for
/// [`STANDOFF_HOLD`] whenever it resumes.
///
/// The gate is the *interactive lane's application activity* — its **offer**
/// clock, not its `RttSample` freshness: with the lane quiet (not offering),
/// loss is either a competitor's signal or the bulk's own queue and competing
/// is the right answer either way; with the lane offering, the shipped
/// delay-first policy yields and the hold clears the queue the competing
/// episode built.  An offer is recorded on the write path (and while send-path
/// data is pending), so it does not gap when the lane's packets queue behind
/// the bulk — reading the `RttSample` clock here made a queued lane look idle
/// and let the bulk compete with the very lane it stands off for.  When no CC
/// link is attached the whole mechanism is inert and the connection's policy is
/// byte-identical to the delay-first one.
#[derive(Debug)]
struct Standoff {
    /// The controller's creation (or last reset) time, used as the quiet
    /// clock's origin when no interactive lane has ever offered on the path.
    started: Instant,
    /// Whether the lane is in the quiet-window competing episode.
    competing: bool,
    /// When the interactive lane resumed while competing; the drain hold runs
    /// until this plus [`STANDOFF_HOLD`].
    hold_until: Option<Instant>,
    decrease: AimdDecrease,
}

impl Standoff {
    fn new(now: Instant) -> Self {
        Self {
            started: now,
            competing: false,
            hold_until: None,
            decrease: AimdDecrease::default(),
        }
    }

    fn reset(&mut self, now: Instant) {
        *self = Self::new(now);
    }

    /// One stand-off decision, or `None` to let the shipped delay-first policy
    /// decide.  The gate is `offer_quiet >= STANDOFF_WINDOW`, where
    /// `offer_quiet` is the time since the interactive lane's last application
    /// offer, read *only* from the offer witness: `RttSample` freshness does
    /// not gate it, so a queued lane that is still offering keeps the bulk
    /// yielded.  A window of zero competes unconditionally (the vacuity probe).
    /// The hold is checked before the gate resumes competing, so a resuming
    /// lane's buffer is drained even if the lane has gone quiet again.
    fn decide(
        &mut self,
        probe: &mut OrdinaryBandwidthProbe,
        input: CongestionInput,
    ) -> Option<CongestionOutcome> {
        if !input.standoff_armed {
            self.competing = false;
            self.hold_until = None;
            return None;
        }
        // R1: a fresh payload gates the quiet-window claim on the interactive
        // lane's *genuine* idleness, and a significant loss sample vetoes the
        // claim. With no payload the gate is the offer clock alone, exactly as
        // shipped.
        let claim_permitted = input
            .payload
            .is_none_or(|shared| claim_armed(&shared, input.loss_event_rate));
        let quiet_for = input
            .interactive_quiet
            .unwrap_or_else(|| input.now.saturating_duration_since(self.started));
        if quiet_for >= crate::cc::STANDOFF_WINDOW && claim_permitted {
            self.hold_until = None;
            self.competing = true;
            return Some(self.decrease.outcome(probe, input));
        }
        if self.competing {
            self.competing = false;
            self.hold_until = Some(input.now + STANDOFF_HOLD);
        }
        if let Some(until) = self.hold_until {
            if input.now < until {
                let target =
                    (input.current_rate * STANDOFF_HOLD_RATE_FRACTION).max(input.minimum_rate);
                return Some(CongestionOutcome::new(
                    CongestionDecision::Drain {
                        floor: input.minimum_rate,
                        target,
                    },
                    None,
                    None,
                ));
            }
            self.hold_until = None;
        }
        None
    }
}

/// State of the test-only AIMD reference law: whether the law is selected, and
/// the shared AIMD decrease state.  Kept local to the reference branch so the
/// production delay/loss policy owns no reference state and its path stays
/// byte-identical.
#[derive(Debug, Default)]
struct ReferenceAimd {
    enabled: bool,
    decrease: AimdDecrease,
}

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
    /// Whether this egress path is shared with a live interactive lane (the
    /// egress path's path signal). On a shared path this connection's own delay gate
    /// is authoritative and loss does not suppress it.
    pub(crate) shared_path: bool,
    /// Whether this connection's bulk interactive stand-off is armed: a
    /// cross-lane CC link is attached *and* the hub runs the stand-off (a
    /// test-only hub can disarm it).  When `false` the stand-off is inert and
    /// the connection keeps the pure delay-first policy byte-for-byte.
    pub(crate) standoff_armed: bool,
    /// How long the path's interactive lane has been without an *application
    /// offer*, or `None` when no interactive lane has ever offered on it (see
    /// [`crate::cc::CcSignal::offered_quiet_for`]).  This is the stand-off's
    /// activity witness and **not** `RttSample` freshness: an offer does not
    /// gap when the lane's packets queue.  A connection with no CC link carries
    /// `None` and never consults it.
    pub(crate) interactive_quiet: Option<Duration>,
    /// The aggregated cross-lane payload for this path, or `None` when no live
    /// interactive lane published a fresh one.  `None` (no CC link, no
    /// interactive lane, or a stale payload) leaves every payload-gated rule
    /// skipped, so the decision is byte-for-byte the shipped policy.  A stale
    /// payload is *absent*, never read as zero or as "unchanged".
    pub(crate) payload: Option<SharedPath>,
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
    /// The bulk lane's interactive stand-off.  Inert when no CC link is
    /// attached; for a bulk (`Dedicated`) lane with a link it competes while
    /// the interactive lane is quiet and drains when it resumes.
    standoff: Standoff,
    /// Test-only AIMD reference law state (never selected in production): when
    /// enabled [`Self::decide`] returns the reference's additive-increase /
    /// multiplicative-decrease decision instead of the production delay/loss
    /// policy.  Seeded from the `#[cfg(feature = "testing")]` connect/accept
    /// `reference_aimd` knob; the production default leaves it disabled, so the
    /// ordinary path is byte-identical.
    reference_aimd: ReferenceAimd,
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
            standoff: Standoff::new(now),
            reference_aimd: ReferenceAimd::default(),
        }
    }

    /// Declare the test-only AIMD reference law.  Production never calls this
    /// (the connect/accept knob that sets it is behind `#[cfg(feature =
    /// "testing")]`, default off), so the ordinary path is unchanged.
    pub(crate) fn set_reference_aimd(&mut self, enabled: bool) {
        self.reference_aimd.enabled = enabled;
    }

    pub(crate) fn reset(&mut self, now: Instant) -> Option<GentleExitCause> {
        let gentle_exit = self.queue_growth.reset(now);
        self.delivery_peak = WindowedDeliveryMax::new(now);
        self.bandwidth_probe.reset();
        self.queue_response.reset();
        self.loss_backoff.reset();
        self.standoff.reset(now);
        self.reference_aimd.decrease = AimdDecrease::default();
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

    /// The no-override observation, used by tests and by callers with no
    /// cross-lane floor to attribute. Production goes through
    /// [`Self::observe_with_floor`].
    #[cfg(test)]
    pub(crate) fn observe(
        &mut self,
        smooth_rtt: Duration,
        jitter: GateJitter,
        loss_event_rate: Option<f64>,
        delivery_rate: f64,
        now: Instant,
        control_rtt: Duration,
    ) -> CongestionObservation {
        self.observe_with_floor(
            smooth_rtt,
            None,
            jitter,
            loss_event_rate,
            delivery_rate,
            now,
            control_rtt,
        )
    }

    /// Observe with R3's attributed floor: a fresh cross-lane floor replaces
    /// this flow's own queue-poisoned floor in the queue gate. `None` is exactly
    /// [`Self::observe`].
    #[allow(clippy::too_many_arguments)] // the gate's full observation plus one attributed floor
    pub(crate) fn observe_with_floor(
        &mut self,
        smooth_rtt: Duration,
        floor_override: Option<Duration>,
        jitter: GateJitter,
        loss_event_rate: Option<f64>,
        delivery_rate: f64,
        now: Instant,
        control_rtt: Duration,
    ) -> CongestionObservation {
        let peak_delivery = self.delivery_peak.update(now, delivery_rate);
        let queue = self.queue_growth.observe_with_floor(
            smooth_rtt,
            floor_override,
            jitter,
            loss_event_rate,
            now,
            control_rtt,
        );
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
        if self.reference_aimd.enabled {
            return self.reference_aimd_outcome(input);
        }
        // The bulk (`Dedicated`) lane stands off for our own interactive lane:
        // while that lane has been quiet past the stand-off window it competes
        // with an external loss-based flow on TCP's terms, and when the lane
        // resumes it drains below the competing rate so the queue it built is
        // cleared before the interactive packets traverse it.  Inert without a
        // CC link, so a connection that never joined a path keeps the pure
        // delay-first policy.  An active or within-window sample falls through
        // to that shipped policy.
        if self.lane.stands_off_for_interactive()
            && let Some(outcome) = self.standoff.decide(&mut self.bandwidth_probe, input)
        {
            return outcome;
        }
        // An application-limited sample can only be *another* flow's queue on
        // a shared lane: a dedicated lane has no competing traffic over its
        // queue, so a standing delay there is its own queue and the ordinary
        // drain applies even when the sender is briefly starved.
        let shared_app_limited = self.lane.app_limited_means_cross_traffic(input.app_limited);
        // On a path shared with an interactive lane, this connection's own
        // delay gate is authoritative: loss from a buffer this group is itself
        // filling is not independent evidence, so it must not suppress the
        // delay drain.  The connection's *own* queue observation drives the
        // decision, at its own RTT-sample cadence — the only cross-connection
        // fact needed is that the path is shared.
        let loss_blocks_delay_control = observation.loss_blocks_delay_control && !input.shared_path;
        // R2: a fresh payload whose interactive lane has armed its own drain
        // gate means the queue on this path belongs to this lane; force the
        // delay path and drain at the shared lane's deeper fraction. Guarded by
        // `input.payload`, so a connection with no payload is unchanged.
        let reclaiming = input.payload.is_some_and(|shared| reclaim_armed(&shared));
        let path = if reclaiming {
            ResponsePath::Drain
        } else {
            select_path(
                observation.queue_building,
                observation.persistent_for.is_some(),
                loss_blocks_delay_control,
                shared_app_limited,
            )
        };
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
                let drain_fraction = if reclaiming {
                    GENTLE_DRAIN_FRAC
                } else {
                    self.queue_growth.drain_frac()
                };
                let decision = self.queue_response.decide_drain(DrainInput {
                    delivery_rate: input.delivery_rate,
                    drain_fraction,
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

    /// The test-only AIMD reference law: an absolute additive increase while
    /// no loss is sampled, and a multiplicative decrease once per loss event
    /// (at most one decrease per control RTT, `AIMD_DECREASE_COOLDOWN_RTTS`).
    /// It replaces the delay/loss policy wholesale (the law is never selected
    /// in production).  The decisions reuse the ordinary probe/backoff
    /// channels, so the sender applies them exactly as it applies production
    /// ones: a `Probe` smooths toward the additive target, a `LossBackoff`
    /// steps the send rate down toward the halved target.
    fn reference_aimd_outcome(&mut self, input: CongestionInput) -> CongestionOutcome {
        self.reference_aimd
            .decrease
            .outcome(&mut self.bandwidth_probe, input)
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
        DRAIN_RATE_FRACTION, SHARED_ADDITIVE_PROBE_REFERENCE_RTT, SHARED_ADDITIVE_PROBE_STEP,
        additive_probe_step,
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
            shared_path: false,
            standoff_armed: false,
            interactive_quiet: None,
            payload: None,
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
            shared_path: false,
            standoff_armed: false,
            interactive_quiet: None,
            payload: None,
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
            shared_path: false,
            standoff_armed: false,
            interactive_quiet: None,
            payload: None,
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
            shared_path: false,
            standoff_armed: false,
            interactive_quiet: None,
            payload: None,
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
            shared_path: false,
            standoff_armed: false,
            interactive_quiet: None,
            payload: None,
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

    /// The test-only AIMD reference law must be a genuine AIMD: an absolute
    /// additive increase with no loss, and a multiplicative decrease (half the
    /// current rate) on a sampled loss — at most once per control RTT, so a
    /// windowed loss event cannot halve the rate on every rate sample.
    /// Before this law the "loss-only" reference used the production
    /// rate-match backoff, whose target is `max(delivery, floor)` — not
    /// proportional to the current rate — so two identical reference flows did
    /// not converge.  This pins both responses, the once-per-event cadence, and
    /// the ratio check fails on the old rate-match law because its target
    /// would not equal `current * 0.5`.
    #[test]
    fn reference_aimd_increases_additively_and_halves_on_loss() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let mut reference = CongestionResponse::new(t0, false, CongestionLane::Dedicated);
        reference.set_reference_aimd(true);

        let input = |current, loss, now| CongestionInput {
            delivery_rate: current * 3.0,
            current_rate: current,
            smooth_rtt: control_rtt,
            control_rtt,
            loss_event_rate: loss,
            app_limited: false,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            shared_path: false,
            standoff_armed: false,
            interactive_quiet: None,
            payload: None,
            now,
        };
        let jitter = GateJitter::uniform(Duration::from_millis(1));
        let step = additive_probe_step(control_rtt);

        // No loss: the target is `current + step`, and the deliberately high
        // delivery sample must not leak in as the contention lane's
        // `max(current, delivery) + step` would.
        let current = 500.0;
        let obs = reference.observe(control_rtt, jitter, Some(0.0), current, t0, control_rtt);
        let CongestionDecision::Probe { target } = reference
            .decide(obs, input(current, Some(0.0), t0))
            .decision()
        else {
            panic!("the reference law must probe while no loss is sampled");
        };
        assert_eq!(
            target,
            current + step,
            "the no-loss reference increase must be the absolute additive step, not a \
             delivery-scaled proposal"
        );

        // Loss: the target is exactly half the current rate, independent of the
        // delivery sample.
        let obs = reference.observe(
            control_rtt,
            jitter,
            Some(0.5),
            current,
            t0 + control_rtt,
            control_rtt,
        );
        let CongestionDecision::LossBackoff { target, floor, raw } = reference
            .decide(obs, input(current, Some(0.5), t0 + control_rtt))
            .decision()
        else {
            panic!("the reference law must back off on loss");
        };
        assert_eq!(target, current * 0.5);
        assert_eq!(floor, current * 0.5);
        assert_eq!(raw, current * 0.5);

        // The decrease is a once-per-loss-event coast, not a per-sample one:
        // with the loss window still non-zero inside the same control RTT the
        // law must probe (additive increase), and only a loss sample at or
        // beyond the cooldown may halve again.  This is the vacuity pin for
        // the saturation fix — a per-sample trigger fails it by backing off
        // on the mid-cooldown sample.
        let mid = t0 + control_rtt + control_rtt / 2;
        let obs = reference.observe(control_rtt, jitter, Some(0.5), current, mid, control_rtt);
        let CongestionDecision::Probe { target } = reference
            .decide(obs, input(current, Some(0.5), mid))
            .decision()
        else {
            panic!(
                "a loss sample inside the cooldown must not halve again — the reference \
                 would collapse into its floor as the per-sample trigger did"
            );
        };
        assert_eq!(target, current + step);

        let after = t0 + control_rtt + control_rtt;
        let obs = reference.observe(control_rtt, jitter, Some(0.5), current, after, control_rtt);
        let CongestionDecision::LossBackoff { target, .. } = reference
            .decide(obs, input(current, Some(0.5), after))
            .decision()
        else {
            panic!("a loss sample at the cooldown boundary must halve again");
        };
        assert_eq!(target, current * 0.5);
    }

    /// The bulk stand-off's gate, state machine, and inertness.  A `Dedicated`
    /// lane with a CC link competes only while the interactive lane has not
    /// *offered* for [`crate::cc::STANDOFF_WINDOW`]; while the lane is offering
    /// it drains (the shipped policy), and the transition from competing back
    /// to offering holds the rate below the competing rate for
    /// `STANDOFF_HOLD`.  The gate reads the offer clock only, so a fresh
    /// `shared_path` (`RttSample` freshness) does not hold it shut.  A
    /// connection with no CC link never leaves the shipped path, so this
    /// mechanism is inert for every caller that never joined a path.
    #[test]
    fn bulk_standoff_competes_only_while_the_interactive_lane_is_quiet() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let observation = CongestionObservation {
            floor: control_rtt,
            tolerance: Duration::from_millis(10),
            queue_building: true,
            persistent_for: Some(Duration::from_millis(200)),
            peak_delivery: 1000.0,
            loss_blocks_delay_control: true,
            gentle_exit: None,
        };
        let input = |now, shared_path, quiet: Option<Duration>, standoff_armed| CongestionInput {
            delivery_rate: 1000.0,
            current_rate: 1000.0,
            smooth_rtt: Duration::from_millis(300),
            control_rtt,
            loss_event_rate: Some(0.0),
            app_limited: false,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            shared_path,
            standoff_armed,
            interactive_quiet: quiet,
            payload: None,
            now,
        };
        let quiet = crate::cc::STANDOFF_WINDOW + Duration::from_millis(1);

        // No CC link: the same standing queue with loss takes the shipped
        // loss backoff, never a competing additive probe.
        let mut no_link = CongestionResponse::new(t0, false, CongestionLane::Dedicated);
        assert!(
            matches!(
                no_link
                    .decide(observation, input(t0, false, None, false))
                    .decision(),
                CongestionDecision::LossBackoff { .. }
            ),
            "without a CC link the stand-off must be inert"
        );

        // A fresh offer keeps the shipped drain even on a path whose RTT
        // clock says shared: the gate reads the offer clock, which is what a
        // queued-but-still-offering lane carries.
        let mut active = CongestionResponse::new(t0, false, CongestionLane::Dedicated);
        assert!(
            matches!(
                active
                    .decide(observation, input(t0, true, Some(Duration::ZERO), true))
                    .decision(),
                CongestionDecision::Drain { .. }
            ),
            "a fresh offer must keep the shipped drain"
        );
        // An *old* offer with a fresh RTT clock (`shared_path` true) must
        // compete: `RttSample` freshness is not the gate, so a lane whose
        // packets queued and whose samples stopped cannot be mistaken for an
        // idle one in the dangerous direction.
        let mut stale_offer = CongestionResponse::new(t0, false, CongestionLane::Dedicated);
        assert!(
            matches!(
                stale_offer
                    .decide(observation, input(t0, true, Some(quiet), true))
                    .decision(),
                CongestionDecision::Probe { .. }
            ),
            "an old offer must let the bulk compete even with a fresh RTT clock"
        );

        // Quiet past the window with a link: compete additively.
        let mut competing = CongestionResponse::new(t0, false, CongestionLane::Dedicated);
        let CongestionDecision::Probe { target } = competing
            .decide(observation, input(t0, false, Some(quiet), true))
            .decision()
        else {
            panic!("a quiet interactive lane must let the bulk lane compete");
        };
        assert_eq!(target, 1000.0 + additive_probe_step(control_rtt));

        // The lane resumes: the bulk must hold below its competing rate, not
        // fall straight back into the shipped policy at full rate.
        let resumed = t0 + Duration::from_millis(50);
        let CongestionDecision::Drain { target, .. } = competing
            .decide(
                observation,
                input(resumed, true, Some(Duration::ZERO), true),
            )
            .decision()
        else {
            panic!("a resuming interactive lane must trigger the hold drain");
        };
        assert_eq!(
            target,
            1000.0 * STANDOFF_HOLD_RATE_FRACTION,
            "the hold must drain well below the competing rate"
        );

        // The hold expires: the shipped delay-first policy resumes.
        let settled = resumed + STANDOFF_HOLD + Duration::from_millis(1);
        assert!(
            matches!(
                competing
                    .decide(
                        observation,
                        input(settled, true, Some(Duration::ZERO), true)
                    )
                    .decision(),
                CongestionDecision::Drain { .. }
            ),
            "after the hold the shipped delay-first policy must run"
        );
    }

    /// On a path shared with an interactive lane, this connection's *own*
    /// standing queue must drive the delay drain even when loss-based control
    /// would otherwise win.
    #[test]
    fn a_shared_path_drains_despite_loss() {
        let now = Instant::now();
        let control_rtt = Duration::from_millis(100);
        // This connection's own queue is standing (RTT above the floor) and
        // the path carries loss.
        let observation = CongestionObservation {
            floor: control_rtt,
            tolerance: Duration::from_millis(10),
            queue_building: true,
            persistent_for: Some(Duration::from_millis(200)),
            peak_delivery: 1000.0,
            loss_blocks_delay_control: true,
            gentle_exit: None,
        };
        let input = |shared_path| CongestionInput {
            delivery_rate: 1000.0,
            current_rate: 1000.0,
            smooth_rtt: Duration::from_millis(300),
            control_rtt,
            loss_event_rate: Some(0.5),
            app_limited: false,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            shared_path,
            standoff_armed: false,
            interactive_quiet: None,
            payload: None,
            now,
        };
        let mut own = CongestionResponse::new(now, false, CongestionLane::Shared);
        assert!(
            matches!(
                own.decide(observation, input(false)).decision(),
                CongestionDecision::LossBackoff { .. }
            ),
            "with loss and no shared path the loss backoff must win"
        );
        let mut shared = CongestionResponse::new(now, false, CongestionLane::Shared);
        assert!(
            matches!(
                shared.decide(observation, input(true)).decision(),
                CongestionDecision::Drain { .. }
            ),
            "on a shared path the connection's own standing queue must drain despite loss"
        );
    }

    /// The absent-payload path is the shipped policy.  A controller with no
    /// fresh cross-lane payload decides exactly as one whose payload is the
    /// *neutral* element (an idle lane, no latch, no floor), so the guard -- not
    /// the rule -- is what keeps the absent case unchanged.  A non-neutral
    /// payload does change the decision, proving the rule is live and the
    /// equivalence above is not an unguarded no-op.
    #[test]
    fn the_absent_payload_path_is_the_shipped_policy() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let quiet = crate::cc::STANDOFF_WINDOW + Duration::from_millis(1);
        // The bulk's own queue is persistent, so the shipped policy's fallback
        // is `Drain`; the stand-off would instead claim with a `Probe`.
        let observation = CongestionObservation {
            floor: control_rtt,
            tolerance: Duration::from_millis(10),
            queue_building: true,
            persistent_for: Some(Duration::from_millis(200)),
            peak_delivery: 1000.0,
            loss_blocks_delay_control: false,
            gentle_exit: None,
        };
        let input = |payload| CongestionInput {
            delivery_rate: 1000.0,
            current_rate: 1000.0,
            smooth_rtt: Duration::from_millis(100),
            control_rtt,
            loss_event_rate: Some(0.0),
            app_limited: false,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            shared_path: true,
            standoff_armed: true,
            interactive_quiet: Some(quiet),
            payload,
            now: t0,
        };
        let neutral = SharedPath {
            pending_bytes: 0,
            write_waiters: 0,
            offered_pps: 0.0,
            control_rtt: Some(control_rtt),
            reclaiming: false,
            floor: None,
            tolerance: None,
            queue_delay: None,
        };
        let absent = CongestionResponse::new(t0, false, CongestionLane::Dedicated)
            .decide(observation, input(None))
            .decision();
        let neutral_out = CongestionResponse::new(t0, false, CongestionLane::Dedicated)
            .decide(observation, input(Some(neutral)))
            .decision();
        assert_eq!(
            absent, neutral_out,
            "the absent payload must decide as the neutral payload"
        );
        // A busy lane blocks the claim the absent payload permits, so the
        // absent case is a real fallback and not one arm of an unguarded rule.
        let mut busy = neutral;
        busy.pending_bytes = 1;
        let busy_out = CongestionResponse::new(t0, false, CongestionLane::Dedicated)
            .decide(observation, input(Some(busy)))
            .decision();
        assert_ne!(
            absent, busy_out,
            "a busy payload must block the claim the absent payload permits"
        );
        assert!(
            matches!(busy_out, CongestionDecision::Drain { .. }),
            "with the claim blocked the shipped delay-first policy must run: {busy_out:?}"
        );
    }

    /// R2: the interactive lane's own latch forces the bulk's reclaim drain
    /// even when the bulk's own queue is not persistent, and it uses the shared
    /// lane's deeper `GENTLE_DRAIN_FRAC`; without the payload the shipped probe
    /// runs instead.
    #[test]
    fn a_latched_interactive_lane_forces_the_bulk_reclaim_drain() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let observation = CongestionObservation {
            floor: control_rtt,
            tolerance: Duration::from_millis(10),
            queue_building: false,
            persistent_for: None,
            peak_delivery: 1000.0,
            loss_blocks_delay_control: false,
            gentle_exit: None,
        };
        let input = |payload| CongestionInput {
            delivery_rate: 1000.0,
            current_rate: 1000.0,
            smooth_rtt: Duration::from_millis(100),
            control_rtt,
            loss_event_rate: Some(0.0),
            app_limited: false,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            shared_path: true,
            standoff_armed: false,
            interactive_quiet: None,
            payload,
            now: t0,
        };
        let latched = SharedPath {
            pending_bytes: 0,
            write_waiters: 0,
            offered_pps: 0.0,
            control_rtt: Some(control_rtt),
            reclaiming: true,
            floor: Some(control_rtt),
            tolerance: Some(Duration::from_millis(10)),
            queue_delay: Some(Duration::from_millis(3)),
        };
        let reclaimed = CongestionResponse::new(t0, false, CongestionLane::Dedicated)
            .decide(observation, input(Some(latched)))
            .decision();
        let CongestionDecision::Drain { target, .. } = reclaimed else {
            panic!("the interactive lane's latch must force a reclaim drain: {reclaimed:?}");
        };
        assert_eq!(target, 1000.0 * GENTLE_DRAIN_FRAC);
        let absent = CongestionResponse::new(t0, false, CongestionLane::Dedicated)
            .decide(observation, input(None))
            .decision();
        assert!(
            matches!(absent, CongestionDecision::Probe { .. }),
            "without the payload the bulk's own (empty) queue must probe: {absent:?}"
        );
    }
}
