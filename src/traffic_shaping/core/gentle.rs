use std::time::{Duration, Instant};

use super::bandwidth_probe::loss_scaled_gain;

// Gentle-mode parameters for the delay-gated congestion controller.  These are
// intentionally conservative: they let a bulk flow drain a self-inflicted
// droptail bottleneck without driving the delay gate so hard that interactive
// cross-traffic keeps getting tail-dropped.
pub(crate) const GENTLE_BW_PROBE_GAIN: f64 = 0.20;

pub(crate) const GENTLE_DRAIN_FRAC: f64 = 0.75;

pub(crate) const GENTLE_ADD_PKTS: f64 = 4.0;
pub(crate) const GENTLE_ENTER_RTTS: f64 = 3.0;
pub(crate) const GENTLE_ENTER_MIN: Duration = Duration::from_secs(1);
pub(crate) const GENTLE_ENTER_MAX_LOSS: f64 = 0.05;
pub(crate) const GENTLE_EXIT_LOSS: f64 = 0.1;
pub(crate) const GENTLE_DRAIN_CHECK_RTTS: f64 = 12.0;
pub(crate) const GENTLE_DRAIN_GAP_SHRINK: f64 = 0.85;

pub(crate) const GENTLE_REENTRY_COOLDOWN: Duration = Duration::from_secs(15);
pub(crate) const DRAIN_RATE_FRACTION: f64 = 0.9;

/// Exact controller transition that ended one gentle-mode episode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GentleExitCause {
    Loss,
    GateOpen,
    DrainGuard,
    OutageReset,
}

/// Result of evaluating the clean-link probe branch.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum GentleProbeOutcome {
    Inactive,
    Apply(f64),
    Exit(GentleExitCause),
}

/// Multiplier for the gentle-mode re-entry cooldown on high-RTT paths.
///
/// The cooldown is the larger of the fixed 15 s floor and 25 control RTTs so
/// that 600 ms+ links are not majority-gentle.
pub(crate) const GENTLE_REENTRY_COOLDOWN_RTTS: f64 = 25.0;

/// Snapshot of a single gentle-mode drain episode.
///
/// The guard records the RTT floor and queue gap at the start of the episode
/// and compares later gaps against that snapshot, rather than against the live
/// floor.  This prevents a smoothly-ratcheting floor from racing ahead of the
/// guard and making an effective drain look ineffective.
#[derive(Debug, Clone)]
pub(crate) struct DrainEpisode {
    pub(crate) start: Instant,
    pub(crate) floor0: Duration,
    pub(crate) gap0: Duration,
}

/// Gentle-mode congestion controller state.  Delay-gated conservative CC
/// sub-mode.
///
/// Wraps the delay-gated gentle-mode entry/exit, probe, drain-guard, and
/// gate-hysteresis logic extracted from [`ReliableLayer`].
#[derive(Debug)]
pub(crate) struct GentleMode {
    draining: bool,
    gentle_mode: bool,
    drain_episode: Option<DrainEpisode>,
    gentle_block_until: Option<Instant>,
    gentle_gate_open_since: Option<Instant>,
}

impl GentleMode {
    pub(crate) fn new() -> Self {
        Self {
            draining: false,
            gentle_mode: false,
            drain_episode: None,
            gentle_block_until: None,
            gentle_gate_open_since: None,
        }
    }

    /// Reset all gentle-mode state (called on outage-recovery epoch start).
    pub(crate) fn reset(&mut self) -> Option<GentleExitCause> {
        let exit = self.gentle_mode.then_some(GentleExitCause::OutageReset);
        self.draining = false;
        self.gentle_mode = false;
        self.drain_episode = None;
        self.gentle_block_until = None;
        self.gentle_gate_open_since = None;
        exit
    }

    /// Gentle-mode entry update: consume the queue-growth owner's persistent-queue
    /// signal, check loss-exit, and try to enter.
    pub(crate) fn update_mode(
        &mut self,
        persistent_queue_for: Option<Duration>,
        loss_event_rate: Option<f64>,
        now: Instant,
        control_rtt: Duration,
    ) -> Option<GentleExitCause> {
        // Loss exit: high loss must leave gentle mode immediately.
        let exit = if self.gentle_mode && loss_event_rate.is_some_and(|lr| lr >= GENTLE_EXIT_LOSS) {
            self.gentle_mode = false;
            self.gentle_gate_open_since = None;
            Some(GentleExitCause::Loss)
        } else {
            None
        };

        // Enter gentle mode after a sustained low-loss queue-building stretch.
        // The sustained stretch is the larger of three control RTTs and the
        // one-second entry floor, so a low-RTT path cannot enter on a sub-
        // second queue blip.
        let low_loss = loss_event_rate.map(|lr| lr < GENTLE_ENTER_MAX_LOSS);
        let block_cleared = self
            .gentle_block_until
            .map(|until| now >= until)
            .unwrap_or(true);
        let enter_after = control_rtt.mul_f64(GENTLE_ENTER_RTTS).max(GENTLE_ENTER_MIN);
        if let Some(stretch) = persistent_queue_for
            && !self.gentle_mode
            && stretch >= enter_after
            && low_loss != Some(false)
            && block_cleared
        {
            self.gentle_mode = true;
            self.draining = false;
            self.drain_episode = None;
        }
        exit
    }

    /// Gate hysteresis: while actively draining in gentle mode, reopen the
    /// delay gate once the queue shrinks back to within half the normal
    /// tolerance (`tol/2`).  Otherwise return the normal tolerance.
    pub(crate) fn gate_tol(&self, tol: Duration) -> Duration {
        if self.gentle_mode && self.draining {
            tol.mul_f64(0.5)
        } else {
            tol
        }
    }

    /// Notify the gentle controller that the gate is not open for probing,
    /// resetting the continuous-open timer.
    pub(crate) fn clear_gate_open(&mut self) {
        self.gentle_gate_open_since = None;
    }

    /// Attempt a gentle-mode probe.
    ///
    /// Distinguishes an inactive controller, an applied gentle probe, and the
    /// clean-gate transition that hands control back to normal probing.
    pub(crate) fn probe(
        &mut self,
        delivery_rate: f64,
        send_rate: f64,
        control_rtt: Duration,
        open_threshold: Duration,
        now: Instant,
        loss_event_rate: Option<f64>,
    ) -> GentleProbeOutcome {
        if !self.gentle_mode {
            return GentleProbeOutcome::Inactive;
        }
        self.draining = false;
        self.drain_episode = None;

        let open_since = self.gentle_gate_open_since.get_or_insert(now);
        let open_for = now.saturating_duration_since(*open_since);
        if open_for >= open_threshold {
            // The gate has been open long enough on a clean link: leave gentle
            // mode and let normal probing take over.
            self.gentle_mode = false;
            self.gentle_gate_open_since = None;
            GentleProbeOutcome::Exit(GentleExitCause::GateOpen)
        } else {
            // Scale the multiplicative gain by the survival fraction so a
            // loss-suppressed delivery rate does not compound the full
            // gentle gain either.  At zero loss the historical 1.2x holds.
            let probed =
                delivery_rate * (1.0 + loss_scaled_gain(GENTLE_BW_PROBE_GAIN, loss_event_rate));
            let additive = GENTLE_ADD_PKTS / control_rtt.as_secs_f64();
            let target = (probed + additive).max(send_rate);
            GentleProbeOutcome::Apply(target)
        }
    }

    /// Get the drain fraction - gentle mode drains more conservatively.
    pub(crate) fn drain_frac(&self) -> f64 {
        if self.gentle_mode {
            GENTLE_DRAIN_FRAC
        } else {
            DRAIN_RATE_FRACTION
        }
    }

    /// Update the drain-episode guard in gentle mode.
    ///
    /// If the drain is deemed ineffective (gap hasn't shrunk enough after the
    /// check window), exits gentle mode and blocks re-entry for a cooldown.
    pub(crate) fn drain_episode_guard(
        &mut self,
        smooth: Duration,
        floor: Duration,
        control_rtt: Duration,
        now: Instant,
    ) -> Option<GentleExitCause> {
        if !self.gentle_mode {
            return None;
        }
        self.draining = true;
        let episode = self.drain_episode.get_or_insert(DrainEpisode {
            start: now,
            floor0: floor,
            gap0: smooth.saturating_sub(floor),
        });
        // Compare against the snapshot floor taken at episode start, not the
        // live floor, so a ratcheting rtt_floor cannot fake gap shrinkage.
        let gap = smooth.saturating_sub(episode.floor0);
        if now.saturating_duration_since(episode.start)
            >= control_rtt.mul_f64(GENTLE_DRAIN_CHECK_RTTS)
            && gap > episode.gap0.mul_f64(GENTLE_DRAIN_GAP_SHRINK)
        {
            self.gentle_mode = false;
            self.draining = false;
            self.drain_episode = None;
            self.gentle_gate_open_since = None;
            self.gentle_block_until = Some(
                now + GENTLE_REENTRY_COOLDOWN
                    .max(control_rtt.mul_f64(GENTLE_REENTRY_COOLDOWN_RTTS)),
            );
            return Some(GentleExitCause::DrainGuard);
        }
        None
    }

    // ----- Test-visible accessors -------------------------------------------

    pub(crate) fn gentle_mode(&self) -> bool {
        self.gentle_mode
    }

    pub(crate) fn draining(&self) -> bool {
        self.draining
    }

    #[cfg(test)]
    pub(crate) fn drain_episode(&self) -> Option<&DrainEpisode> {
        self.drain_episode.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn gentle_block_until(&self) -> Option<Instant> {
        self.gentle_block_until
    }

    #[cfg(test)]
    pub(crate) fn gentle_gate_open_since(&self) -> Option<Instant> {
        self.gentle_gate_open_since
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        DRAIN_RATE_FRACTION, GENTLE_BW_PROBE_GAIN, GENTLE_DRAIN_CHECK_RTTS, GENTLE_DRAIN_FRAC,
        GENTLE_ENTER_MIN, GentleExitCause, GentleMode, GentleProbeOutcome,
    };

    #[test]
    fn gentle_mode_uses_its_dedicated_drain_fraction() {
        let t0 = Instant::now();
        let mut gentle = GentleMode::new();
        assert_eq!(gentle.drain_frac(), DRAIN_RATE_FRACTION);

        let _ = gentle.update_mode(
            Some(GENTLE_ENTER_MIN),
            Some(0.0),
            t0 + GENTLE_ENTER_MIN,
            Duration::from_millis(50),
        );
        assert!(gentle.gentle_mode());
        assert_eq!(gentle.drain_frac(), GENTLE_DRAIN_FRAC);
        assert_eq!(GENTLE_DRAIN_FRAC, 0.75);
    }

    #[test]
    fn gentle_probe_uses_conservative_gain_plus_additive_headroom() {
        let t0 = Instant::now();
        let mut gentle = GentleMode::new();
        let control_rtt = Duration::from_millis(100);
        let _ = gentle.update_mode(
            Some(GENTLE_ENTER_MIN),
            Some(0.0),
            t0 + GENTLE_ENTER_MIN,
            control_rtt,
        );
        let GentleProbeOutcome::Apply(target) = gentle.probe(
            100.0,
            100.0,
            control_rtt,
            Duration::from_secs(1),
            t0 + GENTLE_ENTER_MIN,
            Some(0.0),
        ) else {
            panic!("gentle mode should probe before the open threshold");
        };
        assert_eq!(GENTLE_BW_PROBE_GAIN, 0.20);
        assert_eq!(target, 160.0);
    }

    #[test]
    fn gentle_probe_gain_shrinks_with_loss() {
        let t0 = Instant::now();
        let mut gentle = GentleMode::new();
        let control_rtt = Duration::from_millis(100);
        let _ = gentle.update_mode(
            Some(GENTLE_ENTER_MIN),
            Some(0.0),
            t0 + GENTLE_ENTER_MIN,
            control_rtt,
        );
        // 10% loss scales the 1.2x gain down to 1.18x: probed = 118,
        // + additive 4/0.1 s = 40 -> 158 instead of the zero-loss 160.
        let GentleProbeOutcome::Apply(target) = gentle.probe(
            100.0,
            100.0,
            control_rtt,
            Duration::from_secs(1),
            t0 + GENTLE_ENTER_MIN,
            Some(0.1),
        ) else {
            panic!("gentle mode should probe before the open threshold");
        };
        assert_eq!(target, 158.0);
    }

    #[test]
    fn low_rtt_queue_must_persist_for_the_entry_floor() {
        let t0 = Instant::now();
        let mut gentle = GentleMode::new();
        let control_rtt = Duration::from_millis(50);
        // A 60 ms smooth RTT against a 40 ms floor: the three-control-RTT
        // condition (150 ms) elapses long before the one-second entry floor.
        // The standing queue must persist past GENTLE_ENTER_MIN regardless.
        for i in 0..9 {
            let _ = gentle.update_mode(
                Some(Duration::from_millis(100) * i),
                Some(0.0),
                t0 + Duration::from_millis(100) * i,
                control_rtt,
            );
            assert!(
                !gentle.gentle_mode(),
                "gentle mode must not enter before GENTLE_ENTER_MIN at step {i}"
            );
        }
        let _ = gentle.update_mode(
            Some(Duration::from_millis(950)),
            Some(0.0),
            t0 + Duration::from_millis(950),
            control_rtt,
        );
        assert!(
            !gentle.gentle_mode(),
            "950 ms of standing queue must not enter on a 50 ms control RTT"
        );
        let _ = gentle.update_mode(
            Some(GENTLE_ENTER_MIN),
            Some(0.0),
            t0 + GENTLE_ENTER_MIN,
            control_rtt,
        );
        assert!(
            gentle.gentle_mode(),
            "one second of standing queue must enter gentle mode"
        );
    }

    #[test]
    fn long_rtt_path_still_waits_for_three_control_rtts() {
        let t0 = Instant::now();
        let mut gentle = GentleMode::new();
        let control_rtt = Duration::from_millis(600);
        // 3 * 600 ms = 1800 ms dominates the one-second entry floor, so a
        // 1 s standing queue is not enough on this path.
        let _ = gentle.update_mode(Some(Duration::ZERO), Some(0.0), t0, control_rtt);
        let _ = gentle.update_mode(
            Some(Duration::from_millis(500)),
            Some(0.0),
            t0 + Duration::from_millis(500),
            control_rtt,
        );
        let _ = gentle.update_mode(
            Some(Duration::from_secs(1)),
            Some(0.0),
            t0 + Duration::from_secs(1),
            control_rtt,
        );
        assert!(
            !gentle.gentle_mode(),
            "one second of queue must not enter when 3 control RTTs exceed it"
        );
        gentle.update_mode(
            Some(Duration::from_millis(1800)),
            Some(0.0),
            t0 + Duration::from_millis(1800),
            control_rtt,
        );
        assert!(
            gentle.gentle_mode(),
            "three control RTTs of standing queue must enter gentle mode"
        );
    }
    #[test]
    fn drain_guard_reports_only_the_exit_it_causes() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let entered_at = t0 + GENTLE_ENTER_MIN;
        let floor = Duration::from_millis(50);
        let smooth = Duration::from_millis(150);
        let mut gentle = GentleMode::new();
        let _ = gentle.update_mode(Some(GENTLE_ENTER_MIN), Some(0.0), entered_at, control_rtt);

        assert_eq!(
            gentle.drain_episode_guard(smooth, floor, control_rtt, entered_at),
            None
        );
        let check_at = entered_at + control_rtt.mul_f64(GENTLE_DRAIN_CHECK_RTTS);
        assert_eq!(
            gentle.drain_episode_guard(smooth, floor, control_rtt, check_at),
            Some(GentleExitCause::DrainGuard)
        );
        assert!(!gentle.gentle_mode());
        assert_eq!(
            gentle.drain_episode_guard(smooth, floor, control_rtt, check_at),
            None
        );
    }

    #[test]
    fn every_non_guard_exit_reports_its_exact_cause_once() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let entered_at = t0 + GENTLE_ENTER_MIN;

        let mut loss = GentleMode::new();
        let _ = loss.update_mode(Some(GENTLE_ENTER_MIN), Some(0.0), entered_at, control_rtt);
        assert_eq!(
            loss.update_mode(Some(GENTLE_ENTER_MIN), Some(0.1), entered_at, control_rtt),
            Some(GentleExitCause::Loss)
        );
        assert_eq!(
            loss.update_mode(Some(GENTLE_ENTER_MIN), Some(0.1), entered_at, control_rtt),
            None
        );

        let mut gate = GentleMode::new();
        let _ = gate.update_mode(Some(GENTLE_ENTER_MIN), Some(0.0), entered_at, control_rtt);
        assert!(matches!(
            gate.probe(
                100.0,
                100.0,
                control_rtt,
                Duration::ZERO,
                entered_at,
                Some(0.0),
            ),
            GentleProbeOutcome::Exit(GentleExitCause::GateOpen)
        ));
        assert_eq!(
            gate.probe(
                100.0,
                100.0,
                control_rtt,
                Duration::ZERO,
                entered_at,
                Some(0.0),
            ),
            GentleProbeOutcome::Inactive
        );

        let mut outage = GentleMode::new();
        let _ = outage.update_mode(Some(GENTLE_ENTER_MIN), Some(0.0), entered_at, control_rtt);
        assert_eq!(outage.reset(), Some(GentleExitCause::OutageReset));
        assert_eq!(outage.reset(), None);
    }
}
