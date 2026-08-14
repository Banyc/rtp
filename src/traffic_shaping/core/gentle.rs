use std::time::{Duration, Instant};

// Gentle-mode parameters for the delay-gated congestion controller.  These are
// intentionally conservative: they let a bulk flow drain a self-inflicted
// droptail bottleneck without driving the delay gate so hard that interactive
// cross-traffic keeps getting tail-dropped.
pub(crate) const GENTLE_BW_PROBE_GAIN: f64 = 0.25;
pub(crate) const GENTLE_DRAIN_FRAC: f64 = 0.7;
pub(crate) const GENTLE_ADD_PKTS: f64 = 4.0;
pub(crate) const GENTLE_ENTER_RTTS: f64 = 3.0;
pub(crate) const GENTLE_ENTER_MIN: Duration = Duration::from_secs(1);
pub(crate) const GENTLE_ENTER_MAX_LOSS: f64 = 0.05;
pub(crate) const GENTLE_EXIT_LOSS: f64 = 0.1;
pub(crate) const GENTLE_DRAIN_CHECK_RTTS: f64 = 12.0;
pub(crate) const GENTLE_DRAIN_GAP_SHRINK: f64 = 0.85;
pub(crate) const GENTLE_ENTER_RTTVAR_FACTOR: f64 = 2.0;
pub(crate) const GENTLE_REENTRY_COOLDOWN: Duration = Duration::from_secs(15);

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

#[derive(Debug, Clone, Copy)]
pub(crate) struct GentlePreambleConfig {
    pub(crate) enter_coefficient: f64,
    pub(crate) control_rtt: Duration,
}

/// Gentle-mode congestion controller state.  Delay-gated conservative CC
/// sub-mode.
///
/// Wraps the delay-gated gentle-mode entry/exit, probe, drain-guard, and
/// gate-hysteresis logic extracted from [`ReliableLayer`].
#[derive(Debug)]
pub(crate) struct GentleMode {
    draining: bool,
    queue_since: Option<Instant>,
    gentle_mode: bool,
    drain_episode: Option<DrainEpisode>,
    gentle_block_until: Option<Instant>,
    gentle_gate_open_since: Option<Instant>,
}

impl GentleMode {
    pub(crate) fn new() -> Self {
        Self {
            draining: false,
            queue_since: None,
            gentle_mode: false,
            drain_episode: None,
            gentle_block_until: None,
            gentle_gate_open_since: None,
        }
    }

    /// Reset all gentle-mode state (called on outage-recovery epoch start).
    pub(crate) fn reset(&mut self) {
        self.draining = false;
        self.queue_since = None;
        self.gentle_mode = false;
        self.drain_episode = None;
        self.gentle_block_until = None;
        self.gentle_gate_open_since = None;
    }

    /// Gentle-mode entry update: track whether the smoothed RTT is building a
    /// queue above the enter tolerance, check loss-exit, and try to enter.
    pub(crate) fn update_mode(
        &mut self,
        smooth: Duration,
        floor: Duration,
        rttvar: Duration,
        loss_event_rate: Option<f64>,
        now: Instant,
        config: GentlePreambleConfig,
    ) {
        let GentlePreambleConfig {
            enter_coefficient,
            control_rtt,
        } = config;
        let enter_tol = rttvar
            .mul_f64(enter_coefficient)
            .max(floor.mul_f64(crate::reliable::reliable_layer::QUEUE_TOL_RTT_FRACTION))
            .max(crate::reliable::reliable_layer::QUEUE_RTT_FLOOR);

        let entering = smooth > floor + enter_tol;
        if entering {
            self.queue_since.get_or_insert(now);
        } else {
            self.queue_since = None;
        }

        // Loss exit: high loss must leave gentle mode immediately.
        if self.gentle_mode && loss_event_rate.is_some_and(|lr| lr > GENTLE_EXIT_LOSS) {
            self.gentle_mode = false;
            self.queue_since = None;
            self.gentle_gate_open_since = None;
        }

        // Enter gentle mode after a sustained low-loss queue-building stretch.
        // The sustained stretch is the larger of three control RTTs and the
        // one-second entry floor, so a low-RTT path cannot enter on a sub-
        // second queue blip.
        let stretch = self
            .queue_since
            .map(|start| now.saturating_duration_since(start));
        let low_loss = loss_event_rate.map(|lr| lr < GENTLE_ENTER_MAX_LOSS);
        let block_cleared = self
            .gentle_block_until
            .map(|until| now >= until)
            .unwrap_or(true);
        let enter_after = control_rtt.mul_f64(GENTLE_ENTER_RTTS).max(GENTLE_ENTER_MIN);
        if let Some(stretch) = stretch
            && !self.gentle_mode
            && stretch >= enter_after
            && low_loss != Some(false)
            && block_cleared
        {
            self.gentle_mode = true;
            self.draining = false;
            self.drain_episode = None;
        }
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
    /// Returns `Some(target_rate)` if the gentle probe should be applied (the
    /// caller should set the smooth send rate and return).  Returns `None` if
    /// the normal probe/drain branches should execute.
    pub(crate) fn probe(
        &mut self,
        delivery_rate: f64,
        send_rate: f64,
        control_rtt: Duration,
        smooth_rtt: Duration,
        now: Instant,
    ) -> Option<f64> {
        if !self.gentle_mode {
            return None;
        }
        self.draining = false;
        self.drain_episode = None;

        let open_since = self.gentle_gate_open_since.get_or_insert(now);
        let open_for = now.saturating_duration_since(*open_since);
        let open_threshold = crate::traffic_shaping::core::rate_window::RTT_MIN_BUCKET.max(
            smooth_rtt.saturating_mul(
                crate::traffic_shaping::core::rate_window::RTT_MIN_BUCKET_RTT_SCALE,
            ),
        );
        if open_for >= open_threshold {
            // The gate has been open long enough on a clean link: leave gentle
            // mode and let normal probing take over.
            self.gentle_mode = false;
            self.queue_since = None;
            self.gentle_gate_open_since = None;
            None
        } else {
            let probed = delivery_rate * (1. + GENTLE_BW_PROBE_GAIN);
            let additive = GENTLE_ADD_PKTS / control_rtt.as_secs_f64();
            let target = (probed + additive).max(send_rate);
            Some(target)
        }
    }

    /// Get the drain fraction — gentle mode drains more conservatively.
    pub(crate) fn drain_frac(&self) -> f64 {
        if self.gentle_mode {
            GENTLE_DRAIN_FRAC
        } else {
            crate::reliable::reliable_layer::DRAIN_RATE_FRACTION
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
    ) {
        if !self.gentle_mode {
            return;
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
            self.queue_since = None;
            self.drain_episode = None;
            self.gentle_gate_open_since = None;
            self.gentle_block_until = Some(
                now + GENTLE_REENTRY_COOLDOWN
                    .max(control_rtt.mul_f64(GENTLE_REENTRY_COOLDOWN_RTTS)),
            );
        }
    }

    /// Expose `queue_since` so the drain floor decay code can compute the
    /// `closed_for` interval.
    pub(crate) fn queue_since(&self) -> Option<Instant> {
        self.queue_since
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

    use super::{GENTLE_ENTER_MIN, GentleMode, GentlePreambleConfig};

    #[test]
    fn low_rtt_queue_must_persist_for_the_entry_floor() {
        let t0 = Instant::now();
        let mut gentle = GentleMode::new();
        let config = GentlePreambleConfig {
            enter_coefficient: 2.0,
            control_rtt: Duration::from_millis(50),
        };
        // A 60 ms smooth RTT against a 40 ms floor: the three-control-RTT
        // condition (150 ms) elapses long before the one-second entry floor.
        // The standing queue must persist past GENTLE_ENTER_MIN regardless.
        for i in 0..9 {
            gentle.update_mode(
                Duration::from_millis(60),
                Duration::from_millis(40),
                Duration::from_millis(5),
                Some(0.0),
                t0 + Duration::from_millis(100) * i,
                config,
            );
            assert!(
                !gentle.gentle_mode(),
                "gentle mode must not enter before GENTLE_ENTER_MIN at step {i}"
            );
        }
        gentle.update_mode(
            Duration::from_millis(60),
            Duration::from_millis(40),
            Duration::from_millis(5),
            Some(0.0),
            t0 + Duration::from_millis(950),
            config,
        );
        assert!(
            !gentle.gentle_mode(),
            "950 ms of standing queue must not enter on a 50 ms control RTT"
        );
        gentle.update_mode(
            Duration::from_millis(60),
            Duration::from_millis(40),
            Duration::from_millis(5),
            Some(0.0),
            t0 + GENTLE_ENTER_MIN,
            config,
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
        let config = GentlePreambleConfig {
            enter_coefficient: 2.0,
            control_rtt: Duration::from_millis(600),
        };
        // 3 * 600 ms = 1800 ms dominates the one-second entry floor, so a
        // 1 s standing queue is not enough on this path.
        gentle.update_mode(
            Duration::from_millis(60),
            Duration::from_millis(40),
            Duration::from_millis(5),
            Some(0.0),
            t0,
            config,
        );
        gentle.update_mode(
            Duration::from_millis(60),
            Duration::from_millis(40),
            Duration::from_millis(5),
            Some(0.0),
            t0 + Duration::from_millis(500),
            config,
        );
        gentle.update_mode(
            Duration::from_millis(60),
            Duration::from_millis(40),
            Duration::from_millis(5),
            Some(0.0),
            t0 + Duration::from_secs(1),
            config,
        );
        assert!(
            !gentle.gentle_mode(),
            "one second of queue must not enter when 3 control RTTs exceed it"
        );
        gentle.update_mode(
            Duration::from_millis(60),
            Duration::from_millis(40),
            Duration::from_millis(5),
            Some(0.0),
            t0 + Duration::from_millis(1800),
            config,
        );
        assert!(
            gentle.gentle_mode(),
            "three control RTTs of standing queue must enter gentle mode"
        );
    }
}
