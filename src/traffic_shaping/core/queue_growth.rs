//! Delay-first queue-growth detection.
//!
//! Queue growth is detected before waiting for packet loss.  This module owns
//! the recent RTT baseline, the ordinary delay gate, the wider persistent-
//! queue timer used by gentle mode, and the current queue-building decision so
//! those parts cannot drift apart across the congestion controller.
use std::time::{Duration, Instant};

#[cfg(test)]
use super::gentle::DrainEpisode;
use super::gentle::{GentleExitCause, GentleMode, GentleProbeOutcome};

pub(crate) const RTT_MIN_BUCKET: Duration = Duration::from_secs(5);
/// Shorter RTT-floor bucket for a reorder-tolerant connection (the interactive
/// frame fast-forward lane). A long bucket lets an isolated low RTT sample —
/// the receiver echoes the send timestamp of a reordered packet that took a
/// faster path — pin the propagation floor for the whole bucket, so the
/// ordinary RTT then reads as a standing queue and the delay controller
/// drains the send rate. The interactive lane opts into out-of-order frame
/// delivery, so its floor must track the recent baseline instead of holding
/// one outlier. Bulk and strict paths keep [`RTT_MIN_BUCKET`].
pub(crate) const RTT_MIN_BUCKET_REORDER: Duration = Duration::from_millis(200);
pub(crate) const RTT_MIN_BUCKET_RTT_SCALE: u32 = 10;

pub(crate) const QUEUE_RTT_FACTOR: f64 = 2.0;
/// The persistent-queue timer uses a wider RTT-variance margin than the
/// ordinary delay gate, preventing high jitter from masquerading as a standing
/// queue while still letting the ordinary gate drain transient growth.
pub(crate) const PERSISTENT_QUEUE_RTTVAR_FACTOR: f64 = 2.0;
pub(crate) const QUEUE_TOL_RTT_FRACTION: f64 = 0.25;
pub(crate) const QUEUE_RTT_FLOOR: Duration = Duration::from_millis(5);

/// Minimum of a sliding window of RTT samples.
///
/// RTT rises when a queue builds, but a lifetime min_rtt collapses to ~0 on
/// jittery links and never recovers.  Instead, keep a short windowed minimum:
/// the floor tracks recent baseline RTT and recovers quickly enough to let the
/// delay-based gate close when the queue inflates and reopen when it drains.
#[derive(Debug, Clone)]
pub(crate) struct WindowedRttMin {
    bucket_start: Instant,
    cur: Option<Duration>,
    prev: Option<Duration>,
    min_bucket: Duration,
    /// Scale the bucket by the *established* floor instead of the incoming
    /// sample. Used on the reorder-tolerant lane: a path shift inflates the
    /// incoming smoothed RTT, and scaling the window by that inflated value
    /// would keep the pre-shift floor alive for proportionally longer — the
    /// longer the new RTT, the longer the stale floor pins the delay gate.
    /// Scaling by the established floor keeps the window at the baseline's
    /// timescale until the floor itself tracks the shift.
    baseline_scaled: bool,
}

impl WindowedRttMin {
    #[cfg(test)]
    pub(crate) fn new(now: Instant) -> Self {
        Self::with_min_bucket(now, RTT_MIN_BUCKET, false)
    }

    /// A floor window with a caller-chosen minimum bucket length. The bucket
    /// is still scaled up with the RTT (`rtt * RTT_MIN_BUCKET_RTT_SCALE`), so
    /// a high-RTT path keeps a proportionally long window. `baseline_scaled`
    /// selects the *established floor* as the scaling basis (see the field
    /// docs).
    pub(crate) fn with_min_bucket(
        now: Instant,
        min_bucket: Duration,
        baseline_scaled: bool,
    ) -> Self {
        Self {
            bucket_start: now,
            cur: None,
            prev: None,
            min_bucket,
            baseline_scaled,
        }
    }

    pub(crate) fn update(&mut self, now: Instant, rtt: Duration) -> Duration {
        let basis = if self.baseline_scaled {
            match (self.cur, self.prev) {
                (Some(a), Some(b)) => a.min(b),
                (Some(a), None) | (None, Some(a)) => a,
                (None, None) => rtt,
            }
        } else {
            rtt
        };
        let bucket = self
            .min_bucket
            .max(basis.saturating_mul(RTT_MIN_BUCKET_RTT_SCALE));
        let elapsed = now.duration_since(self.bucket_start);
        if elapsed > bucket * 2 {
            // Idle staleness: both buckets have aged out, mirror LossEventWindow::rotate.
            self.cur = None;
            self.prev = None;
            self.bucket_start = now;
        } else if elapsed > bucket {
            self.prev = self.cur.take();
            self.bucket_start = now;
        }

        self.cur = Some(match self.cur {
            Some(cur) => cur.min(rtt),
            None => rtt,
        });

        let candidates = [self.cur, self.prev].into_iter().flatten();
        candidates.min().unwrap_or(rtt)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct QueueGrowthObservation {
    pub(crate) floor: Duration,
    pub(crate) tolerance: Duration,
    pub(crate) building: bool,
    pub(crate) persistent_for: Option<Duration>,
    pub(crate) gentle_exit: Option<GentleExitCause>,
}

/// Owns every stateful part of delay-first queue detection and its gentle-mode
/// hysteresis.  `ReliableLayer` supplies transport observations and consumes a
/// single coherent [`QueueGrowthObservation`].
#[derive(Debug)]
pub(crate) struct QueueGrowth {
    floor: WindowedRttMin,
    /// `true` for the interactive frame fast-forward lane: reordering is
    /// expected, so the floor window is shortened to reject isolated
    /// reorder-induced low RTT outliers instead of treating them as the path
    /// baseline.
    reorder_tolerant: bool,
    persistent_since: Option<Instant>,
    building: bool,
    gentle: GentleMode,
}

impl QueueGrowth {
    pub(crate) fn new(now: Instant, reorder_tolerant: bool) -> Self {
        let floor = Self::fresh_floor(now, reorder_tolerant);
        Self {
            floor,
            reorder_tolerant,
            persistent_since: None,
            building: false,
            gentle: GentleMode::new(),
        }
    }

    fn fresh_floor(now: Instant, reorder_tolerant: bool) -> WindowedRttMin {
        let (min_bucket, baseline_scaled) = if reorder_tolerant {
            (RTT_MIN_BUCKET_REORDER, true)
        } else {
            (RTT_MIN_BUCKET, false)
        };
        WindowedRttMin::with_min_bucket(now, min_bucket, baseline_scaled)
    }

    pub(crate) fn reset(&mut self, now: Instant) -> Option<GentleExitCause> {
        self.floor = Self::fresh_floor(now, self.reorder_tolerant);
        self.persistent_since = None;
        self.building = false;
        self.gentle.reset()
    }

    pub(crate) fn observe(
        &mut self,
        smooth: Duration,
        rttvar: Duration,
        loss_event_rate: Option<f64>,
        now: Instant,
        control_rtt: Duration,
    ) -> QueueGrowthObservation {
        // The floor is deliberately fed by smoothed RTT.  The raw-min variant
        // measured worse; the separate RTT-variance terms protect jitter.
        let floor = self.floor.update(now, smooth);
        let tolerance = queue_tolerance(rttvar, floor, QUEUE_RTT_FACTOR);
        let persistent_tolerance = queue_tolerance(
            rttvar,
            floor,
            QUEUE_RTT_FACTOR * PERSISTENT_QUEUE_RTTVAR_FACTOR,
        );
        if smooth > floor + persistent_tolerance {
            self.persistent_since.get_or_insert(now);
        } else {
            self.persistent_since = None;
        }
        let persistent_for = self
            .persistent_since
            .map(|start| now.saturating_duration_since(start));

        let was_gentle = self.gentle.gentle_mode();
        let gentle_exit =
            self.gentle
                .update_mode(persistent_for, loss_event_rate, now, control_rtt);
        self.clear_persistence_after_gentle_exit(was_gentle);

        let tolerance = self.gentle.gate_tol(tolerance);
        self.building = smooth > floor + tolerance;
        QueueGrowthObservation {
            floor,
            tolerance,
            building: self.building,
            persistent_for: self
                .persistent_since
                .map(|start| now.saturating_duration_since(start)),
            gentle_exit,
        }
    }

    fn clear_persistence_after_gentle_exit(&mut self, was_gentle: bool) {
        if was_gentle && !self.gentle.gentle_mode() {
            self.persistent_since = None;
        }
    }

    pub(crate) fn clear_gate_open(&mut self) {
        self.gentle.clear_gate_open();
    }

    pub(crate) fn probe(
        &mut self,
        delivery_rate: f64,
        send_rate: f64,
        control_rtt: Duration,
        smooth_rtt: Duration,
        now: Instant,
        loss_event_rate: Option<f64>,
    ) -> GentleProbeOutcome {
        let was_gentle = self.gentle.gentle_mode();
        let open_threshold =
            RTT_MIN_BUCKET.max(smooth_rtt.saturating_mul(RTT_MIN_BUCKET_RTT_SCALE));
        let target = self.gentle.probe(
            delivery_rate,
            send_rate,
            control_rtt,
            open_threshold,
            now,
            loss_event_rate,
        );
        self.clear_persistence_after_gentle_exit(was_gentle);
        target
    }

    pub(crate) fn drain_frac(&self) -> f64 {
        self.gentle.drain_frac()
    }

    pub(crate) fn drain_episode_guard(
        &mut self,
        smooth: Duration,
        floor: Duration,
        control_rtt: Duration,
        now: Instant,
    ) -> Option<GentleExitCause> {
        let was_gentle = self.gentle.gentle_mode();
        let exited = self
            .gentle
            .drain_episode_guard(smooth, floor, control_rtt, now);
        self.clear_persistence_after_gentle_exit(was_gentle);
        exited
    }

    pub(crate) fn building(&self) -> bool {
        self.building
    }

    /// Whether this connection opted into receiver-side frame fast-forward (the
    /// reorder-tolerant interactive lane).
    pub(crate) fn reorder_tolerant(&self) -> bool {
        self.reorder_tolerant
    }

    #[cfg(test)]
    pub(crate) fn set_building(&mut self, building: bool) {
        self.building = building;
    }

    pub(crate) fn gentle_mode(&self) -> bool {
        self.gentle.gentle_mode()
    }

    pub(crate) fn draining(&self) -> bool {
        self.gentle.draining()
    }

    #[cfg(test)]
    pub(crate) fn drain_episode(&self) -> Option<&DrainEpisode> {
        self.gentle.drain_episode()
    }

    #[cfg(test)]
    pub(crate) fn gentle_block_until(&self) -> Option<Instant> {
        self.gentle.gentle_block_until()
    }

    #[cfg(test)]
    pub(crate) fn gentle_gate_open_since(&self) -> Option<Instant> {
        self.gentle.gentle_gate_open_since()
    }

    #[cfg(test)]
    pub(crate) fn replace_floor(&mut self, floor: WindowedRttMin) {
        self.floor = floor;
    }

    #[cfg(test)]
    pub(crate) fn update_floor(&mut self, now: Instant, smooth: Duration) -> Duration {
        self.floor.update(now, smooth)
    }
}

fn queue_tolerance(rttvar: Duration, floor: Duration, coefficient: f64) -> Duration {
    rttvar
        .mul_f64(coefficient)
        .max(floor.mul_f64(QUEUE_TOL_RTT_FRACTION))
        .max(QUEUE_RTT_FLOOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistent_queue_uses_a_wider_jitter_margin_than_the_normal_gate() {
        let now = Instant::now();
        let floor = Duration::from_millis(100);
        let rttvar = Duration::from_millis(50);
        let smooth = Duration::from_millis(250);
        let normal = queue_tolerance(rttvar, floor, QUEUE_RTT_FACTOR);
        let persistent = queue_tolerance(
            rttvar,
            floor,
            QUEUE_RTT_FACTOR * PERSISTENT_QUEUE_RTTVAR_FACTOR,
        );
        assert!(smooth > floor + normal);
        assert!(smooth <= floor + persistent);

        let mut growth = QueueGrowth::new(now, false);
        growth.observe(floor, rttvar, Some(0.0), now, Duration::from_millis(100));
        let observation = growth.observe(
            smooth,
            rttvar,
            Some(0.0),
            now + Duration::from_millis(1),
            Duration::from_millis(100),
        );
        assert!(observation.building);
        assert_eq!(observation.persistent_for, None);
    }

    /// The interactive fast-forward lane opts into out-of-order delivery, so a
    /// single reordered packet's low echoed RTT must not pin the propagation
    /// floor for the whole (multi-second) default bucket. The reorder-tolerant
    /// floor tracks the recent baseline; the default keeps its long bucket.
    #[test]
    fn reorder_tolerant_floor_recovers_from_a_low_outlier_sooner() {
        let t0 = Instant::now();
        let low = Duration::from_millis(25);
        let normal = Duration::from_millis(50);
        let control_rtt = Duration::from_millis(100);

        let mut default = QueueGrowth::new(t0, false);
        let mut reorder = QueueGrowth::new(t0, true);
        default.observe(low, Duration::ZERO, Some(0.0), t0, control_rtt);
        reorder.observe(low, Duration::ZERO, Some(0.0), t0, control_rtt);

        let later = t0 + Duration::from_secs(2);
        let default_floor = default
            .observe(normal, Duration::ZERO, Some(0.0), later, control_rtt)
            .floor;
        let reorder_floor = reorder
            .observe(normal, Duration::ZERO, Some(0.0), later, control_rtt)
            .floor;

        assert_eq!(
            default_floor, low,
            "the default floor must keep its long-bucket baseline"
        );
        assert_eq!(
            reorder_floor, normal,
            "the reorder-tolerant floor must track the recent RTT"
        );
    }

    /// The other direction of floor staleness: a genuine latency STEP up.  The
    /// reorder-tolerant floor scales its short bucket by the *established*
    /// floor, so the step-inflated sample cannot stretch the window that must
    /// forget the old low floor.  The default (sample-scaled) floor keeps the
    /// stale value for the full 5 s bucket, and the reorder floor does not.
    #[test]
    fn reorder_baseline_scaled_floor_tracks_a_latency_step() {
        let t0 = Instant::now();
        let low = Duration::from_millis(50);
        let high = Duration::from_millis(250);
        let control_rtt = Duration::from_millis(200);

        let mut default = QueueGrowth::new(t0, false);
        let mut reorder = QueueGrowth::new(t0, true);
        default.observe(low, Duration::ZERO, Some(0.0), t0, control_rtt);
        reorder.observe(low, Duration::ZERO, Some(0.0), t0, control_rtt);

        let mut default_floor = low;
        let mut reorder_floor = low;
        for i in 1..=3u64 {
            let t = t0 + Duration::from_secs(i);
            default_floor = default
                .observe(high, Duration::ZERO, Some(0.0), t, control_rtt)
                .floor;
            reorder_floor = reorder
                .observe(high, Duration::ZERO, Some(0.0), t, control_rtt)
                .floor;
        }

        assert_eq!(
            default_floor, low,
            "the default floor must keep its long-bucket baseline"
        );
        assert_eq!(
            reorder_floor, high,
            "the reorder-tolerant floor must track the new path RTT"
        );
    }
}
