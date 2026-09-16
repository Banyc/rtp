//! Bounded fast start for the dedicated congestion lane.
//!
//! The ordinary bandwidth probe creeps toward capacity by a small
//! multiplicative gain once per control RTT, so a cold start on a long-RTT,
//! high-BDP path spends seconds below line before the first delay drain.  The
//! ACK-clocked slow start would close that ramp, but its accumulator was a
//! connection-lifetime sum divided by one control RTT: a backlogged flow never
//! drains, so the reported rate grew without bound and the ramp had to be cut
//! off before it could help.
//!
//! This replacement measures the *recent* acknowledged rate in a control-RTT
//! window and ramps multiplicatively from it, leaving fast start as soon as the
//! delivery rate stops growing (the pipe is full).  Both the rate and the exit
//! are bounded by what the path actually delivered, so the ramp cannot drive
//! an unbounded pacer rate or a deeper standing queue.  It is used only on the
//! [`CongestionLane::Dedicated`](super::CongestionLane::Dedicated) lane, which
//! has no cross-traffic to protect; the shared lane keeps the conservative
//! stock exit.

use std::time::{Duration, Instant};

/// Multiplicative gain applied once per window to the recently acknowledged
/// rate.  A value of 2 doubles the pace each control RTT while the pipe is
/// absorbing it.
const FAST_START_GAIN: f64 = 2.0;

/// Minimum window-over-window delivery growth that still counts as an
/// unsaturated pipe.  Below this the delivery rate has plateaued, which is the
/// capacity exit: the ramp settles to the delivered rate instead of driving
/// the pacer past what the path can carry.
const FAST_START_PLATEAU_GROWTH: f64 = 0.4;

/// Outcome of one fast-start window boundary.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum FastStartStep {
    /// No control-RTT window has elapsed; keep the current rate.
    Hold,
    /// Delivery kept growing: ramp toward this target (already floored at the
    /// rate that was paced).
    Ramp(f64),
    /// Delivery plateaued at capacity (or a window delivered nothing): leave
    /// fast start and settle at this rate. A zero-delivery window settles at
    /// the paced rate, never at zero.
    Plateau(f64),
}

/// Windowed ACK-clock driving the dedicated-lane ramp.
#[derive(Debug)]
pub(crate) struct FastStart {
    window_start: Instant,
    window_acked: u64,
    /// Delivered rate of the previous closed window.
    prev_delivered: Option<f64>,
    /// Delivered rate two windows back.  The delivered rate lags the pace by
    /// about one RTT, so the plateau is measured over two windows: during a
    /// doubling ramp the just-past window still reflects the previous pace.
    prev2_delivered: Option<f64>,
    /// Set on the first ack, so the first measured window is a full control
    /// RTT of real data rather than the connection-setup gap.
    started: bool,
}

impl FastStart {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            window_start: now,
            window_acked: 0,
            prev_delivered: None,
            prev2_delivered: None,
            started: false,
        }
    }

    pub(crate) fn reset(&mut self, now: Instant) {
        *self = Self::new(now);
    }

    /// Record `fresh` freshly-acknowledged packets at `now` and, once a full
    /// control RTT has elapsed, decide the next step.  `current_rate` is the
    /// layer's live send rate, used only to keep the ramp monotonic.
    pub(crate) fn on_ack(
        &mut self,
        fresh: usize,
        now: Instant,
        control_rtt: Duration,
        current_rate: f64,
    ) -> FastStartStep {
        if !self.started {
            self.started = true;
            self.window_start = now;
            self.window_acked = fresh as u64;
            return FastStartStep::Hold;
        }
        self.window_acked = self.window_acked.saturating_add(fresh as u64);
        let elapsed = now.saturating_duration_since(self.window_start);
        if elapsed < control_rtt {
            return FastStartStep::Hold;
        }

        let delivered = self.window_acked as f64 / elapsed.as_secs_f64();
        self.window_start = now;
        self.window_acked = 0;

        // A window can close with no freshly-acknowledged packets: an idle
        // gap, an all-lost flight, or a duplicate/coalesced ACK. Zero
        // delivery is neither a capacity plateau nor a rate to settle at, and
        // a transient non-finite computation is equally unusable. Leave slow
        // start at the rate that was actually being paced and leave the
        // two-window baselines untouched, so a resumed flow is compared
        // against the last window that really delivered data.
        if delivered <= 0.0 || !delivered.is_finite() {
            return FastStartStep::Plateau(current_rate);
        }

        // The first two measured windows are bootstrap: there is no
        // two-windows-past delivery to compare against.  A delivery plateau
        // (growth over two windows below the threshold) is the capacity exit;
        // the two-window span absorbs the ~1 RTT ACK-clock lag.
        let stalled = self
            .prev2_delivered
            .is_some_and(|prev| delivered <= prev * (1.0 + FAST_START_PLATEAU_GROWTH));
        self.prev2_delivered = self.prev_delivered;
        self.prev_delivered = Some(delivered);

        if stalled {
            FastStartStep::Plateau(delivered)
        } else {
            let target = (delivered * FAST_START_GAIN).max(current_rate);
            FastStartStep::Ramp(target)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    /// While the path absorbs every paced packet the ramp doubles per window
    /// and never reports a plateau.
    #[test]
    fn ramp_doubles_while_the_pipe_absorbs() {
        let t0 = Instant::now();
        let rtt = Duration::from_millis(100);
        let mut fs = FastStart::new(t0);
        let mut rate = 128.0;
        let mut now = t0;
        // The first ack only seeds the window; it does not ramp.
        assert_eq!(fs.on_ack(0, now, rtt, rate), FastStartStep::Hold);
        for _ in 0..4 {
            // Deliver exactly what was paced.
            let acked = (rate * rtt.as_secs_f64()) as usize;
            now += rtt;
            match fs.on_ack(acked, now, rtt, rate) {
                FastStartStep::Ramp(target) => {
                    assert!(
                        target > rate,
                        "ramp must increase the rate: {target} vs {rate}"
                    );
                    rate = target;
                }
                other => panic!("expected Ramp, got {other:?}"),
            }
        }
        assert!(
            rate >= 128.0 * 8.0,
            "four doublings from 128 must exceed 1024"
        );
    }

    /// Once delivery stops growing the ramp settles to the delivered rate
    /// instead of growing without bound.
    #[test]
    fn plateau_when_delivery_stops_growing() {
        let t0 = Instant::now();
        let rtt = Duration::from_millis(100);
        let mut fs = FastStart::new(t0);
        let mut now = t0;
        // The first ack only seeds the window.
        assert_eq!(fs.on_ack(0, now, rtt, 128.0), FastStartStep::Hold);
        // Two bootstrap windows: delivery 1000 pkt/s, both ramp (the plateau
        // is measured over two windows).
        for _ in 0..2 {
            now += rtt;
            assert!(matches!(
                fs.on_ack(100, now, rtt, 128.0),
                FastStartStep::Ramp(_)
            ));
        }
        // Third window: delivery identical two windows back (no growth, no
        // overshoot).
        now += rtt;
        match fs.on_ack(100, now, rtt, 2000.0) {
            FastStartStep::Plateau(rate) => assert!((rate - 1000.0).abs() < 1.0),
            other => panic!("expected Plateau, got {other:?}"),
        }
    }

    /// Slow delivery growth below the plateau threshold still ends the ramp:
    /// the pipe is near capacity and the pacer must not keep doubling.
    #[test]
    fn slow_growth_is_a_plateau() {
        let t0 = Instant::now();
        let rtt = Duration::from_millis(100);
        let mut fs = FastStart::new(t0);
        let mut now = t0;
        assert_eq!(fs.on_ack(0, now, rtt, 128.0), FastStartStep::Hold);
        now += rtt;
        assert!(matches!(
            fs.on_ack(100, now, rtt, 128.0),
            FastStartStep::Ramp(_)
        ));
        now += rtt;
        assert!(matches!(
            fs.on_ack(110, now, rtt, 128.0),
            FastStartStep::Ramp(_)
        ));
        // 10% growth over two windows is below the 40% plateau threshold.
        now += rtt;
        match fs.on_ack(110, now, rtt, 2000.0) {
            FastStartStep::Plateau(rate) => assert!((rate - 1100.0).abs() < 1.0),
            other => panic!("expected Plateau, got {other:?}"),
        }
    }

    /// A window that closes with no freshly-acknowledged packets must not be
    /// reported as a zero-rate plateau: zero delivery is an idle gap, an
    /// all-lost flight, or a duplicate/coalesced ACK, not evidence that the
    /// pipe is full. The plateau must instead settle at the rate that was
    /// actually being paced; the previous `Plateau(0.0)` reached the caller's
    /// `PosR` construction and panicked the transport worker.
    #[test]
    fn zero_delivery_window_settles_at_the_paced_rate() {
        let t0 = Instant::now();
        let rtt = Duration::from_millis(100);
        let mut fs = FastStart::new(t0);
        let mut now = t0;
        // Seed the window, then close two real windows so the two-windows-past
        // delivery baseline is populated and a plateau is classifiable.
        assert_eq!(fs.on_ack(0, now, rtt, 128.0), FastStartStep::Hold);
        for _ in 0..2 {
            now += rtt;
            assert!(matches!(
                fs.on_ack(100, now, rtt, 128.0),
                FastStartStep::Ramp(_)
            ));
        }
        // The third window delivers nothing: settle at the paced rate, never
        // at zero.
        now += rtt;
        let step = fs.on_ack(0, now, rtt, 128.0);
        assert_eq!(
            step,
            FastStartStep::Plateau(128.0),
            "a zero-delivery window must settle at a valid positive rate"
        );
    }

    /// A window shorter than one control RTT holds the rate.
    #[test]
    fn short_window_holds() {
        let t0 = Instant::now();
        let rtt = Duration::from_millis(100);
        let mut fs = FastStart::new(t0);
        assert_eq!(
            fs.on_ack(5, t0 + Duration::from_millis(50), rtt, 128.0),
            FastStartStep::Hold
        );
    }
}
