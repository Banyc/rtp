use std::time::{Duration, Instant};

use super::pkt_send_space::INIT_CWND;

#[derive(Debug, Clone)]
struct LossEventBucket {
    lost: usize,
    delivered: usize,
    start: Option<Instant>,
}

impl LossEventBucket {
    fn new() -> Self {
        Self {
            lost: 0,
            delivered: 0,
            start: None,
        }
    }

    fn reset(&mut self, start: Instant) {
        self.lost = 0;
        self.delivered = 0;
        self.start = Some(start);
    }
}

/// Sliding two-bucket window over congestion loss events, keyed to one smoothed
/// RTT per bucket.
#[derive(Debug, Clone)]
pub(crate) struct LossEventWindow {
    curr: LossEventBucket,
    prev: LossEventBucket,
}

impl LossEventWindow {
    pub(crate) fn new() -> Self {
        Self {
            curr: LossEventBucket::new(),
            prev: LossEventBucket::new(),
        }
    }

    fn bucket_len(smooth_rtt: Duration) -> Duration {
        smooth_rtt.max(Duration::from_millis(1))
    }

    fn rotate(&mut self, now: Instant, smooth_rtt: Duration) {
        let bucket_len = Self::bucket_len(smooth_rtt);
        let curr_start = self.curr.start.unwrap_or(now);
        let elapsed = now.duration_since(curr_start);
        if elapsed < bucket_len {
            return;
        }
        let gap_buckets = elapsed.as_nanos() / bucket_len.as_nanos();
        if gap_buckets >= 2 {
            self.curr.reset(now);
            self.prev.reset(now);
            return;
        }
        self.prev = self.curr.clone();
        self.curr.reset(now);
    }

    pub(crate) fn record_delivered(
        &mut self,
        delivered: usize,
        now: Instant,
        smooth_rtt: Duration,
    ) {
        self.rotate(now, smooth_rtt);
        if self.curr.start.is_none() {
            self.curr.reset(now);
        }
        self.curr.delivered += delivered;
    }

    pub(crate) fn record_lost(&mut self, lost: usize, now: Instant, smooth_rtt: Duration) {
        self.rotate(now, smooth_rtt);
        if self.curr.start.is_none() {
            self.curr.reset(now);
        }
        self.curr.lost += lost;
    }

    pub(crate) fn rate(&mut self, now: Instant, smooth_rtt: Duration) -> Option<f64> {
        self.rotate(now, smooth_rtt);
        let total = self.curr.lost + self.curr.delivered + self.prev.lost + self.prev.delivered;
        if total < INIT_CWND {
            return None;
        }
        let lost = self.curr.lost + self.prev.lost;
        Some(lost as f64 / total as f64)
    }

    /// Directly inspect the buckets without rotating.
    pub(crate) fn raw_has_loss_event(&self) -> bool {
        self.curr.lost > 0 || self.prev.lost > 0
    }

    pub(crate) fn reset(&mut self, now: Instant, smooth_rtt: Duration) {
        self.curr.reset(now);
        self.prev.reset(now + Self::bucket_len(smooth_rtt));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn loss_event_rate_reads_true_loss() {
        let now = Instant::now();
        let smooth = ms(100);
        let mut w = LossEventWindow::new();
        // Deliver 85, lose 15 over a single bucket worth of samples.
        for _ in 0..85 {
            w.record_delivered(1, now, smooth);
        }
        for _ in 0..15 {
            w.record_lost(1, now, smooth);
        }
        let rate = w.rate(now, smooth).unwrap();
        assert!((rate - 0.15).abs() < 0.001, "rate={rate}");
    }

    #[test]
    fn loss_event_rate_needs_min_samples() {
        let now = Instant::now();
        let smooth = ms(100);
        let mut w = LossEventWindow::new();
        for _ in 0..15 {
            w.record_lost(1, now, smooth);
        }
        assert!(w.rate(now, smooth).is_none());
        for _ in 0..16 {
            w.record_delivered(1, now, smooth);
        }
        assert!(w.rate(now, smooth).is_some());
    }

    #[test]
    fn a_sub_two_bucket_gap_keeps_the_loss_in_prev_on_a_fractional_rtt() {
        let t0 = Instant::now();
        let smooth = Duration::from_micros(10_500);
        let mut w = LossEventWindow::new();
        w.record_lost(1, t0, smooth);
        w.record_delivered(1, t0 + ms(20), smooth);
        assert!(
            w.raw_has_loss_event(),
            "a 1.90-bucket gap aged the loss event out"
        );
    }

    #[test]
    fn loss_events_age_out() {
        let t0 = Instant::now();
        let smooth = ms(100);
        let mut w = LossEventWindow::new();
        // Fill current bucket with losses.
        for _ in 0..100 {
            w.record_lost(1, t0, smooth);
        }
        let r = w.rate(t0, smooth).unwrap();
        assert!(r > 0.9, "r={r}");

        // After one bucket rotation losses move to prev and still count.
        let t1 = t0 + smooth + ms(1);
        for _ in 0..100 {
            w.record_delivered(1, t1, smooth);
        }
        let r = w.rate(t1, smooth).unwrap();
        assert!(r > 0.4 && r < 0.6, "r={r}");

        // After a second rotation the old losses are gone.
        let t2 = t1 + smooth + ms(1);
        for _ in 0..100 {
            w.record_delivered(1, t2, smooth);
        }
        let r = w.rate(t2, smooth).unwrap();
        assert!(r < 0.01, "r={r}");

        // After a 10-bucket idle gap the window is empty and abstains.
        let t3 = t2 + smooth * 10;
        assert!(w.rate(t3, smooth).is_none());
    }
}
