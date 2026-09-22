use std::time::{Duration, Instant};

/// Window over which the delivery-rate peak is tracked.
///
/// Twice RTT_MIN_BUCKET so the peak reflects the recent steady state but ages
/// out after idle gaps, mirroring the queue-growth RTT floor.
const DELIVERY_PEAK_BUCKET: Duration = Duration::from_secs(10);

/// Maximum of a sliding window of delivery-rate samples.
///
/// Mirrors the queue-growth RTT floor but keeps the peak instead of the
/// minimum.  The peak is used to compute a per-flow drain floor: a flow is
/// allowed to drain down to a fraction of its own recent peak so a small
/// incumbent is not pinned at the global MIN_SEND_RATE by a competitor's
/// standing queue.
#[derive(Debug, Clone)]
pub(crate) struct WindowedDeliveryMax {
    bucket_start: Instant,
    cur: Option<f64>,
    prev: Option<f64>,
}

impl WindowedDeliveryMax {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            bucket_start: now,
            cur: None,
            prev: None,
        }
    }

    /// The peak currently held, without folding in a fresh sample.
    pub(crate) fn peek(&self) -> Option<f64> {
        match (self.cur, self.prev) {
            (Some(cur), Some(prev)) => Some(cur.max(prev)),
            (Some(cur), None) | (None, Some(cur)) => Some(cur),
            (None, None) => None,
        }
    }

    pub(crate) fn update(&mut self, now: Instant, rate: f64) -> f64 {
        let elapsed = now.duration_since(self.bucket_start);
        if elapsed > DELIVERY_PEAK_BUCKET * 2 {
            // Idle staleness: both buckets have aged out.
            self.cur = None;
            self.prev = None;
            self.bucket_start = now;
        } else if elapsed > DELIVERY_PEAK_BUCKET {
            self.prev = self.cur.take();
            self.bucket_start = now;
        }

        self.cur = Some(match self.cur {
            Some(cur) => cur.max(rate),
            None => rate,
        });

        let candidates = [self.cur, self.prev].into_iter().flatten();
        candidates.fold(rate, f64::max)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::{DELIVERY_PEAK_BUCKET, WindowedDeliveryMax};

    /// `peek` reports the held windowed maximum without folding in a fresh
    /// sample, and tracks the higher of the current and previous buckets.
    #[test]
    fn peek_reports_the_windowed_maximum_without_a_fresh_sample() {
        let t0 = Instant::now();
        let mut window = WindowedDeliveryMax::new(t0);
        assert_eq!(window.peek(), None);

        assert_eq!(window.update(t0, 100.0), 100.0);
        assert_eq!(window.peek(), Some(100.0));

        // A lower sample does not lower the held maximum.
        assert_eq!(window.update(t0 + DELIVERY_PEAK_BUCKET / 2, 40.0), 100.0);
        assert_eq!(window.peek(), Some(100.0));

        // Rolling the window keeps the previous bucket's maximum alongside the
        // current one until the second bucket ages out.
        assert_eq!(
            window.update(t0 + DELIVERY_PEAK_BUCKET + DELIVERY_PEAK_BUCKET / 2, 70.0),
            100.0
        );
        assert_eq!(window.peek(), Some(100.0));
        assert_eq!(window.update(t0 + DELIVERY_PEAK_BUCKET * 3, 25.0), 70.0);
        assert_eq!(window.peek(), Some(70.0));
    }

    /// The double-bucket reset is `elapsed > DELIVERY_PEAK_BUCKET * 2`, STRICT:
    /// at exactly two buckets the previous bucket is still inside the window
    /// (it is moved into `prev`, not discarded), so a peak that is exactly two
    /// buckets old must still be reported.  `DELIVERY_PEAK_BUCKET * 2` is an
    /// exact `Duration` product and the sample instants are exact, so the
    /// boundary carries no float-derived margin.
    #[test]
    fn a_peak_exactly_two_buckets_old_is_still_in_the_window() {
        let t0 = Instant::now();
        let mut window = WindowedDeliveryMax::new(t0);
        assert_eq!(window.update(t0, 100.0), 100.0);
        assert_eq!(
            window.update(t0 + DELIVERY_PEAK_BUCKET * 2, 10.0),
            100.0,
            "a peak exactly two buckets old is still inside the window"
        );
    }

    /// The single-bucket roll is `elapsed > DELIVERY_PEAK_BUCKET`, STRICT: at
    /// exactly one bucket the sample has not rolled, so the peak it joins stays
    /// in the *current* bucket and keeps that bucket's ageing.  A `>=` mutant
    /// rolls one sample early, which moves the stored bucket start forward;
    /// the peak is then displaced into `prev` a whole bucket sooner and has
    /// aged out by the time this test looks two buckets after the first sample.
    #[test]
    fn a_sample_exactly_one_bucket_old_has_not_rolled_yet() {
        let t0 = Instant::now();
        let mut window = WindowedDeliveryMax::new(t0);
        assert_eq!(window.update(t0, 100.0), 100.0);
        assert_eq!(window.update(t0 + DELIVERY_PEAK_BUCKET, 10.0), 100.0);
        assert_eq!(
            window.update(t0 + DELIVERY_PEAK_BUCKET * 2, 10.0),
            100.0,
            "an early roll ages the peak out a whole bucket sooner"
        );
    }
}
