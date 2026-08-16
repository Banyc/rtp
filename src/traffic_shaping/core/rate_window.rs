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
