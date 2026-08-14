use std::time::{Duration, Instant};

pub(crate) const RTT_MIN_BUCKET: Duration = Duration::from_secs(5);
pub(crate) const RTT_MIN_BUCKET_RTT_SCALE: u32 = 10;

/// Window over which the delivery-rate peak is tracked.
///
/// Twice RTT_MIN_BUCKET so the peak reflects the recent steady state but ages
/// out after idle gaps, mirroring WindowedRttMin.
const DELIVERY_PEAK_BUCKET: Duration = Duration::from_secs(10);

/// Maximum of a sliding window of delivery-rate samples.
///
/// Mirrors WindowedRttMin but keeps the peak instead of the minimum. The peak
/// is used to compute a per-flow drain floor: a flow is allowed to drain down
/// to a fraction of its own recent peak so a small incumbent is not pinned at
/// the global MIN_SEND_RATE by a competitor's standing queue.
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

/// Minimum of a sliding window of RTT samples.
///
/// RTT rises when a queue builds, but a lifetime `min_rtt` collapses to ~0 on
/// jittery links and never recovers. Instead, keep a short windowed minimum:
/// the floor tracks recent baseline RTT and recovers quickly enough to let the
/// delay-based gate close when the queue inflates and reopen when it drains.
#[derive(Debug, Clone)]
pub(crate) struct WindowedRttMin {
    bucket_start: Instant,
    cur: Option<Duration>,
    prev: Option<Duration>,
}

impl WindowedRttMin {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            bucket_start: now,
            cur: None,
            prev: None,
        }
    }

    pub(crate) fn update(&mut self, now: Instant, rtt: Duration) -> Duration {
        let bucket = RTT_MIN_BUCKET.max(rtt.saturating_mul(RTT_MIN_BUCKET_RTT_SCALE));
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
