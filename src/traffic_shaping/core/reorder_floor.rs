//! Policy selection for the RTT-floor bucket.
//!
//! [`QueueGrowth`](super::QueueGrowth) keeps a windowed minimum of the smoothed
//! RTT as the path's propagation floor.  The lane that opted into out-of-order
//! delivery (the interactive frame fast-forward lane) holds that floor
//! differently from the default lanes, because a reordered packet's echoed send
//! timestamp can take a faster path and produce an isolated low RTT sample.
//! This module owns that lane-dependent bucket choice so the two buckets and the
//! lane that selects each cannot drift apart.
//!
//! The window itself (the windowed minimum that consumes the bucket) lives with
//! `QueueGrowth`; this module only decides which bucket it uses.

use std::time::Duration;

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

/// The RTT-floor bucket a lane's windowed minimum uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FloorBucket {
    pub(crate) min_bucket: Duration,
    /// Scale the bucket by the *established* floor instead of the incoming
    /// sample. Used on the reorder-tolerant lane: a path shift inflates the
    /// incoming smoothed RTT, and scaling the window by that inflated value
    /// would keep the pre-shift floor alive for proportionally longer — the
    /// longer the new RTT, the longer the stale floor pins the delay gate.
    /// Scaling by the established floor keeps the window at the baseline's
    /// timescale until the floor itself tracks the shift.
    pub(crate) baseline_scaled: bool,
}

/// Select the RTT-floor bucket for a connection's declared lane.
pub(crate) fn floor_bucket(reorder_tolerant: bool) -> FloorBucket {
    if reorder_tolerant {
        FloorBucket {
            min_bucket: RTT_MIN_BUCKET_REORDER,
            baseline_scaled: true,
        }
    } else {
        FloorBucket {
            min_bucket: RTT_MIN_BUCKET,
            baseline_scaled: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reorder-tolerant lane uses the short, baseline-scaled bucket; every
    /// other lane keeps the long, sample-scaled default.
    #[test]
    fn reorder_tolerant_lane_selects_the_short_baseline_scaled_bucket() {
        let default = floor_bucket(false);
        assert_eq!(default.min_bucket, RTT_MIN_BUCKET);
        assert!(!default.baseline_scaled);

        let reorder = floor_bucket(true);
        assert_eq!(reorder.min_bucket, RTT_MIN_BUCKET_REORDER);
        assert!(reorder.baseline_scaled);
        assert!(reorder.min_bucket < default.min_bucket);
    }
}
