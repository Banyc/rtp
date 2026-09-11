use std::collections::VecDeque;
use std::time::Duration;

use super::rto::RtxTimer;

/// Number of most recent RTT samples spanned by the rolling minimum — the
/// current-path floor used as the observed-reordering suspicion window, so a
/// single early outlier (or a stale lifetime minimum recorded under
/// congestion) cannot pin that window for the connection's life.
const RECENT_MIN_RTT_SAMPLES: usize = 8;

/// Bundle of RTT statistics: the smoothed-RTT / RTO timer, the lifetime
/// minimum RTT, and a rolling minimum over the most recent samples.
#[derive(Debug)]
pub(crate) struct RttStats {
    min_rtt: Option<Duration>,
    /// Minimum RTT over only the most recent [`RECENT_MIN_RTT_SAMPLES`]
    /// samples — the *current* estimator floor.  Unlike the lifetime
    /// `min_rtt` it recovers when the path improves (or an early outlier ages
    /// out), so the reorder-suspicion check does not stay pinned by stale
    /// history.
    recent_min_rtt: Option<Duration>,
    /// The most recent [`RECENT_MIN_RTT_SAMPLES`] RTT samples, oldest first.
    recent_rtt_samples: VecDeque<Duration>,
    rto: RtxTimer,
}

impl RttStats {
    pub(crate) fn new() -> Self {
        Self {
            min_rtt: None,
            recent_min_rtt: None,
            recent_rtt_samples: VecDeque::new(),
            rto: RtxTimer::new(),
        }
    }

    /// Maintain the rolling minimum from the most recent samples.
    fn refresh_recent_min_rtt(&mut self) {
        self.recent_min_rtt = Some(
            self.recent_rtt_samples
                .iter()
                .copied()
                .min()
                .expect("the sample ring is never empty while refreshing"),
        );
    }

    /// Record an RTT sample: update the SRTT filter, the lifetime minimum,
    /// and the rolling recent minimum.
    pub(crate) fn record_rtt(&mut self, rtt: Duration) {
        self.rto.set(rtt);
        self.min_rtt = Some(match self.min_rtt {
            Some(m) => m.min(rtt),
            None => rtt,
        });
        self.recent_rtt_samples.push_back(rtt);
        if self.recent_rtt_samples.len() > RECENT_MIN_RTT_SAMPLES {
            self.recent_rtt_samples.pop_front();
        }
        self.refresh_recent_min_rtt();
    }

    /// Record the lifetime minimum and re-seed the SRTT filter from scratch.
    ///
    /// Used when an outage-recovery epoch closes: the fresh post-outage RTT
    /// reflects a new network state, so we must not merge it with the stale
    /// pre-outage EWMA via `record_rtt`.  The rolling recent minimum is
    /// rebuilt from that fresh sample for the same reason.
    pub(crate) fn record_min_and_reseed_rto(&mut self, rtt: Duration) {
        self.min_rtt = Some(match self.min_rtt {
            Some(m) => m.min(rtt),
            None => rtt,
        });
        self.rto.reset_to(rtt);
        self.recent_rtt_samples.clear();
        self.recent_rtt_samples.push_back(rtt);
        self.recent_min_rtt = Some(rtt);
    }

    pub(crate) fn min_rtt(&self) -> Option<Duration> {
        self.min_rtt
    }

    /// The *current* estimator floor for the reorder-suspicion check: the
    /// minimum over the most recent [`RECENT_MIN_RTT_SAMPLES`] samples.
    /// `None` before any sample exists (the check then abstains).
    pub(crate) fn recent_min_rtt(&self) -> Option<Duration> {
        self.recent_min_rtt
    }

    pub(crate) fn smooth_rtt(&self) -> Duration {
        self.rto.smooth_rtt()
    }

    pub(crate) fn smooth_rtt_var(&self) -> Duration {
        self.rto.smooth_rtt_var()
    }

    pub(crate) fn rto_duration(&self) -> Duration {
        self.rto.rto()
    }

    pub(crate) fn raw_rto(&self) -> Duration {
        self.rto.raw_rto()
    }

    pub(crate) fn reorder_window(&self) -> Duration {
        self.rto.reorder_window()
    }

    /// Fast reorder window used only to schedule retransmission when the
    /// `RTP_JITTER_CAP` toggle is on (see [`RtxTimer::fast_reorder_window`]).
    pub(crate) fn fast_reorder_window(&self) -> Duration {
        self.rto.fast_reorder_window()
    }

    /// Whether the structural low-jitter gate for evidence-gated fast loss
    /// is armed (see [`RtxTimer::fast_loss_armed`]).
    pub(crate) fn fast_loss_armed(&self) -> bool {
        self.rto.fast_loss_armed()
    }

    pub(crate) fn reset_rto(&mut self, rtt: Duration) {
        self.rto.reset_to(rtt);
    }
}

impl Default for RttStats {
    fn default() -> Self {
        Self::new()
    }
}
