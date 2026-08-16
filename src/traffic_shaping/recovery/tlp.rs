use core::time::Duration;
use std::time::Instant;

use super::rtt_stats::RttStats;

/// RFC 8985 tail-loss-probe (TLP / PTO) engine.
///
/// When the send window has unacked data and the tail packet has not been ACKed
/// within `probe_window()`, a single-packet probe retransmission is fired. Up to
/// `MAX_PROBES` probes are sent per tail episode; the budget resets when an ACK
/// arrives or a new packet is pushed.
#[derive(Debug)]
pub struct TailLossProber {
    probes_sent: u8,
}

impl TailLossProber {
    /// Maximum number of tail-loss probes per tail episode.
    const MAX_PROBES: u8 = 2;
    /// Floor for the PTO timer so the first sample is not taken before the peer
    /// has had a reasonable chance to acknowledge the tail packet.
    const MIN_TOL: Duration = Duration::from_millis(10);
    /// Lowered RTO floor used after at least one tail-loss probe has been sent.
    ///
    /// The first timeout signal keeps the 1 s `MIN_RTO` floor to absorb
    /// estimator error; once a tail probe has been sent the timeout is
    /// corroborated and can safely be tightened to 300 ms.
    const TAIL_PROBED_MIN_RTO: Duration = Duration::from_millis(300);

    pub fn new() -> Self {
        Self { probes_sent: 0 }
    }

    /// Reset the probe budget (called on ACK progress or new packet push).
    pub fn reset(&mut self) {
        self.probes_sent = 0;
    }

    /// Whether the probe budget still has room.
    pub fn can_probe(&self) -> bool {
        self.probes_sent < Self::MAX_PROBES
    }

    /// RTO for the current tail episode.
    ///
    /// Before any tail-loss probe has been sent, uses the standard `MIN_RTO`
    /// floor (1 s). After a probe has been sent, tightens to
    /// `TAIL_PROBED_MIN_RTO` (300 ms) so subsequent recovery is faster.
    pub fn rto(&self, rtt_stats: &RttStats) -> Duration {
        if self.probes_sent == 0 {
            return rtt_stats.rto_duration();
        }
        rtt_stats.raw_rto().max(Self::TAIL_PROBED_MIN_RTO)
    }

    /// Time between consecutive tail-loss probes for the current tail episode.
    /// The PTO formula: `max(2*srtt, 2*min_rtt)` with a 10 ms floor, capped at
    /// the RTO currently in use.  The doubled terms use checked multiplication
    /// so a sub-nanosecond RTT cannot round the doubling away.
    pub fn probe_window(&self, rtt_stats: &RttStats) -> Duration {
        let srtt = rtt_stats.smooth_rtt();
        self.probe_window_with_srtt(rtt_stats, srtt)
    }

    fn probe_window_with_srtt(&self, rtt_stats: &RttStats, srtt: Duration) -> Duration {
        debug_assert!(rtt_stats.min_rtt().is_none_or(|min_rtt| min_rtt <= srtt));
        // min_rtt is the lifetime minimum of the samples feeding srtt, so
        // 2 * min_rtt can never exceed 2 * srtt. Keep the formula's dominant
        // term without doubling and comparing both on every poll.
        let doubled_srtt = srtt
            .checked_mul(2)
            .expect("smoothed RTT must fit when doubled for the probe window");
        let cap = self.rto(rtt_stats);
        doubled_srtt.max(Self::MIN_TOL).min(cap)
    }

    /// Whether enough time has passed since `sent_time` for a probe to fire.
    pub fn is_due(&self, sent_time: Instant, rtt_stats: &RttStats, now: Instant) -> bool {
        let sent_elapsed = now.duration_since(sent_time);
        self.probe_window(rtt_stats) <= sent_elapsed
    }

    /// Record that a probe was sent, consuming one budget slot.
    pub fn sent(&mut self) {
        self.probes_sent += 1;
    }

    /// Merge the next probe time into an already-known wake deadline.
    /// A probe cannot precede `sent_time + MIN_TOL`. When another source is
    /// already due by that lower bound, keep it without calculating the RTT-
    /// derived probe window.
    pub fn merge_next_probe_time(
        &self,
        sent_time: Instant,
        rtt_stats: &RttStats,
        current: Option<Instant>,
    ) -> Option<Instant> {
        if !self.can_probe() {
            return current;
        }
        let srtt = rtt_stats.smooth_rtt();
        // Both RTO caps are at least sRTT, while the uncapped probe window is
        // at least max(sRTT, MIN_TOL). Therefore no probe can precede this
        // lower bound. If another wake already wins, avoid the doubled-sRTT
        // and RTO-cap calculation entirely.
        let earliest_probe = sent_time + srtt.max(Self::MIN_TOL);
        if current.is_some_and(|deadline| deadline <= earliest_probe) {
            return current;
        }
        let probe = sent_time + self.probe_window_with_srtt(rtt_stats, srtt);
        Some(current.map_or(probe, |deadline| deadline.min(probe)))
    }

    #[cfg(test)]
    pub(crate) fn probes_sent(&self) -> u8 {
        self.probes_sent
    }
}

impl Default for TailLossProber {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::traffic_shaping::recovery::rtt_stats::RttStats;

    use super::TailLossProber;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn settled_rtt_stats() -> RttStats {
        let mut stats = RttStats::new();
        for i in 0..20 {
            stats.record_rtt(ms(100 + i));
        }
        stats
    }

    #[test]
    fn pre_probe_rto_uses_1s_floor() {
        let rtt_stats = settled_rtt_stats();
        let tlp = TailLossProber::new();
        let r = tlp.rto(&rtt_stats);
        assert!(r >= Duration::from_secs(1), "r={r:?}");
    }

    #[test]
    fn post_probe_rto_uses_300ms_floor() {
        let rtt_stats = settled_rtt_stats();
        let mut tlp = TailLossProber::new();
        tlp.sent();
        let r = tlp.rto(&rtt_stats);
        assert!(
            r >= ms(300) && r < ms(500),
            "post-probe RTO should be 300-500ms, got {r:?}"
        );
    }

    #[test]
    fn post_probe_rto_unchanged_on_jittery_link() {
        let mut stats = RttStats::new();
        for _ in 0..10 {
            stats.record_rtt(ms(100));
            stats.record_rtt(ms(900));
        }
        let mut tlp = TailLossProber::new();
        let first = tlp.rto(&stats);
        tlp.sent();
        let post = tlp.rto(&stats);
        assert_eq!(
            first, post,
            "jitter-dominated RTO must not change after tail probe"
        );
    }

    #[test]
    fn probe_window_fires_between_2srtt_and_rto() {
        let rtt_stats = settled_rtt_stats();
        let tlp = TailLossProber::new();
        let sent = Instant::now();
        let window = tlp.probe_window(&rtt_stats);

        // 2*srtt ~= 200ms, should be at least that.
        assert!(window >= ms(10), "window={window:?}");
        // Must not exceed the 1s RTO.
        assert!(window <= Duration::from_secs(1), "window={window:?}");

        let just_before = sent + window - ms(1);
        assert!(!tlp.is_due(sent, &rtt_stats, just_before));

        let just_after = sent + window + ms(1);
        assert!(tlp.is_due(sent, &rtt_stats, just_after));
    }

    #[test]
    fn probe_window_doubles_rtt_without_fractional_rounding() {
        let mut rtt_stats = RttStats::new();
        let rtt = Duration::from_millis(10) + Duration::from_nanos(1);
        rtt_stats.record_rtt(rtt);
        let tlp = TailLossProber::new();
        assert_eq!(tlp.probe_window(&rtt_stats), rtt.checked_mul(2).unwrap());
    }

    #[test]
    fn lifetime_minimum_is_dominated_across_varying_rtt_samples() {
        let mut rtt_stats = RttStats::new();
        let tlp = TailLossProber::new();

        for rtt in [ms(900), ms(100), ms(700), ms(250), ms(500)] {
            rtt_stats.record_rtt(rtt);
            let srtt = rtt_stats.smooth_rtt();
            let min_rtt = rtt_stats.min_rtt().unwrap();
            assert!(min_rtt <= srtt);
            let old_formula = srtt
                .checked_mul(2)
                .unwrap()
                .max(min_rtt.checked_mul(2).unwrap())
                .max(TailLossProber::MIN_TOL)
                .min(tlp.rto(&rtt_stats));
            assert_eq!(tlp.probe_window(&rtt_stats), old_formula);
        }
    }

    #[test]
    fn budget_exhausted_after_max_probes() {
        let mut tlp = TailLossProber::new();
        assert!(tlp.can_probe());
        tlp.sent();
        assert!(tlp.can_probe());
        tlp.sent();
        assert!(!tlp.can_probe());
        // The merge contributes no deadline when the budget is exhausted.
        assert!(
            tlp.merge_next_probe_time(Instant::now(), &settled_rtt_stats(), None)
                .is_none()
        );
    }

    #[test]
    fn earlier_deadline_at_probe_lower_bound_wins_without_changing_it() {
        let tlp = TailLossProber::new();
        let sent = Instant::now();
        let rtt_stats = settled_rtt_stats();
        let current = sent + rtt_stats.smooth_rtt();
        assert!(current < sent + tlp.probe_window(&rtt_stats));
        assert_eq!(
            tlp.merge_next_probe_time(sent, &rtt_stats, Some(current)),
            Some(current)
        );
    }

    #[test]
    fn reset_restores_budget() {
        let mut tlp = TailLossProber::new();
        tlp.sent();
        tlp.sent();
        assert!(!tlp.can_probe());
        tlp.reset();
        assert!(tlp.can_probe());
        assert_eq!(tlp.probes_sent(), 0);
    }
}
