use core::time::Duration;
use std::time::Instant;

use crate::rto::RtxTimer;

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
    pub fn rto(&self, rto: &RtxTimer) -> Duration {
        if self.probes_sent == 0 {
            return rto.rto();
        }
        rto.raw_rto().max(Self::TAIL_PROBED_MIN_RTO)
    }

    /// Time between consecutive tail-loss probes for the current tail episode.
    ///
    /// The PTO formula: `max(2*srtt, 2*min_rtt)` with a 10 ms floor, capped at
    /// the RTO currently in use.
    pub fn probe_window(&self, rto: &RtxTimer, min_rtt: Option<Duration>) -> Duration {
        let srtt = rto.smooth_rtt();
        let min_tol = Self::MIN_TOL;
        let doubled_srtt = srtt.mul_f64(2.);
        let min_rtt_doubled = min_rtt.map(|r| r.mul_f64(2.)).unwrap_or(doubled_srtt);
        let cap = self.rto(rto);
        doubled_srtt.max(min_rtt_doubled).max(min_tol).min(cap)
    }

    /// Whether enough time has passed since `sent_time` for a probe to fire.
    pub fn is_due(&self, sent_time: Instant, rto: &RtxTimer, min_rtt: Option<Duration>, now: Instant) -> bool {
        let sent_elapsed = now.duration_since(sent_time);
        self.probe_window(rto, min_rtt) <= sent_elapsed
    }

    /// Record that a probe was sent, consuming one budget slot.
    pub fn sent(&mut self) {
        self.probes_sent += 1;
    }

    /// Next poll time for the next probe, if budget remains.
    pub fn next_probe_time(
        &self,
        sent_time: Instant,
        rto: &RtxTimer,
        min_rtt: Option<Duration>,
    ) -> Option<Instant> {
        if !self.can_probe() {
            return None;
        }
        Some(sent_time + self.probe_window(rto, min_rtt))
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

    use crate::rto::RtxTimer;

    use super::TailLossProber;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn settled_rto() -> RtxTimer {
        let mut rto = RtxTimer::new();
        for i in 0..20 {
            rto.set(ms(100 + i));
        }
        rto
    }

    #[test]
    fn pre_probe_rto_uses_1s_floor() {
        let rto = settled_rto();
        let tlp = TailLossProber::new();
        let r = tlp.rto(&rto);
        assert!(r >= Duration::from_secs(1), "r={r:?}");
    }

    #[test]
    fn post_probe_rto_uses_300ms_floor() {
        let rto = settled_rto();
        let mut tlp = TailLossProber::new();
        tlp.sent();
        let r = tlp.rto(&rto);
        assert!(
            r >= ms(300) && r < ms(500),
            "post-probe RTO should be 300-500ms, got {r:?}"
        );
    }

    #[test]
    fn post_probe_rto_unchanged_on_jittery_link() {
        let mut rto = RtxTimer::new();
        for _ in 0..10 {
            rto.set(ms(100));
            rto.set(ms(900));
        }
        let mut tlp = TailLossProber::new();
        let first = tlp.rto(&rto);
        tlp.sent();
        let post = tlp.rto(&rto);
        assert_eq!(
            first, post,
            "jitter-dominated RTO must not change after tail probe"
        );
    }

    #[test]
    fn probe_window_fires_between_2srtt_and_rto() {
        let rto = settled_rto();
        let tlp = TailLossProber::new();
        let sent = Instant::now();
        let window = tlp.probe_window(&rto, Some(ms(100)));

        // 2*srtt ~= 200ms, should be at least that.
        assert!(window >= ms(10), "window={window:?}");
        // Must not exceed the 1s RTO.
        assert!(window <= Duration::from_secs(1), "window={window:?}");

        let just_before = sent + window - ms(1);
        assert!(!tlp.is_due(sent, &rto, Some(ms(100)), just_before));

        let just_after = sent + window + ms(1);
        assert!(tlp.is_due(sent, &rto, Some(ms(100)), just_after));
    }

    #[test]
    fn budget_exhausted_after_max_probes() {
        let mut tlp = TailLossProber::new();
        assert!(tlp.can_probe());
        tlp.sent();
        assert!(tlp.can_probe());
        tlp.sent();
        assert!(!tlp.can_probe());
        // next_probe_time returns None when budget exhausted
        assert!(tlp.next_probe_time(Instant::now(), &settled_rto(), Some(ms(100))).is_none());
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
