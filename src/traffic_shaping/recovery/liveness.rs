use std::time::{Duration, Instant};

use crate::transmission::watchdog_tuning::WatchdogTuning;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStall {
    NoResponse,
    NoProgress,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WatchdogWait {
    started_at: Instant,
    deadline: Instant,
}

impl WatchdogWait {
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    fn new(started_at: Instant, rto: Duration, floor: Duration, tuning: WatchdogTuning) -> Self {
        let timeout = (rto * tuning.rto_multiplier)
            .max(floor)
            .min(tuning.max_timeout);
        Self {
            started_at,
            deadline: started_at + timeout,
        }
    }

    /// The hard front-stall wall: a *fixed* timeout independent of the live
    /// RTO, capped at the tuning maximum.  This is what bounds a session
    /// whose cumulative delivery front stands still while the peer keeps
    /// delivering newer (out-of-order) packets: the delivery staleness
    /// watchdog may be reset forever by those SACKs, so the front-stall wall
    /// is the only thing that guarantees a truly unrecoverable head-of-line
    /// hole still terminates the session, in bounded time.
    fn front_stall(started_at: Instant, max_timeout: Duration) -> Self {
        Self {
            started_at,
            deadline: started_at + max_timeout,
        }
    }

    fn elapsed(self, now: Instant) -> Duration {
        now.duration_since(self.started_at)
    }

    fn expired(self, now: Instant) -> bool {
        now >= self.deadline
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PeerLiveness {
    resp_wait: Option<WatchdogWait>,
    pub(crate) ever_progressed: bool,
    /// Delivery-staleness watchdog: reset by ANY freshly delivered packet —
    /// cumulative or out-of-order SACK — because either proves the peer is
    /// alive and consuming data.  Expiring with packets in flight means the
    /// peer stopped acknowledging anything at all.
    progress_wait: Option<WatchdogWait>,
    /// Front-stall wall: reset ONLY by a cumulative advance (the delivery
    /// front physically moving).  Out-of-order delivery does not reset it, so
    /// a session whose front is stuck behind a hole — however much newer data
    /// the peer keeps SACKing — is still terminated once the front has stood
    /// still for `max_timeout`.  This keeps the truly-unrecoverable-hole case
    /// bounded without letting live out-of-order delivery masquerade as a
    /// stall.
    cumulative_wait: Option<WatchdogWait>,
    tuning: WatchdogTuning,
}

impl PeerLiveness {
    pub(crate) fn new() -> Self {
        Self::with_tuning(WatchdogTuning::default())
    }

    pub(crate) fn with_tuning(tuning: WatchdogTuning) -> Self {
        Self {
            resp_wait: None,
            ever_progressed: false,
            progress_wait: None,
            cumulative_wait: None,
            tuning,
        }
    }

    /// Record delivery progress: a packet was freshly acknowledged, in order
    /// or out of order.  The peer is demonstrably alive and consuming data,
    /// so the delivery-staleness watchdog restarts.  The front-stall wall is
    /// deliberately NOT restarted — out-of-order delivery does not move the
    /// cumulative front.
    pub(crate) fn record_progress(&mut self) {
        self.ever_progressed = true;
        self.progress_wait = None;
    }

    /// Record a cumulative advance: the delivery front physically moved.
    /// Restarts the front-stall wall so an in-flight session must keep
    /// advancing its front at least once per `max_timeout`, no matter how
    /// much out-of-order delivery is flowing.
    pub(crate) fn record_cumulative_advance(&mut self, now: Instant) {
        self.cumulative_wait = Some(WatchdogWait::front_stall(now, self.tuning.max_timeout));
    }

    pub(crate) fn reset_waits(&mut self) {
        self.resp_wait = None;
        self.progress_wait = None;
        self.cumulative_wait = None;
    }

    pub(crate) fn refresh_waits(&mut self, now: Instant, rto: Duration) {
        self.resp_wait = Some(WatchdogWait::new(
            now,
            rto,
            self.tuning.min_no_response,
            self.tuning,
        ));
        if self.progress_wait.is_none() {
            self.progress_wait = Some(WatchdogWait::new(
                now,
                rto,
                self.tuning.min_no_progress,
                self.tuning,
            ));
        }
        if self.cumulative_wait.is_none() {
            self.cumulative_wait = Some(WatchdogWait::front_stall(now, self.tuning.max_timeout));
        }
    }

    pub(crate) fn on_send(&mut self, now: Instant, rto: Duration) {
        if self.resp_wait.is_none() {
            self.resp_wait = Some(WatchdogWait::new(
                now,
                rto,
                self.tuning.min_no_response,
                self.tuning,
            ));
        }
        if self.progress_wait.is_none() {
            self.progress_wait = Some(WatchdogWait::new(
                now,
                rto,
                self.tuning.min_no_progress,
                self.tuning,
            ));
        }
        if self.cumulative_wait.is_none() {
            self.cumulative_wait = Some(WatchdogWait::front_stall(now, self.tuning.max_timeout));
        }
    }

    pub(crate) fn no_resp_for(&self, now: Instant) -> Option<Duration> {
        self.resp_wait.map(|wait| wait.elapsed(now))
    }

    /// How long the session has been stalled according to whichever progress
    /// watchdog last moved: the older of the delivery-staleness wait and the
    /// front-stall wall.  This is the metric that drives outage detection and
    /// the `no_progress_for_ms` termination diagnostic.
    pub(crate) fn no_progress_for(&self, now: Instant) -> Option<Duration> {
        [self.progress_wait, self.cumulative_wait]
            .into_iter()
            .flatten()
            .map(|wait| wait.elapsed(now))
            .max()
    }

    pub(crate) fn stall_reason(&self, now: Instant, has_in_flight: bool) -> Option<PeerStall> {
        // The dead-peer watchdog is strict and independent of delivery: peer
        // stopped acknowledging AT ALL.
        if self.resp_wait.is_some_and(|wait| wait.expired(now)) {
            return Some(PeerStall::NoResponse);
        }
        if !has_in_flight {
            return None;
        }
        let progress_dead = self.progress_wait.is_some_and(|wait| wait.expired(now));
        let front_stalled = self.cumulative_wait.is_some_and(|wait| wait.expired(now));
        (progress_dead || front_stalled).then_some(PeerStall::NoProgress)
    }

    pub(crate) fn next_deadline(&self, has_in_flight: bool) -> Option<Instant> {
        let response = self.resp_wait.map(|wait| wait.deadline());
        let progress = has_in_flight
            .then(|| self.progress_wait.map(|w| w.deadline()))
            .flatten();
        let front = has_in_flight
            .then(|| self.cumulative_wait.map(|w| w.deadline()))
            .flatten();
        response.into_iter().chain(progress).chain(front).min()
    }

    #[cfg(test)]
    pub(crate) fn should_terminate_session(&self, now: Instant, has_in_flight: bool) -> bool {
        self.stall_reason(now, has_in_flight).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn max_watchdog_timeout() -> Duration {
        WatchdogTuning::default().max_timeout
    }
    fn min_no_resp_for() -> Duration {
        WatchdogTuning::default().min_no_response
    }

    #[test]
    fn watchdog_deadline_is_latched_and_capped() {
        let max_wd = max_watchdog_timeout();
        let now = Instant::now();

        let mut latched = PeerLiveness::new();
        latched.on_send(now, Duration::from_millis(100));
        latched.on_send(now + Duration::from_secs(1), Duration::from_secs(30));
        assert!(latched.should_terminate_session(now + Duration::from_secs(47), true));
        assert!(latched.should_terminate_session(now + Duration::from_secs(47), false));
        assert!(latched.should_terminate_session(now + max_wd, true));

        let mut capped = PeerLiveness::new();
        capped.on_send(now, Duration::from_secs(30));
        assert!(!capped.should_terminate_session(now + Duration::from_secs(47), true));
        assert!(capped.should_terminate_session(now + max_wd + Duration::from_secs(1), true,));
    }

    #[test]
    fn response_watchdog_uses_rto_and_minimum_floor() {
        let min_resp = min_no_resp_for();
        let mut l = PeerLiveness::new();
        let rto = Duration::from_millis(100);
        let now = Instant::now();
        l.on_send(now - min_resp - Duration::from_millis(1), rto);
        assert!(l.should_terminate_session(now, false));
        let mut l2 = PeerLiveness::new();
        l2.on_send(now - min_resp + Duration::from_millis(10), rto);
        assert!(!l2.should_terminate_session(now, false));
        let mut l3 = PeerLiveness::new();
        l3.on_send(now - min_resp - Duration::from_millis(1), rto);
        assert!(
            l3.should_terminate_session(now, false),
            "floor enforces 30s minimum"
        );
    }

    #[test]
    fn progress_watchdog_requires_packets_in_flight() {
        let mut l = PeerLiveness::new();
        let rto = Duration::from_millis(100);
        let now = Instant::now();
        l.record_progress();
        l.on_send(now - Duration::from_secs(31), rto);
        l.refresh_waits(now - Duration::from_millis(1), rto);
        assert!(
            !l.should_terminate_session(now, false),
            "no in-flight => progress watchdog must stay silent"
        );
        assert!(
            l.should_terminate_session(now, true),
            "in-flight + stale progress => terminate"
        );
    }

    #[test]
    fn first_cumulative_progress_restarts_watchdog() {
        let now = Instant::now();
        let rto = Duration::from_millis(100);
        let mut liveness = PeerLiveness::new();
        liveness.on_send(now - Duration::from_secs(29), rto);
        liveness.record_progress();
        liveness.record_cumulative_advance(now);
        liveness.refresh_waits(now - Duration::from_secs(1), rto);
        assert!(
            !liveness.should_terminate_session(now + Duration::from_secs(2), true),
            "first cumulative progress must replace the old progress deadline"
        );
        let max_wd = max_watchdog_timeout();
        assert!(
            liveness.should_terminate_session(now + max_wd + Duration::from_secs(1), true),
            "the front-stall wall must still fire when the front stops moving"
        );
    }

    #[test]
    fn stall_reason_distinguishes_response_and_progress_watchdogs() {
        let now = Instant::now();
        let rto = Duration::from_millis(100);
        let mut no_response = PeerLiveness::new();
        no_response.on_send(now - Duration::from_secs(31), rto);
        assert_eq!(
            no_response.stall_reason(now, false),
            Some(PeerStall::NoResponse)
        );
        let mut no_progress = PeerLiveness::new();
        no_progress.record_progress();
        no_progress.on_send(now - Duration::from_secs(31), rto);
        no_progress.refresh_waits(now - Duration::from_secs(31), rto);
        no_progress.resp_wait = None;
        assert_eq!(
            no_progress.stall_reason(now, true),
            Some(PeerStall::NoProgress)
        );
    }

    #[test]
    fn out_of_order_delivery_keeps_session_alive_while_front_is_stuck() {
        let max_wd = max_watchdog_timeout();
        let rto = Duration::from_millis(100);
        let now = Instant::now();
        let mut liveness = PeerLiveness::new();
        liveness.on_send(now, rto);

        // The delivery front advances once, then stands still while the peer
        // keeps delivering newer (out-of-order) packets for far longer than
        // the 30s delivery-staleness floor.
        liveness.record_cumulative_advance(now + Duration::from_secs(1));
        let mut t = now + Duration::from_secs(2);
        while t < now + Duration::from_secs(100) {
            // Each delivery is a fresh ack: it resets the staleness watchdog
            // (and, in the ack path, refreshes the response wait).
            liveness.record_progress();
            liveness.refresh_waits(t, rto);
            assert!(
                !liveness.should_terminate_session(t, true),
                "live out-of-order delivery must not trigger proactive_stall"
            );
            assert_eq!(
                liveness.stall_reason(t, true),
                None,
                "a delivering peer is neither dead nor stalled"
            );
            t += Duration::from_secs(5);
        }

        // The front-stall wall finally expires: the session must still
        // terminate, in bounded time (max_timeout from the last cumulative
        // advance), even though delivery kept flowing.
        let wall_deadline = now + Duration::from_secs(1) + max_wd;
        assert!(
            !liveness.should_terminate_session(wall_deadline - Duration::from_secs(1), true),
            "session must survive until the front-stall wall"
        );
        assert!(
            liveness.should_terminate_session(wall_deadline + Duration::from_secs(1), true),
            "an unrecoverable front with live delivery must still terminate at the wall"
        );
    }

    #[test]
    fn cumulative_advance_restarts_the_front_stall_wall() {
        let max_wd = max_watchdog_timeout();
        let rto = Duration::from_millis(100);
        let now = Instant::now();
        let mut liveness = PeerLiveness::new();
        liveness.on_send(now, rto);
        liveness.record_cumulative_advance(now);

        // A session that keeps moving its front every max_wd/2 survives
        // indefinitely.
        for i in 0..4 {
            let t = now + max_wd / 2 * (i + 1);
            liveness.record_progress();
            liveness.record_cumulative_advance(t);
            liveness.refresh_waits(t, rto);
            assert!(
                !liveness.should_terminate_session(t + Duration::from_secs(1), true),
                "advancing front must keep the wall armed"
            );
        }
    }

    #[test]
    fn dead_peer_no_response_terminates_promptly_regardless_of_wall() {
        let min_resp = min_no_resp_for();
        let rto = Duration::from_millis(100);
        let now = Instant::now();
        let mut liveness = PeerLiveness::new();
        liveness.on_send(now, rto);

        // No response of any kind: the dead-peer watchdog fires at
        // min_no_response, far earlier than the front-stall wall.
        assert!(
            !liveness.should_terminate_session(now + min_resp - Duration::from_secs(1), true),
            "dead-peer bound must not fire early"
        );
        assert!(
            liveness.should_terminate_session(now + min_resp + Duration::from_secs(1), true),
            "a truly silent peer must still be terminated promptly"
        );
        assert_eq!(
            liveness.stall_reason(now + min_resp + Duration::from_secs(1), true),
            Some(PeerStall::NoResponse),
            "silence must be classified as a dead peer, not a stall"
        );
    }
}
