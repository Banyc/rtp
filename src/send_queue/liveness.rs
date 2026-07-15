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
    progress_wait: Option<WatchdogWait>,
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
            tuning,
        }
    }

    pub(crate) fn record_progress(&mut self) {
        self.ever_progressed = true;
        self.progress_wait = None;
    }

    pub(crate) fn reset_waits(&mut self) {
        self.resp_wait = None;
        self.progress_wait = None;
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
    }

    pub(crate) fn no_resp_for(&self, now: Instant) -> Option<Duration> {
        self.resp_wait.map(|wait| wait.elapsed(now))
    }

    pub(crate) fn no_progress_for(&self, now: Instant) -> Option<Duration> {
        self.progress_wait.map(|wait| wait.elapsed(now))
    }

    pub(crate) fn stall_reason(&self, now: Instant, has_in_flight: bool) -> Option<PeerStall> {
        let response_dead = self.resp_wait.is_some_and(|wait| wait.expired(now));
        let progress_dead =
            has_in_flight && self.progress_wait.is_some_and(|wait| wait.expired(now));
        if response_dead {
            Some(PeerStall::NoResponse)
        } else if progress_dead {
            Some(PeerStall::NoProgress)
        } else {
            None
        }
    }

    pub(crate) fn next_deadline(&self, has_in_flight: bool) -> Option<Instant> {
        let response = self.resp_wait.map(|wait| wait.deadline());
        let progress = has_in_flight
            .then(|| self.progress_wait.map(|w| w.deadline()))
            .flatten();
        response.into_iter().chain(progress).min()
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
        liveness.refresh_waits(now - Duration::from_secs(1), rto);
        assert!(
            !liveness.should_terminate_session(now + Duration::from_secs(2), true),
            "first cumulative progress must replace the old progress deadline"
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
}
