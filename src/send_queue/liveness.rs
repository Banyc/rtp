use std::time::{Duration, Instant};

pub(crate) const RTO_WATCHDOG_MULTIPLIER: f64 = 16.0;
pub(crate) const MIN_NO_RESP_FOR: Duration = Duration::from_secs(30);
pub(crate) const MIN_NO_PROGRESS_FOR: Duration = Duration::from_secs(30);
pub(crate) const MAX_WATCHDOG_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Copy)]
pub(crate) struct WatchdogWait {
    started_at: Instant,
    deadline: Instant,
}

impl WatchdogWait {
    fn new(started_at: Instant, rto: Duration, floor: Duration) -> Self {
        let timeout = rto
            .mul_f64(RTO_WATCHDOG_MULTIPLIER)
            .max(floor)
            .min(MAX_WATCHDOG_TIMEOUT);
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
}

impl PeerLiveness {
    pub(crate) fn new() -> Self {
        Self {
            resp_wait: None,
            ever_progressed: false,
            progress_wait: None,
        }
    }

    pub(crate) fn record_progress(&mut self) {
        if self.ever_progressed {
            self.progress_wait = None;
        } else {
            self.ever_progressed = true;
        }
    }

    pub(crate) fn reset_waits(&mut self) {
        self.resp_wait = None;
        self.progress_wait = None;
    }

    pub(crate) fn refresh_waits(&mut self, now: Instant, rto: Duration) {
        self.resp_wait = Some(WatchdogWait::new(now, rto, MIN_NO_RESP_FOR));
        if self.progress_wait.is_none() {
            self.progress_wait = Some(WatchdogWait::new(now, rto, MIN_NO_PROGRESS_FOR));
        }
    }

    pub(crate) fn on_send(&mut self, now: Instant, rto: Duration) {
        if self.resp_wait.is_none() {
            self.resp_wait = Some(WatchdogWait::new(now, rto, MIN_NO_RESP_FOR));
        }
        if self.progress_wait.is_none() {
            self.progress_wait = Some(WatchdogWait::new(now, rto, MIN_NO_PROGRESS_FOR));
        }
    }

    pub(crate) fn no_resp_for(&self, now: Instant) -> Option<Duration> {
        self.resp_wait.map(|wait| wait.elapsed(now))
    }

    pub(crate) fn no_progress_for(&self, now: Instant) -> Option<Duration> {
        self.progress_wait.map(|wait| wait.elapsed(now))
    }

    pub(crate) fn should_terminate_session(&self, now: Instant, has_in_flight: bool) -> bool {
        let response_dead = self.resp_wait.is_some_and(|wait| wait.expired(now));
        let progress_dead =
            has_in_flight && self.progress_wait.is_some_and(|wait| wait.expired(now));
        response_dead || progress_dead
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn watchdog_deadline_is_latched_and_capped() {
        let now = Instant::now();

        let mut latched = PeerLiveness::new();
        latched.on_send(now, Duration::from_millis(100));
        latched.on_send(
            now + Duration::from_secs(1),
            Duration::from_secs(30),
        );
        assert!(
            latched.should_terminate_session(now + Duration::from_secs(47), true)
        );
        assert!(
            latched.should_terminate_session(now + Duration::from_secs(47), false)
        );
        assert!(
            latched.should_terminate_session(now + MAX_WATCHDOG_TIMEOUT, true)
        );

        let mut capped = PeerLiveness::new();
        capped.on_send(now, Duration::from_secs(30));
        assert!(
            !capped.should_terminate_session(now + Duration::from_secs(47), true)
        );
        assert!(
            capped.should_terminate_session(
                now + MAX_WATCHDOG_TIMEOUT + Duration::from_secs(1),
                true,
            )
        );
    }

    #[test]
    fn response_watchdog_uses_rto_and_minimum_floor() {
        let mut l = PeerLiveness::new();
        let rto = Duration::from_millis(100);
        let now = Instant::now();
        l.on_send(
            now - MIN_NO_RESP_FOR - Duration::from_millis(1),
            rto,
        );
        assert!(l.should_terminate_session(now, false));
        let mut l2 = PeerLiveness::new();
        l2.on_send(
            now - MIN_NO_RESP_FOR + Duration::from_millis(10),
            rto,
        );
        assert!(!l2.should_terminate_session(now, false));
        let mut l3 = PeerLiveness::new();
        l3.on_send(
            now - MIN_NO_RESP_FOR - Duration::from_millis(1),
            rto,
        );
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
    fn recent_cumulative_progress_restarts_the_watchdog() {
        let mut l = PeerLiveness::new();
        let rto = Duration::from_millis(100);
        let now = Instant::now();
        l.record_progress();
        l.refresh_waits(now - Duration::from_millis(10), rto);
        assert!(
            !l.should_terminate_session(now, true),
            "progress 10ms ago must fend off watchdog"
        );
    }
}
