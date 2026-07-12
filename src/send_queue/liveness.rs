use std::time::{Duration, Instant};

/// 16×RTO: response-watchdog and progress-watchdog multiplier.
pub(crate) const RTO_WATCHDOG_MULTIPLIER: f64 = 16.0;
/// Response watchdog floor: a peer must not be declared dead before this.
pub(crate) const MIN_NO_RESP_FOR: Duration = Duration::from_secs(30);
/// Progress stall floor: zero forward progress for this long is terminal.
pub(crate) const MIN_NO_PROGRESS_FOR: Duration = Duration::from_secs(30);

/// Peer-liveness monitor: tracks forward-progress and response-watchdog
/// clocks so the send space can detect a silent peer.
#[derive(Debug)]
pub(crate) struct PeerLiveness {
    /// Detect if the peer has died.
    resp_wait_start: Option<Instant>,
    /// Has any packet ever been acked or have we ever had to wait?
    ever_progressed: bool,
    /// Start of the latest interval during which no forward progress was made.
    progress_wait_start: Option<Instant>,
}

impl PeerLiveness {
    pub(crate) fn new() -> Self {
        Self {
            resp_wait_start: None,
            ever_progressed: false,
            progress_wait_start: None,
        }
    }

    /// Whether the peer has ever made forward progress.
    pub(crate) fn ever_progressed(&self) -> bool {
        self.ever_progressed
    }

    /// Call when an ACK delivered at least one new packet.
    pub(crate) fn record_progress(&mut self) {
        self.ever_progressed = true;
        self.progress_wait_start = None;
    }

    /// Call when the send window becomes empty (no packets to track).
    pub(crate) fn reset_waits(&mut self) {
        self.resp_wait_start = None;
        self.progress_wait_start = None;
    }

    /// Call after an ACK when the window is still non-empty: the peer has
    /// responded, and if progress was made earlier the stall clock starts.
    pub(crate) fn refresh_waits(&mut self, now: Instant) {
        self.resp_wait_start = Some(now);
        if self.progress_wait_start.is_none() {
            self.progress_wait_start = Some(now);
        }
    }

    /// Call on every new packet send: starts the response watchdog if it was
    /// idle, and the stall clock if the pipe was empty after prior progress.
    pub(crate) fn on_send(&mut self, now: Instant) {
        if self.resp_wait_start.is_none() {
            self.resp_wait_start = Some(now);
        }
        if self.progress_wait_start.is_none() {
            self.progress_wait_start = Some(now);
        }
    }

    pub(crate) fn no_resp_for(&self, now: Instant) -> Option<Duration> {
        Some(now.duration_since(self.resp_wait_start?))
    }

    pub(crate) fn no_progress_for(&self, now: Instant) -> Option<Duration> {
        Some(now.duration_since(self.progress_wait_start?))
    }

    /// Terminate a stalled session: the peer has stopped responding
    /// (response watchdog) or made zero cumulative forward progress for
    /// longer than the progress watchdog allows.
    ///
    /// `now` is the current monotonic instant; `rto` is the PktSendSpace
    /// RTO duration; `has_in_flight` must be true only when there are
    /// packets in the send window.
    pub(crate) fn should_terminate_session(
        &self,
        now: Instant,
        rto: Duration,
        has_in_flight: bool,
    ) -> bool {
        let rto_watchdog = rto.mul_f64(RTO_WATCHDOG_MULTIPLIER);
        let response_dead = self
            .resp_wait_start
            .is_some_and(|start| now.duration_since(start) >= rto_watchdog.max(MIN_NO_RESP_FOR));
        let progress_dead = has_in_flight
            && self
                .progress_wait_start
                .is_some_and(|start| now.duration_since(start) >= rto_watchdog.max(MIN_NO_PROGRESS_FOR));
        response_dead || progress_dead
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn response_watchdog_uses_rto_and_minimum_floor() {
        let mut l = PeerLiveness::new();
        let rto = Duration::from_millis(100);
        let now = Instant::now();
        l.on_send(now - MIN_NO_RESP_FOR - Duration::from_millis(1));
        assert!(l.should_terminate_session(now, rto, false));
        let mut l2 = PeerLiveness::new();
        l2.on_send(now - MIN_NO_RESP_FOR + Duration::from_millis(10));
        assert!(!l2.should_terminate_session(now, rto, false));
        let mut l3 = PeerLiveness::new();
        l3.on_send(now - MIN_NO_RESP_FOR - Duration::from_millis(1));
        assert!(l3.should_terminate_session(now, Duration::from_millis(1), false), "floor enforces 30s minimum");
    }

    #[test]
    fn progress_watchdog_requires_packets_in_flight() {
        let mut l = PeerLiveness::new();
        let rto = Duration::from_millis(100);
        let now = Instant::now();
        l.record_progress();
        l.on_send(now - Duration::from_secs(31));
        l.refresh_waits(now - Duration::from_millis(1));
        assert!(!l.should_terminate_session(now, rto, false), "no in-flight => progress watchdog must stay silent");
        assert!(l.should_terminate_session(now, rto, true), "in-flight + stale progress => terminate");
    }

    #[test]
    fn recent_cumulative_progress_restarts_the_watchdog() {
        let mut l = PeerLiveness::new();
        let rto = Duration::from_millis(100);
        let now = Instant::now();
        l.record_progress();
        l.refresh_waits(now - Duration::from_millis(10));
        assert!(!l.should_terminate_session(now, rto, true), "progress 10ms ago must fend off watchdog");
    }
}
