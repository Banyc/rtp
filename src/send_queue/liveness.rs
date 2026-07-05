use std::time::{Duration, Instant};

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
        if self.ever_progressed && self.progress_wait_start.is_none() {
            self.progress_wait_start = Some(now);
        }
    }

    /// Call on every new packet send: starts the response watchdog if it was
    /// idle, and the stall clock if the pipe was empty after prior progress.
    pub(crate) fn on_send(&mut self, now: Instant) {
        if self.resp_wait_start.is_none() {
            self.resp_wait_start = Some(now);
        }
        if self.ever_progressed && self.progress_wait_start.is_none() {
            self.progress_wait_start = Some(now);
        }
    }

    pub(crate) fn no_resp_for(&self, now: Instant) -> Option<Duration> {
        Some(now.duration_since(self.resp_wait_start?))
    }

    pub(crate) fn no_progress_for(&self, now: Instant) -> Option<Duration> {
        Some(now.duration_since(self.progress_wait_start?))
    }
}
