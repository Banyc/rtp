use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Timestamp-echo state: stores the peer's send timestamp to echo back in the
/// next ACK, and converts echoed timestamps into RTT samples.
#[derive(Debug)]
pub(crate) struct TsEcho {
    pending: Option<u32>,
}

impl TsEcho {
    const MAX_ECHO_RTT: Duration = Duration::from_secs(60);

    pub(crate) fn new() -> Self {
        Self { pending: None }
    }

    /// Store a peer timestamp to echo in the next ACK.
    pub(crate) fn set(&mut self, send_ts: u32) {
        self.pending = Some(send_ts);
    }

    /// Restore a claimed echo timestamp (used when an ACK flush is retried
    /// after a WouldBlock — the echo must survive the failed flush).
    pub(crate) fn restore(&mut self, send_ts: u32) {
        self.pending = Some(send_ts);
    }

    /// Take the pending echo timestamp, consuming it so each RTT sample is
    /// reflected exactly once.
    pub(crate) fn take(&mut self) -> Option<u32> {
        self.pending.take()
    }

    /// Convert an echoed timestamp into an RTT value, or return `None` if the
    /// computed RTT exceeds [`Self::MAX_ECHO_RTT`].
    pub(crate) fn rtt_from_echo(local_ts: u32, echo_ts: u32) -> Option<Duration> {
        let rtt = Duration::from_micros(local_ts.wrapping_sub(echo_ts) as u64);
        if rtt <= Self::MAX_ECHO_RTT {
            Some(rtt)
        } else {
            None
        }
    }
}

impl Default for TsEcho {
    fn default() -> Self {
        Self::new()
    }
}

/// Recent-echo dedup window: remembers sampled echo timestamps over a ~60 s
/// window (well under the ~71-min u32-µs wrap) so a parity-recovered ACK
/// plus its original do not feed the same echo into the RTT estimator twice.
///
/// Bounded by `MAX_RECENT_ECHOES`; entries older than `MAX_ECHO_AGE` are
/// evicted on every `should_sample` call.
#[derive(Debug)]
pub(crate) struct RecentEchoes {
    seen: HashSet<u32>,
    order: VecDeque<(Instant, u32)>,
}

const MAX_RECENT_ECHOES: usize = 1024;
const MAX_ECHO_AGE: Duration = Duration::from_secs(60);

impl RecentEchoes {
    pub(crate) fn new() -> Self {
        Self {
            seen: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    /// Expire old entries, then return `false` if `echo_ts` was already seen
    /// (duplicate — skip the RTT sample), or insert it and return `true`
    /// (first sighting — feed it to the estimator).
    pub(crate) fn should_sample(&mut self, echo_ts: u32, now: Instant) -> bool {
        // Evict entries older than MAX_ECHO_AGE.
        while let Some(&(sampled_at, _)) = self.order.front() {
            if now.duration_since(sampled_at) > MAX_ECHO_AGE {
                if let Some((_, ts)) = self.order.pop_front() {
                    self.seen.remove(&ts);
                }
            } else {
                break;
            }
        }
        if self.seen.contains(&echo_ts) {
            return false;
        }
        self.seen.insert(echo_ts);
        self.order.push_back((now, echo_ts));
        // Cap the window.
        if self.order.len() > MAX_RECENT_ECHOES
            && let Some((_, ts)) = self.order.pop_front()
        {
            self.seen.remove(&ts);
        }
        true
    }
}

impl Default for RecentEchoes {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn dedup_blocks_duplicate_echo() {
        let mut echoes = RecentEchoes::new();
        let now = Instant::now();
        let echo = 12345u32;

        assert!(echoes.should_sample(echo, now), "first sighting must pass");
        assert!(
            !echoes.should_sample(echo, now),
            "duplicate must be blocked"
        );
    }

    #[test]
    fn dedup_allows_distinct_echo() {
        let mut echoes = RecentEchoes::new();
        let now = Instant::now();

        assert!(echoes.should_sample(1, now));
        assert!(echoes.should_sample(2, now));
        assert!(echoes.should_sample(3, now));
        assert!(!echoes.should_sample(2, now), "repeat of 2 blocked");
    }

    #[test]
    fn dedup_clears_after_window() {
        let mut echoes = RecentEchoes::new();
        let now = Instant::now();
        let echo = 42u32;

        assert!(echoes.should_sample(echo, now));
        assert!(!echoes.should_sample(echo, now));

        // After the 60 s window, the same echo should pass again.
        let later = now + Duration::from_secs(61);
        assert!(
            echoes.should_sample(echo, later),
            "echo must pass after the dedup window expires"
        );
    }

    #[test]
    fn dedup_feeding_duplicate_echo_updates_rtt_once() {
        let mut echoes = RecentEchoes::new();
        let now = Instant::now();
        let echo_ts = 1000u32;
        let local_ts = 2000u32;

        // First sighting: should produce an RTT sample.
        let first = if echoes.should_sample(echo_ts, now) {
            TsEcho::rtt_from_echo(local_ts, echo_ts)
        } else {
            None
        };
        assert!(first.is_some(), "first sighting must produce an RTT sample");

        // Duplicate sighting: must be blocked (no RTT sample).
        let second = if echoes.should_sample(echo_ts, now) {
            TsEcho::rtt_from_echo(local_ts, echo_ts)
        } else {
            None
        };
        assert!(second.is_none(), "duplicate must not produce an RTT sample");
    }
}
