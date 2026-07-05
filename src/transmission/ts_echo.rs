use std::time::Duration;

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
