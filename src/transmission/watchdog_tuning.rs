use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchdogTuning {
    pub(crate) rto_multiplier: u32,
    pub(crate) min_no_response: Duration,
    pub(crate) min_no_progress: Duration,
    pub(crate) max_timeout: Duration,
}

impl Default for WatchdogTuning {
    fn default() -> Self {
        Self {
            rto_multiplier: 16,
            min_no_response: Duration::from_secs(30),
            min_no_progress: Duration::from_secs(30),
            max_timeout: Duration::from_secs(120),
        }
    }
}
