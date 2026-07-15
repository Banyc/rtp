use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchdogTuning {
    pub(crate) rto_multiplier: u32,
    pub(crate) min_no_response: Duration,
    pub(crate) min_no_progress: Duration,
    pub(crate) max_timeout: Duration,
}

impl WatchdogTuning {
    pub fn new(
        rto_multiplier: u32,
        min_no_response: Duration,
        min_no_progress: Duration,
        max_timeout: Duration,
    ) -> Self {
        assert!(
            rto_multiplier > 0,
            "watchdog RTO multiplier must be positive"
        );
        assert!(
            max_timeout >= min_no_response && max_timeout >= min_no_progress,
            "watchdog maximum must not be below either minimum"
        );
        Self {
            rto_multiplier,
            min_no_response,
            min_no_progress,
            max_timeout,
        }
    }
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
