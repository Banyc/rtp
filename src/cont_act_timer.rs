use std::time::{Duration, Instant};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ContActTimerOn {
    Unchanged,
    Hot,
}

/// Only return the value if there is only the same kind of actions performed during a specific duration
#[derive(Debug, Clone)]
pub struct ContActTimer<T> {
    start: Option<Instant>,
    value: T,
    on: ContActTimerOn,
}
impl<T> ContActTimer<T> {
    pub fn new(value: T, now: Instant, on: ContActTimerOn) -> Self {
        let start = match on {
            ContActTimerOn::Unchanged => None,
            ContActTimerOn::Hot => Some(now),
        };
        Self { start, value, on }
    }

    /// `at_least_for`: The timer sets off in at least this duration
    ///
    /// Return [`Some`] iif the timer sets off
    pub fn try_set_and_get<F>(
        &mut self,
        new_value: F,
        at_least_for: Duration,
        now: Instant,
    ) -> Option<&T>
    where
        F: Fn(&T) -> Option<T>,
    {
        // Set value
        let set = match new_value(&self.value) {
            Some(v) => {
                self.value = v;
                true
            }
            None => false,
        };

        // Reset timer and reject
        match (set, &self.on) {
            (true, ContActTimerOn::Unchanged) | (false, ContActTimerOn::Hot) => {
                self.start = None;
                return None;
            }
            _ => (),
        }

        // Get start of the timer
        let start = match self.start {
            Some(x) => x,
            None => {
                self.start = Some(now);
                now
            }
        };

        // Check the duration condition
        let dur = now.duration_since(start);
        if dur < at_least_for {
            return None;
        }

        // Sets off and reset the timer
        self.start = Some(now);
        Some(&self.value)
    }
}
