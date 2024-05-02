use std::time::{Duration, Instant};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ContActTimerOn {
    Unchanged,
    Hot,
}

#[allow(dead_code)]
/// Only return the value if there is only the same kind of actions performed during a specific duration
#[derive(Debug, Clone)]
pub struct ContActTimer<T> {
    timer: PollTimer,
    value: T,
    on: ContActTimerOn,
}
#[allow(dead_code)]
impl<T> ContActTimer<T> {
    pub fn new(value: T, now: Instant, on: ContActTimerOn) -> Self {
        let mut timer = PollTimer::new_cleared();
        if matches!(on, ContActTimerOn::Hot) {
            timer.ensure_started(now);
        }
        Self { timer, value, on }
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

        // Clear timer and reject
        match (set, &self.on) {
            (true, ContActTimerOn::Unchanged) | (false, ContActTimerOn::Hot) => {
                self.timer.clear();
                return None;
            }
            _ => (),
        }

        // Check the duration condition
        if !self.timer.set_and_check(at_least_for, now) {
            return None;
        }

        // Sets off and restart the timer
        self.timer.restart(now);
        Some(&self.value)
    }
}

#[derive(Debug, Clone)]
pub struct PollTimer {
    start: Option<Instant>,
}
impl PollTimer {
    /// Create an cleared timer
    pub fn new_cleared() -> Self {
        Self { start: None }
    }

    pub fn clear(&mut self) {
        self.start = None;
    }

    pub fn restart(&mut self, now: Instant) {
        self.start = Some(now);
    }

    /// Return the start time
    pub fn ensure_started(&mut self, now: Instant) -> Instant {
        match self.start {
            Some(x) => x,
            None => {
                self.start = Some(now);
                now
            }
        }
    }

    /// Return `true` iff the timer sets off
    pub fn set_and_check(&mut self, at_least_for: Duration, now: Instant) -> bool {
        let start = self.ensure_started(now);

        // Check the duration condition
        let dur = now.duration_since(start);
        at_least_for <= dur
    }
}
