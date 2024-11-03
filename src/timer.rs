use std::time::{Duration, Instant};

use primitive::{time::timer::Timer, Clear};

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
    timer: Timer,
    value: T,
    on: ContActTimerOn,
}
#[allow(dead_code)]
impl<T> ContActTimer<T> {
    pub fn new(value: T, now: Instant, on: ContActTimerOn) -> Self {
        let mut timer = Timer::new();
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
        let (set_off, _) = self.timer.ensure_started_and_check(at_least_for, now);
        if !set_off {
            return None;
        }

        // Sets off and restart the timer
        self.timer.restart(now);
        Some(&self.value)
    }
}
