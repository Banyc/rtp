use std::sync::Mutex;
use std::time::Instant;

use tokio::sync::Notify;

use self::state::State;

mod pages;
mod schedule;
mod state;

pub(super) use pages::AckPage;
pub(crate) use pages::MAX_NUM_ACK;
pub(crate) use schedule::AckSchedule;
pub(super) use state::{AckClaim, AckFlushOutcome, ReceivedAckWork};

#[derive(Debug)]
pub(crate) struct AckFeedback {
    state: Mutex<State>,
    schedule_changed: Notify,
}

impl AckFeedback {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(State::new()),
            schedule_changed: Notify::new(),
        }
    }

    pub(crate) fn record(&self, work: ReceivedAckWork) -> bool {
        let schedule_changed = self.state.lock().unwrap().record(work);
        if schedule_changed {
            self.schedule_changed.notify_one();
        }
        schedule_changed
    }

    pub(crate) fn schedule(&self, now: Instant) -> AckSchedule {
        self.state.lock().unwrap().schedule(now)
    }

    pub(crate) fn claim(&self, now: Instant, history_count: usize) -> Option<AckClaim> {
        self.state.lock().unwrap().claim(now, history_count)
    }

    pub(crate) fn complete(&self, claim: AckClaim, outcome: AckFlushOutcome) {
        self.state.lock().unwrap().complete(claim, outcome);
    }

    pub(crate) fn schedule_changed(&self) -> &Notify {
        &self.schedule_changed
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.state.lock().unwrap().is_drained()
    }

    #[cfg(test)]
    pub(crate) fn pending_work(&self) -> (usize, bool) {
        self.state.lock().unwrap().pending_work()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn record_reaches_a_waiter_armed_before_the_state_change() {
        let owner = AckFeedback::new();
        let notified = owner.schedule_changed().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let changed = owner.record(ReceivedAckWork {
            pending_acks: 1,
            fin_ack: false,
            echo_ts: None,
        });
        assert!(changed, "empty-to-pending must report a schedule change");
        tokio::time::timeout(Duration::from_millis(100), &mut notified)
            .await
            .expect("a waiter armed before the state change must be reached");
    }
}
