use std::time::Instant;

use super::pages::{AckPage, AckPagePlan, MAX_NUM_ACK};
use super::schedule::{ACK_FLUSH_COUNT, AckSchedule, schedule};
use crate::transmission::ts_echo::TsEcho;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReceivedAckWork {
    pub(crate) pending_acks: usize,
    pub(crate) fin_ack: bool,
    pub(crate) echo_ts: Option<u32>,
}

#[derive(Debug)]
pub(crate) struct AckClaim {
    id: u64,
    pages: AckPagePlan,
    echo_ts: Option<u32>,
    echo_backup: Option<u32>,
    pending_acks: usize,
    fin_pending: bool,
    flush_started_at: Instant,
}

impl AckClaim {
    pub(crate) fn pages(&self) -> [Option<AckPage>; 2] {
        self.pages.pages()
    }

    pub(crate) fn take_echo(&mut self) -> Option<u32> {
        self.echo_ts.take()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AckFlushOutcome {
    Sent { pages_sent: usize },
    WouldBlock { pages_sent: usize },
    Fatal { pages_sent: usize },
}

impl AckFlushOutcome {
    fn pages_sent(self) -> usize {
        match self {
            Self::Sent { pages_sent }
            | Self::WouldBlock { pages_sent }
            | Self::Fatal { pages_sent } => pages_sent,
        }
    }
}

#[derive(Debug)]
pub(super) struct State {
    ts_echo: TsEcho,
    pending_acks: usize,
    fin_pending: bool,
    last_ack_flush: Option<Instant>,
    ack_page_cursor: usize,
    in_flight: Option<u64>,
    next_claim_id: u64,
}

impl State {
    pub(super) fn new() -> Self {
        Self {
            ts_echo: TsEcho::new(),
            pending_acks: 0,
            fin_pending: false,
            last_ack_flush: None,
            ack_page_cursor: MAX_NUM_ACK,
            in_flight: None,
            next_claim_id: 0,
        }
    }

    fn has_pending(&self) -> bool {
        0 < self.pending_acks || self.fin_pending
    }

    pub(super) fn is_drained(&self) -> bool {
        !self.has_pending() && self.in_flight.is_none()
    }

    pub(super) fn schedule(&self, now: Instant) -> AckSchedule {
        schedule(
            now,
            self.pending_acks,
            self.fin_pending,
            self.last_ack_flush,
        )
    }

    pub(super) fn record(&mut self, work: ReceivedAckWork) -> bool {
        let had_pending = self.has_pending();
        let was_immediately_due = self.fin_pending || ACK_FLUSH_COUNT <= self.pending_acks;
        self.pending_acks += work.pending_acks;
        self.fin_pending |= work.fin_ack;
        if let Some(echo_ts) = work.echo_ts {
            self.ts_echo.set(echo_ts);
        }
        let is_immediately_due = self.fin_pending || ACK_FLUSH_COUNT <= self.pending_acks;
        !had_pending || (!was_immediately_due && is_immediately_due)
    }

    pub(super) fn claim(&mut self, now: Instant, history_count: usize) -> Option<AckClaim> {
        if !self.has_pending() {
            return None;
        }
        assert!(
            self.in_flight.is_none(),
            "the single ACK writer cannot start a second flush claim"
        );
        let id = self.next_claim_id;
        self.next_claim_id = self.next_claim_id.wrapping_add(1);
        self.in_flight = Some(id);
        let echo_ts = self.ts_echo.take();
        Some(AckClaim {
            id,
            pages: AckPagePlan::new(self.ack_page_cursor, history_count),
            echo_ts,
            echo_backup: echo_ts,
            pending_acks: self.pending_acks,
            fin_pending: self.fin_pending,
            flush_started_at: now,
        })
    }

    pub(super) fn complete(&mut self, claim: AckClaim, outcome: AckFlushOutcome) {
        assert_eq!(
            self.in_flight.take(),
            Some(claim.id),
            "ACK claim completion must match the active claim"
        );
        let pages_sent = outcome.pages_sent();
        assert!(pages_sent <= claim.pages.len());
        match outcome {
            AckFlushOutcome::Sent { .. } => {
                assert_eq!(pages_sent, claim.pages.len());
                self.ack_page_cursor = claim.pages.next_cursor();
                self.complete_claim(claim.pending_acks, claim.fin_pending);
                self.last_ack_flush = Some(claim.flush_started_at);
            }
            AckFlushOutcome::WouldBlock { .. } => {
                if let Some(echo_ts) = claim.echo_backup {
                    self.ts_echo.restore(echo_ts);
                }
                self.complete_claim(pages_sent * MAX_NUM_ACK, false);
            }
            AckFlushOutcome::Fatal { .. } => {
                if let Some(echo_ts) = claim.echo_backup {
                    self.ts_echo.restore(echo_ts);
                }
            }
        }
    }

    fn complete_claim(&mut self, claimed_acks: usize, claimed_fin: bool) {
        self.pending_acks -= claimed_acks.min(self.pending_acks);
        if claimed_fin {
            self.fin_pending = false;
        }
    }

    #[cfg(test)]
    pub(super) fn pending_work(&self) -> (usize, bool) {
        (self.pending_acks, self.fin_pending)
    }
}

#[cfg(test)]
mod tests {
    use super::super::schedule::ACK_FLUSH_AGE;
    use super::*;

    #[test]
    fn record_wakes_only_when_work_needs_an_earlier_schedule() {
        let mut state = State::new();
        assert!(
            state.record(ReceivedAckWork {
                pending_acks: 1,
                fin_ack: false,
                echo_ts: None,
            }),
            "empty to pending needs a wake"
        );
        assert!(
            !state.record(ReceivedAckWork {
                pending_acks: ACK_FLUSH_COUNT - 2,
                fin_ack: false,
                echo_ts: None,
            }),
            "work below the count threshold keeps the existing age deadline"
        );
        assert!(
            state.record(ReceivedAckWork {
                pending_acks: 1,
                fin_ack: false,
                echo_ts: None,
            }),
            "crossing the count threshold makes the flush immediately due"
        );
        assert!(
            !state.record(ReceivedAckWork {
                pending_acks: 1,
                fin_ack: false,
                echo_ts: None,
            }),
            "already-immediate work does not need another wake"
        );
        let mut fin = State::new();
        assert!(fin.record(ReceivedAckWork {
            pending_acks: 1,
            fin_ack: false,
            echo_ts: None,
        }));
        assert!(
            fin.record(ReceivedAckWork {
                pending_acks: 0,
                fin_ack: true,
                echo_ts: None,
            }),
            "a newly pending FIN is immediate"
        );
        assert!(
            !fin.record(ReceivedAckWork {
                pending_acks: 0,
                fin_ack: true,
                echo_ts: None,
            }),
            "an already pending FIN has already scheduled the writer"
        );
    }

    #[test]
    fn completing_a_claim_preserves_work_recorded_during_the_send() {
        let mut state = State::new();
        let now = Instant::now();
        state.record(ReceivedAckWork {
            pending_acks: 4,
            fin_ack: false,
            echo_ts: None,
        });
        let claim = state.claim(now, 0).expect("pending work must claim");
        // The recv path records more work while the flush is in flight.
        state.record(ReceivedAckWork {
            pending_acks: 3,
            fin_ack: false,
            echo_ts: None,
        });
        state.complete(claim, AckFlushOutcome::Sent { pages_sent: 1 });
        assert_eq!(
            state.pending_acks, 3,
            "work recorded during the send must survive the completed claim"
        );
        assert!(!state.is_drained());
        // A later flush drains the remaining work.
        let claim = state.claim(now, 0).expect("remaining work must claim");
        state.complete(claim, AckFlushOutcome::Sent { pages_sent: 1 });
        assert!(state.is_drained());
    }

    #[test]
    fn wouldblock_restores_claimed_echo_and_only_completes_sent_pages() {
        let mut state = State::new();
        let now = Instant::now();
        state.record(ReceivedAckWork {
            pending_acks: 100,
            fin_ack: true,
            echo_ts: Some(1234),
        });
        let claim = state
            .claim(now, 2 * MAX_NUM_ACK)
            .expect("pending work must claim");
        assert_eq!(
            claim.pages().iter().flatten().count(),
            2,
            "deep history must claim two pages"
        );
        // The claim consumed the echo; it must come back on WouldBlock.
        assert_eq!(state.ts_echo.take(), None);
        state.complete(claim, AckFlushOutcome::WouldBlock { pages_sent: 1 });
        assert_eq!(
            state.pending_acks,
            100 - MAX_NUM_ACK,
            "WouldBlock completes only the sent page's ACKs"
        );
        assert!(
            state.fin_pending,
            "WouldBlock must not clear the claimed FIN"
        );
        assert_eq!(
            state.ts_echo.take(),
            Some(1234),
            "the claimed echo must be restored for the next flush"
        );
        assert!(state.has_pending(), "unsent work must remain pending");
    }

    #[test]
    fn sparse_schedule_is_rearmed_from_the_successful_flush_start() {
        let mut state = State::new();
        let now = Instant::now();
        assert_eq!(state.schedule(now), AckSchedule::Idle);
        state.record(ReceivedAckWork {
            pending_acks: 1,
            fin_ack: false,
            echo_ts: None,
        });
        assert_eq!(
            state.schedule(now),
            AckSchedule::Due,
            "work without a prior flush is immediately due"
        );
        let claim = state.claim(now, 0).expect("pending work must claim");
        let flush_started_at = now;
        state.complete(claim, AckFlushOutcome::Sent { pages_sent: 1 });
        assert_eq!(
            state.schedule(now),
            AckSchedule::Idle,
            "a successful flush drains the claimed work"
        );
        state.record(ReceivedAckWork {
            pending_acks: 1,
            fin_ack: false,
            echo_ts: None,
        });
        assert_eq!(
            state.schedule(now),
            AckSchedule::At(flush_started_at + ACK_FLUSH_AGE),
            "sparse work is rearmed from the successful flush start"
        );
        assert!(
            state.schedule(flush_started_at + ACK_FLUSH_AGE).is_due(),
            "the age cap must fire at last + ACK_FLUSH_AGE"
        );
    }
}
