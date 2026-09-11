use std::time::Instant;

use super::AckPage;
use super::pages::{AckPagePlan, MAX_NUM_ACK};
use super::schedule::{ACK_FLUSH_COUNT, AckSchedule, schedule};
use crate::metrics::MetricsAckFlushReason;
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
    reason: MetricsAckFlushReason,
    pages: AckPagePlan,
    echo_ts: Option<u32>,
    echo_backup: Option<u32>,
    pending_acks: usize,
    fin_pending: bool,
    flush_started_at: Instant,
}

impl AckClaim {
    /// Why this transactional claim became due (initial, age, count, fin, or
    /// explicit).
    pub(crate) fn reason(&self) -> MetricsAckFlushReason {
        self.reason
    }

    pub(crate) fn pages(&self) -> [Option<AckPage>; 2] {
        self.pages.pages()
    }

    pub(crate) fn take_echo(&mut self) -> Option<u32> {
        self.echo_ts.take()
    }

    /// Whether the claim carries a peer echo-timestamp (peek, non-consuming).
    pub(crate) fn peek_echo(&self) -> Option<u32> {
        self.echo_ts
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AckFlushOutcome {
    Sent {
        pages_sent: usize,
    },
    /// The underlay refused the ACK datagram. `rearm` names whether the
    /// flush schedule must be rearmed so the write driver parks for
    /// `ACK_FLUSH_AGE` instead of busy-looping on an immediately-due
    /// schedule: `true` for a blocked underlay flush (the datagram could not
    /// be sent, so retrying instantly is futile), `false` for a piggyback
    /// claim release (no packet to ride on / retransmission too large — the
    /// standalone flush at the end of the pass should pick the work up
    /// immediately).
    WouldBlock {
        pages_sent: usize,
        rearm: bool,
    },
    Fatal {
        pages_sent: usize,
    },
}

impl AckFlushOutcome {
    fn pages_sent(self) -> usize {
        match self {
            Self::Sent { pages_sent }
            | Self::WouldBlock { pages_sent, .. }
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
        let reason = self
            .schedule(now)
            .reason()
            .unwrap_or(MetricsAckFlushReason::Explicit);
        let echo_ts = self.ts_echo.take();
        Some(AckClaim {
            id,
            reason,
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
                self.ack_page_cursor = claim.pages.cursor_after(pages_sent);
                // Complete only what this flush conveyed: the sum of the sent
                // pages' block capacities bounds the completion, and the
                // claim's snapshot clamps it so work recorded during the
                // send survives. Blocks beyond the conveyed pages stay
                // pending, keeping the flush due so subsequent flushes walk
                // the deep tail without waiting for new ACK-eliciting work.
                let conveyed = claim.pages.conveyed_blocks(pages_sent);
                self.complete_claim(claim.pending_acks.min(conveyed), claim.fin_pending);
                self.last_ack_flush = Some(claim.flush_started_at);
            }
            AckFlushOutcome::WouldBlock { rearm, .. } => {
                if let Some(echo_ts) = claim.echo_backup {
                    self.ts_echo.restore(echo_ts);
                }
                // Advance the deep-walk cursor past the pages that DID go out:
                // a partial flush (head delivered, deep page blocked) must
                // not leave the cursor pinned so the next claim re-plans the
                // same deep slice (and re-sends the head page as a duplicate)
                // — it plans the next deep page instead. The decrement is the
                // conveyed block count of the sent pages, not `pages_sent *
                // MAX_NUM_ACK`, so a short tail page is never billed as a
                // full page.
                let conveyed = claim.pages.conveyed_blocks(pages_sent);
                self.ack_page_cursor = claim.pages.cursor_after(pages_sent);
                self.complete_claim(claim.pending_acks.min(conveyed), false);
                if rearm {
                    // A blocked underlay flush leaves the pending work
                    // untouched, so without a rearm the schedule would stay
                    // immediately due and the write driver would spin on
                    // re-claim/re-encode/re-send without ever parking on the
                    // stop token. Rearm from NOW (the WouldBlock time, not
                    // the claim start — a blocked send may have consumed
                    // real time) so the next wake is `ACK_FLUSH_AGE` away.
                    self.last_ack_flush = Some(Instant::now());
                }
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
    fn sent_completes_only_the_conveyed_pages_and_keeps_the_flush_due() {
        let mut state = State::new();
        let now = Instant::now();
        state.record(ReceivedAckWork {
            pending_acks: 300,
            fin_ack: false,
            echo_ts: None,
        });
        // History far deeper than the two planned pages: one flush conveys at
        // most head [0,64) + deep [64,128), so 172 blocks must stay pending
        // and keep the flush due for the next deep page.
        let claim = state.claim(now, 400).expect("pending work must claim");
        assert_eq!(
            claim.pages().iter().flatten().count(),
            2,
            "deep history must claim two pages"
        );
        state.complete(claim, AckFlushOutcome::Sent { pages_sent: 2 });
        assert_eq!(
            state.pending_acks,
            300 - 2 * MAX_NUM_ACK,
            "a flush completes only the blocks its pages conveyed"
        );
        assert_eq!(
            state.schedule(now),
            AckSchedule::Due(MetricsAckFlushReason::Count),
            "unconveyed blocks must keep the flush due (count) without new work"
        );
        // The next walk picks up where the flushed pages ended, not page 0
        // again.
        let next = state.claim(now, 400).expect("remaining work must claim");
        assert_eq!(
            next.pages()[1]
                .expect("deep history must claim a deep page")
                .first_block_index,
            2 * MAX_NUM_ACK,
            "the deep walk must advance past the pages just sent"
        );
        state.complete(next, AckFlushOutcome::Sent { pages_sent: 2 });
        let tail = state.claim(now, 400).expect("remaining work must claim");
        state.complete(tail, AckFlushOutcome::Sent { pages_sent: 2 });
        assert!(
            state.is_drained(),
            "the deep walk drains the pending blocks"
        );
    }

    #[test]
    fn partial_wouldblock_advances_the_deep_cursor_past_the_sent_page() {
        let mut state = State::new();
        let now = Instant::now();
        state.record(ReceivedAckWork {
            pending_acks: 200,
            fin_ack: false,
            echo_ts: None,
        });
        let claim = state.claim(now, 400).expect("pending work must claim");
        assert_eq!(
            claim.pages()[1]
                .expect("deep history must claim a deep page")
                .first_block_index,
            MAX_NUM_ACK,
            "the first deep page starts at the initial cursor"
        );
        // The head page (page 0) went out on a data datagram; the deep page
        // was blocked. Only the head page's blocks are completed and the
        // cursor must move past it.
        state.complete(
            claim,
            AckFlushOutcome::WouldBlock {
                pages_sent: 1,
                rearm: false,
            },
        );
        assert_eq!(
            state.pending_acks,
            200 - MAX_NUM_ACK,
            "a partial flush completes only the sent page's blocks"
        );
        let next = state.claim(now, 400).expect("remaining work must claim");
        assert_eq!(
            next.pages()[1]
                .expect("deep history must claim a deep page")
                .first_block_index,
            2 * MAX_NUM_ACK,
            "a re-flush must plan the next deep page, not the same slice again"
        );
        assert!(
            state.has_pending(),
            "blocked deep-page work must remain pending"
        );
    }

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
        state.complete(
            claim,
            AckFlushOutcome::WouldBlock {
                pages_sent: 1,
                rearm: false,
            },
        );
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
    fn wouldblock_rearm_parks_the_schedule_instead_of_staying_due() {
        let mut state = State::new();
        let now = Instant::now();
        // A piggyback claim release (rearm: false) leaves the schedule due so
        // the standalone flush at the end of the pass picks the work up
        // immediately — no added ACK latency on the happy path.
        state.record(ReceivedAckWork {
            pending_acks: 1,
            fin_ack: false,
            echo_ts: None,
        });
        let claim = state.claim(now, 0).expect("pending work must claim");
        state.complete(
            claim,
            AckFlushOutcome::WouldBlock {
                pages_sent: 0,
                rearm: false,
            },
        );
        assert!(
            state.schedule(now).is_due(),
            "a piggyback release must keep the flush due"
        );
        // A blocked underlay flush (rearm: true) must rearm the schedule so
        // the write driver parks for ACK_FLUSH_AGE instead of busy-looping
        // on an immediately-due schedule (which would never observe the stop
        // token and would hang graceful shutdown).
        let claim = state.claim(now, 0).expect("pending work must claim");
        state.complete(
            claim,
            AckFlushOutcome::WouldBlock {
                pages_sent: 0,
                rearm: true,
            },
        );
        match state.schedule(now) {
            AckSchedule::At(deadline) => assert!(
                deadline > now,
                "a rearmed deadline must be in the future, got {deadline:?}"
            ),
            other => panic!("a blocked flush must rearm the schedule, got {other:?}"),
        }
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
            AckSchedule::Due(MetricsAckFlushReason::Initial),
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
