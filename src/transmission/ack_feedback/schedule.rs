use std::time::{Duration, Instant};

pub(crate) const ACK_FLUSH_COUNT: usize = 8;
pub(crate) const ACK_FLUSH_AGE: Duration = Duration::from_millis(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckSchedule {
    Idle,
    Due,
    At(Instant),
}

impl AckSchedule {
    pub(crate) fn is_due(self) -> bool {
        matches!(self, Self::Due)
    }
}

pub(super) fn schedule(
    now: Instant,
    pending_acks: usize,
    fin_pending: bool,
    last_ack_flush: Option<Instant>,
) -> AckSchedule {
    if pending_acks == 0 && !fin_pending {
        return AckSchedule::Idle;
    }
    if fin_pending || ACK_FLUSH_COUNT <= pending_acks {
        return AckSchedule::Due;
    }
    match last_ack_flush {
        None => AckSchedule::Due,
        Some(last) if ACK_FLUSH_AGE <= now.duration_since(last) => AckSchedule::Due,
        Some(last) => AckSchedule::At(last + ACK_FLUSH_AGE),
    }
}
