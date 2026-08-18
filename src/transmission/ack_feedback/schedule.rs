use std::time::{Duration, Instant};

use crate::metrics::MetricsAckFlushReason;

pub(crate) const ACK_FLUSH_COUNT: usize = 8;
pub(crate) const ACK_FLUSH_AGE: Duration = Duration::from_millis(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckSchedule {
    Idle,
    Due(MetricsAckFlushReason),
    At(Instant),
}

impl AckSchedule {
    pub(crate) fn is_due(self) -> bool {
        matches!(self, Self::Due(_))
    }

    pub(super) fn reason(self) -> Option<MetricsAckFlushReason> {
        match self {
            Self::Due(reason) => Some(reason),
            Self::Idle | Self::At(_) => None,
        }
    }
}

/// Sparse ACK work sends immediately only for the first claim, then coalesces
/// for `ACK_FLUSH_AGE`; FIN outranks count, count outranks age, and explicit
/// calls remain identifiable (they claim outside this schedule).
pub(super) fn schedule(
    now: Instant,
    pending_acks: usize,
    fin_pending: bool,
    last_ack_flush: Option<Instant>,
) -> AckSchedule {
    if pending_acks == 0 && !fin_pending {
        return AckSchedule::Idle;
    }
    if fin_pending {
        return AckSchedule::Due(MetricsAckFlushReason::Fin);
    }
    if ACK_FLUSH_COUNT <= pending_acks {
        return AckSchedule::Due(MetricsAckFlushReason::Count);
    }
    match last_ack_flush {
        None => AckSchedule::Due(MetricsAckFlushReason::Initial),
        Some(last) if ACK_FLUSH_AGE <= now.duration_since(last) => {
            AckSchedule::Due(MetricsAckFlushReason::Age)
        }
        Some(last) => AckSchedule::At(last + ACK_FLUSH_AGE),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_names_every_flush_trigger_with_policy_precedence() {
        let now = Instant::now();
        let age_ago = now - ACK_FLUSH_AGE;

        // Idle without pending work, whatever the history.
        assert_eq!(
            schedule(now, 0, false, None),
            AckSchedule::Idle,
            "no pending ACK work is idle even without a prior flush"
        );
        assert_eq!(
            schedule(now, 0, false, Some(now)),
            AckSchedule::Idle,
            "no pending ACK work is idle even after a prior flush"
        );

        // FIN outranks count and age.
        assert_eq!(
            schedule(now, ACK_FLUSH_COUNT, true, Some(now)),
            AckSchedule::Due(MetricsAckFlushReason::Fin),
            "a pending FIN outranks the count threshold"
        );
        assert_eq!(
            schedule(now, 1, true, None),
            AckSchedule::Due(MetricsAckFlushReason::Fin),
            "a pending FIN outranks the initial claim"
        );

        // Count outranks age and the initial claim.
        assert_eq!(
            schedule(now, ACK_FLUSH_COUNT, false, Some(now)),
            AckSchedule::Due(MetricsAckFlushReason::Count),
            "the count threshold outranks the age deadline"
        );
        assert_eq!(
            schedule(now, ACK_FLUSH_COUNT, false, None),
            AckSchedule::Due(MetricsAckFlushReason::Count),
            "the count threshold outranks the initial claim"
        );

        // Sparse work: first claim is immediately due (initial).
        assert_eq!(
            schedule(now, 1, false, None),
            AckSchedule::Due(MetricsAckFlushReason::Initial),
            "sparse work without a prior flush is immediately due"
        );

        // Age deadline fires once ACK_FLUSH_AGE elapsed since the last flush.
        assert_eq!(
            schedule(now, 1, false, Some(age_ago)),
            AckSchedule::Due(MetricsAckFlushReason::Age),
            "the age cap must fire at last + ACK_FLUSH_AGE"
        );
        assert_eq!(
            schedule(age_ago, 1, false, Some(age_ago - ACK_FLUSH_AGE)),
            AckSchedule::Due(MetricsAckFlushReason::Age),
            "age is measured from the last flush start"
        );

        // Before the age deadline the writer is armed for the exact deadline.
        assert_eq!(
            schedule(now, 1, false, Some(now)),
            AckSchedule::At(now + ACK_FLUSH_AGE),
            "sparse work is rearmed from the successful flush start"
        );
    }
}
