use std::time::{Duration, Instant};

/// Outage-recovery epoch state machine.
///
/// When forward progress stalls for at least one RTO (with a loss event, or
/// two RTOs without), a new epoch starts.  During the epoch:
///  - pre-outage RTT/rate samples are censored,
///  - cwnd is clamped to INIT_CWND,
///  - pre-outage packets are immediately eligible for retransmission without
///    counting as loss events.
///
/// A fresh post-outage RTT sample closes the epoch, but the cut persists to
/// filter late echoes of pre-outage packets.
#[derive(Debug)]
pub(crate) struct OutageEpoch {
    /// Start of the current outage-recovery epoch, if one is open.
    recovery_start: Option<Instant>,
    /// Packets originally sent before this instant are considered pre-outage.
    /// Their retransmission losses do not mark a congestion event.
    outage_loss_cut: Option<Instant>,
}

/// Outcome of [`OutageEpoch::detect`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutageDetection {
    /// No outage detected.
    None,
    /// Re-arming an already-open epoch: refreshed the cut but caller must
    /// **not** re-seed sRTT or clear loss history.
    Rearming,
    /// New epoch: caller must seed sRTT from the current RTO and clear prior
    /// loss history so the post-outage path is treated as fresh.
    NewEpoch,
}

impl OutageEpoch {
    pub(crate) fn new() -> Self {
        Self {
            recovery_start: None,
            outage_loss_cut: None,
        }
    }

    pub(crate) fn in_outage_recovery(&self) -> bool {
        self.recovery_start.is_some()
    }

    #[allow(dead_code)]
    pub(crate) fn outage_cut(&self) -> Option<Instant> {
        self.outage_loss_cut
    }

    /// Whether a packet sent at `sent_time` is pre-outage and its loss should
    /// not count as a congestion event.
    pub(crate) fn is_pre_outage_loss(&self, sent_time: Instant) -> bool {
        let Some(cut) = self.outage_loss_cut else {
            return false;
        };
        sent_time < cut
    }

    /// Whether an RTT sample should be censored as stale (pre-outage).
    pub(crate) fn should_censor_rtt_sample(&self, rtt: Duration, now: Instant) -> bool {
        let Some(cut) = self.outage_loss_cut else {
            return false;
        };
        let sent_at = now.checked_sub(rtt);
        sent_at.is_none_or(|sent_at| sent_at < cut)
    }

    /// Whether a fresh post-outage RTT sample should close the epoch.
    ///
    /// Returns `true` once per epoch; the caller should re-seed sRTT with the
    /// sample's actual RTT.  Repeated calls return `false` because
    /// `outage_loss_cut` stays in place until a new outage re-arms it.
    pub(crate) fn try_close_epoch_with_fresh_sample(&mut self) -> bool {
        self.outage_loss_cut.is_some() && self.recovery_start.take().is_some()
    }

    /// Whether a rate sample whose `prior_time` predates the outage cut should
    /// be ignored.  Used to prevent bogus blackout-spanning samples from
    /// collapsing the send rate back toward zero.
    pub(crate) fn should_censor_rate_sample(&self, prior_time: Instant) -> bool {
        self.recovery_start.is_some()
            && self.outage_loss_cut.is_some_and(|cut| prior_time < cut)
    }

    /// Detect whether an outage-recovery epoch should start (or be re-armed).
    ///
    /// The caller provides the conditions evaluated against `PktSendSpace`
    /// state; this method only manages the epoch state machine.
    pub(crate) fn detect(
        &mut self,
        now: Instant,
        ever_progressed: bool,
        no_progress_for: Option<Duration>,
        rto: Duration,
        has_loss_event: bool,
    ) -> OutageDetection {
        if !ever_progressed {
            return OutageDetection::None;
        }

        let no_progress_for = match no_progress_for {
            Some(d) => d,
            None => return OutageDetection::None,
        };
        if no_progress_for < rto {
            return OutageDetection::None;
        }

        let loss_or_long_silence = has_loss_event || no_progress_for >= rto * 2;
        if !loss_or_long_silence {
            return OutageDetection::None;
        }

        let rearming = self.recovery_start.is_some();
        self.recovery_start = Some(now);
        self.outage_loss_cut = Some(now);

        if rearming {
            OutageDetection::Rearming
        } else {
            OutageDetection::NewEpoch
        }
    }

    /// Clear the epoch's open marker without clearing the cut.
    ///
    /// `outage_loss_cut` intentionally stays in place so that late echoes of
    /// pre-outage packets are still discarded.  A second outage-length stall
    /// inside an already-closed epoch should refresh the cut via [`detect`].
    pub(crate) fn clear(&mut self) {
        self.recovery_start = None;
    }
}
