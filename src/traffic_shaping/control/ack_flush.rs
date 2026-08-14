use std::time::{Duration, Instant};

use crate::ack::{EncodeAck, MAX_ACK_BLOCKS, next_page_cursor};
use crate::codec::encode_ack_data;
use crate::io_err::IoErr;
use crate::metrics::MetricsTerminationCause;
use crate::transmission::transmission_layer::SendBufs;
use crate::transmission::ts_echo::TsEcho;
use crate::transmission::write_half::WriteHalf;

/// The wire bound on selective ACK blocks per datagram; page size for the
/// ACK-flush paging scheme.
pub(crate) const MAX_NUM_ACK: usize = MAX_ACK_BLOCKS;
pub(crate) const ACK_FLUSH_COUNT: usize = 16;
pub(crate) const ACK_FLUSH_AGE: Duration = Duration::from_millis(1);

/// Shared ACK-flush state, accessed from both the recv path (records ACK
/// work) and the send path (flushes ACKs to the wire).  Protected by a
/// `Mutex` so the recv and send tasks can safely concurrent access it.
#[derive(Debug)]
pub(crate) struct AckFlushState {
    pub(crate) ts_echo: TsEcho,
    pub(crate) pending_acks: usize,
    pub(crate) fin_pending: bool,
    pub(crate) last_ack_flush: Option<Instant>,
    /// Resume offset for deep ack-history pages. Each flush sends cumulative
    /// page 0 plus one deep page starting here. Wrapped back to MAX_NUM_ACK on
    /// reset and when the cursor reaches the end of the history.
    pub(crate) ack_page_cursor: usize,
}

impl AckFlushState {
    pub(crate) fn new() -> Self {
        Self {
            ts_echo: TsEcho::new(),
            pending_acks: 0,
            fin_pending: false,
            last_ack_flush: None,
            ack_page_cursor: MAX_NUM_ACK,
        }
    }

    /// Whether any ACK work is pending a flush.
    pub(crate) fn has_pending(&self) -> bool {
        0 < self.pending_acks || self.fin_pending
    }

    /// Whether a flush is due now: pending work past the age/count thresholds,
    /// or an unacknowledged FIN.
    pub(crate) fn is_due(&self, now: Instant) -> bool {
        if self.pending_acks == 0 && !self.fin_pending {
            return false;
        }
        self.fin_pending
            || ACK_FLUSH_COUNT <= self.pending_acks
            || self
                .last_ack_flush
                .is_none_or(|last| ACK_FLUSH_AGE <= now.duration_since(last))
    }

    /// The next instant at which a flush must run, if pending work exists.
    pub(crate) fn next_deadline(&self, now: Instant) -> Option<Instant> {
        if self.pending_acks >= ACK_FLUSH_COUNT || self.fin_pending {
            Some(now)
        } else if self.pending_acks > 0 {
            self.last_ack_flush
                .map_or(Some(now), |last| Some(last + ACK_FLUSH_AGE))
        } else {
            None
        }
    }

    /// Single-lock query combining the due-ness decision with the wake
    /// deadline, so the send pass can carry one `AckFlushState` result into
    /// the next-wake computation instead of locking twice.
    pub(crate) fn check(&self, now: Instant) -> (bool, Option<Instant>) {
        (self.is_due(now), self.next_deadline(now))
    }

    /// Record ACK work produced by the recv path: one pending ack (or FIN) per
    /// accepted packet plus an optional peer echo timestamp.
    pub(crate) fn record(&mut self, pending_acks: usize, fin_ack: bool, echo_ts: Option<u32>) {
        self.pending_acks += pending_acks;
        self.fin_pending |= fin_ack;
        if let Some(echo_ts) = echo_ts {
            self.ts_echo.set(echo_ts);
        }
    }

    /// Subtract-claimed: decrement `pending_acks` by the number actually
    /// sent (clamped), and clear `fin_pending` only if the FIN was claimed
    /// and sent.  Never wholesale-clear so a WouldBlock/cancel leaves the
    /// remaining work intact for the next flush.
    pub(crate) fn complete_claim(&mut self, claimed_acks: usize, claimed_fin: bool) {
        self.pending_acks -= claimed_acks.min(self.pending_acks);
        if claimed_fin {
            self.fin_pending = false;
        }
    }
}

/// Flush pending ACKs to the wire: cumulative page 0 plus one deep page from
/// the resume cursor.  The cursor/claim/restore-on-WouldBlock bookkeeping is
/// local to this function so the ACK-flush feature stays co-located.
pub(crate) async fn flush(write_half: &mut WriteHalf, bufs: &mut SendBufs) -> Result<(), IoErr> {
    let now = Instant::now();
    let (cursor, history_count, mut echo_ts, claimed_acks, claimed_fin) = {
        let reliable_layer = write_half.reliable_layer.lock().unwrap();
        let queue = reliable_layer.pkt_recv_space().ack_history();
        let count = queue.len();
        let mut s = write_half.ack_flush.lock().unwrap();
        (
            s.ack_page_cursor.max(MAX_NUM_ACK).min(count),
            count,
            s.ts_echo.take(),
            s.pending_acks,
            s.fin_pending,
        )
    };
    let echo_backup = echo_ts;
    let mut page_0 = true;
    let mut skip = 0;
    let fec_enabled = write_half.fec.is_some();
    let mut pages_sent: usize = 0;
    'ack_pages: loop {
        let (codec_pkt, wire_pkt) = {
            let (_, codec_pkt, wire_pkt) = bufs.parts_mut();
            (codec_pkt, wire_pkt)
        };
        let written_bytes = {
            let reliable_layer = write_half.reliable_layer.lock().unwrap();
            let queue = reliable_layer.pkt_recv_space().ack_history();
            let ack = EncodeAck {
                queue,
                first_block_index: skip,
                max_blocks: MAX_NUM_ACK,
            };
            let this_echo = echo_ts.take();
            encode_ack_data(
                write_half.session_tag(),
                Some(ack),
                this_echo,
                None,
                codec_pkt,
            )
            .unwrap()
        };
        let res = write_half
            .send_with_fec(&codec_pkt[..written_bytes], wire_pkt)
            .await;
        match res {
            Ok(_) => {
                pages_sent += 1;
                if fec_enabled {
                    let now = Instant::now();
                    let can_send_tail_fec = {
                        write_half
                            .reliable_layer
                            .lock()
                            .unwrap()
                            .can_send_tail_fec(now)
                    };
                    write_half.close_fec_burst(now, can_send_tail_fec).await?;
                }
            }
            Err(error) if error == std::io::ErrorKind::WouldBlock => {
                if let Some(ts) = echo_ts.take().or(echo_backup) {
                    write_half.ack_flush.lock().unwrap().ts_echo.restore(ts);
                }
                let mut s = write_half.ack_flush.lock().unwrap();
                s.complete_claim(pages_sent * MAX_NUM_ACK, false);
                drop(s);
                write_half.signals.session_outbound_progress.notify_one();
                break 'ack_pages;
            }
            Err(e) => {
                write_half.press_error(e, MetricsTerminationCause::AckWrite);
                if let Some(ts) = echo_ts.take().or(echo_backup) {
                    write_half.ack_flush.lock().unwrap().ts_echo.restore(ts);
                }
                return Err(e);
            }
        }
        if page_0 {
            page_0 = false;
            if history_count < MAX_NUM_ACK {
                let mut s = write_half.ack_flush.lock().unwrap();
                s.ack_page_cursor = MAX_NUM_ACK;
                s.complete_claim(claimed_acks, claimed_fin);
                s.last_ack_flush = Some(now);
                drop(s);
                write_half.signals.session_outbound_progress.notify_one();
                break;
            }
            skip = cursor;
            if skip >= history_count {
                let mut s = write_half.ack_flush.lock().unwrap();
                s.ack_page_cursor = MAX_NUM_ACK;
                s.complete_claim(claimed_acks, claimed_fin);
                s.last_ack_flush = Some(now);
                drop(s);
                write_half.signals.session_outbound_progress.notify_one();
                break;
            }
        } else {
            let mut s = write_half.ack_flush.lock().unwrap();
            s.ack_page_cursor = next_page_cursor(cursor, history_count, MAX_NUM_ACK);
            s.complete_claim(claimed_acks, claimed_fin);
            s.last_ack_flush = Some(now);
            drop(s);
            write_half.signals.session_outbound_progress.notify_one();
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn busy_ack_flush_uses_the_count_threshold_without_changing_the_age_cap() {
        let now = Instant::now();
        let mut s = AckFlushState::new();
        // The count threshold drives a busy flow whose acks arrive faster
        // than the age cap: 16 pending acks flush immediately even though
        // the last flush was instant ago.
        assert_eq!(ACK_FLUSH_COUNT, 16);
        s.pending_acks = ACK_FLUSH_COUNT - 1;
        s.last_ack_flush = Some(now);
        assert!(
            !s.is_due(now),
            "below the count threshold and inside the age cap, no flush is due"
        );
        let (due, deadline) = s.check(now);
        assert!(!due);
        assert_eq!(deadline, s.next_deadline(now));
        s.pending_acks = ACK_FLUSH_COUNT;
        assert!(
            s.is_due(now),
            "at the count threshold the flush is due regardless of the age cap"
        );
        // The age cap is unchanged: 1 ms after the last flush even a single
        // pending ack is due.
        assert_eq!(ACK_FLUSH_AGE, Duration::from_millis(1));
        let mut s = AckFlushState::new();
        s.pending_acks = 1;
        s.last_ack_flush = Some(now);
        assert!(!s.is_due(now));
        assert!(
            s.is_due(now + ACK_FLUSH_AGE),
            "the 1 ms age cap must still fire for a sparse ack flow"
        );
    }
}
