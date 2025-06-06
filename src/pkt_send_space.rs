use core::{num::NonZeroUsize, time::Duration};
use std::time::Instant;

use dre::PacketState;
use primitive::{
    arena::obj_pool::{ObjPool, buf_pool},
    ops::{
        float::{PosR, UnitR},
        len::LenExt,
        opt_cmp::MinNoneOptCmp,
    },
    queue::send_wnd::SendWnd,
};

use crate::{pkt_recv_space::MAX_NUM_RECVING_PKTS, rto::RtxTimer, sack::AckBallSequence};

pub const INIT_CWND: usize = 16;

#[derive(Debug)]
pub struct PktSendSpace {
    send_wnd: SendWnd<u64, Option<TxingPkt>>,
    num_txing: usize,
    min_rtt: Option<Duration>,
    rto: RtxTimer,
    reused_buf: ObjPool<Vec<u8>>,
    cwnd: NonZeroUsize,
    /// Detect if the peer has died
    resp_wait_start: Option<Instant>,
    out_of_order_seq_end: u64,
    /// Any sequence after this does not participate in data loss analysis.
    max_pipe_seq: MinNoneOptCmp<u64>,

    // reused buffers
    unacked_buf: Vec<u64>,
    ack_buf: Vec<u64>,
}
impl PktSendSpace {
    pub fn new() -> Self {
        Self {
            send_wnd: SendWnd::new(0),
            num_txing: 0,
            min_rtt: None,
            rto: RtxTimer::new(),
            reused_buf: buf_pool(Some(MAX_NUM_RECVING_PKTS)),
            cwnd: NonZeroUsize::new(INIT_CWND).unwrap(),
            resp_wait_start: None,
            out_of_order_seq_end: 0,
            max_pipe_seq: MinNoneOptCmp(None),
            unacked_buf: vec![],
            ack_buf: vec![],
        }
    }

    fn unacked(
        send_wnd: &SendWnd<u64, Option<TxingPkt>>,
    ) -> impl Iterator<Item = (u64, &TxingPkt)> {
        send_wnd
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
    }
    fn unacked_mut(
        send_wnd: &mut SendWnd<u64, Option<TxingPkt>>,
    ) -> impl Iterator<Item = (u64, &mut TxingPkt)> {
        send_wnd
            .iter_mut()
            .filter_map(|(k, v)| v.as_mut().map(|v| (k, v)))
    }

    pub fn cwnd(&self) -> NonZeroUsize {
        self.cwnd
    }

    pub fn next_seq(&self) -> u64 {
        *self.send_wnd.next().unwrap()
    }

    pub fn smooth_rtt(&self) -> Duration {
        self.rto.smooth_rtt()
    }

    pub fn num_rtxed_pkts(&self) -> usize {
        let mut n = 0;
        for (_, p) in Self::unacked(&self.send_wnd) {
            if !p.rtxed {
                continue;
            }
            n += 1;
        }
        n
    }

    pub fn reused_buf(&mut self) -> &mut ObjPool<Vec<u8>> {
        &mut self.reused_buf
    }

    pub fn no_resp_for(&self, now: Instant) -> Option<Duration> {
        Some(now.duration_since(self.resp_wait_start?))
    }

    pub fn ack(&mut self, recved: AckBallSequence<'_>, acked: &mut Vec<PacketState>, now: Instant) {
        let next_unacked = self.send_wnd.start().or(self.send_wnd.next()).copied();
        let peer_waiting_for_acked_pkts =
            next_unacked.is_some_and(|next_unacked| recved.first_unacked() < next_unacked);
        if let Some(seq) = recved.out_of_order_seq_end() {
            self.out_of_order_seq_end = self.out_of_order_seq_end.max(seq);
        }
        self.unacked_buf.clear();
        self.unacked_buf
            .extend(Self::unacked(&self.send_wnd).map(|(k, _)| k));
        self.ack_buf.clear();
        recved.ack(&self.unacked_buf, &mut self.ack_buf);
        for &s in &self.ack_buf {
            let p = self.send_wnd.get_mut(&s).unwrap();
            let p = p.take().unwrap();
            self.num_txing -= 1;
            if s == *self.send_wnd.start().unwrap() {
                self.send_wnd.pop().unwrap();
                self.send_wnd.pop_none();
            }

            if !p.rtxed {
                let rtt = now - p.sent_time;
                self.min_rtt = Some(match self.min_rtt {
                    Some(min_rtt) => min_rtt.min(rtt),
                    None => rtt,
                });
                self.rto.set(rtt);
            }
            self.reused_buf.put(p.data);
            acked.push(p.stats);
        }
        if peer_waiting_for_acked_pkts {
            return;
        }
        if self.send_wnd.is_empty() {
            self.resp_wait_start = None;
        } else {
            self.resp_wait_start = Some(now);
        }
    }

    pub fn accepts_new_pkt(&self) -> bool {
        self.num_txing < self.cwnd.get()
    }

    pub fn send(&mut self, data: Vec<u8>, stats: PacketState, now: Instant) -> Pkt<'_> {
        let s = *self.send_wnd.next().unwrap();

        self.max_pipe_seq = MinNoneOptCmp(Some(s));

        let p = TxingPkt {
            stats,
            sent_time: now,
            rtxed: false,
            considered_new_in_cwnd: false,
            data,
            rto: self.rto.rto(),
        };

        self.send_wnd.push(Some(p));
        self.num_txing += 1;
        if self.resp_wait_start.is_none() {
            self.resp_wait_start = Some(now);
        }

        Pkt {
            seq: s,
            data: &self.send_wnd.get(&s).unwrap().as_ref().unwrap().data,
        }
    }

    pub fn rtx(&mut self, now: Instant) -> Option<Pkt<'_>> {
        for (s, p) in Self::unacked_mut(&mut self.send_wnd).take(self.cwnd.get()) {
            let out_of_order_rt =
                s < self.out_of_order_seq_end && p.hits_custom_rto(self.rto.smooth_rtt(), now);

            let should_rtx = p.hits_rto(now) || out_of_order_rt;
            if !should_rtx {
                continue;
            }

            // fresh pkt for this cwnd
            let considered_new_in_cwnd = if self.max_pipe_seq < MinNoneOptCmp(Some(s)) {
                self.max_pipe_seq = MinNoneOptCmp(Some(s));
                true
            } else {
                false
            };

            self_assign::self_assign! {
                p = TxingPkt {
                    stats: _,
                    sent_time: now,
                    rtxed: true,
                    considered_new_in_cwnd,
                    data: _,
                    rto: self.rto.rto(),
                };
            }
            let p = Pkt {
                seq: s,
                data: &p.data,
            };
            return Some(p);
        }
        None
    }

    pub fn min_rtt(&self) -> Option<Duration> {
        self.min_rtt
    }

    pub fn set_send_rate(&mut self, send_rate: PosR<f64>) {
        let cwnd = self.rto.smooth_rtt().as_secs_f64() * send_rate.get();
        let cwnd = cwnd.round() as usize;
        let cwnd = cwnd * 8;
        let cwnd = 1.max(cwnd);
        self.cwnd = NonZeroUsize::new(cwnd).unwrap();

        let last_seq_in_cwnd = || {
            if let Some(s) = Self::unacked(&self.send_wnd).map(|(k, _)| k).nth(cwnd) {
                return Some(s);
            }
            if let Some(s) = Self::unacked(&self.send_wnd).map(|(k, _)| k).last() {
                return Some(s);
            }
            None
        };
        let last_seq_in_cwnd = last_seq_in_cwnd();

        // Retract max sequence in pipe
        if MinNoneOptCmp(last_seq_in_cwnd) < self.max_pipe_seq {
            self.max_pipe_seq = MinNoneOptCmp(last_seq_in_cwnd);
        }
    }

    pub fn no_pkts_in_flight(&self) -> bool {
        self.send_wnd.is_empty()
    }

    pub fn cwnd_stats(&self, now: Instant) -> CwndStats {
        let mut not_lost = 0;
        let mut all_lost_pkts_rtxed = true;
        for p in Self::unacked(&self.send_wnd)
            .map(|(_, v)| v)
            .take(self.cwnd.get())
        {
            if p.hits_rto(now) {
                all_lost_pkts_rtxed = false;
            } else {
                not_lost += 1;
            }
        }
        CwndStats {
            all_lost_pkts_rtxed,
            num_not_lost_txing_pkts: not_lost,
        }
    }
    pub fn all_lost_pkts_rtxed(&self, now: Instant) -> bool {
        for p in Self::unacked(&self.send_wnd)
            .map(|(_, v)| v)
            .take(self.cwnd.get())
        {
            if p.hits_rto(now) {
                return false;
            }
        }
        true
    }
    pub fn num_not_lost_txing_pkts(&self, now: Instant) -> usize {
        let mut not_lost = 0;
        for p in Self::unacked(&self.send_wnd)
            .map(|(_, v)| v)
            .take(self.cwnd.get())
        {
            if p.hits_rto(now) {
                continue;
            }
            not_lost += 1;
        }
        not_lost
    }

    pub fn num_txing_pkts(&self) -> usize {
        self.num_txing
    }

    pub fn huge_data_loss(&self, tolerant_loss_rate: UnitR<f64>, now: Instant) -> bool {
        let Some(data_loss_rate) = self.data_loss_rate(now) else {
            return false;
        };
        let enough_samples_for_stats = INIT_CWND < self.pkts_in_pipe().count();
        enough_samples_for_stats && tolerant_loss_rate.get() < data_loss_rate
    }

    pub fn data_loss_rate(&self, now: Instant) -> Option<f64> {
        let mut lost = 0;
        let mut len = 0;
        for (_, p) in self.pkts_in_pipe() {
            len += 1;
            let rtxed = !p.considered_new_in_cwnd && p.rtxed;
            if rtxed || p.hits_rto(now) {
                lost += 1;
            }
        }
        if len == 0 {
            return None;
        }
        Some(lost as f64 / len as f64)
    }

    pub fn num_pkts_in_pipe(&self) -> usize {
        self.pkts_in_pipe().count()
    }

    fn pkts_in_pipe(&self) -> impl Iterator<Item = (u64, &TxingPkt)> + '_ {
        Self::unacked(&self.send_wnd)
            .take_while(|(s, _)| MinNoneOptCmp(Some(*s)) <= self.max_pipe_seq)
    }

    pub fn next_poll_time(&self) -> Option<Instant> {
        let mut min_next_poll_time: Option<Instant> = None;
        for p in Self::unacked(&self.send_wnd)
            .map(|(_, v)| v)
            .take(self.cwnd.get())
        {
            let t = p.next_rto_time();
            let t = min_next_poll_time.map(|min| min.min(t)).unwrap_or(t);
            min_next_poll_time = Some(t);
        }
        min_next_poll_time
    }

    pub fn rto_duration(&self) -> Duration {
        self.rto.rto()
    }
}
impl Default for PktSendSpace {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CwndStats {
    pub all_lost_pkts_rtxed: bool,
    pub num_not_lost_txing_pkts: usize,
}

#[derive(Debug, Clone)]
struct TxingPkt {
    pub stats: PacketState,
    pub sent_time: Instant,
    pub rtxed: bool,
    pub considered_new_in_cwnd: bool,
    pub data: Vec<u8>,
    pub rto: Duration,
}
impl TxingPkt {
    pub fn hits_rto(&self, now: Instant) -> bool {
        let sent_elapsed = now.duration_since(self.sent_time);
        self.rto <= sent_elapsed
    }

    pub fn hits_custom_rto(&self, rto: Duration, now: Instant) -> bool {
        let sent_elapsed = now.duration_since(self.sent_time);
        rto <= sent_elapsed
    }

    pub fn next_rto_time(&self) -> Instant {
        self.sent_time + self.rto
    }
}

#[derive(Debug, Clone)]
pub struct Pkt<'a> {
    pub seq: u64,
    pub data: &'a [u8],
}
