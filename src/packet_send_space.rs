use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use dre::PacketState;
use primitive::{
    arena::obj_pool::{buf_pool, ObjectPool},
    ops::{
        float::{PosF, UnitF},
        len::{Len, LenExt},
        opt_cmp::MinNoneOptCmp,
    },
    queue::send_wnd::SendWnd,
};

use crate::{
    packet_recv_space::MAX_NUM_RECEIVING_PACKETS, rto::RetransmissionTimer, sack::AckBallSequence,
};

pub const INIT_CWND: usize = 16;

#[derive(Debug)]
pub struct PacketSendSpace {
    send_wnd: SendWnd<u64, Option<TransmittingPacket>>,
    min_rtt: Option<Duration>,
    rto: RetransmissionTimer,
    reused_buf: ObjectPool<Vec<u8>>,
    cwnd: NonZeroUsize,
    /// Detect if the peer has died
    response_wait_start: Option<Instant>,
    out_of_order_seq_end: u64,
    /// Any sequence after this does not participate in data loss analysis.
    max_pipe_seq: MinNoneOptCmp<u64>,

    // reused buffers
    pipe_buf: Vec<u64>,
    ack_buf: Vec<u64>,
}
impl PacketSendSpace {
    pub fn new() -> Self {
        Self {
            send_wnd: SendWnd::new(0),
            min_rtt: None,
            rto: RetransmissionTimer::new(),
            reused_buf: buf_pool(Some(MAX_NUM_RECEIVING_PACKETS)),
            cwnd: NonZeroUsize::new(INIT_CWND).unwrap(),
            response_wait_start: None,
            out_of_order_seq_end: 0,
            max_pipe_seq: MinNoneOptCmp(None),
            pipe_buf: vec![],
            ack_buf: vec![],
        }
    }

    fn unacked(
        send_wnd: &SendWnd<u64, Option<TransmittingPacket>>,
    ) -> impl Iterator<Item = (u64, &TransmittingPacket)> {
        send_wnd
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
    }
    fn unacked_mut(
        send_wnd: &mut SendWnd<u64, Option<TransmittingPacket>>,
    ) -> impl Iterator<Item = (u64, &mut TransmittingPacket)> {
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

    pub fn num_retransmitted_packets(&self) -> usize {
        let mut n = 0;
        for (_, p) in self.send_wnd.iter() {
            let Some(p) = p else {
                continue;
            };
            if !p.retransmitted {
                continue;
            }
            n += 1;
        }
        n
    }

    pub fn reused_buf(&mut self) -> &mut ObjectPool<Vec<u8>> {
        &mut self.reused_buf
    }

    pub fn no_response_for(&self, now: Instant) -> Option<Duration> {
        Some(now.duration_since(self.response_wait_start?))
    }

    pub fn ack(&mut self, seq: AckBallSequence<'_>, acked: &mut Vec<PacketState>, now: Instant) {
        if let Some(seq) = seq.out_of_order_seq_end() {
            self.out_of_order_seq_end = self.out_of_order_seq_end.max(seq);
        }
        self.pipe_buf.clear();
        self.pipe_buf
            .extend(Self::unacked(&self.send_wnd).map(|(k, _)| k));
        self.ack_buf.clear();
        seq.ack(&self.pipe_buf, &mut self.ack_buf);
        for &s in &self.ack_buf {
            let Some(p) = self.send_wnd.get_mut(&s) else {
                continue;
            };
            let Some(p) = p.take() else {
                continue;
            };
            if s == *self.send_wnd.start().unwrap() {
                self.send_wnd.pop().unwrap();
                self.send_wnd.pop_none();
            }

            if !p.retransmitted {
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
        if self.send_wnd.is_empty() {
            self.response_wait_start = None;
        } else {
            self.response_wait_start = Some(now);
        }
    }

    pub fn accepts_new_packet(&self) -> bool {
        self.send_wnd.len() < self.cwnd.get()
    }

    pub fn send(&mut self, data: Vec<u8>, stats: PacketState, now: Instant) -> Packet<'_> {
        let s = *self.send_wnd.next().unwrap();

        self.max_pipe_seq = MinNoneOptCmp(Some(s));

        let p = TransmittingPacket {
            stats,
            sent_time: now,
            retransmitted: false,
            considered_new_in_cwnd: false,
            data,
            rto: self.rto.rto(),
        };

        self.send_wnd.push(Some(p));
        if self.response_wait_start.is_none() {
            self.response_wait_start = Some(now);
        }

        let p = Packet {
            seq: s,
            data: &self.send_wnd.get(&s).unwrap().as_ref().unwrap().data,
        };
        p
    }

    pub fn retransmit(&mut self, now: Instant) -> Option<Packet<'_>> {
        for (s, p) in Self::unacked_mut(&mut self.send_wnd).take(self.cwnd.get()) {
            let out_of_order_rt =
                s < self.out_of_order_seq_end && p.hits_custom_rto(self.rto.smooth_rtt(), now);

            let should_retransmit = p.hits_rto(now) || out_of_order_rt;
            if !should_retransmit {
                continue;
            }

            // fresh packet for this cwnd
            let considered_new_in_cwnd = if self.max_pipe_seq < MinNoneOptCmp(Some(s)) {
                self.max_pipe_seq = MinNoneOptCmp(Some(s));
                true
            } else {
                false
            };

            self_assign::self_assign! {
                p = TransmittingPacket {
                    stats: _,
                    sent_time: now,
                    retransmitted: true,
                    considered_new_in_cwnd,
                    data: _,
                    rto: self.rto.rto(),
                };
            }
            let p = Packet {
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

    pub fn set_send_rate(&mut self, send_rate: PosF<f64>) {
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

    pub fn no_packets_in_flight(&self) -> bool {
        self.send_wnd.is_empty()
    }

    pub fn all_lost_packets_retransmitted(&self, now: Instant) -> bool {
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

    pub fn num_not_lost_transmitting_packets(&self, now: Instant) -> usize {
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

    pub fn num_transmitting_packets(&self) -> usize {
        self.send_wnd.len()
    }

    pub fn huge_data_loss(&self, tolerant_loss_rate: UnitF<f64>, now: Instant) -> bool {
        let Some(data_loss_rate) = self.data_loss_rate(now) else {
            return false;
        };
        let enough_samples_for_stats = INIT_CWND < self.packets_in_pipe().count();
        enough_samples_for_stats && tolerant_loss_rate.get() < data_loss_rate
    }

    pub fn data_loss_rate(&self, now: Instant) -> Option<f64> {
        let len = self.packets_in_pipe().count();
        if len == 0 {
            return None;
        }
        let mut lost = 0;
        for (_, p) in self.packets_in_pipe() {
            let retransmitted = !p.considered_new_in_cwnd && p.retransmitted;
            if retransmitted || p.hits_rto(now) {
                lost += 1;
            }
        }
        Some(lost as f64 / len as f64)
    }

    pub fn num_packets_in_pipe(&self) -> usize {
        self.packets_in_pipe().count()
    }

    fn packets_in_pipe(&self) -> impl Iterator<Item = (u64, &TransmittingPacket)> + '_ {
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
impl Default for PacketSendSpace {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
struct TransmittingPacket {
    pub stats: PacketState,
    pub sent_time: Instant,
    pub retransmitted: bool,
    pub considered_new_in_cwnd: bool,
    pub data: Vec<u8>,
    pub rto: Duration,
}
impl TransmittingPacket {
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
pub struct Packet<'a> {
    pub seq: u64,
    pub data: &'a [u8],
}
