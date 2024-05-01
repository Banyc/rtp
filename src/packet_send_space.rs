use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use dre::PacketState;
use strict_num::NonZeroPositiveF64;

use crate::sack::AckBallSequence;

const SMOOTH_RTT_ALPHA: f64 = 0.1;
const RTO_K: f64 = 1.;
const INIT_SMOOTH_RTT_SECS: f64 = 3.;
const MAX_NUM_REUSED_BUFFERS: usize = 64;
pub const INIT_CWND: usize = 16;

#[derive(Debug, Clone)]
pub struct PacketSendSpace {
    next_seq: u64,
    transmitting: BTreeMap<u64, TransmittingPacket>,
    min_rtt: Option<Duration>,
    smooth_rtt: Duration,
    reused_buf: Vec<Vec<u8>>,
    cwnd: NonZeroUsize,
    response_wait_start: Option<Instant>,
    imm_retrans_seq_end: u64,

    // reused buffers
    pipe_buf: Vec<u64>,
    ack_buf: Vec<u64>,
}
impl PacketSendSpace {
    pub fn new() -> Self {
        Self {
            next_seq: 0,
            transmitting: BTreeMap::new(),
            min_rtt: None,
            smooth_rtt: Duration::from_secs_f64(INIT_SMOOTH_RTT_SECS),
            reused_buf: Vec::with_capacity(MAX_NUM_REUSED_BUFFERS),
            cwnd: NonZeroUsize::new(INIT_CWND).unwrap(),
            response_wait_start: None,
            imm_retrans_seq_end: 0,
            pipe_buf: vec![],
            ack_buf: vec![],
        }
    }

    pub fn cwnd(&self) -> NonZeroUsize {
        self.cwnd
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn smooth_rtt(&self) -> Duration {
        self.smooth_rtt
    }

    pub fn num_retransmitted_packets(&self) -> usize {
        let mut n = 0;
        for p in self.transmitting.values() {
            if !p.retransmitted {
                continue;
            }
            n += 1;
        }
        n
    }

    pub fn reuse_buf(&mut self) -> Option<Vec<u8>> {
        self.reused_buf.pop()
    }

    pub fn no_response_for(&self, now: Instant) -> Option<Duration> {
        Some(now.duration_since(self.response_wait_start?))
    }

    pub fn ack(&mut self, seq: AckBallSequence<'_>, acked: &mut Vec<PacketState>, now: Instant) {
        if let Some(seq) = seq.imm_retrans_seq_end() {
            self.imm_retrans_seq_end = seq;
        }
        self.pipe_buf.clear();
        self.pipe_buf.extend(self.transmitting.keys());
        self.ack_buf.clear();
        seq.ack(&self.pipe_buf, &mut self.ack_buf);
        for s in &self.ack_buf {
            let Some(p) = self.transmitting.remove(s) else {
                continue;
            };
            if !p.retransmitted {
                let rtt = now - p.sent_time;
                self.min_rtt = Some(match self.min_rtt {
                    Some(min_rtt) => min_rtt.min(rtt),
                    None => rtt,
                });
                let smooth_rtt = self.smooth_rtt.as_secs_f64() * (1. - SMOOTH_RTT_ALPHA)
                    + rtt.as_secs_f64() * SMOOTH_RTT_ALPHA;
                self.smooth_rtt = Duration::from_secs_f64(smooth_rtt);
            }
            if self.reused_buf.len() != self.reused_buf.capacity() {
                let mut buf = p.data;
                buf.clear();
                self.reused_buf.push(buf);
            }
            acked.push(p.stats);
        }
        if self.transmitting.is_empty() {
            self.response_wait_start = None;
        } else {
            self.response_wait_start = Some(now);
        }
    }

    pub fn accepts_new_packet(&self) -> bool {
        self.transmitting.len() < self.cwnd.get()
    }

    pub fn send(&mut self, data: Vec<u8>, stats: PacketState, now: Instant) -> Packet<'_> {
        let s = self.next_seq;
        self.next_seq += 1;

        let p = TransmittingPacket {
            stats,
            sent_time: now,
            retransmitted: false,
            data,
        };

        self.transmitting.insert(s, p);
        if self.response_wait_start.is_none() {
            self.response_wait_start = Some(now);
        }

        let p = Packet {
            seq: s,
            data: &self.transmitting.get(&s).unwrap().data,
        };
        p
    }

    pub fn retransmit(&mut self, now: Instant) -> Option<Packet<'_>> {
        for (s, p) in self.transmitting.iter_mut().take(self.cwnd.get()) {
            let out_of_order_rt =
                *s < self.imm_retrans_seq_end && p.rto(self.smooth_rtt.div_f64(2.), now);

            if !p.rto(self.smooth_rtt, now) || !out_of_order_rt {
                continue;
            }

            p.retransmitted = true;
            p.sent_time = now;
            let p = Packet {
                seq: *s,
                data: &p.data,
            };
            return Some(p);
        }
        None
    }

    pub fn min_rtt(&self) -> Option<Duration> {
        self.min_rtt
    }

    pub fn set_send_rate(&mut self, send_rate: NonZeroPositiveF64) {
        let Some(min_rtt) = self.min_rtt else {
            return;
        };
        let cwnd = min_rtt.as_secs_f64() * send_rate.get();
        let cwnd = INIT_CWND.max(cwnd.round() as usize);
        self.cwnd = NonZeroUsize::new(cwnd).unwrap();
    }

    pub fn no_packets_in_flight(&self) -> bool {
        self.transmitting.is_empty()
    }

    pub fn all_lost_packets_retransmitted(&self, now: Instant) -> bool {
        for p in self.transmitting.values().take(self.cwnd.get()) {
            if p.rto(self.smooth_rtt, now) {
                return false;
            }
        }
        true
    }

    pub fn num_not_lost_transmitting_packets(&self, now: Instant) -> usize {
        let mut not_lost = 0;
        for p in self.transmitting.values().take(self.cwnd.get()) {
            if p.rto(self.smooth_rtt, now) {
                continue;
            }
            not_lost += 1;
        }
        not_lost
    }

    pub fn num_transmitting_packets(&self) -> usize {
        self.transmitting.len()
    }

    pub fn data_loss_rate(&self, now: Instant) -> Option<f64> {
        let len = self.transmitting.iter().take(self.cwnd.get()).len();
        if len == 0 {
            return None;
        }
        let mut lost = 0;
        for p in self.transmitting.values().take(len) {
            if p.retransmitted || p.rto(self.smooth_rtt, now) {
                lost += 1;
            }
        }
        Some(lost as f64 / len as f64)
    }

    pub fn next_poll_time(&self) -> Option<Instant> {
        let mut min_next_poll_time: Option<Instant> = None;
        for p in self.transmitting.values().take(self.cwnd.get()) {
            let t = p.next_rto_time(self.smooth_rtt);
            let t = min_next_poll_time.map(|min| min.min(t)).unwrap_or(t);
            min_next_poll_time = Some(t);
        }
        min_next_poll_time
    }

    pub fn rto_duration(&self) -> Duration {
        rto_duration(self.smooth_rtt)
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
    pub data: Vec<u8>,
}
impl TransmittingPacket {
    pub fn rto(&self, smooth_rtt: Duration, now: Instant) -> bool {
        let sent_elapsed = now.duration_since(self.sent_time);
        rto_duration(smooth_rtt) <= sent_elapsed
    }

    pub fn next_rto_time(&self, smooth_rtt: Duration) -> Instant {
        let rto_duration = rto_duration(smooth_rtt);
        self.sent_time + rto_duration
    }
}

fn rto_duration(smooth_rtt: Duration) -> Duration {
    smooth_rtt + Duration::from_secs_f64(smooth_rtt.as_secs_f64() * RTO_K)
}

#[derive(Debug, Clone)]
pub struct Packet<'a> {
    pub seq: u64,
    pub data: &'a [u8],
}
