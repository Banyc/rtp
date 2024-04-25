use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use dre::PacketState;
use strict_num::NonZeroPositiveF64;

const SMOOTH_RTT_ALPHA: f64 = 0.1;
const RTO_K: f64 = 1.;
const INIT_SMOOTH_RTT_SECS: f64 = 3.;
const MAX_NUM_REUSED_BUFFERS: usize = 64;
const INIT_CWND: usize = 16;

#[derive(Debug, Clone)]
pub struct PacketSendSpace {
    next_seq: u64,
    transmitting: BTreeMap<u64, TransmittingPacket>,
    min_rtt: Option<Duration>,
    smooth_rtt: Duration,
    reused_buf: Vec<Vec<u8>>,
    cwnd: NonZeroUsize,
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

    pub fn ack(&mut self, seq: &[u64], acked: &mut Vec<PacketState>, now: Instant) {
        for s in seq {
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
        let p = Packet {
            seq: s,
            data: &self.transmitting.get(&s).unwrap().data,
        };
        p
    }

    pub fn retransmit(&mut self, now: Instant) -> Option<Packet<'_>> {
        for (s, p) in &mut self.transmitting {
            if !p.rto(self.smooth_rtt, now) {
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
        let cwnd = cwnd.round() as usize;
        self.cwnd = NonZeroUsize::new(cwnd).unwrap_or(NonZeroUsize::new(1).unwrap());
    }

    pub fn no_packets_in_flight(&self) -> bool {
        self.transmitting.is_empty()
    }

    pub fn all_lost_packets_retransmitted(&self, now: Instant) -> bool {
        for p in self.transmitting.values() {
            if p.rto(self.smooth_rtt, now) {
                return false;
            }
        }
        true
    }

    pub fn num_transmitting_packets(&self) -> usize {
        self.transmitting.len()
    }

    pub fn data_loss_rate(&self, now: Instant) -> Option<f64> {
        if self.transmitting.is_empty() {
            return None;
        }
        let mut lost = 0;
        for p in self.transmitting.values() {
            if p.retransmitted || p.rto(self.smooth_rtt, now) {
                lost += 1;
            }
        }
        Some(lost as f64 / self.transmitting.len() as f64)
    }

    pub fn next_poll_time(&self) -> Option<Instant> {
        let mut min_next_poll_time: Option<Instant> = None;
        for p in self.transmitting.values() {
            let t = p.next_rto_time(self.smooth_rtt);
            let t = min_next_poll_time.map(|min| min.min(t)).unwrap_or(t);
            min_next_poll_time = Some(t);
        }
        min_next_poll_time
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

pub struct Packet<'a> {
    pub seq: u64,
    pub data: &'a [u8],
}
