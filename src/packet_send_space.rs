use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use dre::PacketState;

const SMOOTH_RTT_ALPHA: f64 = 0.1;
const RTO_K: f64 = 1.5;
const INIT_SMOOTH_RTT_SECS: f64 = 3.;
const MAX_NUM_REUSED_BUFFERS: usize = 64;

#[derive(Debug, Clone)]
pub struct PacketSendSpace {
    next_packet: u64,
    transmitting: BTreeMap<u64, TransmittingPacket>,
    min_rtt: Duration,
    smooth_rtt: Duration,
    reused_buf: Vec<Vec<u8>>,
}
impl PacketSendSpace {
    pub fn new() -> Self {
        Self {
            next_packet: 0,
            transmitting: BTreeMap::new(),
            min_rtt: Duration::MAX,
            smooth_rtt: Duration::from_secs_f64(INIT_SMOOTH_RTT_SECS),
            reused_buf: Vec::with_capacity(MAX_NUM_REUSED_BUFFERS),
        }
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
                self.min_rtt = self.min_rtt.min(rtt);
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

    pub fn send(&mut self, data: Vec<u8>, stats: PacketState, now: Instant) -> Packet<'_> {
        let s = self.next_packet;
        self.next_packet += 1;

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
            p.sent_time = now;
            let p = Packet {
                seq: *s,
                data: &p.data,
            };
            return Some(p);
        }
        None
    }

    pub fn min_rtt(&self) -> Duration {
        self.min_rtt
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
        smooth_rtt.as_secs_f64() * RTO_K < sent_elapsed.as_secs_f64()
    }
}

pub struct Packet<'a> {
    pub seq: u64,
    pub data: &'a [u8],
}
