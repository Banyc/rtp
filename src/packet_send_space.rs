use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use dre::PacketState;
use strict_num::{NonZeroPositiveF64, NormalizedF64};

use crate::{
    comp_option::CompOption, packet_recv_space::MAX_NUM_RECEIVING_PACKETS, reused_buf::ReusedBuf,
    sack::AckBallSequence,
};

const SMOOTH_RTT_ALPHA: f64 = 0.1;
const RTO_K: f64 = 1.;
const INIT_SMOOTH_RTT_SECS: f64 = 3.;
pub const INIT_CWND: usize = 16;

#[derive(Debug, Clone)]
pub struct PacketSendSpace {
    next_seq: u64,
    transmitting: BTreeMap<u64, TransmittingPacket>,
    min_rtt: Option<Duration>,
    smooth_rtt: Duration,
    reused_buf: ReusedBuf<Vec<u8>>,
    cwnd: NonZeroUsize,
    /// Detect if the peer has died
    response_wait_start: Option<Instant>,
    out_of_order_seq_end: u64,
    /// Any sequence after this does not participate in data loss analysis.
    max_pipe_seq: CompOption<u64>,

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
            reused_buf: ReusedBuf::new(MAX_NUM_RECEIVING_PACKETS),
            cwnd: NonZeroUsize::new(INIT_CWND).unwrap(),
            response_wait_start: None,
            out_of_order_seq_end: 0,
            max_pipe_seq: CompOption::new(None),
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

    pub fn reused_buf(&mut self) -> &mut ReusedBuf<Vec<u8>> {
        &mut self.reused_buf
    }

    pub fn no_response_for(&self, now: Instant) -> Option<Duration> {
        Some(now.duration_since(self.response_wait_start?))
    }

    pub fn ack(&mut self, seq: AckBallSequence<'_>, acked: &mut Vec<PacketState>, now: Instant) {
        if let Some(seq) = seq.out_of_order_seq_end() {
            self.out_of_order_seq_end = seq;
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
            self.reused_buf.return_buf(p.data);
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

        self.max_pipe_seq.set(Some(s));

        let p = TransmittingPacket {
            stats,
            sent_time: now,
            retransmitted: false,
            considered_new_in_cwnd: false,
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
            let out_of_order_rt = *s < self.out_of_order_seq_end && p.rto(self.smooth_rtt, now);

            let should_retransmit = p.tolerated_rto(self.smooth_rtt, now) || out_of_order_rt;
            if !should_retransmit {
                continue;
            }

            // fresh packet for this cwnd
            if self.max_pipe_seq < CompOption::new(Some(*s)) {
                self.max_pipe_seq.set(Some(*s));
                p.considered_new_in_cwnd = true;
            } else {
                p.considered_new_in_cwnd = false;
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
        let cwnd = 1.max(cwnd.round() as usize);
        self.cwnd = NonZeroUsize::new(cwnd).unwrap();

        let last_seq_in_cwnd = || {
            if let Some(s) = self.transmitting.keys().nth(cwnd) {
                return Some(*s);
            }
            if let Some(s) = self.transmitting.keys().last() {
                return Some(*s);
            }
            None
        };
        let last_seq_in_cwnd = last_seq_in_cwnd();

        // Retract max sequence in pipe
        if CompOption::new(last_seq_in_cwnd) < self.max_pipe_seq {
            self.max_pipe_seq.set(last_seq_in_cwnd);
        }
    }

    pub fn no_packets_in_flight(&self) -> bool {
        self.transmitting.is_empty()
    }

    pub fn all_lost_packets_retransmitted(&self, now: Instant) -> bool {
        for p in self.transmitting.values().take(self.cwnd.get()) {
            if p.tolerated_rto(self.smooth_rtt, now) {
                return false;
            }
        }
        true
    }

    pub fn num_not_lost_transmitting_packets(&self, now: Instant) -> usize {
        let mut not_lost = 0;
        for p in self.transmitting.values().take(self.cwnd.get()) {
            if p.tolerated_rto(self.smooth_rtt, now) {
                continue;
            }
            not_lost += 1;
        }
        not_lost
    }

    pub fn num_transmitting_packets(&self) -> usize {
        self.transmitting.len()
    }

    pub fn huge_data_loss(&self, tolerant_loss_rate: NormalizedF64, now: Instant) -> bool {
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
            if retransmitted || p.tolerated_rto(self.smooth_rtt, now) {
                lost += 1;
            }
        }
        Some(lost as f64 / len as f64)
    }

    pub fn num_packets_in_pipe(&self) -> usize {
        self.packets_in_pipe().count()
    }

    fn packets_in_pipe(&self) -> impl Iterator<Item = (&u64, &TransmittingPacket)> + '_ {
        self.transmitting
            .iter()
            .take_while(|(s, _)| CompOption::new(Some(**s)) <= self.max_pipe_seq)
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
    pub considered_new_in_cwnd: bool,
    pub data: Vec<u8>,
}
impl TransmittingPacket {
    pub fn rto(&self, timeout: Duration, now: Instant) -> bool {
        let sent_elapsed = now.duration_since(self.sent_time);
        timeout <= sent_elapsed
    }

    pub fn tolerated_rto(&self, smooth_rtt: Duration, now: Instant) -> bool {
        self.rto(rto_duration(smooth_rtt), now)
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
