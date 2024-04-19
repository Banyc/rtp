use std::{collections::VecDeque, num::NonZeroUsize, time::Instant};

use dre::{ConnectionState, DetectAppLimitedPhaseParams, PacketState};
use strict_num::NonZeroPositiveF64;

use crate::{packet_send_space::PacketSendSpace, token_bucket::TokenBucket};

const SEND_DATA_BUFFER_LENGTH: usize = 2 << 12;
const RECV_DATA_BUFFER_LENGTH: usize = 2 << 12;
const INIT_BYTES_PER_SECOND: f64 = 1024.0;
const MAX_BURST_PACKETS: usize = 12;
const MSS: usize = 1024;
const SMOOTH_DELIVERY_RATE_ALPHA: f64 = 0.1;
const INIT_SMOOTH_DELIVERY_RATE: f64 = 12.;
const SMOOTH_DELIVERY_RATE_PROBE_K: f64 = 0.2;

#[derive(Debug, Clone)]
pub struct ReliableLayer {
    send_data_buf: VecDeque<u8>,
    recv_data_buf: VecDeque<u8>,
    token_bucket: TokenBucket,
    connection_stats: ConnectionState,
    packet_send_space: PacketSendSpace,
    smooth_delivery_rate: NonZeroPositiveF64,

    // Reused buffers
    packet_stats_buf: Vec<PacketState>,
    packet_buf: Vec<dre::Packet>,
}
impl ReliableLayer {
    pub fn new(now: Instant) -> Self {
        Self {
            send_data_buf: VecDeque::with_capacity(SEND_DATA_BUFFER_LENGTH),
            recv_data_buf: VecDeque::with_capacity(RECV_DATA_BUFFER_LENGTH),
            token_bucket: TokenBucket::new(
                NonZeroPositiveF64::new(INIT_BYTES_PER_SECOND).unwrap(),
                NonZeroUsize::new(MAX_BURST_PACKETS).unwrap(),
                now,
            ),
            connection_stats: ConnectionState::new(now),
            packet_send_space: PacketSendSpace::new(),
            smooth_delivery_rate: NonZeroPositiveF64::new(INIT_SMOOTH_DELIVERY_RATE).unwrap(),
            packet_stats_buf: Vec::new(),
            packet_buf: Vec::new(),
        }
    }

    pub fn send_data_buf(&mut self, buf: &[u8], now: Instant) -> usize {
        self.detect_application_limited_phases(now);

        let free_bytes = self.send_data_buf.capacity() - self.send_data_buf.len();
        let write_bytes = free_bytes.min(buf.len());
        self.send_data_buf.extend(&buf[..write_bytes]);
        write_bytes
    }

    pub fn send_data_packet(&mut self, packet: &mut [u8], now: Instant) -> Option<DataPacket> {
        self.detect_application_limited_phases(now);

        if !self.token_bucket.take_exact_tokens(1, now) {
            return None;
        }

        if let Some(p) = self.packet_send_space.retransmit(now) {
            packet[..p.data.len()].copy_from_slice(p.data);

            return Some(DataPacket {
                seq: p.seq,
                data_written: NonZeroUsize::new(p.data.len()).unwrap(),
            });
        }

        let packet_bytes = packet.len().min(MSS).min(self.send_data_buf.len());
        let packet_bytes = NonZeroUsize::new(packet_bytes)?;

        let stats = self
            .connection_stats
            .send_packet_2(now, self.packet_send_space.no_packets_in_flight());

        let mut buf = self.packet_send_space.reuse_buf().unwrap_or_default();
        let data = self.send_data_buf.drain(..packet_bytes.get());
        buf.extend(data);
        let data = buf;

        packet[..data.len()].copy_from_slice(&data);
        let p = self.packet_send_space.send(data, stats, now);

        Some(DataPacket {
            seq: p.seq,
            data_written: packet_bytes,
        })
    }

    pub fn recv_ack_packet(&mut self, ack: &[u64], now: Instant) {
        self.detect_application_limited_phases(now);

        self.packet_send_space
            .ack(ack, &mut self.packet_stats_buf, now);

        while let Some(p) = self.packet_stats_buf.pop() {
            self.packet_buf.push(dre::Packet {
                state: p,
                data_length: 1,
            })
        }
        let sr = self.connection_stats.sample_rate(
            &self.packet_buf,
            now,
            self.packet_send_space.min_rtt(),
        );
        self.packet_stats_buf.clear();
        self.packet_buf.clear();

        let Some(sr) = sr else {
            return;
        };
        let target_deliver_rate = match sr.is_app_limited() {
            true => {
                let delivery_rate =
                    sr.delivery_rate() + sr.delivery_rate() * SMOOTH_DELIVERY_RATE_PROBE_K;
                if delivery_rate < self.smooth_delivery_rate.get() {
                    return;
                }
                delivery_rate
            }
            false => sr.delivery_rate(),
        };
        let smooth_delivery_rate = self.smooth_delivery_rate.get()
            * (1. - SMOOTH_DELIVERY_RATE_ALPHA)
            + target_deliver_rate * SMOOTH_DELIVERY_RATE_ALPHA;
        self.smooth_delivery_rate = NonZeroPositiveF64::new(smooth_delivery_rate).unwrap();

        self.token_bucket
            .set_thruput(self.smooth_delivery_rate, now);
    }

    pub fn recv_data_buf(&mut self, buf: &mut [u8]) -> usize {
        let read_bytes = buf.len().min(self.recv_data_buf.len());
        let (a, b) = self.recv_data_buf.as_slices();
        let n_a = a.len().min(read_bytes);
        let n_b = read_bytes - n_a;
        buf[..n_a].copy_from_slice(&a[..n_a]);
        buf[n_a..read_bytes].copy_from_slice(&b[..n_b]);
        self.recv_data_buf.drain(..read_bytes);
        read_bytes
    }

    /// Return `true` if this packet is acknowledged; otherwise, return `false`
    pub fn recv_data_packet(&mut self, packet: &[u8]) -> bool {
        if self.recv_data_buf.capacity() - self.recv_data_buf.len() < packet.len() {
            return false;
        }
        self.recv_data_buf.extend(packet);
        true
    }

    fn detect_application_limited_phases(&mut self, now: Instant) {
        self.connection_stats
            .detect_application_limited_phases_2(DetectAppLimitedPhaseParams {
                few_data_to_send: self.send_data_buf.len() < MSS,
                not_transmitting_a_packet: self.packet_send_space.num_transmitting_packets() == 0,
                cwnd_not_full: 0 < self.token_bucket.tokens(now),
                all_lost_packets_retransmitted: self
                    .packet_send_space
                    .all_lost_packets_retransmitted(now),
                pipe: self.packet_send_space.num_transmitting_packets() as u64,
            });
    }
}

#[derive(Debug, Clone)]
pub struct DataPacket {
    pub seq: u64,
    pub data_written: NonZeroUsize,
}
