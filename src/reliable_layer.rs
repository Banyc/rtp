use core::num::NonZeroUsize;
use std::{
    sync::{Arc, LazyLock},
    time::Instant,
};

use dre::{ConnectionState, PacketState};
use primitive::{
    io::token_bucket::TokenBucket,
    ops::{
        clear::Clear,
        float::{PosR, UnitR},
        len::{Capacity, Len, LenExt},
    },
    queue::cap_queue::CapVecQueue,
    sync::mutex::SpinMutex,
    time::timer::Timer,
};
use serde::{Deserialize, Serialize};

use crate::{
    codec::data_overhead, pkt_recv_space::PktRecvSpace, pkt_send_space::PktSendSpace,
    sack::AckBallSequence,
};

const SEND_DATA_BUF_LEN: usize = 2 << 16;
const RECV_DATA_BUF_LEN: usize = 2 << 16;
const MAX_BURST_PKTS: usize = 64;
const SMOOTH_SEND_RATE_ALPHA: f64 = 0.4;
const MIN_SEND_RATE: f64 = 1.;
const INIT_SEND_RATE: f64 = 128.;
const SEND_RATE_PROBE_RATE: f64 = 1.;
const CC_DATA_LOSS_RATE: f64 = 0.2;
const MAX_DATA_LOSS_RATE: f64 = 0.9;
const PRINT_DEBUG_MSGS: bool = false;

static GLOBAL_INIT_SEND_RATE: LazyLock<Arc<SpinMutex<PosR<f64>>>> =
    LazyLock::new(|| Arc::new(SpinMutex::new(PosR::new(INIT_SEND_RATE).unwrap())));

#[derive(Debug, Clone)]
enum SendFinBuf {
    Empty,
    Some,
    EmptyAndBlocked,
}

#[derive(Debug)]
pub struct ReliableLayer {
    mss: NonZeroUsize,
    send_data_buf: CapVecQueue<u8>,
    send_fin_buf: SendFinBuf,
    recv_data_buf: CapVecQueue<u8>,
    /// set-only
    recv_fin_buf: bool,
    token_bucket: TokenBucket,
    connection_stats: ConnectionState,
    pkt_send_space: PktSendSpace,
    pkt_recv_space: PktRecvSpace,
    send_rate: PosR<f64>,
    prev_sample_rate: Option<dre::RateSample>,
    huge_data_loss_timer: Timer,

    // Reused buffers
    pkt_stats_buf: Vec<PacketState>,
    pkt_buf: Vec<dre::Packet>,
}
impl ReliableLayer {
    pub fn new(mss: NonZeroUsize, now: Instant) -> Self {
        let init_send_rate = GLOBAL_INIT_SEND_RATE.clone();
        let send_rate = *init_send_rate.lock();
        Self {
            mss,
            send_data_buf: CapVecQueue::new_vec(SEND_DATA_BUF_LEN),
            send_fin_buf: SendFinBuf::Empty,
            recv_data_buf: CapVecQueue::new_vec(RECV_DATA_BUF_LEN),
            recv_fin_buf: false,
            token_bucket: TokenBucket::new(
                send_rate,
                NonZeroUsize::new(MAX_BURST_PKTS).unwrap(),
                now,
            ),
            connection_stats: ConnectionState::new(now),
            pkt_send_space: PktSendSpace::new(),
            pkt_recv_space: PktRecvSpace::new(),
            send_rate,
            prev_sample_rate: None,
            huge_data_loss_timer: Timer::new(),
            pkt_stats_buf: Vec::new(),
            pkt_buf: Vec::new(),
        }
    }

    pub fn is_no_data_to_send(&self) -> bool {
        self.is_send_buf_empty() && self.pkt_send_space.num_txing_pkts() == 0
    }

    pub fn is_send_buf_empty(&self) -> bool {
        self.send_data_buf.is_empty()
            && matches!(
                self.send_fin_buf,
                SendFinBuf::Empty | SendFinBuf::EmptyAndBlocked
            )
    }

    pub fn pkt_send_space(&self) -> &PktSendSpace {
        &self.pkt_send_space
    }

    pub fn pkt_recv_space(&self) -> &PktRecvSpace {
        &self.pkt_recv_space
    }

    pub fn token_bucket(&self) -> &TokenBucket {
        &self.token_bucket
    }

    pub fn send_fin_buf(&mut self) {
        if matches!(self.send_fin_buf, SendFinBuf::EmptyAndBlocked) {
            return;
        }
        self.send_fin_buf = SendFinBuf::Some;
    }

    /// Store data in the inner data buffer
    pub fn send_data_buf(&mut self, buf: &[u8], now: Instant) -> usize {
        self.detect_application_limited_phases(now);

        let free_bytes = self.send_data_buf.capacity() - self.send_data_buf.len();
        let write_bytes = free_bytes.min(buf.len());
        self.send_data_buf.batch_enqueue(&buf[..write_bytes]);
        write_bytes
    }

    /// Move data from inner data buffer to inner packet space and return one of the packets if possible
    pub fn send_data_pkt(&mut self, pkt: &mut [u8], now: Instant) -> Option<DataPkt> {
        self.detect_application_limited_phases(now);

        // backoff on unrecovered huge data loss
        let mut f = || {
            let huge_data_loss = self
                .pkt_send_space
                .huge_data_loss(UnitR::new(MAX_DATA_LOSS_RATE).unwrap(), now);
            if !huge_data_loss {
                self.huge_data_loss_timer.clear();
                return;
            }
            let at_least_for = self.pkt_send_space.rto_duration().mul_f64(2.);
            let (set_off, _) = self
                .huge_data_loss_timer
                .ensure_started_and_check(at_least_for, now);
            if !set_off {
                return;
            }
            self.huge_data_loss_timer.clear();
            // exponential backoff
            let send_rate = PosR::new(self.send_rate.get() / 2.).unwrap();
            self.set_send_rate(send_rate, now);
        };
        f();

        if !self.token_bucket.take_exact_tokens(1, now) {
            return None;
        }

        if let Some(p) = self.pkt_send_space.rtx(now) {
            pkt[..p.data.len()].copy_from_slice(p.data);

            let data_written = NonZeroUsize::new(p.data.len())
                .map(DataPktPayload::Data)
                .unwrap_or(DataPktPayload::Fin);
            return Some(DataPkt {
                seq: p.seq,
                data_written,
            });
        }

        // insufficient cwnd
        if !self.pkt_send_space.accepts_new_pkt() {
            return None;
        }

        let pkt_bytes = pkt
            .len()
            .min(self.max_data_size_per_pkt())
            .min(self.send_data_buf.len());
        let pkt_bytes = match (NonZeroUsize::new(pkt_bytes), &self.send_fin_buf) {
            (Some(x), _) => x.get(),
            (None, SendFinBuf::Some) => {
                self.send_fin_buf = SendFinBuf::EmptyAndBlocked;
                0
            }
            (None, _) => return None,
        };

        let stats = self
            .connection_stats
            .send_packet_2(now, self.pkt_send_space.no_pkts_in_flight());

        let mut buf = self.pkt_send_space.reused_buf().take();
        self.send_data_buf.batch_dequeue_extend(pkt_bytes, &mut buf);
        let data = buf;

        pkt[..data.len()].copy_from_slice(&data);
        let p = self.pkt_send_space.send(data, stats, now);

        let data_written = NonZeroUsize::new(pkt_bytes)
            .map(DataPktPayload::Data)
            .unwrap_or(DataPktPayload::Fin);
        Some(DataPkt {
            seq: p.seq,
            data_written,
        })
    }

    /// Take ACKs from the unreliable layer
    pub fn recv_ack_pkt(
        &mut self,
        recved: AckBallSequence<'_>,
        now: Instant,
    ) -> Option<dre::RateSample> {
        self.detect_application_limited_phases(now);

        self.pkt_send_space
            .ack(recved, &mut self.pkt_stats_buf, now);

        self.update_rate_sample_on_ack(now)
    }
    fn update_rate_sample_on_ack(&mut self, now: Instant) -> Option<dre::RateSample> {
        while let Some(p) = self.pkt_stats_buf.pop() {
            self.pkt_buf.push(dre::Packet {
                state: p,
                data_length: 1,
            })
        }
        let min_rtt = self.pkt_send_space.min_rtt()?;
        let sr = self
            .connection_stats
            .sample_rate(&self.pkt_buf, now, min_rtt);
        self.pkt_stats_buf.clear();
        self.pkt_buf.clear();

        let sr = sr?;
        if PRINT_DEBUG_MSGS {
            println!("{sr:?}");
        }
        self.prev_sample_rate = Some(sr.clone());

        self.adjust_send_rate_exponential(&sr, now);

        Some(sr)
    }

    fn adjust_send_rate_exponential(&mut self, sr: &dre::RateSample, now: Instant) {
        let little_data_loss = self
            .pkt_send_space
            .data_loss_rate(now)
            .map(|lr| lr < CC_DATA_LOSS_RATE);
        let should_probe = little_data_loss != Some(false);
        let target_send_rate = match should_probe {
            true => {
                let send_rate = sr.delivery_rate() + sr.delivery_rate() * SEND_RATE_PROBE_RATE;
                if send_rate < self.send_rate.get() {
                    return;
                }
                send_rate
            }
            false => sr.delivery_rate(),
        };

        let smooth_send_rate = self.send_rate.get() * (1. - SMOOTH_SEND_RATE_ALPHA)
            + target_send_rate * SMOOTH_SEND_RATE_ALPHA;
        let send_rate = PosR::new(smooth_send_rate).unwrap();
        self.set_send_rate(send_rate, now);
        if let Some(mut rate) = GLOBAL_INIT_SEND_RATE.try_lock() {
            *rate = send_rate;
        }
    }

    /// Return `true` iff received FIN
    pub fn recv_fin_buf(&self) -> bool {
        self.recv_fin_buf
    }

    /// Return data from the inner data buffer and inner packet space
    ///
    /// Return `0` does not mean it is FIN/EOF; you have to ask [`Self::recv_fin_buf()`].
    pub fn recv_data_buf(&mut self, buf: &mut [u8]) -> usize {
        let read_bytes = buf.len().min(self.recv_data_buf.len());
        let Some((a, b)) = self.recv_data_buf.batch_dequeue(read_bytes) else {
            return 0;
        };
        buf[..a.len()].copy_from_slice(a);
        if let Some(b) = b {
            buf[a.len()..read_bytes].copy_from_slice(b);
        }
        self.move_recv_data();
        read_bytes
    }

    /// Take a pkt from the unreliable layer
    ///
    /// Return `false` if the data is rejected due to window capacity
    pub fn recv_data_pkt(&mut self, seq: u64, pkt: &[u8]) -> bool {
        let mut buf = self.pkt_recv_space.reused_buf().take();
        buf.extend(pkt);
        if !self.pkt_recv_space.recv(seq, buf) {
            return false;
        }
        self.move_recv_data();
        true
    }

    /// Move data from pkt space to data buffer
    fn move_recv_data(&mut self) {
        if self.recv_fin_buf {
            return;
        }
        while let Some(p) = self.pkt_recv_space.peek() {
            if self.recv_data_buf.capacity() - self.recv_data_buf.len() < p.len() {
                return;
            }
            let p = self.pkt_recv_space.pop().unwrap();
            if p.is_empty() {
                self.recv_fin_buf = true;
                self.pkt_recv_space.reused_buf().put(p);
                return;
            }
            self.recv_data_buf.batch_enqueue(&p);
            self.pkt_recv_space.reused_buf().put(p);
        }
    }

    fn detect_application_limited_phases(&mut self, now: Instant) {
        let cwnd_stats = self.pkt_send_space.cwnd_stats(now);
        self.connection_stats.detect_application_limited_phases_2(
            dre::DetectAppLimitedPhaseParams {
                few_data_to_send: self.send_data_buf.len() < self.max_data_size_per_pkt(),
                not_transmitting_a_packet: true,
                cwnd_not_full: self.pkt_send_space.accepts_new_pkt(),
                all_lost_packets_retransmitted: cwnd_stats.all_lost_pkts_rtxed,
                pipe: cwnd_stats.num_not_lost_txing_pkts as u64,
            },
        );
    }

    fn set_send_rate(&mut self, send_rate: PosR<f64>, now: Instant) {
        self.pkt_send_space.set_send_rate(send_rate);
        let send_rate = PosR::new(MIN_SEND_RATE).unwrap().max(send_rate);
        self.send_rate = send_rate;
        self.token_bucket.set_thruput(send_rate, now);
    }

    fn max_data_size_per_pkt(&self) -> usize {
        self.mss.get().checked_sub(data_overhead()).unwrap()
    }

    pub fn log(&self) -> Log {
        let now = Instant::now();
        let min_rtt = self.pkt_send_space.min_rtt();
        Log {
            tokens: self.token_bucket.outdated_tokens(),
            send_rate: self.send_rate.get(),
            loss_rate: self.pkt_send_space.data_loss_rate(now),
            num_tx_pkts: self.pkt_send_space.num_txing_pkts(),
            num_pkts_in_pipe: self.pkt_send_space.num_pkts_in_pipe(),
            num_rt_pkts: self.pkt_send_space.num_rtxed_pkts(),
            send_seq: self.pkt_send_space.next_seq(),
            min_rtt: min_rtt.map(|t| t.as_millis()),
            rtt: self.pkt_send_space.smooth_rtt().as_millis(),
            cwnd: self.pkt_send_space.cwnd().get(),
            num_rx_pkts: self.pkt_recv_space.num_recved_pkts(),
            recv_seq: self.pkt_recv_space.next_seq(),
            delivery_rate: self.prev_sample_rate.as_ref().map(|sr| sr.delivery_rate()),
            app_limited: self.prev_sample_rate.as_ref().map(|sr| sr.is_app_limited()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataPkt {
    pub seq: u64,
    pub data_written: DataPktPayload,
}
#[derive(Debug, Clone)]
pub enum DataPktPayload {
    Data(NonZeroUsize),
    Fin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    pub tokens: f64,
    pub send_rate: f64,
    pub delivery_rate: Option<f64>,
    pub loss_rate: Option<f64>,
    pub num_tx_pkts: usize,
    pub num_pkts_in_pipe: usize,
    pub num_rt_pkts: usize,
    pub send_seq: u64,
    pub min_rtt: Option<u128>,
    pub rtt: u128,
    pub cwnd: usize,
    pub num_rx_pkts: usize,
    pub recv_seq: Option<u64>,
    pub app_limited: Option<bool>,
}
