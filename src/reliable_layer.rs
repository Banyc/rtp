use core::num::NonZeroUsize;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use dre::{ConnectionState, PacketState};
pub(crate) use primitive::io::token_bucket::TokenBucket as SharedTokenBucket;
use primitive::{
    io::token_bucket::TokenBucket,
    ops::{
        clear::Clear,
        float::{PosR, UnitR},
        len::{Capacity, Len, LenExt},
    },
    queue::cap_queue::CapVecQueue,
    time::timer::Timer,
};
use serde::{Deserialize, Serialize};

use crate::{
    codec::data_overhead,
    pkt_recv_space::PktRecvSpace,
    pkt_send_space::{CWND_SEND_RATE_SCALE, PktSendSpace},
    sack::AckBallSequence,
};

const SEND_DATA_BUF_LEN: usize = 8 * 1024;
const MAX_SEND_DATA_BUF_LEN: usize = 64 * 1024;
const RECV_DATA_BUF_LEN: usize = 2 << 16;
const MAX_BURST_PKTS: usize = 64;
const MAX_BURST_PKTS_CEIL: usize = 512;
const SEND_TIMER_INTERVAL_SECS: f64 = 0.001;
const SMOOTH_SEND_RATE_ALPHA: f64 = 0.4;
const MIN_SEND_RATE: f64 = 1.;
pub(crate) const INIT_SEND_RATE: f64 = 128.;
const SEND_RATE_PROBE_RATE: f64 = 1.;
pub(crate) const CC_DATA_LOSS_RATE: f64 = 0.2;
const MAX_DATA_LOSS_RATE: f64 = 0.9;
const PRINT_DEBUG_MSGS: bool = false;
const LINEAR_BACKOFF: bool = true;

const QUEUE_RTT_FACTOR: f64 = 2.0;
const QUEUE_RTT_FLOOR: Duration = Duration::from_millis(25);
const RTT_MIN_BUCKET: Duration = Duration::from_secs(5);
const DRAIN_RATE_FRACTION: f64 = 0.85;

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
    send_rate_limiter: Arc<Mutex<TokenBucket>>,
    connection_stats: ConnectionState,
    pkt_send_space: PktSendSpace,
    pkt_recv_space: PktRecvSpace,
    send_rate: PosR<f64>,
    bucket_burst: NonZeroUsize,
    prev_sample_rate: Option<dre::RateSample>,
    huge_data_loss_timer: Timer,
    rtt_floor: WindowedRttMin,
    slow_start: bool,
    slow_start_acked_pkts: usize,

    // Reused buffers
    pkt_stats_buf: Vec<PacketState>,
    pkt_buf: Vec<dre::Packet>,
}
impl ReliableLayer {
    pub fn new(mss: NonZeroUsize, now: Instant) -> (Self, Arc<Mutex<TokenBucket>>) {
        let send_rate = PosR::new(INIT_SEND_RATE).unwrap();
        let bucket_burst = burst_pkts(send_rate);
        let send_rate_limiter = Arc::new(Mutex::new(token_bucket_with_tokens(
            send_rate,
            bucket_burst,
            bucket_burst.get(),
            now,
        )));
        let this = Self {
            mss,
            send_data_buf: CapVecQueue::new_vec(send_data_buf_len(mss)),
            send_fin_buf: SendFinBuf::Empty,
            recv_data_buf: CapVecQueue::new_vec(RECV_DATA_BUF_LEN),
            recv_fin_buf: false,
            send_rate_limiter: send_rate_limiter.clone(),
            connection_stats: ConnectionState::new(now),
            pkt_send_space: PktSendSpace::new(),
            pkt_recv_space: PktRecvSpace::new(),
            send_rate,
            bucket_burst,
            prev_sample_rate: None,
            huge_data_loss_timer: Timer::new(),
            rtt_floor: WindowedRttMin::new(now),
            slow_start: true,
            slow_start_acked_pkts: 0,
            pkt_stats_buf: Vec::new(),
            pkt_buf: Vec::new(),
        };
        (this, send_rate_limiter)
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

    pub fn can_send_tail_fec(&self, now: Instant) -> bool {
        self.is_send_buf_empty()
            && self.pkt_send_space.accepts_new_pkt()
            && !self.pkt_send_space.has_rtx(now)
    }

    pub fn pkt_send_space(&self) -> &PktSendSpace {
        &self.pkt_send_space
    }

    pub fn pkt_recv_space(&self) -> &PktRecvSpace {
        &self.pkt_recv_space
    }

    pub fn sample_rtt(&mut self, rtt: Duration, now: Instant) {
        self.pkt_send_space.sample_rtt(rtt, now);
    }

    pub fn send_fin_buf(&mut self) {
        if matches!(self.send_fin_buf, SendFinBuf::EmptyAndBlocked) {
            return;
        }
        self.send_fin_buf = SendFinBuf::Some;
    }

    /// Store data in the inner data buffer
    pub fn send_data_buf_capacity(&self) -> usize {
        self.send_data_buf.capacity()
    }

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

        if LINEAR_BACKOFF {
            self.backoff_on_huge_data_loss_linear(now);
        } else {
            self.backoff_on_huge_data_loss_exponential(now);
        }

        // During an outage-recovery epoch the whole pre-outage window is
        // immediately eligible for retransmission.  We must pace these
        // retransmits through the token bucket so a just-restored link is not
        // flooded, but they still bypass the new-packet cwnd gate because they
        // are already in flight.
        if self.pkt_send_space.in_outage_recovery()
            && self.pkt_send_space.has_rtx(now)
            && !self
                .send_rate_limiter
                .lock()
                .unwrap()
                .take_exact_tokens(1, now)
        {
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

        // Tail-loss probes also bypass the cwnd gate and token bucket: like
        // regular retransmits, they resend an already-in-flight packet and
        // must fire during tail silence to avoid waiting the full RTO.
        if self.is_send_buf_empty()
            && let Some(p) = self.pkt_send_space.tail_probe(now)
        {
            pkt[..p.data.len()].copy_from_slice(p.data);

            let data_written = NonZeroUsize::new(p.data.len())
                .map(DataPktPayload::Data)
                .unwrap_or(DataPktPayload::Fin);
            return Some(DataPkt {
                seq: p.seq,
                data_written,
            });
        }

        // No retransmit or tail probe to send. From here on we are sending a
        // *new* packet, so the cwnd gate and the send-rate token bucket both
        // apply. Charge a token only when there is actually a new packet (or
        // a FIN) to send; an idle/cwnd-full call must not drain tokens,
        // otherwise we steal bandwidth from a future send and skew the rate
        // limiter.
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

        // There is a new packet (or FIN) to send: take a token now.
        if !self
            .send_rate_limiter
            .lock()
            .unwrap()
            .take_exact_tokens(1, now)
        {
            // We have data/FIN to send but the rate limiter says not yet.
            // Restore the FIN buffer state so the FIN is retried later
            // instead of being permanently consumed.
            if pkt_bytes == 0 {
                self.send_fin_buf = SendFinBuf::Some;
            }
            return None;
        }

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

        // An ACK means the link has delivered something.  Try to open an outage-
        // recovery epoch first; if one starts, reset the congestion state and send
        // rate to the initial values so the post-outage path restarts cleanly.
        let entered_recovery = self.pkt_send_space.detect_outage_recovery(now);

        self.pkt_send_space
            .ack(recved, &mut self.pkt_stats_buf, now);

        if entered_recovery {
            self.rtt_floor = WindowedRttMin::new(now);
            self.slow_start = false;
            self.slow_start_acked_pkts = 0;
            self.set_send_rate(PosR::new(INIT_SEND_RATE).unwrap(), now);
        }

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

        if self.slow_start {
            self.slow_start_acked_pkts += self.pkt_buf.len();
            let ss_rate = self.slow_start_acked_pkts as f64 / self.control_rtt().as_secs_f64();
            let ss_rate = PosR::new(ss_rate.max(self.send_rate.get())).unwrap();
            self.set_send_rate(ss_rate, now);
            // Per-episode accumulator: once the pipe drains, reset for the next
            // burst so slow-start cannot grow without bound on sparse flows.
            if self.pkt_send_space.no_pkts_in_flight() {
                self.slow_start_acked_pkts = 0;
            }
        }

        self.adjust_send_rate_exponential(&sr, now);

        Some(sr)
    }

    fn adjust_send_rate_exponential(&mut self, sr: &dre::RateSample, now: Instant) {
        // While an outage-recovery epoch is open, ignore rate samples whose prior
        // time predates the outage cut.  A blackout-spanning sample can report a
        // bogus delivery rate (~acked/outage-length) that would collapse the just-
        // restarted INIT_SEND_RATE back toward zero in the same ACK handler.
        if self.pkt_send_space.in_outage_recovery()
            && self
                .pkt_send_space
                .outage_cut()
                .is_some_and(|cut| sr.prior_time() < cut)
        {
            return;
        }

        let smooth = self.pkt_send_space.smooth_rtt();
        let floor = self.rtt_floor.update(now, smooth);
        let tol = self
            .pkt_send_space
            .smooth_rtt_var()
            .mul_f64(QUEUE_RTT_FACTOR)
            .max(floor.mul_f64(QUEUE_RTT_FACTOR - 1.0))
            .max(QUEUE_RTT_FLOOR);
        let queue_building = smooth > floor + tol;

        let little_data_loss = self
            .pkt_send_space
            .loss_event_rate(now)
            .map(|lr| lr < CC_DATA_LOSS_RATE);
        let should_probe = little_data_loss != Some(false) && !queue_building;
        if should_probe {
            if self.slow_start {
                let probed = sr.delivery_rate() * (1. + SEND_RATE_PROBE_RATE);
                let caught_up = self.send_rate.get() >= probed;
                let little_data_loss_false = little_data_loss == Some(false);
                let app_limited_exit = sr.is_app_limited();
                if little_data_loss_false || queue_building || caught_up || app_limited_exit {
                    self.slow_start = false;
                }
            }
            let probed = probe_send_rate_exponential(self.send_rate.get(), sr.delivery_rate());
            let target_send_rate = probed.unwrap_or(self.send_rate.get());
            self.set_smooth_send_rate(target_send_rate, now);
            return;
        }

        if queue_building && little_data_loss != Some(false) {
            self.slow_start = false;
            let control_rtt = self.control_rtt();
            let current = self.send_rate.get();
            let target = (sr.delivery_rate() * DRAIN_RATE_FRACTION)
                .min(current)
                .max(MIN_SEND_RATE);
            let new_rate = backoff_send_rate_linear(current, target, sr.interval(), control_rtt);
            match new_rate {
                Some(new_rate) => self.set_send_rate(PosR::new(new_rate).unwrap(), now),
                None => {
                    let send_rate = PosR::new(self.send_rate.get()).unwrap();
                    self.set_send_rate(send_rate, now);
                }
            }
            return;
        }

        if LINEAR_BACKOFF {
            self.backoff_on_high_loss_ack_linear(sr, now);
        } else {
            self.slow_start = false;
            let target_send_rate = sr.delivery_rate();
            self.set_smooth_send_rate(target_send_rate, now);
        }
    }

    /// Linear backoff toward the delivery rate on a high-loss ACK sample.
    fn backoff_on_high_loss_ack_linear(&mut self, sr: &dre::RateSample, now: Instant) {
        let control_rtt = self.control_rtt();
        let current = self.send_rate.get();
        let target = sr.delivery_rate().min(current).max(MIN_SEND_RATE);
        let new_rate = backoff_send_rate_linear(current, target, sr.interval(), control_rtt);
        let Some(new_rate) = new_rate else {
            return;
        };
        let send_rate = PosR::new(new_rate).unwrap();
        self.set_send_rate(send_rate, now);
    }

    fn set_smooth_send_rate(&mut self, target_send_rate: f64, now: Instant) {
        let smooth_send_rate = self.send_rate.get() * (1. - SMOOTH_SEND_RATE_ALPHA)
            + target_send_rate * SMOOTH_SEND_RATE_ALPHA;
        let send_rate = PosR::new(smooth_send_rate).unwrap();
        self.set_send_rate(send_rate, now);
    }

    /// Linear backoff on unrecovered huge data loss.
    fn backoff_on_huge_data_loss_linear(&mut self, now: Instant) {
        let Some(elapsed) = self.huge_data_loss_gate(now) else {
            return;
        };
        let control_rtt = self.control_rtt();
        let current = self.send_rate.get();
        let new_rate = backoff_send_rate_linear(current, MIN_SEND_RATE, elapsed, control_rtt);
        let Some(new_rate) = new_rate else {
            return;
        };
        self.set_send_rate(PosR::new(new_rate).unwrap(), now);
    }

    /// Original exponential backoff on unrecovered huge data loss.
    fn backoff_on_huge_data_loss_exponential(&mut self, now: Instant) {
        let Some(_) = self.huge_data_loss_gate(now) else {
            return;
        };
        let send_rate = PosR::new(self.send_rate.get() / 2.).unwrap();
        self.set_send_rate(send_rate, now);
    }

    /// Shared gate for huge-data-loss backoff. Returns the elapsed time the
    /// loss has persisted once the `2 * RTO` threshold is reached.
    fn huge_data_loss_gate(&mut self, now: Instant) -> Option<Duration> {
        let huge_data_loss = self
            .pkt_send_space
            .huge_data_loss(UnitR::new(MAX_DATA_LOSS_RATE).unwrap(), now);
        if !huge_data_loss {
            self.huge_data_loss_timer.clear();
            return None;
        }
        let at_least_for = self.pkt_send_space.rto_duration().mul_f64(2.);
        let (set_off, elapsed) = self
            .huge_data_loss_timer
            .ensure_started_and_check(at_least_for, now);
        if !set_off {
            return None;
        }
        self.huge_data_loss_timer.clear();
        Some(elapsed)
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

        let mut limiter = self.send_rate_limiter.lock().unwrap();
        limiter.set_thruput(send_rate, now);
        let tokens = limiter.outdated_coined_tokens();
        let bucket_burst = burst_pkts(send_rate);
        *limiter =
            token_bucket_with_tokens(send_rate, bucket_burst, tokens.min(bucket_burst.get()), now);
        self.bucket_burst = bucket_burst;
    }
}

/// Build a `TokenBucket` that starts with `tokens` already credited while
/// keeping `last_update` anchored at `now` so the send-timer deadline is not
/// stale.
///
/// The primitive `TokenBucket` constructor starts empty, so a fresh connection
/// stalls waiting for the first tokens to accrue. We simulate a prefill by
/// constructing the bucket at an earlier instant (backdated by the time it
/// would take to earn the requested tokens) and then immediately calling
/// `gen_tokens(now)` to credit that interval. This leaves `last_update = now`,
/// satisfying the invariant that `next_token_time() >= now`.
fn token_bucket_with_tokens(
    thruput: PosR<f64>,
    max_tokens: NonZeroUsize,
    tokens: usize,
    now: Instant,
) -> TokenBucket {
    let max_tokens = max_tokens.get();
    let tokens = tokens.min(max_tokens);
    let backdate = Duration::from_secs_f64((tokens as f64 + 0.5) / thruput.get());

    let mut backdate = backdate;
    let start = loop {
        match now.checked_sub(backdate) {
            Some(start) => break start,
            None => {
                backdate /= 2;
                if backdate.is_zero() {
                    break now;
                }
            }
        }
    };

    let mut bucket = TokenBucket::new(
        thruput,
        NonZeroUsize::new(max_tokens).unwrap_or_else(|| NonZeroUsize::new(1).unwrap()),
        start,
    );
    bucket.gen_tokens(now);
    bucket
}

/// Send staging buffer size for a given MSS.
///
/// For the default MSS we keep the historical 8 KiB staging buffer. For larger
/// MSS values we scale the buffer to whole packets so the send-data path never
/// refills with a sub-MSS remainder that would sit in the buffer indefinitely.
fn send_data_buf_len(mss: NonZeroUsize) -> usize {
    if mss.get() <= crate::udp::NO_FEC_MSS {
        return SEND_DATA_BUF_LEN;
    }
    let payload = mss.get() - data_overhead();
    let pkts = MAX_SEND_DATA_BUF_LEN / payload;
    pkts * payload
}

/// Token-bucket burst size for a given send rate.
///
/// The burst is scaled with the send rate so high-rate flows can emit a larger
/// paced window, while low-rate flows keep the historical 64-packet floor. The
/// value is clamped to [`MAX_BURST_PKTS_CEIL`] to avoid runaway kernel buffers.
fn burst_pkts(send_rate: PosR<f64>) -> NonZeroUsize {
    let burst = (send_rate.get() * 2. * SEND_TIMER_INTERVAL_SECS).floor() as usize;
    let burst = burst.clamp(MAX_BURST_PKTS, MAX_BURST_PKTS_CEIL);
    NonZeroUsize::new(burst).unwrap()
}

impl ReliableLayer {
    fn control_rtt(&self) -> Duration {
        self.pkt_send_space
            .smooth_rtt()
            .max(Duration::from_millis(5))
    }

    fn max_data_size_per_pkt(&self) -> usize {
        self.mss.get().checked_sub(data_overhead()).unwrap()
    }

    pub fn log(&self) -> Log {
        let now = Instant::now();
        let min_rtt = self.pkt_send_space.min_rtt();
        Log {
            tokens: self.send_rate_limiter.lock().unwrap().outdated_tokens(),
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

/// Minimum of a sliding window of RTT samples.
///
/// RTT rises when a queue builds, but a lifetime `min_rtt` collapses to ~0 on
/// jittery links and never recovers. Instead, keep a short windowed minimum:
/// the floor tracks recent baseline RTT and recovers quickly enough to let the
/// delay-based gate close when the queue inflates and reopen when it drains.
#[derive(Debug, Clone)]
pub(crate) struct WindowedRttMin {
    bucket_start: Instant,
    cur: Option<Duration>,
    prev: Option<Duration>,
}

const RTT_MIN_BUCKET_RTT_SCALE: u32 = 10;

impl WindowedRttMin {
    fn new(now: Instant) -> Self {
        Self {
            bucket_start: now,
            cur: None,
            prev: None,
        }
    }

    fn update(&mut self, now: Instant, rtt: Duration) -> Duration {
        let bucket = RTT_MIN_BUCKET.max(rtt.saturating_mul(RTT_MIN_BUCKET_RTT_SCALE));
        let elapsed = now.duration_since(self.bucket_start);
        if elapsed > bucket * 2 {
            // Idle staleness: both buckets have aged out, mirror LossEventWindow::rotate.
            self.cur = None;
            self.prev = None;
            self.bucket_start = now;
        } else if elapsed > bucket {
            self.prev = self.cur.take();
            self.bucket_start = now;
        }

        self.cur = Some(match self.cur {
            Some(cur) => cur.min(rtt),
            None => rtt,
        });

        let candidates = [self.cur, self.prev].into_iter().flatten();
        candidates.min().unwrap_or(rtt)
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

/// Exponential probe of the send rate on a low-loss ACK sample.
///
/// Returns `None` if the probed rate would not exceed the current rate.
fn probe_send_rate_exponential(current: f64, delivery_rate: f64) -> Option<f64> {
    let probed = delivery_rate + delivery_rate * SEND_RATE_PROBE_RATE;
    if probed < current {
        return None;
    }
    Some(probed)
}

/// Linear backoff of the send rate toward `target`.
///
/// Returns `None` if `current` is already at or below `target`. Otherwise steps
/// down by at most `gap = current - target`, where the natural step is
/// `current * interval / rtt` and a `probe_floor` of
/// `interval / (8 * rtt^2)` guarantees at least one packet's worth of headway
/// over the RTT window (`cwnd = send_rate * rtt * CWND_SEND_RATE_SCALE`).
fn backoff_send_rate_linear(
    current: f64,
    target: f64,
    interval: Duration,
    control_rtt: Duration,
) -> Option<f64> {
    let gap = current - target;
    if gap <= 0. {
        return None;
    }
    let rtt_secs = control_rtt.as_secs_f64();
    let interval_secs = interval.as_secs_f64();
    let probe_floor = interval_secs / (CWND_SEND_RATE_SCALE as f64 * rtt_secs * rtt_secs);
    let step = (current * interval_secs / rtt_secs)
        .max(probe_floor)
        .min(gap);
    Some((current - step).max(target))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use primitive::ops::float::PosR;

    use super::{
        INIT_SEND_RATE, MAX_BURST_PKTS, MAX_BURST_PKTS_CEIL, MAX_SEND_DATA_BUF_LEN, RTT_MIN_BUCKET,
        SEND_DATA_BUF_LEN, WindowedRttMin, burst_pkts, send_data_buf_len, token_bucket_with_tokens,
    };
    use crate::{codec::data_overhead, udp::NO_FEC_MSS};

    #[test]
    fn windowed_rtt_min_slides_and_forgets() {
        let now = std::time::Instant::now();
        let mut w = WindowedRttMin::new(now);

        // Bucket is max(5 s, 10 * rtt).  Use a 40 ms sample so bucket = 5 s.
        assert_eq!(
            w.update(now, Duration::from_millis(40)),
            Duration::from_millis(40)
        );
        assert_eq!(
            w.update(now, Duration::from_millis(100)),
            Duration::from_millis(40)
        );

        // First rotation: the previous bucket's 40 ms floor is still visible.
        let t1 = now + RTT_MIN_BUCKET + Duration::from_millis(1);
        assert_eq!(
            w.update(t1, Duration::from_millis(90)),
            Duration::from_millis(40)
        );
        assert_eq!(
            w.update(t1, Duration::from_millis(95)),
            Duration::from_millis(40)
        );

        // Second rotation: the stale floor has aged out.
        let t2 = t1 + RTT_MIN_BUCKET + Duration::from_millis(1);
        assert_eq!(
            w.update(t2, Duration::from_millis(95)),
            Duration::from_millis(90)
        );
        assert_eq!(
            w.update(t2, Duration::from_millis(110)),
            Duration::from_millis(90)
        );
    }

    #[test]
    fn windowed_rtt_min_bucket_scales_with_long_rtt() {
        let now = std::time::Instant::now();
        let mut w = WindowedRttMin::new(now);

        // With a 1 s sample the bucket should be 10 s, not the fixed 5 s floor.
        assert_eq!(
            w.update(now, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
        let t1 = now + Duration::from_secs(5) + Duration::from_millis(1);
        // Inside the 10 s bucket, the floor is still the first sample.
        assert_eq!(
            w.update(t1, Duration::from_millis(900)),
            Duration::from_millis(900)
        );

        // After >10 s of staleness both buckets clear, mirroring LossEventWindow.
        let t2 = now + Duration::from_secs(11);
        assert_eq!(
            w.update(t2, Duration::from_millis(800)),
            Duration::from_millis(800)
        );
    }

    #[test]
    fn windowed_rtt_min_clears_after_idle_gap() {
        let now = std::time::Instant::now();
        let mut w = WindowedRttMin::new(now);

        assert_eq!(
            w.update(now, Duration::from_millis(40)),
            Duration::from_millis(40)
        );
        // Idle for more than twice the 5 s bucket.
        let t1 = now + RTT_MIN_BUCKET * 2 + Duration::from_millis(1);
        assert_eq!(
            w.update(t1, Duration::from_millis(100)),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn token_bucket_with_tokens_prefills_without_stale_deadline() {
        let now = std::time::Instant::now();
        let thruput = PosR::new(128.0).unwrap();
        let max_tokens = std::num::NonZeroUsize::new(MAX_BURST_PKTS).unwrap();

        let mut bucket = token_bucket_with_tokens(thruput, max_tokens, 8, now);

        // All prefilled tokens are immediately available.
        assert_eq!(bucket.gen_tokens(now), 8);
        // The deadline is anchored at `now`, not in the past.
        assert!(bucket.next_token_time() >= now);
    }

    #[test]
    fn token_bucket_with_tokens_clamps_prefill_to_capacity() {
        let now = std::time::Instant::now();
        let thruput = PosR::new(128.0).unwrap();
        let max_tokens = std::num::NonZeroUsize::new(4).unwrap();

        let mut bucket = token_bucket_with_tokens(thruput, max_tokens, 100, now);

        // The requested prefill is clamped to the bucket capacity.
        assert_eq!(bucket.gen_tokens(now), 4);
        assert!(bucket.next_token_time() >= now);
    }

    #[test]
    fn send_data_buf_len_keeps_default_at_8_kib() {
        let mss = std::num::NonZeroUsize::new(NO_FEC_MSS).unwrap();
        assert_eq!(send_data_buf_len(mss), SEND_DATA_BUF_LEN);
    }

    #[test]
    fn send_data_buf_len_scales_to_whole_packets_above_default() {
        let mss = std::num::NonZeroUsize::new(8192).unwrap();
        let len = send_data_buf_len(mss);
        let payload = mss.get() - data_overhead();
        let expected = (MAX_SEND_DATA_BUF_LEN / payload) * payload;
        assert_eq!(len, expected);
        assert!(len > SEND_DATA_BUF_LEN);
        assert!(len <= MAX_SEND_DATA_BUF_LEN);

        // Spot checks for the larger-overhead wire format.
        let payload_2015 = 2015 - data_overhead();
        assert_eq!(
            send_data_buf_len(nz(2015)),
            (MAX_SEND_DATA_BUF_LEN / payload_2015) * payload_2015
        );
        let payload_9000 = 9000 - data_overhead();
        assert_eq!(
            send_data_buf_len(nz(9000)),
            (MAX_SEND_DATA_BUF_LEN / payload_9000) * payload_9000
        );

        // Sanity check for the default-MSS path.
        let default_mss = std::num::NonZeroUsize::new(NO_FEC_MSS).unwrap();
        assert_eq!(send_data_buf_len(default_mss), SEND_DATA_BUF_LEN);
    }

    fn nz(n: usize) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(n).unwrap()
    }

    #[test]
    fn burst_pkts_scales_with_send_rate() {
        let low = PosR::new(INIT_SEND_RATE).unwrap();
        assert_eq!(burst_pkts(low).get(), MAX_BURST_PKTS);

        let mid = PosR::new(100_000.0).unwrap();
        let mid_burst = burst_pkts(mid).get();
        assert!(mid_burst > MAX_BURST_PKTS);
        assert!(mid_burst < MAX_BURST_PKTS_CEIL);

        let high = PosR::new(1_000_000.0).unwrap();
        assert_eq!(burst_pkts(high).get(), MAX_BURST_PKTS_CEIL);
    }
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
