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
    frame_delivery::{
        FrameDelivery,
        send::{FrameSendStage, MAX_FRAME_LEN},
    },
    recv_queue::pkt_recv_space::PktRecvSpace,
    sack::AckBallSequence,
    send_queue::pkt_send_space::{CWND_SEND_RATE_SCALE, PktSendSpace},
    transmission::watchdog_tuning::WatchdogTuning,
};

const SEND_DATA_BUF_LEN: usize = 8 * 1024;
const MAX_SEND_DATA_BUF_LEN: usize = 64 * 1024;
/// The frame-delivery [`MAX_FRAME_LEN`] is defined in
/// [`crate::frame_delivery::send`] and must stay equal to the stock
/// `MAX_SEND_DATA_BUF_LEN` so a frame can occupy the whole staging buffer.
const _: () = assert!(MAX_FRAME_LEN == MAX_SEND_DATA_BUF_LEN);

/// Cap on newly accepted staging bytes, in units of a pacing window.
///
/// Staged bytes are FIFO-committed via `batch_dequeue_extend`: once accepted
/// they cannot be reordered, the mux's LatencyControl cannot preempt them, and
/// the FIFO's latency cost scales inversely with the pace rate.  Capping
/// acceptance to a small pacing window keeps the latency of a small interactive
/// frame bounded even when the preceding bulk leaves the stage nearly full at a
/// low send rate.  After a rate spike occupancy may sit above the cap until it
/// drains; this is intentional and is why we gate acceptance, not eviction.
const STAGE_WINDOW_SECS: f64 = 0.005;
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
// RTT-proportional queue-tolerance term.  Previously this was one full
// min-RTT (QUEUE_RTT_FACTOR - 1.0 = 1.0).  We now allow only a small fraction
// of the smoothed floor so rate-capped links stop carrying a permanent
// standing queue.  Jitter protection remains the separate 2*rtvar term
// controlled by QUEUE_RTT_FACTOR, not this fraction.
pub(crate) const QUEUE_TOL_RTT_FRACTION: f64 = 0.25;
pub(crate) const QUEUE_RTT_FLOOR: Duration = Duration::from_millis(5);
pub(crate) const RTT_MIN_BUCKET: Duration = Duration::from_secs(5);
pub(crate) const DRAIN_RATE_FRACTION: f64 = 0.85;

/// Fraction of the recent peak delivery rate used as a drain-floor target.
///
/// The floor keeps a small flow from being drained below a fair share of its own
/// recent peak, while still allowing genuine congestion to back it off via the
/// loss branch.  It is capped at INIT_SEND_RATE so a spike does not permanently
/// pin a low-rate incumbent, and floored at MIN_SEND_RATE.
const DRAIN_FLOOR_PEAK_FRACTION: f64 = 0.25;

/// Grace period, in control RTTs, before a continuously binding drain floor
/// starts to decay.  This prevents transient capacity dips from collapsing the
/// floor, while allowing a genuine long-term capacity drop to drain past a
/// stale windowed peak.
const DRAIN_FLOOR_GRACE_RTTS: f64 = 3.0;

/// Window over which the delivery-rate peak is tracked.
///
/// Twice RTT_MIN_BUCKET so the peak reflects the recent steady state but ages
/// out after idle gaps, mirroring WindowedRttMin.
const DELIVERY_PEAK_BUCKET: Duration = Duration::from_secs(10);

// Gentle-mode parameters are defined in super::gentle and re-exported here
// so the test imports via `super::` continue to work.
pub(crate) use super::gentle::*;

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
    delivery_peak: WindowedDeliveryMax,
    slow_start: bool,
    slow_start_acked_pkts: usize,

    // Gentle-mode congestion controller state.
    pub(crate) gentle: GentleMode,

    /// Whether the bottleneck queue is currently building per delivery-rate
    /// estimation (`smooth > floor + gate_tol`).  Updated on every ACK rate
    /// sample in `adjust_send_rate_exponential`.  Read by the transmission
    /// layer to suppress retransmission-armor duplicate copies
    /// (`RTP_RTX_DUP`) — duplication under a building queue would worsen
    /// the very congestion the dup is meant to recover from.
    queue_building: bool,

    /// How long the drain floor has been continuously binding while the queue
    /// builds.  Used to decay a stale peak delivery-rate floor after a genuine
    /// capacity drop.
    drain_floor_binding_since: Option<Instant>,

    /// Per-connection frame-delivery mode snapshot taken at construction.
    /// When `enabled`, application data is staged as whole frames and
    /// packetized frame-aligned; the receive path may deliver complete frames
    /// out of order past sequence holes.  When disabled, the layer is
    /// byte-for-byte stock (no `FRAME_DATA_TS` is ever emitted, the
    /// byte-stream `recv`/`send` paths are unchanged).
    frame_delivery: FrameDelivery,
    /// Sender-side frame staging and frame-aligned packetization in
    /// frame-delivery mode (see [`crate::frame_delivery::send`]).  Empty when
    /// frame-delivery mode is off (the stock byte-stream path uses
    /// `send_data_buf` directly).
    frame_send_stage: FrameSendStage,

    // Reused buffers
    pkt_stats_buf: Vec<PacketState>,
    pkt_buf: Vec<dre::Packet>,
}

impl ReliableLayer {
    pub fn new(
        mss: NonZeroUsize,
        frame_delivery: FrameDelivery,
        now: Instant,
    ) -> (Self, Arc<Mutex<TokenBucket>>) {
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
            pkt_recv_space: PktRecvSpace::new(frame_delivery),
            send_rate,
            bucket_burst,
            prev_sample_rate: None,
            huge_data_loss_timer: Timer::new(),
            rtt_floor: WindowedRttMin::new(now),
            delivery_peak: WindowedDeliveryMax::new(now),
            slow_start: true,
            slow_start_acked_pkts: 0,
            gentle: GentleMode::new(),
            queue_building: false,
            drain_floor_binding_since: None,
            frame_delivery,
            frame_send_stage: FrameSendStage::new(),
            pkt_stats_buf: Vec::new(),
            pkt_buf: Vec::new(),
        };
        (this, send_rate_limiter)
    }

    pub fn new_with_watchdog_tuning(
        mss: NonZeroUsize,
        frame_delivery: FrameDelivery,
        now: Instant,
        tuning: WatchdogTuning,
    ) -> (Self, Arc<Mutex<TokenBucket>>) {
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
            pkt_send_space: PktSendSpace::new().with_watchdog_tuning(tuning),
            pkt_recv_space: PktRecvSpace::new(frame_delivery),
            send_rate,
            bucket_burst,
            prev_sample_rate: None,
            huge_data_loss_timer: Timer::new(),
            rtt_floor: WindowedRttMin::new(now),
            delivery_peak: WindowedDeliveryMax::new(now),
            slow_start: true,
            slow_start_acked_pkts: 0,
            gentle: GentleMode::new(),
            queue_building: false,
            drain_floor_binding_since: None,
            frame_delivery,
            frame_send_stage: FrameSendStage::new(),
            pkt_stats_buf: Vec::new(),
            pkt_buf: Vec::new(),
        };
        (this, send_rate_limiter)
    }

    /// Whether frame-delivery mode is enabled on this connection.
    pub fn frame_delivery_enabled(&self) -> bool {
        self.frame_delivery.enabled
    }

    pub fn is_no_data_to_send(&self) -> bool {
        self.is_send_buf_empty() && self.pkt_send_space.num_txing_pkts() == 0
    }

    pub fn is_send_buf_empty(&self) -> bool {
        let stock_empty = self.send_data_buf.is_empty()
            && matches!(
                self.send_fin_buf,
                SendFinBuf::Empty | SendFinBuf::EmptyAndBlocked
            );
        if self.frame_delivery.enabled {
            // In frame mode the staging buffer is unused; the frame send
            // stage is the source of truth for unsent application bytes.
            stock_empty && self.frame_send_stage.is_empty()
        } else {
            stock_empty
        }
    }

    pub fn can_send_tail_fec(&self, now: Instant) -> bool {
        self.is_send_buf_empty()
            && self.pkt_send_space.accepts_new_pkt()
            && !self.pkt_send_space.has_rtx(now)
            // A due tail-loss probe is a pending (re)transmission, so the tail
            // is not settled and the tail parity must wait.
            && !self.pkt_send_space.has_tail_probe(now)
    }

    pub fn pkt_send_space(&self) -> &PktSendSpace {
        &self.pkt_send_space
    }

    pub fn pkt_recv_space(&self) -> &PktRecvSpace {
        &self.pkt_recv_space
    }

    /// Whether the delivery-rate congestion controller currently considers the
    /// bottleneck queue to be building (smooth RTT above the floor plus the
    /// gate tolerance).  Used by the transmission layer to suppress
    /// retransmission-armor duplicate copies under congestion.
    pub fn queue_building(&self) -> bool {
        self.queue_building
    }

    /// Test-only: force the `queue_building` flag so the retransmission-armor
    /// duplicate-copy suppression gate can be exercised deterministically
    /// without having to drive a full delivery-rate sample sequence.
    #[cfg(test)]
    pub(crate) fn set_queue_building_for_test(&mut self, v: bool) {
        self.queue_building = v;
    }

    /// Test-only: directly enqueue `buf` into the send buffer, bypassing the
    /// staging-cap gate in `send_data_buf`.  Used by in-stream group FEC tests
    /// to stage many packets worth of data so a single `send_pkts` call emits
    /// a full 8-symbol group.
    #[cfg(test)]
    pub(crate) fn enqueue_send_data_for_test(&mut self, buf: &[u8]) {
        self.send_data_buf.batch_enqueue(buf);
    }

    /// Test-only: shrink the congestion window so the send loop stops after a
    /// fixed number of new packets, leaving the rest of the staged data in the
    /// send buffer.  This makes `is_send_buf_empty` false (and thus the stock
    /// `can_send_tail_fec` gate closed) without draining the token bucket, so
    /// the FEC parity flush still has spare tokens.  Used by in-stream group
    /// FEC tests that need the stock tail gate genuinely closed.
    #[cfg(test)]
    pub(crate) fn set_cwnd_for_test(&mut self, cwnd: NonZeroUsize) {
        self.pkt_send_space.set_cwnd_for_test(cwnd);
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
    #[cfg(test)]
    pub fn send_data_buf_capacity(&self) -> usize {
        self.send_data_buf.capacity()
    }

    /// Maximum application bytes in a single frame in frame-delivery mode.
    /// In stock (non-frame) mode this returns the byte-stream staging
    /// capacity (`send_data_buf_capacity`), preserving the existing
    /// `WriteStream` behavior.  In frame-delivery mode, the AsyncWrite
    /// adapter must cap each write at this value so one write = one frame
    /// (capping at the byte-stream stage cap would split a large frame
    /// into multiple wire frames, breaking the one-write-one-frame
    /// invariant).
    pub fn write_unit_capacity(&self) -> usize {
        if self.frame_delivery.enabled {
            MAX_FRAME_LEN
        } else {
            self.send_data_buf.capacity()
        }
    }

    pub fn send_data_buf(&mut self, buf: &[u8], now: Instant) -> usize {
        self.detect_application_limited_phases(now);

        let stage_pkts = (self.send_rate.get() * STAGE_WINDOW_SECS).ceil() as usize;
        let cap =
            (stage_pkts.max(2) * self.max_data_size_per_pkt()).min(self.send_data_buf.capacity());
        let free_bytes = cap.saturating_sub(self.send_data_buf.len());
        let write_bytes = free_bytes.min(buf.len());
        self.send_data_buf.batch_enqueue(&buf[..write_bytes]);
        write_bytes
    }

    /// Stage a whole application frame in frame-delivery mode.  The frame is
    /// queued in the frame send stage and packetized frame-aligned by
    /// subsequent `send_data_pkt` calls (a packet never carries bytes of two
    /// frames).  Returns `Ok(())` on success, or `Err(InvalidInput)` when the
    /// mode is off, the frame is empty, or the frame exceeds `MAX_FRAME_LEN`.
    pub fn send_frame_buf(&mut self, frame: &[u8], now: Instant) -> Result<(), std::io::ErrorKind> {
        if !self.frame_delivery.enabled {
            return Err(std::io::ErrorKind::InvalidInput);
        }
        crate::frame_delivery::send::validate_frame(frame)?;
        self.detect_application_limited_phases(now);
        // Rate-scale the soft staging cap exactly like the stock path so a
        // small interactive frame is not starved behind a bulk backlog
        // already on the stage.
        let stage_pkts = (self.send_rate.get() * STAGE_WINDOW_SECS).ceil() as usize;
        let cap = (stage_pkts.max(2) * self.max_data_size_per_pkt()).min(MAX_FRAME_LEN);
        self.frame_send_stage.stage_frame(frame, cap)
    }

    /// Total bytes of pending (not yet fully packetized) frames in
    /// frame-delivery mode.  Zero when the mode is off.
    pub fn pending_frame_bytes(&self) -> usize {
        self.frame_send_stage.pending_bytes()
    }

    /// Move data from inner data buffer to inner packet space and return one of the packets if possible
    pub fn send_data_pkt(&mut self, pkt: &mut [u8], now: Instant) -> Option<DataPkt> {
        self.detect_application_limited_phases(now);

        if LINEAR_BACKOFF {
            self.backoff_on_huge_data_loss_linear(now);
        } else {
            self.backoff_on_huge_data_loss_exponential(now);
        }

        // Reconcile any deferred CC loss-events whose stock reorder-window
        // deadline has now elapsed (jitter-tolerant fast-retransmit path).
        // Runs every send tick so deadlines are honoured promptly.
        self.pkt_send_space.poll_deferred_loss(now);

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
                frame_len: p.frame_len,
                was_repair: true,
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
                frame_len: p.frame_len,
                was_repair: true,
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

        if self.frame_delivery.enabled {
            return self.send_data_pkt_frame(pkt, now);
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
        let p = self.pkt_send_space.send(data, stats, None, now);

        let data_written = NonZeroUsize::new(pkt_bytes)
            .map(DataPktPayload::Data)
            .unwrap_or(DataPktPayload::Fin);
        Some(DataPkt {
            seq: p.seq,
            data_written,
            frame_len: None,
            was_repair: false,
        })
    }

    /// Frame-delivery-mode new-packet path.  Staging is via the frame send
    /// stage ([`crate::frame_delivery::send`]); packetization is
    /// frame-aligned (a packet never carries bytes of two frames).  The
    /// first packet of a frame carries `Some(frame_len)`; the continuation
    /// packets carry `None`.  The `pkt` scratch buffer receives the payload
    /// bytes; the returned `DataPkt.frame_len` tells the transmission layer
    /// which codec command to emit.
    fn send_data_pkt_frame(&mut self, pkt: &mut [u8], now: Instant) -> Option<DataPkt> {
        let normal_max_payload = self.max_data_size_per_pkt();
        let first_pkt_max_payload = self
            .mss
            .get()
            .checked_sub(crate::frame_delivery::wire::frame_data_overhead())
            .unwrap();

        let chunk = match self
            .frame_send_stage
            .next_chunk_len(first_pkt_max_payload, normal_max_payload)
        {
            Some(chunk) => Some(chunk),
            None => {
                if !matches!(self.send_fin_buf, SendFinBuf::Some) {
                    return None;
                }
                None
            }
        };

        if !self
            .send_rate_limiter
            .lock()
            .unwrap()
            .take_exact_tokens(1, now)
        {
            return None;
        }

        let Some(chunk) = chunk else {
            self.send_fin_buf = SendFinBuf::EmptyAndBlocked;
            let stats = self
                .connection_stats
                .send_packet_2(now, self.pkt_send_space.no_pkts_in_flight());
            let buf = self.pkt_send_space.reused_buf().take();
            let p = self.pkt_send_space.send(buf, stats, None, now);
            return Some(DataPkt {
                seq: p.seq,
                data_written: DataPktPayload::Fin,
                frame_len: None,
                was_repair: false,
            });
        };

        let stats = self
            .connection_stats
            .send_packet_2(now, self.pkt_send_space.no_pkts_in_flight());

        let mut buf = self.pkt_send_space.reused_buf().take();
        let take_bytes = chunk.take_bytes;
        let frame_len = self.frame_send_stage.pop_chunk(chunk, &mut buf);

        pkt[..buf.len()].copy_from_slice(&buf);
        let p = self.pkt_send_space.send(buf, stats, frame_len, now);

        let data_written = NonZeroUsize::new(take_bytes)
            .map(DataPktPayload::Data)
            .unwrap_or(DataPktPayload::Fin);
        Some(DataPkt {
            seq: p.seq,
            data_written,
            frame_len,
            was_repair: false,
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

        // Reconcile deferred CC loss-events after the ack: any whose seq was
        // just acked has been cancelled, and any whose stock deadline has
        // elapsed should now be recorded (genuine loss).
        self.pkt_send_space.poll_deferred_loss(now);

        if entered_recovery {
            self.rtt_floor = WindowedRttMin::new(now);
            self.delivery_peak = WindowedDeliveryMax::new(now);
            self.slow_start = false;
            self.slow_start_acked_pkts = 0;
            self.gentle.reset();
            self.queue_building = false;
            self.drain_floor_binding_since = None;
            self.set_send_rate(PosR::new(INIT_SEND_RATE).unwrap(), now);
        }

        // ACK-clocked slow start must count *every* ACK, including those that
        // produce no usable rate sample (e.g. zero-RTT first echoes or sparse
        // bursts). The per-burst accumulator lives here, before the rate sample
        // is computed and cleared.
        if self.slow_start {
            self.slow_start_acked_pkts += self.pkt_stats_buf.len();
            let ss_rate = self.slow_start_acked_pkts as f64 / self.control_rtt().as_secs_f64();
            let ss_rate = PosR::new(ss_rate.max(self.send_rate.get())).unwrap();
            self.set_send_rate(ss_rate, now);
        }

        // Per-episode accumulator: once the pipe drains, reset for the next
        // burst so slow-start cannot grow without bound on sparse flows. This
        // runs on every ACK, independent of slow-start state.
        if self.pkt_send_space.no_pkts_in_flight() {
            self.slow_start_acked_pkts = 0;
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

        self.adjust_send_rate_exponential(&sr, now);

        Some(sr)
    }

    fn adjust_send_rate_exponential(&mut self, sr: &dre::RateSample, now: Instant) {
        // While an outage-recovery epoch is open, ignore rate samples whose prior
        // time predates the outage cut.  A blackout-spanning sample can report a
        // bogus delivery rate (~acked/outage-length) that would collapse the just-
        // restarted INIT_SEND_RATE back toward zero in the same ACK handler.
        if self
            .pkt_send_space
            .should_censor_rate_sample(sr.prior_time())
        {
            return;
        }

        let smooth = self.pkt_send_space.smooth_rtt();
        // The floor is fed by a WindowedRttMin over the smoothed RTT, so it can
        // ratchet upward across consecutive WindowedRttMin buckets (self-
        // pollution) when the path keeps delivering slower than prior minima.
        // The raw-min alternative was measured strictly worse, so we keep the
        // smooth-fed floor and use the 2*rtvar jitter term as the safety margin.
        let floor = self.rtt_floor.update(now, smooth);

        // Track the recent peak delivery rate before any branch dispatch so the
        // drain floor is fed even when the current sample is not driving a rate
        // change.
        let peak_delivery = self.delivery_peak.update(now, sr.delivery_rate());
        let tol = self
            .pkt_send_space
            .smooth_rtt_var()
            .mul_f64(QUEUE_RTT_FACTOR)
            .max(floor.mul_f64(QUEUE_TOL_RTT_FRACTION))
            .max(QUEUE_RTT_FLOOR);

        // ----- Gentle-mode entry/exit and gate hysteresis --------------------
        let loss_event_rate = self.pkt_send_space.loss_event_rate(now);
        self.gentle.preamble(
            smooth,
            floor,
            self.pkt_send_space.smooth_rtt_var(),
            loss_event_rate,
            now,
            GentlePreambleConfig {
                enter_coefficient: QUEUE_RTT_FACTOR * GENTLE_ENTER_RTTVAR_FACTOR,
                control_rtt: self.control_rtt(),
            },
        );

        // Gate hysteresis: while actively draining in gentle mode, use tol/2.
        let gate_tol = self.gentle.gate_tol(tol);
        let queue_building = smooth > floor + gate_tol;
        self.queue_building = queue_building;

        let little_data_loss = loss_event_rate.map(|lr| lr < CC_DATA_LOSS_RATE);
        if self.slow_start {
            let probed = sr.delivery_rate() * (1. + SEND_RATE_PROBE_RATE);
            let caught_up = self.send_rate.get() <= probed;
            if little_data_loss == Some(false) || queue_building || caught_up || sr.is_app_limited()
            {
                self.slow_start = false;
            }
        }

        // ----- Gate-open / probe branch ---------------------------------------
        let should_probe = little_data_loss != Some(false) && !queue_building;
        if !should_probe {
            self.gentle.clear_gate_open();
        }

        if should_probe {
            self.drain_floor_binding_since = None;
            if let Some(target) = self.gentle.probe(
                sr.delivery_rate(),
                self.send_rate.get(),
                self.control_rtt(),
                smooth,
                now,
            ) {
                self.set_smooth_send_rate(target, now);
                return;
            }

            let probed = probe_send_rate_exponential(self.send_rate.get(), sr.delivery_rate());
            let target_send_rate = probed.unwrap_or(self.send_rate.get());
            self.set_smooth_send_rate(target_send_rate, now);
            return;
        }

        // ----- Drain branch --------------------------------------------------
        if queue_building && little_data_loss != Some(false) {
            let control_rtt = self.control_rtt();
            let current = self.send_rate.get();
            let drain_frac = self.gentle.drain_frac();
            let base =
                (peak_delivery * DRAIN_FLOOR_PEAK_FRACTION).clamp(MIN_SEND_RATE, INIT_SEND_RATE);

            // Drain-floor decay: if the floor keeps binding while the queue
            // builds, the windowed delivery peak is probably stale (the path
            // capacity has dropped).  Decay the floor exponentially after a grace
            // period so the delay gate can actually drain the standing queue.
            let binding = sr.delivery_rate() * drain_frac < base;
            if binding {
                self.drain_floor_binding_since.get_or_insert(now);
            } else {
                self.drain_floor_binding_since = None;
            }
            let binding_for = self
                .drain_floor_binding_since
                .map(|s| now.saturating_duration_since(s));
            let closed_for = self
                .gentle
                .queue_since()
                .map(|s| now.saturating_duration_since(s));
            let pinned_for = binding_for.zip(closed_for).map(|(b, c)| b.min(c));
            let grace = control_rtt.mul_f64(DRAIN_FLOOR_GRACE_RTTS);
            let drain_floor =
                if loss_event_rate.is_none() || pinned_for.is_none_or(|pinned| pinned <= grace) {
                    base
                } else {
                    let pinned = pinned_for.unwrap();
                    let excess_rtts =
                        (pinned.as_secs_f64() - grace.as_secs_f64()) / control_rtt.as_secs_f64();
                    (base * 0.5f64.powf(excess_rtts)).max(MIN_SEND_RATE)
                };

            let target = (sr.delivery_rate() * drain_frac)
                .max(drain_floor)
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

            self.gentle
                .drain_episode_guard(smooth, floor, control_rtt, now);
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
    pub fn recv_data_pkt(&mut self, seq: u64, frame_len: Option<u32>, pkt: &[u8]) -> bool {
        let mut buf = self.pkt_recv_space.reused_buf().take();
        buf.extend(pkt);
        if !self.pkt_recv_space.recv(seq, buf, frame_len) {
            return false;
        }
        if self.frame_delivery.enabled {
            // In frame mode, complete frames may be delivered out of order;
            // the receive queue handles reassembly.  The byte-stream buffer
            // (`recv_data_buf`) is bypassed entirely in frame mode.
            return true;
        }
        self.move_recv_data();
        true
    }

    /// Pop one complete frame from the receive queue in frame-delivery mode.
    /// Returns `Ok(Some(frame))` when a complete frame is available (possibly
    /// out of order past sequence holes), `Ok(None)` on EOF (FIN received and
    /// no more frames), or `Err(InvalidInput)` when frame-delivery mode is
    /// off.
    pub fn recv_frame_buf(&mut self) -> Result<Option<Vec<u8>>, std::io::ErrorKind> {
        if !self.frame_delivery.enabled {
            return Err(std::io::ErrorKind::InvalidInput);
        }
        // Try to pop a complete frame from the receive queue.
        if let Some(frame) = self.pkt_recv_space.pop_complete_frame() {
            return Ok(Some(frame));
        }
        // FIN handling: in frame mode the FIN is an empty-payload packet
        // with no `frame_len`.  `move_recv_data` (which sets `recv_fin_buf`)
        // is bypassed in frame mode, so we gate EOF on the recv-space cursor
        // directly: a FIN at the in-order head means all earlier data has
        // been delivered, so surface EOF.  A FIN behind an out-of-order gap
        // does NOT surface EOF until the gap fills.
        if self.pkt_recv_space.fin_at_head() {
            self.recv_fin_buf = true;
            return Ok(None);
        }
        // Fallback: if `recv_fin_buf` was set by some other path, honor it.
        if self.recv_fin_buf {
            return Ok(None);
        }
        // No complete frame and no FIN yet: pending.
        Err(std::io::ErrorKind::WouldBlock)
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
        let staged_bytes = if self.frame_delivery.enabled {
            self.pending_frame_bytes()
        } else {
            self.send_data_buf.len()
        };
        self.connection_stats.detect_application_limited_phases_2(
            dre::DetectAppLimitedPhaseParams {
                few_data_to_send: staged_bytes < self.max_data_size_per_pkt(),
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

/// Maximum of a sliding window of delivery-rate samples.
///
/// Mirrors WindowedRttMin but keeps the peak instead of the minimum. The peak
/// is used to compute a per-flow drain floor: a flow is allowed to drain down
/// to a fraction of its own recent peak so a small incumbent is not pinned at
/// the global MIN_SEND_RATE by a competitor's standing queue.
#[derive(Debug, Clone)]
pub(crate) struct WindowedDeliveryMax {
    bucket_start: Instant,
    cur: Option<f64>,
    prev: Option<f64>,
}

impl WindowedDeliveryMax {
    fn new(now: Instant) -> Self {
        Self {
            bucket_start: now,
            cur: None,
            prev: None,
        }
    }

    fn update(&mut self, now: Instant, rate: f64) -> f64 {
        let elapsed = now.duration_since(self.bucket_start);
        if elapsed > DELIVERY_PEAK_BUCKET * 2 {
            // Idle staleness: both buckets have aged out.
            self.cur = None;
            self.prev = None;
            self.bucket_start = now;
        } else if elapsed > DELIVERY_PEAK_BUCKET {
            self.prev = self.cur.take();
            self.bucket_start = now;
        }

        self.cur = Some(match self.cur {
            Some(cur) => cur.max(rate),
            None => rate,
        });

        let candidates = [self.cur, self.prev].into_iter().flatten();
        candidates.fold(rate, f64::max)
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

pub(crate) const RTT_MIN_BUCKET_RTT_SCALE: u32 = 10;

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
    /// Application frame length this packet belongs to.  `Some` only for the
    /// first packet of a frame in frame-delivery mode; the transmission layer
    /// uses this to choose `FRAME_DATA_TS` vs `DATA_TS` on the wire.  `None`
    /// for continuation packets, all stock packets, FIN, and repairs of
    /// continuation packets.  Retransmits/tail-probes inherit the stored
    /// `TxingPkt.frame_len` so the framing is preserved across repairs.
    pub frame_len: Option<u32>,
    /// Whether this packet is a retransmission or a tail-loss probe (a
    /// recovery packet, not new data).  Used by the transmission layer to
    /// decide whether to emit a retransmission-armor duplicate copy
    /// (`RTP_RTX_DUP`).
    pub was_repair: bool,
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
    use core::num::NonZeroU64;
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    use primitive::ops::{
        float::PosR,
        len::{Capacity, Len},
    };

    use super::{
        DRAIN_FLOOR_PEAK_FRACTION, GENTLE_DRAIN_GAP_SHRINK, GENTLE_ENTER_RTTS,
        GENTLE_ENTER_RTTVAR_FACTOR, GENTLE_REENTRY_COOLDOWN, GENTLE_REENTRY_COOLDOWN_RTTS,
        INIT_SEND_RATE, MAX_BURST_PKTS, MAX_BURST_PKTS_CEIL, MAX_SEND_DATA_BUF_LEN,
        QUEUE_RTT_FACTOR, QUEUE_RTT_FLOOR, QUEUE_TOL_RTT_FRACTION, RTT_MIN_BUCKET,
        RTT_MIN_BUCKET_RTT_SCALE, SEND_DATA_BUF_LEN, WindowedRttMin, burst_pkts, send_data_buf_len,
        token_bucket_with_tokens,
    };

    const TEST_MSS: usize = 1200;
    use crate::{
        codec::data_overhead,
        sack::{AckBall, AckBallSequence},
        udp::NO_FEC_MSS,
    };

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

    fn test_layer(now: Instant) -> super::ReliableLayer {
        super::ReliableLayer::new(
            NonZeroUsize::new(TEST_MSS).unwrap(),
            crate::frame_delivery::FrameDelivery::default(),
            now,
        )
        .0
    }

    fn send_burst(rl: &mut super::ReliableLayer, n: usize, now: Instant) {
        let payload = vec![0u8; 100];
        let mut pkt = vec![0u8; TEST_MSS];
        for _ in 0..n {
            assert_eq!(
                rl.send_data_buf(&payload, now),
                payload.len(),
                "send_data_buf must accept the 100-byte payload"
            );
            assert!(
                rl.send_data_pkt(&mut pkt, now).is_some(),
                "send_data_pkt must send a packet"
            );
        }
    }

    fn send_max(rl: &mut super::ReliableLayer, now: Instant) -> usize {
        let payload_len = rl.max_data_size_per_pkt();
        let payload = vec![0u8; payload_len];
        let mut pkt = vec![0u8; TEST_MSS];
        let mut sent = 0;
        for _ in 0..20_000 {
            let free = rl.send_data_buf.capacity() - rl.send_data_buf.len();
            if free >= payload_len && rl.send_data_buf(&payload, now) < payload.len() {
                break;
            }
            if rl.send_data_pkt(&mut pkt, now).is_none() {
                break;
            }
            sent += 1;
        }
        sent
    }

    fn ack_all(rl: &mut super::ReliableLayer, rtt: Option<Duration>, now: Instant) {
        let next_seq = rl.pkt_send_space().next_seq();
        if next_seq == 0 {
            return;
        }
        if let Some(rtt) = rtt {
            rl.sample_rtt(rtt, now);
        }
        let acks = [AckBall {
            start: 0,
            size: NonZeroU64::new(next_seq).unwrap(),
        }];
        rl.recv_ack_pkt(AckBallSequence::new(&acks), now);
    }

    fn send_one(rl: &mut super::ReliableLayer, now: Instant) -> u64 {
        let payload = vec![0u8; 100];
        let mut pkt = vec![0u8; TEST_MSS];
        assert_eq!(
            rl.send_data_buf(&payload, now),
            payload.len(),
            "send_data_buf must accept the 100-byte payload"
        );
        let p = rl
            .send_data_pkt(&mut pkt, now)
            .expect("send_data_pkt must send");
        match p.data_written {
            super::DataPktPayload::Data(_) => p.seq,
            _ => panic!("expected data packet"),
        }
    }

    fn ack_seq(rl: &mut super::ReliableLayer, seq: u64, rtt: Duration, now: Instant) {
        rl.sample_rtt(rtt, now);
        let acks = [AckBall {
            start: seq,
            size: NonZeroU64::new(1).unwrap(),
        }];
        rl.recv_ack_pkt(AckBallSequence::new(&acks), now);
    }

    /// Feed `count` identical RTT samples in rapid succession to converge the
    /// SRTT filter.  This lets tests establish a stable high RTT while the
    /// WindowedRttMin bucket is still the warm-up floor.
    fn feed_rtt(rl: &mut super::ReliableLayer, count: usize, rtt: Duration, start: Instant) {
        for i in 0..count {
            let t = start + Duration::from_micros(i as u64 * 100);
            rl.sample_rtt(rtt, t);
        }
    }

    fn try_send(rl: &mut super::ReliableLayer, n: usize, now: Instant) -> usize {
        let payload_len = rl.max_data_size_per_pkt();
        let payload = vec![0u8; payload_len];
        let mut pkt = vec![0u8; TEST_MSS];
        let mut sent = 0;
        for _ in 0..n {
            let free = rl.send_data_buf.capacity() - rl.send_data_buf.len();
            if free >= payload_len {
                rl.send_data_buf(&payload, now);
            }
            if rl.send_data_pkt(&mut pkt, now).is_none() {
                break;
            }
            sent += 1;
        }
        sent
    }

    fn ack_prefix(rl: &mut super::ReliableLayer, hi: u64, rtt: Duration, now: Instant) -> bool {
        if hi == 0 {
            return false;
        }
        rl.sample_rtt(rtt, now);
        let acks = [AckBall {
            start: 0,
            size: NonZeroU64::new(hi).unwrap(),
        }];
        rl.recv_ack_pkt(AckBallSequence::new(&acks), now).is_some()
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

    #[test]
    fn slow_start_survives_zero_rtt_first_echo() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);

        send_burst(&mut rl, 2, t0);
        ack_all(&mut rl, Some(Duration::ZERO), t0 + Duration::from_micros(1));

        assert!(
            rl.log().send_rate.is_finite(),
            "send_rate must not be inf/nan after zero-rtt first echo"
        );
    }

    #[test]
    fn slow_start_accumulator_is_per_episode() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);

        // First burst: 8 packets, acked after 40 ms.
        send_burst(&mut rl, 8, t0);
        let t1 = t0 + Duration::from_millis(40);
        ack_all(&mut rl, Some(Duration::from_millis(40)), t1);

        // Wait long enough to create a 1960 ms idle gap before the next episode.
        let mut t = t1 + Duration::from_millis(1960);

        // 20 rounds of 4 packets, each acked after 40 ms, then idle for 1960 ms.
        for _ in 0..20 {
            send_burst(&mut rl, 4, t);
            let ack_time = t + Duration::from_millis(40);
            ack_all(&mut rl, Some(Duration::from_millis(40)), ack_time);
            t = ack_time + Duration::from_millis(1960);
        }

        // A lifetime accumulator over all ~88 acked packets with a 40 ms control
        // RTT would read ~88/0.04 = 2200 packets/second.  With per-episode reset
        // it should stay bounded, around 2 * INIT_SEND_RATE.
        assert!(
            rl.log().send_rate <= 2.0 * INIT_SEND_RATE,
            "per-episode accumulator should keep slow-start bounded, got {}",
            rl.log().send_rate
        );
    }

    #[test]
    fn outage_restore_restarts_at_init_rate() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        // 50 rounds of filling cwnd and acking after 40 ms to warm up past
        // 2 * INIT_SEND_RATE.
        for _ in 0..50 {
            send_max(&mut rl, t);
            t += Duration::from_millis(40);
            ack_all(&mut rl, Some(Duration::from_millis(40)), t);
            t += Duration::from_nanos(1);
        }
        assert!(
            rl.log().send_rate > 2.0 * INIT_SEND_RATE,
            "warm-up rate must exceed 2 * INIT_SEND_RATE, got {}",
            rl.log().send_rate
        );

        // Send a new flight, then let it sit unacked for 10 s so the next ACK
        // triggers outage recovery without a fresh RTT echo.
        let _final_sent = send_max(&mut rl, t);
        let restore_time = t + Duration::from_secs(10);
        ack_all(&mut rl, None, restore_time);

        let final_rate = rl.log().send_rate;
        assert!(
            (final_rate - INIT_SEND_RATE).abs() < 1e-9,
            "outage restore should restart at exactly INIT_SEND_RATE, got {final_rate}"
        );
    }

    #[test]
    fn capacity_drop_drain_follows_delivery_past_stale_peak_floor() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        // Ramp delivery at a brisk 10 ms RTT so the slow-start rate climbs
        // quickly.  send_max respects the cwnd limit, sending as many packets
        // as the cwnd and token bucket allow.  After one round the ACK-clocked
        // slow start pushes the rate well above 4× INIT_SEND_RATE; subsequent
        // rounds produce DRE rate samples that exit slow start and let the CC
        // probe branch take over.
        let ramp_rtt = Duration::from_millis(10);
        for _ in 0..40 {
            send_max(&mut rl, t);
            t += ramp_rtt;
            ack_all(&mut rl, Some(ramp_rtt), t);
        }
        let peak_rate = rl.log().send_rate;
        assert!(
            peak_rate > INIT_SEND_RATE * 4.0,
            "peak send rate {peak_rate} must exceed 4× INIT_SEND_RATE ({})",
            INIT_SEND_RATE * 4.0
        );
        assert!(
            rl.delivery_peak.update(t, 0.0) * DRAIN_FLOOR_PEAK_FRACTION > INIT_SEND_RATE,
            "DRE delivery peak * DRAIN_FLOOR_PEAK_FRACTION must exceed INIT_SEND_RATE, got {}",
            rl.delivery_peak.update(t, 0.0)
        );
        assert!(
            !rl.slow_start,
            "slow start must have exited during the ramp"
        );

        // Feed the drop-path RTT through `sample_rtt` so the SRTT and rttvar
        // filters settle before the first real send-ack cycle.
        let drop_rtt = Duration::from_millis(1000);
        feed_rtt(&mut rl, 30, drop_rtt, t);
        t += Duration::from_millis(1);

        // Capacity drops to a deep-buffered ~60 pkt/s path.  Inject a single
        // synthetic loss event so loss_event_rate returns a value and the
        // decay-grace clock can start; without this the blind-flow rule keeps
        // the floor at the full cap.  Keep flight shallow with spread-out
        // try_send so the DRE produces a meaningful interval for linear
        // backoff.  The inject_loss_event now uses smooth_rtt so the loss
        // entry persists long enough for deliveries to accumulate.
        rl.pkt_send_space.inject_loss_event(t);
        for _ in 0..10 {
            let round_start = t;
            for _ in 0..8 {
                try_send(&mut rl, 1, t);
                t += Duration::from_millis(100);
            }
            t = round_start + drop_rtt;
            let next_seq = rl.pkt_send_space().next_seq();
            ack_prefix(&mut rl, next_seq, drop_rtt, t);
        }

        // After ~10 s (3 s grace + 7 excess RTTs) the floor has decayed well
        // below 100 pkt/s and the send rate must follow.  The original
        // delivery peak is at most 10 s old (DELIVERY_PEAK_BUCKET = 10 s),
        // so it is still in the window and contributes a genuine cap — the
        // decay comes from the clocked grace path, not from the peak aging out.
        let final_rate = rl.log().send_rate;
        assert!(
            final_rate < 100.0,
            "send rate must decay below 100 pkt/s within 10 s, got {final_rate}"
        );
    }

    #[test]
    fn gentle_drain_guard_survives_rtt_floor_ratchet_and_scales_cooldown() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);

        // Settle the RTT floor around 600 ms by feeding long-RTT samples.
        let mut t = t0;
        for _ in 0..8 {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(700);
            ack_seq(&mut rl, seq, Duration::from_millis(600), t);
        }
        assert!(
            rl.pkt_send_space.smooth_rtt() >= Duration::from_millis(500),
            "sRTT must settle high, got {:?}",
            rl.pkt_send_space.smooth_rtt()
        );

        // Quickly converge SRTT to 1 s while the snapshot window is still the
        // warm-up 600 ms floor, then keep the queue building at a normal pace
        // until gentle mode enters.  The floor will not ratchet for at least 5 s.
        feed_rtt(&mut rl, 20, Duration::from_millis(1000), t);
        t += Duration::from_millis(1);
        let enter_start = t;
        loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(1100);
            ack_seq(&mut rl, seq, Duration::from_millis(1000), t);
            if rl.gentle.gentle_mode() {
                break;
            }
            assert!(
                t < enter_start + Duration::from_secs(6),
                "gentle mode must enter within a few seconds"
            );
        }
        let episode = rl.gentle.drain_episode().unwrap().clone();
        assert!(
            episode.floor0 <= Duration::from_millis(650),
            "episode floor snapshot must be taken before the ratchet, got {:?}",
            episode.floor0
        );
        let gap0 = episode.gap0;
        assert!(gap0 > Duration::ZERO, "initial gap must be positive");

        // Mid-episode: simulate the WindowedRttMin bucket rotating and
        // ratcheting the live floor upward.  Replace rl.rtt_floor with a
        // fresh window pre-fed ~750 ms while the drain episode holds ~1000 ms.
        // The live gap (smooth - new_floor) genuinely shrinks below the
        // GENTLE_DRAIN_GAP_SHRINK threshold, so the live-floor variant WOULD
        // suppress the guard.  The episode's floor0 snapshot keeps the guard
        // alive because it is compared against the pre-ratchet floor.
        {
            let mut new_floor = WindowedRttMin::new(t);
            for i in 0..6 {
                let feed_time = t + Duration::from_millis(750) * i;
                new_floor.update(feed_time, Duration::from_millis(750));
            }
            rl.rtt_floor = new_floor;
        }
        t += Duration::from_millis(1);
        let shrink_threshold = gap0.mul_f64(GENTLE_DRAIN_GAP_SHRINK);

        // The guard must still fire because it compares against the episode's
        // floor0 snapshot, not the live floor.  Drain at ~1000 ms RTT so
        // smooth stays ~1000 ms and the live gap remains below the threshold
        // for the entire guard window — a live-floor mutant that suppresses
        // the drain guard would fail here.
        let guard_start = t;
        let mut guard_fired = false;
        for _ in 0..20 {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(1100);
            ack_seq(&mut rl, seq, Duration::from_millis(1000), t);

            let smooth = rl.pkt_send_space.smooth_rtt();
            let live_floor = rl.rtt_floor.update(t, smooth);
            let live_gap = smooth.saturating_sub(live_floor);
            assert!(
                live_gap < shrink_threshold,
                "live gap {live_gap:?} must stay below {shrink_threshold:?} (GENTLE_DRAIN_GAP_SHRINK × gap0 = {gap0:?} × {GENTLE_DRAIN_GAP_SHRINK}); \
                 the live-floor variant would suppress the guard"
            );

            if !rl.gentle.gentle_mode() {
                guard_fired = true;
                break;
            }
            assert!(
                t < guard_start + Duration::from_secs(40),
                "guard must fire before the test times out"
            );
        }
        assert!(
            guard_fired,
            "gentle drain guard must fire despite rtt_floor ratchet"
        );

        // The scaled cooldown must dominate the fixed 15 s floor on this path.
        let control_rtt = rl.control_rtt();
        let expected_cooldown =
            GENTLE_REENTRY_COOLDOWN.max(control_rtt.mul_f64(GENTLE_REENTRY_COOLDOWN_RTTS));
        let block_until = rl
            .gentle
            .gentle_block_until()
            .expect("cooldown must be set");
        let cooldown = block_until.saturating_duration_since(t);
        assert!(
            cooldown >= expected_cooldown - Duration::from_millis(50),
            "cooldown must scale with RTT, got {:?} expected {:?}",
            cooldown,
            expected_cooldown
        );
    }

    #[test]
    fn gentle_mode_exits_after_full_floor_window_on_clean_link() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        // Warm up and enter gentle mode via a sustained queue-building stretch.
        // Converge SRTT quickly with a burst of identical high-RTT samples while
        // the floor is still the warm-up 200 ms value, then sustain the queue.
        for _ in 0..6 {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(250);
            ack_seq(&mut rl, seq, Duration::from_millis(200), t);
        }
        feed_rtt(&mut rl, 20, Duration::from_millis(800), t);
        t += Duration::from_millis(1);
        let enter_start = t;
        loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(900);
            ack_seq(&mut rl, seq, Duration::from_millis(800), t);
            if rl.gentle.gentle_mode() {
                break;
            }
            assert!(
                t < enter_start + Duration::from_secs(6),
                "gentle mode must enter within a few seconds"
            );
        }

        // The link is now clean: RTT drops back to the floor and the gate opens.
        // Wait until the gate actually opens, then keep it open for the full floor
        // window (max(5 s, 10 * smooth)) before exiting gentle mode.
        let clean_start = t;
        let threshold =
            RTT_MIN_BUCKET.max(Duration::from_millis(200).saturating_mul(RTT_MIN_BUCKET_RTT_SCALE));

        // Wait for the gate to open (queue_building becomes false).
        let gate_open_start = loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(250);
            ack_seq(&mut rl, seq, Duration::from_millis(200), t);
            if let Some(open_since) = rl.gentle.gentle_gate_open_since() {
                break open_since;
            }
            assert!(
                t < clean_start + Duration::from_secs(3),
                "gate must open after RTT drops"
            );
        };

        // Continue on the clean link until gentle mode exits.
        loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(250);
            ack_seq(&mut rl, seq, Duration::from_millis(200), t);
            if !rl.gentle.gentle_mode() {
                break;
            }
            assert!(
                t < gate_open_start + threshold + Duration::from_secs(2),
                "gentle mode must exit after the floor window"
            );
        }
        let open_for = t.saturating_duration_since(gate_open_start);
        assert!(
            open_for >= threshold - Duration::from_millis(100),
            "gentle mode must stay active until the open threshold elapses, got {:?}",
            open_for
        );
    }

    #[test]
    fn outage_reset_clears_gentle_reentry_cooldown() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        // Trigger the gentle-mode drain guard to set a re-entry cooldown.
        // Converge SRTT quickly with a burst of identical high-RTT samples
        // while the floor is still the warm-up 200 ms value.
        for _ in 0..6 {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(250);
            ack_seq(&mut rl, seq, Duration::from_millis(200), t);
        }
        feed_rtt(&mut rl, 20, Duration::from_millis(800), t);
        t += Duration::from_millis(1);
        let enter_start = t;
        loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(900);
            ack_seq(&mut rl, seq, Duration::from_millis(800), t);
            if rl.gentle.gentle_mode() {
                break;
            }
            assert!(
                t < enter_start + Duration::from_secs(6),
                "gentle mode must enter"
            );
        }
        let guard_start = t;
        loop {
            let seq = send_one(&mut rl, t);
            t += Duration::from_millis(900);
            ack_seq(&mut rl, seq, Duration::from_millis(800), t);
            if !rl.gentle.gentle_mode() {
                break;
            }
            assert!(t < guard_start + Duration::from_secs(14), "guard must fire");
        }
        assert!(
            rl.gentle
                .gentle_block_until()
                .is_some_and(|u| u > t + Duration::from_secs(10)),
            "guard must set a long cooldown"
        );

        // Make forward progress so outage detection is eligible, then let the
        // next packet stall for two RTOs and trigger an outage reset.
        let progress_seq = send_one(&mut rl, t);
        t += Duration::from_millis(50);
        ack_seq(&mut rl, progress_seq, Duration::from_millis(50), t);

        let stall_seq = send_one(&mut rl, t);
        let _ = stall_seq;
        let rto = rl.pkt_send_space.rto_duration();
        let detect_t = t + rto * 2 + Duration::from_millis(1);
        let acks = [AckBall {
            start: 0,
            size: NonZeroU64::new(progress_seq + 1).unwrap(),
        }];
        rl.recv_ack_pkt(AckBallSequence::new(&acks), detect_t);

        // Outage recovery must clear the gentle re-entry cooldown.
        assert!(
            rl.gentle.gentle_block_until().is_none(),
            "outage recovery must clear gentle re-entry cooldown"
        );
    }

    #[test]
    fn high_jitter_without_standing_queue_stays_out_of_gentle_mode() {
        let t0 = Instant::now();
        let mut rl = test_layer(t0);
        let mut t = t0;

        let prime_rtt = Duration::from_millis(100);
        for _ in 0..4 {
            let seq = send_one(&mut rl, t);
            t += prime_rtt;
            ack_seq(&mut rl, seq, prime_rtt, t);
        }
        assert!(
            rl.pkt_send_space().smooth_rtt() <= Duration::from_millis(120),
            "primed smooth RTT must be ~100 ms, got {:?}",
            rl.pkt_send_space().smooth_rtt()
        );

        let rtt_lo = Duration::from_millis(200);
        let rtt_hi = Duration::from_millis(300);
        let target_smooth = Duration::from_millis(250);
        for i in 0..48 {
            let rtt = if i % 2 == 0 { rtt_lo } else { rtt_hi };
            rl.sample_rtt(rtt, t);
            t += Duration::from_micros(100);
        }

        {
            let seq = send_one(&mut rl, t);
            t += rtt_hi;
            ack_seq(&mut rl, seq, rtt_hi, t);
        }

        let smooth = rl.pkt_send_space().smooth_rtt();
        let rttvar = rl.pkt_send_space().smooth_rtt_var();
        let floor_est = prime_rtt;
        let tol = rttvar
            .mul_f64(QUEUE_RTT_FACTOR)
            .max(floor_est.mul_f64(QUEUE_TOL_RTT_FRACTION))
            .max(QUEUE_RTT_FLOOR);
        let enter_tol = rttvar
            .mul_f64(QUEUE_RTT_FACTOR * GENTLE_ENTER_RTTVAR_FACTOR)
            .max(floor_est.mul_f64(QUEUE_TOL_RTT_FRACTION))
            .max(QUEUE_RTT_FLOOR);
        let gap = smooth.saturating_sub(floor_est);

        let margin_above_tol = gap.saturating_sub(tol);
        let margin_below_enter = enter_tol.saturating_sub(gap);
        assert!(
            margin_above_tol >= Duration::from_millis(40),
            "gap {gap:?} must exceed tol {tol:?} by >= 40 ms, margin {margin_above_tol:?}"
        );
        assert!(
            margin_below_enter >= Duration::from_millis(40),
            "enter_tol {enter_tol:?} must exceed gap {gap:?} by >= 40 ms, margin {margin_below_enter:?}"
        );

        let sustain_rtts = GENTLE_ENTER_RTTS * 2.0 + 1.0;
        let sustain_until = t + target_smooth.mul_f64(sustain_rtts);
        let rtts = [rtt_lo, rtt_hi];
        let mut sample_idx = 0;
        while t < sustain_until {
            let rtt = rtts[sample_idx % 2];
            sample_idx += 1;
            let seq = send_one(&mut rl, t);
            t += rtt;
            ack_seq(&mut rl, seq, rtt, t);
            assert!(
                !rl.gentle.gentle_mode(),
                "gentle_mode must stay false at sample {} (t={:?})",
                sample_idx,
                t.saturating_duration_since(t0)
            );
        }
    }

    /// `first_frame_pkt_within_fec_mss` — a 1-byte frame at a legal small MSS
    /// yields an on-wire first packet <= the FEC-reduced MSS.  Before the fix,
    /// the first-packet cap used `mss - data_overhead()` (not
    /// `frame_data_overhead()`), so the first packet was ~4 bytes over budget.
    #[test]
    fn first_frame_pkt_within_fec_mss() {
        let now = Instant::now();
        let mss = NO_FEC_MSS;
        let mut rl = super::ReliableLayer::new(
            NonZeroUsize::new(mss).unwrap(),
            crate::frame_delivery::FrameDelivery::enabled(),
            now,
        )
        .0;

        // A 1-byte frame at a legal small MSS.
        let frame = vec![0u8; 1];
        rl.send_frame_buf(&frame, now).unwrap();

        // Send the first (and only) packet.
        let mut pkt = vec![0u8; mss];
        let p = rl
            .send_data_pkt(&mut pkt, now)
            .expect("must send first pkt");
        assert!(p.frame_len.is_some(), "first packet must carry frame_len");

        let payload_len = match p.data_written {
            super::DataPktPayload::Data(n) => n.get(),
            _ => panic!("expected data packet"),
        };

        // The on-wire size of the first packet = frame_data_overhead + payload.
        let on_wire_first = crate::frame_delivery::wire::frame_data_overhead() + payload_len;
        assert!(
            on_wire_first <= mss,
            "first frame packet on-wire size {on_wire_first} must be <= MSS {mss}"
        );
    }

    /// Fix #11: `large_legal_frame_admitted_to_empty_stage` — a frame
    /// larger than the ~2-packet initial stage cap is admitted into an
    /// EMPTY stage regardless of the soft cap.  Before the fix, it was
    /// rejected and waited forever for a send notification that never
    /// came (hang).
    #[test]
    fn large_legal_frame_admitted_to_empty_stage() {
        let now = Instant::now();
        let mss = NO_FEC_MSS;
        let mut rl = super::ReliableLayer::new(
            NonZeroUsize::new(mss).unwrap(),
            crate::frame_delivery::FrameDelivery::enabled(),
            now,
        )
        .0;

        // The initial stage cap is ~2 packets worth of payload.
        let normal_payload = rl.max_data_size_per_pkt();
        let stage_cap = (2 * normal_payload).min(super::MAX_FRAME_LEN);

        // A frame larger than the stage cap but within MAX_FRAME_LEN.
        let frame_len = stage_cap + normal_payload;
        assert!(
            frame_len <= super::MAX_FRAME_LEN,
            "test frame must be a legal size"
        );
        let frame = vec![0u8; frame_len];

        // This must succeed (not return WouldBlock), proving the empty-stage
        // bypass works.
        let result = rl.send_frame_buf(&frame, now);
        assert!(
            result.is_ok(),
            "large legal frame must be admitted to an empty stage, got {result:?}"
        );
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
