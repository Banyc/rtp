use std::io;
use std::sync::Arc;
use std::time::Instant;

use super::shared::{ReceivedBatch, Shared};
use super::transmission_layer::{
    FEC_DEBUG, MAX_NUM_ACK, RecvBufs, RecvPkts, SendKillPkt, UnreliableRead,
};
use super::ts_echo::{RecentEchoes, TsEcho};
use crate::{
    codec::decode,
    handshake::{Observation, is_post_open_candidate},
    sack::AckBallSequence,
};

pub struct ReadHalf {
    pub(crate) utp_read: Box<dyn UnreliableRead>,
    pub(crate) recent_echoes: RecentEchoes,
    pub(crate) shared: Arc<Shared>,
}

impl std::ops::Deref for ReadHalf {
    type Target = Shared;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

impl ReadHalf {
    pub async fn recv_pkts(
        &mut self,
        bufs: &mut RecvBufs,
    ) -> Result<RecvPkts, (std::io::ErrorKind, SendKillPkt)> {
        let Self {
            utp_read,
            recent_echoes,
            shared,
        } = self;
        let shared: &Shared = shared.as_ref();
        let termination = &shared.termination;
        let throw_error = |e: std::io::ErrorKind| {
            termination.press_error(e);
            e
        };
        let mut recv_pkts = RecvPkts {
            num_ack_segments: 0,
            num_payload_segments: 0,
            num_fin_segments: 0,
        };
        let mut received_batch = ReceivedBatch::default();
        bufs.ack_to_peer.clear();
        for _ in 0..MAX_NUM_ACK {
            shared
                .termination
                .throw_error()
                .map_err(|e| (e, SendKillPkt::No))?;
            let res = {
                match bufs.ack_to_peer.is_empty() {
                    true => utp_read.recv(&mut bufs.utp).await,
                    false => {
                        let res = utp_read.try_recv(&mut bufs.utp);
                        if let Err(e) = &res
                            && *e == std::io::ErrorKind::WouldBlock
                        {
                            break;
                        }
                        res
                    }
                }
            };
            let read_bytes = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err((throw_error(e), SendKillPkt::No));
                }
            };
            let now = Instant::now();
            let read_pkt = &bufs.utp[..read_bytes];
            if is_post_open_candidate(read_pkt) {
                match shared.observe_post_open_handshake(read_pkt, now) {
                    Observation::NotHandshake => {}
                    Observation::Filtered | Observation::Complete => continue,
                    Observation::ReplyQueued => {
                        shared.coord.resume_send.notify_one();
                        continue;
                    }
                }
            }
            bufs.codec_pkts.clear();
            let mut orig_pkt = None;
            match shared.fec.as_ref() {
                Some(fec) => {
                    let mut fec = fec.lock().unwrap();
                    if let Some(payload) = fec.decode(read_pkt) {
                        bufs.codec_pkts.push(payload);
                    }
                    while let Some(recovered) = fec.pop_recovered() {
                        bufs.codec_pkts.push(recovered);
                    }
                }
                None => {
                    orig_pkt = Some(read_pkt);
                }
            }
            let mut end_of_acks = false;
            for pkt in bufs.codec_pkts.iter().map(|p| p.as_slice()).chain(orig_pkt) {
                bufs.ack_from_peer.clear();
                let data = match decode(pkt, &mut bufs.ack_from_peer) {
                    Ok(x) => x,
                    Err(e) => {
                        if FEC_DEBUG {
                            eprintln!("recv_pkts: decode error: {e:?}");
                        }
                        continue;
                    }
                };
                if let Some(echo_ts) = data.echo_ts {
                    let local_ts = shared.wire_ts(now);
                    if recent_echoes.should_sample(echo_ts, now)
                        && let Some(rtt) = TsEcho::rtt_from_echo(local_ts, echo_ts)
                    {
                        shared.reliable_layer.lock().unwrap().sample_rtt(rtt, now);
                    }
                }
                if data.killed {
                    let e = std::io::ErrorKind::BrokenPipe;
                    throw_error(e);
                    return Err((e, SendKillPkt::No));
                }
                let is_fin = data
                    .data
                    .as_ref()
                    .is_some_and(|data| data.buf_range.is_empty() && data.frame_len.is_none());
                let (disposition, recv_eof) = {
                    let mut reliable_layer = shared.reliable_layer.lock().unwrap();
                    reliable_layer.recv_ack_pkt(AckBallSequence::new(&bufs.ack_from_peer), now);
                    if FEC_DEBUG {
                        eprintln!("recv_ack_pkt: balls={:?}", bufs.ack_from_peer);
                    }
                    let disposition = match &data.data {
                        None => None,
                        Some(data) => {
                            let disposition = reliable_layer.recv_data_pkt(
                                data.seq,
                                data.frame_len,
                                &pkt[data.buf_range.clone()],
                            );
                            if FEC_DEBUG {
                                eprintln!(
                                    "recv_data_pkt seq={} empty={} ack={}",
                                    data.seq,
                                    data.buf_range.is_empty(),
                                    disposition.should_ack()
                                );
                            }
                            Some(disposition)
                        }
                    };
                    (disposition, reliable_layer.recv_eof_ready())
                };
                if is_fin
                    && matches!(
                        disposition,
                        Some(crate::recv_queue::pkt_recv_space::RecvDisposition::Inserted)
                    )
                {
                    received_batch.record_inserted_fin();
                }
                received_batch.record_eof(recv_eof);
                recv_pkts.num_ack_segments += 1;
                shared.coord.sent_pkt_acked.notify_waiters();
                if data.data.is_none() {
                    shared.coord.session_outbound_progress.notify_one();
                }
                let Some(data) = data.data else {
                    shared.log("recv_ack_pkt");
                    continue;
                };
                if is_fin {
                    recv_pkts.num_fin_segments += 1;
                } else if disposition.is_some_and(|result| result.is_new()) {
                    recv_pkts.num_payload_segments += 1;
                }
                if disposition.is_some_and(|result| result.should_ack()) {
                    bufs.ack_to_peer.push(data.seq);
                    received_batch.record_ack(is_fin, data.send_ts);
                } else {
                    end_of_acks = true;
                }
                shared.log("recv_data_pkt");
            }
            if end_of_acks {
                break;
            }
        }
        shared.commit_received_batch(received_batch);
        if bufs.ack_to_peer.is_empty() {
            let should_resume_send = {
                let reliable_layer = shared.reliable_layer.lock().unwrap();
                !reliable_layer.is_send_buf_empty()
                    && reliable_layer.pkt_send_space().accepts_new_pkt()
            };
            if should_resume_send {
                shared.coord.resume_send.notify_one();
            }
            return Ok(recv_pkts);
        }
        if !bufs.ack_to_peer.is_empty() {
            shared.coord.recv_data_pkt.notify_waiters();
        }
        Ok(recv_pkts)
    }
}
