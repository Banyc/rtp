use std::sync::Arc;
use std::time::Instant;

use super::connection::{Connection, ReceivedBatch};
use super::transmission_layer::{
    FEC_DEBUG, MAX_NUM_ACK, RecvBufs, RecvPkts, SendKillPkt, UnreliableRead,
};
use super::ts_echo::{RecentEchoes, TsEcho};
use crate::io_err::IoErr;
use crate::metrics::{MetricsSendDriverResumeSource, MetricsTerminationCause};
use crate::traffic_shaping::redundancy::fec::FecDecoderState;
use crate::{
    ack::AckBlocks,
    codec::decode,
    traffic_shaping::control::handshake::{PostOpenVerdict, is_post_open_candidate},
};

/// The read-actor half: owns the unreliable read transport, the FEC decoder
/// state (sole mutator), and the recent-echo RTT dedup window.  All shared
/// state lives behind the [`Connection`] facades, so no mutex guard survives
/// an await.
pub struct ReadHalf {
    utp_read: Box<dyn UnreliableRead>,
    fec: Option<FecDecoderState>,
    recent_echoes: RecentEchoes,
    shared: Arc<Connection>,
}

impl ReadHalf {
    pub(super) fn new(
        utp_read: Box<dyn UnreliableRead>,
        fec: Option<FecDecoderState>,
        shared: Arc<Connection>,
    ) -> Self {
        Self {
            utp_read,
            fec,
            recent_echoes: RecentEchoes::new(),
            shared,
        }
    }

    pub async fn recv_pkts(
        &mut self,
        bufs: &mut RecvBufs,
    ) -> Result<RecvPkts, (IoErr, SendKillPkt)> {
        let Self {
            utp_read,
            fec,
            recent_echoes,
            shared,
        } = self;
        let shared: &Connection = shared.as_ref();
        let record_error = |e: IoErr, cause: MetricsTerminationCause| {
            shared.press_error(e, cause);
            e
        };
        let mut recv_pkts = RecvPkts {
            num_ack_segments: 0,
            num_payload_segments: 0,
            num_fin_segments: 0,
        };
        let mut received_batch = ReceivedBatch::default();
        // ACK work for this receive batch is tracked as a boolean: ACK
        // content already lives in `AckHistory`, so storing the sequence
        // numbers themselves is dead weight.  The flag drives the awaited
        // recv → try_recv switch and the resume/notify control flow below.
        let mut has_ack_to_peer = false;
        for _ in 0..MAX_NUM_ACK {
            shared.check_error().map_err(|e| (e, SendKillPkt::No))?;
            let res = {
                match has_ack_to_peer {
                    // No ACKable packet seen yet: block on the next datagram.
                    false => utp_read.recv(&mut bufs.codec_pkt).await,
                    // The batch already has ACK work: drain without blocking.
                    true => {
                        let res = utp_read.try_recv(&mut bufs.codec_pkt);
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
                    return Err((
                        record_error(e, MetricsTerminationCause::UnreliableRead),
                        SendKillPkt::No,
                    ));
                }
            };
            let now = Instant::now();
            let read_pkt = &bufs.codec_pkt[..read_bytes];
            if is_post_open_candidate(read_pkt) {
                match shared.observe_post_open_handshake(read_pkt, now) {
                    PostOpenVerdict::NotHandshake => {}
                    PostOpenVerdict::Consumed | PostOpenVerdict::Complete => continue,
                    PostOpenVerdict::ReplyQueued => {
                        shared.request_send_driver_resume(
                            MetricsSendDriverResumeSource::PostOpenHandshake,
                        );
                        continue;
                    }
                }
            }
            bufs.codec_pkts.clear();
            let mut orig_pkt = None;
            match fec.as_mut() {
                Some(fec) => {
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
                let data = match decode(pkt, &mut bufs.ack_from_peer, shared.session_tag()) {
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
                        shared.sample_rtt(rtt, now);
                    }
                }
                if data.killed {
                    let e = IoErr::from(std::io::ErrorKind::BrokenPipe);
                    record_error(e, MetricsTerminationCause::PeerKill);
                    return Err((e, SendKillPkt::No));
                }
                let is_fin = data
                    .data
                    .as_ref()
                    .is_some_and(|data| data.buf_range.is_empty() && data.frame_len.is_none());
                let ack_next = data.ack_next;
                let (disposition, recv_eof, gentle_mode_exit) =
                    shared.with_reliable_layer_mut(|reliable_layer| {
                        // An ACK event exists only when the datagram carried an
                        // ACK command (ack_next is Some); a data-only packet must
                        // not fabricate one.
                        let gentle_mode_exit = if let Some(ack_next) = ack_next {
                            reliable_layer
                                .recv_ack_pkt(AckBlocks::new(ack_next, &bufs.ack_from_peer), now);
                            if FEC_DEBUG {
                                eprintln!("recv_ack_pkt: balls={:?}", bufs.ack_from_peer);
                            }
                            reliable_layer.take_gentle_mode_exit()
                        } else {
                            None
                        };
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
                        (
                            disposition,
                            reliable_layer.recv_eof_ready(),
                            gentle_mode_exit,
                        )
                    });
                if let Some(cause) = gentle_mode_exit {
                    shared.log_at(crate::metrics::MetricsEvent::GentleModeExit(cause), now);
                }
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
                if ack_next.is_some() {
                    shared.publish_packet_acknowledged();
                    shared.notify_session_outbound_progress();
                    // ACK processing may have freed send-window capacity, so
                    // wake the writer directly instead of letting it wait for
                    // a timer or the next application push.
                    shared.request_send_driver_resume(MetricsSendDriverResumeSource::PeerAck);
                }
                let Some(data) = data.data else {
                    shared.log(crate::metrics::MetricsEvent::ReceiveAckPacket);
                    continue;
                };
                if is_fin {
                    recv_pkts.num_fin_segments += 1;
                } else if disposition.is_some_and(|result| result.is_new()) {
                    recv_pkts.num_payload_segments += 1;
                }
                if disposition.is_some_and(|result| result.should_ack()) {
                    has_ack_to_peer = true;
                    received_batch.record_ack(is_fin, data.send_ts);
                } else {
                    end_of_acks = true;
                }
                shared.log(crate::metrics::MetricsEvent::ReceiveDataPacket);
            }
            if end_of_acks {
                break;
            }
        }
        shared.commit_received_batch(received_batch);
        if !has_ack_to_peer {
            let should_resume_send = shared.with_reliable_layer(|reliable_layer| {
                !reliable_layer.is_send_buf_empty()
                    && reliable_layer.pkt_send_space().accepts_new_pkt()
            });
            if should_resume_send {
                shared
                    .request_send_driver_resume(MetricsSendDriverResumeSource::ReceiveOpportunity);
            }
            return Ok(recv_pkts);
        }
        if has_ack_to_peer {
            shared.publish_data_received();
        }
        Ok(recv_pkts)
    }
}
