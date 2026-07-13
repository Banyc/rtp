use std::sync::Arc;
use std::time::Instant;

use super::shared::Shared;
use super::transmission_layer::{
    ACK_FLUSH_AGE, ACK_FLUSH_COUNT, FEC_DEBUG, MAX_NUM_ACK, RecvBufs, RecvPkts, SendKillPkt,
    UnreliableRead,
};
use super::ts_echo::{RecentEchoes, TsEcho};
use crate::{codec::decode, sack::AckBallSequence};

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
        let first_error = &shared.first_error;
        let throw_error = |e: std::io::ErrorKind| {
            first_error.set(e);
            e
        };
        let mut recv_pkts = RecvPkts {
            num_ack_segments: 0,
            num_payload_segments: 0,
            num_fin_segments: 0,
        };

        bufs.ack_to_peer.clear();
        let ack_deadline = {
            let s = shared.ack_flush.lock().unwrap();
            if 0 < s.pending_acks {
                s.last_ack_flush.map(|last| last + ACK_FLUSH_AGE)
            } else {
                None
            }
        };
        let mut ack_deadline_hit = false;
        for _ in 0..MAX_NUM_ACK {
            shared
                .first_error
                .throw_error()
                .map_err(|e| (e, SendKillPkt::No))?;

            let res = {
                match bufs.ack_to_peer.is_empty() {
                    true => {
                        if let Some(deadline) = ack_deadline {
                            tokio::select! {
                                res = utp_read.recv(&mut bufs.utp) => res,
                                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                                    ack_deadline_hit = true;
                                    break;
                                }
                            }
                        } else {
                            utp_read.recv(&mut bufs.utp).await
                        }
                    }
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

                let to_ack = {
                    let mut reliable_layer = shared.reliable_layer.lock().unwrap();

                    reliable_layer.recv_ack_pkt(AckBallSequence::new(&bufs.ack_from_peer), now);
                    if FEC_DEBUG {
                        eprintln!("recv_ack_pkt: balls={:?}", bufs.ack_from_peer);
                    }

                    match &data.data {
                        None => false,
                        Some(data) => {
                            let to_ack = reliable_layer.recv_data_pkt(
                                data.seq,
                                data.frame_len,
                                &pkt[data.buf_range.clone()],
                            );
                            if FEC_DEBUG {
                                eprintln!(
                                    "recv_data_pkt seq={} empty={} ack={}",
                                    data.seq,
                                    data.buf_range.is_empty(),
                                    to_ack
                                );
                            }
                            to_ack
                        }
                    }
                };
                recv_pkts.num_ack_segments += 1;
                shared.coord.sent_pkt_acked.notify_waiters();

                let Some(data) = data.data else {
                    shared.log("recv_ack_pkt");
                    continue;
                };

                if data.buf_range.is_empty() && data.frame_len.is_none() {
                    recv_pkts.num_fin_segments += 1;
                } else {
                    recv_pkts.num_payload_segments += 1;
                }
                if to_ack {
                    bufs.ack_to_peer.push(data.seq);
                    if let Some(send_ts) = data.send_ts {
                        shared.ack_flush.lock().unwrap().ts_echo.set(send_ts);
                    }
                } else {
                    end_of_acks = true;
                }
                shared.log("recv_data_pkt");
            }
            if end_of_acks {
                break;
            }
        }

        {
            let mut s = shared.ack_flush.lock().unwrap();
            s.pending_acks += bufs.ack_to_peer.len();
            if 0 < recv_pkts.num_fin_segments {
                s.fin_pending = true;
            }
        }

        if bufs.ack_to_peer.is_empty() && !ack_deadline_hit {
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

        let should_flush = ack_deadline_hit || 0 < recv_pkts.num_fin_segments || {
            let s = shared.ack_flush.lock().unwrap();
            ACK_FLUSH_COUNT <= s.pending_acks
                || s.last_ack_flush
                    .is_none_or(|last| ACK_FLUSH_AGE <= Instant::now().duration_since(last))
                || {
                    let reliable_layer = shared.reliable_layer.lock().unwrap();
                    reliable_layer
                        .pkt_recv_space()
                        .ack_history()
                        .balls()
                        .nth(1)
                        .is_some()
                }
        };
        if should_flush {
            shared.coord.resume_send.notify_one();
        }

        Ok(recv_pkts)
    }
}
