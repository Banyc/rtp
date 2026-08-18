use std::io;
use std::mem::size_of;
use std::time::{Duration, Instant};

use rand::TryRng;

use super::post_open::PostOpenHandshake;
use super::wire::{Kind, PACKET_LEN, Packet, SEND_RETRY_INTERVAL};
use crate::sequence::{InitialSequences, SequenceNumber};
use crate::transmission::transmission_layer::{UnreliableLayer, UnreliableRead, UnreliableWrite};

const OPENING_TIMEOUT: Duration = Duration::from_secs(3);
const RETRY_INTERVAL: Duration = Duration::from_millis(250);
const SEND_RETRY_BUDGET: Duration = Duration::from_millis(500);

/// Domain-separated splitmix64 finalizer over the handshake nonce.  Each
/// derivation domain yields an independent value from the same nonce.
fn mix(nonce: u64, domain: u64) -> u64 {
    let mut z = nonce ^ domain;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Domain for the per-connection session tag that authenticates codec
/// control-plane datagrams after open.  Derived from the handshake nonce
/// (splitmix64 finalizer, domain-separated) rather than used raw, so
/// data-plane traffic does not reveal the recovery-handshake nonce.  Both
/// peers compute the same value from the same nonce; an off-path attacker
/// never sees the nonce and therefore cannot forge a tag.
const SESSION_TAG_DOMAIN: u64 = 0x9e37_79b9_7f4a_7c15;
/// Domain for the client-to-server directional sequence start (`rtp-c2s!`).
const CLIENT_TO_SERVER_DOMAIN: u64 = 0x7274_702d_6332_7321;
/// Domain for the server-to-client directional sequence start (`rtp-s2c!`).
const SERVER_TO_CLIENT_DOMAIN: u64 = 0x7274_702d_7332_6321;

fn session_tag(nonce: u64) -> u64 {
    mix(nonce, SESSION_TAG_DOMAIN)
}

/// Directional initial sequences derived from the handshake nonce: the
/// client sends `client_to_server` and receives `server_to_client`; the
/// server's view is role-inverted.
fn directional_initial_sequences(nonce: u64) -> (SequenceNumber, SequenceNumber) {
    let client_to_server = SequenceNumber::from_wire(mix(nonce, CLIENT_TO_SERVER_DOMAIN));
    let server_to_client = SequenceNumber::from_wire(mix(nonce, SERVER_TO_CLIENT_DOMAIN));
    (client_to_server, server_to_client)
}

enum Received {
    Handshake(Packet),
    NextProtocol,
    Deadline,
}

pub async fn client_opening_handshake(unreliable: &mut UnreliableLayer) -> io::Result<()> {
    let mut nonce_bytes = [0; size_of::<u64>()];
    rand::rngs::SysRng
        .try_fill_bytes(&mut nonce_bytes)
        .expect("operating-system randomness unavailable");
    let nonce = u64::from_be_bytes(nonce_bytes);
    let deadline = Instant::now() + OPENING_TIMEOUT;
    client_phase(unreliable, nonce, Kind::Hello, Kind::HelloAck, deadline).await?;
    // Confirm→ConfirmAck is the final unambiguous leg that directly precedes
    // RTP traffic; the Hello→HelloAck leg is deliberately not sampled.
    let initial_rtt =
        client_phase(unreliable, nonce, Kind::Confirm, Kind::ConfirmAck, deadline).await?;
    unreliable.initial_rtt = initial_rtt;
    unreliable.post_open_handshake = Some(PostOpenHandshake::client(nonce, Instant::now()));
    unreliable.session_tag = Some(session_tag(nonce));
    let (client_to_server, server_to_client) = directional_initial_sequences(nonce);
    unreliable.initial_sequences = InitialSequences::client(client_to_server, server_to_client);
    Ok(())
}

pub async fn server_opening_handshake(unreliable: &mut UnreliableLayer) -> io::Result<()> {
    let deadline = Instant::now() + OPENING_TIMEOUT;
    let hello = loop {
        match receive_until(&mut unreliable.utp_read, deadline).await? {
            Received::Handshake(packet) if packet.kind == Kind::Hello => break packet,
            Received::Deadline => return Err(timeout()),
            Received::Handshake(_) | Received::NextProtocol => {}
        }
    };
    let initial_rtt = server_wait_for_confirm(unreliable, hello.nonce, deadline).await?;
    unreliable.initial_rtt = initial_rtt;
    server_confirm(unreliable, hello.nonce, deadline).await?;
    unreliable.post_open_handshake = Some(PostOpenHandshake::server(hello.nonce, Instant::now()));
    unreliable.session_tag = Some(session_tag(hello.nonce));
    let (client_to_server, server_to_client) = directional_initial_sequences(hello.nonce);
    unreliable.initial_sequences = InitialSequences::server(client_to_server, server_to_client);
    Ok(())
}

async fn client_phase(
    unreliable: &mut UnreliableLayer,
    nonce: u64,
    request: Kind,
    response: Kind,
    deadline: Instant,
) -> io::Result<Option<Duration>> {
    let request = Packet {
        kind: request,
        nonce,
    }
    .encode();
    let mut attempts = 0usize;
    loop {
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        send(&mut unreliable.utp_write, &request, deadline).await?;
        attempts += 1;
        let sent_at = Instant::now();
        let retry_at = retry_at(deadline);
        loop {
            match receive_until(&mut unreliable.utp_read, retry_at).await? {
                Received::Handshake(packet) if packet.nonce == nonce && packet.kind == response => {
                    // A sample is valid only when the request succeeded on its
                    // first transmission; after a retry the response is
                    // ambiguous, so report `None`.
                    return Ok((attempts == 1).then(|| Instant::now().duration_since(sent_at)));
                }
                Received::Deadline => break,
                Received::Handshake(_) | Received::NextProtocol => {}
            }
        }
    }
}

async fn server_wait_for_confirm(
    unreliable: &mut UnreliableLayer,
    nonce: u64,
    deadline: Instant,
) -> io::Result<Option<Duration>> {
    let hello_ack = Packet {
        kind: Kind::HelloAck,
        nonce,
    }
    .encode();
    let mut attempts = 0usize;
    loop {
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        send(&mut unreliable.utp_write, &hello_ack, deadline).await?;
        attempts += 1;
        let sent_at = Instant::now();
        let retry_at = retry_at(deadline);
        loop {
            match receive_until(&mut unreliable.utp_read, retry_at).await? {
                Received::Handshake(packet)
                    if packet.nonce == nonce && packet.kind == Kind::Confirm =>
                {
                    // A sample is valid only when the HelloAck succeeded on
                    // its first transmission; after a retry the Confirm is
                    // ambiguous, so report `None`.
                    return Ok((attempts == 1).then(|| Instant::now().duration_since(sent_at)));
                }
                Received::Handshake(packet)
                    if packet.nonce == nonce && packet.kind == Kind::Hello =>
                {
                    break;
                }
                Received::Deadline => break,
                Received::Handshake(_) | Received::NextProtocol => {}
            }
        }
    }
}

async fn server_confirm(
    unreliable: &mut UnreliableLayer,
    nonce: u64,
    deadline: Instant,
) -> io::Result<()> {
    let confirm_ack = Packet {
        kind: Kind::ConfirmAck,
        nonce,
    }
    .encode();
    send(&mut unreliable.utp_write, &confirm_ack, deadline).await
}

fn retry_at(deadline: Instant) -> Instant {
    Instant::now()
        .checked_add(RETRY_INTERVAL)
        .map(|instant| instant.min(deadline))
        .unwrap_or(deadline)
}

async fn receive_until(
    read: &mut Box<dyn UnreliableRead>,
    deadline: Instant,
) -> io::Result<Received> {
    if Instant::now() >= deadline {
        return Ok(Received::Deadline);
    }
    let mut bytes = [0; PACKET_LEN + 1];
    tokio::select! {
        result = read.recv(&mut bytes) => {
            let len = result.map_err(io::Error::from)?;
            let received = bytes.get(..len).ok_or(io::ErrorKind::InvalidData)?;
            Ok(match Packet::decode(received) {
                Some(packet) => Received::Handshake(packet),
                None => Received::NextProtocol,
            })
        }
        () = tokio::time::sleep_until(deadline.into()) => Ok(Received::Deadline),
    }
}

async fn send(
    write: &mut Box<dyn UnreliableWrite>,
    bytes: &[u8],
    deadline: Instant,
) -> io::Result<()> {
    let send_deadline = deadline.min(Instant::now() + SEND_RETRY_BUDGET);
    loop {
        if Instant::now() >= send_deadline {
            return Err(timeout());
        }
        match write.send(bytes).await {
            Ok(len) if len == bytes.len() => return Ok(()),
            Ok(_) => {}
            Err(error) if error == io::ErrorKind::WouldBlock => {}
            Err(kind) => return Err(io::Error::from(kind)),
        }
        if Instant::now() >= send_deadline {
            return Err(timeout());
        }
        let retry_at = Instant::now()
            .checked_add(SEND_RETRY_INTERVAL)
            .map(|instant| instant.min(send_deadline))
            .unwrap_or(send_deadline);
        tokio::time::sleep_until(retry_at.into()).await;
    }
}

fn timeout() -> io::Error {
    io::ErrorKind::TimedOut.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        codec,
        io_err::IoErr,
        sequence::InitialSequences,
        socket::socket,
        traffic_shaping::{
            control::handshake::{PostOpenVerdict, post_open::POST_OPEN_LIFETIME},
            redundancy::fec::{FecConfig, FecState},
        },
        transmission::test_doubles::PendingWrite,
        udp::wrap_fec,
    };
    use async_trait::async_trait;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::sync::mpsc;

    fn copy_datagram(datagram: &[u8], buf: &mut [u8]) -> Result<usize, IoErr> {
        if datagram.len() > buf.len() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        buf[..datagram.len()].copy_from_slice(datagram);
        Ok(datagram.len())
    }

    #[test]
    fn nonce_derives_distinct_role_inverted_directional_sequences() {
        let nonce = 0x0123_4567_89ab_cdef;
        let (client_to_server, server_to_client) = directional_initial_sequences(nonce);
        assert_ne!(
            client_to_server, server_to_client,
            "the two directional starts must be distinct"
        );
        assert_ne!(client_to_server, SequenceNumber::ZERO);
        assert_ne!(server_to_client, SequenceNumber::ZERO);
        let client = InitialSequences::client(client_to_server, server_to_client);
        let server = InitialSequences::server(client_to_server, server_to_client);
        assert_eq!(client.send, client_to_server, "client sends c2s");
        assert_eq!(client.recv, server_to_client, "client receives s2c");
        assert_eq!(
            server.send, server_to_client,
            "server sends s2c (role-inverted)"
        );
        assert_eq!(
            server.recv, client_to_server,
            "server receives c2s (role-inverted)"
        );
        // A different nonce derives different starts.
        let (c2s_2, s2c_2) = directional_initial_sequences(nonce ^ 1);
        assert_ne!(client_to_server, c2s_2);
        assert_ne!(server_to_client, s2c_2);
        // The session tag differs from both directional starts.
        assert_ne!(session_tag(nonce), client_to_server.to_wire());
        assert_ne!(session_tag(nonce), server_to_client.to_wire());
    }

    #[test]
    fn packets_are_framed_and_rejected_by_both_rtp_wire_modes() {
        for kind in [
            Kind::Hello,
            Kind::HelloAck,
            Kind::Confirm,
            Kind::ConfirmAck,
            Kind::Ready,
        ] {
            let packet = Packet {
                kind,
                nonce: 0x0123_4567_89ab_cdef,
            };
            let encoded = packet.encode();
            assert_eq!(Packet::decode(&encoded), Some(packet));
            let mut overlong = encoded.to_vec();
            overlong.push(0);
            assert_eq!(Packet::decode(&overlong), None);
            assert!(!codec::in_cmd_space(encoded[0]));
            assert!(codec::decode(&encoded, &mut Vec::new(), None).is_err());

            let fec = FecState::new(FecConfig {
                symbol_size: 1_424,
                small_group_parity_count: 1,
            });
            let (_encoder, mut decoder, _stats) = fec.into_actor_parts();
            assert!(decoder.decode(&encoded).is_none());
        }
    }

    #[test]
    fn post_open_recovery_uses_tcp_schedule_and_coalesces_missed_slots() {
        let established_at = Instant::now();
        let nonce = 0x0123_4567_89ab_cdef;
        let mut recovery = PostOpenHandshake::server(nonce, established_at);
        assert_eq!(
            recovery.next_send_time(established_at),
            Some(established_at + Duration::from_secs(1))
        );
        let confirm = Packet {
            kind: Kind::Confirm,
            nonce,
        }
        .encode();
        assert_eq!(
            recovery.observe(&confirm, established_at),
            PostOpenVerdict::ReplyQueued
        );
        assert_eq!(
            recovery.next_send_time(established_at),
            Some(established_at)
        );
        let response = recovery.take_due_response(established_at).unwrap();
        assert_eq!(
            Packet::decode(&response.bytes),
            Some(Packet {
                kind: Kind::ConfirmAck,
                nonce
            })
        );
        let late = established_at + Duration::from_secs(20);
        assert!(recovery.take_due_response(late).is_some());
        assert_eq!(
            recovery.next_send_time(late),
            Some(established_at + Duration::from_secs(31))
        );
        let final_retry = established_at + Duration::from_secs(31);
        assert!(recovery.take_due_response(final_retry).is_some());
        let expired = established_at + POST_OPEN_LIFETIME;
        assert_eq!(recovery.next_send_time(final_retry), Some(expired));
        assert_eq!(
            recovery.observe(&confirm, expired),
            PostOpenVerdict::Consumed
        );
    }

    #[test]
    fn client_queues_nonce_bound_ready_after_confirmation() {
        let established_at = Instant::now();
        let nonce = 0x0123_4567_89ab_cdef;
        let mut recovery = PostOpenHandshake::client(nonce, established_at);
        assert_eq!(
            recovery.next_send_time(established_at),
            Some(established_at)
        );
        let response = recovery.take_due_response(established_at).unwrap();
        assert_eq!(
            Packet::decode(&response.bytes),
            Some(Packet {
                kind: Kind::Ready,
                nonce,
            })
        );
        assert_eq!(
            recovery.next_send_time(established_at),
            Some(established_at + POST_OPEN_LIFETIME),
            "the client must retain its nonce after an unacknowledged UDP send"
        );
        let confirm_ack = Packet {
            kind: Kind::ConfirmAck,
            nonce,
        }
        .encode();
        assert_eq!(
            recovery.observe(&confirm_ack, established_at),
            PostOpenVerdict::ReplyQueued
        );
        assert!(recovery.take_due_response(established_at).is_some());
        let mut server = PostOpenHandshake::server(nonce, established_at);
        assert_eq!(
            server.observe(&response.bytes, established_at),
            PostOpenVerdict::Complete
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_handshake_leg_recovers_from_one_lost_datagram() {
        for dropped in [Kind::Hello, Kind::HelloAck, Kind::Confirm, Kind::ConfirmAck] {
            complete_over_channels(Some(dropped), false).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplicated_handshake_datagrams_are_idempotent() {
        complete_over_channels(None, true).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server_ignores_stale_rtp_before_and_during_opening() {
        let (client_to_server_tx, client_to_server_rx) = mpsc::channel(32);
        let (server_to_client_tx, server_to_client_rx) = mpsc::channel(32);
        client_to_server_tx.send(Vec::new()).await.unwrap();
        let mut client = wrap_fec(
            Box::new(ChannelRead(server_to_client_rx)),
            Box::new(InjectStaleRtpAfterHelloWrite {
                tx: client_to_server_tx,
                injected: false,
            }),
            false,
        );
        let mut server = wrap_fec(
            Box::new(ChannelRead(client_to_server_rx)),
            Box::new(ChannelWrite::new(server_to_client_tx, None, false)),
            false,
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::try_join!(
                client_opening_handshake(&mut client),
                server_opening_handshake(&mut server),
            )
        })
        .await
        .expect("stale RTP traffic stalled opening")
        .expect("stale RTP traffic aborted opening");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server_first_handshake_does_not_wait_for_rtp_traffic() {
        let (client_to_server_tx, client_to_server_rx) = mpsc::channel(32);
        let (server_to_client_tx, server_to_client_rx) = mpsc::channel(32);
        let mut client = wrap_fec(
            Box::new(ChannelRead(server_to_client_rx)),
            Box::new(ChannelWrite::new(client_to_server_tx, None, false)),
            false,
        );
        let mut server = wrap_fec(
            Box::new(ChannelRead(client_to_server_rx)),
            Box::new(ChannelWrite::new(server_to_client_tx, None, false)),
            false,
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::try_join!(
                client_opening_handshake(&mut client),
                server_opening_handshake(&mut server),
            )
        })
        .await
        .expect("server handshake waited for post-handshake RTP traffic")
        .expect("opening handshake failed");
    }

    async fn complete_over_channels(dropped: Option<Kind>, duplicate: bool) {
        let (client_to_server_tx, client_to_server_rx) = mpsc::channel(32);
        let (server_to_client_tx, server_to_client_rx) = mpsc::channel(32);
        let drop_client = dropped.filter(|kind| matches!(kind, Kind::Hello | Kind::Confirm));
        let drop_server = dropped.filter(|kind| matches!(kind, Kind::HelloAck | Kind::ConfirmAck));
        let mut client = wrap_fec(
            Box::new(ChannelRead(server_to_client_rx)),
            Box::new(ChannelWrite::new(
                client_to_server_tx,
                drop_client,
                duplicate,
            )),
            false,
        );
        let mut server = wrap_fec(
            Box::new(ChannelRead(client_to_server_rx)),
            Box::new(ChannelWrite::new(
                server_to_client_tx,
                drop_server,
                duplicate,
            )),
            false,
        );
        if dropped == Some(Kind::ConfirmAck) {
            let (_, server_socket) =
                tokio::time::timeout(OPENING_TIMEOUT + Duration::from_secs(1), async {
                    tokio::try_join!(
                        async { client_opening_handshake(&mut client).await },
                        async {
                            server_opening_handshake(&mut server).await?;
                            Ok::<_, io::Error>(socket(server, None))
                        },
                    )
                })
                .await
                .expect("post-open confirmation recovery hung")
                .expect("post-open confirmation recovery failed");
            assert!(
                client.initial_rtt.is_none(),
                "client must not sample after retransmitting Confirm because ConfirmAck was lost"
            );
            drop(server_socket);
            return;
        }

        let next_protocol = b"first RTP datagram";
        tokio::time::timeout(OPENING_TIMEOUT + Duration::from_secs(1), async {
            tokio::try_join!(
                async {
                    client_opening_handshake(&mut client).await?;
                    client
                        .utp_write
                        .send(next_protocol)
                        .await
                        .map_err(io::Error::from)?;
                    Ok::<_, io::Error>(())
                },
                server_opening_handshake(&mut server),
            )
        })
        .await
        .expect("opening handshake hung")
        .expect("opening handshake failed");

        // The opening-RTT sample must be unambiguous: both peers get `Some`
        // on an unlost first attempt, and the peer whose measured leg was
        // dropped gets `None` (a sample is never assigned after a
        // retransmission, even if the response later arrives quickly).
        if !duplicate {
            match dropped {
                None | Some(Kind::Hello) => {
                    assert!(
                        client.initial_rtt.is_some(),
                        "client must sample Confirm→ConfirmAck on an unlost first attempt"
                    );
                    assert!(
                        server.initial_rtt.is_some(),
                        "server must sample HelloAck→Confirm on an unlost first attempt"
                    );
                }
                Some(Kind::HelloAck) => {
                    assert!(
                        client.initial_rtt.is_some(),
                        "client's Confirm→ConfirmAck leg stays clean when only HelloAck is dropped"
                    );
                    assert!(
                        server.initial_rtt.is_none(),
                        "server must not sample after a HelloAck retransmission"
                    );
                }
                Some(Kind::Confirm) => {
                    // The client measures Confirm→ConfirmAck; its first
                    // Confirm was dropped, so it must not sample.  The server
                    // measures HelloAck→Confirm and legitimately sees a clean
                    // leg (its HelloAck succeeded first-try), so no server
                    // assertion here.
                    assert!(
                        client.initial_rtt.is_none(),
                        "client must not sample after a Confirm retransmission"
                    );
                }
                Some(Kind::ConfirmAck) | Some(Kind::Ready) => {
                    unreachable!("ConfirmAck-dropped path returns above; Ready is never dropped")
                }
            }
        }

        let mut received = [0; 64];
        loop {
            let len = server.utp_read.try_recv(&mut received).unwrap();
            if Packet::decode(&received[..len]).is_none() {
                assert_eq!(&received[..len], next_protocol);
                break;
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn post_open_guard_recovers_after_three_lost_confirmations() {
        let (client_to_server_tx, client_to_server_rx) = mpsc::channel(32);
        let (server_to_client_tx, server_to_client_rx) = mpsc::channel(32);
        let mut client = wrap_fec(
            Box::new(ChannelRead(server_to_client_rx)),
            Box::new(ChannelWrite::new(client_to_server_tx, None, false)),
            true,
        );
        let mut server = wrap_fec(
            Box::new(ChannelRead(client_to_server_rx)),
            Box::new(DropFirstConfirmationsWrite {
                tx: server_to_client_tx,
                remaining: 3,
            }),
            true,
        );
        let (_, server_socket) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::try_join!(
                async { client_opening_handshake(&mut client).await },
                async {
                    server_opening_handshake(&mut server).await?;
                    Ok::<_, io::Error>(socket(server, None))
                },
            )
        })
        .await
        .expect("post-open duplicate confirmation recovery hung")
        .expect("post-open duplicate confirmation recovery failed");
        drop(server_socket);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn post_open_timer_recovers_without_another_client_confirmation() {
        let (client_to_server_tx, client_to_server_rx) = mpsc::channel(32);
        let (server_to_client_tx, server_to_client_rx) = mpsc::channel(32);
        let mut client = wrap_fec(
            Box::new(ChannelRead(server_to_client_rx)),
            Box::new(DeliverFirstConfirmOnlyWrite {
                tx: client_to_server_tx,
                confirm_delivered: false,
            }),
            false,
        );
        let mut server = wrap_fec(
            Box::new(ChannelRead(client_to_server_rx)),
            Box::new(DropFirstConfirmationsWrite {
                tx: server_to_client_tx,
                remaining: 1,
            }),
            false,
        );
        let (_, server_socket) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::try_join!(
                async { client_opening_handshake(&mut client).await },
                async {
                    server_opening_handshake(&mut server).await?;
                    Ok::<_, io::Error>(socket(server, None))
                },
            )
        })
        .await
        .expect("post-open scheduled confirmation recovery hung")
        .expect("post-open scheduled confirmation recovery failed");
        drop(server_socket);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn nonce_bound_ready_retires_post_open_retransmissions() {
        let (client_to_server_tx, client_to_server_rx) = mpsc::channel(32);
        let (server_to_client_tx, server_to_client_rx) = mpsc::channel(32);
        let confirmation_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut client = wrap_fec(
            Box::new(ChannelRead(server_to_client_rx)),
            Box::new(ChannelWrite::new(client_to_server_tx, None, false)),
            false,
        );
        let mut server = wrap_fec(
            Box::new(ChannelRead(client_to_server_rx)),
            Box::new(CountingChannelWrite {
                tx: server_to_client_tx,
                confirmation_attempts: Arc::clone(&confirmation_attempts),
            }),
            false,
        );

        let (client_socket, server_socket) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::try_join!(
                async {
                    client_opening_handshake(&mut client).await?;
                    Ok::<_, io::Error>(socket(client, None))
                },
                async {
                    server_opening_handshake(&mut server).await?;
                    Ok::<_, io::Error>(socket(server, None))
                },
            )
        })
        .await
        .expect("opening handshake hung")
        .expect("opening handshake failed");

        // The server's post-open retransmission is scheduled with
        // `std::time::Instant` (see `claim_post_open_response`), so only real
        // wall-clock time can let the established+1s slot fire. `advance()`
        // moves the tokio clock, not `std::time::Instant`, so a paused
        // runtime cannot fast-forward this window.
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert_eq!(
            confirmation_attempts.load(Ordering::SeqCst),
            1,
            "post-open confirmation timer survived nonce-bound readiness"
        );
        drop((client_socket, server_socket));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lost_ready_is_recovered_by_a_duplicate_confirmation() {
        let (client_to_server_tx, client_to_server_rx) = mpsc::channel(32);
        let (server_to_client_tx, server_to_client_rx) = mpsc::channel(32);
        let ready_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let confirmation_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut client = wrap_fec(
            Box::new(ChannelRead(server_to_client_rx)),
            Box::new(DropFirstReadyWrite {
                tx: client_to_server_tx,
                ready_attempts: Arc::clone(&ready_attempts),
                dropped: false,
            }),
            false,
        );
        let mut server = wrap_fec(
            Box::new(ChannelRead(client_to_server_rx)),
            Box::new(CountingChannelWrite {
                tx: server_to_client_tx,
                confirmation_attempts: Arc::clone(&confirmation_attempts),
            }),
            false,
        );

        let (client_socket, server_socket) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::try_join!(
                async {
                    client_opening_handshake(&mut client).await?;
                    Ok::<_, io::Error>(socket(client, None))
                },
                async {
                    server_opening_handshake(&mut server).await?;
                    Ok::<_, io::Error>(socket(server, None))
                },
            )
        })
        .await
        .expect("opening handshake hung")
        .expect("opening handshake failed");

        // Same wall-clock constraint as the retired-ready test: the +1s and
        // +3s post-open retransmission slots are `std::time::Instant`
        // scheduled, so they can only fire with real time. Waiting 3.2s also
        // proves the server stopped retransmitting after +1s (a non-retired
        // recovery would send again at +3s, tripping `confirmation_attempts`).
        tokio::time::sleep(Duration::from_millis(3_200)).await;
        assert_eq!(ready_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            confirmation_attempts.load(Ordering::SeqCst),
            2,
            "server recovery continued after receiving the retried readiness packet"
        );
        drop((client_socket, server_socket));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_rtp_datagram_cannot_retire_nonce_bound_recovery() {
        let (client_to_server_tx, client_to_server_rx) = mpsc::channel(32);
        let (server_to_client_tx, server_to_client_rx) = mpsc::channel(32);
        let mut client = wrap_fec(
            Box::new(ChannelRead(server_to_client_rx)),
            Box::new(InjectStaleRtpAfterFirstConfirmWrite {
                tx: client_to_server_tx,
                injected: false,
            }),
            false,
        );
        let mut server = wrap_fec(
            Box::new(ChannelRead(client_to_server_rx)),
            Box::new(DropFirstConfirmationsWrite {
                tx: server_to_client_tx,
                remaining: 1,
            }),
            false,
        );
        let (_, server_socket) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::try_join!(
                async { client_opening_handshake(&mut client).await },
                async {
                    server_opening_handshake(&mut server).await?;
                    Ok::<_, io::Error>(socket(server, None))
                },
            )
        })
        .await
        .expect("stale RTP traffic retired opening recovery")
        .expect("nonce-bound recovery failed after stale RTP traffic");
        drop(server_socket);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn forged_control_datagrams_are_ignored_after_open() {
        let (client_to_server_tx, client_to_server_rx) = mpsc::channel(32);
        let (server_to_client_tx, server_to_client_rx) = mpsc::channel(32);
        let forged_tx = client_to_server_tx.clone();
        let mut client = wrap_fec(
            Box::new(ChannelRead(server_to_client_rx)),
            Box::new(ChannelWrite::new(client_to_server_tx, None, false)),
            false,
        );
        let mut server = wrap_fec(
            Box::new(ChannelRead(client_to_server_rx)),
            Box::new(ChannelWrite::new(server_to_client_tx, None, false)),
            false,
        );
        let (
            (mut client_read, mut client_write, _client_supervisor),
            (mut server_read, mut server_write, _server_supervisor),
        ) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::try_join!(
                async {
                    client_opening_handshake(&mut client).await?;
                    Ok::<_, io::Error>(socket(client, None))
                },
                async {
                    server_opening_handshake(&mut server).await?;
                    Ok::<_, io::Error>(socket(server, None))
                },
            )
        })
        .await
        .expect("opening handshake hung")
        .expect("opening handshake failed");

        // Legit tagged traffic flows in both directions after open.
        let mut buf = [0; 64];
        client_write.send(b"hello").await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), server_read.recv(&mut buf))
            .await
            .expect("tagged request timed out")
            .unwrap();
        assert_eq!(&buf[..n], b"hello");
        server_write.send(b"ack").await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), client_read.recv(&mut buf))
            .await
            .expect("tagged response timed out")
            .unwrap();
        assert_eq!(&buf[..n], b"ack");

        // Forged control datagrams delivered from the peer address: an
        // untagged KILL_CMD (2), a KILL_CMD behind a wrong session tag
        // (TAG_CMD 6 + 8-byte tag + KILL_CMD), and an untagged ACK block
        // claiming {start:0, size:u64::MAX} that would release the peer's
        // entire send window.  All three must be dropped by the session-tag
        // check, not honoured.
        forged_tx.send(vec![0x02]).await.unwrap();
        let mut wrong_tag_kill = vec![6u8];
        wrong_tag_kill.extend_from_slice(&0x1111_2222_3333_4444u64.to_be_bytes());
        wrong_tag_kill.push(0x02);
        forged_tx.send(wrong_tag_kill).await.unwrap();
        let mut forged_ack = vec![0u8]; // ACK_CMD
        forged_ack.extend_from_slice(&0u64.to_be_bytes()); // start 0
        forged_ack.extend_from_slice(&u64::MAX.to_be_bytes()); // size u64::MAX
        forged_tx.send(forged_ack).await.unwrap();

        // The session must survive: traffic still flows both ways.
        client_write.send(b"still-alive").await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), server_read.recv(&mut buf))
            .await
            .expect("forged control killed the session")
            .unwrap();
        assert_eq!(&buf[..n], b"still-alive");
        server_write.send(b"still-alive-2").await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), client_read.recv(&mut buf))
            .await
            .expect("forged control killed the session")
            .unwrap();
        assert_eq!(&buf[..n], b"still-alive-2");
    }

    #[tokio::test]
    async fn client_cannot_succeed_without_a_delivered_confirmation() {
        let (client_to_server_tx, _client_to_server_rx) = mpsc::channel(1);
        let (_server_to_client_tx, server_to_client_rx) = mpsc::channel(1);
        let mut client = wrap_fec(
            Box::new(ChannelRead(server_to_client_rx)),
            Box::new(ChannelWrite::new(client_to_server_tx, None, false)),
            false,
        );
        let result = client_phase(
            &mut client,
            0x1234,
            Kind::Confirm,
            Kind::ConfirmAck,
            Instant::now() + Duration::from_millis(20),
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[derive(Debug)]
    struct ChannelRead(mpsc::Receiver<Vec<u8>>);

    #[derive(Debug)]
    struct ChannelWrite {
        tx: mpsc::Sender<Vec<u8>>,
        drop_once: Option<Kind>,
        duplicate: bool,
    }

    #[async_trait]
    impl UnreliableRead for ChannelRead {
        fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
            match self.0.try_recv() {
                Ok(datagram) => copy_datagram(&datagram, buf),
                Err(mpsc::error::TryRecvError::Empty) => Err(io::ErrorKind::WouldBlock.into()),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    Err(io::ErrorKind::BrokenPipe.into())
                }
            }
        }

        async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
            let datagram = self.0.recv().await.ok_or(io::ErrorKind::BrokenPipe)?;
            copy_datagram(&datagram, buf)
        }
    }

    impl ChannelWrite {
        fn new(tx: mpsc::Sender<Vec<u8>>, drop_once: Option<Kind>, duplicate: bool) -> Self {
            Self {
                tx,
                drop_once,
                duplicate,
            }
        }
    }

    #[derive(Debug)]
    struct CountingChannelWrite {
        tx: mpsc::Sender<Vec<u8>>,
        confirmation_attempts: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[derive(Debug)]
    struct DropFirstReadyWrite {
        tx: mpsc::Sender<Vec<u8>>,
        ready_attempts: Arc<std::sync::atomic::AtomicUsize>,
        dropped: bool,
    }
    #[async_trait]
    impl UnreliableWrite for DropFirstReadyWrite {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
            if Packet::decode(buf).is_some_and(|packet| packet.kind == Kind::Ready) {
                self.ready_attempts.fetch_add(1, Ordering::SeqCst);
                if !self.dropped {
                    self.dropped = true;
                    return Ok(buf.len());
                }
            }
            self.tx
                .send(buf.to_vec())
                .await
                .map_err(|_| io::ErrorKind::BrokenPipe)?;
            Ok(buf.len())
        }
    }
    #[async_trait]
    impl UnreliableWrite for CountingChannelWrite {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
            if Packet::decode(buf).is_some_and(|packet| packet.kind == Kind::ConfirmAck) {
                self.confirmation_attempts.fetch_add(1, Ordering::SeqCst);
            }
            self.tx
                .send(buf.to_vec())
                .await
                .map_err(|_| io::ErrorKind::BrokenPipe)?;
            Ok(buf.len())
        }
    }

    #[derive(Debug)]
    struct DropFirstConfirmationsWrite {
        tx: mpsc::Sender<Vec<u8>>,
        remaining: usize,
    }

    #[async_trait]
    impl UnreliableWrite for DropFirstConfirmationsWrite {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
            if self.remaining > 0
                && Packet::decode(buf).is_some_and(|packet| packet.kind == Kind::ConfirmAck)
            {
                self.remaining -= 1;
                return Ok(buf.len());
            }
            self.tx
                .send(buf.to_vec())
                .await
                .map_err(|_| io::ErrorKind::BrokenPipe)?;
            Ok(buf.len())
        }
    }

    #[derive(Debug)]
    struct DeliverFirstConfirmOnlyWrite {
        tx: mpsc::Sender<Vec<u8>>,
        confirm_delivered: bool,
    }

    #[derive(Debug)]
    struct InjectStaleRtpAfterFirstConfirmWrite {
        tx: mpsc::Sender<Vec<u8>>,
        injected: bool,
    }

    #[async_trait]
    impl UnreliableWrite for DeliverFirstConfirmOnlyWrite {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
            if Packet::decode(buf).is_some_and(|packet| packet.kind == Kind::Confirm) {
                if self.confirm_delivered {
                    return Ok(buf.len());
                }
                self.confirm_delivered = true;
            }
            self.tx
                .send(buf.to_vec())
                .await
                .map_err(|_| io::ErrorKind::BrokenPipe)?;
            Ok(buf.len())
        }
    }

    #[derive(Debug)]
    struct InjectStaleRtpAfterHelloWrite {
        tx: mpsc::Sender<Vec<u8>>,
        injected: bool,
    }

    #[async_trait]
    impl UnreliableWrite for InjectStaleRtpAfterHelloWrite {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
            self.tx
                .send(buf.to_vec())
                .await
                .map_err(|_| io::ErrorKind::BrokenPipe)?;
            if !self.injected
                && Packet::decode(buf).is_some_and(|packet| packet.kind == Kind::Hello)
            {
                self.injected = true;
                self.tx
                    .send(Vec::new())
                    .await
                    .map_err(|_| io::ErrorKind::BrokenPipe)?;
            }
            Ok(buf.len())
        }
    }

    #[async_trait]
    impl UnreliableWrite for InjectStaleRtpAfterFirstConfirmWrite {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
            self.tx
                .send(buf.to_vec())
                .await
                .map_err(|_| io::ErrorKind::BrokenPipe)?;
            if !self.injected
                && Packet::decode(buf).is_some_and(|packet| packet.kind == Kind::Confirm)
            {
                self.injected = true;
                self.tx
                    .send(Vec::new())
                    .await
                    .map_err(|_| io::ErrorKind::BrokenPipe)?;
            }
            Ok(buf.len())
        }
    }

    #[async_trait]
    impl UnreliableWrite for ChannelWrite {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
            let kind = Packet::decode(buf).map(|packet| packet.kind);
            if kind.is_some() && self.drop_once == kind {
                self.drop_once = None;
                return Ok(buf.len());
            }
            self.tx
                .send(buf.to_vec())
                .await
                .map_err(|_| io::ErrorKind::BrokenPipe)?;
            if self.duplicate && kind.is_some() {
                self.tx
                    .send(buf.to_vec())
                    .await
                    .map_err(|_| io::ErrorKind::BrokenPipe)?;
            }
            Ok(buf.len())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_times_out_on_sustained_would_block() {
        #[derive(Debug)]
        struct AlwaysWouldBlock(Arc<std::sync::atomic::AtomicUsize>);
        #[async_trait]
        impl UnreliableWrite for AlwaysWouldBlock {
            async fn send(&mut self, _buf: &[u8]) -> Result<usize, IoErr> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(io::ErrorKind::WouldBlock.into())
            }
        }
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut writer: Box<dyn UnreliableWrite> =
            Box::new(AlwaysWouldBlock(Arc::clone(&attempts)));
        let deadline = Instant::now() + Duration::from_millis(5);
        let started = Instant::now();
        let result = send(&mut writer, b"x", deadline).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(started.elapsed() >= Duration::from_millis(5));
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_completes_on_late_writability() {
        #[derive(Debug)]
        struct LateWritable {
            ready_at: Instant,
        }
        #[async_trait]
        impl UnreliableWrite for LateWritable {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                if Instant::now() >= self.ready_at {
                    Ok(buf.len())
                } else {
                    Err(io::ErrorKind::WouldBlock.into())
                }
            }
        }
        let deadline = Instant::now() + Duration::from_millis(200);
        let mut writer: Box<dyn UnreliableWrite> = Box::new(LateWritable {
            ready_at: Instant::now() + Duration::from_millis(150),
        });
        tokio::time::timeout(Duration::from_secs(2), send(&mut writer, b"x", deadline))
            .await
            .expect("send hung")
            .expect("send missed late writability");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_does_not_cancel_pending_write_at_retry_deadline() {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut task = tokio::task::JoinSet::new();
        task.spawn({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let cancelled = Arc::clone(&cancelled);
            async move {
                let mut writer: Box<dyn UnreliableWrite> = Box::new(PendingWrite {
                    started,
                    release,
                    cancelled,
                });
                send(&mut writer, b"x", Instant::now() + Duration::from_millis(5)).await
            }
        });
        started.notified().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(task.try_join_next().is_none());
        assert!(!cancelled.load(Ordering::SeqCst));
        release.notify_one();
        task.join_next().await.unwrap().unwrap().unwrap();
        assert!(!cancelled.load(Ordering::SeqCst));
    }
}
