use super::super::wire::PACKET_LEN;
use super::super::wire::SEND_RETRY_INTERVAL;
use super::*;
use crate::{
    ack::{AckHistory, EncodeAck, MAX_ACK_BLOCKS},
    codec,
    io_err::IoErr,
    sequence::InitialSequences,
    socket::socket,
    traffic_shaping::{
        control::handshake::{
            PostOpenVerdict, post_open::POST_OPEN_LIFETIME, post_open::retry_delay,
        },
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

#[derive(Debug)]
struct RecordingWrite {
    inner: Arc<tokio::net::UdpSocket>,
    sizes: Arc<std::sync::Mutex<Vec<usize>>>,
}
#[async_trait]
impl UnreliableWrite for RecordingWrite {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        self.sizes.lock().unwrap().push(buf.len());
        self.inner.send(buf).await
    }
}

#[derive(Debug)]
struct RecordingRead {
    inner: Arc<tokio::net::UdpSocket>,
    sizes: Arc<std::sync::Mutex<Vec<usize>>>,
}
#[async_trait]
impl UnreliableRead for RecordingRead {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        let n = self.inner.try_recv(buf)?;
        self.sizes.lock().unwrap().push(n);
        Ok(n)
    }
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        let n = self.inner.recv(buf).await?;
        self.sizes.lock().unwrap().push(n);
        Ok(n)
    }
}

/// The operator's path, as measured on the deployed client: a 190 ms minimum
/// round trip with maxima of 1063 ms and 3205 ms.  It is a measurement, not
/// a protocol value — this transport is not RFC-compliant and follows TCP as
/// a baseline best practice — and it is the input the opening's leg budget
/// is derived from.
const FIELD_WORST_RTT: Duration = Duration::from_millis(3_205);

/// The handshake retransmission schedule must fit a *leg* budget: a jittered
/// retry (`RETRY_INTERVAL + RETRY_JITTER_MS`, clamped to the leg's deadline)
/// must be able to fire while that leg is still live, and the pre-handshake
/// burst delay must not exhaust a leg's budget either. These are relations
/// between the constants — asserted without racing wall-clock — so shrinking
/// the leg budget below the max retry offset (or growing the jitter past it)
/// is caught deterministically, exactly the class the end-to-end lost-leg
/// recovery test enforces through real time.
#[test]
fn the_retry_schedule_fits_inside_an_opening_leg() {
    let max_retry_offset = RETRY_INTERVAL + Duration::from_millis(RETRY_JITTER_MS);
    assert!(
        max_retry_offset < OPENING_LEG_TIMEOUT,
        "a jittered retransmission ({max_retry_offset:?}) must be able to fire before the \
         leg's deadline ({OPENING_LEG_TIMEOUT:?})"
    );
    assert!(
        Duration::from_millis(OPENING_JITTER_MS) < OPENING_LEG_TIMEOUT,
        "the pre-handshake burst delay ({OPENING_JITTER_MS} ms) must not exhaust a leg's \
         deadline ({OPENING_LEG_TIMEOUT:?})"
    );
}

/// A leg's budget must outlast the path's worst measured round trip, because
/// a leg *is* one round trip: a budget shorter than the path's round trip
/// expires before the peer's answer can arrive and the opening fails on a
/// live path.
///
/// The measured boundary this replaced is in `GATE.md`'s "The opening
/// handshake's time budget": on the real `rtp`-over-`NetemPair` sweep the old
/// whole-opening budget completed every leg up to 1500 ms of round trip and
/// failed from 1600 ms up, always with `TimedOut`, while the field's path
/// reaches 3205 ms.  This pin is a relation to that measured field value
/// rather than to a value of the test's own choosing, so raising the leg
/// budget without raising it past the field's worst round trip stays red.
#[test]
fn an_opening_leg_outlasts_the_fields_worst_round_trip() {
    assert!(
        OPENING_LEG_TIMEOUT >= FIELD_WORST_RTT,
        "a handshake leg is one round trip on the path, so its budget ({OPENING_LEG_TIMEOUT:?}) \
         must outlast the field's worst measured round trip ({FIELD_WORST_RTT:?}); the opening \
         is then bounded by two legs, not by one"
    );
    let whole_opening = OPENING_LEG_TIMEOUT + OPENING_LEG_TIMEOUT;
    assert!(
        whole_opening >= FIELD_WORST_RTT + FIELD_WORST_RTT,
        "the opening is two round trips, so its worst case ({whole_opening:?}) must cover both \
         of the field's worst, not one"
    );
}

/// A WouldBlocked handshake send must get at least one retry inside its
/// send budget: the jittered retry offset must be strictly below
/// `SEND_RETRY_BUDGET`, otherwise the first WouldBlock parks until the
/// budget elapses and the write dies without ever retrying. Pinned as a
/// constant relation (no timing), mirroring what
/// `send_completes_on_late_writability` enforces through real sleeps.
#[test]
fn a_blocked_send_gets_a_retry_within_its_budget() {
    let max_retry_offset = SEND_RETRY_INTERVAL + Duration::from_millis(SEND_RETRY_JITTER_MS);
    assert!(
        max_retry_offset < SEND_RETRY_BUDGET,
        "a jittered retry ({max_retry_offset:?}) must fit inside the send budget \
         ({SEND_RETRY_BUDGET:?})"
    );
}

/// The opening must complete on a path whose round trip is the field's worst
/// measured spike.  The legs are real wall clock (the handshake's deadlines
/// are `std::time::Instant`s, which a paused runtime clock does not drive),
/// so the row costs roughly two of the field's round trips — the price of
/// covering the one regime no other row reaches.
///
/// This pins the *shape* as well as the value: a slow first leg no longer
/// spends the second leg's budget, so the pair of 3204 ms legs completes in
/// ~6.4 s, which a single 4 s whole-opening budget could not.  Reverting
/// either the value (to 3 s) or the shape (one shared deadline) fails it with
/// `TimedOut`; the relation test above stays green under the shape revert,
/// so the two pins guard different properties.  The before/after sweep it is
/// drawn from is tabulated in `GATE.md`.
#[tokio::test(flavor = "multi_thread")]
async fn the_opening_completes_at_the_fields_worst_round_trip() {
    // Half the field's worst round trip, so a hop's delay is that one-way
    // time and a round trip is the whole 3204 ms.
    let one_way = FIELD_WORST_RTT / 2;
    let (client, server, mut relays) = delayed_handshake_channel_pair(one_way);
    let mut client = client;
    let mut server = server;
    let started = Instant::now();
    tokio::time::timeout(
        OPENING_LEG_TIMEOUT + OPENING_LEG_TIMEOUT + Duration::from_secs(1),
        async {
            tokio::try_join!(
                client_opening_handshake(&mut client),
                server_opening_handshake(&mut server),
            )
        },
    )
    .await
    .expect("the opening did not fit two leg budgets")
    .expect("the opening failed on the field's worst round trip");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= FIELD_WORST_RTT + FIELD_WORST_RTT,
        "the opening is two round trips, so completing in {elapsed:?} would not have crossed \
         the field's worst round trip twice; the delayed pair is not applying the delay"
    );
    // Both legs outlasted the 250 ms retry interval, so neither peer may
    // report a sample: a sample is valid only when the request succeeded on
    // its first transmission, and a delayed leg is ambiguous.  That the
    // opening completed anyway is the point — the route crossed the slow leg
    // twice and still finished inside the two leg budgets.
    assert!(
        client.initial_rtt.is_none(),
        "the client's Confirm→ConfirmAck leg crossed the field's worst round trip, so it was \
         retransmitted and its sample ({:?}) must be withheld",
        client.initial_rtt
    );
    assert!(
        server.initial_rtt.is_none(),
        "the server's HelloAck→Confirm leg crossed the field's worst round trip, so it was \
         retransmitted and its sample ({:?}) must be withheld",
        server.initial_rtt
    );
    relays.shutdown().await;
}

/// A handshake channel pair whose every hop is delayed by `one_way`, so a
/// round trip between the peers is `2 * one_way`.  The delay is a relay in
/// front of each read channel, not a sleep inside `send`: the write must
/// return immediately, because a real path's delay is on the wire after the
/// socket accepted the datagram, while the handshake's send budget bounds
/// only the write itself.  Each datagram is relayed by its own child task so
/// the relay cannot serialise a retransmission burst into a growing queue.
/// The relays live in the caller's [`tokio::task::JoinSet`], because this
/// crate forbids a detached `tokio::spawn`: dropping the owner must abort its
/// children.
fn delayed_handshake_channel_pair(
    one_way: Duration,
) -> (UnreliableLayer, UnreliableLayer, tokio::task::JoinSet<()>) {
    // Each direction is a write channel, a relay that holds each datagram for
    // `one_way`, and the read channel the peer's double reads from.
    let (client_to_server_tx, client_to_server_wire) = mpsc::channel(32);
    let (client_to_server_delayed_tx, client_to_server_rx) = mpsc::channel(32);
    let (server_to_client_tx, server_to_client_wire) = mpsc::channel(32);
    let (server_to_client_delayed_tx, server_to_client_rx) = mpsc::channel(32);
    let mut relays = tokio::task::JoinSet::new();
    relays.spawn(delayed_relay(
        client_to_server_wire,
        client_to_server_delayed_tx,
        one_way,
    ));
    relays.spawn(delayed_relay(
        server_to_client_wire,
        server_to_client_delayed_tx,
        one_way,
    ));
    let client = wrap_fec(
        Box::new(ChannelRead(server_to_client_rx)),
        Box::new(ChannelWrite::new(client_to_server_tx, None, false)),
        false,
    );
    let server = wrap_fec(
        Box::new(ChannelRead(client_to_server_rx)),
        Box::new(ChannelWrite::new(server_to_client_tx, None, false)),
        false,
    );
    (client, server, relays)
}

/// Forward every datagram arriving on `from` to `tx` `one_way` after it
/// arrived there, one child task per datagram so the delays overlap.
async fn delayed_relay(
    mut from: mpsc::Receiver<Vec<u8>>,
    tx: mpsc::Sender<Vec<u8>>,
    one_way: Duration,
) {
    let mut inflight = tokio::task::JoinSet::new();
    while let Some(datagram) = from.recv().await {
        let tx = tx.clone();
        inflight.spawn(async move {
            tokio::time::sleep(one_way).await;
            let _ = tx.send(datagram).await;
        });
        while inflight.try_join_next().is_some() {}
    }
    inflight.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn handshake_datagrams_are_padded_to_variable_sizes() {
    // Run the real opening handshake over a socket pair and record every
    // datagram size on the wire: with handshake padding, no datagram is
    // the old fixed 18-byte size — each carries a random tail up to the
    // MSS-derived bound, and the sizes vary across packets.
    let mss = crate::mss::Mss::try_new(crate::udp::NO_FEC_MSS).unwrap();
    let mut all_sizes = Vec::new();
    for _ in 0..5 {
        let a = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut client = wrap_fec(
            Box::new(RecordingRead {
                inner: Arc::clone(&a),
                sizes: Arc::clone(&sizes),
            }),
            Box::new(RecordingWrite {
                inner: Arc::clone(&a),
                sizes: Arc::clone(&sizes),
            }),
            false,
        );
        let mut server = wrap_fec(
            Box::new(RecordingRead {
                inner: Arc::clone(&b),
                sizes: Arc::clone(&sizes),
            }),
            Box::new(RecordingWrite {
                inner: Arc::clone(&b),
                sizes: Arc::clone(&sizes),
            }),
            false,
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::try_join!(
                client_opening_handshake(&mut client),
                server_opening_handshake(&mut server),
            )
        })
        .await
        .expect("opening handshake hung")
        .expect("opening handshake failed");
        all_sizes.extend(sizes.lock().unwrap().iter().copied());
    }
    assert!(!all_sizes.is_empty());
    assert!(
        all_sizes
            .iter()
            .all(|&n| (PACKET_LEN..=mss.get()).contains(&n)),
        "every handshake datagram must carry a padding tail, got sizes {all_sizes:?}"
    );
    let distinct: std::collections::HashSet<usize> = all_sizes.iter().copied().collect();
    assert!(
        distinct.len() > 1,
        "handshake datagram sizes must vary across packets, got {all_sizes:?}"
    );
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
        // Static payload-sized mode: any tail is padding and is ignored,
        // so a padded packet decodes to the same packet.
        let mut padded = encoded.to_vec();
        padded.extend_from_slice(&[0xAB; 8]);
        assert_eq!(Packet::decode(&padded), Some(packet));
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
        Some(established_at + retry_delay(nonce, 0))
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
        Some(established_at + retry_delay(nonce, 4))
    );
    let final_retry = established_at + retry_delay(nonce, 4);
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
    // One leg per dropped handshake kind, each over its own channel pair with
    // its own nonce, and each recovering in one opening retry interval
    // (250 ms + jitter) rather than in a fixed wait.  The legs are
    // independent, so they are polled concurrently: four sequential ~300 ms
    // episodes cost the tier a second that concurrency does not, and the
    // cells are unchanged -- same drop filter, same assertions, and a leg
    // that fails still fails the test.
    let (c0, s0) = handshake_channel_pair(Some(Kind::Hello), false);
    let (c1, s1) = handshake_channel_pair(Some(Kind::HelloAck), false);
    let (c2, s2) = handshake_channel_pair(Some(Kind::Confirm), false);
    let (c3, s3) = handshake_channel_pair(Some(Kind::ConfirmAck), false);
    tokio::join!(
        complete_over_pair(c0, s0, Some(Kind::Hello), false),
        complete_over_pair(c1, s1, Some(Kind::HelloAck), false),
        complete_over_pair(c2, s2, Some(Kind::Confirm), false),
        complete_over_pair(c3, s3, Some(Kind::ConfirmAck), false),
    );
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

fn handshake_channel_pair(
    dropped: Option<Kind>,
    duplicate: bool,
) -> (UnreliableLayer, UnreliableLayer) {
    let (client_to_server_tx, client_to_server_rx) = mpsc::channel(32);
    let (server_to_client_tx, server_to_client_rx) = mpsc::channel(32);
    let drop_client = dropped.filter(|kind| matches!(kind, Kind::Hello | Kind::Confirm));
    let drop_server = dropped.filter(|kind| matches!(kind, Kind::HelloAck | Kind::ConfirmAck));
    let client = wrap_fec(
        Box::new(ChannelRead(server_to_client_rx)),
        Box::new(ChannelWrite::new(
            client_to_server_tx,
            drop_client,
            duplicate,
        )),
        false,
    );
    let server = wrap_fec(
        Box::new(ChannelRead(client_to_server_rx)),
        Box::new(ChannelWrite::new(
            server_to_client_tx,
            drop_server,
            duplicate,
        )),
        false,
    );
    (client, server)
}

async fn complete_over_channels(dropped: Option<Kind>, duplicate: bool) {
    let (client, server) = handshake_channel_pair(dropped, duplicate);
    complete_over_pair(client, server, dropped, duplicate).await;
}

async fn complete_over_pair(
    mut client: UnreliableLayer,
    mut server: UnreliableLayer,
    dropped: Option<Kind>,
    duplicate: bool,
) {
    if dropped == Some(Kind::ConfirmAck) {
        let (_, server_socket) = tokio::time::timeout(
            OPENING_LEG_TIMEOUT + OPENING_LEG_TIMEOUT + Duration::from_secs(1),
            async {
                tokio::try_join!(
                    async { client_opening_handshake(&mut client).await },
                    async {
                        server_opening_handshake(&mut server).await?;
                        Ok::<_, io::Error>(socket(server, None))
                    },
                )
            },
        )
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
    tokio::time::timeout(
        OPENING_LEG_TIMEOUT + OPENING_LEG_TIMEOUT + Duration::from_secs(1),
        async {
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
        },
    )
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

    let mut received = [0; crate::udp::NO_FEC_MSS];
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
    let confirmations_dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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
            dropped: Arc::clone(&confirmations_dropped),
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
    // The premise the completion proves is that every one of the three
    // confirmations was lost, so the recovery cannot be the opening
    // confirmation arriving normally.  The second and third are the server's
    // answers to the client's 250 ms-jittered opening retransmissions, so this
    // counter only moves once that cadence has fired twice; a filter that
    // stops matching leaves the row green while it exercises nothing.
    assert_eq!(
        confirmations_dropped.load(Ordering::SeqCst),
        3,
        "the premise is three lost ConfirmAcks, so the completion must come from the guard \
         recovery rather than from the opening confirmation arriving on its first attempt"
    );
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
    let confirmations_dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut server = wrap_fec(
        Box::new(ChannelRead(client_to_server_rx)),
        Box::new(DropFirstConfirmationsWrite {
            tx: server_to_client_tx,
            remaining: 1,
            dropped: Arc::clone(&confirmations_dropped),
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
    // The row claims the *scheduled* slot recovered the opening, and that claim
    // rests on the opening confirmation having been lost: with the filter
    // matching nothing the client completes on the first confirmation in ~300
    // ms and this row passes in the same wall clock while proving nothing.
    assert_eq!(
        confirmations_dropped.load(Ordering::SeqCst),
        1,
        "the premise is one lost ConfirmAck; without it the client completes on the opening leg \
         and the +1 s scheduled slot is never needed"
    );
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
    // runtime cannot fast-forward this window. The assertion forbids an
    // event, so the wait must outlast the slot it forbids: the slot is
    // `1s + retry_delay`, whose per-nonce jitter places it in [1.0, 1.5) s,
    // and the nonce is drawn per connection from `SysRng`
    // (`client_opening_handshake`), so the worst case is 1.499 s and a
    // 1.6 s wait leaves the write driver's wake ~100 ms of margin. With the
    // retirement withheld in `PostOpenRecovery::observe`, the test still
    // passed 9 of 10 runs on a 1.1 s wait -- only the runs whose jitter was
    // at most 100 ms reached the slot -- and failed all 6 runs on 1.6 s.
    tokio::time::sleep(Duration::from_millis(1_600)).await;
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

    // Same wall-clock constraint as the retired-ready test: the post-open
    // retransmission slots are `std::time::Instant` scheduled, so they can
    // only fire with real time. The slot this wait exists to cover is the
    // +1s one, which is the server's duplicate confirmation: `1s +
    // retry_delay` puts it 1.0368-1.3833 s after the opening in eight probed
    // runs, and the client's retried Ready follows in the same millisecond,
    // so 1.6 s bounds the worst case (`1s + 499 ms` jitter). The old 3.2s
    // wait also reached past the +3s slot, but not deterministically -- the
    // per-nonce jitter places that slot in [3.0, 3.5) s -- and there is
    // nothing there to see: the retried Ready retires the recovery, so no
    // further confirmation is sent at any slot, and eight probed runs found
    // no event at all between 1.39 s and 4 s. The retirement is pinned
    // exactly, at no wall clock, by
    // `transmission::post_open_recovery::tests::retried_ready_retires_the_scheduled_retransmission_chain`.
    tokio::time::sleep(Duration::from_millis(1_600)).await;
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
    let confirmations_dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut server = wrap_fec(
        Box::new(ChannelRead(client_to_server_rx)),
        Box::new(DropFirstConfirmationsWrite {
            tx: server_to_client_tx,
            remaining: 1,
            dropped: Arc::clone(&confirmations_dropped),
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
    // The row claims a stale RTP datagram cannot retire the recovery, and that
    // claim rests on the recovery still being *live* when the stale datagram
    // lands: the lost first confirmation is what keeps it live.
    assert_eq!(
        confirmations_dropped.load(Ordering::SeqCst),
        1,
        "the premise is one lost ConfirmAck, so the recovery is still pending when the stale \
         RTP datagram arrives"
    );
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
    // untagged KILL, a KILL behind a wrong session tag (TAG_CMD + 8-byte tag
    // + KILL_CMD), and an untagged ACK.  All three must be dropped by the
    // session-tag check, not honoured — an honoured untagged KILL ends the
    // session below.  Each is built with the codec's own encoder, so the
    // datagrams are the codec's current wire shape rather than a hand-written
    // copy of it.
    let mut untagged_kill = [0u8; 16];
    let untagged_kill_len = codec::encode_kill(None, &mut untagged_kill).unwrap();
    forged_tx
        .send(untagged_kill[..untagged_kill_len].to_vec())
        .await
        .unwrap();
    let mut wrong_tag_kill = [0u8; 16];
    let n = codec::encode_kill(Some(0x1111_2222_3333_4444), &mut wrong_tag_kill).unwrap();
    forged_tx.send(wrong_tag_kill[..n].to_vec()).await.unwrap();
    let ack_history = AckHistory::new();
    let mut forged_ack = [0u8; 32];
    let n = codec::encode_ack_data(
        None,
        Some(EncodeAck {
            queue: &ack_history,
            first_block_index: 0,
            max_blocks: MAX_ACK_BLOCKS,
        }),
        None,
        None,
        &mut forged_ack,
    )
    .unwrap();
    forged_tx.send(forged_ack[..n].to_vec()).await.unwrap();

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
    let mss = client.mss;
    let result = client_phase(
        &mut client,
        0x1234,
        Kind::Confirm,
        Kind::ConfirmAck,
        Instant::now() + Duration::from_millis(20),
        mss,
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
            Err(mpsc::error::TryRecvError::Disconnected) => Err(io::ErrorKind::BrokenPipe.into()),
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
    /// How many ConfirmAcks this filter actually discarded.  Every test that
    /// uses it claims a recovery *because* those confirmations were lost, so
    /// the count is asserted: with the filter matching nothing the handshake
    /// completes on the first confirmation and the recovery the row names is
    /// never exercised, which the row's completion assertion alone cannot
    /// tell apart from a working recovery.
    dropped: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl UnreliableWrite for DropFirstConfirmationsWrite {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        if self.remaining > 0
            && Packet::decode(buf).is_some_and(|packet| packet.kind == Kind::ConfirmAck)
        {
            self.remaining -= 1;
            self.dropped.fetch_add(1, Ordering::SeqCst);
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
        if !self.injected && Packet::decode(buf).is_some_and(|packet| packet.kind == Kind::Hello) {
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
        if !self.injected && Packet::decode(buf).is_some_and(|packet| packet.kind == Kind::Confirm)
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
    let mut writer: Box<dyn UnreliableWrite> = Box::new(AlwaysWouldBlock(Arc::clone(&attempts)));
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
        attempts: usize,
    }
    #[async_trait]
    impl UnreliableWrite for LateWritable {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
            if self.attempts == 0 {
                Ok(buf.len())
            } else {
                self.attempts -= 1;
                Err(io::ErrorKind::WouldBlock.into())
            }
        }
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut writer: Box<dyn UnreliableWrite> = Box::new(LateWritable { attempts: 3 });
    tokio::time::timeout(Duration::from_secs(5), send(&mut writer, b"x", deadline))
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
