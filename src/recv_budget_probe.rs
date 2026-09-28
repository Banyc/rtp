//! Report-only probe: the receive-socket budget as a loss ceiling.
//!
//! Two arms.
//!
//! * [`probe_recv_budget_cliff`] measures the socket's own hold capacity in
//!   datagrams as a function of the requested `SO_RCVBUF`, with a paced
//!   sender and a receiver that never reads.  It establishes the law
//!   `arrival rate x stall > budget` in this transport's datagram size and
//!   calibrates the emulated budget the transport arm asks for.
//! * [`probe_recv_budget_transport`] drives `rtp`'s bulk lane at the product's
//!   offered rate over the field's delay with a stall of field magnitude and
//!   counts refusals, the catch-up tail and the wire cost with the budget
//!   this host's default emulates against one sized from the path.
//!
//! Both are opt-in (`#[ignore]`d) and report-only: they assert only their own
//! instrument's integrity (that a transfer finished and a stall was observed),
//! never a product bound.

#![cfg(test)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_udp::UdpSocket as VectoredUdpSocket;

use netem_test::NetemPair;
use netem_test::kit::presets::clean_delay_link;

use crate::udp::{
    AcceptConfig, ConnectConfig, Listener, ListenerConfig, MssConfig, NO_FEC_MSS,
    connect_with_socket,
};

/// The product's bulk-lane offer, the rate the operator's client drives.
const RATE_BYTES_PER_SEC: f64 = 1024.0 * 1024.0;
/// The field's reported minimum round trip.
const FIELD_RTT: Duration = Duration::from_millis(190);
/// One-way delay per direction: half the field's round trip.
const FIELD_OWD: Duration = Duration::from_millis(95);
/// `net.core.rmem_default` on Linux/x86_64: `SK_RMEM_DEFAULT` =
/// `SKB_TRUESIZE(256) * 256` = 832 * 256 = 212 992.
const LINUX_RMEM_DEFAULT: usize = 212_992;
/// `SKB_TRUESIZE(len)` over an rtp datagram: `len +
/// SKB_DATA_ALIGN(sizeof(struct sk_buff)) +`
/// `SKB_DATA_ALIGN(sizeof(struct skb_shared_info))`.  `SKB_DATA_ALIGN` is
/// `ALIGN(X, SMP_CACHE_BYTES)` = 64, and `212992 / 256 = 832 = 256 + 256 +
/// 320` pins the two structure terms to 256 and 320.
const fn skb_truesize(len: usize) -> usize {
    len + 256 + 320
}
/// Datagram on the wire: payload plus this transport's per-packet header.
const WIRE_DATAGRAM: usize = NO_FEC_MSS + 48;

/// Datagrams Linux's default budget holds for a full rtp datagram.
const LINUX_DEFAULT_HOLD_DATAGRAMS: usize = LINUX_RMEM_DEFAULT / skb_truesize(WIRE_DATAGRAM);

fn effective_recv_buf(sock: &VectoredUdpSocket) -> usize {
    sock.async_fd().get_ref().recv_buffer_size().unwrap()
}

fn set_recv_buf(sock: &VectoredUdpSocket, bytes: usize) {
    sock.async_fd()
        .get_ref()
        .set_recv_buffer_size(bytes)
        .unwrap();
}

// ───────────────────── arm 1: the socket's hold capacity ─────────────────

#[test]
#[ignore = "report-only measurement probe; run with --ignored --nocapture"]
fn probe_recv_budget_cliff() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    eprintln!(
        "[probe] Linux rmem_default {LINUX_RMEM_DEFAULT} / truesize({WIRE_DATAGRAM})={} = {LINUX_DEFAULT_HOLD_DATAGRAMS} datagrams",
        skb_truesize(WIRE_DATAGRAM)
    );
    let mut holds = Vec::new();
    for request in [None, Some(159_940usize), Some(398_458)] {
        let (sent, held, effective) = rt.block_on(hold_arm(request));
        eprintln!(
            "[probe] request={request:?} effective={effective} sent={sent} held={held} refused={}",
            sent - held
        );
        // The instrument's own integrity: an arm that measured nothing would
        // print a row a reader could mistake for a saturated hold.
        assert!(
            sent > 0 && held > 0,
            "arm request={request:?} measured nothing: sent={sent} held={held}"
        );
        if request.is_some() {
            holds.push((request, held));
        }
    }
    // The hold must track the *request*, or the emulated budget the transport
    // arm asks for is not the budget this socket has. (The `None` arm above is
    // the host's own as-bound default, which is outside the requested series.)
    assert!(
        holds.windows(2).all(|pair| pair[0].1 < pair[1].1),
        "a larger SO_RCVBUF request did not hold more datagrams: {holds:?}"
    );
}

async fn hold_arm(request: Option<usize>) -> (u64, u64, usize) {
    const SECONDS: f64 = 1.0;
    let receiver = VectoredUdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    if let Some(request) = request {
        set_recv_buf(&receiver, request);
    }
    let effective = effective_recv_buf(&receiver);
    let addr: SocketAddr = receiver.local_addr().unwrap();

    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sender.connect(addr).unwrap();
    let payload = vec![0u8; NO_FEC_MSS];
    let start = Instant::now();
    let mut sent = 0u64;
    while start.elapsed() < Duration::from_secs_f64(SECONDS) {
        let target =
            start + Duration::from_secs_f64(sent as f64 * NO_FEC_MSS as f64 / RATE_BYTES_PER_SEC);
        let now = Instant::now();
        if target > now {
            std::thread::sleep(target - now);
        }
        sender.send(&payload).unwrap();
        sent += 1;
    }

    let mut held = 0u64;
    let mut buf = vec![0u8; 4096];
    let mut idle = Duration::ZERO;
    while idle < Duration::from_millis(200) {
        // Drain the whole queue before sleeping: one datagram per sleep would
        // make the hold capacity a function of the drain loop's cadence.
        let mut drained = false;
        while receiver.try_recv(&mut buf).is_ok() {
            held += 1;
            drained = true;
        }
        if drained {
            idle = Duration::ZERO;
        } else {
            idle += Duration::from_millis(10);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    (sent, held, effective)
}

// ──────────────────── arm 2: the transport-level effect ───────────────────

/// What one transport arm measured.
#[derive(Debug, Clone, Copy)]
struct ArmOutcome {
    requested: Option<usize>,
    effective: usize,
    sent_before: u64,
    sent_total: u64,
    received_total: u64,
    catch_up: Option<Duration>,
    /// Wall clock from the sender's first write to the receiver's last byte,
    /// minus the offer's own ideal (`TOTAL_BYTES / RATE`): what the stall and
    /// its recovery cost the transfer end to end.
    excess: Option<Duration>,
    s2c_forwarded: u64,
    s2c_dropped: u64,
    c2s_forwarded: u64,
}

impl ArmOutcome {
    fn wire_datagrams_per_mib(&self) -> f64 {
        self.s2c_forwarded as f64 / (self.received_total as f64 / (1024.0 * 1024.0))
    }
}

#[test]
#[ignore = "report-only measurement probe; run with --ignored --nocapture"]
fn probe_recv_budget_transport() {
    let transport_bytes = (RATE_BYTES_PER_SEC * FIELD_RTT.as_secs_f64()) as usize;
    // The budget the receiver asks for: the payload one field round trip can
    // put on the wire, with 2x headroom for the stall to be crossed on the
    // path rather than in the socket.
    let path_budget = transport_bytes * 2;
    // The budget Linux's default holds, expressed as the byte request this
    // host needs to hold the same number of datagrams.
    let default_budget = LINUX_DEFAULT_HOLD_DATAGRAMS * WIRE_DATAGRAM;
    // The field's own worst reported stall: what a budget that actually
    // covers the deployed path would have to hold.
    let field_worst_budget = (RATE_BYTES_PER_SEC * 3.205) as usize;
    eprintln!(
        "[probe] rate {RATE_BYTES_PER_SEC} B/s, field rtt {:?}: one-rtt payload {transport_bytes} B = {:.0} datagrams; path budget {path_budget} B; emulated-default budget {default_budget} B = {LINUX_DEFAULT_HOLD_DATAGRAMS} datagrams; field-worst budget {field_worst_budget} B",
        FIELD_RTT,
        transport_bytes as f64 / WIRE_DATAGRAM as f64,
    );

    for (stall_ms, budgets) in [
        // A stall at the field's own floor: below what one round trip of the
        // offer can put on the wire.
        (
            300u64,
            vec![("default", default_budget), ("path", path_budget)],
        ),
        // The field's worst reported stall, with a budget that binds (`tiny`)
        // below every candidate and one that covers the stall (`field`) above
        // them, so the sweep shows what the quantity actually is.
        (
            3205,
            vec![
                ("tiny", 32_768),
                ("default", default_budget),
                ("path", path_budget),
                ("field", field_worst_budget),
            ],
        ),
    ] {
        for (label, request) in budgets {
            let request = Some(request);
            let outcome = transport_arm(request, Duration::from_millis(stall_ms));
            eprintln!(
                "[probe] arm={label} stall={stall_ms}ms request={:?} effective={} offer={:.2} MiB/s sent_before={} sent_bytes={} received_bytes={} catch_up={:?} excess={:?} s2c_forwarded={} s2c_dropped={} c2s_forwarded={} wire_per_mib={:.0}",
                outcome.requested,
                outcome.effective,
                outcome.sent_before as f64 / (1024.0 * 1024.0) / WARMUP.as_secs_f64(),
                outcome.sent_before,
                outcome.sent_total,
                outcome.received_total,
                outcome.catch_up,
                outcome.excess,
                outcome.s2c_forwarded,
                outcome.s2c_dropped,
                outcome.c2s_forwarded,
                outcome.wire_datagrams_per_mib(),
            );
            // The instrument's own integrity, in the probe's own body: this
            // arm delivered the whole transfer through its stall, and the
            // link it ran on dropped nothing — so any datagram the arm paid
            // for beyond the payload is the receiver's own budget refusing.
            assert!(
                outcome.received_total == TOTAL_BYTES,
                "arm {label} at {stall_ms} ms delivered {} of {TOTAL_BYTES} bytes",
                outcome.received_total
            );
            assert!(
                outcome.s2c_dropped == 0,
                "arm {label} at {stall_ms} ms ran on a link that dropped {} datagrams, so the arm did not isolate the receiver's budget",
                outcome.s2c_dropped
            );
        }
    }
}

/// Total payload the sender offers; the stall lands a quarter of the way in.
const TOTAL_BYTES: u64 = 4 * 1024 * 1024;
const WARMUP: Duration = Duration::from_secs(1);

fn transport_arm(request: Option<usize>, stall: Duration) -> ArmOutcome {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let started = Instant::now();
    let (ready_tx, ready_rx) = mpsc::channel::<usize>();
    let (stall_tx, stall_rx) = tokio::sync::oneshot::channel::<(u64, u64)>();
    let (stall_end_tx, stall_end_rx) = mpsc::channel::<(Instant, Instant)>();
    let (done_tx, done_rx) = mpsc::channel::<(u64, Option<Duration>, Instant)>();

    let (listener, server) = rt.block_on(async {
        let listener = Listener::bind("127.0.0.1:0", ListenerConfig::default())
            .await
            .unwrap();
        let server = listener.local_addr();
        (listener, server)
    });
    let pair = NetemPair::spawn(
        server,
        clean_delay_link(FIELD_OWD, 11),
        clean_delay_link(FIELD_OWD, 12),
    )
    .unwrap();
    let relay = pair.client_addr();

    // The receiver lives on its own OS thread with a current-thread runtime so
    // a stall can stop its socket polls without stopping the sender: the
    // kernel socket is then the only place the flight can land.
    let stall_ms = stall.as_millis() as u64;
    let receiver = std::thread::Builder::new()
        .name("recv-budget-receiver".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let sock = VectoredUdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .await
                    .unwrap();
                if let Some(request) = request {
                    set_recv_buf(&sock, request);
                }
                let effective = effective_recv_buf(&sock);
                let connected = connect_with_socket(
                    sock,
                    relay,
                    ConnectConfig {
                        handshake: false,
                        fec: false,
                        mss: MssConfig::Default,
                        ..ConnectConfig::default()
                    },
                )
                .await
                .unwrap();
                let (read, write, supervisor) =
                    (connected.read, connected.write, connected.supervisor);
                // Scope-owned like every rtp task: the set is dropped with
                // this runtime, so the drivers are aborted by their owner.
                let mut drivers = tokio::task::JoinSet::new();
                drivers.spawn(async move {
                    let _ = supervisor.await;
                });
                let mut read = read.into_async_read();
                let mut write = write.into_async_write();
                // The relay learns the client address from this first packet.
                write.write_all(b"x").await.unwrap();
                ready_tx.send(effective).unwrap();

                let (sent_before, stall_ms) = stall_rx.await.unwrap();
                let stall_begin = Instant::now();
                if stall_ms > 0 {
                    std::thread::sleep(Duration::from_millis(stall_ms));
                }
                let stall_end = Instant::now();
                stall_end_tx.send((stall_begin, stall_end)).unwrap();

                let mut buf = vec![0u8; 64 * 1024];
                let mut total = 0u64;
                let mut catch_up = None;
                loop {
                    let n = match read.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    total += n as u64;
                    if catch_up.is_none() && total >= sent_before {
                        catch_up = Some(stall_end.elapsed());
                    }
                    if total >= TOTAL_BYTES {
                        break;
                    }
                }
                done_tx.send((total, catch_up, Instant::now())).unwrap();
            });
        })
        .unwrap();

    // The sender: an rtp server accepted through the listener, writing at the
    // product's bulk offer. The accept loop must keep polling the listener —
    // `UtpListener` dispatches datagrams only while its accept future is
    // polled, so a one-shot accept stops the server's socket reads and the
    // client's ACKs are never seen.
    let sent = Arc::new(AtomicU64::new(0));
    let sent_task = Arc::clone(&sent);
    let stop = Arc::new(AtomicBool::new(false));
    let stop_task = Arc::clone(&stop);
    rt.spawn(async move {
        // Every handle the accept loop creates is held here until the arm ends.
        // A finished handler must not drop its session's drivers, and a repeat
        // accept of the same session must not drop its write half: either would
        // end the lane the arm is measuring. `UtpListener` dispatches datagrams
        // only while its accept future is polled, so the loop must keep running
        // after the lane's connection is served.
        let mut held: Vec<tokio::task::JoinSet<()>> = Vec::new();
        let mut duplicates = Vec::new();
        let mut served = false;
        loop {
            let accepted = listener
                .accept_without_handshake_with(AcceptConfig {
                    fec: false,
                    mss: MssConfig::Default,
                    ..AcceptConfig::default()
                })
                .await;
            let accepted = match accepted {
                Ok(a) => a,
                Err(_) => break,
            };
            if stop_task.load(Ordering::Relaxed) {
                break;
            }
            let mut set = tokio::task::JoinSet::new();
            let supervisor = accepted.supervisor;
            set.spawn(async move {
                let _ = supervisor.await;
            });
            let read = accepted.read.into_async_read();
            set.spawn(async move {
                let mut read = read;
                let mut buf = vec![0u8; 8 * 1024];
                while let Ok(n) = read.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                }
            });
            if served {
                duplicates.push(accepted.write.into_async_write());
                held.push(set);
                continue;
            }
            served = true;
            let mut write = accepted.write.into_async_write();
            let sent_task = Arc::clone(&sent_task);
            let stop_task = Arc::clone(&stop_task);
            set.spawn(async move {
                let chunk = vec![0u8; 32 * 1024];
                let start = Instant::now();
                let mut written = 0u64;
                while written < TOTAL_BYTES && !stop_task.load(Ordering::Relaxed) {
                    let target =
                        start + Duration::from_secs_f64(written as f64 / RATE_BYTES_PER_SEC);
                    let now = Instant::now();
                    if target > now {
                        tokio::time::sleep(target - now).await;
                    }
                    let take = chunk.len().min((TOTAL_BYTES - written) as usize);
                    if write.write_all(&chunk[..take]).await.is_err() {
                        break;
                    }
                    written += take as u64;
                    sent_task.store(written, Ordering::Relaxed);
                }
            });
            held.push(set);
        }
    });

    let effective = ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    std::thread::sleep(WARMUP);
    let sent_before = sent.load(Ordering::Relaxed);
    let _ = stall_tx.send((sent_before, stall_ms));
    let (stall_begin, stall_end) = stall_end_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let (received_total, catch_up, done_at) = done_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the receiver never drained the transfer");
    let s2c = pair.stats_s2c();
    let c2s = pair.stats_c2s();
    let sent_total = sent.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    drop(pair);
    receiver.join().unwrap();

    // The instrument's own integrity: the arm ran a whole transfer through a
    // stall. Without this a zero-sample run would print a table of zeros that
    // a reader could mistake for a measurement.
    assert!(
        received_total >= sent_before + (TOTAL_BYTES - sent_before) / 2,
        "the receiver drained only {received_total} bytes of {TOTAL_BYTES}"
    );
    let observed_stall = stall_end.duration_since(stall_begin);
    assert!(
        observed_stall >= stall,
        "the receiver held its socket un-polled for only {observed_stall:?} of the commanded {stall:?}"
    );
    ArmOutcome {
        requested: request,
        effective,
        sent_before,
        sent_total,
        received_total,
        catch_up,
        excess: Some(
            done_at
                .duration_since(started)
                .saturating_sub(Duration::from_secs_f64(
                    TOTAL_BYTES as f64 / RATE_BYTES_PER_SEC,
                )),
        ),
        s2c_forwarded: s2c.forwarded,
        s2c_dropped: s2c.dropped,
        c2s_forwarded: c2s.forwarded,
    }
}
