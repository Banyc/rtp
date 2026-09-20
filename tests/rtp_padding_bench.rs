//! Bench: fixed single-mode profile padding and fitted ACK padding on the
//! obfuscation layer.
//!
//! Measures (a) the wire size distribution of an obfuscated rtp connection
//! with and without the padding profile — does the padded distribution
//! converge to one peak? — together with (b) the same question for the
//! fitted ACK padding (standalone ACKs zero-filled to the observed
//! data-packet sizes) and (c) the throughput/latency overhead of both
//! under netem impairment.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p rtp --test rtp_padding_bench -- --ignored --nocapture --test-threads=1
//! ```

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use netem_test::kit::payload::{payload, with_timeout};
use netem_test::kit::presets::clean;
use netem_test::kit::stats::combined_stats;
use netem_test::{NetemPair, StdUdpTransport, UdpTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const KEY: [u8; 32] = [7; 32];
/// The random-mode padding settings under test: triangular draw over
/// `[1300, 1400]`, dynamic payload-sized (the length prefix rides in the
/// plaintext).
/// The fixed-profile policy under test: every datagram (data and ACK)
/// padded to exactly 1350 plaintext bytes (wire size 24 + 1350 = 1374).
const PROFILE: rtp::udp::HarmfulPaddingPolicy = rtp::udp::HarmfulPaddingPolicy::AllFixed(1350);

/// A transport wrapper that records every received datagram's size.
struct RecordingTransport {
    inner: Box<dyn UdpTransport>,
    sizes: Arc<Mutex<Vec<usize>>>,
}
impl UdpTransport for RecordingTransport {
    fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (n, from) = self.inner.recv_from(buf)?;
        self.sizes.lock().unwrap().push(n);
        Ok((n, from))
    }
    fn recv_from_timeout(
        &self,
        buf: &mut [u8],
        timeout: Duration,
    ) -> io::Result<(usize, SocketAddr)> {
        let (n, from) = self.inner.recv_from_timeout(buf, timeout)?;
        self.sizes.lock().unwrap().push(n);
        Ok((n, from))
    }
    fn send_to(&self, data: &[u8], dst: SocketAddr) -> io::Result<()> {
        self.inner.send_to(data, dst)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn set_recv_timeout(&self, timeout: Duration) -> io::Result<()> {
        self.inner.set_recv_timeout(timeout)
    }
    fn recv_timeout(&self) -> io::Result<Option<Duration>> {
        self.inner.recv_timeout()
    }
}

/// Spawn an obfuscated rtp echo server with the given DPI-hiding padding
/// policy.
async fn spawn_padded_echo_server(
    tx: &netem_test::kit::TestTaskSubmitter,
    policy: rtp::udp::HarmfulPaddingPolicy,
) -> std::io::Result<SocketAddr> {
    let listener = rtp::udp::Listener::bind(
        "127.0.0.1:0",
        rtp::udp::ListenerConfig {
            obfuscation_key: Some(KEY),
            padding: policy,
            ..rtp::udp::ListenerConfig::default()
        },
    )
    .await?;
    let addr = listener.local_addr();
    netem_test::kit::submit_test_task_required(
        tx,
        "padded rtp echo server",
        Box::pin(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept_without_handshake_with(rtp::udp::AcceptConfig {
                        obfuscation_key: Some(KEY),
                        padding: policy,
                        ..rtp::udp::AcceptConfig::default()
                    }) => {
                        let accepted = match accepted {
                            Ok(a) => a,
                            Err(_) => break,
                        };
                        handlers.spawn(async move {
                            let mut read = accepted.read.into_async_read();
                            let mut write = accepted.write.into_async_write();
                            let supervisor = accepted.supervisor;
                            tokio::pin!(supervisor);
                            let mut buf = vec![0u8; 8 * 1024];
                            loop {
                                tokio::select! {
                                    () = &mut supervisor => break,
                                    n = read.read(&mut buf) => {
                                        match n {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                if write.write_all(&buf[..n]).await.is_err() {
                                                    break;
                                                }
                                            }
                                            Err(_) => break,
                                        }
                                    }
                                }
                            }
                        });
                    }
                    _ = handlers.join_next() => {}
                }
            }
        }),
    );
    Ok(addr)
}

/// Run one transfer through a recording NetemPair and return the wire size
/// histogram and the transfer duration.
async fn run_transfer(
    policy: rtp::udp::HarmfulPaddingPolicy,
    transfer_bytes: usize,
) -> (std::collections::HashMap<usize, usize>, Duration) {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let server_addr = spawn_padded_echo_server(&task_tx, policy).await.unwrap();

            // Recording transports on both sides of the proxy capture the
            // wire datagram sizes.
            let sizes = Arc::new(Mutex::new(Vec::new()));
            let client_rec = Box::new(RecordingTransport {
                inner: Box::new(StdUdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap()),
                sizes: Arc::clone(&sizes),
            });
            let server_rec = Box::new(RecordingTransport {
                inner: Box::new(StdUdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap()),
                sizes: Arc::clone(&sizes),
            });
            let pair = NetemPair::spawn_with_transports(
                server_addr,
                clean(),
                clean(),
                client_rec,
                server_rec,
            )
            .unwrap();

            let connected = rtp::udp::connect_with(
                "0.0.0.0:0",
                &pair.client_addr().to_string(),
                rtp::udp::ConnectConfig {
                    handshake: false,
                    obfuscation_key: Some(KEY),
                    padding: policy,
                    ..rtp::udp::ConnectConfig::default()
                },
            )
            .await
            .unwrap();
            let mut read = connected.read.into_async_read();
            let mut write = connected.write.into_async_write();
            netem_test::kit::submit_test_task_required(
                &task_tx,
                "rtp client session",
                Box::pin(async move {
                    let _ = connected.supervisor.await;
                }),
            );

            let body = payload(transfer_bytes);
            let started = Instant::now();
            let got = with_timeout(Duration::from_secs(30), "padded transfer echo", async {
                write.write_all(&body).await.unwrap();
                let mut echoed = vec![0u8; transfer_bytes];
                read.read_exact(&mut echoed).await.unwrap();
                echoed
            })
            .await;
            assert_eq!(got, body, "the transfer must round-trip intact");
            let elapsed = started.elapsed();

            pair.stop();
            let _ = combined_stats(&pair);
            let histogram = {
                let mut h = std::collections::HashMap::new();
                for &n in sizes.lock().unwrap().iter() {
                    *h.entry(n).or_insert(0) += 1;
                }
                h
            };
            (histogram, elapsed)
        })
        .await
}

/// The padded wire size distribution must be one-peaked: the profile band
/// holds the overwhelming majority of datagrams, and the natural small
/// packet sizes are gone.
///
/// Default tier: the fixed profile pads every datagram to the same plaintext
/// size, so the measure is structural rather than timing-dependent. Over 5
/// runs at load average 4.2-6.4 it held 100% in-band with `small == 0`, and a
/// single 256 KiB transfer completes in well under a second.
#[tokio::test(flavor = "multi_thread")]
async fn padded_wire_sizes_converge_to_one_peak() {
    let (histogram, _) = run_transfer(PROFILE, 256 * 1024).await;

    let total: usize = histogram.values().sum();
    assert!(total > 0, "no wire datagrams captured");
    let in_band: usize = histogram
        .iter()
        .filter(|&(&n, _)| (1374..=1426).contains(&n))
        .map(|(_, &c)| c)
        .sum();
    let small: usize = histogram
        .iter()
        .filter(|&(&n, _)| n < 200)
        .map(|(_, &c)| c)
        .sum();
    println!(
        "padded: total={total} in_profile_band={in_band} ({:.1}%) small(<200)={small} distinct_sizes={}",
        100.0 * in_band as f64 / total as f64,
        histogram.len()
    );
    assert!(
        100.0 * in_band as f64 / total as f64 > 85.0,
        "the padded distribution must be one-peaked in the profile band, got {histogram:?}"
    );
    assert!(
        small == 0,
        "the natural small-packet peak must be gone, got {histogram:?}"
    );
}

/// The unpadded baseline keeps the natural multimodal distribution (small
/// control/ACK packets plus large data packets).
///
/// Default tier: the unpadded baseline always emits control/ACK datagrams
/// below 200 bytes; over 5 runs at load average 4.2-6.4 the small count was
/// 30-85 against the `> 0` bound. One 256 KiB transfer completes in well
/// under a second.
#[tokio::test(flavor = "multi_thread")]
async fn unpadded_wire_sizes_stay_multimodal() {
    let (histogram, _) = run_transfer(rtp::udp::HarmfulPaddingPolicy::None, 256 * 1024).await;

    let total: usize = histogram.values().sum();
    let small: usize = histogram
        .iter()
        .filter(|&(&n, _)| n < 200)
        .map(|(_, &c)| c)
        .sum();
    println!(
        "unpadded: total={total} small(<200)={small} ({:.1}%) distinct_sizes={}",
        100.0 * small as f64 / total as f64,
        histogram.len()
    );
    assert!(
        small > 0,
        "the unpadded baseline must keep small packets, got {histogram:?}"
    );
}

/// Fitted ACK padding hides the ACK packets among the data packets: the
/// tiny standalone-ACK cluster of the baseline disappears (or shrinks
/// dramatically) while the large data peak is preserved.
///
/// The standalone-ACK count is timing-dependent (an ACK flush whose fitted
/// data-size sample window has expired goes out unpadded by design), so the
/// shrink comparison pools several independent trials. Four trials were too
/// few: the pooled 256 KiB fitted/baseline small-cluster ratio reached 0.554
/// in earlier runs, and a 40-experiment release harness on this host still
/// reproduced a 1/40 failure rate (the audit saw ~1/25). Pooling 24 trials
/// concentrated the ratio enough that 0/60 experiments in the same harness
/// exceeded the < 0.5 bound under load average ~10, while the neutralized
/// policy (`AckMimicsData` -> `None`) failed 8/8 — so the bound still
/// separates fitted from unpadded. The 256 KiB transfer keeps the added
/// default-tier cost near two seconds.
const ACK_PADDING_TRIALS: usize = 24;

/// Runs in the default tier: the property is deterministic (fitted ACKs
/// mimic the data envelope) and the 24-trial pool both fits the default
/// budget and separates fitted from unpadded, so leaving it report-only or
/// `#[ignore]`d would have made the assertion unreachable.
#[tokio::test(flavor = "multi_thread")]
async fn ack_padding_hides_ack_packets_among_data() {
    let transfer_bytes = 256 * 1024;

    let small = |h: &std::collections::HashMap<usize, usize>| -> usize {
        h.iter().filter(|&(&n, _)| n < 200).map(|(_, &c)| c).sum()
    };
    let large = |h: &std::collections::HashMap<usize, usize>| -> usize {
        h.iter().filter(|&(&n, _)| n >= 200).map(|(_, &c)| c).sum()
    };

    let mut baseline_small = 0usize;
    let mut fitted_small = 0usize;
    let mut baseline_large = 0usize;
    let mut fitted_large = 0usize;
    for _ in 0..ACK_PADDING_TRIALS {
        let (baseline, _) =
            run_transfer(rtp::udp::HarmfulPaddingPolicy::None, transfer_bytes).await;
        let (fitted, _) = run_transfer(
            rtp::udp::HarmfulPaddingPolicy::AckMimicsData,
            transfer_bytes,
        )
        .await;
        baseline_small += small(&baseline);
        fitted_small += small(&fitted);
        baseline_large += large(&baseline);
        fitted_large += large(&fitted);
    }
    println!(
        "ack_padding: trials={ACK_PADDING_TRIALS} baseline_small={baseline_small} fitted_small={fitted_small} baseline_large={baseline_large} fitted_large={fitted_large}"
    );
    assert!(
        baseline_small > 0,
        "the baseline must keep a distinct small-datagram cluster, got {baseline_small}"
    );
    assert!(
        fitted_small * 2 < baseline_small,
        "the fitted run's small cluster must be dramatically shrunken: baseline={baseline_small} fitted={fitted_small}"
    );
    assert!(
        fitted_large * 10 >= baseline_large * 9,
        "the large-data peak must be preserved: baseline={baseline_large} fitted={fitted_large}"
    );
}

/// The throughput overhead of the fixed profile: padded vs unpadded
/// transfer time over a clean link.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp padding bench; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn padding_throughput_overhead() {
    let transfer_bytes = 512 * 1024;
    let (_, unpadded) = run_transfer(rtp::udp::HarmfulPaddingPolicy::None, transfer_bytes).await;
    let (_, padded) = run_transfer(PROFILE, transfer_bytes).await;

    let unpadded_mbps = transfer_bytes as f64 / unpadded.as_secs_f64() / 1e6;
    let padded_mbps = transfer_bytes as f64 / padded.as_secs_f64() / 1e6;
    println!(
        "throughput: unpadded={unpadded_mbps:.2} MB/s ({unpadded:?}) padded={padded_mbps:.2} MB/s ({padded:?}) overhead={:.1}%",
        100.0 * (padded.as_secs_f64() - unpadded.as_secs_f64()) / unpadded.as_secs_f64()
    );
}

/// Run a transfer through the given preset (both directions) and return the
/// transfer duration.
async fn run_transfer_preset(
    policy: rtp::udp::HarmfulPaddingPolicy,
    preset: netem_test::NetemConfig,
    transfer_bytes: usize,
) -> Duration {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let server_addr = spawn_padded_echo_server(&task_tx, policy).await.unwrap();
            let pair = NetemPair::spawn(server_addr, preset.clone(), preset).unwrap();
            let connected = rtp::udp::connect_with(
                "0.0.0.0:0",
                &pair.client_addr().to_string(),
                rtp::udp::ConnectConfig {
                    handshake: false,
                    obfuscation_key: Some(KEY),
                    padding: policy,
                    ..rtp::udp::ConnectConfig::default()
                },
            )
            .await
            .unwrap();
            let mut read = connected.read.into_async_read();
            let mut write = connected.write.into_async_write();
            netem_test::kit::submit_test_task_required(
                &task_tx,
                "rtp client session",
                Box::pin(async move {
                    let _ = connected.supervisor.await;
                }),
            );
            let body = payload(transfer_bytes);
            let started = Instant::now();
            let got = with_timeout(Duration::from_secs(60), "A/B transfer echo", async {
                write.write_all(&body).await.unwrap();
                let mut echoed = vec![0u8; transfer_bytes];
                read.read_exact(&mut echoed).await.unwrap();
                echoed
            })
            .await;
            assert_eq!(got, body, "the transfer must round-trip intact");
            let elapsed = started.elapsed();
            pair.stop();
            elapsed
        })
        .await
}

/// Run `count` small round-trip echoes through the given preset and return
/// the total time (the small-packet path is where the padding cost shows).
async fn run_small_echoes(
    policy: rtp::udp::HarmfulPaddingPolicy,
    preset: netem_test::NetemConfig,
    count: usize,
    size: usize,
) -> Duration {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let server_addr = spawn_padded_echo_server(&task_tx, policy).await.unwrap();
            let pair = NetemPair::spawn(server_addr, preset.clone(), preset).unwrap();
            let connected = rtp::udp::connect_with(
                "0.0.0.0:0",
                &pair.client_addr().to_string(),
                rtp::udp::ConnectConfig {
                    handshake: false,
                    obfuscation_key: Some(KEY),
                    padding: policy,
                    ..rtp::udp::ConnectConfig::default()
                },
            )
            .await
            .unwrap();
            let mut read = connected.read.into_async_read();
            let mut write = connected.write.into_async_write();
            netem_test::kit::submit_test_task_required(
                &task_tx,
                "rtp client session",
                Box::pin(async move {
                    let _ = connected.supervisor.await;
                }),
            );
            let body = payload(size);
            let started = Instant::now();
            for _ in 0..count {
                write.write_all(&body).await.unwrap();
                let mut echoed = vec![0u8; size];
                read.read_exact(&mut echoed).await.unwrap();
            }
            let elapsed = started.elapsed();
            pair.stop();
            elapsed
        })
        .await
}

/// A/B: bulk throughput under several presets, padded vs unpadded.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp padding bench; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn ab_bulk_throughput_across_presets() {
    let transfer_bytes = 4 * 1024 * 1024;
    let presets: Vec<(&str, netem_test::NetemConfig)> = vec![
        ("clean", clean()),
        ("latency20ms", netem_test::kit::presets::latency(20)),
        (
            "lossy400kibps",
            netem_test::kit::presets::lossy_400kib_per_sec(),
        ),
    ];
    println!("A/B bulk throughput ({transfer_bytes} bytes):");
    for (name, preset) in presets {
        let unpadded = run_transfer_preset(
            rtp::udp::HarmfulPaddingPolicy::None,
            preset.clone(),
            transfer_bytes,
        )
        .await;
        let padded = run_transfer_preset(PROFILE, preset, transfer_bytes).await;
        let unpadded_mbps = transfer_bytes as f64 / unpadded.as_secs_f64() / 1e6;
        let padded_mbps = transfer_bytes as f64 / padded.as_secs_f64() / 1e6;
        println!(
            "  {name:<14} unpadded={unpadded_mbps:7.2} MB/s ({unpadded:?}) padded={padded_mbps:7.2} MB/s ({padded:?}) delta={:.1}%",
            100.0 * (padded_mbps - unpadded_mbps) / unpadded_mbps
        );
    }
}

/// A/B: bulk throughput, fitted ACK padding off vs on (no profile), under
/// the same three presets.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp padding bench; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn ab_bulk_throughput_ack_padding() {
    let transfer_bytes = 4 * 1024 * 1024;
    let presets: Vec<(&str, netem_test::NetemConfig)> = vec![
        ("clean", clean()),
        ("latency20ms", netem_test::kit::presets::latency(20)),
        (
            "lossy400kibps",
            netem_test::kit::presets::lossy_400kib_per_sec(),
        ),
    ];
    println!("A/B bulk throughput ack_padding ({transfer_bytes} bytes):");
    for (name, preset) in presets {
        let off = run_transfer_preset(
            rtp::udp::HarmfulPaddingPolicy::None,
            preset.clone(),
            transfer_bytes,
        )
        .await;
        let on = run_transfer_preset(
            rtp::udp::HarmfulPaddingPolicy::AckMimicsData,
            preset,
            transfer_bytes,
        )
        .await;
        let off_mbps = transfer_bytes as f64 / off.as_secs_f64() / 1e6;
        let on_mbps = transfer_bytes as f64 / on.as_secs_f64() / 1e6;
        println!(
            "  {name:<14} ack_padding=off {off_mbps:7.2} MB/s ({off:?}) ack_padding=on {on_mbps:7.2} MB/s ({on:?}) delta={:.1}%",
            100.0 * (on_mbps - off_mbps) / off_mbps
        );
    }
}

/// A/B: small round-trip latency, padded vs unpadded. The padding pads each
/// small datagram to the profile band, so the small-path cost is visible.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp padding bench; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn ab_small_echo_latency() {
    let count = 200;
    let size = 64;
    let unpadded =
        run_small_echoes(rtp::udp::HarmfulPaddingPolicy::None, clean(), count, size).await;
    let padded = run_small_echoes(PROFILE, clean(), count, size).await;
    let unpadded_per = unpadded.as_secs_f64() / count as f64;
    let padded_per = padded.as_secs_f64() / count as f64;
    println!(
        "A/B small echo ({count} x {size}B): unpadded={:.3} s total ({:.3} ms/echo) padded={:.3} s total ({:.3} ms/echo) slowdown={:.1}x",
        unpadded.as_secs_f64(),
        unpadded_per * 1000.0,
        padded.as_secs_f64(),
        padded_per * 1000.0,
        padded.as_secs_f64() / unpadded.as_secs_f64()
    );
}

/// A/B: small round-trip latency, fitted ACK padding off vs on, on a clean
/// link.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp padding bench; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn ab_small_echo_latency_ack_padding() {
    let count = 200;
    let size = 64;
    let off = run_small_echoes(rtp::udp::HarmfulPaddingPolicy::None, clean(), count, size).await;
    let on = run_small_echoes(
        rtp::udp::HarmfulPaddingPolicy::AckMimicsData,
        clean(),
        count,
        size,
    )
    .await;
    let off_per = off.as_secs_f64() / count as f64;
    let on_per = on.as_secs_f64() / count as f64;
    println!(
        "A/B small echo ack_padding ({count} x {size}B): off={:.3} s total ({:.3} ms/echo) on={:.3} s total ({:.3} ms/echo) slowdown={:.1}x",
        off.as_secs_f64(),
        off_per * 1000.0,
        on.as_secs_f64(),
        on_per * 1000.0,
        on.as_secs_f64() / off.as_secs_f64()
    );
}

/// A/B: small round-trip latency under latency-injected presets, padded vs
/// unpadded. The padding's per-datagram cost is roughly constant, so its
/// absolute delta should persist while its relative share shrinks as the
/// base RTT grows.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "rtp padding bench; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn ab_small_echo_latency_across_presets() {
    let count = 50;
    let size = 64;
    let presets: Vec<(&str, netem_test::NetemConfig)> = vec![
        ("latency20ms", netem_test::kit::presets::latency(20)),
        ("latency50ms", netem_test::kit::presets::latency(50)),
        ("latency100ms", netem_test::kit::presets::latency(100)),
    ];
    println!("A/B small echo ({count} x {size}B) under injected latency:");
    for (name, preset) in presets {
        let unpadded = run_small_echoes(
            rtp::udp::HarmfulPaddingPolicy::None,
            preset.clone(),
            count,
            size,
        )
        .await;
        let padded = run_small_echoes(PROFILE, preset, count, size).await;
        let unpadded_per = unpadded.as_secs_f64() / count as f64;
        let padded_per = padded.as_secs_f64() / count as f64;
        println!(
            "  {name:<14} unpadded={:.3} ms/echo padded={:.3} ms/echo delta={:+.3} ms ({:.1}x)",
            unpadded_per * 1000.0,
            padded_per * 1000.0,
            (padded_per - unpadded_per) * 1000.0,
            padded.as_secs_f64() / unpadded.as_secs_f64()
        );
    }
}
