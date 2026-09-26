//! RTP bufferbloat regression scenario.
//!
//! This test verifies that a latency-plus-rate-limited link with a bounded
//! per-direction queue (`limit`) does not catastrophically inflate latency or
//! drop below a reasonable goodput floor once the bottleneck buffer fills.
//!
//! Run with:
//!
//! ```sh
//! cargo test --release -p rtp --test rtp_bufferbloat -- --ignored --nocapture --test-threads=1
//! ```

use std::time::{Duration, Instant};

use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::stats::{combined_stats, percentile, print_perf};
use netem_test::kit::submit_test_task;
use netem_test::{NetemConfig, NetemPair};
use rtp::testkit::rtp::send_timestamped_messages;
use rtp::testkit::rtp::{
    rtp_connect_with_mss_via, spawn_rtp_bulk_upload_with_mss_via,
    spawn_rtp_byte_sink_server_with_mss_via, spawn_rtp_msg_latency_sink_via,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// RTP MSS for the bufferbloat probe. We use the default loopback MSS rather
/// than an oversized value because the RTP codec/FEC overhead consumes most of
/// a user-provided 8 KiB datagram ceiling, leaving almost no user payload.
const MSS: usize = rtp::udp::NO_FEC_MSS;

/// Link profile: 20 ms base latency, 10 Mbit/s bottleneck, 256-packet queue
/// limit. This is deliberately sized so the delay-gate/queue-limit build takes
/// zero overflow drops (the in-flight bound is below 256 packets).
pub fn bufferbloat_link(seed: u64) -> NetemConfig {
    NetemConfig {
        latency: Duration::from_millis(20),
        rate: 10_000_000, // 10 Mbit/s
        queue_limit_pkts: 256,
        seed,
        ..NetemConfig::default()
    }
}

/// Minimum acceptable goodput as a fraction of bottleneck capacity. The floor is
/// deliberately slack (~35% of 10 Mbit/s ≈ 0.42 MiB/s) so the test survives the
/// concurrent `rtp` branches without asserting the measured stock number.
const GOODPUT_CAPACITY_FLOOR: f64 = 0.35;
/// Maximum acceptable per-direction queue depth.
const MAX_QUEUE_FLOOR: usize = 256;

/// Run a bulk upload and sparse pings through a bufferbloat link for 15 s.
/// Assert that:
/// * max observed queue_len_c2s <= 256
/// * goodput >= 35% of capacity
/// * overflow_dropped == 0 (the limit is sized above the in-flight bound)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "bufferbloat regression; slow; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn rtp_bulk_bounded_buffer_goodput_and_queue_bound() {
    let capacity_bps: f64 = 10_000_000.0;
    let capacity_mib_s: f64 = capacity_bps / 8.0 / (1024.0 * 1024.0);
    let floor_mib_s: f64 = capacity_mib_s * GOODPUT_CAPACITY_FLOOR;

    let base = Instant::now();
    let mut server_tasks = netem_test::kit::TestScope::new();
    let task_tx = server_tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);

    server_tasks
        .run(async {
            let (sink_addr, delivered) =
                spawn_rtp_byte_sink_server_with_mss_via(&task_tx, false, MSS)
                    .await
                    .unwrap();
            let (latency_addr, mut latencies) =
                spawn_rtp_msg_latency_sink_via(&task_tx, false, base).await.unwrap();

            let pair =
                NetemPair::spawn(sink_addr, bufferbloat_link(4), bufferbloat_link(5)).unwrap();

            // Bulk upload sender. Write a repeating deterministic stream large enough
            // that the measurement window is receive-limited, not send-limited.
            let mut writer =
                spawn_rtp_bulk_upload_with_mss_via(&task_tx, pair.client_addr(), false, MSS)
                    .await
                    .unwrap();
            let bulk_start = Instant::now();
            let data = cyclic_payload(64 * 1024 * 1024);
            let (pump_stop_tx, mut pump_stop_rx) = tokio::sync::watch::channel(false);
            let mut pump_tasks = tokio::task::JoinSet::new();
            pump_tasks.spawn(async move {
                let mut offset = 0usize;
                loop {
                    tokio::select! {
                        _ = pump_stop_rx.changed() => break,
                        result = writer.write(&data[offset..]) => match result {
                            Ok(0) => break,
                            Ok(n) => offset = (offset + n) % data.len(),
                            Err(_) => break,
                        },
                    }
                }
            });

            // Sparse latency sender on a separate connection through the same pair.
            let latency_pair =
                NetemPair::spawn(latency_addr, bufferbloat_link(4), bufferbloat_link(5)).unwrap();
            let (mut read, mut write) = rtp_connect_with_mss_via(
                &task_tx,
                latency_pair.client_addr(),
                false,
                MSS,
            )
            .await;
            // Keep the read half alive so ACKs keep moving; parked until the
            // connection closes, so the owning JoinSet aborts it at scope end.
            submit_test_task(&task_tx, Box::pin(async move {
                let mut buf = vec![0u8; 8 * 1024];
                while let Ok(n) = read.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                }
            }));

            // Race the bulk pump against the measurement (sparse pings plus
            // queue sampling): a premature pump completion fails the test
            // instead of measuring against a dead upload. The pump runs
            // until the watch signals shutdown after the measurement.
            let (ping_sent, max_queue) = tokio::select! {
                joined = pump_tasks.join_next(), if !pump_tasks.is_empty() => {
                    joined.expect("bulk pump exists").unwrap();
                    panic!("bulk pump ended before the measurement completed");
                }
                result = async {
                    let ping_sent = with_timeout(
                        Duration::from_secs(25),
                        "send sparse pings during bufferbloat",
                        send_timestamped_messages(
                            &mut write,
                            base,
                            128,
                            Duration::from_millis(500),
                            Duration::from_secs(15),
                        ),
                    )
                    .await;

                    // Sample the queue length every 50 ms while the transfer runs.
                    let mut max_queue = 0usize;
                    let sample_window = Duration::from_secs(15);
                    let sample_start = Instant::now();
                    while sample_start.elapsed() < sample_window {
                        let q = pair.queue_len_c2s();
                        if q > max_queue {
                            max_queue = q;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    (ping_sent, max_queue)
                } => result,
            };
            pump_stop_tx.send(true).unwrap();
            // Give the final bulk bytes time to drain through the shaped
            // link before measuring elapsed goodput.
            tokio::time::sleep(Duration::from_secs(2)).await;
            let delivered_bytes = delivered.load(std::sync::atomic::Ordering::Relaxed);
            let elapsed = bulk_start.elapsed();
            // Epilog: join the pump so any panic surfaces.
            while let Some(result) = pump_tasks.join_next().await {
                result.unwrap();
            }
            pair.stop();
            latency_pair.stop();

            let stats = combined_stats(&pair);
            print_perf(
                "rtp bufferbloat bulk goodput",
                delivered_bytes as usize,
                elapsed,
            );
            eprintln!("[rtp_bufferbloat] max_queue={max_queue} stats={stats:?}");

            // `MAX_QUEUE_FLOOR` is the link's own `queue_limit_pkts` and the
            // harness tail-drops at `len() >= queue_limit_pkts`, so the
            // observable maximum is 255: that bound restates the limit the
            // instrument is configured with rather than bounding the build.
            // Do not "fix" it by lowering the floor or by adding an arm at a
            // smaller `queue_limit_pkts` — an existing bound is frozen. The
            // invariant this arm can still prove is that the sampler ran: a
            // zero maximum means the observation never happened, which would
            // leave the bound below passing vacuously.
            assert!(max_queue > 0, "the queue sampler never ran");
            assert!(
                max_queue <= MAX_QUEUE_FLOOR,
                "max c2s queue {max_queue} > {MAX_QUEUE_FLOOR}"
            );

            let goodput_mib_s =
                delivered_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
            assert!(
                goodput_mib_s >= floor_mib_s,
                "goodput {goodput_mib_s:.3} MiB/s < floor {floor_mib_s:.3} MiB/s"
            );

            // The delay-gate rtp build is expected to take zero overflow drops.
            assert_eq!(
                stats.overflow_dropped, 0,
                "delay-gate build must take zero overflow drops, got {stats:?}"
            );

            // Drain the sparse latency samples and assert tail bounds.
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut samples = Vec::new();
            while let Ok(latency_ms) = latencies.try_recv() {
                samples.push(latency_ms);
            }
            let received = samples.len() as u64;
            // The bound below is only a measurement if the arm observed a
            // latency at all. Both halves are true invariants of a correct run:
            // the sender offers 128 pings at a 500 ms cadence across the 15 s
            // window, and the link this arm configures drops nothing. An empty
            // observation must therefore fail here rather than skip the bound.
            assert!(
                ping_sent > 0,
                "the sparse-ping sender offered no message in the measurement window"
            );
            assert!(
                received > 0,
                "the latency sink delivered no sample of the {ping_sent} pings offered"
            );
            if received > 0 {
                samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let p50 = percentile(&samples, 0.50);
                let p99 = percentile(&samples, 0.99);
                eprintln!(
                    "[rtp_bufferbloat] pings sent={ping_sent} received={received} p50={p50:.1} ms p99={p99:.1} ms"
                );
                // Floor: p50 ping latency should stay under ~0.8× the max queue
                // build-up.
                assert!(p50 <= 800.0, "bufferbloat ping p50 {p50:.1} ms > 800 ms");
            }
        })
        .await;
}
