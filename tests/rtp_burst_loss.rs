//! RTP regression scenarios for bulk goodput under packet loss and
//! sparse-message tail latency.
//!
//! Two independent bulk-goodput shapes live here because they fail for
//! different reasons: the burst-vs-random comparison is a *relative* floor
//! between two impaired arms (a change that slows both arms equally moves the
//! ratio not at all, so it is blind to a common-mode goodput collapse), and the
//! iid-loss-vs-loss-free comparison anchors the loss lane against the same
//! topology with loss disabled (the capacity-relative shape of "the bulk lane
//! keeps a high fraction of the link's capacity").
//!
//! These tests are `#[ignore]`-d by default so they do not slow normal builds.
//! Run them with:
//!
//! ```sh
//! cargo test --release -p rtp --test rtp_burst_loss -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::presets::{
    burst_loss_link, controller_fat_pipe, deterministic_iid_loss_fat_pipe, random_loss_link,
};
use netem_test::kit::stats::{percentile, print_perf};
use netem_test::kit::submit_test_task;
use netem_test::{NetemConfig, NetemPair};
use rtp::testkit::rtp::{
    rtp_connect_with_mss_via, send_timestamped_messages, spawn_rtp_bulk_upload_with_mss_via,
    spawn_rtp_byte_sink_server_with_mss_via, spawn_rtp_msg_latency_sink_via,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// OWD used by both burst-loss tests so the comparison is apples-to-apples.
const OWD: Duration = Duration::from_millis(50);
/// RTP MSS for the burst-loss probes. Use the default loopback MSS so the
/// codec/FEC overhead leaves a reasonable user payload per packet.
const MSS: usize = rtp::udp::NO_FEC_MSS;

/// Number of seconds the bulk-goodput probe keeps the sender busy.
const BULK_WINDOW_S: u64 = 12;
const BULK_REPETITIONS: usize = 6;
/// Minimum anti-collapse floor: burst loss must not collapse relative to
/// independent random loss of the same average rate.
///
/// The 12 s burst-arm goodput is intrinsically bursty and CPU-sensitive (an
/// RTO stall, or a CPU-starved retransmission episode, can halve a window),
/// and under a fully loaded host the *correct* implementation's aggregate
/// burst/random ratio was measured in the 0.78–0.97 band (12 CPU burners on a
/// 10-core host). The historical 0.90 threshold sat inside that band, so it
/// failed on host scheduling rather than on a regression. A genuine
/// burst-recovery collapse drives the aggregate far below this floor, so 0.60
/// keeps the anti-collapse detection power while tolerating CPU contention.
const MIN_BURST_VS_RANDOM_RATIO: f64 = 0.60;
/// Absolute anti-collapse floor for the burst-loss arm: it must also make real
/// progress, because the ratio alone is blind to both arms collapsing together.
const BURST_GOODPUT_STALL_FLOOR_MIB_S: f64 = 1.0;
/// Absolute anti-stall floor for the random-loss baseline. The ratio assertion
/// above passes even when both 12 s runs stall near zero, so the random-loss
/// baseline must also make real progress.
const RANDOM_GOODPUT_STALL_FLOOR_MIB_S: f64 = 1.0;

// ── iid-loss-vs-loss-free fat-pipe goodput ────────────────────────────────
//
// The impaired/loss-free pair reproduces the perf battery's
// `deterministic-iid-loss-fat-pipe` lane and its `controller-fat-pipe`
// reference arm: same 100 Mbit/s rate, 150 ms OWD, 16k-packet queue, 8192-byte
// MSS, differing only in the seeded ~1 % independent per-packet loss. Both
// arms run on the default (`Shared`) congestion lane, the intent a raw `rtp`
// bulk flow declares without a mux.

/// MSS used by the fat-pipe pair, matching the perf battery's `--mss-bytes
/// 8192`.
const FAT_PIPE_MSS: usize = 8192;
/// Warmup excluded from the measured window.
///
/// The measured quantity is a steady-state ratio between the two arms, so the
/// window must exclude the `Shared` controller's *convergence*, not just slow
/// start: at a 20 s warmup the per-rep ratio still carried the ramp's own
/// arm-to-arm spread (0.891-1.043 across reps at the stock tree), while at
/// 40 s the measured window is steady state and the same spread collapsed to
/// ~0.97-1.04 across runs. That stability is what lets the relative floor be
/// tight enough to catch a double-digit percentage collapse.
const FAT_PIPE_WARMUP_S: u64 = 40;
/// Measured window, over which the two arms' verified payload bytes are
/// counted.
const FAT_PIPE_WINDOW_S: u64 = 20;
/// Number of seeded reps pooled into the aggregate ratio; each rep reseeds
/// both directions so the deterministic drop pattern is not one lucky draw.
const FAT_PIPE_REPETITIONS: usize = 3;
/// Base seed for rep 0 (`seed` c2s, `seed + 1` s2c, exactly how the perf
/// battery seeds the two directions); rep `r` uses `base + r`.
const FAT_PIPE_SEED_BASE: u64 = 4;
/// Seed stride between reps: consecutive seeds vary the drop pattern.
const FAT_PIPE_SEED_STRIDE: u64 = 1;
/// Minimum iid-loss goodput as a fraction of the concurrently measured
/// loss-free arm on the identical topology.
///
/// Measured on the stock tree (3 reps x 20 s after the 40 s warmup), two
/// runs: aggregate 0.985 and 1.017, per-rep 0.972-1.035, so the pooled-rep
/// spread is ~3 ratio points end to end. The injected fast-loss re-fire (a
/// retransmitted packet stays fast-loss-eligible, so the ACK-path sync re-arms
/// it on every pass) collapsed the aggregate to 0.775, per-rep
/// 0.903/0.686/0.735, and this floor fails it. 0.85 therefore sits ~13 points
/// below the stock aggregate band and ~7 points above the injection, several
/// times the measured noise in either direction: neither host scheduling nor
/// one unlucky drop pattern can flip the verdict. The floor is relative, so
/// host speed cancels between the two arms; the per-arm anti-stall floors
/// below stop two collapsed arms from satisfying it.
const MIN_IID_LOSS_VS_LOSS_FREE_RATIO: f64 = 0.85;
/// Absolute anti-collapse floor for the iid-loss arm: the ratio alone is blind
/// to both arms collapsing together.
const IID_LOSS_GOODPUT_STALL_FLOOR_MIB_S: f64 = 1.0;
/// Absolute anti-stall floor for the loss-free reference arm.
const LOSS_FREE_GOODPUT_STALL_FLOOR_MIB_S: f64 = 1.0;

/// Number of messages sent by the sparse-message tail-latency probe.
const SPARSE_MSG_COUNT: u64 = 200;
/// Interval between sparse messages.
const SPARSE_MSG_INTERVAL: Duration = Duration::from_millis(300);
/// Message size used by the sparse-message probe.
const SPARSE_MSG_BYTES: usize = 64;
/// Floor: at least 98% of sparse messages must be delivered.
const SPARSE_DELIVERY_FLOOR_PCT: f64 = 0.98;
/// Floor: p99 one-way latency under burst loss must stay below ~2.5 s.
const SPARSE_P99_LATENCY_MS: f64 = 2500.0;
/// Floor: p50 one-way latency under burst loss must stay below ~300 ms. The
/// median is not RTO-quantized; stock behaviour is ~55 ms at this test's 100 ms
/// RTT (OWD = 50 ms), so this leaves a generous allowance.
const SPARSE_P50_LATENCY_MS: f64 = 300.0;

/// Burst-loss profile: long-term 5% loss, mean burst length 8.
const BURST_LOSS_PCT: f64 = 5.0;
const BURST_LOSS_MEAN_LEN: f64 = 8.0;

/// Random-loss profile: same average 5% loss.
const RANDOM_LOSS_PCT: f64 = 5.0;

/// Bulk goodput through a burst-loss link should not collapse below the
/// equivalent independent random loss link.
///
/// The burst link uses a Gilbert-Elliott model with 5% long-term loss and an
/// average burst length of 8. The random link uses the same average rate. The
/// burst-loss arm's 12 s goodput is intrinsically bursty — a single RTO stall
/// can halve one window regardless of host load — so a single burst-vs-random
/// comparison is dominated by that noise. Both arms run **concurrently** over
/// the same 12-second window (so a fluctuating host load cannot favour
/// whichever arm happens to run during a busy interval), and the delivered
/// bytes are pooled across `BULK_REPETITIONS` seeded reps into an *aggregate*
/// ratio. The aggregate averages over many stall/non-stall episodes, so neither
/// host scheduling nor one unlucky episode can flip the verdict, while a
/// genuine burst-loss regression depresses every rep and therefore the
/// aggregate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "burst-loss goodput/tail-latency regression; slow; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn rtp_bulk_goodput_burst_loss_does_not_collapse_vs_random() {
    let window = Duration::from_secs(BULK_WINDOW_S);
    let data: &'static [u8] = Box::leak(cyclic_payload(256 * 1024 * 1024).into_boxed_slice());

    let mut burst_total: u64 = 0;
    let mut random_total: u64 = 0;
    for rep in 0..BULK_REPETITIONS {
        let seed_offset = rep * 100;
        // Same-window measurement: the two arms run concurrently so the
        // burst/random ratio is not confounded by host-load drift between two
        // sequential 12 s windows, and host pressure is common to both arms.
        let burst_fut = run_rtp_sink_upload(
            burst_loss_link(
                BURST_LOSS_PCT,
                BURST_LOSS_MEAN_LEN,
                OWD,
                (11 + seed_offset) as u64,
            ),
            burst_loss_link(
                BURST_LOSS_PCT,
                BURST_LOSS_MEAN_LEN,
                OWD,
                (22 + seed_offset) as u64,
            ),
            data,
            window,
        );
        let random_fut = run_rtp_sink_upload(
            random_loss_link(RANDOM_LOSS_PCT, OWD, (33 + seed_offset) as u64),
            random_loss_link(RANDOM_LOSS_PCT, OWD, (44 + seed_offset) as u64),
            data,
            window,
        );
        let ((burst_pair, burst_delivered), (random_pair, random_delivered)) =
            tokio::join!(burst_fut, random_fut);

        let burst_goodput = burst_delivered as f64 / (1024.0 * 1024.0) / BULK_WINDOW_S as f64;
        let random_goodput = random_delivered as f64 / (1024.0 * 1024.0) / BULK_WINDOW_S as f64;
        print_perf(
            &format!("rtp bulk goodput burst rep {rep}"),
            burst_delivered as usize,
            window,
        );
        print_perf(
            &format!("rtp bulk goodput random rep {rep}"),
            random_delivered as usize,
            window,
        );

        assert!(
            random_goodput >= RANDOM_GOODPUT_STALL_FLOOR_MIB_S,
            "random-loss baseline stalled: {random_goodput:.3} MiB/s < {RANDOM_GOODPUT_STALL_FLOOR_MIB_S} MiB/s"
        );

        let ratio = if random_goodput > 0.0 {
            burst_goodput / random_goodput
        } else if burst_goodput > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
        eprintln!("[rtp_burst_loss] rep {rep} burst/random ratio = {ratio:.3}");
        burst_total += burst_delivered;
        random_total += random_delivered;
        burst_pair.stop();
        random_pair.stop();
    }
    let reps = BULK_REPETITIONS as f64;
    let aggregate_burst_goodput =
        burst_total as f64 / (1024.0 * 1024.0) / (BULK_WINDOW_S as f64 * reps);
    let aggregate_random_goodput =
        random_total as f64 / (1024.0 * 1024.0) / (BULK_WINDOW_S as f64 * reps);
    let aggregate_ratio = if random_total > 0 {
        burst_total as f64 / random_total as f64
    } else if burst_total > 0 {
        f64::INFINITY
    } else {
        0.0
    };
    eprintln!(
        "[rtp_burst_loss] aggregate burst={aggregate_burst_goodput:.3} MiB/s \
         random={aggregate_random_goodput:.3} MiB/s ratio (N={BULK_REPETITIONS}) = {aggregate_ratio:.3}"
    );
    assert!(
        aggregate_burst_goodput >= BURST_GOODPUT_STALL_FLOOR_MIB_S,
        "burst-loss arm collapsed: {aggregate_burst_goodput:.3} MiB/s < {BURST_GOODPUT_STALL_FLOOR_MIB_S} MiB/s"
    );
    assert!(
        aggregate_ratio >= MIN_BURST_VS_RANDOM_RATIO,
        "aggregate burst/random ratio {aggregate_ratio:.3} < {MIN_BURST_VS_RANDOM_RATIO}"
    );
}

/// Bulk goodput under deterministic iid loss must keep a high fraction of the
/// same topology's loss-free goodput.
///
/// This is the assertion `rtp_burst_loss::rtp_bulk_goodput_burst_loss_does_not_
/// collapse_vs_random` structurally cannot make: its two arms are *both*
/// impaired, so a change that slows every loss recovery equally leaves the
/// burst/random ratio at ~1.0 and its 1 MiB/s anti-stall floors far below the
/// ~11 MiB/s the lane actually delivers. A fast-loss re-fire for an
/// already-retransmitted packet is exactly that shape: the impaired arm drops
/// while the loss-free arm cannot move, so the ratio is the measurement that
/// sees it (the burst/random ratio stays green).
///
/// The two arms run concurrently on identical topology (100 Mbit/s, 150 ms
/// OWD, 16k-packet queue, 8192-byte MSS) and differ only in the seeded ~1 %
/// iid loss `deterministic_iid_loss_fat_pipe` adds, so the ratio is a
/// host-cancelling measurement. Each rep reseeds both directions, the measured
/// window excludes the controller's convergence, and the aggregate pools
/// `FAT_PIPE_REPETITIONS` reps, so neither one unlucky drop pattern nor an
/// interval of host load can flip the verdict.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "iid-loss vs loss-free bulk goodput regression; slow; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn rtp_bulk_goodput_under_iid_loss_keeps_a_high_fraction_of_the_loss_free_pipe() {
    let warmup = Duration::from_secs(FAT_PIPE_WARMUP_S);
    let window = Duration::from_secs(FAT_PIPE_WINDOW_S);
    let data: &'static [u8] = Box::leak(cyclic_payload(256 * 1024 * 1024).into_boxed_slice());

    let mut loss_total: u64 = 0;
    let mut loss_free_total: u64 = 0;
    for rep in 0..FAT_PIPE_REPETITIONS {
        let seed = FAT_PIPE_SEED_BASE + rep as u64 * FAT_PIPE_SEED_STRIDE;
        // Same-window measurement: the two arms run concurrently so the ratio
        // is not confounded by host-load drift between two sequential windows,
        // and host pressure is common to both arms.
        let loss_fut = run_rtp_sink_upload_windowed(
            iid_loss_fat_pipe(seed),
            iid_loss_fat_pipe(seed + 1),
            data,
            warmup,
            window,
        );
        let loss_free_fut = run_rtp_sink_upload_windowed(
            loss_free_fat_pipe(seed),
            loss_free_fat_pipe(seed + 1),
            data,
            warmup,
            window,
        );
        let ((loss_pair, loss_delivered), (loss_free_pair, loss_free_delivered)) =
            tokio::join!(loss_fut, loss_free_fut);

        let loss_goodput = mib_per_s(loss_delivered, window);
        let loss_free_goodput = mib_per_s(loss_free_delivered, window);
        eprintln!(
            "[rtp_iid_loss] rep {rep} seed={seed} iid-loss={loss_goodput:.3} MiB/s \
             loss-free={loss_free_goodput:.3} MiB/s"
        );

        assert!(
            loss_goodput >= IID_LOSS_GOODPUT_STALL_FLOOR_MIB_S,
            "iid-loss arm stalled: {loss_goodput:.3} MiB/s < {IID_LOSS_GOODPUT_STALL_FLOOR_MIB_S} MiB/s"
        );
        assert!(
            loss_free_goodput >= LOSS_FREE_GOODPUT_STALL_FLOOR_MIB_S,
            "loss-free reference arm stalled: {loss_free_goodput:.3} MiB/s < {LOSS_FREE_GOODPUT_STALL_FLOOR_MIB_S} MiB/s"
        );

        loss_total += loss_delivered;
        loss_free_total += loss_free_delivered;
        loss_pair.stop();
        loss_free_pair.stop();
    }

    let aggregate_loss_goodput = mib_per_s(loss_total, window * FAT_PIPE_REPETITIONS as u32);
    let aggregate_loss_free_goodput =
        mib_per_s(loss_free_total, window * FAT_PIPE_REPETITIONS as u32);
    let ratio = if loss_free_total > 0 {
        loss_total as f64 / loss_free_total as f64
    } else if loss_total > 0 {
        f64::INFINITY
    } else {
        0.0
    };
    eprintln!(
        "[rtp_iid_loss] aggregate iid-loss={aggregate_loss_goodput:.3} MiB/s \
         loss-free={aggregate_loss_free_goodput:.3} MiB/s ratio (N={FAT_PIPE_REPETITIONS}) = {ratio:.3}"
    );
    assert!(
        aggregate_loss_goodput >= IID_LOSS_GOODPUT_STALL_FLOOR_MIB_S,
        "iid-loss arm collapsed: {aggregate_loss_goodput:.3} MiB/s < {IID_LOSS_GOODPUT_STALL_FLOOR_MIB_S} MiB/s"
    );
    assert!(
        aggregate_loss_free_goodput >= LOSS_FREE_GOODPUT_STALL_FLOOR_MIB_S,
        "loss-free reference arm collapsed: {aggregate_loss_free_goodput:.3} MiB/s < {LOSS_FREE_GOODPUT_STALL_FLOOR_MIB_S} MiB/s"
    );
    assert!(
        ratio >= MIN_IID_LOSS_VS_LOSS_FREE_RATIO,
        "iid-loss bulk goodput is only {ratio:.3} of the loss-free arm on the same 100 Mbit/s topology \
         (aggregate iid-loss={aggregate_loss_goodput:.3} MiB/s, loss-free={aggregate_loss_free_goodput:.3} MiB/s, \
         N={FAT_PIPE_REPETITIONS}); below {MIN_IID_LOSS_VS_LOSS_FREE_RATIO}: the bulk lane does not keep a high \
         fraction of the link's capacity under loss"
    );
}

/// The perf battery's `deterministic-iid-loss-fat-pipe` lane with an explicit
/// seed, so a rep can vary the drop pattern the way the battery's
/// `NETEM_PERF_SEED` does.
fn iid_loss_fat_pipe(seed: u64) -> NetemConfig {
    NetemConfig {
        seed,
        ..deterministic_iid_loss_fat_pipe()
    }
}

/// The same 100 Mbit/s / 150 ms / 16k-queue fat pipe with loss disabled — the
/// perf battery's `controller-fat-pipe` reference arm.
fn loss_free_fat_pipe(seed: u64) -> NetemConfig {
    NetemConfig {
        seed,
        ..controller_fat_pipe()
    }
}

/// Verified payload MiB/s over `window`.
fn mib_per_s(bytes: u64, window: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / window.as_secs_f64()
}

/// Run one direction of the byte-sink upload and return the pair plus the
/// number of verified payload bytes delivered by the server.
async fn run_rtp_sink_upload(
    c2s: NetemConfig,
    s2c: NetemConfig,
    data: &'static [u8],
    window: Duration,
) -> (NetemPair, u64) {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (server_addr, delivered) =
                spawn_rtp_byte_sink_server_with_mss_via(&task_tx, false, MSS)
                    .await
                    .unwrap();
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();
            let mut writer =
                spawn_rtp_bulk_upload_with_mss_via(&task_tx, pair.client_addr(), false, MSS)
                    .await
                    .unwrap();

            let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
            let mut pump_tasks = tokio::task::JoinSet::new();
            pump_tasks.spawn(async move {
                let mut offset = 0usize;
                loop {
                    tokio::select! {
                        _ = stop_rx.changed() => break,
                        result = writer.write(&data[offset..]) => match result {
                            Ok(0) => break,
                            Ok(n) => offset = (offset + n) % data.len(),
                            Err(_) => break,
                        },
                    }
                }
            });

            let d = tokio::select! {
                joined = pump_tasks.join_next(), if !pump_tasks.is_empty() => {
                    // The pump ended before the measurement window completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("pump task exists").unwrap();
                    panic!("bulk pump ended before the measurement window completed");
                }
                _ = tokio::time::sleep(window) => {
                    let d = delivered.load(Ordering::Relaxed);
                    stop_tx.send(true).unwrap();
                    // Epilog: join the pump so any panic surfaces.
                    while let Some(result) = pump_tasks.join_next().await {
                        result.unwrap();
                    }
                    d
                }
            };
            (pair, d)
        })
        .await
}

/// Run one direction of the byte-sink upload for `warmup + window`, returning
/// the pair plus the payload bytes the sink *verified* in the measured window
/// alone. Excluding the warmup keeps slow start (the ~3.75 MB bandwidth-delay
/// product of the 100 Mbit/s x 300 ms fat pipe) out of a goodput that is used
/// as a steady-state ratio between two arms.
async fn run_rtp_sink_upload_windowed(
    c2s: NetemConfig,
    s2c: NetemConfig,
    data: &'static [u8],
    warmup: Duration,
    window: Duration,
) -> (NetemPair, u64) {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let (server_addr, delivered) =
                spawn_rtp_byte_sink_server_with_mss_via(&task_tx, false, FAT_PIPE_MSS)
                    .await
                    .unwrap();
            let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();
            let mut writer = spawn_rtp_bulk_upload_with_mss_via(
                &task_tx,
                pair.client_addr(),
                false,
                FAT_PIPE_MSS,
            )
            .await
            .unwrap();

            let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
            let mut pump_tasks = tokio::task::JoinSet::new();
            pump_tasks.spawn(async move {
                let mut offset = 0usize;
                loop {
                    tokio::select! {
                        _ = stop_rx.changed() => break,
                        result = writer.write(&data[offset..]) => match result {
                            Ok(0) => break,
                            Ok(n) => offset = (offset + n) % data.len(),
                            Err(_) => break,
                        },
                    }
                }
            });

            let d = tokio::select! {
                joined = pump_tasks.join_next(), if !pump_tasks.is_empty() => {
                    // The pump ended before the measurement window completed:
                    // fail the test instead of measuring against a dead upload.
                    joined.expect("pump task exists").unwrap();
                    panic!("bulk pump ended before the measurement window completed");
                }
                delta = async {
                    tokio::time::sleep(warmup).await;
                    let before = delivered.load(Ordering::Relaxed);
                    tokio::time::sleep(window).await;
                    delivered.load(Ordering::Relaxed) - before
                } => {
                    stop_tx.send(true).unwrap();
                    // Epilog: join the pump so any panic surfaces.
                    while let Some(result) = pump_tasks.join_next().await {
                        result.unwrap();
                    }
                    delta
                }
            };
            (pair, d)
        })
        .await
}

/// Sparse timestamped messages through a burst-loss link must deliver most
/// messages within a bounded tail latency.
///
/// We send 64-byte messages every 300 ms for 60 s through a GE burst-loss
/// link (5% loss, mean burst length 3). The messages travel over the native
/// RTP connection (the sparse traffic keeps cumulative progress advancing, so
/// the connection stays alive and this exercises RTP recovery under burst
/// loss rather than RTP's idle broken-pipe heuristic). The server records
/// one-way latency for every delivered message. After draining, we assert:
/// * received / sent >= 98%
/// * p50 <= 300 ms
/// * p99 <= 2500 ms
#[tokio::test(flavor = "multi_thread")]
#[ignore = "burst-loss goodput/tail-latency regression; slow; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn rtp_sparse_message_tail_latency_under_burst_loss() {
    let base = Instant::now();
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let (sent, mut samples) = tasks
        .run(async {
            let (server_addr, mut latencies) =
                spawn_rtp_msg_latency_sink_via(&task_tx, false, base)
                    .await
                    .unwrap();
            let pair = NetemPair::spawn(
                server_addr,
                burst_loss_link(5.0, 3.0, OWD, 11),
                burst_loss_link(5.0, 3.0, OWD, 22),
            )
            .unwrap();

            let (mut read, mut write) =
                rtp_connect_with_mss_via(&task_tx, pair.client_addr(), false, MSS).await;

            // Keep the read half alive for the duration of the test so ACKs keep
            // moving and the connection is not closed while we are only sending
            // pings. Parked until the connection closes; the owning JoinSet
            // aborts it at scope end.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    loop {
                        match read.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                    }
                }),
            );

            let sent = with_timeout(
                Duration::from_secs(80),
                "send sparse timestamped messages",
                send_timestamped_messages(
                    &mut write,
                    base,
                    SPARSE_MSG_BYTES,
                    SPARSE_MSG_INTERVAL,
                    SPARSE_MSG_INTERVAL * SPARSE_MSG_COUNT as u32,
                ),
            )
            .await;

            // Give stragglers a few RTTs to arrive, then drain the latency
            // channel.
            tokio::time::sleep(Duration::from_secs(4)).await;
            let mut samples = Vec::new();
            while let Ok(latency_ms) = latencies.try_recv() {
                samples.push(latency_ms);
            }
            pair.stop();
            (sent, samples)
        })
        .await;

    let received = samples.len() as u64;
    let delivery_pct = received as f64 / sent.max(1) as f64;
    eprintln!("[rtp_burst_loss] sparse sent={sent} received={received} delivery={delivery_pct:.3}");
    assert!(
        delivery_pct >= SPARSE_DELIVERY_FLOOR_PCT,
        "sparse delivery {delivery_pct:.3} < {SPARSE_DELIVERY_FLOOR_PCT}"
    );

    if !samples.is_empty() {
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = percentile(&samples, 0.50);
        let p99 = percentile(&samples, 0.99);
        eprintln!("[rtp_burst_loss] sparse latencies p50={p50:.1} ms p99={p99:.1} ms");
        assert!(
            p50 <= SPARSE_P50_LATENCY_MS,
            "sparse p50 latency {p50:.1} ms > {SPARSE_P50_LATENCY_MS} ms"
        );
        assert!(
            p99 <= SPARSE_P99_LATENCY_MS,
            "sparse p99 latency {p99:.1} ms > {SPARSE_P99_LATENCY_MS} ms"
        );
    }
}
