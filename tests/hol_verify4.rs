//! `DualMux`-v4 bulk-lane A/B report-only probes: the raw `rtp` arms.
//!
//! The dual-lane `DualMux` architecture intends its bulk lane to be a full
//! `mux` session over its own `rtp` connection. These report-only probes
//! measure the raw `rtp` bulk lane on identically-seeded netem links; the mux
//! bulk-lane arms live in the owning crate (`mux/tests/hol_verify4.rs`,
//! `v4_clean_muxbulk` / `v4_ge5_muxbulk`), so each crate owns its half of the
//! A/B comparison and the links stay reproducible from the seeds.
//!
//! Run with:
//!
//! ```sh
//! cargo test --release -p rtp --test hol_verify4 -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use netem_test::kit::payload::{cyclic_payload, with_timeout};
use netem_test::kit::presets::{burst_loss_link, clean_delay_link};
use netem_test::{NetemConfig, NetemPair};
use rtp::testkit::rtp::{spawn_rtp_bulk_upload_via, spawn_rtp_byte_sink_server_via};
use tokio::io::AsyncWriteExt;

/// Wall-clock budget for each probe (1.5 s ramp + 15 s run + 3 s grace + slack).
const BULK_WINDOW: Duration = Duration::from_millis(19_500);

/// Bulk write chunk size: 251-aligned (~256 KiB).
const CHUNK: usize = 262_044;

/// Open a raw `rtp` connection through a `NetemPair` and pump the same
/// deterministic cyclic payload for `BULK_WINDOW`.
///
/// The sink is [`rtp::testkit::rtp::spawn_rtp_byte_sink_server_via`]. Returns
/// the total bytes delivered at the server-side sink.
async fn run_rawbulk(label: &str, c2s: NetemConfig, s2c: NetemConfig) -> u64 {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let (elapsed, delivered_at_window, total) = tasks
        .run(async {
            let (sink_addr, delivered) = spawn_rtp_byte_sink_server_via(&task_tx, false)
                .await
                .unwrap();
            let pair = NetemPair::spawn(sink_addr, c2s, s2c).unwrap();

            let mut writer = spawn_rtp_bulk_upload_via(&task_tx, pair.client_addr(), false)
                .await
                .unwrap();
            let payload = cyclic_payload(CHUNK);
            let start = Instant::now();
            while start.elapsed() < BULK_WINDOW {
                match writer.write_all(&payload[..CHUNK]).await {
                    Ok(()) => {}
                    Err(_) => break,
                }
            }
            let elapsed = start.elapsed();
            let delivered_at_window = delivered.load(Ordering::Relaxed);

            // Explicitly drop the writer so the server sees EOF and stops counting.
            // The server task then completes; that happens outside the raced body
            // (after the body's own teardown below), so it is not an early exit.
            drop(writer);
            // Give stragglers time to drain before stopping the proxy.
            tokio::time::sleep(Duration::from_secs(3)).await;
            pair.stop();

            let total = delivered.load(Ordering::Relaxed);
            (elapsed, delivered_at_window, total)
        })
        .await;

    let mibps = total as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::EPSILON);
    let mibps_window =
        delivered_at_window as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::EPSILON);
    eprintln!(
        "[v4 {label}] delivered={total}B (at-window={delivered_at_window}B) \
         elapsed={elapsed:?} bulk={mibps:.3} MiB/s bulk-window={mibps_window:.3} MiB/s",
    );
    total
}

// ────────────────────────────── report-only probes ───────────────────────────

/// Raw `rtp` bulk lane over Gilbert-Elliott 5% burst loss (c2s seed 33, s2c
/// seed 44) — the raw half of the A/B comparison with the mux crate's
/// `v4_ge5_muxbulk` arm on identically-seeded links.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "DualMux-v4 bulk-lane A/B report-only probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn v4_ge5_rawbulk() {
    with_timeout(
        Duration::from_secs(120),
        "v4 ge5 rawbulk",
        run_rawbulk(
            "ge5 rawbulk",
            burst_loss_link(5.0, 3.0, Duration::from_millis(50), 33),
            burst_loss_link(5.0, 3.0, Duration::from_millis(50), 44),
        ),
    )
    .await;
}

/// Raw `rtp` bulk lane on a clean 50 ms RTT link (c2s seed 11, s2c seed 22) —
/// the raw half of the A/B comparison with the mux crate's `v4_clean_muxbulk`
/// arm on identically-seeded links.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "DualMux-v4 bulk-lane A/B report-only probe; run with --ignored --nocapture --test-threads=1 (see module header)"]
async fn v4_clean_rawbulk() {
    with_timeout(
        Duration::from_secs(120),
        "v4 clean rawbulk",
        run_rawbulk(
            "clean rawbulk",
            clean_delay_link(Duration::from_millis(50), 11),
            clean_delay_link(Duration::from_millis(50), 22),
        ),
    )
    .await;
}
