//! `rtp` + FEC recovery scenarios through [`netem_test::NetemPair`].
//!
//! Verifies that `rtp` with forward error correction enabled recovers lost
//! data symbols via parity even when the netem proxy drops a few percent of
//! packets in each direction.
//!
//! Runs in the default gate: this is the scenario that reaches the sender's
//! in-stream FEC capacity gate, so promoting it keeps the FEC retain/reject
//! decision path exercised by `cargo test -p tests`. It takes about five
//! seconds and is seeded.
//!
//! ```sh
//! cargo test -p rtp --test rtp_fec
//! ```
//!
//! The `max_diversity` coverage scenario in this file is `#[ignore]`d; run it
//! with:
//!
//! ```sh
//! cargo test --release -p rtp --test rtp_fec -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use netem_test::kit::payload::{payload, with_timeout};
use netem_test::kit::stats::combined_stats;
use netem_test::{NetemConfig, NetemPair};
use rtp::testkit::rtp::{
    rtp_connect_max_diversity_with_observer_via, rtp_connect_via, rtp_echo_payload,
    spawn_rtp_echo_server_via,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// `rtp` with FEC enabled should recover under ~3% netem loss in each
/// direction — the byte stream must arrive intact.
#[tokio::test(flavor = "multi_thread")]
async fn rtp_with_fec_recovers_under_netem_loss() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let server_addr = spawn_rtp_echo_server_via(&task_tx, true).await.unwrap();

            let lossy = NetemConfig {
                loss: u32::MAX / 33, // ~3%
                latency: Duration::from_millis(5),
                seed: 99,
                ..NetemConfig::default()
            };
            let pair = NetemPair::spawn(server_addr, lossy.clone(), lossy).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), true).await;

            let payload = payload(1024 * 1024);
            let got = with_timeout(
                Duration::from_secs(90),
                "rtp+FEC 1MiB echo",
                rtp_echo_payload(read, write, &payload),
            )
            .await;
            assert_eq!(got, payload, "FEC + reliable layer must recover all data");

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    assert!(
        stats.dropped > 0,
        "proxy should have dropped some packets, got {stats:?}"
    );
}

/// The `max_diversity` interactive FEC path is `data_count == 1 &&
/// small_group_parity_count > 1`; the default-FEC scenarios never reach it
/// (they use `FecTuning::default()`, parity depth 1).  Drive it under loss
/// with single-packet messages: every message must be delivered byte-exact,
/// the per-message tail latency must stay bounded, and the sender must
/// actually emit parity -- the delivered-stream and latency bounds alone are
/// met with FEC entirely disabled, so without the parity counter the scenario
/// would not pin the path it names.  The measured p50/p99 and the parity
/// counters are printed so a coverage run also reports the interactive cost.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "max_diversity FEC interactive-path coverage; run with --ignored --nocapture --test-threads=1"]
async fn rtp_max_diversity_fec_covers_single_packet_messages_under_loss() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    tasks
        .run(async {
            let server_addr = spawn_rtp_echo_server_via(&task_tx, true).await.unwrap();

            let lossy = NetemConfig {
                loss: u32::MAX / 20, // ~5%
                latency: Duration::from_millis(10),
                seed: 7,
                ..NetemConfig::default()
            };
            let pair = NetemPair::spawn(server_addr, lossy.clone(), lossy).unwrap();
            let parity_sent = Arc::new(AtomicU64::new(0));
            let recovered_symbols = Arc::new(AtomicU64::new(0));
            let sink = (Arc::clone(&parity_sent), Arc::clone(&recovered_symbols));
            // Snapshot only on send attempts and received ACKs: the FEC
            // counters are connection-lifetime cumulative, and a snapshot per
            // event would perturb the latency this scenario measures.
            let observer = rtp::metrics::MetricsObserver::filtered(
                |event, _| {
                    matches!(
                        event,
                        rtp::metrics::MetricsEvent::SendDataPacketAttempt
                            | rtp::metrics::MetricsEvent::ReceiveAckPacket
                    )
                },
                move |observation| {
                    if let Some(counters) = observation
                        .snapshot
                        .and_then(|snapshot| snapshot.fec_counters)
                    {
                        sink.0.store(counters.parity_sent, Ordering::Relaxed);
                        sink.1.store(counters.recovered_symbols, Ordering::Relaxed);
                    }
                },
            );
            let (mut read, mut write) =
                rtp_connect_max_diversity_with_observer_via(&task_tx, pair.client_addr(), observer)
                    .await;

            const N: usize = 200;
            let mut latencies = Vec::with_capacity(N);
            for i in 0..N {
                let msg = format!("m{i:04}");
                let start = std::time::Instant::now();
                write.write_all(msg.as_bytes()).await.unwrap();
                let mut buf = vec![0u8; msg.len()];
                read.read_exact(&mut buf).await.unwrap();
                assert_eq!(buf, msg.as_bytes(), "message {i} must echo byte-exact");
                latencies.push(start.elapsed());
            }

            latencies.sort_unstable();
            let p50 = latencies[N / 2];
            let p99 = latencies[N * 99 / 100];
            eprintln!(
                "[max-diversity fec] n={N} p50={p50:?} p99={p99:?} parity_sent={} recovered={}",
                parity_sent.load(Ordering::Relaxed),
                recovered_symbols.load(Ordering::Relaxed),
            );
            assert!(
                p99 < Duration::from_millis(250),
                "max_diversity FEC interactive p99 {p99:?} exceeds the 250ms bound"
            );
            let parity = parity_sent.load(Ordering::Relaxed);
            assert!(
                parity > 0,
                "the max_diversity FEC path must emit parity on the interactive lane under \
                 5% loss; observed {parity} parity symbols"
            );

            pair.stop();
        })
        .await;
}
