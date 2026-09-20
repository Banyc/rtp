//! Gentle-mode gate-open exit scenario.
//!
//! The delay controller's conservative gentle mode is entered after a
//! *sustained* standing queue (larger of three control RTTs and one second)
//! at low loss, and it leaves via one of four typed transitions.  The gentle
//! *entry* and probe branch are exercised by the bufferbloat and jitter arms,
//! but the clean-link `GateOpen` exit -- the transition that hands control
//! back to ordinary probing after the queue has drained and the delay gate has
//! stayed open long enough -- was unexercised by every committed scenario (a
//! 60 s `extra_20_100` fairness run holds its standing queue for at most
//! ~100 ms, far below the one-second entry floor, so gentle mode never enters
//! there).
//!
//! This scenario creates the transition deterministically in two phases:
//! a continuous full-rate backlog builds a standing queue past the entry
//! floor, then the offered load drops below the gentle drain target so the
//! queue clears and the gate stays open while the controller is still in
//! gentle mode.  A metrics observer counts the `GateOpen` exit event, and the
//! test asserts it fires -- proving the transition works, not merely that it
//! runs.
//!
//! Run with:
//!
//! ```sh
//! cargo test --release -p rtp --test rtp_gentle -- --ignored --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use netem_test::kit::payload::cyclic_payload;
use netem_test::kit::submit_test_task;
use netem_test::{NetemConfig, NetemPair};
use rtp::testkit::rtp::{
    rtp_connect_with_mss_fec_tuning_and_observer_via, spawn_rtp_byte_sink_server_via,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// One-way delay applied to every packet in both directions.
const OWD: Duration = Duration::from_millis(25);
/// Uniform jitter around [`OWD`].
const JITTER: Duration = Duration::from_millis(5);
/// Bottleneck rate for both directions.  2 Mbit/s is slow enough that a
/// backlogged sender's allowed in-flight window (the rate-based cwnd, itself
/// bounded by the minimum-RTT bandwidth-delay-product cap) stands as a
/// multi-hundred-millisecond queue, which is what carries the standing queue
/// past the gentle-entry floor.
const RATE_BPS: u64 = 2_000_000;
/// Full-rate backlog phase: long enough to enter gentle mode (the standing
/// queue must persist for the larger of three control RTTs and one second).
const BACKLOG_PHASE: Duration = Duration::from_secs(6);
/// Trickle phase cap.  The gate-open exit arrives well before this on every
/// run; the cap is only a liveness guard so a regression fails instead of
/// hanging.
const TRICKLE_CAP: Duration = Duration::from_secs(40);
/// Trickle cadence once the backlog stops.
const TRICKLE_CHUNK: usize = 4096;
const TRICKLE_INTERVAL: Duration = Duration::from_millis(50);

/// Two-phase deterministic gentle-mode entry + gate-open exit.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow; two-phase gentle-mode controller exercise; run with --ignored --nocapture --test-threads=1"]
async fn gentle_mode_exits_via_gate_open_after_a_standing_queue_drains() {
    let gate_open_exits = Arc::new(AtomicU64::new(0));
    let exited = Arc::new(AtomicBool::new(false));
    let exit_counter = Arc::clone(&gate_open_exits);
    let exited_flag = Arc::clone(&exited);
    let observer = rtp::metrics::MetricsObserver::selective(
        move |event, _elapsed| match event {
            rtp::metrics::MetricsEvent::GentleModeExit(
                rtp::metrics::MetricsGentleExitCause::GateOpen,
            ) => rtp::metrics::MetricsInterest::EventOnly,
            _ => rtp::metrics::MetricsInterest::Skip,
        },
        move |observation| {
            if matches!(
                observation.event,
                rtp::metrics::MetricsEvent::GentleModeExit(
                    rtp::metrics::MetricsGentleExitCause::GateOpen
                )
            ) {
                exit_counter.fetch_add(1, Ordering::Relaxed);
                exited_flag.store(true, Ordering::Relaxed);
            }
        },
    );

    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let delivered = tasks
        .run(async {
            let (sink_addr, delivered) = spawn_rtp_byte_sink_server_via(&task_tx, false)
                .await
                .unwrap();
            let pair = NetemPair::spawn(
                sink_addr,
                NetemConfig {
                    latency: OWD,
                    jitter: JITTER,
                    rate: RATE_BPS,
                    seed: 41,
                    ..NetemConfig::default()
                },
                NetemConfig {
                    latency: OWD,
                    jitter: JITTER,
                    rate: RATE_BPS,
                    seed: 42,
                    ..NetemConfig::default()
                },
            )
            .unwrap();
            let (mut read, mut write) = rtp_connect_with_mss_fec_tuning_and_observer_via(
                &task_tx,
                pair.client_addr(),
                false,
                rtp::udp::NO_FEC_MSS,
                rtp::FecTuning::default(),
                observer,
            )
            .await;
            // Keep the read half alive so ACKs are processed while sending.
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n) = read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );

            let payload = cyclic_payload(64 * 1024 * 1024);
            // Phase 1: a continuous full-rate backlog builds a standing queue
            // that persists past the gentle-entry floor.
            let backlog_until = Instant::now() + BACKLOG_PHASE;
            let mut offset = 0usize;
            while Instant::now() < backlog_until {
                match write.write(&payload[offset..]).await {
                    Ok(0) => break,
                    Ok(n) => offset = (offset + n) % payload.len(),
                    Err(_) => break,
                }
            }
            // Phase 2: a trickle below the gentle drain target lets the queue
            // clear and the delay gate stay open, until the controller leaves
            // gentle mode through the gate-open transition.
            let trickle_until = Instant::now() + TRICKLE_CAP;
            while Instant::now() < trickle_until {
                if exited.load(Ordering::Relaxed) {
                    break;
                }
                if write
                    .write(&payload[offset..offset + TRICKLE_CHUNK])
                    .await
                    .is_err()
                {
                    break;
                }
                offset = (offset + TRICKLE_CHUNK) % (payload.len() - TRICKLE_CHUNK);
                tokio::time::sleep(TRICKLE_INTERVAL).await;
            }
            // Keep the connection alive briefly so the exit event is dispatched
            // before teardown.
            tokio::time::sleep(Duration::from_millis(100)).await;
            pair.stop();
            delivered.load(Ordering::Relaxed)
        })
        .await;

    let exits = gate_open_exits.load(Ordering::Relaxed);
    eprintln!("[rtp_gentle] delivered={delivered} bytes; gentle_mode_exit_gate_open={exits}");
    assert!(
        delivered > 0,
        "the connection must have delivered the backlog phase's bytes"
    );
    assert!(
        exits >= 1,
        "the clean-link GateOpen gentle-mode exit must fire after the standing \
         queue drains; observed {exits}"
    );
}
