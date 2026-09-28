//! Where the clean interactive lane's loss-independent tail comes from.
//!
//! The harness's four-flow clean arm measured a tail that survives with the
//! forward shaper's drop count at zero: ~0.17-0.43 % of the delivered messages
//! landed above the link's own no-loss ceiling (`OWD + JITTER = 30 ms`) and
//! stopped at ~33-42 ms.  The shaper's delay draw is exactly
//! `uniform[owd - jitter, owd + jitter)` (`netem-test/src/shaper.rs`,
//! `sample_delay`), so no delay *model* can place a datagram above 30 ms, and
//! no arm separated the shaper's own scheduling from a send/receive-path
//! effect.
//!
//! This is that separator, and it is the same offer shape throughout: 256 B
//! timestamped messages at the interactive lane's four-flow interleave (one
//! datagram every 1.25 ms, which is what four 5 ms flows on one lane offer).
//! Each cell is one dimension away from the `rtp` + jittered-shaper baseline:
//!
//! | cell | `rtp` | shaper | endpoints |
//! |------|-------|--------|-----------|
//! | `rtp_shaped_j5` (baseline) | yes | `25 ms +- 5 ms` | tokio |
//! | `rtp_shaped_j0` | yes | `25 ms` | tokio |
//! | `raw_shaped_j5` | **no** | `25 ms +- 5 ms` | tokio |
//! | `raw_shaped_j0` | **no** | `25 ms` | tokio |
//! | `rtp_blk_shaped_j5` | yes | `25 ms +- 5 ms` | blocking threads |
//! | `rtp_blk_shaped_j0` | yes | `25 ms` | blocking threads |
//!
//! The `raw` cells run a plain `UdpSocket` endpoint pair: a tail that appears
//! there carries **no `rtp` code at all**.  The `j0` cells remove the jitter
//! draw and the reorder it induces.  The `blk` cells remove the endpoint
//! runtime from the measurement, leaving the shaper's own thread and socket
//! pipeline alone.
//!
//! What the pair of tests therefore decides, and what it cannot: a tail that
//! appears in a `raw`/`blk` cell is the harness's (shaper or endpoints) and
//! can be removed by no transport change; a tail that appeared only in the
//! `rtp` cells would be the transport's.  It **cannot resolve a transport
//! contribution smaller than the arm-to-arm spread** of the tail *rate* on a
//! loaded host, which is why the rate is printed and compared rather than
//! bounded, and why the shape assertion below is about where the tail sits
//! (on the delay-model ceiling, the signature of added latency) rather than
//! how big it is.
//!
//! Run with:
//!
//! ```sh
//! cargo test --release -p rtp --test rtp_clean_tail -- --ignored --nocapture --test-threads=1
//! ```

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use netem_test::kit::stats::percentile;
use netem_test::kit::{TEST_TASK_QUEUE_BOUND, TestScope, submit_test_task};
use netem_test::{Counters, NetemConfig, NetemPair};
use rtp::testkit::rtp::{rtp_connect_with_mss_via, spawn_rtp_msg_latency_sink_via};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The clean interactive link's one-way delay and jitter: the deployed
/// interactive lane's own numbers (`rtp_mux/tests/mandate_smoke.rs`).
const OWD: Duration = Duration::from_millis(25);
const JITTER: Duration = Duration::from_millis(5);
const OWD_MS: f64 = 25.0;
/// The link's own no-loss ceiling: the shaper's delay draw is exactly
/// `uniform[OWD - JITTER, OWD + JITTER)`, so a datagram above this was made
/// late by something other than the delay model.
const CEILING_MS: f64 = 30.0;
/// The window above the ceiling the shape reading's first bin counts.
const SHAPE_BIN_MS: f64 = 1.0;
/// The shape assertion's bound: the tail's median excess above the delay
/// model's ceiling. Set from the measurement recorded in the family's
/// `GATE.md` row -- every asserted cell's median excess measured 0.01-1.00 ms
/// across the reps on record, while the `hold` fault, the fixed-offset delay of
/// the class the assertion exists to reject, measured several times the bound.
const SHAPE_EXCESS_MS: f64 = 3.0;

const MSG_BYTES: usize = 256;
/// The four-flow interleave of the interactive lane's 5 ms cadence.
const FOUR_FLOW_INTERLEAVE: Duration = Duration::from_micros(1_250);
/// The measured window per cell, and the grace the shaped cells drain in.
/// The window is the surface's one *size*: `RTP_CLEAN_TAIL_WINDOW_SECS`
/// scales every cell's measured seconds, and 4 s at the four-flow interleave
/// carries ~3200 samples per cell, which resolves a tail an order of magnitude
/// thinner than the one the shape assertion is read on.
fn window() -> Duration {
    Duration::from_secs(
        std::env::var("RTP_CLEAN_TAIL_WINDOW_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(4),
    )
}
const DRAIN: Duration = Duration::from_secs(1);
/// The held-message fault's cadence and hold: every second message is written
/// `HOLD` after its send stamp is taken.
const HOLD_EVERY: u64 = 2;
const HOLD: Duration = Duration::from_millis(8);

/// The shaper fault and the `+40 ms` shift the integrity assertion's vacuity
/// demonstration injects into the *link itself*.
enum Fault {
    None,
    Delay,
    Hold,
    Drop,
}

fn fault() -> Fault {
    match std::env::var("RTP_CLEAN_TAIL_FAULT").as_deref() {
        Ok("delay") => Fault::Delay,
        Ok("hold") => Fault::Hold,
        Ok("drop") => Fault::Drop,
        _ => Fault::None,
    }
}

/// The forward and reverse shaper, loss closed unless the `drop` fault is
/// selected: `Some(jitter)` is the deployed clean link, `None` the
/// constant-delay counterpart that removes the jitter draw and the reorder it
/// induces.
fn shaped_link(seed: u64, jitter: Option<Duration>, fault: &Fault) -> NetemConfig {
    NetemConfig {
        latency: OWD
            + if matches!(fault, Fault::Delay) {
                Duration::from_millis(40)
            } else {
                Duration::ZERO
            },
        jitter: jitter.unwrap_or(Duration::ZERO),
        loss: if matches!(fault, Fault::Drop) {
            u32::MAX / 100
        } else {
            0
        },
        seed,
        ..NetemConfig::default()
    }
}

/// One cell's reading.
struct Cell {
    label: String,
    /// `(elapsed s, one-way ms)` per delivered message, in delivery order.
    samples: Vec<(f64, f64)>,
    counters: Counters,
    offered: u64,
    /// Whether this cell's link carried the jitter draw -- the condition the
    /// shape assertion is read under.
    jittered: bool,
    wall: Duration,
}

impl Cell {
    /// The one-way series, ascending: `netem_test::kit::stats::percentile`
    /// indexes a *sorted* slice, so an unsorted call returns the element at the
    /// rank of a delivery-ordered series -- which reads as `p99 < p90`.
    fn sorted_latencies(&self) -> Vec<f64> {
        let mut lats: Vec<f64> = self.samples.iter().map(|s| s.1).collect();
        lats.sort_by(|a, b| a.partial_cmp(b).unwrap());
        lats
    }

    /// The tail above the link's own no-loss ceiling, ascending.
    fn tail(&self) -> Vec<f64> {
        let mut tail: Vec<f64> = self
            .samples
            .iter()
            .map(|s| s.1)
            .filter(|y| *y > CEILING_MS)
            .collect();
        tail.sort_by(|a, b| a.partial_cmp(b).unwrap());
        tail
    }

    /// The fraction of the tail that sits within one millisecond of the delay
    /// model's own ceiling.  An *additive* latency landing on the top of the
    /// bounded uniform draw puts its mode there; a mechanism with a
    /// characteristic offset (a repair rung, a held batch, a pacer boundary)
    /// takes the tail away from it.
    fn tail_on_ceiling(&self) -> f64 {
        let tail = self.tail();
        if tail.is_empty() {
            return 1.0;
        }
        let head = tail
            .iter()
            .filter(|y| **y <= CEILING_MS + SHAPE_BIN_MS)
            .count();
        head as f64 / tail.len() as f64
    }

    /// The tail's **median excess** above the delay model's ceiling, in ms: the
    /// shape statistic the assertion reads, because it separates the mechanism
    /// classes by a factor rather than by a share.  A sub-millisecond additive
    /// lag on the bounded delay draw puts every tail sample just above the
    /// ceiling (excess well under 1 ms), while a mechanism at its own offset --
    /// a held batch, a repair rung, a pacer boundary -- puts half the tail at
    /// that offset.
    fn tail_median_excess(&self) -> f64 {
        let tail = self.tail();
        if tail.is_empty() {
            return 0.0;
        }
        percentile(&tail, 0.50) - CEILING_MS
    }

    fn report(&self) {
        let lats = self.sorted_latencies();
        let tail = self.tail();
        let mut bins = [0usize; 8];
        for y in &tail {
            bins[((y - CEILING_MS) as usize).min(7)] += 1;
        }
        println!(
            "[clean-tail {}] wall={:.2}s offered={} received={} forwarded={} dropped={} samples={} p50={:.2} p90={:.2} p99={:.2} max={:.2} tail>{CEILING_MS:.0}ms={} ({:.3}%) tail_bins={bins:?} tail_on_ceiling={:.3} tail_median_excess_ms={:.2}",
            self.label,
            self.wall.as_secs_f64(),
            self.offered,
            self.counters.received,
            self.counters.forwarded,
            self.counters.dropped,
            lats.len(),
            percentile(&lats, 0.50),
            percentile(&lats, 0.90),
            percentile(&lats, 0.99),
            lats.last().copied().unwrap_or(f64::NAN),
            tail.len(),
            100.0 * tail.len() as f64 / lats.len().max(1) as f64,
            self.tail_on_ceiling(),
            self.tail_median_excess(),
        );
    }
}

/// The instrument's own integrity and shape assertions, each falsifiable.
///
/// `RTP_CLEAN_TAIL_FAULT` is the vacuity selector for all three: `delay` shifts
/// the link by +40 ms so the body assertion fails naming the observed p50,
/// `drop` makes the loss-closed link drop so the loss clause fails, and `hold`
/// writes every second message 8 ms after its send stamp -- a fixed-offset
/// delay of exactly the class a transport mechanism would add -- so the tail
/// leaves the ceiling and the shape assertion fails naming the observed
/// fraction.
fn assert_cell(cell: &Cell) {
    // The reading is printed before the assertions, so a cell that fails still
    // shows what it measured rather than only the assertion's value.
    cell.report();
    let lats = cell.sorted_latencies();
    assert!(
        !lats.is_empty(),
        "[clean-tail {}] the cell delivered no sample: a tail composition of nothing is not a measurement",
        cell.label,
    );
    assert_eq!(
        cell.counters.dropped, 0,
        "[clean-tail {}] the loss-closed link dropped {} datagrams",
        cell.label, cell.counters.dropped,
    );
    let p50 = percentile(&lats, 0.50);
    assert!(
        (p50 - OWD_MS).abs() <= OWD_MS * 0.25,
        "[clean-tail {}] the body sits at p50={p50:.2} ms, outside the clean link's own {OWD_MS:.0} ms +- 25 % band (max={:.2} ms)",
        cell.label,
        lats.last().copied().unwrap_or(f64::NAN),
    );
    if cell.jittered {
        let on_ceiling = cell.tail_on_ceiling();
        let tail = cell.tail();
        let excess = cell.tail_median_excess();
        assert!(
            excess <= SHAPE_EXCESS_MS,
            "[clean-tail {}] the tail's median excess above the delay model's ceiling is {excess:.2} ms, over the {SHAPE_EXCESS_MS:.0} ms band an additive latency on the bounded delay draw can reach (tail={} samples)",
            cell.label,
            tail.len(),
        );
        assert!(
            on_ceiling >= 0.2,
            "[clean-tail {}] only {:.1} % of the {} samples above the delay model's ceiling sit within {SHAPE_BIN_MS:.0} ms of it: the tail is not added latency on the bounded delay draw but a mechanism at its own offset (tail={:?})",
            cell.label,
            100.0 * on_ceiling,
            tail.len(),
            {
                let head: Vec<String> = tail.iter().take(3).map(|y| format!("{y:.2}")).collect();
                let top: Vec<String> = tail
                    .iter()
                    .rev()
                    .take(3)
                    .map(|y| format!("{y:.2}"))
                    .collect();
                format!("first {head:?} last {top:?}")
            },
        );
    }
}

/// One `rtp` cell over a tokio runtime: an `rtp` connection whose read half is
/// drained (so ACKs keep moving) and whose latency sink is collected for the
/// whole arm, offering the same framed, timestamped messages the testkit's own
/// sender writes.
async fn rtp_cell(label: &str, jitter: Option<Duration>, fault: &Fault) -> Cell {
    let wall = Instant::now();
    let base = Instant::now();
    let mut tasks = TestScope::new();
    let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
    let out = tasks
        .run(async {
            let (sink_addr, mut latencies) = spawn_rtp_msg_latency_sink_via(&task_tx, false, base)
                .await
                .unwrap();
            let pair = NetemPair::spawn(
                sink_addr,
                shaped_link(41, jitter, fault),
                shaped_link(42, jitter, fault),
            )
            .unwrap();
            let (mut read, mut write) =
                rtp_connect_with_mss_via(&task_tx, pair.client_addr(), false, rtp::udp::NO_FEC_MSS)
                    .await;
            submit_test_task(
                &task_tx,
                Box::pin(async move {
                    let mut buf = vec![0u8; 8 * 1024];
                    while let Ok(n) = read.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }),
            );
            let collected = Arc::new(Mutex::new(Vec::new()));
            submit_test_task(
                &task_tx,
                Box::pin({
                    let collected = Arc::clone(&collected);
                    async move {
                        while let Some(latency) = latencies.recv().await {
                            collected
                                .lock()
                                .unwrap()
                                .push((base.elapsed().as_secs_f64(), latency));
                        }
                    }
                }),
            );
            let offered = write_messages(&mut write, base, fault).await;
            tokio::time::sleep(DRAIN).await;
            let counters = pair.snapshot_c2s().stats;
            pair.stop();
            let samples = std::mem::take(&mut *collected.lock().unwrap());
            (samples, counters, offered)
        })
        .await;
    let (samples, counters, offered) = out;
    Cell {
        label: label.to_owned(),
        samples,
        counters,
        offered,
        jittered: jitter.is_some(),
        wall: wall.elapsed(),
    }
}

/// The message writer both `rtp` cells share: the testkit sink's framing
/// (`[4-byte LE total length][payload][8-byte LE send stamp]`) at the
/// four-flow interleave, with the `hold` fault writing every
/// [`HOLD_EVERY`]th message [`HOLD`] after its stamp is read.
async fn write_messages(
    write: &mut (impl AsyncWriteExt + Unpin),
    base: Instant,
    fault: &Fault,
) -> u64 {
    let mut iv = tokio::time::interval(FOUR_FLOW_INTERLEAVE);
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let payload: Vec<u8> = (0..MSG_BYTES - 12).map(|i| (i % 251) as u8).collect();
    let start = Instant::now();
    let mut sent = 0u64;
    let mut frame = Vec::with_capacity(MSG_BYTES);
    while start.elapsed() < window() {
        iv.tick().await;
        let us = base.elapsed().as_micros() as u64;
        if matches!(fault, Fault::Hold) && sent.is_multiple_of(HOLD_EVERY) {
            tokio::time::sleep(HOLD).await;
        }
        frame.clear();
        frame.extend_from_slice(&(MSG_BYTES as u32).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&us.to_le_bytes());
        if write.write_all(&frame).await.is_err() {
            break;
        }
        sent += 1;
    }
    sent
}

/// One plain-`UdpSocket` cell: the same offer and the same shaper with **no
/// `rtp` in the path**.
async fn raw_cell(label: &str, jitter: Option<Duration>, fault: &Fault) -> Cell {
    let wall = Instant::now();
    let base = Instant::now();
    let server = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let pair = NetemPair::spawn(
        server_addr,
        shaped_link(41, jitter, fault),
        shaped_link(42, jitter, fault),
    )
    .unwrap();
    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dest: SocketAddr = pair.client_addr();
    let send = async {
        let mut iv = tokio::time::interval(FOUR_FLOW_INTERLEAVE);
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let start = Instant::now();
        let mut sent = 0u64;
        let mut frame = vec![0u8; MSG_BYTES];
        while start.elapsed() < window() {
            iv.tick().await;
            let us = base.elapsed().as_micros() as u64;
            frame[..8].copy_from_slice(&us.to_le_bytes());
            if client.send_to(&frame, dest).await.is_err() {
                break;
            }
            sent += 1;
        }
        sent
    };
    let receive = async {
        let mut out: Vec<(f64, f64)> = Vec::new();
        let until = base + window() + DRAIN;
        let mut buf = vec![0u8; 2048];
        while Instant::now() < until {
            match tokio::time::timeout(Duration::from_millis(50), server.recv_from(&mut buf)).await
            {
                Ok(Ok((n, _))) if n >= 8 => {
                    let sent_us = u64::from_le_bytes(buf[..8].try_into().unwrap());
                    out.push((
                        base.elapsed().as_secs_f64(),
                        (base.elapsed().as_micros() as u64).saturating_sub(sent_us) as f64 / 1000.0,
                    ));
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }
        out
    };
    let (offered, samples) = tokio::join!(send, receive);
    let counters = pair.snapshot_c2s().stats;
    pair.stop();
    Cell {
        label: label.to_owned(),
        samples,
        counters,
        offered,
        jittered: jitter.is_some(),
        wall: wall.elapsed(),
    }
}

/// One blocking-endpoint `rtp`-less cell: `std::thread` senders and a blocking
/// receive thread, so the endpoint runtime is out of the measurement and
/// whatever tail is left is the shaper's own.
///
/// It is deliberately `rtp`-less as well as runtime-less: the shaper's pipeline
/// is the only component the remaining three cells do not already isolate.
fn blocking_cell(label: &str, jitter: Option<Duration>, fault: &Fault) -> Cell {
    let wall = Instant::now();
    let base = Instant::now();
    let server = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    server
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let server_addr = server.local_addr().unwrap();
    let pair = NetemPair::spawn(
        server_addr,
        shaped_link(41, jitter, fault),
        shaped_link(42, jitter, fault),
    )
    .unwrap();
    let client = Arc::new(std::net::UdpSocket::bind("127.0.0.1:0").unwrap());
    let dest = pair.client_addr();
    let sender = std::thread::spawn({
        let client = Arc::clone(&client);
        move || {
            let mut frame = vec![0u8; MSG_BYTES];
            let start = Instant::now();
            let mut sent = 0u64;
            while start.elapsed() < window() {
                let us = base.elapsed().as_micros() as u64;
                frame[..8].copy_from_slice(&us.to_le_bytes());
                if client.send_to(&frame, dest).is_err() {
                    break;
                }
                sent += 1;
                std::thread::sleep(FOUR_FLOW_INTERLEAVE);
            }
            sent
        }
    });
    let until = base + window() + DRAIN;
    let receiver = std::thread::spawn(move || {
        let mut out: Vec<(f64, f64)> = Vec::new();
        let mut buf = vec![0u8; 2048];
        while Instant::now() < until {
            match server.recv_from(&mut buf) {
                Ok((n, _)) if n >= 8 => {
                    let sent_us = u64::from_le_bytes(buf[..8].try_into().unwrap());
                    let now = base.elapsed();
                    out.push((
                        now.as_secs_f64(),
                        (now.as_micros() as u64).saturating_sub(sent_us) as f64 / 1000.0,
                    ));
                }
                Ok(_) => continue,
                Err(_) => continue,
            }
        }
        out
    });
    let offered = sender.join().unwrap();
    let samples = receiver.join().unwrap();
    let counters = pair.snapshot_c2s().stats;
    pair.stop();
    Cell {
        label: label.to_owned(),
        samples,
        counters,
        offered,
        jittered: jitter.is_some(),
        wall: wall.elapsed(),
    }
}

/// The clean interactive lane's loss-independent tail, attributed across the
/// {`rtp` present} x {jitter present} matrix at the deployed four-flow offer.
///
/// The cells are run in one body so they share the host's load, which is the
/// only way the comparison between them is a comparison and not two different
/// hosts.  The tail *rate* is printed per cell and deliberately not bounded:
/// on this host it is dominated by endpoint scheduling (it moved between 0 %
/// and 10 % across reps of the same cell), and a bound on a noise-dominated
/// statistic is not coverage.  What is asserted is the instrument -- every cell
/// measured something, the loss-closed link dropped nothing, the body is on the
/// declared link, and a jittered cell's tail sits on the delay model's ceiling
/// rather than at a mechanism's own offset.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "clean-lane tail attribution: {rtp} x {jitter} matrix, 4 cells at the four-flow offer; ~20 s; run with --ignored --nocapture --test-threads=1"]
async fn clean_tail_shaper_vs_transport_matrix() {
    let fault = fault();
    let mut cells = Vec::new();
    for (stack, jitter) in [
        ("rtp_shaped_j5", Some(JITTER)),
        ("rtp_shaped_j0", None),
        ("raw_shaped_j5", Some(JITTER)),
        ("raw_shaped_j0", None),
    ] {
        let cell = if stack.starts_with("rtp") {
            rtp_cell(stack, jitter, &fault).await
        } else {
            raw_cell(stack, jitter, &fault).await
        };
        assert_cell(&cell);
        cells.push(cell);
    }
    // The baseline cell's three dimensions are the ones every other cell moves
    // exactly one of, and the raw cells' shadowing of the baseline is the
    // reading: printed together so a reader sees them side by side.
    let baseline = cells
        .iter()
        .find(|c| c.label == "rtp_shaped_j5")
        .expect("the baseline cell always runs first");
    for cell in &cells {
        println!(
            "[clean-tail] vs baseline {}: tail_rate {:.3}% against baseline {:.3}%, body p50 {:.2} ms against {:.2} ms",
            cell.label,
            100.0 * cell.tail().len() as f64 / cell.samples.len().max(1) as f64,
            100.0 * baseline.tail().len() as f64 / baseline.samples.len().max(1) as f64,
            percentile(&cell.sorted_latencies(), 0.50),
            percentile(&baseline.sorted_latencies(), 0.50),
        );
    }
}

/// The same shaped offer with the **endpoint runtime removed**: `std::thread`
/// endpoints, no tokio, no `rtp`.  This is the orthogonal member of the
/// `clean-tail` family, one dimension (the endpoint stack) away from the
/// baseline, and it is the arm that decides whether the tail needs the endpoints
/// at all.
#[test]
#[ignore = "clean-lane tail attribution: blocking-endpoint control, 2 cells; ~10 s; run with --ignored --nocapture --test-threads=1"]
fn clean_tail_blocking_endpoint_control() {
    let fault = fault();
    for (label, jitter) in [
        ("raw_blk_shaped_j5", Some(JITTER)),
        ("raw_blk_shaped_j0", None),
    ] {
        let cell = blocking_cell(label, jitter, &fault);
        assert_cell(&cell);
    }
}
