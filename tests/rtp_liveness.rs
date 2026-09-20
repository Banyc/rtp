//! RTP cumulative-progress liveness integration test.
//!
//! The RTP `ack` path was changed to tie cumulative-progress liveness to the
//! send-frontier advance rather than fresh SACKs. This test validates that
//! change end-to-end: a connection with a permanent cumulative hole does not
//! stay alive indefinitely even though periodic small heartbeat frames and
//! ACK/SACK traffic continue to flow.
//!
//! The two end-to-end tests below are `#[ignore]`-d by default so they do not
//! lengthen normal builds; `reverse_traffic_recency_advances_only_on_new_packets`
//! is a fast pure unit test and runs in normal builds.
//! Run the end-to-end tests with:
//!
//! ```sh
//! cargo test -p rtp --test rtp_liveness rtp_fresh_sacks -- --ignored --exact --nocapture
//! ```

use std::time::{Duration, Instant};

use netem_test::kit::submit_test_task;
use netem_test::{NetemConfig, NetemPair};
use rtp::testkit::rtp::send_timestamped_messages;
use rtp::testkit::rtp::{rtp_connect_with_mss_via, spawn_rtp_msg_latency_sink_via};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_DATAGRAM: usize = 512;

const OWD: Duration = Duration::from_millis(50);

const MSG_BYTES: usize = 64;

const MSG_INTERVAL: Duration = Duration::from_secs(1);

const MAX_DURATION: Duration = Duration::from_secs(65);

const MIN_POST_HOLE_WRITES: u64 = 20;

const MIN_BROKEN_PIPE_ELAPSED: Duration = Duration::from_secs(30);

const MAX_REVERSE_TRAFFIC_SILENCE: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct ReverseTrafficTracker {
    last_forwarded: u64,
    last_change: Instant,
}

impl ReverseTrafficTracker {
    fn new(last_forwarded: u64, now: Instant) -> Self {
        Self {
            last_forwarded,
            last_change: now,
        }
    }

    fn observe(&mut self, forwarded: u64, now: Instant) {
        if forwarded > self.last_forwarded {
            self.last_forwarded = forwarded;
            self.last_change = now;
        }
    }

    fn silence_at(&self, now: Instant) -> Duration {
        now.duration_since(self.last_change)
    }
}

#[test]
#[ignore = "runs up to MAX_DURATION (65s) end-to-end; keep out of normal test builds (see module header)"]
fn rtp_fresh_sacks_beyond_permanent_mtu_hole_do_not_keep_connection_alive() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut tasks = netem_test::kit::TestScope::new();
        let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
        let (
            write_error,
            heartbeat_count,
            reverse_traffic,
            termination_time,
            c2s_after,
            s2c_after,
            elapsed,
            pre_hole_frames,
            s2c_before,
            c2s_before,
            mut latency_rx,
        ) = tasks
            .run(async {
                let base = Instant::now();
                let start = Instant::now();

                let (server_addr, mut latency_rx) =
                    spawn_rtp_msg_latency_sink_via(&task_tx, false, base).await.unwrap();

                let c2s = NetemConfig {
                    max_datagram_size: MAX_DATAGRAM,
                    latency: OWD,
                    ..NetemConfig::default()
                };
                let s2c = NetemConfig {
                    latency: OWD,
                    ..NetemConfig::default()
                };
                let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();

                let (mut read, mut write) = rtp_connect_with_mss_via(
                    &task_tx,
                    pair.client_addr(),
                    false,
                    rtp::udp::NO_FEC_MSS,
                )
                .await;

                // Keep the read half alive so ACKs keep flowing; parked until the
                // connection closes, so the owning JoinSet aborts it at scope end.
                submit_test_task(&task_tx, Box::pin(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    loop {
                        let n = read.read(&mut buf).await;
                        match n {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                    }
                }));

                let sent = send_timestamped_messages(
                    &mut write,
                    base,
                    MSG_BYTES,
                    MSG_INTERVAL,
                    Duration::from_millis(500),
                )
                .await;
                assert!(sent > 0, "initial handshake message must be delivered");
                tokio::time::sleep(Duration::from_secs(2)).await;

                let mut pre_hole_frames = 0u64;
                while latency_rx.try_recv().is_ok() {
                    pre_hole_frames += 1;
                }
                assert!(
                    pre_hole_frames > 0,
                    "server must have received the initial handshake frame"
                );

                let hole_payload = netem_test::kit::payload::payload(1024);
                let _ = write.write_all(&hole_payload).await;
                tokio::time::sleep(Duration::from_secs(1)).await;

                let s2c_before = pair.stats_s2c().forwarded;
                let c2s_before = pair.stats_c2s();

                let mut heartbeat_count = 0u64;
                let mut write_error: Option<std::io::ErrorKind> = None;
                let mut reverse_traffic = ReverseTrafficTracker::new(s2c_before, Instant::now());

                loop {
                    if start.elapsed() >= MAX_DURATION {
                        panic!(
                            "Connection stayed alive >{:?} without cumulative progress; \
                             delivered {} post-hole frames",
                            MAX_DURATION, heartbeat_count,
                        );
                    }

                    reverse_traffic.observe(pair.stats_s2c().forwarded, Instant::now());

                    let mut buf = Vec::with_capacity(MSG_BYTES + 12);
                    buf.extend_from_slice(&((MSG_BYTES + 12) as u32).to_le_bytes());
                    buf.extend_from_slice(&netem_test::kit::payload::payload(MSG_BYTES));
                    buf.extend_from_slice(&base.elapsed().as_micros().to_le_bytes());

                    let res =
                        tokio::time::timeout(Duration::from_secs(2), write.write_all(&buf)).await;
                    match res {
                        Ok(Ok(())) => {
                            heartbeat_count += 1;
                            tokio::time::sleep(MSG_INTERVAL).await;
                        }
                        Ok(Err(e)) => {
                            write_error = Some(e.kind());
                            eprintln!(
                                "[rtp_liveness] write failed with {:?} after {} heartbeats",
                                e.kind(),
                                heartbeat_count,
                            );
                            break;
                        }
                        Err(_) => {
                            eprintln!(
                                "[rtp_liveness] write timed out after {} heartbeats; \
                                 BrokenPipe did not fire within 2s",
                                heartbeat_count,
                            );
                            break;
                        }
                    }
                }

                let termination_time = Instant::now();
                let c2s_after = pair.stats_c2s();
                let s2c_after = pair.stats_s2c();
                pair.stop();
                let elapsed = start.elapsed();
                (
                    write_error,
                    heartbeat_count,
                    reverse_traffic,
                    termination_time,
                    c2s_after,
                    s2c_after,
                    elapsed,
                    pre_hole_frames,
                    s2c_before,
                    c2s_before,
                    latency_rx,
                )
            })
            .await;

        let mut server_frames = 0u64;
        while latency_rx.try_recv().is_ok() {
            server_frames += 1;
        }
        eprintln!(
            "[rtp_liveness] elapsed={elapsed:?} post_hole_heartbeats_sent={heartbeat_count} \
             server_frames={server_frames} (pre_hole={pre_hole_frames})"
        );

        assert_eq!(
            write_error,
            Some(std::io::ErrorKind::BrokenPipe),
            "must exit with BrokenPipe, got {:?}",
            write_error,
        );

        assert!(
            elapsed >= MIN_BROKEN_PIPE_ELAPSED,
            "BrokenPipe too early ({elapsed:?}); expected >= {:?}",
            MIN_BROKEN_PIPE_ELAPSED,
        );
        assert!(
            heartbeat_count >= MIN_POST_HOLE_WRITES,
            "too few post-hole heartbeats: {heartbeat_count} < {MIN_POST_HOLE_WRITES}"
        );
        assert!(
            pre_hole_frames + server_frames >= 1,
            "server should have observed at least one delivered frame"
        );

        let oversized_drops = c2s_after.dropped - c2s_before.dropped;
        assert!(
            oversized_drops > 0,
            "c2s must have dropped at least one oversized datagram; dropped={}",
            c2s_after.dropped,
        );

        let c2s_forwarded_post_hole = c2s_after.forwarded - c2s_before.forwarded;
        assert!(
            c2s_forwarded_post_hole > 0,
            "post-hole small-packet heartbeats must be forwarded c2s; forwarded={c2s_forwarded_post_hole}"
        );

        let reverse_traffic_packets = s2c_after.forwarded - s2c_before;
        assert!(
            reverse_traffic_packets > 0,
            "s2c must have forwarded reverse ACK/SACK traffic; forwarded={}",
            s2c_after.forwarded,
        );

        let reverse_silence = reverse_traffic.silence_at(termination_time);
        assert!(
            reverse_silence <= MAX_REVERSE_TRAFFIC_SILENCE,
            "reverse ACK/SACK traffic stale for {:?} (> {:?}); last s2c change at {:?}",
            reverse_silence,
            MAX_REVERSE_TRAFFIC_SILENCE,
            reverse_traffic.last_change,
        );

        eprintln!(
            "[rtp_liveness] c2s dropped={} forwarded={} | s2c forwarded={} reverse_silence={:?}",
            c2s_after.dropped,
            c2s_after.forwarded,
            s2c_after.forwarded,
            reverse_silence,
        );
    });
}

#[test]
#[ignore = "runs up to MAX_DURATION (65s) end-to-end; keep out of normal test builds (see module header)"]
fn rtp_permanent_hole_liveness_smoke() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut tasks = netem_test::kit::TestScope::new();
        let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
        let (
            write_error,
            post_hole_writes,
            c2s_after,
            s2c_after,
            elapsed,
            pre_hole_frames,
            s2c_before,
            c2s_before,
            max_duration,
            mut latency_rx,
        ) = tasks
            .run(async {
                let base = Instant::now();
                let start = Instant::now();

                let (server_addr, mut latency_rx) =
                    spawn_rtp_msg_latency_sink_via(&task_tx, false, base).await.unwrap();

                let c2s = NetemConfig {
                    max_datagram_size: MAX_DATAGRAM,
                    latency: OWD,
                    ..NetemConfig::default()
                };
                let s2c = NetemConfig {
                    latency: OWD,
                    ..NetemConfig::default()
                };
                let pair = NetemPair::spawn(server_addr, c2s, s2c).unwrap();

                let watchdog_tuning = rtp::WatchdogTuning::new(
                    1,
                    Duration::from_millis(1500),
                    Duration::from_millis(1500),
                    Duration::from_secs(3),
                );

                // The watchdog tuning is part of the connect config, so the
                // connection is opened inline (the `_via` connect helpers only
                // accept the default config); the supervisor keepalive is
                // submitted as REQUIRED through the handle, matching the
                // original `spawn_required` registration.
                let connected = rtp::udp::connect_with(
                    "0.0.0.0:0",
                    &pair.client_addr().to_string(),
                    rtp::udp::ConnectConfig {
                        handshake: false,
                        mss: rtp::udp::MssConfig::Custom(rtp::udp::NO_FEC_MSS),
                        watchdog: Some(watchdog_tuning),
                        ..rtp::udp::ConnectConfig::default()
                    },
                )
                .await
                .unwrap();

                let mut read = connected.read.into_async_read();
                let mut write = connected.write.into_async_write();
                // The supervisor owns the session drivers; poll it as a
                // non-required keepalive so a normal FIN shutdown does not
                // fail the test.
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let _ = connected.supervisor.await;
                    }),
                );

                // Keep the read half alive so ACKs keep flowing; parked until the
                // connection closes, so the owning JoinSet aborts it at scope end.
                submit_test_task(&task_tx, Box::pin(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    loop {
                        let n = read.read(&mut buf).await;
                        match n {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                    }
                }));

                let sent = send_timestamped_messages(
                    &mut write,
                    base,
                    MSG_BYTES,
                    MSG_INTERVAL,
                    Duration::from_millis(500),
                )
                .await;
                assert!(sent > 0, "initial handshake message must be delivered");
                tokio::time::sleep(Duration::from_secs(1)).await;

                let mut pre_hole_frames = 0u64;
                while latency_rx.try_recv().is_ok() {
                    pre_hole_frames += 1;
                }
                assert!(
                    pre_hole_frames > 0,
                    "server must have received the initial handshake frame"
                );

                let hole_payload = netem_test::kit::payload::payload(1024);
                let _ = write.write_all(&hole_payload).await;
                tokio::time::sleep(Duration::from_millis(500)).await;

                let s2c_before = pair.stats_s2c().forwarded;
                let c2s_before = pair.stats_c2s();

                let max_duration = Duration::from_secs(5);
                let post_hole_interval = Duration::from_millis(100);
                let msg = netem_test::kit::payload::payload(MSG_BYTES);

                let mut post_hole_writes = 0u64;
                let mut write_error: Option<std::io::ErrorKind> = None;
                let mut reverse_traffic = ReverseTrafficTracker::new(s2c_before, Instant::now());

                loop {
                    if start.elapsed() >= max_duration {
                        panic!(
                            "Connection stayed alive >{:?} without cumulative progress; \
                             delivered {} post-hole writes",
                            max_duration, post_hole_writes,
                        );
                    }

                    reverse_traffic.observe(pair.stats_s2c().forwarded, Instant::now());

                    let res =
                        tokio::time::timeout(Duration::from_secs(2), write.write_all(&msg)).await;
                    match res {
                        Ok(Ok(())) => {
                            post_hole_writes += 1;
                            tokio::time::sleep(post_hole_interval).await;
                        }
                        Ok(Err(e)) => {
                            write_error = Some(e.kind());
                            eprintln!(
                                "[rtp_liveness_smoke] write failed with {:?} after {} post-hole writes",
                                e.kind(),
                                post_hole_writes,
                            );
                            break;
                        }
                        Err(_) => {
                            eprintln!(
                                "[rtp_liveness_smoke] write timed out after {} post-hole writes",
                                post_hole_writes,
                            );
                            break;
                        }
                    }
                }

                let c2s_after = pair.stats_c2s();
                let s2c_after = pair.stats_s2c();
                pair.stop();
                let elapsed = start.elapsed();
                (
                    write_error,
                    post_hole_writes,
                    c2s_after,
                    s2c_after,
                    elapsed,
                    pre_hole_frames,
                    s2c_before,
                    c2s_before,
                    max_duration,
                    latency_rx,
                )
            })
            .await;

        let mut server_frames = 0u64;
        while latency_rx.try_recv().is_ok() {
            server_frames += 1;
        }
        eprintln!(
            "[rtp_liveness_smoke] elapsed={elapsed:?} post_hole_writes={post_hole_writes} \
         server_frames={server_frames} (pre_hole={pre_hole_frames})"
        );

        assert_eq!(
            write_error,
            Some(std::io::ErrorKind::BrokenPipe),
            "must exit with BrokenPipe, got {:?}",
            write_error,
        );

        assert!(
            elapsed <= max_duration,
            "BrokenPipe too late ({elapsed:?}); expected within {:?}",
            max_duration,
        );

        assert!(
            post_hole_writes >= 5,
            "too few post-hole writes: {post_hole_writes} < 5"
        );

        assert!(
            pre_hole_frames + server_frames >= 1,
            "server should have observed at least one delivered frame"
        );

        let oversized_drops = c2s_after.dropped - c2s_before.dropped;
        assert!(
            oversized_drops > 0,
            "c2s must have dropped at least one oversized datagram; dropped={}",
            c2s_after.dropped,
        );

        let reverse_traffic_packets = s2c_after.forwarded - s2c_before;
        assert!(
            reverse_traffic_packets > 0,
            "s2c must have forwarded reverse ACK/SACK traffic; forwarded={}",
            s2c_after.forwarded,
        );

        eprintln!(
            "[rtp_liveness_smoke] c2s dropped={} forwarded={} | s2c forwarded={}",
            c2s_after.dropped, c2s_after.forwarded, s2c_after.forwarded,
        );
    });
}

#[test]
fn reverse_traffic_recency_advances_only_on_new_packets() {
    let t0 = Instant::now();
    let mut tracker = ReverseTrafficTracker::new(7, t0);
    tracker.observe(8, t0 + Duration::from_secs(1));
    tracker.observe(8, t0 + Duration::from_secs(4));
    assert_eq!(
        tracker.silence_at(t0 + Duration::from_secs(5)),
        Duration::from_secs(4),
        "an unchanged counter must not refresh recency"
    );
    tracker.observe(9, t0 + Duration::from_secs(5));
    assert_eq!(
        tracker.silence_at(t0 + Duration::from_secs(5)),
        Duration::ZERO
    );
}
