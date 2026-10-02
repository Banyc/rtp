//! ACK-overhead measurement probe.
//!
//! Measures the receiver-side ACK stream of a **download-only** rtp session
//! (payload flows server -> client, so the reverse direction carries nothing
//! but ACKs) at the shipped `ack_feedback` policy: `ACK_FLUSH_COUNT = 8`,
//! `ACK_FLUSH_AGE = 3 ms`.
//!
//! It is report-only — an example, so it is outside every gate tier — and
//! prints machine-readable `ACK*` lines:
//!
//! * `ACKSIZE`  — the ACK datagram size distribution (min/p50/p90/p99/max)
//! * `ACKBLOCKS` — how many SACK blocks each ACK carried (0 / 1 / 2..4 / 5..8 / >8)
//! * `ACKFLUSH` — flush claims by reason, from the crate's own
//!   `MetricsEvent::AckFlush(reason)` stream (the same events
//!   `testkit::perf_trace` turns into `rtp_ack_flush_<reason>_claims`)
//! * `ACKOVERHEAD` — ACK datagrams per received data packet and ACK wire
//!   bytes per payload byte, from the recording transports and netem's own
//!   per-direction counters
//!
//! Run (from `crates/rtp_ack_ws`):
//!
//! ```sh
//! NETEM_PERF_TRACE_DIR=/tmp/ack-trace ACK_ARM=operator_obf ACK_OUT=/tmp/ack-out \
//!   cargo run --release --example ack_overhead --features testing
//! ```

#[cfg(feature = "testing")]
mod probe {
    use std::collections::BTreeMap;
    use std::io;
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use netem_test::kit::payload::payload;
    use netem_test::kit::{
        TEST_TASK_QUEUE_BOUND, TestScope, TestTaskSubmitter, submit_test_task,
        submit_test_task_required,
    };
    use netem_test::{Counters, LossModel, NetemConfig, NetemPair, StdUdpTransport, UdpTransport};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use rtp::metrics::MetricsObserver;
    use rtp::testkit::perf_trace::PerfTrace;

    const KEY: [u8; 32] = [7; 32];

    /// One recorded datagram as it was offered to the netem link. `is_ack`
    /// and `block_count` are parsed only when the datagram is plaintext
    /// (obfuscation off); an obfuscated frame is opaque at this layer.
    #[derive(Debug, Clone, Copy)]
    struct Datagram {
        len: usize,
        is_ack: bool,
        block_count: Option<u8>,
        /// First wire byte: the codec command when the frame is plaintext.
        cmd: u8,
    }

    /// A transport wrapper recording every received datagram's size and,
    /// when plaintext, its ACK header. Used on both netem legs so the two
    /// directions are separable by which leg recorded them.
    struct RecordingTransport {
        inner: Box<dyn UdpTransport>,
        seen: Arc<Mutex<Vec<Datagram>>>,
        parse_ack: bool,
        /// This leg faces the client, so in a download the datagrams it
        /// receives are ACKs. Used when the frame is obfuscated and the
        /// header cannot be parsed.
        client_leg: bool,
    }

    impl UdpTransport for RecordingTransport {
        fn connect_peer(&self, peer: SocketAddr) -> io::Result<()> {
            self.inner.connect_peer(peer)
        }
        fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            let (n, from) = self.inner.recv_from(buf)?;
            self.record(&buf[..n]);
            Ok((n, from))
        }
        fn recv_from_timeout(
            &self,
            buf: &mut [u8],
            timeout: Duration,
        ) -> io::Result<(usize, SocketAddr)> {
            let (n, from) = self.inner.recv_from_timeout(buf, timeout)?;
            self.record(&buf[..n]);
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

    impl RecordingTransport {
        fn record(&self, buf: &[u8]) {
            let (is_ack, block_count) = if self.parse_ack {
                // ACK wire layout with `handshake: false` (no session tag):
                // ACK_CMD=0 | next u64 | count u8 | count * 16 bytes | echo_ts.
                if buf.first() == Some(&0) && buf.len() >= 10 {
                    (true, Some(buf[9]))
                } else {
                    (false, None)
                }
            } else {
                // Obfuscated frame: the header is opaque, but the client leg
                // of a download-only session carries nothing but ACKs (and
                // the one opening request byte).
                (self.client_leg, None)
            };
            self.seen.lock().unwrap().push(Datagram {
                len: buf.len(),
                is_ack,
                block_count,
                cmd: buf.first().copied().unwrap_or(255),
            });
        }
    }

    /// One measurement arm: the two impairment directions, the offered
    /// payload, the application pacing (the shaper whose *input* rate sets
    /// the data inter-arrival the ACK policy sees), and whether the deployed
    /// obfuscation is on.
    struct Arm {
        name: &'static str,
        c2s: NetemConfig,
        s2c: NetemConfig,
        bytes: usize,
        obfuscated: bool,
        /// Application offer rate in bytes/s; `None` offers as fast as the
        /// transport allows.
        pace: Option<u64>,
    }

    fn base(seed: u64) -> NetemConfig {
        NetemConfig {
            seed,
            queue_limit_pkts: 2048,
            ..NetemConfig::default()
        }
    }

    /// The operator's data inter-arrival: ~1.4 MB/s offered, i.e. about one
    /// MSS-sized packet every millisecond at the rtp layer. Deadline-paced so
    /// the OS timer's granularity cannot stretch the offer rate.
    fn operator_pace() -> Option<u64> {
        Some(1_400_000)
    }

    fn arm(name: &'static str) -> Arm {
        let obf = name.ends_with("_obf");
        match name.trim_end_matches("_obf") {
            // No impairment, unpaced: the Count (8-arrival) trigger should
            // dominate.
            "clean_fast" => Arm {
                name,
                c2s: base(11),
                s2c: base(12),
                bytes: 4 << 20,
                obfuscated: obf,
                pace: None,
            },
            // ~1 ms data inter-arrival: the Age (3 ms) trigger should
            // dominate, matching the operator's panel.
            "clean_paced" => Arm {
                name,
                c2s: NetemConfig {
                    latency: Duration::from_millis(5),
                    ..base(21)
                },
                s2c: NetemConfig {
                    latency: Duration::from_millis(5),
                    ..base(22)
                },
                bytes: 4 << 20,
                obfuscated: obf,
                pace: operator_pace(),
            },
            // 5% iid loss, paced: the SACK-block path is exercised at the
            // operator's inter-arrival.
            "loss_iid" => Arm {
                name,
                c2s: NetemConfig {
                    loss: u32::MAX / 20,
                    latency: Duration::from_millis(10),
                    jitter: Duration::from_millis(5),
                    ..base(31)
                },
                s2c: NetemConfig {
                    loss: u32::MAX / 20,
                    latency: Duration::from_millis(10),
                    jitter: Duration::from_millis(5),
                    ..base(32)
                },
                bytes: 4 << 20,
                obfuscated: obf,
                pace: operator_pace(),
            },
            // The operator's shape: ~190 ms RTT floor, jitter, ~2% loss,
            // ~1 ms data inter-arrival, download only.
            "operator" => Arm {
                name,
                c2s: NetemConfig {
                    latency: Duration::from_millis(95),
                    jitter: Duration::from_millis(20),
                    ..base(41)
                },
                s2c: NetemConfig {
                    latency: Duration::from_millis(95),
                    jitter: Duration::from_millis(20),
                    loss: u32::MAX / 50,
                    ..base(42)
                },
                bytes: 4 << 20,
                obfuscated: obf,
                pace: operator_pace(),
            },
            // 15% iid loss, unpaced: the receive history should accumulate
            // more than one page of SACK intervals, exercising the deep-page
            // second datagram.
            "deep_loss" => Arm {
                name,
                c2s: base(61),
                s2c: NetemConfig {
                    loss: (u32::MAX as f64 * 0.15) as u32,
                    latency: Duration::from_millis(25),
                    ..base(62)
                },
                bytes: 4 << 20,
                obfuscated: obf,
                pace: None,
            },
            // 40% iid loss, unpaced: enough simultaneous holes to exceed the
            // 64-block head page and force the deep page out as a second
            // datagram (the page-inflation worst case).
            "deep_hi" => Arm {
                name,
                c2s: base(71),
                s2c: NetemConfig {
                    loss: (u32::MAX as f64 * 0.40) as u32,
                    latency: Duration::from_millis(25),
                    ..base(72)
                },
                bytes: 4 << 20,
                obfuscated: obf,
                pace: None,
            },
            // Gilbert-Elliot bursts, paced: deep holes, many SACK blocks per
            // ACK.
            "ge_burst" => Arm {
                name,
                c2s: NetemConfig {
                    latency: Duration::from_millis(25),
                    ..base(51)
                },
                s2c: NetemConfig {
                    latency: Duration::from_millis(25),
                    loss_model: gilbert_elliott(5.0, 8.0),
                    ..base(52)
                },
                bytes: 4 << 20,
                obfuscated: obf,
                pace: operator_pace(),
            },
            other => panic!(
                "unknown ACK_ARM {other:?} (clean_fast|clean_paced|loss_iid|ge_burst|operator|deep_loss|deep_hi, optional _obf suffix)"
            ),
        }
    }

    /// The harness's Gilbert-Elliot four-state loss model (same constructor
    /// the impairment presets use).
    fn gilbert_elliott(loss_pct: f64, mean_burst_len: f64) -> LossModel {
        netem_test::kit::presets::gilbert_elliott_loss(loss_pct, mean_burst_len)
    }

    /// Spawn a server that writes `bytes` of payload to the first accepted
    /// rtp connection and then half-closes. The accept observer is the
    /// peer-side PerfTrace capture.
    async fn spawn_download_server(
        tx: &TestTaskSubmitter,
        observer: Option<MetricsObserver>,
        bytes: usize,
        obfuscated: bool,
        pace: Option<u64>,
    ) -> io::Result<SocketAddr> {
        let listener = rtp::udp::Listener::bind(
            "127.0.0.1:0",
            rtp::udp::ListenerConfig {
                obfuscation_key: obfuscated.then_some(KEY),
                ..rtp::udp::ListenerConfig::default()
            },
        )
        .await?;
        let addr = listener.local_addr();
        let listener = Arc::new(listener);
        let listener_for_drain = Arc::clone(&listener);
        let server_tx = tx.clone();
        let keepalive_submitter = server_tx.clone();
        submit_test_task_required(
            &server_tx,
            "download server",
            Box::pin(async move {
                // Optional sender-side pacer seed (packets/s) so a probe can
                // inflate the in-flight window and force the receive
                // history past the 64-block head page.
                let seed_rate: Option<f64> = std::env::var("ACK_SEED_RATE")
                    .ok()
                    .and_then(|value| value.parse().ok());
                let accepted = listener
                    .accept_without_handshake_with(rtp::udp::AcceptConfig {
                        obfuscation_key: obfuscated.then_some(KEY),
                        metrics_observer: observer,
                        initial_send_rate: seed_rate,
                        ..rtp::udp::AcceptConfig::default()
                    })
                    .await
                    .unwrap();
                // The extra-accept drainer keeps udp_listener's dispatcher
                // forwarding datagrams to the accepted connection.
                let drainer = async move {
                    loop {
                        listener_for_drain
                            .accept_without_handshake_with(rtp::udp::AcceptConfig {
                                obfuscation_key: obfuscated.then_some(KEY),
                                ..rtp::udp::AcceptConfig::default()
                            })
                            .await
                            .unwrap();
                    }
                };
                tokio::pin!(drainer);
                let mut read = accepted.read.into_async_read();
                let mut write = accepted.write.into_async_write();
                let supervisor = accepted.supervisor;
                tokio::pin!(supervisor);
                // The peer's ACKs (and its opening request byte) are
                // processed by polling this connection's read half, so it
                // must be driven for the whole download. A spawned
                // non-required keepalive drives it while the write loop
                // sends.
                submit_test_task(
                    &keepalive_submitter,
                    Box::pin(async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        loop {
                            match read.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => {}
                            }
                        }
                    }),
                );
                let body = payload(bytes);
                // Offer at most one sub-MSS chunk per write (1200 bytes fits
                // both the plain and the obfuscated MSS, so a write never
                // splits into a large packet plus a tiny remainder), and hold
                // the cumulative byte count to the pacing deadline so a
                // coarse OS timer cannot stretch the offered rate.
                let chunk = 1200usize;
                let started = Instant::now();
                let mut sent = 0usize;
                while sent < body.len() {
                    let end = (sent + chunk).min(body.len());
                    tokio::select! {
                        () = &mut supervisor => return,
                        () = &mut drainer => {
                            panic!("accept drainer finished before the scenario completed");
                        }
                        result = write.write_all(&body[sent..end]) => {
                            if result.is_err() {
                                return;
                            }
                            sent = end;
                        }
                    }
                    if let Some(bps) = pace {
                        let target = Duration::from_secs_f64(sent as f64 / bps.max(1) as f64);
                        let now = started.elapsed();
                        if target > now {
                            tokio::select! {
                                () = &mut supervisor => return,
                                () = &mut drainer => {
                                    panic!("accept drainer finished before the scenario completed");
                                }
                                () = tokio::time::sleep(target - now) => {}
                            }
                        }
                    }
                }
                let _ = write.shutdown().await;
                // Hold the session until the probe ends; the scope aborts it.
                std::future::pending::<()>().await
            }),
        );
        Ok(addr)
    }

    struct SizeStats {
        min: usize,
        p50: usize,
        p90: usize,
        p99: usize,
        max: usize,
        blocks: BTreeMap<u8, usize>,
        total: usize,
        bytes: u64,
    }

    fn percentile(sorted: &[usize], q: f64) -> usize {
        if sorted.is_empty() {
            return 0;
        }
        let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
        sorted[idx]
    }

    fn size_stats(datagrams: &[Datagram], only_acks: bool) -> SizeStats {
        let mut sizes: Vec<usize> = datagrams
            .iter()
            .filter(|d| !only_acks || d.is_ack)
            .map(|d| d.len)
            .collect();
        let mut blocks: BTreeMap<u8, usize> = BTreeMap::new();
        for d in datagrams.iter().filter(|d| !only_acks || d.is_ack) {
            if let Some(c) = d.block_count {
                *blocks.entry(c).or_insert(0) += 1;
            }
        }
        sizes.sort_unstable();
        SizeStats {
            min: sizes.first().copied().unwrap_or(0),
            p50: percentile(&sizes, 0.50),
            p90: percentile(&sizes, 0.90),
            p99: percentile(&sizes, 0.99),
            max: sizes.last().copied().unwrap_or(0),
            blocks,
            total: sizes.len(),
            bytes: sizes.iter().map(|&s| s as u64).sum(),
        }
    }

    fn stat_line(tag: &str, name: &str, key: &str, value: impl std::fmt::Display) {
        println!("{tag} arm={name} {key} = {value}");
    }

    /// Read the `key,value` manifest PerfTrace writes.
    fn manifest_value(dir: &Path, key: &str) -> Option<String> {
        let text = std::fs::read_to_string(dir.join("manifest.csv")).ok()?;
        for line in text.lines().skip(1) {
            if let Some((k, v)) = line.split_once(',') {
                // PerfTrace's manifest quotes every field.
                let k = k.trim_matches('"');
                if k == key {
                    return Some(v.trim_matches('"').to_string());
                }
            }
        }
        None
    }

    /// Last non-empty value of a named column in a PerfTrace CSV, as f64.
    fn csv_last_column(dir: &Path, file: &str, column: &str) -> Option<f64> {
        let text = std::fs::read_to_string(dir.join(file)).ok()?;
        let mut lines = text.lines();
        let header: Vec<&str> = lines.next()?.split(',').collect();
        let idx = header.iter().position(|h| *h == column)?;
        let mut last = None;
        for line in lines {
            let fields: Vec<&str> = line.split(',').collect();
            if let Some(value) = fields.get(idx)
                && !value.is_empty()
            {
                last = value.parse::<f64>().ok();
            }
        }
        last
    }

    fn counters(tag: &str, name: &str, dir: &str, c: &Counters) {
        stat_line(tag, name, &format!("{dir}_received_pkts"), c.received);
        stat_line(tag, name, &format!("{dir}_forwarded_pkts"), c.forwarded);
        stat_line(
            tag,
            name,
            &format!("{dir}_forwarded_bytes"),
            c.forwarded_bytes,
        );
        stat_line(tag, name, &format!("{dir}_dropped"), c.dropped);
        stat_line(tag, name, &format!("{dir}_duplicated"), c.duplicated);
        stat_line(tag, name, &format!("{dir}_reordered"), c.reordered);
        stat_line(
            tag,
            name,
            &format!("{dir}_overflow_dropped"),
            c.overflow_dropped,
        );
    }

    pub async fn run() {
        let name: &'static str = std::env::var("ACK_ARM")
            .ok()
            .map(|s| Box::leak(s.into_boxed_str()) as &'static str)
            .unwrap_or("operator");
        let arm = arm(name);
        let out_dir = PathBuf::from(
            std::env::var("ACK_OUT").unwrap_or_else(|_| "/tmp/ack-overhead".to_string()),
        );
        std::fs::create_dir_all(&out_dir).expect("create ACK_OUT");
        // PerfTrace writes its own manifest holding the AckFlush reason
        // counters; the trace dir defaults under ACK_OUT when unset.
        if std::env::var_os("NETEM_PERF_TRACE_DIR").is_none() {
            // SAFETY: single-threaded setup, before any thread is spawned.
            unsafe { std::env::set_var("NETEM_PERF_TRACE_DIR", &out_dir) };
        }
        let trace_dir = PathBuf::from(std::env::var("NETEM_PERF_TRACE_DIR").unwrap());

        let mut trace = PerfTrace::from_env();
        let client_observer = trace.as_ref().and_then(PerfTrace::rtp_observer);
        let peer_observer = trace.as_ref().and_then(PerfTrace::rtp_peer_observer);

        let c2s = Arc::new(Mutex::new(Vec::new()));
        let s2c = Arc::new(Mutex::new(Vec::new()));
        let c2s_rec = Arc::clone(&c2s);
        let s2c_rec = Arc::clone(&s2c);
        let parse = !arm.obfuscated;

        let mut tasks = TestScope::new();
        let task_tx = tasks.submitter(TEST_TASK_QUEUE_BOUND);
        let trace_dir_for_netem = trace_dir.clone();
        let result = tasks
            .run(async move {
                let server_addr = spawn_download_server(
                    &task_tx,
                    peer_observer,
                    arm.bytes,
                    arm.obfuscated,
                    arm.pace,
                )
                .await
                .unwrap();

                let client_transport = Box::new(RecordingTransport {
                    inner: Box::new(StdUdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap()),
                    seen: c2s_rec,
                    parse_ack: parse,
                    client_leg: true,
                });
                let server_transport = Box::new(RecordingTransport {
                    inner: Box::new(StdUdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap()),
                    seen: s2c_rec,
                    parse_ack: false,
                    client_leg: false,
                });
                let pair = NetemPair::spawn_with_transports(
                    server_addr,
                    arm.c2s,
                    arm.s2c,
                    client_transport,
                    server_transport,
                )
                .unwrap();
                let pair = Arc::new(pair);

                let connected = rtp::udp::connect_with(
                    "0.0.0.0:0",
                    &pair.client_addr().to_string(),
                    rtp::udp::ConnectConfig {
                        handshake: false,
                        obfuscation_key: arm.obfuscated.then_some(KEY),
                        metrics_observer: client_observer,
                        ..rtp::udp::ConnectConfig::default()
                    },
                )
                .await
                .unwrap();
                let mut read = connected.read.into_async_read();
                let mut write = connected.write.into_async_write();
                let supervisor = connected.supervisor;
                submit_test_task(
                    &task_tx,
                    Box::pin(async move {
                        let _ = supervisor.await;
                    }),
                );
                // A download-only session cannot establish the 4-tuple with
                // nothing in the reverse direction: `handshake: false`
                // accepts on the peer's first datagram, so the client sends
                // one request byte and then only receives and ACKs. The
                // write half stays alive (held) so the stream is not
                // half-closed under the download.
                write.write_all(b"R").await.unwrap();

                let transfer_start = Instant::now();
                let mut buf = vec![0u8; 64 * 1024];
                let mut got = 0usize;
                // A download with no application traffic in the reverse
                // direction: the client only reads and ACKs.
                while got < arm.bytes {
                    match read.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => got += n,
                    }
                }
                let transfer = transfer_start.elapsed();
                pair.stop();
                let c2s_stats = pair.stats_c2s();
                let s2c_stats = pair.stats_s2c();
                (got, transfer, c2s_stats, s2c_stats)
            })
            .await;
        let (got, transfer, c2s_stats, s2c_stats) = result;

        let mut finalize = || -> io::Result<PathBuf> {
            match trace.take() {
                Some(t) => t.finish(&[
                    ("arm", arm.name.to_string()),
                    ("payload_bytes", arm.bytes.to_string()),
                ]),
                None => Ok(trace_dir_for_netem.clone()),
            }
        };
        let trace_dir = finalize().expect("finish PerfTrace");

        // ---- report -----------------------------------------------------
        let c2s_seen = c2s.lock().unwrap().clone();
        let s2c_seen = s2c.lock().unwrap().clone();
        let ack_stats = size_stats(&c2s_seen, true);
        let data_stats = size_stats(&s2c_seen, false);

        stat_line("ACKOVERHEAD", name, "payload_bytes", arm.bytes);
        stat_line("ACKOVERHEAD", name, "delivered_bytes", got);
        stat_line("ACKOVERHEAD", name, "obfuscated", arm.obfuscated);
        stat_line("ACKOVERHEAD", name, "transfer_ms", transfer.as_millis());
        stat_line("ACKOVERHEAD", name, "data_pkts", data_stats.total);
        stat_line("ACKOVERHEAD", name, "data_bytes", data_stats.bytes);
        stat_line("ACKOVERHEAD", name, "data_size_min", data_stats.min);
        stat_line("ACKOVERHEAD", name, "data_size_p50", data_stats.p50);
        stat_line("ACKOVERHEAD", name, "data_size_p99", data_stats.p99);
        stat_line("ACKOVERHEAD", name, "data_size_max", data_stats.max);
        stat_line("ACKOVERHEAD", name, "ack_pkts", ack_stats.total);
        stat_line("ACKOVERHEAD", name, "ack_bytes", ack_stats.bytes);
        stat_line(
            "ACKOVERHEAD",
            name,
            "ack_pkts_per_data_pkt",
            format!(
                "{:.4}",
                ack_stats.total as f64 / data_stats.total.max(1) as f64
            ),
        );
        stat_line(
            "ACKOVERHEAD",
            name,
            "ack_bytes_per_data_byte",
            format!(
                "{:.6}",
                ack_stats.bytes as f64 / data_stats.bytes.max(1) as f64
            ),
        );
        // More ACK datagrams than flush claims means a claim emitted both its
        // head page and its deep page as separate datagrams.
        let claims: u64 = ["initial", "age", "count", "fin", "explicit"]
            .iter()
            .filter_map(|r| manifest_value(&trace_dir, &format!("rtp_ack_flush_{r}_claims")))
            .filter_map(|v| v.parse::<u64>().ok())
            .sum();
        stat_line("ACKOVERHEAD", name, "ack_claims", claims);
        if claims > 0 {
            stat_line(
                "ACKOVERHEAD",
                name,
                "ack_datagrams_per_claim",
                format!("{:.4}", ack_stats.total as f64 / claims as f64),
            );
        }
        stat_line(
            "ACKOVERHEAD",
            name,
            "data_pps",
            format!(
                "{:.1}",
                data_stats.total as f64 / transfer.as_secs_f64().max(1e-9)
            ),
        );
        stat_line(
            "ACKOVERHEAD",
            name,
            "data_interarrival_us",
            format!(
                "{:.1}",
                transfer.as_secs_f64() * 1e6 / data_stats.total.max(1) as f64
            ),
        );

        stat_line("ACKSIZE", name, "n", ack_stats.total);
        stat_line("ACKSIZE", name, "min", ack_stats.min);
        stat_line("ACKSIZE", name, "p50", ack_stats.p50);
        stat_line("ACKSIZE", name, "p90", ack_stats.p90);
        stat_line("ACKSIZE", name, "p99", ack_stats.p99);
        stat_line("ACKSIZE", name, "max", ack_stats.max);
        stat_line("ACKSIZE", name, "obfuscated", arm.obfuscated);
        let mut b0 = 0usize;
        let mut b1 = 0usize;
        let mut b2_4 = 0usize;
        let mut b5_8 = 0usize;
        let mut bgt8 = 0usize;
        for (count, n) in &ack_stats.blocks {
            match count {
                0 => b0 += *n,
                1 => b1 += *n,
                2..=4 => b2_4 += *n,
                5..=8 => b5_8 += *n,
                _ => bgt8 += *n,
            }
        }
        if !arm.obfuscated {
            stat_line(
                "ACKBLOCKS",
                name,
                "parsed",
                ack_stats.blocks.values().sum::<usize>(),
            );
            stat_line("ACKBLOCKS", name, "blocks0", b0);
            stat_line("ACKBLOCKS", name, "blocks1", b1);
            stat_line("ACKBLOCKS", name, "blocks2to4", b2_4);
            stat_line("ACKBLOCKS", name, "blocks5to8", b5_8);
            stat_line("ACKBLOCKS", name, "blocksgt8", bgt8);
        }

        for reason in ["initial", "age", "count", "fin", "explicit"] {
            if let Some(v) = manifest_value(&trace_dir, &format!("rtp_ack_flush_{reason}_claims")) {
                stat_line("ACKFLUSH", name, &format!("client_{reason}"), v);
            }
            if let Some(v) =
                manifest_value(&trace_dir, &format!("rtp_peer_ack_flush_{reason}_claims"))
            {
                stat_line("ACKFLUSH", name, &format!("peer_{reason}"), v);
            }
        }
        stat_line("ACKFLUSH", name, "client_total", claims);

        counters("ACKNETEM", name, "c2s", &c2s_stats);
        counters("ACKNETEM", name, "s2c", &s2c_stats);

        if let Some(v) = csv_last_column(&trace_dir, "rtp_peer.csv", "retransmission_attempts") {
            stat_line("ACKPATH", name, "peer_retransmission_attempts", v);
        }
        if let Some(v) = csv_last_column(&trace_dir, "rtp_peer.csv", "loss_ratio") {
            stat_line("ACKPATH", name, "peer_loss_ratio", v);
        }

        // Raw sizes for the distribution evidence.
        let mut csv = String::from("len,is_ack,block_count\n");
        for d in c2s_seen.iter() {
            csv.push_str(&format!(
                "{},{},{}\n",
                d.len,
                d.is_ack,
                d.block_count.map(|b| b.to_string()).unwrap_or_default()
            ));
        }
        let _ = std::fs::write(out_dir.join(format!("sizes_{name}.csv")), csv);
        // The server->client size distribution, for the same evidence on the
        // data side (is this the direction the panel is measuring?).
        let mut csv = String::from("len,is_ack,cmd\n");
        for d in s2c_seen.iter() {
            csv.push_str(&format!("{},{},{}\n", d.len, d.is_ack, d.cmd));
        }
        let _ = std::fs::write(out_dir.join(format!("sizes_s2c_{name}.csv")), csv);
        // Server->client command histogram (plaintext only): what is the
        // data direction actually made of?
        if !arm.obfuscated {
            let mut cmds: BTreeMap<u8, usize> = BTreeMap::new();
            for d in s2c_seen.iter() {
                *cmds.entry(d.cmd).or_insert(0) += 1;
            }
            for (cmd, n) in cmds {
                stat_line("ACKS2C", name, &format!("cmd{cmd}_pkts"), n);
            }
        }
        eprintln!(
            "ack_overhead: arm={name} delivered={got}/{}B in {}ms; trace={}",
            arm.bytes,
            transfer.as_millis(),
            trace_dir.display()
        );
    }
}

#[cfg(feature = "testing")]
#[tokio::main]
async fn main() {
    probe::run().await;
}

#[cfg(not(feature = "testing"))]
fn main() {
    eprintln!("build/run with --features testing");
}
