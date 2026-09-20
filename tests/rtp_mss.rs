//! `rtp` MSS (maximum segment size) scenarios through [`netem_test::NetemPair`].
//!
//! Verifies that the new `_with_mss()` API in `rtp::udp` correctly negotiates
//! custom datagram sizes and still delivers byte streams intact over the
//! impairment proxy. Small MSS values make loss and latency more stressful
//! because the reliable layer must ACK/segment many more packets.
//!
//! Runs in the default gate (seeded, sub-second):
//!
//! ```sh
//! cargo test -p rtp --test rtp_mss
//! ```

use std::time::Duration;

use netem_test::NetemPair;
use netem_test::kit::payload::{payload, with_timeout};
use netem_test::kit::presets::{clean, mild_loss};
use netem_test::kit::stats::combined_stats;
use rtp::testkit::rtp::{rtp_connect_with_mss_via, spawn_rtp_echo_server_with_mss_via};

/// A small non-default MSS (512 bytes) should still deliver a small payload
/// over a clean netem link.
#[tokio::test(flavor = "multi_thread")]
async fn rtp_small_mss_clean_link_delivers_data() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let mss = 512;
            let server_addr = spawn_rtp_echo_server_with_mss_via(&task_tx, false, mss)
                .await
                .unwrap();

            let pair = NetemPair::spawn(server_addr, clean(), clean()).unwrap();
            let (read, write) =
                rtp_connect_with_mss_via(&task_tx, pair.client_addr(), false, mss).await;

            let payload = b"netem-rtp-small-mss";
            let got = with_timeout(
                Duration::from_secs(10),
                "rtp small-mss clean echo",
                rtp::testkit::rtp::rtp_echo_payload(read, write, payload),
            )
            .await;
            assert_eq!(got, payload);

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    assert_eq!(stats.dropped, 0, "clean link should not drop");
    assert!(stats.forwarded > 0, "proxy should forward packets");
}

/// A very small MSS (256 bytes) should recover from mild loss on a 100 KiB
/// transfer, exercising many more segments than the default MSS.
#[tokio::test(flavor = "multi_thread")]
async fn rtp_tiny_mss_survives_mild_loss() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let mss = 256;
            let server_addr = spawn_rtp_echo_server_with_mss_via(&task_tx, false, mss)
                .await
                .unwrap();

            let pair = NetemPair::spawn(server_addr, mild_loss(), mild_loss()).unwrap();
            let (read, write) =
                rtp_connect_with_mss_via(&task_tx, pair.client_addr(), false, mss).await;

            let payload = payload(100 * 1024);
            let got = with_timeout(
                Duration::from_secs(60),
                "rtp tiny-mss lossy 100KiB echo",
                rtp::testkit::rtp::rtp_echo_payload(read, write, &payload),
            )
            .await;
            assert_eq!(got, payload, "reliable layer must recover all 100KiB");

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    assert!(
        stats.dropped > 0,
        "proxy should have dropped some packets, got {stats:?}"
    );
}

/// A custom MSS (1024 bytes) mid-way between tiny and default should deliver
/// a 200 KiB payload intact over a clean link.
#[tokio::test(flavor = "multi_thread")]
async fn rtp_custom_mss_clean_link_delivers_200kib() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let mss = 1024;
            let server_addr = spawn_rtp_echo_server_with_mss_via(&task_tx, false, mss)
                .await
                .unwrap();

            let pair = NetemPair::spawn(server_addr, clean(), clean()).unwrap();
            let (read, write) =
                rtp_connect_with_mss_via(&task_tx, pair.client_addr(), false, mss).await;

            let payload = payload(200 * 1024);
            let got = with_timeout(
                Duration::from_secs(30),
                "rtp custom-mss clean 200KiB echo",
                rtp::testkit::rtp::rtp_echo_payload(read, write, &payload),
            )
            .await;
            assert_eq!(
                got, payload,
                "custom-mss clean 200KiB delivery must be byte-exact"
            );

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    assert_eq!(
        stats.dropped, 0,
        "clean link should not drop, got {stats:?}"
    );
    assert!(stats.forwarded > 0, "proxy should forward packets");
}
