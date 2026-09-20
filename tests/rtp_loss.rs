//! `rtp` loss-recovery scenarios through [`netem_test::NetemPair`].
//!
//! Verifies the reliable UDP layer recovers from mild bidirectional packet
//! loss introduced by the proxy, both for a 400 KiB loss payload, and that
//! the proxy reports non-zero drops.
//!
//! Runs in the default gate (seeded, sub-second):
//!
//! ```sh
//! cargo test -p rtp --test rtp_loss
//! ```

use std::time::Duration;

use netem_test::NetemPair;
use netem_test::kit::payload::{payload, with_timeout};
use netem_test::kit::presets::mild_loss;
use netem_test::kit::stats::combined_stats;
use rtp::testkit::rtp::{rtp_connect_via, rtp_echo_payload, spawn_rtp_echo_server_via};

/// `rtp`'s reliable layer should recover from mild packet loss introduced by
/// the netem proxy — a 400 KiB byte stream must arrive intact despite ~5%
/// loss in both directions.
#[tokio::test(flavor = "multi_thread")]
async fn rtp_over_netem_survives_mild_loss_400kib() {
    let mut tasks = netem_test::kit::TestScope::new();
    let task_tx = tasks.submitter(netem_test::kit::TEST_TASK_QUEUE_BOUND);
    let stats = tasks
        .run(async {
            let server_addr = spawn_rtp_echo_server_via(&task_tx, false).await.unwrap();

            let pair = NetemPair::spawn(server_addr, mild_loss(), mild_loss()).unwrap();
            let (read, write) = rtp_connect_via(&task_tx, pair.client_addr(), false).await;

            let payload = payload(400 * 1024);
            let got = with_timeout(
                Duration::from_secs(60),
                "rtp lossy 400KiB echo",
                rtp_echo_payload(read, write, &payload),
            )
            .await;
            assert_eq!(got, payload, "reliable layer must recover all 400KiB");

            pair.stop();
            combined_stats(&pair)
        })
        .await;
    assert!(
        stats.dropped > 0,
        "proxy should have dropped some packets, got {stats:?}"
    );
}
