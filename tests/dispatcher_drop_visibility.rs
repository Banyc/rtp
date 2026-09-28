//! The listener's dispatcher-drop count must be readable from outside `rtp`.
//!
//! The harness talks to `rtp`, not to `udp_listener`, so a counter the
//! listener exposes only inside the crate is invisible to every performance
//! arm — and a datagram the dispatcher drops during an overload is exactly the
//! loss the transport cannot attribute. This arm drives the overflow through
//! the public accept API from a *different* crate and reads the counter by
//! naming `rtp`'s own re-export (`rtp::udp::ListenerStats`), which is the path
//! the harness uses. Its property is the crate boundary rather than the exact
//! channel arithmetic: an in-crate test cannot prove the harness can name the
//! type or call the accessor at all.
//!
//! The overflow is driven by holding each accepted flow's future unpolled, so
//! its receive channel is never drained, while a raw source offers a burst
//! larger than that channel.

use std::{sync::Arc, time::Duration};

use rtp::udp::{AcceptConfig, AcceptTask, Listener, ListenerConfig, ListenerStats};

#[tokio::test(flavor = "multi_thread")]
async fn the_dispatcher_drop_count_is_readable_from_outside_rtp() {
    let listener = Arc::new(
        Listener::bind("127.0.0.1:0", ListenerConfig::default())
            .await
            .unwrap(),
    );
    let addr = listener.local_addr();

    let dispatcher_listener = Arc::clone(&listener);
    let mut dispatcher = tokio::task::JoinSet::new();
    dispatcher.spawn(async move {
        let mut held: Vec<AcceptTask> = Vec::new();
        while let Ok(accepted) = dispatcher_listener
            .accept_with(AcceptConfig::default())
            .await
        {
            held.push(accepted);
        }
    });

    let source = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    source.connect(addr).await.unwrap();
    const OFFERED: u64 = 2048;
    for sent in 0..OFFERED {
        source.send(&[0u8; 64]).await.unwrap();
        if sent % 128 == 127 {
            tokio::task::yield_now().await;
        }
    }

    let stats: &ListenerStats = listener.stats();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let accounted = stats
                .packets_dispatched
                .load(std::sync::atomic::Ordering::Relaxed)
                + stats
                    .packets_dropped_dispatcher_full
                    .load(std::sync::atomic::Ordering::Relaxed)
                + stats
                    .packets_dropped_rejected
                    .load(std::sync::atomic::Ordering::Relaxed)
                + stats
                    .packets_dropped_existing_only
                    .load(std::sync::atomic::Ordering::Relaxed)
                + stats
                    .packets_dropped_pkt_buf_overflow
                    .load(std::sync::atomic::Ordering::Relaxed);
            if accounted >= OFFERED {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the offered burst was never fully accounted by the dispatcher");

    use std::sync::atomic::Ordering::Relaxed;
    let received = stats.packets_received.load(Relaxed);
    let delivered = stats.packets_dispatched.load(Relaxed);
    let dropped = stats.packets_dropped_dispatcher_full.load(Relaxed);
    println!(
        "DISPATCH_OVERFLOW_STATS_CROSS_CRATE offered={OFFERED} received={received} \
         delivered={delivered} dropped_dispatcher_full={dropped}"
    );
    assert_eq!(
        delivered
            + dropped
            + stats.packets_dropped_rejected.load(Relaxed)
            + stats.packets_dropped_existing_only.load(Relaxed)
            + stats.packets_dropped_pkt_buf_overflow.load(Relaxed),
        received,
        "every datagram the dispatcher received must be either delivered or dropped by one \
         counted reason"
    );
    assert!(
        dropped > 0,
        "a burst of {OFFERED} datagrams into an undrained flow's channel must overflow: the \
         drop count read from outside `rtp` is {dropped}"
    );
    dispatcher.abort_all();
}
