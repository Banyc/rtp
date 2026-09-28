//! Interactive/bulk strict-priority arbitration on one NIC.
//!
//! This is the kernel fair-queueing idea applied one level up, reduced to the
//! one decision the product needs: when two connections' datagrams are ready
//! to leave the same device, the interactive one goes first.
//!
//! The unit of contention on the sender is the **NIC**, not the mux connection
//! and not the destination. When bulk traffic is spread across several
//! connections (to avoid saturating any one policed destination, or for load
//! balancing), those connections share one egress device, and no per-connection
//! scheduler can order an interactive datagram against a bulk one there. So the
//! operator constructs one [`NicScheduler`] per NIC (or several independent
//! ones) and hands the **same** instance to every connection that egresses it;
//! each connection wraps its own send path in [`NicWrite`] under a [`Class`].
//!
//! The invariant is the one the product's interactive-latency mandate needs:
//!
//! > an interactive datagram is never delayed behind a bulk datagram on the
//! > NIC the arbiter governs — beyond the one bulk datagram already in flight.
//!
//! # It decides order, not rate
//!
//! There is **no rate, no credit, no capacity and no reserve**. The arbiter
//! never limits throughput and needs to know nothing about the link: it only
//! decides which class's datagram is emitted first when both are ready. If no
//! interactive traffic is in flight, bulk is unthrottled; backpressure comes
//! from the socket buffer and the connection's own transport controller, never
//! from here. A configured link rate would be an artificial ceiling, wrong the
//! moment the real capacity differs from the number.
//!
//! # Shape
//!
//! [`NicScheduler::acquire`] returns once the policy admits the datagram, and
//! the connection performs its own send while holding the returned
//! [`NicPermit`]. That keeps the arbiter composable with any send path — a
//! bare UDP socket, or `rtp`'s obfuscating write half — and bounds an
//! interactive datagram's wait to the one bulk datagram already in flight
//! rather than to a queue depth.
//!
//! # What it does not do
//!
//! It orders the **local** send. It cannot drain a network queue that bulk has
//! already filled; keeping the interactive class off a saturated path is the
//! lane-isolation lever, not this one. This is the strongest thing a sender
//! can do at its own egress without cross-layer marking.

use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use tokio::{net::UdpSocket, sync::watch};

use crate::transmission::transmission_layer::{UnreliableLayer, UnreliableRead, UnreliableWrite};

/// The traffic class of a datagram. The arbiter's only job is the ordering
/// between these two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Interactive,
    Bulk,
}

/// The gate. `interactive_active` is a plain counter of interactive sends in
/// flight — counting is fine. The **synchronization is not that counter**: a
/// `watch` channel carries whether bulk may proceed (`true` = no interactive
/// send is active), and a bulk waiter blocks on the channel's `changed()`,
/// never on the counter. The counter only decides *when* the channel
/// transitions (the 1 -> 0 release), so "any interactive active" stays exact
/// without the counter acting as a semaphore's permit count.
struct Shared {
    interactive_active: AtomicUsize,
    gate: watch::Sender<bool>,
}

/// A per-NIC egress arbiter. One instance governs one NIC; construct as many as
/// there are NICs, and hand the same instance to every connection that egresses
/// that NIC. Instances share no state.
#[derive(Clone)]
pub struct NicScheduler {
    shared: Arc<Shared>,
}

impl fmt::Debug for NicScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NicScheduler").finish_non_exhaustive()
    }
}

impl Default for NicScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl NicScheduler {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                interactive_active: AtomicUsize::new(0),
                gate: watch::channel(true).0,
            }),
        }
    }

    /// Wait until the policy admits a datagram of `class`. An interactive
    /// datagram is admitted immediately; a bulk one is admitted only when no
    /// interactive send is active. The caller sends while holding the returned
    /// permit; dropping it releases the class.
    pub async fn acquire(&self, class: Class) -> NicPermit {
        match class {
            Class::Interactive => {
                // Count first, then close the gate, so a bulk waiter cannot
                // observe the gate open across this send.
                self.shared
                    .interactive_active
                    .fetch_add(1, Ordering::AcqRel);
                self.shared.gate.send_replace(false);
                NicPermit {
                    shared: Arc::clone(&self.shared),
                    interactive: true,
                }
            }
            Class::Bulk => {
                let mut open = self.shared.gate.subscribe();
                while !*open.borrow_and_update() {
                    if open.changed().await.is_err() {
                        // The scheduler was dropped; nobody can re-open the
                        // gate, so do not park forever.
                        break;
                    }
                }
                NicPermit {
                    shared: Arc::clone(&self.shared),
                    interactive: false,
                }
            }
        }
    }
}

/// Held for the duration of one admitted send. An interactive permit releases
/// the class (and wakes bulk) when dropped.
pub struct NicPermit {
    shared: Arc<Shared>,
    interactive: bool,
}

impl fmt::Debug for NicPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NicPermit")
            .field("interactive", &self.interactive)
            .finish()
    }
}

impl Drop for NicPermit {
    fn drop(&mut self) {
        if self.interactive
            && self
                .shared
                .interactive_active
                .fetch_sub(1, Ordering::AcqRel)
                == 1
        {
            self.shared.gate.send_replace(true);
        }
    }
}

/// One connection's injection into a per-NIC arbiter: the arbiter it egresses
/// through and its traffic class. Carried on `ConnectConfig`/`AcceptConfig` so
/// any layer (rtp, rtp_mux, and rtp_mux's callers) can pass the same per-NIC
/// instance down to the connection that sends.
#[derive(Debug, Clone)]
pub struct NicLink {
    pub scheduler: NicScheduler,
    pub class: Class,
}

/// Wraps any `UnreliableWrite` so each datagram is admitted by the NIC before
/// it is sent. The instance is shared across connections; the class is fixed
/// per connection.
#[derive(Debug)]
pub struct NicWrite<W> {
    inner: W,
    scheduler: NicScheduler,
    class: Class,
}

impl<W> NicWrite<W> {
    pub fn new(inner: W, scheduler: NicScheduler, class: Class) -> Self {
        Self {
            inner,
            scheduler,
            class,
        }
    }
}

#[async_trait]
impl<W: UnreliableWrite> UnreliableWrite for NicWrite<W> {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, crate::IoErr> {
        let _permit = self.scheduler.acquire(self.class).await;
        self.inner.send(buf).await
    }

    async fn send_vectored(
        &mut self,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Result<usize, crate::IoErr> {
        let _permit = self.scheduler.acquire(self.class).await;
        self.inner.send_vectored(bufs).await
    }
}

/// One connection's end of a NIC when the connection owns its UDP socket. A
/// server that receives an already-open datagram channel instead wraps its own
/// write half with [`NicWrite`].
#[derive(Debug, Clone)]
pub struct NicEndpoint {
    socket: Arc<UdpSocket>,
    scheduler: NicScheduler,
    class: Class,
}

impl NicEndpoint {
    pub async fn bind(
        scheduler: NicScheduler,
        class: Class,
        bind: SocketAddr,
    ) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(bind).await?;
        Ok(Self {
            socket: Arc::new(socket),
            scheduler,
            class,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub async fn connect(&self, peer: SocketAddr) -> std::io::Result<()> {
        self.socket.connect(peer).await
    }

    /// Offer a raw datagram through the arbiter (the socket sends it after
    /// admission). For a load source that is not an `rtp` session.
    pub async fn send(&self, data: &[u8]) -> std::io::Result<usize> {
        let _permit = self.scheduler.acquire(self.class).await;
        // Inherent `UdpSocket::send` (io::Result), not the `UnreliableWrite`
        // impl on `Arc<UdpSocket>`, which a bare method call would select.
        UdpSocket::send(&self.socket, data).await
    }

    /// Build a fully-configured [`UnreliableLayer`] over this endpoint, ready
    /// to hand to [`crate::socket::socket`]. The `config` is the same
    /// [`crate::udp::ConnectConfig`] a socket connect takes, minus the
    /// transport-leg options that cannot be honoured off a socket (rejected by
    /// [`crate::udp::unreliable_layer_with_config`]).
    pub fn into_rtp_layer(
        self,
        config: crate::udp::ConnectConfig<'_>,
    ) -> std::io::Result<UnreliableLayer> {
        let read: Box<dyn UnreliableRead> = Box::new(Arc::clone(&self.socket));
        let write: Box<dyn UnreliableWrite> = Box::new(NicWrite::new(
            Arc::clone(&self.socket),
            self.scheduler.clone(),
            self.class,
        ));
        crate::udp::unreliable_layer_with_config(read, write, config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const PROBE: Duration = Duration::from_millis(50);

    /// An interactive datagram is admitted immediately, even while a bulk send
    /// is in flight: it never waits behind bulk.
    #[tokio::test]
    async fn interactive_never_waits_for_bulk() {
        let scheduler = NicScheduler::new();
        let _bulk = scheduler.acquire(Class::Bulk).await;
        tokio::time::timeout(PROBE, scheduler.acquire(Class::Interactive))
            .await
            .expect("an interactive datagram waited behind bulk");
    }

    /// A bulk datagram is not admitted while an interactive send is active, and
    /// is admitted as soon as none is.
    #[tokio::test]
    async fn bulk_waits_for_interactive_then_proceeds() {
        let scheduler = NicScheduler::new();
        let interactive = scheduler.acquire(Class::Interactive).await;
        assert!(
            tokio::time::timeout(PROBE, scheduler.acquire(Class::Bulk))
                .await
                .is_err(),
            "bulk was admitted while an interactive send was active"
        );
        drop(interactive);
        tokio::time::timeout(PROBE, scheduler.acquire(Class::Bulk))
            .await
            .expect("bulk was not admitted after the interactive send finished");
    }

    /// With no interactive traffic, bulk is unthrottled: the arbiter adds no
    /// rate and no cap of any kind.
    #[tokio::test]
    async fn bulk_is_unthrottled_with_no_interactive_traffic() {
        let scheduler = NicScheduler::new();
        for _ in 0..10_000 {
            let _bulk = scheduler.acquire(Class::Bulk).await;
        }
    }

    /// Two bulk senders can be in flight together: the arbiter delays bulk only
    /// behind interactive, never behind other bulk.
    #[tokio::test]
    async fn bulk_does_not_wait_behind_other_bulk() {
        let scheduler = NicScheduler::new();
        let _a = scheduler.acquire(Class::Bulk).await;
        tokio::time::timeout(PROBE, scheduler.acquire(Class::Bulk))
            .await
            .expect("a bulk send waited behind another bulk send");
    }
}
