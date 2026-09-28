//! Interactive/bulk fair queueing on one NIC.
//!
//! This is the kernel fair-queueing idea applied one level up. Where an egress
//! qdisc such as `fq_codel` is fair *between flows* to keep any one flow's
//! queue short, this is fair *between the two traffic classes this product
//! distinguishes* — interactive and bulk — so the interactive class keeps the
//! short queue no matter how many bulk connections share the device.
//!
//! The unit of contention on the sender is the **NIC**, not the mux connection
//! and not the destination. When bulk traffic is spread across several
//! connections (to avoid saturating any one policed destination, or for load
//! balancing), those connections share one egress device, and no per-connection
//! scheduler can keep an interactive datagram from sitting behind a bulk queue
//! at that device. This crate is that device-level queue: the operator
//! constructs one [`NicScheduler`] per NIC (or several independent ones) and
//! hands the **same** instance to every connection that egresses it; each
//! connection wraps its own send path in [`NicWrite`] under a [`Class`].
//!
//! The invariant is the one the product's interactive-latency mandate needs:
//!
//! > an interactive datagram is never delayed behind a bulk datagram on the
//! > NIC the queue governs — beyond the one datagram already in flight.
//!
//! [`Policy::Priority`] enforces it by serving interactive first and letting
//! bulk consume only the link credit *above* a configured interactive reserve,
//! so the NIC never carries a standing bulk backlog that an interactive
//! datagram would have to queue behind. [`Policy::Fifo`] is the control: same
//! link, arrival order only — the arm an interactive-aware discipline must
//! beat to prove it changed anything.
//!
//! # Shape
//!
//! The scheduler arbitrates **credit**, not datagrams: [`NicScheduler::acquire`]
//! waits until the policy lets a datagram of the given size go, then returns,
//! and the connection performs its own send. That keeps the queue composable
//! with any send path — a bare UDP socket, or `rtp`'s obfuscating write half —
//! and bounds an interactive datagram's wait to the one bulk datagram already
//! in flight rather than to a queue depth.

use std::{
    fmt, io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use tokio::{net::UdpSocket, sync::Notify, time::Instant};

use crate::transmission::transmission_layer::{UnreliableLayer, UnreliableRead, UnreliableWrite};

/// The traffic class of a datagram. The scheduler's only job is the ordering
/// between these two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Interactive,
    Bulk,
}

/// How the scheduler arbitrates the link between the classes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Policy {
    /// Interactive datagrams are served before bulk, and bulk may consume only
    /// the link credit above `interactive_reserve_bytes_per_sec`, so a reserve
    /// is always left for interactive even while bulk is saturating. While an
    /// interactive datagram is waiting for credit, bulk is not admitted at
    /// all.
    ///
    /// The reserve is a **rate**, not a queue depth: it is the link capacity
    /// held in perpetuity for interactive traffic, which is what makes the
    /// guarantee independent of how much bulk is queued.
    Priority {
        interactive_reserve_bytes_per_sec: f64,
    },
    /// Arrival order (the control arm): bulk can occupy the whole link, and an
    /// interactive datagram waits for whatever bulk credit was granted first.
    Fifo,
}

/// Configuration for one [`NicScheduler`].
#[derive(Debug, Clone, Copy)]
pub struct NicConfig {
    /// The egress device's capacity, in bytes per second.
    pub link_rate_bytes_per_sec: f64,
    /// The arbitration policy.
    pub policy: Policy,
}

impl NicConfig {
    /// A priority scheduler holding `reserve_bytes_per_sec` of a
    /// `link_rate_bytes_per_sec` NIC for interactive traffic.
    pub fn priority(link_rate_bytes_per_sec: f64, reserve_bytes_per_sec: f64) -> Self {
        Self {
            link_rate_bytes_per_sec,
            policy: Policy::Priority {
                interactive_reserve_bytes_per_sec: reserve_bytes_per_sec,
            },
        }
    }

    /// The control: the same NIC, arrival order only.
    pub fn fifo(link_rate_bytes_per_sec: f64) -> Self {
        Self {
            link_rate_bytes_per_sec,
            policy: Policy::Fifo,
        }
    }
}

/// The shared credit state for one NIC.
struct Shared {
    state: Mutex<Credit>,
    notify: Notify,
    rate: f64,
    reserve: f64,
    policy: Policy,
}

#[derive(Debug)]
struct Credit {
    tokens: f64,
    last: Instant,
    /// Interactive datagrams currently waiting for credit. While this is
    /// non-zero, bulk is not admitted: an interactive datagram is never
    /// behind bulk.
    interactive_waiters: usize,
}

impl Shared {
    fn refill(&self, st: &mut Credit, now: Instant) {
        let added = self.rate * now.duration_since(st.last).as_secs_f64();
        // An idle link may not bank an unbounded burst: one second of credit.
        st.tokens = (st.tokens + added).min(self.rate);
        st.last = now;
    }

    /// Try to consume credit for a datagram of `len` bytes under the policy.
    fn try_take(&self, st: &mut Credit, class: Class, len: usize, now: Instant) -> bool {
        self.refill(st, now);
        let bytes = len as f64;
        match self.policy {
            Policy::Priority { .. } => match class {
                Class::Interactive => {
                    if st.tokens >= bytes {
                        st.tokens -= bytes;
                        true
                    } else {
                        false
                    }
                }
                Class::Bulk => {
                    if st.interactive_waiters == 0 && st.tokens - bytes >= self.reserve {
                        st.tokens -= bytes;
                        true
                    } else {
                        false
                    }
                }
            },
            Policy::Fifo => {
                if st.tokens >= bytes {
                    st.tokens -= bytes;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// How long until a datagram of `len` bytes could be admitted.
    fn wait_for(&self, st: &mut Credit, class: Class, len: usize, now: Instant) -> Duration {
        self.refill(st, now);
        let bytes = len as f64;
        let secs = match self.policy {
            Policy::Priority { .. } => match class {
                Class::Interactive => (bytes - st.tokens) / self.rate,
                Class::Bulk => (bytes + self.reserve - st.tokens) / self.rate,
            },
            Policy::Fifo => (bytes - st.tokens) / self.rate,
        };
        Duration::from_secs_f64(secs.max(0.0)).max(Duration::from_micros(10))
    }
}

/// A per-NIC egress queue. One instance governs one NIC; construct as many as
/// there are NICs, and hand the same instance to every connection that
/// egresses that NIC. Instances share no state.
#[derive(Clone)]
pub struct NicScheduler {
    shared: Arc<Shared>,
}

impl fmt::Debug for NicScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NicScheduler").finish_non_exhaustive()
    }
}

impl NicScheduler {
    pub fn new(config: NicConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(Credit {
                    tokens: 0.0,
                    last: Instant::now(),
                    interactive_waiters: 0,
                }),
                notify: Notify::new(),
                rate: config.link_rate_bytes_per_sec.max(1.0),
                reserve: match config.policy {
                    Policy::Priority {
                        interactive_reserve_bytes_per_sec,
                    } => interactive_reserve_bytes_per_sec.max(0.0),
                    Policy::Fifo => 0.0,
                },
                policy: config.policy,
            }),
        }
    }

    /// Wait until the policy admits a datagram of `len` bytes of `class` on
    /// this NIC, consuming its credit. The caller sends afterwards.
    pub async fn acquire(&self, class: Class, len: usize) {
        if self.try_once(class, len) {
            return;
        }
        let _guard = WaiterGuard::new(&self.shared, class);
        loop {
            if self.try_once(class, len) {
                return;
            }
            let wait = {
                let mut st = self.shared.state.lock().unwrap();
                self.shared.wait_for(&mut st, class, len, Instant::now())
            };
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = self.shared.notify.notified() => {}
            }
        }
    }

    fn try_once(&self, class: Class, len: usize) -> bool {
        let mut st = self.shared.state.lock().unwrap();
        self.shared.try_take(&mut st, class, len, Instant::now())
    }
}

/// Tracks a waiting datagram so bulk stays blocked while any interactive
/// datagram waits, and so a departing waiter wakes the others.
struct WaiterGuard<'a> {
    shared: &'a Shared,
    class: Class,
}

impl<'a> WaiterGuard<'a> {
    fn new(shared: &'a Shared, class: Class) -> Self {
        if class == Class::Interactive {
            shared.state.lock().unwrap().interactive_waiters += 1;
        }
        Self { shared, class }
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        if self.class == Class::Interactive {
            self.shared.state.lock().unwrap().interactive_waiters -= 1;
        }
        // A departing waiter may unblock another class (an interactive leaving
        // lets bulk proceed); wake everyone to re-check.
        self.shared.notify.notify_waiters();
    }
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
        self.scheduler.acquire(self.class, buf.len()).await;
        self.inner.send(buf).await
    }

    async fn send_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> Result<usize, crate::IoErr> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        self.scheduler.acquire(self.class, total).await;
        self.inner.send_vectored(bufs).await
    }
}

/// One connection's injection into a per-NIC scheduler: the scheduler it
/// egresses through and its traffic class. Carried on `ConnectConfig` /
/// `AcceptConfig` so any layer (rtp, rtp_mux, and rtp_mux's callers) can pass
/// the same per-NIC instance down to the connection that sends.
#[derive(Debug, Clone)]
pub struct NicLink {
    pub scheduler: NicScheduler,
    pub class: Class,
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

    /// Offer a raw datagram through the NIC (the socket sends it after
    /// admission). For a load source that is not an `rtp` session.
    pub async fn send(&self, data: &[u8]) -> std::io::Result<usize> {
        self.scheduler.acquire(self.class, data.len()).await;
        // Inherent `UdpSocket::send` (io::Result), not the `UnreliableWrite`
        // impl on `Arc<UdpSocket>`, which a bare method call would select.
        UdpSocket::send(&self.socket, data).await
    }

    /// Build a fully-configured [`UnreliableLayer`] over this endpoint, ready
    /// to hand to [`crate::socket::socket`]. The `config` is the same
    /// [`crate::udp::ConnectConfig`] a socket connect takes, minus the
    /// transport-leg options that cannot be honoured off a socket (rejected by
    /// [`crate::udp::unreliable_layer_with_config`]); a `config` whose `nic`
    /// would double-inject is ignored here because the endpoint already is the
    /// injection.
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

    const MIB: f64 = 1_048_576.0;
    const BULK_LEN: usize = 16_384;

    /// A shared credit state whose clock is pinned, so the credit arithmetic
    /// does not depend on how long construction took.
    fn pinned(config: NicConfig) -> (Shared, Instant) {
        let base = Instant::now();
        let reserve = match config.policy {
            Policy::Priority {
                interactive_reserve_bytes_per_sec,
            } => interactive_reserve_bytes_per_sec.max(0.0),
            Policy::Fifo => 0.0,
        };
        let shared = Shared {
            state: Mutex::new(Credit {
                tokens: 0.0,
                last: base,
                interactive_waiters: 0,
            }),
            notify: Notify::new(),
            rate: config.link_rate_bytes_per_sec.max(1.0),
            reserve,
            policy: config.policy,
        };
        (shared, base)
    }

    /// Under priority, an interactive datagram is admitted before any bulk on
    /// the same credit, and a bulk datagram cannot take the interactive
    /// reserve.
    #[test]
    fn priority_admits_interactive_before_bulk_and_keeps_the_reserve() {
        let (shared, base) = pinned(NicConfig::priority(MIB, MIB / 2.0));
        let mut st = Credit {
            tokens: MIB,
            last: base,
            interactive_waiters: 0,
        };
        // Bulk may take only above the reserve: it can spend MIB - MIB/2.
        let mut taken = 0.0;
        while shared.try_take(&mut st, Class::Bulk, BULK_LEN, base) {
            taken += BULK_LEN as f64;
        }
        assert!(
            taken >= MIB / 2.0 - BULK_LEN as f64 && taken <= MIB / 2.0,
            "bulk spent {taken} bytes of a 1 MiB link with a 0.5 MiB reserve"
        );
        assert!(
            st.tokens >= MIB / 2.0,
            "the reserve was eaten: {}",
            st.tokens
        );
        // The reserve is still there for interactive.
        assert!(
            shared.try_take(&mut st, Class::Interactive, 64, base),
            "interactive could not use the reserve"
        );
    }

    /// While an interactive datagram waits for credit, bulk is not admitted:
    /// an interactive datagram is never behind bulk.
    #[test]
    fn priority_blocks_bulk_while_interactive_waits() {
        let (shared, base) = pinned(NicConfig::priority(MIB, MIB / 2.0));
        let mut st = Credit {
            tokens: MIB,
            last: base,
            interactive_waiters: 1,
        };
        assert!(
            !shared.try_take(&mut st, Class::Bulk, 64, base),
            "bulk was admitted while an interactive datagram waited"
        );
    }

    /// The control: under FIFO there is no reserve and no class ordering, so
    /// bulk can drain the link and an interactive datagram must wait for the
    /// next refill.
    #[test]
    fn fifo_has_no_reserve() {
        let (shared, base) = pinned(NicConfig::fifo(MIB));
        let mut st = Credit {
            tokens: MIB,
            last: base,
            interactive_waiters: 0,
        };
        let mut taken = 0.0;
        while shared.try_take(&mut st, Class::Bulk, BULK_LEN, base) {
            taken += BULK_LEN as f64;
        }
        assert!(
            taken >= MIB - BULK_LEN as f64,
            "fifo bulk did not drain the link: {taken}"
        );
        assert!(
            !shared.try_take(&mut st, Class::Interactive, 64, base),
            "an interactive datagram was admitted with the link drained"
        );
    }
}
