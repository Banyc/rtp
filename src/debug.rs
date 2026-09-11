//! Process-wide debug toggles, read from the environment exactly once.
//!
//! These gates sit on the per-packet send and receive paths, so they must
//! never call `std::env::var` per packet: `getenv` scans `environ` and
//! allocates a `String` for the value on every call. [`debug_send`] caches
//! the answer in a [`OnceLock`] at first use; every later call is a single
//! atomic load.

use std::sync::OnceLock;

static DEBUG_SEND: OnceLock<bool> = OnceLock::new();

/// Number of times the `RTP_DEBUG_SEND` environment was actually read. Only
/// observable in tests, where it backs
/// [`tests::debug_send_reads_the_environment_at_most_once`].
#[cfg(test)]
static DEBUG_SEND_ENV_READS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Whether `RTP_DEBUG_SEND` is set in the environment.
///
/// The environment is read once and cached process-wide, so this is safe to
/// call on every packet.
pub(crate) fn debug_send() -> bool {
    *DEBUG_SEND.get_or_init(|| {
        #[cfg(test)]
        DEBUG_SEND_ENV_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::var("RTP_DEBUG_SEND").is_ok()
    })
}

#[cfg(test)]
pub(crate) fn debug_send_env_reads() -> usize {
    DEBUG_SEND_ENV_READS.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The per-packet hot path calls [`debug_send`] once or more per packet.
    /// If it re-read the environment each time, a busy sender would pay a
    /// `getenv` scan plus a `String` allocation per packet. The cache must
    /// collapse all of that to at most one environment read for the life of
    /// the process, no matter how many packets (or tests) call it.
    #[test]
    fn debug_send_reads_the_environment_at_most_once() {
        for _ in 0..10_000 {
            let _ = debug_send();
        }
        let reads = debug_send_env_reads();
        assert!(
            reads <= 1,
            "RTP_DEBUG_SEND must be read once and cached, not per call; \
             saw {reads} environment reads after 10,000 calls"
        );
    }
}
