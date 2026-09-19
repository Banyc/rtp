//! FEC parity-burst spare-capacity gates.
//!
//! Two deliberately distinct predicates share the "genuinely spare" core
//! (zero application write waiters, no queue growth).  The stock predicate
//! additionally requires a settled tail; the interactive variant exists
//! because a batched interactive lane is always repairing or tail-probing.

/// Whether the sender currently has *genuinely spare* capacity for a FEC
/// parity burst: the stock tail gate plus zero application write waiters and
/// no queue-building signal.  Pacer tokens alone are not spare capacity —
/// queued/waiting application work, queue growth, cwnd pressure,
/// retransmission, and a pending tail probe must win over parity.
pub(crate) fn has_spare_capacity(
    can_send_tail_fec: bool,
    application_write_waiters: usize,
    queue_building: bool,
) -> bool {
    can_send_tail_fec && application_write_waiters == 0 && !queue_building
}

/// Interactive-lane variant of [`has_spare_capacity`]: the genuinely-spare
/// conditions still hold, but a pending retransmission or tail-loss probe does
/// NOT close the gate.
///
/// On a lossy, batched interactive lane the tail-settled conditions are
/// effectively always false — the lane is either repairing a loss or waiting
/// for its tail probe — so the interactive parity that exists to avoid those
/// repairs was deferred until after the repair and never emitted.  The parity
/// is still bounded: the loss condition gate must be open, the burst is capped
/// at 1/3 of the send budget, and it is emitted only at a group/burst
/// boundary, so it never competes with the data stream.  Used only by tunings
/// that force-flush every burst tail (`instream_flush`); stock/bulk traffic
/// keeps [`has_spare_capacity`] byte-for-byte.
pub(crate) fn has_spare_capacity_interactive(
    accepts_new_pkt: bool,
    application_write_waiters: usize,
    queue_building: bool,
) -> bool {
    // Deliberately does NOT require the send stage to be empty: the in-stream
    // full-group flush is designed to fire mid-burst (at
    // `INSTREAM_DATA_PER_GROUP` symbols) while later application symbols are
    // still staged, and requiring an empty stage defeated it, leaving the
    // batched interactive lane with no parity.  The send window must still
    // have room (`accepts_new_pkt`) and the queue must not be building, so
    // parity never competes with a congestion-limited data stream.
    accepts_new_pkt && application_write_waiters == 0 && !queue_building
}

#[cfg(test)]
mod tests {
    use super::{has_spare_capacity, has_spare_capacity_interactive};

    /// The stock predicate requires the settled tail AND zero application
    /// waiters AND no queue growth: any one failing closes the gate, so parity
    /// never competes with a queued/building data stream.
    #[test]
    fn stock_spare_capacity_requires_every_condition() {
        assert!(has_spare_capacity(true, 0, false));
        assert!(
            !has_spare_capacity(false, 0, false),
            "an unsettled tail must close the stock gate"
        );
        assert!(
            !has_spare_capacity(true, 1, false),
            "a waiting application writer must close the stock gate"
        );
        assert!(
            !has_spare_capacity(true, 0, true),
            "queue growth must close the stock gate"
        );
        assert!(!has_spare_capacity(true, 3, true));
    }

    /// The interactive predicate drops only the settled-tail requirement: it
    /// still requires a sendable window, zero waiters, and no queue growth, so
    /// a non-empty stage alone no longer closes it but application
    /// backpressure and queue growth still do.
    #[test]
    fn interactive_spare_capacity_keeps_backpressure_and_growth_closed() {
        assert!(has_spare_capacity_interactive(true, 0, false));
        assert!(
            !has_spare_capacity_interactive(false, 0, false),
            "no room in the send window must close the interactive gate"
        );
        assert!(
            !has_spare_capacity_interactive(true, 1, false),
            "a waiting application writer must close the interactive gate"
        );
        assert!(
            !has_spare_capacity_interactive(true, 0, true),
            "queue growth must close the interactive gate"
        );
    }

    /// The interactive predicate is strictly weaker than the stock one: it is
    /// open wherever the stock gate is open, so a lane that switched predicate
    /// can never lose a parity opportunity the stock gate would have granted.
    #[test]
    fn interactive_spare_capacity_is_implied_by_the_stock_gate() {
        for can_send in [false, true] {
            for waiters in [0usize, 1, 5] {
                for building in [false, true] {
                    if has_spare_capacity(can_send, waiters, building) {
                        assert!(
                            has_spare_capacity_interactive(can_send, waiters, building),
                            "stock-spare ({can_send}, {waiters}, {building}) must imply interactive-spare"
                        );
                    }
                }
            }
        }
    }
}
