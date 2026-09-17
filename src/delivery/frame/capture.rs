//! Frame-capture and fast-forward policy for the receiver reassembler.
//!
//! These are the two boundary decisions the frame-delivery scan makes.  They
//! live here, apart from the scan/cursor machinery in [`super::recv`], so the
//! rules cannot drift from the mechanism that applies them, and so the
//! tombstone-capture boundary (a slot landing *exactly* on the next
//! continuation abandons; one beyond it does not) has a single home.

use crate::sequence::SequenceNumber;

/// Whether the slot at `seq` captures an in-progress frame's next
/// continuation forever, so the frame can no longer reassemble and must be
/// abandoned.
///
/// An *occupied* slot cannot be replaced — `recv_bytes` treats a packet
/// landing on it as a duplicate — so a capture landing **exactly** on the
/// in-progress frame's next continuation sequence pins the in-order cursor at
/// the frame's start forever: the missing continuation can never arrive.  A
/// capture may be a [`super::recv::RecvSlot::Tombstone`] (a delivered frame's
/// sequence) or a foreign frame start (a packet carrying its own `frame_len`).
/// A legitimate sender never interleaves frames, so a foreign start here only
/// arises from a hostile peer's overlapping frame or a defective sender that
/// abandoned the frame mid-flight; either way the frame is abandoned so the
/// cursor can advance past it.
///
/// A capture *beyond* that continuation lies past a still-vacant hole inside
/// the frame; it takes nothing the frame needs yet (the hole is still
/// fillable by an in-flight or retransmitted packet) and so belongs to a
/// later, already-delivered frame — notably one fast-forwarded past the hole.
/// Abandoning there would tombstone the earlier frame's collected packets and
/// lose it permanently even though its missing symbol later arrives, so the
/// caller resets the in-progress run instead and keeps scanning, exactly as
/// for an absent slot (a hole).
pub(crate) fn captures_next_continuation(
    in_progress_end: Option<SequenceNumber>,
    seq: SequenceNumber,
) -> bool {
    in_progress_end.is_some_and(|end| seq == end.advance(1))
}

/// Whether a complete frame beginning at `frame_start` may be handed up while
/// the in-order front is pinned at `front` (a different, unrepaired
/// sequence).
///
/// By default the frame must begin exactly at the front — the first
/// not-yet-delivered sequence.  The scan skips sequence holes (a missing slot
/// is simply absent from the map), so without this gate a complete later
/// frame would be delivered past an unrepaired hole, violating the ordered,
/// gap-free delivery contract.  A complete frame past a hole is *withheld*:
/// its slots stay in place and the scan resume makes the next call continue
/// past it instead of re-walking it; when the hole fills (retransmission or
/// reordering), the insertion drops the resume and the full rescan re-finds
/// the frame, now at the front, and delivers it.
///
/// With the opt-in receiver-side fast-forward (`allow_reorder`) the complete
/// frame is delivered even though it starts past the hole.  Delivery
/// tombstones the frame's sequence numbers exactly as in the strict path
/// while `front` stays pinned at the hole — an absent head slot keeps the
/// cursor from advancing, so the ACK cumulative front and the liveness
/// watchdog are unaffected.  When the hole finally fills, the cursor reaches
/// the frame's tombstones and collapses them, advancing without
/// redelivering.  Consumers that opt in must restore ordering themselves
/// (e.g. a per-stream reorder buffer); the default `false` is byte-for-byte
/// the strict behaviour.
pub(crate) fn may_deliver_past_front(
    frame_start: SequenceNumber,
    front: SequenceNumber,
    allow_reorder: bool,
) -> bool {
    allow_reorder || frame_start == front
}
