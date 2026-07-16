//! Receiver-side out-of-order frame reassembly.
//!
//! In frame-delivery mode the receiver may deliver any *complete* frame out
//! of order, past sequence holes, by scanning the receive queue for a
//! frame-start slot (a packet carrying `frame_len`) and walking contiguous
//! data slots until the frame length is satisfied.  Slots consumed by an
//! early-delivered frame become [`RecvSlot::Tombstone`]s that still count
//! toward the receive window so a persistent hole cannot unbounded memory;
//! the receive queue collapses contiguous tombstones at its in-order cursor.
//!
//! ACK/SACK generation is untouched by this path: the ACK history is
//! recorded at packet arrival, before any frame is extracted.

use std::collections::BTreeMap;

use primitive::arena::obj_pool::ObjPool;

/// A slot in the receive queue.  `Tombstone` marks a slot consumed by an
/// early-delivered frame; it keeps counting toward the receive window until
/// the in-order cursor collapses it.  In stock (non-frame) mode every slot is
/// `Data` and `frame_len` is always `None`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecvSlot {
    Data(RecvPkt),
    Tombstone,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RecvPkt {
    pub(crate) data: Vec<u8>,
    /// `Some` only for the first packet of a frame; `None` for continuation
    /// packets and for all packets when frame-delivery mode is off.
    pub(crate) frame_len: Option<u32>,
}

/// Find the first complete frame in the slot map.  Scans for a `Data` slot
/// with `frame_len: Some(len)`, then walks consecutive `Data` slots until the
/// accumulated payload equals `len`.  Returns `None` if no complete frame is
/// found.
fn find_complete_frame(slots: &BTreeMap<u64, RecvSlot>) -> Option<Vec<u64>> {
    let mut collected_seqs: Vec<u64> = Vec::new();
    let mut target_len: Option<u32> = None;
    let mut collected: usize = 0;

    for (&seq, slot) in slots.iter() {
        match slot {
            RecvSlot::Data(pkt) => {
                if collected_seqs.is_empty() {
                    // Looking for a frame-start slot.
                    let Some(fl) = pkt.frame_len else {
                        continue;
                    };
                    if fl == 0 {
                        // Zero-length frame in the slot map — shouldn't
                        // happen (FIN is an empty-payload packet with no
                        // frame_len), but handle gracefully.
                        return Some(vec![seq]);
                    }
                    target_len = Some(fl);
                    collected_seqs.push(seq);
                    collected += pkt.data.len();
                    if collected >= fl as usize {
                        return Some(collected_seqs);
                    }
                } else {
                    // Already inside a frame run.
                    // A continuation slot with its own `frame_len` signals
                    // a new frame — stop the current run.
                    if let Some(fl) = pkt.frame_len {
                        // Current frame is incomplete; reset and start
                        // scanning from this new start.
                        collected_seqs.clear();
                        if fl == 0 {
                            return Some(vec![seq]);
                        }
                        target_len = Some(fl);
                        collected_seqs.push(seq);
                        collected = pkt.data.len();
                        if collected >= fl as usize {
                            return Some(collected_seqs);
                        }
                        continue;
                    }
                    // Continuation packet: must be consecutive.
                    let expected = *collected_seqs.last().unwrap() + 1;
                    if seq != expected {
                        // Gap in the frame run — frame is incomplete.
                        collected_seqs.clear();
                        target_len = None;
                        collected = 0;
                        continue;
                    }
                    collected_seqs.push(seq);
                    collected += pkt.data.len();
                    // Check if we've collected the full frame.
                    if let Some(tl) = target_len
                        && collected >= tl as usize
                    {
                        return Some(collected_seqs);
                    }
                }
            }
            RecvSlot::Tombstone => {
                // Tombstones break frame contiguity.
                if !collected_seqs.is_empty() {
                    collected_seqs.clear();
                    target_len = None;
                    collected = 0;
                }
            }
        }
    }
    None
}

/// Pop one complete frame (possibly out of order past sequence holes).  The
/// consumed slots are replaced with `Tombstone`s so window accounting still
/// bounds memory; the caller is responsible for collapsing a contiguous
/// tombstone prefix at its in-order cursor.
pub(crate) fn pop_complete_frame(
    slots: &mut BTreeMap<u64, RecvSlot>,
    reused_buf: &mut ObjPool<Vec<u8>>,
) -> Option<Vec<u8>> {
    let seqs = find_complete_frame(slots)?;
    let mut frame_bytes = Vec::new();

    for &seq in &seqs {
        let slot = slots.remove(&seq).unwrap();
        match slot {
            RecvSlot::Data(pkt) => {
                frame_bytes.extend_from_slice(&pkt.data);
                reused_buf.put(pkt.data);
            }
            RecvSlot::Tombstone => {
                // Shouldn't happen (find_complete_frame skips tombstones),
                // but be defensive.
                slots.insert(seq, RecvSlot::Tombstone);
            }
        }
    }

    // Re-insert tombstones for the consumed slots so window accounting
    // still bounds memory.
    for seq in seqs.iter() {
        slots.insert(*seq, RecvSlot::Tombstone);
    }

    Some(frame_bytes)
}

/// Check whether a FIN (empty-payload packet with no `frame_len`) is sitting
/// at the in-order head of the receive queue — i.e. all earlier packets have
/// been consumed or tombstoned.  This is the EOF signal for frame mode: the
/// byte-stream delivery path (which sets the FIN flag in stock mode) is
/// bypassed in frame mode, so the caller checks this predicate directly to
/// surface EOF.
///
/// Returns `true` only when:
/// - `next` is `Some(n)` and a `Data` slot exists at `n`, and
/// - that slot's payload is empty (`data.is_empty()`), and
/// - that slot has no `frame_len` (it's a FIN, not a zero-length frame).
pub(crate) fn fin_at_head(next: Option<u64>, slots: &BTreeMap<u64, RecvSlot>) -> bool {
    let Some(next) = next else {
        return false;
    };
    match slots.get(&next) {
        Some(RecvSlot::Data(pkt)) => pkt.data.is_empty() && pkt.frame_len.is_none(),
        _ => false,
    }
}
