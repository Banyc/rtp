//! Receiver-side out-of-order frame reassembly, in wrapping sequence space.

use primitive::arena::obj_pool::ObjPool;

use crate::sequence::{SequenceMap, SequenceNumber};

/// A slot in the receive queue.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecvSlot {
    Data(RecvPkt),
    Tombstone,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RecvPkt {
    pub(crate) data: Vec<u8>,
    pub(crate) frame_len: Option<u32>,
}

fn find_complete_frame(
    slots: &SequenceMap<RecvSlot>,
    scan_start: &mut SequenceNumber,
) -> Option<(Vec<SequenceNumber>, u32)> {
    // Scan in logical (wrapping) order: walk past the leading tombstones so
    // the frame scan starts at the first live slot at/after the cursor.
    for (seq, slot) in slots.iter_from(*scan_start) {
        if !matches!(slot, RecvSlot::Tombstone) {
            *scan_start = seq;
            break;
        }
        *scan_start = seq.advance(1);
    }
    let mut collected_seqs: Vec<SequenceNumber> = Vec::new();
    let mut target_len: u32 = 0;
    let mut collected: usize = 0;
    for (seq, slot) in slots.iter_from(*scan_start) {
        match slot {
            RecvSlot::Data(pkt) => {
                if collected_seqs.is_empty() || pkt.frame_len.is_some() {
                    let Some(fl) = pkt.frame_len else {
                        continue;
                    };
                    collected_seqs.clear();
                    collected_seqs.push(seq);
                    target_len = fl;
                    collected = pkt.data.len();
                } else {
                    // Each continuation must equal the previous sequence
                    // advanced by one (wrapping arithmetic).
                    let expected = collected_seqs.last().unwrap().advance(1);
                    if seq != expected {
                        collected_seqs.clear();
                        collected = 0;
                        continue;
                    }
                    collected_seqs.push(seq);
                    collected += pkt.data.len();
                }
                if collected >= target_len as usize {
                    return Some((collected_seqs, target_len));
                }
            }
            RecvSlot::Tombstone => {
                collected_seqs.clear();
                collected = 0;
            }
        }
    }
    None
}

pub(crate) fn pop_complete_frame(
    slots: &mut SequenceMap<RecvSlot>,
    reused_buf: &mut ObjPool<Vec<u8>>,
    scan_start: &mut SequenceNumber,
) -> Option<Vec<u8>> {
    let (seqs, frame_len) = find_complete_frame(slots, scan_start)?;
    let mut frame_bytes = Vec::new();
    for &seq in &seqs {
        if let Some(RecvSlot::Data(pkt)) = slots.insert(seq, RecvSlot::Tombstone) {
            frame_bytes.extend_from_slice(&pkt.data);
            reused_buf.put(pkt.data);
        }
    }
    frame_bytes.truncate(frame_len as usize);
    Some(frame_bytes)
}

pub(crate) fn fin_at_head(next: Option<SequenceNumber>, slots: &SequenceMap<RecvSlot>) -> bool {
    let Some(next) = next else {
        return false;
    };
    match slots.get(&next) {
        Some(RecvSlot::Data(pkt)) => pkt.data.is_empty() && pkt.frame_len.is_none(),
        _ => false,
    }
}
