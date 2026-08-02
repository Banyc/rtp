//! Receiver-side out-of-order frame reassembly.

use std::collections::BTreeMap;

use primitive::arena::obj_pool::ObjPool;

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
    slots: &BTreeMap<u64, RecvSlot>,
    scan_start: &mut u64,
) -> Option<(Vec<u64>, u32)> {
    for (&seq, slot) in slots.range(*scan_start..) {
        if !matches!(slot, RecvSlot::Tombstone) {
            *scan_start = seq;
            break;
        }
        *scan_start = seq + 1;
    }
    let mut collected_seqs: Vec<u64> = Vec::new();
    let mut target_len: u32 = 0;
    let mut collected: usize = 0;
    for (&seq, slot) in slots.range(*scan_start..) {
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
                    let expected = *collected_seqs.last().unwrap() + 1;
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
    slots: &mut BTreeMap<u64, RecvSlot>,
    reused_buf: &mut ObjPool<Vec<u8>>,
    scan_start: &mut u64,
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

pub(crate) fn fin_at_head(next: Option<u64>, slots: &BTreeMap<u64, RecvSlot>) -> bool {
    let Some(next) = next else {
        return false;
    };
    match slots.get(&next) {
        Some(RecvSlot::Data(pkt)) => pkt.data.is_empty() && pkt.frame_len.is_none(),
        _ => false,
    }
}
