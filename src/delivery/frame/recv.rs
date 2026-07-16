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

fn find_complete_frame(slots: &BTreeMap<u64, RecvSlot>) -> Option<Vec<u64>> {
    let mut collected_seqs: Vec<u64> = Vec::new();
    let mut target_len: Option<u32> = None;
    let mut collected: usize = 0;

    for (&seq, slot) in slots.iter() {
        match slot {
            RecvSlot::Data(pkt) => {
                if collected_seqs.is_empty() {
                    let Some(fl) = pkt.frame_len else {
                        continue;
                    };
                    if fl == 0 {
                        return Some(vec![seq]);
                    }
                    target_len = Some(fl);
                    collected_seqs.push(seq);
                    collected += pkt.data.len();
                    if collected >= fl as usize {
                        return Some(collected_seqs);
                    }
                } else {
                    if let Some(fl) = pkt.frame_len {
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
                    let expected = *collected_seqs.last().unwrap() + 1;
                    if seq != expected {
                        collected_seqs.clear();
                        target_len = None;
                        collected = 0;
                        continue;
                    }
                    collected_seqs.push(seq);
                    collected += pkt.data.len();
                    if let Some(tl) = target_len
                        && collected >= tl as usize
                    {
                        return Some(collected_seqs);
                    }
                }
            }
            RecvSlot::Tombstone => {
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
                slots.insert(seq, RecvSlot::Tombstone);
            }
        }
    }

    for seq in seqs.iter() {
        slots.insert(*seq, RecvSlot::Tombstone);
    }

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
