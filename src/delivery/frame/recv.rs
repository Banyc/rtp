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
) -> Option<(SequenceNumber, u64, u32)> {
    let mut scanning_front = true;
    let mut frame_start: Option<SequenceNumber> = None;
    let mut frame_end: Option<SequenceNumber> = None;
    let mut packet_count: u64 = 0;
    let mut target_len: u32 = 0;
    let mut collected: usize = 0;
    for (seq, slot) in slots.iter_from(*scan_start) {
        if scanning_front {
            match slot {
                RecvSlot::Tombstone => *scan_start = seq.advance(1),
                RecvSlot::Data(_) => {
                    *scan_start = seq;
                    scanning_front = false;
                }
            }
        }
        match slot {
            RecvSlot::Data(pkt) => {
                if frame_start.is_none() || pkt.frame_len.is_some() {
                    let Some(fl) = pkt.frame_len else {
                        continue;
                    };
                    frame_start = Some(seq);
                    frame_end = Some(seq);
                    packet_count = 1;
                    target_len = fl;
                    collected = pkt.data.len();
                } else {
                    let expected = frame_end.unwrap().advance(1);
                    if seq != expected {
                        frame_start = None;
                        frame_end = None;
                        packet_count = 0;
                        collected = 0;
                        continue;
                    }
                    frame_end = Some(seq);
                    packet_count += 1;
                    collected += pkt.data.len();
                }
                if collected >= target_len as usize {
                    return Some((frame_start.unwrap(), packet_count, target_len));
                }
            }
            RecvSlot::Tombstone => {
                frame_start = None;
                frame_end = None;
                packet_count = 0;
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
    let (frame_start, packet_count, frame_len) = find_complete_frame(slots, scan_start)?;
    let mut frame_bytes = Vec::with_capacity(frame_len as usize);
    for offset in 0..packet_count {
        let seq = frame_start.advance(offset);
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
