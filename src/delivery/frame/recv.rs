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

/// The outcome of one reassembly scan: the complete frame (if any) plus the
/// frames abandoned because a tombstone landed inside their collected range
/// (a hostile peer's overlapping frames). The abandoned frames' collected
/// seqs must be tombstoned so the in-order cursor can advance past them;
/// otherwise the receiver wedges permanently on the abandoned frame.
#[derive(Debug, Default)]
pub(crate) struct FrameScan {
    /// The complete frame, when the scan found one.
    pub(crate) complete: Option<(SequenceNumber, u64, u32)>,
    /// `(start, packet_count)` of each frame abandoned mid-scan because a
    /// tombstone landed inside its collected range. Empty in every
    /// legitimate arrival pattern (a legitimate sender never interleaves
    /// frames, so a tombstone can only appear inside an in-progress frame
    /// when a hostile peer's frame overlapped it).
    pub(crate) abandoned: Vec<(SequenceNumber, u64)>,
}

fn find_complete_frame(
    slots: &SequenceMap<RecvSlot>,
    scan_start: &mut SequenceNumber,
) -> FrameScan {
    let mut scanning_front = true;
    let mut frame_start: Option<SequenceNumber> = None;
    let mut frame_end: Option<SequenceNumber> = None;
    let mut packet_count: u64 = 0;
    let mut target_len: u32 = 0;
    let mut collected: usize = 0;
    let mut abandoned: Vec<(SequenceNumber, u64)> = Vec::new();
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
                    return FrameScan {
                        complete: Some((frame_start.unwrap(), packet_count, target_len)),
                        abandoned,
                    };
                }
            }
            RecvSlot::Tombstone => {
                // A tombstone inside an in-progress frame means the frame can
                // never reassemble: its retransmissions are swallowed by the
                // tombstone (recv_bytes treats an occupied slot as a
                // duplicate), so the cursor would stay pinned at the frame's
                // start forever. Record it so the caller can tombstone its
                // collected seqs and let the cursor advance.
                if let Some(start) = frame_start {
                    abandoned.push((start, packet_count));
                }
                frame_start = None;
                frame_end = None;
                packet_count = 0;
                collected = 0;
            }
        }
    }
    FrameScan {
        complete: None,
        abandoned,
    }
}

pub(crate) fn pop_complete_frame(
    slots: &mut SequenceMap<RecvSlot>,
    reused_buf: &mut ObjPool<Vec<u8>>,
    scan_start: &mut SequenceNumber,
) -> Option<Vec<u8>> {
    let FrameScan {
        complete,
        abandoned,
    } = find_complete_frame(slots, scan_start);
    // Tombstone the abandoned frames' collected seqs so the in-order cursor
    // can advance past them. Without this, a hostile peer's overlapping
    // frames wedge the receiver: the abandoned frame's retransmissions are
    // swallowed by the interrupting frame's tombstones and the cursor stays
    // pinned at the abandoned frame's start forever.
    for (start, count) in abandoned {
        for offset in 0..count {
            let seq = start.advance(offset);
            if let Some(RecvSlot::Data(pkt)) = slots.insert(seq, RecvSlot::Tombstone) {
                reused_buf.put(pkt.data);
            }
        }
    }
    let (frame_start, packet_count, frame_len) = complete?;
    let mut frame_bytes = Vec::with_capacity(frame_len as usize);
    for offset in 0..packet_count {
        let seq = frame_start.advance(offset);
        if let Some(RecvSlot::Data(pkt)) = slots.insert(seq, RecvSlot::Tombstone) {
            let remaining = frame_len as usize - frame_bytes.len();
            let copy_len = remaining.min(pkt.data.len());
            frame_bytes.extend_from_slice(&pkt.data[..copy_len]);
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
