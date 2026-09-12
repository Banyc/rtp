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

/// Persistent state of an in-progress reassembly scan, saved across
/// `pop_complete_frame` calls so the next scan resumes past the slots it
/// already examined instead of re-walking them.
///
/// The scan is a deterministic function of the ordered slot sequence, so a
/// resumed scan is exactly the tail of the previous one *provided the
/// already-examined prefix is unchanged*. The packet receive space enforces
/// that: a new packet inserted at or before `up_to`, or any head-tombstone
/// collapse / removal / window-anchor advance, drops this state and forces
/// a full rescan, which reproduces the stateless behavior exactly.
///
/// The state is only written by a *benign* scan — one that neither found a
/// complete frame nor abandoned one. A scan that completes or abandons a
/// frame inserts tombstones this call, so its end state cannot be reused
/// and the consumer explicitly clears it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScanResume {
    /// The last occupied sequence the scan examined. Read by the packet
    /// receive space to decide whether a newly inserted packet lands
    /// inside the already-scanned prefix (and so forces a full rescan).
    pub(crate) up_to: SequenceNumber,
    /// Whether the scan had still not seen its first `Data` slot (it was
    /// still skipping leading tombstones).
    scanning_front: bool,
    /// The in-progress frame the scan was accumulating when it stopped, if
    /// any (a contiguity break — a sequence hole or an interrupting frame
    /// start — resets it to `None`).
    frame: Option<FrameProgress>,
}

/// The in-progress frame a scan was accumulating when it stopped. Mirrors
/// the corresponding locals of the scan loop; `end` is the last collected
/// sequence, so the next expected sequence is `end.advance(1)`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FrameProgress {
    start: SequenceNumber,
    end: SequenceNumber,
    packet_count: u64,
    target_len: u32,
    collected: usize,
}

/// Scan state carried between iterations of the reassembly loop. A fresh
/// scan starts with `scanning_front == true` and no frame; a resumed scan
/// continues from the captured [`ScanResume`].
#[derive(Debug, Default)]
struct ScanLocals {
    scanning_front: bool,
    frame_start: Option<SequenceNumber>,
    frame_end: Option<SequenceNumber>,
    packet_count: u64,
    target_len: u32,
    collected: usize,
    abandoned: Vec<(SequenceNumber, u64)>,
}

/// The result of one reassembly scan, plus the resume state to persist for
/// the next scan (or `None` to force a full rescan then). The resume state
/// is `Some` only in the benign end-state above, so persisting it is safe.
struct ScanOutcome {
    scan: FrameScan,
    resume: Option<ScanResume>,
}

fn scan_locals_from_resume(resume: ScanResume) -> ScanLocals {
    ScanLocals {
        scanning_front: resume.scanning_front,
        frame_start: resume.frame.map(|f| f.start),
        frame_end: resume.frame.map(|f| f.end),
        packet_count: resume.frame.map_or(0, |f| f.packet_count),
        target_len: resume.frame.map_or(0, |f| f.target_len),
        collected: resume.frame.map_or(0, |f| f.collected),
        abandoned: Vec::new(),
    }
}

fn frame_progress_from_locals(locals: &ScanLocals) -> Option<FrameProgress> {
    Some(FrameProgress {
        start: locals.frame_start?,
        end: locals.frame_end?,
        packet_count: locals.packet_count,
        target_len: locals.target_len,
        collected: locals.collected,
    })
}

fn find_complete_frame(
    slots: &SequenceMap<RecvSlot>,
    scan_start: &mut SequenceNumber,
    resume: Option<ScanResume>,
) -> ScanOutcome {
    let mut locals = match resume {
        Some(r) => scan_locals_from_resume(r),
        None => ScanLocals {
            scanning_front: true,
            ..ScanLocals::default()
        },
    };
    // Resume where the previous scan left off (the slot after the last one
    // it examined); a full scan starts at the front cursor.
    let start = resume.map_or(*scan_start, |r| r.up_to.advance(1));
    let mut up_to = resume.map(|r| r.up_to);
    let mut complete: Option<(SequenceNumber, u64, u32)> = None;
    for (seq, slot) in slots.iter_from(start) {
        up_to = Some(seq);
        if locals.scanning_front {
            match slot {
                RecvSlot::Tombstone => *scan_start = seq.advance(1),
                RecvSlot::Data(_) => {
                    *scan_start = seq;
                    locals.scanning_front = false;
                }
            }
        }
        match slot {
            RecvSlot::Data(pkt) => {
                if locals.frame_start.is_none() || pkt.frame_len.is_some() {
                    let Some(fl) = pkt.frame_len else {
                        continue;
                    };
                    locals.frame_start = Some(seq);
                    locals.frame_end = Some(seq);
                    locals.packet_count = 1;
                    locals.target_len = fl;
                    locals.collected = pkt.data.len();
                } else {
                    let expected = locals.frame_end.unwrap().advance(1);
                    if seq != expected {
                        locals.frame_start = None;
                        locals.frame_end = None;
                        locals.packet_count = 0;
                        locals.collected = 0;
                        continue;
                    }
                    locals.frame_end = Some(seq);
                    locals.packet_count += 1;
                    locals.collected += pkt.data.len();
                }
                if locals.collected >= locals.target_len as usize {
                    complete = Some((
                        locals.frame_start.unwrap(),
                        locals.packet_count,
                        locals.target_len,
                    ));
                    break;
                }
            }
            RecvSlot::Tombstone => {
                // A tombstone inside an in-progress frame means the frame can
                // never reassemble: its retransmissions are swallowed by the
                // tombstone (recv_bytes treats an occupied slot as a
                // duplicate), so the cursor would stay pinned at the frame's
                // start forever. Record it so the caller can tombstone its
                // collected seqs and let the cursor advance.
                if let Some(start) = locals.frame_start {
                    locals.abandoned.push((start, locals.packet_count));
                }
                locals.frame_start = None;
                locals.frame_end = None;
                locals.packet_count = 0;
                locals.collected = 0;
            }
        }
    }
    let resume = if complete.is_none() && locals.abandoned.is_empty() {
        // Benign end: an incomplete frame (or a run reset by a hole), with
        // neither a completed nor an abandoned frame — this scan inserted
        // no tombstones, so its end state can be reused verbatim next call
        // as long as the map prefix below `up_to` stays unchanged.
        up_to.map(|up_to| ScanResume {
            up_to,
            scanning_front: locals.scanning_front,
            frame: frame_progress_from_locals(&locals),
        })
    } else {
        None
    };
    ScanOutcome {
        scan: FrameScan {
            complete,
            abandoned: locals.abandoned,
        },
        resume,
    }
}

pub(crate) fn pop_complete_frame(
    slots: &mut SequenceMap<RecvSlot>,
    reused_buf: &mut ObjPool<Vec<u8>>,
    scan_start: &mut SequenceNumber,
    resume: &mut Option<ScanResume>,
) -> Option<Vec<u8>> {
    let ScanOutcome {
        scan: FrameScan {
            complete,
            abandoned,
        },
        resume: refresh,
    } = find_complete_frame(slots, scan_start, *resume);
    *resume = refresh;
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
    if packet_count == 1 {
        // Single-packet fast path: the collected slot already holds the
        // whole frame, so hand its Vec straight back. No new allocation,
        // no payload copy — the returned Vec is the pooled buffer the
        // packet was received into. `frame_len` only ever falls short of
        // the slot's payload, never beyond it (the scan labels a frame
        // complete only once the collected bytes reach `frame_len`), so
        // truncation is a pure trim.
        if let Some(RecvSlot::Data(mut pkt)) = slots.insert(frame_start, RecvSlot::Tombstone) {
            if (frame_len as usize) < pkt.data.len() {
                pkt.data.truncate(frame_len as usize);
            }
            return Some(pkt.data);
        }
        // Unreachable after a real scan (the slot was Data); fall through to
        // the generic path, whose first insert misses the same way and
        // reproduces the previous silent short-frame behavior.
    }
    let mut frame_bytes;
    match slots.insert(frame_start, RecvSlot::Tombstone) {
        Some(RecvSlot::Data(mut first)) => {
            // Multi-packet fast path: move the FIRST slot's Vec out and
            // extend it in place with the remaining packets' payloads — one
            // allocation avoided and the first packet's bytes never copied.
            // The scan only labels a frame multi-packet when the first
            // packet carries fewer than `frame_len` bytes (otherwise the
            // first packet alone completes it), so there is always room to
            // top up to the full frame length; reserve it in one shot so
            // the extensions below never reallocate regardless of the
            // first buffer's pooled capacity.
            first
                .data
                .reserve((frame_len as usize).saturating_sub(first.data.len()));
            frame_bytes = first.data;
        }
        _ => {
            // Defensive fallback (unreachable after a real scan): the
            // previous allocate-a-fresh-buffer behavior.
            frame_bytes = Vec::with_capacity(frame_len as usize);
        }
    }
    for offset in 1..packet_count {
        let seq = frame_start.advance(offset);
        if let Some(RecvSlot::Data(pkt)) = slots.insert(seq, RecvSlot::Tombstone) {
            let remaining = (frame_len as usize).saturating_sub(frame_bytes.len());
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
