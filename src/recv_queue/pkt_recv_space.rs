use primitive::arena::obj_pool::{ObjPool, buf_pool};

use crate::{
    ack::AckHistory,
    delivery::frame::recv::{RecvPkt, RecvSlot, ScanResume},
    sequence::{SequenceMap, SequenceNumber, SequencePosition, SequenceVacancyError, le, min},
};

pub const MAX_NUM_RECVING_PKTS: usize = 2 << 14;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecvDisposition {
    Rejected,
    Duplicate,
    Inserted,
}

impl RecvDisposition {
    pub(crate) fn should_ack(self) -> bool {
        matches!(self, Self::Duplicate | Self::Inserted)
    }

    pub(crate) fn is_new(self) -> bool {
        matches!(self, Self::Inserted)
    }
}

#[derive(Debug)]
pub struct PktRecvSpace {
    next: Option<SequenceNumber>,
    slots: SequenceMap<RecvSlot>,
    scan_start: SequenceNumber,
    /// The reassembly scan's persistent end state, valid only while the
    /// already-scanned slot prefix is unchanged. Every mutation that could
    /// change that prefix — a newly inserted packet at or before the
    /// scanned frontier, or any head-tombstone collapse / removal — drops
    /// it so the next scan re-walks from `scan_start` exactly as a
    /// stateless scan would (see [`ScanResume`]).
    scan_resume: Option<ScanResume>,
    reused_buf: ObjPool<Vec<u8>>,
    ack_history: AckHistory,
}

impl PktRecvSpace {
    /// Zero-seeded receive space (connections opened without the handshake).
    pub fn new() -> Self {
        Self::new_at(SequenceNumber::ZERO)
    }

    /// Receive space seeded at `initial_seq` (handshake-derived directional
    /// start): the in-order cursor, the slot window, and the ACK history all
    /// derive from the same initial value.
    pub fn new_at(initial_seq: SequenceNumber) -> Self {
        Self {
            next: Some(initial_seq),
            slots: SequenceMap::new(initial_seq, MAX_NUM_RECVING_PKTS as u64),
            scan_start: initial_seq,
            scan_resume: None,
            reused_buf: buf_pool(Some(MAX_NUM_RECVING_PKTS)),
            ack_history: AckHistory::new_at(initial_seq),
        }
    }

    pub fn ack_history(&self) -> &AckHistory {
        &self.ack_history
    }

    pub fn next_seq(&self) -> Option<SequenceNumber> {
        self.next
    }

    pub fn num_recved_pkts(&self) -> usize {
        self.slots.len()
    }

    /// Whether the bounded receive window is at capacity: every in-window
    /// sequence slot is occupied (Data or Tombstone), so a newly arriving
    /// in-window packet is necessarily a duplicate (acked, not buffered)
    /// and a beyond-window packet is rejected — no further payload memory
    /// can be committed. The slot map's occupancy is bounded by
    /// [`MAX_NUM_RECVING_PKTS`] (insertions outside the live window are
    /// rejected), so this is exactly the memory-saturation point once the
    /// application has stopped draining.
    pub fn is_full(&self) -> bool {
        self.slots.len() >= MAX_NUM_RECVING_PKTS
    }

    pub fn reused_buf(&mut self) -> &mut ObjPool<Vec<u8>> {
        &mut self.reused_buf
    }

    #[cfg(test)]
    pub fn recv(&mut self, seq: u64, data: Vec<u8>, frame_len: Option<u32>) -> bool {
        self.recv_bytes(SequenceNumber::from_wire(seq), &data, frame_len)
            .should_ack()
    }

    #[cfg(test)]
    fn recv_disposition(
        &mut self,
        seq: SequenceNumber,
        data: Vec<u8>,
        frame_len: Option<u32>,
    ) -> RecvDisposition {
        self.recv_bytes(seq, &data, frame_len)
    }

    pub(crate) fn recv_bytes(
        &mut self,
        seq: SequenceNumber,
        data: &[u8],
        frame_len: Option<u32>,
    ) -> RecvDisposition {
        if let Some(frame_len) = frame_len
            && !crate::delivery::frame::send::is_valid_frame_len(frame_len)
        {
            return RecvDisposition::Rejected;
        }
        if self.next.is_none() {
            return RecvDisposition::Rejected;
        }
        let reused_buf = &mut self.reused_buf;
        match self.slots.insert_vacant_with(seq, || {
            let mut owned = reused_buf.take();
            owned.extend(data);
            RecvSlot::Data(RecvPkt {
                data: owned,
                frame_len,
            })
        }) {
            Ok(()) => {}
            Err(SequenceVacancyError::Occupied)
            | Err(SequenceVacancyError::Outside(SequencePosition::Stale)) => {
                return RecvDisposition::Duplicate;
            }
            Err(SequenceVacancyError::Outside(_)) => return RecvDisposition::Rejected,
        }
        self.scan_start = min(self.scan_start, seq);
        // A packet landing at or before the reassembly scan's cached
        // frontier rewrites the already-scanned prefix (it may fill a
        // hole or start a new frame mid-run), so the resume state must be
        // dropped: the next scan re-walks from `scan_start`.
        if self.scan_resume.is_some_and(|r| le(seq, r.up_to)) {
            self.scan_resume = None;
        }
        self.ack_history.insert_in_window(seq);
        RecvDisposition::Inserted
    }

    /// Strict ordered frame delivery: equivalent to
    /// [`Self::pop_complete_frame_with_reorder`] with the receiver-side
    /// fast-forward disabled. Kept as the default entry point so callers that
    /// do not opt in are byte-for-byte unchanged.
    pub fn pop_complete_frame(&mut self) -> Option<Vec<u8>> {
        self.pop_complete_frame_with_reorder(false)
    }

    /// Pop the next deliverable complete frame.
    ///
    /// With `allow_reorder == false` a frame is delivered only when it begins
    /// at the in-order front (strict, gap-free ordering). With
    /// `allow_reorder == true` a complete frame starting past an unrepaired
    /// hole is also delivered, while `next` stays pinned at the hole and the
    /// frame's sequence numbers are tombstoned so the cursor collapses them
    /// once the hole fills. Delivery never changes the ACKs already recorded
    /// by [`Self::recv_bytes`], so liveness is unaffected either way.
    pub fn pop_complete_frame_with_reorder(&mut self, allow_reorder: bool) -> Option<Vec<u8>> {
        let frame_bytes = crate::delivery::frame::recv::pop_complete_frame(
            &mut self.slots,
            &mut self.reused_buf,
            &mut self.scan_start,
            &mut self.scan_resume,
            self.next,
            allow_reorder,
        );
        // Collapse the head-tombstone prefix whether or not a frame was
        // found: the scan may have tombstoned abandoned frames (a hostile
        // peer's overlapping frames) with no complete frame to return, and
        // the cursor must still advance past them or the receiver wedges.
        // Out-of-order tombstones are past the pinned front, so they are
        // retained until `next` reaches them.
        self.collapse_tombstone_prefix();
        frame_bytes
    }

    /// Advance `next` past contiguous tombstone(s) at the head of the slot
    /// map, removing them.  This is what bounds memory: tombstones count
    /// toward the window until collapsed.  The slot window anchor moves with
    /// `next`, and `scan_start` is reset when it leaves the window.
    fn collapse_tombstone_prefix(&mut self) {
        let Some(mut next) = self.next else {
            return;
        };
        let mut collapsed = false;
        while let Some(RecvSlot::Tombstone) = self.slots.get(&next) {
            self.slots.remove(&next);
            collapsed = true;
            next = next.advance(1);
        }
        self.next = Some(next);
        if collapsed {
            // Head-tombstone removal (and any anchor advance below)
            // rewrites the scanned prefix, so a resumed scan would mis-read
            // it: drop the resume state and let the next scan walk from
            // scratch.
            self.scan_resume = None;
        }
        if self.slots.window().anchor() == next {
            return;
        }
        self.slots.advance_anchor(next);
        if next.forward_distance_to(self.scan_start) > crate::sequence::HALF_SEQUENCE_SPACE {
            self.scan_start = next;
        }
    }

    pub fn peek(&self) -> Option<&Vec<u8>> {
        let next = self.next?;
        match self.slots.get(&next)? {
            RecvSlot::Data(pkt) => Some(&pkt.data),
            RecvSlot::Tombstone => None,
        }
    }

    /// In frame-delivery mode, check whether a FIN (empty-payload packet
    /// with no `frame_len`) is sitting at the in-order head of the receive
    /// queue — i.e. all earlier packets have been consumed or tombstoned.
    /// This is the EOF signal for frame mode: `move_recv_data` is bypassed
    /// in frame mode, so the `recv_fin_buf` flag is never set by that path.
    /// Instead, the caller checks this method directly to surface EOF.
    ///
    /// Returns `true` only when:
    /// - `next` is `Some(n)` and a `Data` slot exists at `n`, and
    /// - that slot's payload is empty (`data.is_empty()`), and
    /// - that slot has no `frame_len` (it's a FIN, not a zero-length frame).
    pub fn fin_at_head(&self) -> bool {
        crate::delivery::frame::recv::fin_at_head(self.next, &self.slots)
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        // A stock-mode pop removes a slot the reassembly scan may have
        // examined already; never resume across it.
        self.scan_resume = None;
        loop {
            let next = self.next?;
            match self.slots.remove(&next) {
                Some(RecvSlot::Data(pkt)) => {
                    self.next = Some(next.advance(1));
                    self.collapse_tombstone_prefix();
                    return Some(pkt.data);
                }
                Some(RecvSlot::Tombstone) => {
                    self.next = Some(next.advance(1));
                    continue;
                }
                None => {
                    return None;
                }
            }
        }
    }
}

impl Default for PktRecvSpace {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(n: u64) -> SequenceNumber {
        SequenceNumber::from_wire(n)
    }

    #[test]
    fn stock_recv_pop_in_order() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"hello".to_vec(), None));
        assert!(space.recv(1, b"world".to_vec(), None));
        assert_eq!(space.peek().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b"world");
        assert!(space.pop().is_none());
    }

    #[test]
    fn stock_recv_out_of_order() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(1, b"world".to_vec(), None));
        assert!(space.recv(0, b"hello".to_vec(), None));
        assert_eq!(space.pop().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b"world");
    }

    #[test]
    fn stale_seq_is_acked_but_not_inserted() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"a".to_vec(), None));
        let ack_count_after = space.ack_history().blocks().count();
        space.pop();
        assert_eq!(
            space.recv_disposition(seq(0), b"stale".to_vec(), None),
            RecvDisposition::Duplicate
        );
        assert!(!space.slots.contains_key(&seq(0)));
        assert_eq!(space.ack_history().blocks().count(), ack_count_after);
    }

    #[test]
    fn window_capacity_rejects() {
        let mut space = PktRecvSpace::new();
        let far_seq = MAX_NUM_RECVING_PKTS as u64; // seq 0 is next, so seq 8192 is out of window
        assert!(!space.recv(far_seq, b"x".to_vec(), None));
    }

    #[test]
    fn window_is_full_at_exactly_the_capacity() {
        let mut space = PktRecvSpace::new();
        // Fill the receive window to every in-window sequence: the window is
        // full at exactly MAX_NUM_RECVING_PKTS occupied slots (the anchor
        // never advances because nothing is popped), not one slot later.
        for i in 0..MAX_NUM_RECVING_PKTS as u64 {
            assert!(
                space.recv(i, b"x".to_vec(), None),
                "in-window seq {i} must be accepted"
            );
        }
        assert!(
            space.is_full(),
            "the window is full once every in-window slot is occupied"
        );
        // The next sequence is beyond the window: rejected, so under a
        // strict `>` comparison is_full could never become true.
        assert!(!space.recv(MAX_NUM_RECVING_PKTS as u64, b"x".to_vec(), None));
        assert!(space.is_full());
    }

    #[test]
    fn duplicate_acked_but_not_reinserted() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"a".to_vec(), None));
        assert_eq!(
            space.recv_disposition(seq(0), b"b".to_vec(), None),
            RecvDisposition::Duplicate
        );
        let pkt = space.slots.get(&seq(0)).unwrap();
        match pkt {
            RecvSlot::Data(p) => assert_eq!(p.data, b"a"),
            _ => panic!("expected data"),
        }
    }

    #[test]
    fn fin_empty_payload_no_frame_len() {
        let mut space = PktRecvSpace::new();
        // FIN: empty payload, no frame_len.
        assert!(space.recv(0, vec![], None));
        assert_eq!(space.peek().unwrap().len(), 0);
        let popped = space.pop().unwrap();
        assert!(popped.is_empty());
    }

    #[test]
    fn tombstone_prefix_collapses() {
        let mut space = PktRecvSpace::new();
        // Insert tombstones ahead of the cursor.
        space.slots.insert(seq(0), RecvSlot::Tombstone);
        space.slots.insert(seq(1), RecvSlot::Tombstone);
        space.slots.insert(
            seq(2),
            RecvSlot::Data(RecvPkt {
                data: b"hello".to_vec(),
                frame_len: None,
            }),
        );
        space.next = Some(seq(0));
        space.collapse_tombstone_prefix();
        assert_eq!(space.next, Some(seq(2)));
        assert_eq!(space.pop().unwrap(), b"hello");
    }

    #[test]
    fn pop_skips_head_tombstones() {
        let mut space = PktRecvSpace::new();
        space.slots.insert(seq(0), RecvSlot::Tombstone);
        space.slots.insert(
            seq(1),
            RecvSlot::Data(RecvPkt {
                data: b"world".to_vec(),
                frame_len: None,
            }),
        );
        space.next = Some(seq(0));
        // First pop skips tombstone at 0.
        assert_eq!(space.pop().unwrap(), b"world");
        assert_eq!(space.next, Some(seq(2)));
    }

    // Frame delivery tests — enabled mode.

    // Ordered, gap-free delivery: a complete frame past an unrepaired front
    // hole is withheld — the front hole must fill (retransmission or
    // reordering) before the later frame is handed up.
    #[test]
    fn a_complete_frame_past_a_front_hole_is_withheld_until_the_hole_fills() {
        let mut space = PktRecvSpace::new();
        // Hole at seq 0; a complete 1-packet frame arrives at seq 1.
        assert!(space.recv(1, b"frame1".to_vec(), Some(6)));
        // Complete but past the hole: withheld, its slot untouched.
        assert!(space.pop_complete_frame().is_none());
        assert!(matches!(space.slots.get(&seq(1)), Some(RecvSlot::Data(_))));
        // Fill the hole with a frame of its own.
        assert!(space.recv(0, b"late".to_vec(), Some(4)));
        // The front frame delivers first, then the withheld one, in order.
        assert_eq!(space.pop_complete_frame().unwrap(), b"late");
        assert_eq!(space.pop_complete_frame().unwrap(), b"frame1");
        // Both frames delivered; the cursor reached past their range.
        assert_eq!(space.next, Some(seq(2)));
        assert_eq!(space.slots.len(), 0);
    }

    // The default entry point and an explicit `false` are the strict ordered
    // path: a complete frame past a front hole is withheld and its slot is
    // left in place, byte-for-byte the pre-fast-forward behaviour.
    #[test]
    fn default_and_explicit_strict_make_the_same_withholding_decision() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(1, b"frame1".to_vec(), Some(6)));
        assert!(space.pop_complete_frame().is_none());
        assert!(matches!(space.slots.get(&seq(1)), Some(RecvSlot::Data(_))));
        assert!(space.pop_complete_frame_with_reorder(false).is_none());
        assert!(matches!(space.slots.get(&seq(1)), Some(RecvSlot::Data(_))));
        assert_eq!(space.next, Some(seq(0)));
    }

    // Opt-in receiver-side fast-forward: a complete frame past an unrepaired
    // front hole is delivered immediately, but `next` stays pinned at the
    // hole and the frame's seqs are tombstoned (retained, not collapsed)
    // until the hole fills.
    #[test]
    fn opt_in_reorder_delivers_a_complete_frame_past_a_front_hole_while_the_cursor_stays_pinned() {
        let mut space = PktRecvSpace::new();
        // Hole at seq 0; a complete 1-packet frame arrives at seq 1.
        assert!(space.recv(1, b"frame1".to_vec(), Some(6)));
        // Opted in: the complete frame is delivered immediately.
        assert_eq!(
            space.pop_complete_frame_with_reorder(true).unwrap(),
            b"frame1"
        );
        // `next` is still pinned at the hole; the delivered frame's seq is a
        // tombstone retained ahead of the hole (not collapsed).
        assert_eq!(space.next, Some(seq(0)));
        assert!(matches!(
            space.slots.get(&seq(1)),
            Some(RecvSlot::Tombstone)
        ));
        // The cumulative ACK front is likewise pinned at the hole, so the
        // peer's liveness watchdog still sees no cumulative progress.
        assert_eq!(space.ack_history().next(), seq(0));
        // Nothing else is deliverable.
        assert!(space.pop_complete_frame_with_reorder(true).is_none());
        // The hole finally fills: the cursor advances through the hole and
        // then collapses the retained tombstone without redelivering.
        assert!(space.recv(0, b"late".to_vec(), Some(4)));
        assert_eq!(
            space.pop_complete_frame_with_reorder(true).unwrap(),
            b"late"
        );
        assert_eq!(space.next, Some(seq(2)));
        assert_eq!(space.slots.len(), 0);
    }

    // Fast-forward delivers a multi-packet frame whose packets arrived
    // shuffled behind a front hole, and the whole range tombstone-collapses
    // once the hole fills.
    #[test]
    fn opt_in_reorder_delivers_a_multi_packet_frame_past_a_hole() {
        let mut space = PktRecvSpace::new();
        // Hole at seq 0. Frame B: seqs 1 (start, 11 bytes), 2, 3 (cont).
        assert!(space.recv(1, b"hello ".to_vec(), Some(11)));
        assert!(space.recv(3, b"ld".to_vec(), None));
        assert!(space.recv(2, b"wor".to_vec(), None));
        assert_eq!(
            space.pop_complete_frame_with_reorder(true).unwrap(),
            b"hello world"
        );
        // Cursor pinned; all three seqs tombstoned.
        assert_eq!(space.next, Some(seq(0)));
        for s in 1..=3 {
            assert!(matches!(
                space.slots.get(&seq(s)),
                Some(RecvSlot::Tombstone)
            ));
        }
        // Hole fills; cursor collapses the tombstones and no frame is
        // redelivered.
        assert!(space.recv(0, b"x".to_vec(), Some(1)));
        assert_eq!(space.pop_complete_frame_with_reorder(true).unwrap(), b"x");
        assert_eq!(space.next, Some(seq(4)));
        assert_eq!(space.slots.len(), 0);
        assert!(space.pop_complete_frame_with_reorder(true).is_none());
    }

    // A later frame delivered via fast-forward leaves tombstones past an
    // earlier frame's still-unrepaired hole. A tombstone *beyond* the hole
    // must not abandon the earlier frame: its missing continuation is not
    // captured (the slot is vacant), so the in-flight packet can still fill
    // it and the frame reassemble. (Regression: the scan used to abandon the
    // earlier frame at the first tombstone it met while a frame was in
    // progress, tombstoning its collected seqs so the late continuation
    // could never complete it — the frame was permanently lost.)
    #[test]
    fn fast_forward_tombstone_past_a_hole_does_not_abandon_the_earlier_frame() {
        let mut space = PktRecvSpace::new();
        // Front hole at seq 0 (still in flight). Frame A spans seqs 1..=3
        // (frame_len 9) with seq 3 still in flight (a hole inside it).
        // Frame B spans seqs 4..=5 (frame_len 6) and is complete.
        assert!(space.recv(1, b"AAA".to_vec(), Some(9)));
        assert!(space.recv(2, b"BBB".to_vec(), None));
        assert!(space.recv(4, b"DDD".to_vec(), Some(6)));
        assert!(space.recv(5, b"EEE".to_vec(), None));
        // Fast-forward delivers B, tombstoning seqs 4 and 5.
        assert_eq!(
            space.pop_complete_frame_with_reorder(true).unwrap(),
            b"DDDEEE"
        );
        assert!(matches!(
            space.slots.get(&seq(4)),
            Some(RecvSlot::Tombstone)
        ));
        assert!(matches!(
            space.slots.get(&seq(5)),
            Some(RecvSlot::Tombstone)
        ));
        // The next scan meets those tombstones while A is still in progress
        // with its seq-3 continuation missing. That must not abandon A, so
        // A's collected packets stay in place for the eventual reassembly.
        assert!(space.pop_complete_frame_with_reorder(true).is_none());
        assert!(matches!(space.slots.get(&seq(1)), Some(RecvSlot::Data(_))));
        assert!(matches!(space.slots.get(&seq(2)), Some(RecvSlot::Data(_))));
        // A's in-flight continuation arrives: the frame must reassemble and
        // deliver, exactly as it would without the fast-forward tombstones.
        assert!(space.recv(3, b"CCC".to_vec(), None));
        assert_eq!(
            space.pop_complete_frame_with_reorder(true).unwrap(),
            b"AAABBBCCC"
        );
    }

    // A tombstone landing *exactly* on the in-progress frame's next
    // continuation captures that slot forever, so the frame is genuinely
    // unreparable and is still abandoned (its collected seqs tombstoned).
    // This pins the fast-forward fix's boundary: only a tombstone *beyond*
    // the missing continuation is benign.
    #[test]
    fn tombstone_on_an_in_progress_frames_continuation_still_abandons() {
        let mut space = PktRecvSpace::new();
        // A frame at seq 2 is delivered first (fast-forwarded past the front
        // holes), leaving a tombstone there before frame A exists.
        assert!(space.recv(2, b"ZZZ".to_vec(), Some(3)));
        assert_eq!(space.pop_complete_frame_with_reorder(true).unwrap(), b"ZZZ");
        assert!(matches!(
            space.slots.get(&seq(2)),
            Some(RecvSlot::Tombstone)
        ));
        // Frame A now spans seqs 0..=2 (frame_len 9), needing seq 2 — which
        // the tombstone has captured forever.
        assert!(space.recv(0, b"AAA".to_vec(), Some(9)));
        assert!(space.recv(1, b"BBB".to_vec(), None));
        // A is unreparable: it is abandoned and its collected seqs are
        // tombstoned so the cursor advances past them instead of wedging.
        assert!(space.pop_complete_frame_with_reorder(true).is_none());
        assert_eq!(space.next, Some(seq(3)));
    }

    #[test]
    fn multi_packet_frame_waits_for_all_its_packets() {
        let mut space = PktRecvSpace::new();
        // Frame of 11 bytes across 3 packets: seq 0 (start, 6 bytes), seq 1 (3 bytes), seq 2 (2 bytes).
        // Send all but the last.
        assert!(space.recv(0, b"hello ".to_vec(), Some(11)));
        assert!(space.recv(1, b"wor".to_vec(), None));
        // Frame is incomplete (11 bytes not collected yet).
        assert!(space.pop_complete_frame().is_none());
        // Send the last packet.
        assert!(space.recv(2, b"ld".to_vec(), None));
        // Now complete.
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"hello world");
    }

    #[test]
    fn frame_reassembly_survives_shuffled_arrival() {
        let mut space = PktRecvSpace::new();
        // Frame A: seq 0 (start, len 4), seq 1 (cont)
        // Frame B: seq 2 (start, len 4), seq 3 (cont)
        // Deliver in reverse order to test contiguity handling.
        assert!(space.recv(3, b"B2".to_vec(), None));
        assert!(space.recv(2, b"B1".to_vec(), Some(4)));
        assert!(space.recv(1, b"A2".to_vec(), None));
        assert!(space.recv(0, b"A1".to_vec(), Some(4)));

        // Both frames should deliver in order of their start sequences.
        let frame_a = space.pop_complete_frame().unwrap();
        let frame_b = space.pop_complete_frame().unwrap();

        assert_eq!(frame_a, b"A1A2");
        assert_eq!(frame_b, b"B1B2");

        // All tombstones were collapsed by pop_complete_frame's internal
        // collapse_tombstone_prefix.  The in-order cursor should be at 4.
        assert_eq!(space.next, Some(seq(4)));
        assert_eq!(space.slots.len(), 0);
    }

    #[test]
    fn interrupted_frame_does_not_wedge_the_receiver() {
        let mut space = PktRecvSpace::new();
        // Frame A (seqs 0-3, frame_len 80) is interrupted by a hostile
        // frame-start at seq 3 (frame B, seqs 3-6): the foreign start lands
        // exactly on A's next continuation, capturing that slot forever — A
        // can never reassemble, so the scan abandons A (collecting 0-2 for
        // tombstoning) at the collision. B is then at the in-order front
        // (A's abandonment tombstones collapse past it) and delivers. A's
        // retransmissions of 3-5 are swallowed by B's delivery tombstones
        // (occupied slots read as duplicates). The cursor advances past 0-6
        // instead of wedging on A forever.
        assert!(space.recv(0, b"AAAAAAAAAAAAAAAAAAAA".to_vec(), Some(80)));
        assert!(space.recv(1, b"BBBBBBBBBBBBBBBBBBBB".to_vec(), None));
        assert!(space.recv(2, b"CCCCCCCCCCCCCCCCCCCC".to_vec(), None));
        assert!(space.recv(3, b"DDDDDDDDDDDDDDDDDDDD".to_vec(), Some(80)));
        assert!(space.recv(4, b"EEEEEEEEEEEEEEEEEEEE".to_vec(), None));
        assert!(space.recv(5, b"FFFFFFFFFFFFFFFFFFFF".to_vec(), None));
        assert!(space.recv(6, b"GGGGGGGGGGGGGGGGGGGG".to_vec(), None));
        // B completes first (the scan restarts at the colliding frame-start).
        assert_eq!(
            space.pop_complete_frame().unwrap(),
            b"DDDDDDDDDDDDDDDDDDDDEEEEEEEEEEEEEEEEEEEEFFFFFFFFFFFFFFFFFFFFGGGGGGGGGGGGGGGGGGGG"
        );
        // A's retransmissions of 3-5 are acked as duplicates (occupied
        // slots) but never inserted — they cannot repair A.
        assert!(space.recv(3, b"AAAAAAAAAAAAAAAAAAAA".to_vec(), None));
        assert!(space.recv(4, b"BBBBBBBBBBBBBBBBBBBB".to_vec(), None));
        assert!(space.recv(5, b"CCCCCCCCCCCCCCCCCCCC".to_vec(), None));
        // No complete frame remains, but the abandoned frame A (0-2) is
        // tombstoned and the cursor advances past 0-6 instead of wedging.
        assert!(space.pop_complete_frame().is_none());
        assert_eq!(space.next, Some(seq(7)));
    }

    #[test]
    fn legitimate_out_of_order_frames_still_reassemble_after_a_collision() {
        let mut space = PktRecvSpace::new();
        // Frame A (seqs 0-3, frame_len 80) and frame B (seqs 4-6, frame_len
        // 60) are contiguous from the sender's perspective, but B's packets
        // arrive before A's continuation at seq 3. B's start is one past the
        // hole, so A stays repairable (no abandonment — nothing captured its
        // continuation) while B is complete but withheld: nothing delivers
        // until A's hole fills, then both deliver in order.
        assert!(space.recv(0, b"AAAAAAAAAAAAAAAAAAAA".to_vec(), Some(80)));
        assert!(space.recv(1, b"BBBBBBBBBBBBBBBBBBBB".to_vec(), None));
        assert!(space.recv(2, b"CCCCCCCCCCCCCCCCCCCC".to_vec(), None));
        assert!(space.recv(4, b"DDDDDDDDDDDDDDDDDDDD".to_vec(), Some(60)));
        assert!(space.recv(5, b"EEEEEEEEEEEEEEEEEEEE".to_vec(), None));
        assert!(space.recv(6, b"FFFFFFFFFFFFFFFFFFFF".to_vec(), None));
        // B is complete but begins past A's hole: withheld until A repairs.
        assert!(space.pop_complete_frame().is_none());
        // A's continuation arrives at a vacant slot (B never overlapped A).
        assert!(space.recv(3, b"DDDDDDDDDDDDDDDDDDDD".to_vec(), None));
        // A reassembles and delivers first (in order), then B.
        assert_eq!(
            space.pop_complete_frame().unwrap(),
            b"AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBCCCCCCCCCCCCCCCCCCCCDDDDDDDDDDDDDDDDDDDD"
        );
        assert_eq!(
            space.pop_complete_frame().unwrap(),
            b"DDDDDDDDDDDDDDDDDDDDEEEEEEEEEEEEEEEEEEEEFFFFFFFFFFFFFFFFFFFF"
        );
        assert_eq!(space.next, Some(seq(7)));
    }

    #[test]
    fn single_packet_frame_is_returned_without_reallocation_or_copy() {
        let mut space = PktRecvSpace::new();
        // Stage the frame as the pooled payload buffer the receive path
        // stores in the slot: take a buffer from the pool, fill it, insert
        // it (frame declares only 4 of the 8 staged bytes as its length,
        // like `a_frame_is_delivered_at_its_declared_length`).
        let mut payload = space.reused_buf().take();
        payload.extend_from_slice(b"ABCDEFGH");
        let payload_ptr = payload.as_ptr();
        let payload_cap = payload.capacity();
        space.slots.insert(
            seq(0),
            RecvSlot::Data(RecvPkt {
                data: payload,
                frame_len: Some(4),
            }),
        );
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"ABCD");
        assert_eq!(
            frame.as_ptr(),
            payload_ptr,
            "a single-packet frame must be handed back as the slot's own \
             buffer, not copied into a fresh allocation"
        );
        assert_eq!(
            frame.capacity(),
            payload_cap,
            "the returned frame must reuse the pooled allocation untouched"
        );
    }

    #[test]
    fn multi_packet_frame_reuses_the_first_slot_buffer_and_is_byte_identical() {
        let mut space = PktRecvSpace::new();
        // First packet from the pool, pre-sized so the frame tops up in
        // place without growing; later packets are ordinary payloads.
        let mut first = space.reused_buf().take();
        first.reserve(11);
        first.extend_from_slice(b"hello ");
        let first_ptr = first.as_ptr();
        // Ordered delivery only hands up frames at the front, so seat a
        // single-packet frame at seq 0 and deliver it first; the multi-packet
        // frame below then starts at the in-order cursor (seq 1).
        space.slots.insert(
            seq(0),
            RecvSlot::Data(RecvPkt {
                data: b"_".to_vec(),
                frame_len: Some(1),
            }),
        );
        assert_eq!(space.pop_complete_frame().unwrap(), b"_");
        space.slots.insert(
            seq(1),
            RecvSlot::Data(RecvPkt {
                data: first,
                frame_len: Some(11),
            }),
        );
        space.slots.insert(
            seq(2),
            RecvSlot::Data(RecvPkt {
                data: b"wor".to_vec(),
                frame_len: None,
            }),
        );
        space.slots.insert(
            seq(3),
            RecvSlot::Data(RecvPkt {
                data: b"ld".to_vec(),
                frame_len: None,
            }),
        );
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"hello world");
        assert_eq!(
            frame.as_ptr(),
            first_ptr,
            "the frame must extend the first packet's buffer in place, \
             not allocate a fresh one"
        );
    }

    #[test]
    fn resume_scan_is_dropped_when_a_hole_below_the_scanned_frontier_is_filled() {
        let mut space = PktRecvSpace::new();
        // Frame A across seqs 0..=2 with a hole at seq 1: the first scan
        // examines 0 and 2, resets the run at the hole, and caches its end
        // state at seq 2.
        assert!(space.recv(0, b"AA".to_vec(), Some(6)));
        assert!(space.recv(2, b"BB".to_vec(), None));
        assert!(space.pop_complete_frame().is_none());
        // Filling the hole at seq 1 lands at-or-below the cached frontier
        // (seq 2): it must invalidate the resume state so the next scan
        // re-walks the frame from its start instead of resuming past it.
        assert!(space.recv(1, b"CC".to_vec(), None));
        assert_eq!(space.pop_complete_frame().unwrap(), b"AACCBB");
        assert_eq!(space.next, Some(seq(3)));
    }

    // Frame boundaries are contiguous from a legitimate sender, so a frame
    // start landing exactly on an in-progress frame's next continuation means
    // that continuation slot is captured forever (the interrupting packet
    // occupies it): the old frame is unreparable and is abandoned, and the
    // colliding packet's own frame_len still starts a NEW run (a run is
    // never extended across a frame boundary).
    #[test]
    fn frame_start_on_an_in_progress_frames_continuation_abandons_the_old_frame() {
        let mut space = PktRecvSpace::new();
        // Seed the front so the collision below sits at the in-order cursor.
        assert!(space.recv(0, b"ab".to_vec(), Some(2)));
        assert_eq!(space.pop_complete_frame().unwrap(), b"ab");
        // Frame A: seq 1 (start, len 6) — incomplete, 4 of 6 bytes collected.
        // seq 2 is a NEW frame start (len 3) colliding exactly on A's next
        // continuation: A's seq-2 packet can never be inserted, so A is
        // abandoned and the new run starts at seq 2. seq 3 is an orphaned
        // continuation (the run never extends across the boundary).
        assert!(space.recv(1, b"fram".to_vec(), Some(6)));
        assert!(space.recv(2, b"new".to_vec(), Some(3)));
        assert!(space.recv(3, b"foo".to_vec(), None));
        // B completes at its single packet (3 bytes = frame_len 3) and is at
        // the front after A's abandonment tombstone collapses.
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"new");
        // A's collected seq 1 was abandoned; the cursor advanced past it.
        assert_eq!(space.next, Some(seq(3)));
    }

    #[test]
    fn ack_generation_unaffected_by_early_delivery() {
        let mut space = PktRecvSpace::new();
        // Insert packets out of order and verify ack_history is stock.
        assert!(space.recv(2, b"c".to_vec(), None));
        assert!(space.recv(0, b"a".to_vec(), None));
        assert!(space.recv(1, b"b".to_vec(), None));

        // ack_history should have folded the in-order run into the
        // cumulative front (no selective ranges remain).
        assert_eq!(space.ack_history().next(), seq(3));
        assert_eq!(space.ack_history().blocks().count(), 0);

        // After pop_complete_frame or pop, ack_history is NOT touched.
        space.pop();
        assert_eq!(space.ack_history().next(), seq(3));
        assert_eq!(space.ack_history().blocks().count(), 0);
    }

    // A delivered frame's tombstones are collapsed immediately at the front:
    // ordered delivery only hands frames up at the cursor, so a frame's own
    // tombstone is always head-contiguous and collapses. The window-pinning
    // case is therefore an ABANDONED frame's tombstones sitting ahead of an
    // unrepaired hole — they count toward the window until the front catches
    // up and collapses them.
    #[test]
    fn abandoned_frame_tombstones_count_toward_the_window() {
        let mut space = PktRecvSpace::new();
        // Frame X (seqs 1-2, frame_len 80) is interrupted by a hostile
        // frame-start at seq 2 — exactly X's continuation — so X is
        // abandoned when the scan sees the collision.
        assert!(space.recv(1, b"XXXXXXXXXXXXXXXXXXXX".to_vec(), Some(80)));
        assert!(space.recv(2, b"YYYYYYYYYYYYYYYYYYYY".to_vec(), Some(80)));
        space.pop_complete_frame();
        // seq 1 is now an abandonment tombstone; seq 0 is an unfilled hole.
        assert!(matches!(
            space.slots.get(&seq(1)),
            Some(RecvSlot::Tombstone)
        ));
        assert!(!space.slots.contains_key(&seq(0)));
        // The tombstone at seq 1 keeps `next` pinned at 0 (hole at 0 prevents
        // collapse), so the window is [0, 8191].  seq 8192 is out of window.
        let out_of_window = MAX_NUM_RECVING_PKTS as u64;
        assert!(!space.recv(out_of_window, b"reject".to_vec(), None));
        // seq 8191 is the last in-window seq.
        let last_in_window = out_of_window - 1;
        assert!(space.recv(last_in_window, b"ok".to_vec(), None));
    }

    #[test]
    fn mode_off_is_stock_byte_identical() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"hello".to_vec(), None));
        assert!(space.recv(2, b"world".to_vec(), None));
        assert!(space.recv(1, b" ".to_vec(), None));

        assert_eq!(space.pop().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b" ");
        assert_eq!(space.pop().unwrap(), b"world");
        assert!(space.pop().is_none());
    }

    /// Fix #8: `fin_at_head_after_all_data_delivered` — a FIN (empty-payload
    /// packet with no `frame_len`) delivered in frame mode after all data
    /// surfaces EOF via `fin_at_head()`.  Before the fix, `recv_data_pkt`
    /// returned early in frame mode bypassing `move_recv_data` (the only
    /// place that set `recv_fin_buf`), so an in-order FIN yielded
    /// `WouldBlock` forever (connections hung on close).
    #[test]
    fn fin_at_head_after_all_data_delivered() {
        let mut space = PktRecvSpace::new();
        // Deliver a data frame at seq 0, then a FIN at seq 1.
        assert!(space.recv(0, b"data".to_vec(), Some(4)));
        assert!(space.recv(1, vec![], None));

        // Pop the data frame first.
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"data");

        // Now the FIN is at the in-order head (seq 1, after seq 0 was
        // tombstoned and collapsed).  `fin_at_head()` must return true.
        // First, the tombstone at seq 0 must be collapsed.
        space.collapse_tombstone_prefix();
        assert!(
            space.fin_at_head(),
            "FIN at in-order head must be detected after all data delivered"
        );
    }

    #[test]
    fn a_frame_is_delivered_at_its_declared_length() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"ABCDEFGH".to_vec(), Some(4)));
        assert_eq!(space.pop_complete_frame().unwrap(), b"ABCD");
    }

    #[test]
    fn a_multi_packet_frame_stops_at_its_declared_length() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"AB".to_vec(), Some(5)));
        assert!(space.recv(1, b"CDEFGH".to_vec(), None));
        assert_eq!(space.pop_complete_frame().unwrap(), b"ABCDE");
    }

    #[test]
    fn oversize_frame_len_cannot_pin_the_in_order_cursor() {
        let mut space = PktRecvSpace::new();
        assert_eq!(
            space.recv_disposition(seq(0), b"poison".to_vec(), Some(u32::MAX)),
            RecvDisposition::Rejected
        );
        assert!(space.recv(0, b"real".to_vec(), Some(4)));
        assert_eq!(space.pop_complete_frame().unwrap(), b"real");
        assert_eq!(space.next, Some(seq(1)));
        assert_eq!(space.slots.len(), 0);
    }

    #[test]
    fn zero_frame_len_is_rejected() {
        let mut space = PktRecvSpace::new();
        assert_eq!(
            space.recv_disposition(seq(0), vec![], Some(0)),
            RecvDisposition::Rejected
        );
        assert_eq!(space.slots.len(), 0);
        assert!(space.pop_complete_frame().is_none());
    }

    #[test]
    fn max_frame_len_is_still_accepted() {
        let mut space = PktRecvSpace::new();
        let max = crate::delivery::frame::send::MAX_FRAME_LEN;
        assert_eq!(
            space.recv_disposition(seq(0), vec![0u8; 1], Some(max as u32)),
            RecvDisposition::Inserted
        );
    }

    /// Fix #8 (inverse): `fin_behind_gap_does_not_surface_eof` — a FIN
    /// behind an out-of-order gap does NOT surface EOF until the gap fills.
    #[test]
    fn fin_behind_gap_does_not_surface_eof() {
        let mut space = PktRecvSpace::new();
        // Gap at seq 0; FIN at seq 1 (but seq 0 is missing).
        assert!(space.recv(1, vec![], None));

        // The FIN is NOT at the in-order head (seq 0 is missing).
        assert!(
            !space.fin_at_head(),
            "FIN behind a gap must not surface EOF"
        );

        // Fill the gap with a data frame.
        assert!(space.recv(0, b"late".to_vec(), Some(4)));
        // Pop the data frame.
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"late");
        space.collapse_tombstone_prefix();

        // Now the FIN is at the in-order head.
        assert!(
            space.fin_at_head(),
            "FIN must surface EOF after the gap fills"
        );
    }

    /// EOF is signalled by an *empty-payload packet with no declared frame
    /// length*. A frame *start* whose first packet happens to carry zero
    /// payload bytes declares a frame length, so it is not a FIN and must not
    /// surface EOF: treating it as one would truncate the stream at a frame
    /// boundary whose remaining packets have not arrived yet.
    #[test]
    fn an_empty_payload_frame_start_is_not_a_fin() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, vec![], Some(6)));
        assert!(
            !space.fin_at_head(),
            "a zero-payload frame start (frame_len = 6) must not surface EOF"
        );
        // The frame completes once its continuation arrives.
        assert!(space.recv(1, b"ABCDEF".to_vec(), None));
        assert_eq!(space.pop_complete_frame().unwrap(), b"ABCDEF");
        // A real FIN — empty payload, no frame_len — still surfaces EOF.
        assert!(space.recv(2, vec![], None));
        assert!(space.fin_at_head());
    }

    #[test]
    fn a_late_packet_below_the_scan_cursor_still_completes_its_frame() {
        let mut space = PktRecvSpace::new();
        // 62 single-packet frames at 2..63, plus frame "hello" split across
        // 0 ("hel", fl 5) and 1 ("lo"), all arrive out of order.
        for s in 2..64 {
            assert!(space.recv(s, b"x".to_vec(), Some(1)));
        }
        assert!(space.recv(1, b"lo".to_vec(), None));
        // The front (seq 0) is a hole: no complete later frame may deliver,
        // however many are ready behind it.
        for _ in 2..64 {
            assert!(space.pop_complete_frame().is_none());
        }
        // The front hole fills: the frame at 0 reassembles and delivers, and
        // the 62 ready frames follow in order.
        assert!(space.recv(0, b"hel".to_vec(), Some(5)));
        assert_eq!(
            space.pop_complete_frame().as_deref(),
            Some(&b"hello"[..]),
            "the frame waiting on the late packet was never handed up"
        );
        for _ in 2..64 {
            assert!(space.pop_complete_frame().is_some());
        }
        space.collapse_tombstone_prefix();
        assert_eq!(
            space.next_seq(),
            Some(seq(64)),
            "the in-order cursor did not advance past the delivered frames"
        );
    }

    #[test]
    fn receive_and_cumulative_ack_advance_across_u64_wrap() {
        // A receive space seeded just before the wrap: in-order receipts
        // advance both the in-order cursor and the ACK cumulative front
        // across u64::MAX → 0 without raw-order comparisons.
        let mut space = PktRecvSpace::new_at(seq(u64::MAX - 1));
        assert_eq!(space.next_seq(), Some(seq(u64::MAX - 1)));
        assert_eq!(space.ack_history().next(), seq(u64::MAX - 1));
        assert!(space.recv(u64::MAX - 1, b"a".to_vec(), None));
        assert!(space.recv(u64::MAX, b"b".to_vec(), None));
        assert!(space.recv(0, b"c".to_vec(), None));
        assert_eq!(
            space.ack_history().next(),
            seq(1),
            "the cumulative ACK front must fold the in-order run across the wrap"
        );
        assert_eq!(
            space.next_seq(),
            Some(seq(u64::MAX - 1)),
            "the in-order cursor advances on pop"
        );
        assert_eq!(space.pop().unwrap(), b"a");
        assert_eq!(space.pop().unwrap(), b"b");
        assert_eq!(space.pop().unwrap(), b"c");
        assert_eq!(
            space.next_seq(),
            Some(seq(1)),
            "the in-order cursor advanced across the wrap"
        );
        // Stale (pre-wrap) duplicates are still ACKable as Duplicate.
        assert_eq!(
            space.recv_disposition(seq(u64::MAX), b"dup".to_vec(), None),
            RecvDisposition::Duplicate
        );
    }

    #[test]
    fn frame_reassembly_crosses_u64_wrap_in_logical_order() {
        let mut space = PktRecvSpace::new_at(seq(u64::MAX - 1));
        // A two-packet frame straddling u64::MAX → 0: continuation must
        // follow the wrapped sequence, not raw order.
        assert!(space.recv(u64::MAX - 1, b"A1".to_vec(), Some(4)));
        assert!(space.recv(u64::MAX, b"A2".to_vec(), None));
        assert_eq!(space.pop_complete_frame().unwrap(), b"A1A2");
        // A frame wrapping the boundary itself.
        assert!(space.recv(1, b"B2".to_vec(), None));
        assert!(space.recv(0, b"B1".to_vec(), Some(4)));
        assert_eq!(space.pop_complete_frame().unwrap(), b"B1B2");
        assert_eq!(space.next, Some(seq(2)));
        assert_eq!(space.slots.len(), 0);
    }

    fn withheld_pop_cost(outstanding: u64) -> f64 {
        let mut best = f64::MAX;
        for _ in 0..3 {
            let mut space = PktRecvSpace::new();
            for s in 1..=outstanding {
                assert!(space.recv(s, b"x".to_vec(), Some(1)));
            }
            // The first pop walks the region once (nothing is at the front —
            // seq 0 is a hole) and caches the scan resume behind the
            // withheld frames.
            assert!(space.pop_complete_frame().is_none());
            let start = std::time::Instant::now();
            for _ in 0..outstanding {
                // Each subsequent pop resumes one frame past the cached
                // frontier and withholds it the same way: the per-pop cost
                // must not grow with the number of frames behind the hole.
                assert!(space.pop_complete_frame().is_none());
            }
            best = best.min(start.elapsed().as_secs_f64() / outstanding as f64 * 1e9);
        }
        best
    }

    fn in_order_pop_cost(outstanding: u64) -> f64 {
        let mut best = f64::MAX;
        for _ in 0..3 {
            let mut space = PktRecvSpace::new();
            for s in 0..outstanding {
                assert!(space.recv(s, b"x".to_vec(), None));
            }
            let start = std::time::Instant::now();
            for _ in 0..outstanding {
                assert!(space.pop().is_some());
            }
            best = best.min(start.elapsed().as_secs_f64() / outstanding as f64 * 1e9);
        }
        best
    }

    #[test]
    #[ignore = "perf lane: wall-clock ns/pop ratio; run with cargo test --release -- --ignored"]
    fn withholding_frames_behind_a_hole_costs_no_more_per_pop() {
        let few = withheld_pop_cost(64);
        let many = withheld_pop_cost(4096);
        eprintln!(
            "frames withheld behind a front hole: {few:.1} ns/pop at 64 outstanding, {many:.1} ns/pop at 4096"
        );
        assert!(
            many < few * 8.0,
            "{many:.1} ns/pop at 4096 outstanding against {few:.1} ns at 64: the per-pop cost grows with the number of frames withheld behind the hole"
        );
    }

    #[test]
    #[ignore = "perf lane: wall-clock ns/packet ratio; run with cargo test --release -- --ignored"]
    fn advancing_the_receive_window_costs_no_more_per_packet() {
        let few = in_order_pop_cost(64);
        let many = in_order_pop_cost(4096);
        eprintln!(
            "in-order receive-window advance: {few:.1} ns/packet at 64 outstanding, {many:.1} ns/packet at 4096"
        );
        assert!(
            many < few * 8.0,
            "{many:.1} ns/packet at 4096 outstanding against {few:.1} ns at 64: advancing the receive window scans the outstanding map"
        );
    }
}
