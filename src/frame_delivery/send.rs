//! Sender-side frame staging and frame-aligned packetization.
//!
//! In frame-delivery mode application data is staged as whole frames (one
//! `send_frame` call = one wire frame) instead of the stock byte-stream
//! staging buffer.  The stage packetizes frame-aligned: a packet never
//! carries bytes of two frames, and the first packet of each frame is marked
//! with the total frame length so the receiver can reassemble it out of
//! order (see [`super::recv`]).

/// Maximum application bytes in a single frame in frame-delivery mode.  Set
/// equal to the stock `MAX_SEND_DATA_BUF_LEN` so a frame can occupy the whole
/// staging buffer; the per-packet payload cap (`max_data_size_per_pkt`) still
/// governs how many packets a frame is split into.
pub(crate) const MAX_FRAME_LEN: usize = 64 * 1024;

/// Validate a frame for staging: rejects empty frames and frames larger than
/// [`MAX_FRAME_LEN`] with `InvalidInput`.
pub(crate) fn validate_frame(frame: &[u8]) -> Result<(), std::io::ErrorKind> {
    if frame.is_empty() {
        return Err(std::io::ErrorKind::InvalidInput);
    }
    if frame.len() > MAX_FRAME_LEN {
        return Err(std::io::ErrorKind::InvalidInput);
    }
    Ok(())
}

/// A frame accepted by [`FrameSendStage::stage_frame`] but not yet fully
/// packetized.  `offset` is the number of bytes already sent in earlier
/// packets.
#[derive(Debug, Clone)]
struct PendingFrame {
    data: Vec<u8>,
    offset: usize,
}

/// The next frame-aligned packet payload to emit, as chosen by
/// [`FrameSendStage::next_chunk_len`] and consumed by
/// [`FrameSendStage::pop_chunk`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct FrameChunk {
    /// Number of payload bytes of the head frame to put in this packet.
    pub(crate) take_bytes: usize,
}

/// Pending (not yet fully packetized) frames in frame-delivery mode.
///
/// Each entry is a frame whose bytes have been accepted by
/// [`Self::stage_frame`] but not yet fully drained into the packet send
/// space.  Empty when frame-delivery mode is off (the stock byte-stream path
/// uses the reliable layer's staging buffer directly).
#[derive(Debug, Default)]
pub(crate) struct FrameSendStage {
    pending_frames: Vec<PendingFrame>,
}

impl FrameSendStage {
    pub(crate) fn new() -> Self {
        Self {
            pending_frames: Vec::new(),
        }
    }

    /// Whether no frame bytes are staged.
    pub(crate) fn is_empty(&self) -> bool {
        self.pending_frames.is_empty()
    }

    /// Total bytes of pending (not yet fully packetized) frames.
    pub(crate) fn pending_bytes(&self) -> usize {
        self.pending_frames
            .iter()
            .map(|pf| pf.data.len() - pf.offset)
            .sum()
    }

    /// Stage a whole application frame.  The frame is queued and packetized
    /// frame-aligned by subsequent [`Self::pop_chunk`] calls (a packet never
    /// carries bytes of two frames).  `soft_cap` is the staging-cap in bytes
    /// (rate-scaled by the caller, clamped to [`MAX_FRAME_LEN`]).
    ///
    /// Returns `Ok(())` on success, `Err(InvalidInput)` when the frame is
    /// empty or exceeds [`MAX_FRAME_LEN`], or `Err(WouldBlock)` when the
    /// stage is full.
    pub(crate) fn stage_frame(
        &mut self,
        frame: &[u8],
        soft_cap: usize,
    ) -> Result<(), std::io::ErrorKind> {
        validate_frame(frame)?;
        // Respect the same staging-cap discipline as the stock path so a small
        // interactive frame is not starved behind a bulk backlog already on
        // the stage.  Pending bytes (bytes of in-progress frames still to be
        // packetized) count against the cap.
        //
        // **Empty-stage bypass:** when the stage is empty (no pending frames),
        // admit any legal frame (up to MAX_FRAME_LEN) regardless of the soft
        // cap.  Without this, a frame larger than the ~2-packet initial stage
        // cap is rejected on an empty stage and waits forever for a send
        // notification that never comes (the send path only notifies after
        // draining, which can't start because the frame was never admitted).
        let pending_bytes = self.pending_bytes();
        if pending_bytes == 0 {
            // Empty stage: admit any legal frame.
            self.pending_frames.push(PendingFrame {
                data: frame.to_vec(),
                offset: 0,
            });
            return Ok(());
        }
        let free_bytes = soft_cap.saturating_sub(pending_bytes);
        if free_bytes < frame.len() {
            return Err(std::io::ErrorKind::WouldBlock);
        }
        self.pending_frames.push(PendingFrame {
            data: frame.to_vec(),
            offset: 0,
        });
        Ok(())
    }

    /// Choose the next frame-aligned packet payload without consuming it.
    ///
    /// The first packet of a frame is capped at `first_pkt_max_payload`
    /// (which must leave room for the `frame_len` header — see
    /// [`super::wire::frame_data_overhead`]); continuation packets are
    /// capped at `normal_max_payload`.  Returns `None` when no frame is
    /// staged.
    pub(crate) fn next_chunk_len(
        &self,
        first_pkt_max_payload: usize,
        normal_max_payload: usize,
    ) -> Option<FrameChunk> {
        let pf = self.pending_frames.first()?;
        let remaining = pf.data.len() - pf.offset;
        let cap = if pf.offset == 0 {
            first_pkt_max_payload
        } else {
            normal_max_payload
        };
        Some(FrameChunk {
            take_bytes: remaining.min(cap),
        })
    }

    /// Consume the chunk chosen by [`Self::next_chunk_len`]: append its bytes
    /// to `out`, advance the head frame's offset, and drop the frame once it
    /// is fully packetized.
    ///
    /// Returns `Some(frame_len)` iff this chunk is the *first* packet of its
    /// frame (the caller marks it with the `FRAME_DATA_TS` wire command);
    /// `None` for continuation packets.
    pub(crate) fn pop_chunk(&mut self, chunk: FrameChunk, out: &mut Vec<u8>) -> Option<u32> {
        let take_bytes = chunk.take_bytes;
        let pf = self.pending_frames.first_mut().unwrap();
        let frame_len = pf.data.len() as u32;
        out.extend_from_slice(&pf.data[pf.offset..pf.offset + take_bytes]);
        pf.offset += take_bytes;
        let frame_done = pf.offset == pf.data.len();
        let is_first = pf.offset == take_bytes;
        let frame_len = if is_first { Some(frame_len) } else { None };
        if frame_done {
            self.pending_frames.remove(0);
        }
        frame_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_stage_admits_any_legal_frame_past_soft_cap() {
        let mut stage = FrameSendStage::new();
        let frame = vec![0u8; 10_000];
        // Soft cap far below the frame size: the empty-stage bypass admits it.
        stage.stage_frame(&frame, 100).unwrap();
        assert_eq!(stage.pending_bytes(), 10_000);
    }

    #[test]
    fn non_empty_stage_respects_soft_cap() {
        let mut stage = FrameSendStage::new();
        stage.stage_frame(&[1u8; 100], 1_000).unwrap();
        let err = stage.stage_frame(&[2u8; 1_000], 1_000).unwrap_err();
        assert_eq!(err, std::io::ErrorKind::WouldBlock);
        stage.stage_frame(&[3u8; 900], 1_000).unwrap();
    }

    #[test]
    fn rejects_empty_and_oversize_frames() {
        let mut stage = FrameSendStage::new();
        assert_eq!(
            stage.stage_frame(&[], 1_000).unwrap_err(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(
            stage
                .stage_frame(&vec![0u8; MAX_FRAME_LEN + 1], 1_000)
                .unwrap_err(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn packetization_is_frame_aligned_and_marks_first_packet() {
        let mut stage = FrameSendStage::new();
        stage.stage_frame(&[1u8; 10], usize::MAX).unwrap();
        stage.stage_frame(&[2u8; 3], usize::MAX).unwrap();

        // First packet of frame 1: capped at first_pkt_max_payload (4).
        let chunk = stage.next_chunk_len(4, 6).unwrap();
        assert_eq!(chunk.take_bytes, 4);
        let mut out = Vec::new();
        assert_eq!(stage.pop_chunk(chunk, &mut out), Some(10));
        assert_eq!(out, [1u8; 4]);

        // Continuation packets: capped at normal_max_payload (6), no marker.
        let chunk = stage.next_chunk_len(4, 6).unwrap();
        assert_eq!(chunk.take_bytes, 6);
        let mut out = Vec::new();
        assert_eq!(stage.pop_chunk(chunk, &mut out), None);
        assert_eq!(out, [1u8; 6]);

        // Frame 2 starts fresh: first-packet cap and marker again.  A packet
        // never carries bytes of two frames.
        let chunk = stage.next_chunk_len(4, 6).unwrap();
        assert_eq!(chunk.take_bytes, 3);
        let mut out = Vec::new();
        assert_eq!(stage.pop_chunk(chunk, &mut out), Some(3));
        assert_eq!(out, [2u8; 3]);

        assert!(stage.is_empty());
        assert!(stage.next_chunk_len(4, 6).is_none());
    }
}
