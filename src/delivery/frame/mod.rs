//! Per-connection frame-delivery mode (`FrameMode`).
//!
//! Stock rtp exposes a strict in-order byte stream.  When
//! `FrameMode { enabled: true }` is passed to both peers, the
//! connection switches to a *frame-aware* mode:
//!
//! - The sender marks application frame boundaries on the wire (first
//!   packet of a frame carries a `FRAME_DATA_TS` codec command with the
//!   total frame length; continuation packets use `DATA_TS`).
//! - ARQ / SACK / congestion control are untouched.
//! - The receiver reassembles frames and, by default, delivers them in order,
//!   gap-free, at the in-order cursor: a complete frame past an unrepaired
//!   sequence hole is withheld until the hole fills (retransmission or
//!   reordering), preserving the strict ordering guarantee of stock rtp. A
//!   frame whose next continuation is permanently captured (a hostile peer's
//!   overlapping frame) is abandoned so the cursor never wedges on it.
//! - Opt-in ([`FrameMode::allow_reorder`]) receiver-side fast-forward: a
//!   complete frame that starts past an unrepaired hole may be delivered
//!   immediately while the in-order cursor (`next`) stays pinned at the hole.
//!   The frame's sequence numbers are tombstoned exactly as in the strict
//!   path, so when the hole finally fills the cursor collapses the tombstones
//!   and advances without redelivering. The flag is OFF by default; enabling
//!   it only changes which complete frames are handed up, never the ACKs sent
//!   or the wire format, so liveness is unaffected.
//!
//! # Both peers must enable together
//!
//! There is no in-band negotiation — same coupling as the FEC flag.  Both
//! peers must pass `FrameMode { enabled: true }` to the matching
//! `*_with_mss_fec_tuning_and_frame_delivery` constructor; a mismatch
//! produces a framing desync.

pub(crate) mod capture;
pub(crate) mod mode;
pub(crate) mod recv;
pub(crate) mod send;
pub(crate) mod wire;
