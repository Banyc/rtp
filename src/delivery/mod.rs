//! Per-connection delivery mode: frame-aware vs stock byte-stream.
//!
//! - [`frame`] — frame-delivery mode: application writes are whole frames,
//!   the receiver may deliver complete frames out of order past sequence
//!   holes.  See [`frame::FrameMode`].
//! - [`byte_stream`] — byte-stream mode (the default): strict in-order delivery
//!   of a single byte stream, with head-of-line blocking on loss.

pub(crate) mod byte_stream;
pub mod frame;
