//! Per-connection delivery mode: frame-aware vs stock byte-stream.
//!
//! - [`frame`] — frame-delivery mode: application writes are whole frames,
//!   the receiver may deliver complete frames out of order past sequence
//!   holes.  See [`frame::FrameDelivery`].
//! - [`stock`] — byte-stream mode (the default): strict in-order delivery
//!   of a single byte stream, with head-of-line blocking on loss.

pub mod frame;
pub(crate) mod stock;
