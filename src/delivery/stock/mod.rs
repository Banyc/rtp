//! Stock byte-stream delivery mode — the default.
//!
//! In stock (non-frame) mode, application data flows through a single
//! byte-stream staging buffer on the send side and a single byte-stream
//! receive buffer on the recv side.  Delivery is strict in-order with
//! head-of-line blocking on packet loss.
//!
//! - [`send`] — send-side byte-stream staging buffer with rate-scaled
//!   admission.
//! - [`recv`] — recv-side byte-stream buffer that drains from the
//!   packet receive space.

pub(crate) mod recv;
pub(crate) mod send;
