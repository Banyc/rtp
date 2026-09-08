//! The connection-opening protocol: the client/server opening handshake
//! ([`opening`]), its wire format ([`wire`]), the MSS-derived datagram
//! padding ([`padding`]), and the post-open recovery ([`post_open`]).

pub(crate) mod opening;
pub(crate) mod padding;
pub(crate) mod post_open;
pub(crate) mod wire;

pub(crate) use opening::{client_opening_handshake, server_opening_handshake};
pub use post_open::PostOpenHandshake;
pub(crate) use post_open::{DueResponse, PostOpenVerdict, is_post_open_candidate};
