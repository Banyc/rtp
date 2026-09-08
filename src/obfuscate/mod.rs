//! Datagram obfuscation: a transport wrapper that prefixes every datagram
//! with a 24-byte random nonce and chacha20-encrypts the rest with a given
//! key, so a passive observer sees only random bytes and cannot distinguish
//! the traffic from any other encrypted UDP protocol (QUIC, WireGuard, DTLS).
//!
//! The module is split into two parts:
//! - [`mask`]: the nonce + chacha20 keystream masking wrapper.
//! - [`padding`]: the obfuscated-plaintext padding format
//!   (`[len u16][payload][padding]` with a profile, `[payload]` without).
//!
//! The wrapper is wired into the public constructors through the optional
//! `obfuscation_key` on [`crate::udp::ConnectConfig`] /
//! [`crate::udp::AcceptConfig`]; the wrapper types themselves are internal.

pub(crate) mod mask;
pub(crate) mod padding;

pub(crate) use mask::{KEY_LEN, NONCE_LEN, ObfuscatedWrite, apply_keystream, maybe_wrap};
