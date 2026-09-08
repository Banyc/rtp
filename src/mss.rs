//! The maximum segment size (MSS): the largest datagram payload the
//! transport produces. A newtype so the value cannot be confused with an
//! arbitrary size; construction is fallible (validated against the
//! datagram ceiling and the codec overhead).

use thiserror::Error;

use crate::codec;

/// The largest MSS the transport accepts: the datagram ceiling.
pub const MAX_MSS: usize = 64 * 1024;

/// A maximum segment size, validated against the datagram ceiling and the
/// codec payload overhead. Construction is fallible; every downstream layer
/// builder takes a [`Mss`] and therefore cannot panic on the MSS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mss(usize);

impl Mss {
    pub fn try_new(mss: usize) -> Result<Self, MssError> {
        if mss > MAX_MSS {
            return Err(MssError::ExceedsDatagramCeiling { mss, max: MAX_MSS });
        }
        if codec::data_overhead() >= mss {
            return Err(MssError::NoRoomForCodecPayload { mss });
        }
        Ok(Self(mss))
    }

    /// Reduce the MSS by the datagram-obfuscation nonce length. The nonce is
    /// a wire-level overhead on EVERY datagram (like the codec and FEC
    /// headers), so the MSS — which bounds the wire datagram size — must
    /// leave room for it: with obfuscation enabled, the effective segment
    /// payload is `mss - NONCE_LEN` and the wire datagram still fits in the
    /// configured MSS. Fails when the configured MSS is too small to carry
    /// the nonce on top of the codec payload.
    pub fn reduced_for_obfuscation(self) -> Result<Self, MssError> {
        let nonce = crate::obfuscate::NONCE_LEN;
        let mss = self
            .0
            .checked_sub(nonce)
            .ok_or(MssError::NoRoomForObfuscationNonce { mss: self.0, nonce })?;
        Self::try_new(mss)
    }

    pub const fn get(&self) -> usize {
        self.0
    }

    /// The maximum payload bytes per data packet: `mss - data_overhead()`.
    /// Guaranteed non-negative by [`Mss::try_new`], which rejects an MSS
    /// with no room for the codec payload.
    pub fn max_data_size_per_pkt(&self) -> usize {
        self.0 - codec::data_overhead()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MssError {
    #[error("mss {mss} exceeds the {max}-byte datagram ceiling")]
    ExceedsDatagramCeiling { mss: usize, max: usize },
    #[error("mss {mss} is too small for the FEC header")]
    TooSmallForFec { mss: usize },
    #[error("mss {mss} leaves no room for the codec payload")]
    NoRoomForCodecPayload { mss: usize },
    #[error("mss {mss} leaves no room for the {nonce}-byte obfuscation nonce")]
    NoRoomForObfuscationNonce { mss: usize, nonce: usize },
    #[error("mss {mss} leaves no room for the {key_size}-byte dispatch key")]
    NoRoomForDispatchKey { mss: usize, key_size: usize },
    #[error("mss {mss} leaves no room for the first-frame header")]
    NoRoomForFirstFrameHeader { mss: usize },
}

impl From<MssError> for std::io::Error {
    fn from(error: MssError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
    }
}
