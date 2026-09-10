//! The maximum segment size (MSS): the largest datagram payload the
//! transport produces. A newtype so the value cannot be confused with an
//! arbitrary size; construction is fallible (validated against the
//! datagram ceiling and the codec overhead).

use thiserror::Error;

use crate::codec;

/// The largest MSS the transport accepts: the datagram ceiling. The
/// truncated-datagram detection headroom is deducted by the calculation
/// methods (see [`Mss::max_datagram_size`] and
/// [`Mss::max_data_size_per_pkt`]), so a full-size datagram is never
/// exactly the receive buffer size.
pub const MAX_MSS: usize = 64 * 1024;

/// The headroom the MSS must leave below the datagram ceiling so a
/// full-size datagram is never exactly the receive buffer size: the
/// receiver drops datagrams of exactly the buffer size (they may be
/// truncated), so the largest datagram the transport produces must be at
/// most [`MAX_MSS`] minus this headroom. Every size derived from the MSS
/// deducts this headroom.
pub const TRUNCATION_DETECTION_BYTES: usize = 1;

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

    /// The largest datagram the transport may produce: the MSS minus the
    /// truncated-datagram detection headroom, so a full-size datagram is
    /// never exactly the receive buffer size. Every producer (data, FEC
    /// parity, handshake) bounds its wire datagram by this.
    pub const fn max_datagram_size(&self) -> usize {
        self.0 - TRUNCATION_DETECTION_BYTES
    }

    /// The maximum payload bytes per data packet: `mss - data_overhead()`,
    /// minus the truncated-datagram detection headroom so the wire datagram
    /// is always at least one byte below the MSS (and therefore never
    /// exactly the receive buffer size). Guaranteed non-negative by
    /// [`Mss::try_new`], which rejects an MSS with no room for the codec
    /// payload.
    pub fn max_data_size_per_pkt(&self) -> usize {
        self.0 - TRUNCATION_DETECTION_BYTES - codec::data_overhead()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceiling_and_headroom_keep_the_max_datagram_below_the_buffer() {
        // The receive path reads into a 64 KiB buffer and drops datagrams
        // of exactly that size (they may be a truncated larger datagram), so
        // the largest datagram the transport produces must be one byte below
        // the ceiling.
        assert_eq!(MAX_MSS, 64 * 1024);
        assert_eq!(TRUNCATION_DETECTION_BYTES, 1);
        assert!(
            Mss::try_new(MAX_MSS).is_ok(),
            "the ceiling MSS must be valid"
        );
        assert!(matches!(
            Mss::try_new(MAX_MSS + 1),
            Err(MssError::ExceedsDatagramCeiling { .. })
        ));
        assert_eq!(
            Mss::try_new(MAX_MSS).unwrap().max_datagram_size(),
            MAX_MSS - TRUNCATION_DETECTION_BYTES,
            "the max datagram must be one byte below the ceiling"
        );
    }

    #[test]
    fn max_data_size_per_pkt_deducts_the_detection_headroom() {
        // The wire datagram is `data_overhead + payload`; the headroom keeps
        // it at least one byte below the MSS (and therefore never exactly
        // the receive buffer size).
        let mss = Mss::try_new(1_424).unwrap();
        assert_eq!(
            mss.max_data_size_per_pkt(),
            1_424 - TRUNCATION_DETECTION_BYTES - codec::data_overhead()
        );
        assert_eq!(
            mss.max_data_size_per_pkt() + codec::data_overhead(),
            1_424 - TRUNCATION_DETECTION_BYTES,
            "the codec packet must stay one byte below the MSS"
        );
    }
}
