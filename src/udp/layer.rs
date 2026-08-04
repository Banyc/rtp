use core::num::NonZeroUsize;

use fec::proto::{data_mss, symbol_size};
use thiserror::Error;

use super::MAX_MSS;
#[cfg(test)]
use super::NO_FEC_MSS;
use crate::delivery::frame::FrameMode;
use crate::transmission::{
    fec::{FecConfig, FecState},
    fec_tuning::FecTuning,
    transmission_layer::{UnreliableLayer, UnreliableRead, UnreliableWrite},
};

/// A maximum segment size that has been validated against the datagram
/// ceiling and the codec payload overhead.  Construction is fallible; every
/// downstream layer builder takes a [`ValidMss`] and therefore cannot panic
/// on the MSS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidMss(usize);

impl ValidMss {
    pub fn try_new(mss: usize) -> Result<Self, MssError> {
        if mss > MAX_MSS {
            return Err(MssError::ExceedsDatagramCeiling { mss, max: MAX_MSS });
        }
        if crate::codec::data_overhead() >= mss {
            return Err(MssError::NoRoomForCodecPayload { mss });
        }
        Ok(Self(mss))
    }

    pub fn get(&self) -> usize {
        self.0
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
    #[error("mss {mss} leaves no room for the first-frame header")]
    NoRoomForFirstFrameHeader { mss: usize },
}

impl From<MssError> for std::io::Error {
    fn from(error: MssError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
    }
}

#[cfg(test)]
pub(crate) fn wrap_fec(
    read: Box<dyn UnreliableRead>,
    write: Box<dyn UnreliableWrite>,
    fec: bool,
) -> UnreliableLayer {
    wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        read,
        write,
        fec,
        ValidMss::try_new(NO_FEC_MSS).unwrap(),
        FecTuning::default(),
        FrameMode::default(),
    )
    .unwrap()
}

pub(crate) fn wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
    read: Box<dyn UnreliableRead>,
    write: Box<dyn UnreliableWrite>,
    fec: bool,
    mss: ValidMss,
    tuning: FecTuning,
    frame_delivery: FrameMode,
) -> Result<UnreliableLayer, MssError> {
    let (mss, fec_state, tuning) = checked_mss_and_fec(fec, mss, tuning, frame_delivery)?;
    Ok(UnreliableLayer {
        utp_read: read,
        utp_write: write,
        post_open_handshake: None,
        mss,
        fec: fec_state,
        fec_tuning: tuning,
        frame_delivery,
        rtx_dup: false,
        instream_group_fec: false,
    })
}

pub(crate) fn checked_mss_and_fec(
    fec: bool,
    mss: ValidMss,
    tuning: FecTuning,
    frame_delivery: FrameMode,
) -> Result<(NonZeroUsize, Option<FecState>, FecTuning), MssError> {
    let mss = mss.get();
    let fec_state = if fec {
        let symbol_size = symbol_size(mss).ok_or(MssError::TooSmallForFec { mss })?;
        Some(FecState::new(FecConfig {
            symbol_size,
            small_group_parity_count: tuning.small_group_parity_count,
        }))
    } else {
        None
    };
    let mss = if fec {
        data_mss(mss).ok_or(MssError::TooSmallForFec { mss })?
    } else {
        mss
    };
    if crate::codec::data_overhead() >= mss {
        return Err(MssError::NoRoomForCodecPayload { mss });
    }
    // In frame-delivery mode, the first packet of each frame carries a
    // 4-byte frame-length header (FRAME_DATA_TS), so the MSS must leave
    // room for `frame_data_overhead()` (data_overhead + 4), not just
    // `data_overhead()`.  A too-small MSS would yield 0-byte-payload first
    // packets.
    if frame_delivery.enabled && crate::delivery::frame::wire::frame_data_overhead() >= mss {
        return Err(MssError::NoRoomForFirstFrameHeader { mss });
    }
    // FEC off → depth is irrelevant; normalise to the default so the field is
    // inert. When FEC is on, clamp to 1 so a misconfigured 0 cannot disable
    // parity entirely (the stock path always emits at least 1).
    let tuning = if fec_state.is_none() {
        FecTuning::default()
    } else {
        FecTuning {
            small_group_parity_count: tuning.small_group_parity_count.max(1),
            ..tuning
        }
    };
    Ok((NonZeroUsize::new(mss).unwrap(), fec_state, tuning))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Table of invalid configurations: each row becomes its own test so every
    // failure mode of the fallible mss validation is exercised instead of the
    // first one aborting a shared loop.
    macro_rules! checked_mss_and_fec_error_case {
        ($name:ident, $expected:pat, $fec:expr, $mss:expr, $frame_delivery:expr) => {
            #[test]
            fn $name() {
                let mss = ValidMss::try_new($mss);
                let res = match mss {
                    Ok(mss) => {
                        checked_mss_and_fec($fec, mss, FecTuning::default(), $frame_delivery)
                    }
                    Err(error) => Err(error),
                };
                assert!(
                    matches!(res, Err($expected)),
                    "expected {}, got {res:?}",
                    stringify!($expected)
                );
            }
        };
    }

    checked_mss_and_fec_error_case!(
        mss_over_the_datagram_ceiling_fails,
        MssError::ExceedsDatagramCeiling { .. },
        false,
        MAX_MSS + 1,
        FrameMode::default()
    );
    checked_mss_and_fec_error_case!(
        mss_with_no_room_for_the_codec_payload_fails,
        MssError::NoRoomForCodecPayload { .. },
        false,
        crate::codec::data_overhead(),
        FrameMode::default()
    );
    checked_mss_and_fec_error_case!(
        fec_shrink_leaves_no_codec_room_fails,
        MssError::NoRoomForCodecPayload { .. },
        true,
        crate::codec::data_overhead() + 1,
        FrameMode::default()
    );
    checked_mss_and_fec_error_case!(
        frame_delivery_mss_with_no_room_for_the_first_frame_header_fails,
        MssError::NoRoomForFirstFrameHeader { .. },
        false,
        crate::codec::data_overhead() + 1,
        FrameMode::enabled()
    );

    #[test]
    fn mss_1400_is_accepted_with_fec_on_and_off() {
        let (mss, fec_state, tuning) = checked_mss_and_fec(
            false,
            ValidMss::try_new(1_400).unwrap(),
            FecTuning::default(),
            FrameMode::default(),
        )
        .unwrap();
        assert_eq!(mss.get(), 1_400, "FEC off must keep the raw MSS");
        assert!(fec_state.is_none(), "FEC off must not build FEC state");
        assert_eq!(
            tuning,
            FecTuning::default(),
            "FEC off must normalise tuning to the default"
        );

        let (mss, fec_state, tuning) = checked_mss_and_fec(
            true,
            ValidMss::try_new(1_400).unwrap(),
            FecTuning::default(),
            FrameMode::default(),
        )
        .unwrap();
        assert_eq!(
            mss.get(),
            1_400 - fec::proto::HDR_SIZE - fec::proto::DATA_SYMBOL_HDR_SIZE,
            "FEC on must shrink the MSS by the symbol + data-symbol headers"
        );
        assert!(fec_state.is_some(), "FEC on must build FEC state");
        assert!(
            tuning.small_group_parity_count >= 1,
            "FEC on must clamp the parity depth to at least 1"
        );
    }
}
