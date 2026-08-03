use core::num::NonZeroUsize;

use fec::proto::{data_mss, symbol_size};

#[cfg(test)]
use super::NO_FEC_MSS;
use super::MAX_MSS;
use crate::delivery::frame::FrameDelivery;
use crate::transmission::{
    fec::{FecConfig, FecState},
    fec_tuning::{FecTuning, fec_tuning_from_env},
    transmission_layer::{UnreliableLayer, UnreliableRead, UnreliableWrite},
};

#[cfg(test)]
pub(crate) fn wrap_fec(
    read: impl UnreliableRead,
    write: impl UnreliableWrite,
    fec: bool,
) -> UnreliableLayer {
    wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        read,
        write,
        fec,
        NO_FEC_MSS,
        FecTuning::default(),
        FrameDelivery::default(),
    )
}

// Test-only construction paths; keep `allow` because `expect` would go
// unfulfilled in `--all-targets`/test builds where these are used.
#[allow(dead_code)]
pub(crate) fn wrap_fec_with_mss(
    read: impl UnreliableRead,
    write: impl UnreliableWrite,
    fec: bool,
    mss: usize,
) -> UnreliableLayer {
    wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        read,
        write,
        fec,
        mss,
        fec_tuning_from_env(),
        FrameDelivery::default(),
    )
}

// Test-only construction path; keep `allow` because `expect` would go
// unfulfilled in `--all-targets`/test builds where this is used.
#[allow(dead_code)]
pub(crate) fn wrap_fec_with_mss_and_fec_tuning(
    read: impl UnreliableRead,
    write: impl UnreliableWrite,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
) -> UnreliableLayer {
    wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        read,
        write,
        fec,
        mss,
        tuning,
        FrameDelivery::default(),
    )
}
pub(crate) fn wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
    read: impl UnreliableRead,
    write: impl UnreliableWrite,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    frame_delivery: FrameDelivery,
) -> UnreliableLayer {
    let (mss, fec_state, tuning) = checked_mss_and_fec(fec, mss, tuning, frame_delivery);
    UnreliableLayer {
        utp_read: Box::new(read),
        utp_write: Box::new(write),
        post_open_handshake: None,
        mss,
        fec: fec_state,
        fec_tuning: tuning,
        frame_delivery,
    }
}

pub(crate) fn checked_mss_and_fec(
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    frame_delivery: FrameDelivery,
) -> (NonZeroUsize, Option<FecState>, FecTuning) {
    assert!(
        mss <= MAX_MSS,
        "mss {mss} exceeds the {MAX_MSS}-byte datagram ceiling"
    );
    let fec_state = if fec {
        let symbol_size = symbol_size(mss).expect("mss too small for the FEC header");
        Some(FecState::new(FecConfig {
            symbol_size,
            interactive_parity_depth: tuning.interactive_parity_depth,
        }))
    } else {
        None
    };
    let mss = if fec {
        data_mss(mss).expect("mss too small for the FEC header")
    } else {
        mss
    };
    assert!(
        crate::codec::data_overhead() < mss,
        "mss {mss} leaves no room for the codec payload"
    );
    // In frame-delivery mode, the first packet of each frame carries a
    // 4-byte frame-length header (FRAME_DATA_TS), so the MSS must leave
    // room for `frame_data_overhead()` (data_overhead + 4), not just
    // `data_overhead()`.  A too-small MSS would yield 0-byte-payload first
    // packets.
    if frame_delivery.enabled {
        assert!(
            crate::delivery::frame::wire::frame_data_overhead() < mss,
            "mss {mss} leaves no room for the first-frame header"
        );
    }
    // FEC off → depth is irrelevant; normalise to the default so the field is
    // inert. When FEC is on, clamp to 1 so a misconfigured 0 cannot disable
    // parity entirely (the stock path always emits at least 1).
    let tuning = if fec_state.is_none() {
        FecTuning::default()
    } else {
        FecTuning {
            interactive_parity_depth: tuning.interactive_parity_depth.max(1),
            ..tuning
        }
    };
    (NonZeroUsize::new(mss).unwrap(), fec_state, tuning)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Table of invalid configurations: each row becomes its own
    // `#[should_panic]` test so every panic condition of
    // `checked_mss_and_fec` is exercised instead of the first one aborting a
    // shared loop.
    macro_rules! checked_mss_and_fec_panic_case {
        ($name:ident, $expected:literal, $fec:expr, $mss:expr, $frame_delivery:expr) => {
            #[test]
            #[should_panic(expected = $expected)]
            fn $name() {
                let _ = checked_mss_and_fec(
                    $fec,
                    $mss,
                    FecTuning::default(),
                    $frame_delivery,
                );
            }
        };
    }

    checked_mss_and_fec_panic_case!(
        mss_over_the_datagram_ceiling_panics,
        "datagram ceiling",
        false,
        MAX_MSS + 1,
        FrameDelivery::default()
    );
    checked_mss_and_fec_panic_case!(
        fec_mss_smaller_than_the_fec_header_panics,
        "mss too small for the FEC header",
        true,
        fec::proto::HDR_SIZE - 1,
        FrameDelivery::default()
    );
    checked_mss_and_fec_panic_case!(
        fec_mss_smaller_than_the_data_symbol_header_panics,
        "mss too small for the FEC header",
        true,
        fec::proto::HDR_SIZE,
        FrameDelivery::default()
    );
    checked_mss_and_fec_panic_case!(
        mss_with_no_room_for_the_codec_payload_panics,
        "leaves no room for the codec payload",
        false,
        crate::codec::data_overhead(),
        FrameDelivery::default()
    );
    checked_mss_and_fec_panic_case!(
        frame_delivery_mss_with_no_room_for_the_first_frame_header_panics,
        "first-frame header",
        false,
        crate::codec::data_overhead() + 1,
        FrameDelivery::enabled()
    );

    #[test]
    fn mss_1400_is_accepted_with_fec_on_and_off() {
        let (mss, fec_state, tuning) = checked_mss_and_fec(
            false,
            1_400,
            FecTuning::default(),
            FrameDelivery::default(),
        );
        assert_eq!(mss.get(), 1_400, "FEC off must keep the raw MSS");
        assert!(fec_state.is_none(), "FEC off must not build FEC state");
        assert_eq!(
            tuning,
            FecTuning::default(),
            "FEC off must normalise tuning to the default"
        );

        let (mss, fec_state, tuning) = checked_mss_and_fec(
            true,
            1_400,
            FecTuning::default(),
            FrameDelivery::default(),
        );
        assert_eq!(
            mss.get(),
            1_400 - fec::proto::HDR_SIZE - fec::proto::DATA_SYMBOL_HDR_SIZE,
            "FEC on must shrink the MSS by the symbol + data-symbol headers"
        );
        assert!(fec_state.is_some(), "FEC on must build FEC state");
        assert!(
            tuning.interactive_parity_depth >= 1,
            "FEC on must clamp the parity depth to at least 1"
        );
    }
}
