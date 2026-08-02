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
