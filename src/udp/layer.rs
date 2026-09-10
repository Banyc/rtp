use fec::proto::{data_mss, symbol_size};

#[cfg(test)]
use crate::mss::MAX_MSS;

#[cfg(test)]
use super::NO_FEC_MSS;
use crate::delivery::frame::FrameMode;
use crate::mss::{Mss, MssError};
use crate::obfuscate::padding::AckPaddingMode;
use crate::traffic_shaping::redundancy::{
    RetransmissionArmorConfig,
    fec::{FecConfig, FecState},
    fec_tuning::FecTuning,
};
use crate::transmission::transmission_layer::{UnreliableLayer, UnreliableRead, UnreliableWrite};

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
        Mss::try_new(NO_FEC_MSS).unwrap(),
        FecTuning::default(),
        FrameMode::default(),
    )
    .unwrap()
}

pub(crate) fn wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
    read: Box<dyn UnreliableRead>,
    write: Box<dyn UnreliableWrite>,
    fec: bool,
    mss: Mss,
    tuning: FecTuning,
    frame_delivery: FrameMode,
) -> Result<UnreliableLayer, MssError> {
    let (mss, fec_state, tuning) = checked_mss_and_fec(fec, mss, tuning, frame_delivery)?;
    Ok(UnreliableLayer {
        utp_read: read,
        utp_write: write,
        post_open_handshake: None,
        session_tag: None,
        initial_sequences: crate::sequence::InitialSequences::ZERO,
        initial_rtt: None,
        metrics_observer: None,
        mss,
        fec: fec_state,
        fec_tuning: tuning,
        frame_delivery,
        retransmission_armor: RetransmissionArmorConfig::disabled(),
        instream_group_fec: false,
        ack_padding: AckPaddingMode::None,
    })
}

pub(crate) fn checked_mss_and_fec(
    fec: bool,
    mss: Mss,
    tuning: FecTuning,
    frame_delivery: FrameMode,
) -> Result<(Mss, Option<FecState>, FecTuning), MssError> {
    let mss = mss.get();
    let fec_state = if fec {
        // The parity symbol is `symbol_size` bytes and the parity wire adds
        // the 11-byte header, so the symbol size must leave the
        // truncated-datagram detection headroom: the largest parity
        // datagram is `symbol_size + 11 = mss - 1`, never exactly the
        // receive buffer size.
        let symbol_size = symbol_size(mss - crate::mss::TRUNCATION_DETECTION_BYTES)
            .ok_or(MssError::TooSmallForFec { mss })?;
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
    Ok((Mss::try_new(mss).unwrap(), fec_state, tuning))
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
                let mss = Mss::try_new($mss);
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
            Mss::try_new(1_400).unwrap(),
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
            Mss::try_new(1_400).unwrap(),
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

    // ---- OBFUSCATION NONCE ACCOUNTING ----
    //
    // The datagram-obfuscation wrapper prefixes every datagram with a
    // 24-byte nonce, so the MSS — which bounds the WIRE datagram size — must
    // leave room for it: with obfuscation enabled the effective segment
    // payload is `mss - NONCE_LEN` and the wire datagram still fits in the
    // configured MSS.

    #[test]
    fn reduced_for_obfuscation_subtracts_the_nonce() {
        let mss = Mss::try_new(crate::udp::NO_FEC_MSS)
            .unwrap()
            .reduced_for_obfuscation()
            .unwrap();
        assert_eq!(
            mss.get(),
            crate::udp::NO_FEC_MSS - crate::obfuscate::NONCE_LEN,
            "the obfuscation nonce must be reserved from the MSS"
        );
        // The wire datagram (segment payload + codec overhead + nonce) still
        // fits in the configured MSS: the payload is `mss - data_overhead`
        // minus the truncated-datagram detection headroom, so the wire is
        // `(mss - 1 - data_overhead) + data_overhead + nonce` = `mss - 1 +
        // nonce` = the configured MSS minus the headroom.
        assert_eq!(
            mss.get() + crate::obfuscate::NONCE_LEN,
            crate::udp::NO_FEC_MSS,
            "the wire datagram must stay within the configured MSS"
        );
    }

    #[test]
    fn reduced_for_obfuscation_rejects_a_mss_too_small_for_the_nonce() {
        // A configured MSS that is valid on its own but cannot carry the
        // nonce on top of the codec payload must be rejected.
        let mss = Mss::try_new(crate::codec::data_overhead() + 1).unwrap();
        let err = mss.reduced_for_obfuscation().unwrap_err();
        assert!(
            matches!(err, MssError::NoRoomForObfuscationNonce { .. }),
            "a too-small MSS must fail with NoRoomForObfuscationNonce, got {err:?}"
        );
    }

    #[test]
    fn reduced_for_obfuscation_keeps_the_codec_room_check() {
        // mss - nonce must still leave room for the codec payload.
        let mss =
            Mss::try_new(crate::codec::data_overhead() + crate::obfuscate::NONCE_LEN).unwrap();
        let err = mss.reduced_for_obfuscation().unwrap_err();
        assert!(
            matches!(err, MssError::NoRoomForCodecPayload { .. }),
            "the reduced MSS must still leave room for the codec, got {err:?}"
        );
    }
}
