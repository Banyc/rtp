#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

mod ack;
mod codec;
mod debug;
mod delivery;
pub mod io_err;
pub mod keyed_udp;
pub mod mpudp;
mod mss;
mod obfuscate;
mod recv_queue;
mod reliable;
mod sequence;
pub mod socket;
mod tag;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod traffic_shaping;
mod transmission;
pub mod udp;
pub use traffic_shaping::adjacent::metrics;
pub use traffic_shaping::control::probe;

pub use delivery::frame::{FrameMode, frame_delivery_from_env};
pub use io_err::IoErr;
pub use keyed_udp::{
    Accepted as KeyedAccepted, Connected as KeyedConnected, Connector as KeyedConnector,
    DispatchKey, Listener as KeyedListener,
};
pub use mpudp::{Conn as MpConn, Listener as MpListener, MPUDP_MSS};
pub use probe::{
    EchoDemux, ProbeEcho, decode_echo, decode_echo_obfuscated, encode_probe,
    encode_probe_obfuscated,
};
pub use socket::{
    AsyncReadAdapter, AsyncWriteAdapter, ConnReader, ConnWriter, FrameByteReader, FrameByteWriter,
    IoStream, SessionHandle, socket_with_watchdog_tuning, unsplit,
};
pub use traffic_shaping::redundancy::{FecTuning, fec_tuning_from_env};
pub use transmission::transmission_layer::{
    LogConfig as LayerLogConfig, UnreliableLayer, UnreliableRead, UnreliableWrite,
};
pub use transmission::watchdog_tuning::WatchdogTuning;
pub use udp::{
    AcceptConfig, AcceptTask, Accepted, ConnectConfig, Connected, FrameDeliveryAccept,
    FrameDeliveryIo, Listener, ListenerConfig, LogConfig, MAX_MSS, MssConfig, NO_FEC_MSS,
    connect_with, connect_with_socket,
};
