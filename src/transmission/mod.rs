pub(crate) mod coordination;
pub(crate) mod fec;
pub mod fec_tuning;
pub mod frame_delivery {
    //! Path-compatibility shim: the frame-delivery implementation lives in
    //! [`crate::delivery::frame`].
    pub use crate::delivery::frame::{FrameDelivery, frame_delivery_from_env};
}
pub(crate) mod read_half;
pub(crate) mod shared;
pub(crate) mod termination;
pub mod transmission_layer;
#[cfg(test)]
mod transmission_layer_test_facade;
mod ts_echo;
pub mod watchdog_tuning;
pub(crate) mod write_half;
