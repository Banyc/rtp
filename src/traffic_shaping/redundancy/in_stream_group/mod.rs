//! Interactive in-stream group FEC: the force-flush path that emits a group's
//! parities while the burst is still in flight, and the capacity gate it
//! consults.

mod flush;
mod gate;
pub(crate) mod parity;

pub(crate) use flush::InStreamGroupFlush;
pub(crate) use gate::CapacityGate;
