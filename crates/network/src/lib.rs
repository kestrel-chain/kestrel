//! Peer discovery, transaction gossip, and `KestrelCast` block propagation.

mod kestrel_cast;
mod service;

pub use kestrel_cast::{
    KestrelCast, KestrelCastConfig, KestrelCastError, RelayCandidate, RelayPlan, Shred,
};
pub use service::{
    ConfiguredPeer, GossipConfig, GossipError, InboundShred, InboundTransaction, NetworkFaults,
    NetworkHandle, NetworkNode,
};
