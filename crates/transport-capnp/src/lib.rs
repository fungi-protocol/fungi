//! Cap'n Proto RPC channels for the Fungi transport contract.
//!
//! A dedicated executor keeps thread-local RPC capabilities behind `Send` handles.
//! Handles retain the connection; dropping the last one schedules cleanup.
//! Peer authentication and delivery confirmation belong to the backend protocol.

mod channel;
mod client;
mod error;
mod protocol;
mod server;

pub use channel::{CapnpChannel, CapnpDuplex, CapnpPeer, CapnpRecvHalf, CapnpSendHalf};
pub use error::{RecvError, SendError};
pub use server::serve;

mod channel_capnp {
    #![allow(dead_code, clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/channel_capnp.rs"));
}

#[cfg(test)]
mod tests;
