pub(super) use crate::channel_capnp::{channel, recv_failure, result as rpc_result, send_failure};

pub(super) type RemoteChannel =
    channel::Client<capnp::data::Owned, send_failure::Owned, recv_failure::Owned>;
