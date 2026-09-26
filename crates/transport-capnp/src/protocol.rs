pub(super) use crate::channel_capnp::{
    build_failure, builder, channel, recv_failure, result as rpc_result, send_failure,
};

pub(super) type RemoteChannel =
    channel::Client<capnp::data::Owned, send_failure::Owned, recv_failure::Owned>;
pub(super) type ChannelSchema =
    channel::Owned<capnp::data::Owned, send_failure::Owned, recv_failure::Owned>;
pub(super) type RemoteBuilder =
    builder::Client<capnp::data::Owned, ChannelSchema, build_failure::Owned>;
