use crate::channel::{CapnpChannel, receive_message, send_message};
use crate::client::{
    Bootstrap, channel_actor, new_link, rpc_recv_error, rpc_send_error, start_client,
};
use crate::error::{RecvError, SendError};
use crate::protocol::{channel, recv_failure, send_failure};
use crate::server::serve;
use capnp::{capability::Promise, data};
use fungi_transport::{RecvChannel, SendChannel};
use fungi_transport_testkit::mem::{MemConfig, bidirectional};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};

fn mem_send(_: fungi_transport_testkit::mem::MemError) -> SendError {
    SendError::Closed
}
fn mem_recv(_: fungi_transport_testkit::mem::MemError) -> RecvError {
    RecvError::Closed
}

struct BlockedRpcChannel {
    started: mpsc::UnboundedSender<Vec<u8>>,
    gate: Arc<tokio::sync::Semaphore>,
}

impl channel::Server<data::Owned, send_failure::Owned, recv_failure::Owned> for BlockedRpcChannel {
    fn send(
        &mut self,
        params: channel::SendParams<data::Owned, send_failure::Owned, recv_failure::Owned>,
        mut results: channel::SendResults<data::Owned, send_failure::Owned, recv_failure::Owned>,
    ) -> Promise<(), capnp::Error> {
        self.started
            .send(params.get().unwrap().get_message().unwrap().to_vec())
            .unwrap();
        let gate = Arc::clone(&self.gate);
        Promise::from_future(async move {
            gate.acquire().await.unwrap().forget();
            results.get().init_result().init_ok();
            Ok(())
        })
    }

    fn recv(
        &mut self,
        _: channel::RecvParams<data::Owned, send_failure::Owned, recv_failure::Owned>,
        mut results: channel::RecvResults<data::Owned, send_failure::Owned, recv_failure::Owned>,
    ) -> Promise<(), capnp::Error> {
        results
            .get()
            .init_result()
            .set_ok(b"independent".as_slice())
            .unwrap();
        Promise::ok(())
    }
}

#[tokio::test]
async fn channel_actor_exits_when_all_command_handles_are_dropped() {
    let (started, _starts) = mpsc::unbounded_channel();
    let remote = capnp_rpc::new_client(BlockedRpcChannel {
        started,
        gate: Arc::new(Semaphore::new(0)),
    });
    let (commands, receiver) = mpsc::channel(1);
    drop(commands);
    tokio::time::timeout(Duration::from_secs(5), channel_actor(remote, receiver))
        .await
        .expect("actor must stop after its command queue closes");
}

#[tokio::test]
async fn runtime_initialization_errors_reach_the_connecting_caller() {
    let (io, _peer) = tokio::io::duplex(64);
    let (_link, lifetime) = new_link(1024);
    let (_commands, receiver) = mpsc::channel(2);
    let error = start_client(io, Bootstrap::Channel(receiver), lifetime, || {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "runtime setup failed",
        ))
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "runtime setup failed");
}

#[tokio::test]
async fn servers_return_protocol_errors_to_their_callers() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::LocalSet::new().run_until(async {
            let (backend, _peer) = bidirectional(MemConfig::default());
            let (mut peer, io) = tokio::io::duplex(64);
            peer.write_all(&[1, 2, 3, 4, 0, 0, 0, 0]).await.unwrap();
            peer.shutdown().await.unwrap();
            let (result, drained) =
                futures_util::future::join(serve(backend, mem_send, mem_recv, io), async {
                    peer.read_to_end(&mut Vec::new()).await
                })
                .await;
            drained.unwrap();
            let error = result.unwrap_err();
            assert_eq!(error.kind, capnp::ErrorKind::Failed);
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn stopped_actors_report_closed_channels() {
    let (commands, receiver) = mpsc::channel(1);
    drop(receiver);
    assert!(matches!(
        send_message(&commands, &Arc::new(Semaphore::new(1)), 1024, vec![1]).await,
        Err(SendError::Closed)
    ));
    let mut pending = None;
    assert!(matches!(
        receive_message(&commands, &mut pending).await,
        Err(RecvError::Closed)
    ));
    assert!(pending.is_none());
}

#[test]
fn rpc_disconnects_and_protocol_failures_have_distinct_diagnostics() {
    assert!(matches!(
        rpc_send_error(capnp::Error::disconnected("gone".into())),
        SendError::Closed
    ));
    assert!(matches!(
        rpc_recv_error(capnp::Error::disconnected("gone".into())),
        RecvError::Closed
    ));
    let send = rpc_send_error(capnp::Error::failed("bad response".into()));
    let recv = rpc_recv_error(capnp::Error::failed("bad response".into()));
    for error in [&send as &dyn std::error::Error, &recv] {
        assert!(error.to_string().contains("bad response"));
        assert!(error.source().is_some());
    }
}

#[tokio::test]
async fn channel_errors_are_translated_by_backend_callbacks() {
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::LocalSet::new().run_until(async {
            for sending in [true, false] {
                let (backend, peer) = bidirectional(MemConfig::default());
                let (peer_sender, peer_receiver) = peer.into_parts();
                let retained = if sending {
                    drop(peer_receiver);
                    (Some(peer_sender), None)
                } else {
                    drop(peer_sender);
                    (None, Some(peer_receiver))
                };
                let (client, io) = tokio::io::duplex(64);
                let server = tokio::task::spawn_local(serve(backend, mem_send, mem_recv, io));
                let mut channel = CapnpChannel::connect(client, 1024).unwrap();
                if sending {
                    assert!(matches!(
                        channel.send(vec![1]).await,
                        Err(SendError::Closed)
                    ));
                } else {
                    assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
                }
                drop((channel, retained));
                server.await.unwrap().unwrap();
            }
        }),
    )
    .await
    .unwrap();
}
