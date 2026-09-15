use crate::builder::{CapnpBuilder, reap_child};
use crate::channel::{CapnpChannel, receive_message, send_message};
use crate::client::{
    Bootstrap, BuildCommand, channel_actor, new_link, rpc_build_error, rpc_recv_error,
    rpc_send_error, start_client,
};
use crate::error::{BuildError, RecvError, SendError};
use crate::protocol::{RemoteChannel, channel, recv_failure, send_failure};
use crate::server::{run_server, serve, serve_builder};
use capnp::{capability::Promise, data};
use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel};
use fungi_transport_testkit::mem::{MemConfig, duplex};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};

#[tokio::test]
async fn cleanup_reaps_a_process_that_exited_successfully() {
    let mut child = tokio::process::Command::new("true")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
    reap_child(&mut child).await;
    assert!(child.id().is_none());
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn cleanup_kills_and_reaps_a_process_that_exceeds_the_grace_period() {
    let mut child = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    assert!(child.try_wait().unwrap().is_none());
    tokio::time::timeout(std::time::Duration::from_secs(5), reap_child(&mut child))
        .await
        .unwrap();
    assert!(child.id().is_none());
    assert!(!child.wait().await.unwrap().success());
}

fn mem_send(_: fungi_transport_testkit::mem::MemError) -> SendError {
    SendError::Closed
}
fn mem_recv(_: fungi_transport_testkit::mem::MemError) -> RecvError {
    RecvError::Closed
}
fn mem_build(_: fungi_transport_testkit::mem::MemError) -> BuildError {
    BuildError::Unreachable
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
async fn canceled_sends_stay_bounded_across_splitting_without_blocking_receive() {
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::LocalSet::new().run_until(async {
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let (started, mut starts) = mpsc::unbounded_channel();
            let remote: RemoteChannel = capnp_rpc::new_client(BlockedRpcChannel {
                started,
                gate: Arc::clone(&gate),
            });
            let (client, io) = tokio::io::duplex(64);
            let server = tokio::task::spawn_local(run_server(io, remote.client));
            let mut channel = CapnpChannel::connect(client, 1024).unwrap();
            {
                let sending = channel.send(b"first".to_vec());
                tokio::pin!(sending);
                tokio::select! {
                    result = &mut sending => panic!("send completed before release: {result:?}"),
                    message = starts.recv() => assert_eq!(message.unwrap(), b"first"),
                }
            }
            let (mut sender, mut receiver) = channel.into_channel().into_parts();
            for _ in 0..32 {
                assert!(
                    tokio::time::timeout(
                        Duration::from_millis(2),
                        sender.send(b"canceled".to_vec())
                    )
                    .await
                    .is_err()
                );
            }
            assert!(matches!(
                starts.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert_eq!(receiver.recv().await.unwrap(), b"independent");
            gate.add_permits(1);
            {
                let sending = sender.send(b"after".to_vec());
                tokio::pin!(sending);
                tokio::select! {
                    result = &mut sending => panic!("send completed before release: {result:?}"),
                    message = starts.recv() => assert_eq!(message.unwrap(), b"after"),
                }
                gate.add_permits(1);
                sending.await.unwrap();
            }
            assert!(matches!(
                starts.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            drop((sender, receiver));
            server.await.unwrap().unwrap();
        }),
    )
    .await
    .unwrap();
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
            let (backend, _peer) = duplex(MemConfig::default());
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

            let (backend, _listener) = fungi_transport_testkit::mem::network(MemConfig::default());
            let (mut peer, io) = tokio::io::duplex(64);
            peer.write_all(&[1, 2, 3, 4, 0, 0, 0, 0]).await.unwrap();
            peer.shutdown().await.unwrap();
            let (result, drained) = futures_util::future::join(
                serve_builder(
                    backend,
                    |_| Ok(fungi_transport_testkit::mem::MemAddr),
                    mem_send,
                    mem_recv,
                    mem_build,
                    io,
                ),
                async { peer.read_to_end(&mut Vec::new()).await },
            )
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
async fn stopped_actors_report_closed_channels_and_unreachable_builders() {
    let (commands, receiver) = mpsc::channel(1);
    drop(receiver);
    assert!(matches!(
        send_message(&commands, &Arc::new(Semaphore::new(1)), 1024, vec![1]).await,
        Err(SendError::Closed)
    ));
    let mut pending = None;
    assert!(matches!(
        receive_message(&commands, &mut pending, usize::MAX).await,
        Err(RecvError::Closed)
    ));
    assert!(pending.is_none());

    let (commands, receiver) = mpsc::channel(1);
    drop(receiver);
    let (link, _lifetime) = new_link(1024);
    let mut builder = CapnpBuilder {
        commands,
        _link: link,
        max_recv_message_len: usize::MAX,
    };
    assert!(matches!(
        builder.build(&vec![1]).await,
        Err(BuildError::Unreachable)
    ));
}

#[tokio::test]
async fn canceled_receive_preserves_a_response_already_delivered_to_the_client() {
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::LocalSet::new().run_until(async {
            let (backend, mut peer) = duplex(MemConfig::default());
            let (client, io) = tokio::io::duplex(64);
            let server = tokio::task::spawn_local(serve(backend, mem_send, mem_recv, io));
            let mut channel = CapnpChannel::connect(client, 1024).unwrap();
            channel.send(b"native".to_vec()).await.unwrap();
            assert_eq!(peer.recv().await.unwrap(), b"native");
            assert!(
                tokio::time::timeout(Duration::from_millis(5), channel.recv())
                    .await
                    .is_err()
            );
            peer.send(b"retained".to_vec()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while channel.pending.as_ref().unwrap().is_empty() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            let (sender, mut receiver) = channel.into_channel().into_parts();
            assert_eq!(receiver.recv().await.unwrap(), b"retained");
            drop(peer);
            assert!(
                tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                    .await
                    .unwrap()
                    .is_err()
            );
            drop((sender, receiver));
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn abandoned_actor_reply_reports_an_unreachable_builder() {
    let (commands, mut receiver) = mpsc::channel::<BuildCommand>(1);
    let (link, _lifetime) = new_link(1024);
    let mut builder = CapnpBuilder {
        commands,
        _link: link,
        max_recv_message_len: usize::MAX,
    };
    let actor = async {
        drop(receiver.recv().await.unwrap());
    };
    let (result, ()) = futures_util::future::join(builder.build(&Vec::new()), actor).await;
    assert!(matches!(result, Err(BuildError::Unreachable)));
}

#[derive(Debug)]
struct OnceBuilder(Option<fungi_transport_testkit::mem::MemChannel>);
impl ChannelBuilder for OnceBuilder {
    type Privacy = fungi_transport::Pseudonymous;
    type Input = ();
    type Channel = fungi_transport_testkit::mem::MemChannel;
    type BuildError = fungi_transport_testkit::mem::MemError;
    async fn build(&mut self, _: &()) -> Result<Self::Channel, Self::BuildError> {
        self.0
            .take()
            .ok_or(fungi_transport_testkit::mem::MemError::Closed)
    }
}

#[tokio::test]
async fn builder_channel_errors_are_translated_by_the_backend_callbacks() {
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::LocalSet::new().run_until(async {
            let (backend, peer) = duplex(MemConfig::default());
            let (peer_sender, peer_receiver) = peer.into_parts();
            drop(peer_receiver);
            let (client, io) = tokio::io::duplex(64);
            let server = tokio::task::spawn_local(serve_builder(
                OnceBuilder(Some(backend)),
                |_| Ok(()),
                mem_send,
                mem_recv,
                mem_build,
                io,
            ));
            let mut builder = CapnpBuilder::connect(client, 1024).unwrap();
            let mut channel = builder.build(&Vec::new()).await.unwrap();
            assert!(matches!(
                channel.send(vec![1]).await,
                Err(SendError::Closed)
            ));
            assert!(matches!(
                builder.build(&Vec::new()).await,
                Err(BuildError::Unreachable)
            ));
            drop((channel, builder, peer_sender));
            server.await.unwrap().unwrap();
        }),
    )
    .await
    .unwrap();
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
    assert!(matches!(
        rpc_build_error(capnp::Error::disconnected("gone".into())),
        BuildError::Unreachable
    ));
    let send = rpc_send_error(capnp::Error::failed("bad response".into()));
    let recv = rpc_recv_error(capnp::Error::failed("bad response".into()));
    let build = rpc_build_error(capnp::Error::failed("bad response".into()));
    for error in [&send as &dyn std::error::Error, &recv, &build] {
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
                let (backend, peer) = duplex(MemConfig::default());
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
