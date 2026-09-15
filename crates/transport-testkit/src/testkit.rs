//! Reusable conformance checks for byte channel implementations.
//!
//! Supply fresh channels and select checks for the backend's guarantees.
//! Cancellation safety, closure detection, duplicate-free delivery, and recovery
//! after size rejection are additional properties, not requirements of the traits.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use fungi_transport::{ChannelBuilder, Duplex, RecvChannel, SendChannel};

/// Verify intact delivery in both directions.
pub async fn roundtrip_both_directions<C: SendChannel<Vec<u8>> + RecvChannel<Vec<u8>>>(
    mut left: C,
    mut right: C,
) {
    let (sent, first) =
        futures_util::future::join(left.send(b"first".to_vec()), right.recv()).await;
    sent.unwrap();

    let (sent, second) =
        futures_util::future::join(left.send(b"second".to_vec()), right.recv()).await;
    sent.unwrap();

    assert_eq!(first.unwrap(), b"first");
    assert_eq!(second.unwrap(), b"second");

    let (sent, reply) =
        futures_util::future::join(right.send(b"reply".to_vec()), left.recv()).await;
    sent.unwrap();
    assert_eq!(reply.unwrap(), b"reply");
}

/// Verify that dropping one endpoint closes the other endpoint's receiver.
///
/// Use only for backends that detect peer closure.
pub async fn closed_after_peer_drop<C: RecvChannel<Vec<u8>>>(
    peer: C,
    mut channel: C,
    is_closed: impl FnOnce(&C::RecvError) -> bool,
) {
    drop(peer);
    assert!(is_closed(&channel.recv().await.unwrap_err()));
}

/// Verify messages remain available across repeated receive cancellation.
pub async fn recv_is_cancel_safe<S: SendChannel<Vec<u8>>, R: RecvChannel<Vec<u8>>>(
    mut sender: S,
    mut receiver: R,
) {
    for _ in 0..10 {
        let attempt = tokio::time::timeout(Duration::from_millis(5), receiver.recv()).await;
        assert!(attempt.is_err());
    }

    let mut receiving = Box::pin(receiver.recv());
    let state =
        std::future::poll_fn(|cx| std::task::Poll::Ready(receiving.as_mut().poll(cx))).await;
    assert!(state.is_pending());
    let sending = sender.send(b"message".to_vec());
    tokio::pin!(sending);
    // Retain both futures so peer reception can release submission backpressure.
    let state = tokio::select! {
        sent = &mut sending => {
            sent.unwrap();
            std::future::poll_fn(|cx| std::task::Poll::Ready(receiving.as_mut().poll(cx))).await
        }
        message = &mut receiving => {
            sending.await.unwrap();
            std::task::Poll::Ready(message)
        }
    };
    let completed = match state {
        std::task::Poll::Ready(result) => Some(result),
        std::task::Poll::Pending => None,
    };
    drop(receiving);
    let message = match completed {
        Some(result) => result,
        None => tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("canceling receive must not lose an accepted message"),
    };
    assert_eq!(message.unwrap(), b"message");
}

/// Verify rejection of a payload above the declared limit.
///
/// `max + 1` must be representable and small enough to allocate for this test.
pub async fn too_large<C: SendChannel<Vec<u8>>>(
    mut channel: C,
    max: usize,
    is_too_large: impl FnOnce(&C::SendError) -> bool,
) {
    let size = max
        .checked_add(1)
        .expect("test limit must allow a larger payload");
    let message = vec![0; size];
    assert!(is_too_large(&channel.send(message).await.unwrap_err()));
}

/// Verify that a size rejection leaves the channel usable.
///
/// `max + 1` must be representable and small enough to allocate for this test.
pub async fn too_large_is_recoverable<S: SendChannel<Vec<u8>>, R: RecvChannel<Vec<u8>>>(
    mut sender: S,
    mut receiver: R,
    max: usize,
    is_too_large: impl FnOnce(&S::SendError) -> bool,
) {
    let size = max
        .checked_add(1)
        .expect("test limit must allow a larger payload");
    let message = vec![0; size];
    assert!(is_too_large(&sender.send(message).await.unwrap_err()));

    let recovery_message = vec![0x42; max.min(5)];
    let (sent, received) =
        futures_util::future::join(sender.send(recovery_message.clone()), receiver.recv()).await;
    sent.unwrap();
    assert_eq!(received.unwrap(), recovery_message);
}

/// Verify connection establishment, peer loss, and reconnection.
///
/// Use only for connection-backed channels that detect peer loss.
pub async fn build_use_drop_rebuild<C, L>(mut connector: C, mut listener: L, address: &C::Input)
where
    C: ChannelBuilder,
    C::Channel: SendChannel<Vec<u8>> + RecvChannel<Vec<u8>>,
    L: ChannelBuilder<Input = (), Channel = C::Channel>,
{
    let (client, server) =
        futures_util::future::join(connector.build(address), listener.build(&())).await;
    let (mut client, mut server) = (client.expect("connect"), server.expect("accept"));

    let (sent, received) =
        futures_util::future::join(client.send(b"message".to_vec()), server.recv()).await;
    sent.unwrap();
    assert_eq!(received.unwrap(), b"message");

    drop(server);
    let detected = tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("a dropped peer must not leave recv pending forever");
    assert!(detected.is_err());

    let (client, server) =
        futures_util::future::join(connector.build(address), listener.build(&())).await;
    let (mut client, mut server) = (client.expect("reconnect"), server.expect("reaccept"));
    let (sent, received) =
        futures_util::future::join(client.send(b"again".to_vec()), server.recv()).await;
    sent.unwrap();
    assert_eq!(received.unwrap(), b"again");
}

/// Verify lossless, duplicate-free exchange under backpressure in any order.
///
/// Requires independent directions and completion within five seconds.
pub async fn mutual_bursts_converge<S, R>(left: Duplex<S, R>, right: Duplex<S, R>, burst: usize)
where
    S: SendChannel<Vec<u8>>,
    R: RecvChannel<Vec<u8>>,
{
    async fn drive<S, R>(channel: Duplex<S, R>, tag: u8, peer_tag: u8, burst: usize)
    where
        S: SendChannel<Vec<u8>>,
        R: RecvChannel<Vec<u8>>,
    {
        let (mut sender, mut receiver) = channel.into_parts();
        let sending = async move {
            for index in 0..burst {
                let message = std::iter::once(tag).chain(index.to_le_bytes()).collect();
                sender.send(message).await.unwrap();
            }
        };
        let receiving = async move {
            let mut expected: BTreeSet<Vec<u8>> = (0..burst)
                .map(|index| {
                    std::iter::once(peer_tag)
                        .chain(index.to_le_bytes())
                        .collect()
                })
                .collect();
            for _ in 0..burst {
                let message = receiver.recv().await.unwrap();
                assert!(
                    expected.remove(&message),
                    "unexpected or duplicate burst message: {message:?}"
                );
            }
        };
        futures_util::future::join(sending, receiving).await;
    }
    let exchange = futures_util::future::join(
        drive(left, b'l', b'r', burst),
        drive(right, b'r', b'l', burst),
    );
    tokio::time::timeout(Duration::from_secs(5), exchange)
        .await
        .expect("mutual bursts must make progress");
}
