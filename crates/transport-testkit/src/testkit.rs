//! Reusable conformance checks for byte channel implementations.
//!
//! Supply fresh channels and select checks for the backend's guarantees.
//! Cancellation safety, closure detection, duplicate-free delivery, and recovery
//! after size rejection are additional properties, not requirements of the traits.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use fungi_transport::{Bidirectional, ChannelBuilder, RecvChannel, SendChannel};

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

    // Cancel both when the accepted message may already be inside the receive
    // future and after one further poll may have moved it there.
    for poll_after_send in [false, true] {
        let mut receiving = Box::pin(receiver.recv());
        let state =
            std::future::poll_fn(|cx| std::task::Poll::Ready(receiving.as_mut().poll(cx))).await;
        assert!(state.is_pending());
        let sending = sender.send(b"message".to_vec());
        tokio::pin!(sending);
        // Retain both futures so peer reception can release submission backpressure.
        // Poll submission first so acceptance is observed before the receive
        // future can return what it holds.
        let state = tokio::select! {
            biased;
            sent = &mut sending => {
                sent.unwrap();
                if poll_after_send {
                    std::future::poll_fn(|cx| std::task::Poll::Ready(receiving.as_mut().poll(cx)))
                        .await
                } else {
                    std::task::Poll::Pending
                }
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
}

/// Verify rejection of a payload above the declared limit.
///
/// The payload must be rejected before the peer receives anything.
/// `max + 1` must be representable and small enough to allocate for this test.
pub async fn too_large<S: SendChannel<Vec<u8>>, R: RecvChannel<Vec<u8>>>(
    mut sender: S,
    mut receiver: R,
    max: usize,
    is_too_large: impl FnOnce(&S::SendError) -> bool,
) {
    let reception = std::pin::pin!(receiver.recv());
    assert!(is_too_large(
        &submit_oversized(&mut sender, reception, max).await
    ));
}

/// Verify that a size rejection leaves the channel usable.
///
/// The payload must be rejected before the peer receives anything.
/// `max + 1` must be representable and small enough to allocate for this test.
pub async fn too_large_is_recoverable<S: SendChannel<Vec<u8>>, R: RecvChannel<Vec<u8>>>(
    mut sender: S,
    mut receiver: R,
    max: usize,
    is_too_large: impl FnOnce(&S::SendError) -> bool,
) {
    let mut reception = std::pin::pin!(receiver.recv());
    assert!(is_too_large(
        &submit_oversized(&mut sender, reception.as_mut(), max).await
    ));

    let recovery_message = vec![0x42; max.min(5)];
    let (sent, received) =
        futures_util::future::join(sender.send(recovery_message.clone()), reception).await;
    sent.unwrap();
    assert_eq!(received.unwrap(), recovery_message);
}

/// Submit a payload above the limit and return its rejection.
///
/// Fails if `reception` completes first.
async fn submit_oversized<S: SendChannel<Vec<u8>>, T: std::fmt::Debug>(
    sender: &mut S,
    reception: std::pin::Pin<&mut impl std::future::Future<Output = T>>,
    max: usize,
) -> S::SendError {
    let size = max
        .checked_add(1)
        .expect("test limit must allow a larger payload");
    let submission = std::pin::pin!(sender.send(vec![0; size]));
    match futures_util::future::select(submission, reception).await {
        futures_util::future::Either::Left((result, _)) => result.unwrap_err(),
        futures_util::future::Either::Right((received, _)) => {
            panic!("receiver completed before the oversized payload was rejected: {received:?}")
        }
    }
}

/// Verify connection establishment, peer loss, and reconnection.
///
/// Use only for connection-backed channels that detect peer loss on both the
/// connecting and the accepting side.
pub async fn build_use_drop_rebuild<C, L>(
    mut connector: C,
    mut listener: L,
    address: &C::Input,
    is_closed: impl Fn(&<C::Channel as RecvChannel<Vec<u8>>>::RecvError) -> bool,
) where
    C: ChannelBuilder,
    C::Channel: SendChannel<Vec<u8>> + RecvChannel<Vec<u8>>,
    L: ChannelBuilder<Input = (), Channel = C::Channel>,
{
    // Fail as soon as either side fails; the other may wait indefinitely.
    let (mut client, mut server) = futures_util::future::join(
        async { connector.build(address).await.expect("connect") },
        async { listener.build(&()).await.expect("accept") },
    )
    .await;

    let (sent, received) =
        futures_util::future::join(client.send(b"message".to_vec()), server.recv()).await;
    sent.unwrap();
    assert_eq!(received.unwrap(), b"message");
    let (sent, reply) =
        futures_util::future::join(server.send(b"reply".to_vec()), client.recv()).await;
    sent.unwrap();
    assert_eq!(reply.unwrap(), b"reply");

    drop(server);
    let detected = tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("a dropped peer must not leave recv pending forever");
    assert!(is_closed(&detected.unwrap_err()));

    let (mut client, mut server) = futures_util::future::join(
        async { connector.build(address).await.expect("reconnect") },
        async { listener.build(&()).await.expect("reaccept") },
    )
    .await;
    let (sent, received) =
        futures_util::future::join(client.send(b"again".to_vec()), server.recv()).await;
    sent.unwrap();
    assert_eq!(received.unwrap(), b"again");
    let (sent, reply) =
        futures_util::future::join(server.send(b"answer".to_vec()), client.recv()).await;
    sent.unwrap();
    assert_eq!(reply.unwrap(), b"answer");

    drop(client);
    let detected = tokio::time::timeout(Duration::from_secs(5), server.recv())
        .await
        .expect("a dropped peer must not leave recv pending forever");
    assert!(is_closed(&detected.unwrap_err()));
}

/// Verify that each side receives the peer's burst under backpressure, in any order.
///
/// Each of the `burst` receptions must be a distinct message of the burst;
/// later receptions are not observed. Requires independent directions and
/// completion within five seconds.
pub async fn mutual_bursts_converge<S, R>(
    left: Bidirectional<S, R>,
    right: Bidirectional<S, R>,
    burst: usize,
) where
    S: SendChannel<Vec<u8>>,
    R: RecvChannel<Vec<u8>>,
{
    async fn drive<S, R>(channel: Bidirectional<S, R>, tag: u8, peer_tag: u8, burst: usize)
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
