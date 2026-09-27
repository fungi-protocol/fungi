//! Reusable conformance checks for byte channel implementations.
//!
//! Supply fresh channels and select checks for the backend's guarantees.
//! Closure detection and recovery after size rejection are additional properties,
//! not requirements of the traits.

use fungi_transport::{RecvChannel, SendChannel};

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
