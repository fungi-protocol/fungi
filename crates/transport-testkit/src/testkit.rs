//! Reusable conformance checks for byte channel implementations.
//!
//! Supply fresh channels and select checks for the backend's guarantees.
//! Recovery after size rejection is an additional property, not a requirement
//! of the traits.

use fungi_transport::{RecvChannel, SendChannel};
use futures_util::future::{Either, FusedFuture, FutureExt};

/// Verify intact delivery in both directions.
pub async fn roundtrip_both_directions<C: SendChannel<Vec<u8>> + RecvChannel<Vec<u8>>>(
    mut left: C,
    mut right: C,
) {
    assert_eq!(exchange(&mut left, &mut right, b"first").await, b"first");
    assert_eq!(exchange(&mut left, &mut right, b"second").await, b"second");
    assert_eq!(exchange(&mut right, &mut left, b"reply").await, b"reply");
}

/// Verify rejection of a payload above the declared limit.
///
/// The payload must be rejected before the peer receives anything. A payload
/// delivered along with the rejection fails here; one delivered later fails
/// [`too_large_is_recoverable`]. Closing the channel counts as a rejection.
/// `max + 1` must be representable and small enough to allocate for this test.
pub async fn too_large<S: SendChannel<Vec<u8>>, R: RecvChannel<Vec<u8>>>(
    mut sender: S,
    mut receiver: R,
    max: usize,
    is_too_large: impl FnOnce(&S::SendError) -> bool,
) {
    let mut reception = std::pin::pin!(receiver.recv().fuse());
    assert!(is_too_large(
        &submit_oversized(&mut sender, reception.as_mut(), max).await
    ));
    if let Some(Ok(leaked)) = reception.now_or_never() {
        panic!(
            "the oversized payload reached the peer: {} bytes",
            leaked.len()
        );
    }
}

/// Verify that a size rejection leaves the channel usable.
///
/// The payload must be rejected before the peer receives anything, and the
/// next message must arrive in its place.
/// `max + 1` must be representable and small enough to allocate for this test.
pub async fn too_large_is_recoverable<S: SendChannel<Vec<u8>>, R: RecvChannel<Vec<u8>>>(
    mut sender: S,
    mut receiver: R,
    max: usize,
    is_too_large: impl FnOnce(&S::SendError) -> bool,
) {
    let mut reception = std::pin::pin!(receiver.recv().fuse());
    assert!(is_too_large(
        &submit_oversized(&mut sender, reception.as_mut(), max).await
    ));
    assert!(
        !reception.is_terminated(),
        "the channel closed on the size rejection, so it cannot recover"
    );

    let recovery_message = vec![0x42; max.min(5)];
    let (_, received) = futures_util::future::join(
        async { sender.send(recovery_message.clone()).await.unwrap() },
        reception,
    )
    .await;
    assert_eq!(received.unwrap(), recovery_message);
}

/// Submit a payload above the limit and return its rejection.
///
/// Fails if a payload reaches `reception` first. `reception` ending in an
/// error means the channel closed, which is a rejection, not a delivery.
async fn submit_oversized<S: SendChannel<Vec<u8>>, E>(
    sender: &mut S,
    reception: std::pin::Pin<&mut impl FusedFuture<Output = Result<Vec<u8>, E>>>,
    max: usize,
) -> S::SendError {
    let size = max
        .checked_add(1)
        .expect("test limit must allow a larger payload");
    let submission = std::pin::pin!(sender.send(vec![0; size]));
    match futures_util::future::select(submission, reception).await {
        Either::Left((result, _)) => result.unwrap_err(),
        Either::Right((Err(_), submission)) => submission.await.unwrap_err(),
        Either::Right((Ok(received), _)) => panic!(
            "receiver completed before the oversized payload was rejected: {} bytes",
            received.len()
        ),
    }
}

/// Send `message` while the other end receives it, failing as soon as either
/// side fails rather than waiting on the other.
async fn exchange<S: SendChannel<Vec<u8>>, R: RecvChannel<Vec<u8>>>(
    sender: &mut S,
    receiver: &mut R,
    message: &[u8],
) -> Vec<u8> {
    let sending = async { sender.send(message.to_vec()).await.unwrap() };
    let receiving = async { receiver.recv().await.unwrap() };
    futures_util::future::join(sending, receiving).await.1
}
