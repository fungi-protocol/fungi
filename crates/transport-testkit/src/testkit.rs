//! Reusable conformance checks for byte channel implementations.
//!
//! Supply fresh channels and select checks for the backend's guarantees.
//! Cancellation safety and recovery after size rejection are additional
//! properties, not requirements of the traits.

use std::future::Future;
use std::time::Duration;

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

/// Verify messages remain available across repeated receive cancellation.
///
/// The receiver must wait while no message is available; a receive that never
/// waits has nothing to cancel.
pub async fn recv_is_cancel_safe<S: SendChannel<Vec<u8>>, R: RecvChannel<Vec<u8>>>(
    mut sender: S,
    mut receiver: R,
) {
    for _ in 0..10 {
        let attempt = tokio::time::timeout(Duration::from_millis(5), receiver.recv()).await;
        assert!(
            attempt.is_err(),
            "recv must wait while no message is available"
        );
    }

    // Cancel both when the accepted message may already be inside the receive
    // future and after one further poll may have moved it there.
    for poll_after_send in [false, true] {
        let mut receiving = Box::pin(receiver.recv());
        let state =
            std::future::poll_fn(|cx| std::task::Poll::Ready(receiving.as_mut().poll(cx))).await;
        assert!(
            state.is_pending(),
            "recv must wait while no message is available"
        );
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
