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
