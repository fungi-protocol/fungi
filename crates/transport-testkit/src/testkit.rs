//! Reusable conformance checks for byte channel implementations.
//!
//! Supply fresh channels and select checks for the backend's guarantees.
//! Closure detection is an additional property, not a requirement of the traits.

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
