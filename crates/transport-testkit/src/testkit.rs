//! Reusable conformance checks for byte channel implementations.
//!
//! Supply fresh channels and select checks for the backend's guarantees.

use fungi_transport::{RecvChannel, SendChannel};

/// Verify intact delivery in both directions.
pub async fn roundtrip_both_directions<C: SendChannel<Vec<u8>> + RecvChannel<Vec<u8>>>(
    mut left: C,
    mut right: C,
) {
    assert_eq!(exchange(&mut left, &mut right, b"first").await, b"first");
    assert_eq!(exchange(&mut left, &mut right, b"second").await, b"second");
    assert_eq!(exchange(&mut right, &mut left, b"reply").await, b"reply");
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
