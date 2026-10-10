//! Typed in-memory links.

use fungi_transport::{Bidirectional, ConnectionUnlinkability, RecvChannel, SendChannel};
use tokio::sync::mpsc;

/// Capacity of each direction of an in-memory link.
#[derive(Debug, Clone, Default)]
pub struct MemConfig {
    /// Missing or zero capacity selects one buffered message; capacity above
    /// [`tokio::sync::Semaphore::MAX_PERMITS`] selects that maximum.
    pub capacity: Option<usize>,
}

/// An in-memory queue is closed.
#[derive(Debug, thiserror::Error)]
pub enum MemError {
    /// The other end of the queue has been dropped: sending finds no receiver,
    /// or receiving finds no sender and no buffered message.
    #[error("in-memory path closed")]
    Closed,
}

/// Owned sending direction to one in-memory peer.
#[derive(Debug)]
pub struct MemSender<M> {
    sender: mpsc::Sender<M>,
}

/// Owned receiving direction from one in-memory peer.
#[derive(Debug)]
pub struct MemReceiver<M> {
    receiver: mpsc::Receiver<M>,
}

impl<M: Send> SendChannel<M> for MemSender<M> {
    // A queue links messages within the session without exposing a sender identifier.
    type Privacy = ConnectionUnlinkability;
    type SendError = MemError;

    async fn send(&mut self, message: M) -> Result<(), MemError> {
        self.sender
            .send(message)
            .await
            .map_err(|_| MemError::Closed)
    }
}

impl<M: Send> RecvChannel<M> for MemReceiver<M> {
    type RecvError = MemError;

    async fn recv(&mut self) -> Result<M, MemError> {
        self.receiver.recv().await.ok_or(MemError::Closed)
    }
}

/// One endpoint of two crossed, bounded message queues.
pub type MemChannel<M = Vec<u8>> = Bidirectional<MemSender<M>, MemReceiver<M>>;

/// Create two connected endpoints with independent bounded queues.
///
/// Reception is cancellation-safe. Dropping the last sender closes reception
/// after buffered messages are drained.
pub fn bidirectional<M: Send>(config: MemConfig) -> (MemChannel<M>, MemChannel<M>) {
    let capacity = config
        .capacity
        .unwrap_or(1)
        .clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
    let (left_sender, right_receiver) = mpsc::channel(capacity);
    let (right_sender, left_receiver) = mpsc::channel(capacity);

    (
        Bidirectional::new(
            MemSender {
                sender: left_sender,
            },
            MemReceiver {
                receiver: left_receiver,
            },
        ),
        Bidirectional::new(
            MemSender {
                sender: right_sender,
            },
            MemReceiver {
                receiver: right_receiver,
            },
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;
    use fungi_transport::{RecvChannel, SendChannel};
    use std::time::Duration;

    async fn deadline(workflow: impl std::future::Future<Output = ()>) {
        tokio::time::timeout(Duration::from_secs(5), workflow)
            .await
            .expect("in-memory workflow timed out");
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Message(Box<str>);

    #[test]
    fn directions_preserve_connection_unlinkability() {
        let (channel, _) = bidirectional::<Message>(MemConfig::default());
        fn check<S: SendChannel<Message, Privacy = ConnectionUnlinkability>>(_: &S) {}
        check(&channel);
        let (sender, _) = channel.into_parts();
        check(&sender);
    }

    #[tokio::test(start_paused = true)]
    async fn typed_peers_can_be_decomposed_and_recomposed() {
        deadline(async {
            let (left, mut right) = bidirectional(MemConfig::default());
            let (mut sender, mut receiver) = left.into_parts();
            SendChannel::send(&mut sender, Message("request".into()))
                .await
                .unwrap();
            assert_eq!(right.recv().await.unwrap(), Message("request".into()));
            right.send(Message("reply".into())).await.unwrap();
            assert_eq!(
                RecvChannel::recv(&mut receiver).await.unwrap(),
                Message("reply".into())
            );
            let mut left = Bidirectional::new(sender, receiver);
            left.send(Message("again".into())).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), Message("again".into()));
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn channels_move_send_only_values_and_owned_payloads() {
        deadline(async {
            let (mut left, mut right) = bidirectional(MemConfig { capacity: Some(0) });
            left.send(std::cell::Cell::new(7)).await.unwrap();
            assert_eq!(right.recv().await.unwrap().get(), 7);
            let text = String::from("borrowed");
            let (mut left, mut right) = bidirectional(MemConfig::default());
            left.send(text).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), "borrowed");
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn connected_links_deliver_in_both_directions() {
        deadline(async {
            let (left, right) = bidirectional(MemConfig::default());
            testkit::roundtrip_both_directions(left, right).await;
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn connected_links_report_peer_closure() {
        deadline(async {
            let (left, right) = bidirectional(MemConfig::default());
            testkit::closed_after_peer_drop(left, right, |error| matches!(error, MemError::Closed))
                .await;
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn connected_links_preserve_messages_after_receive_cancellation() {
        deadline(async {
            let (left, right) = bidirectional(MemConfig::default());
            testkit::recv_is_cancel_safe(left, right).await;
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn connected_links_make_progress_under_backpressure() {
        deadline(async {
            let (left, right) = bidirectional(MemConfig::default());
            testkit::mutual_bursts_converge(left, right, 16).await;
        })
        .await;
    }

    #[test]
    fn capacity_is_bounded_to_what_a_queue_supports() {
        const MAX: usize = tokio::sync::Semaphore::MAX_PERMITS;
        for (requested, selected) in [
            (None, 1),
            (Some(0), 1),
            (Some(3), 3),
            (Some(MAX), MAX),
            (Some(MAX + 1), MAX),
            (Some(usize::MAX), MAX),
        ] {
            let (left, right) = bidirectional::<Message>(MemConfig {
                capacity: requested,
            });
            for channel in [left, right] {
                let (sender, _) = channel.into_parts();
                assert_eq!(sender.sender.max_capacity(), selected, "{requested:?}");
            }
        }
    }
}
