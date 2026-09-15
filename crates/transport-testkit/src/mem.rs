//! Typed in-memory links, connection setup, and mailboxes for offline peers.

use std::sync::Arc;

use fungi_transport::{Anonymous, Channel, ChannelBuilder, PeerChannel, RecvChannel, SendChannel};
use tokio::sync::mpsc;

const CONNECTION_QUEUE_CAPACITY: usize = 8;

/// Capacity of each direction of an in-memory link.
#[derive(Debug, Clone, Default)]
pub struct MemConfig {
    /// Missing or zero capacity selects one buffered message.
    pub capacity: Option<usize>,
}

/// An in-memory queue or connection setup path is closed.
#[derive(Debug, thiserror::Error)]
pub enum MemError {
    /// The destination queue or connection setup path has been dropped.
    #[error("in-memory path closed")]
    Closed,
}

/// Identity of one destination within this process.
#[derive(Debug, Clone)]
pub struct MemPeer(Arc<()>);

impl PartialEq for MemPeer {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for MemPeer {}

/// Owned sending direction to one in-memory peer.
#[derive(Debug)]
pub struct MemSender<M> {
    sender: mpsc::Sender<M>,
    peer: MemPeer,
}

impl<M> Clone for MemSender<M> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            peer: self.peer.clone(),
        }
    }
}

/// Owned receiving direction from one in-memory peer.
#[derive(Debug)]
pub struct MemReceiver<M> {
    receiver: mpsc::Receiver<M>,
    peer: MemPeer,
}

impl<M> PeerChannel for MemSender<M> {
    type Peer = MemPeer;
    fn peer(&self) -> &MemPeer {
        &self.peer
    }
}

impl<M> PeerChannel for MemReceiver<M> {
    type Peer = MemPeer;
    fn peer(&self) -> &MemPeer {
        &self.peer
    }
}

impl<M: Send> SendChannel<M> for MemSender<M> {
    // Payload-only queues preserve sender anonymity relative to the destination.
    type Privacy = Anonymous;
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
pub type MemChannel<M = Vec<u8>> = Channel<MemSender<M>, MemReceiver<M>>;

/// Create connected peers with native typed messages to exercise the channel contract.
///
/// The queues have independent capacity and cancellation-safe reception. Dropping
/// the last sender closes reception after buffered messages have been drained.
pub fn duplex<M: Send>(config: MemConfig) -> (MemChannel<M>, MemChannel<M>) {
    let capacity = config.capacity.unwrap_or(1).max(1);
    let (left_sender, right_receiver) = mpsc::channel(capacity);
    let (right_sender, left_receiver) = mpsc::channel(capacity);
    let left_peer = MemPeer(Arc::new(()));
    let right_peer = MemPeer(Arc::new(()));
    (
        Channel::new(
            MemSender {
                sender: left_sender,
                peer: right_peer.clone(),
            },
            MemReceiver {
                receiver: left_receiver,
                peer: right_peer,
            },
        )
        .unwrap(),
        Channel::new(
            MemSender {
                sender: right_sender,
                peer: left_peer.clone(),
            },
            MemReceiver {
                receiver: right_receiver,
                peer: left_peer,
            },
        )
        .unwrap(),
    )
}

/// Address of the single listener associated with a connector.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct MemAddr;

/// Creates links and submits their remote endpoints to a listener.
///
/// The listener queue retains remote endpoints so construction can complete
/// between accept calls. A saturated connection queue waits for acceptance.
/// A live destination receiver retains submitted messages between receive calls.
#[derive(Debug)]
pub struct MemChannelBuilder<M = Vec<u8>> {
    config: MemConfig,
    incoming: mpsc::Sender<MemChannel<M>>,
}

impl<M> Clone for MemChannelBuilder<M> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            incoming: self.incoming.clone(),
        }
    }
}

/// Accepts the remote endpoints created by its connector.
#[derive(Debug)]
pub struct MemListener<M = Vec<u8>> {
    incoming: mpsc::Receiver<MemChannel<M>>,
}

/// Create connection setup independently of the connected-link primitive.
pub fn network<M>(config: MemConfig) -> (MemChannelBuilder<M>, MemListener<M>) {
    let (incoming, receiver) = mpsc::channel(CONNECTION_QUEUE_CAPACITY);
    (
        MemChannelBuilder { config, incoming },
        MemListener { incoming: receiver },
    )
}

impl<M: Send> ChannelBuilder for MemChannelBuilder<M> {
    type Input = MemAddr;
    type Channel = MemChannel<M>;
    type BuildError = MemError;

    async fn build(&mut self, _address: &MemAddr) -> Result<Self::Channel, MemError> {
        let (local, remote) = duplex(self.config.clone());
        self.incoming
            .send(remote)
            .await
            .map_err(|_| MemError::Closed)?;
        Ok(local)
    }
}

impl<M: Send> ChannelBuilder for MemListener<M> {
    type Input = ();
    type Channel = MemChannel<M>;
    type BuildError = MemError;

    async fn build(&mut self, _input: &()) -> Result<Self::Channel, MemError> {
        self.incoming.recv().await.ok_or(MemError::Closed)
    }
}

/// A store that retains a peer's incoming queue between active sessions.
///
/// Storage is bounded and lives only in this process. Sending accepts a message
/// into the destination queue so peers can receive it in a later session. Dropping
/// the destination mailbox closes the path and discards unread messages.
#[derive(Debug)]
pub struct Mailbox<M> {
    sender: MemSender<M>,
    receiver: MemReceiver<M>,
}

/// Exclusive mailbox reception preserving storage ownership for later sessions.
#[derive(Debug)]
pub struct MailboxReceiver<'a, M>(&'a mut MemReceiver<M>);

impl<M> PeerChannel for MailboxReceiver<'_, M> {
    type Peer = MemPeer;
    fn peer(&self) -> &MemPeer {
        self.0.peer()
    }
}

impl<M: Send> RecvChannel<M> for MailboxReceiver<'_, M> {
    type RecvError = MemError;
    fn recv(&mut self) -> impl std::future::Future<Output = Result<M, MemError>> + Send {
        self.0.recv()
    }
}

impl<M: Send> Mailbox<M> {
    /// Attach a peer while keeping ownership of its incoming queue in storage.
    ///
    /// The exclusive borrow permits one active session per mailbox. Dropping
    /// a session keeps buffered messages available to a later session.
    pub fn connect(&mut self) -> Channel<MemSender<M>, MailboxReceiver<'_, M>> {
        Channel::new(self.sender.clone(), MailboxReceiver(&mut self.receiver)).unwrap()
    }
}

/// Create stores that retain messages between independent peer sessions.
///
/// Both stores must remain alive. A full destination queue applies backpressure
/// until that peer drains it. Payload-only queues preserve the in-memory
/// transport's sender anonymity across sessions.
pub fn store_and_forward<M: Send>(config: MemConfig) -> (Mailbox<M>, Mailbox<M>) {
    let (left, right) = duplex(config);
    let mailbox = |channel: MemChannel<M>| {
        let (sender, receiver) = channel.into_split();
        Mailbox { sender, receiver }
    };
    (mailbox(left), mailbox(right))
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
    fn composition_rejects_directions_from_different_peer_links() {
        let (first, _) = duplex::<Message>(MemConfig::default());
        let (second, _) = duplex::<Message>(MemConfig::default());
        fn anonymous<S: SendChannel<Message, Privacy = Anonymous>>(_: &S) {}
        anonymous(&first);
        let (sender, _) = first.into_split();
        let (_, receiver) = second.into_split();
        assert_eq!(
            Channel::new(sender, receiver).unwrap_err(),
            fungi_transport::PeerMismatch,
        );
    }

    #[tokio::test(start_paused = true)]
    async fn typed_peers_can_be_decomposed_and_recomposed() {
        deadline(async {
            let (left, mut right) = duplex(MemConfig::default());
            let (mut sender, mut receiver) = left.into_split();
            SendChannel::send(&mut sender, Message("request".into()))
                .await
                .unwrap();
            assert_eq!(right.recv().await.unwrap(), Message("request".into()));
            right.send(Message("reply".into())).await.unwrap();
            assert_eq!(
                RecvChannel::recv(&mut receiver).await.unwrap(),
                Message("reply".into())
            );
            let mut left = Channel::new(sender, receiver).unwrap();
            let (sender, receiver) = left.into_split();
            left = Channel::new(sender, receiver).unwrap();
            let (mut sender, _) = left.directions();
            sender.send(Message("again".into())).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), Message("again".into()));
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn channels_move_send_only_values_and_owned_payloads() {
        deadline(async {
            let (mut left, mut right) = duplex(MemConfig { capacity: Some(0) });
            left.send(std::cell::Cell::new(7)).await.unwrap();
            assert_eq!(right.recv().await.unwrap().get(), 7);
            let text = String::from("borrowed");
            let (mut left, mut right) = duplex(MemConfig::default());
            left.send(text).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), "borrowed");
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn connected_links_deliver_in_both_directions() {
        deadline(async {
            let (left, right) = duplex(MemConfig::default());
            testkit::roundtrip_both_directions(left, right).await;
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn connected_links_report_peer_closure() {
        deadline(async {
            let (left, right) = duplex(MemConfig::default());
            testkit::closed_after_peer_drop(left, right, |error| matches!(error, MemError::Closed))
                .await;
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn connected_links_preserve_messages_after_receive_cancellation() {
        deadline(async {
            let (left, right) = duplex(MemConfig::default());
            testkit::recv_is_cancel_safe(left, right).await;
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn connected_links_make_progress_under_backpressure() {
        deadline(async {
            let (left, right) = duplex(MemConfig::default());
            testkit::mutual_bursts_converge(left, right, 16).await;
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn connected_links_can_reconnect_after_peer_loss() {
        deadline(async {
            let (connector, listener) = network(MemConfig::default());
            testkit::build_use_drop_rebuild(connector, listener, &MemAddr).await;
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn listener_delivers_typed_connected_peers() {
        deadline(async {
            let (mut connector, mut listener) = network(MemConfig::default());
            let mut left = connector.build(&MemAddr).await.unwrap();
            left.send(Message("before accept".into())).await.unwrap();
            let mut right = listener.build(&()).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), Message("before accept".into()));
            drop(right);
            assert!(matches!(
                left.send(Message("closed".into())).await,
                Err(MemError::Closed)
            ));
            assert!(matches!(left.recv().await, Err(MemError::Closed)));
            drop(listener);
            assert!(matches!(
                connector.build(&MemAddr).await,
                Err(MemError::Closed)
            ));
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn stores_forward_between_non_overlapping_peer_sessions() {
        deadline(async {
            let (mut a_store, mut b_store) = store_and_forward(MemConfig::default());
            {
                let mut a = a_store.connect();
                a.send(Message("request".into())).await.unwrap();
            }
            {
                let mut b = b_store.connect();
                assert_eq!(b.recv().await.unwrap(), Message("request".into()));
                b.send(Message("reply".into())).await.unwrap();
            }
            let mut a = a_store.connect();
            assert_eq!(a.recv().await.unwrap(), Message("reply".into()));
            assert!(
                tokio::time::timeout(Duration::from_millis(1), a.recv())
                    .await
                    .is_err()
            );
            drop(a);
            {
                let mut b = b_store.connect();
                b.send(Message("later".into())).await.unwrap();
            }
            assert_eq!(
                a_store.connect().recv().await.unwrap(),
                Message("later".into())
            );
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn offline_storage_is_bounded_and_closes_when_dropped() {
        deadline(async {
            let (mut a_store, b_store) = store_and_forward(MemConfig::default());
            {
                let mut a = a_store.connect();
                a.send(Message("buffered".into())).await.unwrap();
                assert!(
                    tokio::time::timeout(Duration::from_millis(1), a.send(Message("full".into())))
                        .await
                        .is_err()
                );
            }
            drop(b_store);
            let mut a = a_store.connect();
            assert!(matches!(
                a.send(Message("closed".into())).await,
                Err(MemError::Closed)
            ));
            assert!(matches!(a.recv().await, Err(MemError::Closed)));
        })
        .await;
    }
    #[tokio::test(start_paused = true)]
    async fn store_forwards_through_separate_accepted_connections() {
        deadline(async {
            let (mut connector, mut listener) = network(MemConfig::default());
            let (mut incoming_store, mut outgoing_store) = store_and_forward(MemConfig::default());
            {
                let mut alice = connector.build(&MemAddr).await.unwrap();
                let mut relay = listener.build(&()).await.unwrap();
                alice.send(Message("stored request".into())).await.unwrap();
                incoming_store
                    .connect()
                    .send(relay.recv().await.unwrap())
                    .await
                    .unwrap();
                drop(alice);
                assert!(matches!(relay.recv().await, Err(MemError::Closed)));
            }
            {
                let mut bob = connector.build(&MemAddr).await.unwrap();
                let mut relay = listener.build(&()).await.unwrap();
                relay
                    .send(outgoing_store.connect().recv().await.unwrap())
                    .await
                    .unwrap();
                assert_eq!(bob.recv().await.unwrap(), Message("stored request".into()));
                bob.send(Message("stored reply".into())).await.unwrap();
                outgoing_store
                    .connect()
                    .send(relay.recv().await.unwrap())
                    .await
                    .unwrap();
                drop(bob);
                assert!(matches!(relay.recv().await, Err(MemError::Closed)));
            }
            let mut alice = connector.build(&MemAddr).await.unwrap();
            let mut relay = listener.build(&()).await.unwrap();
            relay
                .send(incoming_store.connect().recv().await.unwrap())
                .await
                .unwrap();
            assert_eq!(alice.recv().await.unwrap(), Message("stored reply".into()));
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn accepting_a_connection_unblocks_a_saturated_connector() {
        deadline(async {
            let (mut connector, mut listener) = network::<usize>(MemConfig::default());
            let mut clients = Vec::new();
            for index in 0..CONNECTION_QUEUE_CAPACITY {
                let mut client = connector.build(&MemAddr).await.unwrap();
                client.send(index).await.unwrap();
                clients.push(client);
            }
            let pending = connector.build(&MemAddr);
            tokio::pin!(pending);
            assert!(futures_util::poll!(&mut pending).is_pending());
            let mut accepted = listener.build(&()).await.unwrap();
            assert_eq!(accepted.recv().await.unwrap(), 0);
            let mut extra_client = pending.await.unwrap();
            extra_client.send(CONNECTION_QUEUE_CAPACITY).await.unwrap();
            for index in 1..=CONNECTION_QUEUE_CAPACITY {
                let mut accepted = listener.build(&()).await.unwrap();
                assert_eq!(accepted.recv().await.unwrap(), index);
            }
            drop(clients);
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn cloned_connectors_keep_concurrent_peer_links_distinct() {
        deadline(async {
            let (connector, mut listener) = network::<usize>(MemConfig::default());
            let connecting = async {
                futures_util::future::join_all((0..16).map(|index| {
                    let mut connector = connector.clone();
                    async move {
                        let mut client = connector.build(&MemAddr).await.unwrap();
                        client.send(index).await.unwrap();
                        assert_eq!(client.recv().await.unwrap(), index + 100);
                    }
                }))
                .await;
            };
            let accepting = async {
                let mut seen = std::collections::BTreeSet::new();
                for _ in 0..16 {
                    let mut server = listener.build(&()).await.unwrap();
                    let index = server.recv().await.unwrap();
                    assert!(seen.insert(index), "duplicate peer connection");
                    server.send(index + 100).await.unwrap();
                }
                assert_eq!(seen, (0..16).collect());
            };
            futures_util::future::join(connecting, accepting).await;
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_listener_wakes_a_connector_waiting_for_capacity() {
        deadline(async {
            let (mut connector, listener) = network::<usize>(MemConfig::default());
            let mut clients = Vec::new();
            for _ in 0..CONNECTION_QUEUE_CAPACITY {
                clients.push(connector.build(&MemAddr).await.unwrap());
            }
            let pending = connector.build(&MemAddr);
            tokio::pin!(pending);
            assert!(futures_util::poll!(&mut pending).is_pending());
            drop(listener);
            assert!(matches!(pending.await, Err(MemError::Closed)));
            for mut client in clients {
                assert!(matches!(client.recv().await, Err(MemError::Closed)));
                assert!(matches!(client.send(0).await, Err(MemError::Closed)));
            }
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn listener_drains_connections_after_all_connector_clones_are_dropped() {
        deadline(async {
            let (mut connector, mut listener) = network::<usize>(MemConfig::default());
            let mut clone = connector.clone();
            let mut first = connector.build(&MemAddr).await.unwrap();
            let mut second = clone.build(&MemAddr).await.unwrap();
            first.send(1).await.unwrap();
            second.send(2).await.unwrap();
            drop(connector);
            let mut accepted = listener.build(&()).await.unwrap();
            assert_eq!(accepted.recv().await.unwrap(), 1);
            drop(clone);
            let mut accepted = listener.build(&()).await.unwrap();
            assert_eq!(accepted.recv().await.unwrap(), 2);
            assert!(matches!(listener.build(&()).await, Err(MemError::Closed)));
            accepted.send(3).await.unwrap();
            assert_eq!(second.recv().await.unwrap(), 3);

            let (connector, mut listener) = network::<usize>(MemConfig::default());
            let clone = connector.clone();
            let pending = listener.build(&());
            tokio::pin!(pending);
            assert!(futures_util::poll!(&mut pending).is_pending());
            drop(connector);
            assert!(futures_util::poll!(&mut pending).is_pending());
            drop(clone);
            assert!(matches!(pending.await, Err(MemError::Closed)));
        })
        .await;
    }
}
