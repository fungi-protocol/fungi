use std::convert::Infallible;
use std::io;

use tokio::sync::mpsc;

use crate::*;

#[derive(Debug)]
struct Sender {
    peer: u8,
    sender: mpsc::Sender<u32>,
}

#[derive(Debug)]
struct Receiver {
    peer: u8,
    receiver: mpsc::Receiver<u32>,
}

impl PeerChannel for Sender {
    type Peer = u8;

    fn peer(&self) -> &u8 {
        &self.peer
    }
}

impl PeerChannel for Receiver {
    type Peer = u8;

    fn peer(&self) -> &u8 {
        &self.peer
    }
}

impl SendChannel<u32> for Sender {
    type Privacy = Pseudonymous;
    type SendError = mpsc::error::SendError<u32>;

    async fn send(&mut self, message: u32) -> Result<(), Self::SendError> {
        self.sender.send(message).await
    }
}

impl SendChannel<String> for Sender {
    type Privacy = Pseudonymous;
    type SendError = mpsc::error::SendError<u32>;

    async fn send(&mut self, message: String) -> Result<(), Self::SendError> {
        self.sender.send(message.len() as u32).await
    }
}

impl RecvChannel<u32> for Receiver {
    type RecvError = io::Error;

    async fn recv(&mut self) -> Result<u32, Self::RecvError> {
        self.receiver
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "receiver closed"))
    }
}

fn simplex(capacity: usize) -> (Sender, Receiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    (Sender { peer: 1, sender }, Receiver { peer: 1, receiver })
}

fn assert_channel<C: Channel<u32, u32>>(_: &C) {}
fn assert_asymmetric_channel<C: Channel<u32, String>>(_: &C) {}
fn assert_pseudonymous<C: SendChannel<u32, Privacy = Pseudonymous>>(_: &C) {}

#[tokio::test]
async fn duplex_combines_sending_and_receiving() {
    let (sender, receiver) = simplex(1);
    let mut channel = Duplex::new(sender, receiver);

    assert_channel(&channel);
    assert_pseudonymous(&channel);

    channel.send(42).await.unwrap();
    assert_eq!(channel.recv().await.unwrap(), 42);

    let (mut sender, mut receiver) = channel.into_parts();
    assert_eq!(sender.peer(), receiver.peer());
    sender.send(43).await.unwrap();
    assert_eq!(receiver.recv().await.unwrap(), 43);
    drop(sender);
    assert_eq!(
        receiver.recv().await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn duplex_supports_different_message_types_by_direction() {
    let (sender, receiver) = simplex(1);
    let mut channel = Duplex::new(sender, receiver);

    assert_asymmetric_channel(&channel);
    SendChannel::<String>::send(&mut channel, "hello".to_owned())
        .await
        .unwrap();
    assert_eq!(channel.recv().await.unwrap(), 5);
}

#[tokio::test]
async fn duplex_does_not_require_peer_identity() {
    let mut channel = Duplex::new(Sequential::default(), Sequential(Some(8)));
    assert_channel(&channel);
    channel.send(7).await.unwrap();
    assert_eq!(channel.recv().await.unwrap(), 8);
    let (mut sender, _) = channel.into_parts();
    assert_eq!(sender.recv().await.unwrap(), 7);
}

#[derive(Debug, Default)]
struct Sequential(Option<u32>);

impl SendChannel<u32> for Sequential {
    type Privacy = Pseudonymous;
    type SendError = Infallible;

    async fn send(&mut self, message: u32) -> Result<(), Self::SendError> {
        self.0 = Some(message);
        Ok(())
    }
}

impl RecvChannel<u32> for Sequential {
    type RecvError = io::Error;

    async fn recv(&mut self) -> Result<u32, Self::RecvError> {
        self.0
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "empty"))
    }
}

impl Channel<u32, u32> for Sequential {}

#[tokio::test]
async fn channel_does_not_require_shared_access() {
    let mut channel = Sequential::default();

    assert_channel(&channel);
    assert_pseudonymous(&channel);
    channel.send(7).await.unwrap();
    assert_eq!(channel.recv().await.unwrap(), 7);
    assert_eq!(
        channel.recv().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}
