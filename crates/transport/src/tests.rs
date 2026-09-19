use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use tokio::sync::mpsc;

use crate::shared;
use crate::*;

#[derive(Debug)]
struct Sender {
    sender: mpsc::Sender<u32>,
}

#[derive(Debug)]
struct Receiver {
    receiver: mpsc::Receiver<u32>,
}

impl SendChannel<u32> for Sender {
    type Privacy = ConnectionUnlinkability;
    type SendError = mpsc::error::SendError<u32>;

    async fn send(&mut self, message: u32) -> Result<(), Self::SendError> {
        self.sender.send(message).await
    }
}

impl SendChannel<String> for Sender {
    type Privacy = ConnectionUnlinkability;
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

fn unidirectional(capacity: usize) -> (Sender, Receiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    (Sender { sender }, Receiver { receiver })
}

#[test]
fn types_carry_their_capabilities() {
    fn bidirectional<C: Bidirectional<I, O>, I, O>() {}
    fn privacy<C: SendChannel<M, Privacy = P>, M, P>() {}

    bidirectional::<Duplex<Sender, Receiver>, u32, u32>();
    bidirectional::<Duplex<Sender, Receiver>, u32, String>();
    bidirectional::<Duplex<Sequential, Sequential>, u32, u32>();
    bidirectional::<Sequential, u32, u32>();

    privacy::<Duplex<Sender, Receiver>, u32, ConnectionUnlinkability>();
    privacy::<Sequential, u32, ConnectionUnlinkability>();
    privacy::<shared::SendHalf<SharedBackend, u32, String>, String, ConnectionUnlinkability>();
}

#[tokio::test]
async fn duplex_combines_sending_and_receiving() {
    let (sender, receiver) = unidirectional(1);
    let mut channel = Duplex::new(sender, receiver);

    channel.send(42).await.unwrap();
    assert_eq!(channel.recv().await.unwrap(), 42);

    let (mut sender, mut receiver) = channel.into_parts();

    sender.send(43).await.unwrap();
    assert_eq!(receiver.recv().await.unwrap(), 43);
    drop(sender);
    assert_eq!(
        receiver.recv().await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn supports_different_message_types_by_direction() {
    let (sender, receiver) = unidirectional(1);
    let mut channel = Duplex::new(sender, receiver);

    SendChannel::<String>::send(&mut channel, "hello".to_owned())
        .await
        .unwrap();
    assert_eq!(channel.recv().await.unwrap(), 5);
}

#[tokio::test]
async fn preserves_independent_direction_state() {
    let mut channel = Duplex::new(Sequential::default(), Sequential(Some(8)));
    channel.send(7).await.unwrap();
    assert_eq!(channel.recv().await.unwrap(), 8);
    let (mut sender, _) = channel.into_parts();
    assert_eq!(sender.recv().await.unwrap(), 7);
}

#[derive(Debug, Default)]
struct Sequential(Option<u32>);

impl SendChannel<u32> for Sequential {
    type Privacy = ConnectionUnlinkability;
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

impl Bidirectional<u32, u32> for Sequential {}

#[derive(Debug)]
struct SharedBackend {
    sender: mpsc::Sender<u32>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<u32>>,
    drops: Arc<AtomicUsize>,
}

impl SharedBackend {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        Self {
            sender,
            receiver: tokio::sync::Mutex::new(receiver),
            drops: Arc::default(),
        }
    }
}

impl Drop for SharedBackend {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl SendChannel<String> for SharedBackend {
    type Privacy = ConnectionUnlinkability;
    type SendError = mpsc::error::SendError<u32>;

    async fn send(&mut self, message: String) -> Result<(), Self::SendError> {
        self.sender.send(message.len() as u32).await
    }
}

impl RecvChannel<u32> for SharedBackend {
    type RecvError = io::Error;

    async fn recv(&mut self) -> Result<u32, Self::RecvError> {
        self.receiver
            .get_mut()
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "receiver closed"))
    }
}

impl Bidirectional<u32, String> for SharedBackend {}

impl SharedChannel<u32, String> for SharedBackend {
    async fn send_shared(&self, message: String) -> Result<(), Self::SendError> {
        self.sender.send(message.len() as u32).await
    }

    async fn recv_shared(&self) -> Result<u32, Self::RecvError> {
        self.receiver
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "receiver closed"))
    }
}

#[tokio::test]
async fn split_halves_do_not_block_each_other() {
    let (mut sender, mut receiver) = shared::split(SharedBackend::new());

    let receive = receiver.recv();
    let mut receive = std::pin::pin!(receive);
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(receive.as_mut().poll(&mut context), Poll::Pending));

    sender.send("shared".to_owned()).await.unwrap();
    assert_eq!(receive.await.unwrap(), 6);
}

#[test]
fn split_keeps_the_backend_until_both_halves_drop() {
    let backend = SharedBackend::new();
    let drops = Arc::clone(&backend.drops);
    let (sender, receiver) = shared::split(backend);

    drop(sender);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(receiver);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
