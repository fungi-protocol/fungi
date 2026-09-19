use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

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
fn assert_pseudonymous_strings<C: SendChannel<String, Privacy = Pseudonymous>>(_: &C) {}
fn assert_pseudonymous_bytes<C: SendChannel<Vec<u8>, Privacy = Pseudonymous>>(_: &C) {}

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

#[derive(Debug)]
struct SharedBackend {
    peer: u8,
    sender: mpsc::Sender<u32>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<u32>>,
    drops: Arc<AtomicUsize>,
}

impl SharedBackend {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        Self {
            peer: 1,
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

impl PeerChannel for SharedBackend {
    type Peer = u8;

    fn peer(&self) -> &Self::Peer {
        &self.peer
    }
}

impl SendChannel<String> for SharedBackend {
    type Privacy = Pseudonymous;
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

impl Channel<u32, String> for SharedBackend {}

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
async fn shared_split_allows_independent_progress() {
    let mut native = SharedBackend::new();
    native.send("native".to_owned()).await.unwrap();
    assert_eq!(native.recv().await.unwrap(), 6);
    native.receiver.get_mut().close();
    assert_eq!(
        native.recv().await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        native.recv_shared().await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );

    let backend = SharedBackend::new();
    let drops = Arc::clone(&backend.drops);
    let (mut sender, mut receiver) = split(backend);

    fn assert_pseudonymous<C: SendChannel<String, Privacy = Pseudonymous>>(_: &C) {}
    assert_pseudonymous(&sender);
    assert_eq!(sender.peer(), receiver.peer());

    {
        let receive = receiver.recv();
        let mut receive = std::pin::pin!(receive);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(receive.as_mut().poll(&mut context), Poll::Pending));

        sender.send("shared".to_owned()).await.unwrap();
        assert_eq!(receive.await.unwrap(), 6);
    }

    sender.send("one".to_owned()).await.unwrap();
    {
        let send = sender.send("next".to_owned());
        let mut send = std::pin::pin!(send);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(send.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(receiver.recv().await.unwrap(), 3);
        send.await.unwrap();
    }
    assert_eq!(receiver.recv().await.unwrap(), 4);

    drop(sender);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(receiver);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn transformations_adapt_each_channel_direction() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (sender, receiver) = simplex(1);
        let mut sender = Contramap::new(sender, |message: String| message.len() as u32);
        let mut receiver = Map::new(receiver, |message: u32| message.to_string());

        assert_pseudonymous_strings(&sender);
        assert_eq!(sender.peer(), receiver.peer());
        sender.send("hello".to_owned()).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), "5");
    })
    .await
    .expect("message transformation must complete");
}

#[tokio::test]
async fn map_accepts_a_stateful_transformation() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (mut sender, receiver) = simplex(2);
        let mut count = 0;
        let mut receiver = Map::new(receiver, move |message| {
            count += 1;
            (count, message)
        });

        sender.send(7).await.unwrap();
        sender.send(8).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), (1, 7));
        assert_eq!(receiver.recv().await.unwrap(), (2, 8));
    })
    .await
    .expect("message transformation must complete");
}

#[tokio::test]
async fn transformations_compose_with_shared_halves() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (sender, receiver) = split(SharedBackend::new());
        let mut sender = Contramap::new(sender, |message: Vec<u8>| {
            String::from_utf8(message).unwrap()
        });
        let mut receiver = Map::new(receiver, |message: u32| message.to_string());

        assert_pseudonymous_bytes(&sender);
        sender.send(b"shared".to_vec()).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), "6");
    })
    .await
    .expect("message transformation must complete");
}

#[tokio::test]
async fn transformations_preserve_transport_errors() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (sender, receiver) = simplex(1);
        drop(receiver);
        let mut sender = Contramap::new(sender, |message: String| message.len() as u32);
        assert!(sender.send("closed".to_owned()).await.is_err());

        let (sender, receiver) = simplex(1);
        drop(sender);
        let mut receiver = Map::new(receiver, |message: u32| message.to_string());
        assert_eq!(
            receiver.recv().await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    })
    .await
    .expect("message transformation must complete");
}

#[tokio::test]
async fn contramap_accepts_non_send_intermediate_messages() {
    struct Sink(Arc<AtomicUsize>);

    impl SendChannel<std::rc::Rc<usize>> for Sink {
        type Privacy = Unspecified;
        type SendError = Infallible;

        fn send(
            &mut self,
            message: std::rc::Rc<usize>,
        ) -> impl Future<Output = Result<(), Self::SendError>> + Send {
            self.0.store(*message, Ordering::SeqCst);
            std::future::ready(Ok(()))
        }
    }

    fn require_send<F: Future + Send>(future: F) -> F {
        future
    }

    let received = Arc::new(AtomicUsize::new(0));
    let mut sender = Contramap::new(Sink(Arc::clone(&received)), std::rc::Rc::new);
    require_send(sender.send(42)).await.unwrap();
    assert_eq!(received.load(Ordering::SeqCst), 42);
}
