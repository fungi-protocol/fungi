//! Reception acknowledgements exercise submission backpressure in generic checks.

use std::{io, time::Duration};

use fungi_transport::{
    Channel, ChannelBuilder, PeerChannel, RecvChannel, SendChannel, Unspecified,
};
use fungi_transport_testkit::testkit;
use tokio::sync::{mpsc, oneshot};

const MAX: usize = 8;
type Message = (Vec<u8>, oneshot::Sender<()>);
struct Sender(mpsc::Sender<Message>);
struct Receiver {
    queue: mpsc::Receiver<Message>,
    mode: Mode,
    saved: Option<Vec<u8>>,
}
#[derive(Clone, Copy)]
enum Mode {
    Deliver,
    Retain,
    Lose,
}
type Fixture = Channel<Sender, Receiver>;

impl PeerChannel for Sender {
    type Peer = ();
    fn peer(&self) -> &() {
        &()
    }
}
impl PeerChannel for Receiver {
    type Peer = ();
    fn peer(&self) -> &() {
        &()
    }
}
impl SendChannel for Sender {
    type Privacy = Unspecified;
    type SendError = io::Error;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), io::Error> {
        if message.len() > MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "payload exceeds limit",
            ));
        }
        let (acknowledge, received) = oneshot::channel();
        self.0
            .send((message, acknowledge))
            .await
            .map_err(io::Error::other)?;
        received.await.map_err(io::Error::other)
    }
}
impl RecvChannel for Receiver {
    type RecvError = io::Error;
    async fn recv(&mut self) -> Result<Vec<u8>, io::Error> {
        if let Some(message) = self.saved.take() {
            return Ok(message);
        }
        let (message, acknowledge) = self
            .queue
            .recv()
            .await
            .ok_or_else(|| io::Error::other("closed"))?;
        let _ = acknowledge.send(());
        match self.mode {
            Mode::Deliver => Ok(message),
            Mode::Retain => {
                self.saved = Some(message);
                std::future::pending().await
            }
            Mode::Lose => std::future::pending().await,
        }
    }
}
fn pair(mode: Mode) -> (Fixture, Fixture) {
    let (left, incoming_right) = mpsc::channel(1);
    let (right, incoming_left) = mpsc::channel(1);
    let receiver = |queue| Receiver {
        queue,
        mode,
        saved: None,
    };
    (
        Channel::new(Sender(left), receiver(incoming_left)).unwrap(),
        Channel::new(Sender(right), receiver(incoming_right)).unwrap(),
    )
}
struct Connector(mpsc::Sender<Fixture>);
struct Listener(mpsc::Receiver<Fixture>);
impl ChannelBuilder for Connector {
    type Input = ();
    type Channel = Fixture;
    type BuildError = io::Error;
    async fn build(&mut self, _: &()) -> Result<Fixture, io::Error> {
        let (client, server) = pair(Mode::Deliver);
        self.0.send(server).await.map_err(io::Error::other)?;
        Ok(client)
    }
}
impl ChannelBuilder for Listener {
    type Input = ();
    type Channel = Fixture;
    type BuildError = io::Error;
    async fn build(&mut self, _: &()) -> Result<Fixture, io::Error> {
        self.0
            .recv()
            .await
            .ok_or_else(|| io::Error::other("listener closed"))
    }
}
#[tokio::test(start_paused = true)]
async fn helpers_accept_submission_that_waits_for_peer_reception() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (left, right) = pair(Mode::Deliver);
        testkit::roundtrip_both_directions(left, right).await;
        let (left, right) = pair(Mode::Deliver);
        testkit::recv_is_cancel_safe(left, right).await;
        let (left, right) = pair(Mode::Deliver);
        testkit::too_large_is_recoverable(left, right, MAX, |error| {
            error.kind() == io::ErrorKind::InvalidInput
        })
        .await;
        let (queue, receiver) = mpsc::channel(1);
        testkit::build_use_drop_rebuild(Connector(queue), Listener(receiver), &()).await;
    })
    .await
    .expect("helpers must drive reception while submission waits");
}
#[tokio::test(start_paused = true)]
async fn cancellation_preserves_a_retained_message_while_submission_waits() {
    let (left, right) = pair(Mode::Retain);
    tokio::time::timeout(
        Duration::from_secs(5),
        testkit::recv_is_cancel_safe(left, right),
    )
    .await
    .expect("retained reception must survive cancellation");
}
#[tokio::test(start_paused = true)]
async fn cancellation_rejects_message_loss_while_submission_waits() {
    let (left, right) = pair(Mode::Lose);
    let task = tokio::spawn(testkit::recv_is_cancel_safe(left, right));
    let result = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("message loss must be detected within the deadline");
    assert!(result.expect_err("helper accepted message loss").is_panic());
}
