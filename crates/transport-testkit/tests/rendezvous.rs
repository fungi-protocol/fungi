//! Reception acknowledgements exercise submission backpressure in generic checks.
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::{io, time::Duration};

use fungi_transport::{Bidirectional, RecvChannel, SendChannel, Unspecified};
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
    Yield,
}
type Fixture = Bidirectional<Sender, Receiver>;

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
    #[cfg_attr(coverage_nightly, coverage(off))]
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
            Mode::Yield => {
                tokio::task::yield_now().await;
                Ok(message)
            }
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
        Bidirectional::new(Sender(left), receiver(incoming_left)),
        Bidirectional::new(Sender(right), receiver(incoming_right)),
    )
}
#[tokio::test(start_paused = true)]
async fn helpers_accept_submission_that_waits_for_peer_reception() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (left, right) = pair(Mode::Deliver);
        testkit::roundtrip_both_directions(left, right).await;
        let (left, right) = pair(Mode::Deliver);
        testkit::closed_after_peer_drop(left, right, |error| error.to_string() == "closed").await;
        let (left, right) = pair(Mode::Deliver);
        testkit::recv_is_cancel_safe(left, right).await;
        let (left, right) = pair(Mode::Deliver);
        testkit::too_large_is_recoverable(left, right, MAX, |error| {
            error.kind() == io::ErrorKind::InvalidInput
        })
        .await;
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
#[tokio::test(start_paused = true)]
async fn cancellation_rejects_loss_after_reception_is_acknowledged() {
    let (left, right) = pair(Mode::Yield);
    let task = tokio::spawn(testkit::recv_is_cancel_safe(left, right));
    let result = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("message loss must be detected within the deadline");
    assert!(result.expect_err("helper accepted message loss").is_panic());
}
