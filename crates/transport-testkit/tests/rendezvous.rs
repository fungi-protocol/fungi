#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

//! Reception acknowledgements exercise submission backpressure in generic checks.

use std::{io, time::Duration};

use fungi_transport::{Bidirectional, RecvChannel, SendChannel, Unspecified};
use fungi_transport_testkit::testkit;
use tokio::sync::{mpsc, oneshot};

const MAX: usize = 8;
type Message = (Vec<u8>, oneshot::Sender<()>);
struct Sender(mpsc::Sender<Message>);
struct Receiver(mpsc::Receiver<Message>);
type BidirectionalChannel = Bidirectional<Sender, Receiver>;

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
        let (message, acknowledge) = self
            .0
            .recv()
            .await
            .ok_or_else(|| io::Error::other("closed"))?;
        let _ = acknowledge.send(());
        Ok(message)
    }
}
fn pair() -> (BidirectionalChannel, BidirectionalChannel) {
    let (left, incoming_right) = mpsc::channel(1);
    let (right, incoming_left) = mpsc::channel(1);
    (
        Bidirectional::new(Sender(left), Receiver(incoming_left)),
        Bidirectional::new(Sender(right), Receiver(incoming_right)),
    )
}
#[tokio::test(start_paused = true)]
async fn helpers_accept_submission_that_waits_for_peer_reception() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (left, right) = pair();
        testkit::roundtrip_both_directions(left, right).await;
        let (left, right) = pair();
        testkit::closed_after_peer_drop(left, right, |error| error.to_string() == "closed").await;
        let (left, right) = pair();
        testkit::too_large_is_recoverable(left, right, MAX, |error| {
            error.kind() == io::ErrorKind::InvalidInput
        })
        .await;
    })
    .await
    .expect("helpers must drive reception while submission waits");
}
#[tokio::test(start_paused = true)]
#[should_panic(expected = "receiver completed before the oversized payload was rejected")]
async fn limit_check_rejects_an_oversized_payload_that_reaches_the_peer() {
    let (left, right) = pair();
    testkit::too_large(left, right, MAX - 1, |_| true).await;
}
#[tokio::test(start_paused = true)]
#[should_panic(expected = "receiver completed before the oversized payload was rejected")]
async fn recovery_rejects_an_oversized_payload_that_reaches_the_peer() {
    let (left, right) = pair();
    testkit::too_large_is_recoverable(left, right, MAX - 1, |_| true).await;
}
