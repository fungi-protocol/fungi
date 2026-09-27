//! Reception acknowledgements exercise submission backpressure in generic checks.

use std::{io, time::Duration};

use fungi_transport::{Bidirectional, RecvChannel, SendChannel, Unspecified};
use fungi_transport_testkit::testkit;
use tokio::sync::{mpsc, oneshot};

type Message = (Vec<u8>, oneshot::Sender<()>);
struct Sender(mpsc::Sender<Message>);
struct Receiver(mpsc::Receiver<Message>);
type Fixture = Bidirectional<Sender, Receiver>;

impl SendChannel for Sender {
    type Privacy = Unspecified;
    type SendError = io::Error;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), io::Error> {
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
fn pair() -> (Fixture, Fixture) {
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
    })
    .await
    .expect("helpers must drive reception while submission waits");
}
