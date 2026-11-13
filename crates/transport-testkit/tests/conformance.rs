#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

//! Exercise conformance helpers independently of a public backend.

use fungi_transport::{ConnectionUnlinkability, Duplex, RecvChannel, SendChannel};
use fungi_transport_testkit::testkit;
use tokio::sync::mpsc;

#[derive(Debug, thiserror::Error)]
#[error("queue closed")]
struct SendError;

#[derive(Debug, thiserror::Error)]
#[error("queue closed")]
struct RecvError;

#[derive(Debug)]
struct Sender(mpsc::Sender<Vec<u8>>);

#[derive(Debug)]
struct Receiver(mpsc::Receiver<Vec<u8>>);

type BidirectionalChannel = Duplex<Sender, Receiver>;

fn pair() -> (BidirectionalChannel, BidirectionalChannel) {
    let (left, incoming_right) = mpsc::channel(1);
    let (right, incoming_left) = mpsc::channel(1);
    (
        Duplex::new(Sender(left), Receiver(incoming_left)),
        Duplex::new(Sender(right), Receiver(incoming_right)),
    )
}

impl SendChannel for Sender {
    type Privacy = ConnectionUnlinkability;
    type SendError = SendError;

    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        self.0.send(message).await.map_err(|_| SendError)
    }
}

impl RecvChannel for Receiver {
    type RecvError = RecvError;

    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        self.0.recv().await.ok_or(RecvError)
    }
}

#[tokio::test(start_paused = true)]
async fn helpers_check_delivery() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (left, right) = pair();
        testkit::roundtrip_both_directions(left, right).await;
    })
    .await
    .expect("conformance helpers must complete");
}

#[derive(Debug, Clone, Copy)]
enum Fault {
    Intact,
    Corrupt,
}

#[derive(Debug)]
struct FaultyReceiver {
    inner: Receiver,
    fault: Fault,
}

impl RecvChannel for FaultyReceiver {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        let mut message = self.inner.recv().await?;
        match self.fault {
            Fault::Intact => {}
            Fault::Corrupt => message.push(0xff),
        }
        Ok(message)
    }
}

fn faulty(channel: BidirectionalChannel, fault: Fault) -> Duplex<Sender, FaultyReceiver> {
    let (sender, receiver) = channel.into_parts();
    Duplex::new(
        sender,
        FaultyReceiver {
            inner: receiver,
            fault,
        },
    )
}

async fn rejected(future: impl std::future::Future<Output = ()> + Send + 'static) {
    let mut task = tokio::spawn(future);
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), &mut task).await;
    match result {
        Ok(result) => assert!(
            result
                .expect_err("helper accepted a defective backend")
                .is_panic()
        ),
        Err(_) => {
            task.abort();
            let _ = task.await;
            panic!("helper did not reject the backend within its deadline");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn roundtrip_rejects_corrupted_delivery() {
    let (left, right) = pair();
    rejected(testkit::roundtrip_both_directions(
        faulty(left, Fault::Intact),
        faulty(right, Fault::Corrupt),
    ))
    .await;
}

#[tokio::test(start_paused = true)]
async fn roundtrip_rejects_corrupted_reply() {
    let (left, right) = pair();
    rejected(testkit::roundtrip_both_directions(
        faulty(left, Fault::Corrupt),
        faulty(right, Fault::Intact),
    ))
    .await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "helper did not reject the backend within its deadline")]
async fn rejection_requires_the_helper_to_stop_within_its_deadline() {
    rejected(std::future::pending()).await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "helper accepted a defective backend")]
async fn rejection_requires_the_helper_to_panic() {
    rejected(async {}).await;
}
