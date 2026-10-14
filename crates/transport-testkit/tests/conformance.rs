#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

//! Exercise conformance helpers independently of a public backend.

use fungi_transport::{Bidirectional, ConnectionUnlinkability, RecvChannel, SendChannel};
use fungi_transport_testkit::testkit;
use tokio::sync::mpsc;

#[derive(Debug, thiserror::Error)]
enum SendError {
    #[error("message exceeds {max} bytes")]
    TooLarge { max: usize },
    #[error("queue closed")]
    Closed,
}

#[derive(Debug, thiserror::Error)]
#[error("queue closed")]
struct RecvError;

#[derive(Debug)]
struct Sender {
    queue: mpsc::Sender<Vec<u8>>,
    max: usize,
}

#[derive(Debug)]
struct Receiver(mpsc::Receiver<Vec<u8>>);

type BidirectionalChannel = Bidirectional<Sender, Receiver>;

fn pair(max: usize) -> (BidirectionalChannel, BidirectionalChannel) {
    let (left, incoming_right) = mpsc::channel(1);
    let (right, incoming_left) = mpsc::channel(1);
    (
        Bidirectional::new(Sender { queue: left, max }, Receiver(incoming_left)),
        Bidirectional::new(Sender { queue: right, max }, Receiver(incoming_right)),
    )
}

impl SendChannel for Sender {
    type Privacy = ConnectionUnlinkability;
    type SendError = SendError;

    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        if message.len() > self.max {
            return Err(SendError::TooLarge { max: self.max });
        }
        self.queue
            .send(message)
            .await
            .map_err(|_| SendError::Closed)
    }
}

impl RecvChannel for Receiver {
    type RecvError = RecvError;

    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        self.0.recv().await.ok_or(RecvError)
    }
}

#[tokio::test(start_paused = true)]
async fn helpers_check_delivery_closure_and_limits() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (left, right) = pair(1024);
        testkit::roundtrip_both_directions(left, right).await;

        let (left, right) = pair(1024);
        testkit::closed_after_peer_drop(left, right, |_| true).await;

        let (left, right) = pair(8);
        testkit::too_large(left, right, 8, |error| {
            matches!(error, SendError::TooLarge { max: 8 })
        })
        .await;

        let (left, right) = pair(8);
        testkit::too_large_is_recoverable(left, right, 8, |error| {
            matches!(error, SendError::TooLarge { max: 8 })
        })
        .await;
    })
    .await
    .expect("conformance helpers must complete");
}

#[derive(Debug, Clone, Copy)]
enum Fault {
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
            Fault::Corrupt => message.push(0xff),
        }
        Ok(message)
    }
}
fn faulty(channel: BidirectionalChannel, fault: Fault) -> Bidirectional<Sender, FaultyReceiver> {
    let (sender, receiver) = channel.into_parts();
    Bidirectional::new(
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
async fn roundtrip_rejects_corruption() {
    let (left, right) = pair(1024);
    rejected(testkit::roundtrip_both_directions(
        faulty(left, Fault::Corrupt),
        faulty(right, Fault::Corrupt),
    ))
    .await;
}
#[tokio::test(start_paused = true)]
async fn closure_rejects_an_unrecognized_error() {
    let (left, right) = pair(1024);
    rejected(testkit::closed_after_peer_drop(left, right, |_| false)).await;
}
#[tokio::test(start_paused = true)]
async fn limit_checks_reject_a_backend_that_accepts_oversized_messages() {
    let (left, right) = pair(1024);
    rejected(testkit::too_large(left, right, 8, |_| true)).await;
    let (left, right) = pair(1024);
    rejected(testkit::too_large_is_recoverable(left, right, 8, |_| true)).await;
}
#[tokio::test(start_paused = true)]
async fn recovery_rejects_corrupted_delivery_after_size_rejection() {
    let (left, right) = pair(8);
    rejected(testkit::too_large_is_recoverable(
        left,
        faulty(right, Fault::Corrupt),
        8,
        |error| matches!(error, SendError::TooLarge { max: 8 }),
    ))
    .await;
}

#[tokio::test]
#[should_panic(expected = "test limit must allow a larger payload")]
async fn size_check_rejects_an_unrepresentable_test_payload() {
    let (sender, receiver) = pair(8);
    testkit::too_large(sender, receiver, usize::MAX, |_| true).await;
}

#[tokio::test]
#[should_panic(expected = "test limit must allow a larger payload")]
async fn recovery_check_rejects_an_unrepresentable_test_payload() {
    let (sender, receiver) = pair(8);
    testkit::too_large_is_recoverable(sender, receiver, usize::MAX, |_| true).await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "helper did not reject the backend within its deadline")]
async fn rejection_requires_the_helper_to_stop_within_its_deadline() {
    rejected(std::future::pending()).await;
}
