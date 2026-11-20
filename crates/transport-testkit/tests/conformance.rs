#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

//! Exercise conformance helpers independently of a public backend.

use std::io;

use fungi_transport::{ChannelBuilder, ConnectionUnlinkability, Duplex, RecvChannel, SendChannel};
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

type BidirectionalChannel = Duplex<Sender, Receiver>;

fn pair(max: usize) -> (BidirectionalChannel, BidirectionalChannel) {
    let (left, incoming_right) = mpsc::channel(1);
    let (right, incoming_left) = mpsc::channel(1);
    (
        Duplex::new(Sender { queue: left, max }, Receiver(incoming_left)),
        Duplex::new(Sender { queue: right, max }, Receiver(incoming_right)),
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

struct Connector(mpsc::Sender<BidirectionalChannel>);
struct Listener(mpsc::Receiver<BidirectionalChannel>);

impl ChannelBuilder for Connector {
    type Privacy = ConnectionUnlinkability;
    type Input = ();
    type Channel = BidirectionalChannel;
    type BuildError = io::Error;

    async fn build(&mut self, _: &()) -> Result<BidirectionalChannel, io::Error> {
        let (client, server) = pair(1024);
        self.0
            .send(server)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "listener closed"))?;
        Ok(client)
    }
}

impl ChannelBuilder for Listener {
    type Privacy = ConnectionUnlinkability;
    type Input = ();
    type Channel = BidirectionalChannel;
    type BuildError = io::Error;

    async fn build(&mut self, _: &()) -> Result<BidirectionalChannel, io::Error> {
        self.0
            .recv()
            .await
            .ok_or(io::Error::new(io::ErrorKind::BrokenPipe, "listener closed"))
    }
}

#[tokio::test(start_paused = true)]
async fn helpers_check_delivery_limits_cancellation_and_reconnection() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (left, right) = pair(1024);
        testkit::roundtrip_both_directions(left, right).await;

        let (left, right) = pair(1024);
        testkit::recv_is_cancel_safe(left, right).await;

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

        let (left, right) = pair(1024);
        testkit::mutual_bursts_converge(left, right, 16).await;

        let (sender, receiver) = mpsc::channel(1);
        testkit::build_use_drop_rebuild(Connector(sender), Listener(receiver), &()).await;
    })
    .await
    .expect("conformance helpers must complete");
}

#[derive(Debug, Clone, Copy)]
enum Fault {
    Intact,
    Corrupt,
    Duplicate,
    Lose,
    Reorder,
}

#[derive(Debug)]
struct FaultyReceiver {
    inner: Receiver,
    fault: Fault,
    saved: Option<Vec<u8>>,
}

impl RecvChannel for FaultyReceiver {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        match self.fault {
            Fault::Intact => self.inner.recv().await,
            Fault::Corrupt => {
                let mut message = self.inner.recv().await?;
                message.push(0xff);
                Ok(message)
            }
            Fault::Duplicate => {
                let message = self.inner.recv().await?;
                if let Some(previous) = &self.saved {
                    return Ok(previous.clone());
                }
                self.saved = Some(message.clone());
                Ok(message)
            }
            Fault::Lose => {
                self.inner.recv().await?;
                std::future::pending().await
            }
            Fault::Reorder => {
                if let Some(message) = self.saved.take() {
                    return Ok(message);
                }
                let first = self.inner.recv().await?;
                match self.inner.recv().await {
                    Ok(second) => {
                        self.saved = Some(first);
                        Ok(second)
                    }
                    // Nothing follows the last message, so it goes out unswapped.
                    Err(_) => Ok(first),
                }
            }
        }
    }
}

fn faulty(channel: BidirectionalChannel, fault: Fault) -> Duplex<Sender, FaultyReceiver> {
    let (sender, receiver) = channel.into_parts();
    Duplex::new(
        sender,
        FaultyReceiver {
            inner: receiver,
            fault,
            saved: None,
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
async fn bursts_reject_corruption_duplicates_and_loss() {
    for fault in [Fault::Corrupt, Fault::Duplicate, Fault::Lose] {
        let (left, right) = pair(1024);
        rejected(testkit::mutual_bursts_converge(
            faulty(left, fault),
            faulty(right, fault),
            300,
        ))
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn bursts_accept_reordering_and_indices_beyond_one_byte() {
    let (left, right) = pair(1024);
    testkit::mutual_bursts_converge(
        faulty(left, Fault::Reorder),
        faulty(right, Fault::Reorder),
        301,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn roundtrip_rejects_corrupted_delivery() {
    let (left, right) = pair(1024);
    rejected(testkit::roundtrip_both_directions(
        faulty(left, Fault::Intact),
        faulty(right, Fault::Corrupt),
    ))
    .await;
}

#[tokio::test(start_paused = true)]
async fn roundtrip_rejects_corrupted_reply() {
    let (left, right) = pair(1024);
    rejected(testkit::roundtrip_both_directions(
        faulty(left, Fault::Corrupt),
        faulty(right, Fault::Intact),
    ))
    .await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_rejects_a_backend_that_consumes_then_suspends() {
    let (left, right) = pair(1024);
    rejected(testkit::recv_is_cancel_safe(
        left,
        faulty(right, Fault::Lose),
    ))
    .await;
}

#[derive(Debug)]
struct YieldingReceiver(Receiver);
impl RecvChannel for YieldingReceiver {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        let message = self.0.recv().await?;
        tokio::task::yield_now().await;
        Ok(message)
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_rejects_a_backend_that_consumes_then_yields() {
    let (left, right) = pair(1024);
    let (_, receiver) = right.into_parts();
    rejected(testkit::recv_is_cancel_safe(
        left,
        YieldingReceiver(receiver),
    ))
    .await;
}

/// Answers every receive at once, so no receive is ever left to cancel.
#[derive(Debug)]
struct NonblockingReceiver;

impl RecvChannel for NonblockingReceiver {
    type RecvError = RecvError;

    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        Err(RecvError)
    }
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "recv must wait while no message is available")]
async fn cancellation_requires_a_receiver_that_waits() {
    let (sender, _peer) = pair(1024);
    testkit::recv_is_cancel_safe(sender, NonblockingReceiver).await;
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

/// Rejects an oversized payload by closing its queue, so the peer sees the
/// channel close before the rejection arrives.
#[derive(Debug)]
struct ClosingSender(Option<mpsc::Sender<Vec<u8>>>);

impl SendChannel for ClosingSender {
    type Privacy = ConnectionUnlinkability;
    type SendError = SendError;

    async fn send(&mut self, _: Vec<u8>) -> Result<(), SendError> {
        self.0.take();
        tokio::task::yield_now().await;
        Err(SendError::TooLarge { max: 8 })
    }
}

/// Delivers an oversized payload and rejects it in the same poll.
#[derive(Debug)]
struct LeakingSender(mpsc::Sender<Vec<u8>>);

impl SendChannel for LeakingSender {
    type Privacy = ConnectionUnlinkability;
    type SendError = SendError;

    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        self.0.try_send(message).map_err(|_| SendError::Closed)?;
        Err(SendError::TooLarge { max: 8 })
    }
}

#[tokio::test(start_paused = true)]
async fn size_check_accepts_a_rejection_that_closes_the_channel() {
    let (queue, incoming) = mpsc::channel(1);
    testkit::too_large(ClosingSender(Some(queue)), Receiver(incoming), 8, |_| true).await;
}

#[tokio::test(start_paused = true)]
async fn size_check_rejects_a_payload_delivered_with_the_rejection() {
    let (queue, incoming) = mpsc::channel(1);
    rejected(testkit::too_large(
        LeakingSender(queue),
        Receiver(incoming),
        8,
        |_| true,
    ))
    .await;
}

#[tokio::test(start_paused = true)]
async fn recovery_rejects_a_channel_closed_by_the_rejection() {
    let (queue, incoming) = mpsc::channel(1);
    rejected(testkit::too_large_is_recoverable(
        ClosingSender(Some(queue)),
        Receiver(incoming),
        8,
        |_| true,
    ))
    .await;
}

struct FailingConnector;
impl ChannelBuilder for FailingConnector {
    type Privacy = ConnectionUnlinkability;
    type Input = ();
    type Channel = BidirectionalChannel;
    type BuildError = io::Error;
    async fn build(&mut self, _: &()) -> Result<BidirectionalChannel, io::Error> {
        Err(io::Error::other("injected construction failure"))
    }
}

#[tokio::test(start_paused = true)]
async fn construction_checks_reject_a_failed_connection() {
    let (_queue, receiver) = mpsc::channel(1);
    rejected(async move {
        testkit::build_use_drop_rebuild(FailingConnector, Listener(receiver), &()).await
    })
    .await;
}

#[derive(Debug)]
struct RetainingReceiver {
    inner: Receiver,
    saved: Option<Vec<u8>>,
}

impl RecvChannel for RetainingReceiver {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        if let Some(message) = self.saved.take() {
            return Ok(message);
        }
        self.saved = Some(self.inner.recv().await?);
        tokio::task::yield_now().await;
        Ok(self.saved.take().unwrap())
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_accepts_a_backend_that_retains_a_partly_received_message() {
    let (sender, receiver) = pair(1024);
    let (_, receiver) = receiver.into_parts();
    testkit::recv_is_cancel_safe(
        sender,
        RetainingReceiver {
            inner: receiver,
            saved: None,
        },
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn bursts_accept_empty_and_single_message_exchanges() {
    for burst in [0, 1] {
        let (left, right) = pair(1024);
        testkit::mutual_bursts_converge(left, right, burst).await;
    }
}

struct FailingReconnect {
    connector: Connector,
    connected: bool,
}

impl ChannelBuilder for FailingReconnect {
    type Privacy = ConnectionUnlinkability;
    type Input = ();
    type Channel = BidirectionalChannel;
    type BuildError = io::Error;
    async fn build(&mut self, input: &()) -> Result<BidirectionalChannel, io::Error> {
        let channel = self.connector.build(input).await?;
        if self.connected {
            Err(io::Error::other("injected reconnection failure"))
        } else {
            self.connected = true;
            Ok(channel)
        }
    }
}

#[tokio::test(start_paused = true)]
async fn construction_checks_reject_a_failed_reconnection() {
    let (sender, receiver) = mpsc::channel(1);
    rejected(async move {
        testkit::build_use_drop_rebuild(
            FailingReconnect {
                connector: Connector(sender),
                connected: false,
            },
            Listener(receiver),
            &(),
        )
        .await;
    })
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

#[tokio::test(start_paused = true)]
#[should_panic(expected = "helper accepted a defective backend")]
async fn rejection_requires_the_helper_to_panic() {
    rejected(async {}).await;
}

/// Gives the client a closed receiver but keeps the real one open, so the
/// server's sends succeed and only the client's reception fails.
struct BrokenReceiveConnector {
    connector: Connector,
    retained: Vec<Receiver>,
}

impl ChannelBuilder for BrokenReceiveConnector {
    type Privacy = ConnectionUnlinkability;
    type Input = ();
    type Channel = BidirectionalChannel;
    type BuildError = io::Error;
    async fn build(&mut self, input: &()) -> Result<BidirectionalChannel, io::Error> {
        let (sender, receiver) = self.connector.build(input).await?.into_parts();
        self.retained.push(receiver);
        let (_, closed) = mpsc::channel(1);
        Ok(Duplex::new(sender, Receiver(closed)))
    }
}

#[tokio::test(start_paused = true)]
async fn construction_checks_reject_a_client_that_cannot_receive() {
    let (sender, receiver) = mpsc::channel(1);
    rejected(async move {
        testkit::build_use_drop_rebuild(
            BrokenReceiveConnector {
                connector: Connector(sender),
                retained: Vec::new(),
            },
            Listener(receiver),
            &(),
        )
        .await;
    })
    .await;
}

/// Like [`BrokenReceiveConnector`], from the second connection on.
struct BrokenReceiveOnReconnect {
    connector: Connector,
    connected: bool,
    retained: Vec<Receiver>,
}

impl ChannelBuilder for BrokenReceiveOnReconnect {
    type Privacy = ConnectionUnlinkability;
    type Input = ();
    type Channel = BidirectionalChannel;
    type BuildError = io::Error;
    async fn build(&mut self, input: &()) -> Result<BidirectionalChannel, io::Error> {
        let channel = self.connector.build(input).await?;
        if !self.connected {
            self.connected = true;
            return Ok(channel);
        }
        let (sender, receiver) = channel.into_parts();
        self.retained.push(receiver);
        let (_, closed) = mpsc::channel(1);
        Ok(Duplex::new(sender, Receiver(closed)))
    }
}

#[tokio::test(start_paused = true)]
async fn construction_checks_reject_a_reconnected_client_that_cannot_receive() {
    let (sender, receiver) = mpsc::channel(1);
    rejected(async move {
        testkit::build_use_drop_rebuild(
            BrokenReceiveOnReconnect {
                connector: Connector(sender),
                connected: false,
                retained: Vec::new(),
            },
            Listener(receiver),
            &(),
        )
        .await;
    })
    .await;
}
