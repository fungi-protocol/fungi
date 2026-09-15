//! Exercise conformance helpers independently of a public backend.

use fungi_transport::{Anonymous, Channel, ChannelBuilder, PeerChannel, RecvChannel, SendChannel};
use std::io;

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

use fungi_transport_testkit::testkit;
use tokio::sync::mpsc;

#[derive(Debug)]
struct Sender {
    queue: mpsc::Sender<Vec<u8>>,
    max: usize,
}

#[derive(Debug)]
struct Receiver(mpsc::Receiver<Vec<u8>>);

type Fixture = Channel<Sender, Receiver>;

fn pair(max: usize) -> (Fixture, Fixture) {
    let (left, incoming_right) = mpsc::channel(1);
    let (right, incoming_left) = mpsc::channel(1);
    (
        Channel::new(Sender { queue: left, max }, Receiver(incoming_left)).unwrap(),
        Channel::new(Sender { queue: right, max }, Receiver(incoming_right)).unwrap(),
    )
}

impl SendChannel for Sender {
    type Privacy = Anonymous;
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

struct Connector(mpsc::Sender<Fixture>);
struct Listener(mpsc::Receiver<Fixture>);

impl ChannelBuilder for Connector {
    type Input = ();
    type Channel = Fixture;
    type BuildError = io::Error;

    async fn build(&mut self, _: &()) -> Result<Fixture, io::Error> {
        let (client, server) = pair(1024);
        self.0
            .send(server)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "listener closed"))?;
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
            .ok_or(io::Error::new(io::ErrorKind::BrokenPipe, "listener closed"))
    }
}

#[tokio::test(start_paused = true)]
async fn helpers_check_delivery_limits_cancellation_and_reconnection() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (left, right) = pair(1024);
        testkit::roundtrip_both_directions(left, right).await;

        let (left, right) = pair(1024);
        testkit::closed_after_peer_drop(left, right, |_| true).await;

        let (left, right) = pair(1024);
        testkit::recv_is_cancel_safe(left, right).await;

        let (left, _) = pair(8);
        testkit::too_large(left, 8, |error| {
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
impl PeerChannel for FaultyReceiver {
    type Peer = ();
    fn peer(&self) -> &() {
        self.inner.peer()
    }
}
impl RecvChannel for FaultyReceiver {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        if matches!(self.fault, Fault::Reorder) {
            if let Some(message) = self.saved.take() {
                return Ok(message);
            }
            self.saved = Some(self.inner.recv().await?);
            return self.inner.recv().await;
        }
        let mut message = self.inner.recv().await?;
        match self.fault {
            Fault::Corrupt => message.push(0xff),
            Fault::Duplicate => {
                if let Some(previous) = &self.saved {
                    return Ok(previous.clone());
                }
                self.saved = Some(message.clone());
            }
            Fault::Lose => std::future::pending::<()>().await,
            Fault::Reorder => unreachable!(),
        }
        Ok(message)
    }
}
fn faulty(channel: Fixture, fault: Fault) -> Channel<Sender, FaultyReceiver> {
    let (sender, receiver) = channel.into_split();
    Channel::new(
        sender,
        FaultyReceiver {
            inner: receiver,
            fault,
            saved: None,
        },
    )
    .unwrap()
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
        300,
    )
    .await;
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
async fn cancellation_rejects_a_backend_that_consumes_then_suspends() {
    let (left, right) = pair(1024);
    rejected(testkit::recv_is_cancel_safe(
        left,
        faulty(right, Fault::Lose),
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
    let (left, _right) = pair(1024);
    rejected(testkit::too_large(left, 8, |_| true)).await;
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
struct FailingConnector;
impl ChannelBuilder for FailingConnector {
    type Input = ();
    type Channel = Fixture;
    type BuildError = io::Error;
    async fn build(&mut self, _: &()) -> Result<Fixture, io::Error> {
        Err(io::Error::other("injected construction failure"))
    }
}
#[tokio::test(start_paused = true)]
async fn construction_checks_reject_a_failed_connection() {
    let (queue, receiver) = mpsc::channel(1);
    let (_, server) = pair(1024);
    queue.send(server).await.unwrap();
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
        std::future::pending().await
    }
}
#[tokio::test(start_paused = true)]
async fn cancellation_accepts_a_backend_that_retains_a_partly_received_message() {
    let (sender, receiver) = pair(1024);
    let (_, receiver) = receiver.into_split();
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
    type Input = ();
    type Channel = Fixture;
    type BuildError = io::Error;
    async fn build(&mut self, input: &()) -> Result<Fixture, io::Error> {
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
