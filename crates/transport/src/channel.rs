//! Independent sending and receiving capabilities for one peer.

use std::error::Error;
use std::future::Future;
use std::sync::Arc;

use futures_core::Stream;

use crate::Privacy;

/// Send individual owned messages to one destination peer.
///
/// `Ok(())` confirms transport acceptance. Destination receipt depends on the
/// backend protocol. Cancellation leaves delivery unknown. Implementations must
/// preserve framing across cancellation or invalidate the shared session so
/// subsequent operations fail; this applies to every view of that session.
///
/// Each sending capability permits one operation at a time. Independently
/// owned directions obtained through [`Channel`] must make independent progress.
/// Delivery and protocol liveness depend on backend guarantees and driver policy.
/// Errors and recovery requirements are defined by the backend.
pub trait SendChannel<M = Vec<u8>>: Send {
    /// Privacy established for this sender relative to its destination peer.
    type Privacy: Privacy;
    /// Failure to send a message.
    type SendError: Error + Send + Sync + 'static;
    /// Submit one message.
    fn send(&mut self, message: M) -> impl Future<Output = Result<(), Self::SendError>> + Send;
}

/// Receive individual owned messages from one peer.
///
/// Receiving must be cancel-safe: messages remain available after a pending
/// receive is dropped. Each receiving capability permits one operation at a time.
/// A silent path failure may leave reception pending forever; consumers own
/// liveness timeouts.
///
/// Messages must own their data. Errors and recovery requirements are defined
/// by the backend, including resource shutdown and reconnection behavior.
pub trait RecvChannel<M = Vec<u8>>: Send {
    /// Failure to receive a message.
    type RecvError: Error + Send + Sync + 'static;
    /// Receive one message.
    fn recv(&mut self) -> impl Future<Output = Result<M, Self::RecvError>> + Send;
}

/// Optional backend interface for independent operations through shared access.
///
/// Implement this interface when the backend can keep send and receive progress
/// independent. Sequential backends only need [`SendChannel`] and [`RecvChannel`].
/// [`Channel::from_shared`] supplies exclusive handles and owned splitting, so
/// shared backends can reuse these capabilities through the adapter.
///
/// The cancellation, message ownership, privacy, and error contracts of the basic
/// capabilities apply here. Callers must keep at most one operation pending per
/// direction. Each direction must progress independently while the other waits.
/// Internal synchronization must release locks shared by both directions before
/// awaiting a message or capacity, preserving that independence. Protocol liveness
/// under network delay or adversarial behavior remains the driver's responsibility.
pub trait SharedChannel<M = Vec<u8>>: Send + Sync {
    /// Privacy established for the sender relative to its destination peer.
    type Privacy: Privacy;
    /// Backend-specific submission failure.
    type SendError: Error + Send + Sync + 'static;
    /// Backend-specific reception failure.
    type RecvError: Error + Send + Sync + 'static;
    /// Submit one message through shared access.
    fn send_shared(&self, message: M) -> impl Future<Output = Result<(), Self::SendError>> + Send;
    /// Receive one owned message through shared access.
    fn recv_shared(&self) -> impl Future<Output = Result<M, Self::RecvError>> + Send;
}

/// Exclusive sending handle sharing ownership of an independent backend.
///
/// Unique ownership and access restricted to sending enforce one pending send.
#[derive(Debug)]
pub struct SharedSendHalf<C>(Arc<C>);

/// Exclusive receiving handle sharing ownership of an independent backend.
///
/// Unique ownership and access restricted to receiving enforce one pending receive.
#[derive(Debug)]
pub struct SharedRecvHalf<C>(Arc<C>);

impl<M, C: SharedChannel<M>> SendChannel<M> for SharedSendHalf<C> {
    type Privacy = C::Privacy;
    type SendError = C::SendError;

    fn send(&mut self, message: M) -> impl Future<Output = Result<(), Self::SendError>> + Send {
        self.0.send_shared(message)
    }
}

impl<M, C: SharedChannel<M>> RecvChannel<M> for SharedRecvHalf<C> {
    type RecvError = C::RecvError;

    fn recv(&mut self) -> impl Future<Output = Result<M, Self::RecvError>> + Send {
        self.0.recv_shared()
    }
}

impl<C: PeerChannel> PeerChannel for SharedSendHalf<C> {
    type Peer = C::Peer;
    fn peer(&self) -> &Self::Peer {
        self.0.peer()
    }
}

impl<C: PeerChannel> PeerChannel for SharedRecvHalf<C> {
    type Peer = C::Peer;
    fn peer(&self) -> &Self::Peer {
        self.0.peer()
    }
}

/// Build a backend-specific capability, including simplex or duplex channels.
///
/// Backends must document whether construction and submission require the peer
/// online, which intermediate services must be reachable, and whether storage
/// allows an offline destination. Connection-backed resources must document
/// when they expire and how reconnection establishes privacy again. This
/// availability requirement is independent of Rust's `Sync` and backpressure.
pub trait ChannelBuilder: Send {
    /// Construction input, such as a destination address.
    type Input: Send + Sync;
    /// Produced capability, with privacy determined by the construction mode.
    type Channel: Send;
    /// Backend-specific construction failure.
    type BuildError: Error + Send + Sync + 'static;
    /// Construct the next capability.
    fn build(
        &mut self,
        input: &Self::Input,
    ) -> impl Future<Output = Result<Self::Channel, Self::BuildError>> + Send;
}

/// Consume a duplex channel and return its independent directions.
///
/// Splitting preserves the sender's established privacy guarantee:
///
/// ```compile_fail
/// use fungi_transport::{Anonymous, Channel, SendChannel, split};
/// fn anonymous<C: SendChannel<u32, Privacy = Anonymous>>(_: C) {}
/// fn elevate<S: SendChannel<u32>, R>(channel: Channel<S, R>) {
///     let (sender, _) = split(channel);
///     anonymous(sender);
/// }
/// ```
pub fn split<S, R>(channel: Channel<S, R>) -> (S, R) {
    channel.into_split()
}

/// Local destination metadata used to verify composition of capabilities.
///
/// Destination identity supports local peer matching; sender attribution belongs
/// to the transport privacy contract.
/// Implement it only when peer matching is needed, for example by a pair adapter.
/// Equal values must identify the same peer within the implementation's scope.
/// The destination must remain stable for the lifetime of the capability.
pub trait PeerChannel {
    /// Backend-specific destination identity.
    type Peer: Eq;
    /// The destination associated with this capability.
    fn peer(&self) -> &Self::Peer;
}

/// Peer mismatch detected while pairing capabilities.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("channel peers differ")]
pub struct PeerMismatch;

/// Duplex adapter owning two independent directions for the same peer.
///
/// The adapter supplies owned splitting so backends can focus on message capabilities.
/// [`Self::new`] pairs native simplex resources; [`Self::from_shared`] wraps a
/// native shared-access duplex resource. Every adapter can be consumed into its
/// directions. Exclusive operations on the whole adapter are sequential; use
/// [`Self::directions`] or [`Self::into_split`] for concurrent operations.
/// Delivery and protocol liveness depend on backend guarantees and driver policy.
///
/// Owned splitting requires the adapter to represent independent directions:
///
/// ```compile_fail
/// use fungi_transport::{RecvChannel, SendChannel, split};
/// fn separate<C: SendChannel<u32> + RecvChannel<u32>>(channel: C) {
///     let _ = split(channel);
/// }
/// ```
///
/// Borrowing the whole adapter twice still requires independent directions:
///
/// ```compile_fail
/// use fungi_transport::{Channel, SendChannel, RecvChannel};
/// async fn concurrent<S: SendChannel<u32>, R: RecvChannel<u32>>(channel: &mut Channel<S, R>) {
///     let send = channel.send(1);
///     let receive = channel.recv();
///     send.await.unwrap();
///     receive.await.unwrap();
/// }
/// ```
///
/// Construction must establish that both capabilities concern the same peer.
/// It must also account for session authentication in either direction when
/// assigning the sender's privacy guarantee. The supplied directions must allow
/// independent progress when used concurrently. Comparing destination values
/// establishes peer identity even when the capabilities share Rust types.
///
/// Pairing preserves the sender's established privacy guarantee:
///
/// ```compile_fail
/// use fungi_transport::{Anonymous, Channel, PeerChannel, RecvChannel, SendChannel};
/// fn anonymous<C: SendChannel<u32, Privacy = Anonymous>>(_: C) {}
/// fn elevate<S, R>(sender: S, receiver: R)
/// where
///     S: SendChannel<u32> + PeerChannel,
///     R: RecvChannel<u32> + PeerChannel<Peer = S::Peer>,
/// {
///     anonymous(Channel::new(sender, receiver).unwrap());
/// }
/// ```
#[derive(Debug)]
pub struct Channel<S, R> {
    sender: S,
    receiver: R,
}

/// Exclusive sending view preserving the paired capability's identity.
#[derive(Debug)]
pub struct SendView<'a, S>(&'a mut S);

/// Exclusive receiving view preserving the paired capability's identity.
#[derive(Debug)]
pub struct RecvView<'a, R>(&'a mut R);

impl<M, S: SendChannel<M>> SendChannel<M> for SendView<'_, S> {
    type Privacy = S::Privacy;
    type SendError = S::SendError;

    fn send(&mut self, message: M) -> impl Future<Output = Result<(), Self::SendError>> + Send {
        self.0.send(message)
    }
}

impl<M, R: RecvChannel<M>> RecvChannel<M> for RecvView<'_, R> {
    type RecvError = R::RecvError;

    fn recv(&mut self) -> impl Future<Output = Result<M, Self::RecvError>> + Send {
        self.0.recv()
    }
}

/// Duplex directions converted through one shared immutable codec.
pub type ConvertedChannel<S, R, K, W = Vec<u8>> =
    Channel<crate::CodecChannel<S, Arc<K>, W>, crate::CodecChannel<R, Arc<K>, W>>;

impl<S: PeerChannel, R: PeerChannel<Peer = S::Peer>> Channel<S, R> {
    /// Bundle capabilities after verifying they refer to the same peer.
    ///
    /// Construction requires operational capabilities as well as destination metadata:
    ///
    /// ```compile_fail
    /// use fungi_transport::{Channel, PeerChannel};
    /// struct Destination(u8);
    /// impl PeerChannel for Destination {
    ///     type Peer = u8;
    ///     fn peer(&self) -> &u8 { &self.0 }
    /// }
    /// let _ = Channel::new(Destination(1), Destination(1));
    /// ```
    pub fn new<M>(sender: S, receiver: R) -> Result<Self, PeerMismatch>
    where
        S: SendChannel<M>,
        R: RecvChannel<M>,
    {
        if sender.peer() != receiver.peer() {
            return Err(PeerMismatch);
        }
        Ok(Self { sender, receiver })
    }
}

impl<S, R> Channel<S, R> {
    /// Consume the adapter and return its independently owned directions.
    pub fn into_split(self) -> (S, R) {
        (self.sender, self.receiver)
    }

    /// Convert both directions while preserving privacy, errors, and ownership.
    pub fn with_codec<M: Send, W: Send, K: crate::Codec<M, W>>(
        self,
        codec: K,
    ) -> ConvertedChannel<S, R, K, W>
    where
        S: SendChannel<W>,
        R: RecvChannel<W>,
    {
        let codec = Arc::new(codec);
        Channel {
            sender: crate::CodecChannel::new(self.sender, Arc::clone(&codec)),
            receiver: crate::CodecChannel::new(self.receiver, codec),
        }
    }

    /// Borrow disjoint directions, allowing simultaneous operations on the pair.
    ///
    /// Directional views preserve the original pairing by restricting access to operations:
    ///
    /// ```compile_fail
    /// use fungi_transport::Channel;
    /// fn replace<S, R>(channel: &mut Channel<S, R>, replacement: S) {
    ///     let (mut sender, _) = channel.directions();
    ///     *sender = replacement;
    /// }
    /// ```
    pub fn directions(&mut self) -> (SendView<'_, S>, RecvView<'_, R>) {
        (SendView(&mut self.sender), RecvView(&mut self.receiver))
    }
}

impl<C> Channel<SharedSendHalf<C>, SharedRecvHalf<C>> {
    /// Share an independent duplex backend between exclusive directional handles.
    ///
    /// Synchronization and privacy guarantees come from the backend. Shared
    /// ownership keeps it alive until both handles are dropped; shutdown remains
    /// backend-specific.
    ///
    /// Shared adaptation preserves the backend's established privacy guarantee:
    ///
    /// ```compile_fail
    /// use fungi_transport::{Anonymous, Channel, SendChannel, SharedChannel};
    /// fn anonymous<C: SendChannel<u32, Privacy = Anonymous>>(_: C) {}
    /// fn elevate<C: SharedChannel<u32>>(channel: C) {
    ///     let (sender, _) = Channel::from_shared(channel).into_split();
    ///     anonymous(sender);
    /// }
    /// ```
    ///
    /// Shared adaptation requires the explicit independent-operation contract:
    ///
    /// ```compile_fail
    /// use fungi_transport::{Channel, RecvChannel, SendChannel};
    /// fn independent<C: SendChannel<u32> + RecvChannel<u32>>(channel: C) {
    ///     let _ = Channel::from_shared(channel);
    /// }
    /// ```
    pub fn from_shared<M>(channel: C) -> Self
    where
        C: SharedChannel<M>,
    {
        let channel = Arc::new(channel);
        Self {
            sender: SharedSendHalf(Arc::clone(&channel)),
            receiver: SharedRecvHalf(channel),
        }
    }
}

impl<S: PeerChannel, R> PeerChannel for Channel<S, R> {
    type Peer = S::Peer;
    fn peer(&self) -> &Self::Peer {
        self.sender.peer()
    }
}

impl<M, S: SendChannel<M>, R: Send> SendChannel<M> for Channel<S, R> {
    type Privacy = S::Privacy;
    type SendError = S::SendError;

    fn send(&mut self, message: M) -> impl Future<Output = Result<(), Self::SendError>> + Send {
        self.sender.send(message)
    }
}

impl<M, S: Send, R: RecvChannel<M>> RecvChannel<M> for Channel<S, R> {
    type RecvError = R::RecvError;

    fn recv(&mut self) -> impl Future<Output = Result<M, Self::RecvError>> + Send {
        self.receiver.recv()
    }
}

/// Adapt reception into a stream yielding its first error before terminating.
///
/// The adapter treats the first error as terminal to give consumers a stopping point.
/// Backend recovery remains backend-specific. Dropping a polled stream releases
/// its owned receiver.
pub fn into_stream<M: Send, C: RecvChannel<M>>(
    channel: C,
) -> impl Stream<Item = Result<M, C::RecvError>> + Send {
    futures_util::stream::unfold((channel, false), |(mut channel, done)| async move {
        if done {
            return None;
        }
        let item = channel.recv().await;
        let done = item.is_err();
        Some((item, (channel, done)))
    })
}
