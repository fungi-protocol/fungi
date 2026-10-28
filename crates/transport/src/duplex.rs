//! Adapter for combining independent channel directions.

use crate::{Bidirectional, RecvChannel, SendChannel};

/// A channel composed from independent sending and receiving capabilities.
///
/// The sending destination and receiving source may differ.
#[derive(Debug)]
pub struct Duplex<S, R> {
    sender: S,
    receiver: R,
}

impl<S, R> Duplex<S, R> {
    /// Combine sending and receiving capabilities.
    pub fn new(sender: S, receiver: R) -> Self {
        Self { sender, receiver }
    }

    /// Recover the sending and receiving capabilities.
    pub fn into_parts(self) -> (S, R) {
        (self.sender, self.receiver)
    }
}

impl<M, S: SendChannel<M>, R: Send> SendChannel<M> for Duplex<S, R> {
    type Privacy = S::Privacy;
    type SendError = S::SendError;

    fn send(&mut self, message: M) -> impl Future<Output = Result<(), Self::SendError>> + Send {
        self.sender.send(message)
    }
}

impl<M, S: Send, R: RecvChannel<M>> RecvChannel<M> for Duplex<S, R> {
    type RecvError = R::RecvError;

    fn recv(&mut self) -> impl Future<Output = Result<M, Self::RecvError>> + Send {
        self.receiver.recv()
    }
}

impl<I, O, S, R> Bidirectional<I, O> for Duplex<S, R>
where
    S: SendChannel<O>,
    R: RecvChannel<I>,
{
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::io;

    use crate::testing::unidirectional;
    use crate::{Bidirectional, ConnectionUnlinkability, Duplex, RecvChannel, SendChannel};

    fn assert_bidirectional<C: Bidirectional<u32, u32>>(_: &C) {}
    fn assert_asymmetric_bidirectional<C: Bidirectional<u32, String>>(_: &C) {}
    fn assert_connection_unlinkability<C: SendChannel<u32, Privacy = ConnectionUnlinkability>>(
        _: &C,
    ) {
    }

    #[tokio::test]
    async fn duplex_combines_sending_and_receiving() {
        let (sender, receiver) = unidirectional(1);
        let mut channel = Duplex::new(sender, receiver);

        assert_bidirectional(&channel);
        assert_connection_unlinkability(&channel);

        channel.send(42).await.unwrap();
        assert_eq!(channel.recv().await.unwrap(), 42);

        let (mut sender, mut receiver) = channel.into_parts();

        sender.send(43).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), 43);
        drop(sender);
        assert_eq!(
            receiver.recv().await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test]
    async fn supports_different_message_types_by_direction() {
        let (sender, receiver) = unidirectional(1);
        let mut channel = Duplex::new(sender, receiver);

        assert_asymmetric_bidirectional(&channel);
        SendChannel::<String>::send(&mut channel, "hello".to_owned())
            .await
            .unwrap();
        assert_eq!(channel.recv().await.unwrap(), 5);
    }

    #[tokio::test]
    async fn preserves_independent_direction_state() {
        let mut channel = Duplex::new(Sequential::default(), Sequential(Some(8)));
        assert_bidirectional(&channel);
        channel.send(7).await.unwrap();
        assert_eq!(channel.recv().await.unwrap(), 8);
        let (mut sender, _) = channel.into_parts();
        assert_eq!(sender.recv().await.unwrap(), 7);
    }

    #[derive(Debug, Default)]
    struct Sequential(Option<u32>);

    impl SendChannel<u32> for Sequential {
        type Privacy = ConnectionUnlinkability;
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

    impl Bidirectional<u32, u32> for Sequential {}

    #[tokio::test]
    async fn bidirectional_does_not_require_shared_access() {
        let mut channel = Sequential::default();

        assert_bidirectional(&channel);
        assert_connection_unlinkability(&channel);
        channel.send(7).await.unwrap();
        assert_eq!(channel.recv().await.unwrap(), 7);
        assert_eq!(
            channel.recv().await.unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
}
