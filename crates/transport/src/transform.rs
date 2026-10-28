//! Type-directed channel transformations.

use std::future::Future;
use std::marker::PhantomData;

use crate::{RecvChannel, SendChannel};

/// A sending channel that applies a function to each message before sending it.
#[derive(Debug)]
pub struct MapBeforeSend<C, F, W> {
    channel: C,
    transform: F,
    message: PhantomData<fn(W)>,
}

impl<C, F, W> MapBeforeSend<C, F, W> {
    /// Transform messages accepted by a sending channel.
    pub fn new(channel: C, transform: F) -> Self {
        Self {
            channel,
            transform,
            message: PhantomData,
        }
    }
}

impl<T, W, C: SendChannel<W>, F: FnMut(T) -> W + Send> SendChannel<T> for MapBeforeSend<C, F, W> {
    type Privacy = C::Privacy;
    type SendError = C::SendError;

    fn send(&mut self, message: T) -> impl Future<Output = Result<(), Self::SendError>> + Send {
        let message = (self.transform)(message);
        self.channel.send(message)
    }
}

/// A receiving channel that applies a function to each message after receiving it.
#[derive(Debug)]
pub struct MapAfterRecv<C, F, W> {
    channel: C,
    transform: F,
    message: PhantomData<fn() -> W>,
}

impl<C, F, W> MapAfterRecv<C, F, W> {
    /// Transform messages produced by a receiving channel.
    pub fn new(channel: C, transform: F) -> Self {
        Self {
            channel,
            transform,
            message: PhantomData,
        }
    }
}

impl<T, W, C: RecvChannel<W>, F: FnMut(W) -> T + Send> RecvChannel<T> for MapAfterRecv<C, F, W> {
    type RecvError = C::RecvError;

    async fn recv(&mut self) -> Result<T, Self::RecvError> {
        let message = self.channel.recv().await?;
        Ok((self.transform)(message))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::mpsc;

    use crate::testing::{Sender, unidirectional};
    use crate::{
        ConnectionUnlinkability, MapAfterRecv, MapBeforeSend, RecvChannel, SendChannel, Unspecified,
    };

    #[test]
    fn adapters_keep_the_sender_privacy() {
        fn privacy<C: SendChannel<M, Privacy = P>, M, P>() {}

        privacy::<MapBeforeSend<Sender, fn(String) -> u32, u32>, String, ConnectionUnlinkability>();
    }

    #[tokio::test]
    async fn transformations_adapt_each_channel_direction() {
        let (sender, receiver) = unidirectional(1);
        let mut sender = MapBeforeSend::new(sender, |message: String| message.len() as u32);
        let mut receiver = MapAfterRecv::new(receiver, |message: u32| message.to_string());

        sender.send("hello".to_owned()).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), "5");
    }

    #[tokio::test]
    async fn map_after_recv_accepts_a_stateful_transformation() {
        let (mut sender, receiver) = unidirectional(1);
        let mut count = 0;
        let mut receiver = MapAfterRecv::new(receiver, move |message| {
            count += 1;
            (count, message)
        });

        sender.send(7).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), (1, 7));
        sender.send(7).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), (2, 7));
    }

    #[tokio::test]
    async fn transformations_preserve_transport_errors() {
        let (sender, receiver) = unidirectional(1);
        drop(receiver);
        let mut sender = MapBeforeSend::new(sender, |message: String| message.len() as u32);
        let mpsc::error::SendError(unsent) = sender.send("closed".to_owned()).await.unwrap_err();
        assert_eq!(unsent, 6);

        let (sender, receiver) = unidirectional(1);
        drop(sender);
        let mut receiver = MapAfterRecv::new(receiver, |message: u32| message.to_string());
        assert_eq!(
            receiver.recv().await.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test]
    async fn map_before_send_accepts_non_send_intermediate_messages() {
        struct Sink(Arc<AtomicUsize>);

        impl SendChannel<std::rc::Rc<usize>> for Sink {
            type Privacy = Unspecified;
            type SendError = Infallible;

            fn send(
                &mut self,
                message: std::rc::Rc<usize>,
            ) -> impl Future<Output = Result<(), Self::SendError>> + Send {
                self.0.store(*message, Ordering::SeqCst);
                std::future::ready(Ok(()))
            }
        }

        fn require_send<F: Future + Send>(future: F) -> F {
            future
        }

        let received = Arc::new(AtomicUsize::new(0));
        let mut sender = MapBeforeSend::new(Sink(Arc::clone(&received)), std::rc::Rc::new);
        require_send(sender.send(42)).await.unwrap();
        assert_eq!(received.load(Ordering::SeqCst), 42);
    }

    // The `Rc` is consumed before the send future is created, so the future stays `Send`.
    #[tokio::test]
    async fn map_before_send_accepts_non_send_messages() {
        fn require_send<F: Future + Send>(future: F) -> F {
            future
        }

        let (sender, mut receiver) = unidirectional(1);
        let mut sender = MapBeforeSend::new(sender, |message: std::rc::Rc<u32>| *message);
        require_send(sender.send(std::rc::Rc::new(42)))
            .await
            .unwrap();
        assert_eq!(receiver.recv().await.unwrap(), 42);
    }
}
