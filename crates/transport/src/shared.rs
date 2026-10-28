//! Adapter for splitting a shared channel backend.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::{RecvChannel, SendChannel, SharedChannel};

/// Sending capability backed by a shared channel.
pub struct SendHalf<C, I, O> {
    channel: Arc<C>,
    // `O` is sent and `I` is received; neither is stored.
    messages: PhantomData<fn(O) -> I>,
}

/// Receiving capability backed by a shared channel.
pub struct RecvHalf<C, I, O> {
    channel: Arc<C>,
    // `O` is sent and `I` is received; neither is stored.
    messages: PhantomData<fn(O) -> I>,
}

/// Split a shared channel into independently owned capabilities.
pub fn split<C, I, O>(channel: C) -> (SendHalf<C, I, O>, RecvHalf<C, I, O>)
where
    C: SharedChannel<I, O>,
{
    let channel = Arc::new(channel);
    (
        SendHalf {
            channel: Arc::clone(&channel),
            messages: PhantomData,
        },
        RecvHalf {
            channel,
            messages: PhantomData,
        },
    )
}

impl<C, I, O> SendChannel<O> for SendHalf<C, I, O>
where
    C: SharedChannel<I, O>,
{
    type Privacy = C::Privacy;
    type SendError = C::SendError;

    fn send(&mut self, message: O) -> impl Future<Output = Result<(), Self::SendError>> + Send {
        self.channel.send_shared(message)
    }
}

impl<C, I, O> RecvChannel<I> for RecvHalf<C, I, O>
where
    C: SharedChannel<I, O>,
{
    type RecvError = C::RecvError;

    fn recv(&mut self) -> impl Future<Output = Result<I, Self::RecvError>> + Send {
        self.channel.recv_shared()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    use tokio::sync::{Mutex, mpsc};

    use super::{SendHalf, split};
    use crate::{Bidirectional, ConnectionUnlinkability, RecvChannel, SendChannel, SharedChannel};

    /// Counts its drops. `send` and `recv` exist only because
    /// `SharedChannel` requires `Bidirectional`.
    #[derive(Debug)]
    struct Backend {
        sender: mpsc::Sender<u32>,
        receiver: Mutex<mpsc::Receiver<u32>>,
        drops: Arc<AtomicUsize>,
    }

    impl Backend {
        fn new() -> Self {
            let (sender, receiver) = mpsc::channel(1);
            Self {
                sender,
                receiver: Mutex::new(receiver),
                drops: Arc::default(),
            }
        }
    }

    impl Drop for Backend {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl SendChannel<String> for Backend {
        type Privacy = ConnectionUnlinkability;
        type SendError = mpsc::error::SendError<u32>;

        async fn send(&mut self, message: String) -> Result<(), Self::SendError> {
            self.send_shared(message).await
        }
    }

    impl RecvChannel<u32> for Backend {
        type RecvError = io::Error;

        async fn recv(&mut self) -> Result<u32, Self::RecvError> {
            self.recv_shared().await
        }
    }

    impl Bidirectional<u32, String> for Backend {}

    impl SharedChannel<u32, String> for Backend {
        async fn send_shared(&self, message: String) -> Result<(), Self::SendError> {
            self.sender.send(message.len() as u32).await
        }

        async fn recv_shared(&self) -> Result<u32, Self::RecvError> {
            self.receiver
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "receiver closed"))
        }
    }

    #[test]
    fn halves_keep_the_backend_privacy() {
        fn privacy<C: SendChannel<M, Privacy = P>, M, P>() {}

        privacy::<SendHalf<Backend, u32, String>, String, ConnectionUnlinkability>();
    }

    #[tokio::test]
    async fn sending_progresses_while_receiving_is_pending() {
        let (mut sender, mut receiver) = split(Backend::new());

        let receive = receiver.recv();
        let mut receive = std::pin::pin!(receive);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(receive.as_mut().poll(&mut context), Poll::Pending));

        sender.send("shared".to_owned()).await.unwrap();
        assert_eq!(receive.await.unwrap(), 6);
    }

    #[tokio::test]
    async fn receiving_progresses_while_sending_is_pending() {
        let (mut sender, mut receiver) = split(Backend::new());
        sender.send("a".to_owned()).await.unwrap();

        let send = sender.send("bb".to_owned());
        let mut send = std::pin::pin!(send);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(send.as_mut().poll(&mut context), Poll::Pending));

        assert_eq!(receiver.recv().await.unwrap(), 1);
        send.await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), 2);
    }

    #[test]
    fn split_keeps_the_backend_until_both_halves_drop() {
        let backend = Backend::new();
        let drops = Arc::clone(&backend.drops);
        let (sender, receiver) = split(backend);

        drop(sender);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(receiver);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
