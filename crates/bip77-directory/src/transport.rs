//! Simplex message channels through a sequence of BIP77 mailboxes.

use std::convert::Infallible;
use std::sync::Arc;

use fungi_mailboxes::{Reader, Sequence, Writer};
use fungi_ohttp::mailbox_api::Client;
use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel, Unspecified};

use crate::{Bip77Directory, DirectoryError, Relay};

type ChannelError<R> = fungi_mailboxes::Error<DirectoryError<<R as Relay>::Error>>;

/// Build simplex channels through one directory, one for each [`Sequence`].
#[derive(Debug)]
pub struct Builder<R> {
    client: Client,
    relay: Arc<R>,
}

/// Sending end of one simplex channel.
#[derive(Debug)]
pub struct Sender<R> {
    writer: Writer<Bip77Directory<Arc<R>>>,
}

/// Receiving end of one simplex channel.
#[derive(Debug)]
pub struct Receiver<R> {
    reader: Reader<Bip77Directory<Arc<R>>>,
}

impl<R: Relay> Builder<R> {
    /// Send every request built by `client` through `relay`.
    pub fn new(client: Client, relay: R) -> Self {
        Self {
            client,
            relay: Arc::new(relay),
        }
    }

    /// Resume sending at a saved [`Sender::next_index`].
    /// See [`Writer::resume`] for recovery requirements.
    pub fn resume_sender(&self, sequence: Sequence, next_index: u64) -> Sender<R> {
        Sender {
            writer: Writer::resume(self.directory(), sequence, next_index),
        }
    }

    /// Construct the receiving end of the channel through `sequence`.
    pub fn receiver(&self, sequence: Sequence) -> Receiver<R> {
        self.resume_receiver(sequence, 0)
    }

    /// Resume receiving at a saved [`Receiver::next_index`].
    pub fn resume_receiver(&self, sequence: Sequence, next_index: u64) -> Receiver<R> {
        Receiver {
            reader: Reader::resume(self.directory(), sequence, next_index),
        }
    }

    fn directory(&self) -> Bip77Directory<Arc<R>> {
        Bip77Directory::new(self.client.clone(), Arc::clone(&self.relay))
    }
}

impl<R: Relay> ChannelBuilder for Builder<R> {
    type Input = Sequence;
    // A relay alone does not establish this API's destination-relative
    // guarantees.
    type Privacy = Unspecified;
    type Channel = Sender<R>;
    type BuildError = Infallible;

    async fn build(&mut self, sequence: &Self::Input) -> Result<Self::Channel, Infallible> {
        Ok(self.resume_sender(sequence.clone(), 0))
    }
}

impl<R: Relay> Sender<R> {
    /// Next send position. See [`Writer::resume`] for persistence requirements.
    pub fn next_index(&self) -> u64 {
        self.writer.next_index()
    }
}

impl<R: Relay> Receiver<R> {
    /// Index of the mailbox the next receive uses.
    pub fn next_index(&self) -> u64 {
        self.reader.next_index()
    }
}

impl<R: Relay> SendChannel for Sender<R> {
    type Privacy = Unspecified;
    type SendError = ChannelError<R>;

    async fn send(&mut self, message: Vec<u8>) -> Result<(), Self::SendError> {
        self.writer.write(&message).await
    }
}

impl<R: Relay> RecvChannel for Receiver<R> {
    type RecvError = ChannelError<R>;

    async fn recv(&mut self) -> Result<Vec<u8>, Self::RecvError> {
        self.reader.read().await
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::testing::{Loopback, sequence};

    fn builder() -> Builder<Loopback> {
        let loopback = Loopback::default();
        Builder::new(loopback.client(), loopback)
    }

    #[tokio::test(start_paused = true)]
    async fn empty_ohttp_polls_can_be_cancelled_without_skipping_messages() {
        use std::future::Future;
        use std::pin::pin;
        use std::task::{Context, Waker};

        let mut builder = builder();
        let mut sender = builder.build(&sequence(18)).await.unwrap();
        let mut receiver = builder.receiver(sequence(18));
        for message in [b"first".to_vec(), b"second".to_vec()] {
            // Poll through an encrypted 202, then cancel the pending receive.
            {
                let mut receive = pin!(receiver.recv());
                for _ in 0..2 {
                    assert!(
                        receive
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                }
            }
            let (sent, received) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(sender.send(message.clone()), receiver.recv())
            })
            .await
            .unwrap();
            sent.unwrap();
            assert_eq!(received.unwrap(), message);
        }
    }

    #[tokio::test]
    async fn channel_ends_resume_after_a_restart() {
        fn unspecified<C: SendChannel<Privacy = Unspecified>>(_: &C) {}
        let mut builder = builder();
        let mut sender = builder.build(&sequence(25)).await.unwrap();
        unspecified(&sender);
        let mut receiver = builder.receiver(sequence(25));
        sender.send(b"before".to_vec()).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), b"before");
        let (send_index, receive_index) = (sender.next_index(), receiver.next_index());

        let mut sender = builder.resume_sender(sequence(25), send_index);
        let mut receiver = builder.resume_receiver(sequence(25), receive_index);
        sender.send(b"after".to_vec()).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), b"after");
    }
}
