//! Simplex message channels backed by a BIP77 linked mailbox.

use std::convert::Infallible;
use std::sync::Arc;

use fungi_mailbox::{Reader, Writer};
use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel, Unspecified};
use url::Url;

use crate::{Bip77MailboxStore, DirectoryExchange, DirectoryUrlError, StoreError, parse_directory};

type Store<X> = Bip77MailboxStore<Arc<X>>;
type ChannelError<X> = fungi_mailbox::Error<StoreError<<X as DirectoryExchange>::Error>>;

/// Build simplex channels keyed by a 32-byte secret through one directory.
#[derive(Debug)]
pub struct Builder<X> {
    exchange: Arc<X>,
    directory: Url,
}

/// Sending end of one simplex channel.
#[derive(Debug)]
pub struct Sender<X> {
    writer: Writer<Store<X>>,
}

/// Receiving end of one simplex channel.
#[derive(Debug)]
pub struct Receiver<X> {
    reader: Reader<Store<X>>,
}

impl<X: DirectoryExchange> Builder<X> {
    /// Configure the exchange and directory base URL.
    pub fn new(exchange: X, directory: &str) -> Result<Self, DirectoryUrlError> {
        Ok(Self {
            exchange: Arc::new(exchange),
            directory: parse_directory(directory)?,
        })
    }

    /// Resume sending at a saved [`Sender::next_index`].
    /// See [`Writer::resume`] for recovery requirements.
    pub fn resume_sender(&self, secret: [u8; 32], next_index: u64) -> Sender<X> {
        Sender {
            writer: Writer::resume(self.store(), secret, next_index),
        }
    }

    /// Construct the receiving end of the channel keyed by `secret`.
    pub fn receiver(&self, secret: [u8; 32]) -> Receiver<X> {
        self.resume_receiver(secret, 0)
    }

    /// Resume receiving at a saved [`Receiver::next_index`].
    pub fn resume_receiver(&self, secret: [u8; 32], next_index: u64) -> Receiver<X> {
        Receiver {
            reader: Reader::resume(self.store(), secret, next_index),
        }
    }

    fn store(&self) -> Store<X> {
        Bip77MailboxStore::with_url(Arc::clone(&self.exchange), self.directory.clone())
    }
}

impl<X: DirectoryExchange> ChannelBuilder for Builder<X> {
    type Input = [u8; 32];
    // A directory exchange alone does not establish this API's
    // destination-relative guarantees.
    type Privacy = Unspecified;
    type Channel = Sender<X>;
    type BuildError = Infallible;

    async fn build(&mut self, secret: &Self::Input) -> Result<Self::Channel, Infallible> {
        Ok(self.resume_sender(*secret, 0))
    }
}

impl<X: DirectoryExchange> Sender<X> {
    /// Next send position. See [`Writer::resume`] for persistence requirements.
    pub fn next_index(&self) -> u64 {
        self.writer.next_index()
    }
}

impl<X: DirectoryExchange> Receiver<X> {
    /// Index of the slot the next receive uses.
    pub fn next_index(&self) -> u64 {
        self.reader.next_index()
    }
}

impl<X: DirectoryExchange> SendChannel for Sender<X> {
    type Privacy = Unspecified;
    type SendError = ChannelError<X>;

    async fn send(&mut self, message: Vec<u8>) -> Result<(), Self::SendError> {
        self.writer.write(&message).await
    }
}

impl<X: DirectoryExchange> RecvChannel for Receiver<X> {
    type RecvError = ChannelError<X>;

    async fn recv(&mut self) -> Result<Vec<u8>, Self::RecvError> {
        self.reader.read().await
    }
}
