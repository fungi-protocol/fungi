//! Simplex message channels through a sequence of BIP77 mailboxes.

use std::convert::Infallible;
use std::sync::Arc;

use fungi_mailboxes::{Reader, Writer};
use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel, Unspecified};
use url::Url;

use crate::{Bip77Directory, DirectoryClient, DirectoryError, DirectoryUrlError, parse_directory};

type ChannelError<C> = fungi_mailboxes::Error<DirectoryError<<C as DirectoryClient>::Error>>;

/// Build simplex channels keyed by a 32-byte secret through one directory.
#[derive(Debug)]
pub struct Builder<C> {
    client: Arc<C>,
    directory: Url,
}

/// Sending end of one simplex channel.
#[derive(Debug)]
pub struct Sender<C> {
    writer: Writer<Bip77Directory<Arc<C>>>,
}

/// Receiving end of one simplex channel.
#[derive(Debug)]
pub struct Receiver<C> {
    reader: Reader<Bip77Directory<Arc<C>>>,
}

impl<C: DirectoryClient> Builder<C> {
    /// Configure the client and directory base URL.
    pub fn new(client: C, directory: &str) -> Result<Self, DirectoryUrlError> {
        Ok(Self {
            client: Arc::new(client),
            directory: parse_directory(directory)?,
        })
    }

    /// Resume sending at a saved [`Sender::next_index`].
    /// See [`Writer::resume`] for recovery requirements.
    pub fn resume_sender(&self, secret: [u8; 32], next_index: u64) -> Sender<C> {
        Sender {
            writer: Writer::resume(self.directory(), secret, next_index),
        }
    }

    /// Construct the receiving end of the channel keyed by `secret`.
    pub fn receiver(&self, secret: [u8; 32]) -> Receiver<C> {
        self.resume_receiver(secret, 0)
    }

    /// Resume receiving at a saved [`Receiver::next_index`].
    pub fn resume_receiver(&self, secret: [u8; 32], next_index: u64) -> Receiver<C> {
        Receiver {
            reader: Reader::resume(self.directory(), secret, next_index),
        }
    }

    fn directory(&self) -> Bip77Directory<Arc<C>> {
        Bip77Directory::with_url(Arc::clone(&self.client), self.directory.clone())
    }
}

impl<C: DirectoryClient> ChannelBuilder for Builder<C> {
    type Input = [u8; 32];
    // A directory client alone does not establish this API's
    // destination-relative guarantees.
    type Privacy = Unspecified;
    type Channel = Sender<C>;
    type BuildError = Infallible;

    async fn build(&mut self, secret: &Self::Input) -> Result<Self::Channel, Infallible> {
        Ok(self.resume_sender(*secret, 0))
    }
}

impl<C: DirectoryClient> Sender<C> {
    /// Next send position. See [`Writer::resume`] for persistence requirements.
    pub fn next_index(&self) -> u64 {
        self.writer.next_index()
    }
}

impl<C: DirectoryClient> Receiver<C> {
    /// Index of the mailbox the next receive uses.
    pub fn next_index(&self) -> u64 {
        self.reader.next_index()
    }
}

impl<C: DirectoryClient> SendChannel for Sender<C> {
    type Privacy = Unspecified;
    type SendError = ChannelError<C>;

    async fn send(&mut self, message: Vec<u8>) -> Result<(), Self::SendError> {
        self.writer.write(&message).await
    }
}

impl<C: DirectoryClient> RecvChannel for Receiver<C> {
    type RecvError = ChannelError<C>;

    async fn recv(&mut self) -> Result<Vec<u8>, Self::RecvError> {
        self.reader.read().await
    }
}
