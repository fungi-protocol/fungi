//! BIP77 directory for fungi-mailboxes.
//!
//! [`Bip77Directory`] builds each mailbox request with a
//! [`mailbox_api::Client`] and sends it through a [`Relay`].

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::error::Error;
use std::future::Future;

use fungi_mailboxes::{Directory, MailboxId};
use fungi_ohttp::OhttpRequest;
use fungi_ohttp::mailbox_api::{self, Client, ShortId};

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod testing;
mod transport;

pub use transport::{Builder, Receiver, Sender};

/// Sends an encapsulated request to its OHTTP relay.
///
/// A failed request must not be resent: repeating its body links the two
/// attempts. Retrying through [`Bip77Directory`] encapsulates the request
/// again.
pub trait Relay: Send + Sync {
    /// Failure to send the request or receive the response.
    type Error: Error + Send + Sync + 'static;

    /// Send `request` and return the encapsulated response body.
    fn post(
        &self,
        request: OhttpRequest,
    ) -> impl Future<Output = Result<Vec<u8>, Self::Error>> + Send;
}

impl<R: Relay> Relay for std::sync::Arc<R> {
    type Error = R::Error;

    async fn post(&self, request: OhttpRequest) -> Result<Vec<u8>, Self::Error> {
        (**self).post(request).await
    }
}

/// Failure to use a BIP77 directory as a [`Directory`].
#[derive(Debug, thiserror::Error)]
pub enum DirectoryError<E: Error + 'static> {
    /// Building the request or decoding the response failed.
    #[error("mailbox API: {0}")]
    Api(#[from] mailbox_api::Error),
    /// The relay failed.
    #[error("relay: {0}")]
    Relay(#[source] E),
}

/// A BIP77 directory exposed through the [`Directory`] contract.
///
/// The directory keeps the first payload POSTed to a mailbox and accepts
/// later POSTs to it, so [`put`] succeeds on an occupied mailbox, as
/// [`Directory`] allows.
///
/// [`put`]: Directory::put
#[derive(Debug)]
pub struct Bip77Directory<R> {
    client: Client,
    relay: R,
}

impl<R> Bip77Directory<R> {
    pub fn new(client: Client, relay: R) -> Self {
        Self { client, relay }
    }
}

/// The BIP77 mailbox identifier of `mailbox`: the first 8 bytes of its ID.
fn short_id(mailbox: MailboxId) -> ShortId {
    let mut id = [0; 8];
    id.copy_from_slice(&mailbox.to_bytes()[..8]);
    ShortId(id)
}

impl<R: Relay> Directory for Bip77Directory<R> {
    type Error = DirectoryError<R::Error>;

    async fn put(&self, mailbox: MailboxId, message: &[u8]) -> Result<(), Self::Error> {
        let (request, context) = self.client.post(short_id(mailbox), message)?;
        let response = self
            .relay
            .post(request)
            .await
            .map_err(DirectoryError::Relay)?;
        Ok(context.finish(&response)?)
    }

    async fn get(&self, mailbox: MailboxId) -> Result<Option<Vec<u8>>, Self::Error> {
        let (request, context) = self.client.get(short_id(mailbox))?;
        let response = self
            .relay
            .post(request)
            .await
            .map_err(DirectoryError::Relay)?;
        Ok(context.finish(&response)?)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_mailboxes::{PAYLOAD_BYTES, Reader, Writer};

    use super::*;
    use crate::testing::{BrokenRelay, Loopback, sequence};

    #[test]
    fn short_id_is_the_start_of_the_mailbox_id() {
        let mailbox = sequence(1).mailbox(0).id();
        assert_eq!(short_id(mailbox).0, mailbox.to_bytes()[..8]);
    }

    #[tokio::test]
    async fn messages_pass_through_the_directory() {
        let directory = Loopback::default().directory();
        let mut writer = Writer::new(&directory, sequence(5));
        let mut reader = Reader::new(&directory, sequence(5));

        writer.write(b"first").await.unwrap();
        writer.write(b"second").await.unwrap();
        assert_eq!(reader.read().await.unwrap(), b"first");
        assert_eq!(reader.read().await.unwrap(), b"second");

        let mailboxes = directory.relay.mailboxes();
        assert_eq!(mailboxes.len(), 2);
        for index in 0..2 {
            let path = format!("/{}", short_id(sequence(5).mailbox(index).id()));
            assert_eq!(mailboxes[&path].len(), PAYLOAD_BYTES, "{path}");
        }
    }

    #[tokio::test]
    async fn requests_are_never_repeated() {
        let directory = Loopback::default().directory();
        let mailbox = sequence(6).mailbox(0).id();
        directory.get(mailbox).await.unwrap();
        directory.get(mailbox).await.unwrap();

        let requests = directory.relay.requests();
        assert_eq!(requests.len(), 2);
        assert_ne!(requests[0], requests[1]);
    }

    #[tokio::test]
    async fn first_payload_wins_and_later_posts_are_accepted() {
        let directory = Loopback::default().directory();
        let mailbox = sequence(2).mailbox(0).id();

        for message in [b"first", b"other", b"first"] {
            directory.put(mailbox, message).await.unwrap();
        }
        assert_eq!(
            directory.get(mailbox).await.unwrap(),
            Some(b"first".to_vec())
        );
    }

    #[tokio::test]
    async fn empty_mailbox_returns_none() {
        let directory = Loopback::default().directory();
        assert_eq!(
            directory.get(sequence(3).mailbox(0).id()).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn writer_at_a_stale_index_loses_its_message_without_an_error() {
        let directory = Loopback::default().directory();
        let mut writer = Writer::new(&directory, sequence(4));
        writer.write(b"first").await.unwrap();

        let mut stale = Writer::resume(&directory, sequence(4), 0);
        stale.write(b"lost").await.unwrap();
        assert_eq!(stale.next_index(), 1);

        let mut reader = Reader::new(&directory, sequence(4));
        assert_eq!(reader.read().await.unwrap(), b"first");
    }

    #[tokio::test]
    async fn directory_failures_reach_mailbox_callers() {
        let directory = Loopback::answering(500).directory();
        assert!(matches!(
            Writer::new(&directory, sequence(21)).write(&[1]).await,
            Err(fungi_mailboxes::Error::Directory(DirectoryError::Api(
                mailbox_api::Error::UnexpectedStatus(500)
            )))
        ));
        assert!(matches!(
            Reader::new(&directory, sequence(21)).read().await,
            Err(fungi_mailboxes::Error::Directory(DirectoryError::Api(
                mailbox_api::Error::UnexpectedStatus(500)
            )))
        ));
    }

    #[tokio::test]
    async fn relay_failures_reach_mailbox_callers() {
        let directory = Bip77Directory::new(Loopback::default().client(), BrokenRelay);
        assert!(matches!(
            Writer::new(&directory, sequence(22)).write(&[1]).await,
            Err(fungi_mailboxes::Error::Directory(DirectoryError::Relay(_)))
        ));
        assert!(matches!(
            Reader::new(&directory, sequence(22)).read().await,
            Err(fungi_mailboxes::Error::Directory(DirectoryError::Relay(_)))
        ));
    }
}
