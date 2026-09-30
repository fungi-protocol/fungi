//! BIP77 Payjoin directory for fungi-mailboxes.
//!
//! Maps each mailbox to a BIP77 path using [`DirectoryClient`] for network
//! I/O.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::error::Error;
use std::fmt;
use std::future::Future;

use bech32::{Hrp, NoChecksum};
use fungi_mailboxes::{Directory, MailboxId};
use url::Url;

mod ohttp;
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod testing;

pub use ohttp::{ENCAPSULATED_MESSAGE_BYTES, OhttpClient, OhttpClientError, Relay, SingleRelay};

/// A BIP77 mailbox identifier encoded as 13 uppercase bech32 characters.
///
/// Uses the first 64 bits of a [`MailboxId`], rather than a Payjoin
/// public-key hash. The encoding omits the HRP, separator, and checksum.
pub(crate) struct ShortId([u8; 8]);

impl From<MailboxId> for ShortId {
    fn from(mailbox: MailboxId) -> Self {
        let mut id = [0; 8];
        id.copy_from_slice(&mailbox.to_bytes()[..8]);
        Self(id)
    }
}

impl fmt::Display for ShortId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hrp = Hrp::parse("ID").map_err(|_| fmt::Error)?;
        let encoded = bech32::encode_upper::<NoChecksum>(hrp, &self.0).map_err(|_| fmt::Error)?;
        formatter.write_str(encoded.strip_prefix("ID1").ok_or(fmt::Error)?)
    }
}

/// Inner HTTP method sent to the BIP77 target resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// Retrieve or long-poll one mailbox.
    Get,
    /// Store one mailbox payload.
    Post,
}

/// One inner request to a BIP77 directory target resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryRequest {
    /// Inner HTTP method.
    pub method: Method,
    /// Absolute target-resource URL.
    pub target: String,
    /// Inner request body.
    pub body: Vec<u8>,
}

/// One decapsulated response from a BIP77 directory target resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryResponse {
    /// Inner HTTP status code.
    pub status: u16,
    /// Inner response body.
    pub body: Vec<u8>,
}

/// Send one request to a directory and return its response.
///
/// Implementations must not expose request-correlated state to callers. An
/// OHTTP implementation must create fresh encapsulation for every call and
/// use that call's response context before returning.
pub trait DirectoryClient: Send + Sync {
    /// Failure to send the request or receive its response.
    type Error: Error + Send + Sync + 'static;

    /// Send `request` and return its response.
    fn send(
        &self,
        request: DirectoryRequest,
    ) -> impl Future<Output = Result<DirectoryResponse, Self::Error>> + Send;
}

impl<C: DirectoryClient> DirectoryClient for std::sync::Arc<C> {
    type Error = C::Error;

    async fn send(&self, request: DirectoryRequest) -> Result<DirectoryResponse, Self::Error> {
        (**self).send(request).await
    }
}

/// Failure to use a BIP77 directory as a [`Directory`].
#[derive(Debug, thiserror::Error)]
pub enum DirectoryError<E: Error + 'static> {
    /// The underlying client failed.
    #[error("directory client failed: {0}")]
    Client(#[source] E),
    /// The directory returned a status not defined for the operation.
    #[error("directory returned unexpected status {status} for {method:?}")]
    UnexpectedStatus {
        /// Operation that received the status.
        method: Method,
        /// Unexpected inner HTTP status.
        status: u16,
    },
}

/// A BIP77 directory exposed through the [`Directory`] contract.
///
/// The directory keeps the first payload POSTed to a mailbox and answers 200
/// to every POST, including one whose payload it discards, as [`Directory`]
/// allows. [`put`] therefore succeeds on an occupied mailbox.
///
/// [`put`]: Directory::put
#[derive(Debug)]
pub struct Bip77Directory<C> {
    client: C,
    directory: Url,
}

/// A directory URL that cannot hold mailbox paths.
#[derive(Debug, thiserror::Error)]
pub enum DirectoryUrlError {
    /// The URL does not parse.
    #[error("invalid directory URL: {0}")]
    Parse(#[source] url::ParseError),
    /// The URL has no path to append a mailbox id to, as in `mailto:` URLs.
    #[error("directory URL has no path")]
    NoPath,
}

impl<C> Bip77Directory<C> {
    /// Construct a directory for a target-resource base URL.
    ///
    /// Mailbox ids are appended to the URL's path; its query is kept.
    pub fn new(client: C, directory: &str) -> Result<Self, DirectoryUrlError> {
        Ok(Self::with_url(client, parse_directory(directory)?))
    }

    pub(crate) fn with_url(client: C, directory: Url) -> Self {
        Self { client, directory }
    }

    fn target(&self, mailbox: MailboxId) -> String {
        let mut target = self.directory.clone();
        target
            .path_segments_mut()
            .expect("parse_directory rejects URLs without a path")
            .pop_if_empty()
            .push(&ShortId::from(mailbox).to_string());
        target.into()
    }
}

/// Parse a directory base URL that mailbox ids can be appended to.
pub(crate) fn parse_directory(directory: &str) -> Result<Url, DirectoryUrlError> {
    let url = Url::parse(directory).map_err(DirectoryUrlError::Parse)?;
    if url.cannot_be_a_base() {
        return Err(DirectoryUrlError::NoPath);
    }
    Ok(url)
}

impl<C: DirectoryClient> Bip77Directory<C> {
    async fn request(
        &self,
        method: Method,
        mailbox: MailboxId,
        body: Vec<u8>,
    ) -> Result<DirectoryResponse, DirectoryError<C::Error>> {
        self.client
            .send(DirectoryRequest {
                method,
                target: self.target(mailbox),
                body,
            })
            .await
            .map_err(DirectoryError::Client)
    }
}

impl<C: DirectoryClient> Directory for Bip77Directory<C> {
    type Error = DirectoryError<C::Error>;

    async fn put(&self, mailbox: MailboxId, message: &[u8]) -> Result<(), Self::Error> {
        let response = self
            .request(Method::Post, mailbox, message.to_vec())
            .await?;
        match response.status {
            200 => Ok(()),
            status => Err(DirectoryError::UnexpectedStatus {
                method: Method::Post,
                status,
            }),
        }
    }

    async fn get(&self, mailbox: MailboxId) -> Result<Option<Vec<u8>>, Self::Error> {
        let response = self.request(Method::Get, mailbox, Vec::new()).await?;
        match response.status {
            200 => Ok(Some(response.body)),
            202 => Ok(None),
            status => Err(DirectoryError::UnexpectedStatus {
                method: Method::Get,
                status,
            }),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {

    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    use fungi_mailboxes::{Directory, PAYLOAD_BYTES, Reader, Writer, derive_mailbox_id};

    use super::*;

    /// Storage semantics of payjoin-mailroom's mailbox endpoints.
    #[derive(Debug, Default)]
    struct ReferenceDirectory {
        mailboxes: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl DirectoryClient for ReferenceDirectory {
        type Error = Infallible;

        async fn send(&self, request: DirectoryRequest) -> Result<DirectoryResponse, Self::Error> {
            let mut mailboxes = self.mailboxes.lock().unwrap();
            let status = match request.method {
                Method::Post => {
                    mailboxes.entry(request.target).or_insert(request.body);
                    200
                }
                Method::Get => match mailboxes.get(&request.target) {
                    Some(message) => {
                        return Ok(DirectoryResponse {
                            status: 200,
                            body: message.clone(),
                        });
                    }
                    None => 202,
                },
            };
            Ok(DirectoryResponse {
                status,
                body: Vec::new(),
            })
        }
    }

    #[test]
    fn short_ids_use_the_bip77_wire_shape() {
        let id = ShortId::from(derive_mailbox_id(&[1; 32], 0)).to_string();
        assert_eq!(id.len(), 13);
        assert!(
            id.bytes()
                .all(|byte| b"QPZRY9X8GF2TVDW0S3JN54KHCE6MUA7L".contains(&byte))
        );

        assert_eq!(ShortId([0; 8]).to_string(), "QQQQQQQQQQQQQ");
    }

    #[tokio::test]
    async fn first_payload_wins_and_later_posts_are_accepted() {
        let directory =
            Bip77Directory::new(ReferenceDirectory::default(), "https://directory.test/").unwrap();
        let mailbox = derive_mailbox_id(&[2; 32], 0);

        for message in [b"first", b"other", b"first"] {
            directory.put(mailbox, message).await.unwrap();
        }
        assert_eq!(
            directory.get(mailbox).await.unwrap(),
            Some(b"first".to_vec())
        );
    }

    #[tokio::test]
    async fn writer_at_a_stale_index_loses_its_message_without_an_error() {
        let directory =
            Bip77Directory::new(ReferenceDirectory::default(), "https://directory.test").unwrap();
        let mut writer = Writer::new(&directory, [4; 32]);
        writer.write(b"first").await.unwrap();

        let mut stale = Writer::resume(&directory, [4; 32], 0);
        stale.write(b"lost").await.unwrap();
        assert_eq!(stale.next_index(), 1);

        let mut reader = Reader::new(&directory, [4; 32]);
        assert_eq!(reader.read().await.unwrap(), b"first");
    }

    #[tokio::test]
    async fn empty_mailbox_maps_accepted_to_none() {
        let directory =
            Bip77Directory::new(ReferenceDirectory::default(), "https://directory.test").unwrap();
        assert_eq!(
            directory.get(derive_mailbox_id(&[3; 32], 0)).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn messages_pass_through_a_directory() {
        let reference = Arc::new(ReferenceDirectory::default());
        let directory =
            Bip77Directory::new(Arc::clone(&reference), "https://directory.test").unwrap();
        let mut writer = Writer::new(&directory, [5; 32]);
        let mut reader = Reader::new(&directory, [5; 32]);

        writer.write(b"first").await.unwrap();
        writer.write(b"second").await.unwrap();
        assert_eq!(reader.read().await.unwrap(), b"first");
        assert_eq!(reader.read().await.unwrap(), b"second");

        let mailboxes = reference.mailboxes.lock().unwrap();
        assert_eq!(mailboxes.len(), 2);
        assert!(mailboxes.values().all(|p| p.len() == PAYLOAD_BYTES));
    }

    struct FixedResponse(u16, Vec<u8>);

    impl DirectoryClient for FixedResponse {
        type Error = Infallible;
        async fn send(&self, _: DirectoryRequest) -> Result<DirectoryResponse, Self::Error> {
            Ok(DirectoryResponse {
                status: self.0,
                body: self.1.clone(),
            })
        }
    }

    #[tokio::test]
    async fn directory_failures_are_not_reported_as_success() {
        let mailbox = derive_mailbox_id(&[21; 32], 0);
        for status in [400, 403, 500] {
            let directory =
                Bip77Directory::new(FixedResponse(status, Vec::new()), "https://directory.test")
                    .unwrap();
            assert!(matches!(
                directory.put(mailbox, &[1]).await,
                Err(DirectoryError::UnexpectedStatus {
                    method: Method::Post,
                    ..
                })
            ));
            assert!(matches!(
                directory.get(mailbox).await,
                Err(DirectoryError::UnexpectedStatus {
                    method: Method::Get,
                    ..
                })
            ));
        }
    }

    #[test]
    fn mailbox_ids_extend_the_directory_path() {
        let mailbox = derive_mailbox_id(&[6; 32], 0);
        let id = ShortId::from(mailbox);
        for (base, target) in [
            (
                "https://directory.test",
                format!("https://directory.test/{id}"),
            ),
            (
                "https://directory.test/",
                format!("https://directory.test/{id}"),
            ),
            (
                "https://directory.test/base/",
                format!("https://directory.test/base/{id}"),
            ),
            (
                "https://directory.test/base?x=1",
                format!("https://directory.test/base/{id}?x=1"),
            ),
        ] {
            let directory = Bip77Directory::new((), base).unwrap();
            assert_eq!(directory.target(mailbox), target, "{base}");
        }
    }

    #[test]
    fn directory_urls_without_a_path_are_rejected() {
        assert!(matches!(
            Bip77Directory::new((), "not a URL"),
            Err(DirectoryUrlError::Parse(_))
        ));
        assert!(matches!(
            Bip77Directory::new((), "mailto:directory@example.com"),
            Err(DirectoryUrlError::NoPath)
        ));
    }
}
