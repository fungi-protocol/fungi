//! BIP77 Payjoin directory storage for Fungi linked mailboxes.
//!
//! Maps mailbox slots to BIP77 paths using [`DirectoryExchange`] for network I/O.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::error::Error;
use std::fmt;
use std::future::Future;

use bech32::{Hrp, NoChecksum};
use fungi_mailbox::{MailboxStore, SlotSecret};
use url::Url;

mod ohttp;
mod transport;

pub use ohttp::{
    ENCAPSULATED_MESSAGE_BYTES, OhttpExchange, OhttpExchangeError, Relay, SingleRelay,
};
pub use transport::{Builder, Receiver, Sender};

/// A BIP77 mailbox identifier encoded as 13 uppercase bech32 characters.
///
/// Uses the first 64 bits of a Fungi slot ID, rather than a Payjoin public-key
/// hash. The encoding omits the HRP, separator, and checksum.
pub(crate) struct ShortId([u8; 8]);

impl From<SlotSecret> for ShortId {
    fn from(slot: SlotSecret) -> Self {
        let mut id = [0; 8];
        id.copy_from_slice(&slot.to_bytes()[..8]);
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

/// Execute one complete directory request/response transaction.
///
/// Implementations must not expose request-correlated state to callers. An
/// OHTTP implementation must create fresh encapsulation for every call and
/// use that call's response context before returning.
pub trait DirectoryExchange: Send + Sync {
    /// Exchange failure.
    type Error: Error + Send + Sync + 'static;

    /// Execute one complete exchange.
    fn exchange(
        &self,
        request: DirectoryRequest,
    ) -> impl Future<Output = Result<DirectoryResponse, Self::Error>> + Send;
}

impl<X: DirectoryExchange> DirectoryExchange for std::sync::Arc<X> {
    type Error = X::Error;

    async fn exchange(&self, request: DirectoryRequest) -> Result<DirectoryResponse, Self::Error> {
        (**self).exchange(request).await
    }
}

/// Failure to use a BIP77 directory as a linked-mailbox store.
#[derive(Debug, thiserror::Error)]
pub enum StoreError<E: Error + 'static> {
    /// The underlying exchange failed.
    #[error("directory exchange failed: {0}")]
    Exchange(#[source] E),
    /// The directory returned a status not defined for the operation.
    #[error("directory returned unexpected status {status} for {method:?}")]
    UnexpectedStatus {
        /// Operation that received the status.
        method: Method,
        /// Unexpected inner HTTP status.
        status: u16,
    },
}

/// A BIP77 directory exposed through the linked-mailbox storage contract.
///
/// The directory keeps the first payload POSTed to a mailbox and answers 200
/// to every POST, including one whose payload it discards, as
/// [`MailboxStore`] allows. [`put`] therefore succeeds on an occupied mailbox.
///
/// [`put`]: MailboxStore::put
#[derive(Debug)]
pub struct Bip77MailboxStore<X> {
    exchange: X,
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

impl<X> Bip77MailboxStore<X> {
    /// Construct a store for a directory target-resource base URL.
    ///
    /// Mailbox ids are appended to the URL's path; its query is kept.
    pub fn new(exchange: X, directory: &str) -> Result<Self, DirectoryUrlError> {
        Ok(Self::with_url(exchange, parse_directory(directory)?))
    }

    pub(crate) fn with_url(exchange: X, directory: Url) -> Self {
        Self {
            exchange,
            directory,
        }
    }

    fn target(&self, slot: SlotSecret) -> String {
        let mut target = self.directory.clone();
        target
            .path_segments_mut()
            .expect("parse_directory rejects URLs without a path")
            .pop_if_empty()
            .push(&ShortId::from(slot).to_string());
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

impl<X: DirectoryExchange> Bip77MailboxStore<X> {
    async fn request(
        &self,
        method: Method,
        slot: SlotSecret,
        body: Vec<u8>,
    ) -> Result<DirectoryResponse, StoreError<X::Error>> {
        self.exchange
            .exchange(DirectoryRequest {
                method,
                target: self.target(slot),
                body,
            })
            .await
            .map_err(StoreError::Exchange)
    }
}

impl<X: DirectoryExchange> MailboxStore for Bip77MailboxStore<X> {
    type Error = StoreError<X::Error>;

    async fn put(&self, slot: SlotSecret, message: &[u8]) -> Result<(), Self::Error> {
        let response = self.request(Method::Post, slot, message.to_vec()).await?;
        match response.status {
            200 => Ok(()),
            status => Err(StoreError::UnexpectedStatus {
                method: Method::Post,
                status,
            }),
        }
    }

    async fn get(&self, slot: SlotSecret) -> Result<Option<Vec<u8>>, Self::Error> {
        let response = self.request(Method::Get, slot, Vec::new()).await?;
        match response.status {
            200 => Ok(Some(response.body)),
            202 => Ok(None),
            status => Err(StoreError::UnexpectedStatus {
                method: Method::Get,
                status,
            }),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
