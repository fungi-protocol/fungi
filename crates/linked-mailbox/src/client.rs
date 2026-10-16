//! Sans-I/O BIP77 directory client.
//!
//! [`Client::get`] and [`Client::post`] return an [`OhttpRequest`] for the
//! caller to send to the relay, and a context. The caller passes the relay's
//! response body to the context, which returns the result.

use ohttp_client::{OhttpClient, Response, ResponseContext};

pub use ohttp_client::{KeyConfig, OhttpRequest, Url, parse_key_config};

use crate::ShortId;

/// BHTTP request size that BIP77 pads to, so the encapsulated request is
/// [`ENCAPSULATED_MESSAGE_BYTES`].
pub const PADDED_BHTTP_REQUEST_BYTES: usize = 8104;

/// Size of every encapsulated directory request and response.
pub const ENCAPSULATED_MESSAGE_BYTES: usize = 8192;

/// A client for one directory, reached through one OHTTP relay.
#[derive(Debug, Clone)]
pub struct Client {
    ohttp: OhttpClient,
}

/// Decodes the response to a [`Client::get`] request.
pub struct GetContext(ResponseContext);

/// Decodes the response to a [`Client::post`] request.
pub struct PostContext(ResponseContext);

/// Failure to build a request or decode its response.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// BHTTP encoding, OHTTP encapsulation, or the reverse failed.
    #[error("OHTTP: {0}")]
    Ohttp(#[from] ohttp_client::Error),
    /// The encapsulated response is not [`ENCAPSULATED_MESSAGE_BYTES`] long.
    #[error("encapsulated response has size {0}, expected {ENCAPSULATED_MESSAGE_BYTES}")]
    ResponseSize(usize),
    /// The directory answered with a status the request does not define.
    #[error("directory returned unexpected status {0}")]
    UnexpectedStatus(u16),
}

/// The directory URL is not an HTTP(S) URL without credentials or a fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unsupported directory URL")]
pub struct UnsupportedDirectory;

impl Client {
    /// Mailbox paths are appended to `directory`'s path.
    ///
    /// `key_config` is the directory gateway's OHTTP key configuration.
    pub fn new(
        relay: Url,
        directory: Url,
        key_config: KeyConfig,
    ) -> Result<Self, UnsupportedDirectory> {
        if !matches!(directory.scheme(), "http" | "https")
            || directory.host().is_none()
            || !directory.username().is_empty()
            || directory.password().is_some()
            || directory.fragment().is_some()
        {
            return Err(UnsupportedDirectory);
        }
        Ok(Self {
            ohttp: OhttpClient::new(relay, directory, key_config)
                .known_length(PADDED_BHTTP_REQUEST_BYTES),
        })
    }

    /// Retrieve the payload of a mailbox.
    ///
    /// The directory holds the request open until the mailbox has a payload or
    /// it times out.
    pub fn get(&self, id: ShortId) -> Result<(OhttpRequest, GetContext), Error> {
        let (request, context) = self.encapsulate("GET", id, None)?;
        Ok((request, GetContext(context)))
    }

    /// Store a payload in a mailbox.
    pub fn post(&self, id: ShortId, payload: &[u8]) -> Result<(OhttpRequest, PostContext), Error> {
        let (request, context) = self.encapsulate("POST", id, Some(payload))?;
        Ok((request, PostContext(context)))
    }

    fn encapsulate(
        &self,
        method: &str,
        id: ShortId,
        body: Option<&[u8]>,
    ) -> Result<(OhttpRequest, ResponseContext), Error> {
        Ok(self
            .ohttp
            .encapsulate(method, &format!("/{id}"), &[], &[], body)?)
    }
}

impl GetContext {
    /// The payload, or `None` if the directory timed out before the mailbox
    /// had one.
    pub fn finish(self, response: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let response = decapsulate(self.0, response)?;
        match response.status() {
            200 => Ok(Some(response.into_body())),
            202 => Ok(None),
            status => Err(Error::UnexpectedStatus(status)),
        }
    }
}

impl PostContext {
    pub fn finish(self, response: &[u8]) -> Result<(), Error> {
        let response = decapsulate(self.0, response)?;
        match response.status() {
            200 => Ok(()),
            status => Err(Error::UnexpectedStatus(status)),
        }
    }
}

fn decapsulate(context: ResponseContext, response: &[u8]) -> Result<Response, Error> {
    if response.len() != ENCAPSULATED_MESSAGE_BYTES {
        return Err(Error::ResponseSize(response.len()));
    }
    Ok(context.decapsulate(response)?)
}
