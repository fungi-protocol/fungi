//! Complete BIP77 OHTTP request/response exchanges.

use std::error::Error;
use std::future::Future;
use std::io::Cursor;

use rand::RngCore;
use url::Url;

use crate::{DirectoryExchange, DirectoryRequest, DirectoryResponse, Method};

/// Fixed BIP77 encapsulated request and response size.
pub const ENCAPSULATED_MESSAGE_BYTES: usize = 8192;

const PADDED_BHTTP_REQUEST_BYTES: usize = 8104;

/// Submit one encapsulated request to an OHTTP relay.
///
/// Do not replay `body`: repeated ciphertext links requests. Retry through
/// [`OhttpExchange::exchange`] for fresh padding and encapsulation.
pub trait Relay: Send + Sync {
    /// Relay transport failure.
    type Error: Error + Send + Sync + 'static;

    /// Submit one encapsulated message and return its encapsulated response.
    fn post(&self, body: Vec<u8>) -> impl Future<Output = Result<Vec<u8>, Self::Error>> + Send;
}

/// Failure during a complete BIP77 OHTTP exchange.
#[derive(Debug, thiserror::Error)]
pub enum OhttpExchangeError<E: Error + 'static> {
    /// The target-resource URL is invalid or unsupported.
    #[error("invalid directory target: {0}")]
    Target(#[source] url::ParseError),
    /// The target must be an HTTP(S) URL without credentials or a fragment.
    #[error("unsupported directory target URL")]
    UnsupportedTarget,
    /// Binary HTTP encoding or decoding failed.
    #[error("binary HTTP: {0}")]
    Bhttp(#[source] bhttp::Error),
    /// OHTTP configuration, encapsulation, or decapsulation failed.
    #[error("OHTTP: {0}")]
    Ohttp(#[source] ohttp::Error),
    /// The relay transport failed.
    #[error("relay: {0}")]
    Relay(#[source] E),
    /// An encapsulated request or response has the wrong size.
    #[error("encapsulated message has size {actual}, expected {expected}")]
    MessageSize {
        /// Actual encoded size.
        actual: usize,
        /// Required BIP77 size.
        expected: usize,
    },
    /// A decoded BHTTP response did not contain a final status.
    #[error("binary HTTP response has no final status")]
    MissingStatus,
}

/// Complete OHTTP exchanges with one directory gateway.
///
/// The caller selects a relay for each exchange and controls retries.
#[derive(Debug)]
pub struct OhttpExchange {
    key_config: Vec<u8>,
}

/// A [`DirectoryExchange`] that sends every request through one relay.
#[derive(Debug)]
pub struct SingleRelay<R> {
    exchange: OhttpExchange,
    relay: R,
}

impl OhttpExchange {
    /// Validate and retain the gateway's encoded RFC 9458 OHTTP key
    /// configuration. The caller must obtain it through an authenticated
    /// bootstrap mechanism.
    pub fn new(key_config: impl Into<Vec<u8>>) -> Result<Self, ohttp::Error> {
        let key_config = key_config.into();
        ohttp::KeyConfig::decode(&key_config)?;
        Ok(Self { key_config })
    }

    /// Execute one complete exchange through `relay`.
    pub async fn exchange<R: Relay>(
        &self,
        relay: &R,
        request: DirectoryRequest,
    ) -> Result<DirectoryResponse, OhttpExchangeError<R::Error>> {
        let (request, context) = self.encode_request(request)?;
        let response = relay
            .post(request)
            .await
            .map_err(OhttpExchangeError::Relay)?;
        Self::decode_response(response, context)
    }

    fn encode_request<E: Error + 'static>(
        &self,
        request: DirectoryRequest,
    ) -> Result<(Vec<u8>, ohttp::ClientResponse), OhttpExchangeError<E>> {
        let url = Url::parse(&request.target).map_err(OhttpExchangeError::Target)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(OhttpExchangeError::UnsupportedTarget);
        }
        let authority = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
            None => url.host_str().unwrap_or_default().to_owned(),
        };
        let mut path = url.path().to_owned();
        if let Some(query) = url.query() {
            path.push('?');
            path.push_str(query);
        }

        let method = match request.method {
            Method::Get => b"GET".to_vec(),
            Method::Post => b"POST".to_vec(),
        };
        let mut message = bhttp::Message::request(
            method,
            url.scheme().as_bytes().to_vec(),
            authority.into_bytes(),
            path.into_bytes(),
        );
        if !request.body.is_empty() {
            message.write_content(&request.body);
        }

        let mut padded = [0; PADDED_BHTTP_REQUEST_BYTES];
        rand::rngs::OsRng.fill_bytes(&mut padded);
        message
            .write_bhttp(bhttp::Mode::KnownLength, &mut padded.as_mut_slice())
            .map_err(OhttpExchangeError::Bhttp)?;

        let client = ohttp::ClientRequest::from_encoded_config(&self.key_config)
            .map_err(OhttpExchangeError::Ohttp)?;
        let (encapsulated, response_context) = client
            .encapsulate(&padded)
            .map_err(OhttpExchangeError::Ohttp)?;
        check_size(encapsulated.len())?;
        Ok((encapsulated, response_context))
    }

    fn decode_response<E: Error + 'static>(
        response: Vec<u8>,
        context: ohttp::ClientResponse,
    ) -> Result<DirectoryResponse, OhttpExchangeError<E>> {
        check_size(response.len())?;
        let plaintext = context
            .decapsulate(&response)
            .map_err(OhttpExchangeError::Ohttp)?;
        let message = bhttp::Message::read_bhttp(&mut Cursor::new(plaintext))
            .map_err(OhttpExchangeError::Bhttp)?;
        let status = message
            .control()
            .status()
            .ok_or(OhttpExchangeError::MissingStatus)?
            .code();
        Ok(DirectoryResponse {
            status,
            body: message.content().to_vec(),
        })
    }
}

impl<R> SingleRelay<R> {
    /// Send every exchange through `relay`.
    pub fn new(exchange: OhttpExchange, relay: R) -> Self {
        Self { exchange, relay }
    }
}

impl<R: Relay> DirectoryExchange for SingleRelay<R> {
    type Error = OhttpExchangeError<R::Error>;

    async fn exchange(&self, request: DirectoryRequest) -> Result<DirectoryResponse, Self::Error> {
        self.exchange.exchange(&self.relay, request).await
    }
}

fn check_size<E: Error + 'static>(actual: usize) -> Result<(), OhttpExchangeError<E>> {
    if actual != ENCAPSULATED_MESSAGE_BYTES {
        return Err(OhttpExchangeError::MessageSize {
            actual,
            expected: ENCAPSULATED_MESSAGE_BYTES,
        });
    }
    Ok(())
}
