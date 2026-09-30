//! OHTTP client for BIP77 directory requests.

use std::error::Error;
use std::future::Future;
use std::io::Cursor;

use rand::RngCore;
use url::Url;

use crate::{DirectoryClient, DirectoryRequest, DirectoryResponse, Method};

/// Fixed BIP77 encapsulated request and response size.
pub const ENCAPSULATED_MESSAGE_BYTES: usize = 8192;

const PADDED_BHTTP_REQUEST_BYTES: usize = 8104;

/// Submit one encapsulated request to an OHTTP relay.
///
/// Do not replay `body`: repeated ciphertext links requests. Retry through
/// [`OhttpClient::send`] for fresh padding and encapsulation.
pub trait Relay: Send + Sync {
    /// Relay transport failure.
    type Error: Error + Send + Sync + 'static;

    /// Submit one encapsulated message and return its encapsulated response.
    fn post(&self, body: Vec<u8>) -> impl Future<Output = Result<Vec<u8>, Self::Error>> + Send;
}

/// Failure to send a directory request through OHTTP.
#[derive(Debug, thiserror::Error)]
pub enum OhttpClientError<E: Error + 'static> {
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

/// OHTTP client for one directory gateway.
///
/// The caller selects a relay for each request and controls retries.
#[derive(Debug)]
pub struct OhttpClient {
    key_config: Vec<u8>,
}

/// A [`DirectoryClient`] that sends every request through one relay.
#[derive(Debug)]
pub struct SingleRelay<R> {
    client: OhttpClient,
    relay: R,
}

impl OhttpClient {
    /// Validate and retain the gateway's encoded RFC 9458 OHTTP key
    /// configuration. The caller must obtain it through an authenticated
    /// bootstrap mechanism.
    pub fn new(key_config: impl Into<Vec<u8>>) -> Result<Self, ohttp::Error> {
        let key_config = key_config.into();
        ohttp::KeyConfig::decode(&key_config)?;
        Ok(Self { key_config })
    }

    /// Send `request` through `relay` and return its response.
    pub async fn send<R: Relay>(
        &self,
        relay: &R,
        request: DirectoryRequest,
    ) -> Result<DirectoryResponse, OhttpClientError<R::Error>> {
        let (request, context) = self.encode_request(request)?;
        let response = relay.post(request).await.map_err(OhttpClientError::Relay)?;
        Self::decode_response(response, context)
    }

    fn encode_request<E: Error + 'static>(
        &self,
        request: DirectoryRequest,
    ) -> Result<(Vec<u8>, ohttp::ClientResponse), OhttpClientError<E>> {
        let url = Url::parse(&request.target).map_err(OhttpClientError::Target)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(OhttpClientError::UnsupportedTarget);
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
            .map_err(OhttpClientError::Bhttp)?;

        let client = ohttp::ClientRequest::from_encoded_config(&self.key_config)
            .map_err(OhttpClientError::Ohttp)?;
        let (encapsulated, response_context) = client
            .encapsulate(&padded)
            .map_err(OhttpClientError::Ohttp)?;
        check_size(encapsulated.len())?;
        Ok((encapsulated, response_context))
    }

    fn decode_response<E: Error + 'static>(
        response: Vec<u8>,
        context: ohttp::ClientResponse,
    ) -> Result<DirectoryResponse, OhttpClientError<E>> {
        check_size(response.len())?;
        let plaintext = context
            .decapsulate(&response)
            .map_err(OhttpClientError::Ohttp)?;
        let message = bhttp::Message::read_bhttp(&mut Cursor::new(plaintext))
            .map_err(OhttpClientError::Bhttp)?;
        let status = message
            .control()
            .status()
            .ok_or(OhttpClientError::MissingStatus)?
            .code();
        Ok(DirectoryResponse {
            status,
            body: message.content().to_vec(),
        })
    }
}

impl<R> SingleRelay<R> {
    /// Send every request through `relay`.
    pub fn new(client: OhttpClient, relay: R) -> Self {
        Self { client, relay }
    }
}

impl<R: Relay> DirectoryClient for SingleRelay<R> {
    type Error = OhttpClientError<R::Error>;

    async fn send(&self, request: DirectoryRequest) -> Result<DirectoryResponse, Self::Error> {
        self.client.send(&self.relay, request).await
    }
}

fn check_size<E: Error + 'static>(actual: usize) -> Result<(), OhttpClientError<E>> {
    if actual != ENCAPSULATED_MESSAGE_BYTES {
        return Err(OhttpClientError::MessageSize {
            actual,
            expected: ENCAPSULATED_MESSAGE_BYTES,
        });
    }
    Ok(())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::convert::Infallible;
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};

    use fungi_mailboxes::{Reader, Writer};

    use super::*;
    use crate::testing::{LoopbackRelay, loopback_relay, ohttp_loopback};
    use crate::{Bip77Directory, DirectoryError};

    fn mailbox_request() -> DirectoryRequest {
        DirectoryRequest {
            method: Method::Get,
            target: "https://directory.test/QQQQQQQQQQQQQ".to_owned(),
            body: Vec::new(),
        }
    }

    #[tokio::test]
    async fn ohttp_client_owns_context_and_never_reuses_ciphertext() {
        let (client, ciphertexts) = ohttp_loopback();

        let first = client.send(mailbox_request()).await.unwrap();
        let second = client.send(mailbox_request()).await.unwrap();
        assert_eq!(first.status, 200);
        assert_eq!(first.body, b"mailbox payload");
        assert_eq!(second, first);

        let ciphertexts = ciphertexts.lock().unwrap();
        assert_eq!(ciphertexts.len(), 2);
        assert_ne!(ciphertexts[0], ciphertexts[1]);
    }

    #[tokio::test]
    async fn messages_pass_over_ohttp() {
        let (client, ciphertexts) = ohttp_loopback();
        let directory = Bip77Directory::new(client, "https://directory.test").unwrap();
        let mut writer = Writer::new(&directory, [8; 32]);
        let mut reader = Reader::new(&directory, [8; 32]);

        writer.write(b"through OHTTP").await.unwrap();
        assert_eq!(reader.read().await.unwrap(), b"through OHTTP");
        writer.write(b"next message").await.unwrap();
        assert_eq!(reader.read().await.unwrap(), b"next message");

        // Each message needs one POST and one GET. Resuming reception must not
        // re-fetch an earlier mailbox.
        let ciphertexts = ciphertexts.lock().unwrap();
        assert_eq!(ciphertexts.len(), 4);
        assert!(ciphertexts.windows(2).all(|pair| pair[0] != pair[1]));
    }

    #[tokio::test]
    async fn oversized_message_is_rejected_before_reaching_the_relay() {
        let (client, ciphertexts) = ohttp_loopback();
        let error = client
            .send(DirectoryRequest {
                method: Method::Post,
                target: "https://directory.test/mailbox".to_owned(),
                body: vec![0; ENCAPSULATED_MESSAGE_BYTES],
            })
            .await
            .unwrap_err();
        assert!(matches!(error, OhttpClientError::Bhttp(_)));
        assert!(ciphertexts.lock().unwrap().is_empty());
    }

    struct CorruptRelay {
        inner: LoopbackRelay,
        truncate: bool,
    }

    impl Relay for CorruptRelay {
        type Error = Infallible;

        async fn post(&self, body: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
            let mut response = self.inner.post(body).await?;
            if self.truncate {
                response.pop();
            } else {
                *response.last_mut().unwrap() ^= 1;
            }
            Ok(response)
        }
    }

    #[tokio::test]
    async fn malformed_or_tampered_responses_are_rejected() {
        for truncate in [false, true] {
            let (relay, keys, _) = loopback_relay();
            let client = OhttpClient::new(keys).unwrap();
            let relay = CorruptRelay {
                inner: relay,
                truncate,
            };
            let result = client.send(&relay, mailbox_request()).await;
            if truncate {
                assert!(matches!(result, Err(OhttpClientError::MessageSize { .. })));
            } else {
                assert!(matches!(result, Err(OhttpClientError::Ohttp(_))));
            }
        }
    }

    #[tokio::test]
    async fn invalid_targets_are_rejected_before_contacting_the_relay() {
        let (client, ciphertexts) = ohttp_loopback();
        for target in [
            "not a URL",
            "file:///mailbox",
            "ftp://directory.test/mailbox",
            "https://user:password@directory.test/mailbox",
            "https://directory.test/mailbox#fragment",
        ] {
            assert!(
                client
                    .send(DirectoryRequest {
                        method: Method::Get,
                        target: target.to_owned(),
                        body: Vec::new(),
                    })
                    .await
                    .is_err()
            );
        }
        assert!(ciphertexts.lock().unwrap().is_empty());
    }

    struct ReplayRelay {
        inner: LoopbackRelay,
        response: Mutex<Option<Vec<u8>>>,
    }

    impl Relay for ReplayRelay {
        type Error = Infallible;

        async fn post(&self, body: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
            if let Some(response) = self.response.lock().unwrap().clone() {
                return Ok(response);
            }
            let response = self.inner.post(body).await?;
            *self.response.lock().unwrap() = Some(response.clone());
            Ok(response)
        }
    }

    #[tokio::test]
    async fn response_from_a_previous_request_is_rejected() {
        let (relay, keys, _) = loopback_relay();
        let client = OhttpClient::new(keys).unwrap();
        let relay = ReplayRelay {
            inner: relay,
            response: Mutex::new(None),
        };
        assert_eq!(
            client.send(&relay, mailbox_request()).await.unwrap().body,
            b"mailbox payload"
        );
        assert!(matches!(
            client.send(&relay, mailbox_request()).await,
            Err(OhttpClientError::Ohttp(_))
        ));
    }

    struct BrokenRelay;

    impl Relay for BrokenRelay {
        type Error = std::io::Error;
        async fn post(&self, _: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
            Err(std::io::Error::other("relay unavailable"))
        }
    }

    #[tokio::test]
    async fn relay_failures_reach_mailbox_callers() {
        let (_, keys, _) = loopback_relay();
        let client = SingleRelay::new(OhttpClient::new(keys).unwrap(), BrokenRelay);
        let directory = Bip77Directory::new(client, "https://directory.test").unwrap();
        assert!(matches!(
            Writer::new(&directory, [22; 32]).write(&[1]).await,
            Err(fungi_mailboxes::Error::Directory(DirectoryError::Client(
                OhttpClientError::Relay(_)
            )))
        ));
        assert!(matches!(
            Reader::new(&directory, [22; 32]).read().await,
            Err(fungi_mailboxes::Error::Directory(DirectoryError::Client(
                OhttpClientError::Relay(_)
            )))
        ));
    }

    struct ResponseRelay {
        server: ::ohttp::Server,
        plaintext: Vec<u8>,
        requests: Arc<Mutex<Vec<bhttp::Message>>>,
    }

    impl Relay for ResponseRelay {
        type Error = Infallible;
        async fn post(&self, body: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
            let (plaintext, context) = self.server.decapsulate(&body).unwrap();
            self.requests
                .lock()
                .unwrap()
                .push(bhttp::Message::read_bhttp(&mut Cursor::new(plaintext)).unwrap());
            Ok(context.encapsulate(&self.plaintext).unwrap())
        }
    }

    #[tokio::test]
    async fn encrypted_response_must_contain_valid_bhttp_response_control() {
        for missing_status in [false, true] {
            let (relay, keys, _) = loopback_relay();
            let mut plaintext = vec![0xff; 8144];
            if missing_status {
                plaintext.fill(0);
                bhttp::Message::request(
                    b"GET".to_vec(),
                    b"https".to_vec(),
                    b"directory.test".to_vec(),
                    b"/".to_vec(),
                )
                .write_bhttp(bhttp::Mode::KnownLength, &mut plaintext.as_mut_slice())
                .unwrap();
            }
            let requests = Arc::new(Mutex::new(Vec::new()));
            let client = OhttpClient::new(keys).unwrap();
            let relay = ResponseRelay {
                server: relay.server,
                plaintext,
                requests: requests.clone(),
            };
            let error = client
                .send(
                    &relay,
                    DirectoryRequest {
                        method: Method::Get,
                        target: "https://[::1]:8443/mailbox?poll=1".to_owned(),
                        body: Vec::new(),
                    },
                )
                .await
                .unwrap_err();
            if missing_status {
                assert!(matches!(error, OhttpClientError::MissingStatus));
            } else {
                assert!(matches!(error, OhttpClientError::Bhttp(_)));
            }
            let requests = requests.lock().unwrap();
            assert_eq!(
                requests[0].control().authority(),
                Some(b"[::1]:8443".as_slice())
            );
            assert_eq!(
                requests[0].control().path(),
                Some(b"/mailbox?poll=1".as_slice())
            );
        }
    }

    #[test]
    fn invalid_key_configuration_is_rejected() {
        assert!(OhttpClient::new(Vec::new()).is_err());
    }
}
