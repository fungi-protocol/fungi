use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use fungi_mailboxes::{Directory, PAYLOAD_BYTES, Reader, Writer, derive_mailbox_secret};

use super::*;

type Ciphertexts = Arc<Mutex<Vec<Vec<u8>>>>;

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
    let id = ShortId::from(derive_mailbox_secret(&[1; 32], 0)).to_string();
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
    let mailbox = derive_mailbox_secret(&[2; 32], 0);

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
        directory
            .get(derive_mailbox_secret(&[3; 32], 0))
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn messages_pass_through_a_directory() {
    let reference = Arc::new(ReferenceDirectory::default());
    let directory = Bip77Directory::new(Arc::clone(&reference), "https://directory.test").unwrap();
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
    let mailbox = derive_mailbox_secret(&[21; 32], 0);
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
    let mailbox = derive_mailbox_secret(&[6; 32], 0);
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

#[derive(Debug)]
struct LoopbackRelay {
    server: ::ohttp::Server,
    ciphertexts: Ciphertexts,
    mailboxes: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl Relay for LoopbackRelay {
    type Error = Infallible;

    async fn post(&self, body: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        assert_eq!(body.len(), ENCAPSULATED_MESSAGE_BYTES);
        self.ciphertexts.lock().unwrap().push(body.clone());

        let (plaintext, response_context) = self.server.decapsulate(&body).unwrap();
        assert_eq!(plaintext.len(), 8104);
        let request = bhttp::Message::read_bhttp(&mut Cursor::new(plaintext)).unwrap();
        let method = request.control().method().unwrap();
        let path = request.control().path().unwrap().to_vec();
        let (status, content) = match method {
            b"POST" => {
                self.mailboxes
                    .lock()
                    .unwrap()
                    .entry(path)
                    .or_insert_with(|| request.content().to_vec());
                (200_u16, Vec::new())
            }
            b"GET" => match self.mailboxes.lock().unwrap().get(&path).cloned() {
                Some(message) => (200, message),
                None => (202, Vec::new()),
            },
            _ => (405, Vec::new()),
        };

        let mut response = bhttp::Message::response(bhttp::StatusCode::try_from(status).unwrap());
        response.write_content(&content);
        let mut padded = [0; 8144];
        response
            .write_bhttp(bhttp::Mode::KnownLength, &mut padded.as_mut_slice())
            .unwrap();
        let encapsulated = response_context.encapsulate(&padded).unwrap();
        assert_eq!(encapsulated.len(), ENCAPSULATED_MESSAGE_BYTES);
        Ok(encapsulated)
    }
}

fn loopback_relay() -> (LoopbackRelay, Vec<u8>, Ciphertexts) {
    use ::ohttp::hpke::{Aead, Kdf, Kem};
    use ::ohttp::{KeyConfig, SymmetricSuite};

    let config = KeyConfig::new(
        1,
        Kem::K256Sha256,
        vec![SymmetricSuite::new(Kdf::HkdfSha256, Aead::ChaCha20Poly1305)],
    )
    .unwrap();
    let server = ::ohttp::Server::new(config).unwrap();
    let encoded_config = server.config().encode().unwrap();
    let ciphertexts = Arc::new(Mutex::new(Vec::new()));
    let mailboxes = Mutex::new(HashMap::from([(
        b"/QQQQQQQQQQQQQ".to_vec(),
        b"mailbox payload".to_vec(),
    )]));
    (
        LoopbackRelay {
            server,
            ciphertexts: Arc::clone(&ciphertexts),
            mailboxes,
        },
        encoded_config,
        ciphertexts,
    )
}

fn ohttp_loopback() -> (SingleRelay<LoopbackRelay>, Ciphertexts) {
    let (relay, keys, ciphertexts) = loopback_relay();
    let client = OhttpClient::new(keys).unwrap();
    (SingleRelay::new(client, relay), ciphertexts)
}

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
