use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use fungi_mailbox::{MailboxStore, PAYLOAD_BYTES, Reader, Writer, derive_slot_secret};

use super::*;

type Ciphertexts = Arc<Mutex<Vec<Vec<u8>>>>;

/// Storage semantics of payjoin-mailroom's mailbox endpoints.
#[derive(Debug, Default)]
struct ReferenceDirectory {
    mailboxes: Mutex<HashMap<String, Vec<u8>>>,
}

impl DirectoryExchange for ReferenceDirectory {
    type Error = Infallible;

    async fn exchange(&self, request: DirectoryRequest) -> Result<DirectoryResponse, Self::Error> {
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
    let id = ShortId::from(derive_slot_secret(&[1; 32], 0)).to_string();
    assert_eq!(id.len(), 13);
    assert!(
        id.bytes()
            .all(|byte| b"QPZRY9X8GF2TVDW0S3JN54KHCE6MUA7L".contains(&byte))
    );

    assert_eq!(ShortId([0; 8]).to_string(), "QQQQQQQQQQQQQ");
}

#[tokio::test]
async fn first_payload_wins_and_later_posts_are_accepted() {
    let store =
        Bip77MailboxStore::new(ReferenceDirectory::default(), "https://directory.test/").unwrap();
    let slot = derive_slot_secret(&[2; 32], 0);

    for message in [b"first", b"other", b"first"] {
        store.put(slot, message).await.unwrap();
    }
    assert_eq!(store.get(slot).await.unwrap(), Some(b"first".to_vec()));
}

#[tokio::test]
async fn writer_at_a_stale_slot_id_loses_its_message_without_an_error() {
    let store =
        Bip77MailboxStore::new(ReferenceDirectory::default(), "https://directory.test").unwrap();
    let mut writer = Writer::new(&store, [4; 32]);
    writer.write(b"first").await.unwrap();

    let mut stale = Writer::resume(&store, [4; 32], 0);
    stale.write(b"lost").await.unwrap();
    assert_eq!(stale.next_slot_id(), 1);

    let mut reader = Reader::new(&store, [4; 32]);
    assert_eq!(reader.read().await.unwrap(), b"first");
}

#[tokio::test]
async fn empty_mailbox_maps_accepted_to_none() {
    let store =
        Bip77MailboxStore::new(ReferenceDirectory::default(), "https://directory.test").unwrap();
    assert_eq!(
        store.get(derive_slot_secret(&[3; 32], 0)).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn writer_and_reader_exchange_messages_through_a_directory() {
    let directory = Arc::new(ReferenceDirectory::default());
    let store = Bip77MailboxStore::new(Arc::clone(&directory), "https://directory.test").unwrap();
    let mut writer = Writer::new(&store, [5; 32]);
    let mut reader = Reader::new(&store, [5; 32]);

    writer.write(b"first").await.unwrap();
    writer.write(b"second").await.unwrap();
    assert_eq!(reader.read().await.unwrap(), b"first");
    assert_eq!(reader.read().await.unwrap(), b"second");

    let mailboxes = directory.mailboxes.lock().unwrap();
    assert_eq!(mailboxes.len(), 2);
    assert!(mailboxes.values().all(|p| p.len() == PAYLOAD_BYTES));
}

struct FixedResponse(u16, Vec<u8>);

impl DirectoryExchange for FixedResponse {
    type Error = Infallible;
    async fn exchange(&self, _: DirectoryRequest) -> Result<DirectoryResponse, Self::Error> {
        Ok(DirectoryResponse {
            status: self.0,
            body: self.1.clone(),
        })
    }
}

#[tokio::test]
async fn directory_failures_are_not_reported_as_success() {
    let slot = derive_slot_secret(&[21; 32], 0);
    for status in [400, 403, 500] {
        let store =
            Bip77MailboxStore::new(FixedResponse(status, Vec::new()), "https://directory.test")
                .unwrap();
        assert!(matches!(
            store.put(slot, &[1]).await,
            Err(StoreError::UnexpectedStatus {
                method: Method::Post,
                ..
            })
        ));
        assert!(matches!(
            store.get(slot).await,
            Err(StoreError::UnexpectedStatus {
                method: Method::Get,
                ..
            })
        ));
    }
}

#[test]
fn mailbox_ids_extend_the_directory_path() {
    let slot = derive_slot_secret(&[6; 32], 0);
    let id = ShortId::from(slot);
    for (directory, target) in [
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
        let store = Bip77MailboxStore::new((), directory).unwrap();
        assert_eq!(store.target(slot), target, "{directory}");
    }
}

#[test]
fn directory_urls_without_a_path_are_rejected() {
    assert!(matches!(
        Bip77MailboxStore::new((), "not a URL"),
        Err(DirectoryUrlError::Parse(_))
    ));
    assert!(matches!(
        Bip77MailboxStore::new((), "mailto:directory@example.com"),
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
    let exchange = OhttpExchange::new(keys).unwrap();
    (SingleRelay::new(exchange, relay), ciphertexts)
}

fn mailbox_request() -> DirectoryRequest {
    DirectoryRequest {
        method: Method::Get,
        target: "https://directory.test/QQQQQQQQQQQQQ".to_owned(),
        body: Vec::new(),
    }
}

#[tokio::test]
async fn ohttp_exchange_owns_context_and_never_reuses_ciphertext() {
    let (exchange, ciphertexts) = ohttp_loopback();

    let first = exchange.exchange(mailbox_request()).await.unwrap();
    let second = exchange.exchange(mailbox_request()).await.unwrap();
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"mailbox payload");
    assert_eq!(second, first);

    let ciphertexts = ciphertexts.lock().unwrap();
    assert_eq!(ciphertexts.len(), 2);
    assert_ne!(ciphertexts[0], ciphertexts[1]);
}

#[tokio::test]
async fn writer_and_reader_exchange_messages_over_ohttp() {
    let (exchange, ciphertexts) = ohttp_loopback();
    let store = Bip77MailboxStore::new(exchange, "https://directory.test").unwrap();
    let mut writer = Writer::new(&store, [8; 32]);
    let mut reader = Reader::new(&store, [8; 32]);

    writer.write(b"through OHTTP").await.unwrap();
    assert_eq!(reader.read().await.unwrap(), b"through OHTTP");
    writer.write(b"next message").await.unwrap();
    assert_eq!(reader.read().await.unwrap(), b"next message");

    // Each message needs one POST and one GET. Resuming reception must not
    // re-fetch an earlier slot.
    let ciphertexts = ciphertexts.lock().unwrap();
    assert_eq!(ciphertexts.len(), 4);
    assert!(ciphertexts.windows(2).all(|pair| pair[0] != pair[1]));
}

#[tokio::test]
async fn oversized_message_is_rejected_before_reaching_the_relay() {
    let (exchange, ciphertexts) = ohttp_loopback();
    let error = exchange
        .exchange(DirectoryRequest {
            method: Method::Post,
            target: "https://directory.test/slot".to_owned(),
            body: vec![0; ENCAPSULATED_MESSAGE_BYTES],
        })
        .await
        .unwrap_err();
    assert!(matches!(error, OhttpExchangeError::Bhttp(_)));
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
        let exchange = OhttpExchange::new(keys).unwrap();
        let relay = CorruptRelay {
            inner: relay,
            truncate,
        };
        let result = exchange.exchange(&relay, mailbox_request()).await;
        if truncate {
            assert!(matches!(
                result,
                Err(OhttpExchangeError::MessageSize { .. })
            ));
        } else {
            assert!(matches!(result, Err(OhttpExchangeError::Ohttp(_))));
        }
    }
}

#[tokio::test]
async fn invalid_targets_are_rejected_before_contacting_the_relay() {
    let (exchange, ciphertexts) = ohttp_loopback();
    for target in [
        "not a URL",
        "file:///mailbox",
        "ftp://directory.test/slot",
        "https://user:password@directory.test/slot",
        "https://directory.test/slot#fragment",
    ] {
        assert!(
            exchange
                .exchange(DirectoryRequest {
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
    let exchange = OhttpExchange::new(keys).unwrap();
    let relay = ReplayRelay {
        inner: relay,
        response: Mutex::new(None),
    };
    assert_eq!(
        exchange
            .exchange(&relay, mailbox_request())
            .await
            .unwrap()
            .body,
        b"mailbox payload"
    );
    assert!(matches!(
        exchange.exchange(&relay, mailbox_request()).await,
        Err(OhttpExchangeError::Ohttp(_))
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
    let exchange = SingleRelay::new(OhttpExchange::new(keys).unwrap(), BrokenRelay);
    let store = Bip77MailboxStore::new(exchange, "https://directory.test").unwrap();
    assert!(matches!(
        Writer::new(&store, [22; 32]).write(&[1]).await,
        Err(fungi_mailbox::Error::Store(StoreError::Exchange(
            OhttpExchangeError::Relay(_)
        )))
    ));
    assert!(matches!(
        Reader::new(&store, [22; 32]).read().await,
        Err(fungi_mailbox::Error::Store(StoreError::Exchange(
            OhttpExchangeError::Relay(_)
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
        let exchange = OhttpExchange::new(keys).unwrap();
        let relay = ResponseRelay {
            server: relay.server,
            plaintext,
            requests: requests.clone(),
        };
        let error = exchange
            .exchange(
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
            assert!(matches!(error, OhttpExchangeError::MissingStatus));
        } else {
            assert!(matches!(error, OhttpExchangeError::Bhttp(_)));
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
    assert!(OhttpExchange::new(Vec::new()).is_err());
}

#[tokio::test(start_paused = true)]
async fn empty_ohttp_polls_can_be_cancelled_without_skipping_messages() {
    use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel};
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Waker};

    let (exchange, _) = ohttp_loopback();
    let mut builder = Builder::new(exchange, "https://directory.test").unwrap();
    let mut sender = builder.build(&[18; 32]).await.unwrap();
    let mut receiver = builder.receiver([18; 32]);
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
    use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel, Unspecified};

    fn unspecified<C: SendChannel<Privacy = Unspecified>>(_: &C) {}
    let (exchange, _) = ohttp_loopback();
    let mut builder = Builder::new(exchange, "https://directory.test").unwrap();
    let mut sender = builder.build(&[25; 32]).await.unwrap();
    unspecified(&sender);
    let mut receiver = builder.receiver([25; 32]);
    sender.send(b"before".to_vec()).await.unwrap();
    assert_eq!(receiver.recv().await.unwrap(), b"before");
    let (send_slot_id, receive_slot_id) = (sender.next_slot_id(), receiver.next_slot_id());

    let mut sender = builder.resume_sender([25; 32], send_slot_id);
    let mut receiver = builder.resume_receiver([25; 32], receive_slot_id);
    sender.send(b"after".to_vec()).await.unwrap();
    assert_eq!(receiver.recv().await.unwrap(), b"after");
}
