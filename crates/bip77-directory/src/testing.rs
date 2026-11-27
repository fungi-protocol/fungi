//! OHTTP relay and directory gateway in one process, for tests.

use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Cursor;
use std::sync::Mutex;

use bhttp::{Message, Mode, StatusCode};
use bitcoin_ohttp::hpke::{Aead, Kdf, Kem};
use bitcoin_ohttp::{KeyConfig, Server, SymmetricSuite};
use fungi_mailboxes::{Sequence, Sequences};
use fungi_ohttp::mailbox_api::{Client, ENCAPSULATED_MESSAGE_BYTES};
use fungi_ohttp::{OhttpRequest, Url};

use crate::{Bip77Directory, Relay};

/// Response BHTTP size that encapsulates to [`ENCAPSULATED_MESSAGE_BYTES`]:
/// a 32-byte nonce and a 16-byte AEAD tag are added.
const PADDED_BHTTP_RESPONSE_BYTES: usize = ENCAPSULATED_MESSAGE_BYTES - 32 - 16;

/// Relays each request to a gateway that stores mailboxes as payjoin-mailroom
/// does: the first POST to a path wins, and GET on an empty path answers 202.
#[derive(Debug)]
pub(crate) struct Loopback {
    server: Server,
    /// Status that replaces every answer, if set.
    status: Option<u16>,
    mailboxes: Mutex<HashMap<String, Vec<u8>>>,
    requests: Mutex<Vec<Vec<u8>>>,
}

impl Default for Loopback {
    fn default() -> Self {
        let config = KeyConfig::new(
            1,
            Kem::K256Sha256,
            vec![SymmetricSuite::new(Kdf::HkdfSha256, Aead::ChaCha20Poly1305)],
        )
        .unwrap();
        Self {
            server: Server::new(config).unwrap(),
            status: None,
            mailboxes: Mutex::new(HashMap::new()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl Loopback {
    /// A gateway that answers every request with `status`.
    pub(crate) fn answering(status: u16) -> Self {
        Self {
            status: Some(status),
            ..Self::default()
        }
    }

    pub(crate) fn client(&self) -> Client {
        Client::new(
            Url::parse("https://relay.test/").unwrap(),
            Url::parse("https://directory.test/").unwrap(),
            self.server.config().clone().into(),
        )
        .unwrap()
    }

    pub(crate) fn directory(self) -> Bip77Directory<Self> {
        Bip77Directory::new(self.client(), self)
    }

    /// Stored payloads by mailbox path.
    pub(crate) fn mailboxes(&self) -> HashMap<String, Vec<u8>> {
        self.mailboxes.lock().unwrap().clone()
    }

    /// Encapsulated requests in the order received.
    pub(crate) fn requests(&self) -> Vec<Vec<u8>> {
        self.requests.lock().unwrap().clone()
    }

    fn answer(&self, request: &Message) -> (u16, Vec<u8>) {
        let control = request.control();
        let path = String::from_utf8(control.path().unwrap().to_vec()).unwrap();
        let mut mailboxes = self.mailboxes.lock().unwrap();
        match control.method().unwrap() {
            b"POST" => {
                mailboxes
                    .entry(path)
                    .or_insert_with(|| request.content().to_vec());
                (200, Vec::new())
            }
            b"GET" => match mailboxes.get(&path) {
                Some(payload) => (200, payload.clone()),
                None => (202, Vec::new()),
            },
            method => panic!("unexpected method {method:?}"),
        }
    }
}

impl Relay for Loopback {
    type Error = Infallible;

    async fn post(&self, request: OhttpRequest) -> Result<Vec<u8>, Infallible> {
        assert_eq!(request.url.as_str(), "https://relay.test/");
        self.requests.lock().unwrap().push(request.body.clone());

        let (bhttp, context) = self.server.decapsulate(&request.body).unwrap();
        let (status, body) = self.answer(&Message::read_bhttp(&mut Cursor::new(bhttp)).unwrap());
        let mut response =
            Message::response(StatusCode::try_from(self.status.unwrap_or(status)).unwrap());
        response.write_content(&body);
        let mut padded = Vec::new();
        response
            .write_bhttp(Mode::KnownLength, &mut padded)
            .unwrap();
        padded.resize(PADDED_BHTTP_RESPONSE_BYTES, 0);
        Ok(context.encapsulate(&padded).unwrap())
    }
}

/// A relay that cannot be reached.
#[derive(Debug)]
pub(crate) struct BrokenRelay;

impl Relay for BrokenRelay {
    type Error = std::io::Error;

    async fn post(&self, _: OhttpRequest) -> std::io::Result<Vec<u8>> {
        Err(std::io::Error::other("relay unavailable"))
    }
}

/// The initiator-to-responder sequence derived from a shared secret of
/// `byte` repeated.
pub(crate) fn sequence(byte: u8) -> Sequence {
    Sequences::derive(&[byte; 32]).initiator_to_responder
}
