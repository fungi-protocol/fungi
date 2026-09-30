//! OHTTP loopback relay for tests.

use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use crate::{ENCAPSULATED_MESSAGE_BYTES, OhttpClient, Relay, SingleRelay};

pub(crate) type Ciphertexts = Arc<Mutex<Vec<Vec<u8>>>>;

#[derive(Debug)]
pub(crate) struct LoopbackRelay {
    pub(crate) server: ::ohttp::Server,
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

pub(crate) fn loopback_relay() -> (LoopbackRelay, Vec<u8>, Ciphertexts) {
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

pub(crate) fn ohttp_loopback() -> (SingleRelay<LoopbackRelay>, Ciphertexts) {
    let (relay, keys, ciphertexts) = loopback_relay();
    let client = OhttpClient::new(keys).unwrap();
    (SingleRelay::new(client, relay), ciphertexts)
}
