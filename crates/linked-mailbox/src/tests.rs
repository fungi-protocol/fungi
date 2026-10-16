use std::io::Cursor;

use bhttp::{Message, Mode, StatusCode};
use bitcoin_ohttp::hpke::{Aead, Kdf, Kem};
use bitcoin_ohttp::{Server, ServerResponse, SymmetricSuite};
use proptest::prelude::*;

use crate::ShortId;
use crate::client::*;

const BECH32_UPPER: &[u8] = b"QPZRY9X8GF2TVDW0S3JN54KHCE6MUA7L";

/// Response BHTTP size that encapsulates to [`ENCAPSULATED_MESSAGE_BYTES`]:
/// a 32-byte nonce and a 16-byte AEAD tag are added.
const PADDED_BHTTP_RESPONSE_BYTES: usize = ENCAPSULATED_MESSAGE_BYTES - 32 - 16;

/// A directory gateway.
fn gateway() -> Server {
    let config = bitcoin_ohttp::KeyConfig::new(
        1,
        Kem::K256Sha256,
        vec![SymmetricSuite::new(Kdf::HkdfSha256, Aead::ChaCha20Poly1305)],
    )
    .unwrap();
    Server::new(config).unwrap()
}

fn try_client(gateway: &Server, directory: &str) -> Result<Client, UnsupportedDirectory> {
    Client::new(
        Url::parse("https://relay.example/").unwrap(),
        Url::parse(directory).unwrap(),
        gateway.config().clone().into(),
    )
}

fn client(gateway: &Server, directory: &str) -> Client {
    try_client(gateway, directory).unwrap()
}

/// Decapsulate `request` as the gateway, returning the padded BHTTP request.
fn receive(gateway: &Server, request: &OhttpRequest) -> (Vec<u8>, ServerResponse) {
    gateway.decapsulate(&request.body).unwrap()
}

fn read_request(bhttp: &[u8]) -> Message {
    Message::read_bhttp(&mut Cursor::new(bhttp)).unwrap()
}

/// Encapsulate a response as the gateway, padded as BIP77 requires.
fn respond(context: ServerResponse, status: u16, body: &[u8]) -> Vec<u8> {
    respond_padded(context, status, body, PADDED_BHTTP_RESPONSE_BYTES)
}

fn respond_padded(context: ServerResponse, status: u16, body: &[u8], padded: usize) -> Vec<u8> {
    let mut message = Message::response(StatusCode::try_from(status).unwrap());
    message.write_content(body);
    let mut bhttp = Vec::new();
    message.write_bhttp(Mode::KnownLength, &mut bhttp).unwrap();
    bhttp.resize(padded, 0);
    context.encapsulate(&bhttp).unwrap()
}

#[test]
fn short_id_vectors() {
    assert_eq!(ShortId([0; 8]).to_string(), "QQQQQQQQQQQQQ");
    // 64 one bits and one zero padding bit.
    assert_eq!(ShortId([0xff; 8]).to_string(), "LLLLLLLLLLLL7");
}

#[test]
fn get_requests_the_mailbox_path() {
    let gateway = gateway();
    let id = ShortId([1; 8]);
    let (request, _) = client(&gateway, "https://directory.example/")
        .get(id)
        .unwrap();
    assert_eq!(request.url.as_str(), "https://relay.example/");
    assert_eq!(request.content_type, "message/ohttp-req");

    let (bhttp, _) = receive(&gateway, &request);
    let inner = read_request(&bhttp);
    let control = inner.control();
    assert_eq!(control.method(), Some(&b"GET"[..]));
    assert_eq!(control.authority(), Some(&b"directory.example"[..]));
    assert_eq!(control.path(), Some(format!("/{id}").as_bytes()));
    assert!(inner.content().is_empty());
}

#[test]
fn post_sends_the_payload_to_the_mailbox_path() {
    let gateway = gateway();
    let id = ShortId([2; 8]);
    let (request, _) = client(&gateway, "https://directory.example/base/")
        .post(id, b"payload")
        .unwrap();

    let (bhttp, _) = receive(&gateway, &request);
    let inner = read_request(&bhttp);
    let control = inner.control();
    assert_eq!(control.method(), Some(&b"POST"[..]));
    assert_eq!(control.path(), Some(format!("/base/{id}").as_bytes()));
    assert_eq!(inner.content(), b"payload");
}

#[test]
fn requests_are_padded() {
    let gateway = gateway();
    let client = client(&gateway, "https://directory.example/");
    let (get, _) = client.get(ShortId([3; 8])).unwrap();
    let (post, _) = client.post(ShortId([3; 8]), &[0; 7000]).unwrap();
    assert_eq!(get.body.len(), ENCAPSULATED_MESSAGE_BYTES);
    assert_eq!(post.body.len(), ENCAPSULATED_MESSAGE_BYTES);
    assert_eq!(receive(&gateway, &get).0.len(), PADDED_BHTTP_REQUEST_BYTES);
    assert_eq!(receive(&gateway, &post).0.len(), PADDED_BHTTP_REQUEST_BYTES);
}

#[test]
fn oversized_payload_is_rejected() {
    let gateway = gateway();
    let result = client(&gateway, "https://directory.example/")
        .post(ShortId([4; 8]), &[0; PADDED_BHTTP_REQUEST_BYTES]);
    assert!(matches!(
        result,
        Err(Error::Ohttp(
            ohttp_client::Error::KnownLengthTooSmall { .. }
        ))
    ));
}

#[test]
fn get_distinguishes_payload_from_timeout() {
    let gateway = gateway();
    let client = client(&gateway, "https://directory.example/");
    let finish = |status, body: &[u8]| {
        let (request, context) = client.get(ShortId([5; 8])).unwrap();
        let (_, response) = receive(&gateway, &request);
        context.finish(&respond(response, status, body))
    };
    assert_eq!(finish(200, b"payload").unwrap(), Some(b"payload".to_vec()));
    assert_eq!(finish(200, b"").unwrap(), Some(Vec::new()));
    assert_eq!(finish(202, b"").unwrap(), None);
}

#[test]
fn post_accepts_ok() {
    let gateway = gateway();
    let (request, context) = client(&gateway, "https://directory.example/")
        .post(ShortId([6; 8]), b"payload")
        .unwrap();
    let (_, response) = receive(&gateway, &request);
    context.finish(&respond(response, 200, b"")).unwrap();
}

#[test]
fn tampered_response_is_rejected() {
    let gateway = gateway();
    let (request, context) = client(&gateway, "https://directory.example/")
        .get(ShortId([7; 8]))
        .unwrap();
    let (_, response) = receive(&gateway, &request);
    let mut response = respond(response, 200, b"payload");
    *response.last_mut().unwrap() ^= 1;
    assert!(matches!(context.finish(&response), Err(Error::Ohttp(_))));
}

#[test]
fn wrong_size_response_is_rejected() {
    let gateway = gateway();
    let client = client(&gateway, "https://directory.example/");
    for padded in [
        PADDED_BHTTP_RESPONSE_BYTES - 1,
        PADDED_BHTTP_RESPONSE_BYTES + 1,
    ] {
        let (request, context) = client.get(ShortId([9; 8])).unwrap();
        let (_, response) = receive(&gateway, &request);
        let response = respond_padded(response, 200, b"payload", padded);
        assert!(matches!(
            context.finish(&response),
            Err(Error::ResponseSize(size)) if size == response.len()
        ));
    }
}

#[test]
fn unsupported_directory_urls_are_rejected() {
    let gateway = gateway();
    for directory in [
        "ftp://directory.example/",
        "mailto:directory@example.com",
        "https://user@directory.example/",
        "https://user:password@directory.example/",
        "https://directory.example/#fragment",
    ] {
        assert_eq!(
            try_client(&gateway, directory).err(),
            Some(UnsupportedDirectory),
            "{directory}"
        );
    }
    for directory in [
        "http://directory.example",
        "https://directory.example:8443/base?query",
    ] {
        assert!(try_client(&gateway, directory).is_ok(), "{directory}");
    }
}

proptest! {
    #[test]
    fn short_id_is_13_uppercase_bech32_chars(bytes: [u8; 8]) {
        let id = ShortId(bytes).to_string();
        prop_assert_eq!(id.len(), 13);
        prop_assert!(id.bytes().all(|c| BECH32_UPPER.contains(&c)));
    }

    #[test]
    fn distinct_short_ids_display_distinctly(a: [u8; 8], b: [u8; 8]) {
        prop_assume!(a != b);
        prop_assert_ne!(ShortId(a).to_string(), ShortId(b).to_string());
    }

    #[test]
    fn statuses_not_defined_by_bip77_are_errors(status in 200u16..600) {
        let gateway = gateway();
        let client = client(&gateway, "https://directory.example/");

        let (request, context) = client.get(ShortId([8; 8])).unwrap();
        let (_, response) = receive(&gateway, &request);
        let result = context.finish(&respond(response, status, b""));
        if !matches!(status, 200 | 202) {
            prop_assert!(matches!(result, Err(Error::UnexpectedStatus(s)) if s == status));
        }

        let (request, context) = client.post(ShortId([8; 8]), b"").unwrap();
        let (_, response) = receive(&gateway, &request);
        let result = context.finish(&respond(response, status, b""));
        if status != 200 {
            prop_assert!(matches!(result, Err(Error::UnexpectedStatus(s)) if s == status));
        }
    }
}
