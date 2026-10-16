use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use fungi_mailboxes::{Directory, PAYLOAD_BYTES, Reader, Writer, derive_mailbox_secret};

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
