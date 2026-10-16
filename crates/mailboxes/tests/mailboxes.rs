use std::collections::{HashMap, hash_map::Entry};
use std::convert::Infallible;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fungi_mailboxes::{
    Directory, Error, MAX_MESSAGE_BYTES, MailboxSecret, PAYLOAD_BYTES, Reader, Writer,
    derive_mailbox_secret,
};
use proptest::prelude::*;
use tokio::sync::Notify;

/// First-write-wins directory with timed long polling.
#[derive(Debug)]
struct MemDirectory {
    mailboxes: Mutex<HashMap<MailboxSecret, Vec<u8>>>,
    notify: Notify,
    poll_timeout: Duration,
}

impl MemDirectory {
    fn payloads(&self) -> Vec<Vec<u8>> {
        self.mailboxes.lock().unwrap().values().cloned().collect()
    }

    fn replace(&self, mailbox: MailboxSecret, payload: Vec<u8>) {
        self.mailboxes.lock().unwrap().insert(mailbox, payload);
    }
}

impl Default for MemDirectory {
    fn default() -> Self {
        Self {
            mailboxes: Mutex::new(HashMap::new()),
            notify: Notify::new(),
            poll_timeout: Duration::from_millis(50),
        }
    }
}

impl Directory for MemDirectory {
    type Error = Infallible;

    async fn put(&self, mailbox: MailboxSecret, message: &[u8]) -> Result<(), Infallible> {
        if let Entry::Vacant(entry) = self.mailboxes.lock().unwrap().entry(mailbox) {
            entry.insert(message.to_vec());
        }
        self.notify.notify_waiters();
        Ok(())
    }

    async fn get(&self, mailbox: MailboxSecret) -> Result<Option<Vec<u8>>, Infallible> {
        loop {
            // Register before checking the mailbox to avoid missing a concurrent write.
            let notified = self.notify.notified();
            if let Some(message) = self.mailboxes.lock().unwrap().get(&mailbox).cloned() {
                return Ok(Some(message));
            }
            if tokio::time::timeout(self.poll_timeout, notified)
                .await
                .is_err()
            {
                return Ok(None);
            }
        }
    }
}

/// Stores the next payload but reports failure, as when a response is lost.
#[derive(Debug, Default)]
struct LostAcknowledgement {
    inner: MemDirectory,
    lose_next: AtomicBool,
}

impl Directory for LostAcknowledgement {
    type Error = std::io::Error;

    async fn put(&self, mailbox: MailboxSecret, message: &[u8]) -> std::io::Result<()> {
        self.inner.put(mailbox, message).await.unwrap();
        if self.lose_next.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("response lost"));
        }
        Ok(())
    }

    async fn get(&self, mailbox: MailboxSecret) -> std::io::Result<Option<Vec<u8>>> {
        Ok(self.inner.get(mailbox).await.unwrap())
    }
}

#[tokio::test(start_paused = true)]
async fn messages_arrive_in_order() {
    let directory = MemDirectory::default();
    let mut writer = Writer::new(&directory, [1; 32]);
    let mut reader = Reader::new(&directory, [1; 32]);

    for message in [b"first".as_slice(), b"second", b"third"] {
        writer.write(message).await.unwrap();
    }
    for message in [b"first".as_slice(), b"second", b"third"] {
        assert_eq!(expect_ready(reader.read()).await.unwrap(), message);
    }
}

#[tokio::test(start_paused = true)]
async fn writer_and_reader_resume_after_a_restart() {
    let directory = MemDirectory::default();
    let secret = [11; 32];
    let mut writer = Writer::new(&directory, secret);
    let mut reader = Reader::new(&directory, secret);
    assert_eq!(writer.next_index(), 0);
    writer.write(b"before").await.unwrap();
    assert_eq!(expect_ready(reader.read()).await.unwrap(), b"before");
    let (write_index, read_index) = (writer.next_index(), reader.next_index());
    assert_eq!((write_index, read_index), (1, 1));

    let mut writer = Writer::resume(&directory, secret, write_index);
    let mut reader = Reader::resume(&directory, secret, read_index);
    writer.write(b"after").await.unwrap();
    assert_eq!(expect_ready(reader.read()).await.unwrap(), b"after");
    assert_eq!(writer.next_index(), 2);
}

#[tokio::test(start_paused = true)]
async fn identical_message_at_a_stale_index_is_merged() {
    let directory = MemDirectory::default();
    let secret = [12; 32];
    Writer::new(&directory, secret)
        .write(b"same")
        .await
        .unwrap();

    // A writer resumed from a stale index cannot tell a new identical
    // message from a resend of the stored one.
    let mut stale = Writer::resume(&directory, secret, 0);
    stale.write(b"same").await.unwrap();
    assert_eq!(stale.next_index(), 1);

    let mut reader = Reader::new(&directory, secret);
    assert_eq!(expect_ready(reader.read()).await.unwrap(), b"same");
    assert!(
        tokio::time::timeout(Duration::from_millis(125), reader.read())
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn read_waits_for_a_message() {
    let directory = MemDirectory::default();
    let mut writer = Writer::new(&directory, [2; 32]);
    let mut reader = Reader::new(&directory, [2; 32]);

    let read = reader.read();
    tokio::pin!(read);
    assert!(
        tokio::time::timeout(Duration::from_millis(125), &mut read)
            .await
            .is_err()
    );
    writer.write(b"only").await.unwrap();
    assert_eq!(expect_ready(read).await.unwrap(), b"only");
}

#[tokio::test(start_paused = true)]
async fn different_secret_sees_no_messages() {
    let directory = MemDirectory::default();
    Writer::new(&directory, [3; 32])
        .write(b"private")
        .await
        .unwrap();

    let mut stranger = Reader::new(&directory, [4; 32]);
    assert!(
        tokio::time::timeout(Duration::from_millis(125), stranger.read())
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn payloads_are_fixed_size_and_hide_messages() {
    let directory = MemDirectory::default();
    let mut writer = Writer::new(&directory, [5; 32]);
    let message = b"a recognisable message";
    writer.write(message).await.unwrap();
    writer.write(message).await.unwrap();

    let payloads = directory.payloads();
    assert_eq!(payloads.len(), 2);
    assert_ne!(payloads[0], payloads[1]);
    for payload in payloads {
        assert_eq!(payload.len(), PAYLOAD_BYTES);
        assert!(!payload.windows(message.len()).any(|w| w == message));
    }
}

#[tokio::test(start_paused = true)]
async fn payload_served_at_another_mailbox_is_rejected() {
    let directory = MemDirectory::default();
    let secret = [6; 32];
    Writer::new(&directory, secret)
        .write(b"mailbox 0")
        .await
        .unwrap();

    let moved = directory.payloads().pop().unwrap();
    let mut reader = Reader::new(&directory, secret);
    expect_ready(reader.read()).await.unwrap();
    directory.replace(derive_mailbox_secret(&secret, 1), moved);
    assert!(matches!(
        expect_ready(reader.read()).await,
        Err(Error::InvalidPayload)
    ));
}

#[tokio::test(start_paused = true)]
async fn message_size_is_limited() {
    assert_eq!(MAX_MESSAGE_BYTES, 7086);
    let directory = MemDirectory::default();
    let mut writer = Writer::new(&directory, [7; 32]);
    let mut reader = Reader::new(&directory, [7; 32]);

    assert!(matches!(
        writer.write(&vec![0; MAX_MESSAGE_BYTES + 1]).await,
        Err(Error::MessageTooLarge(_))
    ));
    assert!(directory.payloads().is_empty());

    for message in [vec![0xab; MAX_MESSAGE_BYTES], Vec::new()] {
        writer.write(&message).await.unwrap();
        assert_eq!(expect_ready(reader.read()).await.unwrap(), message);
    }
}

#[tokio::test(start_paused = true)]
async fn retrying_an_unacknowledged_write_does_not_duplicate() {
    let directory = LostAcknowledgement::default();
    let mut writer = Writer::new(&directory, [8; 32]);
    let mut reader = Reader::new(&directory, [8; 32]);

    directory.lose_next.store(true, Ordering::SeqCst);
    assert!(matches!(
        writer.write(b"once").await,
        Err(Error::Directory(_))
    ));
    writer.write(b"once").await.unwrap();
    writer.write(b"next").await.unwrap();

    assert_eq!(expect_ready(reader.read()).await.unwrap(), b"once");
    assert_eq!(expect_ready(reader.read()).await.unwrap(), b"next");
}

#[tokio::test(start_paused = true)]
async fn a_different_message_after_an_unacknowledged_write_is_lost() {
    let directory = LostAcknowledgement::default();
    let mut writer = Writer::new(&directory, [9; 32]);
    let mut reader = Reader::new(&directory, [9; 32]);

    directory.lose_next.store(true, Ordering::SeqCst);
    assert!(writer.write(b"stored").await.is_err());
    writer.write(b"other").await.unwrap();

    assert_eq!(expect_ready(reader.read()).await.unwrap(), b"stored");
}

#[tokio::test(start_paused = true)]
async fn a_write_to_an_occupied_mailbox_is_lost() {
    let directory = MemDirectory::default();
    let secret = [10; 32];
    directory.replace(derive_mailbox_secret(&secret, 0), vec![0; PAYLOAD_BYTES]);
    Writer::new(&directory, secret)
        .write(b"mine")
        .await
        .unwrap();

    assert!(matches!(
        expect_ready(Reader::new(&directory, secret).read()).await,
        Err(Error::InvalidPayload)
    ));
}

#[test]
fn debug_output_shows_position_without_secrets_or_directory() {
    let directory = MemDirectory::default();
    let writer = Writer::resume(&directory, [42; 32], 7);
    let reader = Reader::resume(&directory, [42; 32], 9);

    assert_eq!(format!("{writer:?}"), "Writer { next_index: 7, .. }");
    assert_eq!(format!("{reader:?}"), "Reader { next_index: 9, .. }");
}

#[test]
fn mailbox_secret_bytes_match_the_hkdf_vector() {
    assert_eq!(
        derive_mailbox_secret(&[1; 32], 0).to_bytes(),
        [
            0x38, 0x2e, 0xd9, 0x9d, 0x2e, 0x4b, 0x66, 0xb5, 0x76, 0x72, 0x52, 0xd4, 0x2d, 0xa9,
            0x04, 0x34, 0x37, 0x0a, 0x45, 0x60, 0xc3, 0x08, 0x45, 0x61, 0x0e, 0xfb, 0x83, 0xb0,
            0x4f, 0x42, 0xbb, 0x4d,
        ]
    );
}

#[tokio::test]
async fn wrong_payload_sizes_do_not_advance_the_reader() {
    let directory = MemDirectory::default();
    let secret = [13; 32];
    let mailbox = derive_mailbox_secret(&secret, 0);
    Writer::new(&directory, secret)
        .write(b"valid")
        .await
        .unwrap();
    let payload = directory.payloads().pop().unwrap();
    let mut reader = Reader::new(&directory, secret);

    for size in [0, PAYLOAD_BYTES - 1, PAYLOAD_BYTES + 1] {
        let mut malformed = payload.clone();
        malformed.resize(size, 0);
        directory.replace(mailbox, malformed);
        assert!(matches!(
            expect_ready(reader.read()).await,
            Err(Error::InvalidPayload)
        ));
        assert_eq!(reader.next_index(), 0);
    }

    directory.replace(mailbox, payload);
    assert_eq!(expect_ready(reader.read()).await.unwrap(), b"valid");
    assert_eq!(reader.next_index(), 1);
}

// These reads use a populated in-memory directory and must not wait for a write.
async fn expect_ready<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        let result = future.as_mut().poll(cx);
        assert!(result.is_ready(), "stored message was not returned");
        result
    })
    .await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn any_message_that_fits_round_trips(
        secret: [u8; 32],
        message in proptest::collection::vec(any::<u8>(), 0..=MAX_MESSAGE_BYTES),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let received = runtime.block_on(async {
            let directory = MemDirectory::default();
            Writer::new(&directory, secret).write(&message).await.unwrap();
            expect_ready(Reader::new(&directory, secret).read()).await.unwrap()
        });
        prop_assert_eq!(received, message);
    }
}
