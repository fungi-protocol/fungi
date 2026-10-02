use std::collections::{HashMap, hash_map::Entry};
use std::convert::Infallible;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fungi_mailbox::{
    Error, MAX_MESSAGE_BYTES, MailboxStore, PAYLOAD_BYTES, PutOutcome, Reader, SlotId, Writer,
    derive_slot_id,
};
use proptest::prelude::*;
use tokio::sync::Notify;

/// First-write-wins store with idempotent writes and timed long polling.
#[derive(Debug)]
struct MemStore {
    slots: Mutex<HashMap<SlotId, Vec<u8>>>,
    notify: Notify,
    poll_timeout: Duration,
}

impl MemStore {
    fn payloads(&self) -> Vec<Vec<u8>> {
        self.slots.lock().unwrap().values().cloned().collect()
    }

    fn replace(&self, slot_id: SlotId, payload: Vec<u8>) {
        self.slots.lock().unwrap().insert(slot_id, payload);
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
            notify: Notify::new(),
            poll_timeout: Duration::from_millis(50),
        }
    }
}

impl MailboxStore for MemStore {
    type Message = Vec<u8>;
    type Error = Infallible;

    async fn put(&self, slot_id: SlotId, message: &Vec<u8>) -> Result<PutOutcome, Infallible> {
        let outcome = match self.slots.lock().unwrap().entry(slot_id) {
            Entry::Occupied(entry) if entry.get() == message => PutOutcome::Stored,
            Entry::Occupied(_) => PutOutcome::Occupied,
            Entry::Vacant(entry) => {
                entry.insert(message.clone());
                PutOutcome::Stored
            }
        };
        self.notify.notify_waiters();
        Ok(outcome)
    }

    async fn get(&self, slot_id: SlotId) -> Result<Option<Vec<u8>>, Infallible> {
        loop {
            // Register before checking the slot to avoid missing a concurrent write.
            let notified = self.notify.notified();
            if let Some(message) = self.slots.lock().unwrap().get(&slot_id).cloned() {
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
    inner: MemStore,
    lose_next: AtomicBool,
}

impl MailboxStore for LostAcknowledgement {
    type Message = Vec<u8>;
    type Error = std::io::Error;

    async fn put(&self, slot_id: SlotId, message: &Vec<u8>) -> std::io::Result<PutOutcome> {
        let outcome = self.inner.put(slot_id, message).await.unwrap();
        if self.lose_next.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("response lost"));
        }
        Ok(outcome)
    }

    async fn get(&self, slot_id: SlotId) -> std::io::Result<Option<Vec<u8>>> {
        Ok(self.inner.get(slot_id).await.unwrap())
    }
}

#[tokio::test(start_paused = true)]
async fn messages_arrive_in_order() {
    let store = MemStore::default();
    let mut writer = Writer::new(&store, [1; 32]);
    let mut reader = Reader::new(&store, [1; 32]);

    for message in [b"first".as_slice(), b"second", b"third"] {
        writer.write(message).await.unwrap();
    }
    for message in [b"first".as_slice(), b"second", b"third"] {
        assert_eq!(reader.read().await.unwrap(), message);
    }
}

#[tokio::test(start_paused = true)]
async fn writer_and_reader_resume_after_a_restart() {
    let store = MemStore::default();
    let secret = [11; 32];
    let mut writer = Writer::new(&store, secret);
    let mut reader = Reader::new(&store, secret);
    writer.write(b"before").await.unwrap();
    assert_eq!(reader.read().await.unwrap(), b"before");
    let (write_index, read_index) = (writer.next_index(), reader.next_index());
    assert_eq!((write_index, read_index), (1, 1));

    assert!(matches!(
        Writer::new(&store, secret).write(b"after").await,
        Err(Error::Occupied)
    ));

    let mut writer = Writer::resume(&store, secret, write_index);
    let mut reader = Reader::resume(&store, secret, read_index);
    writer.write(b"after").await.unwrap();
    assert_eq!(reader.read().await.unwrap(), b"after");
}

#[tokio::test(start_paused = true)]
async fn identical_message_at_a_stale_index_is_merged() {
    let store = MemStore::default();
    let secret = [12; 32];
    Writer::new(&store, secret).write(b"same").await.unwrap();

    // A writer resumed from a stale index cannot tell a new identical
    // message from a resend of the stored one.
    let mut stale = Writer::resume(&store, secret, 0);
    stale.write(b"same").await.unwrap();
    assert_eq!(stale.next_index(), 1);

    let mut reader = Reader::new(&store, secret);
    assert_eq!(reader.read().await.unwrap(), b"same");
    assert!(
        tokio::time::timeout(Duration::from_millis(125), reader.read())
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn read_waits_for_a_message() {
    let store = MemStore::default();
    let mut writer = Writer::new(&store, [2; 32]);
    let mut reader = Reader::new(&store, [2; 32]);

    let read = reader.read();
    tokio::pin!(read);
    assert!(
        tokio::time::timeout(Duration::from_millis(125), &mut read)
            .await
            .is_err()
    );
    writer.write(b"only").await.unwrap();
    assert_eq!(read.await.unwrap(), b"only");
}

#[tokio::test(start_paused = true)]
async fn different_secret_sees_no_messages() {
    let store = MemStore::default();
    Writer::new(&store, [3; 32])
        .write(b"private")
        .await
        .unwrap();

    let mut stranger = Reader::new(&store, [4; 32]);
    assert!(
        tokio::time::timeout(Duration::from_millis(125), stranger.read())
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn payloads_are_fixed_size_and_hide_messages() {
    let store = MemStore::default();
    let mut writer = Writer::new(&store, [5; 32]);
    let message = b"a recognisable message";
    writer.write(message).await.unwrap();
    writer.write(message).await.unwrap();

    let payloads = store.payloads();
    assert_eq!(payloads.len(), 2);
    assert_ne!(payloads[0], payloads[1]);
    for payload in payloads {
        assert_eq!(payload.len(), PAYLOAD_BYTES);
        assert!(!payload.windows(message.len()).any(|w| w == message));
    }
}

#[tokio::test(start_paused = true)]
async fn payload_served_at_another_slot_is_rejected() {
    let store = MemStore::default();
    let secret = [6; 32];
    Writer::new(&store, secret).write(b"slot 0").await.unwrap();

    let moved = store.payloads().pop().unwrap();
    let mut reader = Reader::new(&store, secret);
    reader.read().await.unwrap();
    store.replace(derive_slot_id(&secret, 1), moved);
    assert!(matches!(reader.read().await, Err(Error::InvalidPayload)));
}

#[tokio::test(start_paused = true)]
async fn message_size_is_limited() {
    let store = MemStore::default();
    let mut writer = Writer::new(&store, [7; 32]);
    let mut reader = Reader::new(&store, [7; 32]);

    assert!(matches!(
        writer.write(&vec![0; MAX_MESSAGE_BYTES + 1]).await,
        Err(Error::MessageTooLarge(_))
    ));
    assert!(store.payloads().is_empty());

    for message in [vec![0xab; MAX_MESSAGE_BYTES], Vec::new()] {
        writer.write(&message).await.unwrap();
        assert_eq!(reader.read().await.unwrap(), message);
    }
}

#[tokio::test(start_paused = true)]
async fn retrying_an_unacknowledged_write_does_not_duplicate() {
    let store = LostAcknowledgement::default();
    let mut writer = Writer::new(&store, [8; 32]);
    let mut reader = Reader::new(&store, [8; 32]);

    store.lose_next.store(true, Ordering::SeqCst);
    assert!(matches!(writer.write(b"once").await, Err(Error::Store(_))));
    writer.write(b"once").await.unwrap();
    writer.write(b"next").await.unwrap();

    assert_eq!(reader.read().await.unwrap(), b"once");
    assert_eq!(reader.read().await.unwrap(), b"next");
}

#[tokio::test(start_paused = true)]
async fn a_different_message_after_an_unacknowledged_write_is_occupied() {
    let store = LostAcknowledgement::default();
    let mut writer = Writer::new(&store, [9; 32]);

    store.lose_next.store(true, Ordering::SeqCst);
    assert!(writer.write(b"stored").await.is_err());
    assert!(matches!(writer.write(b"other").await, Err(Error::Occupied)));
}

#[tokio::test(start_paused = true)]
async fn a_foreign_payload_in_the_next_slot_is_occupied() {
    let store = MemStore::default();
    let secret = [10; 32];
    store.replace(derive_slot_id(&secret, 0), vec![0; PAYLOAD_BYTES]);

    assert!(matches!(
        Writer::new(&store, secret).write(b"mine").await,
        Err(Error::Occupied)
    ));
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
            let store = MemStore::default();
            Writer::new(&store, secret).write(&message).await.unwrap();
            Reader::new(&store, secret).read().await.unwrap()
        });
        prop_assert_eq!(received, message);
    }
}
