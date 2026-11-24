//! In-memory directories for tests.

use std::collections::{HashMap, hash_map::Entry};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Wake, Waker};
use std::time::Duration;

use tokio::sync::Notify;

use crate::{Directory, MailboxId};

/// First-write-wins directory with timed long polling.
#[derive(Debug)]
pub(crate) struct MemDirectory {
    mailboxes: Mutex<HashMap<MailboxId, Vec<u8>>>,
    notify: Notify,
    poll_timeout: Duration,
}

impl MemDirectory {
    pub(crate) fn payloads(&self) -> Vec<Vec<u8>> {
        self.mailboxes.lock().unwrap().values().cloned().collect()
    }

    pub(crate) fn replace(&self, mailbox: MailboxId, payload: Vec<u8>) {
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

    async fn put(&self, mailbox: MailboxId, message: &[u8]) -> Result<(), Infallible> {
        if let Entry::Vacant(entry) = self.mailboxes.lock().unwrap().entry(mailbox) {
            entry.insert(message.to_vec());
        }
        self.notify.notify_waiters();
        Ok(())
    }

    async fn get(&self, mailbox: MailboxId) -> Result<Option<Vec<u8>>, Infallible> {
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
pub(crate) struct LostAcknowledgement {
    inner: MemDirectory,
    pub(crate) lose_next: AtomicBool,
}

impl Directory for LostAcknowledgement {
    type Error = std::io::Error;

    async fn put(&self, mailbox: MailboxId, message: &[u8]) -> std::io::Result<()> {
        self.inner.put(mailbox, message).await.unwrap();
        if self.lose_next.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("response lost"));
        }
        Ok(())
    }

    async fn get(&self, mailbox: MailboxId) -> std::io::Result<Option<Vec<u8>>> {
        Ok(self.inner.get(mailbox).await.unwrap())
    }
}

/// Records whether a waker was woken.
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Poll `future` once and check that it waits for something else to wake
/// it, rather than waking itself to poll again.
///
/// A future that wakes itself never lets paused time advance, so a timeout
/// around it would hang instead of failing.
pub(crate) fn expect_waiting<F: Future>(future: Pin<&mut F>) {
    let flag = Arc::new(WakeFlag(AtomicBool::new(false)));
    let waker = Waker::from(flag.clone());
    let poll = future.poll(&mut Context::from_waker(&waker));
    assert!(poll.is_pending(), "future completed instead of waiting");
    assert!(
        !flag.0.load(Ordering::SeqCst),
        "future woke itself instead of waiting"
    );
}

// These reads use a populated in-memory directory and must not wait for a write.
pub(crate) async fn expect_ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        let result = future.as_mut().poll(cx);
        assert!(result.is_ready(), "stored message was not returned");
        result
    })
    .await
}
