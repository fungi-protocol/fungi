//! Helpers shared by the tests.

use std::time::Duration;

use crate::onion;

/// The part of a well-formed onion hostname before `.onion`.
pub(crate) const SERVICE: &str = "duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twagswzczad";

pub(crate) fn addr() -> onion::Addr {
    onion::Addr::new(format!("{SERVICE}.onion"), 9735).unwrap()
}

/// What a proxy answers to choose username/password authentication and
/// accept the credentials.
pub(crate) const CREDENTIALS_ACCEPTED: [u8; 4] = [5, 2, 1, 0];

/// The bytes a client sends to reach [`addr`]: the greeting, the credentials
/// and the request.
pub(crate) const HANDSHAKE_LEN: usize = 4 + 28 + 69;

/// Fail instead of waiting on a peer that will never write.
pub(crate) async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .unwrap()
}
