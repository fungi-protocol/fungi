//! The client side of the daemon's control port, as far as publishing an
//! onion service needs it.

use std::{fmt, io};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::listener::ListenError;
use crate::onion;

/// How to authenticate on the control port.
pub enum ControlAuth {
    /// The daemon asks for no credentials.
    Null,
    /// The daemon asks for the contents of its `control_auth_cookie` file.
    ///
    /// The cookie never crosses the connection: the daemon first proves it
    /// knows the cookie, then this side does (`SAFECOOKIE`). Sending the
    /// cookie itself would hand it to whatever answers on the port.
    Cookie(Vec<u8>),
}

/// Leaves out the cookie, which grants control of the daemon.
impl fmt::Debug for ControlAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("Null"),
            Self::Cookie(_) => f.write_str("Cookie(..)"),
        }
    }
}

pub(crate) async fn authenticate<C: AsyncBufRead + AsyncWrite + Unpin>(
    control: &mut C,
    auth: &ControlAuth,
) -> Result<(), ListenError> {
    let command = match auth {
        ControlAuth::Null => "AUTHENTICATE\r\n".to_owned(),
        ControlAuth::Cookie(cookie) => {
            format!(
                "AUTHENTICATE {}\r\n",
                hex(&safecookie(control, cookie).await?)
            )
        }
    };
    control.write_all(command.as_bytes()).await?;

    let line = read_line(control).await?;
    match Reply::parse(&line) {
        Some(Reply::Last(_)) => Ok(()),
        _ => Err(ListenError::Refused(line)),
    }
}

/// The length of every control cookie.
const COOKIE_LEN: usize = 32;

/// The length of each side's nonce, and of each hash.
const NONCE_LEN: usize = 32;

const SERVER_TO_CONTROLLER: &[u8] = b"Tor safe cookie authentication server-to-controller hash";
const CONTROLLER_TO_SERVER: &[u8] = b"Tor safe cookie authentication controller-to-server hash";

/// Challenge the daemon to prove it knows `cookie`, and return the proof
/// that this side does.
async fn safecookie<C: AsyncBufRead + AsyncWrite + Unpin>(
    control: &mut C,
    cookie: &[u8],
) -> Result<[u8; NONCE_LEN], ListenError> {
    if cookie.len() != COOKIE_LEN {
        return Err(ListenError::CookieLength(cookie.len()));
    }
    let mut client_nonce = [0; NONCE_LEN];
    getrandom::fill(&mut client_nonce).map_err(io::Error::from)?;
    let command = format!("AUTHCHALLENGE SAFECOOKIE {}\r\n", hex(&client_nonce));
    control.write_all(command.as_bytes()).await?;

    let line = read_line(control).await?;
    let challenge = line.strip_prefix("250 AUTHCHALLENGE ").and_then(|fields| {
        let (hash, nonce) = fields.split_once(' ')?;
        let hash = unhex(hash.strip_prefix("SERVERHASH=")?)?;
        let nonce = unhex(nonce.strip_prefix("SERVERNONCE=")?)?;
        Some((hash, nonce))
    });
    let Some((server_hash, server_nonce)) = challenge else {
        return Err(ListenError::Refused(line));
    };

    let message = [cookie, &client_nonce, &server_nonce];
    mac(SERVER_TO_CONTROLLER, &message)
        .verify_slice(&server_hash)
        .map_err(|_| ListenError::UnknownCookie)?;
    Ok(mac(CONTROLLER_TO_SERVER, &message)
        .finalize()
        .into_bytes()
        .into())
}

/// HMAC-SHA256 under `key` of the concatenated `parts`.
fn mac(key: &[u8], parts: &[&[u8]]) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes keys of any length");
    for part in parts {
        mac.update(part);
    }
    mac
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

/// The 32 bytes spelled by `text` in hex, in either case.
fn unhex(text: &str) -> Option<[u8; NONCE_LEN]> {
    if text.len() != 2 * NONCE_LEN {
        return None;
    }
    let mut bytes = [0; NONCE_LEN];
    for (byte, pair) in bytes.iter_mut().zip(text.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(bytes)
}

/// Publish a service forwarding `virt_port` to `local_port` on this host.
///
/// The key is discarded, so the address cannot be published again, and the
/// service lasts until `control` closes.
pub(crate) async fn add_onion<C: AsyncBufRead + AsyncWrite + Unpin>(
    control: &mut C,
    virt_port: u16,
    local_port: u16,
) -> Result<onion::Addr, ListenError> {
    let command = format!(
        "ADD_ONION NEW:ED25519-V3 Flags=DiscardPK Port={virt_port},127.0.0.1:{local_port}\r\n"
    );
    control.write_all(command.as_bytes()).await?;

    let mut service = None;
    loop {
        let line = read_line(control).await?;
        match Reply::parse(&line) {
            Some(Reply::More(text)) => {
                if let Some(id) = text.strip_prefix("ServiceID=") {
                    let addr = onion::Addr::new(format!("{id}.onion"), virt_port);
                    service = Some(addr.map_err(ListenError::ServiceId)?);
                }
            }
            // The daemon sends nothing after its last line, so anything but
            // success has to end the wait here.
            Some(Reply::Last("OK")) => return service.ok_or(ListenError::Refused(line)),
            _ => return Err(ListenError::Refused(line)),
        }
    }
}

/// One line of a successful answer.
enum Reply<'a> {
    /// `250-text`: more lines follow.
    More(&'a str),
    /// `250 text`: the answer ends here.
    Last(&'a str),
}

impl<'a> Reply<'a> {
    fn parse(line: &'a str) -> Option<Self> {
        let more = line.strip_prefix("250-").map(Reply::More);
        let last = line.strip_prefix("250 ").map(Reply::Last);

        more.or(last)
    }
}

/// The longest line read from the control port, line ending included.
const MAX_LINE_LEN: usize = 1024;

async fn read_line<C: AsyncBufRead + Unpin>(control: &mut C) -> Result<String, ListenError> {
    let mut line = String::new();
    (&mut *control)
        .take(MAX_LINE_LEN as u64)
        .read_line(&mut line)
        .await?;
    if line.len() == MAX_LINE_LEN && !line.ends_with('\n') {
        return Err(ListenError::Refused(line));
    }
    // A line cut short by the end of the stream can still parse as success.
    if !line.ends_with('\n') {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
    }

    Ok(line.trim_end().to_owned())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use hmac::Mac;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, DuplexStream};

    use super::{
        CONTROLLER_TO_SERVER, ControlAuth, MAX_LINE_LEN, SERVER_TO_CONTROLLER, add_onion,
        authenticate, hex, mac, read_line, unhex,
    };
    use crate::listener::ListenError;
    use crate::onion;
    use crate::testing::{self, SERVICE, within};

    /// A control connection on which the daemon has already written `answer`.
    async fn answered(answer: &str) -> (DuplexStream, BufReader<DuplexStream>) {
        let (mut daemon, control) = tokio::io::duplex(512);
        daemon.write_all(answer.as_bytes()).await.unwrap();
        (daemon, BufReader::new(control))
    }

    async fn sent(mut daemon: DuplexStream, control: BufReader<DuplexStream>) -> String {
        drop(control);
        let mut commands = String::new();
        daemon.read_to_string(&mut commands).await.unwrap();
        commands
    }

    async fn authenticate_with(answer: &str) -> Result<(), ListenError> {
        let (_daemon, mut control) = answered(answer).await;
        within(authenticate(&mut control, &ControlAuth::Null)).await
    }

    async fn add_onion_with(answer: &str) -> Result<onion::Addr, ListenError> {
        let (_daemon, mut control) = answered(answer).await;
        within(add_onion(&mut control, 9735, 4000)).await
    }

    fn refused(result: Result<impl std::fmt::Debug, ListenError>) -> String {
        match result {
            Err(ListenError::Refused(line)) => line,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// A line as long as the limit allows, ending in `ending`, on a stream
    /// left open.
    async fn read_long_line(ending: &str) -> Result<String, ListenError> {
        let (mut daemon, control) = tokio::io::duplex(MAX_LINE_LEN);
        let text = "5".repeat(MAX_LINE_LEN - ending.len()) + ending;
        daemon.write_all(text.as_bytes()).await.unwrap();
        within(read_line(&mut BufReader::new(control))).await
    }

    #[tokio::test(start_paused = true)]
    async fn reads_a_line_at_the_limit() {
        let line = read_long_line("\r\n").await.unwrap();

        assert_eq!(line.len(), MAX_LINE_LEN - 2);
    }

    /// The process on the control port is not yet known to be the daemon, so
    /// it must not make the client hold more than a line.
    #[tokio::test(start_paused = true)]
    async fn refuses_a_line_above_the_limit() {
        let line = refused(read_long_line("55").await);

        assert_eq!(line.len(), MAX_LINE_LEN);
    }

    #[tokio::test(start_paused = true)]
    async fn authenticates_without_credentials() {
        let (daemon, mut control) = answered("250 OK\r\n").await;

        within(authenticate(&mut control, &ControlAuth::Null))
            .await
            .unwrap();

        assert_eq!(sent(daemon, control).await, "AUTHENTICATE\r\n");
    }

    const COOKIE: [u8; 32] = [7; 32];
    const SERVER_NONCE: [u8; 32] = [9; 32];

    /// The hashes for cookie `[7; 32]`, client nonce `[1; 32]` and server
    /// nonce `[9; 32]`, computed apart from this code.
    #[test]
    fn hashes_as_the_daemon_does() {
        let message: [&[u8]; 3] = [&COOKIE, &[1; 32], &SERVER_NONCE];
        let hash = |key| hex(&mac(key, &message).finalize().into_bytes());

        assert_eq!(
            hash(SERVER_TO_CONTROLLER),
            "3AA0A806AD2EE0A7468EBEFE85A1E83CBD0C55FCD0A8B0836276B50300B1A585"
        );
        assert_eq!(
            hash(CONTROLLER_TO_SERVER),
            "B1DD27057D941677CABBA98898F7B5C77B5BA0011E8219F953F925E8D3997464"
        );
    }

    /// Answers a cookie challenge as a daemon holding `cookie` would, with
    /// `server_key` for its own proof, and returns what the client sent.
    async fn challenged(cookie: [u8; 32], server_key: &[u8]) -> (Result<(), ListenError>, String) {
        let (daemon, control) = tokio::io::duplex(512);
        let mut daemon = BufReader::new(daemon);
        let mut control = BufReader::new(control);

        let answering = async {
            let challenge = read_line(&mut daemon).await.unwrap();
            let nonce = challenge.strip_prefix("AUTHCHALLENGE SAFECOOKIE ").unwrap();
            let message: [&[u8]; 3] = [&cookie, &unhex(nonce).unwrap(), &SERVER_NONCE];
            let server_hash = hex(&mac(server_key, &message).finalize().into_bytes());
            let reply = format!(
                "250 AUTHCHALLENGE SERVERHASH={server_hash} SERVERNONCE={}\r\n",
                hex(&SERVER_NONCE)
            );
            daemon.write_all(reply.as_bytes()).await.unwrap();

            let Ok(proof) = read_line(&mut daemon).await else {
                return (challenge, String::new());
            };
            let expected = hex(&mac(CONTROLLER_TO_SERVER, &message).finalize().into_bytes());
            assert_eq!(proof, format!("AUTHENTICATE {expected}"));
            daemon.write_all(b"250 OK\r\n").await.unwrap();
            (challenge, proof)
        };
        let authenticating = async {
            let result = authenticate(&mut control, &ControlAuth::Cookie(COOKIE.to_vec())).await;
            drop(control);
            result
        };

        let (result, (_, proof)) = within(async { tokio::join!(authenticating, answering) }).await;
        (result, proof)
    }

    #[tokio::test(start_paused = true)]
    async fn authenticates_with_a_cookie_without_sending_it() {
        let (result, proof) = challenged(COOKIE, SERVER_TO_CONTROLLER).await;

        result.unwrap();
        assert!(proof.starts_with("AUTHENTICATE "));
        assert!(!proof.contains(&hex(&COOKIE)));
    }

    /// Whatever answers on the port without knowing the cookie gets no proof
    /// computed from it.
    #[tokio::test(start_paused = true)]
    async fn a_daemon_that_does_not_know_the_cookie_gets_no_proof() {
        for (cookie, key) in [
            ([8; 32], SERVER_TO_CONTROLLER),
            (COOKIE, CONTROLLER_TO_SERVER),
        ] {
            let (result, proof) = challenged(cookie, key).await;

            assert!(matches!(result, Err(ListenError::UnknownCookie)));
            assert_eq!(proof, "");
        }
    }

    /// Every daemon writes a 32-byte cookie, so any other file is not one.
    #[tokio::test(start_paused = true)]
    async fn refuses_a_cookie_of_the_wrong_length() {
        for len in [0, 31, 33] {
            let (daemon, mut control) = answered("").await;
            let auth = ControlAuth::Cookie(vec![7; len]);

            let result = within(authenticate(&mut control, &auth)).await;

            assert!(matches!(result, Err(ListenError::CookieLength(l)) if l == len));
            assert_eq!(sent(daemon, control).await, "");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_malformed_challenge_answer_is_refused() {
        let nonce = hex(&SERVER_NONCE);
        let answers = [
            "515 Authentication failed".to_owned(),
            format!("250 AUTHCHALLENGE SERVERHASH={nonce}"),
            format!("250 AUTHCHALLENGE SERVERHASH={nonce} SERVERNONCE=00"),
            format!(
                "250 AUTHCHALLENGE SERVERHASH=zz{} SERVERNONCE={nonce}",
                &nonce[2..]
            ),
            format!("250 AUTHCHALLENGE SERVERNONCE={nonce} SERVERHASH={nonce}"),
        ];

        for answer in answers {
            let (_daemon, mut control) = answered(&format!("{answer}\r\n")).await;
            let auth = ControlAuth::Cookie(COOKIE.to_vec());

            let result = within(authenticate(&mut control, &auth)).await;

            assert_eq!(refused(result), answer);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn refused_credentials_are_reported() {
        let line = refused(authenticate_with("515 Authentication failed\r\n").await);

        assert_eq!(line, "515 Authentication failed");
    }

    /// More lines than the command calls for mean the two sides disagree about
    /// where the answer ends.
    #[tokio::test(start_paused = true)]
    async fn an_unfinished_answer_to_authentication_is_refused() {
        assert_eq!(refused(authenticate_with("250-OK\r\n").await), "250-OK");
        assert_eq!(refused(authenticate_with("250\r\n").await), "250");
    }

    #[tokio::test(start_paused = true)]
    async fn publishes_a_service_with_a_discarded_key() {
        let answer = format!("250-ServiceID={SERVICE}\r\n250-PrivateKey=unused\r\n250 OK\r\n");
        let (daemon, mut control) = answered(&answer).await;

        let addr = within(add_onion(&mut control, 9735, 4000)).await.unwrap();

        assert_eq!(addr, testing::addr());
        assert_eq!(
            sent(daemon, control).await,
            "ADD_ONION NEW:ED25519-V3 Flags=DiscardPK Port=9735,127.0.0.1:4000\r\n"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_service_is_reported() {
        let line = refused(add_onion_with("512 Bad arguments to ADD_ONION\r\n").await);

        assert_eq!(line, "512 Bad arguments to ADD_ONION");
    }

    /// The daemon has said all it will say, so reading on would never return.
    #[tokio::test(start_paused = true)]
    async fn an_answer_ending_in_anything_but_ok_is_refused() {
        let answer = format!("250-ServiceID={SERVICE}\r\n250 DONE\r\n");

        assert_eq!(refused(add_onion_with(&answer).await), "250 DONE");
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_naming_no_service_is_refused() {
        assert_eq!(refused(add_onion_with("250 OK\r\n").await), "250 OK");
    }

    #[tokio::test(start_paused = true)]
    async fn a_service_name_that_is_no_onion_address_is_unusable() {
        let published = add_onion_with("250-ServiceID=short\r\n250 OK\r\n").await;

        assert!(matches!(
            published,
            Err(ListenError::ServiceId(onion::InvalidAddr::Malformed))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_daemon_that_stops_answering_is_a_failure() {
        let (mut daemon, mut control) = answered("").await;
        daemon.shutdown().await.unwrap();

        let Err(ListenError::Io(error)) = add_onion(&mut control, 9735, 4000).await else {
            panic!("expected the end of the stream");
        };

        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_cut_short_is_a_failure() {
        let (mut daemon, mut control) =
            answered(&format!("250-ServiceID={SERVICE}\r\n250 OK")).await;
        daemon.shutdown().await.unwrap();

        let Err(ListenError::Io(error)) = add_onion(&mut control, 9735, 4000).await else {
            panic!("expected the end of the stream");
        };

        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn debug_output_omits_the_cookie() {
        assert_eq!(format!("{:?}", ControlAuth::Null), "Null");
        assert_eq!(
            format!("{:?}", ControlAuth::Cookie(vec![0xab; 32])),
            "Cookie(..)"
        );
    }
}
