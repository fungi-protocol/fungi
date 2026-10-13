//! The client side of the daemon's control port, as far as publishing an
//! onion service needs it.

use std::fmt;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use crate::listener::ListenError;
use crate::onion;

/// How to authenticate on the control port.
pub enum ControlAuth {
    /// The daemon asks for no credentials.
    Null,
    /// The daemon asks for the contents of its `control_auth_cookie` file.
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
            let hex: String = cookie.iter().map(|byte| format!("{byte:02X}")).collect();
            format!("AUTHENTICATE {hex}\r\n")
        }
    };
    control.write_all(command.as_bytes()).await?;

    let line = read_line(control).await?;
    match Reply::parse(&line) {
        Some(Reply::Last(_)) => Ok(()),
        _ => Err(ListenError::Refused(line)),
    }
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

async fn read_line<C: AsyncBufRead + Unpin>(control: &mut C) -> std::io::Result<String> {
    let mut line = String::new();
    control.read_line(&mut line).await?;
    // A line cut short by the end of the stream can still parse as success.
    if !line.ends_with('\n') {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }

    Ok(line.trim_end().to_owned())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, DuplexStream};

    use super::{ControlAuth, add_onion, authenticate};
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

    #[tokio::test(start_paused = true)]
    async fn authenticates_without_credentials() {
        let (daemon, mut control) = answered("250 OK\r\n").await;

        within(authenticate(&mut control, &ControlAuth::Null))
            .await
            .unwrap();

        assert_eq!(sent(daemon, control).await, "AUTHENTICATE\r\n");
    }

    #[tokio::test(start_paused = true)]
    async fn authenticates_with_the_cookie_in_hex() {
        let (daemon, mut control) = answered("250 OK\r\n").await;
        let auth = ControlAuth::Cookie(vec![0x0d, 0xea, 0xbe, 0xef]);

        within(authenticate(&mut control, &auth)).await.unwrap();

        assert_eq!(sent(daemon, control).await, "AUTHENTICATE 0DEABEEF\r\n");
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
