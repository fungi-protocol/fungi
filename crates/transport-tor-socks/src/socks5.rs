//! The client side of SOCKS5 `CONNECT`, as far as reaching an onion service
//! needs it (RFC 1928), with username/password authentication (RFC 1929).

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tor_socksproto::{
    Buffer, Handshake, NextStep, SocksAddr, SocksAuth, SocksClientHandshake, SocksCmd,
    SocksRequest, SocksStatus, SocksVersion,
};

use crate::connector::ConnectError;
use crate::onion;

/// The username of Tor's extended stream isolation, format type 0: the
/// password alone is the isolation parameter.
const ISOLATION_USERNAME: &[u8] = b"<torS0X>0";

/// Bytes of the random isolation parameter.
const ISOLATION_LEN: usize = 16;

/// The first byte of a username/password message (RFC 1929).
const AUTH_VERSION: u8 = 1;

/// Ask the proxy on `stream` for a tunnel to `addr`.
///
/// Each call sends a fresh random isolation parameter as the password. Tor
/// gives streams with distinct parameters distinct circuits, so no two
/// tunnels share one. Random rather than counted, so that processes sharing
/// a daemon do not repeat each other's. A daemon that predates the extended
/// format takes the username literally, and still isolates by the password.
///
/// On success the next byte read from `stream` is the first from the service.
pub(crate) async fn connect<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    addr: &onion::Addr,
) -> Result<(), ConnectError> {
    // The hostname goes unresolved, for the proxy to look up.
    let host = addr
        .host()
        .to_owned()
        .try_into()
        .expect("an onion hostname is 62 bytes");
    let mut isolation = [0; ISOLATION_LEN];
    getrandom::fill(&mut isolation)
        .map_err(io::Error::from)
        .map_err(ConnectError::Random)?;
    let request = SocksRequest::new(
        SocksVersion::V5,
        SocksCmd::CONNECT,
        SocksAddr::Hostname(host),
        addr.port(),
        SocksAuth::Username(ISOLATION_USERNAME.to_vec(), isolation.to_vec()),
    )
    .expect("an onion address has a nonzero port and credentials fit");

    let mut handshake = SocksClientHandshake::new(request);
    // Precise reads stop at the end of the reply, before the tunnel's bytes.
    let mut buffer = Buffer::new_precise();
    let mut sent = 0;
    let reply = loop {
        let step = match handshake.step(&mut buffer) {
            Ok(step) => step,
            Err(tor_socksproto::Error::AuthRejected) => return Err(ConnectError::NotIsolated),
            Err(_) => return Err(ConnectError::Malformed),
        };
        match step {
            NextStep::Send(data) => {
                // A proxy that picks no authentication is answered with the
                // request itself, right after the greeting.
                if sent == 1 && data.first() != Some(&AUTH_VERSION) {
                    return Err(ConnectError::NotIsolated);
                }
                sent += 1;
                stream.write_all(&data).await?;
            }
            NextStep::Recv(mut recv) => {
                let len = match stream.read(recv.buf()).await {
                    Ok(len) => len,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error.into()),
                };
                if len == 0 {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
                }
                recv.note_received(len)
                    .expect("a read of at least one byte");
            }
            NextStep::Finished(finished) => {
                break finished
                    .into_output()
                    .expect("precise reads leave nothing over");
            }
        }
    };

    match reply.status() {
        SocksStatus::SUCCEEDED => Ok(()),
        // Tor reports a timeout as TTL expired, and with extended errors a
        // missing descriptor, a failed introduction or rendezvous, or an
        // introduction timeout.
        SocksStatus::GENERAL_FAILURE
        | SocksStatus::NETWORK_UNREACHABLE
        | SocksStatus::HOST_UNREACHABLE
        | SocksStatus::CONNECTION_REFUSED
        | SocksStatus::TTL_EXPIRED
        | SocksStatus::HS_DESC_NOT_FOUND
        | SocksStatus::HS_INTRO_FAILED
        | SocksStatus::HS_REND_FAILED
        | SocksStatus::HS_INTRO_TIMEOUT => Err(ConnectError::Unreachable),
        status => Err(ConnectError::Rejected {
            code: status.into(),
        }),
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use proptest::prelude::*;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

    use super::{ISOLATION_LEN, ISOLATION_USERNAME, connect};
    use crate::connector::ConnectError;
    use crate::onion;
    use crate::testing::{CREDENTIALS_ACCEPTED, HANDSHAKE_LEN, SERVICE, addr, within};

    /// The client's greeting: SOCKS5, offering username/password and none.
    const GREETING: [u8; 4] = [5, 2, 2, 0];

    /// A stream on which the proxy has already written `answer`.
    async fn answered(answer: &[u8]) -> (DuplexStream, DuplexStream) {
        let (mut proxy, stream) = tokio::io::duplex(512);
        proxy.write_all(answer).await.unwrap();
        (proxy, stream)
    }

    async fn connect_with(answer: &[u8]) -> Result<(), ConnectError> {
        let (_proxy, mut stream) = answered(answer).await;
        within(connect(&mut stream, &addr())).await
    }

    fn reply(code: u8) -> Vec<u8> {
        [
            &CREDENTIALS_ACCEPTED[..],
            &[5, code, 0, 1, 0, 0, 0, 0, 0, 0],
        ]
        .concat()
    }

    /// What a client sends to reach `addr`, checked but for the isolation
    /// parameter, which it returns.
    fn isolation(sent: &[u8], addr: &onion::Addr) -> Vec<u8> {
        let (greeting, rest) = sent.split_at(GREETING.len());
        assert_eq!(greeting, GREETING);
        let (auth, request) = rest.split_at(3 + ISOLATION_USERNAME.len() + ISOLATION_LEN);
        let (username, password) = auth[1..].split_at(1 + ISOLATION_USERNAME.len());
        assert_eq!(auth[0], 1);
        assert_eq!(username, [&[9][..], b"<torS0X>0"].concat());
        assert_eq!(password[0], ISOLATION_LEN as u8);
        let expected = [
            &[5, 1, 0, 3, 62][..],
            addr.host().as_bytes(),
            &addr.port().to_be_bytes(),
        ]
        .concat();
        assert_eq!(request, expected);
        password[1..].to_vec()
    }

    /// Everything a successful `connect` to `addr` sends.
    async fn sent_by_connect(addr: &onion::Addr) -> Vec<u8> {
        let (mut proxy, mut stream) = answered(&reply(0)).await;
        within(connect(&mut stream, addr)).await.unwrap();
        drop(stream);

        let mut sent = Vec::new();
        proxy.read_to_end(&mut sent).await.unwrap();
        sent
    }

    /// The hostname goes to the proxy unresolved: an onion name has no DNS
    /// entry, and looking it up locally would leak it.
    #[tokio::test(start_paused = true)]
    async fn sends_the_hostname_for_the_proxy_to_resolve() {
        let sent = sent_by_connect(&addr()).await;

        assert_eq!(sent.len(), HANDSHAKE_LEN);
        isolation(&sent, &addr());
    }

    /// Distinct isolation parameters put each tunnel on its own circuit, so
    /// services cannot link one to another.
    #[tokio::test(start_paused = true)]
    async fn each_tunnel_sends_its_own_isolation() {
        let first = isolation(&sent_by_connect(&addr()).await, &addr());
        let second = isolation(&sent_by_connect(&addr()).await, &addr());

        assert_ne!(first, second);
    }

    /// Without the credentials, tunnels may share a circuit.
    #[tokio::test(start_paused = true)]
    async fn a_proxy_refusing_the_credentials_does_not_isolate() {
        let answers = [vec![5, 0, 5, 0, 0, 1, 0, 0, 0, 0, 0, 0], vec![5, 2, 1, 1]];

        for answer in answers {
            assert!(matches!(
                connect_with(&answer).await,
                Err(ConnectError::NotIsolated)
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn leaves_the_stream_at_the_first_tunnel_byte() {
        let bound = [
            vec![5, 0, 0, 1, 1, 2, 3, 4, 0, 80],
            [&[5, 0, 0, 4][..], &[6; 16], &[0, 80]].concat(),
            [&[5, 0, 0, 3, 5][..], b"proxy", &[0, 80]].concat(),
        ];

        for bound in bound {
            let answer = [&CREDENTIALS_ACCEPTED[..], &bound, b"tunnel"].concat();
            let (proxy, mut stream) = answered(&answer).await;

            within(connect(&mut stream, &addr())).await.unwrap();
            drop(proxy);

            let mut rest = Vec::new();
            stream.read_to_end(&mut rest).await.unwrap();
            assert_eq!(rest, b"tunnel");
        }
    }

    /// Through Tor these replies mean the service could not be reached for now;
    /// without extended errors, a general failure covers a missing descriptor and
    /// a failed rendezvous.
    #[tokio::test(start_paused = true)]
    async fn reachability_replies_are_unreachable() {
        for code in [1, 3, 4, 5, 6, 0xf0, 0xf2, 0xf3, 0xf7] {
            assert!(matches!(
                connect_with(&reply(code)).await,
                Err(ConnectError::Unreachable)
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn other_failure_replies_are_rejections() {
        for code in [2, 7, 8, 0xf1, 0xf4, 0xf5, 0xf6] {
            let Err(ConnectError::Rejected { code: rejected }) = connect_with(&reply(code)).await
            else {
                panic!("expected a rejection for {code}");
            };
            assert_eq!(rejected, code);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unexpected_answers_are_malformed() {
        let answers = [
            vec![4, 0],
            vec![5, 0x80],
            vec![5, 2, 2, 0],
            [&CREDENTIALS_ACCEPTED[..], &[4, 0, 0, 1, 0, 0, 0, 0, 0, 0]].concat(),
            [&CREDENTIALS_ACCEPTED[..], &[5, 0, 0, 2, 0, 0, 0, 0, 0, 0]].concat(),
        ];

        for answer in answers {
            assert!(matches!(
                connect_with(&answer).await,
                Err(ConnectError::Malformed)
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_proxy_that_stops_answering_is_an_early_end() {
        let (mut proxy, mut stream) = answered(&CREDENTIALS_ACCEPTED).await;
        proxy.shutdown().await.unwrap();

        let Err(ConnectError::Io(error)) = within(connect(&mut stream, &addr())).await else {
            panic!("expected a stream failure");
        };
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    /// Fails the first read and the first write with the given errors, then
    /// passes through. Passing through keeps a handshake that wrongly retries
    /// a failure from waiting forever.
    struct FailsOnce {
        stream: DuplexStream,
        read: Option<io::ErrorKind>,
        write: Option<io::ErrorKind>,
    }

    impl AsyncRead for FailsOnce {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if let Some(kind) = self.read.take() {
                return Poll::Ready(Err(kind.into()));
            }
            Pin::new(&mut self.stream).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for FailsOnce {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if let Some(kind) = self.write.take() {
                return Poll::Ready(Err(kind.into()));
            }
            Pin::new(&mut self.stream).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }

    /// Answers a successful handshake, failing as given on the way.
    async fn connect_failing_once(
        read: Option<io::ErrorKind>,
        write: Option<io::ErrorKind>,
    ) -> (DuplexStream, Result<(), ConnectError>) {
        let (proxy, stream) = answered(&reply(0)).await;
        let mut stream = FailsOnce {
            stream,
            read,
            write,
        };
        let connected = within(connect(&mut stream, &addr())).await;
        (proxy, connected)
    }

    #[tokio::test(start_paused = true)]
    async fn retries_interrupted_reads_and_writes() {
        let interrupted = Some(io::ErrorKind::Interrupted);
        let (mut proxy, connected) = connect_failing_once(interrupted, interrupted).await;
        connected.unwrap();

        let mut sent = vec![0; HANDSHAKE_LEN];
        proxy.read_exact(&mut sent).await.unwrap();
        isolation(&sent, &addr());
    }

    #[tokio::test(start_paused = true)]
    async fn failed_reads_and_writes_are_stream_failures() {
        for (read, write) in [
            (Some(io::ErrorKind::ConnectionReset), None),
            (None, Some(io::ErrorKind::BrokenPipe)),
        ] {
            let (_proxy, connected) = connect_failing_once(read, write).await;
            assert!(matches!(connected, Err(ConnectError::Io(_))));
        }
    }

    proptest! {
        #[test]
        fn requests_any_port(port in 1u16..) {
            let addr = onion::Addr::new(format!("{SERVICE}.onion"), port).unwrap();
            let sent = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap()
                .block_on(sent_by_connect(&addr));

            isolation(&sent, &addr);
        }
    }
}
