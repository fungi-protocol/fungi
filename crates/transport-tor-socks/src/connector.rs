use std::io;
use std::net::SocketAddr;

use fungi_transport::{ChannelBuilder, ConnectionUnlinkability};
use tokio::net::TcpStream;

use crate::{framed, onion, socks5};

/// A channel builder that dials onion services through the daemon's SOCKS
/// port.
///
/// The daemon hides the caller's IP address from the service and checks that
/// the service holds the key behind the address. Whoever controls the SOCKS
/// port can break both.
///
/// Each channel gets its own circuit, through random SOCKS credentials, so a
/// service cannot link it to another channel by circuit. The daemon sees every
/// channel. A daemon that would not isolate by the credentials, such as one
/// configured with `NoIsolateSOCKSAuth`, answers without authentication, and
/// [`ChannelBuilder::build`] fails with [`ConnectError::NotIsolated`].
///
/// No step has a deadline. Bound [`ChannelBuilder::build`] from outside;
/// dropping its future abandons the attempt.
#[derive(Debug)]
pub struct Connector<const N: usize> {
    proxy: SocketAddr,
}

/// Why [`ChannelBuilder::build`] failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The daemon could not reach the onion service. A later attempt may
    /// succeed.
    #[error("onion service unreachable")]
    Unreachable,
    /// The daemon refused the request.
    #[error("proxy refused the connection with reply {code:#04x}")]
    Rejected {
        /// The SOCKS5 reply code (RFC 1928, section 6).
        code: u8,
    },
    /// The daemon would not take the credentials that keep each channel on
    /// its own circuit.
    #[error("proxy does not isolate connections")]
    NotIsolated,
    /// The daemon's answer is not SOCKS5.
    #[error("unexpected answer from the proxy")]
    Malformed,
    /// The system could not supply the random isolation parameter.
    #[error("no randomness for the isolation parameter")]
    Random(#[source] io::Error),
    /// The connection to the daemon failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl<const N: usize> Connector<N> {
    /// Returns a connector that dials through the SOCKS port at `proxy`. Its
    /// channels carry payloads of at most `N` bytes.
    pub fn new(proxy: SocketAddr) -> Self {
        Self { proxy }
    }
}

impl<const N: usize> ChannelBuilder<framed::Message> for Connector<N> {
    type Input = onion::Addr;
    type Privacy = ConnectionUnlinkability;
    type Channel = framed::Channel<TcpStream, N, ConnectionUnlinkability>;
    type BuildError = ConnectError;

    async fn build(&mut self, addr: &onion::Addr) -> Result<Self::Channel, ConnectError> {
        let mut stream = TcpStream::connect(self.proxy).await?;
        socks5::connect(&mut stream, addr).await?;

        Ok(framed::Channel::tagged(stream, framed::to_message))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::{ConnectError, Connector};
    use crate::testing::{CREDENTIALS_ACCEPTED, HANDSHAKE_LEN, addr, message, within};

    const MAX: usize = 32;

    #[tokio::test]
    async fn builds_a_channel_over_the_tunnel() {
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut connector = Connector::<MAX>::new(proxy.local_addr().unwrap());

        let service = async {
            let (mut stream, _) = proxy.accept().await.unwrap();
            let mut handshake = [0; HANDSHAKE_LEN];

            stream.write_all(&CREDENTIALS_ACCEPTED).await.unwrap();
            stream.read_exact(&mut handshake).await.unwrap();
            stream
                .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();

            let mut sent = [0; 4];
            stream.read_exact(&mut sent).await.unwrap();
            stream.write_all(b"\x02\x04pong").await.unwrap();

            sent
        };

        let client = async {
            let mut channel = connector.build(&addr()).await.unwrap();

            channel.send(message(2, b"hi")).await.unwrap();
            channel.recv().await
        };

        let (sent, received) = within(async { tokio::join!(service, client) }).await;

        assert_eq!(&sent, b"\x02\x02hi");
        assert_eq!(received.unwrap(), message(2, b"pong"));
    }

    #[tokio::test]
    async fn an_absent_proxy_is_a_connection_failure() {
        let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut connector = Connector::<MAX>::new(unused.local_addr().unwrap());
        drop(unused);

        assert!(matches!(
            connector.build(&addr()).await,
            Err(ConnectError::Io(_))
        ));
    }
}
