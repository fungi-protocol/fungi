use std::io;
use std::net::SocketAddr;

use fungi_transport::{ChannelBuilder, ConnectionUnlinkability};
use tokio::net::TcpStream;

use crate::{message_stream, onion, socks5};

/// A channel builder that dials onion services through the daemon's SOCKS
/// port.
///
/// The Tor daemon assumed to be handling the SOCKS connection hides the
/// caller's IP address from the onion service, and authenticates the service
/// using the key in the address.
///
/// Each channel gets its own circuit, through random SOCKS credentials, so a
/// service cannot link it to another channel by circuit. Channels opened
/// through one [`Identity`] share their credentials instead.
///
/// If the Tor daemon would not isolate by the credentials, as when it is
/// configured with `NoIsolateSOCKSAuth`, it answers without authentication,
/// and [`ChannelBuilder::build`] fails with [`ConnectError::NotIsolated`].
///
/// No step has a deadline. Bound [`ChannelBuilder::build`] from outside;
/// dropping its future abandons the attempt.
#[derive(Debug)]
pub struct Connector<const N: usize> {
    proxy: SocketAddr,
}

/// Why [`ChannelBuilder::build`] or [`Connector::identity`] failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The daemon could not reach the onion service. A later attempt may
    /// succeed.
    #[error("onion service unreachable")]
    Unreachable,
    /// The daemon refused the request for a reason other than the service
    /// being unreachable, such as a missing client authorization.
    #[error("proxy refused the connection with reply {code:#04x}")]
    Rejected {
        /// The reply code. RFC 1928, section 6, defines the codes up to `0x08`.
        /// Tor's [SOCKS extensions] define `0xf0` to `0xf7` for onion services.
        ///
        /// [SOCKS extensions]: https://spec.torproject.org/socks-extensions.html#extended-error-codes
        code: u8,
    },
    /// The daemon would not take the SOCKS credentials that it isolates
    /// circuits by.
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

    /// Returns a builder whose channels all send the same fresh random SOCKS
    /// credentials. Each call returns a different identity.
    ///
    /// A service cannot link channels of different identities by circuit,
    /// provided the daemon isolates by SOCKS credentials. Fails only with
    /// [`ConnectError::Random`].
    pub fn identity(&self) -> Result<Identity<N>, ConnectError> {
        Ok(Identity {
            proxy: self.proxy,
            isolation: socks5::Isolation::random()?,
        })
    }
}

/// A channel builder whose channels share one set of SOCKS credentials, so
/// they may share a circuit, and a service may link them to one another.
///
/// Use one per input and one per output: an identity shared between an input
/// and an output lets the service link them by circuit. A clone is the same
/// identity. The daemon reuses a circuit only while it is alive, so shared
/// credentials do not mean one circuit for good. Otherwise
/// [`ChannelBuilder::build`] behaves as for [`Connector`].
#[derive(Clone, Debug)]
pub struct Identity<const N: usize> {
    proxy: SocketAddr,
    isolation: socks5::Isolation,
}

impl<const N: usize> ChannelBuilder<message_stream::Message> for Identity<N> {
    type Input = onion::Addr;
    type Privacy = ConnectionUnlinkability;
    type Channel = message_stream::Channel<TcpStream, N, ConnectionUnlinkability>;
    type BuildError = ConnectError;

    async fn build(&mut self, addr: &onion::Addr) -> Result<Self::Channel, ConnectError> {
        let mut stream = TcpStream::connect(self.proxy).await?;
        socks5::connect(&mut stream, addr, &self.isolation).await?;

        Ok(message_stream::Channel::tagged(
            stream,
            message_stream::to_message,
        ))
    }
}

/// Opens each channel through an identity of its own.
impl<const N: usize> ChannelBuilder<message_stream::Message> for Connector<N> {
    type Input = onion::Addr;
    type Privacy = ConnectionUnlinkability;
    type Channel = message_stream::Channel<TcpStream, N, ConnectionUnlinkability>;
    type BuildError = ConnectError;

    async fn build(&mut self, addr: &onion::Addr) -> Result<Self::Channel, ConnectError> {
        self.identity()?.build(addr).await
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::fmt;

    use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::{ConnectError, Connector};
    use crate::message_stream::Message;
    use crate::onion;
    use crate::testing::{CREDENTIALS_ACCEPTED, HANDSHAKE_LEN, addr, message, within};

    const MAX: usize = 32;

    #[tokio::test]
    async fn builds_a_channel_over_the_tunnel() {
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut connector = Connector::<MAX>::new(proxy.local_addr().unwrap());

        let service = async {
            let (mut stream, _) = tunnel(&proxy).await;

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

    /// Accepts one tunnel on `proxy`, and returns its stream and the
    /// handshake the client sent.
    async fn tunnel(proxy: &TcpListener) -> (TcpStream, [u8; HANDSHAKE_LEN]) {
        let (mut stream, _) = proxy.accept().await.unwrap();
        let mut handshake = [0; HANDSHAKE_LEN];

        stream.write_all(&CREDENTIALS_ACCEPTED).await.unwrap();
        stream.read_exact(&mut handshake).await.unwrap();
        stream
            .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();

        (stream, handshake)
    }

    /// Opens one channel through `builder` and returns the handshake it sent.
    async fn sent_by<B>(proxy: &TcpListener, builder: &mut B) -> [u8; HANDSHAKE_LEN]
    where
        B: ChannelBuilder<Message, Input = onion::Addr>,
        B::BuildError: fmt::Debug,
    {
        let addr = addr();
        let ((_, handshake), built) =
            within(async { tokio::join!(tunnel(proxy), builder.build(&addr)) }).await;

        built.unwrap();
        handshake
    }

    async fn listening() -> (TcpListener, Connector<MAX>) {
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = Connector::new(proxy.local_addr().unwrap());
        (proxy, connector)
    }

    #[tokio::test]
    async fn each_channel_sends_its_own_credentials() {
        let (proxy, mut connector) = listening().await;

        let first = sent_by(&proxy, &mut connector).await;
        let second = sent_by(&proxy, &mut connector).await;

        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn an_identity_sends_the_same_credentials_every_time() {
        let (proxy, connector) = listening().await;
        let mut identity = connector.identity().unwrap();

        let first = sent_by(&proxy, &mut identity).await;
        let second = sent_by(&proxy, &mut identity).await;

        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn a_clone_is_the_same_identity() {
        let (proxy, connector) = listening().await;
        let mut identity = connector.identity().unwrap();
        let mut clone = identity.clone();

        let first = sent_by(&proxy, &mut identity).await;
        let second = sent_by(&proxy, &mut clone).await;

        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn identities_send_different_credentials() {
        let (proxy, connector) = listening().await;

        let first = sent_by(&proxy, &mut connector.identity().unwrap()).await;
        let second = sent_by(&proxy, &mut connector.identity().unwrap()).await;

        assert_ne!(first, second);
    }

    #[test]
    fn debug_leaves_out_the_credentials() {
        let proxy = "127.0.0.1:9050".parse().unwrap();
        let identity = Connector::<MAX>::new(proxy).identity().unwrap();

        assert_eq!(
            format!("{identity:?}"),
            "Identity { proxy: 127.0.0.1:9050, isolation: Isolation(..) }"
        );
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
