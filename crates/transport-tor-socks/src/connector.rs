//! Outbound channels through the daemon's SOCKS port.

use std::io;
use std::net::SocketAddr;

use fungi_transport::{ChannelBuilder, ConnectionUnlinkability};
use tokio::net::TcpStream;

use crate::{framed, onion, socks5};

/// Opens channels to onion services through the daemon's SOCKS port.
///
/// The daemon keeps the caller anonymous and checks the key behind the
/// address, so whoever controls that port controls both guarantees.
///
/// No step has a deadline. Bound [`ChannelBuilder::build`] from outside;
/// dropping it abandons the attempt.
#[derive(Debug)]
pub struct Connector {
    proxy: SocketAddr,
    max_message_len: u32,
}

/// Failure to open a channel.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// The daemon could not reach the onion service. Worth retrying later.
    #[error("onion service unreachable")]
    Unreachable,
    /// The daemon refused the request.
    #[error("proxy refused the connection with reply {code:#04x}")]
    Rejected {
        /// The SOCKS5 reply field.
        code: u8,
    },
    /// The daemon's answer is not SOCKS5 without authentication.
    #[error("unexpected answer from the proxy")]
    Malformed,
    /// The connection to the daemon failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl Connector {
    /// Dial through the SOCKS port at `proxy`, with channels carrying
    /// messages of at most `max_message_len` bytes.
    pub fn new(proxy: SocketAddr, max_message_len: u32) -> Self {
        Self {
            proxy,
            max_message_len,
        }
    }
}

impl ChannelBuilder for Connector {
    type Input = onion::Addr;
    type Privacy = ConnectionUnlinkability;
    type Channel = framed::Channel<TcpStream, ConnectionUnlinkability>;
    type BuildError = ConnectError;

    async fn build(&mut self, addr: &onion::Addr) -> Result<Self::Channel, ConnectError> {
        let mut stream = TcpStream::connect(self.proxy).await?;
        socks5::connect(&mut stream, addr).await?;

        Ok(framed::Channel::tagged(stream, self.max_message_len))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::{ConnectError, Connector};
    use crate::testing::{addr, within};

    /// The channel carries messages over the tunnel the proxy opened.
    #[tokio::test]
    async fn builds_a_channel_over_the_tunnel() {
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut connector = Connector::new(proxy.local_addr().unwrap(), 16);

        let service = async {
            let (mut stream, _) = proxy.accept().await.unwrap();
            let mut request = [0; 3 + 5 + 62 + 2];
            stream.write_all(&[5, 0]).await.unwrap();
            stream.read_exact(&mut request).await.unwrap();
            stream
                .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();

            let mut message = [0; 6];
            stream.read_exact(&mut message).await.unwrap();
            stream.write_all(b"\0\0\0\x04pong").await.unwrap();
            message
        };
        let client = async {
            let mut channel = connector.build(&addr()).await.unwrap();
            channel.send(b"hi".to_vec()).await.unwrap();
            channel.recv().await
        };

        let (message, received) = within(async { tokio::join!(service, client) }).await;
        assert_eq!(&message, b"\0\0\0\x02hi");
        assert_eq!(received.unwrap(), b"pong");
    }

    #[tokio::test]
    async fn an_absent_proxy_is_a_connection_failure() {
        let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut connector = Connector::new(unused.local_addr().unwrap(), 16);
        drop(unused);

        assert!(matches!(
            connector.build(&addr()).await,
            Err(ConnectError::Io(_))
        ));
    }
}
