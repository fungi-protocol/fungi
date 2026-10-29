//! Inbound channels through an onion service the daemon publishes.

use std::io;
use std::net::Ipv4Addr;

use fungi_transport::{ChannelBuilder, Unspecified};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::control::{self, ControlAuth};
use crate::{framed, onion};

/// Accepts channels arriving at an onion service.
///
/// The service has a fresh address and lasts as long as the listener. The
/// daemon forwards its connections to a local port that any local process can
/// also reach.
///
/// If the daemon drops the control connection, the service ends and
/// [`ChannelBuilder::build`] fails with [`AcceptError::Departed`].
#[derive(Debug)]
pub struct Listener {
    local: TcpListener,
    addr: onion::Addr,
    max_message_len: u32,
    // The daemon removes the service when this connection closes.
    control: BufReader<TcpStream>,
}

/// Failure to publish an onion service.
#[derive(Debug, thiserror::Error)]
pub enum ListenError {
    /// The daemon refused a command, or answered without naming a service.
    #[error("control port answered: {0}")]
    Refused(String),
    /// The daemon published a service but named it with an unusable address.
    #[error("daemon named an unusable onion service")]
    ServiceId(#[source] onion::InvalidAddr),
    /// The connection to the daemon failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Failure to accept a channel.
#[derive(Debug, thiserror::Error)]
pub enum AcceptError {
    /// The control connection ended, so the service is gone and no later
    /// build succeeds. Carries the I/O error that ended it, if any.
    #[error("the control connection to the daemon ended")]
    Departed(#[source] Option<io::Error>),
    /// Accepting one connection failed. The service remains, so a later
    /// build may succeed.
    #[error("accepting a connection failed")]
    Accept(#[source] io::Error),
}

impl Listener {
    /// Publish an onion service on `virt_port` through the control port at
    /// 127.0.0.1:`control_port`, with channels carrying messages of at most
    /// `max_message_len` bytes.
    ///
    /// The daemon forwards connections to 127.0.0.1, so it must run on this
    /// host.
    ///
    /// No step has a deadline; bound the call from outside.
    pub async fn bind(
        control_port: u16,
        auth: &ControlAuth,
        virt_port: u16,
        max_message_len: u32,
    ) -> Result<Self, ListenError> {
        let local = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let local_port = local.local_addr()?.port();

        let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, control_port)).await?;
        let mut control = BufReader::new(stream);
        control::authenticate(&mut control, auth).await?;
        let addr = control::add_onion(&mut control, virt_port, local_port).await?;

        Ok(Self {
            local,
            addr,
            max_message_len,
            control,
        })
    }

    /// Where peers reach this listener.
    pub fn addr(&self) -> &onion::Addr {
        &self.addr
    }
}

impl ChannelBuilder for Listener {
    type Input = ();
    // Peers reach this side by its own onion address, which identifies it.
    type Privacy = Unspecified;
    type Channel = framed::Channel<TcpStream>;
    type BuildError = AcceptError;

    async fn build(&mut self, (): &()) -> Result<Self::Channel, AcceptError> {
        tokio::select! {
            // A connection still queued must not hide that the daemon left.
            biased;
            // The daemon sends nothing after publishing, so anything it does
            // send, the end of the connection included, means it left.
            read = self.control.fill_buf() => Err(AcceptError::Departed(read.err())),
            accepted = self.local.accept() => {
                let (stream, _) = accepted.map_err(AcceptError::Accept)?;
                Ok(framed::Channel::new(stream, self.max_message_len))
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::net::SocketAddr;

    use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    use super::{AcceptError, ListenError, Listener};
    use crate::testing::{SERVICE, addr, within};
    use crate::{Connector, ControlAuth};

    /// A control port that publishes one service.
    ///
    /// Returns its connection, which keeps the service alive, and the local
    /// address the daemon would forward the service to.
    async fn publish(daemon: TcpListener) -> (BufReader<TcpStream>, SocketAddr) {
        let (stream, _) = daemon.accept().await.unwrap();
        let mut control = BufReader::new(stream);
        let mut line = String::new();

        control.read_line(&mut line).await.unwrap();
        control.write_all(b"250 OK\r\n").await.unwrap();

        line.clear();
        control.read_line(&mut line).await.unwrap();
        let target = line.trim_end().rsplit_once(',').unwrap().1;
        let answer = format!("250-ServiceID={SERVICE}\r\n250 OK\r\n");
        control.write_all(answer.as_bytes()).await.unwrap();

        (control, target.parse().unwrap())
    }

    async fn bound() -> (Listener, BufReader<TcpStream>, SocketAddr) {
        let daemon = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_port = daemon.local_addr().unwrap().port();

        let (listener, (connection, target)) = within(async {
            tokio::join!(
                Listener::bind(control_port, &ControlAuth::Null, 9735, 16),
                publish(daemon)
            )
        })
        .await;

        (listener.unwrap(), connection, target)
    }

    #[tokio::test]
    async fn reports_the_published_address() {
        let (listener, _connection, _) = bound().await;

        assert_eq!(listener.addr(), &addr());
    }

    #[tokio::test]
    async fn accepts_what_the_daemon_forwards() {
        let (mut listener, _connection, target) = bound().await;

        let forwarded = async {
            let mut stream = TcpStream::connect(target).await.unwrap();
            stream.write_all(b"\0\0\0\x02hi").await.unwrap();
            let mut answer = [0; 8];
            stream.read_exact(&mut answer).await.unwrap();
            answer
        };
        let accepting = async {
            let mut channel = listener.build(&()).await.unwrap();
            let message = channel.recv().await.unwrap();
            channel.send(b"pong".to_vec()).await.unwrap();
            message
        };

        let (answer, message) = within(async { tokio::join!(forwarded, accepting) }).await;
        assert_eq!(message, b"hi");
        assert_eq!(&answer, b"\0\0\0\x04pong");
    }

    /// Closing the control connection is what removes the service.
    #[tokio::test]
    async fn the_service_ends_with_the_listener() {
        let (listener, mut connection, _) = bound().await;

        drop(listener);

        let mut rest = String::new();
        assert_eq!(within(connection.read_line(&mut rest)).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_departed_daemon_fails_the_next_build() {
        let (mut listener, connection, _) = bound().await;

        drop(connection);

        let error = within(listener.build(&())).await.unwrap_err();
        assert!(matches!(error, AcceptError::Departed(None)));
    }

    #[tokio::test]
    async fn a_reset_control_connection_reports_the_reset() {
        let (mut listener, connection, _) = bound().await;
        let stream = connection.into_inner();
        stream.set_zero_linger().unwrap();

        drop(stream);

        let error = within(listener.build(&())).await.unwrap_err();
        let AcceptError::Departed(Some(error)) = error else {
            panic!("expected the reset, got {error:?}");
        };
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
    }

    #[tokio::test]
    async fn a_queued_connection_does_not_hide_a_departed_daemon() {
        let (mut listener, connection, target) = bound().await;
        drop(connection);
        // Fails once the listener has seen the daemon leave.
        within(listener.build(&())).await.unwrap_err();

        let _queued = TcpStream::connect(target).await.unwrap();

        for _ in 0..20 {
            // Lets the runtime see the queued connection.
            tokio::task::yield_now().await;
            let error = within(listener.build(&())).await.unwrap_err();
            assert!(matches!(error, AcceptError::Departed(_)));
        }
    }

    #[tokio::test]
    async fn an_absent_daemon_fails_the_bind() {
        let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_port = unused.local_addr().unwrap().port();
        drop(unused);

        let bound = Listener::bind(control_port, &ControlAuth::Null, 9735, 16).await;

        assert!(matches!(bound, Err(ListenError::Io(_))));
    }

    /// A proxy standing in for the daemon's SOCKS port: it answers one request
    /// and joins the client to `target`.
    async fn forward(proxy: TcpListener, target: SocketAddr) {
        let (mut client, _) = proxy.accept().await.unwrap();
        let mut request = [0; 3 + 5 + 62 + 2];
        client.write_all(&[5, 0]).await.unwrap();
        client.read_exact(&mut request).await.unwrap();
        client
            .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();

        let mut service = TcpStream::connect(target).await.unwrap();
        let _ = tokio::io::copy_bidirectional(&mut client, &mut service).await;
    }

    #[tokio::test]
    async fn a_connector_reaches_a_listener_through_the_daemon() {
        let (mut listener, _connection, target) = bound().await;
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut connector = Connector::new(proxy.local_addr().unwrap(), 16);
        let addr = listener.addr().clone();

        let dialing = async {
            let mut channel = connector.build(&addr).await.unwrap();
            channel.send(b"ping".to_vec()).await.unwrap();
            channel.recv().await.unwrap()
        };
        let accepting = async {
            let mut channel = listener.build(&()).await.unwrap();
            let message = channel.recv().await.unwrap();
            channel.send(b"pong".to_vec()).await.unwrap();
            message
        };

        let (answer, message) = within(async {
            tokio::select! {
                exchanged = async { tokio::join!(dialing, accepting) } => exchanged,
                () = forward(proxy, target) => unreachable!("the proxy outlives the exchange"),
            }
        })
        .await;
        assert_eq!(message, b"ping");
        assert_eq!(answer, b"pong");
    }
}
