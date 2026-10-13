use std::net::SocketAddr;

use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use super::{ListenError, Listener};
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
    let control = daemon.local_addr().unwrap();

    let (listener, (connection, target)) = within(async {
        tokio::join!(
            Listener::bind(control, &ControlAuth::Null, 9735, 16),
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
    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
}

#[tokio::test]
async fn an_absent_daemon_fails_the_bind() {
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let control = unused.local_addr().unwrap();
    drop(unused);

    let bound = Listener::bind(control, &ControlAuth::Null, 9735, 16).await;

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
