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
