use proptest::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

use super::connect;
use crate::connector::ConnectError;
use crate::onion;
use crate::testing::{SERVICE, addr, within};

const NO_AUTH: [u8; 2] = [5, 0];

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
    [&NO_AUTH[..], &[5, code, 0, 1, 0, 0, 0, 0, 0, 0]].concat()
}

/// The hostname goes to the proxy unresolved: an onion name has no DNS
/// entry, and looking it up locally would leak it.
#[tokio::test(start_paused = true)]
async fn sends_the_hostname_for_the_proxy_to_resolve() {
    let (mut proxy, mut stream) = answered(&reply(0)).await;

    within(connect(&mut stream, &addr())).await.unwrap();
    drop(stream);

    let mut request = Vec::new();
    proxy.read_to_end(&mut request).await.unwrap();
    let expected = [
        &[5, 1, 0][..],
        &[5, 1, 0, 3, 62],
        addr().host().as_bytes(),
        &9735u16.to_be_bytes(),
    ]
    .concat();
    assert_eq!(request, expected);
}

#[tokio::test(start_paused = true)]
async fn leaves_the_stream_at_the_first_tunnel_byte() {
    let bound = [
        vec![5, 0, 0, 1, 1, 2, 3, 4, 0, 80],
        [&[5, 0, 0, 4][..], &[6; 16], &[0, 80]].concat(),
        [&[5, 0, 0, 3, 5][..], b"proxy", &[0, 80]].concat(),
    ];

    for bound in bound {
        let answer = [&NO_AUTH[..], &bound, b"tunnel"].concat();
        let (proxy, mut stream) = answered(&answer).await;

        within(connect(&mut stream, &addr())).await.unwrap();
        drop(proxy);

        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, b"tunnel");
    }
}

/// Through tor these replies mean the service could not be reached for now;
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
        vec![5, 2],
        [&NO_AUTH[..], &[4, 0, 0, 1, 0, 0, 0, 0, 0, 0]].concat(),
        [&NO_AUTH[..], &[5, 0, 0, 2, 0, 0, 0, 0, 0, 0]].concat(),
    ];

    for answer in answers {
        assert!(matches!(
            connect_with(&answer).await,
            Err(ConnectError::Malformed)
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn a_proxy_that_hangs_up_is_a_stream_failure() {
    let (proxy, mut stream) = answered(&NO_AUTH).await;
    drop(proxy);

    assert!(matches!(
        connect(&mut stream, &addr()).await,
        Err(ConnectError::Io(_))
    ));
}

proptest! {
    #[test]
    fn requests_any_port(port: u16) {
        let addr = onion::Addr::new(format!("{SERVICE}.onion"), port).unwrap();
        let request = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap()
            .block_on(async {
                let (mut proxy, mut stream) = answered(&reply(0)).await;
                within(connect(&mut stream, &addr)).await.unwrap();
                drop(stream);

                let mut request = Vec::new();
                proxy.read_to_end(&mut request).await.unwrap();
                request
            });

        prop_assert_eq!(&request[8..70], addr.host().as_bytes());
        prop_assert_eq!(&request[70..], port.to_be_bytes());
    }
}
