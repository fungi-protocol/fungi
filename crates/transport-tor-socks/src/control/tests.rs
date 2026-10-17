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
async fn a_service_name_that_is_no_onion_address_is_refused() {
    let line = refused(add_onion_with("250-ServiceID=short\r\n250 OK\r\n").await);

    assert_eq!(line, "250-ServiceID=short");
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

#[test]
fn debug_output_omits_the_cookie() {
    assert_eq!(format!("{:?}", ControlAuth::Null), "Null");
    assert_eq!(
        format!("{:?}", ControlAuth::Cookie(vec![0xab; 32])),
        "Cookie(..)"
    );
}
