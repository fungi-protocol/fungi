//! Minimal RPC builder process used to exercise subprocess transport.

use fungi_transport::{ChannelBuilder, Duplex, RecvChannel, SendChannel, Unspecified};
use fungi_transport_capnp::serve_builder;
use fungi_transport_capnp::{BuildError, RecvError, SendError};
use tokio::sync::mpsc;

#[derive(Debug)]
struct Sender(mpsc::Sender<Vec<u8>>);
#[derive(Debug)]
struct Receiver(mpsc::Receiver<Vec<u8>>);
impl SendChannel for Sender {
    type Privacy = Unspecified;
    type SendError = SendError;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        self.0.send(message).await.map_err(|_| SendError::Closed)
    }
}
impl RecvChannel for Receiver {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        self.0.recv().await.ok_or(RecvError::Closed)
    }
}
type Echo = Duplex<Sender, Receiver>;

#[derive(Debug)]
struct Builder;
impl ChannelBuilder for Builder {
    type Privacy = Unspecified;
    type Input = Vec<u8>;
    type Channel = Echo;
    type BuildError = BuildError;
    async fn build(&mut self, _: &Vec<u8>) -> Result<Echo, BuildError> {
        let (sender, receiver) = mpsc::channel(1);
        Ok(Duplex::new(Sender(sender), Receiver(receiver)))
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), capnp::Error> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(serve_builder(
            Builder,
            Ok,
            std::convert::identity,
            std::convert::identity,
            std::convert::identity,
            tokio::io::join(tokio::io::stdin(), tokio::io::stdout()),
        ))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echo_delivers_messages_and_reports_closure() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut echo = Builder.build(&Vec::new()).await.unwrap();
            echo.send(vec![42]).await.unwrap();
            assert_eq!(echo.recv().await.unwrap(), vec![42]);
            echo.send(Vec::new()).await.unwrap();
            assert!(echo.recv().await.unwrap().is_empty());
            let (mut sender, mut receiver) = echo.into_parts();
            receiver.0.close();
            assert!(matches!(sender.send(vec![1]).await, Err(SendError::Closed)));
            assert!(matches!(receiver.recv().await, Err(RecvError::Closed)));
        })
        .await
        .unwrap();
    }
}
