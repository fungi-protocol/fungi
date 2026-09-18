//! Minimal RPC builder process used to exercise subprocess transport.

use fungi_transport::{
    Channel, ChannelBuilder, PeerChannel, RecvChannel, SendChannel, Unspecified,
};
use fungi_transport_capnp::serve_builder;
use fungi_transport_capnp::{BuildError, RecvError, SendError};
use tokio::sync::mpsc;

#[derive(Debug)]
struct Sender(mpsc::Sender<Vec<u8>>, Peer);
#[derive(Debug)]
struct Receiver(mpsc::Receiver<Vec<u8>>, Peer);
#[derive(Debug, Clone)]
struct Peer(std::sync::Arc<()>);
impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for Peer {}
impl PeerChannel for Sender {
    type Peer = Peer;
    fn peer(&self) -> &Peer {
        &self.1
    }
}
impl PeerChannel for Receiver {
    type Peer = Peer;
    fn peer(&self) -> &Peer {
        &self.1
    }
}
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
type Echo = Channel<Sender, Receiver>;

#[derive(Debug)]
struct Builder;
impl ChannelBuilder for Builder {
    type Input = Vec<u8>;
    type Channel = Echo;
    type BuildError = BuildError;
    async fn build(&mut self, _: &Vec<u8>) -> Result<Echo, BuildError> {
        let (sender, receiver) = mpsc::channel(1);
        let peer = Peer(std::sync::Arc::new(()));
        Ok(Channel::new(Sender(sender, peer.clone()), Receiver(receiver, peer)).unwrap())
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
    async fn echo_sessions_have_distinct_peer_capabilities() {
        let first = Builder.build(&Vec::new()).await.unwrap();
        let second = Builder.build(&Vec::new()).await.unwrap();
        let (sender, receiver) = first.into_split();
        let (other_sender, other_receiver) = second.into_split();
        assert_eq!(sender.peer(), receiver.peer());
        assert_ne!(sender.peer(), other_receiver.peer());
        assert!(Channel::new(sender, other_receiver).is_err());
        assert!(Channel::new(other_sender, receiver).is_err());
    }
    #[tokio::test]
    async fn echo_supports_owned_and_borrowed_operations_and_reports_closure() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut echo = Builder.build(&Vec::new()).await.unwrap();
            echo.send(vec![42]).await.unwrap();
            assert_eq!(echo.recv().await.unwrap(), vec![42]);
            let (mut sender, mut receiver) = echo.directions();
            SendChannel::send(&mut sender, Vec::new()).await.unwrap();
            assert!(RecvChannel::recv(&mut receiver).await.unwrap().is_empty());
            let (sender, mut receiver) = echo.into_split();
            receiver.0.close();
            echo = Channel::new(sender, receiver).unwrap();
            assert!(matches!(echo.send(vec![1]).await, Err(SendError::Closed)));
            assert!(matches!(echo.recv().await, Err(RecvError::Closed)));
            let (mut sender, mut receiver) = echo.directions();
            assert!(matches!(
                SendChannel::send(&mut sender, vec![1]).await,
                Err(SendError::Closed)
            ));
            assert!(matches!(
                RecvChannel::recv(&mut receiver).await,
                Err(RecvError::Closed)
            ));
        })
        .await
        .unwrap();
    }
}
