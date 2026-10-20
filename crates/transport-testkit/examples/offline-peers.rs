//! Stored messages let independent peer sessions exchange typed data.

use fungi_transport::{RecvChannel, SendChannel};
use fungi_transport_testkit::mem::{MemConfig, store_and_forward};

#[derive(Debug)]
struct Message(&'static str);

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (mut alice_store, mut bob_store) = store_and_forward(MemConfig::default());
    {
        let mut alice = alice_store.connect();
        alice.send(Message("hello bob")).await.unwrap();
    }
    {
        let mut bob = bob_store.connect();
        println!("bob received: {}", bob.recv().await.unwrap().0);
        bob.send(Message("hello alice")).await.unwrap();
    }
    let mut alice = alice_store.connect();
    println!("alice received: {}", alice.recv().await.unwrap().0);
}

#[cfg(test)]
mod tests {
    #[test]
    fn peers_exchange_messages_without_overlapping_sessions() {
        super::main();
    }
}
