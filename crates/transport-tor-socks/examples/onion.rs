#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

//! Exchange a message with an onion service through a local tor daemon.
//!
//! With `tor --SocksPort 9050 --ControlPort 9051` running:
//!
//! ```text
//! cargo run --example onion -- listen 127.0.0.1:9051
//! cargo run --example onion -- dial 127.0.0.1:9050 <address printed by listen>
//! ```
//!
//! A daemon started with `--CookieAuthentication 1` also needs the path of
//! the `control_auth_cookie` file in its data directory, after the control
//! address. The file is readable only by the daemon's user unless
//! `--CookieAuthFileGroupReadable 1` is set.

use std::env;
use std::error::Error;

use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel};
use fungi_transport_tor_socks::{Connector, ControlAuth, Listener};

const VIRT_PORT: u16 = 9735;
const MAX_MESSAGE_LEN: u32 = 1024;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();

    match args[..] {
        ["listen", control] => listen(control, ControlAuth::Null).await,
        ["listen", control, cookie] => {
            listen(control, ControlAuth::Cookie(std::fs::read(cookie)?)).await
        }
        ["dial", socks, addr] => dial(socks, addr).await,
        _ => Err("usage: onion listen <control> [cookie] | onion dial <socks> <address>".into()),
    }
}

/// Publish a service and echo each message back to its sender.
async fn listen(control: &str, auth: ControlAuth) -> Result<(), Box<dyn Error>> {
    let mut listener = Listener::bind(control.parse()?, &auth, VIRT_PORT, MAX_MESSAGE_LEN).await?;
    println!("ONION={}", listener.addr());
    println!("READY");

    loop {
        let mut channel = listener.build(&()).await?;
        let message = channel.recv().await?;
        println!("RECEIVED={}", String::from_utf8_lossy(&message));
        channel.send(message).await?;
    }
}

/// Send one message and expect it back.
async fn dial(socks: &str, addr: &str) -> Result<(), Box<dyn Error>> {
    let mut connector = Connector::new(socks.parse()?, MAX_MESSAGE_LEN);
    let mut channel = connector.build(&addr.parse()?).await?;

    channel.send(b"ping over tor".to_vec()).await?;
    let echoed = channel.recv().await?;
    println!("ECHOED={}", String::from_utf8_lossy(&echoed));

    if echoed != b"ping over tor" {
        return Err("the service answered something else".into());
    }
    println!("OK");
    Ok(())
}
