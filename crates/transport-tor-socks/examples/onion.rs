#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

//! Exchange a message with an onion service through a local tor daemon.
//!
//! With `tor --SocksPort 9050 --ControlPort 9051` running:
//!
//! ```text
//! cargo run --example onion -- listen 9051
//! cargo run --example onion -- dial 127.0.0.1:9050 <address printed by listen>
//! ```
//!
//! A daemon started with `--CookieAuthentication 1` also needs the path of
//! the `control_auth_cookie` file in its data directory, after the control
//! port. The file is readable only by the daemon's user unless
//! `--CookieAuthFileGroupReadable 1` is set.

use std::env;
use std::error::Error;

use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel};
use fungi_transport_tor_socks::{AcceptError, Connector, ControlAuth, Listener, framed};
use tokio::net::TcpStream;

const VIRT_PORT: u16 = 9735;
const MAX_MESSAGE_LEN: u32 = 1024;
const USAGE: &str = "usage: onion listen <control-port> [cookie] | onion dial <socks> <address>";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();

    match args[..] {
        ["listen", control_port] => listen(control_port, ControlAuth::Null).await,
        ["listen", control_port, cookie] => {
            listen(control_port, ControlAuth::Cookie(std::fs::read(cookie)?)).await
        }
        ["dial", socks, addr] => dial(socks, addr).await,
        _ => Err(USAGE.into()),
    }
}

/// Publish a service and echo the first message of each channel back.
async fn listen(control_port: &str, auth: ControlAuth) -> Result<(), Box<dyn Error>> {
    let control_port = control_port.parse()?;
    let mut listener = Listener::bind(control_port, &auth, VIRT_PORT, MAX_MESSAGE_LEN).await?;
    println!("ONION={}", listener.addr());
    println!("READY");

    loop {
        let mut channel = match listener.build(&()).await {
            Ok(channel) => channel,
            Err(AcceptError::Accept(error)) => {
                eprintln!("FAILED={error}");
                continue;
            }
            Err(error @ AcceptError::Departed(_)) => return Err(error.into()),
        };
        if let Err(error) = echo(&mut channel).await {
            eprintln!("FAILED={error}");
        }
    }
}

/// Send the next message on `channel` back.
async fn echo(channel: &mut framed::Channel<TcpStream>) -> Result<(), Box<dyn Error>> {
    let message = channel.recv().await?;
    println!("RECEIVED={}", String::from_utf8_lossy(&message));
    channel.send(message).await?;
    Ok(())
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
