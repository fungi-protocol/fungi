#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

//! A message exchange with an onion service through a local tor daemon.
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
use std::time::Duration;

use fungi_transport::tlv::TypedSlice;
use fungi_transport::{ChannelBuilder, RecvChannel, SendChannel};
use fungi_transport_tor_socks::{
    AcceptError, ConnectError, Connector, ControlAuth, Listener, framed,
};
use tokio::net::TcpStream;

const VIRT_PORT: u16 = 9735;
const MAX_MESSAGE_LEN: usize = 1024;
/// The type of the message sent; the service echoes any type.
const PING: u64 = 2;
/// A newly published service takes a while to become reachable.
const ATTEMPTS: u32 = 6;
const RETRY_AFTER: Duration = Duration::from_secs(10);
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

/// Publishes a service and echoes the first message of each channel.
async fn listen(control_port: &str, auth: ControlAuth) -> Result<(), Box<dyn Error>> {
    let control_port = control_port.parse()?;
    let mut listener = Listener::<MAX_MESSAGE_LEN>::bind(control_port, &auth, VIRT_PORT).await?;

    println!("ONION={}", listener.addr());
    println!("READY");

    loop {
        let mut channel = match listener.build(&()).await {
            Ok(channel) => channel,
            Err(AcceptError::Accept(error)) => {
                eprintln!("FAILED={error}");
                // Failures such as running out of file descriptors last until
                // connections close.
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(error @ AcceptError::Departed(_)) => return Err(error.into()),
        };

        tokio::spawn(async move {
            if let Err(error) = echo(&mut channel).await {
                eprintln!("FAILED={error}");
            }
        });
    }
}

/// Receives the next message on `channel` and sends it back.
async fn echo(
    channel: &mut framed::Channel<TcpStream, MAX_MESSAGE_LEN>,
) -> Result<(), Box<dyn Error>> {
    let message = channel.recv().await?;
    println!("RECEIVED={}", String::from_utf8_lossy(&message.value));

    channel.send(message).await?;

    Ok(())
}

/// Sends one message and checks that it comes back.
async fn dial(socks: &str, addr: &str) -> Result<(), Box<dyn Error>> {
    let mut connector = Connector::<MAX_MESSAGE_LEN>::new(socks.parse()?);
    let addr = addr.parse()?;
    let mut attempt = 1;

    let mut channel = loop {
        match connector.build(&addr).await {
            Err(ConnectError::Unreachable) if attempt < ATTEMPTS => {
                eprintln!("UNREACHABLE, retrying");
                tokio::time::sleep(RETRY_AFTER).await;
                attempt += 1;
            }

            built => break built?,
        }
    };

    let ping = TypedSlice {
        ty: PING,
        value: b"ping over tor".to_vec(),
    };

    channel.send(ping.clone()).await?;

    let echoed = channel.recv().await?;
    println!("ECHOED={}", String::from_utf8_lossy(&echoed.value));

    if echoed != ping {
        return Err("the service answered something else".into());
    }

    println!("OK");
    Ok(())
}
