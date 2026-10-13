//! Inbound channels through an onion service the daemon publishes.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};

use fungi_transport::{ChannelBuilder, Unspecified};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::control::{self, ControlAuth};
use crate::{framed, onion};

/// Accepts channels arriving at an onion service.
///
/// The service has a fresh address and lasts as long as the listener. The
/// daemon forwards its connections to a local port that any local process can
/// also reach.
///
/// If the daemon drops the control connection, the service ends and
/// [`ChannelBuilder::build`] fails.
#[derive(Debug)]
pub struct Listener {
    local: TcpListener,
    addr: onion::Addr,
    max_message_len: u32,
    // The daemon removes the service when this connection closes.
    control: BufReader<TcpStream>,
}

/// Failure to publish an onion service.
#[derive(Debug, thiserror::Error)]
pub enum ListenError {
    /// The daemon answered a command with something other than success.
    #[error("control port answered: {0}")]
    Refused(String),
    /// The connection to the daemon failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl Listener {
    /// Publish an onion service on `virt_port` through the control port at
    /// `control`, with channels carrying messages of at most
    /// `max_message_len` bytes.
    ///
    /// No step has a deadline; bound the call from outside.
    pub async fn bind(
        control: SocketAddr,
        auth: &ControlAuth,
        virt_port: u16,
        max_message_len: u32,
    ) -> Result<Self, ListenError> {
        let local = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let local_port = local.local_addr()?.port();

        let mut control = BufReader::new(TcpStream::connect(control).await?);
        control::authenticate(&mut control, auth).await?;
        let addr = control::add_onion(&mut control, virt_port, local_port).await?;

        Ok(Self {
            local,
            addr,
            max_message_len,
            control,
        })
    }

    /// Where peers reach this listener.
    pub fn addr(&self) -> &onion::Addr {
        &self.addr
    }
}

impl ChannelBuilder for Listener {
    type Input = ();
    // Peers reach this side by its own onion address, which identifies it.
    type Privacy = Unspecified;
    type Channel = framed::Channel<TcpStream>;
    type BuildError = io::Error;

    async fn build(&mut self, (): &()) -> io::Result<Self::Channel> {
        tokio::select! {
            accepted = self.local.accept() => {
                let (stream, _) = accepted?;
                Ok(framed::Channel::new(stream, self.max_message_len))
            }
            // The daemon sends nothing after publishing, so anything it does
            // send, the end of the connection included, means it left.
            _ = self.control.fill_buf() => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "the daemon closed the control connection",
            )),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
