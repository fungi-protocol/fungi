//! The client side of the daemon's control port, as far as publishing an
//! onion service needs it.

use std::fmt;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use crate::listener::ListenError;
use crate::onion;

/// How to authenticate on the control port.
pub enum ControlAuth {
    /// The daemon asks for no credentials.
    Null,
    /// The daemon asks for the contents of its `control_auth_cookie` file.
    Cookie(Vec<u8>),
}

/// Leaves out the cookie, which grants control of the daemon.
impl fmt::Debug for ControlAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("Null"),
            Self::Cookie(_) => f.write_str("Cookie(..)"),
        }
    }
}

pub(crate) async fn authenticate<C: AsyncBufRead + AsyncWrite + Unpin>(
    control: &mut C,
    auth: &ControlAuth,
) -> Result<(), ListenError> {
    let command = match auth {
        ControlAuth::Null => "AUTHENTICATE\r\n".to_owned(),
        ControlAuth::Cookie(cookie) => {
            let hex: String = cookie.iter().map(|byte| format!("{byte:02X}")).collect();
            format!("AUTHENTICATE {hex}\r\n")
        }
    };
    control.write_all(command.as_bytes()).await?;

    let line = read_line(control).await?;
    match Reply::parse(&line) {
        Some(Reply::Last(_)) => Ok(()),
        _ => Err(ListenError::Refused(line)),
    }
}

/// Publish a service forwarding `virt_port` to `local_port` on this host.
///
/// The key is discarded, so the address cannot be published again, and the
/// service lasts until `control` closes.
pub(crate) async fn add_onion<C: AsyncBufRead + AsyncWrite + Unpin>(
    control: &mut C,
    virt_port: u16,
    local_port: u16,
) -> Result<onion::Addr, ListenError> {
    let command = format!(
        "ADD_ONION NEW:ED25519-V3 Flags=DiscardPK Port={virt_port},127.0.0.1:{local_port}\r\n"
    );
    control.write_all(command.as_bytes()).await?;

    let mut service = None;
    loop {
        let line = read_line(control).await?;
        match Reply::parse(&line) {
            Some(Reply::More(text)) => {
                if let Some(id) = text.strip_prefix("ServiceID=") {
                    let addr = onion::Addr::new(format!("{id}.onion"), virt_port);
                    service = Some(addr.map_err(|_| ListenError::Refused(line))?);
                }
            }
            // The daemon sends nothing after its last line, so anything but
            // success has to end the wait here.
            Some(Reply::Last("OK")) => return service.ok_or(ListenError::Refused(line)),
            _ => return Err(ListenError::Refused(line)),
        }
    }
}

/// One line of a successful answer.
enum Reply<'a> {
    /// `250-text`: more lines follow.
    More(&'a str),
    /// `250 text`: the answer ends here.
    Last(&'a str),
}

impl<'a> Reply<'a> {
    fn parse(line: &'a str) -> Option<Self> {
        let more = line.strip_prefix("250-").map(Reply::More);
        let last = line.strip_prefix("250 ").map(Reply::Last);

        more.or(last)
    }
}

async fn read_line<C: AsyncBufRead + Unpin>(control: &mut C) -> std::io::Result<String> {
    let mut line = String::new();
    if control.read_line(&mut line).await? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }

    Ok(line.trim_end().to_owned())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
