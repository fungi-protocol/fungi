//! The client side of SOCKS5 `CONNECT`, as far as reaching an onion service
//! needs it (RFC 1928).

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::connector::ConnectError;
use crate::onion;

/// Ask the proxy on `stream` for a tunnel to `addr`.
///
/// On success the next byte read from `stream` is the first from the service.
pub(crate) async fn connect<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    addr: &onion::Addr,
) -> Result<(), ConnectError> {
    stream.write_all(&[VERSION, 1, NO_AUTH]).await?;
    if read::<_, 2>(stream).await? != [VERSION, NO_AUTH] {
        return Err(ConnectError::Malformed);
    }

    // The hostname goes unresolved, for the proxy to look up.
    let host = addr.host().as_bytes();
    let host_len = u8::try_from(host.len()).expect("an onion hostname is 62 bytes");
    let request = [
        &[VERSION, CONNECT, 0, DOMAIN, host_len],
        host,
        &addr.port().to_be_bytes(),
    ]
    .concat();
    stream.write_all(&request).await?;

    let [version, reply, _, bound] = read(stream).await?;
    if version != VERSION {
        return Err(ConnectError::Malformed);
    }
    match reply {
        0 => {}
        // Tor reports a timeout as TTL expired, and with extended errors a
        // missing descriptor, a failed introduction or rendezvous, or an
        // introduction timeout.
        1 | 3..=6 | 0xf0 | 0xf2 | 0xf3 | 0xf7 => return Err(ConnectError::Unreachable),
        code => return Err(ConnectError::Rejected { code }),
    }

    // The proxy's own address and port sit between the reply and the tunnel.
    let bound_len = match bound {
        IPV4 => 4,
        IPV6 => 16,
        DOMAIN => usize::from(stream.read_u8().await?),
        _ => return Err(ConnectError::Malformed),
    };
    let mut skipped = vec![0; bound_len + 2];
    stream.read_exact(&mut skipped).await?;

    Ok(())
}

const VERSION: u8 = 5;
const NO_AUTH: u8 = 0;
const CONNECT: u8 = 1;
const IPV4: u8 = 1;
const DOMAIN: u8 = 3;
const IPV6: u8 = 4;

async fn read<S: AsyncRead + Unpin, const N: usize>(
    stream: &mut S,
) -> Result<[u8; N], std::io::Error> {
    let mut bytes = [0; N];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
