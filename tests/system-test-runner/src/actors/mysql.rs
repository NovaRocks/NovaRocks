use anyhow::{Context, Result};
use mysql::{Conn, OptsBuilder};
use std::time::Duration;

/// Connect through the public MySQL protocol with bounded socket operations.
pub fn connect(user: &str, port: u16, timeout: Duration) -> Result<Conn> {
    connect_with_io_timeout(user, port, timeout, timeout)
}

/// Connect with a distinct bound for socket I/O.
///
/// Performance scenarios use this when the SQL statement and the local job
/// deadline have the same business bound. A small transport grace lets the
/// server return that business result instead of having the synchronous
/// client surface the socket timeout first.
pub fn connect_with_io_timeout(
    user: &str,
    port: u16,
    connect_timeout: Duration,
    io_timeout: Duration,
) -> Result<Conn> {
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .user(Some(user))
        .tcp_connect_timeout(Some(connect_timeout))
        .read_timeout(Some(io_timeout))
        .write_timeout(Some(io_timeout));
    Conn::new(builder).with_context(|| format!("connect MySQL actor at 127.0.0.1:{port}"))
}

/// Connect a query actor whose response is intentionally held until another
/// public MySQL session cancels it. The scenario deadline, not a socket read
/// timeout, bounds the wait: on macOS the synchronous client maps a socket
/// read timeout while awaiting the cancellation response to EAGAIN.
pub fn connect_for_cancellation(user: &str, port: u16, connect_timeout: Duration) -> Result<Conn> {
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .user(Some(user))
        .tcp_connect_timeout(Some(connect_timeout));
    Conn::new(builder)
        .with_context(|| format!("connect cancellation MySQL actor at 127.0.0.1:{port}"))
}
