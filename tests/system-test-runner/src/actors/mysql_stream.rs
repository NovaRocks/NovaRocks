// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Minimal raw MySQL stream actor for protocol-boundary scenarios.
//!
//! The synchronous MySQL client is useful for ordinary SQL assertions, but it
//! deliberately owns response draining. T14 protocol scenarios need exact
//! control of when a schema, a row packet, or a socket close is observed, so
//! they use this actor instead of a second ad-hoc handshake implementation.

use anyhow::{Context, Result, bail, ensure};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream as AsyncTcpStream;
use tokio::time::timeout as async_timeout;

pub struct MysqlStream {
    stream: TcpStream,
}

/// An async form of [`MysqlStream`] for scenarios whose logical client count
/// deliberately exceeds the test process's fixed thread budget. It exposes
/// only the raw protocol operations used by those scenarios; the production
/// MySQL protocol assertions continue to use the synchronous actor above.
pub struct AsyncMysqlStream {
    stream: AsyncTcpStream,
    timeout: Duration,
}

pub struct MysqlPacket {
    sequence: u8,
    payload: Vec<u8>,
}

impl MysqlPacket {
    pub const fn sequence(&self) -> u8 {
        self.sequence
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn is_error(&self) -> bool {
        self.payload.first().copied() == Some(0xff)
    }

    pub fn is_result_terminator(&self) -> bool {
        is_mysql_result_terminator(&self.payload)
    }

    pub fn has_more_results(&self) -> bool {
        mysql_status_flags(&self.payload)
            .map(|flags| flags & 0x0008 != 0)
            .unwrap_or(false)
    }
}

impl MysqlStream {
    pub fn connect(user: &str, port: u16, timeout: Duration) -> Result<Self> {
        Self::connect_with_capabilities(user, port, timeout, false)
    }

    pub fn connect_with_multi_results(user: &str, port: u16, timeout: Duration) -> Result<Self> {
        Self::connect_with_capabilities(user, port, timeout, true)
    }

    fn connect_with_capabilities(
        user: &str,
        port: u16,
        timeout: Duration,
        multi_results: bool,
    ) -> Result<Self> {
        const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
        const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
        const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
        const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
        const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
        const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
        const CLIENT_MULTI_STATEMENTS: u32 = 0x0001_0000;
        const CLIENT_MULTI_RESULTS: u32 = 0x0002_0000;

        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let mut stream = TcpStream::connect_timeout(&address, timeout)
            .with_context(|| format!("connect raw public MySQL client at {address}"))?;
        stream
            .set_read_timeout(Some(timeout))
            .context("set raw MySQL read timeout")?;
        stream
            .set_write_timeout(Some(timeout))
            .context("set raw MySQL write timeout")?;

        let (_, handshake) = read_wire_packet(&mut stream).context("read MySQL handshake")?;
        ensure!(
            handshake.first().copied() == Some(10),
            "expected MySQL protocol v10 handshake, got payload={handshake:?}"
        );

        let mut client_flags = CLIENT_LONG_PASSWORD
            | CLIENT_LONG_FLAG
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH;
        if multi_results {
            client_flags |= CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS;
        }
        let mut response = Vec::with_capacity(user.len() + 64);
        response.extend_from_slice(&client_flags.to_le_bytes());
        response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        response.push(45);
        response.extend_from_slice(&[0u8; 23]);
        response.extend_from_slice(user.as_bytes());
        response.push(0);
        response.push(0);
        response.extend_from_slice(b"mysql_native_password");
        response.push(0);
        write_packet(&mut stream, 1, &response).context("write MySQL handshake response")?;

        let (_, auth_result) =
            read_wire_packet(&mut stream).context("read MySQL authentication result")?;
        if auth_result.first().copied() == Some(0xff) {
            bail!(
                "raw public MySQL authentication failed: {}",
                mysql_error_text(&auth_result)?
            );
        }
        ensure!(
            auth_result.first().copied() == Some(0),
            "unexpected raw MySQL authentication response: {auth_result:?}"
        );
        Ok(Self { stream })
    }

    pub fn query(user: &str, port: u16, sql: &str, timeout: Duration) -> Result<Self> {
        let mut stream = Self::connect(user, port, timeout)?;
        stream.send_query(sql)?;
        Ok(stream)
    }

    pub fn send_query(&mut self, sql: &str) -> Result<()> {
        let mut payload = Vec::with_capacity(sql.len() + 1);
        payload.push(0x03);
        payload.extend_from_slice(sql.as_bytes());
        write_packet(&mut self.stream, 0, &payload).context("write MySQL COM_QUERY packet")
    }

    pub fn expect_ok_packet(&mut self, operation: &str) -> Result<()> {
        let (_, response) = read_wire_packet(&mut self.stream)
            .with_context(|| format!("read response for {operation}"))?;
        if response.first().copied() == Some(0xff) {
            bail!("{operation} failed: {}", mysql_error_text(&response)?);
        }
        ensure!(
            response.first().copied() == Some(0),
            "{operation} expected a MySQL OK packet, got payload={response:?}"
        );
        Ok(())
    }

    /// Reads exactly one server response packet. Protocol scenarios own the
    /// packet sequence and response framing checks above this raw boundary.
    pub fn read_packet(&mut self, operation: &str) -> Result<MysqlPacket> {
        let (sequence, payload) =
            read_wire_packet(&mut self.stream).with_context(|| format!("read {operation}"))?;
        Ok(MysqlPacket { sequence, payload })
    }

    /// Reads the terminal failure of a one-column query after a possible
    /// metadata prefix. A protocol error may occur before schema start, or
    /// after the schema was made visible, but rows and success EOF are never
    /// accepted on this path.
    pub fn read_timeout_query_error(&mut self) -> Result<String> {
        let first = self.read_packet("timed query first response")?;
        if first.is_error() {
            return mysql_error_text(first.payload());
        }

        ensure!(
            first.payload() == [1],
            "expected timed query to begin with one-column metadata or ERR, got payload={:?}",
            first.payload()
        );
        let column = self.read_packet("timed query column metadata")?;
        ensure!(
            !column.is_result_terminator() && !column.is_error(),
            "expected timed query column definition, got payload={:?}",
            column.payload()
        );
        let metadata_end = self.read_packet("timed query metadata terminator")?;
        ensure!(
            metadata_end.is_result_terminator(),
            "expected timed query metadata terminator, got payload={:?}",
            metadata_end.payload()
        );
        let terminal = self.read_packet("timed query terminal error")?;
        mysql_error_text(terminal.payload())
    }

    pub fn shutdown(self) -> Result<()> {
        self.stream
            .shutdown(Shutdown::Both)
            .context("close raw public MySQL client connection")
    }
}

impl AsyncMysqlStream {
    pub async fn connect(user: &str, port: u16, timeout: Duration) -> Result<Self> {
        const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
        const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
        const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
        const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
        const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
        const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let mut stream = async_timeout(timeout, AsyncTcpStream::connect(address))
            .await
            .context("time out connecting raw async public MySQL client")??;

        let (_, handshake) = read_wire_packet_async(&mut stream, timeout)
            .await
            .context("read async MySQL handshake")?;
        ensure!(
            handshake.first().copied() == Some(10),
            "expected MySQL protocol v10 handshake, got payload={handshake:?}"
        );

        let client_flags = CLIENT_LONG_PASSWORD
            | CLIENT_LONG_FLAG
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH;
        let mut response = Vec::with_capacity(user.len() + 64);
        response.extend_from_slice(&client_flags.to_le_bytes());
        response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        response.push(45);
        response.extend_from_slice(&[0u8; 23]);
        response.extend_from_slice(user.as_bytes());
        response.push(0);
        response.push(0);
        response.extend_from_slice(b"mysql_native_password");
        response.push(0);
        write_packet_async(&mut stream, 1, &response, timeout)
            .await
            .context("write async MySQL handshake response")?;

        let (_, auth_result) = read_wire_packet_async(&mut stream, timeout)
            .await
            .context("read async MySQL authentication result")?;
        if auth_result.first().copied() == Some(0xff) {
            bail!(
                "raw async public MySQL authentication failed: {}",
                mysql_error_text(&auth_result)?
            );
        }
        ensure!(
            auth_result.first().copied() == Some(0),
            "unexpected raw async MySQL authentication response: {auth_result:?}"
        );
        Ok(Self { stream, timeout })
    }

    pub async fn send_query(&mut self, sql: &str) -> Result<()> {
        let mut payload = Vec::with_capacity(sql.len() + 1);
        payload.push(0x03);
        payload.extend_from_slice(sql.as_bytes());
        write_packet_async(&mut self.stream, 0, &payload, self.timeout)
            .await
            .context("write async MySQL COM_QUERY packet")
    }

    pub async fn expect_ok_packet(&mut self, operation: &str) -> Result<()> {
        let (_, response) = read_wire_packet_async(&mut self.stream, self.timeout)
            .await
            .with_context(|| format!("read async response for {operation}"))?;
        if response.first().copied() == Some(0xff) {
            bail!("{operation} failed: {}", mysql_error_text(&response)?);
        }
        ensure!(
            response.first().copied() == Some(0),
            "{operation} expected a MySQL OK packet, got payload={response:?}"
        );
        Ok(())
    }

    pub async fn read_timeout_query_error(&mut self) -> Result<String> {
        let (_, first) = read_wire_packet_async(&mut self.stream, self.timeout)
            .await
            .context("read async timed query first response")?;
        if first.first().copied() == Some(0xff) {
            return mysql_error_text(&first);
        }

        ensure!(
            first == [1],
            "expected timed query to begin with one-column metadata or ERR, got payload={first:?}"
        );
        let (_, column) = read_wire_packet_async(&mut self.stream, self.timeout)
            .await
            .context("read async timed query column metadata")?;
        ensure!(
            !is_mysql_result_terminator(&column) && column.first().copied() != Some(0xff),
            "expected timed query column definition, got payload={column:?}"
        );
        let (_, metadata_end) = read_wire_packet_async(&mut self.stream, self.timeout)
            .await
            .context("read async timed query metadata terminator")?;
        ensure!(
            is_mysql_result_terminator(&metadata_end),
            "expected timed query metadata terminator, got payload={metadata_end:?}"
        );
        let (_, terminal) = read_wire_packet_async(&mut self.stream, self.timeout)
            .await
            .context("read async timed query terminal error")?;
        mysql_error_text(&terminal)
    }
}

fn mysql_error_text(payload: &[u8]) -> Result<String> {
    ensure!(
        payload.first().copied() == Some(0xff),
        "expected a MySQL error packet, got payload={payload:?}"
    );
    ensure!(
        payload.len() >= 3,
        "truncated MySQL error packet: {payload:?}"
    );
    let message_offset = if payload.get(3).copied() == Some(b'#') {
        9
    } else {
        3
    };
    Ok(String::from_utf8_lossy(&payload[message_offset..]).into_owned())
}

fn is_mysql_result_terminator(payload: &[u8]) -> bool {
    matches!(payload.first().copied(), Some(0xfe) if payload.len() < 9)
        || payload.first().copied() == Some(0)
}

fn mysql_status_flags(payload: &[u8]) -> Option<u16> {
    match payload.first().copied() {
        Some(0xfe) if payload.len() >= 5 => Some(u16::from_le_bytes([payload[3], payload[4]])),
        Some(0) if payload.len() >= 5 => Some(u16::from_le_bytes([payload[3], payload[4]])),
        _ => None,
    }
}

fn read_wire_packet(stream: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .context("read MySQL packet header")?;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0u8; length];
    stream
        .read_exact(&mut payload)
        .context("read MySQL packet payload")?;
    Ok((header[3], payload))
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) -> Result<()> {
    let length = u32::try_from(payload.len()).context("MySQL packet payload length fits u32")?;
    ensure!(length <= 0x00ff_ffff, "MySQL packet payload is too large");
    let header = [
        (length & 0xff) as u8,
        ((length >> 8) & 0xff) as u8,
        ((length >> 16) & 0xff) as u8,
        sequence,
    ];
    stream
        .write_all(&header)
        .context("write MySQL packet header")?;
    stream
        .write_all(payload)
        .context("write MySQL packet payload")?;
    stream.flush().context("flush MySQL packet")
}

async fn read_wire_packet_async(
    stream: &mut AsyncTcpStream,
    timeout: Duration,
) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    async_timeout(timeout, stream.read_exact(&mut header))
        .await
        .context("time out reading async MySQL packet header")??;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0u8; length];
    async_timeout(timeout, stream.read_exact(&mut payload))
        .await
        .context("time out reading async MySQL packet payload")??;
    Ok((header[3], payload))
}

async fn write_packet_async(
    stream: &mut AsyncTcpStream,
    sequence: u8,
    payload: &[u8],
    timeout: Duration,
) -> Result<()> {
    let length = u32::try_from(payload.len()).context("MySQL packet payload length fits u32")?;
    ensure!(length <= 0x00ff_ffff, "MySQL packet payload is too large");
    let header = [
        (length & 0xff) as u8,
        ((length >> 8) & 0xff) as u8,
        ((length >> 16) & 0xff) as u8,
        sequence,
    ];
    async_timeout(timeout, stream.write_all(&header))
        .await
        .context("time out writing async MySQL packet header")??;
    async_timeout(timeout, stream.write_all(payload))
        .await
        .context("time out writing async MySQL packet payload")??;
    async_timeout(timeout, stream.flush())
        .await
        .context("time out flushing async MySQL packet")??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::mysql_error_text;

    #[test]
    fn mysql_error_text_reads_sqlstate_and_plain_packets() {
        assert_eq!(
            mysql_error_text(&[0xff, 0x01, 0x00, b'#', b'H', b'Y', b'0', b'0', b'0', b'x'])
                .expect("sqlstate packet"),
            "x"
        );
        assert_eq!(
            mysql_error_text(&[0xff, 0x01, 0x00, b'x']).expect("plain packet"),
            "x"
        );
    }
}
