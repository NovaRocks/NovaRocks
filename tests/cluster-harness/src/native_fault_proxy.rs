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

use anyhow::{Context, Result, bail};
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ProxyMode {
    Forward = 0,
    Paused = 1,
    Drop = 2,
}

impl ProxyMode {
    fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Forward,
            1 => Self::Paused,
            _ => Self::Drop,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyDirection {
    ClientToUpstream,
    UpstreamToClient,
}

struct ProxyState {
    stopped: AtomicBool,
    client_to_upstream: AtomicU8,
    upstream_to_client: AtomicU8,
    retained_bytes: AtomicU64,
    max_retained_bytes: u64,
}

/// A bounded TCP proxy for deterministic Native transport partitions.
///
/// `Paused` stops reading from the selected direction so TCP backpressure, not
/// an unbounded userspace buffer, retains data. `Drop` closes that direction.
pub struct NativeFaultProxy {
    address: SocketAddr,
    state: Arc<ProxyState>,
    accept_thread: Option<JoinHandle<()>>,
}

impl NativeFaultProxy {
    pub fn start(upstream: SocketAddr, max_retained_bytes: u64) -> Result<Self> {
        if max_retained_bytes == 0 {
            bail!("native fault proxy retained-byte limit must be positive");
        }
        let listener = TcpListener::bind("127.0.0.1:0").context("bind native fault proxy")?;
        listener
            .set_nonblocking(true)
            .context("set native fault proxy listener nonblocking")?;
        let address = listener.local_addr().context("read proxy address")?;
        let state = Arc::new(ProxyState {
            stopped: AtomicBool::new(false),
            client_to_upstream: AtomicU8::new(ProxyMode::Forward as u8),
            upstream_to_client: AtomicU8::new(ProxyMode::Forward as u8),
            retained_bytes: AtomicU64::new(0),
            max_retained_bytes,
        });
        let thread_state = Arc::clone(&state);
        let accept_thread = thread::Builder::new()
            .name("native-fault-proxy".to_string())
            .spawn(move || accept_loop(listener, upstream, thread_state))
            .context("spawn native fault proxy")?;
        Ok(Self {
            address,
            state,
            accept_thread: Some(accept_thread),
        })
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn set_mode(&self, direction: ProxyDirection, mode: ProxyMode) {
        let target = match direction {
            ProxyDirection::ClientToUpstream => &self.state.client_to_upstream,
            ProxyDirection::UpstreamToClient => &self.state.upstream_to_client,
        };
        target.store(mode as u8, Ordering::Release);
    }

    pub fn retained_bytes(&self) -> u64 {
        self.state.retained_bytes.load(Ordering::Acquire)
    }

    pub fn stop(&mut self) {
        self.state.stopped.store(true, Ordering::Release);
        let _ = TcpStream::connect_timeout(&self.address, Duration::from_millis(100));
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for NativeFaultProxy {
    fn drop(&mut self) {
        self.stop();
    }
}

fn accept_loop(listener: TcpListener, upstream: SocketAddr, state: Arc<ProxyState>) {
    const MAX_CONNECTIONS: usize = 256;
    let workers = Arc::new(Mutex::new(Vec::<JoinHandle<()>>::new()));
    while !state.stopped.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((client, _)) => {
                let mut workers_guard = workers.lock().expect("proxy worker lock");
                let mut index = 0;
                while index < workers_guard.len() {
                    if workers_guard[index].is_finished() {
                        let worker = workers_guard.swap_remove(index);
                        let _ = worker.join();
                    } else {
                        index += 1;
                    }
                }
                if workers_guard.len() >= MAX_CONNECTIONS {
                    drop(client);
                    continue;
                }
                let state = Arc::clone(&state);
                let worker = thread::spawn(move || {
                    if let Ok(server) =
                        TcpStream::connect_timeout(&upstream, Duration::from_secs(2))
                    {
                        let _ = proxy_connection(client, server, state);
                    }
                });
                workers_guard.push(worker);
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => break,
        }
    }
    for worker in workers.lock().expect("proxy worker lock").drain(..) {
        let _ = worker.join();
    }
}

fn proxy_connection(client: TcpStream, server: TcpStream, state: Arc<ProxyState>) -> Result<()> {
    client
        .set_read_timeout(Some(Duration::from_millis(100)))
        .context("set proxy client timeout")?;
    server
        .set_read_timeout(Some(Duration::from_millis(100)))
        .context("set proxy upstream timeout")?;
    client
        .set_write_timeout(Some(Duration::from_millis(100)))
        .context("set proxy client write timeout")?;
    server
        .set_write_timeout(Some(Duration::from_millis(100)))
        .context("set proxy upstream write timeout")?;
    let client_reader = client.try_clone().context("clone proxy client")?;
    let server_writer = server.try_clone().context("clone proxy upstream")?;
    let forward = {
        let state = Arc::clone(&state);
        thread::spawn(move || {
            copy_direction(
                client_reader,
                server_writer,
                ProxyDirection::ClientToUpstream,
                state,
            )
        })
    };
    copy_direction(
        server,
        client,
        ProxyDirection::UpstreamToClient,
        Arc::clone(&state),
    )?;
    let _ = forward.join();
    Ok(())
}

fn copy_direction(
    mut reader: TcpStream,
    mut writer: TcpStream,
    direction: ProxyDirection,
    state: Arc<ProxyState>,
) -> Result<()> {
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        if state.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        let mode = match direction {
            ProxyDirection::ClientToUpstream => &state.client_to_upstream,
            ProxyDirection::UpstreamToClient => &state.upstream_to_client,
        };
        match ProxyMode::from_raw(mode.load(Ordering::Acquire)) {
            ProxyMode::Drop => {
                let _ = reader.shutdown(Shutdown::Read);
                let _ = writer.shutdown(Shutdown::Write);
                return Ok(());
            }
            ProxyMode::Paused => {
                thread::sleep(Duration::from_millis(5));
                continue;
            }
            ProxyMode::Forward => {}
        }
        let count = match reader.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(count) => count,
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                continue;
            }
            Err(error) => return Err(error).context("read proxy direction"),
        };
        let retained = state
            .retained_bytes
            .fetch_add(count as u64, Ordering::AcqRel)
            + count as u64;
        if retained > state.max_retained_bytes {
            state
                .retained_bytes
                .fetch_sub(count as u64, Ordering::AcqRel);
            bail!("native fault proxy retained-byte limit exceeded");
        }
        let write_result = writer.write_all(&buffer[..count]);
        state
            .retained_bytes
            .fetch_sub(count as u64, Ordering::AcqRel);
        write_result.context("write proxy direction")?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwards_bytes_and_stops_without_leaking_retained_capacity() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind echo server");
        let upstream = listener.local_addr().expect("echo address");
        let echo = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept echo client");
            let mut bytes = [0_u8; 4];
            stream.read_exact(&mut bytes).expect("read echo request");
            stream.write_all(&bytes).expect("write echo response");
        });
        let mut proxy = NativeFaultProxy::start(upstream, 1024).expect("start proxy");
        let mut client = TcpStream::connect(proxy.address()).expect("connect proxy");
        client.write_all(b"ping").expect("write request");
        let mut bytes = [0_u8; 4];
        client.read_exact(&mut bytes).expect("read response");
        assert_eq!(&bytes, b"ping");
        drop(client);
        echo.join().expect("join echo server");
        proxy.stop();
        assert_eq!(proxy.retained_bytes(), 0);
    }

    #[test]
    fn paused_direction_resumes_without_userspace_retention() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind echo server");
        let upstream = listener.local_addr().expect("echo address");
        let echo = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept echo client");
            let mut bytes = [0_u8; 4];
            stream.read_exact(&mut bytes).expect("read echo request");
            stream.write_all(&bytes).expect("write echo response");
        });
        let mut proxy = NativeFaultProxy::start(upstream, 1024).expect("start proxy");
        proxy.set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Paused);
        let mut client = TcpStream::connect(proxy.address()).expect("connect proxy");
        client
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("set client timeout");
        client.write_all(b"ping").expect("write request");
        let mut bytes = [0_u8; 4];
        assert!(client.read_exact(&mut bytes).is_err());
        assert_eq!(proxy.retained_bytes(), 0);
        proxy.set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Forward);
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set client timeout");
        client
            .read_exact(&mut bytes)
            .expect("read resumed response");
        assert_eq!(&bytes, b"ping");
        drop(client);
        echo.join().expect("join echo server");
        proxy.stop();
    }

    #[test]
    fn capacity_violation_closes_direction_and_releases_accounting() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind sink server");
        let upstream = listener.local_addr().expect("sink address");
        let sink = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept sink client");
            let mut bytes = [0_u8; 4];
            let _ = stream.read_exact(&mut bytes);
        });
        let mut proxy = NativeFaultProxy::start(upstream, 1).expect("start proxy");
        let mut client = TcpStream::connect(proxy.address()).expect("connect proxy");
        client.write_all(b"ping").expect("write request");
        thread::sleep(Duration::from_millis(50));
        assert_eq!(proxy.retained_bytes(), 0);
        drop(client);
        proxy.stop();
        sink.join().expect("join sink server");
    }
}
