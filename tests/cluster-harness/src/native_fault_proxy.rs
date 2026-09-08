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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyMode {
    Forward,
    Paused,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyDirection {
    ClientToUpstream,
    UpstreamToClient,
}

struct ProxyState {
    stopped: AtomicBool,
    modes: Mutex<[ProxyMode; 2]>,
    connection_generation: AtomicU64,
    active_connections: AtomicU64,
    retained_bytes: AtomicU64,
    peak_retained_bytes: AtomicU64,
    max_retained_bytes: u64,
}

/// A non-owning control handle; dropping it never stops the cluster's proxy.
#[derive(Clone)]
pub struct NativeFaultProxyControl {
    address: SocketAddr,
    state: Arc<ProxyState>,
}

impl NativeFaultProxyControl {
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Pause/resume is serialized with nonblocking socket I/O. Drop rejects
    /// the selected direction by closing its connection, including new ones.
    pub fn set_mode(&self, direction: ProxyDirection, mode: ProxyMode) {
        let index = match direction {
            ProxyDirection::ClientToUpstream => 0,
            ProxyDirection::UpstreamToClient => 1,
        };
        self.state.modes.lock().expect("proxy mode lock")[index] = mode;
    }

    /// Invalidate existing connections while preserving the listening port
    /// and fault modes. A restarted BE must never reuse the old TCP streams.
    pub fn disconnect_all(&self) {
        self.state
            .connection_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    /// Allocated forwarding-buffer capacity, excluding kernel socket buffers.
    pub fn retained_bytes(&self) -> u64 {
        self.state.retained_bytes.load(Ordering::Acquire)
    }

    pub fn peak_retained_bytes(&self) -> u64 {
        self.state.peak_retained_bytes.load(Ordering::Acquire)
    }

    pub fn active_connections(&self) -> u64 {
        self.state.active_connections.load(Ordering::Acquire)
    }

    pub fn is_stopped(&self) -> bool {
        self.state.stopped.load(Ordering::Acquire)
    }
}

/// A bounded TCP proxy for deterministic Native transport partitions.
///
/// `Paused` stops reading from the selected direction so TCP backpressure, not
/// an unbounded userspace buffer, retains data. Already buffered bytes remain
/// charged until forwarded or the connection is closed. One bounded worker
/// owns both directions so an error cannot detach the opposite direction.
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
            modes: Mutex::new([ProxyMode::Forward; 2]),
            connection_generation: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            retained_bytes: AtomicU64::new(0),
            peak_retained_bytes: AtomicU64::new(0),
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

    pub fn control(&self) -> NativeFaultProxyControl {
        NativeFaultProxyControl {
            address: self.address,
            state: Arc::clone(&self.state),
        }
    }

    pub fn set_mode(&self, direction: ProxyDirection, mode: ProxyMode) {
        self.control().set_mode(direction, mode);
    }

    pub fn retained_bytes(&self) -> u64 {
        self.state.retained_bytes.load(Ordering::Acquire)
    }

    pub fn stop(&mut self) {
        self.state.stopped.store(true, Ordering::Release);
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
    let mut workers = Vec::<JoinHandle<()>>::new();
    while !state.stopped.load(Ordering::Acquire) {
        let mut index = 0;
        while index < workers.len() {
            if workers[index].is_finished() {
                let _ = workers.swap_remove(index).join();
            } else {
                index += 1;
            }
        }
        match listener.accept() {
            Ok((client, _)) => {
                if workers.len() >= MAX_CONNECTIONS {
                    drop(client);
                    continue;
                }
                let generation = state.connection_generation.load(Ordering::Acquire);
                let state = Arc::clone(&state);
                if let Ok(worker) = thread::Builder::new()
                    .name("native-proxy-connection".to_string())
                    .spawn(move || {
                        let _connection = ActiveConnection::new(Arc::clone(&state));
                        if let Ok(server) =
                            TcpStream::connect_timeout(&upstream, Duration::from_secs(2))
                        {
                            let _ = proxy_connection(client, server, state, generation);
                        }
                    })
                {
                    workers.push(worker);
                }
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => break,
        }
    }
    // Every worker observes stopped, even while paused or waiting for bytes.
    // The only blocking operation is a bounded two-second upstream connect.
    state.stopped.store(true, Ordering::Release);
    for worker in workers {
        let _ = worker.join();
    }
}

struct ActiveConnection(Arc<ProxyState>);

impl ActiveConnection {
    fn new(state: Arc<ProxyState>) -> Self {
        state.active_connections.fetch_add(1, Ordering::AcqRel);
        Self(state)
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::AcqRel);
    }
}

struct BufferCredit {
    state: Arc<ProxyState>,
    bytes: u64,
}

impl BufferCredit {
    fn acquire(state: &Arc<ProxyState>) -> Option<Self> {
        let mut current = state.retained_bytes.load(Ordering::Acquire);
        loop {
            let bytes = (state.max_retained_bytes - current).min(16 * 1024);
            if bytes == 0 {
                return None;
            }
            match state.retained_bytes.compare_exchange_weak(
                current,
                current + bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    state
                        .peak_retained_bytes
                        .fetch_max(current + bytes, Ordering::AcqRel);
                    return Some(Self {
                        state: Arc::clone(state),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for BufferCredit {
    fn drop(&mut self) {
        self.state
            .retained_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct PendingBytes {
    buffer: Vec<u8>,
    length: usize,
    written: usize,
    _credit: BufferCredit,
}

#[derive(Default)]
struct DirectionPump {
    pending: Option<PendingBytes>,
    eof: bool,
}

impl DirectionPump {
    fn advance(
        &mut self,
        reader: &mut TcpStream,
        writer: &mut TcpStream,
        mode: ProxyMode,
        state: &Arc<ProxyState>,
    ) -> Result<bool> {
        match mode {
            ProxyMode::Drop => bail!("native proxy direction dropped"),
            ProxyMode::Paused => return Ok(false),
            ProxyMode::Forward => {}
        }
        if let Some(pending) = self.pending.as_mut() {
            match writer.write(&pending.buffer[pending.written..pending.length]) {
                Ok(0) => bail!("native proxy destination closed"),
                Ok(count) => {
                    pending.written += count;
                    if pending.written == pending.length {
                        self.pending = None;
                    }
                    return Ok(true);
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(false),
                Err(error) => return Err(error).context("write proxy direction"),
            }
        }
        if self.eof {
            return Ok(false);
        }
        // Readiness probing does not consume or retain stream bytes. Avoid
        // allocating a charged buffer for every idle connection on each poll.
        match reader.peek(&mut [0_u8; 1]) {
            Ok(0) => {
                self.eof = true;
                let _ = writer.shutdown(Shutdown::Write);
                return Ok(true);
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(error).context("probe proxy direction"),
        }
        let Some(credit) = BufferCredit::acquire(state) else {
            return Ok(false);
        };
        let mut buffer = vec![0; credit.bytes as usize];
        match reader.read(&mut buffer) {
            Ok(0) => {
                self.eof = true;
                let _ = writer.shutdown(Shutdown::Write);
                Ok(true)
            }
            Ok(length) => {
                self.pending = Some(PendingBytes {
                    buffer,
                    length,
                    written: 0,
                    _credit: credit,
                });
                Ok(true)
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(error).context("read proxy direction"),
        }
    }
}

fn proxy_connection(
    mut client: TcpStream,
    mut server: TcpStream,
    state: Arc<ProxyState>,
    generation: u64,
) -> Result<()> {
    client
        .set_nonblocking(true)
        .context("set proxy client nonblocking")?;
    server
        .set_nonblocking(true)
        .context("set proxy upstream nonblocking")?;
    let mut request = DirectionPump::default();
    let mut response = DirectionPump::default();
    while !state.stopped.load(Ordering::Acquire)
        && generation == state.connection_generation.load(Ordering::Acquire)
    {
        let progressed = {
            let modes = state.modes.lock().expect("proxy mode lock");
            let request_progress = request.advance(&mut client, &mut server, modes[0], &state)?;
            let response_progress = response.advance(&mut server, &mut client, modes[1], &state)?;
            request_progress || response_progress
        };
        if request.eof && response.eof {
            break;
        }
        if !progressed {
            thread::sleep(Duration::from_millis(5));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn wait_until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !predicate() {
            assert!(
                Instant::now() < deadline,
                "proxy condition did not converge"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

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
    fn tiny_capacity_forwards_without_exceeding_the_budget() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind echo server");
        let upstream = listener.local_addr().expect("echo address");
        let echo = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept echo client");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut bytes = [0_u8; 4];
            stream.read_exact(&mut bytes).expect("read request");
            stream.write_all(&bytes).expect("write response");
        });
        let mut proxy = NativeFaultProxy::start(upstream, 1).expect("start proxy");
        let mut client = TcpStream::connect(proxy.address()).expect("connect proxy");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client.write_all(b"ping").expect("write request");
        let mut bytes = [0; 4];
        client
            .read_exact(&mut bytes)
            .expect("read response through tiny budget");
        assert_eq!(&bytes, b"ping");
        drop(client);
        proxy.stop();
        assert_eq!(proxy.retained_bytes(), 0);
        assert_eq!(proxy.control().peak_retained_bytes(), 1);
        echo.join().expect("join echo server");
    }

    #[test]
    fn response_pause_and_drop_can_be_changed_on_live_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream");
        let mut proxy = NativeFaultProxy::start(listener.local_addr().unwrap(), 1024).unwrap();
        let control = proxy.control();
        let mut client = TcpStream::connect(proxy.address()).unwrap();
        client
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let (mut upstream, _) = listener.accept().unwrap();
        upstream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        control.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Paused);
        upstream.write_all(b"response").unwrap();
        let mut bytes = [0; 8];
        assert!(client.read_exact(&mut bytes).is_err());
        // A response partition must not prevent requests from reaching the BE.
        client.write_all(b"request!").unwrap();
        upstream.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"request!");
        control.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Forward);
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"response");
        control.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Drop);
        wait_until(|| control.active_connections() == 0);
        assert_eq!(client.read(&mut bytes).unwrap(), 0);
        control.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Forward);
        let mut replacement = TcpStream::connect(proxy.address()).unwrap();
        replacement
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (mut replacement_upstream, _) = listener.accept().unwrap();
        replacement_upstream.write_all(b"new").unwrap();
        let mut new_bytes = [0; 3];
        replacement.read_exact(&mut new_bytes).unwrap();
        assert_eq!(&new_bytes, b"new");
        proxy.stop();
        assert_eq!(control.active_connections(), 0);
    }

    #[test]
    fn disconnect_and_stop_release_paused_connections_and_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut proxy = NativeFaultProxy::start(listener.local_addr().unwrap(), 1024).unwrap();
        let control = proxy.control();
        control.set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Paused);
        control.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Paused);
        let client = TcpStream::connect(proxy.address()).unwrap();
        let (upstream, _) = listener.accept().unwrap();
        wait_until(|| control.active_connections() == 1);
        control.disconnect_all();
        wait_until(|| control.active_connections() == 0);
        drop((client, upstream));
        let replacement = TcpStream::connect(proxy.address()).unwrap();
        let (replacement_upstream, _) = listener.accept().unwrap();
        wait_until(|| control.active_connections() == 1);
        let started = Instant::now();
        proxy.stop();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(control.is_stopped());
        assert_eq!(control.active_connections(), 0);
        assert_eq!(control.retained_bytes(), 0);
        let rebound = TcpListener::bind(control.address()).expect("proxy listener was released");
        drop((replacement, replacement_upstream, rebound));
        proxy.stop();
    }
}
