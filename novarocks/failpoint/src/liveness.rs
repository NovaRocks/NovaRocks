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

//! One private liveness connection per role. It carries no domain commands.

use crate::session::{
    HEARTBEAT_INTERVAL, HEARTBEAT_LOSS, LivenessFrame, SessionDeclaration, SessionRole, invalid,
    read_frame_with, secret_matches, write_frame,
};
use std::io::{self, Read};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Nonblocking connection work retains one absolute deadline and cancellation.
#[cfg(novarocks_dev_controls)]
pub fn connect_until(
    path: &std::path::Path,
    deadline: Instant,
    cancelled: impl Fn() -> bool,
) -> io::Result<UnixStream> {
    use socket2::{Domain, SockAddr, Socket, Type};
    use std::os::fd::OwnedFd;
    let address = SockAddr::unix(path)?;
    let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    socket.set_nonblocking(true)?;
    loop {
        if cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "control connection was cancelled",
            ));
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "control connection deadline expired",
            ));
        }
        match socket.connect(&address) {
            Ok(()) => break,
            Err(error) if error.raw_os_error() == Some(libc::EISCONN) => break,
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || matches!(
                        error.raw_os_error(),
                        Some(libc::EINPROGRESS) | Some(libc::EALREADY)
                    ) =>
            {
                if let Some(error) = socket.take_error()? {
                    return Err(error);
                }
                if socket.peer_addr().is_ok() {
                    break;
                }
            }
            Err(error) => return Err(error),
        }
        std::thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(20)),
        );
    }
    if cancelled() {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "control connection was cancelled",
        ));
    }
    if Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "control connection deadline expired",
        ));
    }
    socket.set_nonblocking(false)?;
    let descriptor: OwnedFd = socket.into();
    Ok(UnixStream::from(descriptor))
}

fn read_exact_until(
    stream: &mut UnixStream,
    bytes: &mut [u8],
    deadline: Instant,
    stop: &AtomicBool,
) -> io::Result<()> {
    let mut position = 0;
    while position < bytes.len() {
        if stop.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "test-session owner ended",
            ));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "test-session liveness deadline expired",
            ));
        }
        stream.set_read_timeout(Some(remaining.min(Duration::from_millis(100))))?;
        match stream.read(&mut bytes[position..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "test-session liveness disconnected",
                ));
            }
            Ok(count) => position += count,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error),
        }
    }
    if Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "test-session liveness deadline expired",
        ));
    }
    Ok(())
}

fn frame_until(
    stream: &mut UnixStream,
    deadline: Instant,
    stop: &AtomicBool,
) -> io::Result<LivenessFrame> {
    let frame = read_frame_with(&mut |bytes| read_exact_until(stream, bytes, deadline, stop))?;
    if Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "test-session liveness deadline expired",
        ));
    }
    Ok(frame)
}

// Own the exact socket inode from bind onward, including startup failure paths.
struct BoundSocketFile {
    path: PathBuf,
    device: u64,
    inode: u64,
}
impl BoundSocketFile {
    fn capture(path: PathBuf) -> io::Result<Self> {
        let metadata = std::fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_socket() {
            return Err(invalid("test-session endpoint is not the bound socket"));
        }
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    fn remove(&self) -> io::Result<()> {
        let metadata = match std::fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if !metadata.file_type().is_socket()
            || metadata.dev() != self.device
            || metadata.ino() != self.inode
        {
            return Err(invalid(
                "test-session socket path was replaced; refusing cleanup",
            ));
        }
        std::fs::remove_file(&self.path)
    }
}
impl Drop for BoundSocketFile {
    fn drop(&mut self) {
        let _ = self.remove();
    }
}

fn with_cleanup(primary: io::Result<()>, cleanup: io::Result<()>) -> io::Result<()> {
    match (primary, cleanup) {
        (Ok(()), result) | (result, Ok(())) => result,
        (Err(primary), Err(cleanup)) => Err(io::Error::new(
            primary.kind(),
            format!("{primary}; test-session cleanup also failed: {cleanup}"),
        )),
    }
}

struct RunnerState {
    bound_process: Option<String>,
    failure: Option<String>,
}

/// The endpoint exists before spawn; Drop actually joins its one helper.
pub struct RunnerSession {
    declaration: SessionDeclaration,
    deadline: Instant,
    stop: Arc<AtomicBool>,
    state: Arc<Mutex<RunnerState>>,
    worker: Option<JoinHandle<io::Result<()>>>,
    socket: BoundSocketFile,
}

impl RunnerSession {
    pub fn start(declaration: SessionDeclaration) -> io::Result<Self> {
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(declaration.lifetime_millis))
            .ok_or_else(|| invalid("test-session lifetime cannot be represented"))?;
        Self::start_until(declaration, deadline)
    }

    /// Charge setup and liveness to the original caller deadline without renewal.
    #[expect(
        clippy::disallowed_methods,
        reason = "The development liveness service owns its worker lifecycle; see ADR-0218."
    )]
    pub fn start_until(
        declaration: SessionDeclaration,
        original_deadline: Instant,
    ) -> io::Result<Self> {
        declaration.validate_envelope()?;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(declaration.lifetime_millis))
            .ok_or_else(|| invalid("test-session lifetime cannot be represented"))?
            .min(original_deadline);
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "test-session original deadline expired before startup",
            ));
        }
        let listener = UnixListener::bind(&declaration.liveness_socket)?;
        let socket = BoundSocketFile::capture(declaration.liveness_socket.clone())?;
        if let Err(error) = listener.set_nonblocking(true) {
            return with_cleanup(Err(error), socket.remove()).map(|_| unreachable!());
        }
        let stop = Arc::new(AtomicBool::new(false));
        let state = Arc::new(Mutex::new(RunnerState {
            bound_process: None,
            failure: None,
        }));
        let worker_stop = Arc::clone(&stop);
        let worker_state = Arc::clone(&state);
        let input = declaration.clone();
        let worker = std::thread::Builder::new()
            .name("role-test-liveness-client".into())
            .spawn(move || {
                let run = || -> io::Result<()> {
                    let mut stream = loop {
                        if worker_stop.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        if Instant::now() >= deadline {
                            return Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "test-session expired before role handshake",
                            ));
                        }
                        match listener.accept() {
                            Ok((stream, _)) => break stream,
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                std::thread::sleep(Duration::from_millis(20))
                            }
                            Err(error) => return Err(error),
                        }
                    };
                    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
                    let LivenessFrame::Hello {
                        session_id,
                        launch_id,
                        role,
                        process_id,
                        handshake_secret,
                    } = frame_until(
                        &mut stream,
                        deadline.min(Instant::now() + HEARTBEAT_LOSS),
                        &worker_stop,
                    )?
                    else {
                        return Err(invalid("test-session requires an authenticated Hello"));
                    };
                    if session_id != input.session_id
                        || launch_id != input.launch_id
                        || role != input.role
                        || !secret_matches(&input.handshake_secret, &handshake_secret)
                    {
                        return Err(invalid(
                            "test-session Hello does not match this role launch",
                        ));
                    }
                    // Validate canonical Native UUIDv7 using the existing identity owner.
                    match role {
                        SessionRole::Fe => {
                            process_id
                                .parse::<novarocks_types::FrontendProcessId>()
                                .map_err(|_| invalid("invalid frontend process identity"))?;
                        }
                        SessionRole::Be => {
                            process_id
                                .parse::<novarocks_types::BackendProcessId>()
                                .map_err(|_| invalid("invalid backend process identity"))?;
                        }
                    }
                    crate::target_binding::RoleProcessReceipt::from_hello(
                        &input,
                        process_id.clone(),
                    )?
                    .write_private(&input.liveness_socket.with_extension("process.json"))?;
                    let remaining = deadline
                        .saturating_duration_since(Instant::now())
                        .as_millis();
                    let remaining = u64::try_from(remaining)
                        .map_err(|_| invalid("test-session remaining lifetime overflow"))?;
                    if remaining == 0 {
                        return Err(invalid("test-session expired during handshake"));
                    }
                    write_frame(
                        &mut stream,
                        &LivenessFrame::Bound {
                            remaining_millis: remaining,
                        },
                    )?;
                    worker_state
                        .lock()
                        .map_err(|_| invalid("runner test-session state lock failed"))?
                        .bound_process = Some(process_id);
                    let mut next = Instant::now();
                    loop {
                        if worker_stop.load(Ordering::Acquire) || Instant::now() >= deadline {
                            let _ = write_frame(&mut stream, &LivenessFrame::End);
                            return Ok(());
                        }
                        if Instant::now() >= next {
                            if let Err(error) = write_frame(&mut stream, &LivenessFrame::Heartbeat)
                            {
                                if matches!(
                                    error.kind(),
                                    io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                                ) {
                                    return Ok(());
                                }
                                return Err(error);
                            }
                            next = Instant::now() + HEARTBEAT_INTERVAL;
                        }
                        std::thread::sleep(Duration::from_millis(20));
                    }
                };
                let result = run();
                if let Err(error) = &result
                    && let Ok(mut state) = worker_state.lock()
                {
                    state.failure = Some(error.to_string());
                }
                result
            });
        let worker = match worker {
            Ok(worker) => worker,
            Err(error) => return with_cleanup(Err(error), socket.remove()).map(|_| unreachable!()),
        };
        Ok(Self {
            declaration,
            deadline,
            stop,
            state,
            worker: Some(worker),
            socket,
        })
    }
    pub fn process_receipt_path(&self) -> PathBuf {
        self.declaration
            .liveness_socket
            .with_extension("process.json")
    }
    pub fn declaration(&self) -> &SessionDeclaration {
        &self.declaration
    }
    pub fn bound_process(&self) -> io::Result<Option<String>> {
        let state = self
            .state
            .lock()
            .map_err(|_| invalid("runner test-session state lock failed"))?;
        if state.failure.is_some() {
            return Err(invalid("runner test-session handshake failed"));
        }
        Ok(state.bound_process.clone())
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn end_and_join(&mut self) -> io::Result<()> {
        self.stop.store(true, Ordering::Release);
        let result = match self.worker.take() {
            Some(worker) => worker
                .join()
                .unwrap_or_else(|_| Err(invalid("runner test-session helper panicked"))),
            None => Ok(()),
        };
        with_cleanup(result, self.socket.remove())
    }
}

impl Drop for RunnerSession {
    fn drop(&mut self) {
        let _ = self.end_and_join();
    }
}

#[cfg(novarocks_dev_controls)]
mod receiver {
    use super::*;
    use crate::scope::{CloseCause, ProcessBinding, SessionScope};
    use std::fs::OpenOptions;
    use std::path::Path;

    #[derive(Clone)]
    pub struct PreparedRoleSession {
        declaration: SessionDeclaration,
        bound: Arc<AtomicBool>,
    }
    impl std::fmt::Debug for PreparedRoleSession {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PreparedRoleSession")
                .field("declaration", &self.declaration)
                .finish_non_exhaustive()
        }
    }
    impl PreparedRoleSession {
        pub fn read_and_claim(path: &Path, expected_role: SessionRole) -> io::Result<Self> {
            let declaration = SessionDeclaration::read_bounded(path)?;
            if declaration.role != expected_role {
                return Err(invalid("test-session declaration has the wrong role"));
            }
            let claim = path.with_extension("claimed");
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
            let _claim = options.open(claim)?;
            Ok(Self {
                declaration,
                bound: Arc::new(AtomicBool::new(false)),
            })
        }
        pub fn declaration(&self) -> &SessionDeclaration {
            &self.declaration
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "The authorized development liveness receiver owns its worker lifecycle; see ADR-0218."
        )]
        pub fn bind(&self, process: ProcessBinding) -> io::Result<RoleSessionOwner> {
            if process.role() != self.declaration.role {
                return Err(invalid("test-session declaration belongs to another role"));
            }
            if self.bound.swap(true, Ordering::AcqRel) {
                return Err(invalid("test-session declaration was already bound"));
            }
            let started = Instant::now();
            let waiting = started
                .checked_add(Duration::from_millis(self.declaration.lifetime_millis))
                .ok_or_else(|| invalid("test-session lifetime cannot be represented"))?;
            let stop = Arc::new(AtomicBool::new(false));
            let mut stream = connect_until(
                &self.declaration.liveness_socket,
                waiting.min(started + HEARTBEAT_LOSS),
                || stop.load(Ordering::Acquire),
            )?;
            stream.set_write_timeout(Some(Duration::from_secs(1)))?;
            write_frame(
                &mut stream,
                &LivenessFrame::Hello {
                    session_id: self.declaration.session_id.clone(),
                    launch_id: self.declaration.launch_id.clone(),
                    role: process.role(),
                    process_id: process.process_id(),
                    handshake_secret: self.declaration.handshake_secret.clone(),
                },
            )?;
            let LivenessFrame::Bound { remaining_millis } =
                frame_until(&mut stream, waiting.min(started + HEARTBEAT_LOSS), &stop)?
            else {
                return Err(invalid("test-session handshake did not bind"));
            };
            if remaining_millis == 0 || remaining_millis > self.declaration.lifetime_millis {
                return Err(invalid(
                    "test-session handshake returned an invalid remaining lifetime",
                ));
            }
            // Starting before Hello conservatively charges handshake work instead of renewing it.
            let deadline = started
                .checked_add(Duration::from_millis(remaining_millis))
                .ok_or_else(|| invalid("test-session remaining lifetime cannot be represented"))?;
            let scope = SessionScope::bind(self.declaration.clone(), process, deadline)?;
            let wake_socket = stream.try_clone()?;
            let observed = scope.clone();
            let worker_stop = Arc::clone(&stop);
            let worker = std::thread::Builder::new()
                .name("role-test-liveness-receiver".into())
                .spawn(move || {
                    let mut last = Instant::now();
                    loop {
                        if worker_stop.load(Ordering::Acquire) || !observed.is_active() {
                            break;
                        }
                        let read_deadline = observed.deadline().min(last + HEARTBEAT_LOSS);
                        match frame_until(&mut stream, read_deadline, &worker_stop) {
                            Ok(_) if Instant::now() >= read_deadline => {
                                observed.close(if Instant::now() >= observed.deadline() {
                                    CloseCause::Deadline
                                } else {
                                    CloseCause::HeartbeatLost
                                });
                                break;
                            }
                            Ok(LivenessFrame::Heartbeat) => last = Instant::now(),
                            Ok(LivenessFrame::End) => {
                                observed.close(CloseCause::ExplicitEnd);
                                break;
                            }
                            Ok(_) => {
                                observed.close(CloseCause::Disconnected);
                                break;
                            }
                            Err(error) if error.kind() == io::ErrorKind::TimedOut => {
                                observed.close(if Instant::now() >= observed.deadline() {
                                    CloseCause::Deadline
                                } else {
                                    CloseCause::HeartbeatLost
                                });
                                break;
                            }
                            Err(_) => {
                                observed.close(CloseCause::Disconnected);
                                break;
                            }
                        }
                    }
                })?;
            Ok(RoleSessionOwner {
                scope,
                stop,
                wake_socket,
                worker: Some(worker),
            })
        }
    }

    pub struct RoleSessionOwner {
        scope: SessionScope,
        stop: Arc<AtomicBool>,
        wake_socket: UnixStream,
        worker: Option<JoinHandle<()>>,
    }
    impl RoleSessionOwner {
        pub fn scope(&self) -> &SessionScope {
            &self.scope
        }
        pub fn close(&self) {
            self.scope.close(CloseCause::RoleStopped);
            self.stop.store(true, Ordering::Release);
            let _ = self.wake_socket.shutdown(std::net::Shutdown::Both);
        }
        pub fn close_and_join(&mut self) -> io::Result<()> {
            self.close();
            if let Some(worker) = self.worker.take() {
                worker
                    .join()
                    .map_err(|_| invalid("role test-session receiver panicked"))?;
            }
            Ok(())
        }
    }
    impl Drop for RoleSessionOwner {
        fn drop(&mut self) {
            let _ = self.close_and_join();
        }
    }
}

#[cfg(novarocks_dev_controls)]
pub use receiver::{PreparedRoleSession, RoleSessionOwner};

#[cfg(all(test, novarocks_dev_controls))]
mod tests {
    use super::*;
    #[test]
    fn ready_bytes_cannot_revive_an_expired_frame_deadline() {
        let (mut reader, mut writer) = UnixStream::pair().unwrap();
        write_frame(&mut writer, &LivenessFrame::Heartbeat).unwrap();
        let stopped = AtomicBool::new(false);
        let error = frame_until(
            &mut reader,
            Instant::now() - Duration::from_millis(1),
            &stopped,
        )
        .err()
        .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn a_helper_panic_still_removes_its_exact_owned_socket() {
        let socket = std::env::temp_dir().join(format!("g2-panic-{}.sock", std::process::id()));
        let listener = UnixListener::bind(&socket).unwrap();
        let lease = BoundSocketFile::capture(socket.clone()).unwrap();
        let mut runner = RunnerSession {
            declaration: SessionDeclaration {
                schema_version: 1,
                session_id: "panic-test".into(),
                launch_id: "panic-launch".into(),
                role: SessionRole::Be,
                lifetime_millis: 1000,
                liveness_socket: socket.clone(),
                handshake_secret: "a".repeat(32),
                domain_grants: vec![],
            },
            deadline: Instant::now() + Duration::from_secs(1),
            stop: Arc::new(AtomicBool::new(false)),
            state: Arc::new(Mutex::new(RunnerState {
                bound_process: None,
                failure: None,
            })),
            worker: Some(std::thread::spawn(move || {
                drop(listener);
                panic!("intentional helper failure");
            })),
            socket: lease,
        };
        assert!(runner.end_and_join().is_err());
        assert!(!socket.exists());
        assert!(runner.worker.is_none());
    }
}
