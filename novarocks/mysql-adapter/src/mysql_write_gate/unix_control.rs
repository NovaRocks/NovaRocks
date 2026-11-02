// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Fixture-only Unix control boundary for the exact MySQL write gate.
//! This future spawns nothing. The service owner must await the original MySQL future.
//! Private binary TLV belongs to the opt-in fixture, not the production protocol/API.

use super::late_binding::{MysqlWriteGateController, MysqlWriteGateHub, MysqlWriteHubSnapshot};
use super::{GateFailure, GatePhase, MysqlWriteGateSnapshot};
use novarocks_types::FrontendProcessId;
use opensrv_mysql::{FramingCursor, WritePhase};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Component, PathBuf};
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

const FRAME_WIRE_CAP: usize = 4096;
const HEADER_BYTES: usize = 4;
const BODY_CAP: usize = FRAME_WIRE_CAP - HEADER_BYTES;
const COMMAND_CAP: u8 = 16;
// A conservative, explicitly frozen test bound; not a production path setting.
const SOCKET_PATH_CAP: usize = 90;
#[derive(Clone, Copy)]
#[repr(u8)]
enum WireVersion {
    V1 = 1,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    V2 = 2,
}
const ARM: u8 = 1;
const SNAPSHOT: u8 = 2;
const STOP: u8 = 3;

#[derive(Clone, Copy, Debug)]
pub(crate) enum ControlClass {
    Path,
    Peer,
    Io,
    Eof,
    Deadline,
    Length,
    Fields,
    Identity,
    CommandLimit,
    State,
    Hub,
    Panic,
    Cleanup,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum ControlStage {
    Startup,
    Accept,
    ReadHeader,
    ReadBody,
    Decode,
    Apply,
    WriteReply,
    Cleanup,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct PrefixSummary {
    pub declared_wire_bytes: Option<u64>,
    pub observed_bytes: u64,
    pub sha256: [u8; 32],
}
impl PrefixSummary {
    fn empty() -> Self {
        Self {
            declared_wire_bytes: None,
            observed_bytes: 0,
            sha256: Sha256::digest([]).into(),
        }
    }
}
#[derive(Debug)]
pub(crate) struct ControlFailure {
    pub class: ControlClass,
    pub stage: ControlStage,
    pub frame: PrefixSummary,
    pub response: PrefixSummary,
    pub io_cause: Option<io::Error>,
}
impl std::fmt::Display for ControlFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never format client input, a nonce, SQL text or an unbounded error chain.
        write!(
            f,
            "MySQL fixture control failure: class={:?} stage={:?} request={:?} response={:?}",
            self.class, self.stage, self.frame, self.response
        )?;
        if let Some(cause) = &self.io_cause {
            write!(
                f,
                " io_kind={:?} raw_os={:?}",
                cause.kind(),
                cause.raw_os_error()
            )?;
            if let Some(panic) = cause
                .get_ref()
                .and_then(|source| source.downcast_ref::<ControlPanicDigest>())
            {
                write!(f, " panic_digest=({panic})")?;
            }
        }
        Ok(())
    }
}
impl std::error::Error for ControlFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.io_cause.as_ref().map(|cause| cause as _)
    }
}
#[derive(Debug)]
pub(crate) struct ControlExitError {
    pub primary: ControlFailure,
    pub cleanup: Option<ControlFailure>,
}
impl std::fmt::Display for ControlExitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.primary)?;
        if let Some(cleanup) = &self.cleanup {
            write!(f, "; cleanup: {cleanup}")?
        }
        Ok(())
    }
}
impl std::error::Error for ControlExitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.primary)
    }
}
impl ControlFailure {
    fn new(
        class: ControlClass,
        stage: ControlStage,
        frame: PrefixSummary,
        io_cause: Option<io::Error>,
    ) -> Self {
        Self {
            class,
            stage,
            frame,
            response: PrefixSummary::empty(),
            io_cause,
        }
    }
    fn fixed_reason(&self) -> GateFailure {
        match self.class {
            ControlClass::Deadline => GateFailure::Deadline,
            ControlClass::Length => GateFailure::Length,
            ControlClass::Identity | ControlClass::Peer => GateFailure::Identity,
            _ => GateFailure::Transition,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct ControlFacts {
    pub accepted_peers: u8,
    pub commands: u8,
    pub request_wire_bytes: u64,
    pub response_wire_bytes: u64,
    pub explicit_stop: bool,
    pub last_request: PrefixSummary,
    pub last_response: PrefixSummary,
}
/// A completed protocol is a cleanup result, never an assertion of scene PASS.
pub(crate) struct ControlFinished {
    pub facts: ControlFacts,
    pub hub: MysqlWriteHubSnapshot,
}

#[derive(Clone, Copy)]
struct FileIdentity {
    dev: u64,
    ino: u64,
    uid: u32,
}
impl FileIdentity {
    fn of(meta: &std::fs::Metadata) -> Self {
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
            uid: meta.uid(),
        }
    }
    fn matches(self, meta: &std::fs::Metadata) -> bool {
        self.dev == meta.dev() && self.ino == meta.ino() && self.uid == meta.uid()
    }
}
struct OwnedSocketPath {
    path: PathBuf,
    parent: FileIdentity,
    socket: Option<FileIdentity>,
}
impl OwnedSocketPath {
    fn remove_exact(&mut self) -> io::Result<()> {
        let parent =
            std::fs::symlink_metadata(self.path.parent().expect("validated absolute parent"))?;
        if !self.parent.matches(&parent)
            || !parent.is_dir()
            || parent.permissions().mode() & 0o7777 != 0o700
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "private fixture directory identity changed",
            ));
        }
        let meta = match std::fs::symlink_metadata(&self.path) {
            Ok(meta) => meta,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        let Some(identity) = self.socket else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "created fixture socket identity is unobserved; locator retained",
            ));
        };
        if !identity.matches(&meta) || !meta.file_type().is_socket() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "fixture socket path was replaced; retained",
            ));
        }
        std::fs::remove_file(&self.path)
    }
}
/// The same service owner keeps this object outside the polled control future.
/// Dropping its run future does not lose the peer/listener or the cleanup locator.
pub(crate) struct UnixMysqlWriteControl {
    controller: MysqlWriteGateController,
    wire_version: WireVersion,
    listener: Option<UnixListener>,
    peer: Option<UnixStream>,
    path: OwnedSocketPath,
    frontend: FrontendProcessId,
    nonce: [u8; 16],
    deadline: Instant,
    uid: u32,
    used_run: bool,
    completed_control: bool,
    facts: ControlFacts,
    request: [u8; BODY_CAP],
    reply: [u8; FRAME_WIRE_CAP],
    stage: ControlStage,
    frame_digest: Sha256,
    reply_digest: Sha256,
}
impl UnixMysqlWriteControl {
    /// Startup composition supplies already validated FE identity/nonce and its one original clock.
    /// Environment is not read here, during statements, or from any process-global registry.
    pub(crate) fn bind(
        path: PathBuf,
        actual_frontend: FrontendProcessId,
        nonce: [u8; 16],
        original_absolute_deadline: Instant,
    ) -> Result<(Self, Arc<MysqlWriteGateHub>), ControlExitError> {
        Self::bind_version(
            path,
            actual_frontend,
            nonce,
            original_absolute_deadline,
            WireVersion::V1,
        )
    }
    /// Explicit private native fixture revision; never inferred from incoming frames.
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub(crate) fn bind_v2(
        path: PathBuf,
        actual_frontend: FrontendProcessId,
        nonce: [u8; 16],
        original_absolute_deadline: Instant,
    ) -> Result<(Self, Arc<MysqlWriteGateHub>), ControlExitError> {
        Self::bind_version(
            path,
            actual_frontend,
            nonce,
            original_absolute_deadline,
            WireVersion::V2,
        )
    }
    fn bind_version(
        path: PathBuf,
        actual_frontend: FrontendProcessId,
        nonce: [u8; 16],
        original_absolute_deadline: Instant,
        wire_version: WireVersion,
    ) -> Result<(Self, Arc<MysqlWriteGateHub>), ControlExitError> {
        let empty = PrefixSummary::empty();
        let startup = |class, cause| ControlExitError {
            primary: ControlFailure::new(class, ControlStage::Startup, empty, cause),
            cleanup: None,
        };
        if !path.is_absolute()
            || path.as_os_str().as_bytes().len() > SOCKET_PATH_CAP
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
            || path.as_os_str().as_bytes().contains(&0)
        {
            return Err(startup(ControlClass::Path, None));
        }
        let Some(parent) = path.parent() else {
            return Err(startup(ControlClass::Path, None));
        };
        let Some(name) = path.file_name() else {
            return Err(startup(ControlClass::Path, None));
        };
        if name.as_bytes().is_empty()
            || name.as_bytes().len() > 32
            || !name
                .as_bytes()
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || matches!(*b, b'-' | b'_' | b'.'))
        {
            return Err(startup(ControlClass::Path, None));
        }
        let meta = std::fs::symlink_metadata(parent)
            .map_err(|cause| startup(ControlClass::Path, Some(cause)))?;
        // SAFETY: geteuid has no arguments and no memory preconditions.
        let uid = unsafe { libc::geteuid() };
        if !meta.is_dir()
            || meta.file_type().is_symlink()
            || meta.uid() != uid
            || meta.permissions().mode() & 0o7777 != 0o700
        {
            return Err(startup(ControlClass::Path, None));
        }
        match std::fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(startup(
                    ControlClass::Path,
                    Some(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "fixture socket path already exists",
                    )),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(cause) => return Err(startup(ControlClass::Path, Some(cause))),
        }
        let (hub, mut controller) =
            MysqlWriteGateHub::new(actual_frontend, nonce, original_absolute_deadline)
                .map_err(|cause| startup(ControlClass::Hub, Some(cause)))?;
        let listener = UnixListener::bind(&path).map_err(|cause| {
            controller.fail(GateFailure::Transition);
            startup(ControlClass::Io, Some(cause))
        })?;
        let mut owned = OwnedSocketPath {
            path,
            parent: FileIdentity::of(&meta),
            socket: None,
        };
        let setup = (|| -> io::Result<()> {
            let meta = std::fs::symlink_metadata(&owned.path)?;
            if !meta.file_type().is_socket() || meta.uid() != uid {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "new fixture socket has unexpected identity",
                ));
            }
            owned.socket = Some(FileIdentity::of(&meta));
            std::fs::set_permissions(&owned.path, std::fs::Permissions::from_mode(0o600))?;
            let after = std::fs::symlink_metadata(&owned.path)?;
            if !owned.socket.expect("identity set").matches(&after)
                || after.permissions().mode() & 0o7777 != 0o600
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "fixture socket permissions or identity differ",
                ));
            }
            Ok(())
        })();
        if let Err(cause) = setup {
            controller.fail(GateFailure::Transition);
            drop(listener);
            let cleanup = owned.remove_exact().err().map(|cause| {
                ControlFailure::new(
                    ControlClass::Cleanup,
                    ControlStage::Cleanup,
                    empty,
                    Some(cause),
                )
            });
            return Err(ControlExitError {
                primary: ControlFailure::new(
                    ControlClass::Path,
                    ControlStage::Startup,
                    empty,
                    Some(cause),
                ),
                cleanup,
            });
        }
        Ok((
            Self {
                controller,
                wire_version,
                listener: Some(listener),
                peer: None,
                path: owned,
                frontend: actual_frontend,
                nonce,
                deadline: original_absolute_deadline,
                uid,
                used_run: false,
                completed_control: false,
                facts: ControlFacts {
                    accepted_peers: 0,
                    commands: 0,
                    request_wire_bytes: 0,
                    response_wire_bytes: 0,
                    explicit_stop: false,
                    last_request: empty,
                    last_response: empty,
                },
                request: [0; BODY_CAP],
                reply: [0; FRAME_WIRE_CAP],
                stage: ControlStage::Accept,
                frame_digest: Sha256::new(),
                reply_digest: Sha256::new(),
            },
            hub,
        ))
    }
    fn failure(&self, class: ControlClass, cause: Option<io::Error>) -> ControlFailure {
        let mut failure = ControlFailure::new(class, self.stage, self.facts.last_request, cause);
        failure.response = self.facts.last_response;
        failure
    }
    fn observe_request(&mut self, bytes: &[u8]) -> Result<(), ControlFailure> {
        self.facts.request_wire_bytes = self
            .facts
            .request_wire_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| self.failure(ControlClass::Length, None))?;
        self.facts.last_request.observed_bytes += bytes.len() as u64;
        self.frame_digest.update(bytes);
        self.facts.last_request.sha256 = self.frame_digest.clone().finalize().into();
        Ok(())
    }
    async fn read_frame(&mut self) -> Result<usize, ControlFailure> {
        self.stage = ControlStage::ReadHeader;
        self.facts.last_request = PrefixSummary::empty();
        self.frame_digest = Sha256::new();
        let mut header = [0; HEADER_BYTES];
        let mut read = 0;
        while read < header.len() {
            let n = self
                .peer
                .as_mut()
                .expect("accepted peer")
                .read(&mut header[read..])
                .await
                .map_err(|cause| self.failure(ControlClass::Io, Some(cause)))?;
            if n == 0 {
                return Err(self.failure(ControlClass::Eof, None));
            }
            self.observe_request(&header[read..read + n])?;
            read += n;
        }
        let length = u32::from_le_bytes(header) as usize;
        self.facts.last_request.declared_wire_bytes = Some(length as u64 + HEADER_BYTES as u64);
        if !(2..=BODY_CAP).contains(&length) {
            return Err(self.failure(ControlClass::Length, None));
        }
        self.stage = ControlStage::ReadBody;
        let mut read = 0;
        while read < length {
            let n = self
                .peer
                .as_mut()
                .expect("accepted peer")
                .read(&mut self.request[read..length])
                .await
                .map_err(|cause| self.failure(ControlClass::Io, Some(cause)))?;
            if n == 0 {
                return Err(self.failure(ControlClass::Eof, None));
            }
            // Hash directly from the fixed request buffer; no second body scratch exists.
            self.facts.request_wire_bytes = self
                .facts
                .request_wire_bytes
                .checked_add(n as u64)
                .ok_or_else(|| self.failure(ControlClass::Length, None))?;
            self.facts.last_request.observed_bytes += n as u64;
            self.frame_digest.update(&self.request[read..read + n]);
            self.facts.last_request.sha256 = self.frame_digest.clone().finalize().into();
            read += n;
        }
        Ok(length)
    }
    async fn drive(&mut self) -> Result<ControlFinished, ControlFailure> {
        self.stage = ControlStage::Accept;
        if self.used_run
            || self.listener.is_none()
            || self.controller.snapshot().frontend != self.frontend
        {
            return Err(self.failure(ControlClass::State, None));
        }
        self.used_run = true;
        let (peer, _) = self
            .listener
            .as_ref()
            .expect("startup listener")
            .accept()
            .await
            .map_err(|cause| self.failure(ControlClass::Io, Some(cause)))?;
        self.facts.accepted_peers = 1;
        self.peer = Some(peer);
        // No second accept or accepted child task exists. Drop the actual listener now.
        drop(self.listener.take());
        let cred = self
            .peer
            .as_ref()
            .expect("accepted peer")
            .peer_cred()
            .map_err(|cause| self.failure(ControlClass::Peer, Some(cause)))?;
        if cred.uid() != self.uid {
            return Err(self.failure(ControlClass::Peer, None));
        }
        loop {
            if self.facts.commands == COMMAND_CAP {
                return Err(self.failure(ControlClass::CommandLimit, None));
            }
            let length = self.read_frame().await?;
            self.facts.commands += 1;
            self.stage = ControlStage::Decode;
            let decoded = decode(&self.request[..length], self.wire_version);
            self.request.fill(0);
            let command = decoded.map_err(|class| self.failure(class, None))?;
            if command.frontend != self.frontend || command.nonce != self.nonce {
                return Err(self.failure(ControlClass::Identity, None));
            }
            self.stage = ControlStage::Apply;
            let opcode = match command.operation {
                Operation::Arm {
                    connection_id,
                    sql_sha256,
                    cut,
                } => {
                    self.controller
                        .arm(
                            command.frontend,
                            command.nonce,
                            connection_id,
                            sql_sha256,
                            cut,
                        )
                        .map_err(|cause| self.failure(ControlClass::Hub, Some(cause)))?;
                    ARM
                }
                Operation::Snapshot => SNAPSHOT,
                Operation::Stop => {
                    self.controller.stop();
                    self.facts.explicit_stop = true;
                    STOP
                }
            };
            let hub = self.controller.snapshot();
            // A scope-originating error is whole failure, never an OK snapshot followed by PASS.
            if hub.failure.is_some() || hub.gate.is_some_and(|gate| gate.failure.is_some()) {
                return Err(self.failure(ControlClass::Hub, None));
            }
            self.stage = ControlStage::WriteReply;
            let reply_length =
                encode_reply(&mut self.reply, opcode, self.facts, hub, self.wire_version)
                    .map_err(|class| self.failure(class, None))?;
            self.facts.last_response = PrefixSummary {
                declared_wire_bytes: Some(reply_length as u64),
                ..PrefixSummary::empty()
            };
            self.reply_digest = Sha256::new();
            let mut written = 0;
            while written < reply_length {
                let n = self
                    .peer
                    .as_mut()
                    .expect("accepted peer")
                    .write(&self.reply[written..reply_length])
                    .await
                    .map_err(|cause| self.failure(ControlClass::Io, Some(cause)))?;
                if n == 0 {
                    return Err(self.failure(
                        ControlClass::Io,
                        Some(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "fixture control reply wrote zero",
                        )),
                    ));
                }
                self.facts.response_wire_bytes += n as u64;
                self.facts.last_response.observed_bytes += n as u64;
                self.reply_digest.update(&self.reply[written..written + n]);
                self.facts.last_response.sha256 = self.reply_digest.clone().finalize().into();
                written += n;
            }
            self.reply.fill(0);
            if opcode == STOP {
                return Ok(ControlFinished {
                    facts: self.facts,
                    hub,
                });
            }
        }
    }
    /// Run only while the same supervisor concurrently polls its original MySQL future.
    /// No command, accept, read or write creates a fresh clock.
    pub(crate) async fn run(&mut self) -> Result<ControlFinished, ControlExitError> {
        let absolute = self.deadline;
        let outcome = tokio::time::timeout_at(
            tokio::time::Instant::from_std(absolute),
            catch_control_panic(self.drive()),
        )
        .await;
        let primary = match outcome {
            Ok(Ok(value)) => {
                self.completed_control = true;
                Ok(value)
            }
            Ok(Err(mut error)) => {
                if matches!(error.class, ControlClass::Panic) {
                    error.stage = self.stage;
                    error.frame = self.facts.last_request;
                    error.response = self.facts.last_response;
                }
                self.controller.fail(error.fixed_reason());
                Err(error)
            }
            Err(_) => {
                let error = self.failure(ControlClass::Deadline, None);
                self.controller.fail(GateFailure::Deadline);
                Err(error)
            }
        };
        self.request.fill(0);
        self.reply.fill(0);
        let cleanup = self.close().err();
        match (primary, cleanup) {
            (Ok(value), None) => Ok(value),
            (Ok(_), Some(primary)) => {
                self.controller.fail(primary.fixed_reason());
                Err(ControlExitError {
                    primary,
                    cleanup: None,
                })
            }
            (Err(primary), cleanup) => Err(ControlExitError { primary, cleanup }),
        }
    }
    /// Call after dropping the borrow of run on every supervisor-selected sibling exit.
    /// This closes Unix IO only; it does not await/settle the original MySQL owners.
    pub(crate) fn close(&mut self) -> Result<(), ControlFailure> {
        // A sibling exit that dropped an unfinished run is not a completed control phase.
        if self.used_run && !self.completed_control {
            self.controller.fail(GateFailure::Transition);
        }
        self.controller.stop();
        drop(self.peer.take());
        drop(self.listener.take());
        self.request.fill(0);
        self.reply.fill(0);
        self.stage = ControlStage::Cleanup;
        self.path
            .remove_exact()
            .map_err(|cause| self.failure(ControlClass::Cleanup, Some(cause)))
    }
    pub(crate) fn facts(&self) -> ControlFacts {
        self.facts
    }
    pub(crate) fn hub_snapshot(&self) -> MysqlWriteHubSnapshot {
        self.controller.snapshot()
    }
    pub(crate) fn fail(&mut self, first_reason: GateFailure) {
        self.controller.fail(first_reason)
    }
    pub(crate) fn stop(&mut self) {
        self.controller.stop()
    }
    /// Only after the original MySQL listener/session/watcher future actually joined.
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub(crate) fn finish_after_protocol_join(&mut self) -> io::Result<MysqlWriteHubSnapshot> {
        self.controller.finish_after_protocol_join()
    }
}
impl Drop for UnixMysqlWriteControl {
    fn drop(&mut self) {
        // Fallback physical close is not a cleanup receipt or an async join.
        self.controller.stop();
        drop(self.peer.take());
        drop(self.listener.take());
        self.request.fill(0);
        self.reply.fill(0);
        self.nonce.fill(0);
    }
}

#[derive(Debug)]
struct ControlPanicDigest {
    string_payload: bool,
    bytes: usize,
    sha256: [u8; 32],
}
impl std::fmt::Display for ControlPanicDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "fixture control panic: class_string={} bytes={} sha256={:?}",
            self.string_payload, self.bytes, self.sha256
        )
    }
}
impl std::error::Error for ControlPanicDigest {}
async fn catch_control_panic<F: Future<Output = Result<ControlFinished, ControlFailure>>>(
    work: F,
) -> Result<ControlFinished, ControlFailure> {
    tokio::pin!(work);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(polled) => polled,
            Err(payload) => {
                let string = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied());
                let text = string.unwrap_or("non-string panic payload");
                Poll::Ready(Err(ControlFailure::new(
                    ControlClass::Panic,
                    ControlStage::Apply,
                    PrefixSummary::empty(),
                    Some(io::Error::other(ControlPanicDigest {
                        string_payload: string.is_some(),
                        bytes: text.len(),
                        sha256: Sha256::digest(text.as_bytes()).into(),
                    })),
                )))
            }
        }
    })
    .await
}
#[derive(Clone, Copy)]
struct Command {
    frontend: FrontendProcessId,
    nonce: [u8; 16],
    operation: Operation,
}
#[derive(Clone, Copy)]
enum Operation {
    Arm {
        connection_id: u32,
        sql_sha256: [u8; 32],
        cut: u64,
    },
    Snapshot,
    Stop,
}
fn decode(body: &[u8], wire_version: WireVersion) -> Result<Command, ControlClass> {
    if body.len() < 2 || body[0] != wire_version as u8 {
        return Err(ControlClass::Fields);
    }
    let opcode = body[1];
    if !matches!(opcode, ARM | SNAPSHOT | STOP) {
        return Err(ControlClass::Fields);
    }
    // Exactly five known tags, stored in fixed typed values with a duplicate bitset.
    let mut seen = 0u8;
    let mut frontend = None;
    let mut nonce = None;
    let mut connection_id = None;
    let mut sql_sha256 = None;
    let mut cut = None;
    let mut offset = 2;
    while offset < body.len() {
        if body.len() - offset < 3 {
            return Err(ControlClass::Fields);
        }
        let tag = body[offset];
        if !(1..=5).contains(&tag) {
            return Err(ControlClass::Fields);
        }
        let mask = 1u8 << (tag - 1);
        if seen & mask != 0 {
            return Err(ControlClass::Fields);
        }
        seen |= mask;
        let length = u16::from_le_bytes([body[offset + 1], body[offset + 2]]) as usize;
        offset += 3;
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= body.len())
            .ok_or(ControlClass::Fields)?;
        let value = &body[offset..end];
        match tag {
            1 => {
                frontend = Some(
                    FrontendProcessId::try_from_bytes(
                        value.try_into().map_err(|_| ControlClass::Fields)?,
                    )
                    .map_err(|_| ControlClass::Identity)?,
                );
            }
            2 => {
                nonce = Some(value.try_into().map_err(|_| ControlClass::Fields)?);
            }
            3 => {
                connection_id = Some(u32::from_le_bytes(
                    value.try_into().map_err(|_| ControlClass::Fields)?,
                ));
            }
            4 => {
                sql_sha256 = Some(value.try_into().map_err(|_| ControlClass::Fields)?);
            }
            5 => {
                cut = Some(u64::from_le_bytes(
                    value.try_into().map_err(|_| ControlClass::Fields)?,
                ));
            }
            _ => unreachable!("range checked"),
        }
        offset = end;
    }
    let required = if opcode == ARM { 0b11111 } else { 0b00011 };
    if seen != required {
        return Err(ControlClass::Fields);
    }
    Ok(Command {
        frontend: frontend.ok_or(ControlClass::Fields)?,
        nonce: nonce.ok_or(ControlClass::Fields)?,
        operation: match opcode {
            ARM => Operation::Arm {
                connection_id: connection_id.ok_or(ControlClass::Fields)?,
                sql_sha256: sql_sha256.ok_or(ControlClass::Fields)?,
                cut: cut.ok_or(ControlClass::Fields)?,
            },
            SNAPSHOT => Operation::Snapshot,
            STOP => Operation::Stop,
            _ => unreachable!("opcode checked"),
        },
    })
}

struct ReplyWriter<'a> {
    bytes: &'a mut [u8; FRAME_WIRE_CAP],
    length: usize,
}
impl ReplyWriter<'_> {
    fn put(&mut self, bytes: &[u8]) -> Result<(), ControlClass> {
        let end = self
            .length
            .checked_add(bytes.len())
            .filter(|end| *end <= FRAME_WIRE_CAP)
            .ok_or(ControlClass::Length)?;
        self.bytes[self.length..end].copy_from_slice(bytes);
        self.length = end;
        Ok(())
    }
    fn flag(&mut self, value: bool) -> Result<(), ControlClass> {
        self.put(&[u8::from(value)])
    }
    fn u64(&mut self, value: u64) -> Result<(), ControlClass> {
        self.put(&value.to_le_bytes())
    }
    fn failure(&mut self, value: Option<GateFailure>) -> Result<(), ControlClass> {
        self.put(&[match value {
            None => 0,
            Some(GateFailure::Transition) => 1,
            Some(GateFailure::Identity) => 2,
            Some(GateFailure::Deadline) => 3,
            Some(GateFailure::Length) => 4,
            Some(GateFailure::Receipt) => 5,
            Some(GateFailure::Counter) => 6,
        }])
    }
    fn receipt(&mut self, cursor: Option<FramingCursor>) -> Result<(), ControlClass> {
        self.flag(cursor.is_some())?;
        if let Some(cursor) = cursor {
            self.put(&[
                match cursor.phase {
                    WritePhase::Boundary => 0,
                    WritePhase::Row => 1,
                    WritePhase::Metadata => 2,
                    WritePhase::Terminal => 3,
                    WritePhase::Ended => 4,
                    WritePhase::Poisoned => 5,
                },
                cursor.sequence,
            ])?;
            for value in [
                cursor.logical_total,
                cursor.logical_written,
                cursor.packet_payload_length,
                cursor.packet_payload_written,
            ] {
                self.put(&value.to_le_bytes())?
            }
            self.put(&cursor.header)?;
            self.put(&[cursor.header_written])?;
            self.flag(cursor.zero_terminal_pending)?;
            self.u64(cursor.committed_wire_bytes)?;
            self.u64(cursor.rows_completed)?;
        }
        Ok(())
    }
    fn gate(&mut self, gate: Option<MysqlWriteGateSnapshot>) -> Result<(), ControlClass> {
        self.flag(gate.is_some())?;
        if let Some(gate) = gate {
            self.put(&gate.connection.connection_id().to_le_bytes())?;
            self.u64(gate.connection.generation())?;
            self.flag(gate.statement.is_some())?;
            if let Some(statement) = gate.statement {
                self.put(&gate.connection.connection_id().to_le_bytes())?;
                self.u64(statement.session().key().get())?;
                self.u64(statement.generation())?;
            }
            self.flag(gate.sql_sha256.is_some())?;
            if let Some(hash) = gate.sql_sha256 {
                self.put(&hash)?
            }
            self.put(&[match gate.phase {
                GatePhase::Fresh => 0,
                GatePhase::Armed => 1,
                GatePhase::Rows => 2,
                GatePhase::CancelRecorded => 3,
                GatePhase::Resumed => 4,
                GatePhase::Stopped => 5,
            }])?;
            self.failure(gate.failure)?;
            self.u64(gate.cut_bytes)?;
            self.u64(gate.accepted_prefix_bytes)?;
            self.put(&gate.accepted_prefix_sha256)?;
            self.u64(gate.scalar_inner_polls)?;
            self.u64(gate.vectored_inner_polls)?;
            self.u64(gate.successful_inner_writes)?;
            self.flag(gate.blocked_after_acceptance)?;
            self.receipt(gate.baseline)?;
            self.receipt(gate.cancel_receipt)?;
            self.flag(gate.writer_attached)?;
            self.flag(gate.writer_exited)?;
        }
        Ok(())
    }
}
fn encode_reply(
    bytes: &mut [u8; FRAME_WIRE_CAP],
    opcode: u8,
    facts: ControlFacts,
    hub: MysqlWriteHubSnapshot,
    wire_version: WireVersion,
) -> Result<usize, ControlClass> {
    let mut writer = ReplyWriter {
        bytes,
        length: HEADER_BYTES,
    };
    writer.put(&[wire_version as u8, opcode, 0])?;
    writer.put(&hub.frontend.to_bytes())?;
    writer.put(&[facts.accepted_peers, facts.commands])?;
    writer.u64(facts.request_wire_bytes)?;
    // This snapshot precedes the current response's actual writes; final facts retain those writes.
    writer.u64(facts.response_wire_bytes)?;
    writer.flag(facts.explicit_stop)?;
    writer.flag(hub.used_arm)?;
    writer.flag(hub.stopped)?;
    writer.failure(hub.failure)?;
    writer.flag(hub.original_writer_exited)?;
    writer.gate(hub.gate)?;
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    if matches!(wire_version, WireVersion::V2) {
        writer.original_freeze_scalars(hub.original_freeze)?;
        if writer.length > 744 {
            return Err(ControlClass::Length);
        }
    }
    let length = writer.length;
    writer.bytes[..HEADER_BYTES].copy_from_slice(&((length - HEADER_BYTES) as u32).to_le_bytes());
    Ok(length)
}

#[cfg(test)]
#[path = "unix_control_tests.rs"]
mod tests;

#[cfg(feature = "mem-1-m07-exact-mysql-write")]
#[path = "original_freeze_encoder.rs"]
mod original_freeze_encoder;

#[cfg(all(test, feature = "mem-1-m07-exact-mysql-write"))]
#[path = "unix_control_v2_tests.rs"]
mod v2_tests;
