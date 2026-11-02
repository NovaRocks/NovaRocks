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

//! Independent, finite pressure control protocol. No listener/session task is spawned.
//! One original absolute clock covers accept, commands, barriers and response writes.

use crate::closing_pressure_gate::{
    ArmInput, Failure, Phase, PressureController, SlotSnapshot, TARGETS,
};
use novarocks_types::FrontendProcessId;
use opensrv_mysql::{FramingCursor, WritePhase};
use sha2::{Digest, Sha256};
use std::{
    any::Any,
    fmt,
    future::Future,
    io,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
    },
    path::{Component, PathBuf},
    sync::Mutex,
    task::Poll,
    time::Instant,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};

const FRAME_CAP: usize = 4096;
const BODY_CAP: usize = FRAME_CAP - 4;
const MAGIC: [u8; 8] = *b"NRCL65\0\x01";
const REQUEST_HEADER: usize = 43;
const COMMAND_CAP: u16 = 512;
const ARM: u8 = 1;
const SNAPSHOT: u8 = 2;
const WAIT_ROWS: u8 = 3;
const JOINT: u8 = 4;
const RELEASE: u8 = 5;
const EXIT_ALL: u8 = 6;
const STOP: u8 = 7;

#[derive(Clone, Copy, Debug)]
enum Class {
    Path,
    Peer,
    Io,
    Eof,
    Deadline,
    Length,
    Identity,
    CommandLimit,
    State,
    Panic,
    Cleanup,
}
#[derive(Clone, Copy, Debug)]
enum Stage {
    Startup,
    Accept,
    Header,
    Body,
    Decode,
    Apply,
    Reply,
    Cleanup,
}
#[derive(Clone, Copy, Debug)]
struct Prefix {
    declared: Option<u64>,
    observed: u64,
    sha256: [u8; 32],
}
impl Default for Prefix {
    fn default() -> Self {
        Self {
            declared: None,
            observed: 0,
            sha256: Sha256::digest([]).into(),
        }
    }
}
/// Retains the original panic object without invoking its formatter or destructuring it.
struct OriginalPanic {
    _payload: Mutex<Box<dyn Any + Send>>,
}
impl fmt::Debug for OriginalPanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OriginalPressureControlPanic { payload_retained: true }")
    }
}
impl fmt::Display for OriginalPanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for OriginalPanic {}

pub(crate) struct ControlError {
    class: Class,
    stage: Stage,
    request: Prefix,
    response: Prefix,
    cause: Option<io::Error>,
}
impl fmt::Debug for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Closing pressure control failed; class={:?} stage={:?} request={:?} response={:?} io_kind={:?} raw_os={:?}",
            self.class,
            self.stage,
            self.request,
            self.response,
            self.cause.as_ref().map(io::Error::kind),
            self.cause.as_ref().and_then(io::Error::raw_os_error)
        )
    }
}
impl std::error::Error for ControlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_ref().map(|error| error as _)
    }
}
impl ControlError {
    fn startup(class: Class, cause: Option<io::Error>) -> Self {
        Self {
            class,
            stage: Stage::Startup,
            request: Prefix::default(),
            response: Prefix::default(),
            cause,
        }
    }
    fn reason(&self) -> Failure {
        match self.class {
            Class::Deadline => Failure::Deadline,
            Class::Length | Class::CommandLimit => Failure::Length,
            Class::Identity | Class::Peer => Failure::Identity,
            Class::Panic => Failure::Panic,
            Class::Io => Failure::InnerIo,
            _ => Failure::Transition,
        }
    }
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
struct SocketPath {
    path: PathBuf,
    parent: FileIdentity,
    socket: Option<FileIdentity>,
}
impl SocketPath {
    fn remove_exact(&self) -> io::Result<()> {
        let parent = std::fs::symlink_metadata(self.path.parent().expect("validated parent"))?;
        if !self.parent.matches(&parent)
            || !parent.is_dir()
            || parent.permissions().mode() & 0o7777 != 0o700
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pressure private parent identity changed",
            ));
        }
        let socket = std::fs::symlink_metadata(&self.path)?;
        if !self
            .socket
            .is_some_and(|identity| identity.matches(&socket))
            || !socket.file_type().is_socket()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pressure socket path replaced or unobserved; retained",
            ));
        }
        std::fs::remove_file(&self.path)
    }
}
pub(crate) struct UnixPressureControl {
    listener: Option<UnixListener>,
    peer: Option<UnixStream>,
    path: SocketPath,
    frontend: FrontendProcessId,
    nonce: [u8; 16],
    deadline: Instant,
    uid: u32,
    used: bool,
    completed: bool,
    closed: bool,
    commands: u16,
    request_bytes: u64,
    response_bytes: u64,
    stage: Stage,
    request_prefix: Prefix,
    response_prefix: Prefix,
    request: [u8; BODY_CAP],
    reply: [u8; FRAME_CAP],
}
impl UnixPressureControl {
    pub(crate) fn bind(
        path: PathBuf,
        frontend: FrontendProcessId,
        nonce: [u8; 16],
        deadline: Instant,
    ) -> Result<Self, (ControlError, Option<ControlError>)> {
        let startup = |class, cause| (ControlError::startup(class, cause), None);
        if Instant::now() >= deadline {
            return Err(startup(Class::Deadline, None));
        }
        if nonce == [0; 16] {
            return Err(startup(Class::Identity, None));
        }
        if !path.is_absolute()
            || path.as_os_str().as_bytes().len() > 90
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
            || path.as_os_str().as_bytes().contains(&0)
        {
            return Err(startup(Class::Path, None));
        }
        let Some(name) = path.file_name() else {
            return Err(startup(Class::Path, None));
        };
        if name.as_bytes().is_empty()
            || name.as_bytes().len() > 32
            || !name
                .as_bytes()
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || matches!(*b, b'-' | b'_' | b'.'))
        {
            return Err(startup(Class::Path, None));
        }
        let Some(parent) = path.parent() else {
            return Err(startup(Class::Path, None));
        };
        let meta =
            std::fs::symlink_metadata(parent).map_err(|error| startup(Class::Path, Some(error)))?;
        // SAFETY: geteuid takes no pointers and has no memory preconditions.
        let uid = unsafe { libc::geteuid() };
        if !meta.is_dir()
            || meta.file_type().is_symlink()
            || meta.uid() != uid
            || meta.permissions().mode() & 0o7777 != 0o700
        {
            return Err(startup(Class::Path, None));
        }
        match std::fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(startup(
                    Class::Path,
                    Some(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "pressure socket path already exists",
                    )),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(startup(Class::Path, Some(error))),
        }
        let listener =
            UnixListener::bind(&path).map_err(|error| startup(Class::Io, Some(error)))?;
        let mut owned = SocketPath {
            path,
            parent: FileIdentity::of(&meta),
            socket: None,
        };
        let setup = (|| -> io::Result<()> {
            let socket = std::fs::symlink_metadata(&owned.path)?;
            if !socket.file_type().is_socket() || socket.uid() != uid {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "new pressure socket identity mismatch",
                ));
            }
            owned.socket = Some(FileIdentity::of(&socket));
            std::fs::set_permissions(&owned.path, std::fs::Permissions::from_mode(0o600))?;
            let after = std::fs::symlink_metadata(&owned.path)?;
            if !owned.socket.expect("observed socket").matches(&after)
                || after.permissions().mode() & 0o7777 != 0o600
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "new pressure socket permissions changed",
                ));
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "pressure startup exceeded original clock",
                ));
            }
            Ok(())
        })();
        if let Err(error) = setup {
            drop(listener);
            let cleanup = owned
                .remove_exact()
                .err()
                .map(|error| ControlError::startup(Class::Cleanup, Some(error)));
            return Err((ControlError::startup(Class::Path, Some(error)), cleanup));
        }
        Ok(Self {
            listener: Some(listener),
            peer: None,
            path: owned,
            frontend,
            nonce,
            deadline,
            uid,
            used: false,
            completed: false,
            closed: false,
            commands: 0,
            request_bytes: 0,
            response_bytes: 0,
            stage: Stage::Accept,
            request_prefix: Prefix::default(),
            response_prefix: Prefix::default(),
            request: [0; BODY_CAP],
            reply: [0; FRAME_CAP],
        })
    }
    fn error(&self, class: Class, cause: Option<io::Error>) -> ControlError {
        ControlError {
            class,
            stage: self.stage,
            request: self.request_prefix,
            response: self.response_prefix,
            cause,
        }
    }
    async fn read_frame(&mut self) -> Result<usize, ControlError> {
        self.stage = Stage::Header;
        self.request_prefix = Prefix::default();
        self.response_prefix = Prefix::default();
        let mut header = [0; 4];
        let mut digest = Sha256::new();
        let mut read = 0;
        while read < 4 {
            let n = self
                .peer
                .as_mut()
                .expect("accepted peer")
                .read(&mut header[read..])
                .await
                .map_err(|error| self.error(Class::Io, Some(error)))?;
            if n == 0 {
                return Err(self.error(Class::Eof, None));
            }
            digest.update(&header[read..read + n]);
            self.request_prefix.observed += n as u64;
            self.request_bytes += n as u64;
            self.request_prefix.sha256 = digest.clone().finalize().into();
            read += n;
        }
        let length = u32::from_le_bytes(header) as usize;
        self.request_prefix.declared = Some(length as u64 + 4);
        if !(REQUEST_HEADER..=BODY_CAP).contains(&length) {
            return Err(self.error(Class::Length, None));
        }
        self.stage = Stage::Body;
        read = 0;
        while read < length {
            let n = self
                .peer
                .as_mut()
                .expect("accepted peer")
                .read(&mut self.request[read..length])
                .await
                .map_err(|error| self.error(Class::Io, Some(error)))?;
            if n == 0 {
                return Err(self.error(Class::Eof, None));
            }
            digest.update(&self.request[read..read + n]);
            self.request_prefix.observed += n as u64;
            self.request_bytes += n as u64;
            self.request_prefix.sha256 = digest.clone().finalize().into();
            read += n;
        }
        Ok(length)
    }
    async fn drive(&mut self, controller: &mut PressureController) -> Result<(), ControlError> {
        if self.used || self.closed || self.listener.is_none() {
            return Err(self.error(Class::State, None));
        }
        self.used = true;
        self.stage = Stage::Accept;
        let (peer, _) = self
            .listener
            .as_ref()
            .expect("bound listener")
            .accept()
            .await
            .map_err(|error| self.error(Class::Io, Some(error)))?;
        self.peer = Some(peer);
        drop(self.listener.take());
        let cred = self
            .peer
            .as_ref()
            .expect("accepted peer")
            .peer_cred()
            .map_err(|error| self.error(Class::Peer, Some(error)))?;
        if cred.uid() != self.uid {
            return Err(self.error(Class::Peer, None));
        }
        loop {
            if self.commands == COMMAND_CAP {
                return Err(self.error(Class::CommandLimit, None));
            }
            let length = self.read_frame().await?;
            self.stage = Stage::Decode;
            let sequence = self.commands + 1;
            let operation = decode(
                &self.request[..length],
                self.frontend,
                &self.nonce,
                sequence,
            )
            .map_err(|class| self.error(class, None))?;
            self.commands = sequence;
            self.stage = Stage::Apply;
            let mut snapshot = None;
            let mut joint = None;
            let applied = match operation {
                Operation::Arm(targets) => controller.arm_targets(self.frontend, targets),
                Operation::Snapshot(slot) => controller
                    .snapshot(slot)
                    .map(|value| snapshot = Some(value)),
                Operation::Rows(first, count) => controller.wait_rows(first, count).await,
                Operation::Joint(refusal) => controller
                    .wait_joint_closing(refusal)
                    .await
                    .map(|value| joint = Some(value)),
                Operation::Release(slot) => controller.release(slot),
                Operation::Exit => controller.wait_original_writers_exited().await,
                Operation::Stop => controller.stop_after_original_writers_exited(),
            };
            applied.map_err(|error| self.error(Class::State, Some(error)))?;
            // No input owner or nonce echo is present in a reply. All facts come from the sole controller.
            let opcode = operation.opcode();
            let mut out = Encoder {
                bytes: &mut self.reply,
                length: 4,
            };
            out.put(&MAGIC);
            out.put(&[opcode, 0]);
            out.put(&self.frontend.to_bytes());
            out.put(&sequence.to_le_bytes());
            out.put(&[1]);
            out.put(&self.request_bytes.to_le_bytes());
            out.put(&self.response_bytes.to_le_bytes());
            if let Some(facts) = snapshot {
                out.snapshot(facts);
            }
            if let Some(facts) = joint {
                for value in facts.capacity.held_positions {
                    out.put(&(value as u64).to_le_bytes());
                }
                out.put(&(facts.closing_targets as u64).to_le_bytes());
                out.put(&facts.minimum_original_closing_polls.to_le_bytes());
                out.flag(facts.original_capacity_refusal);
            }
            let reply_length = out.length;
            self.reply[..4].copy_from_slice(&((reply_length - 4) as u32).to_le_bytes());
            self.request.fill(0);
            self.stage = Stage::Reply;
            self.response_prefix.declared = Some(reply_length as u64);
            let mut digest = Sha256::new();
            let mut written = 0;
            while written < reply_length {
                let n = self
                    .peer
                    .as_mut()
                    .expect("accepted peer")
                    .write(&self.reply[written..reply_length])
                    .await
                    .map_err(|error| self.error(Class::Io, Some(error)))?;
                if n == 0 {
                    return Err(self.error(
                        Class::Io,
                        Some(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "pressure control reply wrote zero",
                        )),
                    ));
                }
                digest.update(&self.reply[written..written + n]);
                self.response_prefix.observed += n as u64;
                self.response_bytes += n as u64;
                self.response_prefix.sha256 = digest.clone().finalize().into();
                written += n;
            }
            self.reply.fill(0);
            if opcode == STOP {
                return Ok(());
            }
        }
    }
    pub(crate) async fn run(
        &mut self,
        controller: &mut PressureController,
    ) -> Result<(), ControlError> {
        let deadline = self.deadline;
        let outcome = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            catch_panic(self.drive(controller)),
        )
        .await;
        let result = match outcome {
            Ok(Ok(result)) => result,
            Ok(Err(payload)) => Err(self.error(
                Class::Panic,
                Some(io::Error::other(OriginalPanic {
                    _payload: Mutex::new(payload),
                })),
            )),
            Err(_) => Err(self.error(Class::Deadline, None)),
        };
        let result = result.and_then(|()| {
            if Instant::now() >= deadline {
                Err(self.error(Class::Deadline, None))
            } else {
                Ok(())
            }
        });
        self.request.fill(0);
        self.reply.fill(0);
        match result {
            Ok(()) => {
                self.completed = true;
                Ok(())
            }
            Err(error) => {
                controller.fail(error.reason());
                controller.stop();
                Err(error)
            }
        }
    }
    /// Physical control IO cleanup only; no original MySQL join is implied.
    pub(crate) fn close(
        &mut self,
        controller: &mut PressureController,
    ) -> Result<(), ControlError> {
        if self.closed {
            return Ok(());
        }
        if !self.completed {
            controller.fail(Failure::Transition);
        }
        controller.stop();
        drop(self.peer.take());
        drop(self.listener.take());
        self.request.fill(0);
        self.reply.fill(0);
        self.nonce.fill(0);
        self.stage = Stage::Cleanup;
        self.path
            .remove_exact()
            .map_err(|error| self.error(Class::Cleanup, Some(error)))?;
        self.closed = true;
        Ok(())
    }
    pub(crate) fn completed_and_closed(&self) -> bool {
        self.completed && self.closed
    }
}
impl Drop for UnixPressureControl {
    fn drop(&mut self) {
        // Fallback Drop closes physical IO. It is neither exact path cleanup nor a join receipt.
        drop(self.peer.take());
        drop(self.listener.take());
        self.request.fill(0);
        self.reply.fill(0);
        self.nonce.fill(0);
    }
}
async fn catch_panic<F: Future>(future: F) -> Result<F::Output, Box<dyn Any + Send>> {
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await
}
#[derive(Clone, Copy)]
enum Operation {
    Arm([ArmInput; TARGETS]),
    Snapshot(usize),
    Rows(usize, usize),
    Joint(bool),
    Release(usize),
    Exit,
    Stop,
}
impl Operation {
    fn opcode(self) -> u8 {
        match self {
            Self::Arm(_) => ARM,
            Self::Snapshot(_) => SNAPSHOT,
            Self::Rows(..) => WAIT_ROWS,
            Self::Joint(_) => JOINT,
            Self::Release(_) => RELEASE,
            Self::Exit => EXIT_ALL,
            Self::Stop => STOP,
        }
    }
}
fn decode(
    body: &[u8],
    frontend: FrontendProcessId,
    nonce: &[u8; 16],
    sequence: u16,
) -> Result<Operation, Class> {
    if body.len() < REQUEST_HEADER {
        return Err(Class::Length);
    }
    if body[..8] != MAGIC
        || body[9..25] != *nonce
        || body[25..41] != frontend.to_bytes()
        || body[41..43] != sequence.to_le_bytes()
    {
        return Err(Class::Identity);
    }
    let payload = &body[REQUEST_HEADER..];
    match body[8] {
        ARM if payload.len() == TARGETS * 36 => {
            let targets = std::array::from_fn(|slot| {
                let first = slot * 36;
                ArmInput {
                    handshake_connection_id: u32::from_le_bytes(
                        payload[first..first + 4]
                            .try_into()
                            .expect("checked fixed ARM"),
                    ),
                    original_sql_sha256: payload[first + 4..first + 36]
                        .try_into()
                        .expect("checked fixed ARM"),
                }
            });
            Ok(Operation::Arm(targets))
        }
        SNAPSHOT if payload.len() == 1 && usize::from(payload[0]) < TARGETS => {
            Ok(Operation::Snapshot(usize::from(payload[0])))
        }
        WAIT_ROWS
            if payload.len() == 2
                && payload[1] > 0
                && usize::from(payload[0]) + usize::from(payload[1]) <= TARGETS =>
        {
            Ok(Operation::Rows(
                usize::from(payload[0]),
                usize::from(payload[1]),
            ))
        }
        JOINT if payload.len() == 1 && payload[0] <= 1 => Ok(Operation::Joint(payload[0] == 1)),
        RELEASE if payload.len() == 1 && payload[0] < 64 => {
            Ok(Operation::Release(usize::from(payload[0])))
        }
        EXIT_ALL if payload.is_empty() => Ok(Operation::Exit),
        STOP if payload.is_empty() => Ok(Operation::Stop),
        _ => Err(Class::Length),
    }
}
/// Maximum snapshot reply is below 512 bytes; all fields have fixed scalar size.
struct Encoder<'a> {
    bytes: &'a mut [u8; FRAME_CAP],
    length: usize,
}
impl Encoder<'_> {
    fn put(&mut self, bytes: &[u8]) {
        let end = self.length + bytes.len();
        self.bytes[self.length..end].copy_from_slice(bytes);
        self.length = end;
    }
    fn flag(&mut self, value: bool) {
        self.put(&[u8::from(value)]);
    }
    fn u64(&mut self, value: u64) {
        self.put(&value.to_le_bytes());
    }
    fn receipt(&mut self, cursor: Option<FramingCursor>) {
        self.flag(cursor.is_some());
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
            ]);
            for value in [
                cursor.logical_total,
                cursor.logical_written,
                cursor.packet_payload_length,
                cursor.packet_payload_written,
            ] {
                self.put(&value.to_le_bytes());
            }
            self.put(&cursor.header);
            self.put(&[cursor.header_written]);
            self.flag(cursor.zero_terminal_pending);
            self.u64(cursor.committed_wire_bytes);
            self.u64(cursor.rows_completed);
        }
    }
    fn snapshot(&mut self, facts: SlotSnapshot) {
        self.put(&[
            facts.slot as u8,
            match facts.phase {
                Phase::Unarmed => 0,
                Phase::Armed => 1,
                Phase::Bound => 2,
                Phase::Rows => 3,
                Phase::CancelObserved => 4,
                Phase::ClosingHeld => 5,
                Phase::Released => 6,
                Phase::CapacityRefused => 7,
            },
            facts.failure.map_or(0, |reason| reason as u8),
        ]);
        self.flag(facts.handshake_connection_id.is_some());
        self.put(&facts.handshake_connection_id.unwrap_or(0).to_le_bytes());
        self.flag(facts.connection.is_some());
        self.put(
            &facts
                .connection
                .map_or(0, |token| token.connection_id())
                .to_le_bytes(),
        );
        self.u64(facts.connection.map_or(0, |token| token.generation()));
        self.flag(facts.statement.is_some());
        self.put(
            &facts
                .statement
                .map_or(0, |_| {
                    facts
                        .connection
                        .map_or(0, |connection| connection.connection_id())
                })
                .to_le_bytes(),
        );
        self.u64(
            facts
                .statement
                .map_or(0, |token| token.session().key().get()),
        );
        self.u64(facts.statement.map_or(0, |token| token.generation()));
        self.flag(facts.original_sql_sha256.is_some());
        self.put(&facts.original_sql_sha256.unwrap_or([0; 32]));
        for value in [facts.cut_bytes, facts.accepted_prefix_bytes] {
            self.u64(value);
        }
        self.put(&facts.accepted_prefix_sha256);
        for value in [
            facts.rows_blocked,
            facts.closing_write_blocked,
            facts.closing_flush_blocked,
        ] {
            self.flag(value);
        }
        self.u64(facts.paired_closing_polls);
        self.flag(facts.real_closing_observed);
        self.receipt(facts.baseline);
        self.receipt(facts.cancel_receipt);
        for value in [
            facts.scalar_inner_polls,
            facts.vectored_inner_polls,
            facts.flush_inner_polls,
            facts.successful_inner_writes,
        ] {
            self.u64(value);
        }
        for value in [
            facts.writer_attached,
            facts.writer_destructor_returned,
            facts.physical_shutdown_completed,
            facts.original_capacity_source_retained,
        ] {
            self.flag(value);
        }
    }
}

#[cfg(test)]
#[path = "closing_pressure_control_tests.rs"]
mod tests;
