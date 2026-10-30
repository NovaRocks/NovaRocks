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

//! Private runner frame-v1 codec. Successful exchange is not a native/join receipt.
//! DTOs intentionally have no dependency on adapter, controller, gate or writer types.
//! The caller supplies the original scene's absolute 20-second deadline, once.

use novarocks_types::FrontendProcessId;
use sha2::{Digest, Sha256};
use std::fmt;
use std::io;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

pub const FRAME_WIRE_CAP: usize = 4096;
const HEADER_BYTES: usize = 4;
const BODY_CAP: usize = FRAME_WIRE_CAP - HEADER_BYTES;
pub const COMMAND_CAP: u8 = 16;
const VERSION: u8 = 1;
const MAX_CUT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Arm = 1,
    Snapshot = 2,
    Stop = 3,
}

// No Debug: the nonce and exact SQL hash must not enter generic input logging.
#[derive(Clone, Copy)]
pub enum Command {
    Arm {
        connection_id: u32,
        exact_sql_sha256: [u8; 32],
        cut_bytes: u64,
    },
    Snapshot,
    Stop,
}
impl Command {
    pub fn opcode(self) -> Opcode {
        match self {
            Self::Arm { .. } => Opcode::Arm,
            Self::Snapshot => Opcode::Snapshot,
            Self::Stop => Opcode::Stop,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    Length,
    Truncated,
    Version,
    Opcode,
    Status,
    Identity,
    Boolean,
    Enum,
    Trailing,
    Fields,
}

fn body_length(header: [u8; 4]) -> Result<usize, DecodeError> {
    let n = u32::from_le_bytes(header) as usize;
    if !(2..=BODY_CAP).contains(&n) {
        return Err(DecodeError::Length);
    }
    Ok(n)
}

/// Canonical ascending-tag requests: Arm has exactly five TLVs, others exactly two.
/// The caller passes SHA256(exact original SQL bytes), never a copied SQL string.
pub fn encode_request(
    out: &mut [u8; FRAME_WIRE_CAP],
    actual_frontend: FrontendProcessId,
    nonce: &[u8; 16],
    command: Command,
) -> Result<usize, DecodeError> {
    out.fill(0);
    if *nonce == [0; 16] {
        return Err(DecodeError::Identity);
    }
    if let Command::Arm {
        connection_id,
        cut_bytes,
        ..
    } = command
        && (connection_id == 0 || cut_bytes == 0 || cut_bytes > MAX_CUT_BYTES)
    {
        return Err(DecodeError::Fields);
    }
    let mut at = HEADER_BYTES;
    out[at..at + 2].copy_from_slice(&[VERSION, command.opcode() as u8]);
    at += 2;
    fn tlv(out: &mut [u8; FRAME_WIRE_CAP], at: &mut usize, tag: u8, value: &[u8]) {
        // Only fixed values of 4, 8, 16 or 32 bytes reach this private helper.
        out[*at] = tag;
        out[*at + 1..*at + 3].copy_from_slice(&(value.len() as u16).to_le_bytes());
        out[*at + 3..*at + 3 + value.len()].copy_from_slice(value);
        *at += 3 + value.len();
    }
    tlv(out, &mut at, 1, &actual_frontend.to_bytes());
    tlv(out, &mut at, 2, nonce);
    if let Command::Arm {
        connection_id,
        exact_sql_sha256,
        cut_bytes,
    } = command
    {
        tlv(out, &mut at, 3, &connection_id.to_le_bytes());
        tlv(out, &mut at, 4, &exact_sql_sha256);
        tlv(out, &mut at, 5, &cut_bytes.to_le_bytes());
    }
    out[..4].copy_from_slice(&((at - HEADER_BYTES) as u32).to_le_bytes());
    Ok(at)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureCode {
    Transition,
    Identity,
    Deadline,
    Length,
    Receipt,
    Counter,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatePhase {
    Fresh,
    Armed,
    Rows,
    CancelRecorded,
    Resumed,
    Stopped,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorPhase {
    Boundary,
    Row,
    Metadata,
    Terminal,
    Ended,
    Poisoned,
}

/// Fixed wire facts, not minted application tokens or owner capabilities.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionFacts {
    pub connection_id: u32,
    pub generation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatementFacts {
    pub connection_id: u32,
    pub session_key: u64,
    pub generation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorFacts {
    pub phase: CursorPhase,
    pub sequence: u8,
    pub logical_total: u32,
    pub logical_written: u32,
    pub packet_payload_length: u32,
    pub packet_payload_written: u32,
    pub header: [u8; 4],
    pub header_written: u8,
    pub zero_terminal_pending: bool,
    pub committed_wire_bytes: u64,
    pub rows_completed: u64,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct GateFacts {
    pub connection: ConnectionFacts,
    pub statement: Option<StatementFacts>,
    pub exact_sql_sha256: Option<[u8; 32]>,
    pub phase: GatePhase,
    pub failure: Option<FailureCode>,
    pub cut_bytes: u64,
    pub accepted_prefix_bytes: u64,
    pub accepted_prefix_sha256: [u8; 32],
    pub scalar_inner_polls: u64,
    pub vectored_inner_polls: u64,
    pub successful_inner_writes: u64,
    pub blocked_after_acceptance: bool,
    pub baseline: Option<CursorFacts>,
    pub cancel_receipt: Option<CursorFacts>,
    pub writer_attached: bool,
    pub writer_exited: bool,
}
impl fmt::Debug for GateFacts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GateFacts")
            .field("connection", &self.connection)
            .field("statement", &self.statement)
            .field("phase", &self.phase)
            .field("failure", &self.failure)
            .field("cut_bytes", &self.cut_bytes)
            .field("accepted_prefix_bytes", &self.accepted_prefix_bytes)
            .field("accepted_prefix_sha256", &self.accepted_prefix_sha256)
            .field("scalar_inner_polls", &self.scalar_inner_polls)
            .field("vectored_inner_polls", &self.vectored_inner_polls)
            .field("successful_inner_writes", &self.successful_inner_writes)
            .field("blocked_after_acceptance", &self.blocked_after_acceptance)
            .field("baseline", &self.baseline)
            .field("cancel_receipt", &self.cancel_receipt)
            .field("writer_attached", &self.writer_attached)
            .field("writer_exited", &self.writer_exited)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplyFacts {
    pub opcode: Opcode,
    pub frontend: FrontendProcessId,
    pub accepted_peers: u8,
    pub commands: u8,
    pub request_wire_bytes: u64,
    /// Before this reply's actual writes; do not add the current frame to the DTO.
    pub response_wire_bytes_before_current_reply: u64,
    pub explicit_stop: bool,
    pub used_arm: bool,
    pub stopped: bool,
    pub failure: Option<FailureCode>,
    pub original_writer_exited: bool,
    pub gate: Option<GateFacts>,
}
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl Reader<'_> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let end = self
            .at
            .checked_add(N)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(DecodeError::Truncated)?;
        let value = self.bytes[self.at..end]
            .try_into()
            .map_err(|_| DecodeError::Truncated)?;
        self.at = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, DecodeError> {
        Ok(self.array::<1>()?[0])
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    fn boolean(&mut self) -> Result<bool, DecodeError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(DecodeError::Boolean),
        }
    }
    fn option<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T, DecodeError>,
    ) -> Result<Option<T>, DecodeError> {
        if self.boolean()? {
            Ok(Some(read(self)?))
        } else {
            Ok(None)
        }
    }
    fn failure(&mut self) -> Result<Option<FailureCode>, DecodeError> {
        Ok(match self.byte()? {
            0 => None,
            1 => Some(FailureCode::Transition),
            2 => Some(FailureCode::Identity),
            3 => Some(FailureCode::Deadline),
            4 => Some(FailureCode::Length),
            5 => Some(FailureCode::Receipt),
            6 => Some(FailureCode::Counter),
            _ => return Err(DecodeError::Enum),
        })
    }
    fn cursor(&mut self) -> Result<CursorFacts, DecodeError> {
        let phase = match self.byte()? {
            0 => CursorPhase::Boundary,
            1 => CursorPhase::Row,
            2 => CursorPhase::Metadata,
            3 => CursorPhase::Terminal,
            4 => CursorPhase::Ended,
            5 => CursorPhase::Poisoned,
            _ => return Err(DecodeError::Enum),
        };
        Ok(CursorFacts {
            phase,
            sequence: self.byte()?,
            logical_total: self.u32()?,
            logical_written: self.u32()?,
            packet_payload_length: self.u32()?,
            packet_payload_written: self.u32()?,
            header: self.array()?,
            header_written: self.byte()?,
            zero_terminal_pending: self.boolean()?,
            committed_wire_bytes: self.u64()?,
            rows_completed: self.u64()?,
        })
    }
    fn gate(&mut self) -> Result<GateFacts, DecodeError> {
        let connection = ConnectionFacts {
            connection_id: self.u32()?,
            generation: self.u64()?,
        };
        let statement = self.option(|r| {
            Ok(StatementFacts {
                connection_id: r.u32()?,
                session_key: r.u64()?,
                generation: r.u64()?,
            })
        })?;
        let exact_sql_sha256 = self.option(|r| r.array())?;
        let phase = match self.byte()? {
            0 => GatePhase::Fresh,
            1 => GatePhase::Armed,
            2 => GatePhase::Rows,
            3 => GatePhase::CancelRecorded,
            4 => GatePhase::Resumed,
            5 => GatePhase::Stopped,
            _ => return Err(DecodeError::Enum),
        };
        Ok(GateFacts {
            connection,
            statement,
            exact_sql_sha256,
            phase,
            failure: self.failure()?,
            cut_bytes: self.u64()?,
            accepted_prefix_bytes: self.u64()?,
            accepted_prefix_sha256: self.array()?,
            scalar_inner_polls: self.u64()?,
            vectored_inner_polls: self.u64()?,
            successful_inner_writes: self.u64()?,
            blocked_after_acceptance: self.boolean()?,
            baseline: self.option(Self::cursor)?,
            cancel_receipt: self.option(Self::cursor)?,
            writer_attached: self.boolean()?,
            writer_exited: self.boolean()?,
        })
    }
}

/// Consume exactly one whole frame. UUID validation and comparison use the full actual FE.
/// Cursor numeric facts are preserved verbatim; the codec invents no semantic repair/default.
pub fn decode_reply(
    frame: &[u8],
    expected: Opcode,
    actual_frontend: FrontendProcessId,
) -> Result<ReplyFacts, DecodeError> {
    if frame.len() > FRAME_WIRE_CAP {
        return Err(DecodeError::Length);
    }
    let header: [u8; 4] = frame
        .get(..4)
        .ok_or(DecodeError::Truncated)?
        .try_into()
        .map_err(|_| DecodeError::Truncated)?;
    let length = body_length(header)?;
    if frame.len() < length + HEADER_BYTES {
        return Err(DecodeError::Truncated);
    }
    if frame.len() != length + HEADER_BYTES {
        return Err(DecodeError::Trailing);
    }
    let mut r = Reader {
        bytes: &frame[4..],
        at: 0,
    };
    if r.byte()? != VERSION {
        return Err(DecodeError::Version);
    }
    let opcode = match r.byte()? {
        1 => Opcode::Arm,
        2 => Opcode::Snapshot,
        3 => Opcode::Stop,
        _ => return Err(DecodeError::Opcode),
    };
    if opcode != expected {
        return Err(DecodeError::Opcode);
    }
    if r.byte()? != 0 {
        return Err(DecodeError::Status);
    }
    let frontend =
        FrontendProcessId::try_from_bytes(r.array()?).map_err(|_| DecodeError::Identity)?;
    if frontend != actual_frontend {
        return Err(DecodeError::Identity);
    }
    let reply = ReplyFacts {
        opcode,
        frontend,
        accepted_peers: r.byte()?,
        commands: r.byte()?,
        request_wire_bytes: r.u64()?,
        response_wire_bytes_before_current_reply: r.u64()?,
        explicit_stop: r.boolean()?,
        used_arm: r.boolean()?,
        stopped: r.boolean()?,
        failure: r.failure()?,
        original_writer_exited: r.boolean()?,
        gate: r.option(|r| r.gate())?,
    };
    if r.at != r.bytes.len() {
        return Err(DecodeError::Trailing);
    }
    Ok(reply)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrefixSummary {
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientClass {
    Io,
    Eof,
    Deadline,
    Decode(DecodeError),
    CommandLimit,
    State,
    Cancelled,
    Counter,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientStage {
    Connect,
    Encode,
    WriteRequest,
    ReadHeader,
    ReadBody,
    Decode,
    Closed,
}
// No raw io::Error or input in Debug/Display; both representations are finite.
pub struct ClientFailure {
    pub class: ClientClass,
    pub stage: ClientStage,
    pub request: PrefixSummary,
    pub response: PrefixSummary,
    pub io_kind: Option<io::ErrorKind>,
    pub raw_os_error: Option<i32>,
}
impl fmt::Debug for ClientFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for ClientFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "fixture Unix client failure: class={:?} stage={:?} request={:?} response={:?} io_kind={:?} raw_os={:?}",
            self.class, self.stage, self.request, self.response, self.io_kind, self.raw_os_error
        )
    }
}
impl std::error::Error for ClientFailure {}

/// Sole actual UnixStream owner. No task, reconnect, response history or renewed clock.
/// Dropping an exchange future leaves this owner and its observed prefixes intact.
/// Call close_incomplete after dropping that borrow on any sibling-selected exit.
pub struct UnixControlClient {
    stream: Option<UnixStream>,
    frontend: FrontendProcessId,
    nonce: [u8; 16],
    absolute: Instant,
    request: [u8; FRAME_WIRE_CAP],
    response: [u8; FRAME_WIRE_CAP],
    request_digest: Sha256,
    response_digest: Sha256,
    last_request: PrefixSummary,
    last_response: PrefixSummary,
    stage: ClientStage,
    commands: u8,
    total_request_bytes: u64,
    total_response_bytes: u64,
    in_flight: bool,
    failed: bool,
    stopped: bool,
    used_arm: bool,
}
impl UnixControlClient {
    /// Move one original already-connected stream into its sole owner; no handle clone.
    pub fn from_stream(
        stream: UnixStream,
        actual_frontend: FrontendProcessId,
        nonce: [u8; 16],
        original_absolute_deadline: Instant,
    ) -> Result<Self, ClientFailure> {
        if nonce == [0; 16] || Instant::now() >= original_absolute_deadline {
            return Err(ClientFailure {
                class: if nonce == [0; 16] {
                    ClientClass::Decode(DecodeError::Identity)
                } else {
                    ClientClass::Deadline
                },
                stage: ClientStage::Connect,
                request: PrefixSummary::empty(),
                response: PrefixSummary::empty(),
                io_kind: None,
                raw_os_error: None,
            });
        }
        Ok(Self {
            stream: Some(stream),
            frontend: actual_frontend,
            nonce,
            absolute: original_absolute_deadline,
            request: [0; FRAME_WIRE_CAP],
            response: [0; FRAME_WIRE_CAP],
            request_digest: Sha256::new(),
            response_digest: Sha256::new(),
            last_request: PrefixSummary::empty(),
            last_response: PrefixSummary::empty(),
            stage: ClientStage::Connect,
            commands: 0,
            total_request_bytes: 0,
            total_response_bytes: 0,
            in_flight: false,
            failed: false,
            stopped: false,
            used_arm: false,
        })
    }
    fn failure(&self, class: ClientClass, cause: Option<io::Error>) -> ClientFailure {
        ClientFailure {
            class,
            stage: self.stage,
            request: self.last_request,
            response: self.last_response,
            io_kind: cause.as_ref().map(io::Error::kind),
            raw_os_error: cause.as_ref().and_then(io::Error::raw_os_error),
        }
    }
    fn deadline_check(&self) -> Result<(), ClientFailure> {
        if Instant::now() >= self.absolute {
            Err(self.failure(ClientClass::Deadline, None))
        } else {
            Ok(())
        }
    }
    fn close_stream(&mut self) {
        drop(self.stream.take());
        self.request.fill(0);
        self.response.fill(0);
        self.failed = true;
        self.in_flight = false;
    }
    /// Physical Unix close and finite cancellation facts, not Stop/controller/join authority.
    pub fn close_incomplete(&mut self) -> Option<ClientFailure> {
        let cause = self
            .in_flight
            .then(|| self.failure(ClientClass::Cancelled, None));
        self.close_stream();
        self.stage = ClientStage::Closed;
        cause
    }
    pub fn last_prefixes(&self) -> (PrefixSummary, PrefixSummary) {
        (self.last_request, self.last_response)
    }
    pub fn command_count(&self) -> u8 {
        self.commands
    }

    pub async fn exchange(&mut self, command: Command) -> Result<ReplyFacts, ClientFailure> {
        // A dropped prior future never permits another command on a partially used stream.
        if self.in_flight || self.failed || self.stopped || self.stream.is_none() {
            let cause = self.failure(ClientClass::State, None);
            self.close_stream();
            return Err(cause);
        }
        if self.commands >= COMMAND_CAP || (command.opcode() == Opcode::Arm && self.used_arm) {
            let cause = self.failure(
                if self.commands >= COMMAND_CAP {
                    ClientClass::CommandLimit
                } else {
                    ClientClass::State
                },
                None,
            );
            self.close_stream();
            return Err(cause);
        }
        self.stage = ClientStage::Encode;
        self.last_request = PrefixSummary::empty();
        self.last_response = PrefixSummary::empty();
        self.request_digest = Sha256::new();
        self.response_digest = Sha256::new();
        let length = match encode_request(&mut self.request, self.frontend, &self.nonce, command) {
            Ok(length) => length,
            Err(error) => {
                let cause = self.failure(ClientClass::Decode(error), None);
                self.close_stream();
                return Err(cause);
            }
        };
        self.last_request.declared_wire_bytes = Some(length as u64);
        self.commands += 1;
        self.in_flight = true;
        if command.opcode() == Opcode::Arm {
            self.used_arm = true;
        }
        let absolute = tokio::time::Instant::from_std(self.absolute);
        // drive borrows the actual stream retained in self; timeout drops only this borrow.
        let outcome = tokio::time::timeout_at(absolute, self.drive(command.opcode(), length)).await;
        self.in_flight = false;
        let result = match outcome {
            Ok(result) => result,
            Err(_) => Err(self.failure(ClientClass::Deadline, None)),
        };
        self.request.fill(0);
        self.response.fill(0);
        match result {
            Ok(reply) => {
                if command.opcode() == Opcode::Stop {
                    self.stopped = true;
                    drop(self.stream.take());
                    self.stage = ClientStage::Closed;
                }
                Ok(reply)
            }
            Err(cause) => {
                self.close_stream();
                Err(cause)
            }
        }
    }
    async fn drive(
        &mut self,
        opcode: Opcode,
        request_length: usize,
    ) -> Result<ReplyFacts, ClientFailure> {
        self.stage = ClientStage::WriteRequest;
        let mut written = 0;
        while written < request_length {
            self.deadline_check()?;
            let n = self
                .stream
                .as_mut()
                .expect("retained original Unix stream")
                .write(&self.request[written..request_length])
                .await
                .map_err(|cause| self.failure(ClientClass::Io, Some(cause)))?;
            if n == 0 {
                return Err(self.failure(
                    ClientClass::Io,
                    Some(io::Error::from(io::ErrorKind::WriteZero)),
                ));
            }
            self.request_digest
                .update(&self.request[written..written + n]);
            self.last_request.observed_bytes += n as u64;
            self.last_request.sha256 = self.request_digest.clone().finalize().into();
            self.total_request_bytes += n as u64;
            written += n;
            self.deadline_check()?;
        }
        self.stage = ClientStage::ReadHeader;
        self.read_until(HEADER_BYTES).await?;
        // read_until retained the declared full-wire length before any deadline/length error.
        // Reject oversize here, before reading a single body byte.
        let length = body_length(self.response[..4].try_into().expect("complete header"))
            .map_err(|error| self.failure(ClientClass::Decode(error), None))?;
        self.stage = ClientStage::ReadBody;
        self.read_until(length + HEADER_BYTES).await?;
        self.stage = ClientStage::Decode;
        let reply = decode_reply(
            &self.response[..length + HEADER_BYTES],
            opcode,
            self.frontend,
        )
        .map_err(|error| self.failure(ClientClass::Decode(error), None))?;
        // The server sampled response bytes before writing this reply, not after it.
        if reply.accepted_peers != 1
            || reply.commands != self.commands
            || reply.request_wire_bytes != self.total_request_bytes
            || reply.response_wire_bytes_before_current_reply
                != self.total_response_bytes - self.last_response.observed_bytes
        {
            return Err(self.failure(ClientClass::Counter, None));
        }
        self.deadline_check()?;
        Ok(reply)
    }
    async fn read_until(&mut self, end: usize) -> Result<(), ClientFailure> {
        while (self.last_response.observed_bytes as usize) < end {
            self.deadline_check()?;
            let at = self.last_response.observed_bytes as usize;
            let n = self
                .stream
                .as_mut()
                .expect("retained original Unix stream")
                .read(&mut self.response[at..end])
                .await
                .map_err(|cause| self.failure(ClientClass::Io, Some(cause)))?;
            if n == 0 {
                return Err(self.failure(ClientClass::Eof, None));
            }
            self.response_digest.update(&self.response[at..at + n]);
            self.last_response.observed_bytes += n as u64;
            self.last_response.sha256 = self.response_digest.clone().finalize().into();
            self.total_response_bytes += n as u64;
            if self.last_response.observed_bytes == HEADER_BYTES as u64 {
                // Preserve complete-header facts before timeout can return, including oversize.
                self.last_response.declared_wire_bytes = Some(
                    u32::from_le_bytes(self.response[..4].try_into().expect("complete header"))
                        as u64
                        + 4,
                );
            }
            self.deadline_check()?;
        }
        Ok(())
    }
}
impl Drop for UnixControlClient {
    fn drop(&mut self) {
        drop(self.stream.take());
        self.nonce.fill(0);
        self.request.fill(0);
        self.response.fill(0);
    }
}

#[cfg(test)]
#[path = "exact_mysql_control.tests.rs"]
mod tests;
