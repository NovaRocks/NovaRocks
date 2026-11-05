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

//! Exclusive, synchronous observation fences on the original collector pipe.
//! This module carries no query/result body, RPC or resource-exit authority.

use anyhow::Result;
use std::fmt;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::time::Instant;

const LINE_BYTES: usize = 384;
const LOG_BYTES: u64 = 2_097_152;
const ROLES: [&str; 4] = ["fe", "be-0", "be-1", "be-2"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Prepared,
    Baseline,
    Target,
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailureClass {
    Pipe,
    Order,
    Clock,
    EarlyAck,
    ExtraAck,
    Ack,
    Closed,
    Io,
}

struct Failure {
    class: FailureClass,
    source: Option<io::Error>,
}

/// Clone only the fixed failure owner, never the original io::Error source.
#[derive(Clone)]
struct FenceError(Arc<Failure>);
impl fmt::Debug for FenceError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("SourceFenceError")
            .field("class", &self.0.class)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for FenceError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(output, "original source fence failed ({:?})", self.0.class)
    }
}
impl std::error::Error for FenceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0
            .source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

fn failure(class: FailureClass, source: Option<io::Error>) -> FenceError {
    FenceError(Arc::new(Failure { class, source }))
}

/// The explicit CLI branch constructs exactly one owner after real admission.
///
/// The original calling graph must have no other stdin readers or raw dup
/// consumers. FIFO/identity/CLOEXEC checks do not prove that OS-wide property.
/// No StdinLock is retained: the owner remains Send for the scenario's Mutex.
pub(crate) struct SourceFenceOwner {
    input: OwnedFd,
    phase: Phase,
    log_owners: usize,
    baseline_sizes: Option<[u64; 3]>,
    first_failure: Option<FenceError>,
}
impl fmt::Debug for SourceFenceOwner {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("SourceFenceOwner")
            .field("phase", &self.phase)
            .field("log_owners", &self.log_owners)
            .field("failed", &self.first_failure.is_some())
            .finish_non_exhaustive()
    }
}

impl SourceFenceOwner {
    pub(crate) fn from_original_stdin() -> Result<Self> {
        Self::duplicate_original(libc::STDIN_FILENO).map_err(anyhow::Error::new)
    }

    fn duplicate_original(original: RawFd) -> std::result::Result<Self, FenceError> {
        // Read the actual original descriptor before duplicating; never reopen a path.
        let before = fifo_identity(original)?;
        // SAFETY: fcntl duplicates a currently live caller-owned descriptor;
        // the successful result becomes the sole OwnedFd below exactly once.
        let duplicate = unsafe { libc::fcntl(original, libc::F_DUPFD_CLOEXEC, 3) };
        if duplicate < 0 {
            return Err(failure(FailureClass::Io, Some(io::Error::last_os_error())));
        }
        // SAFETY: successful F_DUPFD_CLOEXEC returned a fresh owned descriptor.
        let input = unsafe { OwnedFd::from_raw_fd(duplicate) };
        let copied = fifo_identity(input.as_raw_fd())?;
        let after = fifo_identity(original)?;
        if before != copied || before != after {
            return Err(failure(FailureClass::Pipe, None));
        }
        // SAFETY: F_GETFD only observes this owner's live original duplicate.
        let flags = unsafe { libc::fcntl(input.as_raw_fd(), libc::F_GETFD) };
        if flags < 0 {
            return Err(failure(FailureClass::Io, Some(io::Error::last_os_error())));
        }
        if flags & libc::FD_CLOEXEC == 0 {
            return Err(failure(FailureClass::Pipe, None));
        }
        Ok(Self {
            input,
            phase: Phase::Prepared,
            log_owners: 0,
            baseline_sizes: None,
            first_failure: None,
        })
    }

    pub(crate) fn prepared(&mut self, deadline: Instant) -> Result<()> {
        self.prepared_with(&mut io::stdout().lock(), deadline)
    }
    pub(crate) fn log_owner(
        &mut self,
        role: &str,
        dev: u64,
        ino: u64,
        deadline: Instant,
    ) -> Result<()> {
        self.log_owner_with(&mut io::stdout().lock(), role, dev, ino, deadline)
    }
    pub(crate) fn baseline(&mut self, sizes: [u64; 3], deadline: Instant) -> Result<()> {
        self.baseline_with(&mut io::stdout().lock(), sizes, deadline)
    }
    pub(crate) fn target(&mut self, sizes: [u64; 3], deadline: Instant) -> Result<()> {
        self.target_with(&mut io::stdout().lock(), sizes, deadline)
    }

    fn sticky<T>(&mut self, error: FenceError) -> Result<T> {
        if self.first_failure.is_none() {
            self.first_failure = Some(error);
        }
        Err(anyhow::Error::new(
            self.first_failure
                .as_ref()
                .expect("failure is retained")
                .clone(),
        ))
    }
    fn existing_failure(&self) -> Result<()> {
        match &self.first_failure {
            Some(error) => Err(anyhow::Error::new(error.clone())),
            None => Ok(()),
        }
    }
    fn clock(&mut self, deadline: Instant) -> Result<()> {
        self.existing_failure()?;
        if Instant::now() >= deadline {
            return self.sticky(failure(FailureClass::Clock, None));
        }
        Ok(())
    }
    fn phase(&mut self, phase: Phase, deadline: Instant) -> Result<()> {
        self.clock(deadline)?;
        if self.phase != phase {
            return self.sticky(failure(FailureClass::Order, None));
        }
        self.no_pending_ack(deadline)
    }
    fn no_pending_ack(&mut self, deadline: Instant) -> Result<()> {
        loop {
            self.clock(deadline)?;
            match readiness(self.input.as_raw_fd(), 0) {
                Ok(0) => return self.clock(deadline),
                Ok(events) if events & libc::POLLNVAL != 0 => {
                    return self.sticky(failure(
                        FailureClass::Io,
                        Some(io::Error::from_raw_os_error(libc::EBADF)),
                    ));
                }
                Ok(events) if events & libc::POLLIN != 0 => {
                    return self.sticky(failure(FailureClass::EarlyAck, None));
                }
                Ok(_) => {
                    return self.sticky(failure(
                        FailureClass::Closed,
                        Some(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "original collector pipe closed",
                        )),
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    // EINTR cannot extend the absolute caller deadline or grow stack.
                    continue;
                }
                Err(error) => return self.sticky(failure(FailureClass::Io, Some(error))),
            }
        }
    }
    fn write_line(
        &mut self,
        output: &mut impl Write,
        line: &[u8],
        deadline: Instant,
    ) -> Result<()> {
        self.clock(deadline)?;
        if line.is_empty() || line.len() > LINE_BYTES || line.last() != Some(&b'\n') {
            return self.sticky(failure(FailureClass::Order, None));
        }
        if let Err(error) = output.write_all(line).and_then(|()| output.flush()) {
            return self.sticky(failure(FailureClass::Io, Some(error)));
        }
        // Synchronous stdout has no physical preemption claim. A late return
        // is failure even if the entire line and an ACK have already arrived.
        self.clock(deadline)
    }
    fn wait_ack(&mut self, deadline: Instant) -> Result<()> {
        loop {
            self.clock(deadline)?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            let millis = remaining
                .as_nanos()
                .saturating_add(999_999)
                .checked_div(1_000_000)
                .unwrap_or(0)
                .min(i32::MAX as u128) as i32;
            let events = match readiness(self.input.as_raw_fd(), millis) {
                Ok(events) => events,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return self.sticky(failure(FailureClass::Io, Some(error))),
            };
            self.clock(deadline)?;
            if events == 0 {
                continue;
            }
            if events & libc::POLLNVAL != 0 {
                return self.sticky(failure(
                    FailureClass::Io,
                    Some(io::Error::from_raw_os_error(libc::EBADF)),
                ));
            }
            // There are no other consumers of this pipe in the admitted
            // original caller graph; read does not wait for a second byte.
            let mut bytes = [0_u8; 2];
            // SAFETY: this owner's fd is live and bytes is a valid two-byte buffer.
            let count = unsafe {
                libc::read(
                    self.input.as_raw_fd(),
                    bytes.as_mut_ptr().cast(),
                    bytes.len(),
                )
            };
            if count < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return self.sticky(failure(FailureClass::Io, Some(error)));
            }
            self.clock(deadline)?;
            if count == 0 {
                return self.sticky(failure(
                    FailureClass::Closed,
                    Some(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "original collector ACK reached EOF",
                    )),
                ));
            }
            if count != 1 {
                return self.sticky(failure(FailureClass::ExtraAck, None));
            }
            if bytes[0] != b'A' {
                return self.sticky(failure(FailureClass::Ack, None));
            }
            // Refuse already resident extra bytes without starting another wait.
            // Future bytes cannot be ruled out by a finite snapshot; the next
            // phase/log event preflight will reject any early subsequent ACK.
            self.no_pending_ack(deadline)?;
            return self.clock(deadline);
        }
    }
    fn prepared_with(&mut self, output: &mut impl Write, deadline: Instant) -> Result<()> {
        self.phase(Phase::Prepared, deadline)?;
        self.write_line(
            output,
            &const {
                let marker =
                    novarocks_failpoint::observation_marker::MEM_1_M07_HELD_LIVE_FENCE.as_bytes();
                let suffix = b" phase=prepared sequence=1\n";
                let mut bytes = [0;
                    novarocks_failpoint::observation_marker::MEM_1_M07_HELD_LIVE_FENCE.len()
                        + b" phase=prepared sequence=1\n".len()];
                let mut index = 0;
                while index < marker.len() {
                    bytes[index] = marker[index];
                    index += 1;
                }
                let mut suffix_index = 0;
                while suffix_index < suffix.len() {
                    bytes[index + suffix_index] = suffix[suffix_index];
                    suffix_index += 1;
                }
                bytes
            },
            deadline,
        )?;
        self.wait_ack(deadline)?;
        self.phase = Phase::Baseline;
        Ok(())
    }
    fn log_owner_with(
        &mut self,
        output: &mut impl Write,
        role: &str,
        dev: u64,
        ino: u64,
        deadline: Instant,
    ) -> Result<()> {
        self.phase(Phase::Baseline, deadline)?;
        if self.log_owners >= ROLES.len() || role != ROLES[self.log_owners] || ino == 0 {
            return self.sticky(failure(FailureClass::Order, None));
        }
        let mut line = [0; LINE_BYTES];
        let length = {
            let mut cursor = io::Cursor::new(&mut line[..]);
            if let Err(error) = writeln!(
                cursor,
                "{} role={role} dev={dev} ino={ino}",
                novarocks_failpoint::observation_marker::MEM_1_M07_HELD_LIVE_LOG_OWNER
            ) {
                return self.sticky(failure(FailureClass::Io, Some(error)));
            }
            cursor.position() as usize
        };
        self.write_line(output, &line[..length], deadline)?;
        // Log owner events consume no ACK positions and permit no queued ACK.
        self.no_pending_ack(deadline)?;
        self.log_owners += 1;
        Ok(())
    }
    fn sizes_line(
        &mut self,
        output: &mut impl Write,
        phase: &'static str,
        sequence: u8,
        sizes: [u64; 3],
        deadline: Instant,
    ) -> Result<()> {
        if sizes.iter().any(|size| *size > LOG_BYTES) {
            return self.sticky(failure(FailureClass::Order, None));
        }
        let mut line = [0; LINE_BYTES];
        let length = {
            let mut cursor = io::Cursor::new(&mut line[..]);
            if let Err(error) = writeln!(
                cursor,
                "{} phase={phase} sequence={sequence} sizes={},{},{}",
                novarocks_failpoint::observation_marker::MEM_1_M07_HELD_LIVE_FENCE,
                sizes[0],
                sizes[1],
                sizes[2]
            ) {
                return self.sticky(failure(FailureClass::Io, Some(error)));
            }
            cursor.position() as usize
        };
        self.write_line(output, &line[..length], deadline)?;
        self.wait_ack(deadline)
    }
    fn baseline_with(
        &mut self,
        output: &mut impl Write,
        sizes: [u64; 3],
        deadline: Instant,
    ) -> Result<()> {
        self.phase(Phase::Baseline, deadline)?;
        if self.log_owners != ROLES.len() {
            return self.sticky(failure(FailureClass::Order, None));
        }
        self.sizes_line(output, "baseline", 2, sizes, deadline)?;
        self.baseline_sizes = Some(sizes);
        self.phase = Phase::Target;
        Ok(())
    }
    fn target_with(
        &mut self,
        output: &mut impl Write,
        sizes: [u64; 3],
        deadline: Instant,
    ) -> Result<()> {
        self.phase(Phase::Target, deadline)?;
        let Some(baseline) = self.baseline_sizes else {
            return self.sticky(failure(FailureClass::Order, None));
        };
        if sizes
            .iter()
            .zip(baseline)
            .any(|(after, before)| *after < before)
        {
            return self.sticky(failure(FailureClass::Order, None));
        }
        self.sizes_line(output, "target", 3, sizes, deadline)?;
        self.phase = Phase::Complete;
        Ok(())
    }
}

fn fifo_identity(fd: RawFd) -> std::result::Result<(u64, u64), FenceError> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat writes the correctly sized output, read only on success.
    if unsafe { libc::fstat(fd, metadata.as_mut_ptr()) } < 0 {
        return Err(failure(FailureClass::Io, Some(io::Error::last_os_error())));
    }
    // SAFETY: successful fstat initialized the complete output struct.
    let metadata = unsafe { metadata.assume_init() };
    if metadata.st_mode & libc::S_IFMT != libc::S_IFIFO {
        return Err(failure(FailureClass::Pipe, None));
    }
    // SAFETY: F_GETFL only inspects the caller's live original descriptor.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(failure(FailureClass::Io, Some(io::Error::last_os_error())));
    }
    if flags & libc::O_ACCMODE == libc::O_WRONLY {
        return Err(failure(FailureClass::Pipe, None));
    }
    // Unix inode and device widths vary by target; the wire identity is always u64.
    #[allow(clippy::unnecessary_cast)]
    let identity = (metadata.st_dev as u64, metadata.st_ino as u64);
    Ok(identity)
}

fn readiness(fd: RawFd, timeout_ms: i32) -> io::Result<i16> {
    let mut descriptor = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one initialized pollfd lives for the entire synchronous poll call.
    let ready = unsafe { libc::poll(&mut descriptor, 1, timeout_ms) };
    if ready < 0 {
        Err(io::Error::last_os_error())
    } else if ready == 0 {
        Ok(0)
    } else {
        Ok(descriptor.revents)
    }
}

#[cfg(test)]
#[path = "source_fence_owner_tests.rs"]
mod tests;
