// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Real-pipe fence contracts; these tests do not provide Native or role-exit evidence.
use super::*;
use std::fs::File;
use std::time::Duration;

fn pipe() -> (OwnedFd, File) {
    let mut descriptors = [-1; 2];
    // SAFETY: pipe initializes two output descriptors on success.
    assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
    // SAFETY: each successful pipe descriptor is owned exactly once.
    let read = unsafe { OwnedFd::from_raw_fd(descriptors[0]) };
    // SAFETY: the distinct write descriptor is owned exactly once.
    let write = File::from(unsafe { OwnedFd::from_raw_fd(descriptors[1]) });
    (read, write)
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(1)
}
#[derive(Clone, Copy)]
enum Ack {
    One,
    Extra,
    Wrong,
    Eof,
    Silent,
}
struct PipeOutput {
    bytes: Vec<u8>,
    flushed: usize,
    sender: Option<File>,
    ack: Ack,
    ack_writes: usize,
}
impl PipeOutput {
    fn new(sender: File, ack: Ack) -> Self {
        Self {
            bytes: Vec::new(),
            flushed: 0,
            sender: Some(sender),
            ack,
            ack_writes: 0,
        }
    }
}
impl Write for PipeOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert!(self.bytes.len() + bytes.len() <= LINE_BYTES * 7);
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        let line = &self.bytes[self.flushed..];
        self.flushed = self.bytes.len();
        if line.starts_with(
            &const {
                let marker =
                    novarocks_failpoint::observation_marker::MEM_1_M07_HELD_LIVE_FENCE.as_bytes();
                let suffix = b" ";
                let mut bytes = [0;
                    novarocks_failpoint::observation_marker::MEM_1_M07_HELD_LIVE_FENCE.len()
                        + b" ".len()];
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
        ) {
            assert_eq!(line.last(), Some(&b'\n'));
            self.ack_writes += 1;
            match self.ack {
                Ack::One => self.sender.as_mut().unwrap().write_all(b"A")?,
                Ack::Extra => self.sender.as_mut().unwrap().write_all(b"AA")?,
                Ack::Wrong => self.sender.as_mut().unwrap().write_all(b"B")?,
                Ack::Eof => drop(self.sender.take()),
                Ack::Silent => {}
            }
        }
        Ok(())
    }
}
fn new_owner(ack: Ack) -> (SourceFenceOwner, PipeOutput) {
    let (reader, sender) = pipe();
    let owner = SourceFenceOwner::duplicate_original(reader.as_raw_fd()).unwrap();
    // The temporary original is no longer needed; the actual duplicate remains.
    drop(reader);
    (owner, PipeOutput::new(sender, ack))
}
fn owners(owner: &mut SourceFenceOwner, output: &mut PipeOutput) {
    for (index, role) in ROLES.into_iter().enumerate() {
        owner
            .log_owner_with(output, role, 7, index as u64 + 1, deadline())
            .unwrap();
    }
}
fn retained_class(error: &anyhow::Error) -> FailureClass {
    error.downcast_ref::<FenceError>().unwrap().0.class
}

#[test]
fn actual_fifo_duplicate_is_cloexec_and_send_for_existing_mutex() {
    fn assert_send<T: Send>() {}
    assert_send::<SourceFenceOwner>();
    let (reader, _sender) = pipe();
    let owner = SourceFenceOwner::duplicate_original(reader.as_raw_fd()).unwrap();
    assert_eq!(
        fifo_identity(reader.as_raw_fd()).unwrap(),
        fifo_identity(owner.input.as_raw_fd()).unwrap()
    );
    // SAFETY: F_GETFD observes this live test-owned descriptor.
    assert_ne!(
        unsafe { libc::fcntl(owner.input.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
        0
    );
}
#[test]
fn ambient_regular_file_and_write_only_pipe_cannot_be_ack_readers() {
    let file = tempfile::tempfile().unwrap();
    assert!(
        matches!(SourceFenceOwner::duplicate_original(file.as_raw_fd()), Err(FenceError(error)) if error.class == FailureClass::Pipe)
    );
    let (_reader, writer) = pipe();
    assert!(
        matches!(SourceFenceOwner::duplicate_original(writer.as_raw_fd()), Err(FenceError(error)) if error.class == FailureClass::Pipe)
    );
}
#[test]
fn actual_three_ack_bytes_and_four_original_owner_lines_match_collector_literals() {
    let (mut owner, mut output) = new_owner(Ack::One);
    owner.prepared_with(&mut output, deadline()).unwrap();
    owners(&mut owner, &mut output);
    owner
        .baseline_with(&mut output, [0, 7, LOG_BYTES], deadline())
        .unwrap();
    owner
        .target_with(&mut output, [12, 8, LOG_BYTES], deadline())
        .unwrap();
    assert_eq!(owner.phase, Phase::Complete);
    assert_eq!(
        output.ack_writes, 3,
        "log owners do not consume ACK positions"
    );
    assert_eq!(
        output.bytes,
        format!(
            concat!(
                "{fence} phase=prepared sequence=1\n",
                "{owner} role=fe dev=7 ino=1\n",
                "{owner} role=be-0 dev=7 ino=2\n",
                "{owner} role=be-1 dev=7 ino=3\n",
                "{owner} role=be-2 dev=7 ino=4\n",
                "{fence} phase=baseline sequence=2 sizes=0,7,2097152\n",
                "{fence} phase=target sequence=3 sizes=12,8,2097152\n",
            ),
            fence = novarocks_failpoint::observation_marker::MEM_1_M07_HELD_LIVE_FENCE,
            owner = novarocks_failpoint::observation_marker::MEM_1_M07_HELD_LIVE_LOG_OWNER,
        )
        .as_bytes()
    );
    for line in output.bytes.split_inclusive(|byte| *byte == b'\n') {
        assert!(line.len() <= LINE_BYTES);
    }
}
#[test]
fn already_resident_ack_fails_before_any_source_line_or_phase_commit() {
    let (mut owner, mut output) = new_owner(Ack::One);
    output.sender.as_mut().unwrap().write_all(b"A").unwrap();
    let error = owner.prepared_with(&mut output, deadline()).unwrap_err();
    assert_eq!(retained_class(&error), FailureClass::EarlyAck);
    assert!(output.bytes.is_empty());
    assert_eq!(owner.phase, Phase::Prepared);
    let first = Arc::clone(&owner.first_failure.as_ref().unwrap().0);
    owner.prepared_with(&mut output, deadline()).unwrap_err();
    assert!(Arc::ptr_eq(
        &first,
        &owner.first_failure.as_ref().unwrap().0
    ));
}
#[test]
fn actual_extra_two_byte_and_wrong_byte_ack_do_not_complete_phase() {
    for (ack, class) in [
        (Ack::Extra, FailureClass::ExtraAck),
        (Ack::Wrong, FailureClass::Ack),
    ] {
        let (mut owner, mut output) = new_owner(ack);
        let error = owner.prepared_with(&mut output, deadline()).unwrap_err();
        assert_eq!(retained_class(&error), class);
        assert_eq!(owner.phase, Phase::Prepared);
        assert_eq!(output.ack_writes, 1);
        assert!(owner.first_failure.is_some());
    }
}
#[test]
fn actual_pipe_eof_after_source_line_retains_io_source_and_cannot_launch_next_phase() {
    let (mut owner, mut output) = new_owner(Ack::Eof);
    let error = owner.prepared_with(&mut output, deadline()).unwrap_err();
    assert_eq!(retained_class(&error), FailureClass::Closed);
    let source = error
        .downcast_ref::<FenceError>()
        .unwrap()
        .0
        .source
        .as_ref()
        .unwrap();
    assert_eq!(source.kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(owner.phase, Phase::Prepared);
    assert_eq!(output.ack_writes, 1);
}
#[test]
fn expired_original_clock_has_no_line_and_fresh_clock_cannot_clear_failure() {
    let (mut owner, mut output) = new_owner(Ack::One);
    let error = owner
        .prepared_with(&mut output, Instant::now())
        .unwrap_err();
    assert_eq!(retained_class(&error), FailureClass::Clock);
    assert!(output.bytes.is_empty());
    assert_eq!(owner.phase, Phase::Prepared);
    assert_eq!(
        retained_class(&owner.prepared_with(&mut output, deadline()).unwrap_err()),
        FailureClass::Clock
    );
    assert!(output.bytes.is_empty());
    // A synchronous writer returning after the supplied clock cannot commit
    // Prepared, even when it has already supplied the one actual ACK byte.
    struct LateOutput {
        inner: PipeOutput,
        until: Instant,
    }
    impl Write for LateOutput {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.inner.write(bytes)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()?;
            while Instant::now() < self.until {
                std::hint::spin_loop();
            }
            Ok(())
        }
    }
    let (mut owner, output) = new_owner(Ack::One);
    let until = Instant::now() + Duration::from_millis(2);
    let mut output = LateOutput {
        inner: output,
        until,
    };
    let error = owner.prepared_with(&mut output, until).unwrap_err();
    assert_eq!(retained_class(&error), FailureClass::Clock);
    assert_eq!(owner.phase, Phase::Prepared);
    // Scheduling may expire before write; either way no late success is allowed.
    assert!(output.inner.ack_writes <= 1);
}

#[test]
fn live_pipe_without_ack_expires_on_the_original_clock_and_preserves_failure() {
    let (mut owner, mut output) = new_owner(Ack::Silent);
    let original_deadline = Instant::now() + Duration::from_millis(50);
    let error = owner
        .prepared_with(&mut output, original_deadline)
        .unwrap_err();
    assert_eq!(retained_class(&error), FailureClass::Clock);
    assert!(Instant::now() >= original_deadline);
    assert_eq!(owner.phase, Phase::Prepared);
    assert_eq!(output.ack_writes, 1);
    assert_eq!(
        output.bytes,
        &const {
            let marker =
                novarocks_failpoint::observation_marker::MEM_1_M07_HELD_LIVE_FENCE.as_bytes();
            let suffix = b" phase=prepared sequence=1\n";
            let mut bytes = [0; novarocks_failpoint::observation_marker::MEM_1_M07_HELD_LIVE_FENCE
                .len()
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
        }
    );
    let first = Arc::clone(&owner.first_failure.as_ref().unwrap().0);
    owner.prepared_with(&mut output, deadline()).unwrap_err();
    assert!(Arc::ptr_eq(
        &first,
        &owner.first_failure.as_ref().unwrap().0
    ));
    assert_eq!(output.ack_writes, 1);
}
#[test]
fn source_order_missing_owner_and_decreasing_or_over_cap_snapshot_refuse_before_line() {
    // A baseline cannot precede the original prepared ACK.
    let (mut owner, mut output) = new_owner(Ack::One);
    assert_eq!(
        retained_class(
            &owner
                .baseline_with(&mut output, [0; 3], deadline())
                .unwrap_err()
        ),
        FailureClass::Order
    );
    assert!(output.bytes.is_empty());
    // All four original log events are required; duplicate/wrong roles cannot fill slots.
    let (mut owner, mut output) = new_owner(Ack::One);
    owner.prepared_with(&mut output, deadline()).unwrap();
    let before = output.bytes.len();
    assert!(
        owner
            .baseline_with(&mut output, [0; 3], deadline())
            .is_err()
    );
    assert_eq!(output.bytes.len(), before);
    for role in ["be-0", "foreign"] {
        let (mut owner, mut output) = new_owner(Ack::One);
        owner.prepared_with(&mut output, deadline()).unwrap();
        let before = output.bytes.len();
        assert!(
            owner
                .log_owner_with(&mut output, role, 1, 1, deadline())
                .is_err()
        );
        assert_eq!(output.bytes.len(), before);
    }
    let (mut owner, mut output) = new_owner(Ack::One);
    owner.prepared_with(&mut output, deadline()).unwrap();
    owner
        .log_owner_with(&mut output, "fe", 1, 1, deadline())
        .unwrap();
    let before = output.bytes.len();
    assert!(
        owner
            .log_owner_with(&mut output, "fe", 1, 1, deadline())
            .is_err()
    );
    assert_eq!(output.bytes.len(), before);
    for sizes in [[0, LOG_BYTES + 1, 0], [9, 1, 0]] {
        let (mut owner, mut output) = new_owner(Ack::One);
        owner.prepared_with(&mut output, deadline()).unwrap();
        owners(&mut owner, &mut output);
        owner
            .baseline_with(&mut output, [10, 1, 0], deadline())
            .unwrap();
        let before = output.bytes.len();
        assert!(owner.target_with(&mut output, sizes, deadline()).is_err());
        assert_eq!(output.bytes.len(), before);
        assert_eq!(owner.phase, Phase::Target);
    }
}
struct ActualIoSource;
impl fmt::Debug for ActualIoSource {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("must not debug actual source")
    }
}
impl fmt::Display for ActualIoSource {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("must not format actual source")
    }
}
impl std::error::Error for ActualIoSource {}
struct FailOutput {
    flush_only: bool,
    written: usize,
}
impl Write for FailOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.flush_only {
            return Err(io::Error::other(ActualIoSource));
        }
        self.written += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other(ActualIoSource))
    }
}
#[test]
fn actual_write_and_flush_causes_are_shared_without_formatting_or_waiting_for_ack() {
    for flush_only in [false, true] {
        let (mut owner, _keep_actual_writer_live) = new_owner(Ack::One);
        let mut output = FailOutput {
            flush_only,
            written: 0,
        };
        let error = owner.prepared_with(&mut output, deadline()).unwrap_err();
        let shared = error.downcast_ref::<FenceError>().unwrap();
        assert_eq!(shared.0.class, FailureClass::Io);
        assert!(
            shared
                .0
                .source
                .as_ref()
                .unwrap()
                .get_ref()
                .unwrap()
                .is::<ActualIoSource>()
        );
        assert!(Arc::ptr_eq(
            &shared.0,
            &owner.first_failure.as_ref().unwrap().0
        ));
        assert_eq!(owner.phase, Phase::Prepared);
        let _ = format!("{shared:?} {shared}"); // Formats only the finite wrapper.
    }
}
#[test]
fn completed_owner_duplicate_prepared_and_zero_inode_never_emit_extra_fences() {
    let (mut owner, mut output) = new_owner(Ack::One);
    owner.prepared_with(&mut output, deadline()).unwrap();
    owners(&mut owner, &mut output);
    owner
        .baseline_with(&mut output, [0; 3], deadline())
        .unwrap();
    owner.target_with(&mut output, [0; 3], deadline()).unwrap();
    let before = output.bytes.len();
    assert!(owner.prepared_with(&mut output, deadline()).is_err());
    assert_eq!(output.bytes.len(), before);
    assert_eq!(output.ack_writes, 3);
    let (mut owner, mut output) = new_owner(Ack::One);
    owner.prepared_with(&mut output, deadline()).unwrap();
    let before = output.bytes.len();
    assert!(
        owner
            .log_owner_with(&mut output, "fe", 0, 0, deadline())
            .is_err()
    );
    assert_eq!(output.bytes.len(), before);
}
