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

//! Independent bounded row/framing oracle for the exact native MySQL matrix.
use crate::actors::exact_mysql_control_v2::{
    ClientBodyFacts, ConnectionFacts, CurrentSourceFacts, CursorFacts, CursorPhase, GatePhase,
    OriginalFreezeFacts, ReplyFacts, RootDataFacts, RootKindFacts, RootTaskFacts, StatementFacts,
};
use anyhow::{Result, ensure};
use novarocks_types::FrontendProcessId;
use sha2::{Digest, Sha256};

const S: u64 = 1_048_576;
const U24: u64 = 0x00ff_ffff;

/// Independently selected exact input, not copied from a production encoder.
#[derive(Clone, Copy)]
pub struct RowInput {
    pub columns: u8,
    pub value_bytes: u32,
    pub repeated_byte: u8,
    pub cut: u64,
    pub expect_complete_tail: bool,
}
impl RowInput {
    pub fn payload_bytes(self) -> Result<u64> {
        // This exact matrix cannot silently borrow a missing-tail outcome from
        // a separate small-row/coalesced/pool-full experiment.
        let frozen = match (self.columns, self.value_bytes, self.repeated_byte) {
            (1, 3, b'a') => (1..=6).contains(&self.cut) && self.expect_complete_tail,
            (1, 1_048_576, b'x') => {
                (S - 1..=S + 1).contains(&self.cut) && self.expect_complete_tail
            }
            (17, 1_048_576, b'q') => self.cut == S + 1 && !self.expect_complete_tail,
            _ => false,
        };
        ensure!(
            frozen,
            "input/cut/outcome is outside the independently frozen matrix"
        );
        Ok(u64::from(self.columns)
            * (u64::from(self.value_bytes) + if self.value_bytes == 3 { 1 } else { 4 }))
    }
    fn payload_byte(self, offset: u64) -> Result<u8> {
        let length = self.payload_bytes()?;
        ensure!(offset < length, "row payload byte is out of range");
        if self.value_bytes == 3 {
            return Ok([3, b'a', b'b', b'c'][offset as usize]);
        }
        let local = offset % (u64::from(self.value_bytes) + 4);
        Ok(match local {
            0 => 0xfd,
            1 => 0,
            2 => 0,
            3 => 0x10,
            _ => self.repeated_byte,
        })
    }
    /// Row-wire offsets exclude metadata. All matrix cuts are in packet one.
    fn wire_byte(self, offset: u64) -> Result<u8> {
        ensure!(offset < self.cut, "row-wire prefix byte is out of range");
        let length = self.payload_bytes()?.min(U24);
        Ok(if offset < 4 {
            [
                (length & 255) as u8,
                ((length >> 8) & 255) as u8,
                ((length >> 16) & 255) as u8,
                self.columns + 3,
            ][offset as usize]
        } else {
            self.payload_byte(offset - 4)?
        })
    }
    pub fn prefix_hash(self) -> Result<[u8; 32]> {
        ensure!(
            self.cut > 0 && self.cut < self.payload_bytes()?.min(U24) + 4,
            "matrix cut must leave the first row packet incomplete"
        );
        let mut hash = Sha256::new();
        let mut scratch = [0; 4096];
        let mut at = 0;
        while at < self.cut {
            let n = (self.cut - at).min(scratch.len() as u64) as usize;
            for (i, byte) in scratch[..n].iter_mut().enumerate() {
                *byte = self.wire_byte(at + i as u64)?;
            }
            hash.update(&scratch[..n]);
            at += n as u64;
        }
        Ok(hash.finalize().into())
    }
}

/// Caller must bind these from actual independent role/handshake/root source
/// observations. Supplying the reply's own values here would be circular.
/// Root fields are compared bit-for-bit, never guessed or minted by this module.
pub struct ActualBinding {
    pub frontend: FrontendProcessId,
    pub connection: ConnectionFacts,
    pub statement: StatementFacts,
    pub root: RootTaskFacts,
    pub backend_uuid_from_role: [u8; 16],
    pub sql_sha256: [u8; 32],
}

impl ActualBinding {
    /// Preserve raw original domains and independently observed descriptor UUID.
    /// No control reply supplies any expected identity field.
    pub(crate) fn from_original_sources(
        raw: crate::exact_mysql_target_binding::OriginalTargetBinding,
        target: &super::independent_root_target::IndependentRootTarget,
    ) -> Result<Self> {
        ensure!(
            raw.frontend_process_id == target.frontend
                && raw.connection_id != 0
                && raw.session_connection_id == raw.connection_id
                && raw.connection_generation != 0
                && raw.session_key != 0
                && raw.statement_generation != 0,
            "original FE target binding differs from independent root frontend or has invalid domains"
        );
        ensure!(
            target.root.backend_process_id() == target.backend_process_from_descriptor,
            "original root differs from independent descriptor UUID"
        );
        let execution = target.root.query_execution_id();
        Ok(Self {
            frontend: raw.frontend_process_id,
            connection: ConnectionFacts {
                connection_id: raw.connection_id,
                generation: raw.connection_generation,
            },
            statement: StatementFacts {
                connection_id: raw.session_connection_id,
                session_key: raw.session_key,
                generation: raw.statement_generation,
            },
            root: RootTaskFacts {
                query_high: execution.query_id().high(),
                query_low: execution.query_id().low(),
                attempt: execution.attempt_id().get(),
                stage: target.root.stage_id().get(),
                task: target.root.task_id().get(),
                backend_uuid: target.root.backend_process_id().to_bytes(),
            },
            backend_uuid_from_role: target.backend_process_from_descriptor.to_bytes(),
            sql_sha256: raw.sql_sha256,
        })
    }
}

fn body(input: RowInput, observed: ClientBodyFacts) -> Result<u64> {
    let payload = input.payload_bytes()?;
    ensure!(
        (1..=S).contains(&observed.body_bytes),
        "validated body byte count is outside S"
    );
    ensure!(
        observed.before_completed_rows == 0 && observed.before_remaining as u64 <= payload,
        "body is not in the original one-row stream"
    );
    let bytes = if observed.before_remaining == 0 {
        ensure!(
            observed.body_bytes >= 5,
            "new row body lacks prefix plus payload"
        );
        observed.body_bytes - 4
    } else {
        observed.body_bytes
    };
    let before = if observed.before_remaining == 0 {
        payload
    } else {
        u64::from(observed.before_remaining)
    };
    ensure!(bytes <= before, "body includes an unexpected suffix row");
    let after = before - bytes;
    ensure!(
        u64::from(observed.after_remaining) == after
            && observed.after_completed_rows == u64::from(after == 0),
        "validated before/after differs from the independent row length"
    );
    Ok(bytes)
}
fn root_data(binding: &ActualBinding, data: RootDataFacts) -> Result<()> {
    ensure!(
        data.root == binding.root && data.root.backend_uuid == binding.backend_uuid_from_role,
        "full original root identity or actual backend process differs"
    );
    ensure!(
        data.profile == 1
            && data.kind == RootKindFacts::ClientRows
            && data.native_sequence > 0
            && (1..=S).contains(&data.body_bytes)
            && data.accepted_consumed < data.native_sequence,
        "root profile/kind/item metadata differs"
    );
    if let Some(end) = data.end_after_data {
        ensure!(
            data.native_sequence.checked_add(1) == Some(end.sequence) && end.output_rows == 1,
            "EndAfterData differs from the original one-row stream"
        );
    }
    Ok(())
}
fn cursor(input: RowInput, baseline: CursorFacts, cut: CursorFacts) -> Result<()> {
    let payload = input.payload_bytes()?;
    let packet = payload.min(U24);
    ensure!(
        baseline.phase == CursorPhase::Boundary
            && baseline.rows_completed == 0
            && baseline.sequence == input.columns + 3,
        "original metadata boundary/sequence differs"
    );
    let header_written = input.cut.min(4) as u8;
    let payload_written = input.cut.saturating_sub(4) as u32;
    ensure!(
        cut.phase == CursorPhase::Row
            && cut.rows_completed == 0
            && u64::from(cut.logical_total) == payload
            && cut.logical_written == payload_written
            && u64::from(cut.packet_payload_length) == packet
            && cut.packet_payload_written == payload_written
            && cut.header_written == header_written
            && cut.header
                == [
                    (packet & 255) as u8,
                    ((packet >> 8) & 255) as u8,
                    ((packet >> 16) & 255) as u8,
                    baseline.sequence
                ]
            && cut.sequence == baseline.sequence
            && cut.zero_terminal_pending == (payload % U24 == 0)
            && baseline.committed_wire_bytes.checked_add(input.cut)
                == Some(cut.committed_wire_bytes),
        "actual original framing cursor differs from the exact cut"
    );
    Ok(())
}

/// Pure scalar comparison only. Not a Closing grant, ACK proof, body hash,
/// allocator/last-alias proof, physical exit receipt or native acceptance verdict.
pub fn compare_cancel(input: RowInput, binding: &ActualBinding, reply: &ReplyFacts) -> Result<()> {
    ensure!(
        reply.frontend == binding.frontend && reply.failure.is_none(),
        "control identity or failure differs"
    );
    let gate = reply
        .gate
        .ok_or_else(|| anyhow::anyhow!("missing actual original gate"))?;
    ensure!(
        gate.connection == binding.connection
            && gate.statement == Some(binding.statement)
            && gate.exact_sql_sha256 == Some(binding.sql_sha256)
            && gate.failure.is_none()
            && gate.phase == GatePhase::Resumed
            && gate.cut_bytes == input.cut
            && gate.accepted_prefix_bytes == input.cut
            && gate.accepted_prefix_sha256 == input.prefix_hash()?
            && gate.blocked_after_acceptance
            && gate.writer_attached
            && gate.successful_inner_writes > 0
            && gate
                .scalar_inner_polls
                .checked_add(gate.vectored_inner_polls)
                .is_some_and(|n| n >= gate.successful_inner_writes),
        "actual gate did not record and locally resume the exact original cut"
    );
    let baseline = gate
        .baseline
        .ok_or_else(|| anyhow::anyhow!("missing metadata baseline"))?;
    let cancellation = gate
        .cancel_receipt
        .ok_or_else(|| anyhow::anyhow!("missing actual cancel receipt"))?;
    cursor(input, baseline, cancellation)?;
    let freeze = reply
        .original_freeze
        .ok_or_else(|| anyhow::anyhow!("missing ONE original freeze"))?;
    ensure!(
        freeze.framing == cancellation,
        "original freeze is not the gate's original cancellation receipt"
    );
    compare_freeze(input, binding, &freeze)
}

pub fn compare_freeze(
    input: RowInput,
    binding: &ActualBinding,
    freeze: &OriginalFreezeFacts,
) -> Result<()> {
    ensure!(
        freeze.had_resident_window,
        "native scene has no original resident window"
    );
    for item in freeze.slots.iter().flatten() {
        root_data(binding, item.data)?;
        // root_relay returns wanted sequence; result_pump stores that very sequence.
        ensure!(
            item.window_sequence == item.data.native_sequence,
            "original window/native item sequence differs"
        );
    }
    if let Some(data) = freeze.fallback_delivery {
        root_data(binding, data)?;
    }
    let (selected, next) = match freeze.current_source {
        CurrentSourceFacts::FrozenDelivering => {
            let selected = freeze.slots[0]
                .ok_or_else(|| anyhow::anyhow!("missing original delivering slot"))?;
            ensure!(
                selected.has_validated_client_rows,
                "original delivering item was not validated"
            );
            (selected.data, freeze.slots[1])
        }
        CurrentSourceFacts::FrozenReady => {
            ensure!(
                freeze.slots[0].is_none_or(|slot| !slot.has_validated_client_rows),
                "ready source bypasses delivering body"
            );
            let selected =
                freeze.slots[1].ok_or_else(|| anyhow::anyhow!("missing original ready slot"))?;
            ensure!(
                selected.has_validated_client_rows,
                "original ready item was not validated"
            );
            (selected.data, None)
        }
        CurrentSourceFacts::OriginalDeliveryFallback => {
            ensure!(
                freeze
                    .slots
                    .iter()
                    .flatten()
                    .all(|slot| !slot.has_validated_client_rows),
                "fallback bypasses an original validated resident body"
            );
            (
                freeze
                    .fallback_delivery
                    .ok_or_else(|| anyhow::anyhow!("missing original fallback"))?,
                None,
            )
        }
        CurrentSourceFacts::None => anyhow::bail!("partial row has no original current body"),
    };
    let current = freeze
        .current
        .ok_or_else(|| anyhow::anyhow!("missing original validated current body"))?;
    ensure!(
        selected.body_bytes == current.body_bytes,
        "selected root and validated body lengths differ"
    );
    if current.before_remaining == 0 {
        ensure!(
            selected.native_sequence == 1,
            "new original row must start at first native item"
        );
    }
    let mut lengths = [body(input, current)?, 0];
    let mut count = 1;
    let mut final_body = current;
    ensure!(
        freeze.next.is_some() == next.is_some_and(|slot| slot.has_validated_client_rows),
        "next validated body presence differs from original slots"
    );
    if let Some(next_body) = freeze.next {
        let next_data = next
            .ok_or_else(|| anyhow::anyhow!("missing original next item"))?
            .data;
        ensure!(
            selected.native_sequence.checked_add(1) == Some(next_data.native_sequence)
                && next_data.body_bytes == next_body.body_bytes
                && (current.after_remaining, current.after_completed_rows)
                    == (next_body.before_remaining, next_body.before_completed_rows),
            "original next item sequence or validated cursor is not contiguous"
        );
        lengths[1] = body(input, next_body)?;
        count = 2;
        final_body = next_body;
    }
    for (slot, validated) in [
        (Some(selected), Some(current)),
        (next.map(|slot| slot.data), freeze.next),
    ] {
        if let (Some(data), Some(validated)) = (slot, validated) {
            if let Some(item) = freeze.slots.iter().flatten().find(|item| item.data == data) {
                ensure!(
                    item.completed_rows_by_item
                        == validated.after_completed_rows - validated.before_completed_rows,
                    "original resident completed-row count differs from validated body"
                );
            }
            if data == selected
                && freeze.current_source == CurrentSourceFacts::OriginalDeliveryFallback
            {
                ensure!(
                    freeze.fallback_delivery_rows
                        == Some(validated.after_completed_rows - validated.before_completed_rows),
                    "original fallback completed-row count differs from validated body"
                );
            }
            if data.end_after_data.is_some() {
                ensure!(
                    validated.after_remaining == 0 && validated.after_completed_rows == 1,
                    "EndAfterData precedes the complete original row"
                );
            }
        }
    }
    let payload = input.payload_bytes()?;
    ensure!(
        freeze.framing.phase == CursorPhase::Row
            && freeze.framing.rows_completed == 0
            && u64::from(freeze.framing.logical_total) == payload
            && u64::from(freeze.framing.logical_written) == input.cut.saturating_sub(4),
        "original freeze framing differs from the independently frozen partial-row cut"
    );
    let remaining = payload
        .checked_sub(u64::from(freeze.framing.logical_written))
        .ok_or_else(|| anyhow::anyhow!("original framing exceeds the frozen row length"))?;
    // Entry to write_body checks the writer's original remaining against this
    // body's before frontier. A one-row Data write can only decrease remaining
    // through this body, and cannot buffer bytes from the following Data body.
    let body_start = if current.before_remaining == 0 {
        payload
    } else {
        u64::from(current.before_remaining)
    };
    let body_end = u64::from(current.after_remaining);
    ensure!(
        body_end <= remaining && remaining <= body_start,
        "original writer frontier is outside the selected current Data body"
    );
    ensure!(
        freeze.buffered_row_bytes <= remaining - body_end,
        "original writer buffer exceeds the uncommitted selected current Data payload"
    );
    let unsent = payload
        .checked_sub(u64::from(freeze.framing.logical_written))
        .and_then(|n| n.checked_sub(freeze.buffered_row_bytes))
        .ok_or_else(|| {
            anyhow::anyhow!("original buffered/current row progress exceeds row length")
        })?;
    let sum = lengths[0]
        .checked_add(lengths[1])
        .ok_or_else(|| anyhow::anyhow!("tail length overflow"))?;
    let complete = unsent == 0
        || (final_body.after_remaining == 0
            && final_body.after_completed_rows == 1
            && sum >= unsent);
    ensure!(
        complete == input.expect_complete_tail && freeze.tail_complete == complete,
        "original validated coverage differs from the required scene"
    );
    if unsent == 0 {
        ensure!(
            freeze.tail_parts == 0
                && freeze.tail_part_bytes == [0, 0]
                && freeze.tail_selected_bytes == 0,
            "already buffered suffix has nonempty selected tail"
        );
    } else if complete {
        let mut skip = sum - unsent;
        for part in &mut lengths[..count] {
            let n = skip.min(*part);
            *part -= n;
            skip -= n;
        }
        ensure!(
            freeze.tail_parts as usize == count
                && freeze.tail_part_bytes == lengths
                && freeze.tail_selected_bytes == unsent,
            "original selected tail differs from independent suffix arithmetic"
        );
    } else {
        ensure!(
            freeze.tail_parts == 0
                && freeze.tail_part_bytes == [0, 0]
                && freeze.tail_selected_bytes == 0,
            "missing original tail carries fabricated selected bytes"
        );
        ensure!(
            final_body.after_remaining > 0 && final_body.after_completed_rows == 0,
            "missing tail is not a validated unfinished original row"
        );
    }
    Ok(())
}

/// Actual reader must feed row packet bytes after strict metadata decode, through
/// exactly the cut. No socket receive-buffer or paused-reader inference is used.
/// Fixed-size state, incremental byte comparison/hash, no row-sized allocation.
pub struct PrefixOracle {
    input: RowInput,
    seen: u64,
    hash: Sha256,
}
impl PrefixOracle {
    pub fn new(input: RowInput) -> Result<Self> {
        input.prefix_hash()?;
        Ok(Self {
            input,
            seen: 0,
            hash: Sha256::new(),
        })
    }
    pub fn consume(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(
            self.seen
                .checked_add(bytes.len() as u64)
                .is_some_and(|n| n <= self.input.cut),
            "observed wire exceeds exact prefix"
        );
        for (i, actual) in bytes.iter().enumerate() {
            ensure!(
                *actual == self.input.wire_byte(self.seen + i as u64)?,
                "actual row wire prefix byte differs"
            );
        }
        self.hash.update(bytes);
        self.seen += bytes.len() as u64;
        Ok(())
    }
    pub fn finish(self, gate_hash: [u8; 32]) -> Result<()> {
        ensure!(
            self.seen == self.input.cut
                && <[u8; 32]>::from(self.hash.finalize()) == gate_hash
                && gate_hash == self.input.prefix_hash()?,
            "actual wire/gate/literal prefix digest differs"
        );
        Ok(())
    }
}

/// One-row independent MySQL packet decoder/oracle. The caller has already
/// strictly decoded complete metadata and its actual next sequence. This seam
/// accepts row wire only, never a terminal packet or a concatenated next row.
pub struct RowWireOracle {
    input: RowInput,
    seen: u64,
    payload_hash: Sha256,
}
impl RowWireOracle {
    pub fn new(input: RowInput, actual_next_sequence: u8) -> Result<Self> {
        input.payload_bytes()?;
        ensure!(
            actual_next_sequence == input.columns + 3,
            "actual metadata packet sequence differs"
        );
        Ok(Self {
            input,
            seen: 0,
            payload_hash: Sha256::new(),
        })
    }
    pub fn row_packets(&self) -> Result<u64> {
        Ok(self.input.payload_bytes()? / U24 + 1)
    }
    pub fn wire_length(&self) -> Result<u64> {
        Ok(self.input.payload_bytes()? + self.row_packets()? * 4)
    }
    pub fn consume(&mut self, bytes: &[u8]) -> Result<()> {
        let wire_length = self.wire_length()?;
        ensure!(
            self.seen
                .checked_add(bytes.len() as u64)
                .is_some_and(|n| n <= wire_length),
            "wire exceeds exactly one original row"
        );
        let payload = self.input.payload_bytes()?;
        for (i, actual) in bytes.iter().enumerate() {
            let at = self.seen + i as u64;
            let ordinal = at / (U24 + 4);
            let local = at % (U24 + 4);
            let length = (payload - ordinal * U24).min(U24);
            let expected = if local < 4 {
                [
                    (length & 255) as u8,
                    ((length >> 8) & 255) as u8,
                    ((length >> 16) & 255) as u8,
                    (self.input.columns + 3).wrapping_add(ordinal as u8),
                ][local as usize]
            } else {
                ensure!(
                    local - 4 < length,
                    "packet payload exceeds exact U24 remainder"
                );
                self.input.payload_byte(ordinal * U24 + local - 4)?
            };
            ensure!(
                *actual == expected,
                "actual MySQL row packet/header/sequence/value differs"
            );
            if local >= 4 {
                self.payload_hash.update([*actual]);
            }
        }
        self.seen += bytes.len() as u64;
        Ok(())
    }
    /// Existing TextResultObservation hashes the row payload followed by LE u64 length.
    pub fn finish(mut self, frozen_row_sha256: [u8; 32]) -> Result<()> {
        ensure!(
            self.seen == self.wire_length()?,
            "original row is incomplete"
        );
        self.payload_hash
            .update(self.input.payload_bytes()?.to_le_bytes());
        ensure!(
            <[u8; 32]>::from(self.payload_hash.finalize()) == frozen_row_sha256,
            "full original row digest differs"
        );
        Ok(())
    }
}

/// Bounded whole ERR packet, after exactly one decoded row. Dynamic message
/// bytes are not used as an expected string, and no successful EOF is accepted.
pub fn interrupted_terminal(input: RowInput, packet: &[u8]) -> Result<()> {
    ensure!(
        (13..=4096).contains(&packet.len()),
        "terminal packet outside frozen observer bound"
    );
    let length =
        usize::from(packet[0]) | usize::from(packet[1]) << 8 | usize::from(packet[2]) << 16;
    let row_packets = input.payload_bytes()? / U24 + 1;
    ensure!(
        length + 4 == packet.len()
            && packet[3] == (input.columns + 3).wrapping_add(row_packets as u8)
            && packet[4..13] == [0xff, 0x25, 0x05, b'#', b'7', b'0', b'1', b'0', b'0'],
        "terminal is not complete ERR1317/70100 at the actual next row boundary"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // Synthetic parser facts only. These tests do not establish actual native IO.
    fn tiny(cut: u64) -> RowInput {
        RowInput {
            columns: 1,
            value_bytes: 3,
            repeated_byte: b'a',
            cut,
            expect_complete_tail: true,
        }
    }
    #[test]
    fn tiny_prefix_literal_and_wrong_sequence_are_distinguished() {
        let mut oracle = PrefixOracle::new(tiny(6)).unwrap();
        oracle.consume(&[4, 0, 0]).unwrap();
        oracle.consume(&[4, 3, b'a']).unwrap();
        oracle.finish(tiny(6).prefix_hash().unwrap()).unwrap();
        let mut wrong = PrefixOracle::new(tiny(6)).unwrap();
        assert!(wrong.consume(&[4, 0, 0, 5, 3, b'a']).is_err());
        let incomplete = PrefixOracle::new(tiny(6)).unwrap();
        assert!(incomplete.finish(tiny(6).prefix_hash().unwrap()).is_err());
    }
    #[test]
    fn row_decoder_rejects_truncation_extra_row_and_err_inside_row() {
        let mut hash = Sha256::new();
        hash.update([3, b'a', b'b', b'c']);
        hash.update(4u64.to_le_bytes());
        let expected: [u8; 32] = hash.finalize().into();
        let mut row = RowWireOracle::new(tiny(6), 4).unwrap();
        row.consume(&[4, 0, 0, 4, 3, b'a', b'b', b'c']).unwrap();
        row.finish(expected).unwrap();
        let mut row = RowWireOracle::new(tiny(6), 4).unwrap();
        assert!(row.consume(&[4, 0, 0, 4, 0xff]).is_err());
        let mut row = RowWireOracle::new(tiny(6), 4).unwrap();
        row.consume(&[4, 0, 0, 4]).unwrap();
        assert!(row.finish(expected).is_err());
        let mut row = RowWireOracle::new(tiny(6), 4).unwrap();
        assert!(row.consume(&[4, 0, 0, 4, 3, b'a', b'b', b'c', 0]).is_err());
    }
    #[test]
    fn whole_interrupted_terminal_rejects_success_and_wrong_sequence() {
        let mut err = [
            9, 0, 0, 5, 0xff, 0x25, 0x05, b'#', b'7', b'0', b'1', b'0', b'0',
        ];
        interrupted_terminal(tiny(6), &err).unwrap();
        err[3] = 4;
        assert!(interrupted_terminal(tiny(6), &err).is_err());
        err[3] = 5;
        err[4] = 0xfe;
        assert!(interrupted_terminal(tiny(6), &err).is_err());
        assert!(interrupted_terminal(tiny(6), &err[..12]).is_err());
    }
}

#[cfg(test)]
mod frozen_geometry_tests {
    use super::*;
    use crate::actors::exact_mysql_control_v2::{GateFacts, Opcode, ResidentFacts};
    // Synthetic fixed DTO fixtures only. No actual FE/BE, token owner, IO,
    // window capability, original freeze, W2 or successful native proof exists.
    fn binding() -> ActualBinding {
        let backend = novarocks_types::BackendProcessId::new_v7().to_bytes();
        ActualBinding {
            frontend: FrontendProcessId::new_v7(),
            connection: ConnectionFacts {
                connection_id: 7,
                generation: 11,
            },
            statement: StatementFacts {
                connection_id: 7,
                session_key: 13,
                generation: 17,
            },
            root: RootTaskFacts {
                query_high: -19,
                query_low: 23,
                attempt: 1,
                stage: 2,
                task: 3,
                backend_uuid: backend,
            },
            backend_uuid_from_role: backend,
            sql_sha256: [29; 32],
        }
    }
    fn large(cut: u64) -> RowInput {
        RowInput {
            columns: 1,
            value_bytes: S as u32,
            repeated_byte: b'x',
            cut,
            expect_complete_tail: true,
        }
    }
    fn tiny(cut: u64) -> RowInput {
        RowInput {
            columns: 1,
            value_bytes: 3,
            repeated_byte: b'a',
            cut,
            expect_complete_tail: true,
        }
    }
    fn data(binding: &ActualBinding, sequence: u64, bytes: u64) -> RootDataFacts {
        RootDataFacts {
            root: binding.root,
            profile: 1,
            kind: RootKindFacts::ClientRows,
            accepted_consumed: sequence - 1,
            native_sequence: sequence,
            body_bytes: bytes,
            end_after_data: None,
        }
    }
    fn baseline(input: RowInput) -> CursorFacts {
        CursorFacts {
            phase: CursorPhase::Boundary,
            sequence: input.columns + 3,
            logical_total: 0,
            logical_written: 0,
            packet_payload_length: 0,
            packet_payload_written: 0,
            header: [0; 4],
            header_written: 0,
            zero_terminal_pending: false,
            committed_wire_bytes: 101,
            rows_completed: 0,
        }
    }
    fn receipt(input: RowInput) -> CursorFacts {
        let payload = input.payload_bytes().unwrap();
        let packet = payload.min(U24);
        CursorFacts {
            phase: CursorPhase::Row,
            sequence: input.columns + 3,
            logical_total: payload as u32,
            logical_written: input.cut.saturating_sub(4) as u32,
            packet_payload_length: packet as u32,
            packet_payload_written: input.cut.saturating_sub(4) as u32,
            header: [
                (packet & 255) as u8,
                ((packet >> 8) & 255) as u8,
                ((packet >> 16) & 255) as u8,
                input.columns + 3,
            ],
            header_written: input.cut.min(4) as u8,
            zero_terminal_pending: payload.is_multiple_of(U24),
            committed_wire_bytes: baseline(input).committed_wire_bytes + input.cut,
            rows_completed: 0,
        }
    }
    fn reply(
        input: RowInput,
        binding: &ActualBinding,
        current: ClientBodyFacts,
        sequence: u64,
        buffered: u64,
    ) -> ReplyFacts {
        let source = data(binding, sequence, current.body_bytes);
        let freeze = OriginalFreezeFacts {
            had_resident_window: true,
            slots: [
                Some(ResidentFacts {
                    data: source,
                    window_sequence: sequence,
                    completed_rows_by_item: current.after_completed_rows
                        - current.before_completed_rows,
                    has_validated_client_rows: true,
                }),
                None,
            ],
            fallback_delivery: Some(source),
            fallback_delivery_rows: Some(
                current.after_completed_rows - current.before_completed_rows,
            ),
            current_source: CurrentSourceFacts::FrozenDelivering,
            framing: receipt(input),
            buffered_row_bytes: buffered,
            current: Some(current),
            next: None,
            tail_complete: true,
            tail_parts: 0,
            tail_part_bytes: [0, 0],
            tail_selected_bytes: 0,
        };
        ReplyFacts {
            opcode: Opcode::Snapshot,
            frontend: binding.frontend,
            accepted_peers: 1,
            commands: 3,
            request_wire_bytes: 1,
            response_wire_bytes_before_current_reply: 1,
            explicit_stop: false,
            used_arm: true,
            stopped: false,
            failure: None,
            original_writer_exited: false,
            gate: Some(GateFacts {
                connection: binding.connection,
                statement: Some(binding.statement),
                exact_sql_sha256: Some(binding.sql_sha256),
                phase: GatePhase::Resumed,
                failure: None,
                cut_bytes: input.cut,
                accepted_prefix_bytes: input.cut,
                accepted_prefix_sha256: input.prefix_hash().unwrap(),
                scalar_inner_polls: 0,
                vectored_inner_polls: 2,
                successful_inner_writes: 1,
                blocked_after_acceptance: true,
                baseline: Some(baseline(input)),
                cancel_receipt: Some(receipt(input)),
                writer_attached: true,
                writer_exited: false,
            }),
            original_freeze: Some(freeze),
        }
    }
    #[test]
    fn s_minus_one_future_native2_body_and_buffer9_cannot_fabricate_complete_tail() {
        let input = large(S - 1);
        let binding = binding();
        let current = ClientBodyFacts {
            body_bytes: 8,
            before_remaining: 8,
            before_completed_rows: 0,
            after_remaining: 0,
            after_completed_rows: 1,
        };
        let observed = reply(input, &binding, current, 2, 9);
        // Old arithmetic alone had remaining9-buffer9==0 and accepted an
        // empty complete tail. Entry to this current Data body is impossible.
        assert!(compare_cancel(input, &binding, &observed).is_err());
    }
    #[test]
    fn s_minus_one_current_native1_cannot_buffer_its_following_native2_bytes() {
        let input = large(S - 1);
        let binding = binding();
        let current = ClientBodyFacts {
            body_bytes: S,
            before_remaining: 0,
            before_completed_rows: 0,
            after_remaining: 8,
            after_completed_rows: 0,
        };
        let observed = reply(input, &binding, current, 1, 9);
        // Current body only has one uncommitted payload byte at this cut.
        // Buffered9 cannot come from it even though the old unsent was zero.
        assert!(compare_cancel(input, &binding, &observed).is_err());
    }
    #[test]
    fn tiny_all_six_cuts_keep_valid_already_buffered_empty_tail() {
        let binding = binding();
        for cut in 1..=6 {
            let input = tiny(cut);
            let current = ClientBodyFacts {
                body_bytes: 8,
                before_remaining: 0,
                before_completed_rows: 0,
                after_remaining: 0,
                after_completed_rows: 1,
            };
            let buffered = input.payload_bytes().unwrap() - cut.saturating_sub(4);
            compare_cancel(
                input,
                &binding,
                &reply(input, &binding, current, 1, buffered),
            )
            .unwrap();
        }
    }
    #[test]
    fn large_s_and_s_plus_one_keep_valid_write_slice_buffered_empty_tail() {
        let binding = binding();
        for cut in [S, S + 1] {
            let input = large(cut);
            let current = ClientBodyFacts {
                body_bytes: 8,
                before_remaining: 8,
                before_completed_rows: 0,
                after_remaining: 0,
                after_completed_rows: 1,
            };
            let buffered = input.payload_bytes().unwrap() - cut.saturating_sub(4);
            compare_cancel(
                input,
                &binding,
                &reply(input, &binding, current, 2, buffered),
            )
            .unwrap();
        }
    }
    #[test]
    fn s_minus_one_keeps_zero_length_current_slice_and_valid_original_next_tail() {
        let input = large(S - 1);
        let binding = binding();
        let current = ClientBodyFacts {
            body_bytes: S,
            before_remaining: 0,
            before_completed_rows: 0,
            after_remaining: 8,
            after_completed_rows: 0,
        };
        let mut observed = reply(input, &binding, current, 1, 1);
        let next = ClientBodyFacts {
            body_bytes: 8,
            before_remaining: 8,
            before_completed_rows: 0,
            after_remaining: 0,
            after_completed_rows: 1,
        };
        let freeze = observed.original_freeze.as_mut().unwrap();
        freeze.slots[1] = Some(ResidentFacts {
            data: data(&binding, 2, 8),
            window_sequence: 2,
            completed_rows_by_item: 1,
            has_validated_client_rows: true,
        });
        freeze.next = Some(next);
        freeze.tail_parts = 2;
        freeze.tail_part_bytes = [0, 8];
        freeze.tail_selected_bytes = 8;
        compare_cancel(input, &binding, &observed).unwrap();
    }
    #[test]
    fn exact_cut_and_outcome_set_cannot_relabel_the_separate_missing_tail_matrix() {
        assert!(tiny(7).payload_bytes().is_err());
        assert!(large(S - 2).payload_bytes().is_err());
        let mut input = tiny(1);
        input.expect_complete_tail = false;
        assert!(input.payload_bytes().is_err());
        let mut input = large(S);
        input.expect_complete_tail = false;
        assert!(input.payload_bytes().is_err());
        let mut wide = RowInput {
            columns: 17,
            value_bytes: S as u32,
            repeated_byte: b'q',
            cut: S + 1,
            expect_complete_tail: false,
        };
        wide.payload_bytes().unwrap();
        wide.cut = S;
        assert!(wide.payload_bytes().is_err());
        wide.cut = S + 1;
        wide.expect_complete_tail = true;
        assert!(wide.payload_bytes().is_err());
    }
}
