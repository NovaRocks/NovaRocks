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

//! Private StatisticsArtifactV1 body codec, independent of root transport.
//!
//! Each record starts with exactly 24 bytes: ASCII `STA1`, then five u32LE
//! declarations (complete record bytes INCLUDING this header, field count,
//! blob-type bytes, body bytes, property count). Properties must be empty.
//! Payload is ordered i32LE field IDs, UTF-8 blob type, then opaque body bytes.
//! Headers and payload may cross segments; there is no padding, sequence or ACK.
//! A consumer must assemble the fixed header, validate declarations BEFORE
//! reserving/copying the payload, and reject an incomplete record at End.
//!
//! Construction inspects only four schema/array shapes, never row data. Each
//! step examines at most 64KiB and performs at most 1024 work operations,
//! including every duplicate-table probe. There is no cursor heap allocation:
//! its hash scratch is inline. The caller prepays the owned RecordBatch clone,
//! cursor storage, original source backing and all output/assembly ownership.
//! This module proves neither source growth admission nor provider membership,
//! execution success, FE collector admission, publication or commit authority.

use arrow::array::{Array, BinaryArray, Int32Array, ListArray, MapArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field};
use novarocks_spi::connector::{
    MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES, MAX_CONNECTOR_STATISTICS_ARTIFACTS,
    MAX_CONNECTOR_STATISTICS_COLUMNS, MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES,
    MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES,
};

pub const STATISTICS_HEADER_BYTES: usize = 24;
pub const STATISTICS_METADATA_BYTES: usize = 16 * 1024 * 1024;
pub const STATISTICS_TURN_BYTES: usize = 64 * 1024;
pub const STATISTICS_TURN_WORK: usize = 1024;
const BUCKETS: usize = 2 * MAX_CONNECTOR_STATISTICS_COLUMNS;
const MAGIC: [u8; 4] = *b"STA1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsCodecError {
    Schema,
    NullValue,
    FieldIds,
    BlobType,
    BodyLimit,
    RowLimit,
    MetadataLimit,
    Properties,
    TruncatedHeader,
    HeaderVersion,
    RecordLength,
    TruncatedRecord,
    TrailingRecordBytes,
    Failed,
    Cancelled,
}
impl std::fmt::Display for StatisticsCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Schema => "statistics artifact schema mismatch",
            Self::NullValue => "statistics artifact contains a null value",
            Self::FieldIds => "statistics artifact field IDs must be positive, unique and bounded",
            Self::BlobType => "statistics artifact blob type is empty or too large",
            Self::BodyLimit => "statistics artifact body exceeds its bounded profile",
            Self::RowLimit => "statistics artifact row count exceeds its bounded profile",
            Self::MetadataLimit => "statistics artifact metadata exceeds its bounded profile",
            Self::Properties => "ANALYZE statistics artifact properties must be empty",
            Self::TruncatedHeader => "statistics artifact header is truncated",
            Self::HeaderVersion => "unsupported statistics artifact header version",
            Self::RecordLength => {
                "statistics artifact record length disagrees with its declarations"
            }
            Self::TruncatedRecord => "statistics artifact record is truncated",
            Self::TrailingRecordBytes => "statistics artifact record has trailing bytes",
            Self::Failed => "statistics artifact cursor has failed",
            Self::Cancelled => "statistics artifact cursor was cancelled",
        })
    }
}
impl std::error::Error for StatisticsCodecError {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatisticsCodecTotals {
    pub rows: usize,
    pub body_bytes: usize,
    /// Includes headers and field IDs as well as blob-type bytes.
    pub metadata_bytes: usize,
}
impl StatisticsCodecTotals {
    /// Charge only the cumulative result-body coordinate using this codec's
    /// original bounded profile. This is pure admission, not a record receipt.
    pub fn checked_body_total(
        current: usize,
        additional: usize,
    ) -> Result<usize, StatisticsCodecError> {
        let body_bytes = current
            .checked_add(additional)
            .ok_or(StatisticsCodecError::BodyLimit)?;
        Self {
            rows: 0,
            body_bytes,
            metadata_bytes: 0,
        }
        .validate()?;
        Ok(body_bytes)
    }

    /// Charge only the cumulative row coordinate using the same profile as
    /// completed-record preflight. Reaching the bound remains legal.
    pub fn checked_row_total(
        current: usize,
        additional: usize,
    ) -> Result<usize, StatisticsCodecError> {
        let rows = current
            .checked_add(additional)
            .ok_or(StatisticsCodecError::RowLimit)?;
        Self {
            rows,
            body_bytes: 0,
            metadata_bytes: 0,
        }
        .validate()?;
        Ok(rows)
    }

    fn validate(self) -> Result<(), StatisticsCodecError> {
        if self.rows > MAX_CONNECTOR_STATISTICS_ARTIFACTS {
            return Err(StatisticsCodecError::RowLimit);
        }
        if self.body_bytes > MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES {
            return Err(StatisticsCodecError::BodyLimit);
        }
        if self.metadata_bytes > STATISTICS_METADATA_BYTES {
            return Err(StatisticsCodecError::MetadataLimit);
        }
        Ok(())
    }
    /// Pure cumulative declaration preflight; publishes no artifact facts.
    pub fn checked_add(
        self,
        header: StatisticsArtifactHeader,
    ) -> Result<Self, StatisticsCodecError> {
        let next = Self {
            rows: self
                .rows
                .checked_add(1)
                .ok_or(StatisticsCodecError::RowLimit)?,
            body_bytes: self
                .body_bytes
                .checked_add(header.body_bytes())
                .ok_or(StatisticsCodecError::BodyLimit)?,
            metadata_bytes: self
                .metadata_bytes
                .checked_add(header.metadata_bytes())
                .ok_or(StatisticsCodecError::MetadataLimit)?,
        };
        next.validate()?;
        Ok(next)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatisticsArtifactHeader {
    field_count: u32,
    blob_type_bytes: u32,
    body_bytes: u32,
    record_bytes: u32,
}
impl StatisticsArtifactHeader {
    fn try_new(fields: usize, blob: usize, body: usize) -> Result<Self, StatisticsCodecError> {
        if fields == 0 || fields > MAX_CONNECTOR_STATISTICS_COLUMNS {
            return Err(StatisticsCodecError::FieldIds);
        }
        if blob == 0 || blob > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(StatisticsCodecError::BlobType);
        }
        if body > MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES {
            return Err(StatisticsCodecError::BodyLimit);
        }
        let record = STATISTICS_HEADER_BYTES
            .checked_add(
                fields
                    .checked_mul(4)
                    .ok_or(StatisticsCodecError::RecordLength)?,
            )
            .and_then(|bytes| bytes.checked_add(blob))
            .and_then(|bytes| bytes.checked_add(body))
            .ok_or(StatisticsCodecError::RecordLength)?;
        Ok(Self {
            field_count: fields as u32,
            blob_type_bytes: blob as u32,
            body_bytes: body as u32,
            record_bytes: u32::try_from(record).map_err(|_| StatisticsCodecError::RecordLength)?,
        })
    }
    /// Reads only the fixed prefix, ignoring any following payload. Lengths
    /// are bounded and checked without copying, allocating or accessing body.
    pub fn parse(prefix: &[u8]) -> Result<Self, StatisticsCodecError> {
        if prefix.len() < STATISTICS_HEADER_BYTES {
            return Err(StatisticsCodecError::TruncatedHeader);
        }
        if prefix[..4] != MAGIC {
            return Err(StatisticsCodecError::HeaderVersion);
        }
        let word = |at: usize| u32::from_le_bytes(prefix[at..at + 4].try_into().unwrap());
        if word(20) != 0 {
            return Err(StatisticsCodecError::Properties);
        }
        let header = Self::try_new(word(8) as usize, word(12) as usize, word(16) as usize)?;
        if header.record_bytes != word(4) {
            return Err(StatisticsCodecError::RecordLength);
        }
        Ok(header)
    }
    pub const fn field_count(self) -> usize {
        self.field_count as usize
    }
    pub const fn blob_type_bytes(self) -> usize {
        self.blob_type_bytes as usize
    }
    pub const fn body_bytes(self) -> usize {
        self.body_bytes as usize
    }
    pub const fn record_bytes(self) -> usize {
        self.record_bytes as usize
    }
    pub const fn metadata_bytes(self) -> usize {
        self.record_bytes() - self.body_bytes()
    }
    /// A single assembled record must have precisely its declared length.
    /// End with fewer bytes is not an empty or completed artifact.
    pub fn validate_record_bytes(self, available: usize) -> Result<(), StatisticsCodecError> {
        match available.cmp(&self.record_bytes()) {
            std::cmp::Ordering::Less => Err(StatisticsCodecError::TruncatedRecord),
            std::cmp::Ordering::Greater => Err(StatisticsCodecError::TrailingRecordBytes),
            std::cmp::Ordering::Equal => Ok(()),
        }
    }
    fn bytes(self) -> [u8; STATISTICS_HEADER_BYTES] {
        let mut bytes = [0; STATISTICS_HEADER_BYTES];
        bytes[..4].copy_from_slice(&MAGIC);
        for (at, value) in [
            (4, self.record_bytes),
            (8, self.field_count),
            (12, self.blob_type_bytes),
            (16, self.body_bytes),
        ] {
            bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
        }
        bytes
    }
}

/// One complete, validated statistics artifact record, borrowed.
#[derive(Clone, Copy, Debug)]
pub struct StatisticsArtifactRecordView<'a> {
    header: StatisticsArtifactHeader,
    payload: &'a [u8],
}

impl<'a> StatisticsArtifactRecordView<'a> {
    /// Validate one assembled record: its header, exact length, positive
    /// field IDs and UTF-8 blob type. Properties are always empty.
    pub fn parse(record: &'a [u8]) -> Result<Self, StatisticsCodecError> {
        let header = StatisticsArtifactHeader::parse(record)?;
        header.validate_record_bytes(record.len())?;
        let view = Self {
            header,
            payload: &record[STATISTICS_HEADER_BYTES..],
        };
        if view.field_ids().any(|field| field <= 0) {
            return Err(StatisticsCodecError::FieldIds);
        }
        std::str::from_utf8(view.blob_bytes()).map_err(|_| StatisticsCodecError::BlobType)?;
        Ok(view)
    }
    pub fn field_ids(&self) -> impl Iterator<Item = i32> + 'a {
        self.payload[..self.header.field_count() * 4]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|id| i32::from_le_bytes(*id))
    }
    fn blob_bytes(&self) -> &'a [u8] {
        let start = self.header.field_count() * 4;
        &self.payload[start..start + self.header.blob_type_bytes()]
    }
    pub fn blob_type(&self) -> &'a str {
        std::str::from_utf8(self.blob_bytes()).expect("blob type validated at parse")
    }
    pub fn body(&self) -> &'a [u8] {
        let start = self.header.field_count() * 4 + self.header.blob_type_bytes();
        &self.payload[start..start + self.header.body_bytes()]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsCodecStatus {
    Yielded,
    NeedsOutput,
    InputComplete,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatisticsCodecTurn {
    pub emitted_bytes: usize,
    /// Reads/probes and emitted bytes together obey the 64KiB CPU-byte bound.
    pub examined_bytes: usize,
    /// Every field load, duplicate-table probe and output copy consumes work.
    pub work: usize,
    pub completed_rows: u64,
    pub status: StatisticsCodecStatus,
}
#[derive(Clone, Copy, Default)]
struct Entry {
    value: i32,
    generation: u32,
}
#[derive(Clone, Copy)]
enum Phase {
    Start,
    Fields,
    Emit,
    Complete,
    Failed,
    Cancelled,
}
struct CursorState {
    row: usize,
    phase: Phase,
    header: Option<StatisticsArtifactHeader>,
    header_bytes: [u8; STATISTICS_HEADER_BYTES],
    field_index: usize,
    candidate: Option<i32>,
    probe: usize,
    emit_offset: usize,
    totals: StatisticsCodecTotals,
    seen: [Entry; BUCKETS],
}
pub struct StatisticsArtifactEncoder {
    batch: Option<RecordBatch>,
    state: CursorState,
}
impl StatisticsArtifactEncoder {
    /// Extra heap backing is zero. A boxed cursor requires this inline layout
    /// (and its allocator alignment) prepaid before allocating that box.
    pub const fn inline_capacity_bytes() -> usize {
        std::mem::size_of::<Self>()
    }
    pub fn try_new(
        batch: RecordBatch,
        totals: StatisticsCodecTotals,
    ) -> Result<Self, StatisticsCodecError> {
        totals.validate()?;
        if totals
            .rows
            .checked_add(batch.num_rows())
            .is_none_or(|rows| rows > MAX_CONNECTOR_STATISTICS_ARTIFACTS)
        {
            return Err(StatisticsCodecError::RowLimit);
        }
        validate_schema(&batch)?;
        Ok(Self {
            state: CursorState {
                row: 0,
                phase: if batch.num_rows() == 0 {
                    Phase::Complete
                } else {
                    Phase::Start
                },
                header: None,
                header_bytes: [0; STATISTICS_HEADER_BYTES],
                field_index: 0,
                candidate: None,
                probe: 0,
                emit_offset: 0,
                totals,
                seen: [Entry::default(); BUCKETS],
            },
            batch: Some(batch),
        })
    }
    /// Counts only records whose entire encoding has been emitted. Callers
    /// carry these totals into the next batch; they are not commit evidence.
    pub const fn totals(&self) -> StatisticsCodecTotals {
        self.state.totals
    }
    pub fn cancel(&mut self) {
        drop(self.batch.take());
        self.state.phase = Phase::Cancelled;
    }
    pub fn step(&mut self, output: &mut [u8]) -> Result<StatisticsCodecTurn, StatisticsCodecError> {
        match self.state.phase {
            Phase::Failed => return Err(StatisticsCodecError::Failed),
            Phase::Cancelled => return Err(StatisticsCodecError::Cancelled),
            _ => {}
        }
        let result = advance(self.batch.as_ref().unwrap(), &mut self.state, output);
        if result.is_err() {
            self.state.phase = Phase::Failed;
        }
        result
    }
}

fn field_matches(field: &Field, name: &str, nullable: bool) -> bool {
    field.name() == name && field.is_nullable() == nullable && field.metadata().is_empty()
}
fn validate_schema(batch: &RecordBatch) -> Result<(), StatisticsCodecError> {
    let schema = batch.schema_ref();
    let fields = schema.fields();
    if fields.len() != 4
        || !schema.metadata().is_empty()
        || !field_matches(&fields[0], "input_fields", false)
        || !field_matches(&fields[1], "blob_type", false)
        || fields[2].name() != "body"
        || !fields[2].metadata().is_empty()
        || !field_matches(&fields[3], "properties", false)
        || !matches!(fields[1].data_type(), DataType::Utf8)
        || !matches!(fields[2].data_type(), DataType::Binary)
    {
        return Err(StatisticsCodecError::Schema);
    }
    let DataType::List(item) = fields[0].data_type() else {
        return Err(StatisticsCodecError::Schema);
    };
    if !field_matches(item, "item", false) || !matches!(item.data_type(), DataType::Int32) {
        return Err(StatisticsCodecError::Schema);
    }
    let DataType::Map(entries, false) = fields[3].data_type() else {
        return Err(StatisticsCodecError::Schema);
    };
    let DataType::Struct(children) = entries.data_type() else {
        return Err(StatisticsCodecError::Schema);
    };
    if !field_matches(entries, "entries", false)
        || children.len() != 2
        || !field_matches(&children[0], "key", false)
        || !field_matches(&children[1], "value", false)
        || !children
            .iter()
            .all(|child| matches!(child.data_type(), DataType::Utf8))
        || batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .is_none_or(|list| !list.values().as_any().is::<Int32Array>())
        || !batch.column(1).as_any().is::<StringArray>()
        || !batch.column(2).as_any().is::<BinaryArray>()
        || !batch.column(3).as_any().is::<MapArray>()
    {
        return Err(StatisticsCodecError::Schema);
    }
    Ok(())
}

fn advance(
    batch: &RecordBatch,
    state: &mut CursorState,
    output: &mut [u8],
) -> Result<StatisticsCodecTurn, StatisticsCodecError> {
    let mut turn = StatisticsCodecTurn {
        emitted_bytes: 0,
        examined_bytes: 0,
        work: 0,
        completed_rows: 0,
        status: StatisticsCodecStatus::Yielded,
    };
    let fields = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let ids = fields
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let blobs = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let bodies = batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let properties = batch.column(3).as_any().downcast_ref::<MapArray>().unwrap();
    loop {
        if matches!(state.phase, Phase::Complete) {
            turn.status = StatisticsCodecStatus::InputComplete;
            break;
        }
        // Account completion even when the last copy filled the caller's
        // output exactly. No additional segment is needed to discover End.
        if matches!(state.phase, Phase::Emit)
            && state.emit_offset == state.header.unwrap().record_bytes()
        {
            if turn.work == STATISTICS_TURN_WORK {
                break;
            }
            turn.work += 1;
            state.totals = state.totals.checked_add(state.header.unwrap())?;
            state.row += 1;
            turn.completed_rows += 1;
            state.phase = if state.row == batch.num_rows() {
                Phase::Complete
            } else {
                Phase::Start
            };
            continue;
        }
        if turn.emitted_bytes == output.len() {
            turn.status = StatisticsCodecStatus::NeedsOutput;
            break;
        }
        if turn.work == STATISTICS_TURN_WORK || turn.examined_bytes == STATISTICS_TURN_BYTES {
            break;
        }
        match state.phase {
            Phase::Start => {
                // Four null checks and four pairs of i32 offsets are fixed
                // work, even for zero-length bodies. No values are copied.
                if STATISTICS_TURN_WORK - turn.work < 12
                    || STATISTICS_TURN_BYTES - turn.examined_bytes < 36
                {
                    break;
                }
                turn.work += 12;
                turn.examined_bytes += 36;
                let row = state.row;
                if fields.is_null(row)
                    || blobs.is_null(row)
                    || bodies.is_null(row)
                    || properties.is_null(row)
                {
                    return Err(StatisticsCodecError::NullValue);
                }
                if properties.value_offsets()[row + 1] != properties.value_offsets()[row] {
                    return Err(StatisticsCodecError::Properties);
                }
                let count =
                    (fields.value_offsets()[row + 1] - fields.value_offsets()[row]) as usize;
                let header = StatisticsArtifactHeader::try_new(
                    count,
                    blobs.value(row).len(),
                    bodies.value(row).len(),
                )?;
                state.totals.checked_add(header)?;
                state.header_bytes = header.bytes();
                state.header = Some(header);
                state.field_index = 0;
                state.candidate = None;
                state.emit_offset = 0;
                state.phase = Phase::Fields;
            }
            Phase::Fields => {
                let count = state.header.unwrap().field_count();
                if state.field_index == count {
                    state.phase = Phase::Emit;
                    continue;
                }
                if state.candidate.is_none() {
                    if STATISTICS_TURN_BYTES - turn.examined_bytes < 5 {
                        break;
                    }
                    turn.work += 1;
                    turn.examined_bytes += 5;
                    let index = fields.value_offsets()[state.row] as usize + state.field_index;
                    if ids.is_null(index) {
                        return Err(StatisticsCodecError::NullValue);
                    }
                    let id = ids.value(index);
                    if id <= 0 {
                        return Err(StatisticsCodecError::FieldIds);
                    }
                    state.candidate = Some(id);
                    state.probe = (id as u32).wrapping_mul(0x9e3779b1) as usize & (BUCKETS - 1);
                    continue;
                }
                if STATISTICS_TURN_BYTES - turn.examined_bytes < std::mem::size_of::<Entry>() {
                    break;
                }
                turn.work += 1;
                turn.examined_bytes += std::mem::size_of::<Entry>();
                let generation = state.row as u32 + 1;
                let candidate = state.candidate.unwrap();
                let entry = &mut state.seen[state.probe];
                if entry.generation != generation {
                    *entry = Entry {
                        value: candidate,
                        generation,
                    };
                    state.field_index += 1;
                    state.candidate = None;
                } else if entry.value == candidate {
                    return Err(StatisticsCodecError::FieldIds);
                } else {
                    state.probe = (state.probe + 1) & (BUCKETS - 1);
                }
            }
            Phase::Emit => {
                let header = state.header.unwrap();
                let field_end = STATISTICS_HEADER_BYTES + header.field_count() * 4;
                let blob_end = field_end + header.blob_type_bytes();
                let at = state.emit_offset;
                let overhead = if at < STATISTICS_HEADER_BYTES {
                    0
                } else if at < field_end {
                    4
                } else {
                    8
                };
                if STATISTICS_TURN_BYTES - turn.examined_bytes <= overhead {
                    break;
                }
                turn.examined_bytes += overhead;
                let id_bytes;
                let remaining = if at < STATISTICS_HEADER_BYTES {
                    &state.header_bytes[at..]
                } else if at < field_end {
                    let field_at = at - STATISTICS_HEADER_BYTES;
                    id_bytes = ids
                        .value(fields.value_offsets()[state.row] as usize + field_at / 4)
                        .to_le_bytes();
                    &id_bytes[field_at % 4..]
                } else if at < blob_end {
                    &blobs.value(state.row).as_bytes()[at - field_end..]
                } else {
                    &bodies.value(state.row)[at - blob_end..]
                };
                let count = remaining
                    .len()
                    .min(output.len() - turn.emitted_bytes)
                    .min(STATISTICS_TURN_BYTES - turn.examined_bytes);
                output[turn.emitted_bytes..turn.emitted_bytes + count]
                    .copy_from_slice(&remaining[..count]);
                state.emit_offset += count;
                turn.emitted_bytes += count;
                turn.examined_bytes += count;
                turn.work += 1;
            }
            Phase::Complete | Phase::Failed | Phase::Cancelled => unreachable!(),
        }
    }
    Ok(turn)
}

impl novarocks_execution_contract::failure::FailureProjection for StatisticsCodecError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let (class, code) = match self {
            Self::Schema => (FailureClass::Invariant, 100),
            Self::NullValue => (FailureClass::Data, 101),
            Self::FieldIds => (FailureClass::Data, 102),
            Self::BlobType => (FailureClass::Data, 103),
            Self::BodyLimit => (FailureClass::Resource, 104),
            Self::RowLimit => (FailureClass::Resource, 105),
            Self::MetadataLimit => (FailureClass::Resource, 106),
            Self::Properties => (FailureClass::Data, 107),
            Self::TruncatedHeader => (FailureClass::Data, 108),
            Self::HeaderVersion => (FailureClass::Data, 109),
            Self::RecordLength => (FailureClass::Data, 110),
            Self::TruncatedRecord => (FailureClass::Data, 111),
            Self::TrailingRecordBytes => (FailureClass::Data, 112),
            Self::Failed => (FailureClass::Invariant, 113),
            Self::Cancelled => (FailureClass::Control, 114),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
