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

//! Private PreparedWriteCommitV1 record codec, independent of root transport.
//!
//! One record per fixed Root write result row. Each record starts with a
//! 48-byte header: ASCII `PWC1`, then u32LE complete record bytes (INCLUDING
//! this header), u8 kind (1 ROW_COUNT, 2 PREPARED_FRAGMENT, 3 ARTIFACT_DRAFT, 4 DELETE_VERIFICATION),
//! three zero bytes, i32LE target ordinal (0 for ROW_COUNT), u64LE row count
//! (ROW_COUNT only), then u32LE fragment bytes, field count, blob-type bytes,
//! body bytes, property count and property key+value bytes. The payload is
//! the fragment bytes, or for an artifact draft: i32LE field IDs, UTF-8 blob
//! type, opaque body, then each property as u32LE key length, key, u32LE value
//! length, value. Records may cross segments; there is no padding.
//!
//! DELETE_VERIFICATION has zero header payload declarations and exactly 40 payload
//! bytes: three i64LE counts and a 16-byte little-endian fingerprint.
//!
//! Every single-value, count and cumulative limit the frontend applies is
//! checked from the row's lengths BEFORE its header is written, so a refused
//! row emits no byte. Exactly one ROW_COUNT is required: a second one is refused
//! before emission, and its absence is refused by [`WriteCommitTotals::finish`]
//! at the sealed End. Field-ID/property-key uniqueness, the frozen target and
//! artifact sets, and commit authority stay with the frontend decoder, which
//! owns those frozen facts.
//!
//! Each step examines at most 64KiB and performs at most 1024 work operations.
//! The cursor allocates nothing; the caller prepays its storage and the
//! RecordBatch columns Vec clone, and retains the original input owner.

use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, Int8Array, Int32Array, Int64Array, ListArray,
    MapArray, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field};
use novarocks_result_render::{RenderTurn, RenderTurnStatus};
use novarocks_spi::connector::write_stack::{
    MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES,
    MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES, MAX_CONNECTOR_WRITE_TARGETS,
    ROOT_WRITE_RESULT_BLOB_TYPE_COLUMN, ROOT_WRITE_RESULT_BODY_COLUMN,
    ROOT_WRITE_RESULT_COLUMN_COUNT, ROOT_WRITE_RESULT_FRAGMENT_COLUMN,
    ROOT_WRITE_RESULT_INPUT_FIELDS_COLUMN, ROOT_WRITE_RESULT_INSUFFICIENT_FINGERPRINT_COLUMN,
    ROOT_WRITE_RESULT_INSUFFICIENT_KEYS_COLUMN, ROOT_WRITE_RESULT_KIND_COLUMN,
    ROOT_WRITE_RESULT_MATCHED_COLUMN, ROOT_WRITE_RESULT_PROPERTIES_COLUMN,
    ROOT_WRITE_RESULT_REQUESTED_COLUMN, ROOT_WRITE_RESULT_ROW_COUNT_COLUMN,
    ROOT_WRITE_RESULT_TARGET_COLUMN, RootRowKind,
};
use novarocks_spi::connector::{
    MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES, MAX_CONNECTOR_STATISTICS_ARTIFACTS,
    MAX_CONNECTOR_STATISTICS_COLUMNS, MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES,
    MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES,
};

pub const WRITE_COMMIT_HEADER_BYTES: usize = 48;
pub const DELETE_VERIFICATION_PAYLOAD_BYTES: usize = 40;
use novarocks_result_contract::DeleteVerificationCounts;
// The spi checks below would allocate a ConnectorError on refusal; the cursor
// applies the same rules directly so that no step ever allocates.
fn kind_from_wire(kind: i8) -> Result<RootRowKind, WriteCommitCodecError> {
    match kind {
        RootRowKind::ROW_COUNT => Ok(RootRowKind::RowCount),
        RootRowKind::PREPARED_FRAGMENT => Ok(RootRowKind::PreparedFragment),
        RootRowKind::ARTIFACT_DRAFT => Ok(RootRowKind::ArtifactDraft),
        RootRowKind::DELETE_VERIFICATION => Ok(RootRowKind::DeleteVerification),
        _ => Err(WriteCommitCodecError::Kind),
    }
}
/// `target_ordinal_from_wire`: non-negative and below the frozen target count.
fn valid_target(target: i32) -> Result<(), WriteCommitCodecError> {
    if usize::try_from(target).is_ok_and(|target| target < MAX_CONNECTOR_WRITE_TARGETS) {
        Ok(())
    } else {
        Err(WriteCommitCodecError::Target)
    }
}
pub const WRITE_COMMIT_TURN_BYTES: usize = 64 * 1024;
pub const WRITE_COMMIT_TURN_WORK: usize = 1024;
const MAGIC: [u8; 4] = *b"PWC1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteCommitCodecError {
    Schema,
    Kind,
    Shape,
    Target,
    RowCount,
    DuplicateRowCount,
    DuplicateDeleteVerification,
    DeleteVerificationCount,
    MissingRowCount,
    FragmentLimit,
    ArtifactLimit,
    FieldIds,
    BlobType,
    BodyLimit,
    PropertyLimit,
    PropertyKey,
    NullValue,
    TruncatedHeader,
    HeaderVersion,
    RecordLength,
    TruncatedRecord,
    TrailingRecordBytes,
    Failed,
}
impl std::fmt::Display for WriteCommitCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Schema => "write commit root schema differs from its fixed contract",
            Self::Kind => "write commit row kind is unknown",
            Self::Shape => "write commit row payload does not match its kind",
            Self::Target => "write commit target ordinal is invalid",
            Self::RowCount => "write commit row count is negative",
            Self::DuplicateRowCount => "write commit stream contains more than one ROW_COUNT",
            Self::MissingRowCount => "write commit stream ended without its ROW_COUNT",
            Self::DeleteVerificationCount => "delete verification count must be nonnegative",
            Self::DuplicateDeleteVerification => {
                "write commit stream contains more than one DELETE_VERIFICATION"
            }
            Self::FragmentLimit => "prepared write set exceeds its frozen fragment budget",
            Self::ArtifactLimit => "write commit artifact count exceeds its bounded profile",
            Self::FieldIds => "write commit artifact field IDs must be positive and bounded",
            Self::BlobType => "write commit artifact blob type is empty or too large",
            Self::BodyLimit => "write commit artifact body exceeds its bounded profile",
            Self::PropertyLimit => "write commit artifact properties exceed their bounded profile",
            Self::PropertyKey => "write commit artifact property key is empty",
            Self::NullValue => "write commit artifact contains a null nested value",
            Self::TruncatedHeader => "write commit record header is truncated",
            Self::HeaderVersion => "unsupported write commit record header",
            Self::RecordLength => "write commit record length disagrees with its declarations",
            Self::TruncatedRecord => "write commit record is truncated",
            Self::TrailingRecordBytes => "write commit record has trailing bytes",
            Self::Failed => "write commit cursor has failed",
        })
    }
}
impl std::error::Error for WriteCommitCodecError {}

/// One validated record declaration. Construction and [`Self::parse`] apply
/// the same single-value limits, so an encoder cannot declare what a decoder
/// would refuse.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteCommitRecordHeader {
    kind: RootRowKind,
    target: i32,
    row_count: u64,
    fragment_bytes: u32,
    field_count: u32,
    blob_type_bytes: u32,
    body_bytes: u32,
    property_count: u32,
    property_bytes: u32,
    record_bytes: u32,
}
impl WriteCommitRecordHeader {
    fn row_count_record(row_count: i64) -> Result<Self, WriteCommitCodecError> {
        let row_count = u64::try_from(row_count).map_err(|_| WriteCommitCodecError::RowCount)?;
        Self::try_new(RootRowKind::RowCount, 0, row_count, 0, [0; 5])
    }
    fn fragment(target: i32, bytes: usize) -> Result<Self, WriteCommitCodecError> {
        let bytes = u32::try_from(bytes).map_err(|_| WriteCommitCodecError::FragmentLimit)?;
        Self::try_new(RootRowKind::PreparedFragment, target, 0, bytes, [0; 5])
    }
    fn artifact(
        target: i32,
        fields: usize,
        blob: usize,
        body: usize,
        properties: usize,
        property_bytes: usize,
    ) -> Result<Self, WriteCommitCodecError> {
        let narrow = |value: usize, error| u32::try_from(value).map_err(|_| error);
        Self::try_new(
            RootRowKind::ArtifactDraft,
            target,
            0,
            0,
            [
                narrow(fields, WriteCommitCodecError::FieldIds)?,
                narrow(blob, WriteCommitCodecError::BlobType)?,
                narrow(body, WriteCommitCodecError::BodyLimit)?,
                narrow(properties, WriteCommitCodecError::PropertyLimit)?,
                narrow(property_bytes, WriteCommitCodecError::PropertyLimit)?,
            ],
        )
    }
    fn try_new(
        kind: RootRowKind,
        target: i32,
        row_count: u64,
        fragment_bytes: u32,
        [
            field_count,
            blob_type_bytes,
            body_bytes,
            property_count,
            property_bytes,
        ]: [u32; 5],
    ) -> Result<Self, WriteCommitCodecError> {
        let artifact_empty = field_count == 0
            && blob_type_bytes == 0
            && body_bytes == 0
            && property_count == 0
            && property_bytes == 0;
        match kind {
            RootRowKind::DeleteVerification => {
                if target != 0 || row_count != 0 || fragment_bytes != 0 || !artifact_empty {
                    return Err(WriteCommitCodecError::Shape);
                }
            }
            RootRowKind::RowCount => {
                if target != 0 || fragment_bytes != 0 || !artifact_empty {
                    return Err(WriteCommitCodecError::Shape);
                }
                if i64::try_from(row_count).is_err() {
                    return Err(WriteCommitCodecError::RowCount);
                }
            }
            RootRowKind::PreparedFragment => {
                valid_target(target)?;
                if row_count != 0 || !artifact_empty {
                    return Err(WriteCommitCodecError::Shape);
                }
                if fragment_bytes as usize > MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES {
                    return Err(WriteCommitCodecError::FragmentLimit);
                }
            }
            RootRowKind::ArtifactDraft => {
                valid_target(target)?;
                if row_count != 0 || fragment_bytes != 0 {
                    return Err(WriteCommitCodecError::Shape);
                }
                if field_count == 0 || field_count as usize > MAX_CONNECTOR_STATISTICS_COLUMNS {
                    return Err(WriteCommitCodecError::FieldIds);
                }
                if blob_type_bytes == 0
                    || blob_type_bytes as usize > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
                {
                    return Err(WriteCommitCodecError::BlobType);
                }
                if body_bytes as usize > MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES {
                    return Err(WriteCommitCodecError::BodyLimit);
                }
                // Every key is non-empty, so a count above the byte total is
                // impossible; reject it before any per-entry work.
                if property_bytes as usize > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
                    || property_count > property_bytes
                {
                    return Err(WriteCommitCodecError::PropertyLimit);
                }
            }
        }
        let record = [
            if kind == RootRowKind::DeleteVerification {
                DELETE_VERIFICATION_PAYLOAD_BYTES
            } else {
                0
            },
            fragment_bytes as usize,
            field_count as usize * 4,
            blob_type_bytes as usize,
            body_bytes as usize,
            property_count as usize * 8,
            property_bytes as usize,
        ]
        .into_iter()
        .try_fold(WRITE_COMMIT_HEADER_BYTES, usize::checked_add)
        .and_then(|bytes| u32::try_from(bytes).ok())
        .ok_or(WriteCommitCodecError::RecordLength)?;
        Ok(Self {
            kind,
            target,
            row_count,
            fragment_bytes,
            field_count,
            blob_type_bytes,
            body_bytes,
            property_count,
            property_bytes,
            record_bytes: record,
        })
    }
    /// Reads only the fixed prefix. Declarations are bounded and checked
    /// without copying, allocating or touching any payload byte.
    pub fn parse(prefix: &[u8]) -> Result<Self, WriteCommitCodecError> {
        if prefix.len() < WRITE_COMMIT_HEADER_BYTES {
            return Err(WriteCommitCodecError::TruncatedHeader);
        }
        if prefix[..4] != MAGIC || prefix[9..12] != [0; 3] {
            return Err(WriteCommitCodecError::HeaderVersion);
        }
        let word = |at: usize| u32::from_le_bytes(prefix[at..at + 4].try_into().unwrap());
        let kind = kind_from_wire(prefix[8] as i8)?;
        let header = Self::try_new(
            kind,
            i32::from_le_bytes(prefix[12..16].try_into().unwrap()),
            u64::from_le_bytes(prefix[16..24].try_into().unwrap()),
            word(24),
            [word(28), word(32), word(36), word(40), word(44)],
        )?;
        if header.record_bytes != word(4) {
            return Err(WriteCommitCodecError::RecordLength);
        }
        Ok(header)
    }
    fn bytes(self) -> [u8; WRITE_COMMIT_HEADER_BYTES] {
        let mut bytes = [0; WRITE_COMMIT_HEADER_BYTES];
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4..8].copy_from_slice(&self.record_bytes.to_le_bytes());
        bytes[8] = self.kind.to_wire() as u8;
        bytes[12..16].copy_from_slice(&self.target.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.row_count.to_le_bytes());
        for (at, value) in [
            (24, self.fragment_bytes),
            (28, self.field_count),
            (32, self.blob_type_bytes),
            (36, self.body_bytes),
            (40, self.property_count),
            (44, self.property_bytes),
        ] {
            bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
        }
        bytes
    }
    pub const fn kind(self) -> RootRowKind {
        self.kind
    }
    pub const fn target(self) -> i32 {
        self.target
    }
    pub const fn row_count(self) -> u64 {
        self.row_count
    }
    pub const fn fragment_bytes(self) -> usize {
        self.fragment_bytes as usize
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
    pub const fn property_count(self) -> usize {
        self.property_count as usize
    }
    pub const fn property_bytes(self) -> usize {
        self.property_bytes as usize
    }
    pub const fn record_bytes(self) -> usize {
        self.record_bytes as usize
    }
    /// An assembled record must have precisely its declared length.
    pub fn validate_record_bytes(self, available: usize) -> Result<(), WriteCommitCodecError> {
        match available.cmp(&self.record_bytes()) {
            std::cmp::Ordering::Less => Err(WriteCommitCodecError::TruncatedRecord),
            std::cmp::Ordering::Greater => Err(WriteCommitCodecError::TrailingRecordBytes),
            std::cmp::Ordering::Equal => Ok(()),
        }
    }
}

/// One complete, validated write-commit record, borrowed without copies.
#[derive(Clone, Copy, Debug)]
pub struct WriteCommitRecordView<'a> {
    header: WriteCommitRecordHeader,
    payload: &'a [u8],
}

impl<'a> WriteCommitRecordView<'a> {
    /// Validate one assembled record: its header, exact length, UTF-8 blob
    /// type and property text, non-empty keys, positive field IDs and the
    /// declared property byte total.
    pub fn parse(record: &'a [u8]) -> Result<Self, WriteCommitCodecError> {
        let header = WriteCommitRecordHeader::parse(record)?;
        header.validate_record_bytes(record.len())?;
        let view = Self {
            header,
            payload: &record[WRITE_COMMIT_HEADER_BYTES..],
        };
        if header.kind() == RootRowKind::ArtifactDraft {
            if view.field_ids().any(|field| field <= 0) {
                return Err(WriteCommitCodecError::FieldIds);
            }
            std::str::from_utf8(view.blob_bytes()).map_err(|_| WriteCommitCodecError::BlobType)?;
            let mut property_bytes = 0usize;
            for property in view.raw_properties() {
                let (key, value) = property?;
                if key.is_empty() {
                    return Err(WriteCommitCodecError::PropertyKey);
                }
                property_bytes += key.len() + value.len();
            }
            if property_bytes != header.property_bytes() {
                return Err(WriteCommitCodecError::RecordLength);
            }
        }
        if header.kind() == RootRowKind::DeleteVerification {
            view.read_delete_verification()?;
        }
        Ok(view)
    }

    fn read_delete_verification(&self) -> Result<DeleteVerificationCounts, WriteCommitCodecError> {
        let count = |at| {
            i64::from_le_bytes(
                self.payload[at..at + 8]
                    .try_into()
                    .expect("fixed record payload"),
            )
        };
        DeleteVerificationCounts::try_from_wire(
            count(0),
            count(8),
            count(16),
            self.payload[24..40]
                .try_into()
                .expect("fixed fingerprint width"),
        )
        .map_err(|_| WriteCommitCodecError::DeleteVerificationCount)
    }
    pub fn delete_verification(&self) -> Option<DeleteVerificationCounts> {
        (self.header.kind() == RootRowKind::DeleteVerification).then(|| {
            self.read_delete_verification()
                .expect("record payload validated at parse")
        })
    }

    pub const fn header(&self) -> WriteCommitRecordHeader {
        self.header
    }
    /// A prepared fragment's canonical bytes; empty for other kinds.
    pub fn fragment(&self) -> &'a [u8] {
        &self.payload[..self.header.fragment_bytes()]
    }
    pub fn field_ids(&self) -> impl Iterator<Item = i32> + 'a {
        let fields = &self.payload[..self.header.field_count() * 4];
        fields
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
    pub fn properties(&self) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.raw_properties()
            .map(|property| property.expect("properties validated at parse"))
    }
    fn raw_properties(
        &self,
    ) -> impl Iterator<Item = Result<(&'a str, &'a str), WriteCommitCodecError>> + 'a {
        let start = self.header.field_count() * 4
            + self.header.blob_type_bytes()
            + self.header.body_bytes();
        let mut rest = &self.payload[start..];
        let count = self.header.property_count();
        (0..count).map(move |_| {
            let mut text = || -> Result<&'a str, WriteCommitCodecError> {
                if rest.len() < 4 {
                    return Err(WriteCommitCodecError::RecordLength);
                }
                let length = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
                let bytes = rest
                    .get(4..4 + length)
                    .ok_or(WriteCommitCodecError::RecordLength)?;
                rest = &rest[4 + length..];
                std::str::from_utf8(bytes).map_err(|_| WriteCommitCodecError::PropertyLimit)
            };
            let key = text()?;
            let value = text()?;
            Ok((key, value))
        })
    }
}

/// Cumulative facts of every record whose encoding has completed. Callers
/// carry them across input batches; they are not commit evidence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteCommitTotals {
    row_count_seen: bool,
    delete_verification_seen: bool,
    /// `PreparedWriteSetLedger` semantics: reaching a bound is legal.
    fragments: usize,
    fragment_bytes: usize,
    artifacts: usize,
    body_bytes: usize,
    property_bytes: usize,
}
impl WriteCommitTotals {
    /// Pure cumulative preflight; publishes nothing.
    pub fn checked_add(
        self,
        header: WriteCommitRecordHeader,
    ) -> Result<Self, WriteCommitCodecError> {
        let mut next = self;
        match header.kind {
            RootRowKind::DeleteVerification => {
                if next.delete_verification_seen {
                    return Err(WriteCommitCodecError::DuplicateDeleteVerification);
                }
                next.delete_verification_seen = true;
            }
            RootRowKind::RowCount => {
                if next.row_count_seen {
                    return Err(WriteCommitCodecError::DuplicateRowCount);
                }
                next.row_count_seen = true;
            }
            RootRowKind::PreparedFragment => {
                next.fragments = next
                    .fragments
                    .checked_add(1)
                    .filter(|count| *count <= MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES)
                    .ok_or(WriteCommitCodecError::FragmentLimit)?;
                next.fragment_bytes = next
                    .fragment_bytes
                    .checked_add(header.fragment_bytes())
                    .filter(|bytes| *bytes <= MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES)
                    .ok_or(WriteCommitCodecError::FragmentLimit)?;
            }
            RootRowKind::ArtifactDraft => {
                next.artifacts = next
                    .artifacts
                    .checked_add(1)
                    .filter(|count| *count <= MAX_CONNECTOR_STATISTICS_ARTIFACTS)
                    .ok_or(WriteCommitCodecError::ArtifactLimit)?;
                next.body_bytes = next
                    .body_bytes
                    .checked_add(header.body_bytes())
                    .filter(|bytes| *bytes <= MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES)
                    .ok_or(WriteCommitCodecError::BodyLimit)?;
                next.property_bytes = next
                    .property_bytes
                    .checked_add(header.property_bytes())
                    .filter(|bytes| *bytes <= MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES)
                    .ok_or(WriteCommitCodecError::PropertyLimit)?;
            }
        }
        Ok(next)
    }
    /// The sealed normal End of one write root: its ROW_COUNT must exist.
    pub fn finish(self) -> Result<(), WriteCommitCodecError> {
        if self.row_count_seen {
            Ok(())
        } else {
            Err(WriteCommitCodecError::MissingRowCount)
        }
    }
    pub const fn row_count_seen(&self) -> bool {
        self.row_count_seen
    }
    pub const fn fragments(&self) -> usize {
        self.fragments
    }
    pub const fn fragment_bytes(&self) -> usize {
        self.fragment_bytes
    }
    pub const fn artifacts(&self) -> usize {
        self.artifacts
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Part {
    Header,
    Fragment,
    DeleteVerification,
    Field(usize),
    Blob,
    Body,
    KeyLength(usize),
    Key(usize),
    ValueLength(usize),
    Value(usize),
    Done,
}
#[derive(Clone, Copy)]
enum Phase {
    Start,
    /// Artifact nested values: next field index, then next property entry.
    Validate {
        field: usize,
        property: usize,
        property_bytes: usize,
    },
    Emit {
        part: Part,
        offset: usize,
    },
    Complete,
    Failed,
}
struct CursorState {
    row: usize,
    phase: Phase,
    header: Option<WriteCommitRecordHeader>,
    header_bytes: [u8; WRITE_COMMIT_HEADER_BYTES],
    verification_bytes: [u8; DELETE_VERIFICATION_PAYLOAD_BYTES],
    pending: WriteCommitTotals,
    totals: WriteCommitTotals,
}

struct Columns<'a> {
    kinds: &'a Int8Array,
    targets: &'a Int32Array,
    counts: &'a Int64Array,
    fragments: &'a BinaryArray,
    fields: &'a ListArray,
    ids: &'a Int32Array,
    blobs: &'a StringArray,
    bodies: &'a BinaryArray,
    properties: &'a MapArray,
    keys: &'a StringArray,
    values: &'a StringArray,
    requested: &'a Int64Array,
    matched: &'a Int64Array,
    insufficient_keys: &'a Int64Array,
    fingerprint: &'a FixedSizeBinaryArray,
}

pub struct WriteCommitEncoder {
    batch: RecordBatch,
    state: CursorState,
}
impl WriteCommitEncoder {
    /// No extra heap backing. A boxed cursor requires this inline layout
    /// prepaid before allocating that box.
    pub const fn inline_capacity_bytes() -> usize {
        std::mem::size_of::<Self>()
    }
    pub fn try_new(
        batch: RecordBatch,
        totals: WriteCommitTotals,
    ) -> Result<Self, WriteCommitCodecError> {
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
                header_bytes: [0; WRITE_COMMIT_HEADER_BYTES],
                verification_bytes: [0; DELETE_VERIFICATION_PAYLOAD_BYTES],
                pending: totals,
                totals,
            },
            batch,
        })
    }
    /// Totals of records whose entire encoding has been emitted.
    pub const fn totals(&self) -> WriteCommitTotals {
        self.state.totals
    }
    pub fn step(&mut self, output: &mut [u8]) -> Result<RenderTurn, WriteCommitCodecError> {
        if matches!(self.state.phase, Phase::Failed) {
            return Err(WriteCommitCodecError::Failed);
        }
        let columns = columns(&self.batch);
        let result = advance(&columns, self.batch.num_rows(), &mut self.state, output);
        if result.is_err() {
            self.state.phase = Phase::Failed;
        }
        result
    }
}

fn plain(field: &Field, name: &str, nullable: bool, data_type: &DataType) -> bool {
    field.name() == name
        && field.is_nullable() == nullable
        && field.metadata().is_empty()
        && field.data_type() == data_type
}
/// The fixed twelve-column contract, compared without building a schema.
fn validate_schema(batch: &RecordBatch) -> Result<(), WriteCommitCodecError> {
    let schema = batch.schema_ref();
    let fields = schema.fields();
    if fields.len() != ROOT_WRITE_RESULT_COLUMN_COUNT
        || !schema.metadata().is_empty()
        || !plain(
            &fields[0],
            ROOT_WRITE_RESULT_KIND_COLUMN,
            false,
            &DataType::Int8,
        )
        || !plain(
            &fields[1],
            ROOT_WRITE_RESULT_TARGET_COLUMN,
            true,
            &DataType::Int32,
        )
        || !plain(
            &fields[2],
            ROOT_WRITE_RESULT_ROW_COUNT_COLUMN,
            true,
            &DataType::Int64,
        )
        || !plain(
            &fields[3],
            ROOT_WRITE_RESULT_FRAGMENT_COLUMN,
            true,
            &DataType::Binary,
        )
        || !plain(
            &fields[5],
            ROOT_WRITE_RESULT_BLOB_TYPE_COLUMN,
            true,
            &DataType::Utf8,
        )
        || !plain(
            &fields[6],
            ROOT_WRITE_RESULT_BODY_COLUMN,
            true,
            &DataType::Binary,
        )
        || !plain(
            &fields[8],
            ROOT_WRITE_RESULT_REQUESTED_COLUMN,
            true,
            &DataType::Int64,
        )
        || !plain(
            &fields[9],
            ROOT_WRITE_RESULT_MATCHED_COLUMN,
            true,
            &DataType::Int64,
        )
        || !plain(
            &fields[10],
            ROOT_WRITE_RESULT_INSUFFICIENT_KEYS_COLUMN,
            true,
            &DataType::Int64,
        )
        || !plain(
            &fields[11],
            ROOT_WRITE_RESULT_INSUFFICIENT_FINGERPRINT_COLUMN,
            true,
            &DataType::FixedSizeBinary(16),
        )
        || fields[4].name() != ROOT_WRITE_RESULT_INPUT_FIELDS_COLUMN
        || !fields[4].is_nullable()
        || !fields[4].metadata().is_empty()
        || fields[7].name() != ROOT_WRITE_RESULT_PROPERTIES_COLUMN
        || !fields[7].is_nullable()
        || !fields[7].metadata().is_empty()
    {
        return Err(WriteCommitCodecError::Schema);
    }
    let DataType::List(item) = fields[4].data_type() else {
        return Err(WriteCommitCodecError::Schema);
    };
    let DataType::Map(entries, false) = fields[7].data_type() else {
        return Err(WriteCommitCodecError::Schema);
    };
    let DataType::Struct(children) = entries.data_type() else {
        return Err(WriteCommitCodecError::Schema);
    };
    if !plain(item, "item", false, &DataType::Int32)
        || entries.name() != "entries"
        || entries.is_nullable()
        || !entries.metadata().is_empty()
        || children.len() != 2
        || !plain(&children[0], "key", false, &DataType::Utf8)
        || !plain(&children[1], "value", false, &DataType::Utf8)
    {
        return Err(WriteCommitCodecError::Schema);
    }
    let concrete = |index: usize| batch.column(index).as_any();
    let list_values = concrete(4)
        .downcast_ref::<ListArray>()
        .map(|list| list.values().as_any().is::<Int32Array>());
    let map_values = concrete(7).downcast_ref::<MapArray>().map(|map| {
        map.keys().as_any().is::<StringArray>() && map.values().as_any().is::<StringArray>()
    });
    if !concrete(0).is::<Int8Array>()
        || !concrete(1).is::<Int32Array>()
        || !concrete(2).is::<Int64Array>()
        || !concrete(3).is::<BinaryArray>()
        || list_values != Some(true)
        || !concrete(5).is::<StringArray>()
        || !concrete(6).is::<BinaryArray>()
        || map_values != Some(true)
        || !concrete(8).is::<Int64Array>()
        || !concrete(9).is::<Int64Array>()
        || !concrete(10).is::<Int64Array>()
        || !concrete(11).is::<FixedSizeBinaryArray>()
    {
        return Err(WriteCommitCodecError::Schema);
    }
    Ok(())
}
fn columns(batch: &RecordBatch) -> Columns<'_> {
    fn exact<T: 'static>(array: &dyn Array) -> &T {
        array
            .as_any()
            .downcast_ref::<T>()
            .expect("write commit carriers are checked at construction")
    }
    let fields = exact::<ListArray>(batch.column(4).as_ref());
    let properties = exact::<MapArray>(batch.column(7).as_ref());
    Columns {
        kinds: exact(batch.column(0).as_ref()),
        targets: exact(batch.column(1).as_ref()),
        counts: exact(batch.column(2).as_ref()),
        fragments: exact(batch.column(3).as_ref()),
        fields,
        ids: exact(fields.values().as_ref()),
        blobs: exact(batch.column(5).as_ref()),
        bodies: exact(batch.column(6).as_ref()),
        properties,
        keys: exact(properties.keys().as_ref()),
        values: exact(properties.values().as_ref()),
        requested: exact(batch.column(8).as_ref()),
        matched: exact(batch.column(9).as_ref()),
        insufficient_keys: exact(batch.column(10).as_ref()),
        fingerprint: exact(batch.column(11).as_ref()),
    }
}

/// Row shape from null flags and lengths only; no value is read.
fn start_row(columns: &Columns<'_>, row: usize) -> Result<Phase, WriteCommitCodecError> {
    let c = columns;
    if c.kinds.is_null(row) {
        return Err(WriteCommitCodecError::Shape);
    }
    let kind = kind_from_wire(c.kinds.value(row))?;
    let present = [
        !c.targets.is_null(row),
        !c.counts.is_null(row),
        !c.fragments.is_null(row),
        !c.fields.is_null(row),
        !c.blobs.is_null(row),
        !c.bodies.is_null(row),
        !c.properties.is_null(row),
        !c.requested.is_null(row),
        !c.matched.is_null(row),
        !c.insufficient_keys.is_null(row),
        !c.fingerprint.is_null(row),
    ];
    let expected = match kind {
        RootRowKind::DeleteVerification => [
            false, false, false, false, false, false, false, true, true, true, true,
        ],
        RootRowKind::RowCount => [
            false, true, false, false, false, false, false, false, false, false, false,
        ],
        RootRowKind::PreparedFragment => [
            true, false, true, false, false, false, false, false, false, false, false,
        ],
        RootRowKind::ArtifactDraft => [
            true, false, false, true, true, true, true, false, false, false, false,
        ],
    };
    if present != expected {
        return Err(WriteCommitCodecError::Shape);
    }
    Ok(match kind {
        RootRowKind::ArtifactDraft => Phase::Validate {
            field: 0,
            property: 0,
            property_bytes: 0,
        },
        RootRowKind::RowCount | RootRowKind::PreparedFragment | RootRowKind::DeleteVerification => {
            Phase::Emit {
                part: Part::Header,
                offset: 0,
            }
        }
    })
}
fn span(offsets: &[i32], row: usize) -> (usize, usize) {
    (offsets[row] as usize, offsets[row + 1] as usize)
}
fn header_for(
    columns: &Columns<'_>,
    row: usize,
    property_bytes: usize,
) -> Result<WriteCommitRecordHeader, WriteCommitCodecError> {
    let c = columns;
    match kind_from_wire(c.kinds.value(row))? {
        RootRowKind::DeleteVerification => {
            verification_from_columns(c, row)?;
            WriteCommitRecordHeader::try_new(RootRowKind::DeleteVerification, 0, 0, 0, [0; 5])
        }
        RootRowKind::RowCount => WriteCommitRecordHeader::row_count_record(c.counts.value(row)),
        RootRowKind::PreparedFragment => WriteCommitRecordHeader::fragment(
            c.targets.value(row),
            c.fragments.value_length(row) as usize,
        ),
        RootRowKind::ArtifactDraft => {
            let (field_start, field_end) = span(c.fields.value_offsets(), row);
            let (entry_start, entry_end) = span(c.properties.value_offsets(), row);
            WriteCommitRecordHeader::artifact(
                c.targets.value(row),
                field_end - field_start,
                c.blobs.value_length(row) as usize,
                c.bodies.value_length(row) as usize,
                entry_end - entry_start,
                property_bytes,
            )
        }
    }
}

fn verification_from_columns(
    c: &Columns<'_>,
    row: usize,
) -> Result<DeleteVerificationCounts, WriteCommitCodecError> {
    DeleteVerificationCounts::try_from_wire(
        c.requested.value(row),
        c.matched.value(row),
        c.insufficient_keys.value(row),
        c.fingerprint
            .value(row)
            .try_into()
            .expect("validated fixed width"),
    )
    .map_err(|_| WriteCommitCodecError::DeleteVerificationCount)
}
fn verification_payload(
    c: &Columns<'_>,
    row: usize,
) -> Result<[u8; DELETE_VERIFICATION_PAYLOAD_BYTES], WriteCommitCodecError> {
    let (counts, fingerprint) = verification_from_columns(c, row)?.into_wire();
    let mut bytes = [0; DELETE_VERIFICATION_PAYLOAD_BYTES];
    for (index, count) in counts.into_iter().enumerate() {
        bytes[index * 8..index * 8 + 8].copy_from_slice(&count.to_le_bytes());
    }
    bytes[24..].copy_from_slice(&fingerprint);
    Ok(bytes)
}

enum Bytes<'a> {
    Borrowed(&'a [u8]),
    Word([u8; 4]),
}
fn part_bytes<'a>(columns: &Columns<'a>, state: &'a CursorState, part: Part) -> Bytes<'a> {
    let c = columns;
    let row = state.row;
    let entry = |index: usize| c.properties.value_offsets()[row] as usize + index;
    let length = |value: &str| Bytes::Word((value.len() as u32).to_le_bytes());
    match part {
        Part::Header => Bytes::Borrowed(&state.header_bytes),
        Part::Fragment => Bytes::Borrowed(c.fragments.value(row)),
        Part::DeleteVerification => Bytes::Borrowed(&state.verification_bytes),
        Part::Field(index) => Bytes::Word(
            c.ids
                .value(c.fields.value_offsets()[row] as usize + index)
                .to_le_bytes(),
        ),
        Part::Blob => Bytes::Borrowed(c.blobs.value(row).as_bytes()),
        Part::Body => Bytes::Borrowed(c.bodies.value(row)),
        Part::KeyLength(index) => length(c.keys.value(entry(index))),
        Part::Key(index) => Bytes::Borrowed(c.keys.value(entry(index)).as_bytes()),
        Part::ValueLength(index) => length(c.values.value(entry(index))),
        Part::Value(index) => Bytes::Borrowed(c.values.value(entry(index)).as_bytes()),
        Part::Done => Bytes::Borrowed(&[]),
    }
}
fn next_part(header: WriteCommitRecordHeader, part: Part) -> Part {
    let fields = header.field_count();
    let properties = header.property_count();
    let first_property = || {
        if properties == 0 {
            Part::Done
        } else {
            Part::KeyLength(0)
        }
    };
    match (header.kind(), part) {
        (RootRowKind::RowCount, _) => Part::Done,
        (RootRowKind::DeleteVerification, Part::Header) => Part::DeleteVerification,
        (RootRowKind::DeleteVerification, _) => Part::Done,
        (RootRowKind::PreparedFragment, Part::Header) => Part::Fragment,
        (RootRowKind::PreparedFragment, _) => Part::Done,
        (RootRowKind::ArtifactDraft, Part::Header) => Part::Field(0),
        (RootRowKind::ArtifactDraft, Part::Field(index)) if index + 1 < fields => {
            Part::Field(index + 1)
        }
        (RootRowKind::ArtifactDraft, Part::Field(_)) => Part::Blob,
        (RootRowKind::ArtifactDraft, Part::Blob) => Part::Body,
        (RootRowKind::ArtifactDraft, Part::Body) => first_property(),
        (RootRowKind::ArtifactDraft, Part::KeyLength(index)) => Part::Key(index),
        (RootRowKind::ArtifactDraft, Part::Key(index)) => Part::ValueLength(index),
        (RootRowKind::ArtifactDraft, Part::ValueLength(index)) => Part::Value(index),
        (RootRowKind::ArtifactDraft, Part::Value(index)) if index + 1 < properties => {
            Part::KeyLength(index + 1)
        }
        (RootRowKind::ArtifactDraft, _) => Part::Done,
    }
}

fn advance(
    columns: &Columns<'_>,
    rows: usize,
    state: &mut CursorState,
    output: &mut [u8],
) -> Result<RenderTurn, WriteCommitCodecError> {
    let mut turn = RenderTurn {
        emitted_bytes: 0,
        examined_bytes: 0,
        visited_cells: 0,
        completed_rows: 0,
        status: RenderTurnStatus::Yielded,
    };
    let c = columns;
    loop {
        if matches!(state.phase, Phase::Complete) {
            turn.status = RenderTurnStatus::InputComplete;
            break;
        }
        if turn.visited_cells == WRITE_COMMIT_TURN_WORK
            || turn.examined_bytes == WRITE_COMMIT_TURN_BYTES
        {
            break;
        }
        match state.phase {
            Phase::Start => {
                // Twelve null flags, a kind, offsets and fixed verification values.
                if WRITE_COMMIT_TURN_WORK - turn.visited_cells < 20 {
                    break;
                }
                turn.visited_cells += 20;
                let next = start_row(c, state.row)?;
                if matches!(next, Phase::Emit { .. }) {
                    let header = header_for(c, state.row, 0)?;
                    state.pending = state.totals.checked_add(header)?;
                    state.header = Some(header);
                    state.header_bytes = header.bytes();
                    if header.kind() == RootRowKind::DeleteVerification {
                        state.verification_bytes = verification_payload(c, state.row)?;
                    }
                }
                state.phase = next;
            }
            Phase::Validate {
                field,
                property,
                property_bytes,
            } => {
                turn.visited_cells += 1;
                let (field_start, field_end) = span(c.fields.value_offsets(), state.row);
                let (entry_start, entry_end) = span(c.properties.value_offsets(), state.row);
                if field_start + field < field_end {
                    let index = field_start + field;
                    if c.ids.is_null(index) {
                        return Err(WriteCommitCodecError::NullValue);
                    }
                    if c.ids.value(index) <= 0 {
                        return Err(WriteCommitCodecError::FieldIds);
                    }
                    state.phase = Phase::Validate {
                        field: field + 1,
                        property,
                        property_bytes,
                    };
                } else if entry_start + property < entry_end {
                    let index = entry_start + property;
                    if c.keys.is_null(index) || c.values.is_null(index) {
                        return Err(WriteCommitCodecError::NullValue);
                    }
                    let key = c.keys.value_length(index) as usize;
                    if key == 0 {
                        return Err(WriteCommitCodecError::PropertyKey);
                    }
                    let property_bytes = property_bytes
                        .checked_add(key)
                        .and_then(|bytes| bytes.checked_add(c.values.value_length(index) as usize))
                        .filter(|bytes| *bytes <= MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES)
                        .ok_or(WriteCommitCodecError::PropertyLimit)?;
                    state.phase = Phase::Validate {
                        field,
                        property: property + 1,
                        property_bytes,
                    };
                } else {
                    let header = header_for(c, state.row, property_bytes)?;
                    state.pending = state.totals.checked_add(header)?;
                    state.header = Some(header);
                    state.header_bytes = header.bytes();
                    state.phase = Phase::Emit {
                        part: Part::Header,
                        offset: 0,
                    };
                }
            }
            Phase::Emit { part, offset } => {
                turn.visited_cells += 1;
                let header = state.header.expect("emission follows its header");
                if part == Part::Done {
                    state.totals = state.pending;
                    state.row += 1;
                    turn.completed_rows += 1;
                    state.header = None;
                    state.phase = if state.row == rows {
                        Phase::Complete
                    } else {
                        Phase::Start
                    };
                    continue;
                }
                let bytes = part_bytes(c, state, part);
                let source = match &bytes {
                    Bytes::Borrowed(slice) => *slice,
                    Bytes::Word(word) => word.as_slice(),
                };
                let remaining = &source[offset..];
                if remaining.is_empty() {
                    state.phase = Phase::Emit {
                        part: next_part(header, part),
                        offset: 0,
                    };
                    continue;
                }
                if turn.emitted_bytes == output.len() {
                    turn.status = RenderTurnStatus::NeedsOutput;
                    break;
                }
                let count = remaining
                    .len()
                    .min(output.len() - turn.emitted_bytes)
                    .min(WRITE_COMMIT_TURN_BYTES - turn.examined_bytes);
                output[turn.emitted_bytes..turn.emitted_bytes + count]
                    .copy_from_slice(&remaining[..count]);
                turn.emitted_bytes += count;
                turn.examined_bytes += count;
                state.phase = Phase::Emit {
                    part,
                    offset: offset + count,
                };
            }
            Phase::Complete | Phase::Failed => unreachable!("terminal phases are checked first"),
        }
    }
    Ok(turn)
}

impl novarocks_execution_contract::failure::FailureProjection for WriteCommitCodecError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let (class, code) = match self {
            Self::Schema => (FailureClass::Invariant, 200),
            Self::Kind => (FailureClass::Data, 201),
            Self::Shape => (FailureClass::Data, 202),
            Self::Target => (FailureClass::Data, 203),
            Self::RowCount => (FailureClass::Data, 204),
            Self::DuplicateRowCount => (FailureClass::Invariant, 205),
            Self::MissingRowCount => (FailureClass::Invariant, 206),
            Self::FragmentLimit => (FailureClass::Resource, 207),
            Self::ArtifactLimit => (FailureClass::Resource, 208),
            Self::FieldIds => (FailureClass::Data, 209),
            Self::BlobType => (FailureClass::Data, 210),
            Self::BodyLimit => (FailureClass::Resource, 211),
            Self::PropertyLimit => (FailureClass::Resource, 212),
            Self::PropertyKey => (FailureClass::Data, 213),
            Self::NullValue => (FailureClass::Data, 214),
            Self::TruncatedHeader => (FailureClass::Data, 215),
            Self::HeaderVersion => (FailureClass::Data, 216),
            Self::RecordLength => (FailureClass::Data, 217),
            Self::TruncatedRecord => (FailureClass::Data, 218),
            Self::TrailingRecordBytes => (FailureClass::Data, 219),
            Self::Failed => (FailureClass::Invariant, 220),
            Self::DuplicateDeleteVerification => (FailureClass::Invariant, 224),
            Self::DeleteVerificationCount => (FailureClass::Data, 225),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
