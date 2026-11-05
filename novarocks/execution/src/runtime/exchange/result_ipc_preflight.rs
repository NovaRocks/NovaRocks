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
use std::collections::HashSet;

use arrow::array::ArrayRef;
use novarocks_execution_contract::ResultByteLimit;

use super::{
    EXCHANGE_PAYLOAD_FLAG_SLOT_IDS, EXCHANGE_PAYLOAD_MAGIC, EXCHANGE_PAYLOAD_VERSION,
    EXCHANGE_ZERO_COLUMN_MARKER_FIELD, ExchangeFailure, failure,
};

#[cfg(test)]
pub(super) const ARROW_IPC_CONTINUATION_MARKER: [u8; 4] =
    novarocks_arrow_ipc_frame::CONTINUATION_MARKER;
const ARROW_IPC_ALIGNMENT: usize = 64;
const MAX_TYPED_ROOT_RESULT_METADATA_BYTES: usize = 4 * 1024 * 1024;
pub(super) const MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES: usize = 1024 * 1024;
pub(super) const MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS: usize = 4_096;
const MAX_TYPED_ROOT_RESULT_DICTIONARIES: usize = 1_024;
const MAX_TYPED_ROOT_RESULT_STRUCTURE_ENTRIES: usize = 65_536;

/// Validated logical byte bounds for decoding one typed root-result packet.
///
/// These bounds cover the Arrow IPC body allocations retained by the decoded
/// batch and a deterministic metadata/structure allowance used while decoding.
/// They are workload-control units, not a claim about allocator bookkeeping,
/// Arrow object heap, or total process RSS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypedRootResultDecodeBounds {
    decode_operation_upper_bound: u64,
    retained_backing_upper_bound: u64,
}

impl TypedRootResultDecodeBounds {
    /// Additional reservation required while raw input remains separately
    /// retained and the IPC decoder runs.
    pub const fn decode_operation_upper_bound(self) -> u64 {
        self.decode_operation_upper_bound
    }

    /// Maximum unique Arrow IPC body bytes retained by the decoded batch.
    pub const fn retained_backing_upper_bound(self) -> u64 {
        self.retained_backing_upper_bound
    }
}

#[derive(Default)]
struct TypedRootResultPreflightState {
    schema_seen: bool,
    record_batch_seen: bool,
    dictionary_ids: HashSet<i64>,
    schema_dictionary_ids: HashSet<i64>,
    metadata_bytes: usize,
    retained_backing_bytes: usize,
    schema_fields: usize,
    field_nodes: usize,
    buffers: usize,
    variadic_counts: usize,
    messages: usize,
}

/// Inspect one complete NRX1 typed root-result payload before Arrow allocates a
/// message body.
///
/// The accepted profile is exactly the stream emitted by this module's current
/// default `StreamWriter`: metadata V5, continuation framing, 64-byte body
/// alignment, no compression, one schema, zero or more replacement dictionary
/// batches, one record batch, and one terminal marker. The scan verifies and
/// skips body bytes without copying or interpreting them.
pub fn preflight_typed_root_result_decode(
    payload: &[u8],
    payload_limit: ResultByteLimit,
) -> Result<TypedRootResultDecodeBounds, ExchangeFailure> {
    if payload.is_empty() {
        return Err(failure::payload("typed root result payload is empty"));
    }
    let payload_limit = payload_limit.get();
    if payload.len() as u128 > payload_limit as u128 {
        return Err(extent(
            DecodeExtent::PayloadBytes,
            payload.len() as u128,
            payload_limit,
        ));
    }
    if !payload.starts_with(EXCHANGE_PAYLOAD_MAGIC) {
        return Err(failure::payload(
            "typed root result payload is missing NRX1 envelope",
        ));
    }

    let envelope_prefix = EXCHANGE_PAYLOAD_MAGIC.len();
    let version = *payload
        .get(envelope_prefix)
        .ok_or_else(|| failure::payload("typed root result payload is missing NRX1 version"))?;
    if version != EXCHANGE_PAYLOAD_VERSION {
        return Err(failure::payload(format_args!(
            "unsupported typed root result payload version: expected={} actual={version}",
            EXCHANGE_PAYLOAD_VERSION
        )));
    }
    let flags = *payload
        .get(envelope_prefix + 1)
        .ok_or_else(|| failure::payload("typed root result payload is missing NRX1 flags"))?;
    if flags != EXCHANGE_PAYLOAD_FLAG_SLOT_IDS {
        return Err(failure::payload(format_args!(
            "typed root result payload requires exactly slot-id flag 0x{EXCHANGE_PAYLOAD_FLAG_SLOT_IDS:02x}, got 0x{flags:02x}"
        )));
    }
    let slot_count_offset = envelope_prefix + 2;
    let slot_count_end = slot_count_offset
        .checked_add(std::mem::size_of::<u32>())
        .ok_or_else(|| failure::payload("typed root result slot-count offset overflow"))?;
    let slot_count_bytes: [u8; 4] = payload
        .get(slot_count_offset..slot_count_end)
        .ok_or_else(|| failure::payload("typed root result payload is missing NRX1 slot count"))?
        .try_into()
        .map_err(|source| failure::metadata("root envelope slot count width", source))?;
    let slot_count = usize::try_from(u32::from_le_bytes(slot_count_bytes))
        .map_err(|source| failure::metadata("root envelope slot count", source))?;
    if slot_count > MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS {
        return Err(extent(
            DecodeExtent::SchemaFields,
            slot_count as u128,
            MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS as u64,
        ));
    }
    let slot_bytes = slot_count
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| failure::payload("typed root result slot-id byte length overflow"))?;
    let arrow_offset = slot_count_end
        .checked_add(slot_bytes)
        .ok_or_else(|| failure::payload("typed root result Arrow IPC offset overflow"))?;
    let arrow_payload = payload
        .get(arrow_offset..)
        .ok_or_else(|| failure::payload("typed root result slot ids exceed payload length"))?;
    if arrow_payload.is_empty() {
        return Err(failure::payload(
            "typed root result payload is missing Arrow IPC stream",
        ));
    }

    let mut state = TypedRootResultPreflightState::default();
    let mut offset = 0usize;
    let mut terminal_seen = false;
    while offset < arrow_payload.len() {
        let prefix = novarocks_arrow_ipc_frame::continuation_prefix(arrow_payload, offset)
            .map_err(|source| failure::metadata("root IPC continuation prefix", source))?;
        let metadata_len = match prefix {
            novarocks_arrow_ipc_frame::ContinuationPrefix::End { next_offset } => {
                terminal_seen = true;
                if next_offset != arrow_payload.len() {
                    return Err(failure::payload(
                        "Arrow IPC terminal marker has trailing bytes",
                    ));
                }
                break;
            }
            novarocks_arrow_ipc_frame::ContinuationPrefix::Metadata { start, len } => {
                offset = start;
                len
            }
        };
        if metadata_len > MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES {
            return Err(extent(
                DecodeExtent::MessageMetadataBytes,
                metadata_len as u128,
                MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES as u64,
            ));
        }
        if metadata_len
            .checked_add(8)
            .is_none_or(|framed_len| framed_len % ARROW_IPC_ALIGNMENT != 0)
        {
            return Err(failure::payload(format_args!(
                "Arrow IPC framed metadata length {} is not {ARROW_IPC_ALIGNMENT}-byte aligned",
                metadata_len.saturating_add(8)
            )));
        }
        state.metadata_bytes = state
            .metadata_bytes
            .checked_add(metadata_len)
            .ok_or_else(|| failure::payload("Arrow IPC total metadata length overflow"))?;
        if state.metadata_bytes > MAX_TYPED_ROOT_RESULT_METADATA_BYTES {
            return Err(extent(
                DecodeExtent::TotalMetadataBytes,
                state.metadata_bytes as u128,
                MAX_TYPED_ROOT_RESULT_METADATA_BYTES as u64,
            ));
        }
        state.messages = state
            .messages
            .checked_add(1)
            .ok_or_else(|| failure::payload("Arrow IPC message count overflow"))?;

        let metadata =
            novarocks_arrow_ipc_frame::metadata_slice(arrow_payload, offset, metadata_len)
                .map_err(|source| failure::metadata("root IPC metadata range", source))?;
        let metadata_end = offset + metadata_len;
        // Preserve the old result profile's exact official verifier defaults.
        // Constant-pool callers supply their separately admitted options.
        let message = novarocks_arrow_ipc_frame::verified_message(
            metadata,
            &novarocks_arrow_ipc_frame::VerifierOptions::default(),
        )
        .map_err(|source| failure::metadata("invalid Arrow IPC message metadata", source))?;
        if message.version() != arrow::ipc::MetadataVersion::V5 {
            return Err(failure::payload(format_args!(
                "Arrow IPC message uses unsupported metadata version {:?}; expected V5",
                message.version()
            )));
        }
        if message.custom_metadata().is_some() {
            return Err(failure::payload(
                "Arrow IPC message-level custom metadata is not allowed",
            ));
        }
        let body_len = checked_nonnegative_ipc_i64(message.bodyLength(), "message body length")?;
        if body_len as u128 > payload_limit as u128 {
            return Err(extent(
                DecodeExtent::PayloadBytes,
                body_len as u128,
                payload_limit,
            ));
        }
        if body_len % ARROW_IPC_ALIGNMENT != 0 {
            return Err(failure::payload(format_args!(
                "Arrow IPC message body length {body_len} is not {ARROW_IPC_ALIGNMENT}-byte aligned"
            )));
        }
        let body_end =
            novarocks_arrow_ipc_frame::checked_range(arrow_payload.len(), metadata_end, body_len)
                .map_err(|source| failure::metadata("root IPC body range", source))?
                .end;

        match message.header_type() {
            arrow::ipc::MessageHeader::Schema => {
                if state.schema_seen || state.messages != 1 {
                    return Err(failure::payload(
                        "Arrow IPC schema must be the first and only schema message",
                    ));
                }
                if body_len != 0 {
                    return Err(failure::payload(
                        "Arrow IPC schema message must not have a body",
                    ));
                }
                let schema = message.header_as_schema().ok_or_else(|| {
                    failure::payload("Arrow IPC schema message is missing schema header")
                })?;
                validate_typed_root_result_schema(schema, slot_count, &mut state)?;
                state.schema_seen = true;
            }
            arrow::ipc::MessageHeader::DictionaryBatch => {
                if !state.schema_seen || state.record_batch_seen {
                    return Err(failure::payload(
                        "Arrow IPC dictionary message must follow schema and precede record batch",
                    ));
                }
                let dictionary = message.header_as_dictionary_batch().ok_or_else(|| {
                    failure::payload("Arrow IPC dictionary message is missing dictionary header")
                })?;
                if dictionary.isDelta() {
                    return Err(failure::payload(
                        "Arrow IPC delta dictionaries are not allowed",
                    ));
                }
                if !state.schema_dictionary_ids.contains(&dictionary.id()) {
                    return Err(failure::payload(format_args!(
                        "Arrow IPC dictionary id {} is not declared by schema",
                        dictionary.id()
                    )));
                }
                if state.dictionary_ids.contains(&dictionary.id()) {
                    return Err(failure::payload(format_args!(
                        "Arrow IPC dictionary id {} is repeated",
                        dictionary.id()
                    )));
                }
                state
                    .dictionary_ids
                    .try_reserve(1)
                    .map_err(failure::allocation)?;
                state.dictionary_ids.insert(dictionary.id());
                if state.dictionary_ids.len() > MAX_TYPED_ROOT_RESULT_DICTIONARIES {
                    return Err(extent(
                        DecodeExtent::Dictionaries,
                        state.dictionary_ids.len() as u128,
                        MAX_TYPED_ROOT_RESULT_DICTIONARIES as u64,
                    ));
                }
                let batch = dictionary.data().ok_or_else(|| {
                    failure::payload("Arrow IPC dictionary message is missing record-batch data")
                })?;
                validate_typed_root_result_record_batch(
                    batch,
                    body_len,
                    &mut state,
                    "dictionary",
                    None,
                )?;
                state.retained_backing_bytes = state
                    .retained_backing_bytes
                    .checked_add(body_len)
                    .ok_or_else(|| {
                    failure::payload("Arrow IPC retained dictionary body length overflow")
                })?;
            }
            arrow::ipc::MessageHeader::RecordBatch => {
                if !state.schema_seen || state.record_batch_seen {
                    return Err(failure::payload(
                        "Arrow IPC stream must contain exactly one record batch",
                    ));
                }
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    failure::payload(
                        "Arrow IPC record-batch message is missing record-batch header",
                    )
                })?;
                let expected_nodes = state.schema_fields;
                validate_typed_root_result_record_batch(
                    batch,
                    body_len,
                    &mut state,
                    "record batch",
                    Some(expected_nodes),
                )?;
                state.retained_backing_bytes = state
                    .retained_backing_bytes
                    .checked_add(body_len)
                    .ok_or_else(|| {
                    failure::payload("Arrow IPC retained record-batch body length overflow")
                })?;
                state.record_batch_seen = true;
            }
            other => {
                return Err(failure::payload(format_args!(
                    "Arrow IPC message type {other:?} is not allowed in typed root results"
                )));
            }
        }
        validate_typed_root_result_structure_limit(&state)?;
        offset = body_end;
    }

    if !terminal_seen {
        return Err(failure::payload(
            "Arrow IPC stream is missing terminal marker",
        ));
    }
    if !state.schema_seen || !state.record_batch_seen {
        return Err(failure::payload(
            "Arrow IPC stream requires one schema and one record batch",
        ));
    }

    let structure_cover = typed_root_result_structure_cover(&state)?;
    let decode_operation_bytes = state
        .retained_backing_bytes
        .checked_add(state.metadata_bytes)
        .and_then(|bytes| bytes.checked_add(structure_cover))
        .ok_or_else(|| failure::payload("typed root result decode-operation bound overflow"))?;
    Ok(TypedRootResultDecodeBounds {
        decode_operation_upper_bound: u64::try_from(decode_operation_bytes)
            .map_err(|source| failure::metadata("root decode operation extent", source))?,
        retained_backing_upper_bound: u64::try_from(state.retained_backing_bytes)
            .map_err(|source| failure::metadata("root retained backing extent", source))?,
    })
}

fn validate_typed_root_result_schema(
    schema: arrow::ipc::Schema<'_>,
    slot_count: usize,
    state: &mut TypedRootResultPreflightState,
) -> Result<(), ExchangeFailure> {
    if schema.endianness() != arrow::ipc::Endianness::Little {
        return Err(failure::payload(format_args!(
            "Arrow IPC schema uses unsupported endianness {:?}; expected Little",
            schema.endianness()
        )));
    }
    if schema.features().is_some() {
        return Err(failure::payload(
            "Arrow IPC schema feature declarations are not allowed",
        ));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| failure::payload("Arrow IPC schema is missing fields"))?;
    let top_level_fields = fields.len();
    let zero_column_marker = slot_count == 0
        && top_level_fields == 1
        && fields.get(0).name() == Some(EXCHANGE_ZERO_COLUMN_MARKER_FIELD);
    if top_level_fields != slot_count && !zero_column_marker {
        return Err(failure::payload(format_args!(
            "Arrow IPC schema has {top_level_fields} top-level fields but NRX1 declares {slot_count} slots"
        )));
    }

    if fields.len() > MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS {
        return Err(extent(
            DecodeExtent::SchemaFields,
            fields.len() as u128,
            MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS as u64,
        ));
    }
    let mut pending = Vec::new();
    pending
        .try_reserve_exact(fields.len())
        .map_err(failure::allocation)?;
    pending.extend(fields.iter());
    while let Some(field) = pending.pop() {
        state.schema_fields = state
            .schema_fields
            .checked_add(1)
            .ok_or_else(|| failure::payload("Arrow IPC schema field count overflow"))?;
        if state.schema_fields > MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS {
            return Err(extent(
                DecodeExtent::SchemaFields,
                state.schema_fields as u128,
                MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS as u64,
            ));
        }
        if let Some(dictionary) = field.dictionary() {
            if !state.schema_dictionary_ids.contains(&dictionary.id()) {
                state
                    .schema_dictionary_ids
                    .try_reserve(1)
                    .map_err(failure::allocation)?;
                state.schema_dictionary_ids.insert(dictionary.id());
            }
            if state.schema_dictionary_ids.len() > MAX_TYPED_ROOT_RESULT_DICTIONARIES {
                return Err(extent(
                    DecodeExtent::Dictionaries,
                    state.schema_dictionary_ids.len() as u128,
                    MAX_TYPED_ROOT_RESULT_DICTIONARIES as u64,
                ));
            }
        }
        if let Some(children) = field.children() {
            let next_len = pending
                .len()
                .checked_add(children.len())
                .ok_or_else(|| failure::payload("Arrow IPC nested schema worklist overflow"))?;
            if next_len > MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS {
                return Err(extent(
                    DecodeExtent::SchemaFields,
                    next_len as u128,
                    MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS as u64,
                ));
            }
            pending
                .try_reserve_exact(children.len())
                .map_err(failure::allocation)?;
            pending.extend(children.iter());
        }
    }
    Ok(())
}

fn validate_typed_root_result_record_batch(
    batch: arrow::ipc::RecordBatch<'_>,
    body_len: usize,
    state: &mut TypedRootResultPreflightState,
    label: &str,
    expected_nodes: Option<usize>,
) -> Result<(), ExchangeFailure> {
    checked_nonnegative_ipc_i64(batch.length(), format_args!("{label} row count"))?;
    if batch.compression().is_some() {
        return Err(failure::payload(format_args!(
            "Arrow IPC {label} compression is not allowed"
        )));
    }
    let nodes = batch.nodes().ok_or_else(|| {
        failure::payload(format_args!("Arrow IPC {label} is missing field nodes"))
    })?;
    let buffers = batch.buffers().ok_or_else(|| {
        failure::payload(format_args!(
            "Arrow IPC {label} is missing buffer descriptors"
        ))
    })?;
    if let Some(expected_nodes) = expected_nodes
        && nodes.len() != expected_nodes
    {
        return Err(failure::payload(format_args!(
            "Arrow IPC {label} has {} field nodes, expected {expected_nodes} from schema",
            nodes.len()
        )));
    }
    state.field_nodes = state
        .field_nodes
        .checked_add(nodes.len())
        .ok_or_else(|| failure::payload("Arrow IPC field-node count overflow"))?;
    state.buffers = state
        .buffers
        .checked_add(buffers.len())
        .ok_or_else(|| failure::payload("Arrow IPC buffer-descriptor count overflow"))?;
    if let Some(variadic_counts) = batch.variadicBufferCounts() {
        for count in variadic_counts.iter() {
            let count =
                checked_nonnegative_ipc_i64(count, format_args!("{label} variadic buffer count"))?;
            if count > buffers.len() {
                return Err(failure::payload(format_args!(
                    "Arrow IPC {label} variadic buffer count {count} exceeds total buffer descriptors {}",
                    buffers.len()
                )));
            }
        }
        state.variadic_counts = state
            .variadic_counts
            .checked_add(variadic_counts.len())
            .ok_or_else(|| failure::payload("Arrow IPC variadic-buffer count overflow"))?;
    }
    for node in nodes.iter() {
        let length =
            checked_nonnegative_ipc_i64(node.length(), format_args!("{label} node length"))?;
        let null_count = checked_nonnegative_ipc_i64(
            node.null_count(),
            format_args!("{label} node null count"),
        )?;
        if null_count > length {
            return Err(failure::payload(format_args!(
                "Arrow IPC {label} node null count {null_count} exceeds length {length}"
            )));
        }
    }

    let mut expected_offset = 0usize;
    for (index, buffer) in buffers.iter().enumerate() {
        let buffer_offset = checked_nonnegative_ipc_i64(
            buffer.offset(),
            format_args!("{label} buffer {index} offset"),
        )?;
        let buffer_len = checked_nonnegative_ipc_i64(
            buffer.length(),
            format_args!("{label} buffer {index} length"),
        )?;
        if buffer_offset != expected_offset {
            return Err(failure::payload(format_args!(
                "Arrow IPC {label} buffer {index} starts at {buffer_offset}, expected current writer offset {expected_offset}"
            )));
        }
        if buffer_offset % ARROW_IPC_ALIGNMENT != 0 {
            return Err(failure::payload(format_args!(
                "Arrow IPC {label} buffer {index} offset {buffer_offset} is not {ARROW_IPC_ALIGNMENT}-byte aligned"
            )));
        }
        let buffer_end = buffer_offset.checked_add(buffer_len).ok_or_else(|| {
            failure::payload(format_args!(
                "Arrow IPC {label} buffer {index} end offset overflow"
            ))
        })?;
        if buffer_end > body_len {
            return Err(failure::payload(format_args!(
                "Arrow IPC {label} buffer {index} ends at {buffer_end}, beyond body length {body_len}"
            )));
        }
        expected_offset = align_ipc_offset(buffer_end)?;
    }
    if expected_offset != body_len {
        return Err(failure::payload(format_args!(
            "Arrow IPC {label} buffers cover {expected_offset} aligned bytes but message body declares {body_len}"
        )));
    }
    Ok(())
}

fn validate_typed_root_result_structure_limit(
    state: &TypedRootResultPreflightState,
) -> Result<(), ExchangeFailure> {
    let entries = state
        .schema_fields
        .checked_add(state.field_nodes)
        .and_then(|count| count.checked_add(state.buffers))
        .and_then(|count| count.checked_add(state.variadic_counts))
        .and_then(|count| count.checked_add(state.messages))
        .ok_or_else(|| failure::payload("Arrow IPC structure-entry count overflow"))?;
    if entries > MAX_TYPED_ROOT_RESULT_STRUCTURE_ENTRIES {
        return Err(extent(
            DecodeExtent::StructureEntries,
            entries as u128,
            MAX_TYPED_ROOT_RESULT_STRUCTURE_ENTRIES as u64,
        ));
    }
    Ok(())
}

fn typed_root_result_structure_cover(
    state: &TypedRootResultPreflightState,
) -> Result<usize, ExchangeFailure> {
    let schema_cover = state
        .schema_fields
        .checked_mul(std::mem::size_of::<arrow::datatypes::Field>())
        .ok_or_else(|| failure::payload("Arrow IPC schema structure cover overflow"))?;
    let node_cover = state
        .field_nodes
        .checked_mul(std::mem::size_of::<arrow_data::ArrayData>())
        .ok_or_else(|| failure::payload("Arrow IPC array structure cover overflow"))?;
    let buffer_cover = state
        .buffers
        .checked_mul(std::mem::size_of::<arrow_buffer::Buffer>())
        .ok_or_else(|| failure::payload("Arrow IPC buffer structure cover overflow"))?;
    let variadic_cover = state
        .variadic_counts
        .checked_mul(std::mem::size_of::<i64>())
        .ok_or_else(|| failure::payload("Arrow IPC variadic structure cover overflow"))?;
    let dictionary_cover = state
        .dictionary_ids
        .len()
        .checked_mul(std::mem::size_of::<(i64, ArrayRef)>())
        .ok_or_else(|| failure::payload("Arrow IPC dictionary structure cover overflow"))?;
    schema_cover
        .checked_add(node_cover)
        .and_then(|bytes| bytes.checked_add(buffer_cover))
        .and_then(|bytes| bytes.checked_add(variadic_cover))
        .and_then(|bytes| bytes.checked_add(dictionary_cover))
        .ok_or_else(|| failure::payload("Arrow IPC total structure cover overflow"))
}

fn checked_nonnegative_ipc_i64(
    value: i64,
    label: impl std::fmt::Display,
) -> Result<usize, ExchangeFailure> {
    novarocks_arrow_ipc_frame::nonnegative_length(value).map_err(|source| {
        failure::metadata(
            format_args!("Arrow IPC {label} is negative or too large: {value}"),
            source,
        )
    })
}

fn align_ipc_offset(offset: usize) -> Result<usize, ExchangeFailure> {
    novarocks_arrow_ipc_frame::align_up(offset, ARROW_IPC_ALIGNMENT)
        .map_err(|source| failure::metadata("root IPC aligned offset", source))
}

// Decoder-profile dimensions, authored here rather than guessed by transport
// or Task recovery. A bound proves no deliverable leading row prefix.
#[derive(Clone, Copy, Debug)]
#[repr(u16)]
enum DecodeExtent {
    PayloadBytes = 115,
    SchemaFields = 116,
    MessageMetadataBytes = 117,
    TotalMetadataBytes = 118,
    Dictionaries = 119,
    StructureEntries = 120,
}
#[derive(Debug)]
struct DecodeExtentFailure {
    dimension: DecodeExtent,
    required: u128,
    limit: u64,
}
impl std::fmt::Display for DecodeExtentFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "root IPC {:?} extent {} exceeds hard limit {}",
            self.dimension, self.required, self.limit
        )
    }
}
impl std::error::Error for DecodeExtentFailure {}
impl novarocks_failure_contract::FailureProjection for DecodeExtentFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let code = std::num::NonZeroU16::new(self.dimension as u16).unwrap();
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::EXCHANGE, code),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::EXCHANGE,
                code,
            },
            required: u64::try_from(self.required).ok(),
            extent_limit: Some(self.limit),
            representable_prefix: None,
        })
    }
}
fn extent(dimension: DecodeExtent, required: u128, limit: u64) -> ExchangeFailure {
    ExchangeFailure::Source(crate::runtime::task::SharedDomainFailure::new(
        DecodeExtentFailure {
            dimension,
            required,
            limit,
        },
    ))
}
