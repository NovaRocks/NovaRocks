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
    EXCHANGE_ZERO_COLUMN_MARKER_FIELD,
};

pub(super) const ARROW_IPC_CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
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
) -> Result<TypedRootResultDecodeBounds, String> {
    if payload.is_empty() {
        return Err("typed root result payload is empty".to_string());
    }
    let payload_limit = usize::try_from(payload_limit.get())
        .map_err(|_| "typed root result payload limit does not fit usize".to_string())?;
    if payload.len() > payload_limit {
        return Err(format!(
            "typed root result payload has {} bytes, exceeding hard limit {}",
            payload.len(),
            payload_limit
        ));
    }
    if !payload.starts_with(EXCHANGE_PAYLOAD_MAGIC) {
        return Err("typed root result payload is missing NRX1 envelope".to_string());
    }

    let envelope_prefix = EXCHANGE_PAYLOAD_MAGIC.len();
    let version = *payload
        .get(envelope_prefix)
        .ok_or_else(|| "typed root result payload is missing NRX1 version".to_string())?;
    if version != EXCHANGE_PAYLOAD_VERSION {
        return Err(format!(
            "unsupported typed root result payload version: expected={} actual={version}",
            EXCHANGE_PAYLOAD_VERSION
        ));
    }
    let flags = *payload
        .get(envelope_prefix + 1)
        .ok_or_else(|| "typed root result payload is missing NRX1 flags".to_string())?;
    if flags != EXCHANGE_PAYLOAD_FLAG_SLOT_IDS {
        return Err(format!(
            "typed root result payload requires exactly slot-id flag 0x{EXCHANGE_PAYLOAD_FLAG_SLOT_IDS:02x}, got 0x{flags:02x}"
        ));
    }
    let slot_count_offset = envelope_prefix + 2;
    let slot_count_end = slot_count_offset
        .checked_add(std::mem::size_of::<u32>())
        .ok_or_else(|| "typed root result slot-count offset overflow".to_string())?;
    let slot_count_bytes: [u8; 4] = payload
        .get(slot_count_offset..slot_count_end)
        .ok_or_else(|| "typed root result payload is missing NRX1 slot count".to_string())?
        .try_into()
        .map_err(|_| "typed root result slot count has invalid width".to_string())?;
    let slot_count = usize::try_from(u32::from_le_bytes(slot_count_bytes))
        .map_err(|_| "typed root result slot count does not fit usize".to_string())?;
    if slot_count > MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS {
        return Err(format!(
            "typed root result slot count {slot_count} exceeds hard limit {MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS}"
        ));
    }
    let slot_bytes = slot_count
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| "typed root result slot-id byte length overflow".to_string())?;
    let arrow_offset = slot_count_end
        .checked_add(slot_bytes)
        .ok_or_else(|| "typed root result Arrow IPC offset overflow".to_string())?;
    let arrow_payload = payload
        .get(arrow_offset..)
        .ok_or_else(|| "typed root result slot ids exceed payload length".to_string())?;
    if arrow_payload.is_empty() {
        return Err("typed root result payload is missing Arrow IPC stream".to_string());
    }

    let mut state = TypedRootResultPreflightState::default();
    let mut offset = 0usize;
    let mut terminal_seen = false;
    while offset < arrow_payload.len() {
        let prefix_end = offset
            .checked_add(8)
            .ok_or_else(|| "Arrow IPC message prefix offset overflow".to_string())?;
        let prefix = arrow_payload.get(offset..prefix_end).ok_or_else(|| {
            format!("Arrow IPC message prefix is truncated at byte offset {offset}")
        })?;
        if prefix[..4] != ARROW_IPC_CONTINUATION_MARKER {
            return Err(format!(
                "Arrow IPC message at byte offset {offset} is not continuation-framed"
            ));
        }
        let metadata_len =
            usize::try_from(u32::from_le_bytes(prefix[4..8].try_into().map_err(
                |_| "Arrow IPC metadata length has invalid width".to_string(),
            )?))
            .map_err(|_| "Arrow IPC metadata length does not fit usize".to_string())?;
        offset = prefix_end;
        if metadata_len == 0 {
            terminal_seen = true;
            if offset != arrow_payload.len() {
                return Err("Arrow IPC terminal marker has trailing bytes".to_string());
            }
            break;
        }
        if metadata_len > MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES {
            return Err(format!(
                "Arrow IPC message metadata has {metadata_len} bytes, exceeding per-message hard limit {MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES}"
            ));
        }
        if metadata_len
            .checked_add(8)
            .is_none_or(|framed_len| framed_len % ARROW_IPC_ALIGNMENT != 0)
        {
            return Err(format!(
                "Arrow IPC framed metadata length {} is not {ARROW_IPC_ALIGNMENT}-byte aligned",
                metadata_len.saturating_add(8)
            ));
        }
        state.metadata_bytes = state
            .metadata_bytes
            .checked_add(metadata_len)
            .ok_or_else(|| "Arrow IPC total metadata length overflow".to_string())?;
        if state.metadata_bytes > MAX_TYPED_ROOT_RESULT_METADATA_BYTES {
            return Err(format!(
                "Arrow IPC total metadata has {} bytes, exceeding hard limit {MAX_TYPED_ROOT_RESULT_METADATA_BYTES}",
                state.metadata_bytes
            ));
        }
        state.messages = state
            .messages
            .checked_add(1)
            .ok_or_else(|| "Arrow IPC message count overflow".to_string())?;

        let metadata_end = offset
            .checked_add(metadata_len)
            .ok_or_else(|| "Arrow IPC metadata end offset overflow".to_string())?;
        let metadata = arrow_payload.get(offset..metadata_end).ok_or_else(|| {
            format!(
                "Arrow IPC message metadata at byte offset {offset} declares {metadata_len} bytes beyond payload"
            )
        })?;
        let message = arrow::ipc::root_as_message(metadata)
            .map_err(|error| format!("invalid Arrow IPC message metadata: {error:?}"))?;
        if message.version() != arrow::ipc::MetadataVersion::V5 {
            return Err(format!(
                "Arrow IPC message uses unsupported metadata version {:?}; expected V5",
                message.version()
            ));
        }
        if message.custom_metadata().is_some() {
            return Err("Arrow IPC message-level custom metadata is not allowed".to_string());
        }
        let body_len = checked_nonnegative_ipc_i64(message.bodyLength(), "message body length")?;
        if body_len > payload_limit {
            return Err(format!(
                "Arrow IPC message body has {body_len} bytes, exceeding hard limit {payload_limit}"
            ));
        }
        if body_len % ARROW_IPC_ALIGNMENT != 0 {
            return Err(format!(
                "Arrow IPC message body length {body_len} is not {ARROW_IPC_ALIGNMENT}-byte aligned"
            ));
        }
        let body_end = metadata_end
            .checked_add(body_len)
            .ok_or_else(|| "Arrow IPC message body end offset overflow".to_string())?;
        if body_end > arrow_payload.len() {
            return Err(format!(
                "Arrow IPC message body at byte offset {metadata_end} declares {body_len} bytes beyond payload"
            ));
        }

        match message.header_type() {
            arrow::ipc::MessageHeader::Schema => {
                if state.schema_seen || state.messages != 1 {
                    return Err(
                        "Arrow IPC schema must be the first and only schema message".to_string()
                    );
                }
                if body_len != 0 {
                    return Err("Arrow IPC schema message must not have a body".to_string());
                }
                let schema = message.header_as_schema().ok_or_else(|| {
                    "Arrow IPC schema message is missing schema header".to_string()
                })?;
                validate_typed_root_result_schema(schema, slot_count, &mut state)?;
                state.schema_seen = true;
            }
            arrow::ipc::MessageHeader::DictionaryBatch => {
                if !state.schema_seen || state.record_batch_seen {
                    return Err(
                        "Arrow IPC dictionary message must follow schema and precede record batch"
                            .to_string(),
                    );
                }
                let dictionary = message.header_as_dictionary_batch().ok_or_else(|| {
                    "Arrow IPC dictionary message is missing dictionary header".to_string()
                })?;
                if dictionary.isDelta() {
                    return Err("Arrow IPC delta dictionaries are not allowed".to_string());
                }
                if !state.schema_dictionary_ids.contains(&dictionary.id()) {
                    return Err(format!(
                        "Arrow IPC dictionary id {} is not declared by schema",
                        dictionary.id()
                    ));
                }
                if !state.dictionary_ids.insert(dictionary.id()) {
                    return Err(format!(
                        "Arrow IPC dictionary id {} is repeated",
                        dictionary.id()
                    ));
                }
                if state.dictionary_ids.len() > MAX_TYPED_ROOT_RESULT_DICTIONARIES {
                    return Err(format!(
                        "Arrow IPC dictionary count exceeds hard limit {MAX_TYPED_ROOT_RESULT_DICTIONARIES}"
                    ));
                }
                let batch = dictionary.data().ok_or_else(|| {
                    "Arrow IPC dictionary message is missing record-batch data".to_string()
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
                    "Arrow IPC retained dictionary body length overflow".to_string()
                })?;
            }
            arrow::ipc::MessageHeader::RecordBatch => {
                if !state.schema_seen || state.record_batch_seen {
                    return Err(
                        "Arrow IPC stream must contain exactly one record batch".to_string()
                    );
                }
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    "Arrow IPC record-batch message is missing record-batch header".to_string()
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
                    "Arrow IPC retained record-batch body length overflow".to_string()
                })?;
                state.record_batch_seen = true;
            }
            other => {
                return Err(format!(
                    "Arrow IPC message type {other:?} is not allowed in typed root results"
                ));
            }
        }
        validate_typed_root_result_structure_limit(&state)?;
        offset = body_end;
    }

    if !terminal_seen {
        return Err("Arrow IPC stream is missing terminal marker".to_string());
    }
    if !state.schema_seen || !state.record_batch_seen {
        return Err("Arrow IPC stream requires one schema and one record batch".to_string());
    }

    let structure_cover = typed_root_result_structure_cover(&state)?;
    let decode_operation_bytes = state
        .retained_backing_bytes
        .checked_add(state.metadata_bytes)
        .and_then(|bytes| bytes.checked_add(structure_cover))
        .ok_or_else(|| "typed root result decode-operation bound overflow".to_string())?;
    Ok(TypedRootResultDecodeBounds {
        decode_operation_upper_bound: u64::try_from(decode_operation_bytes)
            .map_err(|_| "typed root result decode-operation bound does not fit u64".to_string())?,
        retained_backing_upper_bound: u64::try_from(state.retained_backing_bytes)
            .map_err(|_| "typed root result retained-backing bound does not fit u64".to_string())?,
    })
}

fn validate_typed_root_result_schema(
    schema: arrow::ipc::Schema<'_>,
    slot_count: usize,
    state: &mut TypedRootResultPreflightState,
) -> Result<(), String> {
    if schema.endianness() != arrow::ipc::Endianness::Little {
        return Err(format!(
            "Arrow IPC schema uses unsupported endianness {:?}; expected Little",
            schema.endianness()
        ));
    }
    if schema.features().is_some() {
        return Err("Arrow IPC schema feature declarations are not allowed".to_string());
    }
    let fields = schema
        .fields()
        .ok_or_else(|| "Arrow IPC schema is missing fields".to_string())?;
    let top_level_fields = fields.len();
    let zero_column_marker = slot_count == 0
        && top_level_fields == 1
        && fields.get(0).name() == Some(EXCHANGE_ZERO_COLUMN_MARKER_FIELD);
    if top_level_fields != slot_count && !zero_column_marker {
        return Err(format!(
            "Arrow IPC schema has {top_level_fields} top-level fields but NRX1 declares {slot_count} slots"
        ));
    }

    let mut pending = fields.iter().collect::<Vec<_>>();
    while let Some(field) = pending.pop() {
        state.schema_fields = state
            .schema_fields
            .checked_add(1)
            .ok_or_else(|| "Arrow IPC schema field count overflow".to_string())?;
        if state.schema_fields > MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS {
            return Err(format!(
                "Arrow IPC schema field count exceeds hard limit {MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS}"
            ));
        }
        if let Some(dictionary) = field.dictionary() {
            state.schema_dictionary_ids.insert(dictionary.id());
            if state.schema_dictionary_ids.len() > MAX_TYPED_ROOT_RESULT_DICTIONARIES {
                return Err(format!(
                    "Arrow IPC schema dictionary count exceeds hard limit {MAX_TYPED_ROOT_RESULT_DICTIONARIES}"
                ));
            }
        }
        if let Some(children) = field.children() {
            let next_len = pending
                .len()
                .checked_add(children.len())
                .ok_or_else(|| "Arrow IPC nested schema worklist overflow".to_string())?;
            if next_len > MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS {
                return Err(format!(
                    "Arrow IPC nested schema worklist exceeds hard limit {MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS}"
                ));
            }
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
) -> Result<(), String> {
    checked_nonnegative_ipc_i64(batch.length(), &format!("{label} row count"))?;
    if batch.compression().is_some() {
        return Err(format!("Arrow IPC {label} compression is not allowed"));
    }
    let nodes = batch
        .nodes()
        .ok_or_else(|| format!("Arrow IPC {label} is missing field nodes"))?;
    let buffers = batch
        .buffers()
        .ok_or_else(|| format!("Arrow IPC {label} is missing buffer descriptors"))?;
    if let Some(expected_nodes) = expected_nodes
        && nodes.len() != expected_nodes
    {
        return Err(format!(
            "Arrow IPC {label} has {} field nodes, expected {expected_nodes} from schema",
            nodes.len()
        ));
    }
    state.field_nodes = state
        .field_nodes
        .checked_add(nodes.len())
        .ok_or_else(|| "Arrow IPC field-node count overflow".to_string())?;
    state.buffers = state
        .buffers
        .checked_add(buffers.len())
        .ok_or_else(|| "Arrow IPC buffer-descriptor count overflow".to_string())?;
    if let Some(variadic_counts) = batch.variadicBufferCounts() {
        for count in variadic_counts.iter() {
            let count =
                checked_nonnegative_ipc_i64(count, &format!("{label} variadic buffer count"))?;
            if count > buffers.len() {
                return Err(format!(
                    "Arrow IPC {label} variadic buffer count {count} exceeds total buffer descriptors {}",
                    buffers.len()
                ));
            }
        }
        state.variadic_counts = state
            .variadic_counts
            .checked_add(variadic_counts.len())
            .ok_or_else(|| "Arrow IPC variadic-buffer count overflow".to_string())?;
    }
    for node in nodes.iter() {
        let length = checked_nonnegative_ipc_i64(node.length(), &format!("{label} node length"))?;
        let null_count =
            checked_nonnegative_ipc_i64(node.null_count(), &format!("{label} node null count"))?;
        if null_count > length {
            return Err(format!(
                "Arrow IPC {label} node null count {null_count} exceeds length {length}"
            ));
        }
    }

    let mut expected_offset = 0usize;
    for (index, buffer) in buffers.iter().enumerate() {
        let buffer_offset = checked_nonnegative_ipc_i64(
            buffer.offset(),
            &format!("{label} buffer {index} offset"),
        )?;
        let buffer_len = checked_nonnegative_ipc_i64(
            buffer.length(),
            &format!("{label} buffer {index} length"),
        )?;
        if buffer_offset != expected_offset {
            return Err(format!(
                "Arrow IPC {label} buffer {index} starts at {buffer_offset}, expected current writer offset {expected_offset}"
            ));
        }
        if buffer_offset % ARROW_IPC_ALIGNMENT != 0 {
            return Err(format!(
                "Arrow IPC {label} buffer {index} offset {buffer_offset} is not {ARROW_IPC_ALIGNMENT}-byte aligned"
            ));
        }
        let buffer_end = buffer_offset
            .checked_add(buffer_len)
            .ok_or_else(|| format!("Arrow IPC {label} buffer {index} end offset overflow"))?;
        if buffer_end > body_len {
            return Err(format!(
                "Arrow IPC {label} buffer {index} ends at {buffer_end}, beyond body length {body_len}"
            ));
        }
        expected_offset = align_ipc_offset(buffer_end)?;
    }
    if expected_offset != body_len {
        return Err(format!(
            "Arrow IPC {label} buffers cover {expected_offset} aligned bytes but message body declares {body_len}"
        ));
    }
    Ok(())
}

fn validate_typed_root_result_structure_limit(
    state: &TypedRootResultPreflightState,
) -> Result<(), String> {
    let entries = state
        .schema_fields
        .checked_add(state.field_nodes)
        .and_then(|count| count.checked_add(state.buffers))
        .and_then(|count| count.checked_add(state.variadic_counts))
        .and_then(|count| count.checked_add(state.messages))
        .ok_or_else(|| "Arrow IPC structure-entry count overflow".to_string())?;
    if entries > MAX_TYPED_ROOT_RESULT_STRUCTURE_ENTRIES {
        return Err(format!(
            "Arrow IPC structure-entry count {entries} exceeds hard limit {MAX_TYPED_ROOT_RESULT_STRUCTURE_ENTRIES}"
        ));
    }
    Ok(())
}

fn typed_root_result_structure_cover(
    state: &TypedRootResultPreflightState,
) -> Result<usize, String> {
    let schema_cover = state
        .schema_fields
        .checked_mul(std::mem::size_of::<arrow::datatypes::Field>())
        .ok_or_else(|| "Arrow IPC schema structure cover overflow".to_string())?;
    let node_cover = state
        .field_nodes
        .checked_mul(std::mem::size_of::<arrow_data::ArrayData>())
        .ok_or_else(|| "Arrow IPC array structure cover overflow".to_string())?;
    let buffer_cover = state
        .buffers
        .checked_mul(std::mem::size_of::<arrow_buffer::Buffer>())
        .ok_or_else(|| "Arrow IPC buffer structure cover overflow".to_string())?;
    let variadic_cover = state
        .variadic_counts
        .checked_mul(std::mem::size_of::<i64>())
        .ok_or_else(|| "Arrow IPC variadic structure cover overflow".to_string())?;
    let dictionary_cover = state
        .dictionary_ids
        .len()
        .checked_mul(std::mem::size_of::<(i64, ArrayRef)>())
        .ok_or_else(|| "Arrow IPC dictionary structure cover overflow".to_string())?;
    schema_cover
        .checked_add(node_cover)
        .and_then(|bytes| bytes.checked_add(buffer_cover))
        .and_then(|bytes| bytes.checked_add(variadic_cover))
        .and_then(|bytes| bytes.checked_add(dictionary_cover))
        .ok_or_else(|| "Arrow IPC total structure cover overflow".to_string())
}

fn checked_nonnegative_ipc_i64(value: i64, label: &str) -> Result<usize, String> {
    usize::try_from(value)
        .map_err(|_| format!("Arrow IPC {label} is negative or too large: {value}"))
}

fn align_ipc_offset(offset: usize) -> Result<usize, String> {
    let alignment_mask = ARROW_IPC_ALIGNMENT - 1;
    offset
        .checked_add(alignment_mask)
        .map(|value| value & !alignment_mask)
        .ok_or_else(|| "Arrow IPC aligned offset overflow".to_string())
}
