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

//! Strict decoding for Iceberg-private writer recipes and commit artifacts.
//!
//! The outer connector codec has already proved provider, catalog generation,
//! category, revision, and payload integrity before these functions run. This
//! module owns the remaining trust boundary: private protobuf structure,
//! bounded allocation, and the field relationships that can be checked before
//! the write domain's `try_new` constructors apply Iceberg semantics.

use std::collections::BTreeSet;
use std::mem::size_of;

use novarocks_spi::connector::write_stack::{
    MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_WRITER_HANDLE_BYTES,
};
use novarocks_spi::connector::{
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorDecodeContext, ConnectorDecodeLedger,
    ConnectorFieldPath,
};
use prost::Message;

use super::dto;

const MAX_PATH_BYTES: usize = 16 * 1024;
const MAX_NAME_BYTES: usize = 1024;
const MAX_SCHEMA_JSON_BYTES: usize = 8 * 1024 * 1024;
const MAX_PARTITION_VALUES: usize = 4096;
const MAX_PARTITION_VALUE_BYTES: usize = 64 * 1024;
const MAX_COLUMN_STAT_ENTRIES: usize = 4096;
const MAX_COLUMN_STAT_BOUND_BYTES: usize = 64 * 1024;
const MAX_SPLIT_OFFSETS: usize = 4096;
const MAX_PARTITION_COLUMNS: usize = 4096;
const MAX_TRANSFORM_EXPRS: usize = 4096;
const MAX_TRANSFORM_EXPR_BYTES: usize = 64 * 1024;
const MAX_OLD_DELETE_MERGE_TARGETS: usize = 16_384;
const MAX_OLD_DELETE_REFERENCES: usize = 1024;
const MAX_MERGED_OLD_REFERENCES: usize = 1024;
const MAX_EQUALITY_DELETE_COLUMNS: usize = 4096;

pub(crate) const WRITE_CODEC_REVISION: u32 = 1;

pub(crate) fn decode_writer_handle(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<dto::IcebergWriterHandle, ConnectorCodecError> {
    let path = ConnectorFieldPath::root("writer_handle").field("iceberg");
    if payload.len() > MAX_CONNECTOR_WRITER_HANDLE_BYTES {
        return Err(capacity(
            path,
            "Iceberg writer handle exceeds its hard byte limit",
        ));
    }
    context.ledger().charge_raw(payload.len())?;
    scan_message(
        payload,
        MessageKind::WriterHandle,
        &path,
        1,
        context.ledger(),
    )?;
    let value = dto::IcebergWriterHandle::decode(payload)
        .map_err(|error| invalid(path.clone(), format!("malformed protobuf: {error}")))?;
    validate_writer_handle(&value, &path)?;
    let container_bytes = context
        .ledger()
        .items()
        .saturating_mul(size_of::<usize>().saturating_mul(2));
    context.ledger().charge_retained(
        value
            .encoded_len()
            .saturating_add(size_of::<dto::IcebergWriterHandle>())
            .saturating_add(container_bytes),
    )?;
    Ok(value)
}

pub(crate) fn decode_commit_fragment(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<dto::IcebergCommitFragment, ConnectorCodecError> {
    let path = ConnectorFieldPath::root("commit_fragment").field("iceberg");
    if payload.len() > MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES {
        return Err(capacity(
            path,
            "Iceberg commit fragment exceeds its hard byte limit",
        ));
    }
    context.ledger().charge_raw(payload.len())?;
    scan_message(
        payload,
        MessageKind::CommitFragment,
        &path,
        1,
        context.ledger(),
    )?;
    let value = dto::IcebergCommitFragment::decode(payload)
        .map_err(|error| invalid(path.clone(), format!("malformed protobuf: {error}")))?;
    validate_commit_fragment(&value, &path)?;
    let container_bytes = context
        .ledger()
        .items()
        .saturating_mul(size_of::<usize>().saturating_mul(2));
    context.ledger().charge_retained(
        value
            .encoded_len()
            .saturating_add(size_of::<dto::IcebergCommitFragment>())
            .saturating_add(container_bytes),
    )?;
    Ok(value)
}

#[derive(Clone, Copy)]
enum MessageKind {
    WriterHandle,
    WriteTable,
    WriterOutput,
    DataRecipe,
    OldDeleteTarget,
    OldDeleteRef,
    StorageRoute,
    EqualityRecipe,
    EqualityColumn,
    Partition,
    PartitionDescriptor,
    PartitionValue,
    ContentRange,
    Metrics,
    ColumnStats,
    CommitFragment,
    DataFile,
    PositionDeleteFile,
    DeletionVector,
    EqualityDeleteFile,
}

#[derive(Clone, Copy)]
enum FieldKind {
    Varint,
    Text,
    Bytes,
    Message(MessageKind),
    PackedVarint,
    MapStringMessage(MessageKind),
    MapI32Varint,
    MapI32Bytes,
}

#[derive(Clone, Copy)]
struct FieldRule {
    kind: FieldKind,
    repeated: bool,
    exclusive_group: u8,
}

impl FieldRule {
    const fn singular(kind: FieldKind) -> Self {
        Self {
            kind,
            repeated: false,
            exclusive_group: 0,
        }
    }

    const fn repeated(kind: FieldKind) -> Self {
        Self {
            kind,
            repeated: true,
            exclusive_group: 0,
        }
    }

    const fn oneof(kind: FieldKind) -> Self {
        Self {
            kind,
            repeated: false,
            exclusive_group: 1,
        }
    }

    const fn wire(self) -> u8 {
        match self.kind {
            FieldKind::Varint => 0,
            FieldKind::Text
            | FieldKind::Bytes
            | FieldKind::Message(_)
            | FieldKind::PackedVarint
            | FieldKind::MapStringMessage(_)
            | FieldKind::MapI32Varint
            | FieldKind::MapI32Bytes => 2,
        }
    }
}

fn field_rule(message: MessageKind, field: u32) -> Option<FieldRule> {
    use FieldKind as F;
    use MessageKind as M;
    let singular = FieldRule::singular;
    let repeated = FieldRule::repeated;
    match (message, field) {
        (M::WriterHandle, 1) => Some(singular(F::Varint)),
        (M::WriterHandle, 2) => Some(singular(F::Message(M::WriteTable))),
        (M::WriterHandle, 3) => Some(singular(F::Message(M::WriterOutput))),
        (M::WriterHandle, 4) => Some(singular(F::Message(M::DataRecipe))),
        (M::WriterHandle, 5) => Some(repeated(F::MapStringMessage(M::OldDeleteTarget))),
        (M::WriterHandle, 6) => Some(singular(F::Message(M::EqualityRecipe))),

        (M::WriteTable, 1..=6) => Some(singular(F::Text)),
        (M::WriteTable, 7..=11) => Some(singular(F::Varint)),
        (M::WriterOutput, 1..=3) => Some(singular(F::Varint)),
        (M::DataRecipe, 1) => Some(singular(F::Text)),
        (M::DataRecipe, 2..=4) => Some(repeated(F::Text)),
        (M::DataRecipe, 5) => Some(singular(F::Varint)),

        (M::OldDeleteTarget, 1) => Some(singular(F::Text)),
        (M::OldDeleteTarget, 2..=3) | (M::OldDeleteTarget, 5) => Some(singular(F::Varint)),
        (M::OldDeleteTarget, 4) => Some(singular(F::Message(M::Partition))),
        (M::OldDeleteTarget, 6) => Some(repeated(F::Message(M::OldDeleteRef))),
        (M::OldDeleteRef, 1) | (M::OldDeleteRef, 7) => Some(singular(F::Text)),
        (M::OldDeleteRef, 2..=5) | (M::OldDeleteRef, 8..=10) => Some(singular(F::Varint)),
        (M::OldDeleteRef, 6) => Some(singular(F::Message(M::ContentRange))),
        (M::OldDeleteRef, 11) => Some(singular(F::Message(M::StorageRoute))),
        (M::StorageRoute, 1) => Some(singular(F::Text)),

        (M::EqualityRecipe, 1) => Some(repeated(F::Message(M::EqualityColumn))),
        (M::EqualityColumn, 1) | (M::EqualityColumn, 3) => Some(singular(F::Text)),
        (M::EqualityColumn, 2) | (M::EqualityColumn, 4) => Some(singular(F::Varint)),

        (M::Partition, 1..=2) => Some(singular(F::Text)),
        (M::Partition, 3) => Some(singular(F::Varint)),
        (M::Partition, 4) => Some(singular(F::Message(M::PartitionDescriptor))),
        (M::PartitionDescriptor, 1) => Some(repeated(F::Message(M::PartitionValue))),
        (M::PartitionValue, 1) => Some(singular(F::Varint)),
        (M::PartitionValue, 2) => Some(singular(F::Bytes)),
        (M::ContentRange, 1..=2) => Some(singular(F::Varint)),
        (M::Metrics, 1..=2) => Some(singular(F::Varint)),
        (M::Metrics, 3) => Some(repeated(F::PackedVarint)),
        (M::Metrics, 4) => Some(singular(F::Message(M::ColumnStats))),
        (M::ColumnStats, 1..=4) => Some(repeated(F::MapI32Varint)),
        (M::ColumnStats, 5..=6) => Some(repeated(F::MapI32Bytes)),

        (M::CommitFragment, 1) => Some(FieldRule::oneof(F::Message(M::DataFile))),
        (M::CommitFragment, 2) => Some(FieldRule::oneof(F::Message(M::PositionDeleteFile))),
        (M::CommitFragment, 3) => Some(FieldRule::oneof(F::Message(M::DeletionVector))),
        (M::CommitFragment, 4) => Some(FieldRule::oneof(F::Message(M::EqualityDeleteFile))),
        (M::DataFile, 1) => Some(singular(F::Text)),
        (M::DataFile, 2) => Some(singular(F::Varint)),
        (M::DataFile, 3) => Some(singular(F::Message(M::Partition))),
        (M::DataFile, 4) => Some(singular(F::Message(M::Metrics))),
        (M::DataFile, 5) => Some(singular(F::Varint)),
        (M::PositionDeleteFile, 1) | (M::PositionDeleteFile, 4) => Some(singular(F::Text)),
        (M::PositionDeleteFile, 2) => Some(singular(F::Message(M::Partition))),
        (M::PositionDeleteFile, 3) => Some(singular(F::Message(M::Metrics))),
        (M::PositionDeleteFile, 5) => Some(repeated(F::Text)),
        (M::DeletionVector, 1) | (M::DeletionVector, 4) => Some(singular(F::Text)),
        (M::DeletionVector, 2) => Some(singular(F::Message(M::Partition))),
        (M::DeletionVector, 3) => Some(singular(F::Message(M::Metrics))),
        (M::DeletionVector, 5) => Some(singular(F::Message(M::ContentRange))),
        (M::DeletionVector, 6) => Some(singular(F::Varint)),
        (M::DeletionVector, 7) => Some(repeated(F::Text)),
        (M::EqualityDeleteFile, 1) => Some(singular(F::Text)),
        (M::EqualityDeleteFile, 2) => Some(singular(F::Message(M::Partition))),
        (M::EqualityDeleteFile, 3) => Some(singular(F::Message(M::Metrics))),
        (M::EqualityDeleteFile, 4) => Some(repeated(F::PackedVarint)),
        _ => None,
    }
}

fn scan_message(
    mut input: &[u8],
    message: MessageKind,
    path: &ConnectorFieldPath,
    depth: usize,
    ledger: &mut ConnectorDecodeLedger,
) -> Result<(), ConnectorCodecError> {
    ledger.check_depth(depth)?;
    let mut seen = BTreeSet::new();
    let mut oneofs = BTreeSet::new();
    let mut map_keys: BTreeSet<(u32, Vec<u8>)> = BTreeSet::new();
    while !input.is_empty() {
        let key = read_varint(&mut input, path)?;
        let field = u32::try_from(key >> 3)
            .map_err(|_| invalid(path.clone(), "protobuf field number is out of range"))?;
        if field == 0 {
            return Err(invalid(path.clone(), "protobuf field zero is invalid"));
        }
        let wire = (key & 7) as u8;
        let rule = field_rule(message, field).ok_or_else(|| {
            error(
                path.field(format!("field_{field}")),
                ConnectorCodecErrorKind::UnknownField,
                "unknown Iceberg private write field",
            )
        })?;
        let field_path = path.field(format!("field_{field}"));
        if wire != rule.wire() {
            return Err(invalid(
                field_path,
                "Iceberg private write field has the wrong protobuf wire type",
            ));
        }
        if !rule.repeated && !seen.insert(field) {
            return Err(error(
                field_path,
                ConnectorCodecErrorKind::DuplicateField,
                "Iceberg private singular field appears more than once",
            ));
        }
        if rule.exclusive_group != 0 && !oneofs.insert(rule.exclusive_group) {
            return Err(error(
                field_path,
                ConnectorCodecErrorKind::DuplicateField,
                "Iceberg private oneof contains more than one value",
            ));
        }
        ledger.charge_items(1)?;
        match rule.kind {
            FieldKind::Varint => {
                read_varint(&mut input, &field_path)?;
            }
            FieldKind::Text | FieldKind::Bytes | FieldKind::PackedVarint => {
                let value = read_length_delimited(&mut input, &field_path)?;
                ledger.charge_scalar(value.len())?;
                if matches!(rule.kind, FieldKind::PackedVarint) {
                    let mut packed = value;
                    while !packed.is_empty() {
                        read_varint(&mut packed, &field_path)?;
                        ledger.charge_items(1)?;
                    }
                }
            }
            FieldKind::Message(nested) => {
                let value = read_length_delimited(&mut input, &field_path)?;
                scan_message(value, nested, &field_path, depth + 1, ledger)?;
            }
            FieldKind::MapStringMessage(nested) => {
                let value = read_length_delimited(&mut input, &field_path)?;
                let map_key = scan_map_entry(
                    value,
                    MapValue::Message(nested),
                    &field_path,
                    depth + 1,
                    ledger,
                )?;
                if !map_keys.insert((field, map_key)) {
                    return Err(error(
                        field_path,
                        ConnectorCodecErrorKind::DuplicateField,
                        "Iceberg private map contains a duplicate key",
                    ));
                }
            }
            FieldKind::MapI32Varint => {
                let value = read_length_delimited(&mut input, &field_path)?;
                let map_key =
                    scan_map_entry(value, MapValue::Varint, &field_path, depth + 1, ledger)?;
                if !map_keys.insert((field, map_key)) {
                    return Err(error(
                        field_path,
                        ConnectorCodecErrorKind::DuplicateField,
                        "Iceberg private map contains a duplicate key",
                    ));
                }
            }
            FieldKind::MapI32Bytes => {
                let value = read_length_delimited(&mut input, &field_path)?;
                let map_key =
                    scan_map_entry(value, MapValue::Bytes, &field_path, depth + 1, ledger)?;
                if !map_keys.insert((field, map_key)) {
                    return Err(error(
                        field_path,
                        ConnectorCodecErrorKind::DuplicateField,
                        "Iceberg private map contains a duplicate key",
                    ));
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum MapValue {
    Message(MessageKind),
    Varint,
    Bytes,
}

fn scan_map_entry(
    mut input: &[u8],
    value_kind: MapValue,
    path: &ConnectorFieldPath,
    depth: usize,
    ledger: &mut ConnectorDecodeLedger,
) -> Result<Vec<u8>, ConnectorCodecError> {
    ledger.check_depth(depth)?;
    let mut key = None;
    let mut value_seen = false;
    while !input.is_empty() {
        let raw_key = read_varint(&mut input, path)?;
        let field = (raw_key >> 3) as u32;
        let wire = (raw_key & 7) as u8;
        ledger.charge_items(1)?;
        match field {
            1 if key.is_none() => match value_kind {
                MapValue::Message(_) => {
                    if wire != 2 {
                        return Err(invalid(
                            path.field("key"),
                            "map key has the wrong wire type",
                        ));
                    }
                    let value = read_length_delimited(&mut input, &path.field("key"))?;
                    ledger.charge_scalar(value.len())?;
                    key = Some(value.to_vec());
                }
                MapValue::Varint | MapValue::Bytes => {
                    if wire != 0 {
                        return Err(invalid(
                            path.field("key"),
                            "map key has the wrong wire type",
                        ));
                    }
                    key = Some(
                        read_varint(&mut input, &path.field("key"))?
                            .to_le_bytes()
                            .to_vec(),
                    );
                }
            },
            1 => {
                return Err(error(
                    path.field("key"),
                    ConnectorCodecErrorKind::DuplicateField,
                    "map key appears more than once",
                ));
            }
            2 if !value_seen => {
                value_seen = true;
                match value_kind {
                    MapValue::Message(nested) => {
                        if wire != 2 {
                            return Err(invalid(
                                path.field("value"),
                                "map value has the wrong wire type",
                            ));
                        }
                        let value = read_length_delimited(&mut input, &path.field("value"))?;
                        scan_message(value, nested, &path.field("value"), depth + 1, ledger)?;
                    }
                    MapValue::Varint => {
                        if wire != 0 {
                            return Err(invalid(
                                path.field("value"),
                                "map value has the wrong wire type",
                            ));
                        }
                        read_varint(&mut input, &path.field("value"))?;
                    }
                    MapValue::Bytes => {
                        if wire != 2 {
                            return Err(invalid(
                                path.field("value"),
                                "map value has the wrong wire type",
                            ));
                        }
                        let value = read_length_delimited(&mut input, &path.field("value"))?;
                        ledger.charge_scalar(value.len())?;
                    }
                }
            }
            2 => {
                return Err(error(
                    path.field("value"),
                    ConnectorCodecErrorKind::DuplicateField,
                    "map value appears more than once",
                ));
            }
            _ => {
                return Err(error(
                    path.field(format!("field_{field}")),
                    ConnectorCodecErrorKind::UnknownField,
                    "unknown Iceberg private map-entry field",
                ));
            }
        }
    }
    // Proto3 map entries apply the scalar/message default when either entry
    // field is absent. Prost therefore omits a legitimate zero count (and can
    // omit an empty key/value) from canonical bytes. Provider validation below
    // still rejects defaults where the Iceberg relationship requires a
    // non-empty path or a fully populated nested value.
    Ok(key.unwrap_or_else(|| match value_kind {
        MapValue::Message(_) => Vec::new(),
        MapValue::Varint | MapValue::Bytes => 0_u64.to_le_bytes().to_vec(),
    }))
}

fn read_length_delimited<'a>(
    input: &mut &'a [u8],
    path: &ConnectorFieldPath,
) -> Result<&'a [u8], ConnectorCodecError> {
    let length = usize::try_from(read_varint(input, path)?)
        .map_err(|_| invalid(path.clone(), "protobuf length is out of range"))?;
    if input.len() < length {
        return Err(invalid(path.clone(), "truncated length-delimited field"));
    }
    let (value, rest) = input.split_at(length);
    *input = rest;
    Ok(value)
}

fn read_varint(input: &mut &[u8], path: &ConnectorFieldPath) -> Result<u64, ConnectorCodecError> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = input
            .split_first()
            .ok_or_else(|| invalid(path.clone(), "truncated protobuf varint"))?;
        *input = rest;
        if shift == 63 && byte > 1 {
            return Err(invalid(path.clone(), "protobuf varint overflow"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid(path.clone(), "protobuf varint overflow"))
}

fn validate_writer_handle(
    handle: &dto::IcebergWriterHandle,
    path: &ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    let branch = named_branch(handle.branch, path.field("branch"))?;
    validate_table(handle.table.as_ref(), path.field("table"))?;
    validate_output(handle.output.as_ref(), path.field("output"))?;
    match branch {
        dto::IcebergWriteBranch::Data => {
            let recipe = handle
                .data
                .as_ref()
                .ok_or_else(|| inconsistent(path.field("data"), "data branch requires a recipe"))?;
            validate_data_recipe(recipe, path.field("data"))?;
            if !handle.old_deletes.is_empty() || handle.equality.is_some() {
                return Err(inconsistent(
                    path.clone(),
                    "data branch cannot carry delete recipes",
                ));
            }
        }
        dto::IcebergWriteBranch::PositionDelete | dto::IcebergWriteBranch::DeletionVector => {
            if handle.data.is_some() || handle.equality.is_some() {
                return Err(inconsistent(
                    path.clone(),
                    "position-delete branch carries only old-delete targets",
                ));
            }
            bounded_count(
                handle.old_deletes.len(),
                MAX_OLD_DELETE_MERGE_TARGETS,
                path.field("old_deletes"),
            )?;
            for (key, target) in &handle.old_deletes {
                validate_old_delete_target(target, key, path.field("old_deletes").map_key(key))?;
            }
        }
        dto::IcebergWriteBranch::EqualityDelete => {
            if handle.data.is_some() || !handle.old_deletes.is_empty() {
                return Err(inconsistent(
                    path.clone(),
                    "equality-delete branch cannot carry data or old-delete recipes",
                ));
            }
            validate_equality_recipe(
                handle.equality.as_ref().ok_or_else(|| {
                    inconsistent(
                        path.field("equality"),
                        "equality-delete branch requires a recipe",
                    )
                })?,
                path.field("equality"),
            )?;
        }
        dto::IcebergWriteBranch::Unspecified => unreachable!(),
    }
    Ok(())
}

fn validate_table(
    table: Option<&dto::IcebergWriteTableFacts>,
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    let table = table.ok_or_else(|| missing(path.clone(), "writer handle requires table facts"))?;
    for (field, value, max) in [
        ("table_uuid", table.table_uuid.as_str(), MAX_NAME_BYTES),
        ("namespace", table.namespace.as_str(), MAX_NAME_BYTES),
        ("table_name", table.table_name.as_str(), MAX_NAME_BYTES),
        (
            "table_location",
            table.table_location.as_str(),
            MAX_PATH_BYTES,
        ),
        (
            "data_location",
            table.data_location.as_str(),
            MAX_PATH_BYTES,
        ),
        ("target_ref", table.target_ref.as_str(), MAX_NAME_BYTES),
    ] {
        bounded_text(value, max, path.field(field), false)?;
    }
    nonnegative(
        table.base_sequence_number,
        path.field("base_sequence_number"),
    )?;
    nonnegative(i64::from(table.schema_id), path.field("schema_id"))?;
    nonnegative(
        i64::from(table.default_partition_spec_id),
        path.field("default_partition_spec_id"),
    )?;
    if !(1..=3).contains(&table.format_version) {
        return Err(invalid(
            path.field("format_version"),
            "Iceberg format version must be in 1..=3",
        ));
    }
    Ok(())
}

fn validate_output(
    output: Option<&dto::IcebergWriterOutput>,
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    let output =
        output.ok_or_else(|| missing(path.clone(), "writer handle requires output facts"))?;
    named_file_format(output.file_format, path.field("file_format"))?;
    match dto::IcebergCompression::try_from(output.compression) {
        Ok(dto::IcebergCompression::Unspecified) | Err(_) => {
            return Err(invalid_enum(
                path.field("compression"),
                "compression must be named",
            ));
        }
        _ => {}
    }
    if output.parquet_row_group_size_bytes == Some(0) {
        return Err(invalid(
            path.field("parquet_row_group_size_bytes"),
            "Parquet row group size must be positive",
        ));
    }
    Ok(())
}

fn validate_data_recipe(
    recipe: &dto::IcebergDataBranchRecipe,
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    if let Some(schema) = &recipe.input_schema_json {
        bounded_text(
            schema,
            MAX_SCHEMA_JSON_BYTES,
            path.field("input_schema_json"),
            false,
        )?;
    }
    bounded_count(
        recipe.partition_source_column_names.len(),
        MAX_PARTITION_COLUMNS,
        path.field("partition_source_column_names"),
    )?;
    bounded_count(
        recipe.partition_column_names.len(),
        MAX_PARTITION_COLUMNS,
        path.field("partition_column_names"),
    )?;
    bounded_count(
        recipe.transform_exprs.len(),
        MAX_TRANSFORM_EXPRS,
        path.field("transform_exprs"),
    )?;
    if recipe.partition_source_column_names.len() != recipe.partition_column_names.len()
        || recipe.partition_column_names.len() != recipe.transform_exprs.len()
    {
        return Err(inconsistent(
            path.clone(),
            "partition sources, names, and transforms must be parallel",
        ));
    }
    for (index, value) in recipe
        .partition_source_column_names
        .iter()
        .chain(&recipe.partition_column_names)
        .enumerate()
    {
        bounded_text(
            value,
            MAX_NAME_BYTES,
            path.field("partition_columns").index(index),
            false,
        )?;
    }
    for (index, value) in recipe.transform_exprs.iter().enumerate() {
        bounded_text(
            value,
            MAX_TRANSFORM_EXPR_BYTES,
            path.field("transform_exprs").index(index),
            false,
        )?;
    }
    Ok(())
}

fn validate_equality_recipe(
    recipe: &dto::IcebergEqualityDeleteRecipe,
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    bounded_count(
        recipe.columns.len(),
        MAX_EQUALITY_DELETE_COLUMNS,
        path.field("columns"),
    )?;
    if recipe.columns.is_empty() {
        return Err(inconsistent(
            path.field("columns"),
            "equality key cannot be empty",
        ));
    }
    let mut ids = BTreeSet::new();
    for (index, column) in recipe.columns.iter().enumerate() {
        let column_path = path.field("columns").index(index);
        bounded_text(
            &column.name,
            MAX_NAME_BYTES,
            column_path.field("name"),
            false,
        )?;
        bounded_text(
            &column.data_type,
            MAX_NAME_BYTES,
            column_path.field("data_type"),
            false,
        )?;
        nonnegative(i64::from(column.field_id), column_path.field("field_id"))?;
        if !ids.insert(column.field_id) {
            return Err(inconsistent(
                column_path.field("field_id"),
                "equality field ids must be unique",
            ));
        }
    }
    Ok(())
}

fn validate_old_delete_target(
    target: &dto::IcebergOldDeleteMergeTarget,
    key: &str,
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    bounded_text(
        &target.data_file_path,
        MAX_PATH_BYTES,
        path.field("data_file_path"),
        false,
    )?;
    if target.data_file_path != key {
        return Err(inconsistent(
            path.field("data_file_path"),
            "old-delete target must be keyed by its data path",
        ));
    }
    nonnegative(target.base_snapshot_id, path.field("base_snapshot_id"))?;
    validate_partition(target.partition.as_ref(), path.field("partition"))?;
    bounded_count(
        target.references.len(),
        MAX_OLD_DELETE_REFERENCES,
        path.field("references"),
    )?;
    let mut previous = None;
    for (index, reference) in target.references.iter().enumerate() {
        validate_old_delete_ref(reference, path.field("references").index(index))?;
        if previous.is_some_and(|value: &str| value >= reference.path.as_str()) {
            return Err(inconsistent(
                path.field("references").index(index).field("path"),
                "old-delete references must be sorted and unique",
            ));
        }
        previous = Some(reference.path.as_str());
    }
    Ok(())
}

fn validate_old_delete_ref(
    reference: &dto::IcebergOldDeleteArtifactRef,
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    bounded_text(&reference.path, MAX_PATH_BYTES, path.field("path"), false)?;
    let content = named_file_content(reference.content, path.field("content"))?;
    if content != dto::IcebergFileContent::PositionDeletes {
        return Err(inconsistent(
            path.field("content"),
            "old-delete reference must contain position deletes",
        ));
    }
    let format = named_file_format(reference.file_format, path.field("file_format"))?;
    if reference.file_size_in_bytes == 0 {
        return Err(invalid(
            path.field("file_size_in_bytes"),
            "old-delete file cannot be empty",
        ));
    }
    nonnegative(
        i64::from(reference.partition_spec_id),
        path.field("partition_spec_id"),
    )?;
    if let Some(range) = &reference.content_range {
        validate_content_range(range, path.field("content_range"))?;
    }
    match format {
        dto::IcebergWriteFileFormat::Puffin => {
            if reference.content_range.is_none() || reference.referenced_data_file.is_none() {
                return Err(inconsistent(
                    path.clone(),
                    "Puffin deletion vector requires range and referenced data file",
                ));
            }
        }
        dto::IcebergWriteFileFormat::Parquet if reference.content_range.is_some() => {
            return Err(inconsistent(
                path.field("content_range"),
                "Parquet delete file cannot carry a content range",
            ));
        }
        _ => {}
    }
    if let Some(data_file) = &reference.referenced_data_file {
        bounded_text(
            data_file,
            MAX_PATH_BYTES,
            path.field("referenced_data_file"),
            false,
        )?;
    }
    let route = reference.storage_route.as_ref().ok_or_else(|| {
        missing(
            path.field("storage_route"),
            "old-delete reference requires a route",
        )
    })?;
    bounded_text(
        &route.access_binding,
        MAX_NAME_BYTES,
        path.field("storage_route").field("access_binding"),
        false,
    )
}

fn validate_commit_fragment(
    fragment: &dto::IcebergCommitFragment,
    path: &ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    let artifact = fragment
        .artifact
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "commit fragment requires one artifact"))?;
    match artifact {
        dto::iceberg_commit_fragment::Artifact::DataFile(file) => {
            bounded_text(
                &file.path,
                MAX_PATH_BYTES,
                path.field("data_file").field("path"),
                false,
            )?;
            named_file_format(
                file.file_format,
                path.field("data_file").field("file_format"),
            )?;
            validate_partition(
                file.partition.as_ref(),
                path.field("data_file").field("partition"),
            )?;
            validate_metrics(
                file.metrics.as_ref(),
                path.field("data_file").field("metrics"),
            )?;
            if file.first_row_id.is_some_and(|value| value < 0) {
                return Err(invalid(
                    path.field("data_file").field("first_row_id"),
                    "first row id must be nonnegative",
                ));
            }
        }
        dto::iceberg_commit_fragment::Artifact::PositionDeleteFile(file) => {
            let base = path.field("position_delete_file");
            validate_delete_artifact_common(
                &file.path,
                file.partition.as_ref(),
                file.metrics.as_ref(),
                &file.referenced_data_file,
                &file.merged_old_references,
                base,
            )?;
        }
        dto::iceberg_commit_fragment::Artifact::DeletionVector(file) => {
            let base = path.field("deletion_vector");
            validate_delete_artifact_common(
                &file.path,
                file.partition.as_ref(),
                file.metrics.as_ref(),
                &file.referenced_data_file,
                &file.merged_old_references,
                base.clone(),
            )?;
            validate_content_range(
                file.content_range.as_ref().ok_or_else(|| {
                    missing(
                        base.field("content_range"),
                        "deletion vector requires a range",
                    )
                })?,
                base.field("content_range"),
            )?;
        }
        dto::iceberg_commit_fragment::Artifact::EqualityDeleteFile(file) => {
            let base = path.field("equality_delete_file");
            bounded_text(&file.path, MAX_PATH_BYTES, base.field("path"), false)?;
            validate_partition(file.partition.as_ref(), base.field("partition"))?;
            validate_metrics(file.metrics.as_ref(), base.field("metrics"))?;
            bounded_count(
                file.equality_field_ids.len(),
                MAX_EQUALITY_DELETE_COLUMNS,
                base.field("equality_field_ids"),
            )?;
            if file.equality_field_ids.is_empty()
                || file.equality_field_ids.iter().any(|value| *value < 0)
                || file
                    .equality_field_ids
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
            {
                return Err(inconsistent(
                    base.field("equality_field_ids"),
                    "equality field ids must be nonempty, sorted, unique, and nonnegative",
                ));
            }
        }
    }
    Ok(())
}

fn validate_delete_artifact_common(
    path_value: &str,
    partition: Option<&dto::IcebergArtifactPartition>,
    metrics: Option<&dto::IcebergArtifactMetrics>,
    referenced_data_file: &str,
    merged: &[String],
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    bounded_text(path_value, MAX_PATH_BYTES, path.field("path"), false)?;
    bounded_text(
        referenced_data_file,
        MAX_PATH_BYTES,
        path.field("referenced_data_file"),
        false,
    )?;
    validate_partition(partition, path.field("partition"))?;
    validate_metrics(metrics, path.field("metrics"))?;
    bounded_count(
        merged.len(),
        MAX_MERGED_OLD_REFERENCES,
        path.field("merged_old_references"),
    )?;
    let mut previous = None;
    for (index, value) in merged.iter().enumerate() {
        bounded_text(
            value,
            MAX_PATH_BYTES,
            path.field("merged_old_references").index(index),
            false,
        )?;
        if previous.is_some_and(|previous: &str| previous >= value.as_str()) {
            return Err(inconsistent(
                path.field("merged_old_references").index(index),
                "merged old references must be sorted and unique",
            ));
        }
        previous = Some(value);
    }
    Ok(())
}

fn validate_partition(
    partition: Option<&dto::IcebergArtifactPartition>,
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    let partition =
        partition.ok_or_else(|| missing(path.clone(), "artifact requires partition"))?;
    bounded_text(
        &partition.partition_path,
        MAX_PATH_BYTES,
        path.field("partition_path"),
        true,
    )?;
    bounded_text(
        &partition.null_fingerprint,
        MAX_NAME_BYTES,
        path.field("null_fingerprint"),
        true,
    )?;
    nonnegative(
        i64::from(partition.partition_spec_id),
        path.field("partition_spec_id"),
    )?;
    let descriptor = partition
        .descriptor
        .as_ref()
        .ok_or_else(|| missing(path.field("descriptor"), "partition requires descriptor"))?;
    bounded_count(
        descriptor.values.len(),
        MAX_PARTITION_VALUES,
        path.field("descriptor").field("values"),
    )?;
    for (index, value) in descriptor.values.iter().enumerate() {
        let value_path = path.field("descriptor").field("values").index(index);
        match (value.is_null, value.datum_bytes.as_ref()) {
            (true, Some(_)) | (false, None) => {
                return Err(inconsistent(
                    value_path,
                    "partition null marker and datum presence disagree",
                ));
            }
            (false, Some(bytes)) if bytes.len() > MAX_PARTITION_VALUE_BYTES => {
                return Err(capacity(
                    value_path,
                    "partition value exceeds its byte limit",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_metrics(
    metrics: Option<&dto::IcebergArtifactMetrics>,
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    let metrics = metrics.ok_or_else(|| missing(path.clone(), "artifact requires metrics"))?;
    bounded_count(
        metrics.split_offsets.len(),
        MAX_SPLIT_OFFSETS,
        path.field("split_offsets"),
    )?;
    if metrics.split_offsets.iter().any(|value| *value < 0) {
        return Err(invalid(
            path.field("split_offsets"),
            "split offset must be nonnegative",
        ));
    }
    let Some(stats) = &metrics.column_stats else {
        return Ok(());
    };
    for (name, values) in [
        ("column_sizes", &stats.column_sizes),
        ("value_counts", &stats.value_counts),
        ("null_value_counts", &stats.null_value_counts),
        ("nan_value_counts", &stats.nan_value_counts),
    ] {
        bounded_count(
            values.len(),
            MAX_COLUMN_STAT_ENTRIES,
            path.field("column_stats").field(name),
        )?;
        if values.values().any(|value| *value < 0) {
            return Err(invalid(
                path.field("column_stats").field(name),
                "column statistic count must be nonnegative",
            ));
        }
    }
    for (name, values) in [
        ("lower_bounds", &stats.lower_bounds),
        ("upper_bounds", &stats.upper_bounds),
    ] {
        bounded_count(
            values.len(),
            MAX_COLUMN_STAT_ENTRIES,
            path.field("column_stats").field(name),
        )?;
        if values
            .values()
            .any(|value| value.len() > MAX_COLUMN_STAT_BOUND_BYTES)
        {
            return Err(capacity(
                path.field("column_stats").field(name),
                "column statistic bound exceeds its byte limit",
            ));
        }
    }
    Ok(())
}

fn validate_content_range(
    range: &dto::IcebergContentRange,
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    nonnegative(range.offset, path.field("offset"))?;
    if range.size_in_bytes <= 0 {
        return Err(invalid(
            path.field("size_in_bytes"),
            "content range size must be positive",
        ));
    }
    Ok(())
}

fn named_branch(
    value: i32,
    path: ConnectorFieldPath,
) -> Result<dto::IcebergWriteBranch, ConnectorCodecError> {
    match dto::IcebergWriteBranch::try_from(value) {
        Ok(dto::IcebergWriteBranch::Unspecified) | Err(_) => {
            Err(invalid_enum(path, "write branch must be named"))
        }
        Ok(value) => Ok(value),
    }
}

fn named_file_format(
    value: i32,
    path: ConnectorFieldPath,
) -> Result<dto::IcebergWriteFileFormat, ConnectorCodecError> {
    match dto::IcebergWriteFileFormat::try_from(value) {
        Ok(dto::IcebergWriteFileFormat::Unspecified) | Err(_) => {
            Err(invalid_enum(path, "file format must be named"))
        }
        Ok(value) => Ok(value),
    }
}

fn named_file_content(
    value: i32,
    path: ConnectorFieldPath,
) -> Result<dto::IcebergFileContent, ConnectorCodecError> {
    match dto::IcebergFileContent::try_from(value) {
        Ok(dto::IcebergFileContent::Unspecified) | Err(_) => {
            Err(invalid_enum(path, "file content must be named"))
        }
        Ok(value) => Ok(value),
    }
}

fn bounded_text(
    value: &str,
    maximum: usize,
    path: ConnectorFieldPath,
    allow_empty: bool,
) -> Result<(), ConnectorCodecError> {
    if !allow_empty && value.is_empty() {
        return Err(invalid(path, "text value must not be empty"));
    }
    if value.len() > maximum {
        return Err(capacity(path, "text value exceeds its byte limit"));
    }
    Ok(())
}

fn bounded_count(
    actual: usize,
    maximum: usize,
    path: ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    if actual > maximum {
        return Err(capacity(path, "item count exceeds its hard limit"));
    }
    Ok(())
}

fn nonnegative(value: i64, path: ConnectorFieldPath) -> Result<(), ConnectorCodecError> {
    if value < 0 {
        return Err(invalid(path, "value must be nonnegative"));
    }
    Ok(())
}

fn error(
    path: ConnectorFieldPath,
    kind: ConnectorCodecErrorKind,
    detail: impl AsRef<str>,
) -> ConnectorCodecError {
    ConnectorCodecError::new(path, kind, detail)
}

fn missing(path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
    error(path, ConnectorCodecErrorKind::MissingField, detail)
}

fn invalid(path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
    error(path, ConnectorCodecErrorKind::InvalidValue, detail)
}

fn invalid_enum(path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
    error(path, ConnectorCodecErrorKind::InvalidEnum, detail)
}

fn inconsistent(path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
    error(path, ConnectorCodecErrorKind::InconsistentFields, detail)
}

fn capacity(path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
    error(path, ConnectorCodecErrorKind::Capacity, detail)
}
