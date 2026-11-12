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
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorDecodeContext, ConnectorFieldPath,
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
const MAX_MERGED_OLD_REFERENCES: usize = 1024;
const MAX_EQUALITY_DELETE_COLUMNS: usize = 4096;

pub(crate) use crate::contract_revision::ICEBERG_CONTRACT_REVISION as WRITE_CODEC_REVISION;

pub(crate) fn decode_writer_handle(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<dto::IcebergWriterHandle, ConnectorCodecError> {
    let result = (|| {
        let path = ConnectorFieldPath::root("writer_handle").field("iceberg");
        if payload.len() > MAX_CONNECTOR_WRITER_HANDLE_BYTES {
            return Err(capacity(
                path,
                "Iceberg writer handle exceeds its hard byte limit",
            ));
        }
        context.ledger().charge_raw(payload.len())?;
        scan_message(payload, MessageKind::WriterHandle, &path, 1, context)?;
        let value = observe_opaque(context, || dto::IcebergWriterHandle::decode(payload))?
            .map_err(|error| invalid(path.clone(), format!("malformed protobuf: {error}")))?;
        validate_writer_handle(&value, &path, context)?;
        let container_bytes = context
            .ledger()
            .items()
            .saturating_mul(size_of::<usize>().saturating_mul(2));
        let encoded_len = observe_opaque(context, || value.encoded_len())?;
        context.ledger().charge_retained(
            encoded_len
                .saturating_add(size_of::<dto::IcebergWriterHandle>())
                .saturating_add(container_bytes),
        )?;
        Ok(value)
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

/// A fragment admitted by the existing private scanner and wire validator.
///
/// Only this module can construct the value. Shared access preserves the exact
/// validated fields through domain projection; this is not a capacity receipt.
#[derive(Debug)]
pub(crate) struct ValidatedCommitFragmentWire(dto::IcebergCommitFragment);

impl ValidatedCommitFragmentWire {
    pub(crate) fn as_dto(&self) -> &dto::IcebergCommitFragment {
        &self.0
    }
}

pub(crate) fn decode_commit_fragment(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<ValidatedCommitFragmentWire, ConnectorCodecError> {
    let result = (|| {
        let path = ConnectorFieldPath::root("commit_fragment").field("iceberg");
        if payload.len() > MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES {
            return Err(capacity(
                path,
                "Iceberg commit fragment exceeds its hard byte limit",
            ));
        }
        context.ledger().charge_raw(payload.len())?;
        scan_message(payload, MessageKind::CommitFragment, &path, 1, context)?;
        let value = observe_opaque(context, || dto::IcebergCommitFragment::decode(payload))?
            .map_err(|error| invalid(path.clone(), format!("malformed protobuf: {error}")))?;
        validate_commit_fragment(&value, &path, context)?;
        let container_bytes = context
            .ledger()
            .items()
            .saturating_mul(size_of::<usize>().saturating_mul(2));
        let encoded_len = observe_opaque(context, || value.encoded_len())?;
        context.ledger().charge_retained(
            encoded_len
                .saturating_add(size_of::<dto::IcebergCommitFragment>())
                .saturating_add(container_bytes),
        )?;
        Ok(ValidatedCommitFragmentWire(value))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

#[derive(Clone, Copy)]
enum MessageKind {
    WriterHandle,
    MutationReadRecipe,
    MutationSnapshot,
    CreationScope,
    WriteTable,
    WriterOutput,
    DataRecipe,
    PositionRewriteTarget,
    EqualityRecipe,
    EqualityColumn,
    Partition,
    PartitionDescriptor,
    PartitionValue,
    ContentRange,
    Metrics,
    ColumnStats,
    CommitFragment,
    CreationReceipt,
    CreationWriter,
    FinalDeleteEffect,
    DeleteTargetLocator,
    RemoveDataFile,
    DataFile,
    PositionDeleteFile,
    DeletionVector,
    EqualityDeleteFile,
    MergedDeleteReference,
    DeletionVectorReference,
}

#[derive(Clone, Copy)]
enum FieldKind {
    Varint,
    Text,
    Bytes,
    Message(MessageKind),
    PackedVarint,
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
        (M::WriterHandle, 6) => Some(singular(F::Message(M::EqualityRecipe))),
        (M::WriterHandle, 7) => Some(singular(F::Message(M::MutationReadRecipe))),
        (M::WriterHandle, 8) => Some(singular(F::Message(M::CreationScope))),
        (M::WriterHandle, 9) => Some(singular(F::Bytes)),
        (M::WriterHandle, 10) => Some(singular(F::Message(M::PositionRewriteTarget))),
        (M::PositionRewriteTarget, 1) => Some(singular(F::Text)),
        (M::PositionRewriteTarget, 2) => Some(singular(F::Varint)),
        (M::PositionRewriteTarget, 3) => Some(singular(F::Message(M::Partition))),
        (M::CreationScope, 1..=3) => Some(singular(F::Bytes)),
        (M::MutationReadRecipe, 1 | 2) => Some(singular(F::Text)),
        (M::MutationReadRecipe, 3) => Some(singular(F::Message(M::MutationSnapshot))),
        (M::MutationSnapshot, 1) => Some(singular(F::Text)),
        (M::MutationSnapshot, 2) => Some(singular(F::Varint)),

        (M::WriteTable, 1..=6) => Some(singular(F::Text)),
        (M::WriteTable, 7..=11) => Some(singular(F::Varint)),
        (M::WriterOutput, 1..=3) => Some(singular(F::Varint)),
        (M::DataRecipe, 1) => Some(singular(F::Text)),
        (M::DataRecipe, 2..=4) => Some(repeated(F::Text)),
        (M::DataRecipe, 5) => Some(singular(F::Varint)),

        (M::EqualityRecipe, 1) => Some(repeated(F::Message(M::EqualityColumn))),
        (M::EqualityRecipe, 2) => Some(singular(F::Text)),
        (M::EqualityColumn, 1) => Some(singular(F::Text)),
        (M::EqualityColumn, 2) => Some(singular(F::Varint)),

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
        (M::CommitFragment, 5) => Some(FieldRule::oneof(F::Message(M::FinalDeleteEffect))),
        (M::CommitFragment, 6) => Some(singular(F::Message(M::CreationReceipt))),
        (M::CreationReceipt, 1 | 2 | 4) => Some(singular(F::Bytes)),
        (M::CreationReceipt, 3) => Some(singular(F::Varint)),
        (M::CreationReceipt, 5) => Some(singular(F::Message(M::CreationWriter))),
        (M::CreationWriter, 1 | 3) => Some(singular(F::Bytes)),
        (M::CreationWriter, 2 | 4 | 5) => Some(singular(F::Varint)),
        (M::FinalDeleteEffect, 1) => Some(singular(F::Message(M::DeleteTargetLocator))),
        (M::FinalDeleteEffect, 2) => Some(singular(F::Varint)),
        (M::FinalDeleteEffect, 3) => Some(FieldRule::oneof(F::Message(M::RemoveDataFile))),
        (M::FinalDeleteEffect, 4) => Some(FieldRule::oneof(F::Message(M::DeletionVector))),
        (M::DeleteTargetLocator, 1 | 2 | 6) => Some(singular(F::Text)),
        (M::DeleteTargetLocator, 3..=5 | 7) => Some(singular(F::Varint)),
        (M::RemoveDataFile, 1) => Some(repeated(F::Message(M::MergedDeleteReference))),
        (M::RemoveDataFile, 2) => Some(singular(F::Message(M::Partition))),
        (M::DataFile, 1) => Some(singular(F::Text)),
        (M::DataFile, 2) => Some(singular(F::Varint)),
        (M::DataFile, 3) => Some(singular(F::Message(M::Partition))),
        (M::DataFile, 4) => Some(singular(F::Message(M::Metrics))),
        (M::DataFile, 5) => Some(singular(F::Varint)),
        (M::PositionDeleteFile, 1) | (M::PositionDeleteFile, 4) => Some(singular(F::Text)),
        (M::PositionDeleteFile, 2) => Some(singular(F::Message(M::Partition))),
        (M::PositionDeleteFile, 3) => Some(singular(F::Message(M::Metrics))),
        (M::PositionDeleteFile, 5) => Some(repeated(F::Message(M::MergedDeleteReference))),
        (M::DeletionVector, 1) | (M::DeletionVector, 4) => Some(singular(F::Text)),
        (M::DeletionVector, 2) => Some(singular(F::Message(M::Partition))),
        (M::DeletionVector, 3) => Some(singular(F::Message(M::Metrics))),
        (M::DeletionVector, 5) => Some(singular(F::Message(M::ContentRange))),
        (M::DeletionVector, 6) => Some(singular(F::Varint)),
        (M::DeletionVector, 7) => Some(repeated(F::Message(M::MergedDeleteReference))),
        (M::MergedDeleteReference, 1) => Some(FieldRule::oneof(F::Text)),
        (M::MergedDeleteReference, 2) => {
            Some(FieldRule::oneof(F::Message(M::DeletionVectorReference)))
        }
        (M::DeletionVectorReference, 1 | 4) => Some(singular(F::Text)),
        (M::DeletionVectorReference, 2 | 3) => Some(singular(F::Varint)),
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
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    context.ledger().check_depth(depth)?;
    let mut seen = BTreeSet::new();
    let mut oneofs = BTreeSet::new();
    let mut map_keys: BTreeSet<(u32, Vec<u8>)> = BTreeSet::new();
    while !input.is_empty() {
        let key = read_varint(&mut input, path, context)?;
        let field = u32::try_from(key >> 3)
            .map_err(|_| invalid(path.clone(), "protobuf field number is out of range"))?;
        if field == 0 {
            return Err(invalid(path.clone(), "protobuf field zero is invalid"));
        }
        let wire = (key & 7) as u8;
        context.observe_compile_step()?;
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
        context.ledger().charge_items(1)?;
        match rule.kind {
            FieldKind::Varint => {
                read_varint(&mut input, &field_path, context)?;
            }
            FieldKind::Text | FieldKind::Bytes | FieldKind::PackedVarint => {
                let value = read_length_delimited(&mut input, &field_path, context)?;
                context.ledger().charge_scalar(value.len())?;
                if matches!(rule.kind, FieldKind::PackedVarint) {
                    let mut packed = value;
                    while !packed.is_empty() {
                        read_varint(&mut packed, &field_path, context)?;
                        context.ledger().charge_items(1)?;
                        context.observe_compile_step()?;
                    }
                }
            }
            FieldKind::Message(nested) => {
                let value = read_length_delimited(&mut input, &field_path, context)?;
                scan_message(value, nested, &field_path, depth + 1, context)?;
            }
            FieldKind::MapI32Varint => {
                let value = read_length_delimited(&mut input, &field_path, context)?;
                let map_key =
                    scan_map_entry(value, MapValue::Varint, &field_path, depth + 1, context)?;
                // Long-key comparison/allocation remains an opaque BTree owner.
                context.flush_compile_control()?;
                let duplicate = !map_keys.insert((field, map_key));
                context.observe_compile_step()?;
                if duplicate {
                    return Err(error(
                        field_path,
                        ConnectorCodecErrorKind::DuplicateField,
                        "Iceberg private map contains a duplicate key",
                    ));
                }
            }
            FieldKind::MapI32Bytes => {
                let value = read_length_delimited(&mut input, &field_path, context)?;
                let map_key =
                    scan_map_entry(value, MapValue::Bytes, &field_path, depth + 1, context)?;
                // Long-key comparison/allocation remains an opaque BTree owner.
                context.flush_compile_control()?;
                let duplicate = !map_keys.insert((field, map_key));
                context.observe_compile_step()?;
                if duplicate {
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
    Varint,
    Bytes,
}

fn scan_map_entry(
    mut input: &[u8],
    value_kind: MapValue,
    path: &ConnectorFieldPath,
    depth: usize,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Vec<u8>, ConnectorCodecError> {
    context.ledger().check_depth(depth)?;
    let mut key = None;
    let mut value_seen = false;
    while !input.is_empty() {
        let raw_key = read_varint(&mut input, path, context)?;
        let field = (raw_key >> 3) as u32;
        let wire = (raw_key & 7) as u8;
        context.observe_compile_step()?;
        context.ledger().charge_items(1)?;
        match field {
            1 if key.is_none() => {
                if wire != 0 {
                    return Err(invalid(
                        path.field("key"),
                        "map key has the wrong wire type",
                    ));
                }
                key = Some(
                    read_varint(&mut input, &path.field("key"), context)?
                        .to_le_bytes()
                        .to_vec(),
                );
            }
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
                    MapValue::Varint => {
                        if wire != 0 {
                            return Err(invalid(
                                path.field("value"),
                                "map value has the wrong wire type",
                            ));
                        }
                        read_varint(&mut input, &path.field("value"), context)?;
                    }
                    MapValue::Bytes => {
                        if wire != 2 {
                            return Err(invalid(
                                path.field("value"),
                                "map value has the wrong wire type",
                            ));
                        }
                        let value =
                            read_length_delimited(&mut input, &path.field("value"), context)?;
                        context.ledger().charge_scalar(value.len())?;
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
    // Prost omits a default zero map key in canonical bytes.
    Ok(key.unwrap_or_else(|| 0_u64.to_le_bytes().to_vec()))
}

fn read_length_delimited<'a>(
    input: &mut &'a [u8],
    path: &ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<&'a [u8], ConnectorCodecError> {
    let length = usize::try_from(read_varint(input, path, context)?)
        .map_err(|_| invalid(path.clone(), "protobuf length is out of range"))?;
    if input.len() < length {
        return Err(invalid(path.clone(), "truncated length-delimited field"));
    }
    let (value, rest) = input.split_at(length);
    *input = rest;
    context.observe_compile_step()?;
    Ok(value)
}

fn read_varint(
    input: &mut &[u8],
    path: &ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<u64, ConnectorCodecError> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = input
            .split_first()
            .ok_or_else(|| invalid(path.clone(), "truncated protobuf varint"))?;
        *input = rest;
        let overflow = shift == 63 && byte > 1;
        if !overflow {
            value |= u64::from(byte & 0x7f) << shift;
        }
        context.observe_compile_step()?;
        if overflow {
            return Err(invalid(path.clone(), "protobuf varint overflow"));
        }
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid(path.clone(), "protobuf varint overflow"))
}

fn validate_writer_handle(
    handle: &dto::IcebergWriterHandle,
    path: &ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    let result = (|| {
        let branch = named_branch(handle.branch, path.field("branch"))?;
        validate_table(handle.table.as_ref(), path.field("table"), context)?;
        validate_output(handle.output.as_ref(), path.field("output"), context)?;
        if let Some(scope) = &handle.creation_scope {
            let field = path.field("creation_scope");
            require_exact_bytes::<16>(&scope.session_id, field.field("session_id"))?;
            require_exact_bytes::<16>(&scope.step_id, field.field("step_id"))?;
            require_exact_bytes::<32>(&scope.endpoint_digest, field.field("endpoint_digest"))?;
        }
        if let Some(group) = &handle.position_group_id {
            require_exact_bytes::<32>(group, path.field("position_group_id"))?;
        }
        let expects_group =
            handle.mutation_read.is_some() && branch == dto::IcebergWriteBranch::DeletionVector;
        if expects_group != handle.position_group_id.is_some() {
            return Err(inconsistent(
                path.field("position_group_id"),
                "group must belong to a fixed position materializer",
            ));
        }
        if let Some(recipe) = &handle.mutation_read {
            let field = path.field("mutation_read");
            if !matches!(
                branch,
                dto::IcebergWriteBranch::PositionDelete | dto::IcebergWriteBranch::DeletionVector
            ) || handle.position_rewrite.is_some()
            {
                return Err(inconsistent(
                    field,
                    "fixed mutation input conflicts with the writer branch",
                ));
            }
            bounded_text(
                &recipe.metadata_location,
                MAX_PATH_BYTES,
                field.field("metadata_location"),
                false,
            )?;
            bounded_text(
                &recipe.metadata_identity,
                71,
                field.field("metadata_identity"),
                false,
            )?;
            if let Some(snapshot) = &recipe.snapshot {
                bounded_text(
                    &snapshot.manifest_list,
                    MAX_PATH_BYTES,
                    field.field("snapshot").field("manifest_list"),
                    false,
                )?;
                nonnegative(
                    i64::from(snapshot.schema_id),
                    field.field("snapshot").field("schema_id"),
                )?;
            }
            context.observe_compile_step()?;
        }
        if let Some(target) = &handle.position_rewrite {
            let field = path.field("position_rewrite");
            if branch != dto::IcebergWriteBranch::DeletionVector {
                return Err(inconsistent(
                    field,
                    "position rewrite requires a deletion-vector branch",
                ));
            }
            bounded_text(
                &target.data_file_path,
                MAX_PATH_BYTES,
                field.field("data_file_path"),
                false,
            )?;
            if target.record_count == 0 {
                return Err(invalid(
                    field.field("record_count"),
                    "position rewrite requires a nonempty data file",
                ));
            }
            validate_partition(target.partition.as_ref(), field.field("partition"), context)?;
        }
        match branch {
            dto::IcebergWriteBranch::Data => {
                let recipe = handle.data.as_ref().ok_or_else(|| {
                    inconsistent(path.field("data"), "data branch requires a recipe")
                })?;
                validate_data_recipe(recipe, path.field("data"), context)?;
                if handle.equality.is_some() {
                    return Err(inconsistent(
                        path.clone(),
                        "data branch cannot carry delete recipes",
                    ));
                }
            }
            dto::IcebergWriteBranch::PositionDelete | dto::IcebergWriteBranch::DeletionVector => {
                if handle.data.is_some()
                    || handle.equality.is_some()
                    || (handle.mutation_read.is_none() && handle.position_rewrite.is_none())
                {
                    return Err(inconsistent(
                        path.clone(),
                        "position writer requires exactly one position input recipe",
                    ));
                }
            }
            dto::IcebergWriteBranch::EqualityDelete => {
                if handle.data.is_some() || handle.position_rewrite.is_some() {
                    return Err(inconsistent(
                        path.clone(),
                        "equality-delete branch cannot carry a data recipe",
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
                    context,
                )?;
            }
            dto::IcebergWriteBranch::Unspecified => unreachable!(),
        }
        Ok(())
    })();
    context.observe_compile_step()?;
    result
}

fn validate_table(
    table: Option<&dto::IcebergWriteTableFacts>,
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    let result = (|| {
        let table =
            table.ok_or_else(|| missing(path.clone(), "writer handle requires table facts"))?;
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

            context.observe_compile_step()?;
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
    })();
    context.observe_compile_step()?;
    result
}

fn validate_output(
    output: Option<&dto::IcebergWriterOutput>,
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    let result = (|| {
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
    })();
    context.observe_compile_step()?;
    result
}

fn validate_data_recipe(
    recipe: &dto::IcebergDataBranchRecipe,
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    let result = (|| {
        if let Some(schema) = &recipe.target_schema_json {
            bounded_text(
                schema,
                MAX_SCHEMA_JSON_BYTES,
                path.field("target_schema_json"),
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

            context.observe_compile_step()?;
        }
        for (index, value) in recipe.transform_exprs.iter().enumerate() {
            bounded_text(
                value,
                MAX_TRANSFORM_EXPR_BYTES,
                path.field("transform_exprs").index(index),
                false,
            )?;

            context.observe_compile_step()?;
        }
        Ok(())
    })();
    context.observe_compile_step()?;
    result
}

fn validate_equality_recipe(
    recipe: &dto::IcebergEqualityDeleteRecipe,
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    let result = (|| {
        bounded_text(
            &recipe.field_schema_json,
            MAX_SCHEMA_JSON_BYTES,
            path.field("field_schema_json"),
            false,
        )?;
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
            nonnegative(i64::from(column.field_id), column_path.field("field_id"))?;
            if !ids.insert(column.field_id) {
                return Err(inconsistent(
                    column_path.field("field_id"),
                    "equality field ids must be unique",
                ));
            }

            context.observe_compile_step()?;
        }
        Ok(())
    })();
    context.observe_compile_step()?;
    result
}

pub(crate) fn require_exact_bytes<const N: usize>(
    value: &[u8],
    path: ConnectorFieldPath,
) -> Result<[u8; N], ConnectorCodecError> {
    value
        .try_into()
        .map_err(|_| invalid(path, format!("expected exactly {N} bytes")))
}

fn validate_commit_fragment(
    fragment: &dto::IcebergCommitFragment,
    path: &ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    let result = (|| {
        let artifact = fragment
            .artifact
            .as_ref()
            .ok_or_else(|| missing(path.clone(), "commit fragment requires one artifact"))?;
        if let Some(creation) = &fragment.creation {
            let field = path.field("creation");
            require_exact_bytes::<16>(&creation.create_id, field.field("create_id"))?;
            require_exact_bytes::<32>(&creation.identity, field.field("identity"))?;
            require_exact_bytes::<32>(&creation.content_sha256, field.field("content_sha256"))?;
            let physical = creation.physical.as_ref().ok_or_else(|| {
                missing(
                    field.field("physical"),
                    "creation receipt requires its physical writer",
                )
            })?;
            require_exact_bytes::<16>(
                &physical.execution_query_id,
                field.field("physical").field("execution_query_id"),
            )?;
            require_exact_bytes::<16>(
                &physical.task_key,
                field.field("physical").field("task_key"),
            )?;
        }
        match artifact {
            dto::iceberg_commit_fragment::Artifact::FinalDelete(effect) => {
                validate_final_delete(
                    effect,
                    fragment.creation.is_some(),
                    path.field("final_delete"),
                    context,
                )?;
            }
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
                    context,
                )?;
                validate_metrics(
                    file.metrics.as_ref(),
                    path.field("data_file").field("metrics"),
                    context,
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
                    context,
                )?;
            }
            dto::iceberg_commit_fragment::Artifact::DeletionVector(file) => {
                validate_deletion_vector(file, path.field("deletion_vector"), context)?;
            }
            dto::iceberg_commit_fragment::Artifact::EqualityDeleteFile(file) => {
                let base = path.field("equality_delete_file");
                bounded_text(&file.path, MAX_PATH_BYTES, base.field("path"), false)?;
                validate_partition(file.partition.as_ref(), base.field("partition"), context)?;
                validate_metrics(file.metrics.as_ref(), base.field("metrics"), context)?;
                bounded_count(
                    file.equality_field_ids.len(),
                    MAX_EQUALITY_DELETE_COLUMNS,
                    base.field("equality_field_ids"),
                )?;
                if file.equality_field_ids.is_empty()
                    || any_observed(file.equality_field_ids.iter(), context, |value| *value < 0)?
                    || any_observed(file.equality_field_ids.windows(2), context, |pair| {
                        pair[0] >= pair[1]
                    })?
                {
                    return Err(inconsistent(
                        base.field("equality_field_ids"),
                        "equality field ids must be nonempty, sorted, unique, and nonnegative",
                    ));
                }
            }
        }
        Ok(())
    })();
    context.observe_compile_step()?;
    result
}

fn validate_deletion_vector(
    file: &dto::IcebergDeletionVectorArtifact,
    base: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    validate_delete_artifact_common(
        &file.path,
        file.partition.as_ref(),
        file.metrics.as_ref(),
        &file.referenced_data_file,
        &file.merged_old_references,
        base.clone(),
        context,
    )?;
    validate_content_range(
        file.content_range.as_ref().ok_or_else(|| {
            missing(
                base.field("content_range"),
                "deletion vector requires a range",
            )
        })?,
        base.field("content_range"),
        context,
    )
}

fn validate_final_delete(
    effect: &dto::IcebergFinalDeleteEffect,
    created: bool,
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    use dto::iceberg_final_delete_effect::Effect;
    let target = effect
        .target
        .as_ref()
        .ok_or_else(|| missing(path.field("target"), "final delete requires its target"))?;
    for (name, value) in [
        ("manifest_list", target.manifest_list.as_str()),
        ("manifest_path", target.manifest_path.as_str()),
        ("data_file_path", target.data_file_path.as_str()),
    ] {
        bounded_text(
            value,
            MAX_PATH_BYTES,
            path.field("target").field(name),
            false,
        )?;
        context.observe_compile_step()?;
    }
    if effect.newly_deleted_rows == 0 || effect.newly_deleted_rows > target.record_count {
        return Err(inconsistent(
            path.field("newly_deleted_rows"),
            "new positions are outside the target row domain",
        ));
    }
    match effect.effect.as_ref() {
        Some(Effect::RemoveDataFile(remove)) if !created => {
            let field = path.field("remove_data_file");
            validate_partition(remove.partition.as_ref(), field.field("partition"), context)?;
            validate_merged_references(
                &remove.old_exclusive_deletes,
                field.field("old_exclusive_deletes"),
                context,
            )
        }
        Some(Effect::ReplacePositions(vector)) if created => {
            validate_deletion_vector(vector, path.field("replace_positions"), context)
        }
        _ => Err(inconsistent(
            path,
            "final delete effect and new-object creation receipt disagree",
        )),
    }
}

fn validate_delete_artifact_common(
    path_value: &str,
    partition: Option<&dto::IcebergArtifactPartition>,
    metrics: Option<&dto::IcebergArtifactMetrics>,
    referenced_data_file: &str,
    merged: &[dto::IcebergMergedDeleteReference],
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    let result = (|| {
        bounded_text(path_value, MAX_PATH_BYTES, path.field("path"), false)?;
        bounded_text(
            referenced_data_file,
            MAX_PATH_BYTES,
            path.field("referenced_data_file"),
            false,
        )?;
        validate_partition(partition, path.field("partition"), context)?;
        validate_metrics(metrics, path.field("metrics"), context)?;
        validate_merged_references(merged, path.field("merged_old_references"), context)
    })();
    context.observe_compile_step()?;
    result
}

fn validate_merged_references(
    values: &[dto::IcebergMergedDeleteReference],
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    bounded_count(values.len(), MAX_MERGED_OLD_REFERENCES, path.clone())?;
    let mut previous: Option<LogicalDeleteReference<'_>> = None;
    for (index, value) in values.iter().enumerate() {
        let entry_path = path.index(index);
        let identity = validate_merged_reference(value, entry_path.clone(), context)?;
        if let Some(previous) = previous
            && previous.compare(identity, context)? != std::cmp::Ordering::Less
        {
            return Err(inconsistent(
                entry_path,
                "merged old references must be sorted and unique",
            ));
        }
        previous = Some(identity);
        context.observe_compile_step()?;
    }
    Ok(())
}

// Match EntryIdentity's variant/field ordering without copying DTO strings.
#[derive(Clone, Copy)]
enum LogicalDeleteReference<'a> {
    File(&'a str),
    Vector {
        path: &'a str,
        offset: i64,
        length: i64,
        referenced_data_file: &'a str,
    },
}
impl LogicalDeleteReference<'_> {
    fn validate(
        self,
        path: ConnectorFieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<(), ConnectorCodecError> {
        let result = (|| {
            match self {
                Self::File(value) => {
                    bounded_text(value, MAX_PATH_BYTES, path.field("path"), false)?
                }
                Self::Vector {
                    path: value,
                    offset,
                    length,
                    referenced_data_file,
                } => {
                    bounded_text(value, MAX_PATH_BYTES, path.field("path"), false)?;
                    bounded_text(
                        referenced_data_file,
                        MAX_PATH_BYTES,
                        path.field("referenced_data_file"),
                        false,
                    )?;
                    nonnegative(offset, path.field("content_offset"))?;
                    if length <= 0 || offset.checked_add(length).is_none() {
                        return Err(inconsistent(
                            path,
                            "DV reference requires a positive, non-overflowing range",
                        ));
                    }
                }
            }
            Ok(())
        })();
        context.observe_compile_step()?;
        result
    }
    fn compare(
        self,
        other: Self,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<std::cmp::Ordering, ConnectorCodecError> {
        use std::cmp::Ordering;
        context.flush_compile_control()?;
        let ordering = match (self, other) {
            (Self::File(left), Self::File(right)) => compare_text(left, right, context)?,
            (Self::File(_), Self::Vector { .. }) => Ordering::Less,
            (Self::Vector { .. }, Self::File(_)) => Ordering::Greater,
            (
                Self::Vector {
                    path: left,
                    offset: lo,
                    length: ll,
                    referenced_data_file: ld,
                },
                Self::Vector {
                    path: right,
                    offset: ro,
                    length: rl,
                    referenced_data_file: rd,
                },
            ) => {
                let mut ordering = compare_text(left, right, context)?;
                if ordering == Ordering::Equal {
                    ordering = lo.cmp(&ro);
                    context.observe_compile_step()?;
                }
                if ordering == Ordering::Equal {
                    ordering = ll.cmp(&rl);
                    context.observe_compile_step()?;
                }
                if ordering == Ordering::Equal {
                    ordering = compare_text(ld, rd, context)?;
                }
                ordering
            }
        };
        context.observe_compile_step()?;
        Ok(ordering)
    }
}
fn validate_merged_reference<'a>(
    value: &'a dto::IcebergMergedDeleteReference,
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<LogicalDeleteReference<'a>, ConnectorCodecError> {
    context.flush_compile_control()?;
    let result = (|| {
        let identity = match value.entry.as_ref() {
            Some(dto::iceberg_merged_delete_reference::Entry::DeleteFilePath(value)) => {
                LogicalDeleteReference::File(value)
            }
            Some(dto::iceberg_merged_delete_reference::Entry::DeletionVector(value)) => {
                LogicalDeleteReference::Vector {
                    path: &value.path,
                    offset: value.content_offset,
                    length: value.content_size_in_bytes,
                    referenced_data_file: &value.referenced_data_file,
                }
            }
            None => {
                return Err(inconsistent(
                    path.clone(),
                    "merged old reference requires an exact delete entry",
                ));
            }
        };
        identity.validate(path, context)?;
        Ok(identity)
    })();
    context.observe_compile_step()?;
    result
}

fn validate_partition(
    partition: Option<&dto::IcebergArtifactPartition>,
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    let result = (|| {
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

            context.observe_compile_step()?;
        }
        Ok(())
    })();
    context.observe_compile_step()?;
    result
}

fn validate_metrics(
    metrics: Option<&dto::IcebergArtifactMetrics>,
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    let result = (|| {
        let metrics = metrics.ok_or_else(|| missing(path.clone(), "artifact requires metrics"))?;
        bounded_count(
            metrics.split_offsets.len(),
            MAX_SPLIT_OFFSETS,
            path.field("split_offsets"),
        )?;
        if any_observed(metrics.split_offsets.iter(), context, |value| *value < 0)? {
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
            if any_observed(values.values(), context, |value| *value < 0)? {
                return Err(invalid(
                    path.field("column_stats").field(name),
                    "column statistic count must be nonnegative",
                ));
            }

            context.observe_compile_step()?;
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
            if any_observed(values.values(), context, |value| {
                value.len() > MAX_COLUMN_STAT_BOUND_BYTES
            })? {
                return Err(capacity(
                    path.field("column_stats").field(name),
                    "column statistic bound exceeds its byte limit",
                ));
            }

            context.observe_compile_step()?;
        }
        Ok(())
    })();
    context.observe_compile_step()?;
    result
}

fn validate_content_range(
    range: &dto::IcebergContentRange,
    path: ConnectorFieldPath,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<(), ConnectorCodecError> {
    let result = (|| {
        nonnegative(range.offset, path.field("offset"))?;
        if range.size_in_bytes <= 0 {
            return Err(invalid(
                path.field("size_in_bytes"),
                "content range size must be positive",
            ));
        }
        Ok(())
    })();
    context.observe_compile_step()?;
    result
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

fn any_observed<I: IntoIterator>(
    values: I,
    context: &mut ConnectorDecodeContext<'_>,
    mut predicate: impl FnMut(I::Item) -> bool,
) -> Result<bool, ConnectorCodecError> {
    for value in values {
        let matched = predicate(value);
        context.observe_compile_step()?;
        if matched {
            return Ok(true);
        }
    }
    Ok(false)
}
fn compare_text(
    left: &str,
    right: &str,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<std::cmp::Ordering, ConnectorCodecError> {
    if !context.is_compile_observed() {
        return Ok(left.cmp(right));
    }
    for (left, right) in left.bytes().zip(right.bytes()) {
        let ordering = left.cmp(&right);
        context.observe_compile_step()?;
        if ordering != std::cmp::Ordering::Equal {
            return Ok(ordering);
        }
    }
    let ordering = left.len().cmp(&right.len());
    context.observe_compile_step()?;
    Ok(ordering)
}
// The original limits bound opaque library input. This boundary does not
// claim cooperative Prost/encoded_len internals or a host memory grant.
fn observe_opaque<T>(
    context: &mut ConnectorDecodeContext<'_>,
    operation: impl FnOnce() -> T,
) -> Result<T, ConnectorCodecError> {
    context.flush_compile_control()?;
    let result = operation();
    context.observe_compile_step()?;
    context.flush_compile_control()?;
    Ok(result)
}
fn finish_decode<T>(
    result: Result<T, ConnectorCodecError>,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<T, ConnectorCodecError> {
    if result
        .as_ref()
        .is_err_and(|error| error.compile_control_error().is_some())
    {
        return result;
    }
    context.flush_compile_control()?;
    result
}

#[cfg(test)]
mod compile_control_tests {
    use super::*;
    use std::sync::Mutex;

    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEnvelopeHeader, ConnectorInstanceId,
        ConnectorProviderId,
    };
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use parquet::basic::Compression;

    use crate::commit::report::IcebergColumnStats;
    use crate::commit::write_stack::codec::IcebergWriteValueCodec;
    use crate::commit::write_stack::domain::*;
    use crate::commit::write_stack::test_support::table_facts;
    use crate::delete_file::IcebergFileFormat;
    use crate::write_descriptor::{IcebergPartitionDescriptor, IcebergPartitionValueDescriptor};

    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        refuse: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            trace.push((phase, units));
            if let Some((index, cause)) = self.refuse.clone()
                && trace.len() == index
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn causes() -> [CompileControlError; 3] {
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }
    fn header(category: ConnectorCodecCategory) -> ConnectorEnvelopeHeader {
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse(crate::PROVIDER_ID).unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                CatalogVersion::from_bytes([7; 32]),
            ),
            category,
            ConnectorCodecRevision::try_new(WRITE_CODEC_REVISION).unwrap(),
        )
    }
    fn ledger() -> ConnectorDecodeLedger {
        ConnectorDecodeLedger::new(
            ConnectorDecodeLimits::try_new(
                MAX_CONNECTOR_WRITER_HANDLE_BYTES,
                MAX_CONNECTOR_WRITER_HANDLE_BYTES,
                MAX_CONNECTOR_WRITER_HANDLE_BYTES,
                1_000_000,
                64,
            )
            .unwrap(),
        )
    }
    fn output(format: IcebergFileFormat) -> IcebergWriterOutput {
        IcebergWriterOutput::try_new(format, Compression::SNAPPY, None).unwrap()
    }
    fn handles() -> Vec<dto::IcebergWriterHandle> {
        let data = IcebergWriterHandle::try_new_data(
            table_facts(),
            output(IcebergFileFormat::Parquet),
            IcebergDataBranchRecipe::try_new(
                None,
                (0..321).map(|i| format!("source_{i}")).collect(),
                (0..321).map(|i| format!("partition_{i}")).collect(),
                vec!["identity(k1)".into(); 321],
                false,
            )
            .unwrap(),
        )
        .unwrap();
        let equality = IcebergWriterHandle::try_new_equality_delete(
            table_facts(),
            output(IcebergFileFormat::Parquet),
            IcebergEqualityDeleteRecipe::try_new(
                (1..=321)
                    .map(|i| {
                        IcebergEqualityDeleteColumnFacts::try_new(
                            format!("k{i}"),
                            i,
                        )
                        .unwrap()
                    })
                    .collect(),
                serde_json::json!({"type":"struct","schema-id":1,"fields":(1..=321).map(|i| serde_json::json!({"id":i,"name":format!("k{i}"),"required":false,"type":"long"})).collect::<Vec<_>>()}).to_string(),
            )
            .unwrap(),
        )
        .unwrap();
        let table = table_facts();
        let mutation = IcebergMutationReadRecipe::try_new(
            &table,
            "s3://b/wh/db/t/metadata/v4.metadata.json".to_owned(),
            format!("sha256:{}", "a".repeat(64)),
            Some(
                IcebergMutationSnapshot::try_new(
                    "s3://b/wh/db/t/metadata/snap-77.avro".to_owned(),
                    1,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let position = IcebergWriterHandle::try_new_delete(
            IcebergWriteBranch::PositionDelete,
            table.clone(),
            output(IcebergFileFormat::Parquet),
        )
        .unwrap()
        .try_with_mutation_read(mutation, None)
        .unwrap();
        let partition = IcebergArtifactPartition::try_new(
            String::new(),
            String::new(),
            0,
            IcebergPartitionDescriptor {
                values: vec![
                    IcebergPartitionValueDescriptor {
                        is_null: false,
                        datum_bytes: Some(vec![1, 2, 3, 4]),
                    };
                    321
                ],
            },
        )
        .unwrap();
        let vector = IcebergWriterHandle::try_new_delete(
            IcebergWriteBranch::DeletionVector,
            table,
            output(IcebergFileFormat::Puffin),
        )
        .unwrap()
        .try_with_position_rewrite(
            IcebergPositionRewriteTarget::try_new(
                "s3://b/data/a.parquet".to_owned(),
                100,
                partition,
            )
            .unwrap(),
        )
        .unwrap();
        let codec = IcebergWriteValueCodec::new("catalog.iceberg");
        [data, equality, position, vector]
            .iter()
            .map(|handle| codec.encode_writer_handle_value(handle).unwrap())
            .collect()
    }
    fn metrics() -> IcebergArtifactMetrics {
        let mut stats = IcebergColumnStats::default();
        for i in 1..=321 {
            stats.column_sizes.insert(i, 1024);
            stats.value_counts.insert(i, 1000);
            stats.null_value_counts.insert(i, 0);
            stats.nan_value_counts.insert(i, 0);
            stats.lower_bounds.insert(i, vec![0, 1, 255]);
            stats.upper_bounds.insert(i, vec![255, 7]);
        }
        IcebergArtifactMetrics::try_new(1000, 8192, (0..321).collect(), Some(stats)).unwrap()
    }
    fn partition() -> IcebergArtifactPartition {
        IcebergArtifactPartition::try_new(
            "p=1".into(),
            "nonnull".into(),
            0,
            IcebergPartitionDescriptor {
                values: (0..321)
                    .map(|i| IcebergPartitionValueDescriptor {
                        is_null: i % 2 == 0,
                        datum_bytes: (i % 2 != 0).then_some(vec![i as u8, 255]),
                    })
                    .collect(),
            },
        )
        .unwrap()
    }
    fn fragments() -> Vec<dto::IcebergCommitFragment> {
        let merged = || {
            (0..321)
                .map(|i| crate::commit::model::EntryIdentity::DeleteFile {
                    path: format!("s3://b/data/old-{i:04}.parquet"),
                })
                .collect()
        };
        let artifacts = [
            IcebergCommitFragment::data_file(
                IcebergDataFileArtifact::try_new(
                    "s3://b/data/new.parquet".into(),
                    IcebergFileFormat::Parquet,
                    partition(),
                    metrics(),
                    Some(7),
                )
                .unwrap(),
            ),
            IcebergCommitFragment::position_delete_file(
                IcebergPositionDeleteFileArtifact::try_new(
                    "s3://b/data/new-pos.parquet".into(),
                    partition(),
                    metrics(),
                    "s3://b/data/source.parquet".into(),
                    merged(),
                )
                .unwrap(),
            ),
            IcebergCommitFragment::deletion_vector(
                IcebergDeletionVectorArtifact::try_new(
                    "s3://b/data/new.puffin".into(),
                    partition(),
                    metrics(),
                    "s3://b/data/source.parquet".into(),
                    IcebergContentRange::try_new(4, 64).unwrap(),
                    1000,
                    merged(),
                )
                .unwrap(),
            ),
            IcebergCommitFragment::equality_delete_file(
                IcebergEqualityDeleteFileArtifact::try_new(
                    "s3://b/data/new-eq.parquet".into(),
                    partition(),
                    metrics(),
                    (1..=321).collect(),
                )
                .unwrap(),
            ),
        ];
        let codec = IcebergWriteValueCodec::new("catalog.iceberg");
        artifacts
            .iter()
            .map(|artifact| codec.encode_commit_fragment_value(artifact).unwrap())
            .collect()
    }
    fn run_writer(
        bytes: &[u8],
        control: &Control,
    ) -> Result<dto::IcebergWriterHandle, ConnectorCodecError> {
        let expected = header(ConnectorCodecCategory::WriteHandle);
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, control)?;
        decode_writer_handle(bytes, &mut context)
    }
    fn run_fragment(
        bytes: &[u8],
        control: &Control,
    ) -> Result<ValidatedCommitFragmentWire, ConnectorCodecError> {
        let expected = header(ConnectorCodecCategory::CommitFragment);
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, control)?;
        decode_commit_fragment(bytes, &mut context)
    }
    fn assert_ledger_same(old: &ConnectorDecodeLedger, new: &ConnectorDecodeLedger) {
        assert_eq!(old.raw_bytes(), new.raw_bytes());
        assert_eq!(old.scalar_bytes(), new.scalar_bytes());
        assert_eq!(old.items(), new.items());
        assert_eq!(old.retained_bytes(), new.retained_bytes());
    }
    fn representative_checkpoints(trace: &[(CompilePhase, u32)]) -> Vec<usize> {
        assert!(
            trace
                .iter()
                .all(|(phase, units)| *phase == CompilePhase::ProviderValidation && *units <= 256)
        );
        let positives: Vec<_> = trace
            .iter()
            .enumerate()
            .filter_map(|(i, (_, units))| (*units == 256).then_some(i + 1))
            .collect();
        if positives.is_empty() {
            return vec![1, trace.len()];
        }
        let mut positions = vec![
            1,
            positives[0],
            positives[positives.len() / 2],
            *positives.last().unwrap(),
            trace.len(),
        ];
        positions.sort_unstable();
        positions.dedup();
        positions
    }

    #[test]
    fn all_actual_writer_branches_preserve_legacy_dtos_and_ledger_charges() {
        let expected = header(ConnectorCodecCategory::WriteHandle);
        for raw in handles() {
            let bytes = raw.encode_to_vec();
            let mut old_budget = ledger();
            let old = decode_writer_handle(
                &bytes,
                &mut ConnectorDecodeContext::new(&expected, &mut old_budget),
            )
            .unwrap();
            let control = Control::default();
            let mut new_budget = ledger();
            let new = decode_writer_handle(
                &bytes,
                &mut ConnectorDecodeContext::try_new_for_compile(
                    &expected,
                    &mut new_budget,
                    &control,
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(old, raw);
            assert_eq!(new, raw);
            assert_ledger_same(&old_budget, &new_budget);
            let trace = control.trace.lock().unwrap();
            if raw.mutation_read.is_none() {
                assert!(trace.iter().any(|(_, units)| *units == 256));
            }
            representative_checkpoints(&trace);
            assert_eq!(trace.last().unwrap().1, 1);
        }
    }
    #[test]
    fn all_actual_commit_artifacts_preserve_legacy_dtos_and_ledger_charges() {
        let expected = header(ConnectorCodecCategory::CommitFragment);
        for raw in fragments() {
            let bytes = raw.encode_to_vec();
            let mut old_budget = ledger();
            let old = decode_commit_fragment(
                &bytes,
                &mut ConnectorDecodeContext::new(&expected, &mut old_budget),
            )
            .unwrap();
            let control = Control::default();
            let mut new_budget = ledger();
            let new = decode_commit_fragment(
                &bytes,
                &mut ConnectorDecodeContext::try_new_for_compile(
                    &expected,
                    &mut new_budget,
                    &control,
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(old.as_dto(), &raw);
            assert_eq!(new.as_dto(), &raw);
            assert_ledger_same(&old_budget, &new_budget);
            let trace = control.trace.lock().unwrap();
            assert!(trace.iter().any(|(_, units)| *units == 256));
            representative_checkpoints(&trace);
            assert_eq!(trace.last().unwrap().1, 1);
        }
    }
    #[test]
    fn actual_writer_scanner_validation_and_publication_keep_each_original_control() {
        for raw in handles() {
            let bytes = raw.encode_to_vec();
            let baseline = Control::default();
            run_writer(&bytes, &baseline).unwrap();
            let trace = baseline.trace.lock().unwrap().clone();
            for index in representative_checkpoints(&trace) {
                for cause in causes() {
                    let control = Control {
                        trace: Default::default(),
                        refuse: Some((index, cause.clone())),
                    };
                    let error = run_writer(&bytes, &control).unwrap_err();
                    assert_eq!(error.compile_control_error(), Some(&cause));
                    assert_eq!(
                        error.kind(),
                        &ConnectorCodecErrorKind::CompileControl(cause)
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
                }
            }
        }
    }
    #[test]
    fn actual_commit_packed_maps_metrics_and_publication_keep_each_original_control() {
        for raw in fragments() {
            let bytes = raw.encode_to_vec();
            let baseline = Control::default();
            run_fragment(&bytes, &baseline).unwrap();
            let trace = baseline.trace.lock().unwrap().clone();
            for index in representative_checkpoints(&trace) {
                for cause in causes() {
                    let control = Control {
                        trace: Default::default(),
                        refuse: Some((index, cause.clone())),
                    };
                    assert_eq!(
                        run_fragment(&bytes, &control)
                            .unwrap_err()
                            .compile_control_error(),
                        Some(&cause)
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
                }
            }
        }
    }
    #[test]
    fn dto_validation_actual_short_circuit_loops_observe_the_final_scalar_and_original_error() {
        let mut metrics = dto::IcebergArtifactMetrics {
            split_offsets: vec![0; 321],
            ..Default::default()
        };
        metrics.split_offsets[320] = -1;
        let expected = header(ConnectorCodecCategory::CommitFragment);
        let baseline = Control::default();
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &baseline).unwrap();
        let old_error = validate_metrics(
            Some(&metrics),
            ConnectorFieldPath::root("metrics"),
            &mut context,
        )
        .unwrap_err();
        finish_decode::<()>(Err(old_error), &mut context).unwrap_err();
        assert_eq!(
            baseline
                .trace
                .lock()
                .unwrap()
                .iter()
                .map(|(_, n)| *n)
                .collect::<Vec<_>>(),
            vec![0, 256, 66]
        );
        for cause in causes() {
            for index in [2, 3] {
                let control = Control {
                    trace: Default::default(),
                    refuse: Some((index, cause.clone())),
                };
                let mut budget = ledger();
                let mut context =
                    ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                        .unwrap();
                let result = validate_metrics(
                    Some(&metrics),
                    ConnectorFieldPath::root("metrics"),
                    &mut context,
                );
                assert_eq!(
                    finish_decode(result, &mut context)
                        .unwrap_err()
                        .compile_control_error(),
                    Some(&cause)
                );
                let count = control.trace.lock().unwrap().len();
                assert_eq!(
                    context
                        .flush_compile_control()
                        .unwrap_err()
                        .compile_control_error(),
                    Some(&cause)
                );
                assert_eq!(control.trace.lock().unwrap().len(), count);
            }
        }
        let mut budget = ledger();
        let mut context = ConnectorDecodeContext::new(&expected, &mut budget);
        let error = validate_metrics(
            Some(&metrics),
            ConnectorFieldPath::root("metrics"),
            &mut context,
        )
        .unwrap_err();
        assert_eq!(error.kind(), &ConnectorCodecErrorKind::InvalidValue);
        assert_eq!(error.detail(), "split offset must be nonnegative");
    }
    #[test]
    fn ordinary_validation_and_malformed_prost_utf8_observe_tail_without_text_inference() {
        let mut ordinary = handles().remove(0);
        ordinary.data.as_mut().unwrap().partition_column_names.pop();
        let cases = [ordinary.encode_to_vec(), vec![0x12, 3, 0x0a, 1, 0xff]];
        for bytes in cases {
            let baseline = Control::default();
            let error = run_writer(&bytes, &baseline).unwrap_err();
            assert_eq!(error.compile_control_error(), None);
            let trace = baseline.trace.lock().unwrap().clone();
            for cause in causes() {
                let control = Control {
                    trace: Default::default(),
                    refuse: Some((trace.len(), cause.clone())),
                };
                assert_eq!(
                    run_writer(&bytes, &control)
                        .unwrap_err()
                        .compile_control_error(),
                    Some(&cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace);
            }
            let expected = header(ConnectorCodecCategory::WriteHandle);
            let mut budget = ledger();
            let old = decode_writer_handle(
                &bytes,
                &mut ConnectorDecodeContext::new(&expected, &mut budget),
            )
            .unwrap_err();
            assert_eq!(old.kind(), error.kind());
            assert_eq!(old.detail(), error.detail());
        }
        assert_eq!(
            super::invalid(
                ConnectorFieldPath::root("ordinary"),
                "cancelled deadline resource exhausted"
            )
            .compile_control_error(),
            None
        );
    }
    #[test]
    fn long_path_comparison_is_observed_and_matches_native_ordering() {
        let expected = header(ConnectorCodecCategory::WriteHandle);
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((2, cause.clone())),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            let value = "x".repeat(321);
            let error = compare_text(&value, &value, &mut context).unwrap_err();
            assert_eq!(error.compile_control_error(), Some(&cause));
            assert_eq!(control.trace.lock().unwrap()[1].1, 256);
        }
        let mut budget = ledger();
        let mut context = ConnectorDecodeContext::new(&expected, &mut budget);
        for (left, right) in [
            ("é", "文"),
            ("é", "éa"),
            ("ab", "a"),
            ("", ""),
            ("abc", "abd"),
        ] {
            assert_eq!(
                compare_text(left, right, &mut context).unwrap(),
                left.cmp(right)
            );
        }
    }
    #[test]
    fn logical_vector_reference_comparison_observes_the_complete_last_data_path() {
        let expected = header(ConnectorCodecCategory::CommitFragment);
        let data_left = format!("s3://b/{}a", "x".repeat(768));
        let data_right = format!("s3://b/{}b", "x".repeat(768));
        let left = LogicalDeleteReference::Vector {
            path: "s3://b/shared.puffin",
            offset: 4,
            length: 64,
            referenced_data_file: &data_left,
        };
        let right = LogicalDeleteReference::Vector {
            path: "s3://b/shared.puffin",
            offset: 4,
            length: 64,
            referenced_data_file: &data_right,
        };
        let baseline = Control::default();
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &baseline).unwrap();
        assert_eq!(
            left.compare(right, &mut context).unwrap(),
            std::cmp::Ordering::Less
        );
        context.flush_compile_control().unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        // Constructor and comparison-entry checks can observe zero units.
        // The short Puffin path and scalar range fields cannot fill a slice;
        // the first full slice therefore reaches the final data-file field.
        let refuse_at = trace
            .iter()
            .position(|(_, units)| *units == 256)
            .expect("long referenced data path must perform a full observed slice")
            + 1;
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((refuse_at, cause.clone())),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            let error = left.compare(right, &mut context).unwrap_err();
            assert_eq!(error.compile_control_error(), Some(&cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..refuse_at]);
            assert_eq!(control.trace.lock().unwrap().last().unwrap().1, 256);
            let count = control.trace.lock().unwrap().len();
            assert_eq!(
                context
                    .flush_compile_control()
                    .unwrap_err()
                    .compile_control_error(),
                Some(&cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), count);
        }
        let mut budget = ledger();
        let mut context = ConnectorDecodeContext::new(&expected, &mut budget);
        assert_eq!(
            left.compare(right, &mut context).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            right.compare(left, &mut context).unwrap(),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            left.compare(left, &mut context).unwrap(),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn legacy_unknown_duplicate_and_capacity_refusals_keep_their_original_categories() {
        let mut unknown = handles().remove(0).encode_to_vec();
        unknown.extend_from_slice(&[0xf8, 7, 1]);
        let raw = handles().remove(0).encode_to_vec();
        let mut duplicate = raw.clone();
        duplicate.extend_from_slice(&raw);
        let mut excessive = handles().remove(0);
        excessive.data.as_mut().unwrap().transform_exprs[0] =
            "x".repeat(MAX_TRANSFORM_EXPR_BYTES + 1);
        let expected = header(ConnectorCodecCategory::WriteHandle);
        for (bytes, kind) in [
            (unknown, ConnectorCodecErrorKind::UnknownField),
            (duplicate, ConnectorCodecErrorKind::DuplicateField),
            (excessive.encode_to_vec(), ConnectorCodecErrorKind::Capacity),
        ] {
            let control = Control::default();
            let error = run_writer(&bytes, &control).unwrap_err();
            let mut budget = ledger();
            let legacy = decode_writer_handle(
                &bytes,
                &mut ConnectorDecodeContext::new(&expected, &mut budget),
            )
            .unwrap_err();
            assert_eq!(error.kind(), &kind);
            assert_eq!(legacy.kind(), &kind);
            assert_eq!(error.detail(), legacy.detail());
            assert_eq!(error.compile_control_error(), None);
        }
    }
    #[test]
    fn varint_observation_follows_consumed_byte_and_never_rechecks_primary_control() {
        let expected = header(ConnectorCodecCategory::WriteHandle);
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((2, cause.clone())),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            for _ in 0..255 {
                context.observe_compile_step().unwrap();
            }
            let mut bytes: &[u8] = &[7];
            let error = read_varint(
                &mut bytes,
                &ConnectorFieldPath::root("varint"),
                &mut context,
            )
            .unwrap_err();
            assert!(bytes.is_empty());
            assert_eq!(error.compile_control_error(), Some(&cause));
            assert_eq!(
                finish_decode::<u64>(Err(error), &mut context)
                    .unwrap_err()
                    .compile_control_error(),
                Some(&cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), 2);
        }
    }
}
