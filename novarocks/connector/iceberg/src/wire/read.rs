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

//! Pure Iceberg-private read codecs.
//!
//! Public provider/catalog/category/revision and split scheduling facts are
//! validated by the common carrier before these functions run. This module
//! owns only Iceberg's frozen table, view, column and atomic split payloads.

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorSplit, ConnectorTableExecuteHandle, ConnectorTableHandle, ConnectorValue,
    ConnectorValueType, Domain, Range, SchemaTableName, SplitWeight, TupleDomain, ValueSet,
};
use novarocks_spi::connector::{
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorDecodeContext, ConnectorFieldPath,
    ConnectorPrivateDecoder, ConnectorPrivateEncoder,
};
use prost::Message;

use crate::provider_types::IcebergReadView;
use crate::typed_read::{
    ColumnIdentity, ColumnIdentityCategory, FilesTableSplit, FilesTableSplitParams,
    HiveTransactionHandle, IcebergAddedRows, IcebergChangeSplit, IcebergChangeWindowHandle,
    IcebergChangeWindowHandleParams, IcebergColumnHandle, IcebergColumnHandleParams,
    IcebergDeleteFile, IcebergDeleteFileContent, IcebergDeleteFileParams,
    IcebergDeletedDataFileRows, IcebergFileFormat, IcebergInsertTableHandle,
    IcebergInsertTableHandleParams, IcebergMergeTableHandle, IcebergOptimizeHandle,
    IcebergProcedureId, IcebergReadSplit, IcebergRewriteArtifactContentId,
    IcebergRewritePositionDeleteFilesHandle, IcebergRewritePositionDeleteFilesSplit,
    IcebergRewritePositionDeleteFilesSplitParams, IcebergRuntimeRelation, IcebergSplit,
    IcebergSplitParams, IcebergSystemTableReference, IcebergSystemTableReferenceParams,
    IcebergSystemTableType, IcebergTableExecuteHandle, IcebergTableExecuteHandleParams,
    IcebergTableExecuteProcedureHandle, IcebergTableHandle, IcebergTableHandleParams,
    IcebergVisibilityDifferenceRows, ParquetFileDecryptionData, TableChangesChangeType,
    TableChangesFunctionHandle, TableChangesFunctionHandleParams, TableChangesSplit,
    TableChangesSplitParams, TrinoManifestContent, TrinoManifestFile, TrinoManifestFileParams,
};

use super::dto;
use crate::typed_read::split::{decode_read_domain, encode_read_domain};

const MAX_PRIVATE_READ_BYTES: usize = 16 * 1024 * 1024;

/// One stateless codec object shared by every catalog generation.
#[derive(Clone, Copy, Debug, Default)]
pub struct IcebergReadWireCodec;

impl ConnectorPrivateEncoder<IcebergRuntimeRelation> for IcebergReadWireCodec {
    fn encode_private(&self, value: &IcebergRuntimeRelation) -> Result<Bytes, ConnectorCodecError> {
        Ok(Bytes::from(encode_relation(value).encode_to_vec()))
    }
}

impl ConnectorPrivateDecoder<IcebergRuntimeRelation> for IcebergReadWireCodec {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergRuntimeRelation, ConnectorCodecError> {
        let result = (|| {
            let raw = decode_root::<dto::IcebergReadTablePayload>(
                payload,
                context,
                ConnectorFieldPath::root("iceberg_read_table"),
                Schema::ReadTable,
            )?;
            let value = decode_relation(&raw, context)?;
            charge_retained(context, payload.len(), size_of::<IcebergRuntimeRelation>())?;
            Ok(value)
        })();
        context.observe_compile_step()?;
        finish_decode(result, context)
    }
}

impl ConnectorPrivateEncoder<IcebergReadView> for IcebergReadWireCodec {
    fn encode_private(&self, value: &IcebergReadView) -> Result<Bytes, ConnectorCodecError> {
        let raw = dto::IcebergReadViewPayload {
            transaction: Some(encode_transaction(value.transaction())),
        };
        Ok(Bytes::from(raw.encode_to_vec()))
    }
}

impl ConnectorPrivateDecoder<IcebergReadView> for IcebergReadWireCodec {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergReadView, ConnectorCodecError> {
        let result = (|| {
            let raw = decode_root::<dto::IcebergReadViewPayload>(
                payload,
                context,
                ConnectorFieldPath::root("iceberg_read_view"),
                Schema::ReadView,
            )?;
            let transaction = raw.transaction.as_ref().ok_or_else(|| {
                codec_error(
                    "iceberg_read_view.transaction",
                    ConnectorCodecErrorKind::MissingField,
                    "Iceberg read view requires a transaction marker",
                )
            })?;
            let value = IcebergReadView::new(decode_transaction(transaction, context)?);
            charge_retained(context, payload.len(), size_of::<IcebergReadView>())?;
            Ok(value)
        })();
        context.observe_compile_step()?;
        finish_decode(result, context)
    }
}

impl ConnectorPrivateEncoder<IcebergColumnHandle> for IcebergReadWireCodec {
    fn encode_private(&self, value: &IcebergColumnHandle) -> Result<Bytes, ConnectorCodecError> {
        Ok(Bytes::from(encode_column(value).encode_to_vec()))
    }
}

impl ConnectorPrivateDecoder<IcebergColumnHandle> for IcebergReadWireCodec {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergColumnHandle, ConnectorCodecError> {
        let result = (|| {
            let raw = decode_root::<dto::IcebergColumnHandle>(
                payload,
                context,
                ConnectorFieldPath::root("iceberg_read_column"),
                Schema::Column,
            )?;
            let value = decode_column(&raw, context)?;
            charge_retained(context, payload.len(), size_of::<IcebergColumnHandle>())?;
            Ok(value)
        })();
        context.observe_compile_step()?;
        finish_decode(result, context)
    }
}

impl ConnectorPrivateEncoder<IcebergReadSplit> for IcebergReadWireCodec {
    fn encode_private(&self, value: &IcebergReadSplit) -> Result<Bytes, ConnectorCodecError> {
        Ok(Bytes::from(encode_split(value)?.encode_to_vec()))
    }
}

impl IcebergReadWireCodec {
    /// Decode one Iceberg split only after the common carrier has validated its
    /// scheduling facts. The facts are inputs, never duplicated private wire.
    pub fn decode_split_private(
        &self,
        payload: &[u8],
        facts: &novarocks_spi::connector::read_stack::ConnectorReadSplitFacts,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergReadSplit, ConnectorCodecError> {
        let result = (|| {
            let raw = decode_root::<dto::IcebergReadSplitPayload>(
                payload,
                context,
                ConnectorFieldPath::root("iceberg_read_split"),
                Schema::ReadSplit,
            )?;
            validate_common_split_facts(facts)?;
            let split = decode_split(&raw, facts, context)?;
            validate_materialized_split_facts(&split, facts)?;
            charge_retained(
                context,
                payload.len(),
                usize::try_from(split.retained_size_in_bytes()).unwrap_or(usize::MAX),
            )?;
            Ok(split)
        })();
        context.observe_compile_step()?;
        finish_decode(result, context)
    }
}

fn decode_root<M: Message + Default>(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    path: ConnectorFieldPath,
    schema: Schema,
) -> Result<M, ConnectorCodecError> {
    if payload.len() > MAX_PRIVATE_READ_BYTES {
        return Err(codec_error(
            "connector_payload",
            ConnectorCodecErrorKind::Capacity,
            "Iceberg private read payload exceeds 16 MiB",
        ));
    }
    context.ledger().charge_raw(payload.len())?;
    let scalar_before = context.ledger().scalar_bytes();
    let items_before = context.ledger().items();
    scan_message(payload, context, path.clone(), schema, 0)?;
    context.flush_compile_control()?;
    let decoded = M::decode(payload).map_err(|error| {
        ConnectorCodecError::new(
            path,
            ConnectorCodecErrorKind::InvalidValue,
            format!("malformed Iceberg private protobuf: {error}"),
        )
    });
    context.observe_compile_step()?;
    let decoded = finish_decode(decoded, context)?;
    let scalar_bytes = context
        .ledger()
        .scalar_bytes()
        .saturating_sub(scalar_before);
    let items = context.ledger().items().saturating_sub(items_before);
    context.ledger().charge_retained(
        size_of::<M>()
            .saturating_add(scalar_bytes)
            .saturating_add(items.saturating_mul(size_of::<usize>())),
    )?;
    Ok(decoded)
}

fn charge_retained(
    context: &mut ConnectorDecodeContext<'_>,
    _raw_bytes: usize,
    concrete_bytes: usize,
) -> Result<(), ConnectorCodecError> {
    // Raw wire bytes were already charged by `decode_root`. Retained bytes are
    // an independent receiver-side semantic allocation charge.
    context.ledger().charge_retained(concrete_bytes)
}

#[derive(Clone, Copy, Debug)]
enum Schema {
    ValueType,
    Decimal,
    Value,
    Bound,
    Range,
    ValueSet,
    Domain,
    ColumnDomain,
    TupleDomain,
    SchemaName,
    Identity,
    Column,
    Transaction,
    ReadView,
    Pinned,
    ReadDomain,
    CowSourceProof,
    Table,
    TableFunction,
    ChangeWindowHandle,
    SystemTable,
    Optimize,
    Artifact,
    RewriteHandle,
    TableExecute,
    Insert,
    Merge,
    ReadTable,
    Decryption,
    Delete,
    DataSplit,
    TableChangesSplit,
    AddedRows,
    VisibilityDifference,
    DeletedDataFileRows,
    ChangeSplit,
    Manifest,
    FilesSplit,
    RewriteSplit,
    ReadSplit,
}

#[derive(Clone, Copy)]
enum WireValue {
    Varint,
    Fixed32,
    Fixed64,
    Scalar,
    Message(Schema),
    PackedVarint,
    MapI32String,
    MapStringString,
}

#[derive(Clone, Copy)]
struct FieldRule {
    value: WireValue,
    repeated: bool,
    oneof: bool,
}

const fn singular(value: WireValue) -> FieldRule {
    FieldRule {
        value,
        repeated: false,
        oneof: false,
    }
}
const fn repeated(value: WireValue) -> FieldRule {
    FieldRule {
        value,
        repeated: true,
        oneof: false,
    }
}
const fn oneof(value: WireValue) -> FieldRule {
    FieldRule {
        value,
        repeated: false,
        oneof: true,
    }
}

fn field_rule(schema: Schema, field: u32) -> Option<FieldRule> {
    use Schema::*;
    use WireValue::*;
    let rule = match schema {
        ValueType => match field {
            1 => singular(Varint),
            2..=4 => singular(Varint),
            _ => return None,
        },
        Decimal => match field {
            1 => singular(Scalar),
            2 | 3 => singular(Varint),
            _ => return None,
        },
        Value => match field {
            1..=3 | 7..=12 | 17..=19 => oneof(Varint),
            4 => oneof(Fixed32),
            5 => oneof(Fixed64),
            6 => oneof(Message(Decimal)),
            13..=16 => oneof(Scalar),
            _ => return None,
        },
        Bound => match field {
            1 => singular(Varint),
            2 => singular(Message(Value)),
            _ => return None,
        },
        Range => match field {
            1 | 2 => singular(Message(Bound)),
            _ => return None,
        },
        ValueSet => match field {
            1 => singular(Message(ValueType)),
            2 => repeated(Message(Range)),
            _ => return None,
        },
        Domain => match field {
            1 => singular(Message(ValueSet)),
            2 => singular(Varint),
            _ => return None,
        },
        ColumnDomain => match field {
            1 => singular(Message(Column)),
            2 => singular(Message(Domain)),
            _ => return None,
        },
        TupleDomain => match field {
            1 => singular(Varint),
            2 => repeated(Message(ColumnDomain)),
            _ => return None,
        },
        SchemaName => match field {
            1 | 2 => singular(Scalar),
            _ => return None,
        },
        Identity => match field {
            1 | 3 => singular(Varint),
            2 => singular(Scalar),
            4 => repeated(Message(Identity)),
            _ => return None,
        },
        Column => match field {
            1 => singular(Message(Identity)),
            2 | 4 | 6 => singular(Scalar),
            3 => repeated(PackedVarint),
            5 => singular(Varint),
            7 | 8 => singular(Scalar),
            _ => return None,
        },
        Transaction => match field {
            1 => singular(Varint),
            2 => singular(Scalar),
            _ => return None,
        },
        ReadView => match field {
            1 => singular(Message(Transaction)),
            _ => return None,
        },
        Pinned => match field {
            1 => repeated(Scalar),
            _ => return None,
        },
        ReadDomain => match field {
            1..=3 | 5 => singular(Scalar),
            4 => singular(Varint),
            6 | 7 => repeated(MapI32String),
            _ => return None,
        },
        CowSourceProof => match field {
            1..=3 => singular(Scalar),
            4 => repeated(Message(Column)),
            _ => return None,
        },
        Table => match field {
            1 => singular(Message(SchemaName)),
            2 | 4 | 6 | 9 => singular(Varint),
            3 | 11 | 12 => singular(Scalar),
            5 | 16 | 18 => repeated(MapI32String),
            7 | 8 => singular(Message(TupleDomain)),
            10 => repeated(Message(Column)),
            13 => repeated(MapStringString),
            14 => singular(Message(Pinned)),
            15 => singular(Message(ReadDomain)),
            17 => singular(Message(CowSourceProof)),
            _ => return None,
        },
        TableFunction => match field {
            1 => singular(Message(SchemaName)),
            2 | 4 => singular(Scalar),
            3 => repeated(Message(Column)),
            5 | 6 => singular(Varint),
            _ => return None,
        },
        ChangeWindowHandle => match field {
            8 | 9 => singular(Message(ReadDomain)),
            1 => singular(Message(SchemaName)),
            2 | 4 => singular(Scalar),
            3 => repeated(Message(Column)),
            5 | 6 => singular(Varint),
            7 => repeated(MapI32String),
            _ => return None,
        },
        SystemTable => match field {
            1 => singular(Message(SchemaName)),
            2 | 5 => singular(Varint),
            3 | 4 => singular(Scalar),
            _ => return None,
        },
        Optimize => match field {
            1 => singular(Message(Table)),
            2 => singular(Varint),
            _ => return None,
        },
        Artifact => match field {
            1 | 2 => singular(Scalar),
            _ => return None,
        },
        RewriteHandle => match field {
            1 => singular(Message(Table)),
            2 => singular(Message(Artifact)),
            3 => singular(Scalar),
            _ => return None,
        },
        TableExecute => match field {
            1 => singular(Message(SchemaName)),
            2 => singular(Varint),
            3 => singular(Scalar),
            10 => oneof(Message(Optimize)),
            11 => oneof(Message(RewriteHandle)),
            _ => return None,
        },
        Insert => match field {
            1 => singular(Message(SchemaName)),
            2 | 3 => singular(Scalar),
            4 | 5 => singular(Varint),
            _ => return None,
        },
        Merge => match field {
            1 => singular(Message(Table)),
            2 => singular(Message(Insert)),
            _ => return None,
        },
        ReadTable => match field {
            10 => oneof(Message(Table)),
            11 => oneof(Message(TableFunction)),
            12 => oneof(Message(ChangeWindowHandle)),
            13 => oneof(Message(SystemTable)),
            14 => oneof(Message(TableExecute)),
            15 => oneof(Message(Merge)),
            _ => return None,
        },
        Decryption => match field {
            1 | 2 => singular(Scalar),
            _ => return None,
        },
        Delete => match field {
            1 | 3 | 4 | 5 | 7..=11 | 14 => singular(Varint),
            2 | 13 | 15 => singular(Scalar),
            6 => repeated(PackedVarint),
            12 => singular(Message(Decryption)),
            _ => return None,
        },
        DataSplit => match field {
            14 => singular(Message(ReadDomain)),
            1 | 8 => singular(Scalar),
            2..=7 | 11 | 12 => singular(Varint),
            9 => repeated(Message(Delete)),
            10 => singular(Message(TupleDomain)),
            13 => singular(Message(Decryption)),
            _ => return None,
        },
        TableChangesSplit => match field {
            1..=4 | 6..=11 => singular(Varint),
            5 | 12 => singular(Scalar),
            13 => singular(Message(Decryption)),
            _ => return None,
        },
        AddedRows => match field {
            1 => singular(Message(DataSplit)),
            2 => repeated(PackedVarint),
            _ => return None,
        },
        VisibilityDifference => match field {
            1 => singular(Message(DataSplit)),
            2 | 3 => repeated(Message(Delete)),
            _ => return None,
        },
        DeletedDataFileRows => match field {
            1 => singular(Message(DataSplit)),
            _ => return None,
        },
        ChangeSplit => match field {
            10 => oneof(Message(AddedRows)),
            14 => oneof(Message(VisibilityDifference)),
            13 => oneof(Message(DeletedDataFileRows)),
            _ => return None,
        },
        Manifest => match field {
            1 | 15 => singular(Scalar),
            2..=14 => singular(Varint),
            _ => return None,
        },
        FilesSplit => match field {
            1 => singular(Message(Manifest)),
            2 | 3 | 5..=7 => singular(Scalar),
            4 => repeated(MapI32String),
            _ => return None,
        },
        RewriteSplit => match field {
            1 | 4 => singular(Scalar),
            2 | 3 => singular(Varint),
            5 => repeated(Message(Delete)),
            _ => return None,
        },
        ReadSplit => match field {
            10 => oneof(Message(DataSplit)),
            11 => oneof(Message(TableChangesSplit)),
            12 => oneof(Message(ChangeSplit)),
            13 => oneof(Message(FilesSplit)),
            14 => oneof(Message(RewriteSplit)),
            _ => return None,
        },
    };
    Some(rule)
}

fn scan_message(
    mut input: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    path: ConnectorFieldPath,
    schema: Schema,
    depth: usize,
) -> Result<(), ConnectorCodecError> {
    context.ledger().check_depth(depth)?;
    let mut seen = BTreeSet::new();
    let mut seen_oneof = false;
    let mut cow_metadata_members = 0_usize;
    let mut map_keys: BTreeMap<u32, BTreeSet<Vec<u8>>> = BTreeMap::new();
    while !input.is_empty() {
        context.ledger().charge_items(1)?;
        let key = read_varint(&mut input, context, &path)?;
        let field = u32::try_from(key >> 3).map_err(|_| malformed(&path))?;
        let wire = u8::try_from(key & 7).map_err(|_| malformed(&path))?;
        context.observe_compile_step()?;
        let field_path = path.field(format!("field_{field}"));
        let rule = field_rule(schema, field).ok_or_else(|| {
            ConnectorCodecError::new(
                field_path.clone(),
                ConnectorCodecErrorKind::UnknownField,
                "Iceberg private payload contains an unknown field",
            )
        })?;
        let expected = match rule.value {
            WireValue::Varint => 0,
            WireValue::Fixed64 => 1,
            WireValue::Scalar
            | WireValue::Message(_)
            | WireValue::PackedVarint
            | WireValue::MapI32String
            | WireValue::MapStringString => 2,
            WireValue::Fixed32 => 5,
        };
        if wire != expected {
            return Err(ConnectorCodecError::new(
                field_path,
                ConnectorCodecErrorKind::InvalidValue,
                format!("Iceberg private field uses wire type {wire}, expected {expected}"),
            ));
        }
        if (!rule.repeated && !seen.insert(field))
            || (rule.oneof && std::mem::replace(&mut seen_oneof, true))
        {
            return Err(ConnectorCodecError::new(
                path.field(format!("field_{field}")),
                ConnectorCodecErrorKind::DuplicateField,
                "Iceberg private payload repeats a singular or oneof field",
            ));
        }
        if matches!(schema, Schema::CowSourceProof) && field == 4 {
            cow_metadata_members += 1;
            if cow_metadata_members
                > crate::typed_read::ALWAYS_BOUND_METADATA_COLUMNS.len()
                    + crate::typed_read::ROW_LINEAGE_METADATA_COLUMNS.len()
            {
                return Err(codec_error(
                    "iceberg_table.frozen_cow_source.metadata_columns",
                    ConnectorCodecErrorKind::InvalidValue,
                    "Iceberg COW metadata proof exceeds its original four-field shape",
                ));
            }
        }
        match rule.value {
            WireValue::Varint => {
                read_varint(&mut input, context, &path)?;
            }
            WireValue::Fixed64 => {
                take(&mut input, 8, &path)?;
            }
            WireValue::Fixed32 => {
                take(&mut input, 4, &path)?;
            }
            WireValue::Scalar => {
                let len = usize::try_from(read_varint(&mut input, context, &path)?)
                    .map_err(|_| malformed(&path))?;
                context.ledger().charge_scalar(len)?;
                take(&mut input, len, &path)?;
            }
            WireValue::PackedVarint => {
                let len = usize::try_from(read_varint(&mut input, context, &path)?)
                    .map_err(|_| malformed(&path))?;
                let mut packed = take(&mut input, len, &path)?;
                while !packed.is_empty() {
                    context.ledger().charge_items(1)?;
                    read_varint(&mut packed, context, &path)?;
                    context.observe_compile_step()?;
                }
            }
            WireValue::Message(child) => {
                // The DTO proof and its inline column elements survive Prost
                // decode separately from the later typed proof representation.
                // Preflight charges that real raw extent before Prost allocates.
                if matches!(schema, Schema::Table) && field == 17 {
                    context.ledger().charge_retained(
                        size_of::<dto::IcebergCowSourceProof>()
                            + (crate::typed_read::ALWAYS_BOUND_METADATA_COLUMNS.len()
                                + crate::typed_read::ROW_LINEAGE_METADATA_COLUMNS.len())
                                * size_of::<dto::IcebergColumnHandle>(),
                    )?;
                }
                let len = usize::try_from(read_varint(&mut input, context, &path)?)
                    .map_err(|_| malformed(&path))?;
                let nested = take(&mut input, len, &path)?;
                scan_message(
                    nested,
                    context,
                    path.field(format!("field_{field}")),
                    child,
                    depth + 1,
                )?;
            }
            WireValue::MapI32String | WireValue::MapStringString => {
                if matches!(schema, Schema::Table) && matches!(field, 16 | 18) {
                    // A conservative bound covers one field-ID/domain entry,
                    // including a sparsely occupied BTree node, before decode.
                    context.ledger().charge_retained(256)?;
                }
                let len = usize::try_from(read_varint(&mut input, context, &path)?)
                    .map_err(|_| malformed(&path))?;
                let entry = take(&mut input, len, &path)?;
                let key = scan_map_entry(
                    entry,
                    context,
                    &path,
                    matches!(rule.value, WireValue::MapStringString),
                    depth + 1,
                )?;
                context.flush_compile_control()?;
                let duplicate = !map_keys.entry(field).or_default().insert(key);
                context.observe_compile_step()?;
                if duplicate {
                    return Err(ConnectorCodecError::new(
                        path.field(format!("field_{field}")),
                        ConnectorCodecErrorKind::DuplicateField,
                        "Iceberg private map repeats a key",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn scan_map_entry(
    mut input: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    path: &ConnectorFieldPath,
    string_key: bool,
    depth: usize,
) -> Result<Vec<u8>, ConnectorCodecError> {
    context.ledger().check_depth(depth)?;
    let mut key_value = None;
    let mut value_seen = false;
    while !input.is_empty() {
        context.ledger().charge_items(1)?;
        let raw_key = read_varint(&mut input, context, path)?;
        let field = u32::try_from(raw_key >> 3).map_err(|_| malformed(path))?;
        let wire = u8::try_from(raw_key & 7).map_err(|_| malformed(path))?;
        context.observe_compile_step()?;
        match field {
            1 if key_value.is_none()
                && ((!string_key && wire == 0) || (string_key && wire == 2)) =>
            {
                if string_key {
                    let len = usize::try_from(read_varint(&mut input, context, path)?)
                        .map_err(|_| malformed(path))?;
                    context.ledger().charge_scalar(len)?;
                    key_value = Some(copy_slice(take(&mut input, len, path)?, context)?);
                } else {
                    key_value = Some(
                        read_varint(&mut input, context, path)?
                            .to_le_bytes()
                            .to_vec(),
                    );
                }
            }
            2 if !value_seen && wire == 2 => {
                value_seen = true;
                let len = usize::try_from(read_varint(&mut input, context, path)?)
                    .map_err(|_| malformed(path))?;
                context.ledger().charge_scalar(len)?;
                take(&mut input, len, path)?;
            }
            1 | 2 => {
                return Err(ConnectorCodecError::new(
                    path.clone(),
                    ConnectorCodecErrorKind::DuplicateField,
                    "Iceberg private map entry repeats a field or uses the wrong wire type",
                ));
            }
            _ => {
                return Err(ConnectorCodecError::new(
                    path.clone(),
                    ConnectorCodecErrorKind::UnknownField,
                    "Iceberg private map entry contains an unknown field",
                ));
            }
        }
    }
    if !value_seen {
        return Err(ConnectorCodecError::new(
            path.clone(),
            ConnectorCodecErrorKind::MissingField,
            "Iceberg private map entry requires a value",
        ));
    }
    Ok(key_value.unwrap_or_else(|| {
        if string_key {
            Vec::new()
        } else {
            0_u64.to_le_bytes().to_vec()
        }
    }))
}

fn read_varint(
    input: &mut &[u8],
    context: &mut ConnectorDecodeContext<'_>,
    path: &ConnectorFieldPath,
) -> Result<u64, ConnectorCodecError> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let Some((&byte, rest)) = input.split_first() else {
            return Err(malformed(path));
        };
        *input = rest;
        let overflow = shift == 63 && byte > 1;
        if !overflow {
            value |= u64::from(byte & 0x7f) << shift;
        }
        context.observe_compile_step()?;
        if overflow {
            return Err(malformed(path));
        }
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(malformed(path))
}

fn take<'a>(
    input: &mut &'a [u8],
    length: usize,
    path: &ConnectorFieldPath,
) -> Result<&'a [u8], ConnectorCodecError> {
    if input.len() < length {
        return Err(malformed(path));
    }
    let (value, rest) = input.split_at(length);
    *input = rest;
    Ok(value)
}

fn malformed(path: &ConnectorFieldPath) -> ConnectorCodecError {
    ConnectorCodecError::new(
        path.clone(),
        ConnectorCodecErrorKind::InvalidValue,
        "malformed Iceberg private protobuf structure",
    )
}

fn codec_error(
    path: &'static str,
    kind: ConnectorCodecErrorKind,
    detail: impl AsRef<str>,
) -> ConnectorCodecError {
    ConnectorCodecError::new(ConnectorFieldPath::root(path), kind, detail)
}

fn domain_error(path: &'static str, error: impl std::fmt::Display) -> ConnectorCodecError {
    codec_error(
        path,
        ConnectorCodecErrorKind::InvalidValue,
        error.to_string(),
    )
}

fn encode_value_type(value_type: ConnectorValueType) -> dto::ValueType {
    let (kind, decimal_precision, decimal_scale, fixed_length) = match value_type {
        ConnectorValueType::Boolean => (dto::ValueTypeKind::Boolean, None, None, None),
        ConnectorValueType::TinyInt => (dto::ValueTypeKind::TinyInt, None, None, None),
        ConnectorValueType::SmallInt => (dto::ValueTypeKind::SmallInt, None, None, None),
        ConnectorValueType::Integer => (dto::ValueTypeKind::Integer, None, None, None),
        ConnectorValueType::BigInt => (dto::ValueTypeKind::BigInt, None, None, None),
        ConnectorValueType::Real => (dto::ValueTypeKind::Real, None, None, None),
        ConnectorValueType::Double => (dto::ValueTypeKind::Double, None, None, None),
        ConnectorValueType::Decimal { precision, scale } => (
            dto::ValueTypeKind::Decimal,
            Some(u32::from(precision)),
            Some(i32::from(scale)),
            None,
        ),
        ConnectorValueType::Date => (dto::ValueTypeKind::Date, None, None, None),
        ConnectorValueType::TimeMicros => (dto::ValueTypeKind::TimeMicros, None, None, None),
        ConnectorValueType::TimestampMicros => {
            (dto::ValueTypeKind::TimestampMicros, None, None, None)
        }
        ConnectorValueType::TimestampMillis => {
            (dto::ValueTypeKind::TimestampMillis, None, None, None)
        }
        ConnectorValueType::TimestampTzMicros => {
            (dto::ValueTypeKind::TimestampTzMicros, None, None, None)
        }
        ConnectorValueType::TimestampNanos => {
            (dto::ValueTypeKind::TimestampNanos, None, None, None)
        }
        ConnectorValueType::TimestampTzNanos => {
            (dto::ValueTypeKind::TimestampTzNanos, None, None, None)
        }
        ConnectorValueType::Varchar => (dto::ValueTypeKind::Varchar, None, None, None),
        ConnectorValueType::Varbinary => (dto::ValueTypeKind::Varbinary, None, None, None),
        ConnectorValueType::Uuid => (dto::ValueTypeKind::Uuid, None, None, None),
        ConnectorValueType::Fixed { length } => {
            (dto::ValueTypeKind::Fixed, None, None, Some(length))
        }
        ConnectorValueType::NonComparable => (dto::ValueTypeKind::NonComparable, None, None, None),
    };
    dto::ValueType {
        kind: kind as i32,
        decimal_precision,
        decimal_scale,
        fixed_length,
    }
}

fn decode_value_type(
    raw: &dto::ValueType,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<ConnectorValueType, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let kind = dto::ValueTypeKind::try_from(raw.kind).map_err(|_| {
            codec_error(
                "value_type.kind",
                ConnectorCodecErrorKind::InvalidEnum,
                "unknown Iceberg predicate value type",
            )
        })?;
        let value_type = match kind {
            dto::ValueTypeKind::Unspecified => {
                return Err(codec_error(
                    "value_type.kind",
                    ConnectorCodecErrorKind::InvalidEnum,
                    "Iceberg predicate value type must be specified",
                ));
            }
            dto::ValueTypeKind::Decimal => {
                let precision = raw.decimal_precision.ok_or_else(|| {
                    codec_error(
                        "value_type.decimal_precision",
                        ConnectorCodecErrorKind::MissingField,
                        "decimal precision is required",
                    )
                })?;
                let scale = raw.decimal_scale.ok_or_else(|| {
                    codec_error(
                        "value_type.decimal_scale",
                        ConnectorCodecErrorKind::MissingField,
                        "decimal scale is required",
                    )
                })?;
                if precision == 0 || precision > 38 || scale < 0 || scale > precision as i32 {
                    return Err(codec_error(
                        "value_type",
                        ConnectorCodecErrorKind::InvalidValue,
                        "decimal precision or scale is out of range",
                    ));
                }
                if raw.fixed_length.is_some() {
                    return Err(codec_error(
                        "value_type.fixed_length",
                        ConnectorCodecErrorKind::InconsistentFields,
                        "decimal type must not carry fixed length",
                    ));
                }
                ConnectorValueType::Decimal {
                    precision: precision as u8,
                    scale: scale as i8,
                }
            }
            dto::ValueTypeKind::Fixed => {
                let length = raw.fixed_length.ok_or_else(|| {
                    codec_error(
                        "value_type.fixed_length",
                        ConnectorCodecErrorKind::MissingField,
                        "fixed type length is required",
                    )
                })?;
                if length == 0 || length > 64 * 1024 {
                    return Err(codec_error(
                        "value_type.fixed_length",
                        ConnectorCodecErrorKind::InvalidValue,
                        "fixed type length is out of range",
                    ));
                }
                if raw.decimal_precision.is_some() || raw.decimal_scale.is_some() {
                    return Err(codec_error(
                        "value_type.decimal_precision",
                        ConnectorCodecErrorKind::InconsistentFields,
                        "fixed type must not carry decimal parameters",
                    ));
                }
                ConnectorValueType::Fixed { length }
            }
            simple => {
                if raw.decimal_precision.is_some()
                    || raw.decimal_scale.is_some()
                    || raw.fixed_length.is_some()
                {
                    return Err(codec_error(
                        "value_type",
                        ConnectorCodecErrorKind::InconsistentFields,
                        "simple value type carries unrelated parameters",
                    ));
                }
                match simple {
                    dto::ValueTypeKind::Boolean => ConnectorValueType::Boolean,
                    dto::ValueTypeKind::TinyInt => ConnectorValueType::TinyInt,
                    dto::ValueTypeKind::SmallInt => ConnectorValueType::SmallInt,
                    dto::ValueTypeKind::Integer => ConnectorValueType::Integer,
                    dto::ValueTypeKind::BigInt => ConnectorValueType::BigInt,
                    dto::ValueTypeKind::Real => ConnectorValueType::Real,
                    dto::ValueTypeKind::Double => ConnectorValueType::Double,
                    dto::ValueTypeKind::Date => ConnectorValueType::Date,
                    dto::ValueTypeKind::TimeMicros => ConnectorValueType::TimeMicros,
                    dto::ValueTypeKind::TimestampMicros => ConnectorValueType::TimestampMicros,
                    dto::ValueTypeKind::TimestampMillis => ConnectorValueType::TimestampMillis,
                    dto::ValueTypeKind::TimestampTzMicros => ConnectorValueType::TimestampTzMicros,
                    dto::ValueTypeKind::TimestampNanos => ConnectorValueType::TimestampNanos,
                    dto::ValueTypeKind::TimestampTzNanos => ConnectorValueType::TimestampTzNanos,
                    dto::ValueTypeKind::Varchar => ConnectorValueType::Varchar,
                    dto::ValueTypeKind::Varbinary => ConnectorValueType::Varbinary,
                    dto::ValueTypeKind::Uuid => ConnectorValueType::Uuid,
                    dto::ValueTypeKind::NonComparable => ConnectorValueType::NonComparable,
                    dto::ValueTypeKind::Unspecified
                    | dto::ValueTypeKind::Decimal
                    | dto::ValueTypeKind::Fixed => unreachable!("handled above"),
                }
            }
        };
        Ok(value_type)
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_value(value: &ConnectorValue) -> dto::Value {
    let value = match value {
        ConnectorValue::Boolean(value) => dto::value::Value::Boolean(*value),
        ConnectorValue::TinyInt(value) => dto::value::Value::TinyInt(i32::from(*value)),
        ConnectorValue::SmallInt(value) => dto::value::Value::SmallInt(i32::from(*value)),
        ConnectorValue::Integer(value) => dto::value::Value::Integer(*value),
        ConnectorValue::BigInt(value) => dto::value::Value::BigInt(*value),
        ConnectorValue::Real(value) => dto::value::Value::Real(*value),
        ConnectorValue::Double(value) => dto::value::Value::DoubleValue(*value),
        ConnectorValue::Decimal {
            unscaled,
            precision,
            scale,
        } => dto::value::Value::Decimal(dto::DecimalValue {
            unscaled: unscaled.to_be_bytes().to_vec(),
            precision: u32::from(*precision),
            scale: i32::from(*scale),
        }),
        ConnectorValue::Date(value) => dto::value::Value::Date(*value),
        ConnectorValue::TimeMicros(value) => dto::value::Value::TimeMicros(*value),
        ConnectorValue::TimestampMicros(value) => dto::value::Value::TimestampMicros(*value),
        ConnectorValue::TimestampMillis(value) => dto::value::Value::TimestampMillis(*value),
        ConnectorValue::TimestampTzMicros(value) => dto::value::Value::TimestampTzMicros(*value),
        ConnectorValue::TimestampNanos(value) => dto::value::Value::TimestampNanos(*value),
        ConnectorValue::TimestampTzNanos(value) => dto::value::Value::TimestampTzNanos(*value),
        ConnectorValue::Varchar(value) => dto::value::Value::Varchar(value.to_string()),
        ConnectorValue::Varbinary(value) => dto::value::Value::Varbinary(value.to_vec()),
        ConnectorValue::Uuid(value) => dto::value::Value::Uuid(value.to_vec()),
        ConnectorValue::Fixed(value) => dto::value::Value::Fixed(value.to_vec()),
    };
    dto::Value { value: Some(value) }
}

fn decode_value(
    raw: &dto::Value,
    expected: ConnectorValueType,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<ConnectorValue, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let raw = raw.value.as_ref().ok_or_else(|| {
            codec_error(
                "value",
                ConnectorCodecErrorKind::MissingField,
                "predicate value is required",
            )
        })?;
        let value = match raw {
            dto::value::Value::Boolean(value) => ConnectorValue::Boolean(*value),
            dto::value::Value::TinyInt(value) => {
                ConnectorValue::TinyInt(i8::try_from(*value).map_err(|_| {
                    codec_error(
                        "value.tiny_int",
                        ConnectorCodecErrorKind::InvalidValue,
                        "tiny integer is out of range",
                    )
                })?)
            }
            dto::value::Value::SmallInt(value) => {
                ConnectorValue::SmallInt(i16::try_from(*value).map_err(|_| {
                    codec_error(
                        "value.small_int",
                        ConnectorCodecErrorKind::InvalidValue,
                        "small integer is out of range",
                    )
                })?)
            }
            dto::value::Value::Integer(value) => ConnectorValue::Integer(*value),
            dto::value::Value::BigInt(value) => ConnectorValue::BigInt(*value),
            dto::value::Value::Real(value) => ConnectorValue::Real(*value),
            dto::value::Value::DoubleValue(value) => ConnectorValue::Double(*value),
            dto::value::Value::Decimal(value) => {
                if value.unscaled.len() != 16 {
                    return Err(codec_error(
                        "value.decimal.unscaled",
                        ConnectorCodecErrorKind::InvalidValue,
                        "decimal unscaled value must contain 16 bytes",
                    ));
                }
                let mut unscaled = [0_u8; 16];
                unscaled.copy_from_slice(&value.unscaled);
                ConnectorValue::try_decimal(
                    i128::from_be_bytes(unscaled),
                    u8::try_from(value.precision).map_err(|_| {
                        codec_error(
                            "value.decimal.precision",
                            ConnectorCodecErrorKind::InvalidValue,
                            "decimal precision is out of range",
                        )
                    })?,
                    i8::try_from(value.scale).map_err(|_| {
                        codec_error(
                            "value.decimal.scale",
                            ConnectorCodecErrorKind::InvalidValue,
                            "decimal scale is out of range",
                        )
                    })?,
                )
                .map_err(|error| domain_error("value.decimal", error))?
            }
            dto::value::Value::Date(value) => ConnectorValue::Date(*value),
            dto::value::Value::TimeMicros(value) => ConnectorValue::TimeMicros(*value),
            dto::value::Value::TimestampMicros(value) => ConnectorValue::TimestampMicros(*value),
            dto::value::Value::TimestampMillis(value) => ConnectorValue::TimestampMillis(*value),
            dto::value::Value::TimestampTzMicros(value) => {
                ConnectorValue::TimestampTzMicros(*value)
            }
            dto::value::Value::TimestampNanos(value) => ConnectorValue::TimestampNanos(*value),
            dto::value::Value::TimestampTzNanos(value) => ConnectorValue::TimestampTzNanos(*value),
            dto::value::Value::Varchar(value) => {
                ConnectorValue::Varchar(Arc::from(copy_string(value, context)?))
            }
            dto::value::Value::Varbinary(value) => {
                ConnectorValue::Varbinary(Arc::from(copy_slice(value, context)?))
            }
            dto::value::Value::Uuid(value) => {
                if value.len() != 16 {
                    return Err(codec_error(
                        "value.uuid",
                        ConnectorCodecErrorKind::InvalidValue,
                        "UUID value must contain 16 bytes",
                    ));
                }
                let mut uuid = [0_u8; 16];
                uuid.copy_from_slice(value);
                ConnectorValue::Uuid(uuid)
            }
            dto::value::Value::Fixed(value) => {
                ConnectorValue::Fixed(Arc::from(copy_slice(value, context)?))
            }
        };
        if value.value_type() != expected || value.payload_bytes() > 64 * 1024 {
            return Err(codec_error(
                "value",
                ConnectorCodecErrorKind::InconsistentFields,
                "predicate value does not match its exact declared type",
            ));
        }
        Ok(value)
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_identity(value: &ColumnIdentity) -> dto::ColumnIdentity {
    dto::ColumnIdentity {
        field_id: value.field_id(),
        name: value.name().to_string(),
        category: match value.category() {
            ColumnIdentityCategory::Primitive => dto::ColumnIdentityCategory::Primitive,
            ColumnIdentityCategory::Struct => dto::ColumnIdentityCategory::Struct,
            ColumnIdentityCategory::Array => dto::ColumnIdentityCategory::Array,
            ColumnIdentityCategory::Map => dto::ColumnIdentityCategory::Map,
        } as i32,
        children: value.children().iter().map(encode_identity).collect(),
    }
}

fn decode_identity(
    raw: &dto::ColumnIdentity,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<ColumnIdentity, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let category = match dto::ColumnIdentityCategory::try_from(raw.category) {
            Ok(dto::ColumnIdentityCategory::Primitive) => ColumnIdentityCategory::Primitive,
            Ok(dto::ColumnIdentityCategory::Struct) => ColumnIdentityCategory::Struct,
            Ok(dto::ColumnIdentityCategory::Array) => ColumnIdentityCategory::Array,
            Ok(dto::ColumnIdentityCategory::Map) => ColumnIdentityCategory::Map,
            Ok(dto::ColumnIdentityCategory::Unspecified) | Err(_) => {
                return Err(codec_error(
                    "column_identity.category",
                    ConnectorCodecErrorKind::InvalidEnum,
                    "Iceberg column identity category must be known",
                ));
            }
        };
        let children = convert_slice(&raw.children, context, decode_identity)?;
        {
            let opaque_arguments = (raw.field_id, &raw.name, category, children);
            observe_opaque(context, || {
                ColumnIdentity::try_new(
                    opaque_arguments.0,
                    opaque_arguments.1,
                    opaque_arguments.2,
                    opaque_arguments.3,
                )
            })?
        }
        .map_err(|error| domain_error("column_identity", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_column(value: &IcebergColumnHandle) -> dto::IcebergColumnHandle {
    dto::IcebergColumnHandle {
        base_column_identity: Some(encode_identity(value.base_column_identity())),
        base_type_json: value.base_type_json().to_string(),
        field_id_path: value.field_id_path().to_vec(),
        type_json: value.type_json().to_string(),
        nullable: value.nullable(),
        comment: value.comment().map(str::to_string),
        scalar_integer_domain: value
            .scalar_integer_domain()
            .map(|domain| domain.name().to_string()),
        nominal_read_domain: value
            .nominal_read_domain()
            .map(|domain| domain.name().to_string()),
    }
}

fn decode_column(
    raw: &dto::IcebergColumnHandle,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergColumnHandle, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let identity = raw.base_column_identity.as_ref().ok_or_else(|| {
            codec_error(
                "iceberg_column.base_column_identity",
                ConnectorCodecErrorKind::MissingField,
                "Iceberg column requires its base identity",
            )
        })?;
        let parameters = IcebergColumnHandleParams {
            base_column_identity: decode_identity(identity, context)?,
            base_type_json: copy_string(&raw.base_type_json, context)?,
            field_id_path: copy_slice(&raw.field_id_path, context)?,
            type_json: copy_string(&raw.type_json, context)?,
            nullable: raw.nullable,
            comment: raw
                .comment
                .as_deref()
                .map(|value| copy_string(value, context))
                .transpose()?,
        };
        let column = observe_opaque(context, || IcebergColumnHandle::try_new(parameters))?
            .map_err(|error| domain_error("iceberg_column", error))?;
        let domain = raw
            .scalar_integer_domain
            .as_deref()
            .map(|value| {
                observe_opaque(context, || {
                    crate::scalar_integer_domain::ScalarIntegerDomain::parse(value)
                })?
                .map_err(|error| domain_error("iceberg_column", error))
            })
            .transpose()?;
        let column = observe_opaque(context, || column.with_scalar_integer_domain(domain))?
            .map_err(|error| domain_error("iceberg_column", error))?;
        let nominal = raw
            .nominal_read_domain
            .as_deref()
            .map(|value| {
                observe_opaque(context, || {
                    crate::nominal_read_domain::NominalReadDomain::parse(value)
                })?
                .map_err(|error| domain_error("iceberg_column", error))
            })
            .transpose()?;
        observe_opaque(context, || column.with_nominal_read_domain(nominal))?
            .map_err(|error| domain_error("iceberg_column", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_tuple_domain(value: &TupleDomain<IcebergColumnHandle>) -> dto::TupleDomain {
    match value.domains() {
        None => dto::TupleDomain {
            none: true,
            column_domains: Vec::new(),
        },
        Some(domains) => dto::TupleDomain {
            none: false,
            column_domains: domains
                .iter()
                .map(|(column, domain)| dto::ColumnDomain {
                    column: Some(encode_column(column)),
                    domain: Some(encode_domain(domain)),
                })
                .collect(),
        },
    }
}

fn decode_tuple_domain(
    raw: &dto::TupleDomain,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<TupleDomain<IcebergColumnHandle>, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        if raw.none {
            if !raw.column_domains.is_empty() {
                return Err(codec_error(
                    "tuple_domain.column_domains",
                    ConnectorCodecErrorKind::InconsistentFields,
                    "an empty tuple domain must carry no columns",
                ));
            }
            return Ok(TupleDomain::none());
        }
        let mut domains = BTreeMap::new();
        for entry in &raw.column_domains {
            let column = decode_column(
                entry.column.as_ref().ok_or_else(|| {
                    codec_error(
                        "tuple_domain.column",
                        ConnectorCodecErrorKind::MissingField,
                        "column domain requires a column",
                    )
                })?,
                context,
            )?;
            let domain = decode_domain(
                entry.domain.as_ref().ok_or_else(|| {
                    codec_error(
                        "tuple_domain.domain",
                        ConnectorCodecErrorKind::MissingField,
                        "column domain requires a value domain",
                    )
                })?,
                context,
            )?;
            context.flush_compile_control()?;
            let duplicate = domains.insert(column, domain).is_some();
            context.observe_compile_step()?;
            if duplicate {
                return Err(codec_error(
                    "tuple_domain.column",
                    ConnectorCodecErrorKind::DuplicateField,
                    "tuple domain repeats an Iceberg column",
                ));
            }
        }
        {
            let opaque_arguments = (domains,);
            observe_opaque(context, || {
                TupleDomain::with_column_domains(opaque_arguments.0)
            })?
        }
        .map_err(|error| domain_error("tuple_domain", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_domain(value: &Domain) -> dto::Domain {
    dto::Domain {
        values: Some(dto::ValueSet {
            value_type: Some(encode_value_type(value.values().value_type())),
            ranges: value.values().ranges().iter().map(encode_range).collect(),
        }),
        null_allowed: value.null_allowed(),
    }
}

fn decode_domain(
    raw: &dto::Domain,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Domain, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let raw_values = raw.values.as_ref().ok_or_else(|| {
            codec_error(
                "domain.values",
                ConnectorCodecErrorKind::MissingField,
                "domain requires a value set",
            )
        })?;
        let value_type = decode_value_type(
            raw_values.value_type.as_ref().ok_or_else(|| {
                codec_error(
                    "value_set.value_type",
                    ConnectorCodecErrorKind::MissingField,
                    "value set requires an exact type",
                )
            })?,
            context,
        )?;
        let ranges = convert_slice(&raw_values.ranges, context, |range, context| {
            decode_range(range, value_type, context)
        })?;
        let values = {
            let opaque_arguments = (value_type, ranges);
            observe_opaque(context, || {
                ValueSet::of_ranges(opaque_arguments.0, opaque_arguments.1)
            })?
        }
        .map_err(|error| domain_error("value_set", error))?;
        Ok(Domain::new(values, raw.null_allowed))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_range(value: &Range) -> dto::Range {
    dto::Range {
        low: Some(encode_bound(value.low())),
        high: Some(encode_bound(value.high())),
    }
}

fn decode_range(
    raw: &dto::Range,
    value_type: ConnectorValueType,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Range, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        {
            let opaque_arguments = (
                value_type,
                decode_bound(
                    raw.low.as_ref().ok_or_else(|| {
                        codec_error(
                            "range.low",
                            ConnectorCodecErrorKind::MissingField,
                            "range requires its low bound",
                        )
                    })?,
                    value_type,
                    context,
                )?,
                decode_bound(
                    raw.high.as_ref().ok_or_else(|| {
                        codec_error(
                            "range.high",
                            ConnectorCodecErrorKind::MissingField,
                            "range requires its high bound",
                        )
                    })?,
                    value_type,
                    context,
                )?,
            );
            observe_opaque(context, || {
                Range::try_new(opaque_arguments.0, opaque_arguments.1, opaque_arguments.2)
            })?
        }
        .map_err(|error| domain_error("range", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_bound(value: &Bound) -> dto::Bound {
    match value {
        Bound::Unbounded => dto::Bound {
            kind: dto::BoundKind::Unbounded as i32,
            value: None,
        },
        Bound::Inclusive(value) => dto::Bound {
            kind: dto::BoundKind::Inclusive as i32,
            value: Some(encode_value(value)),
        },
        Bound::Exclusive(value) => dto::Bound {
            kind: dto::BoundKind::Exclusive as i32,
            value: Some(encode_value(value)),
        },
    }
}

fn decode_bound(
    raw: &dto::Bound,
    value_type: ConnectorValueType,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Bound, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| match dto::BoundKind::try_from(raw.kind) {
        Ok(dto::BoundKind::Unbounded) => {
            if raw.value.is_some() {
                return Err(codec_error(
                    "bound.value",
                    ConnectorCodecErrorKind::InconsistentFields,
                    "unbounded range bound must not carry a value",
                ));
            }
            Ok(Bound::Unbounded)
        }
        Ok(dto::BoundKind::Inclusive) => Ok(Bound::Inclusive(decode_value(
            raw.value.as_ref().ok_or_else(|| {
                codec_error(
                    "bound.value",
                    ConnectorCodecErrorKind::MissingField,
                    "inclusive range bound requires a value",
                )
            })?,
            value_type,
            context,
        )?)),
        Ok(dto::BoundKind::Exclusive) => Ok(Bound::Exclusive(decode_value(
            raw.value.as_ref().ok_or_else(|| {
                codec_error(
                    "bound.value",
                    ConnectorCodecErrorKind::MissingField,
                    "exclusive range bound requires a value",
                )
            })?,
            value_type,
            context,
        )?)),
        Ok(dto::BoundKind::Unspecified) | Err(_) => Err(codec_error(
            "bound.kind",
            ConnectorCodecErrorKind::InvalidEnum,
            "range bound kind must be known",
        )),
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_schema_name(value: &SchemaTableName) -> dto::SchemaTableName {
    dto::SchemaTableName {
        schema_name: value.schema_name().to_string(),
        table_name: value.table_name().to_string(),
    }
}

fn decode_schema_name(
    raw: &dto::SchemaTableName,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<SchemaTableName, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        {
            let opaque_arguments = (&raw.schema_name, &raw.table_name);
            observe_opaque(context, || {
                SchemaTableName::try_new(opaque_arguments.0, opaque_arguments.1)
            })?
        }
        .map_err(|error| domain_error("schema_table_name", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_transaction(value: &HiveTransactionHandle) -> dto::HiveTransactionHandle {
    dto::HiveTransactionHandle {
        auto_commit: value.auto_commit(),
        uuid: value.uuid().to_vec(),
    }
}

fn decode_transaction(
    raw: &dto::HiveTransactionHandle,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<HiveTransactionHandle, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let uuid: [u8; 16] = raw.uuid.as_slice().try_into().map_err(|_| {
            codec_error(
                "read_view.transaction.uuid",
                ConnectorCodecErrorKind::InvalidValue,
                "Iceberg transaction UUID must contain exactly 16 bytes",
            )
        })?;
        Ok(HiveTransactionHandle::new(raw.auto_commit, uuid))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_table(value: &IcebergTableHandle) -> dto::IcebergTableHandle {
    dto::IcebergTableHandle {
        nominal_read_domains: value
            .nominal_read_domains()
            .iter()
            .map(|(id, domain)| (*id, domain.name().to_string()))
            .collect(),
        scalar_integer_domains: value
            .scalar_integer_domains()
            .iter()
            .map(|(id, domain)| (*id, domain.name().to_string()))
            .collect(),
        read_domain: value.read_domain().map(|domain| encode_read_domain(domain)),
        schema_table_name: Some(encode_schema_name(value.schema_table_name())),
        snapshot_id: value.snapshot_id(),
        frozen_cow_source: value
            .frozen_cow_source()
            .map(crate::typed_read::cow_source_proof::IcebergCowSourceProof::to_proto),
        table_schema_json: value.table_schema_json().to_string(),
        spec_id: value.spec_id(),
        partition_spec_jsons: value
            .partition_spec_jsons()
            .iter()
            .map(|(key, value)| (*key, value.clone()))
            .collect(),
        format_version: value.format_version(),
        unenforced_predicate: Some(encode_tuple_domain(value.unenforced_predicate())),
        enforced_predicate: Some(encode_tuple_domain(value.enforced_predicate())),
        limit: value.limit(),
        projected_columns: value
            .projected_columns()
            .iter()
            .map(encode_column)
            .collect(),
        name_mapping_json: value.name_mapping_json().map(str::to_string),
        table_location: value.table_location().to_string(),
        storage_properties: value
            .storage_properties()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        pinned_data_files: value
            .pinned_data_files()
            .map(|files| dto::IcebergPinnedDataFileSet {
                paths: files.paths().iter().map(ToString::to_string).collect(),
            }),
    }
}

fn decode_cow_source_proof(
    raw: &dto::IcebergCowSourceProof,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<crate::typed_read::cow_source_proof::IcebergCowSourceProof, ConnectorCodecError> {
    use crate::typed_read::cow_source_proof::IcebergCowSourceProof;
    let result = (|| {
        let digest = raw.source_digest.as_slice().try_into().map_err(|_| {
            codec_error(
                "iceberg_table.frozen_cow_source.source_digest",
                ConnectorCodecErrorKind::InvalidValue,
                "Iceberg COW source digest must be 32 bytes",
            )
        })?;
        let base = raw.signed_base.as_slice().try_into().map_err(|_| {
            codec_error(
                "iceberg_table.frozen_cow_source.signed_base",
                ConnectorCodecErrorKind::InvalidValue,
                "Iceberg COW signed base must be 32 bytes",
            )
        })?;
        if raw.metadata_columns.len() > 4 {
            return Err(codec_error(
                "iceberg_table.frozen_cow_source.metadata_columns",
                ConnectorCodecErrorKind::InvalidValue,
                "Iceberg COW metadata proof exceeds its original four-field shape",
            ));
        }
        // Admit the actual typed representation before its Vec/path copies.
        // These decode limits do not confer execution memory capacity.
        context.ledger().charge_retained(
            size_of::<IcebergCowSourceProof>()
                + raw.metadata_columns.len() * size_of::<IcebergColumnHandle>(),
        )?;
        let path = copy_string(&raw.data_file_path, context)?;
        let mut columns = Vec::with_capacity(raw.metadata_columns.len());
        for column in &raw.metadata_columns {
            columns.push(decode_column(column, context)?);
            context.observe_compile_step()?;
        }
        observe_opaque(context, || {
            IcebergCowSourceProof::try_new(digest, base, path, columns)
        })?
        .map_err(|error| domain_error("iceberg_table.frozen_cow_source", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn decode_table(
    raw: &dto::IcebergTableHandle,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergTableHandle, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let mut projected_columns = BTreeSet::new();
        for column in &raw.projected_columns {
            let column = decode_column(column, context)?;
            context.flush_compile_control()?;
            projected_columns.insert(column);
            context.observe_compile_step()?;
        }
        let pinned_data_files = raw
            .pinned_data_files
            .as_ref()
            .map(|files| {
                observe_opaque(context, || {
                    crate::typed_read::IcebergPinnedDataFileSet::try_new(&files.paths)
                })?
                .map_err(|error| domain_error("iceberg_table.pinned_data_files", error))
            })
            .transpose()?;
        let parameters = IcebergTableHandleParams {
            read_domain: raw
                .read_domain
                .as_ref()
                .map(|raw| decode_read_domain_observed(raw, context, "iceberg_table.read_domain"))
                .transpose()?,
            schema_table_name: decode_schema_name(
                raw.schema_table_name.as_ref().ok_or_else(|| {
                    codec_error(
                        "iceberg_table.schema_table_name",
                        ConnectorCodecErrorKind::MissingField,
                        "Iceberg table requires a schema table name",
                    )
                })?,
                context,
            )?,
            snapshot_id: raw.snapshot_id,
            table_schema_json: copy_string(&raw.table_schema_json, context)?,
            spec_id: raw.spec_id,
            partition_spec_jsons: copy_i32_string_map(&raw.partition_spec_jsons, context)?,
            format_version: raw.format_version,
            unenforced_predicate: decode_tuple_domain(
                raw.unenforced_predicate.as_ref().ok_or_else(|| {
                    codec_error(
                        "iceberg_table.unenforced_predicate",
                        ConnectorCodecErrorKind::MissingField,
                        "Iceberg table requires an unenforced predicate",
                    )
                })?,
                context,
            )?,
            enforced_predicate: decode_tuple_domain(
                raw.enforced_predicate.as_ref().ok_or_else(|| {
                    codec_error(
                        "iceberg_table.enforced_predicate",
                        ConnectorCodecErrorKind::MissingField,
                        "Iceberg table requires an enforced predicate",
                    )
                })?,
                context,
            )?,
            limit: raw.limit,
            projected_columns,
            name_mapping_json: raw
                .name_mapping_json
                .as_deref()
                .map(|value| copy_string(value, context))
                .transpose()?,
            table_location: copy_string(&raw.table_location, context)?,
            storage_properties: copy_string_map(&raw.storage_properties, context)?,
            pinned_data_files,
        };
        let table = observe_opaque(context, || IcebergTableHandle::try_new(parameters))?
            .map_err(|error| domain_error("iceberg_table", error))?;
        let mut scalar_integer_domains = BTreeMap::new();
        for (id, value) in &raw.scalar_integer_domains {
            let domain = observe_opaque(context, || {
                crate::scalar_integer_domain::ScalarIntegerDomain::parse(value)
            })?
            .map_err(|error| domain_error("iceberg_table", error))?;
            scalar_integer_domains.insert(*id, domain);
            context.observe_compile_step()?;
        }
        let table = observe_opaque(context, || {
            table.with_scalar_integer_domains(scalar_integer_domains)
        })?
        .map_err(|error| domain_error("iceberg_table", error))?;
        let mut nominal_read_domains = BTreeMap::new();
        for (id, value) in &raw.nominal_read_domains {
            let domain = observe_opaque(context, || {
                crate::nominal_read_domain::NominalReadDomain::parse(value)
            })?
            .map_err(|error| domain_error("iceberg_table", error))?;
            nominal_read_domains.insert(*id, domain);
            context.observe_compile_step()?;
        }
        let table = observe_opaque(context, || {
            table.with_nominal_read_domains(nominal_read_domains)
        })?
        .map_err(|error| domain_error("iceberg_table", error))?;
        let source = raw
            .frozen_cow_source
            .as_ref()
            .map(|source| decode_cow_source_proof(source, context))
            .transpose()?;
        observe_opaque(context, || table.with_frozen_cow_source(source))?
            .map_err(|error| domain_error("iceberg_table.frozen_cow_source", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_table_function(value: &TableChangesFunctionHandle) -> dto::TableChangesFunctionHandle {
    dto::TableChangesFunctionHandle {
        schema_table_name: Some(encode_schema_name(value.schema_table_name())),
        table_schema_json: value.table_schema_json().to_string(),
        columns: value.columns().iter().map(encode_column).collect(),
        name_mapping_json: value.name_mapping_json().map(str::to_string),
        start_snapshot_id: value.start_snapshot_id(),
        end_snapshot_id: value.end_snapshot_id(),
    }
}

fn decode_table_function(
    raw: &dto::TableChangesFunctionHandle,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<TableChangesFunctionHandle, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        {
            let opaque_arguments = (TableChangesFunctionHandleParams {
                schema_table_name: decode_schema_name(
                    raw.schema_table_name.as_ref().ok_or_else(|| {
                        codec_error(
                            "table_changes.schema_table_name",
                            ConnectorCodecErrorKind::MissingField,
                            "table_changes requires a schema table name",
                        )
                    })?,
                    context,
                )?,
                table_schema_json: copy_string(&raw.table_schema_json, context)?,
                columns: convert_slice(&raw.columns, context, decode_column)?,
                name_mapping_json: raw
                    .name_mapping_json
                    .as_deref()
                    .map(|value| copy_string(value, context))
                    .transpose()?,
                start_snapshot_id: raw.start_snapshot_id,
                end_snapshot_id: raw.end_snapshot_id,
            },);
            observe_opaque(context, || {
                TableChangesFunctionHandle::try_new(opaque_arguments.0)
            })?
        }
        .map_err(|error| domain_error("table_changes", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_change_window(value: &IcebergChangeWindowHandle) -> dto::IcebergChangeWindowHandle {
    dto::IcebergChangeWindowHandle {
        from_read_domain: Some(encode_read_domain(value.from_read_domain())),
        to_read_domain: Some(encode_read_domain(value.to_read_domain())),
        schema_table_name: Some(encode_schema_name(value.schema_table_name())),
        table_schema_json: value.table_schema_json().to_string(),
        columns: value.columns().iter().map(encode_column).collect(),
        name_mapping_json: value.name_mapping_json().map(str::to_string),
        from_snapshot_id_exclusive: value.from_snapshot_id_exclusive(),
        to_snapshot_id_inclusive: value.to_snapshot_id_inclusive(),
        partition_spec_jsons: value
            .partition_spec_jsons()
            .iter()
            .map(|(key, value)| (*key, value.clone()))
            .collect(),
    }
}

fn decode_change_window(
    raw: &dto::IcebergChangeWindowHandle,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergChangeWindowHandle, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        {
            let opaque_arguments = (IcebergChangeWindowHandleParams {
                from_read_domain: decode_read_domain_observed(
                    raw.from_read_domain.as_ref().ok_or_else(|| {
                        codec_error(
                            "change_window.from_read_domain",
                            ConnectorCodecErrorKind::MissingField,
                            "change window requires From domain",
                        )
                    })?,
                    context,
                    "change_window.from_read_domain",
                )?,
                to_read_domain: decode_read_domain_observed(
                    raw.to_read_domain.as_ref().ok_or_else(|| {
                        codec_error(
                            "change_window.to_read_domain",
                            ConnectorCodecErrorKind::MissingField,
                            "change window requires To domain",
                        )
                    })?,
                    context,
                    "change_window.to_read_domain",
                )?,
                schema_table_name: decode_schema_name(
                    raw.schema_table_name.as_ref().ok_or_else(|| {
                        codec_error(
                            "change_window.schema_table_name",
                            ConnectorCodecErrorKind::MissingField,
                            "change window requires a schema table name",
                        )
                    })?,
                    context,
                )?,
                table_schema_json: copy_string(&raw.table_schema_json, context)?,
                columns: convert_slice(&raw.columns, context, decode_column)?,
                name_mapping_json: raw
                    .name_mapping_json
                    .as_deref()
                    .map(|value| copy_string(value, context))
                    .transpose()?,
                from_snapshot_id_exclusive: raw.from_snapshot_id_exclusive,
                to_snapshot_id_inclusive: raw.to_snapshot_id_inclusive,
                partition_spec_jsons: copy_i32_string_map(&raw.partition_spec_jsons, context)?,
            },);
            observe_opaque(context, || {
                IcebergChangeWindowHandle::try_new(opaque_arguments.0)
            })?
        }
        .map_err(|error| domain_error("change_window", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_system_table(value: &IcebergSystemTableReference) -> dto::IcebergSystemTableReference {
    dto::IcebergSystemTableReference {
        schema_table_name: Some(encode_schema_name(value.schema_table_name())),
        system_table_type: match value.system_table_type() {
            IcebergSystemTableType::Files => dto::IcebergSystemTableType::Files,
            IcebergSystemTableType::Entries => dto::IcebergSystemTableType::Entries,
            IcebergSystemTableType::Snapshots => dto::IcebergSystemTableType::Snapshots,
            IcebergSystemTableType::History => dto::IcebergSystemTableType::History,
            IcebergSystemTableType::Refs => dto::IcebergSystemTableType::Refs,
            IcebergSystemTableType::Manifests => dto::IcebergSystemTableType::Manifests,
            IcebergSystemTableType::Partitions => dto::IcebergSystemTableType::Partitions,
        } as i32,
        metadata_file_location: value.metadata_file_location().to_string(),
        table_uuid: value.table_uuid().to_string(),
        snapshot_id: value.snapshot_id(),
    }
}

fn decode_system_table(
    raw: &dto::IcebergSystemTableReference,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergSystemTableReference, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let system_table_type = match dto::IcebergSystemTableType::try_from(raw.system_table_type) {
            Ok(dto::IcebergSystemTableType::Files) => IcebergSystemTableType::Files,
            Ok(dto::IcebergSystemTableType::Entries) => IcebergSystemTableType::Entries,
            Ok(dto::IcebergSystemTableType::Snapshots) => IcebergSystemTableType::Snapshots,
            Ok(dto::IcebergSystemTableType::History) => IcebergSystemTableType::History,
            Ok(dto::IcebergSystemTableType::Refs) => IcebergSystemTableType::Refs,
            Ok(dto::IcebergSystemTableType::Manifests) => IcebergSystemTableType::Manifests,
            Ok(dto::IcebergSystemTableType::Partitions) => IcebergSystemTableType::Partitions,
            Ok(dto::IcebergSystemTableType::Unspecified) | Err(_) => {
                return Err(codec_error(
                    "system_table.system_table_type",
                    ConnectorCodecErrorKind::InvalidEnum,
                    "Iceberg system table type must be known",
                ));
            }
        };
        {
            let opaque_arguments = (IcebergSystemTableReferenceParams {
                schema_table_name: decode_schema_name(
                    raw.schema_table_name.as_ref().ok_or_else(|| {
                        codec_error(
                            "system_table.schema_table_name",
                            ConnectorCodecErrorKind::MissingField,
                            "system table requires a schema table name",
                        )
                    })?,
                    context,
                )?,
                system_table_type,
                metadata_file_location: copy_string(&raw.metadata_file_location, context)?,
                table_uuid: copy_string(&raw.table_uuid, context)?,
                snapshot_id: raw.snapshot_id,
            },);
            observe_opaque(context, || {
                IcebergSystemTableReference::try_new(opaque_arguments.0)
            })?
        }
        .map_err(|error| domain_error("system_table", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_procedure(value: IcebergProcedureId) -> dto::IcebergProcedureId {
    match value {
        IcebergProcedureId::Optimize => dto::IcebergProcedureId::Optimize,
        IcebergProcedureId::OptimizeManifests => dto::IcebergProcedureId::OptimizeManifests,
        IcebergProcedureId::DropExtendedStats => dto::IcebergProcedureId::DropExtendedStats,
        IcebergProcedureId::RollbackToSnapshot => dto::IcebergProcedureId::RollbackToSnapshot,
        IcebergProcedureId::ExpireSnapshots => dto::IcebergProcedureId::ExpireSnapshots,
        IcebergProcedureId::RemoveOrphanFiles => dto::IcebergProcedureId::RemoveOrphanFiles,
        IcebergProcedureId::AddFiles => dto::IcebergProcedureId::AddFiles,
        IcebergProcedureId::AddFilesFromTable => dto::IcebergProcedureId::AddFilesFromTable,
        IcebergProcedureId::RewritePositionDeleteFiles => {
            dto::IcebergProcedureId::RewritePositionDeleteFiles
        }
    }
}

fn decode_procedure(
    raw: i32,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergProcedureId, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = match dto::IcebergProcedureId::try_from(raw) {
        Ok(dto::IcebergProcedureId::Optimize) => Ok(IcebergProcedureId::Optimize),
        Ok(dto::IcebergProcedureId::OptimizeManifests) => Ok(IcebergProcedureId::OptimizeManifests),
        Ok(dto::IcebergProcedureId::DropExtendedStats) => Ok(IcebergProcedureId::DropExtendedStats),
        Ok(dto::IcebergProcedureId::RollbackToSnapshot) => {
            Ok(IcebergProcedureId::RollbackToSnapshot)
        }
        Ok(dto::IcebergProcedureId::ExpireSnapshots) => Ok(IcebergProcedureId::ExpireSnapshots),
        Ok(dto::IcebergProcedureId::RemoveOrphanFiles) => Ok(IcebergProcedureId::RemoveOrphanFiles),
        Ok(dto::IcebergProcedureId::AddFiles) => Ok(IcebergProcedureId::AddFiles),
        Ok(dto::IcebergProcedureId::AddFilesFromTable) => Ok(IcebergProcedureId::AddFilesFromTable),
        Ok(dto::IcebergProcedureId::RewritePositionDeleteFiles) => {
            Ok(IcebergProcedureId::RewritePositionDeleteFiles)
        }
        Ok(dto::IcebergProcedureId::Unspecified) | Err(_) => Err(codec_error(
            "table_execute.procedure_id",
            ConnectorCodecErrorKind::InvalidEnum,
            "Iceberg procedure must be known",
        )),
    };
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_table_execute(value: &IcebergTableExecuteHandle) -> dto::IcebergTableExecuteHandle {
    let procedure_handle = value.procedure_handle().map(|handle| match handle {
        IcebergTableExecuteProcedureHandle::Optimize(handle) => {
            dto::iceberg_table_execute_handle::ProcedureHandle::Optimize(
                dto::IcebergOptimizeHandle {
                    table_handle: Some(encode_table(handle.table_handle())),
                    min_file_size_bytes: handle.min_file_size_bytes(),
                },
            )
        }
        IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(handle) => {
            dto::iceberg_table_execute_handle::ProcedureHandle::RewritePositionDeleteFiles(
                dto::IcebergRewritePositionDeleteFilesHandle {
                    table_handle: Some(encode_table(handle.table_handle())),
                    artifact: Some(dto::IcebergRewriteArtifactContentId {
                        artifact_location: handle.artifact().artifact_location().to_string(),
                        artifact_digest_hex: handle.artifact().artifact_digest_hex().to_string(),
                    }),
                    group_digest_hex: handle.group_digest_hex().to_string(),
                },
            )
        }
    });
    dto::IcebergTableExecuteHandle {
        schema_table_name: Some(encode_schema_name(value.schema_table_name())),
        procedure_id: encode_procedure(value.procedure_id()) as i32,
        table_location: value.table_location().to_string(),
        procedure_handle,
    }
}

fn decode_table_execute(
    raw: &dto::IcebergTableExecuteHandle,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergTableExecuteHandle, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let procedure_handle = match raw.procedure_handle.as_ref() {
            None => None,
            Some(dto::iceberg_table_execute_handle::ProcedureHandle::Optimize(handle)) => {
                Some(IcebergTableExecuteProcedureHandle::Optimize(
                    {
                        let opaque_arguments = (
                            decode_table(
                                handle.table_handle.as_ref().ok_or_else(|| {
                                    codec_error(
                                        "table_execute.optimize.table_handle",
                                        ConnectorCodecErrorKind::MissingField,
                                        "Iceberg optimize requires a table handle",
                                    )
                                })?,
                                context,
                            )?,
                            handle.min_file_size_bytes,
                        );
                        observe_opaque(context, || {
                            IcebergOptimizeHandle::try_new(opaque_arguments.0, opaque_arguments.1)
                        })?
                    }
                    .map_err(|error| domain_error("table_execute.optimize", error))?,
                ))
            }
            Some(
                dto::iceberg_table_execute_handle::ProcedureHandle::RewritePositionDeleteFiles(
                    handle,
                ),
            ) => {
                let artifact = handle.artifact.as_ref().ok_or_else(|| {
                    codec_error(
                        "table_execute.rewrite.artifact",
                        ConnectorCodecErrorKind::MissingField,
                        "Iceberg rewrite requires its content identity",
                    )
                })?;
                Some(
                    IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(
                        {
                            let opaque_arguments = (
                                decode_table(
                                    handle.table_handle.as_ref().ok_or_else(|| {
                                        codec_error(
                                            "table_execute.rewrite.table_handle",
                                            ConnectorCodecErrorKind::MissingField,
                                            "Iceberg rewrite requires a table handle",
                                        )
                                    })?,
                                    context,
                                )?,
                                {
                                    let opaque_arguments = (
                                        &artifact.artifact_location,
                                        &artifact.artifact_digest_hex,
                                    );
                                    observe_opaque(context, || {
                                        IcebergRewriteArtifactContentId::try_new(
                                            opaque_arguments.0,
                                            opaque_arguments.1,
                                        )
                                    })?
                                }
                                .map_err(|error| {
                                    domain_error("table_execute.rewrite.artifact", error)
                                })?,
                                &handle.group_digest_hex,
                            );
                            observe_opaque(context, || {
                                IcebergRewritePositionDeleteFilesHandle::try_new(
                                    opaque_arguments.0,
                                    opaque_arguments.1,
                                    opaque_arguments.2,
                                )
                            })?
                        }
                        .map_err(|error| domain_error("table_execute.rewrite", error))?,
                    ),
                )
            }
        };
        {
            let opaque_arguments = (IcebergTableExecuteHandleParams {
                schema_table_name: decode_schema_name(
                    raw.schema_table_name.as_ref().ok_or_else(|| {
                        codec_error(
                            "table_execute.schema_table_name",
                            ConnectorCodecErrorKind::MissingField,
                            "Iceberg table execute requires a schema table name",
                        )
                    })?,
                    context,
                )?,
                procedure_id: decode_procedure(raw.procedure_id, context)?,
                table_location: copy_string(&raw.table_location, context)?,
                procedure_handle,
            },);
            observe_opaque(context, || {
                IcebergTableExecuteHandle::try_new(opaque_arguments.0)
            })?
        }
        .map_err(|error| domain_error("table_execute", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_insert_handle(value: &IcebergInsertTableHandle) -> dto::IcebergInsertTableHandle {
    dto::IcebergInsertTableHandle {
        schema_table_name: Some(encode_schema_name(value.schema_table_name())),
        table_schema_json: value.table_schema_json().to_string(),
        table_location: value.table_location().to_string(),
        format_version: value.format_version(),
        spec_id: value.spec_id(),
    }
}

fn decode_insert_handle(
    raw: &dto::IcebergInsertTableHandle,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergInsertTableHandle, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        {
            let opaque_arguments = (IcebergInsertTableHandleParams {
                schema_table_name: decode_schema_name(
                    raw.schema_table_name.as_ref().ok_or_else(|| {
                        codec_error(
                            "merge.insert.schema_table_name",
                            ConnectorCodecErrorKind::MissingField,
                            "Iceberg insert handle requires a schema table name",
                        )
                    })?,
                    context,
                )?,
                table_schema_json: copy_string(&raw.table_schema_json, context)?,
                table_location: copy_string(&raw.table_location, context)?,
                format_version: raw.format_version,
                spec_id: raw.spec_id,
            },);
            observe_opaque(context, || {
                IcebergInsertTableHandle::try_new(opaque_arguments.0)
            })?
        }
        .map_err(|error| domain_error("merge.insert", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_merge(value: &IcebergMergeTableHandle) -> dto::IcebergMergeTableHandle {
    dto::IcebergMergeTableHandle {
        table_handle: Some(encode_table(value.table_handle())),
        insert_table_handle: Some(encode_insert_handle(value.insert_table_handle())),
    }
}

fn decode_merge(
    raw: &dto::IcebergMergeTableHandle,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergMergeTableHandle, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        {
            let opaque_arguments = (
                decode_table(
                    raw.table_handle.as_ref().ok_or_else(|| {
                        codec_error(
                            "merge.table_handle",
                            ConnectorCodecErrorKind::MissingField,
                            "Iceberg merge requires a read table handle",
                        )
                    })?,
                    context,
                )?,
                decode_insert_handle(
                    raw.insert_table_handle.as_ref().ok_or_else(|| {
                        codec_error(
                            "merge.insert_table_handle",
                            ConnectorCodecErrorKind::MissingField,
                            "Iceberg merge requires an insert table handle",
                        )
                    })?,
                    context,
                )?,
            );
            observe_opaque(context, || {
                IcebergMergeTableHandle::try_new(opaque_arguments.0, opaque_arguments.1)
            })?
        }
        .map_err(|error| domain_error("merge", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_relation(value: &IcebergRuntimeRelation) -> dto::IcebergReadTablePayload {
    let relation = match value {
        IcebergRuntimeRelation::Table(value) => {
            dto::iceberg_read_table_payload::Relation::Table(encode_table(value))
        }
        IcebergRuntimeRelation::TableFunction(value) => {
            dto::iceberg_read_table_payload::Relation::TableFunction(encode_table_function(value))
        }
        IcebergRuntimeRelation::ChangeWindow(value) => {
            dto::iceberg_read_table_payload::Relation::ChangeWindow(encode_change_window(value))
        }
        IcebergRuntimeRelation::SystemTable(value) => {
            dto::iceberg_read_table_payload::Relation::SystemTable(encode_system_table(value))
        }
        IcebergRuntimeRelation::TableExecute(value) => {
            dto::iceberg_read_table_payload::Relation::TableExecute(encode_table_execute(value))
        }
        IcebergRuntimeRelation::MergeTable(value) => {
            dto::iceberg_read_table_payload::Relation::MergeTable(encode_merge(value))
        }
    };
    dto::IcebergReadTablePayload {
        relation: Some(relation),
    }
}

fn decode_relation(
    raw: &dto::IcebergReadTablePayload,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergRuntimeRelation, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| match raw.relation.as_ref().ok_or_else(|| {
        codec_error(
            "iceberg_read_table.relation",
            ConnectorCodecErrorKind::MissingField,
            "Iceberg read table payload requires one relation",
        )
    })? {
        dto::iceberg_read_table_payload::Relation::Table(value) => {
            Ok(IcebergRuntimeRelation::Table(decode_table(value, context)?))
        }
        dto::iceberg_read_table_payload::Relation::TableFunction(value) => Ok(
            IcebergRuntimeRelation::TableFunction(decode_table_function(value, context)?),
        ),
        dto::iceberg_read_table_payload::Relation::ChangeWindow(value) => Ok(
            IcebergRuntimeRelation::ChangeWindow(decode_change_window(value, context)?),
        ),
        dto::iceberg_read_table_payload::Relation::SystemTable(value) => Ok(
            IcebergRuntimeRelation::SystemTable(decode_system_table(value, context)?),
        ),
        dto::iceberg_read_table_payload::Relation::TableExecute(value) => Ok(
            IcebergRuntimeRelation::TableExecute(decode_table_execute(value, context)?),
        ),
        dto::iceberg_read_table_payload::Relation::MergeTable(value) => Ok(
            IcebergRuntimeRelation::MergeTable(decode_merge(value, context)?),
        ),
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_file_format(value: IcebergFileFormat) -> i32 {
    (match value {
        IcebergFileFormat::Orc => dto::IcebergFileFormat::Orc,
        IcebergFileFormat::Parquet => dto::IcebergFileFormat::Parquet,
        IcebergFileFormat::Avro => dto::IcebergFileFormat::Avro,
        IcebergFileFormat::Puffin => dto::IcebergFileFormat::Puffin,
    }) as i32
}

fn decode_file_format(
    raw: i32,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergFileFormat, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = match dto::IcebergFileFormat::try_from(raw) {
        Ok(dto::IcebergFileFormat::Orc) => Ok(IcebergFileFormat::Orc),
        Ok(dto::IcebergFileFormat::Parquet) => Ok(IcebergFileFormat::Parquet),
        Ok(dto::IcebergFileFormat::Avro) => Ok(IcebergFileFormat::Avro),
        Ok(dto::IcebergFileFormat::Puffin) => Ok(IcebergFileFormat::Puffin),
        Ok(dto::IcebergFileFormat::Unspecified) | Err(_) => Err(codec_error(
            "iceberg_split.file_format",
            ConnectorCodecErrorKind::InvalidEnum,
            "Iceberg file format must be known",
        )),
    };
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_decryption(value: &ParquetFileDecryptionData) -> dto::ParquetFileDecryptionData {
    dto::ParquetFileDecryptionData {
        key_metadata: value.key_metadata().to_vec(),
        aad_prefix: value.aad_prefix().to_vec(),
    }
}

fn decode_decryption(
    raw: &dto::ParquetFileDecryptionData,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<ParquetFileDecryptionData, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        {
            let opaque_arguments = (
                copy_slice(&raw.key_metadata, context)?,
                copy_slice(&raw.aad_prefix, context)?,
            );
            observe_opaque(context, || {
                ParquetFileDecryptionData::try_new(opaque_arguments.0, opaque_arguments.1)
            })?
        }
        .map_err(|error| domain_error("iceberg_split.decryption_data", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_delete(value: &IcebergDeleteFile) -> dto::IcebergDeleteFile {
    dto::IcebergDeleteFile {
        partition_spec_id: Some(value.partition_spec_id()),
        partition_data_json: value.partition_data_json().to_string(),
        content: match value.content() {
            IcebergDeleteFileContent::PositionDeletes => {
                dto::IcebergDeleteFileContent::PositionDeletes
            }
            IcebergDeleteFileContent::EqualityDeletes => {
                dto::IcebergDeleteFileContent::EqualityDeletes
            }
        } as i32,
        path: value.path().to_string(),
        format: encode_file_format(value.format()),
        record_count: value.record_count(),
        file_size_in_bytes: value.file_size_in_bytes(),
        equality_field_ids: value.equality_field_ids().to_vec(),
        row_position_lower_bound: value.row_position_lower_bound(),
        row_position_upper_bound: value.row_position_upper_bound(),
        data_sequence_number: value.data_sequence_number(),
        content_offset: value.content_offset(),
        content_size_in_bytes: value.content_size_in_bytes(),
        decryption_data: value.decryption_data().map(encode_decryption),
        referenced_data_file: value.referenced_data_file().map(str::to_string),
    }
}

fn decode_delete(
    raw: &dto::IcebergDeleteFile,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergDeleteFile, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let content = match dto::IcebergDeleteFileContent::try_from(raw.content) {
            Ok(dto::IcebergDeleteFileContent::PositionDeletes) => {
                IcebergDeleteFileContent::PositionDeletes
            }
            Ok(dto::IcebergDeleteFileContent::EqualityDeletes) => {
                IcebergDeleteFileContent::EqualityDeletes
            }
            Ok(dto::IcebergDeleteFileContent::Unspecified) | Err(_) => {
                return Err(codec_error(
                    "iceberg_delete.content",
                    ConnectorCodecErrorKind::InvalidEnum,
                    "Iceberg delete content must be known",
                ));
            }
        };
        {
            let opaque_arguments = (IcebergDeleteFileParams {
                partition_spec_id: raw.partition_spec_id.ok_or_else(|| {
                    codec_error(
                        "iceberg_delete.partition_spec_id",
                        ConnectorCodecErrorKind::MissingField,
                        "Iceberg delete requires its own spec",
                    )
                })?,
                partition_data_json: copy_string(&raw.partition_data_json, context)?,
                content,
                path: copy_string(&raw.path, context)?,
                format: decode_file_format(raw.format, context)?,
                record_count: raw.record_count,
                file_size_in_bytes: raw.file_size_in_bytes,
                equality_field_ids: copy_slice(&raw.equality_field_ids, context)?,
                row_position_lower_bound: raw.row_position_lower_bound,
                row_position_upper_bound: raw.row_position_upper_bound,
                data_sequence_number: raw.data_sequence_number,
                content_offset: raw.content_offset,
                content_size_in_bytes: raw.content_size_in_bytes,
                referenced_data_file: raw
                    .referenced_data_file
                    .as_deref()
                    .map(|value| copy_string(value, context))
                    .transpose()?,
                decryption_data: raw
                    .decryption_data
                    .as_ref()
                    .map(|value| decode_decryption(value, context))
                    .transpose()?,
            },);
            observe_opaque(context, || IcebergDeleteFile::try_new(opaque_arguments.0))?
        }
        .map_err(|error| domain_error("iceberg_delete", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_data_split(value: &IcebergSplit) -> Result<dto::IcebergSplit, ConnectorCodecError> {
    Ok(dto::IcebergSplit {
        read_domain: Some(encode_read_domain(value.read_domain())),
        path: value.path().to_string(),
        start: value.start(),
        length: value.length(),
        file_size: value.file_size(),
        file_record_count: value.file_record_count(),
        file_format: encode_file_format(value.file_format()),
        partition_spec_id: value.partition_spec_id(),
        partition_data_json: value.partition_data_json().to_string(),
        deletes: value
            .deletes()
            .iter()
            .map(|member| {
                member
                    .map(|member| encode_delete(&member))
                    .map_err(|e| domain_error("iceberg_split.deletes", e))
            })
            .collect::<Result<Vec<_>, _>>()?,
        file_statistics_domain: Some(encode_tuple_domain(value.file_statistics_domain())),
        data_sequence_number: value.data_sequence_number(),
        file_first_row_id: value.file_first_row_id(),
        decryption_data: value.decryption_data().map(encode_decryption),
    })
}

fn decode_data_split(
    raw: &dto::IcebergSplit,
    split_weight: SplitWeight,
    affinity_key: Option<String>,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergSplit, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        {
            let opaque_arguments = (IcebergSplitParams {
                read_domain: decode_read_domain_observed(
                    raw.read_domain.as_ref().ok_or_else(|| {
                        codec_error(
                            "iceberg_split.read_domain",
                            ConnectorCodecErrorKind::MissingField,
                            "Iceberg split requires its domain",
                        )
                    })?,
                    context,
                    "iceberg_split.read_domain",
                )?,
                path: copy_string(&raw.path, context)?,
                start: raw.start,
                length: raw.length,
                file_size: raw.file_size,
                file_record_count: raw.file_record_count,
                file_format: decode_file_format(raw.file_format, context)?,
                partition_spec_id: raw.partition_spec_id,
                partition_data_json: copy_string(&raw.partition_data_json, context)?,
                deletes: convert_slice(&raw.deletes, context, decode_delete)?.into(),
                file_statistics_domain: decode_tuple_domain(
                    raw.file_statistics_domain.as_ref().ok_or_else(|| {
                        codec_error(
                            "iceberg_split.file_statistics_domain",
                            ConnectorCodecErrorKind::MissingField,
                            "Iceberg data split requires a statistics domain",
                        )
                    })?,
                    context,
                )?,
                data_sequence_number: raw.data_sequence_number,
                file_first_row_id: raw.file_first_row_id,
                decryption_data: raw
                    .decryption_data
                    .as_ref()
                    .map(|value| decode_decryption(value, context))
                    .transpose()?,
                split_weight,
                affinity_key,
            },);
            observe_opaque(context, || IcebergSplit::try_new(opaque_arguments.0))?
        }
        .map_err(|error| domain_error("iceberg_split", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_table_changes_split(value: &TableChangesSplit) -> dto::TableChangesSplit {
    dto::TableChangesSplit {
        change_type: match value.change_type() {
            TableChangesChangeType::AddedFile => dto::TableChangesChangeType::AddedFile,
            TableChangesChangeType::DeletedFile => dto::TableChangesChangeType::DeletedFile,
        } as i32,
        snapshot_id: value.snapshot_id(),
        snapshot_timestamp_millis: value.snapshot_timestamp_millis(),
        change_ordinal: value.change_ordinal(),
        path: value.path().to_string(),
        start: value.start(),
        length: value.length(),
        file_size: value.file_size(),
        file_record_count: value.file_record_count(),
        file_format: encode_file_format(value.file_format()),
        partition_spec_id: value.partition_spec_id(),
        partition_data_json: value.partition_data_json().to_string(),
        decryption_data: value.decryption_data().map(encode_decryption),
    }
}

fn decode_table_changes_split(
    raw: &dto::TableChangesSplit,
    split_weight: SplitWeight,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<TableChangesSplit, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let change_type = match dto::TableChangesChangeType::try_from(raw.change_type) {
            Ok(dto::TableChangesChangeType::AddedFile) => TableChangesChangeType::AddedFile,
            Ok(dto::TableChangesChangeType::DeletedFile) => TableChangesChangeType::DeletedFile,
            Ok(dto::TableChangesChangeType::Unspecified) | Err(_) => {
                return Err(codec_error(
                    "table_changes_split.change_type",
                    ConnectorCodecErrorKind::InvalidEnum,
                    "table_changes split type must be known",
                ));
            }
        };
        {
            let opaque_arguments = (TableChangesSplitParams {
                change_type,
                snapshot_id: raw.snapshot_id,
                snapshot_timestamp_millis: raw.snapshot_timestamp_millis,
                change_ordinal: raw.change_ordinal,
                path: copy_string(&raw.path, context)?,
                start: raw.start,
                length: raw.length,
                file_size: raw.file_size,
                file_record_count: raw.file_record_count,
                file_format: decode_file_format(raw.file_format, context)?,
                partition_spec_id: raw.partition_spec_id,
                partition_data_json: copy_string(&raw.partition_data_json, context)?,
                decryption_data: raw
                    .decryption_data
                    .as_ref()
                    .map(|value| decode_decryption(value, context))
                    .transpose()?,
                split_weight,
            },);
            observe_opaque(context, || TableChangesSplit::try_new(opaque_arguments.0))?
        }
        .map_err(|error| domain_error("table_changes_split", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_change_split(
    value: &IcebergChangeSplit,
) -> Result<dto::IcebergChangeSplit, ConnectorCodecError> {
    use dto::iceberg_change_split::Rows;
    let rows = match value {
        IcebergChangeSplit::AddedRows(rows) => Rows::AddedRows(dto::IcebergAddedRows {
            data: Some(encode_data_split(rows.data())?),
            restricted_row_ids: rows.restricted_row_ids().to_vec(),
        }),
        IcebergChangeSplit::VisibilityDifference(rows) => {
            Rows::VisibilityDifference(dto::IcebergVisibilityDifference {
                data: Some(encode_data_split(rows.data())?),
                from_deletes: rows.from_deletes().iter().map(encode_delete).collect(),
                to_deletes: rows.to_deletes().iter().map(encode_delete).collect(),
            })
        }
        IcebergChangeSplit::DeletedDataFileRows(rows) => {
            Rows::DeletedDataFileRows(dto::IcebergDeletedDataFileRows {
                data: Some(encode_data_split(rows.data())?),
            })
        }
    };
    Ok(dto::IcebergChangeSplit { rows: Some(rows) })
}

fn required_data(
    raw: Option<&dto::IcebergSplit>,
) -> Result<&dto::IcebergSplit, ConnectorCodecError> {
    raw.ok_or_else(|| {
        codec_error(
            "change_window_split.data",
            ConnectorCodecErrorKind::MissingField,
            "Iceberg change split requires its data file",
        )
    })
}

fn decode_change_split(
    raw: &dto::IcebergChangeSplit,
    split_weight: SplitWeight,
    affinity_key: Option<String>,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergChangeSplit, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        use dto::iceberg_change_split::Rows;
        match raw.rows.as_ref().ok_or_else(|| {
            codec_error(
                "change_window_split.rows",
                ConnectorCodecErrorKind::MissingField,
                "Iceberg change split requires one row variant",
            )
        })? {
            Rows::AddedRows(rows) => Ok(IcebergChangeSplit::AddedRows(
                {
                    let opaque_arguments = (
                        decode_data_split(
                            required_data(rows.data.as_ref())?,
                            split_weight,
                            affinity_key,
                            context,
                        )?,
                        copy_slice(&rows.restricted_row_ids, context)?,
                    );
                    observe_opaque(context, || {
                        IcebergAddedRows::try_new(opaque_arguments.0, opaque_arguments.1)
                    })?
                }
                .map_err(|error| domain_error("change_window_split.added_rows", error))?,
            )),
            Rows::VisibilityDifference(rows) => Ok(IcebergChangeSplit::VisibilityDifference(
                {
                    let opaque_arguments = (
                        decode_data_split(
                            required_data(rows.data.as_ref())?,
                            split_weight,
                            affinity_key,
                            context,
                        )?,
                        convert_slice(&rows.from_deletes, context, decode_delete)?,
                        convert_slice(&rows.to_deletes, context, decode_delete)?,
                    );
                    observe_opaque(context, || {
                        IcebergVisibilityDifferenceRows::try_new(
                            opaque_arguments.0,
                            opaque_arguments.1,
                            opaque_arguments.2,
                        )
                    })?
                }
                .map_err(|error| {
                    domain_error("change_window_split.visibility_difference", error)
                })?,
            )),
            Rows::DeletedDataFileRows(rows) => Ok(IcebergChangeSplit::DeletedDataFileRows(
                {
                    let opaque_arguments = (decode_data_split(
                        required_data(rows.data.as_ref())?,
                        split_weight,
                        affinity_key,
                        context,
                    )?,);
                    observe_opaque(context, || {
                        IcebergDeletedDataFileRows::try_new(opaque_arguments.0)
                    })?
                }
                .map_err(|error| {
                    domain_error("change_window_split.deleted_data_file_rows", error)
                })?,
            )),
        }
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_manifest(value: &TrinoManifestFile) -> dto::TrinoManifestFile {
    dto::TrinoManifestFile {
        path: value.path().to_string(),
        length: value.length(),
        partition_spec_id: value.partition_spec_id(),
        content: value.content().code(),
        sequence_number: value.sequence_number(),
        min_sequence_number: value.min_sequence_number(),
        added_snapshot_id: value.added_snapshot_id(),
        added_files_count: value.added_files_count(),
        existing_files_count: value.existing_files_count(),
        deleted_files_count: value.deleted_files_count(),
        added_rows_count: value.added_rows_count(),
        existing_rows_count: value.existing_rows_count(),
        deleted_rows_count: value.deleted_rows_count(),
        first_row_id: value.first_row_id(),
        key_metadata: Vec::new(),
    }
}

fn decode_manifest(
    raw: &dto::TrinoManifestFile,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<TrinoManifestFile, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        let content = match raw.content {
            0 => TrinoManifestContent::Data,
            1 => TrinoManifestContent::Deletes,
            _ => {
                return Err(codec_error(
                    "files_split.manifest.content",
                    ConnectorCodecErrorKind::InvalidEnum,
                    "Iceberg manifest content must be data or deletes",
                ));
            }
        };
        {
            let opaque_arguments = (TrinoManifestFileParams {
                path: copy_string(&raw.path, context)?,
                length: raw.length,
                partition_spec_id: raw.partition_spec_id,
                content,
                sequence_number: raw.sequence_number,
                min_sequence_number: raw.min_sequence_number,
                added_snapshot_id: raw.added_snapshot_id,
                added_files_count: raw.added_files_count,
                existing_files_count: raw.existing_files_count,
                deleted_files_count: raw.deleted_files_count,
                added_rows_count: raw.added_rows_count,
                existing_rows_count: raw.existing_rows_count,
                deleted_rows_count: raw.deleted_rows_count,
                first_row_id: raw.first_row_id,
                key_metadata: copy_slice(&raw.key_metadata, context)?,
            },);
            observe_opaque(context, || TrinoManifestFile::try_new(opaque_arguments.0))?
        }
        .map_err(|error| domain_error("files_split.manifest", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_files_split(value: &FilesTableSplit) -> dto::FilesTableSplit {
    dto::FilesTableSplit {
        manifest: Some(encode_manifest(value.manifest())),
        table_schema_json: value.table_schema_json().to_string(),
        metadata_table_schema_json: value.metadata_table_schema_json().to_string(),
        partition_spec_jsons: value
            .partition_spec_jsons()
            .iter()
            .map(|(key, value)| (*key, value.clone()))
            .collect(),
        partition_column_type_json: value.partition_column_type_json().map(str::to_string),
        bounds_column_type_json: value.bounds_column_type_json().map(str::to_string),
        encryption_key_id: value.encryption_key_id().map(str::to_string),
    }
}

fn decode_files_split(
    raw: &dto::FilesTableSplit,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<FilesTableSplit, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        {
            let opaque_arguments = (FilesTableSplitParams {
                manifest: decode_manifest(
                    raw.manifest.as_ref().ok_or_else(|| {
                        codec_error(
                            "files_split.manifest",
                            ConnectorCodecErrorKind::MissingField,
                            "Iceberg files split requires a manifest",
                        )
                    })?,
                    context,
                )?,
                table_schema_json: copy_string(&raw.table_schema_json, context)?,
                metadata_table_schema_json: copy_string(&raw.metadata_table_schema_json, context)?,
                partition_spec_jsons: copy_i32_string_map(&raw.partition_spec_jsons, context)?,
                partition_column_type_json: raw
                    .partition_column_type_json
                    .as_deref()
                    .map(|value| copy_string(value, context))
                    .transpose()?,
                bounds_column_type_json: raw
                    .bounds_column_type_json
                    .as_deref()
                    .map(|value| copy_string(value, context))
                    .transpose()?,
                encryption_key_id: raw
                    .encryption_key_id
                    .as_deref()
                    .map(|value| copy_string(value, context))
                    .transpose()?,
            },);
            observe_opaque(context, || FilesTableSplit::try_new(opaque_arguments.0))?
        }
        .map_err(|error| domain_error("files_split", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_rewrite_split(
    value: &IcebergRewritePositionDeleteFilesSplit,
) -> dto::IcebergRewritePositionDeleteFilesSplit {
    dto::IcebergRewritePositionDeleteFilesSplit {
        data_file_path: value.data_file_path().to_string(),
        data_file_size: value.data_file_size(),
        partition_spec_id: value.partition_spec_id(),
        partition_data_json: value.partition_data_json().to_string(),
        selected_position_deletes: value
            .selected_position_deletes()
            .iter()
            .map(encode_delete)
            .collect(),
    }
}

fn decode_rewrite_split(
    raw: &dto::IcebergRewritePositionDeleteFilesSplit,
    split_weight: SplitWeight,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergRewritePositionDeleteFilesSplit, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        {
            let opaque_arguments = (IcebergRewritePositionDeleteFilesSplitParams {
                data_file_path: copy_string(&raw.data_file_path, context)?,
                data_file_size: raw.data_file_size,
                partition_spec_id: raw.partition_spec_id,
                partition_data_json: copy_string(&raw.partition_data_json, context)?,
                selected_position_deletes: convert_slice(
                    &raw.selected_position_deletes,
                    context,
                    decode_delete,
                )?,
                split_weight,
            },);
            observe_opaque(context, || {
                IcebergRewritePositionDeleteFilesSplit::try_new(opaque_arguments.0)
            })?
        }
        .map_err(|error| domain_error("rewrite_position_delete_files_split", error))
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn encode_split(
    value: &IcebergReadSplit,
) -> Result<dto::IcebergReadSplitPayload, ConnectorCodecError> {
    use dto::iceberg_read_split_payload::Split;
    let split = match value {
        IcebergReadSplit::Data(value) => Split::Data(encode_data_split(value)?),
        IcebergReadSplit::TableChanges(value) => {
            Split::TableChanges(encode_table_changes_split(value))
        }
        IcebergReadSplit::ChangeWindow(value) => Split::ChangeWindow(encode_change_split(value)?),
        IcebergReadSplit::SystemFiles(value) => Split::SystemFiles(encode_files_split(value)),
        IcebergReadSplit::RewritePositionDeleteFiles(value) => {
            Split::RewritePositionDeleteFiles(encode_rewrite_split(value))
        }
    };
    Ok(dto::IcebergReadSplitPayload { split: Some(split) })
}

fn decode_split(
    raw: &dto::IcebergReadSplitPayload,
    facts: &novarocks_spi::connector::read_stack::ConnectorReadSplitFacts,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<IcebergReadSplit, ConnectorCodecError> {
    // Existing serde/domain/library operations remain opaque inside this boundary.
    context.flush_compile_control()?;
    let result = (|| {
        use dto::iceberg_read_split_payload::Split;
        match raw.split.as_ref().ok_or_else(|| {
            codec_error(
                "iceberg_read_split.split",
                ConnectorCodecErrorKind::MissingField,
                "Iceberg private split payload requires one split",
            )
        })? {
            Split::Data(value) => Ok(IcebergReadSplit::Data(decode_data_split(
                value,
                facts.split_weight(),
                facts
                    .affinity_key()
                    .map(|value| copy_string(value, context))
                    .transpose()?,
                context,
            )?)),
            Split::TableChanges(value) => {
                require_no_affinity(facts, "table_changes_split")?;
                Ok(IcebergReadSplit::TableChanges(decode_table_changes_split(
                    value,
                    facts.split_weight(),
                    context,
                )?))
            }
            Split::ChangeWindow(value) => Ok(IcebergReadSplit::ChangeWindow(decode_change_split(
                value,
                facts.split_weight(),
                facts
                    .affinity_key()
                    .map(|value| copy_string(value, context))
                    .transpose()?,
                context,
            )?)),
            Split::SystemFiles(value) => {
                require_no_affinity(facts, "files_split")?;
                if facts.split_weight() != SplitWeight::STANDARD {
                    return Err(codec_error(
                        "files_split.split_weight",
                        ConnectorCodecErrorKind::InconsistentFields,
                        "Iceberg files split requires standard split weight",
                    ));
                }
                Ok(IcebergReadSplit::SystemFiles(decode_files_split(
                    value, context,
                )?))
            }
            Split::RewritePositionDeleteFiles(value) => {
                Ok(IcebergReadSplit::RewritePositionDeleteFiles(
                    decode_rewrite_split(value, facts.split_weight(), context)?,
                ))
            }
        }
    })();
    context.observe_compile_step()?;
    finish_decode(result, context)
}

fn validate_common_split_facts(
    facts: &novarocks_spi::connector::read_stack::ConnectorReadSplitFacts,
) -> Result<(), ConnectorCodecError> {
    if !facts.remotely_accessible() || !facts.addresses().is_empty() {
        return Err(codec_error(
            "iceberg_read_split.facts",
            ConnectorCodecErrorKind::InconsistentFields,
            "Iceberg splits are remotely accessible and name no host addresses",
        ));
    }
    Ok(())
}

fn require_no_affinity(
    facts: &novarocks_spi::connector::read_stack::ConnectorReadSplitFacts,
    path: &'static str,
) -> Result<(), ConnectorCodecError> {
    if facts.affinity_key().is_some() {
        return Err(codec_error(
            path,
            ConnectorCodecErrorKind::InconsistentFields,
            "Iceberg split category does not accept an affinity key",
        ));
    }
    Ok(())
}

fn validate_materialized_split_facts(
    split: &IcebergReadSplit,
    facts: &novarocks_spi::connector::read_stack::ConnectorReadSplitFacts,
) -> Result<(), ConnectorCodecError> {
    if split.is_remotely_accessible() != facts.remotely_accessible()
        || split.addresses() != facts.addresses()
        || split.affinity_key() != facts.affinity_key()
        || split.split_weight() != facts.split_weight()
        || split.retained_size_in_bytes() != facts.retained_size_in_bytes()
    {
        return Err(codec_error(
            "iceberg_read_split.facts",
            ConnectorCodecErrorKind::InconsistentFields,
            "Iceberg private split disagrees with validated public scheduling facts",
        ));
    }
    Ok(())
}

// Copy only codec-owned backing cooperatively. Legacy runtime decoding keeps
// its original fast-copy path; interruption never selects that path.
fn copy_slice<T: Copy>(
    values: &[T],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Vec<T>, ConnectorCodecError> {
    if !context.is_compile_observed() {
        return Ok(values.to_vec());
    }
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        result.push(*value);
        context.observe_compile_step()?;
    }
    Ok(result)
}
fn copy_string(
    value: &str,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<String, ConnectorCodecError> {
    if !context.is_compile_observed() {
        return Ok(value.to_owned());
    }
    let mut result = String::with_capacity(value.len());
    for character in value.chars() {
        result.push(character);
        context.observe_compile_step()?;
    }
    Ok(result)
}
fn copy_i32_string_map(
    values: &BTreeMap<i32, String>,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<BTreeMap<i32, String>, ConnectorCodecError> {
    if !context.is_compile_observed() {
        return Ok(values.clone());
    }
    let mut result = BTreeMap::new();
    for (key, value) in values {
        let value = copy_string(value, context)?;
        result.insert(*key, value);
        context.observe_compile_step()?;
    }
    Ok(result)
}
fn copy_string_map(
    values: &BTreeMap<String, String>,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<BTreeMap<String, String>, ConnectorCodecError> {
    if !context.is_compile_observed() {
        return Ok(values.clone());
    }
    let mut result = BTreeMap::new();
    for (key, value) in values {
        let key = copy_string(key, context)?;
        let value = copy_string(value, context)?;
        // The standard BTree comparator/allocator is an opaque library owner.
        context.flush_compile_control()?;
        result.insert(key, value);
        context.observe_compile_step()?;
    }
    Ok(result)
}
fn convert_slice<T, U>(
    values: &[T],
    context: &mut ConnectorDecodeContext<'_>,
    mut convert: impl FnMut(&T, &mut ConnectorDecodeContext<'_>) -> Result<U, ConnectorCodecError>,
) -> Result<Vec<U>, ConnectorCodecError> {
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        result.push(convert(value, context)?);
        context.observe_compile_step()?;
    }
    Ok(result)
}
// Opaque domain/library work is bracketed with the original compile control.
// Its own recursive validation, allocation and comparison are not cooperative.
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

fn decode_read_domain_observed(
    raw: &dto::IcebergReadDomain,
    context: &mut ConnectorDecodeContext<'_>,
    path: &'static str,
) -> Result<Arc<crate::delete_semantics::ReadDomain>, ConnectorCodecError> {
    observe_opaque(context, || decode_read_domain(raw))?.map_err(|error| domain_error(path, error))
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
mod tests {
    use super::*;
    use novarocks_spi::connector::read_stack::ConnectorReadSplitFacts;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEnvelopeHeader, ConnectorInstanceId,
        ConnectorProviderId,
    };

    fn header(category: ConnectorCodecCategory) -> ConnectorEnvelopeHeader {
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse(crate::PROVIDER_ID).unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                CatalogVersion::from_bytes([7; 32]),
            ),
            category,
            ConnectorCodecRevision::try_new(crate::contract_revision::ICEBERG_CONTRACT_REVISION)
                .unwrap(),
        )
    }

    fn limits() -> ConnectorDecodeLimits {
        ConnectorDecodeLimits::try_new(1 << 20, 1 << 20, 1 << 20, 100_000, 64).unwrap()
    }

    fn decode_with<T>(
        category: ConnectorCodecCategory,
        payload: &[u8],
    ) -> Result<T, ConnectorCodecError>
    where
        IcebergReadWireCodec: ConnectorPrivateDecoder<T>,
    {
        let header = header(category);
        let mut ledger = ConnectorDecodeLedger::new(limits());
        IcebergReadWireCodec.decode_private(
            payload,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
        )
    }

    fn primitive_column() -> IcebergColumnHandle {
        IcebergColumnHandle::try_new(IcebergColumnHandleParams {
            base_column_identity: ColumnIdentity::try_new(
                1,
                "id",
                ColumnIdentityCategory::Primitive,
                vec![],
            )
            .unwrap(),
            base_type_json: "\"long\"".to_string(),
            field_id_path: vec![],
            type_json: "\"long\"".to_string(),
            nullable: false,
            comment: Some("primary id".to_string()),
        })
        .unwrap()
    }

    #[test]
    fn table_column_and_independent_read_view_round_trip() {
        let view = IcebergReadView::new(HiveTransactionHandle::new(true, [3; 16]));
        let payload = IcebergReadWireCodec.encode_private(&view).unwrap();
        let decoded: IcebergReadView =
            decode_with(ConnectorCodecCategory::ReadView, &payload).unwrap();
        assert!(decoded.transaction().auto_commit());
        assert_eq!(decoded.transaction().uuid(), &[3; 16]);

        let column = primitive_column();
        let payload = IcebergReadWireCodec.encode_private(&column).unwrap();
        let decoded: IcebergColumnHandle =
            decode_with(ConnectorCodecCategory::ReadColumn, &payload).unwrap();
        assert_eq!(decoded, column);

        let relation = IcebergRuntimeRelation::SystemTable(
            IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
                schema_table_name: SchemaTableName::try_new("db", "events").unwrap(),
                system_table_type: IcebergSystemTableType::Files,
                metadata_file_location: "s3://warehouse/db/events/metadata/v1.json".to_string(),
                table_uuid: "00000000-0000-0000-0000-000000000007".to_string(),
                snapshot_id: Some(11),
            })
            .unwrap(),
        );
        let payload = IcebergReadWireCodec.encode_private(&relation).unwrap();
        let decoded: IcebergRuntimeRelation =
            decode_with(ConnectorCodecCategory::ReadTable, &payload).unwrap();
        match decoded {
            IcebergRuntimeRelation::SystemTable(value) => {
                assert_eq!(value.snapshot_id(), Some(11));
                assert_eq!(value.schema_table_name().table_name(), "events");
            }
            _ => panic!("expected system table relation"),
        }
    }

    #[test]
    fn split_round_trip_cross_checks_public_scheduling_facts() {
        let split = IcebergReadSplit::Data(
            IcebergSplit::try_new(IcebergSplitParams {
                path: "s3://warehouse/db/events/data.parquet".to_string(),
                start: 0,
                length: 32,
                file_size: 32,
                file_record_count: 1,
                file_format: IcebergFileFormat::Parquet,
                partition_spec_id: 0,
                read_domain: crate::typed_read::split::tests::read_domain(),
                partition_data_json: crate::typed_read::split::tests::partition_json(),
                deletes: vec![].into(),
                file_statistics_domain: TupleDomain::all(),
                data_sequence_number: Some(1),
                file_first_row_id: None,
                decryption_data: None,
                split_weight: SplitWeight::STANDARD,
                affinity_key: Some("events".to_string()),
            })
            .unwrap(),
        );
        let payload = IcebergReadWireCodec.encode_private(&split).unwrap();
        let facts = ConnectorReadSplitFacts::new(
            true,
            vec![],
            Some("events"),
            SplitWeight::STANDARD,
            split.retained_size_in_bytes(),
        );
        let header = header(ConnectorCodecCategory::ReadSplit);
        let mut ledger = ConnectorDecodeLedger::new(limits());
        let decoded = IcebergReadWireCodec
            .decode_split_private(
                &payload,
                &facts,
                &mut ConnectorDecodeContext::new(&header, &mut ledger),
            )
            .unwrap();
        assert_eq!(decoded.affinity_key(), Some("events"));

        let bad_facts = ConnectorReadSplitFacts::new(
            true,
            vec![],
            Some("events"),
            SplitWeight::STANDARD,
            split.retained_size_in_bytes() + 1,
        );
        let mut ledger = ConnectorDecodeLedger::new(limits());
        let error = IcebergReadWireCodec
            .decode_split_private(
                &payload,
                &bad_facts,
                &mut ConnectorDecodeContext::new(&header, &mut ledger),
            )
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorCodecErrorKind::InconsistentFields);
    }

    #[test]
    fn planned_suffix_wire_expansion_preserves_public_descriptor_charge() {
        use crate::delete_semantics::*;
        use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use crate::typed_read::split::IcebergSplitDeletes;
        use std::sync::Arc;
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "tenant",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .with_identifier_field_ids([2, 1])
            .build()
            .unwrap();
        let spec = crate::iceberg::spec::PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();
        let domain = Arc::new(ReadDomain::new(
            ReadObservationId::try_new([31; 16]).unwrap(),
            PinnedEndpointFacts::try_new(
                uuid::Uuid::from_bytes([32; 16]),
                "s3://warehouse/metadata/identifiers.json",
                11,
                &schema,
                &[spec.clone()],
            )
            .unwrap(),
        ));
        let table = IcebergRuntimeRelation::Table(
            IcebergTableHandle::try_new(IcebergTableHandleParams {
                schema_table_name: SchemaTableName::try_new("db", "identifier_events").unwrap(),
                snapshot_id: Some(11),
                read_domain: Some(domain.clone()),
                table_schema_json: domain.endpoint().schema_json().to_string(),
                spec_id: Some(0),
                partition_spec_jsons: domain
                    .endpoint()
                    .partition_spec_jsons()
                    .iter()
                    .map(|(id, json)| (*id, json.to_string()))
                    .collect(),
                format_version: 2,
                unenforced_predicate: TupleDomain::all(),
                enforced_predicate: TupleDomain::all(),
                limit: None,
                projected_columns: Default::default(),
                name_mapping_json: None,
                table_location: "s3://warehouse/identifier_events".to_string(),
                storage_properties: Default::default(),
                pinned_data_files: None,
            })
            .unwrap(),
        );
        let table_payload = IcebergReadWireCodec.encode_private(&table).unwrap();
        let decoded: IcebergRuntimeRelation =
            decode_with(ConnectorCodecCategory::ReadTable, &table_payload).unwrap();
        let IcebergRuntimeRelation::Table(decoded_table) = decoded else {
            unreachable!()
        };
        assert_eq!(decoded_table.read_domain(), Some(&domain));
        let identifiers = decoded_table
            .read_domain()
            .unwrap()
            .endpoint()
            .schema()
            .unwrap();
        assert_eq!(
            identifiers
                .identifier_field_ids()
                .collect::<std::collections::HashSet<_>>(),
            [1, 2].into_iter().collect()
        );
        let partition =
            TypedPartition::bind(&spec, &schema, &crate::iceberg::spec::Struct::empty()).unwrap();
        let data_path = "s3://warehouse/independently-frozen/data.parquet";
        let files = [
            (
                "s3://warehouse/delete/short.parquet",
                DeleteKind::Equality(EqualityFieldGroup::bind(&[2, 1], &schema).unwrap()),
                2,
            ),
            (
                "s3://warehouse/delete/a-much-longer-path.parquet",
                DeleteKind::Equality(EqualityFieldGroup::bind(&[2, 1], &schema).unwrap()),
                7,
            ),
            (
                "s3://warehouse/delete/position.parquet",
                DeleteKind::Position {
                    exact_target: Some(Arc::from(data_path)),
                },
                1,
            ),
        ];
        let observation = DeleteObservation::from_manifests([ManifestDeleteObservation {
            manifest_path: Arc::from("s3://warehouse/metadata/delete-manifest.avro"),
            entries: files
                .into_iter()
                .map(|(path, kind, sequence)| RawDeleteEntry {
                    sequence: EntrySequence {
                        format_version: crate::iceberg::spec::FormatVersion::V2,
                        status: crate::iceberg::spec::ManifestStatus::Existing,
                        data_sequence: Some(sequence),
                        manifest_sequence: 9,
                    },
                    file: RawDeleteFile {
                        address: DeleteContentAddress::file(path).unwrap(),
                        kind,
                        partition: partition.clone(),
                        read: DeleteReadFacts {
                            format: DeleteFormat::Parquet,
                            file_size: 32,
                            record_count: 2,
                            key_metadata: Arc::from([3u8, 9]),
                        },
                        metrics: FileMetrics::default(),
                    },
                })
                .collect(),
        }])
        .unwrap();
        let index = DeleteCandidateIndex::try_new(domain.clone(), observation).unwrap();
        for sequence in [1, 3, 8] {
            let data = DataFileFact::try_new(
                data_path,
                DataSequenceNumber::try_new(sequence).unwrap(),
                partition.clone(),
                10,
                FileMetrics::default(),
            )
            .unwrap();
            let view = index
                .for_data(&data)
                .unwrap()
                .load_view(StatisticsPolicy::Disabled);
            let split = IcebergReadSplit::Data(
                IcebergSplit::try_new(IcebergSplitParams {
                    read_domain: domain.clone(),
                    path: data_path.to_string(),
                    start: 0,
                    length: 64,
                    file_size: 64,
                    file_record_count: 10,
                    file_format: IcebergFileFormat::Parquet,
                    partition_spec_id: 0,
                    partition_data_json: partition.to_json_string(),
                    deletes: IcebergSplitDeletes::Planned(view),
                    file_statistics_domain: TupleDomain::all(),
                    data_sequence_number: Some(sequence),
                    file_first_row_id: None,
                    decryption_data: None,
                    split_weight: SplitWeight::STANDARD,
                    affinity_key: None,
                })
                .unwrap(),
            );
            let payload = IcebergReadWireCodec.encode_private(&split).unwrap();
            let facts = ConnectorReadSplitFacts::new(
                true,
                vec![],
                None::<String>,
                SplitWeight::STANDARD,
                split.retained_size_in_bytes(),
            );
            let header = header(ConnectorCodecCategory::ReadSplit);
            let mut ledger = ConnectorDecodeLedger::new(limits());
            let decoded = IcebergReadWireCodec
                .decode_split_private(
                    &payload,
                    &facts,
                    &mut ConnectorDecodeContext::new(&header, &mut ledger),
                )
                .unwrap();
            assert_eq!(
                decoded.retained_size_in_bytes(),
                split.retained_size_in_bytes()
            );
            let IcebergReadSplit::Data(before) = &split else {
                unreachable!()
            };
            let IcebergReadSplit::Data(after) = decoded else {
                unreachable!()
            };
            assert_eq!(
                before.deletes().to_vec().unwrap(),
                after.deletes().to_vec().unwrap()
            );
            assert!(matches!(after.deletes(), IcebergSplitDeletes::Decoded(_)));
            assert_eq!(after.read_domain(), decoded_table.read_domain().unwrap());
            assert_eq!(
                after.read_domain().endpoint().schema_json(),
                decoded_table.table_schema_json()
            );
        }
    }

    fn decode_view_bytes(payload: &[u8], limits: ConnectorDecodeLimits) -> ConnectorCodecError {
        let header = header(ConnectorCodecCategory::ReadView);
        let mut ledger = ConnectorDecodeLedger::new(limits);
        <IcebergReadWireCodec as ConnectorPrivateDecoder<IcebergReadView>>::decode_private(
            &IcebergReadWireCodec,
            payload,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
        )
        .unwrap_err()
    }

    #[test]
    fn strict_scanner_rejects_unknown_duplicate_wrong_wire_and_nested_corruption() {
        assert_eq!(
            decode_view_bytes(&[0x10, 1], limits()).kind(),
            ConnectorCodecErrorKind::UnknownField
        );
        assert_eq!(
            decode_view_bytes(&[0x08, 1], limits()).kind(),
            ConnectorCodecErrorKind::InvalidValue
        );

        let transaction = dto::HiveTransactionHandle {
            auto_commit: true,
            uuid: vec![1; 16],
        }
        .encode_to_vec();
        let mut duplicated = Vec::new();
        for _ in 0..2 {
            duplicated.push(0x0a);
            duplicated.push(transaction.len() as u8);
            duplicated.extend_from_slice(&transaction);
        }
        assert_eq!(
            decode_view_bytes(&duplicated, limits()).kind(),
            ConnectorCodecErrorKind::DuplicateField
        );

        let mut nested_unknown = transaction.clone();
        nested_unknown.extend_from_slice(&[0x18, 1]);
        let mut payload = vec![0x0a, nested_unknown.len() as u8];
        payload.extend_from_slice(&nested_unknown);
        assert_eq!(
            decode_view_bytes(&payload, limits()).kind(),
            ConnectorCodecErrorKind::UnknownField
        );

        let nested_wrong_wire = [0x08, 1, 0x10, 1];
        let mut payload = vec![0x0a, nested_wrong_wire.len() as u8];
        payload.extend_from_slice(&nested_wrong_wire);
        assert_eq!(
            decode_view_bytes(&payload, limits()).kind(),
            ConnectorCodecErrorKind::InvalidValue
        );
    }

    #[test]
    fn strict_scanner_enforces_nested_depth_and_scalar_budget() {
        let nested = dto::ColumnIdentity {
            field_id: 1,
            name: "root".to_string(),
            category: dto::ColumnIdentityCategory::Struct as i32,
            children: vec![dto::ColumnIdentity {
                field_id: 2,
                name: "child".to_string(),
                category: dto::ColumnIdentityCategory::Primitive as i32,
                children: vec![],
            }],
        };
        let raw = dto::IcebergColumnHandle {
            base_column_identity: Some(nested),
            base_type_json: "{}".to_string(),
            field_id_path: vec![1],
            type_json: "{}".to_string(),
            nullable: true,
            comment: None,
            scalar_integer_domain: None,
            nominal_read_domain: None,
        }
        .encode_to_vec();
        let header = header(ConnectorCodecCategory::ReadColumn);
        for limits in [
            ConnectorDecodeLimits::try_new(1 << 20, 1 << 20, 1 << 20, 100, 1).unwrap(),
            ConnectorDecodeLimits::try_new(1 << 20, 1 << 20, 1, 100, 64).unwrap(),
        ] {
            let mut ledger = ConnectorDecodeLedger::new(limits);
            let error = <IcebergReadWireCodec as ConnectorPrivateDecoder<IcebergColumnHandle>>::decode_private(
                &IcebergReadWireCodec, &raw, &mut ConnectorDecodeContext::new(&header, &mut ledger),
            ).unwrap_err();
            assert_eq!(error.kind(), ConnectorCodecErrorKind::Capacity);
        }
    }

    #[test]
    fn strict_private_value_wire_preserves_smallint_and_timestamp_millis() {
        for value in [
            ConnectorValue::SmallInt(i16::MIN),
            ConnectorValue::SmallInt(i16::MAX),
            ConnectorValue::TimestampMillis(-1_704_067_200_123),
            ConnectorValue::TimestampMillis(1_704_067_200_123),
        ] {
            let encoded = encode_value(&value).encode_to_vec();
            let header = header(ConnectorCodecCategory::ReadTable);
            let mut ledger = ConnectorDecodeLedger::new(limits());
            let raw: dto::Value = decode_root(
                &encoded,
                &mut ConnectorDecodeContext::new(&header, &mut ledger),
                ConnectorFieldPath::root("value"),
                Schema::Value,
            )
            .expect("strict value wire");
            assert_eq!(
                decode_value(
                    &raw,
                    value.value_type(),
                    &mut ConnectorDecodeContext::new(&header, &mut ledger)
                )
                .expect("decoded value"),
                value
            );
        }
    }
    #[test]
    fn scalar_integer_private_payload_roundtrip_is_closed_and_preserves_authority() {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
        use crate::scalar_integer_domain::ScalarIntegerDomain;
        let schema = IcebergSchema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                17,
                "tiny",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let column = IcebergColumnHandle::base_column_of(&schema, 17)
            .unwrap()
            .with_scalar_integer_domain(Some(ScalarIntegerDomain::Int8))
            .unwrap();
        let payload = IcebergReadWireCodec.encode_private(&column).unwrap();
        let decoded: IcebergColumnHandle =
            decode_with(ConnectorCodecCategory::ReadColumn, &payload).unwrap();
        assert_eq!(decoded, column);
        assert_eq!(column.dereference(&[]).unwrap(), column);
        let mut invalid = encode_column(&column);
        invalid.scalar_integer_domain = Some("int32".to_string());
        assert!(
            decode_with::<IcebergColumnHandle>(
                ConnectorCodecCategory::ReadColumn,
                &invalid.encode_to_vec()
            )
            .is_err()
        );
        invalid.scalar_integer_domain = Some("tinyint".to_string());
        invalid.type_json = serde_json::to_string(&Type::Primitive(PrimitiveType::Long)).unwrap();
        invalid.base_type_json = invalid.type_json.clone();
        assert!(
            decode_with::<IcebergColumnHandle>(
                ConnectorCodecCategory::ReadColumn,
                &invalid.encode_to_vec()
            )
            .is_err()
        );
    }

    #[test]
    fn nominal_table_and_column_roundtrips_preserve_independent_frozen_declarations() {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
        use crate::nominal_read_domain::NominalReadDomain;
        let schema = IcebergSchema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "n",
                    Type::Primitive(PrimitiveType::Fixed(16)),
                )),
                Arc::new(NestedField::required(
                    2,
                    "h",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "b",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "raw",
                    Type::Primitive(PrimitiveType::Fixed(16)),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "u",
                    Type::Primitive(PrimitiveType::Uuid),
                )),
            ])
            .build()
            .unwrap();
        let domains = BTreeMap::from([
            (1, NominalReadDomain::LargeInt),
            (2, NominalReadDomain::Hll),
            (3, NominalReadDomain::Bitmap),
        ]);
        let table = IcebergTableHandle::try_new(IcebergTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "nominal").unwrap(),
            snapshot_id: None,
            read_domain: None,
            table_schema_json: serde_json::to_string(&schema).unwrap(),
            spec_id: None,
            partition_spec_jsons: BTreeMap::new(),
            format_version: 3,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            projected_columns: BTreeSet::new(),
            name_mapping_json: None,
            table_location: "memory://warehouse/db/nominal".into(),
            storage_properties: BTreeMap::new(),
            pinned_data_files: None,
        })
        .unwrap()
        .with_nominal_read_domains(domains.clone())
        .unwrap();
        assert_eq!(
            IcebergTableHandle::from_proto(&table.to_proto()).unwrap(),
            table
        );
        let relation = IcebergRuntimeRelation::Table(table.clone());
        let payload = IcebergReadWireCodec.encode_private(&relation).unwrap();
        let decoded: IcebergRuntimeRelation =
            decode_with(ConnectorCodecCategory::ReadTable, &payload).unwrap();
        let IcebergRuntimeRelation::Table(decoded_table) = decoded else {
            panic!("expected nominal table");
        };
        assert_eq!(decoded_table, table);
        for id in 1..=5 {
            let column = IcebergColumnHandle::base_column_of(&schema, id)
                .unwrap()
                .with_nominal_read_domain(domains.get(&id).copied())
                .unwrap();
            assert_eq!(
                IcebergColumnHandle::from_proto(&column.to_proto()).unwrap(),
                column
            );
            let bytes = IcebergReadWireCodec.encode_private(&column).unwrap();
            let roundtrip: IcebergColumnHandle =
                decode_with(ConnectorCodecCategory::ReadColumn, &bytes).unwrap();
            assert_eq!(roundtrip, column);
            assert_eq!(column.dereference(&[]).unwrap(), column);
        }
        let mut wrong = table.to_proto();
        wrong.nominal_read_domains.insert(5, "largeint".into());
        assert!(
            IcebergTableHandle::from_proto(&wrong).is_err(),
            "UUID cannot receive a width-derived declaration"
        );
        let mut ledger = ConnectorDecodeLedger::new(
            ConnectorDecodeLimits::try_new(1 << 20, 1, 1 << 20, 100_000, 64).unwrap(),
        );
        let error = <IcebergReadWireCodec as ConnectorPrivateDecoder<IcebergRuntimeRelation>>::decode_private(
            &IcebergReadWireCodec, &payload,
            &mut ConnectorDecodeContext::new(&header(ConnectorCodecCategory::ReadTable), &mut ledger),
        ).unwrap_err();
        assert_eq!(error.kind(), ConnectorCodecErrorKind::Capacity);
    }
}

#[cfg(test)]
mod change_window_wire_tests {
    use super::*;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEnvelopeHeader, ConnectorInstanceId,
        ConnectorProviderId,
    };

    fn scan<T: Message + Default>(bytes: &[u8], schema: Schema) -> Result<T, ConnectorCodecError> {
        let header = ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse(crate::PROVIDER_ID).unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                CatalogVersion::from_bytes([7; 32]),
            ),
            ConnectorCodecCategory::ReadSplit,
            ConnectorCodecRevision::try_new(crate::contract_revision::ICEBERG_CONTRACT_REVISION)
                .unwrap(),
        );
        let mut ledger = ConnectorDecodeLedger::new(
            ConnectorDecodeLimits::try_new(1 << 20, 1 << 20, 1 << 20, 100_000, 64).unwrap(),
        );
        decode_root(
            bytes,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
            ConnectorFieldPath::root("change_split"),
            schema,
        )
    }

    #[test]
    fn retired_partial_change_variants_and_removed_file_prior_field_are_rejected() {
        for retired in [11_u8, 12_u8] {
            let error =
                scan::<dto::IcebergChangeSplit>(&[(retired << 3) | 2, 0], Schema::ChangeSplit)
                    .unwrap_err();
            assert_eq!(error.kind(), ConnectorCodecErrorKind::UnknownField);
        }
        let error =
            scan::<dto::IcebergDeletedDataFileRows>(&[0x12, 0], Schema::DeletedDataFileRows)
                .unwrap_err();
        assert_eq!(error.kind(), ConnectorCodecErrorKind::UnknownField);
    }

    #[test]
    fn complete_endpoint_wire_has_one_owner_and_validates_both_delete_lists() {
        let difference = dto::IcebergVisibilityDifference {
            data: None,
            from_deletes: vec![],
            to_deletes: vec![],
        };
        let raw = dto::IcebergChangeSplit {
            rows: Some(dto::iceberg_change_split::Rows::VisibilityDifference(
                difference,
            )),
        };
        let encoded = raw.encode_to_vec();
        let decoded: dto::IcebergChangeSplit = scan(&encoded, Schema::ChangeSplit).unwrap();
        assert_eq!(decoded, raw);
        let mut duplicated = encoded.clone();
        duplicated.extend_from_slice(&encoded);
        assert_eq!(
            scan::<dto::IcebergChangeSplit>(&duplicated, Schema::ChangeSplit)
                .unwrap_err()
                .kind(),
            ConnectorCodecErrorKind::DuplicateField
        );
        // Both complete endpoint lists recurse through the strict delete schema.
        for field in [2_u8, 3_u8] {
            let invalid_delete = [(field << 3) | 2, 2, 0xf8, 0x01];
            assert!(
                scan::<dto::IcebergVisibilityDifference>(
                    &invalid_delete,
                    Schema::VisibilityDifference
                )
                .is_err()
            );
        }
    }
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

    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        refuse: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            trace.push((phase, units));
            if let Some((index, cause)) = self.refuse
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
            ConnectorCodecRevision::try_new(crate::contract_revision::ICEBERG_CONTRACT_REVISION)
                .unwrap(),
        )
    }
    fn ledger() -> ConnectorDecodeLedger {
        ConnectorDecodeLedger::new(
            ConnectorDecodeLimits::try_new(4 << 20, 8 << 20, 4 << 20, 100_000, 64).unwrap(),
        )
    }
    fn column() -> IcebergColumnHandle {
        IcebergColumnHandle::try_new(IcebergColumnHandleParams {
            base_column_identity: ColumnIdentity::try_new(
                1,
                "id",
                ColumnIdentityCategory::Primitive,
                vec![],
            )
            .unwrap(),
            base_type_json: "\"long\"".into(),
            field_id_path: vec![],
            type_json: "\"long\"".into(),
            nullable: false,
            comment: Some("中文-column-comment-".repeat(321)),
        })
        .unwrap()
    }
    fn relations() -> Vec<IcebergRuntimeRelation> {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let spec = crate::iceberg::spec::PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();
        let domain =
            crate::delete_semantics::test_read_domain(&schema, std::slice::from_ref(&spec), 11);
        let schema_json = domain.endpoint().schema_json().to_string();
        let specs: BTreeMap<_, _> = domain
            .endpoint()
            .partition_spec_jsons()
            .iter()
            .map(|(id, json)| (*id, json.to_string()))
            .collect();
        let name = SchemaTableName::try_new("db", "events").unwrap();
        let location = "s3://warehouse/db/events";
        let table = IcebergTableHandle::try_new(IcebergTableHandleParams {
            schema_table_name: name.clone(),
            snapshot_id: Some(11),
            read_domain: Some(domain.clone()),
            table_schema_json: schema_json.clone(),
            spec_id: Some(0),
            partition_spec_jsons: specs.clone(),
            format_version: 2,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            projected_columns: [column()].into_iter().collect(),
            name_mapping_json: None,
            table_location: location.into(),
            storage_properties: (0..321)
                .map(|i| (format!("property-{i}"), format!("value-{i}")))
                .collect(),
            pinned_data_files: None,
        })
        .unwrap();
        let table_function =
            TableChangesFunctionHandle::try_new(TableChangesFunctionHandleParams {
                schema_table_name: name.clone(),
                table_schema_json: schema_json.clone(),
                columns: vec![column()],
                name_mapping_json: None,
                start_snapshot_id: 10,
                end_snapshot_id: 11,
            })
            .unwrap();
        let window = IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
            schema_table_name: name.clone(),
            table_schema_json: schema_json.clone(),
            columns: vec![column()],
            name_mapping_json: None,
            from_snapshot_id_exclusive: 10,
            to_snapshot_id_inclusive: 11,
            from_read_domain: crate::delete_semantics::test_read_domain(
                &schema,
                std::slice::from_ref(&spec),
                10,
            ),
            to_read_domain: domain,
            partition_spec_jsons: specs,
        })
        .unwrap();
        let system = IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
            schema_table_name: name.clone(),
            system_table_type: IcebergSystemTableType::Files,
            metadata_file_location: "s3://warehouse/metadata/v1.json".into(),
            table_uuid: "00000000-0000-0000-0000-000000000007".into(),
            snapshot_id: Some(11),
        })
        .unwrap();
        let execute = IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
            schema_table_name: name.clone(),
            procedure_id: IcebergProcedureId::ExpireSnapshots,
            table_location: location.into(),
            procedure_handle: None,
        })
        .unwrap();
        let insert = IcebergInsertTableHandle::try_new(IcebergInsertTableHandleParams {
            schema_table_name: name,
            table_schema_json: schema_json,
            table_location: location.into(),
            format_version: 2,
            spec_id: Some(0),
        })
        .unwrap();
        let merge = IcebergMergeTableHandle::try_new(table.clone(), insert).unwrap();
        vec![
            IcebergRuntimeRelation::Table(table),
            IcebergRuntimeRelation::TableFunction(table_function),
            IcebergRuntimeRelation::ChangeWindow(window),
            IcebergRuntimeRelation::SystemTable(system),
            IcebergRuntimeRelation::TableExecute(execute),
            IcebergRuntimeRelation::MergeTable(merge),
        ]
    }
    fn run_column(
        payload: &[u8],
        control: &Control,
    ) -> Result<IcebergColumnHandle, ConnectorCodecError> {
        let expected = header(ConnectorCodecCategory::ReadColumn);
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, control)?;
        IcebergReadWireCodec.decode_private(payload, &mut context)
    }

    #[test]
    fn actual_six_relation_variants_keep_legacy_values_and_ledger_charges() {
        let expected = header(ConnectorCodecCategory::ReadTable);
        for relation in relations() {
            let payload = IcebergReadWireCodec.encode_private(&relation).unwrap();
            let mut legacy_budget = ledger();
            let old: IcebergRuntimeRelation = IcebergReadWireCodec
                .decode_private(
                    &payload,
                    &mut ConnectorDecodeContext::new(&expected, &mut legacy_budget),
                )
                .unwrap();
            let control = Control::default();
            let mut compile_budget = ledger();
            let new: IcebergRuntimeRelation = IcebergReadWireCodec
                .decode_private(
                    &payload,
                    &mut ConnectorDecodeContext::try_new_for_compile(
                        &expected,
                        &mut compile_budget,
                        &control,
                    )
                    .unwrap(),
                )
                .unwrap();
            assert_eq!(IcebergReadWireCodec.encode_private(&old).unwrap(), payload);
            assert_eq!(IcebergReadWireCodec.encode_private(&new).unwrap(), payload);
            assert_eq!(legacy_budget.raw_bytes(), compile_budget.raw_bytes());
            assert_eq!(legacy_budget.items(), compile_budget.items());
            assert_eq!(legacy_budget.scalar_bytes(), compile_budget.scalar_bytes());
            assert_eq!(
                legacy_budget.retained_bytes(),
                compile_budget.retained_bytes()
            );
            let trace = control.trace.lock().unwrap();
            assert!(
                trace
                    .iter()
                    .all(|(phase, units)| *phase == CompilePhase::ProviderValidation
                        && *units <= 256)
            );
            assert_eq!(trace.last().unwrap().1, 1);
        }
    }

    #[test]
    fn actual_long_column_refuses_entry_copy_interior_and_publication_each_cause() {
        let payload = IcebergReadWireCodec.encode_private(&column()).unwrap();
        let baseline = Control::default();
        assert_eq!(run_column(&payload, &baseline).unwrap(), column());
        let trace = baseline.trace.lock().unwrap().clone();
        let positives: Vec<_> = trace
            .iter()
            .enumerate()
            .filter_map(|(i, (_, units))| (*units == 256).then_some(i + 1))
            .collect();
        assert!(positives.len() > 10);
        let mut checkpoints = vec![
            1,
            positives[0],
            positives[positives.len() / 2],
            *positives.last().unwrap(),
            trace.len(),
        ];
        checkpoints.sort_unstable();
        checkpoints.dedup();
        for cause in causes() {
            for &index in &checkpoints {
                let control = Control {
                    trace: Default::default(),
                    refuse: Some((index, cause)),
                };
                let error = run_column(&payload, &control).unwrap_err();
                assert_eq!(error.compile_control_error(), Some(cause));
                assert_eq!(error.kind(), ConnectorCodecErrorKind::CompileControl(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            }
        }
    }

    #[test]
    fn actual_packed_path_scanner_observes_every_consumed_item_without_domain_guesses() {
        let expected = header(ConnectorCodecCategory::ReadColumn);
        let raw = dto::IcebergColumnHandle {
            field_id_path: (0..321).collect(),
            ..Default::default()
        };
        let payload = raw.encode_to_vec();
        let baseline = Control::default();
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &baseline).unwrap();
        let decoded: dto::IcebergColumnHandle = decode_root(
            &payload,
            &mut context,
            ConnectorFieldPath::root("packed"),
            Schema::Column,
        )
        .unwrap();
        assert_eq!(decoded, raw);
        assert_eq!(budget.items(), 322);
        let trace = baseline.trace.lock().unwrap().clone();
        let index = trace.iter().position(|(_, units)| *units == 256).unwrap() + 1;
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((index, cause)),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            assert_eq!(
                decode_root::<dto::IcebergColumnHandle>(
                    &payload,
                    &mut context,
                    ConnectorFieldPath::root("packed"),
                    Schema::Column
                )
                .unwrap_err()
                .compile_control_error(),
                Some(cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
        }
    }

    #[test]
    fn actual_identity_children_and_predicate_ranges_are_observed_in_source_order() {
        let expected = header(ConnectorCodecCategory::ReadColumn);
        let identity = dto::ColumnIdentity {
            field_id: 999,
            name: "record".into(),
            category: dto::ColumnIdentityCategory::Struct as i32,
            children: (1..=321)
                .map(|id| dto::ColumnIdentity {
                    field_id: id,
                    name: format!("f{id}"),
                    category: dto::ColumnIdentityCategory::Primitive as i32,
                    children: vec![],
                })
                .collect(),
        };
        let bound = |n| dto::Bound {
            kind: dto::BoundKind::Inclusive as i32,
            value: Some(dto::Value {
                value: Some(dto::value::Value::BigInt(n)),
            }),
        };
        let domain = dto::Domain {
            values: Some(dto::ValueSet {
                value_type: Some(encode_value_type(ConnectorValueType::BigInt)),
                ranges: (0..321)
                    .map(|n| dto::Range {
                        low: Some(bound(n * 3)),
                        high: Some(bound(n * 3 + 1)),
                    })
                    .collect(),
            }),
            null_allowed: true,
        };
        let mut budget = ledger();
        let baseline = Control::default();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &baseline).unwrap();
        let decoded = decode_identity(&identity, &mut context).unwrap();
        assert_eq!(encode_identity(&decoded), identity);
        let decoded = decode_domain(&domain, &mut context).unwrap();
        assert_eq!(decoded.values().ranges().len(), 321);
        assert_eq!(encode_domain(&decoded), domain);
        // Every child and range must pass its own opaque-domain entry/exit,
        // even when the pending units never reach 256 before a flush.
        assert!(baseline.trace.lock().unwrap().len() > 321 * 4);
        for cause in causes() {
            for use_domain in [false, true] {
                let control = Control {
                    trace: Default::default(),
                    refuse: Some((700, cause)),
                };
                let mut budget = ledger();
                let mut context =
                    ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                        .unwrap();
                let error = if use_domain {
                    decode_domain(&domain, &mut context).unwrap_err()
                } else {
                    decode_identity(&identity, &mut context).unwrap_err()
                };
                assert_eq!(error.compile_control_error(), Some(cause));
                let calls = control.trace.lock().unwrap().len();
                assert_eq!(calls, 700);
                assert_eq!(
                    context
                        .flush_compile_control()
                        .unwrap_err()
                        .compile_control_error(),
                    Some(cause)
                );
                assert_eq!(control.trace.lock().unwrap().len(), calls);
            }
        }
    }

    #[test]
    fn predicate_owned_string_and_binary_copies_keep_values_and_latch_original_controls() {
        let expected = header(ConnectorCodecCategory::ReadColumn);
        for (raw, ty) in [
            (
                dto::Value {
                    value: Some(dto::value::Value::Varchar("é文".repeat(321))),
                },
                ConnectorValueType::Varchar,
            ),
            (
                dto::Value {
                    value: Some(dto::value::Value::Varbinary([0, 255, 7].repeat(321))),
                },
                ConnectorValueType::Varbinary,
            ),
            (
                dto::Value {
                    value: Some(dto::value::Value::Fixed(vec![13; 321])),
                },
                ConnectorValueType::Fixed { length: 321 },
            ),
        ] {
            let mut budget = ledger();
            let legacy = decode_value(
                &raw,
                ty,
                &mut ConnectorDecodeContext::new(&expected, &mut budget),
            )
            .unwrap();
            assert_eq!(encode_value(&legacy), raw);
            for cause in causes() {
                let control = Control {
                    trace: Default::default(),
                    refuse: Some((3, cause)),
                };
                let mut budget = ledger();
                let mut context =
                    ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                        .unwrap();
                assert_eq!(
                    decode_value(&raw, ty, &mut context)
                        .unwrap_err()
                        .compile_control_error(),
                    Some(cause)
                );
                assert_eq!(control.trace.lock().unwrap()[2].1, 256);
                assert_eq!(
                    context
                        .flush_compile_control()
                        .unwrap_err()
                        .compile_control_error(),
                    Some(cause)
                );
                assert_eq!(control.trace.lock().unwrap().len(), 3);
            }
        }
    }

    #[test]
    fn ordinary_domain_error_flushes_original_tail_without_text_classification() {
        let expected = header(ConnectorCodecCategory::ReadColumn);
        let raw = dto::ColumnIdentity {
            field_id: 1,
            name: String::new(),
            category: dto::ColumnIdentityCategory::Primitive as i32,
            children: vec![],
        };
        let baseline = Control::default();
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &baseline).unwrap();
        let ordinary = decode_identity(&raw, &mut context).unwrap_err();
        assert_eq!(ordinary.compile_control_error(), None);
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(trace.len() > 3);
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((trace.len(), cause)),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            assert_eq!(
                decode_identity(&raw, &mut context)
                    .unwrap_err()
                    .compile_control_error(),
                Some(cause)
            );
        }
        let ordinary_text = codec_error(
            "ordinary",
            ConnectorCodecErrorKind::InvalidValue,
            "cancelled deadline resource exhausted",
        );
        assert_eq!(ordinary_text.compile_control_error(), None);
    }

    #[test]
    fn malformed_prost_utf8_flushes_after_opaque_decode_and_keeps_legacy_error() {
        let expected = header(ConnectorCodecCategory::ReadColumn);
        let payload = [0x12, 1, 0xff]; // Column.base_type_json: structurally valid, invalid UTF-8.
        let baseline = Control::default();
        let ordinary = run_column(&payload, &baseline).unwrap_err();
        assert_eq!(ordinary.compile_control_error(), None);
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(
            ordinary
                .detail()
                .contains("malformed Iceberg private protobuf")
        );
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((trace.len(), cause)),
            };
            assert_eq!(
                run_column(&payload, &control)
                    .unwrap_err()
                    .compile_control_error(),
                Some(cause)
            );
        }
        let mut budget = ledger();
        let old: Result<IcebergColumnHandle, _> = IcebergReadWireCodec.decode_private(
            &payload,
            &mut ConnectorDecodeContext::new(&expected, &mut budget),
        );
        assert_eq!(old.unwrap_err().detail(), ordinary.detail());
    }

    #[test]
    fn varint_checkpoint_follows_consumed_byte_and_does_not_count_reservation() {
        let expected = header(ConnectorCodecCategory::ReadColumn);
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((2, cause)),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            copy_slice(&[0u8; 255], &mut context).unwrap();
            let mut input: &[u8] = &[7];
            assert_eq!(
                read_varint(
                    &mut input,
                    &mut context,
                    &ConnectorFieldPath::root("varint")
                )
                .unwrap_err()
                .compile_control_error(),
                Some(cause)
            );
            assert!(input.is_empty());
            assert_eq!(control.trace.lock().unwrap()[1].1, 256);
        }
    }
    #[test]
    fn actual_table_scalar_domains_and_private_maps_keep_declared_source_and_control() {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        let schema = Schema::builder()
            .with_fields(
                (1..=321)
                    .map(|id| {
                        Arc::new(NestedField::required(
                            id,
                            format!("i{id}"),
                            Type::Primitive(PrimitiveType::Int),
                        ))
                    })
                    .collect::<Vec<_>>(),
            )
            .build()
            .unwrap();
        let table = IcebergTableHandle::try_new(IcebergTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "integers").unwrap(),
            snapshot_id: None,
            read_domain: None,
            table_schema_json: serde_json::to_string(&schema).unwrap(),
            spec_id: None,
            partition_spec_jsons: BTreeMap::new(),
            format_version: 2,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            projected_columns: BTreeSet::new(),
            name_mapping_json: None,
            table_location: "s3://warehouse/integers".into(),
            storage_properties: (0..321)
                .map(|n| (format!("key-{n}"), "value".into()))
                .collect(),
            pinned_data_files: None,
        })
        .unwrap()
        .with_scalar_integer_domains(
            (1..=321)
                .map(|id| (id, crate::scalar_integer_domain::ScalarIntegerDomain::Int8))
                .collect(),
        )
        .unwrap();
        let raw = encode_table(&table);
        let expected = header(ConnectorCodecCategory::ReadTable);
        let baseline = Control::default();
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &baseline).unwrap();
        let decoded = decode_table(&raw, &mut context).unwrap();
        assert_eq!(decoded.scalar_integer_domains().len(), 321);
        assert_eq!(encode_table(&decoded), raw);
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(trace.iter().any(|(_, units)| *units == 256));
        assert!(trace.len() > 321 * 2);
        // The scalar-domain loop follows actual table construction and the
        // independently observed string/map copies, then precedes publication.
        for cause in causes() {
            for index in [trace.len() - 100, trace.len()] {
                let control = Control {
                    trace: Default::default(),
                    refuse: Some((index, cause)),
                };
                let mut budget = ledger();
                let mut context =
                    ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                        .unwrap();
                assert_eq!(
                    decode_table(&raw, &mut context)
                        .unwrap_err()
                        .compile_control_error(),
                    Some(cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            }
        }
    }

    /// A legal wide nominal declaration map uses the existing retained policy
    /// and preserves typed interruption through actual private decode.
    /// This owner-local test has no external resources; run on codec changes.
    #[test]
    fn actual_nominal_map_decode_accounts_entries_and_preserves_control_causes() {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use crate::nominal_read_domain::NominalReadDomain;
        let schema = Schema::builder()
            .with_fields(
                (1..=257)
                    .map(|id| {
                        Arc::new(NestedField::optional(
                            id,
                            format!("n{id}"),
                            Type::Primitive(PrimitiveType::Fixed(16)),
                        ))
                    })
                    .collect::<Vec<_>>(),
            )
            .build()
            .unwrap();
        let base = IcebergTableHandle::try_new(IcebergTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "nominals").unwrap(),
            snapshot_id: None,
            read_domain: None,
            table_schema_json: serde_json::to_string(&schema).unwrap(),
            spec_id: None,
            partition_spec_jsons: BTreeMap::new(),
            format_version: 2,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            projected_columns: BTreeSet::new(),
            name_mapping_json: None,
            table_location: "memory://nominal-map".into(),
            storage_properties: BTreeMap::new(),
            pinned_data_files: None,
        })
        .unwrap();
        let expected = header(ConnectorCodecCategory::ReadTable);
        let mut base_budget = ledger();
        let mut context = ConnectorDecodeContext::new(&expected, &mut base_budget);
        let base_raw = IcebergReadWireCodec
            .encode_private(&IcebergRuntimeRelation::Table(base.clone()))
            .unwrap();
        let _: IcebergRuntimeRelation = IcebergReadWireCodec
            .decode_private(&base_raw, &mut context)
            .unwrap();

        let table = base
            .with_nominal_read_domains(
                (1..=257)
                    .map(|id| (id, NominalReadDomain::LargeInt))
                    .collect(),
            )
            .unwrap();
        let raw = IcebergReadWireCodec
            .encode_private(&IcebergRuntimeRelation::Table(table.clone()))
            .unwrap();
        let baseline = Control::default();
        let mut budget = ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &baseline).unwrap();
        let decoded: IcebergRuntimeRelation = IcebergReadWireCodec
            .decode_private(&raw, &mut context)
            .unwrap();
        let IcebergRuntimeRelation::Table(decoded) = decoded else {
            panic!("expected table relation");
        };
        assert_eq!(decoded, table);
        // This is the declared preflight policy, not an allocator/RSS claim.
        assert!(budget.retained_bytes() >= base_budget.retained_bytes() + 257 * 256);
        let trace = baseline.trace.lock().unwrap().clone();
        for cause in causes() {
            let control = Control {
                trace: Default::default(),
                refuse: Some((trace.len(), cause)),
            };
            let mut budget = ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut budget, &control)
                    .unwrap();
            assert_eq!(
                <IcebergReadWireCodec as ConnectorPrivateDecoder<IcebergRuntimeRelation>>::decode_private(&IcebergReadWireCodec, &raw, &mut context)
                    .unwrap_err()
                    .compile_control_error(),
                Some(cause)
            );
        }
        let mut limited = ConnectorDecodeLedger::new(
            ConnectorDecodeLimits::try_new(
                4 << 20,
                budget.retained_bytes() - 1,
                4 << 20,
                100_000,
                64,
            )
            .unwrap(),
        );
        let mut context = ConnectorDecodeContext::new(&expected, &mut limited);
        assert_eq!(
            <IcebergReadWireCodec as ConnectorPrivateDecoder<IcebergRuntimeRelation>>::decode_private(&IcebergReadWireCodec, &raw, &mut context).unwrap_err().kind(),
            ConnectorCodecErrorKind::Capacity
        );
    }
}
