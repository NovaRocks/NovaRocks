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

//! The worker-side reader for Iceberg system relations.
//!
//! Files and entries read one manifest per work unit. Snapshots, history,
//! refs and manifests each read one indivisible relation work unit.
//! The reader opens the frozen metadata file, proves it is the file planning
//! froze through [`IcebergSystemTableReference::verify_loaded_metadata`], and
//! produces pages straight from it. It never re-resolves the table through the
//! catalog and never falls back to a later snapshot: either would answer a
//! different question than the one that was planned.
//!
//! The Provider owns the exact metadata declarations. Bounds use the current
//! schema's types in Files and field-ID keyed text maps in Entries.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryBuilder, Float32Array, Float64Array, Int32Array, Int32Builder, Int64Array,
    Int64Builder, ListBuilder, MapBuilder, MapFieldNames, StringArray, StringBuilder, StructArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
#[cfg(test)]
use arrow::datatypes::Fields;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use bytes::Bytes;
use novarocks_connector_contract::PureProviderCompileError;
use novarocks_fs::{FileReadContext, FileReadRange};
use novarocks_spi::connector::read_stack::{
    ConnectorPollBudget, ConnectorReadSchemaCreation, OwnedConnectorPageStream, PageSourceMetrics,
    SourcePage,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, FunctionValueType, ValueLogicalType, ValueTypeError,
};

use crate::access_binding::IcebergReadBinding;
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Datum, FieldSummary, ListType, Literal, Manifest,
    ManifestFile, ManifestList, ManifestStatus, MapType, NestedField, PartitionSpec,
    PrimitiveLiteral, PrimitiveType, Schema, SnapshotRetention, StructType, TableMetadata, Type,
};

use super::column_handle::{IcebergColumnHandle, corrupt, invalid, type_to_json, unsupported};
use super::page_source::{MaterializedPageStream, MaterializedPages, SplitOperations};
use super::system_table::{
    IcebergSystemTableReference, IcebergSystemTableType, SystemManifestSplit, SystemTableScan,
    TrinoManifestFile,
};

/// The Arrow rendering of `TIMESTAMP WITH TIME ZONE`.
///
/// Iceberg stamps snapshot and history times in UTC milliseconds. Widening to
/// microseconds is exact, and keeping the zone on the Arrow field is what makes
/// the column a *zoned* timestamp rather than an instant that merely happens to
/// be UTC.
const UTC_TIME_ZONE: &str = "UTC";

/// The Iceberg map-entry field names, which the declared Map types and the
/// `MapBuilder`s below must agree on down to the entry struct's name.
fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

#[cfg(test)]
fn map_type(key: DataType, value: DataType) -> DataType {
    let entries = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("key", key, false)),
        Arc::new(Field::new("value", value, true)),
    ]));
    DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
}

#[cfg(test)]
fn list_type(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item, true)))
}

fn timestamp_tz_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into()))
}

/// Physical declarations for the accepted public metadata columns.
pub(crate) fn system_column_domain(kind: IcebergSystemTableType, name: &str) -> ValueLogicalType {
    match (kind, name) {
        (IcebergSystemTableType::Files | IcebergSystemTableType::Entries, "readable_metrics") => {
            ValueLogicalType::Json
        }
        _ => ValueLogicalType::Physical,
    }
}
// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Physical source carriers for the Provider's typed metadata values.
fn iceberg_primitive_to_arrow(primitive: &PrimitiveType) -> Result<DataType, ConnectorError> {
    Ok(match primitive {
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::Long => DataType::Int64,
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Decimal { precision, scale } => DataType::Decimal128(
            u8::try_from(*precision).map_err(|_| {
                invalid(format!(
                    "iceberg decimal precision {precision} is out of range"
                ))
            })?,
            i8::try_from(*scale)
                .map_err(|_| invalid(format!("iceberg decimal scale {scale} is out of range")))?,
        ),
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
        PrimitiveType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        PrimitiveType::Timestamptz => timestamp_tz_type(),
        PrimitiveType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
        PrimitiveType::TimestamptzNs => {
            DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC_TIME_ZONE.into()))
        }
        PrimitiveType::String => DataType::Utf8,
        PrimitiveType::Uuid => DataType::FixedSizeBinary(16),
        PrimitiveType::Fixed(width) => DataType::FixedSizeBinary(
            i32::try_from(*width)
                .map_err(|_| invalid(format!("iceberg fixed width {width} is out of range")))?,
        ),
        PrimitiveType::Binary => DataType::Binary,
    })
}

/// Hands out the field IDs a metadata relation's column identities carry.
///
/// A metadata relation is not an Iceberg table: its columns have no
/// format-assigned IDs, because no manifest ever describes them. The IDs below
/// exist only to make each published identity distinct and internally
/// consistent -- a scan of a system relation is resolved by column *name*
/// against the frozen schema (see [`project_system_relation_columns`]), never
/// by ID. They are minted fresh for every derivation and are never compared
/// against a table field.
struct MetadataRelationFieldIds<'a> {
    next: i32,
    supplied: Option<std::slice::Iter<'a, i32>>,
}

impl<'a> MetadataRelationFieldIds<'a> {
    /// Iceberg field IDs are positive, so the first one is 1.
    const fn new() -> Self {
        Self {
            next: 1,
            supplied: None,
        }
    }

    fn supplied(ids: &'a [i32]) -> Self {
        Self {
            next: 1,
            supplied: Some(ids.iter()),
        }
    }

    fn take(&mut self) -> Result<i32, ConnectorError> {
        if let Some(ids) = self.supplied.as_mut() {
            return ids
                .next()
                .copied()
                .ok_or_else(|| invalid("system column identity has too few mirror fields"));
        }
        let field_id = self.next;
        self.next = self.next.checked_add(1).ok_or_else(|| {
            internal("iceberg metadata relation exhausted its column identity field ids")
        })?;
        Ok(field_id)
    }
}

/// The Iceberg mirror of one frozen metadata-relation Arrow field.
/// The legacy schema authors use the same mirror without compile callbacks.
fn metadata_relation_field(
    field: &Field,
    ids: &mut MetadataRelationFieldIds<'_>,
) -> Result<NestedField, ConnectorError> {
    metadata_relation_field_observed(field, ids, &mut || Ok(()))
}
fn metadata_relation_type(
    data_type: &DataType,
    ids: &mut MetadataRelationFieldIds<'_>,
) -> Result<Type, ConnectorError> {
    metadata_relation_type_observed(data_type, ids, &mut || Ok(()))
}

fn metadata_relation_field_observed<E: From<ConnectorError>>(
    field: &Field,
    ids: &mut MetadataRelationFieldIds<'_>,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<NestedField, E> {
    let field_type = metadata_relation_type_observed(field.data_type(), ids, observe)?;
    let field_id = ids.take()?;
    let field = if field.is_nullable() {
        NestedField::optional(field_id, field.name(), field_type)
    } else {
        NestedField::required(field_id, field.name(), field_type)
    };
    observe()?;
    Ok(field)
}

/// The Iceberg mirror of one frozen metadata-relation Arrow type.
///
/// This is the inverse of [`iceberg_primitive_to_arrow`] over exactly the
/// carriers the frozen schemas above produce, plus the three constructors they
/// nest. It is total on that set and refuses everything else rather than
/// widening: a carrier this function has not been told about would otherwise be
/// published under a type the relation never produces.
///
/// `Utf8` becomes `string`, which is the metadata relation's own frozen column
/// type and not a downgrade of some base-table `uuid`: the frozen schema
/// already renders a UUID partition value or bound as text, so `string` is what
/// the column *is* here.
fn metadata_relation_type_observed<E: From<ConnectorError>>(
    data_type: &DataType,
    ids: &mut MetadataRelationFieldIds<'_>,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<Type, E> {
    let primitive = match data_type {
        DataType::Boolean => Some(PrimitiveType::Boolean),
        DataType::Int32 => Some(PrimitiveType::Int),
        DataType::Int64 => Some(PrimitiveType::Long),
        DataType::Float32 => Some(PrimitiveType::Float),
        DataType::Float64 => Some(PrimitiveType::Double),
        DataType::Decimal128(precision, scale) => Some(PrimitiveType::Decimal {
            precision: u32::from(*precision),
            scale: u32::try_from(*scale)
                .map_err(|_| invalid(format!("iceberg decimal scale {scale} is out of range")))?,
        }),
        DataType::Date32 => Some(PrimitiveType::Date),
        DataType::Time64(TimeUnit::Microsecond) => Some(PrimitiveType::Time),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some(PrimitiveType::Timestamp),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Some(PrimitiveType::Timestamptz),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(PrimitiveType::TimestampNs),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => Some(PrimitiveType::TimestamptzNs),
        DataType::Utf8 => Some(PrimitiveType::String),
        // JSON uses the private Binary mirror plus its explicit SQL domain.
        DataType::Binary | DataType::LargeBinary => Some(PrimitiveType::Binary),
        // The system declaration exposes ordinary Iceberg Fixed as Binary.
        DataType::FixedSizeBinary(16) => Some(PrimitiveType::Uuid),
        DataType::FixedSizeBinary(width) => Some(PrimitiveType::Fixed(
            u64::try_from(*width)
                .map_err(|_| invalid(format!("iceberg fixed width {width} is out of range")))?,
        )),
        _ => None,
    };
    if let Some(primitive) = primitive {
        observe()?;
        return Ok(Type::Primitive(primitive));
    }
    let result: Result<Type, E> = match data_type {
        DataType::Struct(fields) => {
            let mut mirrored = Vec::with_capacity(fields.len());
            for field in fields {
                mirrored.push(Arc::new(metadata_relation_field_observed(
                    field.as_ref(),
                    ids,
                    observe,
                )?));
                observe()?;
            }
            Ok(Type::Struct(StructType::new(mirrored)))
        }
        DataType::List(element) => {
            // Iceberg JSON authors the list child as `element`; Arrow's public
            // carrier names it `item`. Only the Iceberg mirror uses the fixed
            // native name, so its exact declaration survives a JSON round trip.
            let child = metadata_relation_type_observed(element.data_type(), ids, observe)?;
            let id = ids.take()?;
            observe()?;
            Ok(Type::List(ListType::new(Arc::new(
                NestedField::list_element(id, child, !element.is_nullable()),
            ))))
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(internal(
                    "an iceberg metadata relation map carries entries that are not a struct",
                )
                .into());
            };
            let [key, value] = fields.as_ref() else {
                return Err(internal(
                    "an iceberg metadata relation map must carry exactly a key and a value",
                )
                .into());
            };
            Ok(Type::Map(MapType::new(
                Arc::new(metadata_relation_field_observed(
                    key.as_ref(),
                    ids,
                    observe,
                )?),
                Arc::new(metadata_relation_field_observed(
                    value.as_ref(),
                    ids,
                    observe,
                )?),
            )))
        }
        other => Err(unsupported(format!(
            "an iceberg metadata relation column carrier {other:?} has no iceberg type"
        ))
        .into()),
    };
    let result = result?;
    observe()?;
    Ok(result)
}

/// The Iceberg mirror of one frozen metadata-relation schema, as a ROW.
fn metadata_relation_row(schema: &SchemaRef) -> Result<Type, ConnectorError> {
    let mut ids = MetadataRelationFieldIds::new();
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        fields.push(Arc::new(metadata_relation_field(field.as_ref(), &mut ids)?));
    }
    Ok(Type::Struct(StructType::new(fields)))
}

/// The explicit storage mirror of one column from the frozen metadata
/// schema author. Other Arrow schemas do not authorize this projection.
pub(crate) fn metadata_temporal_declaration(
    layout: &DataType,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Option<Type>, PureProviderCompileError<ConnectorError>> {
    if !metadata_has_temporal_storage(layout, creation)? {
        return Ok(None);
    }
    let declared =
        metadata_relation_type_observed(layout, &mut MetadataRelationFieldIds::new(), &mut || {
            creation.work(1)
        })?;
    Ok(Some(declared))
}

fn metadata_has_temporal_storage(
    layout: &DataType,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<bool, PureProviderCompileError<ConnectorError>> {
    creation.work(1)?;
    match layout {
        DataType::Time64(TimeUnit::Microsecond)
        | DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _) => Ok(true),
        DataType::List(child) | DataType::Map(child, _) => {
            metadata_has_temporal_storage(child.data_type(), creation)
        }
        DataType::Struct(fields) => {
            for field in fields {
                if metadata_has_temporal_storage(field.data_type(), creation)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

#[derive(Debug)]
enum SystemMirrorError {
    Control(CompileControlError),
    Source(ConnectorError),
}
impl From<ConnectorError> for SystemMirrorError {
    fn from(error: ConnectorError) -> Self {
        Self::Source(error)
    }
}
impl From<CompileControlError> for SystemMirrorError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValueTypeError> for SystemMirrorError {
    fn from(error: ValueTypeError) -> Self {
        Self::Source(invalid(error.to_string()))
    }
}

/// Validate the original System Arrow source against its private storage mirror.
/// Synthetic identity IDs are supplied in the original postorder; they are not
/// table IDs or a canonical numbering requirement. Unknown public metadata is
/// retained by the caller. Opaque Arrow clones and SDK identity construction
/// have surrounding observations, not an internal cooperation/MEM guarantee.
pub(crate) fn system_column_field_for_compile(
    kind: IcebergSystemTableType,
    column: &IcebergColumnHandle,
    field: &Field,
    logical: ValueLogicalType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NestedField, PureProviderCompileError<ConnectorError>> {
    work.flush()?;
    let result = (|| -> Result<NestedField, SystemMirrorError> {
        if !column.is_base_column() {
            return Err(unsupported(
                "system relation reader does not project a nested column handle",
            )
            .into());
        }
        let dynamic = matches!(
            (kind, field.name().as_str()),
            (
                IcebergSystemTableType::Files,
                "partition" | "lower_bounds" | "upper_bounds"
            ) | (IcebergSystemTableType::Entries, "data_file")
        );
        work.step()?;
        work.flush()?;
        if dynamic {
            let valid = matches!(field.data_type(), DataType::Struct(_))
                && logical == ValueLogicalType::Physical
                && field.is_nullable() != (field.name() == "data_file");
            if !valid {
                return Err(
                    invalid("system schema-derived column differs from its frozen ROW").into(),
                );
            }
        } else {
            let fixed =
                with_pure_declaration(|creation| schema_author::fixed_columns(kind, creation))?;
            let mut expected = None;
            for (name, value) in &fixed {
                work.step()?;
                if name == field.name() {
                    expected = Some(value);
                    break;
                }
            }
            let expected =
                expected.ok_or_else(|| unsupported("system relation has no such output column"))?;
            if expected.nullable() != field.is_nullable() || expected.logical_type() != logical {
                return Err(invalid(
                    "system column differs from its declared NULL or value domain",
                )
                .into());
            }
            let same = novarocks_type_contract::arrow_data_types_exact_observed::<SystemMirrorError>(
                expected.data_type(),
                field.data_type(),
                || work.step().map_err(SystemMirrorError::Control),
            )?;
            if !same {
                return Err(invalid(
                    "system column differs from its exact Arrow source declaration",
                )
                .into());
            }
        }
        // IDs are private identity vocabulary; mirror construction consumes
        // each once in the same child-first order as the original author.
        let mut pending = vec![(column.base_column_identity(), false)];
        let mut ids = Vec::new();
        while let Some((identity, visited)) = pending.pop() {
            if visited {
                ids.push(identity.field_id());
                work.step()?;
            } else {
                pending.push((identity, true));
                work.step()?;
                for child in identity.children().iter().rev() {
                    pending.push((child, false));
                    work.step()?;
                }
            }
        }
        let mut source = MetadataRelationFieldIds::supplied(&ids);
        let mirrored =
            metadata_relation_field_observed::<SystemMirrorError>(field, &mut source, &mut || {
                work.step().map_err(SystemMirrorError::Control)
            })?;
        let exhausted = source.supplied.as_ref().is_some_and(|ids| ids.len() == 0);
        work.step()?;
        if !exhausted {
            return Err(invalid("system column identity has unused mirror fields").into());
        }
        Ok(mirrored)
    })();
    match result {
        Err(SystemMirrorError::Control(cause)) => Err(PureProviderCompileError::Control(cause)),
        result => {
            work.flush()?;
            result.map_err(|error| match error {
                SystemMirrorError::Source(error) => PureProviderCompileError::Provider(error),
                SystemMirrorError::Control(cause) => PureProviderCompileError::Control(cause),
            })
        }
    }
}

/// The column handles one worker system relation publishes, in frozen order.
///
/// The boundary hands these to the engine as the relation's columns, and the
/// reader resolves a scan's assignments back against the same frozen schema, so
/// both ends are derived from one function rather than from two lists that
/// could disagree about a column's name, order, or type.
pub fn system_relation_columns(
    relation: IcebergSystemTableType,
    schema: &Schema,
    specs: &[PartitionSpec],
) -> Result<Vec<IcebergColumnHandle>, ConnectorError> {
    let frozen = system_relation_schema(relation, schema, specs)?;
    let mut ids = MetadataRelationFieldIds::new();
    let mut columns = Vec::with_capacity(frozen.fields().len());
    for field in frozen.fields() {
        let mirrored = metadata_relation_field(field.as_ref(), &mut ids)?;
        columns.push(IcebergColumnHandle::base_column(&mirrored)?);
    }
    Ok(columns)
}

/// Publish relation columns from the complete pinned metadata generation.
/// Historical partition source IDs remain declarations after a source is dropped.
pub fn system_relation_columns_with_metadata(
    relation: IcebergSystemTableType,
    metadata: &TableMetadata,
) -> Result<Vec<IcebergColumnHandle>, ConnectorError> {
    let schema = system_relation_schema_with_metadata(relation, metadata)?;
    let mut ids = MetadataRelationFieldIds::new();
    schema
        .fields()
        .iter()
        .map(|field| IcebergColumnHandle::base_column(&metadata_relation_field(field, &mut ids)?))
        .collect()
}

pub fn partition_row_type_with_metadata(
    metadata: &TableMetadata,
) -> Result<Option<DataType>, ConnectorError> {
    with_pure_declaration(|creation| {
        let facts = schema_author::partition_facts(metadata, creation)?;
        Ok(schema_author::partition(&facts, creation)?.map(|value| value.data_type().clone()))
    })
}

pub fn system_relation_schema_with_metadata(
    relation: IcebergSystemTableType,
    metadata: &TableMetadata,
) -> Result<SchemaRef, ConnectorError> {
    with_pure_declaration(|creation| {
        let columns = schema_author::output_columns(relation, metadata, creation)?;
        source_schema(columns, creation)
    })
}

pub(crate) fn system_relation_columns_from_facts(
    relation: IcebergSystemTableType,
    schema: &Schema,
    partition: &crate::partition_row::UnifiedPartitionRow,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<IcebergColumnHandle>, PureProviderCompileError<ConnectorError>> {
    let columns = schema_author::columns_from_facts(relation, schema, partition, creation)?;
    let schema = source_schema(columns, creation)?;
    let mut ids = MetadataRelationFieldIds::new();
    let mut handles = creation.reserve(schema.fields().len())?;
    for field in schema.fields() {
        let mirrored =
            metadata_relation_field_observed(field.as_ref(), &mut ids, &mut || creation.work(1))?;
        creation.work(1)?;
        handles.push(IcebergColumnHandle::base_column(&mirrored)?);
    }
    Ok(handles)
}

pub fn files_relation_schema_json_with_metadata(
    metadata: &TableMetadata,
) -> Result<String, ConnectorError> {
    system_relation_schema_json_with_metadata(IcebergSystemTableType::Files, metadata)
}

pub fn system_relation_schema_json_with_metadata(
    kind: IcebergSystemTableType,
    metadata: &TableMetadata,
) -> Result<String, ConnectorError> {
    type_to_json(&metadata_relation_row(
        &system_relation_schema_with_metadata(kind, metadata)?,
    )?)
}

/// The frozen `$files` output schema, rendered as the JSON a split carries.
pub fn files_relation_schema_json(
    schema: &Schema,
    specs: &[PartitionSpec],
) -> Result<String, ConnectorError> {
    let frozen = system_relation_schema(IcebergSystemTableType::Files, schema, specs)?;
    type_to_json(&metadata_relation_row(&frozen)?)
}

/// One schema-derived ROW type, rendered as the JSON a `$files` split carries.
pub fn derived_row_type_json(row: &DataType) -> Result<String, ConnectorError> {
    let mut ids = MetadataRelationFieldIds::new();
    type_to_json(&metadata_relation_type(row, &mut ids)?)
}

// ---------------------------------------------------------------------------
// Schema-derived ROW types
// ---------------------------------------------------------------------------

/// The partition ROW assembled from every visible source in historical specs.
pub fn partition_row_type(
    schema: &Schema,
    specs: &[PartitionSpec],
) -> Result<Option<DataType>, ConnectorError> {
    let facts = crate::partition_row::UnifiedPartitionRow::of(schema, specs)?;
    with_pure_declaration(|creation| {
        Ok(schema_author::partition(&facts, creation)?.map(|value| value.data_type().clone()))
    })
}

#[cfg(test)]
pub(crate) fn partition_row_type_from_facts(
    facts: &crate::partition_row::UnifiedPartitionRow,
) -> Result<Option<DataType>, ConnectorError> {
    with_pure_declaration(|creation| {
        Ok(schema_author::partition(facts, creation)?.map(|value| value.data_type().clone()))
    })
}

pub fn bounds_row_type(schema: &Schema) -> Result<Option<DataType>, ConnectorError> {
    with_pure_declaration(|creation| {
        Ok(schema_author::bounds(schema, creation)?.map(|value| value.data_type().clone()))
    })
}

fn with_pure_declaration<T>(
    operation: impl FnOnce(
        &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<T, PureProviderCompileError<ConnectorError>>,
) -> Result<T, ConnectorError> {
    let mut request = |_| Ok(());
    let mut work = |_| Ok(());
    let mut creation = ConnectorReadSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    );
    operation(&mut creation).map_err(crate::metadata::metadata_creation_error)
}

fn source_schema(
    columns: schema_author::Columns,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<SchemaRef, PureProviderCompileError<ConnectorError>> {
    let mut fields = creation.reserve(columns.len())?;
    for (name, value) in columns {
        fields.push(creation.arc(Field::new(
            name,
            value.data_type().clone(),
            value.nullable(),
        ))?);
    }
    let fields = creation.arrow_fields(fields)?;
    creation.arc(ArrowSchema::new(fields))
}

/// The frozen output schema of one worker system relation.
pub fn system_relation_schema(
    relation: IcebergSystemTableType,
    schema: &Schema,
    specs: &[PartitionSpec],
) -> Result<SchemaRef, ConnectorError> {
    with_pure_declaration(|creation| {
        let mut borrowed = creation.reserve(specs.len())?;
        borrowed.extend(specs);
        let columns = schema_author::columns(relation, schema, &borrowed, creation)?;
        source_schema(columns, creation)
    })
}

#[cfg(test)]
fn partition_summary_type() -> DataType {
    let schema = with_pure_declaration(|creation| {
        let columns = schema_author::fixed_columns(IcebergSystemTableType::Manifests, creation)?;
        source_schema(columns, creation)
    })
    .unwrap();
    schema
        .field_with_name("partition_summaries")
        .unwrap()
        .data_type()
        .clone()
}

mod bounds;
mod entry_columns;
mod entry_pages;
mod entry_view;
mod schema_author;
#[cfg(test)]
pub(crate) use schema_author::publish_read_schema;
pub(crate) use schema_author::{output_columns, row as metadata_row_value, value_for_primitive};
pub(crate) use schema_author::{publish_read_schema_from_facts, publish_read_schema_with_metadata};

// ---------------------------------------------------------------------------
// Frozen-metadata I/O
// ---------------------------------------------------------------------------

/// Reads exactly the immutable files a system relation is allowed to open.
struct FrozenMetadataReader<'a> {
    binding: &'a IcebergReadBinding,
    context: &'a FileReadContext,
    bytes_read: u64,
}

impl<'a> FrozenMetadataReader<'a> {
    const fn new(binding: &'a IcebergReadBinding, context: &'a FileReadContext) -> Self {
        Self {
            binding,
            context,
            bytes_read: 0,
        }
    }

    async fn read_whole_file(
        &mut self,
        path: &str,
        length: Option<u64>,
    ) -> Result<Bytes, ConnectorError> {
        let access = self
            .binding
            .resolve_access_for_locations_async([path], self.context)
            .await?;
        let bytes = crate::file_reader::read_bytes_async(
            &access,
            path,
            length,
            FileReadRange::WholeFile,
            self.context,
        )
        .await
        .map_err(|error| unreadable_metadata_file(path, error))?;
        Ok(self.counted(bytes))
    }

    fn counted(&mut self, bytes: Bytes) -> Bytes {
        self.bytes_read = self.bytes_read.saturating_add(bytes.len() as u64);
        bytes
    }

    /// Load and verify the exact metadata file the reference names.
    async fn load_metadata(
        &mut self,
        reference: &IcebergSystemTableReference,
    ) -> Result<TableMetadata, ConnectorError> {
        let bytes = self
            .read_whole_file(reference.metadata_file_location(), None)
            .await?;
        parse_metadata(reference, &bytes)
    }

    /// The manifest list of one snapshot of the verified metadata.
    async fn load_manifest_list(
        &mut self,
        metadata: &TableMetadata,
        snapshot_id: i64,
    ) -> Result<Vec<ManifestFile>, ConnectorError> {
        let location = manifest_list_location(metadata, snapshot_id)?;
        let bytes = self.read_whole_file(location, None).await?;
        parse_manifest_list(metadata, location, &bytes)
    }

    /// One manifest, parsed from its own bytes.
    ///
    /// `ManifestFile::load_manifest` is not used: it reads through a table
    /// `FileIO` instead of this reader's access binding and range service.
    /// Row rendering binds sequence facts after reading the raw entries.
    async fn load_manifest(
        &mut self,
        manifest: &TrinoManifestFile,
    ) -> Result<Manifest, ConnectorError> {
        let length = manifest_length(manifest)?;
        let bytes = self.read_whole_file(manifest.path(), Some(length)).await?;
        parse_manifest(manifest, &bytes)
    }
}

/// A frozen location that cannot be read is not corrupt data: the file may
/// simply be unreachable right now, and the distinction decides whether the
/// attempt is worth retrying.
fn unreadable_metadata_file(path: &str, error: String) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        format!("read iceberg metadata file {path}: {error}"),
    )
    .with_retryable_before_progress()
}

fn parse_metadata(
    reference: &IcebergSystemTableReference,
    bytes: &Bytes,
) -> Result<TableMetadata, ConnectorError> {
    let metadata: TableMetadata = crate::metadata_json::from_slice_document(
        bytes.as_ref(),
        crate::metadata_json::Document::TableMetadata,
    )
    .map_err(|error| {
        ConnectorError::new(
            error.connector_kind(ConnectorErrorKind::CorruptData),
            format!(
                "iceberg metadata file {} is not table metadata: {error}",
                reference.metadata_file_location()
            ),
        )
    })?;
    reference.verify_loaded_metadata(&metadata)?;
    Ok(metadata)
}

fn manifest_list_location(
    metadata: &TableMetadata,
    snapshot_id: i64,
) -> Result<&str, ConnectorError> {
    metadata
        .snapshot_by_id(snapshot_id)
        .map(|snapshot| snapshot.manifest_list())
        .ok_or_else(|| {
            corrupt(format!(
                "iceberg snapshot {snapshot_id} is absent from the frozen metadata"
            ))
        })
}

fn parse_manifest_list(
    metadata: &TableMetadata,
    location: &str,
    bytes: &Bytes,
) -> Result<Vec<ManifestFile>, ConnectorError> {
    let list = ManifestList::parse_with_version(bytes.as_ref(), metadata.format_version())
        .map_err(|error| {
            corrupt(format!(
                "iceberg manifest list {location} is invalid: {error}"
            ))
        })?;
    Ok(list.consume_entries().into_iter().collect())
}

fn manifest_length(manifest: &TrinoManifestFile) -> Result<u64, ConnectorError> {
    u64::try_from(manifest.length()).map_err(|_| {
        corrupt(format!(
            "iceberg manifest {} declares a negative length",
            manifest.path()
        ))
    })
}

fn parse_manifest(manifest: &TrinoManifestFile, bytes: &Bytes) -> Result<Manifest, ConnectorError> {
    crate::manifest::parse_manifest_checked(bytes.as_ref()).map_err(|error| {
        if error.kind() == crate::iceberg::ErrorKind::FeatureUnsupported {
            crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::raw_read_failure(error)
        } else {
            corrupt(format!(
                "iceberg manifest {} is invalid: {error}",
                manifest.path(),
            ))
        }
    })
}

// ---------------------------------------------------------------------------
// Row models
// ---------------------------------------------------------------------------

/// The entry-level facts a manifest entry contributes, after inheritance.
fn file_format_name(format: DataFileFormat) -> &'static str {
    match format {
        DataFileFormat::Parquet => "PARQUET",
        DataFileFormat::Orc => "ORC",
        DataFileFormat::Avro => "AVRO",
        DataFileFormat::Puffin => "PUFFIN",
    }
}

fn content_code(content: DataContentType) -> i32 {
    match content {
        DataContentType::Data => 0,
        DataContentType::PositionDeletes => 1,
        DataContentType::EqualityDeletes => 2,
    }
}

/// Resolve one entry's partition tuple against the spec its manifest names.
fn partition_values(
    data_file: &DataFile,
    spec: &PartitionSpec,
    unified: &crate::partition_row::UnifiedPartitionRow,
) -> Result<Vec<Option<Literal>>, ConnectorError> {
    let values = data_file.partition().fields();
    let mapping = unified
        .slots_by_spec
        .get(&spec.spec_id())
        .ok_or_else(|| corrupt("iceberg partition spec has no canonical slot mapping"))?;
    if values.len() != spec.fields().len() || mapping.len() != values.len() {
        return Err(corrupt(format!(
            "iceberg data file {} partition value count differs from spec {}",
            data_file.file_path(),
            spec.spec_id()
        )));
    }
    let mut output = vec![None; unified.fields.len()];
    for (slot, value) in mapping.iter().zip(values) {
        if let Some(slot) = slot {
            output[*slot] = value.clone();
        }
    }
    Ok(output)
}

fn string_to_string_map_array<'a>(
    rows: impl Iterator<Item = Option<&'a BTreeMap<String, String>>>,
) -> Result<ArrayRef, ConnectorError> {
    let mut builder = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    for entries in rows {
        match entries {
            Some(entries) => {
                for (key, value) in entries {
                    builder.keys().append_value(key);
                    builder.values().append_value(value);
                }
                builder
                    .append(true)
                    .map_err(|error| internal(format!("append iceberg summary map: {error}")))?;
            }
            None => builder
                .append(false)
                .map_err(|error| internal(format!("append null iceberg summary map: {error}")))?,
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

/// Materialize one primitive column from already-typed Iceberg literals.
///
/// The literal's variant must match the declared Arrow type. A mismatch is
/// corrupt metadata and fails: coercing it would put a value of one type into a
/// column of another, which is exactly the type downgrade this stack forbids.
fn primitive_array(
    field: &Field,
    values: &[Option<&PrimitiveLiteral>],
) -> Result<ArrayRef, ConnectorError> {
    let mismatch = |literal: &PrimitiveLiteral| {
        corrupt(format!(
            "iceberg value {literal:?} does not match column {} of type {:?}",
            field.name(),
            field.data_type()
        ))
    };
    macro_rules! collect {
        ($pattern:pat => $extract:expr) => {{
            let mut collected = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    None => collected.push(None),
                    Some($pattern) => collected.push(Some($extract)),
                    Some(other) => return Err(mismatch(other)),
                }
            }
            collected
        }};
    }

    Ok(match field.data_type() {
        DataType::Boolean => {
            let collected = collect!(PrimitiveLiteral::Boolean(value) => *value);
            Arc::new(BooleanArray::from(collected))
        }
        DataType::Int32 => {
            let collected = collect!(PrimitiveLiteral::Int(value) => *value);
            Arc::new(Int32Array::from(collected))
        }
        DataType::Int64 => {
            let collected = collect!(PrimitiveLiteral::Long(value) => *value);
            Arc::new(Int64Array::from(collected))
        }
        DataType::Float32 => {
            let collected = collect!(PrimitiveLiteral::Float(value) => value.0);
            Arc::new(Float32Array::from(collected))
        }
        DataType::Float64 => {
            let collected = collect!(PrimitiveLiteral::Double(value) => value.0);
            Arc::new(Float64Array::from(collected))
        }
        DataType::Date32 => {
            let collected = collect!(PrimitiveLiteral::Int(value) => *value);
            Arc::new(Date32Array::from(collected))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let collected = collect!(PrimitiveLiteral::Long(value) => *value);
            Arc::new(Time64MicrosecondArray::from(collected))
        }
        DataType::Timestamp(TimeUnit::Microsecond, zone) => {
            let collected = collect!(PrimitiveLiteral::Long(value) => *value);
            let array = TimestampMicrosecondArray::from(collected);
            match zone {
                Some(zone) => Arc::new(array.with_timezone(zone.clone())),
                None => Arc::new(array),
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, zone) => {
            let collected = collect!(PrimitiveLiteral::Long(value) => *value);
            let array = TimestampNanosecondArray::from(collected);
            match zone {
                Some(zone) => Arc::new(array.with_timezone(zone.clone())),
                None => Arc::new(array),
            }
        }
        DataType::Utf8 => {
            let mut collected: Vec<Option<String>> = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    None => collected.push(None),
                    Some(PrimitiveLiteral::String(value)) => collected.push(Some(value.clone())),
                    Some(other) => return Err(mismatch(other)),
                }
            }
            Arc::new(StringArray::from(collected))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for value in values {
                match value {
                    None => builder.append_null(),
                    Some(PrimitiveLiteral::Binary(value)) => builder.append_value(value),
                    Some(other) => return Err(mismatch(other)),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::FixedSizeBinary(width) => {
            let mut builder = FixedSizeBinaryBuilder::new(*width);
            for value in values {
                match value {
                    None => builder.append_null(),
                    Some(PrimitiveLiteral::Binary(value)) => {
                        builder.append_value(value).map_err(|error| {
                            corrupt(format!(
                                "iceberg fixed value for column {} is not {width} bytes: {error}",
                                field.name()
                            ))
                        })?;
                    }
                    Some(PrimitiveLiteral::UInt128(value)) if *width == 16 => {
                        builder.append_value(value.to_be_bytes()).map_err(|error| {
                            corrupt(format!("Iceberg UUID bound width is invalid: {error}"))
                        })?;
                    }
                    Some(other) => return Err(mismatch(other)),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Decimal128(precision, scale) => {
            let collected = collect!(PrimitiveLiteral::Int128(value) => *value);
            Arc::new(
                Decimal128Array::from(collected)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| {
                        corrupt(format!(
                            "iceberg decimal column {} does not fit its declared precision: {error}",
                            field.name()
                        ))
                    })?,
            )
        }
        other => Err(unsupported(format!(
            "iceberg system relation column {} has unsupported carrier {other:?}",
            field.name()
        )))?,
    })
}

/// Build the schema-derived `partition` ROW column.
#[cfg(test)]
fn partition_struct_array(
    row_type: &DataType,
    rows: &[&Vec<Option<Literal>>],
) -> Result<ArrayRef, ConnectorError> {
    let DataType::Struct(fields) = row_type else {
        return Err(internal("iceberg partition row type must be a struct"));
    };
    let mut children = Vec::with_capacity(fields.len());
    for (ordinal, field) in fields.iter().enumerate() {
        let target =
            metadata_relation_type(field.data_type(), &mut MetadataRelationFieldIds::new())?;
        let Type::Primitive(target) = target else {
            return Err(internal("iceberg partition declaration is not primitive"));
        };
        let promoted = rows
            .iter()
            .map(|row| match row.get(ordinal) {
                // A spec that does not define this field leaves it null.
                None | Some(None) => Ok(None),
                Some(Some(Literal::Primitive(literal))) => Ok(Some(
                    crate::read_snapshot::resolved_partition_primitive(literal, &target),
                )),
                Some(Some(other)) => Err(corrupt(format!(
                    "iceberg partition field {} holds a non-primitive value {other:?}",
                    field.name()
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let values = promoted.iter().map(Option::as_deref).collect::<Vec<_>>();
        children.push(primitive_array(field, &values)?);
    }
    Ok(Arc::new(
        StructArray::try_new_with_length(fields.clone(), children, None, rows.len())
            .map_err(|error| internal(format!("build iceberg partition row: {error}")))?,
    ))
}

// Metadata-only relations
// ---------------------------------------------------------------------------

fn snapshots_columns(
    schema: &SchemaRef,
    snapshots: &[&crate::iceberg::spec::Snapshot],
) -> Result<Vec<ArrayRef>, ConnectorError> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "committed_at" => Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    snapshots
                        .iter()
                        .map(|snapshot| snapshot.timestamp_ms().saturating_mul(1_000)),
                )
                .with_timezone(UTC_TIME_ZONE),
            ),
            "snapshot_id" => Arc::new(Int64Array::from_iter_values(
                snapshots.iter().map(|snapshot| snapshot.snapshot_id()),
            )),
            "parent_id" => Arc::new(Int64Array::from_iter(
                snapshots
                    .iter()
                    .map(|snapshot| snapshot.parent_snapshot_id()),
            )),
            "operation" => Arc::new(StringArray::from_iter_values(
                snapshots
                    .iter()
                    .map(|snapshot| snapshot.summary().operation.as_str()),
            )),
            "manifest_list" => Arc::new(StringArray::from_iter_values(
                snapshots.iter().map(|snapshot| snapshot.manifest_list()),
            )),
            "summary" => {
                let summaries = snapshots
                    .iter()
                    .map(|snapshot| {
                        let properties = &snapshot.summary().additional_properties;
                        (!properties.is_empty()).then(|| {
                            properties
                                .iter()
                                .map(|(key, value)| (key.clone(), value.clone()))
                                .collect::<BTreeMap<_, _>>()
                        })
                    })
                    .collect::<Vec<_>>();
                string_to_string_map_array(summaries.iter().map(Option::as_ref))?
            }
            other => {
                return Err(internal(format!(
                    "iceberg $snapshots has no column named {other}"
                )));
            }
        };
        columns.push(array);
    }
    Ok(columns)
}

fn history_ancestors(metadata: &TableMetadata) -> std::collections::HashSet<i64> {
    // A snapshot is a current ancestor when the current head reaches it by
    // walking parent pointers. Building the set once keeps the per-row test
    // constant time and, more importantly, keeps the answer consistent across
    // every row of one page.
    let mut ancestors = std::collections::HashSet::new();
    let mut walker = metadata.current_snapshot_id();
    while let Some(id) = walker {
        if !ancestors.insert(id) {
            // Parent pointers must be acyclic; stopping rather than looping
            // keeps corrupt metadata from hanging the reader.
            break;
        }
        walker = metadata
            .snapshot_by_id(id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }

    ancestors
}

fn history_columns(
    schema: &SchemaRef,
    metadata: &TableMetadata,
    range: std::ops::Range<usize>,
    ancestors: &std::collections::HashSet<i64>,
) -> Result<Vec<ArrayRef>, ConnectorError> {
    let entries = &metadata.history()[range];
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "made_current_at" => Arc::new(
                TimestampMicrosecondArray::from(
                    entries
                        .iter()
                        .map(|entry| entry.timestamp_ms.saturating_mul(1_000))
                        .collect::<Vec<_>>(),
                )
                .with_timezone(UTC_TIME_ZONE),
            ),
            "snapshot_id" => Arc::new(Int64Array::from(
                entries
                    .iter()
                    .map(|entry| entry.snapshot_id)
                    .collect::<Vec<_>>(),
            )),
            "parent_id" => Arc::new(Int64Array::from(
                entries
                    .iter()
                    .map(|entry| {
                        metadata
                            .snapshot_by_id(entry.snapshot_id)
                            .and_then(|snapshot| snapshot.parent_snapshot_id())
                    })
                    .collect::<Vec<_>>(),
            )),
            "is_current_ancestor" => Arc::new(BooleanArray::from(
                entries
                    .iter()
                    .map(|entry| ancestors.contains(&entry.snapshot_id))
                    .collect::<Vec<_>>(),
            )),
            other => {
                return Err(internal(format!(
                    "iceberg $history has no column named {other}"
                )));
            }
        };
        columns.push(array);
    }
    Ok(columns)
}

/// Whether a ref name belongs to the provider rather than to the table.
///
/// Write fences and the MV publication fence are commit bookkeeping. They are
/// real refs in the raw metadata, but they name no user branch or tag, so
/// `$refs` must not report them.
fn is_provider_private_ref(name: &str) -> bool {
    crate::commit::write_fence::is_fence_ref(name)
        || name == crate::commit::mv_publication_fence::MV_PUBLICATION_FENCE_REF
}

fn refs_columns(
    schema: &SchemaRef,
    metadata: &TableMetadata,
    names: &[String],
) -> Result<Vec<ArrayRef>, ConnectorError> {
    let refs = || names.iter().map(|name| &metadata.refs()[name]);
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef =
            match field.name().as_str() {
                "name" => Arc::new(StringArray::from_iter_values(names.iter())),
                "type" => Arc::new(StringArray::from_iter_values(refs().map(|reference| {
                    match reference.retention {
                        SnapshotRetention::Branch { .. } => "BRANCH",
                        SnapshotRetention::Tag { .. } => "TAG",
                    }
                }))),
                "snapshot_id" => Arc::new(Int64Array::from_iter_values(
                    refs().map(|reference| reference.snapshot_id),
                )),
                "max_reference_age_in_ms" => Arc::new(Int64Array::from_iter(refs().map(
                    |reference| match reference.retention {
                        SnapshotRetention::Branch { max_ref_age_ms, .. }
                        | SnapshotRetention::Tag { max_ref_age_ms } => max_ref_age_ms,
                    },
                ))),
                "min_snapshots_to_keep" => Arc::new(Int32Array::from_iter(refs().map(
                    |reference| match reference.retention {
                        SnapshotRetention::Branch {
                            min_snapshots_to_keep,
                            ..
                        } => min_snapshots_to_keep,
                        SnapshotRetention::Tag { .. } => None,
                    },
                ))),
                "max_snapshot_age_in_ms" => Arc::new(Int64Array::from_iter(refs().map(
                    |reference| match reference.retention {
                        SnapshotRetention::Branch {
                            max_snapshot_age_ms,
                            ..
                        } => max_snapshot_age_ms,
                        SnapshotRetention::Tag { .. } => None,
                    },
                ))),
                other => {
                    return Err(internal(format!(
                        "iceberg $refs has no column named {other}"
                    )));
                }
            };
        columns.push(array);
    }
    Ok(columns)
}

/// Render one partition-field summary bound, decoded through the field's own
/// type rather than printed as raw bytes.
fn summary_bound(
    bound: Option<&[u8]>,
    partition_type: Option<&PrimitiveType>,
) -> Result<Option<String>, ConnectorError> {
    let Some(bound) = bound else {
        return Ok(None);
    };
    let Some(partition_type) = partition_type else {
        return Err(corrupt(
            "iceberg manifest partition summary has no partition field to decode against",
        ));
    };
    let datum = Datum::try_from_bytes(bound, partition_type.clone()).map_err(|error| {
        corrupt(format!(
            "iceberg manifest partition summary bound is not a {partition_type}: {error}"
        ))
    })?;
    Ok(Some(datum.to_human_string()))
}

/// The primitive types of one spec's partition fields, positionally.
fn spec_partition_types(
    spec: &PartitionSpec,
    metadata: &TableMetadata,
) -> Result<Vec<PrimitiveType>, ConnectorError> {
    spec.fields()
        .iter()
        .map(|partition_field| {
            let source = crate::metadata_batch_reader::partition_source_type(
                metadata,
                partition_field.source_id,
            )
            .map_err(corrupt)?;
            let result_type = partition_field
                .transform
                .result_type(&source)
                .map_err(|error| {
                    corrupt(format!(
                        "iceberg partition field {} has no result type: {error}",
                        partition_field.name
                    ))
                })?;
            match result_type {
                Type::Primitive(primitive) => Ok(primitive),
                other => Err(corrupt(format!(
                    "iceberg partition field {} transforms to {other:?}, not a primitive",
                    partition_field.name
                ))),
            }
        })
        .collect()
}

fn manifests_columns(
    schema: &SchemaRef,
    metadata: &TableMetadata,
    manifests: &[TrinoManifestFile],
    summaries: &[Option<Vec<FieldSummary>>],
) -> Result<Vec<ArrayRef>, ConnectorError> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "content" => Arc::new(Int32Array::from(
                manifests
                    .iter()
                    .map(|manifest| manifest.content().code())
                    .collect::<Vec<_>>(),
            )),
            "path" => Arc::new(StringArray::from(
                manifests
                    .iter()
                    .map(TrinoManifestFile::path)
                    .collect::<Vec<_>>(),
            )),
            "length" => Arc::new(Int64Array::from(
                manifests
                    .iter()
                    .map(TrinoManifestFile::length)
                    .collect::<Vec<_>>(),
            )),
            "partition_spec_id" => Arc::new(Int32Array::from(
                manifests
                    .iter()
                    .map(TrinoManifestFile::partition_spec_id)
                    .collect::<Vec<_>>(),
            )),
            "added_snapshot_id" => Arc::new(Int64Array::from(
                manifests
                    .iter()
                    .map(TrinoManifestFile::added_snapshot_id)
                    .collect::<Vec<_>>(),
            )),
            // Iceberg makes the v1 counts optional. An absent count is unknown,
            // and reporting it as zero would claim a manifest added nothing.
            "added_data_files_count" => Arc::new(Int32Array::from(
                manifests
                    .iter()
                    .map(TrinoManifestFile::added_files_count)
                    .collect::<Vec<_>>(),
            )),
            "added_rows_count" => Arc::new(Int64Array::from(
                manifests
                    .iter()
                    .map(TrinoManifestFile::added_rows_count)
                    .collect::<Vec<_>>(),
            )),
            "existing_data_files_count" => Arc::new(Int32Array::from(
                manifests
                    .iter()
                    .map(TrinoManifestFile::existing_files_count)
                    .collect::<Vec<_>>(),
            )),
            "existing_rows_count" => Arc::new(Int64Array::from(
                manifests
                    .iter()
                    .map(TrinoManifestFile::existing_rows_count)
                    .collect::<Vec<_>>(),
            )),
            "deleted_data_files_count" => Arc::new(Int32Array::from(
                manifests
                    .iter()
                    .map(TrinoManifestFile::deleted_files_count)
                    .collect::<Vec<_>>(),
            )),
            "deleted_rows_count" => Arc::new(Int64Array::from(
                manifests
                    .iter()
                    .map(TrinoManifestFile::deleted_rows_count)
                    .collect::<Vec<_>>(),
            )),
            "partition_summaries" => {
                partition_summaries_array(field.data_type(), metadata, manifests, summaries)?
            }
            other => {
                return Err(internal(format!(
                    "iceberg $manifests has no column named {other}"
                )));
            }
        };
        columns.push(array);
    }
    Ok(columns)
}

fn partition_summaries_array(
    list_type: &DataType,
    metadata: &TableMetadata,
    manifests: &[TrinoManifestFile],
    summaries: &[Option<Vec<FieldSummary>>],
) -> Result<ArrayRef, ConnectorError> {
    let DataType::List(item) = list_type else {
        return Err(internal(
            "iceberg $manifests partition_summaries must be a list",
        ));
    };
    let DataType::Struct(fields) = item.data_type() else {
        return Err(internal(
            "iceberg $manifests partition_summaries item must be a struct",
        ));
    };

    // Build the flat child arrays and the list offsets by hand: an arrow
    // `StructBuilder` would need one downcast per field, and the struct here is
    // fixed and tiny.
    let mut contains_null = Vec::new();
    let mut contains_nan = Vec::new();
    let mut lower_bound = Vec::new();
    let mut upper_bound = Vec::new();
    let mut offsets: Vec<i32> = vec![0];
    let mut validity = Vec::with_capacity(manifests.len());

    for (manifest, summary) in manifests.iter().zip(summaries) {
        let Some(summary) = summary else {
            validity.push(false);
            offsets.push(i32::try_from(contains_null.len()).map_err(|_| {
                internal("iceberg $manifests partition summaries exceed Int32 offsets")
            })?);
            continue;
        };
        let spec = metadata
            .partition_spec_by_id(manifest.partition_spec_id())
            .ok_or_else(|| {
                corrupt(format!(
                    "iceberg manifest {} names partition spec {} which the metadata lacks",
                    manifest.path(),
                    manifest.partition_spec_id()
                ))
            })?;
        let partition_types = spec_partition_types(spec, metadata)?;
        for (index, field_summary) in summary.iter().enumerate() {
            let partition_type = partition_types.get(index);
            contains_null.push(field_summary.contains_null);
            contains_nan.push(field_summary.contains_nan);
            lower_bound.push(summary_bound(
                field_summary
                    .lower_bound
                    .as_ref()
                    .map(|bound| bound.as_slice()),
                partition_type,
            )?);
            upper_bound.push(summary_bound(
                field_summary
                    .upper_bound
                    .as_ref()
                    .map(|bound| bound.as_slice()),
                partition_type,
            )?);
        }
        validity.push(true);
        offsets.push(i32::try_from(contains_null.len()).map_err(|_| {
            internal("iceberg $manifests partition summaries exceed Int32 offsets")
        })?);
    }

    let children: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(contains_null)),
        Arc::new(BooleanArray::from(contains_nan)),
        Arc::new(StringArray::from(lower_bound)),
        Arc::new(StringArray::from(upper_bound)),
    ];
    let values = StructArray::try_new(fields.clone(), children, None)
        .map_err(|error| internal(format!("build iceberg partition summary struct: {error}")))?;
    let array = arrow::array::ListArray::try_new(
        Arc::clone(item),
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        Some(validity.into()),
    )
    .map_err(|error| internal(format!("build iceberg partition summary list: {error}")))?;
    Ok(Arc::new(array))
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

/// Resolve the scan's ordered columns against a frozen relation schema.
///
/// A system relation's columns are named metadata columns, not Iceberg table
/// fields, so the column handle's identity *name* is what selects them. The
/// assignment's scalar `value_type` is deliberately not consulted: the SPI value
/// vocabulary cannot name `MAP`, `ARRAY` or `ROW`, and the frozen relation
/// schema is the authority on those types anyway.
///
/// An empty column list is legal and means a count-only scan.
pub fn project_system_relation_columns(
    schema: &SchemaRef,
    columns: &[IcebergColumnHandle],
) -> Result<Vec<usize>, ConnectorError> {
    columns
        .iter()
        .map(|column| {
            let name = column.base_column_identity().name();
            schema
                .fields()
                .iter()
                .position(|field| field.name() == name)
                .ok_or_else(|| {
                    invalid(format!(
                        "iceberg system relation has no column named {name}"
                    ))
                })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The page source
// ---------------------------------------------------------------------------

/// A fully materialized system relation, handed out one page at a time.
///
/// The relation is read whole before its first page: a metadata relation is
/// bounded by the manifests of one pinned snapshot, and reading it up front
/// means a page request never waits on object storage halfway through a
/// relation whose rows have to agree with each other.
pub struct IcebergSystemPageSource {
    context: FileReadContext,
    output: super::output::OutputCursor,
    emitted: usize,
    bytes_read: u64,
    retained_bytes: u64,
    _staging: Option<novarocks_spi::connector::ConnectorResourceReservation>,
}

impl IcebergSystemPageSource {
    fn with_staging_reservation(
        mut self,
        staging: Option<novarocks_spi::connector::ConnectorResourceReservation>,
    ) -> Self {
        self._staging = staging;
        self
    }

    #[cfg(test)]
    fn new(
        schema: &SchemaRef,
        relation_columns: Vec<ArrayRef>,
        projection: &[usize],
        row_count: usize,
        max_page_rows: NonZeroUsize,
        bytes_read: u64,
        context: &FileReadContext,
    ) -> Result<Self, ConnectorError> {
        if relation_columns.len() != schema.fields().len() {
            return Err(internal(
                "iceberg system relation produced a column count its schema does not declare",
            ));
        }
        for (column, field) in relation_columns.iter().zip(schema.fields()) {
            if column.len() != row_count {
                return Err(internal(format!(
                    "iceberg system relation column {} has {} rows, not {row_count}",
                    field.name(),
                    column.len()
                )));
            }
            if column.data_type() != field.data_type() {
                return Err(internal(format!(
                    "iceberg system relation column {} produced {:?}, not the frozen {:?}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                )));
            }
        }
        let columns = projection
            .iter()
            .map(|index| {
                relation_columns
                    .get(*index)
                    .cloned()
                    .ok_or_else(|| internal("iceberg system relation projection is out of range"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_projection(
            schema,
            columns,
            projection,
            row_count,
            max_page_rows,
            bytes_read,
            context,
            None,
        )
    }

    fn from_projection(
        schema: &SchemaRef,
        columns: Vec<ArrayRef>,
        projection: &[usize],
        row_count: usize,
        max_page_rows: NonZeroUsize,
        bytes_read: u64,
        context: &FileReadContext,
        resources: Option<novarocks_spi::connector::ConnectorExecutionResources>,
    ) -> Result<Self, ConnectorError> {
        if columns.len() != projection.len()
            || columns.iter().zip(projection).any(|(column, index)| {
                column.len() != row_count
                    || column.data_type() != schema.fields()[*index].data_type()
            })
        {
            return Err(internal(
                "Iceberg projected columns differ from their frozen declaration",
            ));
        }
        let retained_bytes = columns
            .iter()
            .map(|column| column.get_array_memory_size() as u64)
            .sum();
        use novarocks_value_arrays::measure::{OutputColumn, OutputRows};
        let specs = columns
            .iter()
            .map(|column| OutputColumn::new(column, column.data_type()))
            .collect::<Vec<_>>();
        let max_rows = u32::try_from(max_page_rows.get())
            .ok()
            .and_then(std::num::NonZeroU32::new)
            .ok_or_else(|| internal("Iceberg metadata output page size exceeds its row domain"))?;
        let temporal = projection
            .iter()
            .map(|index| {
                let field = &schema.fields()[*index];
                match field.data_type() {
                    DataType::Date32 | DataType::Time64(_) | DataType::Timestamp(..) => {
                        let declaration =
                            metadata_relation_field(field, &mut MetadataRelationFieldIds::new())?;
                        Ok(super::output::TemporalOutputColumn::from_declaration(
                            &declaration,
                        ))
                    }
                    _ => Ok(None),
                }
            })
            .collect::<Result<Arc<[_]>, ConnectorError>>()?;
        let output = super::output::OutputCursor::with_policy(
            &specs,
            OutputRows::Contiguous(0..row_count),
            context,
            novarocks_spi::connector::ScanOutputPolicy {
                max_rows,
                ..novarocks_spi::connector::ENGINE_OUTPUT_POLICY
            },
        )?
        .with_temporal(&temporal, &specs)?
        .with_page_resources(resources, retained_bytes);
        Ok(Self {
            context: context.clone(),
            output,
            emitted: 0,
            bytes_read,
            retained_bytes,
            _staging: None,
        })
    }
}

impl IcebergSystemPageSource {
    fn next_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        let page = self.output.next_page(&self.context)?;
        if let Some(page) = &page {
            self.emitted += page.position_count();
        }
        Ok(page)
    }
}

impl MaterializedPages for IcebergSystemPageSource {
    fn poll_next_page(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<SourcePage>, ConnectorError>> {
        std::task::Poll::Ready(self.next_page())
    }

    fn is_exhausted(&self) -> bool {
        self.output.is_exhausted()
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: self.bytes_read,
            completed_positions: self.emitted as u64,
            // The metadata files were read before the first page, so no page
            // request has spent measurable time reading.
            read_time_nanos: 0,
            ..Default::default()
        }
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.retained_bytes
    }
}

// ---------------------------------------------------------------------------
// The provider
// ---------------------------------------------------------------------------

/// The worker-side reader factory for Iceberg system relations.
///
/// It holds the process-local access binding and one request's file-read
/// context, exactly like the ordinary page-source provider, and nothing else:
/// a system relation shares no footer cache and no delete manager because it
/// opens no data file.
pub struct IcebergSystemTableProvider {
    binding: IcebergReadBinding,
    context: FileReadContext,
    max_page_rows: NonZeroUsize,
    resources: Option<novarocks_spi::connector::ConnectorExecutionResources>,
}

impl std::fmt::Debug for IcebergSystemTableProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergSystemTableProvider")
            .field("max_page_rows", &self.max_page_rows)
            .finish_non_exhaustive()
    }
}

impl IcebergSystemTableProvider {
    pub const fn new(
        binding: IcebergReadBinding,
        context: FileReadContext,
        max_page_rows: NonZeroUsize,
    ) -> Self {
        Self {
            binding,
            context,
            max_page_rows,
            resources: None,
        }
    }

    pub(super) fn with_execution_resources(
        mut self,
        resources: Option<novarocks_spi::connector::ConnectorExecutionResources>,
    ) -> Self {
        self.resources = resources;
        self
    }

    /// Dispatch one metadata work unit after matching its exact frozen scan reference.
    pub fn create_page_stream(
        &self,
        scan: &SystemTableScan,
        split: &super::IcebergReadSplit,
        columns: &[IcebergColumnHandle],
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        match split {
            super::IcebergReadSplit::SystemManifest(split) => {
                self.create_manifest_page_stream(scan, split, columns, budget)
            }
            super::IcebergReadSplit::SystemRelation(split)
                if split.reference() == scan.reference() =>
            {
                let selected = scan.project_columns(columns)?;
                if scan.system_table_type() == IcebergSystemTableType::Manifests
                    && scan.format_version() == crate::iceberg::spec::FormatVersion::V1
                {
                    return Err(invalid(
                        "iceberg manifests scan requires format version 2 or 3",
                    ));
                }
                self.create_relation_page_stream(scan, selected, budget)
            }
            _ => Err(invalid(
                "iceberg system work unit differs from its frozen scan reference or relation kind",
            )),
        }
    }

    /// The `$files` page stream for one manifest of the pinned snapshot,
    /// polled with `budget`. Creating it reads nothing; its first poll reads
    /// the manifest through the stream's own operations.
    ///
    /// Files and entries share this manifest-unit read. Their frozen relation
    /// kind chooses whether deleted entries are retained and how rows render.
    fn create_manifest_page_stream(
        &self,
        scan: &SystemTableScan,
        split: &SystemManifestSplit,
        columns: &[IcebergColumnHandle],
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        let relation = FilesSplitRelation::of(scan, split, columns)?;
        let StreamRead {
            binding,
            context,
            operations,
        } = self.stream_read()?;
        let max_page_rows = self.max_page_rows;
        let entry_budget = budget.clone();
        let resources = self.resources.clone();
        let load = async move {
            let mut reader = FrozenMetadataReader::new(&binding, &context);
            // TODO(B6c-2): replace whole-manifest decode with the released
            // SDK entry reader once it exposes the original binding facts.
            let manifest = reader
                .load_manifest(&relation.manifest)
                .await
                .map_err(|error| {
                    error.map_message(|message| {
                        crate::manifest::with_variant_statistics_hint_for_history(
                            message,
                            relation.scan.table_has_variant_history(),
                        )
                    })
                })?;
            let bytes_read = reader.bytes_read;
            Ok(entry_pages::EntryPages::new(
                relation,
                manifest,
                context,
                entry_budget,
                max_page_rows,
                bytes_read,
                resources,
            ))
        };
        Ok(Box::pin(MaterializedPageStream::new(
            Box::pin(load),
            operations,
            budget,
        )))
    }

    /// One of the five single-backend relations, read straight from the
    /// frozen metadata file, as a page stream polled with `budget`. Creating
    /// it reads nothing; its first poll reads the relation through the
    /// stream's own operations.
    fn create_relation_page_stream(
        &self,
        scan: &SystemTableScan,
        selected: Vec<usize>,
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        let StreamRead {
            binding,
            context,
            operations,
        } = self.stream_read()?;
        let scan = scan.clone();
        let max_page_rows = self.max_page_rows;
        let resources = self.resources.clone();
        let load = async move {
            let mut reader = FrozenMetadataReader::new(&binding, &context);
            read_single_backend(&mut reader, scan, selected, max_page_rows, resources).await
        };
        Ok(Box::pin(MaterializedPageStream::new(
            Box::pin(load),
            operations,
            budget,
        )))
    }

    /// One stream's read: the provider's context with the stream's reads bound
    /// to their own child of the task source.
    fn stream_read(&self) -> Result<StreamRead, ConnectorError> {
        let mut context = self.context.clone();
        let operations = SplitOperations::bind(&mut context)?;
        Ok(StreamRead {
            binding: self.binding.clone(),
            context,
            operations,
        })
    }
}

/// What one system page stream reads with and closes.
struct StreamRead {
    binding: IcebergReadBinding,
    context: FileReadContext,
    operations: SplitOperations,
}

pub(crate) use schema_author::StatisticsField;

pub(crate) fn freeze_metric_fields(
    schema: &Schema,
) -> Result<Box<[StatisticsField]>, ConnectorError> {
    let mut fields =
        with_pure_declaration(|creation| schema_author::statistics_fields(schema, creation))?;
    fields.sort_by(|left, right| left.name.encode_utf16().cmp(right.name.encode_utf16()));
    Ok(fields.into_boxed_slice())
}

pub(crate) fn freeze_system_scan_declaration(
    kind: IcebergSystemTableType,
    schema: &Schema,
    partitions: &crate::partition_row::UnifiedPartitionRow,
) -> Result<(Vec<IcebergColumnHandle>, SchemaRef), ConnectorError> {
    with_pure_declaration(|creation| {
        let columns = system_relation_columns_from_facts(kind, schema, partitions, creation)?;
        let source = schema_author::columns_from_facts(kind, schema, partitions, creation)?;
        Ok((columns, source_schema(source, creation)?))
    })
}

/// One `$files` split's frozen shape; deciding it reads nothing.
struct FilesSplitRelation {
    scan: SystemTableScan,
    manifest: TrinoManifestFile,
    projection: Vec<usize>,
}

impl FilesSplitRelation {
    fn of(
        scan: &SystemTableScan,
        split: &SystemManifestSplit,
        columns: &[IcebergColumnHandle],
    ) -> Result<Self, ConnectorError> {
        scan.validate_manifest(split)?;
        let projection = scan.project_columns(columns)?;
        Ok(Self {
            scan: scan.clone(),
            manifest: split.manifest().clone(),
            projection,
        })
    }
}

/// A singleton retains its frozen parent and row positions. Only its current
/// projected prefix owns Arrow values.
struct SingletonPages {
    metadata: TableMetadata,
    rows: SingletonRows,
    schema: SchemaRef,
    selected: Vec<usize>,
    projected_schema: SchemaRef,
    context: FileReadContext,
    position: usize,
    max_page_rows: NonZeroUsize,
    bytes_read: u64,
    emitted: u64,
    output: Option<IcebergSystemPageSource>,
    resources: Option<novarocks_spi::connector::ConnectorExecutionResources>,
}

enum SingletonRows {
    Snapshots(Vec<i64>),
    History(std::collections::HashSet<i64>),
    Refs(Vec<String>),
    Manifests(SnapshotManifests),
}

impl SingletonPages {
    fn len(&self) -> usize {
        match &self.rows {
            SingletonRows::Snapshots(ids) => ids.len(),
            SingletonRows::History(_) => self.metadata.history().len(),
            SingletonRows::Refs(names) => names.len(),
            SingletonRows::Manifests(manifests) => manifests.files.len(),
        }
    }

    fn columns(&self, range: std::ops::Range<usize>) -> Result<Vec<ArrayRef>, ConnectorError> {
        let schema = &self.projected_schema;
        if schema.fields().is_empty() {
            return Ok(Vec::new());
        }
        match &self.rows {
            SingletonRows::Snapshots(ids) => {
                let snapshots = ids[range]
                    .iter()
                    .map(|id| {
                        self.metadata
                            .snapshot_by_id(*id)
                            .expect("frozen snapshot")
                            .as_ref()
                    })
                    .collect::<Vec<_>>();
                snapshots_columns(schema, &snapshots)
            }
            SingletonRows::History(ancestors) => {
                history_columns(schema, &self.metadata, range, ancestors)
            }
            SingletonRows::Refs(names) => refs_columns(schema, &self.metadata, &names[range]),
            SingletonRows::Manifests(manifests) => manifests_columns(
                schema,
                &self.metadata,
                &manifests.files[range.clone()],
                &manifests.summaries[range],
            ),
        }
    }

    fn estimated_row_bytes(&self, row: usize) -> u128 {
        self.projected_schema
            .fields()
            .iter()
            .map(|field| match (&self.rows, field.name().as_str()) {
                (SingletonRows::Snapshots(ids), "summary") => {
                    let snapshot = self
                        .metadata
                        .snapshot_by_id(ids[row])
                        .expect("frozen snapshot");
                    snapshot
                        .summary()
                        .additional_properties
                        .iter()
                        .map(|(key, value)| key.len() as u128 + value.len() as u128 + 24)
                        .sum::<u128>()
                        + 16
                }
                (SingletonRows::Snapshots(ids), "manifest_list") => {
                    let snapshot = self
                        .metadata
                        .snapshot_by_id(ids[row])
                        .expect("frozen snapshot");
                    snapshot.manifest_list().len() as u128 + 16
                }
                (SingletonRows::Refs(names), "name") => names[row].len() as u128 + 16,
                (SingletonRows::Manifests(manifests), "path") => {
                    manifests.files[row].path().len() as u128 + 16
                }
                (SingletonRows::Manifests(manifests), "partition_summaries") => {
                    manifests.summaries[row]
                        .iter()
                        .flatten()
                        .map(|summary| {
                            let lower = summary.lower_bound.as_ref().map_or(0, |bytes| bytes.len());
                            let upper = summary.upper_bound.as_ref().map_or(0, |bytes| bytes.len());
                            256 + (lower as u128 + upper as u128) * 2
                        })
                        .sum::<u128>()
                        + 16
                }
                _ => 32,
            })
            .sum()
    }

    fn page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if let Some(output) = &mut self.output {
            let page = output.next_page()?;
            if output.is_exhausted() {
                self.output = None;
            }
            if let Some(page) = page {
                self.emitted += page.position_count() as u64;
                return Ok(Some(page));
            }
        }
        if self.position == self.len() {
            return Ok(None);
        }
        self.context
            .check_active()
            .map_err(crate::file_reader::map_file_error)?;
        let mut end = self.position;
        let mut bytes = 0_u128;
        let target =
            u128::from(novarocks_spi::connector::ENGINE_OUTPUT_POLICY.soft_target_new_column_bytes);
        while end < self.len() && end - self.position < self.max_page_rows.get() {
            self.context
                .check_active()
                .map_err(crate::file_reader::map_file_error)?;
            let row_bytes = self.estimated_row_bytes(end);
            if end != self.position && bytes + row_bytes > target {
                break;
            }
            bytes += row_bytes;
            end += 1;
        }
        let range = self.position..end;
        let staging = self
            .resources
            .as_ref()
            .map(|resources| {
                use novarocks_spi::connector::ConnectorResourceClass;
                let bytes = u64::try_from(bytes).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "Iceberg metadata estimate is out of range",
                    )
                })?;
                (bytes != 0)
                    .then(|| resources.try_reserve(ConnectorResourceClass::ReaderState, bytes))
                    .transpose()
            })
            .transpose()?
            .flatten();
        let columns = self.columns(range.clone())?;
        self.output = Some(
            IcebergSystemPageSource::from_projection(
                &self.schema,
                columns,
                &self.selected,
                range.len(),
                self.max_page_rows,
                self.bytes_read,
                &self.context,
                self.resources.clone(),
            )?
            .with_staging_reservation(staging),
        );
        self.position = end;
        self.page()
    }
}

impl MaterializedPages for SingletonPages {
    fn poll_next_page(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<SourcePage>, ConnectorError>> {
        std::task::Poll::Ready(self.page())
    }

    fn is_exhausted(&self) -> bool {
        self.output.is_none() && self.position == self.len()
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: self.bytes_read,
            completed_positions: self.emitted,
            ..Default::default()
        }
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.bytes_read
            + self
                .output
                .as_ref()
                .map_or(0, MaterializedPages::memory_usage_bytes)
    }
}

/// Read one singleton from the exact metadata file, and for Manifests from
/// its authorized list. No singleton opens a manifest body.
async fn read_single_backend(
    reader: &mut FrozenMetadataReader<'_>,
    scan: SystemTableScan,
    selected: Vec<usize>,
    max_page_rows: NonZeroUsize,
    resources: Option<novarocks_spi::connector::ConnectorExecutionResources>,
) -> Result<SingletonPages, ConnectorError> {
    let reference = scan.reference();
    let metadata = reader.load_metadata(reference).await?;
    let schema = scan.source_schema().clone();
    let projected_schema = Arc::new(ArrowSchema::new(
        selected
            .iter()
            .map(|index| schema.fields()[*index].clone())
            .collect::<Vec<_>>(),
    ));
    let rows = match reference.system_table_type() {
        IcebergSystemTableType::Snapshots => SingletonRows::Snapshots(
            metadata
                .snapshots()
                .filter(|snapshot| {
                    !crate::commit::write_fence::is_fence_marker_snapshot(snapshot.summary())
                })
                .map(|snapshot| snapshot.snapshot_id())
                .collect(),
        ),
        IcebergSystemTableType::History => {
            let needs_ancestors = projected_schema
                .column_with_name("is_current_ancestor")
                .is_some();
            SingletonRows::History(if needs_ancestors {
                history_ancestors(&metadata)
            } else {
                Default::default()
            })
        }
        IcebergSystemTableType::Refs => {
            let mut names = metadata
                .refs()
                .keys()
                .filter(|name| !is_provider_private_ref(name))
                .cloned()
                .collect::<Vec<_>>();
            names.sort_unstable();
            SingletonRows::Refs(names)
        }
        IcebergSystemTableType::Manifests => {
            if metadata.format_version() == crate::iceberg::spec::FormatVersion::V1 {
                return Err(invalid(
                    "frozen Iceberg $manifests scan must use table format version 2 or 3",
                ));
            }
            SingletonRows::Manifests(read_snapshot_manifests(reader, reference, &metadata).await?)
        }
        IcebergSystemTableType::Files | IcebergSystemTableType::Entries => {
            return Err(invalid(
                "Iceberg manifest relation requires a manifest work unit",
            ));
        }
    };
    Ok(SingletonPages {
        metadata,
        rows,
        schema,
        selected,
        projected_schema,
        context: reader.context.clone(),
        position: 0,
        max_page_rows,
        bytes_read: reader.bytes_read,
        emitted: 0,
        output: None,
        resources,
    })
}

#[derive(Debug, Default)]
struct SnapshotManifests {
    files: Vec<TrinoManifestFile>,
    summaries: Vec<Option<Vec<FieldSummary>>>,
}

impl SnapshotManifests {
    fn of(entries: Vec<ManifestFile>) -> Result<Self, ConnectorError> {
        let mut manifests = Self {
            files: Vec::with_capacity(entries.len()),
            summaries: Vec::with_capacity(entries.len()),
        };
        for entry in entries {
            // `TrinoManifestFile::from_manifest_file` is where an encrypted
            // manifest is refused, so every relation that lists manifests
            // inherits the same rejection.
            manifests
                .files
                .push(TrinoManifestFile::from_manifest_file(&entry)?);
            manifests.summaries.push(entry.partitions);
        }
        Ok(manifests)
    }
}

/// The manifests of the relation's snapshot, with their raw field summaries.
///
/// A relation whose reference pins no snapshot describes a table with no
/// snapshot selected and therefore lists no manifest; that is an empty
/// relation, not a reason to pick the current snapshot.
async fn read_snapshot_manifests(
    reader: &mut FrozenMetadataReader<'_>,
    reference: &IcebergSystemTableReference,
    metadata: &TableMetadata,
) -> Result<SnapshotManifests, ConnectorError> {
    let Some(snapshot_id) = reference.snapshot_id() else {
        return Ok(SnapshotManifests::default());
    };
    let entries = reader.load_manifest_list(metadata, snapshot_id).await?;
    SnapshotManifests::of(entries)
}

/// Check the complete declaration supplied by the frozen system source. Generic
/// field attributes never establish a scalar or nested value domain.
pub(crate) fn validate_system_column_value(
    kind: IcebergSystemTableType,
    declaration: &NestedField,
    source: &Field,
    field: &Field,
    binding: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PureProviderCompileError<ConnectorError>> {
    if field.is_nullable() != binding.nullable() {
        return Err(invalid("system root NULL declaration differs").into());
    }
    if crate::temporal::contains_temporal(&declaration.field_type) {
        let matches = crate::temporal::matches_projection(
            &declaration.field_type,
            source.data_type(),
            field.data_type(),
            binding.value_type(),
            &mut |_| work.step().map_err(SystemMirrorError::Control),
        )
        .map_err(|error| match error {
            SystemMirrorError::Control(cause) => PureProviderCompileError::Control(cause),
            SystemMirrorError::Source(cause) => PureProviderCompileError::Provider(cause),
        })?;
        if !matches {
            return Err(
                invalid("system temporal projection differs from its frozen source").into(),
            );
        }
    }
    system_value(
        field.data_type(),
        binding.value_type(),
        &declaration.field_type,
        system_column_domain(kind, field.name()),
        work,
    )
}

fn system_value(
    layout: &DataType,
    value: &novarocks_type_contract::ValueTypeRef,
    declaration: &Type,
    root_domain: ValueLogicalType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PureProviderCompileError<ConnectorError>> {
    use novarocks_type_contract::ValueKind;
    work.step()?;
    let domain = match declaration {
        Type::Primitive(PrimitiveType::Uuid) => ValueLogicalType::Uuid,
        _ => root_domain,
    };
    if domain != ValueLogicalType::Physical {
        if matches!(value.kind(), ValueKind::Nominal(actual) if *actual == domain)
            && domain.validate_carrier(layout).is_ok()
        {
            return Ok(());
        }
        return Err(invalid("system nominal declaration differs from its source author").into());
    }
    let child = |field: &Field,
                 value: &novarocks_type_contract::ValueTypeRef,
                 declaration: &NestedField,
                 work: &mut CompileCheckpoints<'_>|
     -> Result<(), PureProviderCompileError<ConnectorError>> {
        if field.is_nullable() != value.nullable() || value.nullable() == declaration.required {
            return Err(invalid("system child NULL declaration differs").into());
        }
        system_value(
            field.data_type(),
            value,
            &declaration.field_type,
            ValueLogicalType::Physical,
            work,
        )
    };
    match (layout, value.kind(), declaration) {
        (DataType::Utf8, ValueKind::String, Type::Primitive(PrimitiveType::String))
        | (DataType::Binary, ValueKind::Binary, Type::Primitive(PrimitiveType::Binary)) => Ok(()),
        (layout, ValueKind::Temporal(value), Type::Primitive(primitive))
            if crate::temporal::declaration(primitive) == Some(*value)
                && layout == &value.carrier() =>
        {
            Ok(())
        }
        (layout, ValueKind::Scalar(scalar), Type::Primitive(_)) => {
            let same =
                novarocks_type_contract::arrow_data_types_exact_observed::<SystemMirrorError>(
                    layout,
                    scalar,
                    || work.step().map_err(SystemMirrorError::Control),
                )
                .map_err(|error| match error {
                    SystemMirrorError::Control(cause) => PureProviderCompileError::Control(cause),
                    SystemMirrorError::Source(cause) => PureProviderCompileError::Provider(cause),
                })?;
            if same {
                Ok(())
            } else {
                Err(invalid("system scalar declaration differs").into())
            }
        }
        (DataType::List(field), ValueKind::List(value), Type::List(list)) => {
            child(field, value, &list.element_field, work)
        }
        (DataType::Struct(fields), ValueKind::Struct(values), Type::Struct(row))
            if fields.len() == values.len() && fields.len() == row.fields().len() =>
        {
            for ((field, value), declaration) in fields.iter().zip(values).zip(row.fields()) {
                work.step()?;
                if field.name() != value.name() || field.name() != &declaration.name {
                    return Err(invalid("system named child declaration differs").into());
                }
                child(field, value.value_type(), declaration, work)?;
            }
            Ok(())
        }
        (DataType::Map(entries, false), ValueKind::Map { key, value }, Type::Map(map)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(invalid("system map entries is not a row").into());
            };
            if fields.len() != 2 || entries.is_nullable() {
                return Err(invalid("system map entries declaration differs").into());
            }
            child(&fields[0], key, &map.key_field, work)?;
            child(&fields[1], value, &map.value_field, work)
        }
        _ => Err(invalid("system complete value declaration differs from source").into()),
    }
}

#[cfg(test)]
mod tests;
