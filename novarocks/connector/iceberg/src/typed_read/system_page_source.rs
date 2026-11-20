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
//! Files are distributed by manifest. The eighteen public file columns and
//! twenty-two flat entry columns match the catalog-owned metadata schema.
//! Bounds remain field-ID keyed binary maps, including historical field IDs.
//!
//! Three rules shape every schema here:
//!
//! * the column list, its order and its types are frozen by the design spec, not
//!   inferred from what a reader happens to be able to produce;
//! * a bound retains the manifest's binary encoding under its actual field ID;
//!   it is never narrowed to current-schema fields or rendered as text;
//! * anything this stack cannot prove fails closed. There is no guessed count,
//!   no substituted zero, and no encrypted manifest read with its key ignored.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryBuilder, Float32Array, Float64Array, Int32Array, Int32Builder, Int64Array,
    Int64Builder, ListBuilder, MapBuilder, MapFieldNames, StringArray, StringBuilder, StructArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, SchemaRef, TimeUnit};
use bytes::Bytes;
use novarocks_connector_contract::PureProviderCompileError;
use novarocks_fs::{FileReadContext, FileReadRange};
use novarocks_spi::connector::read_stack::{
    ConnectorPollBudget, OwnedConnectorPageStream, PageSourceMetrics, SourcePage,
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
use crate::manifest::binding::EntrySequences;

use super::column_handle::{IcebergColumnHandle, corrupt, invalid, type_to_json, unsupported};
use super::page_source::{MaterializedPageStream, MaterializedPages, SplitOperations};
use super::system_table::{
    IcebergSystemTableReference, IcebergSystemTableType, SystemManifestSplit, SystemRelationSplit,
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

fn map_type(key: DataType, value: DataType) -> DataType {
    let entries = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("key", key, false)),
        Arc::new(Field::new("value", value, true)),
    ]));
    DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
}

fn list_type(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item, true)))
}

fn timestamp_tz_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into()))
}

fn required(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false)
}

fn nullable(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

/// Physical declarations for the accepted public metadata columns.
pub(crate) fn system_column_domain(_kind: IcebergSystemTableType, _name: &str) -> ValueLogicalType {
    // Public metadata columns carry physical declarations, with no private
    // readable_metrics JSON column in the accepted schema.
    ValueLogicalType::Physical
}
// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Map one Iceberg primitive to its exact Arrow carrier.
///
/// This is deliberately not `metadata_batch_reader::iceberg_type_to_arrow_type`:
/// that mapping drops the time zone of `timestamptz`, which the frozen system
/// schemas are not allowed to do. `uuid` becomes UTF-8 because its canonical
/// hyphenated text is lossless and is how the rest of this crate carries it;
/// `variant` is rejected because it is neither a partition value nor a bound.
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
        PrimitiveType::String | PrimitiveType::Uuid => DataType::Utf8,
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
        DataType::Binary => Some(PrimitiveType::Binary),
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
        // Reuse the exact original closed field authors for static columns.
        // Only the documented schema-derived ROW positions use frozen public
        // shape: the reference itself does not contain table schema/specs.
        let dynamic = matches!(
            (kind, field.name().as_str()),
            (
                IcebergSystemTableType::Files | IcebergSystemTableType::Entries,
                "partition"
            )
        );
        work.step()?;
        work.flush()?;
        let fixed = match kind {
            IcebergSystemTableType::Files => files_fields(
                (field.name() == "partition").then_some(field.data_type()),
                matches!(field.name().as_str(), "lower_bounds" | "upper_bounds")
                    .then_some(field.data_type()),
            ),
            IcebergSystemTableType::Entries => {
                entries_fields((field.name() == "partition").then_some(field.data_type()))
            }
            IcebergSystemTableType::Snapshots => snapshots_fields(),
            IcebergSystemTableType::History => history_fields(),
            IcebergSystemTableType::Refs => refs_fields(),
            IcebergSystemTableType::Manifests => manifests_fields(),
        };
        work.flush()?;
        let mut expected = None;
        for candidate in &fixed {
            let same = candidate.name() == field.name();
            work.step()?;
            if same {
                expected = Some(candidate);
                break;
            }
        }
        let expected =
            expected.ok_or_else(|| unsupported("system relation has no such output column"))?;
        let same_nullable = expected.is_nullable() == field.is_nullable();
        work.step()?;
        if !same_nullable {
            return Err(
                invalid("system column NULL contract differs from its source declaration").into(),
            );
        }
        if dynamic {
            let is_row = matches!(field.data_type(), DataType::Struct(_))
                && logical == ValueLogicalType::Physical;
            work.step()?;
            if !is_row {
                return Err(
                    invalid("system schema-derived column is not its frozen physical ROW").into(),
                );
            }
        } else {
            let expected_domain = system_column_domain(kind, expected.name());
            if logical != expected_domain {
                return Err(invalid(
                    "system column differs from its explicitly declared value domain",
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
    let row = crate::metadata_batch_reader::partition_struct_type(metadata).map_err(corrupt)?;
    let DataType::Struct(fields) = &row else {
        return Err(internal("public partition declaration is not a ROW"));
    };
    Ok((!fields.is_empty()).then_some(row))
}

pub fn system_relation_schema_with_metadata(
    relation: IcebergSystemTableType,
    metadata: &TableMetadata,
) -> Result<SchemaRef, ConnectorError> {
    let fields = match relation {
        IcebergSystemTableType::Files => {
            files_fields(partition_row_type_with_metadata(metadata)?.as_ref(), None)
        }
        IcebergSystemTableType::Entries => {
            entries_fields(partition_row_type_with_metadata(metadata)?.as_ref())
        }
        _ => return system_relation_schema(relation, metadata.current_schema(), &[]),
    };
    Ok(Arc::new(ArrowSchema::new(fields)))
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

/// The `partition` ROW type of one table: the union of every partition spec's
/// fields, ordered by spec id and deduplicated by name.
///
/// Two specs may name the same partition field; they must then agree on its
/// type, because one Arrow column cannot carry two. A disagreement is a hard
/// error rather than a widened column: widening would silently change what a
/// `$files.partition` value means.
///
/// None means there is no partition field under any spec. The public files
/// and flat entries schemas still expose an empty partition ROW in that case.
pub fn partition_row_type(
    schema: &Schema,
    specs: &[PartitionSpec],
) -> Result<Option<DataType>, ConnectorError> {
    Ok(crate::partition_row::UnifiedPartitionRow::of(schema, specs)?.arrow_type())
}

/// The `lower_bounds` / `upper_bounds` ROW type of `$files`.
///
/// Field names are Iceberg field IDs rendered as decimal text and values carry
/// the field's own target type: that is what makes a bound comparable in SQL
/// instead of an opaque byte string the user has to decode by hand.
///
/// `None` means the schema has no primitive field to bound, in which case both
/// bounds columns are absent rather than present-and-always-null.
pub fn bounds_row_type(_schema: &Schema) -> Result<Option<DataType>, ConnectorError> {
    Ok(Some(map_type(DataType::Int32, DataType::Binary)))
}

// ---------------------------------------------------------------------------
// Frozen relation schemas
// ---------------------------------------------------------------------------

/// The eighteen public file columns, in the catalog's accepted order.
/// Bounds preserve field-ID binary maps; partition is always a nullable ROW.
fn files_fields(partition_row: Option<&DataType>, _bounds_row: Option<&DataType>) -> Vec<Field> {
    let mut fields = vec![
        required("content", DataType::Int32),
        required("file_path", DataType::Utf8),
        required("file_format", DataType::Utf8),
        required("spec_id", DataType::Int32),
        required("record_count", DataType::Int64),
        required("file_size_in_bytes", DataType::Int64),
        nullable("column_sizes", map_type(DataType::Int32, DataType::Int64)),
        nullable("value_counts", map_type(DataType::Int32, DataType::Int64)),
        nullable(
            "null_value_counts",
            map_type(DataType::Int32, DataType::Int64),
        ),
        nullable(
            "nan_value_counts",
            map_type(DataType::Int32, DataType::Int64),
        ),
        nullable("lower_bounds", map_type(DataType::Int32, DataType::Binary)),
        nullable("upper_bounds", map_type(DataType::Int32, DataType::Binary)),
        nullable("split_offsets", list_type(DataType::Int64)),
        nullable("equality_ids", list_type(DataType::Int32)),
        nullable("sort_order_id", DataType::Int32),
        nullable("key_metadata", DataType::Binary),
        nullable("first_row_id", DataType::Int64),
    ];
    if let Some(partition) = partition_row {
        fields.push(nullable("partition", partition.clone()));
    }
    fields
}

fn entries_fields(partition_row: Option<&DataType>) -> Vec<Field> {
    let mut fields = vec![
        required("status", DataType::Int32),
        nullable("snapshot_id", DataType::Int64),
        nullable("sequence_number", DataType::Int64),
        nullable("file_sequence_number", DataType::Int64),
    ];
    fields.extend(files_fields(partition_row, None));
    fields
}

fn snapshots_fields() -> Vec<Field> {
    vec![
        required("committed_at", timestamp_tz_type()),
        required("snapshot_id", DataType::Int64),
        nullable("parent_id", DataType::Int64),
        nullable("operation", DataType::Utf8),
        required("manifest_list", DataType::Utf8),
        // Nullable: a v1 snapshot may carry no summary at all, and an absent
        // summary is not the same fact as an empty one.
        nullable("summary", map_type(DataType::Utf8, DataType::Utf8)),
    ]
}

fn history_fields() -> Vec<Field> {
    vec![
        required("made_current_at", timestamp_tz_type()),
        required("snapshot_id", DataType::Int64),
        nullable("parent_id", DataType::Int64),
        required("is_current_ancestor", DataType::Boolean),
    ]
}

fn refs_fields() -> Vec<Field> {
    vec![
        required("name", DataType::Utf8),
        required("type", DataType::Utf8),
        required("snapshot_id", DataType::Int64),
        nullable("max_reference_age_in_ms", DataType::Int64),
        nullable("min_snapshots_to_keep", DataType::Int32),
        nullable("max_snapshot_age_in_ms", DataType::Int64),
    ]
}

fn partition_summary_type() -> DataType {
    list_type(DataType::Struct(Fields::from(vec![
        Arc::new(nullable("contains_null", DataType::Boolean)),
        Arc::new(nullable("contains_nan", DataType::Boolean)),
        Arc::new(nullable("lower_bound", DataType::Utf8)),
        Arc::new(nullable("upper_bound", DataType::Utf8)),
    ])))
}

fn manifests_fields() -> Vec<Field> {
    vec![
        required("content", DataType::Int32),
        required("path", DataType::Utf8),
        required("length", DataType::Int64),
        required("partition_spec_id", DataType::Int32),
        nullable("added_snapshot_id", DataType::Int64),
        required("added_data_files_count", DataType::Int32),
        required("existing_data_files_count", DataType::Int32),
        required("deleted_data_files_count", DataType::Int32),
        required("added_rows_count", DataType::Int64),
        required("existing_rows_count", DataType::Int64),
        required("deleted_rows_count", DataType::Int64),
        nullable("partition_summaries", partition_summary_type()),
    ]
}

/// The frozen output schema of one worker system relation.
///
/// Every schema is derived from the table schema and partition specs the
/// relation is pinned to, so the shape a backend produces is a function of the
/// frozen metadata file and nothing else.
pub fn system_relation_schema(
    relation: IcebergSystemTableType,
    schema: &Schema,
    specs: &[PartitionSpec],
) -> Result<SchemaRef, ConnectorError> {
    let fields = match relation {
        IcebergSystemTableType::Files => files_fields(
            partition_row_type(schema, specs)?.as_ref(),
            bounds_row_type(schema)?.as_ref(),
        ),
        IcebergSystemTableType::Entries => {
            entries_fields(partition_row_type(schema, specs)?.as_ref())
        }
        IcebergSystemTableType::Snapshots => snapshots_fields(),
        IcebergSystemTableType::History => history_fields(),
        IcebergSystemTableType::Refs => refs_fields(),
        IcebergSystemTableType::Manifests => manifests_fields(),
    };
    Ok(Arc::new(ArrowSchema::new(fields)))
}

mod schema_author;
#[cfg(test)]
pub(crate) use schema_author::publish_read_schema;
pub(crate) use schema_author::publish_read_schema_with_metadata;

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
    Ok(list.entries().to_vec())
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
#[derive(Clone, Copy, Debug)]
struct ManifestEntryFacts {
    status: i32,
    snapshot_id: Option<i64>,
    data_sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
}

/// One materialized `$files` row: a manifest entry plus where it was found.
#[derive(Clone, Debug)]
struct FileRow {
    entry: ManifestEntryFacts,
    content: i32,
    file_path: String,
    file_format: &'static str,
    spec_id: i32,
    /// The entry's partition tuple in canonical field-ID slots, resolved
    /// through the exact spec the manifest names.
    partition: Vec<Option<Literal>>,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: BTreeMap<i32, i64>,
    value_counts: BTreeMap<i32, i64>,
    null_value_counts: BTreeMap<i32, i64>,
    nan_value_counts: BTreeMap<i32, i64>,
    lower_bounds: BTreeMap<i32, Datum>,
    upper_bounds: BTreeMap<i32, Datum>,
    key_metadata: Option<Vec<u8>>,
    split_offsets: Option<Vec<i64>>,
    equality_ids: Option<Vec<i32>>,
    sort_order_id: Option<i32>,
    referenced_data_file: Option<String>,
    pos: i64,
    manifest_location: String,
    first_row_id: Option<i64>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

/// Which manifest entries a relation keeps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryStatusRule {
    /// `$files` describes the files a snapshot *has*, so a `DELETED` entry --
    /// the tombstone of a file the snapshot no longer contains -- is skipped.
    SkipDeleted,
    /// `$entries` describes the manifest itself, tombstones included; dropping
    /// them would hide exactly the rows the relation exists to show.
    KeepDeleted,
}

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

fn counts_to_map(
    counts: &HashMap<i32, u64>,
    what: &str,
) -> Result<BTreeMap<i32, i64>, ConnectorError> {
    counts
        .iter()
        .map(|(field_id, count)| {
            let count = i64::try_from(*count).map_err(|_| {
                corrupt(format!(
                    "iceberg {what} for field id {field_id} exceeds Int64"
                ))
            })?;
            Ok((*field_id, count))
        })
        .collect()
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

fn manifest_rows(
    manifest_file: &TrinoManifestFile,
    manifest: &Manifest,
    spec: &PartitionSpec,
    status_rule: EntryStatusRule,
    unified: &crate::partition_row::UnifiedPartitionRow,
) -> Result<Vec<FileRow>, ConnectorError> {
    let mut rows = Vec::with_capacity(manifest.entries().len());
    for (ordinal, entry) in manifest.entries().iter().enumerate() {
        let status = entry.status();
        if status_rule == EntryStatusRule::SkipDeleted && status == ManifestStatus::Deleted {
            continue;
        }
        let sequences = EntrySequences::resolve_from(
            entry,
            manifest_file.added_snapshot_id(),
            manifest_file.sequence_number(),
            false,
        );
        let facts = ManifestEntryFacts {
            status: match status {
                ManifestStatus::Existing => 0,
                ManifestStatus::Added => 1,
                ManifestStatus::Deleted => 2,
            },
            snapshot_id: sequences.snapshot_id,
            data_sequence_number: sequences.data_sequence,
            file_sequence_number: sequences.file_sequence,
        };
        let data_file = entry.data_file();
        let record_count = i64::try_from(data_file.record_count()).map_err(|_| {
            corrupt(format!(
                "iceberg data file {} declares a record count beyond Int64",
                data_file.file_path()
            ))
        })?;
        let file_size_in_bytes = i64::try_from(data_file.file_size_in_bytes()).map_err(|_| {
            corrupt(format!(
                "iceberg data file {} declares a file size beyond Int64",
                data_file.file_path()
            ))
        })?;
        rows.push(FileRow {
            entry: facts,
            content: content_code(data_file.content_type()),
            file_path: data_file.file_path().to_string(),
            // `$files` reports what a manifest recorded; it never opens the
            // file. The Parquet-only restriction on data scans therefore does
            // not apply here, and an ORC or Avro file is reported happily.
            file_format: file_format_name(data_file.file_format()),
            spec_id: manifest_file.partition_spec_id(),
            partition: partition_values(data_file, spec, unified)?,
            record_count,
            file_size_in_bytes,
            column_sizes: counts_to_map(data_file.column_sizes(), "column size")?,
            value_counts: counts_to_map(data_file.value_counts(), "value count")?,
            null_value_counts: counts_to_map(data_file.null_value_counts(), "null value count")?,
            nan_value_counts: counts_to_map(data_file.nan_value_counts(), "nan value count")?,
            lower_bounds: data_file.lower_bounds().clone().into_iter().collect(),
            upper_bounds: data_file.upper_bounds().clone().into_iter().collect(),
            key_metadata: data_file.key_metadata().map(<[u8]>::to_vec),
            split_offsets: data_file.split_offsets().map(<[i64]>::to_vec),
            equality_ids: data_file.equality_ids(),
            sort_order_id: data_file.sort_order_id(),
            referenced_data_file: data_file.referenced_data_file(),
            pos: i64::try_from(ordinal).map_err(|_| {
                corrupt(format!(
                    "iceberg manifest {} has more entries than Int64 can address",
                    manifest_file.path()
                ))
            })?,
            manifest_location: manifest_file.path().to_string(),
            first_row_id: data_file.first_row_id(),
            content_offset: data_file.content_offset(),
            content_size_in_bytes: data_file.content_size_in_bytes(),
        });
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Column builders
// ---------------------------------------------------------------------------

fn int_to_long_map_array<'a>(
    rows: impl Iterator<Item = &'a BTreeMap<i32, i64>>,
) -> Result<ArrayRef, ConnectorError> {
    let mut builder = MapBuilder::new(
        Some(map_field_names()),
        Int32Builder::new(),
        Int64Builder::new(),
    );
    for entries in rows {
        for (key, value) in entries {
            builder.keys().append_value(*key);
            builder.values().append_value(*value);
        }
        builder
            .append(true)
            .map_err(|error| internal(format!("append iceberg metric map: {error}")))?;
    }
    Ok(Arc::new(builder.finish()))
}

fn int_to_binary_map_array<'a>(
    rows: impl Iterator<Item = &'a BTreeMap<i32, Datum>>,
) -> Result<ArrayRef, ConnectorError> {
    let mut builder = MapBuilder::new(
        Some(map_field_names()),
        Int32Builder::new(),
        BinaryBuilder::new(),
    );
    for entries in rows {
        for (key, value) in entries {
            let bytes = value.to_bytes().map_err(|error| {
                corrupt(format!("encode Iceberg bound for field id {key}: {error}"))
            })?;
            builder.keys().append_value(*key);
            builder.values().append_value(bytes.as_ref());
        }
        builder
            .append(true)
            .map_err(|error| internal(format!("append iceberg bound map: {error}")))?;
    }
    Ok(Arc::new(builder.finish()))
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

fn long_list_array<'a>(
    rows: impl Iterator<Item = Option<&'a Vec<i64>>>,
) -> Result<ArrayRef, ConnectorError> {
    let mut builder = ListBuilder::new(Int64Builder::new());
    for values in rows {
        match values {
            Some(values) => {
                for value in values {
                    builder.values().append_value(*value);
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn int_list_array<'a>(
    rows: impl Iterator<Item = Option<&'a Vec<i32>>>,
) -> Result<ArrayRef, ConnectorError> {
    let mut builder = ListBuilder::new(Int32Builder::new());
    for values in rows {
        match values {
            Some(values) => {
                for value in values {
                    builder.values().append_value(*value);
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn binary_array<'a>(
    rows: impl Iterator<Item = Option<&'a Vec<u8>>>,
) -> Result<ArrayRef, ConnectorError> {
    let mut builder = BinaryBuilder::new();
    for value in rows {
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
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
                    // A UUID is stored as an unsigned 128-bit value and is
                    // rendered in its canonical hyphenated form, which is
                    // lossless.
                    Some(PrimitiveLiteral::UInt128(value)) => {
                        collected
                            .push(Some(uuid::Uuid::from_u128(*value).hyphenated().to_string()));
                    }
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

/// The `readable_metrics` JSON of one file row.
///
/// Keys are column names in a stable order, and each value reports the metrics
/// Iceberg recorded for that column. A metric Iceberg did not record is `null`,
/// never a zero.
fn readable_metrics_json(row: &FileRow, schema: &Schema) -> Result<String, ConnectorError> {
    let mut object = serde_json::Map::new();
    for (field_id, _) in schema
        .field_id_to_fields()
        .iter()
        .filter(|(_, field)| matches!(field.field_type.as_ref(), Type::Primitive(_)))
        .collect::<BTreeMap<_, _>>()
    {
        let mut metrics = serde_json::Map::new();
        let number = |value: Option<i64>| {
            value.map_or(serde_json::Value::Null, |value| {
                serde_json::Value::Number(value.into())
            })
        };
        let bound = |value: Option<&Datum>| {
            value.map_or(serde_json::Value::Null, |datum| {
                serde_json::Value::String(datum.to_human_string())
            })
        };
        metrics.insert(
            "column_size".to_string(),
            number(row.column_sizes.get(field_id).copied()),
        );
        metrics.insert(
            "value_count".to_string(),
            number(row.value_counts.get(field_id).copied()),
        );
        metrics.insert(
            "null_value_count".to_string(),
            number(row.null_value_counts.get(field_id).copied()),
        );
        metrics.insert(
            "nan_value_count".to_string(),
            number(row.nan_value_counts.get(field_id).copied()),
        );
        metrics.insert(
            "lower_bound".to_string(),
            bound(row.lower_bounds.get(field_id)),
        );
        metrics.insert(
            "upper_bound".to_string(),
            bound(row.upper_bounds.get(field_id)),
        );
        // The schema's own name index is the key, not the leaf field name:
        // two structs may each hold a field called `x`, and a JSON object
        // would silently keep only one of them.
        let name = schema.name_by_field_id(*field_id).ok_or_else(|| {
            corrupt(format!(
                "iceberg field id {field_id} has no name in its own schema"
            ))
        })?;
        object.insert(name.to_string(), serde_json::Value::Object(metrics));
    }
    serde_json::to_string(&serde_json::Value::Object(object))
        .map_err(|error| internal(format!("render iceberg readable metrics: {error}")))
}

/// Materialize the `$files` columns of one already-frozen relation schema.
fn files_columns(
    schema: &SchemaRef,
    rows: &[FileRow],
    table_schema: &Schema,
) -> Result<Vec<ArrayRef>, ConnectorError> {
    let partitions = rows.iter().map(|row| &row.partition).collect::<Vec<_>>();

    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "status" => Arc::new(Int32Array::from(
                rows.iter().map(|row| row.entry.status).collect::<Vec<_>>(),
            )),
            "snapshot_id" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.entry.snapshot_id)
                    .collect::<Vec<_>>(),
            )),
            "sequence_number" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.entry.data_sequence_number)
                    .collect::<Vec<_>>(),
            )),
            "content" => Arc::new(Int32Array::from(
                rows.iter().map(|row| row.content).collect::<Vec<_>>(),
            )),
            "file_path" => Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.file_path.as_str())
                    .collect::<Vec<_>>(),
            )),
            "file_format" => Arc::new(StringArray::from(
                rows.iter().map(|row| row.file_format).collect::<Vec<_>>(),
            )),
            "spec_id" => Arc::new(Int32Array::from(
                rows.iter().map(|row| row.spec_id).collect::<Vec<_>>(),
            )),
            "partition" => partition_struct_array(field.data_type(), &partitions)?,
            "record_count" => Arc::new(Int64Array::from(
                rows.iter().map(|row| row.record_count).collect::<Vec<_>>(),
            )),
            "file_size_in_bytes" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.file_size_in_bytes)
                    .collect::<Vec<_>>(),
            )),
            "column_sizes" => int_to_long_map_array(rows.iter().map(|row| &row.column_sizes))?,
            "value_counts" => int_to_long_map_array(rows.iter().map(|row| &row.value_counts))?,
            "null_value_counts" => {
                int_to_long_map_array(rows.iter().map(|row| &row.null_value_counts))?
            }
            "nan_value_counts" => {
                int_to_long_map_array(rows.iter().map(|row| &row.nan_value_counts))?
            }
            "lower_bounds" => int_to_binary_map_array(rows.iter().map(|row| &row.lower_bounds))?,
            "upper_bounds" => int_to_binary_map_array(rows.iter().map(|row| &row.upper_bounds))?,
            "key_metadata" => binary_array(rows.iter().map(|row| row.key_metadata.as_ref()))?,
            "split_offsets" => long_list_array(rows.iter().map(|row| row.split_offsets.as_ref()))?,
            "equality_ids" => int_list_array(rows.iter().map(|row| row.equality_ids.as_ref()))?,
            "sort_order_id" => Arc::new(Int32Array::from(
                rows.iter().map(|row| row.sort_order_id).collect::<Vec<_>>(),
            )),
            "readable_metrics" => {
                let mut rendered = Vec::with_capacity(rows.len());
                for row in rows {
                    rendered.push(Some(readable_metrics_json(row, table_schema)?));
                }
                Arc::new(StringArray::from(rendered))
            }
            "added_snapshot_id" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.entry.snapshot_id)
                    .collect::<Vec<_>>(),
            )),
            "file_sequence_number" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.entry.file_sequence_number)
                    .collect::<Vec<_>>(),
            )),
            "data_sequence_number" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.entry.data_sequence_number)
                    .collect::<Vec<_>>(),
            )),
            "referenced_data_file" => Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.referenced_data_file.as_deref())
                    .collect::<Vec<_>>(),
            )),
            "pos" => Arc::new(Int64Array::from(
                rows.iter().map(|row| row.pos).collect::<Vec<_>>(),
            )),
            "manifest_location" => Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.manifest_location.as_str())
                    .collect::<Vec<_>>(),
            )),
            "first_row_id" => Arc::new(Int64Array::from(
                rows.iter().map(|row| row.first_row_id).collect::<Vec<_>>(),
            )),
            "content_offset" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.content_offset)
                    .collect::<Vec<_>>(),
            )),
            "content_size_in_bytes" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.content_size_in_bytes)
                    .collect::<Vec<_>>(),
            )),
            other => {
                return Err(internal(format!(
                    "iceberg $files has no column named {other}"
                )));
            }
        };
        columns.push(array);
    }
    Ok(columns)
}

/// Materialize `$entries`: the entry columns plus the nested `data_file` ROW.
fn entries_columns(
    schema: &SchemaRef,
    rows: &[FileRow],
    table_schema: &Schema,
) -> Result<Vec<ArrayRef>, ConnectorError> {
    files_columns(schema, rows, table_schema)
}

// ---------------------------------------------------------------------------
// Metadata-only relations
// ---------------------------------------------------------------------------

fn snapshots_columns(
    schema: &SchemaRef,
    metadata: &TableMetadata,
) -> Result<Vec<ArrayRef>, ConnectorError> {
    let mut committed_at = Vec::new();
    let mut snapshot_id = Vec::new();
    let mut parent_id = Vec::new();
    let mut operation = Vec::new();
    let mut manifest_list = Vec::new();
    let mut summary = Vec::new();
    for snapshot in metadata.snapshots() {
        let snapshot_summary = snapshot.summary();
        // A write-fence marker is provider bookkeeping: it carries no data and
        // describes no user write, so it is not part of the table's snapshot
        // history even though it is present in the raw metadata (ADR-0068).
        if crate::commit::write_fence::is_fence_marker_snapshot(snapshot_summary) {
            continue;
        }
        committed_at.push(snapshot.timestamp_ms().saturating_mul(1_000));
        snapshot_id.push(snapshot.snapshot_id());
        parent_id.push(snapshot.parent_snapshot_id());
        operation.push(snapshot_summary.operation.as_str().to_string());
        manifest_list.push(snapshot.manifest_list().to_string());
        // The parsed model always materializes a property map, so the only
        // signal that a snapshot carried no summary is that the map is empty.
        // Reporting that as NULL rather than as an empty MAP keeps a v1
        // snapshot from looking like one that summarized nothing.
        summary.push(
            (!snapshot_summary.additional_properties.is_empty()).then(|| {
                snapshot_summary
                    .additional_properties
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<BTreeMap<_, _>>()
            }),
        );
    }

    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "committed_at" => Arc::new(
                TimestampMicrosecondArray::from(committed_at.clone()).with_timezone(UTC_TIME_ZONE),
            ),
            "snapshot_id" => Arc::new(Int64Array::from(snapshot_id.clone())),
            "parent_id" => Arc::new(Int64Array::from(parent_id.clone())),
            "operation" => Arc::new(StringArray::from(operation.clone())),
            "manifest_list" => Arc::new(StringArray::from(manifest_list.clone())),
            "summary" => string_to_string_map_array(summary.iter().map(Option::as_ref))?,
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

fn history_columns(
    schema: &SchemaRef,
    metadata: &TableMetadata,
) -> Result<Vec<ArrayRef>, ConnectorError> {
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

    let entries = metadata.history();
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
) -> Result<Vec<ArrayRef>, ConnectorError> {
    struct RefRow {
        name: String,
        kind: &'static str,
        snapshot_id: i64,
        max_reference_age_in_ms: Option<i64>,
        min_snapshots_to_keep: Option<i32>,
        max_snapshot_age_in_ms: Option<i64>,
    }

    let mut rows = metadata
        .refs()
        .iter()
        .filter(|(name, _)| !is_provider_private_ref(name))
        .map(|(name, reference)| {
            let (kind, max_ref_age, min_snapshots, max_snapshot_age) = match &reference.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep,
                    max_snapshot_age_ms,
                    max_ref_age_ms,
                } => (
                    "BRANCH",
                    *max_ref_age_ms,
                    *min_snapshots_to_keep,
                    *max_snapshot_age_ms,
                ),
                SnapshotRetention::Tag { max_ref_age_ms } => ("TAG", *max_ref_age_ms, None, None),
            };
            RefRow {
                name: name.clone(),
                kind,
                snapshot_id: reference.snapshot_id,
                max_reference_age_in_ms: max_ref_age,
                min_snapshots_to_keep: min_snapshots,
                max_snapshot_age_in_ms: max_snapshot_age,
            }
        })
        .collect::<Vec<_>>();
    // Refs live in a hash map, so an order has to be imposed for the output to
    // be reproducible across runs of the same query.
    rows.sort_by(|left, right| left.name.cmp(&right.name));

    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "name" => Arc::new(StringArray::from(
                rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
            )),
            "type" => Arc::new(StringArray::from(
                rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
            )),
            "snapshot_id" => Arc::new(Int64Array::from(
                rows.iter().map(|row| row.snapshot_id).collect::<Vec<_>>(),
            )),
            "max_reference_age_in_ms" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.max_reference_age_in_ms)
                    .collect::<Vec<_>>(),
            )),
            "min_snapshots_to_keep" => Arc::new(Int32Array::from(
                rows.iter()
                    .map(|row| row.min_snapshots_to_keep)
                    .collect::<Vec<_>>(),
            )),
            "max_snapshot_age_in_ms" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.max_snapshot_age_in_ms)
                    .collect::<Vec<_>>(),
            )),
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
    /// The projected output columns, already in assignment order.
    columns: Vec<ArrayRef>,
    row_count: usize,
    emitted: usize,
    max_page_rows: usize,
    bytes_read: u64,
    retained_bytes: u64,
}

impl IcebergSystemPageSource {
    fn new(
        schema: &SchemaRef,
        relation_columns: Vec<ArrayRef>,
        projection: &[usize],
        row_count: usize,
        max_page_rows: NonZeroUsize,
        bytes_read: u64,
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
        let retained_bytes = columns
            .iter()
            .map(|column| column.get_array_memory_size() as u64)
            .sum();
        Ok(Self {
            columns,
            row_count,
            emitted: 0,
            max_page_rows: max_page_rows.get(),
            bytes_read,
            retained_bytes,
        })
    }
}

impl MaterializedPages for IcebergSystemPageSource {
    fn next_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if self.is_exhausted() {
            return Ok(None);
        }
        let rows = (self.row_count - self.emitted).min(self.max_page_rows);
        let page = if self.columns.is_empty() {
            // A count-only scan legitimately reports positions and no column.
            SourcePage::zero_channel(rows)
        } else {
            SourcePage::try_new(
                rows,
                self.columns
                    .iter()
                    .map(|column| column.slice(self.emitted, rows))
                    .collect(),
            )?
        };
        self.emitted += rows;
        Ok(Some(page))
    }

    fn is_exhausted(&self) -> bool {
        self.emitted >= self.row_count
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
        }
    }

    /// Dispatch one metadata work unit after matching its exact frozen scan reference.
    pub fn create_page_stream(
        &self,
        reference: &IcebergSystemTableReference,
        split: &super::IcebergReadSplit,
        columns: &[IcebergColumnHandle],
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        match split {
            super::IcebergReadSplit::SystemManifest(split) if split.reference() == reference => {
                self.create_manifest_page_stream(split, columns, budget)
            }
            super::IcebergReadSplit::SystemRelation(split) if split.reference() == reference => {
                self.create_relation_page_stream(split, columns, budget)
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
        split: &SystemManifestSplit,
        columns: &[IcebergColumnHandle],
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        let relation = FilesSplitRelation::of(split, columns)?;
        let StreamRead {
            binding,
            context,
            operations,
        } = self.stream_read()?;
        let max_page_rows = self.max_page_rows;
        let load = async move {
            let mut reader = FrozenMetadataReader::new(&binding, &context);
            let manifest = reader
                .load_manifest(&relation.manifest)
                .await
                .map_err(|error| {
                    error.map_message(|message| {
                        crate::manifest::with_variant_statistics_hint_for_history(
                            message,
                            relation.table_has_variant_history,
                        )
                    })
                })?;
            let bytes_read = reader.bytes_read;
            relation.into_page_source(&manifest, max_page_rows, bytes_read)
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
        split: &SystemRelationSplit,
        columns: &[IcebergColumnHandle],
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        let reference = split.reference();
        let StreamRead {
            binding,
            context,
            operations,
        } = self.stream_read()?;
        let reference = reference.clone();
        let columns = columns.to_vec();
        let max_page_rows = self.max_page_rows;
        let load_budget = budget.clone();
        let load = async move {
            let mut reader = FrozenMetadataReader::new(&binding, &context);
            read_single_backend(
                &mut reader,
                &reference,
                &columns,
                max_page_rows,
                &load_budget,
            )
            .await
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

/// One `$files` split's frozen shape; deciding it reads nothing.
struct FilesSplitRelation {
    unified: crate::partition_row::UnifiedPartitionRow,
    kind: IcebergSystemTableType,
    table_has_variant_history: bool,
    manifest: TrinoManifestFile,
    spec: PartitionSpec,
    schema: SchemaRef,
    projection: Vec<usize>,
    table_schema: Schema,
}

impl FilesSplitRelation {
    fn of(
        split: &SystemManifestSplit,
        columns: &[IcebergColumnHandle],
    ) -> Result<Self, ConnectorError> {
        let table_schema: Schema = crate::metadata_json::from_str(split.table_schema_json())
            .map_err(|error| invalid(format!("iceberg table schema json is invalid: {error}")))?;
        let spec = split.parse_partition_spec(split.manifest().partition_spec_id())?;
        let specs = split
            .partition_spec_jsons()
            .keys()
            .map(|spec_id| split.parse_partition_spec(*spec_id))
            .collect::<Result<Vec<_>, _>>()?;
        let unified = crate::partition_row::UnifiedPartitionRow::of(&table_schema, &specs)?;
        let partition = unified.arrow_type();
        let frozen_partition = split
            .partition_column_type_json()
            .map(|json| super::column_handle::parse_type(json, "partition_column_type_json"))
            .transpose()?;
        let expected_partition = partition
            .as_ref()
            .map(|row| metadata_relation_type(row, &mut MetadataRelationFieldIds::new()))
            .transpose()?;
        if frozen_partition != expected_partition
            || split.partition_slot_field_ids() != unified.field_ids()
        {
            return Err(invalid(
                "frozen files partition declaration or field IDs differ from canonical metadata",
            ));
        }
        let kind = split.reference().system_table_type();
        let fields = match kind {
            IcebergSystemTableType::Files => files_fields(partition.as_ref(), None),
            IcebergSystemTableType::Entries => entries_fields(partition.as_ref()),
            _ => {
                return Err(invalid(
                    "iceberg manifest work unit requires files or entries",
                ));
            }
        };
        let schema = Arc::new(ArrowSchema::new(fields));
        let declared = super::column_handle::parse_type(
            split.metadata_table_schema_json(),
            "metadata_table_schema_json",
        )?;
        if declared != metadata_relation_row(&schema)? {
            return Err(invalid(
                "frozen files relation schema differs from its exact source declaration",
            ));
        }
        let projection = project_system_relation_columns(&schema, columns)?;
        Ok(Self {
            unified,
            kind,
            table_has_variant_history: split.table_has_variant_history(),
            manifest: split.manifest().clone(),
            spec,
            schema,
            projection,
            table_schema,
        })
    }

    fn into_page_source(
        self,
        manifest: &Manifest,
        max_page_rows: NonZeroUsize,
        bytes_read: u64,
    ) -> Result<IcebergSystemPageSource, ConnectorError> {
        let rows = manifest_rows(
            &self.manifest,
            manifest,
            &self.spec,
            match self.kind {
                IcebergSystemTableType::Files => EntryStatusRule::SkipDeleted,
                IcebergSystemTableType::Entries => EntryStatusRule::KeepDeleted,
                _ => {
                    return Err(invalid(
                        "iceberg manifest work unit requires files or entries",
                    ));
                }
            },
            &self.unified,
        )?;
        let relation_columns = match self.kind {
            IcebergSystemTableType::Files => {
                files_columns(&self.schema, &rows, &self.table_schema)?
            }
            IcebergSystemTableType::Entries => {
                entries_columns(&self.schema, &rows, &self.table_schema)?
            }
            _ => {
                return Err(invalid(
                    "iceberg manifest work unit requires files or entries",
                ));
            }
        };
        IcebergSystemPageSource::new(
            &self.schema,
            relation_columns,
            &self.projection,
            rows.len(),
            max_page_rows,
            bytes_read,
        )
    }
}

/// A single-backend relation's frozen shape and the scan's projection onto it.
struct SingleBackendRelation {
    kind: SingleBackendKind,
    schema: SchemaRef,
    projection: Vec<usize>,
}

/// What a single-backend relation is rendered from.
#[derive(Clone, Copy, Debug)]
enum SingleBackendKind {
    /// The metadata file alone.
    Metadata(MetadataRelation),
    /// The pinned snapshot's manifest list.
    Manifests,
}

#[derive(Clone, Copy, Debug)]
enum MetadataRelation {
    Snapshots,
    History,
    Refs,
}

/// A relation's columns before the scan's projection.
struct RenderedRelation {
    columns: Vec<ArrayRef>,
    row_count: usize,
}

impl SingleBackendRelation {
    fn of(
        reference: &IcebergSystemTableReference,
        metadata: &TableMetadata,
        columns: &[IcebergColumnHandle],
    ) -> Result<Self, ConnectorError> {
        let schema = system_relation_schema_with_metadata(reference.system_table_type(), metadata)?;
        let projection = project_system_relation_columns(&schema, columns)?;
        let kind = match reference.system_table_type() {
            IcebergSystemTableType::Files | IcebergSystemTableType::Entries => {
                return Err(invalid(
                    "iceberg $files is a distributed relation and arrives as a split",
                ));
            }
            IcebergSystemTableType::Snapshots => {
                SingleBackendKind::Metadata(MetadataRelation::Snapshots)
            }
            IcebergSystemTableType::History => {
                SingleBackendKind::Metadata(MetadataRelation::History)
            }
            IcebergSystemTableType::Refs => SingleBackendKind::Metadata(MetadataRelation::Refs),
            IcebergSystemTableType::Manifests => SingleBackendKind::Manifests,
        };
        Ok(Self {
            kind,
            schema,
            projection,
        })
    }

    fn render_metadata(
        &self,
        relation: MetadataRelation,
        metadata: &TableMetadata,
    ) -> Result<RenderedRelation, ConnectorError> {
        let (columns, row_count) = match relation {
            MetadataRelation::Snapshots => {
                let columns = snapshots_columns(&self.schema, metadata)?;
                let row_count = columns.first().map_or(0, |column| column.len());
                (columns, row_count)
            }
            MetadataRelation::History => {
                let columns = history_columns(&self.schema, metadata)?;
                (columns, metadata.history().len())
            }
            MetadataRelation::Refs => {
                let columns = refs_columns(&self.schema, metadata)?;
                let row_count = columns.first().map_or(0, |column| column.len());
                (columns, row_count)
            }
        };
        Ok(RenderedRelation { columns, row_count })
    }

    fn render_manifests(
        &self,
        metadata: &TableMetadata,
        manifests: &SnapshotManifests,
    ) -> Result<RenderedRelation, ConnectorError> {
        Ok(RenderedRelation {
            columns: manifests_columns(
                &self.schema,
                metadata,
                &manifests.files,
                &manifests.summaries,
            )?,
            row_count: manifests.files.len(),
        })
    }

    fn into_page_source(
        self,
        rendered: RenderedRelation,
        max_page_rows: NonZeroUsize,
        bytes_read: u64,
    ) -> Result<IcebergSystemPageSource, ConnectorError> {
        IcebergSystemPageSource::new(
            &self.schema,
            rendered.columns,
            &self.projection,
            rendered.row_count,
            max_page_rows,
            bytes_read,
        )
    }
}

/// Read one single-backend relation from its frozen metadata. The relation's
/// shape is decided, and a wrong reference refused, before any snapshot file
/// is read; each manifest it parses spends one unit of `budget`.
async fn read_single_backend(
    reader: &mut FrozenMetadataReader<'_>,
    reference: &IcebergSystemTableReference,
    columns: &[IcebergColumnHandle],
    max_page_rows: NonZeroUsize,
    budget: &ConnectorPollBudget,
) -> Result<IcebergSystemPageSource, ConnectorError> {
    let metadata = reader.load_metadata(reference).await?;
    let relation = SingleBackendRelation::of(reference, &metadata, columns)?;
    let rendered = match relation.kind {
        SingleBackendKind::Metadata(table) => relation.render_metadata(table, &metadata)?,
        SingleBackendKind::Manifests => {
            let manifests = read_snapshot_manifests(reader, reference, &metadata, budget).await?;
            relation.render_manifests(&metadata, &manifests)?
        }
    };
    let bytes_read = reader.bytes_read;
    relation.into_page_source(rendered, max_page_rows, bytes_read)
}

/// One pinned snapshot's manifest list.
///
/// The field summaries travel alongside the manifests rather than inside
/// [`TrinoManifestFile`]: they are raw, spec-dependent bytes that only
/// `$manifests` reports, and the frozen split contract deliberately does not
/// carry them across the wire.
#[derive(Debug, Default)]
struct SnapshotManifests {
    files: Vec<TrinoManifestFile>,
    summaries: Vec<Option<Vec<FieldSummary>>>,
}

impl SnapshotManifests {
    fn of(entries: &[ManifestFile]) -> Result<Self, ConnectorError> {
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
                .push(TrinoManifestFile::from_manifest_file(entry)?);
            manifests.summaries.push(entry.partitions.clone());
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
    budget: &ConnectorPollBudget,
) -> Result<SnapshotManifests, ConnectorError> {
    let Some(snapshot_id) = reference.snapshot_id() else {
        return Ok(SnapshotManifests::default());
    };
    let mut entries = reader.load_manifest_list(metadata, snapshot_id).await?;
    for entry in &mut entries {
        if entry.added_files_count.is_some()
            && entry.existing_files_count.is_some()
            && entry.deleted_files_count.is_some()
            && entry.added_rows_count.is_some()
            && entry.existing_rows_count.is_some()
            && entry.deleted_rows_count.is_some()
        {
            continue;
        }
        let published = TrinoManifestFile::from_manifest_file(entry)?;
        let manifest = reader.load_manifest(&published).await?;
        complete_manifest_counts(entry, &manifest, budget).await?;
    }
    SnapshotManifests::of(&entries)
}

async fn complete_manifest_counts(
    entry: &mut ManifestFile,
    manifest: &Manifest,
    budget: &ConnectorPollBudget,
) -> Result<(), ConnectorError> {
    let mut files = [0_u32; 3];
    let mut rows = [0_u64; 3];
    for actual in manifest.entries() {
        budget.consume(1).await;
        let ordinal = match actual.status() {
            ManifestStatus::Added => 0,
            ManifestStatus::Existing => 1,
            ManifestStatus::Deleted => 2,
        };
        files[ordinal] = files[ordinal]
            .checked_add(1)
            .ok_or_else(|| corrupt("Iceberg manifest entry count exceeds UInt32"))?;
        rows[ordinal] = rows[ordinal]
            .checked_add(actual.data_file().record_count())
            .ok_or_else(|| corrupt("Iceberg manifest row count exceeds UInt64"))?;
    }
    // A missing summary is unknown, never implicitly zero. Only a complete
    // read of the same manifest can establish these public required facts.
    entry.added_files_count.get_or_insert(files[0]);
    entry.existing_files_count.get_or_insert(files[1]);
    entry.deleted_files_count.get_or_insert(files[2]);
    entry.added_rows_count.get_or_insert(rows[0]);
    entry.existing_rows_count.get_or_insert(rows[1]);
    entry.deleted_rows_count.get_or_insert(rows[2]);
    Ok(())
}

/// Walk every manifest of the relation's snapshot into `$files`-shaped rows.
/// A snapshot can hold many manifests that are all ready at once, so each
/// one parsed spends one unit of `budget`: the walk yields its turn instead
/// of holding the driver.
#[cfg(test)]
async fn read_snapshot_file_rows(
    reader: &mut FrozenMetadataReader<'_>,
    metadata: &TableMetadata,
    reference: &IcebergSystemTableReference,
    status_rule: EntryStatusRule,
    budget: &ConnectorPollBudget,
) -> Result<Vec<FileRow>, ConnectorError> {
    let manifests = read_snapshot_manifests(reader, reference, metadata, budget).await?;
    let mut rows = Vec::new();
    for manifest_file in &manifests.files {
        let spec = manifest_spec(metadata, manifest_file)?;
        let manifest = reader.load_manifest(manifest_file).await.map_err(|error| {
            error.map_message(|message| {
                crate::manifest::with_variant_statistics_hint(message, metadata)
            })
        })?;
        budget.consume(1).await;
        let specs = metadata
            .partition_specs_iter()
            .map(|spec| spec.as_ref().clone())
            .collect::<Vec<_>>();
        let unified =
            crate::partition_row::UnifiedPartitionRow::of(metadata.current_schema(), &specs)?;
        rows.extend(manifest_rows(
            manifest_file,
            &manifest,
            &spec,
            status_rule,
            &unified,
        )?);
    }
    Ok(rows)
}

/// The partition spec a manifest of the verified metadata was written with.
#[cfg(test)]
fn manifest_spec(
    metadata: &TableMetadata,
    manifest_file: &TrinoManifestFile,
) -> Result<PartitionSpec, ConnectorError> {
    Ok(metadata
        .partition_spec_by_id(manifest_file.partition_spec_id())
        .ok_or_else(|| {
            corrupt(format!(
                "iceberg manifest {} names partition spec {} which the metadata lacks",
                manifest_file.path(),
                manifest_file.partition_spec_id()
            ))
        })?
        .as_ref()
        .clone())
}

/// Check the complete declaration supplied by the frozen system source. Generic
/// field attributes never establish a scalar or nested value domain.
pub(crate) fn validate_system_column_value(
    kind: IcebergSystemTableType,
    field: &Field,
    binding: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PureProviderCompileError<ConnectorError>> {
    if field.is_nullable() != binding.nullable() {
        return Err(invalid("system root NULL declaration differs").into());
    }
    system_value(
        field.data_type(),
        binding.value_type(),
        system_column_domain(kind, field.name()),
        work,
    )
}
fn system_value(
    layout: &DataType,
    value: &novarocks_type_contract::ValueTypeRef,
    domain: ValueLogicalType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PureProviderCompileError<ConnectorError>> {
    use novarocks_type_contract::ValueKind;
    work.step()?;
    if domain != ValueLogicalType::Physical {
        if matches!(value.kind(), ValueKind::Nominal(actual) if *actual == domain)
            && *layout == DataType::Utf8
        {
            return Ok(());
        }
        return Err(invalid("system nominal declaration differs from its source author").into());
    }
    let child = |field: &Field,
                 value: &novarocks_type_contract::ValueTypeRef,
                 work: &mut CompileCheckpoints<'_>| {
        if field.is_nullable() != value.nullable() {
            return Err(invalid("system child NULL declaration differs").into());
        }
        system_value(field.data_type(), value, ValueLogicalType::Physical, work)
    };
    match (layout, value.kind()) {
        (DataType::Utf8, ValueKind::String) | (DataType::Binary, ValueKind::Binary) => Ok(()),
        (layout, ValueKind::Scalar(scalar)) => {
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
        (DataType::List(field), ValueKind::List(value)) => child(field, value, work),
        (DataType::Struct(fields), ValueKind::Struct(values)) if fields.len() == values.len() => {
            for (field, value) in fields.iter().zip(values) {
                work.step()?;
                if field.name() != value.name() {
                    return Err(invalid("system named child declaration differs").into());
                }
                child(field, value.value_type(), work)?;
            }
            Ok(())
        }
        (DataType::Map(entries, false), ValueKind::Map { key, value }) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(invalid("system map entries is not a row").into());
            };
            if fields.len() != 2 || entries.is_nullable() {
                return Err(invalid("system map entries declaration differs").into());
            }
            child(&fields[0], key, work)?;
            child(&fields[1], value, work)
        }
        _ => Err(invalid("system complete value declaration differs from source").into()),
    }
}

#[cfg(test)]
mod tests;
