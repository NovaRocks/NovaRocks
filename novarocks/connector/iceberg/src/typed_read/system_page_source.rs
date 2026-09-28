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
//! Five relations -- `$entries`, `$snapshots`, `$history`, `$refs` and
//! `$manifests` -- run on exactly one selected backend with no split at all.
//! The reader opens the frozen metadata file, proves it is the file planning
//! froze through [`IcebergSystemTableReference::verify_loaded_metadata`], and
//! produces pages straight from it. It never re-resolves the table through the
//! catalog and never falls back to a later snapshot: either would answer a
//! different question than the one that was planned.
//!
//! `$files` is the one distributed relation. One [`FilesTableSplit`] carries one
//! manifest, and this module turns that manifest into the frozen 27-column
//! `$files` shape.
//!
//! `$partitions` is the aggregation Trino calls `PartitionsView`, computed over
//! the same pinned snapshot's `$files` rows. It carries its own reference kind
//! because a `$files` reference reaching a backend reads as the un-aggregated
//! relation -- one row per data file where the query asked for one row per
//! partition -- but both kinds are minted by one resolution, so they can never
//! describe different snapshots. It is read on a single backend: an aggregate
//! over one manifest would report a partition that only part of the snapshot
//! describes.
//!
//! Three rules shape every schema here:
//!
//! * the column list, its order and its types are frozen by the design spec, not
//!   inferred from what a reader happens to be able to produce;
//! * a bound is decoded to the target type its field ID names -- never left as
//!   binary and never rendered as text to make it fit;
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
use novarocks_fs::{FileReadContext, FileReadRange};
use novarocks_spi::connector::read_stack::{
    ConnectorPollBudget, ConnectorSession, OwnedConnectorPageStream, PageSourceMetrics, SourcePage,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_types::logical::{LogicalType, field_with_logical_type};

use crate::access_binding::IcebergReadBinding;
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Datum, FieldSummary, ListType, Literal, Manifest,
    ManifestFile, ManifestList, ManifestStatus, MapType, NestedField, PartitionSpec,
    PrimitiveLiteral, PrimitiveType, Schema, SnapshotRetention, StructType, TableMetadata, Type,
};

use super::column_handle::{IcebergColumnHandle, corrupt, invalid, type_to_json, unsupported};
use super::page_source::{MaterializedPageStream, MaterializedPages, SplitOperations};
use super::system_table::{
    FilesTableSplit, IcebergSystemTableExecution, IcebergSystemTableReference,
    IcebergSystemTableType, TrinoManifestFile,
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

/// A `JSON` column. Arrow has no JSON type, so the crate-wide convention is a
/// UTF-8 field tagged with the engine's logical-type metadata; that keeps the
/// value exact and still tells the engine it is JSON rather than free text.
fn json_column(name: &str) -> Field {
    field_with_logical_type(nullable(name, DataType::Utf8), LogicalType::Json)
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
        PrimitiveType::Variant => {
            return Err(unsupported(
                "an iceberg variant is neither a partition value nor a metric bound",
            ));
        }
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
struct MetadataRelationFieldIds {
    next: i32,
}

impl MetadataRelationFieldIds {
    /// Iceberg field IDs are positive, so the first one is 1.
    const fn new() -> Self {
        Self { next: 1 }
    }

    fn take(&mut self) -> Result<i32, ConnectorError> {
        let field_id = self.next;
        self.next = self.next.checked_add(1).ok_or_else(|| {
            internal("iceberg metadata relation exhausted its column identity field ids")
        })?;
        Ok(field_id)
    }
}

/// The Iceberg mirror of one frozen metadata-relation Arrow field.
fn metadata_relation_field(
    field: &Field,
    ids: &mut MetadataRelationFieldIds,
) -> Result<NestedField, ConnectorError> {
    let field_type = metadata_relation_type(field.data_type(), ids)?;
    let field_id = ids.take()?;
    Ok(if field.is_nullable() {
        NestedField::optional(field_id, field.name(), field_type)
    } else {
        NestedField::required(field_id, field.name(), field_type)
    })
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
fn metadata_relation_type(
    data_type: &DataType,
    ids: &mut MetadataRelationFieldIds,
) -> Result<Type, ConnectorError> {
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
        return Ok(Type::Primitive(primitive));
    }
    match data_type {
        DataType::Struct(fields) => {
            let mut mirrored = Vec::with_capacity(fields.len());
            for field in fields {
                mirrored.push(Arc::new(metadata_relation_field(field.as_ref(), ids)?));
            }
            Ok(Type::Struct(StructType::new(mirrored)))
        }
        DataType::List(element) => Ok(Type::List(ListType::new(Arc::new(
            metadata_relation_field(element.as_ref(), ids)?,
        )))),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(internal(
                    "an iceberg metadata relation map carries entries that are not a struct",
                ));
            };
            let [key, value] = fields.as_ref() else {
                return Err(internal(
                    "an iceberg metadata relation map must carry exactly a key and a value",
                ));
            };
            Ok(Type::Map(MapType::new(
                Arc::new(metadata_relation_field(key.as_ref(), ids)?),
                Arc::new(metadata_relation_field(value.as_ref(), ids)?),
            )))
        }
        other => Err(unsupported(format!(
            "an iceberg metadata relation column carrier {other:?} has no iceberg type"
        ))),
    }
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
/// `None` means the table has no partition field at all under any spec, which
/// is why the `partition` column is absent from an unpartitioned table's
/// `$files`, `$entries.data_file` and `$partitions`.
pub fn partition_row_type(
    schema: &Schema,
    specs: &[PartitionSpec],
) -> Result<Option<DataType>, ConnectorError> {
    let mut ordered: Vec<&PartitionSpec> = specs.iter().collect();
    ordered.sort_by_key(|spec| spec.spec_id());

    let mut fields: Vec<Arc<Field>> = Vec::new();
    for spec in ordered {
        for partition_field in spec.fields() {
            let source = schema
                .field_by_id(partition_field.source_id)
                .ok_or_else(|| {
                    corrupt(format!(
                        "iceberg partition field {} references missing source field id {}",
                        partition_field.name, partition_field.source_id
                    ))
                })?;
            let result_type = partition_field
                .transform
                .result_type(source.field_type.as_ref())
                .map_err(|error| {
                    corrupt(format!(
                        "iceberg partition field {} has no result type: {error}",
                        partition_field.name
                    ))
                })?;
            let Type::Primitive(primitive) = &result_type else {
                return Err(corrupt(format!(
                    "iceberg partition field {} does not transform to a primitive",
                    partition_field.name
                )));
            };
            let arrow_type = iceberg_primitive_to_arrow(primitive)?;
            if let Some(existing) = fields
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(&partition_field.name))
            {
                if existing.data_type() != &arrow_type {
                    return Err(corrupt(format!(
                        "iceberg partition field {} has incompatible types across specs: {:?} vs {arrow_type:?}",
                        partition_field.name,
                        existing.data_type()
                    )));
                }
                continue;
            }
            fields.push(Arc::new(nullable(&partition_field.name, arrow_type)));
        }
    }
    if fields.is_empty() {
        return Ok(None);
    }
    Ok(Some(DataType::Struct(Fields::from(fields))))
}

/// Every primitive field of a schema, in field-id order, with its Arrow type.
///
/// Iceberg keys metrics by field ID, including the IDs of fields nested inside
/// structs, lists and maps, so this walks the whole id index rather than only
/// the top level.
fn primitive_fields_by_id(schema: &Schema) -> Result<Vec<(i32, DataType)>, ConnectorError> {
    let mut fields: Vec<(i32, DataType)> = Vec::new();
    for (field_id, field) in schema.field_id_to_fields() {
        let Type::Primitive(primitive) = field.field_type.as_ref() else {
            continue;
        };
        // A variant has no comparable bound, so it simply contributes no
        // bounds field rather than failing the whole relation.
        if matches!(primitive, PrimitiveType::Variant) {
            continue;
        }
        fields.push((*field_id, iceberg_primitive_to_arrow(primitive)?));
    }
    fields.sort_by_key(|(field_id, _)| *field_id);
    Ok(fields)
}

/// The `lower_bounds` / `upper_bounds` ROW type of `$files`.
///
/// Field names are Iceberg field IDs rendered as decimal text and values carry
/// the field's own target type: that is what makes a bound comparable in SQL
/// instead of an opaque byte string the user has to decode by hand.
///
/// `None` means the schema has no primitive field to bound, in which case both
/// bounds columns are absent rather than present-and-always-null.
pub fn bounds_row_type(schema: &Schema) -> Result<Option<DataType>, ConnectorError> {
    let primitives = primitive_fields_by_id(schema)?;
    if primitives.is_empty() {
        return Ok(None);
    }
    let fields = primitives
        .into_iter()
        .map(|(field_id, arrow_type)| Arc::new(nullable(&field_id.to_string(), arrow_type)))
        .collect::<Vec<_>>();
    Ok(Some(DataType::Struct(Fields::from(fields))))
}

/// The `$partitions.data` ROW type: one field per top-level primitive column,
/// each a `ROW(min, max, null_count, nan_count)` over that column's own type.
///
/// Only top-level columns take part: a nested field's name is not unique in the
/// table, and a ROW cannot carry two fields with one name.
fn partitions_metrics_row_type(schema: &Schema) -> Result<Option<DataType>, ConnectorError> {
    let mut fields: Vec<Arc<Field>> = Vec::new();
    for field in schema.as_struct().fields() {
        let Type::Primitive(primitive) = field.field_type.as_ref() else {
            continue;
        };
        if matches!(primitive, PrimitiveType::Variant) {
            continue;
        }
        let value_type = iceberg_primitive_to_arrow(primitive)?;
        let metrics = DataType::Struct(Fields::from(vec![
            Arc::new(nullable("min", value_type.clone())),
            Arc::new(nullable("max", value_type)),
            Arc::new(nullable("null_count", DataType::Int64)),
            Arc::new(nullable("nan_count", DataType::Int64)),
        ]));
        fields.push(Arc::new(nullable(&field.name, metrics)));
    }
    if fields.is_empty() {
        return Ok(None);
    }
    Ok(Some(DataType::Struct(Fields::from(fields))))
}

// ---------------------------------------------------------------------------
// Frozen relation schemas
// ---------------------------------------------------------------------------

/// The `$files` column list, frozen by the design spec.
///
/// `partition`, `lower_bounds` and `upper_bounds` are the three schema-derived
/// columns: each is present exactly when the table has something for it to
/// describe, which is why the list is 27 columns for a partitioned table and 26
/// without a partition spec.
///
/// A column is declared non-null only where the manifest format itself
/// guarantees a value. Iceberg makes most `DataFile` metrics optional, and a
/// missing count is genuinely unknown -- reporting it as `0` would be a
/// fabricated fact, not a default.
fn files_fields(partition_row: Option<&DataType>, bounds_row: Option<&DataType>) -> Vec<Field> {
    let mut fields = vec![
        required("content", DataType::Int32),
        required("file_path", DataType::Utf8),
        required("file_format", DataType::Utf8),
        required("spec_id", DataType::Int32),
    ];
    if let Some(partition_row) = partition_row {
        fields.push(nullable("partition", partition_row.clone()));
    }
    fields.extend([
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
    ]);
    if let Some(bounds_row) = bounds_row {
        fields.push(nullable("lower_bounds", bounds_row.clone()));
        fields.push(nullable("upper_bounds", bounds_row.clone()));
    }
    fields.extend([
        nullable("key_metadata", DataType::Binary),
        nullable("split_offsets", list_type(DataType::Int64)),
        nullable("equality_ids", list_type(DataType::Int32)),
        nullable("sort_order_id", DataType::Int32),
        json_column("readable_metrics"),
        nullable("added_snapshot_id", DataType::Int64),
        nullable("file_sequence_number", DataType::Int64),
        nullable("data_sequence_number", DataType::Int64),
        nullable("referenced_data_file", DataType::Utf8),
        // `pos` and `manifest_location` are the reader's own address of the
        // row: which manifest it came from and where in that manifest. Both are
        // always known, so both are non-null.
        required("pos", DataType::Int64),
        required("manifest_location", DataType::Utf8),
        nullable("first_row_id", DataType::Int64),
        nullable("content_offset", DataType::Int64),
        nullable("content_size_in_bytes", DataType::Int64),
    ]);
    fields
}

/// The `$entries.data_file` ROW: the file's own facts, nested rather than
/// flattened.
///
/// The bounds are `MAP(INTEGER, VARCHAR)` here, not the typed ROW `$files`
/// uses. That is the frozen contract: `$entries` reports what the manifest
/// literally recorded for every field ID it mentions, including IDs the current
/// schema no longer has, and a ROW cannot name a field the schema dropped.
fn entries_data_file_type(partition_row: Option<&DataType>) -> DataType {
    let mut fields: Vec<Arc<Field>> = vec![
        Arc::new(required("content", DataType::Int32)),
        Arc::new(required("file_path", DataType::Utf8)),
        Arc::new(required("file_format", DataType::Utf8)),
        Arc::new(required("spec_id", DataType::Int32)),
    ];
    if let Some(partition_row) = partition_row {
        fields.push(Arc::new(nullable("partition", partition_row.clone())));
    }
    fields.extend(
        [
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
            nullable("lower_bounds", map_type(DataType::Int32, DataType::Utf8)),
            nullable("upper_bounds", map_type(DataType::Int32, DataType::Utf8)),
            nullable("key_metadata", DataType::Binary),
            nullable("split_offsets", list_type(DataType::Int64)),
            nullable("equality_ids", list_type(DataType::Int32)),
            nullable("sort_order_id", DataType::Int32),
        ]
        .map(Arc::new),
    );
    DataType::Struct(Fields::from(fields))
}

fn entries_fields(partition_row: Option<&DataType>) -> Vec<Field> {
    vec![
        required("status", DataType::Int32),
        nullable("snapshot_id", DataType::Int64),
        nullable("sequence_number", DataType::Int64),
        nullable("file_sequence_number", DataType::Int64),
        required("data_file", entries_data_file_type(partition_row)),
        json_column("readable_metrics"),
    ]
}

fn snapshots_fields() -> Vec<Field> {
    vec![
        required("committed_at", timestamp_tz_type()),
        required("snapshot_id", DataType::Int64),
        nullable("parent_id", DataType::Int64),
        required("operation", DataType::Utf8),
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
        Arc::new(required("contains_null", DataType::Boolean)),
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
        required("added_snapshot_id", DataType::Int64),
        nullable("added_data_files_count", DataType::Int32),
        nullable("added_rows_count", DataType::Int64),
        nullable("existing_data_files_count", DataType::Int32),
        nullable("existing_rows_count", DataType::Int64),
        nullable("deleted_data_files_count", DataType::Int32),
        nullable("deleted_rows_count", DataType::Int64),
        nullable("partition_summaries", partition_summary_type()),
    ]
}

fn partitions_fields(
    partition_row: Option<&DataType>,
    metrics_row: Option<&DataType>,
) -> Vec<Field> {
    let mut fields = Vec::new();
    if let Some(partition_row) = partition_row {
        fields.push(nullable("partition", partition_row.clone()));
    }
    fields.extend([
        required("record_count", DataType::Int64),
        required("file_count", DataType::Int64),
        required("total_size", DataType::Int64),
    ]);
    if let Some(metrics_row) = metrics_row {
        fields.push(nullable("data", metrics_row.clone()));
    }
    fields
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
        IcebergSystemTableType::Partitions => partitions_fields(
            partition_row_type(schema, specs)?.as_ref(),
            partitions_metrics_row_type(schema)?.as_ref(),
        ),
    };
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// The frozen output schema of the `$partitions` view.
///
/// It is a separate entry point because `$partitions` is not a worker relation:
/// it aggregates the `$files` rows of the same pinned snapshot.
pub fn partitions_view_schema(
    schema: &Schema,
    specs: &[PartitionSpec],
) -> Result<SchemaRef, ConnectorError> {
    Ok(Arc::new(ArrowSchema::new(partitions_fields(
        partition_row_type(schema, specs)?.as_ref(),
        partitions_metrics_row_type(schema)?.as_ref(),
    ))))
}

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
    /// `FileIO` instead of this reader's access binding and range service, and
    /// the inherited entry values it would have applied are applied explicitly
    /// in [`ManifestEntryFacts::inherit`], where the inheritance rule is
    /// visible instead of hidden.
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
    let metadata: TableMetadata = serde_json::from_slice(bytes.as_ref()).map_err(|error| {
        corrupt(format!(
            "iceberg metadata file {} is not table metadata: {error}",
            reference.metadata_file_location()
        ))
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
    Manifest::parse_avro(bytes.as_ref()).map_err(|error| {
        corrupt(format!(
            "iceberg manifest {} is invalid: {error}",
            manifest.path()
        ))
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

/// Iceberg's initial sequence number, the value that marks a manifest written
/// before sequence numbers were assigned.
const INITIAL_SEQUENCE_NUMBER: i64 = 0;

impl ManifestEntryFacts {
    /// Apply the manifest-list inheritance the Iceberg spec defines.
    ///
    /// A null entry value is not "unknown": the spec says it is inherited from
    /// the manifest that lists the file. Reproducing the rule here keeps the
    /// values identical to the ones a catalog-side read would report.
    fn inherit(
        status: ManifestStatus,
        snapshot_id: Option<i64>,
        data_sequence_number: Option<i64>,
        file_sequence_number: Option<i64>,
        manifest: &TrinoManifestFile,
    ) -> Self {
        let inheritable = status == ManifestStatus::Added
            || manifest.sequence_number() == INITIAL_SEQUENCE_NUMBER;
        Self {
            status: match status {
                ManifestStatus::Existing => 0,
                ManifestStatus::Added => 1,
                ManifestStatus::Deleted => 2,
            },
            snapshot_id: snapshot_id.or(Some(manifest.added_snapshot_id())),
            data_sequence_number: data_sequence_number
                .or_else(|| inheritable.then(|| manifest.sequence_number())),
            file_sequence_number: file_sequence_number
                .or_else(|| inheritable.then(|| manifest.sequence_number())),
        }
    }
}

/// One materialized `$files` row: a manifest entry plus where it was found.
#[derive(Clone, Debug)]
struct FileRow {
    entry: ManifestEntryFacts,
    content: i32,
    file_path: String,
    file_format: &'static str,
    spec_id: i32,
    /// The entry's partition tuple, already resolved to `name -> value` through
    /// the spec the manifest names.
    partition: BTreeMap<String, Option<Literal>>,
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
) -> Result<BTreeMap<String, Option<Literal>>, ConnectorError> {
    let values = data_file.partition().fields();
    if values.len() != spec.fields().len() {
        return Err(corrupt(format!(
            "iceberg data file {} carries {} partition values for spec {} with {} fields",
            data_file.file_path(),
            values.len(),
            spec.spec_id(),
            spec.fields().len()
        )));
    }
    Ok(spec
        .fields()
        .iter()
        .zip(values)
        .map(|(field, value)| (field.name.clone(), value.clone()))
        .collect())
}

/// Turn the entries of one manifest into `$files`-shaped rows.
fn manifest_rows(
    manifest_file: &TrinoManifestFile,
    manifest: &Manifest,
    spec: &PartitionSpec,
    status_rule: EntryStatusRule,
) -> Result<Vec<FileRow>, ConnectorError> {
    let mut rows = Vec::with_capacity(manifest.entries().len());
    for (ordinal, entry) in manifest.entries().iter().enumerate() {
        let status = entry.status();
        if status_rule == EntryStatusRule::SkipDeleted && status == ManifestStatus::Deleted {
            continue;
        }
        let facts = ManifestEntryFacts::inherit(
            status,
            entry.snapshot_id(),
            entry.sequence_number(),
            entry.file_sequence_number,
            manifest_file,
        );
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
            partition: partition_values(data_file, spec)?,
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

fn int_to_string_map_array<'a>(
    rows: impl Iterator<Item = &'a BTreeMap<i32, Datum>>,
) -> Result<ArrayRef, ConnectorError> {
    let mut builder = MapBuilder::new(
        Some(map_field_names()),
        Int32Builder::new(),
        StringBuilder::new(),
    );
    for entries in rows {
        for (key, value) in entries {
            builder.keys().append_value(*key);
            builder.values().append_value(value.to_human_string());
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
    rows: &[&BTreeMap<String, Option<Literal>>],
) -> Result<ArrayRef, ConnectorError> {
    let DataType::Struct(fields) = row_type else {
        return Err(internal("iceberg partition row type must be a struct"));
    };
    let mut children = Vec::with_capacity(fields.len());
    for field in fields {
        let values = rows
            .iter()
            .map(|row| match row.get(field.name()) {
                // A spec that does not define this field leaves it null, which
                // is the truthful answer for a file written under that spec.
                None | Some(None) => Ok(None),
                Some(Some(Literal::Primitive(literal))) => Ok(Some(literal)),
                Some(Some(other)) => Err(corrupt(format!(
                    "iceberg partition field {} holds a non-primitive value {other:?}",
                    field.name()
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        children.push(primitive_array(field, &values)?);
    }
    Ok(Arc::new(
        StructArray::try_new(fields.clone(), children, None)
            .map_err(|error| internal(format!("build iceberg partition row: {error}")))?,
    ))
}

/// Build the typed `lower_bounds` / `upper_bounds` ROW column.
///
/// The bound of a field ID the current schema no longer has simply has no field
/// to land in; it is dropped rather than degraded into text. `$entries` reports
/// those IDs, which is why its bounds are a map.
fn bounds_struct_array(
    row_type: &DataType,
    rows: &[&BTreeMap<i32, Datum>],
) -> Result<ArrayRef, ConnectorError> {
    let DataType::Struct(fields) = row_type else {
        return Err(internal("iceberg bounds row type must be a struct"));
    };
    let mut children = Vec::with_capacity(fields.len());
    for field in fields {
        let field_id: i32 = field.name().parse().map_err(|_| {
            internal(format!(
                "iceberg bounds row field {} is not a field id",
                field.name()
            ))
        })?;
        let values = rows
            .iter()
            .map(|row| row.get(&field_id).map(Datum::literal))
            .collect::<Vec<_>>();
        children.push(primitive_array(field, &values)?);
    }
    Ok(Arc::new(
        StructArray::try_new(fields.clone(), children, None)
            .map_err(|error| internal(format!("build iceberg bounds row: {error}")))?,
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
    let lower = rows.iter().map(|row| &row.lower_bounds).collect::<Vec<_>>();
    let upper = rows.iter().map(|row| &row.upper_bounds).collect::<Vec<_>>();

    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
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
            "lower_bounds" => bounds_struct_array(field.data_type(), &lower)?,
            "upper_bounds" => bounds_struct_array(field.data_type(), &upper)?,
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
            "file_sequence_number" => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.entry.file_sequence_number)
                    .collect::<Vec<_>>(),
            )),
            "data_file" => entries_data_file_array(field.data_type(), rows)?,
            "readable_metrics" => {
                let mut rendered = Vec::with_capacity(rows.len());
                for row in rows {
                    rendered.push(Some(readable_metrics_json(row, table_schema)?));
                }
                Arc::new(StringArray::from(rendered))
            }
            other => {
                return Err(internal(format!(
                    "iceberg $entries has no column named {other}"
                )));
            }
        };
        columns.push(array);
    }
    Ok(columns)
}

fn entries_data_file_array(
    row_type: &DataType,
    rows: &[FileRow],
) -> Result<ArrayRef, ConnectorError> {
    let DataType::Struct(fields) = row_type else {
        return Err(internal("iceberg $entries data_file must be a struct"));
    };
    let partitions = rows.iter().map(|row| &row.partition).collect::<Vec<_>>();
    let mut children = Vec::with_capacity(fields.len());
    for field in fields {
        let array: ArrayRef = match field.name().as_str() {
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
            "lower_bounds" => int_to_string_map_array(rows.iter().map(|row| &row.lower_bounds))?,
            "upper_bounds" => int_to_string_map_array(rows.iter().map(|row| &row.upper_bounds))?,
            "key_metadata" => binary_array(rows.iter().map(|row| row.key_metadata.as_ref()))?,
            "split_offsets" => long_list_array(rows.iter().map(|row| row.split_offsets.as_ref()))?,
            "equality_ids" => int_list_array(rows.iter().map(|row| row.equality_ids.as_ref()))?,
            "sort_order_id" => Arc::new(Int32Array::from(
                rows.iter().map(|row| row.sort_order_id).collect::<Vec<_>>(),
            )),
            other => {
                return Err(internal(format!(
                    "iceberg $entries data_file has no field named {other}"
                )));
            }
        };
        children.push(array);
    }
    Ok(Arc::new(
        StructArray::try_new(fields.clone(), children, None)
            .map_err(|error| internal(format!("build iceberg $entries data_file: {error}")))?,
    ))
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
    schema: &Schema,
) -> Result<Vec<PrimitiveType>, ConnectorError> {
    spec.fields()
        .iter()
        .map(|partition_field| {
            let source = schema
                .field_by_id(partition_field.source_id)
                .ok_or_else(|| {
                    corrupt(format!(
                        "iceberg partition field {} references missing source field id {}",
                        partition_field.name, partition_field.source_id
                    ))
                })?;
            let result_type = partition_field
                .transform
                .result_type(source.field_type.as_ref())
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
        let partition_types = spec_partition_types(spec, metadata.current_schema())?;
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
// The `$partitions` view
// ---------------------------------------------------------------------------

/// One aggregated partition of the `$partitions` view.
struct PartitionAggregate {
    partition: BTreeMap<String, Option<Literal>>,
    record_count: i64,
    file_count: i64,
    total_size: i64,
    lower_bounds: BTreeMap<i32, Datum>,
    upper_bounds: BTreeMap<i32, Datum>,
    null_counts: BTreeMap<i32, i64>,
    nan_counts: BTreeMap<i32, i64>,
}

/// Aggregate the pinned snapshot's `$files` rows into `$partitions` rows.
///
/// Only data files take part. The old NovaRocks position/equality delete-count
/// columns are gone from this relation, so a delete file has nothing to
/// contribute here; `$files.content` is where delete files are visible.
fn aggregate_partitions(rows: &[FileRow]) -> Vec<PartitionAggregate> {
    // Partitions are reported in the order the snapshot's manifests first
    // mention them, which is reproducible for one pinned snapshot.
    let mut index: HashMap<Vec<Option<Literal>>, usize> = HashMap::new();
    let mut aggregates: Vec<PartitionAggregate> = Vec::new();

    for row in rows.iter().filter(|row| row.content == 0) {
        let key = row.partition.values().cloned().collect::<Vec<_>>();
        let slot = match index.get(&key) {
            Some(slot) => *slot,
            None => {
                let slot = aggregates.len();
                index.insert(key, slot);
                aggregates.push(PartitionAggregate {
                    partition: row.partition.clone(),
                    record_count: 0,
                    file_count: 0,
                    total_size: 0,
                    lower_bounds: BTreeMap::new(),
                    upper_bounds: BTreeMap::new(),
                    null_counts: BTreeMap::new(),
                    nan_counts: BTreeMap::new(),
                });
                slot
            }
        };
        let aggregate = &mut aggregates[slot];
        aggregate.record_count = aggregate.record_count.saturating_add(row.record_count);
        aggregate.file_count = aggregate.file_count.saturating_add(1);
        aggregate.total_size = aggregate.total_size.saturating_add(row.file_size_in_bytes);
        for (field_id, datum) in &row.lower_bounds {
            aggregate
                .lower_bounds
                .entry(*field_id)
                .and_modify(|existing| {
                    if datum < existing {
                        *existing = datum.clone();
                    }
                })
                .or_insert_with(|| datum.clone());
        }
        for (field_id, datum) in &row.upper_bounds {
            aggregate
                .upper_bounds
                .entry(*field_id)
                .and_modify(|existing| {
                    if datum > existing {
                        *existing = datum.clone();
                    }
                })
                .or_insert_with(|| datum.clone());
        }
        for (field_id, count) in &row.null_value_counts {
            *aggregate.null_counts.entry(*field_id).or_insert(0) += *count;
        }
        for (field_id, count) in &row.nan_value_counts {
            *aggregate.nan_counts.entry(*field_id).or_insert(0) += *count;
        }
    }
    aggregates
}

fn partitions_columns(
    schema: &SchemaRef,
    aggregates: &[PartitionAggregate],
    table_schema: &Schema,
) -> Result<Vec<ArrayRef>, ConnectorError> {
    let partitions = aggregates
        .iter()
        .map(|aggregate| &aggregate.partition)
        .collect::<Vec<_>>();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "partition" => partition_struct_array(field.data_type(), &partitions)?,
            "record_count" => Arc::new(Int64Array::from(
                aggregates
                    .iter()
                    .map(|aggregate| aggregate.record_count)
                    .collect::<Vec<_>>(),
            )),
            "file_count" => Arc::new(Int64Array::from(
                aggregates
                    .iter()
                    .map(|aggregate| aggregate.file_count)
                    .collect::<Vec<_>>(),
            )),
            "total_size" => Arc::new(Int64Array::from(
                aggregates
                    .iter()
                    .map(|aggregate| aggregate.total_size)
                    .collect::<Vec<_>>(),
            )),
            "data" => partitions_metrics_array(field.data_type(), aggregates, table_schema)?,
            other => {
                return Err(internal(format!(
                    "iceberg $partitions has no column named {other}"
                )));
            }
        };
        columns.push(array);
    }
    Ok(columns)
}

fn partitions_metrics_array(
    row_type: &DataType,
    aggregates: &[PartitionAggregate],
    table_schema: &Schema,
) -> Result<ArrayRef, ConnectorError> {
    let DataType::Struct(fields) = row_type else {
        return Err(internal("iceberg $partitions data must be a struct"));
    };
    let mut children = Vec::with_capacity(fields.len());
    for field in fields {
        let field_id = table_schema.field_id_by_name(field.name()).ok_or_else(|| {
            internal(format!(
                "iceberg $partitions metrics field {} has no schema field id",
                field.name()
            ))
        })?;
        let DataType::Struct(metric_fields) = field.data_type() else {
            return Err(internal(
                "iceberg $partitions metrics field must be a struct",
            ));
        };
        let mut metric_children: Vec<ArrayRef> = Vec::with_capacity(metric_fields.len());
        for metric_field in metric_fields {
            let array: ArrayRef = match metric_field.name().as_str() {
                "min" => primitive_array(
                    metric_field,
                    &aggregates
                        .iter()
                        .map(|aggregate| aggregate.lower_bounds.get(&field_id).map(Datum::literal))
                        .collect::<Vec<_>>(),
                )?,
                "max" => primitive_array(
                    metric_field,
                    &aggregates
                        .iter()
                        .map(|aggregate| aggregate.upper_bounds.get(&field_id).map(Datum::literal))
                        .collect::<Vec<_>>(),
                )?,
                "null_count" => Arc::new(Int64Array::from(
                    aggregates
                        .iter()
                        .map(|aggregate| aggregate.null_counts.get(&field_id).copied())
                        .collect::<Vec<_>>(),
                )),
                "nan_count" => Arc::new(Int64Array::from(
                    aggregates
                        .iter()
                        .map(|aggregate| aggregate.nan_counts.get(&field_id).copied())
                        .collect::<Vec<_>>(),
                )),
                other => {
                    return Err(internal(format!(
                        "iceberg $partitions metrics has no field named {other}"
                    )));
                }
            };
            metric_children.push(array);
        }
        children.push(Arc::new(
            StructArray::try_new(metric_fields.clone(), metric_children, None)
                .map_err(|error| internal(format!("build iceberg $partitions metrics: {error}")))?,
        ) as ArrayRef);
    }
    Ok(Arc::new(
        StructArray::try_new(fields.clone(), children, None)
            .map_err(|error| internal(format!("build iceberg $partitions data: {error}")))?,
    ))
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

    /// The `$files` page stream for one manifest of the pinned snapshot,
    /// polled with `budget`. Creating it reads nothing; its first poll reads
    /// the manifest through the stream's own operations.
    ///
    /// `$files` is the one distributed system relation, so it arrives as a
    /// split rather than through the direct whole-relation provider path,
    /// whose contract has no split at all.
    pub fn create_files_page_stream(
        &self,
        split: &FilesTableSplit,
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
            let manifest = reader.load_manifest(&relation.manifest).await?;
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
    fn create_single_backend_page_stream(
        &self,
        reference: &IcebergSystemTableReference,
        columns: &[IcebergColumnHandle],
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
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
    manifest: TrinoManifestFile,
    spec: PartitionSpec,
    schema: SchemaRef,
    projection: Vec<usize>,
    table_schema: Schema,
}

impl FilesSplitRelation {
    fn of(
        split: &FilesTableSplit,
        columns: &[IcebergColumnHandle],
    ) -> Result<Self, ConnectorError> {
        let table_schema: Schema = serde_json::from_str(split.table_schema_json())
            .map_err(|error| invalid(format!("iceberg table schema json is invalid: {error}")))?;
        let spec = split.parse_partition_spec(split.manifest().partition_spec_id())?;
        let specs = split
            .partition_spec_jsons()
            .keys()
            .map(|spec_id| split.parse_partition_spec(*spec_id))
            .collect::<Result<Vec<_>, _>>()?;
        let schema = system_relation_schema(IcebergSystemTableType::Files, &table_schema, &specs)?;
        let projection = project_system_relation_columns(&schema, columns)?;
        Ok(Self {
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
            EntryStatusRule::SkipDeleted,
        )?;
        let relation_columns = files_columns(&self.schema, &rows, &self.table_schema)?;
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

/// A single-backend relation's frozen shape: its schema, the scan's
/// projection onto it, and the table schema its values are rendered with.
struct SingleBackendRelation {
    kind: SingleBackendKind,
    schema: SchemaRef,
    projection: Vec<usize>,
    table_schema: Schema,
}

/// What a single-backend relation is rendered from.
#[derive(Clone, Copy, Debug)]
enum SingleBackendKind {
    /// The metadata file alone.
    Metadata(MetadataRelation),
    /// The pinned snapshot's manifest list.
    Manifests,
    /// The rows of every manifest of the pinned snapshot.
    FileRows(FileRowsRelation),
}

#[derive(Clone, Copy, Debug)]
enum MetadataRelation {
    Snapshots,
    History,
    Refs,
}

#[derive(Clone, Copy, Debug)]
enum FileRowsRelation {
    Entries,
    Partitions,
}

impl FileRowsRelation {
    const fn status_rule(self) -> EntryStatusRule {
        match self {
            Self::Entries => EntryStatusRule::KeepDeleted,
            Self::Partitions => EntryStatusRule::SkipDeleted,
        }
    }
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
        let specs = metadata
            .partition_specs_iter()
            .map(|spec| spec.as_ref().clone())
            .collect::<Vec<_>>();
        let table_schema = metadata.current_schema().as_ref().clone();
        let schema = system_relation_schema(reference.system_table_type(), &table_schema, &specs)?;
        let projection = project_system_relation_columns(&schema, columns)?;
        let kind = match reference.system_table_type() {
            IcebergSystemTableType::Files => {
                return Err(invalid(
                    "iceberg $files is a distributed relation and arrives as a split",
                ));
            }
            IcebergSystemTableType::Entries => {
                SingleBackendKind::FileRows(FileRowsRelation::Entries)
            }
            IcebergSystemTableType::Partitions => {
                SingleBackendKind::FileRows(FileRowsRelation::Partitions)
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
            table_schema,
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

    fn render_file_rows(
        &self,
        relation: FileRowsRelation,
        rows: &[FileRow],
    ) -> Result<RenderedRelation, ConnectorError> {
        match relation {
            FileRowsRelation::Entries => Ok(RenderedRelation {
                columns: entries_columns(&self.schema, rows, &self.table_schema)?,
                row_count: rows.len(),
            }),
            // The aggregation reads the same rows `$files` produces for this
            // pinned snapshot and then groups them. It walks every manifest
            // rather than one, because an aggregate over a single manifest
            // would report a partition that only part of the snapshot
            // describes -- which is why it cannot be a distributed relation.
            FileRowsRelation::Partitions => {
                let aggregates = aggregate_partitions(rows);
                Ok(RenderedRelation {
                    columns: partitions_columns(&self.schema, &aggregates, &self.table_schema)?,
                    row_count: aggregates.len(),
                })
            }
        }
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
            let manifests = read_snapshot_manifests(reader, reference, &metadata).await?;
            relation.render_manifests(&metadata, &manifests)?
        }
        SingleBackendKind::FileRows(table) => {
            let rows =
                read_snapshot_file_rows(reader, &metadata, reference, table.status_rule(), budget)
                    .await?;
            relation.render_file_rows(table, &rows)?
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
) -> Result<SnapshotManifests, ConnectorError> {
    let Some(snapshot_id) = reference.snapshot_id() else {
        return Ok(SnapshotManifests::default());
    };
    SnapshotManifests::of(&reader.load_manifest_list(metadata, snapshot_id).await?)
}

/// Walk every manifest of the relation's snapshot into `$files`-shaped rows.
/// A snapshot can hold many manifests that are all ready at once, so each
/// one parsed spends one unit of `budget`: the walk yields its turn instead
/// of holding the driver.
async fn read_snapshot_file_rows(
    reader: &mut FrozenMetadataReader<'_>,
    metadata: &TableMetadata,
    reference: &IcebergSystemTableReference,
    status_rule: EntryStatusRule,
    budget: &ConnectorPollBudget,
) -> Result<Vec<FileRow>, ConnectorError> {
    let manifests = read_snapshot_manifests(reader, reference, metadata).await?;
    let mut rows = Vec::new();
    for manifest_file in &manifests.files {
        let spec = manifest_spec(metadata, manifest_file)?;
        let manifest = reader.load_manifest(manifest_file).await?;
        budget.consume(1).await;
        rows.extend(manifest_rows(manifest_file, &manifest, &spec, status_rule)?);
    }
    Ok(rows)
}

/// The partition spec a manifest of the verified metadata was written with.
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

impl<P> novarocks_spi::connector::read_stack::adapter::ProviderReadSystemTableProvider<P>
    for IcebergSystemTableProvider
where
    P: novarocks_spi::connector::read_stack::adapter::ProviderReadRuntime<
            Table = crate::typed_read::IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = crate::typed_read::HiveTransactionHandle,
            Split = crate::typed_read::IcebergReadSplit,
        >,
{
    fn create_system_page_stream(
        &self,
        _session: &ConnectorSession,
        table: &crate::typed_read::IcebergRuntimeRelation,
        columns: &[novarocks_spi::connector::read_stack::Assignment<IcebergColumnHandle>],
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        let reference = direct_system_reference(table)?;
        self.create_single_backend_page_stream(reference, &assigned_columns(columns), budget)
    }
}

/// The single-backend reference a direct system read names.
fn direct_system_reference(
    table: &crate::typed_read::IcebergRuntimeRelation,
) -> Result<&IcebergSystemTableReference, ConnectorError> {
    let crate::typed_read::IcebergRuntimeRelation::SystemTable(reference) = table else {
        return Err(invalid(
            "an iceberg system page source reads a system relation, not another relation",
        ));
    };
    match reference.system_table_type().execution() {
        IcebergSystemTableExecution::SingleBackendDirectPageSource => Ok(reference),
        IcebergSystemTableExecution::DistributedSplits => Err(invalid(format!(
            "iceberg {} is scheduled through a split source, not the direct system page source",
            reference.system_table_type().suffix()
        ))),
    }
}

fn assigned_columns(
    columns: &[novarocks_spi::connector::read_stack::Assignment<IcebergColumnHandle>],
) -> Vec<IcebergColumnHandle> {
    columns
        .iter()
        .map(|assignment| assignment.column().clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::time::{Duration, Instant};

    use arrow::array::{ListArray, MapArray};
    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::read_stack::SchemaTableName;
    use novarocks_types::logical::logical_type_of_field;

    use crate::iceberg::spec::{
        DataFileBuilder, DataFileFormat, FormatVersion, ManifestListWriter, ManifestWriterBuilder,
        NestedField, Operation, PartitionSpec, Snapshot, SnapshotReference, SortOrder, Struct,
        Summary, TableMetadataBuilder, Transform,
    };
    use crate::typed_read::system_table::{
        FilesTableSplitParams, IcebergSystemTableReferenceParams, TrinoManifestFileParams,
    };
    use crate::typed_read::test_spawners::{GatedTaskSpawner, route_reads_through};
    use crate::typed_read::test_streams::DrivenStream;

    use super::*;

    const SNAPSHOT_ID: i64 = 100;
    const SNAPSHOT_SEQUENCE_NUMBER: i64 = 3;

    fn runtime_and_binding() -> (tokio::runtime::Runtime, IcebergReadBinding, FileReadContext) {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(60)),
            runtime: file_runtime,
            task_spawner,
            range: None,
        };
        (runtime, binding, context)
    }

    fn provider(
        binding: &IcebergReadBinding,
        context: &FileReadContext,
    ) -> IcebergSystemTableProvider {
        IcebergSystemTableProvider::new(
            binding.clone(),
            context.clone(),
            NonZeroUsize::new(1024).expect("nonzero"),
        )
    }

    // -- pure schema fixtures, no I/O -------------------------------------

    fn test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("schema")
    }

    fn identity_spec(schema: &Schema, spec_id: i32, column: &str) -> PartitionSpec {
        named_identity_spec(schema, spec_id, column, column)
    }

    fn named_identity_spec(
        schema: &Schema,
        spec_id: i32,
        column: &str,
        partition_name: &str,
    ) -> PartitionSpec {
        PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(spec_id)
            .add_partition_field(column, partition_name, Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec")
    }

    fn column_names(schema: &SchemaRef) -> Vec<String> {
        schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    #[test]
    fn files_freezes_its_twenty_seven_columns_in_order() {
        let schema = test_schema();
        let specs = vec![identity_spec(&schema, 7, "region")];
        let relation =
            system_relation_schema(IcebergSystemTableType::Files, &schema, &specs).expect("schema");

        assert_eq!(
            column_names(&relation),
            vec![
                "content",
                "file_path",
                "file_format",
                "spec_id",
                "partition",
                "record_count",
                "file_size_in_bytes",
                "column_sizes",
                "value_counts",
                "null_value_counts",
                "nan_value_counts",
                "lower_bounds",
                "upper_bounds",
                "key_metadata",
                "split_offsets",
                "equality_ids",
                "sort_order_id",
                "readable_metrics",
                "added_snapshot_id",
                "file_sequence_number",
                "data_sequence_number",
                "referenced_data_file",
                "pos",
                "manifest_location",
                "first_row_id",
                "content_offset",
                "content_size_in_bytes",
            ]
        );
        assert_eq!(relation.fields().len(), 27);

        let field = |name: &str| relation.field_with_name(name).expect("field").clone();
        assert_eq!(field("content").data_type(), &DataType::Int32);
        assert_eq!(field("file_path").data_type(), &DataType::Utf8);
        assert_eq!(field("record_count").data_type(), &DataType::Int64);
        assert_eq!(
            field("column_sizes").data_type(),
            &map_type(DataType::Int32, DataType::Int64)
        );
        assert_eq!(field("key_metadata").data_type(), &DataType::Binary);
        assert_eq!(
            field("split_offsets").data_type(),
            &list_type(DataType::Int64)
        );
        assert_eq!(
            field("equality_ids").data_type(),
            &list_type(DataType::Int32)
        );
        assert_eq!(field("sort_order_id").data_type(), &DataType::Int32);
        // JSON is carried as UTF-8 tagged with the engine's logical type, which
        // is the only way Arrow can name it.
        assert_eq!(field("readable_metrics").data_type(), &DataType::Utf8);
        assert_eq!(
            logical_type_of_field(&field("readable_metrics")),
            Some(LogicalType::Json)
        );
        for name in [
            "added_snapshot_id",
            "file_sequence_number",
            "data_sequence_number",
            "pos",
            "first_row_id",
            "content_offset",
            "content_size_in_bytes",
        ] {
            assert_eq!(field(name).data_type(), &DataType::Int64, "{name}");
        }
        for name in ["referenced_data_file", "manifest_location"] {
            assert_eq!(field(name).data_type(), &DataType::Utf8, "{name}");
        }
    }

    #[test]
    fn files_bounds_are_typed_rows_keyed_by_field_id_not_binary() {
        let schema = test_schema();
        let specs = vec![identity_spec(&schema, 7, "region")];
        let relation =
            system_relation_schema(IcebergSystemTableType::Files, &schema, &specs).expect("schema");

        for name in ["lower_bounds", "upper_bounds"] {
            let field = relation.field_with_name(name).expect("bounds field");
            let DataType::Struct(fields) = field.data_type() else {
                panic!("{name} must be a ROW, not {:?}", field.data_type());
            };
            assert_eq!(
                fields
                    .iter()
                    .map(|field| (field.name().clone(), field.data_type().clone()))
                    .collect::<Vec<_>>(),
                vec![
                    ("1".to_string(), DataType::Int64),
                    ("2".to_string(), DataType::Utf8),
                ],
                "{name}"
            );
        }
    }

    #[test]
    fn entries_nests_its_data_file_row_and_flattens_nothing() {
        let schema = test_schema();
        let specs = vec![identity_spec(&schema, 7, "region")];
        let relation = system_relation_schema(IcebergSystemTableType::Entries, &schema, &specs)
            .expect("schema");

        assert_eq!(
            column_names(&relation),
            vec![
                "status",
                "snapshot_id",
                "sequence_number",
                "file_sequence_number",
                "data_file",
                "readable_metrics",
            ]
        );
        assert_eq!(
            relation
                .field_with_name("status")
                .expect("status")
                .data_type(),
            &DataType::Int32
        );

        let DataType::Struct(fields) = relation
            .field_with_name("data_file")
            .expect("data_file")
            .data_type()
        else {
            panic!("data_file must be a ROW");
        };
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            vec![
                "content",
                "file_path",
                "file_format",
                "spec_id",
                "partition",
                "record_count",
                "file_size_in_bytes",
                "column_sizes",
                "value_counts",
                "null_value_counts",
                "nan_value_counts",
                "lower_bounds",
                "upper_bounds",
                "key_metadata",
                "split_offsets",
                "equality_ids",
                "sort_order_id",
            ]
        );
        // `$entries` reports what the manifest recorded for every field ID,
        // including IDs the current schema dropped, so its bounds are maps.
        for name in ["lower_bounds", "upper_bounds"] {
            assert_eq!(
                fields
                    .iter()
                    .find(|field| field.name() == name)
                    .expect("bounds")
                    .data_type(),
                &map_type(DataType::Int32, DataType::Utf8),
                "{name}"
            );
        }
    }

    #[test]
    fn snapshots_history_refs_and_manifests_freeze_their_exact_shapes() {
        let schema = test_schema();
        let specs = vec![identity_spec(&schema, 7, "region")];

        let snapshots = system_relation_schema(IcebergSystemTableType::Snapshots, &schema, &specs)
            .expect("schema");
        assert_eq!(
            column_names(&snapshots),
            vec![
                "committed_at",
                "snapshot_id",
                "parent_id",
                "operation",
                "manifest_list",
                "summary",
            ]
        );
        assert_eq!(
            snapshots
                .field_with_name("committed_at")
                .expect("committed_at")
                .data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into()))
        );
        assert_eq!(
            snapshots
                .field_with_name("summary")
                .expect("summary")
                .data_type(),
            &map_type(DataType::Utf8, DataType::Utf8)
        );

        let history = system_relation_schema(IcebergSystemTableType::History, &schema, &specs)
            .expect("schema");
        assert_eq!(
            column_names(&history),
            vec![
                "made_current_at",
                "snapshot_id",
                "parent_id",
                "is_current_ancestor",
            ]
        );
        assert_eq!(
            history
                .field_with_name("made_current_at")
                .expect("made_current_at")
                .data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into()))
        );
        assert_eq!(
            history
                .field_with_name("is_current_ancestor")
                .expect("is_current_ancestor")
                .data_type(),
            &DataType::Boolean
        );

        let refs =
            system_relation_schema(IcebergSystemTableType::Refs, &schema, &specs).expect("schema");
        assert_eq!(
            refs.fields()
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect::<Vec<_>>(),
            vec![
                ("name".to_string(), DataType::Utf8),
                ("type".to_string(), DataType::Utf8),
                ("snapshot_id".to_string(), DataType::Int64),
                ("max_reference_age_in_ms".to_string(), DataType::Int64),
                ("min_snapshots_to_keep".to_string(), DataType::Int32),
                ("max_snapshot_age_in_ms".to_string(), DataType::Int64),
            ]
        );

        let manifests = system_relation_schema(IcebergSystemTableType::Manifests, &schema, &specs)
            .expect("schema");
        assert_eq!(
            manifests
                .fields()
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect::<Vec<_>>(),
            vec![
                ("content".to_string(), DataType::Int32),
                ("path".to_string(), DataType::Utf8),
                ("length".to_string(), DataType::Int64),
                ("partition_spec_id".to_string(), DataType::Int32),
                ("added_snapshot_id".to_string(), DataType::Int64),
                ("added_data_files_count".to_string(), DataType::Int32),
                ("added_rows_count".to_string(), DataType::Int64),
                ("existing_data_files_count".to_string(), DataType::Int32),
                ("existing_rows_count".to_string(), DataType::Int64),
                ("deleted_data_files_count".to_string(), DataType::Int32),
                ("deleted_rows_count".to_string(), DataType::Int64),
                ("partition_summaries".to_string(), partition_summary_type()),
            ]
        );
    }

    #[test]
    fn partitions_drops_the_old_delete_count_columns() {
        let schema = test_schema();
        let specs = vec![identity_spec(&schema, 7, "region")];
        let relation = partitions_view_schema(&schema, &specs).expect("schema");

        assert_eq!(
            column_names(&relation),
            vec![
                "partition",
                "record_count",
                "file_count",
                "total_size",
                "data"
            ]
        );
        assert!(
            relation
                .field_with_name("position_delete_file_count")
                .is_err()
        );
        assert!(
            relation
                .field_with_name("equality_delete_file_count")
                .is_err()
        );

        let DataType::Struct(metrics) = relation.field_with_name("data").expect("data").data_type()
        else {
            panic!("data must be a ROW");
        };
        assert_eq!(
            metrics
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            vec!["id", "region"]
        );
        let DataType::Struct(id_metrics) = metrics[0].data_type() else {
            panic!("per-column metrics must be a ROW");
        };
        assert_eq!(
            id_metrics
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect::<Vec<_>>(),
            vec![
                ("min".to_string(), DataType::Int64),
                ("max".to_string(), DataType::Int64),
                ("null_count".to_string(), DataType::Int64),
                ("nan_count".to_string(), DataType::Int64),
            ]
        );
    }

    #[test]
    fn the_partition_row_is_the_union_of_every_spec_and_a_conflict_is_an_error() {
        let schema = test_schema();
        let by_region = identity_spec(&schema, 1, "region");
        let by_id = identity_spec(&schema, 2, "id");
        let row = partition_row_type(&schema, &[by_id.clone(), by_region.clone()])
            .expect("partition row")
            .expect("partitioned");
        let DataType::Struct(fields) = row else {
            panic!("partition row must be a struct");
        };
        // Ordered by spec id, so the spec-1 field comes first even though the
        // caller listed spec 2 first.
        assert_eq!(
            fields
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect::<Vec<_>>(),
            vec![
                ("region".to_string(), DataType::Utf8),
                ("id".to_string(), DataType::Int64),
            ]
        );

        // The same partition-field name may be repeated across specs, but not
        // with two types: one Arrow column cannot carry both.
        let text_part = named_identity_spec(&schema, 3, "region", "part");
        let long_part = named_identity_spec(&schema, 4, "id", "part");
        assert!(
            partition_row_type(&schema, &[text_part.clone(), text_part.clone()])
                .expect("repeated but agreeing")
                .is_some()
        );
        let error = partition_row_type(&schema, &[text_part, long_part])
            .expect_err("conflicting partition field types");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);

        // An unpartitioned table simply has no partition column.
        assert!(
            partition_row_type(&schema, &[PartitionSpec::unpartition_spec()])
                .expect("unpartitioned")
                .is_none()
        );
        let unpartitioned = system_relation_schema(
            IcebergSystemTableType::Files,
            &schema,
            &[PartitionSpec::unpartition_spec()],
        )
        .expect("schema");
        assert_eq!(unpartitioned.fields().len(), 26);
        assert!(unpartitioned.field_with_name("partition").is_err());
    }

    // -- real metadata on disk --------------------------------------------

    /// A real Iceberg table on disk: manifests, a manifest list, and the exact
    /// metadata file a frozen reference points at. The caller owns the tempdir.
    struct Warehouse {
        metadata_location: String,
        metadata: TableMetadata,
    }

    fn data_file(path: &str, region: &str, records: u64, size: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter([Some(Literal::Primitive(
                PrimitiveLiteral::String(region.to_string()),
            ))]))
            .partition_spec_id(0)
            .record_count(records)
            .file_size_in_bytes(size)
            .lower_bounds(HashMap::from([(1, Datum::long(1_i64))]))
            .upper_bounds(HashMap::from([(1, Datum::long(9_i64))]))
            .null_value_counts(HashMap::from([(1_i32, 0_u64)]))
            .column_sizes(HashMap::from([(1_i32, 64_u64)]))
            .build()
            .expect("data file")
    }

    /// One snapshot with two manifests: the first holds two live data files and
    /// the second holds one file that has already been deleted, so `$files` and
    /// `$entries` must disagree about it.
    async fn build_warehouse(dir: &Path, binding: IcebergReadBinding) -> Warehouse {
        let location = format!("file://{}", dir.display());
        std::fs::create_dir_all(dir.join("metadata")).expect("metadata dir");
        let schema = test_schema();
        // `TableMetadataBuilder::new` reassigns the initial spec to id 0, so the
        // manifests must declare the same id or they would name a spec the
        // metadata does not have.
        let spec = identity_spec(&schema, 0, "region");
        let file_io = crate::fs_io::build_file_io_for_location(&location, binding);

        let mut manifests = Vec::new();
        let live_path = format!("{location}/metadata/live.avro");
        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(&live_path).expect("manifest output"),
            Some(SNAPSHOT_ID),
            None,
            Arc::new(schema.clone()),
            spec.clone(),
        )
        .build_v2_data();
        writer
            .add_file(data_file("data/a.parquet", "east", 10, 1_024), 1)
            .expect("add file");
        writer
            .add_file(data_file("data/b.parquet", "west", 20, 2_048), 1)
            .expect("add file");
        let mut manifest = writer.write_manifest_file().await.expect("write manifest");
        manifest.sequence_number = 1;
        manifest.min_sequence_number = 1;
        manifests.push(manifest);

        let removed_path = format!("{location}/metadata/removed.avro");
        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(&removed_path).expect("manifest output"),
            Some(SNAPSHOT_ID),
            None,
            Arc::new(schema.clone()),
            spec.clone(),
        )
        .build_v2_data();
        writer
            .add_delete_file(data_file("data/c.parquet", "east", 5, 512), 1, Some(1))
            .expect("delete file");
        let mut manifest = writer.write_manifest_file().await.expect("write manifest");
        manifest.sequence_number = 1;
        manifest.min_sequence_number = 1;
        manifests.push(manifest);

        let manifest_list_path = format!("{location}/metadata/snap-{SNAPSHOT_ID}.avro");
        let mut list_writer = ManifestListWriter::v2(
            file_io
                .new_output(&manifest_list_path)
                .expect("manifest list output"),
            SNAPSHOT_ID,
            None,
            SNAPSHOT_SEQUENCE_NUMBER,
        );
        list_writer
            .add_manifests(manifests.into_iter())
            .expect("add manifests");
        list_writer.close().await.expect("close manifest list");

        let snapshot = Snapshot::builder()
            .with_snapshot_id(SNAPSHOT_ID)
            .with_sequence_number(SNAPSHOT_SEQUENCE_NUMBER)
            .with_timestamp_ms(1_700_000_000_000)
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from([(
                    "added-data-files".to_string(),
                    "2".to_string(),
                )]),
            })
            .with_schema_id(0)
            .build();
        let metadata = TableMetadataBuilder::new(
            schema,
            spec.into_unbound(),
            SortOrder::unsorted_order(),
            location.clone(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .add_snapshot(snapshot)
        .expect("add snapshot")
        .set_ref(
            "main",
            SnapshotReference::new(
                SNAPSHOT_ID,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: Some(2),
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        )
        .expect("set main")
        .set_ref(
            crate::commit::mv_publication_fence::MV_PUBLICATION_FENCE_REF,
            SnapshotReference::new(
                SNAPSHOT_ID,
                SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            ),
        )
        .expect("set fence ref")
        .build()
        .expect("metadata")
        .metadata;

        let metadata_location = format!("{location}/metadata/00001-frozen.metadata.json");
        std::fs::write(
            dir.join("metadata/00001-frozen.metadata.json"),
            serde_json::to_vec(&metadata).expect("metadata json"),
        )
        .expect("write metadata");

        Warehouse {
            metadata_location,
            metadata,
        }
    }

    fn reference(
        warehouse: &Warehouse,
        relation: IcebergSystemTableType,
        snapshot_id: Option<i64>,
    ) -> IcebergSystemTableReference {
        IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            system_table_type: relation,
            metadata_file_location: warehouse.metadata_location.clone(),
            table_uuid: warehouse.metadata.uuid().hyphenated().to_string(),
            snapshot_id,
        })
        .expect("reference")
    }

    fn refusal(result: Result<Vec<Vec<ArrayRef>>, ConnectorError>) -> ConnectorError {
        result.expect_err("the reader was expected to fail closed")
    }

    /// Reads a stream to its end and closes it.
    fn drain_driven(mut source: DrivenStream<'_>) -> Result<Vec<Vec<ArrayRef>>, ConnectorError> {
        let mut pages = Vec::new();
        while let Some(page) = source.next_page()? {
            let (_, columns) = page.into_columns().expect("columns");
            pages.push(columns);
        }
        assert!(source.is_finished());
        source.close()?;
        Ok(pages)
    }

    /// A single-backend relation read to its end through its page stream.
    fn single_backend_pages(
        runtime: &tokio::runtime::Runtime,
        provider: &IcebergSystemTableProvider,
        reference: &IcebergSystemTableReference,
        columns: &[IcebergColumnHandle],
    ) -> Result<Vec<Vec<ArrayRef>>, ConnectorError> {
        let budget = ConnectorPollBudget::new();
        let stream = provider.create_single_backend_page_stream(reference, columns, &budget)?;
        drain_driven(DrivenStream::new(runtime, stream, budget))
    }

    /// A `$files` split read to its end through its page stream.
    fn files_pages(
        runtime: &tokio::runtime::Runtime,
        provider: &IcebergSystemTableProvider,
        split: &FilesTableSplit,
        columns: &[IcebergColumnHandle],
    ) -> Result<Vec<Vec<ArrayRef>>, ConnectorError> {
        let budget = ConnectorPollBudget::new();
        let stream = provider.create_files_page_stream(split, columns, &budget)?;
        drain_driven(DrivenStream::new(runtime, stream, budget))
    }

    /// A budget no read in these tests runs out of.
    fn unbounded_budget() -> ConnectorPollBudget {
        let budget = ConnectorPollBudget::new();
        budget.refill(u64::MAX);
        budget
    }

    fn all_columns(schema: &SchemaRef) -> Vec<IcebergColumnHandle> {
        // Column handles carry an Iceberg field ID and an Iceberg type; a
        // system relation's columns are named metadata columns, so the identity
        // name is the part that selects them and the type is only along for the
        // ride.
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                let nested = NestedField::optional(
                    i32::try_from(ordinal + 1).expect("ordinal"),
                    field.name(),
                    Type::Primitive(PrimitiveType::Long),
                );
                IcebergColumnHandle::base_column(&nested).expect("column handle")
            })
            .collect()
    }

    fn string_column(columns: &[ArrayRef], index: usize) -> Vec<Option<String>> {
        let array = columns[index]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");
        (0..array.len())
            .map(|row| (!array.is_null(row)).then(|| array.value(row).to_string()))
            .collect()
    }

    fn long_column(columns: &[ArrayRef], index: usize) -> Vec<Option<i64>> {
        let array = columns[index]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("long column");
        (0..array.len())
            .map(|row| (!array.is_null(row)).then(|| array.value(row)))
            .collect()
    }

    fn int_column(columns: &[ArrayRef], index: usize) -> Vec<Option<i32>> {
        let array = columns[index]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int column");
        (0..array.len())
            .map(|row| (!array.is_null(row)).then(|| array.value(row)))
            .collect()
    }

    #[test]
    fn refs_excludes_provider_private_refs() {
        let (runtime, binding, context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let provider = provider(&binding, &context);
        let reference = reference(&warehouse, IcebergSystemTableType::Refs, None);
        let schema = system_relation_schema(
            IcebergSystemTableType::Refs,
            warehouse.metadata.current_schema(),
            &[],
        )
        .expect("schema");

        let pages = single_backend_pages(&runtime, &provider, &reference, &all_columns(&schema))
            .expect("pages");
        assert_eq!(pages.len(), 1);
        // `main` survives; the MV publication fence is provider bookkeeping and
        // must not appear as a table ref.
        assert_eq!(string_column(&pages[0], 0), vec![Some("main".to_string())]);
        assert_eq!(
            string_column(&pages[0], 1),
            vec![Some("BRANCH".to_string())]
        );
        assert_eq!(long_column(&pages[0], 2), vec![Some(SNAPSHOT_ID)]);
        assert_eq!(int_column(&pages[0], 4), vec![Some(2)]);
    }

    #[test]
    fn snapshots_reports_a_zoned_commit_time_and_a_summary_map() {
        let (runtime, binding, context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let provider = provider(&binding, &context);
        let reference = reference(&warehouse, IcebergSystemTableType::Snapshots, None);
        let schema = system_relation_schema(
            IcebergSystemTableType::Snapshots,
            warehouse.metadata.current_schema(),
            &[],
        )
        .expect("schema");

        let pages = single_backend_pages(&runtime, &provider, &reference, &all_columns(&schema))
            .expect("pages");
        let columns = &pages[0];

        let committed_at = columns[0]
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("zoned timestamp");
        assert_eq!(
            committed_at.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into()))
        );
        assert_eq!(committed_at.value(0), 1_700_000_000_000 * 1_000);
        assert_eq!(long_column(columns, 1), vec![Some(SNAPSHOT_ID)]);
        assert_eq!(long_column(columns, 2), vec![None]);
        assert_eq!(string_column(columns, 3), vec![Some("append".to_string())]);

        let summary = columns[5]
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("summary map");
        let entries = summary.value(0);
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("keys");
        assert_eq!(keys.value(0), "added-data-files");
    }

    #[test]
    fn history_marks_the_current_ancestor() {
        let (runtime, binding, context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let provider = provider(&binding, &context);
        let reference = reference(&warehouse, IcebergSystemTableType::History, None);
        let schema = system_relation_schema(
            IcebergSystemTableType::History,
            warehouse.metadata.current_schema(),
            &[],
        )
        .expect("schema");

        let pages = single_backend_pages(&runtime, &provider, &reference, &all_columns(&schema))
            .expect("pages");
        let columns = &pages[0];
        assert_eq!(long_column(columns, 1), vec![Some(SNAPSHOT_ID)]);
        let ancestor = columns[3]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean");
        assert!(ancestor.value(0));
    }

    #[test]
    fn manifests_reports_an_array_of_partition_summary_rows() {
        let (runtime, binding, context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let provider = provider(&binding, &context);
        let reference = reference(
            &warehouse,
            IcebergSystemTableType::Manifests,
            Some(SNAPSHOT_ID),
        );
        let schema = system_relation_schema(
            IcebergSystemTableType::Manifests,
            warehouse.metadata.current_schema(),
            &[],
        )
        .expect("schema");

        let pages = single_backend_pages(&runtime, &provider, &reference, &all_columns(&schema))
            .expect("pages");
        let columns = &pages[0];
        assert_eq!(columns[0].len(), 2);
        assert_eq!(int_column(columns, 0), vec![Some(0), Some(0)]);
        assert_eq!(
            int_column(columns, 3),
            vec![Some(0), Some(0)],
            "both manifests use the table's only partition spec"
        );

        let summaries = columns[11]
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("partition summaries list");
        let first = summaries.value(0);
        let first = first
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("summary struct");
        assert_eq!(first.num_columns(), 4);
        let lower = first
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("lower bound");
        // The bound is decoded through the partition field's own type, not
        // printed as raw bytes.
        assert_eq!(lower.value(0), "east");
    }

    #[test]
    fn files_skips_deleted_entries_while_entries_keeps_them() {
        let (runtime, binding, context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let provider = provider(&binding, &context);

        let entries_reference = reference(
            &warehouse,
            IcebergSystemTableType::Entries,
            Some(SNAPSHOT_ID),
        );
        let specs = warehouse
            .metadata
            .partition_specs_iter()
            .map(|spec| spec.as_ref().clone())
            .collect::<Vec<_>>();
        let entries_schema = system_relation_schema(
            IcebergSystemTableType::Entries,
            warehouse.metadata.current_schema(),
            &specs,
        )
        .expect("schema");
        let pages = single_backend_pages(
            &runtime,
            &provider,
            &entries_reference,
            &all_columns(&entries_schema),
        )
        .expect("pages");
        let columns = &pages[0];
        // Two live entries plus the tombstone the second manifest carries.
        assert_eq!(columns[0].len(), 3);
        assert_eq!(int_column(columns, 0), vec![Some(1), Some(1), Some(2)]);
        // Sequence numbers are inherited from the manifest list entry.
        assert_eq!(
            long_column(columns, 2),
            vec![Some(1), Some(1), Some(1)],
            "data sequence numbers"
        );

        let data_file = columns[4]
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("data_file row");
        let paths = data_file
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("file paths");
        assert_eq!(paths.value(2), "data/c.parquet");

        // `$files` over the same snapshot reports only the two live files.
        let files_rows = {
            let mut reader = FrozenMetadataReader::new(&binding, &context);
            let metadata = runtime
                .block_on(reader.load_metadata(&reference(
                    &warehouse,
                    IcebergSystemTableType::Files,
                    Some(SNAPSHOT_ID),
                )))
                .expect("metadata");
            runtime
                .block_on(read_snapshot_file_rows(
                    &mut reader,
                    &metadata,
                    &reference(&warehouse, IcebergSystemTableType::Files, Some(SNAPSHOT_ID)),
                    EntryStatusRule::SkipDeleted,
                    &unbounded_budget(),
                ))
                .expect("rows")
        };
        assert_eq!(
            files_rows
                .iter()
                .map(|row| row.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["data/a.parquet", "data/b.parquet"]
        );
    }

    #[test]
    fn files_materializes_one_manifest_split_with_typed_bounds() {
        let (runtime, binding, context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let provider = provider(&binding, &context);

        let mut reader = FrozenMetadataReader::new(&binding, &context);
        let files_reference =
            reference(&warehouse, IcebergSystemTableType::Files, Some(SNAPSHOT_ID));
        let metadata = runtime
            .block_on(reader.load_metadata(&files_reference))
            .expect("metadata");
        let manifests = runtime
            .block_on(read_snapshot_manifests(
                &mut reader,
                &files_reference,
                &metadata,
            ))
            .expect("manifests")
            .files;

        let schema = warehouse.metadata.current_schema().as_ref().clone();
        let spec = identity_spec(&schema, 0, "region");
        let split = FilesTableSplit::try_new(FilesTableSplitParams {
            manifest: manifests[0].clone(),
            table_schema_json: serde_json::to_string(&schema).expect("schema json"),
            metadata_table_schema_json: r#"{"relation":"$files"}"#.to_string(),
            partition_spec_jsons: BTreeMap::from([(
                0,
                serde_json::to_string(&spec).expect("spec json"),
            )]),
            partition_column_type_json: None,
            bounds_column_type_json: None,
            encryption_key_id: None,
        })
        .expect("split");

        let relation = system_relation_schema(IcebergSystemTableType::Files, &schema, &[spec])
            .expect("schema");
        let pages =
            files_pages(&runtime, &provider, &split, &all_columns(&relation)).expect("pages");
        let columns = &pages[0];
        assert_eq!(columns.len(), 27);
        assert_eq!(columns[0].len(), 2);
        assert_eq!(
            string_column(columns, 1),
            vec![
                Some("data/a.parquet".to_string()),
                Some("data/b.parquet".to_string())
            ]
        );
        assert_eq!(
            string_column(columns, 2),
            vec![Some("PARQUET".to_string()); 2]
        );
        assert_eq!(int_column(columns, 3), vec![Some(0), Some(0)]);

        let partition = columns[4]
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("partition row");
        let region = partition
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region");
        assert_eq!(region.value(0), "east");

        // The bound arrives as the target type, not as binary and not as text.
        let lower = columns[11]
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("lower bounds row");
        assert_eq!(lower.column(0).data_type(), &DataType::Int64);
        let id_lower = lower
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id lower bound");
        assert_eq!(id_lower.value(0), 1);

        // The reader's own address of each row.
        assert_eq!(long_column(columns, 22), vec![Some(0), Some(1)]);
        assert_eq!(
            string_column(columns, 23),
            vec![Some(manifests[0].path().to_string()); 2]
        );
        assert_eq!(
            long_column(columns, 18),
            vec![Some(SNAPSHOT_ID), Some(SNAPSHOT_ID)],
            "added snapshot id is inherited from the manifest list entry"
        );
    }

    #[test]
    fn partitions_aggregates_the_same_pinned_files() {
        let (runtime, binding, context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let provider = provider(&binding, &context);

        let partitions = reference(
            &warehouse,
            IcebergSystemTableType::Partitions,
            Some(SNAPSHOT_ID),
        );
        let specs = warehouse
            .metadata
            .partition_specs_iter()
            .map(|spec| spec.as_ref().clone())
            .collect::<Vec<_>>();
        let schema = system_relation_schema(
            IcebergSystemTableType::Partitions,
            warehouse.metadata.current_schema(),
            &specs,
        )
        .expect("schema");

        let pages = single_backend_pages(&runtime, &provider, &partitions, &all_columns(&schema))
            .expect("pages");
        let columns = &pages[0];
        assert_eq!(columns.len(), 5);
        // One partition per distinct region among the live data files.
        assert_eq!(columns[0].len(), 2);
        assert_eq!(long_column(columns, 1), vec![Some(10), Some(20)]);
        assert_eq!(long_column(columns, 2), vec![Some(1), Some(1)]);
        assert_eq!(long_column(columns, 3), vec![Some(1_024), Some(2_048)]);

        let metrics = columns[4]
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("data row");
        let id_metrics = metrics
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("id metrics");
        let min = id_metrics
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("min");
        assert_eq!(min.value(0), 1);
    }

    #[test]
    fn a_uuid_or_snapshot_mismatch_fails_closed_before_any_row() {
        let (runtime, binding, context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let provider = provider(&binding, &context);
        let schema = system_relation_schema(
            IcebergSystemTableType::Snapshots,
            warehouse.metadata.current_schema(),
            &[],
        )
        .expect("schema");
        let columns = all_columns(&schema);

        let wrong_uuid = IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            system_table_type: IcebergSystemTableType::Snapshots,
            metadata_file_location: warehouse.metadata_location.clone(),
            table_uuid: "9d1f4c1e-6a1f-4a0b-9c3a-0f2b6d5e7a11".to_string(),
            snapshot_id: None,
        })
        .expect("reference");
        assert_eq!(
            refusal(single_backend_pages(
                &runtime,
                &provider,
                &wrong_uuid,
                &columns
            ))
            .kind(),
            ConnectorErrorKind::CorruptData
        );

        let missing_snapshot =
            IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
                schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
                system_table_type: IcebergSystemTableType::Manifests,
                metadata_file_location: warehouse.metadata_location.clone(),
                table_uuid: warehouse.metadata.uuid().hyphenated().to_string(),
                snapshot_id: Some(4242),
            })
            .expect("reference");
        assert_eq!(
            refusal(single_backend_pages(
                &runtime,
                &provider,
                &missing_snapshot,
                &columns
            ))
            .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn a_non_empty_manifest_key_metadata_is_rejected() {
        let error = TrinoManifestFile::try_new(TrinoManifestFileParams {
            path: "m0.avro".to_string(),
            length: 128,
            partition_spec_id: 7,
            content: crate::typed_read::system_table::TrinoManifestContent::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: SNAPSHOT_ID,
            added_files_count: Some(1),
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: Some(1),
            existing_rows_count: None,
            deleted_rows_count: None,
            first_row_id: None,
            key_metadata: vec![0xAB, 0xCD],
        })
        .expect_err("encrypted manifest");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn a_distributed_relation_is_refused_by_the_direct_page_source() {
        let (runtime, binding, context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let provider = provider(&binding, &context);
        let reference = reference(&warehouse, IcebergSystemTableType::Files, Some(SNAPSHOT_ID));
        let error = refusal(single_backend_pages(&runtime, &provider, &reference, &[]));
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn an_unknown_projected_column_is_refused() {
        let schema = test_schema();
        let relation =
            system_relation_schema(IcebergSystemTableType::Refs, &schema, &[]).expect("schema");
        let nested = NestedField::optional(1, "not_a_column", Type::Primitive(PrimitiveType::Long));
        let handle = IcebergColumnHandle::base_column(&nested).expect("handle");
        assert_eq!(
            project_system_relation_columns(&relation, &[handle])
                .expect_err("unknown column")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        // No projected column at all is a count-only scan, not an error.
        assert!(
            project_system_relation_columns(&relation, &[])
                .expect("count-only")
                .is_empty()
        );
    }

    fn drain_stream(
        runtime: &tokio::runtime::Runtime,
        stream: &mut OwnedConnectorPageStream,
    ) -> Vec<Vec<ArrayRef>> {
        use futures::StreamExt;
        runtime.block_on(async {
            let mut pages = Vec::new();
            while let Some(page) = stream.next().await {
                let (_, columns) = page.expect("page").into_columns().expect("columns");
                pages.push(columns);
            }
            pages
        })
    }

    /// The `$files` split of the warehouse's first manifest, and every column
    /// of its relation.
    fn first_manifest_split(
        runtime: &tokio::runtime::Runtime,
        binding: &IcebergReadBinding,
        context: &FileReadContext,
        warehouse: &Warehouse,
    ) -> (FilesTableSplit, Vec<IcebergColumnHandle>) {
        let mut reader = FrozenMetadataReader::new(binding, context);
        let files_reference =
            reference(warehouse, IcebergSystemTableType::Files, Some(SNAPSHOT_ID));
        let metadata = runtime
            .block_on(reader.load_metadata(&files_reference))
            .expect("metadata");
        let manifests = runtime
            .block_on(read_snapshot_manifests(
                &mut reader,
                &files_reference,
                &metadata,
            ))
            .expect("manifests")
            .files;
        let schema = warehouse.metadata.current_schema().as_ref().clone();
        let spec = identity_spec(&schema, 0, "region");
        let split = FilesTableSplit::try_new(FilesTableSplitParams {
            manifest: manifests[0].clone(),
            table_schema_json: serde_json::to_string(&schema).expect("schema json"),
            metadata_table_schema_json: r#"{"relation":"$files"}"#.to_string(),
            partition_spec_jsons: BTreeMap::from([(
                0,
                serde_json::to_string(&spec).expect("spec json"),
            )]),
            partition_column_type_json: None,
            bounds_column_type_json: None,
            encryption_key_id: None,
        })
        .expect("split");
        let relation = system_relation_schema(IcebergSystemTableType::Files, &schema, &[spec])
            .expect("schema");
        (split, all_columns(&relation))
    }

    #[test]
    fn every_single_backend_stream_emits_what_its_page_source_emits() {
        let (runtime, binding, context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let provider = provider(&binding, &context);
        let specs = warehouse
            .metadata
            .partition_specs_iter()
            .map(|spec| spec.as_ref().clone())
            .collect::<Vec<_>>();
        for relation in [
            IcebergSystemTableType::Entries,
            IcebergSystemTableType::Partitions,
            IcebergSystemTableType::Snapshots,
            IcebergSystemTableType::History,
            IcebergSystemTableType::Refs,
            IcebergSystemTableType::Manifests,
        ] {
            let reference = reference(&warehouse, relation, Some(SNAPSHOT_ID));
            let schema =
                system_relation_schema(relation, warehouse.metadata.current_schema(), &specs)
                    .expect("schema");
            let columns = all_columns(&schema);
            // One unit for the whole relation, so every later unit of work
            // yields its turn first and the yields count the units spent.
            let budget = ConnectorPollBudget::new();
            budget.refill(1);
            let mut stream = provider
                .create_single_backend_page_stream(&reference, &columns, &budget)
                .expect("page stream");
            let streamed = drain_stream(&runtime, &mut stream);
            runtime
                .block_on(stream.close())
                .expect("closing a drained stream");

            assert!(!streamed.is_empty(), "{relation:?} has rows");
            // `$entries` and `$partitions` walk both manifests of the
            // snapshot and spend a unit on each, besides one per page.
            let walked = match relation {
                IcebergSystemTableType::Entries | IcebergSystemTableType::Partitions => 2,
                _ => 0,
            };
            let spent = walked + streamed.len() as u64;
            assert_eq!(budget.exhaustions(), spent - 1, "{relation:?}");
        }
    }

    #[test]
    fn a_files_split_stream_reads_nothing_until_polled_then_reads_its_one_manifest() {
        let (runtime, binding, mut context) = runtime_and_binding();
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
        let (split, columns) = first_manifest_split(&runtime, &binding, &context, &warehouse);
        let expected =
            files_pages(&runtime, &provider(&binding, &context), &split, &columns).expect("pages");

        let spawner = GatedTaskSpawner::closed(runtime.handle().clone());
        let task_operations =
            route_reads_through(&mut context, spawner.clone(), runtime.handle().clone());
        let budget = ConnectorPollBudget::new();
        let mut stream = provider(&binding, &context)
            .create_files_page_stream(&split, &columns, &budget)
            .expect("page stream");
        assert_eq!(spawner.started(), 0, "creating the stream reads nothing");
        spawner.release(64);
        budget.refill(1024);
        let streamed = drain_stream(&runtime, &mut stream);
        assert_eq!(spawner.started(), 1, "one read of the split's manifest");
        assert_eq!(streamed, expected);
        runtime
            .block_on(stream.close())
            .expect("closing a drained stream");
        assert_eq!(task_operations.live_operations(), 0);
    }
}
