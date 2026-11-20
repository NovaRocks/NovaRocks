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

//! Physical binding of the scan's ordered columns onto one data file.
//!
//! Binding answers exactly one question per output column: where does this
//! column's data come from? The answer is decided once, against the immutable
//! file schema and the frozen table schema, and is then replayed unchanged for
//! every batch of the split.
//!
//! The resolution order is fixed and total:
//!
//! 1. a physical field of the file schema carrying the same Iceberg field ID;
//! 2. an identity partition constant frozen on the split;
//! 3. a legacy name mapping, and only when the file schema carries no field ID
//!    at all -- a partially identified file is corrupt, not an invitation to
//!    guess;
//! 4. the frozen table schema's initial default, applied recursively into
//!    struct fields, array elements, and map values;
//! 5. a typed null, and only for a nullable field.
//!
//! A required field that reaches the end of that list is `ICEBERG_BAD_DATA`.
//! There is no name fallback, no ordinal fallback, and no implicit widening
//! outside the promotions Iceberg actually defines.

pub(crate) mod mutation_partition;
pub(crate) use mutation_partition::MutationMetadataSchema;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StructArray, UInt64Array, make_array};
use arrow::buffer::NullBuffer;
use arrow::compute::{CastOptions, cast, cast_with_options};
use arrow::datatypes::{DataType, Field, FieldRef, Schema as ArrowSchema, SchemaRef, TimeUnit};
use novarocks_spi::connector::ConnectorError;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::default_value::{
    ICEBERG_INITIAL_DEFAULT_META_KEY, build_iceberg_default_array, literal_to_constant_array,
};
use crate::file_reader::variant::collapse_variant_struct_to_largebinary;
use crate::iceberg::spec::{
    Literal, NameMapping, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Transform,
    Type,
};
use crate::row_lineage_synth::{
    ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL,
    ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, ICEBERG_RESERVED_FIELD_ID_ROW_ID,
    ICEBERG_ROW_ID_COL, ICEBERG_ROW_POS_COL, IcebergRowLineageColumn, StoredRowLineageIndices,
    stored_row_lineage_indices, synthesize_last_updated_sequence_number, synthesize_row_id,
};
use crate::schema_mapping::{
    apply_name_mapping_to_schema, field_id_for_arrow_field, schema_field_id_coverage,
    sql_read_schema_from_iceberg, unidentified_fields_are_only_opaque_variants,
};

use super::column_handle::{IcebergColumnHandle, corrupt, invalid, parse_type, unsupported};

/// `_file`: the absolute location of the data file a row came from.
pub const ICEBERG_METADATA_FIELD_ID_PATH: i32 = i32::MAX - 1;
/// `_pos`: the row's file-level absolute zero-based position.
pub const ICEBERG_METADATA_FIELD_ID_ROW_POSITION: i32 = i32::MAX - 2;
/// `_deleted`: whether a delete matched the row. Internal only.
pub const ICEBERG_METADATA_FIELD_ID_IS_DELETED: i32 = i32::MAX - 3;

/// A column that is not a field of the table schema.
///
/// The names are the engine's own: `_file`, `_pos`, `_row_id` and
/// `_last_updated_sequence_number` are what a query writes, what the frontend
/// asks a relation to bind, and therefore what the boundary must publish. This
/// enum is the single place they are spelled, so the identity a scan is planned
/// with and the identity a reader answers can never drift apart.
///
/// Every variant here is one this page source can actually produce. `_deleted`
/// is the exception and is deliberately not externally visible: only a read
/// that keeps deleted rows can answer it, so it exists solely for a delete
/// filter to name what it needs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergMetadataColumn {
    Path,
    RowPosition,
    RowId,
    LastUpdatedSequenceNumber,
    IsDeleted,
}

/// The metadata columns every data relation exposes, in publication order.
///
/// `_file` and `_pos` are facts of the split and of the row's position in its
/// file, so they exist for every Iceberg table regardless of format version.
pub const ALWAYS_BOUND_METADATA_COLUMNS: [IcebergMetadataColumn; 2] = [
    IcebergMetadataColumn::Path,
    IcebergMetadataColumn::RowPosition,
];

/// The metadata columns a row-lineage-bearing relation adds, in publication
/// order.
///
/// They are absent from a table without row lineage rather than bound and
/// empty. A v3 file without `first_row_id` produces NULL for these columns;
/// otherwise `_row_id` inherits `first_row_id + position`.
pub const ROW_LINEAGE_METADATA_COLUMNS: [IcebergMetadataColumn; 2] = [
    IcebergMetadataColumn::RowId,
    IcebergMetadataColumn::LastUpdatedSequenceNumber,
];

impl IcebergMetadataColumn {
    pub const fn field_id(self) -> i32 {
        match self {
            Self::Path => ICEBERG_METADATA_FIELD_ID_PATH,
            Self::RowPosition => ICEBERG_METADATA_FIELD_ID_ROW_POSITION,
            Self::RowId => ICEBERG_RESERVED_FIELD_ID_ROW_ID,
            Self::LastUpdatedSequenceNumber => {
                ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
            }
            Self::IsDeleted => ICEBERG_METADATA_FIELD_ID_IS_DELETED,
        }
    }

    pub const fn column_name(self) -> &'static str {
        match self {
            Self::Path => ICEBERG_FILE_PATH_COL,
            Self::RowPosition => ICEBERG_ROW_POS_COL,
            Self::RowId => ICEBERG_ROW_ID_COL,
            Self::LastUpdatedSequenceNumber => ICEBERG_LAST_UPDATED_SEQ_COL,
            Self::IsDeleted => "_deleted",
        }
    }

    /// The Iceberg type a handle for this column must declare.
    ///
    /// Both ends read it from here: the boundary mints the published handle
    /// with it and the binder checks the handle against it, so a declared type
    /// cannot disagree with the array the reader builds.
    pub const fn declared_type(self) -> PrimitiveType {
        match self {
            Self::Path => PrimitiveType::String,
            Self::RowPosition | Self::RowId | Self::LastUpdatedSequenceNumber => {
                PrimitiveType::Long
            }
            Self::IsDeleted => PrimitiveType::Boolean,
        }
    }

    /// Whether this column may be NULL.
    ///
    /// Like [`Self::declared_type`], every statement of the column reads it
    /// from here: the table metadata states it to SQL and the read binding
    /// mints the published handle with it, so the type a scan is planned with
    /// and the type its frozen read publishes are one exact type. The reader
    /// never yields NULL for a required column: `_file` and `_pos` are facts of
    /// every row's split and position. Both lineage columns may be NULL for
    /// files that predate the first v3 row-ID allocation.
    pub const fn nullable(self) -> bool {
        match self {
            Self::Path | Self::RowPosition | Self::IsDeleted => false,
            Self::RowId | Self::LastUpdatedSequenceNumber => true,
        }
    }

    /// Whether a scan may name this column in its declared output.
    pub const fn is_externally_visible(self) -> bool {
        match self {
            Self::Path | Self::RowPosition | Self::RowId | Self::LastUpdatedSequenceNumber => true,
            Self::IsDeleted => false,
        }
    }

    /// The row-lineage column this one is, when it is one at all.
    ///
    /// Only these two may be materialized in a data file under a reserved
    /// field ID; `_file`, `_pos` and `_deleted` have no physical carrier and
    /// can only ever be synthesized.
    pub const fn row_lineage_column(self) -> Option<IcebergRowLineageColumn> {
        match self {
            Self::RowId => Some(IcebergRowLineageColumn::RowId),
            Self::LastUpdatedSequenceNumber => {
                Some(IcebergRowLineageColumn::LastUpdatedSequenceNumber)
            }
            Self::Path | Self::RowPosition | Self::IsDeleted => None,
        }
    }

    pub const fn from_field_id(field_id: i32) -> Option<Self> {
        // Written as a chain rather than a `match` because the arms are
        // associated constants, which patterns cannot name.
        if field_id == ICEBERG_METADATA_FIELD_ID_PATH {
            Some(Self::Path)
        } else if field_id == ICEBERG_METADATA_FIELD_ID_ROW_POSITION {
            Some(Self::RowPosition)
        } else if field_id == ICEBERG_RESERVED_FIELD_ID_ROW_ID {
            Some(Self::RowId)
        } else if field_id == ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER {
            Some(Self::LastUpdatedSequenceNumber)
        } else if field_id == ICEBERG_METADATA_FIELD_ID_IS_DELETED {
            Some(Self::IsDeleted)
        } else {
            None
        }
    }
}

/// The one Iceberg-defined promotion a same-field-ID pair may need.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergTypePromotion {
    IntToLong,
    FloatToDouble,
    DecimalPrecisionWidening,
    DateToTimestamp,
    DateToTimestampNs,
    UnknownToAny,
}

/// How a physical column becomes its target column.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergPhysicalAdaptation {
    /// The physical carrier already is the target carrier.
    Identity,
    /// Only nested Arrow field metadata differs; re-tag without touching data.
    RetagNestedMetadata,
    /// A physical VARIANT struct collapses into its opaque binary carrier.
    CollapseVariant,
    /// A carrier change that preserves the Iceberg logical type exactly, such
    /// as a dictionary-encoded page or a UTC time zone annotation.
    RepresentationCast,
    /// A promotion Iceberg defines for schema evolution.
    Promotion(IcebergTypePromotion),
}

/// Where one bound output column's data comes from.
#[derive(Clone, Debug)]
pub enum IcebergColumnSource {
    /// A physical field of the file, addressed by the base column's field ID
    /// and then dereferenced through struct children by field ID.
    Physical {
        base_field_id: i32,
        dereference: Vec<i32>,
    },
    /// An identity partition value frozen on the split. `None` is a real null
    /// partition value, not a missing fact.
    IdentityPartitionConstant(Option<Literal>),
    MutationSpecId(i32),
    MutationPartition(Vec<Option<Literal>>),
    /// The frozen table schema's initial default for this field.
    InitialDefault,
    /// A typed null, legal only for a nullable field.
    TypedNull,
    /// A column that is not a field of the table schema.
    Metadata(IcebergMetadataColumn),
    /// A row-lineage column this data file materializes under its reserved
    /// Iceberg field ID.
    ///
    /// Whether a file stores `_row_id` or `_last_updated_sequence_number` is a
    /// per-file fact -- a freshly written file inherits both, a rewritten one
    /// carries forward the lineage its rows already had -- so this is decided
    /// against the footer, once per split. The variant exists to make the
    /// reader project the reserved column: the values themselves are resolved
    /// by the same inherit-vs-stored rule as [`Self::Metadata`], which simply
    /// finds nothing stored when the file materialized nothing.
    StoredRowLineage(IcebergRowLineageColumn),
}

/// One output column and the decision that produces it.
#[derive(Clone, Debug)]
pub struct IcebergBoundColumn {
    handle: IcebergColumnHandle,
    /// The Arrow field the reader must produce, carrying the Iceberg field ID
    /// and, when the frozen schema declares one, the initial default.
    target: FieldRef,
    /// The Arrow field of the base column, which a dereference walks into.
    base_target: FieldRef,
    source: IcebergColumnSource,
}

impl IcebergBoundColumn {
    pub const fn handle(&self) -> &IcebergColumnHandle {
        &self.handle
    }

    pub const fn target(&self) -> &FieldRef {
        &self.target
    }

    pub const fn source(&self) -> &IcebergColumnSource {
        &self.source
    }
}

/// The frozen split facts a metadata column is built from.
///
/// Every field is a planning or physical fact. Nothing here is defaulted: a
/// metadata column whose fact is absent fails closed instead of reading as
/// null.
#[derive(Clone, Copy, Debug)]
pub struct IcebergSplitFacts<'a> {
    pub path: &'a str,
    pub file_first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}

/// Whether the file schema identifies its fields at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileFieldIdCoverage {
    /// Every field carries an Iceberg field ID.
    Complete,
    /// No field carries one; a legacy name mapping is the only way in.
    None,
}

/// The complete, replayable binding of one scan's ordered columns.
#[derive(Clone, Debug)]
pub struct IcebergSchemaBinding {
    columns: Vec<IcebergBoundColumn>,
    physical_base_field_ids: Vec<i32>,
    coverage: FileFieldIdCoverage,
    name_mapping: Option<Arc<NameMapping>>,
}

impl IcebergSchemaBinding {
    pub fn columns(&self) -> &[IcebergBoundColumn] {
        &self.columns
    }

    /// The base-column field IDs this binding reads, ascending and deduped.
    /// This is exactly the projection the physical reader must open.
    pub fn physical_base_field_ids(&self) -> &[i32] {
        &self.physical_base_field_ids
    }

    pub const fn coverage(&self) -> FileFieldIdCoverage {
        self.coverage
    }

    /// Whether any bound column reads a physical field at all.
    pub fn reads_any_physical_field(&self) -> bool {
        !self.physical_base_field_ids.is_empty()
    }

    /// Whether any bound column needs file-level absolute row positions.
    ///
    /// A stored `_row_id` still needs them: any row the file left null falls
    /// back to `first_row_id + position`, and which rows those are is not
    /// known until the batch is read.
    pub fn requires_row_positions(&self) -> bool {
        self.columns.iter().any(|column| {
            matches!(
                column.source,
                IcebergColumnSource::Metadata(
                    IcebergMetadataColumn::RowId | IcebergMetadataColumn::RowPosition
                ) | IcebergColumnSource::StoredRowLineage(IcebergRowLineageColumn::RowId)
            )
        })
    }

    /// Best-effort retained size of the binding itself.
    pub fn retained_size_in_bytes(&self) -> u64 {
        let mut retained =
            size_of::<Self>() + self.physical_base_field_ids.len() * size_of::<i32>();
        for column in &self.columns {
            retained += size_of::<IcebergBoundColumn>()
                + column.handle.base_type_json().len()
                + column.handle.type_json().len()
                + size_of_val(column.handle.field_id_path());
        }
        retained as u64
    }

    /// Produce every bound column for one physical batch, in scan order.
    ///
    /// `absolute_positions` are file-level, zero-based, and must line up with
    /// `batch` row for row. They are required only by the columns that consume
    /// them; a scan that names none of those may pass `None`.
    pub fn materialize(
        &self,
        batch: &RecordBatch,
        absolute_positions: Option<&UInt64Array>,
        facts: &IcebergSplitFacts<'_>,
    ) -> Result<Vec<ArrayRef>, ConnectorError> {
        let row_count = batch.num_rows();
        if let Some(positions) = absolute_positions
            && positions.len() != row_count
        {
            return Err(corrupt(format!(
                "iceberg data file {} produced {} absolute row positions for {row_count} rows",
                facts.path,
                positions.len()
            )));
        }
        // The physical schema is re-indexed per batch because a reader may
        // legally reorder or narrow its output; the binding decisions
        // themselves never change.
        let physical = PhysicalIndex::build(&batch.schema(), self.name_mapping.as_deref())?;

        let mut columns = Vec::with_capacity(self.columns.len());
        for bound in &self.columns {
            columns.push(match &bound.source {
                IcebergColumnSource::Physical {
                    base_field_id,
                    dereference,
                } => {
                    let index = physical.index_of(*base_field_id).ok_or_else(|| {
                        corrupt(format!(
                            "iceberg data file {} no longer exposes field id {base_field_id}",
                            facts.path
                        ))
                    })?;
                    let base = adapt_array(
                        batch.column(index),
                        bound.base_target.as_ref(),
                        facts.path,
                        matches!(
                            super::column_handle::parse_type(
                                bound.handle.base_type_json(),
                                "base_type_json"
                            )?,
                            Type::Variant(_)
                        ),
                    )?;
                    let value =
                        dereference_struct_path(&base, bound.base_target.as_ref(), dereference)?;
                    match bound.handle.scalar_integer_domain() {
                        Some(domain) => domain.array(&value)?,
                        None => value,
                    }
                }
                IcebergColumnSource::IdentityPartitionConstant(value) => {
                    partition_constant(value.as_ref(), bound.target.as_ref(), row_count)?
                }
                IcebergColumnSource::MutationSpecId(spec_id) => {
                    Arc::new(arrow::array::Int32Array::from(vec![*spec_id; row_count]))
                }
                IcebergColumnSource::MutationPartition(values) => {
                    let DataType::Struct(fields) = bound.target.data_type() else {
                        return Err(corrupt(
                            "Iceberg mutation partition lost its Struct carrier",
                        ));
                    };
                    mutation_partition::materialize(values, fields, row_count)?
                }
                IcebergColumnSource::InitialDefault => {
                    build_iceberg_default_array(bound.target.as_ref(), row_count)
                        .map_err(|error| corrupt(format!("iceberg initial default: {error}")))?
                }
                IcebergColumnSource::TypedNull => {
                    arrow::array::new_null_array(bound.target.data_type(), row_count)
                }
                IcebergColumnSource::Metadata(metadata) => metadata_column(
                    *metadata,
                    bound.target.as_ref(),
                    batch,
                    absolute_positions,
                    facts,
                )?,
                // A stored row-lineage column is resolved by the same rule as
                // a synthesized one. The binding already made the only choice
                // that differs -- projecting the reserved column out of the
                // file -- so the rule simply finds the stored values here.
                IcebergColumnSource::StoredRowLineage(column) => row_lineage_column(
                    *column,
                    bound.target.as_ref(),
                    batch,
                    absolute_positions,
                    facts,
                )?,
            });
        }
        Ok(columns)
    }
}

/// Everything binding needs, and nothing a worker would have to resolve.
pub struct IcebergSchemaBindingRequest<'a> {
    /// The frozen table schema the scan was planned against.
    pub table_schema: &'a Schema,
    /// The immutable Arrow schema of the data file's footer.
    pub file_schema: &'a SchemaRef,
    /// The table's legacy name mapping, when it declares one.
    pub name_mapping: Option<Arc<NameMapping>>,
    /// The partition spec the data file was written under.
    pub partition_spec: Option<&'a PartitionSpec>,
    /// The file's frozen partition values, in partition-spec field order.
    pub partition_values: Option<&'a Struct>,
    /// The scan's ordered output columns.
    pub columns: &'a [IcebergColumnHandle],
}

/// Decide where every ordered output column comes from.
pub fn bind_scan_columns(
    request: IcebergSchemaBindingRequest<'_>,
) -> Result<IcebergSchemaBinding, ConnectorError> {
    let coverage = file_field_id_coverage(request.file_schema)?;
    // Name mapping repairs a legacy file, so it is consulted only when the
    // file identifies nothing. Applying it to a partly identified file would
    // be a rename, which Iceberg's name mapping is explicitly not.
    let name_mapping = match (coverage, request.name_mapping) {
        (FileFieldIdCoverage::None, Some(mapping)) => Some(mapping),
        (FileFieldIdCoverage::None, None) | (FileFieldIdCoverage::Complete, _) => None,
    };
    let physical = PhysicalIndex::build(request.file_schema, None)?;
    let mapped_physical = match name_mapping.as_deref() {
        Some(mapping) => Some(PhysicalIndex::build(request.file_schema, Some(mapping))?),
        None => None,
    };

    let read_schema = annotated_read_schema(request.table_schema)?;
    let identity_partitions = identity_partition_values(
        request.partition_spec,
        request.partition_values,
        request.table_schema,
    )?;
    // Which row-lineage columns this one file materializes. A name-mapped
    // legacy file identifies nothing, and a file that predates row lineage
    // materializes nothing, so both correctly answer "neither".
    let stored_row_lineage = stored_row_lineage_indices(request.file_schema);

    let mut columns = Vec::with_capacity(request.columns.len());
    let mut physical_base_field_ids = Vec::new();
    for handle in request.columns {
        let bound = if mutation_partition::is_file_metadata(handle.base_field_id()) {
            let spec = request
                .partition_spec
                .ok_or_else(|| corrupt("Iceberg mutation metadata has no bound file spec"))?;
            let values = request
                .partition_values
                .ok_or_else(|| corrupt("Iceberg mutation metadata has no bound file partition"))?;
            let (target, source) = mutation_partition::bind(handle, spec, values)?;
            IcebergBoundColumn {
                handle: handle.clone(),
                base_target: Arc::clone(&target),
                target,
                source,
            }
        } else {
            bind_one_column(
                handle,
                &read_schema,
                &physical,
                mapped_physical.as_ref(),
                &identity_partitions,
                stored_row_lineage,
            )?
        };
        let projected = match &bound.source {
            IcebergColumnSource::Physical { base_field_id, .. } => Some(*base_field_id),
            IcebergColumnSource::StoredRowLineage(column) => Some(column.field_id()),
            IcebergColumnSource::IdentityPartitionConstant(_)
            | IcebergColumnSource::MutationSpecId(_)
            | IcebergColumnSource::MutationPartition(_)
            | IcebergColumnSource::InitialDefault
            | IcebergColumnSource::TypedNull
            | IcebergColumnSource::Metadata(_) => None,
        };
        if let Some(field_id) = projected
            && !physical_base_field_ids.contains(&field_id)
        {
            physical_base_field_ids.push(field_id);
        }
        columns.push(bound);
    }
    physical_base_field_ids.sort_unstable();

    Ok(IcebergSchemaBinding {
        columns,
        physical_base_field_ids,
        coverage,
        name_mapping,
    })
}

/// Classify the file schema's field-ID coverage, or reject a partial one.
///
/// A file that identifies some of its fields and not others cannot be repaired:
/// name mapping would rename the identified half, and field-ID matching would
/// silently drop the rest. The single exception is an opaque VARIANT child,
/// whose encoding sub-fields are deliberately unidentified.
pub fn file_field_id_coverage(
    file_schema: &SchemaRef,
) -> Result<FileFieldIdCoverage, ConnectorError> {
    let (identified, total) = schema_field_id_coverage(file_schema)
        .map_err(|error| corrupt(format!("iceberg data file schema: {error}")))?;
    if identified == total {
        return Ok(FileFieldIdCoverage::Complete);
    }
    if identified == 0 {
        return Ok(FileFieldIdCoverage::None);
    }
    if unidentified_fields_are_only_opaque_variants(file_schema)
        .map_err(|error| corrupt(format!("iceberg data file schema: {error}")))?
    {
        return Ok(FileFieldIdCoverage::Complete);
    }
    Err(corrupt(
        "iceberg data file mixes fields with and without field ids",
    ))
}

/// Decide whether a physical carrier may become a target carrier at all.
///
/// The accepted set is closed: exact carriers, nested-metadata-only
/// differences, the opaque VARIANT collapse, representation changes that
/// preserve the Iceberg logical type, and the promotions Iceberg defines.
/// Anything else is a same-field-ID mismatch and fails closed.
pub fn physical_adaptation(
    source: &DataType,
    target: &DataType,
    logical_variant: bool,
) -> Result<IcebergPhysicalAdaptation, ConnectorError> {
    if source == target {
        return Ok(IcebergPhysicalAdaptation::Identity);
    }
    // An unknown column carries no values at all, so it promotes to anything.
    if matches!(source, DataType::Null) {
        return Ok(IcebergPhysicalAdaptation::Promotion(
            IcebergTypePromotion::UnknownToAny,
        ));
    }
    if logical_variant
        && matches!(target, DataType::LargeBinary)
        && matches!(source, DataType::Struct(_))
    {
        return Ok(IcebergPhysicalAdaptation::CollapseVariant);
    }
    // A dictionary page is an encoding of its value type, never a distinct
    // Iceberg type, so it is unwrapped before any further judgement.
    if let DataType::Dictionary(_, values) = source {
        return match physical_adaptation(values.as_ref(), target, logical_variant)? {
            IcebergPhysicalAdaptation::Identity => {
                Ok(IcebergPhysicalAdaptation::RepresentationCast)
            }
            other => Ok(other),
        };
    }
    if let Some(promotion) = iceberg_promotion(source, target) {
        return Ok(IcebergPhysicalAdaptation::Promotion(promotion));
    }
    if representation_equivalent(source, target) {
        return Ok(IcebergPhysicalAdaptation::RepresentationCast);
    }
    if differ_only_by_nested_field_metadata(source, target) {
        return Ok(IcebergPhysicalAdaptation::RetagNestedMetadata);
    }
    if nested_representation_equivalent(source, target) {
        return Ok(IcebergPhysicalAdaptation::RepresentationCast);
    }
    Err(corrupt(format!(
        "iceberg physical field carries {source:?} where the table schema requires {target:?}"
    )))
}

fn iceberg_promotion(source: &DataType, target: &DataType) -> Option<IcebergTypePromotion> {
    match (source, target) {
        (DataType::Int32, DataType::Int64) => Some(IcebergTypePromotion::IntToLong),
        (DataType::Float32, DataType::Float64) => Some(IcebergTypePromotion::FloatToDouble),
        (
            DataType::Decimal128(source_precision, source_scale),
            DataType::Decimal128(precision, scale),
        ) if source_scale == scale && source_precision <= precision => {
            Some(IcebergTypePromotion::DecimalPrecisionWidening)
        }
        (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            Some(IcebergTypePromotion::DateToTimestamp)
        }
        (DataType::Date32, DataType::Timestamp(TimeUnit::Nanosecond, _)) => {
            Some(IcebergTypePromotion::DateToTimestampNs)
        }
        _ => None,
    }
}

/// Carrier pairs that spell the same Iceberg logical type.
///
/// These are not promotions: no value changes, only its Arrow representation.
/// The time-zone arm exists because Iceberg `timestamptz` is stored as UTC
/// instants while the read carrier deliberately drops the annotation.
fn representation_equivalent(source: &DataType, target: &DataType) -> bool {
    matches!(
        (source, target),
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) | (
            DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_),
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
        )
    ) || matches!(
        (source, target),
        (DataType::Timestamp(source_unit, _), DataType::Timestamp(target_unit, _))
            if source_unit == target_unit
    )
}

/// Whether two carriers agree except for Arrow field metadata inside a nested
/// type. Iceberg field IDs travel in that metadata, so a re-tag is a relabel.
fn differ_only_by_nested_field_metadata(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::List(source), DataType::List(target))
        | (DataType::LargeList(source), DataType::LargeList(target)) => {
            differ_only_by_nested_field_metadata(source.data_type(), target.data_type())
        }
        (
            DataType::FixedSizeList(source, source_size),
            DataType::FixedSizeList(target, target_size),
        ) => {
            source_size == target_size
                && differ_only_by_nested_field_metadata(source.data_type(), target.data_type())
        }
        (DataType::Map(source, source_sorted), DataType::Map(target, target_sorted)) => {
            source_sorted == target_sorted
                && differ_only_by_nested_field_metadata(source.data_type(), target.data_type())
        }
        (DataType::Struct(source), DataType::Struct(target)) => {
            source.len() == target.len()
                && source.iter().zip(target.iter()).all(|(source, target)| {
                    differ_only_by_nested_field_metadata(source.data_type(), target.data_type())
                })
        }
        _ => source == target,
    }
}

/// Recursively admit only the same closed leaf representations already legal
/// at the root. Container shape/order stays fixed; schema evolution and field
/// reordering remain separate binding decisions.
fn nested_representation_equivalent(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::List(source), DataType::List(target))
        | (DataType::LargeList(source), DataType::LargeList(target)) => {
            nested_representation_equivalent(source.data_type(), target.data_type())
        }
        (DataType::FixedSizeList(source, n), DataType::FixedSizeList(target, m)) => {
            n == m && nested_representation_equivalent(source.data_type(), target.data_type())
        }
        (DataType::Map(source, s), DataType::Map(target, t)) => {
            s == t && nested_representation_equivalent(source.data_type(), target.data_type())
        }
        (DataType::Struct(source), DataType::Struct(target)) => {
            source.len() == target.len()
                && source.iter().zip(target).all(|(source, target)| {
                    nested_representation_equivalent(source.data_type(), target.data_type())
                })
        }
        (DataType::Dictionary(_, values), target) => {
            nested_representation_equivalent(values, target)
        }
        _ => source == target || representation_equivalent(source, target),
    }
}

// ---------------------------------------------------------------------------
// Binding internals
// ---------------------------------------------------------------------------

/// Top-level Iceberg field ID to physical column index of one file schema.
struct PhysicalIndex {
    by_field_id: HashMap<i32, usize>,
}

impl PhysicalIndex {
    fn build(
        schema: &SchemaRef,
        name_mapping: Option<&NameMapping>,
    ) -> Result<Self, ConnectorError> {
        let schema = match name_mapping {
            None => Arc::clone(schema),
            Some(mapping) => apply_name_mapping_to_schema(schema, mapping)
                .map_err(|error| corrupt(format!("iceberg name mapping: {error}")))?,
        };
        let mut by_field_id = HashMap::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            let Some(field_id) = field_id_for_arrow_field(field.as_ref())
                .map_err(|error| corrupt(format!("iceberg data file schema: {error}")))?
            else {
                continue;
            };
            // Two physical columns claiming one field ID make every binding
            // ambiguous; there is no rule that picks a winner.
            if by_field_id.insert(field_id, index).is_some() {
                return Err(corrupt(format!(
                    "iceberg data file schema declares field id {field_id} twice"
                )));
            }
        }
        Ok(Self { by_field_id })
    }

    fn index_of(&self, field_id: i32) -> Option<usize> {
        self.by_field_id.get(&field_id).copied()
    }
}

fn bind_one_column(
    handle: &IcebergColumnHandle,
    read_schema: &SchemaRef,
    physical: &PhysicalIndex,
    mapped_physical: Option<&PhysicalIndex>,
    identity_partitions: &HashMap<i32, Option<Literal>>,
    stored_row_lineage: StoredRowLineageIndices,
) -> Result<IcebergBoundColumn, ConnectorError> {
    let base_field_id = handle.base_field_id();

    if let Some(metadata) = IcebergMetadataColumn::from_field_id(base_field_id) {
        return bind_metadata_column(handle, metadata, stored_row_lineage);
    }

    let base_target = read_schema
        .fields()
        .iter()
        .find(|field| {
            field_id_for_arrow_field(field.as_ref()).ok().flatten() == Some(base_field_id)
        })
        .cloned()
        .ok_or_else(|| {
            corrupt(format!(
                "iceberg field id {base_field_id} is not a top-level field of the frozen table schema"
            ))
        })?;
    let target = dereference_target_field(&base_target, handle.field_id_path())?;
    let target = match handle.scalar_integer_domain() {
        Some(domain) => Arc::new(target.as_ref().clone().with_data_type(domain.data_type())),
        None => target,
    };

    // 1. a physical field with the same field id in the file schema.
    if let Some(index) = physical.index_of(base_field_id) {
        let _ = index;
        return Ok(IcebergBoundColumn {
            handle: handle.clone(),
            target,
            base_target,
            source: IcebergColumnSource::Physical {
                base_field_id,
                dereference: handle.field_id_path().to_vec(),
            },
        });
    }

    // 2. an identity partition constant.
    if handle.is_base_column()
        && let Some(value) = identity_partitions.get(&base_field_id)
    {
        return Ok(IcebergBoundColumn {
            handle: handle.clone(),
            target,
            base_target,
            source: IcebergColumnSource::IdentityPartitionConstant(value.clone()),
        });
    }

    // 3. a legacy name mapping, only for a file that identifies nothing.
    if let Some(mapped) = mapped_physical
        && mapped.index_of(base_field_id).is_some()
    {
        return Ok(IcebergBoundColumn {
            handle: handle.clone(),
            target,
            base_target,
            source: IcebergColumnSource::Physical {
                base_field_id,
                dereference: handle.field_id_path().to_vec(),
            },
        });
    }

    // 4. the frozen table schema's initial default.
    if target
        .metadata()
        .contains_key(ICEBERG_INITIAL_DEFAULT_META_KEY)
    {
        return Ok(IcebergBoundColumn {
            handle: handle.clone(),
            target,
            base_target,
            source: IcebergColumnSource::InitialDefault,
        });
    }

    // 5. a typed null, for a nullable field only.
    if handle.nullable() {
        return Ok(IcebergBoundColumn {
            handle: handle.clone(),
            target,
            base_target,
            source: IcebergColumnSource::TypedNull,
        });
    }

    Err(corrupt(format!(
        "iceberg data file is missing required field id {base_field_id} and the table schema declares no initial default for it"
    )))
}

pub(crate) fn metadata_target_field(
    handle: &IcebergColumnHandle,
    metadata: IcebergMetadataColumn,
) -> Result<FieldRef, ConnectorError> {
    if handle.base_column_identity().name() != metadata.column_name() {
        return Err(corrupt(format!(
            "iceberg metadata field id {} is named {} rather than {}",
            metadata.field_id(),
            handle.base_column_identity().name(),
            metadata.column_name()
        )));
    }
    if !handle.is_base_column() {
        return Err(invalid(
            "an iceberg metadata column has no dereferenceable fields",
        ));
    }
    let declared = parse_type(handle.type_json(), "type_json")?;
    expect_primitive(&declared, metadata.declared_type(), metadata)?;
    let data_type = match metadata {
        IcebergMetadataColumn::Path => DataType::Utf8,
        IcebergMetadataColumn::RowPosition
        | IcebergMetadataColumn::RowId
        | IcebergMetadataColumn::LastUpdatedSequenceNumber => DataType::Int64,
        // `_deleted` only means something to a reader that keeps deleted rows.
        // This page source excludes them, so the column would be a constant
        // false dressed up as an answer.
        IcebergMetadataColumn::IsDeleted => {
            return Err(unsupported(
                "iceberg _deleted is only produced by an equality-match read",
            ));
        }
    };
    let field = Arc::new(
        Field::new(metadata.column_name(), data_type, handle.nullable()).with_metadata(
            [(
                PARQUET_FIELD_ID_META_KEY.to_owned(),
                metadata.field_id().to_string(),
            )]
            .into_iter()
            .collect::<std::collections::HashMap<String, String>>(),
        ),
    );
    Ok(field)
}

fn bind_metadata_column(
    handle: &IcebergColumnHandle,
    metadata: IcebergMetadataColumn,
    stored_row_lineage: StoredRowLineageIndices,
) -> Result<IcebergBoundColumn, ConnectorError> {
    let field = metadata_target_field(handle, metadata)?;
    // A rewritten v3 file materializes the row-lineage columns it carries
    // forward. Reading them is what keeps a row's history from being rewritten
    // into the rewriting snapshot's own sequence; only a file that stores
    // nothing is fully synthesized.
    let source = match metadata.row_lineage_column() {
        Some(column) if stored_row_lineage.contains(column) => {
            IcebergColumnSource::StoredRowLineage(column)
        }
        Some(_) | None => IcebergColumnSource::Metadata(metadata),
    };
    Ok(IcebergBoundColumn {
        handle: handle.clone(),
        target: Arc::clone(&field),
        base_target: field,
        source,
    })
}

fn expect_primitive(
    declared: &Type,
    expected: PrimitiveType,
    metadata: IcebergMetadataColumn,
) -> Result<(), ConnectorError> {
    match declared {
        Type::Primitive(primitive) if *primitive == expected => Ok(()),
        Type::Primitive(_) | Type::Variant(_) | Type::Struct(_) | Type::List(_) | Type::Map(_) => {
            Err(invalid(format!(
                "iceberg metadata column {} must be declared as {expected}",
                metadata.column_name()
            )))
        }
    }
}

/// The read carrier of the frozen table schema, annotated with initial defaults.
///
/// iceberg-rust's Arrow conversion drops initial defaults, so a file written
/// before `ADD COLUMN ... DEFAULT` would otherwise read back as null.
pub(crate) fn annotated_read_schema(table_schema: &Schema) -> Result<SchemaRef, ConnectorError> {
    let read_schema = sql_read_schema_from_iceberg(table_schema)
        .map_err(|error| invalid(format!("iceberg frozen table schema: {error}")))?;
    let frozen = table_schema.as_struct().fields();
    if read_schema.fields().len() != frozen.len() {
        return Err(invalid(
            "iceberg frozen table schema does not match its read carrier",
        ));
    }
    let fields = read_schema
        .fields()
        .iter()
        .zip(frozen)
        .map(|(field, frozen)| annotate_field(field.as_ref(), frozen.as_ref()).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields,
        read_schema.metadata().clone(),
    )))
}

fn annotate_field(field: &Field, frozen: &NestedField) -> Result<Field, ConnectorError> {
    let mut metadata = field.metadata().clone();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_owned(), frozen.id.to_string());
    if let Some(default) = frozen.initial_default.as_ref() {
        let json = default
            .clone()
            .try_into_json(frozen.field_type.as_ref())
            .map_err(|error| {
                invalid(format!(
                    "iceberg initial default of field {} cannot be encoded: {error}",
                    frozen.name
                ))
            })?;
        metadata.insert(
            ICEBERG_INITIAL_DEFAULT_META_KEY.to_owned(),
            json.to_string(),
        );
    }
    let data_type = annotate_data_type(field, frozen)?;
    Ok(Field::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata))
}

fn annotate_data_type(field: &Field, frozen: &NestedField) -> Result<DataType, ConnectorError> {
    // A VARIANT is one Iceberg primitive whose physical carrier happens to be
    // a struct; its children are encoding details, not nested fields.
    if matches!(frozen.field_type.as_ref(), Type::Variant(_)) {
        return Ok(field.data_type().clone());
    }
    Ok(match (field.data_type(), frozen.field_type.as_ref()) {
        (DataType::Struct(children), Type::Struct(frozen_struct)) => {
            if children.len() != frozen_struct.fields().len() {
                return Err(invalid(format!(
                    "iceberg struct field {} does not match its read carrier",
                    frozen.name
                )));
            }
            DataType::Struct(
                children
                    .iter()
                    .zip(frozen_struct.fields())
                    .map(|(child, frozen)| {
                        annotate_field(child.as_ref(), frozen.as_ref()).map(Arc::new)
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )
        }
        (DataType::List(child), Type::List(frozen_list)) => DataType::List(Arc::new(
            annotate_field(child.as_ref(), frozen_list.element_field.as_ref())?,
        )),
        (DataType::LargeList(child), Type::List(frozen_list)) => DataType::LargeList(Arc::new(
            annotate_field(child.as_ref(), frozen_list.element_field.as_ref())?,
        )),
        (DataType::FixedSizeList(child, size), Type::List(frozen_list)) => DataType::FixedSizeList(
            Arc::new(annotate_field(
                child.as_ref(),
                frozen_list.element_field.as_ref(),
            )?),
            *size,
        ),
        (DataType::Map(entries, sorted), Type::Map(frozen_map)) => {
            let DataType::Struct(children) = entries.data_type() else {
                return Err(invalid(format!(
                    "iceberg map field {} has non-struct entries",
                    frozen.name
                )));
            };
            if children.len() != 2 {
                return Err(invalid(format!(
                    "iceberg map field {} must carry exactly a key and a value",
                    frozen.name
                )));
            }
            let key = annotate_field(children[0].as_ref(), frozen_map.key_field.as_ref())?;
            let value = annotate_field(children[1].as_ref(), frozen_map.value_field.as_ref())?;
            DataType::Map(
                Arc::new(
                    Field::new(
                        entries.name(),
                        DataType::Struct(vec![Arc::new(key), Arc::new(value)].into()),
                        entries.is_nullable(),
                    )
                    .with_metadata(entries.metadata().clone()),
                ),
                *sorted,
            )
        }
        (data_type, _) => data_type.clone(),
    })
}

/// Walk a dereference path through Arrow struct children by field ID.
pub(crate) fn dereference_target_field(
    base: &FieldRef,
    field_id_path: &[i32],
) -> Result<FieldRef, ConnectorError> {
    let mut current = Arc::clone(base);
    for field_id in field_id_path {
        let DataType::Struct(children) = current.data_type() else {
            return Err(unsupported(format!(
                "iceberg projection of field id {field_id} descends into {:?}, which this reader cannot address as a column",
                current.data_type()
            )));
        };
        let child = children
            .iter()
            .find(|child| {
                field_id_for_arrow_field(child.as_ref()).ok().flatten() == Some(*field_id)
            })
            .ok_or_else(|| {
                corrupt(format!(
                    "iceberg dereference path field id {field_id} is not a struct field of the frozen table schema"
                ))
            })?;
        current = Arc::clone(child);
    }
    Ok(current)
}

fn identity_partition_values(
    partition_spec: Option<&PartitionSpec>,
    partition_values: Option<&Struct>,
    table_schema: &Schema,
) -> Result<HashMap<i32, Option<Literal>>, ConnectorError> {
    let (Some(spec), Some(values)) = (partition_spec, partition_values) else {
        return Ok(HashMap::new());
    };
    // The partition struct is positional against the spec's fields; a
    // mismatched arity would silently shift every constant by one column.
    if values.iter().len() != spec.fields().len() {
        return Err(corrupt(format!(
            "iceberg split carries {} partition values for a spec with {} fields",
            values.iter().len(),
            spec.fields().len()
        )));
    }
    let mut result = HashMap::new();
    for (index, field) in spec.fields().iter().enumerate() {
        if field.transform != Transform::Identity {
            continue;
        }
        // A partition source that no longer exists cannot be bound to an
        // output column, so it is dropped rather than guessed at.
        if table_schema
            .as_struct()
            .field_by_id(field.source_id)
            .is_none()
        {
            continue;
        }
        let value = values.iter().nth(index).flatten().cloned();
        result.insert(field.source_id, value);
    }
    Ok(result)
}

fn adapt_array(
    source: &ArrayRef,
    target: &Field,
    path: &str,
    logical_variant: bool,
) -> Result<ArrayRef, ConnectorError> {
    match physical_adaptation(source.data_type(), target.data_type(), logical_variant)? {
        IcebergPhysicalAdaptation::Identity => Ok(Arc::clone(source)),
        IcebergPhysicalAdaptation::RetagNestedMetadata => {
            crate::file_reader::retag_iceberg_array(source, target.data_type()).map_err(|error| {
                corrupt(format!(
                    "iceberg field {} in {path} cannot be re-tagged: {error}",
                    target.name()
                ))
            })
        }
        IcebergPhysicalAdaptation::CollapseVariant => {
            collapse_variant_struct_to_largebinary(source, target.name()).map_err(|error| {
                corrupt(format!(
                    "iceberg variant field {} in {path}: {error}",
                    target.name()
                ))
            })
        }
        IcebergPhysicalAdaptation::RepresentationCast => adapt_representation(source, target, path),
        IcebergPhysicalAdaptation::Promotion(_) => cast(source.as_ref(), target.data_type())
            .map_err(|error| {
                corrupt(format!(
                    "iceberg field {} in {path} cannot be converted from {:?} to {:?}: {error}",
                    target.name(),
                    source.data_type(),
                    target.data_type()
                ))
            }),
    }
}

/// Preserve container offsets, slices and validity while adapting each leaf.
/// Rebuilding uses the same runtime-nullability rule as metadata retagging;
/// a nullable physical child is not evidence that any value is NULL.
fn adapt_representation(
    source: &ArrayRef,
    target: &Field,
    path: &str,
) -> Result<ArrayRef, ConnectorError> {
    let data = source.to_data();
    let child_fields: Option<Vec<FieldRef>> = match (source.data_type(), target.data_type()) {
        (DataType::Struct(_), DataType::Struct(fields)) => Some(fields.iter().cloned().collect()),
        (DataType::List(_), DataType::List(field))
        | (DataType::LargeList(_), DataType::LargeList(field))
        | (DataType::FixedSizeList(_, _), DataType::FixedSizeList(field, _))
        | (DataType::Map(_, _), DataType::Map(field, _)) => Some(vec![Arc::clone(field)]),
        _ => None,
    };
    let conversion_error = |error: &dyn std::fmt::Display| {
        corrupt(format!(
            "iceberg field {} in {path} cannot be converted from {:?} to {:?}: {error}",
            target.name(),
            source.data_type(),
            target.data_type()
        ))
    };
    let Some(fields) = child_fields else {
        // Arrow's default safe cast can replace offset overflow with NULL.
        // Representation changes are lossless or fail with the original
        // provider CorruptData category; no row may silently disappear.
        return cast_with_options(
            source.as_ref(),
            target.data_type(),
            &CastOptions {
                safe: false,
                ..CastOptions::default()
            },
        )
        .map_err(|error| conversion_error(&error));
    };
    let mut children = Vec::with_capacity(fields.len());
    let mut runtime_fields = Vec::with_capacity(fields.len());
    for (child, field) in data.child_data().iter().zip(&fields) {
        let array = adapt_array(&make_array(child.clone()), field, path, false)?;
        runtime_fields.push(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(array.data_type().clone())
                .with_nullable(field.is_nullable() || array.null_count() > 0),
        ));
        children.push(array.to_data());
    }
    let data_type = match target.data_type() {
        DataType::Struct(_) => DataType::Struct(runtime_fields.into()),
        DataType::List(_) => DataType::List(runtime_fields.remove(0)),
        DataType::LargeList(_) => DataType::LargeList(runtime_fields.remove(0)),
        DataType::FixedSizeList(_, size) => {
            DataType::FixedSizeList(runtime_fields.remove(0), *size)
        }
        DataType::Map(_, sorted) => DataType::Map(runtime_fields.remove(0), *sorted),
        _ => unreachable!("nested representation container was matched above"),
    };
    let converted = data
        .into_builder()
        .data_type(data_type)
        .child_data(children)
        .build()
        .map_err(|error| conversion_error(&error))?;
    Ok(make_array(converted))
}

/// Extract a nested struct field, propagating every ancestor's null mask.
fn dereference_struct_path(
    base: &ArrayRef,
    base_target: &Field,
    field_id_path: &[i32],
) -> Result<ArrayRef, ConnectorError> {
    let mut current = Arc::clone(base);
    let mut current_field = Arc::new(base_target.clone());
    for field_id in field_id_path {
        let DataType::Struct(children) = current_field.data_type().clone() else {
            return Err(unsupported(format!(
                "iceberg projection of field id {field_id} descends into a non-struct carrier"
            )));
        };
        let index = children
            .iter()
            .position(|child| {
                field_id_for_arrow_field(child.as_ref()).ok().flatten() == Some(*field_id)
            })
            .ok_or_else(|| {
                corrupt(format!(
                    "iceberg dereference path field id {field_id} is not a struct field"
                ))
            })?;
        let parent = current
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| corrupt("iceberg struct carrier is not a physical struct array"))?;
        let child = parent.column(index).clone();
        // An absent parent makes the whole subtree null; the child's own
        // validity buffer says nothing about its parent.
        let combined = NullBuffer::union(parent.nulls(), child.nulls());
        let data = child
            .to_data()
            .into_builder()
            .nulls(combined)
            .build()
            .map_err(|error| {
                corrupt(format!(
                    "iceberg dereference of field id {field_id} failed: {error}"
                ))
            })?;
        current = make_array(data);
        current_field = Arc::clone(&children[index]);
    }
    Ok(current)
}

fn partition_constant(
    value: Option<&Literal>,
    target: &Field,
    row_count: usize,
) -> Result<ArrayRef, ConnectorError> {
    match value {
        None => Ok(arrow::array::new_null_array(target.data_type(), row_count)),
        Some(literal) => {
            literal_to_constant_array(literal, target.data_type(), row_count).map_err(|error| {
                corrupt(format!(
                    "iceberg identity partition constant for {}: {error}",
                    target.name()
                ))
            })
        }
    }
}

/// Widen the reader's absolute row positions into the signed domain every
/// position-derived column works in.
fn absolute_positions_as_i64(positions: &UInt64Array) -> Result<Vec<i64>, ConnectorError> {
    positions
        .iter()
        .map(|value| {
            value
                .ok_or_else(|| corrupt("iceberg absolute row position is null"))
                .and_then(|value| {
                    i64::try_from(value)
                        .map_err(|_| corrupt("iceberg absolute row position exceeds int64"))
                })
        })
        .collect()
}

/// Resolve one row-lineage column for a batch, stored values included.
///
/// This is the single place the inherit-vs-stored rule is applied. A column
/// the file materialized was projected by the binding and is found in `batch`;
/// one it did not materialize is absent there, and every row inherits. The
/// rule itself lives in [`crate::row_lineage_synth`].
fn row_lineage_column(
    column: IcebergRowLineageColumn,
    target: &Field,
    batch: &RecordBatch,
    absolute_positions: Option<&UInt64Array>,
    facts: &IcebergSplitFacts<'_>,
) -> Result<ArrayRef, ConnectorError> {
    let schema = batch.schema();
    let row_count = batch.num_rows();
    let values = match column {
        IcebergRowLineageColumn::RowId => {
            let positions = if facts.file_first_row_id.is_some() {
                absolute_positions_as_i64(require_absolute_positions(
                    absolute_positions,
                    IcebergMetadataColumn::RowId,
                    facts,
                )?)?
            } else {
                Vec::new()
            };
            synthesize_row_id(
                &schema,
                batch.columns(),
                row_count,
                facts.file_first_row_id,
                &positions,
            )
        }
        IcebergRowLineageColumn::LastUpdatedSequenceNumber => {
            synthesize_last_updated_sequence_number(
                &schema,
                batch.columns(),
                row_count,
                facts.file_first_row_id,
                facts.data_sequence_number,
            )
        }
    }
    .map_err(|error| {
        corrupt(format!(
            "iceberg data file {} cannot resolve {}: {error}",
            facts.path,
            column.column_name()
        ))
    })?;
    cast_to_target(
        Arc::new(arrow::array::Int64Array::from(values)),
        column.column_name(),
        target,
    )
}

/// Bring a synthesized metadata array onto the carrier the scan declared.
fn cast_to_target(
    array: ArrayRef,
    column_name: &str,
    target: &Field,
) -> Result<ArrayRef, ConnectorError> {
    if array.data_type() == target.data_type() {
        return Ok(array);
    }
    cast(array.as_ref(), target.data_type()).map_err(|error| {
        corrupt(format!(
            "iceberg metadata column {column_name} cannot be converted to {:?}: {error}",
            target.data_type()
        ))
    })
}

fn require_absolute_positions<'a>(
    absolute_positions: Option<&'a UInt64Array>,
    metadata: IcebergMetadataColumn,
    facts: &IcebergSplitFacts<'_>,
) -> Result<&'a UInt64Array, ConnectorError> {
    absolute_positions.ok_or_else(|| {
        corrupt(format!(
            "iceberg metadata column {} needs file-level absolute row positions that {} did not produce",
            metadata.column_name(),
            facts.path
        ))
    })
}

fn metadata_column(
    metadata: IcebergMetadataColumn,
    target: &Field,
    batch: &RecordBatch,
    absolute_positions: Option<&UInt64Array>,
    facts: &IcebergSplitFacts<'_>,
) -> Result<ArrayRef, ConnectorError> {
    let row_count = batch.num_rows();
    let array: ArrayRef = match metadata {
        IcebergMetadataColumn::Path => {
            Arc::new(arrow::array::StringArray::from(vec![facts.path; row_count]))
        }
        IcebergMetadataColumn::RowPosition => {
            let positions = require_absolute_positions(absolute_positions, metadata, facts)?;
            Arc::new(arrow::array::Int64Array::from(absolute_positions_as_i64(
                positions,
            )?))
        }
        // A row-lineage column goes through the same rule whether or not the
        // file stores it: the rule finds nothing stored here and every row
        // inherits, which is exactly what an unmaterialized column means.
        IcebergMetadataColumn::RowId => {
            return row_lineage_column(
                IcebergRowLineageColumn::RowId,
                target,
                batch,
                absolute_positions,
                facts,
            );
        }
        IcebergMetadataColumn::LastUpdatedSequenceNumber => {
            return row_lineage_column(
                IcebergRowLineageColumn::LastUpdatedSequenceNumber,
                target,
                batch,
                absolute_positions,
                facts,
            );
        }
        IcebergMetadataColumn::IsDeleted => {
            return Err(unsupported(format!(
                "iceberg metadata column {} is not produced by this page source",
                metadata.column_name()
            )));
        }
    };
    cast_to_target(array, metadata.column_name(), target)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod sdk_nested_representation_tests;
