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

//! One split, one page source.
//!
//! A page source owns its cursor, its reader, its buffers, and one close
//! latch. Nothing here is process-global and nothing survives an attempt: a
//! replacement attempt builds a new page source over the same frozen split
//! rather than resuming this one.
//!
//! Three invariants shape the reader:
//!
//! * a row position is file-level, absolute, and zero-based -- byte-range
//!   selection narrows which row groups are read, never how rows are numbered;
//! * the scan's ordered columns are the output prefix, and whatever the delete
//!   filter needs is appended as a hidden suffix that is dropped again once the
//!   deletes and the remaining predicate have run over the complete page;
//! * `next_source_page() == None` means "nothing right now". Only
//!   [`ConnectorPageSource::is_finished`] is terminal.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{Field, FieldRef, Schema as ArrowSchema};
use novarocks_fs::{
    FileBatchReader, FileIdentity, FileProjection, FileReadBudget, FileReadContext, FileReadRange,
    FileReadRequest, FileReaderOptions, FsAccessHandle, MinMaxPredicateOp, MinMaxPredicateValue,
    ParquetMetadataInspection, ParquetPhysicalType, ParquetStatisticsSortOrder,
    ParquetStatisticsValue, PhysicalPruning, ScanPredicate, ScanPredicateDomain,
    ScanPredicateSource, inspect_parquet_metadata, open_file_reader,
};
use novarocks_spi::connector::read_stack::DynamicFilter;
use novarocks_spi::connector::read_stack::{
    Bound, BoundsMatch, ColumnValueBounds, ConnectorPageSource, ConnectorSplit, ConnectorValue,
    ConnectorValueType, Domain, PageSourceFileMetrics, PageSourceMetrics, SourcePage, TupleDomain,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::access_binding::IcebergReadBinding;
use crate::file_reader::map_file_error;
use crate::iceberg::spec::{
    Literal, NameMapping, PartitionSpec, PrimitiveType, Schema, Struct, Type,
};

use super::change_window::IcebergChangeWindowHandle;
use super::column_handle::{IcebergColumnHandle, corrupt, invalid, parse_type, unsupported};
use super::delete_manager::{DeleteEvaluationMode, DeleteManager, SplitDeleteFilter};
pub(super) type IcebergDynamicFilter = dyn DynamicFilter<IcebergColumnHandle>;

/// Lower the exact, non-null part of the typed Iceberg predicate into the
/// connector-neutral file predicate vocabulary.
///
/// This is an optional pruning optimization. The page source still evaluates
/// the complete typed domain row by row, so any shape that cannot be expressed
/// exactly here is deliberately omitted instead of being approximated. In
/// particular, nullable domains and nested columns stay residual: pruning
/// either with a physical min/max statistic could discard a matching NULL or
/// bind an incomplete Parquet path.
fn static_file_predicates(predicate: &TupleDomain<IcebergColumnHandle>) -> Vec<ScanPredicate> {
    let Some(domains) = predicate.domains() else {
        return Vec::new();
    };
    let mut predicates = Vec::new();
    for (column, domain) in domains {
        if domain.null_allowed() || !column.is_base_column() {
            continue;
        }
        let values = domain.values();
        if let Some(discrete) = values.discrete_values() {
            let Some(values) = discrete
                .into_iter()
                .map(file_predicate_value)
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            let (Some(min), Some(max)) = (values.first().cloned(), values.last().cloned()) else {
                continue;
            };
            predicates.push(file_predicate(
                column,
                ScanPredicateDomain::DiscreteSet { values, min, max },
            ));
            continue;
        }

        let [range] = values.ranges() else {
            // Multiple non-discrete ranges form a union, while multiple file
            // predicates form a conjunction. Keep that shape residual.
            continue;
        };
        if let Some(value) = range.single_value() {
            let Some(value) = file_predicate_value(value) else {
                continue;
            };
            predicates.push(file_predicate(
                column,
                ScanPredicateDomain::Range {
                    op: MinMaxPredicateOp::Eq,
                    value,
                },
            ));
            continue;
        }
        let Some(low) = file_bound_predicate(range.low(), true) else {
            continue;
        };
        let Some(high) = file_bound_predicate(range.high(), false) else {
            continue;
        };
        for (op, value) in low.into_iter().chain(high) {
            predicates.push(file_predicate(
                column,
                ScanPredicateDomain::Range { op, value },
            ));
        }
    }
    predicates
}

fn file_predicate(column: &IcebergColumnHandle, domain: ScanPredicateDomain) -> ScanPredicate {
    ScanPredicate::new(
        column.base_column_identity().name(),
        domain,
        ScanPredicateSource::Static,
    )
    .with_physical_field_id(column.base_field_id())
}

/// `None` means the bound's value has no exact physical min/max carrier;
/// `Some(None)` means the bound is unbounded.
fn file_bound_predicate(
    bound: &Bound,
    lower: bool,
) -> Option<Option<(MinMaxPredicateOp, MinMaxPredicateValue)>> {
    match bound {
        Bound::Unbounded => Some(None),
        Bound::Inclusive(value) => Some(Some((
            if lower {
                MinMaxPredicateOp::Ge
            } else {
                MinMaxPredicateOp::Le
            },
            file_predicate_value(value)?,
        ))),
        Bound::Exclusive(value) => Some(Some((
            if lower {
                MinMaxPredicateOp::Gt
            } else {
                MinMaxPredicateOp::Lt
            },
            file_predicate_value(value)?,
        ))),
    }
}

fn file_predicate_value(value: &ConnectorValue) -> Option<MinMaxPredicateValue> {
    match value {
        ConnectorValue::Boolean(value) => Some(MinMaxPredicateValue::Boolean(*value)),
        ConnectorValue::Integer(value) => Some(MinMaxPredicateValue::Int32(*value)),
        ConnectorValue::BigInt(value) => Some(MinMaxPredicateValue::Int64(*value)),
        // Parquet exposes DATE statistics and page indexes as physical INT32.
        ConnectorValue::Date(value) => Some(MinMaxPredicateValue::Int32(*value)),
        ConnectorValue::TimeMicros(value)
        | ConnectorValue::TimestampMicros(value)
        | ConnectorValue::TimestampTzMicros(value)
        | ConnectorValue::TimestampNanos(value)
        | ConnectorValue::TimestampTzNanos(value) => Some(MinMaxPredicateValue::Int64(*value)),
        ConnectorValue::TinyInt(_)
        | ConnectorValue::Real(_)
        | ConnectorValue::Double(_)
        | ConnectorValue::Decimal { .. }
        | ConnectorValue::Varchar(_)
        | ConnectorValue::Varbinary(_)
        | ConnectorValue::Uuid(_)
        | ConnectorValue::Fixed(_) => None,
    }
}

use super::schema_binding::{
    FileFieldIdCoverage, IcebergColumnSource, IcebergSchemaBinding, IcebergSchemaBindingRequest,
    IcebergSplitFacts, bind_scan_columns,
};
use super::split::{IcebergFileFormat, IcebergSplit, ParquetFileDecryptionData};
use super::table_handle::IcebergTableHandle;

/// Where the live dynamic filter is consulted.
///
/// The three checkpoints are the only moments at which pruning could still
/// save work: once the footer is known, once before the split's first row
/// group is decoded, and once before every row group that has not been read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DynamicFilterCheckpoint {
    Footer,
    FirstRowGroup,
    NextRowGroup,
}

/// What the dynamic-filter seam decided about one row group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DynamicFilterVerdict {
    ReadEverything,
    /// The filter proved this row group cannot hold a matching row.
    SkipRowGroup,
}

/// What one live dynamic filter looked like when the page source was built.
///
/// One reading of the live filter, taken fresh at each checkpoint rather than
/// once at open, so a filter that arrives mid-split is seen by the row groups
/// that have not been read yet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DynamicFilterObservation {
    covered_columns: usize,
    constrains_anything: bool,
    complete: bool,
}

impl DynamicFilterObservation {
    pub fn observe(filter: &IcebergDynamicFilter) -> Self {
        let snapshot = filter.snapshot();
        Self {
            covered_columns: filter.columns_covered().len(),
            constrains_anything: !snapshot.current_predicate().is_all(),
            complete: snapshot.is_complete(),
        }
    }

    /// The unconstrained observation, matching `CompleteAllDynamicFilter`.
    pub const fn complete_all() -> Self {
        Self {
            covered_columns: 0,
            constrains_anything: false,
            complete: true,
        }
    }

    pub const fn covered_columns(&self) -> usize {
        self.covered_columns
    }

    /// Whether the filter had narrowed anything at all when it was observed.
    pub const fn constrains_anything(&self) -> bool {
        self.constrains_anything
    }

    pub const fn is_complete(&self) -> bool {
        self.complete
    }
}

/// The live filter this split consults, plus the scheduling identity a
/// row-group decision is attributed to.
///
/// The filter is a shared handle rather than a snapshot so it can be re-read;
/// the sequence id is the split's position in its task attempt, which is the
/// only scheduling identity there is.
#[derive(Clone)]
pub struct LiveDynamicFilter {
    filter: Arc<IcebergDynamicFilter>,
    scheduled_split_sequence_id: u64,
}

impl LiveDynamicFilter {
    pub fn new(filter: Arc<IcebergDynamicFilter>, scheduled_split_sequence_id: u64) -> Self {
        Self {
            filter,
            scheduled_split_sequence_id,
        }
    }

    pub const fn scheduled_split_sequence_id(&self) -> u64 {
        self.scheduled_split_sequence_id
    }

    /// Read the filter as it stands right now.
    pub fn observe(&self) -> DynamicFilterObservation {
        DynamicFilterObservation::observe(self.filter.as_ref())
    }

    /// Whether the filter can still tighten.
    ///
    /// A complete filter lets the whole surviving remainder of a split be read
    /// through one reader; an incomplete one is re-read before every row group
    /// that has not been read yet, so a filter arriving mid-split still prunes
    /// the rest.
    pub fn is_complete(&self) -> bool {
        self.filter.is_complete()
    }

    /// The scan's dynamic-filter columns that this file can actually answer
    /// for, resolved once against the immutable footer.
    ///
    /// A covered column with no primitive leaf in this file, or whose Iceberg
    /// and Parquet types have no agreed comparison, is simply absent: it can
    /// never contribute a prune, and asking about it once per row group would
    /// cost work for an answer that is always `Unknown`.
    fn resolve_columns(&self, footer: &ParquetMetadataInspection) -> Vec<DynamicFilterColumn> {
        self.filter
            .columns_covered()
            .iter()
            .filter_map(|column| DynamicFilterColumn::resolve(column, footer))
            .collect()
    }

    /// Name one row group of this split without inventing an identity for it.
    ///
    /// The scheduled-split sequence id is the split's position in its task
    /// attempt, and the ordinal is the row group's position in the file; both
    /// are facts the task already owns, so no membership digest is involved.
    const fn row_group(&self, row_group_ordinal: u32) -> DynamicFilterRowGroupId {
        DynamicFilterRowGroupId {
            scheduled_split_sequence_id: self.scheduled_split_sequence_id,
            row_group_ordinal,
        }
    }
}

/// Names one judged row group of one scheduled split.
///
/// This mirrors `novarocks_execution::runtime_filter::row_group_domain::
/// RuntimeFilterRowGroupId`; a connector must not depend on the execution
/// crate, so the pair is restated here rather than imported.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DynamicFilterRowGroupId {
    pub scheduled_split_sequence_id: u64,
    pub row_group_ordinal: u32,
}

/// One covered column bound to a primitive leaf of this Parquet file.
#[derive(Clone, Debug)]
struct DynamicFilterColumn {
    column: IcebergColumnHandle,
    /// The footer's physical-column ordinal for this Iceberg field id.
    physical_ordinal: u32,
    /// How a raw footer statistic becomes a comparable connector value.
    value_kind: StatisticsValueKind,
}

impl DynamicFilterColumn {
    fn resolve(column: &IcebergColumnHandle, footer: &ParquetMetadataInspection) -> Option<Self> {
        let handle = column.clone();
        // A nested field has no single primitive leaf statistic to read, and
        // inferring one from an ancestor would be a guess.
        if !handle.is_base_column() {
            return None;
        }
        let iceberg_type = parse_type(handle.type_json(), "type_json").ok()?;
        let field_id = handle.base_field_id();
        let physical = footer
            .physical_columns()
            .iter()
            .find(|physical| physical.field_id() == Some(field_id))?;
        let value_kind = StatisticsValueKind::resolve(&iceberg_type, physical.physical_type())?;
        Some(Self {
            column: column.clone(),
            physical_ordinal: physical.ordinal(),
            value_kind,
        })
    }
}

/// The exactly one conversion from a raw footer statistic to a connector value
/// that this column's Iceberg type and Parquet physical type agree on.
///
/// The set is deliberately the set the runtime-filter artifact can compare. A
/// type outside it produces no bounds at all rather than a coerced value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StatisticsValueKind {
    Boolean,
    Integer,
    BigInt,
    Date,
    TimestampMicros,
    TimestampNanos,
    Varchar,
}

impl StatisticsValueKind {
    fn resolve(iceberg_type: &Type, physical: ParquetPhysicalType) -> Option<Self> {
        // Both sides must agree: the Iceberg type says what the value means and
        // the Parquet physical type says how it is stored. A disagreement is a
        // file this page source will not guess about.
        match (iceberg_type, physical) {
            (Type::Primitive(PrimitiveType::Boolean), ParquetPhysicalType::Boolean) => {
                Some(Self::Boolean)
            }
            (Type::Primitive(PrimitiveType::Int), ParquetPhysicalType::Int32) => {
                Some(Self::Integer)
            }
            (Type::Primitive(PrimitiveType::Long), ParquetPhysicalType::Int64) => {
                Some(Self::BigInt)
            }
            (Type::Primitive(PrimitiveType::Date), ParquetPhysicalType::Int32) => Some(Self::Date),
            (Type::Primitive(PrimitiveType::Timestamp), ParquetPhysicalType::Int64) => {
                Some(Self::TimestampMicros)
            }
            (Type::Primitive(PrimitiveType::TimestampNs), ParquetPhysicalType::Int64) => {
                Some(Self::TimestampNanos)
            }
            (Type::Primitive(PrimitiveType::String), ParquetPhysicalType::ByteArray) => {
                Some(Self::Varchar)
            }
            // Every other pairing -- a nested type, a float or decimal, a zoned
            // timestamp, a raw byte payload, or a physical type that disagrees
            // with the Iceberg type -- has no comparison the artifact accepts.
            _ => None,
        }
    }

    /// Convert one raw footer statistic, or refuse it.
    fn value(self, raw: &ParquetStatisticsValue) -> Option<ConnectorValue> {
        match (self, raw) {
            (Self::Boolean, ParquetStatisticsValue::Boolean(value)) => {
                Some(ConnectorValue::Boolean(*value))
            }
            (Self::Integer, ParquetStatisticsValue::Int32(value)) => {
                Some(ConnectorValue::Integer(*value))
            }
            (Self::BigInt, ParquetStatisticsValue::Int64(value)) => {
                Some(ConnectorValue::BigInt(*value))
            }
            (Self::Date, ParquetStatisticsValue::Int32(value)) => {
                Some(ConnectorValue::Date(*value))
            }
            (Self::TimestampMicros, ParquetStatisticsValue::Int64(value)) => {
                Some(ConnectorValue::TimestampMicros(*value))
            }
            (Self::TimestampNanos, ParquetStatisticsValue::Int64(value)) => {
                Some(ConnectorValue::TimestampNanos(*value))
            }
            (Self::Varchar, ParquetStatisticsValue::ByteArray(bytes)) => {
                // A string bound that is not valid UTF-8 is not comparable as a
                // string, so it yields nothing instead of a lossy conversion.
                std::str::from_utf8(bytes)
                    .ok()
                    .map(|value| ConnectorValue::Varchar(Arc::from(value)))
            }
            // The footer disagrees with the physical type this column resolved
            // to, which is a file fact this page source will not reinterpret.
            _ => None,
        }
    }
}

/// Read one column's bounds for one row group out of the immutable footer.
///
/// Nothing here leaves the backend: the footer was already read for this split,
/// and the statistics are read locally to ask the filter a local question.
fn row_group_bounds(
    footer: &ParquetMetadataInspection,
    row_group_ordinal: u32,
    column: &DynamicFilterColumn,
) -> ColumnValueBounds {
    let row_count = footer
        .row_groups()
        .iter()
        .find(|layout| layout.ordinal == row_group_ordinal)
        .map(|layout| layout.row_count);
    let Some(statistics) = footer.column_statistics(row_group_ordinal, column.physical_ordinal)
    else {
        return ColumnValueBounds {
            value_count: row_count,
            ..ColumnValueBounds::default()
        };
    };
    // A truncated, deprecated, or unknown-sort-order bound cannot prove
    // anything, so it is reported as inexact rather than dropped: the row-group
    // size and null count are still useful facts.
    let bounds_are_exact = statistics.min_is_exact()
        && statistics.max_is_exact()
        && !statistics.min_max_deprecated()
        && !matches!(
            statistics.sort_order(),
            ParquetStatisticsSortOrder::Undefined
        );
    ColumnValueBounds {
        min: statistics
            .min()
            .and_then(|value| column.value_kind.value(value)),
        max: statistics
            .max()
            .and_then(|value| column.value_kind.value(value)),
        null_count: statistics.null_count(),
        value_count: row_count,
        bounds_are_exact,
    }
}

impl std::fmt::Debug for LiveDynamicFilter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LiveDynamicFilter")
            .field(
                "scheduled_split_sequence_id",
                &self.scheduled_split_sequence_id,
            )
            .finish_non_exhaustive()
    }
}

/// The single seam where a live backend dynamic filter prunes this split.
///
/// It is deliberately the only place any dynamic-filter decision is made. The
/// filter is re-read here rather than at open, so a filter that arrives
/// mid-split still prunes the row groups that follow.
///
/// The backend's runtime-filter artifact is a predicate oracle, not an
/// enumerable domain (ADR-0043), so the question asked is "could this row
/// group's bounds match", never "what does the filter contain". A row group is
/// skipped only when some column proves it cannot match; `Possible` and
/// `Unknown` both keep it, because a wrong prune silently returns fewer rows
/// while a wrong keep only costs work.
fn consult_dynamic_filter(
    live: &LiveDynamicFilter,
    row_group_ordinal: u32,
    columns: &[DynamicFilterColumn],
    footer: &ParquetMetadataInspection,
) -> DynamicFilterVerdict {
    let _observation = live.observe();
    for column in columns {
        let bounds = row_group_bounds(footer, row_group_ordinal, column);
        match live.filter.bounds_may_match(&column.column, &bounds) {
            BoundsMatch::Impossible => return DynamicFilterVerdict::SkipRowGroup,
            BoundsMatch::Possible | BoundsMatch::Unknown => {}
        }
    }
    DynamicFilterVerdict::ReadEverything
}

/// Whether this build emits connector-reader evidence markers.
///
/// This mirrors the backend's `debug_emit_connector_reader_marker` gate: same
/// variable, and compiled out of release builds so a production binary cannot
/// print one. A connector must not depend on the backend crate, so the gate is
/// restated rather than imported.
#[cfg(debug_assertions)]
fn connector_reader_marker_enabled() -> bool {
    std::env::var_os("NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER").is_some()
}

#[cfg(not(debug_assertions))]
const fn connector_reader_marker_enabled() -> bool {
    false
}

/// Report one pruned row group by identity only.
///
/// The marker carries the scheduled-split sequence, the row-group ordinal, and
/// the checkpoint that decided. It never carries a bound, a value, or anything
/// else derived from the filter's key material.
fn emit_row_group_pruned_marker(
    row_group: DynamicFilterRowGroupId,
    checkpoint: DynamicFilterCheckpoint,
) {
    if !connector_reader_marker_enabled() {
        return;
    }
    let checkpoint = match checkpoint {
        DynamicFilterCheckpoint::Footer => "footer",
        DynamicFilterCheckpoint::FirstRowGroup => "first_row_group",
        DynamicFilterCheckpoint::NextRowGroup => "next_row_group",
    };
    println!(
        "NOVAROCKS_RUNTIME_FILTER_ROW_GROUP_PRUNED scheduled_split_sequence_id={} row_group_ordinal={} checkpoint={checkpoint}",
        row_group.scheduled_split_sequence_id, row_group.row_group_ordinal
    );
}

/// The half-open absolute row-position window a split actually read.
///
/// It adapts the reader's per-batch positions into the `[start, end)` form the
/// Iceberg reader reasons in. It is private on purpose: neither the SPI nor the
/// wire carries a row-position window, and publishing one would invite a
/// scheduler to treat it as split identity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReaderPageSourceWithRowPositions {
    start_row_position: Option<u64>,
    end_row_position: Option<u64>,
}

impl ReaderPageSourceWithRowPositions {
    const fn end_row_position(&self) -> Option<u64> {
        self.end_row_position
    }

    /// Fold one batch's absolute positions into the window.
    fn observe(&mut self, positions: &UInt64Array, path: &str) -> Result<(), ConnectorError> {
        if positions.is_empty() {
            return Ok(());
        }
        if positions.null_count() != 0 {
            return Err(corrupt(format!(
                "iceberg data file {path} produced a null absolute row position"
            )));
        }
        let first = positions.value(0);
        let last = positions.value(positions.len() - 1);
        if last < first {
            return Err(corrupt(format!(
                "iceberg data file {path} produced absolute row positions out of order"
            )));
        }
        if let Some(previous_end) = self.end_row_position
            && first < previous_end
        {
            return Err(corrupt(format!(
                "iceberg data file {path} revisited absolute row position {first}"
            )));
        }
        if self.start_row_position.is_none() {
            self.start_row_position = Some(first);
        }
        self.end_row_position = Some(last.saturating_add(1));
        Ok(())
    }
}

/// One immutable Parquet footer per data file, shared by the splits of a scan.
///
/// The cache lives on the provider, which lives for one fragment instance and
/// scan node. Splits of the same file therefore read the footer once, and
/// nothing survives the provider.
#[derive(Debug, Default)]
pub struct ParquetFooterCache {
    entries: Mutex<HashMap<Arc<str>, ParquetMetadataInspection>>,
}

impl ParquetFooterCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read one data file's footer, or hand back the copy this scan already has.
    pub fn footer(
        &self,
        access: &FsAccessHandle,
        context: &FileReadContext,
        path: &str,
        file_size: u64,
    ) -> Result<ParquetMetadataInspection, ConnectorError> {
        if let Some(cached) = self.lock()?.get(path) {
            return Ok(cached.clone());
        }
        let file = access
            .bind_location(path, FileIdentity::new(path, file_size, None))
            .map_err(map_file_error)?;
        let inspection =
            inspect_parquet_metadata(file, None, context.clone()).map_err(map_file_error)?;
        self.lock()?.insert(Arc::from(path), inspection.clone());
        Ok(inspection)
    }

    /// How many distinct footers this scan has read.
    pub fn len(&self) -> Result<usize, ConnectorError> {
        Ok(self.lock()?.len())
    }

    pub fn is_empty(&self) -> Result<bool, ConnectorError> {
        Ok(self.lock()?.is_empty())
    }

    fn lock(
        &self,
    ) -> Result<
        std::sync::MutexGuard<'_, HashMap<Arc<str>, ParquetMetadataInspection>>,
        ConnectorError,
    > {
        self.entries.lock().map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("iceberg parquet footer cache lock: {error}"),
            )
        })
    }
}

/// The relation-level facts one page source reads a data file against.
///
/// A data scan and a change window are different relations with different
/// handles, and neither handle converts into the other -- a change window has
/// no snapshot, no pushdown predicate, and no table location. What a reader
/// actually needs from either is the same four facts, so they are named here
/// directly instead of forcing one relation to impersonate the other.
#[derive(Clone, Debug)]
pub struct IcebergReadRelation {
    table_schema: Arc<Schema>,
    partition_spec: PartitionSpec,
    name_mapping: Option<Arc<NameMapping>>,
    effective_predicate: TupleDomain<IcebergColumnHandle>,
}

impl IcebergReadRelation {
    /// The facts a data scan of one split reads against.
    pub fn of_table(
        handle: &IcebergTableHandle,
        partition_spec_id: i32,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            table_schema: Arc::new(handle.parse_table_schema()?),
            partition_spec: handle.parse_partition_spec(partition_spec_id)?,
            name_mapping: parse_name_mapping(handle.name_mapping_json())?,
            effective_predicate: handle.effective_predicate()?,
        })
    }

    /// The facts a change-window split reads against.
    ///
    /// A change window pushes no predicate down: its rows are the difference
    /// of two pinned endpoints, and a filter the enumeration never saw could
    /// only remove rows the difference owns.
    pub fn of_change_window(
        handle: &IcebergChangeWindowHandle,
        partition_spec_id: i32,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            table_schema: Arc::new(handle.parse_table_schema()?),
            partition_spec: handle.parse_partition_spec(partition_spec_id)?,
            name_mapping: parse_name_mapping(handle.name_mapping_json())?,
            effective_predicate: TupleDomain::all(),
        })
    }

    pub fn table_schema(&self) -> &Arc<Schema> {
        &self.table_schema
    }
}

/// Everything one page source needs, all of it already frozen or process-local.
pub struct IcebergPageSourceRequest<'a> {
    pub relation: &'a IcebergReadRelation,
    pub split: &'a IcebergSplit,
    /// The scan's ordered output columns; they become the page's prefix.
    pub columns: &'a [IcebergColumnHandle],
    pub delete_manager: Arc<DeleteManager>,
    /// How the split's delete state is spent. An ordinary scan excludes
    /// deleted rows; a change window's reverse side selects them.
    pub delete_mode: DeleteEvaluationMode,
    pub footers: Arc<ParquetFooterCache>,
    pub access_binding: IcebergReadBinding,
    pub context: FileReadContext,
    pub budget: FileReadBudget,
    pub reader_options: FileReaderOptions,
    /// Names this split within its task attempt; the only scheduling identity.
    pub scheduled_split_sequence_id: u64,
    pub dynamic_filter: Arc<IcebergDynamicFilter>,
}

/// Build the page source for one Iceberg data split.
pub fn create_iceberg_page_source(
    request: IcebergPageSourceRequest<'_>,
) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
    let split = request.split;
    admit_file_format(split.file_format())?;
    reject_encryption_material(
        split.decryption_data(),
        &format!("iceberg data file {}", split.path()),
    )?;
    for delete in split.deletes() {
        reject_encryption_material(
            delete.decryption_data(),
            &format!("iceberg delete file {}", delete.path()),
        )?;
    }

    let table_schema = Arc::clone(&request.relation.table_schema);
    let partition_spec = request.relation.partition_spec.clone();
    if partition_spec.spec_id() != split.partition_spec_id() {
        return Err(invalid(format!(
            "iceberg data file {} was planned under partition spec {} but is read against spec {}",
            split.path(),
            split.partition_spec_id(),
            partition_spec.spec_id()
        )));
    }
    let partition_values = parse_partition_values(split, &partition_spec, &table_schema)?;
    let effective_predicate = request.relation.effective_predicate.clone();

    // The fast path answers "every row of this file", which is only the right
    // answer when deletes hide rows. A mode that *selects* rows -- the change
    // window's reverse side, or reverse equality projection -- would get every
    // row of the file back with the right shape and the wrong contents.
    if request.delete_mode == DeleteEvaluationMode::ExcludeDeleted
        && let Some(fast_path) = try_partition_only_page_source(
            split,
            request.columns,
            &partition_spec,
            &partition_values,
            &table_schema,
            &effective_predicate,
            request.budget,
        )?
    {
        return Ok(Box::new(fast_path));
    }

    let name_mapping = request.relation.name_mapping.clone();
    let delete_filter =
        request
            .delete_manager
            .open_split(split, &table_schema, request.delete_mode)?;
    let hidden_columns = delete_filter.required_hidden_columns().to_vec();

    Ok(Box::new(IcebergParquetPageSource {
        split: split.clone(),
        table_schema,
        name_mapping,
        partition_spec,
        partition_values,
        prefix_len: request.columns.len(),
        bound_handles: request
            .columns
            .iter()
            .cloned()
            .chain(hidden_columns)
            .collect(),
        delete_filter,
        effective_predicate,
        access_binding: request.access_binding,
        context: request.context,
        footers: request.footers,
        budget: request.budget,
        reader_options: request.reader_options,
        dynamic_filter: LiveDynamicFilter::new(
            request.dynamic_filter,
            request.scheduled_split_sequence_id,
        ),
        footer: None,
        dynamic_filter_columns: Vec::new(),
        pruned_row_groups: Vec::new(),
        state: ReaderState::NotOpened,
        row_window: ReaderPageSourceWithRowPositions::default(),
        retired_bytes: 0,
        completed_bytes: 0,
        retired_file_metrics: Default::default(),
        file_metrics: Default::default(),
        completed_positions: 0,
        read_time_nanos: 0,
        retained_bytes: split.retained_size_in_bytes(),
        finished: false,
        closed: false,
    }))
}

/// Parquet is implemented; the other formats keep their contract slot and a
/// stable rejection rather than a partially working reader.
fn admit_file_format(format: IcebergFileFormat) -> Result<(), ConnectorError> {
    match format {
        IcebergFileFormat::Parquet => Ok(()),
        IcebergFileFormat::Orc => Err(unsupported(
            "iceberg ORC data files are not readable by this page source",
        )),
        IcebergFileFormat::Avro => Err(unsupported(
            "iceberg AVRO data files are not readable by this page source",
        )),
        IcebergFileFormat::Puffin => Err(invalid(
            "an iceberg data split is never in the puffin delete-artifact format",
        )),
    }
}

/// Modular encryption is contracted but not implemented.
///
/// The rejection names the file and nothing else: key metadata and AAD
/// prefixes never reach a message, a log, or a `Debug` rendering.
fn reject_encryption_material(
    material: Option<&ParquetFileDecryptionData>,
    what: &str,
) -> Result<(), ConnectorError> {
    let Some(material) = material else {
        return Ok(());
    };
    if material.key_metadata().is_empty() && material.aad_prefix().is_empty() {
        return Ok(());
    }
    Err(unsupported(format!(
        "{what} carries parquet decryption material, which this read stack does not implement"
    )))
}

fn parse_name_mapping(json: Option<&str>) -> Result<Option<Arc<NameMapping>>, ConnectorError> {
    let Some(json) = json else {
        return Ok(None);
    };
    let mapping: NameMapping = serde_json::from_str(json)
        .map_err(|error| invalid(format!("iceberg name mapping json is invalid: {error}")))?;
    Ok(Some(Arc::new(mapping)))
}

fn parse_partition_values(
    split: &IcebergSplit,
    partition_spec: &PartitionSpec,
    table_schema: &Schema,
) -> Result<Struct, ConnectorError> {
    let partition_type = partition_spec
        .partition_type(table_schema)
        .map_err(|error| {
            invalid(format!(
                "iceberg partition spec {} does not bind to the frozen table schema: {error}",
                partition_spec.spec_id()
            ))
        })?;
    let json: serde_json::Value =
        serde_json::from_str(split.partition_data_json()).map_err(|error| {
            corrupt(format!(
                "iceberg split partition data json is invalid: {error}"
            ))
        })?;
    let literal = Literal::try_from_json(json, &crate::iceberg::spec::Type::Struct(partition_type))
        .map_err(|error| corrupt(format!("iceberg split partition data: {error}")))?;
    match literal {
        Some(Literal::Struct(values)) => Ok(values),
        // A partition struct is never absent: an unpartitioned file encodes an
        // empty struct, which is a value, not a missing fact.
        Some(_) | None => Err(corrupt(
            "iceberg split partition data json is not a partition struct",
        )),
    }
}

// ---------------------------------------------------------------------------
// Partition-only fast path
// ---------------------------------------------------------------------------

/// Build the partition-only page source when every precondition holds.
///
/// The path is legal only when the split covers the whole file, has no
/// deletes, has an unconstrained effective predicate, and every requested
/// column is an identity partition column. All four facts are frozen, so the
/// data file is never opened and the record count comes straight from the
/// manifest.
fn try_partition_only_page_source(
    split: &IcebergSplit,
    columns: &[IcebergColumnHandle],
    partition_spec: &PartitionSpec,
    partition_values: &Struct,
    table_schema: &Schema,
    effective_predicate: &TupleDomain<IcebergColumnHandle>,
    budget: FileReadBudget,
) -> Result<Option<IcebergPartitionOnlyPageSource>, ConnectorError> {
    if !split.is_whole_file() || !split.deletes().is_empty() || !effective_predicate.is_all() {
        return Ok(None);
    }
    let binding = bind_scan_columns(IcebergSchemaBindingRequest {
        table_schema,
        // An empty file schema forces every column onto a non-physical source,
        // so a column that would need the file cannot slip into this path.
        file_schema: &Arc::new(ArrowSchema::empty()),
        name_mapping: None,
        partition_spec: Some(partition_spec),
        partition_values: Some(partition_values),
        columns,
    });
    // A binding failure here only proves the fast path does not apply. The
    // ordinary reader re-binds against the real footer and raises the same
    // error properly if it is a genuine one.
    let Ok(binding) = binding else {
        return Ok(None);
    };
    let mut constants = Vec::with_capacity(binding.columns().len());
    for column in binding.columns() {
        match column.source() {
            IcebergColumnSource::IdentityPartitionConstant(value) => {
                constants.push((Arc::clone(column.target()), value.clone()));
            }
            IcebergColumnSource::Physical { .. }
            | IcebergColumnSource::InitialDefault
            | IcebergColumnSource::TypedNull
            | IcebergColumnSource::Metadata(_)
            | IcebergColumnSource::StoredRowLineage(_) => return Ok(None),
        }
    }
    let total_rows = u64::try_from(split.file_record_count()).map_err(|_| {
        corrupt(format!(
            "iceberg data file {} declares a negative record count",
            split.path()
        ))
    })?;
    Ok(Some(IcebergPartitionOnlyPageSource {
        constants,
        total_rows,
        emitted_rows: 0,
        max_batch_rows: budget.max_rows.get(),
        retained_bytes: split.retained_size_in_bytes(),
        closed: false,
    }))
}

/// A scan that needs no byte of the data file.
pub struct IcebergPartitionOnlyPageSource {
    constants: Vec<(FieldRef, Option<Literal>)>,
    total_rows: u64,
    emitted_rows: u64,
    max_batch_rows: usize,
    retained_bytes: u64,
    closed: bool,
}

impl ConnectorPageSource for IcebergPartitionOnlyPageSource {
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if self.closed || self.emitted_rows >= self.total_rows {
            return Ok(None);
        }
        let remaining = self.total_rows - self.emitted_rows;
        let rows = usize::try_from(remaining.min(self.max_batch_rows as u64)).map_err(|_| {
            ConnectorError::new(ConnectorErrorKind::Internal, "row budget overflow")
        })?;
        let page = if self.constants.is_empty() {
            // A count-only or partition-only scan legitimately produces
            // positions without producing a single column.
            SourcePage::zero_channel(rows)
        } else {
            let mut columns = Vec::with_capacity(self.constants.len());
            for (field, value) in &self.constants {
                columns.push(constant_column(value.as_ref(), field.as_ref(), rows)?);
            }
            SourcePage::try_new(rows, columns)?
        };
        self.emitted_rows += rows as u64;
        Ok(Some(page))
    }

    fn is_finished(&self) -> bool {
        self.closed || self.emitted_rows >= self.total_rows
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: 0,
            completed_positions: self.emitted_rows,
            read_time_nanos: 0,
            ..Default::default()
        }
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.retained_bytes
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        Ok(())
    }
}

fn constant_column(
    value: Option<&Literal>,
    field: &Field,
    rows: usize,
) -> Result<ArrayRef, ConnectorError> {
    match value {
        None => Ok(arrow::array::new_null_array(field.data_type(), rows)),
        Some(literal) => {
            crate::default_value::literal_to_constant_array(literal, field.data_type(), rows)
                .map_err(|error| {
                    corrupt(format!(
                        "iceberg identity partition constant for {}: {error}",
                        field.name()
                    ))
                })
        }
    }
}

fn runtime_page_schema(schema: &ArrowSchema, columns: &[ArrayRef]) -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new_with_metadata(
        schema
            .fields()
            .iter()
            .zip(columns)
            .map(|(field, column)| {
                Arc::new(
                    Field::new(
                        field.name(),
                        column.data_type().clone(),
                        field.is_nullable() || column.null_count() > 0,
                    )
                    .with_metadata(field.metadata().clone()),
                )
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

// ---------------------------------------------------------------------------
// The Parquet page source
// ---------------------------------------------------------------------------

enum ReaderState {
    NotOpened,
    Open {
        /// Absent between two runs of the plan, while the next surviving row
        /// groups are still being chosen.
        reader: Option<Box<dyn FileBatchReader>>,
        plan: RowGroupPlan,
        binding: IcebergSchemaBinding,
        page_schema: Arc<ArrowSchema>,
        checks: Vec<PredicateCheck>,
        positions_required: bool,
    },
    Drained,
}

/// Which row groups are still ahead of the reader, and how they may be chosen.
///
/// A whole-file split owns every footer row group, so it can name each one and
/// re-ask the filter before reading it. A byte-bounded split cannot: the reader
/// selects row groups by byte offset and the public footer facts carry no
/// row-group offset, so the surviving set is chosen once and handed to the
/// reader, which intersects it with the range.
enum RowGroupPlan {
    PerRowGroup {
        /// Ascending row-group ordinals, from the next one to consider.
        remaining: std::collections::VecDeque<u32>,
        first_pending: bool,
    },
    WholeRange {
        /// Ordinals that survived the one consultation this split can make.
        surviving: Option<Vec<u32>>,
        pending: bool,
    },
}

impl RowGroupPlan {
    /// Whether every row group this split owns has been read or skipped.
    fn is_exhausted(&self) -> bool {
        match self {
            Self::PerRowGroup { remaining, .. } => remaining.is_empty(),
            Self::WholeRange { pending, .. } => !*pending,
        }
    }
}

/// The row groups the plan offers for the next reader.
enum NextCandidates {
    /// Already judged; `None` means "whatever the byte range selects".
    WholeRange(Option<Vec<u32>>),
    /// Still to be judged, ascending.
    PerRowGroup(Vec<u32>),
}

/// One column domain the page source still has to prove per row.
struct PredicateCheck {
    channel: usize,
    domain: Domain,
}

/// The per-split Parquet reader.
pub struct IcebergParquetPageSource {
    split: IcebergSplit,
    table_schema: Arc<Schema>,
    name_mapping: Option<Arc<NameMapping>>,
    partition_spec: PartitionSpec,
    partition_values: Struct,
    /// How many leading channels the scan actually asked for.
    prefix_len: usize,
    /// The ordered output columns followed by the delete filter's hidden suffix.
    bound_handles: Vec<IcebergColumnHandle>,
    delete_filter: SplitDeleteFilter,
    effective_predicate: TupleDomain<IcebergColumnHandle>,
    access_binding: IcebergReadBinding,
    context: FileReadContext,
    footers: Arc<ParquetFooterCache>,
    budget: FileReadBudget,
    reader_options: FileReaderOptions,
    dynamic_filter: LiveDynamicFilter,
    /// The immutable footer, kept for the life of the split so a row group can
    /// be judged without reading it again.
    footer: Option<ParquetMetadataInspection>,
    /// The scan's dynamic-filter columns this file can answer for. Empty means
    /// no prune is possible for this split, however the filter evolves.
    dynamic_filter_columns: Vec<DynamicFilterColumn>,
    /// The row groups this split proved it need not read, in decision order.
    pruned_row_groups: Vec<DynamicFilterRowGroupId>,
    state: ReaderState,
    row_window: ReaderPageSourceWithRowPositions,
    /// Bytes read by readers this split has already closed. The open reader's
    /// own counter is added on top, so a plan that opens one reader per row
    /// group still reports one monotonic total.
    retired_bytes: u64,
    completed_bytes: u64,
    /// File counters from readers already retired by this split.
    retired_file_metrics: novarocks_fs::FileMetricsSnapshot,
    /// Monotonic total including the currently open reader.
    file_metrics: novarocks_fs::FileMetricsSnapshot,
    completed_positions: u64,
    read_time_nanos: u64,
    retained_bytes: u64,
    finished: bool,
    closed: bool,
}

impl IcebergParquetPageSource {
    fn open(&mut self) -> Result<(), ConnectorError> {
        let access = self.access_binding.resolve_access(self.split.path())?;
        let file_size = u64::try_from(self.split.file_size()).map_err(|_| {
            corrupt(format!(
                "iceberg data file {} declares a negative size",
                self.split.path()
            ))
        })?;
        let footer = self
            .footers
            .footer(&access, &self.context, self.split.path(), file_size)?;
        self.dynamic_filter_columns = self.dynamic_filter.resolve_columns(&footer);
        // The footer checkpoint judges the whole split at once. A split whose
        // every row group is proven impossible is finished without opening any
        // reader at all.
        let (surviving, pruned) = self.surviving_row_groups(&footer);
        if surviving.is_empty() && !footer.row_groups().is_empty() {
            self.record_pruned(&pruned, DynamicFilterCheckpoint::Footer);
            self.footer = Some(footer);
            self.finished = true;
            self.state = ReaderState::Drained;
            return Ok(());
        }

        let binding = bind_scan_columns(IcebergSchemaBindingRequest {
            table_schema: &self.table_schema,
            file_schema: footer.schema(),
            name_mapping: self.name_mapping.clone(),
            partition_spec: Some(&self.partition_spec),
            partition_values: Some(&self.partition_values),
            columns: &self.bound_handles,
        })?;

        // A bounded split is defined by a row range, so its rows can only be
        // named by their file-level absolute positions. Deletes and row
        // lineage need them for the same reason.
        let positions_required = !self.split.is_whole_file()
            || !self.delete_filter.is_empty()
            || binding.requires_row_positions();

        let page_schema = Arc::new(ArrowSchema::new(
            binding
                .columns()
                .iter()
                .map(|column| Arc::clone(column.target()))
                .collect::<Vec<_>>(),
        ));
        let checks = self.build_predicate_checks()?;

        // A whole-file split owns every footer row group by ordinal, so it can
        // re-ask the filter before each one it has not read. A byte-bounded
        // split cannot name its own row groups -- the reader selects them by
        // byte offset and the footer facts published here carry no offset --
        // so it consults once and hands the surviving set to the reader.
        let plan = if self.split.is_whole_file() {
            RowGroupPlan::PerRowGroup {
                remaining: footer
                    .row_groups()
                    .iter()
                    .map(|layout| layout.ordinal)
                    .collect(),
                first_pending: true,
            }
        } else {
            // This is the one decision a byte-bounded split can make, so it is
            // also the one that is recorded.
            self.record_pruned(&pruned, DynamicFilterCheckpoint::Footer);
            RowGroupPlan::WholeRange {
                surviving: (surviving.len() != footer.row_groups().len()).then_some(surviving),
                pending: true,
            }
        };

        self.footer = Some(footer);
        self.state = ReaderState::Open {
            reader: None,
            plan,
            binding,
            page_schema,
            checks,
            positions_required,
        };
        Ok(())
    }

    /// Judge every row group of this split once, keeping the two sides apart.
    ///
    /// The caller decides whether this consultation is the one that gets
    /// recorded: a whole-file split re-asks per row group and records there,
    /// so recording here as well would report one prune twice.
    fn surviving_row_groups(&self, footer: &ParquetMetadataInspection) -> (Vec<u32>, Vec<u32>) {
        let mut surviving = Vec::with_capacity(footer.row_groups().len());
        let mut pruned = Vec::new();
        for layout in footer.row_groups() {
            match consult_dynamic_filter(
                &self.dynamic_filter,
                layout.ordinal,
                &self.dynamic_filter_columns,
                footer,
            ) {
                DynamicFilterVerdict::ReadEverything => surviving.push(layout.ordinal),
                DynamicFilterVerdict::SkipRowGroup => pruned.push(layout.ordinal),
            }
        }
        (surviving, pruned)
    }

    /// Log and report the row groups this split proved it need not read.
    fn record_pruned(&mut self, ordinals: &[u32], checkpoint: DynamicFilterCheckpoint) {
        for ordinal in ordinals {
            let row_group = self.dynamic_filter.row_group(*ordinal);
            emit_row_group_pruned_marker(row_group, checkpoint);
            self.pruned_row_groups.push(row_group);
        }
    }

    /// Open the reader for the next run of row groups this split must read.
    ///
    /// Returns `false` once the plan is exhausted. Every row group is judged
    /// against the filter as it stands at this moment, which is what lets a
    /// filter that arrived mid-split prune the row groups that follow.
    fn open_next_run(&mut self) -> Result<bool, ConnectorError> {
        let Some(footer) = self.footer.clone() else {
            return Ok(false);
        };
        // Read the plan out first: judging a row group needs the filter and the
        // pruned-row-group log, which cannot be borrowed while the plan is.
        let Some((candidates, checkpoint)) = self.take_next_candidates() else {
            return Ok(false);
        };

        let row_groups = match candidates {
            NextCandidates::WholeRange(surviving) => surviving,
            NextCandidates::PerRowGroup(candidates) => {
                // A filter that can still tighten is re-asked before every row
                // group that has not been read, so one run is one row group. A
                // filter that cannot tighten answers the whole remainder now.
                let stop_after_first = !self.dynamic_filter.is_complete();
                let mut run = Vec::new();
                let mut rest = std::collections::VecDeque::new();
                let mut pruned = Vec::new();
                for ordinal in candidates {
                    if !run.is_empty() && stop_after_first {
                        rest.push_back(ordinal);
                        continue;
                    }
                    match consult_dynamic_filter(
                        &self.dynamic_filter,
                        ordinal,
                        &self.dynamic_filter_columns,
                        &footer,
                    ) {
                        DynamicFilterVerdict::ReadEverything => run.push(ordinal),
                        DynamicFilterVerdict::SkipRowGroup => pruned.push(ordinal),
                    }
                }
                self.restore_remaining(rest);
                self.record_pruned(&pruned, checkpoint);
                if run.is_empty() {
                    return Ok(false);
                }
                // Reading every row group of the file is what this split did
                // before any filter existed, so it is expressed the same way.
                (run.len() != footer.row_groups().len()).then_some(run)
            }
        };

        let reader = self.open_reader(row_groups)?;
        let ReaderState::Open { reader: slot, .. } = &mut self.state else {
            return Ok(false);
        };
        *slot = Some(reader);
        Ok(true)
    }

    /// Take the row groups the plan offers next, marking that offer consumed.
    fn take_next_candidates(&mut self) -> Option<(NextCandidates, DynamicFilterCheckpoint)> {
        let ReaderState::Open { plan, .. } = &mut self.state else {
            return None;
        };
        match plan {
            RowGroupPlan::WholeRange { surviving, pending } => {
                if !*pending {
                    return None;
                }
                *pending = false;
                Some((
                    NextCandidates::WholeRange(surviving.clone()),
                    DynamicFilterCheckpoint::FirstRowGroup,
                ))
            }
            RowGroupPlan::PerRowGroup {
                remaining,
                first_pending,
            } => {
                if remaining.is_empty() {
                    return None;
                }
                let checkpoint = if *first_pending {
                    *first_pending = false;
                    DynamicFilterCheckpoint::FirstRowGroup
                } else {
                    DynamicFilterCheckpoint::NextRowGroup
                };
                let candidates = remaining.iter().copied().collect();
                remaining.clear();
                Some((NextCandidates::PerRowGroup(candidates), checkpoint))
            }
        }
    }

    /// Give the plan back the row groups this run did not claim.
    fn restore_remaining(&mut self, rest: std::collections::VecDeque<u32>) {
        if let ReaderState::Open {
            plan: RowGroupPlan::PerRowGroup { remaining, .. },
            ..
        } = &mut self.state
        {
            *remaining = rest;
        }
    }

    /// Open one Parquet reader over an explicit row-group selection.
    ///
    /// `None` means "whatever the range selects", which is byte for byte what
    /// this split did before any filter existed.
    fn open_reader(
        &self,
        row_groups: Option<Vec<u32>>,
    ) -> Result<Box<dyn FileBatchReader>, ConnectorError> {
        let access = self.access_binding.resolve_access(self.split.path())?;
        let file_size = u64::try_from(self.split.file_size()).map_err(|_| {
            corrupt(format!(
                "iceberg data file {} declares a negative size",
                self.split.path()
            ))
        })?;
        let ReaderState::Open { binding, .. } = &self.state else {
            return Err(invalid(
                "iceberg page source opened a reader before binding its columns",
            ));
        };
        let projection = match binding.coverage() {
            // A legacy file has no field ids to project by, so the whole file
            // is opened and the name mapping resolves the columns afterwards.
            FileFieldIdCoverage::None => FileProjection::All,
            FileFieldIdCoverage::Complete => {
                FileProjection::FieldIds(binding.physical_base_field_ids().to_vec())
            }
        };
        let range = if self.split.is_whole_file() {
            FileReadRange::WholeFile
        } else {
            let start = u64::try_from(self.split.start())
                .map_err(|_| corrupt("iceberg split start offset is negative".to_owned()))?;
            let length = u64::try_from(self.split.length())
                .map_err(|_| corrupt("iceberg split length is negative".to_owned()))?;
            FileReadRange::bounded(start, length).map_err(map_file_error)?
        };
        let file = access
            .bind_location(
                self.split.path(),
                FileIdentity::new(self.split.path(), file_size, None),
            )
            .map_err(map_file_error)?;
        open_file_reader(FileReadRequest {
            file,
            format: novarocks_fs::FileFormat::Parquet,
            range,
            projection,
            budget: self.budget,
            predicates: static_file_predicates(&self.effective_predicate),
            pruning: PhysicalPruning {
                row_groups: row_groups.map(|ordinals| {
                    ordinals
                        .into_iter()
                        .map(|ordinal| ordinal as usize)
                        .collect()
                }),
                pages: Vec::new(),
            },
            options: self.reader_options,
            cache: None,
            context: self.context.clone(),
        })
        .map_err(map_file_error)
    }

    /// The row groups this split proved it need not read.
    pub fn pruned_row_groups(&self) -> &[DynamicFilterRowGroupId] {
        &self.pruned_row_groups
    }

    /// Bind the effective predicate onto the page's channels.
    ///
    /// A constrained column the page does not produce cannot be proved here,
    /// and silently dropping it would let the scan return rows the predicate
    /// excludes.
    fn build_predicate_checks(&self) -> Result<Vec<PredicateCheck>, ConnectorError> {
        let Some(domains) = self.effective_predicate.domains() else {
            // An unsatisfiable predicate is handled before any read; reaching
            // it here would mean the split should never have been opened.
            return Err(invalid(
                "iceberg scan opened a split whose effective predicate is unsatisfiable",
            ));
        };
        let mut checks = Vec::with_capacity(domains.len());
        for (column, domain) in domains {
            let channel = self
                .bound_handles
                .iter()
                .position(|handle| handle == column)
                .ok_or_else(|| {
                    unsupported(format!(
                        "iceberg scan constrains field id {} that its page does not produce",
                        column.base_field_id()
                    ))
                })?;
            checks.push(PredicateCheck {
                channel,
                domain: domain.clone(),
            });
        }
        Ok(checks)
    }
}

impl ConnectorPageSource for IcebergParquetPageSource {
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if self.closed || self.finished {
            return Ok(None);
        }
        let began = Instant::now();
        let result = self.produce_page();
        self.read_time_nanos = self
            .read_time_nanos
            .saturating_add(began.elapsed().as_nanos() as u64);
        result
    }

    fn is_finished(&self) -> bool {
        self.finished || self.closed
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: self.completed_bytes,
            completed_positions: self.completed_positions,
            read_time_nanos: self.read_time_nanos,
            file: page_source_file_metrics(self.file_metrics),
        }
    }

    fn memory_usage_bytes(&self) -> u64 {
        let binding = match &self.state {
            ReaderState::Open { binding, .. } => binding.retained_size_in_bytes(),
            ReaderState::NotOpened | ReaderState::Drained => 0,
        };
        self.retained_bytes.saturating_add(binding)
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        // The reader is dropped whatever its own close says: a page source
        // that has been closed must not keep an open file handle alive because
        // the underlying close reported a late I/O error.
        let state = std::mem::replace(&mut self.state, ReaderState::Drained);
        if let ReaderState::Open {
            reader: Some(mut reader),
            ..
        } = state
        {
            self.file_metrics =
                file_metrics_saturating_add(self.retired_file_metrics, reader.metrics_snapshot());
            self.completed_bytes = self.file_metrics.bytes_read;
            reader.close().map_err(map_file_error)?;
        }
        Ok(())
    }
}

impl IcebergParquetPageSource {
    fn produce_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if matches!(self.state, ReaderState::NotOpened) {
            self.open()?;
        }
        loop {
            // A run ends at a row-group boundary, which is exactly where a
            // tighter dynamic filter can still save the next decode.
            if matches!(&self.state, ReaderState::Open { reader: None, .. })
                && !self.open_next_run()?
            {
                self.finished = true;
                self.state = ReaderState::Drained;
                return Ok(None);
            }

            let ReaderState::Open {
                reader: Some(reader),
                plan,
                binding,
                page_schema,
                checks,
                positions_required,
            } = &mut self.state
            else {
                self.finished = true;
                return Ok(None);
            };

            let next = reader.next_batch();
            self.file_metrics =
                file_metrics_saturating_add(self.retired_file_metrics, reader.metrics_snapshot());
            self.completed_bytes = self.file_metrics.bytes_read;
            let next = next.map_err(map_file_error)?;
            let Some(file_batch) = next else {
                // This run is drained. Retire its byte counter and let the plan
                // decide, against the filter as it now stands, what comes next.
                let exhausted = plan.is_exhausted();
                self.retired_bytes = self.completed_bytes;
                self.retired_file_metrics = self.file_metrics;
                let ReaderState::Open { reader, .. } = &mut self.state else {
                    self.finished = true;
                    return Ok(None);
                };
                if let Some(mut reader) = reader.take() {
                    reader.close().map_err(map_file_error)?;
                }
                if exhausted {
                    self.finished = true;
                    self.state = ReaderState::Drained;
                    return Ok(None);
                }
                continue;
            };

            let positions = file_batch.physical_row_positions;
            if *positions_required && positions.is_none() {
                return Err(corrupt(format!(
                    "iceberg data file {} did not report the absolute row position of the split's first row group",
                    self.split.path()
                )));
            }
            if let Some(positions) = positions.as_ref() {
                self.row_window.observe(positions, self.split.path())?;
                if let Some(end) = self.row_window.end_row_position()
                    && end > self.split.file_record_count() as u64
                {
                    return Err(corrupt(format!(
                        "iceberg data file {} produced absolute row position {} beyond its {} records",
                        self.split.path(),
                        end - 1,
                        self.split.file_record_count()
                    )));
                }
            }

            let facts = IcebergSplitFacts {
                path: self.split.path(),
                file_first_row_id: self.split.file_first_row_id(),
                data_sequence_number: self.split.data_sequence_number(),
            };
            let columns = binding.materialize(&file_batch.batch, positions.as_ref(), &facts)?;
            let rows = file_batch.batch.num_rows();
            let runtime_schema = runtime_page_schema(page_schema, &columns);
            let page_batch = RecordBatch::try_new_with_options(
                runtime_schema,
                columns.clone(),
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .map_err(|error| {
                corrupt(format!(
                    "iceberg page assembly for {} failed: {error}",
                    self.split.path()
                ))
            })?;

            // Deletes and the remaining predicate both judge the complete page,
            // hidden suffix included, before the suffix is dropped.
            let mut keep = if self.delete_filter.is_empty() {
                None
            } else {
                let positions = positions.as_ref().ok_or_else(|| {
                    corrupt(format!(
                        "iceberg deletes for {} need absolute row positions",
                        self.split.path()
                    ))
                })?;
                Some(self.delete_filter.evaluate(&page_batch, positions)?)
            };
            for check in checks.iter() {
                let mask = evaluate_domain(
                    page_batch.column(check.channel),
                    &check.domain,
                    self.split.path(),
                )?;
                keep = Some(match keep {
                    None => mask,
                    Some(previous) => arrow::compute::and(&previous, &mask).map_err(|error| {
                        corrupt(format!("iceberg row predicate conjunction failed: {error}"))
                    })?,
                });
            }

            let mut page = SourcePage::try_new(rows, columns)?;
            if let Some(keep) = keep {
                let selected = surviving_positions(&keep);
                if selected.len() != rows {
                    page.select_positions(&selected)?;
                }
            }
            page.truncate_channels(self.prefix_len)?;
            if page.position_count() == 0 {
                // Everything in this batch was deleted or filtered out. The
                // split is not finished, so the next row group is read rather
                // than reporting an empty page as if it were data.
                continue;
            }
            self.completed_positions = self
                .completed_positions
                .saturating_add(page.position_count() as u64);
            return Ok(Some(page));
        }
    }
}

fn file_metrics_saturating_add(
    left: novarocks_fs::FileMetricsSnapshot,
    right: novarocks_fs::FileMetricsSnapshot,
) -> novarocks_fs::FileMetricsSnapshot {
    novarocks_fs::FileMetricsSnapshot {
        bytes_read: left.bytes_read.saturating_add(right.bytes_read),
        read_requests: left.read_requests.saturating_add(right.read_requests),
        rows_decoded: left.rows_decoded.saturating_add(right.rows_decoded),
        batches_delivered: left
            .batches_delivered
            .saturating_add(right.batches_delivered),
        cache_hits: left.cache_hits.saturating_add(right.cache_hits),
        cache_misses: left.cache_misses.saturating_add(right.cache_misses),
        io_time_ns: left.io_time_ns.saturating_add(right.io_time_ns),
        decode_time_ns: left.decode_time_ns.saturating_add(right.decode_time_ns),
        row_groups_read: left.row_groups_read.saturating_add(right.row_groups_read),
        row_groups_pruned: left
            .row_groups_pruned
            .saturating_add(right.row_groups_pruned),
        delayed_materialization_ranges: left
            .delayed_materialization_ranges
            .saturating_add(right.delayed_materialization_ranges),
        page_index_attempts: left
            .page_index_attempts
            .saturating_add(right.page_index_attempts),
        page_index_fallbacks: left
            .page_index_fallbacks
            .saturating_add(right.page_index_fallbacks),
        page_index_rows_considered: left
            .page_index_rows_considered
            .saturating_add(right.page_index_rows_considered),
        page_index_rows_pruned: left
            .page_index_rows_pruned
            .saturating_add(right.page_index_rows_pruned),
    }
}

fn page_source_file_metrics(metrics: novarocks_fs::FileMetricsSnapshot) -> PageSourceFileMetrics {
    PageSourceFileMetrics {
        bytes_read: metrics.bytes_read,
        read_requests: metrics.read_requests,
        rows_decoded: metrics.rows_decoded,
        batches_delivered: metrics.batches_delivered,
        cache_hits: metrics.cache_hits,
        cache_misses: metrics.cache_misses,
        io_time_ns: metrics.io_time_ns,
        decode_time_ns: metrics.decode_time_ns,
        row_groups_read: metrics.row_groups_read,
        row_groups_pruned: metrics.row_groups_pruned,
        delayed_materialization_ranges: metrics.delayed_materialization_ranges,
        page_index_attempts: metrics.page_index_attempts,
        page_index_fallbacks: metrics.page_index_fallbacks,
        page_index_rows_considered: metrics.page_index_rows_considered,
        page_index_rows_pruned: metrics.page_index_rows_pruned,
    }
}

fn surviving_positions(keep: &BooleanArray) -> Vec<u32> {
    (0..keep.len())
        .filter(|row| !keep.is_null(*row) && keep.value(*row))
        .map(|row| row as u32)
        .collect()
}

/// Prove one column domain row by row.
fn evaluate_domain(
    column: &ArrayRef,
    domain: &Domain,
    path: &str,
) -> Result<BooleanArray, ConnectorError> {
    let null_allowed = domain.null_allowed();
    let values = domain.values();
    let mut keep = Vec::with_capacity(column.len());
    for row in 0..column.len() {
        if column.is_null(row) {
            keep.push(null_allowed);
            continue;
        }
        let value = connector_value_at(column, domain.value_type(), row, path)?;
        keep.push(values.contains_value(&value)?);
    }
    Ok(BooleanArray::from(keep))
}

/// Read one exactly typed value out of a physical column.
///
/// The domain's own type is the authority: a column whose carrier cannot
/// produce that type is a binding failure, never a coercion opportunity.
fn connector_value_at(
    column: &ArrayRef,
    value_type: ConnectorValueType,
    row: usize,
    path: &str,
) -> Result<ConnectorValue, ConnectorError> {
    fn downcast<'a, T: 'static>(column: &'a ArrayRef, path: &str) -> Result<&'a T, ConnectorError> {
        column.as_any().downcast_ref::<T>().ok_or_else(|| {
            corrupt(format!(
                "iceberg predicate column of {path} has carrier {:?}, which cannot be compared",
                column.data_type()
            ))
        })
    }

    Ok(match value_type {
        // No wire Value arm exists for it and the decoder refuses a domain
        // typed this way, so reaching here means a predicate was built over a
        // column the engine already proved has no comparable counterpart.
        ConnectorValueType::NonComparable => {
            return Err(corrupt(format!(
                "iceberg predicate column of {path} is typed non-comparable, which has no value"
            )));
        }
        ConnectorValueType::Boolean => {
            ConnectorValue::Boolean(downcast::<BooleanArray>(column, path)?.value(row))
        }
        // Only an engine-derived column is eight-bit, and no such column is
        // read from a file, so no predicate can name one here.
        ConnectorValueType::TinyInt => {
            return Err(corrupt(format!(
                "iceberg predicate column of {path} is eight-bit, which no iceberg field is"
            )));
        }
        ConnectorValueType::Integer => {
            ConnectorValue::Integer(downcast::<Int32Array>(column, path)?.value(row))
        }
        ConnectorValueType::BigInt => {
            ConnectorValue::BigInt(downcast::<Int64Array>(column, path)?.value(row))
        }
        ConnectorValueType::Real => {
            ConnectorValue::Real(downcast::<Float32Array>(column, path)?.value(row))
        }
        ConnectorValueType::Double => {
            ConnectorValue::Double(downcast::<Float64Array>(column, path)?.value(row))
        }
        ConnectorValueType::Decimal { precision, scale } => ConnectorValue::Decimal {
            unscaled: downcast::<Decimal128Array>(column, path)?.value(row),
            precision,
            scale,
        },
        ConnectorValueType::Date => {
            ConnectorValue::Date(downcast::<Date32Array>(column, path)?.value(row))
        }
        ConnectorValueType::TimeMicros => {
            ConnectorValue::TimeMicros(downcast::<Time64MicrosecondArray>(column, path)?.value(row))
        }
        ConnectorValueType::TimestampMicros => ConnectorValue::TimestampMicros(
            downcast::<TimestampMicrosecondArray>(column, path)?.value(row),
        ),
        ConnectorValueType::TimestampTzMicros => ConnectorValue::TimestampTzMicros(
            downcast::<TimestampMicrosecondArray>(column, path)?.value(row),
        ),
        ConnectorValueType::TimestampNanos => ConnectorValue::TimestampNanos(
            downcast::<TimestampNanosecondArray>(column, path)?.value(row),
        ),
        ConnectorValueType::TimestampTzNanos => ConnectorValue::TimestampTzNanos(
            downcast::<TimestampNanosecondArray>(column, path)?.value(row),
        ),
        ConnectorValueType::Varchar => match column.data_type() {
            arrow::datatypes::DataType::LargeUtf8 => ConnectorValue::Varchar(Arc::from(
                downcast::<LargeStringArray>(column, path)?.value(row),
            )),
            _ => ConnectorValue::Varchar(Arc::from(
                downcast::<StringArray>(column, path)?.value(row),
            )),
        },
        ConnectorValueType::Varbinary => match column.data_type() {
            arrow::datatypes::DataType::LargeBinary => ConnectorValue::Varbinary(Arc::from(
                downcast::<LargeBinaryArray>(column, path)?.value(row),
            )),
            _ => ConnectorValue::Varbinary(Arc::from(
                downcast::<BinaryArray>(column, path)?.value(row),
            )),
        },
        ConnectorValueType::Uuid => {
            let bytes = downcast::<FixedSizeBinaryArray>(column, path)?.value(row);
            ConnectorValue::Uuid(<[u8; 16]>::try_from(bytes).map_err(|_| {
                corrupt(format!(
                    "iceberg uuid column of {path} is not sixteen bytes wide"
                ))
            })?)
        }
        ConnectorValueType::Fixed { .. } => ConnectorValue::Fixed(Arc::from(
            downcast::<FixedSizeBinaryArray>(column, path)?.value(row),
        )),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::ConnectorErrorKind;
    use novarocks_spi::connector::read_stack::{DynamicFilter, SplitWeight, TupleDomain};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::iceberg::spec::{
        NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type,
    };
    use crate::typed_read::schema_binding::IcebergMetadataColumn;
    use crate::typed_read::split::{
        IcebergDeleteFile, IcebergDeleteFileContent, IcebergDeleteFileParams, IcebergSplitParams,
    };
    use crate::typed_read::table_handle::{IcebergTableHandle, IcebergTableHandleParams};

    const ROWS_PER_GROUP: usize = 4;

    fn tokio_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().expect("build Tokio runtime")
    }

    fn read_context(runtime: &tokio::runtime::Runtime) -> FileReadContext {
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(60)),
            runtime: file_runtime,
            task_spawner,
        }
    }

    fn read_binding(runtime: &tokio::runtime::Runtime) -> IcebergReadBinding {
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        IcebergReadBinding::new(None, FsAccessResolver::new(), file_runtime, task_spawner)
    }

    fn iceberg_schema() -> IcebergSchema {
        IcebergSchema::builder()
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
            .expect("frozen table schema")
    }

    #[test]
    fn typed_bigint_lower_bound_becomes_a_physical_file_predicate() {
        let schema = iceberg_schema();
        let column = IcebergColumnHandle::base_column_of(&schema, 1).expect("id handle");
        let range = novarocks_spi::connector::read_stack::Range::try_new(
            ConnectorValueType::BigInt,
            Bound::Inclusive(ConnectorValue::BigInt(199_000)),
            Bound::Unbounded,
        )
        .expect("lower bound");
        let domain = Domain::new(
            novarocks_spi::connector::read_stack::ValueSet::of_ranges(
                ConnectorValueType::BigInt,
                vec![range],
            )
            .expect("value set"),
            false,
        );
        let predicate = TupleDomain::with_column_domains([(column, domain)].into_iter().collect())
            .expect("tuple domain");

        let predicates = static_file_predicates(&predicate);

        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].column(), "id");
        assert_eq!(predicates[0].physical_field_id(), Some(1));
        assert_eq!(predicates[0].source(), ScanPredicateSource::Static);
        assert_eq!(
            predicates[0].domain(),
            &ScanPredicateDomain::Range {
                op: MinMaxPredicateOp::Ge,
                value: MinMaxPredicateValue::Int64(199_000),
            }
        );
    }

    #[test]
    fn nullable_typed_domain_stays_residual() {
        let schema = iceberg_schema();
        let column = IcebergColumnHandle::base_column_of(&schema, 1).expect("id handle");
        let domain = Domain::new(
            novarocks_spi::connector::read_stack::ValueSet::of_values(
                ConnectorValueType::BigInt,
                vec![ConnectorValue::BigInt(7)],
            )
            .expect("value set"),
            true,
        );
        let predicate = TupleDomain::with_column_domains([(column, domain)].into_iter().collect())
            .expect("tuple domain");

        assert!(static_file_predicates(&predicate).is_empty());
    }

    fn arrow_file_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(
                [(PARQUET_FIELD_ID_META_KEY.to_owned(), "1".to_owned())]
                    .into_iter()
                    .collect(),
            ),
            Field::new("region", DataType::Utf8, true).with_metadata(
                [(PARQUET_FIELD_ID_META_KEY.to_owned(), "2".to_owned())]
                    .into_iter()
                    .collect(),
            ),
        ]))
    }

    /// Write `groups * ROWS_PER_GROUP` rows into that many Parquet row groups.
    fn write_data_file(path: &Path, groups: usize) -> u64 {
        let schema = arrow_file_schema();
        let file = fs::File::create(path).expect("create data file");
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
            .build();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
            .expect("create parquet writer");
        for group in 0..groups {
            let base = (group * ROWS_PER_GROUP) as i64;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(
                        (0..ROWS_PER_GROUP as i64)
                            .map(|row| base + row)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        (0..ROWS_PER_GROUP)
                            .map(|row| format!("r{}", base as usize + row))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("build data batch");
            writer.write(&batch).expect("write data batch");
            writer.flush().expect("close row group");
        }
        writer.close().expect("close parquet writer");
        fs::metadata(path).expect("stat data file").len()
    }

    /// The Arrow schema of a data file that materializes its row lineage.
    ///
    /// This is the shape a rewrite produces: the two reserved field IDs are
    /// real Parquet columns alongside the table's own. Both are nullable here
    /// because Iceberg declares them optional and a null row means "inherit",
    /// which is the half of the rule a required column could never exercise.
    fn arrow_row_lineage_file_schema() -> Arc<ArrowSchema> {
        fn with_field_id(field: Field, field_id: i32) -> Field {
            field.with_metadata(
                [(PARQUET_FIELD_ID_META_KEY.to_owned(), field_id.to_string())]
                    .into_iter()
                    .collect(),
            )
        }
        Arc::new(ArrowSchema::new(vec![
            with_field_id(Field::new("id", DataType::Int64, false), 1),
            with_field_id(Field::new("region", DataType::Utf8, true), 2),
            with_field_id(
                Field::new(
                    crate::row_lineage_synth::ICEBERG_ROW_ID_COL,
                    DataType::Int64,
                    true,
                ),
                crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
            ),
            with_field_id(
                Field::new(
                    crate::row_lineage_synth::ICEBERG_LAST_UPDATED_SEQ_COL,
                    DataType::Int64,
                    true,
                ),
                crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            ),
        ]))
    }

    /// Write one row group whose rows carry the lineage a rewrite preserved.
    ///
    /// The four rows deliberately cover every corner of the rule: both stored,
    /// only `_row_id` stored, only `_last_updated_sequence_number` stored, and
    /// neither.
    fn write_row_lineage_data_file(path: &Path) -> u64 {
        let schema = arrow_row_lineage_file_schema();
        let file = fs::File::create(path).expect("create data file");
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
            .build();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
            .expect("create parquet writer");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![0_i64, 1, 2, 3])),
                Arc::new(StringArray::from(vec!["r0", "r1", "r2", "r3"])),
                Arc::new(Int64Array::from(vec![
                    Some(70_i64),
                    Some(71),
                    Some(72),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(2), None])),
            ],
        )
        .expect("build data batch");
        writer.write(&batch).expect("write data batch");
        writer.close().expect("close parquet writer");
        fs::metadata(path).expect("stat data file").len()
    }

    /// The byte offset at which each row group's data starts.
    fn row_group_offsets(path: &Path) -> Vec<u64> {
        let file = fs::File::open(path).expect("open data file");
        let reader =
            parquet::file::reader::SerializedFileReader::new(file).expect("open parquet footer");
        parquet::file::reader::FileReader::metadata(&reader)
            .row_groups()
            .iter()
            .map(|group| {
                let column = group.column(0);
                let offset = column
                    .dictionary_page_offset()
                    .unwrap_or_else(|| column.data_page_offset())
                    .min(column.data_page_offset());
                u64::try_from(offset).expect("nonnegative row group offset")
            })
            .collect()
    }

    fn table_handle(schema: &IcebergSchema, partitioned: bool) -> IcebergTableHandle {
        let mut partition_spec_jsons = std::collections::BTreeMap::new();
        let spec = if partitioned {
            crate::iceberg::spec::PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("region", "region", Transform::Identity)
                .expect("identity partition field")
                .build()
                .expect("partition spec")
        } else {
            crate::iceberg::spec::PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .build()
                .expect("unpartitioned spec")
        };
        partition_spec_jsons.insert(
            0,
            serde_json::to_string(&spec).expect("serialize partition spec"),
        );
        IcebergTableHandle::try_new(IcebergTableHandleParams {
            schema_table_name: novarocks_spi::connector::read_stack::SchemaTableName::try_new(
                "sales", "orders",
            )
            .expect("schema table name"),
            snapshot_id: Some(11),
            table_schema_json: serde_json::to_string(schema).expect("serialize schema"),
            spec_id: Some(0),
            partition_spec_jsons,
            format_version: 2,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            projected_columns: Default::default(),
            name_mapping_json: None,
            table_location: "/tmp/iceberg/orders".to_owned(),
            storage_properties: Default::default(),
            pinned_data_files: None,
        })
        .expect("table handle")
    }

    struct SplitOptions {
        start: i64,
        length: i64,
        file_size: i64,
        file_record_count: i64,
        format: IcebergFileFormat,
        partition_data_json: String,
        deletes: Vec<IcebergDeleteFile>,
        first_row_id: Option<i64>,
        decryption: Option<ParquetFileDecryptionData>,
    }

    impl SplitOptions {
        fn whole_file(file_size: u64, records: i64) -> Self {
            Self {
                start: 0,
                length: file_size as i64,
                file_size: file_size as i64,
                file_record_count: records,
                format: IcebergFileFormat::Parquet,
                partition_data_json: "{}".to_owned(),
                deletes: Vec::new(),
                first_row_id: None,
                decryption: None,
            }
        }
    }

    fn build_split(name: &str, options: SplitOptions) -> IcebergSplit {
        IcebergSplit::try_new(IcebergSplitParams {
            path: name.to_owned(),
            start: options.start,
            length: options.length,
            file_size: options.file_size,
            file_record_count: options.file_record_count,
            file_format: options.format,
            partition_spec_id: 0,
            partition_data_json: options.partition_data_json,
            deletes: options.deletes,
            file_statistics_domain: TupleDomain::all(),
            data_sequence_number: Some(3),
            file_first_row_id: options.first_row_id,
            decryption_data: options.decryption,
            split_weight: SplitWeight::STANDARD,
            affinity_key: None,
        })
        .expect("split")
    }

    struct Harness {
        _runtime: tokio::runtime::Runtime,
        _directory: tempfile::TempDir,
        binding: IcebergReadBinding,
        context: FileReadContext,
        footers: Arc<ParquetFooterCache>,
        delete_manager: Arc<DeleteManager>,
        file_size: u64,
        offsets: Vec<u64>,
        file_name: String,
    }

    fn harness(groups: usize) -> Harness {
        harness_of(|path| write_data_file(path, groups))
    }

    /// A harness over a data file this test wrote itself.
    fn harness_of(write: impl FnOnce(&Path) -> u64) -> Harness {
        let runtime = tokio_runtime();
        let directory = tempfile::tempdir().expect("temporary directory");
        let data_path = directory.path().join("data.parquet");
        let file_size = write(&data_path);
        let offsets = row_group_offsets(&data_path);
        let context = read_context(&runtime);
        let access_binding = read_binding(&runtime);
        let delete_manager = Arc::new(DeleteManager::new(access_binding.clone(), context.clone()));
        Harness {
            _runtime: runtime,
            _directory: directory,
            binding: access_binding,
            context,
            footers: Arc::new(ParquetFooterCache::new()),
            delete_manager,
            file_size,
            offsets,
            file_name: data_path.to_string_lossy().to_string(),
        }
    }

    impl Harness {
        fn page_source(
            &self,
            split: &IcebergSplit,
            handle: &IcebergTableHandle,
            columns: &[IcebergColumnHandle],
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            self.page_source_with_mode(split, handle, columns, DeleteEvaluationMode::ExcludeDeleted)
        }

        fn page_source_with_mode(
            &self,
            split: &IcebergSplit,
            handle: &IcebergTableHandle,
            columns: &[IcebergColumnHandle],
            delete_mode: DeleteEvaluationMode,
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            let relation = IcebergReadRelation::of_table(handle, split.partition_spec_id())?;
            create_iceberg_page_source(IcebergPageSourceRequest {
                relation: &relation,
                split,
                columns,
                delete_manager: Arc::clone(&self.delete_manager),
                delete_mode,
                footers: Arc::clone(&self.footers),
                access_binding: self.binding.clone(),
                context: self.context.clone(),
                budget: FileReadBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero"),
                    max_bytes: NonZeroUsize::new(8 * 1024 * 1024).expect("nonzero"),
                },
                reader_options: FileReaderOptions::default(),
                scheduled_split_sequence_id: 0,
                dynamic_filter: Arc::new(
                    novarocks_spi::connector::read_stack::CompleteAllDynamicFilter::new(
                        std::collections::BTreeSet::new(),
                    ),
                ) as Arc<IcebergDynamicFilter>,
            })
        }
    }

    fn drain_ids(source: &mut Box<dyn ConnectorPageSource>) -> Vec<i64> {
        let mut ids = Vec::new();
        while !source.is_finished() {
            let Some(page) = source.next_source_page().expect("page") else {
                continue;
            };
            let (rows, columns) = page.into_columns().expect("materialize");
            assert_eq!(columns[0].len(), rows);
            let values = columns[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 ids");
            ids.extend(values.values().iter().copied());
        }
        ids
    }

    /// Drain every page and return the first `count` output columns as i64.
    fn drain_i64_columns(source: &mut Box<dyn ConnectorPageSource>, count: usize) -> Vec<Vec<i64>> {
        let mut out = vec![Vec::new(); count];
        while !source.is_finished() {
            let Some(page) = source.next_source_page().expect("page") else {
                continue;
            };
            let (rows, columns) = page.into_columns().expect("materialize");
            for (ordinal, values) in out.iter_mut().enumerate() {
                let column = columns[ordinal]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 column");
                assert_eq!(column.len(), rows);
                assert_eq!(column.null_count(), 0, "column {ordinal} reports no nulls");
                values.extend(column.values().iter().copied());
            }
        }
        out
    }

    /// The handle a scan names a hidden metadata column with. It mirrors what
    /// the boundary publishes: optional, and typed by the column itself.
    fn metadata_handle(metadata: IcebergMetadataColumn) -> IcebergColumnHandle {
        IcebergColumnHandle::base_column(&NestedField::optional(
            metadata.field_id(),
            metadata.column_name(),
            Type::Primitive(metadata.declared_type()),
        ))
        .expect("metadata column handle")
    }

    fn id_column(schema: &IcebergSchema) -> Vec<IcebergColumnHandle> {
        vec![IcebergColumnHandle::base_column_of(schema, 1).expect("id handle")]
    }

    // ------------------------------------------------------ dynamic filtering

    /// A dynamic filter a test drives directly.
    ///
    /// It answers exactly what the test tells it to and records every question
    /// it was asked, so a test can tell "never consulted" apart from "consulted
    /// and kept".
    struct TestDynamicFilter {
        covered: std::collections::BTreeSet<IcebergColumnHandle>,
        state: Mutex<TestFilterState>,
        asked: Mutex<Vec<ColumnValueBounds>>,
    }

    struct TestFilterState {
        /// Row groups whose minimum id is inside this set are impossible.
        rejected_minimums: Vec<i64>,
        /// When set, every row group is impossible.
        reject_everything: bool,
        answer_unknown: bool,
        complete: bool,
    }

    impl TestDynamicFilter {
        fn new(covered_field_id: i32, schema: &IcebergSchema) -> Arc<Self> {
            let handle =
                IcebergColumnHandle::base_column_of(schema, covered_field_id).expect("handle");
            Arc::new(Self {
                covered: std::collections::BTreeSet::from([handle]),
                state: Mutex::new(TestFilterState {
                    rejected_minimums: Vec::new(),
                    reject_everything: false,
                    answer_unknown: false,
                    complete: false,
                }),
                asked: Mutex::new(Vec::new()),
            })
        }

        fn reject_row_groups_starting_at(&self, minimums: &[i64]) {
            self.state.lock().expect("filter state").rejected_minimums = minimums.to_vec();
        }

        fn reject_everything(&self) {
            self.state.lock().expect("filter state").reject_everything = true;
        }

        fn answer_unknown(&self) {
            self.state.lock().expect("filter state").answer_unknown = true;
        }

        fn questions(&self) -> Vec<ColumnValueBounds> {
            self.asked.lock().expect("asked").clone()
        }
    }

    impl DynamicFilter<IcebergColumnHandle> for TestDynamicFilter {
        fn columns_covered(&self) -> &std::collections::BTreeSet<IcebergColumnHandle> {
            &self.covered
        }

        fn current_predicate(&self) -> TupleDomain<IcebergColumnHandle> {
            TupleDomain::all()
        }

        fn is_complete(&self) -> bool {
            self.state.lock().expect("filter state").complete
        }

        fn is_awaitable(&self) -> bool {
            false
        }

        fn bounds_may_match(
            &self,
            column: &IcebergColumnHandle,
            bounds: &ColumnValueBounds,
        ) -> BoundsMatch {
            self.asked.lock().expect("asked").push(bounds.clone());
            if !self.covered.contains(column) {
                return BoundsMatch::Unknown;
            }
            let state = self.state.lock().expect("filter state");
            if state.answer_unknown {
                return BoundsMatch::Unknown;
            }
            if state.reject_everything {
                return BoundsMatch::Impossible;
            }
            let Some(ConnectorValue::BigInt(min)) = bounds.min else {
                return BoundsMatch::Unknown;
            };
            if state.rejected_minimums.contains(&min) {
                BoundsMatch::Impossible
            } else {
                BoundsMatch::Possible
            }
        }
    }

    impl Harness {
        fn page_source_with_filter(
            &self,
            split: &IcebergSplit,
            handle: &IcebergTableHandle,
            columns: &[IcebergColumnHandle],
            dynamic_filter: Arc<IcebergDynamicFilter>,
            scheduled_split_sequence_id: u64,
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            let relation = IcebergReadRelation::of_table(handle, split.partition_spec_id())?;
            create_iceberg_page_source(IcebergPageSourceRequest {
                relation: &relation,
                split,
                columns,
                delete_manager: Arc::clone(&self.delete_manager),
                delete_mode: DeleteEvaluationMode::ExcludeDeleted,
                footers: Arc::clone(&self.footers),
                access_binding: self.binding.clone(),
                context: self.context.clone(),
                budget: FileReadBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero"),
                    max_bytes: NonZeroUsize::new(8 * 1024 * 1024).expect("nonzero"),
                },
                reader_options: FileReaderOptions::default(),
                scheduled_split_sequence_id,
                dynamic_filter,
            })
        }
    }

    fn whole_file_split(harness: &Harness, groups: usize) -> IcebergSplit {
        build_split(
            &harness.file_name,
            SplitOptions::whole_file(harness.file_size, (groups * ROWS_PER_GROUP) as i64),
        )
    }

    #[test]
    fn a_pruned_row_group_is_named_by_its_split_sequence_and_ordinal() {
        let live = LiveDynamicFilter::new(
            Arc::new(
                novarocks_spi::connector::read_stack::CompleteAllDynamicFilter::new(
                    std::collections::BTreeSet::new(),
                ),
            ) as Arc<IcebergDynamicFilter>,
            41,
        );
        assert_eq!(
            live.row_group(2),
            DynamicFilterRowGroupId {
                scheduled_split_sequence_id: 41,
                row_group_ordinal: 2,
            }
        );
        assert_eq!(live.scheduled_split_sequence_id(), 41);
    }

    #[test]
    fn a_filter_present_before_the_first_row_group_prunes_a_disjoint_one() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = whole_file_split(&harness, 3);
        let filter = TestDynamicFilter::new(1, &schema);
        // The first row group holds ids 0..3, so rejecting minimum 0 rejects it.
        filter.reject_row_groups_starting_at(&[0]);
        let mut source = harness
            .page_source_with_filter(
                &split,
                &handle,
                &id_column(&schema),
                Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
                7,
            )
            .expect("page source");
        let ids = drain_ids(&mut source);
        // Rows that were never read cannot appear. This page source never drops
        // a row for the dynamic filter, so a missing row group proves a skip.
        assert_eq!(
            ids,
            (ROWS_PER_GROUP as i64..(3 * ROWS_PER_GROUP) as i64).collect::<Vec<_>>()
        );
        assert!(
            filter
                .questions()
                .iter()
                .any(|bounds| bounds.min == Some(ConnectorValue::BigInt(0))),
            "the first row group's own bounds must be the ones judged"
        );
    }

    #[test]
    fn a_filter_arriving_mid_split_prunes_only_the_not_yet_read_row_groups() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = whole_file_split(&harness, 3);
        let filter = TestDynamicFilter::new(1, &schema);
        let mut source = harness
            .page_source_with_filter(
                &split,
                &handle,
                &id_column(&schema),
                Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
                0,
            )
            .expect("page source");

        // The first row group is read while nothing is constrained.
        let first = source
            .next_source_page()
            .expect("page")
            .expect("the first row group is readable");
        assert_eq!(first.position_count(), ROWS_PER_GROUP);

        // The filter tightens after that row group was already decoded.
        filter.reject_everything();
        let rest = drain_ids(&mut source);
        assert!(
            rest.is_empty(),
            "a filter that arrives mid-split must prune every row group that has not been read"
        );
        assert!(source.is_finished());
    }

    #[test]
    fn an_answer_of_unknown_keeps_every_row_group() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = whole_file_split(&harness, 3);
        let filter = TestDynamicFilter::new(1, &schema);
        filter.answer_unknown();
        let mut source = harness
            .page_source_with_filter(
                &split,
                &handle,
                &id_column(&schema),
                Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
                0,
            )
            .expect("page source");
        let ids = drain_ids(&mut source);
        assert_eq!(ids, (0..(3 * ROWS_PER_GROUP) as i64).collect::<Vec<_>>());
        assert!(
            !filter.questions().is_empty(),
            "an undecided answer is still an answer that was asked for"
        );
    }

    #[test]
    fn a_covered_column_carries_this_row_groups_own_statistics() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = whole_file_split(&harness, 3);
        let filter = TestDynamicFilter::new(1, &schema);
        filter.answer_unknown();
        let mut source = harness
            .page_source_with_filter(
                &split,
                &handle,
                &id_column(&schema),
                Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
                0,
            )
            .expect("page source");
        let _ = drain_ids(&mut source);
        let questions = filter.questions();
        let first = questions
            .iter()
            .find(|bounds| bounds.min == Some(ConnectorValue::BigInt(0)))
            .expect("the first row group is judged on its own bounds");
        assert_eq!(first.max, Some(ConnectorValue::BigInt(3)));
        assert_eq!(first.value_count, Some(ROWS_PER_GROUP as u64));
        assert_eq!(first.null_count, Some(0));
        assert!(first.bounds_are_exact);
    }

    #[test]
    fn a_covered_column_absent_from_the_file_is_never_asked_about() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = whole_file_split(&harness, 3);
        // Field id 99 exists in no Parquet leaf of this file, so it can never
        // contribute a prune and must never be asked about.
        let absent = IcebergSchema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                99,
                "absent",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("a schema naming a column this file does not have");
        let filter = TestDynamicFilter::new(99, &absent);
        filter.reject_everything();
        let mut source = harness
            .page_source_with_filter(
                &split,
                &handle,
                &id_column(&schema),
                Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
                0,
            )
            .expect("page source");
        let ids = drain_ids(&mut source);
        assert_eq!(ids.len(), 3 * ROWS_PER_GROUP);
        assert!(
            filter.questions().is_empty(),
            "a covered column with no leaf in this file is never a question"
        );
    }

    #[test]
    fn a_covered_column_the_scan_does_not_project_still_prunes() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = whole_file_split(&harness, 3);
        // `region` is a real column of the file that this scan does not output.
        // Its footer statistics can still prove a row group impossible.
        let filter = TestDynamicFilter::new(2, &schema);
        filter.reject_everything();
        let mut source = harness
            .page_source_with_filter(
                &split,
                &handle,
                &id_column(&schema),
                Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
                0,
            )
            .expect("page source");
        assert!(drain_ids(&mut source).is_empty());
    }

    #[test]
    fn an_entirely_pruned_split_reads_no_data() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = whole_file_split(&harness, 3);
        let filter = TestDynamicFilter::new(1, &schema);
        filter.reject_everything();
        let mut source = harness
            .page_source_with_filter(
                &split,
                &handle,
                &id_column(&schema),
                Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
                3,
            )
            .expect("page source");
        let ids = drain_ids(&mut source);
        assert!(ids.is_empty());
        assert!(source.is_finished());
        assert_eq!(
            source.metrics().completed_bytes,
            0,
            "a split whose every row group is proven impossible opens no reader"
        );
    }

    #[test]
    fn a_closed_scan_reads_no_footer_and_asks_the_filter_nothing() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = whole_file_split(&harness, 3);
        let filter = TestDynamicFilter::new(1, &schema);
        let mut source = harness
            .page_source_with_filter(
                &split,
                &handle,
                &id_column(&schema),
                Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
                0,
            )
            .expect("page source");
        source.close().expect("close");
        assert!(source.is_finished());
        assert!(source.next_source_page().expect("no page").is_none());
        assert!(
            filter.questions().is_empty(),
            "a closed scan must not consult the filter"
        );
        assert!(
            harness.footers.is_empty().expect("footer cache"),
            "a closed scan must not read a footer"
        );
    }

    #[test]
    fn a_whole_file_split_reads_every_row_with_absolute_positions() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = build_split(
            &harness.file_name,
            SplitOptions::whole_file(harness.file_size, (3 * ROWS_PER_GROUP) as i64),
        );
        let mut source = harness
            .page_source(&split, &handle, &id_column(&schema))
            .expect("page source");
        let ids = drain_ids(&mut source);
        assert_eq!(ids, (0..(3 * ROWS_PER_GROUP) as i64).collect::<Vec<_>>());
        assert!(source.is_finished());
    }

    #[test]
    fn byte_range_selection_takes_a_row_group_at_the_lower_bound_and_not_at_the_upper() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let records = (3 * ROWS_PER_GROUP) as i64;

        // A range that starts exactly on the second row group's first byte and
        // ends exactly on the third's takes only the second: the range is
        // half-open.
        let start = harness.offsets[1];
        let end = harness.offsets[2];
        let split = build_split(
            &harness.file_name,
            SplitOptions {
                start: start as i64,
                length: (end - start) as i64,
                file_size: harness.file_size as i64,
                file_record_count: records,
                ..SplitOptions::whole_file(harness.file_size, records)
            },
        );
        let mut source = harness
            .page_source(&split, &handle, &id_column(&schema))
            .expect("page source");
        let ids = drain_ids(&mut source);
        assert_eq!(
            ids,
            (ROWS_PER_GROUP as i64..(2 * ROWS_PER_GROUP) as i64).collect::<Vec<_>>(),
            "the row group starting at the exclusive upper bound must not be read"
        );
    }

    #[test]
    fn absolute_row_positions_survive_row_group_pruning() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let records = (3 * ROWS_PER_GROUP) as i64;
        let start = harness.offsets[2];
        let split = build_split(
            &harness.file_name,
            SplitOptions {
                start: start as i64,
                length: (harness.file_size - start) as i64,
                file_size: harness.file_size as i64,
                file_record_count: records,
                first_row_id: Some(1_000),
                ..SplitOptions::whole_file(harness.file_size, records)
            },
        );
        // `_row_id` is `first_row_id + absolute position`, so it proves the
        // positions were not renumbered from the start of the split.
        let row_id = IcebergColumnHandle::try_new(
            crate::typed_read::column_handle::IcebergColumnHandleParams {
                base_column_identity: crate::typed_read::column_handle::ColumnIdentity::try_new(
                    crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                    crate::row_lineage_synth::ICEBERG_ROW_ID_COL,
                    crate::typed_read::column_handle::ColumnIdentityCategory::Primitive,
                    Vec::new(),
                )
                .expect("identity"),
                base_type_json: "\"long\"".to_owned(),
                field_id_path: Vec::new(),
                type_json: "\"long\"".to_owned(),
                nullable: false,
                comment: None,
            },
        )
        .expect("row id handle");
        let mut source = harness
            .page_source(&split, &handle, &[row_id])
            .expect("page source");
        let ids = drain_ids(&mut source);
        assert_eq!(
            ids,
            (1_000 + 2 * ROWS_PER_GROUP as i64..1_000 + 3 * ROWS_PER_GROUP as i64)
                .collect::<Vec<_>>()
        );
    }

    /// A rewrite preserves row history only if the reader reads it back.
    ///
    /// The data file materializes both reserved row-lineage columns, which is
    /// what a rewritten v3 file looks like. Synthesizing over them would report
    /// `first_row_id + position` and the rewriting snapshot's own sequence
    /// number for every row -- silently claiming that rows the rewrite merely
    /// copied had changed. Each column falls back on its own, so a row may
    /// inherit one and keep the other.
    #[test]
    fn a_stored_row_lineage_column_is_read_rather_than_synthesized() {
        let harness = harness_of(write_row_lineage_data_file);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = build_split(
            &harness.file_name,
            SplitOptions {
                first_row_id: Some(1_000),
                ..SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64)
            },
        );
        let mut source = harness
            .page_source(
                &split,
                &handle,
                &[
                    metadata_handle(IcebergMetadataColumn::RowId),
                    metadata_handle(IcebergMetadataColumn::LastUpdatedSequenceNumber),
                ],
            )
            .expect("page source");
        let columns = drain_i64_columns(&mut source, 2);

        // Rows 0..3 store a `_row_id`; row 3 stores none and inherits
        // `first_row_id + position` = 1000 + 3.
        assert_eq!(columns[0], vec![70, 71, 72, 1_003], "_row_id");
        // Rows 0 and 2 store a sequence number; rows 1 and 3 inherit the data
        // file's own, which `build_split` freezes at 3.
        assert_eq!(
            columns[1],
            vec![1, 3, 2, 3],
            "_last_updated_sequence_number"
        );
    }

    #[test]
    fn none_is_not_end_of_stream_and_close_is_idempotent() {
        let harness = harness(1);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = build_split(
            &harness.file_name,
            SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64),
        );
        let mut source = harness
            .page_source(&split, &handle, &id_column(&schema))
            .expect("page source");

        assert!(!source.is_finished(), "a fresh page source is not finished");
        let first = source.next_source_page().expect("first page");
        assert!(first.is_some());
        assert!(
            !source.is_finished(),
            "a produced page never terminates the source on its own"
        );
        let second = source.next_source_page().expect("second call");
        assert!(second.is_none(), "the reader is drained");
        assert!(source.is_finished(), "only is_finished is terminal");

        source.close().expect("close");
        source.close().expect("close is idempotent");
        assert!(source.next_source_page().expect("after close").is_none());
    }

    #[test]
    fn a_zero_column_scan_still_counts_its_rows() {
        let harness = harness(2);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = build_split(
            &harness.file_name,
            SplitOptions::whole_file(harness.file_size, (2 * ROWS_PER_GROUP) as i64),
        );
        let mut source = harness
            .page_source(&split, &handle, &[])
            .expect("page source");
        let mut positions = 0usize;
        while !source.is_finished() {
            let Some(page) = source.next_source_page().expect("page") else {
                continue;
            };
            assert_eq!(page.channel_count(), 0, "a zero-column page is legal");
            positions += page.position_count();
        }
        assert_eq!(positions, 2 * ROWS_PER_GROUP);
    }

    #[test]
    fn the_partition_only_fast_path_never_opens_the_data_file() {
        let harness = harness(2);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, true);
        let records = (2 * ROWS_PER_GROUP) as i64;
        let split = build_split(
            "s3://bucket/this/file/does/not/exist.parquet",
            SplitOptions {
                partition_data_json: "{\"1000\":\"emea\"}".to_owned(),
                ..SplitOptions::whole_file(harness.file_size, records)
            },
        );
        let region = IcebergColumnHandle::base_column_of(&schema, 2).expect("region handle");
        let mut source = harness
            .page_source(&split, &handle, &[region])
            .expect("page source");

        let mut positions = 0usize;
        while !source.is_finished() {
            let Some(mut page) = source.next_source_page().expect("page") else {
                continue;
            };
            let column = page.block(0).expect("partition constant").clone();
            let regions = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 constant");
            for row in 0..regions.len() {
                assert_eq!(regions.value(row), "emea");
            }
            positions += page.position_count();
        }
        assert_eq!(
            positions as i64, records,
            "the fast path must account for every record of the file"
        );
        assert!(
            harness.footers.is_empty().expect("footer cache"),
            "the fast path reads no footer at all"
        );
    }

    #[test]
    fn the_partition_only_fast_path_may_emit_zero_column_pages() {
        let harness = harness(1);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, true);
        let split = build_split(
            "s3://bucket/absent.parquet",
            SplitOptions {
                partition_data_json: "{\"1000\":\"emea\"}".to_owned(),
                ..SplitOptions::whole_file(harness.file_size, 7)
            },
        );
        let mut source = harness
            .page_source(&split, &handle, &[])
            .expect("page source");
        let page = source
            .next_source_page()
            .expect("page")
            .expect("a zero-column page is still a page");
        assert_eq!(page.channel_count(), 0);
        assert_eq!(page.position_count(), 7);
        assert!(source.is_finished());
    }

    #[test]
    fn orc_and_avro_are_rejected_at_page_source_admission() {
        let harness = harness(1);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        for format in [IcebergFileFormat::Orc, IcebergFileFormat::Avro] {
            let split = build_split(
                &harness.file_name,
                SplitOptions {
                    format,
                    ..SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64)
                },
            );
            let error = harness
                .page_source(&split, &handle, &id_column(&schema))
                .err()
                .expect("only parquet is implemented");
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported, "{format:?}");
        }
    }

    #[test]
    fn decryption_material_is_rejected_without_leaking_it() {
        let harness = harness(1);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let secret = b"super-secret-key-metadata".to_vec();
        let split = build_split(
            &harness.file_name,
            SplitOptions {
                decryption: Some(
                    ParquetFileDecryptionData::try_new(secret.clone(), Vec::new())
                        .expect("decryption material"),
                ),
                ..SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64)
            },
        );
        let error = harness
            .page_source(&split, &handle, &id_column(&schema))
            .err()
            .expect("modular encryption is not implemented");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        let rendered = format!("{} {:?}", error.message(), error);
        assert!(
            !rendered.contains("super-secret"),
            "key material must never reach a message: {rendered}"
        );
        // The redacted `Debug` of the material itself is the same guarantee.
        let material =
            ParquetFileDecryptionData::try_new(secret, Vec::new()).expect("decryption material");
        assert!(!format!("{material:?}").contains("super-secret"));
    }

    #[test]
    fn a_delete_closure_adds_a_hidden_suffix_that_is_truncated_after_evaluation() {
        let harness = harness(2);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let records = (2 * ROWS_PER_GROUP) as i64;

        // An equality delete on `region` needs `region` in the page even though
        // the scan only asked for `id`.
        let delete_path = std::path::Path::new(&harness.file_name)
            .parent()
            .expect("data directory")
            .join("eq-delete.parquet");
        write_equality_delete(&delete_path, &["r2", "r5"]);
        let delete = IcebergDeleteFile::try_new(IcebergDeleteFileParams {
            content: IcebergDeleteFileContent::EqualityDeletes,
            path: delete_path.to_string_lossy().to_string(),
            format: IcebergFileFormat::Parquet,
            record_count: 2,
            file_size_in_bytes: fs::metadata(&delete_path).expect("stat").len() as i64,
            equality_field_ids: vec![2],
            row_position_lower_bound: None,
            row_position_upper_bound: None,
            data_sequence_number: 9,
            content_offset: None,
            content_size_in_bytes: None,
            referenced_data_file: None,
            decryption_data: None,
        })
        .expect("delete descriptor");

        let split = build_split(
            &harness.file_name,
            SplitOptions {
                deletes: vec![delete],
                ..SplitOptions::whole_file(harness.file_size, records)
            },
        );
        let mut source = harness
            .page_source(&split, &handle, &id_column(&schema))
            .expect("page source");

        let mut ids = Vec::new();
        while !source.is_finished() {
            let Some(page) = source.next_source_page().expect("page") else {
                continue;
            };
            assert_eq!(
                page.channel_count(),
                1,
                "the hidden delete suffix must be dropped before the page leaves"
            );
            let (_, columns) = page.into_columns().expect("materialize");
            let values = columns[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 ids");
            ids.extend(values.values().iter().copied());
        }
        assert_eq!(ids, vec![0, 1, 3, 4, 6, 7]);
    }

    fn write_equality_delete(path: &Path, regions: &[&str]) {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("region", DataType::Utf8, true).with_metadata(
                [(PARQUET_FIELD_ID_META_KEY.to_owned(), "2".to_owned())]
                    .into_iter()
                    .collect(),
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(regions.to_vec()))],
        )
        .expect("build equality delete batch");
        let file = fs::File::create(path).expect("create delete file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
        writer.write(&batch).expect("write delete batch");
        writer.close().expect("close parquet writer");
    }

    #[test]
    fn a_bounded_split_records_its_half_open_row_position_window() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let records = (3 * ROWS_PER_GROUP) as i64;
        let start = harness.offsets[1];
        let end = harness.offsets[2];
        let split = build_split(
            &harness.file_name,
            SplitOptions {
                start: start as i64,
                length: (end - start) as i64,
                file_size: harness.file_size as i64,
                file_record_count: records,
                ..SplitOptions::whole_file(harness.file_size, records)
            },
        );
        let mut window = ReaderPageSourceWithRowPositions::default();
        window
            .observe(
                &UInt64Array::from((4_u64..8).collect::<Vec<_>>()),
                "data.parquet",
            )
            .expect("observe");
        assert_eq!(window.start_row_position, Some(4));
        assert_eq!(window.end_row_position, Some(8));
        assert!(
            window
                .observe(&UInt64Array::from(vec![6_u64]), "data.parquet")
                .is_err(),
            "a revisited row position is corrupt"
        );

        let mut source = harness
            .page_source(&split, &handle, &id_column(&schema))
            .expect("page source");
        let _ = drain_ids(&mut source);
    }
}
