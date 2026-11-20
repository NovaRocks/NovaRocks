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

//! One split, one page stream.
//!
//! A split's reader state owns its cursor, its reader and its buffers; the
//! page stream (see [`stream`]) moves it through awaited steps. Nothing here
//! is process-global and nothing survives an attempt: a replacement attempt
//! builds a new stream over the same frozen split rather than resuming this
//! one.
//!
//! Two invariants shape the reader:
//!
//! * a row position is file-level, absolute, and zero-based -- byte-range
//!   selection narrows which row groups are read, never how rows are numbered;
//! * the scan's ordered columns are the output prefix, and whatever the delete
//!   filter needs is appended as a hidden suffix that is dropped again once the
//!   deletes and the remaining predicate have run over the complete page.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{Field, FieldRef, Schema as ArrowSchema};
use novarocks_fs::{
    FileIdentity, FileProjection, FileReadBudget, FileReadContext, FileReadRange, FileReadRequest,
    FileReaderOptions, FsAccessHandle, MinMaxPredicateOp, MinMaxPredicateValue,
    ParquetMetadataInspection, ParquetPhysicalType, ParquetStatisticsSortOrder,
    ParquetStatisticsValue, PhysicalPruning, PreparedFileInput, ScanPredicate, ScanPredicateDomain,
    ScanPredicateSource, inspect_parquet_metadata, plan_parquet_input_ranges,
};
use novarocks_spi::connector::read_stack::DynamicFilter;
use novarocks_spi::connector::read_stack::{
    Bound, BoundsMatch, ColumnValueBounds, ConnectorPollBudget, ConnectorPreparationControl,
    ConnectorPreparationProgress, ConnectorSplit, ConnectorValue, ConnectorValueType, Domain,
    PageSourceFileMetrics, PageSourceMetrics, SourcePage, TupleDomain,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, StorageAccessDomainId};

use crate::access_binding::IcebergReadBinding;
use crate::file_reader::map_file_error;
use crate::iceberg::spec::{
    Literal, NameMapping, PartitionSpec, PrimitiveType, Schema, Struct, Type,
};

use super::change_window::IcebergChangeWindowHandle;
use super::column_handle::{IcebergColumnHandle, corrupt, invalid, parse_type, unsupported};
use super::delete_manager::{DeleteEvaluationMode, DeleteManager, SplitDeleteFilter};
use super::preparation::{PlannedInput, PreparedRangeCandidate, SuccessorPreparationGroup};

type FooterCache = HashMap<(StorageAccessDomainId, FileIdentity), ParquetMetadataInspection>;
type FooterCacheGuard<'a> = std::sync::MutexGuard<'a, FooterCache>;

mod stream;
pub use stream::{
    IcebergParquetPageStream, IcebergPartitionOnlyPageStream, create_iceberg_page_stream,
};
pub(crate) use stream::{MaterializedPageStream, MaterializedPages, SplitOperations};

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
        | ConnectorValue::TimestampMillis(value)
        | ConnectorValue::TimestampTzMicros(value)
        | ConnectorValue::TimestampNanos(value)
        | ConnectorValue::TimestampTzNanos(value) => Some(MinMaxPredicateValue::Int64(*value)),
        ConnectorValue::TinyInt(value) => Some(MinMaxPredicateValue::Int32(i32::from(*value))),
        ConnectorValue::SmallInt(value) => Some(MinMaxPredicateValue::Int32(i32::from(*value))),
        ConnectorValue::Real(_)
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

/// Report one pruned row group by identity only.
///
/// The marker carries the scheduled-split sequence, the row-group ordinal, and
/// the checkpoint that decided. It never carries a bound, a value, or anything
/// else derived from the filter's key material.
#[cfg(novarocks_dev_controls)]
fn emit_row_group_pruned_marker(
    binding: &IcebergReadBinding,
    row_group: DynamicFilterRowGroupId,
    checkpoint: DynamicFilterCheckpoint,
) {
    let Some(slot) = binding.test_observation() else {
        return;
    };
    let Some(scope) = binding
        .request_context()
        .and_then(|context| context.range_scope())
    else {
        return;
    };
    let checkpoint = match checkpoint {
        DynamicFilterCheckpoint::Footer => "footer",
        DynamicFilterCheckpoint::FirstRowGroup => "first_row_group",
        DynamicFilterCheckpoint::NextRowGroup => "next_row_group",
    };
    slot.reader(scope, || {
        println!(
            "{} scheduled_split_sequence_id={} row_group_ordinal={} checkpoint={checkpoint}",
            novarocks_failpoint::observation_marker::RUNTIME_FILTER_ROW_GROUP_PRUNED,
            row_group.scheduled_split_sequence_id,
            row_group.row_group_ordinal
        );
    });
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

/// One immutable Parquet footer per exact file identity and access domain,
/// shared by the splits of a scan.
///
/// The cache lives on the provider, which lives for one fragment instance and
/// scan node. Splits of the same file therefore read the footer once, and
/// nothing survives the provider.
#[derive(Debug, Default)]
pub struct ParquetFooterCache {
    entries: Mutex<FooterCache>,
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
        let file = access
            .bind_location(path, FileIdentity::new(path, file_size, None))
            .map_err(map_file_error)?;
        context.check_active().map_err(map_file_error)?;
        let key = (file.access_domain(), file.identity().clone());
        if let Some(cached) = self.lock()?.get(&key) {
            return Ok(cached.clone());
        }
        let inspection = inspect_parquet_metadata(
            file,
            None,
            context.clone(),
            novarocks_fs::ParquetInterpretationPolicy::SkipArrowHint,
        )
        .map_err(map_file_error)?;
        self.lock()?.insert(key, inspection.clone());
        Ok(inspection)
    }

    /// Awaited [`Self::footer`].
    pub async fn footer_async(
        &self,
        access: &FsAccessHandle,
        context: &FileReadContext,
        path: &str,
        file_size: u64,
    ) -> Result<ParquetMetadataInspection, ConnectorError> {
        let file = access
            .bind_location(path, FileIdentity::new(path, file_size, None))
            .map_err(map_file_error)?;
        context.check_active().map_err(map_file_error)?;
        let key = (file.access_domain(), file.identity().clone());
        if let Some(cached) = self.lock()?.get(&key) {
            return Ok(cached.clone());
        }
        let inspection = novarocks_fs::inspect_parquet_metadata_async(
            file,
            None,
            context.clone(),
            novarocks_fs::ParquetInterpretationPolicy::SkipArrowHint,
        )
        .await
        .map_err(map_file_error)?;
        self.lock()?.insert(key, inspection.clone());
        Ok(inspection)
    }

    /// Publish an identity-bound footer parsed from B-accounted preparation.
    /// A later demanded reader reuses this exact snapshot through the normal
    /// file/domain lookup rather than issuing a second footer request.
    pub fn remember(&self, inspection: ParquetMetadataInspection) -> Result<(), ConnectorError> {
        if inspection.interpretation_policy()
            != novarocks_fs::ParquetInterpretationPolicy::SkipArrowHint
        {
            return Err(invalid(
                "Iceberg footer requires the provider's Arrow hint policy",
            ));
        }
        let key = (inspection.access_domain(), inspection.identity().clone());
        self.lock()?.entry(key).or_insert(inspection);
        Ok(())
    }

    /// How many distinct footers this scan has read.
    pub fn len(&self) -> Result<usize, ConnectorError> {
        Ok(self.lock()?.len())
    }

    pub fn is_empty(&self) -> Result<bool, ConnectorError> {
        Ok(self.lock()?.is_empty())
    }

    fn lock(&self) -> Result<FooterCacheGuard<'_>, ConnectorError> {
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
    scalar_integer_domains: crate::scalar_integer_domain::ScalarIntegerDomains,
    delete_domains: super::delete_manager::DeleteDomainBindings,
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
            scalar_integer_domains: handle.scalar_integer_domains().clone(),
            delete_domains: super::delete_manager::DeleteDomainBindings::Single(Arc::clone(
                handle.read_domain().ok_or_else(|| {
                    invalid("iceberg data reader requires its pinned read domain")
                })?,
            )),
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
            scalar_integer_domains: handle
                .columns()
                .iter()
                .filter_map(|column| {
                    column
                        .scalar_integer_domain()
                        .map(|domain| (column.base_field_id(), domain))
                })
                .collect(),
            delete_domains: super::delete_manager::DeleteDomainBindings::Window {
                from: Arc::clone(handle.from_read_domain()),
                to: Arc::clone(handle.to_read_domain()),
            },
        })
    }

    /// Read one complete endpoint against its independently pinned owner.
    pub fn of_change_window_endpoint(
        handle: &IcebergChangeWindowHandle,
        partition_spec_id: i32,
        from: bool,
    ) -> Result<Self, ConnectorError> {
        let mut relation = Self::of_change_window(handle, partition_spec_id)?;
        relation.delete_domains =
            super::delete_manager::DeleteDomainBindings::Single(Arc::clone(if from {
                handle.from_read_domain()
            } else {
                handle.to_read_domain()
            }));
        Ok(relation)
    }

    pub fn table_schema(&self) -> &Arc<Schema> {
        &self.table_schema
    }
}

/// Everything one split's page stream needs, all of it already frozen or
/// process-local.
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
    pub cache: Option<novarocks_fs::DataCachePolicy>,
    pub budget: FileReadBudget,
    pub reader_options: FileReaderOptions,
    /// Names this split within its task attempt; the only scheduling identity.
    pub scheduled_split_sequence_id: u64,
    pub dynamic_filter: Arc<IcebergDynamicFilter>,
    /// Opaque data input prepared for this exact bound file. The active reader
    /// still chooses the authoritative decode ranges and fills any misses.
    pub prepared_input: Option<PreparedFileInput>,
    pub pending_preparation_control: Option<Arc<dyn ConnectorPreparationControl>>,
}

/// A split's facts once its format, encryption material and partition spec
/// are admitted. Admission reads nothing.
struct AdmittedSplit {
    table_schema: Arc<Schema>,
    partition_spec: PartitionSpec,
    partition_values: Struct,
    effective_predicate: TupleDomain<IcebergColumnHandle>,
}

fn admit_split(request: &IcebergPageSourceRequest<'_>) -> Result<AdmittedSplit, ConnectorError> {
    let split = request.split;
    admit_file_format(split.file_format())?;
    reject_encryption_material(
        split.decryption_data(),
        &format!("iceberg data file {}", split.path()),
    )?;
    for delete in split.deletes().iter() {
        let delete = delete?;
        reject_encryption_material(
            delete.decryption_data(),
            &format!("iceberg delete file {}", delete.path()),
        )?;
    }

    for column in request.columns {
        let declaration = request
            .relation
            .scalar_integer_domains
            .get(&column.base_field_id())
            .copied();
        if column.scalar_integer_domain() != declaration {
            return Err(invalid(
                "Iceberg scan column declaration differs from its frozen relation",
            ));
        }
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
    let partition_values = parse_partition_values(split, &partition_spec)?;
    // Validate the relation/split binding before even the partition-only path
    // can answer rows. The relation supplies the independent expected domain.
    DeleteManager::preview_hidden_columns(
        split,
        &table_schema,
        &request.relation.delete_domains,
        &request.delete_mode,
    )?;
    Ok(AdmittedSplit {
        table_schema,
        partition_spec,
        partition_values,
        effective_predicate: request.relation.effective_predicate.clone(),
    })
}

/// The fast path that needs no byte of the data file, when the split has one.
fn partition_only_source(
    request: &IcebergPageSourceRequest<'_>,
    admitted: &AdmittedSplit,
) -> Result<Option<IcebergPartitionOnlyPageSource>, ConnectorError> {
    // The fast path answers "every row of this file", which is only the right
    // answer when deletes hide rows. A mode that *selects* rows -- the change
    // window's reverse side, or reverse equality projection -- would get every
    // row of the file back with the right shape and the wrong contents.
    if request.delete_mode != DeleteEvaluationMode::ExcludeDeleted {
        return Ok(None);
    }
    try_partition_only_page_source(
        request.split,
        request.columns,
        &admitted.partition_spec,
        &admitted.partition_values,
        &admitted.table_schema,
        &admitted.effective_predicate,
        request.budget,
    )
}

/// Everything one data split's reader is built from, owned so that a page
/// stream can build it in its first poll.
struct ParquetSplitRequest {
    split: IcebergSplit,
    name_mapping: Option<Arc<NameMapping>>,
    columns: Vec<IcebergColumnHandle>,
    delete_manager: Arc<DeleteManager>,
    delete_domains: super::delete_manager::DeleteDomainBindings,
    footers: Arc<ParquetFooterCache>,
    access_binding: IcebergReadBinding,
    context: FileReadContext,
    cache: Option<novarocks_fs::DataCachePolicy>,
    budget: FileReadBudget,
    reader_options: FileReaderOptions,
    scheduled_split_sequence_id: u64,
    dynamic_filter: Arc<IcebergDynamicFilter>,
    prepared_input: Option<PreparedFileInput>,
    pending_preparation_control: Option<Arc<dyn ConnectorPreparationControl>>,
}

impl ParquetSplitRequest {
    fn of(request: IcebergPageSourceRequest<'_>) -> (Self, DeleteEvaluationMode) {
        (
            Self {
                split: request.split.clone(),
                name_mapping: request.relation.name_mapping.clone(),
                columns: request.columns.to_vec(),
                delete_manager: request.delete_manager,
                delete_domains: request.relation.delete_domains.clone(),
                footers: request.footers,
                access_binding: request.access_binding,
                context: request.context,
                cache: request.cache,
                budget: request.budget,
                reader_options: request.reader_options,
                scheduled_split_sequence_id: request.scheduled_split_sequence_id,
                dynamic_filter: request.dynamic_filter,
                prepared_input: request.prepared_input,
                pending_preparation_control: request.pending_preparation_control,
            },
            request.delete_mode,
        )
    }

    /// The prepared input's backing capacity, held from creation until the
    /// split's reader is closed.
    fn prepared_retained_capacity(&self) -> u64 {
        self.prepared_input
            .as_ref()
            .map_or(0, |input| input.retained_backing_capacity() as u64)
    }

    /// What the split retains before a reader is open; the page source built
    /// from it reports the same.
    fn retained_base_bytes(&self) -> u64 {
        self.split
            .retained_size_in_bytes()
            .saturating_add(self.prepared_retained_capacity())
    }

    fn into_source(
        self,
        admitted: AdmittedSplit,
        delete_filter: SplitDeleteFilter,
        delete_mode: DeleteEvaluationMode,
        successor_control: Arc<SuccessorPreparationGroup>,
    ) -> IcebergParquetPageSource {
        let hidden_columns = delete_filter.required_hidden_columns().to_vec();
        let prepared_retained_capacity = self.prepared_retained_capacity();
        IcebergParquetPageSource {
            delete_domains: self.delete_domains,
            delete_mode,
            table_schema: admitted.table_schema,
            name_mapping: self.name_mapping,
            partition_spec: admitted.partition_spec,
            partition_values: admitted.partition_values,
            prefix_len: self.columns.len(),
            bound_handles: self.columns.into_iter().chain(hidden_columns).collect(),
            delete_filter,
            effective_predicate: admitted.effective_predicate,
            access_binding: self.access_binding,
            context: self.context,
            cache: self.cache,
            footers: self.footers,
            budget: self.budget,
            reader_options: self.reader_options,
            dynamic_filter: LiveDynamicFilter::new(
                self.dynamic_filter,
                self.scheduled_split_sequence_id,
            ),
            prepared_retained_capacity,
            prepared_input: self.prepared_input,
            successor_preparation: None,
            successor_control,
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
            retained_bytes: self.split.retained_size_in_bytes(),
            split: self.split,
            finished: false,
        }
    }
}

pub(super) struct PreparedInputRequest<'a> {
    pub(super) relation: &'a IcebergReadRelation,
    pub(super) split: &'a IcebergSplit,
    pub(super) columns: &'a [IcebergColumnHandle],
    pub(super) delete_mode: &'a DeleteEvaluationMode,
    pub(super) footer: &'a ParquetMetadataInspection,
    pub(super) access_binding: &'a IcebergReadBinding,
    pub(super) context: FileReadContext,
    pub(super) cache: Option<novarocks_fs::DataCachePolicy>,
    pub(super) budget: FileReadBudget,
    pub(super) reader_options: FileReaderOptions,
    pub(super) row_group_ordinal: Option<u32>,
}

/// Plan one conservative physical input range from an already prepared footer
/// on the scan CPU. This path performs no metadata network request.
/// Delete closure classification determines projection only; it does not load
/// delete artifacts. The live filter is intentionally rechecked at promotion.
pub(super) fn plan_iceberg_prepared_input(
    request: PreparedInputRequest<'_>,
) -> Result<Option<PlannedInput>, ConnectorError> {
    let PreparedInputRequest {
        relation,
        split,
        columns,
        delete_mode,
        footer,
        access_binding,
        context,
        cache,
        budget,
        mut reader_options,
        row_group_ordinal,
    } = request;
    admit_file_format(split.file_format())?;
    reject_encryption_material(
        split.decryption_data(),
        &format!("iceberg data file {}", split.path()),
    )?;
    for delete in split.deletes().iter() {
        let delete = delete?;
        reject_encryption_material(
            delete.decryption_data(),
            &format!("iceberg delete file {}", delete.path()),
        )?;
    }
    if relation.partition_spec.spec_id() != split.partition_spec_id() {
        return Err(invalid(format!(
            "iceberg data file {} was planned under partition spec {} but is read against spec {}",
            split.path(),
            split.partition_spec_id(),
            relation.partition_spec.spec_id()
        )));
    }
    let partition_values = parse_partition_values(split, &relation.partition_spec)?;
    if *delete_mode == DeleteEvaluationMode::ExcludeDeleted
        && try_partition_only_page_source(
            split,
            columns,
            &relation.partition_spec,
            &partition_values,
            &relation.table_schema,
            &relation.effective_predicate,
            budget,
        )?
        .is_some()
    {
        return Ok(None);
    }
    // The small-file path already retained its entire input while reading the
    // footer. A second speculative physical range would duplicate that work.
    let file_size = u64::try_from(split.file_size()).map_err(|_| {
        corrupt(format!(
            "iceberg data file {} declares a negative size",
            split.path()
        ))
    })?;
    if file_size <= novarocks_fs::SMALL_FILE_PROBE_MAX_BYTES {
        return Ok(None);
    }
    let access = access_binding.resolve_access(split.path())?;
    let hidden = DeleteManager::preview_hidden_columns(
        split,
        &relation.table_schema,
        &relation.delete_domains,
        delete_mode,
    )?;
    let bound_handles = columns.iter().cloned().chain(hidden).collect::<Vec<_>>();
    let binding = bind_scan_columns(IcebergSchemaBindingRequest {
        table_schema: &relation.table_schema,
        file_schema: footer.schema(),
        name_mapping: relation.name_mapping.clone(),
        partition_spec: Some(&relation.partition_spec),
        partition_values: Some(&partition_values),
        columns: &bound_handles,
    })?;
    let projection = match binding.coverage() {
        FileFieldIdCoverage::None => FileProjection::All,
        FileFieldIdCoverage::Complete => {
            FileProjection::FieldIds(binding.physical_base_field_ids().to_vec())
        }
    };
    let range = if split.is_whole_file() {
        FileReadRange::WholeFile
    } else {
        let start = u64::try_from(split.start())
            .map_err(|_| corrupt("iceberg split start offset is negative".to_owned()))?;
        let length = u64::try_from(split.length())
            .map_err(|_| corrupt("iceberg split length is negative".to_owned()))?;
        FileReadRange::bounded(start, length).map_err(map_file_error)?
    };
    let file = access
        .bind_location(
            split.path(),
            FileIdentity::new(split.path(), file_size, None),
        )
        .map_err(map_file_error)?;
    // An incomplete runtime filter can arrive after preparation. Page-index
    // work here would embody an early predicate decision; use conservative
    // column ranges, leaving exact pages to the active decoder.
    reader_options = crate::file_reader::iceberg_reader_options(reader_options);
    reader_options.enable_parquet_reader_page_index = false;
    let request = FileReadRequest {
        file: file.clone(),
        format: novarocks_fs::FileFormat::Parquet,
        range,
        projection,
        budget,
        predicates: Vec::new(),
        pruning: PhysicalPruning {
            row_groups: row_group_ordinal.map(|ordinal| vec![ordinal as usize]),
            pages: Vec::new(),
        },
        options: reader_options,
        cache,
        prepared_input: None,
        context,
    };
    let ranges = plan_parquet_input_ranges(&request, footer).map_err(map_file_error)?;
    Ok(ranges
        .into_iter()
        .next()
        .map(|range| PlannedInput { file, range }))
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
    let mapping: NameMapping = crate::metadata_json::from_str(json)
        .map_err(|error| invalid(format!("iceberg name mapping json is invalid: {error}")))?;
    Ok(Some(Arc::new(mapping)))
}

fn parse_partition_values(
    split: &IcebergSplit,
    partition_spec: &PartitionSpec,
) -> Result<Struct, ConnectorError> {
    let partition_type = split
        .read_domain()
        .endpoint()
        .partition_type(partition_spec.spec_id())
        .map_err(|error| corrupt(format!("iceberg pinned partition type: {error}")))?;
    crate::delete_semantics::decode_partition_data_json_with_type(
        partition_spec,
        &partition_type,
        split.partition_data_json(),
    )
    .map_err(|error| corrupt(format!("iceberg split partition data: {error}")))
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
            | IcebergColumnSource::MutationSpecId(_)
            | IcebergColumnSource::MutationPartition(_)
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
    }))
}

/// A split that needs no byte of its data file: its pages are constants.
pub struct IcebergPartitionOnlyPageSource {
    constants: Vec<(FieldRef, Option<Literal>)>,
    total_rows: u64,
    emitted_rows: u64,
    max_batch_rows: usize,
    retained_bytes: u64,
}

impl IcebergPartitionOnlyPageSource {
    fn next_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if self.is_exhausted() {
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

    fn is_exhausted(&self) -> bool {
        self.emitted_rows >= self.total_rows
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: 0,
            completed_positions: self.emitted_rows,
            read_time_nanos: 0,
            ..Default::default()
        }
    }

    fn retained_bytes(&self) -> u64 {
        self.retained_bytes
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
// The Parquet split
// ---------------------------------------------------------------------------

/// The physical reader of one run, awaited by the split's page stream.
type SplitReader = Box<novarocks_fs::AsyncFileBatchReader>;

enum ReaderState {
    NotOpened,
    Open {
        /// Absent between two runs of the plan, while the next surviving row
        /// groups are still being chosen.
        reader: Option<SplitReader>,
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

/// The per-split Parquet reader state a page stream moves through its steps.
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
    delete_domains: super::delete_manager::DeleteDomainBindings,
    delete_mode: DeleteEvaluationMode,
    effective_predicate: TupleDomain<IcebergColumnHandle>,
    access_binding: IcebergReadBinding,
    context: FileReadContext,
    cache: Option<novarocks_fs::DataCachePolicy>,
    footers: Arc<ParquetFooterCache>,
    budget: FileReadBudget,
    reader_options: FileReaderOptions,
    dynamic_filter: LiveDynamicFilter,
    prepared_input: Option<PreparedFileInput>,
    prepared_retained_capacity: u64,
    /// At most the next ordered row group of the active source. Its control
    /// does not own or cancel the active reader's demand operation.
    successor_preparation: Option<(u32, PreparedRangeCandidate)>,
    successor_control: Arc<SuccessorPreparationGroup>,
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
}

impl IcebergParquetPageSource {
    /// Resolves the file's access, reads its footer and binds the split to
    /// it.
    async fn open(&mut self) -> Result<(), ConnectorError> {
        let access = self
            .access_binding
            .resolve_access_for_locations_async([self.split.path()], &self.context)
            .await?;
        let file_size = self.file_size()?;
        let footer = self
            .footers
            .footer_async(&access, &self.context, self.split.path(), file_size)
            .await?;
        self.open_with_footer(footer)
    }

    fn file_size(&self) -> Result<u64, ConnectorError> {
        u64::try_from(self.split.file_size()).map_err(|_| {
            corrupt(format!(
                "iceberg data file {} declares a negative size",
                self.split.path()
            ))
        })
    }

    /// Judges and binds the split against its footer; no I/O.
    fn open_with_footer(
        &mut self,
        footer: ParquetMetadataInspection,
    ) -> Result<(), ConnectorError> {
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
    fn record_pruned(&mut self, ordinals: &[u32], _checkpoint: DynamicFilterCheckpoint) {
        for ordinal in ordinals {
            let row_group = self.dynamic_filter.row_group(*ordinal);
            #[cfg(novarocks_dev_controls)]
            emit_row_group_pruned_marker(&self.access_binding, row_group, _checkpoint);
            self.pruned_row_groups.push(row_group);
        }
    }

    /// Open the reader for the next run of row groups this split must read.
    ///
    /// Returns `false` once the plan is exhausted. Every row group is judged
    /// against the filter as it stands at this moment, which is what lets a
    /// filter that arrived mid-split prune the row groups that follow.
    async fn open_next_run(&mut self) -> Result<bool, ConnectorError> {
        let Some(row_groups) = self.next_run()? else {
            return Ok(false);
        };
        let reader = self.open_reader(row_groups).await?;
        Ok(self.install_reader(reader))
    }

    fn install_reader(&mut self, reader: SplitReader) -> bool {
        let ReaderState::Open { reader: slot, .. } = &mut self.state else {
            return false;
        };
        *slot = Some(reader);
        true
    }

    /// Chooses the next run's row groups against the filter as it stands now,
    /// and takes over the successor's prepared input when the run reads it.
    /// `None` once the plan is exhausted; `Some(None)` reads whatever the
    /// byte range selects.
    fn next_run(&mut self) -> Result<Option<Option<Vec<u32>>>, ConnectorError> {
        let Some(footer) = self.footer.clone() else {
            return Ok(None);
        };
        // Read the plan out first: judging a row group needs the filter and the
        // pruned-row-group log, which cannot be borrowed while the plan is.
        let Some((candidates, checkpoint)) = self.take_next_candidates() else {
            return Ok(None);
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
                    return Ok(None);
                }
                // Reading every row group of the file is what this split did
                // before any filter existed, so it is expressed the same way.
                (run.len() != footer.row_groups().len()).then_some(run)
            }
        };

        if let Some((ordinal, mut candidate)) = self.successor_preparation.take() {
            let selected = row_groups
                .as_ref()
                .is_none_or(|groups| groups.contains(&ordinal));
            if selected {
                self.prepared_input = candidate.take_ready()?;
            }
            drop(candidate);
        }
        Ok(Some(row_groups))
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
    async fn open_reader(
        &mut self,
        row_groups: Option<Vec<u32>>,
    ) -> Result<SplitReader, ConnectorError> {
        let access = self
            .access_binding
            .resolve_access_for_locations_async([self.split.path()], &self.context)
            .await?;
        let request = self.reader_request(&access, row_groups)?;
        novarocks_fs::open_file_reader_async(request, self.footer.as_ref())
            .await
            .map(Box::new)
            .map_err(map_file_error)
    }

    /// The physical read request of one run; no I/O.
    fn reader_request(
        &mut self,
        access: &FsAccessHandle,
        row_groups: Option<Vec<u32>>,
    ) -> Result<FileReadRequest, ConnectorError> {
        let file_size = self.file_size()?;
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
        Ok(FileReadRequest {
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
            options: crate::file_reader::iceberg_reader_options(self.reader_options),
            cache: self.cache.clone(),
            prepared_input: self.prepared_input.take(),
            context: self.context.clone(),
        })
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

impl IcebergParquetPageSource {
    /// Advances the preparation of the next ordered row group within the
    /// supplied capacity, without moving the decode cursor.
    fn advance_successor_preparation(
        &mut self,
        remaining_input_bytes: u64,
        remaining_candidates: usize,
    ) -> Result<ConnectorPreparationProgress, ConnectorError> {
        if self.finished {
            return Ok(ConnectorPreparationProgress::Deferred);
        }
        let next_ordinal = match &self.state {
            ReaderState::Open {
                reader: Some(_),
                plan: RowGroupPlan::PerRowGroup { remaining, .. },
                ..
            } => remaining.front().copied(),
            _ => None,
        };
        let Some(next_ordinal) = next_ordinal else {
            return Ok(ConnectorPreparationProgress::Deferred);
        };
        if self
            .successor_preparation
            .as_ref()
            .is_some_and(|(ordinal, _)| *ordinal != next_ordinal)
        {
            // A newly arrived filter may skip the old target. Its operation
            // stays in the stable control until the actual exit is observed.
            self.successor_preparation = None;
        }
        if self.successor_preparation.is_none() {
            if remaining_candidates == 0 {
                return Ok(ConnectorPreparationProgress::Deferred);
            }
            let relation = IcebergReadRelation {
                table_schema: Arc::clone(&self.table_schema),
                partition_spec: self.partition_spec.clone(),
                name_mapping: self.name_mapping.clone(),
                effective_predicate: self.effective_predicate.clone(),
                scalar_integer_domains: self.bound_handles[..self.prefix_len]
                    .iter()
                    .filter_map(|column| {
                        column
                            .scalar_integer_domain()
                            .map(|domain| (column.base_field_id(), domain))
                    })
                    .collect(),
                delete_domains: self.delete_domains.clone(),
            };
            let split = self.split.clone();
            let columns = self.bound_handles[..self.prefix_len].to_vec();
            let delete_mode = self.delete_mode.clone();
            let Some(footer) = self.footer.clone() else {
                return Ok(ConnectorPreparationProgress::Deferred);
            };
            let binding = self.access_binding.clone();
            let cache = self.cache.clone();
            let budget = self.budget;
            let options = self.reader_options;
            let planner = Arc::new(move |context: FileReadContext| {
                plan_iceberg_prepared_input(PreparedInputRequest {
                    relation: &relation,
                    split: &split,
                    columns: &columns,
                    delete_mode: &delete_mode,
                    footer: &footer,
                    access_binding: &binding,
                    context,
                    cache: cache.clone(),
                    budget,
                    reader_options: options,
                    row_group_ordinal: Some(next_ordinal),
                })
            });
            let Some(candidate) = PreparedRangeCandidate::new(self.context.clone(), planner) else {
                return Ok(ConnectorPreparationProgress::Deferred);
            };
            self.successor_control.add(candidate.control());
            self.successor_preparation = Some((next_ordinal, candidate));
        }
        self.successor_preparation
            .as_mut()
            .expect("successor candidate was just installed")
            .1
            .advance(remaining_input_bytes)
    }

    fn successor_preparation_candidate_count(&self) -> usize {
        let ready_active = self
            .successor_preparation
            .as_ref()
            .is_some_and(|(_, candidate)| candidate.control().is_drained());
        self.successor_control
            .undrained_candidate_count()
            .saturating_add(usize::from(ready_active))
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: self.completed_bytes,
            completed_positions: self.completed_positions,
            read_time_nanos: self.read_time_nanos,
            file: page_source_file_metrics(self.file_metrics),
        }
    }
}

impl IcebergParquetPageSource {
    /// Memory the split itself retains, apart from its successor and promoted
    /// preparation, which report their own live retention.
    fn retained_base_bytes(&self) -> u64 {
        let binding = match &self.state {
            ReaderState::Open { binding, .. } => binding.retained_size_in_bytes(),
            ReaderState::NotOpened | ReaderState::Drained => 0,
        };
        self.retained_bytes
            .saturating_add(binding)
            .saturating_add(self.prepared_retained_capacity)
    }

    /// Drops the split's open reader; waits for nothing.
    fn close_reader(&mut self) -> Result<(), ConnectorError> {
        // The reader is dropped whatever its own close says: a closed split
        // must not keep an open file handle alive because the underlying
        // close reported a late I/O error.
        let state = std::mem::replace(&mut self.state, ReaderState::Drained);
        let reader_result = if let ReaderState::Open {
            reader: Some(mut reader),
            ..
        } = state
        {
            self.file_metrics =
                file_metrics_saturating_add(self.retired_file_metrics, reader.metrics_snapshot());
            self.completed_bytes = self.file_metrics.bytes_read;
            reader.close().map_err(map_file_error)
        } else {
            Ok(())
        };
        self.prepared_retained_capacity = 0;
        reader_result
    }
}

impl IcebergParquetPageSource {
    /// The split's next page: a decoded batch judged by deletes and the
    /// remaining predicate. Each decoded batch spends one unit of the host's
    /// turn budget, so a run of batches that all filter to nothing still ends
    /// the turn.
    async fn produce_page_async(
        &mut self,
        budget: &ConnectorPollBudget,
    ) -> Result<Option<SourcePage>, ConnectorError> {
        if matches!(self.state, ReaderState::NotOpened) {
            self.open().await?;
        }
        loop {
            let Some(file_batch) = self.next_file_batch().await? else {
                return Ok(None);
            };
            let page = self.assemble_page(file_batch)?;
            budget.consume(1).await;
            if let Some(page) = page {
                return Ok(Some(page));
            }
        }
    }

    /// The next decoded batch of the split, opening the next run of row
    /// groups whenever the current one is drained. `None` once the split is.
    async fn next_file_batch(&mut self) -> Result<Option<novarocks_fs::FileBatch>, ConnectorError> {
        loop {
            // A run ends at a row-group boundary, which is exactly where a
            // tighter dynamic filter can still save the next decode.
            if matches!(&self.state, ReaderState::Open { reader: None, .. })
                && !self.open_next_run().await?
            {
                return Ok(self.finish());
            }
            let ReaderState::Open {
                reader: Some(reader),
                ..
            } = &mut self.state
            else {
                self.finished = true;
                return Ok(None);
            };
            let next = reader.next_batch().await;
            if let Some(batch) = self.observe_batch(next)? {
                return Ok(Some(batch));
            }
            if self.retire_run()? {
                return Ok(self.finish());
            }
        }
    }

    fn finish(&mut self) -> Option<novarocks_fs::FileBatch> {
        self.finished = true;
        self.state = ReaderState::Drained;
        None
    }

    /// Folds the open reader's counters into the split's and returns its
    /// batch, if it produced one.
    fn observe_batch(
        &mut self,
        next: novarocks_fs::FileResult<Option<novarocks_fs::FileBatch>>,
    ) -> Result<Option<novarocks_fs::FileBatch>, ConnectorError> {
        if let ReaderState::Open {
            reader: Some(reader),
            ..
        } = &self.state
        {
            self.file_metrics =
                file_metrics_saturating_add(self.retired_file_metrics, reader.metrics_snapshot());
            self.completed_bytes = self.file_metrics.bytes_read;
        }
        next.map_err(map_file_error)
    }

    /// Retires the drained run's reader and its counters. True when the plan
    /// has nothing left, so the split is drained; otherwise the plan decides,
    /// against the filter as it now stands, what comes next.
    fn retire_run(&mut self) -> Result<bool, ConnectorError> {
        self.retired_bytes = self.completed_bytes;
        self.retired_file_metrics = self.file_metrics;
        let ReaderState::Open { reader, plan, .. } = &mut self.state else {
            return Ok(true);
        };
        let exhausted = plan.is_exhausted();
        if let Some(mut reader) = reader.take() {
            reader.close().map_err(map_file_error)?;
        }
        Ok(exhausted)
    }

    /// Turns one decoded batch into the split's page: materialized, judged by
    /// deletes and the remaining predicate over the complete page, and cut to
    /// the scan's columns. `None` when nothing in it survives. No I/O.
    fn assemble_page(
        &mut self,
        file_batch: novarocks_fs::FileBatch,
    ) -> Result<Option<SourcePage>, ConnectorError> {
        let ReaderState::Open {
            binding,
            page_schema,
            checks,
            positions_required,
            ..
        } = &self.state
        else {
            return Err(invalid(
                "iceberg page source assembled a page before binding its columns",
            ));
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
            // Equality keys bind against their unique internal projection.
            // Output columns may repeat the same field ID, including aliases.
            let delete_columns: Vec<_> = (self.prefix_len..page_batch.num_columns()).collect();
            let delete_batch = page_batch
                .project(&delete_columns)
                .map_err(|error| corrupt(format!("iceberg delete projection failed: {error}")))?;
            Some(self.delete_filter.evaluate(&delete_batch, positions)?)
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
            return Ok(None);
        }
        self.completed_positions = self
            .completed_positions
            .saturating_add(page.position_count() as u64);
        Ok(Some(page))
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
        partial_prefetch_copy_bytes: left
            .partial_prefetch_copy_bytes
            .saturating_add(right.partial_prefetch_copy_bytes),
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
        ConnectorValueType::TinyInt => {
            ConnectorValue::TinyInt(downcast::<arrow::array::Int8Array>(column, path)?.value(row))
        }
        ConnectorValueType::SmallInt => {
            ConnectorValue::SmallInt(downcast::<Int16Array>(column, path)?.value(row))
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
        ConnectorValueType::TimestampMillis => ConnectorValue::TimestampMillis(
            downcast::<TimestampMillisecondArray>(column, path)?.value(row),
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
mod tests;
