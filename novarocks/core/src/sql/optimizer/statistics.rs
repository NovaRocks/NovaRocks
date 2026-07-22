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

//! Statistics types for the cost-based optimizer.

use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::stats_input::{StatsMissingReason, StatsSource};

pub(crate) const MAX_FINITE_COST: f64 = 1.0e300;
pub(crate) const DEFAULT_CPU_COST_WEIGHT: f64 = 0.5;
pub(crate) const DEFAULT_MEMORY_COST_WEIGHT: f64 = 2.0;
pub(crate) const DEFAULT_NETWORK_COST_WEIGHT: f64 = 1.5;

pub(crate) fn finite_non_negative_dimension(value: f64) -> f64 {
    if value.is_finite() {
        if value > 0.0 {
            value.min(MAX_FINITE_COST)
        } else {
            0.0
        }
    } else if value.is_infinite() && value.is_sign_positive() {
        MAX_FINITE_COST
    } else {
        0.0
    }
}

fn finite_non_negative_weight(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value.min(MAX_FINITE_COST)
    } else {
        0.0
    }
}

fn add_cost_dimensions(left: f64, right: f64) -> f64 {
    finite_non_negative_dimension(
        finite_non_negative_dimension(left) + finite_non_negative_dimension(right),
    )
}

/// Trustworthiness of a statistic. Variant order is meaningful: derived
/// `Ord` makes `Measured > Exact > Estimated > Fallback`, so `min` yields
/// the least-confident input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Confidence {
    #[default]
    Fallback, // relied on a heuristic/default (missing-stats rows, default selectivity/NDV)
    Estimated, // derived via formula from at-least-partially-real inputs
    Exact,     // sourced from real catalog/Iceberg stats (Puffin NDV, metadata row_count)
    /// Measured source (MV materialized row count / runtime feedback / sampling).
    /// Strictly more trustworthy than catalog `Exact`.
    /// Currently has no producer (stub) — inert until a measured source lands.
    Measured,
}

impl Confidence {
    /// Least-confident of two confidences.
    pub fn combine(self, other: Confidence) -> Confidence {
        self.min(other)
    }

    /// Confidence of a value produced by applying a formula to `inputs`.
    /// A formula result is never better than `Estimated`; any `Fallback`
    /// input — or `used_default` — degrades the result to `Fallback`.
    pub fn derive(inputs: &[Confidence], used_default: bool) -> Confidence {
        if used_default {
            return Confidence::Fallback;
        }
        let least = inputs
            .iter()
            .copied()
            .min()
            .unwrap_or(Confidence::Estimated);
        least.min(Confidence::Estimated)
    }
}

/// Per-column distinct-value metadata with explicit missing state.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DistinctValueCount {
    Known {
        value: f64,
        confidence: Confidence,
        source: StatsSource,
    },
    Unknown {
        reason: StatsMissingReason,
    },
}

impl DistinctValueCount {
    pub(crate) fn known(value: f64, confidence: Confidence, source: StatsSource) -> Self {
        Self::Known {
            value,
            confidence,
            source,
        }
    }

    pub(crate) fn unknown(reason: StatsMissingReason) -> Self {
        Self::Unknown { reason }
    }

    pub(crate) fn known_value(&self) -> Option<f64> {
        match self {
            Self::Known { value, .. } => Some(*value),
            Self::Unknown { .. } => None,
        }
    }

    pub(crate) fn trusted_value(&self) -> Option<(f64, Confidence)> {
        match self {
            Self::Known {
                value, confidence, ..
            } if *confidence > Confidence::Fallback && value.is_finite() && *value >= 1.0 => {
                Some((*value, *confidence))
            }
            _ => None,
        }
    }

    pub(crate) fn source(&self) -> Option<StatsSource> {
        match self {
            Self::Known { source, .. } => Some(*source),
            Self::Unknown { .. } => None,
        }
    }
}

impl Default for DistinctValueCount {
    fn default() -> Self {
        Self::unknown(StatsMissingReason::ColumnNotReported("ndv".to_string()))
    }
}

/// Per-column statistics derived from catalog or connector metadata.
#[derive(Clone, Debug, Default)]
pub struct ColumnStatistic {
    pub min_value: f64,
    pub max_value: f64,
    pub nulls_fraction: f64,
    pub average_row_size: f64,
    pub ndv: DistinctValueCount,
    pub confidence: Confidence,
}

impl ColumnStatistic {
    pub fn unknown() -> Self {
        Self {
            min_value: f64::NEG_INFINITY,
            max_value: f64::INFINITY,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            ndv: DistinctValueCount::unknown(StatsMissingReason::ColumnNotReported(
                "ndv".to_string(),
            )),
            confidence: Confidence::Fallback,
        }
    }

    pub(crate) fn with_known_ndv(
        mut self,
        ndv: f64,
        confidence: Confidence,
        source: StatsSource,
    ) -> Self {
        self.set_known_ndv(ndv, confidence, source);
        self
    }

    pub(crate) fn set_known_ndv(&mut self, ndv: f64, confidence: Confidence, source: StatsSource) {
        self.ndv = DistinctValueCount::known(ndv, confidence, source);
        self.confidence = self.confidence.max(confidence);
    }

    pub(crate) fn ndv_value(&self) -> Option<f64> {
        self.ndv.known_value()
    }

    #[cfg(test)]
    pub(crate) fn ndv_or_legacy_unknown_sentinel_for_test(&self) -> f64 {
        self.ndv_value().unwrap_or(1.0)
    }

    pub(crate) fn trusted_ndv(&self) -> Option<(f64, Confidence)> {
        self.ndv.trusted_value()
    }

    pub(crate) fn trusted_ndv_value(&self) -> Option<f64> {
        self.trusted_ndv().map(|(value, _)| value)
    }

    pub(crate) fn ndv_source(&self) -> Option<StatsSource> {
        self.ndv.source()
    }

    #[cfg(test)]
    pub(crate) fn for_test_with_ndv(ndv: f64, confidence: Confidence) -> Self {
        Self::unknown().with_known_ndv(ndv, confidence, StatsSource::TestFixture)
    }
}

/// Operator-level statistics propagated through the plan tree.
#[derive(Clone, Debug, Default)]
pub struct Statistics {
    pub output_row_count: f64,
    pub row_count_confidence: Confidence,
    pub column_statistics: HashMap<ColumnId, ColumnStatistic>,
}

impl Statistics {
    const MAX_FINITE_SIZE: f64 = 1.0e300;

    pub fn avg_row_size(&self) -> f64 {
        if self.column_statistics.is_empty() {
            8.0
        } else {
            self.column_statistics
                .values()
                .map(|c| c.average_row_size)
                .sum()
        }
    }

    pub fn compute_size(&self) -> f64 {
        self.output_row_count * self.avg_row_size()
    }

    pub fn safe_output_row_count(&self) -> f64 {
        if self.output_row_count.is_finite() && self.output_row_count > 0.0 {
            self.output_row_count
        } else if self.output_row_count.is_infinite() && self.output_row_count.is_sign_positive() {
            Self::MAX_FINITE_SIZE
        } else {
            1.0
        }
    }

    pub fn compute_size_for_columns(&self, columns: &[ColumnId]) -> f64 {
        let row_width = if columns.is_empty() {
            self.safe_width_for_all_columns()
        } else {
            let mut row_width = 0.0;
            for column_id in columns {
                row_width = Self::add_safe_width(
                    row_width,
                    self.column_statistics
                        .get(column_id)
                        .map(|c| c.average_row_size),
                );
                if row_width >= Self::MAX_FINITE_SIZE {
                    return Self::MAX_FINITE_SIZE;
                }
            }
            row_width
        };
        self.safe_size(row_width)
    }

    fn safe_width_for_all_columns(&self) -> f64 {
        if self.column_statistics.is_empty() {
            return 8.0;
        }
        let mut row_width = 0.0;
        for column in self.column_statistics.values() {
            row_width = Self::add_safe_width(row_width, Some(column.average_row_size));
            if row_width >= Self::MAX_FINITE_SIZE {
                return Self::MAX_FINITE_SIZE;
            }
        }
        row_width
    }

    fn add_safe_width(total: f64, width: Option<f64>) -> f64 {
        let contribution = match width {
            Some(width) if width.is_finite() && width > 0.0 => width,
            Some(width) if width.is_infinite() && width.is_sign_positive() => {
                return Self::MAX_FINITE_SIZE;
            }
            _ => 8.0,
        };
        let total = total + contribution;
        if total.is_finite() && total >= 0.0 {
            total.min(Self::MAX_FINITE_SIZE)
        } else {
            Self::MAX_FINITE_SIZE
        }
    }

    fn safe_size(&self, row_width: f64) -> f64 {
        if row_width >= Self::MAX_FINITE_SIZE {
            return Self::MAX_FINITE_SIZE;
        }
        let row_count = self.safe_output_row_count();
        if row_count >= Self::MAX_FINITE_SIZE {
            return Self::MAX_FINITE_SIZE;
        }
        let size = row_count * row_width;
        if size.is_finite() && size >= 0.0 {
            size.min(Self::MAX_FINITE_SIZE)
        } else {
            Self::MAX_FINITE_SIZE
        }
    }
}

pub(crate) fn generate_series_row_count_f64(start: i64, end: i64, step: i64) -> f64 {
    if step == 0 {
        return 1.0;
    }
    let start = i128::from(start);
    let end = i128::from(end);
    let step = i128::from(step);
    if step > 0 {
        if start > end {
            return 0.0;
        }
        ((end - start) / step + 1) as f64
    } else {
        if start < end {
            return 0.0;
        }
        ((start - end) / step.abs() + 1) as f64
    }
}

/// Three-dimensional cost estimate (aligned with StarRocks CostEstimate).
#[derive(Clone, Debug, Default)]
pub struct CostEstimate {
    pub cpu_cost: f64,
    pub memory_cost: f64,
    pub network_cost: f64,
}

impl CostEstimate {
    #[cfg(test)]
    pub fn total_cost(&self) -> f64 {
        self.weighted_total(
            DEFAULT_CPU_COST_WEIGHT,
            DEFAULT_MEMORY_COST_WEIGHT,
            DEFAULT_NETWORK_COST_WEIGHT,
        )
    }

    pub fn sanitized(&self) -> CostEstimate {
        CostEstimate {
            cpu_cost: finite_non_negative_dimension(self.cpu_cost),
            memory_cost: finite_non_negative_dimension(self.memory_cost),
            network_cost: finite_non_negative_dimension(self.network_cost),
        }
    }

    pub fn weighted_total(&self, cpu_weight: f64, memory_weight: f64, network_weight: f64) -> f64 {
        let cost = self.sanitized();
        let cpu =
            finite_non_negative_dimension(cost.cpu_cost * finite_non_negative_weight(cpu_weight));
        let memory = finite_non_negative_dimension(
            cost.memory_cost * finite_non_negative_weight(memory_weight),
        );
        let network = finite_non_negative_dimension(
            cost.network_cost * finite_non_negative_weight(network_weight),
        );
        add_cost_dimensions(add_cost_dimensions(cpu, memory), network)
    }

    #[allow(dead_code)] // used by cost model tests
    pub fn add(&self, other: &CostEstimate) -> CostEstimate {
        self.add_sanitized(other)
    }

    pub fn add_sanitized(&self, other: &CostEstimate) -> CostEstimate {
        CostEstimate {
            cpu_cost: add_cost_dimensions(self.cpu_cost, other.cpu_cost),
            memory_cost: add_cost_dimensions(self.memory_cost, other.memory_cost),
            network_cost: add_cost_dimensions(self.network_cost, other.network_cost),
        }
    }
}

/// Table-level statistics aggregated from file metadata.
#[derive(Clone, Debug)]
pub struct TableStatistics {
    pub row_count: u64,
    pub column_stats: HashMap<String, ColumnStatistic>,
}

impl TableStatistics {
    pub(crate) fn try_from_base_stats_with_confidence(
        base: &crate::sql::optimizer::stats_input::BaseTableStatistics,
    ) -> Option<(Self, Confidence)> {
        use crate::sql::optimizer::stats_input::StatValue;

        let (row_count, row_count_confidence) = match &base.row_count {
            StatValue::Known {
                value, confidence, ..
            } => (*value, *confidence),
            StatValue::Missing { .. } => return None,
        };

        let column_stats = base
            .columns
            .iter()
            .map(|(name, base_column)| {
                let mut stat = ColumnStatistic::unknown();
                let mut confidence = stat.confidence;

                if let StatValue::Known { value, .. } = &base_column.min_value {
                    stat.min_value = *value;
                }
                if let StatValue::Known { value, .. } = &base_column.max_value {
                    stat.max_value = *value;
                }
                if let StatValue::Known { value, .. } = &base_column.nulls_fraction {
                    stat.nulls_fraction = *value;
                }
                if let StatValue::Known { value, .. } = &base_column.average_row_size {
                    stat.average_row_size = *value;
                }
                if let StatValue::Known {
                    value,
                    confidence: field_confidence,
                    source,
                } = &base_column.ndv
                {
                    stat.set_known_ndv(*value, *field_confidence, *source);
                    confidence = confidence.max(*field_confidence);
                }
                stat.confidence = confidence;

                (name.to_ascii_lowercase(), stat)
            })
            .collect();

        Some((
            Self {
                row_count,
                column_stats,
            },
            row_count_confidence,
        ))
    }
}

/// Build table-level statistics from `IcebergDataFileInfo` entries.
///
/// Aggregates row counts and per-column Iceberg statistics across all files.
/// Returns `None` if no file has a row count (e.g., non-Iceberg sources).
///
/// `columns`, when provided, supplies the per-column Arrow data type used to
/// decode manifest `lower_bound`/`upper_bound` bytes into numeric `min_value`
/// / `max_value` ranges. Without it, bounds stay at +/-infinity (the legacy
/// behavior).
#[allow(dead_code)] // kept for tests and external callers that do not have column schema handy
pub fn build_table_statistics(
    files: &[crate::connector::iceberg::scan_model::IcebergDataFileInfo],
) -> Option<TableStatistics> {
    build_table_statistics_with_columns(files, &[])
}

/// Like `build_table_statistics`, but also decodes manifest min/max bounds
/// using the supplied column schema. The `columns` slice should match
/// `TableDef::columns` so that `column.name` maps to the correct Arrow
/// `DataType` for decoding.
pub fn build_table_statistics_with_columns(
    files: &[crate::connector::iceberg::scan_model::IcebergDataFileInfo],
    columns: &[novarocks_catalog::schema::ColumnDef],
) -> Option<TableStatistics> {
    build_table_statistics_with_ndv(files, columns, &HashMap::new(), &HashMap::new())
}

/// Like `build_table_statistics_with_columns`, but additionally accepts an
/// Iceberg Puffin NDV map keyed by column name (lowercased) so that the
/// optimizer can use precise Theta-sketch cardinality where available.
///
/// `name_to_field_id` is unused by this function (NDV is keyed by name to
/// match the column lookup), but is retained on the signature so callers can
/// pre-compute it from `IcebergSchemaDef` once per query.
///
/// Puffin NDV is retained when present for the column. Without Puffin, NDV
/// remains missing and consumers decide whether their local fallback is valid.
///
/// Iceberg manifest `value_counts` is a non-null value count, not an NDV. Using
/// it as distinct-count metadata makes equality predicates on low-cardinality
/// string columns look almost unique, which causes severe join-order mistakes.
pub fn build_table_statistics_with_ndv(
    files: &[crate::connector::iceberg::scan_model::IcebergDataFileInfo],
    columns: &[novarocks_catalog::schema::ColumnDef],
    ndv_by_name: &HashMap<String, f64>,
    _name_to_field_id: &HashMap<String, i32>,
) -> Option<TableStatistics> {
    // Need at least one file with a row count to produce meaningful stats.
    let all_have_row_count = !files.is_empty() && files.iter().all(|f| f.row_count.is_some());
    if !all_have_row_count {
        return None;
    }

    let total_rows: u64 = files
        .iter()
        .map(|f| f.row_count.unwrap().max(0) as u64)
        .sum();

    // Build a column name → Arrow type lookup for bound decoding.
    let type_by_name: HashMap<&str, &DataType> = columns
        .iter()
        .map(|c| (c.name.as_str(), &c.data_type))
        .collect();

    // Aggregate per-column stats across files.
    let mut col_null_total: HashMap<String, i64> = HashMap::new();
    let mut col_size_total: HashMap<String, i64> = HashMap::new();
    let mut col_count: HashMap<String, u64> = HashMap::new();
    let mut col_min: HashMap<String, f64> = HashMap::new();
    let mut col_max: HashMap<String, f64> = HashMap::new();

    for file in files {
        if let Some(ref cs) = file.column_stats {
            for (col_name, stats) in cs {
                *col_count.entry(col_name.clone()).or_default() += 1;
                if let Some(nc) = stats.null_count {
                    *col_null_total.entry(col_name.clone()).or_default() += nc;
                }
                if let Some(sz) = stats.column_size {
                    *col_size_total.entry(col_name.clone()).or_default() += sz;
                }
                if let Some(dtype) = type_by_name.get(col_name.as_str()) {
                    if let Some(bytes) = stats.lower_bound.as_deref()
                        && let Some(lo) = decode_bound_to_f64(bytes, dtype)
                    {
                        let entry = col_min.entry(col_name.clone()).or_insert(lo);
                        if lo < *entry {
                            *entry = lo;
                        }
                    }
                    if let Some(bytes) = stats.upper_bound.as_deref()
                        && let Some(hi) = decode_bound_to_f64(bytes, dtype)
                    {
                        let entry = col_max.entry(col_name.clone()).or_insert(hi);
                        if hi > *entry {
                            *entry = hi;
                        }
                    }
                }
            }
        }
    }

    let num_files = files.len() as u64;
    let mut column_stats = HashMap::new();
    for (col_name, count) in &col_count {
        // Only include columns that appear in all files for consistency.
        if *count < num_files {
            continue;
        }
        let nulls = col_null_total.get(col_name).copied().unwrap_or(0);
        let nulls_fraction = if total_rows > 0 {
            nulls as f64 / total_rows as f64
        } else {
            0.0
        };
        let avg_row_size = if total_rows > 0 {
            let total_size = col_size_total.get(col_name).copied().unwrap_or(0);
            total_size as f64 / total_rows as f64
        } else {
            8.0
        };
        let min_value = col_min.get(col_name).copied().unwrap_or(f64::NEG_INFINITY);
        let max_value = col_max.get(col_name).copied().unwrap_or(f64::INFINITY);
        let key = col_name.to_lowercase();
        let non_null = (total_rows as f64 * (1.0 - nulls_fraction)).max(1.0);
        let known_ndv = ndv_by_name
            .get(&key)
            .filter(|ndv| ndv.is_finite() && **ndv >= 0.0)
            .map(|ndv| ndv.min(non_null).max(1.0));
        let mut stat = ColumnStatistic {
            min_value,
            max_value,
            nulls_fraction,
            average_row_size: if avg_row_size > 0.0 {
                avg_row_size
            } else {
                8.0
            },
            confidence: if known_ndv.is_some() {
                Confidence::Exact
            } else {
                Confidence::Fallback
            },
            ..ColumnStatistic::unknown()
        };
        if let Some(ndv) = known_ndv {
            stat.set_known_ndv(ndv, Confidence::Exact, StatsSource::IcebergPuffin);
        }
        column_stats.insert(col_name.clone(), stat);
    }

    Some(TableStatistics {
        row_count: total_rows,
        column_stats,
    })
}

#[allow(dead_code)] // Task 5 consumes this through QueryStatsCollector.
pub(crate) fn build_base_table_statistics_with_ndv(
    files: &[crate::connector::iceberg::scan_model::IcebergDataFileInfo],
    columns: &[novarocks_catalog::schema::ColumnDef],
    ndv_by_name: &HashMap<String, f64>,
    name_to_field_id: &HashMap<String, i32>,
) -> crate::sql::optimizer::stats_input::BaseTableStatistics {
    use crate::sql::optimizer::stats_input::{
        BaseColumnStatistics, BaseTableStatistics, StatValue, StatsMissingReason, StatsSource,
    };

    if files.is_empty() {
        return BaseTableStatistics {
            row_count: StatValue::known(0, Confidence::Exact, StatsSource::IcebergManifest),
            columns: HashMap::new(),
            source: StatsSource::IcebergManifest,
        };
    }

    if files.iter().any(|file| file.row_count.is_none()) {
        return BaseTableStatistics::missing(StatsMissingReason::ManifestMissingRowCount);
    }

    let total_rows: u64 = files
        .iter()
        .map(|file| file.row_count.unwrap().max(0) as u64)
        .sum();
    let type_by_name: HashMap<String, &DataType> = columns
        .iter()
        .map(|column| (column.name.to_ascii_lowercase(), &column.data_type))
        .collect();
    let mut column_names: Vec<String> = type_by_name.keys().cloned().collect();
    for name in ndv_by_name.keys().chain(name_to_field_id.keys()) {
        let lower = name.to_ascii_lowercase();
        if !column_names.iter().any(|existing| existing == &lower) {
            column_names.push(lower);
        }
    }

    let columns = column_names
        .into_iter()
        .map(|column_name| {
            let missing_reason = StatsMissingReason::ColumnNotReported(column_name.clone());
            let mut all_null_counts = true;
            let mut null_count_total: i64 = 0;
            let mut all_column_sizes = true;
            let mut column_size_total: i64 = 0;
            let mut all_lower_bounds = true;
            let mut min_value: Option<f64> = None;
            let mut all_upper_bounds = true;
            let mut max_value: Option<f64> = None;

            for file in files {
                let file_stats = file.column_stats.as_ref().and_then(|stats| {
                    stats
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(&column_name))
                        .map(|(_, stats)| stats)
                });
                match file_stats.and_then(|stats| stats.null_count) {
                    Some(value) => null_count_total += value,
                    None => all_null_counts = false,
                }
                match file_stats.and_then(|stats| stats.column_size) {
                    Some(value) => column_size_total += value,
                    None => all_column_sizes = false,
                }

                let data_type = type_by_name.get(&column_name).copied();
                let decoded_lower = data_type.and_then(|data_type| {
                    file_stats
                        .and_then(|stats| stats.lower_bound.as_deref())
                        .and_then(|bytes| decode_bound_to_f64(bytes, data_type))
                        .filter(|value| value.is_finite())
                });
                match decoded_lower {
                    Some(value) => {
                        min_value = Some(min_value.map_or(value, |current| current.min(value)))
                    }
                    None => all_lower_bounds = false,
                }
                let decoded_upper = data_type.and_then(|data_type| {
                    file_stats
                        .and_then(|stats| stats.upper_bound.as_deref())
                        .and_then(|bytes| decode_bound_to_f64(bytes, data_type))
                        .filter(|value| value.is_finite())
                });
                match decoded_upper {
                    Some(value) => {
                        max_value = Some(max_value.map_or(value, |current| current.max(value)))
                    }
                    None => all_upper_bounds = false,
                }
            }

            let nulls_fraction = if all_null_counts {
                let fraction = if total_rows == 0 {
                    0.0
                } else {
                    null_count_total as f64 / total_rows as f64
                };
                StatValue::known(fraction, Confidence::Exact, StatsSource::IcebergManifest)
            } else {
                StatValue::missing(missing_reason.clone())
            };
            let average_row_size = if all_column_sizes {
                let average = if total_rows == 0 {
                    0.0
                } else {
                    column_size_total as f64 / total_rows as f64
                };
                StatValue::known(average, Confidence::Exact, StatsSource::IcebergManifest)
            } else {
                StatValue::missing(missing_reason.clone())
            };
            let min_value = if all_lower_bounds {
                min_value
                    .map(|value| {
                        StatValue::known(value, Confidence::Exact, StatsSource::IcebergManifest)
                    })
                    .unwrap_or_else(|| StatValue::missing(missing_reason.clone()))
            } else {
                StatValue::missing(missing_reason.clone())
            };
            let max_value = if all_upper_bounds {
                max_value
                    .map(|value| {
                        StatValue::known(value, Confidence::Exact, StatsSource::IcebergManifest)
                    })
                    .unwrap_or_else(|| StatValue::missing(missing_reason.clone()))
            } else {
                StatValue::missing(missing_reason.clone())
            };
            let ndv = ndv_by_name
                .get(&column_name)
                .filter(|value| value.is_finite() && **value >= 0.0)
                .map(|value| {
                    StatValue::known(*value, Confidence::Exact, StatsSource::IcebergPuffin)
                })
                .unwrap_or_else(|| StatValue::missing(missing_reason.clone()));

            (
                column_name,
                BaseColumnStatistics {
                    nulls_fraction,
                    average_row_size,
                    min_value,
                    max_value,
                    ndv,
                },
            )
        })
        .collect();

    BaseTableStatistics {
        row_count: StatValue::known(total_rows, Confidence::Exact, StatsSource::IcebergManifest),
        columns,
        source: StatsSource::IcebergManifest,
    }
}

/// Decode an Iceberg manifest lower/upper bound byte payload into a numeric
/// `f64` based on the column's Arrow data type. Returns `None` for types that
/// do not have a meaningful numeric ordering (strings, binary, nested).
///
/// Encoding follows the Iceberg spec (see `Datum::to_bytes`):
/// - BOOLEAN: 1 byte, 0 or 1
/// - INT: 4-byte little-endian i32
/// - LONG / DATE+epoch days are encoded as INT; TIMESTAMP/TIMESTAMPTZ as LONG
/// - FLOAT: 4-byte little-endian f32
/// - DOUBLE: 8-byte little-endian f64
/// - DECIMAL: big-endian two's-complement unscaled, truncated to min bytes
fn decode_bound_to_f64(bytes: &[u8], dtype: &DataType) -> Option<f64> {
    match dtype {
        DataType::Boolean => match bytes {
            [0] => Some(0.0),
            [1] => Some(1.0),
            _ => None,
        },
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Date32 => {
            if bytes.len() == 4 {
                let arr: [u8; 4] = bytes.try_into().ok()?;
                Some(f64::from(i32::from_le_bytes(arr)))
            } else {
                None
            }
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time64(_)
        | DataType::Duration(_) => {
            if bytes.len() == 8 {
                let arr: [u8; 8] = bytes.try_into().ok()?;
                Some(i64::from_le_bytes(arr) as f64)
            } else {
                None
            }
        }
        DataType::Time32(_) => {
            if bytes.len() == 4 {
                let arr: [u8; 4] = bytes.try_into().ok()?;
                Some(f64::from(i32::from_le_bytes(arr)))
            } else {
                None
            }
        }
        DataType::Float32 => {
            if bytes.len() == 4 {
                let arr: [u8; 4] = bytes.try_into().ok()?;
                Some(f64::from(f32::from_le_bytes(arr)))
            } else {
                None
            }
        }
        DataType::Float64 => {
            if bytes.len() == 8 {
                let arr: [u8; 8] = bytes.try_into().ok()?;
                Some(f64::from_le_bytes(arr))
            } else {
                None
            }
        }
        DataType::Decimal128(_, scale) => decode_decimal_be_bytes(bytes, *scale as i32),
        DataType::Decimal256(_, scale) => decode_decimal_be_bytes(bytes, *scale as i32),
        // Strings, binary, nested and other types have no meaningful numeric
        // ordering for optimizer cost; leave bounds unset.
        _ => None,
    }
}

/// Decode a big-endian two's-complement unscaled decimal byte payload into an
/// approximate `f64` using the given scale. Lossy for large precision but
/// sufficient as a cost-model bound.
fn decode_decimal_be_bytes(bytes: &[u8], scale: i32) -> Option<f64> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    // Sign-extend to 16 bytes.
    let sign_byte = bytes[0];
    let is_negative = sign_byte & 0x80 != 0;
    let mut buf = [if is_negative { 0xFF } else { 0x00 }; 16];
    let start = 16 - bytes.len();
    buf[start..].copy_from_slice(bytes);
    let raw = i128::from_be_bytes(buf);
    let pow = 10f64.powi(scale);
    if pow == 0.0 {
        None
    } else {
        Some(raw as f64 / pow)
    }
}

/// Selectivity constants aligned with StarRocks StatisticsEstimateCoefficient.
pub const PREDICATE_UNKNOWN_FILTER: f64 = 0.25;
pub const IS_NULL_FILTER: f64 = 0.1;
pub const IN_PREDICATE_DEFAULT_FILTER: f64 = 0.5;
pub const UNKNOWN_GROUP_BY_CORRELATION: f64 = 0.75;
pub const SEMI_JOIN_SELECTIVITY: f64 = 0.3;
pub const ANTI_JOIN_SELECTIVITY: f64 = 0.4;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::stats_input::{
        BaseColumnStatistics, BaseTableStatistics, StatValue, StatsMissingReason, StatsSource,
    };

    #[test]
    fn base_stats_adapter_does_not_promote_missing_column_confidence() {
        let mut columns = HashMap::new();
        columns.insert(
            "k".to_string(),
            BaseColumnStatistics {
                nulls_fraction: StatValue::known(
                    0.1,
                    Confidence::Exact,
                    StatsSource::IcebergManifest,
                ),
                average_row_size: StatValue::known(
                    8.0,
                    Confidence::Exact,
                    StatsSource::IcebergManifest,
                ),
                min_value: StatValue::known(1.0, Confidence::Exact, StatsSource::IcebergManifest),
                max_value: StatValue::known(9.0, Confidence::Exact, StatsSource::IcebergManifest),
                ndv: StatValue::missing(StatsMissingReason::ColumnNotReported("k".to_string())),
            },
        );
        let base = BaseTableStatistics {
            row_count: StatValue::known(10, Confidence::Exact, StatsSource::IcebergManifest),
            columns,
            source: StatsSource::IcebergManifest,
        };

        let (converted, _) =
            TableStatistics::try_from_base_stats_with_confidence(&base).expect("converted stats");

        assert_eq!(converted.row_count, 10);
        assert_eq!(converted.column_stats["k"].confidence, Confidence::Fallback);
    }

    #[test]
    fn cost_estimate_total() {
        let cost = CostEstimate {
            cpu_cost: 100.0,
            memory_cost: 50.0,
            network_cost: 0.0,
        };
        assert!((cost.total_cost() - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn cost_estimate_add() {
        let a = CostEstimate {
            cpu_cost: 10.0,
            memory_cost: 20.0,
            network_cost: 5.0,
        };
        let b = CostEstimate {
            cpu_cost: 30.0,
            memory_cost: 10.0,
            network_cost: 15.0,
        };
        let c = a.add(&b);
        assert!((c.cpu_cost - 40.0).abs() < f64::EPSILON);
        assert!((c.memory_cost - 30.0).abs() < f64::EPSILON);
        assert!((c.network_cost - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn cost_estimate_sanitized_caps_positive_infinity_and_drops_invalid_values() {
        let cost = CostEstimate {
            cpu_cost: f64::INFINITY,
            memory_cost: f64::NAN,
            network_cost: -1.0,
        }
        .sanitized();

        assert_eq!(cost.cpu_cost, MAX_FINITE_COST);
        assert_eq!(cost.memory_cost, 0.0);
        assert_eq!(cost.network_cost, 0.0);
    }

    #[test]
    fn cost_estimate_add_sanitized_never_keeps_infinite_dimensions() {
        let a = CostEstimate {
            cpu_cost: f64::INFINITY,
            memory_cost: 10.0,
            network_cost: f64::NAN,
        };
        let b = CostEstimate {
            cpu_cost: 1.0,
            memory_cost: f64::INFINITY,
            network_cost: f64::NEG_INFINITY,
        };

        let sum = a.add_sanitized(&b);

        assert_eq!(sum.cpu_cost, MAX_FINITE_COST);
        assert_eq!(sum.memory_cost, MAX_FINITE_COST);
        assert_eq!(sum.network_cost, 0.0);
        assert!(sum.cpu_cost.is_finite());
        assert!(sum.memory_cost.is_finite());
        assert!(sum.network_cost.is_finite());
    }

    #[test]
    fn weighted_total_keeps_positive_infinity_expensive_after_sanitize() {
        let cost = CostEstimate {
            cpu_cost: f64::INFINITY,
            memory_cost: f64::NAN,
            network_cost: -1.0,
        };

        assert_eq!(cost.weighted_total(1.0, 1.0, 1.0), MAX_FINITE_COST);
    }

    #[test]
    fn statistics_compute_size_for_requested_columns() {
        let mut stats = Statistics {
            output_row_count: 10.0,
            ..Default::default()
        };
        stats.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                average_row_size: 4.0,
                ..Default::default()
            },
        );
        stats.column_statistics.insert(
            ColumnId::new_for_test(2),
            ColumnStatistic {
                average_row_size: 16.0,
                ..Default::default()
            },
        );

        assert_eq!(
            stats.compute_size_for_columns(&[ColumnId::new_for_test(2)]),
            160.0
        );
        assert_eq!(
            stats.compute_size_for_columns(&[ColumnId::new_for_test(1), ColumnId::new_for_test(2)]),
            200.0
        );
    }

    #[test]
    fn statistics_compute_size_for_missing_columns_uses_default_width() {
        let stats = Statistics {
            output_row_count: 5.0,
            ..Default::default()
        };

        assert_eq!(
            stats.compute_size_for_columns(&[ColumnId::new_for_test(99)]),
            40.0
        );
    }

    #[test]
    fn base_table_stats_adapter_requires_known_row_count() {
        let base = BaseTableStatistics::missing(StatsMissingReason::NoDataFiles);

        assert!(TableStatistics::try_from_base_stats_with_confidence(&base).is_none());
    }

    #[test]
    fn base_table_stats_adapter_lowercases_columns_and_preserves_known_values() {
        let mut columns = HashMap::new();
        columns.insert(
            "OrderKey".to_string(),
            BaseColumnStatistics {
                nulls_fraction: StatValue::known(0.25, Confidence::Estimated, StatsSource::Derived),
                average_row_size: StatValue::known(
                    16.0,
                    Confidence::Exact,
                    StatsSource::IcebergManifest,
                ),
                min_value: StatValue::known(1.0, Confidence::Measured, StatsSource::TestFixture),
                max_value: StatValue::missing(StatsMissingReason::StatsFileMissing),
                ndv: StatValue::known(100.0, Confidence::Exact, StatsSource::IcebergPuffin),
            },
        );
        let base = BaseTableStatistics {
            row_count: StatValue::known(1000, Confidence::Estimated, StatsSource::Derived),
            columns,
            source: StatsSource::Derived,
        };

        let (table_stats, _) = TableStatistics::try_from_base_stats_with_confidence(&base).unwrap();
        let column = table_stats.column_stats.get("orderkey").unwrap();

        assert_eq!(table_stats.row_count, 1000);
        assert_eq!(column.nulls_fraction, 0.25);
        assert_eq!(column.average_row_size, 16.0);
        assert_eq!(column.min_value, 1.0);
        assert_eq!(column.max_value, f64::INFINITY);
        assert_eq!(column.ndv_or_legacy_unknown_sentinel_for_test(), 100.0);
        assert_eq!(column.confidence, Confidence::Exact);
    }

    #[test]
    fn statistics_compute_size_for_columns_saturates_invalid_sizes() {
        let mut invalid_stats = Statistics {
            output_row_count: f64::NAN,
            ..Default::default()
        };
        invalid_stats.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                average_row_size: f64::NAN,
                ..Default::default()
            },
        );

        assert_eq!(
            invalid_stats.compute_size_for_columns(&[ColumnId::new_for_test(1)]),
            8.0
        );
        assert_eq!(invalid_stats.compute_size_for_columns(&[]), 8.0);

        let mut infinite_row_stats = Statistics {
            output_row_count: f64::INFINITY,
            ..Default::default()
        };
        infinite_row_stats.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                average_row_size: 4.0,
                ..Default::default()
            },
        );

        assert_eq!(
            infinite_row_stats.compute_size_for_columns(&[ColumnId::new_for_test(1)]),
            Statistics::MAX_FINITE_SIZE
        );
        assert_eq!(
            infinite_row_stats.compute_size_for_columns(&[]),
            Statistics::MAX_FINITE_SIZE
        );

        let mut infinite_width_stats = Statistics {
            output_row_count: 10.0,
            ..Default::default()
        };
        infinite_width_stats.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                average_row_size: f64::INFINITY,
                ..Default::default()
            },
        );

        assert_eq!(
            infinite_width_stats.compute_size_for_columns(&[ColumnId::new_for_test(1)]),
            Statistics::MAX_FINITE_SIZE
        );
        assert_eq!(
            infinite_width_stats.compute_size_for_columns(&[]),
            Statistics::MAX_FINITE_SIZE
        );

        let mut overflow_stats = Statistics {
            output_row_count: 1.0e299,
            ..Default::default()
        };
        overflow_stats.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                average_row_size: 1.0e10,
                ..Default::default()
            },
        );

        assert_eq!(
            overflow_stats.compute_size_for_columns(&[ColumnId::new_for_test(1)]),
            Statistics::MAX_FINITE_SIZE
        );
        assert_eq!(
            overflow_stats.compute_size_for_columns(&[]),
            Statistics::MAX_FINITE_SIZE
        );
    }

    #[test]
    fn cost_estimate_weighted_total_uses_explicit_weights() {
        let cost = CostEstimate {
            cpu_cost: 100.0,
            memory_cost: 10.0,
            network_cost: 20.0,
        };

        assert_eq!(cost.weighted_total(0.5, 2.0, 1.5), 100.0);
    }

    #[test]
    fn generate_series_row_count_uses_wide_arithmetic() {
        assert_eq!(generate_series_row_count_f64(10, 2, -2), 5.0);
        assert_eq!(generate_series_row_count_f64(2, 10, -2), 0.0);
        assert!(generate_series_row_count_f64(i64::MIN, i64::MAX, 1).is_finite());
    }

    #[test]
    fn statistics_compute_size() {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 4.0,
                ..ColumnStatistic::for_test_with_ndv(50.0, Confidence::Exact)
            },
        );
        col_stats.insert(
            ColumnId::new_for_test(2),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1000.0,
                nulls_fraction: 0.1,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(200.0, Confidence::Exact)
            },
        );
        let stats = Statistics {
            output_row_count: 1000.0,
            column_statistics: col_stats,
            ..Default::default()
        };
        assert!((stats.compute_size() - 12000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn statistics_empty_columns_default_size() {
        let stats = Statistics {
            output_row_count: 100.0,
            column_statistics: HashMap::new(),
            ..Default::default()
        };
        assert!((stats.avg_row_size() - 8.0).abs() < f64::EPSILON);
    }

    #[test]
    fn column_statistic_unknown() {
        let cs = ColumnStatistic::unknown();
        assert!(cs.min_value.is_infinite());
        assert_eq!(cs.ndv_or_legacy_unknown_sentinel_for_test(), 1.0);
    }

    #[test]
    fn statistics_default_confidence_fields() {
        let unknown = ColumnStatistic::unknown();
        assert_eq!(unknown.confidence, Confidence::Fallback);

        let column_default = ColumnStatistic::default();
        assert_eq!(column_default.confidence, Confidence::Fallback);

        let stats_default = Statistics::default();
        assert_eq!(stats_default.row_count_confidence, Confidence::Fallback);
    }

    #[test]
    fn confidence_strict_total_order() {
        // Variant order must be Fallback < Estimated < Exact < Measured so that
        // the `< Exact` comparison in cost.rs correctly trusts Exact and Measured.
        assert!(Confidence::Measured > Confidence::Exact);
        assert!(Confidence::Exact > Confidence::Estimated);
        assert!(Confidence::Estimated > Confidence::Fallback);
        // Default must stay Fallback.
        assert_eq!(Confidence::default(), Confidence::Fallback);
    }

    #[test]
    fn decode_int_bound_le_bytes() {
        let bytes = (-12345_i32).to_le_bytes();
        let v = decode_bound_to_f64(&bytes, &DataType::Int32).expect("decode int");
        assert!((v - -12345.0).abs() < f64::EPSILON);
    }

    #[test]
    fn decode_long_bound_le_bytes() {
        let bytes = (9_876_543_210_i64).to_le_bytes();
        let v = decode_bound_to_f64(&bytes, &DataType::Int64).expect("decode long");
        assert!((v - 9_876_543_210.0).abs() < 1.0);
    }

    #[test]
    fn decode_double_bound_le_bytes() {
        let bytes = (12.345_f64).to_le_bytes();
        let v = decode_bound_to_f64(&bytes, &DataType::Float64).expect("decode double");
        assert!((v - 12.345).abs() < 1e-9);
    }

    #[test]
    fn decode_float_bound_le_bytes() {
        let bytes = (2.5_f32).to_le_bytes();
        let v = decode_bound_to_f64(&bytes, &DataType::Float32).expect("decode float");
        assert!((v - 2.5).abs() < 1e-6);
    }

    #[test]
    fn decode_boolean_bound() {
        let lo = decode_bound_to_f64(&[0u8], &DataType::Boolean).expect("decode false");
        let hi = decode_bound_to_f64(&[1u8], &DataType::Boolean).expect("decode true");
        assert_eq!(lo, 0.0);
        assert_eq!(hi, 1.0);
    }

    #[test]
    fn decode_timestamp_bound_le_bytes() {
        // 2026-01-01T00:00:00Z in microseconds-since-epoch
        let micros: i64 = 1_767_225_600_000_000;
        let bytes = micros.to_le_bytes();
        use arrow::datatypes::TimeUnit;
        let v = decode_bound_to_f64(&bytes, &DataType::Timestamp(TimeUnit::Microsecond, None))
            .expect("decode ts");
        assert!((v - micros as f64).abs() < 1.0);
    }

    #[test]
    fn decode_date_bound_le_bytes() {
        let days: i32 = 20_454; // ~2026-01-01
        let bytes = days.to_le_bytes();
        let v = decode_bound_to_f64(&bytes, &DataType::Date32).expect("decode date");
        assert!((v - 20_454.0).abs() < f64::EPSILON);
    }

    #[test]
    fn decode_string_bound_returns_none() {
        let bytes = b"hello";
        assert!(decode_bound_to_f64(bytes, &DataType::Utf8).is_none());
    }

    #[test]
    fn decode_truncated_int_bytes_returns_none() {
        let bytes = [0u8, 1u8]; // too short for i32
        assert!(decode_bound_to_f64(&bytes, &DataType::Int32).is_none());
    }

    #[test]
    fn decode_decimal_be_bytes_basic() {
        // Decimal(10, 2) value = 12345 → 123.45
        let raw: i128 = 12345;
        // Big-endian, minimum bytes (truncated).
        let be = raw.to_be_bytes();
        // Strip leading zero bytes per Iceberg spec.
        let start = be.iter().position(|&b| b != 0).unwrap_or(15);
        let bytes = &be[start..];
        let v = decode_decimal_be_bytes(bytes, 2).expect("decode decimal");
        assert!((v - 123.45).abs() < 1e-9);
    }

    #[test]
    fn decode_decimal_be_bytes_negative() {
        let raw: i128 = -250;
        let be = raw.to_be_bytes();
        // Negative values: pick a minimal sign-extension slice. Use last 2 bytes
        // since -250 fits in i16 range (0xFF06).
        let bytes = &be[14..];
        let v = decode_decimal_be_bytes(bytes, 1).expect("decode neg decimal");
        assert!((v - -25.0).abs() < 1e-9);
    }

    #[test]
    fn build_table_statistics_decodes_int_min_max_without_using_value_count_as_ndv() {
        use crate::connector::iceberg::scan_model::{IcebergColumnStats, IcebergDataFileInfo};
        use novarocks_catalog::schema::ColumnDef;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(100),
            column_stats: Some(HashMap::from([(
                "a".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: Some(60),
                    column_size: Some(400),
                    lower_bound: Some(10_i32.to_le_bytes().to_vec()),
                    upper_bound: Some(100_i32.to_le_bytes().to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let cols = vec![ColumnDef {
            name: "a".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let ts = build_table_statistics_with_columns(&[file], &cols).expect("table stats present");
        let col = ts.column_stats.get("a").expect("col stats present");
        assert!((col.min_value - 10.0).abs() < f64::EPSILON);
        assert!((col.max_value - 100.0).abs() < f64::EPSILON);
        // Iceberg value_count is a non-null row count, not a distinct-value
        // count. Without Puffin NDV, leave NDV missing.
        assert!(col.ndv_value().is_none());
        assert_eq!(col.ndv_or_legacy_unknown_sentinel_for_test(), 1.0);
        assert_eq!(col.confidence, Confidence::Fallback);
    }

    #[test]
    fn build_table_statistics_skips_string_bounds() {
        use crate::connector::iceberg::scan_model::{IcebergColumnStats, IcebergDataFileInfo};
        use novarocks_catalog::schema::ColumnDef;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(50),
            column_stats: Some(HashMap::from([(
                "name".to_string(),
                IcebergColumnStats {
                    null_count: Some(5),
                    value_count: None,
                    column_size: Some(200),
                    lower_bound: Some(b"alice".to_vec()),
                    upper_bound: Some(b"zoe".to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let cols = vec![ColumnDef {
            name: "name".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let ts = build_table_statistics_with_columns(&[file], &cols).expect("table stats present");
        let col = ts.column_stats.get("name").expect("col stats present");
        // String bounds are not decoded, so min/max stay at +/-infinity.
        assert!(col.min_value.is_infinite() && col.min_value.is_sign_negative());
        assert!(col.max_value.is_infinite() && col.max_value.is_sign_positive());
    }

    #[test]
    fn build_table_statistics_without_columns_leaves_ndv_missing() {
        use crate::connector::iceberg::scan_model::{IcebergColumnStats, IcebergDataFileInfo};

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(10_000),
            column_stats: Some(HashMap::from([(
                "x".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: None,
                    column_size: None,
                    lower_bound: None,
                    upper_bound: None,
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let ts = build_table_statistics(&[file]).expect("table stats present");
        let col = ts.column_stats.get("x").expect("col stats present");
        // No Puffin NDV means no reliable distinct-count metadata.
        assert!(col.ndv_value().is_none());
        assert_eq!(col.ndv_or_legacy_unknown_sentinel_for_test(), 1.0);
        assert_eq!(col.confidence, Confidence::Fallback);
    }

    #[test]
    fn build_table_statistics_with_ndv_overrides_value_count_heuristic() {
        use crate::connector::iceberg::scan_model::{IcebergColumnStats, IcebergDataFileInfo};
        use novarocks_catalog::schema::ColumnDef;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(10_000),
            column_stats: Some(HashMap::from([(
                "x".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    // Manifest value_count would give NDV=8000; the Puffin
                    // NDV must override.
                    value_count: Some(8000),
                    column_size: None,
                    lower_bound: None,
                    upper_bound: None,
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let cols = vec![ColumnDef {
            name: "x".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];
        let mut ndv_by_name = HashMap::new();
        ndv_by_name.insert("x".to_string(), 1234.0);
        let ts = build_table_statistics_with_ndv(&[file], &cols, &ndv_by_name, &HashMap::new())
            .expect("table stats");
        let col = ts.column_stats.get("x").expect("col stats present");
        // Puffin NDV (1234) wins over manifest value_count (8000) and the
        // heuristic (sqrt(10000)*10 = 1000).
        assert!((col.ndv_or_legacy_unknown_sentinel_for_test() - 1234.0).abs() < f64::EPSILON);
        assert_eq!(col.confidence, Confidence::Exact);
    }

    #[test]
    fn confidence_ordering_and_combine() {
        use Confidence::*;
        assert!(Exact > Estimated && Estimated > Fallback);
        // combine = least-confident wins
        assert_eq!(Exact.combine(Fallback), Fallback);
        assert_eq!(Exact.combine(Estimated), Estimated);
        // derive: a formula result is at best Estimated; any Fallback input -> Fallback
        assert_eq!(Confidence::derive(&[Exact, Exact], false), Estimated);
        assert_eq!(Confidence::derive(&[Exact, Fallback], false), Fallback);
        assert_eq!(Confidence::derive(&[Exact, Exact], true), Fallback);
        assert_eq!(Confidence::default(), Fallback);
    }

    #[test]
    fn build_table_statistics_with_ndv_clamps_to_non_null_count() {
        use crate::connector::iceberg::scan_model::{IcebergColumnStats, IcebergDataFileInfo};
        use novarocks_catalog::schema::ColumnDef;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(1_000),
            column_stats: Some(HashMap::from([(
                "x".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: Some(1000),
                    column_size: None,
                    lower_bound: None,
                    upper_bound: None,
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let cols = vec![ColumnDef {
            name: "x".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];
        // NDV overshoots row count — clamp.
        let mut ndv_by_name = HashMap::new();
        ndv_by_name.insert("x".to_string(), 1e7);
        let ts = build_table_statistics_with_ndv(&[file], &cols, &ndv_by_name, &HashMap::new())
            .expect("table stats");
        let col = ts.column_stats.get("x").expect("col stats present");
        // Clamped to non_null = 1000.
        assert!((col.ndv_or_legacy_unknown_sentinel_for_test() - 1000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn build_base_table_statistics_empty_files_returns_exact_zero() {
        let base = build_base_table_statistics_with_ndv(&[], &[], &HashMap::new(), &HashMap::new());

        assert_eq!(
            base.row_count,
            StatValue::known(0, Confidence::Exact, StatsSource::IcebergManifest)
        );
        assert!(base.columns.is_empty());
        assert_eq!(base.source, StatsSource::IcebergManifest);
    }

    #[test]
    fn build_base_table_statistics_missing_row_count_stays_missing() {
        use crate::connector::iceberg::scan_model::IcebergDataFileInfo;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: None,
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };

        let base =
            build_base_table_statistics_with_ndv(&[file], &[], &HashMap::new(), &HashMap::new());

        assert_eq!(
            base,
            BaseTableStatistics::missing(StatsMissingReason::ManifestMissingRowCount)
        );
    }

    #[test]
    fn build_base_table_statistics_keeps_puffin_ndv_without_manifest_column_stats() {
        use crate::connector::iceberg::scan_model::IcebergDataFileInfo;
        use novarocks_catalog::schema::ColumnDef;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(100),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let columns = vec![ColumnDef {
            name: "OrderKey".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];
        let mut ndv_by_name = HashMap::new();
        ndv_by_name.insert("orderkey".to_string(), 17.0);

        let base =
            build_base_table_statistics_with_ndv(&[file], &columns, &ndv_by_name, &HashMap::new());

        let col = base.columns.get("orderkey").expect("lowercase key");
        assert_eq!(
            col.ndv,
            StatValue::known(17.0, Confidence::Exact, StatsSource::IcebergPuffin)
        );
        assert_eq!(
            col.nulls_fraction,
            StatValue::missing(StatsMissingReason::ColumnNotReported(
                "orderkey".to_string()
            ))
        );
        assert_eq!(
            col.average_row_size,
            StatValue::missing(StatsMissingReason::ColumnNotReported(
                "orderkey".to_string()
            ))
        );
    }

    #[test]
    fn build_base_table_statistics_marks_heuristic_ndv_missing() {
        use crate::connector::iceberg::scan_model::{IcebergColumnStats, IcebergDataFileInfo};
        use novarocks_catalog::schema::ColumnDef;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(100),
            column_stats: Some(HashMap::from([(
                "OrderKey".to_string(),
                IcebergColumnStats {
                    null_count: Some(10),
                    value_count: Some(90),
                    column_size: Some(720),
                    lower_bound: Some(1_i32.to_le_bytes().to_vec()),
                    upper_bound: Some(50_i32.to_le_bytes().to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let columns = vec![ColumnDef {
            name: "OrderKey".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];

        let base = build_base_table_statistics_with_ndv(
            &[file],
            &columns,
            &HashMap::new(),
            &HashMap::new(),
        );

        assert_eq!(
            base.row_count,
            StatValue::known(100, Confidence::Exact, StatsSource::IcebergManifest)
        );
        let col = base.columns.get("orderkey").expect("lowercase key");
        assert_eq!(
            col.nulls_fraction,
            StatValue::known(0.1, Confidence::Exact, StatsSource::IcebergManifest)
        );
        assert_eq!(
            col.average_row_size,
            StatValue::known(7.2, Confidence::Exact, StatsSource::IcebergManifest)
        );
        assert_eq!(
            col.min_value,
            StatValue::known(1.0, Confidence::Exact, StatsSource::IcebergManifest)
        );
        assert_eq!(
            col.max_value,
            StatValue::known(50.0, Confidence::Exact, StatsSource::IcebergManifest)
        );
        assert_eq!(
            col.ndv,
            StatValue::missing(StatsMissingReason::ColumnNotReported(
                "orderkey".to_string()
            ))
        );
    }

    #[test]
    fn build_base_table_statistics_marks_missing_manifest_fields_missing() {
        use crate::connector::iceberg::scan_model::{IcebergColumnStats, IcebergDataFileInfo};
        use novarocks_catalog::schema::ColumnDef;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(100),
            column_stats: Some(HashMap::from([(
                "OrderKey".to_string(),
                IcebergColumnStats {
                    null_count: None,
                    value_count: Some(100),
                    column_size: None,
                    lower_bound: Some(1_i32.to_le_bytes().to_vec()),
                    upper_bound: Some(50_i32.to_le_bytes().to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let columns = vec![ColumnDef {
            name: "OrderKey".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];

        let base = build_base_table_statistics_with_ndv(
            &[file],
            &columns,
            &HashMap::new(),
            &HashMap::new(),
        );

        let col = base.columns.get("orderkey").expect("lowercase key");
        assert_eq!(
            col.nulls_fraction,
            StatValue::missing(StatsMissingReason::ColumnNotReported(
                "orderkey".to_string()
            ))
        );
        assert_eq!(
            col.average_row_size,
            StatValue::missing(StatsMissingReason::ColumnNotReported(
                "orderkey".to_string()
            ))
        );
        assert_eq!(
            col.min_value,
            StatValue::known(1.0, Confidence::Exact, StatsSource::IcebergManifest)
        );
        assert_eq!(
            col.max_value,
            StatValue::known(50.0, Confidence::Exact, StatsSource::IcebergManifest)
        );
    }

    #[test]
    fn build_base_table_statistics_treats_non_finite_float_bounds_as_missing() {
        use crate::connector::iceberg::scan_model::{IcebergColumnStats, IcebergDataFileInfo};
        use novarocks_catalog::schema::ColumnDef;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(10),
            column_stats: Some(HashMap::from([
                (
                    "FloatNaN".to_string(),
                    IcebergColumnStats {
                        null_count: Some(0),
                        value_count: Some(10),
                        column_size: Some(40),
                        lower_bound: Some(f32::NAN.to_le_bytes().to_vec()),
                        upper_bound: Some(3.5_f32.to_le_bytes().to_vec()),
                    },
                ),
                (
                    "DoubleInf".to_string(),
                    IcebergColumnStats {
                        null_count: Some(0),
                        value_count: Some(10),
                        column_size: Some(80),
                        lower_bound: Some(1.25_f64.to_le_bytes().to_vec()),
                        upper_bound: Some(f64::INFINITY.to_le_bytes().to_vec()),
                    },
                ),
            ])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let columns = vec![
            ColumnDef {
                name: "FloatNaN".to_string(),
                data_type: DataType::Float32,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "DoubleInf".to_string(),
                data_type: DataType::Float64,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ];

        let base = build_base_table_statistics_with_ndv(
            &[file],
            &columns,
            &HashMap::new(),
            &HashMap::new(),
        );

        let float_nan = base.columns.get("floatnan").expect("float column");
        assert_eq!(
            float_nan.min_value,
            StatValue::missing(StatsMissingReason::ColumnNotReported(
                "floatnan".to_string()
            ))
        );
        assert_eq!(
            float_nan.max_value,
            StatValue::known(3.5, Confidence::Exact, StatsSource::IcebergManifest)
        );

        let double_inf = base.columns.get("doubleinf").expect("double column");
        assert_eq!(
            double_inf.min_value,
            StatValue::known(1.25, Confidence::Exact, StatsSource::IcebergManifest)
        );
        assert_eq!(
            double_inf.max_value,
            StatValue::missing(StatsMissingReason::ColumnNotReported(
                "doubleinf".to_string()
            ))
        );
    }

    #[test]
    fn build_base_table_statistics_preserves_puffin_ndv() {
        use crate::connector::iceberg::scan_model::{IcebergColumnStats, IcebergDataFileInfo};
        use novarocks_catalog::schema::ColumnDef;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(100),
            column_stats: Some(HashMap::from([(
                "OrderKey".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: Some(100),
                    column_size: Some(800),
                    lower_bound: None,
                    upper_bound: None,
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let columns = vec![ColumnDef {
            name: "OrderKey".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];
        let mut ndv_by_name = HashMap::new();
        ndv_by_name.insert("orderkey".to_string(), 17.0);

        let base =
            build_base_table_statistics_with_ndv(&[file], &columns, &ndv_by_name, &HashMap::new());

        let col = base.columns.get("orderkey").expect("lowercase key");
        assert_eq!(
            col.ndv,
            StatValue::known(17.0, Confidence::Exact, StatsSource::IcebergPuffin)
        );
    }

    #[test]
    fn build_base_table_statistics_preserves_zero_puffin_ndv() {
        use crate::connector::iceberg::scan_model::IcebergDataFileInfo;
        use novarocks_catalog::schema::ColumnDef;

        let file = IcebergDataFileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(100),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let columns = vec![ColumnDef {
            name: "OrderKey".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];
        let mut ndv_by_name = HashMap::new();
        ndv_by_name.insert("orderkey".to_string(), 0.0);

        let base =
            build_base_table_statistics_with_ndv(&[file], &columns, &ndv_by_name, &HashMap::new());

        let col = base.columns.get("orderkey").expect("lowercase key");
        assert_eq!(
            col.ndv,
            StatValue::known(0.0, Confidence::Exact, StatsSource::IcebergPuffin)
        );
    }
}
