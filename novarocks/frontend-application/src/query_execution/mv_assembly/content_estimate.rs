// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Pre-dispatch content sizing from the original window and pinned statistics.

mod join;
pub(crate) use join::estimate_join_amplification;

use std::{fmt, sync::Arc};

use arrow::datatypes::DataType;
use novarocks_execution::exec::hash_table::{ContentKeyIndexSizing, estimate_content_key_index};
use novarocks_spi::connector::{
    ConnectorChangeWindowFacts, StatisticsDataVersion, StatisticsEvidence,
    StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricSource, StatisticsMetricState,
    StatisticsMetricValue, StatisticsNumericNature, StatisticsRowCoverage,
};
use novarocks_type_contract::FunctionValueType;

pub(crate) struct ContentEstimateColumn<'a> {
    pub(crate) storage_column: &'a str,
    pub(crate) storage_type: &'a FunctionValueType,
    /// Supplied by the original To pin for a verified direct, width-preserving
    /// source field of this output. Expressions have no such leg.
    pub(crate) source: Option<ContentSourceStatistics<'a>>,
}

pub(crate) struct ContentSourceStatistics<'a> {
    pub(crate) occurrence_id: u32,
    pub(crate) source_column: &'a str,
    pub(crate) statistics: &'a StatisticsEvidence,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ContentJoinAmplification {
    NotJoined,
    /// An admitted estimate from the original bound optimizer join.
    OptimizerEstimate(f64),
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ContentAverageSizeBasis {
    pub(crate) pinned_version: StatisticsDataVersion,
    pub(crate) basis_version: StatisticsDataVersion,
    pub(crate) evidence_revision: StatisticsEvidenceRevision,
    pub(crate) metric_source: StatisticsMetricSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ContentColumnWidthSource {
    FixedStorageType,
    StorageAverageSize(ContentAverageSizeBasis),
    DirectSourceAverageSize {
        occurrence_id: u32,
        source_column: String,
        basis: ContentAverageSizeBasis,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ContentColumnWidthEstimate {
    pub(crate) storage_column: String,
    pub(crate) bytes: u128,
    pub(crate) source: ContentColumnWidthSource,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ContentSizeEstimate {
    pub(crate) source_event_rows_upper_bound: u128,
    pub(crate) row_count: u128,
    pub(crate) join: ContentJoinAmplification,
    pub(crate) storage_row_bytes: u128,
    pub(crate) columns: Vec<ContentColumnWidthEstimate>,
    pub(crate) key_index: ContentKeyIndexSizing,
    pub(crate) estimate_bytes: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ContentEstimateUnavailable {
    MissingChangeWindows,
    UnavailableChangeWindow { ordinal: usize },
    MissingStorageColumns,
    MissingColumnWidth { storage_column: String },
    MissingJoinAmplification,
    InvalidJoinAmplification,
    ArithmeticOverflow,
}

impl fmt::Display for ContentEstimateUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingChangeWindows => formatter.write_str("source change windows are missing"),
            Self::UnavailableChangeWindow { ordinal } => {
                write!(
                    formatter,
                    "source occurrence {ordinal} has no change volume"
                )
            }
            Self::MissingStorageColumns => {
                formatter.write_str("storage content columns are missing")
            }
            Self::MissingColumnWidth { storage_column } => {
                write!(
                    formatter,
                    "no row-width evidence for storage column {storage_column}"
                )
            }
            Self::MissingJoinAmplification => {
                formatter.write_str("optimizer join amplification is unavailable")
            }
            Self::InvalidJoinAmplification => {
                formatter.write_str("optimizer join amplification is invalid")
            }
            Self::ArithmeticOverflow => {
                formatter.write_str("content working-size estimate is not representable")
            }
        }
    }
}

type EstimateResult<T> = Result<T, ContentEstimateUnavailable>;

/// The caller has already matched each statistics answer to its exact S or To
/// pin. This function has no I/O or access to session budgets and estimates a
/// single owner holding every original occurrence.
///
/// Data averages and the optimizer factor are estimates rather than bounds.
/// The index reports the locked resident structures it can price before
/// capture. Runtime admission continues to own all actual allocation peaks.
pub(crate) fn estimate_content_apply<'a>(
    windows: impl IntoIterator<Item = &'a ConnectorChangeWindowFacts>,
    columns: &[ContentEstimateColumn<'_>],
    target_statistics: Option<&StatisticsEvidence>,
    join: ContentJoinAmplification,
) -> EstimateResult<ContentSizeEstimate> {
    let source_event_rows_upper_bound = source_event_rows(windows)?;
    let row_count = match join {
        ContentJoinAmplification::NotJoined => source_event_rows_upper_bound,
        ContentJoinAmplification::OptimizerEstimate(factor) => {
            amplified_rows(source_event_rows_upper_bound, factor)?
        }
        ContentJoinAmplification::Unavailable => {
            return Err(ContentEstimateUnavailable::MissingJoinAmplification);
        }
    };
    if columns.is_empty() {
        return Err(ContentEstimateUnavailable::MissingStorageColumns);
    }
    let widths = columns
        .iter()
        .map(|column| estimate_width(column, target_statistics))
        .collect::<EstimateResult<Vec<_>>>()?;
    let storage_row_bytes = widths.iter().try_fold(0u128, |sum, width| {
        sum.checked_add(width.bytes)
            .ok_or(ContentEstimateUnavailable::ArithmeticOverflow)
    })?;
    let key_index = estimate_content_key_index(
        row_count,
        columns.iter().map(|column| column.storage_type.data_type()),
    )
    .ok_or(ContentEstimateUnavailable::ArithmeticOverflow)?;
    let estimate_bytes = storage_row_bytes
        .checked_mul(row_count)
        .and_then(|bytes| bytes.checked_add(key_index.total_bytes))
        .ok_or(ContentEstimateUnavailable::ArithmeticOverflow)?;
    Ok(ContentSizeEstimate {
        source_event_rows_upper_bound,
        row_count,
        join,
        storage_row_bytes,
        columns: widths,
        key_index,
        estimate_bytes,
    })
}

fn source_event_rows<'a>(
    windows: impl IntoIterator<Item = &'a ConnectorChangeWindowFacts>,
) -> EstimateResult<u128> {
    let mut windows = windows.into_iter().enumerate().peekable();
    if windows.peek().is_none() {
        return Err(ContentEstimateUnavailable::MissingChangeWindows);
    }
    windows.try_fold(0u128, |total, (ordinal, window)| {
        let rows = match window {
            ConnectorChangeWindowFacts::NetZero(_) => 0,
            ConnectorChangeWindowFacts::Difference { volume, .. } => {
                volume.source_event_rows_upper_bound
            }
            ConnectorChangeWindowFacts::NotIncremental(_) => {
                return Err(ContentEstimateUnavailable::UnavailableChangeWindow { ordinal });
            }
        };
        total
            .checked_add(rows)
            .ok_or(ContentEstimateUnavailable::ArithmeticOverflow)
    })
}

/// Multiply by the finite optimizer estimate, rounding the original integral
/// count upward without a conversion of the count to floating point.
fn amplified_rows(rows: u128, factor: f64) -> EstimateResult<u128> {
    use ContentEstimateUnavailable::{ArithmeticOverflow, InvalidJoinAmplification};
    if !factor.is_finite() || factor < 1.0 {
        return Err(InvalidJoinAmplification);
    }
    if rows == 0 {
        return Ok(0);
    }
    // Any larger cardinality could not be represented by this owner's index.
    let rows = usize::try_from(rows).map_err(|_| ArithmeticOverflow)? as u128;
    let bits = factor.to_bits();
    let significand = u128::from((1 << 52) | (bits & ((1 << 52) - 1)));
    let exponent = ((bits >> 52) & 0x7ff) as i32 - 1023 - 52;
    let scaled = rows.checked_mul(significand).ok_or(ArithmeticOverflow)?;
    if exponent < 0 {
        Ok(scaled.div_ceil(1 << (-exponent)))
    } else {
        scaled
            .checked_mul(
                1u128
                    .checked_shl(exponent as u32)
                    .ok_or(ArithmeticOverflow)?,
            )
            .ok_or(ArithmeticOverflow)
    }
}

pub(crate) fn needs_average_size(storage_type: &FunctionValueType) -> bool {
    fixed_storage_width(storage_type.data_type()).is_none()
}

/// A usable S value means there is no source fallback read for this column.
pub(crate) fn has_average_size(evidence: &StatisticsEvidence, column: &str) -> bool {
    average_size(evidence, column).is_some()
}

fn fixed_storage_width(ty: &DataType) -> Option<u128> {
    match ty {
        DataType::Null => Some(0),
        // Estimate one byte for the fixed Boolean's content representation.
        DataType::Boolean => Some(1),
        DataType::FixedSizeBinary(width) => u128::try_from(*width).ok(),
        _ => ty.primitive_width().map(|width| width as u128),
    }
}

fn estimate_width(
    column: &ContentEstimateColumn<'_>,
    storage: Option<&StatisticsEvidence>,
) -> EstimateResult<ContentColumnWidthEstimate> {
    let storage_column = column.storage_column;
    let (bytes, source) = if let Some(bytes) = fixed_storage_width(column.storage_type.data_type())
    {
        (bytes, ContentColumnWidthSource::FixedStorageType)
    } else if let Some((bytes, basis)) =
        storage.and_then(|evidence| average_size(evidence, storage_column))
    {
        (bytes, ContentColumnWidthSource::StorageAverageSize(basis))
    } else if let Some(source) = &column.source
        && let Some((bytes, basis)) = average_size(source.statistics, source.source_column)
    {
        (
            bytes,
            ContentColumnWidthSource::DirectSourceAverageSize {
                occurrence_id: source.occurrence_id,
                source_column: source.source_column.to_owned(),
                basis,
            },
        )
    } else {
        return Err(ContentEstimateUnavailable::MissingColumnWidth {
            storage_column: storage_column.to_owned(),
        });
    };
    Ok(ContentColumnWidthEstimate {
        storage_column: storage_column.to_owned(),
        bytes,
        source,
    })
}

fn average_size(
    evidence: &StatisticsEvidence,
    column: &str,
) -> Option<(u128, ContentAverageSizeBasis)> {
    if evidence.row_coverage() != StatisticsRowCoverage::AllVisibleRows {
        return None;
    }
    let metric = StatisticsMetric::AverageSize {
        column: Arc::from(column),
    };
    let StatisticsMetricState::Available(value) = evidence.metrics().get(&metric)? else {
        return None;
    };
    if !value.describes_queried_rows()
        || value.numeric_nature() == StatisticsNumericNature::LowerBound
    {
        return None;
    }
    let bytes = match value.value() {
        StatisticsMetricValue::U64(value) => u128::from(*value),
        StatisticsMetricValue::I64(value) => u128::try_from(*value).ok()?,
        StatisticsMetricValue::F64(value) if value.is_finite() && *value >= 0.0 => {
            let rounded = value.ceil();
            if rounded >= 2_f64.powi(128) {
                return None;
            }
            rounded as u128
        }
        _ => return None,
    };
    Some((
        bytes,
        ContentAverageSizeBasis {
            pinned_version: evidence.data_version().clone(),
            basis_version: value.basis_version().clone(),
            evidence_revision: evidence.evidence_revision().clone(),
            metric_source: value.source().clone(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorChangeWindowNegativeWeight, ConnectorChangeWindowNetZeroBasis,
        ConnectorChangeWindowNotIncrementalReason, ConnectorChangeWindowPartitionImpact,
        ConnectorChangeWindowVolume, StatisticsBasisRelation, StatisticsMetricObservation,
    };

    fn difference(rows: u128) -> ConnectorChangeWindowFacts {
        ConnectorChangeWindowFacts::Difference {
            negative_weight: ConnectorChangeWindowNegativeWeight::Possible,
            partition_impact: ConnectorChangeWindowPartitionImpact::Unavailable,
            volume: ConnectorChangeWindowVolume {
                source_event_rows_upper_bound: rows,
                source_data_file_bytes: 0,
            },
        }
    }

    fn column<'a>(name: &'a str, ty: &'a FunctionValueType) -> ContentEstimateColumn<'a> {
        ContentEstimateColumn {
            storage_column: name,
            storage_type: ty,
            source: None,
        }
    }

    fn stats(
        column: &str,
        value: f64,
        coverage: StatisticsRowCoverage,
        relation: StatisticsBasisRelation,
    ) -> StatisticsEvidence {
        let queried =
            StatisticsDataVersion::try_new(bytes::Bytes::from_static(b"queried")).unwrap();
        let basis = StatisticsDataVersion::try_new(bytes::Bytes::from_static(b"basis")).unwrap();
        StatisticsEvidence::try_new(
            queried,
            StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(b"revision")).unwrap(),
            coverage,
            [(
                StatisticsMetric::AverageSize {
                    column: Arc::from(column),
                },
                StatisticsMetricState::Available(StatisticsMetricObservation::new(
                    StatisticsMetricValue::F64(value),
                    basis,
                    StatisticsMetricSource::CurrentManifest,
                    StatisticsNumericNature::TwoSidedApproximate,
                    relation,
                )),
            )]
            .into_iter()
            .collect(),
        )
        .unwrap()
    }

    #[test]
    fn counts_each_occurrence_and_prices_a_single_full_owner() {
        let integer = FunctionValueType::new(DataType::Int64, false);
        let fixed = FunctionValueType::new(DataType::FixedSizeBinary(12), false);
        let boolean = FunctionValueType::new(DataType::Boolean, false);
        let columns = [
            column("a", &integer),
            column("b", &fixed),
            column("c", &boolean),
        ];
        let left = difference(7);
        let right = difference(11);
        let zero =
            ConnectorChangeWindowFacts::NetZero(ConnectorChangeWindowNetZeroBasis::SameSnapshot);
        let estimate = estimate_content_apply(
            [&left, &right, &left, &zero],
            &columns,
            None,
            ContentJoinAmplification::OptimizerEstimate(1.25),
        )
        .unwrap();
        assert_eq!(estimate.source_event_rows_upper_bound, 25);
        assert_eq!(estimate.row_count, 32);
        assert_eq!(estimate.storage_row_bytes, 21);
        assert_eq!(
            estimate.key_index,
            estimate_content_key_index(
                32,
                columns.iter().map(|column| column.storage_type.data_type())
            )
            .unwrap()
        );
        assert_eq!(
            estimate.estimate_bytes,
            32 * 21 + estimate.key_index.total_bytes
        );
        assert!(
            estimate
                .columns
                .iter()
                .all(|column| column.source == ContentColumnWidthSource::FixedStorageType)
        );
    }

    #[test]
    fn prefers_the_storage_basis_and_falls_back_only_through_a_direct_source() {
        let text = FunctionValueType::new(DataType::Utf8, false);
        let pinned_source = stats(
            "original",
            9.1,
            StatisticsRowCoverage::AllVisibleRows,
            StatisticsBasisRelation::Identical,
        );
        let pinned_storage = stats(
            "stored",
            31.4,
            StatisticsRowCoverage::AllVisibleRows,
            StatisticsBasisRelation::Identical,
        );
        let stale_storage = stats(
            "stored",
            31.4,
            StatisticsRowCoverage::AllVisibleRows,
            StatisticsBasisRelation::BasisIsSubset,
        );
        let columns = [ContentEstimateColumn {
            source: Some(ContentSourceStatistics {
                occurrence_id: 19,
                source_column: "original",
                statistics: &pinned_source,
            }),
            ..column("stored", &text)
        }];
        let window = difference(9);
        let stable = estimate_content_apply(
            [&window],
            &columns,
            Some(&pinned_storage),
            ContentJoinAmplification::NotJoined,
        )
        .unwrap();
        assert_eq!(stable.storage_row_bytes, 32);
        assert!(
            matches!(&stable.columns[0].source, ContentColumnWidthSource::StorageAverageSize(basis)
            if basis.pinned_version == *pinned_storage.data_version())
        );
        let fallback = estimate_content_apply(
            [&window],
            &columns,
            Some(&stale_storage),
            ContentJoinAmplification::NotJoined,
        )
        .unwrap();
        assert_eq!(fallback.storage_row_bytes, 10);
        assert!(
            matches!(&fallback.columns[0].source, ContentColumnWidthSource::DirectSourceAverageSize { occurrence_id: 19, source_column, basis }
            if source_column == "original" && basis.pinned_version == *pinned_source.data_version())
        );
    }

    #[test]
    fn missing_width_or_original_join_evidence_refuses_the_estimate() {
        let text = FunctionValueType::new(DataType::Utf8, false);
        let partial = stats(
            "result",
            2.3,
            StatisticsRowCoverage::PartialRows,
            StatisticsBasisRelation::Identical,
        );
        let window = difference(9);
        assert_eq!(
            estimate_content_apply(
                [&window],
                &[column("result", &text)],
                Some(&partial),
                ContentJoinAmplification::NotJoined
            ),
            Err(ContentEstimateUnavailable::MissingColumnWidth {
                storage_column: "result".into()
            }),
        );
        let integer = FunctionValueType::new(DataType::Int64, false);
        assert_eq!(
            estimate_content_apply(
                [&window],
                &[column("a", &integer)],
                None,
                ContentJoinAmplification::Unavailable
            ),
            Err(ContentEstimateUnavailable::MissingJoinAmplification),
        );
        assert_eq!(
            estimate_content_apply(
                [&window],
                &[column("a", &integer)],
                None,
                ContentJoinAmplification::OptimizerEstimate(f64::NAN)
            ),
            Err(ContentEstimateUnavailable::InvalidJoinAmplification),
        );
    }

    #[test]
    fn change_windows_and_capacity_require_representable_original_facts() {
        let integer = FunctionValueType::new(DataType::Int64, false);
        let columns = [column("a", &integer)];
        let unavailable = ConnectorChangeWindowFacts::NotIncremental(
            ConnectorChangeWindowNotIncrementalReason::SchemaChanged { snapshot_id: 19 },
        );
        let window = difference(1);
        let huge = difference(u128::MAX);
        for (windows, expected) in [
            (vec![], ContentEstimateUnavailable::MissingChangeWindows),
            (
                vec![&window, &unavailable],
                ContentEstimateUnavailable::UnavailableChangeWindow { ordinal: 1 },
            ),
            (
                vec![&huge, &window],
                ContentEstimateUnavailable::ArithmeticOverflow,
            ),
            (vec![&huge], ContentEstimateUnavailable::ArithmeticOverflow),
        ] {
            assert_eq!(
                estimate_content_apply(
                    windows,
                    &columns,
                    None,
                    ContentJoinAmplification::NotJoined
                ),
                Err(expected)
            );
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn amplification_does_not_round_the_original_count_through_f64() {
        let rows = (1u128 << 55) + 1;
        assert_eq!(amplified_rows(rows, 1.0), Ok(rows));
        assert_eq!(amplified_rows(rows, 1.5), Ok(rows.div_ceil(2) + rows));
    }
}
