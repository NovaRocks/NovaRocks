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

//! Optimizer-owned amplification estimate for two frozen MV delta branches.

use arrow::datatypes::Schema;
use novarocks_spi::connector::{StatisticsEvidence, StatisticsRowCoverage};
use novarocks_types::schema::ColumnDef;

use crate::common::JoinKind;
use crate::optimizer::{
    estimate::cardinality::{JoinCardInput, estimate_join_cardinality},
    statistics::Confidence,
    stats_input::{BaseTableStatistics, StatValue},
};

pub struct SqlMvContentJoinSource<'a> {
    pub schema: &'a Schema,
    pub evidence: &'a StatisticsEvidence,
}

pub struct SqlMvContentJoinKey<'a> {
    pub left_to: &'a str,
    pub right_from: &'a str,
}

/// The two branches are DeltaLeft JOIN Right@From and Left@To JOIN DeltaRight.
/// Each uses its original occurrence's event upper bound as its delta row
/// count. Pinned endpoint NDVs supply the optimizer's selectivity estimates
/// for both operands. The maximum applicable fanout prices the entire window.
pub fn estimate_content_join_amplification(
    left_to: SqlMvContentJoinSource<'_>,
    right_from: SqlMvContentJoinSource<'_>,
    keys: &[SqlMvContentJoinKey<'_>],
    left_event_rows: u128,
    right_event_rows: u128,
) -> Option<f64> {
    if keys.is_empty() {
        return None;
    }
    let left = pinned_statistics(left_to, keys.iter().map(|key| key.left_to))?;
    let right = pinned_statistics(right_from, keys.iter().map(|key| key.right_from))?;
    let left_rows = row_count(&left.row_count)?;
    let right_rows = row_count(&right.row_count)?;
    let branches = [(true, left_event_rows), (false, right_event_rows)];
    let mut maximum = 1.0_f64;
    for (left_is_delta, events) in branches {
        if events == 0 {
            continue;
        }
        let untouched = if left_is_delta { right_rows } else { left_rows };
        if untouched == (0.0, Confidence::Exact) {
            continue;
        }
        if untouched.0 == 0.0 {
            return None;
        }
        let events = u64::try_from(events).ok()? as f64;
        let delta = (events, Confidence::Estimated);
        let (left_input, right_input) = if left_is_delta {
            (delta, right_rows)
        } else {
            (left_rows, delta)
        };
        let eq_key_ndvs = keys
            .iter()
            .map(|key| {
                let left_ndv = ndv(&left, key.left_to)?;
                let right_ndv = ndv(&right, key.right_from)?;
                Some((
                    left_ndv.0.min(left_input.0),
                    right_ndv.0.min(right_input.0),
                    left_ndv.1.combine(right_ndv.1),
                ))
            })
            .collect::<Option<Vec<_>>>()?;
        let (rows, confidence) = estimate_join_cardinality(&JoinCardInput {
            left: left_input,
            right: right_input,
            kind: JoinKind::Inner,
            eq_key_ndvs,
            non_equi_selectivity: None,
        });
        if confidence == Confidence::Fallback || !rows.is_finite() {
            return None;
        }
        maximum = maximum.max(rows / events);
    }
    Some(maximum)
}

fn pinned_statistics<'a>(
    source: SqlMvContentJoinSource<'_>,
    keys: impl IntoIterator<Item = &'a str>,
) -> Option<BaseTableStatistics> {
    if source.evidence.row_coverage() != StatisticsRowCoverage::AllVisibleRows {
        return None;
    }
    let columns = keys
        .into_iter()
        .map(|name| {
            let field = source.schema.field_with_name(name).ok()?;
            Some(ColumnDef {
                name: field.name().to_owned(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                write_default: None,
                logical_type: None,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(super::super::dml::evidence_to_base_statistics(
        source.evidence,
        &columns,
    ))
}

fn row_count(stat: &StatValue<u64>) -> Option<(f64, Confidence)> {
    Some((*stat.known_value()? as f64, stat.confidence()))
}

fn ndv(source: &BaseTableStatistics, name: &str) -> Option<(f64, Confidence)> {
    let value = &source.columns.get(&name.to_ascii_lowercase())?.ndv;
    let observed = *value.known_value()?;
    (observed.is_finite() && observed > 0.0).then_some((observed, value.confidence()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        StatisticsBasisRelation, StatisticsDataVersion, StatisticsEvidenceRevision,
        StatisticsMetric, StatisticsMetricObservation, StatisticsMetricSource,
        StatisticsMetricState, StatisticsMetricValue, StatisticsNumericNature,
    };

    use super::*;
    use crate::compiler::{
        SqlImvJoinContractFacts, SqlImvJoinKindFacts, SqlImvJoinPredicateFacts,
        SqlImvQualifiedFieldFacts, SqlMvRelationOccurrenceId,
    };

    fn evidence(
        version: &'static [u8],
        rows: u64,
        key: &str,
        distinct: Option<(f64, StatisticsBasisRelation)>,
    ) -> StatisticsEvidence {
        let version = StatisticsDataVersion::try_new(Bytes::from_static(version)).unwrap();
        let count = StatisticsMetricObservation::new(
            StatisticsMetricValue::U64(rows),
            version.clone(),
            StatisticsMetricSource::CurrentManifest,
            StatisticsNumericNature::Exact,
            StatisticsBasisRelation::Identical,
        );
        let mut metrics = std::collections::BTreeMap::from([(
            StatisticsMetric::RowCount,
            StatisticsMetricState::Available(count),
        )]);
        if let Some((value, relation)) = distinct {
            let basis = if relation == StatisticsBasisRelation::Identical {
                version.clone()
            } else {
                StatisticsDataVersion::try_new(Bytes::from_static(b"unrelated")).unwrap()
            };
            metrics.insert(
                StatisticsMetric::ThetaNdv {
                    column: Arc::from(key),
                },
                StatisticsMetricState::Available(StatisticsMetricObservation::new(
                    StatisticsMetricValue::F64(value),
                    basis,
                    StatisticsMetricSource::ProviderArtifact,
                    StatisticsNumericNature::TwoSidedApproximate,
                    relation,
                )),
            );
        }
        StatisticsEvidence::try_new(
            version,
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"original-revision")).unwrap(),
            StatisticsRowCoverage::AllVisibleRows,
            metrics,
        )
        .unwrap()
    }

    fn estimate(
        left: &StatisticsEvidence,
        right: &StatisticsEvidence,
        events_left: u128,
        events_right: u128,
    ) -> Option<f64> {
        let left_schema = Schema::new(vec![Field::new("left_key", DataType::Int64, false)]);
        let right_schema = Schema::new(vec![Field::new(
            "right_key_at_from",
            DataType::Int64,
            false,
        )]);
        estimate_content_join_amplification(
            SqlMvContentJoinSource {
                schema: &left_schema,
                evidence: left,
            },
            SqlMvContentJoinSource {
                schema: &right_schema,
                evidence: right,
            },
            &[SqlMvContentJoinKey {
                left_to: "left_key",
                right_from: "right_key_at_from",
            }],
            events_left,
            events_right,
        )
    }

    #[test]
    fn content_join_uses_the_original_delta_counts_and_the_correct_unchanged_endpoint() {
        let left = evidence(
            b"L@To",
            100,
            "left_key",
            Some((7.0, StatisticsBasisRelation::Identical)),
        );
        let right = evidence(
            b"R@From",
            1000,
            "right_key_at_from",
            Some((100.0, StatisticsBasisRelation::Identical)),
        );
        assert_eq!(estimate(&left, &right, 10, 0), Some(10.0));
        assert_eq!(estimate(&left, &right, 0, 20), Some(5.0));
        assert_eq!(estimate(&left, &right, 10, 20), Some(10.0));
    }

    #[test]
    fn content_join_refuses_missing_or_stale_ndv_and_keeps_original_empty_row_evidence() {
        let left = evidence(
            b"L@To",
            100,
            "left_key",
            Some((7.0, StatisticsBasisRelation::Identical)),
        );
        let right_missing = evidence(b"R@From", 1000, "right_key_at_from", None);
        let stale = evidence(
            b"R@From",
            1000,
            "right_key_at_from",
            Some((100.0, StatisticsBasisRelation::BasisIsSubset)),
        );
        assert_eq!(estimate(&left, &right_missing, 10, 20), None);
        assert_eq!(estimate(&left, &stale, 10, 20), None);
        let empty = evidence(b"R@From", 0, "right_key_at_from", None);
        assert_eq!(estimate(&left, &empty, 10, 0), Some(1.0));
    }

    #[test]
    fn content_join_orients_reversed_keys_by_the_original_self_join_occurrences() {
        let left = SqlMvRelationOccurrenceId::new(27);
        let right = SqlMvRelationOccurrenceId::new(21);
        let left_key = SqlImvQualifiedFieldFacts::try_new(
            left,
            "c.s.t".into(),
            "source_left".into(),
            Bytes::from_static(b"field-a"),
        )
        .unwrap();
        let right_key = SqlImvQualifiedFieldFacts::try_new(
            right,
            "c.s.t".into(),
            "source_right".into(),
            Bytes::from_static(b"field-b"),
        )
        .unwrap();
        let join = SqlImvJoinContractFacts::try_new(
            SqlImvJoinKindFacts::InnerEquiJoin,
            vec![SqlImvJoinPredicateFacts::try_new(right_key, left_key).unwrap()],
        )
        .unwrap();
        assert_eq!(
            join.content_key_field_pairs(left, right),
            Some(vec![(&b"field-a"[..], &b"field-b"[..])])
        );
        assert_eq!(join.content_key_field_pairs(left, left), None);
    }
}
