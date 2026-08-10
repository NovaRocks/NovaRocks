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

use std::cmp::Ordering;

use novarocks_execution::exec::min_max_predicate::{
    MinMaxPredicate, MinMaxPredicateOp, MinMaxPredicateValue,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) enum ScanPredicateSource {
    Static,
    RuntimeIn,
    RuntimeMembership,
    RuntimeMinMax,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScanPredicateDomainKind {
    Range,
    DiscreteSet,
    Membership,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MembershipPredicate {
    BloomProbe { values: Vec<MinMaxPredicateValue> },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ScanPredicateDomain {
    Range {
        op: MinMaxPredicateOp,
        value: MinMaxPredicateValue,
    },
    DiscreteSet {
        values: Vec<MinMaxPredicateValue>,
        min: MinMaxPredicateValue,
        max: MinMaxPredicateValue,
    },
    Membership(MembershipPredicate),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PruneVerdict {
    Skip,
    Keep,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) enum ScanLayer {
    File,
    Split,
    RowGroup,
    Page,
}

pub(crate) type UnitId = usize;

pub(crate) trait ColumnStats {
    fn min_max(&self) -> Option<(MinMaxPredicateValue, MinMaxPredicateValue)> {
        None
    }

    fn contains(&self, _value: &MinMaxPredicateValue) -> Option<bool> {
        None
    }

    fn may_satisfy_range(
        &self,
        op: MinMaxPredicateOp,
        value: &MinMaxPredicateValue,
    ) -> Result<Option<bool>, String> {
        let Some((min, max)) = self.min_max() else {
            return Ok(None);
        };
        Ok(Some(range_stats_may_satisfy(op, value, &min, &max)))
    }

    fn may_satisfy_discrete_set(
        &self,
        values: &[MinMaxPredicateValue],
        min: &MinMaxPredicateValue,
        max: &MinMaxPredicateValue,
    ) -> Result<Option<bool>, String> {
        let mut has_missing_contains_answer = false;
        for value in values {
            match self.contains(value) {
                Some(false) => {}
                Some(true) => return Ok(Some(true)),
                None => has_missing_contains_answer = true,
            }
        }
        if !has_missing_contains_answer {
            return Ok(Some(false));
        }

        let Some((stats_min, stats_max)) = self.min_max() else {
            return Ok(None);
        };
        Ok(Some(ranges_may_overlap(&stats_min, &stats_max, min, max)))
    }
}

pub(crate) trait ScanPruner {
    #[allow(dead_code)]
    fn layer(&self) -> ScanLayer;

    fn accepts_domain(&self, _kind: ScanPredicateDomainKind) -> bool {
        false
    }

    fn units(&self) -> &[UnitId];

    fn column_stats<'a>(&'a self, column: &str, unit: UnitId) -> Option<Box<dyn ColumnStats + 'a>>;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PruneResult {
    pub(crate) kept_units: Vec<UnitId>,
    pub(crate) skipped_units: Vec<UnitId>,
}

impl ScanPredicateDomain {
    pub(crate) fn kind(&self) -> ScanPredicateDomainKind {
        match self {
            Self::Range { .. } => ScanPredicateDomainKind::Range,
            Self::DiscreteSet { .. } => ScanPredicateDomainKind::DiscreteSet,
            Self::Membership(_) => ScanPredicateDomainKind::Membership,
        }
    }

    pub(crate) fn fallback_domains(&self) -> Vec<Self> {
        match self {
            Self::Range { .. } | Self::Membership(_) => Vec::new(),
            Self::DiscreteSet { min, max, .. } => {
                vec![
                    Self::Range {
                        op: MinMaxPredicateOp::Ge,
                        value: min.clone(),
                    },
                    Self::Range {
                        op: MinMaxPredicateOp::Le,
                        value: max.clone(),
                    },
                ]
            }
        }
    }

    pub(crate) fn evaluate(&self, stats: &dyn ColumnStats) -> Result<PruneVerdict, String> {
        let verdict = match self {
            Self::Range { op, value } => {
                if stats.may_satisfy_range(*op, value)? == Some(false) {
                    PruneVerdict::Skip
                } else {
                    PruneVerdict::Keep
                }
            }
            Self::DiscreteSet { values, min, max } => {
                if stats.may_satisfy_discrete_set(values, min, max)? == Some(false) {
                    PruneVerdict::Skip
                } else {
                    PruneVerdict::Keep
                }
            }
            Self::Membership(MembershipPredicate::BloomProbe { values }) => {
                if values_all_absent(stats, values) {
                    PruneVerdict::Skip
                } else {
                    PruneVerdict::Keep
                }
            }
        };
        Ok(verdict)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScanPredicate {
    column: String,
    domain: ScanPredicateDomain,
    source: ScanPredicateSource,
}

impl ScanPredicate {
    pub(crate) fn new(
        column: String,
        domain: ScanPredicateDomain,
        source: ScanPredicateSource,
    ) -> Self {
        Self {
            column,
            domain,
            source,
        }
    }

    pub(crate) fn from_min_max_predicate(
        predicate: MinMaxPredicate,
        source: ScanPredicateSource,
    ) -> Self {
        let column = predicate.column().to_string();
        let domain = ScanPredicateDomain::Range {
            op: predicate.op(),
            value: predicate.value().clone(),
        };
        Self::new(column, domain, source)
    }

    pub(crate) fn discrete_set(
        column: String,
        mut values: Vec<MinMaxPredicateValue>,
        source: ScanPredicateSource,
    ) -> Result<Self, String> {
        let Some(first) = values.first() else {
            return Err("scan predicate discrete set cannot be empty".to_string());
        };
        let Some(family) = ScanPredicateValueFamily::from_value(first) else {
            return Err("mixed scan predicate value families are unsupported".to_string());
        };
        if values
            .iter()
            .any(|value| ScanPredicateValueFamily::from_value(value) != Some(family))
        {
            return Err("mixed scan predicate value families are unsupported".to_string());
        }

        values.sort_by(compare_scan_predicate_values);
        values.dedup_by(|left, right| compare_scan_predicate_values(left, right).is_eq());

        let min = values
            .first()
            .expect("discrete set should retain at least one value")
            .clone();
        let max = values
            .last()
            .expect("discrete set should retain at least one value")
            .clone();

        Ok(Self::new(
            column,
            ScanPredicateDomain::DiscreteSet { values, min, max },
            source,
        ))
    }

    pub(crate) fn column(&self) -> &str {
        &self.column
    }

    pub(crate) fn source(&self) -> ScanPredicateSource {
        self.source
    }

    pub(crate) fn domain(&self) -> &ScanPredicateDomain {
        &self.domain
    }

    pub(crate) fn range_op(&self) -> Option<MinMaxPredicateOp> {
        match &self.domain {
            ScanPredicateDomain::Range { op, .. } => Some(*op),
            ScanPredicateDomain::DiscreteSet { .. } | ScanPredicateDomain::Membership(_) => None,
        }
    }

    pub(crate) fn fallback_predicates(&self) -> Vec<Self> {
        self.domain
            .fallback_domains()
            .into_iter()
            .map(|domain| Self::new(self.column.clone(), domain, self.source))
            .collect()
    }

    pub(crate) fn to_min_max_predicates(&self) -> Vec<MinMaxPredicate> {
        match &self.domain {
            ScanPredicateDomain::Range { op, value } => {
                vec![min_max_predicate_from_parts(
                    self.column.clone(),
                    *op,
                    value.clone(),
                )]
            }
            ScanPredicateDomain::DiscreteSet { .. } | ScanPredicateDomain::Membership(_) => self
                .fallback_predicates()
                .into_iter()
                .flat_map(|predicate| predicate.to_min_max_predicates())
                .collect(),
        }
    }
}

pub(crate) fn prune_units(
    pruner: &dyn ScanPruner,
    predicates: &[ScanPredicate],
) -> Result<PruneResult, String> {
    let mut result = PruneResult::default();

    'unit: for &unit in pruner.units() {
        for predicate in predicates {
            let domains = accepted_domains(pruner, predicate.domain());
            if domains.is_empty() {
                continue;
            }

            let Some(stats) = pruner.column_stats(predicate.column(), unit) else {
                continue;
            };

            for domain in domains {
                if domain.evaluate(stats.as_ref())? == PruneVerdict::Skip {
                    result.skipped_units.push(unit);
                    continue 'unit;
                }
            }
        }
        result.kept_units.push(unit);
    }

    Ok(result)
}

fn accepted_domains(
    pruner: &dyn ScanPruner,
    domain: &ScanPredicateDomain,
) -> Vec<ScanPredicateDomain> {
    if pruner.accepts_domain(domain.kind()) {
        return vec![domain.clone()];
    }

    domain
        .fallback_domains()
        .into_iter()
        .flat_map(|fallback| accepted_domains(pruner, &fallback))
        .collect()
}

fn range_stats_may_satisfy(
    op: MinMaxPredicateOp,
    value: &MinMaxPredicateValue,
    min: &MinMaxPredicateValue,
    max: &MinMaxPredicateValue,
) -> bool {
    match op {
        MinMaxPredicateOp::Le => !compare_min_max_values(min, value).is_some_and(Ordering::is_gt),
        MinMaxPredicateOp::Ge => !compare_min_max_values(max, value).is_some_and(Ordering::is_lt),
        MinMaxPredicateOp::Lt => compare_min_max_values(min, value).is_none_or(Ordering::is_lt),
        MinMaxPredicateOp::Gt => compare_min_max_values(max, value).is_none_or(Ordering::is_gt),
        MinMaxPredicateOp::Eq => ranges_may_overlap(min, max, value, value),
    }
}

fn ranges_may_overlap(
    left_min: &MinMaxPredicateValue,
    left_max: &MinMaxPredicateValue,
    right_min: &MinMaxPredicateValue,
    right_max: &MinMaxPredicateValue,
) -> bool {
    if compare_min_max_values(left_max, right_min).is_some_and(Ordering::is_lt) {
        return false;
    }
    if compare_min_max_values(left_min, right_max).is_some_and(Ordering::is_gt) {
        return false;
    }
    true
}

fn values_all_absent(stats: &dyn ColumnStats, values: &[MinMaxPredicateValue]) -> bool {
    !values.is_empty()
        && values
            .iter()
            .all(|value| stats.contains(value) == Some(false))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanPredicateValueFamily {
    Boolean,
    Int32,
    Int64,
    ByteArray,
    FixedLenByteArray,
    Date32,
    DateTimeMicros,
    DateTimeNanos,
    LargeInt,
    Decimal128 { precision: u8, scale: i8 },
}

impl ScanPredicateValueFamily {
    fn from_value(value: &MinMaxPredicateValue) -> Option<Self> {
        match value {
            MinMaxPredicateValue::Boolean(_) => Some(Self::Boolean),
            MinMaxPredicateValue::Int32(_) => Some(Self::Int32),
            MinMaxPredicateValue::Int64(_) => Some(Self::Int64),
            MinMaxPredicateValue::Float(_) | MinMaxPredicateValue::Double(_) => None,
            MinMaxPredicateValue::ByteArray(_) => Some(Self::ByteArray),
            MinMaxPredicateValue::FixedLenByteArray(_) => Some(Self::FixedLenByteArray),
            MinMaxPredicateValue::Date32(_) => Some(Self::Date32),
            MinMaxPredicateValue::DateTimeMicros(_) => Some(Self::DateTimeMicros),
            MinMaxPredicateValue::DateTimeNanos(_) => Some(Self::DateTimeNanos),
            MinMaxPredicateValue::LargeInt(_) => Some(Self::LargeInt),
            MinMaxPredicateValue::Decimal128 {
                precision, scale, ..
            } => Some(Self::Decimal128 {
                precision: *precision,
                scale: *scale,
            }),
        }
    }
}

fn compare_scan_predicate_values(
    left: &MinMaxPredicateValue,
    right: &MinMaxPredicateValue,
) -> Ordering {
    debug_assert_eq!(
        ScanPredicateValueFamily::from_value(left),
        ScanPredicateValueFamily::from_value(right)
    );

    match (left, right) {
        (MinMaxPredicateValue::Boolean(left), MinMaxPredicateValue::Boolean(right)) => {
            left.cmp(right)
        }
        (MinMaxPredicateValue::Int32(left), MinMaxPredicateValue::Int32(right)) => left.cmp(right),
        (MinMaxPredicateValue::Int64(left), MinMaxPredicateValue::Int64(right)) => left.cmp(right),
        (MinMaxPredicateValue::ByteArray(left), MinMaxPredicateValue::ByteArray(right)) => {
            left.cmp(right)
        }
        (
            MinMaxPredicateValue::FixedLenByteArray(left),
            MinMaxPredicateValue::FixedLenByteArray(right),
        ) => left.cmp(right),
        (MinMaxPredicateValue::Date32(left), MinMaxPredicateValue::Date32(right)) => {
            left.cmp(right)
        }
        (
            MinMaxPredicateValue::DateTimeMicros(left),
            MinMaxPredicateValue::DateTimeMicros(right),
        ) => left.cmp(right),
        (MinMaxPredicateValue::DateTimeNanos(left), MinMaxPredicateValue::DateTimeNanos(right)) => {
            left.cmp(right)
        }
        (MinMaxPredicateValue::LargeInt(left), MinMaxPredicateValue::LargeInt(right)) => {
            left.cmp(right)
        }
        (
            MinMaxPredicateValue::Decimal128 {
                value: left,
                precision: left_precision,
                scale: left_scale,
            },
            MinMaxPredicateValue::Decimal128 {
                value: right,
                precision: right_precision,
                scale: right_scale,
            },
        ) if left_precision == right_precision && left_scale == right_scale => left.cmp(right),
        _ => Ordering::Equal,
    }
}

fn compare_min_max_values(
    left: &MinMaxPredicateValue,
    right: &MinMaxPredicateValue,
) -> Option<Ordering> {
    match (left, right) {
        (MinMaxPredicateValue::Boolean(left), MinMaxPredicateValue::Boolean(right)) => {
            Some(left.cmp(right))
        }
        (MinMaxPredicateValue::Int32(left), MinMaxPredicateValue::Int32(right)) => {
            Some(left.cmp(right))
        }
        (MinMaxPredicateValue::Int64(left), MinMaxPredicateValue::Int64(right)) => {
            Some(left.cmp(right))
        }
        (MinMaxPredicateValue::Float(left), MinMaxPredicateValue::Float(right)) => {
            left.partial_cmp(right)
        }
        (MinMaxPredicateValue::Double(left), MinMaxPredicateValue::Double(right)) => {
            left.partial_cmp(right)
        }
        (MinMaxPredicateValue::ByteArray(left), MinMaxPredicateValue::ByteArray(right)) => {
            Some(left.cmp(right))
        }
        (
            MinMaxPredicateValue::FixedLenByteArray(left),
            MinMaxPredicateValue::FixedLenByteArray(right),
        ) => Some(left.cmp(right)),
        (MinMaxPredicateValue::Date32(left), MinMaxPredicateValue::Date32(right)) => {
            Some(left.cmp(right))
        }
        (
            MinMaxPredicateValue::DateTimeMicros(left),
            MinMaxPredicateValue::DateTimeMicros(right),
        ) => Some(left.cmp(right)),
        (MinMaxPredicateValue::DateTimeNanos(left), MinMaxPredicateValue::DateTimeNanos(right)) => {
            Some(left.cmp(right))
        }
        (MinMaxPredicateValue::LargeInt(left), MinMaxPredicateValue::LargeInt(right)) => {
            Some(left.cmp(right))
        }
        (
            MinMaxPredicateValue::Decimal128 {
                value: left,
                precision: left_precision,
                scale: left_scale,
            },
            MinMaxPredicateValue::Decimal128 {
                value: right,
                precision: right_precision,
                scale: right_scale,
            },
        ) if left_precision == right_precision && left_scale == right_scale => {
            Some(left.cmp(right))
        }
        _ => None,
    }
}

fn min_max_predicate_from_parts(
    column: String,
    op: MinMaxPredicateOp,
    value: MinMaxPredicateValue,
) -> MinMaxPredicate {
    match op {
        MinMaxPredicateOp::Le => MinMaxPredicate::Le { column, value },
        MinMaxPredicateOp::Ge => MinMaxPredicate::Ge { column, value },
        MinMaxPredicateOp::Lt => MinMaxPredicate::Lt { column, value },
        MinMaxPredicateOp::Gt => MinMaxPredicate::Gt { column, value },
        MinMaxPredicateOp::Eq => MinMaxPredicate::Eq { column, value },
    }
}

#[cfg(test)]
mod tests {
    use crate::common::scan_predicate::{
        ColumnStats, MembershipPredicate, PruneVerdict, ScanLayer, ScanPredicate,
        ScanPredicateDomain, ScanPredicateDomainKind, ScanPredicateSource, ScanPruner, UnitId,
        prune_units,
    };
    use novarocks_execution::exec::min_max_predicate::{
        MinMaxPredicate, MinMaxPredicateOp, MinMaxPredicateValue,
    };

    #[test]
    fn range_predicate_round_trips_to_min_max_predicate() {
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Ge {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(10),
            },
            ScanPredicateSource::Static,
        );

        assert_eq!(predicate.column(), "0");
        assert_eq!(predicate.source(), ScanPredicateSource::Static);
        assert_eq!(
            predicate.to_min_max_predicates(),
            vec![MinMaxPredicate::Ge {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(10),
            }]
        );
    }

    #[test]
    fn discrete_set_builds_stable_envelope() {
        let predicate = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int32(100),
                MinMaxPredicateValue::Int32(1),
                MinMaxPredicateValue::Int32(50),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete predicate");

        assert_eq!(
            predicate.domain(),
            &ScanPredicateDomain::DiscreteSet {
                values: vec![
                    MinMaxPredicateValue::Int32(1),
                    MinMaxPredicateValue::Int32(50),
                    MinMaxPredicateValue::Int32(100),
                ],
                min: MinMaxPredicateValue::Int32(1),
                max: MinMaxPredicateValue::Int32(100),
            }
        );
        assert_eq!(
            predicate.to_min_max_predicates(),
            vec![
                MinMaxPredicate::Ge {
                    column: "0".to_string(),
                    value: MinMaxPredicateValue::Int32(1),
                },
                MinMaxPredicate::Le {
                    column: "0".to_string(),
                    value: MinMaxPredicateValue::Int32(100),
                },
            ]
        );
    }

    #[test]
    fn discrete_set_rejects_empty_values() {
        let err = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![],
            ScanPredicateSource::RuntimeMembership,
        )
        .expect_err("empty discrete sets are unsupported");

        assert!(err.contains("discrete set cannot be empty"));
    }

    #[test]
    fn discrete_set_sorts_and_deduplicates_values() {
        let predicate = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int64(9),
                MinMaxPredicateValue::Int64(3),
                MinMaxPredicateValue::Int64(9),
                MinMaxPredicateValue::Int64(1),
                MinMaxPredicateValue::Int64(3),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete predicate");

        assert_eq!(
            predicate.domain(),
            &ScanPredicateDomain::DiscreteSet {
                values: vec![
                    MinMaxPredicateValue::Int64(1),
                    MinMaxPredicateValue::Int64(3),
                    MinMaxPredicateValue::Int64(9),
                ],
                min: MinMaxPredicateValue::Int64(1),
                max: MinMaxPredicateValue::Int64(9),
            }
        );
    }

    #[test]
    fn discrete_set_rejects_float_and_double_values() {
        for value in [
            MinMaxPredicateValue::Float(1.0),
            MinMaxPredicateValue::Double(1.0),
        ] {
            let err = ScanPredicate::discrete_set(
                "0".to_string(),
                vec![value],
                ScanPredicateSource::RuntimeIn,
            )
            .expect_err("floating point values are unsupported");

            assert!(err.contains("mixed scan predicate value families"));
        }
    }

    #[test]
    fn discrete_set_sorts_decimal128_with_same_precision_and_scale() {
        let predicate = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Decimal128 {
                    value: 300,
                    precision: 12,
                    scale: 2,
                },
                MinMaxPredicateValue::Decimal128 {
                    value: 100,
                    precision: 12,
                    scale: 2,
                },
                MinMaxPredicateValue::Decimal128 {
                    value: 200,
                    precision: 12,
                    scale: 2,
                },
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("decimal discrete predicate");

        assert_eq!(
            predicate.domain(),
            &ScanPredicateDomain::DiscreteSet {
                values: vec![
                    MinMaxPredicateValue::Decimal128 {
                        value: 100,
                        precision: 12,
                        scale: 2,
                    },
                    MinMaxPredicateValue::Decimal128 {
                        value: 200,
                        precision: 12,
                        scale: 2,
                    },
                    MinMaxPredicateValue::Decimal128 {
                        value: 300,
                        precision: 12,
                        scale: 2,
                    },
                ],
                min: MinMaxPredicateValue::Decimal128 {
                    value: 100,
                    precision: 12,
                    scale: 2,
                },
                max: MinMaxPredicateValue::Decimal128 {
                    value: 300,
                    precision: 12,
                    scale: 2,
                },
            }
        );
    }

    #[test]
    fn discrete_set_rejects_decimal128_precision_or_scale_mismatch() {
        let err = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Decimal128 {
                    value: 100,
                    precision: 12,
                    scale: 2,
                },
                MinMaxPredicateValue::Decimal128 {
                    value: 100,
                    precision: 12,
                    scale: 3,
                },
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect_err("decimal scale mismatch is unsupported");

        assert!(err.contains("mixed scan predicate value families"));
    }

    #[test]
    fn discrete_set_rejects_mixed_value_families() {
        let err = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int32(1),
                MinMaxPredicateValue::ByteArray(b"a".to_vec()),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect_err("mixed families are unsupported");

        assert!(err.contains("mixed scan predicate value families"));
    }

    #[test]
    fn range_domain_exposes_operator() {
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Lt {
                column: "2".to_string(),
                value: MinMaxPredicateValue::Int64(9),
            },
            ScanPredicateSource::RuntimeMinMax,
        );

        assert_eq!(predicate.range_op(), Some(MinMaxPredicateOp::Lt));
    }

    #[derive(Clone, Debug, Default)]
    struct TestColumnStats {
        min_max: Option<(MinMaxPredicateValue, MinMaxPredicateValue)>,
        contains: Vec<(MinMaxPredicateValue, bool)>,
    }

    impl ColumnStats for TestColumnStats {
        fn min_max(&self) -> Option<(MinMaxPredicateValue, MinMaxPredicateValue)> {
            self.min_max.clone()
        }

        fn contains(&self, value: &MinMaxPredicateValue) -> Option<bool> {
            self.contains
                .iter()
                .find_map(|(candidate, present)| (candidate == value).then_some(*present))
        }
    }

    #[derive(Clone, Debug, Default)]
    struct OverrideColumnStats {
        range_result: Option<bool>,
        discrete_result: Option<bool>,
    }

    impl ColumnStats for OverrideColumnStats {
        fn may_satisfy_range(
            &self,
            _op: MinMaxPredicateOp,
            _value: &MinMaxPredicateValue,
        ) -> Result<Option<bool>, String> {
            Ok(self.range_result)
        }

        fn may_satisfy_discrete_set(
            &self,
            _values: &[MinMaxPredicateValue],
            _min: &MinMaxPredicateValue,
            _max: &MinMaxPredicateValue,
        ) -> Result<Option<bool>, String> {
            Ok(self.discrete_result)
        }
    }

    struct TestPruner {
        units: Vec<UnitId>,
        accepted: Vec<ScanPredicateDomainKind>,
        stats: Option<TestColumnStats>,
    }

    impl ScanPruner for TestPruner {
        fn layer(&self) -> ScanLayer {
            ScanLayer::RowGroup
        }

        fn accepts_domain(&self, kind: ScanPredicateDomainKind) -> bool {
            self.accepted.contains(&kind)
        }

        fn units(&self) -> &[UnitId] {
            &self.units
        }

        fn column_stats<'a>(
            &'a self,
            _column: &str,
            _unit: UnitId,
        ) -> Option<Box<dyn ColumnStats + 'a>> {
            self.stats
                .clone()
                .map(|stats| Box::new(stats) as Box<dyn ColumnStats>)
        }
    }

    #[test]
    fn domain_kind_and_fallback_are_explicit() {
        let range = ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Ge,
            value: MinMaxPredicateValue::Int32(10),
        };
        assert_eq!(range.kind(), ScanPredicateDomainKind::Range);
        assert!(range.fallback_domains().is_empty());

        let discrete = ScanPredicateDomain::DiscreteSet {
            values: vec![
                MinMaxPredicateValue::Int32(3),
                MinMaxPredicateValue::Int32(8),
            ],
            min: MinMaxPredicateValue::Int32(3),
            max: MinMaxPredicateValue::Int32(8),
        };
        assert_eq!(discrete.kind(), ScanPredicateDomainKind::DiscreteSet);
        assert_eq!(
            discrete.fallback_domains(),
            vec![
                ScanPredicateDomain::Range {
                    op: MinMaxPredicateOp::Ge,
                    value: MinMaxPredicateValue::Int32(3),
                },
                ScanPredicateDomain::Range {
                    op: MinMaxPredicateOp::Le,
                    value: MinMaxPredicateValue::Int32(8),
                },
            ]
        );

        let membership = ScanPredicateDomain::Membership(MembershipPredicate::BloomProbe {
            values: vec![MinMaxPredicateValue::Int32(3)],
        });
        assert_eq!(membership.kind(), ScanPredicateDomainKind::Membership);
        assert!(membership.fallback_domains().is_empty());
    }

    #[test]
    fn range_domain_evaluate_skips_disjoint_stats() {
        let stats = TestColumnStats {
            min_max: Some((
                MinMaxPredicateValue::Int32(10),
                MinMaxPredicateValue::Int32(20),
            )),
            contains: Vec::new(),
        };
        let domain = ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Lt,
            value: MinMaxPredicateValue::Int32(5),
        };

        assert_eq!(
            domain.evaluate(&stats).expect("range evaluate"),
            PruneVerdict::Skip
        );
    }

    #[test]
    fn range_domain_evaluate_uses_column_stats_override() {
        let stats = OverrideColumnStats {
            range_result: Some(false),
            discrete_result: None,
        };
        let domain = ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Ge,
            value: MinMaxPredicateValue::Int32(10),
        };

        assert_eq!(
            domain.evaluate(&stats).expect("range override"),
            PruneVerdict::Skip
        );
    }

    #[test]
    fn discrete_set_evaluate_uses_contains_before_min_max() {
        let stats = TestColumnStats {
            min_max: Some((
                MinMaxPredicateValue::Int32(0),
                MinMaxPredicateValue::Int32(100),
            )),
            contains: vec![
                (MinMaxPredicateValue::Int32(3), false),
                (MinMaxPredicateValue::Int32(8), false),
            ],
        };
        let domain = ScanPredicateDomain::DiscreteSet {
            values: vec![
                MinMaxPredicateValue::Int32(3),
                MinMaxPredicateValue::Int32(8),
            ],
            min: MinMaxPredicateValue::Int32(3),
            max: MinMaxPredicateValue::Int32(8),
        };

        assert_eq!(
            domain.evaluate(&stats).expect("discrete evaluate"),
            PruneVerdict::Skip
        );
    }

    #[test]
    fn discrete_set_evaluate_uses_column_stats_override() {
        let stats = OverrideColumnStats {
            range_result: None,
            discrete_result: Some(false),
        };
        let domain = ScanPredicateDomain::DiscreteSet {
            values: vec![
                MinMaxPredicateValue::Int32(3),
                MinMaxPredicateValue::Int32(8),
            ],
            min: MinMaxPredicateValue::Int32(3),
            max: MinMaxPredicateValue::Int32(8),
        };

        assert_eq!(
            domain.evaluate(&stats).expect("discrete override"),
            PruneVerdict::Skip
        );
    }

    #[test]
    fn discrete_set_partial_contains_answers_keep_without_min_max() {
        let stats = TestColumnStats {
            min_max: None,
            contains: vec![(MinMaxPredicateValue::Int32(3), false)],
        };
        let domain = ScanPredicateDomain::DiscreteSet {
            values: vec![
                MinMaxPredicateValue::Int32(3),
                MinMaxPredicateValue::Int32(8),
            ],
            min: MinMaxPredicateValue::Int32(3),
            max: MinMaxPredicateValue::Int32(8),
        };

        assert_eq!(
            domain.evaluate(&stats).expect("partial contains"),
            PruneVerdict::Keep
        );
    }

    #[test]
    fn discrete_set_present_contains_answer_keeps_despite_missing_answer_and_disjoint_min_max() {
        let stats = TestColumnStats {
            min_max: Some((
                MinMaxPredicateValue::Int32(10),
                MinMaxPredicateValue::Int32(20),
            )),
            contains: vec![(MinMaxPredicateValue::Int32(3), true)],
        };
        let domain = ScanPredicateDomain::DiscreteSet {
            values: vec![
                MinMaxPredicateValue::Int32(3),
                MinMaxPredicateValue::Int32(8),
            ],
            min: MinMaxPredicateValue::Int32(3),
            max: MinMaxPredicateValue::Int32(8),
        };

        assert_eq!(
            domain.evaluate(&stats).expect("present contains answer"),
            PruneVerdict::Keep
        );
    }

    #[test]
    fn membership_domain_keeps_without_contains_and_skips_when_all_values_absent() {
        let domain = ScanPredicateDomain::Membership(MembershipPredicate::BloomProbe {
            values: vec![MinMaxPredicateValue::Int32(7)],
        });
        let no_membership_stats = TestColumnStats {
            min_max: Some((
                MinMaxPredicateValue::Int32(0),
                MinMaxPredicateValue::Int32(10),
            )),
            contains: Vec::new(),
        };
        assert_eq!(
            domain
                .evaluate(&no_membership_stats)
                .expect("membership without contains"),
            PruneVerdict::Keep
        );

        let absent_stats = TestColumnStats {
            min_max: None,
            contains: vec![(MinMaxPredicateValue::Int32(7), false)],
        };
        assert_eq!(
            domain.evaluate(&absent_stats).expect("membership absent"),
            PruneVerdict::Skip
        );
    }

    #[test]
    fn prune_units_uses_fallback_when_pruner_rejects_discrete_set() {
        let predicate = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int32(1),
                MinMaxPredicateValue::Int32(2),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete predicate");
        let pruner = TestPruner {
            units: vec![0],
            accepted: vec![ScanPredicateDomainKind::Range],
            stats: Some(TestColumnStats {
                min_max: Some((
                    MinMaxPredicateValue::Int32(10),
                    MinMaxPredicateValue::Int32(20),
                )),
                contains: Vec::new(),
            }),
        };

        let result = prune_units(&pruner, &[predicate]).expect("prune units");
        assert!(result.kept_units.is_empty());
        assert_eq!(result.skipped_units, vec![0]);
    }

    #[test]
    fn prune_units_keeps_rejected_membership_domain() {
        let predicate = ScanPredicate::new(
            "0".to_string(),
            ScanPredicateDomain::Membership(MembershipPredicate::BloomProbe {
                values: vec![MinMaxPredicateValue::Int32(7)],
            }),
            ScanPredicateSource::RuntimeMembership,
        );
        let pruner = TestPruner {
            units: vec![0],
            accepted: vec![ScanPredicateDomainKind::Range],
            stats: Some(TestColumnStats {
                min_max: None,
                contains: vec![(MinMaxPredicateValue::Int32(7), false)],
            }),
        };

        let result = prune_units(&pruner, &[predicate]).expect("prune units");
        assert_eq!(result.kept_units, vec![0]);
        assert!(result.skipped_units.is_empty());
    }

    #[test]
    fn prune_units_keeps_when_no_fallback_domain_is_accepted() {
        let predicate = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int32(1),
                MinMaxPredicateValue::Int32(2),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete predicate");
        let pruner = TestPruner {
            units: vec![0],
            accepted: Vec::new(),
            stats: Some(TestColumnStats {
                min_max: Some((
                    MinMaxPredicateValue::Int32(10),
                    MinMaxPredicateValue::Int32(20),
                )),
                contains: Vec::new(),
            }),
        };

        let result = prune_units(&pruner, &[predicate]).expect("prune units");
        assert_eq!(result.kept_units, vec![0]);
        assert!(result.skipped_units.is_empty());
    }

    #[test]
    fn prune_units_treats_multiple_predicates_as_and() {
        let keep_predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Ge {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(5),
            },
            ScanPredicateSource::Static,
        );
        let skip_predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Lt {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(5),
            },
            ScanPredicateSource::Static,
        );
        let pruner = TestPruner {
            units: vec![0],
            accepted: vec![ScanPredicateDomainKind::Range],
            stats: Some(TestColumnStats {
                min_max: Some((
                    MinMaxPredicateValue::Int32(10),
                    MinMaxPredicateValue::Int32(20),
                )),
                contains: Vec::new(),
            }),
        };

        let result = prune_units(&pruner, &[keep_predicate, skip_predicate]).expect("prune units");
        assert!(result.kept_units.is_empty());
        assert_eq!(result.skipped_units, vec![0]);
    }

    #[test]
    fn to_min_max_predicates_uses_domain_fallback_and_drops_membership() {
        let discrete = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int32(8),
                MinMaxPredicateValue::Int32(3),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete predicate");
        assert_eq!(
            discrete.to_min_max_predicates(),
            vec![
                MinMaxPredicate::Ge {
                    column: "0".to_string(),
                    value: MinMaxPredicateValue::Int32(3),
                },
                MinMaxPredicate::Le {
                    column: "0".to_string(),
                    value: MinMaxPredicateValue::Int32(8),
                },
            ]
        );

        let membership = ScanPredicate::new(
            "0".to_string(),
            ScanPredicateDomain::Membership(MembershipPredicate::BloomProbe {
                values: vec![MinMaxPredicateValue::Int32(3)],
            }),
            ScanPredicateSource::RuntimeMembership,
        );
        assert!(membership.to_min_max_predicates().is_empty());
    }
}
