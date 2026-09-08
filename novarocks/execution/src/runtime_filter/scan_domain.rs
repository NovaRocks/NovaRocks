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

//! Execution-owned evaluation of one sealed connector scan unit against an
//! immutable runtime-filter snapshot.
//!
//! Design: ADR-0043 (docs/adr/ADR-0043-runtime-filter-artifact-query-and-evaluator-boundary.md)

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_spi::connector::{
    ConnectorPreparedScanUnit, ConnectorScalarType, ConnectorScanUnitColumnDomain,
    ConnectorScanUnitDomainFacts, ConnectorScanUnitFactsEvidence,
    ConnectorScanUnitFactsMissingReason,
};

use super::{
    LogicalVersion, RuntimeFilterBindingId, RuntimeFilterContractViolation,
    RuntimeFilterContractViolationKind, RuntimeFilterSnapshot,
    evaluator::RuntimeFilterArtifactQueryError,
};

macro_rules! capability_bool {
    ($result:expr, $binding:expr, $unit_id:expr, $logical_version:expr $(,)?) => {
        match capability_value($result)? {
            CapabilityValue::Value(value) => value,
            CapabilityValue::NotEvaluated(reason) => {
                return Ok(not_evaluated(
                    $binding,
                    $unit_id,
                    reason,
                    Some($logical_version),
                ));
            }
        }
    };
}

/// The physical provider field frozen by Frontend preparation for a scan-domain
/// consumer. The ordinal has provider schema meaning; it is never inferred by a
/// Backend reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterScanDomainTarget {
    field_ordinal: u32,
    data_type: DataType,
    nullable: bool,
}

impl RuntimeFilterScanDomainTarget {
    pub const fn new(field_ordinal: u32, data_type: DataType, nullable: bool) -> Self {
        Self {
            field_ordinal,
            data_type,
            nullable,
        }
    }

    pub const fn field_ordinal(&self) -> u32 {
        self.field_ordinal
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterScanUnitId {
    membership_digest: [u8; 32],
    unit_ordinal: u32,
}

impl RuntimeFilterScanUnitId {
    pub const fn new(membership_digest: [u8; 32], unit_ordinal: u32) -> Self {
        Self {
            membership_digest,
            unit_ordinal,
        }
    }

    pub const fn membership_digest(&self) -> [u8; 32] {
        self.membership_digest
    }

    pub const fn unit_ordinal(&self) -> u32 {
        self.unit_ordinal
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterScanUnitDecision {
    Pruned,
    Kept,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterScanUnitNotEvaluatedReason {
    UnitFactsMissing(ConnectorScanUnitFactsMissingReason),
    ColumnFactsMissing(ConnectorScanUnitFactsMissingReason),
    DataTypeUnsupported,
    PredicateCapabilityUnsupported,
    ResourceUnavailable,
    SnapshotUnavailable,
    SnapshotTimedOut,
    SnapshotNotPublished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterScanUnitEvaluation {
    Evaluated {
        decision: RuntimeFilterScanUnitDecision,
        logical_version: LogicalVersion,
    },
    NotEvaluated {
        reason: RuntimeFilterScanUnitNotEvaluatedReason,
        observed_version: Option<LogicalVersion>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterScanUnitOutcome {
    binding_id: RuntimeFilterBindingId,
    unit_id: RuntimeFilterScanUnitId,
    evaluation: RuntimeFilterScanUnitEvaluation,
}

impl RuntimeFilterScanUnitOutcome {
    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }

    pub const fn unit_id(&self) -> RuntimeFilterScanUnitId {
        self.unit_id
    }

    pub const fn evaluation(&self) -> RuntimeFilterScanUnitEvaluation {
        self.evaluation
    }

    pub fn effect(&self) -> Option<RuntimeFilterScanUnitEffect> {
        let RuntimeFilterScanUnitEvaluation::Evaluated {
            decision,
            logical_version,
        } = self.evaluation
        else {
            return None;
        };
        Some(RuntimeFilterScanUnitEffect {
            binding_id: self.binding_id,
            unit_id: self.unit_id,
            decision,
            logical_version,
        })
    }
}

/// An effect is unconstructable for an unevaluated unit, which prevents
/// profile and event callers from fabricating a versioned prune/keep fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterScanUnitEffect {
    binding_id: RuntimeFilterBindingId,
    unit_id: RuntimeFilterScanUnitId,
    decision: RuntimeFilterScanUnitDecision,
    logical_version: LogicalVersion,
}

impl RuntimeFilterScanUnitEffect {
    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }

    pub const fn unit_id(&self) -> RuntimeFilterScanUnitId {
        self.unit_id
    }

    pub const fn decision(&self) -> RuntimeFilterScanUnitDecision {
        self.decision
    }

    pub const fn logical_version(&self) -> LogicalVersion {
        self.logical_version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterScanDomainBinding {
    binding_id: RuntimeFilterBindingId,
    target: RuntimeFilterScanDomainTarget,
}

impl RuntimeFilterScanDomainBinding {
    pub const fn new(
        binding_id: RuntimeFilterBindingId,
        target: RuntimeFilterScanDomainTarget,
    ) -> Self {
        Self { binding_id, target }
    }

    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }

    pub const fn target(&self) -> &RuntimeFilterScanDomainTarget {
        &self.target
    }
}

/// Immutable input copied from a sealed connector unit. Execution owns only
/// the identity and facts, never the provider payload or a reader handle.
pub struct RuntimeFilterScanUnitInput<'a> {
    unit_id: RuntimeFilterScanUnitId,
    facts: &'a ConnectorScanUnitDomainFacts,
}

impl<'a> RuntimeFilterScanUnitInput<'a> {
    pub const fn new(
        unit_id: RuntimeFilterScanUnitId,
        facts: &'a ConnectorScanUnitDomainFacts,
    ) -> Self {
        Self { unit_id, facts }
    }

    pub fn from_prepared(unit: &'a ConnectorPreparedScanUnit) -> Self {
        Self::new(
            RuntimeFilterScanUnitId::new(*unit.membership_digest(), unit.ordinal()),
            unit.domain_facts(),
        )
    }

    pub const fn unit_id(&self) -> RuntimeFilterScanUnitId {
        self.unit_id
    }
}

pub fn evaluate_scan_unit(
    binding: &RuntimeFilterScanDomainBinding,
    snapshot: Option<&RuntimeFilterSnapshot>,
    input: RuntimeFilterScanUnitInput<'_>,
) -> Result<RuntimeFilterScanUnitOutcome, RuntimeFilterContractViolation> {
    let Some(snapshot) = snapshot else {
        return Ok(not_evaluated(
            binding,
            input.unit_id,
            RuntimeFilterScanUnitNotEvaluatedReason::SnapshotUnavailable,
            None,
        ));
    };
    if snapshot.binding_id() != binding.binding_id {
        return Err(violation(
            "runtime-filter scan-domain binding differs from the immutable snapshot",
        ));
    }
    let capability = snapshot.artifact_query();
    let Some(expected_scalar_type) = connector_scalar_type(binding.target.data_type()) else {
        return Ok(not_evaluated(
            binding,
            input.unit_id,
            RuntimeFilterScanUnitNotEvaluatedReason::DataTypeUnsupported,
            Some(snapshot.logical_version()),
        ));
    };
    if capability.data_type() != binding.target.data_type() {
        return Err(violation(
            "runtime-filter scan-domain capability type differs from the frozen target",
        ));
    }

    let facts = match input.facts {
        ConnectorScanUnitDomainFacts::Missing(reason) => {
            return Ok(not_evaluated(
                binding,
                input.unit_id,
                RuntimeFilterScanUnitNotEvaluatedReason::UnitFactsMissing(*reason),
                Some(snapshot.logical_version()),
            ));
        }
        ConnectorScanUnitDomainFacts::Available(facts) => facts,
    };
    let column = match facts
        .columns()
        .iter()
        .find(|column| column.column().field_ordinal() == binding.target.field_ordinal())
    {
        Some(column) => column,
        None => {
            return Ok(not_evaluated(
                binding,
                input.unit_id,
                RuntimeFilterScanUnitNotEvaluatedReason::ColumnFactsMissing(
                    ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
                ),
                Some(snapshot.logical_version()),
            ));
        }
    };
    if column.column().data_type() != expected_scalar_type
        || column.column().nullable() != binding.target.nullable()
    {
        return Err(violation(
            "connector scan-unit facts differ from the frozen target type or nullability",
        ));
    }
    let Some(domain) = column.domain() else {
        return Ok(not_evaluated(
            binding,
            input.unit_id,
            RuntimeFilterScanUnitNotEvaluatedReason::ColumnFactsMissing(
                column
                    .missing_reason()
                    .expect("missing domain has a reason"),
            ),
            Some(snapshot.logical_version()),
        ));
    };

    let decision = match domain {
        ConnectorScanUnitColumnDomain::AllNull { null_count } => {
            if *null_count != facts.physical_row_count() {
                return Err(violation(
                    "all-null scan-unit facts do not cover the physical row count",
                ));
            }
            if capability_bool!(
                capability.matches_null(),
                binding,
                input.unit_id,
                snapshot.logical_version(),
            ) {
                RuntimeFilterScanUnitDecision::Kept
            } else {
                RuntimeFilterScanUnitDecision::Pruned
            }
        }
        ConnectorScanUnitColumnDomain::Range {
            inclusive_min,
            inclusive_max,
            null_count,
        } => {
            if inclusive_min.data_type() != expected_scalar_type
                || inclusive_max.data_type() != expected_scalar_type
                || inclusive_min.compare_same_type(inclusive_max).is_none()
            {
                return Err(violation(
                    "connector scan-unit range differs from the frozen target type",
                ));
            }
            let null_matches = *null_count > 0
                && capability_bool!(
                    capability.matches_null(),
                    binding,
                    input.unit_id,
                    snapshot.logical_version(),
                );
            let non_null_matches = capability_bool!(
                capability.has_non_null_matches(),
                binding,
                input.unit_id,
                snapshot.logical_version(),
            ) && capability_bool!(
                capability.non_null_range_may_match(inclusive_min, inclusive_max),
                binding,
                input.unit_id,
                snapshot.logical_version(),
            );
            if null_matches || non_null_matches {
                RuntimeFilterScanUnitDecision::Kept
            } else {
                RuntimeFilterScanUnitDecision::Pruned
            }
        }
    };
    let _evidence: ConnectorScanUnitFactsEvidence = facts.evidence();
    Ok(RuntimeFilterScanUnitOutcome {
        binding_id: binding.binding_id,
        unit_id: input.unit_id,
        evaluation: RuntimeFilterScanUnitEvaluation::Evaluated {
            decision,
            logical_version: snapshot.logical_version(),
        },
    })
}

fn connector_scalar_type(data_type: &DataType) -> Option<ConnectorScalarType> {
    match data_type {
        DataType::Boolean => Some(ConnectorScalarType::Boolean),
        DataType::Int8 => Some(ConnectorScalarType::Int8),
        DataType::Int16 => Some(ConnectorScalarType::Int16),
        DataType::Int32 => Some(ConnectorScalarType::Int32),
        DataType::Int64 => Some(ConnectorScalarType::Int64),
        DataType::Date32 => Some(ConnectorScalarType::Date32),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            Some(ConnectorScalarType::TimestampMillis)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Some(ConnectorScalarType::TimestampMicros)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            Some(ConnectorScalarType::TimestampNanos)
        }
        DataType::Utf8 => Some(ConnectorScalarType::Utf8),
        _ => None,
    }
}

enum CapabilityValue {
    Value(bool),
    NotEvaluated(RuntimeFilterScanUnitNotEvaluatedReason),
}

fn capability_value(
    result: Result<bool, RuntimeFilterArtifactQueryError>,
) -> Result<CapabilityValue, RuntimeFilterContractViolation> {
    result
        .map(CapabilityValue::Value)
        .or_else(|error| match error {
            RuntimeFilterArtifactQueryError::ContractViolation => Err(violation(
                "runtime-filter scan-domain capability rejected its retained artifact",
            )),
            RuntimeFilterArtifactQueryError::Unsupported => Ok(CapabilityValue::NotEvaluated(
                RuntimeFilterScanUnitNotEvaluatedReason::PredicateCapabilityUnsupported,
            )),
            RuntimeFilterArtifactQueryError::ResourceUnavailable => {
                Ok(CapabilityValue::NotEvaluated(
                    RuntimeFilterScanUnitNotEvaluatedReason::ResourceUnavailable,
                ))
            }
        })
}

fn not_evaluated(
    binding: &RuntimeFilterScanDomainBinding,
    unit_id: RuntimeFilterScanUnitId,
    reason: RuntimeFilterScanUnitNotEvaluatedReason,
    observed_version: Option<LogicalVersion>,
) -> RuntimeFilterScanUnitOutcome {
    RuntimeFilterScanUnitOutcome {
        binding_id: binding.binding_id,
        unit_id,
        evaluation: RuntimeFilterScanUnitEvaluation::NotEvaluated {
            reason,
            observed_version,
        },
    }
}

fn violation(detail: &'static str) -> RuntimeFilterContractViolation {
    RuntimeFilterContractViolation::new(
        RuntimeFilterContractViolationKind::ContractMismatch,
        detail,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use novarocks_spi::connector::{
        ConnectorScalarValue, ConnectorScanUnitColumn, ConnectorScanUnitColumnFacts,
        ConnectorScanUnitFactsEvidence,
    };

    use crate::runtime_filter::{
        RuntimeFilterArtifactQuery, RuntimeFilterArtifactQueryError, RuntimeFilterScalarRef,
        RuntimeFilterSnapshot,
    };

    struct Domain {
        data_type: DataType,
        matches_null: bool,
        has_non_null_matches: bool,
        range_may_match: bool,
    }

    impl RuntimeFilterArtifactQuery for Domain {
        fn data_type(&self) -> &DataType {
            &self.data_type
        }
        fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(self.matches_null)
        }
        fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(self.has_non_null_matches)
        }
        fn non_null_value_may_match(
            &self,
            _: RuntimeFilterScalarRef<'_>,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(self.range_may_match)
        }
        fn non_null_range_may_match(
            &self,
            _: &ConnectorScalarValue,
            _: &ConnectorScalarValue,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(self.range_may_match)
        }
    }

    struct ResourceConstrainedDomain;

    impl RuntimeFilterArtifactQuery for ResourceConstrainedDomain {
        fn data_type(&self) -> &DataType {
            &DataType::Int64
        }
        fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(false)
        }
        fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Err(RuntimeFilterArtifactQueryError::ResourceUnavailable)
        }
        fn non_null_value_may_match(
            &self,
            _: RuntimeFilterScalarRef<'_>,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(false)
        }
        fn non_null_range_may_match(
            &self,
            _: &ConnectorScalarValue,
            _: &ConnectorScalarValue,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(false)
        }
    }

    fn target(nullable: bool) -> RuntimeFilterScanDomainBinding {
        RuntimeFilterScanDomainBinding::new(
            super::RuntimeFilterBindingId::new(7),
            RuntimeFilterScanDomainTarget::new(3, DataType::Int64, nullable),
        )
    }

    fn snapshot(domain: Domain) -> RuntimeFilterSnapshot {
        RuntimeFilterSnapshot::new(
            super::RuntimeFilterBindingId::new(7),
            LogicalVersion::new(2),
            [9; 32],
            Arc::new(domain),
        )
    }

    fn facts(column: ConnectorScanUnitColumnFacts) -> ConnectorScanUnitDomainFacts {
        ConnectorScanUnitDomainFacts::available(
            10,
            ConnectorScanUnitFactsEvidence::Exact,
            vec![column],
        )
        .expect("valid facts")
    }

    fn range(nullable: bool) -> ConnectorScanUnitColumnFacts {
        ConnectorScanUnitColumnDomain::try_range(
            ConnectorScanUnitColumn::new(3, ConnectorScalarType::Int64, nullable),
            ConnectorScalarValue::Int64(10),
            ConnectorScalarValue::Int64(20),
            0,
            10,
        )
        .expect("valid range")
    }

    #[test]
    fn range_without_matching_values_is_pruned_with_a_versioned_effect() {
        let facts = facts(range(false));
        let outcome = evaluate_scan_unit(
            &target(false),
            Some(&snapshot(Domain {
                data_type: DataType::Int64,
                matches_null: false,
                has_non_null_matches: true,
                range_may_match: false,
            })),
            RuntimeFilterScanUnitInput::new(RuntimeFilterScanUnitId::new([4; 32], 1), &facts),
        )
        .expect("evaluation succeeds");
        assert!(
            matches!(outcome.evaluation(), RuntimeFilterScanUnitEvaluation::Evaluated {
            decision: RuntimeFilterScanUnitDecision::Pruned,
            logical_version: version,
        } if version == LogicalVersion::new(2))
        );
        assert!(
            matches!(outcome.effect(), Some(effect) if effect.decision() == RuntimeFilterScanUnitDecision::Pruned)
        );
    }

    #[test]
    fn all_null_unit_is_kept_only_when_the_capability_matches_null() {
        let facts = facts(
            ConnectorScanUnitColumnDomain::try_all_null(
                ConnectorScanUnitColumn::new(3, ConnectorScalarType::Int64, true),
                10,
                10,
            )
            .expect("valid all-null facts"),
        );
        let outcome = evaluate_scan_unit(
            &target(true),
            Some(&snapshot(Domain {
                data_type: DataType::Int64,
                matches_null: true,
                has_non_null_matches: false,
                range_may_match: false,
            })),
            RuntimeFilterScanUnitInput::new(RuntimeFilterScanUnitId::new([4; 32], 1), &facts),
        )
        .expect("evaluation succeeds");
        assert!(matches!(
            outcome.evaluation(),
            RuntimeFilterScanUnitEvaluation::Evaluated {
                decision: RuntimeFilterScanUnitDecision::Kept,
                ..
            }
        ));
    }

    #[test]
    fn missing_facts_fail_open_without_an_effect() {
        let facts = ConnectorScanUnitDomainFacts::missing(
            ConnectorScanUnitFactsMissingReason::NoPinnedStatistics,
        );
        let outcome = evaluate_scan_unit(
            &target(false),
            Some(&snapshot(Domain {
                data_type: DataType::Int64,
                matches_null: false,
                has_non_null_matches: true,
                range_may_match: true,
            })),
            RuntimeFilterScanUnitInput::new(RuntimeFilterScanUnitId::new([4; 32], 1), &facts),
        )
        .expect("missing facts fail open");
        assert!(
            matches!(outcome.evaluation(), RuntimeFilterScanUnitEvaluation::NotEvaluated {
            reason: RuntimeFilterScanUnitNotEvaluatedReason::UnitFactsMissing(
                ConnectorScanUnitFactsMissingReason::NoPinnedStatistics),
            observed_version: Some(version),
        } if version == LogicalVersion::new(2))
        );
        assert_eq!(outcome.effect(), None);
    }

    #[test]
    fn unsupported_target_type_is_not_evaluated() {
        let binding = RuntimeFilterScanDomainBinding::new(
            super::RuntimeFilterBindingId::new(7),
            RuntimeFilterScanDomainTarget::new(3, DataType::Binary, false),
        );
        let facts = facts(range(false));
        let outcome = evaluate_scan_unit(
            &binding,
            Some(&snapshot(Domain {
                data_type: DataType::Binary,
                matches_null: false,
                has_non_null_matches: true,
                range_may_match: true,
            })),
            RuntimeFilterScanUnitInput::new(RuntimeFilterScanUnitId::new([4; 32], 1), &facts),
        )
        .expect("unsupported type fails open");
        assert!(matches!(
            outcome.evaluation(),
            RuntimeFilterScanUnitEvaluation::NotEvaluated {
                reason: RuntimeFilterScanUnitNotEvaluatedReason::DataTypeUnsupported,
                ..
            }
        ));
    }

    #[test]
    fn resource_constrained_capability_fails_open_without_an_effect() {
        let facts = facts(range(false));
        let snapshot = RuntimeFilterSnapshot::new(
            super::RuntimeFilterBindingId::new(7),
            LogicalVersion::new(2),
            [9; 32],
            Arc::new(ResourceConstrainedDomain),
        );
        let outcome = evaluate_scan_unit(
            &target(false),
            Some(&snapshot),
            RuntimeFilterScanUnitInput::new(RuntimeFilterScanUnitId::new([4; 32], 1), &facts),
        )
        .expect("resource constraint fails open");
        assert!(
            matches!(outcome.evaluation(), RuntimeFilterScanUnitEvaluation::NotEvaluated {
            reason: RuntimeFilterScanUnitNotEvaluatedReason::ResourceUnavailable,
            observed_version: Some(version),
        } if version == LogicalVersion::new(2))
        );
        assert_eq!(outcome.effect(), None);
    }

    #[test]
    fn scan_domain_types_preserve_int16_and_timestamp_milliseconds() {
        assert_eq!(
            connector_scalar_type(&DataType::Int16),
            Some(ConnectorScalarType::Int16)
        );
        assert_eq!(
            connector_scalar_type(&DataType::Timestamp(TimeUnit::Millisecond, None)),
            Some(ConnectorScalarType::TimestampMillis)
        );
        assert_eq!(
            connector_scalar_type(&DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into())
            )),
            None
        );
    }
}
