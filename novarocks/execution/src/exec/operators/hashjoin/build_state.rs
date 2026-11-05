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
//! Join-build sink state abstraction.
//!
//! Responsibilities:
//! - Defines the contract for collecting build-side rows and producing probe-ready artifacts.
//! - Allows broadcast and partitioned hash-join build paths to share sink orchestration logic.
//!
//! Key exported interfaces:
//! - Types: `JoinBuildSinkState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::hash_table::key_index::{BulkDomain, IndexPurpose};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::ops::Range;

/// A pure fact proven from BOTH signed input declarations, never sampled data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NaaValueMode {
    General,
    AllNonNull,
    SingleNullable { value: usize },
}
impl NaaValueMode {
    pub(crate) fn from_signed_values(
        probe: &[novarocks_type_contract::FunctionValueType],
        build: &[novarocks_type_contract::FunctionValueType],
        null_safe: &[bool],
        eligibility: bool,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> crate::runtime::task::ExecutionResult<Self> {
        if probe.is_empty() || probe.len() != build.len() || build.len() != null_safe.len() {
            return Err(build_contract(
                "NAA value mode has no complete original signed declaration",
            ));
        }
        control.checkpoint(0)?;
        if eligibility {
            return Ok(Self::General);
        }
        let mut nullable = None;
        for (value, ((probe, build), &safe)) in probe.iter().zip(build).zip(null_safe).enumerate() {
            control.checkpoint(1)?;
            if safe {
                return Ok(Self::General);
            }
            if (probe.nullable() || build.nullable()) && nullable.replace(value).is_some() {
                return Ok(Self::General);
            }
        }
        Ok(match nullable {
            None => Self::AllNonNull,
            Some(value) => Self::SingleNullable { value },
        })
    }
}

/// Original K instances shared by the installed input producers. Full NAA
/// keys retain SQL scope followed by value components. The grouping projection
/// is explicit: an admitted linear specialization may additionally group by
/// required value components without changing their SQL expression roles.
/// Each producer captures the same evaluated arrays in these original domains.
pub(crate) enum ProjectionDomains {
    /// Nested-loop input has physical payload pages and no K domain at all.
    PayloadOnly,
    Ordinary {
        keys: BulkDomain,
        purpose: IndexPurpose,
    },
    Naa {
        value_mode: NaaValueMode,
        group: Option<BulkDomain>,
        full: BulkDomain,
        group_full_components: Vec<usize>,
        value_components: Range<usize>,
        full_purpose: IndexPurpose,
        _storage: ValueTypeStorageLifetime,
    },
}

#[derive(Debug)]
struct JoinBuildStateContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for JoinBuildStateContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for JoinBuildStateContract {}
impl novarocks_failure_contract::FailureProjection for JoinBuildStateContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::JoinBuildStateContract.reason(),
        )
    }
}
pub(super) fn build_contract(
    detail: impl std::fmt::Display,
) -> crate::runtime::task::ExecutionFailure {
    crate::runtime::task::ExecutionFailure::from_domain(JoinBuildStateContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_type_contract::FunctionValueType;
    #[test]
    fn naa_linear_mode_requires_both_signed_sides_ordinary_equality_and_no_eligibility() {
        let nn = FunctionValueType::new(DataType::Int64, false);
        let nullable = FunctionValueType::new(DataType::Int64, true);
        let control = super::super::join_completion::tests::Control;
        let classify = |probe: &[FunctionValueType],
                        build: &[FunctionValueType],
                        modes: &[bool],
                        eligibility| {
            NaaValueMode::from_signed_values(probe, build, modes, eligibility, &control).unwrap()
        };
        assert_eq!(
            classify(
                &[nn.clone(), nn.clone()],
                &[nn.clone(), nn.clone()],
                &[false, false],
                false
            ),
            NaaValueMode::AllNonNull
        );
        assert_eq!(
            classify(
                &[nullable.clone(), nn.clone()],
                &[nn.clone(), nn.clone()],
                &[false, false],
                false
            ),
            NaaValueMode::SingleNullable { value: 0 }
        );
        assert_eq!(
            classify(
                &[nn.clone(), nn.clone()],
                &[nn.clone(), nullable.clone()],
                &[false, false],
                false
            ),
            NaaValueMode::SingleNullable { value: 1 }
        );
        assert_eq!(
            classify(
                &[nullable.clone(), nn.clone()],
                &[nn.clone(), nullable.clone()],
                &[false, false],
                false
            ),
            NaaValueMode::General
        );
        assert_eq!(
            classify(
                &[nullable.clone(), nn.clone()],
                &[nn.clone(), nn.clone()],
                &[false, true],
                false
            ),
            NaaValueMode::General
        );
        assert_eq!(
            classify(
                std::slice::from_ref(&nn),
                std::slice::from_ref(&nn),
                &[false],
                true
            ),
            NaaValueMode::General
        );
        assert!(NaaValueMode::from_signed_values(&[], &[], &[], false, &control).is_err());
    }
}
