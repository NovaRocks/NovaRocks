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

use std::error::Error;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecPlanInvariant {
    Node,
    Expression,
    Layout,
    Schema,
    Sink,
    Internal,
}

impl fmt::Display for ExecPlanInvariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node => f.write_str("node invariant"),
            Self::Expression => f.write_str("expression invariant"),
            Self::Layout => f.write_str("layout invariant"),
            Self::Schema => f.write_str("schema invariant"),
            Self::Sink => f.write_str("sink invariant"),
            Self::Internal => f.write_str("internal invariant"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecPlanBuildError {
    invariant: ExecPlanInvariant,
    detail: String,
    source: Option<crate::runtime::task::SharedDomainFailure>,
}

impl ExecPlanBuildError {
    pub fn new(invariant: ExecPlanInvariant, detail: impl Into<String>) -> Self {
        Self {
            invariant,
            detail: detail.into(),
            source: None,
        }
    }
    /// Build location is independent from the domain that rejected the source.
    pub fn from_domain<E>(invariant: ExecPlanInvariant, source: E) -> Self
    where
        E: Error + novarocks_failure_contract::FailureProjection + Send + Sync + 'static,
    {
        let source = crate::runtime::task::SharedDomainFailure::new(source);
        Self {
            invariant,
            detail: source.detail().to_owned(),
            source: Some(source),
        }
    }

    pub fn invariant(&self) -> ExecPlanInvariant {
        self.invariant
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for ExecPlanBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "fragment plan build error ({}): {}",
            self.invariant, self.detail
        )
    }
}

impl Error for ExecPlanBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|source| source as _)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentBindingTarget {
    Program,
    Instance,
    ScanNode(i32),
    ExchangeNode(i32),
    Sink,
    RuntimeFilter(i32),
}

impl fmt::Display for FragmentBindingTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Program => f.write_str("program"),
            Self::Instance => f.write_str("instance"),
            Self::ScanNode(node_id) => write!(f, "scan node {node_id}"),
            Self::ExchangeNode(node_id) => write!(f, "exchange node {node_id}"),
            Self::Sink => f.write_str("sink"),
            Self::RuntimeFilter(filter_id) => write!(f, "runtime filter {filter_id}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentBindingErrorKind {
    MissingAssignment,
    ExtraAssignment,
    WrongAssignmentKind,
    InvalidAssignment,
    SchemaMismatch,
    LayoutMismatch,
    ExpressionMismatch,
    RuntimeFilterMismatch,
    ContractVersionMismatch,
}

impl fmt::Display for FragmentBindingErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingAssignment => f.write_str("missing assignment"),
            Self::ExtraAssignment => f.write_str("extra assignment"),
            Self::WrongAssignmentKind => f.write_str("wrong assignment kind"),
            Self::InvalidAssignment => f.write_str("invalid assignment"),
            Self::SchemaMismatch => f.write_str("schema mismatch"),
            Self::LayoutMismatch => f.write_str("layout mismatch"),
            Self::ExpressionMismatch => f.write_str("expression mismatch"),
            Self::RuntimeFilterMismatch => f.write_str("runtime filter mismatch"),
            Self::ContractVersionMismatch => f.write_str("contract version mismatch"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentBindingError {
    target: FragmentBindingTarget,
    kind: FragmentBindingErrorKind,
    detail: String,
}

impl FragmentBindingError {
    pub fn new(
        target: FragmentBindingTarget,
        kind: FragmentBindingErrorKind,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            target,
            kind,
            detail: detail.into(),
        }
    }

    pub fn target(&self) -> FragmentBindingTarget {
        self.target
    }

    pub fn kind(&self) -> FragmentBindingErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for FragmentBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "fragment binding error for {} ({}): {}",
            self.target, self.kind, self.detail
        )
    }
}

impl Error for FragmentBindingError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_build_error_exposes_invariant_and_detail() {
        let error = ExecPlanBuildError::new(
            ExecPlanInvariant::Layout,
            "slot 7 has no materialized column",
        );
        assert_eq!(error.invariant(), ExecPlanInvariant::Layout);
        assert_eq!(error.detail(), "slot 7 has no materialized column");
        assert_eq!(
            error.to_string(),
            "fragment plan build error (layout invariant): slot 7 has no materialized column"
        );
    }

    #[test]
    fn binding_error_keeps_typed_target_and_kind() {
        let error = FragmentBindingError::new(
            FragmentBindingTarget::ScanNode(17),
            FragmentBindingErrorKind::WrongAssignmentKind,
            "expected scan morsels",
        );
        assert_eq!(error.target(), FragmentBindingTarget::ScanNode(17));
        assert_eq!(error.kind(), FragmentBindingErrorKind::WrongAssignmentKind);
        assert_eq!(error.detail(), "expected scan morsels");
        assert_eq!(
            error.to_string(),
            "fragment binding error for scan node 17 (wrong assignment kind): expected scan morsels"
        );
    }

    #[test]
    fn exec_fragment_vocabulary_labels_are_stable() {
        for (invariant, expected) in [
            (ExecPlanInvariant::Node, "node invariant"),
            (ExecPlanInvariant::Expression, "expression invariant"),
            (ExecPlanInvariant::Layout, "layout invariant"),
            (ExecPlanInvariant::Schema, "schema invariant"),
            (ExecPlanInvariant::Sink, "sink invariant"),
            (ExecPlanInvariant::Internal, "internal invariant"),
        ] {
            assert_eq!(invariant.to_string(), expected);
        }

        for (target, expected) in [
            (FragmentBindingTarget::Program, "program"),
            (FragmentBindingTarget::Instance, "instance"),
            (FragmentBindingTarget::ScanNode(17), "scan node 17"),
            (FragmentBindingTarget::ExchangeNode(23), "exchange node 23"),
            (FragmentBindingTarget::Sink, "sink"),
            (
                FragmentBindingTarget::RuntimeFilter(31),
                "runtime filter 31",
            ),
        ] {
            assert_eq!(target.to_string(), expected);
        }

        for (kind, expected) in [
            (
                FragmentBindingErrorKind::MissingAssignment,
                "missing assignment",
            ),
            (
                FragmentBindingErrorKind::ExtraAssignment,
                "extra assignment",
            ),
            (
                FragmentBindingErrorKind::WrongAssignmentKind,
                "wrong assignment kind",
            ),
            (
                FragmentBindingErrorKind::InvalidAssignment,
                "invalid assignment",
            ),
            (FragmentBindingErrorKind::SchemaMismatch, "schema mismatch"),
            (FragmentBindingErrorKind::LayoutMismatch, "layout mismatch"),
            (
                FragmentBindingErrorKind::ExpressionMismatch,
                "expression mismatch",
            ),
            (
                FragmentBindingErrorKind::RuntimeFilterMismatch,
                "runtime filter mismatch",
            ),
            (
                FragmentBindingErrorKind::ContractVersionMismatch,
                "contract version mismatch",
            ),
        ] {
            assert_eq!(kind.to_string(), expected);
        }
    }
}

impl novarocks_failure_contract::FailureProjection for FragmentBindingError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self.kind {
            FragmentBindingErrorKind::MissingAssignment => {
                ExecutionReason::BindingMissingAssignment
            }
            FragmentBindingErrorKind::ExtraAssignment => ExecutionReason::BindingExtraAssignment,
            FragmentBindingErrorKind::WrongAssignmentKind => {
                ExecutionReason::BindingWrongAssignmentKind
            }
            FragmentBindingErrorKind::InvalidAssignment => {
                ExecutionReason::BindingInvalidAssignment
            }
            FragmentBindingErrorKind::SchemaMismatch => ExecutionReason::BindingSchemaMismatch,
            FragmentBindingErrorKind::LayoutMismatch => ExecutionReason::BindingLayoutMismatch,
            FragmentBindingErrorKind::ExpressionMismatch => {
                ExecutionReason::BindingExpressionMismatch
            }
            FragmentBindingErrorKind::RuntimeFilterMismatch => {
                ExecutionReason::BindingRuntimeFilterMismatch
            }
            FragmentBindingErrorKind::ContractVersionMismatch => {
                ExecutionReason::BindingContractVersionMismatch
            }
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<FragmentBindingError> for crate::runtime::task::ExecutionFailure {
    fn from(cause: FragmentBindingError) -> Self {
        Self::from_domain(cause)
    }
}
impl novarocks_failure_contract::FailureProjection for ExecPlanBuildError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        if let Some(source) = self.source.as_ref() {
            return novarocks_failure_contract::FailureProjection::failure_fact(source);
        }
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self.invariant {
            ExecPlanInvariant::Node => ExecutionReason::PlanNodeInvariant,
            ExecPlanInvariant::Expression => ExecutionReason::PlanExpressionInvariant,
            ExecPlanInvariant::Layout => ExecutionReason::PlanLayoutInvariant,
            ExecPlanInvariant::Schema => ExecutionReason::PlanSchemaInvariant,
            ExecPlanInvariant::Sink => ExecutionReason::PlanSinkInvariant,
            ExecPlanInvariant::Internal => ExecutionReason::PlanInternalInvariant,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<ExecPlanBuildError> for crate::runtime::task::ExecutionFailure {
    fn from(cause: ExecPlanBuildError) -> Self {
        Self::from_domain(cause)
    }
}

/// An author rejects a frozen program binding with its explicit reason kind.
pub(crate) fn program_binding_failure(
    kind: FragmentBindingErrorKind,
    detail: impl std::fmt::Display,
) -> crate::runtime::task::ExecutionFailure {
    let detail = novarocks_failure_contract::BoundedDetail::truncating_display(detail);
    FragmentBindingError::new(FragmentBindingTarget::Program, kind, detail.as_str()).into()
}

/// Explicit frozen-node guard selected by its construction owner.
#[cfg(test)]
pub(crate) fn node_construction_failure(
    detail: impl std::fmt::Display,
) -> crate::runtime::task::ExecutionFailure {
    let detail = novarocks_failure_contract::BoundedDetail::truncating_display(detail);
    ExecPlanBuildError::new(ExecPlanInvariant::Node, detail.as_str()).into()
}

#[cfg(test)]
mod build_source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn build_location_preserves_actual_layout_failure_facts_and_sdk_source() {
        let source = std::alloc::Layout::array::<u8>(usize::MAX).unwrap_err();
        let source = novarocks_functions::array_projection::ArrayProjectionFailure::ArrayLayout {
            count: usize::MAX,
            width: 1,
            source,
        };
        let expected = source.failure_fact();
        let error = ExecPlanBuildError::from_domain(ExecPlanInvariant::Schema, source);
        assert_eq!(error.invariant(), ExecPlanInvariant::Schema);
        assert_eq!(error.failure_fact(), expected);
        assert_eq!(error.failure_fact().class(), FailureClass::Representation);
        let extent = error.failure_fact().representation().unwrap();
        assert_eq!(extent.required, Some(usize::MAX as u64));
        assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
        assert!(error.failure_fact().resource().is_none());
        let source = error.source().unwrap().source().unwrap().source().unwrap();
        assert!(source.downcast_ref::<std::alloc::LayoutError>().is_some());
        assert_eq!(error.clone(), error);
    }
}
