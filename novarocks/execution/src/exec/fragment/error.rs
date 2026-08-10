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
}

impl ExecPlanBuildError {
    pub fn new(invariant: ExecPlanInvariant, detail: impl Into<String>) -> Self {
        Self {
            invariant,
            detail: detail.into(),
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

impl Error for ExecPlanBuildError {}

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
