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

//! Set-operation state facts; callees keep their own construction causes.
use crate::runtime::fragment::ExecutionFailure;
use crate::runtime::fragment::ExecutionReason;
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
use std::fmt;
#[derive(Debug)]
pub(super) enum SetStateFailure {
    ZeroColumns {
        node: &'static str,
    },
    GroupSequence {
        node: &'static str,
        expected: usize,
        actual: usize,
    },
    GroupBounds {
        node: &'static str,
        group: usize,
        markers: usize,
    },
}
impl fmt::Display for SetStateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroColumns { node } => write!(f, "{node} with zero columns is unsupported"),
            Self::GroupSequence {
                node,
                expected,
                actual,
            } => write!(
                f,
                "{node} group id sequence mismatch: expected {expected}, got {actual}"
            ),
            Self::GroupBounds {
                node,
                group,
                markers,
            } => write!(f, "{node} group id {group} is outside {markers} markers"),
        }
    }
}
impl std::error::Error for SetStateFailure {}
impl FailureProjection for SetStateFailure {
    fn failure_fact(&self) -> FailureFact {
        let code = match self {
            Self::ZeroColumns { .. } => ExecutionReason::DistinctSetZeroColumns,
            Self::GroupSequence { .. } => ExecutionReason::DistinctSetGroupSequence,
            Self::GroupBounds { .. } => ExecutionReason::DistinctSetGroupBounds,
        };
        FailureFact::new(FailureClass::Invariant, code.reason())
    }
}
pub(super) fn fail(cause: SetStateFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(cause)
}
