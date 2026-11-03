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
//! Sort-key preparation owns its structural failures and retains type/Arrow causes.
use crate::runtime::task::{ExecutionFailure, ExecutionReason, SharedDomainFailure};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
use novarocks_type_contract::ExecutionValueTypeError;
use std::{error::Error, fmt};
#[derive(Debug)]
pub(crate) enum SortKeyFailure {
    MissingDeclaration,
    MissingChild { index: usize },
    MapChild { index: usize },
    MapDeclaration,
    MapWrapper,
    PhysicalWrapper(&'static str),
    LargeIntWidth { actual: i32 },
    Type(ExecutionValueTypeError),
    Arrow(arrow::error::ArrowError),
}
impl fmt::Display for SortKeyFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingDeclaration => {
                f.write_str("typed sort key is missing its complete value declaration")
            }
            Self::MissingChild { index } => write!(
                f,
                "typed sort key is missing declared semantic child {index}"
            ),
            Self::MapChild { index } => {
                write!(f, "typed Map sort key has undeclared entries child {index}")
            }
            Self::MapDeclaration => {
                f.write_str("typed Map sort key has no complete Map declaration")
            }
            Self::MapWrapper => {
                f.write_str("typed Map sort key has a physical wrapper as its declaration")
            }
            Self::PhysicalWrapper(expected) => write!(
                f,
                "sort key has a different physical wrapper, expected {expected}"
            ),
            Self::LargeIntWidth { actual } => write!(
                f,
                "LARGEINT sort key width mismatch: expected 16, got {actual}"
            ),
            Self::Type(source) => source.fmt(f),
            Self::Arrow(source) => write!(f, "normalize LARGEINT sort key failed: {source}"),
        }
    }
}
impl Error for SortKeyFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Type(source) => Some(source),
            Self::Arrow(source) => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for SortKeyFailure {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self {
            Self::MissingDeclaration => ExecutionReason::SortKeyMissingDeclaration,
            Self::MissingChild { .. } => ExecutionReason::SortKeyMissingChild,
            Self::MapChild { .. } => ExecutionReason::SortKeyMapChild,
            Self::MapDeclaration => ExecutionReason::SortKeyMapDeclaration,
            Self::MapWrapper => ExecutionReason::SortKeyMapWrapper,
            Self::PhysicalWrapper(_) => ExecutionReason::SortKeyPhysicalWrapper,
            Self::LargeIntWidth { .. } => ExecutionReason::SortKeyLargeIntWidth,
            Self::Type(source) => return source.failure_fact(),
            Self::Arrow(arrow::error::ArrowError::MemoryError(_)) => {
                return FailureFact::new(
                    FailureClass::Resource,
                    ExecutionReason::SortKeyArrowMemory.reason(),
                );
            }
            Self::Arrow(_) => ExecutionReason::SortKeyArrow,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<SortKeyFailure> for crate::exec::pipeline::operator::ConstructionFailure {
    fn from(source: SortKeyFailure) -> Self {
        Self::Source(SharedDomainFailure::new(source))
    }
}
impl From<SortKeyFailure> for ExecutionFailure {
    fn from(source: SortKeyFailure) -> Self {
        Self::from_domain(source)
    }
}
