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
//! Compiled series receiver invariants; function data causes remain Functions-owned.
use crate::runtime::task::{ExecutionFailure, ExecutionReason};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
use std::{error::Error, fmt};
#[derive(Debug)]
pub(super) enum CompiledSeriesFailure {
    DuplicateBounds,
    MissingBatchRows,
    InvalidBatchRows,
    InputPort,
    Arrow(arrow::error::ArrowError),
    AlreadyFailed,
}
impl fmt::Display for CompiledSeriesFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::DuplicateBounds => "compiled series accepts its bounds port once",
            Self::MissingBatchRows => "compiled series batch size was not bound",
            Self::InvalidBatchRows => "compiled series batch size is zero",
            Self::InputPort => "compiled series input differs from its frozen bounds port",
            Self::AlreadyFailed => "compiled series instance has already failed",
            Self::Arrow(error) => return error.fmt(f),
        })
    }
}
impl Error for CompiledSeriesFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Arrow(error) => Some(error),
            _ => None,
        }
    }
}
impl FailureProjection for CompiledSeriesFailure {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self {
            Self::DuplicateBounds => ExecutionReason::SeriesDuplicateBounds,
            Self::MissingBatchRows => ExecutionReason::SeriesMissingBatchRows,
            Self::InvalidBatchRows => ExecutionReason::SeriesInvalidBatchRows,
            Self::InputPort => ExecutionReason::SeriesInputPort,
            Self::Arrow(_) => ExecutionReason::SeriesArrow,
            Self::AlreadyFailed => ExecutionReason::SeriesAlreadyFailed,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<CompiledSeriesFailure> for ExecutionFailure {
    fn from(cause: CompiledSeriesFailure) -> Self {
        Self::from_domain(cause)
    }
}
