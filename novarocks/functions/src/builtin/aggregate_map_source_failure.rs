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
//! MAP_AGG owns its frozen carriers and actual output construction failures.
use crate::KernelFailure;
use arrow_schema::ArrowError;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    OuterFailureProjection, domain,
};
use std::{fmt, num::NonZeroU16};
#[derive(Debug)]
enum MapAggregateFailure {
    Contract(BoundedDetail),
    Constructor(ArrowError),
    UnsupportedKey(novarocks_type_contract::UnsupportedKeyType),
}
impl fmt::Display for MapAggregateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) => detail.fmt(f),
            Self::Constructor(source) => write!(f, "map_agg: {source}"),
            Self::UnsupportedKey(cause) => {
                write!(f, "map_agg key relation is unavailable for {cause:?}")
            }
        }
    }
}
impl std::error::Error for MapAggregateFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(_) | Self::UnsupportedKey(_) => None,
            Self::Constructor(source) => Some(source),
        }
    }
}
impl FailureProjection for MapAggregateFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::Contract(_) => (FailureClass::Invariant, 343),
            Self::Constructor(ArrowError::MemoryError(_)) => (FailureClass::Resource, 345),
            Self::Constructor(_) => (FailureClass::Invariant, 344),
            Self::UnsupportedKey(_) => (FailureClass::Unsupported, 853),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
        )
    }
}
impl OuterFailureProjection for MapAggregateFailure {}
pub(super) fn contract(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(MapAggregateFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn constructor(source: ArrowError) -> KernelFailure {
    KernelFailure::from_domain(MapAggregateFailure::Constructor(source))
}

pub(super) fn key_type(cause: novarocks_type_contract::UnsupportedKeyType) -> KernelFailure {
    KernelFailure::from_domain(MapAggregateFailure::UnsupportedKey(cause))
}

/// Diagnostic construction is secondary to the already observed whole-value
/// failure. Neither source retains an invocation graph on this failed path.
#[derive(Debug)]
pub(super) struct MapDiagnosticFailure {
    pub(super) origin: KernelFailure,
    pub(super) _diagnostic: KernelFailure,
    pub(super) phase: crate::AggregateInvocationPhase,
}
impl fmt::Display for MapDiagnosticFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::{
            AggregateInvocationPhase as Phase, aggregate_format::AggregateFailureStage as Stage,
        };
        let stage = match self.phase {
            Phase::Update => Stage::Update,
            Phase::Merge => Stage::Merge,
            Phase::Intermediate => Stage::BuildIntermediate,
            Phase::Final => Stage::BuildFinal,
        };
        stage.message(&self.origin).fmt(f)
    }
}
impl std::error::Error for MapDiagnosticFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.origin)
    }
}
impl FailureProjection for MapDiagnosticFailure {
    fn failure_fact(&self) -> FailureFact {
        self.origin.failure_fact()
    }
}
impl OuterFailureProjection for MapDiagnosticFailure {}
