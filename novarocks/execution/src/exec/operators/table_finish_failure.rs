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

//! TableFinish authors only its own protocol/shape refusals. Connector,
//! memory, type and Chunk causes are forwarded without diagnostic conversion.
use crate::runtime::task::{ExecutionFailure, ExecutionReason};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, ResourceDimension, ResourceFacts, ResourceScope,
    domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Clone, Copy, Debug)]
pub(super) enum FinishContractSite {
    Relation,
    Coverage,
    FinalGroups,
    RootArtifact,
    Binding,
}
#[derive(Debug)]
struct FinishContractFailure {
    site: FinishContractSite,
    detail: BoundedDetail,
}
impl fmt::Display for FinishContractFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}
impl Error for FinishContractFailure {}
impl FailureProjection for FinishContractFailure {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self.site {
            FinishContractSite::Relation => ExecutionReason::FinishRelation,
            FinishContractSite::Coverage => ExecutionReason::FinishCoverage,
            FinishContractSite::FinalGroups => ExecutionReason::FinishFinalGroups,
            FinishContractSite::RootArtifact => ExecutionReason::FinishRootArtifact,
            FinishContractSite::Binding => ExecutionReason::FinishBinding,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
pub(super) fn finish_contract(
    site: FinishContractSite,
    detail: impl fmt::Display,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(FinishContractFailure {
        site,
        detail: BoundedDetail::truncating_display(detail),
    })
}
#[derive(Clone, Copy, Debug)]
pub(super) enum FinishArrowOperation {
    Summary,
    Fragment,
    Encoding,
    Filter,
    #[cfg(test)]
    Artifact,
    List,
    Map,
}
#[derive(Debug)]
struct FinishArrowFailure {
    operation: FinishArrowOperation,
    source: arrow::error::ArrowError,
}
impl fmt::Display for FinishArrowFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "table finish {:?}: {}", self.operation, self.source)
    }
}
impl Error for FinishArrowFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for FinishArrowFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, reason) = match self.source {
            arrow::error::ArrowError::MemoryError(_) => {
                (FailureClass::Resource, ExecutionReason::FinishArrowMemory)
            }
            arrow::error::ArrowError::OffsetOverflowError(required) => {
                return FailureFact::new(
                    FailureClass::Representation,
                    ExecutionReason::FinishArrowOffset.reason(),
                )
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::EXECUTION,
                        code: NonZeroU16::new(6).unwrap(),
                    },
                    required: u64::try_from(required).ok(),
                    extent_limit: None,
                    representable_prefix: None,
                });
            }
            arrow::error::ArrowError::IoError(..) | arrow::error::ArrowError::ExternalError(_) => (
                FailureClass::Unexpected,
                ExecutionReason::FinishArrowUnexpected,
            ),
            _ => (FailureClass::Invariant, ExecutionReason::FinishArrow),
        };
        FailureFact::new(class, reason.reason())
    }
}
pub(super) fn finish_arrow(
    operation: FinishArrowOperation,
    source: arrow::error::ArrowError,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(FinishArrowFailure { operation, source })
}
#[derive(Debug)]
pub(super) struct FinishOutputBound {
    pub required: Option<usize>,
    pub limit: usize,
}
impl fmt::Display for FinishOutputBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "table finish Root output requires {:?} bytes under a {} byte construction profile",
            self.required, self.limit
        )
    }
}
impl Error for FinishOutputBound {}
impl FailureProjection for FinishOutputBound {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Resource,
            ExecutionReason::FinishOutputBound.reason(),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Request),
            constraint_scope: Some(ResourceScope::Request),
            dimension: ResourceDimension::Bytes,
            requested: self.required.and_then(|n| u64::try_from(n).ok()),
            required_growth: None,
            limit: u64::try_from(self.limit).ok(),
            live: None,
            committed: None,
        })
    }
}
