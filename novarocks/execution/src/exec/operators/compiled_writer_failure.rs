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
//! Compiled writer preparation owns frozen binding and call identity failures.
use crate::exec::fragment::error::{FragmentBindingErrorKind, program_binding_failure};
use crate::runtime::task::{ExecutionFailure, ExecutionReason, ExecutionResult};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{
    fmt,
    num::{NonZeroU16, TryFromIntError},
};

pub(super) fn contract(detail: impl fmt::Display) -> ExecutionFailure {
    program_binding_failure(
        FragmentBindingErrorKind::InvalidAssignment,
        BoundedDetail::truncating_display(detail).into_string(),
    )
}
pub(super) fn missing(detail: impl fmt::Display) -> ExecutionFailure {
    program_binding_failure(
        FragmentBindingErrorKind::MissingAssignment,
        BoundedDetail::truncating_display(detail).into_string(),
    )
}
#[derive(Clone, Copy, Debug)]
pub(super) enum IndexKind {
    PartialCall,
    FinalCall,
    Mapping,
    Literal,
}
#[derive(Debug)]
struct WriterIndexFailure {
    kind: IndexKind,
    required: usize,
    source: TryFromIntError,
}
impl fmt::Display for WriterIndexFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "compiled writer {:?} index {} exceeds u32",
            self.kind, self.required
        )
    }
}
impl std::error::Error for WriterIndexFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for WriterIndexFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Representation,
            ExecutionReason::CompiledWriterIndexExtent.reason(),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::MinimumUnit,
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: NonZeroU16::new(45).unwrap(),
            },
            required: u64::try_from(self.required).ok(),
            extent_limit: Some(u32::MAX as u64),
            representable_prefix: None,
        })
    }
}
pub(super) fn index(kind: IndexKind, required: usize) -> ExecutionResult<u32> {
    u32::try_from(required).map_err(|source| {
        ExecutionFailure::from_domain(WriterIndexFailure {
            kind,
            required,
            source,
        })
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn exact_call_and_mapping_identity_failures_retain_original_narrowing() {
        for kind in [
            IndexKind::PartialCall,
            IndexKind::FinalCall,
            IndexKind::Mapping,
            IndexKind::Literal,
        ] {
            assert_eq!(index(kind, u32::MAX as usize).unwrap(), u32::MAX);
            if let Some(required) = (u32::MAX as usize).checked_add(1) {
                let failure = index(kind, required).unwrap_err();
                let facts = failure.failure_fact().representation().unwrap();
                assert_eq!(facts.required, Some(required as u64));
                assert_eq!(facts.extent_limit, Some(u32::MAX as u64));
                assert_eq!(facts.representable_prefix, None);
                let source = failure
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .downcast_ref::<WriterIndexFailure>()
                    .unwrap();
                assert!(source.source().unwrap().is::<TryFromIntError>());
            }
        }
    }
}
