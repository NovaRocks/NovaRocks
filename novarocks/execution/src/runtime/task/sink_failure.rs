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

//! Runtime materialization owns binding validity and executable identity extents.
use super::{ExecutionFailure, ExecutionReason};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{
    fmt,
    num::{NonZeroU16, TryFromIntError},
};

#[derive(Debug)]
enum SinkMaterializationFailure {
    Contract(BoundedDetail),
    RootIdentity {
        required: usize,
        source: TryFromIntError,
    },
    KeyIdentity {
        required: usize,
        source: TryFromIntError,
    },
}
impl fmt::Display for SinkMaterializationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) => detail.fmt(f),
            Self::RootIdentity { required, .. } => {
                write!(f, "compiled program root index {required} exceeds i32")
            }
            Self::KeyIdentity { required, .. } => {
                write!(f, "compiled partition key index {required} exceeds u32")
            }
        }
    }
}
impl std::error::Error for SinkMaterializationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RootIdentity { source, .. } | Self::KeyIdentity { source, .. } => Some(source),
            Self::Contract(_) => None,
        }
    }
}
impl FailureProjection for SinkMaterializationFailure {
    fn failure_fact(&self) -> FailureFact {
        let (reason, required, layout, maximum) = match self {
            Self::Contract(_) => {
                return FailureFact::new(
                    FailureClass::Invariant,
                    ExecutionReason::SinkMaterializationContract.reason(),
                );
            }
            Self::RootIdentity { required, .. } => (
                ExecutionReason::SinkRootIdentityExtent,
                *required,
                42,
                i32::MAX as u64,
            ),
            Self::KeyIdentity { required, .. } => (
                ExecutionReason::SinkKeyIdentityExtent,
                *required,
                43,
                u32::MAX as u64,
            ),
        };
        FailureFact::new(FailureClass::Representation, reason.reason()).with_representation(
            RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: NonZeroU16::new(layout).unwrap(),
                },
                required: u64::try_from(required).ok(),
                extent_limit: Some(maximum),
                representable_prefix: None,
            },
        )
    }
}
pub(super) fn contract(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(SinkMaterializationFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn root_identity(required: usize) -> Result<i32, ExecutionFailure> {
    i32::try_from(required).map_err(|source| {
        ExecutionFailure::from_domain(SinkMaterializationFailure::RootIdentity { required, source })
    })
}
pub(super) fn key_identity(required: usize) -> Result<u32, ExecutionFailure> {
    u32::try_from(required).map_err(|source| {
        ExecutionFailure::from_domain(SinkMaterializationFailure::KeyIdentity { required, source })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn actual_identity_narrowing_keeps_its_source_and_no_input_prefix() {
        assert_eq!(root_identity(i32::MAX as usize).unwrap(), i32::MAX);
        let required = i32::MAX as usize + 1;
        let failure = root_identity(required).unwrap_err();
        let facts = failure.failure_fact().representation().unwrap();
        assert_eq!(facts.required, Some(required as u64));
        assert_eq!(facts.extent_limit, Some(i32::MAX as u64));
        assert_eq!(facts.representable_prefix, None);
        let source = failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<SinkMaterializationFailure>()
            .unwrap();
        assert!(source.source().unwrap().is::<TryFromIntError>());
        assert_eq!(key_identity(u32::MAX as usize).unwrap(), u32::MAX);
        if let Some(required) = (u32::MAX as usize).checked_add(1) {
            let failure = key_identity(required).unwrap_err();
            assert_eq!(
                failure
                    .failure_fact()
                    .representation()
                    .unwrap()
                    .extent_limit,
                Some(u32::MAX as u64)
            );
        }
    }
}
