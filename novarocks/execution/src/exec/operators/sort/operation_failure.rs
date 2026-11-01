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
//! Sort operations retain actual Arrow causes independently of their call stage.
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason};
use arrow::error::ArrowError;
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
#[cfg(test)]
use novarocks_failure_contract::{
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};
use std::fmt;
#[cfg(test)]
use std::num::{NonZeroU16, TryFromIntError};

#[derive(Clone, Copy, Debug)]
pub(super) enum Operation {
    RowConverter,
    Lexsort,
    Take,
    Batch,
    Concat,
    ConcatBatches,
}
#[derive(Debug)]
enum SortOperationFailure {
    Arrow {
        operation: Operation,
        source: ArrowError,
    },
    #[cfg(test)]
    RowIndex {
        required: usize,
        source: TryFromIntError,
    },
}
impl fmt::Display for SortOperationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arrow { operation, source } => write!(f, "sort {operation:?} failed: {source}"),
            #[cfg(test)]
            Self::RowIndex { required, .. } => {
                write!(f, "row index {required} exceeds UInt32Array range")
            }
        }
    }
}
impl std::error::Error for SortOperationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow { source, .. } => Some(source),
            #[cfg(test)]
            Self::RowIndex { source, .. } => Some(source),
        }
    }
}
impl FailureProjection for SortOperationFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Arrow {
                source: ArrowError::MemoryError(_),
                ..
            } => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::SortOperationAllocation.reason(),
            ),
            // These calls operate on already validated sort declarations and
            // indices; Arrow's error text cannot supply value or offset facts.
            Self::Arrow { .. } => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::SortOperationArrow.reason(),
            ),
            #[cfg(test)]
            Self::RowIndex { required, .. } => FailureFact::new(
                FailureClass::Representation,
                ExecutionReason::SortRowIndexExtent.reason(),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: NonZeroU16::new(44).unwrap(),
                },
                required: u64::try_from(*required).ok(),
                extent_limit: Some(u32::MAX as u64),
                representable_prefix: None,
            }),
        }
    }
}
pub(super) fn arrow(operation: Operation, source: ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(SortOperationFailure::Arrow { operation, source })
}

#[cfg(test)]
fn row_index(required: usize) -> Result<u32, ExecutionFailure> {
    u32::try_from(required).map_err(|source| {
        ExecutionFailure::from_domain(SortOperationFailure::RowIndex { required, source })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn real_arrow_converter_refusal_keeps_sdk_source_and_drops_field() {
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;
        let field = Arc::new(Field::new(
            "ResourceExhausted",
            // A Map entries field must be Struct. Arrow 60 supports valid Maps;
            // this malformed declaration exercises its actual constructor error.
            DataType::Int32,
            false,
        ));
        let weak = Arc::downgrade(&field);
        let malformed = DataType::Map(field.clone(), false);
        let source = arrow::row::RowConverter::new(vec![arrow::row::SortField::new(malformed)])
            .expect_err("Map entries must be a Struct");
        let failure = arrow(Operation::RowConverter, source);
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().resource(), None);
        let source = failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<SortOperationFailure>()
            .unwrap();
        assert!(source.source().unwrap().is::<ArrowError>());
        drop(field);
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn row_identity_extent_is_not_a_minimum_row_size_or_an_input_prefix() {
        assert_eq!(row_index(u32::MAX as usize).unwrap(), u32::MAX);
        if let Some(required) = (u32::MAX as usize).checked_add(1) {
            let failure = row_index(required).unwrap_err();
            let fact = failure.failure_fact().representation().unwrap();
            assert_eq!(fact.boundary, RepresentationBoundary::SingleOutput);
            assert_eq!(fact.required, Some(required as u64));
            assert_eq!(fact.extent_limit, Some(u32::MAX as u64));
            assert_eq!(fact.representable_prefix, None);
        }
    }
}
