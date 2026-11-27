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

//! Constant admission owns declaration, value and diagnostic SDK boundaries.
use super::ConstantError;
use arrow_buffer::MutableBufferError;
use arrow_schema::ArrowError;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, SharedFailureCause, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Debug)]
enum ArrowBoundary {
    Validation,
    DecimalValue,
    Diagnostic,
}
#[derive(Debug)]
struct ConstantArrowFailure {
    boundary: ArrowBoundary,
    source: ArrowError,
}
impl fmt::Display for ConstantArrowFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}
impl Error for ConstantArrowFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::CONSTANT, NonZeroU16::new(code).unwrap()),
    )
}
impl FailureProjection for ConstantArrowFailure {
    fn failure_fact(&self) -> FailureFact {
        if matches!(self.source, ArrowError::MemoryError(_)) {
            return fact(FailureClass::Resource, 3);
        }
        match self.boundary {
            ArrowBoundary::Validation => fact(FailureClass::Invariant, 4),
            ArrowBoundary::DecimalValue => fact(FailureClass::Data, 5),
            ArrowBoundary::Diagnostic => match &self.source {
                ArrowError::CastError(_) | ArrowError::ParseError(_) => fact(FailureClass::Data, 6),
                ArrowError::ExternalError(_) => fact(FailureClass::Unexpected, 7),
                _ => fact(FailureClass::Invariant, 4),
            },
        }
    }
}
impl ConstantError {
    pub fn from_domain<E: Error + FailureProjection + Send + Sync + 'static>(source: E) -> Self {
        Self::Source(SharedFailureCause::new(source))
    }
    /// Arrow rejected an already-declared carrier or its construction contract.
    pub fn arrow_validation(source: ArrowError) -> Self {
        Self::from_domain(ConstantArrowFailure {
            boundary: ArrowBoundary::Validation,
            source,
        })
    }
    /// The coefficient does not fit the admitted decimal precision.
    pub(crate) fn arrow_decimal_value(source: ArrowError) -> Self {
        Self::from_domain(ConstantArrowFailure {
            boundary: ArrowBoundary::DecimalValue,
            source,
        })
    }
    pub(crate) fn arrow_diagnostic(source: ArrowError) -> Self {
        Self::from_domain(ConstantArrowFailure {
            boundary: ArrowBoundary::Diagnostic,
            source,
        })
    }
}
impl FailureProjection for ConstantError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Source(source) => source.failure_fact(),
            Self::Invalid(_) => fact(FailureClass::Invariant, 1),
            Self::Limit(_) => fact(FailureClass::Resource, 2),
        }
    }
}

#[derive(Debug)]
struct ConstantAllocationFailure(std::collections::TryReserveError);
impl fmt::Display for ConstantAllocationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "constant allocation refused: {}", self.0)
    }
}
impl Error for ConstantAllocationFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for ConstantAllocationFailure {
    fn failure_fact(&self) -> FailureFact {
        fact(FailureClass::Resource, 8)
    }
}
#[derive(Debug)]
struct ConstantBackingFailure {
    required: u128,
    source: std::alloc::LayoutError,
}
impl fmt::Display for ConstantBackingFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "constant backing requires {} bytes beyond the Rust array extent: {}",
            self.required, self.source
        )
    }
}
impl Error for ConstantBackingFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for ConstantBackingFailure {
    fn failure_fact(&self) -> FailureFact {
        use novarocks_failure_contract::{
            RepresentationBoundary, RepresentationFacts, RepresentationLayout,
        };
        fact(FailureClass::Representation, 9).with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::CONSTANT,
                code: NonZeroU16::new(1).unwrap(),
            },
            required: u64::try_from(self.required).ok(),
            extent_limit: Some(isize::MAX as u64),
            representable_prefix: None,
        })
    }
}
#[derive(Debug)]
struct ConstantOffsetExtentFailure {
    required: u64,
    extent_limit: u64,
}

impl fmt::Display for ConstantOffsetExtentFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "constant extent {} exceeds the carrier limit {}",
            self.required, self.extent_limit
        )
    }
}

impl Error for ConstantOffsetExtentFailure {}

impl FailureProjection for ConstantOffsetExtentFailure {
    fn failure_fact(&self) -> FailureFact {
        use novarocks_failure_contract::{
            RepresentationBoundary, RepresentationFacts, RepresentationLayout,
        };
        fact(FailureClass::Representation, 13).with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::CONSTANT,
                code: NonZeroU16::new(2).unwrap(),
            },
            required: Some(self.required),
            extent_limit: Some(self.extent_limit),
            representable_prefix: None,
        })
    }
}

#[derive(Debug)]
struct ConstantMutableBufferFailure {
    requested: usize,
    source: MutableBufferError,
}

impl fmt::Display for ConstantMutableBufferFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}

impl Error for ConstantMutableBufferFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl FailureProjection for ConstantMutableBufferFailure {
    fn failure_fact(&self) -> FailureFact {
        use novarocks_failure_contract::{
            RepresentationBoundary, RepresentationFacts, RepresentationLayout, ResourceDimension,
            ResourceFacts,
        };

        let (reason, required, limit) = match self.source {
            MutableBufferError::AllocationError(layout) => {
                return fact(FailureClass::Resource, 8).with_resource(ResourceFacts {
                    request_scope: None,
                    constraint_scope: None,
                    dimension: ResourceDimension::Bytes,
                    requested: u64::try_from(layout.size()).ok(),
                    required_growth: None,
                    limit: None,
                    live: None,
                    committed: None,
                });
            }
            MutableBufferError::LayoutError => {
                let align = arrow_buffer::alloc::ALIGNMENT;
                (
                    14,
                    u64::try_from(self.requested).ok(),
                    u64::try_from((isize::MAX as usize) & !(align - 1)).ok(),
                )
            }
            MutableBufferError::LengthOverflow => (15, None, u64::try_from(usize::MAX).ok()),
        };
        fact(FailureClass::Representation, reason).with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::CONSTANT,
                code: NonZeroU16::new(3).unwrap(),
            },
            required,
            extent_limit: limit,
            representable_prefix: None,
        })
    }
}

impl ConstantError {
    /// A value or its constituent extent cannot be represented by its original carrier.
    pub fn offset_extent(required: u64, extent_limit: u64) -> Self {
        Self::from_domain(ConstantOffsetExtentFailure {
            required,
            extent_limit,
        })
    }

    /// Project a local Arrow buffer construction failure using the requested byte length.
    pub fn arrow_buffer_allocation(bytes: usize, source: MutableBufferError) -> Self {
        Self::from_domain(ConstantMutableBufferFailure {
            requested: bytes,
            source,
        })
    }

    /// True for a concrete fallible allocation failure at constant construction.
    pub fn is_local_allocation_failure(&self) -> bool {
        let Self::Source(source) = self else {
            return false;
        };
        source.downcast_ref::<ConstantAllocationFailure>().is_some()
            || matches!(
                source.downcast_ref::<ConstantMutableBufferFailure>(),
                Some(ConstantMutableBufferFailure {
                    source: MutableBufferError::AllocationError(_),
                    ..
                })
            )
    }

    pub(crate) fn allocation(source: std::collections::TryReserveError) -> Self {
        Self::from_domain(ConstantAllocationFailure(source))
    }
    pub(crate) fn array_backing<T>(count: usize, source: std::alloc::LayoutError) -> Self {
        Self::from_domain(ConstantBackingFailure {
            required: count as u128 * std::mem::size_of::<T>() as u128,
            source,
        })
    }
}

#[derive(Clone, Copy)]
#[repr(u16)]
enum ConstantTableReason {
    MissingPool = 10,
    DuplicatePool = 11,
    SourceTypeMismatch = 12,
}
impl FailureProjection for crate::ConstantTableError {
    fn failure_fact(&self) -> FailureFact {
        use crate::ConstantTableError as E;
        let reason = match self {
            E::Control(source) => return source.failure_fact(),
            E::Constant(source) => return source.failure_fact(),
            E::MissingPool(_) => ConstantTableReason::MissingPool,
            E::DuplicatePool(_) => ConstantTableReason::DuplicatePool,
            E::SourceTypeMismatch(_) => ConstantTableReason::SourceTypeMismatch,
        };
        fact(FailureClass::Invariant, reason as u16)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::{MutableBuffer, alloc::ALIGNMENT};
    use novarocks_failure_contract::RepresentationBoundary;
    use std::alloc::Layout;

    #[test]
    fn mutable_buffer_allocation_failure_retains_actual_layout_request() {
        let layout = Layout::from_size_align(256, ALIGNMENT).unwrap();
        let failure = ConstantError::arrow_buffer_allocation(
            256,
            MutableBufferError::AllocationError(layout),
        );
        let fact = failure.failure_fact();
        assert!(failure.is_local_allocation_failure());
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.resource().unwrap().requested, Some(256));
        assert!(fact.representation().is_none());
    }

    #[test]
    fn mutable_buffer_layout_error_retains_requested_extent_and_alignment_limit() {
        let limit = (isize::MAX as usize) & !(ALIGNMENT - 1);
        let bytes = limit + 1;
        let source = MutableBuffer::try_from_len_zeroed(bytes).unwrap_err();
        assert_eq!(source, MutableBufferError::LayoutError);
        let failure = ConstantError::arrow_buffer_allocation(bytes, source);
        let fact = failure.failure_fact();
        assert!(!failure.is_local_allocation_failure());
        assert_eq!(fact.class(), FailureClass::Representation);
        let representation = fact.representation().unwrap();
        assert_eq!(
            representation.boundary,
            RepresentationBoundary::SingleOutput
        );
        assert_eq!(representation.required, Some(bytes as u64));
        assert_eq!(representation.extent_limit, Some(limit as u64));
    }

    #[test]
    fn mutable_buffer_length_overflow_does_not_infer_a_required_extent() {
        let failure = ConstantError::arrow_buffer_allocation(0, MutableBufferError::LengthOverflow);
        let fact = failure.failure_fact();
        assert!(!failure.is_local_allocation_failure());
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(fact.representation().unwrap().required, None);
    }
}
