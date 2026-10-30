// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original value-domain facts. Caller failures are never reclassified here.
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, OuterFailureProjection,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum ValueViolation {
    Carrier = 100,
    Dictionary = 101,
    Bounds = 102,
    RequiredNull = 103,
    Layout = 104,
    UndefinedRow = 105,
    RowTransaction = 106,
    Structure = 107,
    Utf8Boundary = 108,
    Depth = 109,
    Nodes = 110,
}

/// Stable field-specific representation identifiers: 1..=6 belong to
/// ByteLayout, 7 counts view regions, 8 measures a view's start offset in bytes,
/// 9 measures Arrow offset extent, 10 counts dictionary entries, and 11 measures
/// owned allocation bytes (bounded by the Rust allocation Layout).
pub fn field_representation(
    code: u16,
    boundary: RepresentationBoundary,
    required: Option<u64>,
    limit: Option<u64>,
    prefix: Option<u64>,
) -> RepresentationFacts {
    RepresentationFacts {
        boundary,
        layout: RepresentationLayout {
            domain: domain::VALUE_ARRAYS,
            code: NonZeroU16::new(code).expect("nonzero value layout"),
        },
        required,
        extent_limit: limit,
        representable_prefix: prefix,
    }
}

#[derive(Debug)]
pub enum ValueError {
    Layout(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    Invariant(ValueViolation),
    Representation(RepresentationFacts),
    Allocation {
        requested: usize,
        source: std::collections::TryReserveError,
    },
    Allocator {
        requested: usize,
        source: allocator_api2::collections::TryReserveError,
    },
    Arrow(arrow_schema::ArrowError),
}
impl ValueError {
    pub const fn invariant(violation: ValueViolation) -> Self {
        Self::Invariant(violation)
    }
    pub fn representation(&self) -> Option<RepresentationFacts> {
        match self {
            Self::Representation(facts) => Some(*facts),
            _ => None,
        }
    }
}
impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Layout(source) => source.fmt(f),
            Self::Invariant(violation) => write!(f, "value array contract violated: {violation:?}"),
            Self::Representation(facts) => write!(
                f,
                "value representation cannot satisfy {:?} layout {}:{}",
                facts.boundary, facts.layout.domain, facts.layout.code
            ),
            Self::Allocation { requested, .. } | Self::Allocator { requested, .. } => {
                write!(f, "value array allocation refused: {requested} bytes")
            }
            Self::Arrow(source) => write!(f, "value array construction failed: {source}"),
        }
    }
}
impl Error for ValueError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Layout(source) => Some(source),
            Self::Allocation { source, .. } => Some(source),
            Self::Allocator { source, .. } => Some(source),
            Self::Arrow(source) => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for ValueError {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::Layout(source) => return source.failure_fact(),
            Self::Invariant(violation) => (FailureClass::Invariant, *violation as u16),
            Self::Representation(_) => (FailureClass::Representation, 1),
            Self::Allocation { .. } | Self::Allocator { .. } => (FailureClass::Resource, 2),
            Self::Arrow(arrow_schema::ArrowError::MemoryError(_)) => (FailureClass::Resource, 4),
            Self::Arrow(arrow_schema::ArrowError::OffsetOverflowError(_)) => {
                (FailureClass::Representation, 5)
            }
            Self::Arrow(arrow_schema::ArrowError::DictionaryKeyOverflowError) => {
                (FailureClass::Representation, 6)
            }
            Self::Arrow(
                arrow_schema::ArrowError::ExternalError(_) | arrow_schema::ArrowError::IoError(..),
            ) => (FailureClass::Unexpected, 7),
            Self::Arrow(_) => (FailureClass::Invariant, 3),
        };
        let mut fact = FailureFact::new(
            class,
            FailureReason::new(domain::VALUE_ARRAYS, NonZeroU16::new(code).unwrap()),
        );
        if let Self::Representation(details) = self {
            fact = fact.with_representation(*details);
        }
        if let Self::Allocation { requested, .. } | Self::Allocator { requested, .. } = self {
            use novarocks_failure_contract::{ResourceDimension, ResourceFacts};
            fact = fact.with_resource(ResourceFacts {
                request_scope: None,
                constraint_scope: None,
                dimension: ResourceDimension::Bytes,
                requested: u64::try_from(*requested).ok(),
                required_growth: None,
                limit: None,
                live: None,
                committed: None,
            });
        }
        if let Self::Arrow(source) = self {
            match source {
                arrow_schema::ArrowError::OffsetOverflowError(required) => {
                    fact = fact.with_representation(field_representation(
                        9,
                        RepresentationBoundary::SingleOutput,
                        u64::try_from(*required).ok(),
                        None,
                        None,
                    ));
                }
                arrow_schema::ArrowError::DictionaryKeyOverflowError => {
                    fact = fact.with_representation(field_representation(
                        10,
                        RepresentationBoundary::SingleOutput,
                        None,
                        None,
                        None,
                    ));
                }
                _ => {}
            }
        }
        fact
    }
}
impl OuterFailureProjection for ValueError {}
impl From<arrow_schema::ArrowError> for ValueError {
    fn from(source: arrow_schema::ArrowError) -> Self {
        Self::Arrow(source)
    }
}

#[derive(Debug)]
pub enum ValueFailure<E> {
    Value(ValueError),
    Caller(E),
}
impl<E> From<ValueError> for ValueFailure<E> {
    fn from(error: ValueError) -> Self {
        Self::Value(error)
    }
}
impl<E: fmt::Display> fmt::Display for ValueFailure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(error) => error.fmt(f),
            Self::Caller(error) => error.fmt(f),
        }
    }
}
impl<E: Error + 'static> Error for ValueFailure<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Value(error) => error,
            Self::Caller(error) => error,
        })
    }
}
impl<E: FailureProjection> FailureProjection for ValueFailure<E> {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Value(error) => error.failure_fact(),
            Self::Caller(error) => error.failure_fact(),
        }
    }
}
impl<E: OuterFailureProjection> OuterFailureProjection for ValueFailure<E> {}

impl ValueFailure<std::convert::Infallible> {
    pub fn into_error(self) -> ValueError {
        match self {
            Self::Value(error) => error,
            Self::Caller(impossible) => match impossible {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ByteLayout;
    use novarocks_failure_contract::RepresentationBoundary;
    #[test]
    fn complete_representation_facts_keep_boundary_units_and_proven_prefix() {
        for boundary in [
            RepresentationBoundary::MinimumUnit,
            RepresentationBoundary::SingleOutput,
        ] {
            let details = ByteLayout::Utf8.representation(
                boundary,
                Some(2147483648),
                Some(2147483647),
                Some(3),
            );
            let fact = ValueError::Representation(details).failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            assert_eq!((fact.reason().domain(), fact.reason().code()), (22, 1));
            assert_eq!(fact.representation(), Some(details));
            assert_eq!(details.layout.code.get(), 1);
            assert_eq!(details.representable_prefix, Some(3));
        }
        assert_eq!(
            ByteLayout::Utf8View
                .representation(RepresentationBoundary::MinimumUnit, None, None, None)
                .representable_prefix,
            None
        );
    }
    #[test]
    fn shared_layout_error_preserves_the_original_neutral_source_projection() {
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
        for source in [
            LayoutResourceError::SourceModel,
            LayoutResourceError::ArcBacking,
            LayoutResourceError::Arithmetic,
        ] {
            let expected = source.failure_fact();
            let error = ValueError::Layout(source);
            assert_eq!(error.failure_fact(), expected);
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<LayoutResourceError>(),
                Some(&source)
            );
        }
    }
    #[test]
    fn arrow_growth_and_extent_keep_their_source_and_semantic_class() {
        let memory = ValueError::from(arrow_schema::ArrowError::MemoryError("refused".into()));
        assert_eq!(memory.failure_fact().class(), FailureClass::Resource);
        assert!(memory.source().unwrap().is::<arrow_schema::ArrowError>());
        let offset = ValueError::from(arrow_schema::ArrowError::OffsetOverflowError(17));
        let facts = offset.failure_fact().representation().unwrap();
        assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(facts.required, Some(17));
        assert_eq!(facts.extent_limit, None);
        assert_eq!(facts.representable_prefix, None);
        assert!(offset.source().unwrap().is::<arrow_schema::ArrowError>());
    }
    #[test]
    fn caller_stop_observation_is_forwarded_without_new_origin() {
        #[derive(Debug)]
        struct Stopped;
        impl FailureProjection for Stopped {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::stop_observation(FailureReason::new(
                    domain::EXECUTION,
                    NonZeroU16::new(44).unwrap(),
                ))
            }
        }
        assert_eq!(
            ValueFailure::Caller(Stopped).failure_fact(),
            Stopped.failure_fact()
        );
    }
}
