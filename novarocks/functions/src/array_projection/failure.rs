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

//! Original causes for execution-array preparation and its construction model.
use arrow_schema::ArrowError;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use novarocks_type_contract::{ValueTypeError, owned_resources::layout::LayoutResourceError};
use std::{error::Error, fmt, num::NonZeroU16};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum ArrayProjectionViolation {
    LayoutMismatch = 100,
    RequiredRootNull = 101,
    MapEntriesType = 102,
    MapEntriesArity = 103,
    ScalarChanged = 104,
    MissingDictionaryId = 105,
    MissingDictionaryOrder = 106,
    PhysicalWrapper = 107,
    DictionaryKeyType = 108,
    RunEndChildren = 109,
    ScalarRebuild = 110,
    UnsupportedContainer = 111,
    StructOccurrence = 112,
    HydrationPath = 113,
    MissingDictionaryCarrier = 114,
    UnsupportedEncodingTransition = 115,
    NonemptyNullString = 116,
    UnsupportedNullNormalization = 117,
    NonInt32Dictionary = 118,
    NonStringDictionary = 119,
    DictionaryKeyBounds = 120,
    UnmaskedRequiredNull = 121,
    ListCarrier = 122,
    ChildRange = 123,
    UnsupportedPath = 124,
    StringOffsets = 125,
    DecodeEncoding = 126,
    NegativeDictionaryKey = 127,
    InvalidStringOffsets = 128,
}
impl fmt::Display for ArrayProjectionViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::LayoutMismatch => "compact typed source differs from its complete value layout",
            Self::RequiredRootNull => "compact typed required source contains logical NULL",
            Self::MapEntriesType => "execution Map entries are not a Struct",
            Self::MapEntriesArity => "execution Map entries do not contain key and value",
            Self::ScalarChanged => "execution array normalization found a changed scalar type",
            Self::MissingDictionaryId => "normalized dictionary field has no actual dictionary ID",
            Self::MissingDictionaryOrder => {
                "normalized dictionary field has no actual dictionary order"
            }
            Self::PhysicalWrapper => "execution array type does not match its physical wrapper",
            Self::DictionaryKeyType => "execution dictionary has an invalid key type",
            Self::RunEndChildren => "execution run-end encoding requires two children",
            Self::ScalarRebuild => "execution normalization cannot change a scalar representation",
            Self::UnsupportedContainer => {
                "default validated normalization has an unsupported container"
            }
            Self::StructOccurrence => "typed hydration Struct occurrence is missing",
            Self::HydrationPath => "typed hydration path disagrees with its complete binding",
            Self::MissingDictionaryCarrier => {
                "required decoded leaf lacks its original Dictionary carrier"
            }
            Self::UnsupportedEncodingTransition => {
                "required decoded leaf has an unsupported encoding transition"
            }
            Self::NonemptyNullString => "required decoded String NULL has a nonempty offset range",
            Self::UnsupportedNullNormalization => {
                "required decoded NULL normalization supports only Int32 UTF-8 dictionaries"
            }
            Self::NonInt32Dictionary => {
                "typed dictionary does not have its admitted Int32 physical keys"
            }
            Self::NonStringDictionary => "typed dictionary does not have direct string values",
            Self::DictionaryKeyBounds => "typed dictionary key is outside its value array",
            Self::UnmaskedRequiredNull => {
                "typed required dictionary child contains an unmasked logical null"
            }
            Self::ListCarrier => "typed List required-child path has a different carrier",
            Self::ChildRange => "typed required List child range overflow",
            Self::UnsupportedPath => "typed required-child path has an unsupported encoding step",
            Self::StringOffsets => "validated String wrapper requires UTF-8 offsets",
            Self::DecodeEncoding => "decode observation does not authorize another encoding",
            Self::NegativeDictionaryKey => "negative dictionary key during observation",
            Self::InvalidStringOffsets => "observed String offsets are invalid",
        })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrayProjectionOperation {
    DecodeDictionary,
    RebuildArray,
    RebuildMapEntries,
    RebuildDictionary,
    NormalizeRequiredString,
    RebuildUnionFields,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StringOffsetWidth {
    I32,
    I64,
}
impl StringOffsetWidth {
    pub(super) fn limit(self) -> u64 {
        match self {
            Self::I32 => i32::MAX as u64,
            Self::I64 => i64::MAX as u64,
        }
    }
}
#[derive(Debug)]
pub enum ArrayProjectionFailure {
    Value(novarocks_value_arrays::ValueError),
    ValueType(ValueTypeError),
    Layout(LayoutResourceError),
    UnionFields(novarocks_type_contract::owned_resources::arrow_union::UnionFieldsRequestError),
    Contract(ArrayProjectionViolation),
    RequestArithmetic,
    ArrayLayout {
        count: usize,
        width: usize,
        source: std::alloc::LayoutError,
    },
    DecodedStringExtent {
        required: Option<u64>,
        width: StringOffsetWidth,
    },
    Arrow {
        operation: ArrayProjectionOperation,
        source: ArrowError,
    },
}
impl fmt::Display for ArrayProjectionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(source) => source.fmt(f),
            Self::ValueType(source) => source.fmt(f),
            Self::UnionFields(source) => source.fmt(f),
            Self::Layout(source) => write!(f, "array construction layout request: {source:?}"),
            Self::Contract(source) => source.fmt(f),
            Self::RequestArithmetic => {
                f.write_str("array construction observation arithmetic exceeds the host extent")
            }
            Self::ArrayLayout { count, width, .. } => write!(
                f,
                "array construction request cannot represent {count} elements of {width} bytes"
            ),
            Self::DecodedStringExtent { required, width } => write!(
                f,
                "decoded string bytes {required:?} exceed the {width:?} offset representation"
            ),
            Self::Arrow { operation, source } => {
                write!(f, "array preparation {operation:?}: {source}")
            }
        }
    }
}
impl Error for ArrayProjectionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Value(source) => Some(source),
            Self::ValueType(source) => Some(source),
            Self::UnionFields(source) => Some(source),
            Self::ArrayLayout { source, .. } => Some(source),
            Self::Arrow { source, .. } => Some(source),
            _ => None,
        }
    }
}
fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
    )
}
fn representation(
    code: u16,
    layout: u16,
    required: Option<u64>,
    limit: Option<u64>,
) -> FailureFact {
    fact(FailureClass::Representation, code).with_representation(RepresentationFacts {
        boundary: RepresentationBoundary::SingleOutput,
        layout: RepresentationLayout {
            domain: domain::KERNEL,
            code: NonZeroU16::new(layout).unwrap(),
        },
        required,
        extent_limit: limit,
        representable_prefix: None,
    })
}
impl FailureProjection for ArrayProjectionFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Value(source) => source.failure_fact(),
            Self::ValueType(source) => source.failure_fact(),
            Self::UnionFields(source) => source.failure_fact(),
            Self::Layout(source) => source.failure_fact(),
            Self::Contract(source) => fact(FailureClass::Invariant, *source as u16),
            Self::RequestArithmetic => fact(FailureClass::Resource, 140),
            Self::ArrayLayout { count, width, .. } => representation(
                141,
                crate::allocation_failure::RUST_ARRAY_BACKING_LAYOUT.get(),
                u64::try_from(*count as u128 * *width as u128).ok(),
                Some(isize::MAX as u64),
            ),
            Self::DecodedStringExtent { required, width } => representation(
                142,
                match width {
                    StringOffsetWidth::I32 => 5,
                    StringOffsetWidth::I64 => 6,
                },
                *required,
                Some(width.limit()),
            ),
            Self::Arrow {
                source: ArrowError::MemoryError(_),
                ..
            } => fact(FailureClass::Resource, 143),
            Self::Arrow {
                source: ArrowError::OffsetOverflowError(required),
                ..
            } => representation(144, 3, u64::try_from(*required).ok(), None),
            Self::Arrow {
                source: ArrowError::DictionaryKeyOverflowError,
                ..
            } => representation(145, 4, None, None),
            // These are pure representation operations under an admitted type.
            // Arithmetic/parse/compute errors here are broken source contracts,
            // not authored SQL-row Data. Original Arrow causes stay inspectable.
            Self::Arrow {
                source: ArrowError::ExternalError(_) | ArrowError::IoError(..),
                ..
            } => fact(FailureClass::Unexpected, 146),
            Self::Arrow { .. } => fact(FailureClass::Invariant, 147),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ArrayProjectionFailure {}
impl From<ArrayProjectionFailure> for crate::KernelFailure {
    fn from(source: ArrayProjectionFailure) -> Self {
        Self::from_domain(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arrow_cause_and_typed_reason_survive_without_parsing_its_message() {
        for (source, class, code) in [
            (
                ArrowError::MemoryError("numeric overflow".into()),
                FailureClass::Resource,
                143,
            ),
            (
                ArrowError::SchemaError("memory capacity exceeded".into()),
                FailureClass::Invariant,
                147,
            ),
            (
                ArrowError::OffsetOverflowError(19),
                FailureClass::Representation,
                144,
            ),
            (
                ArrowError::DictionaryKeyOverflowError,
                FailureClass::Representation,
                145,
            ),
        ] {
            let failure = ArrayProjectionFailure::Arrow {
                operation: ArrayProjectionOperation::DecodeDictionary,
                source,
            };
            assert_eq!(failure.failure_fact().class(), class);
            assert_eq!(failure.failure_fact().reason().code(), code);
            let outer = crate::KernelFailure::from(failure);
            assert_eq!(outer.failure_fact().class(), class);
            let actual = outer
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<ArrayProjectionFailure>()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<ArrowError>()
                .unwrap() as *const _;
            // Clones retain the exact originating Arrow object.
            let clone = outer.clone();
            let cloned = clone
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<ArrayProjectionFailure>()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<ArrowError>()
                .unwrap() as *const _;
            assert_eq!(actual, cloned);
        }
    }
    #[test]
    fn type_profile_limits_delegate_to_the_original_owner() {
        for source in [ValueTypeError::TooDeep, ValueTypeError::TooManyNodes] {
            assert_eq!(
                ArrayProjectionFailure::ValueType(source).failure_fact(),
                source.failure_fact()
            );
        }
    }
}
