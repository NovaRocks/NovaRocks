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
//! Original N admission, stable sort and scalar codec, over explicit storage.
//! N state owns argument validity, codec validity and encoding extents.
use crate::KernelFailure;
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    OuterFailureProjection, RepresentationBoundary, RepresentationFacts, RepresentationLayout,
    domain,
};
use std::{
    error::Error,
    fmt,
    num::{NonZeroU16, TryFromIntError},
};
#[derive(Clone, Copy, Debug)]
pub(super) enum Length {
    Limit,
    Count,
    Utf8,
    DecimalText,
}
impl Length {
    fn code(self) -> u16 {
        match self {
            Self::Limit => 75,
            Self::Count => 76,
            Self::Utf8 => 77,
            Self::DecimalText => 78,
        }
    }
}
#[derive(Debug)]
enum NStateFailure {
    Value(BoundedDetail),
    Argument(BoundedDetail),
    Malformed(BoundedDetail),
    Contract(BoundedDetail),
    NumericConversion(TryFromIntError),
    Utf8 {
        carrier: &'static str,
        source: std::str::Utf8Error,
    },
    Slice {
        carrier: BoundedDetail,
        source: std::array::TryFromSliceError,
    },
    Format {
        kind: Length,
        required: usize,
        source: TryFromIntError,
    },
}
impl fmt::Display for NStateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(detail)
            | Self::Argument(detail)
            | Self::Malformed(detail)
            | Self::Contract(detail) => detail.fmt(f),
            Self::NumericConversion(_) => f.write_str("min_n/max_n limit overflow"),
            Self::Utf8 { carrier, source } => {
                write!(f, "min_n/max_n {carrier} decode failed: {source}")
            }
            Self::Slice { carrier, .. } => write!(f, "min_n/max_n {carrier} decode failed"),
            Self::Format { kind, required, .. } => write!(
                f,
                "min_n/max_n {kind:?} length {required} exceeds u32 state encoding"
            ),
        }
    }
}
impl Error for NStateFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::NumericConversion(source) | Self::Format { source, .. } => Some(source),
            Self::Utf8 { source, .. } => Some(source),
            Self::Slice { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for NStateFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Value(_) => crate::DataFailureReason::InvalidValue.failure_fact(),
            Self::Argument(_) => FailureFact::new(
                FailureClass::Data,
                novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
            ),
            Self::Malformed(_) | Self::Utf8 { .. } => {
                crate::DataFailureReason::InvalidEncoding.failure_fact()
            }
            Self::NumericConversion(_) => {
                crate::DataFailureReason::NumericOutOfRange.failure_fact()
            }
            Self::Contract(_) => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(333).unwrap()),
            ),
            Self::Slice { .. } => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(334).unwrap()),
            ),
            Self::Format { kind, required, .. } => FailureFact::new(
                FailureClass::Representation,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(332).unwrap()),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: NonZeroU16::new(kind.code()).unwrap(),
                },
                required: u64::try_from(*required).ok(),
                extent_limit: Some(u64::from(u32::MAX)),
                representable_prefix: None,
            }),
        }
    }
}
impl OuterFailureProjection for NStateFailure {}
pub(super) fn value(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(NStateFailure::Value(BoundedDetail::truncating_display(
        detail,
    )))
}
pub(super) fn argument(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(NStateFailure::Argument(BoundedDetail::truncating_display(
        detail,
    )))
}
pub(super) fn malformed(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(NStateFailure::Malformed(BoundedDetail::truncating_display(
        detail,
    )))
}
pub(super) fn contract(detail: impl fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(NStateFailure::Contract(BoundedDetail::truncating_display(
        detail,
    )))
}
pub(super) fn numeric_conversion(source: TryFromIntError) -> KernelFailure {
    KernelFailure::from_domain(NStateFailure::NumericConversion(source))
}
pub(super) fn utf8(carrier: &'static str, source: std::str::Utf8Error) -> KernelFailure {
    KernelFailure::from_domain(NStateFailure::Utf8 { carrier, source })
}
pub(super) fn slice(carrier: &str, source: std::array::TryFromSliceError) -> KernelFailure {
    KernelFailure::from_domain(NStateFailure::Slice {
        carrier: BoundedDetail::truncating_display(carrier),
        source,
    })
}
pub(super) fn encoded_len(kind: Length, required: usize) -> Result<u32, KernelFailure> {
    u32::try_from(required).map_err(|source| {
        KernelFailure::from_domain(NStateFailure::Format {
            kind,
            required,
            source,
        })
    })
}
#[derive(Debug)]
struct NDecimalParseFailure<E>(E);
impl<E: Error> fmt::Display for NDecimalParseFailure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("min_n/max_n decimal256 parse failed")
    }
}
impl<E: Error + 'static> Error for NDecimalParseFailure<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl<E> FailureProjection for NDecimalParseFailure<E> {
    fn failure_fact(&self) -> FailureFact {
        crate::DataFailureReason::InvalidEncoding.failure_fact()
    }
}
impl<E: Error + Send + Sync + 'static> OuterFailureProjection for NDecimalParseFailure<E> {}
pub(super) fn decimal_parse<E: Error + Send + Sync + 'static>(source: E) -> KernelFailure {
    KernelFailure::from_domain(NDecimalParseFailure(source))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn each_n_state_length_keeps_exact_format_extent_and_real_narrowing_source() {
        for kind in [
            Length::Limit,
            Length::Count,
            Length::Utf8,
            Length::DecimalText,
        ] {
            let Ok(limit) = usize::try_from(u32::MAX) else {
                continue;
            };
            assert_eq!(encoded_len(kind, limit).unwrap(), u32::MAX);
            let Some(required) = limit.checked_add(1) else {
                continue;
            };
            let failure = encoded_len(kind, required).unwrap_err();
            let fact = failure.failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            let facts = fact.representation().unwrap();
            assert_eq!(facts.layout.code.get(), kind.code());
            assert_eq!(facts.required, Some(required as u64));
            assert_eq!(facts.extent_limit, Some(u64::from(u32::MAX)));
            assert_eq!(facts.representable_prefix, None);
            let mut source = failure.source();
            let mut actual_conversion = false;
            while let Some(actual) = source {
                actual_conversion |= actual.is::<TryFromIntError>();
                source = actual.source();
            }
            assert!(actual_conversion);
        }
    }
}
