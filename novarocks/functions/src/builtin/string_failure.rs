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

//! String domains keep maskable syntax/value refusals separate from outer SDK failures.
use crate::{DataFailureReason, KernelFailure, RowDataError};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, OuterFailureProjection,
    ResourceDimension, ResourceFacts, domain,
};
use std::{error::Error, fmt, num::NonZeroU16, sync::Arc};

/// Only a real SDK Syntax variant can create this maskable cause. It carries
/// no input array or execution capability; compiled-size refusals never enter it.
#[derive(Clone, Debug)]
pub struct RegexSyntax(Arc<regex::Error>);
impl fmt::Display for RegexSyntax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for RegexSyntax {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[derive(Clone, Copy)]
pub enum StringDataError<'a> {
    Regex(&'a RegexSyntax),
    RegexLiteral {
        pattern: &'a str,
        source: &'a RegexSyntax,
    },
}
impl fmt::Display for StringDataError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Regex(source) => source.fmt(f),
            Self::RegexLiteral { pattern, source } => write!(
                f,
                "Invalid regex expression: {pattern}. Detail message: {source}"
            ),
        }
    }
}

#[derive(Debug)]
enum RequiredStringCause {
    Regex(RegexSyntax),
    RegexLiteral {
        pattern: novarocks_failure_contract::BoundedDetail,
        source: RegexSyntax,
    },
    Journal(RowDataError),
}
#[derive(Debug)]
struct RequiredStringRowFailure {
    _row: usize,
    cause: RequiredStringCause,
}
impl fmt::Display for RequiredStringRowFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            RequiredStringCause::Regex(source) => source.fmt(f),
            RequiredStringCause::RegexLiteral { pattern, source } => write!(
                f,
                "Invalid regex expression: {}. Detail message: {source}",
                pattern.as_str()
            ),
            RequiredStringCause::Journal(source) => source.fmt(f),
        }
    }
}
impl Error for RequiredStringRowFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match &self.cause {
            RequiredStringCause::Regex(source)
            | RequiredStringCause::RegexLiteral { source, .. } => source as &dyn Error,
            RequiredStringCause::Journal(source) => source as &dyn Error,
        })
    }
}
impl FailureProjection for RequiredStringRowFailure {
    fn failure_fact(&self) -> FailureFact {
        match &self.cause {
            RequiredStringCause::Regex(_) | RequiredStringCause::RegexLiteral { .. } => {
                DataFailureReason::InvalidValue
            }
            RequiredStringCause::Journal(source) => source.reason(),
        }
        .failure_fact()
    }
}
impl OuterFailureProjection for RequiredStringRowFailure {}
pub(super) fn required_row(row: usize, source: StringDataError<'_>) -> KernelFailure {
    KernelFailure::from_domain(RequiredStringRowFailure {
        _row: row,
        cause: match source {
            StringDataError::Regex(source) => RequiredStringCause::Regex(source.clone()),
            StringDataError::RegexLiteral { pattern, source } => {
                RequiredStringCause::RegexLiteral {
                    pattern: novarocks_failure_contract::BoundedDetail::truncating(pattern),
                    source: source.clone(),
                }
            }
        },
    })
}
pub(super) fn required_journal(source: &RowDataError) -> KernelFailure {
    KernelFailure::from_domain(RequiredStringRowFailure {
        _row: source.selected_ordinal(),
        cause: RequiredStringCause::Journal(source.clone()),
    })
}

#[derive(Debug)]
enum StringOuterFailure {
    MurmurCast(arrow_schema::ArrowError),
    RegexCompile(Arc<regex::Error>),
    Json(serde_json::Error),
    ArrayLayout {
        count: usize,
        width: usize,
        source: std::alloc::LayoutError,
    },
    Reservation {
        count: usize,
        width: usize,
        source: std::collections::TryReserveError,
    },
}
impl fmt::Display for StringOuterFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MurmurCast(source) => {
                write!(f, "cast to Utf8 failed for murmur_hash3_32: {source}")
            }
            Self::RegexCompile(source) => source.fmt(f),
            Self::Json(source) => source.fmt(f),
            Self::ArrayLayout {
                count,
                width,
                source,
            } => write!(
                f,
                "string output layout for {count} elements of {width} bytes: {source}"
            ),
            Self::Reservation {
                count,
                width,
                source,
            } => write!(
                f,
                "string output reservation for {count} elements of {width} bytes: {source}"
            ),
        }
    }
}
impl Error for StringOuterFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::MurmurCast(source) => source as &dyn Error,
            Self::RegexCompile(source) => source.as_ref() as &dyn Error,
            Self::Json(source) => source as &dyn Error,
            Self::ArrayLayout { source, .. } => source as &dyn Error,
            Self::Reservation { source, .. } => source as &dyn Error,
        })
    }
}
fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
    )
}
impl FailureProjection for StringOuterFailure {
    fn failure_fact(&self) -> FailureFact {
        use arrow_schema::ArrowError;
        match self {
            Self::MurmurCast(ArrowError::MemoryError(_)) => fact(FailureClass::Resource, 350),
            Self::MurmurCast(
                ArrowError::CastError(_)
                | ArrowError::ParseError(_)
                | ArrowError::NotYetImplemented(_)
                | ArrowError::InvalidArgumentError(_),
            ) => DataFailureReason::InvalidValue.failure_fact(),
            Self::MurmurCast(ArrowError::ArithmeticOverflow(_) | ArrowError::DivideByZero) => {
                DataFailureReason::NumericOutOfRange.failure_fact()
            }
            Self::MurmurCast(_) => fact(FailureClass::Invariant, 351),
            Self::Json(_) => fact(FailureClass::Invariant, 352),
            Self::ArrayLayout { count, width, .. } | Self::Reservation { count, width, .. } => {
                fact(
                    FailureClass::Resource,
                    if matches!(self, Self::ArrayLayout { .. }) {
                        355
                    } else {
                        356
                    },
                )
                .with_resource(ResourceFacts {
                    request_scope: None,
                    constraint_scope: None,
                    dimension: ResourceDimension::Bytes,
                    requested: u64::try_from(*count as u128 * *width as u128).ok(),
                    required_growth: None,
                    limit: None,
                    live: None,
                    committed: None,
                })
            }
            Self::RegexCompile(source) => match source.as_ref() {
                regex::Error::CompiledTooBig(limit) => fact(FailureClass::Resource, 353)
                    .with_resource(ResourceFacts {
                        request_scope: None,
                        constraint_scope: None,
                        dimension: ResourceDimension::Bytes,
                        requested: None,
                        required_growth: None,
                        limit: u64::try_from(*limit).ok(),
                        live: None,
                        committed: None,
                    }),
                _ => fact(FailureClass::Invariant, 354),
            },
        }
    }
}
impl OuterFailureProjection for StringOuterFailure {}
pub(super) fn murmur_cast(source: arrow_schema::ArrowError) -> KernelFailure {
    KernelFailure::from_domain(StringOuterFailure::MurmurCast(source))
}
pub(super) fn json(source: serde_json::Error) -> KernelFailure {
    KernelFailure::from_domain(StringOuterFailure::Json(source))
}
pub(super) fn regex_syntax(source: &Arc<regex::Error>) -> Result<RegexSyntax, KernelFailure> {
    match source.as_ref() {
        regex::Error::Syntax(_) => Ok(RegexSyntax(Arc::clone(source))),
        _ => Err(KernelFailure::from_domain(
            StringOuterFailure::RegexCompile(Arc::clone(source)),
        )),
    }
}

#[cfg(test)]
pub(super) fn original_source<E: Error + 'static>(failure: &KernelFailure) -> &E {
    let mut source: &(dyn Error + 'static) = failure;
    loop {
        if let Some(source) = source.downcast_ref::<E>() {
            return source;
        }
        source = source
            .source()
            .expect("original typed SDK cause must remain available");
    }
}

#[cfg(test)]
mod properties {
    use super::super::string_extended::{StringOperation, evaluate_required};
    use super::*;
    use arrow_array::{ArrayRef, Int32Array, StringArray};

    #[test]
    fn actual_regex_compiled_size_refusal_keeps_original_sdk_and_known_limit() {
        let source = Arc::new(
            regex::RegexBuilder::new("[a-z]{100}")
                .size_limit(0)
                .build()
                .unwrap_err(),
        );
        let failure = regex_syntax(&source).unwrap_err();
        let facts = failure.failure_fact();
        assert_eq!(facts.class(), FailureClass::Resource);
        assert_eq!(facts.reason().code(), 353);
        assert_eq!(facts.resource().unwrap().limit, Some(0));
        assert_eq!(facts.resource().unwrap().requested, None);
        assert_eq!(facts.representation(), None);
        assert!(std::ptr::eq(
            original_source::<regex::Error>(&failure),
            source.as_ref()
        ));
    }
    #[test]
    fn actual_required_syntax_failure_retains_first_row_and_releases_all_input_arrays() {
        let pattern = format!("{}(", "ResourceExhausted".repeat(128));
        let original = regex::Regex::new(&pattern).unwrap_err().to_string();
        let arrays: [ArrayRef; 3] = [
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec![pattern.as_str(), "(later"])),
            Arc::new(Int32Array::from(vec![0, 0])),
        ];
        let weak = arrays.each_ref().map(Arc::downgrade);
        let failure = evaluate_required(StringOperation::RegexpExtract, &arrays, 2).unwrap_err();
        drop(arrays);
        assert!(weak.iter().all(|input| input.upgrade().is_none()));
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 40);
        assert_eq!(
            original_source::<RequiredStringRowFailure>(&failure)._row,
            0
        );
        assert_eq!(
            original_source::<regex::Error>(&failure).to_string(),
            original
        );
        assert!(failure.to_string().len() <= crate::MAX_ROW_ERROR_MESSAGE_BYTES);
    }
}

/// A layout observation is not an allocation grant.
pub(super) fn array_layout<T>(count: usize) -> Result<std::alloc::Layout, KernelFailure> {
    std::alloc::Layout::array::<T>(count).map_err(|source| {
        KernelFailure::from_domain(StringOuterFailure::ArrayLayout {
            count,
            width: std::mem::size_of::<T>(),
            source,
        })
    })
}
pub(super) fn reservation<T>(
    count: usize,
    source: std::collections::TryReserveError,
) -> KernelFailure {
    KernelFailure::from_domain(StringOuterFailure::Reservation {
        count,
        width: std::mem::size_of::<T>(),
        source,
    })
}

#[cfg(test)]
mod fixed_storage_properties {
    use super::*;
    #[test]
    fn actual_layout_failure_retains_original_std_error_without_input_representation_claim() {
        let failure = array_layout::<u64>(usize::MAX).unwrap_err();
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().code(), 355);
        assert_eq!(fact.representation(), None);
        assert_eq!(fact.resource().unwrap().limit, None);
        assert_eq!(fact.resource().unwrap().committed, None);
        assert_eq!(
            fact.resource().unwrap().requested,
            u64::try_from(usize::MAX as u128 * 8).ok()
        );
        let _ = original_source::<std::alloc::LayoutError>(&failure);
    }
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn fixed_i32_layout_has_no_variable_width_i32_byte_offset_limit() {
        let rows = i32::MAX as usize / 4 + 1;
        let layout = array_layout::<i32>(rows).unwrap();
        assert_eq!(layout.size(), rows * 4);
        assert!(layout.size() > i32::MAX as usize);
        // This observes only a layout. No memory is allocated or authorized.
    }
}

/// The direct regexp predicate is a required row boundary. Only the SDK's
/// actual Syntax variant is a value failure; compiled-size/control failures
/// retain the same outer author used by all other regex consumers.
pub fn required_regex_compile_failure(row: usize, source: Arc<regex::Error>) -> KernelFailure {
    match regex_syntax(&source) {
        Ok(syntax) => required_row(row, StringDataError::Regex(&syntax)),
        Err(cause) => cause,
    }
}

#[cfg(test)]
mod required_regex_predicate_properties {
    use super::*;
    #[test]
    fn required_predicate_keeps_the_actual_syntax_object_and_value_reason() {
        #[expect(
            clippy::invalid_regex,
            reason = "The test requires the SDK error for a deliberately malformed pattern."
        )]
        let source = Arc::new(regex::Regex::new("(").unwrap_err());
        let failure = required_regex_compile_failure(7, source.clone());
        assert_eq!(
            failure.failure_fact(),
            crate::DataFailureReason::InvalidValue.failure_fact()
        );
        assert!(std::ptr::eq(
            original_source::<regex::Error>(&failure),
            source.as_ref()
        ));
    }
    #[test]
    fn compiled_size_is_outer_resource_with_only_the_sdk_known_limit() {
        let source = Arc::new(
            regex::RegexBuilder::new("a{100}")
                .size_limit(1)
                .build()
                .unwrap_err(),
        );
        let failure = required_regex_compile_failure(7, source.clone());
        assert_eq!(failure.failure_fact().class(), FailureClass::Resource);
        let resource = failure.failure_fact().resource().unwrap();
        assert_eq!(resource.limit, Some(1));
        assert_eq!(resource.requested, None);
        assert_eq!(resource.live, None);
        assert!(std::ptr::eq(
            original_source::<regex::Error>(&failure),
            source.as_ref()
        ));
    }
}
