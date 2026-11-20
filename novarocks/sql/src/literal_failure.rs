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

//! SQL literal author causes. No input expression, type tree or value storage
//! is kept alive by its diagnostic; external domain facts remain their own.
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason, ResourceDimension,
    ResourceFacts, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Debug)]
pub struct LiteralFailure(Box<Cause>);
#[derive(Debug)]
enum Cause {
    Rejected(BoundedDetail),
    Numeric(&'static str),
    Hex {
        context: BoundedDetail,
        source: hex::FromHexError,
    },
    Base64(base64::DecodeError),
    JsonSerialize(serde_json::Error),
    DecimalParameters(arrow::error::ArrowError),
    Address {
        required: u128,
        source: std::num::TryFromIntError,
    },
    Variant {
        context: &'static str,
        source: novarocks_types::value::variant_encode::VariantEncodeFailure,
    },
    Bitmap(novarocks_types::value::bitmap::BitmapValueFailure),
    Layout {
        required: u128,
        source: std::alloc::LayoutError,
    },
    Allocation {
        required: u128,
        source: std::collections::TryReserveError,
    },
}
impl LiteralFailure {
    pub(super) fn address(required: u128, source: std::num::TryFromIntError) -> Self {
        Self(Box::new(Cause::Address { required, source }))
    }
    pub(super) fn decimal_parameters(source: arrow::error::ArrowError) -> Self {
        Self(Box::new(Cause::DecimalParameters(source)))
    }
    pub(super) fn rejected(detail: impl fmt::Display) -> Self {
        Self(Box::new(Cause::Rejected(
            BoundedDetail::truncating_display(detail),
        )))
    }
    pub(super) fn numeric(detail: &'static str) -> Self {
        Self(Box::new(Cause::Numeric(detail)))
    }
    pub(super) fn hex(context: impl fmt::Display, source: hex::FromHexError) -> Self {
        Self(Box::new(Cause::Hex {
            context: BoundedDetail::truncating_display(context),
            source,
        }))
    }
    pub(super) fn base64(source: base64::DecodeError) -> Self {
        Self(Box::new(Cause::Base64(source)))
    }
    pub(super) fn json(source: serde_json::Error) -> Self {
        Self(Box::new(Cause::JsonSerialize(source)))
    }
    pub(super) fn variant(
        context: &'static str,
        source: novarocks_types::value::variant_encode::VariantEncodeFailure,
    ) -> Self {
        Self(Box::new(Cause::Variant { context, source }))
    }
    pub(super) fn layout(required: u128, source: std::alloc::LayoutError) -> Self {
        Self(Box::new(Cause::Layout { required, source }))
    }
    pub(super) fn allocation(required: u128, source: std::collections::TryReserveError) -> Self {
        Self(Box::new(Cause::Allocation { required, source }))
    }
}
impl From<novarocks_types::value::bitmap::BitmapValueFailure> for LiteralFailure {
    fn from(source: novarocks_types::value::bitmap::BitmapValueFailure) -> Self {
        Self(Box::new(Cause::Bitmap(source)))
    }
}
impl fmt::Display for LiteralFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            Cause::Rejected(detail) => f.write_str(detail.as_str()),
            Cause::Numeric(detail) => f.write_str(detail),
            Cause::Hex { context, source } => write!(f, "{}: {source}", context.as_str()),
            Cause::Address { required, source } => write!(
                f,
                "literal value storage requires {required} bytes beyond its addressable count: {source}"
            ),
            Cause::DecimalParameters(source) => {
                write!(f, "invalid literal DECIMAL parameters: {source}")
            }
            Cause::Base64(source) => write!(f, "to_binary encode64 decode failed: {source}"),
            Cause::JsonSerialize(source) => write!(f, "json_object stringify failed: {source}"),
            Cause::Variant { context, source } => write!(f, "{context}: {source}"),
            Cause::Bitmap(source) => source.fmt(f),
            Cause::Layout { required, source } => write!(
                f,
                "literal value storage requires {required} bytes: {source}"
            ),
            Cause::Allocation { required, source } => write!(
                f,
                "literal value storage allocation requires {required} bytes: {source}"
            ),
        }
    }
}
impl Error for LiteralFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.0.as_ref() {
            Cause::Hex { source, .. } => Some(source),
            Cause::Base64(source) => Some(source),
            Cause::DecimalParameters(source) => Some(source),
            Cause::Address { source, .. } => Some(source),
            Cause::JsonSerialize(source) => Some(source),
            Cause::Variant { source, .. } => Some(source),
            Cause::Bitmap(source) => Some(source),
            Cause::Layout { source, .. } => Some(source),
            Cause::Allocation { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for LiteralFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self.0.as_ref() {
            Cause::Rejected(_) => (FailureClass::Data, 1),
            Cause::DecimalParameters(_) => (FailureClass::Data, 2),
            Cause::Address { .. } => (FailureClass::Resource, 6),
            Cause::Numeric(_) => {
                return novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact();
            }
            Cause::Hex { .. } | Cause::Base64(_) => {
                return novarocks_functions::DataFailureReason::InvalidEncoding.failure_fact();
            }
            Cause::Variant { source, .. } => return source.failure_fact(),
            Cause::Bitmap(source) => return source.failure_fact(),
            Cause::JsonSerialize(_) => (FailureClass::Invariant, 3),
            Cause::Layout { .. } => (FailureClass::Resource, 4),
            Cause::Allocation { .. } => (FailureClass::Resource, 5),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::SQL, NonZeroU16::new(code).unwrap()),
        );
        match self.0.as_ref() {
            Cause::Layout { required, .. }
            | Cause::Allocation { required, .. }
            | Cause::Address { required, .. } => fact.with_resource(ResourceFacts {
                request_scope: None,
                constraint_scope: None,
                dimension: ResourceDimension::Bytes,
                requested: u64::try_from(*required).ok(),
                required_growth: None,
                limit: if matches!(
                    self.0.as_ref(),
                    Cause::Layout { .. } | Cause::Address { .. }
                ) {
                    Some(isize::MAX as u64)
                } else {
                    None
                },
                live: None,
                committed: None,
            }),
            _ => fact,
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for LiteralFailure {}
