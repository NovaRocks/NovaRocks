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

//! Original definition-request projection. Component preparation keeps the
//! exact source, type table, pool namespace and caller control borrowed through
//! emission. It does not certify complete Fragment coverage or a MEM grant.
//! Decoded entries must enter the original checked Fragment constructor.

use crate::{
    physical_binding_v2::ArgumentTypeIds, physical_semantics_v2::SemanticsCodecError,
    physical_type_v2::TypeCodecError,
};
use novarocks_physical_plan::{CallRequestError, ConstantReferenceError, PhysicalCallSite};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::fmt;

mod encode;
mod read;
pub(crate) use encode::encode_unit_requests_in;
pub use encode::{PreparedCallRequestsEncode, encode_call_requests, prepare_call_requests_encode};
pub use encode::{prepare_call_requests_encode_in, prepare_call_requests_encode_with_host_in};
pub use read::{
    CallRequestTypeOwner, DecodedCallRequests, PreparedCallRequestsDecode, decode_call_requests,
    decode_call_requests_with_creator_in, prepare_call_requests_decode,
    prepare_call_requests_decode_with_creator_in,
};
pub(crate) use read::{
    decode_call_requests_in, decode_unit_requests_in, prepare_call_requests_decode_in,
};

/// IDs name exact complete FVT roots in the same emitted type table. They are
/// a projection loan, not another author of the original request.
#[derive(Clone, Copy)]
pub struct CallRequestTypeIds<'a, K = PhysicalCallSite> {
    pub definition: K,
    pub arguments: &'a [ArgumentTypeIds<'a>],
    pub expected_result_type: Option<u32>,
}
pub type UnitRequestTypeIds<'a> = CallRequestTypeIds<'a, novarocks_expression_contract::ExprId>;

#[derive(Clone, Copy, Debug)]
pub struct CallRequestProjectionLimits {
    pub max_definitions: usize,
    pub max_type_references: usize,
    pub max_request_bytes: usize,
    pub max_allocation_requests: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallRequestProjectionFacts {
    pub definition_count: usize,
    pub type_reference_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}

/// Replace this child contribution on the caller's original package meter.
pub type CallRequestAdmit<'a> =
    dyn FnMut(&CallRequestProjectionFacts) -> Result<(), CompileControlError> + 'a;

#[derive(Debug)]
pub enum CallRequestCodecError {
    Control(CompileControlError),
    InvalidShape(&'static str),
    Type(TypeCodecError),
    Constant(ConstantReferenceError),
    Requests(CallRequestError),
    Semantics(SemanticsCodecError),
}
impl From<CompileControlError> for CallRequestCodecError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<TypeCodecError> for CallRequestCodecError {
    fn from(error: TypeCodecError) -> Self {
        match error {
            TypeCodecError::Control(cause) => Self::Control(cause),
            other => Self::Type(other),
        }
    }
}
impl From<ConstantReferenceError> for CallRequestCodecError {
    fn from(error: ConstantReferenceError) -> Self {
        match error {
            ConstantReferenceError::Control(cause)
            | ConstantReferenceError::Constant(
                novarocks_constant_contract::ConstantError::Control(cause),
            ) => Self::Control(cause),
            other => Self::Constant(other),
        }
    }
}
impl From<CallRequestError> for CallRequestCodecError {
    fn from(error: CallRequestError) -> Self {
        match error {
            CallRequestError::Control(cause) => Self::Control(cause),
            other => Self::Requests(other),
        }
    }
}
impl From<SemanticsCodecError> for CallRequestCodecError {
    fn from(error: SemanticsCodecError) -> Self {
        match error {
            SemanticsCodecError::Control(cause) => Self::Control(cause),
            other => Self::Semantics(other),
        }
    }
}

impl<H> From<CompileControlError>
    for crate::host_projection_v2::ProjectionFailure<CallRequestCodecError, H>
{
    fn from(cause: CompileControlError) -> Self {
        Self::Codec(cause.into())
    }
}
impl<H> From<TypeCodecError>
    for crate::host_projection_v2::ProjectionFailure<CallRequestCodecError, H>
{
    fn from(error: TypeCodecError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<ConstantReferenceError>
    for crate::host_projection_v2::ProjectionFailure<CallRequestCodecError, H>
{
    fn from(error: ConstantReferenceError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<CallRequestError>
    for crate::host_projection_v2::ProjectionFailure<CallRequestCodecError, H>
{
    fn from(error: CallRequestError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<SemanticsCodecError>
    for crate::host_projection_v2::ProjectionFailure<CallRequestCodecError, H>
{
    fn from(error: SemanticsCodecError) -> Self {
        Self::Codec(error.into())
    }
}

impl fmt::Display for CallRequestCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::InvalidShape(message) => f.write_str(message),
            Self::Type(error) => error.fmt(f),
            Self::Constant(error) => error.fmt(f),
            Self::Requests(error) => error.fmt(f),
            Self::Semantics(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for CallRequestCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Constant(source) => Some(source),
            Self::Requests(source) => Some(source),
            Self::Semantics(source) => Some(source),
            _ => None,
        }
    }
}
fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, CallRequestCodecError>,
) -> Result<T, CallRequestCodecError> {
    if matches!(&result, Err(CallRequestCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

impl From<novarocks_constant_contract::ConstantTableError> for CallRequestCodecError {
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(ConstantReferenceError::from(error))
    }
}
impl<H> From<novarocks_constant_contract::ConstantTableError>
    for crate::host_projection_v2::ProjectionFailure<CallRequestCodecError, H>
{
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(ConstantReferenceError::from(error))
    }
}
