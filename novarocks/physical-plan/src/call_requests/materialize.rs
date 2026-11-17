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

//! Materialize one original neutral request from the admitted pool namespace.
//! The caller owns source/type/allocation admission and entry/completion on the
//! same meter. This is neither physical-shape inference nor pure preparation.

use crate::{
    ConstantPolicy, ConstantPools, ConstantReferenceError, PhysicalCallRequest,
    StaticFunctionArgument,
};
use novarocks_function_contract::{FunctionArgument, FunctionBindingRequest};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, FunctionValueType, ValueSchemaCreationFactory,
    ValueSchemaCreationScope, ValueTypeStorageLifetime,
};
use std::alloc::Layout;

/// The original creator/control/source failures stay distinct. Argument-count
/// limits are supplied by the existing call owner, not minted by this adapter.
#[derive(Debug)]
pub enum CallRequestMaterializationError {
    Control(CompileControlError),
    Reference(ConstantReferenceError),
    Invalid(&'static str),
}
impl From<CompileControlError> for CallRequestMaterializationError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<ConstantReferenceError> for CallRequestMaterializationError {
    fn from(value: ConstantReferenceError) -> Self {
        match value {
            ConstantReferenceError::Control(cause)
            | ConstantReferenceError::Constant(
                novarocks_constant_contract::ConstantError::Control(cause),
            ) => Self::Control(cause),
            ConstantReferenceError::Constant(
                novarocks_constant_contract::ConstantError::Limit(_),
            ) => Self::Control(CompileControlError::ResourceExhausted),
            other => Self::Reference(other),
        }
    }
}
impl std::fmt::Display for CallRequestMaterializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Reference(e) => e.fmt(f),
            Self::Invalid(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for CallRequestMaterializationError {}

/// Original source and optional result constraint stay borrowed. Owned argument
/// headers retain admitted constants without reconstructing their backing.
/// Type clones, Vec-to-Box shrink and allocator internals remain caller-admitted
/// opaque operations; checked Layout extents do not grant memory.
#[derive(Debug)]
pub struct MaterializedCallRequest<'a> {
    source: &'a PhysicalCallRequest,
    arguments: Box<[FunctionArgument]>,
    // Drop the consumed argument backing before its finite construction hold.
    _storage: ValueTypeStorageLifetime,
}
impl MaterializedCallRequest<'_> {
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.source.logical_argument_count,
            expected_result_type: self.source.expected_result_type.as_ref(),
        }
    }
    pub fn constant_policy(&self) -> ConstantPolicy {
        self.source.constant_policy
    }
}

/// Original source/control errors and the real constructor refusal stay distinct.
#[derive(Debug)]
pub enum CallRequestMaterializationFailure<H> {
    Control(CompileControlError),
    Creator(H),
    Owner(CallRequestMaterializationError),
}
impl<H: std::fmt::Display> std::fmt::Display for CallRequestMaterializationFailure<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Creator(cause) => cause.fmt(f),
            Self::Owner(cause) => cause.fmt(f),
        }
    }
}
impl<H: std::error::Error + 'static> std::error::Error for CallRequestMaterializationFailure<H> {}
impl<H> From<CompileControlError> for CallRequestMaterializationFailure<H> {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl<H> From<CallRequestMaterializationError> for CallRequestMaterializationFailure<H> {
    fn from(cause: CallRequestMaterializationError) -> Self {
        match cause {
            CallRequestMaterializationError::Control(cause) => Self::Control(cause),
            other => Self::Owner(other),
        }
    }
}
impl<H> From<ConstantReferenceError> for CallRequestMaterializationFailure<H> {
    fn from(cause: ConstantReferenceError) -> Self {
        CallRequestMaterializationError::from(cause).into()
    }
}

/// Same source and constructor body with a real temporary operation owner.
/// The returned request holds only its argument headers and existing source loans.
pub fn materialize_call_request_with_creator_in<'a, H>(
    source: &'a PhysicalCallRequest,
    pools: &ConstantPools,
    creation: &mut dyn ValueSchemaCreationScope<H>,
    work: &mut CompileCheckpoints<'_>,
    max_arguments: usize,
) -> Result<MaterializedCallRequest<'a>, CallRequestMaterializationFailure<H>> {
    materialize_core(
        source,
        pools,
        |_| Ok(Some(BorrowedCallScope(creation))),
        work,
        max_arguments,
    )
}

/// Open the actual temporary scope only at the original argument-allocation
/// slot, after source header checks and their original pending-control flush.
pub fn materialize_call_request_with_factory_in<'a, H, F: ValueSchemaCreationFactory<H>>(
    source: &'a PhysicalCallRequest,
    pools: &ConstantPools,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
    max_arguments: usize,
) -> Result<MaterializedCallRequest<'a>, CallRequestMaterializationFailure<H>> {
    materialize_core(
        source,
        pools,
        |count| {
            if count == 0 {
                Ok(None)
            } else {
                factory.begin_scope().map(Some)
            }
        },
        work,
        max_arguments,
    )
}

// This stack adapter reborrows existing ports; it opens no scope or grant.
struct BorrowedCallScope<'a, H>(&'a mut dyn ValueSchemaCreationScope<H>);
impl<H> ValueSchemaCreationScope<H> for BorrowedCallScope<'_, H> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0.storage_lifetime()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), H> {
        self.0.request_layout(layout)
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), H> {
        self.0.observe_opaque_upper_bound(bytes)
    }
}

pub fn materialize_call_request_observed<'a>(
    source: &'a PhysicalCallRequest,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
    max_arguments: usize,
) -> Result<MaterializedCallRequest<'a>, CallRequestMaterializationError> {
    materialize_core::<std::convert::Infallible, BorrowedCallScope<'_, std::convert::Infallible>>(
        source,
        pools,
        |_| Ok(None),
        work,
        max_arguments,
    )
    .map_err(|cause| match cause {
        CallRequestMaterializationFailure::Control(cause) => {
            CallRequestMaterializationError::Control(cause)
        }
        CallRequestMaterializationFailure::Owner(cause) => cause,
        CallRequestMaterializationFailure::Creator(never) => match never {},
    })
}
fn materialize_core<'a, H, S: ValueSchemaCreationScope<H>>(
    source: &'a PhysicalCallRequest,
    pools: &ConstantPools,
    begin: impl FnOnce(usize) -> Result<Option<S>, H>,
    work: &mut CompileCheckpoints<'_>,
    max_arguments: usize,
) -> Result<MaterializedCallRequest<'a>, CallRequestMaterializationFailure<H>> {
    let count = source.arguments.len();
    if count > max_arguments {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    Layout::array::<FunctionArgument>(count).map_err(|_| CompileControlError::ResourceExhausted)?;
    let invalid_count = source.logical_argument_count > count;
    work.step()?;
    if invalid_count {
        return Err(CallRequestMaterializationError::Invalid(
            "original request logical argument count exceeds channels",
        )
        .into());
    }
    // Admit all owned parameter-header extents before the first output reserve.
    // The original package owns complete type/resource validity; this does not
    // duplicate its recursive grammar or invent a second retained-byte budget.
    for argument in &source.arguments {
        if let StaticFunctionArgument::Lambda {
            parameter_types, ..
        } = argument
        {
            if parameter_types.len() > max_arguments {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            Layout::array::<FunctionValueType>(parameter_types.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
        }
        work.step()?;
    }
    // On every failed construction, drop any argument backing before its
    // actual owned scope. The initializer itself remains at the reserve slot.
    let mut scope = None;
    let mut arguments = Vec::new();
    reserve(&mut arguments, count, begin, &mut scope, work)?;
    let mut creation = scope
        .as_mut()
        .map(|scope| scope as &mut dyn ValueSchemaCreationScope<H>);
    for argument in &source.arguments {
        let argument = match argument {
            StaticFunctionArgument::Value {
                value_type,
                constant,
            } => {
                let constant = match constant {
                    Some(reference) => {
                        work.flush()?;
                        let value = pools.resolve_observed(*reference, value_type, work)?;
                        work.flush()?;
                        Some(value)
                    }
                    None => None,
                };
                FunctionArgument::Value {
                    value_type: clone_type(value_type, work)?,
                    constant,
                }
            }
            StaticFunctionArgument::Lambda {
                parameter_types,
                result_type,
            } => {
                // The original source already owns this complete immutable
                // parameter collection and its finite creator responsibility.
                let parameters = parameter_types.clone_observed(|| work.step())?;
                FunctionArgument::Lambda {
                    parameter_types: parameters,
                    result_type: clone_type(result_type, work)?,
                }
            }
        };
        arguments.push(argument);
        work.step()?;
    }
    let arguments = boxed(arguments, &mut creation, work)?;
    let storage = match creation {
        Some(scope) => scope.storage_lifetime(),
        None => ValueTypeStorageLifetime::pure(),
    };
    Ok(MaterializedCallRequest {
        source,
        arguments,
        _storage: storage,
    })
}

fn clone_type(
    source: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, CallRequestMaterializationError> {
    work.flush()?;
    let copied = source.clone();
    work.step()?;
    work.flush()?;
    Ok(copied)
}
fn reserve<T, H, S: ValueSchemaCreationScope<H>>(
    values: &mut Vec<T>,
    count: usize,
    begin: impl FnOnce(usize) -> Result<Option<S>, H>,
    creation: &mut Option<S>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), CallRequestMaterializationFailure<H>> {
    if count == 0 {
        *creation = begin(count).map_err(CallRequestMaterializationFailure::Creator)?;
        return Ok(());
    }
    let layout = Layout::array::<T>(count).map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    *creation = begin(count).map_err(CallRequestMaterializationFailure::Creator)?;
    if let Some(scope) = creation.as_mut() {
        scope
            .request_layout(layout)
            .map_err(CallRequestMaterializationFailure::Creator)?;
    }
    values
        .try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;
    work.flush()?;
    Ok(())
}
fn boxed<T, H>(
    values: Vec<T>,
    creation: &mut Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[T]>, CallRequestMaterializationFailure<H>> {
    if values.is_empty() {
        return Ok(Box::default());
    }
    work.flush()?;
    if values.capacity() > values.len()
        && let Some(scope) = creation
    {
        let layout =
            Layout::array::<T>(values.len()).map_err(|_| CompileControlError::ResourceExhausted)?;
        scope
            .request_layout(layout)
            .map_err(CallRequestMaterializationFailure::Creator)?;
    }
    let values = values.into_boxed_slice();
    work.step()?;
    work.flush()?;
    Ok(values)
}

impl From<novarocks_constant_contract::ConstantTableError> for CallRequestMaterializationError {
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(ConstantReferenceError::from(error))
    }
}
impl<H> From<novarocks_constant_contract::ConstantTableError>
    for CallRequestMaterializationFailure<H>
{
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(ConstantReferenceError::from(error))
    }
}
