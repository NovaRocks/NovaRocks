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

//! Capture one authenticated aggregate source before physical state erasure.

use std::alloc::Layout;

use novarocks_functions::{
    ConstantValidation, FunctionArgument, FunctionBindingError, FunctionBindingRequest,
    FunctionResultType, MAX_CALL_EFFECT_ARGUMENTS,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionKind, PureCompileControl,
};

use super::{AggregateArgumentSource, SqlFunctionBinding};
use crate::analysis::{SortItem, TypedExpr};

#[derive(Debug)]
pub(crate) enum AggregateRequestCaptureError {
    Control(CompileControlError),
    Binding(FunctionBindingError),
    MissingLogicalSource,
    InvalidSource(&'static str),
}
impl From<CompileControlError> for AggregateRequestCaptureError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<FunctionBindingError> for AggregateRequestCaptureError {
    fn from(error: FunctionBindingError) -> Self {
        match error {
            FunctionBindingError::Control(cause) => Self::Control(cause),
            error => Self::Binding(error),
        }
    }
}
impl std::fmt::Display for AggregateRequestCaptureError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(cause) => std::fmt::Display::fmt(cause, formatter),
            Self::Binding(error) => write!(formatter, "aggregate source argument: {error}"),
            Self::MissingLogicalSource => {
                formatter.write_str("aggregate logical source is missing")
            }
            Self::InvalidSource(detail) => formatter.write_str(detail),
        }
    }
}
impl std::error::Error for AggregateRequestCaptureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(cause) => Some(cause),
            Self::Binding(error) => Some(error),
            Self::MissingLogicalSource | Self::InvalidSource(_) => None,
        }
    }
}

/// This SQL-owned request preserves admitted constant handles and the original
/// immutable binding/selection loan and policy. It is not a runtime state demand, frozen call proof,
/// neutral wire value, or allocation grant. Actual site and phase association
/// remain with the final lowering journal and full publication owner.
#[derive(Debug)]
pub(crate) struct CapturedAggregateLogicalRequest {
    binding: SqlFunctionBinding,
    identity: super::AggregateLogicalSourceIdentity,
    arguments: Box<[FunctionArgument]>,
    logical_count: usize,
    filter_input: Option<crate::column_id::ColumnId>,
    constant_policy: ConstantValidation,
}
impl CapturedAggregateLogicalRequest {
    pub(crate) fn filter_input(&self) -> Option<crate::column_id::ColumnId> {
        self.filter_input
    }

    pub(crate) fn logical_identity(&self) -> &super::AggregateLogicalSourceIdentity {
        &self.identity
    }
    pub(crate) fn binding(&self) -> &SqlFunctionBinding {
        &self.binding
    }
    pub(crate) fn constant_policy(&self) -> ConstantValidation {
        self.constant_policy
    }
    pub(crate) fn request(&self) -> FunctionBindingRequest<'_> {
        let FunctionResultType::Scalar(result) = &self.binding.resolved().selected.result_type
        else {
            unreachable!("private capture requires an immutable scalar aggregate result")
        };
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_count,
            expected_result_type: Some(result),
        }
    }
}

/// Capture actual logical arguments followed by actual function ORDER channels
/// using the sole typed-expression argument author. Uncertified state slots
/// refuse before argument capture, irrespective of selected type/arity matches.
/// Literals admitted here must be reused when lowering physical constants;
/// existing ConstantValue handles retain their backing and original ordinal.
/// Caller admission covers type/AST clones, opaque factory scratch and retained
/// coexistence; the bounded reserve is not a host grant or formal MEM model.
pub(crate) fn capture_aggregate_logical_request(
    source: &AggregateArgumentSource<TypedExpr, SortItem>,
    policy: ConstantValidation,
    control: &dyn PureCompileControl,
) -> Result<CapturedAggregateLogicalRequest, AggregateRequestCaptureError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        let (arguments, order_by, binding) = source
            .logical_parts()
            .ok_or(AggregateRequestCaptureError::MissingLogicalSource)?;
        let identity = source
            .logical_identity()
            .ok_or(AggregateRequestCaptureError::MissingLogicalSource)?;
        work.step()?;
        let count = arguments
            .len()
            .checked_add(order_by.len())
            .ok_or(CompileControlError::ResourceExhausted)?;
        if count > MAX_CALL_EFFECT_ARGUMENTS
            || binding.resolved().selected.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        let resolved = binding.resolved();
        if resolved.kind != FunctionKind::Aggregate
            || resolved.logical_argument_count != arguments.len()
            || resolved.selected.argument_types.len() != count
            || resolved.selected.aggregate.is_none()
            || !matches!(resolved.selected.result_type, FunctionResultType::Scalar(_))
        {
            return Err(AggregateRequestCaptureError::InvalidSource(
                "logical aggregate source differs from its original selected contract",
            ));
        }
        Layout::array::<FunctionArgument>(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.flush()?;
        let mut captured = Vec::new();
        captured
            .try_reserve_exact(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.flush()?;
        for expression in arguments.iter().chain(order_by.iter().map(|key| &key.expr)) {
            work.flush()?;
            let argument = crate::analysis::function_argument(expression, policy, work.control())?;
            work.step()?;
            captured.push(argument);
        }
        work.flush()?;
        work.flush()?;
        let identity = identity.clone();
        work.step()?;
        work.flush()?;
        Ok(CapturedAggregateLogicalRequest {
            identity,
            binding: binding.clone(),
            arguments: captured.into_boxed_slice(),
            logical_count: arguments.len(),
            filter_input: source.filter_input(),
            constant_policy: policy,
        })
    })();
    if matches!(&result, Err(AggregateRequestCaptureError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
#[path = "aggregate_request_tests.rs"]
mod tests;
