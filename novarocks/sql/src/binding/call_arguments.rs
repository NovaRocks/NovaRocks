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

//! Preserve original logical request data without certifying producer origin.

use std::alloc::Layout;

use novarocks_functions::{
    ConstantValidation, FunctionArgument, FunctionBindingError, FunctionBindingRequest,
    FunctionResultType, MAX_CALL_EFFECT_ARGUMENTS,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use super::SqlFunctionBinding;
use crate::analysis::TypedExpr;

#[derive(Debug)]
pub(crate) enum LogicalCallArgumentCaptureError {
    Control(CompileControlError),
    Binding(FunctionBindingError),
    InvalidSource(&'static str),
}
impl From<CompileControlError> for LogicalCallArgumentCaptureError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<FunctionBindingError> for LogicalCallArgumentCaptureError {
    fn from(error: FunctionBindingError) -> Self {
        match error {
            FunctionBindingError::Control(cause) => Self::Control(cause),
            error => Self::Binding(error),
        }
    }
}
impl std::fmt::Display for LogicalCallArgumentCaptureError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(cause) => std::fmt::Display::fmt(cause, formatter),
            Self::Binding(error) => write!(formatter, "logical call argument: {error}"),
            Self::InvalidSource(detail) => formatter.write_str(detail),
        }
    }
}
impl std::error::Error for LogicalCallArgumentCaptureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(cause) => Some(cause),
            Self::Binding(error) => Some(error),
            Self::InvalidSource(_) => None,
        }
    }
}

/// Original selected request data, including the immutable SQL binding loan.
/// This is not certification of a producer, a physical use, late nullability,
/// installed capability, effects, or an allocation grant. Captured literals
/// must be reused by a later emitter rather than admitted a second time.
#[derive(Debug)]
pub(crate) struct CapturedLogicalCallArguments {
    binding: SqlFunctionBinding,
    arguments: Box<[FunctionArgument]>,
    logical_argument_count: usize,
    constant_policy: ConstantValidation,
}
impl CapturedLogicalCallArguments {
    pub(crate) fn binding(&self) -> &SqlFunctionBinding {
        &self.binding
    }
    pub(crate) fn constant_policy(&self) -> ConstantValidation {
        self.constant_policy
    }
    pub(crate) fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_argument_count,
            expected_result_type: match &self.binding.resolved().selected.result_type {
                FunctionResultType::Scalar(result) => Some(result),
                FunctionResultType::Relation(_) => None,
            },
        }
    }
}

/// Capture actual already-typed logical channels in their original order,
/// followed by any function ORDER BY channels supplied by the caller. The sole
/// analysis argument author owns literals, CV type checks and Lambda shape;
/// this owner does not infer constants through Cast or Nested expressions.
/// The selected count bounds reservation before traversal, and an overlong
/// iterator refuses before authoring or pushing its unadmitted extra item.
/// Caller admission covers iterator/source/type clones, factory scratch and
/// retained coexistence. There is no independent resource wallet here.
pub(crate) fn capture_logical_call_arguments<'a>(
    binding: &SqlFunctionBinding,
    logical_argument_count: usize,
    ordered: impl IntoIterator<Item = &'a TypedExpr>,
    constant_policy: ConstantValidation,
    control: &dyn PureCompileControl,
) -> Result<CapturedLogicalCallArguments, LogicalCallArgumentCaptureError> {
    capture_call_arguments_with(
        binding,
        logical_argument_count,
        ordered,
        constant_policy,
        control,
        |expression, control| {
            crate::analysis::function_argument(expression, constant_policy, control)
                .map_err(LogicalCallArgumentCaptureError::from)
        },
    )
}

/// Move the actual request already authored by a synthetic SQL producer.
/// This does not infer constant presence or certify provenance. In particular,
/// a conversion resolver's original None remains None for a physical Constant.
/// Caller admission includes both the supplied and collected owned buffers.
pub(crate) fn move_authored_call_arguments_observed(
    binding: &SqlFunctionBinding,
    logical_argument_count: usize,
    arguments: Vec<FunctionArgument>,
    constant_policy: ConstantValidation,
    control: &dyn PureCompileControl,
) -> Result<CapturedLogicalCallArguments, LogicalCallArgumentCaptureError> {
    capture_call_arguments_with(
        binding,
        logical_argument_count,
        arguments,
        constant_policy,
        control,
        |argument, _| Ok(argument),
    )
}

fn capture_call_arguments_with<I: IntoIterator>(
    binding: &SqlFunctionBinding,
    logical_argument_count: usize,
    ordered: I,
    constant_policy: ConstantValidation,
    control: &dyn PureCompileControl,
    mut author: impl FnMut(
        I::Item,
        &dyn PureCompileControl,
    ) -> Result<FunctionArgument, LogicalCallArgumentCaptureError>,
) -> Result<CapturedLogicalCallArguments, LogicalCallArgumentCaptureError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        let selected_count = binding.resolved().selected.argument_types.len();
        if selected_count > MAX_CALL_EFFECT_ARGUMENTS {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        let exact_logical = binding.resolved().logical_argument_count == logical_argument_count
            && logical_argument_count <= selected_count;
        work.step()?;
        if !exact_logical {
            return Err(LogicalCallArgumentCaptureError::InvalidSource(
                "logical call count differs from its original selected binding",
            ));
        }
        Layout::array::<FunctionArgument>(selected_count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.flush()?;
        let mut arguments = Vec::new();
        arguments
            .try_reserve_exact(selected_count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.step()?;
        work.flush()?;
        for expression in ordered {
            let within = arguments.len() < selected_count;
            work.step()?;
            if !within {
                return Err(LogicalCallArgumentCaptureError::InvalidSource(
                    "logical call iterator has more channels than its selected binding",
                ));
            }
            work.flush()?;
            let argument = author(expression, work.control())?;
            work.step()?;
            arguments.push(argument);
        }
        let exact_count = arguments.len() == selected_count;
        work.step()?;
        if !exact_count {
            return Err(LogicalCallArgumentCaptureError::InvalidSource(
                "logical call iterator has fewer channels than its selected binding",
            ));
        }
        work.flush()?;
        let arguments = arguments.into_boxed_slice();
        work.step()?;
        work.flush()?;
        let binding = binding.clone();
        work.step()?;
        Ok(CapturedLogicalCallArguments {
            binding,
            arguments,
            logical_argument_count,
            constant_policy,
        })
    })();
    work.finish_result(result, LogicalCallArgumentCaptureError::from)
}

#[cfg(test)]
#[path = "call_arguments_tests.rs"]
mod tests;
