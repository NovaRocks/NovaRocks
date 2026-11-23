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

//! Exact immutable call facts shared by each dedicated control/lifecycle ABI.

use crate::kernel_control::{compile_failure, invalid};
use crate::kernel_input::validate_type_observed;
use crate::{
    CallEffectInput, FunctionArgumentType, FunctionBindingSelection, FunctionId,
    FunctionResultType, KernelFailure, RefinedCallEffects,
};
use novarocks_type_contract::{
    CallEffectContext, CallEffects, CompileCheckpoints, CompilePhase, DecimalOverflowPolicy,
    FunctionKind, PureCompileControl, SemanticParameterProjectionError, SemanticParameters,
};
use std::sync::Arc;

/// One locally verified call's immutable public facts. Constants needed by a
/// specialization live in its prepared implementation, not in a live service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionCallContract {
    function_id: FunctionId,
    kind: FunctionKind,
    logical_argument_count: usize,
    context: CallEffectContext,
    decimal_overflow_policy: DecimalOverflowPolicy,
    selected: Arc<FunctionBindingSelection>,
    effects: CallEffects,
    parameters: SemanticParameters,
    temporal_source: Option<crate::TemporalCallContract>,
}
impl FunctionCallContract {
    pub fn from_refined(
        input: CallEffectInput<'_>,
        receipt: &RefinedCallEffects<'_>,
        selected: Arc<FunctionBindingSelection>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result = (|| {
            receipt
                .validate_input(input)
                .map_err(|_| invalid("call refinement receipt differs from exact input"))?;
            if !std::ptr::eq(input.selected, selected.as_ref()) {
                return Err(invalid(
                    "signature owner differs from the exact input borrow",
                ));
            }
            if !input.context.matches_kind(input.kind)
                || (input.kind == FunctionKind::Aggregate) != input.selected.aggregate.is_some()
            {
                return Err(invalid(
                    "aggregate state contract differs from the exact function kind",
                ));
            }
            match &input.selected.result_type {
                FunctionResultType::Scalar(result) if input.kind != FunctionKind::Table => {
                    validate_type_observed(result, &mut work)?
                }
                FunctionResultType::Relation(results) if input.kind == FunctionKind::Table => {
                    if results.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                        return Err(KernelFailure::ResourceExhausted);
                    }
                    for result in results {
                        validate_type_observed(result, &mut work)?;
                        work.step().map_err(compile_failure)?;
                    }
                }
                _ => return Err(invalid("result shape differs from the exact function kind")),
            }
            if let Some(aggregate) = &input.selected.aggregate {
                validate_type_observed(&aggregate.intermediate_type, &mut work)?;
            }
            for argument in &input.selected.argument_types {
                match argument {
                    FunctionArgumentType::Value(value) => validate_type_observed(value, &mut work)?,
                    FunctionArgumentType::Lambda {
                        parameter_types,
                        result_type,
                    } => {
                        if parameter_types.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                            return Err(KernelFailure::ResourceExhausted);
                        }
                        validate_type_observed(result_type, &mut work)?;
                        for parameter in parameter_types {
                            validate_type_observed(parameter, &mut work)?;
                            work.step().map_err(compile_failure)?;
                        }
                    }
                }
                work.step().map_err(compile_failure)?;
            }
            let parameters = input
                .parameters
                .project_observed(
                    receipt.facts().environment.iter().copied(),
                    CompilePhase::FunctionSpecialization,
                    control,
                )
                .map_err(|error| match error {
                    SemanticParameterProjectionError::Control(error) => compile_failure(error),
                    SemanticParameterProjectionError::Parameter(_) => {
                        invalid("call environment is not frozen")
                    }
                })?;
            let temporal_source = crate::temporal_call::own_temporal_call(input, &mut work)?;
            Ok((parameters, temporal_source))
        })();
        let (parameters, temporal_source) = match result {
            Err(error) if error.is_interruption() => return Err(error),
            result => {
                work.finish().map_err(compile_failure)?;
                result?
            }
        };
        Ok(Self {
            function_id: input.function_id.clone(),
            kind: input.kind,
            logical_argument_count: input.request.logical_argument_count,
            context: input.context,
            decimal_overflow_policy: input.decimal_overflow_policy,
            selected,
            effects: receipt.facts().clone(),
            parameters,
            temporal_source,
        })
    }
    pub fn temporal_source(&self) -> Option<&crate::TemporalCallContract> {
        self.temporal_source.as_ref()
    }
    pub const fn function_id(&self) -> &FunctionId {
        &self.function_id
    }
    pub const fn context(&self) -> CallEffectContext {
        self.context
    }
    pub const fn decimal_overflow_policy(&self) -> DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }
    pub const fn selected_owner(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub fn selected(&self) -> &FunctionBindingSelection {
        &self.selected
    }
    pub const fn effects(&self) -> &CallEffects {
        &self.effects
    }
    pub const fn parameters(&self) -> &SemanticParameters {
        &self.parameters
    }
    pub const fn kind(&self) -> FunctionKind {
        self.kind
    }
    pub const fn logical_argument_count(&self) -> usize {
        self.logical_argument_count
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "call_contract/control_tail_tests.rs"]
mod control_tail_tests;
