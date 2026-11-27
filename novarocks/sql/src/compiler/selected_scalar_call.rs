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

//! Selected scalar requests and conversion authors for lowering.
//! No physical definition, optimizer arena, Task, or runtime instance is owned here.

use super::SqlFunctionCatalog;
use crate::binding::SqlFunctionBinding;
use arrow::datatypes::DataType;
use novarocks_functions::{
    FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionBindingSelection,
    FunctionResultType,
};
use novarocks_type_contract::{
    DecimalOverflowPolicy, FunctionArgumentType, FunctionValueType, PureCompileControl,
};
use std::sync::Arc;

pub(crate) fn canonical_scalar_request_observed(
    functions: &dyn SqlFunctionCatalog,
    binding: &SqlFunctionBinding,
    logical_argument_count: usize,
    arguments: &[FunctionArgument],
    constraint: Option<&FunctionValueType>,
    control: &dyn PureCompileControl,
) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError> {
    let original = binding.resolved();
    functions.select_exact_overload_observed(
        &original.function_id,
        original.kind,
        &original.selected.overload,
        FunctionBindingRequest {
            arguments,
            logical_argument_count,
            expected_result_type: constraint,
        },
        control,
    )
}

/// Bare NULL follows the original selected argument's complete declaration.
/// Both consumers create their own admitted constant through the same target.
pub(crate) fn canonical_null_target(
    actual: &FunctionValueType,
    expected: &FunctionArgumentType,
) -> Option<FunctionValueType> {
    if let FunctionArgumentType::Value(expected) = expected
        && *expected.data_type() != DataType::Null
        && *actual.data_type() == DataType::Null
    {
        Some(
            expected
                .clone()
                .with_nullable(true)
                .expect("validated root NULL contract"),
        )
    } else {
        None
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CarrierStep {
    target: FunctionValueType,
    cast_mode: novarocks_type_contract::TrinoCastMode,
}
impl CarrierStep {
    pub(crate) fn cast_mode(&self) -> novarocks_type_contract::TrinoCastMode {
        self.cast_mode
    }
    pub(crate) fn target(&self) -> &FunctionValueType {
        &self.target
    }
}

/// This plan owns conversion decisions before an actual expression occurrence exists.
/// Lowering emits each selected conversion into the scalar unit.
#[derive(Clone, Debug)]
pub(crate) enum PlannedConversion {
    Identity,
    Carrier(CarrierStep),
    ValueConversion {
        binding: SqlFunctionBinding,
        tail: Option<CarrierStep>,
    },
}
pub(crate) fn plan_conversion(
    authority: crate::binding::PlanBindingAuthority,
    functions: &dyn SqlFunctionCatalog,
    source: &FunctionValueType,
    target: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    cast_mode: Option<novarocks_type_contract::TrinoCastMode>,
    control: &dyn PureCompileControl,
) -> Result<PlannedConversion, FunctionBindingError> {
    if let Some(cast_mode) = cast_mode {
        control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
        return Ok(PlannedConversion::Carrier(CarrierStep {
            target: target.clone(),
            cast_mode,
        }));
    }
    if source == target {
        return Ok(PlannedConversion::Identity);
    }
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let intermediate = if novarocks_type_contract::is_null_list_to_json_cast(source, target) {
        None
    } else {
        novarocks_functions::builtin::value_conversion::conversion_intermediate_type(source, target)
            .map_err(|_| {
                FunctionBindingError::InvalidBinding("invalid value conversion domain".into())
            })?
    };
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let step = || CarrierStep {
        cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
        target: target.clone(),
    };
    let Some(intermediate) = intermediate else {
        return Ok(PlannedConversion::Carrier(step()));
    };
    let argument = FunctionArgument::Value {
        value_type: source.clone(),
        constant: None,
    };
    let binding = functions.resolve_value_conversion_binding(&argument, &intermediate, control)?;
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
        return Err(FunctionBindingError::InvalidBinding(
            "value conversion owner selected a relation result".into(),
        ));
    };
    if result != &intermediate
        || binding.selected.argument_types.as_ref() != [FunctionArgumentType::Value(source.clone())]
    {
        return Err(FunctionBindingError::InvalidBinding(
            "value conversion binding differs from its source and explicit target".into(),
        ));
    }
    let tail = (result != target).then(step);
    let binding = SqlFunctionBinding::from_plan_with_constraint(
        authority,
        binding,
        policy,
        crate::binding::SqlResultConstraintOrigin::ValueDomainConversion {
            final_target: target.clone(),
        },
        Some(intermediate),
    );
    Ok(PlannedConversion::ValueConversion { binding, tail })
}

pub(crate) fn comparison_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<FunctionValueType, novarocks_type_contract::ComparisonCoercionFailure> {
    novarocks_type_contract::wider_comparison_value_type(left, right)
}
pub(crate) fn comparison_target(
    common: &FunctionValueType,
    source: &FunctionValueType,
) -> FunctionValueType {
    common
        .clone()
        .with_nullable(source.nullable())
        .expect("validated root NULL contract")
}

#[cfg(test)]
mod tests;
