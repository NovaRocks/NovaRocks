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

use arrow_schema::{DataType, Field};
use novarocks_constant_contract::{
    ConstantPolicy, ConstantPoolId, ConstantPools, ConstantReference, ConstantValue,
};
use novarocks_expression_contract::*;
use novarocks_function_contract::FunctionArgument;
use novarocks_type_contract::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub struct Control {
    pub events: Mutex<Vec<(CompilePhase, u32)>>,
    pub stop: Option<(usize, CompileControlError)>,
}
impl Control {
    pub fn good() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            stop: None,
        }
    }
    pub fn at(index: usize, cause: CompileControlError) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            stop: Some((index, cause)),
        }
    }
    pub fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.events.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let index = events.len();
        events.push((phase, units));
        if let Some((at, cause)) = &self.stop
            && index == *at
        {
            return Err(cause.clone());
        }
        Ok(())
    }
}
pub fn limits() -> UnitLimits {
    UnitLimits {
        expression_semantic_depth: MAX_CONTROL_DEPTH,
    }
}
pub fn boolean(nullable: bool) -> ValueType {
    ValueType::new(DataType::Boolean, nullable)
}
pub fn integer(nullable: bool) -> ValueType {
    ValueType::new(DataType::Int64, nullable)
}
pub fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
pub fn null_source(ty: &ValueType) -> (ConstantPools, ConstantReference) {
    let value = ConstantValue::null(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty.clone(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(4), value.pool().clone())
        .unwrap();
    (
        pools,
        ConstantReference {
            pool: ConstantPoolId::new(4),
            ordinal: value.ordinal(),
        },
    )
}
pub fn i64_source(value: i64) -> (ConstantPools, ConstantReference) {
    let value = ConstantValue::from_i64(
        Arc::new(Field::new("source", DataType::Int64, false)),
        integer(false),
        value,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(4), value.pool().clone())
        .unwrap();
    (
        pools,
        ConstantReference {
            pool: ConstantPoolId::new(4),
            ordinal: value.ordinal(),
        },
    )
}
pub fn function(
    name: &str,
    arguments: Vec<FunctionArgumentType>,
    result: ValueType,
) -> BoundFunction {
    BoundFunction::from_exact_signature(
        FunctionId::try_new(name).unwrap(),
        FunctionOverloadId::try_new("unit/overload").unwrap(),
        FunctionKind::Scalar,
        TypedFactSlice::from_pure_boxed(arguments.into_boxed_slice()),
        result,
    )
}
pub fn request(function: &BoundFunction) -> PhysicalCallRequest {
    let arguments = function
        .argument_types
        .iter()
        .map(|ty| match ty {
            FunctionArgumentType::Value(ty) => FunctionArgument::Value {
                value_type: ty.clone(),
                constant: None,
            },
            FunctionArgumentType::Lambda {
                parameter_types,
                result_type,
            } => FunctionArgument::Lambda {
                parameter_types: parameter_types.clone(),
                result_type: result_type.clone(),
            },
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    PhysicalCallRequest {
        logical_argument_count: arguments.len(),
        arguments,
        expected_result_type: None,
        constant_policy: policy(),
    }
}
pub fn claim(context: ExpressionEffectContext, control: ArgumentControl) -> ExpressionCallClaim {
    ExpressionCallClaim {
        context,
        effects: CallEffects {
            value_stability: FunctionVolatility::Immutable,
            own_row_error: FunctionIntrinsicRowError::NoRowError,
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: if matches!(
                control,
                ArgumentControl::If
                    | ArgumentControl::Coalesce
                    | ArgumentControl::SimpleCase
                    | ArgumentControl::SearchedCase
                    | ArgumentControl::TemporalSource(_)
            ) {
                FunctionNullBehavior::ControlDefined
            } else {
                FunctionNullBehavior::CalledOnNull
            },
            argument_control: control,
            instance_state: FunctionInstanceState::None,
            observable_effects: ObservableEffects::NONE,
            environment: Box::default(),
            proof_scope: CallProofScope::Region(context.region),
        },
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        temporal_source: None,
    }
}
pub fn no_calls(
    builder: BoundExprSetBuilder,
    ports: Vec<ExprPort>,
    control: &Control,
) -> Result<BoundExprSet, UnitError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        builder
            .seal(ports, &mut work)?
            .finish(BTreeMap::new(), &mut work)
    })();
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    if result.as_ref().err().is_some_and(|e| {
        matches!(
            e.failure_fact().class(),
            FailureClass::Control | FailureClass::Resource | FailureClass::Representation
        )
    }) {
        return result;
    }
    work.finish()?;
    result
}
pub fn with_call_claims(
    sealed: SealedUnit,
    control: ArgumentControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundExprSet, UnitError> {
    let claims = sealed
        .uses()
        .iter()
        .filter(|(_, u)| {
            matches!(
                sealed.definition(u.definition).unwrap().kind,
                ExprKind::FunctionCall { .. }
            )
        })
        .map(|(&id, u)| (id, claim(u.context, control)))
        .collect();
    sealed.finish(claims, work)
}
