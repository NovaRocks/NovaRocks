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

use arrow_schema::DataType;
use novarocks_functions::*;
use novarocks_type_contract::FunctionValueType;
use novarocks_type_contract::NativeBetweenPlan;
use novarocks_type_contract::PureCompileControl;
use novarocks_type_contract::ValueLogicalType;
use novarocks_type_contract::{CompileControlError, CompilePhase};
use std::sync::Mutex;
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: usize,
    cause: CompileControlError,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        assert!(
            trace.len() < self.stop,
            "no callback after first compile refusal"
        );
        trace.push(n);
        if trace.len() == self.stop {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
fn control(stop: usize, cause: CompileControlError) -> Control {
    Control {
        trace: Mutex::new(vec![]),
        stop,
        cause,
    }
}
#[test]
fn between_recipe_original_expansion_and_exact_flat_full_types() {
    for negated in [false, true] {
        let plan = NativeBetweenPlan::new(negated);
        assert_eq!(
            plan.sources(),
            [
                novarocks_type_contract::BetweenSourceRole::Operand,
                novarocks_type_contract::BetweenSourceRole::Lower,
                novarocks_type_contract::BetweenSourceRole::Operand,
                novarocks_type_contract::BetweenSourceRole::Upper
            ]
        );
        for dtype in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::Decimal128(9, 3),
            DataType::Date32,
        ] {
            for nullable in [false, true] {
                let ty =
                    FunctionValueType::from_legacy_declared_projection(dtype.clone(), nullable);
                let out = FunctionValueType::new(DataType::Boolean, nullable);
                let recipe = PreparedNativeBetweenRecipe::try_new(
                    plan,
                    &ty,
                    &ty,
                    &ty,
                    &out,
                    &control(usize::MAX, CompileControlError::Cancelled),
                )
                .unwrap();
                assert_eq!(recipe.lower().operator(), plan.lower());
                assert_eq!(recipe.upper().operator(), plan.upper());
                assert_eq!(recipe.lower().left_type(), &ty);
                assert_eq!(recipe.upper().right_type(), &ty);
            }
        }
        for logical in [ValueLogicalType::LargeInt, ValueLogicalType::Uuid] {
            let mut ty = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
            ty = FunctionValueType::try_with_logical_type(
                ty.data_type().clone(),
                ty.nullable(),
                logical,
            )
            .expect("valid declared function domain");
            PreparedNativeBetweenRecipe::try_new(
                plan,
                &ty,
                &ty,
                &ty,
                &FunctionValueType::new(DataType::Boolean, true),
                &control(usize::MAX, CompileControlError::Cancelled),
            )
            .unwrap();
        }
    }
}
#[test]
fn between_recipe_refuses_inexact_domain_or_successful_null_loss() {
    let plan = NativeBetweenPlan::new(false);
    let ty = FunctionValueType::new(DataType::Int64, true);
    let c = control(usize::MAX, CompileControlError::Cancelled);
    assert!(matches!(
        PreparedNativeBetweenRecipe::try_new(
            plan,
            &ty,
            &ty,
            &ty,
            &FunctionValueType::new(DataType::Boolean, false),
            &c
        ),
        Err(ComparisonPrepareError::TypeMismatch)
    ));
    let other = FunctionValueType::new(DataType::Int32, true);
    assert!(matches!(
        PreparedNativeBetweenRecipe::try_new(
            plan,
            &ty,
            &other,
            &ty,
            &FunctionValueType::new(DataType::Boolean, true),
            &c
        ),
        Err(ComparisonPrepareError::TypeMismatch)
    ));
    let nested = FunctionValueType::from_legacy_declared_projection(
        DataType::List(std::sync::Arc::new(arrow_schema::Field::new(
            "actual",
            DataType::Int64,
            true,
        ))),
        true,
    );
    assert!(matches!(
        PreparedNativeBetweenRecipe::try_new(
            plan,
            &nested,
            &nested,
            &nested,
            &FunctionValueType::new(DataType::Boolean, true),
            &c
        ),
        Err(ComparisonPrepareError::Unsupported)
    ));
    let null = FunctionValueType::new(DataType::Null, false);
    assert!(matches!(
        PreparedNativeBetweenRecipe::try_new(
            plan,
            &null,
            &null,
            &null,
            &FunctionValueType::new(DataType::Boolean, false),
            &c
        ),
        Err(ComparisonPrepareError::TypeMismatch)
    ));
}
#[test]
fn between_recipe_every_compile_callback_preserves_first_three_causes_without_footer() {
    let ty = FunctionValueType::new(DataType::Decimal128(9, 3), true);
    let result = FunctionValueType::new(DataType::Boolean, true);
    let recording = control(usize::MAX, CompileControlError::Cancelled);
    PreparedNativeBetweenRecipe::try_new(
        NativeBetweenPlan::new(false),
        &ty,
        &ty,
        &ty,
        &result,
        &recording,
    )
    .unwrap();
    let trace = recording.trace.lock().unwrap().clone();
    for stop in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = control(stop, cause.clone());
            let error = PreparedNativeBetweenRecipe::try_new(
                NativeBetweenPlan::new(false),
                &ty,
                &ty,
                &ty,
                &result,
                &c,
            )
            .unwrap_err();
            assert_eq!(error.control_error(), Some(cause));
            assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
        }
    }
}

#[test]
fn between_recipe_nominal_four_use_guards_and_value_demand_are_checked() {
    use novarocks_type_contract::{EvaluationDemand, GuardKind, control_argument_semantics};
    for negated in [false, true] {
        let shape = novarocks_type_contract::ControlShape::Between { negated };
        assert!(control_argument_semantics(shape, 3, 0, EvaluationDemand::Value).is_err());
        assert!(control_argument_semantics(shape, 4, 4, EvaluationDemand::Value).is_err());
        for ordinal in 0..4 {
            let (demand, guard) =
                control_argument_semantics(shape, 4, ordinal, EvaluationDemand::TruthOnly).unwrap();
            assert_eq!(demand, EvaluationDemand::Value);
            assert_eq!(
                guard,
                if ordinal == 0 {
                    None
                } else {
                    Some(GuardKind::BetweenAfterSource {
                        ordinal: (ordinal - 1) as u32,
                    })
                }
            );
        }
        assert!(!novarocks_type_contract::ArgumentControl::Eager.matches_scalar_shape(shape));
    }
}
