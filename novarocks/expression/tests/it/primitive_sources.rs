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

use novarocks_expression::*;
use novarocks_failure_contract::{FailureProjection, FailureReason, domain};
use novarocks_functions::{CastPrepareError as C, ComparisonPrepareError as B, KernelFailure};
use novarocks_type_contract::{CompileControlError, ValueLogicalType};
use std::{error::Error, num::NonZeroU16};

#[test]
fn primitive_preparation_keeps_bare_kernel_stops_capacity_and_domain_sources() {
    for original in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::Capacity(novarocks_functions::KernelCapacityRefusal::new(
            FailureReason::new(domain::MEMORY, NonZeroU16::new(8).unwrap()),
            16,
            8,
            16,
        )),
        KernelFailure::from(novarocks_type_contract::ValueTypeError::TooDeep),
        KernelFailure::from(
            novarocks_type_contract::ValueTypeError::InvalidLogicalCarrier(ValueLogicalType::Json),
        ),
    ] {
        let expected = original.failure_fact();
        for error in [
            PrimitiveError::from(B::Kernel(original.clone())),
            PrimitiveError::from(C::Kernel(original.clone())),
        ] {
            assert!(!matches!(error, PrimitiveError::Control(_)));
            assert_eq!(error.failure_fact(), expected);
            let kernel = error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<KernelFailure>()
                .unwrap();
            // Kernel Source equality retains the same shared occurrence,
            // not just a replacement with equal diagnostic text.
            assert_eq!(kernel, &original);
        }
    }
}

#[test]
fn primitive_preparation_unwraps_only_actual_caller_control_channels() {
    for original in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::from_domain(novarocks_type_contract::ValueTypeError::TooDeep),
    ] {
        let expected = original.failure_fact();
        for error in [
            PrimitiveError::from(B::Control(original.clone())),
            PrimitiveError::from(C::Control(original.clone())),
            PrimitiveError::from(B::Kernel(KernelFailure::CompileControl(original.clone()))),
            PrimitiveError::from(C::Kernel(KernelFailure::CompileControl(original.clone()))),
        ] {
            assert_eq!(error.failure_fact(), expected);
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&original)
            );
            assert!(matches!(error, PrimitiveError::Control(actual) if actual == original));
        }
    }
}

struct RecipeControl;
impl novarocks_type_contract::PureCompileControl for RecipeControl {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[test]
fn cast_recipe_keeps_illegal_and_type_mismatch_failures() {
    use arrow::datatypes::DataType;
    use novarocks_functions::{CastOperation, FunctionValueType};
    use novarocks_type_contract::TrinoCastMode;

    let date = FunctionValueType::new(DataType::Date32, true);
    let tinyint = FunctionValueType::new(DataType::Int8, true);
    let illegal = prepare_cast_node(
        CastOperation::Carrier,
        &date,
        &tinyint,
        TrinoCastMode::Explicit,
        &RecipeControl,
    )
    .unwrap_err();
    assert!(matches!(illegal, PrimitiveError::Cast(C::Illegal)));

    let required = FunctionValueType::new(DataType::Int64, false);
    let nullable = required.with_nullable(true).unwrap();
    let narrow = prepare_cast_node(
        CastOperation::Carrier,
        &nullable,
        &required,
        TrinoCastMode::Explicit,
        &RecipeControl,
    )
    .unwrap_err();
    assert!(matches!(narrow, PrimitiveError::Cast(C::TypeMismatch)));
}
