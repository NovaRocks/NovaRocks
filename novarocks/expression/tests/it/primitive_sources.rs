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
use novarocks_functions::{
    ArithmeticPrepareError as A, CastPrepareError as C, ComparisonPrepareError as B, KernelFailure,
};
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
            PrimitiveError::from(A::Kernel(original.clone())),
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
            PrimitiveError::from(A::Control(original.clone())),
            PrimitiveError::from(B::Control(original.clone())),
            PrimitiveError::from(C::Control(original.clone())),
            PrimitiveError::from(A::Kernel(KernelFailure::CompileControl(original.clone()))),
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
fn every_unsupported_recipe_keeps_its_exact_owner() {
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::{CastOperation, FunctionValueType};
    use novarocks_type_contract::{ArithmeticOperator, ComparisonOperator, DecimalOverflowPolicy};
    use std::sync::Arc;

    let integer = FunctionValueType::new(DataType::Int64, true);
    let boolean = FunctionValueType::new(DataType::Boolean, false);
    let nested = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    );
    let errors = [
        prepare_arithmetic_node(
            ArithmeticOperator::Add,
            &boolean,
            &boolean,
            &integer,
            DecimalOverflowPolicy::OutputNull,
            true,
            &RecipeControl,
        )
        .unwrap_err(),
        prepare_cast_node(
            CastOperation::Carrier,
            &FunctionValueType::new(DataType::Date32, true),
            &FunctionValueType::new(DataType::Int8, true),
            DecimalOverflowPolicy::ReportError,
            true,
            &RecipeControl,
        )
        .unwrap_err(),
        prepare_comparison_node(ComparisonOperator::Eq, &nested, &nested, &RecipeControl)
            .unwrap_err(),
        prepare_null_safe_comparison_node(&nested, &nested, &RecipeControl).unwrap_err(),
    ];
    assert!(matches!(
        errors[0],
        PrimitiveError::Arithmetic(A::Unsupported)
    ));
    assert!(matches!(errors[1], PrimitiveError::Cast(C::Unsupported)));
    assert!(matches!(
        errors[2],
        PrimitiveError::Comparison(B::Unsupported)
    ));
    assert!(matches!(
        errors[3],
        PrimitiveError::Comparison(B::Unsupported)
    ));
}

#[test]
fn every_recipe_type_mismatch_preserves_original_primitive_error() {
    use arrow::datatypes::DataType;
    use novarocks_functions::{CastOperation, FunctionValueType};
    use novarocks_type_contract::{ArithmeticOperator, ComparisonOperator, DecimalOverflowPolicy};

    let integer = FunctionValueType::new(DataType::Int64, false);
    let nullable = FunctionValueType::new(DataType::Int64, true);
    let boolean = FunctionValueType::new(DataType::Boolean, false);
    let errors = [
        prepare_arithmetic_node(
            ArithmeticOperator::Add,
            &integer,
            &integer,
            &integer,
            DecimalOverflowPolicy::OutputNull,
            true,
            &RecipeControl,
        )
        .unwrap_err(),
        prepare_cast_node(
            CastOperation::Carrier,
            &nullable,
            &integer,
            DecimalOverflowPolicy::ReportError,
            true,
            &RecipeControl,
        )
        .unwrap_err(),
        prepare_comparison_node(ComparisonOperator::Eq, &integer, &boolean, &RecipeControl)
            .unwrap_err(),
        prepare_null_safe_comparison_node(&integer, &boolean, &RecipeControl).unwrap_err(),
    ];
    assert!(matches!(
        errors[0],
        PrimitiveError::Arithmetic(A::TypeMismatch)
    ));
    assert!(matches!(errors[1], PrimitiveError::Cast(C::TypeMismatch)));
    assert!(matches!(
        errors[2],
        PrimitiveError::Comparison(B::TypeMismatch)
    ));
    assert!(matches!(
        errors[3],
        PrimitiveError::Comparison(B::TypeMismatch)
    ));
}
