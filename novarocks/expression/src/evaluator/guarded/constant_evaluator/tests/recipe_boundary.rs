// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Original typed recipe failures retain their exact entry classification.
// The first two tests use actual wrapper refusals. The last test injects the
// original typed failures solely at the existing classification boundary.
use super::super::*;
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    ArithmeticPrepareError as A, CastPrepareError as C, ComparisonPrepareError as B,
};
use novarocks_type_contract::{
    ArithmeticOperator, ComparisonOperator, CompileControlError, DecimalOverflowPolicy,
};
use std::sync::Arc;
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[test]
fn every_recipe_unsupported_declines_as_capability_missing() {
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
            &Control,
        )
        .unwrap_err(),
        prepare_cast_node(
            CastOperation::Carrier,
            &FunctionValueType::new(DataType::Date32, true),
            &FunctionValueType::new(DataType::Int8, true),
            DecimalOverflowPolicy::ReportError,
            true,
            &Control,
        )
        .unwrap_err(),
        prepare_comparison_node(ComparisonOperator::Eq, &nested, &nested, &Control).unwrap_err(),
        prepare_null_safe_comparison_node(&nested, &nested, &Control).unwrap_err(),
    ];
    assert!(matches!(
        errors[0],
        ProgramPrimitiveError::Arithmetic(A::Unsupported)
    ));
    assert!(matches!(
        errors[1],
        ProgramPrimitiveError::Cast(C::Unsupported)
    ));
    assert!(matches!(
        errors[2],
        ProgramPrimitiveError::Comparison(B::Unsupported)
    ));
    assert!(matches!(
        errors[3],
        ProgramPrimitiveError::Comparison(B::Unsupported)
    ));
    for error in errors {
        assert!(matches!(
            Refusal::from(error),
            Refusal::Declined(DeclineReason::CapabilityMissing)
        ));
    }
}

#[test]
fn every_recipe_type_mismatch_preserves_original_primitive_error() {
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
            &Control,
        )
        .unwrap_err(),
        prepare_cast_node(
            CastOperation::Carrier,
            &nullable,
            &integer,
            DecimalOverflowPolicy::ReportError,
            true,
            &Control,
        )
        .unwrap_err(),
        prepare_comparison_node(ComparisonOperator::Eq, &integer, &boolean, &Control).unwrap_err(),
        prepare_null_safe_comparison_node(&integer, &boolean, &Control).unwrap_err(),
    ];
    assert!(matches!(
        errors[0],
        ProgramPrimitiveError::Arithmetic(A::TypeMismatch)
    ));
    assert!(matches!(
        errors[1],
        ProgramPrimitiveError::Cast(C::TypeMismatch)
    ));
    assert!(matches!(
        errors[2],
        ProgramPrimitiveError::Comparison(B::TypeMismatch)
    ));
    assert!(matches!(
        errors[3],
        ProgramPrimitiveError::Comparison(B::TypeMismatch)
    ));
    for error in errors {
        match Refusal::from(error.clone()) {
            Refusal::Failure(ConstantNodeFailure::Primitive(actual)) => assert_eq!(actual, error),
            _ => panic!("the actual typed primitive error must be preserved"),
        }
    }
    // This tests the precise boundary. It does not invent a public constructor
    // capable of violating ProgramLexicalBindings' private invariants.
    assert!(matches!(
        Refusal::from(ProgramPrimitiveError::Invalid("invalid ordered use")),
        Refusal::Failure(ConstantNodeFailure::Primitive(
            ProgramPrimitiveError::Invalid("invalid ordered use")
        ))
    ));
}

#[test]
fn original_kernel_and_control_failures_keep_their_categories_for_all_recipe_owners() {
    let owners: [fn(KernelFailure) -> ProgramPrimitiveError; 3] = [
        |e| ProgramPrimitiveError::Arithmetic(A::Kernel(e)),
        |e| ProgramPrimitiveError::Cast(C::Kernel(e)),
        |e| ProgramPrimitiveError::Comparison(B::Kernel(e)),
    ];
    for wrap in owners {
        for kernel in super::failure::original_kernel_causes() {
            let Refusal::Failure(actual) = Refusal::from(wrap(kernel.clone())) else {
                panic!("original outer kernel cause must not decline")
            };
            super::failure::assert_original_entry_failure(actual, &kernel);
        }
    }
    let controls: [fn(CompileControlError) -> ProgramPrimitiveError; 4] = [
        ProgramPrimitiveError::Control,
        |e| ProgramPrimitiveError::Arithmetic(A::Control(e)),
        |e| ProgramPrimitiveError::Cast(C::Control(e)),
        |e| ProgramPrimitiveError::Comparison(B::Control(e)),
    ];
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        ),
        CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
        ),
    ] {
        for wrap in controls {
            let Refusal::Failure(actual) = Refusal::from(wrap(cause.clone())) else {
                panic!("original compile-control wrapper must not decline")
            };
            super::failure::assert_original_entry_failure(
                actual,
                &KernelFailure::CompileControl(cause.clone()),
            );
        }
    }
}

fn original_constant(array: arrow::array::ArrayRef) -> ConstantValue {
    let ty = FunctionValueType::new(array.data_type().clone(), array.null_count() != 0);
    ConstantValue::from_scalar_array(
        Arc::new(ty.try_to_field("original child").unwrap()),
        ty,
        array.to_data(),
        crate::testkit::constant_policy(),
        CompilePhase::FunctionSpecialization,
        &Control,
    )
    .unwrap()
}

#[test]
fn public_entry_declines_each_real_unsupported_recipe_family() {
    use arrow::array::{BooleanArray, Date32Array, new_null_array};
    let boolean = || original_constant(Arc::new(BooleanArray::from(vec![true])));
    let nested_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let nested = || original_constant(new_null_array(&nested_type, 1));
    let tiny = FunctionValueType::new(DataType::Int8, true);
    let cases = [
        (
            ConstantNode::Arithmetic {
                operator: ArithmeticOperator::Add,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: true,
            },
            vec![boolean(), boolean()],
            FunctionValueType::new(DataType::Int64, true),
        ),
        (
            ConstantNode::Conversion(ConstantConversion::Carrier(ConstantCarrierStep {
                target: &tiny,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: true,
            })),
            vec![original_constant(Arc::new(Date32Array::from(vec![1])))],
            tiny.clone(),
        ),
        (
            ConstantNode::Comparison {
                operator: ComparisonOperator::Eq,
                left: ConstantConversion::Identity,
                right: ConstantConversion::Identity,
            },
            vec![nested(), nested()],
            FunctionValueType::new(DataType::Boolean, true),
        ),
        (
            ConstantNode::NullSafeEquality {
                left: ConstantConversion::Identity,
                right: ConstantConversion::Identity,
            },
            vec![nested(), nested()],
            FunctionValueType::new(DataType::Boolean, false),
        ),
    ];
    for (node, children, result) in cases {
        assert!(matches!(
            evaluate(
                node,
                &children,
                &result,
                &Control,
                CompilePhase::FunctionSpecialization
            )
            .unwrap(),
            ConstantNodeOutcome::Declined(DeclineReason::CapabilityMissing)
        ));
    }
}

#[test]
fn public_entry_preserves_each_real_type_mismatch_as_original_primitive_error() {
    use arrow::array::{BooleanArray, Int64Array};
    let integer = || original_constant(Arc::new(Int64Array::from(vec![1])));
    let boolean = || original_constant(Arc::new(BooleanArray::from(vec![true])));
    let nonnullable = FunctionValueType::new(DataType::Int64, false);
    let cases = [
        (
            ConstantNode::Arithmetic {
                operator: ArithmeticOperator::Add,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: true,
            },
            vec![integer(), integer()],
            nonnullable.clone(),
        ),
        (
            ConstantNode::Conversion(ConstantConversion::Carrier(ConstantCarrierStep {
                target: &nonnullable,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: true,
            })),
            vec![original_constant(Arc::new(Int64Array::from(vec![None])))],
            nonnullable.clone(),
        ),
        (
            ConstantNode::Comparison {
                operator: ComparisonOperator::Eq,
                left: ConstantConversion::Identity,
                right: ConstantConversion::Identity,
            },
            vec![integer(), boolean()],
            FunctionValueType::new(DataType::Boolean, false),
        ),
        (
            ConstantNode::NullSafeEquality {
                left: ConstantConversion::Identity,
                right: ConstantConversion::Identity,
            },
            vec![integer(), boolean()],
            FunctionValueType::new(DataType::Boolean, false),
        ),
    ];
    let expected = [
        ProgramPrimitiveError::Arithmetic(A::TypeMismatch),
        ProgramPrimitiveError::Cast(C::TypeMismatch),
        ProgramPrimitiveError::Comparison(B::TypeMismatch),
        ProgramPrimitiveError::Comparison(B::TypeMismatch),
    ];
    for ((node, children, result), expected) in cases.into_iter().zip(expected) {
        let error = evaluate(
            node,
            &children,
            &result,
            &Control,
            CompilePhase::FunctionSpecialization,
        )
        .unwrap_err();
        let ConstantNodeFailure::Primitive(actual) = error else {
            panic!("the originating typed primitive must survive: {error:?}")
        };
        assert_eq!(actual, expected);
    }
}
