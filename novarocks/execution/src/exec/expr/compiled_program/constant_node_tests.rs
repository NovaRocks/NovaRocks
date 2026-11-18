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

use super::constant_node::*;
use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::{
    ConstantPolicy, ConstantValue, FunctionValueType, KernelEvaluationControl,
};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::{
    ArithmeticOperator, ComparisonOperator, CompileControlError, CompilePhase,
    DecimalOverflowPolicy, PureCompileControl,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
fn constant(array: ArrayRef) -> ConstantValue {
    let ty = FunctionValueType::new(array.data_type().clone(), array.null_count() != 0);
    ConstantValue::from_scalar_array(
        Arc::new(ty.try_to_field("literal").unwrap()),
        ty,
        array.to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &Control,
    )
    .unwrap()
}
fn bool_value(value: Option<bool>) -> ConstantValue {
    constant(Arc::new(BooleanArray::from(vec![value])))
}
fn integer(value: i64) -> ConstantValue {
    constant(Arc::new(Int64Array::from(vec![value])))
}
fn array(outcome: ConstantNodeOutcome) -> ArrayRef {
    match outcome {
        ConstantNodeOutcome::Value(value) => value,
        other => panic!("expected a value: {other:?}"),
    }
}
#[test]
fn value_boolean_nodes_preserve_all_nine_three_valued_pairs() {
    let result = FunctionValueType::new(DataType::Boolean, true);
    for (left, right, and, or) in [
        (Some(false), Some(false), Some(false), Some(false)),
        (Some(false), Some(true), Some(false), Some(true)),
        (Some(false), None, Some(false), None),
        (Some(true), Some(false), Some(false), Some(true)),
        (Some(true), Some(true), Some(true), Some(true)),
        (Some(true), None, None, Some(true)),
        (None, Some(false), Some(false), None),
        (None, Some(true), None, Some(true)),
        (None, None, None, None),
    ] {
        for (node, expected) in [
            (ConstantNode::Conjunction, and),
            (ConstantNode::Disjunction, or),
        ] {
            let output = array(
                evaluate_constant_node(
                    node,
                    &[bool_value(left), bool_value(right)],
                    &result,
                    &Control,
                    CompilePhase::FunctionSpecialization,
                )
                .unwrap(),
            );
            let output = output.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(
                if output.is_null(0) {
                    None
                } else {
                    Some(output.value(0))
                },
                expected
            );
        }
    }
    for (value, expected) in [
        (Some(true), Some(false)),
        (Some(false), Some(true)),
        (None, None),
    ] {
        let output = array(
            evaluate_constant_node(
                ConstantNode::Not,
                &[bool_value(value)],
                &result,
                &Control,
                CompilePhase::FunctionSpecialization,
            )
            .unwrap(),
        );
        let output = output.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            if output.is_null(0) {
                None
            } else {
                Some(output.value(0))
            },
            expected
        );
    }
}
#[test]
fn arithmetic_and_both_comparison_conversion_steps_share_selected_completion() {
    let int = FunctionValueType::new(DataType::Int64, true);
    let output = array(
        evaluate_constant_node(
            ConstantNode::Arithmetic {
                operator: ArithmeticOperator::Add,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: true,
            },
            &[integer(20), integer(22)],
            &int,
            &Control,
            CompilePhase::FunctionSpecialization,
        )
        .unwrap(),
    );
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    let float = FunctionValueType::new(DataType::Float64, false);
    let step = ConstantCarrierStep {
        target: &float,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        allow_throw_exception: true,
    };
    let output = array(
        evaluate_constant_node(
            ConstantNode::Comparison {
                operator: ComparisonOperator::Lt,
                left: ConstantConversion::Carrier(step),
                right: ConstantConversion::Carrier(step),
            },
            &[integer(20), integer(22)],
            &FunctionValueType::new(DataType::Boolean, false),
            &Control,
            CompilePhase::FunctionSpecialization,
        )
        .unwrap(),
    );
    assert!(
        output
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    let output = array(
        evaluate_constant_node(
            ConstantNode::Conversion(ConstantConversion::Carrier(step)),
            &[integer(42)],
            &float,
            &Control,
            CompilePhase::FunctionSpecialization,
        )
        .unwrap(),
    );
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        42.0
    );
}
#[test]
fn null_safe_equality_never_turns_successful_null_into_a_row_error() {
    let result = FunctionValueType::new(DataType::Boolean, false);
    for (left, right, expected) in [
        (None, None, true),
        (Some(true), None, false),
        (None, Some(false), false),
        (Some(true), Some(true), true),
    ] {
        let output = array(
            evaluate_constant_node(
                ConstantNode::NullSafeEquality {
                    left: ConstantConversion::Identity,
                    right: ConstantConversion::Identity,
                },
                &[bool_value(left), bool_value(right)],
                &result,
                &Control,
                CompilePhase::FunctionSpecialization,
            )
            .unwrap(),
        );
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0),
            expected
        );
    }
}
#[test]
fn cast_overflow_is_a_row_error_and_capability_absence_declines() {
    // Decimal-to-signed overflow is a row error under ReportError. Invalid
    // integral text retains the existing successful SQL NULL behavior.
    let int = FunctionValueType::new(DataType::Int8, true);
    let source = constant(Arc::new(
        arrow::array::Decimal128Array::from(vec![1000])
            .with_precision_and_scale(4, 0)
            .unwrap(),
    ));
    let outcome = evaluate_constant_node(
        ConstantNode::Conversion(ConstantConversion::Carrier(ConstantCarrierStep {
            target: &int,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            allow_throw_exception: true,
        })),
        &[source],
        &int,
        &Control,
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    assert!(matches!(outcome, ConstantNodeOutcome::RowError(_)));
    let source = constant(Arc::new(arrow::array::Date32Array::from(vec![1])));
    let target = FunctionValueType::new(DataType::Int8, true);
    let outcome = evaluate_constant_node(
        ConstantNode::Conversion(ConstantConversion::Carrier(ConstantCarrierStep {
            target: &target,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            allow_throw_exception: true,
        })),
        &[source],
        &target,
        &Control,
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    assert!(matches!(
        outcome,
        ConstantNodeOutcome::Declined(DeclineReason::CapabilityMissing)
    ));
}
#[test]
fn every_entry_control_refusal_is_primary_and_receives_no_completion_callback() {
    struct Stop {
        calls: AtomicUsize,
        stop: usize,
        cause: CompileControlError,
    }
    impl PureCompileControl for Stop {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            let current = self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                current <= self.stop,
                "entry called control after its originating stop"
            );
            if current == self.stop {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
    }
    let values = [integer(20), integer(22)];
    let result = FunctionValueType::new(DataType::Int64, true);
    let node = || ConstantNode::Arithmetic {
        operator: ArithmeticOperator::Add,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        allow_throw_exception: true,
    };
    let baseline = Stop {
        calls: AtomicUsize::new(0),
        stop: usize::MAX,
        cause: CompileControlError::Cancelled,
    };
    evaluate_constant_node(
        node(),
        &values,
        &result,
        &baseline,
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    for stop in 0..baseline.calls.load(Ordering::SeqCst) {
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
            let control = Stop {
                calls: AtomicUsize::new(0),
                stop,
                cause: cause.clone(),
            };
            assert!(
                matches!(evaluate_constant_node(node(),&values,&result,&control,CompilePhase::FunctionSpecialization),Err(ConstantNodeFailure::Control(actual)) if actual==cause)
            );
            assert_eq!(control.calls.load(Ordering::SeqCst), stop + 1);
        }
    }
}
#[test]
fn compile_control_adapter_never_allows_a_wait() {
    let control = novarocks_functions::CompileKernelControl::new(
        &Control,
        CompilePhase::FunctionSpecialization,
    );
    assert!(matches!(
        control.wait(std::time::Duration::ZERO),
        Err(novarocks_functions::KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn numeric_not_preserves_zero_nonzero_nan_and_null_in_the_original_successful_domain() {
    let inputs: Vec<ArrayRef> = vec![
        Arc::new(arrow::array::Int8Array::from(vec![Some(0), Some(-1), None])),
        Arc::new(arrow::array::Int16Array::from(vec![
            Some(0),
            Some(-1),
            None,
        ])),
        Arc::new(arrow::array::Int32Array::from(vec![
            Some(0),
            Some(-1),
            None,
        ])),
        Arc::new(Int64Array::from(vec![Some(0), Some(-1), None])),
        Arc::new(arrow::array::Float32Array::from(vec![
            Some(-0.0),
            Some(f32::NAN),
            None,
        ])),
        Arc::new(Float64Array::from(vec![Some(-0.0), Some(f64::NAN), None])),
        Arc::new(
            arrow::array::Decimal128Array::from(vec![Some(0), Some(-1), None])
                .with_precision_and_scale(18, 4)
                .unwrap(),
        ),
    ];
    let result = FunctionValueType::new(DataType::Boolean, true);
    // SQL NOT is true exactly at numeric zero, false at every nonzero value
    // (including NaN), and preserves SQL NULL; these are independent expectations.
    for input in inputs {
        for (row, expected) in [Some(true), Some(false), None].into_iter().enumerate() {
            let value = constant(input.slice(row, 1));
            let output = array(
                evaluate_constant_node(
                    ConstantNode::Not,
                    &[value],
                    &result,
                    &Control,
                    CompilePhase::FunctionSpecialization,
                )
                .unwrap(),
            );
            let output = output.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(
                if output.is_null(0) {
                    None
                } else {
                    Some(output.value(0))
                },
                expected
            );
        }
    }
}

#[test]
fn numeric_not_actual_sql_program_and_constant_entry_keep_the_same_sparse_values() {
    use super::super::numeric_unary_owned_transaction_tests::{batch, programs_with_catalogue};
    use super::super::sql_test_source::sql_source;
    use super::CompiledExpressionInstance;
    let functions =
        super::super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
    let inputs: Vec<ArrayRef> = vec![
        Arc::new(arrow::array::Int8Array::from(vec![0, 1, -1, 0])),
        Arc::new(arrow::array::Int16Array::from(vec![0, 1, -1, 0])),
        Arc::new(arrow::array::Int32Array::from(vec![0, 1, -1, 0])),
        Arc::new(Int64Array::from(vec![0, 1, -1, 0])),
        Arc::new(arrow::array::Float32Array::from(vec![
            0.0,
            1.0,
            f32::NAN,
            -0.0,
        ])),
        Arc::new(Float64Array::from(vec![0.0, 1.0, f64::NAN, -0.0])),
        Arc::new(
            arrow::array::Decimal128Array::from(vec![0, 1, -1, 0])
                .with_precision_and_scale(18, 4)
                .unwrap(),
        ),
    ];
    for input in inputs {
        let source = sql_source(
            "SELECT NOT k AS inverted FROM fixture.source",
            input.data_type().clone(),
        );
        let programs = programs_with_catalogue(&source, &functions);
        let mut found = None;
        for program in programs.values() {
            for (unit_index, unit) in program.units().iter().enumerate() {
                for (ordinal, root) in unit.roots().iter().enumerate() {
                    if matches!(
                        unit.definitions().node(root.definition).unwrap().kind(),
                        novarocks_expression::StaticExprKind::Not(_)
                    ) {
                        assert!(found.is_none());
                        let reference = novarocks_local_program::ProgramRootRef {
                            unit: novarocks_local_program::ProgramUnitId::new(unit_index),
                            root: novarocks_expression_contract::RootOrdinal::new(
                                u32::try_from(ordinal).unwrap(),
                            ),
                        };
                        found = Some((program.clone(), reference));
                    }
                }
            }
        }
        let (program, root) = found.expect("published numeric NOT root");
        let data = batch(&program, root, input.clone());
        let mut frame = CompiledExpressionInstance::try_new(
            program.unit(root.unit).unwrap().clone(),
            root.root,
            &Control,
        )
        .unwrap();
        let output = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                ScalarInvocationScope::new(
                    novarocks_functions::Selection::all(4),
                    novarocks_functions::Selection::all(4),
                ),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output.values().to_data(),
            BooleanArray::from(vec![true, false, false, true]).to_data()
        );
        let rows = [0, 2];
        let sparse = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                {
                    let selection = novarocks_functions::Selection::try_sparse(4, &rows).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(sparse.errors().is_empty());
        assert_eq!(
            sparse.values().to_data(),
            BooleanArray::from(vec![true, false]).to_data()
        );
        for (row, expected) in [true, false, false, true].into_iter().enumerate() {
            let output = array(
                evaluate_constant_node(
                    ConstantNode::Not,
                    &[constant(input.slice(row, 1))],
                    &FunctionValueType::new(DataType::Boolean, false),
                    &Control,
                    CompilePhase::FunctionSpecialization,
                )
                .unwrap(),
            );
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }
}

impl novarocks_functions::KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
        panic!("numeric NOT cannot wait")
    }
}

#[test]
fn identity_root_rejects_a_foreign_carrier_and_a_narrowed_null_promise() {
    let one = integer(1);
    assert!(matches!(
        evaluate_constant_node(
            ConstantNode::Conversion(ConstantConversion::Identity),
            &[one],
            &FunctionValueType::new(DataType::Int32, false),
            &Control,
            CompilePhase::FunctionSpecialization
        ),
        Err(ConstantNodeFailure::InvalidFrozenNode(_))
    ));
    let null = constant(Arc::new(Int64Array::from(vec![None])));
    assert!(matches!(
        evaluate_constant_node(
            ConstantNode::Conversion(ConstantConversion::Identity),
            &[null],
            &FunctionValueType::new(DataType::Int64, false),
            &Control,
            CompilePhase::FunctionSpecialization
        ),
        Err(ConstantNodeFailure::InvalidFrozenNode(_))
    ));
}

#[test]
fn unsupported_arithmetic_and_comparison_families_decline_without_reclassifying_malformed_types() {
    let list = constant(Arc::new(arrow::array::ListArray::from_iter_primitive::<
        arrow::datatypes::Int64Type,
        _,
        _,
    >([Some(vec![Some(1)])])));
    let outcome = evaluate_constant_node(
        ConstantNode::Arithmetic {
            operator: ArithmeticOperator::Add,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            allow_throw_exception: false,
        },
        &[list.clone(), list.clone()],
        &FunctionValueType::new(DataType::Int64, true),
        &Control,
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    assert!(matches!(
        outcome,
        ConstantNodeOutcome::Declined(DeclineReason::CapabilityMissing)
    ));
    for (node, nullable) in [
        (
            ConstantNode::Comparison {
                operator: ComparisonOperator::Eq,
                left: ConstantConversion::Identity,
                right: ConstantConversion::Identity,
            },
            true,
        ),
        (
            ConstantNode::NullSafeEquality {
                left: ConstantConversion::Identity,
                right: ConstantConversion::Identity,
            },
            false,
        ),
    ] {
        assert!(matches!(
            evaluate_constant_node(
                node,
                &[list.clone(), list.clone()],
                &FunctionValueType::new(DataType::Boolean, nullable),
                &Control,
                CompilePhase::FunctionSpecialization
            )
            .unwrap(),
            ConstantNodeOutcome::Declined(DeclineReason::CapabilityMissing)
        ));
    }
    // SQL's real arithmetic author always freezes a nullable result. Forging
    // a nonnullable declaration is malformed input, not missing capability.
    assert!(matches!(
        evaluate_constant_node(
            ConstantNode::Arithmetic {
                operator: ArithmeticOperator::Add,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: false
            },
            &[integer(20), integer(22)],
            &FunctionValueType::new(DataType::Int64, false),
            &Control,
            CompilePhase::FunctionSpecialization,
        ),
        Err(ConstantNodeFailure::Primitive(
            novarocks_expression::PrimitiveError::Arithmetic(
                novarocks_functions::ArithmeticPrepareError::TypeMismatch
            )
        ))
    ));
}
