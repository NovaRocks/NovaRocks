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

use super::*;
use arrow::{
    array::{Array, Float32Array},
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::ConstantPool;
use novarocks_functions::{FunctionArgumentType, FunctionVolatility};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, ValueLogicalType};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after the original refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantValidation {
    crate::constant::test_constant_policy()
}
fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data_type, nullable)
}
fn literal(value: crate::common::expr::LiteralValue, value_type: FunctionValueType) -> TypedExpr {
    TypedExpr {
        kind: crate::analysis::ExprKind::Literal(value),
        value_type,
    }
}
fn column(value_type: FunctionValueType) -> TypedExpr {
    TypedExpr {
        kind: crate::analysis::ExprKind::ColumnRef {
            column_id: crate::column_id::ColumnId(9),
            qualifier: None,
            column: "original".into(),
        },
        value_type,
    }
}
// These are explicitly structural request-data fixtures, not installed owners
// or certificates of actual SQL producer origin.
fn binding(
    args: &[TypedExpr],
    result: FunctionValueType,
    logical_count: usize,
) -> SqlFunctionBinding {
    let original = crate::analysis::test_function_binding(
        "logical_capture",
        args,
        result.data_type().clone(),
        result.nullable(),
        FunctionVolatility::Immutable,
    );
    let mut resolved = original.resolved().clone();
    resolved.logical_argument_count = logical_count;
    resolved.selected.result_type = FunctionResultType::Scalar(result);
    SqlFunctionBinding::new(resolved, DecimalOverflowPolicy::ReportError)
}
fn value(
    argument: &FunctionArgument,
) -> (
    &FunctionValueType,
    Option<&novarocks_functions::ConstantValue>,
) {
    let FunctionArgument::Value {
        value_type,
        constant,
    } = argument
    else {
        panic!("actual value argument")
    };
    (value_type, constant.as_ref())
}
fn prefixes(
    binding: &SqlFunctionBinding,
    logical_count: usize,
    args: &[TypedExpr],
    expected_success: bool,
) {
    let control = Control::default();
    let result = capture_logical_call_arguments(binding, logical_count, args, policy(), &control);
    assert_eq!(result.is_ok(), expected_success);
    let baseline = control.trace();
    assert_eq!(baseline[0], 0);
    assert!(!baseline.is_empty());
    if expected_success {
        assert!(baseline.iter().any(|units| *units > 0));
    }
    for stop in 0..baseline.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(capture_logical_call_arguments(binding, logical_count, args, policy(), &control),
                Err(LogicalCallArgumentCaptureError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), baseline[..=stop]);
        }
    }
}

#[test]
fn logical_capture_identity_cast_and_nested_keep_original_nonconstant_none() {
    use crate::common::expr::LiteralValue;
    let source = literal(LiteralValue::Int(17), ty(DataType::Int64, false));
    let cast = TypedExpr {
        kind: crate::analysis::ExprKind::Cast {
            cast_mode: None,
            expr: novarocks_parser::ast::AstBox::new(source.clone()),
            target: DataType::Int64,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        },
        value_type: source.value_type.clone(),
    };
    let nested = TypedExpr {
        kind: crate::analysis::ExprKind::Nested(novarocks_parser::ast::AstBox::new(source.clone())),
        value_type: source.value_type.clone(),
    };
    let args = [source, cast, nested];
    let selected = binding(&args, ty(DataType::Int64, false), 3);
    let captured =
        capture_logical_call_arguments(&selected, 3, &args, policy(), &Control::default()).unwrap();
    for (index, (input, actual)) in args.iter().zip(captured.request().arguments).enumerate() {
        let direct =
            crate::analysis::function_argument(input, policy(), &Control::default()).unwrap();
        let (expected_ty, expected_constant) = value(&direct);
        let (actual_ty, actual_constant) = value(actual);
        assert_eq!(actual_ty, expected_ty);
        assert_eq!(actual_ty, &input.value_type);
        assert_eq!(actual_constant.is_some(), index == 0);
        assert_eq!(actual_constant.is_some(), expected_constant.is_some());
        if let Some(constant) = actual_constant {
            assert_eq!(constant.try_i64().unwrap(), Some(17));
        }
    }
    // In particular an identity Cast remains an original None even though a
    // later physical lowering may erase that cast and expose its child CV.
}

#[test]
fn logical_capture_typed_null_and_nested_full_type_are_distinct_original_channels() {
    use crate::common::expr::LiteralValue;
    let nested_type = ty(
        DataType::Struct(
            vec![Arc::new(
                Field::new("source_child", DataType::Int64, true)
                    .with_metadata(HashMap::from([("field-id".into(), "19".into())])),
            )]
            .into(),
        ),
        true,
    );
    let null = literal(LiteralValue::Null, nested_type.clone());
    let nested = TypedExpr {
        kind: crate::analysis::ExprKind::Nested(novarocks_parser::ast::AstBox::new(null.clone())),
        value_type: nested_type.clone(),
    };
    let args = [null, nested];
    let selected = binding(&args, ty(DataType::Boolean, true), 2);
    let captured =
        capture_logical_call_arguments(&selected, 2, &args, policy(), &Control::default()).unwrap();
    let (actual_type, cv) = value(&captured.request().arguments[0]);
    assert_eq!(actual_type, &nested_type);
    let cv = cv.unwrap();
    assert_eq!(
        cv.field().data_type().clone(),
        nested_type.data_type().clone()
    );
    assert!(
        cv.is_null_observed(CompilePhase::FunctionSpecialization, &Control::default())
            .unwrap()
    );
    assert!(value(&captured.request().arguments[1]).1.is_none());
}

#[test]
fn logical_capture_cv_keeps_nonzero_ordinal_raw_bits_field_and_backing_without_readmission() {
    let full_type = ty(DataType::Float32, true);
    let field = Arc::new(
        Field::new("original_float", DataType::Float32, true)
            .with_metadata(HashMap::from([("source.field-id".into(), "73".into())])),
    );
    let bits = 0x7f80_0043u32;
    let array = Float32Array::from(vec![
        Some(1.0),
        Some(f32::from_bits(bits)),
        None,
        Some(-0.0),
    ]);
    let pool = ConstantPool::try_new(
        field.clone(),
        full_type.clone(),
        array.to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap();
    let args = [1, 2, 3].map(|ordinal| TypedExpr {
        kind: crate::analysis::ExprKind::Constant(pool.value(ordinal).unwrap()),
        value_type: full_type.clone(),
    });
    let selected = binding(&args, ty(DataType::Boolean, false), 3);
    let zero = novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
            max_rows: 0,
            ..policy()
        },
    );
    let captured =
        capture_logical_call_arguments(&selected, 3, &args, zero, &Control::default()).unwrap();
    assert_eq!(captured.constant_policy(), zero);
    for (actual, ordinal) in captured.request().arguments.iter().zip([1, 2, 3]) {
        let (actual_type, cv) = value(actual);
        let cv = cv.unwrap();
        assert_eq!(actual_type, &full_type);
        assert_eq!(cv.ordinal(), ordinal);
        assert!(Arc::ptr_eq(cv.pool().field_ref(), &field));
        assert!(Arc::ptr_eq(cv.pool().array(), pool.array()));
    }
    assert_eq!(
        value(&captured.request().arguments[0])
            .1
            .unwrap()
            .try_f32_bits()
            .unwrap(),
        Some(bits)
    );
    assert_eq!(
        value(&captured.request().arguments[1])
            .1
            .unwrap()
            .try_f32_bits()
            .unwrap(),
        None
    );
    assert_eq!(
        value(&captured.request().arguments[2])
            .1
            .unwrap()
            .try_f32_bits()
            .unwrap(),
        Some((-0.0f32).to_bits())
    );
    let wrong = TypedExpr {
        value_type: ty(DataType::Float32, false),
        ..args[0].clone()
    };
    let bad = binding(
        std::slice::from_ref(&wrong),
        ty(DataType::Boolean, false),
        1,
    );
    assert!(matches!(
        capture_logical_call_arguments(&bad, 1, [&wrong], policy(), &Control::default()),
        Err(LogicalCallArgumentCaptureError::Binding(
            FunctionBindingError::InvalidBinding(_)
        ))
    ));
}

fn lambda(parameters: usize) -> TypedExpr {
    let parameter_type = ty(
        DataType::Struct(
            vec![Arc::new(
                Field::new("lambda_name", DataType::Utf8, true)
                    .with_metadata(HashMap::from([("lambda.metadata".into(), "kept".into())])),
            )]
            .into(),
        ),
        false,
    );
    let result_type = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    TypedExpr {
        kind: crate::analysis::ExprKind::LambdaFunction {
            params: (0..parameters)
                .map(|ordinal| crate::analysis::LambdaParam {
                    name: format!("p{ordinal}"),
                    slot_id: ordinal as i32,
                    value_type: parameter_type.clone(),
                })
                .collect(),
            body: novarocks_parser::ast::AstBox::new(column(result_type)),
        },
        value_type: ty(DataType::Null, true),
    }
}
#[test]
fn logical_capture_lambda_preserves_complete_parameter_and_result_domains() {
    let args = [lambda(2)];
    let selected = binding(&args, ty(DataType::Boolean, false), 1);
    let captured =
        capture_logical_call_arguments(&selected, 1, &args, policy(), &Control::default()).unwrap();
    let crate::analysis::ExprKind::LambdaFunction { params, body } = &args[0].kind else {
        panic!("lambda source")
    };
    let FunctionArgument::Lambda {
        parameter_types,
        result_type,
    } = &captured.request().arguments[0]
    else {
        panic!("lambda argument")
    };
    assert_eq!(parameter_types.len(), 2);
    for (actual, original) in parameter_types.iter().zip(params) {
        assert_eq!(actual, &original.value_type);
    }
    assert_eq!(result_type, &body.value_type);
    assert_eq!(result_type.logical_type(), ValueLogicalType::Json);
    prefixes(&selected, 1, &args, true);
}

#[test]
fn logical_capture_result_loan_and_order_channels_preserve_original_request_split() {
    let args = [
        column(ty(DataType::Int64, false)),
        column(ty(DataType::Utf8, true)),
    ];
    let result = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let selected = binding(&args, result.clone(), 1);
    let captured =
        capture_logical_call_arguments(&selected, 1, &args, policy(), &Control::default()).unwrap();
    assert!(std::ptr::eq(
        selected.resolved(),
        captured.binding().resolved()
    ));
    let FunctionResultType::Scalar(original) = &selected.resolved().selected.result_type else {
        panic!("scalar result")
    };
    let request = captured.request();
    assert!(std::ptr::eq(
        request.expected_result_type.unwrap(),
        original
    ));
    assert_eq!(request.logical_argument_count, 1);
    assert_eq!(request.arguments.len(), 2);
    assert_eq!(value(&request.arguments[0]).0, &args[0].value_type);
    assert_eq!(value(&request.arguments[1]).0, &args[1].value_type);
    let list = column(ty(
        DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
        true,
    ));
    let direct = crate::analysis::function_argument(&list, policy(), &Control::default()).unwrap();
    let resolved = crate::compiler::SqlFunctionCatalog::resolve_table_binding(
        crate::functions::builtin_sql_function_catalog(),
        "unnest",
        &[direct],
        &Control::default(),
    )
    .unwrap();
    let table = SqlFunctionBinding::new(resolved, DecimalOverflowPolicy::OutputNull);
    let captured =
        capture_logical_call_arguments(&table, 1, [&list], policy(), &Control::default()).unwrap();
    assert!(std::ptr::eq(
        table.resolved(),
        captured.binding().resolved()
    ));
    assert!(matches!(
        captured.binding().resolved().selected.result_type,
        FunctionResultType::Relation(_)
    ));
    assert!(captured.request().expected_result_type.is_none());
}

#[test]
fn logical_capture_small_success_and_ordinary_tails_keep_every_original_control_prefix() {
    use crate::common::expr::LiteralValue;
    let args = [literal(LiteralValue::Int(7), ty(DataType::Int64, false))];
    let selected = binding(&args, ty(DataType::Int64, false), 1);
    prefixes(&selected, 1, &args, true);
    prefixes(&selected, 0, &args, false);
    prefixes(&selected, 1, &[], false);
    let too_many = [args[0].clone(), args[0].clone()];
    prefixes(&selected, 1, &too_many, false);
    let invalid = [literal(LiteralValue::Int(300), ty(DataType::Int8, false))];
    let invalid_binding = binding(&invalid, ty(DataType::Int8, false), 1);
    let control = Control::default();
    assert!(matches!(
        capture_logical_call_arguments(&invalid_binding, 1, &invalid, policy(), &control),
        Err(LogicalCallArgumentCaptureError::Binding(
            FunctionBindingError::Constant(novarocks_constant_contract::ConstantError::Invalid(
                "SQL integer syntax exceeds its declared carrier"
            ))
        ))
    ));
    let trace = control.trace();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let late = Control {
            refusal: Some((trace.len(), cause)),
            ..Control::default()
        };
        assert!(matches!(
            capture_logical_call_arguments(&invalid_binding, 1, &invalid, policy(), &late),
            Err(LogicalCallArgumentCaptureError::Binding(
                FunctionBindingError::Constant(
                    novarocks_constant_contract::ConstantError::Invalid(
                        "SQL integer syntax exceeds its declared carrier"
                    )
                )
            ))
        ));
        assert_eq!(late.trace(), trace);
    }
    prefixes(&invalid_binding, 1, &invalid, false);
    let zero = novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
            max_rows: 0,
            ..policy()
        },
    );
    assert!(matches!(
        capture_logical_call_arguments(&selected, 1, &args, zero, &Control::default()),
        Err(LogicalCallArgumentCaptureError::Binding(
            FunctionBindingError::Constant(novarocks_constant_contract::ConstantError::Limit(
                "constant factory row limit exceeded"
            ))
        ))
    ));
}

#[test]
fn logical_capture_count_admission_precedes_iterator_and_wide_lambda_observes_real_quantum() {
    let args = [column(ty(DataType::Int64, false))];
    let original = binding(&args, ty(DataType::Int64, false), 1);
    let mut resolved = original.resolved().clone();
    resolved.selected.argument_types = vec![
        FunctionArgumentType::Value(args[0].value_type.clone());
        MAX_CALL_EFFECT_ARGUMENTS + 1
    ]
    .into();
    let over = SqlFunctionBinding::new(resolved, DecimalOverflowPolicy::OutputNull);
    let traversed = std::cell::Cell::new(0usize);
    let control = Control::default();
    assert!(matches!(
        capture_logical_call_arguments(
            &over,
            1,
            args.iter().inspect(|_| traversed.set(traversed.get() + 1)),
            policy(),
            &control
        ),
        Err(LogicalCallArgumentCaptureError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(traversed.get(), 0);
    assert_eq!(control.trace(), [0]);
    let args = [lambda(320)];
    let selected = binding(&args, ty(DataType::Boolean, false), 1);
    let control = Control::default();
    let captured = capture_logical_call_arguments(&selected, 1, &args, policy(), &control).unwrap();
    let FunctionArgument::Lambda {
        parameter_types, ..
    } = &captured.request().arguments[0]
    else {
        panic!("lambda")
    };
    assert_eq!(parameter_types.len(), 320);
    let baseline = control.trace();
    let quantum = baseline
        .iter()
        .position(|units| *units == 256)
        .expect("actual original Lambda parameter loop quantum");
    let mut stops = vec![0, quantum, baseline.len() - 1];
    stops.sort();
    stops.dedup();
    for stop in stops {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(capture_logical_call_arguments(&selected,1,&args,policy(),&control),Err(LogicalCallArgumentCaptureError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), baseline[..=stop]);
        }
    }
}

#[test]
fn authored_move_keeps_actual_none_and_original_binding_without_constant_admission() {
    let args = [column(ty(DataType::Int64, false))];
    let selected = binding(&args, ty(DataType::Int64, false), 1);
    let argument = FunctionArgument::Value {
        value_type: args[0].value_type.clone(),
        constant: None,
    };
    let zero = novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
            max_rows: 0,
            ..policy()
        },
    );
    let captured = move_authored_call_arguments_observed(
        &selected,
        1,
        vec![argument],
        zero,
        &Control::default(),
    )
    .unwrap();
    assert!(value(&captured.request().arguments[0]).1.is_none());
    assert!(std::ptr::eq(
        selected.resolved(),
        captured.binding().resolved()
    ));
    assert_eq!(captured.constant_policy(), zero);
}

#[test]
fn authored_move_uses_the_original_bounded_collection_and_control_tails() {
    let args = [column(ty(DataType::Int64, false))];
    let selected = binding(&args, ty(DataType::Int64, false), 1);
    let make = |count| {
        (0..count)
            .map(|_| FunctionArgument::Value {
                value_type: args[0].value_type.clone(),
                constant: None,
            })
            .collect::<Vec<_>>()
    };
    for (logical, count, ordinary_error) in [
        (1, 1, None),
        (
            0,
            1,
            Some("logical call count differs from its original selected binding"),
        ),
        (
            1,
            0,
            Some("logical call iterator has fewer channels than its selected binding"),
        ),
        (
            1,
            2,
            Some("logical call iterator has more channels than its selected binding"),
        ),
    ] {
        let control = Control::default();
        let result = move_authored_call_arguments_observed(
            &selected,
            logical,
            make(count),
            policy(),
            &control,
        );
        match (&result, ordinary_error) {
            (Ok(_), None) => {}
            (Err(LogicalCallArgumentCaptureError::InvalidSource(actual)), Some(expected)) => {
                assert_eq!(*actual, expected);
            }
            _ => panic!("unexpected original authored-move result: {result:?}"),
        }
        let baseline = control.trace();
        assert_eq!(baseline[0], 0);
        if ordinary_error.is_none() {
            assert!(baseline.last().is_some_and(|units| *units > 0));
        }
        for stop in 0..baseline.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(move_authored_call_arguments_observed(&selected,logical,make(count),policy(),&control),Err(LogicalCallArgumentCaptureError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), baseline[..=stop]);
            }
        }
        if let Some(expected) = ordinary_error {
            // An authored ordinary error is already final. A refusal armed
            // only for a later footer must not start a new callback or replace
            // that original error; every real earlier prefix remains above.
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let late = Control {
                    refusal: Some((baseline.len(), cause)),
                    ..Control::default()
                };
                assert!(matches!(
                    move_authored_call_arguments_observed(
                        &selected,
                        logical,
                        make(count),
                        policy(),
                        &late,
                    ),
                    Err(LogicalCallArgumentCaptureError::InvalidSource(actual)) if actual == expected
                ));
                assert_eq!(late.trace(), baseline);
            }
        }
    }
    let wide_args = vec![args[0].clone(); 320];
    let wide = binding(&wide_args, ty(DataType::Int64, false), 320);
    let control = Control::default();
    let captured =
        move_authored_call_arguments_observed(&wide, 320, make(320), policy(), &control).unwrap();
    assert_eq!(captured.request().arguments.len(), 320);
    let trace = control.trace();
    assert!(
        trace.iter().copied().map(u64::from).sum::<u64>() >= 640,
        "actual authored channel checks and moves are observed"
    );
    assert!(
        trace.iter().filter(|units| **units > 0).count() >= 320,
        "opaque-author boundaries permit short checkpoints on every channel"
    );
    let mut over = selected.resolved().clone();
    over.selected.argument_types = vec![
        FunctionArgumentType::Value(args[0].value_type.clone());
        MAX_CALL_EFFECT_ARGUMENTS + 1
    ]
    .into();
    let over = SqlFunctionBinding::new(over, DecimalOverflowPolicy::OutputNull);
    let control = Control::default();
    assert!(matches!(
        move_authored_call_arguments_observed(&over, 1, make(1), policy(), &control),
        Err(LogicalCallArgumentCaptureError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(control.trace(), [0]);
}

#[test]
fn authored_move_preserves_original_cv_ordinal_field_and_backing_under_zero_admission_policy() {
    let full_type = ty(DataType::Float32, true);
    let field = Arc::new(
        Field::new("authored", DataType::Float32, true)
            .with_metadata(HashMap::from([("source.field-id".into(), "81".into())])),
    );
    let bits = 0x7f80_0077u32;
    let pool = ConstantPool::try_new(
        field.clone(),
        full_type.clone(),
        Float32Array::from(vec![Some(0.0), Some(f32::from_bits(bits))]).to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap();
    let original = pool.value(1).unwrap();
    let args = [column(full_type.clone())];
    let selected = binding(&args, ty(DataType::Boolean, false), 1);
    let zero = novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
            max_rows: 0,
            ..policy()
        },
    );
    let captured = move_authored_call_arguments_observed(
        &selected,
        1,
        vec![FunctionArgument::Value {
            value_type: full_type.clone(),
            constant: Some(original),
        }],
        zero,
        &Control::default(),
    )
    .unwrap();
    let (actual, cv) = value(&captured.request().arguments[0]);
    let cv = cv.unwrap();
    assert_eq!(actual, &full_type);
    assert_eq!(cv.ordinal(), 1);
    assert!(Arc::ptr_eq(cv.pool().field_ref(), &field));
    assert!(Arc::ptr_eq(cv.pool().array(), pool.array()));
    assert_eq!(cv.try_f32_bits().unwrap(), Some(bits));
    assert!(std::ptr::eq(
        selected.resolved(),
        captured.binding().resolved()
    ));
}
