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

//! Lowest-owner static type correspondence, including actual TypeOnly children.
use super::support::*;
use arrow_schema::{DataType, Field, TimeUnit};
use novarocks_expression_contract::*;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_type_contract::*;
use std::{collections::HashMap, sync::Arc};

fn type_only_root(b: &mut BoundExprSetBuilder, child: ExprId, output: ValueType) -> ExprId {
    // This is the same explicit neutral fixture declaration as the existing
    // control_derivation TypeOnly case, not a runtime function installation.
    let function = function(
        "fixture/static-types/type-only",
        vec![FunctionArgumentType::Value(output)],
        boolean(false),
    );
    let request = request(&function);
    let root = b
        .add(
            None,
            boolean(false),
            ExprKind::FunctionCall {
                function,
                args: Box::new([child]),
            },
        )
        .unwrap();
    b.request(root, request).unwrap();
    b.definition_control(
        root,
        DefinitionControl {
            shape: ControlShape::TypeOnly,
            temporal_sources: None,
        },
    )
    .unwrap();
    root
}

fn seal_fixture(
    b: BoundExprSetBuilder,
    ports: Vec<ExprPort>,
    hidden: bool,
    control: &Control,
) -> Result<BoundExprSet, UnitError> {
    if !hidden {
        return no_calls(b, ports, control);
    }
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        with_call_claims(
            b.seal(ports, &mut work)?,
            ArgumentControl::TypeOnly,
            &mut work,
        )
    })();
    if result.as_ref().err().is_some_and(|error| {
        matches!(
            error.failure_fact().class(),
            FailureClass::Control | FailureClass::Resource | FailureClass::Representation
        )
    }) {
        return result;
    }
    work.finish()?;
    result
}

fn checked(
    inputs: &[ValueType],
    output: ValueType,
    kind: impl FnOnce(&[ExprId]) -> ExprKind,
    hidden: bool,
    demand: EvaluationDemand,
    control: &Control,
) -> Result<(BoundExprSet, ExprId), UnitError> {
    let mut b = BoundExprSetBuilder::new(limits());
    let definitions: Vec<_> = inputs
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            b.add(
                None,
                ty.clone(),
                ExprKind::Value(ValueId::new(ordinal as u32)),
            )
            .unwrap()
        })
        .collect();
    let definition = b.add(None, output.clone(), kind(&definitions)).unwrap();
    let root = if hidden {
        type_only_root(&mut b, definition, output)
    } else {
        definition
    };
    b.root(root, demand, RootScope::Port(0)).unwrap();
    let ports = vec![ExprPort {
        values: inputs
            .iter()
            .enumerate()
            .map(|(ordinal, ty)| (ValueId::new(ordinal as u32), ty.clone()))
            .collect(),
    }];
    let unit = seal_fixture(b, ports, hidden, control)?;
    assert_eq!(
        unit.definition(definition).unwrap().ty,
        if hidden {
            match &unit.definition(root).unwrap().kind {
                ExprKind::FunctionCall { function, .. } => match &function.argument_types[0] {
                    FunctionArgumentType::Value(ty) => ty.clone(),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        } else {
            unit.definition(root).unwrap().ty.clone()
        }
    );
    assert_eq!(
        unit.control_flow()
            .uses()
            .values()
            .any(|invocation| invocation.definition == definition),
        !hidden
    );
    Ok((unit, definition))
}
fn binary(op: BinaryOperator, ids: &[ExprId]) -> ExprKind {
    ExprKind::Binary {
        left: ids[0],
        right: ids[1],
        op,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        allow_throw_exception: None,
    }
}
fn type_error(result: Result<(BoundExprSet, ExprId), UnitError>, expected: &str) {
    let error = result.unwrap_err();
    assert!(
        matches!(&error, UnitError::Invalid { definition: Some(_), root: None, message } if message.contains(expected)),
        "{error:?}"
    );
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
}
fn nested(child_nullable: bool, json: bool, dictionary_id: i64, annotation: &str) -> ValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let children = [
        ValueField::new(
            "dictionary",
            ValueTypeRef::try_new(ValueKind::String, child_nullable, storage.clone()).unwrap(),
            storage.clone(),
        ),
        ValueField::new(
            "payload",
            ValueTypeRef::try_new(
                if json {
                    ValueKind::Nominal(ValueLogicalType::Json)
                } else {
                    ValueKind::Binary
                },
                true,
                storage.clone(),
            )
            .unwrap(),
            storage.clone(),
        ),
    ];
    #[allow(deprecated)]
    let source = DataType::Struct(
        vec![
            Field::new_dict(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                child_nullable,
                dictionary_id,
                true,
            )
            .with_metadata(HashMap::from([(
                "provider.annotation".into(),
                annotation.into(),
            )])),
            Field::new("payload", DataType::LargeBinary, true),
        ]
        .into(),
    );
    ValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::Struct(children.into()), false, storage.clone()).unwrap(),
        ValueLayout::try_new(&source, storage).unwrap(),
    )
    .unwrap()
}

#[test]
fn static_comparisons_keep_nullsafe_domains_and_type_only_checks() {
    let exact = nested(true, true, -91, "original");
    for op in [
        BinaryOperator::Eq,
        BinaryOperator::NotEq,
        BinaryOperator::EqForNull,
        BinaryOperator::Lt,
        BinaryOperator::LtEq,
        BinaryOperator::Gt,
        BinaryOperator::GtEq,
    ] {
        for hidden in [false, true] {
            for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
                let nullable = op != BinaryOperator::EqForNull;
                let run = |left, right, result| {
                    checked(
                        &[left, right],
                        result,
                        |ids| binary(op, ids),
                        hidden,
                        demand,
                        &Control::good(),
                    )
                };
                let (unit, definition) =
                    run(integer(true), integer(false), boolean(nullable)).unwrap();
                assert_eq!(unit.definition(definition).unwrap().ty, boolean(nullable));
                type_error(
                    run(integer(true), integer(false), boolean(!nullable)),
                    "binary expression types are inconsistent",
                );
                run(
                    exact.clone(),
                    nested(true, true, 17, "nonsemantic source change"),
                    boolean(nullable),
                )
                .unwrap();
                for changed in [
                    nested(false, true, -91, "original"),
                    nested(true, false, -91, "original"),
                ] {
                    type_error(
                        run(exact.clone(), changed, boolean(nullable)),
                        "binary expression types are inconsistent",
                    );
                }
                for (left, right) in [
                    (
                        ValueType::try_with_logical_type(
                            DataType::LargeBinary,
                            false,
                            ValueLogicalType::Json,
                        )
                        .unwrap(),
                        ValueType::new(DataType::LargeBinary, false),
                    ),
                    (
                        ValueType::try_with_logical_type(
                            DataType::FixedSizeBinary(16),
                            false,
                            ValueLogicalType::Uuid,
                        )
                        .unwrap(),
                        ValueType::new(DataType::FixedSizeBinary(16), false),
                    ),
                    (
                        ValueType::new(DataType::Decimal128(18, 2), false),
                        ValueType::new(DataType::Decimal128(18, 3), false),
                    ),
                    (
                        ValueType::new(
                            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                            false,
                        ),
                        ValueType::new(
                            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                            false,
                        ),
                    ),
                    (integer(false), ValueType::new(DataType::Int32, false)),
                ] {
                    type_error(
                        run(left, right, boolean(nullable)),
                        "binary expression types are inconsistent",
                    );
                }
                if demand == EvaluationDemand::Value {
                    type_error(
                        run(integer(false), integer(false), integer(false)),
                        "binary expression types are inconsistent",
                    );
                }
            }
        }
    }
}

#[test]
fn static_case_keeps_exact_branches_implicit_null_and_type_only_checks() {
    let exact = nested(true, true, -91, "original");
    let encoded = nested(true, true, 17, "different source annotation");
    for hidden in [false, true] {
        for simple in [false, true] {
            let case = |ids: &[ExprId], has_else: bool| ExprKind::Case {
                ieee_float_equality: false,
                operand: simple.then_some(ids[0]),
                when_then: Box::new([(ids[usize::from(simple)], ids[usize::from(simple) + 1])]),
                else_expr: has_else.then(|| ids[usize::from(simple) + 2]),
            };
            let inputs = if simple {
                vec![
                    exact.clone(),
                    encoded.clone(),
                    exact.clone(),
                    encoded.clone(),
                ]
            } else {
                vec![boolean(true), exact.clone(), encoded.clone()]
            };
            checked(
                &inputs,
                exact.clone(),
                |ids| case(ids, true),
                hidden,
                EvaluationDemand::Value,
                &Control::good(),
            )
            .unwrap();
            let without_else = &inputs[..inputs.len() - 1];
            type_error(
                checked(
                    without_else,
                    exact.clone(),
                    |ids| case(ids, false),
                    hidden,
                    EvaluationDemand::Value,
                    &Control::good(),
                ),
                "CASE result stops admitting null",
            );
            let nullable = exact.with_nullable(true).unwrap();
            checked(
                without_else,
                nullable,
                |ids| case(ids, false),
                hidden,
                EvaluationDemand::Value,
                &Control::good(),
            )
            .unwrap();
            for ordinal in [usize::from(simple) + 1, usize::from(simple) + 2] {
                let mut changed = inputs.clone();
                changed[ordinal] = nested(false, true, -91, "original");
                type_error(
                    checked(
                        &changed,
                        exact.clone(),
                        |ids| case(ids, true),
                        hidden,
                        EvaluationDemand::Value,
                        &Control::good(),
                    ),
                    "CASE",
                );
            }
            let mut nullable_arm = inputs.clone();
            nullable_arm[usize::from(simple) + 1] = exact.with_nullable(true).unwrap();
            type_error(
                checked(
                    &nullable_arm,
                    exact.clone(),
                    |ids| case(ids, true),
                    hidden,
                    EvaluationDemand::Value,
                    &Control::good(),
                ),
                "CASE result stops admitting null",
            );
            if !simple {
                let mut wrong = inputs.clone();
                wrong[0] = integer(false);
                type_error(
                    checked(
                        &wrong,
                        exact.clone(),
                        |ids| case(ids, true),
                        hidden,
                        EvaluationDemand::Value,
                        &Control::good(),
                    ),
                    "CASE condition",
                );
            }
        }
    }
}

#[test]
fn static_unary_null_predicates_preserve_value_demand_and_reject_narrowing() {
    for hidden in [false, true] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            for negated in [false, true] {
                let run = |input, result| {
                    checked(
                        &[input],
                        result,
                        |ids| ExprKind::IsNull {
                            expr: ids[0],
                            negated,
                        },
                        hidden,
                        demand,
                        &Control::good(),
                    )
                };
                for input in [
                    boolean(true),
                    integer(true),
                    ValueType::try_with_logical_type(
                        DataType::LargeBinary,
                        false,
                        ValueLogicalType::Json,
                    )
                    .unwrap(),
                ] {
                    let (unit, definition) = run(input.clone(), boolean(false)).unwrap();
                    assert_eq!(unit.definition(definition).unwrap().ty, boolean(false));
                    if !hidden {
                        let root = unit
                            .control_flow()
                            .uses()
                            .values()
                            .find(|u| u.definition == definition)
                            .unwrap();
                        assert_eq!(
                            unit.control_flow().uses()[&root.arguments[0]]
                                .context
                                .demand,
                            EvaluationDemand::Value
                        );
                    }
                    type_error(run(input, boolean(true)), "non-nullable Boolean");
                }
            }
            let run = |input, result| {
                checked(
                    &[input],
                    result,
                    |ids| ExprKind::Unary {
                        expr: ids[0],
                        op: UnaryOperator::Not,
                    },
                    hidden,
                    demand,
                    &Control::good(),
                )
            };
            run(boolean(true), boolean(true)).unwrap();
            type_error(run(boolean(true), boolean(false)), "unary expression types");
            for unsupported in [
                ValueType::new(DataType::Utf8, false),
                ValueType::try_with_logical_type(
                    DataType::LargeBinary,
                    false,
                    ValueLogicalType::Json,
                )
                .unwrap(),
            ] {
                type_error(run(unsupported, boolean(false)), "unary expression types");
            }
        }
    }
}

#[test]
fn wide_static_comparison_type_checks_keep_every_original_control_prefix() {
    let full = ValueType::new(
        DataType::Struct(
            (0..320)
                .map(|ordinal| Field::new(format!("field{ordinal}"), DataType::Int64, true))
                .collect::<Vec<_>>()
                .into(),
        ),
        false,
    );
    let changed = ValueType::new(
        DataType::Struct(
            (0..320)
                .map(|ordinal| {
                    Field::new(format!("field{ordinal}"), DataType::Int64, ordinal != 319)
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        false,
    );
    for hidden in [false, true] {
        for op in [BinaryOperator::Eq, BinaryOperator::EqForNull] {
            for right in [&full, &changed] {
                let run = |control: &Control| {
                    checked(
                        &[full.clone(), right.clone()],
                        boolean(false),
                        |ids| binary(op, ids),
                        hidden,
                        EvaluationDemand::Value,
                        control,
                    )
                };
                let good = Control::good();
                if std::ptr::eq(right, &full) {
                    run(&good).unwrap();
                } else {
                    type_error(run(&good), "binary expression types are inconsistent");
                }
                let trace = good.trace();
                assert!(trace.iter().any(|(_, work)| *work == 256));
                for at in 0..trace.len() {
                    for cause in [
                        CompileControlError::Cancelled,
                        CompileControlError::DeadlineExceeded,
                        CompileControlError::ResourceExhausted,
                    ] {
                        let control = Control::at(at, cause.clone());
                        assert!(
                            matches!(run(&control),Err(UnitError::Control(actual)) if actual==cause)
                        );
                        assert_eq!(control.trace(), trace[..=at]);
                    }
                }
            }
        }
    }
}

fn cast_list_type(item: DataType, child_nullable: bool, root_nullable: bool) -> ValueType {
    ValueType::new(
        DataType::List(Arc::new(Field::new("declared-item", item, child_nullable))),
        root_nullable,
    )
}

fn cast_json_list_type(child_nullable: bool, root_nullable: bool) -> ValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let child = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        child_nullable,
        storage.clone(),
    )
    .unwrap();
    let value =
        ValueTypeRef::try_new(ValueKind::List(child), root_nullable, storage.clone()).unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new(
            "declared-item",
            DataType::LargeBinary,
            child_nullable,
        ))),
        storage,
    )
    .unwrap();
    ValueType::try_from_value(value, layout).unwrap()
}

fn checked_array_cast(
    source: ValueType,
    result: ValueType,
    source_kind: ExprKind,
    mode: TrinoCastMode,
    unused: bool,
) -> Result<(BoundExprSet, ExprId), UnitError> {
    let input = ValueId::new(0);
    let mut builder = BoundExprSetBuilder::new(limits());
    let child = builder.add(None, source.clone(), source_kind).unwrap();
    let cast = builder
        .add(
            None,
            result.clone(),
            ExprKind::Cast {
                expr: child,
                target: result.data_type().clone(),
                cast_mode: mode,
            },
        )
        .unwrap();
    let root = if unused {
        type_only_root(&mut builder, cast, result.clone())
    } else {
        cast
    };
    builder
        .root(root, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = seal_fixture(
        builder,
        vec![ExprPort {
            values: Box::new([(input, source.clone())]),
        }],
        unused,
        &Control::good(),
    )?;
    let retained = unit.definition(cast).unwrap();
    assert_eq!(unit.definition(child).unwrap().ty, source);
    assert_eq!(retained.ty, result);
    assert!(matches!(
        &retained.kind,
        ExprKind::Cast {
            expr,
            target,
            cast_mode,
        } if *expr == child
            && target == result.data_type()
            && *cast_mode == mode
    ));
    assert_eq!(unit.control_flow().uses().len(), if unused { 1 } else { 2 });
    assert_eq!(
        unit.control_flow()
            .uses()
            .values()
            .any(|usage| usage.definition == cast),
        !unused
    );
    Ok((unit, cast))
}

#[test]
fn array_cast_modes_preserve_live_and_unused_declarations() {
    use TrinoCastMode as Mode;
    let list = |item, nullable| cast_list_type(item, true, nullable);
    for unused in [false, true] {
        for mode in [
            Mode::Explicit,
            Mode::Try,
            Mode::Implicit,
            Mode::StoreAssignment,
        ] {
            checked_array_cast(
                list(DataType::Int32, true),
                list(DataType::Int64, true),
                ExprKind::Value(ValueId::new(0)),
                mode,
                unused,
            )
            .unwrap();
            type_error(
                checked_array_cast(
                    list(DataType::Int32, true),
                    list(DataType::Int64, false),
                    ExprKind::Value(ValueId::new(0)),
                    mode,
                    unused,
                ),
                "cast stops admitting null its input admits",
            );
        }
    }
}

#[test]
fn null_list_json_cast_checks_live_and_unused_definitions_and_full_types() {
    let input = ExprKind::Value(ValueId::new(0));
    for unused in [false, true] {
        for (source_nullable, result_nullable) in [(false, false), (false, true), (true, true)] {
            checked_array_cast(
                cast_list_type(DataType::Null, true, source_nullable),
                cast_json_list_type(true, result_nullable),
                input.clone(),
                TrinoCastMode::Implicit,
                unused,
            )
            .unwrap();
        }
        for (source, result) in [
            (
                cast_list_type(DataType::Null, true, false),
                cast_json_list_type(false, true),
            ),
            (
                cast_list_type(DataType::Null, true, true),
                cast_json_list_type(true, false),
            ),
            (
                ValueType::new(DataType::Null, true),
                cast_json_list_type(true, true),
            ),
        ] {
            type_error(
                checked_array_cast(
                    source,
                    result,
                    input.clone(),
                    TrinoCastMode::Implicit,
                    unused,
                ),
                "carrier CAST cannot establish or erase a logical identity",
            );
        }
    }
}

#[test]
fn checked_nonempty_list_constant_cannot_gain_json_identity_in_live_or_unused_cast() {
    use arrow_array::{
        Array, ArrayRef,
        builder::{LargeBinaryBuilder, ListBuilder, StringBuilder},
    };
    use novarocks_constant_contract::{
        ConstantPool, ConstantPoolId, ConstantPools, ConstantReference,
    };
    let mut utf8 = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
        "declared-item",
        DataType::Utf8,
        true,
    )));
    utf8.values().append_value("original nonempty UTF8");
    utf8.append(true);
    let mut binary = ListBuilder::new(LargeBinaryBuilder::new()).with_field(Arc::new(Field::new(
        "declared-item",
        DataType::LargeBinary,
        true,
    )));
    binary.values().append_value(b"\x01\x20");
    binary.append(true);
    let target = cast_json_list_type(true, true);
    let arrays: [(ArrayRef, bool); 2] = [
        (Arc::new(utf8.finish()), false),
        (Arc::new(binary.finish()), true),
    ];
    for (array, same_carrier) in arrays {
        let source = ValueType::new(array.data_type().clone(), false);
        assert_eq!(source.data_type() == target.data_type(), same_carrier);
        let control = Control::good();
        let pool = ConstantPool::try_new(
            Arc::new(Field::new(
                "original_list",
                array.data_type().clone(),
                false,
            )),
            source.clone(),
            array.to_data(),
            policy(),
            CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let reference = ConstantReference {
            pool: ConstantPoolId::new(4),
            ordinal: 0,
        };
        let mut pools = ConstantPools::empty();
        pools.insert(reference.pool, pool).unwrap();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let original = pools.resolve_source_observed(reference, &mut work).unwrap();
        assert_eq!(original.value_type(), &source);
        work.finish().unwrap();
        for unused in [false, true] {
            type_error(
                checked_array_cast(
                    original.value_type().clone(),
                    target.clone(),
                    ExprKind::Constant(reference),
                    TrinoCastMode::Implicit,
                    unused,
                ),
                "carrier CAST cannot establish or erase a logical identity",
            );
        }
    }
}
