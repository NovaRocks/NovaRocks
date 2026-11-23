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

use super::{
    Control, FrameFixture, POLICY, draft, finish_frame,
    guarded::{argument, author},
};
use crate::evaluator::CompiledExpressionInstance;
use crate::testkit::{CountingAllocator, add_call, checked_constants, register_constant};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, StructArray},
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{ExprKind, ValueId};
use novarocks_functions::{
    ConstantValue, EngineFunctionCatalogBuilder, FunctionKind, InstalledPureKernel,
    PureEngineFunctionCatalog,
};
use novarocks_type_contract::{
    CompilePhase, ControlShape, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

fn actual_functions() -> PureEngineFunctionCatalog {
    static CATALOG: std::sync::OnceLock<PureEngineFunctionCatalog> = std::sync::OnceLock::new();
    CATALOG.get_or_init(|| {
        let actual = novarocks_functions::builtin::catalogue::array_projection_diagnostic_private_test_catalog();
        let builtin = novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        let mut installed = Vec::new();
        for (catalog, definition) in actual.definitions().iter().map(|definition| (&actual,definition)).chain(std::iter::once((&builtin,builtin.definition("rand",FunctionKind::Scalar).unwrap()))) {
            let binding = definition.binding_declaration().unwrap();
            builder.register(definition.clone()).unwrap();
            for overload in binding.overloads() {
                let loan = catalog.pure_overload_declaration_observed(binding.function_id(),definition.kind(),&overload.identity,&Control).unwrap();
                installed.push(InstalledPureKernel {function:binding.function_id().clone(),kind:definition.kind(),implementation:loan.implementation().clone(),aggregate_state_format:None});
            }
        }
        // Every record comes from the same real installed adapter. RAND is an
        // actually volatile, stateful, fully bound tail, never a toy counter.
        builder.seal_pure(installed).unwrap()
    }).clone()
}

fn input_type() -> FunctionValueType {
    FunctionValueType::new(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::new(Field::new("ID", DataType::Int32, true))].into()),
            true,
        ))),
        true,
    )
}
#[derive(Clone, Copy)]
pub enum Guard {
    Direct,
    IfThen,
    IfElse,
    And,
    Or,
    Filter,
}
pub fn invocation_program(count: usize, guard: Guard) -> FrameFixture {
    invocation_program_with(count, guard, actual_functions(), "id", SourceMode::Slot)
}
#[derive(Clone, Copy, PartialEq)]
pub enum SourceMode {
    Slot,
    Computed,
    RequiredError,
    IfThen,
    IfElse,
    NestedIf,
}
fn invocation_program_with(
    count: usize,
    guard: Guard,
    functions: PureEngineFunctionCatalog,
    field_name: &str,
    source_mode: SourceMode,
) -> FrameFixture {
    let mut source_pools = ConstantPools::empty();
    let source_type = input_type();
    let flag_type = FunctionValueType::new(DataType::Boolean, true);
    let tail_type = FunctionValueType::new(DataType::Int64, true);
    let columns = [
        (ValueId::new(1), source_type.clone()),
        (ValueId::new(2), flag_type.clone()),
        (ValueId::new(3), tail_type.clone()),
    ];
    let mut builder = draft();
    let mut authors = BTreeMap::new();
    // Retain the original producer constant pool order.
    for (_, ty) in &columns {
        checked_constants::kind(&mut source_pools, checked_constants::null(ty, POLICY));
    }
    let mut data = builder
        .add(None, source_type.clone(), ExprKind::Value(columns[0].0))
        .unwrap();
    let name_type = FunctionValueType::new(DataType::Utf8, false);
    let name = if source_mode == SourceMode::Slot {
        ConstantValue::from_utf8(
            Arc::new(name_type.try_to_field("actual field name").unwrap()),
            name_type.clone(),
            field_name,
            POLICY,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap()
    } else {
        let array: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![
            "poison_not_a_field",
            field_name,
        ]));
        novarocks_functions::ConstantPool::try_new(
            Arc::new(name_type.try_to_field("actual field name").unwrap()),
            name_type.clone(),
            array.to_data(),
            POLICY,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap()
        .value(1)
        .unwrap()
    };
    let name_expr = builder
        .add(
            None,
            name_type.clone(),
            ExprKind::Constant(register_constant(&mut source_pools, &name)),
        )
        .unwrap();
    let allow_ref = novarocks_type_contract::SemanticParameterRef {
        id: novarocks_type_contract::SemanticParameterId::new(u32::MAX),
        expected_key: novarocks_type_contract::SemanticParameterKey::AllowThrowException,
    };
    if source_mode == SourceMode::Computed {
        data = builder
            .add(
                None,
                source_type.clone(),
                ExprKind::Cast {
                    cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
                    expr: data,
                    target: source_type.data_type().clone(),
                },
            )
            .unwrap();
    } else if source_mode == SourceMode::RequiredError {
        let tail = builder
            .add(None, tail_type.clone(), ExprKind::Value(columns[2].0))
            .unwrap();
        let one_ty = FunctionValueType::new(DataType::Int64, false);
        let one = builder
            .add(
                None,
                one_ty.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::i64(&(one_ty.clone()), 1, POLICY),
                ),
            )
            .unwrap();
        // Fractional Divide(1, 0) is a successful NULL by the frozen
        // arithmetic contract. Integer Modulo(1, 0) is the real RowData
        // failure required to test transport from outside outer demand.
        let remainder_ty = novarocks_type_contract::arithmetic_result_value_type_with_op(
            &one_ty,
            &tail_type,
            novarocks_type_contract::ArithmeticOperator::Modulo,
        )
        .unwrap();
        let remainder = builder
            .add(
                None,
                remainder_ty,
                ExprKind::Binary {
                    op: novarocks_expression_contract::BinaryOperator::Modulo,
                    left: one,
                    right: tail,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                    allow_throw_exception: Some(allow_ref),
                },
            )
            .unwrap();
        let bool_ty = FunctionValueType::new(DataType::Boolean, true);
        let condition = builder
            .add(
                None,
                bool_ty.clone(),
                ExprKind::Cast {
                    cast_mode: novarocks_type_contract::TrinoCastMode::Explicit,
                    expr: remainder,
                    target: DataType::Boolean,
                },
            )
            .unwrap();
        data = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(bool_ty, None),
                    argument(source_type.clone(), None),
                    argument(source_type.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![condition, data, data],
        );
    }
    if matches!(
        source_mode,
        SourceMode::IfThen | SourceMode::IfElse | SourceMode::NestedIf
    ) {
        let flag = builder
            .add(None, flag_type.clone(), ExprKind::Value(columns[1].0))
            .unwrap();
        let null = builder
            .add(
                None,
                source_type.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(&(source_type.clone()), POLICY),
                ),
            )
            .unwrap();
        if source_mode == SourceMode::NestedIf {
            let tail = builder
                .add(None, tail_type.clone(), ExprKind::Value(columns[2].0))
                .unwrap();
            let inner = builder
                .add(
                    None,
                    flag_type.clone(),
                    ExprKind::Cast {
                        cast_mode: novarocks_type_contract::TrinoCastMode::Explicit,
                        expr: tail,
                        target: DataType::Boolean,
                    },
                )
                .unwrap();
            data = add_call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "if",
                    vec![
                        argument(flag_type.clone(), None),
                        argument(source_type.clone(), None),
                        argument(source_type.clone(), None),
                    ],
                    ControlShape::If,
                ),
                vec![inner, data, null],
            );
        }
        let (left, right) = if source_mode == SourceMode::IfElse {
            (null, data)
        } else {
            (data, null)
        };
        data = add_call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(flag_type.clone(), None),
                    argument(source_type.clone(), None),
                    argument(source_type.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![flag, left, right],
        );
    }
    let mut arguments = vec![argument(source_type, None), argument(name_type, Some(name))];
    let mut definitions = vec![data, name_expr];
    // Complete bound volatile/math tail definitions are retained. The first
    // real RAND would create a mutable instance if demanded. Other tails retain
    // original divide-zero semantics; their value is never evaluated here.
    for ordinal in 2..count {
        if ordinal == 2 {
            let random = add_call(
                &mut builder,
                &mut authors,
                author(&functions, "rand", vec![], ControlShape::Eager),
                vec![],
            );
            let ty = builder.definition(random).unwrap().ty.clone();
            definitions.push(random);
            arguments.push(argument(ty, None));
            continue;
        }
        let tail = builder
            .add(None, tail_type.clone(), ExprKind::Value(columns[2].0))
            .unwrap();
        let zero = builder
            .add(
                None,
                (FunctionValueType::new(DataType::Int64, false)).clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::i64(
                        &(FunctionValueType::new(DataType::Int64, false)),
                        0,
                        POLICY,
                    ),
                ),
            )
            .unwrap();
        let divided_type = novarocks_type_contract::arithmetic_result_value_type_with_op(
            &tail_type,
            &FunctionValueType::new(DataType::Int64, false),
            novarocks_type_contract::ArithmeticOperator::Divide,
        )
        .unwrap();
        let divided = builder
            .add(
                None,
                divided_type.clone(),
                ExprKind::Binary {
                    op: novarocks_expression_contract::BinaryOperator::Divide,
                    left: tail,
                    right: zero,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                    allow_throw_exception: Some(novarocks_type_contract::SemanticParameterRef {
                        id: novarocks_type_contract::SemanticParameterId::new(u32::MAX),
                        expected_key:
                            novarocks_type_contract::SemanticParameterKey::AllowThrowException,
                    }),
                },
            )
            .unwrap();
        definitions.push(divided);
        arguments.push(argument(divided_type, None));
    }
    let demand = if novarocks_functions::invocation_arity::ARRAY_STRUCT_SUBFIELD_ARITY
        .failure(count)
        .is_some()
    {
        ControlShape::NoArguments
    } else {
        ControlShape::Eager
    };
    let projected = add_call(
        &mut builder,
        &mut authors,
        author(&functions, "__array_struct_subfield", arguments, demand),
        definitions,
    );
    let expr = match guard {
        Guard::Direct => projected,
        Guard::And | Guard::Or => {
            let predicate = builder
                .add(
                    None,
                    FunctionValueType::new(DataType::Boolean, false),
                    ExprKind::IsNull {
                        expr: projected,
                        negated: false,
                    },
                )
                .unwrap();
            let decisive = builder
                .add(
                    None,
                    (FunctionValueType::new(DataType::Boolean, false)).clone(),
                    checked_constants::kind(
                        &mut source_pools,
                        checked_constants::boolean(
                            &(FunctionValueType::new(DataType::Boolean, false)),
                            matches!(guard, Guard::Or),
                            POLICY,
                        ),
                    ),
                )
                .unwrap();
            let kind = if matches!(guard, Guard::And) {
                ExprKind::Conjunction {
                    args: Box::from([predicate, decisive]),
                }
            } else {
                ExprKind::Disjunction {
                    args: Box::from([predicate, decisive]),
                }
            };
            builder
                .add(None, FunctionValueType::new(DataType::Boolean, false), kind)
                .unwrap()
        }
        Guard::Filter => builder
            .add(
                None,
                FunctionValueType::new(DataType::Boolean, false),
                ExprKind::IsNull {
                    expr: projected,
                    negated: false,
                },
            )
            .unwrap(),
        Guard::IfThen | Guard::IfElse => {
            // Original IS NULL is a non-null Boolean consumer. Whole ARRAY
            // Data escapes before it can mask or produce a Boolean result.
            let predicate_type = FunctionValueType::new(DataType::Boolean, false);
            let predicate = builder
                .add(
                    None,
                    predicate_type.clone(),
                    ExprKind::IsNull {
                        expr: projected,
                        negated: false,
                    },
                )
                .unwrap();
            let otherwise_type = FunctionValueType::new(DataType::Boolean, true);
            let flag = builder
                .add(None, flag_type.clone(), ExprKind::Value(columns[1].0))
                .unwrap();
            let otherwise = builder
                .add(
                    None,
                    otherwise_type.clone(),
                    checked_constants::kind(
                        &mut source_pools,
                        checked_constants::null(&(otherwise_type.clone()), POLICY),
                    ),
                )
                .unwrap();
            let (then_expr, else_expr, then_type, else_type) = if matches!(guard, Guard::IfThen) {
                (predicate, otherwise, predicate_type, otherwise_type)
            } else {
                (otherwise, predicate, otherwise_type, predicate_type)
            };
            add_call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "if",
                    vec![
                        argument(flag_type, None),
                        argument(then_type, None),
                        argument(else_type, None),
                    ],
                    ControlShape::If,
                ),
                vec![flag, then_expr, else_expr],
            )
        }
    };
    // The complete dead arithmetic definition retains its explicit original
    // arithmetic fixture policy and semantic parameter, even with demand zero.
    let parameters = if count >= 5
        || matches!(
            source_mode,
            SourceMode::Computed | SourceMode::RequiredError | SourceMode::NestedIf
        ) {
        SemanticParameters::try_new([(
            novarocks_type_contract::SemanticParameterId::new(u32::MAX),
            novarocks_type_contract::SemanticParameterValue::AllowThrowException(true),
        )])
        .unwrap()
    } else {
        SemanticParameters::try_new([]).unwrap()
    };
    let demand = if matches!(guard, Guard::Filter) {
        EvaluationDemand::TruthOnly
    } else {
        EvaluationDemand::Value
    };
    finish_frame(
        &functions,
        builder,
        (expr, demand),
        Vec::from(columns),
        parameters,
        source_pools,
        authors,
    )
}

pub fn input(program: &FrameFixture, flags: Vec<Option<bool>>, root_null: bool) -> RecordBatch {
    let rows = flags.len();
    let struct_field = Arc::new(Field::new("ID", DataType::Int32, true));
    let values = Arc::new(StructArray::new(
        vec![struct_field].into(),
        vec![Arc::new(Int32Array::from(vec![Some(7); rows])) as ArrayRef],
        None,
    )) as ArrayRef;
    let offsets = OffsetBuffer::new(ScalarBuffer::from(
        (0..=rows)
            .map(|r| i32::try_from(r).unwrap())
            .collect::<Vec<_>>(),
    ));
    let declared_type = input_type();
    let DataType::List(item) = declared_type.data_type() else {
        unreachable!()
    };
    let list = Arc::new(ListArray::new(
        item.clone(),
        offsets,
        values,
        root_null.then(|| NullBuffer::new_null(rows)),
    )) as ArrayRef;
    RecordBatch::try_new(
        program.schema.clone(),
        vec![
            list,
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(vec![Some(1); rows])),
        ],
    )
    .unwrap()
}
pub fn tracked_instance(
    program: &FrameFixture,
) -> (CompiledExpressionInstance, Arc<CountingAllocator>) {
    let allocator = Arc::new(CountingAllocator::new(usize::MAX));
    (
        CompiledExpressionInstance::try_new_with_allocator(
            program.unit.clone(),
            program.root,
            &Control,
            Some(allocator.clone()),
        )
        .unwrap(),
        allocator,
    )
}

pub fn original_source_program(field_name: &str, guard: Guard, mode: SourceMode) -> FrameFixture {
    let catalog =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for name in ["__array_struct_subfield", "if", "rand"] {
        let definition = catalog.definition(name, FunctionKind::Scalar).unwrap();
        let binding = definition.binding_declaration().unwrap();
        builder.register(definition.clone()).unwrap();
        for overload in binding.overloads() {
            let declaration = catalog
                .pure_overload_declaration_observed(
                    binding.function_id(),
                    definition.kind(),
                    &overload.identity,
                    &Control,
                )
                .unwrap();
            installed.push(InstalledPureKernel {
                function: binding.function_id().clone(),
                kind: definition.kind(),
                implementation: declaration.implementation().clone(),
                aggregate_state_format: None,
            });
        }
    }
    invocation_program_with(
        2,
        guard,
        builder.seal_pure(installed).unwrap(),
        field_name,
        mode,
    )
}

pub fn sliced_full_child_batch(program: &FrameFixture, flags: Vec<Option<bool>>) -> RecordBatch {
    assert_eq!(flags.len(), 2);
    let structure = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("ID", DataType::Int32, true))].into(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50]))],
        None,
    )) as ArrayRef;
    let DataType::List(item) = input_type().data_type().clone() else {
        unreachable!()
    };
    // These are real sliced offsets and unused children, not a prediction from
    // the implementation's selected output or a normalized fixture.
    let list = Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![2i32, 4, 5])),
        structure,
        None,
    ));
    RecordBatch::try_new(
        program.schema.clone(),
        vec![
            list,
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(vec![1, 1])),
        ],
    )
    .unwrap()
}
