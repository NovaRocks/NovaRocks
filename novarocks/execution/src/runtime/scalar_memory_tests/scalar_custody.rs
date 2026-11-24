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

use crate::exec::expr::fixtures::{author, call, compile_checked_fragment_with_parameters};
use crate::exec::expr::test_support::HarnessControl as Control;
use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all};
use crate::runtime::{
    query_memory::QueryMemoryBinding,
    runtime_state::RuntimeState,
    scalar_memory::{
        RuntimeScalarMemoryJournal, RuntimeScalarMemoryRefusal, RuntimeScalarMemoryScope,
    },
    task::{ExecutionFailure, ExecutionFailureCause},
};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array},
    datatypes::DataType,
};
use novarocks_expression::CompiledExprSet;
use novarocks_expression::testkit::checked_constants;
use novarocks_expression::{CompiledExpressionInstance, CompiledFilterConjunctionInstance};
use novarocks_expression::{EvaluationInput, testkit::PassThroughCallHost};
use novarocks_expression_contract::{ExprKind, RootOrdinal, RootScope};
use novarocks_functions::ScalarInvocationScope;
use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionKind, InstalledPureKernel, PureEngineFunctionCatalog,
    ScalarInvocationActivation as Activation,
};
use novarocks_functions::{KernelFailure, Selection};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority};
use novarocks_physical_plan::{
    FragmentBuilder, FragmentId, NodeId, ResultField, ResultPort, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::EvaluationDemand;
use novarocks_type_contract::{ControlShape, FunctionValueType, SemanticParameters};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy)]
enum Shape {
    Direct,
    Nested,
    IfThen,
    IfElse,
    Filter,
}
fn functions() -> PureEngineFunctionCatalog {
    let actual = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = vec![];
    for name in [
        "bitwise_left_shift",
        "bitwise_right_shift_arithmetic",
        "bitwise_right_shift",
        "if",
        "bitwise_and",
        "bitwise_or",
        "bitwise_xor",
        "bitwise_not",
        "slice",
    ] {
        let definition = actual.definition(name, FunctionKind::Scalar).unwrap();
        builder.register(definition.clone()).unwrap();
        let binding = definition.binding_declaration().unwrap();
        for overload in binding.overloads() {
            let declaration = actual
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
    builder.seal_pure(installed).unwrap()
}
fn argument(ty: &FunctionValueType) -> novarocks_functions::FunctionArgument {
    novarocks_functions::FunctionArgument::Value {
        value_type: ty.clone(),
        constant: None,
    }
}
fn fixture(name: &str, shape: Shape) -> Arc<LocalProgram> {
    fixture_typed(name, shape, DataType::Int64)
}
fn fixture_typed(name: &str, shape: Shape, source_type: DataType) -> Arc<LocalProgram> {
    fixture_family(name, shape, source_type, false)
}
fn fixture_family(
    name: &str,
    shape: Shape,
    source_type: DataType,
    bitwise: bool,
) -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = functions();
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let integer = FunctionValueType::new(source_type, true);
    let count_type = FunctionValueType::new(
        if bitwise {
            DataType::Int64
        } else {
            DataType::Int32
        },
        true,
    );
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let columns = [
        (ValueId::new(1), integer.clone()),
        (ValueId::new(2), count_type.clone()),
        (ValueId::new(3), boolean.clone()),
    ];
    let mut builder = FragmentBuilder::new(FragmentId::new(301));
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = vec![];
    for (id, ty) in &columns {
        let expr = builder
            .unit(input)
            .unwrap()
            .add(
                None,
                ty.clone(),
                checked_constants::kind(
                    &mut source_pools,
                    checked_constants::null(
                        &(ty.clone()),
                        crate::exec::expr::test_support::constant_policy(),
                    ),
                ),
            )
            .unwrap();
        let expr = builder
            .unit(input)
            .unwrap()
            .root(expr, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    root: expr,
                },
            })
            .unwrap();
        items.push((expr, *id));
    }
    builder
        .add_project(
            input,
            source,
            items.into_boxed_slice(),
            columns.iter().map(|(id, _)| *id).collect(),
        )
        .unwrap();
    let value = builder
        .unit(output)
        .unwrap()
        .add(None, integer.clone(), ExprKind::Value(columns[0].0))
        .unwrap();
    let count = if bitwise && name == "bitwise_not" && !matches!(shape, Shape::Nested) {
        // NOT has no right child. Do not create an unreachable definition.
        value
    } else {
        builder
            .unit(output)
            .unwrap()
            .add(None, count_type.clone(), ExprKind::Value(columns[1].0))
            .unwrap()
    };
    let flag = if matches!(shape, Shape::IfThen | Shape::IfElse | Shape::Filter) {
        Some(
            builder
                .unit(output)
                .unwrap()
                .add(None, boolean.clone(), ExprKind::Value(columns[2].0))
                .unwrap(),
        )
    } else {
        None
    };
    let mut authors = BTreeMap::new();
    let shifted = call(
        &mut builder,
        &mut authors,
        author(
            &functions,
            name,
            if name == "slice" {
                vec![
                    argument(&integer),
                    argument(&count_type),
                    argument(&count_type),
                ]
            } else if bitwise && name == "bitwise_not" {
                vec![argument(&integer)]
            } else {
                vec![argument(&integer), argument(&count_type)]
            },
            ControlShape::Eager,
        ),
        if name == "slice" {
            vec![value, count, count]
        } else if bitwise && name == "bitwise_not" {
            vec![value]
        } else {
            vec![value, count]
        },
    );
    let result = match shape {
        Shape::Direct | Shape::Filter => shifted,
        Shape::Nested => call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                if bitwise {
                    "bitwise_xor"
                } else {
                    "bitwise_right_shift_arithmetic"
                },
                vec![argument(&integer), argument(&count_type)],
                ControlShape::Eager,
            ),
            vec![shifted, count],
        ),
        Shape::IfThen | Shape::IfElse => {
            let branches = if matches!(shape, Shape::IfThen) {
                vec![flag.unwrap(), shifted, value]
            } else {
                vec![flag.unwrap(), value, shifted]
            };
            call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "if",
                    vec![argument(&boolean), argument(&integer), argument(&integer)],
                    ControlShape::If,
                ),
                branches,
            )
        }
    };
    let result_value = ValueId::new(101);
    let result_type = if matches!(shape, Shape::Filter) {
        let zero = if integer.data_type() == &DataType::Int64 {
            builder
                .unit(output)
                .unwrap()
                .add(
                    None,
                    integer.clone(),
                    checked_constants::kind(
                        &mut source_pools,
                        checked_constants::i64(
                            &(integer.clone()),
                            0,
                            crate::exec::expr::test_support::constant_policy(),
                        ),
                    ),
                )
                .unwrap()
        } else {
            // A narrow literal is not in the frozen literal vocabulary. The
            // new fixture compares two exact narrow values without a cast.
            value
        };
        let compare = builder
            .unit(output)
            .unwrap()
            .add(
                None,
                boolean.clone(),
                ExprKind::Binary {
                    op: novarocks_expression_contract::BinaryOperator::Gt,
                    left: result,
                    right: zero,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: None,
                },
            )
            .unwrap();
        let flag = builder
            .unit(output)
            .unwrap()
            .root(
                flag.unwrap(),
                EvaluationDemand::TruthOnly,
                RootScope::Port(0),
            )
            .unwrap();
        let compare = builder
            .unit(output)
            .unwrap()
            .root(compare, EvaluationDemand::TruthOnly, RootScope::Port(0))
            .unwrap();
        builder
            .add_filter(output, input, Box::from([flag, compare]))
            .unwrap();
        integer.clone()
    } else {
        let result = builder
            .unit(output)
            .unwrap()
            .root(result, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: result_value,
                ty: integer.clone(),
                origin: ValueOrigin::Expr {
                    node: output,
                    root: result,
                },
            })
            .unwrap();
        builder
            .add_project(
                output,
                input,
                Box::from([(result, result_value)]),
                Box::from([result_value]),
            )
            .unwrap();
        integer.clone()
    };
    let visible = if matches!(shape, Shape::Filter) {
        columns[0].0
    } else {
        result_value
    };
    let result_port = ResultPort {
        scalar_schema: None,
        fragment: builder.id(),
        output: builder.construction_node(output).unwrap().output.clone(),
        fields: if matches!(shape, Shape::Filter) {
            columns
                .iter()
                .map(|(id, ty)| ResultField {
                    domain: crate::test_result_domain::result_value_domain(ty),
                    name: format!("input_{}", id.get()).into(),
                    alias: None,
                    value: *id,
                    ty: ty.clone(),
                })
                .collect()
        } else {
            Box::from([ResultField {
                domain: crate::test_result_domain::result_value_domain(&result_type),
                name: "shift_result".into(),
                alias: None,
                value: visible,
                ty: result_type,
            }])
        },
    };
    compile_checked_fragment_with_parameters(
        &functions,
        builder,
        output,
        &authors,
        result_port,
        SemanticParameters::try_new([]).unwrap(),
        source_pools,
    )
}
fn root(program: &LocalProgram) -> ProgramRootRef {
    let ProgramNodeKind::Project { exprs, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("actual scalar Project")
    };
    exprs[0]
}
fn predicate_units(program: &LocalProgram) -> Vec<(Arc<CompiledExprSet>, RootOrdinal)> {
    let ProgramNodeKind::Filter { predicates, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("actual scalar Filter")
    };
    predicates
        .iter()
        .map(|site| (program.unit(site.unit).unwrap().clone(), site.root))
        .collect()
}

fn batch(
    program: &LocalProgram,
    values: Vec<Option<i64>>,
    flags: Vec<Option<bool>>,
) -> RecordBatch {
    let rows = values.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            match program.graph().nodes()[1]
                .output_layout()
                .schema()
                .field(0)
                .data_type()
            {
                DataType::Int8 => Arc::new(Int8Array::from(
                    values
                        .into_iter()
                        .map(|v| v.map(|v| i8::try_from(v).unwrap()))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                DataType::Int16 => Arc::new(Int16Array::from(
                    values
                        .into_iter()
                        .map(|v| v.map(|v| i16::try_from(v).unwrap()))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                DataType::Int32 => Arc::new(Int32Array::from(
                    values
                        .into_iter()
                        .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                DataType::Int64 => Arc::new(Int64Array::from(values)) as ArrayRef,
                _ => panic!("exact shift fixture source"),
            },
            Arc::new(Int32Array::from(vec![1; rows])),
            Arc::new(BooleanArray::from(flags)),
        ],
    )
    .unwrap()
}
fn bound() -> QueryMemoryBinding {
    let mut cfg = AuthorityConfig::new(131072, 65536, 65536);
    cfg.max_accounts = 32;
    cfg.max_active_owners = 4;
    cfg.metadata_budget_bytes = 16 * 1024;
    cfg.top_up = novarocks_memory::TopUpPolicy::uniform(1024);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 302), AttemptId::new(1).unwrap()).unwrap(),
        authority,
        account,
    )
    .unwrap()
}
fn runtime_control(binding: Option<&QueryMemoryBinding>) -> RuntimeKernelControl {
    let state = RuntimeState::default().with_query_memory(binding.cloned());
    let mut control = RuntimeKernelControl::new(state.error_state());
    control.bind_runtime_memory(&state);
    control
}

#[test]
fn runtime_scalar_memory_array_slice_compacts_actual_selected_lists() {
    use arrow::{array::ListArray, datatypes::Int64Type};
    let rows = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(10), Some(20), Some(30), Some(40)]),
        Some(vec![Some(88)]),
        Some(vec![Some(11), Some(22), Some(33)]),
    ]);
    let program = fixture_family("slice", Shape::Direct, rows.data_type().clone(), true);
    let batch = RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(rows),
            Arc::new(Int64Array::from(vec![2, 0, 1])),
            Arc::new(BooleanArray::from(vec![true; 3])),
        ],
    )
    .unwrap();
    let binding = bound();
    let mut control = runtime_control(Some(&binding));
    control.bind_mem_tracker(crate::runtime::mem_tracker::MemTracker::new_root(
        "array-slice-selected-lists",
    ));
    let root = root(&program);
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        program.unit(root.unit).unwrap().clone(),
        root.root,
        &control,
        control.allocator(),
    )
    .unwrap();
    let selection = Selection::try_sparse(3, &[0, 2]).unwrap();
    let mut journal = RuntimeScalarMemoryJournal::default();
    let result = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            Activation::Activated,
            &control,
            &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
        )
        .unwrap();
    assert!(result.errors().is_empty());
    let expected = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(20), Some(30)]),
        Some(vec![Some(11)]),
    ]);
    assert_eq!(result.values().to_data(), expected.to_data());
    assert_eq!(journal.body.stopped, Some(Ok(())));
}
#[test]
fn runtime_scalar_memory_original_frame_nested_and_selected_values_match_old_entry() {
    for name in [
        "bitwise_left_shift",
        "bitwise_right_shift_arithmetic",
        "bitwise_right_shift",
    ] {
        for shape in [Shape::Direct, Shape::Nested, Shape::IfThen, Shape::IfElse] {
            let program = fixture(name, shape);
            let input = batch(
                &program,
                vec![Some(7), None, Some(-1)],
                vec![Some(true), Some(false), Some(false)],
            );
            let binding = bound();
            let control = runtime_control(Some(&binding));
            for sparse in [false, true] {
                let selection = if sparse {
                    Selection::try_sparse(3, &[0, 2]).unwrap()
                } else {
                    Selection::all(3)
                };
                let mut original = CompiledExpressionInstance::try_new(
                    program.unit(root(&program).unit).unwrap().clone(),
                    root(&program).root,
                    &Control,
                )
                .unwrap();
                let expected = original
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &input,
                            source: None,
                        },
                        ScalarInvocationScope::new(selection, selection),
                        Activation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                let mut actual = CompiledExpressionInstance::try_new(
                    program.unit(root(&program).unit).unwrap().clone(),
                    root(&program).root,
                    &control,
                )
                .unwrap();
                let mut journal = RuntimeScalarMemoryJournal::default();
                let result = actual
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &input,
                            source: None,
                        },
                        ScalarInvocationScope::new(selection, selection),
                        Activation::Activated,
                        &control,
                        &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                    )
                    .map_err(ExecutionFailure::from)
                    .unwrap();
                assert_eq!(result.values().to_data(), expected.values().to_data());
                assert!(journal.body.settlement.is_some());
                assert_eq!(journal.body.stopped, Some(Ok(())));
            }
        }
    }
}
#[test]
fn runtime_scalar_memory_required_helper_missing_binding_fails_root_without_scalar_call() {
    let program = fixture("bitwise_left_shift", Shape::Direct);
    let input = batch(&program, vec![Some(7)], vec![Some(true)]);
    let control = runtime_control(None);
    let mut actual = CompiledExpressionInstance::try_new(
        program.unit(root(&program).unit).unwrap().clone(),
        root(&program).root,
        &control,
    )
    .unwrap();
    let error = evaluate_all(&mut actual, root(&program), &input, &control).unwrap_err();
    assert_eq!(
        error.cause(),
        &ExecutionFailureCause::RuntimeScalarMemory(RuntimeScalarMemoryRefusal::MissingQueryMemory)
    );
    let binding = bound();
    let funded = runtime_control(Some(&binding));
    assert!(matches!(
        evaluate_all(&mut actual, root(&program), &input, &funded)
            .unwrap_err()
            .cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
    ));
}
#[test]
fn runtime_scalar_memory_if_undemanded_validate_only_and_strict_null_bypass_remain_original() {
    for (shape, flags, values) in [
        (Shape::IfThen, vec![Some(false); 3], vec![Some(7); 3]),
        (Shape::IfElse, vec![Some(true); 3], vec![Some(7); 3]),
        (Shape::Direct, vec![Some(true); 3], vec![None; 3]),
    ] {
        let program = fixture("bitwise_left_shift", shape);
        let input = batch(&program, values, flags);
        let control = runtime_control(None);
        let mut actual = CompiledExpressionInstance::try_new(
            program.unit(root(&program).unit).unwrap().clone(),
            root(&program).root,
            &control,
        )
        .unwrap();
        evaluate_all(&mut actual, root(&program), &input, &control).unwrap();
    }
    let program = fixture("bitwise_left_shift", Shape::Direct);
    let input = batch(&program, vec![], vec![]);
    let control = runtime_control(None);
    let mut actual = CompiledExpressionInstance::try_new(
        program.unit(root(&program).unit).unwrap().clone(),
        root(&program).root,
        &control,
    )
    .unwrap();
    evaluate_all(&mut actual, root(&program), &input, &control).unwrap();
    // The original strict-zero-row leaf bypass remains outside this invoice.
    let mut actual = CompiledExpressionInstance::try_new(
        program.unit(root(&program).unit).unwrap().clone(),
        root(&program).root,
        &control,
    )
    .unwrap();
    let mut journal = RuntimeScalarMemoryJournal::default();
    let result = actual
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
            Activation::Activated,
            &control,
            &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
        )
        .map_err(ExecutionFailure::from);
    assert_eq!(result.unwrap().values().len(), 0);
    assert!(journal.body.settlement.is_none());
}
#[test]
fn runtime_scalar_memory_direct_ordered_filter_uses_same_loan_and_first_host_failure() {
    let program = fixture("bitwise_left_shift", Shape::Filter);
    let input = batch(
        &program,
        vec![Some(7), Some(3), None],
        vec![Some(true), Some(false), Some(true)],
    );
    let mut legacy = CompiledFilterConjunctionInstance::try_new_with_allocator(
        &predicate_units(&program),
        &Control,
        None,
    )
    .unwrap();
    let expected = legacy
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            Selection::all(3),
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    for funded in [false, true] {
        let binding = bound();
        let control = runtime_control(funded.then_some(&binding));
        let mut actual = CompiledFilterConjunctionInstance::try_new_with_allocator(
            &predicate_units(&program),
            &control,
            None,
        )
        .unwrap();
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = actual
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                Selection::all(3),
                &control,
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
            )
            .map_err(ExecutionFailure::from);
        if funded {
            assert_eq!(
                result
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            );
            assert!(journal.body.settlement.is_some());
        } else {
            assert_eq!(
                result.unwrap_err().cause(),
                &ExecutionFailureCause::RuntimeScalarMemory(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory
                )
            );
            let mut journal = RuntimeScalarMemoryJournal::default();
            assert!(matches!(
                actual
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &input,
                            source: None
                        },
                        Selection::all(3),
                        &control,
                        &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                    )
                    .map_err(ExecutionFailure::from)
                    .unwrap_err()
                    .cause(),
                ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
            ));
        }
    }
}

#[test]
fn runtime_scalar_memory_nine_narrow_frames_and_direct_filters_use_original_scope() {
    for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
        for name in [
            "bitwise_left_shift",
            "bitwise_right_shift_arithmetic",
            "bitwise_right_shift",
        ] {
            for shape in [Shape::Direct, Shape::Nested, Shape::IfThen, Shape::IfElse] {
                let program = fixture_typed(name, shape, ty.clone());
                let input = batch(
                    &program,
                    vec![Some(7), None, Some(-1)],
                    vec![Some(true), Some(false), Some(false)],
                );
                let binding = bound();
                let control = runtime_control(Some(&binding));
                for sparse in [false, true] {
                    let selection = if sparse {
                        Selection::try_sparse(3, &[0, 2]).unwrap()
                    } else {
                        Selection::all(3)
                    };
                    let mut original = CompiledExpressionInstance::try_new(
                        program.unit(root(&program).unit).unwrap().clone(),
                        root(&program).root,
                        &Control,
                    )
                    .unwrap();
                    let expected = original
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &input,
                                source: None,
                            },
                            ScalarInvocationScope::new(selection, selection),
                            Activation::Activated,
                            &Control,
                            &mut PassThroughCallHost,
                        )
                        .unwrap();
                    let mut actual = CompiledExpressionInstance::try_new(
                        program.unit(root(&program).unit).unwrap().clone(),
                        root(&program).root,
                        &control,
                    )
                    .unwrap();
                    let mut journal = RuntimeScalarMemoryJournal::default();
                    let result = actual
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &input,
                                source: None,
                            },
                            ScalarInvocationScope::new(selection, selection),
                            Activation::Activated,
                            &control,
                            &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                        )
                        .map_err(ExecutionFailure::from)
                        .unwrap();
                    assert_eq!(result.values().data_type(), &ty);
                    assert_eq!(result.values().to_data(), expected.values().to_data());
                    assert!(journal.body.settlement.is_some());
                    assert_eq!(journal.body.settlement.as_ref().unwrap().debt, 0);
                }
            }
            let program = fixture_typed(name, Shape::Filter, ty.clone());
            let input = batch(
                &program,
                vec![Some(7), Some(3), None],
                vec![Some(true), Some(false), Some(true)],
            );
            let mut original = CompiledFilterConjunctionInstance::try_new_with_allocator(
                &predicate_units(&program),
                &Control,
                None,
            )
            .unwrap();
            let expected = original
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    Selection::all(3),
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            for funded in [false, true] {
                let binding = bound();
                let control = runtime_control(funded.then_some(&binding));
                let mut actual = CompiledFilterConjunctionInstance::try_new_with_allocator(
                    &predicate_units(&program),
                    &control,
                    None,
                )
                .unwrap();
                let mut journal = RuntimeScalarMemoryJournal::default();
                let result = actual
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &input,
                            source: None,
                        },
                        Selection::all(3),
                        &control,
                        &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                    )
                    .map_err(ExecutionFailure::from);
                if funded {
                    assert_eq!(result.unwrap().to_data(), expected.to_data());
                    assert!(journal.body.settlement.is_some());
                } else {
                    assert_eq!(
                        result.unwrap_err().cause(),
                        &ExecutionFailureCause::RuntimeScalarMemory(
                            RuntimeScalarMemoryRefusal::MissingQueryMemory
                        )
                    );
                    let mut journal = RuntimeScalarMemoryJournal::default();
                    assert!(matches!(
                        actual
                            .evaluate(
                                EvaluationInput::Batch {
                                    batch: &input,
                                    source: None
                                },
                                Selection::all(3),
                                &control,
                                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                            )
                            .map_err(ExecutionFailure::from)
                            .unwrap_err()
                            .cause(),
                        ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
                    ));
                }
            }
        }
    }
}

#[test]
fn runtime_scalar_memory_narrow_roots_keep_missing_latch_and_strict_bypass_boundary() {
    for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
        for name in [
            "bitwise_left_shift",
            "bitwise_right_shift_arithmetic",
            "bitwise_right_shift",
        ] {
            let program = fixture_typed(name, Shape::Direct, ty.clone());
            let input = batch(&program, vec![Some(7)], vec![Some(true)]);
            let missing = runtime_control(None);
            let mut actual = CompiledExpressionInstance::try_new(
                program.unit(root(&program).unit).unwrap().clone(),
                root(&program).root,
                &missing,
            )
            .unwrap();
            assert_eq!(
                evaluate_all(&mut actual, root(&program), &input, &missing)
                    .unwrap_err()
                    .cause(),
                &ExecutionFailureCause::RuntimeScalarMemory(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory
                )
            );
            let binding = bound();
            let funded = runtime_control(Some(&binding));
            assert!(matches!(
                evaluate_all(&mut actual, root(&program), &input, &funded)
                    .unwrap_err()
                    .cause(),
                ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
            ));
            for rows in [0, 3] {
                let input = batch(&program, vec![None; rows], vec![Some(true); rows]);
                let mut bypass = CompiledExpressionInstance::try_new(
                    program.unit(root(&program).unit).unwrap().clone(),
                    root(&program).root,
                    &missing,
                )
                .unwrap();
                let values = evaluate_all(&mut bypass, root(&program), &input, &missing).unwrap();
                assert_eq!(values.data_type(), &ty);
                assert_eq!(values.len(), rows);
                assert_eq!(values.null_count(), rows);
            }
        }
    }
}

#[test]
fn runtime_scalar_memory_bitwise_bigint_frames_and_filters_keep_same_scope_and_refusal() {
    let names = ["bitwise_and", "bitwise_or", "bitwise_xor", "bitwise_not"];
    for name in names {
        for shape in [
            Shape::Direct,
            Shape::Nested,
            Shape::IfThen,
            Shape::IfElse,
            Shape::Filter,
        ] {
            let program = fixture_family(name, shape, DataType::Int64, true);
            let input = RecordBatch::try_new(
                program.graph().nodes()[1].output_layout().schema().clone(),
                vec![
                    Arc::new(Int64Array::from(vec![Some(-7), None, Some(-7), Some(-7)]))
                        as ArrayRef,
                    Arc::new(Int64Array::from(vec![Some(3), Some(3), None, Some(3)])) as ArrayRef,
                    Arc::new(BooleanArray::from(vec![
                        Some(true),
                        Some(true),
                        Some(false),
                        Some(false),
                    ])) as ArrayRef,
                ],
            )
            .unwrap();
            let binding = bound();
            let funded = runtime_control(Some(&binding));
            if matches!(shape, Shape::Filter) {
                assert!(matches!(
                    program.graph().nodes()[2].kind(),
                    novarocks_local_program::ProgramNodeKind::Filter { .. }
                ));
                let mut original = CompiledFilterConjunctionInstance::try_new_with_allocator(
                    &predicate_units(&program),
                    &Control,
                    None,
                )
                .unwrap();
                let expected = original
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &input,
                            source: None,
                        },
                        Selection::all(4),
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                for has_binding in [false, true] {
                    let control = runtime_control(has_binding.then_some(&binding));
                    let mut actual = CompiledFilterConjunctionInstance::try_new_with_allocator(
                        &predicate_units(&program),
                        &control,
                        None,
                    )
                    .unwrap();
                    let mut journal = RuntimeScalarMemoryJournal::default();
                    let result = actual
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &input,
                                source: None,
                            },
                            Selection::all(4),
                            &control,
                            &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                        )
                        .map_err(ExecutionFailure::from);
                    if has_binding {
                        assert_eq!(result.unwrap().to_data(), expected.to_data());
                        assert_eq!(journal.body.settlement.unwrap().debt, 0);
                    } else {
                        assert_eq!(
                            result.unwrap_err().cause(),
                            &ExecutionFailureCause::RuntimeScalarMemory(
                                RuntimeScalarMemoryRefusal::MissingQueryMemory
                            )
                        );
                        let mut retry = RuntimeScalarMemoryJournal::default();
                        assert!(matches!(
                            actual
                                .evaluate(
                                    EvaluationInput::Batch {
                                        batch: &input,
                                        source: None
                                    },
                                    Selection::all(4),
                                    &funded,
                                    &mut RuntimeScalarMemoryScope::new(&funded, &mut retry),
                                )
                                .map_err(ExecutionFailure::from)
                                .unwrap_err()
                                .cause(),
                            ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
                        ));
                    }
                }
            } else {
                for sparse in [false, true] {
                    let selection = if sparse {
                        Selection::try_sparse(4, &[0, 3]).unwrap()
                    } else {
                        Selection::all(4)
                    };
                    let mut original = CompiledExpressionInstance::try_new(
                        program.unit(root(&program).unit).unwrap().clone(),
                        root(&program).root,
                        &Control,
                    )
                    .unwrap();
                    let expected = original
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &input,
                                source: None,
                            },
                            ScalarInvocationScope::new(selection, selection),
                            Activation::Activated,
                            &Control,
                            &mut PassThroughCallHost,
                        )
                        .unwrap();
                    let mut actual = CompiledExpressionInstance::try_new(
                        program.unit(root(&program).unit).unwrap().clone(),
                        root(&program).root,
                        &funded,
                    )
                    .unwrap();
                    let mut journal = RuntimeScalarMemoryJournal::default();
                    let result = actual
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &input,
                                source: None,
                            },
                            ScalarInvocationScope::new(selection, selection),
                            Activation::Activated,
                            &funded,
                            &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
                        )
                        .map_err(ExecutionFailure::from)
                        .unwrap();
                    assert_eq!(result.values().data_type(), &DataType::Int64);
                    assert_eq!(result.values().to_data(), expected.values().to_data());
                    assert_eq!(journal.body.settlement.as_ref().unwrap().debt, 0);
                }
                let missing = runtime_control(None);
                let mut actual = CompiledExpressionInstance::try_new(
                    program.unit(root(&program).unit).unwrap().clone(),
                    root(&program).root,
                    &missing,
                )
                .unwrap();
                assert_eq!(
                    evaluate_all(&mut actual, root(&program), &input, &missing)
                        .unwrap_err()
                        .cause(),
                    &ExecutionFailureCause::RuntimeScalarMemory(
                        RuntimeScalarMemoryRefusal::MissingQueryMemory
                    )
                );
                assert!(matches!(
                    evaluate_all(&mut actual, root(&program), &input, &funded)
                        .unwrap_err()
                        .cause(),
                    ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
                ));
                if matches!(shape, Shape::Direct) {
                    for n in [0, 4] {
                        let input = RecordBatch::try_new(
                            program.graph().nodes()[1].output_layout().schema().clone(),
                            vec![
                                Arc::new(Int64Array::from(vec![None; n])) as ArrayRef,
                                Arc::new(Int64Array::from(vec![None; n])) as ArrayRef,
                                Arc::new(BooleanArray::from(vec![Some(true); n])) as ArrayRef,
                            ],
                        )
                        .unwrap();
                        let mut bypass = CompiledExpressionInstance::try_new(
                            program.unit(root(&program).unit).unwrap().clone(),
                            root(&program).root,
                            &missing,
                        )
                        .unwrap();
                        let output =
                            evaluate_all(&mut bypass, root(&program), &input, &missing).unwrap();
                        assert_eq!(output.data_type(), &DataType::Int64);
                        assert_eq!(output.len(), n);
                        assert_eq!(output.null_count(), n);
                    }
                }
            }
        }
    }
}
