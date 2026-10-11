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

use super::guarded_tests::{author, call, compile_checked_fragment_with_parameters};
use super::*;
use crate::exec::expr::pure_differential::HarnessControl as Control;
use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all};
use crate::runtime::{
    fragment::ExecutionFailureCause,
    query_memory::QueryMemoryBinding,
    runtime_state::{RuntimeErrorState, RuntimeState},
    scalar_memory::{
        RuntimeScalarEvaluationFailure, RuntimeScalarMemoryJournal, RuntimeScalarMemoryRefusal,
        RuntimeScalarMemoryScope,
    },
};
use arrow::{
    array::{BooleanArray, Int64Array},
    datatypes::DataType,
};
use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionKind, InstalledPureKernel, PureEngineFunctionCatalog,
    ScalarInvocationActivation as Activation,
};
use novarocks_local_program::ProgramNodeExpressionRole;
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority};
use novarocks_physical_plan::{
    ExprKind, FragmentBuilder, FragmentId, FragmentSink, LiteralValue, NodeId, ResultField,
    ResultPort, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{ControlShape, FunctionValueType, SemanticParameters};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};

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
        "bit_shift_left",
        "bit_shift_right",
        "bit_shift_right_logical",
        "if",
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
    let functions = functions();
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let integer = FunctionValueType::new(DataType::Int64, true);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let columns = [
        (ValueId::new(1), integer.clone()),
        (ValueId::new(2), integer.clone()),
        (ValueId::new(3), boolean.clone()),
    ];
    let mut builder = FragmentBuilder::new(FragmentId::new(301));
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = vec![];
    for (id, ty) in &columns {
        let expr = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr { node: input, expr },
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
        .add_expression(output, integer.clone(), ExprKind::Value(columns[0].0))
        .unwrap();
    let count = builder
        .add_expression(output, integer.clone(), ExprKind::Value(columns[1].0))
        .unwrap();
    let flag = if matches!(shape, Shape::IfThen | Shape::IfElse | Shape::Filter) {
        Some(
            builder
                .add_expression(output, boolean.clone(), ExprKind::Value(columns[2].0))
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
            vec![argument(&integer), argument(&integer)],
            ControlShape::Eager,
        ),
        vec![value, count],
    );
    let result = match shape {
        Shape::Direct | Shape::Filter => shifted,
        Shape::Nested => call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "bit_shift_right",
                vec![argument(&integer), argument(&integer)],
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
        let zero = builder
            .add_expression(
                output,
                integer.clone(),
                ExprKind::Literal(LiteralValue::Int64(0)),
            )
            .unwrap();
        let compare = builder
            .add_expression(
                output,
                boolean.clone(),
                ExprKind::Binary {
                    op: novarocks_physical_plan::BinaryOperator::Gt,
                    left: result,
                    right: zero,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: None,
                },
            )
            .unwrap();
        builder
            .add_filter(output, input, Box::from([flag.unwrap(), compare]))
            .unwrap();
        integer.clone()
    } else {
        builder
            .insert_value(ValueDef {
                id: result_value,
                ty: integer.clone(),
                origin: ValueOrigin::Expr {
                    node: output,
                    expr: result,
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
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            novarocks_physical_plan::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let result_port = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: fragment.nodes()[&output].output.clone(),
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
        fragment,
        &authors,
        result_port,
        SemanticParameters::try_new([]).unwrap(),
    )
}
fn root() -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(2),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    }
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
            Arc::new(Int64Array::from(values)),
            Arc::new(Int64Array::from(vec![1; rows])),
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
fn runtime_scalar_memory_original_frame_nested_and_selected_values_match_old_entry() {
    for name in [
        "bit_shift_left",
        "bit_shift_right",
        "bit_shift_right_logical",
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
                let mut original =
                    CompiledExpressionInstance::try_new(program.clone(), root(), &Control).unwrap();
                let expected = original
                    .evaluate_evaluation(&input, selection, Activation::Activated, &Control)
                    .unwrap();
                let mut actual =
                    CompiledExpressionInstance::try_new(program.clone(), root(), &control).unwrap();
                let mut journal = RuntimeScalarMemoryJournal::default();
                let result = actual
                    .evaluate_runtime(
                        &input,
                        selection,
                        Activation::Activated,
                        &control,
                        &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                    )
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
    let program = fixture("bit_shift_left", Shape::Direct);
    let input = batch(&program, vec![Some(7)], vec![Some(true)]);
    let control = runtime_control(None);
    let mut actual =
        CompiledExpressionInstance::try_new(program.clone(), root(), &control).unwrap();
    let error = evaluate_all(&mut actual, root(), &input, &control).unwrap_err();
    assert_eq!(
        error.cause(),
        &ExecutionFailureCause::RuntimeScalarMemory(RuntimeScalarMemoryRefusal::MissingQueryMemory)
    );
    assert!(actual.failed);
    let call = actual.instances.values_mut().next().unwrap();
    let super::scalar_invocation::CallInstance::Kernel(instance) = call else {
        panic!("original ScalarV1");
    };
    let values: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&counts),
    ];
    // The scalar owner remains uncalled, while its enclosing root is failed.
    instance
        .evaluate(Selection::all(1), &args, &control)
        .unwrap();
    let binding = bound();
    let funded = runtime_control(Some(&binding));
    assert!(matches!(
        evaluate_all(&mut actual, root(), &input, &funded)
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
        let program = fixture("bit_shift_left", shape);
        let input = batch(&program, values, flags);
        let control = runtime_control(None);
        let mut actual =
            CompiledExpressionInstance::try_new(program.clone(), root(), &control).unwrap();
        evaluate_all(&mut actual, root(), &input, &control).unwrap();
    }
    let program = fixture("bit_shift_left", Shape::Direct);
    let input = batch(&program, vec![], vec![]);
    let control = runtime_control(None);
    let mut actual =
        CompiledExpressionInstance::try_new(program.clone(), root(), &control).unwrap();
    evaluate_all(&mut actual, root(), &input, &control).unwrap();
    // The original strict-zero-row leaf bypass remains outside this invoice.
    let mut actual = CompiledExpressionInstance::try_new(program, root(), &control).unwrap();
    let mut journal = RuntimeScalarMemoryJournal::default();
    let result = actual.evaluate_runtime(
        &input,
        Selection::all(0),
        Activation::Activated,
        &control,
        &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
    );
    assert_eq!(result.unwrap().values().len(), 0);
    assert!(journal.body.settlement.is_none());
}
#[test]
fn runtime_scalar_memory_direct_ordered_filter_uses_same_loan_and_first_host_failure() {
    let program = fixture("bit_shift_left", Shape::Filter);
    let input = batch(
        &program,
        vec![Some(7), Some(3), None],
        vec![Some(true), Some(false), Some(true)],
    );
    let mut legacy = CompiledFilterConjunctionInstance::try_new(
        program.clone(),
        ProgramNodeId::new(2),
        &Control,
    )
    .unwrap();
    let expected = legacy
        .evaluate_required(&input, Selection::all(3), &Control)
        .unwrap();
    for funded in [false, true] {
        let binding = bound();
        let control = runtime_control(funded.then_some(&binding));
        let mut actual = CompiledFilterConjunctionInstance::try_new(
            program.clone(),
            ProgramNodeId::new(2),
            &control,
        )
        .unwrap();
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = actual.evaluate_required_runtime(
            &input,
            Selection::all(3),
            &control,
            &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
        );
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
                    .evaluate_required_runtime(
                        &input,
                        Selection::all(3),
                        &control,
                        &mut RuntimeScalarMemoryScope::new(&control, &mut journal)
                    )
                    .unwrap_err()
                    .cause(),
                ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
            ));
        }
    }
}
