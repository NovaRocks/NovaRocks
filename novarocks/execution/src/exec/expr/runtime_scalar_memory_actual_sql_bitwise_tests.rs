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
//! Actual SQL author, package, installed scalar and original runtime consumers.
//! Package compilation here is not a Native wire witness; Server and native
//! acceptance separately exercise the original v2 codec/interpreter.
use super::compiled_program::{CompiledExpressionInstance, CompiledFilterConjunctionInstance};
use super::filter_conjunction_actual_sql_compiler_tests::{compiler_results, source_packages};
use super::ndv_filter_actual_sql_source_tests::sql_source_with_columns;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all};
use crate::runtime::{
    fragment::ExecutionFailureCause,
    query_memory::QueryMemoryBinding,
    runtime_state::RuntimeState,
    scalar_memory::{
        RuntimeScalarMemoryJournal, RuntimeScalarMemoryRefusal, RuntimeScalarMemoryScope,
    },
};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_functions::PureKernelAbi;
use novarocks_functions::{FunctionKind, KernelFailure, ScalarInvocationActivation, Selection};
use novarocks_local_program::{
    ProgramExpressionRootSite, ProgramNodeKind, ProgramRootInput, ProgramStateTemplate,
    root_input_layout,
};
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TopUpPolicy};
use novarocks_physical_plan::{
    ExprKind, FunctionArgumentType, PhysicalCallDefinition, StaticFunctionArgument,
};
use novarocks_sql::compiler::SqlPhysicalEmissionMode;
use novarocks_type_contract::ValueLogicalType;
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use std::{collections::BTreeMap, sync::Arc};

const NAMES: [&str; 4] = ["bitand", "bitor", "bitxor", "bitnot"];
fn arity(name: &str) -> usize {
    if name == "bitnot" { 1 } else { 2 }
}
fn value(name: &str) -> i64 {
    match name {
        "bitand" => 1,
        "bitor" => -5,
        "bitxor" => -6,
        "bitnot" => 6,
        _ => panic!("exact SQL bitwise fixture"),
    }
}
fn array(ty: &DataType, values: &[Option<i64>]) -> ArrayRef {
    macro_rules! narrow {
        ($array:ty, $native:ty) => {
            Arc::new(<$array>::from(
                values
                    .iter()
                    .map(|v| v.map(|v| <$native>::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match ty {
        DataType::Int8 => narrow!(Int8Array, i8),
        DataType::Int16 => narrow!(Int16Array, i16),
        DataType::Int32 => narrow!(Int32Array, i32),
        DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
        _ => panic!("accurate SQL physical column"),
    }
}
fn rows(values: &ArrayRef) -> Vec<Option<i64>> {
    macro_rules! read {
        ($array:ty) => {
            values
                .as_any()
                .downcast_ref::<$array>()
                .unwrap()
                .iter()
                .map(|v| v.map(i64::from))
                .collect()
        };
    }
    match values.data_type() {
        DataType::Int8 => read!(Int8Array),
        DataType::Int16 => read!(Int16Array),
        DataType::Int32 => read!(Int32Array),
        DataType::Int64 => read!(Int64Array),
        _ => panic!("original bitwise result width"),
    }
}
fn binding() -> QueryMemoryBinding {
    let mut config = AuthorityConfig::new(1 << 20, 1 << 19, 1 << 19);
    config.max_accounts = 16;
    config.max_active_owners = 8;
    config.metadata_budget_bytes = 1 << 16;
    config.top_up = TopUpPolicy::uniform(1024);
    let authority = Arc::new(MemoryAuthority::new(config).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 306), AttemptId::new(1).unwrap()).unwrap(),
        authority,
        account,
    )
    .unwrap()
}
fn control(binding: Option<&QueryMemoryBinding>) -> RuntimeKernelControl {
    let state = RuntimeState::default().with_query_memory(binding.cloned());
    let mut control = RuntimeKernelControl::new(state.error_state());
    control.bind_runtime_memory(&state);
    control
}

fn actual(name: &str, ty: DataType, filter: bool, guarded: bool) {
    let invocation = if arity(name) == 1 {
        format!("{name}(a)")
    } else {
        format!("{name}(a,b)")
    };
    let sql = if filter {
        format!("SELECT a FROM fixture.ndv_null_contract WHERE f AND {invocation} > a")
    } else if guarded {
        format!("SELECT IF(f,{invocation},a) AS r FROM fixture.ndv_null_contract")
    } else {
        format!("SELECT {invocation} AS r FROM fixture.ndv_null_contract")
    };
    let source = sql_source_with_columns(
        &sql,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        &[
            ("a", ty.clone()),
            ("b", ty.clone()),
            ("f", DataType::Boolean),
        ],
    );
    let metadata = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let id = metadata
        .definition(name, FunctionKind::Scalar)
        .unwrap()
        .binding_declaration()
        .unwrap()
        .function_id()
        .clone();
    let mut source_calls = 0;
    let mut column_arguments = BTreeMap::new();
    for fragment in source.plan().fragments().values() {
        for (expression_id, expression) in fragment.expressions().iter() {
            let ExprKind::FunctionCall { function, args } = &expression.kind else {
                continue;
            };
            if function.function_id != id {
                continue;
            }
            source_calls += 1;
            assert_eq!(args.len(), arity(name));
            assert_eq!(function.argument_types.len(), arity(name));
            assert_eq!(function.result_type.data_type, ty);
            assert!(function.result_type.nullable);
            assert_eq!(
                function.result_type.logical_type,
                ValueLogicalType::Physical
            );
            let request = fragment
                .call_requests()
                .get(PhysicalCallDefinition::Expression(*expression_id))
                .unwrap();
            assert_eq!(request.logical_argument_count, arity(name));
            assert_eq!(request.arguments.len(), arity(name));
            for (argument_ordinal, ((child, bound), requested)) in args
                .iter()
                .zip(&function.argument_types)
                .zip(&request.arguments)
                .enumerate()
            {
                let FunctionArgumentType::Value(bound) = bound else {
                    panic!("ordinary value channel")
                };
                let StaticFunctionArgument::Value {
                    value_type,
                    constant: None,
                } = requested
                else {
                    panic!("actual typed column, not folded constant")
                };
                assert_eq!(bound, value_type);
                assert_eq!(value_type.data_type, ty);
                assert_eq!(value_type.logical_type, ValueLogicalType::Physical);
                assert_eq!(&fragment.expressions().get(*child).unwrap().ty, bound);
                let ExprKind::Value(value) = fragment.expressions().get(*child).unwrap().kind
                else {
                    panic!("the original typed column remains a Value child")
                };
                let scan = fragment
                    .nodes()
                    .values()
                    .find_map(|node| match &node.kind {
                        novarocks_physical_plan::NodeKind::Scan {
                            provider_outputs, ..
                        } => provider_outputs
                            .iter()
                            .position(|(_, output)| *output == value),
                        _ => None,
                    })
                    .expect("actual ordered argument belongs to its original scan output");
                assert!(
                    column_arguments
                        .insert(format!("fixture_column_{scan}"), argument_ordinal)
                        .is_none()
                );
            }
        }
    }
    assert_eq!(
        source_calls, 1,
        "actual SQL must retain its scalar occurrence"
    );
    let packages = source_packages(&source);
    assert_eq!(packages.len(), source.plan().fragments().len());
    let mut found = None;
    for program in compiler_results(&source, &installed_builtin_owner_catalogue()).into_values() {
        let program = Arc::new(program.unwrap());
        for call in program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .calls()
            .values()
        {
            let ProgramStateTemplate::Scalar { scope, kernel } = call.state_template() else {
                continue;
            };
            if kernel.contract().function_id() != &id {
                continue;
            }
            assert_eq!(call.implementation().abi, PureKernelAbi::ScalarV1);
            assert!(kernel.invocation_resource_profile().is_some());
            assert_eq!(kernel.contract().value_argument_types().len(), arity(name));
            for argument in kernel.contract().value_argument_types() {
                assert_eq!(argument.data_type, ty);
                assert_eq!(argument.logical_type, ValueLogicalType::Physical);
            }
            assert_eq!(kernel.contract().result_type().data_type, ty);
            assert!(kernel.contract().result_type().nullable);
            assert_eq!(kernel.contract().call(), call.call_contract());
            assert_eq!(kernel.contract().effects(), call.call_contract().effects());
            assert!(found.replace((program.clone(), scope.root)).is_none());
        }
    }
    let (program, root) = found.expect("actual SQL installs exactly one bitwise ScalarV1");
    let ProgramRootInput::Layout { node, role } = root_input_layout(program.graph(), root).unwrap()
    else {
        panic!("actual scalar input layout")
    };
    let layout = program
        .checked()
        .channels()
        .channel_layout(node, role)
        .unwrap();
    let input = RecordBatch::try_new(
        layout.schema().clone(),
        layout
            .schema()
            .fields()
            .iter()
            .map(|field| {
                if field.data_type() == &DataType::Boolean {
                    assert!(filter || guarded);
                    Arc::new(BooleanArray::from(vec![
                        Some(true),
                        Some(true),
                        Some(false),
                        Some(false),
                    ])) as ArrayRef
                } else {
                    assert_eq!(field.data_type(), &ty);
                    match column_arguments[field.name()] {
                        0 => array(&ty, &[Some(-7), None, Some(-7), Some(-7)]),
                        1 => array(&ty, &[Some(3), Some(3), None, Some(3)]),
                        _ => panic!("actual bitwise argument ordinal"),
                    }
                }
            })
            .collect(),
    )
    .unwrap();
    let binding = binding();
    let funded = control(Some(&binding));
    for sparse in [false, true] {
        let selection = if sparse {
            Selection::try_sparse(4, &[0, 3]).unwrap()
        } else {
            Selection::all(4)
        };
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), root, &funded).unwrap();
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = frame
            .evaluate_runtime(
                &input,
                selection,
                ScalarInvocationActivation::Activated,
                &funded,
                &mut RuntimeScalarMemoryScope::new(&funded, &mut journal),
            )
            .unwrap();
        assert!(result.errors().is_empty());
        let ordinals = if sparse { vec![0, 3] } else { vec![0, 1, 2, 3] };
        if filter {
            assert_eq!(
                result
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                ordinals
                    .iter()
                    .map(|row| match row {
                        0 | 3 => Some(true),
                        1 => None,
                        2 if arity(name) == 2 => None,
                        2 => Some(true),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>()
            );
        } else {
            assert_eq!(result.values().data_type(), &ty);
            assert_eq!(
                rows(result.values()),
                ordinals
                    .iter()
                    .map(|row| {
                        if *row == 1 || (!guarded && arity(name) == 2 && *row == 2) {
                            None
                        } else if guarded && *row >= 2 {
                            Some(-7)
                        } else {
                            Some(value(name))
                        }
                    })
                    .collect::<Vec<_>>()
            );
        }
        let settlement = journal.body.settlement.as_ref().unwrap();
        assert_eq!(settlement.debt, 0);
        assert_eq!(settlement.next_step, Ok(()));
        assert_eq!(journal.body.stopped, Some(Ok(())));
    }
    let missing = control(None);
    let mut frame = CompiledExpressionInstance::try_new(program.clone(), root, &missing).unwrap();
    assert_eq!(
        evaluate_all(&mut frame, root, &input, &missing)
            .unwrap_err()
            .cause(),
        &ExecutionFailureCause::RuntimeScalarMemory(RuntimeScalarMemoryRefusal::MissingQueryMemory)
    );
    assert!(matches!(
        evaluate_all(&mut frame, root, &input, &funded)
            .unwrap_err()
            .cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
    ));
    if filter {
        let ProgramExpressionRootSite::Node { node, .. } = root else {
            panic!("actual ordered Filter")
        };
        assert!(matches!(
            program.graph().nodes()[node.index()].kind(),
            ProgramNodeKind::Filter { .. } | ProgramNodeKind::Scan { .. }
        ));
        for has_binding in [false, true] {
            let control = control(has_binding.then_some(&binding));
            let mut filter =
                CompiledFilterConjunctionInstance::try_new(program.clone(), node, &control)
                    .unwrap();
            let mut journal = RuntimeScalarMemoryJournal::default();
            let result = filter.evaluate_required_runtime(
                &input,
                Selection::all(4),
                &control,
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
            );
            if has_binding {
                assert_eq!(
                    result
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(true), Some(false), Some(false), Some(false)]
                );
                assert_eq!(journal.body.settlement.unwrap().debt, 0);
            } else {
                assert_eq!(
                    result.unwrap_err().cause(),
                    &ExecutionFailureCause::RuntimeScalarMemory(
                        RuntimeScalarMemoryRefusal::MissingQueryMemory
                    )
                );
            }
        }
    }
}
#[test]
fn runtime_scalar_memory_actual_sql_bitwise_all_16_project_and_guarded_sources() {
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for name in NAMES {
            for guarded in [false, true] {
                actual(name, ty.clone(), false, guarded);
            }
        }
    }
}
#[test]
fn runtime_scalar_memory_actual_sql_bitwise_all_16_ordered_predicate_sources() {
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for name in NAMES {
            actual(name, ty.clone(), true, false);
        }
    }
}
