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

use super::super::{
    filter_conjunction_actual_sql_compiler_tests::compiler_results,
    numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue,
    sql_test_source::sql_source_with_declared_fields,
};
use super::*;
use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all};
use crate::runtime::{
    mem_tracker::MemTracker,
    query_memory::QueryMemoryBinding,
    runtime_state::RuntimeState,
    scalar_memory::{
        RuntimeScalarMemoryJournal, RuntimeScalarMemoryRefusal, RuntimeScalarMemoryScope,
    },
    task::{ExecutionFailure, ExecutionFailureCause},
};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::{Array, BooleanArray, Int64Array, StringArray},
    datatypes::Field,
};
use novarocks_expression::EvaluationInput;
use novarocks_expression::{
    StaticExprKind,
    evaluator_view::{UnitStateTemplate, state_template},
};
use novarocks_functions::ScalarInvocationActivation as Activation;
use novarocks_functions::ScalarInvocationScope;
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef, ProgramUnitId};
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TopUpPolicy};

use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};

fn binding() -> QueryMemoryBinding {
    let mut cfg = AuthorityConfig::new(256 << 20, 128 << 20, 128 << 20);
    cfg.max_accounts = 32;
    cfg.max_active_owners = 16;
    cfg.metadata_budget_bytes = 1 << 20;
    cfg.top_up = TopUpPolicy::uniform(1024);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 309), AttemptId::new(1).unwrap()).unwrap(),
        authority,
        account,
    )
    .unwrap()
}
fn runtime_control(memory: Option<&QueryMemoryBinding>) -> RuntimeKernelControl {
    let state = RuntimeState::default().with_query_memory(memory.cloned());
    let mut control = RuntimeKernelControl::new(state.error_state());
    control.bind_runtime_memory(&state);
    control.bind_mem_tracker(MemTracker::new_root("actual-json-sql"));
    control
}
fn programs(sql: &str) -> Vec<Arc<LocalProgram>> {
    let fields = [
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ];
    let source = sql_source_with_declared_fields(sql, &fields);
    compiler_results(&source, &installed_builtin_owner_catalogue())
        .into_values()
        .map(|p| Arc::new(p.expect("actual SQL package compiles")))
        .collect()
}
fn project(sql: &str) -> (Arc<LocalProgram>, ProgramRootRef, RecordBatch) {
    let mut selected = None;
    for program in programs(sql) {
        for (unit_ordinal, unit) in program.units().iter().enumerate() {
            for &occurrence in unit.calls().keys() {
                let Some(UnitStateTemplate::ScalarInvocation { scope, kernel }) =
                    state_template(unit, occurrence)
                else {
                    continue;
                };
                if kernel.invocation_resource_profile().is_none() {
                    continue;
                }
                let site = ProgramRootRef {
                    unit: ProgramUnitId::new(unit_ordinal),
                    root: scope.root,
                };
                for node in program.graph().nodes() {
                    let ProgramNodeKind::Project { input, exprs, .. } = node.kind() else {
                        continue;
                    };
                    if !exprs.contains(&site) {
                        continue;
                    }
                    let root = unit.root(site.root).unwrap();
                    let use_ = &unit.control_flow().uses()[&root.use_id];
                    let StaticExprKind::BoundCall { .. } =
                        unit.definitions().node(root.definition).unwrap().kind()
                    else {
                        panic!("original JSON project root")
                    };
                    let key_ordinal = unit
                        .input(use_.arguments[0])
                        .map(|binding| binding.ordinal as usize);
                    let value_ordinal = unit
                        .input(use_.arguments[1])
                        .expect("original JSON value channel")
                        .ordinal as usize;
                    let layout = program.graph().nodes()[input.index()].output_layout();
                    let columns = layout
                        .schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(ordinal, field)| match field.data_type() {
                            DataType::Utf8 if key_ordinal == Some(ordinal) => {
                                Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]))
                                    as ArrayRef
                            }
                            DataType::Utf8 if ordinal == value_ordinal => {
                                Arc::new(StringArray::from(vec![
                                    "[1,2]",
                                    "unused",
                                    "ordinary text",
                                ])) as ArrayRef
                            }
                            DataType::Utf8 => {
                                Arc::new(StringArray::from(vec!["unused"; 3])) as ArrayRef
                            }
                            DataType::Int64 => {
                                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef
                            }
                            DataType::Boolean => {
                                Arc::new(BooleanArray::from(vec![true; 3])) as ArrayRef
                            }
                            other => panic!("explicit SQL fixture field {other:?}"),
                        })
                        .collect();
                    let batch = RecordBatch::try_new(layout.schema().clone(), columns).unwrap();
                    assert!(
                        selected.replace((program.clone(), site, batch)).is_none(),
                        "one actual JSON project root"
                    );
                }
            }
        }
    }
    selected.expect("actual installed JSON project")
}

#[test]
fn runtime_invocation_memory_actual_sql_compiler_project_common_helper_and_lossless_data() {
    {
        for data in [false, true] {
            let sql = if data {
                "SELECT json_object([n], v) AS j FROM t"
            } else {
                "SELECT json_object(k, v) AS j FROM t"
            };
            let (program, site, input) = project(sql);
            let binding = binding();
            let control = runtime_control(Some(&binding));
            let mut frame = CompiledExpressionInstance::try_new_with_allocator(
                program.unit(site.unit).unwrap().clone(),
                site.root,
                &control,
                control.allocator(),
            )
            .unwrap();
            let mut journal = RuntimeScalarMemoryJournal::default();
            let result = frame
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    {
                        let selection = Selection::try_sparse(3, &[0, 2]).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    Activation::Activated,
                    &control,
                    &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                )
                .map_err(ExecutionFailure::from);
            assert!(journal.preparation.as_ref().unwrap().settlement.is_some());
            assert!(journal.body.settlement.is_some());
            if data {
                let error = result.unwrap_err();
                let ExecutionFailureCause::ScalarInvocationData(data) = error.cause() else {
                    panic!("original complex key Data: {error:?}")
                };
                assert_eq!(
                    data.message(),
                    "json_object key does not support complex type"
                );
                assert_eq!(data.source_len(), 2);
                assert_eq!(data.source_row(0), Some(0));
                assert_eq!(data.source_row(1), Some(2));
                let replay = frame
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &input,
                            source: None,
                        },
                        ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                        Activation::Activated,
                        &control,
                        &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
                    )
                    .map_err(ExecutionFailure::from);
                assert!(matches!(
                    replay,
                    Err(error) if matches!(error.cause(), ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed))
                ));
            } else {
                let output = result.unwrap();
                let strings = output
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(strings.value(0), "{\"a\": [1, 2]}");
                assert_eq!(strings.value(1), "{\"b\": \"ordinary text\"}");
                drop(output);
                let mut actual = CompiledExpressionInstance::try_new_with_allocator(
                    program.unit(site.unit).unwrap().clone(),
                    site.root,
                    &control,
                    control.allocator(),
                )
                .unwrap();
                let output = evaluate_all(&mut actual, site, &input, &control).unwrap();
                let strings = output.as_any().downcast_ref::<StringArray>().unwrap();
                assert!(strings.is_null(1));
                let empty = input.slice(0, 0);
                let missing = runtime_control(None);
                let mut actual = CompiledExpressionInstance::try_new_with_allocator(
                    program.unit(site.unit).unwrap().clone(),
                    site.root,
                    &missing,
                    missing.allocator(),
                )
                .unwrap();
                let mut journal = RuntimeScalarMemoryJournal::default();
                let output = actual
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &empty,
                            source: None,
                        },
                        ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
                        Activation::ValidateOnly,
                        &missing,
                        &mut RuntimeScalarMemoryScope::new(&missing, &mut journal),
                    )
                    .map_err(ExecutionFailure::from)
                    .unwrap();
                assert!(output.values().is_empty());
                assert!(actual.instances.is_empty());
                assert!(journal.workset_bytes.is_none());
            }
        }
    }
}

#[test]
fn runtime_invocation_memory_actual_sql_frame_cached_host_rebind_and_missing_latch_before_body() {
    let (program, site, input) = project("SELECT json_object(k, v) AS j FROM t");
    for rebind in [false, true] {
        let binding = binding();
        let mut control = runtime_control(rebind.then_some(&binding));
        let mut frame = CompiledExpressionInstance::try_new_with_allocator(
            program.unit(site.unit).unwrap().clone(),
            site.root,
            &control,
            control.allocator(),
        )
        .unwrap();
        if rebind {
            control.bind_mem_tracker(MemTracker::new_root("rebound-original-controller"));
        }
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                Activation::Activated,
                &control,
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
            )
            .map_err(ExecutionFailure::from);
        if rebind {
            assert!(matches!(
                result,
                Err(error) if matches!(error.cause(), ExecutionFailureCause::RuntimeScalarSource(novarocks_functions::ScalarResourceError::Invariant(_)))
            ));
            assert!(journal.preparation.is_some());
        } else {
            assert!(matches!(
                result,
                Err(error) if matches!(error.cause(), ExecutionFailureCause::RuntimeScalarMemory(RuntimeScalarMemoryRefusal::MissingQueryMemory))
            ));
        }
        assert!(journal.body.settlement.is_none());
        let replay = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                Activation::Activated,
                &control,
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
            )
            .map_err(ExecutionFailure::from);
        assert!(matches!(
            replay,
            Err(error) if matches!(error.cause(), ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed))
        ));
    }
}

#[test]
fn runtime_invocation_memory_actual_sql_ordered_filter_or_scan_suppresses_and_maps_selected_data() {
    {
        let mut selected = None;
        for program in programs("SELECT n FROM t WHERE flag AND json_object([n], v) IS NULL") {
            for node in program.graph().nodes() {
                let predicates = match node.kind() {
                    ProgramNodeKind::Filter { predicates, .. } => predicates.as_ref(),
                    ProgramNodeKind::Scan { residuals, .. } => residuals.as_slice(),
                    _ => continue,
                };
                if predicates.len() != 2 {
                    continue;
                }
                let predicate_units = predicates
                    .iter()
                    .map(|site| (program.unit(site.unit).unwrap().clone(), site.root))
                    .collect::<Vec<_>>();
                let first = &predicate_units[0];
                let root = first.0.root(first.1).unwrap();
                let schema = first.0.scope(root.scope).unwrap().schema().clone();
                assert!(
                    selected.replace((predicate_units, schema)).is_none(),
                    "one actual ordered pair"
                );
            }
        }
        let (predicates, schema) = selected.expect("actual SQL ordered two-predicate Filter/Scan");
        for demanded in [false, true] {
            let binding = binding();
            let control = runtime_control(demanded.then_some(&binding));
            let columns = schema
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Boolean => Arc::new(BooleanArray::from(if demanded {
                        vec![false, true, true]
                    } else {
                        vec![false; 3]
                    })) as ArrayRef,
                    DataType::Int64 => Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                    DataType::Utf8 => Arc::new(StringArray::from(vec!["unused"; 3])) as ArrayRef,
                    other => panic!("explicit ordered fixture field {other:?}"),
                })
                .collect();
            let input = RecordBatch::try_new(schema.clone(), columns).unwrap();
            let mut actual = CompiledFilterConjunctionInstance::try_new_with_allocator(
                &predicates,
                &control,
                control.allocator(),
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
            if demanded {
                use crate::runtime::task::ExecutionFailureCause;
                let error = result.unwrap_err();
                let ExecutionFailureCause::ScalarInvocationData(data) = error.cause() else {
                    panic!("original selected full Data")
                };
                assert_eq!(
                    data.message(),
                    "json_object key does not support complex type"
                );
                assert_eq!(data.source_len(), 2);
                assert_eq!(data.source_row(0), Some(1));
                assert_eq!(data.source_row(1), Some(2));
                assert!(journal.body.settlement.is_some());
            } else {
                assert_eq!(
                    result
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(false); 3]
                );
                assert_eq!(actual.constructed_root_call_instances(1), 0);
                assert!(journal.workset_bytes.is_none());
            }
        }
    }
}
