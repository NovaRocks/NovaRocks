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
    numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_declared_fields,
};
use super::*;
use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all};
use crate::runtime::{
    mem_tracker::MemTracker,
    query_memory::QueryMemoryBinding,
    runtime_state::RuntimeState,
    scalar_memory::{
        RuntimeScalarEvaluationFailure, RuntimeScalarMemoryJournal, RuntimeScalarMemoryRefusal,
        RuntimeScalarMemoryScope,
    },
};
use arrow::{
    array::{Array, BooleanArray, Int64Array, StringArray},
    datatypes::Field,
};
use novarocks_functions::ScalarInvocationActivation as Activation;
use novarocks_local_program::{ProgramNodeKind, ProgramStateTemplate, StaticExprKind};
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TopUpPolicy};
use novarocks_sql::compiler::SqlPhysicalEmissionMode;
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
fn programs(sql: &str, mode: SqlPhysicalEmissionMode) -> Vec<Arc<LocalProgram>> {
    let fields = [
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ];
    let source = sql_source_with_declared_fields(sql, &fields, mode);
    compiler_results(&source, &installed_builtin_owner_catalogue())
        .into_values()
        .map(|p| Arc::new(p.expect("actual SQL package compiles")))
        .collect()
}
fn project(
    sql: &str,
    mode: SqlPhysicalEmissionMode,
) -> (Arc<LocalProgram>, ProgramExpressionRootSite, RecordBatch) {
    let mut selected = None;
    for program in programs(sql, mode) {
        let calls = program.checked().channels().expressions().resolved_calls();
        for call in calls.calls().values() {
            let ProgramStateTemplate::ScalarInvocation { scope, kernel } = call.state_template()
            else {
                continue;
            };
            if kernel.invocation_resource_profile().is_none() {
                continue;
            }
            let site = scope.root;
            let ProgramExpressionRootSite::Node { node, .. } = site else {
                continue;
            };
            let ProgramNodeKind::Project { input, .. } =
                program.graph().nodes()[node.index()].kind()
            else {
                continue;
            };
            let snapshot = calls.snapshot();
            let use_id = snapshot.bindings()[&site];
            let flow = &snapshot.flows()[&site.arena()];
            let arena = &snapshot.roots().arenas()[&site.arena()];
            let StaticExprKind::BoundCall { args, .. } =
                arena.node(flow.uses()[&use_id].definition).unwrap().kind()
            else {
                panic!("original JSON project root")
            };
            let key_slot = if let StaticExprKind::SlotId(slot) = arena.node(args[0]).unwrap().kind()
            {
                Some(*slot)
            } else {
                None
            };
            let StaticExprKind::SlotId(value_slot) = arena.node(args[1]).unwrap().kind() else {
                panic!("original JSON value channel")
            };
            let layout = program.graph().nodes()[input.index()].output_layout();
            let columns = layout
                .schema()
                .fields()
                .iter()
                .zip(layout.slots())
                .map(|(f, slot)| match f.data_type() {
                    DataType::Utf8 if key_slot == Some(*slot) => {
                        Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])) as ArrayRef
                    }
                    DataType::Utf8 if *slot == *value_slot => {
                        Arc::new(StringArray::from(vec!["[1,2]", "unused", "ordinary text"]))
                            as ArrayRef
                    }
                    DataType::Utf8 => Arc::new(StringArray::from(vec!["unused"; 3])) as ArrayRef,
                    DataType::Int64 => Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                    DataType::Boolean => Arc::new(BooleanArray::from(vec![true; 3])) as ArrayRef,
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
    selected.expect("actual installed JSON project")
}

#[test]
fn runtime_invocation_memory_actual_sql_compiler_project_common_helper_and_lossless_data() {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        for data in [false, true] {
            let sql = if data {
                "SELECT json_object([n], v) AS j FROM t"
            } else {
                "SELECT json_object(k, v) AS j FROM t"
            };
            let (program, site, input) = project(sql, mode);
            let binding = binding();
            let control = runtime_control(Some(&binding));
            let mut frame = CompiledExpressionInstance::try_new_with_allocator(
                program.clone(),
                site,
                &control,
                control.allocator(),
            )
            .unwrap();
            let mut journal = RuntimeScalarMemoryJournal::default();
            let result = frame.evaluate_runtime(
                &input,
                Selection::try_sparse(3, &[0, 2]).unwrap(),
                Activation::Activated,
                &control,
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
            );
            assert!(journal.preparation.as_ref().unwrap().settlement.is_some());
            assert!(journal.body.settlement.is_some());
            if data {
                let Err(RuntimeScalarEvaluationFailure::Data(data)) = result else {
                    panic!("original complex key Data")
                };
                assert_eq!(
                    data.message(),
                    "json_object key does not support complex type"
                );
                assert_eq!(data.source_len(), 2);
                assert_eq!(data.source_row(0), Some(0));
                assert_eq!(data.source_row(1), Some(2));
                assert!(matches!(
                    frame.evaluate_runtime(
                        &input,
                        Selection::all(3),
                        Activation::Activated,
                        &control,
                        &mut RuntimeScalarMemoryScope::new(&control, &mut journal)
                    ),
                    Err(RuntimeScalarEvaluationFailure::Kernel(
                        KernelFailure::InstanceFailed
                    ))
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
                    program.clone(),
                    site,
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
                    program.clone(),
                    site,
                    &missing,
                    missing.allocator(),
                )
                .unwrap();
                let mut journal = RuntimeScalarMemoryJournal::default();
                let output = actual
                    .evaluate_runtime(
                        &empty,
                        Selection::all(0),
                        Activation::ValidateOnly,
                        &missing,
                        &mut RuntimeScalarMemoryScope::new(&missing, &mut journal),
                    )
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
    let (program, site, input) = project(
        "SELECT json_object(k, v) AS j FROM t",
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
    for rebind in [false, true] {
        let binding = binding();
        let mut control = runtime_control(rebind.then_some(&binding));
        let mut frame = CompiledExpressionInstance::try_new_with_allocator(
            program.clone(),
            site,
            &control,
            control.allocator(),
        )
        .unwrap();
        if rebind {
            control.bind_mem_tracker(MemTracker::new_root("rebound-original-controller"));
        }
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = frame.evaluate_runtime(
            &input,
            Selection::all(3),
            Activation::Activated,
            &control,
            &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
        );
        if rebind {
            assert!(matches!(
                result,
                Err(RuntimeScalarEvaluationFailure::Source(
                    novarocks_functions::ScalarResourceError::Invariant(_)
                ))
            ));
            assert!(journal.preparation.is_some());
        } else {
            assert!(matches!(
                result,
                Err(RuntimeScalarEvaluationFailure::Host(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory
                ))
            ));
        }
        assert!(journal.body.settlement.is_none());
        assert!(matches!(
            frame.evaluate_runtime(
                &input,
                Selection::all(3),
                Activation::Activated,
                &control,
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal)
            ),
            Err(RuntimeScalarEvaluationFailure::Kernel(
                KernelFailure::InstanceFailed
            ))
        ));
    }
}

#[test]
fn runtime_invocation_memory_actual_sql_ordered_filter_or_scan_suppresses_and_maps_selected_data() {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        let mut selected = None;
        for program in programs(
            "SELECT n FROM t WHERE flag AND json_object([n], v) IS NULL",
            mode,
        ) {
            for (index, node) in program.graph().nodes().iter().enumerate() {
                let predicates = match node.kind() {
                    ProgramNodeKind::Filter { predicates, .. } => predicates.as_ref(),
                    ProgramNodeKind::Scan { residuals, .. } => residuals.as_slice(),
                    _ => continue,
                };
                if predicates.len() != 2 {
                    continue;
                }
                let node_id = ProgramNodeId::new(index);
                let scan = matches!(node.kind(), ProgramNodeKind::Scan { .. });
                let site = ProgramExpressionRootSite::Node {
                    node: node_id,
                    role: if scan {
                        novarocks_local_program::ProgramNodeExpressionRole::ScanResidual {
                            predicate: 0,
                        }
                    } else {
                        novarocks_local_program::ProgramNodeExpressionRole::FilterPredicate {
                            predicate: 0,
                        }
                    },
                };
                let ProgramRootInput::Layout { node: input, role } =
                    root_input_layout(program.graph(), site).unwrap()
                else {
                    panic!("actual ordered input")
                };
                let layout = program
                    .checked()
                    .channels()
                    .channel_layout(input, role)
                    .unwrap();
                assert!(
                    selected
                        .replace((program.clone(), node_id, layout.clone()))
                        .is_none(),
                    "one actual ordered pair"
                );
            }
        }
        let (program, node, layout) =
            selected.expect("actual SQL ordered two-predicate Filter/Scan");
        for demanded in [false, true] {
            let binding = binding();
            let control = runtime_control(demanded.then_some(&binding));
            let columns = layout
                .schema()
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
            let input = RecordBatch::try_new(layout.schema().clone(), columns).unwrap();
            let mut actual = CompiledFilterConjunctionInstance::try_new_with_allocator(
                program.clone(),
                node,
                &control,
                control.allocator(),
            )
            .unwrap();
            let mut journal = RuntimeScalarMemoryJournal::default();
            let result = actual.evaluate_required_runtime(
                &input,
                Selection::all(3),
                &control,
                &mut RuntimeScalarMemoryScope::new(&control, &mut journal),
            );
            if demanded {
                use crate::runtime::fragment::ExecutionFailureCause;
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
