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
//! Actual SQL roots, package, LocalCompiler and the ordered Filter consumer.
use super::compiled_program::CompiledFilterConjunctionInstance;
use super::filter_conjunction_actual_sql_compiler_tests::{
    compile_options, compiler_results, source_packages, validate_package,
};
use super::ndv_filter_actual_sql_source_tests::ndv_sql_source;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use crate::runtime::task::ExecutionFailureCause;
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array},
    record_batch::RecordBatch,
};
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_compiler::{FragmentCompileError, compile_fragment};
use novarocks_local_program::{
    DiagnosticSourceNodeId, LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind,
};
use novarocks_physical_plan::{FragmentId, NodeKind};
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("these Filter roots do not wait")
    }
}
const NATIVE: &str = "SELECT k FROM fixture.ndv_null_contract GROUP BY k\nHAVING ndv(v) = 0 AND approx_count_distinct(v) = 0 ORDER BY k;";
struct Fixture {
    source: SqlAuthoredPhysicalPlan,
    fragment: FragmentId,
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
}
fn fixture(sql: &str) -> Fixture {
    let source = ndv_sql_source(sql);
    let functions = installed_builtin_owner_catalogue();
    let mut results = compiler_results(&source, &functions);
    let mut found = None;
    for (&fragment, physical) in source.plan().fragments() {
        for source_node in physical.nodes().values() {
            if let NodeKind::Filter { predicates } = &source_node.kind {
                if predicates.len() < 2 {
                    continue;
                }
                let program = Arc::new(
                    results
                        .remove(&fragment)
                        .unwrap()
                        .expect("actual full Filter compiler"),
                );
                let node = program
                    .graph()
                    .nodes()
                    .iter()
                    .find(|node| {
                        node.physical_sources()
                            == [DiagnosticSourceNodeId::new(source_node.id.get())]
                            && matches!(node.kind(), ProgramNodeKind::Filter { .. })
                    })
                    .unwrap()
                    .local_id()
                    .unwrap();
                let ProgramNodeKind::Filter {
                    predicates: local, ..
                } = program.graph().nodes()[node.index()].kind()
                else {
                    unreachable!()
                };
                assert_eq!(local.len(), predicates.len());
                let snapshot = program
                    .checked()
                    .channels()
                    .expressions()
                    .resolved_calls()
                    .snapshot();
                for (ordinal, definition) in local.iter().enumerate() {
                    let site = ProgramExpressionRootSite::Node {
                        node,
                        role: ProgramNodeExpressionRole::FilterPredicate {
                            predicate: u32::try_from(ordinal).unwrap(),
                        },
                    };
                    let use_id = snapshot.bindings()[&site];
                    let invocation = &snapshot.flows()[&site.arena()].uses()[&use_id];
                    assert_eq!(invocation.definition, *definition);
                    assert_eq!(
                        invocation.context.demand,
                        novarocks_type_contract::EvaluationDemand::TruthOnly
                    );
                }
                assert!(found.replace((fragment, program, node)).is_none());
            }
        }
    }
    let (fragment, program, node) = found.expect("retained original multiple-predicate Filter");
    Fixture {
        source,
        fragment,
        program,
        node,
    }
}
// Feed the actual Filter input port, not a fabricated scan or physical type.
// Values are authored against each original Aggregate output identity.
fn batch(f: &Fixture, counts: &[i64], doubles: &[Option<f64>]) -> RecordBatch {
    let ProgramNodeKind::Filter { input, .. } = f.program.graph().nodes()[f.node.index()].kind()
    else {
        unreachable!()
    };
    let layout = f.program.graph().nodes()[input.index()].output_layout();
    let physical = &f.source.plan().fragments()[&f.fragment];
    let node = physical
        .nodes()
        .values()
        .find(|node| {
            f.program.graph().nodes()[f.node.index()].physical_sources()
                == [DiagnosticSourceNodeId::new(node.id.get())]
        })
        .unwrap();
    let source = &physical.nodes()[&node.inputs[0]];
    let NodeKind::Aggregate {
        calls, group_by, ..
    } = &source.kind
    else {
        panic!("actual test Filter input is its Aggregate output")
    };
    assert_eq!(layout.schema().fields().len(), source.output.columns.len());
    let mut arrays = Vec::new();
    for (&value, field) in source.output.columns.iter().zip(layout.schema().fields()) {
        let array: ArrayRef = if group_by.iter().any(|(_, output)| *output == value) {
            Arc::new(Int32Array::from_iter_values(
                (0..counts.len()).map(|n| i32::try_from(n + 1).unwrap()),
            ))
        } else {
            let call = calls.iter().find(|call| call.output == value).unwrap();
            match call.binding.function.function_id.as_str() {
                "builtin.aggregate/ndv/v1" | "builtin.aggregate/approx_count_distinct/v1" => {
                    Arc::new(Int64Array::from_iter_values(counts.iter().copied()))
                }
                "builtin.aggregate/max/v1" => {
                    assert_eq!(counts.len(), doubles.len());
                    Arc::new(Float64Array::from(doubles.to_vec()))
                }
                other => panic!("unaccounted actual test Aggregate output {other} for {value:?}"),
            }
        };
        assert_eq!(physical.values()[&value].ty.data_type(), array.data_type());
        assert_eq!(field.data_type(), array.data_type());
        arrays.push(array);
    }
    RecordBatch::try_new(layout.schema().clone(), arrays).unwrap()
}
fn bits(array: &ArrayRef) -> Vec<Option<bool>> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn group(f: &Fixture) -> CompiledFilterConjunctionInstance {
    CompiledFilterConjunctionInstance::try_new(Arc::clone(&f.program), f.node, &Control).unwrap()
}
#[test]
fn filter_conjunction_actual_native_sql_frame_compiled_and_sparse_empty() {
    {
        let f = fixture(NATIVE);
        let input = batch(&f, &[0, 1, 0], &[]);
        let mut instance = group(&f);
        assert_eq!(
            bits(
                &instance
                    .evaluate_required(&input, Selection::all(3), &Control)
                    .unwrap()
            ),
            vec![Some(true), Some(false), Some(true)]
        );
        assert_eq!(
            bits(
                &instance
                    .evaluate_required(&input, Selection::try_sparse(3, &[0, 2]).unwrap(), &Control)
                    .unwrap()
            ),
            vec![Some(true), Some(true)]
        );
        assert_eq!(
            instance
                .evaluate_required(&input, Selection::try_sparse(3, &[]).unwrap(), &Control)
                .unwrap()
                .len(),
            0
        );
        let sliced = input.slice(1, 2);
        assert_eq!(
            bits(
                &instance
                    .evaluate_required(&sliced, Selection::all(2), &Control)
                    .unwrap()
            ),
            vec![Some(false), Some(true)]
        );
    }
}
#[test]
fn filter_conjunction_actual_pure_decider_masks_and_required_site_batchrow_latches() {
    let f = fixture(
        "SELECT k FROM fixture.ndv_null_contract GROUP BY k HAVING CAST(MAX(CAST(v AS DOUBLE)) AS DATE) IS NOT NULL AND ndv(v)=0",
    );
    let input = batch(
        &f,
        &[1, 0, 0],
        &[Some(f64::NAN), Some(f64::NAN), Some(20200101.)],
    );
    assert_eq!(
        bits(
            &group(&f)
                .evaluate_required(&input, Selection::try_sparse(3, &[0, 2]).unwrap(), &Control)
                .unwrap()
        ),
        vec![Some(false), Some(true)]
    );
    let mut instance = group(&f);
    let error = instance
        .evaluate_required(&input, Selection::try_sparse(3, &[1, 2]).unwrap(), &Control)
        .unwrap_err();
    let ExecutionFailureCause::RequiredRow(error) = error.cause() else {
        panic!("actual required row error")
    };
    assert_eq!(
        error.root(),
        ProgramExpressionRootSite::Node {
            node: f.node,
            role: ProgramNodeExpressionRole::FilterPredicate { predicate: 0 }
        }
    );
    assert_eq!(error.batch_row(), 1);
    let second = instance
        .evaluate_required(&input, Selection::all(3), &Control)
        .unwrap_err();
    assert!(matches!(
        second.cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
    ));
}
#[test]
fn filter_conjunction_actual_nested_not_is_null_keeps_value_demand() {
    let f = fixture(
        "SELECT k FROM fixture.ndv_null_contract GROUP BY k HAVING NOT (CAST(MAX(CAST(v AS DOUBLE)) AS DATE) IS NULL) AND ndv(v)=0",
    );
    let input = batch(&f, &[0, 1, 0], &[None, Some(f64::NAN), Some(20200101.)]);
    assert_eq!(
        bits(
            &group(&f)
                .evaluate_required(&input, Selection::all(3), &Control)
                .unwrap()
        ),
        vec![Some(false), Some(false), Some(true)]
    );
}
#[test]
fn filter_conjunction_actual_rand_domains_and_cross_batch_state() {
    let f = fixture(
        "SELECT k FROM fixture.ndv_null_contract GROUP BY k HAVING ndv(v)=0 AND rand(13)<0.5",
    );
    let mut narrow = group(&f);
    let mut guarded = group(&f);
    let selected = batch(&f, &[0, 0, 0], &[]);
    let rejected = batch(&f, &[0, 1, 0], &[]);
    let a = bits(
        &narrow
            .evaluate_required(
                &selected,
                Selection::try_sparse(3, &[0, 2]).unwrap(),
                &Control,
            )
            .unwrap(),
    );
    let b = bits(
        &guarded
            .evaluate_required(&rejected, Selection::all(3), &Control)
            .unwrap(),
    );
    assert_eq!(a, vec![b[0], b[2]]);
    assert_eq!(b[1], Some(false));
    assert_eq!(
        bits(
            &narrow
                .evaluate_required(&selected, Selection::all(3), &Control)
                .unwrap()
        ),
        bits(
            &guarded
                .evaluate_required(&selected, Selection::all(3), &Control)
                .unwrap()
        )
    );
}
#[test]
fn filter_conjunction_actual_pending_error_commits_before_rand_boundary() {
    let f = fixture(
        "SELECT k FROM fixture.ndv_null_contract GROUP BY k HAVING CAST(MAX(CAST(v AS DOUBLE)) AS DATE) IS NOT NULL AND rand(13)<0.5",
    );
    let input = batch(&f, &[0], &[Some(f64::NAN)]);
    let mut instance = group(&f);
    let error = instance
        .evaluate_required(&input, Selection::all(1), &Control)
        .unwrap_err();
    let ExecutionFailureCause::RequiredRow(error) = error.cause() else {
        panic!("pending error must become necessary before a nonpure boundary")
    };
    assert_eq!(
        error.root(),
        ProgramExpressionRootSite::Node {
            node: f.node,
            role: ProgramNodeExpressionRole::FilterPredicate { predicate: 0 }
        }
    );
    assert_eq!(error.batch_row(), 0);
    assert_eq!(
        instance.constructed_root_call_instances(1),
        0,
        "a terminal row never creates the later actual RAND instance"
    );
}
struct Stop {
    at: usize,
    cause: KernelFailure,
    trace: Mutex<Vec<u32>>,
}
impl KernelEvaluationControl for Stop {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        let mut trace = self.trace.lock().unwrap();
        if trace.len() == self.at {
            return Err(self.cause.clone());
        }
        trace.push(n);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("no wait author")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InstanceFailed,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("source invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("source internal")),
        KernelFailure::Operational(KernelDiagnostic::new("source operational")),
    ]
}
#[test]
fn filter_conjunction_actual_every_runtime_callback_seven_causes_prefix_no_tail_latch() {
    let f = fixture(NATIVE);
    let input = batch(&f, &[0, 1, 0], &[]);
    let success = Stop {
        at: usize::MAX,
        cause: KernelFailure::Cancelled,
        trace: Mutex::new(Vec::new()),
    };
    group(&f)
        .evaluate_required(&input, Selection::all(3), &success)
        .unwrap();
    let trace = success.trace.into_inner().unwrap();
    assert!(!trace.is_empty());
    for index in 0..trace.len() {
        for cause in causes() {
            let stop = Stop {
                at: index,
                cause: cause.clone(),
                trace: Mutex::new(Vec::new()),
            };
            let mut instance = group(&f);
            let error = instance
                .evaluate_required(&input, Selection::all(3), &stop)
                .unwrap_err();
            assert!(
                matches!(error.cause(),ExecutionFailureCause::Kernel(actual) if *actual==cause)
            );
            assert_eq!(*stop.trace.lock().unwrap(), trace[..index]);
            let len = stop.trace.lock().unwrap().len();
            assert!(matches!(
                instance
                    .evaluate_required(&input, Selection::all(3), &stop)
                    .unwrap_err()
                    .cause(),
                ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
            ));
            assert_eq!(stop.trace.lock().unwrap().len(), len);
        }
    }
}
#[test]
fn filter_conjunction_actual_row_quantum_and_empty_schema_validation() {
    let f = fixture(NATIVE);
    let input = batch(&f, &vec![0; 321], &[]);
    let control = Stop {
        at: usize::MAX,
        cause: KernelFailure::Cancelled,
        trace: Mutex::new(Vec::new()),
    };
    assert_eq!(
        group(&f)
            .evaluate_required(&input, Selection::all(321), &control)
            .unwrap()
            .len(),
        321
    );
    assert!(control.trace.lock().unwrap().contains(&256));
    let mut fields = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields[0] = fields[0].clone().with_name("not_the_original_field");
    let wrong = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        input.columns().to_vec(),
    )
    .unwrap();
    assert!(
        group(&f)
            .evaluate_required(&wrong, Selection::try_sparse(321, &[]).unwrap(), &Control)
            .is_err()
    );
}
struct CompileStop {
    at: usize,
    cause: CompileControlError,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for CompileStop {
    fn checkpoint(&self, phase: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        if trace.len() == self.at {
            return Err(self.cause.clone());
        }
        trace.push((phase, n));
        Ok(())
    }
}
#[test]
fn filter_conjunction_actual_local_compile_three_causes_sampled_prefix() {
    let source = ndv_sql_source(NATIVE);
    let functions = installed_builtin_owner_catalogue();
    let (fragment, _) = source
        .plan()
        .fragments()
        .iter()
        .find(|(_, fragment)| {
            fragment.nodes().values().any(
                |node| matches!(&node.kind,NodeKind::Filter{predicates} if predicates.len()==2),
            )
        })
        .unwrap();
    let package = source_packages(&source).remove(fragment).unwrap();
    let success = CompileStop {
        at: usize::MAX,
        cause: CompileControlError::Cancelled,
        trace: Mutex::new(Vec::new()),
    };
    compile_fragment(
        validate_package(Arc::clone(&package)),
        &functions,
        compile_options(),
        &success,
    )
    .unwrap();
    let trace = success.trace.into_inner().unwrap();
    assert!(!trace.is_empty());
    // Sample real first/middle/final compiler callbacks. The all-callback
    // runtime proof above is distinct from this bounded compile prefix probe.
    for index in [0, trace.len() / 2, trace.len() - 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let stop = CompileStop {
                at: index,
                cause: cause.clone(),
                trace: Mutex::new(Vec::new()),
            };
            let result = compile_fragment(
                validate_package(Arc::clone(&package)),
                &functions,
                compile_options(),
                &stop,
            );
            assert!(matches!(result,Err(FragmentCompileError::Control(actual)) if actual==cause));
            assert_eq!(*stop.trace.lock().unwrap(), trace[..index]);
        }
    }
}
