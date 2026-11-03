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

//! Ordered original Scan residuals use the existing conjunction consumer.
//! This isolated required predicate preserves an explicit optimizer variant;
//! it does not claim native whole-case or provider execution acceptance.
use super::compiled_program::CompiledFilterConjunctionInstance;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use super::numeric_unary_owned_transaction_tests::programs_with_catalogue_and_original_inlist_source;
use super::sql_test_source::sql_source_with_single_field_and_core_residuals;
use arrow::{
    array::{ArrayRef, BooleanArray, Int32Array},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_functions::{KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::ProgramNodeKind;
use novarocks_physical_plan::NodeKind;
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use std::{sync::Arc, time::Duration};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("scan residuals never wait")
    }
}
fn source() -> SqlAuthoredPhysicalPlan {
    sql_source_with_single_field_and_core_residuals(
        "SELECT k FROM t_filter_is_not_null_and_range WHERE k IS NOT NULL AND k >= 10 AND k < 20",
        Field::new("k", DataType::Int32, true),
    )
}
fn inspect(source: &SqlAuthoredPhysicalPlan) -> usize {
    let mut counts = Vec::new();
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Scan {
                residuals,
                relation,
                ..
            } = &node.kind
            {
                assert_eq!(relation.schema().len(), 1);
                assert_eq!(relation.schema()[0].ty.data_type().clone(), DataType::Int32);
                assert!(relation.schema()[0].ty.nullable());
                assert!(relation.predicate_guarantees().is_empty());
                for (ordinal, id) in residuals.iter().enumerate() {
                    let e = fragment.expressions().get(*id).unwrap();
                    println!(
                        "original scan residual ordinal={ordinal} id={id:?} type={:?} kind={:?}",
                        e.ty, e.kind
                    );
                    assert_eq!(e.ty.data_type().clone(), DataType::Boolean);
                }
                counts.push(residuals.len());
            }
        }
    }
    assert_eq!(counts.len(), 1);
    assert!(
        counts[0] >= 2,
        "original optimizer must retain multiple residuals"
    );
    counts[0]
}
#[test]
fn scan_ordered_required_source_keeps_original_nullable_field_and_residuals() {
    inspect(&source());
}
#[test]
fn scan_ordered_required_compiler_conjunction_preserves_true_only_null_and_boundaries() {
    let source = source();
    inspect(&source);
    let functions = installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue_and_original_inlist_source(&source, &functions);
    let mut scans = Vec::new();
    for program in programs.values() {
        for node in program.graph().nodes() {
            if matches!(node.kind(), ProgramNodeKind::Scan { .. }) {
                scans.push((program.clone(), node.local_id().unwrap()));
            }
        }
    }
    assert_eq!(scans.len(), 1);
    let (program, node) = scans.pop().unwrap();
    let schema = program.graph().nodes()[node.index()]
        .output_layout()
        .schema()
        .clone();
    let values = Arc::new(Int32Array::from(vec![
        None,
        Some(5),
        Some(10),
        Some(11),
        Some(19),
        Some(20),
    ])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
    let mut instance = CompiledFilterConjunctionInstance::try_new(program, node, &Control).unwrap();
    let result = instance
        .evaluate_required(&batch, Selection::all(6), &Control)
        .unwrap();
    let truth = result.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(
        truth.iter().collect::<Vec<_>>(),
        vec![
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false)
        ]
    );
}

fn prepared_scan() -> (
    Arc<novarocks_local_program::LocalProgram>,
    novarocks_local_program::ProgramNodeId,
) {
    let original = source();
    let count = inspect(&original);
    let functions = installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue_and_original_inlist_source(&original, &functions);
    let mut result = None;
    for program in programs.values() {
        for node in program.graph().nodes() {
            if let ProgramNodeKind::Scan { residuals, .. } = node.kind() {
                assert_eq!(residuals.len(), count);
                assert!(result.is_none());
                result = Some((program.clone(), node.local_id().unwrap()));
            }
        }
    }
    result.expect("original scan node survives compilation")
}
struct Callback {
    trace: std::sync::Mutex<Vec<u32>>,
    stop: usize,
    cause: KernelFailure,
}
impl Callback {
    fn new(stop: usize, cause: KernelFailure) -> Self {
        Self {
            trace: std::sync::Mutex::new(Vec::new()),
            stop,
            cause,
        }
    }
}
impl KernelEvaluationControl for Callback {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        assert!(trace.len() < self.stop, "callback after original refusal");
        trace.push(n);
        if trace.len() == self.stop {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("scan predicates never wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    use novarocks_functions::KernelDiagnostic;
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("scan-original-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("scan-original-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("scan-original-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn scan_ordered_required_actual_indexed_roots_and_sparse_empty_slices() {
    use novarocks_local_program::{ProgramExpressionRootSite, ProgramNodeExpressionRole};
    use novarocks_type_contract::EvaluationDemand;
    let (program, node) = prepared_scan();
    let ProgramNodeKind::Scan { residuals, .. } = program.graph().nodes()[node.index()].kind()
    else {
        panic!("actual scan")
    };
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    for (index, definition) in residuals.iter().enumerate() {
        let site = ProgramExpressionRootSite::Node {
            node,
            role: ProgramNodeExpressionRole::ScanResidual {
                predicate: u32::try_from(index).unwrap(),
            },
        };
        let id = snapshot.bindings()[&site];
        let root = &snapshot.flows()[&site.arena()].uses()[&id];
        assert_eq!(root.definition, *definition);
        assert_eq!(root.context.demand, EvaluationDemand::TruthOnly);
    }
    let input = Int32Array::from(vec![
        Some(999),
        None,
        Some(5),
        Some(10),
        Some(11),
        Some(19),
        Some(20),
        Some(-999),
    ])
    .slice(1, 6);
    let batch = RecordBatch::try_new(
        program.graph().nodes()[node.index()]
            .output_layout()
            .schema()
            .clone(),
        vec![Arc::new(input) as ArrayRef],
    )
    .unwrap();
    for (rows, expected) in [
        (vec![0, 2, 5], vec![Some(false), Some(true), Some(false)]),
        (vec![2, 3, 4], vec![Some(true); 3]),
        (Vec::new(), Vec::new()),
    ] {
        let mut instance =
            CompiledFilterConjunctionInstance::try_new(program.clone(), node, &Control).unwrap();
        let output = instance
            .evaluate_required(&batch, Selection::try_sparse(6, &rows).unwrap(), &Control)
            .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
    }
}
#[test]
fn scan_ordered_required_all_seven_actual_callback_causes_stop_and_latch() {
    use crate::runtime::task::ExecutionFailureCause;
    let (program, node) = prepared_scan();
    let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
    CompiledFilterConjunctionInstance::try_new(program.clone(), node, &record).unwrap();
    let trace = record.trace.lock().unwrap().clone();
    for stop in 1..=trace.len() {
        for cause in causes() {
            let c = Callback::new(stop, cause.clone());
            assert!(
                matches!(CompiledFilterConjunctionInstance::try_new(program.clone(),node,&c),Err(actual) if actual==cause)
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
        }
    }
    let values = Int32Array::from(
        (0..513)
            .map(|row| if row % 7 == 0 { None } else { Some(row % 25) })
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::try_new(
        program.graph().nodes()[node.index()]
            .output_layout()
            .schema()
            .clone(),
        vec![Arc::new(values) as ArrayRef],
    )
    .unwrap();
    for rows in [
        (0..513).collect::<Vec<_>>(),
        (0..513).step_by(3).collect(),
        Vec::new(),
    ] {
        let selection = Selection::try_sparse(513, &rows).unwrap();
        let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
        let mut instance =
            CompiledFilterConjunctionInstance::try_new(program.clone(), node, &Control).unwrap();
        instance
            .evaluate_required(&batch, selection, &record)
            .unwrap();
        let trace = record.trace.lock().unwrap().clone();
        if rows.len() == 513 {
            assert!(
                trace.contains(&256),
                "actual dense path reaches its work quantum"
            );
        }
        for stop in 1..=trace.len() {
            for cause in causes() {
                let c = Callback::new(stop, cause.clone());
                let mut instance =
                    CompiledFilterConjunctionInstance::try_new(program.clone(), node, &Control)
                        .unwrap();
                let failure = instance
                    .evaluate_required(&batch, selection, &c)
                    .unwrap_err();
                assert_eq!(failure.cause(), &ExecutionFailureCause::Kernel(cause));
                assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
                let again = instance
                    .evaluate_required(&batch, selection, &c)
                    .unwrap_err();
                assert_eq!(
                    again.cause(),
                    &ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
            }
        }
    }
}
