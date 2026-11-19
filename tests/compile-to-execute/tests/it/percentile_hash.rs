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
//! Actual SQL author -> package -> LocalCompiler -> existing single Frame.
//! Noop folding retains the genuine N1 call; this is separate from real FE fold evidence.
use arrow::{
    array::{Array, ArrayRef, BinaryArray, Int32Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, StaticExprKind};
use novarocks_expression_contract::ExprKind;
use novarocks_expression_contract::RootOrdinal;
use novarocks_functions::testkit::installed_builtin_owner_catalogue;
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef, ProgramUnitId};
use novarocks_sql_testkit::approx_sql_source;
use novarocks_test_compile_to_execute::compiler_results::compiler_results;

use novarocks_type_contract::{ControlShape, FunctionVolatility};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after primary refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("HASH does not wait")
    }
}
fn authored(sql: &str, expected: usize) -> Vec<Arc<LocalProgram>> {
    let source = approx_sql_source(sql);
    let mut count = 0;
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let Some(unit) = fragment.unit(node.id) else {
                continue;
            };
            for expr in unit.definitions() {
                let ExprKind::FunctionCall { function, args } = &expr.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/percentile_hash/v1" {
                    continue;
                }
                assert_eq!(args.len(), expected);
                assert_eq!(function.argument_types.len(), expected);
                let request = &unit.requests()[&expr.id];
                assert_eq!(request.logical_argument_count, expected);
                assert_eq!(request.arguments.len(), expected);
                eprintln!(
                    "HASH actual authored complete signature/source: {:?} {function:?} {request:?}",
                    expr.id
                );
                count += 1;
            }
        }
    }
    assert_eq!(count, 1);
    compiler_results(&source, &installed_builtin_owner_catalogue())
        .into_values()
        .map(|program| Arc::new(program.expect("complete actual HASH source must compile")))
        .collect()
}
fn root(programs: &[Arc<LocalProgram>], expected: usize) -> (Arc<LocalProgram>, ProgramRootRef) {
    let mut found = None;
    for p in programs {
        for (unit_ordinal, unit) in p.units().iter().enumerate() {
            for (root_ordinal, root) in unit.roots().iter().enumerate() {
                let flow = unit.control_flow();
                let invocation = &flow.uses()[&root.use_id];
                let node = unit.definitions().node(root.definition).unwrap();
                let StaticExprKind::BoundCall { args, .. } = node.kind() else {
                    continue;
                };
                let Some(call) = unit.calls().get(&root.use_id) else {
                    continue;
                };
                if call.call_contract().function_id().as_str()
                    != "builtin.scalar/percentile_hash/v1"
                {
                    continue;
                }
                assert_eq!(invocation.control, ControlShape::Eager);
                assert_eq!(args.len(), 1);
                assert_eq!(invocation.arguments.len(), 1);
                assert_eq!(flow.uses()[&invocation.arguments[0]].definition, args[0]);
                assert_eq!(
                    call.call_contract().selected().argument_types.len(),
                    expected
                );
                assert_eq!(call.call_contract().logical_argument_count(), expected);
                assert_eq!(
                    call.call_contract().effects().value_stability,
                    FunctionVolatility::Immutable
                );
                assert!(found.is_none());
                found = Some((
                    p.clone(),
                    ProgramRootRef {
                        unit: ProgramUnitId::new(unit_ordinal),
                        root: RootOrdinal::new(u32::try_from(root_ordinal).unwrap()),
                    },
                ));
            }
        }
    }
    found.expect("actual source must retain exactly one HASH producer root")
}
fn input(program: &LocalProgram, root: ProgramRootRef, n: usize) -> RecordBatch {
    let node = program.graph().nodes().iter().find(|node| matches!(node.kind(), ProgramNodeKind::Project { exprs, .. } if exprs.contains(&root))).expect("actual project root");
    let ProgramNodeKind::Project { input, .. } = node.kind() else {
        panic!("actual project")
    };
    let schema = program.graph().nodes()[input.index()]
        .output_layout()
        .schema()
        .clone();
    let columns = schema
        .fields()
        .iter()
        .map(|f| {
            assert_eq!(
                f.data_type(),
                &DataType::Int32,
                "actual HASH source requests only authored v/w/id Int32"
            );
            Arc::new(Int32Array::from(
                (0..n)
                    .map(|i| {
                        if i % 3 == 1 {
                            None
                        } else {
                            Some(i as i32 + 17)
                        }
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}
#[test]
fn hash_native_n1_actual_sql_source_and_frame_original_n1() {
    for (sql, n) in [(
        "SELECT percentile_hash(v) FROM fixture.t_agg_percentile_semantics",
        1,
    )] {
        let programs = authored(sql, n);
        let (p, site) = root(&programs, n);
        let batch = input(&p, site, 3);
        let tracker = Arc::new(novarocks_expression::testkit::CountingAllocator::new(
            8 * 1024 * 1024,
        ));
        let control = Control::default();
        let mut frame = CompiledExpressionInstance::try_new_with_allocator(
            p.unit(site.unit).unwrap().clone(),
            site.root,
            &control,
            Some(tracker.clone()),
        )
        .unwrap();
        let out = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                ScalarInvocationActivation::Activated,
                &control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(out.errors().is_empty());
        let actual = out.values().as_any().downcast_ref::<BinaryArray>().unwrap();
        for i in 0..3 {
            let v = if i == 1 { None } else { Some(i as f64 + 17.0) };
            assert_eq!(
                actual.value(i),
                novarocks_functions::percentile_hash_core::encode_numeric(v)
                    .expect("percentile hash fixture")
            );
        }
        drop(out);
        let rows = [2];
        let out = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(3, &rows).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(out.selection().row(0), Some(2));
        assert_eq!(out.values().null_count(), 0);
        drop(out);
        let out = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(3, &[]).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(out.values().len(), 0);
        drop(out);
        drop(frame);
        assert_eq!(tracker.outstanding(), 0);
    }
}
