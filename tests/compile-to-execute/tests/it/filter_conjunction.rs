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
use arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array},
    record_batch::RecordBatch,
};
use novarocks_expression::CompiledFilterConjunctionInstance;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::testkit::installed_builtin_owner_catalogue;
use novarocks_functions::{KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::{
    DiagnosticSourceNodeId, LocalProgram, ProgramNodeId, ProgramNodeKind,
};
use novarocks_physical_plan::{FragmentId, NodeKind};
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use novarocks_sql_testkit::ndv_sql_source;
use novarocks_test_compile_to_execute::compiler_results::compiler_results;
use std::{sync::Arc, time::Duration};
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
const NATIVE: &str = "SELECT k FROM fixture.ndv_null_contract GROUP BY k\nHAVING approx_distinct(v) = 0 AND approx_distinct(CAST(v AS BIGINT)) = 0 ORDER BY k;";
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
                for reference in local {
                    let unit = program.unit(reference.unit).unwrap();
                    let root = unit.root(reference.root).unwrap();
                    let invocation = &unit.control_flow().uses()[&root.use_id];
                    assert_eq!(invocation.definition, root.definition);
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
                "builtin.aggregate/approx_distinct/v1" => {
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
fn predicates(
    f: &Fixture,
) -> Vec<(
    Arc<novarocks_expression::CompiledExprSet>,
    novarocks_expression_contract::RootOrdinal,
)> {
    let ProgramNodeKind::Filter { predicates, .. } =
        f.program.graph().nodes()[f.node.index()].kind()
    else {
        panic!("actual Filter")
    };
    predicates
        .iter()
        .map(|site| (f.program.unit(site.unit).unwrap().clone(), site.root))
        .collect()
}
fn group(f: &Fixture) -> CompiledFilterConjunctionInstance {
    CompiledFilterConjunctionInstance::try_new_with_allocator(&predicates(f), &Control, None)
        .unwrap()
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
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &input,
                            source: None
                        },
                        Selection::all(3),
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap()
            ),
            vec![Some(true), Some(false), Some(true)]
        );
        assert_eq!(
            bits(
                &instance
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &input,
                            source: None
                        },
                        Selection::try_sparse(3, &[0, 2]).unwrap(),
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap()
            ),
            vec![Some(true), Some(true)]
        );
        assert_eq!(
            instance
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None
                    },
                    Selection::try_sparse(3, &[]).unwrap(),
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap()
                .len(),
            0
        );
        let sliced = input.slice(1, 2);
        assert_eq!(
            bits(
                &instance
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &sliced,
                            source: None
                        },
                        Selection::all(2),
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap()
            ),
            vec![Some(false), Some(true)]
        );
    }
}
#[test]
fn filter_conjunction_keeps_first_error_before_a_deciding_operand() {
    let f = fixture(
        "SELECT k FROM fixture.ndv_null_contract GROUP BY k HAVING CAST(MAX(CAST(v AS DOUBLE)) AS INTEGER) IS NOT NULL AND approx_distinct(v) = 0",
    );
    let input = batch(
        &f,
        &[1, 0, 0],
        &[Some(f64::NAN), Some(f64::NAN), Some(20200101.)],
    );
    let first = group(&f)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            Selection::try_sparse(3, &[0, 2]).unwrap(),
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap_err();
    let ExprEvaluationFailure::RequiredRow(first) = first else {
        panic!("actual first row error");
    };
    assert_eq!(first.batch_row(), 0);
    let mut instance = group(&f);
    let error = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            Selection::try_sparse(3, &[1, 2]).unwrap(),
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap_err();
    let ExprEvaluationFailure::RequiredRow(error) = error else {
        panic!("actual required row error");
    };
    let ProgramNodeKind::Filter {
        predicates: sites, ..
    } = f.program.graph().nodes()[f.node.index()].kind()
    else {
        panic!("actual Filter");
    };
    assert_eq!(error.root(), sites[0].root);
    assert_eq!(error.batch_row(), 1);
    let second = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            Selection::all(3),
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap_err();
    assert!(matches!(
        second,
        ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
    ));
}
#[test]
fn filter_conjunction_actual_nested_not_is_null_keeps_value_demand() {
    let f = fixture(
        "SELECT k FROM fixture.ndv_null_contract GROUP BY k HAVING NOT (CAST(MAX(CAST(v AS DOUBLE)) AS INTEGER) IS NULL) AND approx_distinct(v) = 0",
    );
    let input = batch(&f, &[0, 1, 0], &[None, Some(1.), Some(20200101.)]);
    assert_eq!(
        bits(
            &group(&f)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None
                    },
                    Selection::all(3),
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap()
        ),
        vec![Some(false), Some(false), Some(true)]
    );
}
