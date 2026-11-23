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
use arrow::{
    array::{ArrayRef, BooleanArray, Int32Array},
    record_batch::RecordBatch,
};
use novarocks_expression::CompiledFilterConjunctionInstance;
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::testkit::installed_builtin_owner_catalogue;
use novarocks_functions::{KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::ProgramNodeKind;
use novarocks_sql_testkit::{inspect_ordered_scan_source, ordered_scan_source};
use novarocks_test_compile_to_execute::programs::programs_with_catalogue_and_original_inlist_source;
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
#[test]
fn scan_ordered_required_compiler_conjunction_preserves_true_only_null_and_boundaries() {
    let source = ordered_scan_source();
    inspect_ordered_scan_source(&source);
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
    let mut instance = CompiledFilterConjunctionInstance::try_new_with_allocator(
        &predicate_units(&program, node),
        &Control,
        None,
    )
    .unwrap();
    let result = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            Selection::all(6),
            &Control,
            &mut PassThroughCallHost,
        )
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

fn predicate_units(
    program: &novarocks_local_program::LocalProgram,
    node: novarocks_local_program::ProgramNodeId,
) -> Vec<(
    Arc<novarocks_expression::CompiledExprSet>,
    novarocks_expression_contract::RootOrdinal,
)> {
    let ProgramNodeKind::Scan { residuals, .. } = program.graph().nodes()[node.index()].kind()
    else {
        panic!("actual Scan")
    };
    residuals
        .iter()
        .map(|site| (program.unit(site.unit).unwrap().clone(), site.root))
        .collect()
}

fn prepared_scan() -> (
    Arc<novarocks_local_program::LocalProgram>,
    novarocks_local_program::ProgramNodeId,
) {
    let original = ordered_scan_source();
    let count = inspect_ordered_scan_source(&original);
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
#[test]
fn scan_ordered_required_actual_indexed_roots_and_sparse_empty_slices() {
    use novarocks_type_contract::EvaluationDemand;
    let (program, node) = prepared_scan();
    let ProgramNodeKind::Scan { residuals, .. } = program.graph().nodes()[node.index()].kind()
    else {
        panic!("actual scan")
    };
    for reference in residuals {
        let unit = program.unit(reference.unit).unwrap();
        let compiled = unit.root(reference.root).unwrap();
        let root = &unit.control_flow().uses()[&compiled.use_id];
        assert_eq!(root.definition, compiled.definition);
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
        let mut instance = CompiledFilterConjunctionInstance::try_new_with_allocator(
            &predicate_units(&program, node),
            &Control,
            None,
        )
        .unwrap();
        let output = instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                Selection::try_sparse(6, &rows).unwrap(),
                &Control,
                &mut PassThroughCallHost,
            )
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
fn unsafe_decimal_join_keeps_the_closed_provider_equality_and_original_join_predicate() {
    use arrow::datatypes::{DataType, Field};
    use novarocks_expression_contract::ExprKind;
    use novarocks_physical_plan::NodeKind;
    use novarocks_sql_testkit::sql_source_with_declared_fields;
    use novarocks_test_compile_to_execute::compiler_results::source_packages;
    use novarocks_type_contract::OperatorFunction;

    let decimal = DataType::Decimal128(2, 1);
    let source = sql_source_with_declared_fields(
        "SELECT l.id AS left_id, r.id AS right_id \
         FROM (SELECT id, k FROM fixture WHERE id = 0) l \
         JOIN fixture r ON l.k = r.n / r.d",
        &[
            Field::new("id", DataType::Int32, true),
            Field::new("k", decimal.clone(), true),
            Field::new("n", decimal.clone(), true),
            Field::new("d", decimal, true),
        ],
    );
    let packages = source_packages(&source);
    assert_eq!(packages.len(), source.plan().fragments().len());

    let mut joins = 0;
    let mut exact_guarantees = 0;
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            match &node.kind {
                NodeKind::NestLoopJoin { .. } => joins += 1,
                NodeKind::Scan { relation, .. } => {
                    for guarantee in relation.predicate_guarantees() {
                        assert_eq!(
                            guarantee.kind,
                            novarocks_physical_plan::PredicateGuaranteeKind::Exact
                        );
                        let root = guarantee.predicate;
                        let unit = fragment.unit(node.id).unwrap();
                        let expression = unit
                            .definition(unit.root(root).unwrap().definition)
                            .unwrap();
                        if matches!(&expression.kind, ExprKind::FunctionCall { function, .. }
                            if function.function_id == OperatorFunction::Equal.function_id())
                        {
                            exact_guarantees += 1;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    assert_eq!(joins, 1);
    assert_eq!(exact_guarantees, 1);
}
