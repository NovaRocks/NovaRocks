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

#[derive(Default)]
struct RecordedWaits(std::sync::Mutex<Vec<Duration>>);
impl KernelEvaluationControl for RecordedWaits {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK);
        Ok(())
    }

    fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
        self.0.lock().unwrap().push(duration);
        Ok(())
    }
}

struct ScanAndFilter {
    scan: (
        Arc<novarocks_local_program::LocalProgram>,
        novarocks_local_program::ProgramNodeId,
    ),
    filter: (
        Arc<novarocks_local_program::LocalProgram>,
        novarocks_local_program::ProgramNodeId,
    ),
}

fn scan_and_filter(sql: &str) -> ScanAndFilter {
    use arrow::datatypes::{DataType, Field};
    use novarocks_sql_testkit::sql_source_with_declared_fields;

    let source = sql_source_with_declared_fields(
        sql,
        &[
            Field::new("id", DataType::Int8, false),
            Field::new("d", DataType::Int64, false),
            Field::new("first_delay", DataType::Int32, false),
            Field::new("second_delay", DataType::Int16, false),
            Field::new("enabled", DataType::Boolean, true),
        ],
    );
    let functions = installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue_and_original_inlist_source(&source, &functions);
    let mut scan = None;
    let mut filter = None;
    for program in programs.values() {
        for node in program.graph().nodes() {
            match node.kind() {
                ProgramNodeKind::Scan { residuals, .. } => {
                    // Bare BOOLEAN is kept as an executable Scan residual by
                    // the SQL/provider contract, including NULL as non-TRUE.
                    assert_eq!(residuals.len(), 1);
                    assert!(
                        scan.replace((program.clone(), node.local_id().unwrap()))
                            .is_none()
                    );
                }
                ProgramNodeKind::Filter { .. } => {
                    assert!(
                        filter
                            .replace((program.clone(), node.local_id().unwrap()))
                            .is_none()
                    );
                }
                _ => {}
            }
        }
    }
    ScanAndFilter {
        scan: scan.expect("actual compiled Scan"),
        filter: filter.expect("actual compiled residual Filter"),
    }
}

fn demand_batch(schema: &arrow::datatypes::SchemaRef) -> RecordBatch {
    use arrow::array::{Int8Array, Int16Array, Int64Array};
    use arrow::datatypes::DataType;

    let arrays = schema
        .fields()
        .iter()
        .map(|field| -> ArrayRef {
            match field.data_type() {
                DataType::Int8 => Arc::new(Int8Array::from(vec![1, 2, 3, 4])),
                DataType::Int16 => Arc::new(Int16Array::from(vec![20, 21, 22, 23])),
                DataType::Int32 => Arc::new(Int32Array::from(vec![10, 11, 12, 13])),
                DataType::Int64 => Arc::new(Int64Array::from(vec![0, 0, 3, 0])),
                DataType::Boolean => Arc::new(BooleanArray::from(vec![
                    Some(false),
                    None,
                    Some(true),
                    Some(true),
                ])),
                other => panic!("unexpected source field {other:?}"),
            }
        })
        .collect();
    RecordBatch::try_new(schema.clone(), arrays).unwrap()
}

impl ScanAndFilter {
    fn apply(
        &self,
        original_rows: &[usize],
        control: &RecordedWaits,
    ) -> Result<ArrayRef, novarocks_expression::ExprEvaluationFailure<std::convert::Infallible>>
    {
        let (scan, scan_node) = &self.scan;
        let scan_batch = demand_batch(
            scan.graph().nodes()[scan_node.index()]
                .output_layout()
                .schema(),
        );
        let selection = Selection::try_sparse(scan_batch.num_rows(), original_rows).unwrap();
        let scan_result = CompiledFilterConjunctionInstance::try_new_with_allocator(
            &predicate_units(scan, *scan_node),
            control,
            None,
        )
        .unwrap()
        .evaluate(
            EvaluationInput::Batch {
                batch: &scan_batch,
                source: None,
            },
            selection,
            control,
            &mut PassThroughCallHost,
        )
        .unwrap();
        let kept = scan_result.as_any().downcast_ref::<BooleanArray>().unwrap();
        let rows = original_rows
            .iter()
            .copied()
            .zip(kept.iter())
            .filter_map(|(row, truth)| (truth == Some(true)).then_some(row))
            .collect::<Vec<_>>();

        let (filter, filter_node) = &self.filter;
        let ProgramNodeKind::Filter {
            input, predicates, ..
        } = filter.graph().nodes()[filter_node.index()].kind()
        else {
            panic!("actual residual Filter");
        };
        let filter_batch = demand_batch(
            filter.graph().nodes()[input.index()]
                .output_layout()
                .schema(),
        );
        let sites = predicates
            .iter()
            .map(|site| (filter.unit(site.unit).unwrap().clone(), site.root))
            .collect::<Vec<_>>();
        CompiledFilterConjunctionInstance::try_new_with_allocator(&sites, control, None)
            .unwrap()
            .evaluate(
                EvaluationInput::Batch {
                    batch: &filter_batch,
                    source: None,
                },
                Selection::try_sparse(filter_batch.num_rows(), &rows).unwrap(),
                control,
                &mut PassThroughCallHost,
            )
    }
}

#[test]
fn scan_necessary_term_cuts_wait_and_row_error_domain_with_original_wait_order() {
    use novarocks_expression::ExprEvaluationFailure;

    let source = scan_and_filter(
        "SELECT id FROM fixture WHERE 12 / d > 3 AND sleep(CAST(first_delay AS BIGINT))          AND sleep(CAST(second_delay AS BIGINT)) AND enabled",
    );
    let control = RecordedWaits::default();
    let output = source.apply(&[0, 1], &control).unwrap();
    assert!(output.is_empty());
    assert!(control.0.lock().unwrap().is_empty());

    let output = source.apply(&[0, 1, 2], &control).unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(true)],
    );
    assert_eq!(
        *control.0.lock().unwrap(),
        vec![Duration::from_secs(12), Duration::from_secs(22)],
    );

    let control = RecordedWaits::default();
    let Err(ExprEvaluationFailure::RequiredRow(error)) = source.apply(&[3], &control) else {
        panic!("the surviving bad divisor still reports a row error");
    };
    assert_eq!(error.batch_row(), 3);
    assert!(control.0.lock().unwrap().is_empty());
}

#[test]
fn scan_does_not_advance_a_fallible_term_before_the_original_wait() {
    use novarocks_expression::ExprEvaluationFailure;

    let source = scan_and_filter(
        "SELECT id FROM fixture WHERE sleep(CAST(first_delay AS BIGINT)) AND 12 / d > 3          AND enabled",
    );
    let control = RecordedWaits::default();
    let Err(ExprEvaluationFailure::RequiredRow(error)) = source.apply(&[3], &control) else {
        panic!("the original division still fails after the selected wait");
    };
    assert_eq!(error.batch_row(), 3);
    assert_eq!(*control.0.lock().unwrap(), vec![Duration::from_secs(13)]);
}
