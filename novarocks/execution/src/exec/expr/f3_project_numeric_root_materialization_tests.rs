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
//! Actual SQL -> LocalCompiler -> Project -> RootResult materialization.
//! Mount as a test child of exec::expr; input and expected data are independent.
#![cfg(novarocks_dev_controls)]

use super::filter_conjunction_actual_sql_compiler_tests::compiler_results_with_declared_read_fields;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use super::sql_test_source::sql_source_with_declared_fields;
use crate::exec::chunk::{
    Chunk, ChunkSchema, RootArrayStorageError, RootArrayStorageLimits, borrowed_root_array_storage,
    borrowed_root_chunk_storage,
};
use crate::exec::operators::compiled_expression::prepare_project_factory_for_test;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::test_driver::TestDriver;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind, StaticSinkProgram};
use std::sync::Arc;

// Preserve each SQL source declaration through the real package extraction and
// LocalCompiler author. The numeric-unary fixture intentionally admits only a
// single nonnull column and is not an author for these multi/nullable sources.
fn programs(
    source: novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
    fields: &[Field],
) -> std::collections::BTreeMap<novarocks_physical_plan::FragmentId, Arc<LocalProgram>> {
    // Freeze the real application's root purpose from this same SQL result
    // declaration before the compiler publishes aliases to the source plan.
    let render = novarocks_sql::compiler::client_render_schema_from_port(
        source.plan().result_port(),
        novarocks_sql::compiler::RenderFamilyRequest::MysqlText {
            timezone_offset_seconds: 0,
        },
    )
    .expect("the original SQL result declares its client render schema");
    let source = source
        .with_root_output(novarocks_result_contract::RootOutputContract::new(
            novarocks_result_contract::RootProfileId::V1,
            novarocks_result_contract::FrozenRootOutput::ClientRows(render),
        ))
        .expect("the same SQL source freezes its actual RootResult purpose");
    compiler_results_with_declared_read_fields(
        &source,
        fields,
        &installed_builtin_owner_catalogue(),
    )
    .into_iter()
    .map(|(fragment, result)| {
        (
            fragment,
            Arc::new(
                result.expect(
                    "the original SQL Project must compile with its exact input declaration",
                ),
            ),
        )
    })
    .collect()
}

fn run_project(program: Arc<LocalProgram>, node: Option<ProgramNodeId>, input: Chunk) -> Chunk {
    let errors = Arc::new(RuntimeErrorState::default());
    let factory = prepare_project_factory_for_test(program, node, errors.clone()).unwrap();
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("compiled numeric project"),
            errors,
        ).unwrap(),
    );
    let state = RuntimeState::default();
    operator.prepare().unwrap();
    operator.bind_runtime_state(&state).unwrap();
    let mut output = TestDriver::new(operator.as_mut(), &state)
        .input_and_collect(input)
        .unwrap();
    assert_eq!(output.len(), 1);
    output.remove(0)
}

#[test]
fn numeric_arithmetic_project_preserves_concrete_carriers_through_the_root_boundary() {
    let limits = RootArrayStorageLimits {
        bytes: 1024 * 1024,
        nodes: 256,
        depth: 64,
    };
    // The original count(*) - count(DISTINCT _row_id) corpus, including both
    // NULL sides, is unchanged. SQL's integral division is explicit Float64;
    // CAST authors the separate integral public result on this fixed corpus.
    let cases = [
        ("+", 12, 12, 12.0, 12.0),
        ("-", 0, 2, 0.0, 2.0),
        ("*", 36, 35, 36.0, 35.0),
        ("/", 1, 1, 1.0, 1.4),
        ("%", 0, 2, 0.0, 2.0),
    ];
    for mode in 0..3 {
        for (operation, first_int, last_int, first_float, last_float) in cases {
            if mode == 2 && operation != "/" {
                continue;
            }
            let input_type = if mode == 1 {
                DataType::Float64
            } else {
                DataType::Int64
            };
            let output_type = if mode == 0 {
                DataType::Int64
            } else {
                DataType::Float64
            };
            let expression = format!("count_all {operation} count_distinct");
            let expression = if mode == 0 && operation == "/" {
                format!("CAST({expression} AS BIGINT)")
            } else {
                expression
            };
            let fields = [
                Field::new("count_all", input_type.clone(), true),
                Field::new("count_distinct", input_type, true),
            ];
            let source = sql_source_with_declared_fields(
                &format!("SELECT {expression} AS arithmetic_result FROM fixture"),
                &fields,
            );
            assert_eq!(
                source.plan().result_port().unwrap().fields[0]
                    .ty
                    .data_type(),
                &output_type
            );
            let programs = programs(source, &fields);
            // Select the actual computing Project by its frozen two-column
            // input port. A result alias/gather is not a second producer.
            let mut producers = Vec::new();
            for program in programs.values() {
                for node in program.graph().nodes() {
                    if let ProgramNodeKind::Project { input, exprs, .. } = node.kind() {
                        let parent = program.graph().nodes()[input.index()].output_layout();
                        if exprs.len() == 1
                            && matches!(
                                program.graph().nodes()[input.index()].kind(),
                                ProgramNodeKind::Scan { .. }
                            )
                            && parent.schema().fields().len() == 2
                            && node.output_layout().schema().fields().len() == 1
                        {
                            producers.push((program.clone(), node.local_id().unwrap(), *input));
                        }
                    }
                }
            }
            assert_eq!(
                producers.len(),
                1,
                "one actual computing Project: {expression}"
            );
            let (program, node, input_node) = producers.pop().unwrap();
            let layout = program.graph().nodes()[input_node.index()].output_layout();
            let left: ArrayRef = if mode == 1 {
                Arc::new(Float64Array::from(vec![
                    Some(6.0),
                    None,
                    Some(7.0),
                    Some(6.0),
                ]))
            } else {
                Arc::new(Int64Array::from(vec![Some(6), None, Some(7), Some(6)]))
            };
            let right: ArrayRef = if mode == 1 {
                Arc::new(Float64Array::from(vec![
                    Some(6.0),
                    Some(2.0),
                    Some(5.0),
                    None,
                ]))
            } else {
                Arc::new(Int64Array::from(vec![Some(6), Some(2), Some(5), None]))
            };
            // The actual provider recipe keeps relation declaration order;
            // its frozen transport names are not the SQL column labels.
            // compiler_results_with_declared_read_fields checks each original
            // relation field against fields[ordinal] before freezing the scan.
            assert_eq!(layout.schema().fields().len(), fields.len());
            for (ordinal, declared) in fields.iter().enumerate() {
                assert_eq!(
                    layout.schema().field(ordinal).data_type(),
                    declared.data_type()
                );
                assert_eq!(
                    layout.schema().field(ordinal).is_nullable(),
                    declared.is_nullable()
                );
            }
            let columns = vec![left, right];
            let input = Chunk::try_new_with_chunk_schema(
                RecordBatch::try_new(layout.schema().clone(), columns).unwrap(),
                ChunkSchema::from_compiled_layout(layout).unwrap(),
            )
            .unwrap();
            let output = run_project(program, Some(node), input);
            if mode == 0 {
                assert_eq!(
                    output.columns()[0]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    &Int64Array::from(vec![Some(first_int), None, Some(last_int), None])
                );
            } else {
                assert_eq!(
                    output.columns()[0]
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap(),
                    &Float64Array::from(vec![Some(first_float), None, Some(last_float), None])
                );
            }
            borrowed_root_chunk_storage(&output, limits)
                .expect("actual compiled arithmetic Project publishes a concrete standard carrier");
            let source_array = output.columns()[0].clone();
            let roots = programs
                .values()
                .filter(|program| {
                    matches!(
                        program.graph().sink(),
                        Some(StaticSinkProgram::RootResult(_))
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(roots.len(), 1);
            let root_program = roots[0];
            let layout =
                root_program.graph().nodes()[root_program.graph().root().index()].output_layout();
            assert_eq!(layout.schema().fields().len(), 1);
            assert_eq!(layout.schema().field(0).data_type(), &output_type);
            assert_eq!(
                layout.value_binding_at(0).unwrap(),
                output.chunk_schema().slots()[0].typed_binding()
            );
            // Supply the frozen RootResult port with the actual produced
            // array. No expression is evaluated again and no value is rebuilt.
            let input = Chunk::try_new_with_chunk_schema(
                RecordBatch::try_new(layout.schema().clone(), vec![source_array.clone()]).unwrap(),
                ChunkSchema::from_compiled_layout(layout).unwrap(),
            )
            .unwrap();
            let output = run_project(root_program.clone(), None, input);
            assert!(Arc::ptr_eq(&source_array, &output.columns()[0]));
            borrowed_root_chunk_storage(&output, limits)
                .expect("actual compiled final boundary preserves the arithmetic array owner");
            let wrapped: ArrayRef = Arc::new(output.columns()[0].clone());
            assert_eq!(
                borrowed_root_array_storage(wrapped.as_ref(), limits),
                Err(RootArrayStorageError::CarrierOuterOwnerMismatch),
                "producer correctness must not weaken root outer-owner rejection"
            );
        }
    }
}

#[test]
fn project_casts_final_output_to_declared_slot_type() {
    use arrow::array::Int32Array;
    let fields = [Field::new("k", DataType::Int64, false)];
    let source =
        sql_source_with_declared_fields("SELECT CAST(k AS INT) AS out FROM fixture", &fields);
    let programs = programs(source, &fields);
    let mut producers = Vec::new();
    for program in programs.values() {
        for node in program.graph().nodes() {
            if let ProgramNodeKind::Project { input, exprs, .. } = node.kind() {
                let parent = program.graph().nodes()[input.index()].output_layout();
                if exprs.len() == 1
                    && parent.schema().fields().len() == 1
                    && parent.schema().field(0).data_type() == &DataType::Int64
                    && node.output_layout().schema().field(0).data_type() == &DataType::Int32
                {
                    producers.push((program.clone(), node.local_id().unwrap(), *input));
                }
            }
        }
    }
    assert_eq!(producers.len(), 1);
    let (program, node, input) = producers.pop().unwrap();
    let layout = program.graph().nodes()[input.index()].output_layout();
    let input = Chunk::try_new_with_chunk_schema(
        RecordBatch::try_new(
            layout.schema().clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64]))],
        )
        .unwrap(),
        ChunkSchema::from_compiled_layout(layout).unwrap(),
    )
    .unwrap();
    let output = run_project(program, Some(node), input);
    assert_eq!(
        output.record_batch().schema().field(0).data_type(),
        &DataType::Int32
    );
    let values = output
        .record_batch()
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("output column");
    assert_eq!(values.value(0), 1);
    assert_eq!(values.value(1), 2);
}

fn identity_fixture() -> (Arc<LocalProgram>, ProgramNodeId, Chunk) {
    use arrow::array::StringArray;
    let fields = [Field::new("k", DataType::Utf8, true)];
    let source = sql_source_with_declared_fields("SELECT k AS alias FROM fixture", &fields);
    let programs = programs(source, &fields);
    let mut producers = Vec::new();
    for program in programs.values() {
        for node in program.graph().nodes() {
            if let ProgramNodeKind::Project { input, exprs, .. } = node.kind() {
                let input_node = &program.graph().nodes()[input.index()];
                let layout = input_node.output_layout();
                if exprs.len() == 1
                    && matches!(input_node.kind(), ProgramNodeKind::Scan { .. })
                    && layout.schema().fields().len() == fields.len()
                    && layout.schema().field(0).data_type() == fields[0].data_type()
                    && layout.schema().field(0).is_nullable() == fields[0].is_nullable()
                {
                    producers.push((program.clone(), node.local_id().unwrap(), *input));
                }
            }
        }
    }
    assert_eq!(producers.len(), 1);
    let (program, node, input) = producers.pop().unwrap();
    let layout = program.graph().nodes()[input.index()].output_layout();
    let chunk = Chunk::try_new_with_chunk_schema(
        RecordBatch::try_new(
            layout.schema().clone(),
            vec![Arc::new(StringArray::from(vec!["{}", "[]"]))],
        )
        .unwrap(),
        ChunkSchema::from_compiled_layout(layout).unwrap(),
    )
    .unwrap();
    (program, node, chunk)
}

fn real_creation(
    tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
) -> Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory> {
    crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap()
}

#[test]
fn project_prepare_requires_actual_factory_before_input() {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::runtime::fragment::ExecutionFailureCause;
    let (program, node, input) = identity_fixture();
    let factory = prepare_project_factory_for_test(
        program,
        Some(node),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    assert!(operator.prepared_schema_for_test().is_none());
    let failure = operator.prepare().unwrap_err();
    assert_eq!(
        failure.cause(),
        &ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
    );
    let original = input.columns()[0].clone();
    let state = RuntimeState::default();
    let after_input = TestDriver::new(operator.as_mut(), &state)
        .input_and_collect(input)
        .unwrap_err();
    assert_eq!(after_input.cause(), failure.cause());
    assert!(operator.prepared_schema_for_test().is_none());
    assert_eq!(
        original
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        "{}"
    );
}

#[test]
fn compiled_project_prepare_header_refusal_rolls_back_owned_storage() {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::runtime::fragment::ExecutionFailureCause;
    let (program, node, _) = identity_fixture();
    let factory = prepare_project_factory_for_test(
        program,
        Some(node),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let tracker =
        crate::runtime::mem_tracker::MemTracker::new_root("compiled project prepare refusal");
    let creation = real_creation(tracker.clone());
    let baseline = tracker.current();
    tracker.install_limit_once(baseline + 1).unwrap();
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(creation.clone());
    let failure = operator.prepare().unwrap_err();
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(tracker.current(), baseline);
    assert!(operator.prepared_schema_for_test().is_none());
    // Actual compiled preparation is called once by its lifecycle owner.
    // The deleted fixture's private sticky-reentry latch is not recreated.
    drop(operator);
    drop(creation);
    drop(factory);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn project_prepared_independent_slot_keeps_only_finite_creator_until_last_reference() {
    let (program, node, input) = identity_fixture();
    let expected = program.graph().nodes()[node.index()]
        .output_layout()
        .value_binding_at(0)
        .unwrap()
        .clone();
    let factory = prepare_project_factory_for_test(
        program.clone(),
        Some(node),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let tracker =
        crate::runtime::mem_tracker::MemTracker::new_root("compiled project independent slot");
    let creation = real_creation(tracker.clone());
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(creation.clone());
    operator.prepare().unwrap();
    let output = operator
        .prepared_schema_for_test()
        .expect("actual prepared compiled output");
    let slot = output.slots()[0].clone();
    assert_eq!(slot.typed_binding(), &expected);
    assert_eq!(slot.data_type(), &DataType::Utf8);
    assert!(slot.nullable());
    let source_array = Arc::downgrade(&input.columns()[0]);
    drop(output);
    drop(operator);
    drop(factory);
    drop(program);
    drop(input);
    drop(creation);
    assert!(source_array.upgrade().is_none());
    assert!(tracker.current() > 0);
    assert_eq!(slot.typed_binding(), &expected);
    drop(slot);
    assert_eq!(tracker.current(), 0);
}
