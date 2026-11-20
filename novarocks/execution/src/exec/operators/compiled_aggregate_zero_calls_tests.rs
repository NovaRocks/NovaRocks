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

//! Genuine zero-call Physical aggregate; no invented LocalProgram or call token.
use super::aggregate_fixture::{add_aggregate, compile, extrema_catalog, finish, packages, values};
use super::family_fixture::{FixtureControl, int64};
use super::*;
use arrow::array::Int64Array;
use novarocks_physical_plan::{
    AggregateGrouping, FragmentId, FragmentSink, PlanVersionId, ResultPort,
};

fn run(
    factory: &CompiledAggregateProcessorFactory,
    input: &[Chunk],
    tracker: Arc<MemTracker>,
) -> Chunk {
    let state = RuntimeState::default();
    let mut operator = factory.create(2, 0);
    operator.set_construction_factory(aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        factory.error.clone(),
    ));
    operator.set_mem_tracker(tracker);
    operator.prepare().expect("prepare actual aggregate schema");
    let mut processor =
        crate::exec::pipeline::test_driver::TestDriver::new(operator.as_mut(), &state);
    for chunk in input {
        processor.input_without_output(chunk.clone()).unwrap();
    }
    processor.signal_eos().unwrap();
    let result = processor
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap()
        .unwrap();
    assert!(
        processor
            .advance_to_boundary()
            .map(|step| step.output)
            .unwrap()
            .is_none()
    );
    result
}
fn input(program: &LocalProgram) -> Chunk {
    let node = program
        .graph()
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), ProgramNodeKind::Values { .. }))
        .unwrap();
    let schema = ChunkSchema::from_compiled_layout(node.output_layout()).unwrap();
    let array = Arc::new(Int64Array::from_iter_values(1..=100_000)) as ArrayRef;
    Chunk::try_new_with_chunk_schema(
        RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap(),
        schema,
    )
    .unwrap()
}

fn zero_program() -> Arc<LocalProgram> {
    let fixture_sources = crate::exec::operators::family_fixture::constant_sources::Sources::new();
    let parameters = novarocks_type_contract::SemanticParameters::try_new([]).unwrap();

    let catalog = extrema_catalog();
    let fragment = FragmentId::new(1);
    let mut builder =
        crate::exec::operators::family_fixture::constant_sources::FragmentBuilder::new(
            fragment,
            &fixture_sources,
        );
    let source = builder.reserve_node_id().unwrap();
    values(
        &mut builder,
        source,
        &[int64(false)],
        &[vec![
            crate::exec::operators::family_fixture::constant_sources::i64(1),
        ]],
    );
    let (root, _) = add_aggregate(&mut builder, source, &[], &[], AggregateGrouping::Complete);
    let definition = finish(
        builder,
        root,
        FragmentSink::Result,
        2,
        &catalog,
        &parameters,
    );
    let output = definition.nodes()[&root].output.clone();
    assert!(output.columns.is_empty());
    let mut plan = crate::exec::operators::family_fixture::constant_sources::PlanBuilder::new(
        PlanVersionId::try_new([121; 16]).unwrap(),
        &fixture_sources,
    )
    .with_semantic_parameters(parameters.clone());
    plan.add_fragment(definition).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment,
        fields: Box::default(),
        output,
    })
    .unwrap();
    let plan = plan.finish_observed(&FixtureControl).unwrap();
    compile(
        packages(&plan).remove(&fragment).unwrap(),
        &catalog,
        2,
        true,
    )
}
fn zero_factory(program: &Arc<LocalProgram>) -> CompiledAggregateProcessorFactory {
    let id = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .find_map(|(i, n)| {
            matches!(n.kind(), ProgramNodeKind::Aggregate { .. }).then_some(ProgramNodeId::new(i))
        })
        .unwrap();
    let factory = CompiledAggregateProcessorFactory::try_new(
        program.clone(),
        id,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    assert!(factory.calls.is_empty());
    assert_eq!(factory.groups, 0);
    assert!(factory.requires_local_update_stages());
    factory
}
#[test]
fn compiled_aggregate_zero_calls_original_and_real_compiled_before() {
    // A complete global grouping has one unit group, even with no calls and
    // no input. Two local partial unit groups merge into that same one group.
    // The explicit zero-column output schema above pins relational cardinality
    // independently of the retired processor's result.
    let program = zero_program();
    let source = input(&program);
    let factory = zero_factory(&program);
    let raw_tracker = MemTracker::new_root("zero calls actual raw creator");
    let empty_tracker = MemTracker::new_root("zero calls actual empty creator");
    let left_tracker = MemTracker::new_root("zero calls actual left creator");
    let right_tracker = MemTracker::new_root("zero calls actual right creator");
    let final_tracker = MemTracker::new_root("zero calls actual final creator");
    let raw = run(&factory, &[source], raw_tracker.clone());
    let empty = run(&factory, &[], empty_tracker.clone());
    let (partial, final_stage) = factory.into_local_update_stages().unwrap();
    let left = run(&partial, &[], left_tracker.clone());
    let right = run(&partial, &[], right_tracker.clone());
    for output in [&raw, &empty, &left, &right] {
        assert_eq!(output.len(), 1);
        assert!(output.record_batch().columns().is_empty());
    }
    assert!(left_tracker.current() > 0);
    assert!(right_tracker.current() > 0);
    let out = run(&final_stage, &[left, right], final_tracker.clone());
    assert_eq!(out.len(), 1);
    assert!(out.record_batch().columns().is_empty());
    assert_eq!(left_tracker.current(), 0);
    assert_eq!(right_tracker.current(), 0);
    for (output, tracker) in [
        (raw, raw_tracker),
        (empty, empty_tracker),
        (out, final_tracker),
    ] {
        assert!(
            tracker.current() > 0,
            "live typed output retains its actual creator"
        );
        drop(output);
        assert_eq!(tracker.current(), 0);
    }
}
