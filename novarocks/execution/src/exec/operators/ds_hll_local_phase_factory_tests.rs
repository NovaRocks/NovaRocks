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

//! Real prepared factories, with no forged local nodes or phase contracts.
use super::aggregate_fixture::{
    CallSpec, add_aggregate, aggregate_catalog, bind, compile, finish, packages, values,
};
use super::family_fixture::{FixtureControl, int64};
use super::*;
use arrow::array::{Array, BinaryArray, Int64Array};
use novarocks_functions::PureKernelAbi;
use novarocks_physical_plan::{
    AggregateCallId, AggregateGrouping, AggregatePhase, AggregateSequenceId, FragmentId,
    FragmentSink, PlanVersionId, ResultField, ResultPort,
};

fn program(two_phase: bool) -> Arc<LocalProgram> {
    let fixture_sources = crate::exec::operators::family_fixture::constant_sources::Sources::new();

    let catalog = aggregate_catalog(&[("ds_hll_count_distinct", PureKernelAbi::AggregateV1)]);
    let bound = bind(&catalog, "ds_hll_count_distinct", &[int64(false)]);
    let fragment = FragmentId::new(1);
    let mut builder =
        crate::exec::operators::family_fixture::constant_sources::FragmentBuilder::new(
            fragment,
            &fixture_sources,
        );
    let source = builder.reserve_node_id().unwrap();
    let input = values(
        &mut builder,
        source,
        &[int64(false)],
        &[vec![
            crate::exec::operators::family_fixture::constant_sources::i64(1),
        ]],
    );
    let sequence = AggregateSequenceId::new(1);
    let (first, state) = add_aggregate(
        &mut builder,
        source,
        &[],
        &[CallSpec {
            bound: &bound,
            phase: if two_phase {
                AggregatePhase::Partial { sequence }
            } else {
                AggregatePhase::Single
            },
            id: AggregateCallId::new(1),
            arguments: input,
            distinct: false,
        }],
        if two_phase {
            AggregateGrouping::Partial
        } else {
            AggregateGrouping::Complete
        },
    );
    let root = if two_phase {
        add_aggregate(
            &mut builder,
            first,
            &[],
            &[CallSpec {
                bound: &bound,
                phase: AggregatePhase::Final { sequence },
                id: AggregateCallId::new(2),
                arguments: state,
                distinct: false,
            }],
            AggregateGrouping::Complete,
        )
        .0
    } else {
        first
    };
    let definition = finish(builder, root, FragmentSink::Result, 2);
    let output = definition.nodes()[&root].output.clone();
    let mut plan = crate::exec::operators::family_fixture::constant_sources::PlanBuilder::new(
        PlanVersionId::try_new([117; 16]).unwrap(),
        &fixture_sources,
    );
    plan.add_fragment(definition).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment,
        fields: output
            .columns
            .iter()
            .map(|value| ResultField {
                domain: crate::test_result_domain::result_value_domain(&bound.result_type()),
                name: "estimate".into(),
                alias: None,
                value: *value,
                ty: bound.result_type(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        output,
    })
    .unwrap();
    let plan = plan.finish_observed(&FixtureControl).unwrap();
    compile(
        packages(&plan, &catalog).remove(&fragment).unwrap(),
        &catalog,
        2,
        true,
    )
}
fn factory(
    program: &Arc<LocalProgram>,
    phase: novarocks_functions::AggregateKernelPhase,
) -> CompiledAggregateProcessorFactory {
    let node = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .find_map(|(index, node)| {
            if !matches!(node.kind(), ProgramNodeKind::Aggregate { .. }) {
                return None;
            }
            let id = ProgramNodeId::new(index);
            match program.state_template(ProgramCallSite::Aggregate { node: id, call: 0 }) {
                Some(ProgramStateTemplate::Aggregate { kernel, .. })
                    if kernel.contract().phase() == phase =>
                {
                    Some(id)
                }
                _ => None,
            }
        })
        .unwrap();
    CompiledAggregateProcessorFactory::try_new(
        program.clone(),
        node,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap()
}
fn prepared_schema(
    factory: &CompiledAggregateProcessorFactory,
    tracker: Arc<MemTracker>,
) -> ChunkSchemaRef {
    let creator = aggregate_fixture::runtime_construction_factory(tracker, factory.error.clone());
    prepare_aggregate_schema(
        creator.as_ref(),
        &factory.output_layout,
        factory.groups,
        &factory.calls,
        factory.output_role,
    )
    .expect("actual aggregate declaration schema creator")
}
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
#[test]
fn ds_hll_local_phase_before_real_factories_100k() {
    use novarocks_functions::AggregateKernelPhase as P;
    let tracker = MemTracker::new_root("DsHllLocalPhaseFactories");
    {
        let single = program(false);
        let source = input(&single);
        let raw = run(
            &factory(&single, P::Single),
            std::slice::from_ref(&source),
            tracker.clone(),
        );
        println!(
            "DS_PHASE PURE_RAW estimate={}",
            raw.record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        );
        let stages = program(true);
        let source = input(&stages);
        let partial = factory(&stages, P::Partial);
        let first = Chunk::new_like(source.record_batch().slice(0, 50_000), &source);
        let second = Chunk::new_like(source.record_batch().slice(50_000, 50_000), &source);
        let left = run(&partial, &[first], tracker.clone());
        let right = run(&partial, &[second], tracker.clone());
        let left_flags = left
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0)[5];
        let right_flags = right
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0)[5];
        for reverse in [false, true] {
            let outputs = if reverse {
                [right.clone(), left.clone()]
            } else {
                [left.clone(), right.clone()]
            };
            let result = run(&factory(&stages, P::Final), &outputs, tracker.clone());
            println!(
                "DS_PHASE PURE_TWO reverse={reverse} estimate={} left_flags={left_flags} right_flags={right_flags}",
                result
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            );
        }
    }
    assert_eq!(
        tracker.current(),
        0,
        "actual tracker charges drop after every factory"
    );
}

#[cfg(test)]
#[path = "ds_hll_local_stage_after_tests.rs"]
mod ds_hll_local_stage_after_tests;

#[cfg(test)]
#[path = "compiled_aggregate_zero_calls_tests.rs"]
mod compiled_aggregate_zero_calls_tests;

#[cfg(test)]
#[path = "ds_hll_grouped_local_before_tests.rs"]
mod ds_hll_grouped_local_before_tests;

#[path = "percentile_union_private_factory_tests.rs"]
mod percentile_union_private_factory_tests;
