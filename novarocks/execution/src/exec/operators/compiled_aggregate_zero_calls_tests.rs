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
use super::*;
use crate::exec::operators::aggregate::{AggregateProcessorFactory, empty_execution_function_set};
fn zero_program() -> Arc<LocalProgram> {
    let catalog = aggregate_catalog(&[("ds_hll_count_distinct", PureKernelAbi::AggregateV1)]);
    let fragment = FragmentId::new(1);
    let mut builder = FragmentBuilder::new(fragment);
    let source = builder.reserve_node_id().unwrap();
    values(
        &mut builder,
        source,
        &[int64(false)],
        &[vec![LiteralValue::Int64(1)]],
    );
    let (root, _) = add_aggregate(&mut builder, source, &[], &[], AggregateGrouping::Complete);
    let definition = finish(builder, root, FragmentSink::Result, 2);
    let output = definition.nodes()[&root].output.clone();
    assert!(output.columns.is_empty());
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([121; 16]).unwrap());
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
        packages(&plan, &catalog).remove(&fragment).unwrap(),
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
fn original_zero(partial: bool, direct: bool, inputs: &[Chunk]) -> (Chunk, Arc<MemTracker>) {
    // Zero calls and zero grouping columns explicitly declare an empty typed output.
    let schema = Arc::new(
        ChunkSchema::try_new_typed(
            vec![],
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .expect("explicit zero-column aggregate declaration"),
    );
    let factory = AggregateProcessorFactory::new_native(
        7,
        Arc::new(crate::exec::expr::ExprArena::default()),
        vec![],
        vec![],
        empty_execution_function_set(),
        vec![],
        partial,
        direct,
        schema,
        vec![],
        None,
        1,
        None,
    )
    .unwrap();
    let state = RuntimeState::default();
    // Each original witness is one complete local driver, without a final-domain session.
    let tracker = MemTracker::new_root("OriginalZeroCalls");
    let mut operator = factory.create(1, 0);
    operator.set_mem_tracker(tracker.clone());
    operator.set_construction_factory(aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        state.error_state(),
    ));
    operator.prepare().expect("prepare original aggregate");
    let processor = operator.as_processor_mut().unwrap();
    for input in inputs {
        processor.push_chunk(&state, input.clone()).unwrap();
    }
    processor.set_finishing(&state).unwrap();
    let result = processor.pull_chunk(&state).unwrap().unwrap();
    assert_eq!(result.len(), 1);
    assert!(result.record_batch().columns().is_empty());
    assert!(processor.pull_chunk(&state).unwrap().is_none());
    drop(operator);
    assert!(
        tracker.current() > 0,
        "the live original typed output retains its actual creator"
    );
    (result, tracker)
}
#[test]
fn compiled_aggregate_zero_calls_original_and_real_compiled_before() {
    // Original row-count evidence precedes the real package/compiler boundary.
    let (empty, empty_tracker) = original_zero(false, false, &[]);
    assert_eq!(empty.len(), 1);
    let (left, left_tracker) = original_zero(true, false, &[]);
    let (right, right_tracker) = original_zero(true, false, &[]);
    let (final_result, final_tracker) = original_zero(false, true, &[left, right]);
    assert_eq!(left_tracker.current(), 0);
    assert_eq!(right_tracker.current(), 0);
    assert_eq!(final_result.len(), 1);
    let program = zero_program();
    let source = input(&program);
    let (original, original_tracker) = original_zero(false, false, &[source.clone()]);
    assert_eq!(original.len(), 1);
    for (output, tracker) in [
        (empty, empty_tracker),
        (final_result, final_tracker),
        (original, original_tracker),
    ] {
        assert!(
            tracker.current() > 0,
            "live original output retains its creator"
        );
        drop(output);
        assert_eq!(tracker.current(), 0);
    }
    // The assertion is permanently the actual original row-count behavior.
    // Before correction this genuine pure factory returns its original Arrow error.
    let tracker = MemTracker::new_root("ZeroCallsOriginalAndCompiled");
    let factory = zero_factory(&program);
    let raw = run(&factory, &[source], tracker.clone());
    assert_eq!(raw.len(), 1);
    let empty = run(&factory, &[], tracker.clone());
    assert_eq!(empty.len(), 1);
    let (partial, final_stage) = factory.into_local_update_stages().unwrap();
    let left = run(&partial, &[], tracker.clone());
    let right = run(&partial, &[], tracker.clone());
    let out = run(&final_stage, &[left, right], tracker.clone());
    assert_eq!(out.len(), 1);
    assert!(out.record_batch().columns().is_empty());
    assert!(
        tracker.current() > 0,
        "live typed output keeps its actual creator"
    );
    drop(raw);
    drop(empty);
    drop(out);
    assert_eq!(tracker.current(), 0);
}
