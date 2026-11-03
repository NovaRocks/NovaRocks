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

use super::*;
use crate::exec::pipeline::builder::compiled_statistics_writer_fixture;
use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
use crate::runtime::task::ExecutionFailureCause;
use novarocks_local_program::{PreparedWriterAggregateCall, PreparedWriterAggregateCalls};

#[derive(Debug)]
enum PreparedFixtureFailure {
    Creator {
        _cause: ConstructionFailure,
    },
    Resource {
        _cause: novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    },
    Source {
        _cause: novarocks_local_program::PreparedWriterCallsError,
    },
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for PreparedFixtureFailure
{
    fn from(value: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource { _cause: value }
    }
}
impl From<novarocks_local_program::PreparedWriterCallsError> for PreparedFixtureFailure {
    fn from(value: novarocks_local_program::PreparedWriterCallsError) -> Self {
        Self::Source { _cause: value }
    }
}

struct Source {
    state: RuntimeState,
    tracker: Arc<MemTracker>,
    factory: Arc<dyn InputConstructionFactory>,
    calls: PreparedWriterAggregateCalls,
    input: StaticLayout,
    output: StaticLayout,
}
fn source_fixture() -> Source {
    let (program, _binding, state) = compiled_statistics_writer_fixture();
    let tracker = MemTracker::new_root("prepared-writer-original-owner");
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        state.error_state(),
    )
    .unwrap();
    let node = program.graph().root();
    let ProgramNodeKind::TableWriter {
        projection,
        writer_multiplex_layout,
        partial_aggregates,
        ..
    } = program.graph().nodes()[node.index()].kind()
    else {
        panic!("actual writer fixture");
    };
    let owner = factory
        .begin_compiled_layout_construction(writer_multiplex_layout)
        .unwrap();
    owner
        .request(Layout::array::<PreparedWriterAggregateCall>(partial_aggregates.len()).unwrap())
        .unwrap();
    let mut calls = Vec::new();
    calls.try_reserve_exact(partial_aggregates.len()).unwrap();
    for (index, call) in partial_aggregates.iter().enumerate() {
        let site = ProgramCallSite::WriterPartial {
            node,
            call: u32::try_from(index).unwrap(),
        };
        let Some(ProgramStateTemplate::Aggregate { kernel, .. }) = program.state_template(site)
        else {
            panic!("original frozen compiler prepares each call");
        };
        let position = ordinal(writer_multiplex_layout, call.intermediate_slot_id).unwrap();
        calls.push(PreparedWriterAggregateCall::new(
            kernel.clone(),
            call.input_slot_id,
            call.intermediate_slot_id,
            position,
            owner.storage_lifetime(),
        ));
    }
    if calls.capacity() != calls.len() {
        owner
            .request(Layout::array::<PreparedWriterAggregateCall>(calls.len()).unwrap())
            .unwrap();
    }
    let calls = PreparedWriterAggregateCalls::try_new_observed::<PreparedFixtureFailure>(
        calls.into_boxed_slice(),
        AggregateKernelPhase::Partial,
        owner.storage_lifetime(),
        |layout| {
            owner
                .request(layout)
                .map_err(|_cause| PreparedFixtureFailure::Creator { _cause })
        },
        || {
            statistics_work(owner.as_ref(), 1)
                .map_err(|_cause| PreparedFixtureFailure::Creator { _cause })
        },
    )
    .unwrap();
    let input = projection.layout.clone();
    let output = writer_multiplex_layout.clone();
    drop(owner);
    Source {
        state,
        tracker,
        factory,
        calls,
        input,
        output,
    }
}
fn create(source: &Source) -> Box<dyn Operator> {
    let owner = source
        .factory
        .begin_compiled_layout_construction(&source.output)
        .unwrap();
    let mut operator = CompiledWriterPartialFactory::create_prepared_in(
        source.calls.clone(),
        source.input.clone(),
        source.output.clone(),
        source.state.error_state(),
        owner.as_ref(),
    )
    .unwrap();
    operator.set_mem_tracker(source.tracker.clone());
    operator.set_construction_factory(source.factory.clone());
    operator
}

#[test]
fn plan_tree_prepared_partial_and_last_independent_schema_keep_actual_creation() {
    let source = source_fixture();
    let mut operator = create(&source);
    operator.prepare().unwrap();
    operator.bind_runtime_state(&source.state).unwrap();
    let error = source.state.error_state();
    let mut work = crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 0);
    let mut value_conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = DriverStep::new(&error, &mut work, &mut value_conversions);
    let step = operator.as_mut();
    assert!(matches!(step.initial_step(), NextStep::NeedInput));
    step.finish(&mut ctx).unwrap();
    let paused = step.drive(&mut ctx).unwrap();
    assert!(paused.output.is_none());
    assert!(matches!(paused.next, NextStep::Yield));
    ctx.work().begin_turn(8).unwrap();
    let completed = step.drive(&mut ctx).unwrap();
    assert!(matches!(completed.next, NextStep::Finished));
    let output = completed
        .output
        .expect("empty-input intermediate state row");
    step.finish(&mut ctx).unwrap();
    let repeated = step.drive(&mut ctx).unwrap();
    assert!(
        repeated.output.is_none(),
        "EOS cannot duplicate the state row"
    );
    assert!(matches!(repeated.next, NextStep::Finished));
    assert_eq!(output.len(), 1, "original partial empty-input state row");
    assert_eq!(output.columns().len(), 2, "original c1/c2 Theta channels");
    for (array, call) in output.columns().iter().zip(source.calls.calls()) {
        assert_eq!(
            array.data_type(),
            call.handle().contract().intermediate_type().data_type()
        );
    }
    let slot = output.chunk_schema().slots()[0].clone();
    let kept_calls = source.calls.clone();
    let kept_call = source.calls.calls()[0].clone();
    let tracker = source.tracker.clone();
    drop(operator);
    drop(source);
    assert!(
        tracker.current() > 0,
        "output/source independently retain actual responsibility"
    );
    drop(output);
    assert!(
        tracker.current() > 0,
        "independent slot and prepared source remain"
    );
    drop(slot);
    assert!(
        tracker.current() > 0,
        "last original prepared collection owns its creation"
    );
    drop(kept_calls);
    assert!(
        tracker.current() > 0,
        "independent prepared child retains its original creation"
    );
    drop(kept_call);
    assert_eq!(
        tracker.current(),
        0,
        "last finite header/schema/source exit releases charges"
    );
}

#[test]
fn plan_tree_prepared_partial_refuses_before_installed_prepare_owner() {
    let source = source_fixture();
    let owner = source
        .factory
        .begin_compiled_layout_construction(&source.output)
        .unwrap();
    let mut operator = CompiledWriterPartialFactory::create_prepared_in(
        source.calls.clone(),
        source.input.clone(),
        source.output.clone(),
        source.state.error_state(),
        owner.as_ref(),
    )
    .unwrap();
    let failure = operator
        .prepare()
        .expect_err("runtime schema needs its installed factory");
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
    ));
    let tracker = source.tracker.clone();
    drop(operator);
    drop(owner);
    drop(source);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn plan_tree_prepared_partial_preserves_first_memory_and_original_caller_without_tail() {
    let source = source_fixture();
    let owner = source
        .factory
        .begin_compiled_layout_construction(&source.output)
        .unwrap();
    let baseline = source.tracker.current();
    source.tracker.install_limit_once(baseline).unwrap();
    let before = source.tracker.allocated();
    let failure = CompiledWriterPartialFactory::create_prepared_in(
        source.calls.clone(),
        source.input.clone(),
        source.output.clone(),
        source.state.error_state(),
        owner.as_ref(),
    )
    .err()
    .expect("first real header request is refused");
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(source.tracker.current(), baseline);
    assert_eq!(
        source.tracker.allocated() - before,
        i64::try_from("COMPILED_WRITER_PARTIAL_AGGREGATE".len()).unwrap(),
        "no later processor allocation callback"
    );
    let tracker = source.tracker.clone();
    drop(owner);
    drop(source);
    assert_eq!(tracker.current(), 0);

    let source = source_fixture();
    let owner = source
        .factory
        .begin_compiled_layout_construction(&source.output)
        .unwrap();
    let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Internal(
        novarocks_functions::KernelDiagnostic::new("original writer preparation stop"),
    ))
    .at_operator(2, crate::runtime::task::PipelineOperation::Admission);
    source.state.error_state().set_error(original.clone());
    let before = source.tracker.allocated();
    let failure = CompiledWriterPartialFactory::create_prepared_in(
        source.calls.clone(),
        source.input.clone(),
        source.output.clone(),
        source.state.error_state(),
        owner.as_ref(),
    )
    .err()
    .expect("original caller refusal stays primary");
    assert_eq!(failure.cause(), original.cause());
    assert_eq!(failure.site(), original.site());
    assert_eq!(
        source.tracker.allocated(),
        before,
        "first caller failure has no request/footer tail"
    );
    let tracker = source.tracker.clone();
    drop(owner);
    drop(source);
    assert_eq!(tracker.current(), 0);
}
