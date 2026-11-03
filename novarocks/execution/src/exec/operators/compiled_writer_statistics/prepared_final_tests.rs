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
use crate::exec::pipeline::builder::compiled_statistics_finish_fixture;
use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
use crate::runtime::task::ExecutionFailureCause;
use novarocks_local_program::{
    PreparedWriterAggregateCall, PreparedWriterAggregateCalls, PreparedWriterCallsError,
};

#[derive(Debug)]
enum TestSourceFailure {
    Creator {
        _cause: ConstructionFailure,
    },
    Resource {
        _cause: novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    },
    Source {
        _cause: PreparedWriterCallsError,
    },
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for TestSourceFailure
{
    fn from(cause: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource { _cause: cause }
    }
}
impl From<PreparedWriterCallsError> for TestSourceFailure {
    fn from(cause: PreparedWriterCallsError) -> Self {
        Self::Source { _cause: cause }
    }
}
struct Source {
    state: RuntimeState,
    tracker: Arc<MemTracker>,
    factory: Arc<dyn InputConstructionFactory>,
    calls: PreparedWriterAggregateCalls,
    input: ChunkSchemaRef,
    grouping_input: SlotId,
    grouping_output: SlotId,
}
fn source() -> Source {
    let (program, _binding, state) = compiled_statistics_finish_fixture();
    let tracker = MemTracker::new_root("prepared-final-original-owner");
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        state.error_state(),
    )
    .unwrap();
    let node = program.graph().root();
    let ProgramNodeKind::TableFinish {
        writer_multiplex_layout,
        final_aggregates,
        ..
    } = program.graph().nodes()[node.index()].kind()
    else {
        panic!("actual compiled finish");
    };
    let input = factory
        .create_compiled_chunk_schema(writer_multiplex_layout)
        .unwrap();
    let owner = factory.begin_typed_construction(&input).unwrap();
    owner
        .request(
            Layout::array::<PreparedWriterAggregateCall>(final_aggregates.calls.len()).unwrap(),
        )
        .unwrap();
    let mut calls = Vec::new();
    calls
        .try_reserve_exact(final_aggregates.calls.len())
        .unwrap();
    for (ordinal, call) in final_aggregates.calls.iter().enumerate() {
        let Some(ProgramStateTemplate::Aggregate { kernel, .. }) =
            program.state_template(ProgramCallSite::WriterFinal {
                node,
                call: u32::try_from(ordinal).unwrap(),
            })
        else {
            panic!("actual frozen final handle");
        };
        calls.push(PreparedWriterAggregateCall::new(
            kernel.clone(),
            call.intermediate_input_slot_id,
            call.final_output_slot_id,
            ordinal + 1,
            owner.storage_lifetime(),
        ));
    }
    if calls.capacity() != calls.len() {
        owner
            .request(Layout::array::<PreparedWriterAggregateCall>(calls.len()).unwrap())
            .unwrap();
    }
    let calls = PreparedWriterAggregateCalls::try_new_observed::<TestSourceFailure>(
        calls.into_boxed_slice(),
        AggregateKernelPhase::Final,
        owner.storage_lifetime(),
        |layout| {
            owner
                .request(layout)
                .map_err(|_cause| TestSourceFailure::Creator { _cause })
        },
        || {
            statistics_work(owner.as_ref(), 1)
                .map_err(|_cause| TestSourceFailure::Creator { _cause })
        },
    )
    .unwrap();
    let unpivot = final_aggregates
        .unpivot
        .as_ref()
        .expect("actual grouped declaration");
    let grouping_input = unpivot.grouping_input_slot_id;
    let grouping_output = unpivot.grouping_output_slot_id;
    drop(owner);
    Source {
        state,
        tracker,
        factory,
        calls,
        input,
        grouping_input,
        grouping_output,
    }
}
fn create(source: &Source) -> Box<WriterFinalStep> {
    let owner = source
        .factory
        .begin_typed_construction(&source.input)
        .unwrap();
    let mut operator = prepared_final_in(
        source.calls.clone(),
        source.grouping_input,
        source.grouping_output,
        source.input.clone(),
        source.state.error_state(),
        owner.as_ref(),
    )
    .unwrap();
    operator.set_construction_factory(source.factory.clone());
    operator.set_mem_tracker(source.tracker.clone());
    operator
}
#[test]
fn plan_tree_prepared_final_schema_and_last_independent_child_keep_actual_creation() {
    let source = source();
    let mut operator = create(&source);
    operator.prepare().unwrap();
    let slot = operator.kernel_for_test().output.as_ref().unwrap().slots()[1].clone();
    assert_eq!(
        slot.typed_binding(),
        source.calls.calls()[0]
            .handle()
            .contract()
            .final_type()
            .typed_binding()
    );
    let child = source.calls.calls()[0].clone();
    let tracker = source.tracker.clone();
    drop(operator);
    drop(source);
    assert!(tracker.current() > 0);
    drop(slot);
    assert!(
        tracker.current() > 0,
        "independent prepared child still owns original creation"
    );
    drop(child);
    assert_eq!(
        tracker.current(),
        0,
        "last source/schema reference releases finite construction"
    );
}
#[test]
fn plan_tree_prepared_final_refuses_before_installed_prepare_owner() {
    let source = source();
    let owner = source
        .factory
        .begin_typed_construction(&source.input)
        .unwrap();
    let mut operator = prepared_final_in(
        source.calls.clone(),
        source.grouping_input,
        source.grouping_output,
        source.input.clone(),
        source.state.error_state(),
        owner.as_ref(),
    )
    .unwrap();
    let failure = operator.prepare().expect_err("no factory fallback");
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
fn plan_tree_prepared_final_preserves_first_memory_and_original_caller_without_tail() {
    let source = source();
    let owner = source
        .factory
        .begin_typed_construction(&source.input)
        .unwrap();
    let baseline = source.tracker.current();
    source.tracker.install_limit_once(baseline).unwrap();
    let before = source.tracker.allocated();
    let failure = prepared_final_in(
        source.calls.clone(),
        source.grouping_input,
        source.grouping_output,
        source.input.clone(),
        source.state.error_state(),
        owner.as_ref(),
    )
    .err()
    .expect("first name construction is refused");
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(source.tracker.current(), baseline);
    assert_eq!(
        source.tracker.allocated() - before,
        i64::try_from("PREPARED_WRITER_FINAL_AGGREGATE".len()).unwrap(),
        "no Box request tail"
    );
    drop(owner);
    drop(source);
    let source = super_source();
    let owner = source
        .factory
        .begin_typed_construction(&source.input)
        .unwrap();
    let original = ExecutionFailure::from(KernelFailure::DeadlineExceeded)
        .at_operator(7, crate::runtime::task::PipelineOperation::Admission);
    source.state.error_state().set_error(original.clone());
    let before = source.tracker.allocated();
    let failure = prepared_final_in(
        source.calls.clone(),
        source.grouping_input,
        source.grouping_output,
        source.input.clone(),
        source.state.error_state(),
        owner.as_ref(),
    )
    .err()
    .expect("original caller stops before allocation");
    assert_eq!(failure.cause(), original.cause());
    assert_eq!(failure.site(), original.site());
    assert_eq!(source.tracker.allocated(), before);
    let tracker = source.tracker.clone();
    drop(owner);
    drop(source);
    assert_eq!(tracker.current(), 0);
}
fn super_source() -> Source {
    source()
}
