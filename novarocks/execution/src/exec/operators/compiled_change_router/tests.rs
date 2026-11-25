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
use crate::exec::pipeline::{
    operator::OperatorLifecycle,
    shape::{ControlStep, SinkKernel, SinkStep, Transform},
    step::NextStep,
    test_driver::TestDriver,
};
use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeState};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
struct Recorder {
    rows: Arc<Mutex<Vec<i8>>>,
    withdrawn: Arc<AtomicBool>,
}
struct RecordFactory {
    rows: Arc<Mutex<Vec<i8>>>,
    withdrawn: Arc<AtomicBool>,
}
impl OperatorFactory for RecordFactory {
    fn name(&self) -> &str {
        "route recorder"
    }
    fn create(&self, _: i32, _: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(Recorder {
            rows: self.rows.clone(),
            withdrawn: self.withdrawn.clone(),
        }))
    }
}
impl OperatorLifecycle for Recorder {
    fn name(&self) -> &str {
        "route recorder"
    }
}
impl InputEncodingPolicy for Recorder {}
impl SinkKernel for Recorder {
    fn poll_control(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        Ok(if self.withdrawn.load(Ordering::Acquire) {
            ControlStep::Withdrawn
        } else {
            ControlStep::Active
        })
    }
    fn consume(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        self.rows.lock().unwrap().extend(
            input.chunk().columns()[0]
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values()
                .iter()
                .copied(),
        );
        Ok(SinkStep::new(
            NextStep::NeedInput,
            input.chunk().len() as u64,
        ))
    }
}
fn input(values: Vec<Option<i8>>) -> Chunk {
    let n = values.len();
    crate::exec::chunk::typed_fixture::columns(
        &["effect"],
        &[SlotId(301)],
        &[novarocks_type_contract::FunctionValueType::new(
            DataType::Int8,
            true,
        )],
        vec![Arc::new(Int8Array::from(values))],
        n,
    )
}
fn prepare(operator: &mut dyn Operator, state: &RuntimeState) {
    operator.set_mem_tracker(MemTracker::new_root("router test"));
    operator.set_construction_factory(super::super::logical_change_summary::tests::creator(state));
    operator.bind_runtime_state(state).unwrap();
    operator.prepare().unwrap();
}
fn routes() -> Arc<[StaticRouterRoute]> {
    vec![
        StaticRouterRoute {
            branch: 0,
            layout: novarocks_local_program::ProgramWriteLayoutPreference::None,
            accepted_effects: Arc::from([Effect::Insert, Effect::Replace]),
        },
        StaticRouterRoute {
            branch: 1,
            layout: novarocks_local_program::ProgramWriteLayoutPreference::None,
            accepted_effects: Arc::from([Effect::Delete, Effect::Replace]),
        },
    ]
    .into()
}
#[test]
fn change_router_logical_nine_physical_five_and_seven_across_driver_and_batch_boundaries() {
    for dop in [1, 3, 4] {
        let summary = TaskSummaryState::new(
            novarocks_result_contract::SummaryKind::WriteCounts,
            dop,
            MemTracker::new_root("task contributors"),
        )
        .unwrap();
        let data = Arc::new(Mutex::new(Vec::new()));
        let deletes = Arc::new(Mutex::new(Vec::new()));
        let factory = CompiledChangeRouterFactory::new(
            SlotId(301),
            routes(),
            [data.clone(), deletes.clone()]
                .into_iter()
                .map(|rows| {
                    Arc::new(RecordFactory {
                        rows,
                        withdrawn: Arc::new(AtomicBool::new(false)),
                    }) as Arc<dyn OperatorFactory>
                })
                .collect(),
            Some(summary.clone()),
        )
        .unwrap();
        assert!(factory.bind_pipeline_dop(dop as i32 + 1).is_err());
        factory.bind_pipeline_dop(dop as i32).unwrap();
        let events = [3, 2, 1, 2, 1, 3, 1, 2, 1];
        for driver in 0..dop {
            let state = RuntimeState::default();
            let mut operator = factory.create(dop as i32, driver as i32);
            prepare(operator.as_mut(), &state);
            let mut calls = TestDriver::new(operator.as_mut(), &state);
            assert!(matches!(calls.drive().unwrap().next, NextStep::NeedInput));
            for rows in events
                .iter()
                .enumerate()
                .filter(|(i, _)| i % dop == driver)
                .map(|(_, v)| *v)
                .collect::<Vec<_>>()
                .chunks(2)
            {
                calls
                    .input_without_output(input(rows.iter().copied().map(Some).collect()))
                    .unwrap();
            }
            calls.signal_eos().unwrap();
            calls.collect_until_finished().unwrap();
        }
        assert_eq!(data.lock().unwrap().len(), 5);
        assert_eq!(deletes.lock().unwrap().len(), 7);
        // The real summary source, independent of all provider counters.
        let state = RuntimeState::default();
        let mut source = super::super::logical_change_summary::SummarySourceFactory::new(
            summary,
            super::super::logical_change_summary::tests::summary_layout(),
        )
        .unwrap()
        .create(1, 0);
        prepare(source.as_mut(), &state);
        let output = source.drive_for_test(&state).unwrap().output.unwrap();
        assert_eq!(
            output
                .columns()
                .iter()
                .map(|a| a
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(0))
                .collect::<Vec<_>>(),
            [2, 3, 4]
        );
    }
}
#[test]
fn change_router_validates_the_complete_effect_block_before_any_branch_side_effect() {
    for bad in [None, Some(0), Some(4)] {
        let state = RuntimeState::default();
        let summary = TaskSummaryState::new(
            novarocks_result_contract::SummaryKind::WriteCounts,
            1,
            MemTracker::new_root("invalid effect summary"),
        )
        .unwrap();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let factory = CompiledChangeRouterFactory::new(
            SlotId(301),
            routes(),
            (0..2)
                .map(|_| {
                    Arc::new(RecordFactory {
                        rows: recorded.clone(),
                        withdrawn: Arc::new(AtomicBool::new(false)),
                    }) as Arc<dyn OperatorFactory>
                })
                .collect(),
            Some(summary),
        )
        .unwrap();
        let mut operator = factory.create(1, 0);
        prepare(operator.as_mut(), &state);
        let mut calls = TestDriver::new(operator.as_mut(), &state);
        assert!(matches!(calls.drive().unwrap().next, NextStep::NeedInput));
        assert!(
            calls
                .input_without_output(input(vec![Some(3), Some(2), bad]))
                .is_err()
        );
        assert!(recorded.lock().unwrap().is_empty());
    }
}
#[test]
fn change_router_required_branch_withdrawal_is_an_error_even_before_the_first_batch() {
    let state = RuntimeState::default();
    let summary = TaskSummaryState::new(
        novarocks_result_contract::SummaryKind::WriteCounts,
        1,
        MemTracker::new_root("strict summary"),
    )
    .unwrap();
    let factory = CompiledChangeRouterFactory::new(
        SlotId(301),
        routes(),
        (0..2)
            .map(|_| {
                Arc::new(RecordFactory {
                    rows: Arc::new(Mutex::new(Vec::new())),
                    withdrawn: Arc::new(AtomicBool::new(true)),
                }) as Arc<dyn OperatorFactory>
            })
            .collect(),
        Some(summary),
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    prepare(operator.as_mut(), &state);
    let errors = state.error_state();
    let mut work = crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 1);
    let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    assert!(
        operator
            .poll_control(&mut DriverStep::new(&errors, &mut work, &mut conversions))
            .is_err()
    );
}

#[test]
fn insert_only_routes_refuse_a_delete_effect() {
    use crate::exec::fragment::error::{
        FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
    };
    use crate::runtime::task::ExecutionFailureCause;

    let state = RuntimeState::default();
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let factory = CompiledChangeRouterFactory::new(
        SlotId(301),
        Arc::from([StaticRouterRoute {
            branch: 0,
            layout: novarocks_local_program::ProgramWriteLayoutPreference::None,
            accepted_effects: Arc::from([Effect::Insert]),
        }]),
        vec![Arc::new(RecordFactory {
            rows: recorded.clone(),
            withdrawn: Arc::new(AtomicBool::new(false)),
        })],
        None,
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    prepare(operator.as_mut(), &state);
    let mut calls = TestDriver::new(operator.as_mut(), &state);
    assert!(matches!(calls.drive().unwrap().next, NextStep::NeedInput));

    let insert = Some(Effect::Insert as i8);
    calls.input_without_output(input(vec![insert])).unwrap();
    assert_eq!(*recorded.lock().unwrap(), [Effect::Insert as i8]);

    let failure = calls
        .input_without_output(input(vec![insert, Some(Effect::Delete as i8)]))
        .unwrap_err();
    let ExecutionFailureCause::Domain(cause) = failure.cause() else {
        panic!("expected a frozen program binding error, got {failure}");
    };
    let binding = cause.downcast_ref::<FragmentBindingError>().unwrap();
    assert_eq!(binding.target(), FragmentBindingTarget::Program);
    assert_eq!(binding.kind(), FragmentBindingErrorKind::InvalidAssignment);
    assert_eq!(
        binding.detail(),
        "change router effect has no accepting provider route"
    );
    assert_eq!(*recorded.lock().unwrap(), [Effect::Insert as i8]);
}
