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

//! An effect-validated, strict fanout over the original compiled input layout.
//! Each branch's DataStreamSink performs its own frozen wire projection. Keeping
//! the complete root input here preserves compiled partition-root bindings.
// Design: ADR-0208 (docs/adr/ADR-0208-logical-write-counts-and-strict-stream-delivery.md)
use super::{
    compiled_writer_failure::contract,
    data_stream_sink::take_partition_rows_created,
    logical_change_summary::{LogicalSummaryContributor, LogicalSummaryState},
};
use crate::{
    exec::{
        chunk::Chunk,
        pipeline::{
            input::PreparedInput,
            operator::{InputConstructionFactory, InputEncodingPolicy, Operator},
            operator_factory::OperatorFactory,
            shape::fanout::{Fanout, FanoutRouter, RouteStep},
            step_context::StepContext,
        },
    },
    runtime::{fragment::ExecutionResult, observable::Observable},
};
use arrow::{
    array::{Array, Int8Array},
    datatypes::DataType,
};
use novarocks_local_program::{LogicalChangeCounts, StaticRouterRoute};
use novarocks_spi::connector::ConnectorRowMutationEffect as Effect;
use novarocks_types::SlotId;
use std::sync::Arc;

pub(crate) struct CompiledChangeRouterFactory {
    name: String,
    effect: SlotId,
    routes: Arc<[StaticRouterRoute]>,
    branches: Vec<Arc<dyn OperatorFactory>>,
    summary: LogicalSummaryState,
}
impl CompiledChangeRouterFactory {
    /// Branches are provider routes only. The Task's one summary source is an
    /// independent pipeline, so it never waits for writer finalization.
    pub(crate) fn new(
        effect: SlotId,
        routes: Arc<[StaticRouterRoute]>,
        branches: Vec<Arc<dyn OperatorFactory>>,
        summary: LogicalSummaryState,
    ) -> ExecutionResult<Self> {
        if routes.is_empty()
            || routes.len() != branches.len()
            || routes
                .iter()
                .enumerate()
                .any(|(i, r)| r.branch as usize != i || r.accepted_effects.is_empty())
        {
            return Err(contract(
                "change router provider routes are not an exact dense branch set",
            ));
        }
        Ok(Self {
            name: "CompiledChangeRouter".into(),
            effect,
            routes,
            branches,
            summary,
        })
    }
}
impl OperatorFactory for CompiledChangeRouterFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn is_sink(&self) -> bool {
        true
    }
    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        self.summary.validate_geometry(dop)?;
        for branch in &self.branches {
            branch.bind_pipeline_dop(dop)?;
        }
        Ok(())
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let contribution = self.summary.validate_geometry(dop).and_then(|()| {
            usize::try_from(driver_id)
                .map_err(|_| contract("change router has a negative driver ordinal"))
                .and_then(|id| self.summary.contributor(id))
        });
        let (contributor, failure) = match contribution {
            Ok(c) => (Some(c), None),
            Err(e) => (None, Some(e)),
        };
        Box::new(Fanout::new(
            self.name.clone(),
            self.branches
                .iter()
                .map(|b| b.create(dop, driver_id))
                .collect(),
            ChangeRouter {
                effect: self.effect,
                routes: self.routes.clone(),
                contributor,
                exhausted: false,
            },
            failure,
        ))
    }
}
struct ChangeRouter {
    effect: SlotId,
    routes: Arc<[StaticRouterRoute]>,
    contributor: Option<LogicalSummaryContributor>,
    exhausted: bool,
}
pub(crate) struct ChangeCursor {
    input: Chunk,
    effect_ordinal: usize,
    next: usize,
}
impl InputEncodingPolicy for ChangeRouter {
    fn retains_encoded_column(&self, slot: SlotId, _: &DataType) -> bool {
        slot != self.effect
    }
    fn retains_encoded_path(
        &self,
        value: crate::exec::pipeline::operator::EncodedValueOccurrence<'_>,
    ) -> bool {
        value.slot != self.effect
    }
}
impl FanoutRouter for ChangeRouter {
    type Cursor = ChangeCursor;
    fn begin(
        &mut self,
        input: PreparedInput,
        _: &dyn InputConstructionFactory,
        ctx: &mut StepContext<'_>,
    ) -> ExecutionResult<ChangeCursor> {
        let input = input.into_uncovered_chunk()?;
        let ordinal = *input
            .slot_id_to_index()
            .get(&self.effect)
            .ok_or_else(|| contract("change router input has no frozen effect slot"))?;
        let effects = input.columns()[ordinal]
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| contract("change router effect is not Int8"))?;
        let mut counts = LogicalChangeCounts::ZERO;
        // Validate the WHOLE block before any branch projection or contribution.
        for row in 0..effects.len() {
            if row % 256 == 0 {
                ctx.checkpoint()?;
            }
            let effect = if effects.is_null(row) {
                return Err(contract("change router effect is NULL"));
            } else {
                match effects.value(row) {
                    1 => Effect::Delete,
                    2 => Effect::Replace,
                    3 => Effect::Insert,
                    _ => {
                        return Err(contract(
                            "change router effect is outside the frozen vocabulary",
                        ));
                    }
                }
            };
            if !self
                .routes
                .iter()
                .any(|route| route.accepted_effects.contains(&effect))
            {
                return Err(contract(
                    "change router effect has no accepting provider route",
                ));
            }
            let delta = match effect {
                Effect::Insert => LogicalChangeCounts::INSERT,
                Effect::Replace => LogicalChangeCounts::REPLACE,
                Effect::Delete => LogicalChangeCounts::DELETE,
            };
            counts = counts.checked_add(delta).map_err(|e| {
                super::logical_change_summary::summary_failure(
                    super::logical_change_summary::SummaryFailure::Count(e),
                )
            })?;
        }
        ctx.checkpoint()?;
        self.contributor
            .as_mut()
            .ok_or_else(|| contract("change router contributor is missing"))?
            .accept(counts)?;
        Ok(ChangeCursor {
            input,
            effect_ordinal: ordinal,
            next: 0,
        })
    }
    fn advance(
        &mut self,
        cursor: &mut ChangeCursor,
        factory: &dyn InputConstructionFactory,
        ctx: &mut StepContext<'_>,
    ) -> ExecutionResult<RouteStep> {
        let Some(route) = self.routes.get(cursor.next) else {
            return Ok(RouteStep::Finished);
        };
        cursor.next += 1;
        let effects = cursor.input.columns()[cursor.effect_ordinal]
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("whole block validated before branch work");
        let owner = factory.begin_input_construction(&cursor.input)?;
        owner.request(std::alloc::Layout::array::<u32>(effects.len()).map_err(|_|crate::exec::pipeline::operator::ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut rows = Vec::with_capacity(effects.len());
        for row in 0..effects.len() {
            if row % 256 == 0 {
                ctx.checkpoint()?;
            }
            if route
                .accepted_effects
                .iter()
                .any(|e| *e as i8 == effects.value(row))
            {
                rows.push(
                    u32::try_from(row)
                        .map_err(|_| contract("change router row index exceeds UInt32"))?,
                );
            }
        }
        // Empty routes do no projection; continue the bounded cursor next turn.
        if rows.is_empty() {
            return Ok(RouteStep::Skip);
        }
        let chunk =
            take_partition_rows_created(&cursor.input, &rows, factory, &|| ctx.checkpoint())?;
        Ok(RouteStep::Input {
            branch: route.branch as usize,
            chunk,
        })
    }
    fn accepts_branch(&self, _: usize) -> bool {
        true
    }
    fn requires_branch_drain(&self, _: usize) -> bool {
        true
    }
    fn input_exhausted(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<()> {
        ctx.checkpoint()?;
        if !self.exhausted {
            self.contributor
                .as_mut()
                .ok_or_else(|| contract("change router contributor is missing"))?
                .finish()?;
            self.exhausted = true;
        }
        Ok(())
    }
    fn visit_demand_events(&self, _: &mut dyn FnMut(Arc<Observable>)) {}
}

#[cfg(test)]
mod tests {
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
        fn poll_control(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
            Ok(if self.withdrawn.load(Ordering::Acquire) {
                ControlStep::Withdrawn
            } else {
                ControlStep::Active
            })
        }
        fn consume(
            &mut self,
            _: &mut StepContext<'_>,
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
        operator
            .set_construction_factory(super::super::logical_change_summary::tests::creator(state));
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
            let summary =
                LogicalSummaryState::new(dop, MemTracker::new_root("task contributors")).unwrap();
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
                summary.clone(),
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
            let mut source =
                super::super::logical_change_summary::LogicalSummarySourceFactory::new(
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
            let summary =
                LogicalSummaryState::new(1, MemTracker::new_root("invalid effect summary"))
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
                summary,
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
        let summary = LogicalSummaryState::new(1, MemTracker::new_root("strict summary")).unwrap();
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
            summary,
        )
        .unwrap();
        let mut operator = factory.create(1, 0);
        prepare(operator.as_mut(), &state);
        let errors = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 1);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        assert!(
            operator
                .poll_control(&mut StepContext::new(&errors, &mut work, &mut conversions))
                .is_err()
        );
    }
}
