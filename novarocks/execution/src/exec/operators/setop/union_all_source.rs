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
//! UNION ALL source operator.
//!
//! Responsibilities:
//! - Pulls buffered chunks from shared UNION ALL state and emits merged output stream.
//! - Terminates when all sink branches are finished and queues are drained.
//!
//! Key exported interfaces:
//! - Types: `UnionAllSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::task::ExecutionResult;

use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{Source, SourceKernel, SourceRead, SourceStart};
use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};

use super::union_all_shared::UnionAllSharedState;
#[cfg(test)]
use crate::runtime::runtime_state::RuntimeState;

/// Factory for UNION ALL source operators that emit merged branch chunk streams.
pub struct UnionAllSourceFactory {
    name: String,
    state: UnionAllSharedState,
}

impl UnionAllSourceFactory {
    pub(crate) fn new(state: UnionAllSharedState, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("UnionAllSource (id={node_id})")
        } else {
            "UnionAllSource".to_string()
        };
        Self { name, state }
    }
}

impl OperatorFactory for UnionAllSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        // UNION ALL fan-in owns a single-consumer shared queue: only the
        // pipeline-local driver 0 drains it.
        Box::new(Source::new(UnionAllSourceOperator {
            name: self.name.clone(),
            state: self.state.clone(),
            active: driver_id == 0,
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct UnionAllSourceOperator {
    name: String,
    state: UnionAllSharedState,
    active: bool,
}

impl OperatorLifecycle for UnionAllSourceOperator {
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        if self.active {
            self.state.close_consumer();
        }
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        if self.active {
            self.state.close_consumer();
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl SourceKernel for UnionAllSourceOperator {
    fn start(&self) -> SourceStart {
        if self.active {
            SourceStart::Ready
        } else {
            SourceStart::Finished
        }
    }
    fn poll_output(
        &mut self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<StepResult> {
        let observable = self.state.observable();
        let observed_generation = observable.generation();
        let (output, next) = match self.state.next_chunk() {
            SourceRead::Chunk(chunk) => (Some(chunk), NextStep::Runnable),
            SourceRead::Finished => (None, NextStep::Finished),
            SourceRead::Pending => (
                None,
                NextStep::Wait(WaitFact {
                    reason: WaitReason::Input,
                    observable,
                    observed_generation,
                    deadline: None,
                }),
            ),
        };
        Ok(StepResult::new(output, next, 0))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use novarocks_types::SlotId;

    fn one_row_chunk() -> Chunk {
        let field = Field::new("v", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .expect("record batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(SlotId(1), &field, None).expect("chunk slot"),
            ])
            .expect("chunk schema"),
        );
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    #[test]
    fn last_producer_publication_is_delivered_before_source_eos() {
        use crate::exec::pipeline::driver_step::{DriverStep, TurnWork};
        use std::num::NonZeroU64;
        let state = RuntimeState::default();
        let shared = UnionAllSharedState::new_with_limits(2, 7, 1024, 1024);
        let mut source = UnionAllSourceFactory::new(shared.clone(), 7).create(1, 0);
        let control = state.error_state();
        let mut work = TurnWork::new(NonZeroU64::new(1).unwrap(), 8);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut context = DriverStep::new(&control, &mut work, &mut value_conversions);
        let source = source.as_mut();
        let waiting = source.drive(&mut context).unwrap();
        assert!(waiting.output.is_none());
        let NextStep::Wait(wait) = waiting.next else {
            panic!("an empty live source must wait");
        };
        shared.producer_finished();
        assert!(matches!(
            source.drive(&mut context).unwrap().next,
            NextStep::Wait(_)
        ));
        // Publication and last-producer close precede any wait registration.
        // The frozen generation must still force a poll of the retained row.
        shared
            .push_chunk(shared.queue_mem_tracker(&state).as_ref(), one_row_chunk())
            .unwrap();
        shared.producer_finished();
        assert!(wait.is_ready());
        let data = source.drive(&mut context).unwrap();
        assert_eq!(data.output.unwrap().len(), 1);
        assert!(matches!(data.next, NextStep::Runnable));
        let eos = source.drive(&mut context).unwrap();
        assert!(eos.output.is_none());
        assert!(matches!(eos.next, NextStep::Finished));
        assert!(source.drive(&mut context).unwrap().output.is_none());
    }

    #[test]
    fn non_zero_source_driver_does_not_drain_shared_queue() {
        let state = RuntimeState::default();
        let shared = UnionAllSharedState::new_with_limits(1, 7, 1024, 1024);
        shared
            .push_chunk(shared.queue_mem_tracker(&state).as_ref(), one_row_chunk())
            .unwrap();

        let factory = UnionAllSourceFactory::new(shared.clone(), 7);
        let mut inactive = factory.create(2, 1);
        let idle = inactive.drive_for_test(&state).unwrap();
        assert!(idle.output.is_none());
        assert!(matches!(idle.next, NextStep::Finished));

        let mut active = factory.create(2, 0);
        let step = active.drive_for_test(&state).unwrap();
        assert_eq!(step.output.expect("active source chunk").len(), 1);
        assert!(!matches!(step.next, NextStep::Finished));
    }
}
