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

use super::super::{
    SetOpDeclaration,
    finalize::FinalizeWorker,
    frame::{SetOpFrame, capture_work},
};
use crate::{
    exec::{
        chunk::typed_fixture,
        hash_table::{
            allocation::AggregateAllocator,
            key_index::{BulkMatch, BulkWork, EvaluatedPage, KeyInput, PreparedKeyBatch},
        },
        operators::local_exchanger::LocalExchanger,
        pipeline::{
            driver_step::TurnWork,
            operator::{
                InputConstructionOwner, input_construction::try_runtime_input_construction_factory,
            },
        },
        shared_stage::stage_completion::StageParticipants,
    },
    runtime::{
        mem_tracker::MemTracker, runtime_state::RuntimeErrorState,
        tracked_state_allocator::expression_allocation_host,
    },
};
use arrow::{
    array::ArrayRef,
    datatypes::{Field, Schema},
};
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use novarocks_local_program::{ProgramNodeId, SetOpKind};
use novarocks_type_contract::FunctionValueType;
use novarocks_types::SlotId;
use novarocks_value_arrays::{decode::ValueConversionContext, reachability::ReachableRows};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("test does not sleep")
    }
}

pub(super) struct Fixture {
    pub(super) frame: Arc<SetOpFrame>,
    pub(super) handoff: Arc<LocalExchanger>,
    pub(super) tracker: Arc<MemTracker>,
    owner: Box<dyn InputConstructionOwner>,
}

impl Fixture {
    pub(super) fn new(kind: SetOpKind, ty: &FunctionValueType, lanes: usize) -> Self {
        let tracker = MemTracker::new_root("set operation original creator");
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let layout = typed_fixture::layout(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                ty.data_type().clone(),
                ty.nullable(),
            )])),
            &[SlotId::new(1)],
            std::slice::from_ref(ty),
        );
        let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
        let owner = factory.begin_typed_construction(&schema).unwrap();
        let handoff = LocalExchanger::new_handoff(3, 2, 4);
        let declaration = SetOpDeclaration::new(
            ProgramNodeId::new(0),
            layout,
            kind,
            lanes,
            StageParticipants {
                inputs: 2,
                finalizers: NonZeroUsize::new(3).unwrap(),
                probes: 2 * lanes,
                tails: 3,
            },
            handoff.clone(),
        );
        let work = Self::ports(owner.as_ref(), &tracker);
        let frame = declaration.prepare(&schema, &work).unwrap();
        frame.completion.activate(owner.as_ref()).unwrap();
        Self {
            frame,
            handoff,
            tracker,
            owner,
        }
    }

    fn ports<'a>(owner: &'a dyn InputConstructionOwner, tracker: &Arc<MemTracker>) -> BulkWork<'a> {
        BulkWork {
            owner,
            allocator: AggregateAllocator::new(tracker.clone()),
            host: expression_allocation_host(tracker.clone()),
            control: &Control,
        }
    }

    pub(super) fn work(&self) -> BulkWork<'_> {
        Self::ports(self.owner.as_ref(), &self.tracker)
    }

    pub(super) fn input(&self, slot: usize, array: ArrayRef) {
        let work = self.work();
        let ports = capture_work(&work);
        let mut writer = self.frame.domain.begin_capture(&ports).unwrap();
        let readable = ReachableRows::all(array.len())
            .materialize_observed(&mut |_| Ok::<_, KernelFailure>(()), &mut |_| Ok(()))
            .unwrap();
        let occurrences = (0..array.len()).collect::<Vec<_>>();
        writer
            .push_evaluated(
                EvaluatedPage {
                    arrays: &[array],
                    bindings: self.frame.domain.spec().types(),
                    readable: &readable,
                    occurrences: &occurrences,
                },
                &mut ValueConversionContext::default(),
                &ports,
            )
            .unwrap();
        self.frame
            .inputs
            .normal_input(slot, writer.seal(&ports).unwrap())
            .unwrap();
    }

    pub(super) fn workers(&self) -> [FinalizeWorker; 3] {
        std::array::from_fn(|_| FinalizeWorker::new(&self.work()))
    }

    pub(super) fn finalize(&self, workers: &mut [FinalizeWorker; 3]) {
        for _ in 0..5000 {
            for worker in workers.iter_mut() {
                worker.step(&self.frame, &mut turn(), &self.work()).unwrap();
            }
            if self.frame.completion.published().unwrap().is_some() {
                return;
            }
        }
        panic!("all workers must reach original publication")
    }

    pub(super) fn probe(&self, lane: usize, slot: usize, array: ArrayRef) {
        let published = self.frame.completion.published().unwrap().unwrap();
        let view = &published.artifact.keys.view;
        let spec = view.spec();
        let work = self.work();
        let readable = ReachableRows::all(array.len())
            .materialize_observed(&mut |_| Ok::<_, KernelFailure>(()), &mut |_| Ok(()))
            .unwrap();
        let rows = (0..array.len()).collect::<Vec<_>>();
        let input = KeyInput::new_for_spec(
            spec,
            &[array],
            spec.types(),
            &readable,
            &rows,
            &mut ValueConversionContext::default(),
            work.owner,
            &|| Ok(()),
        )
        .unwrap();
        let prepared =
            PreparedKeyBatch::from_captured(spec, &input, work.allocator.clone(), &|| Ok(()))
                .unwrap();
        let found = view
            .probe(&prepared, None, work.owner, work.control)
            .unwrap();
        for &key in found.matches() {
            match key {
                BulkMatch::Found(key) => published.artifact.mark(lane, key).unwrap(),
                BulkMatch::Miss => {}
                BulkMatch::Unmatchable => panic!("NULL-equal set keys participate"),
            }
        }
        self.frame.completion.normal_after_output(slot).unwrap();
    }
}

pub(super) fn turn() -> TurnWork {
    TurnWork::new(NonZeroU64::MIN, 64)
}
