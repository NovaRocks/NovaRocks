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

//! Lifecycle forwarding for the receiving preparation and computation owners.

use super::{HydrateInput, InputPreparation, NextStep, Transform, TransformKernel};
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::exec::pipeline::operator::{
    ExitObservation, FinishWatch, InputConstructionFactory, InputEncodingPolicy,
};
use crate::runtime::{
    mem_tracker::MemTracker,
    profile::OperatorProfiles,
    runtime_state::RuntimeState,
    task::{ExecutionResult, io::TaskEventSink},
};
use std::sync::Arc;

impl<K: TransformKernel + InputEncodingPolicy> Transform<K, HydrateInput> {
    pub fn hydrating(kernel: K) -> Self {
        Self::new(kernel, HydrateInput::default())
    }
}

// Lifecycle forwarding keeps each kernel's private bindings on the same owner.
impl<K: TransformKernel + InputEncodingPolicy + OperatorLifecycle, P: InputPreparation<K>>
    OperatorLifecycle for Transform<K, P>
{
    fn name(&self) -> &str {
        self.kernel.name()
    }

    fn bind_poll_budget(
        &mut self,
        budget: novarocks_spi::connector::read_stack::ConnectorPollBudget,
    ) {
        self.kernel.bind_poll_budget(budget);
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.receiver
            .preparation
            .set_mem_tracker(Arc::clone(&tracker));
        self.kernel.set_mem_tracker(tracker);
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.receiver
            .preparation
            .set_construction_factory(Arc::clone(&factory));
        self.kernel.set_construction_factory(factory);
    }
    fn creates_typed_storage(&self) -> bool {
        self.receiver.preparation.creates_typed_storage() || self.kernel.creates_typed_storage()
    }
    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        self.receiver.preparation.set_profiles(profiles.clone());
        self.kernel.set_profiles(profiles);
    }
    fn compiled_schema_preparation_site(
        &self,
    ) -> Option<(
        &Arc<novarocks_local_program::LocalProgram>,
        crate::runtime::preparation_metadata::CompiledSchemaSite,
    )> {
        self.kernel.compiled_schema_preparation_site()
    }
    #[cfg(any(test, novarocks_dev_controls))]
    fn prepared_schema_for_test(&self) -> Option<crate::exec::chunk::ChunkSchemaRef> {
        self.kernel.prepared_schema_for_test()
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        let result = self.kernel.prepare();
        if let Err(failure) = &result {
            self.stop_work(failure.clone());
        }
        result
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.kernel.bind_runtime_state(state)
    }
    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.kernel.activate(state)
    }
    fn set_task_event_sink(&mut self, sink: Arc<dyn TaskEventSink>) {
        self.kernel.set_task_event_sink(sink);
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.next = NextStep::Finished;
        self.receiver.discard();
        self.kernel.begin_exit()
    }
    fn observe_exit(&mut self) -> ExitObservation {
        self.kernel.observe_exit()
    }
    fn visit_exits(&mut self, visit: &mut dyn FnMut(ExitObservation)) {
        self.kernel.visit_exits(visit);
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.next = NextStep::Finished;
        let result = self.kernel.close();
        self.receiver.discard();
        result
    }
    fn cancel(&mut self) {
        self.next = NextStep::Finished;
        self.receiver.discard();
        self.kernel.cancel();
    }
    fn on_driver_failure(&mut self) {
        self.next = NextStep::Finished;
        self.receiver.discard();
        self.kernel.on_driver_failure();
    }

    fn pending_finish(&self) -> Option<FinishWatch> {
        self.kernel.pending_finish()
    }
}
