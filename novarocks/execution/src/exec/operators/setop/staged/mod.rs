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

mod artifact;
mod finalize;
mod frame;
mod input;
mod tail;

pub(crate) use finalize::SetOpFinalizeFactory;
pub(crate) use frame::SetOpDeclaration;
pub(crate) use input::SetOpInputFactory;
pub(crate) use tail::SetOpTailFactory;

use crate::{
    exec::{
        chunk::ChunkSchemaRef,
        hash_table::{allocation::AggregateAllocator, key_index::BulkWork},
        operators::compiled_expression::RuntimeKernelControl,
        pipeline::{
            driver_step::DriverStep,
            operator::{ConstructionFailure, InputConstructionFactory, InputConstructionOwner},
            step::{NextStep, WaitFact, WaitReason},
        },
    },
    runtime::{
        mem_tracker::MemTracker,
        observable::Observable,
        runtime_state::{RuntimeErrorState, RuntimeState},
        task::{ExecutionFailure, ExecutionReason, ExecutionResult},
    },
};
use novarocks_failure_contract::{BoundedDetail, FailureClass, FailureFact, FailureProjection};
use std::sync::Arc;

#[derive(Debug)]
struct SetOperationContract(BoundedDetail);

impl std::fmt::Display for SetOperationContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "compiled set operation: {}", self.0)
    }
}

impl std::error::Error for SetOperationContract {}

impl FailureProjection for SetOperationContract {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::SetOperationStageContract.reason(),
        )
    }
}

pub(super) fn contract(detail: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_domain(SetOperationContract(BoundedDetail::truncating_display(
        detail,
    )))
}

struct PreparedPorts {
    schema: ChunkSchemaRef,
    frame: Arc<frame::SetOpFrame>,
}

struct OperatorPorts {
    declaration: Arc<SetOpDeclaration>,
    error: Arc<RuntimeErrorState>,
    control: RuntimeKernelControl,
    factory: Option<Arc<dyn InputConstructionFactory>>,
    tracker: Option<Arc<MemTracker>>,
    prepared: Option<PreparedPorts>,
}

impl OperatorPorts {
    fn new(declaration: Arc<SetOpDeclaration>, error: Arc<RuntimeErrorState>) -> Self {
        Self {
            control: RuntimeKernelControl::new(error.clone()).at_expression_node(declaration.node),
            error,
            declaration,
            factory: None,
            tracker: None,
            prepared: None,
        }
    }

    fn set_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(factory.clone());
        self.factory = Some(factory);
    }

    fn set_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(tracker.clone());
        self.tracker = Some(tracker);
    }

    fn bind_runtime(&mut self, state: &RuntimeState) {
        self.control.bind_runtime_memory(state);
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.prepared.is_some() {
            return Ok(());
        }
        let schema = self
            .factory
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?
            .create_compiled_chunk_schema(&self.declaration.layout)?;
        let owner = self.control.construction_owner(&schema)?;
        let frame = self
            .declaration
            .prepare(&schema, &self.work(owner.as_ref())?)?;
        self.prepared = Some(PreparedPorts { frame, schema });
        Ok(())
    }

    fn activate(&self) -> ExecutionResult<()> {
        let prep = self.prepared()?;
        let owner = self.control.construction_owner(&prep.schema)?;
        prep.frame.completion.activate(owner.as_ref())
    }

    fn prepared(&self) -> ExecutionResult<&PreparedPorts> {
        self.prepared
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }

    fn work<'a>(&'a self, owner: &'a dyn InputConstructionOwner) -> ExecutionResult<BulkWork<'a>> {
        Ok(BulkWork {
            owner,
            allocator: AggregateAllocator::new(
                self.tracker
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?
                    .clone(),
            ),
            host: self
                .control
                .allocator()
                .ok_or(ConstructionFailure::MissingOwner)?,
            control: &self.control,
        })
    }

    fn observable(&self) -> Option<Arc<Observable>> {
        self.prepared
            .as_ref()
            .map(|prep| prep.frame.completion.observable())
    }

    fn report(
        &self,
        ctx: &DriverStep<'_>,
        cause: ExecutionFailure,
        retain: impl FnOnce(ExecutionFailure) -> ExecutionFailure,
    ) -> ExecutionFailure {
        let cause = retain(ctx.locate(self.control.project_required_failure(cause)));
        ctx.report_failure(cause.clone());
        cause
    }
}

fn wait_fact(observable: Arc<Observable>, generation: u64) -> WaitFact {
    WaitFact {
        reason: WaitReason::Dependency,
        observable,
        observed_generation: generation,
        deadline: None,
    }
}

fn wait(observable: Arc<Observable>, generation: u64) -> NextStep {
    if observable.generation() == generation {
        NextStep::Wait(wait_fact(observable, generation))
    } else {
        NextStep::Runnable
    }
}

fn installed_dop(dop: i32, expected: usize) -> ExecutionResult<()> {
    if usize::try_from(dop).ok() == Some(expected) {
        Ok(())
    } else {
        Err(contract(
            "pipeline DOP differs from the installed set operation roster",
        ))
    }
}

fn installed_slot(dop: i32, driver: i32, expected: usize) -> ExecutionResult<usize> {
    installed_dop(dop, expected)?;
    usize::try_from(driver)
        .ok()
        .filter(|driver| *driver < expected)
        .ok_or_else(|| contract("set operation driver has no installed position"))
}

#[cfg(test)]
mod tests;
