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

//! One metadata batch per pull; immutable Arrow buffers are shared without concatenation.

use crate::exec::pipeline::operator::OperatorLifecycle;
use std::sync::Arc;

use crate::exec::pipeline::operator::ConstructionFailure;
use arrow::array::ArrayRef;
use novarocks_functions::{KernelDiagnostic, KernelFailure};
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::operators::compiled_expression::RuntimeKernelControl;
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{Source, SourceKernel, SourceStart};
use crate::exec::pipeline::step::{NextStep, StepResult};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

fn invalid(message: &str) -> ExecutionFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message)).into()
}

/// Source factory for one compiled system relation node that has dynamic cells.
pub(crate) struct CompiledSystemRelationSourceFactory {
    name: String,
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
    error: Arc<RuntimeErrorState>,
}

impl CompiledSystemRelationSourceFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> crate::runtime::task::ExecutionResult<Self> {
        let graph_node = program.graph().nodes().get(node.index()).ok_or_else(|| {
            crate::exec::fragment::error::program_binding_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::MissingAssignment,
                "compiled system relation node is absent",
            )
        })?;
        let ProgramNodeKind::SystemRelation { batches: values } = graph_node.kind() else {
            return Err(crate::exec::fragment::error::program_binding_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                "compiled node is not a system relation source",
            ));
        };
        if !values.layout().is_typed() {
            return Err(crate::exec::fragment::error::program_binding_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                "compiled system relation require their complete frozen layout",
            ));
        }
        Ok(Self {
            name: format!("COMPILED_SYSTEM_RELATION (node={})", node.index()),
            program,
            node,
            error,
        })
    }
}

impl OperatorFactory for CompiledSystemRelationSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(Source::new(CompiledSystemRelationSource {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            node: self.node,
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            driver_id,
            cursor: 0,
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct CompiledSystemRelationSource {
    name: String,
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    control: RuntimeKernelControl,
    driver_id: i32,
    cursor: usize,
}

impl OperatorLifecycle for CompiledSystemRelationSource {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.control.bind_construction_factory(factory.clone());
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        let ProgramNodeKind::SystemRelation { batches: values } =
            self.program.graph().nodes()[self.node.index()].kind()
        else {
            return Err(invalid("compiled system relation node changed"));
        };
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_compiled_layout_construction(values.layout())?;
        let output = ChunkSchema::from_compiled_layout_owned_observed::<ConstructionFailure>(
            values.layout(),
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )?;
        self.output = Some(output);
        Ok(())
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.control.bind_mem_tracker(tracker);
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl SourceKernel for CompiledSystemRelationSource {
    fn start(&self) -> SourceStart {
        if self.driver_id != 0 {
            SourceStart::Finished
        } else {
            SourceStart::Ready
        }
    }
    fn poll_output(
        &mut self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<StepResult> {
        // The source pipeline has one driver; every row comes from it, so the
        // cells are evaluated once and rows keep their frozen order.
        if self.driver_id != 0 {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        let output = self
            .output
            .as_ref()
            .ok_or_else(|| invalid("compiled system relation source was not prepared"))?;
        let ProgramNodeKind::SystemRelation { batches } =
            self.program.graph().nodes()[self.node.index()].kind()
        else {
            return Err(invalid("system relation node changed"));
        };
        let Some(batch) = batches.batches().get(self.cursor) else {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        };
        let owner = self.control.construction_owner(output.as_ref())?;
        let storage = owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(&batches.storage_lifetime(), |l| {
                owner.request(l)
            })?;
        owner.request(std::alloc::Layout::array::<ArrayRef>(batch.num_columns()).map_err(|_|ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let chunk = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            output.clone(),
            batch.columns().to_vec(),
            batch.num_rows(),
            storage,
            |step| owner.observe(0, &[], step),
        )?;
        self.cursor += 1;
        let next = if self.cursor == batches.batches().len() {
            NextStep::Finished
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(chunk), next, 0))
    }
}
