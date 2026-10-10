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

//! Original task construction and synchronous scalar owners for GLOBAL tests.
//! The Frame entry and the complete-wrapper contract are distinct witnesses.

use super::{RuntimeKernelControl, evaluate_all};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::runtime::{
    execution_runtime::ExecutionRuntime,
    fragment::{
        ExecutionResult,
        runtime_state::{RuntimeStateInputs, build_runtime_state},
    },
    kernel_memory::KernelMemoryJournal,
    query_memory::QueryMemoryBinding,
    runtime_state::RuntimeState,
    scalar_memory::{
        RuntimeScalarEvaluationFailure, RuntimeScalarMemoryJournal, RuntimeScalarMemoryScope,
        RuntimeScalarOperationScope,
    },
};
use arrow::{array::ArrayRef, record_batch::RecordBatch};
use novarocks_execution_contract::TaskIdentity;
use novarocks_functions::{EvaluatedArgument, ScalarEvaluationInstance, SelectedValues, Selection};
use novarocks_local_program::{LocalProgram, ProgramExpressionRootSite, ProgramStateTemplate};
use std::sync::Arc;

fn task_state(
    runtime: Arc<ExecutionRuntime>,
    task: TaskIdentity,
    memory: Option<QueryMemoryBinding>,
) -> Result<Arc<RuntimeState>, String> {
    build_runtime_state(RuntimeStateInputs {
        query_options: None,
        query_id: Some(task.query_execution_id().query_id()),
        fragment_instance_id: None,
        backend_num: None,
        mem_tracker: None,
        runtime_filter_session: None,
        execution_runtime: Some(runtime),
        query_memory: memory,
        task_identity: Some(task),
    })
}
fn control(state: &RuntimeState) -> RuntimeKernelControl {
    let mut control = RuntimeKernelControl::new(state.error_state());
    control.bind_runtime_memory(state);
    control
}

/// Executes the original production common helper and its single Frame.
pub struct RuntimeScalarFrameForTest {
    instance: CompiledExpressionInstance,
    site: ProgramExpressionRootSite,
    control: RuntimeKernelControl,
    _state: Arc<RuntimeState>,
}
impl RuntimeScalarFrameForTest {
    pub fn try_new(
        program: Arc<LocalProgram>,
        site: ProgramExpressionRootSite,
        runtime: Arc<ExecutionRuntime>,
        task: TaskIdentity,
        memory: Option<QueryMemoryBinding>,
    ) -> Result<Self, String> {
        let state = task_state(runtime, task, memory)?;
        let control = control(&state);
        let instance = CompiledExpressionInstance::try_new(program, site, &control)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            instance,
            site,
            control,
            _state: state,
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "The original nominal runtime cause remains inline."
    )]
    pub fn evaluate(&mut self, input: &RecordBatch) -> ExecutionResult<ArrayRef> {
        evaluate_all(&mut self.instance, self.site, input, &self.control)
    }
}

/// Fixed receipt from the original complete-wrapper scope. It does not
/// describe the Frame's child gathers, NULL restoration or root metadata.
pub struct RuntimeScalarWrapperReceiptForTest {
    pub workset_bytes: Option<usize>,
    pub body: KernelMemoryJournal,
}

/// The original prepared ScalarV1 and concrete task host, without Frame's
/// strict-NULL pruning. This is a complete-wrapper contract witness, not a
/// production Frame bitmap witness or an alternate expression evaluator.
pub struct RuntimeScalarWrapperForTest {
    instance: ScalarEvaluationInstance,
    control: RuntimeKernelControl,
    _state: Arc<RuntimeState>,
}
impl RuntimeScalarWrapperForTest {
    pub fn try_new(
        program: &LocalProgram,
        site: ProgramExpressionRootSite,
        runtime: Arc<ExecutionRuntime>,
        task: TaskIdentity,
        memory: Option<QueryMemoryBinding>,
    ) -> Result<Self, String> {
        let state = task_state(runtime, task, memory)?;
        let control = control(&state);
        let mut selected = None;
        for call in program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .calls()
            .values()
        {
            if let ProgramStateTemplate::Scalar { scope, kernel } = call.state_template()
                && scope.root == site
                && selected.replace(Arc::clone(kernel)).is_some()
            {
                return Err("wrapper fixture requires one exact scalar occurrence".into());
            }
        }
        let prepared = selected.ok_or("wrapper fixture has no scalar occurrence")?;
        if prepared.invocation_resource_profile().is_none() {
            return Err("wrapper fixture scalar source is uncovered".into());
        }
        let instance =
            ScalarEvaluationInstance::instantiate(prepared).map_err(|error| error.to_string())?;
        Ok(Self {
            instance,
            control,
            _state: state,
        })
    }
    pub fn evaluate<'a>(
        &mut self,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
    ) -> (
        Result<SelectedValues<'a>, RuntimeScalarEvaluationFailure>,
        RuntimeScalarWrapperReceiptForTest,
    ) {
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = RuntimeScalarMemoryScope::new(&self.control, &mut journal).evaluate(
            &mut self.instance,
            selection,
            arguments,
            &self.control,
        );
        (
            result,
            RuntimeScalarWrapperReceiptForTest {
                workset_bytes: journal.workset_bytes,
                body: journal.body,
            },
        )
    }
}
