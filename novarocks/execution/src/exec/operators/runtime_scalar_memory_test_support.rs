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
use novarocks_expression::CompiledExpressionInstance;
use novarocks_expression::testkit::{call_invocation, call_scalar};
use crate::runtime::{
    execution_runtime::ExecutionRuntime,
    kernel_memory::KernelMemoryJournal,
    query_memory::QueryMemoryBinding,
    runtime_state::RuntimeState,
    scalar_memory::{
        RuntimeScalarMemoryJournal, RuntimeScalarMemoryRefusal, RuntimeScalarMemoryScope,
    },
    task::{
        ExecutionResult,
        runtime_state::{RuntimeStateInputs, build_runtime_state},
    },
};
use arrow::{array::ArrayRef, record_batch::RecordBatch};
use novarocks_execution_contract::TaskIdentity;
use novarocks_expression::ScalarCallFailure;
use novarocks_expression::evaluator_view::{UnitStateTemplate, state_template};
use novarocks_functions::{
    AggregateStateAllocator, EvaluatedArgument, InvocationScalarEvaluationInstance,
    ScalarEvaluationInstance, ScalarInvocationActivation, SelectedValues, Selection,
};
use novarocks_local_program::{LocalProgram, ProgramRootRef};
use std::sync::Arc;

fn task_state(
    runtime: Arc<ExecutionRuntime>,
    task: TaskIdentity,
    memory: Option<QueryMemoryBinding>,
) -> ExecutionResult<Arc<RuntimeState>> {
    build_runtime_state(RuntimeStateInputs {
        query_options: None,
        query_id: Some(task.query_execution_id().query_id()),
        task_key: None,
        backend_num: None,
        mem_tracker: None,
        runtime_filter_session: None,
        execution_runtime: Some(runtime),
        query_memory: memory,
        task_identity: Some(task),
        task_control: Some(crate::runtime::task_control::test_support::control_for(
            task,
        )),
        #[cfg(novarocks_dev_controls)]
        exchange_test_observer: None,
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
    site: ProgramRootRef,
    control: RuntimeKernelControl,
    _state: Arc<RuntimeState>,
}
impl RuntimeScalarFrameForTest {
    /// Borrow the tracker already created by this exact task state. The
    /// original Frame caches the same host Arc used for instance birth.
    pub fn try_new_with_allocation_host(
        program: Arc<LocalProgram>,
        site: ProgramRootRef,
        runtime: Arc<ExecutionRuntime>,
        task: TaskIdentity,
        memory: Option<QueryMemoryBinding>,
    ) -> ExecutionResult<Self> {
        let state = task_state(runtime, task, memory)?;
        let mut control = control(&state);
        control.bind_mem_tracker(
            state
                .mem_tracker()
                .ok_or_else(|| fixture_failure("task state has no actual allocation tracker"))?,
        );
        let unit = program.unit(site.unit).ok_or_else(|| {
            fixture_failure("frame fixture has no actual compiled expression unit")
        })?;
        let instance = CompiledExpressionInstance::try_new_with_allocator(
            unit.clone(),
            site.root,
            &control,
            control.allocator(),
        )?;
        Ok(Self {
            instance,
            site,
            control,
            _state: state,
        })
    }
    pub fn try_new(
        program: Arc<LocalProgram>,
        site: ProgramRootRef,
        runtime: Arc<ExecutionRuntime>,
        task: TaskIdentity,
        memory: Option<QueryMemoryBinding>,
    ) -> ExecutionResult<Self> {
        let state = task_state(runtime, task, memory)?;
        let control = control(&state);
        let unit = program.unit(site.unit).ok_or_else(|| {
            fixture_failure("frame fixture has no actual compiled expression unit")
        })?;
        let instance = CompiledExpressionInstance::try_new(unit.clone(), site.root, &control)?;
        Ok(Self {
            instance,
            site,
            control,
            _state: state,
        })
    }
    pub fn evaluate(&mut self, input: &RecordBatch) -> ExecutionResult<ArrayRef> {
        evaluate_all(&mut self.instance, self.site, input, &self.control)
    }
}

/// Fixed actual receipts of the two original synchronous operations. These
/// describe the complete wrapper, excluding Frame gathers and instance birth.
pub struct RuntimeInvocationWrapperReceiptForTest {
    pub preparation_workset_bytes: Option<usize>,
    pub preparation: Option<KernelMemoryJournal>,
    pub workset_bytes: Option<usize>,
    pub body: KernelMemoryJournal,
}

/// The installed whole-invocation occurrence and its original task-owned
/// allocation host. This borrows the production scope, not a test evaluator.
pub struct RuntimeInvocationWrapperForTest {
    instance: InvocationScalarEvaluationInstance,
    control: RuntimeKernelControl,
    host: Arc<dyn AggregateStateAllocator>,
    _state: Arc<RuntimeState>,
}
impl RuntimeInvocationWrapperForTest {
    pub fn try_new(
        program: &LocalProgram,
        site: ProgramRootRef,
        runtime: Arc<ExecutionRuntime>,
        task: TaskIdentity,
        memory: Option<QueryMemoryBinding>,
    ) -> ExecutionResult<Self> {
        let state = task_state(runtime, task, memory)?;
        let mut control = control(&state);
        control.bind_mem_tracker(
            state
                .mem_tracker()
                .ok_or_else(|| fixture_failure("task state has no actual allocation tracker"))?,
        );
        let unit = program
            .unit(site.unit)
            .ok_or_else(|| fixture_failure("wrapper fixture has no whole-invocation occurrence"))?;
        let mut selected = None;
        for &occurrence in unit.calls().keys() {
            if let Some(UnitStateTemplate::ScalarInvocation { scope, kernel }) =
                state_template(unit, occurrence)
                && scope.root == site.root
                && selected.replace(Arc::clone(kernel)).is_some()
            {
                return Err(fixture_failure(
                    "wrapper fixture requires one exact whole-invocation occurrence",
                ));
            }
        }
        let prepared = selected
            .ok_or_else(|| fixture_failure("wrapper fixture has no whole-invocation occurrence"))?;
        if prepared.invocation_resource_profile().is_none() {
            return Err(fixture_failure(
                "wrapper fixture whole-invocation source is uncovered",
            ));
        }
        let host = control
            .allocator()
            .ok_or_else(|| fixture_failure("task state has no actual allocation host"))?;
        let instance = InvocationScalarEvaluationInstance::instantiate_with_allocator(
            prepared,
            Some(host.clone()),
        )?;
        Ok(Self {
            instance,
            control,
            host,
            _state: state,
        })
    }
    pub fn evaluate<'a>(
        &mut self,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        activation: ScalarInvocationActivation,
    ) -> (
        Result<SelectedValues<'a>, ScalarCallFailure<RuntimeScalarMemoryRefusal>>,
        RuntimeInvocationWrapperReceiptForTest,
    ) {
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = call_invocation(
            &mut RuntimeScalarMemoryScope::new(&self.control, &mut journal),
            &mut self.instance,
            selection,
            arguments,
            activation,
            Some(&self.host),
            &self.control,
        );
        (
            result,
            RuntimeInvocationWrapperReceiptForTest {
                preparation_workset_bytes: journal.preparation_workset_bytes,
                preparation: journal.preparation,
                workset_bytes: journal.workset_bytes,
                body: journal.body,
            },
        )
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
        site: ProgramRootRef,
        runtime: Arc<ExecutionRuntime>,
        task: TaskIdentity,
        memory: Option<QueryMemoryBinding>,
    ) -> ExecutionResult<Self> {
        let state = task_state(runtime, task, memory)?;
        let control = control(&state);
        let unit = program
            .unit(site.unit)
            .ok_or_else(|| fixture_failure("wrapper fixture has no scalar occurrence"))?;
        let mut selected = None;
        for &occurrence in unit.calls().keys() {
            if let Some(UnitStateTemplate::Scalar { scope, kernel }) =
                state_template(unit, occurrence)
                && scope.root == site.root
                && selected.replace(Arc::clone(kernel)).is_some()
            {
                return Err(fixture_failure(
                    "wrapper fixture requires one exact scalar occurrence",
                ));
            }
        }
        let prepared =
            selected.ok_or_else(|| fixture_failure("wrapper fixture has no scalar occurrence"))?;
        if prepared.invocation_resource_profile().is_none() {
            return Err(fixture_failure(
                "wrapper fixture scalar source is uncovered",
            ));
        }
        let instance = ScalarEvaluationInstance::instantiate(prepared)?;
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
        Result<SelectedValues<'a>, ScalarCallFailure<RuntimeScalarMemoryRefusal>>,
        RuntimeScalarWrapperReceiptForTest,
    ) {
        let mut journal = RuntimeScalarMemoryJournal::default();
        let result = call_scalar(
            &mut RuntimeScalarMemoryScope::new(&self.control, &mut journal),
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

fn fixture_failure(detail: &str) -> crate::runtime::task::ExecutionFailure {
    novarocks_functions::KernelFailure::InvalidProgram(novarocks_functions::KernelDiagnostic::new(
        detail,
    ))
    .into()
}
