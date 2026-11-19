// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Runtime-owned loan at the original scalar invocation site. Pure source
//! facts never carry an account, controller or allocation permission.

use super::kernel_memory::{self, KernelMemoryAdmission, KernelMemoryJournal};
use super::query_memory::QueryMemoryBinding;
use crate::exec::operators::compiled_expression::RuntimeKernelControl;
use novarocks_expression::{
    ScalarCallFailure, ScalarCallHost, ScalarCallPhase, ScalarCallRequestFacts,
};
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl, ScalarResourceError};
use novarocks_memory::attribution::scope::{AmbientEntryObservation, AmbientExitObservation};
use novarocks_memory::{CapacityError, ShortageReceipt};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeScalarMemoryRefusal {
    MissingQueryMemory,
    SharedShortage(ShortageReceipt),
    Capacity(CapacityError),
}
impl std::fmt::Display for RuntimeScalarMemoryRefusal {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingQueryMemory => out.write_str("scalar runtime requires query memory"),
            Self::SharedShortage(receipt) => {
                write!(out, "scalar runtime shared shortage: {receipt:?}")
            }
            Self::Capacity(cause) => cause.fmt(out),
        }
    }
}
impl std::error::Error for RuntimeScalarMemoryRefusal {}

type ScalarMemoryCallFailure = ScalarCallFailure<RuntimeScalarMemoryRefusal>;

/// Fixed secondary disposition for every completed leaf, retained even when
/// the last-operation storage is reused. These facts never choose a cause.
#[derive(Debug, Default)]
pub(crate) struct RuntimeScalarMemoryObservations {
    pub entry_refused: bool,
    pub entry_tls_unavailable: bool,
    pub restore_tls_unavailable: bool,
    pub debt: bool,
    pub next_step_refused: bool,
    pub stop_failed: bool,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeScalarMemoryJournal {
    pub preparation_workset_bytes: Option<usize>,
    pub preparation: Option<KernelMemoryJournal>,
    pub workset_bytes: Option<usize>,
    pub pending: Option<novarocks_memory::CoverageReceipt>,
    pub shortage: Option<ShortageReceipt>,
    pub capacity: Option<CapacityError>,
    pub body: KernelMemoryJournal,
    pub observations: RuntimeScalarMemoryObservations,
}
impl RuntimeScalarMemoryJournal {
    fn dispose(&mut self) {
        match self.body.observation.entry() {
            AmbientEntryObservation::LaneEntryRefused => self.observations.entry_refused = true,
            AmbientEntryObservation::TlsUnavailable => {
                self.observations.entry_tls_unavailable = true
            }
            AmbientEntryObservation::NotAttempted | AmbientEntryObservation::Bound => {}
        }
        match self.body.observation.exit() {
            AmbientExitObservation::RestoreTlsUnavailable => {
                self.observations.restore_tls_unavailable = true
            }
            AmbientExitObservation::NotExited
            | AmbientExitObservation::EntryWasUnbound
            | AmbientExitObservation::Restored => {}
        }
        if let Some(receipt) = &self.body.settlement {
            self.observations.debt |= receipt.debt != 0;
            self.observations.next_step_refused |= receipt.next_step.is_err();
        }
        self.observations.stop_failed |= self.body.stopped.as_ref().is_some_and(Result::is_err);
    }
    fn begin(&mut self) {
        self.dispose();
        self.preparation_workset_bytes = None;
        self.preparation = None;
        self.workset_bytes = None;
        self.pending = None;
        self.shortage = None;
        self.capacity = None;
        self.body = KernelMemoryJournal::default();
    }
}

/// The actual operator owns both loans on its synchronous stack. Its concrete
/// original controller supplies the validated task binding and the SAME stop
/// Mutex; an arbitrary diagnostic-producing control cannot author this host.
pub(crate) struct RuntimeScalarMemoryScope<'a> {
    control: &'a RuntimeKernelControl,
    journal: &'a mut RuntimeScalarMemoryJournal,
}
impl<'a> RuntimeScalarMemoryScope<'a> {
    pub(crate) fn new(
        control: &'a RuntimeKernelControl,
        journal: &'a mut RuntimeScalarMemoryJournal,
    ) -> Self {
        Self { control, journal }
    }
    fn capacity(&mut self, cause: CapacityError) -> ScalarMemoryCallFailure {
        // CapacityError has only fixed nominal fields. No message is formatted
        // or allocated here, including while preserving an original body Err.
        self.journal.capacity = Some(cause.clone());
        ScalarCallFailure::Host(RuntimeScalarMemoryRefusal::Capacity(cause))
    }
    fn run<T, R>(
        &mut self,
        peak: usize,
        body: impl FnOnce() -> Result<T, ScalarMemoryCallFailure>,
        mut request: R,
    ) -> Result<T, ScalarMemoryCallFailure>
    where
        R: FnMut(&QueryMemoryBinding, usize) -> KernelMemoryAdmission,
    {
        self.journal.workset_bytes = Some(peak);
        loop {
            // Warm the original platform Mutex before the funded wrapper, and
            // stop before qualification. Pending never retains a live writer.
            self.control.checkpoint(0)?;
            let admission = match self.control.query_memory() {
                Some(binding) => request(binding, peak),
                None => KernelMemoryAdmission::MissingQueryMemory,
            };
            match admission {
                KernelMemoryAdmission::Granted(ready) => {
                    self.control.checkpoint(0)?;
                    let result = ready.run(&mut self.journal.body, body);
                    // Infallible disposition precedes every return/overwrite.
                    // Entry/restoration coverage faults retain Lane's policy.
                    self.journal.dispose();
                    return match result {
                        Err(cause) => Err(self.capacity(cause)),
                        Ok(Err(original)) => Err(original),
                        Ok(Ok(value)) => {
                            match self
                                .journal
                                .body
                                .settlement
                                .as_ref()
                                .and_then(|receipt| receipt.next_step.as_ref().err())
                                .cloned()
                            {
                                Some(cause) => Err(self.capacity(cause)),
                                None => Ok(value),
                            }
                        }
                    };
                }
                KernelMemoryAdmission::SettlementPending(receipt) => {
                    self.journal.pending = Some(receipt);
                    // This is an explicit local maintenance cadence. Stop and
                    // wait registration still belong to the original Condvar.
                    self.control.wait(Duration::from_millis(50))?;
                }
                KernelMemoryAdmission::SharedShortage(receipt) => {
                    self.journal.shortage = Some(receipt.clone());
                    return Err(ScalarCallFailure::Host(
                        RuntimeScalarMemoryRefusal::SharedShortage(receipt),
                    ));
                }
                KernelMemoryAdmission::Refused(cause) => return Err(self.capacity(cause)),
                KernelMemoryAdmission::MissingQueryMemory => {
                    return Err(ScalarCallFailure::Host(
                        RuntimeScalarMemoryRefusal::MissingQueryMemory,
                    ));
                }
            }
        }
    }
}
impl ScalarCallHost for RuntimeScalarMemoryScope<'_> {
    type Refusal = RuntimeScalarMemoryRefusal;

    fn begin_call(&mut self) {
        self.journal.begin();
    }

    fn run_admitted(
        &mut self,
        phase: ScalarCallPhase,
        facts: ScalarCallRequestFacts,
        body: &mut dyn FnMut() -> Result<(), ScalarMemoryCallFailure>,
    ) -> Result<(), ScalarMemoryCallFailure> {
        let workset = complete_workset(facts.bytes(), facts.requests())?;
        self.run(workset, body, |binding, peak| {
            kernel_memory::request_complete_operation(Some(binding), peak)
        })?;
        if phase == ScalarCallPhase::Preparation {
            // Preserve the original settled prefix before reusing the journal.
            let preparation = std::mem::take(&mut self.journal.body);
            self.journal.begin();
            self.journal.preparation = Some(preparation);
            self.journal.preparation_workset_bytes = Some(workset);
        }
        Ok(())
    }

    fn callback_request_facts(
        &mut self,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    ) -> Result<ScalarCallRequestFacts, ScalarMemoryCallFailure> {
        use novarocks_type_contract::{CompileControlError, ControlResourceError};
        let host = self
            .control
            .invocation_host(allocator)
            .map_err(ScalarCallFailure::Source)?;
        let facts = host.limit_failure_request_facts().map_err(|cause| {
            ScalarCallFailure::Source(match cause {
                ControlResourceError::SourceModel(detail) => {
                    ScalarResourceError::SourceModel(detail)
                }
                ControlResourceError::Control(CompileControlError::ResourceExhausted) => {
                    ScalarResourceError::Arithmetic
                }
                ControlResourceError::Control(_) => ScalarResourceError::Invariant(
                    "whole-invocation host numerical source returned a control refusal",
                ),
            })
        })?;
        Ok(ScalarCallRequestFacts::new(
            facts.requested_bytes,
            facts.requests,
        ))
    }
}

fn complete_workset(bytes: usize, requests: usize) -> Result<usize, ScalarMemoryCallFailure> {
    requests
        .checked_mul(novarocks_memory::attribution::ATTRIBUTION_TOKEN_BYTES)
        .and_then(|tokens| bytes.checked_add(tokens))
        .ok_or(ScalarCallFailure::Source(ScalarResourceError::Arithmetic))
}
impl Drop for RuntimeScalarMemoryScope<'_> {
    fn drop(&mut self) {
        // Original unwind guards complete the body journal before this stack
        // loan is dropped. Preserve those secondary facts without intercepting
        // or replacing the panic owned by the original outer runtime.
        self.journal.dispose();
    }
}

#[cfg(test)]
#[path = "scalar_memory_tests.rs"]
mod contract_tests;

#[cfg(test)]
mod tests {
    use super::*;

    use novarocks_expression::{
        ScalarCallFailure, ScalarCallHost, ScalarCallPhase, ScalarCallRequestFacts,
    };

    // Compile the actual dyn/lifetime contract before any production grant.
    fn borrow_scope(
        scope: &mut impl ScalarCallHost<Refusal = RuntimeScalarMemoryRefusal>,
    ) -> &mut dyn ScalarCallHost<Refusal = RuntimeScalarMemoryRefusal> {
        scope
    }

    struct Refuse;
    impl ScalarCallHost for Refuse {
        type Refusal = RuntimeScalarMemoryRefusal;

        fn begin_call(&mut self) {}

        fn run_admitted(
            &mut self,
            _: ScalarCallPhase,
            _: ScalarCallRequestFacts,
            _: &mut dyn FnMut() -> Result<(), ScalarCallFailure<Self::Refusal>>,
        ) -> Result<(), ScalarCallFailure<Self::Refusal>> {
            Err(ScalarCallFailure::Host(
                RuntimeScalarMemoryRefusal::MissingQueryMemory,
            ))
        }

        fn callback_request_facts(
            &mut self,
            _: Option<&Arc<dyn AggregateStateAllocator>>,
        ) -> Result<ScalarCallRequestFacts, ScalarCallFailure<Self::Refusal>> {
            Err(ScalarCallFailure::Host(
                RuntimeScalarMemoryRefusal::MissingQueryMemory,
            ))
        }
    }

    #[test]
    fn runtime_scalar_memory_scope_is_a_borrowed_dyn_compatible_port() {
        let mut refusal = Refuse;
        let _: &mut dyn ScalarCallHost<Refusal = RuntimeScalarMemoryRefusal> =
            borrow_scope(&mut refusal);

        let control = RuntimeKernelControl::new(Arc::default());
        let mut journal = RuntimeScalarMemoryJournal::default();
        let mut scope = RuntimeScalarMemoryScope::new(&control, &mut journal);
        let _: &mut dyn ScalarCallHost<Refusal = RuntimeScalarMemoryRefusal> =
            borrow_scope(&mut scope);
    }
}

/// Original owner seams for isolated Server GLOBAL integration tests.
#[cfg(novarocks_dev_controls)]
pub mod test_support {
    pub use crate::exec::operators::compiled_expression::runtime_scalar_memory_test_support::{
        RuntimeInvocationWrapperForTest, RuntimeInvocationWrapperReceiptForTest,
        RuntimeScalarFrameForTest, RuntimeScalarWrapperForTest, RuntimeScalarWrapperReceiptForTest,
    };
}

impl novarocks_failure_contract::FailureProjection for RuntimeScalarMemoryRefusal {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        match self {
            Self::MissingQueryMemory => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::ScalarMissingQueryMemory.reason(),
            ),
            Self::SharedShortage(cause) => {
                novarocks_memory_failure::ShortageFailureProjection::new(cause).failure_fact()
            }
            Self::Capacity(cause) => {
                novarocks_memory_failure::CapacityFailureProjection::new(cause).failure_fact()
            }
        }
    }
}
