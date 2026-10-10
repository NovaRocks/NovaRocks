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

//! Runtime-owned loan at the original ScalarV1 invocation site. Pure source
//! facts never carry an account, controller or allocation permission.

use super::kernel_memory::{self, KernelMemoryAdmission, KernelMemoryJournal};
use super::query_memory::QueryMemoryBinding;
use crate::exec::operators::compiled_expression::RuntimeKernelControl;
use novarocks_functions::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, ScalarEvaluationInstance,
    ScalarInvocationData, ScalarInvocationFailure, ScalarResourceError, SelectedValues, Selection,
};
use novarocks_memory::attribution::scope::{AmbientEntryObservation, AmbientExitObservation};
use novarocks_memory::{CapacityError, ShortageReceipt};
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

/// The original Frame transports each first cause nominally. Source/host
/// refusal must bypass the old kernel footer and fail the original root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeScalarEvaluationFailure {
    Kernel(KernelFailure),
    Data(ScalarInvocationData),
    Source(ScalarResourceError),
    Host(RuntimeScalarMemoryRefusal),
}
impl From<KernelFailure> for RuntimeScalarEvaluationFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl From<ScalarInvocationFailure> for RuntimeScalarEvaluationFailure {
    fn from(cause: ScalarInvocationFailure) -> Self {
        match cause {
            ScalarInvocationFailure::Kernel(cause) => Self::Kernel(cause),
            ScalarInvocationFailure::Data(cause) => Self::Data(cause),
        }
    }
}
impl std::fmt::Display for RuntimeScalarEvaluationFailure {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kernel(cause) => cause.fmt(out),
            Self::Data(cause) => cause.fmt(out),
            Self::Source(cause) => write!(out, "scalar runtime request: {cause}"),
            Self::Host(cause) => cause.fmt(out),
        }
    }
}
impl std::error::Error for RuntimeScalarEvaluationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Kernel(cause) => Some(cause),
            Self::Data(cause) => Some(cause),
            Self::Source(cause) => Some(cause),
            Self::Host(cause) => Some(cause),
        }
    }
}

/// Explicit synchronous loan from the real runtime caller to its one Frame.
/// Lifetime-only generics keep this trusted Execution port dyn-compatible.
/// The concrete host owns the static FnOnce and any account qualification.
/// Its original observed controller must come from that same runtime caller;
/// this interface does not prove arbitrary third-party trait implementations.
pub trait RuntimeScalarOperationScope {
    fn evaluate<'a>(
        &mut self,
        instance: &mut ScalarEvaluationInstance,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, RuntimeScalarEvaluationFailure>;
}

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
    fn capacity(&mut self, cause: CapacityError) -> RuntimeScalarEvaluationFailure {
        // CapacityError has only fixed nominal fields. No message is formatted
        // or allocated here, including while preserving an original body Err.
        self.journal.capacity = Some(cause.clone());
        RuntimeScalarEvaluationFailure::Host(RuntimeScalarMemoryRefusal::Capacity(cause))
    }
    fn run<T, R>(
        &mut self,
        peak: usize,
        body: impl FnOnce() -> Result<T, RuntimeScalarEvaluationFailure>,
        mut request: R,
    ) -> Result<T, RuntimeScalarEvaluationFailure>
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
                    return Err(RuntimeScalarEvaluationFailure::Host(
                        RuntimeScalarMemoryRefusal::SharedShortage(receipt),
                    ));
                }
                KernelMemoryAdmission::Refused(cause) => return Err(self.capacity(cause)),
                KernelMemoryAdmission::MissingQueryMemory => {
                    return Err(RuntimeScalarEvaluationFailure::Host(
                        RuntimeScalarMemoryRefusal::MissingQueryMemory,
                    ));
                }
            }
        }
    }
}
impl RuntimeScalarOperationScope for RuntimeScalarMemoryScope<'_> {
    fn evaluate<'a>(
        &mut self,
        instance: &mut ScalarEvaluationInstance,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, RuntimeScalarEvaluationFailure> {
        self.journal.begin();
        let Some(profile) = instance.invocation_resource_profile() else {
            // Explicitly uncovered sources keep their original call. A positive
            // source's model/host error never enters this branch.
            return instance
                .evaluate(selection, arguments, control)
                .map_err(Into::into);
        };
        let facts = profile
            .requests_for(selection, arguments)
            .map_err(RuntimeScalarEvaluationFailure::Source)?;
        let peak = facts
            .requests()
            .checked_mul(novarocks_memory::attribution::ATTRIBUTION_TOKEN_BYTES)
            .and_then(|tokens| facts.bytes().checked_add(tokens))
            .ok_or(RuntimeScalarEvaluationFailure::Source(
                ScalarResourceError::Arithmetic,
            ))?;
        self.run(
            peak,
            || {
                instance
                    .evaluate(selection, arguments, control)
                    .map_err(Into::into)
            },
            |binding, peak| kernel_memory::request_complete_operation(Some(binding), peak),
        )
    }
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

    // Compile the actual dyn/lifetime contract before any production grant.
    fn borrow_scope<'loan>(
        scope: &'loan mut impl RuntimeScalarOperationScope,
    ) -> &'loan mut dyn RuntimeScalarOperationScope {
        scope
    }
    struct Refuse;
    impl RuntimeScalarOperationScope for Refuse {
        fn evaluate<'a>(
            &mut self,
            _: &mut ScalarEvaluationInstance,
            _: Selection<'a>,
            _: &'a [EvaluatedArgument<'a>],
            _: &dyn KernelEvaluationControl,
        ) -> Result<SelectedValues<'a>, RuntimeScalarEvaluationFailure> {
            Err(RuntimeScalarEvaluationFailure::Host(
                RuntimeScalarMemoryRefusal::MissingQueryMemory,
            ))
        }
    }
    #[test]
    fn runtime_scalar_memory_scope_is_a_borrowed_dyn_compatible_port() {
        let mut scope = Refuse;
        let _: &mut dyn RuntimeScalarOperationScope = borrow_scope(&mut scope);
    }
}

/// Original owner seams for isolated Server GLOBAL integration tests.
#[cfg(feature = "test-support")]
pub mod test_support {
    pub use crate::exec::operators::compiled_expression::runtime_scalar_memory_test_support::{
        RuntimeScalarFrameForTest, RuntimeScalarWrapperForTest, RuntimeScalarWrapperReceiptForTest,
    };
}
