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

use std::sync::Arc;

use crate::exec::pipeline::operator::input_construction::ReceiverInputConstructionStopOwner;

#[cfg(test)]
use crate::runtime::exchange::ExchangeColumnBinding;
use crate::runtime::task::error::{
    LocalTaskLaunchError, LocalTaskLaunchErrorKind, LocalTaskLaunchStage,
};
use crate::runtime::task::io::{
    ExchangeReceiverKey, ExchangeReceiverPort, ExchangeReceiverRegistration, TaskCommitPort,
    TaskCommitRegistration,
};
use crate::runtime::task::io::{ResultAbort, ResultWriteSpec, TaskResultSession, TaskResultWriter};
use novarocks_types::UniqueId;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ResourceKind {
    SinkCommit,
    Result,
    Exchange,
}

impl ResourceKind {
    fn cleanup_failure_detail(self) -> &'static str {
        match self {
            Self::SinkCommit => "injected sink commit cleanup failure",
            Self::Result => "injected result registration cleanup failure",
            Self::Exchange => "injected exchange registration cleanup failure",
        }
    }
}

#[derive(Debug)]
struct InjectedCleanupFailure(ResourceKind);
impl std::fmt::Display for InjectedCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.cleanup_failure_detail())
    }
}
impl std::error::Error for InjectedCleanupFailure {}
impl novarocks_failure_contract::FailureProjection for InjectedCleanupFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            super::ExecutionReason::InjectedCleanupFailure.reason(),
        )
    }
}

#[derive(Clone, Default)]
pub(crate) struct ResourceCleanupFaults {
    failed: std::collections::BTreeSet<ResourceKind>,
}

#[allow(
    dead_code,
    reason = "Cleanup fault injection is retained for fragment lifecycle regression coverage."
)]
impl ResourceCleanupFaults {
    #[cfg(test)]
    pub(crate) fn with_failure(mut self, resource: ResourceKind) -> Self {
        self.failed.insert(resource);
        self
    }

    fn should_fail(&self, resource: ResourceKind) -> bool {
        self.failed.contains(&resource)
    }
}

pub(crate) struct SinkCommitRegistration {
    registration: Option<Box<dyn TaskCommitRegistration>>,
    cleanup_should_fail: bool,
    settled: bool,
}

impl SinkCommitRegistration {
    pub(crate) fn acquire(
        port: &Arc<dyn TaskCommitPort>,
        task_key: UniqueId,
        cleanup_should_fail: bool,
    ) -> Result<Self, LocalTaskLaunchError> {
        let lease = port.acquire(task_key).map_err(registration_error)?;
        Ok(Self {
            registration: Some(lease),
            cleanup_should_fail,
            settled: false,
        })
    }

    fn rollback(&mut self) -> super::ExecutionResult<()> {
        if !std::mem::replace(&mut self.settled, true)
            && let Some(lease) = self.registration.as_mut()
        {
            lease.rollback()?;
        }
        if self.cleanup_should_fail {
            self.cleanup_should_fail = false;
            return Err(super::ExecutionFailure::from_domain(
                InjectedCleanupFailure(ResourceKind::SinkCommit),
            ));
        }
        Ok(())
    }
}

impl Drop for SinkCommitRegistration {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

pub(crate) struct ResultRegistration {
    session: Arc<dyn TaskResultSession>,
    active: bool,
    cleanup_should_fail: bool,
}

impl ResultRegistration {
    pub(crate) fn acquire(
        writer: &Arc<dyn TaskResultWriter>,
        spec: ResultWriteSpec,
        cleanup_should_fail: bool,
    ) -> Result<Self, LocalTaskLaunchError> {
        let session = writer.open(spec).map_err(registration_error)?;
        Ok(Self {
            session,
            active: true,
            cleanup_should_fail,
        })
    }

    fn rollback(&mut self) -> super::ExecutionResult<()> {
        if self.active {
            self.active = false;
            self.session.abort(ResultAbort::PrepareRollback);
        }
        if self.cleanup_should_fail {
            self.cleanup_should_fail = false;
            return Err(super::ExecutionFailure::from_domain(
                InjectedCleanupFailure(ResourceKind::Result),
            ));
        }
        Ok(())
    }

    fn finish_success(&mut self) {
        self.active = false;
        self.cleanup_should_fail = false;
    }

    fn finish_failure(&mut self, error: String) {
        if self.active {
            self.active = false;
            self.session.abort(ResultAbort::Failed(error));
        }
        self.cleanup_should_fail = false;
    }

    fn finish_cancelled(&mut self, reason: String) {
        if self.active {
            self.active = false;
            self.session.abort(ResultAbort::Cancelled(reason));
        }
        self.cleanup_should_fail = false;
    }

    /// Interrupt a result write as soon as cancellation wins, while retaining
    /// this registration until the fragment has actually stopped.
    fn abort_for_cancellation(&mut self, reason: String) {
        if self.active {
            self.active = false;
            self.session.abort(ResultAbort::Cancelled(reason));
        }
    }
}

impl Drop for ResultRegistration {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

struct RootResultRegistration {
    session: Arc<dyn crate::runtime::task::io::RootResultSession>,
    active: bool,
    cleanup_should_fail: bool,
}
impl RootResultRegistration {
    fn abort(&mut self, reason: ResultAbort) {
        if self.active {
            self.active = false;
            self.session.abort(reason);
        }
    }
    fn rollback(&mut self) -> super::ExecutionResult<()> {
        self.abort(ResultAbort::PrepareRollback);
        if std::mem::take(&mut self.cleanup_should_fail) {
            return Err(super::ExecutionFailure::from_domain(
                InjectedCleanupFailure(ResourceKind::Result),
            ));
        }
        Ok(())
    }
}
impl Drop for RootResultRegistration {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

pub(crate) struct ExchangeRegistration {
    port: Arc<dyn ExchangeReceiverPort>,
    keys: Vec<ExchangeReceiverKey>,
    close_observers: Vec<(
        ExchangeReceiverKey,
        crate::runtime::exchange::ExchangeReceiverCloseObserver,
    )>,
    _close_observer_storage: novarocks_type_contract::ValueTypeStorageLifetime,
    active: bool,
    cleanup_should_fail: bool,
    construction_stop: Option<ReceiverInputConstructionStopOwner>,
}

impl ExchangeRegistration {
    /// Register the program's receivers, already projected for this instance,
    /// under their rollback and terminal-cleanup owner.
    pub(crate) fn acquire_registrations(
        port: Arc<dyn ExchangeReceiverPort>,
        registrations: Vec<ExchangeReceiverRegistration>,
        cleanup_should_fail: bool,
        construction_stop: Option<ReceiverInputConstructionStopOwner>,
    ) -> Result<Option<Self>, LocalTaskLaunchError> {
        if registrations.is_empty() {
            return Ok(None);
        }
        // One fixed observer slot per original receiver. The same admitted
        // input-construction owner pays its actual metadata, not another Work
        // or a copied stream-facts table. Its lifetime outlasts all these pins.
        let first = registrations
            .first()
            .expect("nonempty original receiver set");
        let storage = first
            .creation_factory
            .begin_typed_construction(&first.expected_chunk_schema)
            .map_err(registration_error)?;
        let layout = std::alloc::Layout::array::<(
            ExchangeReceiverKey,
            crate::runtime::exchange::ExchangeReceiverCloseObserver,
        )>(registrations.len())
        .map_err(|_| {
            registration_error(crate::exec::pipeline::operator::ConstructionFailure::from(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            ))
        })?;
        storage.request(layout).map_err(registration_error)?;
        let mut close_observers = Vec::new();
        close_observers
            .try_reserve_exact(registrations.len())
            .map_err(|source| {
                registration_error(super::ExecutionFailure::from_domain(
                    novarocks_functions::allocation_failure::AllocationRefusal::new(
                        "reserve original exchange close observers",
                        source,
                    ),
                ))
            })?;
        if close_observers.capacity() > registrations.len() {
            return Err(registration_error(
                crate::exec::pipeline::operator::ConstructionFailure::InvalidSource(
                    "exchange observer metadata exceeds its admitted layout".into(),
                ),
            ));
        }
        let mut registration = Self {
            port,
            keys: Vec::with_capacity(registrations.len()),
            close_observers,
            _close_observer_storage: storage.storage_lifetime(),
            active: true,
            cleanup_should_fail,
            construction_stop,
        };
        for entry in registrations {
            let key = entry.key;
            let observer = match registration.port.register_with_close_observer(entry) {
                Ok(observer) => observer,
                Err(error) => {
                    let diagnostics = registration.rollback().err().into_iter().collect();
                    return Err(registration_error(error).with_cleanup_diagnostics(diagnostics));
                }
            };
            registration.keys.push(key);
            registration.close_observers.push((key, observer));
        }
        Ok(Some(registration))
    }

    fn rollback(&mut self) -> super::ExecutionResult<()> {
        self.stop_construction(novarocks_functions::KernelFailure::Cancelled.into());
        if self.active {
            for key in self.keys.drain(..).rev() {
                self.port.cancel(key);
            }
            self.active = false;
        }
        if self.cleanup_should_fail {
            self.cleanup_should_fail = false;
            return Err(super::ExecutionFailure::from_domain(
                InjectedCleanupFailure(ResourceKind::Exchange),
            ));
        }
        Ok(())
    }

    fn finish_success(&mut self) {
        self.stop_construction(novarocks_functions::KernelFailure::Cancelled.into());
        if self.active {
            for key in self.keys.drain(..).rev() {
                self.port.remove(key);
            }
            self.active = false;
        }
        self.cleanup_should_fail = false;
    }

    fn finish_cancelled(&mut self) {
        self.stop_construction(novarocks_functions::KernelFailure::Cancelled.into());
        if self.active {
            for key in self.keys.drain(..).rev() {
                self.port.cancel(key);
            }
            self.active = false;
        }
        self.cleanup_should_fail = false;
    }

    fn stop_construction(&self, cause: super::ExecutionFailure) {
        if let Some(stop) = &self.construction_stop {
            stop.stop(cause);
        }
    }
}

impl Drop for ExchangeRegistration {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

/// The original receiver registration, moved to its application's retirement gate.
///
/// Dropping an uncompleted or rolled-back task cancels its original receivers.
/// A raw successful terminal fact selects the original success removal instead.
/// This capability is deliberately not cloneable.
pub struct ExchangeReceiverRetirement {
    registration: ExchangeRegistration,
    disposition: ExchangeRetirementDisposition,
}

#[derive(Clone, Copy)]
enum ExchangeRetirementDisposition {
    Cancel,
    Remove,
}

impl ExchangeReceiverRetirement {
    /// Read this exact original registration, including after kernel cancel
    /// removed its route. These facts never authorize normal close by themselves.
    pub fn stream_close_facts(
        &self,
        key: ExchangeReceiverKey,
        expected: &[novarocks_execution_contract::ExchangeStreamIdentity],
    ) -> Result<
        Vec<novarocks_execution_contract::ExchangeStreamClose>,
        crate::runtime::exchange::ExchangeFailure,
    > {
        let observer = self
            .registration
            .close_observers
            .iter()
            .find(|(actual, _)| *actual == key)
            .map(|(_, observer)| observer)
            .ok_or_else(crate::runtime::exchange::ExchangeFailure::receiver_unavailable)?;
        observer.stream_close_facts(expected)
    }
    /// Record the kernel's fact before publishing task convergence. This does
    /// not remove or cancel a receiver; the application's gate still owns that.
    pub fn record_terminal(&mut self, fact: &super::LocalTaskTerminalFact) {
        self.disposition = match fact.outcome() {
            super::LocalTaskOutcome::Succeeded => ExchangeRetirementDisposition::Remove,
            super::LocalTaskOutcome::Failed(error) => {
                if self.registration.construction_stop.is_some() {
                    self.stop_construction(error.cause().clone());
                }
                ExchangeRetirementDisposition::Cancel
            }
            super::LocalTaskOutcome::Cancelled { .. } => ExchangeRetirementDisposition::Cancel,
        };
    }

    /// Stop construction on a real failure or abort without retiring ingress.
    pub fn stop_construction(&self, cause: super::ExecutionFailure) {
        self.registration.stop_construction(cause);
    }

    /// Move the unique live source owner after the application has fenced
    /// actual ingress borrowers. This is metadata transfer only: it neither
    /// closes the signal nor destroys a source or registration. The original
    /// supervised host must call close_after_ingress and dispose the returned
    /// source outside that fence and all other dependency locks. Keys and the
    /// original success/cancellation disposition remain with this registration.
    pub fn take_construction_stop(&mut self) -> Option<ReceiverInputConstructionStopOwner> {
        self.registration.construction_stop.take()
    }

    /// Retire synchronously while the caller holds its original inbound gate.
    pub fn retire(self) {
        drop(self);
    }
}

impl Drop for ExchangeReceiverRetirement {
    fn drop(&mut self) {
        match self.disposition {
            ExchangeRetirementDisposition::Remove => self.registration.finish_success(),
            ExchangeRetirementDisposition::Cancel => self.registration.finish_cancelled(),
        }
    }
}

pub(crate) struct LocalTaskResources {
    commit_port: Option<Arc<dyn TaskCommitPort>>,
    exchange_receiver_port: Option<Arc<dyn ExchangeReceiverPort>>,
    sink_commit: Option<SinkCommitRegistration>,
    result: Option<ResultRegistration>,
    root_result: Option<RootResultRegistration>,
    exchange: Option<ExchangeRegistration>,
    cleanup_faults: ResourceCleanupFaults,
    query_task: Option<crate::runtime::query_context::QueryTaskGuard>,
}

impl LocalTaskResources {
    pub(crate) fn new(
        commit_port: Arc<dyn TaskCommitPort>,
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
        cleanup_faults: ResourceCleanupFaults,
    ) -> Self {
        Self {
            commit_port: Some(commit_port),
            exchange_receiver_port: Some(exchange_receiver_port),
            sink_commit: None,
            result: None,
            root_result: None,
            exchange: None,
            cleanup_faults,
            query_task: None,
        }
    }

    pub(crate) fn hold_query_task(
        &mut self,
        guard: Option<crate::runtime::query_context::QueryTaskGuard>,
    ) {
        assert!(
            self.query_task.is_none(),
            "one original query Task resource guard"
        );
        self.query_task = guard;
    }

    pub(crate) fn acquire_sink_commit(
        &mut self,
        task_key: UniqueId,
    ) -> Result<(), LocalTaskLaunchError> {
        self.sink_commit = Some(SinkCommitRegistration::acquire(
            self.commit_port.as_ref().expect("preparing commit factory"),
            task_key,
            self.cleanup_faults.should_fail(ResourceKind::SinkCommit),
        )?);
        Ok(())
    }

    pub(crate) fn acquire_result_for_static(
        &mut self,
        sink: Option<&novarocks_local_program::StaticSinkProgram>,
        writer: &Arc<dyn TaskResultWriter>,
        spec: ResultWriteSpec,
    ) -> Result<(), LocalTaskLaunchError> {
        if !matches!(
            sink,
            Some(novarocks_local_program::StaticSinkProgram::Result)
        ) {
            return Ok(());
        }
        self.result = Some(ResultRegistration::acquire(
            writer,
            spec,
            self.cleanup_faults.should_fail(ResourceKind::Result),
        )?);
        Ok(())
    }

    pub(crate) fn acquire_root_result_for_static(
        &mut self,
        sink: Option<&novarocks_local_program::StaticSinkProgram>,
        session: Option<Arc<dyn crate::runtime::task::io::RootResultSession>>,
        identity: Option<novarocks_execution_contract::TaskIdentity>,
    ) -> Result<(), LocalTaskLaunchError> {
        let Some(novarocks_local_program::StaticSinkProgram::RootResult(contract)) = sink else {
            if let Some(session) = session {
                session.abort(ResultAbort::PrepareRollback);
                return Err(root_registration_contract(
                    "bounded root session supplied for a different sink",
                ));
            }
            return Ok(());
        };
        let session = session.ok_or_else(|| {
            root_registration_contract("frozen bounded root requires its host-owned session")
        })?;
        if Some(session.spec().task) != identity
            || session.spec().contract.as_ref() != contract.as_ref()
        {
            session.abort(ResultAbort::PrepareRollback);
            return Err(root_registration_contract(
                "bounded root session disagrees with exact task or frozen contract",
            ));
        }
        self.root_result = Some(RootResultRegistration {
            session,
            active: true,
            cleanup_should_fail: self.cleanup_faults.should_fail(ResourceKind::Result),
        });
        Ok(())
    }
    pub(crate) fn root_result_session(
        &self,
    ) -> Option<Arc<dyn crate::runtime::task::io::RootResultSession>> {
        self.root_result
            .as_ref()
            .map(|root| Arc::clone(&root.session))
    }

    pub(crate) fn result_session(&self) -> Option<Arc<dyn TaskResultSession>> {
        self.result
            .as_ref()
            .map(|registration| Arc::clone(&registration.session))
    }

    pub(crate) fn acquire_compiled_exchange(
        &mut self,
        registrations: Vec<ExchangeReceiverRegistration>,
        construction_stop: Option<ReceiverInputConstructionStopOwner>,
    ) -> Result<(), LocalTaskLaunchError> {
        self.exchange = ExchangeRegistration::acquire_registrations(
            Arc::clone(
                self.exchange_receiver_port
                    .as_ref()
                    .expect("preparing receiver factory"),
            ),
            registrations,
            self.cleanup_faults.should_fail(ResourceKind::Exchange),
            construction_stop,
        )?;
        Ok(())
    }

    pub(crate) fn take_exchange_retirement(&mut self) -> Option<ExchangeReceiverRetirement> {
        self.exchange
            .take()
            .map(|registration| ExchangeReceiverRetirement {
                registration,
                disposition: ExchangeRetirementDisposition::Cancel,
            })
    }

    pub(crate) fn rollback(&mut self) -> Vec<super::ExecutionFailure> {
        let mut diagnostics = Vec::new();
        if let Some(mut root) = self.root_result.take()
            && let Err(error) = root.rollback()
        {
            diagnostics.push(error);
        }
        if let Some(mut exchange) = self.exchange.take()
            && let Err(error) = exchange.rollback()
        {
            diagnostics.push(error);
        }
        if let Some(mut result) = self.result.take()
            && let Err(error) = result.rollback()
        {
            diagnostics.push(error);
        }
        if let Some(mut sink_commit) = self.sink_commit.take()
            && let Err(error) = sink_commit.rollback()
        {
            diagnostics.push(error);
        }
        diagnostics
    }
}

impl LocalTaskResources {
    pub(crate) fn into_runtime(mut self) -> RuntimeTaskResources {
        let exchange = self.exchange.take().map(|mut registration| {
            // Each original key has one independent hook and one original port
            // reference. A bad key cannot discard unvisited sibling keys.
            registration.active = false;
            registration.cleanup_should_fail = false;
            let keys = std::mem::take(&mut registration.keys)
                .into_iter()
                .rev()
                .map(|key| ResourceCell::new((Arc::clone(&registration.port), key)))
                .collect();
            RuntimeExchange {
                stop: registration.construction_stop.take().map(ResourceCell::new),
                keys,
                registration: ResourceCell::new(registration),
            }
        });
        RuntimeTaskResources {
            root: self.root_result.take().map(ResourceCell::new),
            result: self.result.take().map(ResourceCell::new),
            sink_commit: self.sink_commit.take().map(ResourceCell::new),
            exchange,
            commit_port: self.commit_port.take().map(ResourceCell::new),
            exchange_port: self.exchange_receiver_port.take().map(ResourceCell::new),
            query_task: self.query_task.take().map(ResourceCell::new),
        }
    }
}

use crate::exec::pipeline::global_driver_executor::completion_custody::{
    CompletionFailure, CompletionKind, Payload, Phase,
};
use crate::runtime::runtime_state::RuntimeErrorState;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};

// A failed diagnostic notification is metadata. Its fixed opaque custody
// outlives the original cell independently and owns neither a registration,
// RuntimeErrorState nor an execution control. It cannot revoke actual exit.
struct NotificationCustody {
    payloads: std::sync::Mutex<[Option<Payload>; 2]>,
    quarantine: std::sync::Mutex<Option<Arc<Self>>>,
}
impl NotificationCustody {
    fn capture(owner: &mut Option<Arc<Self>>, ordinal: usize, payload: Payload) {
        let custody = owner.get_or_insert_with(|| {
            Arc::new(Self {
                payloads: std::sync::Mutex::new([None, None]),
                quarantine: std::sync::Mutex::new(None),
            })
        });
        custody.payloads.lock().unwrap_or_else(|e| e.into_inner())[ordinal] = Some(payload);
        let mut pin = custody.quarantine.lock().unwrap_or_else(|e| e.into_inner());
        if pin.is_none() {
            *pin = Some(custody.clone());
        }
    }

    fn both(
        owner: &mut Option<Arc<Self>>,
        control: &RuntimeErrorState,
        cause: &super::ExecutionFailure,
    ) {
        for (ordinal, report) in [
            RuntimeErrorState::report_cleanup as fn(&RuntimeErrorState, &super::ExecutionFailure),
            RuntimeErrorState::report_unresolved_exit,
        ]
        .into_iter()
        .enumerate()
        {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| report(control, cause))) {
                Self::capture(owner, ordinal, ManuallyDrop::new(payload));
            }
        }
    }
}

#[derive(Default)]
struct ResourcePhase {
    started: bool,
    completed: bool,
    payload: Option<Payload>,
    notification: Option<Arc<NotificationCustody>>,
}
impl ResourcePhase {
    fn run<T>(
        &mut self,
        control: &RuntimeErrorState,
        phase: Phase,
        f: impl FnOnce() -> T,
    ) -> Option<T> {
        if self.started {
            return None;
        }
        self.started = true;
        match catch_unwind(AssertUnwindSafe(f)) {
            Ok(value) => {
                self.completed = true;
                Some(value)
            }
            Err(payload) => {
                self.payload = Some(ManuallyDrop::new(payload));
                let cause = super::ExecutionFailure::from_domain(CompletionFailure {
                    kind: CompletionKind::PipelineFinisher,
                    phase,
                });
                NotificationCustody::both(&mut self.notification, control, &cause);
                None
            }
        }
    }
}

#[derive(Default)]
struct CleanupNotification {
    started: bool,
    custody: Option<Arc<NotificationCustody>>,
}
impl CleanupNotification {
    fn report(&mut self, control: &RuntimeErrorState, original: &super::ExecutionFailure) {
        if self.started {
            return;
        }
        self.started = true;
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| control.report_cleanup(original))) {
            NotificationCustody::capture(&mut self.custody, 0, ManuallyDrop::new(payload));
            // The returned cleanup error keeps its own exact original fact.
            // Only this failed metadata call supplies the independent health fact.
            let notification = super::ExecutionFailure::from_domain(CompletionFailure {
                kind: CompletionKind::PipelineFinisher,
                phase: Phase::CleanupNotification,
            });
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                control.report_unresolved_exit(&notification)
            })) {
                NotificationCustody::capture(&mut self.custody, 1, ManuallyDrop::new(payload));
            }
        }
    }
}

struct ResourceCell<T> {
    holder: Option<ManuallyDrop<T>>,
    hook: ResourcePhase,
    dispose: ResourcePhase,
    returned_error: Option<ManuallyDrop<super::ExecutionFailure>>,
    error_report: CleanupNotification,
    error_dispose: ResourcePhase,
}
impl<T> ResourceCell<T> {
    fn new(holder: T) -> Self {
        Self {
            holder: Some(ManuallyDrop::new(holder)),
            hook: ResourcePhase::default(),
            dispose: ResourcePhase::default(),
            returned_error: None,
            error_report: CleanupNotification::default(),
            error_dispose: ResourcePhase::default(),
        }
    }
    fn hook(
        &mut self,
        control: &RuntimeErrorState,
        f: impl FnOnce(&mut T) -> super::ExecutionResult<()>,
    ) {
        let Some(holder) = self.holder.as_mut() else {
            return;
        };
        if let Some(Err(error)) = self.hook.run(control, Phase::ResourceHook, || f(holder)) {
            self.returned_error = Some(ManuallyDrop::new(error));
        }
        if let Some(error) = self.returned_error.as_ref() {
            self.error_report.report(control, error);
        }
    }
    fn retire(&mut self, control: &RuntimeErrorState) {
        if !self.hook.completed {
            return;
        }
        if !self.dispose.started {
            let holder = self.holder.take();
            self.dispose.run(control, Phase::ResourceHolderDrop, || {
                if let Some(holder) = holder {
                    drop(ManuallyDrop::into_inner(holder));
                }
            });
        }
        if self.returned_error.is_some() && !self.error_dispose.started {
            let error = self.returned_error.take().expect("retained cleanup error");
            self.error_dispose
                .run(control, Phase::ResourceHolderDrop, || {
                    drop(ManuallyDrop::into_inner(error))
                });
        }
    }
    fn exited(&self) -> bool {
        self.hook.completed
            && self.dispose.completed
            && (!self.error_dispose.started || self.error_dispose.completed)
    }
}

struct RuntimeExchange {
    stop: Option<ResourceCell<ReceiverInputConstructionStopOwner>>,
    keys: Vec<ResourceCell<(Arc<dyn ExchangeReceiverPort>, ExchangeReceiverKey)>>,
    registration: ResourceCell<ExchangeRegistration>,
}

/// Original registrations, split only at the dormant-to-running transfer.
/// Every dynamically sized cleanup cell comes from the original registered
/// keys. The paid read-only observer pins remain with that same registration;
/// no rejected-work queue, copied stream-facts table or parking owner is created.
pub(crate) struct RuntimeTaskResources {
    root: Option<ResourceCell<RootResultRegistration>>,
    result: Option<ResourceCell<ResultRegistration>>,
    sink_commit: Option<ResourceCell<SinkCommitRegistration>>,
    exchange: Option<RuntimeExchange>,
    commit_port: Option<ResourceCell<Arc<dyn TaskCommitPort>>>,
    exchange_port: Option<ResourceCell<Arc<dyn ExchangeReceiverPort>>>,
    query_task: Option<ResourceCell<crate::runtime::query_context::QueryTaskGuard>>,
}
impl RuntimeTaskResources {
    pub(crate) fn interrupt(&mut self, reason: &str, control: &RuntimeErrorState) {
        if let Some(root) = &mut self.root {
            root.hook(control, |root| {
                root.cleanup_should_fail = false;
                root.abort(ResultAbort::Cancelled(reason.to_owned()));
                Ok(())
            });
        }
        if let Some(result) = &mut self.result {
            result.hook(control, |result| {
                result.cleanup_should_fail = false;
                result.abort_for_cancellation(reason.to_owned());
                Ok(())
            });
        }
    }

    pub(crate) fn retire(
        &mut self,
        outcome: &super::LocalTaskOutcome,
        control: &RuntimeErrorState,
    ) -> bool {
        use super::LocalTaskOutcome;
        if let Some(root) = &mut self.root {
            root.hook(control, |root| {
                root.cleanup_should_fail = false;
                match outcome {
                    LocalTaskOutcome::Succeeded => root.active = false,
                    LocalTaskOutcome::Failed(error) => {
                        root.abort(ResultAbort::Failed(error.to_string()))
                    }
                    LocalTaskOutcome::Cancelled { reason } => {
                        root.abort(ResultAbort::Cancelled(reason.detail().to_owned()))
                    }
                }
                Ok(())
            });
            root.retire(control);
        }
        if let Some(result) = &mut self.result {
            result.hook(control, |result| {
                match outcome {
                    LocalTaskOutcome::Succeeded => result.finish_success(),
                    LocalTaskOutcome::Failed(error) => result.finish_failure(error.to_string()),
                    LocalTaskOutcome::Cancelled { reason } => {
                        result.finish_cancelled(reason.detail().to_owned())
                    }
                }
                Ok(())
            });
            result.retire(control);
        }
        if let Some(exchange) = &mut self.exchange {
            if let Some(stop) = &mut exchange.stop {
                stop.hook(control, |stop| {
                    stop.stop(novarocks_functions::KernelFailure::Cancelled.into());
                    Ok(())
                });
                stop.retire(control);
            }
            for key in &mut exchange.keys {
                key.hook(control, |(port, key)| {
                    match outcome {
                        LocalTaskOutcome::Succeeded => port.remove(*key),
                        _ => port.cancel(*key),
                    }
                    Ok(())
                });
                key.retire(control);
            }
            exchange.registration.hook(control, |_| Ok(()));
            exchange.registration.retire(control);
        }
        if let Some(commit) = &mut self.sink_commit {
            commit.hook(control, SinkCommitRegistration::rollback);
            commit.retire(control);
        }
        if let Some(port) = &mut self.commit_port {
            port.hook(control, |_| Ok(()));
            port.retire(control);
        }
        if let Some(port) = &mut self.exchange_port {
            port.hook(control, |_| Ok(()));
            port.retire(control);
        }
        let resources_exited = self.root.as_ref().is_none_or(ResourceCell::exited)
            && self.result.as_ref().is_none_or(ResourceCell::exited)
            && self.sink_commit.as_ref().is_none_or(ResourceCell::exited)
            && self.commit_port.as_ref().is_none_or(ResourceCell::exited)
            && self.exchange_port.as_ref().is_none_or(ResourceCell::exited)
            && self.exchange.as_ref().is_none_or(|exchange| {
                exchange.stop.as_ref().is_none_or(ResourceCell::exited)
                    && exchange.keys.iter().all(ResourceCell::exited)
                    && exchange.registration.exited()
            });
        if !resources_exited {
            return false;
        }
        if let Some(query_task) = &mut self.query_task {
            query_task.hook(control, |_| Ok(()));
            query_task.retire(control);
        }
        self.query_task.as_ref().is_none_or(ResourceCell::exited)
    }
}

impl Drop for LocalTaskResources {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

fn root_registration_contract(detail: &'static str) -> LocalTaskLaunchError {
    use crate::exec::fragment::error::{
        FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
    };
    registration_error(FragmentBindingError::new(
        FragmentBindingTarget::Sink,
        FragmentBindingErrorKind::InvalidAssignment,
        detail,
    ))
}

fn registration_error(
    cause: impl Into<crate::runtime::task::ExecutionFailure>,
) -> LocalTaskLaunchError {
    LocalTaskLaunchError::from_failure(
        LocalTaskLaunchStage::Register,
        LocalTaskLaunchErrorKind::ResourceUnavailable,
        cause.into(),
    )
}

#[cfg(test)]
mod receiver_construction_tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchemaRef, typed_fixture};
    use crate::exec::pipeline::operator::input_construction::{
        try_receiver_input_construction_factory, try_runtime_input_construction_factory,
    };
    use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionFactory};
    use crate::runtime::execution_runtime::test_execution_runtime;
    use crate::runtime::task::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort;
    use crate::runtime::task::{
        ExecutionFailure, LocalTaskExecutionError, LocalTaskExecutionErrorKind, LocalTaskOutcome,
        LocalTaskTerminalFact, TaskCancelDetail,
    };
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use novarocks_types::QueryId;

    fn typed_schema(factory: &dyn InputConstructionFactory) -> ChunkSchemaRef {
        let declaration =
            typed_fixture::layout(Arc::new(arrow::datatypes::Schema::empty()), &[], &[]);
        factory.create_compiled_chunk_schema(&declaration).unwrap()
    }

    fn entry(
        key: ExchangeReceiverKey,
        schema: &ChunkSchemaRef,
        factory: &Arc<dyn InputConstructionFactory>,
    ) -> ExchangeReceiverRegistration {
        ExchangeReceiverRegistration {
            key,
            expected_senders: 1,
            wire_chunk_schema: schema.clone(),
            expected_chunk_schema: schema.clone(),
            column_binding: ExchangeColumnBinding::Positional,
            creation_factory: factory.clone(),
        }
    }

    // A duplicate is the real registry's supported refusal, not a fabricated
    // register failure. Protect prefix rollback and original primary/secondary
    // diagnostics using exact typed creators and independent existing keys.
    // This owner-local fixture needs no service, controller or timing gate.
    #[test]
    fn receiver_construction_registration_duplicate_rolls_back_only_its_successful_prefix() {
        for has_prefix in [false, true] {
            let runtime = test_execution_runtime();
            let port: Arc<dyn ExchangeReceiverPort> =
                Arc::new(ExecutionRuntimeExchangeReceiverPort::new(runtime));
            let existing_tracker = MemTracker::new_root("existing receiver declarations");
            existing_tracker.install_limit_once(1024 * 1024).unwrap();
            let existing_factory = try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                existing_tracker.clone(),
                Arc::new(RuntimeErrorState::default()),
            )
            .unwrap();
            let existing_schema = typed_schema(existing_factory.as_ref());
            let instance = UniqueId::new(85_100, i64::from(has_prefix));
            let prefix = ExchangeReceiverKey {
                task_key: instance,
                node_id: 1,
            };
            let conflict = ExchangeReceiverKey {
                task_key: instance,
                node_id: 2,
            };
            let sibling = ExchangeReceiverKey {
                task_key: instance,
                node_id: 3,
            };
            port.register(entry(conflict, &existing_schema, &existing_factory))
                .unwrap();
            port.register(entry(sibling, &existing_schema, &existing_factory))
                .unwrap();
            let before_conflict = port.snapshot(conflict).unwrap();
            let before_sibling = port.snapshot(sibling).unwrap();

            let tracker = MemTracker::new_root("candidate receiver declarations");
            tracker.install_limit_once(1024 * 1024).unwrap();
            let (factory, stop) = try_receiver_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                tracker.clone(),
                Arc::new(RuntimeErrorState::default()),
            )
            .unwrap();
            let schema = typed_schema(factory.as_ref());
            let mut registrations = Vec::new();
            if has_prefix {
                registrations.push(entry(prefix, &schema, &factory));
            }
            registrations.push(entry(conflict, &schema, &factory));
            let result = ExchangeRegistration::acquire_registrations(
                port.clone(),
                registrations,
                has_prefix,
                Some(stop),
            );
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("the original registry must refuse its already registered key"),
            };
            assert_eq!(error.stage(), LocalTaskLaunchStage::Register);
            // Launch kind records the acquisition phase; the actual registry
            // author, not this coarse envelope, owns semantic classification.
            assert_eq!(error.kind(), LocalTaskLaunchErrorKind::ResourceUnavailable);
            let fact = novarocks_failure_contract::FailureProjection::failure_fact(error.cause());
            assert_eq!(
                fact.class(),
                novarocks_failure_contract::FailureClass::Invariant
            );
            assert_eq!(
                fact.reason().domain(),
                novarocks_failure_contract::domain::EXCHANGE.get()
            );
            assert_eq!(fact.reason().code(), 100);
            assert_eq!(
                error.detail(),
                format!(
                    "exchange receiver already registered: task_key={} node_id=2",
                    instance.to_uuid_string(),
                )
            );
            if has_prefix {
                assert_eq!(
                    error
                        .cleanup_diagnostics()
                        .iter()
                        .map(|cause| cause.detail())
                        .collect::<Vec<_>>(),
                    &["injected exchange registration cleanup failure"]
                );
                assert!(port.snapshot(prefix).is_none());
                let refused = match port.receiver_pin(prefix, 1) {
                    Err(error) => error,
                    Ok(_) => panic!("the cancelled prefix cannot acquire a receiver handle"),
                };
                let fact = novarocks_failure_contract::FailureProjection::failure_fact(&refused);
                assert_eq!(
                    fact.class(),
                    novarocks_failure_contract::FailureClass::Control
                );
                assert_eq!(fact.reason().code(), 102);
            } else {
                assert!(error.cleanup_diagnostics().is_empty());
            }
            for (key, before) in [(conflict, before_conflict), (sibling, before_sibling)] {
                let after = port
                    .snapshot(key)
                    .expect("a preexisting receiver must survive another owner's rollback");
                assert_eq!(after.expected_senders, before.expected_senders);
                assert_eq!(after.finished_senders, before.finished_senders);
                assert_eq!(after.queued_chunks, before.queued_chunks);
                assert_eq!(after.queued_rows, before.queued_rows);
                assert_eq!(after.canceled, before.canceled);
                assert!(!after.canceled);
                assert!(port.receiver_pin(key, 1).is_ok());
            }
            let cancelled: ExecutionFailure = novarocks_functions::KernelFailure::Cancelled.into();
            assert_eq!(
                factory.observe_input_work(0).unwrap_err(),
                ConstructionFailure::Caller(Box::new(cancelled))
            );
            drop(schema);
            drop(factory);
            assert_eq!(
                tracker.current(),
                0,
                "the refused owner's actual creator aliases have exited"
            );
            port.remove(conflict);
            port.remove(sibling);
            drop(existing_schema);
            drop(existing_factory);
            assert_eq!(existing_tracker.current(), 0);
        }
    }

    // Raw cancellation includes normal Quiesce and cannot end DATA creation;
    // an originating failure must carry its exact typed cause immediately.
    // The real registration remains installed until the original guard retires.
    #[test]
    fn receiver_construction_retirement_preserves_raw_cancel_and_original_failed_cause() {
        for failed in [false, true] {
            let runtime = test_execution_runtime();
            let port: Arc<dyn ExchangeReceiverPort> =
                Arc::new(ExecutionRuntimeExchangeReceiverPort::new(runtime));
            let tracker = MemTracker::new_root("receiver terminal control");
            tracker.install_limit_once(1024 * 1024).unwrap();
            let (factory, stop) = try_receiver_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                tracker.clone(),
                Arc::new(RuntimeErrorState::default()),
            )
            .unwrap();
            let schema = typed_schema(factory.as_ref());
            let instance = UniqueId::new(85_101, i64::from(failed));
            let key = ExchangeReceiverKey {
                task_key: instance,
                node_id: 1,
            };
            let registration = ExchangeRegistration::acquire_registrations(
                port.clone(),
                vec![entry(key, &schema, &factory)],
                false,
                Some(stop),
            )
            .unwrap()
            .unwrap();
            let mut retirement = ExchangeReceiverRetirement {
                registration,
                disposition: ExchangeRetirementDisposition::Cancel,
            };
            let original: ExecutionFailure =
                novarocks_functions::KernelFailure::DeadlineExceeded.into();
            let outcome = if failed {
                LocalTaskOutcome::Failed(LocalTaskExecutionError::from_failure(
                    LocalTaskExecutionErrorKind::Pipeline,
                    original.clone(),
                ))
            } else {
                LocalTaskOutcome::Cancelled {
                    reason: TaskCancelDetail::new("normal quiesce"),
                }
            };
            retirement.record_terminal(&LocalTaskTerminalFact::new(
                QueryId::new(85_100, 1),
                instance,
                outcome,
                None,
            ));
            if failed {
                assert_eq!(
                    factory.observe_input_work(1).unwrap_err(),
                    ConstructionFailure::Caller(Box::new(original.clone()))
                );
            } else {
                factory.observe_input_work(1).unwrap();
            }
            assert!(
                port.snapshot(key).is_some(),
                "a terminal fact alone cannot delete ingress"
            );
            retirement.retire();
            assert!(port.snapshot(key).is_none());
            let stopped = if failed {
                original
            } else {
                novarocks_functions::KernelFailure::Cancelled.into()
            };
            assert_eq!(
                factory.observe_input_work(1).unwrap_err(),
                ConstructionFailure::Caller(Box::new(stopped))
            );
            drop(schema);
            drop(factory);
            assert_eq!(tracker.current(), 0);
        }
    }

    #[test]
    fn receiver_source_owner_moves_without_retiring_registration_or_its_original_cause() {
        use novarocks_failure_contract::FailureProjection;
        use std::sync::{
            Weak,
            atomic::{AtomicUsize, Ordering},
        };
        struct Source {
            factory: Weak<dyn InputConstructionFactory>,
            drops: Arc<AtomicUsize>,
        }
        impl std::fmt::Debug for Source {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("original receiver retirement source")
            }
        }
        impl std::fmt::Display for Source {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(self, f)
            }
        }
        impl std::error::Error for Source {}
        impl novarocks_failure_contract::FailureProjection for Source {
            fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
                novarocks_functions::KernelFailure::ResourceExhausted.failure_fact()
            }
        }
        impl Drop for Source {
            fn drop(&mut self) {
                let factory = self
                    .factory
                    .upgrade()
                    .expect("the metadata alias remains live");
                let ConstructionFailure::Caller(error) = factory.observe_input_work(1).unwrap_err()
                else {
                    panic!("closed metadata keeps its typed stop observation")
                };
                assert_eq!(
                    error.failure_fact().evidence(),
                    novarocks_failure_contract::FailureEvidence::StopObservation
                );
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let runtime = test_execution_runtime();
        let port: Arc<dyn ExchangeReceiverPort> =
            Arc::new(ExecutionRuntimeExchangeReceiverPort::new(runtime));
        let tracker = MemTracker::new_root("receiver detached source");
        tracker.install_limit_once(1024 * 1024).unwrap();
        let (factory, stop) = try_receiver_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let schema = typed_schema(factory.as_ref());
        let key = ExchangeReceiverKey {
            task_key: UniqueId::new(85_102, 1),
            node_id: 1,
        };
        let registration = ExchangeRegistration::acquire_registrations(
            port.clone(),
            vec![entry(key, &schema, &factory)],
            false,
            Some(stop),
        )
        .unwrap()
        .unwrap();
        let mut retirement = ExchangeReceiverRetirement {
            registration,
            disposition: ExchangeRetirementDisposition::Cancel,
        };
        let drops = Arc::new(AtomicUsize::new(0));
        let original = ExecutionFailure::from_domain(Source {
            factory: Arc::downgrade(&factory),
            drops: drops.clone(),
        });
        let early_alias = original.clone();
        retirement.record_terminal(&LocalTaskTerminalFact::new(
            QueryId::new(85_102, 1),
            key.task_key,
            LocalTaskOutcome::Failed(LocalTaskExecutionError::from_failure(
                LocalTaskExecutionErrorKind::Pipeline,
                original.clone(),
            )),
            None,
        ));
        let owner = retirement.take_construction_stop().unwrap();
        assert!(retirement.take_construction_stop().is_none());
        assert_eq!(retirement.registration.keys, vec![key]);
        assert!(retirement.registration.active);
        assert!(port.snapshot(key).is_some());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let detached = owner.close_after_ingress().unwrap();
        assert!(detached.same_occurrence(&original));
        assert!(factory.observe_input_work(1).is_err());
        drop(detached);
        drop(original);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "the real early source alias remains"
        );
        drop(early_alias);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(
            port.snapshot(key).is_some(),
            "source disposal does not remove ingress"
        );
        retirement.retire();
        assert!(port.snapshot(key).is_none());
        assert!(factory.observe_input_work(1).is_err());
        drop(schema);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn taking_receiver_source_owner_preserves_original_success_disposition() {
        let runtime = test_execution_runtime();
        let port: Arc<dyn ExchangeReceiverPort> =
            Arc::new(ExecutionRuntimeExchangeReceiverPort::new(runtime));
        let tracker = MemTracker::new_root("successful receiver detached source");
        tracker.install_limit_once(1024 * 1024).unwrap();
        let (factory, stop) = try_receiver_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let schema = typed_schema(factory.as_ref());
        let key = ExchangeReceiverKey {
            task_key: UniqueId::new(85_102, 2),
            node_id: 1,
        };
        let registration = ExchangeRegistration::acquire_registrations(
            port.clone(),
            vec![entry(key, &schema, &factory)],
            false,
            Some(stop),
        )
        .unwrap()
        .unwrap();
        let mut retirement = ExchangeReceiverRetirement {
            registration,
            disposition: ExchangeRetirementDisposition::Cancel,
        };
        retirement.record_terminal(&LocalTaskTerminalFact::new(
            QueryId::new(85_102, 2),
            key.task_key,
            LocalTaskOutcome::Succeeded,
            None,
        ));
        let owner = retirement.take_construction_stop().unwrap();
        assert!(matches!(
            retirement.disposition,
            ExchangeRetirementDisposition::Remove
        ));
        assert_eq!(retirement.registration.keys, vec![key]);
        assert!(owner.close_after_ingress().is_none());
        assert!(port.snapshot(key).is_some());
        retirement.retire();
        assert!(port.snapshot(key).is_none());
        assert!(factory.observe_input_work(1).is_err());
        drop(schema);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
}

#[cfg(test)]
mod cleanup_notification_tests {
    use super::*;
    use crate::runtime::task::ExecutionFailure;
    use crate::runtime::task_control::{
        TaskExecutionControl, TaskExecutionPort, test_support::Owner,
    };
    use novarocks_execution_contract::{
        AcceptedTaskConclusion, TaskConclusionPort, TaskConclusionProposal, TaskFailurePhase,
        TaskIdentity,
    };
    use novarocks_failure_contract::{FailureFact, FailureProjection};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    struct OpaqueMetadata(Arc<AtomicUsize>);
    impl Drop for OpaqueMetadata {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("opaque notification payload must remain quarantined");
        }
    }
    struct FailingNotification {
        owner: Arc<Owner>,
        payload_drops: Arc<AtomicUsize>,
        observed: Mutex<Option<FailureFact>>,
        health: Mutex<Option<FailureFact>>,
    }
    impl TaskConclusionPort<ExecutionFailure> for FailingNotification {
        fn identity(&self) -> TaskIdentity {
            self.owner.identity()
        }
        fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
            self.owner.accepted()
        }
        fn submit(&self, proposal: TaskConclusionProposal<ExecutionFailure>) {
            self.owner.submit(proposal)
        }
    }
    impl TaskExecutionPort for FailingNotification {
        fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth> {
            self.owner.execution_health()
        }
        fn report_cleanup(&self, cause: &ExecutionFailure, phase: TaskFailurePhase) {
            *self.observed.lock().unwrap() = Some(cause.failure_fact());
            self.owner.report_cleanup(cause, phase);
            std::panic::panic_any(OpaqueMetadata(self.payload_drops.clone()));
        }
        fn report_unresolved_exit(&self, fact: FailureFact, phase: TaskFailurePhase) {
            *self.health.lock().unwrap() = Some(fact);
            self.owner.report_unresolved_exit(fact, phase);
        }
        fn is_stopped(&self) -> bool {
            self.owner.is_stopped()
        }
        fn stop_observable(&self) -> Arc<crate::runtime::observable::Observable> {
            self.owner.stop_observable()
        }
        fn wait_until_stopped(&self, duration: std::time::Duration) -> bool {
            self.owner.wait_until_stopped(duration)
        }
    }
    #[derive(Debug)]
    struct OriginalCleanupSource(Arc<AtomicUsize>);
    impl std::fmt::Display for OriginalCleanupSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original resource cleanup failure")
        }
    }
    impl std::error::Error for OriginalCleanupSource {}
    impl FailureProjection for OriginalCleanupSource {
        fn failure_fact(&self) -> FailureFact {
            InjectedCleanupFailure(ResourceKind::SinkCommit).failure_fact()
        }
    }
    impl Drop for OriginalCleanupSource {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn failed_cleanup_notification_does_not_retain_exited_resource_or_task_authority() {
        let owner = Owner::new();
        owner.submit(TaskConclusionProposal::Aborted(
            novarocks_execution_contract::AbortCause::PeerTaskFailed,
        ));
        let accepted = owner.accepted().unwrap();
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let port = Arc::new(FailingNotification {
            owner: owner.clone(),
            payload_drops: payload_drops.clone(),
            observed: Mutex::new(None),
            health: Mutex::new(None),
        });
        let weak_port = Arc::downgrade(&port);
        let control = Arc::new(
            TaskExecutionControl::for_test(
                port.identity(),
                port.clone(),
                TaskFailurePhase::Execution,
            )
            .unwrap(),
        );
        let state = RuntimeErrorState::for_task(control.clone());
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("returned cleanup source holder");
        let mut cell = ResourceCell::new(
            crate::runtime::mem_tracker::TrackedBytes::try_new(128, tracker.clone()).unwrap(),
        );
        let source_drops = Arc::new(AtomicUsize::new(0));
        let error = ExecutionFailure::from_domain(OriginalCleanupSource(source_drops.clone()));
        let original_fact = error.failure_fact();
        cell.hook(&state, |_| Err(error));
        cell.retire(&state);
        assert!(
            cell.exited(),
            "metadata delivery is independent from actual hook/holder/source exit"
        );
        assert_eq!(tracker.current(), 0);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
        assert_eq!(*port.observed.lock().unwrap(), Some(original_fact));
        assert_eq!(
            port.health.lock().unwrap().unwrap().reason(),
            super::super::ExecutionReason::PipelineFinisherCallPanicked.reason()
        );
        assert!(
            Arc::ptr_eq(&owner.accepted().unwrap(), &accepted),
            "cleanup cannot refine the actual Peer placeholder"
        );
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
        drop(cell);
        drop(state);
        drop(control);
        drop(port);
        assert!(
            weak_port.upgrade().is_none(),
            "metadata custody contains no Task execution authority"
        );
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    }
}
