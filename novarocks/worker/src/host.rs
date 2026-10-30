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

//! Transport-neutral execution ports driven by the Worker lifecycle owner.

use std::fmt;
use std::sync::Arc;

use novarocks_execution_contract::task_execution::creation::{
    PreparedTaskFacts, TaskCreationInput,
};
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::task_execution::domain::CodecOwnedContent;
use novarocks_execution_contract::task_execution::identity::QueryContextRef;
use novarocks_execution_contract::task_execution::operation::{CredentialUpdate, TaskDomainUpdate};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, SafeDetail, TaskFailureCategory,
};

use novarocks_execution_contract::failure::FailureProjection;

use crate::RuntimeFilterReleaseObservation;
use crate::TaskStatusReporter;
use crate::root_result_channel::RootResultChannel;

/// Actual preparation work borrows its accepted owner. Only this work scope
/// may hand a controller to the installed runtime; immutable metadata receives
/// the separate weak view and cannot acquire a reporting capability from it.
#[derive(Clone, Copy)]
pub struct TaskPreparationContext<'a> {
    owner: &'a Arc<crate::TaskStatusOwner>,
    cadence: std::time::Duration,
}
impl<'a> TaskPreparationContext<'a> {
    pub(crate) fn new(
        owner: &'a Arc<crate::TaskStatusOwner>,
        cadence: std::time::Duration,
    ) -> Self {
        Self { owner, cadence }
    }
    /// Registers only the original child's exit metadata, before any runtime
    /// commit. Immutable TaskPreparationView deliberately has no such method.
    pub fn register_child(
        &self,
        child: Arc<dyn crate::TaskPreparationChild>,
    ) -> Result<(), crate::TaskPreparationChildRegistrationFailure> {
        self.owner.preparation_scope().register(child)
    }
    pub fn view(&self) -> crate::TaskPreparationView {
        crate::TaskPreparationView::from_owner(self.owner, self.cadence)
    }
    pub fn runtime_control(
        &self,
    ) -> Arc<novarocks_execution::runtime::task_control::TaskExecutionControl> {
        self.owner
            .execution_control(novarocks_execution_contract::TaskFailurePhase::Execution)
    }
}

/// One operation's short lease on the exact accepted Task. Immutable metadata
/// keeps only a weak observer; this loan alone pins the owner while work or a
/// bounded wait is in progress. It grants no account, capacity or retry right.
pub struct PreparationControlLoan<'a> {
    control: Option<novarocks_execution::runtime::task_control::TaskExecutionControl>,
    cadence: std::time::Duration,
    _borrow: std::marker::PhantomData<&'a crate::TaskPreparationView>,
}

#[derive(Debug)]
struct PreparationOwnerUnavailable;
impl std::fmt::Display for PreparationOwnerUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("preparation requires a live, unfinished Task owner")
    }
}
impl std::error::Error for PreparationOwnerUnavailable {}
impl novarocks_execution_contract::failure::FailureProjection for PreparationOwnerUnavailable {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::PreparationOwnerUnavailable.reason(),
        )
    }
}
impl<'a> PreparationControlLoan<'a> {
    pub(crate) fn from_view(view: &'a crate::TaskPreparationView) -> Self {
        let (owner, cadence) = view.control();
        let control = owner.map(|owner| {
            novarocks_execution::runtime::task_control::TaskExecutionControl::new(
                owner.identity(),
                owner,
                novarocks_execution_contract::TaskFailurePhase::Preparation,
            )
            .expect("preparation borrows its exact Task owner")
        });
        Self {
            control,
            cadence,
            _borrow: std::marker::PhantomData,
        }
    }
    pub fn checkpoint(&self) -> novarocks_execution::runtime::fragment::ExecutionResult<()> {
        let Some(control) = &self.control else {
            return Err(
                novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(
                    PreparationOwnerUnavailable,
                ),
            );
        };
        if control.is_stopped() {
            // A successful conclusion is not permission to start new work.
            return Err(control.error().unwrap_or_else(|| {
                novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(
                    PreparationOwnerUnavailable,
                )
            }));
        }
        Ok(())
    }
    pub fn wait(&self) -> novarocks_execution::runtime::fragment::ExecutionResult<()> {
        self.checkpoint()?;
        let control = self
            .control
            .as_ref()
            .expect("checkpoint qualified the live Task");
        control.wait_until_stopped(self.cadence);
        self.checkpoint()
    }
}

/// Direct host tests own an actual Task acceptance domain for their declared
/// identity. The fixture must outlive all operations using its weak views.
#[cfg(any(test, novarocks_dev_controls))]
pub struct TestPreparationControl {
    status: Arc<crate::TaskStatusOwner>,
    view: crate::TaskPreparationView,
}
#[cfg(any(test, novarocks_dev_controls))]
impl TestPreparationControl {
    pub fn new(
        identity: novarocks_execution_contract::TaskIdentity,
        cadence: std::time::Duration,
    ) -> Self {
        assert!(
            !cadence.is_zero(),
            "test preparation cadence must be explicit and nonzero"
        );
        let status = Arc::new(crate::TaskStatusOwner::new(
            identity,
            Arc::new(crate::TaskStatusSource::new()),
            Arc::new(crate::ManualClock::new()),
            std::time::Duration::ZERO,
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
        ));
        Self::from_status(status, cadence)
    }
    pub fn from_status(status: Arc<crate::TaskStatusOwner>, cadence: std::time::Duration) -> Self {
        let cell = crate::task_registry_entry::CreationCell::for_accepted_fixture(&status);
        cell.accept(status.clone());
        Self {
            status,
            view: cell.preparation_stop_view(cadence),
        }
    }
    pub fn status(&self) -> &Arc<crate::TaskStatusOwner> {
        &self.status
    }
    pub fn loan(&self) -> PreparationControlLoan<'_> {
        self.view.borrow_control()
    }
    pub fn stop_view(&self) -> crate::TaskPreparationView {
        self.view.clone()
    }
    pub fn scope(&self) -> TaskPreparationContext<'_> {
        TaskPreparationContext::new(&self.status, self.view.control().1)
    }
    /// The direct host call has returned. Drive the original finite rollback
    /// scope with the actual host cleanup calls, without accepting a new cause.
    /// The fixture must first publish its real refusal or explicit stop intent.
    pub fn rollback_host(&self, remove_inbound: impl FnOnce(), remove_receiver: impl FnOnce()) {
        let scope = self.status.preparation_scope();
        scope.request_rollback();
        scope.cleanup(
            crate::task_preparation::PreparationCleanup::Inbound,
            remove_inbound,
        );
        scope.cleanup(
            crate::task_preparation::PreparationCleanup::Receiver,
            remove_receiver,
        );
        scope.body_exited();
    }
    /// Actual child/holder exit, rather than disappearance of a fixture alias.
    pub fn rollback_exited(&self) -> bool {
        self.status.preparation_scope().rollback_exited()
    }
    pub fn stop(&self, stop: crate::TestPreparationIntent) -> bool {
        use novarocks_execution_contract::{
            TaskConclusion, TaskConclusionPort, TaskConclusionProposal, TerminationDetail,
        };
        let (proposal, expected) = match stop {
            crate::TestPreparationIntent::Cancel(reason) => (
                TaskConclusionProposal::Canceled(reason),
                TerminationDetail::Canceled(reason),
            ),
            crate::TestPreparationIntent::Abort(cause) => (
                TaskConclusionProposal::Aborted(cause),
                TerminationDetail::Aborted(cause),
            ),
        };
        self.status.submit(proposal);
        // This helper binds the synchronous Worker implementation.
        self.status.accepted().unwrap().neutral().conclusion()
            == &TaskConclusion::Terminated(expected)
    }
}

/// Pure preparation facts and the exact runtime owner created alongside them.
/// The context's creation transaction takes the optional root atomically;
/// transport handles never enter the shared execution-contract vocabulary.
pub struct PreparedTaskInstallation {
    facts: PreparedTaskFacts,
    root: Option<Arc<RootResultChannel>>,
}
impl std::fmt::Debug for PreparedTaskInstallation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedTaskInstallation")
            .field("facts", &self.facts)
            .field("has_root", &self.root.is_some())
            .finish()
    }
}
impl PreparedTaskInstallation {
    pub fn new(
        facts: PreparedTaskFacts,
        root: Option<Arc<RootResultChannel>>,
    ) -> Result<Self, HostRejection> {
        if root.is_some()
            && facts.sink_kind() != novarocks_execution_contract::FragmentSinkKind::Result
        {
            return Err(installation_rejection(
                PreparedInstallationFailure::RootSink,
            ));
        }
        Ok(Self { facts, root })
    }
    pub fn facts(&self) -> PreparedTaskFacts {
        self.facts
    }
    pub fn validate_task(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        if let Some(root) = self.root.as_ref()
            && root.spec().task != descriptor.identity()
        {
            return Err(installation_rejection(
                PreparedInstallationFailure::RootTask {
                    expected: descriptor.identity(),
                    actual: root.spec().task,
                },
            ));
        }
        Ok(())
    }
    pub fn into_parts(self) -> (PreparedTaskFacts, Option<Arc<RootResultChannel>>) {
        (self.facts, self.root)
    }
}

/// The shared facts one establish installs, handed over as a single unit.
///
/// They are passed together because they become observable together: the
/// context reaches `Active` only once all four are materialized, so a host
/// never publishes a catalog binding a query's credential cannot yet read.
pub struct SharedFactsRequest<'a> {
    context: QueryContextRef,
    catalog_binding: &'a Arc<dyn CodecOwnedContent>,
    initial_runtime_filter: &'a Arc<dyn CodecOwnedContent>,
    query_options: &'a Arc<dyn CodecOwnedContent>,
    initial_credential: &'a CredentialUpdate,
}

impl<'a> SharedFactsRequest<'a> {
    pub const fn new(
        context: QueryContextRef,
        catalog_binding: &'a Arc<dyn CodecOwnedContent>,
        initial_runtime_filter: &'a Arc<dyn CodecOwnedContent>,
        query_options: &'a Arc<dyn CodecOwnedContent>,
        initial_credential: &'a CredentialUpdate,
    ) -> Self {
        Self {
            context,
            catalog_binding,
            initial_runtime_filter,
            query_options,
            initial_credential,
        }
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn catalog_binding(&self) -> &'a Arc<dyn CodecOwnedContent> {
        self.catalog_binding
    }

    pub const fn initial_runtime_filter(&self) -> &'a Arc<dyn CodecOwnedContent> {
        self.initial_runtime_filter
    }

    pub const fn query_options(&self) -> &'a Arc<dyn CodecOwnedContent> {
        self.query_options
    }

    pub const fn initial_credential(&self) -> &'a CredentialUpdate {
        self.initial_credential
    }
}

#[derive(Debug)]
enum PreparedInstallationFailure {
    RootSink,
    RootTask {
        expected: novarocks_execution_contract::TaskIdentity,
        actual: novarocks_execution_contract::TaskIdentity,
    },
}
impl fmt::Display for PreparedInstallationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootSink => f.write_str("bounded root channel requires a result sink"),
            Self::RootTask { expected, actual } => write!(
                f,
                "prepared root channel names a different task: expected {expected}, got {actual}"
            ),
        }
    }
}
impl std::error::Error for PreparedInstallationFailure {}
impl novarocks_execution_contract::failure::FailureProjection for PreparedInstallationFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            match self {
                Self::RootSink => R::PreparedRootSink,
                Self::RootTask { .. } => R::PreparedRootTask,
            }
            .reason(),
        )
    }
}
fn installation_rejection(source: PreparedInstallationFailure) -> HostRejection {
    // This local formatter contains only static policy text and exact IDs.
    let detail = SafeDetail::truncating_display(&source);
    HostRejection::from_domain_with_detail(TaskFailureCategory::Protocol, source, detail)
}

#[cfg(novarocks_dev_controls)]
#[derive(Debug)]
pub(crate) enum TestUpdateConsumptionFailure {
    UnsupportedHost,
    LiveTaskRequired(novarocks_execution_contract::TaskIdentity),
}
#[cfg(novarocks_dev_controls)]
impl fmt::Display for TestUpdateConsumptionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedHost => f.write_str(
                "this execution host does not support a controlled split-consumption gate",
            ),
            Self::LiveTaskRequired(identity) => {
                write!(f, "replay gate requires actual live Task {identity}")
            }
        }
    }
}
#[cfg(novarocks_dev_controls)]
impl std::error::Error for TestUpdateConsumptionFailure {}
#[cfg(novarocks_dev_controls)]
impl novarocks_execution_contract::failure::FailureProjection for TestUpdateConsumptionFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            match self {
                Self::UnsupportedHost => R::TestUpdateConsumptionUnsupportedHost.reason(),
                Self::LiveTaskRequired(_) => R::TestUpdateConsumptionLiveTaskRequired.reason(),
            },
        )
    }
}

/// A port refusal retaining its actual local domain source. The legacy
/// category/detail are this operation's response projection, never a Task
/// conclusion and never an authority that can reclassify the domain facts.
/// Only TaskStatusOwner accepts a Task failure, with explicit scope/phase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostRejection {
    response_category: TaskFailureCategory,
    detail: SafeDetail,
    fact: novarocks_execution_contract::failure::FailureFact,
    source: novarocks_execution::runtime::fragment::ExecutionFailure,
    closed_queue: bool,
}
impl HostRejection {
    pub fn from_domain<E>(category: TaskFailureCategory, source: E) -> Self
    where
        E: std::error::Error
            + novarocks_execution_contract::failure::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        Self::from_failure(
            category,
            novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(source),
        )
    }
    pub fn from_failure(
        category: TaskFailureCategory,
        source: novarocks_execution::runtime::fragment::ExecutionFailure,
    ) -> Self {
        let fact = source.failure_fact();
        let detail = source.safe_detail_for_fact(fact);
        Self {
            response_category: category,
            detail,
            fact,
            source,
            closed_queue: false,
        }
    }
    /// The boundary supplies context made only from known-safe typed facts.
    /// The source's class/reason and local error chain remain unchanged.
    pub fn from_domain_with_detail<E>(
        category: TaskFailureCategory,
        source: E,
        detail: SafeDetail,
    ) -> Self
    where
        E: std::error::Error
            + novarocks_execution_contract::failure::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        let source = novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(source);
        let fact = source.failure_fact();
        Self {
            response_category: category,
            detail,
            fact,
            source,
            closed_queue: false,
        }
    }
    pub fn from_closed_queue<E>(category: TaskFailureCategory, source: E) -> Self
    where
        E: std::error::Error
            + novarocks_execution_contract::failure::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        Self {
            closed_queue: true,
            ..Self::from_domain(category, source)
        }
    }
    pub const fn is_closed_queue(&self) -> bool {
        self.closed_queue
    }
    pub const fn category(&self) -> TaskFailureCategory {
        self.response_category
    }
    pub const fn detail(&self) -> &SafeDetail {
        &self.detail
    }
    /// Freeze this port's neutral facts before source transfer or foreign cleanup.
    pub fn operation_failure(
        &self,
    ) -> novarocks_execution_contract::operation::OperationDomainFailure {
        novarocks_execution_contract::operation::OperationDomainFailure::new(
            self.fact,
            self.detail.clone(),
        )
    }
    /// Transfer the original execution carrier without rebuilding it from the
    /// response projection or retaining it in a neutral tombstone.
    pub fn into_failure(self) -> novarocks_execution::runtime::fragment::ExecutionFailure {
        self.source
    }
}
impl fmt::Display for HostRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.response_category, self.detail)
    }
}
impl std::error::Error for HostRejection {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_execution_contract::failure::FailureProjection for HostRejection {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        self.fact
    }
}

/// The opaque terminal evidence one query-context release sealed.
///
/// The Worker owns the lifetime of this fact: a context keeps it after shared
/// facts are released so the transport adapter can include it in the matching
/// release acknowledgement.  It deliberately does not name the generated
/// message.  Only the role-local adapter that produced the content may recover
/// and encode that representation.
#[derive(Clone)]
pub struct ReleasedContextEvidence {
    runtime_filter: Option<Arc<dyn CodecOwnedContent>>,
    runtime_filter_observation: RuntimeFilterReleaseObservation,
}

impl Default for ReleasedContextEvidence {
    fn default() -> Self {
        Self::none()
    }
}

impl fmt::Debug for ReleasedContextEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReleasedContextEvidence")
            .field(
                "runtime_filter_fingerprint",
                &self
                    .runtime_filter
                    .as_ref()
                    .map(|content| content.fingerprint()),
            )
            .field(
                "runtime_filter_encoded_len",
                &self
                    .runtime_filter
                    .as_ref()
                    .map(|content| content.encoded_len()),
            )
            .field(
                "runtime_filter_observation",
                &self.runtime_filter_observation,
            )
            .finish()
    }
}

impl ReleasedContextEvidence {
    /// Evidence from a tear-down that found no participant to seal.
    pub const fn none() -> Self {
        Self {
            runtime_filter: None,
            runtime_filter_observation: RuntimeFilterReleaseObservation::Absent,
        }
    }

    /// Sealed runtime-filter evidence and its Worker-owned classification.
    ///
    /// A caller may not attach an `Absent` classification to present content:
    /// that would make the retained value and the lifecycle marker contradict
    /// one another.
    pub fn with_runtime_filter(
        runtime_filter: Arc<dyn CodecOwnedContent>,
        runtime_filter_observation: RuntimeFilterReleaseObservation,
    ) -> Self {
        assert!(
            !matches!(
                runtime_filter_observation,
                RuntimeFilterReleaseObservation::Absent
            ),
            "present runtime-filter evidence must not be classified absent"
        );
        Self {
            runtime_filter: Some(runtime_filter),
            runtime_filter_observation,
        }
    }

    pub fn runtime_filter(&self) -> Option<&Arc<dyn CodecOwnedContent>> {
        self.runtime_filter.as_ref()
    }

    pub const fn runtime_filter_observation(&self) -> RuntimeFilterReleaseObservation {
        self.runtime_filter_observation
    }
}

/// The query-context side of execution.
///
/// `materialize` runs while the context is `Establishing`, outside the owner's
/// lock and already racing the sequence-zero lease. Whatever it installs must
/// be undone by `release`, because an establish that loses that race rolls
/// back rather than reaching `Active` late.
pub trait QueryContextHost: Send + Sync {
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection>;

    /// Undoes everything `materialize` installed. Called for a rollback and
    /// for a normal release, so it must be idempotent.
    ///
    /// Returns the terminal evidence the tear-down sealed. A release is the
    /// only point at which the runtime-filter observation is complete -- every
    /// local task is a terminal record and nothing further will be observed --
    /// so the seal happens here and its result is handed back rather than left
    /// inside the host for a later reader to go looking for.
    fn release(&self, context: QueryContextRef) -> ReleasedContextEvidence;

    /// Applies a shared-domain advance the owner already classified as
    /// applicable.
    fn advance_shared_domain(
        &self,
        context: QueryContextRef,
        domain: &novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate,
    ) -> Result<(), HostRejection>;
}

/// A submitted, runnable task.
///
/// The handle is deliberately narrow: the owner publishes status and decides
/// terminal outcomes. The registry opens its completion gate and starts the
/// dormant task only after installing its Live record.
pub trait RunnableTask: fmt::Debug + Send + Sync {
    /// Opens completion processing and starts the prepared task after the
    /// registry has installed the Live creation. The task may publish a
    /// synchronous stop; its exact completion slot retains that fact.
    fn commit_creation(&self);

    fn cancel(&self, reason: CancelReason);

    /// Stop normally after the context admission fence while retaining
    /// already published root results and write evidence until release.
    fn quiesce(&self) {
        self.cancel(CancelReason::UpstreamNoLongerNeeded);
    }

    fn abort(&self, cause: AbortCause);
}

/// A test-only reservation rolls back unless the real ACK-drop takes custody.
#[cfg(novarocks_dev_controls)]
pub trait TestTaskUpdateConsumptionReservation: Send {
    /// Called only after the actual accepted terminal ACK-drop claim exists.
    fn activate(&mut self) -> Result<(), String>;
    /// Transfers the pause only once the boundary really drops that ACK.
    fn acknowledgement_dropped(&mut self);
}

/// The task side of execution.
///
/// The three install steps are called in exactly the order the creation
/// transaction commits them, and `submit_runnable` is last. It prepares a
/// dormant runnable; `commit_creation` starts executable work only after the
/// registry has installed the Live record.
pub trait TaskExecutionHost: Send + Sync {
    /// Closes data-plane admission for every task of this exact query
    /// execution. The registry calls this while it linearizes context
    /// termination, before any per-task capability can be withdrawn.
    fn close_context_admission(&self, context: QueryContextRef);

    /// An abort revokes any earlier normal-close answers for this execution.
    fn abort_context_admission(&self, context: QueryContextRef) {
        self.close_context_admission(context);
    }
    /// Releases an idle execution context after every task has physically
    /// converged and no new task can be admitted for this exact attempt.
    fn retire_context_execution(&self, context: QueryContextRef);

    /// Reclaims the compact context fence after the registry has forgotten
    /// the context itself. No task capability for the execution may remain.
    fn forget_context_admission(&self, context: QueryContextRef);

    /// Whether this context still owns a reserved normal-close answer that
    /// must survive retained-context capacity pressure until its request horizon.
    fn retains_normal_close(&self, _context: QueryContextRef) -> bool {
        false
    }

    /// Prepares the task a creation winner owns, from the input its create
    /// request carried.
    ///
    /// This is the only call that interprets a task's static plan. It proves
    /// the plan and the assignment against the descriptor before anything is
    /// published, prepares a dormant runtime, and returns what it proved.
    /// A refusal returns only after this host has undone everything it
    /// prepared: the owner has not yet recorded an installed receiver, so it
    /// cannot undo anything on the host's behalf.
    fn install_receiver(
        &self,
        descriptor: &TaskDescriptor,
        input: TaskCreationInput,
        preparation: TaskPreparationContext<'_>,
    ) -> Result<PreparedTaskInstallation, HostRejection>;

    fn remove_receiver(&self, descriptor: &TaskDescriptor);

    fn install_inbound_capability(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection>;

    /// Reserve the bounded late-frame record before Create is Accepted.
    fn reserve_inbound_close_capacity(
        &self,
        _descriptor: &TaskDescriptor,
    ) -> Result<(), HostRejection> {
        Ok(())
    }

    fn remove_inbound_capability(&self, descriptor: &TaskDescriptor);

    /// The production host serializes receiver removal with inbound delivery.
    fn retire_receiver_normally(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        self.remove_inbound_capability(descriptor);
        self.remove_receiver(descriptor);
        Ok(())
    }

    fn submit_runnable(
        &self,
        descriptor: &TaskDescriptor,
        reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection>;

    /// Reserves a finite input pause for an explicitly opted-in test fault.
    #[cfg(novarocks_dev_controls)]
    fn reserve_test_update_consumption(
        &self,
        _request: &novarocks_execution_contract::UpdateTask,
        _control: novarocks_failpoint::scope::ControlCancellation,
    ) -> Result<Box<dyn TestTaskUpdateConsumptionReservation>, HostRejection> {
        Err(HostRejection::from_domain(
            TaskFailureCategory::Internal,
            TestUpdateConsumptionFailure::UnsupportedHost,
        ))
    }

    /// Applies a task-scoped domain advance the owner already classified as
    /// applicable, under the original CreateTask or UpdateTask operation.
    ///
    /// Returns how many splits the plan node still holds after the offer, for
    /// the domains that have a queue. The sender reads it as backpressure, so
    /// it must be measured rather than defaulted: reporting zero for a domain
    /// that never counted says the task is idle and invites the sender to keep
    /// filling a queue that is already full. `None` means this domain has no
    /// queue to report, which is a different statement from an empty one.
    fn apply_task_domain(
        &self,
        descriptor: &TaskDescriptor,
        domain: &TaskDomainUpdate,
        operation: novarocks_execution_contract::TaskOperationId,
    ) -> Result<Option<u64>, HostRejection>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct ClosedQueueFixture(String);
    impl fmt::Display for ClosedQueueFixture {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }
    impl std::error::Error for ClosedQueueFixture {}
    impl novarocks_execution_contract::failure::FailureProjection for ClosedQueueFixture {
        fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
            use novarocks_execution_contract::failure::*;
            FailureFact::stop_observation(FailureReason::new(
                domain::WORKER,
                std::num::NonZeroU16::new(65000).unwrap(),
            ))
        }
    }

    #[test]
    fn a_port_rejection_stays_bounded_and_preserves_the_closed_queue_fact() {
        let rejection = HostRejection::from_closed_queue(
            TaskFailureCategory::Protocol,
            ClosedQueueFixture("x".repeat(8 * 1024)),
        );

        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
        assert!(rejection.is_closed_queue());
        assert!(rejection.detail().as_str().len() < 8 * 1024);
    }

    fn preparation_identity() -> novarocks_execution_contract::TaskIdentity {
        use novarocks_types::identity::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        novarocks_execution_contract::TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(812, 1), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }

    #[test]
    fn preparation_control_stop_before_wait_and_notification_race_never_lose_stop() {
        for before in [false, true] {
            for _ in 0..64 {
                let owner = std::sync::Arc::new(TestPreparationControl::new(
                    preparation_identity(),
                    std::time::Duration::from_secs(30),
                ));
                let stop =
                    crate::TestPreparationIntent::Cancel(CancelReason::UpstreamNoLongerNeeded);
                if before {
                    assert!(owner.stop(stop));
                }
                let (send, receive) = std::sync::mpsc::channel();
                let other = Arc::clone(&owner);
                let worker = std::thread::spawn(move || {
                    send.send(other.loan().wait()).unwrap();
                });
                if !before {
                    assert!(owner.stop(stop));
                }
                let observed = receive
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .unwrap()
                    .unwrap_err();
                use novarocks_execution_contract::{
                    TaskConclusion, TaskConclusionPort, TerminationDetail,
                };
                assert_eq!(
                    owner.status().accepted().unwrap().neutral().conclusion(),
                    &TaskConclusion::Terminated(TerminationDetail::Canceled(
                        CancelReason::UpstreamNoLongerNeeded
                    ))
                );
                assert_eq!(
                    novarocks_execution_contract::failure::FailureProjection::failure_fact(
                        &observed
                    )
                    .evidence(),
                    novarocks_execution_contract::failure::FailureEvidence::StopObservation
                );
                worker.join().unwrap();
                assert!(owner.loan().checkpoint().is_err());
            }
        }
    }

    #[test]
    fn preparation_control_explicit_cadence_returns_retry_without_creating_stop() {
        let owner = TestPreparationControl::new(
            preparation_identity(),
            std::time::Duration::from_millis(1),
        );
        assert!(owner.loan().wait().is_ok());
        assert!(owner.loan().checkpoint().is_ok());
    }
}

#[cfg(test)]
mod operation_failure_snapshot_tests {
    use super::*;
    use novarocks_execution_contract::failure::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Debug)]
    struct ActualDomain {
        fact: FailureFact,
        drops: Arc<AtomicUsize>,
    }
    impl std::fmt::Display for ActualDomain {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("private provider material")
        }
    }
    impl std::error::Error for ActualDomain {}
    impl FailureProjection for ActualDomain {
        fn failure_fact(&self) -> FailureFact {
            self.fact
        }
    }
    impl Drop for ActualDomain {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[derive(Debug)]
    struct CountedProjectionDomain {
        first: FailureFact,
        later: FailureFact,
        projections: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl std::fmt::Display for CountedProjectionDomain {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("private counted provider material")
        }
    }
    impl std::error::Error for CountedProjectionDomain {}
    impl FailureProjection for CountedProjectionDomain {
        fn failure_fact(&self) -> FailureFact {
            if self.projections.fetch_add(1, Ordering::AcqRel) == 0 {
                self.first
            } else {
                // Deliberately reveal a forbidden second call instead of
                // allowing a stable mock to hide re-projection in replay.
                self.later
            }
        }
    }
    impl Drop for CountedProjectionDomain {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[test]
    fn frozen_operation_snapshot_and_cached_replay_never_reproject_the_original_source() {
        use novarocks_execution::runtime::fragment::{ExecutionFailure, PipelineOperation};
        use novarocks_execution_contract::operation::{DomainFailureOutcome, OperationResult};
        let nz = |n| std::num::NonZeroU16::new(n).unwrap();
        let first = FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(nz(65534), nz(65001)),
        )
        .with_row_position(FailureRowPosition {
            selected_ordinal: 0,
            batch_row: 19,
        });
        let later = FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(nz(65534), nz(65002)),
        );
        for explicit_detail in [false, true] {
            let projections = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let cause = CountedProjectionDomain {
                first,
                later,
                projections: Arc::clone(&projections),
                drops: Arc::clone(&drops),
            };
            let rejection = if explicit_detail {
                HostRejection::from_domain_with_detail(
                    TaskFailureCategory::Protocol,
                    cause,
                    SafeDetail::new("producer checked context").unwrap(),
                )
            } else {
                HostRejection::from_failure(
                    TaskFailureCategory::Protocol,
                    ExecutionFailure::from_domain(cause).at_operator(7, PipelineOperation::Push),
                )
            };
            assert_eq!(
                projections.load(Ordering::Acquire),
                1,
                "both actual constructors freeze exactly one fact"
            );
            let expected_detail = if explicit_detail {
                "producer checked context"
            } else {
                "Representation failure (reason 65534:65001) at operator 7 during Push at batch row 19 (selected ordinal 0)"
            };
            assert_eq!(rejection.detail().as_str(), expected_detail);
            let original = rejection.source.clone();
            let alias = rejection.clone();
            let snapshot = rejection.operation_failure();
            assert_eq!(snapshot.fact(), first);
            assert_eq!(snapshot.detail().as_str(), expected_detail);
            assert_eq!(alias.operation_failure(), snapshot);
            assert_eq!(rejection.operation_failure(), snapshot);
            assert_eq!(rejection.failure_fact(), first);
            let cached = crate::task_registry_entry::CreationFailure::domain(
                DomainFailureOutcome::InvalidStateOrRequest,
                snapshot.clone(),
            );
            for _ in 0..3 {
                let receipt: crate::OperationReceipt<()> =
                    cached.receipt(novarocks_execution_contract::TaskOperationId::new_v7());
                let OperationResult::DomainFailure { failure, .. } = receipt.result() else {
                    panic!("the actual cached domain branch must survive replay");
                };
                assert_eq!(failure, &snapshot);
                assert!(receipt.acknowledgement().is_none());
            }
            assert_eq!(
                projections.load(Ordering::Acquire),
                1,
                "all snapshots and cached replay use the already frozen fact"
            );
            drop(alias);
            let carried = rejection.into_failure();
            assert!(std::ptr::eq(carried.cause(), original.cause()));
            drop(carried);
            drop(original);
            assert_eq!(
                drops.load(Ordering::Acquire),
                1,
                "the actual domain source exits once"
            );
            assert_eq!(projections.load(Ordering::Acquire), 1);
            assert_eq!(snapshot.fact(), first);
        }
    }

    #[test]
    fn frozen_port_failure_keeps_original_facts_without_retaining_domain_state() {
        let drops = Arc::new(AtomicUsize::new(0));
        let nz = |n| std::num::NonZeroU16::new(n).unwrap();
        let fact = FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(nz(65534), nz(65535)),
        )
        .with_row_position(FailureRowPosition {
            selected_ordinal: 0,
            batch_row: 19,
        })
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Request),
            constraint_scope: Some(ResourceScope::Task),
            dimension: ResourceDimension::Bytes,
            requested: Some(0),
            required_growth: None,
            limit: Some(u64::MAX),
            live: None,
            committed: Some(0),
        })
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: nz(65535),
                code: nz(1),
            },
            required: Some(0),
            extent_limit: None,
            representable_prefix: Some(0),
        });
        let source =
            novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(ActualDomain {
                fact,
                drops: drops.clone(),
            });
        let original = source.clone();
        let rejection = HostRejection::from_failure(TaskFailureCategory::Protocol, source);
        let frozen = rejection.operation_failure();
        assert_eq!(frozen.fact(), fact);
        assert!(
            !frozen
                .detail()
                .as_str()
                .contains("private provider material")
        );
        let carried = rejection.into_failure();
        assert!(std::ptr::eq(carried.cause(), original.cause()));
        drop(carried);
        drop(original);
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert_eq!(
            frozen.fact(),
            fact,
            "neutral operation failure owns no source or Task control"
        );
    }
}
