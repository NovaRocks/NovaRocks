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
use novarocks_execution_contract::task_execution::identity::ParticipationRef;
use novarocks_execution_contract::task_execution::operation::{CredentialUpdate, TaskDomainUpdate};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, SafeDetail, TaskFailureCategory,
};

use novarocks_execution_contract::failure::FailureProjection;

use crate::RuntimeFilterReleaseObservation;
use crate::TaskStatusReporter;
use crate::root_result_channel::RootResultChannel;

/// The actual preparation capture moves this complete original set of work and capabilities.
/// Binding refusal returns the same set; observers cannot recreate its work or capabilities.
pub struct TaskPreparationCapture {
    pub completion_work: novarocks_execution_contract::AdmittedWorkParts<
        novarocks_execution::runtime::task::ExecutionFailure,
    >,
    pub start: crate::TaskStartPermit,
    pub terminal: crate::TaskTerminalPermit,
    pub statistics: crate::TaskStatisticsProducer,
    pub dynamic_filters: crate::TaskDynamicFilterProducer,
    pub final_statistics: crate::TaskStatisticsFinalizer,
    pub completion_exit: crate::CompletionExitNotice,
}
impl TaskPreparationCapture {
    pub(crate) fn matches(&self, identity: novarocks_execution_contract::TaskIdentity) -> bool {
        self.completion_work.identity() == Some(identity)
            && self.start.identity() == identity
            && self.terminal.identity() == identity
            && self.statistics.identity() == identity
            && self.dynamic_filters.identity() == identity
            && self.final_statistics.identity() == identity
            && self.completion_exit.identity() == identity
    }
}
impl fmt::Debug for TaskPreparationCapture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskPreparationCapture")
            .field("identity", &self.start.identity())
            .finish_non_exhaustive()
    }
}

/// Immutable metadata observes one exact accepted Task without retaining its
/// owner, reporter, runtime, input, or publication capability. A short loan
/// upgrades the owner only while an operation is actually using it.
#[derive(Clone)]
pub struct TaskPreparationView {
    owner: std::sync::Weak<dyn novarocks_execution::runtime::task_control::TaskExecutionPort>,
    identity: novarocks_execution_contract::TaskIdentity,
    cadence: std::time::Duration,
    admission: Option<novarocks_control_runtime::obligation::AdmissionProof>,
}

impl TaskPreparationView {
    pub(crate) fn from_execution_port(
        owner: &Arc<dyn novarocks_execution::runtime::task_control::TaskExecutionPort>,
        cadence: std::time::Duration,
        admission: Result<
            novarocks_control_runtime::obligation::AdmissionProof,
            novarocks_control_runtime::transaction::RuntimeError,
        >,
    ) -> Self {
        Self {
            owner: Arc::downgrade(owner),
            identity: owner.identity(),
            cadence,
            admission: admission.ok(),
        }
    }
    pub const fn identity(&self) -> novarocks_execution_contract::TaskIdentity {
        self.identity
    }
    pub fn borrow_control(&self) -> crate::PreparationControlLoan<'_> {
        crate::PreparationControlLoan::from_view(self)
    }

    pub(crate) fn control(
        &self,
    ) -> (
        Option<Arc<dyn novarocks_execution::runtime::task_control::TaskExecutionPort>>,
        std::time::Duration,
    ) {
        (self.owner.upgrade(), self.cadence)
    }
}

/// One actual preparation body owns one original, finite set of Task capabilities.
/// Metadata gets only a weak stop view. Moving this context into the Native
/// host cannot copy the finisher or sign another settlement registration.
pub struct TaskPreparationContext<'a> {
    owner: Arc<dyn novarocks_execution::runtime::task_control::TaskExecutionPort>,
    children: &'a Arc<crate::task_preparation::TaskPreparationScope>,
    cadence: std::time::Duration,
    readiness_waker: std::task::Waker,
    preparation_capture: Option<TaskPreparationCapture>,
    admission: novarocks_control_runtime::obligation::AdmissionProof,
    task_key: novarocks_types::UniqueId,
}
impl<'a> TaskPreparationContext<'a> {
    pub(crate) fn from_admitted(
        owner: Arc<dyn novarocks_execution::runtime::task_control::TaskExecutionPort>,
        children: &'a Arc<crate::task_preparation::TaskPreparationScope>,
        cadence: std::time::Duration,
        capture: TaskPreparationCapture,
        readiness_waker: std::task::Waker,
        admission: novarocks_control_runtime::obligation::AdmissionProof,
        task_key: novarocks_types::UniqueId,
    ) -> Result<
        Self,
        (
            novarocks_execution::runtime::task_control::TaskControlIdentityMismatch,
            TaskPreparationCapture,
        ),
    > {
        if !capture.matches(owner.identity()) || children.identity() != owner.identity() {
            return Err((
                novarocks_execution::runtime::task_control::TaskControlIdentityMismatch,
                capture,
            ));
        }
        Ok(Self {
            owner,
            children,
            cadence,
            readiness_waker,
            preparation_capture: Some(capture),
            admission,
            task_key,
        })
    }
    #[cfg(test)]
    pub(crate) fn original_children_for_test(
        &self,
    ) -> &Arc<crate::task_preparation::TaskPreparationScope> {
        self.children
    }
    /// The same original preparation readiness source, installed before Root
    /// birth. Cloning its standard Waker grants no Task/output authority.
    pub fn readiness_waker(&self) -> std::task::Waker {
        self.readiness_waker.clone()
    }
    /// Registers before the provider child attaches kernel/root captures. The
    /// original preparation owner continues to supervise rollback/adoption.
    pub fn register_child(
        &self,
        child: Arc<dyn crate::TaskPreparationChild>,
    ) -> Result<(), crate::TaskPreparationChildRegistrationFailure> {
        self.children.register(child)
    }
    pub fn view(&self) -> crate::TaskPreparationView {
        crate::TaskPreparationView::from_execution_port(
            &self.owner,
            self.cadence,
            Ok(self.admission.clone()),
        )
    }
    pub fn runtime_control(
        &self,
    ) -> Arc<novarocks_execution::runtime::task_control::TaskExecutionControl> {
        Arc::new(
            novarocks_execution::runtime::task_control::TaskExecutionControl::new(
                &self.admission,
                self.owner.identity(),
                self.owner.clone(),
                novarocks_execution_contract::TaskFailurePhase::Execution,
            )
            .expect("admitted preparation retains its exact Task port"),
        )
    }
    pub fn scan_sources(&self) -> TaskScanSourceIssuer {
        TaskScanSourceIssuer {
            admission: self.admission.clone(),
            execution: self.owner.identity().query_execution_id(),
            task_key: self.task_key,
        }
    }
    pub fn scan_source(
        &self,
        node_id: i32,
    ) -> Result<
        (
            novarocks_spi::connector::ConnectorRangeScope,
            novarocks_spi::connector::read_stack::ConnectorSourceOperations,
        ),
        novarocks_spi::connector::ConnectorError,
    > {
        self.scan_sources().issue(node_id)
    }
    #[cfg(test)]
    pub(crate) fn admission_for_test(
        &self,
    ) -> (
        novarocks_control_runtime::obligation::AdmissionProof,
        novarocks_types::UniqueId,
    ) {
        (self.admission.clone(), self.task_key)
    }
    /// Move the complete original capture once, before the Native builder
    /// attaches any kernel or completion callback. An error returns no partial
    /// set, and metadata views expose no capability factory.
    pub fn take_capture(&mut self) -> Result<TaskPreparationCapture, TaskPreparationCaptureTaken> {
        self.preparation_capture
            .take()
            .ok_or(TaskPreparationCaptureTaken)
    }
}
/// An opaque, Task-specific source issuer. It carries signed placement facts,
/// never general work-derivation authority or a capacity grant.
#[derive(Clone)]
pub struct TaskScanSourceIssuer {
    admission: novarocks_control_runtime::obligation::AdmissionProof,
    execution: novarocks_types::QueryExecutionId,
    task_key: novarocks_types::UniqueId,
}
impl TaskScanSourceIssuer {
    /// Worker supplies every identity component from its original Task record;
    /// the execution host supplies only the scan node from the compiled program.
    pub fn issue(
        &self,
        node_id: i32,
    ) -> Result<
        (
            novarocks_spi::connector::ConnectorRangeScope,
            novarocks_spi::connector::read_stack::ConnectorSourceOperations,
        ),
        novarocks_spi::connector::ConnectorError,
    > {
        let range = novarocks_spi::connector::ConnectorRangeScope::admitted(
            &self.admission,
            self.execution.query_id().high(),
            self.execution.query_id().low(),
            self.execution.attempt_id().get(),
            self.task_key.high(),
            self.task_key.low(),
            node_id,
        )?;
        Ok((
            range,
            novarocks_spi::connector::read_stack::ConnectorSourceOperations::root(&self.admission),
        ))
    }
    #[cfg(any(test, novarocks_dev_controls))]
    pub fn for_test(
        execution: novarocks_types::QueryExecutionId,
        task_key: novarocks_types::UniqueId,
    ) -> Self {
        use novarocks_control_runtime::transaction::RuntimeLedger;
        use novarocks_control_runtime_core::{
            capacity::ReservationResources,
            identity::Generation,
            scope::{WorkAdmission, WorkKind},
        };
        let ledger = RuntimeLedger::new(Default::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut work, mut capacity) = scope
            .reserve_next(
                generation,
                ReservationResources::work_only(),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let issuer = Self {
            admission: work.child_authority().unwrap().verify_live().unwrap(),
            execution,
            task_key,
        };
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        ledger.reap_scope_ready(scope.id(), 1).unwrap();
        capacity.finish().unwrap();
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
        issuer
    }
}

#[derive(Debug)]
pub struct TaskPreparationCaptureTaken;
impl fmt::Display for TaskPreparationCaptureTaken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the original Task preparation capture was already transferred")
    }
}
impl std::error::Error for TaskPreparationCaptureTaken {}
impl FailureProjection for TaskPreparationCaptureTaken {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
        )
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
pub(crate) struct PreparationOwnerUnavailable;
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
        let control = owner
            .zip(view.admission.as_ref())
            .and_then(|(owner, admission)| {
                novarocks_execution::runtime::task_control::TaskExecutionControl::new(
                    admission,
                    view.identity,
                    owner,
                    novarocks_execution_contract::TaskFailurePhase::Preparation,
                )
                .ok()
            });
        Self {
            control,
            cadence,
            _borrow: std::marker::PhantomData,
        }
    }
    pub fn checkpoint(&self) -> novarocks_execution::runtime::task::ExecutionResult<()> {
        let Some(control) = &self.control else {
            return Err(
                novarocks_execution::runtime::task::ExecutionFailure::from_domain(
                    PreparationOwnerUnavailable,
                ),
            );
        };
        if control.is_stopped() {
            // A successful conclusion is not permission to start new work.
            return Err(control.error().unwrap_or_else(|| {
                novarocks_execution::runtime::task::ExecutionFailure::from_domain(
                    PreparationOwnerUnavailable,
                )
            }));
        }
        Ok(())
    }
    pub fn wait(&self) -> novarocks_execution::runtime::task::ExecutionResult<()> {
        self.checkpoint()?;
        let control = self
            .control
            .as_ref()
            .expect("checkpoint qualified the live Task");
        control.wait_until_stopped(self.cadence);
        self.checkpoint()
    }
}

/// Pure preparation facts and the exact runtime owner created alongside them.
/// The participation's creation transaction takes the optional root atomically;
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
/// participation reaches `Active` only once all four are materialized, so a host
/// never publishes a catalog binding a query's credential cannot yet read.
pub struct SharedFactsRequest<'a> {
    participation: ParticipationRef,
    catalog_binding: &'a Arc<dyn CodecOwnedContent>,
    initial_runtime_filter: &'a Arc<dyn CodecOwnedContent>,
    query_options: &'a Arc<dyn CodecOwnedContent>,
    initial_credential: &'a CredentialUpdate,
}

impl<'a> SharedFactsRequest<'a> {
    pub const fn new(
        participation: ParticipationRef,
        catalog_binding: &'a Arc<dyn CodecOwnedContent>,
        initial_runtime_filter: &'a Arc<dyn CodecOwnedContent>,
        query_options: &'a Arc<dyn CodecOwnedContent>,
        initial_credential: &'a CredentialUpdate,
    ) -> Self {
        Self {
            participation,
            catalog_binding,
            initial_runtime_filter,
            query_options,
            initial_credential,
        }
    }

    pub const fn participation(&self) -> ParticipationRef {
        self.participation
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
}
#[cfg(novarocks_dev_controls)]
impl fmt::Display for TestUpdateConsumptionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedHost => f.write_str(
                "this execution host does not support a controlled split-consumption gate",
            ),
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
            },
        )
    }
}

/// A port refusal retaining its actual local domain source. The legacy
/// category/detail are this operation's response projection, never a Task
/// conclusion and never an authority that can reclassify the domain facts.
/// The pure Participation core accepts Task proposals with explicit scope and phase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostRejection {
    response_category: TaskFailureCategory,
    detail: SafeDetail,
    fact: novarocks_execution_contract::failure::FailureFact,
    source: novarocks_execution::runtime::task::ExecutionFailure,
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
            novarocks_execution::runtime::task::ExecutionFailure::from_domain(source),
        )
    }
    pub fn from_failure(
        category: TaskFailureCategory,
        source: novarocks_execution::runtime::task::ExecutionFailure,
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
        let source = novarocks_execution::runtime::task::ExecutionFailure::from_domain(source);
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
    pub fn into_failure(self) -> novarocks_execution::runtime::task::ExecutionFailure {
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

/// Immutable codec facts with no host object or foreign destructor.
/// The producing adapter owns the exact domain tag, canonical fingerprint and
/// encoding. Retaining or dropping this value invokes no codec or host callback.
#[derive(Clone)]
pub struct FrozenCodecContent {
    domain_tag: &'static [u8],
    fingerprint: novarocks_execution_contract::ContentFingerprint,
    bytes: Arc<[u8]>,
}
impl FrozenCodecContent {
    pub fn new(
        domain_tag: &'static [u8],
        fingerprint: novarocks_execution_contract::ContentFingerprint,
        bytes: Arc<[u8]>,
    ) -> Self {
        Self {
            domain_tag,
            fingerprint,
            bytes,
        }
    }
    pub const fn domain_tag(&self) -> &'static [u8] {
        self.domain_tag
    }
    pub const fn fingerprint(&self) -> novarocks_execution_contract::ContentFingerprint {
        self.fingerprint
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn encoded_len(&self) -> usize {
        self.bytes.len()
    }
}
impl fmt::Debug for FrozenCodecContent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FrozenCodecContent")
            .field("fingerprint", &self.fingerprint)
            .field("encoded_len", &self.bytes.len())
            .finish()
    }
}

/// The frozen terminal evidence one participation release sealed.
///
/// Worker retains only plain immutable codec facts after the host has exited.
/// The producing role-local adapter validates the exact domain and decodes the
/// original bytes for the matching release acknowledgement.
#[derive(Clone)]
pub struct ReleasedParticipationEvidence {
    runtime_filter: Option<FrozenCodecContent>,
    runtime_filter_observation: RuntimeFilterReleaseObservation,
}

impl Default for ReleasedParticipationEvidence {
    fn default() -> Self {
        Self::none()
    }
}

impl fmt::Debug for ReleasedParticipationEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReleasedParticipationEvidence")
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

impl ReleasedParticipationEvidence {
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
        runtime_filter: FrozenCodecContent,
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

    pub fn runtime_filter(&self) -> Option<&FrozenCodecContent> {
        self.runtime_filter.as_ref()
    }

    pub const fn runtime_filter_observation(&self) -> RuntimeFilterReleaseObservation {
        self.runtime_filter_observation
    }
}

/// The participation side of execution.
///
/// `materialize` runs while the participation is `Establishing`, outside the owner's
/// lock and already racing the sequence-zero lease. Whatever it installs must
/// be undone by `release`, because an establish that loses that race rolls
/// back rather than reaching `Active` late.
pub trait ParticipationHost: Send + Sync {
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection>;

    /// Undoes everything `materialize` installed. Called for a rollback and
    /// for a normal release, so it must be idempotent.
    ///
    /// Returns the terminal evidence the tear-down sealed. A release is the
    /// only point at which the runtime-filter observation is complete -- every
    /// local task is a terminal record and nothing further will be observed --
    /// so the seal happens here and its result is handed back rather than left
    /// inside the host for a later reader to go looking for.
    fn release(&self, participation: ParticipationRef) -> ReleasedParticipationEvidence;

    /// Applies a shared-domain advance the owner already classified as
    /// applicable.
    fn advance_shared_domain(
        &self,
        participation: ParticipationRef,
        domain: &novarocks_execution_contract::task_execution::operation::ParticipationDomainUpdate,
    ) -> Result<(), HostRejection>;
}

/// Allocates one actual host instance inside an admitted materialization job.
/// The caller publishes the returned capability to its Participation owner before
/// invoking materialization. Release and updates must use that same capability.
/// Allocation does not establish the Participation or publish a protocol result.
/// The factory allocates metadata only; I/O belongs to `materialize`. An error
/// must leave no new unpublished instance behind.
pub trait ParticipationHostFactory: Send + Sync {
    fn create_instance(
        self: Arc<Self>,
        participation: ParticipationRef,
    ) -> Result<Arc<dyn ParticipationHostInstance>, HostRejection>;
}

/// Read-only stop observation borrowed from the original admitted Participation Scope.
/// It grants no release, completion, or resource-retirement authority.
pub trait ParticipationMaterializationControl: Sync {
    fn is_stopped(&self) -> bool;
}

/// The host resources of one Participation incarnation. No operation resolves a
/// Participation key again: a delayed job retains its original instance, never a
/// later occupant of the directory. The owning scope must retain this
/// capability until all admitted calls and their reports have actually exited.
pub trait ParticipationHostInstance: Send + Sync {
    fn participation(&self) -> ParticipationRef;

    /// Observe the same Scope's committed stop while installing shared facts.
    /// Already started calls retain their original custody until actual exit.
    fn materialize(
        &self,
        request: SharedFactsRequest<'_>,
        control: &dyn ParticipationMaterializationControl,
    ) -> Result<(), HostRejection>;

    fn release(&self) -> ReleasedParticipationEvidence;

    fn advance_shared_domain(
        &self,
        domain: &novarocks_execution_contract::task_execution::operation::ParticipationDomainUpdate,
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

    /// Stop normally after the participation admission fence while retaining
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
/// The original Core's receiver retirement decision. Preserving normal close
/// requires the receiver owner's checked facts; it is never a removal fallback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskIngressRetirement {
    Remove,
    PreserveNormalClose,
}

pub trait TaskExecutionHost: Send + Sync {
    /// Closes data-plane admission for every task of this exact query
    /// execution. The registry calls this while it linearizes participation
    /// termination, before any per-task capability can be withdrawn.
    fn close_participation_admission(&self, participation: ParticipationRef);

    /// An abort revokes any earlier normal-close answers for this execution.
    fn abort_participation_admission(&self, participation: ParticipationRef) {
        self.close_participation_admission(participation);
    }
    /// Releases an idle execution context after every task has physically
    /// converged and no new task can be admitted for this exact attempt.
    fn retire_participation_execution(&self, participation: ParticipationRef);

    /// Reclaims the compact participation fence after the registry has forgotten
    /// the participation itself. No task capability for the execution may remain.
    fn forget_participation_admission(&self, participation: ParticipationRef);

    /// Whether this participation retains exact normal-close facts until its
    /// request horizon. This observation grants no resource capacity.
    fn retains_normal_close(&self, _participation: ParticipationRef) -> bool {
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

    /// Retire the exact receiver only after its owner checks the frozen mode.
    /// A refusal preserves original Worker cleanup custody and is not success.
    fn retire_receiver_checked(
        &self,
        descriptor: &TaskDescriptor,
        retirement: TaskIngressRetirement,
    ) -> Result<(), HostRejection>;

    fn install_inbound_capability(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection>;

    fn remove_inbound_capability(&self, descriptor: &TaskDescriptor);

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
    fn frozen_operation_snapshot_never_reprojects_the_original_source() {
        use novarocks_execution::runtime::task::{ExecutionFailure, PipelineOperation};
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
                    SafeDetail::new("producer checked participation").unwrap(),
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
                "producer checked participation"
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
            assert_eq!(
                projections.load(Ordering::Acquire),
                1,
                "all snapshots use the already frozen fact"
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
            novarocks_execution::runtime::task::ExecutionFailure::from_domain(ActualDomain {
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
