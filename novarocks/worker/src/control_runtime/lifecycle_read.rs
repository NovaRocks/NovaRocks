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

//! A Task observation changes read authority once, from its actual live port
//! to the final neutral facts. Readers never retain a rich Task projection.

use crate::task_port::{TaskNeutralObservation, TaskRuntimePort};
use novarocks_control_runtime::{
    lease::WorkLease,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::identity::WorkId;
use novarocks_execution_contract::TaskIdentity;
use std::sync::{Arc, Mutex, Weak};

pub(crate) type TaskReadSnapshot = TaskNeutralObservation;
#[derive(Debug)]
pub enum TaskReadError {
    OwnerMissing,
    WorkStillLive,
    ProtocolStillLive,
    SourcesStillHeld,
    ExecutionStillBorrowable,
    ConflictingSeal,
    HostInventoryStillLive,
    Runtime(RuntimeError),
}

/// A read-only binding outlives executable Work and may be retained by context
/// feedback/statistics observers. Its live owner is weak; its sealed value is
/// neutral and owns neither source, runtime, callback nor capacity authority.
pub(crate) struct TaskReadBinding {
    identity: TaskIdentity,
    owner: Weak<TaskRuntimePort>,
    sealed: Mutex<Option<Arc<TaskReadSnapshot>>>,
}
impl TaskReadBinding {
    pub(crate) const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    fn sealed(&self) -> Option<Arc<TaskReadSnapshot>> {
        self.sealed
            .lock()
            .expect("Task neutral read authority")
            .clone()
    }
    pub(crate) fn snapshot(&self) -> Result<Arc<TaskReadSnapshot>, TaskReadError> {
        if let Some(sealed) = self.sealed() {
            return Ok(sealed);
        }
        if let Some(owner) = self.owner.upgrade() {
            // No rich Arc escapes the publication borrow. Only known neutral
            // fields are copied, and no read-authority lock encloses port Drop.
            // A reader that borrowed before sealing may observe its real live
            // snapshot; readers after sealing always use the sealed authority.
            return Ok(Arc::new(owner.observe_neutral()));
        }
        // Sealing can race the failed weak upgrade. Never fabricate Created,
        // terminal or convergence facts when no authority actually exists.
        self.sealed().ok_or(TaskReadError::OwnerMissing)
    }
}

/// The exact original Task lifetime Work, kept with its observation issuer.
/// A foreign Work with numerically equal IDs cannot authorize the read handoff.
pub(crate) struct TaskLifetime {
    work: WorkLease,
    read: Arc<TaskReadBinding>,
}
impl TaskLifetime {
    pub(crate) fn from_admitted(work: WorkLease, port: &Arc<TaskRuntimePort>) -> Self {
        Self {
            work,
            read: Arc::new(TaskReadBinding {
                identity: port.identity(),
                owner: Arc::downgrade(port),
                sealed: Mutex::new(None),
            }),
        }
    }
    pub(crate) fn observation(&self) -> Arc<TaskReadBinding> {
        self.read.clone()
    }
    pub(crate) fn retirement_observer(
        &self,
    ) -> novarocks_control_runtime::lease::WorkRetirementObserver {
        self.work.retirement_observer()
    }
    pub(crate) const fn id(&self) -> WorkId {
        self.work.id()
    }
    pub(crate) fn scope(&self) -> &ScopeHandle {
        self.work.scope()
    }
    pub(crate) fn body_exited(&mut self) -> Result<(), RuntimeError> {
        self.work.body_exited()
    }
    pub(crate) fn captures_exited(&mut self) -> Result<(), RuntimeError> {
        self.work.captures_exited()
    }
    pub(crate) fn actual_exit_ready(&self) -> Result<bool, RuntimeError> {
        self.work.actual_exit_ready()
    }

    /// Necessary Work/projection gates only. The actual host inventory caller
    /// must first retire its causal inputs and all independent report legs.
    /// This method grants no physical exit and disposes no rich Task state.
    pub(super) fn seal_after_exit(&self) -> Result<Arc<TaskReadSnapshot>, TaskReadError> {
        if let Some(sealed) = self.read.sealed() {
            return Ok(sealed);
        }
        if !self
            .work
            .actual_exit_ready()
            .map_err(TaskReadError::Runtime)?
        {
            return Err(TaskReadError::WorkStillLive);
        }
        let owner = self
            .read
            .owner
            .upgrade()
            .ok_or(TaskReadError::OwnerMissing)?;
        let failed = self
            .work
            .scope()
            .gates()
            .map_err(TaskReadError::Runtime)?
            .failed;
        let actual = owner.observe_neutral();
        if !actual.convergence.retired() && !failed {
            return Err(TaskReadError::ProtocolStillLive);
        }
        // A failed Core may never author Retired. Actual host inventory and
        // Work exit still gate sealing; preserve its last neutral facts rather
        // than manufacturing convergence merely because failure occurred.
        let source_held = owner.publication().borrow().has_rich_source();
        if source_held || owner.ingress().retained_sources() != 0 {
            return Err(TaskReadError::SourcesStillHeld);
        }
        // Closing execution borrowing is a distinct earlier cleanup boundary.
        // Merely observing terminal status cannot stand in for that close.
        owner
            .prepare_source_retirement()
            .map_err(|_| TaskReadError::ExecutionStillBorrowable)?;
        let next = Arc::new(actual);
        let mut authority = self
            .read
            .sealed
            .lock()
            .expect("Task neutral read authority");
        match authority.as_ref() {
            Some(previous) if **previous != *next => Err(TaskReadError::ConflictingSeal),
            Some(previous) => Ok(previous.clone()),
            None => {
                *authority = Some(next.clone());
                Ok(next)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use novarocks_worker_protocol::context::ContextCommand;
    mod reporter;
    use super::super::actors::{WorkerAction, WorkerActor};
    use super::super::task_publication::commit_task_projection;
    use super::*;
    use crate::{
        control_admission::{AdmissionStorage, Dimension},
        control_runtime::{
            actors::{ContextActor, ContextMessage, RequestMetadata, WorkerMessage},
            effect_dispatch::EffectDispatch,
            task::{TaskHostBinding, TaskHostRecord, TaskHostResources},
        },
        task_port::TaskPendingNotice,
        task_proposal_ingress::{PairRoute, TaskProposalIngress},
    };
    use novarocks_control_runtime::driver::{ActorRuntime, EffectTicket, InputPhase};
    use novarocks_control_runtime::{
        actor::ActorConfig,
        lease::{CapacityBundle, WorkLease},
        transaction::{RuntimeLedger, ScopeHandle},
    };
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        completion::CompletionError,
        effect::EffectState,
        identity::{ActorId, ConsumerId, Generation, InputId, ReservationId, Tick},
        mailbox::InputClass,
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_execution::runtime::execution_health::ExecutionHealth;
    use novarocks_execution_contract::{
        QueryContextRef, SafeDetail, TaskConclusionProposal, TaskFailurePhase, TaskIdentity,
        TaskOperationId,
        failure::{FailureClass, FailureFact, FailureProjection},
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_worker_protocol::context::{ContextEffect, PhysicalFact};
    use novarocks_worker_protocol::{
        context::{ContextConfig, ContextCore, ContextInput},
        creation::{CreationCost, PayloadId},
        lease::{LeaseBounds, MonotonicInstant},
        proposal_summary::{Diagnostic, ProposalSummary, SourceId},
    };
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    struct Pending;
    impl TaskPendingNotice for Pending {
        fn pending(&self) {}
    }
    fn actor() -> ActorId {
        ActorId::new(1).unwrap()
    }
    fn generation() -> Generation {
        Generation::new(1).unwrap()
    }
    fn operation(n: u8) -> TaskOperationId {
        let mut bytes = [n; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        TaskOperationId::try_from_bytes(bytes).unwrap()
    }
    struct Fixture {
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        runtime: ActorRuntime<WorkerActor>,
        port: Option<Arc<TaskRuntimePort>>,
        host: Option<TaskHostRecord>,
        host_port: Option<Arc<TaskRuntimePort>>,
        causal: Option<super::super::causal::TaskCausalInbox>,
        sources_retired: bool,
        context: QueryContextRef,
        // Exact SourceIds from the actual prefix, observed Committed before
        // its input record is retired. No failed-owner receipt is fabricated.
        core_consumed: Mutex<BTreeMap<SourceId, InputId>>,
        roles: Vec<(WorkLease, CapacityBundle)>,
        jobs: BTreeMap<ReservationId, (WorkLease, CapacityBundle)>,
        dispatch: EffectDispatch,
    }
    impl Fixture {
        fn new() -> Self {
            let ledger = RuntimeLedger::new(
                ResourceVector::new([
                    (Dimension::ActiveTasks.id(), 1),
                    (Dimension::PreparingTasks.id(), 1),
                    (Dimension::PreparingBytes.id(), 100),
                    (Dimension::InboundRecords.id(), 1),
                    (Dimension::InboundBytes.id(), 100),
                ])
                .unwrap(),
                64,
                1,
            );
            let scope = ledger.open_scope_next(None, generation(), 64, 64).unwrap();
            let mut roles = Vec::new();
            for kind in [WorkKind::Cleanup, WorkKind::Step, WorkKind::Step] {
                let (mut work, capacity) = scope
                    .reserve_next(
                        generation(),
                        ResourceVector::default(),
                        kind,
                        WorkAdmission::External,
                    )
                    .unwrap()
                    .bind(kind)
                    .unwrap();
                if kind != WorkKind::Cleanup {
                    work.start().unwrap();
                }
                roles.push((work, capacity));
            }
            let context = QueryContextRef::new(
                QueryExecutionId::new(QueryId::new(551, 552), AttemptId::new(1).unwrap()).unwrap(),
                FrontendProcessId::new_v7(),
                BackendProcessId::new_v7(),
            );
            let task = TaskIdentity::new(
                context.query_execution_id(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                context.backend_process_id(),
            );
            let config = ContextConfig {
                max_tasks: 1,
                max_creation_waiters: 2,
                max_pending_requests: 4,
                max_preparing: 1,
                max_task_bytes: 100,
                step_budget: ContextCore::MAX_STEP_WORK,
                lease_bounds: LeaseBounds::DEFAULT,
                metric_publish_interval: Duration::from_millis(100),
            };
            let core = ContextCore::new(
                context,
                generation(),
                crate::control_runtime::actors::tests::initial_establishment(),
                config,
            )
            .unwrap();
            let runtime = ActorRuntime::new(1);
            runtime
                .register_supervised_next(
                    actor(),
                    scope.clone(),
                    WorkerActor::Context(Box::new(ContextActor::new(
                        core,
                        scope.clone(),
                        AdmissionStorage {
                            loading: ResourceVector::default(),
                            accepted: ResourceVector::default(),
                            context: ResourceVector::default(),
                            ticket: ResourceVector::default(),
                        },
                        roles[1].0.id(),
                        roles[2].0.id(),
                    ))),
                    ActorConfig {
                        mailbox: [16, 16, 16, 1],
                        max_batches: 49,
                        max_effects_per_step:
                            crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
                        max_dependencies_per_effect:
                            crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
                        turn_budget: crate::control_runtime::actors::ContextActor::TURN_BUDGET,
                        max_step_work: crate::control_runtime::actors::ContextActor::MAX_STEP_WORK,
                    },
                    &roles[0].0,
                )
                .unwrap_or_else(|(error, _)| panic!("actual actor admission: {error:?}"));
            let mut f = Self {
                ledger,
                scope,
                runtime,
                port: None,
                host: None,
                host_port: None,
                causal: None,
                sources_retired: false,
                context,
                core_consumed: Mutex::new(BTreeMap::new()),
                roles,
                jobs: BTreeMap::new(),
                dispatch: EffectDispatch::new(actor(), 64),
            };
            for (command, request) in [
                (
                    ContextInput::MaterializationFinished {
                        result: Ok(crate::control_runtime::actors::tests::initial_materialized()),
                    },
                    None,
                ),
                (
                    ContextInput::Request(ContextCommand::Create {
                        task,
                        payload: PayloadId(1),
                    }),
                    Some(operation(1)),
                ),
                (
                    ContextInput::Loaded {
                        task,
                        result: Ok(CreationCost {
                            input_retained_bytes: 1,
                            ingress_retained_bytes: 1,
                            structural_bytes: 1,
                            inbound_records: 0,
                            inbound_bytes: 0,
                        }),
                    },
                    None,
                ),
                (ContextInput::Installed { task }, None),
            ] {
                let input = f.send(command, request);
                f.drain();
                f.runtime.retire_input(actor(), input).unwrap();
            }
            f
        }
        fn port(&self) -> &Arc<TaskRuntimePort> {
            self.port
                .as_ref()
                .expect("the actual LoadInput elected this Task")
        }
        fn send(&self, command: ContextInput, request: Option<TaskOperationId>) -> InputId {
            let sources: Vec<_> = match &command {
                ContextInput::Summary { summary, .. } => summary.candidate_sources().collect(),
                _ => Vec::new(),
            };
            let input = self
                .runtime
                .reserve_input(actor(), ActorId::new(2).unwrap(), InputClass::Lifecycle)
                .unwrap()
                .fill(WorkerMessage::Context(ContextMessage::Command {
                    work: self.roles[2].0.id(),
                    command,
                    request: request.map(|operation| RequestMetadata {
                        operation,
                        deadline: MonotonicInstant::from_origin(Duration::from_secs(10)),
                    }),
                }))
                .unwrap();
            self.runtime.poll_one(Tick(1)).unwrap();
            if !sources.is_empty() {
                assert_eq!(
                    self.runtime
                        .input_custody(actor(), input)
                        .unwrap()
                        .unwrap()
                        .phase,
                    InputPhase::Committed,
                    "only the original prefix's real Core commit proves source consumption"
                );
                for source in sources {
                    assert!(
                        self.core_consumed
                            .lock()
                            .unwrap()
                            .insert(source, input)
                            .is_none()
                    );
                }
            }
            input
        }
        fn next(&mut self) -> Option<EffectTicket<WorkerAction>> {
            // This fixture has no external request observer. Its neutral reply
            // bodies complete on this host stack; acknowledge only the actual
            // runtime completion facts, never an emitted effect's position.
            for (_, input) in self.runtime.committed_batches(actor(), None, 64).unwrap() {
                let inventory = self
                    .runtime
                    .effect_inventory(actor(), input, None, 15)
                    .unwrap();
                let states: BTreeMap<_, _> = inventory.iter().map(|(d, s)| (d.id, *s)).collect();
                for (descriptor, state) in inventory {
                    if state == EffectState::Pending {
                        for predecessor in descriptor.dependencies {
                            if states[&predecessor] == EffectState::Completed {
                                self.runtime
                                    .acknowledge_dependency(
                                        actor(),
                                        input,
                                        descriptor.id,
                                        predecessor,
                                    )
                                    .unwrap();
                            }
                        }
                    }
                }
            }
            self.dispatch.index_committed(&self.runtime, 128).unwrap();
            self.dispatch.take_ready(&self.runtime, 64).unwrap()
        }
        fn commit(&mut self, ticket: &EffectTicket<WorkerAction>) {
            drop(commit_task_projection(&self.runtime, ticket, self.port()).unwrap());
            self.dispatch
                .publication_committed(&self.runtime, ticket)
                .unwrap();
        }
        fn drain(&mut self) {
            while let Some(ticket) = self.next() {
                let publication = matches!(
                    ticket.effect,
                    WorkerAction::Context(
                        ContextEffect::PublishTask { .. }
                            | ContextEffect::PublishMetrics { .. }
                            | ContextEffect::PublishTaskConvergence { .. }
                            | ContextEffect::SummaryConsumed { .. }
                            | ContextEffect::PublishConclusion { .. }
                            | ContextEffect::RetireTask { .. }
                    )
                );
                if publication {
                    self.commit(&ticket);
                }
                match &ticket.effect {
                    WorkerAction::Context(ContextEffect::LoadInput { .. }) => {
                        let host = TaskHostRecord::adopt_loading(
                            &self.runtime,
                            &ticket,
                            TaskHostBinding {
                                context: self.context,
                                task_source: ActorId::new(2).unwrap(),
                                protocol_generation: generation(),
                                max_cuts: 4,
                                source_storage_model: true,
                                pending: Arc::new(Pending),
                                health: Arc::new(ExecutionHealth::default()),
                            },
                            &self.roles[0].0,
                        )
                        .unwrap();
                        assert!(self.port.replace(host.port()).is_none());
                        assert!(self.host.replace(host).is_none());
                    }
                    WorkerAction::Context(ContextEffect::EnqueuePreparation {
                        reservation,
                        ..
                    }) => {
                        // This unit observes the Core's real preparation Work;
                        // it does not invoke a Native loader or execution host.
                        let (mut work, capacity) = self
                            .runtime
                            .take_step_reservation(&ticket, *reservation)
                            .unwrap()
                            .bind(WorkKind::Business)
                            .unwrap();
                        work.start().unwrap();
                        assert!(self.jobs.insert(*reservation, (work, capacity)).is_none());
                    }
                    _ => {}
                }
                self.runtime.effect_completed(ticket).unwrap();
            }
        }
        fn retire_source_pair(
            &self,
            ingress: &TaskProposalIngress,
            route: PairRoute,
        ) -> [super::super::source_positions::ReapedPosition; 2] {
            for unused in ingress
                .take_unused_pair(route)
                .unwrap()
                .into_iter()
                .flatten()
            {
                unused.exit();
            }
            for position in ingress.pair_inventory(route).unwrap() {
                assert!(position.custody.actual_exit_ready().unwrap());
                if let Some(source) = position.source {
                    assert!(
                        self.core_consumed.lock().unwrap().contains_key(&source),
                        "this exact source's original prefix really committed before input retirement"
                    );
                    position
                        .custody
                        .report()
                        .consume(ConsumerId::new(1).unwrap())
                        .unwrap();
                    position
                        .custody
                        .report()
                        .consume(ConsumerId::new(2).unwrap())
                        .unwrap();
                } else {
                    for consumer in [1, 2] {
                        assert_eq!(
                            position
                                .custody
                                .report()
                                .consume(ConsumerId::new(consumer).unwrap()),
                            Err(
                                novarocks_control_runtime::transaction::RuntimeError::Completion(
                                    CompletionError::NotFilled
                                )
                            ),
                            "unused holders revoke reports; neither consumer receives a result"
                        );
                    }
                }
                assert!(position.custody.try_retire().unwrap());
            }
            let headers = self.scope.snapshot().unwrap().work;
            self.ledger
                .reap_scope_ready(self.scope.id(), headers)
                .unwrap();
            ingress
                .prepare_pair_reap(route)
                .unwrap()
                .map(|probe| probe.observe_reaped().unwrap())
        }
        fn exit_jobs(&mut self) {
            for (mut work, mut capacity) in std::mem::take(&mut self.jobs).into_values() {
                work.body_exited().unwrap();
                work.captures_exited().unwrap();
                self.ledger.reap_scope_ready(self.scope.id(), 64).unwrap();
                capacity.release(&capacity.resources().unwrap()).unwrap();
            }
        }
        fn retire_sources(&mut self) {
            if self.sources_retired {
                return;
            }
            let ingress = self.port().ingress().clone();
            ingress.seal_source_admission();
            let _open = self.retire_source_pair(&ingress, PairRoute::Open);
            let consuming = self.retire_source_pair(&ingress, PairRoute::ConsumingOpen);
            if !self.core_consumed.lock().unwrap().is_empty() {
                ingress
                    .finish_open_after_reap(consuming)
                    .unwrap_or_else(|_| panic!("same committed prefix's real Source exit"));
            }
            assert_eq!(ingress.retained_sources(), 0);
            self.sources_retired = true;
        }
        fn finish(mut self) {
            assert!(
                self.host.is_none(),
                "lifetime authority moved exactly once from the real elected Task"
            );
            assert!(
                self.sources_retired,
                "the live port remains owned until actual sources and reports retire"
            );
            assert!(self.jobs.is_empty());
            let causal = self.causal.take().unwrap();
            assert!(causal.is_empty());
            drop(causal);
            drop(self.host_port.take());
            drop(self.port.take());
            for (mut work, mut capacity) in self.roles.drain(..) {
                work.body_exited().unwrap();
                work.captures_exited().unwrap();
                self.ledger.reap_scope_ready(self.scope.id(), 64).unwrap();
                capacity.release(&capacity.resources().unwrap()).unwrap();
            }
            self.scope.close_external().unwrap();
            while let Some(original_host_turn) = self.runtime.claim_host_actor() {
                original_host_turn.release(false).unwrap();
            }
            self.runtime.remove_actor(actor()).unwrap();
            self.ledger.remove_scope(self.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
        }
    }

    impl Fixture {
        fn command(&mut self, command: ContextInput) {
            let input = self.send(command, None);
            self.drain();
            self.runtime.retire_input(actor(), input).unwrap();
        }
        fn lifetime(&mut self) -> (TaskLifetime, CapacityBundle) {
            let original_work_count = self.scope.snapshot().unwrap().work;
            let TaskHostResources {
                lifetime,
                active,
                port,
                host_calls,
                disposal,
                loader,
                loading_events,
                loading_return,
                pending_loading,
                preparation,
                preparation_remainder,
                accepted_source_storage,
                pending_preparation_adoption,
                mut source_partials,
                completion,
                completion_action,
                lifecycle_inputs,
                statistics,
                dynamic_filters,
                open_summary,
                causal,
                production,
            } = self
                .host
                .take()
                .expect("one real elected lifetime")
                .into_resources();
            assert!(
                production.is_none(),
                "component fixture never created a production runtime/root owner"
            );
            assert!(lifecycle_inputs.is_empty());
            assert!(statistics.is_none());
            assert!(dynamic_filters.is_none());
            assert!(
                loader.is_none()
                    && loading_events.is_none()
                    && loading_return.is_none()
                    && pending_loading.is_none()
                    && preparation.is_none()
                    && preparation_remainder.is_none()
                    && completion.is_none()
                    && completion_action.is_none()
                    && open_summary.is_none()
            );
            assert!(accepted_source_storage.is_none());
            assert!(pending_preparation_adoption.is_none());
            for partial in &mut source_partials {
                let mut retired = false;
                for _ in 0..64 {
                    if partial.try_retire(&self.ledger).unwrap() {
                        retired = true;
                        break;
                    }
                }
                assert!(retired);
            }
            assert!(source_partials.iter().all(|partial| partial.is_empty()));
            assert!(causal.is_empty());
            assert!(self.host_port.replace(port).is_none());
            assert!(self.causal.replace(causal).is_none());
            crate::control_runtime::task::retire_unused_host_calls_for_test(
                &host_calls,
                &self.ledger,
            );
            let mut disposal_retired = false;
            for _ in 0..64 {
                if disposal.retire_unused(&self.ledger).unwrap() {
                    disposal_retired = true;
                    break;
                }
            }
            assert!(disposal_retired && disposal.is_retired());
            assert_eq!(
                self.scope.snapshot().unwrap().work,
                original_work_count - host_calls.len() - 1,
                "only the actual unused host-call and disposal headers exited while moving the lifetime"
            );
            assert_eq!(lifetime.observation().identity(), self.port().identity());
            (lifetime, active)
        }
        fn retire_task(&mut self) {
            let task = self.port().identity();
            self.command(ContextInput::CompletionIntent {
                task,
                completed_output: Some(novarocks_execution_contract::TaskOutputFacts::new(true)),
            });
            self.command(ContextInput::CompletionExited { task });
            for fact in [
                PhysicalFact::LoaderExited,
                PhysicalFact::PreparationBodyExited,
                PhysicalFact::PreparationExited,
                PhysicalFact::OutputReleased,
                PhysicalFact::RootIdle,
            ] {
                self.command(ContextInput::Physical { task, fact });
            }
            assert!(self.port().publication().read().convergence().retired());
        }
        fn publish_ingress(&mut self) {
            let input = self.port().ingress().take_next_summary().unwrap();
            self.command(ContextInput::Summary {
                task: self.port().identity(),
                generation: generation(),
                cut: input.cut,
                summary: input.summary,
            });
            self.port().ingress().summary_consumed(input.cut).unwrap();
        }
        fn detach_source(&mut self) {
            // The Core emits this existing metadata-publication role without
            // changing facts. The host only commits its actual source cleanup
            // candidate under that real runtime fence; it authors no status.
            let input = self.send(
                ContextInput::Summary {
                    task: self.port().identity(),
                    generation: generation(),
                    cut: None,
                    summary: ProposalSummary::default(),
                },
                None,
            );
            let ticket = self.next().unwrap();
            assert!(matches!(
                ticket.effect,
                WorkerAction::Context(ContextEffect::SummaryConsumed { .. })
            ));
            let candidate = self.port().prepare_source_retirement().unwrap();
            let old = self
                .runtime
                .commit_snapshot(&ticket, self.port().publication(), Arc::new(candidate))
                .unwrap_or_else(|_| {
                    panic!("the actual cleanup publication fence accepts its candidate")
                });
            drop(old);
            self.dispatch
                .publication_committed(&self.runtime, &ticket)
                .unwrap();
            self.runtime.effect_completed(ticket).unwrap();
            self.drain();
            self.runtime.retire_input(actor(), input).unwrap();
        }
    }
    fn exit_lifetime(f: &mut Fixture, lifetime: &mut TaskLifetime, capacity: &mut CapacityBundle) {
        f.port().retire_execution_port();
        f.retire_sources();
        f.exit_jobs();
        lifetime.body_exited().unwrap();
        lifetime.captures_exited().unwrap();
        assert!(lifetime.actual_exit_ready().unwrap());
        capacity.release(&capacity.resources().unwrap()).unwrap();
    }

    #[test]
    fn original_lifetime_exit_protocol_retirement_and_source_detachment_are_independent_gates() {
        let mut f = Fixture::new();
        let (mut lifetime, mut capacity) = f.lifetime();
        assert!(matches!(
            lifetime.seal_after_exit(),
            Err(TaskReadError::WorkStillLive)
        ));
        lifetime.body_exited().unwrap();
        assert!(matches!(
            lifetime.seal_after_exit(),
            Err(TaskReadError::WorkStillLive)
        ));
        lifetime.captures_exited().unwrap();
        assert!(
            !lifetime.actual_exit_ready().unwrap(),
            "the elected Task still owns its actual paid Source children"
        );
        assert!(matches!(
            lifetime.seal_after_exit(),
            Err(TaskReadError::WorkStillLive)
        ));
        f.port().submit(TaskConclusionProposal::Failed {
            cause: novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(
                novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new(
                        "original lifecycle source remains held",
                    ),
                ),
            ),
            phase: TaskFailurePhase::Execution,
        });
        f.publish_ingress();
        f.retire_task();
        assert!(f.port().publication().borrow().has_rich_source());
        assert_eq!(f.port().ingress().retained_sources(), 1);
        assert!(
            matches!(
                lifetime.seal_after_exit(),
                Err(TaskReadError::WorkStillLive)
            ),
            "the actual source role is a child of this same elected lifetime"
        );
        f.port().retire_execution_port();
        f.detach_source();
        assert!(!f.port().publication().borrow().has_rich_source());
        assert!(
            !lifetime.actual_exit_ready().unwrap(),
            "detachment is not report consumption or header reaping"
        );
        f.retire_sources();
        f.exit_jobs();
        assert!(lifetime.actual_exit_ready().unwrap());
        let sealed = lifetime.seal_after_exit().unwrap();
        assert!(sealed.convergence.retired());
        capacity.release(&capacity.resources().unwrap()).unwrap();
        f.finish();

        // A separate original Task exits its real inventory without a Core
        // Retired fact. No synthetic sibling Work is used to reach this gate.
        let mut pending = Fixture::new();
        let (mut lifetime, mut capacity) = pending.lifetime();
        exit_lifetime(&mut pending, &mut lifetime, &mut capacity);
        assert!(matches!(
            lifetime.seal_after_exit(),
            Err(TaskReadError::ProtocolStillLive)
        ));
        pending.finish();
    }

    #[test]
    fn a_sealed_observer_outlives_the_actual_port_and_scope_without_recreating_either() {
        let mut f = Fixture::new();
        let (mut lifetime, mut capacity) = f.lifetime();
        let observer = lifetime.observation();
        let weak_port = Arc::downgrade(f.port());
        let initial = observer.snapshot().unwrap();
        assert_eq!(initial.status, f.port().current());
        assert!(!initial.convergence.retired());
        let mut summary = ProposalSummary::default();
        summary.cleanup(Diagnostic {
            phase: TaskFailurePhase::Execution,
            fact: FailureFact::new(
                FailureClass::Unexpected,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            ),
            detail: SafeDetail::truncating("actual final neutral diagnostic"),
            cleanup: true,
        });
        f.command(ContextInput::Summary {
            task: f.port().identity(),
            generation: generation(),
            cut: None,
            summary,
        });
        f.retire_task();
        f.port().retire_execution_port();
        f.detach_source();
        exit_lifetime(&mut f, &mut lifetime, &mut capacity);
        let final_view = lifetime.seal_after_exit().unwrap();
        let actual = f.port().observe_neutral();
        assert_eq!(*final_view, actual);
        assert_eq!(
            final_view.status.state(),
            novarocks_execution_contract::TaskState::Finished
        );
        assert!(final_view.convergence.retired());
        assert_eq!(final_view.diagnostics.entries().len(), 1);
        f.finish();
        assert!(
            weak_port.upgrade().is_none(),
            "the observer holds no strong port or rich projection"
        );
        assert_eq!(*observer.snapshot().unwrap(), actual);
        assert!(Arc::ptr_eq(&observer.snapshot().unwrap(), &final_view));
        assert!(Arc::ptr_eq(
            &lifetime.seal_after_exit().unwrap(),
            &final_view
        ));
    }

    #[test]
    fn failed_scope_sealing_requires_the_closed_execution_gate_and_preserves_false_facts() {
        let mut f = Fixture::new();
        let (mut lifetime, mut capacity) = f.lifetime();
        let observer = lifetime.observation();
        let actual = observer.snapshot().unwrap();
        assert!(
            matches!(
                f.port().prepare_source_retirement(),
                Err(crate::task_port::ProjectionError::SourceStillBorrowed)
            ),
            "an open execution borrow gate independently refuses source retirement"
        );
        assert!(matches!(
            lifetime.seal_after_exit(),
            Err(TaskReadError::WorkStillLive)
        ));
        exit_lifetime(&mut f, &mut lifetime, &mut capacity);
        f.scope.fail().unwrap();
        let sealed = lifetime.seal_after_exit().unwrap();
        assert_eq!(
            sealed, actual,
            "failure retains actual neutral facts rather than inventing convergence"
        );
        assert!(!sealed.convergence.actual_stopped());
        assert!(!sealed.convergence.resources_converged());
        assert!(!sealed.convergence.retired());
        // This lower-level original-lifetime test signs no failure publication
        // drain or Host TaskSeal. All unit Work really exits during teardown.
        f.finish();
        assert_eq!(observer.snapshot().unwrap(), actual);
    }

    #[test]
    fn equal_ids_in_another_ledger_cannot_supply_the_original_lifetime_exit() {
        let mut f = Fixture::new();
        let (mut lifetime, mut capacity) = f.lifetime();
        let other = RuntimeLedger::new(ResourceVector::default(), 4, 1);
        let scope = other.open_scope_next(None, generation(), 4, 4).unwrap();
        let (mut foreign, mut foreign_capacity) = scope
            .reserve(
                novarocks_control_runtime_core::capacity::ReservationRequest {
                    guard: lifetime.id(),
                    id: ReservationId::new(100).unwrap(),
                    generation: generation(),
                    resources: ResourceVector::default(),
                    kind: WorkKind::Business,
                    admission: WorkAdmission::External,
                },
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        assert_eq!(scope.id(), lifetime.scope().id());
        assert_eq!(foreign.id(), lifetime.id());
        foreign.start().unwrap();
        foreign.body_exited().unwrap();
        foreign.captures_exited().unwrap();
        assert!(foreign.actual_exit_ready().unwrap());
        assert!(!lifetime.actual_exit_ready().unwrap());
        assert!(matches!(
            lifetime.seal_after_exit(),
            Err(TaskReadError::WorkStillLive)
        ));
        foreign_capacity
            .release(&foreign_capacity.resources().unwrap())
            .unwrap();
        other.reap_scope_ready(scope.id(), 4).unwrap();
        scope.close_external().unwrap();
        other.remove_scope(scope.id()).unwrap();
        assert!(other.is_empty());
        exit_lifetime(&mut f, &mut lifetime, &mut capacity);
        f.finish();
    }

    #[test]
    fn unknown_original_child_exit_cannot_be_replaced_by_scope_failure_or_parent_exit() {
        let mut f = Fixture::new();
        let (mut lifetime, capacity) = f.lifetime();
        let (mut child, child_capacity) = lifetime
            .work
            .derive(
                novarocks_control_runtime_core::capacity::ReservationRequest {
                    guard: WorkId::new(100).unwrap(),
                    id: ReservationId::new(100).unwrap(),
                    generation: generation(),
                    resources: ResourceVector::default(),
                    kind: WorkKind::Cleanup,
                    admission: WorkAdmission::Derived(lifetime.id()),
                },
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        child.start().unwrap();
        child.mark_unresolved();
        lifetime.body_exited().unwrap();
        lifetime.captures_exited().unwrap();
        f.scope.fail().unwrap();
        f.port().retire_execution_port();
        assert!(!lifetime.actual_exit_ready().unwrap());
        assert!(matches!(
            lifetime.seal_after_exit(),
            Err(TaskReadError::WorkStillLive)
        ));
        assert!(lifetime.read.sealed().is_none());
        // Unknown original holders remain retained. Test teardown cannot call
        // body/capture exit merely to make the ledger look empty.
        std::mem::forget((f, lifetime, capacity, child, child_capacity));
    }

    #[test]
    fn live_neutral_reads_do_not_retain_a_rich_source_or_drop_it_under_the_read_mutex() {
        struct OriginalSource {
            observation: Weak<TaskReadBinding>,
            drops: Arc<AtomicUsize>,
        }
        impl std::fmt::Debug for OriginalSource {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("neutral observation source lock probe")
            }
        }
        impl std::fmt::Display for OriginalSource {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("neutral observation source lock probe")
            }
        }
        impl std::error::Error for OriginalSource {}
        impl FailureProjection for OriginalSource {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::new(
                    FailureClass::Data,
                    crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
                )
            }
        }
        impl Drop for OriginalSource {
            fn drop(&mut self) {
                let observation = self.observation.upgrade().unwrap();
                assert!(
                    observation.sealed.try_lock().is_ok(),
                    "no rich Drop under the neutral read-authority mutex"
                );
                let actual = observation.snapshot().unwrap();
                assert!(actual.status.termination().is_some());
                self.drops.fetch_add(1, Ordering::AcqRel);
            }
        }
        let mut f = Fixture::new();
        let (mut lifetime, mut capacity) = f.lifetime();
        let observer = lifetime.observation();
        let drops = Arc::new(AtomicUsize::new(0));
        f.port().submit(TaskConclusionProposal::Failed {
            cause: novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(
                OriginalSource {
                    observation: Arc::downgrade(&observer),
                    drops: drops.clone(),
                },
            ),
            phase: TaskFailurePhase::Execution,
        });
        f.publish_ingress();
        let neutral_receipt = f.port().publication().borrow().neutral_accepted().cloned();
        assert!(neutral_receipt.is_some());
        let strong_before = Arc::strong_count(f.port());
        let actual = observer.snapshot().unwrap();
        assert_eq!(Arc::strong_count(f.port()), strong_before);
        assert_eq!(actual.status, f.port().current());
        assert_eq!(drops.load(Ordering::Acquire), 0);
        f.port().retire_execution_port();
        f.detach_source();
        assert_eq!(
            f.port().publication().borrow().neutral_accepted(),
            neutral_receipt.as_ref(),
            "source detachment preserves the actual accepted generation watermark"
        );
        assert_eq!(
            drops.load(Ordering::Acquire),
            1,
            "retaining the neutral snapshot cannot retain the original source"
        );
        assert_eq!(actual.status, observer.snapshot().unwrap().status);
        exit_lifetime(&mut f, &mut lifetime, &mut capacity);
        f.finish();
    }

    #[test]
    fn a_borrowed_neutral_copy_cannot_move_the_last_source_drop_to_the_reader() {
        struct Source {
            drops: Arc<AtomicUsize>,
            disposal_thread: Arc<Mutex<Option<std::thread::ThreadId>>>,
        }
        impl std::fmt::Debug for Source {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("borrowed neutral-copy source")
            }
        }
        impl std::fmt::Display for Source {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("borrowed neutral-copy source")
            }
        }
        impl std::error::Error for Source {}
        impl FailureProjection for Source {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::new(
                    FailureClass::Data,
                    crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
                )
            }
        }
        impl Drop for Source {
            fn drop(&mut self) {
                *self.disposal_thread.lock().unwrap() = Some(std::thread::current().id());
                self.drops.fetch_add(1, Ordering::Release);
            }
        }
        let mut f = Fixture::new();
        let (mut lifetime, mut capacity) = f.lifetime();
        let observation = lifetime.observation();
        let drops = Arc::new(AtomicUsize::new(0));
        let disposal_thread = Arc::new(Mutex::new(None));
        f.port().submit(TaskConclusionProposal::Failed {
            cause: novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(Source {
                drops: drops.clone(),
                disposal_thread: disposal_thread.clone(),
            }),
            phase: TaskFailurePhase::Execution,
        });
        f.publish_ingress();
        f.port().retire_execution_port();
        let port = f.port().clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (copied_tx, copied_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            // Pause at the exact primitive used by observe_neutral: no rich
            // projection Arc is cloned. The source cannot be detached until
            // this actual borrow has released the publication value mutex.
            let borrowed = port.publication().borrow();
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            let neutral = borrowed.neutral_observation();
            drop(borrowed);
            let observed = observation.snapshot().unwrap();
            assert_eq!(*observed, neutral);
            copied_tx.send(()).unwrap();
            finish_rx.recv().unwrap();
            // Keep both real neutral results alive across source cleanup.
            assert_eq!(*observed, neutral);
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let (copied, completed, disposal_owner) = std::thread::scope(|scope| {
            let cleanup = scope.spawn(|| {
                started_tx.send(std::thread::current().id()).unwrap();
                f.detach_source();
                done_tx.send(()).unwrap();
            });
            let owner = started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let blocked = done_rx.recv_timeout(Duration::from_millis(20));
            release_tx.send(()).unwrap();
            let copied = copied_rx.recv_timeout(Duration::from_secs(5));
            let completed = done_rx.recv_timeout(Duration::from_secs(5));
            // Always release the reader before assertions/joins, including
            // the timeout failure path of this controlled regression.
            finish_tx.send(()).unwrap();
            cleanup.join().unwrap();
            assert!(
                blocked.is_err(),
                "the real publication cannot overtake its borrowed neutral copy"
            );
            (copied, completed, owner)
        });
        reader.join().unwrap();
        assert!(copied.is_ok() && completed.is_ok());
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert_eq!(
            *disposal_thread.lock().unwrap(),
            Some(disposal_owner),
            "the original source exits on its host cleanup stack, never the metadata reader"
        );
        exit_lifetime(&mut f, &mut lifetime, &mut capacity);
        f.finish();
    }

    #[test]
    fn missing_live_owner_is_an_error_and_never_a_created_fallback() {
        let mut f = Fixture::new();
        let (mut lifetime, mut capacity) = f.lifetime();
        let observer = lifetime.observation();
        exit_lifetime(&mut f, &mut lifetime, &mut capacity);
        f.finish();
        assert!(matches!(
            observer.snapshot(),
            Err(TaskReadError::OwnerMissing)
        ));
    }
}
