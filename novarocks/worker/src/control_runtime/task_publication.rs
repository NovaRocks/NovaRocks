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

//! Actual Task publication commits one existing snapshot cell. A failed fence
//! returns the original candidate to its host custodian; it supplies no source,
//! body, capture, report or capacity exit proof.

use super::actors::{WorkerAction, WorkerActor};
use crate::task_port::{ProjectionError, TaskProjection, TaskRuntimePort};
use novarocks_control_runtime::driver::{ActorRuntime, DriverError, EffectTicket};
use novarocks_execution::runtime::fragment::ExecutionFailure;
use novarocks_execution_contract::{AcceptedTaskConclusion, RetainedTaskSource};
use novarocks_worker_protocol::context::ContextEffect;
use std::{num::NonZeroU64, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskPublicationError {
    WrongEffect,
    WrongTask,
    Projection(ProjectionError),
    Runtime(DriverError),
}

/// The started host job retains these exact values on failure. Neither a
/// refused publication nor dropping its RPC observer authorizes their disposal.
pub(crate) struct TaskPublicationRefusal {
    pub error: TaskPublicationError,
    pub projection: Option<Arc<TaskProjection>>,
    pub conclusion: Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>>,
    pub source: Option<RetainedTaskSource<ExecutionFailure>>,
}
impl std::fmt::Debug for TaskPublicationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskPublicationRefusal")
            .field("error", &self.error)
            .field("retains_projection", &self.projection.is_some())
            .field("retains_conclusion", &self.conclusion.is_some())
            .field("retains_source", &self.source.is_some())
            .finish()
    }
}
impl TaskPublicationRefusal {
    fn metadata(error: TaskPublicationError) -> Self {
        Self {
            error,
            projection: None,
            conclusion: None,
            source: None,
        }
    }
}

/// This performs the real runtime-fenced swap. The caller must separately
/// release its Task publication lane from that actual commit, then retire the
/// original effect only after its own body/captures have actually exited.
/// The replaced projection is returned for destruction outside runtime locks.
pub(crate) fn commit_task_projection(
    runtime: &ActorRuntime<WorkerActor>,
    ticket: &EffectTicket<WorkerAction>,
    port: &TaskRuntimePort,
) -> Result<Arc<TaskProjection>, TaskPublicationRefusal> {
    let WorkerAction::Context(effect) = &ticket.effect else {
        return Err(TaskPublicationRefusal::metadata(
            TaskPublicationError::WrongEffect,
        ));
    };
    let task = match effect {
        ContextEffect::PublishTask { task, .. }
        | ContextEffect::PublishMetrics { task, .. }
        | ContextEffect::PublishTaskConvergence { task, .. }
        | ContextEffect::SummaryConsumed { task, .. }
        | ContextEffect::PublishConclusion { task, .. }
        | ContextEffect::RetireTask { task, .. } => *task,
        _ => {
            return Err(TaskPublicationRefusal::metadata(
                TaskPublicationError::WrongEffect,
            ));
        }
    };
    if task != port.identity() {
        return Err(TaskPublicationRefusal::metadata(
            TaskPublicationError::WrongTask,
        ));
    }
    let current = port.publication().read();
    let projection = match effect {
        ContextEffect::PublishTask {
            snapshot,
            convergence,
            ..
        } => current
            .with_status(snapshot.clone())
            .and_then(|next| next.with_convergence(*convergence)),
        ContextEffect::PublishMetrics { snapshot, .. } => current.with_status(snapshot.clone()),
        ContextEffect::PublishTaskConvergence { snapshot, .. } => {
            current.with_convergence(*snapshot)
        }
        ContextEffect::SummaryConsumed { diagnostics, .. } => current.with_diagnostics(diagnostics),
        ContextEffect::RetireTask { convergence, .. } => current.with_convergence(*convergence),
        ContextEffect::PublishConclusion {
            generation,
            conclusion,
            source,
            ..
        } => {
            let Some(generation) = NonZeroU64::new(*generation) else {
                return Err(TaskPublicationRefusal::metadata(
                    TaskPublicationError::Projection(ProjectionError::ConclusionGeneration),
                ));
            };
            // Reject a duplicate/older effect before transferring its source.
            if current
                .neutral_accepted()
                .is_some_and(|accepted| accepted.generation().get() >= generation.get())
            {
                return Err(TaskPublicationRefusal::metadata(
                    TaskPublicationError::Projection(ProjectionError::ConclusionGeneration),
                ));
            }
            let accepted = port
                .prepare_conclusion(generation, conclusion.clone(), *source)
                .map_err(|refusal| TaskPublicationRefusal {
                    error: TaskPublicationError::Projection(refusal.error),
                    projection: None,
                    conclusion: None,
                    source: refusal.source,
                })?;
            match current.with_conclusion(accepted.clone()) {
                Ok(next) => Ok(next),
                Err(error) => {
                    return Err(TaskPublicationRefusal {
                        error: TaskPublicationError::Projection(error),
                        projection: None,
                        conclusion: Some(accepted),
                        source: None,
                    });
                }
            }
        }
        _ => unreachable!("only actual Task publication effects are qualified"),
    }
    .map_err(|error| TaskPublicationRefusal::metadata(TaskPublicationError::Projection(error)))?;
    runtime
        .commit_snapshot(ticket, port.publication(), Arc::new(projection))
        .map_err(|(error, original)| TaskPublicationRefusal {
            error: TaskPublicationError::Runtime(error),
            projection: Some(original),
            conclusion: None,
            source: None,
        })
}

#[cfg(test)]
mod tests {
    //! Real Core effects and the production Task lane are the input oracle.
    //! These tests cover publication custody, not Native host body execution.
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
    use novarocks_control_runtime::{
        actor::ActorConfig,
        driver::InputPhase,
        lease::{ReservationBundle, WorkLease},
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
        CancelReason, QueryContextRef, SafeDetail, TaskConclusionProposal, TaskFailurePhase,
        TaskIdentity, TaskOperationId,
        failure::{FailureClass, FailureFact, FailureProjection},
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_worker_protocol::context::ContextCommand;
    use novarocks_worker_protocol::{
        context::{ContextConfig, ContextCore, ContextInput},
        creation::{CreationCost, PayloadId},
        lease::{LeaseBounds, MonotonicInstant},
        proposal_summary::{Diagnostic, ProposalSummary, SourceId},
    };
    use std::{
        collections::BTreeMap,
        sync::{
            Mutex, Weak,
            atomic::{AtomicUsize, Ordering},
        },
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
        registration: novarocks_control_runtime::driver::ActorRegistration,
        port: Option<Arc<TaskRuntimePort>>,
        host: Option<TaskHostRecord>,
        context: QueryContextRef,
        // Exact SourceIds from the actual prefix, observed Committed before
        // its input record is retired. No failed-owner receipt is fabricated.
        core_consumed: Mutex<BTreeMap<SourceId, InputId>>,
        roles: Vec<(WorkLease, ReservationBundle)>,
        jobs: BTreeMap<ReservationId, (WorkLease, ReservationBundle)>,
        dispatch: EffectDispatch,
    }
    impl Fixture {
        fn new() -> Self {
            let ledger = RuntimeLedger::new(
                ResourceVector::new([
                    (Dimension::ActiveTasks.id(), 1),
                    (Dimension::PreparingTasks.id(), 1),
                    (Dimension::PreparingBytes.id(), 100),
                ])
                .unwrap(),
            );
            let scope = ledger.open_scope_next(None, generation()).unwrap();
            let mut roles = Vec::new();
            for (index, kind) in [WorkKind::Cleanup, WorkKind::Step, WorkKind::Step]
                .into_iter()
                .enumerate()
            {
                let (mut work, capacity) = scope
                    .reserve_next(
                        generation(),
                        novarocks_control_runtime_core::capacity::ReservationResources::new(ResourceVector::default(), novarocks_control_runtime_core::owned_storage::OwnedSlotNeed { work_headers: 1, inputs: if index == 2 {48} else if index == 1 {1} else {0}, batches: if index == 2 {48} else if index == 1 {1} else {0}, effects: (if index == 2 {48} else if index == 1 {1} else {0}) * ContextActor::MAX_STEP_ACTIONS, replies: if index == 2 {48} else {0}, ..novarocks_control_runtime_core::owned_storage::OwnedSlotNeed::default() }),
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
                request_horizon: crate::RequestHorizon::DEFAULT,
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
            let runtime = ActorRuntime::new();
            let registration = runtime.register_supervised_bound(
                    actor(),
                    scope.clone(),
                    WorkerActor::Context(Box::new(ContextActor::new(
                        core,
                        scope.clone(),
                        AdmissionStorage {
                            loading: (crate::control_runtime_policy::ControlRuntimePolicy::try_new(crate::control_runtime_policy::IngressRoleLimits { ordinary_running: 1, ordinary_waiting: 0, control_running: 1, control_waiting: 0, max_batch_items: 1, root_read_holders: 1 }).unwrap()).loading.resources().unwrap(),
                            accepted: (crate::control_runtime_policy::ControlRuntimePolicy::try_new(crate::control_runtime_policy::IngressRoleLimits { ordinary_running: 1, ordinary_waiting: 0, control_running: 1, control_waiting: 0, max_batch_items: 1, root_read_holders: 1 }).unwrap()).accepted.resources().unwrap(),
                            context: novarocks_control_runtime_core::capacity::ReservationResources::work_only(),
                        },
                        roles[1].0.reference(),
                        roles[2].0.reference(),
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
                registration,
                port: None,
                host: None,
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
                .reserve_admitted_input_for(
                    &self.registration,
                    ActorId::new(2).unwrap(),
                    InputClass::Lifecycle,
                    &self.roles[2].0,
                )
                .unwrap()
                .fill(WorkerMessage::Context(ContextMessage::Command {
                    work: self.roles[2].0.reference(),
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
                            &TaskHostBinding {
                                context: self.context,
                                task_source: ActorId::new(2).unwrap(),
                                protocol_generation: generation(),
                                max_cuts: 4,
                                registration: self.registration.clone(),
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
        fn finish(mut self) {
            // The complete unit owner has ended: no reader or host call may
            // borrow this port again. This is not production Task sealing or
            // OwnerFailed publication, especially after a real scope failure.
            let port = self.port.take().unwrap();
            port.retire_execution_port();
            let ingress = port.ingress().clone();
            ingress.seal_source_admission();
            let TaskHostResources {
                mut lifetime,
                mut active,
                port: host_port,
                host_calls,
                disposal,
                loader,
                loading_events,
                loading_return,
                pending_loading,
                preparation,
                preparation_remainder,
                accepted_role_storage,
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
            } = self.host.take().unwrap().into_resources();
            assert!(
                production.is_none(),
                "component fixture never created a production runtime/root owner"
            );
            assert!(loader.is_none());
            assert!(loading_events.is_none());
            assert!(loading_return.is_none());
            assert!(pending_loading.is_none());
            assert!(preparation.is_none());
            assert!(preparation_remainder.is_none());
            assert!(completion.is_none());
            assert!(completion_action.is_none());
            assert!(lifecycle_inputs.is_empty());
            assert!(statistics.is_none());
            assert!(dynamic_filters.is_none());
            assert!(
                open_summary.is_none(),
                "this component issued no open-summary role"
            );
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
            assert_eq!(
                Arc::strong_count(&port),
                2,
                "only these actual unit owners remain"
            );
            // Preserve the original ingress custodian while destroying the
            // last published rich snapshot, outside every runtime/port lock.
            drop(port);
            drop(host_port);
            for (mut work, mut capacity) in std::mem::take(&mut self.jobs).into_values() {
                work.body_exited().unwrap();
                work.captures_exited().unwrap();
                self.ledger.reap_scope_ready(self.scope.id(), 64).unwrap();
                capacity
                    .release_quota(&capacity.resources().unwrap().quota)
                    .unwrap();
                capacity.finish().unwrap();
            }
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
            lifetime.body_exited().unwrap();
            lifetime.captures_exited().unwrap();
            assert!(
                !lifetime.actual_exit_ready().unwrap(),
                "the original Task lifetime still owns its unused Source children"
            );
            let _open = self.retire_source_pair(&ingress, PairRoute::Open);
            let consuming = self.retire_source_pair(&ingress, PairRoute::ConsumingOpen);
            if !self.core_consumed.lock().unwrap().is_empty() {
                ingress
                    .finish_open_after_reap(consuming)
                    .unwrap_or_else(|_| panic!("the actual consumed source prefix retires"));
            } else {
                drop(consuming);
            }
            assert_eq!(ingress.retained_sources(), 0);
            // No further Task host issuer remains. Only this parcel's unused
            // role inventory is returned; bound Source/report exits above are real.
            if let Some(mut original) = accepted_role_storage {
                assert!(original.resources().unwrap().quota.is_empty());
                original.finish().unwrap();
            }
            assert!(lifetime.actual_exit_ready().unwrap());
            self.ledger.reap_scope_ready(self.scope.id(), 64).unwrap();
            active
                .release_quota(&active.resources().unwrap().quota)
                .unwrap();
            active.finish().unwrap();
            drop(causal);
            drop(ingress);
            for (mut work, mut capacity) in self.roles.drain(..) {
                work.body_exited().unwrap();
                work.captures_exited().unwrap();
                self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
                capacity
                    .release_quota(&capacity.resources().unwrap().quota)
                    .unwrap();
                capacity.finish().unwrap();
            }
            self.scope.close_external().unwrap();
            while let Some(original_host_turn) = self.runtime.claim_host_actor() {
                original_host_turn.release(false).unwrap();
            }
            self.runtime.remove_actor(actor()).unwrap();
            self.ledger.reap_scope_ready(self.scope.id(), 3).unwrap();
            self.ledger.remove_scope(self.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
        }
    }
    fn cleanup_summary() -> ProposalSummary {
        let mut summary = ProposalSummary::default();
        summary.cleanup(Diagnostic {
            phase: TaskFailurePhase::Execution,
            fact: FailureFact::new(
                FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            ),
            detail: SafeDetail::truncating("ordered cleanup observation"),
            cleanup: true,
        });
        summary
    }

    #[test]
    fn task_status_and_convergence_become_visible_in_the_same_real_commit() {
        let mut f = Fixture::new();
        let task = f.port().identity();
        let canceled = f.send(
            ContextInput::Request(ContextCommand::Cancel {
                task,
                reason: CancelReason::UpstreamNoLongerNeeded,
            }),
            Some(operation(2)),
        );
        f.drain();
        f.runtime.retire_input(actor(), canceled).unwrap();
        let before = f.port().publication().read();
        assert!(!before.status().is_terminal());
        assert!(!before.convergence().conclusion_stable());
        let input = f.send(ContextInput::FinishWithOutput { task, output: None }, None);
        let ticket = f.next().unwrap();
        let WorkerAction::Context(ContextEffect::PublishTask {
            snapshot,
            convergence,
            ..
        }) = &ticket.effect
        else {
            panic!("the Core publishes its final status and convergence together")
        };
        assert!(snapshot.is_terminal());
        assert!(convergence.conclusion_stable());
        assert_eq!(f.port().publication().read().status(), before.status());
        assert_eq!(
            f.port().publication().read().convergence(),
            before.convergence()
        );
        f.commit(&ticket);
        let actual = f.port().publication().read();
        assert!(actual.status().is_terminal());
        assert!(actual.convergence().conclusion_stable());
        assert!(
            !actual.convergence().actual_stopped(),
            "terminal publication supplies no physical exit"
        );
        let state = f
            .runtime
            .effect_inventory(actor(), input, None, 15)
            .unwrap();
        assert_eq!(
            state[0].1,
            EffectState::Published,
            "commit does not finish the host body"
        );
        f.runtime.effect_completed(ticket).unwrap();
        f.drain();
        f.runtime.retire_input(actor(), input).unwrap();
        f.finish();
    }

    #[test]
    fn diagnostic_publications_keep_task_fifo_across_batches_until_actual_commit() {
        let mut f = Fixture::new();
        let task = f.port().identity();
        let first = f.send(
            ContextInput::Summary {
                task,
                generation: generation(),
                cut: None,
                summary: cleanup_summary(),
            },
            None,
        );
        let second = f.send(
            ContextInput::Summary {
                task,
                generation: generation(),
                cut: None,
                summary: cleanup_summary(),
            },
            None,
        );
        let first_ticket = f.next().unwrap();
        assert_eq!(first_ticket.input, first);
        assert!(matches!(
            first_ticket.effect,
            WorkerAction::Context(ContextEffect::SummaryConsumed { .. })
        ));
        assert!(
            f.next().is_none(),
            "later Task publication waits for the earlier real commit"
        );
        assert_eq!(
            f.port()
                .publication()
                .read()
                .secondary_diagnostics()
                .entries()
                .len(),
            0
        );
        f.commit(&first_ticket);
        assert_eq!(
            f.port()
                .publication()
                .read()
                .secondary_diagnostics()
                .entries()
                .len(),
            1
        );
        let second_ticket = f.next().unwrap();
        assert_eq!(second_ticket.input, second);
        assert_eq!(
            f.runtime
                .effect_inventory(actor(), first, None, 15)
                .unwrap()[0]
                .1,
            EffectState::Published
        );
        f.commit(&second_ticket);
        assert_eq!(
            f.port()
                .publication()
                .read()
                .secondary_diagnostics()
                .entries()
                .len(),
            2
        );
        assert!(
            f.port().accepted().is_none(),
            "diagnostic publication has no conclusion authority"
        );
        f.runtime.effect_completed(second_ticket).unwrap();
        f.runtime.effect_completed(first_ticket).unwrap();
        f.runtime.retire_input(actor(), second).unwrap();
        f.runtime.retire_input(actor(), first).unwrap();
        f.finish();
    }

    #[test]
    fn stale_publication_returns_the_original_source_and_claims_no_exit() {
        #[derive(Debug)]
        struct Source {
            drops: Arc<AtomicUsize>,
            ingress: Weak<TaskProposalIngress>,
            source: Arc<Mutex<Option<SourceId>>>,
        }
        impl std::fmt::Display for Source {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("source custody probe")
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
                let ingress = self.ingress.upgrade().expect("original ingress custodian");
                let source = self
                    .source
                    .lock()
                    .unwrap()
                    .expect("actual summary source id");
                // Reenter the real ingress fence during rich snapshot disposal.
                // This same source's graph leg has not exited its destructor.
                let original = ingress.source_inventory(source).unwrap();
                assert_eq!(
                    original.phase,
                    super::super::source_positions::SourcePositionPhase::Taken
                );
                assert!(!original.custody.actual_exit_ready().unwrap());
                assert!(!original.custody.reusable());
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        for stale in [false, true] {
            let mut f = Fixture::new();
            let task = f.port().identity();
            let drops = Arc::new(AtomicUsize::new(0));
            let source = Arc::new(Mutex::new(None));
            let original = ExecutionFailure::from_domain(Source {
                drops: drops.clone(),
                ingress: Arc::downgrade(f.port().ingress()),
                source: source.clone(),
            });
            let source_address = original.cause() as *const _;
            f.port().submit(TaskConclusionProposal::Failed {
                cause: original,
                phase: TaskFailurePhase::Execution,
            });
            let summary = f.port().ingress().take_next_summary().unwrap();
            let source_id = summary.summary.candidate_sources().next().unwrap();
            *source.lock().unwrap() = Some(source_id);
            let summary_input = f.send(
                ContextInput::Summary {
                    task,
                    generation: generation(),
                    cut: summary.cut,
                    summary: summary.summary,
                },
                None,
            );
            let input = if stale {
                // First publish the original source through the real runtime.
                // The later terminal candidate retains that precise source.
                f.drain();
                f.port().ingress().summary_consumed(summary.cut).unwrap();
                f.runtime.retire_input(actor(), summary_input).unwrap();
                f.send(ContextInput::FinishWithOutput { task, output: None }, None)
            } else {
                summary_input
            };
            let ticket = f.next().unwrap();
            assert!(matches!(
                (&ticket.effect, stale),
                (
                    WorkerAction::Context(ContextEffect::PublishTask { .. }),
                    true
                ) | (
                    WorkerAction::Context(ContextEffect::PublishConclusion { .. }),
                    false
                )
            ));
            let status_before = f.port().current();
            let convergence_before = f.port().publication().read().convergence();
            let work_before = f.scope.snapshot().unwrap().work;
            let foreign_runtime = ActorRuntime::<WorkerActor>::new();
            // Exercise the real actor failure boundary, not only a ledger
            // generation change: the original fixed failure source then owns
            // the old batch's explicit abandonment and actual cleanup.
            let failed_input = stale.then(|| {
                let unknown_task = TaskIdentity::new(
                    f.context.query_execution_id(),
                    StageId::new(1).unwrap(),
                    TaskId::new(999).unwrap(),
                    f.context.backend_process_id(),
                );
                let original = f.send(ContextInput::Installed { task: unknown_task }, None);
                assert!(f.runtime.snapshot(actor()).unwrap().failed);
                assert_eq!(
                    f.runtime
                        .input_custody(actor(), original)
                        .unwrap()
                        .unwrap()
                        .phase,
                    InputPhase::AbandonedByOwnerFailure
                );
                original
            });
            let target = if stale { &f.runtime } else { &foreign_runtime };
            let mut refusal = match commit_task_projection(target, &ticket, f.port()) {
                Err(refusal) => refusal,
                Ok(_) => panic!("a refused effect cannot publish its candidate"),
            };
            if stale {
                assert!(matches!(
                    refusal.error,
                    TaskPublicationError::Runtime(DriverError::FailurePublicationRequired)
                ));
            } else {
                assert!(matches!(
                    refusal.error,
                    TaskPublicationError::Runtime(DriverError::UnknownActor)
                ));
                assert_eq!(
                    f.port().ingress().retained_sources(),
                    1,
                    "the candidate still owes its original source inventory position"
                );
            }
            let inventory = f.port().ingress().source_inventory(source_id).unwrap();
            assert_eq!(
                inventory.phase,
                super::super::source_positions::SourcePositionPhase::Taken
            );
            assert!(!inventory.custody.actual_exit_ready().unwrap());
            assert!(!inventory.custody.reusable());
            let candidate = refusal.projection.as_ref().unwrap().accepted().unwrap();
            assert!(std::ptr::eq(
                candidate.originating_failure().unwrap().cause(),
                source_address
            ));
            assert_eq!(f.port().accepted().is_some(), stale);
            assert_eq!(f.port().current(), status_before);
            assert_eq!(
                f.port().publication().read().convergence(),
                convergence_before
            );
            assert!(!f.port().is_stopped());
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert_eq!(f.scope.snapshot().unwrap().work, work_before);
            assert_eq!(
                f.runtime
                    .effect_inventory(actor(), input, None, 15)
                    .unwrap()[0]
                    .1,
                EffectState::Started,
                "refusal supplies no body or capture exit"
            );
            drop(candidate);
            if !stale {
                // Recover using the exact returned value; calling the helper
                // again would incorrectly try to take the original source twice.
                let original = refusal.projection.take().unwrap();
                drop(
                    f.runtime
                        .commit_snapshot(&ticket, f.port().publication(), original)
                        .unwrap_or_else(|(error, _)| panic!("original runtime commit: {error:?}")),
                );
                f.dispatch
                    .publication_committed(&f.runtime, &ticket)
                    .unwrap();
            }
            drop(refusal);
            assert_eq!(
                drops.load(Ordering::SeqCst),
                0,
                "the live published projection still owns its original source"
            );
            f.runtime.effect_completed(ticket).unwrap();
            if !stale {
                f.drain();
                f.port().ingress().summary_consumed(summary.cut).unwrap();
            } else {
                // The failed Scope cannot start the remaining publications.
                // The original started host call returned above; abandon only
                // still-unstarted actions, without publishing their candidate,
                // inventing an exit or releasing the original rich snapshot.
                let remaining = f
                    .runtime
                    .effect_inventory(actor(), input, None, 15)
                    .unwrap();
                let mut abandoned = 0;
                for (descriptor, state) in remaining {
                    if state == EffectState::Pending {
                        f.runtime
                            .abandon_unstarted_effect(actor(), input, descriptor.id)
                            .unwrap();
                        abandoned += 1;
                    } else {
                        assert_eq!(state, EffectState::Completed);
                    }
                }
                assert!(abandoned > 0);
                assert_eq!(f.port().current(), status_before);
                assert_eq!(drops.load(Ordering::SeqCst), 0);
            }
            f.runtime.retire_input(actor(), input).unwrap();
            if let Some(failed_input) = failed_input {
                // Retire the actual failed input and run its separately paid
                // fixed cleanup action. None of these neutral receipts owns
                // the original rich Source, which still awaits port disposal.
                f.dispatch.clear_failed_index(&f.runtime).unwrap();
                let novarocks_control_runtime::driver::FailureSourceState::ClosedPending(cleanup) =
                    f.runtime.failure_source_state(actor()).unwrap()
                else {
                    panic!("the original failure source retains its actual cleanup");
                };
                let descriptor = f
                    .runtime
                    .effect_inventory(actor(), cleanup, None, 1)
                    .unwrap()[0]
                    .0
                    .clone();
                let cleanup_ticket = f
                    .runtime
                    .take_effect(actor(), cleanup, descriptor.id)
                    .unwrap();
                assert!(matches!(
                    cleanup_ticket.effect,
                    WorkerAction::OwnerFailed { scope, .. } if scope == f.scope.id()
                ));
                assert_eq!(cleanup_ticket.descriptor.work, f.roles[0].0.reference());
                f.roles[0].0.start().unwrap();
                f.runtime.effect_completed(cleanup_ticket).unwrap();
                f.runtime.retire_input(actor(), cleanup).unwrap();
                f.runtime.retire_input(actor(), failed_input).unwrap();
                drop(f.runtime.take_failure_payload(actor()).unwrap().unwrap());
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                assert_eq!(f.port().current(), status_before);
            }
            f.finish();
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }
}
