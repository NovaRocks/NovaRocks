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

//! The original finite terminal action owns its actual exit authority. Its
//! custodian retains unknown responsibility and never infers event consumption
//! from a committed predecessor summary or from physical action exit.

use super::causal::{CausalAuthorBinding, CausalError, TaskCausalInbox, TaskCausalSender};
use crate::{
    task_completion_roles::AdmittedCompletionRole,
    task_lifecycle::{CompletionExitCapability, CompletionExitNotice},
    task_port::TaskPendingNotice,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{completion::Consumption, identity::ConsumerId};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::{
    TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact},
};
use novarocks_worker_protocol::{context::ContextInput, proposal_summary::CutId};
use std::sync::{Arc, Mutex};

struct Custody {
    task: TaskIdentity,
    cut: CutId,
    binding: CausalAuthorBinding,
    role: ExitRole,
    report: ReportSlot,
    sender: Mutex<Option<TaskCausalSender>>,
    // A refused handoff retains its exact neutral input in this same prepaid
    // responsibility. It cannot be reconstructed as a consumed Core event.
    rejected: Mutex<Option<ContextInput>>,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
#[must_use = "The original terminal action and its report retain actual responsibility"]
pub(crate) struct CompletionActionCustody(Arc<Custody>);
#[derive(Debug)]
pub(crate) enum CompletionActionError {
    Causal(CausalError),
    Role(ExitRoleError),
    Report(RuntimeError),
}
struct Exit {
    custody: CompletionActionCustody,
    authority: Option<ExitAuthority>,
}
impl CompletionActionCustody {
    /// The Accepted Task host supplies this exact Work, report and already
    /// bound Input-kind sender once, before attaching any completion action.
    /// This constructor acquires no capacity and signs no additional role.
    pub(crate) fn from_admitted(
        task: TaskIdentity,
        admitted: AdmittedCompletionRole,
        inbox: &mut TaskCausalInbox,
        cut: CutId,
        sender: TaskCausalSender,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> (Self, CompletionExitNotice) {
        // This internal constructor is called by the original TaskHost issuer.
        // An invariant violation fails before an exit authority or callback is
        // constructed; it never converts a different producer into this role.
        let binding = inbox
            .author_binding(task, cut, admitted.work.id(), &sender)
            .expect("the original action uses its exact admitted sender and Work");
        let report = admitted.report.clone();
        let (role, authority) = ExitRole::from_admitted(super::admitted_role::adopt_reported_work(
            admitted.work,
            admitted.capacity,
            admitted.report,
        ));
        let custody = Arc::new(Custody {
            task,
            cut,
            binding,
            role,
            report,
            sender: Mutex::new(Some(sender)),
            rejected: Mutex::new(None),
            pending,
            health,
            retained: Mutex::new(None),
        });
        *custody
            .retained
            .lock()
            .expect("terminal action original retention") = Some(custody.clone());
        let owner = Self(custody);
        inbox
            .bind_completion_action(owner.clone())
            .expect("the original action binds its admitted causal role before handoff");
        let notice = CompletionExitNotice::from_admitted_role(
            task,
            Box::new(Exit {
                custody: owner.clone(),
                authority: Some(authority),
            }),
        );
        // Issuance is not callback invocation. The actual CompletionSupervisor
        // owns its invocation phase; this original holder can remain Admitted
        // until its real action/fact/reporter disposal supplies exit proof.
        (owner, notice)
    }
    pub(crate) fn cut(&self) -> CutId {
        self.0.cut
    }
    pub(crate) fn task(&self) -> TaskIdentity {
        self.0.task
    }
    pub(crate) fn report(&self) -> &ReportSlot {
        &self.0.report
    }
    pub(crate) fn phase(&self) -> ExitRolePhase {
        self.0.role.phase()
    }
    pub(crate) fn actual_exit_ready(&self) -> Result<bool, ExitRoleError> {
        self.0.role.actual_exit_ready()
    }
    /// Called by the exclusive host custodian before retiring causal inputs.
    /// Actual action exit and its exact event commit acknowledge different
    /// report legs. Neither waits for the Source pair's later open segment.
    pub(crate) fn observe_consumers(
        &self,
        inbox: &mut TaskCausalInbox,
    ) -> Result<bool, CompletionActionError> {
        if self.phase() == ExitRolePhase::Retired {
            return Ok(true);
        }
        if !self
            .actual_exit_ready()
            .map_err(CompletionActionError::Role)?
        {
            return Ok(false);
        }
        // A failed/missing Core observation must not suppress this independent
        // positive observation of the actual host body and captures.
        let observer = ConsumerId::new(2).unwrap();
        if self
            .report()
            .consumption(observer)
            .map_err(CompletionActionError::Report)?
            == Consumption::Pending
        {
            self.report()
                .consume(observer)
                .map_err(CompletionActionError::Report)?;
        }
        let core_consumed = inbox
            .bound_author_report_consumed(&self.0.binding)
            .map_err(CompletionActionError::Causal)?;
        let core = ConsumerId::new(1).unwrap();
        if core_consumed
            && self
                .report()
                .consumption(core)
                .map_err(CompletionActionError::Report)?
                == Consumption::Pending
        {
            self.report()
                .consume(core)
                .map_err(CompletionActionError::Report)?;
        }
        self.try_retire().map_err(CompletionActionError::Role)
    }
    /// Failure takeover can transfer the precise refused payload. Taking it
    /// supplies no positive action exit or input-consumption observation.
    pub(crate) fn take_rejected(&self) -> Option<ContextInput> {
        self.0
            .rejected
            .lock()
            .expect("terminal action refused input")
            .take()
    }
    /// Report consumers are deliberately explicit at the real host. In
    /// particular a sent action's Core consumer 1 requires this cut's actual
    /// event input to commit; its predecessor alone cannot authorize release.
    /// An unused cut instead requires the host's actual revocation/exit receipt,
    /// without inventing an event or its consumption.
    pub(crate) fn try_retire(&self) -> Result<bool, ExitRoleError> {
        let retired = self.phase() == ExitRolePhase::Retired || self.0.role.try_retire()?;
        if retired {
            let retained = self
                .0
                .retained
                .lock()
                .expect("terminal action original retention")
                .take();
            drop(retained);
        }
        Ok(retired)
    }
    fn unresolved(&self) {
        self.0.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.0.task,
                phase: TaskFailurePhase::Execution,
            },
            FailureFact::new(
                FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            ),
        );
        self.0.pending.pending();
    }
}
impl Exit {
    fn take_sender(&self) -> TaskCausalSender {
        self.custody
            .0
            .sender
            .lock()
            .expect("original terminal action sender")
            .take()
            .expect("one original terminal action handoff")
    }
    fn finish(mut self, completed: bool) {
        let sender = self.take_sender();
        if completed {
            if let Err(input) = sender.send_input(ContextInput::CompletionExited {
                task: self.custody.0.task,
            }) {
                *self
                    .custody
                    .0
                    .rejected
                    .lock()
                    .expect("terminal action refused input") = Some(input);
                // Drop of the unique authority records unknown exit. Neither
                // queue refusal nor a missing receiver supplies a positive fact.
                drop(self.authority.take());
                self.custody.unresolved();
                return;
            }
        } else {
            // This cut never transferred an event. Revocation is distinct from
            // an actual Core consumer and publishes no CompletionExited.
            sender.unused();
        }
        let result = self
            .authority
            .take()
            .expect("one original terminal action exit")
            .exited();
        if result.is_err() {
            self.custody.unresolved();
        } else {
            self.custody.0.pending.pending();
        }
    }
}
impl CompletionExitCapability for Exit {
    fn exited(self: Box<Self>) {
        (*self).finish(true);
    }
    fn withdrawn(self: Box<Self>) {
        (*self).finish(false);
    }
    fn revoke_unused(self: Box<Self>) {
        (*self).finish(false);
    }
    fn unresolved(mut self: Box<Self>) {
        drop(self.authority.take());
        self.custody.unresolved();
    }
}
impl Drop for Exit {
    fn drop(&mut self) {
        if let Some(authority) = self.authority.take() {
            // The surrounding notice normally chooses this branch explicitly;
            // unwinding the issuer must retain the same responsibility too.
            drop(authority);
            self.custody.unresolved();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_admission::{AdmissionStorage, Dimension};
    use crate::control_runtime::actors::*;
    use crate::control_runtime::causal::{CausalSourceRetirement, TaskCausalInbox};
    use crate::control_runtime::source_positions::ReapedPosition;
    use crate::task_proposal_ingress::{
        CutSourceAdmission, PairRoute, RetiredCut, TaskProposalIngress,
    };
    use novarocks_control_runtime::{
        actor::ActorConfig,
        driver::ActorRuntime,
        lease::{CapacityBundle, WorkLease},
        transaction::{RuntimeLedger, ScopeHandle},
    };
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        completion::{CompletionError, ConsumerKind, Consumption},
        effect::EffectState,
        identity::{ActorId, ConsumerId, Generation, InputId, ReservationId, Tick},
        mailbox::InputClass,
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_execution_contract::{
        CancelReason, OperationOutcome, QueryContextRef, TaskConclusionProposal, TaskOperationId,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_worker_protocol::{
        context::{ContextConfig, ContextCore, ContextEffect, RequestResult},
        creation::{CreationCost, PayloadId},
        lease::{LeaseBounds, MonotonicInstant},
    };
    use std::{
        collections::BTreeMap,
        mem::ManuallyDrop,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    fn generation() -> Generation {
        Generation::new(1).unwrap()
    }
    fn actor() -> ActorId {
        ActorId::new(1).unwrap()
    }
    fn context() -> QueryContextRef {
        QueryContextRef::new(
            QueryExecutionId::new(QueryId::new(7301, 7302), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }
    fn task(c: QueryContextRef, n: u32) -> TaskIdentity {
        TaskIdentity::new(
            c.query_execution_id(),
            StageId::new(1).unwrap(),
            TaskId::new(n).unwrap(),
            c.backend_process_id(),
        )
    }
    fn operation(n: u8) -> TaskOperationId {
        let mut bytes = [n; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        TaskOperationId::try_from_bytes(bytes).unwrap()
    }

    fn empty() -> ResourceVector {
        ResourceVector::default()
    }
    struct RuntimeFixture {
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        runtime: ActorRuntime<WorkerActor>,
        identity: QueryContextRef,
        roles: Vec<(WorkLease, CapacityBundle)>,
        jobs: BTreeMap<ReservationId, (WorkLease, CapacityBundle)>,
    }
    impl RuntimeFixture {
        fn new() -> Self {
            Self::with_byte_limits(100)
        }
        fn with_byte_limits(bytes: u64) -> Self {
            Self::with_actor_turn_budget(bytes, 64)
        }
        fn with_actor_turn_budget(bytes: u64, turn_budget: usize) -> Self {
            let ledger = RuntimeLedger::new(
                ResourceVector::new([
                    (Dimension::ActiveTasks.id(), 2),
                    (Dimension::PreparingTasks.id(), 1),
                    (Dimension::PreparingBytes.id(), bytes),
                    (Dimension::InboundRecords.id(), 4),
                    (Dimension::InboundBytes.id(), bytes),
                    (Dimension::Tickets.id(), 1),
                    (Dimension::Contexts.id(), 1),
                ])
                .unwrap(),
                32 + 6, // Four base Source roles and the cut's two original roles.
                2,
            );
            let scope = ledger
                .open_scope_next(None, generation(), 32 + 6, 32 + 6)
                .unwrap();
            let mut roles = Vec::new();
            for kind in [WorkKind::Cleanup, WorkKind::Step, WorkKind::Step] {
                let (mut work, capacity) = scope
                    .reserve_next(generation(), empty(), kind, WorkAdmission::External)
                    .unwrap()
                    .bind(kind)
                    .unwrap();
                if kind != WorkKind::Cleanup {
                    work.start().unwrap();
                }
                roles.push((work, capacity));
            }
            let identity = context();
            let core = ContextCore::new(
                identity,
                generation(),
                crate::control_runtime::actors::tests::initial_establishment(),
                ContextConfig {
                    max_tasks: 4,
                    max_creation_waiters: 4,
                    max_pending_requests: 16,
                    max_preparing: 1,
                    max_task_bytes: bytes,
                    step_budget: ContextCore::MAX_STEP_WORK,
                    lease_bounds: LeaseBounds::DEFAULT,
                    metric_publish_interval: Duration::from_millis(100),
                },
            )
            .unwrap();
            let runtime = ActorRuntime::new(2);
            let bound = ContextActor::new(
                core,
                scope.clone(),
                AdmissionStorage {
                    loading: empty(),
                    accepted: empty(),
                    context: empty(),
                    ticket: empty(),
                },
                roles[1].0.id(),
                roles[2].0.id(),
            );
            runtime
                .register_supervised_next(
                    actor(),
                    scope.clone(),
                    WorkerActor::Context(Box::new(bound)),
                    ActorConfig {
                        mailbox: [16, 16, 16, 1],
                        max_batches: 49,
                        max_effects_per_step: ContextCore::MAX_STEP_WORK,
                        max_dependencies_per_effect: ContextCore::MAX_STEP_WORK,
                        turn_budget,
                        max_step_work: ContextCore::MAX_STEP_WORK,
                    },
                    &roles[0].0,
                )
                .unwrap_or_else(|(e, _)| panic!("context registration: {e:?}"));
            let mut fixture = Self {
                ledger,
                scope,
                runtime,
                identity,
                roles,
                jobs: BTreeMap::new(),
            };
            let input = fixture.send(
                ContextInput::MaterializationFinished {
                    result: Ok(crate::control_runtime::actors::tests::initial_materialized()),
                },
                None,
                InputClass::Lifecycle,
                2,
            );
            fixture.runtime.poll_one(Tick(1)).unwrap();
            fixture.drain(input);
            fixture
        }
        fn send(
            &self,
            command: ContextInput,
            request: Option<TaskOperationId>,
            class: InputClass,
            source: u64,
        ) -> InputId {
            self.runtime
                .reserve_input(actor(), ActorId::new(source).unwrap(), class)
                .unwrap()
                .fill(WorkerMessage::Context(ContextMessage::Command {
                    work: self.roles[2].0.id(),
                    request: request.map(|operation| RequestMetadata {
                        operation,
                        deadline: MonotonicInstant::from_origin(Duration::from_secs(10)),
                    }),
                    command,
                }))
                .unwrap()
        }
        fn run(
            &mut self,
            command: ContextInput,
            request: Option<TaskOperationId>,
        ) -> Vec<WorkerAction> {
            let input = self.send(command, request, InputClass::Ordinary, 2);
            self.runtime.poll_one(Tick(1)).unwrap();
            self.drain(input)
        }
        fn drain(&mut self, input: InputId) -> Vec<WorkerAction> {
            self.drain_for(actor(), input)
        }
        fn drain_for(&mut self, target: ActorId, input: InputId) -> Vec<WorkerAction> {
            let mut outputs = Vec::new();
            loop {
                let inventory = self
                    .runtime
                    .effect_inventory(target, input, None, ContextCore::MAX_STEP_WORK)
                    .unwrap();
                let states: BTreeMap<_, _> = inventory.iter().map(|(d, s)| (d.id, *s)).collect();
                for (descriptor, state) in inventory {
                    if state == EffectState::Pending {
                        for predecessor in descriptor.dependencies {
                            if states[&predecessor] == EffectState::Completed {
                                self.runtime
                                    .acknowledge_dependency(
                                        target,
                                        input,
                                        descriptor.id,
                                        predecessor,
                                    )
                                    .unwrap();
                            }
                        }
                    }
                }
                let ready = self
                    .runtime
                    .ready_effects(target, input, ContextCore::MAX_STEP_WORK)
                    .unwrap();
                if ready.is_empty() {
                    break;
                }
                for action in ready {
                    let ticket = self.runtime.take_effect(target, input, action).unwrap();
                    let admitted = match &ticket.effect {
                        WorkerAction::Context(
                            ContextEffect::LoadInput { reservation, .. }
                            | ContextEffect::EnqueuePreparation { reservation, .. },
                        ) => Some(*reservation),
                        WorkerAction::Control { admitted, .. } => *admitted,
                        _ => None,
                    };
                    if let Some(reservation) = admitted {
                        let guard = self
                            .runtime
                            .take_step_reservation(&ticket, reservation)
                            .unwrap();
                        assert_eq!(guard.work_id(), ticket.descriptor.work);
                        let (mut work, capacity) = guard.bind(WorkKind::Business).unwrap();
                        work.start().unwrap();
                        assert!(self.jobs.insert(reservation, (work, capacity)).is_none());
                    }
                    if let WorkerAction::Control {
                        effect:
                            novarocks_worker_protocol::control::ControlEffect::RetireReservation(id),
                        ..
                    } = &ticket.effect
                    {
                        let (mut work, mut capacity) = self
                            .jobs
                            .remove(id)
                            .expect("actual control reservation custody");
                        work.body_exited().unwrap();
                        work.captures_exited().unwrap();
                        self.ledger.reap_scope_ready(work.scope().id(), 32).unwrap();
                        capacity.release(&capacity.resources().unwrap()).unwrap();
                    }
                    if let WorkerAction::OwnerFailed { scope, .. } = &ticket.effect {
                        assert_eq!(*scope, self.scope.id());
                        self.roles[0].0.start().unwrap();
                    }
                    outputs.push(ticket.effect.clone());
                    self.runtime.effect_completed(ticket).unwrap();
                }
            }
            self.runtime.retire_input(target, input).unwrap();
            outputs
        }
        fn finish(mut self) {
            let mut jobs: Vec<_> = std::mem::take(&mut self.jobs).into_values().collect();
            for (work, _) in &mut jobs {
                work.body_exited().unwrap();
                work.captures_exited().unwrap();
            }
            self.ledger
                .reap_scope_ready(self.scope.id(), 32 + 6)
                .unwrap();
            for (work, capacity) in &mut jobs {
                assert!(work.actual_exit_ready().unwrap());
                capacity.release(&capacity.resources().unwrap()).unwrap();
            }
            for (mut work, mut capacity) in self.roles.drain(..) {
                work.body_exited().unwrap();
                work.captures_exited().unwrap();
                self.ledger.reap_scope_ready(work.scope().id(), 32).unwrap();
                capacity.release(&capacity.resources().unwrap()).unwrap();
            }
            while let Some(id) = self.runtime.claim_effect_actor() {
                self.runtime.release_effect_actor(id, false).unwrap();
            }
            drop(self.runtime.take_failure_payload(actor()).unwrap());
            self.scope.close_external().unwrap();
            self.runtime
                .remove_actor(actor())
                .unwrap_or_else(|e| panic!("context removal: {e:?}"));
            self.ledger.remove_scope(self.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
        }
    }

    struct Pending(AtomicUsize);
    impl TaskPendingNotice for Pending {
        fn pending(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Fixture {
        runtime: RuntimeFixture,
        ingress: Arc<TaskProposalIngress>,
        inbox: TaskCausalInbox,
        custody: CompletionActionCustody,
        notice: Option<CompletionExitNotice>,
        pending: Arc<Pending>,
        health: Arc<ExecutionHealth>,
    }
    fn core() -> ConsumerId {
        ConsumerId::new(1).unwrap()
    }
    fn observer() -> ConsumerId {
        ConsumerId::new(2).unwrap()
    }
    fn source_roles<const N: usize>(
        scope: &ScopeHandle,
        parent: ReservationId,
    ) -> [AdmittedCompletionRole; N] {
        std::array::from_fn(|_| {
            let guard = scope
                .reserve_child_next(generation(), parent, empty(), WorkKind::Cleanup)
                .unwrap();
            // These Source roles belong to this real host observer. All four
            // cases carry no rich source; a Core source receipt is not issued.
            let report = ReportSlot::reserve_next(
                scope,
                generation(),
                guard.work_id(),
                1,
                [(observer(), ConsumerKind::Observer)],
            )
            .unwrap();
            let (work, capacity) = guard.bind(WorkKind::Cleanup).unwrap();
            AdmittedCompletionRole {
                work,
                capacity,
                report,
            }
        })
    }
    fn fixture() -> Fixture {
        let mut runtime = RuntimeFixture::new();
        let task = task(runtime.identity, 1);
        let created = runtime.run(
            ContextInput::Create {
                request: operation(1),
                task,
                payload: PayloadId(1),
            },
            Some(operation(1)),
        );
        let parent = created
            .iter()
            .find_map(|effect| match effect {
                WorkerAction::Context(ContextEffect::LoadInput {
                    task: owner,
                    reservation,
                    ..
                }) if *owner == task => Some(*reservation),
                _ => None,
            })
            .expect("the real Create elected this Task's original loading lifetime");
        assert_eq!(runtime.jobs[&parent].1.id(), parent);
        let accepted = runtime.run(
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
        );
        assert!(accepted.iter().any(|effect| matches!(effect, WorkerAction::Reply(reply) if reply.result == RequestResult::Outcome(OperationOutcome::Accepted))));
        let (work, capacity) = runtime
            .scope
            .reserve_next(
                generation(),
                ResourceVector::new([(Dimension::ActiveTasks.id(), 1)]).unwrap(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let report = ReportSlot::reserve_next(
            &runtime.scope,
            generation(),
            work.id(),
            2,
            [
                (
                    core(),
                    ConsumerKind::Owner {
                        scope: runtime.scope.id(),
                        generation: generation(),
                    },
                ),
                (observer(), ConsumerKind::Observer),
            ],
        )
        .unwrap();
        let health = Arc::new(ExecutionHealth::default());
        let pending = Arc::new(Pending(AtomicUsize::new(0)));
        let ingress = Arc::new(
            TaskProposalIngress::from_admitted(
                task,
                1,
                source_roles(&runtime.scope, parent),
                pending.clone(),
                health.clone(),
            )
            .unwrap_or_else(|_| panic!("the four actual base Source roles fit")),
        );
        let mut inbox = TaskCausalInbox::new(
            runtime.runtime.clone(),
            actor(),
            ActorId::new(7002).unwrap(),
            task,
            generation(),
            ingress.clone(),
            health.clone(),
        );
        let (cut, sender) = inbox
            .admit_input(
                work.id(),
                1,
                pending.clone(),
                CutSourceAdmission::Fresh(source_roles(&runtime.scope, parent)),
            )
            .unwrap_or_else(|_| panic!("the actual cut pair was paid by this Task lifetime"));
        let (custody, notice) = CompletionActionCustody::from_admitted(
            task,
            AdmittedCompletionRole {
                work,
                capacity,
                report,
            },
            &mut inbox,
            cut,
            sender,
            pending.clone(),
            health.clone(),
        );
        Fixture {
            runtime,
            ingress,
            inbox,
            custody,
            notice: Some(notice),
            pending,
            health,
        }
    }
    impl Fixture {
        fn retire_unused_source_observations(
            &self,
            positions: [crate::control_runtime::source_positions::SourcePositionObservation; 2],
        ) {
            for position in positions {
                assert!(
                    position.source.is_none(),
                    "this real cut carried no rich Source"
                );
                assert!(position.custody.actual_exit_ready().unwrap());
                assert_eq!(
                    position.custody.report().consume(observer()),
                    Err(
                        novarocks_control_runtime::transaction::RuntimeError::Completion(
                            CompletionError::NotFilled
                        )
                    ),
                    "the unused Source report was revoked, never delivered to an observer"
                );
                assert!(position.custody.try_retire().unwrap());
            }
            let headers = self.runtime.scope.snapshot().unwrap().work;
            self.runtime
                .ledger
                .reap_scope_ready(self.runtime.scope.id(), headers)
                .unwrap();
        }
        fn retire_source_route(&self, route: PairRoute) -> [ReapedPosition; 2] {
            for unused in self
                .ingress
                .take_unused_pair(route)
                .unwrap()
                .into_iter()
                .flatten()
            {
                unused.exit();
            }
            self.retire_unused_source_observations(self.ingress.pair_inventory(route).unwrap());
            self.ingress
                .prepare_pair_reap(route)
                .unwrap()
                .map(|probe| probe.observe_reaped().unwrap())
        }
        fn retire_returned_pair(&self, retired: RetiredCut) {
            for unused in self
                .ingress
                .take_unused_owned_pair(&retired.pair)
                .unwrap()
                .into_iter()
                .flatten()
            {
                unused.exit();
            }
            self.retire_unused_source_observations(
                self.ingress.owned_pair_inventory(&retired.pair).unwrap(),
            );
            let witnesses = self
                .ingress
                .prepare_owned_pair_reap(&retired.pair)
                .unwrap()
                .map(|probe| probe.observe_reaped().unwrap());
            self.ingress
                .release_owned_pair(&retired.pair, witnesses)
                .unwrap_or_else(|_| panic!("the same returned pair really retired"));
        }
        fn retire_cut(&mut self, cut: CutId) {
            let retired = self
                .inbox
                .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
                .unwrap_or_else(|_| panic!("the actual no-source cut returns its original pair"));
            self.retire_returned_pair(retired);
        }
        fn finish(self) {
            assert!(self.notice.is_none());
            assert!(self.inbox.is_empty());
            assert_eq!(self.custody.phase(), ExitRolePhase::Retired);
            self.ingress.seal_source_admission();
            // Returned cut ownership was already discharged. These remaining
            // base addresses still owe their own unused holder/report exits.
            let _open = self.retire_source_route(PairRoute::Open);
            let _spare = self.retire_source_route(PairRoute::ConsumingOpen);
            assert_eq!(self.ingress.retained_sources(), 0);
            self.runtime
                .ledger
                .reap_scope_ready(self.runtime.scope.id(), 32)
                .unwrap();
            drop(self.inbox);
            self.runtime.finish();
        }
    }
    #[test]
    fn actual_action_exit_seals_the_original_prefix_but_does_not_consume_its_report() {
        let mut f = fixture();
        assert_eq!(f.custody.phase(), ExitRolePhase::Admitted);
        assert_eq!(f.runtime.ledger.used(Dimension::ActiveTasks.id()), 2);
        drop(f.ingress.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        )));
        let mut notice = f.notice.take().unwrap();
        notice.bind();
        notice.completed();
        assert_eq!(f.custody.phase(), ExitRolePhase::Exited);
        assert!(f.custody.actual_exit_ready().unwrap());
        let mut other = fixture();
        assert!(matches!(
            f.custody.observe_consumers(&mut other.inbox),
            Err(CompletionActionError::Causal(CausalError::WrongTask))
        ));
        assert_eq!(
            f.custody.report().consumption(observer()).unwrap(),
            Consumption::Consumed,
            "a Core lookup error cannot suppress actual Host exit observation"
        );
        assert_eq!(
            f.custody.report().consumption(core()).unwrap(),
            Consumption::Pending
        );
        drop(other.notice.take());
        assert!(other.custody.observe_consumers(&mut other.inbox).unwrap());
        other
            .ingress
            .observe_cut_exit(other.custody.cut(), true, true)
            .unwrap();
        other.retire_cut(other.custody.cut());
        other.finish();
        let delivery = f.inbox.deliver_next().unwrap().unwrap();
        assert_eq!(delivery.cut, f.custody.cut());
        assert!(delivery.prefix.is_some());
        assert!(!f.inbox.event_core_consumed(delivery.cut).unwrap());
        assert!(!f.custody.try_retire().unwrap());
        assert!(!f.custody.observe_consumers(&mut f.inbox).unwrap());
        assert!(!f.custody.observe_consumers(&mut f.inbox).unwrap());
        assert_eq!(
            f.custody.report().consumption(core()).unwrap(),
            Consumption::Pending
        );
        assert!(!f.custody.try_retire().unwrap());
        assert_eq!(f.runtime.ledger.used(Dimension::ActiveTasks.id()), 2);
        f.runtime.runtime.poll_one(Tick(1)).unwrap();
        assert!(!f.runtime.runtime.snapshot(actor()).unwrap().failed);
        assert_eq!(
            f.inbox.observe_core(delivery.cut).unwrap(),
            Consumption::Consumed
        );
        assert!(f.inbox.event_core_consumed(delivery.cut).unwrap());
        if let Some(prefix) = delivery.prefix {
            f.runtime.drain(prefix);
        }
        f.runtime.drain(delivery.event);
        assert!(
            f.inbox.event_core_consumed(delivery.cut).unwrap(),
            "the exact saved commit survives runtime input retirement"
        );
        let funding = f.ingress.cut_funding(delivery.cut).unwrap();
        assert!(
            funding
                .custody
                .iter()
                .all(|source| !source.actual_exit_ready().unwrap()),
            "the original funded pair now belongs to the live open segment"
        );
        f.ingress
            .observe_cut_exit(delivery.cut, true, true)
            .unwrap();
        f.ingress.consume_cut(delivery.cut, 0).unwrap();
        let premature = f
            .inbox
            .retire_after_exit(delivery.cut, CausalSourceRetirement::Unclaimed)
            .err()
            .expect("the original event receipt cannot be discarded before action ACK");
        assert_eq!(
            premature.error,
            CausalError::Driver(novarocks_control_runtime::driver::DriverError::RoleBusy)
        );
        assert!(matches!(
            premature.retirement,
            CausalSourceRetirement::Unclaimed
        ));
        assert!(f.custody.observe_consumers(&mut f.inbox).unwrap());
        assert!(f.custody.observe_consumers(&mut f.inbox).unwrap());
        assert_eq!(f.runtime.ledger.used(Dimension::ActiveTasks.id()), 1);
        f.retire_cut(delivery.cut);
        assert!(f.custody.try_retire().unwrap());
        assert_eq!(f.runtime.ledger.used(Dimension::ActiveTasks.id()), 1);
        assert!(f.pending.0.load(Ordering::SeqCst) >= 2);
        assert!(f.health.failure().is_none());
        f.finish();
    }
    #[test]
    fn unused_and_withdrawn_holders_revoke_the_cut_without_a_completion_event_or_consumption() {
        for bound in [false, true] {
            let mut f = fixture();
            let mut notice = f.notice.take().unwrap();
            if bound {
                notice.bind();
                notice.withdrawn();
            } else {
                drop(notice);
            }
            assert!(f.ingress.event_unused(f.custody.cut()).unwrap());
            assert!(f.ingress.take_next_event(false).is_err());
            assert_eq!(f.custody.phase(), ExitRolePhase::Exited);
            assert!(!f.custody.try_retire().unwrap());
            assert_eq!(f.runtime.ledger.used(Dimension::ActiveTasks.id()), 2);
            // The actual original revocation authorizes the unused report,
            // independently of Source storage and without a fabricated event.
            assert!(f.custody.observe_consumers(&mut f.inbox).unwrap());
            f.ingress
                .observe_cut_exit(f.custody.cut(), true, true)
                .unwrap();
            f.retire_cut(f.custody.cut());
            assert!(f.custody.try_retire().unwrap());
            assert!(f.health.failure().is_none());
            let weak_ingress = Arc::downgrade(&f.ingress);
            f.finish();
            assert!(
                weak_ingress.upgrade().is_none(),
                "the sender must not retain an ingress/source graph"
            );
        }
    }
    #[test]
    fn unknown_action_exit_keeps_its_work_report_and_sender_with_the_real_host_inventory() {
        // Expected fail-stop custody: retain the exact real host inventories,
        // including the live causal positions. Unknown exit cannot cleanly
        // remove that actor or consume/clear its cut and report for the test.
        let mut f = ManuallyDrop::new(fixture());
        let mut notice = f.notice.take().unwrap();
        notice.bind();
        drop(notice);
        assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
        assert!(f.custody.0.sender.lock().unwrap().is_some());
        assert!(!f.ingress.event_unused(f.custody.cut()).unwrap());
        assert!(!f.custody.try_retire().unwrap());
        assert_eq!(f.runtime.ledger.used(Dimension::ActiveTasks.id()), 2);
        let failure = f.health.failure().unwrap();
        assert_eq!(
            failure.origin(),
            ExecutionExitOrigin::Task {
                identity: f.custody.0.task,
                phase: TaskFailurePhase::Execution
            }
        );
        let retained = Arc::downgrade(&f.custody.0);
        let custody = f.custody.clone();
        drop(custody);
        assert_eq!(
            retained.upgrade().unwrap().role.phase(),
            ExitRolePhase::Unresolved
        );
    }
    #[test]
    fn a_refused_handoff_does_not_turn_unknown_action_exit_into_a_positive_fact() {
        let mut f = ManuallyDrop::new(fixture());
        // The original cut was already withdrawn before this faulty late
        // completion. The same sender returns its exact refused input; it
        // cannot manufacture a second event or a positive action exit.
        f.ingress.revoke_event(f.custody.cut()).unwrap();
        let mut notice = f.notice.take().unwrap();
        notice.bind();
        notice.completed();
        assert!(
            matches!(f.custody.take_rejected(), Some(ContextInput::CompletionExited { task }) if task == f.custody.0.task)
        );
        assert!(f.custody.take_rejected().is_none());
        assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
        assert!(!f.custody.actual_exit_ready().unwrap());
        assert!(!f.custody.try_retire().unwrap());
        assert!(f.ingress.event_unused(f.custody.cut()).unwrap());
        assert_eq!(f.runtime.ledger.used(Dimension::ActiveTasks.id()), 2);
        assert!(f.health.failure().is_some());
    }
}
