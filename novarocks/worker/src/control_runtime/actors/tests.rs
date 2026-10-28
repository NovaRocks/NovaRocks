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

//! These tests drive the actual common runtime and synchronous capacity port.
//! The host records neutral effects; they do not claim Native/Execution wiring.
use super::*;
use crate::control_admission::Dimension;
use crate::control_runtime_policy::{ControlRuntimePolicy, IngressRoleLimits, StorageNeed};
use novarocks_control_runtime::{
    actor::ActorConfig,
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError},
    lease::{CapacityBundle, WorkLease},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::{ConsumerKind, Consumption},
    effect::EffectState,
    identity::{ActorId, ConsumerId, InputId, ReservationId},
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::failure::FailureProjection;
use novarocks_execution_contract::{
    LeaseValidFor, OperationOutcome, QueryContextRef, TaskOperationId,
};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use novarocks_worker_protocol::{
    context::{ContextConfig, PhysicalFact, RequestResult},
    creation::{CreationCost, PayloadId},
    lease::LeaseBounds,
};
pub(in crate::control_runtime) fn generation() -> Generation {
    Generation::new(1).unwrap()
}
pub(in crate::control_runtime) fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn context() -> QueryContextRef {
    QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(7301, 7302), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    )
}
pub(in crate::control_runtime) fn task(c: QueryContextRef, n: u32) -> TaskIdentity {
    TaskIdentity::new(
        c.query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(n).unwrap(),
        c.backend_process_id(),
    )
}
pub(in crate::control_runtime) fn operation(n: u8) -> TaskOperationId {
    let mut bytes = [n; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    TaskOperationId::try_from_bytes(bytes).unwrap()
}

/// Explicit initial facts for standalone Task/Context runtime fixtures. The
/// real Context bootstrap tests instead consume their actual Materialize ticket.
pub(in crate::control_runtime) fn initial_establishment()
-> novarocks_worker_protocol::context::ContextEstablishment {
    use novarocks_execution_contract::{
        AdmissionTicketId, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    };
    novarocks_worker_protocol::context::ContextEstablishment {
        operation: operation(1),
        ticket: AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
        payload: PayloadId(1),
        elected_at: MonotonicInstant::ORIGIN,
        semantics: novarocks_worker_protocol::control::EstablishSemanticFacts {
            catalog: ContentFingerprint::from_bytes([1; 16]),
            shared_filter: ContentFingerprint::from_bytes([2; 16]),
            query_options: ContentFingerprint::from_bytes([3; 16]),
            credential_lease: CredentialLeaseId::new(1),
            credential_epoch: CredentialEpoch::new(1).unwrap(),
            initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
        },
    }
}
/// Symbolic credential slot supplied by the metadata-only materialization
/// fixture. Native resource creation and physical exit are separate tests.
pub(in crate::control_runtime) fn initial_materialized()
-> novarocks_worker_protocol::context::ContextMaterialized {
    novarocks_worker_protocol::context::ContextMaterialized {
        initial_credential_resource: 101,
    }
}

#[test]
fn actual_accepted_guard_moves_once_to_the_host_preparation_queue_and_retains_close_capacity() {
    use crate::control_runtime::preparation::{
        PREPARATION_CORE_CONSUMER, PREPARATION_OBSERVER_CONSUMER, PreparationCustody,
        PreparationOutcome, PreparationQueue,
    };
    use crate::task_port::TaskPendingNotice;
    use novarocks_execution_contract::{CreationContent, FrozenBytes, TaskCreationInput};
    use std::{any::Any, sync::Arc};
    struct Pending;
    impl TaskPendingNotice for Pending {
        fn pending(&self) {}
    }
    #[derive(Debug)]
    struct Assignment;
    impl CreationContent for Assignment {
        fn encoded_len(&self) -> usize {
            0
        }
        fn retained_bytes(&self) -> usize {
            0
        }
        fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }
    let mut f = Fixture::new();
    let task = task(f.identity, 1);
    let input = TaskCreationInput::new(
        FrozenBytes::freeze(bytes::Bytes::new()),
        Box::new(Assignment),
    );
    let cost = CreationCost {
        input_retained_bytes: u64::try_from(input.retained_bytes()).unwrap(),
        ingress_retained_bytes: 2,
        structural_bytes: 4,
        inbound_records: 1,
        inbound_bytes: 9,
    };
    let loading = f.run(
        ContextInput::Create {
            request: operation(1),
            task,
            payload: PayloadId(1),
        },
        Some(operation(1)),
    );
    let source_parent = loading
        .iter()
        .find_map(|effect| match effect {
            WorkerAction::Context(ContextEffect::LoadInput { reservation, .. }) => {
                Some(*reservation)
            }
            _ => None,
        })
        .unwrap();
    let loaded = f.send(
        ContextInput::Loaded {
            task,
            result: Ok(cost),
        },
        None,
        InputClass::Lifecycle,
        2,
    );
    f.runtime.poll_one(Tick(2)).unwrap();
    let mut dispatch = crate::control_runtime::effect_dispatch::EffectDispatch::new(actor(), 15);
    dispatch.index_committed(&f.runtime, 32).unwrap();
    let publication = dispatch.take_ready(&f.runtime, 15).unwrap().unwrap();
    let WorkerAction::Context(ContextEffect::PublishTask { snapshot, .. }) = &publication.effect
    else {
        panic!("Accepted snapshot is the first actual publication");
    };
    assert!(
        dispatch.take_ready(&f.runtime, 15).unwrap().is_none(),
        "preparation and reply cannot precede Accepted visibility"
    );
    let target = novarocks_control_runtime::publication::PublishedSnapshot::new(
        &f.scope,
        Arc::new(novarocks_execution_contract::TaskStatus::created(task)),
    );
    drop(
        f.runtime
            .commit_snapshot(&publication, &target, Arc::new(snapshot.clone()))
            .unwrap(),
    );
    dispatch
        .publication_committed(&f.runtime, &publication)
        .unwrap();
    let ticket = dispatch.take_ready(&f.runtime, 15).unwrap().unwrap();
    let reservation = match ticket.effect {
        WorkerAction::Context(ContextEffect::EnqueuePreparation { reservation, .. }) => reservation,
        _ => unreachable!(),
    };
    let port = crate::task_port::TaskRuntimePort::from_admitted(
        task,
        &f.scope,
        paid_ingress(&f, task, source_parent, 4),
        Arc::new(Pending),
        Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
    );
    let (custody, job) = PreparationCustody::adopt(
        &f.runtime,
        &ticket,
        input,
        crate::control_runtime::preparation::PreparationBinding {
            stop: port.stop_signal(),
            pending: Arc::new(Pending),
            health: port.health(),
        },
    )
    .unwrap_or_else(|(error, _)| panic!("original Accepted guard adoption: {error}"));
    assert!(matches!(
        f.runtime.take_step_reservation(&ticket, reservation),
        Err(DriverError::UnknownReservation)
    ));
    let queue = PreparationQueue::new(1, 1, 1, 1);
    queue
        .enqueue(job)
        .unwrap_or_else(|_| panic!("prepaid actual queue position"));
    f.runtime.effect_completed(ticket).unwrap();
    f.runtime.effect_completed(publication).unwrap();
    f.drain(loaded);
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 1);
    assert!(matches!(
        queue.pop().unwrap().run(|input, _| {
            let (_, assignment) = input.into_parts();
            assert!(assignment.into_stored().downcast::<Assignment>().is_ok());
        }),
        PreparationOutcome::Returned(())
    ));
    f.run(
        ContextInput::Physical {
            task,
            fact: PhysicalFact::PreparationBodyExited,
        },
        None,
    );
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 1);
    assert!(custody.publish_whole_exit().unwrap());
    f.run(
        ContextInput::Physical {
            task,
            fact: PhysicalFact::PreparationExited,
        },
        None,
    );
    for report in [custody.body_report(), custody.exit_report()] {
        report.consume(PREPARATION_CORE_CONSUMER).unwrap();
        report.consume(PREPARATION_OBSERVER_CONSUMER).unwrap();
    }
    let mut retained = custody.take_after_exit().unwrap().unwrap();
    assert_eq!(retained.id(), reservation);
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 0);
    assert_eq!(f.ledger.used(Dimension::InboundBytes.id()), 9);
    let resources = retained.resources().unwrap();
    retained.release(&resources).unwrap();
    finish_source_routes(&f, port.ingress());
    drop(port);
    f.finish();
}
fn empty() -> ResourceVector {
    ResourceVector::default()
}
fn source_role(
    scope: &ScopeHandle,
    parent: ReservationId,
) -> crate::task_completion_roles::AdmittedCompletionRole {
    let generation = scope.gates().unwrap().generation;
    let admitted = scope
        .reserve_child_next(generation, parent, empty(), WorkKind::Cleanup)
        .unwrap();
    let report = ReportSlot::reserve_next(
        scope,
        generation,
        admitted.work_id(),
        2,
        [
            (
                ConsumerId::new(1).unwrap(),
                ConsumerKind::Owner {
                    scope: scope.id(),
                    generation,
                },
            ),
            (ConsumerId::new(2).unwrap(), ConsumerKind::Observer),
        ],
    )
    .unwrap();
    let (work, capacity) = admitted.bind(WorkKind::Cleanup).unwrap();
    crate::task_completion_roles::AdmittedCompletionRole {
        work,
        capacity,
        report,
    }
}
fn paid_ingress(
    fixture: &Fixture,
    task: TaskIdentity,
    parent: ReservationId,
    cuts: usize,
) -> std::sync::Arc<crate::task_proposal_ingress::TaskProposalIngress> {
    std::sync::Arc::new(
        crate::task_proposal_ingress::TaskProposalIngress::from_admitted(
            task,
            cuts,
            std::array::from_fn(|_| source_role(&fixture.scope, parent)),
            std::sync::Arc::new(CausalPending),
            std::sync::Arc::new(
                novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
            ),
        )
        .unwrap_or_else(|_| panic!("the original Task lifetime funded its four source positions")),
    )
}
fn dispose_retired_pair(
    fixture: &Fixture,
    ingress: &crate::task_proposal_ingress::TaskProposalIngress,
    retired: crate::task_proposal_ingress::RetiredCut,
) {
    let crate::task_proposal_ingress::RetiredCut { pair, funding } = retired;
    let observations = ingress.owned_pair_inventory(&pair).unwrap();
    if observations.iter().all(|observation| {
        observation.phase == crate::control_runtime::source_positions::SourcePositionPhase::Retired
    }) {
        assert!(
            observations
                .iter()
                .all(|observation| observation.custody.reusable())
        );
        drop(pair);
        drop(funding);
        return;
    }
    for holder in ingress
        .take_unused_owned_pair(&pair)
        .unwrap()
        .into_iter()
        .flatten()
    {
        holder.exit();
    }
    for observation in ingress.owned_pair_inventory(&pair).unwrap() {
        assert!(
            observation.source.is_none(),
            "used sources require their actual Core proof"
        );
        assert!(observation.custody.actual_exit_ready().unwrap());
        assert!(observation.custody.try_retire().unwrap());
    }
    fixture
        .ledger
        .reap_scope_ready(fixture.scope.id(), 256)
        .unwrap();
    let witnesses = ingress
        .prepare_owned_pair_reap(&pair)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    ingress.release_owned_pair(&pair, witnesses).unwrap();
    drop(pair);
    // Funding can refer to a different still-live pair after seal rotation.
    // Dropping these observers returns no reservation or inferred quota.
    drop(funding);
}
fn dispose_unused_admission(
    fixture: &Fixture,
    task: TaskIdentity,
    admission: crate::task_proposal_ingress::CutSourceAdmission,
) {
    let crate::task_proposal_ingress::CutSourceAdmission::Fresh(roles) = admission else {
        panic!("this fixture reserved two original fresh source roles");
    };
    for role in roles {
        let (custody, permit) =
            crate::control_runtime::source_custody::SourceCustody::from_admitted(
                task,
                role,
                std::sync::Arc::new(CausalPending),
                std::sync::Arc::new(
                    novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
                ),
            );
        drop(permit);
        assert!(custody.actual_exit_ready().unwrap());
        assert!(custody.try_retire().unwrap());
        fixture
            .ledger
            .reap_scope_ready(fixture.scope.id(), 256)
            .unwrap();
        assert!(custody.reusable());
    }
}
pub(in crate::control_runtime) fn finish_source_routes(
    fixture: &Fixture,
    ingress: &crate::task_proposal_ingress::TaskProposalIngress,
) {
    use crate::task_proposal_ingress::PairRoute;
    ingress.seal_source_admission();
    for route in [PairRoute::Open, PairRoute::ConsumingOpen] {
        for holder in ingress
            .take_unused_pair(route)
            .unwrap()
            .into_iter()
            .flatten()
        {
            holder.exit();
        }
        for observation in ingress.pair_inventory(route).unwrap() {
            assert!(
                observation.source.is_none(),
                "a retained source must be explicitly disposed first"
            );
            assert!(observation.custody.actual_exit_ready().unwrap());
            assert!(observation.custody.try_retire().unwrap());
        }
        fixture
            .ledger
            .reap_scope_ready(fixture.scope.id(), 256)
            .unwrap();
        let witnesses = ingress
            .prepare_pair_reap(route)
            .unwrap()
            .map(|probe| probe.observe_reaped().unwrap());
        // The complete bank is about to be destroyed, not rearmed. Every
        // original body, report and common Work header really retired first.
        drop(witnesses);
    }
}
const STORAGE_DIMENSIONS: [Dimension; 8] = [
    Dimension::Inputs,
    Dimension::Batches,
    Dimension::Effects,
    Dimension::Cuts,
    Dimension::Replies,
    Dimension::Completions,
    Dimension::Captures,
    Dimension::Work,
];
fn assert_bundle_storage(bundle: &CapacityBundle, expected: StorageNeed) {
    let actual = bundle.resources().unwrap();
    let expected = expected.resources().unwrap();
    for dimension in STORAGE_DIMENSIONS {
        assert_eq!(
            actual.get(dimension.id()),
            expected.get(dimension.id()),
            "the original bundle must retain its exact {dimension:?} payment"
        );
    }
}
pub(in crate::control_runtime) struct Fixture {
    pub(in crate::control_runtime) ledger: RuntimeLedger,
    pub(in crate::control_runtime) scope: ScopeHandle,
    pub(in crate::control_runtime) runtime: ActorRuntime<WorkerActor>,
    pub(in crate::control_runtime) identity: QueryContextRef,
    roles: Vec<(WorkLease, CapacityBundle)>,
    jobs: BTreeMap<ReservationId, (WorkLease, CapacityBundle)>,
    /// Only the fixed-Task funding fixture enables actual dims8..15 payment.
    policy: Option<ControlRuntimePolicy>,
}
impl Fixture {
    pub(in crate::control_runtime) fn new() -> Self {
        Self::with_byte_limits(100)
    }
    pub(in crate::control_runtime) fn with_byte_limits(bytes: u64) -> Self {
        Self::with_admission_storage(bytes, None)
    }
    pub(in crate::control_runtime) fn with_policy_byte_limits(bytes: u64) -> Self {
        Self::with_policy_scope_slot_limit(bytes, None)
    }
    fn with_policy_scope_slot_limit(bytes: u64, scope_slots: Option<usize>) -> Self {
        let policy = ControlRuntimePolicy::try_new(IngressRoleLimits {
            ordinary_running: 1,
            ordinary_waiting: 0,
            control_running: 1,
            control_waiting: 0,
            max_batch_items: 1,
            root_read_holders: 1,
        })
        .unwrap();
        Self::with_scope_slot_limit(bytes, Some(policy), scope_slots)
    }
    fn with_admission_storage(bytes: u64, policy: Option<ControlRuntimePolicy>) -> Self {
        Self::with_scope_slot_limit(bytes, policy, None)
    }
    fn with_scope_slot_limit(
        bytes: u64,
        policy: Option<ControlRuntimePolicy>,
        scope_slots: Option<usize>,
    ) -> Self {
        // A=1 for this fixed-Task funding slice. Shared request/root pools and
        // production Service composition are deliberately not asserted here.
        let task_limit = if policy.is_some() { 1 } else { 2 };
        let limits = ResourceVector::new([
            (Dimension::ActiveTasks.id(), task_limit),
            (Dimension::PreparingTasks.id(), 1),
            (Dimension::PreparingBytes.id(), bytes),
            (Dimension::InboundRecords.id(), 4),
            (Dimension::InboundBytes.id(), bytes),
            (Dimension::Tickets.id(), 1),
            (Dimension::Contexts.id(), 1),
        ])
        .unwrap();
        let (limits, storage, work_headers, report_headers) = match policy {
            Some(policy) => (
                limits
                    .checked_add(&policy.task_base.resources().unwrap())
                    .unwrap(),
                AdmissionStorage {
                    loading: policy.loading.resources().unwrap(),
                    accepted: policy.accepted.resources().unwrap(),
                    context: empty(),
                    ticket: empty(),
                },
                policy.task_base.work.checked_add(3).unwrap(),
                policy.task_base.completions,
            ),
            None => (
                limits,
                AdmissionStorage {
                    loading: empty(),
                    accepted: empty(),
                    context: empty(),
                    ticket: empty(),
                },
                256,
                256,
            ),
        };
        // The three original supervisor/Step headers are context-unit owners,
        // separate from the policy's Task lifetime and fixed Source inventory.
        let ledger = RuntimeLedger::new(limits, work_headers, 2);
        let scope = ledger
            .open_scope_next(
                None,
                generation(),
                work_headers,
                scope_slots.unwrap_or(report_headers),
            )
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
            initial_establishment(),
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
            storage,
            roles[1].0.id(),
            roles[2].0.id(),
        );
        runtime
            .register_supervised_next(
                actor(),
                scope.clone(),
                WorkerActor::Context(Box::new(bound)),
                match policy {
                    Some(policy) => policy.context_actor(1).unwrap(),
                    None => ActorConfig {
                        mailbox: [16, 16, 16, 1],
                        max_batches: 49,
                        max_effects_per_step: ContextCore::MAX_STEP_WORK,
                        max_dependencies_per_effect: ContextCore::MAX_STEP_WORK,
                        turn_budget: 64,
                        max_step_work: ContextCore::MAX_STEP_WORK,
                    },
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
            policy,
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
    fn assert_storage_paid(&self, expected: StorageNeed) {
        assert!(
            self.policy.is_some(),
            "empty-vector protocol fixtures prove no storage payment"
        );
        let resources = expected.resources().unwrap();
        for dimension in STORAGE_DIMENSIONS {
            assert_eq!(
                self.ledger.used(dimension.id()),
                resources.get(dimension.id()),
                "actual fixed Task payment for {dimension:?}"
            );
        }
    }
    /// Elect the real Task through Create and transfer its original LoadInput
    /// guard once. Shared component fixtures must not reserve a substitute Task.
    pub(in crate::control_runtime) fn adopt_task_host(
        &mut self,
        task: TaskIdentity,
        max_cuts: usize,
    ) -> crate::control_runtime::task::TaskHostRecord {
        use crate::control_runtime::task::{TaskHostBinding, TaskHostRecord};
        let create = self.send(
            ContextInput::Create {
                request: operation(1),
                task,
                payload: PayloadId(1),
            },
            Some(operation(1)),
            InputClass::Ordinary,
            2,
        );
        self.runtime.poll_one(Tick(1)).unwrap();
        let load = self.runtime.ready_effects(actor(), create, 15).unwrap()[0];
        let ticket = self.runtime.take_effect(actor(), create, load).unwrap();
        let host = TaskHostRecord::adopt_loading(
            &self.runtime,
            &ticket,
            TaskHostBinding {
                context: self.identity,
                task_source: ActorId::new(2).unwrap(),
                protocol_generation: generation(),
                max_cuts,
                pending: std::sync::Arc::new(CausalPending),
                health: std::sync::Arc::new(
                    novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
                ),
            },
        )
        .unwrap();
        self.runtime.effect_completed(ticket).unwrap();
        self.drain(create);
        host
    }
    pub(in crate::control_runtime) fn send(
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
    pub(in crate::control_runtime) fn drain(&mut self, input: InputId) -> Vec<WorkerAction> {
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
                                .acknowledge_dependency(target, input, descriptor.id, predecessor)
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
                    effect: novarocks_worker_protocol::control::ControlEffect::RetireReservation(id),
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
    /// Dispose only the original jobs taken from real Core reservations.
    /// Task lifetime join tests must observe this child exit before closing.
    pub(in crate::control_runtime) fn finish_jobs(&mut self) {
        for (_, (mut work, mut capacity)) in std::mem::take(&mut self.jobs) {
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            self.ledger.reap_scope_ready(work.scope().id(), 32).unwrap();
            capacity.release(&capacity.resources().unwrap()).unwrap();
        }
    }
    pub(in crate::control_runtime) fn finish(mut self) {
        self.finish_jobs();
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
#[test]
fn source_bootstrap_slot_refusal_returns_the_original_paid_unpublished_task_until_actual_exit() {
    use crate::control_runtime::task::{TaskHostBinding, TaskHostError, TaskHostRecord};
    use novarocks_control_runtime::transaction::RuntimeError;
    use novarocks_control_runtime_core::scope::ScopeError;
    use std::sync::Arc;

    // One original Actor attachment plus three Source reports fit. Source4's
    // fourth report fails without changing the actual Task funding policy.
    let mut f = Fixture::with_policy_scope_slot_limit(100, Some(4));
    let policy = f.policy.unwrap();
    let task = task(f.identity, 1);
    let original = f.scope.snapshot().unwrap();
    assert_eq!(original.storage, 1);
    assert_eq!(original.unresolved, 0);
    f.assert_storage_paid(StorageNeed::default());
    let create = f.send(
        ContextInput::Create {
            request: operation(1),
            task,
            payload: PayloadId(1),
        },
        Some(operation(1)),
        InputClass::Ordinary,
        2,
    );
    f.runtime.poll_one(Tick(1)).unwrap();
    let action = f.runtime.ready_effects(actor(), create, 15).unwrap()[0];
    let ticket = f.runtime.take_effect(actor(), create, action).unwrap();
    let reservation = match ticket.effect {
        WorkerAction::Context(ContextEffect::LoadInput { reservation, .. }) => reservation,
        _ => panic!("the actual elected Task's LoadInput effect"),
    };
    let task_work = ticket.descriptor.work;
    let elected = f.scope.snapshot().unwrap();
    assert_eq!(elected.work, original.work + 1);
    f.assert_storage_paid(policy.loading);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    let refusal = TaskHostRecord::adopt_loading(
        &f.runtime,
        &ticket,
        TaskHostBinding {
            context: f.identity,
            task_source: ActorId::new(2).unwrap(),
            protocol_generation: generation(),
            max_cuts: 1,
            pending: Arc::new(CausalPending),
            health: Arc::new(
                novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
            ),
        },
    )
    .err()
    .expect("the actual fourth Source report exceeds this Scope's slot capacity");
    assert!(matches!(
        refusal.error,
        TaskHostError::Runtime(RuntimeError::Scope(ScopeError::SlotCapacity))
    ));
    let mut unpublished = refusal
        .unpublished
        .expect("the bound original Task is returned, never dropped");
    assert_eq!(unpublished.task, task);
    assert_eq!(unpublished.lifetime.id(), task_work);
    assert_eq!(unpublished.active.id(), reservation);
    assert_eq!(
        unpublished.active.resources().unwrap(),
        policy
            .loading
            .resources()
            .unwrap()
            .checked_add(&ResourceVector::new([(Dimension::ActiveTasks.id(), 1)]).unwrap())
            .unwrap()
    );
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    f.assert_storage_paid(policy.loading);
    let partial = f.scope.snapshot().unwrap();
    assert_eq!(
        partial.storage, original.storage,
        "only truly unused reports were revoked and retired"
    );
    assert_eq!(
        partial.work,
        elected.work + 3,
        "three bound Source children await the actual common reaper"
    );
    assert_eq!(
        partial.unresolved, 0,
        "a recoverable bootstrap refusal is not Unknown Work"
    );
    assert!(f.ledger.unresolved_work().is_empty());
    let observer = unpublished.lifetime.retirement_observer();
    assert!(!observer.is_retired());
    assert!(!unpublished.lifetime.actual_exit_ready().unwrap());
    assert!(matches!(
        f.runtime.take_step_reservation(&ticket, reservation),
        Err(DriverError::UnknownReservation)
    ));
    f.runtime.effect_completed(ticket).unwrap();
    f.drain(create);

    // No input, loader callback, E, or host capture was ever attached. The
    // successful partial Source roles exited via their real unused path;
    // reaping those exact children still does not release the original payer.
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 3).unwrap(), 3);
    assert_eq!(f.scope.snapshot().unwrap().work, elected.work);
    assert!(!observer.is_retired());
    f.assert_storage_paid(policy.loading);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    unpublished.lifetime.body_exited().unwrap();
    assert!(!unpublished.lifetime.actual_exit_ready().unwrap());
    unpublished.lifetime.captures_exited().unwrap();
    assert!(unpublished.lifetime.actual_exit_ready().unwrap());
    assert!(!observer.is_retired());
    f.assert_storage_paid(policy.loading);
    let resources = unpublished.active.resources().unwrap();
    unpublished.active.release(&resources).unwrap();
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 0);
    f.assert_storage_paid(StorageNeed::default());
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    assert!(observer.is_retired());
    assert_eq!(f.scope.snapshot().unwrap().work, original.work);
    assert_eq!(f.scope.snapshot().unwrap().unresolved, 0);
    drop(unpublished);
    f.finish();
}

#[test]
fn causal_loader_partial_source_refusal_returns_unread_input_and_reaps_only_unused_original_roles()
{
    use crate::control_runtime::{
        causal::{CausalError, CausalSourceRetirement},
        loading::TaskCreatePayload,
        task::{TaskHostError, TaskHostResources},
    };
    use novarocks_control_runtime::{exit_role::ExitRolePhase, transaction::RuntimeError};
    use novarocks_control_runtime_core::scope::ScopeError;
    use novarocks_execution_contract::{CreationContent, FrozenBytes, TaskCreationInput};
    use std::{
        any::Any,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[derive(Debug)]
    struct Assignment {
        reads: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl CreationContent for Assignment {
        fn encoded_len(&self) -> usize {
            self.reads.fetch_add(1, Ordering::SeqCst);
            5
        }
        fn retained_bytes(&self) -> usize {
            self.reads.fetch_add(1, Ordering::SeqCst);
            7
        }
        fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }
    impl Drop for Assignment {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    // Source4 plus the Actor attachment use five original Scope slots. The
    // loader/input reports use two, the first cut's Source2 use two, and only
    // the second cut's first Source can fit in the tenth and final position.
    let mut f = Fixture::with_policy_scope_slot_limit(4096, Some(10));
    let policy = f.policy.unwrap();
    let task = task(f.identity, 1);
    let context_headers = f.scope.snapshot().unwrap().work;
    let mut host = f.adopt_task_host(task, 2);
    let elected = f.scope.snapshot().unwrap();
    assert_eq!(elected.storage, 5);
    assert_eq!(elected.work, context_headers + 5);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    f.assert_storage_paid(policy.loading);

    let reads = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let assignment = Box::new(Assignment {
        reads: reads.clone(),
        drops: drops.clone(),
    });
    let assignment_pointer = std::ptr::from_ref(assignment.as_ref());
    let frozen = FrozenBytes::freeze(bytes::Bytes::from(vec![3, 5, 7]));
    let input = TaskCreationInput::new(frozen.clone(), assignment);
    let descriptor = novarocks_execution_contract::TaskDescriptor::try_new(
        task,
        novarocks_types::UniqueId::new(1, 1),
        std::num::NonZeroUsize::new(1).unwrap(),
        vec![],
        novarocks_execution_contract::ExchangeTopology::try_new(vec![], vec![]).unwrap(),
    )
    .unwrap();
    let request = Arc::new(
        novarocks_execution_contract::CreateTask::try_new(
            operation(1),
            f.identity,
            descriptor,
            vec![],
        )
        .unwrap(),
    );
    let payload = TaskCreatePayload {
        id: PayloadId(1),
        request: request.clone(),
        input,
        ingress_retained_bytes: 2,
    };
    let (error, returned) = match host.begin_host_loader(payload) {
        Err(refused) => refused,
        Ok(_) => panic!("the second Source2 must exceed the actual tenth Scope slot"),
    };
    assert!(matches!(
        error,
        TaskHostError::Runtime(RuntimeError::Scope(ScopeError::SlotCapacity))
    ));
    assert_eq!(returned.id, PayloadId(1));
    assert_eq!(returned.ingress_retained_bytes, 2);
    assert!(Arc::ptr_eq(&request, &returned.request));
    assert!(
        returned
            .input
            .static_fragment()
            .shares_backing_with(&frozen)
    );
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(host.loader().is_none());
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    f.assert_storage_paid(policy.loading);
    let refused = f.scope.snapshot().unwrap();
    assert_eq!(refused.storage, 7);
    assert_eq!(
        refused.work,
        elected.work + 5,
        "loader/input, the first Source2 and one partial Source await their actual reapers"
    );
    assert_eq!(refused.unresolved, 0);
    assert!(f.ledger.unresolved_work().is_empty());
    assert!(!f.runtime.snapshot(actor()).unwrap().failed);

    let port = host.port();
    let cuts = host.causal().cut_inventory(None, 2);
    assert_eq!(cuts.len(), 1, "only the first original author was admitted");
    let cut = cuts[0];
    assert!(port.ingress().event_unused(cut).unwrap());
    let funding = port.ingress().cut_funding(cut).unwrap();
    for custody in &funding.custody {
        assert_eq!(custody.phase(), ExitRolePhase::Admitted);
        assert!(!custody.actual_exit_ready().unwrap());
        assert!(!custody.reusable());
    }
    assert!(
        !f.runtime
            .role_inventory(actor(), None, 32)
            .unwrap()
            .is_empty()
    );

    // The factory's unused loader/input and partial Source1 really exited.
    // Their reports have retired; only the original common reaper can remove
    // their three Work headers. The admitted first cut remains in this Task.
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 3).unwrap(), 3);
    assert_eq!(f.scope.snapshot().unwrap().work, elected.work + 2);
    assert_eq!(f.scope.snapshot().unwrap().storage, 7);
    f.assert_storage_paid(policy.loading);
    let TaskHostResources {
        mut lifetime,
        mut active,
        port: owned_port,
        loader,
        loading_events,
        loading_return,
        pending_loading,
        preparation,
        preparation_remainder,
        completion,
        completion_action,
        lifecycle_inputs,
        open_summary,
        mut causal,
    } = host.into_resources();
    assert_eq!(
        active.resources().unwrap(),
        policy
            .loading
            .resources()
            .unwrap()
            .checked_add(&ResourceVector::new([(Dimension::ActiveTasks.id(), 1)]).unwrap())
            .unwrap(),
        "the elected Task keeps one original loading payer"
    );
    assert!(Arc::ptr_eq(&port, &owned_port));
    assert!(loader.is_none());
    assert!(loading_events.is_none());
    assert!(loading_return.is_none());
    assert!(pending_loading.is_none());
    assert!(preparation.is_none());
    assert!(preparation_remainder.is_none());
    assert!(completion.is_none());
    assert!(completion_action.is_none());
    assert!(lifecycle_inputs.is_empty());
    assert!(open_summary.is_none());
    assert!(!lifetime.actual_exit_ready().unwrap());

    // This author was revoked unused before attaching any loader capture or
    // effect. Its original loader/input Work headers were actually reaped
    // above; record capture exit and observe the same unused paid role's
    // actual effect exit separately from any Core acknowledgement.
    port.ingress().observe_cut_exit(cut, true, false).unwrap();
    assert!(causal.observe_effects_exited(cut).unwrap());
    let retired = causal
        .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
        .unwrap();
    assert!(causal.is_empty());
    assert!(matches!(
        causal.observe_effects_exited(cut),
        Err(CausalError::UnknownRole)
    ));
    assert!(
        f.runtime
            .role_inventory(actor(), None, 32)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        retired.pair.positions(),
        funding.keys.each_ref().map(|key| key.address())
    );
    for (original, returned) in funding.custody.iter().zip(&retired.funding.custody) {
        assert_eq!(original.report().id(), returned.report().id());
        assert_eq!(returned.phase(), ExitRolePhase::Admitted);
    }
    let unused = port
        .ingress()
        .take_unused_owned_pair(&retired.pair)
        .unwrap();
    assert!(unused.iter().all(Option::is_some));
    for holder in unused.into_iter().flatten() {
        holder.exit();
    }
    for custody in &retired.funding.custody {
        assert!(custody.actual_exit_ready().unwrap());
        assert!(custody.try_retire().unwrap());
        assert_eq!(custody.phase(), ExitRolePhase::Retired);
        assert!(
            !custody.reusable(),
            "a retired report is not a reaped Work header"
        );
    }
    assert_eq!(f.scope.snapshot().unwrap().storage, 5);
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 2).unwrap(), 2);
    assert!(
        retired
            .funding
            .custody
            .iter()
            .all(|custody| custody.reusable())
    );
    assert!(funding.custody.iter().all(|custody| custody.reusable()));
    let reaped = port
        .ingress()
        .prepare_owned_pair_reap(&retired.pair)
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    port.ingress()
        .release_owned_pair(&retired.pair, reaped)
        .unwrap();
    drop(retired);
    drop(funding);
    drop(causal);
    assert_eq!(f.scope.snapshot().unwrap().work, elected.work);
    f.assert_storage_paid(policy.loading);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);

    // This unit destroys an empty bank after its real original Source4 exit;
    // it does not forge Loaded/LoaderExited or any protocol read-seal fact.
    finish_source_routes(&f, port.ingress());
    assert_eq!(f.scope.snapshot().unwrap().storage, 1);
    assert_eq!(f.scope.snapshot().unwrap().work, context_headers + 1);
    assert!(!lifetime.actual_exit_ready().unwrap());
    let TaskCreatePayload {
        input,
        request: returned_request,
        ..
    } = returned;
    let (returned_frozen, assignment) = input.into_parts();
    assert!(returned_frozen.shares_backing_with(&frozen));
    let assignment = assignment.into_stored().downcast::<Assignment>().unwrap();
    assert_eq!(std::ptr::from_ref(assignment.as_ref()), assignment_pointer);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(assignment);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(returned_frozen);
    drop(frozen);
    drop(returned_request);
    drop(request);
    drop(owned_port);
    drop(port);
    lifetime.body_exited().unwrap();
    assert!(!lifetime.actual_exit_ready().unwrap());
    lifetime.captures_exited().unwrap();
    assert!(lifetime.actual_exit_ready().unwrap());
    f.assert_storage_paid(policy.loading);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    active.release(&active.resources().unwrap()).unwrap();
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 0);
    f.assert_storage_paid(StorageNeed::default());
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    assert_eq!(f.scope.snapshot().unwrap().work, context_headers);
    drop(lifetime);
    drop(active);
    f.finish();
}

#[test]
fn actual_fair_delivery_uses_each_envelope_token_even_when_issuance_order_differs() {
    let mut f = Fixture::new();
    let first_task = task(f.identity, 1);
    let second_task = task(f.identity, 2);
    let first = f.send(
        ContextInput::Create {
            request: operation(1),
            task: first_task,
            payload: PayloadId(1),
        },
        Some(operation(1)),
        InputClass::Ordinary,
        2,
    );
    let second = f.send(
        ContextInput::Create {
            request: operation(2),
            task: second_task,
            payload: PayloadId(2),
        },
        Some(operation(2)),
        InputClass::Control,
        3,
    );
    assert!(second > first);
    f.runtime.poll_one(Tick(1)).unwrap();
    assert!(!f.runtime.snapshot(actor()).unwrap().failed);
    let a = f.drain(first);
    let b = f.drain(second);
    assert!(
        matches!(a.as_slice(), [WorkerAction::Context(ContextEffect::LoadInput { task, .. })] if *task == first_task)
    );
    assert!(
        matches!(b.as_slice(), [WorkerAction::Context(ContextEffect::LoadInput { task, .. })] if *task == second_task)
    );
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 2);
    let mut replied = Vec::new();
    for token in [first, second] {
        let identity = if token == first {
            first_task
        } else {
            second_task
        };
        let outputs = f.run(
            ContextInput::Loaded {
                task: identity,
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
        replied.extend(outputs.into_iter().filter_map(|effect| {
            if let WorkerAction::Reply(reply) = effect {
                Some(reply)
            } else {
                None
            }
        }));
    }
    assert_eq!(replied.len(), 2);
    assert_eq!(replied[0].request.token, first);
    assert_eq!(
        replied[0].result,
        RequestResult::Outcome(OperationOutcome::Accepted)
    );
    assert_eq!(replied[1].request.token, second);
    assert_eq!(
        replied[1].result,
        RequestResult::Outcome(OperationOutcome::PreparationBusy)
    );
    f.finish();
}
#[test]
fn accepted_preparation_keeps_its_original_bundle_after_the_body_fact() {
    let mut f = Fixture::new();
    let identity = task(f.identity, 1);
    f.run(
        ContextInput::Create {
            request: operation(1),
            task: identity,
            payload: PayloadId(1),
        },
        Some(operation(1)),
    );
    let outputs = f.run(
        ContextInput::Loaded {
            task: identity,
            result: Ok(CreationCost {
                input_retained_bytes: 5,
                ingress_retained_bytes: 7,
                structural_bytes: 11,
                inbound_records: 1,
                inbound_bytes: 13,
            }),
        },
        None,
    );
    assert!(outputs.iter().any(|action| matches!(action, WorkerAction::Reply(reply) if reply.result == RequestResult::Outcome(OperationOutcome::Accepted))));
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 1);
    assert_eq!(f.ledger.used(Dimension::PreparingBytes.id()), 23);
    assert_eq!(f.ledger.used(Dimension::InboundBytes.id()), 13);
    f.run(
        ContextInput::Physical {
            task: identity,
            fact: PhysicalFact::LoaderExited,
        },
        None,
    );
    f.run(
        ContextInput::Physical {
            task: identity,
            fact: PhysicalFact::PreparationBodyExited,
        },
        None,
    );
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 1);
    assert_eq!(f.ledger.used(Dimension::PreparingBytes.id()), 23);
    assert_eq!(f.ledger.used(Dimension::InboundRecords.id()), 1);
    f.finish();
}
#[test]
fn failed_core_retains_unstarted_original_reservations_for_the_fixed_cleanup_owner() {
    let mut f = Fixture::new();
    let identity = task(f.identity, 1);
    let earlier = f.send(
        ContextInput::Create {
            request: operation(1),
            task: identity,
            payload: PayloadId(1),
        },
        Some(operation(1)),
        InputClass::Ordinary,
        2,
    );
    f.runtime.poll_one(Tick(1)).unwrap();
    let (reservation, _) = f
        .runtime
        .step_reservations(actor(), earlier, None, 1)
        .unwrap()[0];
    let mut dispatch = crate::control_runtime::effect_dispatch::EffectDispatch::new(
        actor(),
        ContextCore::MAX_STEP_WORK,
    );
    dispatch.index_committed(&f.runtime, 16).unwrap();
    assert!(!dispatch.is_empty());
    assert!(dispatch.clear_failed_index(&f.runtime).is_err());
    let failed = f.send(
        ContextInput::Installed {
            task: task(f.identity, 999),
        },
        None,
        InputClass::Lifecycle,
        2,
    );
    f.runtime.poll_one(Tick(1)).unwrap();
    assert!(f.runtime.snapshot(actor()).unwrap().failed);
    assert!(matches!(
        dispatch.take_ready(&f.runtime, 16),
        Err(crate::control_runtime::effect_dispatch::DispatchError::OwnerFailed)
    ));
    dispatch.clear_failed_index(&f.runtime).unwrap();
    assert!(dispatch.is_empty());
    assert_eq!(
        f.runtime
            .step_reservations(actor(), earlier, None, 1)
            .unwrap()
            .len(),
        1,
        "clearing a routing index cannot release the runtime's actual guard"
    );
    let failure = f
        .runtime
        .take_effect(actor(), failed, ActionId::new(1).unwrap())
        .unwrap();
    assert_eq!(failure.descriptor.work, f.roles[0].0.id());
    assert!(
        matches!(failure.effect, WorkerAction::OwnerFailed { scope, generation } if scope == f.scope.id() && generation.get() == 2)
    );
    f.roles[0].0.start().unwrap();
    f.runtime.effect_completed(failure).unwrap();
    f.runtime.retire_input(actor(), failed).unwrap();
    let descriptor = f
        .runtime
        .effect_inventory(actor(), earlier, None, 1)
        .unwrap()[0]
        .0
        .clone();
    assert_eq!(
        f.runtime.take_effect(actor(), earlier, descriptor.id).err(),
        Some(DriverError::Effect(
            novarocks_control_runtime_core::effect::EffectError::NotReady
        ))
    );
    f.runtime
        .abandon_unstarted_effect(actor(), earlier, descriptor.id)
        .unwrap();
    assert_eq!(
        f.runtime.retire_input(actor(), earlier),
        Err(DriverError::ReservationsRemain)
    );
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    drop(
        f.runtime
            .take_failed_reservation(actor(), earlier, reservation)
            .unwrap(),
    );
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 0);
    f.runtime.retire_input(actor(), earlier).unwrap();
    f.finish();
}

#[test]
fn logical_context_close_does_not_invalidate_the_runtime_responsibility_generation() {
    let mut f = Fixture::new();
    let identity = task(f.identity, 1);
    f.run(
        ContextInput::Create {
            request: operation(1),
            task: identity,
            payload: PayloadId(1),
        },
        Some(operation(1)),
    );
    f.run(
        ContextInput::Loaded {
            task: identity,
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
    let aborted = f.run(
        ContextInput::Abort {
            request: operation(2),
            cause: novarocks_execution_contract::AbortCause::QueryFailed,
        },
        Some(operation(2)),
    );
    let close = aborted
        .iter()
        .find_map(|action| match action {
            WorkerAction::Context(ContextEffect::ContinueClose { generation }) => Some(*generation),
            _ => None,
        })
        .unwrap();
    assert_eq!(close.get(), 2);
    let outputs = f.run(ContextInput::ContinueClose, None);
    assert!(outputs.iter().any(|action| matches!(action,
        WorkerAction::Context(ContextEffect::ApplyTaskClose { task, generation }) if *task == identity && *generation == close)));
    let outputs = f.run(
        ContextInput::ApplyTaskClose {
            task: identity,
            generation: close,
        },
        None,
    );
    assert!(outputs.iter().any(|action| matches!(action,
        WorkerAction::Context(ContextEffect::StopTask { task, generation }) if *task == identity && *generation == close)));
    assert_eq!(
        f.runtime.snapshot(actor()).unwrap().generation,
        generation()
    );
    assert!(!f.runtime.snapshot(actor()).unwrap().failed);
    // The host still owns all accepted physical work after logical shutdown.
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 1);
    f.finish();
}

#[test]
fn process_control_and_context_share_one_scheduler_with_independent_original_custody() {
    use novarocks_execution_contract::{
        AcquireQueryContextAdmissionTicket, AdmissionTicketId, ContentFingerprint, CredentialEpoch,
        CredentialLeaseId, QueryContextState,
    };
    use novarocks_types::NativeCompatibilityId;
    use novarocks_worker_protocol::{
        admission::AdmissionTicketConfig,
        control::{
            ControlConfig, ControlCore, ControlEffect, ControlInput, EstablishSemanticFacts,
        },
    };
    let mut f = Fixture::new();
    let identity = task(f.identity, 1);
    f.run(
        ContextInput::Create {
            request: operation(1),
            task: identity,
            payload: PayloadId(1),
        },
        Some(operation(1)),
    );
    f.run(
        ContextInput::Loaded {
            task: identity,
            result: Ok(CreationCost {
                input_retained_bytes: 5,
                ingress_retained_bytes: 7,
                structural_bytes: 11,
                inbound_records: 1,
                inbound_bytes: 13,
            }),
        },
        None,
    );
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 1);
    let control_scope = f
        .ledger
        .open_scope_next(None, generation(), 16, 16)
        .unwrap();
    let mut roles = Vec::new();
    for kind in [WorkKind::Cleanup, WorkKind::Step, WorkKind::Step] {
        let (mut work, capacity) = control_scope
            .reserve_next(generation(), empty(), kind, WorkAdmission::External)
            .unwrap()
            .bind(kind)
            .unwrap();
        if kind != WorkKind::Cleanup {
            work.start().unwrap();
        }
        roles.push((work, capacity));
    }
    let core = ControlCore::new(
        ControlConfig {
            max_contexts: 3,
            max_waiters: 4,
            max_pending_requests: 8,
            step_budget: ControlCore::MAX_STEP_WORK,
            retained_contexts: 2,
            gone_fences: 3,
            retention: Duration::from_secs(10),
        },
        AdmissionTicketConfig::DEFAULT,
        [1; 8],
        MonotonicInstant::ORIGIN,
    )
    .unwrap();
    let capability = core.admission().capability();
    let control = ControlActor::new(
        core,
        control_scope.clone(),
        AdmissionStorage {
            loading: empty(),
            accepted: empty(),
            context: empty(),
            ticket: empty(),
        },
        roles[1].0.id(),
    );
    let target = ActorId::new(2).unwrap();
    f.runtime
        .register_supervised_next(
            target,
            control_scope.clone(),
            WorkerActor::Control(Box::new(control)),
            ActorConfig {
                mailbox: [8, 8, 8, 1],
                max_batches: 25,
                max_effects_per_step: ControlCore::MAX_STEP_WORK,
                max_dependencies_per_effect: ControlCore::MAX_STEP_WORK,
                turn_budget: 64,
                max_step_work: ControlCore::MAX_STEP_WORK,
            },
            &roles[0].0,
        )
        .unwrap_or_else(|(e, _)| panic!("control registration: {e:?}"));
    // This is another attempt. Its control work must remain eligible even
    // while the first context retains all physical preparation positions.
    let c = f.identity;
    let next = QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(7401, 7402), AttemptId::new(1).unwrap()).unwrap(),
        c.frontend_process_id(),
        c.backend_process_id(),
    );
    let ticket_id = AdmissionTicketId::try_from_bytes([4; 16]).unwrap();
    let send = |f: &mut Fixture, command, request: Option<TaskOperationId>| {
        let materialized = matches!(&command, ControlInput::EstablishmentDecided { .. });
        let input = f
            .runtime
            .reserve_input(target, ActorId::new(3).unwrap(), InputClass::Ordinary)
            .unwrap()
            .fill(WorkerMessage::Control(ControlMessage::Command {
                work: roles[2].0.id(),
                request: request.map(|operation| RequestMetadata {
                    operation,
                    deadline: MonotonicInstant::from_origin(Duration::from_secs(10)),
                }),
                command,
            }))
            .unwrap();
        f.runtime.poll_one(Tick(1)).unwrap();
        if materialized {
            let ready = f
                .runtime
                .ready_effects(target, input, ControlCore::MAX_STEP_WORK)
                .unwrap();
            assert_eq!(
                ready.len(),
                1,
                "the establish reply must wait for actual route publication"
            );
            assert!(matches!(
                f.runtime
                    .pending_effect_metadata(target, input, ready[0])
                    .unwrap(),
                Some(WorkerAction::Control {
                    effect: ControlEffect::Route {
                        state: QueryContextState::Active,
                        ..
                    },
                    ..
                })
            ));
        }
        f.drain_for(target, input)
    };
    let request = AcquireQueryContextAdmissionTicket::new(
        operation(3),
        next,
        LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        NativeCompatibilityId::new([3; 32]),
        capability,
    );
    let outputs = send(
        &mut f,
        ControlInput::Acquire {
            request,
            ticket: ticket_id,
        },
        Some(operation(3)),
    );
    let reservation = outputs
        .iter()
        .find_map(|action| match action {
            WorkerAction::Control {
                effect: ControlEffect::AdmissionReply { result: Ok(_), .. },
                admitted: Some(id),
            } => Some(*id),
            _ => None,
        })
        .expect("the ticket grant body carries its original synchronous reservation");
    assert_eq!(f.jobs[&reservation].0.scope().id(), control_scope.id());
    assert_eq!(f.ledger.used(Dimension::Tickets.id()), 1);
    assert_eq!(f.ledger.used(Dimension::PreparingBytes.id()), 23);
    let semantics = EstablishSemanticFacts {
        catalog: ContentFingerprint::from_bytes([1; 16]),
        shared_filter: ContentFingerprint::from_bytes([2; 16]),
        query_options: ContentFingerprint::from_bytes([3; 16]),
        credential_lease: CredentialLeaseId::new(1),
        credential_epoch: CredentialEpoch::new(1).unwrap(),
        initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    };
    send(
        &mut f,
        ControlInput::Establish {
            request: operation(4),
            context: next,
            ticket: ticket_id,
            payload: PayloadId(2),
            semantics,
            generation: generation(),
        },
        Some(operation(4)),
    );
    assert_eq!(f.ledger.used(Dimension::Contexts.id()), 1);
    send(
        &mut f,
        ControlInput::EstablishmentDecided {
            context: next,
            decision: novarocks_worker_protocol::context::ContextEstablishmentDecision {
                state: novarocks_execution_contract::QueryContextState::Active,
                outcome: Ok(OperationOutcome::Accepted),
            },
        },
        None,
    );
    // A broken context actor loses only its own scope, including uncompleted
    // physical work. The same runtime keeps the process actor live.
    let failed = f.run(ContextInput::Installed { task: task(c, 999) }, None);
    assert!(
        failed
            .iter()
            .any(|effect| matches!(effect, WorkerAction::OwnerFailed { .. }))
    );
    assert!(f.runtime.snapshot(actor()).unwrap().failed);
    assert!(!f.runtime.snapshot(target).unwrap().failed);
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 1);
    let outputs = send(&mut f, ControlInput::Retained { context: next }, None);
    assert!(outputs.iter().any(|action| matches!(
        action,
        WorkerAction::Control {
            effect: ControlEffect::Route {
                state: QueryContextState::TerminalRetained,
                ..
            },
            ..
        }
    )));
    assert_eq!(f.ledger.used(Dimension::Tickets.id()), 0);
    assert_eq!(f.ledger.used(Dimension::Contexts.id()), 1);
    send(&mut f, ControlInput::HostRemoved { context: next }, None);
    assert_eq!(f.ledger.used(Dimension::Contexts.id()), 0);
    for (mut work, mut capacity) in roles {
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        f.ledger.reap_scope_ready(control_scope.id(), 16).unwrap();
        capacity.release(&capacity.resources().unwrap()).unwrap();
    }
    while let Some(id) = f.runtime.claim_effect_actor() {
        f.runtime.release_effect_actor(id, false).unwrap();
    }
    control_scope.close_external().unwrap();
    f.runtime
        .remove_actor(target)
        .unwrap_or_else(|e| panic!("control removal: {e:?}"));
    f.ledger.remove_scope(control_scope.id()).unwrap();
    f.finish();
}

#[test]
fn host_publications_follow_core_order_without_waiting_for_old_body_capture_retirement() {
    use crate::control_runtime::effect_dispatch::{DispatchError, EffectDispatch};
    use novarocks_control_runtime::publication::PublishedSnapshot;
    use novarocks_execution_contract::{TaskState, TaskStatus};
    use std::sync::Arc;
    let mut f = Fixture::new();
    let identity = task(f.identity, 1);
    f.run(
        ContextInput::Create {
            request: operation(1),
            task: identity,
            payload: PayloadId(1),
        },
        Some(operation(1)),
    );
    f.run(
        ContextInput::Loaded {
            task: identity,
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
    let installed = f.send(
        ContextInput::Installed { task: identity },
        None,
        InputClass::Ordinary,
        2,
    );
    let running = f.send(
        ContextInput::Running { task: identity },
        None,
        InputClass::Lifecycle,
        2,
    );
    f.runtime.poll_one(Tick(1)).unwrap();
    let mut dispatch = EffectDispatch::new(actor(), ContextCore::MAX_STEP_WORK * 2);
    assert!(
        dispatch
            .index_committed(&f.runtime, 1)
            .unwrap()
            .inputs
            .is_empty()
    );
    assert_eq!(
        dispatch.index_committed(&f.runtime, 32).unwrap().inputs,
        vec![installed, running]
    );
    let first = dispatch.take_ready(&f.runtime, 8).unwrap().unwrap();
    assert_eq!(first.input, installed);
    assert!(matches!(
        dispatch.publication_committed(&f.runtime, &first),
        Err(DispatchError::InvalidPublication)
    ));
    assert!(
        dispatch.take_ready(&f.runtime, 8).unwrap().is_none(),
        "the later snapshot cannot overtake its original predecessor"
    );
    let target = PublishedSnapshot::new(&f.scope, Arc::new(TaskStatus::created(identity)));
    let WorkerAction::Context(ContextEffect::PublishTask { snapshot, .. }) = &first.effect else {
        panic!("installed snapshot effect")
    };
    let previous = f
        .runtime
        .commit_snapshot(&first, &target, Arc::new(snapshot.clone()))
        .unwrap();
    drop(previous);
    dispatch.publication_committed(&f.runtime, &first).unwrap();
    assert_eq!(
        f.runtime.retire_input(actor(), installed),
        Err(DriverError::EffectsRemain)
    );
    let second = dispatch.take_ready(&f.runtime, 8).unwrap().unwrap();
    assert_eq!(second.input, running);
    let WorkerAction::Context(ContextEffect::PublishTask { snapshot, .. }) = &second.effect else {
        panic!("running snapshot effect")
    };
    let previous = f
        .runtime
        .commit_snapshot(&second, &target, Arc::new(snapshot.clone()))
        .unwrap();
    drop(previous);
    dispatch.publication_committed(&f.runtime, &second).unwrap();
    assert_eq!(target.read().state(), TaskState::Running);
    assert!(
        dispatch.is_empty(),
        "ordering is released by actual publication, not original body exit"
    );
    // Independent actual body/capture owners can finish in the reverse order.
    f.runtime.effect_completed(second).unwrap();
    f.runtime.retire_input(actor(), running).unwrap();
    f.runtime.effect_completed(first).unwrap();
    f.runtime.retire_input(actor(), installed).unwrap();
    f.finish();
}

#[test]
fn actual_conclusion_and_status_publication_precede_stop_dispatch_and_reply_visibility() {
    use crate::control_runtime::effect_dispatch::EffectDispatch;
    use novarocks_control_runtime::publication::{PublishedReply, PublishedSnapshot};
    use novarocks_execution_contract::{CancelReason, TaskConclusion, TaskState, TaskStatus};
    use std::sync::Arc;
    let mut f = Fixture::new();
    let identity = task(f.identity, 1);
    f.run(
        ContextInput::Create {
            request: operation(1),
            task: identity,
            payload: PayloadId(1),
        },
        Some(operation(1)),
    );
    f.run(
        ContextInput::Loaded {
            task: identity,
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
    let input = f.send(
        ContextInput::Cancel {
            request: operation(2),
            task: identity,
            reason: CancelReason::UpstreamNoLongerNeeded,
        },
        Some(operation(2)),
        InputClass::Control,
        2,
    );
    f.runtime.poll_one(Tick(1)).unwrap();
    let settled = f.send(
        ContextInput::Settle {
            task: identity,
            work: f.roles[2].0.id(),
        },
        None,
        InputClass::Lifecycle,
        2,
    );
    f.runtime.poll_one(Tick(2)).unwrap();
    let mut dispatch = EffectDispatch::new(actor(), ContextCore::MAX_STEP_WORK);
    dispatch.index_committed(&f.runtime, 32).unwrap();
    let conclusion = dispatch.take_ready(&f.runtime, 16).unwrap().unwrap();
    assert!(matches!(
        conclusion.effect,
        WorkerAction::Context(ContextEffect::PublishConclusion { .. })
    ));
    assert!(
        dispatch.take_ready(&f.runtime, 16).unwrap().is_none(),
        "neither stop nor the accepted reply may precede actual publication"
    );
    let accepted = PublishedSnapshot::new(&f.scope, Arc::new(None::<TaskConclusion>));
    let WorkerAction::Context(ContextEffect::PublishConclusion {
        conclusion: value, ..
    }) = &conclusion.effect
    else {
        unreachable!()
    };
    drop(
        f.runtime
            .commit_snapshot(&conclusion, &accepted, Arc::new(Some(value.clone())))
            .unwrap(),
    );
    dispatch
        .publication_committed(&f.runtime, &conclusion)
        .unwrap();
    let status = dispatch.take_ready(&f.runtime, 16).unwrap().unwrap();
    let WorkerAction::Context(ContextEffect::PublishTask { snapshot, .. }) = &status.effect else {
        panic!("Task status must follow the conclusion")
    };
    let current = PublishedSnapshot::new(&f.scope, Arc::new(TaskStatus::created(identity)));
    drop(
        f.runtime
            .commit_snapshot(&status, &current, Arc::new(snapshot.clone()))
            .unwrap(),
    );
    dispatch.publication_committed(&f.runtime, &status).unwrap();
    assert!(accepted.read().is_some());
    assert_eq!(current.read().state(), TaskState::Canceling);
    let mut stopped = false;
    let mut replied = false;
    let mut barrier_published = false;
    let barrier_target = PublishedReply::new(&f.scope);
    while let Some(ticket) = dispatch.take_ready(&f.runtime, 16).unwrap() {
        stopped |= matches!(
            ticket.effect,
            WorkerAction::Context(ContextEffect::StopTask { .. })
        );
        replied |= matches!(ticket.effect, WorkerAction::Reply(_));
        if matches!(
            ticket.effect,
            WorkerAction::Context(ContextEffect::SettlementBarrier { .. })
        ) {
            assert!(accepted.read().is_some());
            assert_eq!(current.read().state(), TaskState::Canceling);
            f.runtime
                .commit_reply(&ticket, &barrier_target, Arc::new(true))
                .unwrap();
            dispatch.publication_committed(&f.runtime, &ticket).unwrap();
            barrier_published = true;
        }
        let action = ticket.descriptor.id;
        f.runtime.effect_completed(ticket).unwrap();
        for (dependent, state) in f
            .runtime
            .effect_inventory(actor(), input, None, ContextCore::MAX_STEP_WORK)
            .unwrap()
        {
            if state == EffectState::Pending && dependent.dependencies.contains(&action) {
                f.runtime
                    .acknowledge_dependency(actor(), input, dependent.id, action)
                    .unwrap();
            }
        }
    }
    assert!(stopped && replied && barrier_published);
    assert!(dispatch.is_empty());
    f.runtime.effect_completed(status).unwrap();
    f.runtime.effect_completed(conclusion).unwrap();
    f.runtime.retire_input(actor(), input).unwrap();
    f.runtime.retire_input(actor(), settled).unwrap();
    f.finish();
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SettlementScenario {
    Unused,
    Registered,
    FailBeforeBarrier,
    FailAfterBarrier,
}
fn exercise_original_fragment_roles(task_stopped: bool, settlement: SettlementScenario) {
    exercise_fragment_roles_with_start(task_stopped, settlement, false);
}
fn exercise_fragment_roles_with_start(
    task_stopped: bool,
    settlement: SettlementScenario,
    start_role: bool,
) {
    use crate::control_runtime::{
        effect_dispatch::EffectDispatch,
        preparation::PreparationOutcome,
        task::{TaskHostBinding, TaskHostError, TaskHostRecord, TaskHostResources},
    };
    use crate::{
        control_runtime_policy::TaskLifecycleRole, settlement::RegistrationNotice,
        task_port::TaskPendingNotice,
    };
    use novarocks_control_runtime_core::identity::ConsumerId;
    use novarocks_execution_contract::{CreationContent, FrozenBytes, TaskCreationInput};
    use std::{
        any::Any,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    struct Pending;
    impl TaskPendingNotice for Pending {
        fn pending(&self) {}
    }
    struct Registered(Arc<AtomicUsize>);
    impl RegistrationNotice for Registered {
        fn bind_work(&mut self, _: novarocks_control_runtime_core::identity::WorkId) {}
        fn registered(self: Box<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn unused(self: Box<Self>) {
            self.0.fetch_add(2, Ordering::SeqCst);
        }
    }
    #[derive(Debug)]
    struct Assignment;
    impl CreationContent for Assignment {
        fn encoded_len(&self) -> usize {
            0
        }
        fn retained_bytes(&self) -> usize {
            0
        }
        fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }
    let mut f = Fixture::with_policy_byte_limits(4096);
    let policy = f.policy.expect("actual fixed Task aggregate policy");
    assert_eq!(
        policy.loading.checked_add(policy.accepted).unwrap(),
        policy.task_base
    );
    f.assert_storage_paid(StorageNeed::default());
    let task = task(f.identity, 1);
    let create = f.send(
        ContextInput::Create {
            request: operation(1),
            task,
            payload: PayloadId(1),
        },
        Some(operation(1)),
        InputClass::Ordinary,
        2,
    );
    f.runtime.poll_one(Tick(1)).unwrap();
    let load = f
        .runtime
        .ready_effects(actor(), create, 15)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let ticket = f.runtime.take_effect(actor(), create, load).unwrap();
    f.assert_storage_paid(policy.loading);
    let loading_headers = f.scope.snapshot().unwrap();
    let mut host = TaskHostRecord::adopt_loading(
        &f.runtime,
        &ticket,
        TaskHostBinding {
            context: f.identity,
            task_source: ActorId::new(2).unwrap(),
            protocol_generation: generation(),
            max_cuts: 4,
            pending: Arc::new(Pending),
            health: Arc::new(
                novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
            ),
        },
    )
    .unwrap();
    let source4_headers = f.scope.snapshot().unwrap();
    assert_eq!(source4_headers.work, loading_headers.work + 4);
    assert_eq!(source4_headers.storage, loading_headers.storage + 4);
    f.assert_storage_paid(policy.loading);
    f.runtime.effect_completed(ticket).unwrap();
    f.drain(create);
    assert!(matches!(
        host.admit_settlement_notice(1),
        Err(crate::control_runtime::task::TaskHostError::CompletionBeforeAccepted)
    ));
    assert!(
        f.runtime
            .role_inventory(actor(), None, 32)
            .unwrap()
            .is_empty(),
        "loading cannot borrow post-Accepted input positions from another Task"
    );
    assert!(matches!(
        host.issue_start_permit(),
        Err(TaskHostError::CompletionBeforeAccepted)
    ));
    let registration = Arc::new(AtomicUsize::new(0));
    let (error, notice) =
        match host.issue_completion_roles(Box::new(Registered(registration.clone()))) {
            Err(refused) => refused,
            Ok(_) => panic!("completion before Accepted must fail"),
        };
    assert!(matches!(error, TaskHostError::CompletionBeforeAccepted));
    assert_eq!(registration.load(Ordering::SeqCst), 0);
    notice.unused();
    assert_eq!(registration.load(Ordering::SeqCst), 2);
    registration.store(0, Ordering::SeqCst);
    let input = TaskCreationInput::new(
        FrozenBytes::freeze(bytes::Bytes::new()),
        Box::new(Assignment),
    );
    let inbound = novarocks_execution_contract::ExchangeInbound::try_new(
        novarocks_execution_contract::FragmentNodeId::new(7),
        vec![novarocks_execution_contract::ExchangeSource::new(
            self::task(f.identity, 2),
            novarocks_types::UniqueId::new(2, 2),
            0,
        )],
        novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
    )
    .unwrap();
    let descriptor = novarocks_execution_contract::TaskDescriptor::try_new(
        task,
        novarocks_types::UniqueId::new(1, 1),
        std::num::NonZeroUsize::new(1).unwrap(),
        vec![],
        novarocks_execution_contract::ExchangeTopology::try_new(vec![], vec![inbound]).unwrap(),
    )
    .unwrap();
    let inbound_bytes =
        novarocks_worker_protocol::inbound_cost::record_bytes(&descriptor).unwrap() as u64;
    let request = Arc::new(
        novarocks_execution_contract::CreateTask::try_new(
            operation(1),
            f.identity,
            descriptor,
            vec![],
        )
        .unwrap(),
    );
    let request_weak = Arc::downgrade(&request);
    let loader = host
        .begin_loader(crate::control_runtime::loading::TaskCreatePayload {
            id: PayloadId(1),
            request,
            input,
            ingress_retained_bytes: 2,
        })
        .unwrap_or_else(|(error, _)| panic!("actual loader admission: {error}"));
    let cost = match loader.run() {
        crate::control_runtime::loading::LoaderOutcome::Measured(Ok(cost)) => cost,
        _ => panic!("actual elected loader must measure its own input"),
    };
    assert!(
        !host.loader().unwrap().ready_for_exit().unwrap(),
        "input source is still retained, so loader whole exit remains false"
    );
    let loaded = f.send(
        ContextInput::Loaded {
            task,
            result: Ok(cost),
        },
        None,
        InputClass::Lifecycle,
        2,
    );
    f.runtime.poll_one(Tick(2)).unwrap();
    f.assert_storage_paid(policy.task_base);
    let mut dispatch = EffectDispatch::new(actor(), 15);
    dispatch.index_committed(&f.runtime, 32).unwrap();
    let publication = dispatch.take_ready(&f.runtime, 15).unwrap().unwrap();
    let WorkerAction::Context(ContextEffect::PublishTask { snapshot, .. }) = &publication.effect
    else {
        panic!("actual Accepted snapshot")
    };
    let port = host.port();
    let next = port
        .publication()
        .read()
        .with_status(snapshot.clone())
        .unwrap();
    drop(
        f.runtime
            .commit_snapshot(&publication, port.publication(), Arc::new(next))
            .unwrap_or_else(|(error, _)| panic!("actual Task snapshot commit: {error}")),
    );
    dispatch
        .publication_committed(&f.runtime, &publication)
        .unwrap();
    let preparation = dispatch.take_ready(&f.runtime, 15).unwrap().unwrap();
    let job = host
        .adopt_preparation(&f.runtime, &preparation)
        .unwrap_or_else(|error| panic!("actual Task prep: {error}"));
    struct CountedRegistration {
        notice: Box<dyn RegistrationNotice>,
        calls: Arc<AtomicUsize>,
    }
    impl RegistrationNotice for CountedRegistration {
        fn bind_work(&mut self, work: novarocks_control_runtime_core::identity::WorkId) {
            self.notice.bind_work(work);
        }
        fn registered(self: Box<Self>) {
            self.notice.registered();
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        fn unused(self: Box<Self>) {
            self.notice.unused();
            self.calls.fetch_add(2, Ordering::SeqCst);
        }
    }
    if start_role {
        let permit = host.issue_start_permit().unwrap();
        assert_eq!(permit.identity(), task);
        let issued = f.scope.snapshot().unwrap();
        let cut = host
            .lifecycle_input(TaskLifecycleRole::Running)
            .unwrap()
            .cut();
        assert!(matches!(
            host.issue_start_permit(),
            Err(TaskHostError::LifecycleAlreadyIssued(
                TaskLifecycleRole::Running
            ))
        ));
        assert_eq!(f.scope.snapshot().unwrap(), issued);
        // The dormant start is never called. Its unique typed permit owns
        // this unused withdrawal, independently of the still-live preparation.
        drop(permit);
        assert!(host.observe_lifecycle_inputs().unwrap());
        assert!(port.ingress().event_unused(cut).unwrap());
        port.ingress().observe_cut_exit(cut, true, true).unwrap();
        let retired = host
            .causal()
            .retire_after_exit(
                cut,
                crate::control_runtime::causal::CausalSourceRetirement::Unclaimed,
            )
            .unwrap();
        dispose_retired_pair(&f, port.ingress(), retired);
        f.ledger.reap_scope_ready(f.scope.id(), 32).unwrap();
        assert_eq!(
            host.lifecycle_input(TaskLifecycleRole::Running)
                .unwrap()
                .phase(),
            novarocks_control_runtime::exit_role::ExitRolePhase::Retired
        );
        assert!(host.causal().is_empty());
        assert!(
            f.runtime
                .role_inventory(actor(), None, 32)
                .unwrap()
                .is_empty()
        );
        f.assert_storage_paid(policy.task_base);
    }
    let before_source2 = f.scope.snapshot().unwrap();
    let (cut, notice) = host.admit_settlement_notice(1).unwrap();
    let after_source2 = f.scope.snapshot().unwrap();
    assert_eq!(after_source2.work, before_source2.work + 2);
    assert_eq!(after_source2.storage, before_source2.storage + 2);
    assert!(
        port.ingress()
            .cut_funding(cut)
            .unwrap()
            .custody
            .iter()
            .all(|role| role.phase()
                == novarocks_control_runtime::exit_role::ExitRolePhase::Admitted)
    );
    f.assert_storage_paid(policy.task_base);
    assert!(matches!(
        host.admit_settlement_notice(1),
        Err(crate::control_runtime::task::TaskHostError::CompletionAlreadyIssued)
    ));
    assert_eq!(
        f.runtime.role_inventory(actor(), None, 32).unwrap().len(),
        2
    );
    let admitted = host
        .issue_completion_roles(Box::new(CountedRegistration {
            notice,
            calls: registration.clone(),
        }))
        .unwrap_or_else(|(error, _)| panic!("original completion issuer: {error}"));
    assert!(matches!(
        host.issue_completion_roles(Box::new(Registered(registration.clone()))),
        Err((TaskHostError::CompletionAlreadyIssued, _))
    ));
    f.runtime.effect_completed(preparation).unwrap();
    f.runtime.effect_completed(publication).unwrap();
    f.drain(loaded);
    f.ledger.reap_scope_ready(f.scope.id(), 32).unwrap();
    assert!(host.loader().unwrap().ready_for_exit().unwrap());
    f.run(
        ContextInput::Physical {
            task,
            fact: PhysicalFact::LoaderExited,
        },
        None,
    );
    let loader = host.loader().unwrap();
    loader
        .report()
        .consume(ConsumerId::new(1).unwrap())
        .unwrap();
    loader
        .report()
        .consume(ConsumerId::new(2).unwrap())
        .unwrap();
    assert!(loader.try_retire().unwrap());
    if task_stopped {
        let canceled = f.send(
            ContextInput::Cancel {
                request: operation(3),
                task,
                reason: novarocks_execution_contract::CancelReason::UpstreamNoLongerNeeded,
            },
            Some(operation(3)),
            InputClass::Control,
            2,
        );
        f.runtime.poll_one(Tick(3)).unwrap();
        let mut cancel_dispatch = EffectDispatch::new(actor(), 15);
        cancel_dispatch.index_committed(&f.runtime, 32).unwrap();
        for _ in 0..2 {
            let publication = cancel_dispatch.take_ready(&f.runtime, 15).unwrap().unwrap();
            let current = port.publication().read();
            let next = match &publication.effect {
                WorkerAction::Context(ContextEffect::PublishConclusion {
                    generation,
                    conclusion,
                    source,
                    ..
                }) => {
                    let accepted = port
                        .prepare_conclusion(
                            std::num::NonZeroU64::new(*generation).unwrap(),
                            conclusion.clone(),
                            *source,
                        )
                        .unwrap();
                    current.with_conclusion(accepted).unwrap()
                }
                WorkerAction::Context(ContextEffect::PublishTask { snapshot, .. }) => {
                    current.with_status(snapshot.clone()).unwrap()
                }
                _ => panic!("actual ordered cancellation publication"),
            };
            drop(current);
            drop(
                f.runtime
                    .commit_snapshot(&publication, port.publication(), Arc::new(next))
                    .unwrap_or_else(|(error, _)| panic!("actual cancellation commit: {error}")),
            );
            cancel_dispatch
                .publication_committed(&f.runtime, &publication)
                .unwrap();
            f.runtime.effect_completed(publication).unwrap();
        }
        for _ in 0..15 {
            let effect = cancel_dispatch
                .take_ready(&f.runtime, 15)
                .unwrap()
                .expect("actual producer stop remains ready");
            let stop = matches!(
                effect.effect,
                WorkerAction::Context(ContextEffect::StopTask { .. })
            );
            if stop {
                port.notify_stopped();
            }
            f.runtime.effect_completed(effect).unwrap();
            if stop {
                break;
            }
        }
        f.drain(canceled);
        assert!(port.is_stopped());
        assert!(
            !f.scope.snapshot().unwrap().stopped,
            "Task stop keeps the context scope live for other Tasks"
        );
    }
    let called = AtomicUsize::new(0);
    let queue = crate::control_runtime::preparation::PreparationQueue::new(1, 1, 1, 1);
    queue
        .enqueue(job)
        .unwrap_or_else(|_| panic!("actual Task request/input queue custody"));
    let outcome = queue.pop().unwrap().run(|_request, input, _| {
        called.fetch_add(1, Ordering::SeqCst);
        drop(input)
    });
    if task_stopped {
        assert!(matches!(outcome, PreparationOutcome::TaskStopped));
        assert_eq!(called.load(Ordering::SeqCst), 0);
    } else {
        assert!(matches!(outcome, PreparationOutcome::Returned(())));
        assert_eq!(called.load(Ordering::SeqCst), 1);
    }
    assert!(
        request_weak.upgrade().is_none(),
        "preparation exit must release the original opaque Create carrier while Task finishers remain alive"
    );
    let prep = host.preparation().unwrap();
    assert!(
        prep.publish_whole_exit().unwrap(),
        "original finishers are Task children, not preparation children"
    );
    for report in [prep.body_report(), prep.exit_report()] {
        report.consume(ConsumerId::new(1).unwrap()).unwrap();
        report.consume(ConsumerId::new(2).unwrap()).unwrap();
    }
    assert!(host.collect_preparation_remainder().unwrap());
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), 0);
    assert_eq!(f.ledger.used(Dimension::PreparingBytes.id()), 0);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), 1);
    assert_eq!(f.ledger.used(Dimension::InboundBytes.id()), inbound_bytes);
    f.assert_storage_paid(policy.task_base);
    let completion = host.completion().unwrap().clone();
    assert!(
        !completion.try_retire(),
        "the original capabilities are still live"
    );
    let failed = matches!(
        settlement,
        SettlementScenario::FailBeforeBarrier | SettlementScenario::FailAfterBarrier
    );
    let nested_dispatch = Arc::new(AtomicUsize::new(0));
    if settlement == SettlementScenario::Unused {
        // Stop is not actual holder exit. A port clone cannot sign another role.
        f.scope.request_stop().unwrap();
        assert!(!completion.try_retire());
        drop(admitted);
        completion.observe_unused_settlement();
        assert_eq!(registration.load(Ordering::SeqCst), 2);
    } else {
        use crate::settlement::{SettlementDispatchError, SettlementRun};
        let (finisher, wall_time, publisher) = admitted.into_parts();
        let (permit, mut original) = finisher.into_parts();
        original.bind();
        original.started();
        let calls = Arc::new(AtomicUsize::new(0));
        let called = calls.clone();
        let expected = port.clone();
        let nested = nested_dispatch.clone();
        let reentrant = completion.clone();
        permit.register(Box::new(move |snapshot| {
            nested.store(
                if matches!(
                    reentrant.dispatch_settlement(&expected),
                    Ok(crate::settlement::SettlementRun::Deferred)
                ) {
                    1
                } else {
                    2
                },
                Ordering::SeqCst,
            );
            if failed {
                let accepted = snapshot.expect("failure conclusion is published before callback");
                assert_eq!(
                    accepted
                        .originating_failure()
                        .unwrap()
                        .failure_fact()
                        .class(),
                    novarocks_execution_contract::failure::FailureClass::Invariant
                );
                assert!(std::ptr::eq(
                    accepted.as_ref(),
                    expected.accepted().unwrap().as_ref()
                ));
            } else if task_stopped {
                assert!(snapshot.is_some());
            } else {
                assert!(snapshot.is_none());
            }
            called.fetch_add(1, Ordering::SeqCst);
        }));
        assert_eq!(registration.load(Ordering::SeqCst), 1);
        assert_eq!(
            completion.dispatch_settlement(&port).unwrap(),
            SettlementRun::Deferred
        );
        let work = completion.settlement_work();
        let delivery = host.causal().deliver_next().unwrap().unwrap();
        assert_eq!(delivery.cut, cut);
        assert!(
            delivery.prefix.is_none(),
            "a genuinely empty prefix needs no extra Core dispatch"
        );
        let settled = delivery.event;
        assert_eq!(
            host.causal().observe_core(cut).unwrap(),
            novarocks_control_runtime_core::completion::Consumption::Pending
        );
        f.runtime.poll_one(Tick(4)).unwrap();
        let mut ordered = EffectDispatch::new(actor(), 15);
        ordered.index_committed(&f.runtime, 32).unwrap();
        let barrier = ordered.take_ready(&f.runtime, 15).unwrap().unwrap();
        assert!(matches!(barrier.effect,
            WorkerAction::Context(ContextEffect::SettlementBarrier { task: t, work: w })
                if t == task && w == work));
        if settlement != SettlementScenario::FailBeforeBarrier {
            completion
                .commit_settlement_barrier(&f.runtime, &barrier, &port)
                .unwrap();
            ordered.publication_committed(&f.runtime, &barrier).unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        if failed {
            let bad = f.send(
                ContextInput::Installed {
                    task: self::task(f.identity, 999),
                },
                None,
                InputClass::Lifecycle,
                2,
            );
            f.runtime.poll_one(Tick(5)).unwrap();
            assert!(f.runtime.snapshot(actor()).unwrap().failed);
            assert_eq!(
                completion.dispatch_settlement(&port).unwrap(),
                SettlementRun::Deferred
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(port.accepted().is_none());
            let cleanup = f
                .runtime
                .take_effect(actor(), bad, ActionId::new(1).unwrap())
                .unwrap();
            f.roles[0].0.start().unwrap();
            let mut session = f.runtime.claim_failure_publication(&cleanup).unwrap();
            assert!(matches!(
                completion.commit_failure_settlement_barrier(&mut session, &port),
                Err(SettlementDispatchError::FailureSnapshotMissing)
            ));
            if settlement == SettlementScenario::FailBeforeBarrier {
                assert!(matches!(
                    completion.commit_settlement_barrier(&f.runtime, &barrier, &port),
                    Err(SettlementDispatchError::Driver(DriverError::Effect(
                        novarocks_control_runtime_core::effect::EffectError::StaleGeneration
                    )))
                ));
            }
            let projection = port.prepare_owner_failure().unwrap();
            drop(
                session
                    .commit_snapshot(port.publication(), Arc::new(projection))
                    .unwrap_or_else(|(error, _)| panic!("actual failure projection: {error}")),
            );
            port.notify_stopped();
            if settlement == SettlementScenario::FailBeforeBarrier {
                completion
                    .commit_failure_settlement_barrier(&mut session, &port)
                    .unwrap();
            } else {
                assert!(matches!(
                    completion.commit_failure_settlement_barrier(&mut session, &port),
                    Err(SettlementDispatchError::Driver(
                        DriverError::ReplyAlreadyCommitted
                    ))
                ));
            }
            f.runtime.effect_completed(cleanup).unwrap();
            f.runtime.retire_input(actor(), bad).unwrap();
        }
        assert_eq!(
            completion.dispatch_settlement(&port).unwrap(),
            SettlementRun::Exited
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            completion.dispatch_settlement(&port).is_err(),
            "original FnOnce is not retried"
        );
        original.completed();
        drop(wall_time);
        drop(publisher);
        f.runtime.effect_completed(barrier).unwrap();
        assert_eq!(
            host.causal().observe_core(cut).unwrap(),
            novarocks_control_runtime_core::completion::Consumption::Consumed,
            "an actual pre-failure Core commitment remains positive consumption"
        );
        f.runtime.retire_input(actor(), settled).unwrap();
        port.ingress().consume_cut(cut, 0).unwrap();
    }
    assert!(
        host.causal()
            .retire_after_exit(
                cut,
                crate::control_runtime::causal::CausalSourceRetirement::Unclaimed
            )
            .is_err(),
        "input retirement cannot manufacture source/capture/effect exit"
    );
    port.ingress().observe_cut_exit(cut, true, true).unwrap();
    let retired = host
        .causal()
        .retire_after_exit(
            cut,
            crate::control_runtime::causal::CausalSourceRetirement::Unclaimed,
        )
        .unwrap();
    dispose_retired_pair(&f, port.ingress(), retired);
    assert!(host.causal().is_empty());
    for role in [
        TaskLifecycleRole::Finisher,
        TaskLifecycleRole::Settlement,
        TaskLifecycleRole::WallTime,
        TaskLifecycleRole::TerminalPublisher,
    ] {
        if failed {
            completion
                .report(role)
                .abandon_failed_owner(ConsumerId::new(1).unwrap(), &f.scope, generation())
                .unwrap();
        } else {
            completion
                .report(role)
                .consume(ConsumerId::new(1).unwrap())
                .unwrap();
        }
        completion
            .report(role)
            .consume(ConsumerId::new(2).unwrap())
            .unwrap();
    }
    completion.try_retire();
    f.ledger.reap_scope_ready(f.scope.id(), 32).unwrap();
    assert!(completion.try_retire());
    drop(completion);
    drop(port);
    let TaskHostResources {
        mut lifetime,
        mut active,
        port,
        loader,
        loading_events,
        loading_return,
        pending_loading,
        preparation,
        mut preparation_remainder,
        completion,
        completion_action,
        lifecycle_inputs,
        open_summary,
        causal,
    } = host.into_resources();
    assert!(
        loading_events.is_none(),
        "this fixture used the loader component directly"
    );
    assert!(loading_return.is_none());
    assert!(pending_loading.is_none());
    assert!(
        completion_action.is_none(),
        "this fixture issued only original fragment roles"
    );
    assert!(
        open_summary.is_none(),
        "this component issued no open-summary role"
    );
    assert!(causal.is_empty());
    if start_role {
        assert_eq!(lifecycle_inputs.len(), 1);
        assert_eq!(
            lifecycle_inputs[&TaskLifecycleRole::Running].phase(),
            novarocks_control_runtime::exit_role::ExitRolePhase::Retired
        );
    } else {
        assert!(lifecycle_inputs.is_empty());
    }
    drop(lifecycle_inputs);
    drop(causal);
    drop(completion);
    drop(preparation);
    drop(loader);
    let retained = preparation_remainder.as_mut().unwrap();
    let expected_active = policy
        .loading
        .resources()
        .unwrap()
        .checked_add(&ResourceVector::new([(Dimension::ActiveTasks.id(), 1)]).unwrap())
        .unwrap();
    assert_eq!(
        active.resources().unwrap(),
        expected_active,
        "the same elected Task bundle retains loading Source4 storage"
    );
    assert_bundle_storage(retained, policy.accepted);
    assert_eq!(
        retained
            .resources()
            .unwrap()
            .get(Dimension::PreparingTasks.id()),
        0
    );
    assert_eq!(
        retained
            .resources()
            .unwrap()
            .get(Dimension::PreparingBytes.id()),
        0
    );
    assert_eq!(
        retained
            .resources()
            .unwrap()
            .get(Dimension::InboundBytes.id()),
        inbound_bytes
    );
    f.assert_storage_paid(policy.task_base);
    lifetime.body_exited().unwrap();
    lifetime.captures_exited().unwrap();
    assert!(
        !lifetime.actual_exit_ready().unwrap(),
        "original Source4/cut2 children still prevent Task header retirement"
    );
    finish_source_routes(&f, port.ingress());
    drop(port);
    f.ledger.reap_scope_ready(f.scope.id(), 32).unwrap();
    assert!(lifetime.actual_exit_ready().unwrap());
    f.assert_storage_paid(policy.task_base);
    active.release(&active.resources().unwrap()).unwrap();
    f.assert_storage_paid(policy.accepted);
    assert_bundle_storage(retained, policy.accepted);
    retained.release(&retained.resources().unwrap()).unwrap();
    f.assert_storage_paid(StorageNeed::default());
    f.finish();
    assert_eq!(
        nested_dispatch.load(Ordering::SeqCst),
        usize::from(settlement != SettlementScenario::Unused),
        "a reentrant executor cannot refresh or run the same original callback"
    );
}

#[test]
fn an_accepted_start_issuer_holds_one_original_role_and_withdraws_only_its_unused_permit() {
    exercise_fragment_roles_with_start(false, SettlementScenario::Unused, true);
}

#[test]
fn original_fragment_roles_belong_to_task_lifetime_and_do_not_hold_preparation_positions() {
    exercise_original_fragment_roles(false, SettlementScenario::Unused);
}
#[test]
fn task_stop_prevents_queued_preparation_start_without_stopping_the_context_scope() {
    exercise_original_fragment_roles(true, SettlementScenario::Unused);
}

#[test]
fn registered_fragment_settlement_consumes_its_actual_core_barrier_before_invocation() {
    exercise_original_fragment_roles(false, SettlementScenario::Registered);
}
#[test]
fn registered_fragment_settlement_reads_the_already_published_cancellation() {
    exercise_original_fragment_roles(true, SettlementScenario::Registered);
}
#[test]
fn failed_core_settles_an_unpublished_original_barrier_only_after_failure_projection() {
    exercise_original_fragment_roles(false, SettlementScenario::FailBeforeBarrier);
}
#[test]
fn failure_after_barrier_commit_preserves_the_call_and_waits_for_actual_failure_publication() {
    exercise_original_fragment_roles(false, SettlementScenario::FailAfterBarrier);
}

// These tests exercise only neutral causal authors. The full TaskHostRecord
// tests above separately bind the original callback Work and capture reports.
struct CausalHarness {
    fixture: Fixture,
    task: TaskIdentity,
    ingress: std::sync::Arc<crate::task_proposal_ingress::TaskProposalIngress>,
    inbox: crate::control_runtime::causal::TaskCausalInbox,
    source_parent: ReservationId,
}
struct CausalPending;
impl crate::task_port::TaskPendingNotice for CausalPending {
    fn pending(&self) {}
}
impl CausalHarness {
    fn new(cuts: usize) -> Self {
        let mut fixture = Fixture::new();
        let task = task(fixture.identity, 1);
        let loading = fixture.run(
            ContextInput::Create {
                request: operation(1),
                task,
                payload: PayloadId(1),
            },
            Some(operation(1)),
        );
        let source_parent = loading
            .iter()
            .find_map(|effect| match effect {
                WorkerAction::Context(ContextEffect::LoadInput { reservation, .. }) => {
                    Some(*reservation)
                }
                _ => None,
            })
            .expect("the real Task lifetime was adopted from its LoadInput effect");
        fixture.run(
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
        let ingress = paid_ingress(&fixture, task, source_parent, cuts);
        let inbox = crate::control_runtime::causal::TaskCausalInbox::new(
            fixture.runtime.clone(),
            actor(),
            ActorId::new(7001).unwrap(),
            task,
            generation(),
            ingress.clone(),
            std::sync::Arc::new(
                novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
            ),
        );
        Self {
            fixture,
            task,
            ingress,
            inbox,
            source_parent,
        }
    }
    fn admission(&self) -> crate::task_proposal_ingress::CutSourceAdmission {
        crate::task_proposal_ingress::CutSourceAdmission::Fresh(std::array::from_fn(|_| {
            source_role(&self.fixture.scope, self.source_parent)
        }))
    }
    fn work(&mut self) -> WorkId {
        let (work, capacity) = self
            .fixture
            .scope
            .reserve_next(
                generation(),
                empty(),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        let id = work.id();
        self.fixture.roles.push((work, capacity));
        id
    }
    fn settlement(
        &mut self,
    ) -> (
        novarocks_worker_protocol::proposal_summary::CutId,
        Box<dyn crate::settlement::RegistrationNotice>,
    ) {
        let work = self.work();
        let admission = self.admission();
        let (cut, mut sender) = self
            .inbox
            .admit_settlement(1, std::sync::Arc::new(CausalPending), admission)
            .unwrap();
        sender.bind_work(work);
        (cut, sender)
    }
    fn retire(
        &mut self,
        delivery: crate::control_runtime::causal::CausalDelivery,
    ) -> Vec<WorkerAction> {
        use novarocks_control_runtime_core::completion::Consumption;
        assert_eq!(
            self.inbox.observe_core(delivery.cut).unwrap(),
            Consumption::Consumed
        );
        assert!(
            self.inbox
                .retire_after_exit(
                    delivery.cut,
                    crate::control_runtime::causal::CausalSourceRetirement::Unclaimed
                )
                .is_err()
        );
        let mut result = Vec::new();
        if let Some(prefix) = delivery.prefix {
            result.extend(self.fixture.drain(prefix));
        }
        result.extend(self.fixture.drain(delivery.event));
        assert!(
            self.inbox
                .retire_after_exit(
                    delivery.cut,
                    crate::control_runtime::causal::CausalSourceRetirement::Unclaimed
                )
                .is_err(),
            "Core consumption is not physical exit"
        );
        // The neutral test executor returned and disposed all of these values;
        // no opaque source/callback is attached to these component authors.
        self.ingress
            .observe_cut_exit(delivery.cut, true, true)
            .unwrap();
        self.ingress.consume_cut(delivery.cut, 0).unwrap();
        let retired = self
            .inbox
            .retire_after_exit(
                delivery.cut,
                crate::control_runtime::causal::CausalSourceRetirement::Unclaimed,
            )
            .unwrap();
        dispose_retired_pair(&self.fixture, &self.ingress, retired);
        result
    }
    fn finish(self) {
        assert!(self.inbox.is_empty());
        drop(self.inbox);
        finish_source_routes(&self.fixture, &self.ingress);
        drop(self.ingress);
        self.fixture.finish();
    }
}

#[test]
fn lifecycle_event_report_cannot_use_its_prefix_as_its_own_core_receipt() {
    use crate::control_runtime::causal::TaskCausalInbox;
    use novarocks_control_runtime::driver::Clock;
    use novarocks_control_runtime_core::completion::Consumption;
    use novarocks_execution_contract::{CancelReason, TaskConclusionProposal};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    struct BetweenSteps {
        inbox: Arc<Mutex<TaskCausalInbox>>,
        cut: novarocks_worker_protocol::proposal_summary::CutId,
        calls: AtomicUsize,
    }
    impl Clock for BetweenSteps {
        fn now(&self) -> Tick {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 2 {
                // Initial sample, prefix sample, then event sample. The real
                // driver samples outside all locks, after removing the event
                // but before executing its Core step. Keep production fairness.
                let mut inbox = self.inbox.lock().unwrap();
                assert_eq!(inbox.observe_core(self.cut).unwrap(), Consumption::Consumed);
                assert!(
                    !inbox.event_core_consumed(self.cut).unwrap(),
                    "the committed summary is not the event's own Core receipt"
                );
            }
            Tick(1)
        }
    }
    let mut h = CausalHarness::new(1);
    let work = h.work();
    let admission = h.admission();
    let (cut, sender) = h
        .inbox
        .admit_input(work, 1, std::sync::Arc::new(CausalPending), admission)
        .unwrap();
    drop(h.ingress.submit(TaskConclusionProposal::Canceled(
        CancelReason::UpstreamNoLongerNeeded,
    )));
    sender
        .send_input(ContextInput::Physical {
            task: h.task,
            fact: PhysicalFact::OutputReleased,
        })
        .unwrap();
    let delivery = h.inbox.deliver_next().unwrap().unwrap();
    assert!(delivery.prefix.is_some());
    assert!(!h.inbox.event_core_consumed(cut).unwrap());
    let inbox = Arc::new(Mutex::new(h.inbox));
    let clock = BetweenSteps {
        inbox: inbox.clone(),
        cut,
        calls: AtomicUsize::new(0),
    };
    h.fixture.runtime.poll_one_with_clock(&clock).unwrap();
    assert_eq!(
        clock.calls.load(Ordering::SeqCst),
        3,
        "the controlled intermediate observation really ran"
    );
    drop(clock);
    h.inbox = Arc::try_unwrap(inbox)
        .ok()
        .expect("the original custodian returned")
        .into_inner()
        .unwrap();
    assert!(h.inbox.event_core_consumed(cut).unwrap());
    h.retire(delivery);
    h.finish();
}

#[test]
fn actual_causal_registration_order_seals_each_prefix_and_does_not_wait_for_host_effect_exit() {
    use novarocks_control_runtime_core::completion::Consumption;
    use novarocks_execution_contract::{
        CancelReason, TaskConclusion, TaskConclusionProposal, TerminationDetail,
    };
    let mut h = CausalHarness::new(2);
    let (first, first_sender) = h.settlement();
    let (second, second_sender) = h.settlement();
    assert!(first < second);
    h.ingress.submit(TaskConclusionProposal::Canceled(
        CancelReason::UpstreamNoLongerNeeded,
    ));
    second_sender.registered();
    h.ingress.submit(TaskConclusionProposal::Aborted(
        novarocks_execution_contract::AbortCause::PeerTaskFailed,
    ));
    first_sender.registered();
    let earlier = h.inbox.deliver_next().unwrap().unwrap();
    assert_eq!(
        earlier.cut, second,
        "handoff order is independent of reservation order"
    );
    assert!(earlier.prefix.is_some());
    assert!(h.inbox.deliver_next().unwrap().is_none());
    assert_eq!(h.inbox.observe_core(second).unwrap(), Consumption::Pending);
    h.fixture.runtime.poll_one(Tick(1)).unwrap();
    assert_eq!(h.inbox.observe_core(second).unwrap(), Consumption::Consumed);
    // The original prefix's publications/captures remain unretired, but the
    // next already-paid role can enter the same Task FIFO after Core consumed.
    assert_eq!(
        h.fixture
            .runtime
            .retire_input(actor(), earlier.prefix.unwrap()),
        Err(DriverError::EffectsRemain)
    );
    let later = h.inbox.deliver_next().unwrap().unwrap();
    assert_eq!(later.cut, first);
    h.fixture.runtime.poll_one(Tick(2)).unwrap();
    assert!(!h.fixture.runtime.snapshot(actor()).unwrap().failed);
    assert_eq!(h.inbox.observe_core(first).unwrap(), Consumption::Consumed);
    let earlier_effects = h.retire(earlier);
    let later_effects = h.retire(later);
    assert!(earlier_effects.iter().any(|effect| matches!(effect,
        WorkerAction::Context(ContextEffect::PublishConclusion { conclusion, .. })
            if *conclusion == TaskConclusion::Terminated(TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded)))));
    assert!(!later_effects.iter().any(|effect| matches!(
        effect,
        WorkerAction::Context(ContextEffect::PublishConclusion { .. })
    )));
    h.finish();
}

#[test]
fn actual_task_close_handoff_advances_the_next_prefix_generation_in_the_same_fifo() {
    use novarocks_control_runtime_core::completion::Consumption;
    use novarocks_execution_contract::{AbortCause, CancelReason, TaskConclusionProposal};
    let mut h = CausalHarness::new(2);
    // Close and settlement are already admitted responsibilities. The actual
    // Context stop commit must not be bypassed by fresh external admission.
    let work = h.work();
    let admission = h.admission();
    let settlement_work = h.work();
    let settlement_admission = h.admission();
    let mut closing = h.fixture.run(
        ContextInput::Abort {
            request: operation(2),
            cause: AbortCause::PeerTaskFailed,
        },
        Some(operation(2)),
    );
    assert!(closing.iter().any(|effect| matches!(
        effect,
        WorkerAction::Context(ContextEffect::ContinueClose { .. })
    )));
    closing.extend(h.fixture.run(ContextInput::ContinueClose, None));
    let next = closing
        .iter()
        .find_map(|effect| match effect {
            WorkerAction::Context(ContextEffect::ApplyTaskClose { task, generation })
                if *task == h.task =>
            {
                Some(*generation)
            }
            _ => None,
        })
        .expect("the pure close owner emits this Task's finite causal role");
    assert_ne!(next, generation());
    let (close_cut, close) = h
        .inbox
        .admit_close(next, work, 1, std::sync::Arc::new(CausalPending), admission)
        .unwrap();
    h.ingress
        .submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
    close.send();
    let close = h.inbox.deliver_next().unwrap().unwrap();
    assert_eq!(close.cut, close_cut);
    assert!(close.prefix.is_some());
    let (_, mut settle) = h
        .inbox
        .admit_settlement(1, std::sync::Arc::new(CausalPending), settlement_admission)
        .unwrap();
    settle.bind_work(settlement_work);
    h.ingress.submit(TaskConclusionProposal::Canceled(
        CancelReason::UpstreamNoLongerNeeded,
    ));
    settle.registered();
    h.fixture.runtime.poll_one(Tick(1)).unwrap();
    assert_eq!(
        h.inbox.observe_core(close_cut).unwrap(),
        Consumption::Consumed
    );
    let settled = h.inbox.deliver_next().unwrap().unwrap();
    assert!(settled.prefix.is_some());
    h.fixture.runtime.poll_one(Tick(2)).unwrap();
    assert!(
        !h.fixture.runtime.snapshot(actor()).unwrap().failed,
        "the post-close summary must carry the new protocol epoch"
    );
    h.retire(close);
    h.retire(settled);
    h.finish();
}

#[test]
fn actual_causal_role_refusal_rolls_back_only_unused_positions_and_unused_holder_waits_for_exit() {
    use crate::control_runtime::causal::CausalError;
    use novarocks_worker_protocol::proposal_summary::CutError;
    let mut h = CausalHarness::new(1);
    let (cut, sender) = h.settlement();
    assert_eq!(
        h.fixture
            .runtime
            .role_inventory(actor(), None, 32)
            .unwrap()
            .len(),
        2
    );
    let admission = h.admission();
    let crate::task_proposal_ingress::CutSourceAdmission::Fresh(ref roles) = admission else {
        unreachable!();
    };
    let original_work = roles.each_ref().map(|role| role.work.id());
    let refusal = h
        .inbox
        .admit_settlement(1, std::sync::Arc::new(CausalPending), admission)
        .err()
        .expect("the one real cut is already reserved");
    assert_eq!(refusal.error, CausalError::Cut(CutError::Full));
    let crate::task_proposal_ingress::CutSourceAdmission::Fresh(ref roles) = refusal.admission
    else {
        panic!("refusal preserves the original paid roles");
    };
    assert_eq!(roles.each_ref().map(|role| role.work.id()), original_work);
    dispose_unused_admission(&h.fixture, h.task, refusal.admission);
    assert_eq!(
        h.fixture
            .runtime
            .role_inventory(actor(), None, 32)
            .unwrap()
            .len(),
        2,
        "a failed cut admission returns both unbound input positions"
    );
    sender.unused();
    assert!(
        h.inbox
            .retire_after_exit(
                cut,
                crate::control_runtime::causal::CausalSourceRetirement::Unclaimed
            )
            .is_err()
    );
    assert_eq!(
        h.fixture
            .runtime
            .role_inventory(actor(), None, 32)
            .unwrap()
            .len(),
        2
    );
    h.ingress.observe_cut_exit(cut, true, true).unwrap();
    let retired = h
        .inbox
        .retire_after_exit(
            cut,
            crate::control_runtime::causal::CausalSourceRetirement::Unclaimed,
        )
        .unwrap();
    dispose_retired_pair(&h.fixture, &h.ingress, retired);
    assert!(
        h.fixture
            .runtime
            .role_inventory(actor(), None, 32)
            .unwrap()
            .is_empty()
    );
    h.finish();
}

#[test]
fn missing_actual_input_observation_cannot_be_reconstructed_from_a_retired_queue() {
    use crate::control_runtime::causal::CausalError;
    use novarocks_control_runtime::driver::InputPhase;
    let mut h = CausalHarness::new(1);
    let (cut, sender) = h.settlement();
    sender.registered();
    let delivery = h.inbox.deliver_next().unwrap().unwrap();
    assert!(delivery.prefix.is_none());
    h.fixture.runtime.poll_one(Tick(1)).unwrap();
    let actual = h
        .fixture
        .runtime
        .input_custody(actor(), delivery.event)
        .unwrap()
        .unwrap();
    assert_eq!(actual.phase, InputPhase::Committed);
    h.fixture.drain(delivery.event);
    assert_eq!(
        h.inbox.observe_core(cut),
        Err(CausalError::InputObservationMissing)
    );
    assert_eq!(
        h.ingress.cut_consumption(cut),
        Some(novarocks_control_runtime_core::completion::Consumption::Pending)
    );
    // This fixture saved the real committed record before retiring it. Use
    // that actual proof for teardown, rather than infer it from absence.
    assert_eq!(actual.phase, InputPhase::Committed);
    h.ingress.summary_consumed(Some(cut)).unwrap();
    h.ingress.observe_cut_exit(cut, true, true).unwrap();
    h.ingress.consume_cut(cut, 0).unwrap();
    let retired = h
        .inbox
        .retire_after_exit(
            cut,
            crate::control_runtime::causal::CausalSourceRetirement::Unclaimed,
        )
        .unwrap();
    dispose_retired_pair(&h.fixture, &h.ingress, retired);
    h.finish();
}

#[test]
fn a_runtime_input_refusal_returns_the_same_two_paid_source_roles() {
    use crate::task_proposal_ingress::CutSourceAdmission;
    let mut h = CausalHarness::new(1);
    let mut occupied = Vec::new();
    for _ in 0..16 {
        occupied.push(
            h.fixture
                .runtime
                .reserve_input(actor(), ActorId::new(7002).unwrap(), InputClass::Lifecycle)
                .unwrap()
                .into_role()
                .unwrap_or_else(|_| panic!("original empty input role")),
        );
    }
    let admission = h.admission();
    let CutSourceAdmission::Fresh(ref roles) = admission else {
        unreachable!()
    };
    let original = roles.each_ref().map(|role| role.work.id());
    let refusal = h
        .inbox
        .admit_settlement(1, std::sync::Arc::new(CausalPending), admission)
        .err()
        .expect("all real lifecycle positions are already held");
    assert!(matches!(
        refusal.error,
        crate::control_runtime::causal::CausalError::Driver(_)
    ));
    let CutSourceAdmission::Fresh(ref roles) = refusal.admission else {
        panic!("runtime refusal preserves the original source roles");
    };
    assert_eq!(roles.each_ref().map(|role| role.work.id()), original);
    dispose_unused_admission(&h.fixture, h.task, refusal.admission);
    for role in &mut occupied {
        role.close_after_exit().unwrap();
    }
    drop(occupied);
    h.finish();
}

#[test]
fn source_core_receipt_names_its_real_prefix_and_retirement_returns_both_reap_witnesses() {
    use crate::control_runtime::causal::{CausalSourceRetirement, TaskCausalInbox};
    use crate::task_proposal_ingress::PairRoute;
    use novarocks_control_runtime::driver::Clock;
    use novarocks_execution::runtime::fragment::ExecutionFailure;
    use novarocks_execution_contract::{TaskConclusionProposal, TaskFailurePhase};
    use novarocks_worker_protocol::proposal_summary::SourceId;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct OriginalSource(Arc<AtomicUsize>);
    impl std::fmt::Display for OriginalSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("actual causal prefix source")
        }
    }
    impl std::error::Error for OriginalSource {}
    impl FailureProjection for OriginalSource {
        fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
            use novarocks_execution_contract::failure::{
                FailureClass, FailureFact, FailureReason, domain,
            };
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(65001).unwrap()),
            )
        }
    }
    impl Drop for OriginalSource {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[derive(Debug, Eq, PartialEq)]
    struct PrefixObservation {
        cut: Consumption,
        prefix: Option<InputId>,
        source: Consumption,
        event_consumed: bool,
    }
    struct BetweenSteps {
        inbox: Arc<Mutex<TaskCausalInbox>>,
        cut: novarocks_worker_protocol::proposal_summary::CutId,
        source: SourceId,
        calls: AtomicUsize,
        observed: Mutex<Option<PrefixObservation>>,
    }
    impl Clock for BetweenSteps {
        fn now(&self) -> Tick {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 2 {
                let mut inbox = self.inbox.lock().unwrap();
                *self.observed.lock().unwrap() = Some(PrefixObservation {
                    cut: inbox.observe_core(self.cut).unwrap(),
                    prefix: inbox.source_prefix_input(self.cut, self.source).unwrap(),
                    source: inbox
                        .source_core_consumption(self.cut, self.source)
                        .unwrap(),
                    event_consumed: inbox.event_core_consumed(self.cut).unwrap(),
                });
            }
            Tick(1)
        }
    }

    let mut h = CausalHarness::new(1);
    let (cut, sender) = h.settlement();
    let drops = Arc::new(AtomicUsize::new(0));
    let handoff = h.ingress.submit(TaskConclusionProposal::Failed {
        cause: ExecutionFailure::from_domain(OriginalSource(drops.clone())),
        phase: TaskFailurePhase::Execution,
    });
    assert!(handoff.transferred && handoff.discarded.is_none() && handoff.error.is_none());
    sender.registered();
    let source = h
        .ingress
        .pair_inventory(PairRoute::Cut(cut))
        .unwrap()
        .into_iter()
        .find_map(|observation| observation.source)
        .expect("the original candidate has its actual admitted SourceId");
    let delivery = h.inbox.deliver_next().unwrap().unwrap();
    let prefix = delivery
        .prefix
        .expect("a source-bearing summary always owns a prefix input");
    assert_ne!(prefix, delivery.event);
    assert_eq!(
        h.inbox.source_prefix_input(cut, source).unwrap(),
        Some(prefix)
    );
    assert_eq!(
        h.inbox.source_core_consumption(cut, source).unwrap(),
        Consumption::Pending
    );
    let inbox = Arc::new(Mutex::new(h.inbox));
    let clock = BetweenSteps {
        inbox: inbox.clone(),
        cut,
        source,
        calls: AtomicUsize::new(0),
        observed: Mutex::new(None),
    };
    h.fixture.runtime.poll_one_with_clock(&clock).unwrap();
    assert_eq!(clock.calls.load(Ordering::SeqCst), 3);
    let intermediate = clock.observed.lock().unwrap().take();
    drop(clock);
    h.inbox = Arc::try_unwrap(inbox)
        .ok()
        .expect("actual source proof returned")
        .into_inner()
        .unwrap();
    // Both real Core steps completed. Keep their actual observations for
    // cleanup, then assess the saved middle observation after all owners exit.
    // A red rule must not strand the runtime's original Inbox during unwind.
    h.inbox.observe_core(cut).unwrap();
    let prefix_effects = h.fixture.drain(prefix);
    assert!(prefix_effects.iter().any(|effect| matches!(effect,
        WorkerAction::Context(ContextEffect::PublishConclusion { source: Some(actual), .. }) if *actual == source)));
    h.fixture.drain(delivery.event);
    // Only the actual Core effect names this retained source. Dispose it on
    // this component's host stack, then consume reports using that saved proof.
    let retained = h.ingress.take_source(source).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(retained);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    for holder in h
        .ingress
        .take_unused_pair(PairRoute::Cut(cut))
        .unwrap()
        .into_iter()
        .flatten()
    {
        holder.exit();
    }
    for observation in h.ingress.pair_inventory(PairRoute::Cut(cut)).unwrap() {
        assert!(observation.custody.actual_exit_ready().unwrap());
        if let Some(actual) = observation.source {
            assert_eq!(
                h.inbox.source_core_consumption(cut, actual).unwrap(),
                Consumption::Consumed
            );
            observation
                .custody
                .report()
                .consume(ConsumerId::new(1).unwrap())
                .unwrap();
            observation
                .custody
                .report()
                .consume(ConsumerId::new(2).unwrap())
                .unwrap();
        }
        assert!(observation.custody.try_retire().unwrap());
    }
    h.fixture
        .ledger
        .reap_scope_ready(h.fixture.scope.id(), 256)
        .unwrap();
    let reaped = h
        .ingress
        .prepare_pair_reap(PairRoute::Cut(cut))
        .unwrap()
        .map(|probe| probe.observe_reaped().unwrap());
    let identities = format!("{reaped:?}");
    let refused = h
        .inbox
        .retire_after_exit(cut, CausalSourceRetirement::Reaped(reaped))
        .err()
        .expect("cut capture/effect/consumer facts are still outstanding");
    let CausalSourceRetirement::Reaped(ref same) = refused.retirement else {
        panic!("refusal returns both original reap witnesses");
    };
    assert_eq!(format!("{same:?}"), identities);
    h.ingress.observe_cut_exit(cut, true, true).unwrap();
    h.ingress.consume_cut(cut, 0).unwrap();
    let retired = h.inbox.retire_after_exit(cut, refused.retirement).unwrap();
    dispose_retired_pair(&h.fixture, &h.ingress, retired);
    h.finish();
    assert_eq!(
        intermediate,
        Some(PrefixObservation {
            cut: Consumption::Consumed,
            prefix: Some(prefix),
            source: Consumption::Consumed,
            event_consumed: false,
        }),
        "the source receipt belongs to its real prefix before the later event commits"
    );
}
