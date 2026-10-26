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
use novarocks_control_runtime::{
    actor::ActorConfig,
    driver::{ActorRuntime, DriverError},
    lease::{CapacityBundle, WorkLease},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    effect::EffectState,
    identity::{ActorId, InputId, ReservationId},
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{
    LeaseValidFor, OperationOutcome, QueryContextRef, TaskOperationId,
};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use novarocks_worker_protocol::{
    context::{ContextConfig, PhysicalFact, RequestResult},
    creation::{CreationCost, PayloadId},
    lease::{InstalledLease, LeaseBounds},
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
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<WorkerActor>,
    identity: QueryContextRef,
    roles: Vec<(WorkLease, CapacityBundle)>,
    jobs: BTreeMap<ReservationId, (WorkLease, CapacityBundle)>,
}
impl Fixture {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(
            ResourceVector::new([
                (Dimension::ActiveTasks.id(), 2),
                (Dimension::PreparingTasks.id(), 1),
                (Dimension::PreparingBytes.id(), 100),
                (Dimension::InboundRecords.id(), 4),
                (Dimension::InboundBytes.id(), 100),
                (Dimension::Tickets.id(), 1),
                (Dimension::Contexts.id(), 1),
            ])
            .unwrap(),
            32,
            2,
        );
        let scope = ledger.open_scope_next(None, generation(), 32, 32).unwrap();
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
            ContextConfig {
                max_tasks: 4,
                max_creation_waiters: 4,
                max_pending_requests: 16,
                max_preparing: 1,
                max_task_bytes: 100,
                step_budget: ContextCore::MAX_STEP_WORK,
                lease_bounds: LeaseBounds::DEFAULT,
                metric_publish_interval: Duration::from_millis(100),
            },
            MonotonicInstant::ORIGIN,
            InstalledLease::install_initial(
                LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
                LeaseBounds::DEFAULT,
                MonotonicInstant::ORIGIN,
            ),
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
                    turn_budget: 64,
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
        let input = fixture.send(ContextInput::Established, None, InputClass::Lifecycle, 2);
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
    fn finish(mut self) {
        for (_, (mut work, mut capacity)) in std::mem::take(&mut self.jobs) {
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            self.ledger.reap_scope_ready(work.scope().id(), 32).unwrap();
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
        let materialized = matches!(&command, ControlInput::Materialized { .. });
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
    };
    send(
        &mut f,
        ControlInput::Establish {
            request: operation(4),
            context: next,
            ticket: ticket_id,
            payload: PayloadId(2),
            semantics,
            compared_payload: None,
            same_credential: false,
            generation: generation(),
        },
        Some(operation(4)),
    );
    assert_eq!(f.ledger.used(Dimension::Contexts.id()), 1);
    send(
        &mut f,
        ControlInput::Materialized {
            context: next,
            result: Ok(()),
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
    use novarocks_control_runtime::publication::PublishedSnapshot;
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
    while let Some(ticket) = dispatch.take_ready(&f.runtime, 16).unwrap() {
        stopped |= matches!(
            ticket.effect,
            WorkerAction::Context(ContextEffect::StopTask { .. })
        );
        replied |= matches!(ticket.effect, WorkerAction::Reply(_));
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
    assert!(stopped && replied);
    assert!(dispatch.is_empty());
    f.runtime.effect_completed(status).unwrap();
    f.runtime.effect_completed(conclusion).unwrap();
    f.runtime.retire_input(actor(), input).unwrap();
    f.finish();
}
