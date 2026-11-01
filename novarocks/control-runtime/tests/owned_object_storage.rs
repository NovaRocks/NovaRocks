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

#![cfg(not(loom))]

use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, StepContext},
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError, InputAuthor, MetadataConsumption, TimerSourceState},
    exit_role::ExitRole,
    publication::{PublishedReply, PublishedSnapshot},
    reported_work::ReportedWork,
    transaction::{RoleSpec, RuntimeError, RuntimeLedger, ScopeParentAdmission},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ReservationResources, ResourceVector},
    completion::ConsumerKind,
    effect::{EffectDescriptor, EffectKind},
    identity::{
        ActionId, ActorId, ConsumerId, DimensionId, Generation, ScopeId, ScopedWorkRef, Tick,
    },
    mailbox::{InputClass, InputEnvelope},
    owned_storage::{OwnedSlotKind, OwnedSlotNeed, OwnedStorageError},
    scope::{WorkAdmission, WorkKind},
};
use std::sync::Arc;

fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn consumer() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}
fn quota() -> ResourceVector {
    ResourceVector::new([(dimension(), 1)]).unwrap()
}
fn child_resources(completions: usize) -> ReservationResources {
    ReservationResources::new(
        ResourceVector::default(),
        OwnedSlotNeed {
            work_headers: 1,
            completions,
            captures: 1,
            ..OwnedSlotNeed::default()
        },
    )
}

#[test]
fn original_inventory_births_one_real_child_and_waits_for_its_capture_and_report_consumption() {
    let ledger = RuntimeLedger::new(quota());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let guard = scope
        .reserve_next(
            generation(),
            ReservationResources::new(
                quota(),
                OwnedSlotNeed {
                    work_headers: 2,
                    completions: 2,
                    captures: 2,
                    ..OwnedSlotNeed::default()
                },
            ),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap();
    let (parent, mut funding) = guard.bind(WorkKind::Cleanup).unwrap();
    let original = funding.resources().unwrap();
    let before = scope.snapshot().unwrap();
    let refusal = scope.reserve_child_from(
        &mut funding,
        &parent,
        RoleSpec {
            generation: generation(),
            kind: WorkKind::Cleanup,
            resources: child_resources(3),
            consumers: vec![(consumer(), ConsumerKind::Observer)],
        },
    );
    assert!(matches!(
        refusal,
        Err(RuntimeError::Capacity(CapacityError::OwnedStorage(
            OwnedStorageError::MissingPosition(OwnedSlotKind::Completion)
        )))
    ));
    assert_eq!(funding.resources().unwrap(), original);
    assert_eq!(scope.snapshot().unwrap(), before);
    let child = scope
        .reserve_child_from(
            &mut funding,
            &parent,
            RoleSpec {
                generation: generation(),
                kind: WorkKind::Cleanup,
                resources: child_resources(1),
                consumers: vec![(consumer(), ConsumerKind::Observer)],
            },
        )
        .unwrap();
    assert_eq!(scope.snapshot().unwrap().work, 2);
    assert_eq!(ledger.used(dimension()), 1);
    let child_report = child.report().clone();
    let (child_role, child_authority) = ExitRole::from_admitted(child);
    let report = ReportSlot::reserve_next(
        &mut funding,
        generation(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .unwrap();
    let parent = ReportedWork::try_new(parent, funding, report.clone()).unwrap();
    let (parent_role, parent_authority) = ExitRole::from_admitted(parent);
    scope.close_external().unwrap();
    parent_authority.exited().unwrap();
    report.consume(consumer()).unwrap();
    assert!(!parent_role.try_retire().unwrap());
    assert_eq!(ledger.used(dimension()), 1);
    child_authority.exited().unwrap();
    assert!(!child_role.try_retire().unwrap());
    child_report.consume(consumer()).unwrap();
    assert!(child_role.try_retire().unwrap());
    ledger.reap_scope_ready(scope.id(), 1).unwrap();
    assert!(parent_role.try_retire().unwrap());
    ledger.reap_scope_ready(scope.id(), 1).unwrap();
    assert_eq!(ledger.used(dimension()), 0);
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}

#[test]
fn a_bound_cut_keeps_its_original_scope_after_the_reported_work_and_quota_exit() {
    let ledger = RuntimeLedger::new(quota());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let guard = scope
        .reserve_next(
            generation(),
            ReservationResources::new(
                quota(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    cuts: 1,
                    ..OwnedSlotNeed::default()
                },
            ),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap();
    let (work, mut funding) = guard.bind(WorkKind::Cleanup).unwrap();
    let mut cut = funding.take_position(OwnedSlotKind::Cut).unwrap();
    cut.bind().unwrap();
    let report = ReportSlot::reserve_next(
        &mut funding,
        generation(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .unwrap();
    let (role, authority) =
        ExitRole::from_admitted(ReportedWork::try_new(work, funding, report.clone()).unwrap());
    scope.close_external().unwrap();
    authority.exited().unwrap();
    report.consume(consumer()).unwrap();
    assert!(role.try_retire().unwrap());
    ledger.reap_scope_ready(scope.id(), 1).unwrap();
    assert_eq!(ledger.used(dimension()), 0);
    assert_eq!(scope.snapshot().unwrap().work, 0);
    assert_eq!(scope.snapshot().unwrap().storage, 1);
    assert_eq!(
        ledger.remove_scope(scope.id()),
        Err(RuntimeError::ScopeNotRetired)
    );
    cut.retire_after_exit().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}

// This uses the production actor/ledger/exit-role path to exercise one original
// request across execution detachment. The host consumes only neutral effects;
// no thread, I/O callback or synthetic physical-exit receipt is introduced.
#[derive(Debug)]
enum DelayedRequestInput {
    Request,
    Timer,
    Metadata(u64),
    SourcePanic,
    HotSourcePanic,
}
#[derive(Debug, Eq, PartialEq)]
enum DelayedRequestEffect {
    Business,
    Reply(u64),
    RetainedNotice(u64),
    MetadataApplied(u64),
}
#[derive(Debug, Default)]
struct DelayedRequestActor {
    author: Option<InputAuthor>,
    deadline: Option<Tick>,
    own_metadata_work: Option<ScopedWorkRef>,
}
impl Actor for DelayedRequestActor {
    type Input = DelayedRequestInput;
    type Effect = DelayedRequestEffect;
    fn timer_input(_: Tick) -> Self::Input {
        DelayedRequestInput::Timer
    }
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        input: Self::Input,
        step: &mut StepContext<'_, Self::Effect>,
    ) {
        match input {
            DelayedRequestInput::HotSourcePanic => {
                panic!("the original hot source fails before execution detachment")
            }
            DelayedRequestInput::SourcePanic => {
                panic!("the original retained source fails after committing its notice")
            }
            DelayedRequestInput::Metadata(value) => {
                assert!(step.is_runtime_metadata());
                assert!(step.input_author().is_none());
                step.effect(
                    EffectDescriptor {
                        id: ActionId::new(1).unwrap(),
                        work: self
                            .own_metadata_work
                            .expect("the destination's actual control owner"),
                        kind: EffectKind::Publication,
                        dependencies: vec![],
                    },
                    DelayedRequestEffect::MetadataApplied(value),
                )
                .unwrap();
            }
            DelayedRequestInput::Request => {
                assert!(!step.is_retained());
                let author = step.input_author().unwrap().clone();
                step.effect(
                    EffectDescriptor {
                        id: ActionId::new(1).unwrap(),
                        work: author.work(),
                        kind: EffectKind::Business,
                        dependencies: vec![],
                    },
                    DelayedRequestEffect::Business,
                )
                .unwrap();
                self.author = Some(author);
                self.deadline = Some(Tick(10));
            }
            DelayedRequestInput::Timer => {
                assert!(step.is_retained());
                assert!(step.input_author().is_none());
                let author = self.author.take().unwrap();
                step.metadata(DelayedRequestEffect::RetainedNotice(73))
                    .unwrap();
                step.effect_from_author(
                    &author,
                    EffectDescriptor {
                        id: ActionId::new(1).unwrap(),
                        work: author.work(),
                        kind: EffectKind::Reply,
                        dependencies: vec![],
                    },
                    DelayedRequestEffect::Reply(29),
                )
                .unwrap();
                self.deadline = None;
            }
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.deadline
    }
    fn owner_failed(
        _: ScopeId,
        _: Generation,
        _: InputEnvelope,
        step: &mut StepContext<'_, Self::Effect>,
    ) {
        // Only the hot branch has an original failure Cleanup author. The
        // retained branch must never invoke this hook with its Root Scope.
        assert!(step.failure_work().is_some());
    }
}

#[test]
fn original_process_request_survives_context_exit_and_retained_timer_without_borrowing_equal_work_id()
 {
    for failed_source in [false, true] {
        original_retained_request_flow(failed_source);
    }
    original_hot_failure_detaches_to_the_same_custodian();
}

fn original_hot_failure_detaches_to_the_same_custodian() {
    use novarocks_control_runtime::driver::FailureSourceState;

    let ledger = RuntimeLedger::new(quota());
    let process = ledger.open_scope_next(None, generation()).unwrap();
    let (request, mut request_funding) = process
        .reserve_next(
            generation(),
            ReservationResources::new(
                quota(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..OwnedSlotNeed::default()
                },
            ),
            WorkKind::Step,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Step)
        .unwrap();
    let request_report = ReportSlot::reserve_next(
        &mut request_funding,
        generation(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .unwrap();
    let request_retirement = request.retirement_observer();
    let execution = ledger
        .open_scope_next(
            Some(ScopeParentAdmission {
                scope: process.id(),
                generation: generation(),
                kind: WorkKind::Cleanup,
                admission: WorkAdmission::Derived(request.id()),
            }),
            generation(),
        )
        .unwrap();
    let mut metadata = Vec::new();
    for kind in [WorkKind::Step, WorkKind::Step, WorkKind::Cleanup] {
        let (mut work, funding) = execution
            .reserve_next(
                generation(),
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        ..OwnedSlotNeed::default()
                    },
                ),
                kind,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(kind)
            .unwrap();
        work.start().unwrap();
        metadata.push((work, funding));
    }
    let actor = ActorId::new(1).unwrap();
    let runtime = ActorRuntime::new();
    let registration = runtime
        .register_supervised_bound(
            actor,
            execution.clone(),
            DelayedRequestActor::default(),
            ActorConfig {
                mailbox: [2; 4],
                max_batches: 2,
                max_effects_per_step: 2,
                max_dependencies_per_effect: 0,
                turn_budget: 12,
                max_step_work: 2,
            },
            &metadata[2].0,
        )
        .unwrap();
    let mut source = runtime
        .reserve_admitted_input_for(&registration, actor, InputClass::Control, &metadata[0].0)
        .unwrap()
        .into_role()
        .unwrap();
    let permit = runtime
        .reserve_admitted_input_for(&registration, actor, InputClass::Ordinary, &request)
        .unwrap();
    let reply = PublishedReply::new_for_input(&permit.author().unwrap());
    let (request_role, mut request_body) = ExitRole::from_admitted(
        ReportedWork::try_new(request, request_funding, request_report.clone()).unwrap(),
    );
    request_body.start().unwrap();
    let (mut cleanup, mut cleanup_funding) = process
        .reserve_next(
            generation(),
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    ..OwnedSlotNeed::default()
                },
            ),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    cleanup.start().unwrap();
    let failed_input = permit.fill(DelayedRequestInput::HotSourcePanic).unwrap();
    assert!(runtime.poll_one(Tick(1)).unwrap());
    let proof = runtime.failure_proof_for(&registration).unwrap();
    assert!(execution.gates().unwrap().failed);
    assert!(!process.gates().unwrap().failed);
    assert_eq!(proof.initial_epoch(), generation());
    assert_eq!(proof.failed_epoch(), generation().next().unwrap());
    let mut host = runtime.claim_host_actor().unwrap();
    assert!(host.matches_registration(&registration));
    let mut owners: Vec<_> = metadata
        .iter_mut()
        .map(|(work, funding)| (work, funding))
        .collect();
    let before = execution.snapshot().unwrap();
    assert_eq!(before.work, 3);
    assert_eq!(
        host.detach_failed_execution_for_custodian(&proof, &process, &mut owners),
        Err(DriverError::FailurePublicationRequired)
    );
    assert_eq!(execution.snapshot().unwrap(), before);
    let mut session = runtime
        .claim_actor_failure_publication(&proof, &cleanup)
        .unwrap();
    session.commit_reply(&reply, Arc::new(99)).unwrap();
    assert_eq!(reply.take_once().as_deref(), Some(&99));
    assert_eq!(
        host.detach_failed_execution_for_custodian(&proof, &process, &mut owners),
        Err(DriverError::FailurePublicationRequired)
    );
    session.finish().unwrap();
    runtime.retire_input(actor, failed_input).unwrap();
    let FailureSourceState::ClosedPending(supervisor) =
        runtime.failure_source_state(actor).unwrap()
    else {
        panic!("the original failed hot step owns its supervisor input");
    };
    runtime.retire_input(actor, supervisor).unwrap();
    assert_eq!(
        host.detach_failed_execution_for_custodian(&proof, &process, &mut owners),
        Err(DriverError::ActorNotRetired)
    );
    assert_eq!(execution.snapshot().unwrap(), before);
    assert!(registration.execution_attached());
    assert!(!registration.retained_in_process(&process));
    assert!(
        owners
            .iter()
            .all(|(work, funding)| !work.actual_exit_ready().unwrap() && !funding.is_released())
    );
    source.close_after_exit().unwrap();
    assert_eq!(
        host.detach_failed_execution_for_custodian(&proof, &process, &mut owners),
        Err(DriverError::ActorNotRetired)
    );
    assert_eq!(execution.snapshot().unwrap(), before);
    let payload = runtime.take_failure_payload(actor).unwrap().unwrap();
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("the original hot source fails before execution detachment")
    );
    drop(payload);
    assert_eq!(
        host.detach_execution_with_metadata(&process, &mut owners),
        Err(DriverError::ActorNotRetired)
    );
    host.detach_failed_execution_for_custodian(&proof, &process, &mut owners)
        .unwrap();
    assert!(
        owners
            .iter()
            .all(|(work, funding)| work.actual_exit_ready().unwrap() && funding.is_released())
    );
    assert!(registration.same_registration(proof.registration()));
    assert!(!registration.execution_attached());
    assert!(registration.retained_in_process(&process));
    assert!(!registration.retained_in_process(&execution));
    assert!(runtime.snapshot(actor).unwrap().failed);
    assert_eq!(
        runtime.snapshot(actor).unwrap().generation,
        proof.failed_epoch()
    );
    assert_eq!(execution.snapshot().unwrap().work, 0);
    assert_eq!(execution.snapshot().unwrap().storage, 0);
    ledger.remove_scope(execution.id()).unwrap();
    assert_eq!(process.snapshot().unwrap().work, 2);
    assert_eq!(ledger.reap_scope_ready(process.id(), 1).unwrap(), 0);
    assert_eq!(ledger.used(dimension()), 1);
    assert!(!request_retirement.is_retired());
    assert!(!request_role.body_exited());
    runtime
        .arm_failed_custodian_deadline(&proof, Tick(20))
        .unwrap();
    host.release(false).unwrap();
    assert_eq!(runtime.advance_time(Tick(20), 1).unwrap(), 1);
    let host = runtime.claim_host_actor().unwrap();
    assert!(host.matches_registration(&registration));
    assert!(!runtime.poll_one(Tick(20)).unwrap());
    assert_eq!(runtime.pending_timer_input(actor).unwrap(), None);
    assert_eq!(
        runtime.take_failed_custodian_deadline(&proof).unwrap(),
        Some(Tick(20))
    );
    assert!(!process.gates().unwrap().failed);
    assert_eq!(
        runtime.close_timer_source(actor).unwrap(),
        TimerSourceState::ClosedDrained
    );
    runtime.close_actor_admission(actor).unwrap();
    let failed_core = host.retire_actor().unwrap();
    assert!(failed_core.deadline.is_none());
    assert!(runtime.is_empty());
    cleanup.body_exited().unwrap();
    cleanup.captures_exited().unwrap();
    cleanup_funding.finish().unwrap();
    assert_eq!(ledger.reap_scope_ready(process.id(), 1).unwrap(), 1);
    process.close_external().unwrap();
    request_body.exited().unwrap();
    assert!(!request_role.try_retire().unwrap());
    request_report.consume(consumer()).unwrap();
    assert!(request_role.try_retire().unwrap());
    assert_eq!(ledger.reap_scope_ready(process.id(), 1).unwrap(), 1);
    assert!(request_retirement.is_retired());
    assert_eq!(ledger.used(dimension()), 0);
    ledger.remove_scope(process.id()).unwrap();
    assert!(ledger.is_empty());
}

fn original_retained_request_flow(failed_source: bool) {
    use novarocks_control_runtime::history::HistoryCapture;
    use novarocks_control_runtime_core::history::{
        EffectHistoryPhase, HistoryActorTarget, HistoryFact,
    };
    // This existing flow crosses both owners and the old execution Scope's
    // real removal; its trace must preserve those independently observed facts.
    let history = HistoryCapture::new(512);
    let limit = ResourceVector::new([(dimension(), 2)]).unwrap();
    let ledger = RuntimeLedger::with_history(limit, history.clone());
    let process = ledger.open_scope_next(None, generation()).unwrap();
    let resources = || {
        ReservationResources::new(
            quota(),
            OwnedSlotNeed {
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..OwnedSlotNeed::default()
            },
        )
    };
    let (request, mut request_funding) = process
        .reserve_next(
            generation(),
            resources(),
            WorkKind::Step,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Step)
        .unwrap();
    let request_work = request.reference();
    let request_retirement = request.retirement_observer();
    let request_report = ReportSlot::reserve_next(
        &mut request_funding,
        generation(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .unwrap();
    let context_generation = Generation::new(7).unwrap();
    let execution = ledger
        .open_scope_next(
            Some(ScopeParentAdmission {
                scope: process.id(),
                generation: generation(),
                kind: WorkKind::Cleanup,
                admission: WorkAdmission::Derived(request.id()),
            }),
            context_generation,
        )
        .unwrap();
    let (context_work, mut context_funding) = execution
        .reserve_next(
            context_generation,
            resources(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let context_reference = context_work.reference();
    assert_eq!(request_work.work, context_reference.work);
    assert_ne!(request_work.scope, context_reference.scope);
    let context_retirement = context_work.retirement_observer();
    let context_report = ReportSlot::reserve_next(
        &mut context_funding,
        context_generation,
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .unwrap();
    // This is a separate capture-free metadata owner, not a child whose join
    // could keep the actual physical/report role alive. Its unused input has
    // never acquired a payload and must survive a refused atomic detach.
    let (mut metadata_work, mut metadata_bundle) = execution
        .reserve_next(
            context_generation,
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    inputs: 1,
                    ..OwnedSlotNeed::default()
                },
            ),
            WorkKind::Step,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Step)
        .unwrap();
    metadata_work.start().unwrap();
    let metadata_retirement = metadata_work.retirement_observer();
    let actor = ActorId::new(1).unwrap();
    let runtime = ActorRuntime::with_history(history.clone());
    let registration = runtime
        .register_supervised_bound(
            actor,
            execution.clone(),
            DelayedRequestActor::default(),
            ActorConfig {
                mailbox: [2; 4],
                max_batches: 2,
                max_effects_per_step: 2,
                max_dependencies_per_effect: 0,
                turn_budget: 12,
                max_step_work: 2,
            },
            &context_work,
        )
        .unwrap();
    let original_registration = registration.clone();
    let original_epoch = runtime.snapshot(actor).unwrap().generation;
    let mut source_failure_proof = None;
    let failure_projection =
        PublishedSnapshot::new_for_actor(&registration, Arc::new(original_epoch));
    let mut metadata_source = runtime
        .reserve_admitted_input_for(&registration, actor, InputClass::Control, &metadata_work)
        .unwrap()
        .into_role()
        .unwrap();
    let metadata_source_author = metadata_source.author().unwrap();

    let permit = runtime
        .reserve_admitted_input_for(&registration, actor, InputClass::Ordinary, &request)
        .unwrap();
    let author = permit.author().unwrap();
    assert_eq!(author.work(), request_work);
    assert_eq!(author.generation(), generation());
    assert_eq!(original_epoch, context_generation);
    assert_ne!(author.generation(), original_epoch);
    let reply = PublishedReply::new_for_input(&author);
    // Even a second genuine position funded by this same Work is not this R's
    // reply target. Its unused position is withdrawn before any payload binds.
    let other = runtime
        .reserve_admitted_input_for(&registration, actor, InputClass::Ordinary, &request)
        .unwrap();
    let other_author = other.author().unwrap();
    assert!(!author.same_binding(&other_author));
    let wrong_reply = PublishedReply::new_for_input(&other_author);
    other.revoke_unused().unwrap();
    // This actual second input is authored by the same process Work, before
    // that lease moves to its ExitRole. It grants no new Work or execution debt.
    let mut failure_input = failed_source.then(|| {
        runtime
            .reserve_admitted_input_for(&registration, actor, InputClass::Control, &request)
            .unwrap()
    });
    let (request_role, mut request_body) = ExitRole::from_admitted(
        ReportedWork::try_new(request, request_funding, request_report.clone()).unwrap(),
    );
    request_body.start().unwrap();
    let (context_role, mut context_body) = ExitRole::from_admitted(
        ReportedWork::try_new(context_work, context_funding, context_report.clone()).unwrap(),
    );
    context_body.start().unwrap();
    assert_eq!(ledger.used(dimension()), 2);
    let input = permit.fill(DelayedRequestInput::Request).unwrap();
    assert!(runtime.poll_one(Tick(1)).unwrap());
    let mut host = runtime.claim_host_actor().unwrap();
    assert!(host.matches_registration(&original_registration));

    execution.close_external().unwrap();
    context_body.exited().unwrap();
    assert!(!context_role.try_retire().unwrap());
    assert_eq!(ledger.used(dimension()), 2);
    context_report.consume(consumer()).unwrap();
    assert!(context_role.try_retire().unwrap());
    assert_eq!(ledger.reap_scope_ready(execution.id(), 1).unwrap(), 1);
    assert!(context_retirement.is_retired());
    assert_eq!(execution.snapshot().unwrap().work, 1);
    assert_eq!(execution.snapshot().unwrap().storage, 1);
    assert_eq!(ledger.used(dimension()), 1);
    let original_metadata_inventory = metadata_bundle.resources().unwrap();
    assert_eq!(original_metadata_inventory.owned_slots.inputs, 1);
    let before_detach = execution.snapshot().unwrap();
    // Only the actual metadata owner and actor attachment remain. The pending
    // process-R Business must refuse atomic detach without exiting that owner.
    assert_eq!(
        host.detach_execution_with_metadata_sources(
            &process,
            &mut [(&mut metadata_work, &mut metadata_bundle)],
            &mut [&mut metadata_source],
        ),
        Err(DriverError::ActorNotRetired)
    );
    assert_eq!(execution.snapshot().unwrap(), before_detach);
    assert_eq!(
        metadata_bundle.resources().unwrap(),
        original_metadata_inventory
    );
    assert!(!metadata_bundle.is_released());
    assert!(!metadata_work.actual_exit_ready().unwrap());
    assert!(!metadata_retirement.is_retired());
    assert!(registration.execution_attached());
    assert!(metadata_source_author.same_binding(&metadata_source.author().unwrap()));
    assert_eq!(metadata_source.in_flight().unwrap(), None);
    let business = runtime
        .take_effect(actor, input, ActionId::new(1).unwrap())
        .unwrap();
    assert_eq!(business.descriptor.work, request_work);
    assert_eq!(business.effect, DelayedRequestEffect::Business);
    runtime.validate_effect_ticket(&business).unwrap();
    // The test host actually consumes this neutral command before reporting its
    // dispatch complete. No batch or effect is removed to bypass its real state.
    runtime
        .effect_completed_for(&registration, business)
        .unwrap();
    runtime.retire_input(actor, input).unwrap();
    host.detach_execution_with_metadata_sources(
        &process,
        &mut [(&mut metadata_work, &mut metadata_bundle)],
        &mut [&mut metadata_source],
    )
    .unwrap();
    // Absence of the actual header plus this same WorkLease's successful exit
    // observation checks both the ledger commit and the owner's local flags.
    assert!(metadata_retirement.is_retired());
    assert!(metadata_work.actual_exit_ready().unwrap());
    assert!(metadata_bundle.is_released());
    assert_eq!(metadata_source.in_flight(), Err(DriverError::UnknownRole));
    assert_eq!(
        metadata_source
            .fill(DelayedRequestInput::SourcePanic)
            .unwrap_err()
            .0,
        DriverError::UnknownRole
    );
    assert_eq!(
        metadata_bundle.resources().unwrap(),
        ReservationResources::default()
    );
    let detached_execution = execution.snapshot().unwrap();
    assert_eq!(detached_execution.work, 0);
    assert_eq!(detached_execution.storage, 0);
    assert!(detached_execution.retirement_ready);
    assert!(registration.same_registration(&original_registration));
    assert_eq!(runtime.snapshot(actor).unwrap().generation, original_epoch);
    assert!(!registration.execution_attached());
    host.release(false).unwrap();
    ledger.remove_scope(execution.id()).unwrap();
    // Actual child removal also ends its intrinsic parent-link Work. Retained
    // operation does not wait on an unrelated process reaper turn to clear it.
    assert_eq!(process.snapshot().unwrap().work, 1);
    assert_eq!(ledger.reap_scope_ready(process.id(), 1).unwrap(), 0);
    assert_eq!(ledger.used(dimension()), 1);
    assert!(!request_retirement.is_retired());
    assert!(!request_role.try_retire().unwrap());
    assert!(!reply.is_committed());

    assert_eq!(runtime.advance_time(Tick(10), 1).unwrap(), 1);
    let tick = runtime.pending_timer_input(actor).unwrap().unwrap();
    assert!(runtime.poll_one(Tick(10)).unwrap());
    let mut host = runtime.claim_host_actor().unwrap();
    assert!(host.matches_registration(&original_registration));
    let delayed = runtime
        .take_effect(actor, tick, ActionId::new(1).unwrap())
        .unwrap();
    assert_eq!(delayed.descriptor.work, request_work);
    assert_eq!(delayed.generation, original_epoch);
    assert_eq!(delayed.author_generation, author.generation());
    assert_eq!(delayed.effect, DelayedRequestEffect::Reply(29));
    let value = match &delayed.effect {
        DelayedRequestEffect::Reply(value) => Arc::new(*value),
        _ => panic!("the actual delayed reply must carry its original value"),
    };
    let (error, original_value) = runtime
        .commit_reply(&delayed, &wrong_reply, value.clone())
        .unwrap_err();
    assert_eq!(error, DriverError::PublicationTargetMismatch);
    assert!(Arc::ptr_eq(&value, &original_value));
    assert!(!wrong_reply.is_committed());
    runtime
        .commit_reply(&delayed, &reply, original_value)
        .unwrap();
    assert_eq!(reply.take_once().as_deref(), Some(&29));
    assert!(reply.take_once().is_none());
    runtime
        .effect_completed_for(&registration, delayed)
        .unwrap();
    assert_eq!(
        runtime.retire_input(actor, tick),
        Err(DriverError::MetadataStillPending)
    );
    let notice = host.take_metadata(tick).unwrap().unwrap();
    assert_eq!(*notice.value(), DelayedRequestEffect::RetainedNotice(73));
    // A different real actor owns the destination window. A reserved original
    // input exhausts that window without inventing any request or Work quota.
    let (mut destination_work, mut destination_storage) = process
        .reserve_next(
            generation(),
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..OwnedSlotNeed::default()
                },
            ),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let destination_retirement = destination_work.retirement_observer();
    let destination_report = ReportSlot::reserve_next(
        &mut destination_storage,
        generation(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .unwrap();
    destination_work
        .bind_owned_captures(&mut destination_storage, 1)
        .unwrap();
    destination_work.start().unwrap();
    let destination = runtime
        .register_supervised_bound(
            ActorId::new(2).unwrap(),
            process.clone(),
            DelayedRequestActor {
                own_metadata_work: Some(destination_work.reference()),
                ..Default::default()
            },
            ActorConfig {
                mailbox: [1; 4],
                max_batches: 2,
                max_effects_per_step: 2,
                max_dependencies_per_effect: 0,
                turn_budget: 12,
                max_step_work: 2,
            },
            &destination_work,
        )
        .unwrap();
    let blocker = runtime
        .reserve_admitted_input_for(
            &destination,
            destination.actor(),
            InputClass::Control,
            &destination_work,
        )
        .unwrap();
    // Consume any ready notification owed by the already completed reply
    // before isolating the Full-to-credit-wake transition below.
    host.release(false).unwrap();
    if let Some(previous) = runtime.claim_host_actor() {
        assert!(previous.matches_registration(&registration));
        previous.release(false).unwrap();
    }
    assert!(runtime.claim_host_actor().is_none());
    let (error, original_metadata) = runtime
        .forward_metadata_for(
            &registration,
            &destination,
            &notice,
            InputClass::Control,
            DelayedRequestInput::Metadata(73),
        )
        .unwrap_err();
    assert_eq!(error, DriverError::InputCapacity);
    assert!(matches!(
        original_metadata,
        DelayedRequestInput::Metadata(73)
    ));
    assert!(
        runtime
            .metadata_waiting_for(&registration, &notice)
            .unwrap()
    );
    assert_eq!(runtime.snapshot(destination.actor()).unwrap().inputs, 1);
    // A separate actual process-owned operation has a predeclared Core leg,
    // including when its consumer's execution Scope no longer exists. Its
    // synchronous body really ends; the unconsumed report still holds quota.
    let (report_work, mut report_funding) = process
        .reserve_next(
            generation(),
            ReservationResources::new(
                quota(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    ..OwnedSlotNeed::default()
                },
            ),
            WorkKind::Step,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Step)
        .unwrap();
    let original_report = ReportSlot::reserve_next(
        &mut report_funding,
        generation(),
        1,
        [(
            consumer(),
            ConsumerKind::Owner {
                scope: registration.scope_id(),
                generation: registration.initial_epoch(),
            },
        )],
    )
    .unwrap();
    assert_eq!(
        original_report
            .bind_actor_consumer(&runtime, &destination, consumer())
            .unwrap_err(),
        DriverError::PublicationTargetMismatch
    );
    let original_consumer = original_report
        .bind_actor_consumer(&runtime, &registration, consumer())
        .unwrap();
    let (report_role, mut report_body) = ExitRole::from_admitted(
        ReportedWork::try_new(report_work, report_funding, original_report.clone()).unwrap(),
    );
    report_body.start().unwrap();
    report_body.exited().unwrap();
    assert!(!report_role.try_retire().unwrap());
    assert_eq!(ledger.used(dimension()), 2);
    assert_eq!(
        runtime.failure_proof_for(&registration).unwrap_err(),
        DriverError::ActorNotRetired
    );
    if let Some(original) = failure_input.take() {
        let failed = original.fill(DelayedRequestInput::SourcePanic).unwrap();
        assert!(runtime.poll_one(Tick(10)).unwrap());
        assert!(runtime.snapshot(actor).unwrap().failed);
        assert!(!process.gates().unwrap().failed);
        // The failure custodian receives the real panic payload; it never
        // resumes or interrogates the failed protocol Core.
        let payload = runtime.take_failure_payload(actor).unwrap().unwrap();
        assert_eq!(
            payload.downcast_ref::<&str>().copied(),
            Some("the original retained source fails after committing its notice")
        );
        drop(payload);
        runtime.retire_input(actor, failed).unwrap();
        let failed_host = runtime.claim_host_actor().unwrap();
        assert!(failed_host.matches_registration(&registration));
        failed_host.release(false).unwrap();
        assert!(
            runtime
                .metadata_waiting_for(&registration, &notice)
                .unwrap()
        );
        assert!(reply.is_committed());
        let proof = runtime.failure_proof_for(&registration).unwrap();
        assert!(
            proof
                .registration()
                .same_registration(&original_registration)
        );
        assert_eq!(proof.initial_epoch(), original_epoch);
        assert_eq!(proof.failed_epoch(), original_epoch.next().unwrap());
        // The original Core Tick still owns its committed metadata batch.
        // A custodian deadline cannot consume that position or resume the Core.
        let original_inputs = runtime.snapshot(actor).unwrap();
        assert_eq!(
            runtime.take_failed_custodian_deadline(&proof).unwrap(),
            None
        );
        runtime
            .arm_failed_custodian_deadline(&proof, Tick(20))
            .unwrap();
        runtime
            .arm_failed_custodian_deadline(&proof, Tick(20))
            .unwrap();
        assert_eq!(
            runtime
                .arm_failed_custodian_deadline(&proof, Tick(21))
                .unwrap_err(),
            DriverError::CustodianDeadlineMismatch
        );
        assert_eq!(runtime.next_deadline(), Some(Tick(20)));
        assert_eq!(runtime.advance_time(Tick(19), 1).unwrap(), 0);
        assert_eq!(
            runtime.take_failed_custodian_deadline(&proof).unwrap(),
            None
        );
        assert_eq!(runtime.advance_time(Tick(20), 1).unwrap(), 0);
        assert_eq!(runtime.pending_timer_input(actor).unwrap(), Some(tick));
        assert_eq!(runtime.snapshot(actor).unwrap(), original_inputs);
        assert_eq!(
            runtime.take_failed_custodian_deadline(&proof).unwrap(),
            None
        );
        source_failure_proof = Some(proof.clone());
        let cleanup_before = process.snapshot().unwrap();
        let mut session = runtime
            .claim_actor_failure_publication(&proof, &destination_work)
            .unwrap();
        assert!(
            session
                .registration()
                .same_registration(&original_registration)
        );
        assert!(!session.registration().same_registration(&destination));
        assert_eq!(session.generation(), proof.failed_epoch());
        assert!(session.matches_actor_runtime(&runtime, actor).unwrap());
        assert!(
            !session
                .matches_actor_runtime(&runtime, destination.actor())
                .unwrap()
        );
        // The original value was already taken. Failure cannot reopen or
        // overwrite the same oncecell, and refusal returns the exact new value.
        let attempted = Arc::new(99);
        let (error, original) = session.commit_reply(&reply, attempted.clone()).unwrap_err();
        assert_eq!(error, DriverError::ReplyAlreadyCommitted);
        assert!(Arc::ptr_eq(&original, &attempted));
        assert!(reply.is_committed());
        assert!(reply.take_once().is_none());
        // The original actor's bootstrap projection also survives removal of
        // its hot Scope. Publish only the actual failure epoch from its proof.
        assert_eq!(
            *session
                .commit_snapshot(&failure_projection, Arc::new(proof.failed_epoch()))
                .unwrap(),
            original_epoch
        );
        assert_eq!(*failure_projection.read(), proof.failed_epoch());
        session.finish().unwrap();
        assert_eq!(
            runtime
                .claim_actor_failure_publication(&proof, &destination_work)
                .unwrap_err(),
            DriverError::FailurePublicationAlreadyClaimed
        );
        // Closing publication neither fails the Root financial Scope nor
        // observes the Cleanup's independent body, captures or report exit.
        assert!(!process.gates().unwrap().failed);
        assert_eq!(process.snapshot().unwrap(), cleanup_before);
        assert!(!destination_work.actual_exit_ready().unwrap());
        assert!(!destination_retirement.is_retired());
        assert_eq!(
            destination_report.consumption(consumer()).unwrap(),
            novarocks_control_runtime_core::completion::Consumption::Pending
        );
        assert!(!destination_report.try_retire().unwrap());
        original_consumer.abandon(&proof).unwrap();
        assert_eq!(
            original_report.consumption(consumer()).unwrap(),
            novarocks_control_runtime_core::completion::Consumption::AbandonedByOwnerFailure
        );
        assert!(original_consumer.consume().is_err());
    } else {
        original_consumer.consume().unwrap();
        assert_eq!(
            original_report.consumption(consumer()).unwrap(),
            novarocks_control_runtime_core::completion::Consumption::Consumed
        );
        assert!(original_consumer.consume().is_err());
    }
    assert!(report_role.try_retire().unwrap());
    assert_eq!(ledger.reap_scope_ready(process.id(), 1).unwrap(), 1);
    assert_eq!(ledger.used(dimension()), 1);
    // Full alone cannot spin the source. Returning the actual target credit
    // wakes that exact original source and reserves its place before new work.
    assert!(runtime.claim_host_actor().is_none());
    blocker.revoke_unused().unwrap();
    assert!(
        !runtime
            .metadata_waiting_for(&registration, &notice)
            .unwrap()
    );
    assert_eq!(
        runtime
            .reserve_input(
                destination.actor(),
                destination.actor(),
                InputClass::Control,
            )
            .err(),
        Some(DriverError::InputCapacity)
    );
    let host = runtime.claim_host_actor().unwrap();
    assert!(host.matches_registration(&registration));
    let delivery = runtime
        .forward_metadata_for(
            &registration,
            &destination,
            &notice,
            InputClass::Control,
            original_metadata,
        )
        .unwrap();
    assert_eq!(delivery.consumption(), MetadataConsumption::Pending);
    let (error, notice) = runtime
        .metadata_completed_for(&registration, notice)
        .unwrap_err();
    assert_eq!(error, DriverError::MetadataStillPending);
    host.release(false).unwrap();
    assert!(runtime.poll_one(Tick(10)).unwrap());
    assert_eq!(delivery.consumption(), MetadataConsumption::Consumed);
    assert!(
        runtime
            .input_custody(destination.actor(), delivery.input())
            .unwrap()
            .is_some()
    );
    let mut source_host = None;
    let mut destination_host = None;
    for _ in 0..2 {
        let claimed = runtime.claim_host_actor().unwrap();
        if claimed.matches_registration(&registration) {
            assert!(source_host.replace(claimed).is_none());
        } else {
            assert!(claimed.matches_registration(&destination));
            assert!(destination_host.replace(claimed).is_none());
        }
    }
    let host = source_host.unwrap();
    let destination_host = destination_host.unwrap();
    // Source consumption does not wait for the destination's physical effect
    // tail; that same target batch owns the tail until actual completion.
    runtime
        .metadata_completed_for(&registration, notice)
        .unwrap();

    runtime.retire_input(actor, tick).unwrap();
    let host = if let Some(proof) = source_failure_proof.as_ref() {
        // Returning the actual old Tick releases the deferred original
        // deadline. Its delivery wakes only this same failed actor's host.
        host.release(false).unwrap();
        // Completing the original metadata ticket above already marked this
        // claimed source dirty. Observe that distinct real completion notice
        // before testing that the custodian's deadline is the next wake.
        let completion_host = runtime.claim_host_actor().unwrap();
        assert!(completion_host.matches_registration(&original_registration));
        assert_eq!(runtime.take_failed_custodian_deadline(proof).unwrap(), None);
        completion_host.release(false).unwrap();
        assert!(runtime.claim_host_actor().is_none());
        let original_inputs = runtime.snapshot(actor).unwrap();
        let original_resources = process.snapshot().unwrap();
        assert_eq!(runtime.next_deadline(), Some(Tick(20)));
        assert_eq!(runtime.advance_time(Tick(20), 1).unwrap(), 1);
        let host = runtime.claim_host_actor().unwrap();
        assert!(host.matches_registration(&original_registration));
        assert_eq!(runtime.pending_timer_input(actor).unwrap(), None);
        assert!(!runtime.poll_one(Tick(20)).unwrap());
        assert_eq!(runtime.snapshot(actor).unwrap(), original_inputs);
        assert_eq!(process.snapshot().unwrap(), original_resources);
        assert_eq!(
            runtime.take_failed_custodian_deadline(proof).unwrap(),
            Some(Tick(20))
        );
        assert_eq!(runtime.take_failed_custodian_deadline(proof).unwrap(), None);
        runtime
            .arm_failed_custodian_deadline(proof, Tick(20))
            .unwrap();
        assert_eq!(runtime.next_deadline(), None);
        assert_eq!(runtime.advance_time(Tick(21), 1).unwrap(), 0);
        assert_eq!(
            runtime.close_timer_source(actor).unwrap(),
            TimerSourceState::ClosedDrained
        );
        assert_eq!(
            runtime
                .arm_failed_custodian_deadline(proof, Tick(20))
                .unwrap_err(),
            DriverError::Closed
        );
        host
    } else {
        host
    };
    assert_eq!(
        runtime.snapshot(actor).unwrap().generation,
        if failed_source {
            original_epoch.next().unwrap()
        } else {
            original_epoch
        }
    );
    assert_eq!(ledger.used(dimension()), 1);
    assert!(!request_role.body_exited());
    assert!(!request_role.try_retire().unwrap());
    runtime.close_actor_admission(actor).unwrap();
    let retired = host.retire_actor().unwrap();
    if let Some(proof) = source_failure_proof.as_ref() {
        assert_eq!(
            runtime
                .arm_failed_custodian_deadline(proof, Tick(20))
                .unwrap_err(),
            DriverError::UnknownActor
        );
    }
    assert!(retired.deadline.is_none());
    assert!(retired.author.is_none());
    assert!(runtime.snapshot(destination.actor()).is_ok());
    let applied = runtime
        .take_effect(
            destination.actor(),
            delivery.input(),
            ActionId::new(1).unwrap(),
        )
        .unwrap();
    assert_eq!(applied.effect, DelayedRequestEffect::MetadataApplied(73));
    let snapshot = PublishedSnapshot::new_for_actor(&destination, Arc::new(0u64));
    assert_eq!(
        *runtime
            .commit_snapshot(&applied, &snapshot, Arc::new(73))
            .unwrap(),
        0
    );
    runtime.effect_completed_for(&destination, applied).unwrap();
    assert!(
        runtime
            .input_custody(destination.actor(), delivery.input())
            .unwrap()
            .is_none()
    );
    assert_eq!(delivery.consumption(), MetadataConsumption::Consumed);

    process.close_external().unwrap();
    request_body.exited().unwrap();
    assert!(request_role.actual_exit_ready().unwrap());
    assert!(!request_role.try_retire().unwrap());
    assert_eq!(ledger.used(dimension()), 1);
    request_report.consume(consumer()).unwrap();
    assert!(request_role.try_retire().unwrap());
    assert_eq!(ledger.reap_scope_ready(process.id(), 1).unwrap(), 1);
    assert!(request_retirement.is_retired());
    assert_eq!(ledger.used(dimension()), 0);
    destination_work.body_exited().unwrap();
    assert!(!destination_work.actual_exit_ready().unwrap());
    destination_work
        .captures_exited_with_report(&destination_report)
        .unwrap();
    destination_report.body_exited().unwrap();
    destination_report.captures_exited().unwrap();
    assert!(!destination_report.try_retire().unwrap());
    destination_report.consume(consumer()).unwrap();
    assert!(destination_report.try_retire().unwrap());
    assert!(destination_work.actual_exit_ready().unwrap());
    destination_storage.finish().unwrap();
    assert!(!destination_retirement.is_retired());
    assert_eq!(ledger.reap_scope_ready(process.id(), 1).unwrap(), 1);
    assert!(destination_retirement.is_retired());
    runtime.close_actor_admission(destination.actor()).unwrap();
    let destination_actor = destination_host.retire_actor().unwrap();
    assert!(destination_actor.author.is_none());
    assert!(runtime.is_empty());
    assert_eq!(process.snapshot().unwrap().storage, 0);
    ledger.remove_scope(process.id()).unwrap();
    assert!(ledger.is_empty());
    let trace = history.drain(512);
    assert!(trace.complete);
    let original_target = HistoryActorTarget {
        actor,
        attachment: registration.attachment(),
        scope_origin: execution.id(),
    };
    let delayed_effects: Vec<_> = trace
        .entries
        .iter()
        .filter_map(|entry| match entry.fact {
            HistoryFact::Effect {
                target,
                input,
                action,
                actor_generation,
                financial_work,
                author_generation,
                phase,
            } if target == original_target
                && input == tick
                && action == ActionId::new(1).unwrap() =>
            {
                assert_eq!(actor_generation, original_epoch);
                assert_eq!(financial_work, request_work);
                assert_eq!(author_generation, generation());
                assert_ne!(target.scope_origin, financial_work.scope);
                assert_ne!(actor_generation, author_generation);
                Some(phase)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        delayed_effects,
        vec![
            EffectHistoryPhase::Dispatched,
            EffectHistoryPhase::Published,
            EffectHistoryPhase::Completed
        ]
    );
    let fixed_publications: Vec<_> = trace
        .entries
        .iter()
        .filter_map(|entry| match entry.fact {
            HistoryFact::FailurePublication {
                target,
                actor_generation,
                financial_work,
                financial_generation,
                ..
            } if target == original_target => {
                assert_eq!(actor_generation, original_epoch.next().unwrap());
                assert_eq!(financial_work, destination_work.reference());
                assert_eq!(financial_generation, generation());
                assert_ne!(target.scope_origin, financial_work.scope);
                assert_ne!(actor_generation, financial_generation);
                Some(())
            }
            _ => None,
        })
        .collect();
    assert_eq!(fixed_publications.len(), usize::from(failed_source));
}
