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

use super::*;
use crate::task_execution::host::observations::{
    ObservationBudget,
    tests::{Bank, ManualClock},
};
use crate::task_execution::intent::OperationAcknowledgement;
use novarocks_attempt_protocol::{
    config::{CoordinationBudgets, FrozenTransportLimits},
    effect::Publication,
    ids::IdSeed,
    input::{PublicationApplied, SendGeneration},
    start::{
        AttemptSources, FrozenAttemptPlan, FrozenCreation, FrozenEstablish, FrozenTask,
        FrozenTaskGraph, PayloadRef,
    },
};
use novarocks_control_runtime::{causal_input::CausalInputObligation, transaction::RuntimeLedger};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    owned_storage::{OwnedSlotKind, OwnedSlotNeed},
    scope::WorkAdmission,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivation, AttemptActivationIdentity, AttemptAuthorizationLimits,
    AttemptCompletionPolicy, AttemptLifecyclePolicy,
};
use novarocks_execution_contract::{AdmissionEpochCapability, OperationKind};
use novarocks_types::NativeCompatibilityId;
use std::num::NonZeroUsize;

fn start() -> AttemptStart {
    let legacy = crate::task_execution::tests::manifest_unstarted_regression_fixture();
    let graph = legacy.execution().graph();
    let root = graph.root_identity();
    let original = graph.task(root.task_id()).unwrap();
    let participation = original.participation();
    let backend = participation.backend_process_id();
    let activation = AttemptActivationIdentity::from_parts(92, root.query_execution_id(), 1);
    let coordination = CoordinationBudgets::DEFAULT;
    let deployment_window = NonZeroUsize::new(coordination.dispatch.create_permits()).unwrap();
    let source = |id| AttemptSourceId(positive(id));
    AttemptStart {
        activation: AttemptActivation {
            identity: activation,
            policy: AttemptLifecyclePolicy {
                completion: AttemptCompletionPolicy::AcceptedRootSuccessSeal,
                retain_diagnostics: false,
            },
            inherited_admissions: Box::new([]),
        },
        plan: FrozenAttemptPlan {
            graph: FrozenTaskGraph {
                root,
                tasks: [(
                    root.task_id(),
                    FrozenTask {
                        identity: root,
                        participation,
                        task_key: original.task_key(),
                        fragment_id: original.fragment_id(),
                        split_plan_nodes: Box::new([]),
                        creation: FrozenCreation {
                            resource: PayloadRef(positive(1)),
                            queued_bytes: 256,
                            plan_carrier_bytes: 64,
                            descriptor_encoded_bytes: 64,
                        },
                    },
                )]
                .into(),
                stages: [(root.stage_id(), Box::<[_]>::from([root]))].into(),
                edges: Default::default(),
            },
            authorization_limits: AttemptAuthorizationLimits {
                activation,
                establish_per_participation: NonZeroUsize::MIN,
                abort_per_participation: NonZeroUsize::MIN,
            },
            writers: Default::default(),
            summary_sources: Default::default(),
            compatibility: NativeCompatibilityId::new([0x41; 32]),
            admission_epochs: [(
                backend,
                AdmissionEpochCapability::try_from_bytes([0x61; 16]).unwrap(),
            )]
            .into(),
            preparation_positions: [(backend, deployment_window)].into(),
            deployment_window,
            establish_material: [(
                participation,
                FrozenEstablish {
                    material: PayloadRef(positive(2)),
                    queued_bytes: 256,
                },
            )]
            .into(),
            coordination,
            transport: FrozenTransportLimits {
                max_batch_items: 4,
                max_batch_encoded_bytes: 4096,
                max_descriptor_encoded_bytes: 4096,
                max_query_backend_queued_operations: 1024,
                max_query_backend_queued_bytes: 1_048_576,
                max_backend_queued_operations: 4096,
                max_backend_queued_bytes: 4_194_304,
                max_tasks_per_participation: 1024,
                max_active_tasks_per_backend: 1024,
                frontend_queue_residence: Duration::from_secs(5),
            },
            retry: Default::default(),
            pumps: Box::new([]),
            initial_lease: Duration::from_secs(30),
            steady_lease: Duration::from_secs(5),
        },
        ids: IdSeed::new(1, 100),
        initial_now: MonotonicInstant::from_origin(Duration::ZERO),
        sources: AttemptSources {
            actor: source(1),
            publication: source(2),
            qa: source(3),
            process: source(4),
            native: source(5),
            observation: source(6),
            capacity: source(7),
            membership: source(8),
            mailbox: [8; 4],
        },
        observe_dev_root: false,
    }
}

#[test]
fn original_runtime_wakes_capacity_and_projects_isolated_failure() {
    let runtime = ActorRuntime::<FrontendActor>::new();
    let transport = novarocks_task_codec::TransportBudget::new(
        1,
        128,
        64,
        2,
        1024,
        4,
        4096,
        1,
        2,
        Duration::from_secs(1),
    )
    .unwrap();
    let layout = NativeTransportCapacity::from_transport(transport).unwrap();
    let ledger =
        RuntimeLedger::with_keyed_families(layout.process_limits(), layout.keyed_families())
            .unwrap();
    let generation = Generation::new(1).unwrap();
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let reserve = |kind| {
        scope
            .reserve_next(
                generation,
                ReservationResources::for_work(ResourceVector::default()),
                kind,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(kind)
            .unwrap()
    };
    let (mut failure_work, mut failure_capacity) = reserve(WorkKind::Cleanup);
    let (mut original_work, mut original_capacity) = reserve(WorkKind::Settlement);
    original_work.start().unwrap();
    let start = start();
    let members: Arc<[FrozenMember]> = start
        .plan
        .establish_material
        .keys()
        .map(|participation| FrozenMember {
            participation: *participation,
            endpoint: novarocks_execution_contract::RuntimeEndpoint::new("127.0.0.1", 19091)
                .unwrap(),
        })
        .collect();
    let activation = start.activation.identity;
    let root = start.plan.graph.root;
    let target = start.plan.graph.root.backend_process_id();
    let sources = start.sources;
    let max_step_work = start.minimum_core_step_units().unwrap() + OUTPUT_UNITS;
    let mut registered = AttemptActor::register(
        &runtime,
        start,
        &members,
        scope.clone(),
        layout.clone(),
        ActorConfig {
            mailbox: sources.mailbox,
            max_batches: 8,
            max_effects_per_step: OUTPUT_UNITS,
            max_dependencies_per_effect: 0,
            turn_budget: 4 * (max_step_work + 1),
            max_step_work,
        },
        &failure_work,
    )
    .unwrap();
    let registration = registered.original.clone();
    let actor = registration.actor();
    let membership = registered
        .bind_membership(&runtime, &original_work)
        .unwrap()
        .target;
    assert!(membership.registration.same_registration(&registration));
    assert!(Arc::ptr_eq(&membership.members, &members));
    assert!(matches!(
        registered.bind_membership(&runtime, &original_work),
        Err(AttemptRegistrationError::InvalidConfig(_))
    ));
    assert!(
        runtime
            .standing_input_inventory(actor, None, 8)
            .unwrap()
            .iter()
            .any(|row| row.source == runtime_source(sources.membership)
                && row.class == InputClass::Ordinary
                && row.in_flight.is_none())
    );
    let mut original = CausalInputObligation::reserve_admitted(
        &runtime,
        &registration,
        actor,
        InputClass::Lifecycle,
        &original_work,
    )
    .unwrap();
    let author = original.event_author().unwrap();
    let mut capacity_source = registered
        .bind(
            &runtime,
            AttemptEffectAuthors {
                business: author.clone(),
                publication: author.clone(),
                reply: author.clone(),
                cleanup: author.clone(),
                settlement: author,
            },
            &original_capacity,
            &original_work,
        )
        .unwrap();

    let budget = ObservationBudget {
        queued_frames: 1,
        total_positions: 3,
        max_frame_bytes: 2048,
        control_bytes: 512,
        total_bytes: 2560,
    };
    let (mut observation_work, mut observation_capacity) = scope
        .reserve_next(
            generation,
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    inputs: budget.total_positions,
                    ..Default::default()
                },
            ),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    observation_work.start().unwrap();
    let positions = (0..budget.total_positions)
        .map(|_| {
            let mut slot = observation_capacity
                .take_position(OwnedSlotKind::Input)
                .unwrap();
            slot.bind().unwrap();
            slot
        })
        .collect();
    let lookalike =
        RootControlBinding::for_registered_source(registration.clone(), activation, root).unwrap();
    assert!(!registered.root_control_binding().same_original(&lookalike));
    let pool = Bank::make(budget, positions, registered.root_control_binding().clone());
    let clock = Arc::new(ManualClock::default());
    let inventory = || runtime.standing_input_inventory(actor, None, 16).unwrap();
    let before = inventory();
    let control_scope = ledger.open_scope_next(None, generation).unwrap();
    let (mut root_control_work, mut root_control_capacity) = control_scope
        .reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    root_control_work.start().unwrap();
    let mut bind = |source: Arc<dyn ObservationSourcePool>| {
        registered.bind_observations(
            &runtime,
            &observation_work,
            &root_control_work,
            source,
            clock.clone(),
            Waker::noop().clone(),
        )
    };
    pool.refuse_metadata_once();
    assert!(matches!(bind(pool.clone()), Err(ObservationError::Budget)));
    assert_eq!(inventory(), before);
    let observations = bind(pool.clone()).unwrap();
    assert!(matches!(bind(pool.clone()), Err(ObservationError::Busy)));
    let [lifecycle, control] = observations.standing_ids().map(|id| {
        inventory()
            .into_iter()
            .find(|row| row.standing_input_id == id)
            .unwrap()
    });
    assert_eq!(lifecycle.source, runtime_source(sources.observation));
    assert_eq!(control.source, lifecycle.source);
    assert_eq!(lifecycle.class, InputClass::Lifecycle);
    assert_eq!(control.class, InputClass::Control);

    let before_progress = inventory();
    let read_progress = registered
        .bind_read_progress(&runtime, &observation_work, None)
        .unwrap();
    assert!(
        read_progress
            .keys()
            .eq(members.iter().map(|member| &member.participation))
    );
    let progress_positions: Vec<_> = inventory()
        .into_iter()
        .filter(|row| !before_progress.contains(row))
        .collect();
    assert_eq!(progress_positions.len(), members.len());
    assert!(
        progress_positions
            .iter()
            .all(|row| { row.source == lifecycle.source && row.class == InputClass::Ordinary })
    );
    assert!(matches!(
        registered.bind_read_progress(&runtime, &observation_work, None),
        Err(ObservationError::Busy)
    ));
    for role in read_progress.values() {
        role.close_after_exit().unwrap();
    }

    let before_owner_controls = inventory();
    let mut owner_controls = registered
        .prepare_owner_controls(&runtime, &original_work)
        .unwrap();
    let owner_controls_inventory: Vec<_> = inventory()
        .into_iter()
        .filter(|row| !before_owner_controls.contains(row))
        .collect();
    assert_eq!(owner_controls_inventory.len(), 2);
    assert!(
        owner_controls_inventory
            .iter()
            .all(|row| { row.source == actor && row.class == InputClass::Control })
    );

    let before_abort_route = inventory();
    let abort_router = crate::query_execution::native_execution_adapter::LogicalAbortRouter::new();
    let (abort_route, abort_ingress, abort_settlement) = abort_router
        .prepare_registered_attempt(&mut registered, &runtime, &original_work)
        .unwrap();
    let abort_positions: Vec<_> = inventory()
        .into_iter()
        .filter(|row| !before_abort_route.contains(row))
        .collect();
    assert_eq!(abort_positions.len(), members.len());
    assert!(
        abort_positions
            .iter()
            .all(|row| row.source == actor && row.class == InputClass::Control)
    );
    assert!(
        abort_router
            .prepare_registered_attempt(&mut registered, &runtime, &original_work)
            .is_err()
    );

    let post = |input: AttemptInput| FrontendInput::Attempt(Box::new(input));
    let mut startup = registered
        .reserve_standing(&runtime, actor, InputClass::Lifecycle, &original_work)
        .unwrap();
    let bootstrap = startup
        .fill(FrontendInput::AttemptStartup(StartupInput::Bootstrap))
        .unwrap();
    assert_eq!(startup.in_flight().unwrap(), Some(bootstrap));
    assert!(runtime.poll_one(Tick(1)).unwrap());
    for (_, input) in runtime.committed_batches(actor, None, 8).unwrap() {
        runtime.retire_input(actor, input).unwrap();
    }

    let other = BackendProcessId::new_v7();
    let occupy = |backend, items| {
        scope
            .reserve_next(
                generation,
                layout
                    .queue_resources(backend, TransportCapacityLane::Ordinary, items, 32)
                    .unwrap(),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
    };
    let binding = registered.binding.get().unwrap();
    let target_blocker = occupy(target, 2);
    assert!(
        matches!(binding.reserve_queue(&scope, &layout, false, target, 1, 32),
        Err(QueueStage::TargetFull(denied)) if denied == target)
    );
    assert!(capacity_source.input.in_flight().unwrap().is_none());
    drop(target_blocker);
    let first_wake = capacity_source
        .input
        .in_flight()
        .unwrap()
        .expect("actual target release wakes the actor");

    let process_target = occupy(target, 2);
    let process_other = occupy(other, 1);
    assert!(matches!(
        binding.reserve_queue(&scope, &layout, false, other, 1, 32),
        Err(QueueStage::ProcessFull)
    ));
    drop(process_other);
    drop(process_target);
    assert_eq!(capacity_source.input.in_flight().unwrap(), Some(first_wake));
    assert!(
        binding
            .reserve_queue(&scope, &layout, false, other, 1, 32)
            .is_ok()
    );
    assert!(!capacity_source.retire_ready().unwrap());
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert!(capacity_source.retire_ready().unwrap());
    let second_wake = capacity_source
        .input
        .in_flight()
        .unwrap()
        .expect("actual process release was coalesced");
    assert_ne!(first_wake, second_wake);

    let publication = (0..8)
        .find_map(|_| {
            assert_eq!(runtime.advance_time(Tick(1), 1).unwrap(), 1);
            assert!(
                runtime.poll_one(Tick(1)).unwrap(),
                "the owned continuation must wake"
            );
            let batches = runtime.committed_batches(actor, None, 8).unwrap();
            let mut publication = None;
            for (_, input) in batches {
                let inventory = runtime
                    .effect_inventory(actor, input, None, OUTPUT_UNITS)
                    .unwrap();
                if let Some((descriptor, _)) = inventory.first() {
                    assert!(publication.is_none());
                    publication = Some(runtime.take_effect(actor, input, descriptor.id).unwrap());
                } else {
                    runtime.retire_input(actor, input).unwrap();
                }
            }
            publication
        })
        .expect("initial protocol publication within the original round");
    let FrontendEffect::Attempt(effect) = &publication.effect else {
        panic!("expected the original Core publication");
    };
    assert!(matches!(
        effect.protocol.body,
        EffectBody::Publish(Publication::ProtocolBatch(_))
    ));
    assert_eq!(publication.descriptor.work, original_work.reference());
    let effect = effect.protocol.id;
    let input = publication.input;
    runtime.effect_completed(publication).unwrap();
    runtime.retire_input(actor, input).unwrap();
    assert_eq!(runtime.advance_time(Tick(1), 1).unwrap(), 0);
    assert!(
        !runtime.poll_one(Tick(1)).unwrap(),
        "handoff does not complete the original publisher"
    );

    assert!(
        !capacity_source.retire_ready().unwrap(),
        "capacity cannot cross the selected publication"
    );
    let applied = runtime
        .reserve_admitted_input(
            actor,
            runtime_source(sources.publication),
            InputClass::Lifecycle,
            &original_work,
        )
        .unwrap()
        .fill(post(AttemptInput::Executor(
            ExecutorFact::PublicationApplied {
                effect,
                result: Ok(PublicationApplied::ProtocolBatch { published: true }),
            },
        )))
        .unwrap();
    assert!(runtime.poll_one(Tick(1)).unwrap());
    runtime.retire_input(actor, applied).unwrap();
    assert!(
        capacity_source.retire_ready().unwrap(),
        "actual publication allows the deferred wake"
    );
    assert!(capacity_source.input.in_flight().unwrap().is_none());

    let wrong = runtime
        .reserve_admitted_input(actor, actor, InputClass::Control, &original_work)
        .unwrap()
        .fill(post(AttemptInput::StartProtocol))
        .unwrap();
    let key = SendKey {
        operation: TaskOperationId::new_v7(),
        generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::MIN)),
    };
    let mut receipt = registered
        .reserve_native_receipt(&runtime, &original_work, key, OperationKind::CreateTask)
        .unwrap();
    let original_ack =
        OperationAcknowledgement::transport_unknown(key.operation, OperationKind::CreateTask);
    receipt
        .record(key, original_ack.clone(), None, None)
        .unwrap();
    assert!(
        runtime
            .standing_input_inventory(actor, None, 16)
            .unwrap()
            .iter()
            .any(|row| {
                row.source == runtime_source(sources.native)
                    && row.class == InputClass::Lifecycle
                    && row.in_flight.is_none()
            })
    );
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert!(runtime.snapshot(actor).unwrap().failed);
    assert!(matches!(receipt.submit(), Err(ActorTurnError::ActorFailed)));
    let failed = runtime.take_failure_payload(actor).unwrap();
    assert!(failed.is_some());
    let actions: Vec<_> = runtime
        .committed_batches(actor, None, 8)
        .unwrap()
        .into_iter()
        .flat_map(|(_, input)| {
            runtime
                .effect_inventory(actor, input, None, OUTPUT_UNITS)
                .unwrap()
                .into_iter()
                .map(move |(effect, _)| (input, effect))
        })
        .collect();
    let [(input, descriptor)] = actions.as_slice() else {
        panic!("only one effect from immutable failure authority");
    };
    let cleanup = runtime.take_effect(actor, *input, descriptor.id).unwrap();
    runtime.validate_failure_effect_ticket(&cleanup).unwrap();
    assert_eq!(cleanup.descriptor.work, failure_work.reference());
    assert!(
        matches!(&cleanup.effect, FrontendEffect::AttemptOwnerFailed {
        scope: original_scope, generation: failed, failed_input,
    } if *original_scope == scope.id() && *failed != generation && failed_input.input == wrong)
    );
    failure_work
        .start_in_generation(cleanup.generation)
        .unwrap();
    {
        let session = runtime.claim_failure_publication(&cleanup).unwrap();
        assert!(session.registration().same_registration(&registration));
        assert_eq!(session.generation(), cleanup.generation);
    }
    runtime.effect_completed(cleanup).unwrap();
    runtime.retire_input(actor, *input).unwrap();
    runtime.retire_input(actor, wrong).unwrap();
    assert!(
        runtime.remove_actor(actor).is_err(),
        "handoff does not prove the Work exit"
    );

    original_work.body_exited().unwrap();
    original_work.captures_exited().unwrap();
    let std::task::Poll::Ready(Some(retained)) = receipt.retire_ready_after_exit().unwrap() else {
        panic!("the failure owner retains the raw acknowledgement");
    };
    assert_eq!(retained.original, original_ack);
    assert!(
        matches!(retained.unpublished, Some(FrontendInput::Attempt(input))
        if matches!(&*input, AttemptInput::Executor(ExecutorFact::Operation(observation))
            if observation.generation == key.generation))
    );
    assert!(abort_ingress.close_after_exit().unwrap());
    drop(abort_route);
    drop(abort_settlement);
    startup.close_after_exit().unwrap();
    owner_controls.close_after_exit().unwrap();
    owner_controls.close_after_exit().unwrap();
    membership.close_after_exit().unwrap();
    observation_work.body_exited().unwrap();
    observation_work.captures_exited().unwrap();
    observations.close_after_exit().unwrap();
    root_control_work.body_exited().unwrap();
    root_control_work.captures_exited().unwrap();
    root_control_capacity.finish().unwrap();
    control_scope.close_external().unwrap();
    control_scope.reap_ready(8).unwrap();
    ledger.remove_scope(control_scope.id()).unwrap();
    pool.retire_positions();
    capacity_source.close_after_exit().unwrap();
    original.close_after_exit().unwrap();
    failure_work.body_exited().unwrap();
    failure_work.captures_exited().unwrap();
    original_capacity.finish().unwrap();
    observation_capacity.finish().unwrap();
    failure_capacity.finish().unwrap();
    scope.close_external().unwrap();
    ledger.reap_scope_ready(scope.id(), 8).unwrap();
    while let Some(turn) = runtime.claim_host_actor() {
        turn.release(false).unwrap();
    }
    runtime.remove_actor(actor).unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(runtime.is_empty() && ledger.is_empty());
}

#[test]
fn bound_observation_backing_waits_for_actual_strong_weak_and_progress_aliases() {
    use crate::task_execution::host::observations::{
        backing::OBSERVATION_BACKING_BYTES, credits::OBSERVATION_WINDOW_BYTES,
    };

    let runtime = ActorRuntime::<FrontendActor>::new();
    let layout =
        NativeTransportCapacity::from_transport(novarocks_task_codec::TransportBudget::DEFAULT)
            .unwrap();
    let budget = ObservationBudget::for_task_attempt();
    let quota = budget
        .byte_window()
        .unwrap()
        .checked_add(&ObservationBackingQuote::process_limit(budget, NonZeroUsize::MIN).unwrap())
        .unwrap();
    let ledger = RuntimeLedger::new(quota);
    let generation = Generation::new(1).unwrap();
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let control_scope = ledger.open_scope_next(None, generation).unwrap();
    let reserve = |scope: &ScopeHandle, resources, kind| {
        scope
            .reserve_next(generation, resources, kind, WorkAdmission::External)
            .unwrap()
            .bind(kind)
            .unwrap()
    };
    let (mut failure, mut failure_capacity) =
        reserve(&scope, ReservationResources::work_only(), WorkKind::Cleanup);
    let start = start();
    let participants = start
        .plan
        .establish_material
        .keys()
        .map(|participation| FrozenMember {
            participation: *participation,
            endpoint: novarocks_execution_contract::RuntimeEndpoint::new("127.0.0.1", 19091)
                .unwrap(),
        })
        .collect();
    let max_step_work = start.minimum_core_step_units().unwrap() + OUTPUT_UNITS;
    let config = ActorConfig {
        mailbox: start.sources.mailbox,
        max_batches: 8,
        max_effects_per_step: OUTPUT_UNITS,
        max_dependencies_per_effect: 0,
        turn_budget: 4 * (max_step_work + 1),
        max_step_work,
    };
    let mut attempt = AttemptActor::register(
        &runtime,
        start,
        &participants,
        scope.clone(),
        layout,
        config,
        &failure,
    )
    .unwrap();
    let actor = attempt.original.actor();
    let requests = RootControlRequests::new(
        attempt.root_control_binding().clone(),
        attempt.failure_publications().clone(),
    );
    let resources = attempt.observation_resources().unwrap();
    let bytes = resources.quota.get(OBSERVATION_BACKING_BYTES);
    let (mut work, mut positions) = reserve(&scope, resources, WorkKind::Settlement);
    let (mut root_work, mut root_capacity) = reserve(
        &control_scope,
        ReservationResources::work_only(),
        WorkKind::Settlement,
    );
    work.start().unwrap();
    root_work.start().unwrap();
    let ingress = attempt
        .bind_observation_source(
            &runtime,
            ObservationSourceBacking {
                work: &work,
                positions: &mut positions,
            },
            &root_work,
            &requests,
            Arc::new(ManualClock::default()),
            Waker::noop().clone(),
        )
        .unwrap();
    let usage = || {
        ledger
            .try_capacity_dimensions([OBSERVATION_WINDOW_BYTES, OBSERVATION_BACKING_BYTES])
            .unwrap()
            .unwrap()
            .used
    };
    assert_eq!(usage(), [budget.total_bytes as u64, bytes]);
    assert!(positions.resources().unwrap().is_empty());

    let extra = ingress.clone();
    let weak = ingress.weak();
    let progress = attempt.read_progress_roles.as_ref().unwrap()[0].clone();
    ingress.close_after_exit().unwrap();
    assert_eq!(usage(), [0, bytes]);
    drop(attempt);
    drop(ingress);
    assert!(weak.upgrade().is_some());
    drop(extra);
    assert!(weak.upgrade().is_none());
    assert_eq!(usage(), [0, bytes]);
    drop(weak);
    assert_eq!(usage(), [0, bytes]);
    drop(progress);
    assert_eq!(usage(), [0, 0]);
    drop(requests);

    failure.start().unwrap();
    for (work, reservation) in [
        (&mut failure, &mut failure_capacity),
        (&mut work, &mut positions),
        (&mut root_work, &mut root_capacity),
    ] {
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        reservation.finish().unwrap();
    }
    for original in [&scope, &control_scope] {
        original.close_external().unwrap();
        original.reap_ready(8).unwrap();
    }
    while let Some(turn) = runtime.claim_host_actor() {
        turn.release(false).unwrap();
    }
    runtime.remove_actor(actor).unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    ledger.remove_scope(control_scope.id()).unwrap();
    assert!(runtime.is_empty() && ledger.is_empty());
}
