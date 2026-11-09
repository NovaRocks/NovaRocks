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
use novarocks_attempt_protocol::{
    config::{CoordinationBudgets, FrozenTransportLimits},
    effect::Publication,
    ids::IdSeed,
    input::PublicationApplied,
    start::{
        AttemptSources, FrozenAttemptPlan, FrozenCreation, FrozenEstablish, FrozenTask,
        FrozenTaskGraph, PayloadRef,
    },
};
use novarocks_control_runtime::{causal_input::CausalInputObligation, transaction::RuntimeLedger};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    scope::WorkAdmission,
};
use novarocks_coordination_contract::{
    AttemptActivation, AttemptActivationIdentity, AttemptAuthorizationLimits,
    AttemptCompletionPolicy, AttemptLifecyclePolicy,
};
use novarocks_execution_contract::AdmissionEpochCapability;
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
                        fragment_id: i32::try_from(original.fragment_id()).unwrap(),
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
    let target = start.plan.graph.root.backend_process_id();
    let sources = start.sources;
    let max_step_work = start.minimum_core_step_units().unwrap() + OUTPUT_UNITS;
    let mut registered = AttemptActor::register(
        &runtime,
        start,
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

    let post = |input: AttemptInput| FrontendInput::Attempt(Box::new(input));
    original
        .fill_with_receipt(None, post(AttemptInput::Bootstrap))
        .unwrap_or_else(|failure| panic!("original input fill: {:?}", failure.error));
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
        effect.body,
        EffectBody::Publish(Publication::ProtocolBatch(_))
    ));
    assert_eq!(publication.descriptor.work, original_work.reference());
    let effect = effect.id;
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
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert!(runtime.snapshot(actor).unwrap().failed);
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
    capacity_source.close_after_exit().unwrap();
    original.close_after_exit().unwrap();
    failure_work.body_exited().unwrap();
    failure_work.captures_exited().unwrap();
    original_capacity.finish().unwrap();
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
