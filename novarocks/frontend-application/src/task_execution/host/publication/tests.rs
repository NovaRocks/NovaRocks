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

use super::*;
use crate::task_execution::protocol_facts::AttemptProtocolFactsReceiver;
use novarocks_attempt_protocol::publication::{ProtocolPublication, ProtocolTaskChange};
use novarocks_execution_contract::{TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion};
use novarocks_physical_plan::PlanVersionId;
use novarocks_query_application::api::PlanSeal;
use novarocks_result_contract::PerKind;
use std::collections::BTreeSet;

pub(in crate::task_execution::host) fn fixture() -> (
    PublicationRecord,
    AttemptProtocolFactsReceiver,
    TaskIdentity,
    PlanSeal,
) {
    // Reuse the real application-bound graph before any Task or lifecycle work.
    let round = crate::task_execution::tests::manifest_unstarted_regression_fixture();
    let graph = round.execution().graph();
    let root = graph.root_identity();
    let plan = PlanSeal::Version(PlanVersionId::try_new([192; 16]).unwrap());
    let (protocol, receiver) = AttemptProtocolFactsPublisher::for_graph(
        plan,
        graph,
        &BTreeSet::new(),
        &PerKind {
            write_counts: None,
            delete_verification: None,
        },
    )
    .unwrap();
    let activation = AttemptActivationIdentity::from_parts(92, root.query_execution_id(), 1);
    (
        PublicationRecord {
            activation,
            root: None,
            protocol,
            profile: None,
            diagnostic_records: None,
            ready: None,
            #[cfg(novarocks_dev_controls)]
            root_observer: None,
        },
        receiver,
        root,
        plan,
    )
}

pub(in crate::task_execution::host) fn batch(
    revision: u64,
    changes: Vec<ProtocolTaskChange>,
) -> Publication {
    Publication::ProtocolBatch(ProtocolPublication {
        owner_revision: revision,
        initial_membership: (revision == 0).then(|| std::sync::Arc::new(Default::default())),
        changes,
        selected_failure: None,
        root_success_sealed: false,
    })
}

#[test]
fn protocol_publication_preserves_actual_publish_and_noop_receipts() {
    let (mut record, mut receiver, root, plan) = fixture();
    let activation = record.activation;
    assert_eq!(
        record.publish(&batch(0, Vec::new()), None).unwrap(),
        PublicationApplied::ProtocolBatch { published: true }
    );
    let initial = receiver
        .take_changes(activation, root, plan)
        .unwrap()
        .unwrap();
    assert!(initial.is_initial());
    let initial_revision = initial.revision();
    assert_eq!(
        record.publish(&batch(0, Vec::new()), None).unwrap(),
        PublicationApplied::ProtocolBatch { published: false }
    );
    assert!(
        receiver
            .take_changes(activation, root, plan)
            .unwrap()
            .is_none()
    );

    let status = TaskStatus::try_new(
        root,
        TaskStatusVersion::new(1).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::default(),
    )
    .unwrap();
    assert_eq!(
        record
            .publish(
                &batch(
                    1,
                    vec![ProtocolTaskChange {
                        identity: root,
                        status: Some(status.clone()),
                        first_writer_witness: None,
                    }]
                ),
                None
            )
            .unwrap(),
        PublicationApplied::ProtocolBatch { published: true }
    );
    let delta = receiver
        .take_changes(activation, root, plan)
        .unwrap()
        .unwrap();
    assert!(!delta.is_initial());
    assert_eq!(delta.revision(), initial_revision + 1);
    assert_eq!(delta.tasks().len(), 1);
    assert_eq!(delta.tasks()[0].identity(), root);
    assert_eq!(delta.tasks()[0].status(), Some(&status));
    assert_eq!(
        record.publish(&batch(1, Vec::new()), None).unwrap(),
        PublicationApplied::ProtocolBatch { published: false }
    );
    assert!(
        receiver
            .take_changes(activation, root, plan)
            .unwrap()
            .is_none()
    );
}

#[test]
fn protocol_publication_preserves_typed_error_without_readmodel_mutation() {
    let (mut record, mut receiver, root, plan) = fixture();
    let activation = record.activation;
    record.publish(&batch(0, Vec::new()), None).unwrap();
    receiver
        .take_changes(activation, root, plan)
        .unwrap()
        .unwrap();
    // An unknown Task must be rejected by the original frozen slot author.
    let foreign = TaskIdentity::new(
        root.query_execution_id(),
        root.stage_id(),
        novarocks_types::TaskId::new(u32::MAX).unwrap(),
        root.backend_process_id(),
    );
    let result = record.publish(
        &batch(
            1,
            vec![ProtocolTaskChange {
                identity: foreign,
                status: None,
                first_writer_witness: None,
            }],
        ),
        None,
    );
    assert!(matches!(
        result,
        Err(PublicationError::Protocol(TaskExecutionError::Schedule(_)))
    ));
    assert!(
        receiver
            .take_changes(activation, root, plan)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        record.publish(&batch(1, Vec::new()), None).unwrap(),
        PublicationApplied::ProtocolBatch { published: true }
    );
    assert!(
        receiver
            .take_changes(activation, root, plan)
            .unwrap()
            .is_some()
    );
}

#[test]
fn final_info_publication_checks_original_profile_before_resolving_a_reference() {
    let (mut record, _, root, _) = fixture();
    let reference = novarocks_attempt_protocol::input::FinalTaskInfoRef::new(
        novarocks_coordination_contract::AttemptRequestToken::new(std::num::NonZeroU64::MIN),
        root,
    );
    assert!(matches!(
        record.publish(&Publication::FinalTaskInfo(reference), None),
        Err(PublicationError::MissingProfileObserver)
    ));
}

#[tokio::test]
async fn original_root_request_uses_ordered_control_author_and_shared_reply_cell() {
    use crate::{
        control_runtime::{
            actor::{FrontendActor, FrontendEffect, FrontendInput, TestActor},
            transport_withdrawal::AttemptEffectEnvelope,
        },
        task_execution::{
            bridge::root_control::{RootControlRequests, reply_kind},
            host::{
                adapter::descriptor,
                failure_publication::FailurePublications,
                observations::{
                    MetadataRetirementRefusal, ObservationBudget, ObservationError,
                    ObservationMetadataLayout, ObservationMetadataObligation,
                    OrderedObservationInput, RootControlBinding, StreamKey,
                    credits::ObservationCredits,
                    read_progress::ReadProgressInput,
                    source::{BoundObservationSource, ObservationMetadataAuthor},
                    tests::ManualClock,
                },
            },
        },
    };
    use novarocks_attempt_protocol::{
        effect::{AttemptEffect, EffectBody, EffectClass},
        input::{AttemptInput, ExecutorFact},
    };
    use novarocks_control_runtime::{
        actor::{ActorConfig, ActorStep},
        actor_turn::ActorRuntime,
        obligation::OwnedPositionObligation,
        transaction::RuntimeLedger,
    };
    use novarocks_control_runtime_core::{
        capacity::{ReservationResources, ResourceVector},
        effect::EffectKind,
        identity::{ActorId, Generation, Tick},
        mailbox::{InputClass, InputEnvelope},
        owned_storage::{OwnedSlotKind, OwnedSlotNeed},
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_coordination_contract::{
        AttemptActionId, AttemptToLogical, AttemptToLogicalBody, LogicalToAttemptBody,
    };
    use novarocks_execution_contract::ParticipationRef;
    use novarocks_query_application::coordination::accepted_root_status_projection_with_control_port;
    use novarocks_types::FrontendProcessId;
    use std::{num::NonZeroU64, sync::Mutex, task::Waker};

    // The test supplies a real Capture obligation as the fixture's original
    // metadata backing. It deliberately refuses its first retirement.
    struct FixtureMetadata(Mutex<Option<OwnedPositionObligation>>);
    struct FixtureBacking {
        capture: Option<OwnedPositionObligation>,
        refused: bool,
    }
    impl ObservationMetadataAuthor for FixtureMetadata {
        fn retain(
            &self,
            required: ObservationMetadataLayout,
            participants: usize,
        ) -> Result<Box<dyn ObservationMetadataObligation>, ObservationError> {
            assert_eq!(required.total_positions, 3);
            assert_eq!(participants, 1);
            Ok(Box::new(FixtureBacking {
                capture: self.0.lock().unwrap().take(),
                refused: false,
            }))
        }
    }
    impl ObservationMetadataObligation for FixtureBacking {
        fn retire_after_exit(mut self: Box<Self>) -> Result<(), MetadataRetirementRefusal> {
            if !self.refused {
                self.refused = true;
                return Err(MetadataRetirementRefusal {
                    error: ObservationError::Busy,
                    original: self,
                });
            }
            self.capture.take().unwrap().retire_after_exit().unwrap();
            Ok(())
        }
    }

    struct ReplyActor;
    impl TestActor for ReplyActor {
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            value: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            let FrontendInput::Attempt(input) = value else {
                panic!("original root input")
            };
            if matches!(
                *input,
                AttemptInput::Executor(ExecutorFact::ReadProgress(_))
            ) {
                return;
            }
            let AttemptInput::Port(port) = *input else {
                panic!("original root port")
            };
            let body = match port.body {
                LogicalToAttemptBody::TerminalControlDemand { token, .. } => {
                    AttemptToLogicalBody::TerminalControlResult {
                        token,
                        result: Ok(()),
                    }
                }
                LogicalToAttemptBody::SealRequest { token } => AttemptToLogicalBody::SealResult {
                    token,
                    result: Ok(Arc::from([])),
                },
                _ => panic!("original root control kind"),
            };
            let id = AttemptActionId::new(NonZeroU64::MIN);
            let effect = AttemptEffectEnvelope::new(AttemptEffect {
                id,
                class: EffectClass::Reply,
                body: EffectBody::Port(AttemptToLogical {
                    activation: port.activation,
                    action: id,
                    body,
                }),
            });
            let author = step.input_author().unwrap().clone();
            step.effect_from_author(
                &author,
                descriptor(id.get(), author.work(), EffectKind::Reply),
                FrontendEffect::Attempt(effect),
            )
            .unwrap();
        }
    }

    let ledger = RuntimeLedger::new(ResourceVector::default());
    let generation = Generation::new(1).unwrap();
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let bridge = ledger.open_scope_next(None, generation).unwrap();
    let budget = ObservationBudget {
        queued_frames: 1,
        total_positions: 3,
        max_frame_bytes: 2048,
        control_bytes: 512,
        total_bytes: 2560,
    };
    let (mut observations, mut positions) = scope
        .reserve_next(
            generation,
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    inputs: budget.total_positions,
                    captures: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    let (mut control, mut control_payer) = bridge
        .reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    observations.start().unwrap();
    control.start().unwrap();
    let runtime = ActorRuntime::new();
    let actor = ActorId::new(1).unwrap();
    let source = ActorId::new(2).unwrap();
    let registration = runtime
        .register_bound(
            actor,
            scope.clone(),
            FrontendActor::Test(Box::new(ReplyActor)),
            ActorConfig {
                mailbox: [8; 4],
                max_batches: 4,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 8,
                max_step_work: 1,
            },
        )
        .unwrap_or_else(|_| panic!("original root actor"));
    let (mut publication, _, root, _) = fixture();
    let activation = publication.activation;
    let binding =
        RootControlBinding::for_registered_source(registration.clone(), activation, root).unwrap();
    let failure = Arc::new(FailurePublications::new(
        registration.clone(),
        activation,
        &scope,
    ));
    let requests = RootControlRequests::new(binding.clone(), failure);
    let participant = ParticipationRef::new(
        root.query_execution_id(),
        FrontendProcessId::new_v7(),
        root.backend_process_id(),
    );
    let credits =
        ObservationCredits::bind(budget, [participant], &observations, &mut positions).unwrap();
    let mut capture = positions.take_position(OwnedSlotKind::Capture).unwrap();
    capture.bind().unwrap();
    let reserve = |class, work| {
        runtime
            .reserve_admitted_input_for(&registration, source, class, work)
            .unwrap()
            .into_standing_input()
            .unwrap()
    };
    let progress = ReadProgressInput::new(
        &registration,
        participant,
        reserve(InputClass::Ordinary, &observations),
    );
    let pool = BoundObservationSource::new(
        binding.clone(),
        requests.clone(),
        credits,
        [(participant, progress.clone())].into(),
        Arc::new(FixtureMetadata(Mutex::new(Some(capture)))),
    );
    let control_position = reserve(InputClass::Control, &control);
    binding.bind_reply_author(control_position.author().unwrap());
    let ingress = OrderedObservationInput::new(
        reserve(InputClass::Lifecycle, &observations),
        control_position,
        pool.clone(),
        binding,
        Arc::new(ManualClock::default()),
        |input| FrontendInput::Attempt(Box::new(input)),
        Waker::noop().clone(),
    )
    .unwrap_or_else(|_| panic!("original ordered root Source"));
    requests.bind_ingress(&ingress);
    let key = StreamKey {
        participation: participant,
        generation: NonZeroU64::MIN,
    };
    let foreign = StreamKey {
        participation: ParticipationRef::new(
            root.query_execution_id(),
            FrontendProcessId::new_v7(),
            root.backend_process_id(),
        ),
        ..key
    };
    assert_eq!(
        ingress.register_stream(foreign),
        Err(ObservationError::UnknownStream)
    );
    ingress.register_stream(key).unwrap();
    assert_eq!(
        ingress.register_stream(key),
        Err(ObservationError::GenerationAlreadyRegistered)
    );
    let original_read = ingress.reserve_read(key).await.unwrap();
    ingress.job_closed(key).unwrap();
    assert_eq!(ingress.retire_stream(key), Err(ObservationError::Busy));
    drop(original_read);
    assert_eq!(ingress.retire_stream(key), Err(ObservationError::Busy));
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert!(progress.retire_ready().unwrap());
    ingress.retire_stream(key).unwrap();
    assert_eq!(
        ingress.retire_stream(key),
        Err(ObservationError::UnknownStream)
    );
    let (sender, status) =
        accepted_root_status_projection_with_control_port(root, requests.clone());
    sender
        .publish_attempt_observation(
            TaskStatus::try_new(
                root,
                TaskStatusVersion::new(1).unwrap(),
                TaskState::Finished,
                None,
                TaskOutputFacts::new(true),
            )
            .unwrap(),
            AcceptedAttemptFailure::None,
        )
        .unwrap();
    publication.root = Some(sender);

    for seal in [false, true] {
        let mut reply = if seal {
            status.begin_success_seal_request()
        } else {
            status.begin_terminal_control_request()
        }
        .unwrap();
        assert!(status.begin_terminal_control_request().is_err());
        assert!(runtime.poll_one(Tick(1)).unwrap());
        let batches = runtime.committed_batches(actor, None, 4).unwrap();
        assert_eq!(batches.len(), 1);
        let input = batches[0].1;
        let (action, _) = runtime
            .effect_inventory(actor, input, None, 1)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(action.work, control.reference());
        let ticket = runtime.take_effect(actor, input, action.id).unwrap();
        let FrontendEffect::Attempt(effect) = &ticket.effect else {
            panic!("original root reply effect")
        };
        let EffectBody::Port(port) = &effect.protocol.body else {
            panic!("original root reply port")
        };
        let (token, _) = reply_kind(&port.body).unwrap();
        requests
            .commit_reply(&runtime, &ticket, Arc::new(port.clone()), None)
            .unwrap();
        assert!(matches!(
            reply.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        runtime.effect_completed(ticket).unwrap();
        runtime.retire_input(actor, input).unwrap();
        ingress.root_control_processed(input).unwrap();
        ingress.input_retired(input).unwrap();
        requests.deliver(token, &mut publication).unwrap();
        reply.await.unwrap().unwrap();
    }
    assert!(
        publication.root.is_none(),
        "success moved the original publisher"
    );
    let original_storage = scope.snapshot().unwrap().storage;
    assert_eq!(ingress.close_after_exit(), Err(ObservationError::Busy));
    let before_metadata = scope.snapshot().unwrap().storage;
    assert!(before_metadata < original_storage);
    ingress.close_after_exit().unwrap();
    assert_eq!(scope.snapshot().unwrap().storage + 1, before_metadata);
    for (work, payer, owner) in [
        (&mut observations, &mut positions, &scope),
        (&mut control, &mut control_payer, &bridge),
    ] {
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        payer.finish().unwrap();
        owner.close_external().unwrap();
        ledger.reap_scope_ready(owner.id(), 16).unwrap();
    }
    while let Some(turn) = runtime.claim_host_actor() {
        turn.release(false).unwrap();
    }
    runtime.remove_actor(actor).unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    ledger.remove_scope(bridge.id()).unwrap();
    assert!(runtime.is_empty() && ledger.is_empty());
}
