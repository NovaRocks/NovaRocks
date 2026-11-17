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

#[path = "tests/current_covered_recovery.rs"]
mod current_covered_recovery;

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
    // Reuse the SAME frozen graph without constructing a retired round owner.
    let graph = crate::task_execution::graph_manifest_tests::unstarted_chain_graph();
    let root = graph.root_identity();
    let plan = PlanSeal::Version(PlanVersionId::try_new([192; 16]).unwrap());
    let (protocol, receiver) = AttemptProtocolFactsPublisher::for_graph(
        plan,
        &graph,
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
        Err(PublicationError::Protocol(original)) if matches!(*original, TaskExecutionError::Schedule(_))
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
    use crate::control_runtime::logical::adapter::{LogicalEffectEnvelope, LogicalHostEffect};
    use crate::{
        control_runtime::{
            actor::{FrontendActor, FrontendEffect, FrontendInput, TestActor},
            transport_withdrawal::AttemptEffectEnvelope,
        },
        task_execution::host::{
            adapter::descriptor,
            observations::{
                MetadataRetirementRefusal, ObservationBudget, ObservationError,
                ObservationMetadataLayout, ObservationMetadataObligation, OrderedObservationInput,
                RootControlBinding, StreamKey,
                credits::{OBSERVATION_WINDOW_BYTES, ObservationCredits},
                host_turn::OrderedObservationHost,
                read_progress::ReadProgressInput,
                source::{BoundObservationSource, ObservationMetadataAuthor},
                tests::ManualClock,
            },
            port::{
                control::{
                    ControlBinding, ControlPositions, PortControlError, SourcePublishedSlot,
                },
                facts::{AttemptFactBank, FactRole},
                root_control::RootControlRequests,
            },
        },
    };
    use novarocks_attempt_protocol::port_output::{AttemptToLogical, AttemptToLogicalBody};
    use novarocks_attempt_protocol::{
        effect::{AttemptEffect, EffectBody, EffectClass},
        input::{
            AttemptInput, ExecutorFact, ObservationEvent, ObservationFact, ObservationFailure,
        },
    };
    use novarocks_control_runtime::{
        actor::{ActorConfig, ActorStep},
        actor_turn::ActorRuntime,
        completion::ReportSlot,
        obligation::OwnedPositionObligation,
        reported_work::ReportedWork,
        transaction::RuntimeLedger,
    };
    use novarocks_control_runtime_core::{
        capacity::ReservationResources,
        completion::ConsumerKind,
        effect::{EffectKind, EffectState},
        identity::{ActionId, ActorId, ConsumerId, Generation, ScopedWorkRef, Tick},
        mailbox::{InputClass, InputEnvelope},
        owned_storage::{OwnedSlotKind, OwnedSlotNeed},
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_coordination_contract::{
        AttemptActionId, AttemptRequestToken, LogicalToAttempt, LogicalToAttemptBody,
    };
    use novarocks_execution_contract::ParticipationRef;
    use novarocks_logical_execution_protocol::{
        LogicalActionId,
        effect::{LogicalEffect, LogicalEffectBody},
    };
    use novarocks_types::FrontendProcessId;
    use std::{
        future::ready,
        num::NonZeroU64,
        pin::Pin,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    #[derive(Default)]
    struct CloseWakes(AtomicUsize);
    impl Wake for CloseWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

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
            assert!(step.charge(1));
            let FrontendInput::Attempt(input) = value else {
                panic!("original root input")
            };
            if matches!(
                *input,
                AttemptInput::Executor(
                    ExecutorFact::ReadProgress(_) | ExecutorFact::Observations(_)
                )
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

    // SAME LP financial Work emits the original root commands, then receives
    // the AP facts on three separately prepaid standing positions.
    struct LogicalActor {
        next: Arc<Mutex<Option<FrontendEffect>>>,
        seen: Arc<Mutex<Vec<novarocks_coordination_contract::logical_port::AttemptToLogical>>>,
        work: ScopedWorkRef,
    }
    impl TestActor for LogicalActor {
        fn handle(
            &mut self,
            _: Tick,
            envelope: InputEnvelope,
            value: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            assert!(step.charge(1));
            match value {
                FrontendInput::Test(_) => {
                    assert_eq!(envelope.class, InputClass::Control);
                    let original = self.next.lock().unwrap().take().unwrap();
                    step.effect(descriptor(1, self.work, EffectKind::Cleanup), original)
                        .unwrap();
                }
                FrontendInput::Logical(input) => {
                    let novarocks_logical_execution_protocol::input::LogicalInput::AttemptPort(
                        message,
                    ) = *input
                    else {
                        panic!("only the original AP reply enters this LP fixture");
                    };
                    assert_eq!(envelope.class, InputClass::Lifecycle);
                    self.seen.lock().unwrap().push(*message);
                }
                _ => panic!("unexpected original LP input"),
            }
        }
    }

    let budget = ObservationBudget {
        queued_frames: 1,
        total_positions: 3,
        max_frame_bytes: 2048,
        control_bytes: 512,
        total_bytes: 2560,
    };
    let ledger = RuntimeLedger::new(budget.byte_window().unwrap());
    let used_bytes = || {
        ledger
            .try_capacity_dimensions([OBSERVATION_WINDOW_BYTES])
            .unwrap()
            .unwrap()
            .used[0]
    };
    let generation = Generation::new(1).unwrap();
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let bridge = ledger.open_scope_next(None, generation).unwrap();
    let (mut observations, mut positions) = scope
        .reserve_next(
            generation,
            ReservationResources::new(
                budget.byte_window().unwrap(),
                OwnedSlotNeed {
                    work_headers: 1,
                    // Existing observation/read roles plus three final port roles.
                    inputs: budget.total_positions + 3,
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
            ReservationResources::new(
                Default::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    // Two LP command triggers, three AP fact roles, one original
                    // ordered Control input targeted at the AP.
                    inputs: 6,
                    ..Default::default()
                },
            ),
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
                turn_budget: 12,
                max_step_work: 2,
            },
        )
        .unwrap_or_else(|_| panic!("original root actor"));
    let (publication, _, root, _) = fixture();
    let activation = publication.activation;
    let publisher = AttemptPublicationOwner::new(publication, None);
    let next = Arc::new(Mutex::new(None));
    let received = Arc::new(Mutex::new(Vec::new()));
    let logical = runtime
        .register_bound(
            source,
            bridge.clone(),
            FrontendActor::Test(Box::new(LogicalActor {
                next: next.clone(),
                seen: received.clone(),
                work: control.reference(),
            })),
            ActorConfig {
                mailbox: [8; 4],
                max_batches: 4,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 12,
                max_step_work: 2,
            },
        )
        .unwrap_or_else(|_| panic!("original LP financial author"));
    let binding =
        RootControlBinding::for_registered_source(registration.clone(), activation, root).unwrap();
    let requests = RootControlRequests::new(binding.clone());
    publisher.bind_port_root(requests.clone());
    assert!(publisher.matches_root(&binding));
    let facts = AttemptFactBank::reserve(
        &runtime,
        &logical,
        &registration,
        activation,
        &control,
        Waker::noop().clone(),
        Waker::noop().clone(),
    )
    .unwrap();
    let mut triggers = (0..2)
        .map(|_| {
            runtime
                .reserve_admitted_input_for(
                    &logical,
                    registration.actor(),
                    InputClass::Control,
                    &control,
                )
                .unwrap()
                .into_standing_input()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let commands = [
        LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::TerminalControlDemand {
                token: AttemptRequestToken::new(NonZeroU64::new(1).unwrap()),
                root,
            },
        },
        LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::SealRequest {
                token: AttemptRequestToken::new(NonZeroU64::new(2).unwrap()),
            },
        },
    ];
    // Emit BOTH original tickets before accepting any observation prefix.
    // Looking for a later LP ticket must not accidentally consume that prefix.
    let mut command_tickets = Vec::new();
    for (index, command) in commands.iter().enumerate() {
        *next.lock().unwrap() = Some(FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope {
            reservation: control_payer.id(),
            body: LogicalHostEffect::Protocol(Box::new(LogicalEffect {
                action: LogicalActionId::new(NonZeroU64::new(index as u64 + 1).unwrap()),
                body: LogicalEffectBody::AttemptPort(Box::new(command.clone())),
            })),
        })));
        let input = triggers[index]
            .fill(FrontendInput::Test(index as u64))
            .unwrap();
        for _ in 0..8 {
            if runtime
                .effect_inventory(source, input, None, 1)
                .is_ok_and(|rows| !rows.is_empty())
            {
                break;
            }
            assert!(runtime.poll_one(Tick(1)).unwrap());
        }
        command_tickets.push(Some(
            runtime
                .take_effect(source, input, ActionId::new(1).unwrap())
                .unwrap(),
        ));
    }
    let participant = ParticipationRef::new(
        root.query_execution_id(),
        FrontendProcessId::new_v7(),
        root.backend_process_id(),
    );
    let credits =
        ObservationCredits::bind(budget, [participant], &observations, &mut positions, None)
            .unwrap();
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
    let wakes = Arc::new(CloseWakes::default());
    let ingress = OrderedObservationInput::new(
        reserve(InputClass::Lifecycle, &observations),
        control_position,
        pool.clone(),
        binding,
        Arc::new(ManualClock::default()),
        |input| FrontendInput::Attempt(Box::new(input)),
        Waker::from(wakes.clone()),
    )
    .unwrap_or_else(|_| panic!("original ordered root Source"));
    let different_binding =
        RootControlBinding::for_registered_source(registration.clone(), activation, root).unwrap();
    let foreign_requests = RootControlRequests::new(different_binding);
    assert!(matches!(
        foreign_requests.bind_ingress(&ingress),
        Err(PortControlError::ForeignPair)
    ));
    requests.bind_ingress(&ingress).unwrap();
    assert!(matches!(
        requests.bind_ingress(&ingress),
        Err(PortControlError::ForeignPair)
    ));
    let observer = ConsumerId::new(1).unwrap();
    let (storage_work, mut storage_payer) = bridge
        .reserve_child_next(
            generation,
            control_payer.id(),
            ReservationResources::new(
                Default::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Settlement,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    let storage_header = storage_work.retirement_observer();
    let report = ReportSlot::reserve_next(
        &mut storage_payer,
        generation,
        1,
        [(observer, ConsumerKind::Observer)],
    )
    .unwrap();
    let storage = ReportedWork::try_new(storage_work, storage_payer, report).unwrap();
    let (slot, mut reader) = SourcePublishedSlot::bind(
        &runtime,
        ControlBinding {
            lp: logical.clone(),
            ap: registration.clone(),
            activation,
            positions: ControlPositions {
                stop: reserve(InputClass::Control, &observations),
                convergence: reserve(InputClass::Control, &observations),
                changed: reserve(InputClass::Ordinary, &observations),
            },
            storage,
            observer,
            roots: requests.clone(),
        },
    )
    .unwrap_or_else(|error| panic!("same original paid root reader: {error:?}"));
    let ordered_host = OrderedObservationHost::new(ingress.clone());
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
    for (index, seal) in [false, true].into_iter().enumerate() {
        let prefix_key = StreamKey {
            generation: NonZeroU64::new(2).unwrap(),
            ..key
        };
        if seal {
            ingress.register_stream(prefix_key).unwrap();
            let mut credit = Some(ingress.reserve_read(prefix_key).await.unwrap());
            assert!(runtime.poll_one(Tick(1)).unwrap());
            assert!(progress.retire_ready().unwrap());
            let mut opened = ready(Ok::<(), ObservationFailure>(()));
            assert!(matches!(
                ingress
                    .poll_open(
                        prefix_key,
                        &mut credit,
                        Pin::new(&mut opened),
                        &mut Context::from_waker(Waker::noop()),
                    )
                    .unwrap()
                    .result,
                Poll::Ready(Ok(()))
            ));
            ingress
                .accept(
                    prefix_key,
                    ObservationEvent::Fact {
                        source_revision: None,
                        fact: ObservationFact::Bookmark {
                            generation: prefix_key.generation.get(),
                            sequence: 1,
                            covered_prefix: 1,
                            source_cut: 1,
                        },
                    },
                    32,
                    credit.take().unwrap(),
                )
                .unwrap();
        }
        let original_command = command_tickets[index].take().unwrap();
        slot.commit(&runtime, &original_command, &commands[index])
            .unwrap();
        assert_eq!(
            runtime
                .effect_inventory(source, original_command.input, None, 1)
                .unwrap()[0]
                .1,
            EffectState::Published
        );
        runtime.effect_completed(original_command).unwrap();
        assert!(triggers[index].retire_ready_after_exit().unwrap());
        reader.begin_pass();
        for _ in 0..4 {
            let progress = reader.progress_ordered(5).unwrap();
            if !progress.more {
                break;
            }
        }
        // Only the actual writer drives the already committed original row.
        // The Seal remains behind the accepted observation's FIFO prefix.
        ingress.drive().unwrap();
        assert!(matches!(
            requests.close_after_exit(),
            Err(PortControlError::UnsettledRoot)
        ));
        if seal {
            // This Seal has been accepted behind an actual observation. A
            // close request must fence new root materialization while leaving this FIFO
            // prefix and the already accepted Seal runnable.
            ordered_host.fence_root_controls();
            assert!(!ordered_host.ready_for_close());
            assert_eq!(ingress.close_after_exit(), Err(ObservationError::Busy));
            assert!(runtime.poll_one(Tick(1)).unwrap());
            let batches = runtime.committed_batches(actor, None, 4).unwrap();
            assert_eq!(batches.len(), 1);
            let prefix = batches[0].1;
            assert!(
                runtime
                    .effect_inventory(actor, prefix, None, 1)
                    .unwrap()
                    .is_empty()
            );
            runtime.retire_input(actor, prefix).unwrap();
            ingress.input_retired(prefix).unwrap();
            ingress.job_closed(prefix_key).unwrap();
            ingress.retire_stream(prefix_key).unwrap();
        }
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
        let original = effect.clone();
        let projected = novarocks_attempt_protocol::port_projection::project_attempt_message(
            port,
            &mut |_| None,
            &mut |_, _| None,
            &mut |_, _, _| None,
        )
        .unwrap();
        let (token, reply_is_seal) = match &projected.body {
            novarocks_coordination_contract::logical_port::AttemptToLogicalBody::SealResult { token, result } => {
                let AttemptToLogicalBody::SealResult { result: Ok(before), .. } = &port.body else {
                    panic!("same original Seal result");
                };
                assert!(Arc::ptr_eq(before, result.as_ref().unwrap()));
                (*token, true)
            }
            novarocks_coordination_contract::logical_port::AttemptToLogicalBody::TerminalControlResult { token, result } => {
                assert!(result.is_ok()); (*token, false)
            }
            _ => panic!("exact original root reply"),
        };
        assert_eq!(reply_is_seal, seal);
        assert_eq!(
            token,
            match &commands[index].body {
                LogicalToAttemptBody::SealRequest { token }
                | LogicalToAttemptBody::TerminalControlDemand { token, .. } => *token,
                _ => unreachable!(),
            }
        );
        assert_eq!(
            runtime.effect_inventory(actor, input, None, 1).unwrap(),
            vec![(action.clone(), EffectState::Started)],
            "the original AP reply ticket is started before its actual LP fact handoff"
        );
        let mut loan = facts.try_loan(FactRole::Lifecycle).unwrap();
        let handoff = loan
            .publish_root_reply_now(&runtime, &ticket, projected.clone())
            .unwrap();
        drop(loan);
        assert_eq!(
            runtime.effect_inventory(actor, input, None, 1).unwrap(),
            vec![(action.clone(), EffectState::Started)],
            "the exact handed-off reply still awaits its original physical completion"
        );
        assert_eq!(
            received.lock().unwrap().len(),
            index,
            "actual fact handoff does not await or pretend LP consumption"
        );
        assert!(
            matches!(
                requests.close_after_exit(),
                Err(PortControlError::UnsettledRoot)
            ),
            "actual handoff alone cannot acknowledge the exact root-command row"
        );
        runtime.effect_completed(ticket).unwrap();
        assert_eq!(
            runtime.effect_inventory(actor, input, None, 1).unwrap(),
            vec![(action, EffectState::Completed)],
            "only actual completion finishes the same original AP reply effect"
        );
        runtime.retire_input(actor, input).unwrap();
        let retirement_wakes = wakes.0.load(Ordering::SeqCst);
        ingress.root_control_processed(input).unwrap();
        ingress.input_retired(input).unwrap();
        assert!(
            wakes.0.load(Ordering::SeqCst) > retirement_wakes,
            "actual original ordered input retirement wakes its same Source"
        );
        if seal {
            let retained = scope.snapshot().unwrap().storage;
            assert!(matches!(
                requests.close_after_exit(),
                Err(PortControlError::UnsettledRoot)
            ));
            assert_eq!(ingress.close_after_exit(), Err(ObservationError::Busy));
            assert_eq!(scope.snapshot().unwrap().storage, retained);
            assert_eq!(used_bytes(), budget.total_bytes as u64);
        }
        let receipt = requests
            .acknowledge_fact_handoff(handoff)
            .unwrap_or_else(|_| panic!("same original root fact handoff"));
        assert!(
            !requests.reply_delivered(&projected).unwrap(),
            "exact row acknowledges once"
        );
        let ack = publisher
            .acknowledge_root_reply(&original, receipt)
            .unwrap();
        match ack {
            Some((
                AttemptInput::Executor(ExecutorFact::PublicationApplied {
                    effect,
                    result: Ok(novarocks_attempt_protocol::input::PublicationApplied::SealResult),
                }),
                None,
            )) => {
                assert!(seal);
                assert_eq!(effect, original.protocol.id);
            }
            None => assert!(!seal, "Terminal has no invented publication ACK"),
            _ => panic!("only a real Seal handoff can construct its original ACK"),
        }
        assert!(runtime.poll_one(Tick(1)).unwrap());
        let got = received.lock().unwrap()[index].clone();
        assert_eq!(got.activation, activation);
        assert_eq!(got.action, projected.action);
        match (got.body, &projected.body) {
            (novarocks_coordination_contract::logical_port::AttemptToLogicalBody::SealResult { token: a, result: Ok(a_result) },
             novarocks_coordination_contract::logical_port::AttemptToLogicalBody::SealResult { token: b, result: Ok(b_result) }) => {
                assert_eq!(a, *b); assert!(Arc::ptr_eq(&a_result, b_result));
            }
            (novarocks_coordination_contract::logical_port::AttemptToLogicalBody::TerminalControlResult { token: a, result: Ok(()) },
             novarocks_coordination_contract::logical_port::AttemptToLogicalBody::TerminalControlResult { token: b, result: Ok(()) }) => assert_eq!(a, *b),
            _ => panic!("same one original reply payload reached LP"),
        }
        assert!(facts.progress(3, false, false).unwrap().progressed);
        if seal {
            assert!(ordered_host.ready_for_close());
        }
    }
    assert_eq!(
        received.lock().unwrap().len(),
        2,
        "one actual reply for each original root command"
    );
    assert!(command_tickets.iter().all(Option::is_none));
    slot.close_publication();
    assert!(!storage_header.is_retired());
    assert!(reader.close_after_exit(&runtime).unwrap());
    assert!(
        storage_header.is_retired(),
        "same reader report/Work/header really retired"
    );
    facts.close_admission();
    facts.original_producer_exited();
    assert!(facts.progress(3, true, false).unwrap().complete);
    assert!(facts.is_closed());
    for trigger in &mut triggers {
        trigger.close_after_exit().unwrap();
    }
    let original_storage = scope.snapshot().unwrap().storage;
    assert_eq!(ingress.close_after_exit(), Err(ObservationError::Busy));
    assert_eq!(
        used_bytes(),
        0,
        "the frame and canonical request backing exited"
    );
    let before_metadata = scope.snapshot().unwrap().storage;
    assert!(before_metadata < original_storage);
    ingress.close_after_exit().unwrap();
    assert_eq!(scope.snapshot().unwrap().storage + 1, before_metadata);
    for (work, payer, owner) in [
        (&mut observations, &mut positions, &scope),
        (&mut control, &mut control_payer, &bridge),
    ] {
        let original_header = work.retirement_observer();
        assert!(!original_header.is_retired());
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        payer.finish().unwrap();
        owner.close_external().unwrap();
        ledger.reap_scope_ready(owner.id(), 16).unwrap();
        assert!(original_header.is_retired());
    }
    while let Some(turn) = runtime.claim_host_actor() {
        turn.release(false).unwrap();
    }
    runtime.remove_actor(actor).unwrap();
    runtime.remove_actor(source).unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    ledger.remove_scope(bridge.id()).unwrap();
    assert!(runtime.is_empty() && ledger.is_empty());
}
