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
use crate::control_runtime::actors::tests::{Fixture, actor, initial_establishment, operation};
use novarocks_control_runtime_core::{identity::Tick, mailbox::InputClass};
use novarocks_worker_protocol::{
    context::{ContextInput, EstablishReplayRequest, EstablishReplayResult},
    creation::RequestContext,
    lease::MonotonicInstant,
};
use std::time::Duration;

#[test]
fn context_control_handoffs_keep_core_order_across_batches_and_reverse_request_tokens() {
    let f = Fixture::new();
    let initial = initial_establishment();
    // These are correlation values from the requesting actor, not Context
    // inputs or Context Work authority. Their numeric order is irrelevant.
    let requests = [900, 100].map(|token| EstablishReplayRequest {
        request: RequestContext {
            token: InputId::new(token).unwrap(),
            operation: operation(2),
            deadline: MonotonicInstant::from_origin(Duration::from_secs(10)),
        },
        ticket: initial.ticket,
        payload: initial.payload,
        semantics: initial.semantics,
    });
    let inputs = requests.map(|request| {
        f.send(
            ContextInput::ProbeEstablish {
                context: f.identity,
                request,
                comparison: None,
            },
            None,
            InputClass::Lifecycle,
            2,
        )
    });
    f.runtime.poll_one(Tick(2)).unwrap();
    let mut dispatch =
        EffectDispatch::context(f.runtime.clone(), actor(), ContextCore::MAX_STEP_WORK * 2);
    let indexed = dispatch.index_committed(&f.runtime, 32).unwrap();
    let first = dispatch.take_ready(&f.runtime, 8).unwrap().unwrap();
    let first_ref = EffectRef {
        input: first.input,
        action: first.descriptor.id,
    };
    let first_request = match &first.effect {
        WorkerAction::Context(ContextEffect::EstablishProbed {
            request,
            result: EstablishReplayResult::Decided(_),
            ..
        }) => *request,
        _ => panic!("actual first Context owner verdict"),
    };
    let premature = dispatch.completion_observed(first_ref);
    let overtaken = dispatch.take_ready(&f.runtime, 8).unwrap();
    let blocked = overtaken.is_none();
    f.runtime.effect_completed(first).unwrap();
    let first_completed = dispatch.completion_observed(first_ref);
    let second = overtaken
        .or_else(|| dispatch.take_ready(&f.runtime, 8).unwrap())
        .expect("second original Context handoff");
    let stale = dispatch.completion_observed(first_ref);
    let second_ref = EffectRef {
        input: second.input,
        action: second.descriptor.id,
    };
    let second_request = match &second.effect {
        WorkerAction::Context(ContextEffect::EstablishProbed { request, .. }) => *request,
        _ => panic!("actual second Context owner verdict"),
    };
    f.runtime.effect_completed(second).unwrap();
    let second_completed = dispatch.completion_observed(second_ref);
    let empty = dispatch.is_empty();
    for input in inputs {
        f.runtime.retire_input(actor(), input).unwrap();
    }
    f.finish();

    // Verify after the actual actor/input/Work cleanup, including on an
    // ordering regression. A drop assertion must not hide the first failure.
    assert_eq!(indexed.inputs, inputs);
    assert_eq!(first_request, requests[0].request);
    assert_eq!(second_request, requests[1].request);
    assert!(
        blocked,
        "later Context metadata cannot overtake its handoff"
    );
    assert!(premature.is_err());
    assert!(first_completed.is_ok());
    assert!(stale.is_err());
    assert!(second_completed.is_ok());
    assert!(empty);
}

#[test]
fn a_later_request_waits_for_actual_task_publication_but_not_its_capture_exit() {
    use crate::control_runtime::actors::tests::task;
    use novarocks_control_runtime::publication::PublishedSnapshot;
    use novarocks_control_runtime_core::effect::EffectState;
    use novarocks_execution_contract::TaskStatus;
    use novarocks_worker_protocol::{
        context::ContextCommand,
        creation::{CreationCost, PayloadId},
    };
    use std::sync::Arc;

    // The fixture runs the actual actor, ledger, inputs and effect tickets.
    // Installed is a pure protocol model fact, not a Native physical-exit proof.
    let mut f = Fixture::new();
    let task = task(f.identity, 1);
    let create = f.send(
        ContextInput::Request(ContextCommand::Create {
            task,
            payload: PayloadId(71),
        }),
        Some(operation(71)),
        InputClass::Ordinary,
        2,
    );
    f.runtime.poll_one(Tick(1)).unwrap();
    f.drain(create);
    let loaded = f.send(
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
        InputClass::Lifecycle,
        2,
    );
    f.runtime.poll_one(Tick(1)).unwrap();
    f.drain(loaded);

    let publishing = f.send(
        ContextInput::Installed { task },
        None,
        InputClass::Lifecycle,
        2,
    );
    f.runtime.poll_one(Tick(2)).unwrap();
    let request = f.send(
        ContextInput::Request(ContextCommand::GetDynamicFilters {
            task,
            acknowledged: None,
        }),
        Some(operation(72)),
        InputClass::Ordinary,
        2,
    );
    f.runtime.poll_one(Tick(2)).unwrap();
    let request_effects = f
        .runtime
        .effect_inventory(actor(), request, None, ContextCore::MAX_STEP_WORK)
        .unwrap()
        .len();
    let mut dispatch =
        EffectDispatch::context(f.runtime.clone(), actor(), ContextCore::MAX_STEP_WORK * 4);
    dispatch.index_committed(&f.runtime, 64).unwrap();
    let publication = dispatch.take_ready(&f.runtime, 16).unwrap().unwrap();
    let snapshot = match &publication.effect {
        WorkerAction::Context(ContextEffect::PublishTask {
            task: actual,
            snapshot,
            ..
        }) if *actual == task => snapshot.clone(),
        _ => panic!("the earlier installed Task has an original status publication"),
    };
    let mut held_replies = Vec::new();
    let mut premature = false;
    let mut auxiliary_exits_proven = true;
    // Complete any unrelated actual effects while retaining the original
    // publication and reply tickets. No synthetic body effect is introduced.
    let complete_auxiliary = |ticket: EffectTicket<WorkerAction>| {
        let input = ticket.input;
        let predecessor = ticket.descriptor.id;
        f.runtime.effect_completed(ticket).unwrap();
        let inventory = f
            .runtime
            .effect_inventory(actor(), input, None, ContextCore::MAX_STEP_WORK)
            .unwrap();
        let exited = inventory.iter().any(|(descriptor, state)| {
            descriptor.id == predecessor && *state == EffectState::Completed
        });
        if exited {
            for (descriptor, state) in inventory {
                if state == EffectState::Pending && descriptor.dependencies.contains(&predecessor) {
                    f.runtime
                        .acknowledge_dependency(actor(), input, descriptor.id, predecessor)
                        .unwrap();
                }
            }
        }
        exited
    };
    while let Some(ticket) = dispatch.take_ready(&f.runtime, 16).unwrap() {
        if matches!(&ticket.effect, WorkerAction::Reply(reply) if reply.request.token == request) {
            premature = true;
            held_replies.push(ticket);
        } else {
            auxiliary_exits_proven &= complete_auxiliary(ticket);
        }
    }
    let target = PublishedSnapshot::new(&f.scope, Arc::new(TaskStatus::created(task)));
    drop(
        f.runtime
            .commit_snapshot(&publication, &target, Arc::new(snapshot))
            .unwrap(),
    );
    dispatch
        .publication_committed(&f.runtime, &publication)
        .unwrap();
    while let Some(ticket) = dispatch.take_ready(&f.runtime, 16).unwrap() {
        if matches!(&ticket.effect, WorkerAction::Reply(reply) if reply.request.token == request) {
            held_replies.push(ticket);
        } else {
            auxiliary_exits_proven &= complete_auxiliary(ticket);
        }
    }
    let complete_body = held_replies.iter().all(|ticket| {
        matches!(
            &ticket.effect,
            WorkerAction::Reply(reply)
                if matches!(reply.body.as_ref(),
                    novarocks_worker_protocol::context::ContextReplyBody::Verdict(
                        novarocks_worker_protocol::context::RequestResult::Outcome(
                            novarocks_execution_contract::OperationOutcome::Idempotent)))
        )
    });
    let reply_requests: Vec<_> = held_replies
        .iter()
        .filter_map(|ticket| match &ticket.effect {
            WorkerAction::Reply(reply) => Some(reply.request),
            _ => None,
        })
        .collect();
    let publication_still_held = f
        .runtime
        .effect_inventory(actor(), publishing, None, ContextCore::MAX_STEP_WORK)
        .unwrap()
        .iter()
        .any(|(descriptor, state)| {
            descriptor.id == publication.descriptor.id && *state == EffectState::Published
        });
    // Reply ordering releases at real start. The original ticket and body are
    // still held here; a slow external sender must not keep the Task lane.
    let ordering_drained = dispatch.is_empty();
    f.runtime.effect_completed(publication).unwrap();
    for reply in held_replies {
        f.runtime.effect_completed(reply).unwrap();
    }
    // Preserve the ordering observations above before a strict final drain.
    // A regression may leave an original pending effect; finish its actual
    // dependency/capture sequence before reporting the failed oracle instead
    // of making driver Drop hide it with a second panic.
    let mut cleanup_outputs = f.drain(publishing);
    cleanup_outputs.extend(f.drain(request));
    drop(target);
    f.finish();

    assert!(
        !premature,
        "a later reply cannot precede actual Task publication"
    );
    assert!(
        auxiliary_exits_proven,
        "only actual Completed auxiliary facts release dependencies"
    );
    assert!(
        cleanup_outputs.is_empty(),
        "the ordering exercise must itself finish every original effect"
    );
    assert_eq!(
        request_effects, 1,
        "a complete reply needs no separate body effect"
    );
    assert!(
        complete_body,
        "the unique reply retains the Core-authored complete body"
    );
    assert_eq!(reply_requests.len(), 1);
    assert_eq!(reply_requests[0].token, request);
    assert_eq!(reply_requests[0].operation, operation(72));
    assert!(
        publication_still_held,
        "publication ordering is independent of capture exit"
    );
    assert!(
        ordering_drained,
        "the started reply cannot retain the Task ordering lane"
    );
}

#[test]
fn original_read_preparation_gates_task_order_until_actual_completion_only() {
    use crate::control_runtime::actors::tests::task;
    use novarocks_control_runtime::publication::PublishedSnapshot;
    use novarocks_execution_contract::{
        CancelReason, DomainVersion, OperationOutcome, TaskConclusion, TaskStatus,
    };
    use novarocks_worker_protocol::{
        context::{ContextCommand, ContextReplyBody},
        creation::{CreationCost, PayloadId},
    };
    use std::sync::Arc;

    // These are actual actor inputs and original effect tickets. The fixture's
    // Installed/RF facts are a pure protocol model: completing Prepare below
    // models only a synchronous mechanical pin, not an actual RF loan issuer,
    // getter, codec, transport, Native exit or S8 recovery proof.
    let mut f = Fixture::new();
    let task = task(f.identity, 1);
    for (command, request, class) in [
        (
            ContextInput::Request(ContextCommand::Create {
                task,
                payload: PayloadId(81),
            }),
            Some(operation(81)),
            InputClass::Ordinary,
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
            InputClass::Lifecycle,
        ),
        (
            ContextInput::Installed { task },
            None,
            InputClass::Lifecycle,
        ),
        (
            ContextInput::DynamicFilterPublished {
                task,
                version: DomainVersion::FIRST,
                payload: PayloadId(82),
                domain_count: 1,
            },
            None,
            InputClass::Lifecycle,
        ),
    ] {
        let input = f.send(command, request, class, 2);
        f.runtime.poll_one(Tick(2)).unwrap();
        f.drain(input);
    }
    let read = f.send(
        ContextInput::Request(ContextCommand::GetDynamicFilters {
            task,
            acknowledged: None,
        }),
        Some(operation(82)),
        InputClass::Ordinary,
        2,
    );
    f.runtime.poll_one(Tick(2)).unwrap();
    let newer = f.send(
        ContextInput::DynamicFilterPublished {
            task,
            version: DomainVersion::new(2).unwrap(),
            payload: PayloadId(83),
            domain_count: 1,
        },
        None,
        InputClass::Lifecycle,
        2,
    );
    f.runtime.poll_one(Tick(2)).unwrap();
    let cancel = f.send(
        ContextInput::Request(ContextCommand::Cancel {
            task,
            reason: CancelReason::UpstreamNoLongerNeeded,
        }),
        Some(operation(83)),
        InputClass::Control,
        2,
    );
    f.runtime.poll_one(Tick(2)).unwrap();

    let read_effects = f
        .runtime
        .effect_inventory(actor(), read, None, ContextCore::MAX_STEP_WORK)
        .unwrap()
        .len();
    let mut dispatch =
        EffectDispatch::context(f.runtime.clone(), actor(), ContextCore::MAX_STEP_WORK * 4);
    dispatch.index_committed(&f.runtime, 64).unwrap();
    let prepare = dispatch.take_ready(&f.runtime, 16).unwrap().unwrap();
    let exact_prepare = matches!(&prepare.effect,
        WorkerAction::Context(ContextEffect::PrepareReadPayload {
            request, task: same_task, version, payload,
        }) if *request == read && *same_task == task
            && *version == DomainVersion::FIRST && *payload == PayloadId(82));
    let prepare_ref = EffectRef {
        input: prepare.input,
        action: prepare.descriptor.id,
    };
    let rejected_early = dispatch
        .task_preparation_completed(task, prepare_ref)
        .is_err();
    let rejected_wrong_task = dispatch
        .task_preparation_completed(
            crate::control_runtime::actors::tests::task(f.identity, 2),
            prepare_ref,
        )
        .is_err();
    let overtaken = dispatch.take_ready(&f.runtime, 32).unwrap();
    let blocked = overtaken.is_none();
    f.runtime.effect_completed(prepare).unwrap();
    let released = dispatch
        .task_preparation_completed(task, prepare_ref)
        .is_ok();
    let stale_rejected = dispatch
        .task_preparation_completed(task, prepare_ref)
        .is_err();

    let status = PublishedSnapshot::new(&f.scope, Arc::new(TaskStatus::created(task)));
    let conclusion = PublishedSnapshot::new(&f.scope, Arc::new(None::<TaskConclusion>));
    let mut held = Vec::new();
    let mut next = overtaken;
    let mut read_replies = 0;
    let mut exact_body = false;
    let mut newer_after_started_reply = false;
    let mut stopped_after_started_reply = false;
    let mut cancel_replied = false;
    while let Some(ticket) = next
        .take()
        .or_else(|| dispatch.take_ready(&f.runtime, 32).unwrap())
    {
        match &ticket.effect {
            WorkerAction::Reply(reply) if reply.request.token == read => {
                read_replies += 1;
                exact_body = matches!(reply.body.as_ref(), ContextReplyBody::ReadPayload {
                    version, payload, outcome: OperationOutcome::Accepted,
                } if *version == DomainVersion::FIRST && *payload == PayloadId(82));
            }
            WorkerAction::Reply(reply) if reply.request.token == cancel => {
                cancel_replied = read_replies == 1;
            }
            WorkerAction::Context(ContextEffect::PublishTask { snapshot, .. }) => {
                if ticket.input == newer {
                    newer_after_started_reply = read_replies == 1;
                }
                drop(
                    f.runtime
                        .commit_snapshot(&ticket, &status, Arc::new(snapshot.clone()))
                        .unwrap(),
                );
                dispatch.publication_committed(&f.runtime, &ticket).unwrap();
            }
            WorkerAction::Context(ContextEffect::PublishConclusion {
                conclusion: value, ..
            }) => {
                drop(
                    f.runtime
                        .commit_snapshot(&ticket, &conclusion, Arc::new(Some(value.clone())))
                        .unwrap(),
                );
                dispatch.publication_committed(&f.runtime, &ticket).unwrap();
            }
            WorkerAction::Context(ContextEffect::StopTask { .. }) => {
                stopped_after_started_reply = read_replies == 1;
            }
            _ => {}
        }
        // In particular, retain the actual started read reply across the newer
        // publication, stop and cancel reply. Its wire/captures cannot hold the
        // Task ordering lane after the mechanical preparation really completed.
        held.push(ticket);
    }
    let ordering_empty = dispatch.is_empty();
    for ticket in held {
        f.runtime.effect_completed(ticket).unwrap();
    }
    let mut cleanup = f.drain(read);
    cleanup.extend(f.drain(newer));
    cleanup.extend(f.drain(cancel));
    drop(status);
    drop(conclusion);
    f.finish();

    // Preserve real cleanup even when an ordering oracle fails; a second
    // driver Drop panic must not obscure the original failed observation.
    assert_eq!(read_effects, 2, "one preparation and one complete reply");
    assert!(exact_prepare && rejected_early && rejected_wrong_task);
    assert!(
        blocked,
        "same Task publication, stop and replies wait for actual preparation"
    );
    assert!(released && stale_rejected);
    assert_eq!(read_replies, 1);
    assert!(
        exact_body,
        "later v2 state cannot replace the original v1 read selection"
    );
    assert!(newer_after_started_reply && stopped_after_started_reply && cancel_replied);
    assert!(
        ordering_empty,
        "slow delivery/captures do not retain the Task lane"
    );
    assert!(
        cleanup.is_empty(),
        "the exercise itself completed every original effect"
    );
}
