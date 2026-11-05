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

//! Finalize's actual execution exit can close the Task source before the
//! original normal RetireTask ticket reaches its Worker owner.

use super::*;

struct ReleaseOriginalRetireHandoff {
    node: Arc<WorkerNodeHost>,
    participation: ParticipationRef,
    task: TaskIdentity,
}
impl Drop for ReleaseOriginalRetireHandoff {
    fn drop(&mut self) {
        self.node
            .release_normal_retire_handoff_for_test(self.participation, self.task);
    }
}

#[test]
fn real_node_late_update_after_finalize_before_retire_handoff_keeps_original_terminal_contract() {
    let mismatches = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = mismatches.clone();
    let observed = Arc::new(DisposalObservation::default());
    let disposed = observed.clone();
    let submitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let actual_submitted = submitted.clone();
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        |root| start_tasks_with_bundle(root, Some(false)),
        move |scene, participation, resources| {
            let task = TaskIdentity::new(
                participation.query_execution_id(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                participation.backend_process_id(),
            );
            if !scene
                .node
                .hold_normal_retire_handoff_for_test(participation, task)
            {
                return Err(Box::new(
                    "the exact original normal Retire handoff gate could not bind",
                ));
            }
            // All fixture and request errors release the original ticket before
            // the outer Node lifecycle can attempt its actual close.
            let _release = ReleaseOriginalRetireHandoff {
                node: scene.node.clone(),
                participation,
                task,
            };
            let hook: PreparingReadHeldHook = Box::new(
                move |scene, participation, resources, task, old_request| {
                    let mut actual = None;
                    let window = wait_until(|| {
                        actual = scene
                            .node
                            .finalized_task_closing_for_test(participation, task);
                        actual.is_some_and(|facts| {
                            facts.payload == PayloadId(9)
                                && facts.original_payload == facts.payload
                                && facts.production_retired
                                && facts.finalize_projection_exited
                                && !facts.request_open
                                && facts.source_closed_and_open_drained
                                && !facts.accepted_retirement
                                && !facts.read_sealed
                        })
                    });
                    let attached = scene
                        .node
                        .original_task_request_for_test(participation, old_request.author);
                    if !window
                        || scene.ledger.used(Dimension::Requests.id()) != 1
                        || attached.is_none_or(|facts| {
                            facts.0 != old_request || facts.1 != task || !facts.2
                        })
                    {
                        recorded.lock().unwrap().push(format!(
                            "fixture: the actual Finalize-only closed-source window was not established: facts={actual:?}, attached={attached:?}"
                        ));
                        // A missing window is not the intended contract RED.
                        // Still release the original ticket and let the old
                        // Task read/Root/Node complete their ordinary cleanup.
                        scene
                            .node
                            .release_normal_retire_handoff_for_test(participation, task);
                        return Ok(());
                    }
                    let actual = actual.expect("the actual window was observed");
                    eprintln!(
                        "actual Finalize-only update fixture: task={task:?}, payload={:?}, close_epoch={:?}, production_retired=true, finalize_projection_exited=true, source_closed=true, live_host=true, request_open=false, accepted_retirement=false, read_sealed=false, old_request={old_request:?}",
                        actual.payload, actual.close_epoch,
                    );
                    let operation = operation_number(62);
                    let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
                    let request = original_update(task, operation, observed.clone(), None);
                    let (ingress_gate, ingress) = original_ingress();
                    submitted.store(true, Ordering::SeqCst);
                    let control = scene.task_control(participation);
                    let mut caller = resources._async_runtime.spawn(async move {
                        control
                            .update_task(participation, request, deadline, ingress)
                            .await
                    });
                    // The old Task read owns R1. Either the actual second R
                    // was admitted or the real pre-R refusal has returned.
                    // Never hold the Retire ticket while awaiting Core reply:
                    // its later completion effect uses the same dispatcher.
                    let admission_returned = wait_until(|| {
                        scene.ledger.used(Dimension::Requests.id()) == 2 || caller.is_finished()
                    });
                    scene
                        .node
                        .release_normal_retire_handoff_for_test(participation, task);
                    if !admission_returned {
                        recorded.lock().unwrap().push(
                            "fixture: the actual Update neither admitted its R nor returned its pre-R refusal within the original bound".into(),
                        );
                    }
                    let answer = resources._async_runtime.block_on(async {
                        tokio::time::timeout(Duration::from_secs(5), &mut caller).await
                    });
                    match answer {
                        Ok(Ok(Ok(delivery))) => {
                            let mut failures = recorded.lock().unwrap();
                            if delivery.operation() != operation
                                || delivery.request_deadline_for_test() != deadline
                                || !matches!(
                                    delivery.body(),
                                    ParticipationReplyBody::Verdict(
                                        novarocks_worker_protocol::participation::RequestResult::Outcome(
                                            OperationOutcome::TerminalRejected
                                        )
                                    )
                                )
                            {
                                failures.push("the actual Finalize-window Core reply changed its operation/deadline/terminal verdict".into());
                            }
                            if scene.ledger.used(Dimension::Requests.id()) != 2 {
                                failures.push("the actual Finalize-window reply and old Task read did not retain their two original Rs".into());
                            }
                            drop(failures);
                            drop(delivery);
                        }
                        Ok(Ok(Err(crate::WorkerRequestError::HandoffRefused(refusal)))) => {
                            recorded.lock().unwrap().push(format!(
                                "validated Finalize-only retirement refused the original late Update at {}",
                                refusal.stage(),
                            ));
                            let original = refusal.into_original_for_test();
                            if !scene
                                .node
                                .task_domain_refusal_is_closed_for_test(task, &original)
                            {
                                return Err(Box::new((
                                    "the original Finalize-window refusal was not exact Task-source RequestAdmissionClosed",
                                    original,
                                )));
                            }
                            scene
                                .node
                                .dispose_task_domain_refusal_for_test(participation, original)
                                .map_err(|original| Box::new(original) as Failure)?;
                        }
                        Ok(original) => {
                            return Err(Box::new((
                                "the original Finalize-window caller did not deliver its owned reply/refusal",
                                original,
                            )));
                        }
                        Err(_) => {
                            caller.abort();
                            let joined = resources._async_runtime.block_on(async {
                                tokio::time::timeout(Duration::from_secs(5), &mut caller).await
                            });
                            return Err(Box::new((
                                "the original Finalize-window caller timed out after releasing the actual Retire ticket",
                                joined,
                                caller,
                            )));
                        }
                    }
                    if !wait_until(|| {
                        observed.drops.load(Ordering::SeqCst) == 1
                            && observed.host_drops.load(Ordering::SeqCst) == 1
                            && ingress_gate.available_permits() == 1
                            && scene.ledger.used(Dimension::Requests.id()) == 1
                    }) {
                        return Err(Box::new(
                            "the actual Finalize-window rich Drop/ingress/R did not settle to the still-held original Task-read R",
                        ));
                    }
                    Ok(())
                },
            );
            exercise_root_with_preparing_read_hook(
                scene,
                participation,
                resources,
                true,
                Some(hook),
            )?;
            if !wait_until(|| {
                scene.ledger.used(Dimension::Requests.id()) == 0
                    && scene.ledger.used(Dimension::ActiveTasks.id()) == 0
                    && scene
                        .node
                        .retired_task_contract_for_test(participation, task)
                        == Some((false, true))
            }) {
                return Err(Box::new(
                    "the original Task-read/Root ACK/normal Task did not actually retire after the Finalize-window request",
                ));
            }
            Ok(None)
        },
        shutdown,
    );
    if actual_submitted.load(Ordering::SeqCst) {
        assert_eq!(disposed.drops.load(Ordering::SeqCst), 1);
        assert_eq!(disposed.host_drops.load(Ordering::SeqCst), 1);
    }
    let mismatches = mismatches.lock().unwrap();
    assert!(mismatches.is_empty(), "{mismatches:?}");
}
