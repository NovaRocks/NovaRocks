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

//! The original Core terminal decision must remain reachable after real Task
//! retirement. A read seal is observed here; it grants no domain authority.

mod closed_admission;
mod finalize_admission;

use super::*;
use novarocks_execution_contract::{
    CodecOwnedContent, ContentFingerprint, DomainVersion, OperationOutcome, UpdateTask,
};

#[derive(Debug, Default)]
struct DisposalObservation {
    drops: AtomicUsize,
    host_drops: AtomicUsize,
}
#[derive(Debug)]
struct OriginalDomainContent {
    bytes: Option<Vec<u8>>,
    observed: Arc<DisposalObservation>,
    drop_gate: Option<Arc<PreparationGate>>,
}
impl CodecOwnedContent for OriginalDomainContent {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([91; 16])
    }
    fn encoded_len(&self) -> usize {
        self.bytes.as_ref().map_or(0, Vec::len)
    }
}
impl Drop for OriginalDomainContent {
    fn drop(&mut self) {
        if let Some(gate) = &self.drop_gate {
            gate.wait();
        }
        drop(self.bytes.take());
        self.observed.host_drops.fetch_add(
            usize::from(
                std::thread::current()
                    .name()
                    .is_some_and(|name| name.starts_with("worker-host-")),
            ),
            Ordering::SeqCst,
        );
        self.observed.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn finish_original_task(
    scene: &Scenario<TaskResources>,
    participation: ParticipationRef,
    resources: &mut TaskResources,
) -> Result<TaskIdentity, Failure> {
    exercise_root(scene, participation, resources, true)?;
    let task = TaskIdentity::new(
        participation.query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        participation.backend_process_id(),
    );
    // No manual owner removal, fabricated receipt or terminal input:
    // the compiled Task completed and its original disposal retired it.
    if !wait_until(|| {
        scene
            .node
            .retired_task_contract_for_test(participation, task)
            == Some((false, true))
            && scene.ledger.used(Dimension::ActiveTasks.id()) == 0
            && scene.ledger.used(Dimension::Requests.id()) == 0
            && resources
                .host
                .observed
                .protocol_events
                .lock()
                .unwrap()
                .iter()
                .any(|event| {
                    matches!(event,
            crate::TaskProtocolEvent::TaskTerminalRetained { identity, state }
            if *identity == task && *state == TaskState::Finished)
                })
    }) {
        return Err(Box::new(
            "the real Task did not reach its exact normal retirement seal",
        ));
    }
    Ok(task)
}

#[test]
fn real_node_late_update_after_normal_task_retirement_returns_original_terminal_rejection() {
    let mismatches = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = mismatches.clone();
    let disposed = Arc::new(DisposalObservation::default());
    let observed = disposed.clone();
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        start_tasks,
        move |scene, participation, resources| {
            let task = finish_original_task(scene, participation, resources)?;
            let operation = operation_number(49);
            let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
            let request = UpdateTask::try_new(
                operation,
                task,
                vec![TaskDomainUpdate::TaskDynamicFilter {
                    version: DomainVersion::FIRST,
                    payload: Arc::new(OriginalDomainContent {
                        bytes: Some(vec![91; 64]),
                        observed: observed.clone(),
                        drop_gate: None,
                    }),
                }],
            )
            .unwrap();
            let ingress_gate = Arc::new(tokio::sync::Semaphore::new(1));
            let ingress = crate::WorkerIngressResponsibility::from_admitted(Arc::new(
                ComponentIngressBacking {
                    _permit: ingress_gate.clone().try_acquire_owned().unwrap(),
                },
            ));
            let control = scene.task_control(participation);
            let result = resources._async_runtime.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    control.update_task(participation, request, deadline, ingress),
                )
                .await
            });
            match result {
                Ok(Ok(delivery)) => {
                    let mut failures = recorded.lock().unwrap();
                    if delivery.operation() != operation
                        || delivery.request_deadline_for_test() != deadline
                    {
                        failures.push("the original operation or absolute deadline changed".into());
                    }
                    if !matches!(
                        delivery.body(),
                        ParticipationReplyBody::Verdict(
                            novarocks_worker_protocol::participation::RequestResult::Outcome(
                                OperationOutcome::TerminalRejected
                            )
                        )
                    ) {
                        failures.push(format!("late UpdateTask returned {:?}", delivery.body()));
                    }
                    if scene.ledger.used(Dimension::Requests.id()) != 1 {
                        failures.push(
                            "the original terminal reply did not retain exactly one paid R".into(),
                        );
                    }
                    drop(failures);
                    drop(delivery);
                }
                Ok(Err(crate::WorkerRequestError::HandoffRefused(refusal))) => {
                    recorded.lock().unwrap().push(format!(
                        "late UpdateTask was refused before Core terminal adjudication at {}",
                        refusal.stage(),
                    ));
                    // Expected RED must still retire the actual rich original
                    // on the existing paid Root cleanup host, before asserting.
                    scene
                        .node
                        .dispose_task_domain_refusal_for_test(
                            participation,
                            refusal.into_original_for_test(),
                        )
                        .map_err(|original| Box::new(original) as Failure)?;
                }
                Ok(Err(error)) => {
                    recorded
                        .lock()
                        .unwrap()
                        .push(format!("late UpdateTask failed: {error}"));
                }
                Err(_) => return Err(Box::new("the original late UpdateTask did not answer")),
            }
            if !wait_until(|| {
                observed.drops.load(Ordering::SeqCst) == 1
                    && ingress_gate.available_permits() == 1
                    && scene.ledger.used(Dimension::Requests.id()) == 0
            }) {
                return Err(Box::new(
                    "the original late-update payload, ingress or R did not actually retire",
                ));
            }
            Ok(None)
        },
        shutdown,
    );
    // The complete Node lifecycle and its original executor joins run before
    // this contract assertion, including on the deliberately observed RED path.
    assert_eq!(disposed.drops.load(Ordering::SeqCst), 1);
    assert_eq!(disposed.host_drops.load(Ordering::SeqCst), 1);
    let mismatches = mismatches.lock().unwrap();
    assert!(mismatches.is_empty(), "{mismatches:?}");
}

fn original_update(
    task: TaskIdentity,
    operation: novarocks_execution_contract::TaskOperationId,
    observed: Arc<DisposalObservation>,
    drop_gate: Option<Arc<PreparationGate>>,
) -> UpdateTask {
    UpdateTask::try_new(
        operation,
        task,
        vec![TaskDomainUpdate::TaskDynamicFilter {
            version: DomainVersion::FIRST,
            payload: Arc::new(OriginalDomainContent {
                bytes: Some(vec![91; 64]),
                observed,
                drop_gate,
            }),
        }],
    )
    .unwrap()
}
fn original_ingress() -> (
    Arc<tokio::sync::Semaphore>,
    crate::WorkerIngressResponsibility,
) {
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    let original =
        crate::WorkerIngressResponsibility::from_admitted(Arc::new(ComponentIngressBacking {
            _permit: gate.clone().try_acquire_owned().unwrap(),
        }));
    (gate, original)
}
fn wait_original_update(
    resources: &TaskResources,
    mut caller: tokio::task::JoinHandle<
        Result<crate::WorkerTaskDelivery, crate::WorkerRequestError>,
    >,
) -> Result<crate::WorkerTaskDelivery, Failure> {
    let joined = resources
        ._async_runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(5), &mut caller).await });
    match joined {
        Ok(Ok(Ok(delivery))) => Ok(delivery),
        Ok(original) => Err(Box::new((
            "the original late-update caller did not return its delivery",
            original,
        ))),
        Err(_) => Err(Box::new((
            "the original late-update caller did not join",
            caller,
        ))),
    }
}
fn record_original_terminal_reply(
    scene: &Scenario<TaskResources>,
    delivery: &crate::WorkerTaskDelivery,
    operation: novarocks_execution_contract::TaskOperationId,
    deadline: MonotonicInstant,
    mismatches: &Mutex<Vec<String>>,
) {
    let mut failures = mismatches.lock().unwrap();
    if delivery.operation() != operation || delivery.request_deadline_for_test() != deadline {
        failures.push("the actual late-update reply changed its operation or deadline".into());
    }
    if !matches!(
        delivery.body(),
        ParticipationReplyBody::Verdict(
            novarocks_worker_protocol::participation::RequestResult::Outcome(
                OperationOutcome::TerminalRejected
            )
        )
    ) {
        failures.push(format!(
            "the original Core late-update answer was {:?}",
            delivery.body()
        ));
    }
    if scene.ledger.used(Dimension::Requests.id()) != 1 {
        failures.push("the held actual late-update delivery did not retain one paid R".into());
    }
}

#[test]
fn real_node_retired_update_keeps_original_r_and_ingress_until_blocked_rich_drop_exits() {
    let mismatches = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = mismatches.clone();
    let observed = Arc::new(DisposalObservation::default());
    let disposed = observed.clone();
    let drop_gate = Arc::new(PreparationGate::default());
    let held_drop = drop_gate.clone();
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        start_tasks,
        move |scene, participation, resources| {
            let _release = ReleasePreparationGate(held_drop.clone());
            let task = finish_original_task(scene, participation, resources)?;
            let operation = operation_number(50);
            let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
            let request =
                original_update(task, operation, observed.clone(), Some(held_drop.clone()));
            let (ingress_gate, ingress) = original_ingress();
            let control = scene.task_control(participation);
            let caller = resources._async_runtime.spawn(async move {
                control
                    .update_task(participation, request, deadline, ingress)
                    .await
            });
            let delivery = wait_original_update(resources, caller)?;
            record_original_terminal_reply(scene, &delivery, operation, deadline, &recorded);
            // Inspection disposal follows the original external reply's end.
            // Ending that delivery still cannot retire a blocked raw Drop.
            drop(delivery);
            if !wait_until(|| held_drop.entered()) {
                return Err(Box::new(
                    "the original rich destructor did not enter its real host barrier",
                ));
            }
            // Both the Core reply and its external delivery have ended. The
            // actual 64-byte Vec remains owned inside this blocked Drop.
            if observed.drops.load(Ordering::SeqCst) != 0
                || ingress_gate.available_permits() != 0
                || scene.ledger.used(Dimension::Requests.id()) != 1
            {
                recorded.lock().unwrap().push(
                    "blocked original rich Drop refunded its bytes, ingress or R early".into(),
                );
            }
            held_drop.release();
            if !wait_until(|| {
                observed.drops.load(Ordering::SeqCst) == 1
                    && ingress_gate.available_permits() == 1
                    && scene.ledger.used(Dimension::Requests.id()) == 0
            }) {
                return Err(Box::new(
                    "the unblocked original rich Drop, ingress and R did not really retire",
                ));
            }
            Ok(None)
        },
        shutdown,
    );
    assert_eq!(disposed.drops.load(Ordering::SeqCst), 1);
    assert_eq!(disposed.host_drops.load(Ordering::SeqCst), 1);
    let mismatches = mismatches.lock().unwrap();
    assert!(mismatches.is_empty(), "{mismatches:?}");
}

#[test]
fn real_node_retired_update_caller_drop_before_raw_disposal_keeps_original_custody() {
    let mismatches = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = mismatches.clone();
    let observed = Arc::new(DisposalObservation::default());
    let disposed = observed.clone();
    let gate = Arc::new(PreparationGate::default());
    let disposal_gate = gate.clone();
    let held_disposal = gate.clone();
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        move |root| start_tasks_with_lifecycle_gate(root, None, Some(disposal_gate)),
        move |scene, participation, resources| {
            let _release = ReleasePreparationGate(held_disposal.clone());
            let task = finish_original_task(scene, participation, resources)?;
            let operation = operation_number(51);
            let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
            let request = original_update(task, operation, observed.clone(), None);
            let (ingress_gate, ingress) = original_ingress();
            let control = scene.task_control(participation);
            let delivery_held = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let held_by_caller = delivery_held.clone();
            let mut caller = resources._async_runtime.spawn(async move {
                let delivery = control
                    .update_task(participation, request, deadline, ingress)
                    .await?;
                // Keep this original external delivery inside its actual
                // caller Future until that Future is really cancelled.
                held_by_caller.store(true, Ordering::Release);
                std::future::pending::<()>().await;
                Ok::<_, crate::WorkerRequestError>(delivery)
            });
            if !wait_until(|| delivery_held.load(Ordering::Acquire) || caller.is_finished())
                || !delivery_held.load(Ordering::Acquire)
            {
                return Err(Box::new((
                    "the original caller did not hold its actual reply delivery",
                    caller,
                )));
            }
            caller.abort();
            let joined = resources._async_runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), &mut caller).await
            });
            match joined {
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(original) => {
                    return Err(Box::new((
                        "the original caller Future did not actually drop",
                        original,
                    )));
                }
                Err(_) => {
                    return Err(Box::new((
                        "the original caller did not really join after cancellation",
                        caller,
                    )));
                }
            }
            if !wait_until(|| held_disposal.entered()) {
                return Err(Box::new(
                    "the original raw disposal did not enter after the caller really exited",
                ));
            }
            // The caller and its actual delivery have really exited. The
            // same started R authority, rich raw and ingress remain with the
            // original disposal job, before that job enters the destructor.
            if observed.drops.load(Ordering::SeqCst) != 0
                || ingress_gate.available_permits() != 0
                || scene.ledger.used(Dimension::Requests.id()) != 1
            {
                recorded
                    .lock()
                    .unwrap()
                    .push("caller Drop refunded the original queued raw disposal".into());
            }
            held_disposal.release();
            if !wait_until(|| {
                observed.drops.load(Ordering::SeqCst) == 1
                    && ingress_gate.available_permits() == 1
                    && scene.ledger.used(Dimension::Requests.id()) == 0
            }) {
                return Err(Box::new(
                    "the same original capture did not finish after its caller was gone",
                ));
            }
            Ok(None)
        },
        shutdown,
    );
    assert_eq!(disposed.drops.load(Ordering::SeqCst), 1);
    assert_eq!(disposed.host_drops.load(Ordering::SeqCst), 1);
    let mismatches = mismatches.lock().unwrap();
    assert!(mismatches.is_empty(), "{mismatches:?}");
}

#[test]
fn real_node_late_update_during_accepted_normal_retirement_keeps_original_terminal_contract() {
    let mismatches = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = mismatches.clone();
    let observed = Arc::new(DisposalObservation::default());
    let disposed = observed.clone();
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        |root| start_tasks_with_bundle(root, Some(false)),
        move |scene, participation, resources| {
            let held_observation = observed.clone();
            let hook: PreparingReadHeldHook = Box::new(
                move |scene, participation, resources, task, old_request| -> Result<(), Failure> {
                    // The original preparing GetDynamicFilters delivery is still
                    // alive with its actual non-final-info Task-causal route;
                    // no additional Source, Work, counter or Core fact is minted.
                    if scene.ledger.used(Dimension::Requests.id()) != 1 {
                        return Err(Box::new(
                            "the original preparing Task read did not retain its actual R",
                        ));
                    }
                    let mut held_facts = None;
                    if !wait_until(|| {
                        held_facts = scene
                            .node
                            .closing_task_retirement_for_test(participation, task);
                        held_facts.is_some_and(|(payload, _, accepted, committed, open, sealed)| {
                            payload == PayloadId(9) && accepted && committed && !open && !sealed
                        })
                    }) {
                        return Err(Box::new((
                            "the real original Task read did not hold an accepted live closed unsealed Task",
                            held_facts,
                        )));
                    }
                    let attached = scene
                        .node
                        .original_task_request_for_test(participation, old_request.author);
                    if attached
                        .is_none_or(|facts| facts.0 != old_request || facts.1 != task || !facts.2)
                    {
                        return Err(Box::new((
                            "the actual old Task read lost its original attached route while closing",
                            attached,
                        )));
                    }
                    let operation = operation_number(52);
                    let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
                    let request = original_update(task, operation, held_observation.clone(), None);
                    let (ingress_gate, ingress) = original_ingress();
                    let control = scene.task_control(participation);
                    let answer = resources._async_runtime.block_on(async {
                        tokio::time::timeout(
                            Duration::from_secs(5),
                            control.update_task(participation, request, deadline, ingress),
                        )
                        .await
                    });
                    match answer {
                        Ok(Ok(delivery)) => {
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
                                failures.push("the original closing-window Core reply changed its identity/deadline/verdict".into());
                            }
                            // Both actual independent deliveries are still held:
                            // one old Task-read R plus this newly paid direct Update R.
                            if scene.ledger.used(Dimension::Requests.id()) != 2 {
                                failures.push("the two held original deliveries did not retain their actual two Rs".into());
                            }
                            drop(failures);
                            drop(delivery);
                        }
                        Ok(Err(crate::WorkerRequestError::HandoffRefused(refusal))) => {
                            recorded.lock().unwrap().push(format!(
                                "accepted normal retirement refused the original late Update at {}",
                                refusal.stage()
                            ));
                            let original = refusal.into_original_for_test();
                            if !scene
                                .node
                                .task_domain_refusal_is_closed_for_test(task, &original)
                            {
                                recorded.lock().unwrap().push(
                                "the actual refused leaf was not original RequestAdmissionClosed"
                                    .into(),
                            );
                            }
                            scene
                                .node
                                .dispose_task_domain_refusal_for_test(participation, original)
                                .map_err(|original| Box::new(original) as Failure)?;
                        }
                        Ok(Err(original)) => {
                            return Err(Box::new((
                                "the closing-window original request owner failed",
                                original,
                            )));
                        }
                        Err(_) => {
                            return Err(Box::new(
                                "the original closing-window caller did not actually return",
                            ));
                        }
                    }
                    if !wait_until(|| {
                        held_observation.drops.load(Ordering::SeqCst) == 1
                            && ingress_gate.available_permits() == 1
                            && scene.ledger.used(Dimension::Requests.id()) == 1
                    }) {
                        return Err(Box::new(
                            "the actual late-update rich body and R did not return to the still-held original Task-read R",
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
            let task = TaskIdentity::new(
                participation.query_execution_id(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                participation.backend_process_id(),
            );
            if !wait_until(|| {
                scene.ledger.used(Dimension::Requests.id()) == 0
                    && scene.ledger.used(Dimension::ActiveTasks.id()) == 0
                    && scene
                        .node
                        .retired_task_contract_for_test(participation, task)
                        == Some((false, true))
            }) {
                return Err(Box::new(
                    "the released original Task read, Root ACK and normal Task did not actually retire",
                ));
            }
            Ok(None)
        },
        shutdown,
    );
    assert_eq!(disposed.drops.load(Ordering::SeqCst), 1);
    assert_eq!(disposed.host_drops.load(Ordering::SeqCst), 1);
    let mismatches = mismatches.lock().unwrap();
    assert!(mismatches.is_empty(), "{mismatches:?}");
}
