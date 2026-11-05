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

//! Quiesce preserves original Task update inspection while ordinary Participation
//! domain admission refuses execution. Both retain actual paid raw disposal.

use super::*;
use novarocks_execution_contract::{ParticipationState, QuiesceParticipation};

fn quiesce_original_context(
    scene: &Scenario<TaskResources>,
    participation: ParticipationRef,
    resources: &TaskResources,
) -> Result<crate::WorkerTaskControl, Failure> {
    let control = scene.task_control(participation);
    let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
    let quiesce_operation = operation_number(58);
    let quiesce = resources._async_runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            control.quiesce(
                QuiesceParticipation::new(quiesce_operation, participation),
                deadline,
            ),
        )
        .await
    });
    let quiesce = match quiesce {
        Ok(Ok(delivery)) => delivery,
        original => {
            return Err(Box::new(("the original Quiesce did not answer", original)));
        }
    };
    if quiesce.operation() != quiesce_operation
        || !matches!(
            quiesce.body(),
            ParticipationReplyBody::Quiesce {
                outcome: OperationOutcome::Accepted,
                receipt,
            } if receipt.participation() == participation
                && receipt.state() == ParticipationState::Quiescing
        )
    {
        return Err(Box::new(("the original Quiesce was not Accepted", quiesce)));
    }
    drop(quiesce);
    if !wait_until(|| {
        scene
            .node
            .ordinary_participation_admission_closed_for_test(participation)
            && scene.ledger.used(Dimension::Requests.id()) == 0
    }) {
        return Err(Box::new(
            "the real Quiesce did not close ordinary admission with R0",
        ));
    }
    eprintln!(
        "actual closed Participation update fixture: original Quiesce Accepted, Quiescing, ordinary_closed=true, requests=0"
    );
    Ok(control)
}

#[test]
fn real_node_quiesced_context_update_keeps_original_terminal_reply_and_physical_disposal() {
    let mismatches = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = mismatches.clone();
    let disposed = Arc::new(DisposalObservation::default());
    let observed = disposed.clone();
    let gate = Arc::new(PreparationGate::default());
    let held_drop = gate.clone();
    run_node_lifecycle_with_task(
        1,
        NodeClose::Abort,
        compiled_task_geometry(),
        start_tasks,
        move |scene, participation, resources| {
            let _release = ReleasePreparationGate(held_drop.clone());
            let task = finish_original_task(scene, participation, resources)?;
            let control = quiesce_original_context(scene, participation, resources)?;
            let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
            let operation = operation_number(59);
            let request =
                original_update(task, operation, observed.clone(), Some(held_drop.clone()));
            let (ingress_gate, ingress) = original_ingress();
            let reply = resources._async_runtime.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    control.update_task(participation, request, deadline, ingress),
                )
                .await
            });
            let delivery = match reply {
                Ok(Ok(delivery)) => delivery,
                original => {
                    return Err(Box::new((
                        "the original quiesced Task update did not reach Core inspection",
                        original,
                    )));
                }
            };
            record_original_terminal_reply(scene, &delivery, operation, deadline, &recorded);
            drop(delivery);
            if !wait_until(|| held_drop.entered()) {
                return Err(Box::new(
                    "the original quiesced Task update rich destructor did not enter",
                ));
            }
            if observed.drops.load(Ordering::SeqCst) != 0
                || observed.host_drops.load(Ordering::SeqCst) != 0
                || ingress_gate.available_permits() != 0
                || scene.ledger.used(Dimension::Requests.id()) != 1
            {
                recorded.lock().unwrap().push(
                    "the quiesced Task update refunded its original rich raw, ingress or R before actual Drop".into(),
                );
            }
            held_drop.release();
            if !wait_until(|| {
                observed.drops.load(Ordering::SeqCst) == 1
                    && observed.host_drops.load(Ordering::SeqCst) == 1
                    && ingress_gate.available_permits() == 1
                    && scene.ledger.used(Dimension::Requests.id()) == 0
            }) {
                return Err(Box::new(
                    "the original quiesced Task update did not physically dispose its raw and ingress",
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
fn real_node_closed_context_domain_refusal_physically_disposes_original_ingress() {
    let mismatches = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = mismatches.clone();
    let disposed = Arc::new(DisposalObservation::default());
    let observed = disposed.clone();
    run_node_lifecycle_with_task(
        1,
        NodeClose::Abort,
        compiled_task_geometry(),
        start_tasks,
        move |scene, participation, resources| {
            finish_original_task(scene, participation, resources)?;
            let control = quiesce_original_context(scene, participation, resources)?;
            let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
            let operation = operation_number(60);
            let request = novarocks_execution_contract::AdvanceParticipationDomain::new(
                operation,
                participation,
                novarocks_execution_contract::ParticipationDomainUpdate::SharedDynamicFilter {
                    version: DomainVersion::FIRST,
                    payload: Arc::new(OriginalDomainContent {
                        bytes: Some(vec![91; 64]),
                        observed: observed.clone(),
                        drop_gate: None,
                    }),
                },
            );
            let (ingress_gate, ingress) = original_ingress();
            let refusal = resources._async_runtime.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    control.advance_query_context_domain(request, deadline, ingress),
                )
                .await
            });
            let refusal = match refusal {
                Ok(Err(crate::WorkerRequestError::HandoffRefused(refusal))) => {
                    refusal.into_original_for_test()
                }
                original => {
                    return Err(Box::new((
                        "closed ordinary Participation-domain update changed its refusal policy",
                        original,
                    )));
                }
            };
            let Some(retains_original) = scene.node.closed_participation_domain_refusal_for_test(
                participation,
                operation,
                &refusal,
            ) else {
                return Err(Box::new((
                    "closed Participation-domain update lost its exact original refusal",
                    refusal,
                )));
            };
            if !wait_until(|| {
                observed.drops.load(Ordering::SeqCst) == 1
                    && observed.host_drops.load(Ordering::SeqCst) == 1
                    && ingress_gate.available_permits() == 1
                    && scene.ledger.used(Dimension::Requests.id()) == 0
            }) {
                recorded.lock().unwrap().push(format!(
                    "closed Participation-domain HandoffRefused retained original rich input/ingress without automatic paid disposal: retains_original={retains_original}",
                ));
            }
            scene
                .node
                .dispose_closed_participation_domain_refusal_for_test(
                    participation,
                    operation,
                    refusal,
                )
                .map_err(|original| Box::new(original) as Failure)?;
            if !wait_until(|| {
                observed.drops.load(Ordering::SeqCst) == 1
                    && observed.host_drops.load(Ordering::SeqCst) == 1
                    && ingress_gate.available_permits() == 1
                    && scene.ledger.used(Dimension::Requests.id()) == 0
            }) {
                return Err(Box::new(
                    "the exact refused Participation-domain original did not physically clean up",
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
