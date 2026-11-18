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

//! Actual authorization, original launch custody, then original Root cancel.
//! No Worker receipt, terminal or actual exit is inferred from a missing RPC.
use super::*;
use crate::fixtures::history::{CapturedHistory, WorkExpectation};
use crate::task_execution::host::transport::OriginalEstablishRelease;
use novarocks_control_runtime::exit_work::HandoffState;
use novarocks_control_runtime_core::history::HistoryFact;

pub(super) async fn cancel_original_authorized_handoff(
    original: &mut OriginalRecoveryRun,
    probe: &Arc<OriginalInstallProbe>,
    metadata: &Arc<MetadataIoAdmission>,
    first: &Arc<crate::query_execution::native_execution_adapter::original_install_fixture::OriginalInstallRow>,
) {
    let observed = drive(
        "SAME actual authorized Establish before launch",
        Some(probe),
        None,
        &mut original.bootstrap,
        metadata,
        false,
        async {
            loop {
                if let Some(gate) = first.original_establish_handoff()
                    && let Some(held) = gate.held()
                {
                    break (gate, held);
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    // The whole Run owns this alias before any frontier assertion/await.
    original.held_establish = Some(observed.0);
    let held = observed.1;
    assert_eq!(held.participation, first.participations[0]);
    assert_eq!(held.result_actor, first.actual_actor().unwrap());
    let gate = original.held_establish.as_ref().unwrap();
    assert_eq!(gate.additional_matches(), 0);
    assert_eq!(gate.held_handoff_state(), Some(HandoffState::Pending));
    assert!(gate.accepted().is_none());
    assert!(!first.actual_exit_observed());
    let history = original.bootstrap.fixture.history();
    original.captured_history = Some(CapturedHistory::drain(&history, usize::MAX));
    original
        .captured_history
        .as_ref()
        .unwrap()
        .check_work(held.result_work, WorkExpectation::Unknown)
        .unwrap();
    // Reaching this SAME bound SendBatch requires the real AP to have
    // consumed LP's exact authorization. LP freezes TransportOwned in that
    // original authorization turn; this fixture does not issue a reference.
    original
        .root
        .as_ref()
        .unwrap()
        .owner
        .cancellation_requester()
        .request(novarocks_workload_control::CancellationReason::Requested)
        .unwrap();
    let captured = original.captured_history.as_mut().unwrap();
    drive(
        "SAME AP stops and withdraws its original unlaunched Establish",
        Some(probe),
        None,
        &mut original.bootstrap,
        metadata,
        false,
        async {
            loop {
                captured.append_from(&history, usize::MAX);
                let stopped = captured.entries().iter().any(|entry| {
                    matches!(entry.fact, HistoryFact::ScopeStopped { scope }
                        if scope == held.result_work.scope)
                });
                if stopped && gate.held_handoff_state() == Some(HandoffState::Withdrawn) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    assert_eq!(gate.held(), Some(held));
    assert_eq!(gate.additional_matches(), 0);
    assert!(gate.accepted().is_none());
    assert!(!first.actual_exit_observed());
    assert!(
        !original
            .bootstrap
            .fixture
            .runtime()
            .snapshot(held.result_actor)
            .unwrap()
            .failed
    );
    assert!(
        futures::poll!(std::future::poll_fn(|cx| original
            .start
            .exit
            .as_mut()
            .unwrap()
            .poll_exit(cx)))
        .is_pending()
    );
    original
        .captured_history
        .as_ref()
        .unwrap()
        .check_work(held.result_work, WorkExpectation::Unknown)
        .unwrap();
    // The original handoff/withdrawal winner determines NotStarted. Returning
    // this exact job never creates a second operation, permission or RPC.
    assert_eq!(gate.release(), OriginalEstablishRelease::Accepted(held));
    assert_eq!(gate.accepted(), Some(held));
    assert_eq!(gate.additional_matches(), 0);
    assert!(gate.held().is_none());
    drop(original.held_establish.take());
    let error = drive(
        "SAME cancelled business outcome without Ready",
        Some(probe),
        None,
        &mut original.bootstrap,
        metadata,
        true,
        original.start.ready.take().unwrap(),
    )
    .await
    .err()
    .expect("an original cancelled computation cannot publish Ready");
    assert_eq!(
        error.kind(),
        novarocks_coordination_contract::QueryExecutionErrorKind::Cancelled
    );
    drop(original.output.take());
    let mut exit = original.start.exit.take().unwrap();
    let receipt = drive(
        "SAME actual Native/AP/LP exit after original job settlement",
        Some(probe),
        None,
        &mut original.bootstrap,
        metadata,
        true,
        std::future::poll_fn(|cx| exit.poll_exit(cx)),
    )
    .await
    .unwrap();
    assert!(first.actual_exit_observed());
    {
        let view = receipt.view();
        assert!(matches!(
            view.runtime_actor_result(),
            Some(novarocks_query_application::api::logical_runtime::LogicalExit::Completed)
        ));
        assert!(!view.role_failed());
        assert!(!view.internal_consumer_failed());
        assert!(view.native_preparation_failure().is_none());
        assert!(view.native_run_panic().is_none());
        assert!(view.native_run_cleanup_panic().is_none());
        assert!(view.native_body_cleanup_panic().is_none());
        assert!(!view.native_dormant_failed());
    }
    let actual = original.peers[0].applied_requests();
    let decoded = actual
        .iter()
        .flat_map(|request| &request.operations)
        .map(|wire| {
            novarocks_task_codec::operation::decode_operation(
                wire,
                novarocks_proto_codec::FieldPath::root("original_authorized_cancel"),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(decoded.iter().any(|operation| matches!(operation,
        novarocks_task_codec::operation::DecodedOperation::AcquireParticipationAdmissionTicket(ticket)
            if ticket.participation() == held.participation)));
    assert!(!decoded.iter().any(|operation| matches!(
        operation,
        novarocks_task_codec::operation::DecodedOperation::UpdateParticipation(
            novarocks_task_codec::operation::DecodedUpdateParticipation::Establish(_)
        ) | novarocks_task_codec::operation::DecodedOperation::CreateTask(_)
    )));
    // No-RPC above is tied to the original positive Withdrawn winner and
    // actual complete settlement/Join; it is not itself a remote-close proof.
    let captured = original.captured_history.as_mut().unwrap();
    captured.append_from(&history, usize::MAX);
    assert!(!captured.entries().iter().any(|entry| matches!(
        entry.fact,
        HistoryFact::FailurePublication { .. } | HistoryFact::OwnerFailed { .. }
    )));
    assert!(probe.row(1).is_none());
    drop((receipt, exit));
}

#[tokio::test]
async fn original_authorized_establish_remains_in_its_actual_handoff_custody_across_cancel() {
    super::run_original(1, true).await;
}
