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

//! Public Core facts only: normal startup observation is not AP terminal,
//! Ready, visibility or physical exit. Actual future/Drop projection is QA's.
use super::super::support::*;
use super::contract::{RecoveryFixture, at, backend_failure, roster};
use novarocks_coordination_contract::AttemptIssueSettlement;
use novarocks_execution_contract::OperationOutcome;
use novarocks_logical_execution_protocol::start::LogicalConclusion;
use std::time::Duration;

#[test]
fn protocol_closed_startup_and_original_ap_failure_recover_in_both_arrival_orders() {
    for startup_first in [true, false] {
        let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
        let issued =
            r.establish_initial(AttemptIssueSettlement::Worker(OperationOutcome::Accepted));
        assert_eq!(issued.len(), roster(1).len());
        let original = backend_failure(1, "original admitted backend lost before any visible row");
        let run = r.f.run.take().unwrap();
        let completed = NativeFact::RunCompleted {
            activation: activation(1),
        };
        let failed_at = if startup_first { at(2) } else { at(1) };
        r.f.now = at(1);
        if startup_first {
            let outstanding = r.f.core.facts().outstanding_actions();
            r.f.result(run, ActionResult::Native(completed.clone()));
            assert_eq!(
                r.f.core.facts().outstanding_actions(),
                outstanding,
                "the same run result without ActionExited does not retire its original action"
            );
            assert_eq!(r.f.core.facts().conclusion(), None);
            assert!(r.f.core.facts().failure().is_none());
            assert!(!r.f.effects.iter().any(|effect| matches!(
                effect.body,
                LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
            )));
            r.f.now = at(2);
            r.fail(activation(1), original.clone());
        } else {
            r.fail(activation(1), original.clone());
            let first =
                r.f.effects
                    .iter()
                    .find(|effect| {
                        matches!(
                            effect.body,
                            LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
                        )
                    })
                    .expect("the original AP failure opens its preparation window")
                    .action;
            r.f.now = at(2);
            let outstanding = r.f.core.facts().outstanding_actions();
            r.f.result(run, ActionResult::Native(completed));
            assert_eq!(
                r.f.core.facts().outstanding_actions(),
                outstanding,
                "late normal startup observation is not an actual action exit"
            );
            assert!(r.f.effects.iter().any(|effect| effect.action == first));
        }
        let prepares =
            r.f.effects
                .iter()
                .filter(|effect| {
                    matches!(
                        effect.body,
                        LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
                    )
                })
                .collect::<Vec<_>>();
        assert_eq!(
            prepares.len(),
            1,
            "one original AP failure, one recovery intent"
        );
        let LogicalEffectBody::Native(NativeEffect::PrepareSuccessor {
            failed,
            replacement,
            topology,
            deadline,
        }) = &prepares[0].body
        else {
            unreachable!()
        };
        assert_eq!(*failed, activation(1).execution());
        assert_eq!(*replacement, activation(2).execution());
        assert!(matches!(
            *topology,
            SuccessorTopology::ExcludeProcess(process) if process == root(1).backend_process_id()
        ));
        assert_eq!(
            *deadline,
            failed_at.saturating_add(Duration::from_millis(50)),
            "startup completion neither opens nor renews the AP failure's preparation window"
        );
        assert_eq!(r.f.core.facts().conclusion(), None);
        assert!(r.f.core.facts().failure().is_none());
        assert_eq!(r.f.core.facts().attempts_started(), 1);
        assert!(!r.f.core.facts().output_visible());
        assert!(r.f.core.facts().ready_cut().is_none());
        assert!(!r.f.core.facts().protocol_join_ready());
        let attempt = r.f.core.facts().attempts().next().unwrap();
        assert_eq!(attempt.participations, roster(1));
        assert_eq!(attempt.accepted_failure, Some(original.input()));
        assert!(attempt.native_run_failure.is_none());
        assert!(!attempt.all_participations_closed);
        assert!(!attempt.cleanup_complete);
        assert!(
            !r.f.effects.iter().any(|effect| matches!(
                &effect.body,
                LogicalEffectBody::Consumer(
                    ConsumerEffect::PublishFailure(_)
                        | ConsumerEffect::PublishCompletion
                        | ConsumerEffect::PublishReady { .. }
                        | ConsumerEffect::ReserveEndSlot { .. }
                        | ConsumerEffect::EnqueueEnd { .. }
                ) | LogicalEffectBody::Native(NativeEffect::RecordStreamingSuccess { .. })
                    | LogicalEffectBody::Relay(RelayEffect::Reply {
                        response: RelayResponse::VisibilityGranted,
                        ..
                    })
            )),
            "normal startup closure cannot publish failure, success, Ready or visibility"
        );
    }
}

#[test]
fn independent_native_drop_failure_still_vetoes_with_the_same_original_backend_report() {
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
    r.establish_initial(AttemptIssueSettlement::Worker(OperationOutcome::Accepted));
    let original = backend_failure(
        1,
        "original AP error retained across an independent Drop failure",
    );
    r.fail(activation(1), original.clone());
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(r.f.effects.iter().any(|effect| matches!(
        effect.body,
        LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { .. })
    )));
    // The projection for a failed original future Drop can retain the ordinary
    // returned error as its report. The independent-veto fact discriminator,
    // not a newly invented panic error, carries the separate supervision failure.
    let report = FailureReport::from_attempt(&original);
    let run = r.f.run.take().unwrap();
    r.f.result(
        run,
        ActionResult::Native(NativeFact::NativeRunFailed {
            activation: activation(1),
            failure: report.clone(),
        }),
    );
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Failed)
    );
    assert_eq!(r.f.core.facts().failure(), Some(original.error()));
    assert_eq!(r.f.core.facts().attempts_started(), 1);
    assert!(!r.f.core.facts().output_visible());
    assert!(r.f.core.facts().ready_cut().is_none());
    let attempt = r.f.core.facts().attempts().next().unwrap();
    assert_eq!(attempt.accepted_failure, Some(original.input()));
    assert_eq!(attempt.native_run_failure, Some(&report));
    assert!(!attempt.all_participations_closed);
    assert!(!attempt.cleanup_complete);
    assert!(!r.f.core.facts().protocol_join_ready());
    assert!(!r.f.effects.iter().any(|effect| matches!(
        effect.body,
        LogicalEffectBody::Native(
            NativeEffect::ActivateReservation { .. } | NativeEffect::ActivateAttempt { .. }
        ) | LogicalEffectBody::Consumer(
            ConsumerEffect::PublishCompletion
                | ConsumerEffect::PublishReady { .. }
                | ConsumerEffect::EnqueueEnd { .. }
        )
    )));
}
