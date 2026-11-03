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

//! Real original Work/cut/author custody joined to the pure Participation actor.
//! These symbolic terminal facts do not attest to runtime physical exit.

use super::*;
use novarocks_execution::runtime::task::{LocalTaskOutcome, LocalTaskTerminalFact};
use novarocks_execution_contract::{OperatorStatistics, SafeDetail, TaskOutputFacts, TaskState};
use novarocks_types::UniqueId;
use novarocks_worker_protocol::participation::ParticipationCommand;
use std::panic::{AssertUnwindSafe, catch_unwind};

fn observation() -> crate::TaskTerminalObservation {
    crate::TaskTerminalObservation {
        intent: crate::TaskTerminalIntent::FinishExistingTermination {
            output: Some(TaskOutputFacts::new(true)),
        },
        normal_close_validated: false,
        output_released: true,
        final_statistics: Some(crate::TaskTerminalStatistics {
            statistics: vec![
                OperatorStatistics::new(1, SafeDetail::truncating("scan")).with_output_rows(7),
            ],
            truncated: true,
        }),
    }
}
fn fact(identity: TaskIdentity) -> LocalTaskTerminalFact {
    LocalTaskTerminalFact::new(
        identity.query_execution_id().query_id(),
        UniqueId::new(1, 2),
        LocalTaskOutcome::Succeeded,
        None,
    )
}
fn fees(f: &Fixture) -> u64 {
    f.runtime.ledger.used(Dimension::ActiveTasks.id())
}
fn assert_unknown(f: &Fixture, funded: u64) {
    assert_eq!(f.custody.phase(), ExitCustodyPhase::Unresolved);
    assert!(!f.custody.actual_exit_ready().unwrap());
    assert!(!f.custody.try_retire().unwrap());
    assert!(!f.ingress.event_unused(f.custody.cut()).unwrap());
    assert_eq!(fees(f), funded);
    assert!(f.health.failure().is_some());
}

#[test]
fn a_bound_terminal_action_hands_off_one_observation_after_its_cancel_prefix() {
    let mut f = fixture_with_author(true);
    let task = f.custody.task();
    let permit = crate::TaskTerminalPermit::from_admitted_author(f.author.take().unwrap());
    let ingress = f.ingress.clone();
    let mut action = crate::TaskTerminalAction::new(
        permit,
        Box::new(move |_, observation_permit| {
            drop(ingress.submit(TaskConclusionProposal::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )));
            observation_permit.handoff(observation()).unwrap();
        }),
    );
    action.bind();
    let funded = fees(&f);
    assert!(action.invoke(&fact(task)));
    drop(action);
    assert_eq!(f.custody.phase(), ExitCustodyPhase::Exited);
    assert!(!f.custody.observe_consumers(&mut f.inbox).unwrap());
    assert_eq!(fees(&f), funded);
    let delivery = f.inbox.deliver_next().unwrap().unwrap();
    assert!(delivery.prefix.is_some());
    assert!(!f.inbox.event_core_consumed(delivery.cut).unwrap());
    f.runtime.runtime.poll_one(Tick(1)).unwrap();
    assert!(!f.runtime.runtime.snapshot(actor()).unwrap().failed);
    f.inbox.observe_core(delivery.cut).unwrap();
    let mut effects = Vec::new();
    if let Some(prefix) = delivery.prefix {
        effects.extend(f.runtime.drain(prefix));
    }
    effects.extend(f.runtime.drain(delivery.event));
    let terminal = effects.iter().position(|effect| matches!(effect,
        WorkerAction::Participation(ParticipationEffect::PublishTask {state: TaskState::Canceled, convergence, ..})
            if !convergence.actual_stopped())).expect("the original cancel finishes without a physical-exit claim");
    let output = effects
        .iter()
        .position(|effect| {
            matches!(effect,
        WorkerAction::Participation(ParticipationEffect::PublishTaskConvergence {snapshot, ..})
            if snapshot.output_released() && !snapshot.actual_stopped())
        })
        .expect("the independent positive output fact follows");
    assert!(terminal < output);
    let final_effects = f.runtime.run(
        ParticipationInput::Request(ParticipationCommand::GetFinalInfo { task }),
        Some(operation(20)),
    );
    let info = final_effects
        .iter()
        .find_map(|effect| match effect {
            WorkerAction::Reply(reply) => match reply.body.as_ref() {
                novarocks_worker_protocol::participation::ParticipationReplyBody::FinalInfo(
                    info,
                ) => Some(info.clone()),
                _ => None,
            },
            _ => None,
        })
        .expect("the same actor froze the final statistics");
    assert_eq!(info.final_status().state(), TaskState::Canceled);
    assert_eq!(info.operator_statistics()[0].output_rows(), Some(7));
    assert!(info.operator_statistics_truncated());
    assert!(f.custody.observe_consumers(&mut f.inbox).unwrap());
    f.ingress
        .observe_cut_exit(delivery.cut, true, true)
        .unwrap();
    f.ingress.consume_cut(delivery.cut, 0).unwrap();
    f.retire_cut(delivery.cut);
    assert!(f.health.failure().is_none());
    f.finish();
}

#[test]
fn withdrawing_a_bound_uninvoked_terminal_action_revokes_only_its_original_input() {
    let mut f = fixture_with_author(true);
    let permit = crate::TaskTerminalPermit::from_admitted_author(f.author.take().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let mut action = crate::TaskTerminalAction::new(
        permit,
        Box::new(move |_, _| {
            called.fetch_add(1, Ordering::SeqCst);
        }),
    );
    action.bind();
    let funded = fees(&f);
    assert!(action.withdraw_uninvoked());
    drop(action);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(f.ingress.event_unused(f.custody.cut()).unwrap());
    assert!(f.inbox.deliver_next().unwrap().is_none());
    assert_eq!(f.custody.phase(), ExitCustodyPhase::Exited);
    assert_eq!(fees(&f), funded);
    assert!(!f.custody.try_retire().unwrap());
    assert!(f.custody.observe_consumers(&mut f.inbox).unwrap());
    let cut = f.custody.cut();
    f.ingress.observe_cut_exit(cut, true, true).unwrap();
    f.retire_cut(cut);
    assert!(f.health.failure().is_none());
    f.finish();
}

#[test]
fn dropping_a_bound_terminal_action_without_invocation_keeps_unknown_and_its_fee() {
    let mut f = ManuallyDrop::new(fixture_with_author(true));
    let permit = crate::TaskTerminalPermit::from_admitted_author(f.author.take().unwrap());
    let mut action = crate::TaskTerminalAction::new(
        permit,
        Box::new(|_, _| panic!("this callback was not invoked")),
    );
    action.bind();
    let funded = fees(&f);
    drop(action);
    assert_unknown(&f, funded);
    assert!(f.inbox.deliver_next().unwrap().is_none());
    // No actual callback/exit receipt or failure publication-drained proof is
    // available; retain the original finite fixture instead of fake cleanup.
}

#[test]
fn a_panicking_or_silently_discarding_callback_cannot_complete_the_original_observation() {
    for panics in [false, true] {
        let mut f = ManuallyDrop::new(fixture_with_author(true));
        let task = f.custody.task();
        let permit = crate::TaskTerminalPermit::from_admitted_author(f.author.take().unwrap());
        let mut action = crate::TaskTerminalAction::new(
            permit,
            Box::new(move |_, original| {
                if panics {
                    panic!("original terminal callback panicked before handoff");
                }
                drop(original);
            }),
        );
        action.bind();
        let funded = fees(&f);
        let invoked = catch_unwind(AssertUnwindSafe(|| action.invoke(&fact(task))));
        match (panics, invoked) {
            (true, Err(payload)) => drop(payload),
            (false, Ok(completed)) => assert!(!completed),
            (true, Ok(_)) => panic!("the original panic must not be reported as a normal return"),
            (false, Err(payload)) => std::panic::resume_unwind(payload),
        }
        drop(action);
        assert_unknown(&f, funded);
        assert!(f.inbox.deliver_next().unwrap().is_none());
    }
}

#[test]
fn an_unbound_terminal_handoff_retains_the_exact_observation_as_unknown() {
    let mut f = ManuallyDrop::new(fixture_with_author(true));
    let task = f.custody.task();
    let permit = crate::TaskTerminalPermit::from_admitted_author(f.author.take().unwrap());
    let expected = observation();
    let funded = fees(&f);
    assert!(permit.handoff(expected.clone()).is_err());
    assert!(
        matches!(f.custody.take_rejected(), Some(ParticipationInput::TerminalObservation {task: original, observation}) if original == task && observation == expected)
    );
    assert!(f.custody.take_rejected().is_none());
    assert_unknown(&f, funded);
    assert!(f.inbox.deliver_next().unwrap().is_none());
}
