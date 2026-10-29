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

//! Actual statistics role/input custody with explicit pure Context facts.
//! The model does not claim Native runtime or callback physical exit.

use super::*;
use crate::{
    control_admission::Dimension,
    control_runtime::actors::{
        WorkerAction,
        tests::{Fixture, actor, generation, operation, task},
    },
};
use novarocks_control_runtime::{completion::ReportSlot, driver::EffectTicket};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::ConsumerKind,
    effect::EffectState,
    identity::Tick,
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{OperatorStatistics, SafeDetail, TaskOutputFacts};
use novarocks_worker_protocol::context::ContextCommand;
use novarocks_worker_protocol::{
    context::{ContextEffect, TerminalIntent, TerminalObservation},
    creation::{CreationCost, PayloadId},
};
use std::{
    collections::BTreeMap,
    mem::ManuallyDrop,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Notice(AtomicUsize);
impl TaskPendingNotice for Notice {
    fn pending(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn sample(rows: u64, truncated: bool) -> TerminalStatistics {
    TerminalStatistics {
        statistics: vec![
            OperatorStatistics::new(1, SafeDetail::truncating("scan")).with_output_rows(rows),
        ],
        truncated,
    }
}
fn command(f: &mut Fixture, input: ContextInput) -> Vec<WorkerAction> {
    let input = f.send(input, None, InputClass::Lifecycle, 2);
    f.runtime.poll_one(Tick(1)).unwrap();
    f.drain(input)
}
fn request(
    f: &mut Fixture,
    command: ContextCommand,
    operation: novarocks_execution_contract::TaskOperationId,
) -> Vec<WorkerAction> {
    let input = f.send(
        ContextInput::Request(command),
        Some(operation),
        InputClass::Ordinary,
        2,
    );
    f.runtime.poll_one(Tick(1)).unwrap();
    f.drain(input)
}
fn fixture() -> (
    Fixture,
    TaskStatisticsInbox,
    TaskStatisticsProducer,
    TaskStatisticsFinalizer,
) {
    let mut f = Fixture::new();
    let identity = task(f.identity, 1);
    request(
        &mut f,
        ContextCommand::Create {
            task: identity,
            payload: PayloadId(3),
        },
        operation(3),
    );
    let mut loaded = command(
        &mut f,
        ContextInput::Loaded {
            task: identity,
            result: Ok(CreationCost {
                input_retained_bytes: 1,
                ingress_retained_bytes: 1,
                structural_bytes: 1,
                inbound_records: 0,
                inbound_bytes: 0,
            }),
        },
    );
    while loaded
        .iter()
        .any(|effect| matches!(effect, WorkerAction::Context(ContextEffect::WakeInternal)))
    {
        loaded = command(&mut f, ContextInput::NextInternal);
    }
    command(&mut f, ContextInput::Installed { task: identity });
    // This role's separate position is charged explicitly before any producer
    // captures it; the Task host factory is tested in its integration slice.
    let (work, capacity) = f
        .scope
        .reserve_next(
            generation(),
            ResourceVector::new([(Dimension::ActiveTasks.id(), 1)]).unwrap(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let report = ReportSlot::reserve_next(
        &f.scope,
        generation(),
        work.id(),
        2,
        [
            (
                ConsumerId::new(1).unwrap(),
                ConsumerKind::Owner {
                    scope: f.scope.id(),
                    generation: generation(),
                },
            ),
            (ConsumerId::new(2).unwrap(), ConsumerKind::Observer),
        ],
    )
    .unwrap();
    let input = f
        .runtime
        .reserve_admitted_input(
            actor(),
            ActorId::new(23).unwrap(),
            InputClass::Ordinary,
            &work,
        )
        .unwrap()
        .into_role()
        .unwrap_or_else(|_| panic!("the original prepaid statistics input converts once"));
    let (inbox, producer, finalizer) = TaskStatisticsInbox::from_admitted(
        AdmittedCompletionRole {
            work,
            capacity,
            report,
        },
        input,
        StatisticsBinding {
            task: identity,
            runtime: f.runtime.clone(),
            actor: actor(),
            pending: Arc::new(Notice(AtomicUsize::new(0))),
            health: Arc::new(ExecutionHealth::default()),
        },
    );
    (f, inbox, producer, finalizer)
}
fn final_info(
    f: &mut Fixture,
    snapshot: TerminalStatistics,
) -> Arc<novarocks_execution_contract::FinalTaskInfo> {
    final_info_with_snapshot(f, Some(snapshot))
}
fn final_info_with_snapshot(
    f: &mut Fixture,
    snapshot: Option<TerminalStatistics>,
) -> Arc<novarocks_execution_contract::FinalTaskInfo> {
    let identity = task(f.identity, 1);
    command(
        f,
        ContextInput::TerminalObservation {
            task: identity,
            observation: TerminalObservation {
                intent: TerminalIntent::SuccessfulProduction {
                    completed_output: Some(TaskOutputFacts::new(true)),
                },
                normal_close_validated: false,
                output_released: true,
                final_statistics: snapshot,
            },
        },
    );
    command(f, ContextInput::CompletionExited { task: identity });
    request(
        f,
        ContextCommand::GetFinalInfo { task: identity },
        operation(20),
    )
    .into_iter()
    .find_map(|effect| match effect {
        WorkerAction::Reply(reply) => match reply.body.as_ref() {
            novarocks_worker_protocol::context::ContextReplyBody::FinalInfo(info) => {
                Some(info.clone())
            }
            _ => None,
        },
        _ => None,
    })
    .unwrap()
}
// Complete actual neutral effects without retiring the original input. Its
// single retirement owner remains TaskStatisticsInbox::advance.
fn complete_effects(f: &Fixture, input: InputId) -> Vec<WorkerAction> {
    let mut effects = Vec::new();
    loop {
        let inventory = f
            .runtime
            .effect_inventory(actor(), input, None, 15)
            .unwrap();
        let states: BTreeMap<_, _> = inventory.iter().map(|(d, s)| (d.id, *s)).collect();
        for (descriptor, state) in inventory {
            if state == EffectState::Pending {
                for predecessor in descriptor.dependencies {
                    if states[&predecessor] == EffectState::Completed {
                        f.runtime
                            .acknowledge_dependency(actor(), input, descriptor.id, predecessor)
                            .unwrap();
                    }
                }
            }
        }
        let ready = f.runtime.ready_effects(actor(), input, 15).unwrap();
        if ready.is_empty() {
            break;
        }
        for action in ready {
            let ticket: EffectTicket<WorkerAction> =
                f.runtime.take_effect(actor(), input, action).unwrap();
            effects.push(ticket.effect.clone());
            f.runtime.effect_completed(ticket).unwrap();
        }
    }
    effects
}

#[test]
fn repeated_samples_replace_one_pending_snapshot_while_the_original_input_is_busy() {
    let (mut f, mut inbox, producer, finalizer) = fixture();
    let funded = f.ledger.used(Dimension::ActiveTasks.id());
    let original_work = f.scope.snapshot().unwrap().work;
    assert_eq!(
        producer.submit(sample(1, true)),
        StatisticsSubmission::Pending
    );
    assert!(inbox.advance().unwrap().submitted);
    let first = inbox.input.in_flight().unwrap().unwrap();
    for rows in 2..=100 {
        assert_eq!(
            producer.submit(sample(rows, false)),
            StatisticsSubmission::Pending
        );
    }
    let waiting = inbox.advance().unwrap();
    assert!(!waiting.submitted && !waiting.consumed && !waiting.retired);
    assert_eq!(inbox.input.in_flight().unwrap(), Some(first));
    assert_eq!(f.scope.snapshot().unwrap().work, original_work);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), funded);
    f.runtime.poll_one(Tick(1)).unwrap();
    let next = inbox.advance().unwrap();
    assert!(next.consumed && next.submitted && !next.retired);
    let second = inbox.input.in_flight().unwrap().unwrap();
    assert_ne!(first, second);
    // A dispatched occurrence still owns its own input. Sealing captures
    // that latest snapshot without retiring the occurrence on its behalf.
    let actual = finalizer.seal_for_terminal(None).unwrap();
    assert_eq!(actual.statistics, sample(100, false).statistics);
    assert!(actual.truncated);
    assert_eq!(inbox.input.in_flight().unwrap(), Some(second));
    f.runtime.poll_one(Tick(1)).unwrap();
    assert!(inbox.advance().unwrap().consumed);
    assert_eq!(
        producer.submit(sample(999, false)),
        StatisticsSubmission::Sealed
    );
    let info = final_info(&mut f, actual);
    assert_eq!(info.operator_statistics()[0].output_rows(), Some(100));
    assert!(info.operator_statistics_truncated());
    assert!(!inbox.advance().unwrap().retired);
    drop(producer);
    assert!(inbox.advance().unwrap().retired);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()) + 1, funded);
    drop(inbox);
    f.finish();
}

#[test]
fn the_last_sample_after_an_empty_owner_take_survives_the_actual_producer_exit() {
    let (mut f, mut inbox, producer, finalizer) = fixture();
    // The worker below now owns the only actual producer. No terminal
    // finalizer can submit a replacement snapshot on this test's behalf.
    drop(finalizer);
    let funded = f.ledger.used(Dimension::ActiveTasks.id());
    let release = Arc::new(std::sync::Barrier::new(2));
    let worker_release = release.clone();
    let worker = std::thread::spawn(move || {
        worker_release.wait();
        let submitted = producer.submit(sample(91, true));
        drop(producer);
        submitted
    });
    let (progress, turn) = inbox.prepare_turn().unwrap();
    let turn = turn.expect("the original owner turn was prepared");
    let prepared_before_exit = !turn.producer_exited && turn.snapshot.is_none();
    release.wait();
    let submitted = worker.join().unwrap();
    let applied = inbox.apply_turn(progress, turn).unwrap();
    let premature = applied.retired
        || inbox.is_retired()
        || inbox
            .report()
            .consumption(ConsumerId::new(1).unwrap())
            .unwrap()
            == Consumption::Consumed
        || f.ledger.used(Dimension::ActiveTasks.id()) != funded;
    if premature {
        // A regressed retirement already claimed consumption without the
        // last sample. Keep its exact inventory; inventing an exit or a new
        // input here would hide the bug and could cause a second Drop panic.
        let _retained_original = ManuallyDrop::new((f, inbox));
        assert!(
            !premature,
            "the last sample was retired before Core consumption"
        );
        return;
    }
    assert!(prepared_before_exit);
    assert_eq!(submitted, StatisticsSubmission::Pending);
    assert!(!applied.submitted && !applied.consumed);
    assert!(inbox.input.in_flight().unwrap().is_none());
    let next = inbox.advance().unwrap();
    assert!(next.submitted && !next.retired);
    let original_input = inbox.input.in_flight().unwrap().unwrap();
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(2).unwrap())
            .unwrap(),
        Consumption::Consumed
    );
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(1).unwrap())
            .unwrap(),
        Consumption::Pending
    );
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), funded);
    f.runtime.poll_one(Tick(1)).unwrap();
    let drained = inbox.advance().unwrap();
    assert!(drained.consumed && drained.retired);
    assert!(
        f.runtime
            .input_custody(actor(), original_input)
            .unwrap()
            .is_none()
    );
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()) + 1, funded);
    // No terminal statistics override: only the actual periodic input can
    // account for these rows in the pure Context's final information.
    let info = final_info_with_snapshot(&mut f, None);
    let rows = info.operator_statistics()[0].output_rows();
    let truncated = info.operator_statistics_truncated();
    drop(inbox);
    f.finish();
    assert_eq!(rows, Some(91));
    assert!(truncated);
}

#[test]
fn committed_statistics_wait_for_actual_effect_exit_and_last_producer_before_refund() {
    let (mut f, mut inbox, producer, finalizer) = fixture();
    let identity = task(f.identity, 1);
    command(
        &mut f,
        ContextInput::Proposal {
            task: identity,
            proposal: novarocks_worker_protocol::proposal_summary::Proposal {
                conclusion: novarocks_execution_contract::TaskConclusion::Terminated(
                    novarocks_execution_contract::TerminationDetail::Canceled(
                        novarocks_execution_contract::CancelReason::UpstreamNoLongerNeeded,
                    ),
                ),
                evidence: None,
                source: None,
                diagnostic: None,
            },
        },
    );
    producer.submit(sample(7, false));
    assert!(inbox.advance().unwrap().submitted);
    let input = inbox.input.in_flight().unwrap().unwrap();
    let funded = f.ledger.used(Dimension::ActiveTasks.id());
    f.runtime.poll_one(Tick(1)).unwrap();
    drop(finalizer);
    drop(producer);
    let pending = inbox.advance().unwrap();
    assert!(!pending.retired && !pending.consumed);
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(2).unwrap())
            .unwrap(),
        Consumption::Consumed
    );
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(1).unwrap())
            .unwrap(),
        Consumption::Pending
    );
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), funded);
    assert!(f.runtime.input_custody(actor(), input).unwrap().is_some());
    let effects = complete_effects(&f, input);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        WorkerAction::Context(ContextEffect::StopTask { .. })
    )));
    let done = inbox.advance().unwrap();
    assert!(done.consumed && done.retired);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()) + 1, funded);
    drop(inbox);
    f.finish();
}

#[test]
fn terminal_seal_preserves_undispatched_latest_or_the_explicit_final_snapshot_and_sticky_truncation()
 {
    for final_rows in [None, Some(42)] {
        let (mut f, mut inbox, producer, finalizer) = fixture();
        producer.submit(sample(3, true));
        producer.submit(sample(9, false));
        assert!(inbox.input.in_flight().unwrap().is_none());
        let actual = finalizer
            .seal_for_terminal(final_rows.map(|rows| sample(rows, false)))
            .unwrap();
        let expected = final_rows.unwrap_or(9);
        assert_eq!(actual.statistics, sample(expected, false).statistics);
        assert!(actual.truncated);
        assert_eq!(
            producer.submit(sample(1000, false)),
            StatisticsSubmission::Sealed
        );
        let info = final_info(&mut f, actual);
        assert_eq!(info.operator_statistics()[0].output_rows(), Some(expected));
        assert!(info.operator_statistics_truncated());
        assert!(!inbox.advance().unwrap().submitted);
        drop(producer);
        assert!(inbox.advance().unwrap().retired);
        drop(inbox);
        f.finish();
    }
}

#[test]
fn periodic_and_final_statistics_relinquish_oversized_backing_without_false_truncation() {
    let wide_snapshot = |rows, entries| {
        let mut statistics = Vec::with_capacity(100_000);
        statistics.extend((0..entries).map(|_| {
            OperatorStatistics::new(1, SafeDetail::truncating("scan")).with_output_rows(rows)
        }));
        TerminalStatistics {
            statistics,
            truncated: false,
        }
    };
    for (entries, expected_truncated) in [(1, false), (FINAL_TASK_INFO_MAX_OPERATORS + 1, true)] {
        let (mut f, mut inbox, producer, finalizer) = fixture();
        let submission = producer.submit(wide_snapshot(13, entries));
        let (pending_bounded, latest_bounded, pending_len, pending_truncated) = {
            let state = inbox.custody.state.lock().unwrap();
            let pending = state.pending.as_ref().unwrap();
            let latest = state.latest.as_ref().unwrap();
            (
                pending.statistics.capacity() <= FINAL_TASK_INFO_MAX_OPERATORS,
                latest.statistics.capacity() <= FINAL_TASK_INFO_MAX_OPERATORS,
                pending.statistics.len(),
                pending.truncated,
            )
        };
        let actual = finalizer
            .seal_for_terminal(Some(wide_snapshot(31, entries)))
            .unwrap();
        let final_bounded = actual.statistics.capacity() <= FINAL_TASK_INFO_MAX_OPERATORS;
        let final_truncated = actual.truncated;
        let info = final_info(&mut f, actual);
        let final_len = info.operator_statistics().len();
        let final_rows = info.operator_statistics()[0].output_rows();
        let info_truncated = info.operator_statistics_truncated();
        drop(producer);
        let retired = inbox.advance().unwrap().retired;
        drop(inbox);
        f.finish();
        // Check after the real role/input/scope cleanup, so an oversized
        // backing regression reports this oracle without a second Drop panic.
        assert_eq!(submission, StatisticsSubmission::Pending);
        assert!(pending_bounded && latest_bounded && final_bounded);
        assert_eq!(pending_len, entries.min(FINAL_TASK_INFO_MAX_OPERATORS));
        assert_eq!(final_len, entries.min(FINAL_TASK_INFO_MAX_OPERATORS));
        assert_eq!(final_rows, Some(31));
        assert_eq!(pending_truncated, expected_truncated);
        assert_eq!(final_truncated, expected_truncated);
        assert_eq!(info_truncated, expected_truncated);
        assert!(retired);
    }
}

#[test]
fn observer_aliases_cannot_sign_producer_exit_and_only_the_last_actual_clone_releases_it() {
    let (f, mut inbox, producer, finalizer) = fixture();
    let clone = producer.clone();
    let funded = f.ledger.used(Dimension::ActiveTasks.id());
    let observer_alias = inbox.custody.clone();
    assert!(finalizer.seal_for_terminal(None).is_none());
    drop(producer);
    drop(observer_alias);
    assert_eq!(inbox.phase(), ExitRolePhase::Admitted);
    assert!(!inbox.advance().unwrap().retired);
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), funded);
    drop(clone);
    assert_eq!(inbox.phase(), ExitRolePhase::Exited);
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(1).unwrap())
            .unwrap(),
        Consumption::Pending
    );
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(2).unwrap())
            .unwrap(),
        Consumption::Pending
    );
    assert!(inbox.advance().unwrap().retired);
    assert!(inbox.is_retired());
    drop(inbox);
    f.finish();
}

#[test]
fn a_failed_scope_keeps_the_exact_refused_sample_and_independent_physical_observer_leg() {
    let (f, mut inbox, producer, finalizer) = fixture();
    let identity = producer.identity();
    let funded = f.ledger.used(Dimension::ActiveTasks.id());
    producer.submit(sample(17, true));
    f.scope.fail().unwrap();
    assert!(matches!(
        inbox.advance(),
        Err(StatisticsError::Driver(DriverError::ActorFailed))
    ));
    match inbox.rejected.as_ref() {
        Some(WorkerMessage::Context(ContextMessage::Command {
            request: None,
            command:
                ContextInput::OperatorStatistics {
                    task,
                    statistics,
                    truncated: true,
                },
            ..
        })) => {
            assert_eq!(*task, identity);
            assert_eq!(statistics, &sample(17, true).statistics);
        }
        _ => panic!("the exact refused original statistics occurrence remains owned"),
    }
    assert!(inbox.input.in_flight().unwrap().is_none());
    drop(finalizer);
    drop(producer);
    assert!(matches!(
        inbox.advance(),
        Err(StatisticsError::RefusedInput)
    ));
    assert_eq!(inbox.phase(), ExitRolePhase::Exited);
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(2).unwrap())
            .unwrap(),
        Consumption::Consumed
    );
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(1).unwrap())
            .unwrap(),
        Consumption::Pending
    );
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), funded);
    assert!(!inbox.is_retired());
    // The producer really ended, but this failed Core has no actual S8
    // publication-drained/abandonment proof. Retain its exact finite inventory.
    let _retained_original = ManuallyDrop::new((f, inbox));
}

#[test]
fn an_actual_failed_owner_input_is_reported_without_claiming_core_consumption() {
    let (f, inbox, producer, finalizer) = fixture();
    // This case has no failure-publication-drained receipt. Retain the exact
    // failed actor, paid role and original taken payload instead of claiming
    // S8 disposal or using a successful Core report as substitute evidence.
    let f = ManuallyDrop::new(f);
    let mut inbox = ManuallyDrop::new(inbox);
    drop(finalizer);
    let funded = f.ledger.used(Dimension::ActiveTasks.id());
    let identity = producer.identity();
    let bad = f.send(
        ContextInput::Installed {
            task: task(f.identity, 999),
        },
        None,
        InputClass::Lifecycle,
        2,
    );
    producer.submit(sample(37, true));
    assert!(inbox.advance().unwrap().submitted);
    let statistics_input = inbox.input.in_flight().unwrap().unwrap();
    assert_ne!(bad, statistics_input);
    // The real Lifecycle input has priority over the Ordinary statistics
    // occurrence, which has never been consumed by the failed core.
    f.runtime.poll_one(Tick(1)).unwrap();
    assert!(f.runtime.snapshot(actor()).unwrap().failed);
    let taken_original = ManuallyDrop::new(
        f.runtime
            .take_failed_input(actor(), InputClass::Ordinary)
            .unwrap()
            .expect("the failed owner takes the actual queued statistics occurrence"),
    );
    assert_eq!(taken_original.0.input, statistics_input);
    match &taken_original.1 {
        WorkerMessage::Context(ContextMessage::Command {
            request: None,
            command:
                ContextInput::OperatorStatistics {
                    task,
                    statistics,
                    truncated: true,
                },
            ..
        }) => {
            assert_eq!(*task, identity);
            assert_eq!(statistics, &sample(37, true).statistics);
        }
        _ => panic!("the actual failure owner retains the same original statistics payload"),
    }
    assert_eq!(
        f.runtime
            .input_custody(actor(), statistics_input)
            .unwrap()
            .unwrap()
            .phase,
        InputPhase::AbandonedByOwnerFailure
    );
    assert!(matches!(
        inbox.advance(),
        Err(StatisticsError::OwnerFailed { input }) if input == statistics_input
    ));
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(2).unwrap())
            .unwrap(),
        Consumption::Pending
    );
    drop(producer);
    assert!(matches!(
        inbox.advance(),
        Err(StatisticsError::OwnerFailed { input }) if input == statistics_input
    ));
    assert_eq!(inbox.phase(), ExitRolePhase::Exited);
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(2).unwrap())
            .unwrap(),
        Consumption::Consumed
    );
    assert_eq!(
        inbox
            .report()
            .consumption(ConsumerId::new(1).unwrap())
            .unwrap(),
        Consumption::Pending
    );
    assert_eq!(f.ledger.used(Dimension::ActiveTasks.id()), funded);
    assert_eq!(inbox.input.in_flight().unwrap(), Some(statistics_input));
    assert!(!inbox.is_retired());
}
