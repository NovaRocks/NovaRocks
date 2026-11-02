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

// Included inside manifest_round::tests; use its original QA actor/drive and
// exact TaskRound fixtures rather than a second status or completion owner.
mod native_tail {
    use super::*;
    use crate::task_execution::completion::WriteVerdict;
    use crate::task_execution::protocol_facts::WriteProtocolTracker;
    use crate::task_execution::status_intake::{StatusEvent, StatusIntakeHandle};
    use novarocks_execution_contract::{
        QueryContextRef, SafeDetail, TaskFailure, TaskFailurePhase, TaskIdentity, TaskOutputFacts,
        TaskState, TaskStatus, TaskStatusVersion, TerminationDetail,
    };
    use novarocks_failure_contract::FailureProjection;
    use novarocks_query_application::api::PlanSeal;
    use novarocks_query_application::execution_error::{ExecutionFailureInput, QueryFailureReason};
    use std::collections::BTreeSet;
    use std::future::{Future, poll_fn};
    use std::pin::Pin;
    use std::task::Poll;

    /// Poll the same original run exactly once. Its output is forbidden while
    /// a frozen strict obligation is still absent; dropping this borrowed Pin
    /// does not recreate, cancel or transfer the actual run future.
    async fn still_pending<F: Future + ?Sized>(mut future: Pin<&mut F>) {
        poll_fn(|cx| match future.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("root seal cannot end Native run before its strict tail"),
        })
        .await;
    }

    fn finish_task(status: &StatusIntakeHandle, task: TaskIdentity) {
        for (version, state, output) in [
            (2, TaskState::Running, false),
            (3, TaskState::Flushing, false),
            (4, TaskState::Finished, true),
        ] {
            status.publish(StatusEvent::Published(Box::new(
                TaskStatus::try_new(
                    task,
                    TaskStatusVersion::new(version).unwrap(),
                    state,
                    None,
                    TaskOutputFacts::new(output),
                )
                .unwrap()
                .with_installed(),
            )));
        }
    }

    async fn close_actor(
        mut logical: novarocks_query_application::test_support::LogicalExecutionTestHarness,
        contexts: &[QueryContextRef],
    ) {
        logical.abandon_running_attempt();
        for &context in contexts {
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while logical
                    .stand_down_snapshot(context)
                    .await
                    .unwrap()
                    .is_none()
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            logical
                .observe_worker_process_replaced(context)
                .await
                .unwrap();
        }
        logical
            .finish_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn original_run_keeps_folding_independent_writer_and_summary_tail_after_root_seal() {
        for writer_first in [true, false] {
            let (mut round, status, _acks) =
                super::super::super::tests::strict_split_failure_convergence_round_fixture();
            let root = round.root_task();
            let upstream = round
                .execution()
                .graph()
                .tasks()
                .map(|task| task.identity())
                .filter(|task| *task != root)
                .collect::<Vec<_>>();
            assert_eq!(upstream.len(), 2);
            let source_task = *upstream
                .iter()
                .find(|task| task.stage_id().get() == 1)
                .unwrap();
            let writer_task = *upstream
                .iter()
                .find(|task| task.stage_id().get() == 2)
                .unwrap();
            let contexts = round
                .execution()
                .graph()
                .contexts()
                .copied()
                .collect::<Vec<_>>();
            let (logical, drive, cancellation, _, _abort) =
                terminal_regression_drive_for_contexts(contexts.clone(), false).await;
            let plan = PlanSeal::Version(
                novarocks_physical_plan::PlanVersionId::try_new([0x19; 16]).unwrap(),
            );
            let mut protocol = round
                .install_protocol_facts(
                    plan,
                    &BTreeSet::from([writer_task]),
                    &BTreeSet::from([source_task]),
                )
                .unwrap();
            let source = round.take_root_status_source().unwrap();
            let mut seal = source.begin_success_seal_request().unwrap();
            // Give the original ordered control intake its own complete turn.
            round.turn().unwrap();
            assert_eq!(seal.try_recv().unwrap(), Ok(()));
            assert!(round.accepted_root_success_sealed());
            assert!(!round.execution().normal_drain_started());
            let mut assembled = unstarted_close_round(round);
            let notify = Arc::clone(&assembled.notify);
            let mut run = Box::pin(assembled.run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            ));
            still_pending(run.as_mut()).await;
            let initial = protocol
                .take_changes(drive.identity(), root, plan)
                .unwrap()
                .unwrap();
            assert!(initial.root_success_sealed());
            let mut tracker = WriteProtocolTracker::from_initial(&initial).unwrap();
            assert!(tracker.execution_verdict().is_pending());
            let (first, last) = if writer_first {
                (writer_task, source_task)
            } else {
                (source_task, writer_task)
            };
            finish_task(&status, first);
            notify.notify_one();
            still_pending(run.as_mut()).await;
            let changed = protocol
                .take_changes(drive.identity(), root, plan)
                .unwrap()
                .unwrap();
            tracker.observe(&changed).unwrap();
            assert!(
                matches!(tracker.execution_verdict(), WriteVerdict::TaskNotFinished { task, .. } if task == last)
            );
            finish_task(&status, last);
            notify.notify_one();
            let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), run.as_mut())
                .await
                .expect("the same run must fold its final late terminal");
            assert!(matches!(terminal, NativeAttemptTerminal::Completed));
            let changed = protocol
                .take_changes(drive.identity(), root, plan)
                .unwrap()
                .unwrap();
            tracker.observe(&changed).unwrap();
            assert_eq!(tracker.execution_verdict(), WriteVerdict::Complete);
            drop(run);
            assert!(assembled.round.execution().normal_drain_started());
            assert!(assembled.round.accepted_root_success_sealed());
            drop(assembled);
            drop(drive);
            close_actor(logical, &contexts).await;
        }
    }

    #[tokio::test]
    async fn original_run_preserves_source_failure_and_cancellation_during_strict_tail() {
        for cancel in [false, true] {
            let (mut round, status, _acks) =
                super::super::super::tests::strict_split_failure_convergence_round_fixture();
            let root = round.root_task();
            let source_task = round
                .execution()
                .graph()
                .tasks()
                .map(|task| task.identity())
                .find(|task| task.stage_id().get() == 1)
                .unwrap();
            let contexts = round
                .execution()
                .graph()
                .contexts()
                .copied()
                .collect::<Vec<_>>();
            let (logical, drive, cancellation, requester, _abort) =
                terminal_regression_drive_for_contexts(contexts.clone(), false).await;
            let source = round.take_root_status_source().unwrap();
            let mut seal = source.begin_success_seal_request().unwrap();
            round.turn().unwrap();
            assert_eq!(seal.try_recv().unwrap(), Ok(()));
            assert!(round.accepted_root_success_sealed());
            assert!(!round.execution().normal_drain_started());
            let mut assembled = unstarted_close_round(round);
            let notify = Arc::clone(&assembled.notify);
            let mut run = Box::pin(assembled.run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            ));
            still_pending(run.as_mut()).await;
            let expected = if cancel {
                requester.request(CancellationReason::Requested).unwrap();
                let NativeAttemptTerminal::Failed(original) =
                    cancellation_failure(CancellationReason::Requested)
                else {
                    unreachable!()
                };
                original.input().clone()
            } else {
                let failure = TaskFailure::new_in_phase(
                    QueryFailureReason::SplitAssignmentFailed.failure_fact(),
                    SafeDetail::truncating("original strict source failed after root seal"),
                    TaskFailurePhase::Execution,
                );
                let termination = TerminationDetail::Failed(failure);
                let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
                    source_task,
                    termination.clone(),
                );
                let original = ExecutionFailureInput::from_selected_task(&selected).unwrap();
                status.publish(StatusEvent::Published(Box::new(
                    TaskStatus::try_new(
                        source_task,
                        TaskStatusVersion::new(2).unwrap(),
                        TaskState::Failing,
                        Some(termination),
                        TaskOutputFacts::default(),
                    )
                    .unwrap()
                    .with_installed(),
                )));
                original
            };
            notify.notify_one();
            let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), run.as_mut())
                .await
                .expect("strict tail control cannot wait for a successful normal drain");
            let NativeAttemptTerminal::Failed(failure) = terminal else {
                panic!("root seal cannot erase the original strict-tail failure/control");
            };
            assert_eq!(failure.input(), &expected);
            if !cancel {
                assert!(
                    matches!(failure.input().site(), novarocks_query_application::execution_error::ExecutionFailureSite::Task { identity, .. } if identity == source_task)
                );
            }
            drop(run);
            assert!(assembled.round.accepted_root_success_sealed());
            assert!(!assembled.round.execution().normal_drain_started());
            assert_eq!(assembled.round.root_task(), root);
            drop(assembled);
            drop(drive);
            close_actor(logical, &contexts).await;
        }
    }

    #[tokio::test]
    async fn ordinary_read_native_run_still_returns_at_root_seal_with_unrelated_tasks_live() {
        let (mut round, _acks) = super::super::super::tests::manifest_terminal_regression_fixture();
        let root = round.root_task();
        let unrelated = round
            .execution()
            .graph()
            .tasks()
            .map(|task| task.identity())
            .filter(|task| *task != root)
            .collect::<Vec<_>>();
        assert!(!unrelated.is_empty());
        assert!(unrelated.iter().all(|task| {
            !round
                .execution()
                .task(task.task_id())
                .unwrap()
                .is_terminal()
        }));
        let contexts = round
            .execution()
            .graph()
            .contexts()
            .copied()
            .collect::<Vec<_>>();
        let (logical, drive, cancellation, _, _abort) =
            terminal_regression_drive_for_contexts(contexts.clone(), false).await;
        let source = round.take_root_status_source().unwrap();
        let mut seal = source.begin_success_seal_request().unwrap();
        round.turn().unwrap();
        assert_eq!(seal.try_recv().unwrap(), Ok(()));
        assert!(round.execution().normal_drain_started());
        let mut assembled = unstarted_close_round(round);
        let terminal = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            assembled.run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            ),
        )
        .await
        .expect("DemandDriven reads must not acquire a writer-style all-task barrier");
        assert!(matches!(terminal, NativeAttemptTerminal::Completed));
        assert!(unrelated.iter().all(|task| {
            !assembled
                .round
                .execution()
                .task(task.task_id())
                .unwrap()
                .is_terminal()
        }));
        drop(assembled);
        drop(drive);
        close_actor(logical, &contexts).await;
    }
}
