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

// Included inside attempt_convergence::tests. Only the original source, pump,
// accepted covered receipts and convergence owner author the observed facts.
mod split_failure_tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use crate::native::data_runtime::FrontendDataRuntime;
    use crate::query_execution::split_assignment::SplitAssignmentDriverError;
    use crate::query_execution::split_assignment_round::{
        SplitAssignmentProfileRead, SplitAssignmentRoundGuard, failure_observation_plan,
    };
    use crate::task_execution::status_intake::{NotifyWake, StatusIntakeWake};
    use novarocks_failure_contract::FailureProjection;
    use novarocks_query_application::execution_error::QueryFailureReason;

    // This gate belongs to the real existing source fixture. Releasing on
    // unwind prevents an assertion from abandoning its original blocking close.
    struct SourceCloseRelease(Option<std::sync::mpsc::Sender<()>>);
    impl SourceCloseRelease {
        fn open(&mut self) {
            self.0.take().unwrap().send(()).unwrap();
        }
    }
    impl Drop for SourceCloseRelease {
        fn drop(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn normal_convergence_observes_original_split_failure_once_then_really_closes() {
        let (mut round, _original_status, _original_acks) =
            crate::task_execution::tests::strict_split_failure_convergence_round_fixture();
        let contexts = round
            .execution()
            .graph()
            .contexts()
            .copied()
            .collect::<Vec<_>>();
        let execution = contexts[0].query_execution_id();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let mut executors =
            crate::task_execution::blocking_io::tests::OriginalExecutorsFixture::install(
                data_runtime.connector_blocking_io(),
            );
        let (release, released) = std::sync::mpsc::channel();
        let mut release = SourceCloseRelease(Some(release));
        let (plan, enumerations, closes) = failure_observation_plan(
            execution,
            data_runtime.connector_blocking_io().clone(),
            Some(released),
        );
        let notify = Arc::new(tokio::sync::Notify::new());
        let guard = SplitAssignmentRoundGuard::install(
            &mut round,
            execution,
            plan,
            Arc::clone(&split_delivery)
                as Arc<dyn crate::query_execution::split_assignment::TaskUpdateTransport>,
            data_runtime,
            Arc::new(NotifyWake::new(notify)) as Arc<dyn StatusIntakeWake>,
        )
        .expect("the original failing source installs its actual split pump");
        round.seal_pumps();
        let source = guard.failure_source();
        let mut owner = original_convergence(&round);
        owner.bind_split_failure_source(Some(source.clone()));
        owner.begin(
            &mut round,
            &split_delivery,
            NativeAttemptConvergenceMode::Normal,
        );
        assert_eq!(owner.mode, Some(NativeAttemptConvergenceMode::Normal));
        assert!(
            !round.execution().normal_drain_started(),
            "strict Task terminals are still outstanding"
        );
        assert!(source.failure().is_none());
        assert_eq!(enumerations.load(Ordering::SeqCst), 0);
        assert_eq!(closes.load(Ordering::SeqCst), 0);

        // The same convergence step must actually drive the original source,
        // receive its real enumeration failure, then switch the original owner.
        let error = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match owner.step(
                    &mut round,
                    &split_delivery,
                    NativeAttemptConvergenceMode::Normal,
                    false,
                ) {
                    Err(error) => break error,
                    Ok(NativeAttemptConvergenceStep::Pending { .. }) => {
                        tokio::task::yield_now().await
                    }
                    Ok(NativeAttemptConvergenceStep::Ready) => {
                        panic!("a live source and unfenced strict Tasks cannot be ready")
                    }
                }
            }
        })
        .await
        .expect("the real source must enumerate and report its own bounded failure");
        let TaskExecutionError::SplitAssignment(observed) = &error else {
            panic!("the original source failure must retain the SplitAssignment branch: {error:?}")
        };
        let original = source
            .failure()
            .expect("the original pump retains its selected failure");
        assert!(matches!(
            original,
            SplitAssignmentDriverError::SourceEnumeration { .. }
        ));
        assert_eq!(observed, &original);
        assert_eq!(guard.failure(), Some(original.clone()));
        let original_input = original.execution_failure().unwrap();
        assert_eq!(error.preparation_input(), original_input);
        assert_ne!(
            original_input.fact(),
            QueryFailureReason::SplitAssignmentFailed.failure_fact()
        );
        assert_ne!(
            original_input.fact().reason(),
            QueryFailureReason::BackendLost.failure_fact().reason()
        );
        assert_eq!(
            original_input.detail().as_str(),
            "enumerating the connector split source failed"
        );
        assert!(
            !original_input
                .detail()
                .as_str()
                .contains("opaque provider text")
        );
        assert_eq!(owner.mode, Some(NativeAttemptConvergenceMode::Cleanup));
        assert_eq!(enumerations.load(Ordering::SeqCst), 1);
        assert!(
            !round.pumps_closed(),
            "an observed business failure is not the actual source close"
        );

        // Remote exit remains an independent positive fact from each original
        // covered owner; the split failure supplies no process/exit evidence.
        for context in contexts {
            publish_exit(&mut round, context);
        }
        assert!(owner.observed_contexts(&round).is_some());
        let next = owner
            .step(
                &mut round,
                &split_delivery,
                NativeAttemptConvergenceMode::Normal,
                false,
            )
            .expect("the persistent original failure cannot be re-emitted after Cleanup begins");
        assert!(
            matches!(next, NativeAttemptConvergenceStep::Pending { .. }),
            "remote proof cannot pass the actual source close gate"
        );
        owner.observe_local_close(&mut round).unwrap();
        assert_eq!(owner.mode, Some(NativeAttemptConvergenceMode::Cleanup));
        assert_eq!(
            source.failure(),
            Some(original.clone()),
            "the source cause is retained, not cleared to make progress"
        );

        tokio::time::timeout(Duration::from_secs(5), async {
            while closes.load(Ordering::SeqCst) == 0 {
                assert!(matches!(
                    owner
                        .step(
                            &mut round,
                            &split_delivery,
                            NativeAttemptConvergenceMode::Normal,
                            false
                        )
                        .expect("the next turns must start the original close"),
                    NativeAttemptConvergenceStep::Pending { .. }
                ));
                owner.observe_local_close(&mut round).unwrap();
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the real source must enter its own blocking close");
        assert!(!guard.is_finished());
        assert!(
            !round.pumps_closed(),
            "entry into close is not actual close exit"
        );
        assert_eq!(source.failure(), Some(original.clone()));
        release.open();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let step = owner.step(&mut round, &split_delivery, NativeAttemptConvergenceMode::Normal, false)
                    .expect("Cleanup keeps driving the original close instead of returning the same source failure");
                owner.observe_local_close(&mut round).unwrap();
                assert_eq!(owner.mode, Some(NativeAttemptConvergenceMode::Cleanup));
                assert_eq!(source.failure(), Some(original.clone()));
                if matches!(step, NativeAttemptConvergenceStep::Ready) { break; }
                tokio::task::yield_now().await;
            }
        }).await.expect("the same source-close job must actually exit and join");
        assert_eq!(enumerations.load(Ordering::SeqCst), 1);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert!(guard.is_finished());
        assert!(round.pumps_closed());
        assert!(matches!(
            guard.profile_source().current(),
            SplitAssignmentProfileRead::Failed
        ));
        assert_eq!(
            guard
                .published_close_failure_for_test()
                .unwrap()
                .execution_failure(),
            Some(original_input)
        );
        assert!(
            owner.local_unresolved.is_none(),
            "successful actual close is not an unresolved physical exit"
        );
        assert!(
            round.take_pump_close_error().is_none(),
            "the existing owner consumed any one-shot close diagnostic"
        );
        drop((guard, source, owner, round, split_delivery));
        executors.shutdown().await;
    }
}
