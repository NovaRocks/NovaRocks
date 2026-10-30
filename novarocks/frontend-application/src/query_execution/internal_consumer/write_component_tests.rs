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

//! Child of write.rs: production has no test-only seed construction port.

use super::*;
use crate::{
    native::{
        data_runtime::FrontendDataRuntime,
        fragment_transport::{
            DynamicFilterRead, DynamicFilterReadError, FinalTaskInfoRead, TaskResultTransport,
        },
    },
    query_execution::{
        internal_consumer::{
            attempt_handoff::component_tests::OriginalAttempt,
            test_fixture::{internal_write_description, offer_original_credentials},
        },
        internal_result_cpu::InternalResultCpuOwner,
        write_session::tests::{
            commit_fragment_bytes, fixture_with_outcome, known_committed, request_context,
        },
    },
};
use bytes::Bytes;
use novarocks_execution_contract::{
    DomainVersion, OperationOutcome, ResultPacketSequence, SafeDetail, TaskFailure,
    TaskFailurePhase,
    failure::{FailureClass, FailureFact, FailureReason},
    root_result::{RootReadOutcome, RootReadResponse, RootResultData, RootResultReply},
};
use novarocks_query_application::coordination::LogicalConclusion;
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{LogicalExecutionNativePort, LogicalNativeOpenRequest, RetainedRootReply},
    coordination::{LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig},
    execution_error::ExecutionFailureInput,
};
use novarocks_result_contract::RootProfileId;
use novarocks_types::identity::{FrontendProcessId, QueryProcessNamespace};
use novarocks_workload_control::ResultWindowClass;
use std::{
    future::Future,
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    task::Poll,
    time::{Duration, Instant},
};

async fn watchdog<T>(stage: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("Write component watchdog: {stage}"))
}
#[derive(Debug)]
struct ReservationOnly;
impl LogicalExecutionNativePort for ReservationOnly {
    fn bind_in(
        &self,
        _source: &mut Option<LogicalNativeOpenRequest>,
        _target: &mut Option<novarocks_query_application::api::LogicalNativeSession>,
    ) -> Result<(), novarocks_query_application::api::LogicalNativeOpenError> {
        panic!("reserving an identity cannot bind Native work")
    }
}
struct UnavailableInfo;
impl TaskResultTransport for UnavailableInfo {
    fn final_task_info(
        &self,
        _: TaskIdentity,
    ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        Box::pin(async { Ok(FinalTaskInfoRead::Unavailable(OperationOutcome::Gone)) })
    }
    fn dynamic_filters(
        &self,
        _: TaskIdentity,
        _: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        panic!("write consumer must not fetch diagnostic dynamic filters")
    }
}
fn original_failure() -> TaskFailure {
    TaskFailure::new_in_phase(
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(NonZeroU16::new(u16::MAX).unwrap(), NonZeroU16::MIN),
        ),
        SafeDetail::new("original fixed Write Task failure").unwrap(),
        TaskFailurePhase::Execution,
    )
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompletionEvent {
    RootRows,
    RootEnd,
    WriterSuccess,
    SummarySuccess,
    SummaryFailure,
    TaskFailure,
    Cancelled,
    DeadlineExpired,
}

fn causal_orders(events: &[CompletionEvent]) -> Vec<Vec<CompletionEvent>> {
    fn permutations(events: &[CompletionEvent]) -> Vec<Vec<CompletionEvent>> {
        if events.is_empty() {
            return vec![Vec::new()];
        }
        let mut orders = Vec::new();
        for index in 0..events.len() {
            let mut remaining = events.to_vec();
            let event = remaining.remove(index);
            for mut suffix in permutations(&remaining) {
                suffix.insert(0, event);
                orders.push(suffix);
            }
        }
        orders
    }
    permutations(events)
        .into_iter()
        .filter(|order| {
            order
                .iter()
                .position(|event| *event == CompletionEvent::RootRows)
                .unwrap()
                < order
                    .iter()
                    .position(|event| *event == CompletionEvent::RootEnd)
                    .unwrap()
        })
        .collect()
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ControlCut {
    Arrival,
    AfterReady,
}

async fn run_arrival_order(order: &[CompletionEvent], publish: bool, cut: ControlCut) {
    run_arrival_order_with_summary_source(order, publish, cut, false).await;
}

async fn run_arrival_order_with_summary_source(
    order: &[CompletionEvent],
    publish: bool,
    cut: ControlCut,
    independent_summary_source: bool,
) {
    let (mut identity_owner, client) =
        crate::query_execution::logical_execution_test_profile::process(
            tokio::runtime::Handle::current(),
            Arc::new(ReservationOnly),
            QueryProcessNamespace::new(0x9b7),
            FrontendProcessId::new_v7(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::MIN,
                NonZeroUsize::MIN,
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::MIN,
                    NonZeroU32::MIN,
                    Duration::from_secs(1),
                ),
            ),
        );
    let initial = InitialComputationReservation::new(client.reserve_identity().unwrap()).unwrap();
    let credentials = initial.credentials();
    let execution = credentials.execution();
    let context = initial
        .connector_request_context(request_context(), None)
        .unwrap();
    offer_original_credentials(context.vended_credential_lease_sink().unwrap().as_ref());
    // Consume the actual original Round source through its actual one-shot
    // drain. This neither seals TerminalOnly nor constructs a storage table.
    let source = credentials.take_source(execution).unwrap();
    let leases = source.test_into_credential_leases().unwrap();
    assert!(!leases.leases().is_empty());
    let native_storage = leases.into_storage();
    credentials.verify_native_storage(&native_storage).unwrap();
    assert!(credentials.take_source(execution).is_err());
    let (description, plan) = internal_write_description();
    let (mut attempt, resources, activation) =
        watchdog("original admission and Establish", async {
            if independent_summary_source {
                OriginalAttempt::new_bound_with_summary_source(
                    execution,
                    Arc::new(UnavailableInfo),
                    description.clone(),
                    plan,
                    Arc::clone(&native_storage),
                )
                .await
            } else {
                OriginalAttempt::new_bound(
                    execution,
                    Arc::new(UnavailableInfo),
                    description.clone(),
                    plan,
                    true,
                    Some(Arc::clone(&native_storage)),
                    true,
                )
                .await
            }
        })
        .await;
    assert!(Arc::ptr_eq(&attempt.native_credentials, &native_storage));
    let cancellation_requester = attempt.cancellation_requester.take().unwrap();
    let control = order.iter().copied().find(|event| {
        matches!(
            event,
            CompletionEvent::Cancelled | CompletionEvent::DeadlineExpired
        )
    });
    // This is the actual context later moved into the Session factory. An
    // already elapsed absolute deadline tests that factory's existing gate;
    // the separate Root event below tests governed cancellation delivery.
    let publication_stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let publication_deadline = if control == Some(CompletionEvent::DeadlineExpired) {
        Instant::now()
    } else {
        Instant::now() + Duration::from_secs(60)
    };
    let publication_context = novarocks_spi::connector::ConnectorRequestContext::try_new(
        publication_deadline,
        publication_stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let session = Arc::clone(&fixture.session);
    let window = attempt
        .capacity
        .try_acquire(&attempt.scope, ResultWindowClass::Internal)
        .unwrap();
    let capacity_binding =
        QueryResultCapacityBinding::try_new(&attempt.scope, window.retain_alias()).unwrap();
    drop(window);
    let mut cpu_owner = InternalResultCpuOwner::try_new_for_test().unwrap();
    // This child module exercises the closed consumer. The frozen-template
    // from_frozen constructor remains a separately named integration gate.
    let frozen_targets =
        [novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).unwrap()];
    let seed = WriteComputationSeed {
        contract: RootWriteDecodeContract::try_new(&frozen_targets, session.targets()).unwrap(),
        decode: session.computation_decode(&frozen_targets).unwrap(),
        session: Arc::clone(&session),
        initial: Arc::clone(&credentials),
        // This fixture exercises a pure registered-consumer relation without
        // starting Native writers; production from_frozen always binds a source.
        production_execution: None,
    };
    credentials.bind_write_production_session(&session).unwrap();
    session
        .retain_computation_credentials(&credentials)
        .unwrap();
    let (consumer, offers, mut output) = write_computation_consumer(
        &description,
        plan,
        seed,
        DecodedConsumerRuntime {
            computation: None,
            binding: capacity_binding.clone(),
            cpu: cpu_owner.runtime(),
            runtime: FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            cancellation: QueryCancellationView::governed(
                attempt.scope.cancellation().unwrap(),
                None,
            ),
        },
    )
    .unwrap();
    let custody = output.cpu_custody();
    let mut producer = offers.reserve(execution).unwrap();
    producer
        .stage(execution, attempt.root, plan, resources)
        .unwrap();
    attempt.bind_original_drive(&mut producer);
    let body =
        crate::query_execution::write_result::tests::component_write_body(commit_fragment_bytes());
    let backing = body.len() as u64;
    let mut segment = Some(Arc::new(
        RetainedRootReply::try_new(
            RootReadResponse::Reply(RootResultReply {
                root_task: attempt.root,
                profile: RootProfileId::V1,
                kind: RootOutputKind::InternalFacts(InternalResultDomain::PreparedWriteCommitV1),
                accepted_consumed: 0,
                outcome: RootReadOutcome::Data(
                    RootResultData::try_new(
                        RootOutputKind::InternalFacts(InternalResultDomain::PreparedWriteCommitV1),
                        NonZeroU64::MIN,
                        Bytes::from(body),
                        None,
                    )
                    .unwrap(),
                ),
            }),
            capacity_binding.window_alias(),
            backing,
        )
        .unwrap(),
    ));
    let root_status = attempt
        .round
        .execution()
        .task(attempt.root.task_id())
        .unwrap()
        .status()
        .unwrap()
        .clone();
    let end = RootResultEnd {
        sequence: NonZeroU64::new(2).unwrap(),
        output_rows: 2,
    };
    let terminal;
    let mut delivery_refusals = Vec::new();
    {
        let logical = &attempt.logical;
        let consume = logical.run_registered_internal_consumer(Arc::clone(&consumer), activation);
        tokio::pin!(consume);
        let original_binding = Binding {
            activation: logical.native_attempt_drive().identity(),
            root: attempt.root,
            plan,
        };
        // Enumerate input/control arrivals only after this same consumer has
        // actually registered and claimed its original handoff.
        watchdog(
            "original registered consumer waits for its first input",
            std::future::poll_fn(|cx| {
                if let Poll::Ready(result) = consume.as_mut().poll(cx) {
                    panic!("consumer completed before any matrix input: {result:?}");
                }
                let receiving = output
                    .holder
                    .receiving
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                match &*receiving {
                    Receiving::Vacant => Poll::Pending,
                    Receiving::Reserved(binding) => {
                        assert!(
                            *binding == original_binding,
                            "same original activation/root/plan binding"
                        );
                        Poll::Ready(())
                    }
                    _ => panic!("consumer cannot stage or take before any matrix input"),
                }
            }),
        )
        .await;
        let mut result = None;
        let mut expected = None;
        let mut end_received = false;
        let mut writers_succeeded = false;
        let mut summary_succeeded = !independent_summary_source;
        let mut control_requested = false;
        let mut accepted_before_control = None;
        let mut retained_ready = None;
        for event in order {
            match event {
                CompletionEvent::RootRows => {
                    assert!(
                        result.is_none(),
                        "a completed consumer cannot receive a successor body"
                    );
                    let delivery = logical.deliver_original_internal_segment(
                        ResultPacketSequence::new(0),
                        segment.take().unwrap(),
                        2,
                    );
                    tokio::pin!(delivery);
                    let delivered = watchdog(
                        "original segment receipt or first consumer failure",
                        async {
                            tokio::select! { biased;
                                delivered = delivery.as_mut() => Some(delivered),
                                completed = consume.as_mut() => { result = Some(completed); None },
                            }
                        },
                    )
                    .await;
                    let delivered = match delivered {
                        Some(delivered) => delivered,
                        None => {
                            watchdog(
                                "issued segment's original terminal receipt",
                                delivery.as_mut(),
                            )
                            .await
                        }
                    };
                    if let Err(refusal) = delivered {
                        delivery_refusals.push(refusal);
                        break;
                    }
                }
                CompletionEvent::RootEnd => {
                    assert!(
                        result.is_none(),
                        "a completed consumer cannot receive a successor End"
                    );
                    let delivery = logical.deliver_original_internal_end(
                        attempt.root,
                        root_status.clone(),
                        end,
                    );
                    tokio::pin!(delivery);
                    let delivered =
                        watchdog("original End receipt or first consumer failure", async {
                            tokio::select! { biased;
                                delivered = delivery.as_mut() => Some(delivered),
                                completed = consume.as_mut() => { result = Some(completed); None },
                            }
                        })
                        .await;
                    let delivered = match delivered {
                        Some(delivered) => delivered,
                        None => {
                            watchdog("issued End's original terminal receipt", delivery.as_mut())
                                .await
                        }
                    };
                    match delivered {
                        Ok(()) => end_received = true,
                        Err(refusal) => {
                            delivery_refusals.push(refusal);
                            break;
                        }
                    }
                }
                CompletionEvent::WriterSuccess => {
                    let upstream = attempt.pending_upstream.as_ref().unwrap();
                    if independent_summary_source {
                        upstream.publish_original_distinct_writer_success(&mut attempt.round);
                    } else {
                        upstream.publish_original_writer_success(&mut attempt.round);
                    }
                    writers_succeeded = true;
                }
                CompletionEvent::SummarySuccess => {
                    assert!(independent_summary_source);
                    attempt
                        .pending_upstream
                        .as_ref()
                        .unwrap()
                        .publish_original_summary_success(&mut attempt.round);
                    summary_succeeded = true;
                }
                CompletionEvent::SummaryFailure => {
                    assert!(independent_summary_source);
                    let selected = attempt
                        .pending_upstream
                        .as_ref()
                        .unwrap()
                        .publish_original_summary_failure(&mut attempt.round, original_failure());
                    expected = Some(ExecutionFailureInput::from_selected_task(&selected).unwrap());
                }
                CompletionEvent::TaskFailure => {
                    let selected = attempt
                        .pending_upstream
                        .as_ref()
                        .unwrap()
                        .publish_original_failure(&mut attempt.round, original_failure());
                    expected = Some(ExecutionFailureInput::from_selected_task(&selected).unwrap());
                }
                CompletionEvent::Cancelled | CompletionEvent::DeadlineExpired => {
                    if cut == ControlCut::AfterReady {
                        assert!(end_received && writers_succeeded && summary_succeeded);
                        if result.is_none() {
                            result = Some(
                                watchdog(
                                    "actual consumer Ready and local CPU exit before control",
                                    consume.as_mut(),
                                )
                                .await,
                            );
                        }
                        assert!(
                            matches!(result.as_ref(), Some(Ok(()))),
                            "this cut requires the real consumer's successful completion"
                        );
                        let token = logical
                            .original_internal_outcome()
                            .borrow()
                            .clone()
                            .unwrap()
                            .unwrap();
                        assert_eq!(token.end(), end);
                        assert_eq!(token.root(), attempt.root);
                        assert_eq!(token.activation().execution(), execution);
                        assert_eq!(session.accumulated_row_count().unwrap(), 3);
                        assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
                        assert_eq!(fixture.recorded.lock().unwrap().abort, 0);
                        accepted_before_control = Some(token);
                    }
                    let reason = if *event == CompletionEvent::Cancelled {
                        novarocks_workload_control::CancellationReason::Requested
                    } else {
                        novarocks_workload_control::CancellationReason::DeadlineExceeded
                    };
                    // This is the original Root's cancellation author, derived
                    // before its unique WorkOwner moved into the actor.
                    match cancellation_requester.request_with_outcome(reason.clone()).unwrap() {
                        novarocks_workload_control::WorkCancellationRequestOutcome::Requested => {
                            assert_eq!(attempt.scope.cancellation().unwrap().reason(), Some(reason));
                        }
                        novarocks_workload_control::WorkCancellationRequestOutcome::AlreadyRequested(original) => {
                            assert_eq!(original, reason);
                        }
                        novarocks_workload_control::WorkCancellationRequestOutcome::SuccessSealed => {
                            assert!(matches!(*logical.original_internal_outcome().borrow(), Some(Ok(_))),
                                "a sealed computation keeps its accepted Ready");
                        }
                    }
                    if *event == CompletionEvent::Cancelled {
                        publication_stop.request_stop();
                    }
                    control_requested = true;
                }
            }
            if result.is_none() {
                result = std::future::poll_fn(|cx| {
                    Poll::Ready(match consume.as_mut().poll(cx) {
                        Poll::Pending => None,
                        Poll::Ready(completed) => Some(completed),
                    })
                })
                .await;
            }
            if matches!(result.as_ref(), Some(Err(_))) {
                // A real first failure can reject the remaining input suffix.
                // No successful delivery receipt is manufactured for it.
                break;
            }
            if expected.is_none()
                && !control_requested
                && (!end_received || !writers_succeeded || !summary_succeeded)
            {
                assert!(
                    result.is_none(),
                    "Ready requires complete Root and every original writer/source terminal: {order:?}"
                );
                assert!(logical.original_internal_outcome().borrow().is_none());
                assert_eq!(
                    session.accumulated_row_count().unwrap(),
                    0,
                    "EOF decoded the original generation; union needs both writer and summary-source proof"
                );
                assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
            }
        }
        let result = match result {
            Some(result) => result,
            None => {
                watchdog(
                    "original consumer result and CPU actual exit",
                    consume.as_mut(),
                )
                .await
            }
        };
        match (expected, result) {
            (Some(expected), result) => {
                let error = result.expect_err("an original failed Task cannot reach Ready");
                assert_eq!(error.execution_failure(), Some(&expected));
                terminal = Some(
                novarocks_query_application::api::NativeAttemptTerminal::Failed(
                    novarocks_query_application::api::NativeAttemptPreparationFailure::from_input(
                        expected,
                    ),
                ),
            );
                assert_eq!(session.accumulated_row_count().unwrap(), 0);
                assert_eq!(
                    logical.actor_snapshot().await.unwrap().conclusion,
                    Some(LogicalConclusion::Failed)
                );
                assert!(!matches!(
                    *logical.original_internal_outcome().borrow(),
                    Some(Ok(_))
                ));
                assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
            }
            (None, Err(error)) => {
                let expected_kind = match control.expect("only original control may stop this case")
                {
                    CompletionEvent::Cancelled => {
                        novarocks_query_application::api::QueryExecutionErrorKind::Cancelled
                    }
                    CompletionEvent::DeadlineExpired => {
                        novarocks_query_application::api::QueryExecutionErrorKind::DeadlineExceeded
                    }
                    _ => unreachable!(),
                };
                assert_eq!(
                    error.kind(),
                    expected_kind,
                    "order={order:?}; error={error:?}; end_received={end_received}; writers_succeeded={writers_succeeded}; control_requested={control_requested}; root_control={:?}; actor_outcome={:?}",
                    attempt.scope.cancellation().map(|view| view.reason()),
                    *logical.original_internal_outcome().borrow()
                );
                assert!(!matches!(
                    *logical.original_internal_outcome().borrow(),
                    Some(Ok(_))
                ));
                assert_eq!(session.accumulated_row_count().unwrap(), 0);
                assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
                // Cleanup is real, without inventing a Native/Provider failure fact.
                terminal = None;
            }
            (None, Ok(())) => {
                let token = logical
                    .original_internal_outcome()
                    .borrow()
                    .clone()
                    .unwrap()
                    .unwrap();
                let ready = output.take_ready(token).unwrap();
                assert_eq!(token.end(), end);
                assert_eq!(token.root(), attempt.root);
                assert_eq!(token.activation().execution(), execution);
                if let Some(original) = accepted_before_control {
                    assert_eq!(token, original, "control cannot retract accepted Ready");
                }
                assert_eq!(ready.accepted.row_count(), 3);
                assert!(!ready.accepted.is_empty());
                assert!(ready.accepted.belongs_to(&session));
                assert!(Arc::ptr_eq(&ready.session, &session));
                assert!(Arc::ptr_eq(&ready._credentials, &native_storage));
                assert_eq!(session.accumulated_row_count().unwrap(), 3);
                assert!(output.take_ready(token).is_err());
                assert_eq!(
                    fixture.recorded.lock().unwrap().finish,
                    0,
                    "Ready is stable union custody, not Provider publication"
                );
                assert_eq!(fixture.recorded.lock().unwrap().abort, 0);
                // Domain publication control does not retract successful Native computation.
                terminal = Some(novarocks_query_application::api::NativeAttemptTerminal::Completed);
                if cut == ControlCut::AfterReady {
                    retained_ready = Some(ready);
                } else {
                    drop(ready);
                }
            }
        }
        if !delivery_refusals.is_empty() {
            assert!(
                control.is_some()
                    || order.iter().any(|event| matches!(
                        event,
                        CompletionEvent::TaskFailure | CompletionEvent::SummaryFailure
                    )),
                "an otherwise valid completion cannot lose its original delivery"
            );
        }
        if let Some(control) = control {
            // Ready may already be accepted. The domain veto belongs to the
            // actual Session context before its publication claim, not to an
            // invented actor rollback or a second completion reducer.
            let mut call = None;
            let mut refusal = None;
            session
                .prepare_finish_or_abort_call_in(publication_context, &mut call, &mut refusal)
                .unwrap();
            let expected_kind = if control == CompletionEvent::Cancelled {
                novarocks_spi::connector::ConnectorErrorKind::Cancelled
            } else {
                novarocks_spi::connector::ConnectorErrorKind::DeadlineExceeded
            };
            assert_eq!(refusal.as_ref().unwrap().kind(), expected_kind);
            if let Some(original) = accepted_before_control {
                let ready = retained_ready.as_ref().unwrap();
                assert_eq!(
                    logical
                        .original_internal_outcome()
                        .borrow()
                        .clone()
                        .unwrap()
                        .unwrap(),
                    original
                );
                assert_eq!(session.accumulated_row_count().unwrap(), 3);
                assert_eq!(ready.accepted.row_count(), 3);
                assert!(ready.accepted.belongs_to(&session));
                assert!(Arc::ptr_eq(&ready.session, &session));
                assert!(Arc::ptr_eq(&ready._credentials, &native_storage));
            }
            let mut call = call.unwrap();
            watchdog(
                "same original abort actual completion",
                std::future::poll_fn(|cx| call.poll_completed(cx)),
            )
            .await;
            let mut outcome = None;
            call.take_abort_outcome_in(&mut outcome).unwrap();
            assert!(matches!(
                outcome,
                Some(Ok(
                    novarocks_spi::connector::ConnectorWriteAbortOutcome::KnownUncommitted { .. }
                ))
            ));
            assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
            assert_eq!(fixture.recorded.lock().unwrap().abort, 1);
            if let Some(original) = accepted_before_control {
                let ready = retained_ready.as_ref().unwrap();
                assert_eq!(
                    logical
                        .original_internal_outcome()
                        .borrow()
                        .clone()
                        .unwrap()
                        .unwrap(),
                    original
                );
                assert_eq!(session.accumulated_row_count().unwrap(), 3);
                assert_eq!(
                    fixture.recorded.lock().unwrap().abort_production,
                    Some(novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::Unproven)
                );
                assert_eq!(fixture.recorded.lock().unwrap().abort_received_rows, None);
                assert_eq!(
                    fixture.recorded.lock().unwrap().abort_received_fragments,
                    None
                );
                assert_eq!(ready.accepted.row_count(), 3);
                assert!(Arc::ptr_eq(&ready._credentials, &native_storage));
            }
        } else if publish
            && !order.iter().any(|event| {
                matches!(
                    event,
                    CompletionEvent::TaskFailure | CompletionEvent::SummaryFailure
                )
            })
        {
            let mut call = session.prepare_finish_call(publication_context).unwrap();
            watchdog(
                "same Ready union original publication Call",
                std::future::poll_fn(|cx| call.poll_completed(cx)),
            )
            .await;
            let mut outcome = None;
            call.take_outcome_in(&mut outcome).unwrap();
            assert!(matches!(outcome, Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { .. }))));
            assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        }
        // The same accepted holder spans the actual refusal/abort Call, then
        // releases before the original CPU/Native/window last-exit checks.
        drop(retained_ready.take());
    }
    drop(segment.take());
    let tail = attempt.pending_upstream.take().unwrap();
    tail.begin_original_convergence(&mut attempt.round, terminal.as_ref());
    attempt.release = if terminal.is_none() {
        let reason = match control.expect("only original control has no Native terminal") {
            CompletionEvent::Cancelled => novarocks_workload_control::CancellationReason::Requested,
            CompletionEvent::DeadlineExpired => {
                novarocks_workload_control::CancellationReason::DeadlineExceeded
            }
            _ => unreachable!(),
        };
        tail.complete_for_original_control(
            &mut attempt.round,
            &attempt.acks,
            &attempt.scope,
            reason,
        )
    } else {
        tail.complete(&mut attempt.round, &attempt.acks)
    };
    watchdog("original CPU custody actual exit", custody.join_closed())
        .await
        .unwrap();
    drop((
        custody,
        output,
        consumer,
        producer,
        offers,
        capacity_binding,
        session,
        fixture,
    ));
    assert_eq!(attempt.capacity.snapshot().held_positions, [0; 4]);
    drop(cancellation_requester);
    watchdog("original Task/Native/QA owners drain", attempt.finish()).await;
    drop(delivery_refusals);
    drop((context, native_storage, credentials, initial));
    watchdog(
        "original finite CPU executor actual exit",
        cpu_owner.shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
    watchdog(
        "reservation-only QA owner actual exit",
        identity_owner.shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
}
async fn run_case(fail_writer: bool) {
    run_arrival_order(
        &[
            CompletionEvent::RootRows,
            CompletionEvent::RootEnd,
            if fail_writer {
                CompletionEvent::TaskFailure
            } else {
                CompletionEvent::WriterSuccess
            },
        ],
        false,
        ControlCut::Arrival,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_write_eof_then_original_writer_proof_then_second_cpu_union_reaches_ready() {
    run_case(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn fixed_write_original_te_task_failure_after_end_keeps_exact_cause_and_no_union() {
    run_case(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn independent_summary_terminal_and_original_writer_accept_every_causal_arrival_order() {
    let orders = causal_orders(&[
        CompletionEvent::RootRows,
        CompletionEvent::RootEnd,
        CompletionEvent::WriterSuccess,
        CompletionEvent::SummarySuccess,
    ]);
    assert_eq!(orders.len(), 12);
    for order in orders {
        run_arrival_order_with_summary_source(&order, true, ControlCut::Arrival, true).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn summary_source_failure_after_eof_and_writer_success_keeps_original_cause_and_no_union() {
    run_arrival_order_with_summary_source(
        &[
            CompletionEvent::RootRows,
            CompletionEvent::RootEnd,
            CompletionEvent::WriterSuccess,
            CompletionEvent::SummaryFailure,
        ],
        false,
        ControlCut::Arrival,
        true,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_control_after_eof_and_writer_success_vetoes_before_summary_session_union() {
    for control in [CompletionEvent::Cancelled, CompletionEvent::DeadlineExpired] {
        run_arrival_order_with_summary_source(
            &[
                CompletionEvent::RootRows,
                CompletionEvent::RootEnd,
                CompletionEvent::WriterSuccess,
                control,
            ],
            true,
            ControlCut::Arrival,
            true,
        )
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn exact_root_rows_eof_and_all_success_accept_every_causal_arrival_order() {
    let orders = causal_orders(&[
        CompletionEvent::RootRows,
        CompletionEvent::RootEnd,
        CompletionEvent::WriterSuccess,
    ]);
    assert_eq!(orders.len(), 3);
    for order in orders {
        run_arrival_order(&order, true, ControlCut::Arrival).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task_failure_cancel_and_deadline_veto_every_causal_arrival_order() {
    let cases: &[(&[CompletionEvent], usize)] = &[
        (
            &[
                CompletionEvent::RootRows,
                CompletionEvent::RootEnd,
                CompletionEvent::TaskFailure,
            ],
            3,
        ),
        (
            &[
                CompletionEvent::RootRows,
                CompletionEvent::RootEnd,
                CompletionEvent::WriterSuccess,
                CompletionEvent::Cancelled,
            ],
            12,
        ),
        (
            &[
                CompletionEvent::RootRows,
                CompletionEvent::RootEnd,
                CompletionEvent::WriterSuccess,
                CompletionEvent::DeadlineExpired,
            ],
            12,
        ),
    ];
    for (events, count) in cases {
        let orders = causal_orders(events);
        assert_eq!(orders.len(), *count);
        for order in orders {
            run_arrival_order(&order, true, ControlCut::Arrival).await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_ready_then_cancel_vetoes_only_the_original_publication_claim() {
    run_arrival_order(
        &[
            CompletionEvent::RootRows,
            CompletionEvent::RootEnd,
            CompletionEvent::WriterSuccess,
            CompletionEvent::Cancelled,
        ],
        true,
        ControlCut::AfterReady,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_ready_then_expired_context_vetoes_only_the_original_publication_claim() {
    run_arrival_order(
        &[
            CompletionEvent::RootRows,
            CompletionEvent::RootEnd,
            CompletionEvent::WriterSuccess,
            CompletionEvent::DeadlineExpired,
        ],
        true,
        ControlCut::AfterReady,
    )
    .await;
}

#[tokio::test]
async fn distinct_stage_credentials_keep_one_write_union_through_terminal_context_exit() {
    use crate::query_execution::write_session::tests::{commit_unknown, fragment_bytes};
    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose,
        CatalogProperties, ConnectorStorageResolver, CredentialConsumerRole, StorageAccessRequest,
        StorageCredentialScopePrefix, VendedS3CredentialLeaseContribution,
        VendedS3CredentialLeaseEntry,
    };
    for unknown in [false, true] {
        let (mut identity_owner, client) =
            crate::query_execution::logical_execution_test_profile::process(
                tokio::runtime::Handle::current(),
                Arc::new(ReservationOnly),
                QueryProcessNamespace::new(0x9b8),
                FrontendProcessId::new_v7(),
                LogicalExecutionSupervisorConfig::new(
                    NonZeroUsize::new(8).unwrap(),
                    NonZeroUsize::MIN,
                    NonZeroUsize::MIN,
                    LogicalExecutionRowsConfig::new(
                        NonZeroUsize::MIN,
                        NonZeroU32::MIN,
                        Duration::from_secs(1),
                    ),
                ),
            );
        let fixture = fixture_with_outcome(
            1,
            16,
            if unknown {
                commit_unknown()
            } else {
                known_committed()
            },
        );
        let session = Arc::clone(&fixture.session);
        let properties = CatalogProperties::new(
            session.catalog_properties().handle().clone(),
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg").unwrap(),
            1,
            vec![],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                    CatalogCredentialMode::Vended,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let request = StorageAccessRequest::try_new(
            properties.handle().clone(),
            "s3://warehouse/db/t/metadata/v2.metadata.json",
        )
        .unwrap();
        let target = novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).unwrap();
        let mut originals = Vec::new();
        let mut selected_lease = None;
        for stage in 0..2 {
            let initial =
                InitialComputationReservation::new(client.reserve_identity().unwrap()).unwrap();
            let credentials = initial.credentials();
            let context = initial
                .connector_request_context(request_context(), None)
                .unwrap();
            context
                .vended_credential_lease_sink()
                .unwrap()
                .offer_vended_s3_credential_lease(
                    &properties,
                    VendedS3CredentialLeaseContribution::try_new(
                        vec![
                            VendedS3CredentialLeaseEntry::try_new(
                                StorageCredentialScopePrefix::try_from_normalized(
                                    "s3://warehouse/db/t",
                                )
                                .unwrap(),
                                u64::MAX,
                                SecretValue::new("stage-access"),
                                SecretValue::new("stage-secret"),
                                SecretValue::new("stage-token"),
                            )
                            .unwrap(),
                        ],
                        None,
                    )
                    .unwrap(),
                )
                .unwrap();
            credentials.bind_write_production_session(&session).unwrap();
            session
                .retain_computation_credentials(&credentials)
                .unwrap();
            session
                .retain_computation_credentials(&credentials)
                .unwrap();
            // The original first-party relation cannot be overwritten by a
            // different Session before or after typed registration.
            let foreign = fixture_with_outcome(1, 16, known_committed());
            assert!(
                credentials
                    .bind_write_production_session(&foreign.session)
                    .is_err()
            );
            assert!(
                foreign
                    .session
                    .retain_computation_credentials(&credentials)
                    .is_err()
            );
            assert!(
                foreign
                    .session
                    .following_terminal_context(request_context())
                    .unwrap()
                    .storage_resolver()
                    .is_none()
            );
            assert!(
                credentials
                    .write_production_session()
                    .unwrap()
                    .ptr_eq(&Arc::downgrade(&session))
            );
            let terminal = credentials.terminal_storage_resolver();
            selected_lease = Some(terminal.resolve_vended_s3(&request).unwrap().lease_id());
            originals.push(Arc::downgrade(&terminal));
            let source = credentials.take_source(credentials.execution()).unwrap();
            let storage = source.test_into_credential_leases().unwrap().into_storage();
            credentials.verify_native_storage(&storage).unwrap();
            // Real Session decode/accept/union, with unique canonical fragments.
            // This finite relation fixture does not claim Native execution.
            let decoded = session
                .computation_decode(&[target])
                .unwrap()
                .interpret(
                    crate::query_execution::write_result::DecodedPreparedWriteSet::for_test(
                        3 + stage,
                        vec![(
                            target,
                            fragment_bytes(&format!(
                                "s3://warehouse/db/t/data/stage-{stage}.parquet"
                            )),
                        )],
                    ),
                )
                .unwrap();
            session.receiving_port().accept(decoded).unwrap();
            drop((storage, terminal, credentials, initial, context));
            assert!(
                originals
                    .iter()
                    .all(|original| original.upgrade().is_some())
            );
        }
        assert_eq!(session.accumulated_row_count().unwrap(), 7);
        let following = session
            .following_terminal_context(request_context())
            .unwrap();
        assert_eq!(
            following
                .storage_resolver()
                .unwrap()
                .resolve_vended_s3(&request)
                .unwrap()
                .lease_id(),
            selected_lease.unwrap()
        );
        let completion = session.publish_for_test(request_context()).unwrap();
        assert_eq!(matches!(&completion, novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { .. }), unknown);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        assert_eq!(fixture.recorded.lock().unwrap().finish_rows, 7);
        assert_eq!(
            fixture.recorded.lock().unwrap().terminal_storage,
            Some(Ok("s3://warehouse/db/t".into()))
        );
        assert!(
            originals
                .iter()
                .all(|original| original.upgrade().is_some())
        );
        drop((completion, session, fixture));
        assert!(
            originals
                .iter()
                .all(|original| original.upgrade().is_some()),
            "the following Context is the actual last router holder"
        );
        assert_eq!(
            following
                .storage_resolver()
                .unwrap()
                .resolve_vended_s3(&request)
                .unwrap()
                .lease_id(),
            selected_lease.unwrap()
        );
        drop(following);
        assert!(
            originals
                .iter()
                .all(|original| original.upgrade().is_none())
        );
        watchdog(
            "two-reservation QA owner actual exit",
            identity_owner.shutdown_until(Instant::now() + Duration::from_secs(5)),
        )
        .await
        .unwrap();
    }
}
