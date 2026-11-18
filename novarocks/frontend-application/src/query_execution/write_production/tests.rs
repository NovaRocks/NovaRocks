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

//! Real original metadata/decoder/session boundaries, without starting BE writers.
use super::*;
use crate::query_execution::write_result::{RootWriteDecodeContract, RootWriteResultDecoder};
use crate::query_execution::write_session::tests::{
    commit_fragment_bytes, fixture_with_outcome, known_committed, request_context,
};
use novarocks_workload_control::joint_admission::*;
use novarocks_workload_control::{
    ControlStorageConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..200_000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original metadata proof did not finish");
}
fn receive<A: Future + Unpin>(admission: &mut A) -> A::Output {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected original finite metadata reply"),
    }
}
fn original_runtime() -> (WorkloadControl, JointProgressDriver) {
    let storage = ControlStorageConfig::new(8, &[]);
    let config = ResponsibilityConfig {
        work_per_turn: NonZeroUsize::new(7).unwrap(),
    };
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: 8,
        business_limit: 8,
        query_concurrency_limit: 8,
        scope_records_limit: 8,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_joint, driver) = control.configure_responsibility(config).unwrap();
    control.configure_control_storage(storage).unwrap();
    control.mark_ready().unwrap();
    (control, driver)
}
fn bind(scope: &WorkScope, driver: &mut JointProgressDriver) {
    let mut proposal = scope.begin_control_binding().unwrap();
    proposal
        .push_dependency(&novarocks_resource_contract::SharedDependency::LocalHost)
        .unwrap();
    proposal.end_change().unwrap();
    let mut admission = proposal.submit().unwrap();
    drive(driver);
    let mut reply = receive(&mut admission).unwrap();
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(scope)
        .unwrap()
        .notify();
    drop(reply);
    drive(driver);
}
fn allocate_paid_cohorts(
    session: &ConnectorWriteSession,
    scope: &WorkScope,
    driver: &mut JointProgressDriver,
) -> (
    QueryExecutionId,
    PlanSeal,
    UnreceivedProductionReady,
    Option<Arc<[u8]>>,
) {
    let cohorts = TaskManifestBinding::original_writer_occurrences_for_test();
    assert_eq!(
        cohorts.len(),
        2,
        "two actual placements of the checked writer"
    );
    let execution = cohorts[0].task().query_execution_id();
    let plan = cohorts[0].plan();
    let quote = WriteProductionQuote::for_occurrences(cohorts.len())
        .unwrap()
        .unwrap();
    assert_eq!(quote.occurrences(), 2);
    assert_eq!(quote.metadata_bytes(), 2 * COHORT_BYTES);
    let origin: Arc<dyn Send + Sync> = session.decode_origin.clone();
    let mut admission = scope
        .begin_unreceived_productions(Arc::downgrade(&origin), quote.quote)
        .unwrap();
    drive(driver);
    let ready = receive(&mut admission).unwrap();
    // Reverse the exact placements to exercise sorting in the paid storage.
    let facts = Some(encode_occurrences(cohorts.into_iter().rev(), quote).unwrap());
    (execution, plan, ready, facts)
}

fn register_original(
    session: &ConnectorWriteSession,
    scope: &WorkScope,
    driver: &mut JointProgressDriver,
) -> (
    QueryExecutionId,
    PlanSeal,
    UnreceivedProductionOwner,
    Vec<u8>,
) {
    let (execution, plan, mut ready, mut facts) = allocate_paid_cohorts(session, scope, driver);
    let expected = facts.as_ref().unwrap().to_vec();
    let (notification, result) = session
        .register_production_ready_in(
            execution,
            plan,
            &[WriteTargetOrdinal::try_new(0).unwrap()],
            &mut ready,
            &mut facts,
        )
        .unwrap();
    drop((ready, facts));
    notification.notify();
    result.unwrap();
    let owner = session.productions.lock().unwrap().owner.clone().unwrap();
    (execution, plan, owner, expected)
}

fn decoder(
    session: &ConnectorWriteSession,
    execution: QueryExecutionId,
    plan: PlanSeal,
) -> RootWriteResultDecoder {
    let targets = [WriteTargetOrdinal::try_new(0).unwrap()];
    let mut decoder = RootWriteResultDecoder::for_test_local(
        RootWriteDecodeContract::try_new(
            &targets,
            session.targets(),
            novarocks_result_contract::PerKind::empty(),
        )
        .unwrap(),
    );
    decoder.bind_production_execution(execution, plan);
    decoder
}
/// The exact provider registration can verify writer membership before End,
/// independently of the frontend production mutex and the paid grant's phase.
#[test]
fn original_writer_index_is_available_before_first_record_and_without_frontend_lock() {
    let (control, mut driver) = original_runtime();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    bind(&scope, &mut driver);
    let fixture = fixture_with_outcome(2, 1, known_committed());
    let session = &fixture.session;
    let (execution, plan, owner, facts) = register_original(session, &scope, &mut driver);
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let proof = {
        let recorded = fixture.recorded.lock().unwrap();
        let [(query, targets, proof)] = recorded.received_query_registrations.as_slice() else {
            panic!("exactly one provider registration must precede arrivals");
        };
        assert_eq!(
            *query,
            super::super::write_query_identity(execution, plan).unwrap()
        );
        assert_eq!(targets, &[target]);
        Arc::clone(proof.as_ref().unwrap())
    };
    assert_eq!(owner.pending_records().unwrap(), 2);
    assert_eq!(fixture.received_fragment_count(), 0);

    let (done, completed) = std::sync::mpsc::sync_channel(1);
    let lock = session.productions.lock().unwrap();
    let checker = std::thread::spawn(move || {
        let cohorts = TaskManifestBinding::original_writer_occurrences_for_test();
        for cohort in cohorts {
            let mut query = [0; 16];
            query[..8].copy_from_slice(&execution.query_id().high().to_be_bytes());
            query[8..].copy_from_slice(&execution.query_id().low().to_be_bytes());
            let attempt = execution.attempt_id().get();
            let mut key = [0; 16];
            key[..8].copy_from_slice(&cohort.task_key().high().to_be_bytes());
            key[8..].copy_from_slice(&cohort.task_key().low().to_be_bytes());
            let mut foreign_query = query;
            foreign_query[0] ^= 1;
            let mut foreign_key = key;
            foreign_key[0] ^= 1;
            let physical = |query, attempt, task, writer_ordinal| {
                ConnectorWriterPhysicalContext::new(query, attempt, task, 17, writer_ordinal)
            };
            assert!(
                proof
                    .admits(target, &physical(query, attempt, key, 0))
                    .unwrap()
            );
            for other in [
                physical(foreign_query, attempt, key, 0),
                physical(query, attempt + 1, key, 0),
                physical(query, attempt, foreign_key, 0),
                physical(query, attempt, key, 1),
            ] {
                assert!(!proof.admits(target, &other).unwrap());
            }
            assert!(
                !proof
                    .admits(
                        WriteTargetOrdinal::try_new(1).unwrap(),
                        &physical(query, attempt, key, 0),
                    )
                    .unwrap()
            );
        }
        drop(proof);
        let _ = done.send(());
    });
    let completed_under_lock = completed.recv_timeout(std::time::Duration::from_secs(5));
    drop(lock);
    checker.join().unwrap();
    completed_under_lock.expect("writer proof must not require the frontend production lock");

    let mut root = decoder(session, execution, plan);
    let mut decode = session.computation_decode(&[target]).unwrap();
    decode
        .apply_body(
            &mut root,
            &crate::query_execution::write_result::tests::component_write_body(
                commit_fragment_bytes(),
            ),
        )
        .unwrap();
    assert_eq!(fixture.received_fragment_count(), 1);
    assert_eq!(owner.pending_records().unwrap(), 2);
    root.check_relay_end(2).unwrap();
    root.observe_root_eof().unwrap();
    let segment = decode.complete_root(root.finish().unwrap()).unwrap();
    session.receiving_port().accept(segment).unwrap();
    assert_eq!(owner.pending_records().unwrap(), 0);
    assert!(owner.retains_facts(&facts).unwrap());
    session.abort_for_test(request_context()).unwrap();
    assert!(!owner.retains_facts(&facts).unwrap());
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

/// A definite provider registration refusal before dispatch cannot leave an
/// unknown production in the original workload ledger.
#[test]
fn provider_registration_refusal_withdraws_only_its_undispatched_production() {
    let (control, mut driver) = original_runtime();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    bind(&scope, &mut driver);
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let session = &fixture.session;
    fixture.recorded.lock().unwrap().query_registration_failure = Some(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "scripted provider rejected the original query registration",
    ));
    let (execution, plan, mut ready, mut facts) =
        allocate_paid_cohorts(session, &scope, &mut driver);
    let (notice, result) = session
        .register_production_ready_in(
            execution,
            plan,
            &[WriteTargetOrdinal::try_new(0).unwrap()],
            &mut ready,
            &mut facts,
        )
        .unwrap();
    assert!(
        facts.is_none(),
        "the paid cohort reached the original ledger"
    );
    drop(ready);
    notice.notify();
    assert_eq!(result.unwrap_err().kind(), ConnectorErrorKind::CorruptData);
    assert!(session.production_never_registered().unwrap());
    assert!(
        fixture
            .recorded
            .lock()
            .unwrap()
            .received_query_registrations
            .is_empty()
    );
    assert_eq!(
        session.abort_production().unwrap(),
        novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::NotStarted,
    );
    session.abort_for_test(request_context()).unwrap();
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert_eq!(control.snapshot().root_responsibilities, 0);
}

#[test]
fn checked_writer_prefix_without_end_keeps_actual_record_and_refuses_cleanup_complete() {
    for targets in [1, 2] {
        let (control, mut driver) = original_runtime();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        bind(&scope, &mut driver);
        let fixture = fixture_with_outcome(targets, 1, known_committed());
        let session = fixture.session;
        let (execution, plan, owner, facts) = register_original(&session, &scope, &mut driver);
        let mut decoder = decoder(&session, execution, plan);
        decoder
            .apply_relay_body(
                &crate::query_execution::write_result::tests::component_write_body(
                    commit_fragment_bytes(),
                ),
            )
            .unwrap();
        assert!(
            decoder.finish().is_err(),
            "a real prefix cannot manufacture complete End"
        );
        let failure = session
            .prepare_finish_call(request_context())
            .err()
            .expect("a registered writer prefix cannot publish without complete End");
        assert_eq!(failure.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(session.terminal.lock().unwrap().is_none());
        assert_eq!(session.accumulated_row_count().unwrap(), 0);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
        let mut call = session.prepare_abort_call(request_context()).unwrap();
        assert!(
            call.poll_completed(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        assert!(
            call.local_health_failure().is_none(),
            "missing End is retained responsibility, not a local poll failure"
        );
        let mut outcome = None;
        call.take_abort_outcome_in(&mut outcome).unwrap();
        let outcome = outcome.unwrap().unwrap();
        assert!(matches!(
            outcome,
            ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Failed(_)
            }
        ));
        assert_eq!(owner.pending_records().unwrap(), 2);
        assert!(owner.retains_facts(&facts).unwrap());
        assert!(
            session.has_no_accepted_formation().unwrap(),
            "actual abort may retire decoded formation without disposing unknown production"
        );
        assert_eq!(
            session.cleanup_responsibility().unwrap(),
            novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained
        );
        let retired = work.owner.retirement_observer().unwrap();
        drop(session);
        work.business.release();
        work.owner.complete();
        drive(&mut driver);
        assert_eq!(owner.pending_records().unwrap(), 2);
        assert!(owner.retains_facts(&facts).unwrap());
        assert!(!retired.is_reclaimed().unwrap());
    }
}
#[test]
fn complete_actual_end_moves_same_set_then_requires_explicit_publication_disposition() {
    let (control, mut driver) = original_runtime();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    bind(&scope, &mut driver);
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let session = fixture.session;
    let (execution, plan, owner, facts) = register_original(&session, &scope, &mut driver);
    let mut decoder = decoder(&session, execution, plan);
    decoder
        .apply_relay_body(
            &crate::query_execution::write_result::tests::component_write_body(
                commit_fragment_bytes(),
            ),
        )
        .unwrap();
    decoder.check_relay_end(2).unwrap();
    decoder.observe_root_eof().unwrap();
    let prepared = decoder.finish().unwrap();
    let decode = session
        .computation_decode(&[WriteTargetOrdinal::try_new(0).unwrap()])
        .unwrap();
    let interpreted = decode.interpret(prepared).unwrap();
    let accepted = session.receiving_port().accept(interpreted).unwrap();
    assert!(accepted.belongs_to(&session));
    assert_eq!(accepted.row_count(), 3);
    assert_eq!(owner.pending_records().unwrap(), 0);
    assert!(owner.retains_facts(&facts).unwrap());
    let retired = work.owner.retirement_observer().unwrap();
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert!(
        !retired.is_reclaimed().unwrap(),
        "the known publication record still owns its charge"
    );
    // Real original scripted Provider invocation; its verdict is stable here
    // before the borrowed disposition/notification seam is consumed.
    let mut call = session.prepare_finish_call(request_context()).unwrap();
    assert!(owner.retains_facts(&facts).unwrap());
    assert!(!retired.is_reclaimed().unwrap());
    assert!(
        call.poll_needed_ready(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert!(call.needed_ready().is_some());
    assert_eq!(
        call.publication_disposition(),
        Some(novarocks_spi::connector::LakePublicationDisposition::KnownCommitted)
    );
    // The actual Call has already installed the disposition and consumed its
    // original notice. Do not settle or notify the same cohort a second time.
    assert!(
        call.poll_completed(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    drop(call);
    drive(&mut driver);
    assert!(!owner.retains_facts(&facts).unwrap());
    assert!(retired.is_reclaimed().unwrap());
}

#[test]
fn terminal_decision_during_paid_registration_keeps_both_original_sources() {
    let (control, mut driver) = original_runtime();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    bind(&scope, &mut driver);
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let session = fixture.session;
    let (execution, plan, mut ready, mut facts) =
        allocate_paid_cohorts(&session, &scope, &mut driver);
    let expected = facts.as_ref().unwrap().to_vec();
    let _actual_abort = session.abort_for_test(request_context()).unwrap();
    assert!(
        session
            .register_production_ready_in(
                execution,
                plan,
                &[WriteTargetOrdinal::try_new(0).unwrap()],
                &mut ready,
                &mut facts,
            )
            .is_err()
    );
    assert_eq!(facts.as_ref().unwrap().as_ref(), expected);
    assert!(session.productions.lock().unwrap().current.is_none());
    // No writer could have been dispatched: this is a rejected, uninstalled
    // grant, so its original ordinary rollback is still the correct author.
    drop((ready, facts));
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert!(control.snapshot().root_responsibilities == 0);
}

#[test]
fn complete_original_end_transfers_same_union_to_abort_call_and_retires_canonical_rows() {
    let (control, mut driver) = original_runtime();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    bind(&scope, &mut driver);
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let session = fixture.session;
    let (execution, plan, owner, facts) = register_original(&session, &scope, &mut driver);
    let mut decoder = decoder(&session, execution, plan);
    decoder
        .apply_relay_body(
            &crate::query_execution::write_result::tests::component_write_body(
                commit_fragment_bytes(),
            ),
        )
        .unwrap();
    decoder.check_relay_end(2).unwrap();
    decoder.observe_root_eof().unwrap();
    let original = decoder.finish().unwrap();
    let decode = session
        .computation_decode(&[WriteTargetOrdinal::try_new(0).unwrap()])
        .unwrap();
    let interpreted = decode.interpret(original).unwrap();
    let accepted = session.receiving_port().accept(interpreted).unwrap();
    assert!(accepted.belongs_to(&session));
    assert_eq!(owner.pending_records().unwrap(), 0);
    assert!(owner.retains_facts(&facts).unwrap());
    let mut call = session.prepare_abort_call(request_context()).unwrap();
    assert!(
        !session.has_no_accepted_formation().unwrap(),
        "accepted output in the original abort input still owns its complete payload"
    );
    assert_eq!(session.accumulated_row_count().unwrap(), 3);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(call.poll_completed(&mut cx).is_ready());
    let mut result = None;
    call.take_abort_outcome_in(&mut result).unwrap();
    assert!(matches!(
        result,
        Some(Ok(ConnectorWriteAbortOutcome::KnownUncommitted { .. }))
    ));
    let recorded = fixture.recorded.lock().unwrap();
    assert_eq!(
        recorded.abort_production,
        Some(
            novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::CompleteReceived
        )
    );
    assert_eq!(recorded.abort_received_fragments, Some(1));
    assert_eq!(recorded.finish, 0);
    drop(recorded);
    assert!(
        !owner.retains_facts(&facts).unwrap(),
        "the original Call consumed the actual non-Unknown disposition"
    );
    let retired = work.owner.retirement_observer().unwrap();
    drop((call, session, accepted));
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert!(retired.is_reclaimed().unwrap());
}

#[test]
fn cancel_between_precheck_and_claim_hands_same_complete_end_to_original_abort() {
    let (control, mut driver) = original_runtime();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    bind(&scope, &mut driver);
    let fixture = fixture_with_outcome(1, 1, known_committed());
    let session = fixture.session;
    let (execution, plan, owner, facts) = register_original(&session, &scope, &mut driver);
    let mut decoder = decoder(&session, execution, plan);
    decoder
        .apply_relay_body(
            &crate::query_execution::write_result::tests::component_write_body(
                commit_fragment_bytes(),
            ),
        )
        .unwrap();
    decoder.check_relay_end(2).unwrap();
    decoder.observe_root_eof().unwrap();
    let decode = session
        .computation_decode(&[WriteTargetOrdinal::try_new(0).unwrap()])
        .unwrap();
    let accepted = session
        .receiving_port()
        .accept(decode.interpret(decoder.finish().unwrap()).unwrap())
        .unwrap();
    assert!(accepted.belongs_to(&session));
    assert_eq!(session.accumulated_row_count().unwrap(), 3);
    let stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(60),
        stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    assert!(
        !context.is_cancelled(),
        "the earlier business precheck passed"
    );
    stop.request_stop();
    let mut call = None;
    let mut refusal = None;
    session
        .prepare_finish_or_abort_call_in(context, &mut call, &mut refusal)
        .unwrap();
    assert_eq!(
        refusal.as_ref().unwrap().kind(),
        ConnectorErrorKind::Cancelled
    );
    let mut call = call.unwrap();
    assert!(
        call.poll_completed(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    let mut outcome = None;
    call.take_abort_outcome_in(&mut outcome).unwrap();
    assert!(matches!(
        outcome,
        Some(Ok(ConnectorWriteAbortOutcome::KnownUncommitted { .. }))
    ));
    let recorded = fixture.recorded.lock().unwrap();
    assert_eq!(
        recorded.abort_production,
        Some(
            novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::CompleteReceived
        )
    );
    assert_eq!(recorded.abort_received_fragments, Some(1));
    assert_eq!(recorded.finish, 0);
    drop(recorded);
    assert!(!owner.retains_facts(&facts).unwrap());
    let retired = work.owner.retirement_observer().unwrap();
    drop((call, session, accepted, refusal));
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert!(retired.is_reclaimed().unwrap());
}
