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

use super::*;
use novarocks_state_store_api::InDoubtAdjudicator;

fn evidence_cell(rig: &Rig) -> Arc<Cell> {
    lock(&rig.receiving.cells)
        .iter()
        .rev()
        .find(|cell| cell.original.as_ref().type_id() == vocabulary().evidence_type())
        .expect("actual standalone evidence source")
        .clone()
}
async fn publish_evidence(rig: &Rig, phase: Phase) -> Arc<Cell> {
    let cell = evidence_cell(rig);
    let request = cell.original.requested().expect("same evidence request");
    assert_eq!(request.phase, phase);
    rig.prepare_selected(cell.clone(), request)
        .await
        .publish(&rig.executor)
        .await;
    cell
}

#[tokio::test]
async fn standalone_read_delivers_committed_value_only_after_its_close_footer() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path().join("store.sqlite")).await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(request().deadline);
    let (attempt, observation) = store.attempts.reserve().unwrap();
    let id = attempt.id();
    let mut transaction = rig
        .run(store.begin_original_write_in(attempt, "evidence original", &execution))
        .await
        .unwrap();
    rig.run(transaction.put_in(key(), value(), Precondition::Any))
        .await
        .unwrap();
    let mut commit = Box::pin(transaction.commit_in());
    assert!(
        commit
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let prepared = rig.prepare_next().await;
    assert_eq!(prepared.request.phase, Phase::Commit);
    prepared.publish(&rig.executor).await;
    // The actual commit exited, but its own settlement tail has not run yet.
    // Read that durable evidence through the distinct production start path.
    let evidence = store.evidence.clone();
    let mut read = Box::pin(evidence.adjudicate_in(id, &execution));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let cell = publish_evidence(&rig, Phase::EvidenceRead).await;
    assert!(!cell.original.is_complete());
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    publish_evidence(&rig, Phase::Close).await;
    let actual = read.await.unwrap();
    assert!(matches!(&actual, AttemptOutcome::Committed(_)));
    assert!(cell.original.is_complete());
    assert!(cell.original.local_failure().is_none());
    let CommitOutcome::Committed(receipt) = rig.run(commit).await.unwrap() else {
        panic!("original transaction must retain its committed result");
    };
    assert_eq!(actual, AttemptOutcome::Committed(receipt.clone()));
    assert_eq!(
        observation.peek().unwrap(),
        Some(AttemptOutcome::Committed(receipt))
    );
    drop(transaction);
    rig.drain().await;
    drop(cell);
    rig.stop().await;
}

#[tokio::test]
async fn cancelled_evidence_borrower_retains_same_file_until_worker_close() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("store.sqlite");
    let store = store(path.clone()).await;
    let lifetime = Arc::downgrade(&store.lifetime);
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(request().deadline);
    let (attempt, observation) = store.attempts.reserve().unwrap();
    let id = attempt.id();
    let evidence = store.evidence.clone();
    let mut read = Box::pin(evidence.adjudicate_in(id, &execution));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let cell = evidence_cell(&rig);
    drop(read); // Stable receiving, not this borrowed future, owns the input.
    drop(evidence);
    drop(attempt);
    drop(observation);
    drop(store);
    assert!(lifetime.upgrade().is_some());
    assert!(crate::acquire_owner_lock(&path).is_err());
    publish_evidence(&rig, Phase::EvidenceRead).await;
    assert!(!cell.original.is_complete());
    assert!(lifetime.upgrade().is_some());
    assert!(crate::acquire_owner_lock(&path).is_err());
    let close = rig.prepare_next().await;
    assert_eq!(close.request.phase, Phase::Close);
    close.withdraw();
    assert!(!cell.original.is_complete());
    assert!(cell.original.requested().is_none());
    assert!(lifetime.upgrade().is_some());
    assert!(crate::acquire_owner_lock(&path).is_err());
    cell.original.request_close();
    publish_evidence(&rig, Phase::Close).await;
    assert!(cell.original.is_complete());
    assert!(cell.original.local_failure().is_none());
    assert!(lifetime.upgrade().is_none());
    assert!(crate::acquire_owner_lock(&path).is_ok());
    drop(cell);
    rig.stop().await;
}

#[tokio::test]
async fn foreign_attempt_is_refused_before_evidence_source_registration() {
    let temp = tempfile::tempdir().unwrap();
    let a = store(temp.path().join("a.sqlite")).await;
    let b = store(temp.path().join("b.sqlite")).await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(a.attempts.scope()));
    let execution = rig.context(request().deadline);
    let (foreign, observation) = b.attempts.reserve().unwrap();
    let refused = a
        .evidence
        .adjudicate_in(foreign.id(), &execution)
        .await
        .unwrap_err();
    assert_eq!(
        refused.downcast_ref::<StateStoreError>().unwrap().kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert!(lock(&rig.receiving.cells).is_empty());
    assert_ne!(
        vocabulary().evidence_type(),
        vocabulary().transaction_type()
    );
    assert_ne!(vocabulary().evidence_type(), vocabulary().open_type());
    assert_eq!(a.evidence.original_scope().unwrap(), a.attempts.scope());
    drop(foreign);
    drop(observation);
    rig.stop().await;
}

#[tokio::test]
async fn evidence_physical_panic_keeps_exact_cause_and_original_file() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("store.sqlite");
    let store = store(path.clone()).await;
    let lifetime = Arc::downgrade(&store.lifetime);
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(request().deadline);
    let (attempt, observation) = store.attempts.reserve().unwrap();
    let evidence = store.evidence.clone();
    let mut read = Box::pin(evidence.adjudicate_in(attempt.id(), &execution));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let prepared = rig.prepare_next().await;
    let cell = prepared.cell.clone();
    assert_eq!(prepared.request.phase, Phase::EvidenceRead);
    prepared
        .call
        .panic_after_run
        .store(true, std::sync::atomic::Ordering::Release);
    prepared.publish(&rig.executor).await;
    let raw = read.await.unwrap_err();
    assert_eq!(raw, cell.original.local_failure().unwrap());
    assert_eq!(lock(&cell.health).as_ref().unwrap(), &raw);
    assert!(
        raw.downcast_ref::<novarocks_query_application::cpu::QueryBlockingFailure>()
            .is_some()
    );
    assert!(!cell.original.is_complete());
    assert!(cell.original.requested().is_none());
    drop(evidence);
    drop(attempt);
    drop(observation);
    drop(store);
    assert!(lifetime.upgrade().is_some());
    assert!(crate::acquire_owner_lock(&path).is_err());
    drop(cell);
    // Explicit failed fixture disposition only after the actual worker footer;
    // no positive Source close or production disposal is invented.
    rig.stop_workers_with_retained_health().await;
    assert!(lifetime.upgrade().is_none());
    assert!(crate::acquire_owner_lock(&path).is_ok());
}

#[tokio::test]
async fn ordinary_evidence_release_failure_waits_for_close_without_health() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path().join("store.sqlite")).await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(request().deadline);
    let (attempt, observation) = store.attempts.reserve().unwrap();
    attempt.cancel_before_dispatch().unwrap();
    assert_eq!(
        observation.outcome().await.unwrap(),
        AttemptOutcome::NotCommitted
    );
    store.evidence.fail_next_release();
    let evidence = store.evidence.clone();
    let mut release = Box::pin(evidence.release_evidence_in(attempt.id(), &execution));
    assert!(
        release
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let cell = publish_evidence(&rig, Phase::EvidenceRelease).await;
    assert!(
        release
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(!cell.original.is_complete());
    assert!(cell.original.local_failure().is_none());
    publish_evidence(&rig, Phase::Close).await;
    let failure = release.await.unwrap_err();
    assert_eq!(
        failure.downcast_ref::<StateStoreError>().unwrap().kind(),
        StateStoreErrorKind::ProviderUnavailable
    );
    assert!(cell.original.is_complete());
    assert!(cell.original.local_failure().is_none());
    assert!(lock(&cell.health).is_none());
    drop(cell);
    rig.stop().await;
}
