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

#![cfg(all(feature = "foundationdb-provider", feature = "state-store-test-hooks"))]

use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use foundationdb::options::{StreamingMode, TransactionOption};
use foundationdb::{Database, KeySelector, RangeOption};
use novarocks_state_store_api::{
    AttemptOutcome, CommitObservation, CommitOutcome, Direction, Key, KeyRange, Precondition,
    RangeRequest, StateStore, StateStoreErrorKind, Value, WriteTransaction,
};
use novarocks_state_store_foundationdb::{
    FoundationDbClientConfig, FoundationDbCommitGateControl, FoundationDbProviderTestHarness,
    FoundationDbTestLimitOverrides, FoundationDbTestProviderConfig, FoundationDbTestStoreConfig,
    arm_next_foundationdb_commit,
};
use uuid::Uuid;

use novarocks_state_store_testkit::conformance::{
    self as state_store_conformance, FaultStateStoreFactory, PostDispatchControl,
    PostDispatchController, PostDispatchScenario, StateStoreFactory, StateStoreFaultFixture,
};

fn client_config() -> FoundationDbClientConfig {
    FoundationDbClientConfig {
        disable_multi_version_client: true,
        tls_cert_path: None,
        tls_key_path: None,
        tls_ca_path: None,
        tls_verify_peers: None,
        tls_password: None,
    }
}

fn cluster_file() -> PathBuf {
    PathBuf::from(
        std::env::var("NOVAROCKS_FDB_CLUSTER_FILE").expect("FoundationDB fixture cluster file"),
    )
}

fn store_config(cluster_id: &str, keyspace_id: Uuid) -> FoundationDbTestStoreConfig {
    FoundationDbTestStoreConfig {
        cluster_id: cluster_id.to_owned(),
        limits: FoundationDbTestLimitOverrides::default(),
        provider: FoundationDbTestProviderConfig::Foundationdb {
            cluster_file: cluster_file(),
            keyspace_id,
        },
    }
}

fn transaction_store_config(cluster_id: &str, keyspace_id: Uuid) -> FoundationDbTestStoreConfig {
    let mut config = store_config(cluster_id, keyspace_id);
    config.limits.max_transaction_bytes = Some(16 * 1024);
    config
}

fn test_deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

fn raw_database() -> Database {
    Database::from_path(
        cluster_file()
            .to_str()
            .expect("UTF-8 FoundationDB cluster file"),
    )
    .expect("open raw FoundationDB inspection handle")
}

fn raw_transaction(database: &Database) -> foundationdb::Transaction {
    let transaction = database.create_trx().expect("raw inspection transaction");
    transaction
        .set_option(TransactionOption::Timeout(4_000))
        .expect("raw inspection timeout");
    transaction
        .set_option(TransactionOption::RetryLimit(0))
        .expect("raw inspection retry limit");
    transaction
}

fn keyspace_root(keyspace_id: Uuid) -> Vec<u8> {
    [b"NRSS\x01".as_slice(), keyspace_id.as_bytes()].concat()
}

/// Counts the commit-state keys a keyspace currently holds.
///
/// The subspace is addressed without knowing any instance's private tag, which
/// is the point: a test can tell how much evidence a keyspace is carrying
/// without being able to forge, or even name, one attempt's key.
async fn commit_state_key_count(keyspace_id: Uuid) -> usize {
    let database = raw_database();
    let transaction = raw_transaction(&database);
    let root = keyspace_root(keyspace_id);
    let start = [root.as_slice(), &[0x03]].concat();
    let end = [root.as_slice(), &[0x04]].concat();
    let values = transaction
        .get_range(
            &RangeOption {
                begin: KeySelector::first_greater_or_equal(start),
                end: KeySelector::first_greater_or_equal(end),
                mode: StreamingMode::WantAll,
                ..RangeOption::default()
            },
            1,
            false,
        )
        .await
        .expect("read the commit-state subspace");
    values.len()
}

/// Waits for a keyspace's evidence to settle at `expected` keys.
///
/// Evidence is released by the commit owner, not by the caller it answered, so
/// a caller that has already been told "committed" may still be a moment ahead
/// of the release. Polling states the invariant without pretending the two are
/// synchronous.
async fn await_commit_state_key_count(keyspace_id: Uuid, expected: usize) {
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count = commit_state_key_count(keyspace_id).await;
            if count == expected {
                return count;
            }
            // Every poll is a real FoundationDB transaction, so this backs off
            // rather than spinning one round trip per scheduler tick.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        observed.is_ok(),
        "commit-state evidence never settled at {expected} keys; last count was {}",
        commit_state_key_count(keyspace_id).await
    );
}

async fn write_partial_identity(keyspace_id: Uuid) {
    let database = raw_database();
    let transaction = raw_transaction(&database);
    let schema_key = [keyspace_root(keyspace_id).as_slice(), &[0x00, 0x00]].concat();
    transaction.set(&schema_key, &[2]);
    transaction
        .commit()
        .await
        .expect("persist partial identity corruption");
}

/// Writes a complete identity in the retired change-feed schema.
///
/// FoundationDB has no DDL that could migrate such a keyspace, so opening it
/// has to fail rather than reinterpret its rows. Nothing here resets it either:
/// an operator's data is never rewritten by a version check.
async fn write_version_one_identity(keyspace_id: Uuid, cluster_id: &str) {
    let database = raw_database();
    let transaction = raw_transaction(&database);
    let root = keyspace_root(keyspace_id);
    transaction.set(&[root.as_slice(), &[0x00, 0x00]].concat(), &[1]);
    transaction.set(
        &[root.as_slice(), &[0x00, 0x01]].concat(),
        cluster_id.as_bytes(),
    );
    transaction.set(
        &[root.as_slice(), &[0x00, 0x02]].concat(),
        Uuid::new_v4().as_bytes(),
    );
    transaction.set(
        &[root.as_slice(), &[0x00, 0x03]].concat(),
        &1_u64.to_be_bytes(),
    );
    transaction
        .commit()
        .await
        .expect("persist a version-one keyspace");
}

fn key(bytes: impl Into<Bytes>) -> Key {
    Key::try_from(bytes.into()).expect("valid test key")
}

fn value(bytes: impl Into<Bytes>) -> Value {
    Value::try_from(bytes.into()).expect("valid test value")
}

fn range(
    start: &'static [u8],
    end: &'static [u8],
    direction: Direction,
    page_size: usize,
) -> RangeRequest {
    RangeRequest {
        range: KeyRange::new(key(Bytes::from_static(start)), key(Bytes::from_static(end)))
            .expect("valid range"),
        direction,
        page_size,
        continuation: None,
    }
}

/// Reserves one attempt and begins the write it authorises.
async fn begin(
    store: &Arc<dyn StateStore>,
    purpose: &str,
) -> (Box<dyn WriteTransaction>, CommitObservation) {
    let (attempt, observation) = store.attempts().reserve().expect("reserve a write attempt");
    let transaction = store
        .begin_write(attempt, purpose)
        .await
        .expect("begin a write transaction");
    assert_eq!(transaction.attempt(), observation.id());
    (transaction, observation)
}

fn assert_committed(outcome: CommitOutcome) {
    assert!(
        matches!(outcome, CommitOutcome::Committed(_)),
        "{outcome:?}"
    );
}

async fn seed(store: &Arc<dyn StateStore>, records: &[(&'static [u8], &'static [u8])]) {
    let (mut transaction, observation) = begin(store, "seed").await;
    for (item, payload) in records {
        transaction
            .put(
                key(Bytes::from_static(item)),
                value(Bytes::from_static(payload)),
                Precondition::Any,
            )
            .await
            .expect("stage seed record");
    }
    assert_committed(transaction.commit().await);
    assert_eq!(
        observation.outcome().await.expect("seed verdict"),
        observation.peek().expect("seed peek").expect("published"),
    );
}

async fn transaction_scenarios(harness: &FoundationDbProviderTestHarness) {
    let keyspace_id = Uuid::new_v4();
    let store = harness
        .open_store(
            transaction_store_config("transaction-cluster", keyspace_id),
            test_deadline(),
        )
        .await
        .expect("open transaction keyspace");

    let binary_key = key(Bytes::from_static(&[0x00, 0xff, 0x10]));
    let binary_value = value(Bytes::from_static(&[0xff, 0x00, 0x20]));
    let (mut ordered, _ordered_observation) = begin(&store, "ordered-overlay").await;
    ordered
        .put(binary_key.clone(), binary_value, Precondition::Absent)
        .await
        .expect("put absent");
    let first = ordered
        .get(&binary_key)
        .await
        .expect("overlay get")
        .expect("overlay record");
    assert!(
        first
            .version
            .as_bytes()
            .starts_with(b"fdb-provisional-v1\0")
    );
    ordered
        .delete(binary_key.clone(), Precondition::Version(first.version))
        .await
        .expect("delete provisional version");
    ordered
        .put(
            binary_key.clone(),
            value(Bytes::from_static(b"final")),
            Precondition::Absent,
        )
        .await
        .expect("put after overlay delete");
    assert_committed(ordered.commit().await);
    let mut repeatable = store.begin_read().await.expect("begin repeatable read");
    let before = repeatable.get(&binary_key).await.expect("first read");
    seed(&store, &[(&[0x00, 0xff, 0x10], b"changed")]).await;
    assert_eq!(
        repeatable.get(&binary_key).await.expect("second read"),
        before
    );
    repeatable.abort().await.expect("abort repeatable read");

    let conflict_key = key(Bytes::from_static(b"same-key"));
    seed(&store, &[(b"same-key", b"base")]).await;
    let (mut left, _left_observation) = begin(&store, "same-left").await;
    let (mut right, right_observation) = begin(&store, "same-right").await;
    left.get(&conflict_key).await.expect("left read");
    right.get(&conflict_key).await.expect("right read");
    left.put(
        conflict_key.clone(),
        value(Bytes::from_static(b"left")),
        Precondition::Present,
    )
    .await
    .expect("left put");
    right
        .put(
            conflict_key,
            value(Bytes::from_static(b"right")),
            Precondition::Present,
        )
        .await
        .expect("right put");
    assert_committed(left.commit().await);
    assert!(matches!(right.commit().await, CommitOutcome::Conflict(_)));
    // A conflict is FoundationDB's own statement that the write did not land,
    // so it is published as a proven denial rather than left in doubt.
    assert_eq!(
        right_observation.peek().expect("conflict peek"),
        Some(AttemptOutcome::NotCommitted)
    );

    seed(&store, &[(b"skew-a", b"1"), (b"skew-b", b"1")]).await;
    let (mut skew_left, _skew_left_observation) = begin(&store, "skew-left").await;
    let (mut skew_right, _skew_right_observation) = begin(&store, "skew-right").await;
    skew_left
        .get(&key(Bytes::from_static(b"skew-a")))
        .await
        .expect("read skew a");
    skew_right
        .get(&key(Bytes::from_static(b"skew-b")))
        .await
        .expect("read skew b");
    skew_left
        .put(
            key(Bytes::from_static(b"skew-b")),
            value(Bytes::from_static(b"0")),
            Precondition::Any,
        )
        .await
        .expect("write skew b");
    skew_right
        .put(
            key(Bytes::from_static(b"skew-a")),
            value(Bytes::from_static(b"0")),
            Precondition::Any,
        )
        .await
        .expect("write skew a");
    assert_committed(skew_left.commit().await);
    assert!(matches!(
        skew_right.commit().await,
        CommitOutcome::Conflict(_)
    ));

    let (mut phantom, _phantom_observation) = begin(&store, "phantom-reader").await;
    phantom
        .range(&range(b"phantom-", b"phantom.", Direction::Forward, 2))
        .await
        .expect("read empty phantom range");
    seed(&store, &[(b"phantom-key", b"inserted")]).await;
    phantom
        .put(
            key(Bytes::from_static(b"phantom-outcome")),
            value(Bytes::from_static(b"value")),
            Precondition::Any,
        )
        .await
        .expect("stage phantom outcome");
    assert!(matches!(phantom.commit().await, CommitOutcome::Conflict(_)));

    seed(
        &store,
        &[
            (b"page-0", b"0"),
            (b"page-1", b"1"),
            (b"page-2", b"2"),
            (b"page-3", b"3"),
            (b"page-4", b"4"),
            (b"page-5", b"5"),
        ],
    )
    .await;
    let (mut overlay, overlay_observation) = begin(&store, "overlay-refill").await;
    for item in [b"page-0", b"page-1", b"page-2"] {
        overlay
            .delete(key(Bytes::from_static(item)), Precondition::Any)
            .await
            .expect("overlay delete");
    }
    let page = overlay
        .range(&range(b"page-", b"page.", Direction::Forward, 2))
        .await
        .expect("forward refill");
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.key.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"page-3".as_slice(), b"page-4".as_slice()]
    );
    assert!(page.continuation.is_some());
    assert_eq!(
        overlay
            .put(
                key(Bytes::from_static(b"page-new")),
                value(Bytes::from_static(b"new")),
                Precondition::Any,
            )
            .await
            .expect_err("continuation freezes mutations")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    overlay.abort().await.expect("abort overlay refill");
    assert_eq!(
        overlay_observation.outcome().await.expect("abort verdict"),
        AttemptOutcome::NotCommitted,
        "an aborted transaction never reached storage"
    );

    let (mut reverse_overlay, _reverse_observation) = begin(&store, "reverse-overlay-refill").await;
    for item in [b"page-5", b"page-4", b"page-3"] {
        reverse_overlay
            .delete(key(Bytes::from_static(item)), Precondition::Any)
            .await
            .expect("reverse overlay delete");
    }
    let reverse_refill = reverse_overlay
        .range(&range(b"page-", b"page.", Direction::Reverse, 2))
        .await
        .expect("reverse refill");
    assert_eq!(
        reverse_refill
            .records
            .iter()
            .map(|record| record.key.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"page-2".as_slice(), b"page-1".as_slice()]
    );
    reverse_overlay
        .abort()
        .await
        .expect("abort reverse overlay refill");

    let page_request = range(b"page-", b"page.", Direction::Forward, 2);
    let mut snapshot_scan = store.begin_read().await.expect("begin snapshot scan");
    let first_page = snapshot_scan
        .range(&page_request)
        .await
        .expect("snapshot first page");
    let continuation = first_page.continuation.expect("snapshot continuation");
    seed(&store, &[(b"page-15", b"between")]).await;
    let mut continued_request = page_request.clone();
    continued_request.continuation = Some(continuation.clone());
    let same_snapshot = snapshot_scan
        .range(&continued_request)
        .await
        .expect("same transaction next page");
    assert_eq!(same_snapshot.records[0].key.as_bytes(), b"page-2");
    snapshot_scan.abort().await.expect("abort snapshot scan");
    let mut checkpoint_scan = store.begin_read().await.expect("begin checkpoint scan");
    let checkpoint = checkpoint_scan
        .range(&continued_request)
        .await
        .expect("new transaction checkpoint page");
    assert_eq!(checkpoint.records[0].key.as_bytes(), b"page-15");
    checkpoint_scan
        .abort()
        .await
        .expect("abort checkpoint scan");

    let mut reverse = store.begin_read().await.expect("begin reverse scan");
    let reverse_page = reverse
        .range(&range(b"page-", b"page.", Direction::Reverse, 2))
        .await
        .expect("reverse page");
    assert_eq!(
        reverse_page
            .records
            .iter()
            .map(|record| record.key.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"page-5".as_slice(), b"page-4".as_slice()]
    );
    reverse.abort().await.expect("abort reverse scan");

    let limited_keyspace_id = Uuid::new_v4();
    let limited_store = harness
        .open_store(
            FoundationDbTestStoreConfig {
                cluster_id: "limited-cluster".to_owned(),
                limits: FoundationDbTestLimitOverrides {
                    max_transaction_bytes: Some(16 * 1024),
                    ..Default::default()
                },
                provider: FoundationDbTestProviderConfig::Foundationdb {
                    cluster_file: cluster_file(),
                    keyspace_id: limited_keyspace_id,
                },
            },
            test_deadline(),
        )
        .await
        .expect("open limited keyspace");
    let (mut limited, limited_observation) = begin(&limited_store, "pre-io-limit").await;
    assert_eq!(
        limited
            .put(
                key(Bytes::from_static(b"large")),
                value(Bytes::from(vec![0x55; 16 * 1024])),
                Precondition::Any,
            )
            .await
            .expect_err("physical envelope exceeds public transaction budget")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    limited.abort().await.expect("abort limited transaction");
    assert_eq!(
        limited_observation
            .outcome()
            .await
            .expect("pre-I/O limit verdict"),
        AttemptOutcome::NotCommitted
    );
    assert_eq!(
        commit_state_key_count(limited_keyspace_id).await,
        0,
        "a pre-I/O limit failure must leave no evidence behind"
    );
    drop(limited_store);
    drop(store);
}

async fn durable_commit_scenarios(harness: &FoundationDbProviderTestHarness) {
    let keyspace_id = Uuid::new_v4();
    let store = harness
        .open_store(
            transaction_store_config("durable-cluster", keyspace_id),
            test_deadline(),
        )
        .await
        .expect("open durable commit keyspace");

    // A precondition FoundationDB refuses is proof, and a refused transaction
    // stages its commit-state key inside the transaction that never committed.
    seed(&store, &[(b"precondition", b"present")]).await;
    let (mut mismatch, mismatch_observation) = begin(&store, "durable precondition failure").await;
    mismatch
        .put(
            key(Bytes::from_static(b"precondition")),
            value(Bytes::from_static(b"rejected")),
            Precondition::Absent,
        )
        .await
        .expect("stage precondition failure");
    assert!(matches!(
        mismatch.commit().await,
        CommitOutcome::Conflict(_)
    ));
    assert_eq!(
        mismatch_observation.outcome().await.expect("verdict"),
        AttemptOutcome::NotCommitted
    );

    let (mut committed, committed_observation) = begin(&store, "durable committed").await;
    for item in [b"change-c", b"change-a", b"change-b"] {
        committed
            .put(
                key(Bytes::from_static(item)),
                value(Bytes::from_static(b"value")),
                Precondition::Any,
            )
            .await
            .expect("stage committed change");
    }
    let receipt = match committed.commit().await {
        CommitOutcome::Committed(receipt) => receipt,
        other => panic!("expected durable commit, got {other:?}"),
    };
    assert_eq!(receipt.attempt, committed_observation.id());
    assert_eq!(
        committed_observation.peek().expect("published verdict"),
        Some(AttemptOutcome::Committed(receipt.clone()))
    );
    // A published verdict survives its own evidence, which is exactly why the
    // evidence may be dropped as soon as the verdict is recorded.
    assert_eq!(
        committed_observation.outcome().await.expect("verdict"),
        AttemptOutcome::Committed(receipt)
    );

    // Every commit above was witnessed by its own owner, so every commit-state
    // key it wrote is released. A provider that only cleaned up on abandonment
    // would be holding one key per successful write here.
    await_commit_state_key_count(keyspace_id, 0).await;

    drop(store);
}

async fn cancellation_safe_supervisor_scenarios(harness: &FoundationDbProviderTestHarness) {
    let keyspace_id = Uuid::new_v4();
    let store = harness
        .open_store(
            transaction_store_config("supervisor-cluster", keyspace_id),
            test_deadline(),
        )
        .await
        .expect("open supervisor keyspace");

    let cancellation_control =
        arm_next_foundationdb_commit(true, false, false).expect("arm pre-native gate");
    let (mut cancellation, cancellation_observation) = begin(&store, "cancel commit waiter").await;
    cancellation
        .put(
            key(Bytes::from_static(b"cancel-owner")),
            value(Bytes::from_static(b"committed-by-owner")),
            Precondition::Any,
        )
        .await
        .expect("stage cancellation transaction");
    let waiter = tokio::spawn(async move { cancellation.commit().await });
    cancellation_control.wait_pre_native().await;
    assert_eq!(
        cancellation_observation
            .outcome()
            .await
            .expect("resolve held attempt"),
        AttemptOutcome::Unresolved,
        "a held commit is undecided; absence of evidence is not evidence of absence"
    );
    assert_eq!(
        cancellation_observation.peek().expect("peek held attempt"),
        None
    );
    waiter.abort();
    assert!(waiter.await.expect_err("cancel waiter").is_cancelled());
    // Losing the caller tells the store nothing, and the owner keeps going.
    assert_eq!(
        cancellation_observation
            .outcome()
            .await
            .expect("resolve after cancellation"),
        AttemptOutcome::Unresolved
    );
    cancellation_control.release_pre_native();
    cancellation_control.wait_response().await;
    assert!(matches!(
        await_terminal(&cancellation_observation).await,
        AttemptOutcome::Committed(_)
    ));

    let response_control =
        arm_next_foundationdb_commit(false, true, true).expect("arm response-loss gate");
    let (mut response, response_observation) = begin(&store, "lose committed response").await;
    response
        .put(
            key(Bytes::from_static(b"response-loss")),
            value(Bytes::from_static(b"committed")),
            Precondition::Any,
        )
        .await
        .expect("stage response-loss transaction");
    let waiter = tokio::spawn(async move { response.commit().await });
    response_control.wait_response().await;
    assert!(matches!(
        await_terminal(&response_observation).await,
        AttemptOutcome::Committed(_)
    ));
    response_control.release_response();
    assert!(matches!(
        waiter.await.expect("join response-loss waiter"),
        CommitOutcome::CommitUnknown(_)
    ));

    // The ambiguous commit is the one case with no release hook: its verdict was
    // published by the observation, not by the owner, so the provider was never
    // told the key was spent. One key per such attempt is the honest cost, and
    // the cancelled-then-released commit above is not part of it -- its owner
    // witnessed a plain success and cleaned up after itself.
    await_commit_state_key_count(keyspace_id, 1).await;

    drop(store);
}

async fn await_terminal(observation: &CommitObservation) -> AttemptOutcome {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let verdict = observation.outcome().await.expect("resolve attempt");
            if verdict.is_terminal() {
                return verdict;
            }
            // An unresolved answer costs one auxiliary transaction, so the
            // poll backs off instead of hammering the cluster.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a supervised attempt reaches a terminal state")
}

fn conformance_limit_overrides() -> FoundationDbTestLimitOverrides {
    FoundationDbTestLimitOverrides {
        max_key_bytes: Some(64),
        max_value_bytes: Some(1_899),
        max_page_size: Some(10),
        max_transaction_operations: Some(8),
        max_transaction_bytes: Some(16 * 1024),
        transaction_deadline_ms: Some(4_000),
    }
}

fn conformance_store_config() -> FoundationDbTestStoreConfig {
    FoundationDbTestStoreConfig {
        cluster_id: "foundationdb-conformance-cluster".to_owned(),
        limits: conformance_limit_overrides(),
        provider: FoundationDbTestProviderConfig::Foundationdb {
            cluster_file: cluster_file(),
            // A fresh keyspace per open: the attempt group only means something
            // if two calls really are two instances.
            keyspace_id: Uuid::new_v4(),
        },
    }
}

fn conformance_factory(runtime: Rc<FoundationDbProviderTestHarness>) -> StateStoreFactory {
    Rc::new(move || {
        let runtime = Rc::clone(&runtime);
        Box::pin(async move {
            runtime
                .open_store(conformance_store_config(), test_deadline())
                .await
        })
    })
}

fn conformance_fault_factory(
    runtime: Rc<FoundationDbProviderTestHarness>,
) -> FaultStateStoreFactory {
    Rc::new(move || {
        let runtime = Rc::clone(&runtime);
        Box::pin(async move {
            let store = runtime
                .open_store(conformance_store_config(), test_deadline())
                .await?;
            let controller: Arc<dyn PostDispatchController> =
                Arc::new(FoundationDbPostDispatchController);
            Ok(StateStoreFaultFixture::new(store, controller))
        })
    })
}

struct FoundationDbPostDispatchController;

#[async_trait]
impl PostDispatchController for FoundationDbPostDispatchController {
    async fn arm(&self, scenario: PostDispatchScenario) -> Box<dyn PostDispatchControl> {
        let gate = match scenario {
            PostDispatchScenario::CancelWaiterBeforeApply => {
                arm_next_foundationdb_commit(true, false, false)
            }
            PostDispatchScenario::LoseCommittedResponse => {
                arm_next_foundationdb_commit(false, true, true)
            }
        }
        .expect("arm real FoundationDB provider commit gate");
        Box::new(FoundationDbPostDispatchControl { scenario, gate })
    }
}

struct FoundationDbPostDispatchControl {
    scenario: PostDispatchScenario,
    gate: FoundationDbCommitGateControl,
}

#[async_trait]
impl PostDispatchControl for FoundationDbPostDispatchControl {
    async fn wait_dispatched(&self) {
        match self.scenario {
            PostDispatchScenario::CancelWaiterBeforeApply => self.gate.wait_pre_native().await,
            PostDispatchScenario::LoseCommittedResponse => self.gate.wait_response().await,
        }
    }

    async fn wait_waiter_cancelled(&self) {
        if self.scenario == PostDispatchScenario::CancelWaiterBeforeApply {
            self.gate.wait_waiter_dropped().await;
        }
    }

    async fn allow_provider_progress(&self) {
        if self.scenario == PostDispatchScenario::CancelWaiterBeforeApply {
            self.gate.release_pre_native();
        }
    }

    async fn release_response(&self) {
        if self.scenario == PostDispatchScenario::LoseCommittedResponse {
            self.gate.release_response();
        }
    }

    async fn wait_inner_dropped(&self) {
        if self.scenario == PostDispatchScenario::CancelWaiterBeforeApply {
            self.gate.wait_waiter_dropped().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn foundationdb_suite() {
    let runtime = Rc::new(
        FoundationDbProviderTestHarness::boot(client_config())
            .expect("boot process-owned FoundationDB runtime"),
    );

    let keyspace_id = Uuid::new_v4();
    let config = store_config("identity-cluster", keyspace_id);
    let (left, right) = tokio::join!(
        runtime.open_store(config.clone(), test_deadline()),
        runtime.open_store(config, test_deadline())
    );
    let left = left.expect("initialize FoundationDB keyspace");
    let right = right.expect("concurrent open converges on keyspace identity");
    let left_identity = left.identity().await.expect("read left identity");
    let right_identity = right.identity().await.expect("read right identity");
    assert_eq!(left_identity, right_identity);
    assert_eq!(left_identity.cluster_id, "identity-cluster");
    // Two opens of one keyspace are two instances, and a capability from one is
    // not answerable by the other.
    assert_ne!(left.attempts().scope(), right.attempts().scope());
    {
        // Scoped on purpose: an observation keeps its instance's supervisor --
        // and through it the provider handle -- alive, so one held past the end
        // of the test would block the runtime from ever draining.
        let (foreign, foreign_observation) = left.attempts().reserve().expect("reserve on left");
        assert_eq!(
            right
                .begin_write(foreign, "capability from the other open")
                .await
                .err()
                .expect("a foreign capability must be refused")
                .kind(),
            StateStoreErrorKind::InvalidRequest
        );
        assert_eq!(
            foreign_observation.outcome().await.expect("verdict"),
            AttemptOutcome::NotCommitted,
            "a refusal happens before dispatch, so the attempt is proven effect-free"
        );
    }

    let mismatch = match runtime
        .open_store(
            store_config("different-cluster", keyspace_id),
            test_deadline(),
        )
        .await
    {
        Ok(_) => panic!("existing keyspace must reject a cluster identity mismatch"),
        Err(error) => error,
    };
    assert_eq!(mismatch.kind(), StateStoreErrorKind::InvalidConfiguration);

    let corrupt_keyspace = Uuid::new_v4();
    write_partial_identity(corrupt_keyspace).await;
    let corruption = match runtime
        .open_store(
            store_config("identity-cluster", corrupt_keyspace),
            test_deadline(),
        )
        .await
    {
        Ok(_) => panic!("partial identity must fail closed"),
        Err(error) => error,
    };
    assert_eq!(corruption.kind(), StateStoreErrorKind::Corruption);

    let legacy_keyspace = Uuid::new_v4();
    write_version_one_identity(legacy_keyspace, "identity-cluster").await;
    let legacy = match runtime
        .open_store(
            store_config("identity-cluster", legacy_keyspace),
            test_deadline(),
        )
        .await
    {
        Ok(_) => panic!("a change-feed-era keyspace must be refused"),
        Err(error) => error,
    };
    assert_eq!(legacy.kind(), StateStoreErrorKind::Corruption);
    let legacy_root = keyspace_root(legacy_keyspace);
    let database = raw_database();
    let inspection = raw_transaction(&database);
    assert_eq!(
        inspection
            .get(&[legacy_root.as_slice(), &[0x00, 0x00]].concat(), false)
            .await
            .expect("read refused schema version")
            .map(|value| value.to_vec()),
        Some(vec![1]),
        "a refused keyspace is left exactly as the operator wrote it"
    );
    drop(inspection);

    transaction_scenarios(runtime.as_ref()).await;
    durable_commit_scenarios(runtime.as_ref()).await;
    cancellation_safe_supervisor_scenarios(runtime.as_ref()).await;

    let factory = conformance_factory(Rc::clone(&runtime));
    state_store_conformance::run_basic_suite(&factory).await;
    state_store_conformance::run_attempt_suite(&factory).await;
    drop(factory);
    let fault_factory = conformance_fault_factory(Rc::clone(&runtime));
    state_store_conformance::run_fault_suite(&fault_factory).await;
    drop(fault_factory);

    drop(right);
    drop(left);
    let mut runtime = match Rc::try_unwrap(runtime) {
        Ok(runtime) => runtime,
        Err(_) => panic!("all FoundationDB runtime owners drained"),
    };
    runtime
        .shutdown(test_deadline())
        .await
        .expect("shutdown FoundationDB runtime after all handles drain");
}
