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
#[cfg(feature = "state-store-test-hooks")]
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "state-store-test-hooks")]
use async_trait::async_trait;
use bytes::Bytes;
use foundationdb::Database;
use foundationdb::options::TransactionOption;
use novarocks_spi::state_store::{
    ChangePollRequest, CommitOutcome, CommitResolution, Direction, Key, KeyRange, Precondition,
    RangeRequest, StateStore, StateStoreErrorKind, TransactionId, Value,
};
use novarocks_state_store_foundationdb::{
    FoundationDbClientConfig, FoundationDbProviderTestHarness, FoundationDbTestLimitOverrides,
    FoundationDbTestProviderConfig, FoundationDbTestStoreConfig,
};
#[cfg(feature = "state-store-test-hooks")]
use novarocks_state_store_foundationdb::{
    FoundationDbCommitGateControl, arm_next_foundationdb_commit,
};
use uuid::Uuid;

#[cfg(feature = "state-store-test-hooks")]
use novarocks_spi::state_store::conformance::{
    self as state_store_conformance, PostDispatchControl, PostDispatchController,
    PostDispatchScenario, StateStoreConformanceFixture, StateStoreFactory,
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

async fn write_partial_identity(keyspace_id: Uuid) {
    let path = cluster_file();
    let database = Database::from_path(path.to_str().expect("UTF-8 cluster file"))
        .expect("create direct FoundationDB test handle");
    let transaction = database
        .create_trx()
        .expect("create corruption transaction");
    transaction
        .set_option(TransactionOption::Timeout(4_000))
        .expect("set corruption transaction timeout");
    transaction
        .set_option(TransactionOption::RetryLimit(0))
        .expect("disable corruption transaction retries");
    let schema_key = [
        b"NRSS\x01".as_slice(),
        keyspace_id.as_bytes(),
        &[0x00, 0x00],
    ]
    .concat();
    transaction.set(&schema_key, &[1]);
    transaction
        .commit()
        .await
        .expect("persist partial identity corruption");
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

fn assert_committed(outcome: CommitOutcome) {
    assert!(
        matches!(outcome, CommitOutcome::Committed(_)),
        "{outcome:?}"
    );
}

async fn seed(store: &dyn StateStore, records: &[(&'static [u8], &'static [u8])]) {
    let mut transaction = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "seed")
        .await
        .expect("begin seed transaction");
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
    let mut ordered = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "ordered-overlay")
        .await
        .expect("begin ordered overlay");
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
    seed(store.as_ref(), &[(&[0x00, 0xff, 0x10], b"changed")]).await;
    assert_eq!(
        repeatable.get(&binary_key).await.expect("second read"),
        before
    );
    repeatable.abort().await.expect("abort repeatable read");

    let conflict_key = key(Bytes::from_static(b"same-key"));
    seed(store.as_ref(), &[(b"same-key", b"base")]).await;
    let mut left = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "same-left")
        .await
        .expect("begin left");
    let mut right = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "same-right")
        .await
        .expect("begin right");
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

    seed(store.as_ref(), &[(b"skew-a", b"1"), (b"skew-b", b"1")]).await;
    let mut skew_left = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "skew-left")
        .await
        .expect("begin skew left");
    let mut skew_right = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "skew-right")
        .await
        .expect("begin skew right");
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

    let mut phantom = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "phantom-reader")
        .await
        .expect("begin phantom reader");
    phantom
        .range(&range(b"phantom-", b"phantom.", Direction::Forward, 2))
        .await
        .expect("read empty phantom range");
    seed(store.as_ref(), &[(b"phantom-key", b"inserted")]).await;
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
        store.as_ref(),
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
    let mut overlay = store
        .begin_write(TransactionId::from(Uuid::new_v4()), "overlay-refill")
        .await
        .expect("begin overlay refill");
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

    let mut reverse_overlay = store
        .begin_write(
            TransactionId::from(Uuid::new_v4()),
            "reverse-overlay-refill",
        )
        .await
        .expect("begin reverse overlay refill");
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
    seed(store.as_ref(), &[(b"page-15", b"between")]).await;
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
    let transaction_id = TransactionId::from(Uuid::new_v4());
    let mut limited = limited_store
        .begin_write(transaction_id, "pre-io-limit")
        .await
        .expect("begin limited transaction without provider I/O");
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
    let database = Database::from_path(
        cluster_file()
            .to_str()
            .expect("UTF-8 FoundationDB cluster file"),
    )
    .expect("open raw FoundationDB controller handle");
    let controller = database.create_trx().expect("controller transaction");
    controller
        .set_option(TransactionOption::Timeout(4_000))
        .expect("controller timeout");
    controller
        .set_option(TransactionOption::RetryLimit(0))
        .expect("controller retry limit");
    let reservation_key = [
        b"NRSS\x01".as_slice(),
        limited_keyspace_id.as_bytes(),
        &[0x03],
        transaction_id.as_uuid().as_bytes(),
    ]
    .concat();
    assert!(
        controller
            .get(&reservation_key, false)
            .await
            .expect("read reservation key")
            .is_none(),
        "pre-I/O limit failure must not create a durable reservation"
    );
    drop(limited_store);
    drop(store);
}

async fn durable_commit_and_change_scenarios(harness: &FoundationDbProviderTestHarness) {
    let store = harness
        .open_store(
            transaction_store_config("durable-cluster", Uuid::new_v4()),
            test_deadline(),
        )
        .await
        .expect("open durable commit keyspace");

    let tombstoned = TransactionId::from(Uuid::new_v4());
    assert_eq!(
        store
            .resolve_commit(&tombstoned)
            .await
            .expect("create absent resolution tombstone"),
        CommitResolution::NotCommitted
    );
    assert_eq!(
        store
            .resolve_commit(&tombstoned)
            .await
            .expect("repeat stable tombstone resolution"),
        CommitResolution::NotCommitted
    );
    let mut rejected = store
        .begin_write(tombstoned, "reuse tombstoned transaction")
        .await
        .expect("begin tombstoned transaction");
    rejected
        .put(
            key(Bytes::from_static(b"tombstoned")),
            value(Bytes::from_static(b"must-not-commit")),
            Precondition::Present,
        )
        .await
        .expect("stage mismatched tombstoned transaction");
    assert!(matches!(
        rejected.commit().await,
        CommitOutcome::DefiniteFailure(ref error)
            if error.kind() == StateStoreErrorKind::InvalidRequest
    ));
    assert_eq!(
        store
            .resolve_commit(&tombstoned)
            .await
            .expect("resolve tombstone after rejected mismatched commit"),
        CommitResolution::NotCommitted
    );

    let precondition_id = TransactionId::from(Uuid::new_v4());
    seed(store.as_ref(), &[(b"precondition", b"present")]).await;
    let mut mismatch = store
        .begin_write(precondition_id, "durable precondition failure")
        .await
        .expect("begin precondition failure");
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
        store
            .resolve_commit(&precondition_id)
            .await
            .expect("resolve precondition failure"),
        CommitResolution::NotCommitted
    );

    let baseline = store
        .poll_changes(&ChangePollRequest {
            after: None,
            page_size: store.limits().max_page_size,
        })
        .await
        .expect("poll durable scenario baseline")
        .next_cursor;
    let committed_id = TransactionId::from(Uuid::new_v4());
    let mut committed = store
        .begin_write(committed_id, "durable committed transaction")
        .await
        .expect("begin committed transaction");
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
    assert_eq!(
        store
            .resolve_commit(&committed_id)
            .await
            .expect("resolve committed transaction"),
        CommitResolution::Committed(receipt.clone())
    );

    let mut duplicate = store
        .begin_write(committed_id, "duplicate committed transaction")
        .await
        .expect("begin duplicate committed transaction");
    duplicate
        .put(
            key(Bytes::from_static(b"duplicate-must-not-apply")),
            value(Bytes::from_static(b"value")),
            Precondition::Any,
        )
        .await
        .expect("stage duplicate transaction");
    assert_eq!(
        duplicate.commit().await,
        CommitOutcome::Committed(receipt.clone())
    );

    let first = store
        .poll_changes(&ChangePollRequest {
            after: Some(baseline),
            page_size: 2,
        })
        .await
        .expect("poll first same-revision page");
    assert_eq!(first.hints.len(), 2);
    assert!(
        first
            .hints
            .iter()
            .all(|hint| hint.revision == receipt.revision)
    );
    let second = store
        .poll_changes(&ChangePollRequest {
            after: Some(first.next_cursor),
            page_size: 2,
        })
        .await
        .expect("poll final same-revision page");
    assert_eq!(second.hints.len(), 1);
    assert_eq!(second.high_watermark, receipt.revision);
    assert_eq!(
        first
            .hints
            .iter()
            .chain(second.hints.iter())
            .map(|hint| hint.key.as_bytes())
            .collect::<Vec<_>>(),
        vec![
            b"change-a".as_slice(),
            b"change-b".as_slice(),
            b"change-c".as_slice(),
        ]
    );
    let identity = store.identity().await.expect("read durable identity");
    let (cursor_revision, cursor_sequence) = second
        .next_cursor
        .decode(identity.store_id)
        .expect("decode exhausted cursor");
    assert_eq!(cursor_revision, receipt.revision);
    assert_eq!(cursor_sequence, u32::MAX);

    let empty_id = TransactionId::from(Uuid::new_v4());
    let empty = store
        .begin_write(empty_id, "high watermark without changes")
        .await
        .expect("begin empty transaction");
    let empty_receipt = match empty.commit().await {
        CommitOutcome::Committed(receipt) => receipt,
        other => panic!("expected empty durable commit, got {other:?}"),
    };
    let empty_page = store
        .poll_changes(&ChangePollRequest {
            after: Some(second.next_cursor),
            page_size: 2,
        })
        .await
        .expect("poll high-watermark-only commit");
    assert!(empty_page.hints.is_empty());
    assert_eq!(empty_page.high_watermark, empty_receipt.revision);

    drop(store);
}

#[cfg(feature = "state-store-test-hooks")]
async fn cancellation_safe_supervisor_scenarios(harness: &FoundationDbProviderTestHarness) {
    let store = harness
        .open_store(
            transaction_store_config("supervisor-cluster", Uuid::new_v4()),
            test_deadline(),
        )
        .await
        .expect("open supervisor keyspace");

    let cancellation_control =
        arm_next_foundationdb_commit(true, false, false).expect("arm pre-native gate");
    let cancellation_id = TransactionId::from(Uuid::new_v4());
    let mut cancellation = store
        .begin_write(cancellation_id, "cancel commit waiter")
        .await
        .expect("begin cancellation transaction");
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
        store
            .resolve_commit(&cancellation_id)
            .await
            .expect("resolve held cancellation transaction"),
        CommitResolution::Unresolved
    );
    waiter.abort();
    assert!(waiter.await.expect_err("cancel waiter").is_cancelled());
    cancellation_control.release_pre_native();
    cancellation_control.wait_response().await;
    assert!(matches!(
        await_terminal(store.as_ref(), cancellation_id).await,
        CommitResolution::Committed(_)
    ));

    let response_control =
        arm_next_foundationdb_commit(false, true, true).expect("arm response-loss gate");
    let response_id = TransactionId::from(Uuid::new_v4());
    let mut response = store
        .begin_write(response_id, "lose committed response")
        .await
        .expect("begin response-loss transaction");
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
        store
            .resolve_commit(&response_id)
            .await
            .expect("resolve committed response-loss transaction"),
        CommitResolution::Committed(_)
    ));
    response_control.release_response();
    assert!(matches!(
        waiter.await.expect("join response-loss waiter"),
        CommitOutcome::CommitUnknown(_)
    ));

    drop(store);
}

#[cfg(feature = "state-store-test-hooks")]
fn conformance_limit_overrides() -> FoundationDbTestLimitOverrides {
    FoundationDbTestLimitOverrides {
        max_key_bytes: Some(64),
        max_value_bytes: Some(1_899),
        max_page_size: Some(10),
        max_transaction_operations: Some(8),
        max_transaction_bytes: Some(16 * 1024),
        transaction_deadline_ms: Some(4_000),
        runner_max_attempts: Some(3),
    }
}

#[cfg(feature = "state-store-test-hooks")]
fn conformance_factory(runtime: Rc<FoundationDbProviderTestHarness>) -> StateStoreFactory {
    Rc::new(move || {
        let runtime = Rc::clone(&runtime);
        Box::pin(async move {
            let store = runtime
                .open_store(
                    FoundationDbTestStoreConfig {
                        cluster_id: "foundationdb-conformance-cluster".to_owned(),
                        limits: conformance_limit_overrides(),
                        provider: FoundationDbTestProviderConfig::Foundationdb {
                            cluster_file: cluster_file(),
                            keyspace_id: Uuid::new_v4(),
                        },
                    },
                    test_deadline(),
                )
                .await?;
            let controller: Arc<dyn PostDispatchController> =
                Arc::new(FoundationDbPostDispatchController);
            Ok(StateStoreConformanceFixture::new(store, controller))
        })
    })
}

#[cfg(feature = "state-store-test-hooks")]
struct FoundationDbPostDispatchController;

#[cfg(feature = "state-store-test-hooks")]
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

#[cfg(feature = "state-store-test-hooks")]
struct FoundationDbPostDispatchControl {
    scenario: PostDispatchScenario,
    gate: FoundationDbCommitGateControl,
}

#[cfg(feature = "state-store-test-hooks")]
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

#[cfg(feature = "state-store-test-hooks")]
async fn await_terminal(store: &dyn StateStore, transaction_id: TransactionId) -> CommitResolution {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let resolution = store
                .resolve_commit(&transaction_id)
                .await
                .expect("resolve supervised transaction");
            if !matches!(resolution, CommitResolution::Unresolved) {
                return resolution;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("supervised transaction reaches durable terminal")
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

    transaction_scenarios(runtime.as_ref()).await;
    durable_commit_and_change_scenarios(runtime.as_ref()).await;
    #[cfg(feature = "state-store-test-hooks")]
    cancellation_safe_supervisor_scenarios(runtime.as_ref()).await;
    #[cfg(feature = "state-store-test-hooks")]
    {
        let factory = conformance_factory(Rc::clone(&runtime));
        state_store_conformance::run_state_store_conformance(Rc::clone(&factory)).await;
        drop(factory);
    }

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
