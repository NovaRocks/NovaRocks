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

#![cfg(all(
    feature = "mysql-state-store-provider",
    feature = "state-store-test-hooks"
))]

#[cfg(feature = "state-store-test-hooks")]
use std::cell::RefCell;
use std::process::Command;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_secret::SecretValue;
#[cfg(feature = "state-store-test-hooks")]
use novarocks_spi::state_store::ContinuationToken;
use novarocks_spi::state_store::{
    CommitOutcome, Direction, Key, KeyRange, Precondition, RangeRequest, StateStore,
    StateStoreErrorKind, TransactionId, Value,
};
#[cfg(feature = "state-store-test-hooks")]
use novarocks_state_store_mysql::test_support::{
    MysqlChangeTestApi, MysqlCommitTestApi, MysqlOpenGatePhase, MysqlPostDispatchTestControl,
    MysqlPrepareRollbackFailure, MysqlStatementTestApi, arm_mysql_open_gate, hold_connection,
};
use novarocks_state_store_mysql::test_support::{
    MysqlOccTestApi, MysqlProviderTestHarness, MysqlSchemaColumnSnapshot, MysqlSchemaMutation,
    MysqlSchemaTableSnapshot, MysqlTransactionTestApi, MysqlWriteTestApi,
    acquire_schema_advisory_lock, active_readiness, advisory_lock_name, apply_schema_mutation,
    is_schema_advisory_lock_free, schema_snapshot, schema_timeout_connection_is_destroyed,
    store_readiness_snapshot,
};
use novarocks_state_store_mysql::{
    MYSQL_STATE_STORE_PROVIDER_ID, MySqlClientConfig, MySqlTlsMode, MysqlTestLimitOverrides,
    MysqlTestProviderConfig, MysqlTestStoreConfig,
};
use sha2::{Digest, Sha256};
use uuid::{Uuid, Version};

mod common;

use novarocks_spi::state_store::conformance::{
    self as state_store_conformance, PostDispatchControl, PostDispatchController,
    PostDispatchScenario, StateStoreConformanceFixture, StateStoreFactory,
};

const CLUSTER_ID: &str = "mysql-schema-test-cluster";
const EXPECTED_SCHEMA_DIGEST: &str =
    "ddc1a524fb8fe17b143b3783d105267187e4a0d0019556ac0825cfa4c2a9faf7";

struct TestDatabase {
    name: String,
}

impl TestDatabase {
    fn provision(test_name: &str, suffix: &str) -> Self {
        let digest = Sha256::digest([test_name.as_bytes(), suffix.as_bytes()].concat());
        let prefix = test_name
            .chars()
            .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
            .take(12)
            .collect::<String>();
        let case_id = format!("ss3t4_{prefix}_{}", hex::encode(&digest[..4]));
        let script =
            common::repo_root().join("docker/mysql-state-store/provision-test-database.sh");
        let output = Command::new(script)
            .args(["create", &case_id])
            .output()
            .expect("run MySQL test database provisioner child process");
        assert!(
            output.status.success(),
            "MySQL test database provisioner create failed"
        );
        let name = String::from_utf8(output.stdout)
            .expect("provisioner database output is UTF-8")
            .trim()
            .to_owned();
        assert!(!name.is_empty(), "provisioner returned an empty database");
        Self { name }
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let script =
            common::repo_root().join("docker/mysql-state-store/provision-test-database.sh");
        let status = Command::new(script).args(["drop", &self.name]).status();
        if !status.is_ok_and(|status| status.success()) && !std::thread::panicking() {
            panic!("MySQL test database provisioner drop failed");
        }
    }
}

fn fixture_client_config() -> MySqlClientConfig {
    let password_env =
        std::env::var("NOVAROCKS_MYSQL_PASSWORD_ENV").expect("fixture password env name");
    MySqlClientConfig {
        host: std::env::var("NOVAROCKS_MYSQL_HOST").expect("fixture host"),
        port: std::env::var("NOVAROCKS_MYSQL_PORT")
            .expect("fixture port")
            .parse()
            .expect("numeric fixture port"),
        username: std::env::var("NOVAROCKS_MYSQL_USERNAME").expect("fixture username"),
        password: SecretValue::new(std::env::var(password_env).expect("fixture password value")),
        tls_mode: MySqlTlsMode::Disabled,
        tls_ca_path: None,
        tls_cert_path: None,
        tls_key_path: None,
        connect_timeout_ms: 1_000,
        pool_min: 1,
        pool_max: 4,
        inactive_connection_ttl_ms: 1_000,
    }
}

fn store_config(database: &str, cluster_id: &str, deadline_ms: u64) -> MysqlTestStoreConfig {
    MysqlTestStoreConfig {
        cluster_id: cluster_id.to_owned(),
        limits: MysqlTestLimitOverrides {
            transaction_deadline_ms: Some(deadline_ms),
            ..MysqlTestLimitOverrides::default()
        },
        provider: MysqlTestProviderConfig::Mysql {
            database: database.to_owned(),
        },
    }
}

fn transaction_store_config(
    database: &str,
    limits: MysqlTestLimitOverrides,
) -> MysqlTestStoreConfig {
    MysqlTestStoreConfig {
        cluster_id: CLUSTER_ID.to_owned(),
        limits,
        provider: MysqlTestProviderConfig::Mysql {
            database: database.to_owned(),
        },
    }
}

async fn open_mysql_store(
    runtime: &MysqlProviderTestHarness,
    config: MysqlTestStoreConfig,
) -> Result<Arc<dyn StateStore>, novarocks_spi::state_store::StateStoreError> {
    runtime
        .open_store(config, Instant::now() + Duration::from_secs(30))
        .await
}

async fn open_store(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    cluster_id: &str,
    deadline_ms: u64,
) -> Result<std::sync::Arc<dyn StateStore>, novarocks_spi::state_store::StateStoreError> {
    open_mysql_store(runtime, store_config(database, cluster_id, deadline_ms)).await
}

fn key(bytes: impl Into<Bytes>) -> Key {
    Key::try_from(bytes.into()).expect("valid test key")
}

fn value(bytes: impl Into<Bytes>) -> Value {
    Value::try_from(bytes.into()).expect("valid test value")
}

fn transaction_id() -> TransactionId {
    TransactionId::from(Uuid::now_v7())
}

fn assert_committed(outcome: CommitOutcome) {
    assert!(
        matches!(outcome, CommitOutcome::Committed(_)),
        "{outcome:?}"
    );
}

struct UnusedPostDispatch;

#[async_trait]
impl PostDispatchController for UnusedPostDispatch {
    async fn arm(&self, _scenario: PostDispatchScenario) -> Box<dyn PostDispatchControl> {
        panic!("Task 5 conformance cases do not use post-dispatch controls")
    }
}

fn shared_factory(store: Arc<dyn StateStore>) -> StateStoreFactory {
    Rc::new(move || {
        let store = Arc::clone(&store);
        Box::pin(async move {
            Ok(StateStoreConformanceFixture::new(
                store,
                Arc::new(UnusedPostDispatch),
            ))
        })
    })
}

#[cfg(feature = "state-store-test-hooks")]
struct MysqlPostDispatchController;

#[cfg(feature = "state-store-test-hooks")]
#[async_trait]
impl PostDispatchController for MysqlPostDispatchController {
    async fn arm(&self, scenario: PostDispatchScenario) -> Box<dyn PostDispatchControl> {
        Box::new(MysqlPostDispatchControl {
            inner: MysqlCommitTestApi::arm_shared_post_dispatch(matches!(
                scenario,
                PostDispatchScenario::LoseCommittedResponse
            )),
        })
    }
}

#[cfg(feature = "state-store-test-hooks")]
struct MysqlPostDispatchControl {
    inner: MysqlPostDispatchTestControl,
}

#[cfg(feature = "state-store-test-hooks")]
#[async_trait]
impl PostDispatchControl for MysqlPostDispatchControl {
    async fn wait_dispatched(&self) {
        self.inner.wait_dispatched().await;
    }

    async fn wait_waiter_cancelled(&self) {
        tokio::task::yield_now().await;
    }

    async fn allow_provider_progress(&self) {
        self.inner.allow_provider_progress();
    }

    async fn release_response(&self) {}

    async fn wait_inner_dropped(&self) {
        tokio::task::yield_now().await;
    }
}

#[cfg(feature = "state-store-test-hooks")]
fn shared_post_dispatch_factory(store: Arc<dyn StateStore>) -> StateStoreFactory {
    Rc::new(move || {
        let store = Arc::clone(&store);
        Box::pin(async move {
            Ok(StateStoreConformanceFixture::new(
                store,
                Arc::new(MysqlPostDispatchController),
            ))
        })
    })
}

#[cfg(feature = "state-store-test-hooks")]
fn mysql_conformance_limits() -> MysqlTestLimitOverrides {
    const CONFORMANCE_MAX_VALUE_BYTES: usize = 1_899;
    let mutation_bytes = MysqlWriteTestApi::put_accounted_bytes(
        &[11, 0xfe, 1],
        &vec![0; CONFORMANCE_MAX_VALUE_BYTES],
        &Precondition::Any,
    )
    .expect("account MySQL conformance mutation");
    let four_mutation_bytes = MysqlWriteTestApi::transaction_envelope_bytes()
        .checked_add(
            mutation_bytes
                .checked_mul(4)
                .expect("four MySQL conformance mutations fit usize"),
        )
        .expect("MySQL conformance budget fits usize");
    MysqlTestLimitOverrides {
        max_key_bytes: Some(64),
        max_value_bytes: Some(CONFORMANCE_MAX_VALUE_BYTES),
        max_page_size: Some(10),
        max_transaction_operations: Some(8),
        max_transaction_bytes: Some(four_mutation_bytes),
        transaction_deadline_ms: Some(4_000),
        runner_max_attempts: Some(3),
    }
}

#[cfg(feature = "state-store-test-hooks")]
fn mysql_conformance_factory(
    runtime: Rc<MysqlProviderTestHarness>,
    databases: Rc<RefCell<Vec<TestDatabase>>>,
) -> StateStoreFactory {
    Rc::new(move || {
        let database = TestDatabase::provision("mysql_suite", "conformance");
        let database_name = database.name.clone();
        databases.borrow_mut().push(database);
        let runtime = Rc::clone(&runtime);
        Box::pin(async move {
            let store = open_mysql_store(
                runtime.as_ref(),
                MysqlTestStoreConfig {
                    cluster_id: "mysql-conformance-cluster".to_owned(),
                    limits: mysql_conformance_limits(),
                    provider: MysqlTestProviderConfig::Mysql {
                        database: database_name,
                    },
                },
            )
            .await?;
            Ok(StateStoreConformanceFixture::new(
                store,
                Arc::new(MysqlPostDispatchController),
            ))
        })
    })
}

fn expected_tables() -> Vec<MysqlSchemaTableSnapshot> {
    vec![
        MysqlSchemaTableSnapshot {
            name: "state_store_changes".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                MysqlSchemaColumnSnapshot::new("revision", "bigint unsigned", false, 1),
                MysqlSchemaColumnSnapshot::new("sequence", "int unsigned", false, 2),
                MysqlSchemaColumnSnapshot::new("key_bytes", "varbinary(3072)", false, 0),
            ],
            primary_key: vec!["revision".to_owned(), "sequence".to_owned()],
            secondary_indexes: Vec::new(),
        },
        MysqlSchemaTableSnapshot {
            name: "state_store_commits".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                MysqlSchemaColumnSnapshot::new("transaction_id", "binary(16)", false, 1),
                MysqlSchemaColumnSnapshot::new("state", "tinyint unsigned", false, 0),
                MysqlSchemaColumnSnapshot::new("reservation_token", "binary(16)", true, 0),
                MysqlSchemaColumnSnapshot::new("revision", "bigint unsigned", true, 0),
                MysqlSchemaColumnSnapshot::new("updated_at_ms", "bigint unsigned", false, 0),
            ],
            primary_key: vec!["transaction_id".to_owned()],
            secondary_indexes: Vec::new(),
        },
        MysqlSchemaTableSnapshot {
            name: "state_store_kv".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                MysqlSchemaColumnSnapshot::new("key_bytes", "varbinary(3072)", false, 1),
                MysqlSchemaColumnSnapshot::new("value_bytes", "mediumblob", false, 0),
                MysqlSchemaColumnSnapshot::new("version_bytes", "binary(12)", false, 0),
            ],
            primary_key: vec!["key_bytes".to_owned()],
            secondary_indexes: Vec::new(),
        },
        MysqlSchemaTableSnapshot {
            name: "state_store_meta".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                MysqlSchemaColumnSnapshot::new("meta_key", "varbinary(64)", false, 1),
                MysqlSchemaColumnSnapshot::new("meta_value", "varbinary(4096)", false, 0),
            ],
            primary_key: vec!["meta_key".to_owned()],
            secondary_indexes: Vec::new(),
        },
    ]
}

async fn assert_open_corruption(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    cluster_id: &str,
) {
    let error = match open_store(runtime, database, cluster_id, 4_000).await {
        Ok(_) => panic!("schema drift must fail closed"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StateStoreErrorKind::Corruption);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_bootstraps_exact_four_tables_and_meta() {
    let database = TestDatabase::provision(
        "mysql_schema_bootstraps_exact_four_tables_and_meta",
        "exact",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");

    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("bootstrap MySQL state store");
    assert_eq!(
        store.metrics_snapshot().provider,
        MYSQL_STATE_STORE_PROVIDER_ID
    );
    assert_eq!(store.limits().max_key_bytes, 3072);
    let identity = store.identity().await.expect("store identity");
    assert_eq!(identity.cluster_id, CLUSTER_ID);
    assert_eq!(identity.store_id.get_version(), Some(Version::SortRand));

    let snapshot = schema_snapshot(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("schema snapshot");
    assert_eq!(snapshot.tables, expected_tables());
    assert!(snapshot.views.is_empty());
    assert!(snapshot.triggers.is_empty());
    assert_eq!(
        snapshot.meta_keys,
        vec![
            "change_retention_floor",
            "cluster_id",
            "current_revision",
            "initial_incarnation",
            "schema_digest",
            "schema_version",
            "store_id",
        ]
    );
    assert_eq!(snapshot.schema_version, 1);
    assert_eq!(snapshot.schema_digest, EXPECTED_SCHEMA_DIGEST);
    assert_eq!(snapshot.store_id, identity.store_id);
    assert_eq!(snapshot.cluster_id, CLUSTER_ID);
    assert_eq!(snapshot.initial_incarnation, 1);
    assert_eq!(snapshot.current_revision, 0);
    assert_eq!(snapshot.change_retention_floor, (0, u32::MAX));

    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_schema_concurrent_open_converges_on_one_identity() {
    let database = TestDatabase::provision(
        "mysql_schema_concurrent_open_converges_on_one_identity",
        "concurrent",
    );
    let mut first =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct first runtime");
    let mut second =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct second runtime");

    let (first_store, second_store) = tokio::join!(
        open_store(&first, &database.name, CLUSTER_ID, 4_000),
        open_store(&second, &database.name, CLUSTER_ID, 4_000)
    );
    let first_store = first_store.expect("first concurrent open");
    let second_store = second_store.expect("second concurrent open");
    assert_eq!(
        first_store.identity().await.expect("first identity"),
        second_store.identity().await.expect("second identity")
    );

    drop(first_store);
    drop(second_store);
    first
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown first runtime");
    second
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown second runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_rejects_cluster_identity_mismatch() {
    let database =
        TestDatabase::provision("mysql_schema_rejects_cluster_identity_mismatch", "cluster");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("initialize first cluster identity");
    drop(store);

    let error = match open_store(
        &runtime,
        &database.name,
        "different-sensitive-cluster",
        4_000,
    )
    .await
    {
        Ok(_) => panic!("cluster mismatch must fail closed"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_rejects_partial_and_extra_objects() {
    let partial =
        TestDatabase::provision("mysql_schema_rejects_partial_and_extra_objects", "partial");
    let extra = TestDatabase::provision("mysql_schema_rejects_partial_and_extra_objects", "extra");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");

    apply_schema_mutation(
        &runtime,
        &partial.name,
        MysqlSchemaMutation::CreatePartialMetaTable,
        Duration::from_secs(4),
    )
    .await
    .expect("create partial schema");
    assert_open_corruption(&runtime, &partial.name, CLUSTER_ID).await;

    let store = open_store(&runtime, &extra.name, CLUSTER_ID, 4_000)
        .await
        .expect("bootstrap extra-object database");
    drop(store);
    apply_schema_mutation(
        &runtime,
        &extra.name,
        MysqlSchemaMutation::CreateExtraTable,
        Duration::from_secs(4),
    )
    .await
    .expect("create extra object");
    assert_open_corruption(&runtime, &extra.name, CLUSTER_ID).await;

    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_rejects_engine_row_format_column_and_index_drift() {
    let cases = [
        ("engine", MysqlSchemaMutation::DriftEngine),
        ("row_format", MysqlSchemaMutation::DriftRowFormat),
        ("column", MysqlSchemaMutation::DriftColumn),
        ("index", MysqlSchemaMutation::DriftIndex),
    ];
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let mut databases = Vec::new();

    for (suffix, mutation) in cases {
        let database = TestDatabase::provision(
            "mysql_schema_rejects_engine_row_format_column_and_index_drift",
            suffix,
        );
        let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
            .await
            .expect("bootstrap drift database");
        drop(store);
        apply_schema_mutation(&runtime, &database.name, mutation, Duration::from_secs(4))
            .await
            .expect("apply schema drift");
        assert_open_corruption(&runtime, &database.name, CLUSTER_ID).await;
        databases.push(database);
    }

    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
    drop(databases);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_rejects_missing_malformed_older_and_newer_meta() {
    let cases = [
        ("missing", MysqlSchemaMutation::DeleteSchemaVersion),
        ("malformed", MysqlSchemaMutation::MalformedSchemaVersion),
        ("older", MysqlSchemaMutation::OlderSchemaVersion),
        ("newer", MysqlSchemaMutation::NewerSchemaVersion),
    ];
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let mut databases = Vec::new();

    for (suffix, mutation) in cases {
        let database = TestDatabase::provision(
            "mysql_schema_rejects_missing_malformed_older_and_newer_meta",
            suffix,
        );
        let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
            .await
            .expect("bootstrap meta drift database");
        drop(store);
        apply_schema_mutation(&runtime, &database.name, mutation, Duration::from_secs(4))
            .await
            .expect("apply meta drift");
        assert_open_corruption(&runtime, &database.name, CLUSTER_ID).await;
        databases.push(database);
    }

    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
    drop(databases);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_advisory_lock_name_is_hashed_and_at_most_64_bytes() {
    let database = TestDatabase::provision(
        "mysql_schema_advisory_lock_name_is_hashed_and_at_most_64_bytes",
        "lock_name",
    );
    let lock_name = advisory_lock_name(&database.name);
    let digest = Sha256::digest(database.name.as_bytes());

    assert_eq!(
        lock_name,
        format!("novarocks-ss3-{}", hex::encode(&digest[..24]))
    );
    assert_eq!(lock_name.len(), 62);
    assert!(lock_name.len() <= 64);
    assert!(!lock_name.contains(&database.name));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_schema_advisory_lock_timeout_and_release_are_deterministic() {
    let database = TestDatabase::provision(
        "mysql_schema_advisory_lock_timeout_and_release_are_deterministic",
        "lock",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let held = acquire_schema_advisory_lock(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("hold schema advisory lock");

    let error = match open_store(&runtime, &database.name, CLUSTER_ID, 100).await {
        Ok(_) => panic!("held advisory lock must bound open"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded);
    held.release(Duration::from_secs(4))
        .await
        .expect("release schema advisory lock");
    assert!(
        is_schema_advisory_lock_free(&runtime, &database.name, Duration::from_secs(4))
            .await
            .expect("lock free after explicit release")
    );

    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open after lock release");
    drop(store);
    let mismatch = match open_store(&runtime, &database.name, "different-cluster", 4_000).await {
        Ok(_) => panic!("identity validation failure must fail closed"),
        Err(error) => error,
    };
    assert_eq!(mismatch.kind(), StateStoreErrorKind::InvalidConfiguration);
    assert!(
        is_schema_advisory_lock_free(&runtime, &database.name, Duration::from_secs(4))
            .await
            .expect("lock free after validation failure")
    );

    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_never_creates_database_alters_or_drops_objects() {
    let partial = TestDatabase::provision(
        "mysql_schema_never_creates_database_alters_or_drops_objects",
        "partial",
    );
    let drift = TestDatabase::provision(
        "mysql_schema_never_creates_database_alters_or_drops_objects",
        "drift",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");

    let missing_database = format!(
        "novarocks_ss3_missing_{}",
        hex::encode(&Sha256::digest(partial.name.as_bytes())[..8])
    );
    let missing = match open_store(&runtime, &missing_database, CLUSTER_ID, 1_000).await {
        Ok(_) => panic!("provider must not create a missing database"),
        Err(error) => error,
    };
    assert_eq!(missing.kind(), StateStoreErrorKind::InvalidConfiguration);

    apply_schema_mutation(
        &runtime,
        &partial.name,
        MysqlSchemaMutation::CreatePartialMetaTable,
        Duration::from_secs(4),
    )
    .await
    .expect("create partial schema");
    let partial_before = schema_snapshot(&runtime, &partial.name, Duration::from_secs(4))
        .await
        .expect("partial snapshot before open");
    assert_open_corruption(&runtime, &partial.name, CLUSTER_ID).await;
    let partial_after = schema_snapshot(&runtime, &partial.name, Duration::from_secs(4))
        .await
        .expect("partial snapshot after open");
    assert_eq!(partial_after, partial_before);

    let store = open_store(&runtime, &drift.name, CLUSTER_ID, 4_000)
        .await
        .expect("bootstrap drift database");
    drop(store);
    apply_schema_mutation(
        &runtime,
        &drift.name,
        MysqlSchemaMutation::DriftEngine,
        Duration::from_secs(4),
    )
    .await
    .expect("drift engine");
    let drift_before = schema_snapshot(&runtime, &drift.name, Duration::from_secs(4))
        .await
        .expect("drift snapshot before open");
    assert_open_corruption(&runtime, &drift.name, CLUSTER_ID).await;
    let drift_after = schema_snapshot(&runtime, &drift.name, Duration::from_secs(4))
        .await
        .expect("drift snapshot after open");
    assert_eq!(drift_after, drift_before);

    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_rejects_cluster_id_beyond_meta_limit_before_ddl() {
    let boundary = TestDatabase::provision(
        "mysql_schema_rejects_cluster_id_beyond_meta_limit_before_ddl",
        "boundary",
    );
    let oversized = TestDatabase::provision(
        "mysql_schema_rejects_cluster_id_beyond_meta_limit_before_ddl",
        "oversized",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");

    let boundary_cluster_id = "c".repeat(4096);
    let store = open_store(&runtime, &boundary.name, &boundary_cluster_id, 4_000)
        .await
        .expect("4096-byte cluster identity must fit meta value");
    drop(store);

    let oversized_cluster_id = "c".repeat(4097);
    let error = match open_store(&runtime, &oversized.name, &oversized_cluster_id, 4_000).await {
        Ok(_) => panic!("4097-byte cluster identity must fail before DDL"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    assert_eq!(
        error.to_string(),
        "InvalidConfiguration: MySQL state store configuration is invalid"
    );
    let snapshot = schema_snapshot(&runtime, &oversized.name, Duration::from_secs(4))
        .await
        .expect("oversized cluster inventory snapshot");
    assert!(snapshot.tables.is_empty());
    assert!(snapshot.views.is_empty());
    assert!(snapshot.triggers.is_empty());
    assert!(snapshot.meta_keys.is_empty());

    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_store_readiness_validates_inventory_identity_and_transactions() {
    let database = TestDatabase::provision(
        "mysql_store_readiness_validates_inventory_identity_and_transactions",
        "ready",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");

    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open ready store");
    assert_eq!(
        store.metrics_snapshot().provider,
        MYSQL_STATE_STORE_PROVIDER_ID
    );
    assert_eq!(
        store.identity().await.expect("ready identity").cluster_id,
        CLUSTER_ID
    );
    let readiness =
        store_readiness_snapshot(&runtime, &database.name, CLUSTER_ID, Duration::from_secs(4))
            .await
            .expect("transaction readiness");
    assert!(readiness.read_only_started_and_rolled_back);
    assert!(readiness.write_started_and_rolled_back);
    assert!(
        schema_timeout_connection_is_destroyed(
            &runtime,
            &database.name,
            Duration::from_millis(100),
            Duration::from_secs(4),
        )
        .await
        .expect("schema timeout disposition")
    );

    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_store_readiness_rejects_schema_or_identity_drift_after_pool_checkout() {
    let schema_database = TestDatabase::provision(
        "mysql_store_readiness_rejects_schema_or_identity_drift_after_pool_checkout",
        "schema",
    );
    let identity_database = TestDatabase::provision(
        "mysql_store_readiness_rejects_schema_or_identity_drift_after_pool_checkout",
        "identity",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");

    active_readiness(&runtime, &schema_database.name, Duration::from_secs(4))
        .await
        .expect("checkout schema pool before bootstrap");
    let schema_store = open_store(&runtime, &schema_database.name, CLUSTER_ID, 4_000)
        .await
        .expect("bootstrap schema database");
    drop(schema_store);
    apply_schema_mutation(
        &runtime,
        &schema_database.name,
        MysqlSchemaMutation::DriftIndex,
        Duration::from_secs(4),
    )
    .await
    .expect("drift schema after pool checkout");
    assert_open_corruption(&runtime, &schema_database.name, CLUSTER_ID).await;

    active_readiness(&runtime, &identity_database.name, Duration::from_secs(4))
        .await
        .expect("checkout identity pool before bootstrap");
    let identity_store = open_store(&runtime, &identity_database.name, CLUSTER_ID, 4_000)
        .await
        .expect("bootstrap identity database");
    drop(identity_store);
    let mismatch = match open_store(
        &runtime,
        &identity_database.name,
        "different-cluster",
        4_000,
    )
    .await
    {
        Ok(_) => panic!("identity drift after checkout must fail"),
        Err(error) => error,
    };
    assert_eq!(mismatch.kind(), StateStoreErrorKind::InvalidConfiguration);

    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_snapshot_repeatable_read() {
    let database = TestDatabase::provision("mysql_shared_snapshot_repeatable_read", "snapshot");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let factory = shared_factory(store);

    state_store_conformance::snapshot_repeatable_read(&factory).await;

    drop(factory);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_read_abort_uses_explicit_rollback() {
    let database = TestDatabase::provision("mysql_read_abort_uses_explicit_rollback", "rollback");
    let mut client = fixture_client_config();
    client.pool_max = 1;
    let mut runtime = MysqlProviderTestHarness::boot(client).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let rollback_count = MysqlTransactionTestApi::explicit_rollback_count();
    let reader = store.begin_read().await.expect("begin read transaction");

    reader.abort().await.expect("abort read transaction");

    assert_eq!(
        MysqlTransactionTestApi::explicit_rollback_count(),
        rollback_count + 1
    );
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_read_get_preserves_arbitrary_binary_payload() {
    let database = TestDatabase::provision(
        "mysql_read_get_preserves_arbitrary_binary_payload",
        "binary",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let item = key(Bytes::from_static(&[0x00, 0xff, 0x7f, 0x80]));
    let payload = value(Bytes::from_static(&[0xff, 0x00, 0x80, 0x7f]));
    let mut writer = store
        .begin_write(transaction_id(), "binary payload seed")
        .await
        .expect("begin binary payload seed");
    writer
        .put(item.clone(), payload.clone(), Precondition::Any)
        .await
        .expect("stage binary payload");
    assert_committed(writer.commit().await);

    let mut reader = store.begin_read().await.expect("begin binary read");
    let record = reader
        .get(&item)
        .await
        .expect("read binary payload")
        .expect("binary payload record");
    assert_eq!(record.key, item);
    assert_eq!(record.value, payload);
    reader.abort().await.expect("abort binary read");

    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_forward_reverse_pages() {
    let database = TestDatabase::provision("mysql_shared_forward_reverse_pages", "pages");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let factory = shared_factory(store);

    state_store_conformance::forward_reverse_pages(&factory).await;

    drop(factory);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_range_continuation_stays_in_one_snapshot_and_binds_request() {
    let database = TestDatabase::provision(
        "mysql_range_continuation_stays_in_one_snapshot_and_binds_request",
        "continuation",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let range = KeyRange::new(key([0x31, 0x00].to_vec()), key([0x31, 0xff].to_vec()))
        .expect("bounded range");
    let mut seed = store
        .begin_write(transaction_id(), "pagination seed")
        .await
        .expect("begin pagination seed");
    for suffix in 1_u8..=3 {
        seed.put(
            key([0x31, suffix].to_vec()),
            value([suffix].to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage pagination seed");
    }
    assert_committed(seed.commit().await);
    let request = RangeRequest {
        range: range.clone(),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let mut reader = store.begin_read().await.expect("begin paginated snapshot");
    let first = reader.range(&request).await.expect("first snapshot page");
    let continuation = first.continuation.expect("first page continuation");
    let mut concurrent = store
        .begin_write(transaction_id(), "concurrent pagination insert")
        .await
        .expect("begin concurrent insert");
    concurrent
        .put(
            key([0x31, 0x04].to_vec()),
            value([0x04].to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage concurrent insert");
    assert_committed(concurrent.commit().await);

    let second = reader
        .range(&RangeRequest {
            continuation: Some(continuation.clone()),
            ..request.clone()
        })
        .await
        .expect("second snapshot page");
    assert_eq!(second.records[0].key.as_bytes(), &[0x31, 0x02]);
    let wrong = RangeRequest {
        direction: Direction::Reverse,
        continuation: Some(continuation),
        ..request
    };
    assert_eq!(
        reader
            .range(&wrong)
            .await
            .expect_err("continuation must bind direction")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    reader.abort().await.expect("abort paginated snapshot");

    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_range_decodes_extra_row_before_forward_and_reverse_pagination() {
    let database = TestDatabase::provision(
        "mysql_range_decodes_extra_row_before_forward_and_reverse_pagination",
        "extra_row",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let forward_valid = key(Bytes::from_static(b"extra-forward-a"));
    let forward_malformed = b"extra-forward-b";
    let reverse_malformed = b"extra-reverse-a";
    let reverse_valid = key(Bytes::from_static(b"extra-reverse-b"));
    let mut seed = store
        .begin_write(transaction_id(), "extra row corruption seed")
        .await
        .expect("begin extra row seed");
    seed.put(
        forward_valid,
        value(Bytes::from_static(b"forward-valid")),
        Precondition::Any,
    )
    .await
    .expect("stage forward valid row");
    seed.put(
        reverse_valid,
        value(Bytes::from_static(b"reverse-valid")),
        Precondition::Any,
    )
    .await
    .expect("stage reverse valid row");
    assert_committed(seed.commit().await);
    MysqlTransactionTestApi::insert_malformed_kv_row(
        &runtime,
        &database.name,
        forward_malformed,
        Duration::from_secs(4),
    )
    .await
    .expect("insert forward malformed extra row");
    MysqlTransactionTestApi::insert_malformed_kv_row(
        &runtime,
        &database.name,
        reverse_malformed,
        Duration::from_secs(4),
    )
    .await
    .expect("insert reverse malformed extra row");

    let mut reader = store.begin_read().await.expect("begin extra row reader");
    let forward = RangeRequest {
        range: KeyRange::new(
            key(Bytes::from_static(b"extra-forward-")),
            key(Bytes::from_static(b"extra-forward-z")),
        )
        .expect("forward extra row range"),
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    assert_eq!(
        reader
            .range(&forward)
            .await
            .expect_err("forward malformed extra row must fail")
            .kind(),
        StateStoreErrorKind::Corruption
    );
    let reverse = RangeRequest {
        range: KeyRange::new(
            key(Bytes::from_static(b"extra-reverse-")),
            key(Bytes::from_static(b"extra-reverse-z")),
        )
        .expect("reverse extra row range"),
        direction: Direction::Reverse,
        page_size: 1,
        continuation: None,
    };
    assert_eq!(
        reader
            .range(&reverse)
            .await
            .expect_err("reverse malformed extra row must fail")
            .kind(),
        StateStoreErrorKind::Corruption
    );
    reader.abort().await.expect("abort extra row reader");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_read_continuation_rejects_cross_transaction_and_cross_store_before_io() {
    let first_database = TestDatabase::provision(
        "mysql_read_continuation_rejects_cross_transaction_and_cross_store_before_io",
        "first",
    );
    let second_database = TestDatabase::provision(
        "mysql_read_continuation_rejects_cross_transaction_and_cross_store_before_io",
        "second",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let first_store = open_store(&runtime, &first_database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open first MySQL store");
    let second_store = open_store(&runtime, &second_database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open second MySQL store");
    let range = KeyRange::new(key([0x61, 0].to_vec()), key([0x61, 0xff].to_vec()))
        .expect("continuation ownership range");
    let mut seed = first_store
        .begin_write(transaction_id(), "read continuation ownership seed")
        .await
        .expect("begin continuation seed");
    for suffix in 1_u8..=3 {
        seed.put(
            key([0x61, suffix].to_vec()),
            value([suffix].to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage continuation seed");
    }
    assert_committed(seed.commit().await);
    let request = RangeRequest {
        range,
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let mut owner = first_store
        .begin_read()
        .await
        .expect("begin continuation owner");
    let token = owner
        .range(&request)
        .await
        .expect("owner first page")
        .continuation
        .expect("owner continuation");
    let retry = RangeRequest {
        continuation: Some(token.clone()),
        ..request.clone()
    };
    let first_retry = owner.range(&retry).await.expect("first same-owner retry");
    let second_retry = owner.range(&retry).await.expect("repeat same-owner retry");
    assert_eq!(first_retry, second_retry);

    let mut other_transaction = first_store
        .begin_read()
        .await
        .expect("begin other transaction");
    let before_transaction = MysqlStatementTestApi::statement_count();
    assert_eq!(
        other_transaction
            .range(&retry)
            .await
            .expect_err("cross-transaction continuation must reject")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert_eq!(MysqlStatementTestApi::statement_count(), before_transaction);

    let mut other_store = second_store
        .begin_read()
        .await
        .expect("begin other-store transaction");
    let before_store = MysqlStatementTestApi::statement_count();
    assert_eq!(
        other_store
            .range(&retry)
            .await
            .expect_err("cross-store continuation must reject")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert_eq!(MysqlStatementTestApi::statement_count(), before_store);

    let forged = RangeRequest {
        continuation: Some(
            ContinuationToken::try_from(Bytes::from_static(b"forged"))
                .expect("opaque continuation"),
        ),
        ..request
    };
    let before_forged = MysqlStatementTestApi::statement_count();
    assert_eq!(
        owner
            .range(&forged)
            .await
            .expect_err("forged continuation must reject")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert_eq!(MysqlStatementTestApi::statement_count(), before_forged);
    owner.abort().await.expect("abort owner");
    other_transaction
        .abort()
        .await
        .expect("abort other transaction");
    other_store.abort().await.expect("abort other store");
    drop(first_store);
    drop(second_store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_write_continuation_rejects_cross_transaction_and_cross_store_before_io() {
    let first_database = TestDatabase::provision(
        "mysql_write_continuation_rejects_cross_transaction_and_cross_store_before_io",
        "first",
    );
    let second_database = TestDatabase::provision(
        "mysql_write_continuation_rejects_cross_transaction_and_cross_store_before_io",
        "second",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let first_store = open_store(&runtime, &first_database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open first MySQL store");
    let second_store = open_store(&runtime, &second_database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open second MySQL store");
    let range = KeyRange::new(key([0x62, 0].to_vec()), key([0x62, 0xff].to_vec()))
        .expect("write continuation ownership range");
    let mut seed = first_store
        .begin_write(transaction_id(), "write continuation ownership seed")
        .await
        .expect("begin write continuation seed");
    for suffix in 1_u8..=3 {
        seed.put(
            key([0x62, suffix].to_vec()),
            value([suffix].to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage write continuation seed");
    }
    assert_committed(seed.commit().await);
    let request = RangeRequest {
        range,
        direction: Direction::Forward,
        page_size: 1,
        continuation: None,
    };
    let mut owner = first_store
        .begin_write(transaction_id(), "write continuation owner")
        .await
        .expect("begin write continuation owner");
    let token = owner
        .range(&request)
        .await
        .expect("write owner first page")
        .continuation
        .expect("write owner continuation");
    let retry = RangeRequest {
        continuation: Some(token),
        ..request
    };
    owner.range(&retry).await.expect("same write actor retry");

    let mut other_transaction = first_store
        .begin_write(transaction_id(), "other write transaction")
        .await
        .expect("begin other write transaction");
    let before_transaction = MysqlStatementTestApi::statement_count();
    assert_eq!(
        other_transaction
            .range(&retry)
            .await
            .expect_err("write cross-transaction continuation must reject")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert_eq!(MysqlStatementTestApi::statement_count(), before_transaction);

    let mut other_store = second_store
        .begin_write(transaction_id(), "other store write transaction")
        .await
        .expect("begin other-store write transaction");
    let before_store = MysqlStatementTestApi::statement_count();
    assert_eq!(
        other_store
            .range(&retry)
            .await
            .expect_err("write cross-store continuation must reject")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert_eq!(MysqlStatementTestApi::statement_count(), before_store);
    owner.abort().await.expect("abort write owner");
    other_transaction
        .abort()
        .await
        .expect("abort other write transaction");
    other_store.abort().await.expect("abort other store write");
    drop(first_store);
    drop(second_store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_limits_before_io() {
    let database = TestDatabase::provision("mysql_shared_limits_before_io", "limits");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let max_transaction_bytes = MysqlWriteTestApi::transaction_envelope_bytes()
        + 4 * MysqlWriteTestApi::put_accounted_bytes(&[11, 0xfe, 1], &[0; 32], &Precondition::Any)
            .expect("physical conformance put budget");
    let config = transaction_store_config(
        &database.name,
        MysqlTestLimitOverrides {
            max_key_bytes: Some(16),
            max_value_bytes: Some(32),
            max_page_size: Some(2),
            max_transaction_operations: Some(4),
            max_transaction_bytes: Some(max_transaction_bytes),
            transaction_deadline_ms: Some(4_000),
            runner_max_attempts: Some(2),
        },
    );
    let store = open_mysql_store(&runtime, config)
        .await
        .expect("open limited MySQL store");
    let factory = shared_factory(store);

    state_store_conformance::limits_before_io(&factory).await;

    drop(factory);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_arbitrary_binary_payloads() {
    let database = TestDatabase::provision("mysql_shared_arbitrary_binary_payloads", "binary");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let factory = shared_factory(store);

    state_store_conformance::arbitrary_binary_payloads(&factory).await;

    drop(factory);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_read_rejects_malformed_persisted_rows_as_corruption() {
    let database = TestDatabase::provision(
        "mysql_read_rejects_malformed_persisted_rows_as_corruption",
        "corruption",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let item = key(Bytes::from_static(b"malformed-row"));
    MysqlTransactionTestApi::insert_malformed_kv_row(
        &runtime,
        &database.name,
        item.as_bytes(),
        Duration::from_secs(4),
    )
    .await
    .expect("insert malformed persisted row");

    let mut reader = store.begin_read().await.expect("begin corruption read");
    assert_eq!(
        reader
            .get(&item)
            .await
            .expect_err("malformed row must fail closed")
            .kind(),
        StateStoreErrorKind::Corruption
    );
    reader.abort().await.expect("abort corruption read");

    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_write_overlay_is_ordered_and_read_your_writes() {
    let database = TestDatabase::provision(
        "mysql_write_overlay_is_ordered_and_read_your_writes",
        "overlay",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let keys = (1_u8..=5)
        .map(|suffix| key([0x43, suffix].to_vec()))
        .collect::<Vec<_>>();
    let mut seed = store
        .begin_write(transaction_id(), "ordered overlay seed")
        .await
        .expect("begin ordered overlay seed");
    for suffix in 1_u8..=4 {
        seed.put(
            keys[usize::from(suffix - 1)].clone(),
            value([suffix].to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage ordered overlay seed");
    }
    assert_committed(seed.commit().await);
    let mut writer = store
        .begin_write(transaction_id(), "ordered overlay")
        .await
        .expect("begin ordered overlay");
    writer
        .put(
            keys[1].clone(),
            value(Bytes::from_static(b"intermediate")),
            Precondition::Any,
        )
        .await
        .expect("stage repeated key put");
    writer
        .delete(keys[1].clone(), Precondition::Any)
        .await
        .expect("stage repeated key delete");
    writer
        .put(
            keys[1].clone(),
            value(Bytes::from_static(b"final")),
            Precondition::Any,
        )
        .await
        .expect("stage repeated key final put");
    writer
        .delete(keys[2].clone(), Precondition::Any)
        .await
        .expect("stage base delete");
    writer
        .put(
            keys[4].clone(),
            value(Bytes::from_static(b"inserted")),
            Precondition::Absent,
        )
        .await
        .expect("stage overlay insert");
    assert_eq!(
        writer
            .get(&keys[1])
            .await
            .expect("read final overlay")
            .expect("final overlay record")
            .value
            .as_bytes(),
        b"final"
    );
    assert!(
        writer
            .get(&keys[2])
            .await
            .expect("read deleted overlay")
            .is_none()
    );
    let range = KeyRange::new(key([0x43, 0].to_vec()), key([0x43, 0xff].to_vec()))
        .expect("ordered overlay range");
    let forward_first_request = RangeRequest {
        range: range.clone(),
        direction: Direction::Forward,
        page_size: 2,
        continuation: None,
    };
    let forward_first = writer
        .range(&forward_first_request)
        .await
        .expect("read first forward overlay page");
    assert_eq!(
        forward_first
            .records
            .iter()
            .map(|record| record.key.clone())
            .collect::<Vec<_>>(),
        vec![keys[0].clone(), keys[1].clone()]
    );
    let forward_second = writer
        .range(&RangeRequest {
            continuation: forward_first.continuation,
            ..forward_first_request
        })
        .await
        .expect("read second forward overlay page");
    assert_eq!(
        forward_second
            .records
            .iter()
            .map(|record| record.key.clone())
            .collect::<Vec<_>>(),
        vec![keys[3].clone(), keys[4].clone()]
    );
    assert!(forward_second.continuation.is_none());
    let reverse_first_request = RangeRequest {
        range,
        direction: Direction::Reverse,
        page_size: 2,
        continuation: None,
    };
    let reverse_first = writer
        .range(&reverse_first_request)
        .await
        .expect("read first reverse overlay page");
    assert_eq!(
        reverse_first
            .records
            .iter()
            .map(|record| record.key.clone())
            .collect::<Vec<_>>(),
        vec![keys[4].clone(), keys[3].clone()]
    );
    let reverse_second = writer
        .range(&RangeRequest {
            continuation: reverse_first.continuation,
            ..reverse_first_request
        })
        .await
        .expect("read second reverse overlay page");
    assert_eq!(
        reverse_second
            .records
            .iter()
            .map(|record| record.key.clone())
            .collect::<Vec<_>>(),
        vec![keys[1].clone(), keys[0].clone()]
    );
    assert!(reverse_second.continuation.is_none());
    assert_eq!(
        writer
            .put(
                key([0x43, 6].to_vec()),
                value([6].to_vec()),
                Precondition::Any,
            )
            .await
            .expect_err("pagination must freeze later mutations")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    assert_committed(writer.commit().await);

    let mut reader = store.begin_read().await.expect("begin persisted read");
    assert_eq!(
        reader
            .get(&keys[1])
            .await
            .expect("read persisted final")
            .expect("persisted final record")
            .value
            .as_bytes(),
        b"final"
    );
    assert!(
        reader
            .get(&keys[2])
            .await
            .expect("read persisted delete")
            .is_none()
    );
    assert_eq!(
        reader
            .get(&keys[4])
            .await
            .expect("read persisted insert")
            .expect("persisted insert exists")
            .value
            .as_bytes(),
        b"inserted"
    );
    reader.abort().await.expect("abort persisted read");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_write_provisional_versions_are_stable_and_operation_specific() {
    let database = TestDatabase::provision(
        "mysql_write_provisional_versions_are_stable_and_operation_specific",
        "versions",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let first_key = key(Bytes::from_static(b"provisional/first"));
    let second_key = key(Bytes::from_static(b"provisional/second"));
    let mut writer = store
        .begin_write(transaction_id(), "provisional versions")
        .await
        .expect("begin provisional versions");
    writer
        .put(
            first_key.clone(),
            value(Bytes::from_static(b"one")),
            Precondition::Any,
        )
        .await
        .expect("stage first provisional");
    let first = writer
        .get(&first_key)
        .await
        .expect("read first provisional")
        .expect("first provisional record");
    assert_eq!(
        writer
            .get(&first_key)
            .await
            .expect("repeat first provisional")
            .expect("repeat first provisional record")
            .version,
        first.version
    );
    writer
        .put(
            second_key.clone(),
            value(Bytes::from_static(b"two")),
            Precondition::Any,
        )
        .await
        .expect("stage second provisional");
    let second = writer
        .get(&second_key)
        .await
        .expect("read second provisional")
        .expect("second provisional record");
    assert_ne!(first.version, second.version);
    writer.abort().await.expect("abort provisional versions");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_write_freezes_mutation_after_range_pagination() {
    let database = TestDatabase::provision(
        "mysql_write_freezes_mutation_after_range_pagination",
        "freeze",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let mut seed = store
        .begin_write(transaction_id(), "freeze seed")
        .await
        .expect("begin freeze seed");
    for suffix in 1_u8..=2 {
        seed.put(
            key([0x42, suffix].to_vec()),
            value([suffix].to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage freeze seed");
    }
    assert_committed(seed.commit().await);
    let mut writer = store
        .begin_write(transaction_id(), "range freeze")
        .await
        .expect("begin range freeze");
    let page = writer
        .range(&RangeRequest {
            range: KeyRange::new(key([0x42, 0].to_vec()), key([0x42, 0xff].to_vec()))
                .expect("freeze range"),
            direction: Direction::Forward,
            page_size: 1,
            continuation: None,
        })
        .await
        .expect("read paginated write range");
    assert!(page.continuation.is_some());
    assert_eq!(
        writer
            .put(
                key([0x42, 3].to_vec()),
                value([3].to_vec()),
                Precondition::Any,
            )
            .await
            .expect_err("mutation must freeze after pagination")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    writer.abort().await.expect("abort frozen writer");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_write_abort_rolls_back_without_durable_artifacts() {
    let database = TestDatabase::provision(
        "mysql_write_abort_rolls_back_without_durable_artifacts",
        "abort",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let item = key(Bytes::from_static(b"abort/no-artifact"));
    let mut writer = store
        .begin_write(transaction_id(), "abort no artifact")
        .await
        .expect("begin abort writer");
    writer
        .put(
            item.clone(),
            value(Bytes::from_static(b"not-durable")),
            Precondition::Any,
        )
        .await
        .expect("stage aborted put");
    writer.abort().await.expect("abort writer");
    let mut reader = store.begin_read().await.expect("begin post-abort read");
    assert!(reader.get(&item).await.expect("post-abort get").is_none());
    reader.abort().await.expect("abort post-abort read");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_write_budget_accepts_and_rejects_exact_boundaries() {
    let database = TestDatabase::provision(
        "mysql_write_budget_accepts_and_rejects_exact_boundaries",
        "budget",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_mysql_store(
        &runtime,
        transaction_store_config(
            &database.name,
            MysqlTestLimitOverrides {
                max_transaction_operations: Some(2),
                max_transaction_bytes: Some(
                    MysqlWriteTestApi::transaction_envelope_bytes()
                        + MysqlWriteTestApi::delete_accounted_bytes(b"a", &Precondition::Any)
                            .expect("first delete accounting")
                        + MysqlWriteTestApi::delete_accounted_bytes(b"b", &Precondition::Absent)
                            .expect("second delete accounting"),
                ),
                transaction_deadline_ms: Some(4_000),
                ..MysqlTestLimitOverrides::default()
            },
        ),
    )
    .await
    .expect("open budgeted MySQL store");
    let mut writer = store
        .begin_write(transaction_id(), "exact budget")
        .await
        .expect("begin exact budget");
    writer
        .delete(key(Bytes::from_static(b"a")), Precondition::Any)
        .await
        .expect("stage first exact-budget delete");
    writer
        .delete(key(Bytes::from_static(b"b")), Precondition::Absent)
        .await
        .expect("stage second exact-budget mutation");
    assert_eq!(
        writer
            .put(
                key(Bytes::from_static(b"c")),
                value(Bytes::from_static(b"x")),
                Precondition::Any,
            )
            .await
            .expect_err("reject operation beyond exact budget")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    assert_committed(writer.commit().await);
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_write_budget_rejects_fixed_envelope_before_io() {
    let database = TestDatabase::provision(
        "mysql_write_budget_rejects_fixed_envelope_before_io",
        "envelope",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let envelope = MysqlWriteTestApi::transaction_envelope_bytes();
    let under = open_mysql_store(
        &runtime,
        transaction_store_config(
            &database.name,
            MysqlTestLimitOverrides {
                max_transaction_operations: Some(100),
                max_transaction_bytes: Some(envelope - 1),
                transaction_deadline_ms: Some(4_000),
                ..MysqlTestLimitOverrides::default()
            },
        ),
    )
    .await
    .expect("open under-envelope store");
    let before = MysqlStatementTestApi::statement_count();
    let error = match under
        .begin_write(transaction_id(), "under fixed envelope")
        .await
    {
        Ok(_) => panic!("fixed envelope minus one must reject"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StateStoreErrorKind::LimitExceeded);
    assert_eq!(MysqlStatementTestApi::statement_count(), before);
    drop(under);

    let exact = open_mysql_store(
        &runtime,
        transaction_store_config(
            &database.name,
            MysqlTestLimitOverrides {
                max_transaction_operations: Some(100),
                max_transaction_bytes: Some(envelope),
                transaction_deadline_ms: Some(4_000),
                ..MysqlTestLimitOverrides::default()
            },
        ),
    )
    .await
    .expect("open exact-envelope store");
    let writer = exact
        .begin_write(transaction_id(), "exact fixed envelope")
        .await
        .expect("exact fixed envelope begins");
    assert_committed(writer.commit().await);
    drop(exact);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_write_budget_accepts_exact_mutation_and_rejects_plus_one_before_io() {
    let database = TestDatabase::provision(
        "mysql_write_budget_accepts_exact_mutation_and_rejects_plus_one_before_io",
        "mutation_budget",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let item = b"exact-mutation";
    let exact_value = vec![0x41; 16];
    let exact_budget = MysqlWriteTestApi::transaction_envelope_bytes()
        + MysqlWriteTestApi::put_accounted_bytes(item, &exact_value, &Precondition::Any)
            .expect("exact put accounting");

    for (value_bytes, should_fit) in [(15_usize, true), (16, true), (17, false)] {
        let store = open_mysql_store(
            &runtime,
            transaction_store_config(
                &database.name,
                MysqlTestLimitOverrides {
                    max_transaction_operations: Some(100),
                    max_transaction_bytes: Some(exact_budget),
                    transaction_deadline_ms: Some(4_000),
                    ..MysqlTestLimitOverrides::default()
                },
            ),
        )
        .await
        .expect("open exact-mutation store");
        let mut writer = store
            .begin_write(transaction_id(), "exact mutation boundary")
            .await
            .expect("begin exact-mutation writer");
        let before = MysqlStatementTestApi::statement_count();
        let result = writer
            .put(
                key(item.to_vec()),
                value(vec![0x41; value_bytes]),
                Precondition::Any,
            )
            .await;
        assert_eq!(MysqlStatementTestApi::statement_count(), before);
        if should_fit {
            result.expect("mutation at or below exact boundary");
            assert_committed(writer.commit().await);
        } else {
            assert_eq!(
                result.expect_err("mutation plus one must reject").kind(),
                StateStoreErrorKind::LimitExceeded
            );
            writer.abort().await.expect("abort rejected writer");
        }
        drop(store);
    }
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_preconditions_stage_successfully_and_fail_only_at_commit() {
    let database = TestDatabase::provision(
        "mysql_preconditions_stage_successfully_and_fail_only_at_commit",
        "stage",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let present = key(Bytes::from_static(b"precondition/present"));
    let missing = key(Bytes::from_static(b"precondition/missing"));
    let mut seed = store
        .begin_write(transaction_id(), "precondition seed")
        .await
        .expect("begin precondition seed");
    seed.put(
        present.clone(),
        value(Bytes::from_static(b"seed")),
        Precondition::Any,
    )
    .await
    .expect("stage precondition seed");
    assert_committed(seed.commit().await);
    let stale = novarocks_spi::state_store::VersionToken::try_from(Bytes::from_static(b"stale"))
        .expect("stale version");

    for (item, precondition) in [
        (present.clone(), Precondition::Absent),
        (missing.clone(), Precondition::Present),
        (present.clone(), Precondition::Version(stale.clone())),
        (missing.clone(), Precondition::Version(stale)),
    ] {
        let mut writer = store
            .begin_write(transaction_id(), "stage-only precondition")
            .await
            .expect("begin stage-only precondition");
        writer
            .put(
                item,
                value(Bytes::from_static(b"must-conflict")),
                precondition,
            )
            .await
            .expect("stale precondition must stage successfully");
        assert!(
            matches!(writer.commit().await, CommitOutcome::Conflict(_)),
            "stale precondition must conflict only at commit"
        );
    }
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_preconditions() {
    let database = TestDatabase::provision("mysql_shared_preconditions", "preconditions");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let factory = shared_factory(store);

    state_store_conformance::preconditions(&factory).await;

    drop(factory);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_same_key_conflict_first_commit_does_not_wait_for_second_reader() {
    let database = TestDatabase::provision(
        "mysql_shared_same_key_conflict_first_commit_does_not_wait_for_second_reader",
        "same_key",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let factory = shared_factory(store);

    tokio::time::timeout(
        Duration::from_secs(4),
        state_store_conformance::same_key_conflict(&factory),
    )
    .await
    .expect("first same-key commit must not wait for second reader");

    drop(factory);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_write_skew_conflict_first_commit_does_not_wait_for_second_reader() {
    let database = TestDatabase::provision(
        "mysql_shared_write_skew_conflict_first_commit_does_not_wait_for_second_reader",
        "write_skew",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let factory = shared_factory(store);

    tokio::time::timeout(
        Duration::from_secs(4),
        state_store_conformance::write_skew_conflict(&factory),
    )
    .await
    .expect("first write-skew commit must not wait for second reader");

    drop(factory);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_range_phantom_conflict_first_commit_does_not_wait_for_second_reader() {
    let database = TestDatabase::provision(
        "mysql_shared_range_phantom_conflict_first_commit_does_not_wait_for_second_reader",
        "phantom",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let factory = shared_factory(store);

    tokio::time::timeout(
        Duration::from_secs(4),
        state_store_conformance::range_phantom_conflict(&factory),
    )
    .await
    .expect("first phantom commit must not wait for second reader");

    drop(factory);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_point_occ_compares_presence_and_persisted_version_exactly() {
    let database = TestDatabase::provision(
        "mysql_point_occ_compares_presence_and_persisted_version_exactly",
        "point_occ",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let item = key(Bytes::from_static(b"point-occ"));
    let mut observer = store
        .begin_write(transaction_id(), "point observer")
        .await
        .expect("begin point observer");
    assert!(observer.get(&item).await.expect("observe absent").is_none());
    let mut concurrent = store
        .begin_write(transaction_id(), "point concurrent")
        .await
        .expect("begin point concurrent");
    concurrent
        .put(
            item.clone(),
            value(Bytes::from_static(b"present")),
            Precondition::Any,
        )
        .await
        .expect("stage concurrent presence");
    assert_committed(concurrent.commit().await);
    observer
        .put(
            item.clone(),
            value(Bytes::from_static(b"observer")),
            Precondition::Any,
        )
        .await
        .expect("stage observed key");
    assert!(matches!(
        observer.commit().await,
        CommitOutcome::Conflict(_)
    ));

    let mut version_observer = store
        .begin_write(transaction_id(), "version observer")
        .await
        .expect("begin version observer");
    version_observer
        .get(&item)
        .await
        .expect("observe persisted version");
    let mut update = store
        .begin_write(transaction_id(), "version update")
        .await
        .expect("begin version update");
    update
        .put(
            item.clone(),
            value(Bytes::from_static(b"updated")),
            Precondition::Any,
        )
        .await
        .expect("stage version update");
    assert_committed(update.commit().await);
    version_observer
        .put(
            item,
            value(Bytes::from_static(b"stale-observer")),
            Precondition::Any,
        )
        .await
        .expect("stage stale observer");
    assert!(matches!(
        version_observer.commit().await,
        CommitOutcome::Conflict(_)
    ));
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_range_observation_conflicts_on_any_revision_drift() {
    let database = TestDatabase::provision(
        "mysql_range_observation_conflicts_on_any_revision_drift",
        "range_drift",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let request = RangeRequest {
        range: KeyRange::new(key([0x51, 0].to_vec()), key([0x51, 0xff].to_vec()))
            .expect("range observation"),
        direction: Direction::Forward,
        page_size: 8,
        continuation: None,
    };
    let mut observer = store
        .begin_write(transaction_id(), "range observer")
        .await
        .expect("begin range observer");
    observer.range(&request).await.expect("observe range");
    let mut concurrent = store
        .begin_write(transaction_id(), "range concurrent")
        .await
        .expect("begin range concurrent");
    concurrent
        .put(
            key([0x51, 1].to_vec()),
            value([1].to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage range drift");
    assert_committed(concurrent.commit().await);
    observer
        .put(
            key([0x51, 2].to_vec()),
            value([2].to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage observer mutation");
    assert!(matches!(
        observer.commit().await,
        CommitOutcome::Conflict(_)
    ));
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_range_observation_conflicts_on_unrelated_out_of_range_commit() {
    let database = TestDatabase::provision(
        "mysql_range_observation_conflicts_on_unrelated_out_of_range_commit",
        "unrelated",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let mut observer = store
        .begin_write(transaction_id(), "unrelated range observer")
        .await
        .expect("begin unrelated observer");
    observer
        .range(&RangeRequest {
            range: KeyRange::new(key([0x52, 0].to_vec()), key([0x52, 0xff].to_vec()))
                .expect("unrelated range"),
            direction: Direction::Forward,
            page_size: 8,
            continuation: None,
        })
        .await
        .expect("observe unrelated range");
    let mut concurrent = store
        .begin_write(transaction_id(), "out of range commit")
        .await
        .expect("begin out of range commit");
    concurrent
        .put(
            key([0x53, 1].to_vec()),
            value([1].to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage out of range commit");
    assert_committed(concurrent.commit().await);
    observer
        .put(
            key([0x52, 1].to_vec()),
            value([1].to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage observed-range mutation");
    assert!(matches!(
        observer.commit().await,
        CommitOutcome::Conflict(_)
    ));
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_touched_keys_lock_in_stable_byte_order() {
    let database =
        TestDatabase::provision("mysql_touched_keys_lock_in_stable_byte_order", "lock_order");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let low = key(Bytes::from_static(b"lock/a"));
    let high = key(Bytes::from_static(b"lock/z"));
    let mut first = store
        .begin_write(transaction_id(), "stable locks first")
        .await
        .expect("begin stable locks first");
    first
        .put(
            high.clone(),
            value(Bytes::from_static(b"first-high")),
            Precondition::Any,
        )
        .await
        .expect("stage first high");
    first
        .put(
            low.clone(),
            value(Bytes::from_static(b"first-low")),
            Precondition::Any,
        )
        .await
        .expect("stage first low");
    let mut second = store
        .begin_write(transaction_id(), "stable locks second")
        .await
        .expect("begin stable locks second");
    #[cfg(feature = "state-store-test-hooks")]
    let expected_lock_order = vec![low.as_bytes().to_vec(), high.as_bytes().to_vec()];
    second
        .put(
            low.clone(),
            value(Bytes::from_static(b"second-low")),
            Precondition::Any,
        )
        .await
        .expect("stage second low");
    second
        .put(
            high.clone(),
            value(Bytes::from_static(b"second-high")),
            Precondition::Any,
        )
        .await
        .expect("stage second high");
    assert_committed(first.commit().await);
    #[cfg(feature = "state-store-test-hooks")]
    assert_eq!(
        MysqlOccTestApi::last_touched_lock_order(),
        expected_lock_order
    );
    assert_committed(second.commit().await);
    #[cfg(feature = "state-store-test-hooks")]
    assert_eq!(
        MysqlOccTestApi::last_touched_lock_order(),
        expected_lock_order
    );
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_commit_replays_multiple_preconditions_in_call_order() {
    let database = TestDatabase::provision(
        "mysql_commit_replays_multiple_preconditions_in_call_order",
        "replay",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let item = key(Bytes::from_static(b"ordered-preconditions"));
    let mut writer = store
        .begin_write(transaction_id(), "ordered preconditions")
        .await
        .expect("begin ordered preconditions");
    writer
        .put(
            item.clone(),
            value(Bytes::from_static(b"first")),
            Precondition::Absent,
        )
        .await
        .expect("stage absent put");
    let first_version = writer
        .get(&item)
        .await
        .expect("read first staged value")
        .expect("first staged record")
        .version;
    writer
        .delete(item.clone(), Precondition::Version(first_version))
        .await
        .expect("stage version delete");
    writer
        .put(
            item,
            value(Bytes::from_static(b"final")),
            Precondition::Absent,
        )
        .await
        .expect("stage final absent put");
    assert_committed(writer.commit().await);

    let guarded = key(Bytes::from_static(b"ordered-preconditions-guarded"));
    let mut seed = store
        .begin_write(transaction_id(), "ordered precondition failure seed")
        .await
        .expect("begin ordered failure seed");
    seed.put(
        guarded.clone(),
        value(Bytes::from_static(b"original")),
        Precondition::Any,
    )
    .await
    .expect("stage ordered failure seed");
    assert_committed(seed.commit().await);
    let mut reader = store
        .begin_read()
        .await
        .expect("begin original version read");
    let original = reader
        .get(&guarded)
        .await
        .expect("read original guarded value")
        .expect("guarded value exists");
    reader.abort().await.expect("abort original version read");
    let mut failing = store
        .begin_write(transaction_id(), "ordered intermediate failure")
        .await
        .expect("begin ordered intermediate failure");
    failing
        .put(
            guarded.clone(),
            value(Bytes::from_static(b"intermediate")),
            Precondition::Version(original.version.clone()),
        )
        .await
        .expect("stage valid first ordered mutation");
    failing
        .delete(
            guarded.clone(),
            Precondition::Version(original.version.clone()),
        )
        .await
        .expect("stage invalid intermediate ordered mutation");
    failing
        .put(
            guarded.clone(),
            value(Bytes::from_static(b"must-not-win")),
            Precondition::Any,
        )
        .await
        .expect("stage final mutation after invalid intermediate");
    assert!(matches!(
        failing.commit().await,
        CommitOutcome::Conflict(ref error)
            if error.kind() == StateStoreErrorKind::PreconditionFailed
    ));
    let mut reader = store
        .begin_read()
        .await
        .expect("begin failed order verification");
    assert_eq!(
        reader
            .get(&guarded)
            .await
            .expect("read guarded value after conflict")
            .expect("guarded value remains")
            .value,
        original.value
    );
    reader
        .abort()
        .await
        .expect("abort failed order verification");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_same_value_put_assigns_new_version_and_conflicts_stale_observer() {
    let database = TestDatabase::provision(
        "mysql_same_value_put_assigns_new_version_and_conflicts_stale_observer",
        "same_value",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let item = key(Bytes::from_static(b"same-value-version"));
    let payload = value(Bytes::from_static(b"unchanged"));
    let mut seed = store
        .begin_write(transaction_id(), "same value seed")
        .await
        .expect("begin same value seed");
    seed.put(item.clone(), payload.clone(), Precondition::Any)
        .await
        .expect("stage same value seed");
    assert_committed(seed.commit().await);
    let mut stale = store
        .begin_write(transaction_id(), "same value stale observer")
        .await
        .expect("begin stale observer");
    let original = stale
        .get(&item)
        .await
        .expect("read original same value")
        .expect("same value seed exists");
    stale
        .put(
            item.clone(),
            value(Bytes::from_static(b"stale-write")),
            Precondition::Version(original.version.clone()),
        )
        .await
        .expect("stage stale observer write");
    let mut same_value = store
        .begin_write(transaction_id(), "same value replacement")
        .await
        .expect("begin same value replacement");
    same_value
        .put(item.clone(), payload.clone(), Precondition::Any)
        .await
        .expect("stage same value replacement");
    assert_committed(same_value.commit().await);
    let mut reader = store.begin_read().await.expect("begin same value read");
    let replaced = reader
        .get(&item)
        .await
        .expect("read same value replacement")
        .expect("same value replacement exists");
    assert_eq!(replaced.value, payload);
    assert_ne!(replaced.version, original.version);
    reader.abort().await.expect("abort same value read");
    assert!(matches!(
        stale.commit().await,
        CommitOutcome::Conflict(ref error)
            if error.kind() == StateStoreErrorKind::Conflict
    ));
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_deadlock_1213_maps_to_conflict() {
    let database = TestDatabase::provision("mysql_deadlock_1213_maps_to_conflict", "deadlock");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    MysqlOccTestApi::deadlock_1213_maps_to_conflict(
        &runtime,
        &database.name,
        Duration::from_secs(8),
    )
    .await
    .expect("deadlock mapping");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_lock_timeout_1205_rolls_back_before_conflict() {
    let database = TestDatabase::provision(
        "mysql_lock_timeout_1205_rolls_back_before_conflict",
        "lock_timeout",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let locked_key = key(Bytes::from_static(b"public-1205-lock"));
    let holder = MysqlOccTestApi::hold_kv_lock(
        &runtime,
        &database.name,
        locked_key.as_bytes(),
        Duration::from_secs(8),
    )
    .await
    .expect("hold physical MySQL key lock");
    let mut writer = store
        .begin_write(transaction_id(), "public 1205 actor")
        .await
        .expect("begin public lock waiter");
    writer
        .put(
            locked_key.clone(),
            value(Bytes::from_static(b"must-not-persist")),
            Precondition::Any,
        )
        .await
        .expect("stage blocked public put");
    let outcome = writer.commit().await;
    assert!(
        matches!(
            outcome,
            CommitOutcome::Conflict(ref error)
                if error.kind() == StateStoreErrorKind::Conflict
        ),
        "{outcome:?}"
    );
    holder.release().await.expect("release physical key lock");
    let mut reader = store.begin_read().await.expect("begin verification read");
    assert!(
        reader
            .get(&locked_key)
            .await
            .expect("read timed-out public key")
            .is_none()
    );
    reader.abort().await.expect("abort verification read");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_statement_deadline_destroys_undrained_connection() {
    let database = TestDatabase::provision(
        "mysql_statement_deadline_destroys_undrained_connection",
        "statement_deadline",
    );
    let mut client = fixture_client_config();
    client.pool_max = 2;
    let mut runtime = MysqlProviderTestHarness::boot(client).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 400)
        .await
        .expect("open MySQL transaction store");
    let locked_key = key(Bytes::from_static(b"public-deadline-lock"));
    let holder = MysqlOccTestApi::hold_kv_lock(
        &runtime,
        &database.name,
        locked_key.as_bytes(),
        Duration::from_secs(8),
    )
    .await
    .expect("hold physical MySQL key lock");
    let mut writer = store
        .begin_write(transaction_id(), "public deadline actor")
        .await
        .expect("begin public deadline waiter");
    let actor_connection_id = MysqlStatementTestApi::last_write_actor_connection_id();
    assert_ne!(actor_connection_id, 0);
    writer
        .put(
            locked_key.clone(),
            value(Bytes::from_static(b"must-not-persist")),
            Precondition::Any,
        )
        .await
        .expect("stage deadline-blocked public put");
    let outcome = writer.commit().await;
    assert!(
        matches!(
            outcome,
            CommitOutcome::DefiniteFailure(ref error)
                if error.kind() == StateStoreErrorKind::DeadlineExceeded
        ),
        "{outcome:?}"
    );
    let replacement_connection_id =
        active_readiness(&runtime, &database.name, Duration::from_secs(4))
            .await
            .expect("checkout replacement after actor deadline")
            .connection_id;
    assert_ne!(replacement_connection_id, actor_connection_id);
    holder.release().await.expect("release physical key lock");
    let mut reader = store.begin_read().await.expect("begin verification read");
    assert!(
        reader
            .get(&locked_key)
            .await
            .expect("read deadline public key")
            .is_none()
    );
    reader.abort().await.expect("abort verification read");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_provider_state_store_accepts_3072_and_rejects_3073_before_io() {
    let database = TestDatabase::provision(
        "mysql_provider_state_store_accepts_3072_and_rejects_3073_before_io",
        "physical_boundary",
    );
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL transaction store");
    let statements_before_boundary = MysqlStatementTestApi::statement_count();

    let mut boundary_bytes = (0..3072)
        .map(|index| ((index * 131) % 256) as u8)
        .collect::<Vec<_>>();
    boundary_bytes[3071] = 0x7f;
    let boundary_key = key(boundary_bytes.clone());
    let payload = value(vec![0x00, 0xff, 0x80, 0x7f, 0x01]);
    let mut writer = store
        .begin_write(transaction_id(), "3072-byte physical boundary")
        .await
        .expect("begin boundary writer");
    writer
        .put(boundary_key.clone(), payload.clone(), Precondition::Any)
        .await
        .expect("stage 3072-byte key");
    assert_committed(writer.commit().await);

    let mut reader = store.begin_read().await.expect("begin boundary reader");
    let record = reader
        .get(&boundary_key)
        .await
        .expect("read 3072-byte key")
        .expect("3072-byte key exists");
    assert_eq!(record.value, payload);
    let mut end_bytes = boundary_bytes;
    end_bytes[3071] = 0x80;
    let page = reader
        .range(&RangeRequest {
            range: KeyRange::new(boundary_key.clone(), key(end_bytes))
                .expect("3072-byte bounded range"),
            direction: Direction::Forward,
            page_size: 1,
            continuation: None,
        })
        .await
        .expect("range 3072-byte key");
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].key, boundary_key);
    assert!(page.continuation.is_none());
    reader.abort().await.expect("abort boundary reader");
    assert!(
        MysqlStatementTestApi::statement_count() > statements_before_boundary,
        "statement counter must observe accepted provider SQL"
    );

    let mut oversized_writer = store
        .begin_write(transaction_id(), "3073-byte pre-I/O rejection")
        .await
        .expect("begin oversized writer");
    let statements_before = MysqlStatementTestApi::statement_count();
    let error = oversized_writer
        .put(
            key(vec![0x5a; 3073]),
            value(Bytes::from_static(b"must-not-reach-mysql")),
            Precondition::Any,
        )
        .await
        .expect_err("reject 3073-byte key");
    assert_eq!(error.kind(), StateStoreErrorKind::LimitExceeded);
    assert_eq!(
        MysqlStatementTestApi::statement_count(),
        statements_before,
        "3073-byte key must be rejected before SQL"
    );
    oversized_writer
        .abort()
        .await
        .expect("abort oversized writer");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_schema_cancellation_after_lock_is_safely_disposed() {
    let database = TestDatabase::provision(
        "mysql_schema_cancellation_after_lock_is_safely_disposed",
        "lock",
    );
    let mut client_config = fixture_client_config();
    client_config.pool_max = 1;
    let observer_client_config = client_config.clone();
    let runtime = std::sync::Arc::new(
        MysqlProviderTestHarness::boot(client_config).expect("construct MySQL runtime"),
    );
    let gate = arm_mysql_open_gate(&database.name, MysqlOpenGatePhase::AfterAdvisoryLock)
        .expect("arm advisory-lock cancellation gate");
    let waiter_runtime = std::sync::Arc::clone(&runtime);
    let waiter_database = database.name.clone();
    let waiter = tokio::spawn(async move {
        open_store(&waiter_runtime, &waiter_database, CLUSTER_ID, 4_000).await
    });

    tokio::time::timeout(Duration::from_secs(4), gate.wait_reached())
        .await
        .expect("advisory-lock gate reached");
    let original_connection_id = gate.connection_id();
    assert_ne!(original_connection_id, 0);
    waiter.abort();
    match waiter.await {
        Err(error) if error.is_cancelled() => {}
        _ => panic!("open waiter must be cancelled"),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), gate.wait_completed())
            .await
            .is_err(),
        "provider-owned open must remain alive after waiter cancellation"
    );
    let mut runtime = std::sync::Arc::try_unwrap(runtime).expect("sole runtime owner");
    let mut shutdown = tokio::spawn(async move {
        runtime
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err(),
        "shutdown must wait for advisory-lock cancellation cleanup"
    );
    gate.release();
    tokio::time::timeout(Duration::from_secs(4), gate.wait_completed())
        .await
        .expect("advisory-lock disposition completed");
    shutdown
        .await
        .expect("join MySQL runtime shutdown")
        .expect("shutdown MySQL runtime");

    let mut observer =
        MysqlProviderTestHarness::boot(observer_client_config).expect("construct observer runtime");
    let replacement = active_readiness(&observer, &database.name, Duration::from_secs(4))
        .await
        .expect("readiness after cancelled advisory lock");
    assert_ne!(replacement.connection_id, original_connection_id);
    assert!(
        is_schema_advisory_lock_free(&observer, &database.name, Duration::from_secs(4))
            .await
            .expect("lock state after waiter cancellation")
    );
    observer
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown observer runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_store_readiness_cancellation_after_start_is_safely_disposed() {
    let database = TestDatabase::provision(
        "mysql_store_readiness_cancellation_after_start_is_safely_disposed",
        "transaction",
    );
    let mut client_config = fixture_client_config();
    client_config.pool_max = 1;
    let observer_client_config = client_config.clone();
    let runtime = std::sync::Arc::new(
        MysqlProviderTestHarness::boot(client_config).expect("construct MySQL runtime"),
    );
    let gate = arm_mysql_open_gate(&database.name, MysqlOpenGatePhase::AfterReadOnlyStart)
        .expect("arm transaction cancellation gate");
    let waiter_runtime = std::sync::Arc::clone(&runtime);
    let waiter_database = database.name.clone();
    let waiter = tokio::spawn(async move {
        open_store(&waiter_runtime, &waiter_database, CLUSTER_ID, 4_000).await
    });

    tokio::time::timeout(Duration::from_secs(4), gate.wait_reached())
        .await
        .expect("transaction gate reached");
    let original_connection_id = gate.connection_id();
    assert_ne!(original_connection_id, 0);
    waiter.abort();
    match waiter.await {
        Err(error) if error.is_cancelled() => {}
        _ => panic!("open waiter must be cancelled"),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), gate.wait_completed())
            .await
            .is_err(),
        "provider-owned readiness must remain alive after waiter cancellation"
    );
    let mut runtime = std::sync::Arc::try_unwrap(runtime).expect("sole runtime owner");
    let mut shutdown = tokio::spawn(async move {
        runtime
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err(),
        "shutdown must wait for transaction cancellation cleanup"
    );
    gate.release();
    tokio::time::timeout(Duration::from_secs(4), gate.wait_completed())
        .await
        .expect("transaction disposition completed");
    shutdown
        .await
        .expect("join MySQL runtime shutdown")
        .expect("shutdown MySQL runtime");

    let mut observer =
        MysqlProviderTestHarness::boot(observer_client_config).expect("construct observer runtime");
    let replacement = active_readiness(&observer, &database.name, Duration::from_secs(4))
        .await
        .expect("readiness after cancelled transaction");
    assert_ne!(replacement.connection_id, original_connection_id);
    observer
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown observer runtime");
}

#[cfg(feature = "state-store-test-hooks")]
async fn run_task6_change_case(test_name: &str, scenario: &str) {
    let database = TestDatabase::provision(test_name, scenario);
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL state store");
    MysqlChangeTestApi::run_scenario(&runtime, &database.name, store, scenario)
        .await
        .expect("run MySQL change scenario");
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
async fn run_task6_commit_case(test_name: &str, scenario: &str) {
    let database = TestDatabase::provision(test_name, scenario);
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL state store");
    MysqlCommitTestApi::run_scenario(&mut runtime, &database.name, store, scenario)
        .await
        .expect("run MySQL commit scenario");
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
async fn open_task6_shared_fixture(
    test_name: &str,
    suffix: &str,
) -> (TestDatabase, MysqlProviderTestHarness, Arc<dyn StateStore>) {
    let database = TestDatabase::provision(test_name, suffix);
    let runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL state store");
    (database, runtime, store)
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_commit_progresses_with_single_connection_pool() {
    let database = TestDatabase::provision(
        "mysql_commit_progresses_with_single_connection_pool",
        "single_connection",
    );
    let mut client = fixture_client_config();
    client.pool_min = 1;
    client.pool_max = 1;
    let mut runtime = MysqlProviderTestHarness::boot(client).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL state store");
    let committed_key = key(Bytes::from_static(b"single-connection/commit"));
    let mut writer = store
        .begin_write(transaction_id(), "single connection commit")
        .await
        .expect("begin single-connection writer");
    writer
        .put(
            committed_key.clone(),
            value(Bytes::from_static(b"durable")),
            Precondition::Any,
        )
        .await
        .expect("stage single-connection mutation");
    assert_committed(writer.commit().await);
    let mut reader = store
        .begin_read()
        .await
        .expect("begin single-connection verification read");
    assert_eq!(
        reader
            .get(&committed_key)
            .await
            .expect("read single-connection commit")
            .expect("single-connection key exists")
            .value,
        value(Bytes::from_static(b"durable"))
    );
    reader
        .abort()
        .await
        .expect("abort single-connection verification read");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_commit_predispatch_gate_deadline_terminalizes() {
    let database = TestDatabase::provision("task6_predispatch_deadline", "predispatch_deadline");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 600)
        .await
        .expect("open MySQL state store");
    let transaction_id = transaction_id();
    let control = MysqlCommitTestApi::arm_shared_post_dispatch(false);
    let mut writer = store
        .begin_write(transaction_id, "pre-dispatch deadline")
        .await
        .expect("begin pre-dispatch deadline writer");
    writer
        .put(
            key(Bytes::from_static(b"predispatch/deadline")),
            value(Bytes::from_static(b"must-not-commit")),
            Precondition::Any,
        )
        .await
        .expect("stage pre-dispatch deadline mutation");
    let waiter = tokio::spawn(async move { writer.commit().await });
    control.wait_dispatched().await;
    assert_eq!(
        store
            .resolve_commit(&transaction_id)
            .await
            .expect("resolve gated pre-dispatch commit"),
        novarocks_spi::state_store::CommitResolution::Unresolved
    );
    let outcome = waiter.await.expect("join pre-dispatch deadline waiter");
    assert!(
        matches!(
            outcome,
            CommitOutcome::DefiniteFailure(ref error)
                if error.kind() == StateStoreErrorKind::DeadlineExceeded
        ),
        "{outcome:?}"
    );
    assert_eq!(
        store
            .resolve_commit(&transaction_id)
            .await
            .expect("resolve terminalized pre-dispatch commit"),
        novarocks_spi::state_store::CommitResolution::NotCommitted
    );
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
async fn assert_prepare_failure_terminalizes_after_rollback_failure(
    test_name: &str,
    rollback: MysqlPrepareRollbackFailure,
) {
    let database = TestDatabase::provision(test_name, "prepare_rollback");
    let mut client = fixture_client_config();
    client.pool_min = 1;
    client.pool_max = 2;
    let mut runtime = MysqlProviderTestHarness::boot(client).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL state store");
    let transaction_id = transaction_id();
    MysqlCommitTestApi::fail_next_prepare_after_reservation(rollback);
    let mut writer = store
        .begin_write(transaction_id, "prepare failure after reservation")
        .await
        .expect("begin prepare failure writer");
    writer
        .put(
            key(Bytes::from_static(b"prepare/rollback-failure")),
            value(Bytes::from_static(b"must-not-commit")),
            Precondition::Any,
        )
        .await
        .expect("stage prepare failure mutation");
    let outcome = writer.commit().await;
    assert!(
        matches!(
            outcome,
            CommitOutcome::TransientBeforeCommit(ref error)
                if error.kind() == StateStoreErrorKind::ProviderUnavailable
        ),
        "{outcome:?}"
    );
    assert_eq!(
        store
            .resolve_commit(&transaction_id)
            .await
            .expect("resolve terminalized prepare failure"),
        novarocks_spi::state_store::CommitResolution::NotCommitted
    );
    let failed_connection = MysqlCommitTestApi::last_prepare_failure_connection_id();
    assert_ne!(failed_connection, 0);
    let replacement = active_readiness(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("checkout after failed rollback")
        .connection_id;
    assert_ne!(replacement, failed_connection);
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_prepare_error_terminalizes_own_pending_after_rollback_error() {
    assert_prepare_failure_terminalizes_after_rollback_failure(
        "prepare_rollback_error",
        MysqlPrepareRollbackFailure::Error,
    )
    .await;
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_prepare_error_terminalizes_own_pending_after_rollback_timeout() {
    assert_prepare_failure_terminalizes_after_rollback_failure(
        "prepare_rollback_timeout",
        MysqlPrepareRollbackFailure::Timeout,
    )
    .await;
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_prepare_error_reports_unknown_when_terminalization_cannot_checkout() {
    let database = TestDatabase::provision("prepare_terminalize_timeout", "pool_exhausted");
    let mut client = fixture_client_config();
    client.pool_min = 1;
    client.pool_max = 1;
    let mut runtime = MysqlProviderTestHarness::boot(client).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL state store");
    let transaction_id = transaction_id();
    MysqlCommitTestApi::fail_next_prepare_after_reservation(MysqlPrepareRollbackFailure::Error);
    let terminalization = MysqlCommitTestApi::arm_terminalization();
    let mut writer = store
        .begin_write(transaction_id, "terminalization checkout timeout")
        .await
        .expect("begin terminalization timeout writer");
    writer
        .put(
            key(Bytes::from_static(b"prepare/terminalize-timeout")),
            value(Bytes::from_static(b"must-not-commit")),
            Precondition::Any,
        )
        .await
        .expect("stage terminalization timeout mutation");
    let waiter = tokio::spawn(async move { writer.commit().await });
    terminalization.wait_dispatched().await;
    let held = hold_connection(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("exhaust pool before terminalization checkout");
    terminalization.allow_provider_progress();
    let outcome = waiter.await.expect("join terminalization timeout waiter");
    assert!(
        matches!(outcome, CommitOutcome::CommitUnknown(_)),
        "{outcome:?}"
    );
    drop(held);
    assert_eq!(
        store
            .resolve_commit(&transaction_id)
            .await
            .expect("resolve pending after unknown terminalization"),
        novarocks_spi::state_store::CommitResolution::Unresolved
    );
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_prepare_error_terminalization_timeout_destroys_locked_connection() {
    let database = TestDatabase::provision("prepare_terminalize_row_lock", "locked_ledger");
    let mut client = fixture_client_config();
    client.pool_min = 1;
    client.pool_max = 2;
    let mut runtime = MysqlProviderTestHarness::boot(client).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL state store");
    let transaction_id = transaction_id();
    MysqlCommitTestApi::fail_next_prepare_after_reservation(MysqlPrepareRollbackFailure::Error);
    let terminalization = MysqlCommitTestApi::arm_terminalization();
    let mut writer = store
        .begin_write(transaction_id, "terminalization row lock timeout")
        .await
        .expect("begin locked terminalization writer");
    writer
        .put(
            key(Bytes::from_static(b"prepare/terminalize-row-lock")),
            value(Bytes::from_static(b"must-not-commit")),
            Precondition::Any,
        )
        .await
        .expect("stage locked terminalization mutation");
    let waiter = tokio::spawn(async move { writer.commit().await });
    terminalization.wait_dispatched().await;
    let blocker = MysqlCommitTestApi::hold_ledger_lock(&runtime, &database.name, transaction_id)
        .await
        .expect("lock own pending ledger row");
    MysqlStatementTestApi::reset_last_explicit_destroy();
    let terminalization_query = MysqlCommitTestApi::arm_terminalization_query();
    terminalization.allow_provider_progress();
    terminalization_query.wait_dispatched().await;
    let terminalization_connection = terminalization_query.connection_id();
    assert_ne!(terminalization_connection, 0);
    let started = tokio::time::Instant::now();
    let outcome = waiter.await.expect("join locked terminalization waiter");
    assert!(
        matches!(
            outcome,
            CommitOutcome::CommitUnknown(ref error)
                if error.kind() == StateStoreErrorKind::DeadlineExceeded
        ),
        "{outcome:?}"
    );
    assert!(started.elapsed() >= Duration::from_millis(1_500));
    blocker.release().await.expect("release pending ledger row");
    assert_eq!(
        store
            .resolve_commit(&transaction_id)
            .await
            .expect("resolve locked terminalization timeout"),
        novarocks_spi::state_store::CommitResolution::Unresolved
    );

    let mut first = hold_connection(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("checkout first connection after terminalization timeout");
    let first_id = first
        .connection_id(Duration::from_secs(4))
        .await
        .expect("read first post-timeout connection ID");
    let mut second = hold_connection(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("checkout second connection after terminalization timeout");
    let second_id = second
        .connection_id(Duration::from_secs(4))
        .await
        .expect("read second post-timeout connection ID");
    eprintln!(
        "MYSQL_TERMINALIZATION_TIMEOUT_CONNECTIONS terminalization={terminalization_connection} post=[{first_id},{second_id}]"
    );
    assert_ne!(first_id, terminalization_connection);
    assert_ne!(second_id, terminalization_connection);
    drop(first);
    drop(second);
    active_readiness(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("provider remains protocol-clean after terminalization timeout");
    assert_eq!(
        MysqlStatementTestApi::last_explicitly_destroyed_connection_id(),
        terminalization_connection,
        "timed-out terminalization connection must be explicitly destroyed"
    );
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_prepare_error_prefers_authoritative_committed_receipt() {
    let database = TestDatabase::provision("prepare_committed_precedence", "committed");
    let mut runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL state store");
    let transaction_id = transaction_id();
    MysqlCommitTestApi::fail_next_prepare_after_reservation(MysqlPrepareRollbackFailure::Error);
    let terminalization = MysqlCommitTestApi::arm_terminalization();
    let mut writer = store
        .begin_write(transaction_id, "committed terminalization precedence")
        .await
        .expect("begin committed precedence writer");
    writer
        .put(
            key(Bytes::from_static(b"prepare/committed-precedence")),
            value(Bytes::from_static(b"ignored")),
            Precondition::Any,
        )
        .await
        .expect("stage committed precedence mutation");
    let waiter = tokio::spawn(async move { writer.commit().await });
    terminalization.wait_dispatched().await;
    MysqlCommitTestApi::force_committed_ledger(&runtime, &database.name, transaction_id, 91)
        .await
        .expect("publish authoritative committed ledger");
    terminalization.allow_provider_progress();
    let outcome = waiter.await.expect("join committed precedence waiter");
    assert!(
        matches!(
            outcome,
            CommitOutcome::Committed(ref receipt)
                if receipt.transaction_id == transaction_id
                    && receipt.revision.as_bytes() == 91_u64.to_be_bytes()
        ),
        "{outcome:?}"
    );
    assert!(matches!(
        store
            .resolve_commit(&transaction_id)
            .await
            .expect("resolve authoritative committed ledger"),
        novarocks_spi::state_store::CommitResolution::Committed(_)
    ));
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_auxiliary_statement_timeout_destroys_connection() {
    let database = TestDatabase::provision("task6_aux_timeout", "aux_timeout");
    let mut client = fixture_client_config();
    client.pool_min = 1;
    client.pool_max = 1;
    let mut runtime = MysqlProviderTestHarness::boot(client).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL state store");
    let timed_out_connection =
        MysqlCommitTestApi::auxiliary_statement_timeout_disposes(&runtime, &database.name)
            .await
            .expect("dispose timed-out auxiliary connection");
    let replacement = active_readiness(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("checkout replacement auxiliary connection")
        .connection_id;
    assert_ne!(replacement, timed_out_connection);
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_auxiliary_native_error_rolls_back_active_transaction() {
    let database = TestDatabase::provision("task6_aux_rollback", "aux_rollback");
    let mut client = fixture_client_config();
    client.pool_min = 1;
    client.pool_max = 1;
    let mut runtime = MysqlProviderTestHarness::boot(client).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 4_000)
        .await
        .expect("open MySQL state store");
    MysqlCommitTestApi::auxiliary_native_error_rolls_back(&runtime, &database.name)
        .await
        .expect("rollback active auxiliary transaction after native error");
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_change_poll_cancellation_destroys_active_connection_and_holds_guard() {
    let database = TestDatabase::provision("task6_poll_cancel", "poll_cancel");
    let mut client = fixture_client_config();
    client.pool_min = 1;
    client.pool_max = 1;
    let mut runtime = MysqlProviderTestHarness::boot(client).expect("construct MySQL runtime");
    let store = open_store(&runtime, &database.name, CLUSTER_ID, 500)
        .await
        .expect("open MySQL state store");
    let original_connection = active_readiness(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("read original connection id")
        .connection_id;
    let control = MysqlChangeTestApi::arm_delayed_poll_query();
    let poll_store = Arc::clone(&store);
    let waiter = tokio::spawn(async move {
        poll_store
            .poll_changes(&novarocks_spi::state_store::ChangePollRequest {
                after: None,
                page_size: 1,
            })
            .await
    });
    control.wait_reached().await;
    waiter.abort();
    assert!(
        waiter.await.is_err_and(|error| error.is_cancelled()),
        "public poll waiter must be cancelled"
    );
    drop(store);

    let shutdown_error = runtime
        .shutdown(Instant::now() + Duration::from_millis(100))
        .await
        .expect_err("provider-owned poll must keep shutdown waiting");
    assert_eq!(shutdown_error.kind(), StateStoreErrorKind::DeadlineExceeded);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let replacement = active_readiness(&runtime, &database.name, Duration::from_secs(4))
        .await
        .expect("read replacement connection id")
        .connection_id;
    assert_ne!(replacement, original_connection);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_same_revision_change_pages() {
    let (_database, mut runtime, store) =
        open_task6_shared_fixture("task6_shared_same_revision", "same_revision_pages").await;
    let factory = shared_factory(Arc::clone(&store));
    state_store_conformance::same_revision_change_pages(&factory).await;
    drop(factory);
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_atomic_commit() {
    let (_database, mut runtime, store) =
        open_task6_shared_fixture("task6_shared_atomic", "shared_atomic").await;
    let factory = shared_factory(Arc::clone(&store));
    state_store_conformance::atomic_commit(&factory).await;
    drop(factory);
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_notification_delivery_faults() {
    let (_database, mut runtime, store) =
        open_task6_shared_fixture("task6_shared_notifications", "shared_notifications").await;
    let factory = shared_factory(Arc::clone(&store));
    state_store_conformance::notification_delivery_faults(&factory).await;
    drop(factory);
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_post_dispatch_response_loss_reconciles() {
    let (_database, mut runtime, store) =
        open_task6_shared_fixture("task6_shared_response_loss", "shared_response_loss").await;
    let factory = shared_post_dispatch_factory(Arc::clone(&store));
    state_store_conformance::post_dispatch_response_loss_reconciles(&factory).await;
    drop(factory);
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_shared_post_dispatch_cancel_waiter_reconciles() {
    let (_database, mut runtime, store) =
        open_task6_shared_fixture("task6_shared_cancel_waiter", "shared_cancel_waiter").await;
    let factory = shared_post_dispatch_factory(Arc::clone(&store));
    state_store_conformance::post_dispatch_cancel_waiter_reconciles(&factory).await;
    drop(factory);
    drop(store);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mysql_suite() {
    let runtime = Rc::new(
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("construct MySQL runtime"),
    );
    let databases = Rc::new(RefCell::new(Vec::new()));
    let factory = mysql_conformance_factory(Rc::clone(&runtime), Rc::clone(&databases));
    state_store_conformance::run_state_store_conformance(Rc::clone(&factory)).await;
    drop(factory);
    let mut runtime = Rc::try_unwrap(runtime).expect("all MySQL conformance handles drained");
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown MySQL conformance runtime");
    drop(databases);
}

macro_rules! task6_change_test {
    ($name:ident, $scenario:literal) => {
        #[cfg(feature = "state-store-test-hooks")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            run_task6_change_case(stringify!($name), $scenario).await;
        }
    };
}

macro_rules! task6_commit_test {
    ($name:ident, $scenario:literal) => {
        #[cfg(feature = "state-store-test-hooks")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            run_task6_commit_case(stringify!($name), $scenario).await;
        }
    };
}

task6_change_test!(
    mysql_revision_assigns_one_revision_and_stable_key_sequences,
    "revision_sequence"
);
task6_change_test!(
    mysql_version_is_exact_big_endian_revision_and_sequence,
    "version_encoding"
);
task6_change_test!(
    mysql_change_poll_handles_empty_high_watermark_future_and_stale_cursor,
    "cursor_boundaries"
);
task6_change_test!(
    mysql_change_poll_reports_retention_gap_without_cleanup,
    "retention_gap"
);
task6_change_test!(
    mysql_change_poll_rejects_duplicate_revision_sequence_rows,
    "duplicate_position"
);
task6_change_test!(
    mysql_change_poll_rejects_cursor_and_sequence_gaps,
    "cursor_sequence_gap"
);

task6_commit_test!(
    mysql_commit_reservation_absent_becomes_own_pending,
    "reservation_absent"
);
task6_commit_test!(
    mysql_commit_reservation_returns_existing_committed_receipt,
    "reservation_committed"
);
task6_commit_test!(
    mysql_commit_reservation_rejects_not_committed_reuse,
    "reservation_not_committed"
);
task6_commit_test!(
    mysql_commit_reservation_never_steals_foreign_pending,
    "reservation_foreign_pending"
);
task6_commit_test!(
    mysql_commit_reservation_conflict_requires_authoritative_reload,
    "reservation_reload"
);
task6_commit_test!(
    mysql_commit_ledger_rejects_malformed_and_nonterminal_mutation,
    "ledger_corruption"
);
task6_commit_test!(
    mysql_atomic_commit_publishes_kv_change_revision_and_ledger_together,
    "atomic_publication"
);
task6_commit_test!(
    mysql_commit_error_after_dispatch_is_always_unknown,
    "dispatch_error_unknown"
);
task6_commit_test!(
    mysql_commit_success_response_loss_resolves_committed,
    "response_loss"
);
task6_commit_test!(
    mysql_reservation_deadline_is_bounded_and_terminalized,
    "reservation_deadline"
);
task6_commit_test!(
    mysql_commit_dispatch_deadline_returns_unknown_and_resolves,
    "dispatch_deadline"
);
task6_commit_test!(
    mysql_resolve_absent_persists_not_committed_before_return,
    "resolve_absent"
);
task6_commit_test!(
    mysql_resolve_and_reservation_race_has_one_stable_terminal,
    "resolve_reservation_race"
);
task6_commit_test!(
    mysql_cleanup_terminalizes_only_absent_or_own_pending,
    "cleanup_own"
);
task6_commit_test!(
    mysql_cleanup_preserves_foreign_pending_and_terminal_states,
    "cleanup_foreign"
);
task6_commit_test!(
    mysql_cleanup_outlives_waiter_and_holds_runtime_guard,
    "cleanup_guard"
);
task6_commit_test!(
    mysql_prepare_error_cannot_mask_authoritative_committed_receipt,
    "prepare_fallback"
);
task6_commit_test!(
    mysql_resolution_deadline_never_fabricates_not_committed,
    "resolution_deadline"
);
