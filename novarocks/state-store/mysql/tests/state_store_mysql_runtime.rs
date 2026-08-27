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
#![allow(
    clippy::await_holding_lock,
    reason = "tests serialize shared MySQL fixture environment mutations across await points"
)]

use std::time::{Duration, Instant};

use novarocks_secret::SecretValue;
use novarocks_spi::state_store::StateStoreErrorKind;
use novarocks_state_store_mysql::test_support::{
    MysqlProviderTestHarness, acquire_operation, acquire_provider_handle, active_readiness,
    begin_shutdown, hold_connection, is_accepting, pollute_session, pool_count, prepare_pool,
    restart_mysql_fixture, runtime_owner, validate_owner,
};
#[cfg(feature = "state-store-test-hooks")]
use novarocks_state_store_mysql::test_support::{
    delayed_active_readiness, run_sleep_until_deadline,
};
use novarocks_state_store_mysql::{MySqlClientConfig, MySqlTlsMode};

fn client_config(password: &str) -> MySqlClientConfig {
    MySqlClientConfig {
        host: "mysql.runtime.test.invalid".to_owned(),
        port: 3306,
        username: "runtime_test".to_owned(),
        password: SecretValue::new(password),
        tls_mode: MySqlTlsMode::Disabled,
        tls_ca_path: None,
        tls_cert_path: None,
        tls_key_path: None,
        connect_timeout_ms: 100,
        pool_min: 1,
        pool_max: 2,
        inactive_connection_ttl_ms: 1_000,
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
        pool_max: 2,
        inactive_connection_ttl_ms: 1_000,
    }
}

fn fixture_database() -> String {
    std::env::var("NOVAROCKS_MYSQL_DATABASE").expect("fixture database")
}

#[test]
fn mysql_runtime_requires_tokio_context() {
    let error = MysqlProviderTestHarness::boot(client_config("secret"))
        .expect_err("MySQL runtime construction must require an active Tokio runtime");
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_runtime_rejects_pid_mismatch() {
    let mut runtime =
        MysqlProviderTestHarness::boot(client_config("secret")).expect("construct MySQL runtime");
    let owner = runtime_owner(&runtime).expect("read runtime owner");
    let error = validate_owner(&runtime, owner.pid.wrapping_add(1))
        .expect_err("a different process must be rejected");
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown owner runtime");
}

#[test]
fn mysql_runtime_rejects_independent_tokio_runtime() {
    let first = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build first runtime");
    let runtime = first
        .block_on(async { MysqlProviderTestHarness::boot(client_config("secret")) })
        .expect("construct MySQL runtime");
    let second = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build second runtime");
    let (mut runtime, error) = second.block_on(async move {
        let error = prepare_pool(&runtime, "novarocks_wrong_tokio_runtime")
            .await
            .expect_err("an independent Tokio runtime must be rejected automatically");
        (runtime, error)
    });
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    first
        .block_on(runtime.shutdown(Instant::now() + Duration::from_secs(5)))
        .expect("shutdown on owner runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_runtime_reuses_one_pool_per_database() {
    let mut runtime =
        MysqlProviderTestHarness::boot(client_config("secret")).expect("construct MySQL runtime");
    prepare_pool(&runtime, "novarocks_runtime_pool")
        .await
        .expect("prepare first pool");
    prepare_pool(&runtime, "novarocks_runtime_pool")
        .await
        .expect("reuse first pool");
    assert_eq!(pool_count(&runtime).expect("pool count"), 1);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown owner runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_runtime_shutdown_is_retryable_after_handles_drain() {
    let mut runtime =
        MysqlProviderTestHarness::boot(client_config("secret")).expect("construct MySQL runtime");
    let handle = acquire_provider_handle(&runtime).expect("acquire provider handle");
    let first = runtime
        .shutdown(Instant::now() + Duration::from_millis(20))
        .await
        .expect_err("live handles must defer shutdown");
    assert_eq!(first.kind(), StateStoreErrorKind::DeadlineExceeded);
    drop(handle);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .expect("shutdown must be retryable after drain");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_runtime_stops_new_open_and_operations_during_shutdown() {
    let mut runtime =
        MysqlProviderTestHarness::boot(client_config("secret")).expect("construct MySQL runtime");
    let handle = acquire_provider_handle(&runtime).expect("acquire provider handle");
    begin_shutdown(&runtime).expect("begin shutdown");
    assert!(!is_accepting(&runtime).expect("runtime state"));
    let open_error = prepare_pool(&runtime, "novarocks_runtime_stopping")
        .await
        .expect_err("shutdown must stop new opens");
    assert_eq!(open_error.kind(), StateStoreErrorKind::ProviderUnavailable);
    let operation_error =
        acquire_operation(&runtime).expect_err("shutdown must stop new operations");
    assert_eq!(
        operation_error.kind(),
        StateStoreErrorKind::ProviderUnavailable
    );
    drop(handle);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .expect("shutdown after handle drain");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_runtime_debug_redacts_client_configuration() {
    let ca = tempfile::NamedTempFile::new().expect("temporary CA file");
    let ca_path = ca.path().to_path_buf();
    let config = MySqlClientConfig {
        host: "sensitive.mysql.internal".to_owned(),
        username: "sensitive-user".to_owned(),
        password: SecretValue::new("debug-secret-value"),
        tls_ca_path: Some(ca_path.clone()),
        ..client_config("debug-secret-value")
    };
    let config_debug = format!("{config:?}");
    assert!(config_debug.contains("password_configured: true"));
    assert!(!config_debug.contains("debug-secret-value"));
    let mut runtime = MysqlProviderTestHarness::boot(config).expect("construct MySQL runtime");
    let debug = format!("{runtime:?}");
    for secret in [
        "sensitive.mysql.internal",
        "sensitive-user",
        "debug-secret-value",
        ca_path.to_str().expect("UTF-8 temporary path"),
        "mysql://",
    ] {
        assert!(!debug.contains(secret), "Debug leaked {secret}: {debug}");
    }
    assert!(debug.contains("MysqlProviderTestHarness::Mysql"));
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown owner runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_runtime_rejects_empty_direct_password() {
    let error = MysqlProviderTestHarness::boot(client_config(""))
        .expect_err("empty direct password must fail");
    assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_client_pool_is_lazy_until_active_readiness() {
    let mut config = client_config("secret");
    config.host = "127.0.0.1".to_owned();
    config.port = 1;
    let mut runtime = MysqlProviderTestHarness::boot(config).expect("lazy runtime construction");
    prepare_pool(&runtime, "novarocks_lazy_pool")
        .await
        .expect("Pool::new must stay lazy");
    assert_eq!(pool_count(&runtime).expect("pool count"), 1);
    let error = active_readiness(&runtime, "novarocks_lazy_pool", Duration::from_millis(50))
        .await
        .expect_err("active readiness must perform the first connection");
    assert!(matches!(
        error.kind(),
        StateStoreErrorKind::DeadlineExceeded | StateStoreErrorKind::ProviderUnavailable
    ));
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown lazy pool");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_client_readiness_checks_exact_server_and_session_contract() {
    let mut runtime = MysqlProviderTestHarness::boot(fixture_client_config())
        .expect("fixture runtime construction");
    let snapshot = active_readiness(&runtime, &fixture_database(), Duration::from_secs(3))
        .await
        .expect("active readiness");
    assert_eq!(snapshot.server_version, "8.4.10");
    assert_eq!(snapshot.innodb_page_size, 16_384);
    assert!(snapshot.innodb_available);
    assert_eq!(snapshot.default_storage_engine, "InnoDB");
    assert!(snapshot.sql_mode.contains("STRICT"));
    assert_eq!(snapshot.time_zone, "+00:00");
    assert_eq!(snapshot.character_set, "utf8mb4");
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown fixture runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_client_readiness_maps_auth_database_and_tls_failures() {
    let database = fixture_database();

    let mut bad_auth = fixture_client_config();
    bad_auth.password = SecretValue::new("wrong-password");
    let mut auth_runtime = MysqlProviderTestHarness::boot(bad_auth).expect("auth runtime");
    let auth = active_readiness(&auth_runtime, &database, Duration::from_secs(2))
        .await
        .expect_err("bad auth must fail");
    assert_eq!(auth.kind(), StateStoreErrorKind::InvalidConfiguration);
    auth_runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown auth runtime");

    let mut database_runtime =
        MysqlProviderTestHarness::boot(fixture_client_config()).expect("database runtime");
    let missing_database = active_readiness(
        &database_runtime,
        "novarocks_ss3_database_does_not_exist",
        Duration::from_secs(2),
    )
    .await
    .expect_err("missing database must fail");
    assert_eq!(
        missing_database.kind(),
        StateStoreErrorKind::InvalidConfiguration
    );
    database_runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown database runtime");

    let invalid_ca = tempfile::NamedTempFile::new().expect("invalid CA file");
    std::fs::write(invalid_ca.path(), b"not a certificate").expect("write invalid CA");
    let mut bad_tls = fixture_client_config();
    bad_tls.host = "localhost".to_owned();
    bad_tls.tls_mode = MySqlTlsMode::VerifyIdentity;
    bad_tls.tls_ca_path = Some(invalid_ca.path().to_path_buf());
    let mut tls_runtime = MysqlProviderTestHarness::boot(bad_tls).expect("TLS runtime");
    let tls = active_readiness(&tls_runtime, &database, Duration::from_secs(2))
        .await
        .expect_err("invalid CA must fail");
    assert_eq!(tls.kind(), StateStoreErrorKind::InvalidConfiguration);
    tls_runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown TLS runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_client_pool_reset_and_checkout_hygiene_clear_session_pollution() {
    let database = fixture_database();
    let mut runtime = MysqlProviderTestHarness::boot(fixture_client_config())
        .expect("fixture runtime construction");
    pollute_session(&runtime, &database, Duration::from_secs(2))
        .await
        .expect("pollute checked-out session");
    let snapshot = active_readiness(&runtime, &database, Duration::from_secs(2))
        .await
        .expect("readiness reapplies hygiene");
    assert_eq!(snapshot.time_zone, "+00:00");
    assert!(snapshot.sql_mode.contains("STRICT"));
    assert_eq!(snapshot.character_set, "utf8mb4");
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown fixture runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_client_connect_deadline_is_bounded() {
    let mut config = client_config("secret");
    config.host = "203.0.113.1".to_owned();
    config.port = 3306;
    config.connect_timeout_ms = 100;
    let mut runtime = MysqlProviderTestHarness::boot(config).expect("deadline runtime");
    let started = std::time::Instant::now();
    let error = active_readiness(
        &runtime,
        "novarocks_connect_deadline",
        Duration::from_millis(150),
    )
    .await
    .expect_err("unreachable server must honor deadline");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(matches!(
        error.kind(),
        StateStoreErrorKind::DeadlineExceeded | StateStoreErrorKind::ProviderUnavailable
    ));
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown deadline runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_client_pool_exhaustion_honors_operation_deadline() {
    let mut config = fixture_client_config();
    config.pool_min = 1;
    config.pool_max = 1;
    let database = fixture_database();
    let mut runtime = MysqlProviderTestHarness::boot(config).expect("fixture runtime");
    let held = hold_connection(&runtime, &database, Duration::from_secs(2))
        .await
        .expect("hold the only connection");
    let error = active_readiness(&runtime, &database, Duration::from_millis(50))
        .await
        .expect_err("pool wait must honor total deadline");
    assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded);
    drop(held);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown fixture runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_client_pool_wait_can_outlive_connect_timeout_within_operation_deadline() {
    let mut config = fixture_client_config();
    config.connect_timeout_ms = 50;
    config.pool_min = 1;
    config.pool_max = 1;
    let database = fixture_database();
    let mut runtime = MysqlProviderTestHarness::boot(config).expect("fixture runtime");
    let held = hold_connection(&runtime, &database, Duration::from_secs(2))
        .await
        .expect("hold the only connection");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(held);
    });

    active_readiness(&runtime, &database, Duration::from_secs(1))
        .await
        .expect("pool wait may exceed connect timeout while inside total deadline");
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown fixture runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "current_thread")]
async fn mysql_client_readiness_timeout_destroys_undrained_connection() {
    let mut config = fixture_client_config();
    config.pool_min = 1;
    config.pool_max = 1;
    let database = fixture_database();
    let mut runtime = MysqlProviderTestHarness::boot(config).expect("fixture runtime");
    let before = active_readiness(&runtime, &database, Duration::from_secs(2))
        .await
        .expect("establish pooled connection");

    let error = delayed_active_readiness(&runtime, &database, Duration::from_millis(100))
        .await
        .expect_err("delayed production readiness query must time out");
    assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded);

    let after = active_readiness(&runtime, &database, Duration::from_secs(2))
        .await
        .expect("checkout after timed-out readiness");
    assert_ne!(
        before.connection_id, after.connection_id,
        "undrained readiness connection must not return to the pool"
    );
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown fixture runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn mysql_runtime_discards_stale_connections_after_server_restart() {
    let mut config = fixture_client_config();
    config.pool_min = 1;
    config.pool_max = 1;
    let database = fixture_database();
    let mut runtime = MysqlProviderTestHarness::boot(config).expect("fixture runtime");
    active_readiness(&runtime, &database, Duration::from_secs(2))
        .await
        .expect("establish pooled connection before restart");

    restart_mysql_fixture()
        .await
        .expect("restart fixture and wait for readiness");

    let snapshot = active_readiness(&runtime, &database, Duration::from_secs(5))
        .await
        .expect("discard stale connection and establish a fresh one");
    assert_eq!(snapshot.server_version, "8.4.10");
    assert_eq!(pool_count(&runtime).expect("pool count"), 1);
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown fixture runtime");
}

#[cfg(feature = "state-store-test-hooks")]
#[tokio::test(flavor = "current_thread")]
async fn mysql_runtime_destroys_connection_when_cancelled_statement_is_not_drained() {
    let mut config = fixture_client_config();
    config.pool_min = 1;
    config.pool_max = 1;
    let database = fixture_database();
    let mut runtime = MysqlProviderTestHarness::boot(config).expect("fixture runtime");
    let before = active_readiness(&runtime, &database, Duration::from_secs(2))
        .await
        .expect("establish pooled connection");

    let error = run_sleep_until_deadline(&runtime, &database, Duration::from_millis(100))
        .await
        .expect_err("cancelled statement must report its deadline");
    assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded);

    let after = active_readiness(&runtime, &database, Duration::from_secs(2))
        .await
        .expect("checkout after cancelled statement");
    assert_ne!(
        before.connection_id, after.connection_id,
        "the cancelled statement connection must not return to the pool"
    );
    runtime
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown fixture runtime");
}
