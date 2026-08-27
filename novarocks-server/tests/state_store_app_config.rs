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

use novarocks_server::app_config::NovaRocksConfig;
use novarocks_state_store_sqlite::SqliteHistoryRetentionConfig;
use std::process::Command;

#[test]
fn server_load_resolves_environment_references_once_without_secret_diagnostics()
-> anyhow::Result<()> {
    const CHILD_CONFIG_ENV: &str = "NOVAROCKS_NWT1_ENV_REFERENCE_CONFIG";
    const CHILD_SCENARIO_ENV: &str = "NOVAROCKS_NWT1_ENV_REFERENCE_SCENARIO";
    const ACCESS_KEY_ENV: &str = "NOVAROCKS_NWT1_ACCESS_KEY";
    const ACCESS_SECRET_ENV: &str = "NOVAROCKS_NWT1_ACCESS_SECRET";
    const CANARY: &str = "nwt-1-config-secret-canary";

    if let Some(config_path) = std::env::var_os(CHILD_CONFIG_ENV) {
        let result = NovaRocksConfig::load_from_file(std::path::Path::new(&config_path));
        match std::env::var(CHILD_SCENARIO_ENV).as_deref() {
            Ok("success") => {
                let config = result?;
                let object_store = config
                    .connector
                    .object_store
                    .expect("object store configuration");
                assert_eq!(
                    object_store
                        .access_key_id
                        .as_ref()
                        .map(|value| value.expose_secret()),
                    Some(CANARY)
                );
                assert_eq!(
                    object_store
                        .access_key_secret
                        .as_ref()
                        .map(|value| value.expose_secret()),
                    Some(CANARY)
                );
                assert!(!format!("{object_store:?}").contains(CANARY));
            }
            Ok("missing") => assert_error_category(result, "missing"),
            Ok("empty") => assert_error_category(result, "empty"),
            Ok("malformed") => assert_error_category(result, "not an exact ${ENV:VAR} reference"),
            scenario => panic!("unexpected environment-reference child scenario: {scenario:?}"),
        }
        return Ok(());
    }

    for (scenario, config, access_key, access_secret) in [
        (
            "success",
            r#"
[connector.object_store]
endpoint = "http://object-store:9000"
access_key_id = "${ENV:NOVAROCKS_NWT1_ACCESS_KEY}"
access_key_secret = "${ENV:NOVAROCKS_NWT1_ACCESS_SECRET}"
"#,
            Some(CANARY),
            Some(CANARY),
        ),
        (
            "missing",
            r#"
[connector.object_store]
access_key_id = "${ENV:NOVAROCKS_NWT1_ACCESS_KEY}"
"#,
            None,
            None,
        ),
        (
            "empty",
            r#"
[connector.object_store]
access_key_id = "${ENV:NOVAROCKS_NWT1_ACCESS_KEY}"
"#,
            Some(""),
            None,
        ),
        (
            "malformed",
            r#"
[connector.object_store]
access_key_id = "prefix-${ENV:NOVAROCKS_NWT1_ACCESS_KEY}"
"#,
            Some(CANARY),
            None,
        ),
    ] {
        let config_path = tempfile::NamedTempFile::new()?;
        std::fs::write(config_path.path(), config)?;
        let output = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("server_load_resolves_environment_references_once_without_secret_diagnostics")
            .arg("--nocapture")
            .env(CHILD_CONFIG_ENV, config_path.path())
            .env(CHILD_SCENARIO_ENV, scenario)
            .env_remove(ACCESS_KEY_ENV)
            .env_remove(ACCESS_SECRET_ENV)
            .envs(access_key.map(|value| (ACCESS_KEY_ENV, value)))
            .envs(access_secret.map(|value| (ACCESS_SECRET_ENV, value)))
            .output()?;
        let diagnostics = format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "{scenario} child failed\n{diagnostics}"
        );
        assert!(
            !diagnostics.contains(CANARY),
            "{scenario} child diagnostics leaked secret\n{diagnostics}"
        );
    }
    Ok(())
}

fn assert_error_category(result: anyhow::Result<NovaRocksConfig>, expected: &str) {
    let error = match result {
        Ok(_) => panic!("environment reference must fail"),
        Err(error) => error,
    };
    let message = format!("{error:#}");
    assert!(
        message.contains(expected),
        "expected {expected:?}, got {message:?}"
    );
    assert!(!message.contains("nwt-1-config-secret-canary"));
}

#[test]
fn sqlite_config_loads_defaults_and_explicit_retention() -> anyhow::Result<()> {
    let config_path = tempfile::NamedTempFile::new()?;
    std::fs::write(
        config_path.path(),
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"

[state_store.history_retention]
max_age_secs = 3600
max_change_rows = 2000000
max_commit_receipts = 3000000
maintenance_interval_commits = 512
incremental_vacuum_pages = 2048
"#,
    )?;

    let loaded = NovaRocksConfig::load_from_file(config_path.path())?;
    let state_store = loaded.state_store.expect("state store config").store;
    assert_eq!(state_store.cluster_id, "production-cluster");
    assert_eq!(
        state_store.path,
        std::path::PathBuf::from("meta/frontend-state.sqlite")
    );
    assert_eq!(state_store.history_retention.max_age_secs, 3600);
    assert_eq!(state_store.history_retention.max_change_rows, 2_000_000);
    assert_eq!(state_store.history_retention.max_commit_receipts, 3_000_000);
    assert_eq!(
        state_store.history_retention.maintenance_interval_commits,
        512
    );
    assert_eq!(state_store.history_retention.incremental_vacuum_pages, 2048);

    let defaults_path = tempfile::NamedTempFile::new()?;
    std::fs::write(
        defaults_path.path(),
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"
"#,
    )?;
    let defaults = NovaRocksConfig::load_from_file(defaults_path.path())?
        .state_store
        .expect("state store config")
        .store
        .history_retention;
    assert_eq!(defaults, SqliteHistoryRetentionConfig::default());
    Ok(())
}

#[test]
fn sqlite_config_rejects_remote_provider_and_unknown_remote_arguments() -> anyhow::Result<()> {
    for config in [
        r#"
[state_store]
provider = "mysql"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"
database = "remote_state"
"#,
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"
deployment_owner = "fe-a"
"#,
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"

[foundationdb_client]
disable_multi_version_client = true
"#,
    ] {
        let config_path = tempfile::NamedTempFile::new()?;
        std::fs::write(config_path.path(), config)?;
        assert!(
            NovaRocksConfig::load_from_file(config_path.path()).is_err(),
            "remote server configuration must be rejected: {config}"
        );
    }
    Ok(())
}

#[test]
fn sqlite_config_rejects_invalid_history_retention_before_opening() -> anyhow::Result<()> {
    for config in [
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"

[state_store.history_retention]
max_age_secs = 0
"#,
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"

[state_store.limits]
max_transaction_operations = 100

[state_store.history_retention]
max_change_rows = 99
"#,
    ] {
        let config_path = tempfile::NamedTempFile::new()?;
        std::fs::write(config_path.path(), config)?;
        let error = match NovaRocksConfig::load_from_file(config_path.path()) {
            Ok(_) => panic!("invalid retention configuration must fail before opening"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("InvalidStateStoreConfig"),
            "unexpected error: {error:#}"
        );
    }
    Ok(())
}
