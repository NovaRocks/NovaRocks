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

use novarocks_frontend::StateStoreRunPolicy;
use novarocks_server::app_config::{ApplicationConfig, NovaRocksConfig};
use novarocks_spi::connector::{CatalogCredentialPurpose, StaticCredentialReference};
use novarocks_types::ClusterRole;
use std::process::Command;
use std::time::Duration;

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
                let registry = config
                    .connector
                    .credential_registry(ClusterRole::Fe)
                    .map_err(anyhow::Error::msg)?;
                let reference = StaticCredentialReference::try_new("warehouse", "blue")?;
                let object_store = registry
                    .resolve(CatalogCredentialPurpose::ObjectStoreData, &reference)
                    .and_then(|material| material.as_s3())
                    .expect("object-store credential");
                assert_eq!(object_store.access_key_id().expose_secret(), CANARY);
                assert_eq!(object_store.access_key_secret().expose_secret(), CANARY);
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
[[connector.credentials]]
purpose = "object-store-data"
name = "warehouse"
generation = "blue"
kind = "s3"
access_key_id = "${ENV:NOVAROCKS_NWT1_ACCESS_KEY}"
access_key_secret = "${ENV:NOVAROCKS_NWT1_ACCESS_SECRET}"
"#,
            Some(CANARY),
            Some(CANARY),
        ),
        (
            "missing",
            r#"
[[connector.credentials]]
purpose = "object-store-data"
name = "warehouse"
generation = "blue"
kind = "s3"
access_key_id = "${ENV:NOVAROCKS_NWT1_ACCESS_KEY}"
access_key_secret = "secret"
"#,
            None,
            None,
        ),
        (
            "empty",
            r#"
[[connector.credentials]]
purpose = "object-store-data"
name = "warehouse"
generation = "blue"
kind = "s3"
access_key_id = "${ENV:NOVAROCKS_NWT1_ACCESS_KEY}"
access_key_secret = "secret"
"#,
            Some(""),
            None,
        ),
        (
            "malformed",
            r#"
[[connector.credentials]]
purpose = "object-store-data"
name = "warehouse"
generation = "blue"
kind = "s3"
access_key_id = "prefix-${ENV:NOVAROCKS_NWT1_ACCESS_KEY}"
access_key_secret = "secret"
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
fn storage_limits_and_application_policy_are_configured_apart() -> anyhow::Result<()> {
    // What the provider enforces and what the application is willing to spend
    // are two decisions with two owners, so they live in two sections. They
    // used to share one, which let a storage file dictate retry behaviour.
    let config_path = tempfile::NamedTempFile::new()?;
    std::fs::write(
        config_path.path(),
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"

[state_store.limits]
max_transaction_operations = 100
transaction_timeout_ms = 1500

[application.state_store_policy]
max_attempts = 2
operation_timeout_ms = 900
"#,
    )?;

    let loaded = NovaRocksConfig::load_from_file(config_path.path())?;
    let state_store = loaded.state_store.expect("state store config").store;
    assert_eq!(state_store.cluster_id, "production-cluster");
    assert_eq!(
        state_store.path,
        std::path::PathBuf::from("meta/frontend-state.sqlite")
    );
    assert_eq!(state_store.limits.max_transaction_operations, Some(100));
    assert_eq!(state_store.limits.transaction_timeout_ms, Some(1500));

    let policy = loaded.application.state_store_policy.resolve()?;
    assert_eq!(policy.max_attempts(), 2);
    assert_eq!(policy.operation_timeout(), Duration::from_millis(900));
    Ok(())
}

#[test]
fn an_absent_application_section_means_the_built_in_policy() -> anyhow::Result<()> {
    let config_path = tempfile::NamedTempFile::new()?;
    std::fs::write(
        config_path.path(),
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"
"#,
    )?;
    let loaded = NovaRocksConfig::load_from_file(config_path.path())?;
    assert_eq!(loaded.application, ApplicationConfig::default());
    assert_eq!(
        loaded.application.state_store_policy.resolve()?,
        StateStoreRunPolicy::default()
    );
    Ok(())
}

#[test]
fn an_application_policy_may_tighten_but_never_relax() -> anyhow::Result<()> {
    // Refused at load, not at first use: a file nothing can honour should fail
    // where it is written.
    for config in [
        r#"
[application.state_store_policy]
max_attempts = 0
"#,
        r#"
[application.state_store_policy]
max_attempts = 99
"#,
        r#"
[application.state_store_policy]
operation_timeout_ms = 0
"#,
        r#"
[application.state_store_policy]
operation_timeout_ms = 60000
"#,
    ] {
        let config_path = tempfile::NamedTempFile::new()?;
        std::fs::write(config_path.path(), config)?;
        let error = match NovaRocksConfig::load_from_file(config_path.path()) {
            Ok(_) => panic!("a relaxed application policy must fail before startup: {config}"),
            Err(error) => error,
        };
        assert!(
            format!("{error:#}").contains("InvalidApplicationConfig"),
            "unexpected error: {error:#}"
        );
    }
    Ok(())
}

#[test]
fn the_retired_history_and_runner_settings_are_refused_not_ignored() -> anyhow::Result<()> {
    // These named a change feed and a provider-dictated attempt ceiling that no
    // longer exist. Silently accepting them would leave an operator believing a
    // setting still applies.
    for config in [
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"

[state_store.history_retention]
max_age_secs = 3600
"#,
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"

[state_store.limits]
runner_max_attempts = 3
"#,
        r#"
[state_store]
provider = "sqlite"
cluster_id = "production-cluster"
path = "meta/frontend-state.sqlite"

[state_store.limits]
transaction_deadline_ms = 1500
"#,
    ] {
        let config_path = tempfile::NamedTempFile::new()?;
        std::fs::write(config_path.path(), config)?;
        assert!(
            NovaRocksConfig::load_from_file(config_path.path()).is_err(),
            "a retired setting must be rejected: {config}"
        );
    }
    Ok(())
}
