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

//! Optional small readonly HMS preflight. This does not replace the large CL input.
use super::connector::require_three_backends;
use super::hms_bulk_readonly_native::{
    HmsRegistration, finish_evidence_errors, install_credential_overlays, register,
    save_failure_diagnostic,
};
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Result, bail, ensure};
use mysql::prelude::Queryable;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant};

// This caps bytes retained from a local input, not Vec capacity or result memory.
const FILE_CAP: usize = 1_048_576;
const SECRET_NAMES: [&str; 2] = ["AWS_S3_ACCESS_KEY_ID", "AWS_S3_SECRET_ACCESS_KEY"];
const VIEW_REFUSAL: &str = "list_views is not supported by this catalog";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    schema_version: u32,
    parent_freeze_sha256: String,
    pre_native_oracle_sha256: String,
    publication_sha256: String,
    hms_manifest_sha256: String,
    base_config_sha256: String,
    catalog_name: String,
    namespace: String,
    hms_uri: String,
    warehouse: String,
    object_store_endpoint: String,
    // The parent clips this to its existing absolute work/phase deadline,
    // leaving 24 seconds for the four actual role stops and final evidence.
    assertion_budget_millis: u64,
}

pub(crate) struct HmsClassificationPreflight {
    input: Binding,
    assertions_deadline: Instant,
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take((FILE_CAP + 1) as u64)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= FILE_CAP,
        "HMS preflight file exceeds frozen local-file bound"
    );
    Ok(bytes)
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn safe_sql_error(error: &mysql::Error) -> anyhow::Error {
    // Only a digest is retained; unexpected provider payloads can contain secrets.
    let code = match error {
        mysql::Error::MySqlError(server) => Some(server.code),
        _ => None,
    };
    anyhow::anyhow!(
        "HMS preflight SQL failed; server_code={code:?}; error_sha256={}",
        digest(error.to_string().as_bytes())
    )
}

fn require_view_refusal(result: std::result::Result<(), mysql::Error>) -> Result<Value> {
    match result {
        Err(mysql::Error::MySqlError(error)) => {
            ensure!(
                error.code == 1105
                    && error.state == "HY000"
                    && error.message.contains("Unsupported:")
                    && error.message.contains(VIEW_REFUSAL),
                "HMS view operation did not return the exact unsupported source reason"
            );
            Ok(
                json!({"code":error.code,"state":error.state,"reason_sha256":digest(error.message.as_bytes())}),
            )
        }
        Err(error) => Err(safe_sql_error(&error)),
        Ok(()) => bail!("HMS unsupported view operation returned success"),
    }
}

impl HmsClassificationPreflight {
    pub(crate) fn from_binding(path: &Path, base_config: &Path) -> Result<Self> {
        let input: Binding = serde_json::from_slice(&read_bounded(path)?)?;
        ensure!(
            input.schema_version == 1
                && identifier(&input.catalog_name)
                && identifier(&input.namespace),
            "HMS preflight identity differs"
        );
        for hash in [
            &input.parent_freeze_sha256,
            &input.pre_native_oracle_sha256,
            &input.publication_sha256,
            &input.hms_manifest_sha256,
            &input.base_config_sha256,
        ] {
            ensure!(
                hash.len() == 64
                    && hash
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
                "HMS preflight proof hash is malformed"
            );
        }
        ensure!(
            digest(&read_bounded(base_config)?) == input.base_config_sha256,
            "HMS preflight base config differs"
        );
        ensure!(
            input.hms_uri.starts_with("thrift://127.0.0.1:")
                && input.warehouse.starts_with("s3://warehouse/")
                && input.object_store_endpoint.starts_with("http://127.0.0.1:"),
            "HMS preflight public authority differs"
        );
        // The Python owner validates exact canonical endpoint and warehouse
        // equality against its already checked RuntimeOwner/HiveOwner binding.
        ensure!(
            (1..=216_000).contains(&input.assertion_budget_millis),
            "HMS assertion budget differs"
        );
        let assertions_deadline =
            Instant::now() + Duration::from_millis(input.assertion_budget_millis);
        Ok(Self {
            input,
            assertions_deadline,
        })
    }

    fn remaining(&self) -> Result<Duration> {
        let remaining = self
            .assertions_deadline
            .saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "HMS preflight assertion deadline expired"
        );
        Ok(remaining)
    }
}

impl Scenario for HmsClassificationPreflight {
    fn name(&self) -> &'static str {
        "catalog/mem-1-m07-hms-classification-preflight"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
        self.remaining()?;
        let mut launch = ScenarioLaunchConfig::default();
        // The fresh owner supplies a provider-neutral base. Use exactly the bulk
        // FE metadata / BE data overlays, never a combined duplicate credential.
        install_credential_overlays(&mut launch)?;
        for name in SECRET_NAMES {
            let value = std::env::var(name)
                .map_err(|_| anyhow::anyhow!("HMS fixture credential reference is unavailable"))?;
            ensure!(
                !value.is_empty(),
                "HMS fixture credential reference is empty"
            );
            launch
                .child_environment
                .fe
                .insert(name.into(), value.clone());
            launch.child_environment.be.insert(name.into(), value);
        }
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        use novarocks_cluster_harness::test_sessions::{ClusterTestSessionPlan, RoleSlot};
        self.remaining()?;
        ensure!(
            cluster_size == 3,
            "HMS classification requires original 1FE+3BE"
        );
        let mut plan =
            ClusterTestSessionPlan::new(original_deadline.min(self.assertions_deadline))?;
        let frontend = plan.reserve_launch(RoleSlot::Frontend)?;
        plan.authorize_ordinary_launch(&frontend, binary.to_path_buf())?;
        for index in 0..cluster_size {
            let backend = plan.reserve_launch(RoleSlot::Backend(index))?;
            plan.authorize_ordinary_launch(&backend, binary.to_path_buf())?;
        }
        // This small correctness preflight grants no HMS collector, root marker
        // or fault action and cannot stand in for the independent bulk input.
        Ok(Some(plan))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let identities = context.recheck_live_process_launch_identities()?;
        let roles: BTreeSet<&str> = identities.iter().map(|id| id.role.as_str()).collect();
        let pids: BTreeSet<u32> = identities.iter().map(|id| id.pid).collect();
        ensure!(
            identities.len() == 4
                && pids.len() == 4
                && roles == BTreeSet::from(["fe", "be-0", "be-1", "be-2"]),
            "HMS preflight does not own four exact role identities"
        );
        let mut facts = json!({"schema_version":1,"scope":"small-hms-classification-only", "assertions":"pending",
            "launch_identities":identities, "parent_freeze_sha256":self.input.parent_freeze_sha256,
            "pre_native_oracle_sha256":self.input.pre_native_oracle_sha256,
            "mutation_operations":"excluded: IRU-7 owns HMS mutation correctness"});
        let receipt = context
            .scenario_root()
            .join("hms-classification-assertions.json");
        std::fs::write(&receipt, serde_json::to_vec_pretty(&facts)?)?;
        let assertions: Result<()> = (|| {
            facts["phase"] = json!("same-bounded-bulk-register");
            let input = HmsRegistration {
                catalog: &self.input.catalog_name,
                namespace: &self.input.namespace,
                hms_uri: &self.input.hms_uri,
                warehouse: &self.input.warehouse,
                object_store_endpoint: &self.input.object_store_endpoint,
            };
            facts["registration"] = register(
                &input,
                context.mysql_user(),
                context.mysql_port(),
                self.assertions_deadline,
            )?;
            // Existing business checks have their own original client/session.
            // Its USE below selects that session, not a second CREATE.
            facts["phase"] = json!("post-registration-connect");
            let timeout = self.remaining()?.min(Duration::from_secs(15));
            let mut client =
                mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout).map_err(
                    |error| {
                        anyhow::anyhow!(
                            "HMS preflight connection failed; error_sha256={}",
                            digest(error.to_string().as_bytes())
                        )
                    },
                )?;
            let catalog = &self.input.catalog_name;
            let namespace = &self.input.namespace;
            facts["phase"] = json!("use-namespace");
            self.remaining()?;
            client
                .query_drop(format!("USE {catalog}.{namespace}"))
                .map_err(|error| safe_sql_error(&error))?;
            // HMS table listings return every name in the metastore database
            // (ADR-0216), so this preflight no longer asserts an Iceberg-only
            // table list or table types for its externally prepared namespace.
            facts["phase"] = json!("show-views");
            self.remaining()?;
            facts["show_views"] = require_view_refusal(client.query_drop("SHOW VIEWS"))?;
            // IRU-7 owns HMS mutation correctness. This readonly preflight never
            // issues DROP DATABASE; the external fixture owner checks unchanged
            // metadata and performs its own cleanup after all native roles exit.
            self.remaining()?;
            facts["phase"] = json!("complete");
            Ok(())
        })();
        let primary = assertions.err();
        let diagnostic = primary
            .as_ref()
            .map(|error| save_failure_diagnostic(context.scenario_root(), error));
        if let Some(Ok(summary)) = &diagnostic {
            facts["failure_diagnostic"] = summary.clone();
        }
        facts["assertions"] = json!(if primary.is_none() {
            "passed"
        } else {
            "failed"
        });
        // Actual primary + diagnostic/evidence IO sources survive role cleanup.
        // No arbitrary source formatter is invoked for this receipt.
        let saved = serde_json::to_vec_pretty(&facts)
            .map_err(anyhow::Error::from)
            .and_then(|raw| std::fs::write(receipt, raw).map_err(Into::into));
        finish_evidence_errors(primary, diagnostic, saved)?;
        context.action("small readonly HMS namespace selection and precise view refusal passed; HMS mutations excluded for IRU-7");
        Ok(())
    }
}

#[cfg(test)]
mod refusal_tests {
    use super::*;

    fn server_error(
        code: u16,
        state: &str,
        message: &str,
    ) -> std::result::Result<(), mysql::Error> {
        Err(mysql::Error::MySqlError(mysql::MySqlError {
            code,
            state: state.into(),
            message: message.into(),
        }))
    }

    #[test]
    fn sql_failure_preserves_numeric_code_without_provider_payload() {
        let error = mysql::Error::MySqlError(mysql::MySqlError {
            code: 1054,
            state: "HY000".into(),
            message: "provider-secret-must-not-be-retained".into(),
        });
        let safe = safe_sql_error(&error).to_string();
        assert!(safe.contains("server_code=Some(1054)"));
        assert!(safe.contains("error_sha256="));
        assert!(!safe.contains("provider-secret"));
    }

    #[test]
    fn only_actual_server_unsupported_source_reason_passes() {
        assert!(
            require_view_refusal(server_error(
                1105,
                "HY000",
                "Unsupported: list_views is not supported by this catalog"
            ))
            .is_ok()
        );
        assert!(require_view_refusal(Ok(())).is_err());
        for (code, state, message) in [
            (
                1064,
                "42000",
                "Unsupported: list_views is not supported by this catalog",
            ),
            (
                1105,
                "HY000",
                "Unsupported: another operation is not supported",
            ),
            (1105, "HY000", "list_views is not supported by this catalog"),
            (1105, "HY000", "catalog transport unavailable"),
        ] {
            assert!(require_view_refusal(server_error(code, state, message)).is_err());
        }
    }
}
