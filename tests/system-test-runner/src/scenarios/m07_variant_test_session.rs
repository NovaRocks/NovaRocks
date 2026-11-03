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

//! Finite declarations for the existing admitted M07 fixture owners.
use crate::scenario::ScenarioContext;
use anyhow::{Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_cluster_harness::test_sessions::{ClusterTestSessionPlan, RoleSlot};
use novarocks_failpoint::session::DomainGrant;
use serde_json::json;
use std::path::Path;
use std::time::Instant;

pub(in crate::scenarios) fn grant(
    domain: &str,
    grant_id: &str,
    parameters: serde_json::Value,
) -> DomainGrant {
    DomainGrant {
        domain: domain.into(),
        grant_id: grant_id.into(),
        parameters,
    }
}

pub(in crate::scenarios) fn fixture_plan(
    binary: &Path,
    cluster_size: usize,
    deadline: Instant,
    fixture_grants: Vec<DomainGrant>,
    variant: &str,
    variant_owners: &[&str],
    statement_tokens: &[&str],
) -> Result<ClusterTestSessionPlan> {
    ensure!(
        cluster_size == 3,
        "M07 fixture requires its original 1FE+3BE"
    );
    let mut plan = ClusterTestSessionPlan::new(deadline)?;
    let frontend = plan.reserve_launch(RoleSlot::Frontend)?;
    // Every role uses the admitted primary variant; grants remain role-local.
    plan.set_launch_binary(&frontend, binary.to_path_buf())?;
    let mut frontend_grants = fixture_grants;
    let mut frontend_capabilities = vec![
        ("novarocks-server".into(), "test-controls".into()),
        ("novarocks-failpoint".into(), "test-controls".into()),
        (
            "novarocks-frontend-application".into(),
            "test-controls".into(),
        ),
    ];
    for owner in variant_owners {
        frontend_capabilities.push(((*owner).into(), variant.into()));
    }
    if !statement_tokens.is_empty() {
        frontend_grants.push(grant(
            "statement-observation",
            "m07-exact-statement-owner",
            json!({"action":"observe", "bearer_secret":plan.fresh_domain_secret()?}),
        ));
        frontend_capabilities.push(("novarocks-native-adapter".into(), "test-controls".into()));
    }
    for index in 0..cluster_size {
        let backend = plan.reserve_launch(RoleSlot::Backend(index))?;
        if statement_tokens.is_empty() {
            plan.authorize_ordinary_launch(&backend, binary.to_path_buf())?;
            continue;
        }
        plan.set_launch_binary(&backend, binary.to_path_buf())?;
        let root = plan.reserve_transport(&format!("m07-root-be-{index}"))?;
        let mut backend_grants = Vec::new();
        for token in statement_tokens {
            frontend_grants.push(grant(
                "native-observation",
                &format!("bind-{token}-be-{index}"),
                json!({"action":"bind-task-protocol", "root":root,
                    "backend_index":index, "statement_run_token":token,
                    "target_session_id":backend.session_id,
                    "target_process_receipt":backend.process_receipt}),
            ));
            backend_grants.push(grant(
                "native-observation",
                &format!("observe-{token}-be-{index}"),
                // Existing log readers retain their stricter byte, line and
                // task-marker bounds. This bounds all events for this one
                // actual statement, including lease and terminal observations.
                json!({"action":novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), "root":root,
                    "backend_index":index, "statement_run_token":token,
                    "target_session_id":backend.session_id, "max_events":256}),
            ));
        }
        plan.authorize_launch(
            &backend,
            backend_grants,
            vec![
                ("novarocks-server".into(), "test-controls".into()),
                ("novarocks-failpoint".into(), "test-controls".into()),
                ("novarocks-native-adapter".into(), "test-controls".into()),
            ],
        )?;
    }
    plan.authorize_launch(&frontend, frontend_grants, frontend_capabilities)?;
    Ok(plan)
}

/// The existing harness control API is blocking. Its one scoped helper is
/// actually joined, including when the caller is running a current-thread
/// MySQL actor; no Tokio runtime or control future is abandoned on timeout.
pub(in crate::scenarios) fn arm_statement_until(
    context: &mut ScenarioContext,
    token: &str,
    connection: u32,
    sql: &str,
    deadline: Instant,
) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "M07 statement arm deadline expired"
    );
    let result = std::thread::scope(|scope| -> Result<()> {
        let helper = std::thread::Builder::new()
            .name("m07-statement-arm".into())
            .spawn_scoped(scope, || {
                context
                    .handle()
                    .arm_selected_statement_observation(token, connection, sql, deadline)
                    .map(|_| ())
            })?;
        helper.join().map_err(|payload| ArmJoinFailure {
            payload: std::sync::Mutex::new(payload),
        })?
    });
    let postcheck = if Instant::now() < deadline {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "M07 statement arm exceeded original deadline"
        ))
    };
    match (result, postcheck) {
        (Ok(()), result) | (result, Ok(())) => result,
        (Err(primary), Err(deadline)) => Err(ArmDeadlineFailure { primary, deadline }.into()),
    }
}

struct ArmJoinFailure {
    payload: std::sync::Mutex<Box<dyn std::any::Any + Send>>,
}
impl std::fmt::Debug for ArmJoinFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original M07 statement arm helper panicked; source retained")
    }
}
impl std::fmt::Display for ArmJoinFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.payload;
        f.write_str("original M07 statement arm helper panicked; source retained")
    }
}
impl std::error::Error for ArmJoinFailure {}
struct ArmDeadlineFailure {
    primary: anyhow::Error,
    deadline: anyhow::Error,
}
impl std::fmt::Debug for ArmDeadlineFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for ArmDeadlineFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.deadline;
        f.write_str("M07 statement arm and original deadline failed; both sources retained")
    }
}
impl std::error::Error for ArmDeadlineFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn exact_root_evidence_keeps_role_grants_on_the_selected_primary() {
        let binary = std::env::current_exe().unwrap();
        let result = fixture_plan(
            &binary,
            3,
            Instant::now() + Duration::from_secs(5),
            vec![grant(
                "mysql-fixture",
                "exact",
                json!({"action":"exact-write"}),
            )],
            "mem-1-m07-exact-mysql-write",
            &["novarocks-mysql-adapter"],
            &["original-target"],
        );
        let plan = result.unwrap();
        let roles = plan.planned_launch_summaries();
        let roles = roles.as_array().unwrap();
        assert_eq!(roles.len(), 4);
        assert!(roles.iter().all(|role| role["binary"] == json!(binary)));
        let frontend = roles.iter().find(|role| role["role"] == "fe").unwrap();
        assert!(frontend["grants"].as_array().unwrap().iter().any(|grant| {
            grant["domain"] == "mysql-fixture" && grant["action"] == "exact-write"
        }));
        for backend in roles.iter().filter(|role| role["role"] != "fe") {
            assert_eq!(backend["ordinary"], false);
            let grants = backend["grants"].as_array().unwrap();
            assert_eq!(grants.len(), 1);
            assert_eq!(grants[0]["domain"], "native-observation");
            assert_eq!(
                grants[0]["action"],
                novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str()
            );
        }
    }

    #[test]
    fn an_exact_fixture_requires_the_selected_primary_to_exist() {
        let directory = tempfile::tempdir().unwrap();
        let result = fixture_plan(
            &directory.path().join("missing-primary"),
            3,
            Instant::now() + Duration::from_secs(5),
            vec![grant(
                "mysql-fixture",
                "exact",
                json!({"action":"exact-write"}),
            )],
            "mem-1-m07-exact-mysql-write",
            &["novarocks-mysql-adapter"],
            &["original-target"],
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("binary is missing")
        );
    }

    #[test]
    fn held_root_grant_and_exact_mysql_grant_remain_distinct() {
        let binary = std::env::current_exe().unwrap();
        let plan = fixture_plan(
            &binary,
            3,
            Instant::now() + Duration::from_secs(5),
            vec![grant(
                "root-observation",
                "neutral",
                json!({"action":"observe"}),
            )],
            "mem-1-m07-root-observation",
            &["novarocks-frontend-application"],
            &["original-target", "original-health"],
        )
        .unwrap();
        let summary = plan.planned_launch_summaries();
        let launches = summary.as_array().unwrap();
        assert_eq!(launches.len(), 4);
        let frontend = launches
            .iter()
            .find(|launch| launch["role"] == "fe")
            .unwrap();
        let grants = frontend["grants"].as_array().unwrap();
        assert!(
            grants
                .iter()
                .any(|grant| grant["domain"] == "root-observation")
        );
        assert!(
            !grants
                .iter()
                .any(|grant| grant["domain"] == "mysql-fixture")
        );
        assert_eq!(
            grants
                .iter()
                .filter(|grant| grant["action"] == "bind-task-protocol")
                .count(),
            6
        );
        for backend in launches.iter().filter(|launch| launch["role"] != "fe") {
            let grants = backend["grants"].as_array().unwrap();
            assert_eq!(grants.len(), 2);
            assert!(
                grants
                    .iter()
                    .all(|grant| grant["domain"] == "native-observation")
            );
            assert_eq!(backend["ordinary"], false);
        }
    }

    #[test]
    fn hms_observation_grants_only_the_original_frontend_catalog() {
        let binary = std::env::current_exe().unwrap();
        let plan = fixture_plan(
            &binary,
            3,
            Instant::now() + Duration::from_secs(5),
            vec![grant(
                "hms-listing-observation",
                "catalog",
                json!({"action":"observe", "catalog_name":"original_hms"}),
            )],
            "mem-1-m07-hms-listing-observe",
            &["novarocks-connector-iceberg"],
            &[],
        )
        .unwrap();
        for launch in plan.planned_launch_summaries().as_array().unwrap() {
            assert_eq!(launch["binary"], json!(binary));
            if launch["role"] == "fe" {
                assert_eq!(launch["grants"].as_array().unwrap().len(), 1);
                assert_eq!(launch["grants"][0]["domain"], "hms-listing-observation");
            } else {
                assert_eq!(launch["ordinary"], true);
                assert!(launch["grants"].as_array().unwrap().is_empty());
            }
        }
    }
}
