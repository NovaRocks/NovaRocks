// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0 (the
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

//! Finite MV-owned launch declarations and exact statement observation cleanup.

use crate::actors::sql::StatementResult;
use crate::scenario::ScenarioContext;
use crate::scenarios::native_sql_controls::SelectedStatement;
use crate::scenarios::paimon::scan_statement_actor::{ScanActor, combine};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::test_sessions::{ClusterTestSessionPlan, RoleSlot};
use novarocks_failpoint::session::DomainGrant;
use serde_json::{Value, json};
use std::path::Path;
use std::time::Instant;

pub(in crate::scenarios) const REWRITE_SQL: &str =
    "SELECT k1, SUM(v2) FROM orders GROUP BY k1 ORDER BY k1";
pub(in crate::scenarios) const REWRITE_BINDING_TOKEN: &str = "mv-rewrite-final-target";
pub(in crate::scenarios) const STORAGE_REWRITE_TOKEN: &str = "mv-storage-final-target";
pub(in crate::scenarios) const FIRST_REFRESH_TOKEN: &str = "mv-first-refresh-staging";
pub(in crate::scenarios) const FIRST_REFRESH_SQL: &str =
    "REFRESH MATERIALIZED VIEW orders_start_fault_mv";

pub(in crate::scenarios) enum FeLaunchIntent {
    Ordinary,
    MvActions(Vec<DomainGrant>),
    ObservedMvActions(Vec<DomainGrant>),
}

pub(in crate::scenarios) fn action(id: &str, parameters: Value) -> DomainGrant {
    DomainGrant {
        domain: "mv".into(),
        grant_id: id.into(),
        parameters,
    }
}

pub(in crate::scenarios) fn observation(
    id: &str,
    catalog: &str,
    namespace: &str,
    mv: &str,
    level: &str,
) -> DomainGrant {
    action(
        id,
        json!({"action":"document-observation","catalog":catalog,"namespace":namespace,"mv":mv,"level":level}),
    )
}

pub(in crate::scenarios) fn document_expiry(
    id: &str,
    catalog: &str,
    namespace: &str,
    mv: &str,
    token: &str,
    directory: Option<&Path>,
) -> DomainGrant {
    let mut parameters = json!({"action":"document-expiry","catalog":catalog,"namespace":namespace,"mv":mv,"token":token});
    if let Some(directory) = directory {
        parameters["directory"] = json!(directory);
    }
    action(id, parameters)
}

pub(in crate::scenarios) fn wipe(id: &str) -> DomainGrant {
    // This is the versioned consumer input, validated by the canonical owner
    // novarocks_mv_application::state_family::MV_ACCELERATOR_STATE_FAMILY.
    action(
        id,
        json!({"action":"accelerator-wipe","family":"novarocks/frontend/mv/accelerator/v3"}),
    )
}

pub(in crate::scenarios) fn scheduler(
    id: &str,
    catalog: &str,
    namespace: &str,
    mv: &str,
    directory: &Path,
) -> DomainGrant {
    action(
        id,
        json!({"action":"scheduler-hold","catalog":catalog,"namespace":namespace,"mv":mv,"directory":directory}),
    )
}

pub(in crate::scenarios) fn scheduler_outcome(
    id: &str,
    catalog: &str,
    namespace: &str,
    mv: &str,
    directory: &Path,
) -> DomainGrant {
    action(
        id,
        json!({"action":"scheduler-outcome","catalog":catalog,"namespace":namespace,"mv":mv,"directory":directory}),
    )
}

pub(in crate::scenarios) fn transient(
    id: &str,
    catalog: &str,
    namespace: &str,
    mv: &str,
    directory: &Path,
) -> DomainGrant {
    action(
        id,
        json!({"action":"transient-preparation","catalog":catalog,"namespace":namespace,"mv":mv,"directory":directory}),
    )
}

pub(in crate::scenarios) fn recovery(
    id: &str,
    catalog: &str,
    namespace: &str,
    mv: &str,
    phase: &str,
    token: &str,
    directory: &Path,
) -> Result<DomainGrant> {
    ensure!(
        matches!(
            phase,
            "source-pinned"
                | "source-prepared"
                | "data-prepared"
                | "write-committed"
                | "publication-committed"
        ),
        "unknown MV recovery phase"
    );
    ensure!(
        directory.is_absolute(),
        "MV recovery directory must be absolute"
    );
    Ok(action(
        id,
        json!({"action":"recovery-hold","catalog":catalog,"namespace":namespace,"mv":mv,"phase":phase,"token":token,"directory":directory}),
    ))
}

pub(in crate::scenarios) fn rewrite(
    id: &str,
    catalog: &str,
    namespace: &str,
    run_token: &str,
    directory: &Path,
) -> DomainGrant {
    action(
        id,
        json!({"action":"rewrite-hold","catalog":catalog,"namespace":namespace,"sql":REWRITE_SQL,"statement_run_token":run_token,"directory":directory}),
    )
}

fn mv_requirements() -> Vec<(String, String)> {
    [
        "novarocks-server",
        "novarocks-frontend-application",
        "novarocks-mv-application",
    ]
    .into_iter()
    .map(|owner| (owner.into(), "test-controls".into()))
    .collect()
}

fn statement_observer(plan: &ClusterTestSessionPlan) -> Result<DomainGrant> {
    Ok(DomainGrant {
        domain: "statement-observation".into(),
        grant_id: "exact-statement-observer".into(),
        parameters: json!({"action":"observe","bearer_secret":plan.fresh_domain_secret()?}),
    })
}

/// Empty intent slots select an uncontrolled successor explicitly. No action is
/// inferred from a scenario name or an inherited child environment.
pub(in crate::scenarios) fn plan(
    cluster_size: usize,
    binary: &Path,
    original_deadline: Instant,
    frontend_launches: Vec<FeLaunchIntent>,
) -> Result<ClusterTestSessionPlan> {
    ensure!(cluster_size == 3, "MV scenarios require native 1FE+3BE");
    ensure!(
        !frontend_launches.is_empty(),
        "MV plan has no frontend launch"
    );
    let mut plan = ClusterTestSessionPlan::new(original_deadline)?;
    for intent in frontend_launches {
        let reference = plan.reserve_launch(RoleSlot::Frontend)?;
        let grants = match intent {
            FeLaunchIntent::Ordinary => {
                plan.authorize_ordinary_launch(&reference, binary.to_path_buf())?;
                continue;
            }
            FeLaunchIntent::MvActions(grants) => grants,
            FeLaunchIntent::ObservedMvActions(mut grants) => {
                grants.push(statement_observer(&plan)?);
                grants
            }
        };
        ensure!(
            !grants.is_empty(),
            "controlled MV launch has no explicit action"
        );
        plan.set_launch_binary(&reference, binary.to_path_buf())?;
        plan.authorize_launch(&reference, grants, mv_requirements())?;
    }
    for index in 0..cluster_size {
        let reference = plan.reserve_launch(RoleSlot::Backend(index))?;
        plan.authorize_ordinary_launch(&reference, binary.to_path_buf())?;
    }
    plan.validate_selected_intents(cluster_size)?;
    Ok(plan)
}

pub(in crate::scenarios) fn first_refresh_plan(
    cluster_size: usize,
    binary: &Path,
    original_deadline: Instant,
) -> Result<ClusterTestSessionPlan> {
    ensure!(
        cluster_size == 3,
        "MV first refresh requires native 1FE+3BE"
    );
    let mut plan = ClusterTestSessionPlan::new(original_deadline)?;
    let root = plan.reserve_transport("query-lifecycle-faults")?;
    let frontend = plan.reserve_launch(RoleSlot::Frontend)?;
    let backend = plan.reserve_launch(RoleSlot::Backend(0))?;
    let parameters = json!({"action":"bind-task-execution-failure","root":root,"backend_index":0,
        "statement_run_token":FIRST_REFRESH_TOKEN,"target_session_id":backend.session_id,
        "target_process_receipt":backend.process_receipt});
    let mut grants = vec![DomainGrant {
        domain: "native".into(),
        grant_id: "bind-first-refresh-failure".into(),
        parameters,
    }];
    grants.push(statement_observer(&plan)?);
    let mut requirements = mv_requirements();
    requirements.push(("novarocks-native-adapter".into(), "test-controls".into()));
    plan.set_launch_binary(&frontend, binary.to_path_buf())?;
    plan.authorize_launch(&frontend, grants, requirements)?;
    plan.set_launch_binary(&backend, binary.to_path_buf())?;
    plan.authorize_launch(
        &backend,
        vec![DomainGrant {
            domain: "native".into(),
            grant_id: "first-refresh-failure".into(),
            parameters: json!({"action":"task-execution-failure","root":root,"backend_index":0,
            "statement_run_token":FIRST_REFRESH_TOKEN,"target_session_id":backend.session_id}),
        }],
        [
            "novarocks-server",
            "novarocks-native-adapter",
            "novarocks-worker",
            "novarocks-execution",
        ]
        .into_iter()
        .map(|owner| (owner.into(), "test-controls".into()))
        .collect(),
    )?;
    for index in 1..cluster_size {
        let reference = plan.reserve_launch(RoleSlot::Backend(index))?;
        plan.authorize_ordinary_launch(&reference, binary.to_path_buf())?;
    }
    plan.validate_selected_intents(cluster_size)?;
    Ok(plan)
}

/// Keep the primary result and actual cleanup failure, without converting a
/// missing action or failed drain into a successful scenario.
pub(in crate::scenarios) fn with_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    combine(primary, cleanup)
}

pub(in crate::scenarios) struct ObservedQuery {
    actor: ScanActor<StatementResult>,
}

impl ObservedQuery {
    pub(in crate::scenarios) fn start(
        context: &mut ScenarioContext,
        catalog: &str,
        namespace: &str,
        run_token: &str,
    ) -> Result<Self> {
        let selected = SelectedStatement {
            sql: REWRITE_SQL.into(),
            token: run_token.into(),
            faults: Vec::new(),
            deadline: context.deadline(),
        };
        let catalog = catalog.to_owned();
        let namespace = namespace.to_owned();
        let actor = ScanActor::start_with_setup(
            context,
            REWRITE_SQL.into(),
            Some(selected),
            move |session| {
                session.execute(format!("USE {catalog}.{namespace}"))?;
                Ok(())
            },
            Ok,
        )?;
        Ok(Self { actor })
    }

    pub(in crate::scenarios) fn is_finished(&self) -> bool {
        self.actor.is_finished()
    }

    fn finish(self, context: &mut ScenarioContext, action: &str) -> Result<Vec<(i32, i64)>> {
        context.action(action);
        let result = context.remaining(action).and_then(|remaining| {
            self.actor
                .done
                .recv_timeout(remaining)
                .context("exact MV query did not finish")
                .and_then(|result| result.map_err(anyhow::Error::new))
        });
        // The original actor cancels a nonterminal failure, joins its reader,
        // and drains the same exact query binding with the original deadline.
        let result = self.actor.settle(context, result)?;
        crate::scenarios::mv_uea7::mv_rows(&result)
    }

    pub(in crate::scenarios) fn finish_after_release(
        self,
        context: &mut ScenarioContext,
        phase: Result<()>,
        release: Result<()>,
        action: &str,
    ) -> Result<Vec<(i32, i64)>> {
        let phase = with_cleanup(phase, release);
        let result = self.finish(context, action);
        match (phase, result) {
            (Ok(()), result) => result,
            (Err(primary), Ok(_)) => Err(primary),
            (Err(primary), Err(query)) => with_cleanup(Err(primary), Err(query)),
        }
    }
}
