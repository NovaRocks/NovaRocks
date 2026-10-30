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

//! Mechanical finite SQL submissions supplied by each scenario owner.

use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{
    QueryLifecycleStatementObservation, ServerHandle,
    test_sessions::{ClusterTestSessionPlan, RoleLaunchReference, RoleSlot},
};
use novarocks_failpoint::session::DomainGrant;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Instant,
};

pub(in crate::scenarios) struct StatementSpec {
    pub sql: String,
    pub faults: Vec<(usize, &'static str)>,
    pub observations: Vec<(usize, &'static str, Option<u64>)>,
}
#[derive(Clone)]
pub(in crate::scenarios) struct SelectedStatement {
    pub sql: String,
    pub token: String,
    pub faults: Vec<(usize, &'static str)>,
    pub deadline: Instant,
}
impl SelectedStatement {
    fn verify_submission(&self, sql: &str, deadline: Instant) -> Result<()> {
        ensure!(
            sql == self.sql,
            "SQL differs from its frozen Native submission"
        );
        ensure!(
            deadline == self.deadline && Instant::now() < self.deadline,
            "Native SQL original deadline changed or expired"
        );
        Ok(())
    }
    pub fn activate_deferred_faults(&self, context: &mut ScenarioContext) -> Result<()> {
        self.verify_submission(&self.sql, context.deadline())?;
        ensure!(
            !self.faults.is_empty(),
            "selected statement declares no deferred action"
        );
        for &(index, kind) in &self.faults {
            ensure!(
                kind == "task-execution-failure-after-visible-prefix",
                "selected action is not deferred"
            );
            context
                .handle()
                .activate_selected_deferred_query_lifecycle_fault(index, kind, &self.token)?;
        }
        Ok(())
    }

    pub fn arm(
        &self,
        context: &mut ScenarioContext,
        connection: u32,
        sql: &str,
    ) -> Result<QueryLifecycleStatementObservation> {
        self.verify_submission(sql, context.deadline())?;
        for &(index, kind) in &self.faults {
            context
                .handle()
                .arm_selected_query_lifecycle_fault(index, kind, &self.token)?;
        }
        context.handle().arm_selected_statement_observation(
            &self.token,
            connection,
            &self.sql,
            self.deadline,
        )
    }
}
struct Launch {
    reference: RoleLaunchReference,
    grants: Vec<DomainGrant>,
}
fn add(launch: &mut Launch, domain: &str, parameters: serde_json::Value) -> String {
    let id = format!("{domain}-{}", launch.grants.len());
    launch.grants.push(DomainGrant {
        domain: domain.into(),
        grant_id: id.clone(),
        parameters,
    });
    id
}
struct Frozen {
    statements: Vec<SelectedStatement>,
    _root: PathBuf,
    _references: BTreeMap<RoleSlot, RoleLaunchReference>,
    _replacement_references: Vec<RoleLaunchReference>,
    used: BTreeSet<usize>,
}
#[derive(Default)]
pub(in crate::scenarios) struct SqlIntentOwner(Mutex<Option<Frozen>>);
#[derive(Clone, Copy)]
enum StatementRefinement {
    BeforeSchemaFailure,
    RemoteWriterSender(usize),
}
impl SqlIntentOwner {
    pub fn freeze(
        &self,
        specs: Vec<StatementSpec>,
        replacements: &[usize],
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<ClusterTestSessionPlan> {
        self.freeze_internal(specs, replacements, cluster_size, binary, deadline, None)
    }

    /// Coordinate the first declared Root failure with the original FE protocol handoff.
    pub fn freeze_before_schema_failure(
        &self,
        specs: Vec<StatementSpec>,
        replacements: &[usize],
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<ClusterTestSessionPlan> {
        ensure!(
            specs.first().is_some_and(|spec| spec
                .faults
                .iter()
                .any(|(_, kind)| *kind == "root-task-execution-failure-before-data")),
            "before-schema handoff requires the declared first Root failure"
        );
        self.freeze_internal(
            specs,
            replacements,
            cluster_size,
            binary,
            deadline,
            Some(StatementRefinement::BeforeSchemaFailure),
        )
    }

    /// Refine the existing pair of actions to one actual remote writer Task.
    pub fn freeze_remote_writer_sender(
        &self,
        specs: Vec<StatementSpec>,
        statement_index: usize,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<ClusterTestSessionPlan> {
        let selected = specs
            .get(statement_index)
            .context("remote writer statement is absent")?;
        let expected = (0..cluster_size)
            .flat_map(|index| {
                [
                    (index, "task-execution-failure"),
                    (index, "create-task-before-worker-hold"),
                ]
            })
            .collect::<BTreeSet<_>>();
        ensure!(
            selected.faults.iter().copied().collect::<BTreeSet<_>>() == expected,
            "remote writer selection requires its declared hold/failure candidates on each actual backend"
        );
        self.freeze_internal(
            specs,
            &[],
            cluster_size,
            binary,
            deadline,
            Some(StatementRefinement::RemoteWriterSender(statement_index)),
        )
    }

    fn freeze_internal(
        &self,
        specs: Vec<StatementSpec>,
        replacements: &[usize],
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
        refinement: Option<StatementRefinement>,
    ) -> Result<ClusterTestSessionPlan> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("Native SQL intent owner poisoned"))?;
        ensure!(owner.is_none(), "Native SQL intent already frozen");
        ensure!(
            cluster_size == 3 && !specs.is_empty() && specs.len() <= 64,
            "Native SQL requires finite native 1FE+3BE statements"
        );
        let mut plan = ClusterTestSessionPlan::new(deadline)?;
        let root = plan.reserve_transport("query-lifecycle-faults")?;
        let mut launches = BTreeMap::new();
        for role in
            std::iter::once(RoleSlot::Frontend).chain((0..cluster_size).map(RoleSlot::Backend))
        {
            launches.insert(
                role,
                Launch {
                    reference: plan.reserve_launch(role)?,
                    grants: vec![],
                },
            );
        }
        let secret = plan.fresh_domain_secret()?;
        add(
            launches.get_mut(&RoleSlot::Frontend).unwrap(),
            "statement-observation",
            json!({"action":"observe","bearer_secret":secret}),
        );
        let mut statements = vec![];
        for (statement_index, spec) in specs.into_iter().enumerate() {
            ensure!(
                !spec.sql.trim().is_empty(),
                "Native SQL submission must be complete"
            );
            let token = plan.fresh_domain_secret()?;
            if matches!(refinement, Some(StatementRefinement::BeforeSchemaFailure))
                && statement_index == 0
            {
                add(
                    launches.get_mut(&RoleSlot::Frontend).unwrap(),
                    "result-handoff",
                    json!({"action":"await-terminal-before-protocol", "statement_run_token":token}),
                );
            }
            let mut seen = BTreeSet::new();
            for &(index, kind) in &spec.faults {
                ensure!(
                    index < cluster_size && seen.insert((index, kind)),
                    "invalid or duplicate Native SQL fault target"
                );
                novarocks_failpoint::QueryLifecycleFaultKind::parse(kind)
                    .context("unsupported finite Native fault")?;
                let target = launches[&RoleSlot::Backend(index)].reference.clone();
                let mut bind = json!({"action":format!("bind-{kind}"),"root":root,"backend_index":index,"statement_run_token":token,"target_session_id":target.session_id,"target_process_receipt":target.process_receipt});
                let mut fault = json!({"action":kind,"root":root,"backend_index":index,"statement_run_token":token,"target_session_id":target.session_id});
                if matches!(refinement, Some(StatementRefinement::RemoteWriterSender(selected)) if selected == statement_index)
                {
                    bind["task_selector"] = json!("remote-writer-sender");
                    fault["task_selector"] = json!("remote-writer-sender");
                }
                add(
                    launches.get_mut(&RoleSlot::Frontend).unwrap(),
                    "native",
                    bind,
                );
                add(
                    launches.get_mut(&RoleSlot::Backend(index)).unwrap(),
                    "native",
                    fault,
                );
            }
            let mut seen = BTreeSet::new();
            let mut modes = BTreeMap::new();
            for (index, action, attempt) in spec.observations {
                ensure!(
                    index < cluster_size && seen.insert((index, action, attempt)),
                    "invalid or duplicate Native SQL observation target"
                );
                if let Some(explicit) = modes.insert((index, action), attempt.is_some()) {
                    ensure!(
                        explicit == attempt.is_some(),
                        "Native observation cannot mix first-actual and explicit attempts"
                    );
                }
                let target = launches[&RoleSlot::Backend(index)].reference.clone();
                let mut bind = json!({"action":format!("bind-{action}"),"root":root,"backend_index":index,"statement_run_token":token,"target_session_id":target.session_id,"target_process_receipt":target.process_receipt});
                let mut observe = json!({"action":action,"root":root,"backend_index":index,"statement_run_token":token,"target_session_id":target.session_id,"max_events":16384});
                if let Some(attempt) = attempt {
                    ensure!(attempt > 0, "Native observation attempt must be positive");
                    bind["attempt_id"] = json!(attempt);
                    observe["attempt_id"] = json!(attempt);
                }
                add(
                    launches.get_mut(&RoleSlot::Frontend).unwrap(),
                    "native-observation",
                    bind,
                );
                add(
                    launches.get_mut(&RoleSlot::Backend(index)).unwrap(),
                    "native-observation",
                    observe,
                );
            }
            statements.push(SelectedStatement {
                sql: spec.sql,
                token,
                faults: spec.faults,
                deadline,
            });
        }
        let references = launches
            .iter()
            .map(|(&role, launch)| (role, launch.reference.clone()))
            .collect();
        for (role, launch) in launches {
            if launch.grants.is_empty() {
                plan.authorize_ordinary_launch(&launch.reference, binary.to_path_buf())?;
            } else {
                let owner = match role {
                    RoleSlot::Frontend => "novarocks-frontend-application",
                    RoleSlot::Backend(_) => "novarocks-native-adapter",
                };
                plan.authorize_launch(
                    &launch.reference,
                    launch.grants,
                    vec![
                        ("novarocks-server".into(), "test-controls".into()),
                        (owner.into(), "test-controls".into()),
                    ],
                )?;
                plan.set_launch_binary(&launch.reference, binary.to_path_buf())?;
            }
        }
        let mut unique = BTreeSet::new();
        let mut replacement_references = vec![];
        for &index in replacements {
            ensure!(
                index < cluster_size && unique.insert(index),
                "invalid or duplicate Native replacement role"
            );
            let reference = plan.reserve_launch(RoleSlot::Backend(index))?;
            plan.authorize_ordinary_launch(&reference, binary.to_path_buf())?;
            replacement_references.push(reference);
        }
        plan.validate_selected_intents(cluster_size)?;
        *owner = Some(Frozen {
            statements,
            _root: root,
            _references: references,
            _replacement_references: replacement_references,
            used: BTreeSet::new(),
        });
        Ok(plan)
    }
    pub fn take(&self, index: usize) -> Result<SelectedStatement> {
        let mut owner = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("Native SQL intent owner poisoned"))?;
        let frozen = owner
            .as_mut()
            .context("Native SQL intent was not frozen before launch")?;
        let statement = frozen
            .statements
            .get(index)
            .context("submission is outside finite Native SQL plan")?
            .clone();
        ensure!(
            frozen.used.insert(index),
            "Native SQL submission was already selected"
        );
        Ok(statement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn spec(sql: &str) -> StatementSpec {
        StatementSpec {
            sql: sql.into(),
            faults: vec![],
            observations: vec![],
        }
    }
    fn deadline() -> Instant {
        Instant::now() + std::time::Duration::from_secs(10)
    }
    #[test]
    fn remote_writer_selection_declares_both_candidates_before_launch_without_touching_other_statements()
     {
        let binary = std::env::current_exe().unwrap();
        let owner = SqlIntentOwner::default();
        let mut incomplete = spec("INSERT INTO t SELECT * FROM s");
        incomplete.faults = vec![(0, "task-execution-failure")];
        assert!(
            owner
                .freeze_remote_writer_sender(vec![incomplete], 0, 3, &binary, deadline())
                .is_err()
        );
        let mut selected = spec("INSERT INTO t SELECT * FROM s");
        selected.faults = (0..3)
            .flat_map(|index| {
                [
                    (index, "task-execution-failure"),
                    (index, "create-task-before-worker-hold"),
                ]
            })
            .collect();
        let plan = owner
            .freeze_remote_writer_sender(
                vec![selected, spec("INSERT INTO t VALUES (1)")],
                0,
                3,
                &binary,
                deadline(),
            )
            .unwrap();
        let chosen = owner.take(0).unwrap();
        assert_eq!(chosen.faults.len(), 6);
        assert!(owner.take(1).unwrap().faults.is_empty());
        let summary = plan.planned_launch_summaries();
        let launches = summary.as_array().unwrap();
        for index in 0..3 {
            let backend = launches
                .iter()
                .find(|launch| launch["role"] == format!("be-{index}"))
                .unwrap();
            let actions = backend["grants"]
                .as_array()
                .unwrap()
                .iter()
                .map(|grant| grant["action"].as_str().unwrap())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                actions,
                BTreeSet::from(["task-execution-failure", "create-task-before-worker-hold"])
            );
        }
        assert_eq!(chosen.deadline, plan.deadline());
    }

    #[test]
    fn same_sql_has_separate_once_submissions_and_fault_does_not_spill_to_baseline() {
        let binary = std::env::current_exe().unwrap();
        let owner = SqlIntentOwner::default();
        let mut fault = spec("SELECT sleep(10)");
        fault.faults.push((0, "create-task-rejected-after-apply"));
        let plan = owner
            .freeze(
                vec![fault, spec("SELECT sleep(10)")],
                &[],
                3,
                &binary,
                deadline(),
            )
            .unwrap();
        let selected = owner.take(0).unwrap();
        let baseline = owner.take(1).unwrap();
        assert_ne!(selected.token, baseline.token);
        assert_eq!(selected.faults.len(), 1);
        assert!(baseline.faults.is_empty());
        assert!(owner.take(0).is_err());
        assert!(owner.take(2).is_err());
        assert_eq!(selected.deadline, plan.deadline());
        assert!(
            selected
                .verify_submission("SELECT sleep(10)", plan.deadline())
                .is_ok()
        );
        assert!(
            selected
                .verify_submission("SELECT sleep(11)", plan.deadline())
                .is_err()
        );
        assert!(
            selected
                .verify_submission(
                    "SELECT sleep(10)",
                    plan.deadline() + std::time::Duration::from_millis(1)
                )
                .is_err()
        );
    }
    #[test]
    fn attempt_refinement_is_finite_and_never_mixes_first_actual_with_explicit() {
        let binary = std::env::current_exe().unwrap();
        let owner = SqlIntentOwner::default();
        let mut statement = spec("SELECT 1");
        statement.observations = vec![(0, "grpc-fragment", Some(1)), (0, "grpc-fragment", Some(2))];
        let plan = owner
            .freeze(vec![statement], &[0, 1, 2], 3, &binary, deadline())
            .unwrap();
        let summaries = plan.planned_launch_summaries();
        let launches = summaries.as_array().unwrap();
        assert!(
            launches
                .iter()
                .all(|launch| launch["binary"] == json!(binary))
        );
        for index in 0..3 {
            let backend_launches = launches
                .iter()
                .filter(|launch| launch["role"] == format!("be-{index}"))
                .collect::<Vec<_>>();
            assert_eq!(backend_launches.len(), 2);
            assert_ne!(
                backend_launches[0]["session_id"],
                backend_launches[1]["session_id"]
            );
            assert_eq!(backend_launches[1]["ordinary"], true);
            assert!(backend_launches[1]["grants"].as_array().unwrap().is_empty());
        }
        let frozen = owner.0.lock().unwrap();
        assert_eq!(frozen.as_ref().unwrap()._replacement_references.len(), 3);
        assert_eq!(
            frozen.as_ref().unwrap().statements[0].deadline,
            plan.deadline()
        );
        drop(frozen);
        let owner = SqlIntentOwner::default();
        let mut mixed = spec("SELECT 1");
        mixed.observations = vec![(0, "grpc-fragment", None), (0, "grpc-fragment", Some(2))];
        assert!(
            owner
                .freeze(vec![mixed], &[], 3, &binary, deadline())
                .is_err()
        );
        assert!(owner.take(0).is_err());
    }
    #[test]
    fn oversized_role_intent_is_rejected_before_owner_publication() {
        let binary = std::env::current_exe().unwrap();
        let owner = SqlIntentOwner::default();
        let maximum = novarocks_failpoint::session::MAX_GRANTS;
        let statement_count = maximum.div_ceil(3);
        assert!(statement_count <= 64);
        // The FE owns one statement-observation grant plus these exact binds.
        let statements = (0..statement_count)
            .map(|at| {
                let mut statement = spec("SELECT 1");
                let observations = (maximum - at * 3).min(3);
                statement.observations = (0..observations)
                    .map(|index| (index, "grpc-fragment", None))
                    .collect();
                statement
            })
            .collect();
        let error = owner
            .freeze(statements, &[], 3, &binary, deadline())
            .err()
            .unwrap();
        assert_eq!(
            error.to_string(),
            "test-session requires a bounded nonempty grant set"
        );
        assert!(owner.take(0).is_err());
    }
}
