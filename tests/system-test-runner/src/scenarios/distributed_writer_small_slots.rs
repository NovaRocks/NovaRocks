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

//! The original row-level contracts under small exchange geometry.
//! A selected append scope holds real writer Work. Ending only that scope
//! resumes the original append; it neither cancels the Task nor replaces SQL.
use super::*;

const APPEND_HOLD: &str = "connector-write-append-hold";
pub(super) const PARTITION_OBSERVATION_INDEX: usize = super::row_level_contract::OBSERVATION_COUNT;
pub(super) const SELECTED_UPDATE_OBSERVATION_INDEX: usize = PARTITION_OBSERVATION_INDEX + 1;

pub(super) fn selected_update_observation(cluster_size: usize) -> StatementSpec {
    let mut spec = partition_observation(cluster_size);
    spec.sql = row_level_contract::selected_update_case2_sql(
        row_level_contract::CATALOG,
        row_level_contract::DATABASE,
    );
    spec
}

pub(super) fn partition_observation(cluster_size: usize) -> StatementSpec {
    StatementSpec {
        sql: row_level_contract::partition_move_update_sql(
            row_level_contract::CATALOG,
            row_level_contract::DATABASE,
            1,
        ),
        faults: vec![],
        observations: (0..cluster_size)
            .flat_map(|index| {
                [
                    (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), None),
                    (index, novarocks_failpoint::observation_marker::ObservationAction::WriterMarker.as_str(), None),
                    (index, novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker.as_str(), None),
                    (index, novarocks_failpoint::observation_marker::ObservationAction::ExchangeSnapshot.as_str(), None),
                ]
            })
            .collect(),
    }
}
const LIMITS: &[(&str, i64)] = &[
    ("exchange_instance_floor_slots", 1),
    ("exchange_instance_max_slots", 2),
    ("exchange_floating_slots", 0),
    ("exchange_stream_window", 1),
];

pub(super) fn launch_config() -> Result<ScenarioLaunchConfig> {
    let mut launch = connector_launch_config();
    let backend = launch
        .config_overlay
        .be
        .take()
        .context("original row-level connector BE overlay")?;
    anyhow::ensure!(
        backend.matches("[runtime]\n").count() == 1,
        "original row-level BE overlay must contain one runtime table"
    );
    let settings = LIMITS
        .iter()
        .map(|(name, value)| format!("{name} = {value}\n"))
        .collect::<String>();
    launch.config_overlay.be =
        Some(backend.replacen("[runtime]\n", &format!("[runtime]\n{settings}"), 1));
    Ok(launch)
}

pub(super) fn require_effective_config(context: &mut ScenarioContext) -> Result<()> {
    let evidence: serde_json::Value =
        serde_json::from_slice(context.effective_launch_config_evidence().artifact_bytes())?;
    let roles = evidence["semantics"]["roles"]
        .as_array()
        .context("small-slots writer has no effective role inventory")?;
    let backends = roles
        .iter()
        .filter(|role| role["role"] == "be")
        .collect::<Vec<_>>();
    anyhow::ensure!(
        backends.len() == context.handle().be_count(),
        "small-slots effective BE inventory differs from the actual server"
    );
    for backend in backends {
        let config = &backend["effective_config"];
        anyhow::ensure!(
            config["type"] == "table" && config["value"]["runtime"]["type"] == "table",
            "small-slots writer effective BE config has no runtime table"
        );
        for &(name, expected) in LIMITS {
            let actual = &config["value"]["runtime"]["value"][name];
            anyhow::ensure!(
                actual["type"] == "integer" && actual["value"].as_i64() == Some(expected),
                "small-slots writer effective BE {name} must be exactly {expected}, got {actual}"
            );
        }
    }
    Ok(())
}

pub(super) fn add_append_holds(specs: &mut [StatementSpec], cluster_size: usize) -> Result<()> {
    anyhow::ensure!(
        specs.len() == PARTITION_OBSERVATION_INDEX && cluster_size > 0,
        "small-slots writer requires the complete original row-level observation plan"
    );
    // The first two original nominations are the unmatched MERGEs at DOP 1/4.
    // All original writer/Task/reader observations remain independently live.
    for spec in &mut specs[..2] {
        anyhow::ensure!(
            spec.faults.is_empty(),
            "original row-level MERGE already declares a Native fault"
        );
        spec.faults = (0..cluster_size)
            .map(|index| (index, APPEND_HOLD))
            .collect();
    }
    Ok(())
}

fn end_scopes(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
) -> (Vec<usize>, Result<()>) {
    let mut ended = Vec::new();
    let mut result = Ok(());
    for &(index, kind) in &selected.faults {
        let end = context
            .handle()
            .end_selected_query_lifecycle_fault(index, kind, &selected.token);
        if end.is_ok() {
            ended.push(index);
        }
        result = writer_statement_actor::combine(result, end);
    }
    (ended, result)
}

/// Return the actual SQL result independently of control/cleanup failures.
/// The caller still owns its original lifecycle observation and public-error
/// reentry, including when this coordinator failed to observe or end a scope.
pub(super) fn query_drop(
    context: &mut ScenarioContext,
    control: &mut SqlSession,
    selected: &SelectedStatement,
    pending: PendingStatement,
    dop: i32,
) -> (
    Option<std::result::Result<StatementResult, StatementFailure>>,
    Result<()>,
) {
    if selected.faults.is_empty() {
        let cancel = pending.cancel_handle();
        let outcome = control.run_pending(pending);
        let cleanup = if matches!(&outcome, Err(error) if !matches!(error, StatementFailure::Trino(_)))
        {
            let cancelled = cancel
                .delete(selected.deadline)
                .map_err(anyhow::Error::from);
            let shutdown = if cancelled.is_err() {
                context.shutdown()
            } else {
                Ok(())
            };
            writer_statement_actor::combine(cancelled, shutdown)
        } else {
            Ok(())
        };
        return (Some(outcome), cleanup);
    }
    if selected.faults.iter().any(|(_, kind)| *kind != APPEND_HOLD) {
        let (_, ended) = end_scopes(context, selected);
        let ended = writer_statement_actor::combine(
            ended,
            pending
                .cancel_handle()
                .delete(selected.deadline)
                .map_err(Into::into),
        );
        return (
            None,
            writer_statement_actor::combine(
                Err(anyhow!(
                    "row-level append coordinator received another Native fault"
                )),
                ended,
            ),
        );
    }
    let client_query_id = pending.query_id.clone();
    let cancel = pending.cancel_handle();
    let deadline = selected.deadline;
    let (outcome, coordination, hits, ended, killed, joined) = thread::scope(|scope| {
        // Borrow the original session: its SET pipeline_dop effects and this
        // exact armed statement remain with the existing row-level contract.
        let query = thread::Builder::new()
            .name("small-slots-original-row-level-sql".into())
            .spawn_scoped(scope, move || control.run_pending(pending));
        let query = match query {
            Ok(query) => query,
            Err(error) => {
                let (ended, cleanup) = end_scopes(context, selected);
                return (
                    None,
                    writer_statement_actor::combine(
                        Err(error.into()),
                        writer_statement_actor::combine(
                            cleanup,
                            cancel.delete(deadline).map_err(Into::into),
                        ),
                    ),
                    Vec::new(),
                    ended,
                    false,
                    false,
                );
            }
        };
        let mut hits = Vec::new();
        let progress = (|| -> Result<()> {
            loop {
                hits.clear();
                for &(index, kind) in &selected.faults {
                    if context.handle().selected_query_lifecycle_fault_hit(
                        index,
                        kind,
                        &selected.token,
                    )? {
                        hits.push(index);
                    }
                }
                anyhow::ensure!(
                    !query.is_finished(),
                    "original row-level SQL exited before its held append was released"
                );
                if !hits.is_empty() {
                    return Ok(());
                }
                let remaining = context.remaining("observe original writer append hold")?;
                // Polling does not slow the writer: its real append Future is
                // held by the authenticated Native scope until end_scopes.
                thread::sleep(remaining.min(Duration::from_millis(10)));
            }
        })();
        // Every nominated scope is ended on every path, before joining the
        // original SQL thread. SessionEnd releases append without Task stop.
        let (ended, released) = end_scopes(context, selected);
        let mut coordination = writer_statement_actor::combine(progress, released);
        let mut killed = false;
        if coordination.is_err() && !query.is_finished() {
            killed = true;
            let cancelled = cancel.delete(deadline).map_err(anyhow::Error::from);
            let shutdown = if cancelled.is_err() {
                context.shutdown()
            } else {
                Ok(())
            };
            coordination = writer_statement_actor::combine(
                coordination,
                writer_statement_actor::combine(cancelled, shutdown),
            );
        }
        let outcome = match query.join() {
            Ok(outcome) => Some(outcome),
            Err(_) => {
                coordination = writer_statement_actor::combine(
                    coordination,
                    Err(anyhow!("original row-level SQL thread panicked")),
                );
                None
            }
        };
        // A finished reader with a local/HTTP failure did not report the
        // server terminal. Cancel the same query once even after actual join.
        if !killed
            && matches!(&outcome, Some(Err(error)) if !matches!(error, StatementFailure::Trino(_)))
        {
            killed = true;
            let cancelled = cancel.delete(deadline).map_err(anyhow::Error::from);
            let shutdown = if cancelled.is_err() {
                context.shutdown()
            } else {
                Ok(())
            };
            coordination = writer_statement_actor::combine(
                coordination,
                writer_statement_actor::combine(cancelled, shutdown),
            );
        }
        (outcome, coordination, hits, ended, killed, true)
    });
    let retained = (|| -> Result<()> {
        std::fs::write(
            context
                .scenario_root()
                .join(format!("small-slots-writer-append-dop-{dop}.json")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "requested_dop": dop,
                "selected_statement_token": selected.token,
                "client_query_id": client_query_id,
                "actual_append_hold_hit_backends": hits,
                "scope_end_successful_backends": ended,
                "original_sql_thread_joined": joined,
                "original_sql_outcome_available": outcome.is_some(),
                "original_sql_succeeded": matches!(&outcome, Some(Ok(_))),
                "scenario_requested_cancel": killed,
                "coordination_succeeded": coordination.is_ok(),
            }))?,
        )?;
        Ok(())
    })();
    (
        outcome,
        writer_statement_actor::combine(coordination, retained),
    )
}

pub(super) fn retain_idle_flow(context: &mut ScenarioContext) -> Result<()> {
    let snapshots = super::super::exchange_flow::await_flow_idle(context)?;
    anyhow::ensure!(
        snapshots.iter().all(|snapshot| {
            snapshot.instance_floor == 1
                && snapshot.instance_max == 2
                && snapshot.floating == 0
                && snapshot.stream_window == 1
        }),
        "actual exchange owners differ from the selected small-slots geometry: {snapshots:?}"
    );
    std::fs::write(
        context
            .scenario_root()
            .join("small-slots-writer-final-flow.json"),
        serde_json::to_vec_pretty(&snapshots)?,
    )?;
    println!("small-slots row-level actual exchange flow idle: {snapshots:?}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiny_diagnostics_append_exact_statements_without_changing_original_indices() {
        let mut specs = row_level_contract::observation_specs(3);
        let original_sql: Vec<_> = specs.iter().map(|spec| spec.sql.clone()).collect();
        add_append_holds(&mut specs, 3).unwrap();
        assert_eq!(specs.len(), PARTITION_OBSERVATION_INDEX);
        specs.push(partition_observation(3));
        specs.push(selected_update_observation(3));
        assert_eq!(specs.len(), 6);
        assert_eq!(
            specs[..4]
                .iter()
                .map(|spec| spec.sql.clone())
                .collect::<Vec<_>>(),
            original_sql
        );
        let diagnostic = &specs[PARTITION_OBSERVATION_INDEX];
        assert_eq!(
            diagnostic.sql,
            "UPDATE row_level_writer.row_level_writer_db.partition_move_1 SET p=1,v=11 WHERE id=1"
        );
        assert!(diagnostic.faults.is_empty());
        assert_eq!(diagnostic.observations.len(), 12);
        assert!(
            diagnostic
                .observations
                .iter()
                .all(|(_, _, attempt)| attempt.is_none())
        );
        assert_eq!(row_level_contract::observation_specs(3).len(), 4);
        let selected = &specs[SELECTED_UPDATE_OBSERVATION_INDEX];
        assert_eq!(
            selected.sql,
            "MERGE INTO row_level_writer.row_level_writer_db.selected_update_1 t USING row_level_writer.row_level_writer_db.selected_source_update_1 s ON t.id=s.id AND s.case_id=2 WHEN MATCHED AND s.selected THEN UPDATE SET v=s.new_v"
        );
        assert!(selected.faults.is_empty());
        assert_eq!(selected.observations, diagnostic.observations);
    }

    #[test]
    fn small_slots_preserves_every_original_launch_setting_except_its_four_limits() {
        let original = connector_launch_config();
        let small = launch_config().unwrap();
        assert_eq!(small.config_overlay.fe, original.config_overlay.fe);
        assert_eq!(
            small.config_overlay.be_by_index,
            original.config_overlay.be_by_index
        );
        let original_be = original.config_overlay.be.unwrap();
        let mut small_be = small.config_overlay.be.unwrap();
        for (name, expected) in [
            ("exchange_instance_floor_slots", 1),
            ("exchange_instance_max_slots", 2),
            ("exchange_floating_slots", 0),
            ("exchange_stream_window", 1),
        ] {
            let setting = format!("{name} = {expected}\n");
            assert_eq!(small_be.matches(setting.as_str()).count(), 1);
            assert!(!original_be.contains(name));
            small_be = small_be.replacen(setting.as_str(), "", 1);
        }
        assert_eq!(small_be, original_be);
        assert_eq!(
            DistributedWriterRowLevel::default().name(),
            "connector/distributed-writer-row-level"
        );
        assert_eq!(
            DistributedWriterRowLevel::small_slots().name(),
            "connector/distributed-writer-row-level-small-slots"
        );
    }
}
