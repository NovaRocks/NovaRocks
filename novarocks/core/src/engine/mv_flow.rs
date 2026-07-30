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

//! Materialized-view statement dispatch through `MvBackend`.

use std::sync::Arc;

use crate::engine::mv::lifecycle::{
    CreateMvRequest, DropMvRequest, ListMvsRequest, RefreshCtx, RefreshError, RefreshRequest,
};
use crate::engine::statement::{AlterIcebergPropertiesStmt, PropertiesOp};
use crate::engine::{StandaloneState, StatementResult};
use crate::mv::model::{MvStorageEngine, MvTarget};
use crate::mv::persistence::definition::{
    StoredMvDefinition, StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest,
};
use crate::mv::repository::MvRepository;
use crate::runtime::query_result::QueryResult;
use crate::sql::parser::ast::{
    AlterMaterializedViewAction, AlterMaterializedViewStmt, CreateMaterializedViewStmt,
    DropMaterializedViewStmt, MaterializedViewRefreshPolicy, RefreshMaterializedViewStmt,
    ShowMaterializedViewsStmt,
};
use crate::sql::parser::query_refs::extract_three_part_table_ref_occurrences;
use novarocks_catalog::identifier::normalize_identifier;

fn backend_by_engine(
    state: &Arc<StandaloneState>,
    engine: MvStorageEngine,
) -> Result<Arc<dyn crate::connector::backend::MvBackend>, String> {
    state
        .connectors
        .read()
        .expect("connector registry read")
        .mv_backend(engine.backend_name())
}

#[cfg(test)]
mod lifecycle_tests {
    use std::sync::{Arc, Mutex};

    use crate::connector::backend::MvBackend;
    use crate::engine::mv::lifecycle::{
        BackendRefreshOutcome, BackendRefreshPlan, CreateMvRequest, DropMvRequest, ListMvsRequest,
        MvListRow, RefreshCtx, RefreshError, RefreshOutcome, RefreshPlan, RefreshRequest,
        StarRocksTableRefreshOutcome, StarRocksTableRefreshPlan,
    };
    use crate::mv::model::{MvStorageEngine, MvTarget};
    use crate::mv::refresh::planning::{RefreshPlanContract, RefreshStateBaseline};
    use crate::mv::refresh::snapshot::ExecutableRefreshDecision;
    use novarocks_catalog::identifier::TableIdentity;

    #[derive(Default)]
    struct Calls {
        plan: usize,
        execute: usize,
        commit: usize,
        rollback: usize,
        rollback_outcome_present: Option<bool>,
        rollback_ctx: Option<RefreshCtx>,
        compensation_count: usize,
    }

    struct MockBackend {
        calls: Arc<Mutex<Calls>>,
        plan_err: Option<RefreshError>,
        execute_err: Option<RefreshError>,
        commit_err: Option<RefreshError>,
        rollback_err: Option<RefreshError>,
    }

    impl MockBackend {
        fn ok(calls: Arc<Mutex<Calls>>) -> Self {
            Self {
                calls,
                plan_err: None,
                execute_err: None,
                commit_err: None,
                rollback_err: None,
            }
        }
    }

    impl MvBackend for MockBackend {
        fn name(&self) -> &'static str {
            "mock"
        }

        fn create_mv(&self, _req: CreateMvRequest) -> Result<(), String> {
            Ok(())
        }

        fn drop_mv(&self, _req: DropMvRequest) -> Result<(), String> {
            Ok(())
        }

        fn list_mvs(&self, _req: ListMvsRequest) -> Result<Vec<MvListRow>, String> {
            Ok(vec![])
        }

        fn plan_refresh(
            &self,
            req: RefreshRequest,
            _connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<RefreshPlan, RefreshError> {
            self.calls.lock().unwrap().plan += 1;
            if let Some(err) = &self.plan_err {
                return Err(err.clone());
            }
            Ok(RefreshPlan {
                contract: RefreshPlanContract {
                    mv_id: Some(1),
                    target: req.target,
                    storage_engine: MvStorageEngine::StarRocks,
                    decision: ExecutableRefreshDecision::Incremental,
                    state_baseline: RefreshStateBaseline::Pinless,
                    base_refs: vec![TableIdentity {
                        catalog: "ice".to_string(),
                        namespace: "ns".to_string(),
                        table: "base".to_string(),
                    }],
                    snapshot_pins: Default::default(),
                    affected_partitions: crate::mv::model::AffectedTargetPartitions::not_derived(
                        "mock MV backend does not plan affected partitions",
                    ),
                },
                backend_plan: BackendRefreshPlan::StarRocks(StarRocksTableRefreshPlan {
                    stmt: req.statement,
                    current_catalog: req.current_catalog,
                    current_database: req.current_database,
                }),
            })
        }

        fn execute_refresh(
            &self,
            plan: &RefreshPlan,
            _ctx: &mut RefreshCtx,
        ) -> Result<RefreshOutcome, RefreshError> {
            self.calls.lock().unwrap().execute += 1;
            if let Some(err) = &self.execute_err {
                return Err(err.clone());
            }
            Ok(RefreshOutcome {
                mv_id: plan.contract.mv_id,
                target: plan.contract.target.clone(),
                rows: Some(0),
                base_snapshots: Default::default(),
                base_table_uuids: Default::default(),
                target_snapshot_id: None,
                backend_outcome: BackendRefreshOutcome::StarRocks(StarRocksTableRefreshOutcome {
                    completed_inside_execute: true,
                }),
            })
        }

        fn commit_refresh(
            &self,
            _outcome: &RefreshOutcome,
            _ctx: &mut RefreshCtx,
        ) -> Result<(), RefreshError> {
            self.calls.lock().unwrap().commit += 1;
            if let Some(err) = &self.commit_err {
                return Err(err.clone());
            }
            Ok(())
        }

        fn rollback_refresh(
            &self,
            outcome: Option<&RefreshOutcome>,
            ctx: &mut RefreshCtx,
        ) -> Result<(), RefreshError> {
            let mut calls = self.calls.lock().unwrap();
            calls.rollback += 1;
            calls.rollback_outcome_present = Some(outcome.is_some());
            calls.rollback_ctx = Some(ctx.clone());
            if let Some(err) = &self.rollback_err {
                return Err(err.clone());
            }
            Ok(())
        }
    }

    fn refresh_request() -> RefreshRequest {
        let stmt = match crate::sql::parser::parse_sql("REFRESH MATERIALIZED VIEW mv1")
            .expect("parse")
            .remove(0)
        {
            crate::sql::parser::ast::Statement::RefreshMaterializedView(stmt) => stmt,
            other => panic!("unexpected statement: {other:?}"),
        };
        RefreshRequest {
            target: MvTarget {
                catalog: None,
                database: "default".to_string(),
                name: "mv1".to_string(),
            },
            current_catalog: None,
            current_database: "default".to_string(),
            statement: stmt,
        }
    }

    fn run_test_refresh_lifecycle(
        backend: Arc<dyn MvBackend>,
        request: RefreshRequest,
    ) -> Result<(), String> {
        super::run_refresh_lifecycle(backend, request, &crate::connector::test_request_context())
    }

    #[test]
    fn plan_error_stops_lifecycle_without_rollback() {
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut backend = MockBackend::ok(Arc::clone(&calls));
        backend.plan_err = Some(RefreshError::user("bad plan"));
        let err = run_test_refresh_lifecycle(Arc::new(backend), refresh_request()).unwrap_err();
        assert_eq!(err, "bad plan");
        let calls = calls.lock().unwrap();
        assert_eq!(calls.plan, 1);
        assert_eq!(calls.execute, 0);
        assert_eq!(calls.commit, 0);
        assert_eq!(calls.rollback, 0);
    }

    #[test]
    fn execute_error_rolls_back_without_commit() {
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut backend = MockBackend::ok(Arc::clone(&calls));
        backend.execute_err = Some(RefreshError::pre_commit("execute failed"));
        let err = run_test_refresh_lifecycle(Arc::new(backend), refresh_request()).unwrap_err();
        assert_eq!(err, "execute failed");
        let calls = calls.lock().unwrap();
        assert_eq!(calls.plan, 1);
        assert_eq!(calls.execute, 1);
        assert_eq!(calls.commit, 0);
        assert_eq!(calls.rollback, 1);
    }

    #[test]
    fn pre_commit_failure_rolls_back_empty_context_without_compensation() {
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut backend = MockBackend::ok(Arc::clone(&calls));
        backend.execute_err = Some(RefreshError::pre_commit("contract drift"));

        let error = run_test_refresh_lifecycle(Arc::new(backend), refresh_request())
            .expect_err("pre-commit validation must fail");

        assert_eq!(error, "contract drift");
        let calls = calls.lock().unwrap();
        assert_eq!(calls.plan, 1);
        assert_eq!(calls.execute, 1);
        assert_eq!(calls.commit, 0);
        assert_eq!(calls.rollback, 1);
        assert_eq!(calls.rollback_outcome_present, Some(false));
        let rollback_ctx = calls.rollback_ctx.as_ref().expect("rollback context");
        assert_eq!(rollback_ctx.refresh_id, None);
        assert_eq!(rollback_ctx.expected_target_snapshot_id, None);
        assert!(!rollback_ctx.recovery_required);
        assert_eq!(calls.compensation_count, 0);
    }

    #[test]
    fn execute_commit_unknown_does_not_roll_back() {
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut backend = MockBackend::ok(Arc::clone(&calls));
        backend.execute_err = Some(RefreshError::commit_unknown("execute commit unknown"));
        let err = run_test_refresh_lifecycle(Arc::new(backend), refresh_request()).unwrap_err();
        assert_eq!(err, "execute commit unknown");
        let calls = calls.lock().unwrap();
        assert_eq!(calls.plan, 1);
        assert_eq!(calls.execute, 1);
        assert_eq!(calls.commit, 0);
        assert_eq!(calls.rollback, 0);
    }

    #[test]
    fn commit_unknown_does_not_roll_back() {
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut backend = MockBackend::ok(Arc::clone(&calls));
        backend.commit_err = Some(RefreshError::commit_unknown("commit unknown"));
        let err = run_test_refresh_lifecycle(Arc::new(backend), refresh_request()).unwrap_err();
        assert_eq!(err, "commit unknown");
        let calls = calls.lock().unwrap();
        assert_eq!(calls.plan, 1);
        assert_eq!(calls.execute, 1);
        assert_eq!(calls.commit, 1);
        assert_eq!(calls.rollback, 0);
    }

    #[test]
    fn rollback_error_is_appended_to_original_error() {
        let calls = Arc::new(Mutex::new(Calls::default()));
        let mut backend = MockBackend::ok(Arc::clone(&calls));
        backend.execute_err = Some(RefreshError::pre_commit("execute failed"));
        backend.rollback_err = Some(RefreshError::pre_commit("rollback failed"));
        let err = run_test_refresh_lifecycle(Arc::new(backend), refresh_request()).unwrap_err();
        assert_eq!(
            err,
            "execute failed; additionally failed to rollback MV refresh: rollback failed"
        );
        assert_eq!(calls.lock().unwrap().rollback, 1);
    }
}

fn default_mv_storage_engine(_state: &Arc<StandaloneState>) -> &str {
    "iceberg"
}

fn storage_engine_for_create(
    state: &Arc<StandaloneState>,
    stmt: &CreateMaterializedViewStmt,
) -> Result<MvStorageEngine, String> {
    let raw = stmt
        .properties
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("storage_engine"))
        .map(|(_, value)| value.as_str())
        .unwrap_or_else(|| default_mv_storage_engine(state))
        .trim();
    match raw.to_ascii_lowercase().as_str() {
        "iceberg" => Ok(MvStorageEngine::Iceberg),
        "starrocks" => Err(
            "storage_engine='starrocks' is no longer supported for standalone materialized views; use storage_engine='iceberg'"
                .to_string(),
        ),
        _ => Err(format!(
            "unknown materialized view storage_engine `{raw}`"
        )),
    }
}

fn existing_mv_storage_engine_by_target(
    state: &Arc<StandaloneState>,
    target: &crate::engine::mv::iceberg_refresh::IcebergMvTarget,
) -> Result<Option<MvStorageEngine>, String> {
    let Some(definition) = state
        .mv_repository
        .find_by_target(&MvTarget {
            catalog: Some(target.catalog.clone()),
            database: target.namespace.clone(),
            name: target.table.clone(),
        })
        .map_err(|e| format!("load MV definition by target failed: {e}"))?
    else {
        return Ok(None);
    };
    MvStorageEngine::from_sql_str(&definition.storage_engine).map(Some)
}

fn stored_refresh_policy(
    policy: &MaterializedViewRefreshPolicy,
) -> (StoredMvRefreshPolicy, Option<i64>) {
    match policy {
        MaterializedViewRefreshPolicy::Manual => (StoredMvRefreshPolicy::Manual, None),
        MaterializedViewRefreshPolicy::AsyncOnChange => {
            (StoredMvRefreshPolicy::AsyncOnChange, None)
        }
        MaterializedViewRefreshPolicy::AsyncInterval { interval_ms } => {
            (StoredMvRefreshPolicy::AsyncInterval, Some(*interval_ms))
        }
    }
}

pub(crate) fn initial_refresh_configuration_for_create(
    policy: &MaterializedViewRefreshPolicy,
) -> crate::mv::repository::InitialMvRefreshConfiguration {
    let (policy, interval_ms) = stored_refresh_policy(policy);
    crate::mv::repository::InitialMvRefreshConfiguration {
        policy,
        paused: false,
        interval_ms,
        max_staleness_ms: None,
        next_refresh_after_ms: None,
    }
}

pub(crate) fn refresh_metadata_request_for_create(
    mv_id: i64,
    policy: &MaterializedViewRefreshPolicy,
) -> UpdateMvRefreshMetadataRequest {
    let initial = initial_refresh_configuration_for_create(policy);
    UpdateMvRefreshMetadataRequest {
        mv_id,
        refresh_policy: initial.policy,
        refresh_paused: false,
        refresh_interval_ms: initial.interval_ms,
        max_staleness_ms: initial.max_staleness_ms,
        last_scheduler_error: None,
        next_refresh_after_ms: None,
    }
}

fn refresh_metadata_request_for_policy(
    definition: &StoredMvDefinition,
    policy: &MaterializedViewRefreshPolicy,
    refresh_paused: bool,
) -> UpdateMvRefreshMetadataRequest {
    let (refresh_policy, refresh_interval_ms) = stored_refresh_policy(policy);
    UpdateMvRefreshMetadataRequest {
        mv_id: definition.mv_id,
        refresh_policy,
        refresh_paused,
        refresh_interval_ms,
        max_staleness_ms: definition.max_staleness_ms,
        last_scheduler_error: None,
        next_refresh_after_ms: None,
    }
}

fn load_definition_for_alter(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    name: &crate::sql::parser::ast::ObjectName,
) -> Result<StoredMvDefinition, String> {
    let target =
        crate::engine::mv::iceberg_refresh::resolve_refresh_target(current_catalog, db, name)?;
    let Some(definition) = state
        .mv_repository
        .find_by_target(&MvTarget {
            catalog: Some(target.catalog.clone()),
            database: target.namespace.clone(),
            name: target.table.clone(),
        })
        .map_err(|e| format!("load MV definition by target failed: {e}"))?
    else {
        return Err(format!(
            "materialized view does not exist: {}.{}.{}",
            target.catalog, target.namespace, target.table
        ));
    };
    if MvStorageEngine::from_sql_str(&definition.storage_engine)? != MvStorageEngine::Iceberg {
        return Err(
            "ALTER MATERIALIZED VIEW is only supported for Iceberg-backed materialized views"
                .to_string(),
        );
    }
    Ok(definition)
}

fn refresh_error_with_rollback(
    original: RefreshError,
    rollback: Result<(), RefreshError>,
) -> String {
    match rollback {
        Ok(()) => original.to_string(),
        Err(rollback_err) => format!(
            "{}; additionally failed to rollback MV refresh: {}",
            original, rollback_err
        ),
    }
}

fn run_refresh_lifecycle(
    backend: Arc<dyn crate::connector::backend::MvBackend>,
    req: RefreshRequest,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    crate::connector::validate_request_context(connector_context)?;
    let mut ctx = RefreshCtx::new(connector_context.clone());
    let plan = backend
        .plan_refresh(req, connector_context)
        .map_err(|err| err.to_string())?;
    crate::connector::validate_request_context(&ctx.connector_context)?;
    let outcome = match backend.execute_refresh(&plan, &mut ctx) {
        Ok(outcome) => outcome,
        Err(err) if err.kind.should_rollback_after_commit() => {
            let rollback = backend.rollback_refresh(None, &mut ctx);
            return Err(refresh_error_with_rollback(err, rollback));
        }
        Err(err) => {
            ctx.recovery_required = true;
            tracing::warn!(
                backend = backend.name(),
                recovery_required = ctx.recovery_required,
                error = %err,
                "MV refresh execution returned a non-rollbackable error; recovery is required"
            );
            return Err(err.to_string());
        }
    };
    match backend.commit_refresh(&outcome, &mut ctx) {
        Ok(()) => Ok(()),
        Err(err) if err.kind.should_rollback_after_commit() => {
            let rollback = backend.rollback_refresh(Some(&outcome), &mut ctx);
            Err(refresh_error_with_rollback(err, rollback))
        }
        Err(err) => {
            ctx.recovery_required = true;
            tracing::warn!(
                backend = backend.name(),
                recovery_required = ctx.recovery_required,
                error = %err,
                "MV refresh commit result is unknown; recovery is required"
            );
            Err(err.to_string())
        }
    }
}

pub(crate) fn create_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &CreateMaterializedViewStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    let engine = storage_engine_for_create(state, stmt)?;
    backend_by_engine(state, engine)?.create_mv(CreateMvRequest {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        current_database: db.to_string(),
        connector_context: connector_context.clone(),
    })?;
    Ok(StatementResult::Ok)
}

pub(crate) fn drop_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let target = crate::engine::mv::iceberg_refresh::resolve_refresh_target(
        current_catalog,
        db,
        &stmt.name,
    )?;
    if let Some(engine) = existing_mv_storage_engine_by_target(state, &target)?
        && engine != MvStorageEngine::Iceberg
    {
        return Err(
            "DROP MATERIALIZED VIEW is only supported for Iceberg-backed materialized views"
                .to_string(),
        );
    }
    backend_by_engine(state, MvStorageEngine::Iceberg)?.drop_mv(DropMvRequest {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        current_database: db.to_string(),
    })?;
    Ok(StatementResult::Ok)
}

#[cfg(test)]
pub(crate) fn alter_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &AlterMaterializedViewStmt,
) -> Result<StatementResult, String> {
    alter_mv_with_connector_context(
        state,
        current_catalog,
        db,
        stmt,
        &crate::connector::test_request_context(),
    )
}

pub(crate) fn alter_mv_with_connector_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &AlterMaterializedViewStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    if matches!(
        stmt.action,
        AlterMaterializedViewAction::Repartition(_) | AlterMaterializedViewAction::SetProperties(_)
    ) {
        let current_catalog = current_catalog.ok_or_else(|| {
            "ALTER MATERIALIZED VIEW requires current Iceberg catalog".to_string()
        })?;
        let target = crate::engine::mv::iceberg_refresh::resolve_refresh_target(
            Some(current_catalog),
            db,
            &stmt.name,
        )?;
        let engine = existing_mv_storage_engine_by_target(state, &target)?.ok_or_else(|| {
            format!(
                "materialized view {}.{}.{} not found",
                target.catalog, target.namespace, target.table
            )
        })?;
        if engine != MvStorageEngine::Iceberg {
            return Err(
                "ALTER MATERIALIZED VIEW is only supported for Iceberg-backed materialized views"
                    .to_string(),
            );
        }
        if let AlterMaterializedViewAction::SetProperties(entries) = &stmt.action {
            crate::connector::iceberg::catalog::alter_table_properties(
                state,
                &AlterIcebergPropertiesStmt {
                    table: stmt.name.clone(),
                    op: PropertiesOp::Set {
                        entries: entries.clone(),
                    },
                },
                Some(current_catalog),
                db,
            )?;
            return Ok(StatementResult::Ok);
        }
        return crate::engine::mv::iceberg_refresh::repartition_iceberg_mv_with_connector_context(
            state,
            Some(current_catalog),
            db,
            stmt,
            connector_context,
        );
    }
    let definition = load_definition_for_alter(state, current_catalog, db, &stmt.name)?;
    let req = match &stmt.action {
        AlterMaterializedViewAction::SetRefresh(policy) => {
            refresh_metadata_request_for_policy(&definition, policy, definition.refresh_paused)
        }
        AlterMaterializedViewAction::PauseRefresh => UpdateMvRefreshMetadataRequest {
            mv_id: definition.mv_id,
            refresh_policy: definition.refresh_policy.clone(),
            refresh_paused: true,
            refresh_interval_ms: definition.refresh_interval_ms,
            max_staleness_ms: definition.max_staleness_ms,
            last_scheduler_error: definition.last_scheduler_error.clone(),
            next_refresh_after_ms: definition.next_refresh_after_ms,
        },
        AlterMaterializedViewAction::ResumeRefresh => UpdateMvRefreshMetadataRequest {
            mv_id: definition.mv_id,
            refresh_policy: definition.refresh_policy.clone(),
            refresh_paused: false,
            refresh_interval_ms: definition.refresh_interval_ms,
            max_staleness_ms: definition.max_staleness_ms,
            last_scheduler_error: definition.last_scheduler_error.clone(),
            next_refresh_after_ms: definition.next_refresh_after_ms,
        },
        AlterMaterializedViewAction::Repartition(_) => {
            unreachable!("repartition is handled before refresh metadata update")
        }
        AlterMaterializedViewAction::SetProperties(_) => {
            unreachable!("properties are handled before refresh metadata update")
        }
    };
    state
        .mv_repository
        .update_refresh_metadata(req.clone())
        .map_err(|e| format!("update MV refresh metadata failed: {e}"))?;
    crate::engine::mv::iceberg_refresh::sync_iceberg_mv_descriptor(
        state,
        &definition,
        &req.refresh_policy,
        req.refresh_paused,
        req.refresh_interval_ms,
    )
    .map_err(|e| format!("sync Iceberg MV descriptor refresh metadata failed: {e}"))?;
    Ok(StatementResult::Ok)
}

#[cfg(test)]
pub(crate) fn refresh_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    refresh_mv_with_connector_context(
        state,
        current_catalog,
        db,
        stmt,
        &crate::connector::test_request_context(),
    )
}

pub(crate) fn refresh_mv_with_connector_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &RefreshMaterializedViewStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    let target = crate::engine::mv::iceberg_refresh::resolve_refresh_target(
        current_catalog,
        db,
        &stmt.name,
    )?;
    let engine = existing_mv_storage_engine_by_target(state, &target)?.ok_or_else(|| {
        format!(
            "materialized view does not exist: {}.{}.{}",
            target.catalog, target.namespace, target.table
        )
    })?;
    if engine != MvStorageEngine::Iceberg {
        return Err(
            "REFRESH MATERIALIZED VIEW is only supported for Iceberg-backed materialized views"
                .to_string(),
        );
    };
    let target = MvTarget {
        catalog: Some(target.catalog),
        database: target.namespace,
        name: target.table,
    };
    let requested_object = crate::mv::dependency::model::iceberg_mv_dependency_ref(
        target
            .catalog
            .as_deref()
            .ok_or_else(|| "iceberg MV refresh target missing catalog".to_string())?,
        &target.database,
        &target.name,
    );
    let steps =
        crate::engine::mv::dependency::build_upstream_refresh_steps(state, &requested_object)?;
    for step in steps {
        let backend = backend_by_engine(state, step.storage_engine)?;
        let statement = RefreshMaterializedViewStmt {
            name: crate::sql::parser::ast::ObjectName {
                parts: match step.target.catalog.as_deref() {
                    Some(_) => vec![step.target.database.clone(), step.target.name.clone()],
                    None => vec![step.target.name.clone()],
                },
            },
            full: stmt.full,
        };
        let req = RefreshRequest {
            target: step.target.clone(),
            current_catalog: step.target.catalog.clone(),
            current_database: step.target.database.clone(),
            statement,
        };
        if let Err(err) = run_refresh_lifecycle(backend, req, connector_context) {
            if step.object != requested_object {
                return Err(format!(
                    "cannot refresh materialized view {}: upstream materialized view {} failed: {err}",
                    requested_object.display_name().trim_start_matches("mv:"),
                    step.object.display_name().trim_start_matches("mv:")
                ));
            }
            return Err(err);
        }
    }
    crate::engine::mv_maintenance::notify_refresh_completed(state);
    Ok(StatementResult::Ok)
}

pub(crate) fn list_mvs(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    stmt: &ShowMaterializedViewsStmt,
) -> Result<StatementResult, String> {
    let req = ListMvsRequest {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
    };
    let mut rows = backend_by_engine(state, MvStorageEngine::Iceberg)?.list_mvs(req)?;
    rows.sort_by(|left, right| {
        left.database
            .cmp(&right.database)
            .then(left.name.cmp(&right.name))
    });
    Ok(StatementResult::Query(
        crate::engine::mv::analysis_adapter::build_mv_rows_result(&rows)?,
    ))
}

/// Analyze the output column types of a MV SELECT SQL without executing it.
///
/// Runs the semantic analyzer on the ORIGINAL (un-rewritten) SQL and returns
/// the visible output columns. This is used by the aggregate MV refresh path
/// to obtain visible-shaped types for `build_aggregate_mv_layout`, which expects
/// types matching `shape.visible_outputs` — not the state-shaped columns that
/// the rewritten SELECT (AVG → SUM + COUNT) produces.
pub(crate) fn analyze_visible_output_types(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<crate::sql::analysis::OutputColumn>, String> {
    Ok(analyze_visible_query(state, current_database, sql, connector_context)?.output_columns)
}

pub(crate) fn analyze_visible_query(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::sql::analysis::ResolvedQuery, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err(
            "aggregate MV visible type analysis: stored SQL must be a SELECT query".to_string(),
        );
    };

    let catalog_service = crate::engine::catalog_service_snapshot(state);
    let connectors = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let provider = crate::engine::build_catalog_service_provider(
        None,
        &catalog_service,
        &connectors,
        connector_context.clone(),
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );
    let (resolved, _cte_registry, _factory) =
        crate::sql::analyzer::analyze(&query, &provider, current_database)
            .map_err(|e| format!("aggregate MV visible type analysis failed: {e}"))?;
    Ok(resolved)
}

pub(crate) fn execute_query_for_mv_refresh(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<QueryResult, String> {
    execute_query_for_mv_refresh_with_catalog(state, None, current_database, sql, connector_context)
}

pub(crate) fn execute_query_for_mv_refresh_with_catalog(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    sql: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(mut query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };

    if crate::engine::query_prep::has_time_travel_refs(&query) {
        crate::engine::query_prep::rewrite_time_travel_refs(
            state,
            current_catalog,
            current_database,
            &mut query,
            connector_context,
        )?;
    }

    crate::engine::execute_preexpanded_mv_refresh_query_with_catalog_service_with_connector_context(
        state,
        current_catalog,
        current_database,
        &query,
        None,
        connector_context,
    )
}

fn normalize_incremental_mv_base_ref(
    base_ref: &novarocks_catalog::identifier::TableIdentity,
) -> Result<(String, String, String), String> {
    Ok((
        normalize_identifier(&base_ref.catalog)?,
        normalize_identifier(&base_ref.namespace)?,
        normalize_identifier(&base_ref.table)?,
    ))
}

pub(crate) fn validate_incremental_mv_base_ref(
    query: &sqlparser::ast::Query,
    base_ref: &novarocks_catalog::identifier::TableIdentity,
) -> Result<(String, String, String), String> {
    let refs = extract_three_part_table_ref_occurrences(query);
    if refs.len() != 1 {
        return Err(format!(
            "incremental MV refresh stored SQL must reference exactly one 3-part Iceberg table, got {}",
            refs.len()
        ));
    }

    let actual = {
        let (catalog, namespace, table) = &refs[0];
        (
            normalize_identifier(catalog).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid catalog reference: {e}")
            })?,
            normalize_identifier(namespace).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid namespace reference: {e}")
            })?,
            normalize_identifier(table).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid table reference: {e}")
            })?,
        )
    };
    let expected = normalize_incremental_mv_base_ref(base_ref)?;
    if actual != expected {
        return Err(format!(
            "incremental MV refresh stored SQL base table mismatch: expected {}.{}.{}, got {}.{}.{}",
            expected.0, expected.1, expected.2, actual.0, actual.1, actual.2
        ));
    }
    Ok(expected)
}

pub(crate) fn write_mv_delete_temp_parquet(
    namespace: &str,
    table_name: &str,
    deleted_rows: &[arrow::record_batch::RecordBatch],
) -> Result<(String, i64, Option<i64>), String> {
    let first_batch = deleted_rows
        .first()
        .ok_or_else(|| "delete-side mv refresh has no rows to write".to_string())?;
    let dir = std::env::temp_dir().join(format!(
        "novarocks_mv_deletes_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("create temp dir for delete-side mv refresh: {e}"))?;
    let path = dir.join(format!("{namespace}_{table_name}.parquet"));
    let schema = first_batch.schema();
    let file = std::fs::File::create(&path)
        .map_err(|e| format!("create temp parquet for delete-side mv refresh: {e}"))?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)
        .map_err(|e| format!("create temp parquet writer for delete-side mv refresh: {e}"))?;
    for batch in deleted_rows {
        writer
            .write(batch)
            .map_err(|e| format!("write temp parquet batch for delete-side mv refresh: {e}"))?;
    }
    writer
        .close()
        .map_err(|e| format!("close temp parquet writer for delete-side mv refresh: {e}"))?;

    // The downstream HDFS_SCAN treats this size as `range.file_len` and seeks
    // to `(file_len - 8)` to read the parquet footer magic. We must report the
    // actual on-disk parquet size, not the in-memory Arrow column footprint —
    // the latter is materially smaller (one row of a couple of i64/string
    // columns is ~200-400 bytes in memory but ~700+ bytes as a parquet file
    // including magic + schema + footer), which makes the reader truncate and
    // surface "Invalid Parquet file. Corrupt footer".
    let total_size = std::fs::metadata(&path)
        .map(|m| m.len() as i64)
        .map_err(|e| format!("stat temp parquet for delete-side mv refresh: {e}"))?;
    let total_rows = Some(
        deleted_rows
            .iter()
            .map(|batch| batch.num_rows() as i64)
            .sum(),
    );

    Ok((format!("file://{}", path.display()), total_size, total_rows))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize sql");
        let statement =
            crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse sql");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        *query
    }

    fn base_ref() -> novarocks_catalog::identifier::TableIdentity {
        novarocks_catalog::identifier::TableIdentity {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
        }
    }

    #[test]
    fn validate_incremental_mv_base_ref_rejects_projection_subquery_extra_ref() {
        let query =
            parse_query("select k, (select count(*) from ice.db.t) as c from ice.db.t where v > 0");
        let err = super::validate_incremental_mv_base_ref(&query, &base_ref())
            .expect_err("extra 3-part ref must fail");

        assert!(err.contains("exactly one 3-part Iceberg table, got 2"));
    }

    #[test]
    fn validate_incremental_mv_base_ref_rejects_where_subquery_extra_ref() {
        let query =
            parse_query("select k from ice.db.t where exists (select 1 from ice.db.t where v > 0)");
        let err = super::validate_incremental_mv_base_ref(&query, &base_ref())
            .expect_err("extra 3-part ref must fail");

        assert!(err.contains("exactly one 3-part Iceberg table, got 2"));
    }

    #[test]
    fn validate_incremental_mv_base_ref_rejects_having_subquery_extra_ref() {
        let query = parse_query(
            "select k, count(*) from ice.db.t group by k \
             having count(*) > (select count(*) from ice.db.t)",
        );
        let err = super::validate_incremental_mv_base_ref(&query, &base_ref())
            .expect_err("extra 3-part ref must fail");

        assert!(err.contains("exactly one 3-part Iceberg table, got 2"));
    }

    #[test]
    fn delete_temp_delta_file_omits_row_lineage_metadata() {
        let file = crate::engine::query_prep::delete_temp_iceberg_file_for_query(
            "file:///tmp/delete.parquet".to_string(),
            128,
            Some(1),
            None,
        );

        assert_eq!(file.first_row_id, None);
        assert_eq!(file.data_sequence_number, None);
    }

    #[test]
    fn mv_delete_temp_parquet_preserves_iceberg_field_ids() {
        let metadata = HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())]);
        let field = Field::new("renamed_id", DataType::Int32, false).with_metadata(metadata);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        )
        .expect("batch");
        assert_eq!(
            batch
                .schema()
                .field(0)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("7")
        );

        let (path, _, _) = super::write_mv_delete_temp_parquet("ns", "orders", &[batch])
            .expect("write temp parquet");
        let local_path = path.strip_prefix("file://").expect("file path");
        let file = std::fs::File::open(local_path).expect("open temp parquet");
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("builder");
        assert_eq!(
            builder
                .schema()
                .field(0)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("7")
        );
    }

    /// Regression: the returned `total_size` must equal the on-disk parquet
    /// file length, not the in-memory Arrow column footprint. The downstream
    /// HDFS_SCAN treats this value as `range.file_len` and seeks to
    /// `(file_len - 8)` to read the parquet footer magic; a smaller value
    /// (Arrow buffer size) makes the reader read into data bytes and report
    /// "Invalid Parquet file. Corrupt footer".
    #[test]
    fn mv_delete_temp_parquet_size_matches_on_disk_length() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10])) as ArrayRef,
            ],
        )
        .expect("batch");

        let (path, total_size, _) =
            super::write_mv_delete_temp_parquet("ns", "orders", &[batch]).expect("write");
        let local_path = path.strip_prefix("file://").expect("file path");
        let on_disk = std::fs::metadata(local_path)
            .expect("stat temp parquet")
            .len() as i64;

        assert_eq!(
            total_size, on_disk,
            "write_mv_delete_temp_parquet must return on-disk file length \
             (got total_size={total_size}, on_disk={on_disk}); a smaller value \
             causes downstream HDFS_SCAN to treat the file as truncated"
        );
    }
}
