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

//! `CatalogBackend` / `TableSource` / `TableSink` / `MvBackend`
//! implementations for StarRocks table, wrapping `catalog.rs`, `ddl.rs`,
//! `txn.rs`, `mv_ddl.rs`, and `mv_refresh.rs`.

use std::sync::{Arc, Weak};

use arrow::record_batch::RecordBatch;

use crate::connector::backend::{
    CatalogBackend, CreateTableRequest, MvBackend, ResolvedTable, TableSink, TableSource,
};
use crate::engine::StandaloneState;
use crate::engine::mv::lifecycle::{
    BackendRefreshOutcome, BackendRefreshPlan, CreateMvRequest, DropMvRequest, ListMvsRequest,
    MvListRow, RefreshCtx, RefreshError, RefreshOutcome, RefreshPlan, RefreshRequest,
    StarRocksTableRefreshOutcome, StarRocksTableRefreshPlan,
};
use crate::mv::model::{MvStorageEngine, MvTarget};
use crate::mv::refresh::execution::{
    RefreshExecutionObservation, ValidatedRefreshExecution, validate_refresh_execution,
};
use crate::mv::refresh::planning::{RefreshPlanContract, RefreshStateBaseline};
use crate::mv::refresh::snapshot::ExecutableRefreshDecision;
use crate::sql::parser::ast::{Literal, ObjectName};
use crate::sql::planner::table::TableDef;
use novarocks_catalog::identifier::TableIdentity;

pub(crate) struct StarRocksTableBackend {
    state: Weak<StandaloneState>,
}

impl StarRocksTableBackend {
    pub(crate) fn new(state: &Arc<StandaloneState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }

    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state
            .upgrade()
            .ok_or_else(|| "standalone state dropped".to_string())
    }
}

impl CatalogBackend for StarRocksTableBackend {
    fn name(&self) -> &'static str {
        "starrocks"
    }

    fn namespace_exists(&self, _catalog: &str, database: &str) -> Result<bool, String> {
        let state = self.state()?;
        let logical = state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog read lock");
        logical.database_exists(database)
    }

    fn create_namespace(&self, _catalog: &str, database: &str) -> Result<(), String> {
        let state = self.state()?;
        let mut logical = state
            .catalog_service
            .local()
            .write()
            .expect("standalone catalog write lock");
        logical.create_database(database)
    }

    fn drop_namespace(&self, _catalog: &str, database: &str, force: bool) -> Result<(), String> {
        let state = self.state()?;
        if force {
            let table_names = state
                .starrocks_table
                .read()
                .expect("standalone StarRocks table read lock")
                .list_tables_in_database(database)
                .unwrap_or_default();
            for table in table_names {
                super::ddl::drop_starrocks_table(&state, database, &table)?;
            }
            if state.starrocks_table_config.is_some() {
                super::ddl::drop_starrocks_database_entry(&state, database)?;
            }
        }
        let mut logical = state
            .catalog_service
            .local()
            .write()
            .expect("standalone catalog write lock");
        logical.drop_database(database)
    }

    fn create_table(&self, req: CreateTableRequest) -> Result<(), String> {
        if !req.partition_fields.is_empty() {
            return Err(
                "StarRocks table CREATE TABLE does not support Iceberg PARTITION BY".to_string(),
            );
        }
        let state = self.state()?;
        super::ddl::create_starrocks_table(
            state.as_ref(),
            &ObjectName {
                parts: vec![req.table],
            },
            &req.namespace,
            &req.columns,
            req.key_desc.as_ref(),
            req.bucket_count,
        )
        .map(|_| ())
    }

    fn table_exists(&self, _catalog: &str, database: &str, table: &str) -> Result<bool, String> {
        let state = self.state()?;
        let logical = state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog read lock");
        Ok(logical.get(database, table).is_ok())
    }

    fn drop_table(
        &self,
        _catalog: &str,
        database: &str,
        table: &str,
        _if_exists: bool,
    ) -> Result<(), String> {
        let state = self.state()?;
        super::ddl::drop_starrocks_table(&state, database, table).map(|_| ())
    }

    fn load_table(
        &self,
        _catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<ResolvedTable, String> {
        let state = self.state()?;
        let logical = state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog read lock");
        let table_def = logical.get(database, table)?;
        Ok(ResolvedTable {
            catalog: String::new(),
            namespace: database.to_string(),
            table: table.to_string(),
            columns: table_def.columns,
        })
    }
}

pub(crate) struct StarRocksTableSource {
    _state: Weak<StandaloneState>,
}

impl StarRocksTableSource {
    pub(crate) fn new(state: &Arc<StandaloneState>) -> Self {
        Self {
            _state: Arc::downgrade(state),
        }
    }
}

impl TableSource for StarRocksTableSource {
    fn name(&self) -> &'static str {
        "starrocks"
    }

    fn build_table_def(&self, _table: &ResolvedTable) -> Result<TableDef, String> {
        Err(
            "StarRocks table definitions are registered through register_starrocks_table_in_catalog"
                .to_string(),
        )
    }
}

pub(crate) struct StarRocksTableSink {
    state: Weak<StandaloneState>,
}

impl StarRocksTableSink {
    pub(crate) fn new(state: &Arc<StandaloneState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }

    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state
            .upgrade()
            .ok_or_else(|| "standalone state dropped".to_string())
    }
}

impl TableSink for StarRocksTableSink {
    fn name(&self) -> &'static str {
        "starrocks"
    }

    fn append_rows(&self, table: &ResolvedTable, rows: &[Vec<Literal>]) -> Result<(), String> {
        let state = self.state()?;
        super::txn::insert_rows_into_starrocks_table(&state, &table.namespace, &table.table, rows)
    }

    fn append_batch(&self, table: &ResolvedTable, batch: RecordBatch) -> Result<(), String> {
        let state = self.state()?;
        super::txn::insert_batch_into_starrocks_table(&state, &table.namespace, &table.table, batch)
    }

    fn supports_pipeline_insert(&self) -> bool {
        true
    }
}

pub(crate) struct StarRocksTableMvBackend {
    state: Weak<StandaloneState>,
}

impl StarRocksTableMvBackend {
    pub(crate) fn new(state: &Arc<StandaloneState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }

    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state
            .upgrade()
            .ok_or_else(|| "standalone state dropped".to_string())
    }
}

impl MvBackend for StarRocksTableMvBackend {
    fn name(&self) -> &'static str {
        "starrocks"
    }

    fn create_mv(&self, req: CreateMvRequest) -> Result<(), String> {
        let state = self.state()?;
        super::mv_ddl::create_mv(
            &state,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
        )
        .map(|_| ())
    }

    fn drop_mv(&self, req: DropMvRequest) -> Result<(), String> {
        let state = self.state()?;
        super::mv_ddl::drop_mv(
            &state,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
        )
        .map(|_| ())
    }

    fn list_mvs(&self, req: ListMvsRequest) -> Result<Vec<MvListRow>, String> {
        let state = self.state()?;
        super::mv_ddl::list_mv_rows(
            &state,
            req.current_catalog.as_deref(),
            &req.stmt,
            Some(MvStorageEngine::StarRocks),
        )
    }

    fn plan_refresh(&self, req: RefreshRequest) -> Result<RefreshPlan, RefreshError> {
        let (database, name) =
            crate::mv::analysis::resolve_mv_name(&req.statement.name, &req.current_database)
                .map_err(RefreshError::pre_commit)?;
        Ok(RefreshPlan {
            contract: RefreshPlanContract {
                mv_id: None,
                target: req.target,
                storage_engine: MvStorageEngine::StarRocks,
                decision: ExecutableRefreshDecision::Incremental,
                state_baseline: RefreshStateBaseline::Pinless,
                base_refs: vec![TableIdentity {
                    catalog: "starrocks".to_string(),
                    namespace: database,
                    table: name,
                }],
                snapshot_pins: Default::default(),
                affected_partitions: crate::mv::model::AffectedTargetPartitions::not_derived(
                    "StarRocks table MV partition planning is not implemented",
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
        let BackendRefreshPlan::StarRocks(plan_payload) = &plan.backend_plan else {
            return Err(RefreshError::user(
                "StarRocks table backend received non-StarRocks refresh plan",
            ));
        };
        let validated = validate_starrocks_refresh_contract(plan_payload, &plan.contract)
            .map_err(RefreshError::pre_commit)?;
        if matches!(
            validated.decision(),
            ExecutableRefreshDecision::FirstRefresh | ExecutableRefreshDecision::Incremental
        ) {
            let state = self.state().map_err(RefreshError::pre_commit)?;
            super::mv_refresh::refresh_mv(
                &state,
                plan_payload.current_catalog.as_deref(),
                &plan_payload.current_database,
                &plan_payload.stmt,
            )
            .map_err(RefreshError::pre_commit)?;
        }
        Ok(RefreshOutcome {
            mv_id: plan.contract.mv_id,
            target: plan.contract.target.clone(),
            rows: None,
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
        Ok(())
    }

    fn rollback_refresh(
        &self,
        _outcome: Option<&RefreshOutcome>,
        _ctx: &mut RefreshCtx,
    ) -> Result<(), RefreshError> {
        Ok(())
    }
}

fn validate_starrocks_refresh_contract<'a>(
    plan: &StarRocksTableRefreshPlan,
    contract: &'a RefreshPlanContract,
) -> Result<ValidatedRefreshExecution<'a>, String> {
    let (database, name) =
        crate::mv::analysis::resolve_mv_name(&plan.stmt.name, &plan.current_database)?;
    let target = MvTarget {
        catalog: plan.current_catalog.clone(),
        database: database.clone(),
        name: name.clone(),
    };
    let base_refs = [TableIdentity {
        catalog: "starrocks".to_string(),
        namespace: database,
        table: name,
    }];
    let state_baseline = RefreshStateBaseline::Pinless;
    validate_refresh_execution(
        contract,
        &RefreshExecutionObservation {
            backend: MvStorageEngine::StarRocks,
            // This backend has no independently persisted MV id.
            mv_id: None,
            target: &target,
            base_refs: &base_refs,
            state_baseline: &state_baseline,
            snapshot_pins: None,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_and_contract() -> (StarRocksTableRefreshPlan, RefreshPlanContract) {
        let statement = match crate::sql::parser::parse_sql("REFRESH MATERIALIZED VIEW mv1")
            .expect("parse refresh")
            .remove(0)
        {
            crate::sql::parser::ast::Statement::RefreshMaterializedView(statement) => statement,
            other => panic!("unexpected statement: {other:?}"),
        };
        (
            StarRocksTableRefreshPlan {
                stmt: statement,
                current_catalog: None,
                current_database: "default".to_string(),
            },
            RefreshPlanContract {
                mv_id: None,
                target: MvTarget {
                    catalog: None,
                    database: "default".to_string(),
                    name: "mv1".to_string(),
                },
                storage_engine: MvStorageEngine::StarRocks,
                decision: ExecutableRefreshDecision::Incremental,
                state_baseline: RefreshStateBaseline::Pinless,
                base_refs: vec![TableIdentity::new("starrocks", "default", "mv1")],
                snapshot_pins: Default::default(),
                affected_partitions: crate::mv::model::AffectedTargetPartitions::not_derived(
                    "test",
                ),
            },
        )
    }

    #[test]
    fn pinless_refresh_contract_executes_when_identity_matches() {
        let (plan, contract) = plan_and_contract();
        validate_starrocks_refresh_contract(&plan, &contract).expect("valid contract");
    }

    #[test]
    fn refresh_contract_rejects_backend_mismatch() {
        let (plan, mut contract) = plan_and_contract();
        contract.storage_engine = MvStorageEngine::Iceberg;
        assert!(
            validate_starrocks_refresh_contract(&plan, &contract)
                .unwrap_err()
                .contains("backend")
        );
    }

    #[test]
    fn refresh_contract_rejects_target_mismatch() {
        let (plan, mut contract) = plan_and_contract();
        contract.target.name = "other".to_string();
        assert!(
            validate_starrocks_refresh_contract(&plan, &contract)
                .unwrap_err()
                .contains("target")
        );
    }

    #[test]
    fn refresh_contract_rejects_mv_id_mismatch() {
        let (plan, mut contract) = plan_and_contract();
        contract.mv_id = Some(1);
        assert!(
            validate_starrocks_refresh_contract(&plan, &contract)
                .unwrap_err()
                .contains("mv id")
        );
    }
}
