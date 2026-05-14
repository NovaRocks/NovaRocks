//! `CatalogBackend` / `TableSource` / `TableSink` / `MvBackend`
//! implementations for managed-lake, wrapping `catalog.rs`, `ddl.rs`,
//! `txn.rs`, `mv_ddl.rs`, and `mv_refresh.rs`.

use std::sync::{Arc, Weak};

use arrow::record_batch::RecordBatch;

use crate::connector::backend::{
    CatalogBackend, CreateTableRequest, MvBackend, ResolvedTable, TableSink, TableSource,
};
use crate::engine::StandaloneState;
use crate::engine::mv::lifecycle::{
    BackendRefreshOutcome, BackendRefreshPlan, CreateMvRequest, DropMvRequest, ListMvsRequest,
    ManagedLakeRefreshOutcome, ManagedLakeRefreshPlan, MvBaseRef, MvListRow, MvStorageEngine,
    RefreshCtx, RefreshError, RefreshMode, RefreshOutcome, RefreshPlan, RefreshRequest,
};
use crate::sql::catalog::TableDef;
use crate::sql::parser::ast::{Literal, ObjectName};

pub(crate) struct ManagedLakeBackend {
    state: Weak<StandaloneState>,
}

impl ManagedLakeBackend {
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

impl CatalogBackend for ManagedLakeBackend {
    fn name(&self) -> &'static str {
        "managed"
    }

    fn namespace_exists(&self, _catalog: &str, database: &str) -> Result<bool, String> {
        let state = self.state()?;
        let logical = state.catalog.read().expect("standalone catalog read lock");
        logical.database_exists(database)
    }

    fn create_namespace(&self, _catalog: &str, database: &str) -> Result<(), String> {
        let state = self.state()?;
        let mut logical = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        logical.create_database(database)
    }

    fn drop_namespace(&self, _catalog: &str, database: &str, force: bool) -> Result<(), String> {
        let state = self.state()?;
        if force {
            let table_names = state
                .managed_lake
                .read()
                .expect("standalone managed lake read lock")
                .list_tables_in_database(database)
                .unwrap_or_default();
            for table in table_names {
                super::ddl::drop_managed_table(&state, database, &table)?;
            }
            if state.managed_lake_config.is_some() {
                super::ddl::drop_managed_database_entry(&state, database)?;
            }
        }
        let mut logical = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        logical.drop_database(database)
    }

    fn create_table(&self, req: CreateTableRequest) -> Result<(), String> {
        if !req.partition_fields.is_empty() {
            return Err(
                "managed-lake CREATE TABLE does not support Iceberg PARTITION BY".to_string(),
            );
        }
        let state = self.state()?;
        super::ddl::create_managed_table(
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
        let logical = state.catalog.read().expect("standalone catalog read lock");
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
        super::ddl::drop_managed_table(&state, database, table).map(|_| ())
    }

    fn load_table(
        &self,
        _catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<ResolvedTable, String> {
        let state = self.state()?;
        let logical = state.catalog.read().expect("standalone catalog read lock");
        let table_def = logical.get(database, table)?;
        Ok(ResolvedTable {
            catalog: String::new(),
            namespace: database.to_string(),
            table: table.to_string(),
            columns: table_def.columns,
        })
    }
}

pub(crate) struct ManagedLakeTableSource {
    _state: Weak<StandaloneState>,
}

impl ManagedLakeTableSource {
    pub(crate) fn new(state: &Arc<StandaloneState>) -> Self {
        Self {
            _state: Arc::downgrade(state),
        }
    }
}

impl TableSource for ManagedLakeTableSource {
    fn name(&self) -> &'static str {
        "managed"
    }

    fn build_table_def(&self, _table: &ResolvedTable) -> Result<TableDef, String> {
        Err(
            "managed-lake table definitions are registered through register_managed_table_in_catalog"
                .to_string(),
        )
    }
}

pub(crate) struct ManagedLakeTableSink {
    state: Weak<StandaloneState>,
}

impl ManagedLakeTableSink {
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

impl TableSink for ManagedLakeTableSink {
    fn name(&self) -> &'static str {
        "managed"
    }

    fn append_rows(&self, table: &ResolvedTable, rows: &[Vec<Literal>]) -> Result<(), String> {
        let state = self.state()?;
        super::txn::insert_rows_into_managed_lake_table(
            &state,
            &table.namespace,
            &table.table,
            rows,
        )
    }

    fn append_batch(&self, table: &ResolvedTable, batch: RecordBatch) -> Result<(), String> {
        let state = self.state()?;
        super::txn::insert_batch_into_managed_lake_table(
            &state,
            &table.namespace,
            &table.table,
            batch,
        )
    }

    fn supports_pipeline_insert(&self) -> bool {
        true
    }
}

pub(crate) struct ManagedLakeMvBackend {
    state: Weak<StandaloneState>,
}

impl ManagedLakeMvBackend {
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

impl MvBackend for ManagedLakeMvBackend {
    fn name(&self) -> &'static str {
        "managed"
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
            Some(MvStorageEngine::ManagedLake),
        )
    }

    fn plan_refresh(&self, req: RefreshRequest) -> Result<RefreshPlan, RefreshError> {
        Ok(RefreshPlan {
            mv_id: None,
            target: req.target,
            storage_engine: MvStorageEngine::ManagedLake,
            mode: RefreshMode::Incremental,
            base_refs: vec![MvBaseRef {
                catalog: "managed".to_string(),
                namespace: req.current_database.clone(),
                table: req.statement.name.parts.join("."),
            }],
            snapshot_pins: Default::default(),
            backend_plan: BackendRefreshPlan::ManagedLake(ManagedLakeRefreshPlan {
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
        let BackendRefreshPlan::ManagedLake(plan_payload) = &plan.backend_plan else {
            return Err(RefreshError::user(
                "managed-lake backend received non-managed refresh plan",
            ));
        };
        let state = self.state().map_err(RefreshError::pre_commit)?;
        super::mv_refresh::refresh_mv(
            &state,
            plan_payload.current_catalog.as_deref(),
            &plan_payload.current_database,
            &plan_payload.stmt,
        )
        .map_err(RefreshError::pre_commit)?;
        Ok(RefreshOutcome {
            mv_id: plan.mv_id,
            target: plan.target.clone(),
            rows: None,
            base_snapshots: Default::default(),
            base_table_uuids: Default::default(),
            target_snapshot_id: None,
            backend_outcome: BackendRefreshOutcome::ManagedLake(ManagedLakeRefreshOutcome {
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
