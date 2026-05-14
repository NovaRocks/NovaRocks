//! Iceberg-backed materialized-view backend.

use std::sync::{Arc, Weak};

use crate::connector::backend::MvBackend;
use crate::engine::StandaloneState;
use crate::engine::mv::lifecycle::{
    BackendRefreshOutcome, BackendRefreshPlan, CreateMvRequest, DropMvRequest, ListMvsRequest,
    MvListRow, MvStorageEngine, RefreshCtx, RefreshError, RefreshOutcome, RefreshPlan,
    RefreshRequest,
};

pub(crate) struct IcebergMvBackend {
    state: Weak<StandaloneState>,
}

impl IcebergMvBackend {
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

impl MvBackend for IcebergMvBackend {
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn create_mv(&self, req: CreateMvRequest) -> Result<(), String> {
        let state = self.state()?;
        crate::engine::mv::iceberg_refresh::create_iceberg_mv(
            &state,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
        )
        .map(|_| ())
    }

    fn drop_mv(&self, req: DropMvRequest) -> Result<(), String> {
        let state = self.state()?;
        crate::engine::mv::iceberg_refresh::drop_iceberg_mv(
            &state,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.stmt,
        )
        .map(|_| ())
    }

    fn list_mvs(&self, req: ListMvsRequest) -> Result<Vec<MvListRow>, String> {
        let state = self.state()?;
        crate::connector::starrocks::managed::mv_ddl::list_mv_rows(
            &state,
            req.current_catalog.as_deref(),
            &req.stmt,
            Some(MvStorageEngine::Iceberg),
        )
    }

    fn plan_refresh(&self, req: RefreshRequest) -> Result<RefreshPlan, RefreshError> {
        let state = self.state().map_err(RefreshError::pre_commit)?;
        crate::engine::mv::iceberg_refresh::plan_iceberg_mv_refresh(
            &state,
            req.current_catalog.as_deref(),
            &req.current_database,
            &req.statement,
            req.target,
        )
    }

    fn execute_refresh(
        &self,
        plan: &RefreshPlan,
        _ctx: &mut RefreshCtx,
    ) -> Result<RefreshOutcome, RefreshError> {
        let BackendRefreshPlan::Iceberg(plan_payload) = &plan.backend_plan else {
            return Err(RefreshError::user(
                "iceberg backend received non-iceberg refresh plan",
            ));
        };
        let state = self.state().map_err(RefreshError::pre_commit)?;
        let outcome =
            crate::engine::mv::iceberg_refresh::execute_iceberg_mv_refresh(&state, plan_payload)?;
        Ok(RefreshOutcome {
            mv_id: plan.mv_id,
            target: plan.target.clone(),
            rows: None,
            base_snapshots: Default::default(),
            base_table_uuids: Default::default(),
            target_snapshot_id: None,
            backend_outcome: BackendRefreshOutcome::Iceberg(outcome),
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
