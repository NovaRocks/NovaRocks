//! Materialized-view statement dispatch through `MvBackend`.

use std::sync::Arc;

use crate::runtime::query_result::QueryResult;
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, RefreshMaterializedViewStmt,
    ShowMaterializedViewsStmt,
};
use crate::standalone::engine::{StandaloneState, StatementResult};

fn mv_backend(
    state: &Arc<StandaloneState>,
) -> Result<Arc<dyn crate::connector::backend::MvBackend>, String> {
    state
        .connectors
        .read()
        .expect("connector registry read")
        .mv_backend("managed")
}

pub(crate) fn create_mv(
    state: &Arc<StandaloneState>,
    db: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.create_mv(stmt, db)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn drop_mv(
    state: &Arc<StandaloneState>,
    db: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.drop_mv(stmt, db)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn refresh_mv(
    state: &Arc<StandaloneState>,
    db: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.refresh_mv(stmt, db)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn list_mvs(
    state: &Arc<StandaloneState>,
    stmt: &ShowMaterializedViewsStmt,
) -> Result<StatementResult, String> {
    let result: QueryResult = mv_backend(state)?.list_mvs(stmt)?;
    Ok(StatementResult::Query(result))
}
