//! Stream-load entrypoint for local (on-disk parquet) tables: parse the raw
//! CSV/JSON payload into a `Vec<Vec<Literal>>` and dispatch it through the
//! normal local-insert path.
//!
//! The CSV/JSON parsing lives in the neutral
//! `crate::standalone::engine::stream_load` module so the managed-lake
//! backend can share it.

use std::sync::Arc;

use super::normalize_identifier;
use crate::plan_nodes::TFileFormatType;
use crate::sql::parser::ast::InsertSource;
use crate::standalone::engine::stream_load::{
    parse_csv_stream_load_rows, parse_json_stream_load_rows, parse_stream_load_columns,
};
use crate::standalone::engine::{
    ResolvedLocalTableName, StandaloneState, StandaloneStreamLoadRequest,
    StandaloneStreamLoadResult,
};

use super::insert::insert_into_local_table;

pub(crate) fn stream_load_local_table(
    state: &Arc<StandaloneState>,
    request: StandaloneStreamLoadRequest,
) -> Result<StandaloneStreamLoadResult, String> {
    let database = normalize_identifier(&request.database)?;
    let table = normalize_identifier(&request.table)?;
    let resolved = ResolvedLocalTableName { database, table };
    let table_def = {
        let guard = state.catalog.read().expect("standalone catalog read lock");
        guard.get(&resolved.database, &resolved.table)?
    };

    let insert_columns = parse_stream_load_columns(request.columns.as_deref(), &table_def)?;
    let rows = match request.format_type {
        TFileFormatType::FORMAT_JSON => parse_json_stream_load_rows(
            &request.payload,
            &insert_columns,
            request.jsonpaths.as_deref(),
            request.strip_outer_array.unwrap_or(false),
        )?,
        TFileFormatType::FORMAT_CSV_PLAIN => parse_csv_stream_load_rows(
            &request.payload,
            &insert_columns,
            request.column_separator.as_deref(),
            request.row_delimiter.as_deref(),
            request.skip_header.unwrap_or(0),
            request.trim_space.unwrap_or(false),
            request.enclose,
            request.escape,
        )?,
        other => {
            return Err(format!(
                "standalone stream load only supports CSV/JSON, got {:?}",
                other
            ));
        }
    };

    insert_into_local_table(
        state,
        &resolved,
        &table_def,
        &insert_columns,
        &InsertSource::Values(rows.clone()),
    )?;
    Ok(StandaloneStreamLoadResult {
        loaded_rows: rows.len() as i64,
        loaded_bytes: request.payload.len() as i64,
    })
}
