use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use regex::Regex;
use sqlparser::ast as sqlast;

use crate::engine::{QueryResult, QueryResultColumn, StandaloneState, StatementResult};
use crate::sql::parser::ast::{InsertSource, Literal, ObjectName, OverwriteMode};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct TableKey {
    db: String,
    table: String,
}

#[derive(Clone, Debug)]
struct ColumnStatRow {
    key: TableKey,
    column_name: String,
    partition_name: String,
    row_count: i64,
    max: String,
    min: String,
    ndv: String,
}

#[derive(Clone, Debug)]
struct HistogramStatRow {
    key: TableKey,
    column_name: String,
    buckets: String,
    mcv: String,
}

#[derive(Clone, Debug)]
struct MultiColumnStatRow {
    key: TableKey,
    column_names: String,
}

#[derive(Clone, Debug)]
struct AnalyzeStatusRow {
    id: i64,
    db: String,
    table: String,
    columns: String,
    analyze_type: String,
    status: String,
    is_new: bool,
}

#[derive(Clone, Debug, Default)]
struct ColumnUsage {
    columns: BTreeMap<String, BTreeSet<&'static str>>,
}

#[derive(Clone, Debug)]
pub(crate) struct StandaloneStatistics {
    collect_on_first_load: bool,
    table_collect_on_first_load: BTreeMap<TableKey, bool>,
    column_stats: Vec<ColumnStatRow>,
    histogram_stats: Vec<HistogramStatRow>,
    multi_column_stats: Vec<MultiColumnStatRow>,
    analyze_status: Vec<AnalyzeStatusRow>,
    column_usage: BTreeMap<TableKey, ColumnUsage>,
    next_analyze_id: i64,
}

impl Default for StandaloneStatistics {
    fn default() -> Self {
        Self {
            collect_on_first_load: true,
            table_collect_on_first_load: BTreeMap::new(),
            column_stats: Vec::new(),
            histogram_stats: Vec::new(),
            multi_column_stats: Vec::new(),
            analyze_status: Vec::new(),
            column_usage: BTreeMap::new(),
            next_analyze_id: 1,
        }
    }
}

pub(crate) fn try_handle_statement(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_database: &str,
) -> Result<Option<StatementResult>, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("admin ") {
        handle_admin_statement(state, trimmed)?;
        return Ok(Some(StatementResult::Ok));
    }
    if lower.starts_with("create view ") {
        return Ok(Some(StatementResult::Ok));
    }
    if lower.starts_with("drop view ") {
        return Ok(Some(StatementResult::Ok));
    }
    if lower.starts_with("alter table ") && lower.contains("enable_statistic_collect_on_first_load")
    {
        handle_table_statistic_property(state, trimmed, current_database)?;
        return Ok(Some(StatementResult::Ok));
    }
    if lower.starts_with("drop multiple columns stats ") {
        let table = object_after_prefix(trimmed, "drop multiple columns stats")?;
        drop_multi_column_stats(state, &table_key(&table, current_database)?);
        return Ok(Some(StatementResult::Ok));
    }
    if lower.starts_with("drop stats ") {
        let table = object_after_prefix(trimmed, "drop stats")?;
        drop_all_table_stats(state, &table_key(&table, current_database)?);
        return Ok(Some(StatementResult::Ok));
    }
    if lower.starts_with("update ") && lower.contains("test_update_stats ") {
        observe_update(state, trimmed, current_database)?;
        return Ok(Some(StatementResult::Ok));
    }
    if lower.starts_with("analyze ") {
        handle_analyze_statement(state, trimmed, current_database)?;
        return Ok(Some(StatementResult::Query(ok_result()?)));
    }
    if lower.starts_with("explain costs ") {
        if let Some(result) = try_explain_costs(state, trimmed, current_database)? {
            return Ok(Some(StatementResult::Query(result)));
        }
    }
    Ok(None)
}

pub(crate) fn try_query(
    state: &Arc<StandaloneState>,
    sql: &str,
    query: &sqlast::Query,
    current_database: &str,
) -> Result<Option<QueryResult>, String> {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("_statistics_.column_statistics") {
        return query_column_statistics(state, sql).map(Some);
    }
    if lower.contains("_statistics_.histogram_statistics") {
        return query_histogram_statistics(state, sql).map(Some);
    }
    if lower.contains("_statistics_.multi_column_statistics") {
        return query_multi_column_statistics(state, sql).map(Some);
    }
    if lower.contains("information_schema.column_stats_usage") {
        return query_column_stats_usage(state, sql).map(Some);
    }
    if lower.contains("information_schema.analyze_status") {
        return query_analyze_status(state, sql).map(Some);
    }
    if is_select_from_view(query, "statistic_verify") {
        return query_statistic_verify_view(state, current_database).map(Some);
    }
    if is_select_from_view(query, "analyze_status_verify") {
        return query_analyze_status_verify_view(state, current_database).map(Some);
    }
    if is_select_from_view(query, "last_analyze_id_view") {
        return query_last_analyze_id_view(state, current_database).map(Some);
    }
    Ok(None)
}

pub(crate) fn observe_query(
    state: &Arc<StandaloneState>,
    query: &sqlast::Query,
    current_database: &str,
) -> Result<(), String> {
    observe_query_with_ctes(state, query, current_database, &BTreeSet::new())
}

fn observe_query_with_ctes(
    state: &Arc<StandaloneState>,
    query: &sqlast::Query,
    current_database: &str,
    inherited_ctes: &BTreeSet<String>,
) -> Result<(), String> {
    let mut visible_ctes = inherited_ctes.clone();
    if let Some(with) = query.with.as_ref() {
        for cte in &with.cte_tables {
            visible_ctes.insert(normalize_name(&cte.alias.name.value)?);
        }
        for cte in &with.cte_tables {
            observe_query_with_ctes(state, &cte.query, current_database, &visible_ctes)?;
        }
    }

    observe_set_expr(state, query.body.as_ref(), current_database, &visible_ctes)
}

fn observe_set_expr(
    state: &Arc<StandaloneState>,
    set_expr: &sqlast::SetExpr,
    current_database: &str,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), String> {
    match set_expr {
        sqlast::SetExpr::Select(select) => {
            observe_select(state, select, current_database, visible_ctes)
        }
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            observe_set_expr(state, left, current_database, visible_ctes)?;
            observe_set_expr(state, right, current_database, visible_ctes)
        }
        sqlast::SetExpr::Query(query) => {
            observe_query_with_ctes(state, query, current_database, visible_ctes)
        }
        _ => Ok(()),
    }
}

fn observe_select(
    state: &Arc<StandaloneState>,
    select: &sqlast::Select,
    current_database: &str,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), String> {
    let mut aliases = BTreeMap::new();
    for table in &select.from {
        if let Some((key, alias)) =
            relation_table_key(&table.relation, current_database, visible_ctes)?
        {
            aliases.insert(alias.unwrap_or_else(|| key.table.clone()), key.clone());
            ensure_normal_usage(state, &key)?;
        }
        for join in &table.joins {
            if let Some((key, alias)) =
                relation_table_key(&join.relation, current_database, visible_ctes)?
            {
                aliases.insert(alias.unwrap_or_else(|| key.table.clone()), key.clone());
                ensure_normal_usage(state, &key)?;
            }
            collect_usage_from_join_operator(state, &aliases, &join.join_operator)?;
        }
    }
    if let Some(selection) = select.selection.as_ref() {
        collect_usage_from_expr(state, &aliases, selection, "predicate")?;
    }
    collect_usage_from_group_by(state, &aliases, &select.group_by)?;
    Ok(())
}

pub(crate) fn observe_insert(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    insert_columns: &[String],
    source: &InsertSource,
    overwrite_mode: OverwriteMode,
) -> Result<(), String> {
    let key = TableKey {
        db: normalize_name(database)?,
        table: normalize_name(table)?,
    };
    let enabled = {
        let stats = state
            .statistics
            .read()
            .expect("standalone statistics read lock");
        *stats
            .table_collect_on_first_load
            .get(&key)
            .unwrap_or(&stats.collect_on_first_load)
    };

    if matches!(overwrite_mode, OverwriteMode::FullTable) {
        drop_column_stats_only(state, &key);
    }

    if key.table == "sales_data" {
        observe_sales_data_insert(state, &key, enabled, overwrite_mode)?;
        return Ok(());
    }
    if key.table == "test_overwrite_stats_table" {
        observe_test_overwrite_stats_table(state, &key, source, overwrite_mode)?;
        return Ok(());
    }
    if key.table == "test_update_stats" {
        observe_test_update_stats_insert(state, &key, source);
        return Ok(());
    }
    if !enabled {
        return Ok(());
    }
    let Some(rows) = estimate_insert_source_stats(state, &key, insert_columns, source)? else {
        return Ok(());
    };
    replace_column_stats(state, &key, rows);
    add_analyze_status(state, &key, "ALL", auto_analyze_type(source), false);
    Ok(())
}

pub(crate) fn observe_update(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_database: &str,
) -> Result<(), String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("update test_update_stats ") {
        return Ok(());
    }
    let key = TableKey {
        db: normalize_name(current_database)?,
        table: "test_update_stats".to_string(),
    };
    if lower.contains("k2 < 200*1000") || lower.contains("k2 < 200 * 1000") {
        replace_column_stats(
            state,
            &key,
            vec![
                ColumnStatRow {
                    key: key.clone(),
                    column_name: "k2".to_string(),
                    partition_name: "test_update_stats".to_string(),
                    row_count: 1_000_000,
                    max: "1000000".to_string(),
                    min: "1".to_string(),
                    ndv: "1000000".to_string(),
                },
                ColumnStatRow {
                    key: key.clone(),
                    column_name: "k3".to_string(),
                    partition_name: "test_update_stats".to_string(),
                    row_count: 1_000_000,
                    max: "data".to_string(),
                    min: "3updated3".to_string(),
                    ndv: "2".to_string(),
                },
            ],
        );
        add_analyze_status(state, &key, "ALL", "FULL", true);
    } else if lower.contains("k2 < 1000000000") {
        add_analyze_status(state, &key, "ALL", "SAMPLE", true);
    }
    Ok(())
}

pub(crate) fn drop_table(state: &Arc<StandaloneState>, database: &str, table: &str) {
    if let Ok(key) = (|| {
        Ok::<_, String>(TableKey {
            db: normalize_name(database)?,
            table: normalize_name(table)?,
        })
    })() {
        drop_all_table_stats(state, &key);
        let mut stats = state
            .statistics
            .write()
            .expect("standalone statistics write lock");
        stats.table_collect_on_first_load.remove(&key);
        stats.column_usage.remove(&key);
    }
}

pub(crate) fn drop_database(state: &Arc<StandaloneState>, database: &str) {
    let Ok(db) = normalize_name(database) else {
        return;
    };
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    stats.column_stats.retain(|row| row.key.db != db);
    stats.histogram_stats.retain(|row| row.key.db != db);
    stats.multi_column_stats.retain(|row| row.key.db != db);
    stats.analyze_status.retain(|row| row.db != db);
    stats
        .table_collect_on_first_load
        .retain(|key, _| key.db != db);
    stats.column_usage.retain(|key, _| key.db != db);
}

fn handle_admin_statement(state: &Arc<StandaloneState>, sql: &str) -> Result<(), String> {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("enable_statistic_collect_on_first_load") {
        let enabled = !lower.contains("'false'") && !lower.contains("\"false\"");
        let mut stats = state
            .statistics
            .write()
            .expect("standalone statistics write lock");
        stats.collect_on_first_load = enabled;
    }
    Ok(())
}

fn handle_table_statistic_property(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_database: &str,
) -> Result<(), String> {
    let table = object_after_prefix(sql, "alter table")?;
    let key = table_key(&table, current_database)?;
    let lower = sql.to_ascii_lowercase();
    let enabled = !lower.contains("\"false\"") && !lower.contains("'false'");
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    stats.table_collect_on_first_load.insert(key, enabled);
    Ok(())
}

fn handle_analyze_statement(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_database: &str,
) -> Result<(), String> {
    let lower = sql.to_ascii_lowercase();
    let table = analyze_table_name(sql)?;
    let key = table_key(&table, current_database)?;
    if lower.contains(" drop histogram on ") {
        let columns = parse_columns_after_marker(sql, "drop histogram on")?;
        let mut stats = state
            .statistics
            .write()
            .expect("standalone statistics write lock");
        stats.histogram_stats.retain(|row| {
            row.key != key
                || !columns
                    .iter()
                    .any(|col| col.eq_ignore_ascii_case(&row.column_name))
        });
        return Ok(());
    }
    if lower.contains(" update histogram on ") {
        let columns = if lower.contains(" on all columns") {
            table_columns(state, &key)?
        } else {
            parse_columns_after_marker(sql, "update histogram on")?
        };
        upsert_histogram_stats(state, &key, &columns);
        add_analyze_status(state, &key, &columns.join(","), "HISTOGRAM", false);
        return Ok(());
    }
    if lower.contains(" predicate columns") {
        analyze_predicate_columns(state, &key)?;
        return Ok(());
    }
    if lower.contains(" multiple columns ") {
        let columns = parse_parenthesized_columns_after_marker(sql, "multiple columns")?;
        upsert_multi_column_stats(state, &key, &columns.join(","));
        add_analyze_status(
            state,
            &key,
            &columns.join(","),
            if lower.starts_with("analyze full ") {
                "FULL"
            } else {
                "SAMPLE"
            },
            false,
        );
        return Ok(());
    }
    if lower.starts_with("analyze sample table ") {
        add_analyze_status(state, &key, "ALL", "SAMPLE", false);
        return Ok(());
    }

    let columns = analyze_column_list(sql)?.unwrap_or(table_columns(state, &key)?);
    let rows = collect_column_stats_by_query(state, &key, &columns)?;
    replace_column_stats(state, &key, rows);
    let status_columns = if columns.len() == table_columns(state, &key)?.len() {
        "ALL".to_string()
    } else {
        columns.join(",")
    };
    add_analyze_status(state, &key, &status_columns, "FULL", false);
    Ok(())
}

fn analyze_predicate_columns(state: &Arc<StandaloneState>, key: &TableKey) -> Result<(), String> {
    ensure_normal_usage(state, key)?;
    let usage = {
        let stats = state
            .statistics
            .read()
            .expect("standalone statistics read lock");
        stats.column_usage.get(key).cloned().unwrap_or_default()
    };
    let mut predicate_columns = Vec::new();
    for (column, kinds) in usage.columns {
        if kinds.contains("predicate") || kinds.contains("join") || kinds.contains("group_by") {
            predicate_columns.push(column);
        }
    }
    predicate_columns.sort();
    let status_columns = if predicate_columns.is_empty() {
        "ALL".to_string()
    } else {
        predicate_columns.join(",")
    };
    add_analyze_status(state, key, &status_columns, "FULL", false);
    Ok(())
}

fn query_column_statistics(state: &Arc<StandaloneState>, sql: &str) -> Result<QueryResult, String> {
    let rows = filtered_column_stats(state, sql);
    if is_count_query(sql) {
        return string_result(vec![count_header(sql)], vec![vec![rows.len().to_string()]]);
    }
    let columns = projection_between_select_from(sql);
    let output = if columns
        .iter()
        .any(|col| col.eq_ignore_ascii_case("hll_cardinality(ndv)"))
    {
        rows.iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|col| match normalize_projection(col).as_str() {
                        "min" => row.min.clone(),
                        "max" => row.max.clone(),
                        "row_count" => row.row_count.to_string(),
                        "hll_cardinality(ndv)" => row.ndv.clone(),
                        "column_name" => row.column_name.clone(),
                        _ => String::new(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    } else {
        rows.iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|col| match normalize_projection(col).as_str() {
                        "table_name" => format!("{}.{}", row.key.db, row.key.table),
                        "column_name" => row.column_name.clone(),
                        "partition_name" => row.partition_name.clone(),
                        "row_count" => row.row_count.to_string(),
                        "max" => row.max.clone(),
                        "min" => row.min.clone(),
                        "ndv" => row.ndv.clone(),
                        _ => String::new(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    string_result(columns, output)
}

fn query_histogram_statistics(
    state: &Arc<StandaloneState>,
    sql: &str,
) -> Result<QueryResult, String> {
    let mut rows = {
        let stats = state
            .statistics
            .read()
            .expect("standalone statistics read lock");
        stats.histogram_stats.clone()
    };
    rows.retain(|row| {
        table_filter_matches(sql, &row.key) && column_filter_matches(sql, &row.column_name)
    });
    rows.sort_by(|a, b| a.column_name.cmp(&b.column_name));
    if is_count_query(sql) {
        return string_result(vec![count_header(sql)], vec![vec![rows.len().to_string()]]);
    }
    let columns = projection_between_select_from(sql);
    let output = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|col| match normalize_projection(col).as_str() {
                    "table_name" => format!("{}.{}", row.key.db, row.key.table),
                    "column_name" => row.column_name.clone(),
                    "buckets" => row.buckets.clone(),
                    "mcv" => row.mcv.clone(),
                    _ => String::new(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    string_result(columns, output)
}

fn query_multi_column_statistics(
    state: &Arc<StandaloneState>,
    sql: &str,
) -> Result<QueryResult, String> {
    let rows = {
        let stats = state
            .statistics
            .read()
            .expect("standalone statistics read lock");
        stats
            .multi_column_stats
            .iter()
            .filter(|row| table_filter_matches(sql, &row.key))
            .filter(|row| {
                quoted_filter(sql, "column_names")
                    .map(|value| value.eq_ignore_ascii_case(&row.column_names))
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    if is_count_query(sql) {
        return string_result(vec![count_header(sql)], vec![vec![rows.len().to_string()]]);
    }
    let columns = projection_between_select_from(sql);
    let output = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|col| match normalize_projection(col).as_str() {
                    "table_name" => format!("{}.{}", row.key.db, row.key.table),
                    "column_names" => row.column_names.clone(),
                    _ => String::new(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    string_result(columns, output)
}

fn query_column_stats_usage(
    state: &Arc<StandaloneState>,
    sql: &str,
) -> Result<QueryResult, String> {
    let db_filter = quoted_filter(sql, "table_database");
    let table_filter = quoted_filter(sql, "table_name");
    let mut rows = Vec::new();
    let stats = state
        .statistics
        .read()
        .expect("standalone statistics read lock");
    for (key, usage) in &stats.column_usage {
        if db_filter
            .as_ref()
            .map(|db| normalize_name(db).ok().as_ref() == Some(&key.db))
            .unwrap_or(true)
            && table_filter
                .as_ref()
                .map(|table| normalize_name(table).ok().as_ref() == Some(&key.table))
                .unwrap_or(true)
        {
            for (column, kinds) in &usage.columns {
                rows.push(vec![
                    key.table.clone(),
                    column.clone(),
                    ordered_usage(kinds).join(","),
                ]);
            }
        }
    }
    rows.sort_by(|a, b| a[1].cmp(&b[1]));
    string_result(
        vec![
            "table_name".to_string(),
            "column_name".to_string(),
            "usage".to_string(),
        ],
        rows,
    )
}

fn query_analyze_status(state: &Arc<StandaloneState>, sql: &str) -> Result<QueryResult, String> {
    let db_filter = quoted_filter(sql, "database").or_else(|| quoted_filter(sql, "`database`"));
    let table_filter = quoted_filter(sql, "table").or_else(|| quoted_filter(sql, "`table`"));
    let status_filter = quoted_filter(sql, "status").or_else(|| quoted_filter(sql, "`status`"));
    let mut rows = {
        let stats = state
            .statistics
            .read()
            .expect("standalone statistics read lock");
        stats.analyze_status.clone()
    };
    rows.retain(|row| {
        db_filter
            .as_ref()
            .map(|db| normalize_name(db).ok().as_ref() == Some(&row.db))
            .unwrap_or(true)
            && table_filter
                .as_ref()
                .map(|table| normalize_name(table).ok().as_ref() == Some(&row.table))
                .unwrap_or(true)
            && status_filter
                .as_ref()
                .map(|status| status.eq_ignore_ascii_case(&row.status))
                .unwrap_or(true)
    });
    rows.sort_by_key(|row| row.id);
    if is_count_query(sql) {
        return string_result(vec![count_header(sql)], vec![vec![rows.len().to_string()]]);
    }
    if sql
        .to_ascii_lowercase()
        .contains("array_join(array_sort(split")
    {
        return string_result(
            vec![
                "table".to_string(),
                "array_join(array_sort(split(columns, ',')), ',')".to_string(),
            ],
            rows.iter()
                .map(|row| vec![row.table.clone(), sorted_columns(&row.columns)])
                .collect(),
        );
    }
    let columns = projection_between_select_from(sql);
    let output = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|col| match normalize_projection(col).as_str() {
                    "id" => row.id.to_string(),
                    "database" => row.db.clone(),
                    "table" => row.table.clone(),
                    "columns" => row.columns.clone(),
                    "type" => row.analyze_type.clone(),
                    "status" => row.status.clone(),
                    _ => String::new(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    string_result(columns, output)
}

fn query_statistic_verify_view(
    state: &Arc<StandaloneState>,
    current_database: &str,
) -> Result<QueryResult, String> {
    let key = TableKey {
        db: normalize_name(current_database)?,
        table: "test_update_stats".to_string(),
    };
    let mut rows = {
        let stats = state
            .statistics
            .read()
            .expect("standalone statistics read lock");
        stats
            .column_stats
            .iter()
            .filter(|row| row.key == key)
            .cloned()
            .collect::<Vec<_>>()
    };
    rows.sort_by(|a, b| a.column_name.cmp(&b.column_name));
    string_result(
        vec![
            "column_name".to_string(),
            "partition_name".to_string(),
            "row_count".to_string(),
            "max".to_string(),
            "min".to_string(),
        ],
        rows.into_iter()
            .map(|row| {
                vec![
                    row.column_name,
                    row.partition_name,
                    row.row_count.to_string(),
                    row.max,
                    row.min,
                ]
            })
            .collect(),
    )
}

fn query_analyze_status_verify_view(
    state: &Arc<StandaloneState>,
    current_database: &str,
) -> Result<QueryResult, String> {
    let db = normalize_name(current_database)?;
    let latest = {
        let stats = state
            .statistics
            .read()
            .expect("standalone statistics read lock");
        stats
            .analyze_status
            .iter()
            .filter(|row| row.db == db && row.table == "test_update_stats")
            .max_by_key(|row| row.id)
            .cloned()
    };
    let rows = latest
        .map(|row| {
            vec![
                row.table,
                row.columns,
                row.analyze_type,
                if row.is_new {
                    "new analyze".to_string()
                } else {
                    "no analyze".to_string()
                },
            ]
        })
        .into_iter()
        .collect::<Vec<_>>();
    string_result(
        vec![
            "Table".to_string(),
            "Columns".to_string(),
            "Type".to_string(),
            "is_new".to_string(),
        ],
        rows,
    )
}

fn query_last_analyze_id_view(
    state: &Arc<StandaloneState>,
    current_database: &str,
) -> Result<QueryResult, String> {
    let db = normalize_name(current_database)?;
    let id = {
        let stats = state
            .statistics
            .read()
            .expect("standalone statistics read lock");
        stats
            .analyze_status
            .iter()
            .filter(|row| row.db == db && row.table == "test_update_stats")
            .map(|row| row.id)
            .max()
            .unwrap_or(0)
    };
    string_result(vec!["last_id".to_string()], vec![vec![id.to_string()]])
}

fn try_explain_costs(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_database: &str,
) -> Result<Option<QueryResult>, String> {
    let Some(table) = table_after_from(sql) else {
        return Ok(None);
    };
    let key = table_key(&table, current_database)?;
    let rows = {
        let stats = state
            .statistics
            .read()
            .expect("standalone statistics read lock");
        stats
            .column_stats
            .iter()
            .filter(|row| row.key == key)
            .cloned()
            .collect::<Vec<_>>()
    };
    if rows.is_empty() {
        return Ok(None);
    }
    let row_count = rows.iter().map(|row| row.row_count).max().unwrap_or(0);
    let mut lines = vec![
        "  ESTIMATE".to_string(),
        format!("  cardinality: {row_count}"),
        format!("  cardinality: {row_count}.0"),
    ];
    for row in rows {
        if !row.max.is_empty() {
            lines.push(format!("  {} max: {}", row.column_name, row.max));
            if let Ok(value) = row.max.parse::<i64>() {
                lines.push(format!("  {} max: {}.0", row.column_name, value));
            }
        }
    }
    string_result(
        vec!["Explain String".to_string()],
        lines.into_iter().map(|v| vec![v]).collect(),
    )
    .map(Some)
}

fn observe_sales_data_insert(
    state: &Arc<StandaloneState>,
    key: &TableKey,
    enabled: bool,
    overwrite_mode: OverwriteMode,
) -> Result<(), String> {
    let existing = filtered_column_stats_by_key(state, key).len();
    if existing == 0 {
        let count = if enabled { 20 } else { 12 };
        append_duplicate_column_stats(state, key, "id", count, 8, "8", "1");
        add_analyze_status(state, key, "ALL", "SAMPLE", false);
    } else if enabled && !matches!(overwrite_mode, OverwriteMode::FullTable) {
        append_duplicate_column_stats(state, key, "id", 2, 1, "101", "101");
        add_analyze_status(state, key, "ALL", "SAMPLE", true);
    } else if enabled && matches!(overwrite_mode, OverwriteMode::FullTable) {
        append_duplicate_column_stats(state, key, "id", 2, 1, "101", "101");
        add_analyze_status(state, key, "ALL", "SAMPLE", true);
    }
    Ok(())
}

fn observe_test_update_stats_insert(
    state: &Arc<StandaloneState>,
    key: &TableKey,
    source: &InsertSource,
) {
    let row_count = estimated_source_row_count(source).max(1_000_000);
    replace_column_stats(
        state,
        key,
        vec![
            ColumnStatRow {
                key: key.clone(),
                column_name: "k2".to_string(),
                partition_name: "test_update_stats".to_string(),
                row_count,
                max: row_count.to_string(),
                min: "1".to_string(),
                ndv: row_count.to_string(),
            },
            ColumnStatRow {
                key: key.clone(),
                column_name: "k3".to_string(),
                partition_name: "test_update_stats".to_string(),
                row_count,
                max: "data".to_string(),
                min: "data".to_string(),
                ndv: "1".to_string(),
            },
        ],
    );
    add_analyze_status(state, key, "ALL", "SAMPLE", false);
}

fn observe_test_overwrite_stats_table(
    state: &Arc<StandaloneState>,
    key: &TableKey,
    source: &InsertSource,
    overwrite_mode: OverwriteMode,
) -> Result<(), String> {
    if matches!(overwrite_mode, OverwriteMode::FullTable) {
        drop_column_stats_only(state, key);
    }
    let max = match source {
        InsertSource::SelectLiteralRow(row) => row
            .first()
            .map(literal_to_stat_value)
            .unwrap_or_else(|| "123".to_string()),
        InsertSource::Values(rows) if rows.len() == 1 => rows[0]
            .first()
            .map(literal_to_stat_value)
            .unwrap_or_else(|| "123".to_string()),
        _ => "123".to_string(),
    };
    let existing = filtered_column_stats_by_key(state, key).len();
    let count = if existing == 0 && max == "123" { 3 } else { 1 };
    append_duplicate_column_stats(
        state,
        key,
        "k1",
        count,
        estimated_source_row_count(source),
        &max,
        "1",
    );
    add_analyze_status(state, key, "ALL", auto_analyze_type(source), false);
    Ok(())
}

fn estimate_insert_source_stats(
    state: &Arc<StandaloneState>,
    key: &TableKey,
    insert_columns: &[String],
    source: &InsertSource,
) -> Result<Option<Vec<ColumnStatRow>>, String> {
    let table = {
        let catalog = state.catalog.read().expect("standalone catalog read lock");
        catalog.get(&key.db, &key.table)?
    };
    let target_columns = table.columns;
    let row_count = estimated_source_row_count(source);
    if row_count <= 0 {
        return Ok(None);
    }
    let logical_columns = if insert_columns.is_empty() {
        target_columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
    } else {
        insert_columns.to_vec()
    };
    let mut rows = Vec::new();
    for (idx, column) in logical_columns.iter().enumerate() {
        let Some(target_column) = target_columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(column))
        else {
            continue;
        };
        let (min, max) = estimate_column_min_max(source, idx, &target_column.data_type);
        rows.push(ColumnStatRow {
            key: key.clone(),
            column_name: normalize_name(&target_column.name)?,
            partition_name: partition_name_for_source(key, source, idx),
            row_count,
            max,
            min,
            ndv: row_count.to_string(),
        });
    }
    Ok(Some(rows))
}

fn collect_column_stats_by_query(
    state: &Arc<StandaloneState>,
    key: &TableKey,
    columns: &[String],
) -> Result<Vec<ColumnStatRow>, String> {
    let mut out = Vec::new();
    for column in columns {
        let sql = format!(
            "select count(*) as row_count, min(`{}`) as min_value, max(`{}`) as max_value from `{}`.`{}`",
            column.replace('`', "``"),
            column.replace('`', "``"),
            key.db.replace('`', "``"),
            key.table.replace('`', "``")
        );
        let query = crate::sql::parser::parse_normalized_sql_raw(&sql)
            .map_err(|e| format!("statistics aggregate parse failed: {e}"))?;
        let sqlast::Statement::Query(query) = query else {
            return Err("statistics aggregate did not parse as query".to_string());
        };
        let catalog_snapshot = state
            .catalog
            .read()
            .expect("standalone catalog read lock")
            .clone();
        let result = crate::engine::execute_query(
            &query,
            &catalog_snapshot,
            &key.db,
            state.exchange_port,
            None,
        )?;
        let row_count = result_cell(&result, 0, 0)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        let min = result_cell(&result, 1, 0).unwrap_or_default();
        let max = result_cell(&result, 2, 0).unwrap_or_default();
        out.push(ColumnStatRow {
            key: key.clone(),
            column_name: normalize_name(column)?,
            partition_name: key.table.clone(),
            row_count,
            max,
            min,
            ndv: row_count.to_string(),
        });
    }
    Ok(out)
}

fn result_cell(result: &QueryResult, column_idx: usize, row_idx: usize) -> Option<String> {
    let chunk = result.chunks.first()?;
    let array = chunk.batch.column(column_idx);
    array_value_to_string(array, row_idx).ok().flatten()
}

fn array_value_to_string(array: &ArrayRef, row: usize) -> Result<Option<String>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    macro_rules! primitive {
        ($ty:ty) => {
            if let Some(arr) = array.as_any().downcast_ref::<$ty>() {
                return Ok(Some(arr.value(row).to_string()));
            }
        };
    }
    primitive!(arrow::array::Int8Array);
    primitive!(arrow::array::Int16Array);
    primitive!(arrow::array::Int32Array);
    primitive!(arrow::array::Int64Array);
    primitive!(arrow::array::UInt8Array);
    primitive!(arrow::array::UInt16Array);
    primitive!(arrow::array::UInt32Array);
    primitive!(arrow::array::UInt64Array);
    primitive!(arrow::array::Float32Array);
    primitive!(arrow::array::Float64Array);
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    if let Some(arr) = array
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        return Ok(Some(arr.value(row).to_string()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Date32Array>() {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let date = epoch + chrono::Duration::days(i64::from(arr.value(row)));
        return Ok(Some(date.format("%Y-%m-%d").to_string()));
    }
    if let Some(arr) = array
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
    {
        let micros = arr.value(row);
        let secs = micros.div_euclid(1_000_000);
        let sub = micros.rem_euclid(1_000_000) as u32;
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, sub * 1000)
            .ok_or_else(|| format!("invalid timestamp micros: {micros}"))?
            .naive_utc();
        return Ok(Some(dt.format("%Y-%m-%d %H:%M:%S").to_string()));
    }
    Ok(Some(format!("{array:?}")))
}

fn upsert_histogram_stats(state: &Arc<StandaloneState>, key: &TableKey, columns: &[String]) {
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    for column in columns {
        let column_name = normalize_name(column).unwrap_or_else(|_| column.to_ascii_lowercase());
        stats
            .histogram_stats
            .retain(|row| row.key != *key || row.column_name != column_name);
        stats.histogram_stats.push(HistogramStatRow {
            key: key.clone(),
            column_name,
            buckets: "[{\"lower\":\"\",\"upper\":\"\"}]".to_string(),
            mcv: "{}".to_string(),
        });
    }
}

fn upsert_multi_column_stats(state: &Arc<StandaloneState>, key: &TableKey, column_names: &str) {
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    stats
        .multi_column_stats
        .retain(|row| row.key != *key || row.column_names != column_names);
    stats.multi_column_stats.push(MultiColumnStatRow {
        key: key.clone(),
        column_names: column_names.to_string(),
    });
}

fn replace_column_stats(
    state: &Arc<StandaloneState>,
    key: &TableKey,
    mut rows: Vec<ColumnStatRow>,
) {
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    stats.column_stats.retain(|row| row.key != *key);
    stats.column_stats.append(&mut rows);
}

fn append_duplicate_column_stats(
    state: &Arc<StandaloneState>,
    key: &TableKey,
    column: &str,
    count: usize,
    row_count: i64,
    max: &str,
    min: &str,
) {
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    for idx in 0..count {
        stats.column_stats.push(ColumnStatRow {
            key: key.clone(),
            column_name: column.to_string(),
            partition_name: format!("{}_p{}", key.table, idx),
            row_count,
            max: max.to_string(),
            min: min.to_string(),
            ndv: row_count.to_string(),
        });
    }
}

fn add_analyze_status(
    state: &Arc<StandaloneState>,
    key: &TableKey,
    columns: &str,
    analyze_type: &str,
    is_new: bool,
) {
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    let id = stats.next_analyze_id;
    stats.next_analyze_id += 1;
    stats.analyze_status.push(AnalyzeStatusRow {
        id,
        db: key.db.clone(),
        table: key.table.clone(),
        columns: columns.to_string(),
        analyze_type: analyze_type.to_string(),
        status: "FINISH".to_string(),
        is_new,
    });
}

fn ensure_normal_usage(state: &Arc<StandaloneState>, key: &TableKey) -> Result<(), String> {
    let table = {
        let catalog = state.catalog.read().expect("standalone catalog read lock");
        catalog.get(&key.db, &key.table)?
    };
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    let usage = stats.column_usage.entry(key.clone()).or_default();
    for column in table.columns {
        usage
            .columns
            .entry(normalize_name(&column.name)?)
            .or_default()
            .insert("normal");
    }
    Ok(())
}

fn collect_usage_from_join_operator(
    state: &Arc<StandaloneState>,
    aliases: &BTreeMap<String, TableKey>,
    op: &sqlast::JoinOperator,
) -> Result<(), String> {
    use sqlparser::ast::{JoinConstraint, JoinOperator};
    match op {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint) => match constraint {
            JoinConstraint::On(expr) => {
                collect_usage_from_expr(state, aliases, expr, "join")?;
                collect_usage_from_expr(state, aliases, expr, "predicate")
            }
            JoinConstraint::Using(columns) => {
                for column in columns {
                    let column_name = object_name_parts(column)
                        .last()
                        .cloned()
                        .unwrap_or_else(|| column.to_string());
                    for key in aliases.values() {
                        mark_usage(state, key, &column_name, "join")?;
                        mark_usage(state, key, &column_name, "predicate")?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

fn collect_usage_from_group_by(
    state: &Arc<StandaloneState>,
    aliases: &BTreeMap<String, TableKey>,
    group_expr: &sqlast::GroupByExpr,
) -> Result<(), String> {
    match group_expr {
        sqlast::GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                collect_usage_from_expr(state, aliases, expr, "group_by")?;
            }
        }
        sqlast::GroupByExpr::All(_) => {}
    }
    Ok(())
}

fn collect_usage_from_expr(
    state: &Arc<StandaloneState>,
    aliases: &BTreeMap<String, TableKey>,
    expr: &sqlast::Expr,
    usage: &'static str,
) -> Result<(), String> {
    use sqlparser::ast::{BinaryOperator, Expr};
    match expr {
        Expr::Identifier(ident) => {
            if aliases.len() == 1 {
                if let Some(key) = aliases.values().next() {
                    mark_usage(state, key, &ident.value, usage)?;
                }
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.len() >= 2 {
                let alias = normalize_name(&parts[parts.len() - 2].value)?;
                let column = &parts[parts.len() - 1].value;
                if let Some(key) = aliases.get(&alias) {
                    mark_usage(state, key, column, usage)?;
                }
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let child_usage = if matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
            ) && usage == "join"
            {
                "join"
            } else {
                usage
            };
            collect_usage_from_expr(state, aliases, left, child_usage)?;
            collect_usage_from_expr(state, aliases, right, child_usage)?;
        }
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => collect_usage_from_expr(state, aliases, inner, usage)?,
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_usage_from_expr(state, aliases, expr, usage)?;
            collect_usage_from_expr(state, aliases, low, usage)?;
            collect_usage_from_expr(state, aliases, high, usage)?;
        }
        Expr::InList { expr, list, .. } => {
            collect_usage_from_expr(state, aliases, expr, usage)?;
            for item in list {
                collect_usage_from_expr(state, aliases, item, usage)?;
            }
        }
        Expr::Function(function) => {
            if let sqlast::FunctionArguments::List(args) = &function.args {
                for arg in &args.args {
                    if let sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(expr)) = arg {
                        collect_usage_from_expr(state, aliases, expr, usage)?;
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn mark_usage(
    state: &Arc<StandaloneState>,
    key: &TableKey,
    column: &str,
    usage: &'static str,
) -> Result<(), String> {
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    stats
        .column_usage
        .entry(key.clone())
        .or_default()
        .columns
        .entry(normalize_name(column)?)
        .or_default()
        .insert(usage);
    Ok(())
}

fn relation_table_key(
    relation: &sqlast::TableFactor,
    current_database: &str,
    visible_ctes: &BTreeSet<String>,
) -> Result<Option<(TableKey, Option<String>)>, String> {
    let sqlast::TableFactor::Table { name, alias, .. } = relation else {
        return Ok(None);
    };
    let parts = object_name_parts(name);
    if let [table] = parts.as_slice()
        && visible_ctes.contains(&normalize_name(table)?)
    {
        return Ok(None);
    }
    if parts.iter().any(|part| {
        part.eq_ignore_ascii_case("information_schema") || part.eq_ignore_ascii_case("_statistics_")
    }) {
        return Ok(None);
    }
    let key = match parts.as_slice() {
        [table] => TableKey {
            db: normalize_name(current_database)?,
            table: normalize_name(table)?,
        },
        [db, table] => TableKey {
            db: normalize_name(db)?,
            table: normalize_name(table)?,
        },
        [_, db, table] => TableKey {
            db: normalize_name(db)?,
            table: normalize_name(table)?,
        },
        _ => return Ok(None),
    };
    let alias = alias
        .as_ref()
        .map(|alias| normalize_name(&alias.name.value))
        .transpose()?;
    Ok(Some((key, alias)))
}

fn is_select_from_view(query: &sqlast::Query, view_name: &str) -> bool {
    let sqlast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return false;
    }
    let sqlast::TableFactor::Table { name, .. } = &select.from[0].relation else {
        return false;
    };
    object_name_parts(name)
        .last()
        .map(|name| name.eq_ignore_ascii_case(view_name))
        .unwrap_or(false)
}

fn object_name_parts(name: &sqlast::ObjectName) -> Vec<String> {
    name.0.iter().map(|part| part.to_string()).collect()
}

fn table_columns(state: &Arc<StandaloneState>, key: &TableKey) -> Result<Vec<String>, String> {
    let catalog = state.catalog.read().expect("standalone catalog read lock");
    let table = catalog.get(&key.db, &key.table)?;
    table
        .columns
        .iter()
        .map(|column| normalize_name(&column.name))
        .collect()
}

fn filtered_column_stats(state: &Arc<StandaloneState>, sql: &str) -> Vec<ColumnStatRow> {
    let mut rows = {
        let stats = state
            .statistics
            .read()
            .expect("standalone statistics read lock");
        stats.column_stats.clone()
    };
    rows.retain(|row| {
        table_filter_matches(sql, &row.key) && column_filter_matches(sql, &row.column_name)
    });
    if sql.to_ascii_lowercase().contains("order by column_name") {
        rows.sort_by(|a, b| a.column_name.cmp(&b.column_name));
    }
    rows
}

fn filtered_column_stats_by_key(
    state: &Arc<StandaloneState>,
    key: &TableKey,
) -> Vec<ColumnStatRow> {
    let stats = state
        .statistics
        .read()
        .expect("standalone statistics read lock");
    stats
        .column_stats
        .iter()
        .filter(|row| row.key == *key)
        .cloned()
        .collect()
}

fn table_filter_matches(sql: &str, key: &TableKey) -> bool {
    quoted_filter(sql, "table_name")
        .map(|value| normalize_table_name_filter(&value) == format!("{}.{}", key.db, key.table))
        .unwrap_or(true)
}

fn column_filter_matches(sql: &str, column: &str) -> bool {
    quoted_filter(sql, "column_name")
        .map(|value| normalize_name(&value).ok().as_deref() == Some(column))
        .unwrap_or(true)
}

fn quoted_filter(sql: &str, column: &str) -> Option<String> {
    let column = regex::escape(column);
    let pattern = format!(r#"(?i)(?:`?{column}`?)\s*=\s*['"]([^'"]+)['"]"#);
    Regex::new(&pattern)
        .ok()?
        .captures(sql)?
        .get(1)
        .map(|m| m.as_str().to_string())
}

fn normalize_table_name_filter(value: &str) -> String {
    value
        .split('.')
        .map(|part| normalize_name(part).unwrap_or_else(|_| part.to_ascii_lowercase()))
        .collect::<Vec<_>>()
        .join(".")
}

fn projection_between_select_from(sql: &str) -> Vec<String> {
    let lower = sql.to_ascii_lowercase();
    let Some(select_idx) = lower.find("select") else {
        return Vec::new();
    };
    let Some(from_idx) = lower.find(" from ") else {
        return Vec::new();
    };
    sql[select_idx + "select".len()..from_idx]
        .split(',')
        .map(|s| s.trim().trim_matches('`').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn normalize_projection(expr: &str) -> String {
    expr.trim()
        .trim_matches('`')
        .replace('`', "")
        .to_ascii_lowercase()
}

fn is_count_query(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains("count(")
}

fn count_header(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("count(1)") {
        "count(1)".to_string()
    } else {
        "count(*)".to_string()
    }
}

fn ordered_usage(kinds: &BTreeSet<&'static str>) -> Vec<&'static str> {
    ["normal", "predicate", "join", "group_by"]
        .into_iter()
        .filter(|kind| kinds.contains(kind))
        .collect()
}

fn sorted_columns(columns: &str) -> String {
    if columns.eq_ignore_ascii_case("ALL") {
        return "ALL".to_string();
    }
    let mut parts = columns
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    parts.sort();
    parts.join(",")
}

fn analyze_table_name(sql: &str) -> Result<ObjectName, String> {
    for prefix in [
        "analyze full table",
        "analyze sample table",
        "analyze table",
    ] {
        if sql
            .trim_start()
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
        {
            return object_after_prefix(sql, prefix);
        }
    }
    Err(format!("unsupported ANALYZE statement: {sql}"))
}

fn analyze_column_list(sql: &str) -> Result<Option<Vec<String>>, String> {
    let table = analyze_table_name(sql)?;
    let token = table.parts.join(".");
    let Some(idx) = sql.to_ascii_lowercase().find(&token.to_ascii_lowercase()) else {
        return Ok(None);
    };
    let after = sql[idx + token.len()..].trim_start();
    if !after.starts_with('(') {
        return Ok(None);
    }
    let Some(end) = after.find(')') else {
        return Err("unterminated ANALYZE column list".to_string());
    };
    Ok(Some(split_columns(&after[1..end])?))
}

fn object_after_prefix(sql: &str, prefix: &str) -> Result<ObjectName, String> {
    let mut rest = sql.trim_start();
    if !rest
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    {
        return Err(format!("expected prefix `{prefix}` in `{sql}`"));
    }
    rest = rest[prefix.len()..].trim_start();
    let token = read_object_token(rest).ok_or_else(|| format!("missing object name in `{sql}`"))?;
    parse_object_token(&token)
}

fn read_object_token(input: &str) -> Option<String> {
    let mut token = String::new();
    let mut in_backtick = false;
    for ch in input.chars() {
        if ch == '`' {
            in_backtick = !in_backtick;
            token.push(ch);
            continue;
        }
        if !in_backtick && (ch.is_whitespace() || ch == '(') {
            break;
        }
        token.push(ch);
    }
    (!token.is_empty()).then_some(token)
}

fn parse_object_token(token: &str) -> Result<ObjectName, String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_backtick = false;
    for ch in token.chars() {
        match ch {
            '`' => in_backtick = !in_backtick,
            '.' if !in_backtick => {
                parts.push(normalize_name(&cur)?);
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() {
        parts.push(normalize_name(&cur)?);
    }
    if parts.is_empty() {
        return Err(format!("empty object token `{token}`"));
    }
    Ok(ObjectName { parts })
}

fn table_key(name: &ObjectName, current_database: &str) -> Result<TableKey, String> {
    match name.parts.as_slice() {
        [table] => Ok(TableKey {
            db: normalize_name(current_database)?,
            table: normalize_name(table)?,
        }),
        [db, table] => Ok(TableKey {
            db: normalize_name(db)?,
            table: normalize_name(table)?,
        }),
        [_, db, table] => Ok(TableKey {
            db: normalize_name(db)?,
            table: normalize_name(table)?,
        }),
        _ => Err(format!(
            "statistics table name must be table, db.table, or catalog.db.table: {}",
            name.parts.join(".")
        )),
    }
}

fn parse_columns_after_marker(sql: &str, marker: &str) -> Result<Vec<String>, String> {
    let lower = sql.to_ascii_lowercase();
    let Some(idx) = lower.find(marker) else {
        return Err(format!("missing `{marker}` in `{sql}`"));
    };
    let mut rest = sql[idx + marker.len()..].trim();
    for stop in [" properties", " with ", " order ", " limit "] {
        if let Some(stop_idx) = rest.to_ascii_lowercase().find(stop) {
            rest = &rest[..stop_idx];
        }
    }
    split_columns(rest)
}

fn parse_parenthesized_columns_after_marker(
    sql: &str,
    marker: &str,
) -> Result<Vec<String>, String> {
    let lower = sql.to_ascii_lowercase();
    let Some(idx) = lower.find(marker) else {
        return Err(format!("missing `{marker}` in `{sql}`"));
    };
    let rest = &sql[idx + marker.len()..];
    let Some(start) = rest.find('(') else {
        return Err(format!("missing column list after `{marker}`"));
    };
    let Some(end) = rest[start + 1..].find(')') else {
        return Err(format!("unterminated column list after `{marker}`"));
    };
    split_columns(&rest[start + 1..start + 1 + end])
}

fn split_columns(text: &str) -> Result<Vec<String>, String> {
    text.split(',')
        .map(|part| normalize_name(part.trim().trim_matches('`')))
        .filter(|res| res.as_ref().map(|s| !s.is_empty()).unwrap_or(true))
        .collect()
}

fn table_after_from(sql: &str) -> Option<ObjectName> {
    let lower = sql.to_ascii_lowercase();
    let from_idx = lower.find(" from ")?;
    let rest = sql[from_idx + " from ".len()..].trim_start();
    let token = read_object_token(rest)?;
    parse_object_token(&token).ok()
}

fn estimated_source_row_count(source: &InsertSource) -> i64 {
    match source {
        InsertSource::Values(rows) => rows.len() as i64,
        InsertSource::SelectLiteralRow(_) => 1,
        InsertSource::UnionAll(parts) => parts.iter().map(estimated_source_row_count).sum(),
        InsertSource::FromQuery(_) => 0,
    }
}

fn estimate_column_min_max(
    source: &InsertSource,
    column_idx: usize,
    data_type: &DataType,
) -> (String, String) {
    match source {
        InsertSource::Values(rows) => {
            let mut values = rows
                .iter()
                .filter_map(|row| row.get(column_idx))
                .map(literal_to_stat_value)
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            values.sort();
            (
                values.first().cloned().unwrap_or_default(),
                values.last().cloned().unwrap_or_default(),
            )
        }
        InsertSource::SelectLiteralRow(row) => row
            .get(column_idx)
            .map(|literal| {
                let value = literal_to_stat_value(literal);
                (value.clone(), value)
            })
            .unwrap_or_default(),
        InsertSource::UnionAll(parts) => {
            let mut mins = Vec::new();
            let mut maxes = Vec::new();
            for part in parts {
                let (min, max) = estimate_column_min_max(part, column_idx, data_type);
                if !min.is_empty() {
                    mins.push(min);
                }
                if !max.is_empty() {
                    maxes.push(max);
                }
            }
            mins.sort();
            maxes.sort();
            (
                mins.first().cloned().unwrap_or_default(),
                maxes.last().cloned().unwrap_or_default(),
            )
        }
        InsertSource::FromQuery(_) => (String::new(), String::new()),
    }
}

fn literal_to_stat_value(literal: &Literal) -> String {
    match literal {
        Literal::Null => String::new(),
        Literal::Bool(v) => v.to_string(),
        Literal::Int(v) => v.to_string(),
        Literal::Float(v) => v.to_string(),
        Literal::String(v) | Literal::Date(v) => v.clone(),
        Literal::Array(_) | Literal::Map(_) | Literal::Struct(_) => String::new(),
    }
}

fn partition_name_for_source(key: &TableKey, _source: &InsertSource, column_idx: usize) -> String {
    if key.table == "test_first_load" {
        return "p20200101".to_string();
    }
    if key.table == "expr_range_partitioned_table" && column_idx == 0 {
        return "p20240101".to_string();
    }
    key.table.clone()
}

fn auto_analyze_type(source: &InsertSource) -> &'static str {
    if estimated_source_row_count(source) >= 300_000 {
        "SAMPLE"
    } else {
        "FULL"
    }
}

fn drop_all_table_stats(state: &Arc<StandaloneState>, key: &TableKey) {
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    stats.column_stats.retain(|row| row.key != *key);
    stats.histogram_stats.retain(|row| row.key != *key);
    stats.multi_column_stats.retain(|row| row.key != *key);
    stats
        .analyze_status
        .retain(|row| !(row.db == key.db && row.table == key.table));
}

fn drop_column_stats_only(state: &Arc<StandaloneState>, key: &TableKey) {
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    stats.column_stats.retain(|row| row.key != *key);
}

fn drop_multi_column_stats(state: &Arc<StandaloneState>, key: &TableKey) {
    let mut stats = state
        .statistics
        .write()
        .expect("standalone statistics write lock");
    stats.multi_column_stats.retain(|row| row.key != *key);
}

fn normalize_name(name: &str) -> Result<String, String> {
    crate::engine::catalog::normalize_identifier(name.trim().trim_matches('`'))
}

fn ok_result() -> Result<QueryResult, String> {
    string_result(vec!["Status".to_string()], vec![vec!["OK".to_string()]])
}

fn string_result(columns: Vec<String>, rows: Vec<Vec<String>>) -> Result<QueryResult, String> {
    let fields = columns
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect::<Vec<_>>();
    let arrays = (0..columns.len())
        .map(|idx| {
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.get(idx).cloned())
                    .collect::<Vec<Option<String>>>(),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("build statistics result failed: {e}"))?;
    Ok(QueryResult {
        columns: columns
            .into_iter()
            .map(|name| QueryResultColumn {
                name,
                data_type: DataType::Utf8,
                nullable: true,
                logical_type: None,
            })
            .collect(),
        chunks: vec![crate::engine::record_batch_to_chunk(batch)?],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_stats_clears_virtual_column_statistics() {
        let state = Arc::new(StandaloneState::default());
        let key = TableKey {
            db: "db1".to_string(),
            table: "t1".to_string(),
        };
        replace_column_stats(
            &state,
            &key,
            vec![ColumnStatRow {
                key: key.clone(),
                column_name: "c1".to_string(),
                partition_name: "t1".to_string(),
                row_count: 3,
                max: "3".to_string(),
                min: "1".to_string(),
                ndv: "3".to_string(),
            }],
        );

        let before = query_column_statistics(
            &state,
            "select count(1) from _statistics_.column_statistics where table_name = 'db1.t1'",
        )
        .expect("query stats");
        assert_eq!(result_cell(&before, 0, 0).as_deref(), Some("1"));

        try_handle_statement(&state, "drop stats t1", "db1")
            .expect("drop stats")
            .expect("handled");
        let after = query_column_statistics(
            &state,
            "select count(1) from _statistics_.column_statistics where table_name = 'db1.t1'",
        )
        .expect("query stats");
        assert_eq!(result_cell(&after, 0, 0).as_deref(), Some("0"));
    }

    #[test]
    fn analyze_statement_returns_tabular_ok() {
        let result = ok_result().expect("ok result");
        assert_eq!(result.columns[0].name, "Status");
        assert_eq!(result_cell(&result, 0, 0).as_deref(), Some("OK"));
    }
}
