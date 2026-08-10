use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks::exec::chunk::{Chunk, ChunkSchema};
use novarocks::runtime::query_result::{QueryResult, QueryResultColumn};
use novarocks_types::SlotId;
use sqlparser::ast as sqlast;

use super::FrontendStatisticsService;
use super::model::{AnalyzeStatusRow, ColumnStatRow, TableKey};

pub(super) fn try_query(
    service: &FrontendStatisticsService,
    sql: &str,
    query: &sqlast::Query,
    current_database: &str,
) -> Result<Option<QueryResult>, String> {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("_statistics_.column_statistics") {
        return query_column_statistics(service, sql).map(Some);
    }
    if lower.contains("_statistics_.histogram_statistics") {
        return query_histogram_statistics(service, sql).map(Some);
    }
    if lower.contains("_statistics_.multi_column_statistics") {
        return query_multi_column_statistics(service, sql).map(Some);
    }
    if lower.contains("information_schema.column_stats_usage") {
        return query_column_stats_usage(service, sql).map(Some);
    }
    if lower.contains("information_schema.analyze_status") {
        return query_analyze_status(service, sql).map(Some);
    }
    if is_select_from_view(query, "statistic_verify") {
        return query_statistic_verify_view(service, current_database).map(Some);
    }
    if is_select_from_view(query, "analyze_status_verify") {
        return query_analyze_status_verify_view(service, current_database).map(Some);
    }
    if is_select_from_view(query, "last_analyze_id_view") {
        return query_last_analyze_id_view(service, current_database).map(Some);
    }
    Ok(None)
}

fn query_column_statistics(
    service: &FrontendStatisticsService,
    sql: &str,
) -> Result<QueryResult, String> {
    let rows = filtered_column_stats(service, sql);
    if is_count_query(sql) {
        return string_result(vec![count_header(sql)], vec![vec![rows.len().to_string()]]);
    }
    let columns = projection_between_select_from(sql);
    let output = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| match normalize_projection(column).as_str() {
                    "table_name" => format!("{}.{}", row.key.db, row.key.table),
                    "column_name" => row.column_name.clone(),
                    "partition_name" => row.partition_name.clone(),
                    "row_count" => row.row_count.to_string(),
                    "max" => row.max.clone(),
                    "min" => row.min.clone(),
                    "ndv" | "hll_cardinality(ndv)" => row.ndv.clone(),
                    _ => String::new(),
                })
                .collect()
        })
        .collect();
    string_result(columns, output)
}

fn query_histogram_statistics(
    service: &FrontendStatisticsService,
    sql: &str,
) -> Result<QueryResult, String> {
    let mut rows = {
        let state = service.state.read().expect("frontend statistics read lock");
        state.histogram_stats.clone()
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
                .map(|column| match normalize_projection(column).as_str() {
                    "table_name" => format!("{}.{}", row.key.db, row.key.table),
                    "column_name" => row.column_name.clone(),
                    "buckets" => row.buckets.clone(),
                    "mcv" => row.mcv.clone(),
                    _ => String::new(),
                })
                .collect()
        })
        .collect();
    string_result(columns, output)
}

fn query_multi_column_statistics(
    service: &FrontendStatisticsService,
    sql: &str,
) -> Result<QueryResult, String> {
    let rows = {
        let state = service.state.read().expect("frontend statistics read lock");
        state
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
                .map(|column| match normalize_projection(column).as_str() {
                    "table_name" => format!("{}.{}", row.key.db, row.key.table),
                    "column_names" => row.column_names.clone(),
                    _ => String::new(),
                })
                .collect()
        })
        .collect();
    string_result(columns, output)
}

fn query_column_stats_usage(
    service: &FrontendStatisticsService,
    sql: &str,
) -> Result<QueryResult, String> {
    let db_filter = quoted_filter(sql, "table_database");
    let table_filter = quoted_filter(sql, "table_name");
    let mut rows = Vec::new();
    {
        let state = service.state.read().expect("frontend statistics read lock");
        for (key, usage) in &state.column_usage {
            if matches_normalized_filter(db_filter.as_deref(), &key.db)
                && matches_normalized_filter(table_filter.as_deref(), &key.table)
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

fn query_analyze_status(
    service: &FrontendStatisticsService,
    sql: &str,
) -> Result<QueryResult, String> {
    let db_filter = quoted_filter(sql, "database").or_else(|| quoted_filter(sql, "`database`"));
    let table_filter = quoted_filter(sql, "table").or_else(|| quoted_filter(sql, "`table`"));
    let status_filter = quoted_filter(sql, "status").or_else(|| quoted_filter(sql, "`status`"));
    let mut rows = {
        let state = service.state.read().expect("frontend statistics read lock");
        state.analyze_status.clone()
    };
    rows.retain(|row| {
        matches_normalized_filter(db_filter.as_deref(), &row.db)
            && matches_normalized_filter(table_filter.as_deref(), &row.table)
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
    analyze_status_result(rows, projection_between_select_from(sql))
}

fn analyze_status_result(
    rows: Vec<AnalyzeStatusRow>,
    columns: Vec<String>,
) -> Result<QueryResult, String> {
    let output = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| match normalize_projection(column).as_str() {
                    "id" => row.id.to_string(),
                    "database" => row.db.clone(),
                    "table" => row.table.clone(),
                    "columns" => row.columns.clone(),
                    "type" => row.analyze_type.clone(),
                    "status" => row.status.clone(),
                    _ => String::new(),
                })
                .collect()
        })
        .collect();
    string_result(columns, output)
}

fn query_statistic_verify_view(
    service: &FrontendStatisticsService,
    current_database: &str,
) -> Result<QueryResult, String> {
    let key = TableKey {
        db: normalize_name(current_database)?,
        table: "test_update_stats".to_string(),
    };
    let mut rows = {
        let state = service.state.read().expect("frontend statistics read lock");
        state
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
    service: &FrontendStatisticsService,
    current_database: &str,
) -> Result<QueryResult, String> {
    let db = normalize_name(current_database)?;
    let latest = {
        let state = service.state.read().expect("frontend statistics read lock");
        state
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
        .collect();
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
    service: &FrontendStatisticsService,
    current_database: &str,
) -> Result<QueryResult, String> {
    let db = normalize_name(current_database)?;
    let id = {
        let state = service.state.read().expect("frontend statistics read lock");
        state
            .analyze_status
            .iter()
            .filter(|row| row.db == db && row.table == "test_update_stats")
            .map(|row| row.id)
            .max()
            .unwrap_or(0)
    };
    string_result(vec!["last_id".to_string()], vec![vec![id.to_string()]])
}

fn filtered_column_stats(service: &FrontendStatisticsService, sql: &str) -> Vec<ColumnStatRow> {
    let mut rows = {
        let state = service.state.read().expect("frontend statistics read lock");
        state.column_stats.clone()
    };
    rows.retain(|row| {
        table_filter_matches(sql, &row.key) && column_filter_matches(sql, &row.column_name)
    });
    rows.sort_by(|a, b| a.column_name.cmp(&b.column_name));
    rows
}

fn table_filter_matches(sql: &str, key: &TableKey) -> bool {
    quoted_filter(sql, "table_name")
        .map(|value| normalize_table_name_filter(&value) == format!("{}.{}", key.db, key.table))
        .unwrap_or(true)
}

fn column_filter_matches(sql: &str, column: &str) -> bool {
    quoted_filter(sql, "column_name")
        .map(|value| value.eq_ignore_ascii_case(column))
        .unwrap_or(true)
}

fn matches_normalized_filter(filter: Option<&str>, actual: &str) -> bool {
    filter
        .map(|value| normalize_name(value).ok().as_deref() == Some(actual))
        .unwrap_or(true)
}

fn quoted_filter(sql: &str, column: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let column = column.to_ascii_lowercase();
    let mut offset = 0;
    while let Some(found) = lower[offset..].find(&column) {
        let start = offset + found + column.len();
        let rest = sql[start..].trim_start();
        let Some(rest) = rest.strip_prefix('=') else {
            offset = start;
            continue;
        };
        let rest = rest.trim_start();
        let quote = rest.chars().next()?;
        if quote != '\'' && quote != '"' {
            offset = start;
            continue;
        }
        let value = &rest[quote.len_utf8()..];
        return value.find(quote).map(|end| value[..end].to_string());
    }
    None
}

fn normalize_table_name_filter(value: &str) -> String {
    value
        .split('.')
        .map(|part| part.trim_matches('`').to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

fn projection_between_select_from(sql: &str) -> Vec<String> {
    let lower = sql.to_ascii_lowercase();
    let start = lower.find("select").map(|index| index + 6).unwrap_or(0);
    let end = lower[start..]
        .find(" from ")
        .map(|index| start + index)
        .unwrap_or(sql.len());
    sql[start..end]
        .split(',')
        .map(|column| column.trim().to_string())
        .collect()
}

fn normalize_projection(expression: &str) -> String {
    expression
        .split_whitespace()
        .next()
        .unwrap_or(expression)
        .trim_matches('`')
        .to_ascii_lowercase()
}

fn is_count_query(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains("count(")
}

fn count_header(sql: &str) -> String {
    projection_between_select_from(sql)
        .into_iter()
        .next()
        .unwrap_or_else(|| "count(*)".to_string())
}

fn ordered_usage(kinds: &BTreeSet<&'static str>) -> Vec<&'static str> {
    ["normal", "join", "predicate", "group_by"]
        .into_iter()
        .filter(|kind| kinds.contains(kind))
        .collect()
}

fn sorted_columns(columns: &str) -> String {
    let mut columns = columns
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>();
    columns.sort_unstable();
    columns.join(",")
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
    name.0
        .last()
        .map(|name| name.to_string().eq_ignore_ascii_case(view_name))
        .unwrap_or(false)
}

pub(super) fn normalize_name(name: &str) -> Result<String, String> {
    novarocks_catalog::identifier::normalize_identifier(name.trim().trim_matches('`'))
}

pub(super) fn ok_result() -> Result<QueryResult, String> {
    string_result(vec!["Status".to_string()], vec![vec!["OK".to_string()]])
}

fn string_result(columns: Vec<String>, rows: Vec<Vec<String>>) -> Result<QueryResult, String> {
    let fields = columns
        .iter()
        .map(|column| Field::new(column, DataType::Utf8, true))
        .collect::<Vec<_>>();
    let arrays = (0..columns.len())
        .map(|column_index| {
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.get(column_index).cloned().unwrap_or_default())
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| format!("statistics result batch failed: {error}"))?;
    let slot_ids = (1..=batch.num_columns())
        .map(|index| {
            u32::try_from(index)
                .map(SlotId::new)
                .map_err(|_| "too many statistics output columns".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slot_ids)?;
    let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema)?;
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
        chunks: vec![chunk],
    })
}
