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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;

use crate::engine::StandaloneState;
use crate::runtime::query_result::QueryResult;
use crate::sql::parser::ast::ObjectName;

pub struct StatisticsRequestContext<'a> {
    pub current_catalog: Option<&'a str>,
    pub current_database: &'a str,
}

#[derive(Debug)]
pub enum StatisticsStatementResult {
    Ok,
    Query(QueryResult),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsOverwriteMode {
    Append,
    FullTable,
    DynamicPartitions,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StatisticsLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Date(String),
    Array(Vec<StatisticsLiteral>),
    Map(Vec<(StatisticsLiteral, StatisticsLiteral)>),
    Struct(Vec<StatisticsLiteral>),
}

#[derive(Clone, Debug)]
pub enum StatisticsInsertSource {
    Values(Vec<Vec<StatisticsLiteral>>),
    SelectLiteralRow(Vec<StatisticsLiteral>),
    UnionAll(Vec<StatisticsInsertSource>),
    FromQuery(Box<sqlparser::ast::Query>),
}

pub struct StatisticsInsertObservation<'a> {
    pub database: &'a str,
    pub table: &'a str,
    pub insert_columns: &'a [String],
    pub source: &'a StatisticsInsertSource,
    pub overwrite_mode: StatisticsOverwriteMode,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogColumnStatistics {
    pub column_name: String,
    pub row_count: i64,
    pub min: String,
    pub max: String,
    pub ndv: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogTableStatistics {
    pub columns: Vec<CatalogColumnStatistics>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableTarget {
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub name_parts: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsColumn {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CollectedColumnStatistics {
    pub column_name: String,
    pub row_count: i64,
    pub min: String,
    pub max: String,
    pub ndv: String,
}

pub trait StatisticsEngine: Send + Sync {
    fn resolve_table_columns(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<Vec<StatisticsColumn>, String>;

    fn resolve_local_table_columns(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<Vec<StatisticsColumn>>, String>;

    fn collect_table_statistics(
        &self,
        target: &StatisticsTableTarget,
        columns: &[String],
    ) -> Result<Vec<CollectedColumnStatistics>, String>;
}

impl StatisticsEngine for Arc<StandaloneState> {
    fn resolve_table_columns(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<Vec<StatisticsColumn>, String> {
        let name = object_name_from_parts(&target.name_parts)?;
        super::query_prep::materialize_external_schema_table_for_statement(
            self,
            target.current_catalog.as_deref(),
            &target.current_database,
            &name,
        )?;
        table_columns_for_materialized_target(self, &target.current_database, &name)
    }

    fn resolve_local_table_columns(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<Vec<StatisticsColumn>>, String> {
        optional_table_columns_from_local_catalog(self, database, table)
    }

    fn collect_table_statistics(
        &self,
        target: &StatisticsTableTarget,
        columns: &[String],
    ) -> Result<Vec<CollectedColumnStatistics>, String> {
        collect_statistics_through_engine(self, target, columns)
    }
}

pub trait StatisticsService: Send + Sync {
    fn try_handle_statement(
        &self,
        engine: &dyn StatisticsEngine,
        sql: &str,
        context: StatisticsRequestContext<'_>,
    ) -> Result<Option<StatisticsStatementResult>, String>;

    fn try_query(
        &self,
        sql: &str,
        query: &sqlparser::ast::Query,
        context: StatisticsRequestContext<'_>,
    ) -> Result<Option<QueryResult>, String>;

    fn observe_query(
        &self,
        query: &sqlparser::ast::Query,
        current_database: &str,
    ) -> Result<(), String>;

    fn observe_insert(
        &self,
        engine: &dyn StatisticsEngine,
        observation: StatisticsInsertObservation<'_>,
    ) -> Result<(), String>;

    fn observe_update(&self, sql: &str, current_database: &str) -> Result<(), String>;
    fn drop_table(&self, database: &str, table: &str);
    fn drop_database(&self, database: &str);

    fn catalog_table_statistics(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<CatalogTableStatistics>, String>;
}

pub struct EmptyStatisticsService;

impl StatisticsService for EmptyStatisticsService {
    fn try_handle_statement(
        &self,
        _engine: &dyn StatisticsEngine,
        sql: &str,
        _context: StatisticsRequestContext<'_>,
    ) -> Result<Option<StatisticsStatementResult>, String> {
        if is_statistics_statement(sql) {
            return Err("statistics service is not injected".to_string());
        }
        Ok(None)
    }

    fn try_query(
        &self,
        sql: &str,
        query: &sqlparser::ast::Query,
        _context: StatisticsRequestContext<'_>,
    ) -> Result<Option<QueryResult>, String> {
        if is_statistics_query(sql, query)? {
            return Err("statistics service is not injected".to_string());
        }
        Ok(None)
    }

    fn observe_query(
        &self,
        _query: &sqlparser::ast::Query,
        _current_database: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    fn observe_insert(
        &self,
        _engine: &dyn StatisticsEngine,
        _observation: StatisticsInsertObservation<'_>,
    ) -> Result<(), String> {
        Ok(())
    }

    fn observe_update(&self, _sql: &str, _current_database: &str) -> Result<(), String> {
        Ok(())
    }

    fn drop_table(&self, _database: &str, _table: &str) {}

    fn drop_database(&self, _database: &str) {}

    fn catalog_table_statistics(
        &self,
        _database: &str,
        _table: &str,
    ) -> Result<Option<CatalogTableStatistics>, String> {
        Ok(None)
    }
}

fn is_statistics_statement(sql: &str) -> bool {
    let lower = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
    lower.starts_with("admin ")
        || (lower.starts_with("alter table ")
            && lower.contains("enable_statistic_collect_on_first_load"))
        || lower.starts_with("drop multiple columns stats ")
        || lower.starts_with("drop stats ")
        || (lower.starts_with("update ") && lower.contains("test_update_stats "))
        || lower.starts_with("analyze ")
}

fn is_statistics_query(sql: &str, query: &sqlparser::ast::Query) -> Result<bool, String> {
    let lower = sql.to_ascii_lowercase();
    Ok(lower.contains("_statistics_.column_statistics")
        || lower.contains("_statistics_.histogram_statistics")
        || lower.contains("_statistics_.multi_column_statistics")
        || lower.contains("information_schema.column_stats_usage")
        || lower.contains("information_schema.analyze_status")
        || is_select_from_view(query, "statistic_verify")
        || is_select_from_view(query, "analyze_status_verify")
        || is_select_from_view(query, "last_analyze_id_view"))
}

fn is_select_from_view(query: &sqlparser::ast::Query, view_name: &str) -> bool {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.iter().any(|from| {
        let sqlparser::ast::TableFactor::Table { name, .. } = &from.relation else {
            return false;
        };
        name.0
            .last()
            .map(|part| part.to_string().eq_ignore_ascii_case(view_name))
            .unwrap_or(false)
    })
}

fn object_name_from_parts(parts: &[String]) -> Result<ObjectName, String> {
    if !(1..=3).contains(&parts.len()) {
        return Err(format!(
            "statistics table name must be table, db.table, or catalog.db.table: {}",
            parts.join(".")
        ));
    }
    let parts = parts
        .iter()
        .map(|part| normalize_name(part))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ObjectName { parts })
}

fn table_columns_from_local_catalog(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
) -> Result<Vec<StatisticsColumn>, String> {
    let catalog = state
        .catalog_service
        .local()
        .read()
        .expect("standalone catalog read lock");
    let table = catalog.get(database, table)?;
    Ok(table
        .columns
        .iter()
        .map(|column| StatisticsColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
        })
        .collect())
}

fn table_columns_for_materialized_target(
    state: &Arc<StandaloneState>,
    current_database: &str,
    name: &ObjectName,
) -> Result<Vec<StatisticsColumn>, String> {
    let (database, table) = resolve_database_and_table(name, current_database)?;
    table_columns_from_local_catalog(state, &database, &table)
}

fn optional_table_columns_from_local_catalog(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
) -> Result<Option<Vec<StatisticsColumn>>, String> {
    normalize_name(database)?;
    normalize_name(table)?;
    match table_columns_from_local_catalog(state, database, table) {
        Ok(columns) => Ok(Some(columns)),
        Err(error)
            if error.starts_with("unknown database:") || error.starts_with("unknown table:") =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn collect_statistics_through_engine(
    state: &Arc<StandaloneState>,
    target: &StatisticsTableTarget,
    columns: &[String],
) -> Result<Vec<CollectedColumnStatistics>, String> {
    let name = object_name_from_parts(&target.name_parts)?;
    let (database, table) = resolve_database_and_table(&name, &target.current_database)?;
    let ndv_by_name = crate::connector::iceberg::analyze::analyze_iceberg_puffin_stats(
        state,
        target.current_catalog.as_deref(),
        &target.current_database,
        &name,
        columns,
    )?;
    let mut output = Vec::with_capacity(columns.len());
    for column in columns {
        let sql = format!(
            "select count(*) as row_count, min(`{}`) as min_value, max(`{}`) as max_value from `{}`.`{}`",
            column.replace('`', "``"),
            column.replace('`', "``"),
            database.replace('`', "``"),
            table.replace('`', "``")
        );
        let query = crate::sql::parser::parse_normalized_sql_raw(&sql)
            .map_err(|error| format!("statistics aggregate parse failed: {error}"))?;
        let sqlparser::ast::Statement::Query(query) = query else {
            return Err("statistics aggregate did not parse as query".to_string());
        };
        let result = crate::engine::execute_query_with_catalog_service(
            state,
            target.current_catalog.as_deref(),
            &database,
            &query,
            None,
        )?;
        let row_count = result_cell(&result, 0, 0)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        output.push(CollectedColumnStatistics {
            column_name: normalize_name(column)?,
            row_count,
            min: result_cell(&result, 1, 0).unwrap_or_default(),
            max: result_cell(&result, 2, 0).unwrap_or_default(),
            ndv: ndv_by_name
                .get(&column.to_lowercase())
                .map(|value| (value.round() as i64).to_string())
                .unwrap_or_else(|| row_count.to_string()),
        });
    }
    Ok(output)
}

fn resolve_database_and_table(
    name: &ObjectName,
    current_database: &str,
) -> Result<(String, String), String> {
    match name.parts.as_slice() {
        [table] => Ok((normalize_name(current_database)?, normalize_name(table)?)),
        [database, table] => Ok((normalize_name(database)?, normalize_name(table)?)),
        [_, database, table] => Ok((normalize_name(database)?, normalize_name(table)?)),
        _ => Err(format!(
            "statistics table name must be table, db.table, or catalog.db.table: {}",
            name.parts.join(".")
        )),
    }
}

fn normalize_name(name: &str) -> Result<String, String> {
    novarocks_catalog::identifier::normalize_identifier(name.trim().trim_matches('`'))
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
            if let Some(array) = array.as_any().downcast_ref::<$ty>() {
                return Ok(Some(array.value(row).to_string()));
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
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(array.value(row).to_string()));
    }
    if let Some(array) = array
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        return Ok(Some(array.value(row).to_string()));
    }
    if let Some(array) = array.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        return Ok(Some(array.value(row).to_string()));
    }
    if let Some(array) = array.as_any().downcast_ref::<arrow::array::Date32Array>() {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let date = epoch + chrono::Duration::days(i64::from(array.value(row)));
        return Ok(Some(date.format("%Y-%m-%d").to_string()));
    }
    if let Some(array) = array
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
    {
        let micros = array.value(row);
        let seconds = micros.div_euclid(1_000_000);
        let subsecond_micros = micros.rem_euclid(1_000_000) as u32;
        let datetime =
            chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, subsecond_micros * 1000)
                .ok_or_else(|| format!("invalid timestamp micros: {micros}"))?
                .naive_utc();
        return Ok(Some(datetime.format("%Y-%m-%d %H:%M:%S").to_string()));
    }
    Ok(Some(format!("{array:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct FakeStatisticsEngine;

    impl StatisticsEngine for FakeStatisticsEngine {
        fn resolve_table_columns(
            &self,
            _target: &StatisticsTableTarget,
        ) -> Result<Vec<StatisticsColumn>, String> {
            Ok(Vec::new())
        }

        fn resolve_local_table_columns(
            &self,
            _database: &str,
            _table: &str,
        ) -> Result<Option<Vec<StatisticsColumn>>, String> {
            Ok(None)
        }

        fn collect_table_statistics(
            &self,
            _target: &StatisticsTableTarget,
            _columns: &[String],
        ) -> Result<Vec<CollectedColumnStatistics>, String> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn statistics_ports_are_object_safe() {
        fn accept_service(_: Arc<dyn StatisticsService>) {}
        fn accept_engine(_: &dyn StatisticsEngine) {}

        accept_service(Arc::new(EmptyStatisticsService));
        let engine = FakeStatisticsEngine;
        accept_engine(&engine);
    }

    #[test]
    fn statistics_engine_resolves_qualified_table_columns_outside_current_database() {
        use novarocks_catalog::schema::ColumnDef;

        let state = Arc::new(StandaloneState::default());
        {
            let mut catalog = state
                .catalog_service
                .local()
                .write()
                .expect("standalone catalog write lock");
            catalog
                .create_database("other_db")
                .expect("create database");
            catalog
                .register(
                    "other_db",
                    crate::sql::planner::table::TableDef {
                        name: "t".to_string(),
                        columns: vec![ColumnDef {
                            name: "k".to_string(),
                            data_type: DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        }],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: crate::sql::planner::table::ScanSource::StarRocks {
                            db_id: 1,
                            table_id: 2,
                        },
                    },
                )
                .expect("register table");
        }

        for name_parts in [
            vec!["other_db".to_string(), "t".to_string()],
            vec![
                "default_catalog".to_string(),
                "other_db".to_string(),
                "t".to_string(),
            ],
        ] {
            let name = object_name_from_parts(&name_parts).expect("qualified object name");
            let columns = table_columns_for_materialized_target(&state, "current_db", &name)
                .expect("resolve qualified table");
            assert_eq!(columns[0].name, "k");
        }
    }

    #[test]
    fn empty_service_rejects_analyze_but_keeps_observation_noop() {
        let service = EmptyStatisticsService;
        let engine = FakeStatisticsEngine;
        let context = StatisticsRequestContext {
            current_catalog: None,
            current_database: "db1",
        };
        let error = service
            .try_handle_statement(&engine, "ANALYZE TABLE t1", context)
            .expect_err("statistics statement must fail without injection");
        assert_eq!(error, "statistics service is not injected");

        service
            .observe_update("UPDATE t1 SET k = 1", "db1")
            .expect("ordinary observation is a no-op");
        assert!(
            service
                .catalog_table_statistics("db1", "t1")
                .expect("catalog statistics")
                .is_none()
        );
    }

    #[test]
    fn empty_service_leaves_non_statistics_explain_costs_unhandled() {
        let service = EmptyStatisticsService;
        let engine = FakeStatisticsEngine;
        let context = StatisticsRequestContext {
            current_catalog: None,
            current_database: "db1",
        };

        assert!(matches!(
            service.try_handle_statement(&engine, "EXPLAIN COSTS SELECT 3", context),
            Ok(None)
        ));
    }
}
