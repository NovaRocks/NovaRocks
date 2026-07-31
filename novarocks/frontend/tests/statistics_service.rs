use std::sync::Mutex;

use arrow::array::StringArray;
use arrow::datatypes::DataType;
use novarocks::engine::statistics::{
    CollectedColumnStatistics, StatisticsColumn, StatisticsEngine, StatisticsInsertObservation,
    StatisticsInsertSource, StatisticsLiteral, StatisticsOverwriteMode, StatisticsRequestContext,
    StatisticsService, StatisticsStatementResult, StatisticsTableTarget,
};
use novarocks::runtime::query_result::QueryResult;
use novarocks_frontend::FrontendStatisticsService;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

struct FakeStatisticsEngine {
    local_columns: Vec<StatisticsColumn>,
    collect_result: Mutex<Result<Vec<CollectedColumnStatistics>, String>>,
    collect_requests: Mutex<Vec<(String, Vec<String>)>>,
}

impl FakeStatisticsEngine {
    fn with_local_columns(columns: Vec<StatisticsColumn>) -> Self {
        Self {
            local_columns: columns,
            collect_result: Mutex::new(Ok(Vec::new())),
            collect_requests: Mutex::new(Vec::new()),
        }
    }

    fn with_collect_result(rows: Vec<CollectedColumnStatistics>) -> Self {
        Self {
            local_columns: rows
                .iter()
                .map(|row| StatisticsColumn {
                    name: row.column_name.clone(),
                    data_type: DataType::Int64,
                })
                .collect(),
            collect_result: Mutex::new(Ok(rows)),
            collect_requests: Mutex::new(Vec::new()),
        }
    }

    fn failing_collect(message: &str) -> Self {
        Self {
            local_columns: vec![StatisticsColumn {
                name: "k".to_string(),
                data_type: DataType::Int64,
            }],
            collect_result: Mutex::new(Err(message.to_string())),
            collect_requests: Mutex::new(Vec::new()),
        }
    }

    fn collect_requests(&self) -> Vec<(String, Vec<String>)> {
        self.collect_requests
            .lock()
            .expect("collect requests")
            .clone()
    }
}

impl StatisticsEngine for FakeStatisticsEngine {
    fn resolve_table_columns(
        &self,
        _target: &StatisticsTableTarget,
    ) -> Result<Vec<StatisticsColumn>, String> {
        Ok(self.local_columns.clone())
    }

    fn resolve_local_table_columns(
        &self,
        _database: &str,
        _table: &str,
    ) -> Result<Option<Vec<StatisticsColumn>>, String> {
        Ok(Some(self.local_columns.clone()))
    }

    fn collect_table_statistics(
        &self,
        target: &StatisticsTableTarget,
        columns: &[String],
    ) -> Result<Vec<CollectedColumnStatistics>, String> {
        self.collect_requests
            .lock()
            .expect("collect requests")
            .push((target.name_parts.join("."), columns.to_vec()));
        self.collect_result.lock().expect("collect result").clone()
    }
}

fn parse_query(sql: &str) -> sqlparser::ast::Query {
    let mut statements = Parser::parse_sql(&GenericDialect, sql).expect("parse test query");
    assert_eq!(statements.len(), 1);
    match statements.remove(0) {
        sqlparser::ast::Statement::Query(query) => *query,
        statement => panic!("expected query, got {statement:?}"),
    }
}

fn string_rows(result: &QueryResult) -> Vec<Vec<String>> {
    result
        .chunks
        .iter()
        .flat_map(|chunk| {
            (0..chunk.batch.num_rows()).map(|row| {
                chunk
                    .batch
                    .columns()
                    .iter()
                    .map(|column| {
                        column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("statistics compatibility result is Utf8")
                            .value(row)
                            .to_string()
                    })
                    .collect()
            })
        })
        .collect()
}

#[test]
fn query_observation_records_join_predicate_and_group_usage() {
    let service = FrontendStatisticsService::new();
    let query = parse_query("SELECT a.k FROM a JOIN b ON a.k = b.k WHERE a.v > 7 GROUP BY a.k");
    service.observe_query(&query, "db1").unwrap();

    let sql = "SELECT table_name, column_name, usage \
               FROM information_schema.column_stats_usage \
               WHERE table_database = 'db1' ORDER BY column_name";
    let result = service
        .try_query(
            sql,
            &parse_query(sql),
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap()
        .expect("statistics query");
    assert_eq!(
        string_rows(&result),
        vec![
            vec![
                "a".to_string(),
                "k".to_string(),
                "join,group_by".to_string()
            ],
            vec!["b".to_string(), "k".to_string(), "join".to_string()],
            vec!["a".to_string(), "v".to_string(), "predicate".to_string()],
        ],
    );
}

#[test]
fn full_overwrite_drops_old_column_stats_before_observe() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine::with_local_columns(vec![StatisticsColumn {
        name: "k".to_string(),
        data_type: DataType::Int64,
    }]);
    let first = StatisticsInsertSource::Values(vec![vec![StatisticsLiteral::Int(1)]]);
    service
        .observe_insert(
            &engine,
            StatisticsInsertObservation {
                database: "db1",
                table: "t1",
                insert_columns: &[],
                source: &first,
                overwrite_mode: StatisticsOverwriteMode::Append,
            },
        )
        .unwrap();
    let replacement = StatisticsInsertSource::Values(vec![vec![StatisticsLiteral::Int(99)]]);
    service
        .observe_insert(
            &engine,
            StatisticsInsertObservation {
                database: "db1",
                table: "t1",
                insert_columns: &[],
                source: &replacement,
                overwrite_mode: StatisticsOverwriteMode::FullTable,
            },
        )
        .unwrap();

    assert_eq!(
        service
            .catalog_table_statistics("db1", "t1")
            .unwrap()
            .unwrap()
            .columns[0]
            .min,
        "99"
    );
}

#[test]
fn column_statistics_support_count_projection_filter_and_sort() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine::with_local_columns(vec![
        StatisticsColumn {
            name: "k".to_string(),
            data_type: DataType::Int64,
        },
        StatisticsColumn {
            name: "v".to_string(),
            data_type: DataType::Int64,
        },
    ]);
    let source = StatisticsInsertSource::Values(vec![
        vec![StatisticsLiteral::Int(2), StatisticsLiteral::Int(20)],
        vec![StatisticsLiteral::Int(1), StatisticsLiteral::Int(10)],
    ]);
    service
        .observe_insert(
            &engine,
            StatisticsInsertObservation {
                database: "db1",
                table: "t1",
                insert_columns: &[],
                source: &source,
                overwrite_mode: StatisticsOverwriteMode::Append,
            },
        )
        .unwrap();

    let count_sql =
        "SELECT count(*) FROM _statistics_.column_statistics WHERE table_name = 'db1.t1'";
    let count = service
        .try_query(
            count_sql,
            &parse_query(count_sql),
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(string_rows(&count), vec![vec!["2".to_string()]]);

    let rows_sql = "SELECT column_name, min, max, row_count \
                    FROM _statistics_.column_statistics \
                    WHERE table_name = 'db1.t1' ORDER BY column_name";
    let rows = service
        .try_query(
            rows_sql,
            &parse_query(rows_sql),
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        string_rows(&rows),
        vec![
            vec![
                "k".to_string(),
                "1".to_string(),
                "2".to_string(),
                "2".to_string(),
            ],
            vec![
                "v".to_string(),
                "10".to_string(),
                "20".to_string(),
                "2".to_string(),
            ],
        ]
    );
}

#[test]
fn analyze_collects_requested_columns_and_publishes_status() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine::with_collect_result(vec![CollectedColumnStatistics {
        column_name: "k".to_string(),
        row_count: 3,
        min: "1".to_string(),
        max: "3".to_string(),
        ndv: "3".to_string(),
    }]);
    let result = service
        .try_handle_statement(
            &engine,
            "ANALYZE TABLE db1.t1(k)",
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap()
        .expect("ANALYZE route");
    assert!(matches!(result, StatisticsStatementResult::Query(_)));
    assert_eq!(
        engine.collect_requests(),
        vec![("db1.t1".to_string(), vec!["k".to_string()])]
    );
    assert_eq!(
        service
            .catalog_table_statistics("db1", "t1")
            .unwrap()
            .unwrap()
            .columns[0]
            .ndv,
        "3"
    );
}

#[test]
fn analyze_engine_error_does_not_publish_partial_memory_rows() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine::failing_collect("aggregate scan failed");
    let error = service
        .try_handle_statement(
            &engine,
            "ANALYZE TABLE db1.t1",
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .expect_err("collect failure");
    assert_eq!(error, "aggregate scan failed");
    assert!(
        service
            .catalog_table_statistics("db1", "t1")
            .unwrap()
            .is_none()
    );
}

#[test]
fn normalized_union_values_preserve_numeric_min_max() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine::with_local_columns(vec![StatisticsColumn {
        name: "k".to_string(),
        data_type: DataType::Int64,
    }]);
    let source = StatisticsInsertSource::Values(vec![
        vec![StatisticsLiteral::Int(9)],
        vec![StatisticsLiteral::Int(-3)],
        vec![StatisticsLiteral::Int(4)],
    ]);
    service
        .observe_insert(
            &engine,
            StatisticsInsertObservation {
                database: "db1",
                table: "union_values",
                insert_columns: &[],
                source: &source,
                overwrite_mode: StatisticsOverwriteMode::Append,
            },
        )
        .unwrap();
    let stats = service
        .catalog_table_statistics("db1", "union_values")
        .unwrap()
        .unwrap();
    assert_eq!(stats.columns[0].row_count, 3);
    assert_eq!(stats.columns[0].min, "-3");
    assert_eq!(stats.columns[0].max, "9");
}

#[test]
fn generate_series_source_estimates_row_count_and_projection_min_max() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine::with_local_columns(vec![StatisticsColumn {
        name: "k".to_string(),
        data_type: DataType::Int64,
    }]);
    let source = StatisticsInsertSource::FromQuery(Box::new(parse_query(
        "SELECT x * 2 FROM generate_series(2, 5) AS t(x)",
    )));
    service
        .observe_insert(
            &engine,
            StatisticsInsertObservation {
                database: "db1",
                table: "generated",
                insert_columns: &[],
                source: &source,
                overwrite_mode: StatisticsOverwriteMode::Append,
            },
        )
        .unwrap();

    let stats = service
        .catalog_table_statistics("db1", "generated")
        .unwrap()
        .unwrap();
    assert_eq!(stats.columns[0].row_count, 4);
    assert_eq!(stats.columns[0].min, "4");
    assert_eq!(stats.columns[0].max, "10");
}

#[test]
fn drop_table_and_database_remove_statistics_and_usage() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine::with_local_columns(vec![StatisticsColumn {
        name: "k".to_string(),
        data_type: DataType::Int64,
    }]);
    for (database, table) in [("db1", "t1"), ("db2", "t2")] {
        let source = StatisticsInsertSource::Values(vec![vec![StatisticsLiteral::Int(1)]]);
        service
            .observe_insert(
                &engine,
                StatisticsInsertObservation {
                    database,
                    table,
                    insert_columns: &[],
                    source: &source,
                    overwrite_mode: StatisticsOverwriteMode::Append,
                },
            )
            .unwrap();
        service
            .observe_query(
                &parse_query(&format!("SELECT k FROM {database}.{table} WHERE k > 0")),
                database,
            )
            .unwrap();
    }

    service.drop_table("db1", "t1");
    assert!(
        service
            .catalog_table_statistics("db1", "t1")
            .unwrap()
            .is_none()
    );
    service.drop_database("db2");
    assert!(
        service
            .catalog_table_statistics("db2", "t2")
            .unwrap()
            .is_none()
    );
}

#[test]
fn observe_insert_skips_tables_missing_from_local_engine_catalog() {
    struct MissingTableEngine;
    impl StatisticsEngine for MissingTableEngine {
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

    let service = FrontendStatisticsService::new();
    let source = StatisticsInsertSource::Values(vec![vec![StatisticsLiteral::Int(1)]]);
    service
        .observe_insert(
            &MissingTableEngine,
            StatisticsInsertObservation {
                database: "db1",
                table: "external_only",
                insert_columns: &[],
                source: &source,
                overwrite_mode: StatisticsOverwriteMode::Append,
            },
        )
        .unwrap();
    assert!(
        service
            .catalog_table_statistics("db1", "external_only")
            .unwrap()
            .is_none()
    );
}

#[test]
fn analyze_collects_columns_and_surfaces_collection_errors() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine::with_collect_result(vec![CollectedColumnStatistics {
        column_name: "k".to_string(),
        row_count: 7,
        min: "1".to_string(),
        max: "9".to_string(),
        ndv: "4".to_string(),
    }]);
    let handled = service
        .try_handle_statement(
            &engine,
            "ANALYZE TABLE db1.t1(k)",
            StatisticsRequestContext {
                current_catalog: Some("default_catalog"),
                current_database: "db1",
            },
        )
        .unwrap();
    assert!(handled.is_some());
    assert_eq!(
        engine.collect_requests(),
        vec![("db1.t1".to_string(), vec!["k".to_string()])]
    );
    assert_eq!(
        service
            .catalog_table_statistics("db1", "t1")
            .unwrap()
            .unwrap()
            .columns[0],
        novarocks::engine::statistics::CatalogColumnStatistics {
            column_name: "k".to_string(),
            row_count: 7,
            min: "1".to_string(),
            max: "9".to_string(),
            ndv: "4".to_string(),
        }
    );

    let error = service
        .try_handle_statement(
            &FakeStatisticsEngine::failing_collect("statistics scan failed"),
            "ANALYZE TABLE db1.broken(k)",
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap_err();
    assert_eq!(error, "statistics scan failed");
}

#[test]
fn histogram_multi_column_and_status_queries_preserve_compatibility_rows() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine::with_local_columns(vec![
        StatisticsColumn {
            name: "k".to_string(),
            data_type: DataType::Int64,
        },
        StatisticsColumn {
            name: "v".to_string(),
            data_type: DataType::Int64,
        },
    ]);
    for statement in [
        "ANALYZE TABLE db1.t1 UPDATE HISTOGRAM ON k",
        "ANALYZE FULL TABLE db1.t1 MULTIPLE COLUMNS (k,v)",
    ] {
        service
            .try_handle_statement(
                &engine,
                statement,
                StatisticsRequestContext {
                    current_catalog: None,
                    current_database: "db1",
                },
            )
            .unwrap()
            .expect("statistics statement");
    }

    let histogram_sql = "SELECT column_name, buckets, mcv \
                         FROM _statistics_.histogram_statistics \
                         WHERE table_name = 'db1.t1'";
    let histogram = service
        .try_query(
            histogram_sql,
            &parse_query(histogram_sql),
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        string_rows(&histogram),
        vec![vec![
            "k".to_string(),
            "[{\"lower\":\"\",\"upper\":\"\"}]".to_string(),
            "{}".to_string(),
        ]]
    );

    let multi_sql = "SELECT table_name, column_names \
                     FROM _statistics_.multi_column_statistics \
                     WHERE table_name = 'db1.t1'";
    let multi = service
        .try_query(
            multi_sql,
            &parse_query(multi_sql),
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        string_rows(&multi),
        vec![vec!["db1.t1".to_string(), "k,v".to_string()]]
    );

    let status_sql = "SELECT `table`, columns, type, status \
                      FROM information_schema.analyze_status \
                      WHERE `database` = 'db1' ORDER BY id";
    let status = service
        .try_query(
            status_sql,
            &parse_query(status_sql),
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        string_rows(&status),
        vec![
            vec![
                "t1".to_string(),
                "k".to_string(),
                "HISTOGRAM".to_string(),
                "FINISH".to_string(),
            ],
            vec![
                "t1".to_string(),
                "k,v".to_string(),
                "FULL".to_string(),
                "FINISH".to_string(),
            ],
        ]
    );
}

#[test]
fn compatibility_overwrite_table_appends_one_row_after_initial_seed_and_reseeds_on_overwrite() {
    let service = FrontendStatisticsService::new();
    let engine = FakeStatisticsEngine::with_local_columns(vec![StatisticsColumn {
        name: "k1".to_string(),
        data_type: DataType::Int64,
    }]);
    let source = StatisticsInsertSource::Values(vec![vec![StatisticsLiteral::Int(123)]]);

    for expected in [3, 4] {
        service
            .observe_insert(
                &engine,
                StatisticsInsertObservation {
                    database: "db1",
                    table: "test_overwrite_stats_table",
                    insert_columns: &[],
                    source: &source,
                    overwrite_mode: StatisticsOverwriteMode::Append,
                },
            )
            .unwrap();
        assert_eq!(
            compatibility_overwrite_row_count(&service),
            expected,
            "append must seed three compatibility rows only once"
        );
    }

    service
        .observe_insert(
            &engine,
            StatisticsInsertObservation {
                database: "db1",
                table: "test_overwrite_stats_table",
                insert_columns: &[],
                source: &source,
                overwrite_mode: StatisticsOverwriteMode::FullTable,
            },
        )
        .unwrap();
    assert_eq!(compatibility_overwrite_row_count(&service), 3);
}

fn compatibility_overwrite_row_count(service: &FrontendStatisticsService) -> usize {
    let sql = "SELECT count(*) FROM _statistics_.column_statistics \
               WHERE table_name = 'db1.test_overwrite_stats_table'";
    let result = service
        .try_query(
            sql,
            &parse_query(sql),
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap()
        .unwrap();
    string_rows(&result)[0][0].parse().unwrap()
}

#[test]
fn cte_name_shadows_a_physical_table_in_outer_query_usage() {
    let service = FrontendStatisticsService::new();
    let query = parse_query(
        "WITH shadowed AS (SELECT source.k FROM source WHERE source.k > 0) \
         SELECT shadowed.k FROM shadowed \
         JOIN physical ON shadowed.k = physical.k \
         WHERE physical.v > 1",
    );
    service.observe_query(&query, "db1").unwrap();

    let sql = "SELECT table_name, column_name, usage \
               FROM information_schema.column_stats_usage \
               WHERE table_database = 'db1' ORDER BY column_name";
    let rows = service
        .try_query(
            sql,
            &parse_query(sql),
            StatisticsRequestContext {
                current_catalog: None,
                current_database: "db1",
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        string_rows(&rows),
        vec![
            vec!["physical".to_string(), "k".to_string(), "join".to_string()],
            vec![
                "source".to_string(),
                "k".to_string(),
                "predicate".to_string(),
            ],
            vec![
                "physical".to_string(),
                "v".to_string(),
                "predicate".to_string(),
            ],
        ]
    );
}
