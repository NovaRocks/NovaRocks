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

//! SQL analysis reports the retired LARGEINT declaration with its original source context.

use super::*;
use arrow::datatypes::DataType;
use std::sync::Arc;

fn analyze_largeint_cast(
    sql: &str,
    source_type: DataType,
    control: SqlCompileControl,
) -> Result<SqlAnalyzeOutput, SqlCompileError> {
    let mut catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    catalog.create_database("db").unwrap();
    catalog
        .register(
            "db",
            crate::planner::table::TableDef {
                name: "t".into(),
                columns: vec![novarocks_types::schema::ColumnDef {
                    name: "v".into(),
                    data_type: source_type,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: crate::planner::table::test_sql_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
        )
        .unwrap();
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(crate::compiler::test_query(sql)),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: Default::default(),
            current_catalog: None,
            current_database: "db".into(),
            optimizer_settings: Default::default(),
        },
        SqlPlanningEnvironment::Distributed,
        &snapshot,
        crate::functions::builtin_sql_function_catalog(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        None,
        crate::constant::test_constant_policy(),
        control,
    ))
}

fn assert_retired_largeint_target(sql: &str, source: DataType) {
    let error = match analyze_largeint_cast(sql, source, SqlCompileControl::unbounded()) {
        Err(SqlCompileError::Analyze(error)) => error,
        Err(error) => panic!("expected a retired type error, got {error:?}"),
        Ok(_) => panic!("a retired LARGEINT conversion was accepted: {sql}"),
    };
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::TypeMismatch
    );
    assert_eq!(error.code().as_str(), "sql.analyze.type_mismatch");
    let start = sql.find("LARGEINT").unwrap();
    assert_eq!(
        error.span(),
        Some(novarocks_parser::Span::new(start, start + 8))
    );
}

#[test]
fn public_numeric_largeint_cast_reports_the_retired_target_for_each_source() {
    for source in [
        DataType::Decimal128(38, 15),
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
        DataType::Int64,
        DataType::Binary,
    ] {
        assert_retired_largeint_target("SELECT CAST(v AS LARGEINT) FROM t", source);
    }
}

#[test]
fn sql_null_and_try_cast_keep_the_same_retired_largeint_diagnostic() {
    for sql in [
        "SELECT CAST(NULL AS LARGEINT)",
        "SELECT CAST(CAST(NULL AS DECIMAL(38,15)) AS LARGEINT)",
        "SELECT TRY_CAST(NULL AS LARGEINT)",
    ] {
        assert_retired_largeint_target(sql, DataType::Decimal128(38, 15));
    }
}

#[test]
fn public_decimal_largeint_cast_preserves_cancellation() {
    struct Cancelled;
    impl SqlCancellationObservation for Cancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }
    let result = analyze_largeint_cast(
        "SELECT CAST(v AS LARGEINT) FROM t",
        DataType::Decimal128(38, 15),
        SqlCompileControl::new(None, Arc::new(Cancelled)),
    );
    assert!(matches!(result, Err(SqlCompileError::Cancelled)));
}
