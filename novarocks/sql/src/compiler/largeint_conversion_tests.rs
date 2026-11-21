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

//! SQL analysis preserves exact LARGEINT conversion, typing, and control boundaries.

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

#[test]
fn public_numeric_largeint_cast_accepts_all_five_new_source_types() {
    // B8c-6 installs these conversions; the prior Decimal refusal is retired.
    for source in [
        DataType::Decimal128(38, 15),
        DataType::Decimal256(50, 15),
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
    ] {
        assert!(
            analyze_largeint_cast(
                "SELECT CAST(v AS LARGEINT) FROM t",
                source.clone(),
                SqlCompileControl::unbounded(),
            )
            .is_ok(),
            "the exact {source:?} -> LARGEINT conversion must bind"
        );
    }
}

#[test]
fn public_largeint_cast_preserves_null_and_other_conversion_paths() {
    for sql in [
        "SELECT CAST(NULL AS LARGEINT)",
        "SELECT CAST(CAST(NULL AS DECIMAL(38,15)) AS LARGEINT)",
    ] {
        assert!(
            analyze_largeint_cast(
                sql,
                DataType::Decimal128(38, 15),
                SqlCompileControl::unbounded(),
            )
            .is_ok(),
            "typed SQL NULL must keep its existing conversion path: {sql}"
        );
    }
    assert!(
        analyze_largeint_cast(
            "SELECT CAST(v AS LARGEINT) FROM t",
            DataType::Int64,
            SqlCompileControl::unbounded(),
        )
        .is_ok(),
        "the installed signed-integer conversion remains available"
    );
    let error = match analyze_largeint_cast(
        "SELECT CAST(v AS LARGEINT) FROM t",
        DataType::Binary,
        SqlCompileControl::unbounded(),
    ) {
        Err(SqlCompileError::Analyze(error)) => error,
        Err(error) => panic!("expected existing type error, got {error:?}"),
        Ok(_) => panic!("invalid binary conversion was accepted"),
    };
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::TypeMismatch
    );
    assert_eq!(error.code().as_str(), "sql.analyze.type_mismatch");
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
