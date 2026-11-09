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

//! The public SQL compiler refuses only the missing exact conversion shape.

use super::*;
use arrow::datatypes::DataType;
use std::sync::Arc;

fn analyze_decimal_cast(
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
        noop_constant_evaluator(),
        None,
        crate::constant::test_constant_policy(),
        control,
    ))
}

#[test]
fn public_decimal_largeint_cast_is_an_exact_unsupported_expression() {
    for source in [DataType::Decimal128(38, 15), DataType::Decimal256(50, 15)] {
        let error = match analyze_decimal_cast(
            "SELECT CAST(v AS LARGEINT) FROM t",
            source,
            SqlCompileControl::unbounded(),
        ) {
            Err(SqlCompileError::Analyze(error)) => error,
            Err(error) => panic!("expected SQL analysis refusal, got {error:?}"),
            Ok(_) => panic!("unimplemented decimal conversion was accepted"),
        };
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression,
        );
        assert_eq!(error.code().as_str(), "sql.analyze.unsupported_expression");
        assert!(error.span().is_some(), "use the actual CAST parser span");
        assert!(error.control_error().is_none());
        assert!(error.source_failure().is_none());
    }
}

#[test]
fn public_decimal_largeint_refusal_preserves_null_and_other_conversion_paths() {
    for sql in [
        "SELECT CAST(NULL AS LARGEINT)",
        "SELECT CAST(CAST(NULL AS DECIMAL(38,15)) AS LARGEINT)",
    ] {
        assert!(
            analyze_decimal_cast(
                sql,
                DataType::Decimal128(38, 15),
                SqlCompileControl::unbounded(),
            )
            .is_ok(),
            "typed SQL NULL must keep its existing conversion path: {sql}"
        );
    }
    assert!(
        analyze_decimal_cast(
            "SELECT CAST(v AS LARGEINT) FROM t",
            DataType::Int64,
            SqlCompileControl::unbounded(),
        )
        .is_ok(),
        "the installed signed-integer conversion remains available"
    );
    let error = match analyze_decimal_cast(
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
fn public_decimal_largeint_refusal_does_not_replace_cancellation() {
    struct Cancelled;
    impl SqlCancellationObservation for Cancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }
    let result = analyze_decimal_cast(
        "SELECT CAST(v AS LARGEINT) FROM t",
        DataType::Decimal128(38, 15),
        SqlCompileControl::new(None, Arc::new(Cancelled)),
    );
    assert!(matches!(result, Err(SqlCompileError::Cancelled)));
}
