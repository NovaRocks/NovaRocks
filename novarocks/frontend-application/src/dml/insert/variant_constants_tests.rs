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

use super::shaping::{shape_insert_source, shape_insert_source_with_constants};
use arrow::datatypes::DataType;
use novarocks_parser::ast::{self, SyntaxEq};
use novarocks_sql::compiler::{
    SessionOptimizerSettings, SqlAnalyzeRequest, SqlCompileControl, SqlCompileError,
    SqlCompileIntent, SqlCompiler, SqlExpressionConstants, SqlPlannerTableSnapshot,
    SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use novarocks_types::schema::ColumnDef;

fn parse_query(sql: &str) -> ast::Query {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query")
    };
    query.clone()
}
fn columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef::from_value_type(
            "id".into(),
            FunctionValueType::new(DataType::Int32, true),
            None,
        )
        .unwrap(),
        ColumnDef::from_value_type(
            "v".into(),
            FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                true,
                ValueLogicalType::Variant,
            )
            .unwrap(),
            None,
        )
        .unwrap(),
    ]
}
fn shape(sql: &str) -> (ast::Query, Option<SqlExpressionConstants>) {
    let columns = columns();
    shape_insert_source_with_constants(
        &parse_query(sql),
        &[],
        &columns,
        &columns,
        crate::application::test_constant_policy(),
        &SqlCompileControl::unbounded(),
    )
    .unwrap()
    .unwrap()
}
fn analyze(
    query: ast::Query,
    constants: Option<SqlExpressionConstants>,
) -> Result<(), SqlCompileError> {
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let functions = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let control = SqlCompileControl::unbounded();
    let statement = match constants {
        Some(constants) => SqlStatementInput::query_with_expression_constants(
            Box::new(query),
            constants,
            &control,
        )?,
        None => SqlStatementInput::parsed_query(Box::new(query)),
    };
    SqlCompiler::analyze(SqlAnalyzeRequest::new(
        statement,
        SqlCompileIntent::Query,
        SqlSessionContext {
            current_catalog: None,
            current_database: "db".into(),
            optimizer_settings: SessionOptimizerSettings::default(),
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        },
        SqlPlanningEnvironment::Distributed,
        &snapshot,
        &functions,
        crate::query_execution::constant_eval::constant_evaluator(),
        None,
        crate::application::test_constant_policy(),
        control,
    ))?
    .into_pending()
    .map(|_| ())
}

#[test]
fn original_encoder_keeps_variant_constants_and_mixed_multiline_syntax() {
    let sql = r#"VALUES (1 + 2, parse_json('{"a":1}')), (NULL, parse_json('null')), (7, NULL)"#;
    let (shaped, constants) = shape(sql);
    let constants = constants.unwrap();
    let columns = columns();
    let original = shape_insert_source(&parse_query(sql), &[], &columns, &columns)
        .unwrap()
        .unwrap();
    let (ast::SetExpr::Values(actual), ast::SetExpr::Values(expected)) =
        (shaped.body.as_ref(), original.body.as_ref())
    else {
        panic!("values")
    };
    assert_eq!(constants.len(), 2);
    assert!(shaped.body.syntax_eq(&original.body));
    for (row, original_row) in actual.rows.iter().zip(&expected.rows) {
        assert!(
            row[0].syntax_eq(&original_row[0]),
            "other cells use their original source"
        );
    }
    for (row, json) in [(0, r#"{"a":1}"#), (1, "null")] {
        let value = constants[row].value();
        assert_eq!(value.value_type().logical_type(), ValueLogicalType::Variant);
        assert_eq!(value.value_type().data_type(), &DataType::LargeBinary);
        assert!(!value.value_type().nullable());
        assert!(value.field().metadata().as_arc().is_none());
        let expected =
            novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes(json)
                .unwrap();
        assert_eq!(value.try_binary().unwrap(), Some(expected.as_slice()));
    }
    assert_eq!(constants.len(), 2, "SQL NULL has no replacement constant");
    analyze(shaped, Some(constants)).expect("actual compiler keeps complete Variant domains");
}

#[test]
fn ordinary_binary_bytes_do_not_establish_variant() {
    let (query, constants) = shape("VALUES (1, X'050000001100000C2A')");
    assert!(constants.is_none());
    let error = analyze(query, None).unwrap_err();
    assert!(
        error.to_string().contains("no matching declared overload"),
        "{error}"
    );
}

#[test]
fn mixed_source_reports_the_original_first_syntax_error() {
    let (query, constants) = shape(
        "VALUES (original_first_column, parse_json('{}')), (unreachable_later_column, parse_json('null'))",
    );
    let actual = analyze(query, constants).unwrap_err();
    let (query, constants) =
        shape("VALUES (original_first_column, NULL), (unreachable_later_column, NULL)");
    let original = analyze(query, constants).unwrap_err();
    assert_eq!(actual, original);
    assert!(actual.to_string().contains("original_first_column"));
    assert!(!actual.to_string().contains("unreachable_later_column"));
}

#[test]
fn original_control_refusal_stops_before_the_next_variant_encoder() {
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    struct Refusal(std::sync::Mutex<usize>);
    impl PureCompileControl for Refusal {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            let mut calls = self.0.lock().unwrap();
            *calls += 1;
            assert_eq!(*calls, 1, "no callback after first refusal");
            Err(CompileControlError::Cancelled)
        }
    }
    let control = Refusal(std::sync::Mutex::new(0));
    let columns = columns();
    let error = shape_insert_source_with_constants(
        &parse_query("VALUES (1, parse_json('{}')), (2, parse_json('{'))"),
        &[],
        &columns,
        &columns,
        crate::application::test_constant_policy(),
        &control,
    )
    .unwrap_err();
    assert_eq!(error, CompileControlError::Cancelled.to_string());
    assert_eq!(*control.0.lock().unwrap(), 1);
}

#[test]
fn constant_occurrences_follow_the_original_insert_column_mapping() {
    let columns = columns();
    let (query, constants) = shape_insert_source_with_constants(
        &parse_query("VALUES (parse_json('42'), 7), (NULL, 8)"),
        &["v".into(), "id".into()],
        &columns,
        &columns,
        crate::application::test_constant_policy(),
        &SqlCompileControl::unbounded(),
    )
    .unwrap()
    .unwrap();
    let constants = constants.unwrap();
    assert_eq!(constants.len(), 1);
    assert_eq!(
        constants[0].value().value_type().logical_type(),
        ValueLogicalType::Variant
    );
    analyze(query, Some(constants))
        .expect("the typed constant follows its exact target occurrence");
}

#[test]
fn all_original_select_nested_and_union_leaves_keep_encoded_constants() {
    let columns = vec![columns().remove(1)];
    for (sql, expected_occurrences) in [
        ("VALUES (parse_json('42'))", 1),
        ("SELECT parse_json(('42'))", 1),
        ("VALUES (CAST(parse_json('42') AS VARIANT))", 1),
        // The preserved explicit source cast and target wrapper are distinct
        // occurrences borrowing the same original encoded constant.
        ("SELECT CAST(parse_json('42') AS VARIANT)", 2),
        ("SELECT ((CAST(parse_json('42') AS VARIANT)))", 2),
        ("VALUES (parse_json(CAST('42' AS VARCHAR)))", 1),
        ("SELECT parse_json(CAST('42' AS VARCHAR))", 1),
        (
            "(SELECT parse_json('42')) UNION ALL (SELECT parse_json('null'))",
            2,
        ),
        (
            "VALUES (parse_json('42')) UNION ALL SELECT parse_json('null')",
            2,
        ),
    ] {
        let (query, constants) = shape_insert_source_with_constants(
            &parse_query(sql),
            &[],
            &columns,
            &columns,
            crate::application::test_constant_policy(),
            &SqlCompileControl::unbounded(),
        )
        .unwrap()
        .unwrap();
        let original = shape_insert_source(&parse_query(sql), &[], &columns, &columns)
            .unwrap()
            .unwrap();
        assert!(
            query.body.syntax_eq(&original.body),
            "the old source tree is retained: {sql}"
        );
        let constants = constants.expect("every original eligible leaf retains its source type");
        assert_eq!(constants.len(), expected_occurrences);
        if matches!(
            sql,
            "SELECT CAST(parse_json('42') AS VARIANT)"
                | "SELECT ((CAST(parse_json('42') AS VARIANT)))"
        ) {
            assert_eq!(
                constants[0].value().pool().backing_identity(),
                constants[1].value().pool().backing_identity()
            );
        }
        analyze(query, Some(constants)).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}
#[test]
fn select_filter_having_and_distinct_keep_the_original_inner_owner() {
    let source =
        parse_query("SELECT DISTINCT parse_json('42') AS v WHERE false HAVING v IS NOT NULL");
    let columns = vec![columns().remove(1)];
    let (query, constants) = shape_insert_source_with_constants(
        &source,
        &[],
        &columns,
        &columns,
        crate::application::test_constant_policy(),
        &SqlCompileControl::unbounded(),
    )
    .unwrap()
    .unwrap();
    let ast::SetExpr::Query(wrapper) = query.body.as_ref() else {
        panic!("target wrapper")
    };
    let ast::SetExpr::Select(wrapper) = wrapper.body.as_ref() else {
        panic!("target SELECT")
    };
    let ast::TableFactor::Derived { subquery, .. } = &wrapper.from[0].relation else {
        panic!("inner owner")
    };
    assert!(subquery.body.syntax_eq(&source.body));
    analyze(query, constants)
        .expect("the original SELECT still owns its cardinality and alias policies");
}
