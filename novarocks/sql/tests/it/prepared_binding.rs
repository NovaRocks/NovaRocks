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

use arrow::datatypes::{DataType, Field};
use novarocks_functions::ConstantValue;
use novarocks_parser::{
    ast::TrinoStatement,
    parser::{parse_expression, parse_statement},
};
use novarocks_sql::compiler::{
    PreparedParameterError, SqlCompileControl, SqlCompileError, SqlPreparedParameters,
};
use novarocks_sql_testkit::sql_fixture_constant_policy;
use novarocks_type_contract::{CompilePhase, FunctionValueType};

fn statement_query(statement: &TrinoStatement) -> &novarocks_parser::ast::Query {
    let TrinoStatement::Query(query) = statement else {
        panic!("expected a query");
    };
    query
}

fn count(value: Option<i64>) -> ConstantValue {
    let nullable = value.is_none();
    let field = Arc::new(Field::new("count", DataType::Int64, nullable));
    let ty = FunctionValueType::new(DataType::Int64, nullable);
    let policy = sql_fixture_constant_policy();
    let phase = CompilePhase::Validate;
    let control = SqlCompileControl::unbounded();
    match value {
        Some(value) => ConstantValue::from_i64(field, ty, value, policy, phase, &control),
        None => ConstantValue::null(field, ty, policy, phase, &control),
    }
    .unwrap()
}

#[test]
fn binding_uses_the_lexical_positions_of_the_original_statement() {
    let template = "WITH SESSION pipeline_dop = ? WITH t(x) AS (VALUES ?) SELECT x, ? FROM t ORDER BY ? LIMIT ?";
    let statement = parse_statement(template).unwrap();
    let arguments = ["1", "11", "CAST(NULL AS BIGINT)", "2", "1 + 1"]
        .map(|text| parse_expression(text).unwrap())
        .to_vec();
    let parameters =
        SqlPreparedParameters::bind(&statement, arguments.clone(), Arc::from("USING")).unwrap();
    let spans = parameters.positions();
    assert_eq!(
        spans.iter().map(|span| span.start()).collect::<Vec<_>>(),
        template
            .match_indices('?')
            .map(|(offset, _)| offset)
            .collect::<Vec<_>>()
    );
    for (span, argument) in spans.iter().zip(&arguments) {
        assert_eq!(parameters.argument_at(*span), Some(argument));
    }
    let query = statement_query(&statement);
    assert_eq!(parameters.contextual_needs(query)[0].span(), spans[4]);
    let error = parameters.complete_query(query.clone()).unwrap_err();
    assert!(matches!(error, SqlCompileError::InvalidRequest(_)));
    let completed = parameters
        .with_contextual_values(query, vec![count(Some(2))], &SqlCompileControl::unbounded())
        .unwrap();
    let prepared = completed.complete_query(query.clone()).unwrap();
    assert!(completed.contextual_needs(&prepared).is_empty());
    assert_eq!(parameters.positions().len(), 5);
    assert_eq!(completed.argument_at(spans[3]), Some(&arguments[3]));
}

#[test]
fn row_counts_reject_null_and_negative_facts_at_their_use_site() {
    let statement = parse_statement("SELECT 1 LIMIT ?").unwrap();
    let query = statement_query(&statement);
    let parameters = SqlPreparedParameters::bind(
        &statement,
        vec![parse_expression("1").unwrap()],
        Arc::from("USING 1"),
    )
    .unwrap();
    for (value, code) in [
        (None, "sql.analyze.invalid_argument"),
        (Some(-1), "sql.analyze.numeric_value_out_of_range"),
    ] {
        let error = parameters
            .with_contextual_values(query, vec![count(value)], &SqlCompileControl::unbounded())
            .unwrap_err();
        let SqlCompileError::Analyze(error) = error else {
            panic!("{error:?}")
        };
        assert_eq!(error.code().as_str(), code);
        assert_eq!(error.span(), Some(parameters.positions()[0]));
    }
}

#[test]
fn descriptions_refuse_version_selection_before_catalog_work() {
    let statement = parse_statement("SELECT * FROM iceberg.db.t FOR VERSION AS OF ?").unwrap();
    let description = SqlPreparedParameters::describe(&statement);
    let error = description
        .complete_query(statement_query(&statement).clone())
        .unwrap_err();
    let SqlCompileError::Analyze(error) = error else {
        panic!("{error:?}")
    };
    assert_eq!(error.code().as_str(), "sql.analyze.unsupported_query_shape");
    assert_eq!(error.span(), Some(description.positions()[0]));
}

#[test]
fn argument_validation_distinguishes_column_references_and_unbound_markers() {
    let statement = parse_statement("SELECT ?").unwrap();
    for (source, expected) in [("x", true), ("?", false)] {
        let expression = parse_expression(source).unwrap();
        let span = expression.span();
        let failure = SqlPreparedParameters::bind(&statement, vec![expression], Arc::from(source))
            .unwrap_err();
        assert_eq!(
            failure,
            if expected {
                PreparedParameterError::NonConstant(span)
            } else {
                PreparedParameterError::UnboundArgument(span)
            }
        );
    }
}
