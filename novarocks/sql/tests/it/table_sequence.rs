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
use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::compiler::{
    SqlCompilation, SqlCompileControl, SqlCompileError, SqlCompileProgress,
    SqlCompileProgressError, SqlCompiler, SqlFactBatch, SqlNeedBatch, TableSequenceArgumentFact,
    TableSequenceArgumentNeed,
};
use novarocks_type_contract::CompilePhase;

use crate::fixtures::{session, table_free_request};

fn start(
    text: &str,
    control: &SqlCompileControl,
) -> Result<SqlCompileProgress, SqlCompileProgressError> {
    let request = table_free_request(text, session(Default::default(), true), control);
    SqlCompiler::start(request.try_into_completion()?, control)
}

fn needs(compilation: &SqlCompilation) -> &[TableSequenceArgumentNeed] {
    let SqlNeedBatch::TableSequenceArguments(needs) = compilation.needs() else {
        panic!("expected table sequence arguments");
    };
    needs
}

fn evaluate_fixture(need: &TableSequenceArgumentNeed, value: i64) -> TableSequenceArgumentFact {
    let original = need.prepared();
    let field = Arc::new(Field::new(
        "value",
        DataType::Int64,
        original.value_type().nullable(),
    ));
    let value = ConstantValue::from_i64(
        field,
        original.value_type().clone(),
        value,
        original.constant_policy(),
        CompilePhase::Validate,
        original.control(),
    )
    .unwrap();
    TableSequenceArgumentFact::from_evaluated(need, value).unwrap()
}

#[test]
fn table_sequence_reuses_one_cte_argument_request() {
    let control = SqlCompileControl::unbounded();
    let sql = "WITH sample AS (
        SELECT sequential_number FROM TABLE(sequence(stop => CAST(floor(random() * 100) AS BIGINT)))
    )
    SELECT min(n) = max(n) AS same_count
    FROM (SELECT count(*) AS n FROM sample UNION ALL SELECT count(*) AS n FROM sample) AS counts";
    let SqlCompileProgress::Incomplete(compilation) = start(sql, &control).unwrap() else {
        panic!("argument value must come from original statement evaluation");
    };
    let [stop] = needs(&compilation) else {
        panic!("one STOP argument for both CTE references");
    };
    let fact = evaluate_fixture(stop, 3);
    let result = SqlCompiler::finish(
        compilation,
        SqlFactBatch::TableSequenceArguments(Box::new([fact])),
        &control,
    )
    .unwrap()
    .into_complete()
    .unwrap();
    let sequence_sources = result
        .plan()
        .fragments()
        .values()
        .flat_map(|fragment| fragment.nodes().values())
        .filter(|node| {
            matches!(
                node.kind,
                novarocks_physical_plan::NodeKind::GenerateSeries { .. }
            )
        })
        .count();
    assert_eq!(sequence_sources, 1);
}

#[test]
fn table_sequence_rejects_facts_from_another_prepared_occurrence() {
    let control = SqlCompileControl::unbounded();
    let start_query = || {
        let SqlCompileProgress::Incomplete(compilation) =
            start("SELECT * FROM TABLE(sequence(stop => 1 + 2))", &control).unwrap()
        else {
            panic!("prepared table argument");
        };
        compilation
    };
    let foreign = start_query();
    let wrong = evaluate_fixture(&needs(&foreign)[0], 3);
    assert!(
        SqlCompiler::finish(
            start_query(),
            SqlFactBatch::TableSequenceArguments(Box::new([wrong])),
            &control,
        )
        .is_err()
    );
}

#[test]
fn table_sequence_reports_typed_binding_errors() {
    let control = SqlCompileControl::unbounded();
    for (call, kind) in [
        ("sequence(3)", AnalyzeErrorKind::MissingArgument),
        ("sequence(0, stop => 3)", AnalyzeErrorKind::InvalidArgument),
        (
            "sequence(stop => 3, stop => 5)",
            AnalyzeErrorKind::InvalidFunctionArgument,
        ),
        (
            "sequence(\"stop\" => 3)",
            AnalyzeErrorKind::InvalidFunctionArgument,
        ),
        ("sequence(stop => '3')", AnalyzeErrorKind::TypeMismatch),
        (
            "sequence(stop => (SELECT 3))",
            AnalyzeErrorKind::ExpressionNotConstant,
        ),
        ("generate_series(0, 3)", AnalyzeErrorKind::UnknownFunction),
    ] {
        let text = format!("SELECT * FROM TABLE({call})");
        let Err(SqlCompileProgressError::Compile(SqlCompileError::Analyze(error))) =
            start(&text, &control)
        else {
            panic!("expected typed rejection for {call}");
        };
        assert_eq!(error.kind(), kind, "{call}: {error}");
        assert!(error.span().is_some(), "{call}");
    }
}
