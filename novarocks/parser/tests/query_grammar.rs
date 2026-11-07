// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Query grammar vertical-slice contracts. These cases prove that parser-owned
//! syntax can round-trip without making it a frontend execution route.

use novarocks_parser::{
    ast::{BinaryOperator, Expr, SelectHintValue, SetExpr, Statement, SyntaxEq, TableFactor},
    parse,
    printer::Printer,
};

#[test]
fn query_forms_parse_and_print_as_typed_syntax() {
    for source in [
        "SELECT a, count(*) AS n FROM db.t AS t WHERE a >= 1 GROUP BY a HAVING count(*) > 0 ORDER BY a DESC LIMIT 5 OFFSET 2 ROWS FETCH FIRST 1 ROW ONLY",
        "SELECT CASE WHEN a IS NULL THEN CAST(0 AS INT) ELSE CAST(a AS INT) END AS normalized FROM t WHERE a NOT BETWEEN 1 AND 3",
        "SELECT sum(v) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        "SELECT sum(v) OVER w FROM t WINDOW w AS (PARTITION BY k ORDER BY ts ROWS 1 PRECEDING)",
        "SELECT k, sum(v) FROM t GROUP BY GROUPING SETS ((), (k)) ORDER BY k",
        "SELECT k, sum(v) FROM t GROUP BY ROLLUP(k) UNION ALL SELECT k, sum(v) FROM t GROUP BY CUBE(k)",
        "SELECT u.x FROM t CROSS JOIN LATERAL UNNEST(t.arr) AS u(x)",
        "SELECT x.a FROM (SELECT 1 AS a) AS x",
        "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id) AND a IN (SELECT b FROM v)",
        "SELECT * FROM TABLE(generate_series(1, 10)) AS g(x)",
        "SELECT * FROM a NATURAL LEFT JOIN b",
        "SELECT * FROM events FOR VERSION AS OF 'release_7' AS e",
        "SELECT * FROM events FOR SYSTEM_TIME AS OF 42",
        "SELECT group_concat(DISTINCT a ORDER BY b DESC SEPARATOR ',') FILTER (WHERE a IS NOT NULL) FROM t",
        "SELECT first_value(v) OVER (ORDER BY x) window_value FROM t",
        "WITH c AS (SELECT 1 AS a) SELECT a FROM c UNION ALL SELECT 2 ORDER BY a LIMIT 3",
        "SELECT * FROM t ORDER BY id LIMIT 5, 10",
        "SELECT * FROM t ORDER BY id DESC NULLS LAST",
        "SELECT count(*) AS \"order count\" FROM t",
        "SELECT db.t.*, w.* FROM db.t AS w",
        "WITH a AS (SELECT ARRAY_AGG(`t`.`a`) AS x FROM t AS t) SELECT * FROM a",
        "SELECT ARRAY_AGG(a ORDER BY a ASC NULLS LAST) FROM t",
        "SELECT greatest(date('2026-01-11'), timestamp('2026-01-14 00:00:00'))",
        "SELECT * FROM TABLE(generate_series(end => 5, start => 2))",
        "SELECT * FROM left_table l JOIN right_table r ON l.id = r.id WHERE r.id IS NOT NULL",
        "((SELECT 1 UNION ALL SELECT 2)) ORDER BY 1",
        "SELECT * FROM (((SELECT 1 UNION ALL SELECT 2))) AS source",
        "SELECT * FROM (SELECT 1) catalog",
        "EXPLAIN VERBOSE SELECT k1 FROM __nr_ivm_delta('orders', 0, 0)",
        "EXPLAIN LOGICAL VERBOSE SELECT k1 FROM t",
        "EXPLAIN LOGICAL COSTS SELECT k1 FROM t",
        "SELECT 1 AS \"order count\"",
        "SELECT array_sortby((x) -> x.item, x)",
        "SELECT l.id FROM left_table l LEFT JOIN right_table r ON l.id = r.id JOIN third_table s ON s.id = l.id",
        "EXPLAIN ANALYZE VALUES (1), (2)",
    ] {
        let statements = parse(source).unwrap_or_else(|error| panic!("{source}: {error:?}"));
        assert_eq!(statements.len(), 1, "{source}");
        assert!(matches!(
            statements.first(),
            Some(Statement::Query(_) | Statement::ExplainQuery(_))
        ));
        let canonical = Printer::new().statements(&statements);
        let reparsed =
            parse(&canonical).unwrap_or_else(|error| panic!("canonical `{canonical}`: {error:?}"));
        assert!(matches!(
            reparsed.first(),
            Some(Statement::Query(_) | Statement::ExplainQuery(_))
        ));
    }
}

#[test]
fn source_significant_query_forms_are_retained_by_the_typed_ast() {
    let source = "SELECT t.id AS value FROM source AS t INNER JOIN LATERAL UNNEST(t.items) AS u(item) ON true LEFT OUTER JOIN target AS r ON r.id = t.id";
    let statements = parse(source).expect("source-significant query should parse");

    assert_eq!(
        Printer::new().statements(&statements),
        "SELECT t.id AS value FROM source AS t INNER JOIN LATERAL UNNEST(t.items) AS u(item) ON TRUE LEFT OUTER JOIN target AS r ON r.id = t.id"
    );
}

#[test]
fn table_function_named_arguments_are_typed_locally() {
    let statements = parse("SELECT * FROM TABLE(generate_series(end => 5, start => 2))")
        .expect("table-function named arguments should parse");
    let Statement::Query(query) = &statements[0] else {
        panic!("expected query statement");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected SELECT query body");
    };
    let TableFactor::TableFunction {
        expr: Expr::FunctionCall(call),
        ..
    } = &select.from[0].relation
    else {
        panic!("expected table function call");
    };
    assert!(call.arguments.iter().all(|argument| {
        matches!(
            argument,
            Expr::Binary(binary) if binary.operator == BinaryOperator::NamedArgument
        )
    }));
}

#[test]
fn select_optimizer_hints_are_typed_and_roundtrip() {
    let source = "SELECT /*+ SET_VAR(enable_recursive_cte=true, recursive_cte_max_depth=10) */ 1";
    let statements = parse(source).expect("optimizer hint should parse");

    assert_eq!(
        Printer::new().statements(&statements),
        "SELECT /*+ SET_VAR(enable_recursive_cte = TRUE, recursive_cte_max_depth = 10) */ 1"
    );
}

#[test]
fn select_optimizer_assignment_hint_is_typed_and_roundtrips() {
    let source = "SELECT /*+ new_planner_agg_stage = 3 */ 1";
    let statements = parse(source).expect("assignment-style optimizer hint should parse");

    let Statement::Query(query) = statements.first().expect("one query statement") else {
        panic!("assignment-style optimizer hint must remain a typed query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("assignment-style optimizer hint must remain attached to SELECT");
    };
    assert!(matches!(
        select.hints.as_slice(),
        [hint] if matches!(hint.value, SelectHintValue::Assignment { .. })
    ));

    let canonical = Printer::new().statements(&statements);
    assert_eq!(canonical, "SELECT /*+ new_planner_agg_stage = 3 */ 1");

    let reparsed = parse(&canonical).expect("canonical assignment-style hint should parse");
    assert_eq!(Printer::new().statements(&reparsed), canonical);
}

#[test]
fn comma_limit_syntax_is_retained_by_the_typed_ast() {
    let statements = parse("SELECT * FROM t LIMIT 2, 10").expect("comma LIMIT should parse");

    assert_eq!(
        Printer::new().statements(&statements),
        "SELECT * FROM t LIMIT 2, 10"
    );
}

#[test]
fn metadata_table_suffix_is_typed_parse_broad_and_roundtrips() {
    let source = "SELECT * FROM ice.analytics.orders$future_metadata AS metadata";
    let statements = parse(source).expect("metadata-table syntax should parse");
    let Statement::Query(query) = &statements[0] else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected SELECT");
    };
    let TableFactor::Table { name, metadata, .. } = &select.from[0].relation else {
        panic!("expected table relation");
    };
    assert_eq!(name.parts.len(), 3);
    assert_eq!(name.parts[0].value, "ice");
    assert_eq!(name.parts[1].value, "analytics");
    assert_eq!(name.parts[2].value, "orders");
    let metadata = metadata.as_ref().expect("typed metadata suffix");
    assert_eq!(metadata.value, "future_metadata");
    assert_eq!(
        &source[metadata.span.start()..metadata.span.end()],
        "future_metadata"
    );

    let canonical = Printer::new().statements(&statements);
    assert_eq!(canonical, source);
    let reparsed = parse(&canonical).expect("canonical metadata-table syntax should parse");
    let Statement::Query(reparsed_query) = &reparsed[0] else {
        panic!("expected reparsed query");
    };
    assert!(query.syntax_eq(reparsed_query));
}

#[test]
fn quoted_or_incomplete_dollar_names_remain_regular_table_names() {
    for source in [
        "SELECT * FROM `orders$snapshots`",
        "SELECT * FROM orders$",
        "SELECT * FROM $orders",
    ] {
        let statements = parse(source).expect("regular identifier should parse");
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected SELECT");
        };
        let TableFactor::Table { metadata, .. } = &select.from[0].relation else {
            panic!("expected table relation");
        };
        assert!(metadata.is_none(), "{source}");
        assert_eq!(Printer::new().statements(&statements), source);
    }
}

#[test]
fn table_hints_are_rejected_in_every_relation_position() {
    for source in [
        "SELECT * FROM [broadcast] t",
        "SELECT * FROM t [shuffle]",
        "SELECT * FROM t[_META_]",
        "SELECT * FROM t [_META_]",
        "SELECT * FROM t JOIN [broadcast] u ON t.id = u.id",
        "SELECT * FROM t JOIN [skew|t.id(1, 2)] u ON t.id = u.id",
        "SELECT * FROM t JOIN [broadcast] (SELECT 1) u ON true",
        "SELECT * FROM TABLE(generate_series(1, 3)) [shuffle]",
    ] {
        assert!(parse(source).is_err(), "{source}");
    }
}
