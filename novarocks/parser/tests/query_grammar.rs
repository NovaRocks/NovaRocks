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
    ast::{Expr, Relation, Select, SetExpr, Statement, SyntaxEq, Table},
    parse,
    printer::Printer,
};

#[test]
fn query_forms_parse_and_print_as_typed_syntax() {
    for source in [
        "SELECT a, count(*) AS n FROM db.t AS t WHERE a >= 1 GROUP BY a HAVING count(*) > 0 ORDER BY a DESC LIMIT 5 OFFSET 2 ROWS",
        "SELECT a FROM db.t ORDER BY a DESC OFFSET 2 ROWS FETCH FIRST 1 ROW ONLY",
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
        "SELECT t.id AS value FROM source AS t JOIN UNNEST(t.items) AS u(item) ON TRUE LEFT JOIN target AS r ON r.id = t.id"
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
    let Some(Relation::TableFunction(call)) = &select.from else {
        panic!("expected table function invocation");
    };
    assert_eq!(call.arguments.len(), 2);
    for (argument, (name, value)) in call.arguments.iter().zip([("end", 5), ("start", 2)]) {
        assert_eq!(argument.name.as_ref().unwrap().value, name);
        assert!(
            matches!(&argument.value,novarocks_parser::ast::TableFunctionArgumentValue::Expression(Expr::Literal(novarocks_parser::ast::Literal {kind:novarocks_parser::ast::LiteralKind::Long{parsed_value,..},..})) if *parsed_value==value)
        );
    }
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
    assert_eq!(query.session_properties.len(), 1);
    assert_eq!(
        query.session_properties[0].name.parts[0].value,
        "new_planner_agg_stage"
    );
    assert!(matches!(
        &query.session_properties[0].value,
        Expr::Literal(_)
    ));

    let canonical = Printer::new().statements(&statements);
    assert_eq!(
        canonical,
        "SELECT /*+ SET_VAR(new_planner_agg_stage = 3) */ 1"
    );

    let reparsed = parse(&canonical).expect("canonical assignment-style hint should parse");
    assert_eq!(Printer::new().statements(&reparsed), canonical);
}

#[test]
fn comma_limit_normalizes_to_offset_and_limit() {
    use novarocks_parser::ast::{Literal, LiteralKind, QueryLimit};
    let source = "SELECT * FROM t LIMIT 2, 10";
    let statements = parse(source).expect("comma LIMIT should parse");
    let Statement::Query(query) = &statements[0] else {
        panic!("query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("SELECT");
    };
    let offset = select.modifiers.offset.as_ref().expect("original offset");
    let Some(QueryLimit::Limit(limit)) = &select.modifiers.limit else {
        panic!("original row count");
    };
    assert!(matches!(
        offset.row_count,
        Expr::Literal(Literal {
            kind: LiteralKind::Long {
                parsed_value: 2,
                ..
            },
            ..
        })
    ));
    assert!(matches!(
        limit.row_count,
        Expr::Literal(Literal {
            kind: LiteralKind::Long {
                parsed_value: 10,
                ..
            },
            ..
        })
    ));
    let offset_span = offset.row_count.span();
    let count_span = limit.row_count.span();
    assert_eq!(&source[offset_span.start()..offset_span.end()], "2");
    assert_eq!(&source[count_span.start()..count_span.end()], "10");
    let canonical = Printer::new().statements(&statements);
    assert_eq!(canonical, "SELECT * FROM t OFFSET 2 ROWS LIMIT 10");
    let reparsed = parse(&canonical).expect("canonical count clauses parse");
    let Statement::Query(reparsed) = &reparsed[0] else {
        panic!("canonical query");
    };
    assert!(query.syntax_eq(reparsed));
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
    let table = table_relation(select);
    let name = &table.name;
    assert_eq!(name.parts.len(), 3);
    assert_eq!(name.parts[0].value, "ice");
    assert_eq!(name.parts[1].value, "analytics");
    assert_eq!(name.parts[2].value, "orders$future_metadata");
    assert!(!name.parts[2].quoted);
    assert_eq!(
        &source[name.parts[2].span.start()..name.parts[2].span.end()],
        "orders$future_metadata"
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
        let table = table_relation(select);
        assert_eq!(table.name.parts.len(), 1);
        let last = &table.name.parts[0];
        assert_eq!(last.quoted, source.contains('`'));
        assert_eq!(
            last.value,
            source
                .trim_start_matches("SELECT * FROM ")
                .trim_matches('`')
        );
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

fn table_relation(select: &Select) -> &Table {
    let mut relation = select.from.as_ref().unwrap();
    loop {
        match relation {
            Relation::Aliased(node) => relation = &node.relation,
            Relation::Table(node) => return node,
            _ => panic!("real table relation"),
        }
    }
}

#[test]
fn deep_cte_and_derived_sources_preserve_the_actual_queries_and_leaf_spans() {
    use novarocks_parser::{Span, ast::LiteralKind};
    const DEPTH: usize = 4_096;
    fn parsed(source: &str) -> novarocks_parser::ast::Query {
        let mut statements = parse(source).unwrap();
        let Statement::Query(query) = statements.remove(0) else {
            panic!("query")
        };
        query
    }
    let source = format!(
        "{}SELECT 7{}",
        "WITH c AS (".repeat(DEPTH),
        ") SELECT 1".repeat(DEPTH)
    );
    let query = parsed(&source);
    let mut cursor = &query;
    for _ in 0..DEPTH {
        let with = cursor.with.as_ref().unwrap();
        assert!(!with.recursive);
        assert_eq!(with.ctes.len(), 1);
        let cte = &with.ctes[0];
        assert_eq!(cte.name.value, "c");
        assert!(cte.columns.is_none());
        cursor = &cte.query;
    }
    assert!(cursor.with.is_none());
    let SetExpr::Select(select) = cursor.body.as_ref() else {
        panic!("actual leaf SELECT")
    };
    let novarocks_parser::ast::SelectItem::UnnamedExpr(Expr::Literal(value)) =
        &select.projection[0]
    else {
        panic!("leaf literal")
    };
    assert!(matches!(
        value.kind,
        LiteralKind::Long {
            parsed_value: 7,
            ..
        }
    ));
    let start = source.find('7').unwrap();
    assert_eq!(value.span, Span::new(start, start + 1));
    drop(query);
    let prefix = "SELECT * FROM (";
    let source = format!("{}SELECT 7{}", prefix.repeat(DEPTH), ")".repeat(DEPTH));
    let query = parsed(&source);
    let mut cursor = &query;
    for level in 0..DEPTH {
        let SetExpr::Select(select) = cursor.body.as_ref() else {
            panic!("actual SELECT")
        };
        let Some(Relation::TableSubquery(child)) = &select.from else {
            panic!("real shared subquery")
        };
        assert_eq!(
            child.span,
            Span::new(
                level * prefix.len() + prefix.find('(').unwrap(),
                source.len() - level
            )
        );
        cursor = &child.query;
    }
    let SetExpr::Select(select) = cursor.body.as_ref() else {
        panic!("leaf SELECT")
    };
    assert!(select.from.is_none());
    let novarocks_parser::ast::SelectItem::UnnamedExpr(Expr::Literal(value)) =
        &select.projection[0]
    else {
        panic!("leaf literal")
    };
    assert!(matches!(
        value.kind,
        LiteralKind::Long {
            parsed_value: 7,
            ..
        }
    ));
    let start = source.find('7').unwrap();
    assert_eq!(value.span, Span::new(start, start + 1));
    drop(query);
    // Scalar-subquery ranges exercise the borrowed expression/statement bridge.
    // This input depth is a test parameter; repeated range scanning remains a
    // separate algorithm cost from token storage and stack growth.
    const SCALAR_DEPTH: usize = 1_024;
    let prefix = "SELECT (";
    let source = format!(
        "{}SELECT 7{}",
        prefix.repeat(SCALAR_DEPTH),
        ")".repeat(SCALAR_DEPTH)
    );
    let query = parsed(&source);
    let mut cursor = &query;
    for _ in 0..SCALAR_DEPTH {
        let SetExpr::Select(select) = cursor.body.as_ref() else {
            panic!("actual scalar SELECT")
        };
        let novarocks_parser::ast::SelectItem::UnnamedExpr(Expr::Subquery(child)) =
            &select.projection[0]
        else {
            panic!("actual scalar subquery")
        };
        cursor = &child.query;
    }
    let SetExpr::Select(select) = cursor.body.as_ref() else {
        panic!("leaf SELECT")
    };
    let novarocks_parser::ast::SelectItem::UnnamedExpr(Expr::Literal(value)) =
        &select.projection[0]
    else {
        panic!("leaf literal")
    };
    assert!(matches!(
        value.kind,
        LiteralKind::Long {
            parsed_value: 7,
            ..
        }
    ));
    let start = source.find('7').unwrap();
    assert_eq!(value.span, Span::new(start, start + 1));
    drop(query);
}
