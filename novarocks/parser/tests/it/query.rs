// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use novarocks_parser::ast::*;
use novarocks_parser::{trino_parser, trino_printer};

fn query(sql: &str) -> Query {
    trino_parser::parse_query(sql).expect(sql)
}

fn select(query: &Query) -> &Select {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a direct specification")
    };
    select
}

fn expression(item: &SelectItem) -> &Expr {
    let SelectItem::SingleColumn(item) = item else {
        panic!("expected a single column")
    };
    &item.expression
}

fn parameter(expression: &Expr) -> usize {
    let Expr::Parameter(parameter) = expression else {
        panic!("expected a parameter")
    };
    parameter.position
}

#[test]
fn trino_query_suffix_has_the_actual_query_body_owner() {
    let parsed = query("SELECT a FROM t ORDER BY a OFFSET 2 ROWS FETCH FIRST ROW ONLY");
    assert!(parsed.modifiers.is_empty());
    let specification = select(&parsed);
    assert_eq!(specification.projection.len(), 1);
    let order = specification.modifiers.order_by.as_ref().unwrap();
    assert_eq!(order.items[0].ordering, SortOrdering::Ascending);
    assert_eq!(order.items[0].null_ordering, NullOrdering::Undefined);
    let Expr::Literal(Literal {
        kind: LiteralKind::Long {
            spelling,
            parsed_value,
        },
        ..
    }) = &specification.modifiers.offset.as_ref().unwrap().row_count
    else {
        panic!()
    };
    assert_eq!((spelling.as_str(), *parsed_value), ("2", 2));
    let QueryLimit::FetchFirst(fetch) = specification.modifiers.limit.as_ref().unwrap() else {
        panic!()
    };
    assert!(fetch.row_count.is_none());
    assert!(!fetch.with_ties);

    let parsed = query("VALUES 1, ROW(2) ORDER BY 1 LIMIT ALL");
    assert!(parsed.modifiers.order_by.is_some());
    let QueryLimit::Limit(limit) = parsed.modifiers.limit.as_ref().unwrap() else {
        panic!()
    };
    assert!(matches!(limit.row_count, Expr::AllRows(_)));
    let SetExpr::Values(values) = parsed.body.as_ref() else {
        panic!()
    };
    assert!(matches!(values.rows[0], Expr::Literal(_)));
    assert!(matches!(&values.rows[1], Expr::Row(row) if row.fields.len()==1));

    let parsed = query("(SELECT 1) ORDER BY 1 LIMIT 2");
    assert!(parsed.modifiers.order_by.is_some());
    let SetExpr::Query(inner) = parsed.body.as_ref() else {
        panic!()
    };
    assert!(inner.query.modifiers.is_empty());
    assert!(select(&inner.query).modifiers.is_empty());
}

#[test]
fn trino_query_parameter_order_matches_actual_astbuilder_visits() {
    let parsed = query("WITH c AS (SELECT ?) SELECT (SELECT ?), ? FROM c OFFSET ? ROWS LIMIT ?");
    let specification = select(&parsed);
    let Expr::Subquery(scalar) = expression(&specification.projection[0]) else {
        panic!()
    };
    assert_eq!(
        parameter(expression(&select(&scalar.query).projection[0])),
        0
    );
    assert_eq!(parameter(expression(&specification.projection[1])), 1);
    assert_eq!(
        parameter(&specification.modifiers.offset.as_ref().unwrap().row_count),
        2
    );
    let QueryLimit::Limit(limit) = specification.modifiers.limit.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(parameter(&limit.row_count), 3);
    assert_eq!(
        parameter(expression(
            &select(&parsed.with.as_ref().unwrap().ctes[0].query).projection[0]
        )),
        4
    );

    let parsed = query("WITH SESSION x = ? WITH c AS (SELECT ?) SELECT ?");
    assert_eq!(parameter(expression(&select(&parsed).projection[0])), 0);
    assert_eq!(
        parameter(expression(
            &select(&parsed.with.as_ref().unwrap().ctes[0].query).projection[0]
        )),
        1
    );
    assert_eq!(parameter(&parsed.session_properties[0].value), 2);
}

#[test]
fn trino_query_set_precedence_and_parentheses_survive_flattening() {
    let parsed =
        query("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 INTERSECT SELECT 4 EXCEPT SELECT 5");
    let SetExpr::SetOperation(except) = parsed.body.as_ref() else {
        panic!()
    };
    assert_eq!(except.operator, SetOperator::Except);
    assert!(except.distinct);
    let SetExpr::SetOperation(union) = &except.inputs[0] else {
        panic!()
    };
    assert_eq!(union.operator, SetOperator::Union);
    assert!(!union.distinct);
    assert_eq!(union.inputs.len(), 3);
    assert!(
        matches!(&union.inputs[2], SetExpr::SetOperation(op) if op.operator==SetOperator::Intersect)
    );

    let parsed = query("(SELECT 1 UNION ALL SELECT 2) UNION ALL SELECT 3");
    let SetExpr::SetOperation(union) = parsed.body.as_ref() else {
        panic!()
    };
    assert_eq!(union.inputs.len(), 2);
    assert!(matches!(union.inputs[0], SetExpr::Query(_)));
    let parsed = query("SELECT 1 UNION ALL CORRESPONDING BY (x) SELECT 2 UNION ALL SELECT 3");
    let SetExpr::SetOperation(union) = parsed.body.as_ref() else {
        panic!()
    };
    assert_eq!(union.inputs.len(), 2);
    let SetExpr::SetOperation(left) = &union.inputs[0] else {
        panic!()
    };
    assert_eq!(left.corresponding.as_ref().unwrap().columns[0].value, "x");
}

#[test]
fn trino_query_values_keeps_scalar_rows_and_nested_row_cells_distinct() {
    let parsed = query("VALUES 1, ROW(2), ROW(ROW(3))");
    let SetExpr::Values(values) = parsed.body.as_ref() else {
        panic!()
    };
    assert_eq!(values.rows.len(), 3);
    assert!(matches!(values.rows[0], Expr::Literal(_)));
    let Expr::Row(one) = &values.rows[1] else {
        panic!()
    };
    assert!(matches!(one.fields[0].expression, Expr::Literal(_)));
    let Expr::Row(outer) = &values.rows[2] else {
        panic!()
    };
    assert!(matches!(outer.fields[0].expression, Expr::Row(_)));
}

#[test]
fn trino_query_projection_uses_true_primary_context_and_alias_facts() {
    let parsed = query("SELECT t.*, (a + b).* AS (x, y), a AS offset FROM t LIMIT 3");
    let specification = select(&parsed);
    assert_eq!(specification.projection.len(), 3);
    let SelectItem::AllColumns(target) = &specification.projection[1] else {
        panic!()
    };
    assert!(matches!(target.target, Some(Expr::Binary(_))));
    assert_eq!(
        target
            .aliases
            .iter()
            .map(|x| x.value.as_str())
            .collect::<Vec<_>>(),
        ["x", "y"]
    );
    assert!(specification.modifiers.limit.is_some());
    assert!(trino_parser::parse_query("SELECT a + b.* FROM t").is_err());
    assert!(trino_parser::parse_query("SELECT * AS (x) FROM t").is_err());
    let parsed = query("SELECT a limit FROM t offset");
    let SelectItem::SingleColumn(item) = &select(&parsed).projection[0] else {
        panic!()
    };
    assert_eq!(item.alias.as_ref().unwrap().value, "limit");
    let Relation::Aliased(table) = select(&parsed).from.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(table.alias.value, "offset");
}

#[test]
fn trino_query_grouping_sets_preserve_real_grouping_lists() {
    let parsed = query(
        "SELECT a, b FROM t GROUP BY DISTINCT (a,b), ROW(a,b), (), GROUPING SETS ((a),(b)), ROLLUP ((a,b),c)",
    );
    let group = select(&parsed).group_by.as_ref().unwrap();
    assert!(group.distinct);
    let GroupingElement::Simple(columns) = &group.elements[0] else {
        panic!()
    };
    assert_eq!(columns.columns.len(), 2);
    let GroupingElement::Simple(columns) = &group.elements[1] else {
        panic!()
    };
    assert!(matches!(&columns.columns[0], Expr::Row(row) if row.fields.len()==2));
    let GroupingElement::Simple(empty) = &group.elements[2] else {
        panic!()
    };
    assert!(empty.columns.is_empty());
    let GroupingElement::Sets(sets) = &group.elements[3] else {
        panic!()
    };
    assert_eq!(sets.kind, GroupingSetsKind::Explicit);
    assert_eq!(sets.sets.len(), 2);
    let GroupingElement::Sets(rollup) = &group.elements[4] else {
        panic!()
    };
    assert_eq!(rollup.kind, GroupingSetsKind::Rollup);
    assert_eq!((rollup.sets[0].len(), rollup.sets[1].len()), (2, 1));
    // The actual pinned GroupingSets constructor rejects an empty outer list.
    for sql in ["SELECT 1 GROUP BY ROLLUP()", "SELECT 1 GROUP BY CUBE()"] {
        assert!(trino_parser::parse_query(sql).is_err(), "{sql}");
    }
    let parsed = query("SELECT 1 GROUP BY GROUPING SETS (())");
    let GroupingElement::Sets(one_empty) = &select(&parsed).group_by.as_ref().unwrap().elements[0]
    else {
        panic!()
    };
    assert_eq!(one_empty.sets.len(), 1);
    assert!(one_empty.sets[0].is_empty());
}

#[test]
fn trino_query_join_tree_follows_the_grammar_right_operand() {
    let parsed = query("SELECT * FROM a JOIN b JOIN c ON b.k = c.k ON a.k = b.k");
    let Relation::Join(join) = select(&parsed).from.as_ref().unwrap() else {
        panic!()
    };
    assert!(matches!(join.left.as_ref(), Relation::Table(_)));
    assert!(matches!(join.right.as_ref(), Relation::Join(_)));
    let parsed = query("SELECT * FROM a LEFT JOIN b USING (k), c NATURAL JOIN d");
    let Relation::Join(implicit) = select(&parsed).from.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(implicit.kind, JoinType::Implicit);
    let Relation::Join(left) = implicit.left.as_ref() else {
        panic!()
    };
    assert_eq!(left.kind, JoinType::Left);
    assert!(
        matches!(&left.criteria, Some(JoinCriteria::Using(keys)) if keys.columns[0].value=="k")
    );
    let Relation::Join(right) = implicit.right.as_ref() else {
        panic!()
    };
    assert!(matches!(right.criteria, Some(JoinCriteria::Natural(_))));
}

#[test]
fn trino_query_relations_keep_period_alias_and_subquery_owners() {
    let parsed = query("SELECT * FROM t FOR VERSION AS OF ? AS v(x)");
    let Relation::Aliased(alias) = select(&parsed).from.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(alias.alias.value, "v");
    assert_eq!(alias.column_names.as_ref().unwrap()[0].value, "x");
    let Relation::Table(table) = alias.relation.as_ref() else {
        panic!()
    };
    assert!(table.branch.is_none());
    let period = table.query_period.as_ref().unwrap();
    assert_eq!(period.range_type, QueryPeriodRangeType::Version);
    assert!(period.start.is_none());
    assert_eq!(parameter(period.end.as_ref().unwrap()), 0);
    assert!(trino_parser::parse_query("SELECT * FROM t@branch").is_err());
    let parsed = query("SELECT * FROM ((SELECT 1) ORDER BY 1 LIMIT 2) AS q(x)");
    let Relation::Aliased(alias) = select(&parsed).from.as_ref().unwrap() else {
        panic!()
    };
    let Relation::TableSubquery(table) = alias.relation.as_ref() else {
        panic!()
    };
    assert!(table.query.modifiers.order_by.is_some());
    assert!(matches!(table.query.body.as_ref(), SetExpr::Query(_)));
}

#[test]
fn trino_query_unnest_lateral_and_sampling_are_real_core_relations() {
    let parsed = query("SELECT * FROM UNNEST(ARRAY[1], ARRAY[2]) WITH ORDINALITY AS u(a,b)");
    let Relation::Aliased(alias) = select(&parsed).from.as_ref().unwrap() else {
        panic!()
    };
    let Relation::Unnest(unnest) = alias.relation.as_ref() else {
        panic!()
    };
    assert_eq!(unnest.expressions.len(), 2);
    assert!(unnest.with_ordinality);
    let parsed = query("SELECT * FROM LATERAL (SELECT ?) AS s(x) CROSS JOIN t");
    let Relation::Join(join) = select(&parsed).from.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(join.kind, JoinType::Cross);
    let Relation::Aliased(alias) = join.left.as_ref() else {
        panic!()
    };
    assert!(matches!(alias.relation.as_ref(), Relation::Lateral(_)));
    let parsed = query("SELECT * FROM t TABLESAMPLE BERNOULLI (10)");
    assert!(
        matches!(select(&parsed).from.as_ref(), Some(Relation::Sampled(sample)) if sample.kind==SamplingType::Bernoulli)
    );
}

#[test]
fn trino_query_windows_keep_defaults_bounds_and_parameter_visit_order() {
    let parsed = query(
        "SELECT sum(?) OVER (PARTITION BY ? ORDER BY ? DESC NULLS LAST ROWS BETWEEN ? PRECEDING AND CURRENT ROW EXCLUDE TIES)",
    );
    let Expr::FunctionCall(call) = expression(&select(&parsed).projection[0]) else {
        panic!()
    };
    assert_eq!(parameter(&call.arguments[0].value), 3);
    let Window::Specification(window) = call.window.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(parameter(&window.partition_by[0]), 0);
    assert_eq!(
        parameter(&window.order_by.as_ref().unwrap().items[0].sort_key),
        1
    );
    let frame = window.window_frame.as_ref().unwrap();
    assert_eq!(frame.units, WindowFrameUnits::Rows);
    assert_eq!(frame.exclusion, WindowFrameExclusion::Ties);
    let WindowFrameBound::Preceding(Some(value), _) = &frame.start_bound else {
        panic!()
    };
    assert_eq!(parameter(value), 2);
    assert!(matches!(
        frame.end_bound,
        Some(WindowFrameBound::CurrentRow(_))
    ));
    let parsed = query("SELECT sum(1) OVER w WINDOW w AS (ORDER BY 1 RANGE CURRENT ROW)");
    assert_eq!(select(&parsed).windows.len(), 1);
    assert!(
        select(&parsed).windows[0]
            .specification
            .existing_window_name
            .is_none()
    );
}

#[test]
fn trino_query_keeps_root_prefixes_out_of_nested_query_grammar() {
    for name in ["FUNCTION", "SESSION"] {
        let parsed = query(&format!("WITH {name} AS (SELECT 1) SELECT 1"));
        assert_eq!(parsed.with.as_ref().unwrap().ctes[0].name.value, name);
    }
    for sql in [
        "SELECT 1;",
        "SELECT 1 SELECT 2",
        "SELECT 1 OFFSET -1",
        "SELECT 1 LIMIT ALL OFFSET 1",
        "SELECT 1 FETCH FIRST 1 PERCENT ROWS ONLY",
        "(WITH c AS (SELECT 1) SELECT 1)",
        "SELECT * FROM (WITH SESSION x = 1 SELECT 1) q",
    ] {
        assert!(trino_parser::parse_query(sql).is_err(), "{sql}");
    }
}

#[test]
fn trino_query_first_slice_roundtrips_preserve_all_syntax_facts() {
    for sql in [
        "SELECT 1",
        "WITH c(x) AS (VALUES ROW(1)) SELECT x FROM c ORDER BY x LIMIT ALL",
        "WITH SESSION x = ? WITH c AS (SELECT ?) SELECT ?",
        "SELECT (SELECT ?), ? OFFSET ? ROWS FETCH NEXT ? ROWS WITH TIES",
        "VALUES 1, ROW(2), ROW(ROW(3)) ORDER BY 1 DESC NULLS FIRST",
        "(SELECT 1 UNION ALL SELECT 2) UNION ALL SELECT 3",
        "SELECT 1 UNION ALL CORRESPONDING BY (x) SELECT 2",
        "SELECT a, b FROM t GROUP BY (a,b), ROW(a,b), ()",
        "SELECT * FROM a LEFT JOIN b USING (k), c NATURAL JOIN d",
        "SELECT * FROM t FOR VERSION AS OF ? AS v(x)",
        "SELECT sum(1) OVER w WINDOW w AS (PARTITION BY x ORDER BY y ROWS UNBOUNDED PRECEDING)",
        "SELECT \"MiXeD\" AS \"out\" FROM \"T\"",
    ] {
        let first = query(sql);
        let rendered = trino_printer::print_query(&first);
        let second = query(&rendered);
        assert!(syntax_eq_query(&first, &second), "{sql}\n{rendered}");
    }
}

#[test]
fn relation_parentheses_distinguish_query_wrappers_from_an_inner_alias() {
    let aliased = query("SELECT * FROM ((SELECT 1) AS x)");
    let SetExpr::Select(select) = aliased.body.as_ref() else {
        panic!()
    };
    let Some(Relation::Aliased(alias)) = &select.from else {
        panic!()
    };
    assert_eq!(alias.alias.value, "x");
    assert!(
        matches!(alias.relation.as_ref(), Relation::TableSubquery(value) if matches!(value.query.body.as_ref(), SetExpr::Select(_)))
    );
    let nested = query("SELECT * FROM ((SELECT 1))");
    let SetExpr::Select(select) = nested.body.as_ref() else {
        panic!()
    };
    assert!(
        matches!(&select.from, Some(Relation::TableSubquery(value)) if matches!(value.query.body.as_ref(), SetExpr::Query(_)))
    );
    for original in [aliased, nested] {
        let printed = trino_printer::print_query(&original);
        assert!(syntax_eq_query(&original, &query(&printed)), "{printed}");
    }
}
