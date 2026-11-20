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
use novarocks_parser::parser::parse_query;
use novarocks_parser::{parser, printer};

fn query(source: &str) -> Query {
    parser::parse_query(source).expect(source)
}

fn relation(query: &Query) -> &Relation {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected SELECT")
    };
    select.from.as_ref().expect("FROM")
}

fn invocation(query: &Query) -> &TableFunctionInvocation {
    let Relation::TableFunction(invocation) = relation(query) else {
        panic!("expected TVF")
    };
    invocation
}

fn parameter(expression: &Expr) -> usize {
    let Expr::Parameter(parameter) = expression else {
        panic!("expected parameter")
    };
    parameter.position
}

fn table_argument(argument: &TableFunctionArgument) -> &TableFunctionTableArgument {
    let TableFunctionArgumentValue::Table(table) = &argument.value else {
        panic!("expected table argument")
    };
    table
}

#[test]
fn trino_table_function_all_argument_domains_and_real_source_ranges() {
    let source = "SELECT * FROM TABLE(cat.f(input => TABLE(orders) AS ord(a,b) PARTITION BY (a,b) PRUNE WHEN EMPTY ORDER BY (a DESC NULLS LAST,b), d => DESCRIPTOR(x BIGINT,y), n => CAST(NULL AS DESCRIPTOR), amount => ? COPARTITION (ord,nation),(cat.a,cat.b,cat.c)))";
    let parsed = query(source);
    let call = invocation(&parsed);
    assert_eq!(
        call.name
            .parts
            .iter()
            .map(|part| part.value.as_str())
            .collect::<Vec<_>>(),
        ["cat", "f"]
    );
    assert_eq!(
        &source[call.span.start()..call.span.end()],
        &source[source.find("cat.f(").unwrap()..source.len() - 1]
    );
    assert_eq!(call.arguments.len(), 4);
    assert_eq!(call.arguments[0].name.as_ref().unwrap().value, "input");
    let table = table_argument(&call.arguments[0]);
    let Relation::Aliased(alias) = table.table.as_ref() else {
        panic!()
    };
    assert_eq!(alias.alias.value, "ord");
    assert_eq!(alias.column_names.as_ref().unwrap().len(), 2);
    let Relation::Table(base) = alias.relation.as_ref() else {
        panic!()
    };
    assert_eq!(base.name.parts[0].value, "orders");
    assert_eq!(
        &source[base.name.parts[0].span.start()..base.name.parts[0].span.end()],
        "orders"
    );
    assert!(base.query_period.is_none() && base.branch.is_none());
    assert_eq!(table.partition_by.as_ref().unwrap().len(), 2);
    let order = table.order_by.as_ref().unwrap();
    assert_eq!(order.items.len(), 2);
    assert_eq!(order.items[0].ordering, SortOrdering::Descending);
    assert_eq!(order.items[0].null_ordering, NullOrdering::Last);
    assert_eq!(order.items[1].ordering, SortOrdering::Ascending);
    assert_eq!(order.items[1].null_ordering, NullOrdering::Undefined);
    assert_eq!(
        table.empty_table_treatment.as_ref().unwrap().treatment,
        EmptyTableTreatmentKind::Prune
    );
    let TableFunctionArgumentValue::Descriptor(descriptor) = &call.arguments[1].value else {
        panic!()
    };
    let descriptor = descriptor.descriptor.as_ref().unwrap();
    assert_eq!(descriptor.fields.len(), 2);
    assert!(descriptor.fields[0].data_type.is_some());
    assert!(descriptor.fields[1].data_type.is_none());
    let TableFunctionArgumentValue::Descriptor(descriptor) = &call.arguments[2].value else {
        panic!()
    };
    assert!(descriptor.descriptor.is_none());
    let TableFunctionArgumentValue::Expression(expression) = &call.arguments[3].value else {
        panic!()
    };
    assert_eq!(parameter(expression), 0);
    assert_eq!(
        call.copartitioning.iter().map(Vec::len).collect::<Vec<_>>(),
        [2, 3]
    );
}

#[test]
fn trino_table_argument_absence_and_explicit_empty_are_distinct() {
    let parsed = query(
        "SELECT * FROM TABLE(f(TABLE(t), TABLE(t) PARTITION BY (), TABLE(t) KEEP WHEN EMPTY, TABLE(t) PARTITION BY (ROW(1,2))))",
    );
    let call = invocation(&parsed);
    assert!(table_argument(&call.arguments[0]).partition_by.is_none());
    assert!(
        table_argument(&call.arguments[1])
            .partition_by
            .as_ref()
            .unwrap()
            .is_empty()
    );
    let third = table_argument(&call.arguments[2]);
    assert!(third.partition_by.is_none());
    assert_eq!(
        third.empty_table_treatment.as_ref().unwrap().treatment,
        EmptyTableTreatmentKind::Keep
    );
    let fourth = table_argument(&call.arguments[3]);
    assert_eq!(fourth.partition_by.as_ref().unwrap().len(), 1);
    assert!(matches!(
        fourth.partition_by.as_ref().unwrap()[0],
        Expr::Row(_)
    ));
    let empty = query("SELECT * FROM TABLE(f())");
    assert!(invocation(&empty).arguments.is_empty());
    assert!(invocation(&empty).copartitioning.is_empty());
}

#[test]
fn trino_table_query_alias_and_parameters_follow_builder_children() {
    let parsed = query(
        "SELECT * FROM TABLE(f(input => TABLE(SELECT ?) AS q(x) PARTITION BY (?) ORDER BY (? DESC NULLS FIRST), ?))",
    );
    let call = invocation(&parsed);
    let table = table_argument(&call.arguments[0]);
    let Relation::Aliased(alias) = table.table.as_ref() else {
        panic!()
    };
    let Relation::TableSubquery(subquery) = alias.relation.as_ref() else {
        panic!()
    };
    let SetExpr::Select(select) = subquery.query.body.as_ref() else {
        panic!()
    };
    let SelectItem::SingleColumn(column) = &select.projection[0] else {
        panic!()
    };
    assert_eq!(parameter(&column.expression), 0);
    assert_eq!(parameter(&table.partition_by.as_ref().unwrap()[0]), 1);
    assert_eq!(
        parameter(&table.order_by.as_ref().unwrap().items[0].sort_key),
        2
    );
    let TableFunctionArgumentValue::Expression(expression) = &call.arguments[1].value else {
        panic!()
    };
    assert_eq!(parameter(expression), 3);
}

#[test]
fn trino_table_argument_parenthesized_list_and_expression_alternatives() {
    let parsed = query(
        "SELECT * FROM TABLE(f(TABLE(t) PARTITION BY (a,b) ORDER BY (a,b), TABLE(t) PARTITION BY (a+b) * 2 ORDER BY (a,b) DESC))",
    );
    let call = invocation(&parsed);
    let first = table_argument(&call.arguments[0]);
    assert_eq!(first.partition_by.as_ref().unwrap().len(), 2);
    assert_eq!(first.order_by.as_ref().unwrap().items.len(), 2);
    let second = table_argument(&call.arguments[1]);
    assert_eq!(second.partition_by.as_ref().unwrap().len(), 1);
    assert!(matches!(
        second.partition_by.as_ref().unwrap()[0],
        Expr::Binary(_)
    ));
    let order = second.order_by.as_ref().unwrap();
    assert_eq!(order.items.len(), 1);
    assert_eq!(order.items[0].ordering, SortOrdering::Descending);
    assert!(matches!(order.items[0].sort_key, Expr::Row(_)));
}

#[test]
fn trino_table_argument_copartition_alias_constraints() {
    for source in [
        "SELECT * FROM TABLE(f(TABLE(t) AS copartition(a,b)))",
        "SELECT * FROM TABLE(f(TABLE(t) \"COPARTITION\"(a,b)))",
        "SELECT * FROM TABLE(f(TABLE(SELECT * FROM t copartition(a,b))))",
        "SELECT * FROM TABLE(f(TABLE(t) PARTITION BY a COPARTITION(t,u)))",
    ] {
        query(source);
    }
    assert!(parser::parse_query("SELECT * FROM TABLE(f(TABLE(t) copartition(a,b)))").is_err());
}

#[test]
fn trino_pivot_real_columns_value_groups_alias_and_source_extent() {
    let source = "SELECT * FROM (t) PIVOT (? AS total, ? mean FOR (\"t\".a,b) IN ((?,?) AS pair, ROW(?,?) AS one) GROUP BY ()) AS p(x,y) TABLESAMPLE SYSTEM(5)";
    let parsed = query(source);
    let Relation::Sampled(sample) = relation(&parsed) else {
        panic!()
    };
    let Relation::Aliased(alias) = sample.relation.as_ref() else {
        panic!()
    };
    assert_eq!(alias.alias.value, "p");
    assert_eq!(alias.column_names.as_ref().unwrap().len(), 2);
    let Relation::Pivot(pivot) = alias.relation.as_ref() else {
        panic!()
    };
    assert_eq!(pivot.span.start(), source.find("(t)").unwrap());
    assert_eq!(
        &source[pivot.span.start()..pivot.span.end()],
        &source[source.find("(t)").unwrap()..source.rfind(" AS p(").unwrap()]
    );
    assert_eq!(pivot.aggregations.len(), 2);
    assert_eq!(parameter(&pivot.aggregations[0].expression), 0);
    assert_eq!(parameter(&pivot.aggregations[1].expression), 1);
    let Expr::Dereference(column) = &pivot.pivot_columns[0] else {
        panic!()
    };
    let Expr::Identifier(base) = &column.base else {
        panic!()
    };
    assert!(base.quoted);
    assert_eq!(base.value, "t");
    assert_eq!(&source[base.span.start()..base.span.end()], "\"t\"");
    assert_eq!(column.field.as_ref().unwrap().value, "a");
    assert_eq!(pivot.value_groups[0].values.len(), 2);
    assert_eq!(parameter(&pivot.value_groups[0].values[0]), 2);
    assert_eq!(parameter(&pivot.value_groups[0].values[1]), 3);
    assert_eq!(pivot.value_groups[1].values.len(), 1);
    let Expr::Row(row) = &pivot.value_groups[1].values[0] else {
        panic!()
    };
    assert_eq!(parameter(&row.fields[0].expression), 4);
    assert_eq!(parameter(&row.fields[1].expression), 5);
    assert_eq!(pivot.value_groups[1].alias.as_ref().unwrap().value, "one");
    let GroupingElement::Simple(simple) = &pivot.group_by.as_ref().unwrap().elements[0] else {
        panic!()
    };
    assert!(simple.columns.is_empty());
}

#[test]
fn trino_pivot_grouped_expression_is_not_a_tuple_value_group() {
    let parsed = query("SELECT * FROM t PIVOT (sum(a) FOR b IN ((1,2) + 3 AS value))");
    let Relation::Pivot(pivot) = relation(&parsed) else {
        panic!()
    };
    assert_eq!(pivot.value_groups[0].values.len(), 1);
    assert!(matches!(pivot.value_groups[0].values[0], Expr::Binary(_)));
    assert!(pivot.group_by.is_none());
}

#[test]
fn trino_nearest_real_relation_where_match_parameters_and_aliases() {
    let source = "SELECT * FROM NEAREST(FROM (SELECT ?) AS q(x) WHERE ? MATCH ?) n";
    let parsed = query(source);
    let Relation::Aliased(alias) = relation(&parsed) else {
        panic!()
    };
    assert_eq!(alias.alias.value, "n");
    let Relation::Nearest(nearest) = alias.relation.as_ref() else {
        panic!()
    };
    let Relation::Aliased(input) = nearest.relation.as_ref() else {
        panic!()
    };
    let Relation::TableSubquery(subquery) = input.relation.as_ref() else {
        panic!()
    };
    let SetExpr::Select(select) = subquery.query.body.as_ref() else {
        panic!()
    };
    let SelectItem::SingleColumn(column) = &select.projection[0] else {
        panic!()
    };
    assert_eq!(parameter(&column.expression), 0);
    assert_eq!(parameter(nearest.where_clause.as_ref().unwrap()), 1);
    assert_eq!(parameter(&nearest.match_expression), 2);
    assert_eq!(
        &source[nearest.span.start()..nearest.span.end()],
        "NEAREST(FROM (SELECT ?) AS q(x) WHERE ? MATCH ?)"
    );
    let no_where = query("SELECT * FROM NEAREST(FROM t MATCH TRUE)");
    let Relation::Nearest(nearest) = relation(&no_where) else {
        panic!()
    };
    assert!(nearest.where_clause.is_none());
    query("SELECT * FROM NEAREST(FROM t match WHERE a MATCH (SELECT b FROM u) MATCH TRUE)");
    query("SELECT * FROM NEAREST(FROM (t match) WHERE TRUE MATCH (SELECT match FROM u))");
    query("SELECT * FROM NEAREST(FROM NEAREST(FROM t MATCH TRUE) MATCH TRUE)");
    query("SELECT * FROM NEAREST(FROM t MATCH a MATCH (SELECT b FROM u))");
    let ordinary_alias = query("SELECT * FROM t match");
    let Relation::Aliased(alias) = relation(&ordinary_alias) else {
        panic!()
    };
    assert_eq!(alias.alias.value, "match");
}

#[test]
fn trino_advanced_relation_rejects_grammar_failures_without_value_defaults() {
    for source in [
        "SELECT * FROM TABLE(f(TABLE()))",
        "SELECT * FROM TABLE(f(TABLE(t) ORDER BY ()))",
        "SELECT * FROM TABLE(f(1 COPARTITION(t)))",
        "SELECT * FROM t PIVOT (FOR a IN (1))",
        "SELECT * FROM t PIVOT (sum(a) FOR () IN (1))",
        "SELECT * FROM t PIVOT (sum(a) FOR b IN ())",
        "SELECT * FROM NEAREST(FROM t)",
        "SELECT * FROM NEAREST(FROM t WHERE TRUE)",
    ] {
        assert!(parser::parse_query(source).is_err(), "{source}");
    }
}

#[test]
fn trino_advanced_relation_roundtrips_include_all_table_argument_list_shapes() {
    for source in [
        "SELECT * FROM TABLE(f())",
        "SELECT * FROM TABLE(f(TABLE(t) PARTITION BY (a,b)))",
        "SELECT * FROM TABLE(f(TABLE(t) PARTITION BY ()))",
        "SELECT * FROM TABLE(f(TABLE(t) ORDER BY (a,b)))",
        "SELECT * FROM TABLE(f(TABLE(VALUES ROW(?)) AS t(x) KEEP WHEN EMPTY ORDER BY (? DESC)))",
        "SELECT * FROM TABLE(f(DESCRIPTOR(x BIGINT,y),CAST(NULL AS DESCRIPTOR),1 COPARTITION(t,u)))",
        "SELECT * FROM t PIVOT (sum(x) AS total FOR (a,b) IN ((1,2) pair,ROW(3,4) single) GROUP BY ()) p",
        "SELECT * FROM NEAREST(FROM t WHERE a=b MATCH ts<=?)",
    ] {
        let parsed = query(source);
        let printed = printer::print_query(&parsed).unwrap();
        let reparsed = query(&printed);
        assert!(syntax_eq_query(&parsed, &reparsed), "{source}\n{printed}");
    }
}

#[test]
fn trino_advanced_relation_reference_coverage_acceptance() {
    let rules = [
        "pivot",
        "pivotAggregation",
        "pivotColumns",
        "pivotValueGroup",
        "tableFunctionCall",
        "tableFunctionArgument",
        "tableArgument",
        "tableArgumentRelation",
        "descriptorArgument",
        "descriptorField",
        "copartitionTables",
    ];
    let mut count = 0;
    for line in include_str!("grammar-coverage.tsv").lines() {
        let columns = line.split('\t').collect::<Vec<_>>();
        if columns.len() != 10 || columns[7] != "accept" {
            continue;
        }
        if rules.contains(&columns[1])
            || (columns[1] == "relationPrimary"
                && matches!(columns[3], "nearest" | "tableFunctionInvocation"))
        {
            query(&super::coverage::sample(columns[5]));
            count += 1;
        }
    }
    assert!(
        count >= 60,
        "advanced relation coverage inputs must remain present"
    );
}

#[test]
fn descriptor_argument_selection_requires_the_complete_grammar_alternative() {
    for source in [
        "SELECT * FROM TABLE(f(DESCRIPTOR()))",
        "SELECT * FROM TABLE(f(DESCRIPTOR(1)))",
        "SELECT * FROM TABLE(f(named => DESCRIPTOR(x) + 1))",
        "SELECT * FROM TABLE(f(DESCRIPTOR(x) FILTER (WHERE TRUE)))",
        "SELECT * FROM TABLE(f(CAST(NULL AS DESCRIPTOR) IS NULL))",
    ] {
        let parsed = parse_query(source).unwrap();
        let SetExpr::Select(select) = parsed.body.as_ref() else {
            panic!()
        };
        let Some(Relation::TableFunction(invocation)) = &select.from else {
            panic!()
        };
        assert!(
            matches!(
                invocation.arguments[0].value,
                TableFunctionArgumentValue::Expression(_)
            ),
            "{source}"
        );
        let printed = novarocks_parser::printer::print_query(&parsed).unwrap();
        let reparsed = parse_query(&printed).unwrap();
        assert!(syntax_eq_query(&parsed, &reparsed), "{source} -> {printed}");
    }
    for source in [
        "SELECT * FROM TABLE(f(DESCRIPTOR(x)))",
        "SELECT * FROM TABLE(f(CAST(NULL AS DESCRIPTOR)))",
    ] {
        let parsed = parse_query(source).unwrap();
        let SetExpr::Select(select) = parsed.body.as_ref() else {
            panic!()
        };
        let Some(Relation::TableFunction(invocation)) = &select.from else {
            panic!()
        };
        assert!(
            matches!(
                invocation.arguments[0].value,
                TableFunctionArgumentValue::Descriptor(_)
            ),
            "{source}"
        );
    }
}

#[test]
fn pivot_aliased_expression_keeps_a_bare_interval_field_as_the_real_alias() {
    let parsed = parse_query("SELECT * FROM t PIVOT ((a - b) DAY FOR c IN ((a - b) DAY))").unwrap();
    let SetExpr::Select(select) = parsed.body.as_ref() else {
        panic!()
    };
    let Some(Relation::Pivot(pivot)) = &select.from else {
        panic!()
    };
    assert_eq!(pivot.aggregations[0].alias.as_ref().unwrap().value, "DAY");
    assert!(matches!(pivot.aggregations[0].expression, Expr::Binary(_)));
    assert_eq!(pivot.value_groups[0].alias.as_ref().unwrap().value, "DAY");
    assert!(matches!(pivot.value_groups[0].values[0], Expr::Binary(_)));
    let printed = novarocks_parser::printer::print_query(&parsed).unwrap();
    assert!(syntax_eq_query(&parsed, &parse_query(&printed).unwrap()));
}
