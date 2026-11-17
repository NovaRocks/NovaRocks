// SPDX-License-Identifier: Apache-2.0
use novarocks_parser::ast::*;
use novarocks_parser::{ParseError, ParserError, Span, trino_parser, trino_printer};

fn roundtrip(source: &str) -> Query {
    let original = trino_parser::parse_query(source)
        .unwrap_or_else(|error| panic!("source: {source}\nerror: {error:?}"));
    let printed = trino_printer::print_query(&original).unwrap();
    let reparsed = trino_parser::parse_query(&printed)
        .unwrap_or_else(|error| panic!("source: {source}\nprinted: {printed}\nerror: {error:?}"));
    assert!(
        original.syntax_eq(&reparsed),
        "source: {source}\nprinted: {printed}\noriginal: {original:?}\nreparsed: {reparsed:?}"
    );
    original
}

fn relation(query: &Query) -> &Relation {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("not SELECT")
    };
    select.from.as_ref().expect("FROM")
}
fn invocation(query: &Query) -> &TableFunctionInvocation {
    let Relation::TableFunction(value) = relation(query) else {
        panic!("not TVF")
    };
    value
}
fn table_argument(value: &TableFunctionArgument) -> &TableFunctionTableArgument {
    let TableFunctionArgumentValue::Table(value) = &value.value else {
        panic!("not table argument")
    };
    value
}
fn parameter(value: &Expr) -> usize {
    let Expr::Parameter(value) = value else {
        panic!("not parameter")
    };
    value.position
}

#[test]
fn advanced_relation_coverage_including_original_formatter_gaps_roundtrips() {
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
    let mut checked = 0;
    for line in include_str!("grammar-coverage.tsv").lines() {
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 10 || fields[7] != "accept" {
            continue;
        }
        if rules.contains(&fields[1])
            || fields[1] == "relationPrimary"
                && matches!(fields[3], "nearest" | "tableFunctionInvocation")
        {
            roundtrip(&super::coverage::sample(fields[5]));
            checked += 1;
        }
    }
    assert_eq!(
        checked, 78,
        "bound advanced relation sample obligations changed"
    );
}

#[test]
fn table_partition_and_sort_lists_keep_empty_and_single_row_keys_distinct() {
    let original = roundtrip(
        "SELECT * FROM TABLE(f(TABLE(t), TABLE(t) PARTITION BY (), TABLE(t) PARTITION BY (a,b) ORDER BY (a,b), TABLE(t) PARTITION BY (ROW(a,b)) ORDER BY ((a,b) DESC), TABLE(t) KEEP WHEN EMPTY))",
    );
    let call = invocation(&original);
    let absent = table_argument(&call.arguments[0]);
    assert!(absent.partition_by.is_none());
    assert!(absent.order_by.is_none());
    assert!(absent.empty_table_treatment.is_none());
    let empty = table_argument(&call.arguments[1]);
    assert!(empty.partition_by.as_ref().unwrap().is_empty());
    let two = table_argument(&call.arguments[2]);
    assert_eq!(two.partition_by.as_ref().unwrap().len(), 2);
    assert_eq!(two.order_by.as_ref().unwrap().items.len(), 2);
    assert!(matches!(
        &two.partition_by.as_ref().unwrap()[0],
        Expr::Identifier(_)
    ));
    let one_row = table_argument(&call.arguments[3]);
    assert_eq!(one_row.partition_by.as_ref().unwrap().len(), 1);
    assert!(
        matches!(&one_row.partition_by.as_ref().unwrap()[0], Expr::Row(value) if value.fields.len() == 2)
    );
    let order = one_row.order_by.as_ref().unwrap();
    assert_eq!(order.items.len(), 1);
    assert_eq!(order.items[0].ordering, SortOrdering::Descending);
    assert_eq!(order.items[0].null_ordering, NullOrdering::Undefined);
    assert!(matches!(&order.items[0].sort_key, Expr::Row(value) if value.fields.len() == 2));
    assert_eq!(
        table_argument(&call.arguments[4])
            .empty_table_treatment
            .as_ref()
            .unwrap()
            .treatment,
        EmptyTableTreatmentKind::Keep
    );
    let no_arguments = roundtrip("SELECT * FROM TABLE(f())");
    assert!(invocation(&no_arguments).arguments.is_empty());
    assert!(invocation(&no_arguments).copartitioning.is_empty());
}

#[test]
fn tvf_real_query_alias_descriptor_and_parameter_children_survive_rendering() {
    let source = "SELECT * FROM TABLE(\"cat\".f(input => TABLE(SELECT ?) AS \"q\"(\"x\") PARTITION BY (?) PRUNE WHEN EMPTY ORDER BY (? DESC NULLS FIRST), d => DESCRIPTOR(\"a\" BIGINT,b), n => CAST(NULL AS DESCRIPTOR), ? COPARTITION (\"q\",cat.t)))";
    let original = roundtrip(source);
    let call = invocation(&original);
    assert!(call.name.parts[0].quoted);
    assert_eq!(call.name.parts[0].value, "cat");
    assert_eq!(
        &source[call.span.start()..call.span.end()],
        &source[source.find("\"cat\".f(").unwrap()..source.len() - 1]
    );
    assert_eq!(call.arguments[0].name.as_ref().unwrap().value, "input");
    let table = table_argument(&call.arguments[0]);
    let Relation::Aliased(alias) = table.table.as_ref() else {
        panic!("not alias")
    };
    assert!(alias.alias.quoted);
    assert!(alias.column_names.as_ref().unwrap()[0].quoted);
    let Relation::TableSubquery(query) = alias.relation.as_ref() else {
        panic!("not real query")
    };
    let SetExpr::Select(select) = query.query.body.as_ref() else {
        panic!("not SELECT")
    };
    let SelectItem::SingleColumn(column) = &select.projection[0] else {
        panic!("not column")
    };
    assert_eq!(parameter(&column.expression), 0);
    assert_eq!(parameter(&table.partition_by.as_ref().unwrap()[0]), 1);
    let item = &table.order_by.as_ref().unwrap().items[0];
    assert_eq!(parameter(&item.sort_key), 2);
    assert_eq!(item.null_ordering, NullOrdering::First);
    assert_eq!(
        table.empty_table_treatment.as_ref().unwrap().treatment,
        EmptyTableTreatmentKind::Prune
    );
    let TableFunctionArgumentValue::Descriptor(value) = &call.arguments[1].value else {
        panic!("not descriptor")
    };
    let fields = &value.descriptor.as_ref().unwrap().fields;
    assert!(fields[0].name.quoted && fields[0].data_type.is_some());
    assert!(fields[1].data_type.is_none());
    let TableFunctionArgumentValue::Descriptor(value) = &call.arguments[2].value else {
        panic!("not null descriptor")
    };
    assert!(value.descriptor.is_none());
    let TableFunctionArgumentValue::Expression(value) = &call.arguments[3].value else {
        panic!("not expression")
    };
    assert_eq!(parameter(value), 3);
    assert_eq!(call.copartitioning.len(), 1);
    assert_eq!(call.copartitioning[0].len(), 2);
    assert!(call.copartitioning[0][0].parts[0].quoted);
    // Different grammar domains can have identical-looking inner expressions.
    // The printer must retain the real Expression domain without a fake node.
    let ordinary = roundtrip("SELECT * FROM TABLE(f((DESCRIPTOR(x)), (CAST(NULL AS DESCRIPTOR))))");
    assert!(
        invocation(&ordinary)
            .arguments
            .iter()
            .all(|argument| matches!(&argument.value, TableFunctionArgumentValue::Expression(_)))
    );
}

#[test]
fn pivot_value_group_cardinality_aliases_names_and_group_by_are_real_fields() {
    let source = "SELECT * FROM (t) PIVOT (? AS \"total\", ? AS mean FOR (\"t\".a,b) IN ((?,?) AS pair, ROW(?,?) AS one) GROUP BY ()) AS p(x,y)";
    let original = roundtrip(source);
    let Relation::Aliased(alias) = relation(&original) else {
        panic!("not alias")
    };
    assert_eq!(alias.column_names.as_ref().unwrap().len(), 2);
    let Relation::Pivot(pivot) = alias.relation.as_ref() else {
        panic!("not pivot")
    };
    assert_eq!(pivot.span.start(), source.find("(t)").unwrap());
    assert!(pivot.aggregations[0].alias.as_ref().unwrap().quoted);
    assert_eq!(parameter(&pivot.aggregations[0].expression), 0);
    assert_eq!(parameter(&pivot.aggregations[1].expression), 1);
    let Expr::Dereference(column) = &pivot.pivot_columns[0] else {
        panic!("not qualified column")
    };
    let Expr::Identifier(base) = &column.base else {
        panic!("not real base")
    };
    assert!(base.quoted);
    assert_eq!(base.value, "t");
    assert_eq!(pivot.value_groups[0].values.len(), 2);
    assert_eq!(parameter(&pivot.value_groups[0].values[0]), 2);
    assert_eq!(parameter(&pivot.value_groups[0].values[1]), 3);
    assert_eq!(pivot.value_groups[1].values.len(), 1);
    let Expr::Row(row) = &pivot.value_groups[1].values[0] else {
        panic!("not singleton Row")
    };
    assert_eq!(parameter(&row.fields[0].expression), 4);
    assert_eq!(parameter(&row.fields[1].expression), 5);
    let GroupingElement::Simple(simple) = &pivot.group_by.as_ref().unwrap().elements[0] else {
        panic!("not grouping element")
    };
    assert!(simple.columns.is_empty());
    let absent = roundtrip("SELECT * FROM t PIVOT (sum(a) FOR b IN ((1,2)+3 AS value))");
    let Relation::Pivot(pivot) = relation(&absent) else {
        panic!("not pivot")
    };
    assert!(pivot.group_by.is_none());
    assert_eq!(pivot.value_groups[0].values.len(), 1);
    assert!(matches!(&pivot.value_groups[0].values[0], Expr::Binary(_)));
}

#[test]
fn nearest_uses_its_actual_where_match_and_nested_relation_boundaries() {
    let original = roundtrip("SELECT * FROM NEAREST(FROM (SELECT ?) AS q(x) WHERE ? MATCH ?) AS n");
    let Relation::Aliased(alias) = relation(&original) else {
        panic!("not alias")
    };
    let Relation::Nearest(nearest) = alias.relation.as_ref() else {
        panic!("not NEAREST")
    };
    assert_eq!(parameter(nearest.where_clause.as_ref().unwrap()), 1);
    assert_eq!(parameter(&nearest.match_expression), 2);
    let absent = roundtrip("SELECT * FROM NEAREST(FROM t MATCH TRUE)");
    let Relation::Nearest(nearest) = relation(&absent) else {
        panic!("not NEAREST")
    };
    assert!(nearest.where_clause.is_none());
    for source in [
        "SELECT * FROM NEAREST(FROM t match WHERE a MATCH (SELECT b FROM u) MATCH TRUE)",
        "SELECT * FROM NEAREST(FROM (t match) WHERE TRUE MATCH (SELECT match FROM u))",
        "SELECT * FROM NEAREST(FROM NEAREST(FROM t MATCH TRUE) MATCH TRUE)",
        "SELECT * FROM NEAREST(FROM t MATCH a MATCH (SELECT b FROM u))",
    ] {
        roundtrip(source);
    }
}

#[test]
fn copartition_coverage_witnesses_reach_real_clause_fields_and_keep_alias_diagnostics() {
    // This guard requires the separate three-row coverage amendment. The old
    // no-argument text is kept in the input ledger, not assigned a fake oracle.
    let mut checked = 0;
    for line in include_str!("grammar-coverage.tsv").lines() {
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 10 || fields[1] != "copartitionTables" {
            continue;
        }
        let query = roundtrip(&super::coverage::sample(fields[5]));
        let call = invocation(&query);
        assert_eq!(call.arguments.len(), 1);
        assert!(matches!(
            &call.arguments[0].value,
            TableFunctionArgumentValue::Expression(Expr::Literal(Literal {
                kind: LiteralKind::Long {
                    parsed_value: 1,
                    ..
                },
                ..
            }))
        ));
        assert_eq!(call.copartitioning.len(), 1);
        assert_eq!(
            call.copartitioning[0].len(),
            if fields[0] == "alternative" && fields[4] == "0" {
                2
            } else {
                3
            }
        );
        checked += 1;
    }
    assert_eq!(checked, 3);
    let source = "SELECT * FROM TABLE(f(TABLE(t) copartition(a,b)))";
    let error = trino_parser::parse_query(source).unwrap_err();
    let ParserError::Parse(ParseError::UnexpectedToken { span, found, .. }) = error else {
        panic!("not the original typed diagnostic")
    };
    let start = source.find("copartition").unwrap();
    assert_eq!(span, Span::new(start, start + "copartition".len()));
    assert_eq!(found, "copartition");
    for source in [
        "SELECT * FROM TABLE(f(TABLE(t) AS copartition(a,b)))",
        "SELECT * FROM TABLE(f(TABLE(t) \"COPARTITION\"(a,b)))",
    ] {
        roundtrip(source);
    }
}
