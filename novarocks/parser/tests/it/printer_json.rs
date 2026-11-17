// SPDX-License-Identifier: Apache-2.0
use novarocks_parser::ast::*;
use novarocks_parser::trino_parser::{parse_expression, parse_query};
use novarocks_parser::trino_printer::{print_expression, print_query};

fn expression_roundtrip(source: &str) -> Expr {
    let original = parse_expression(source).unwrap();
    let printed = print_expression(&original).unwrap();
    let reparsed = parse_expression(&printed)
        .unwrap_or_else(|error| panic!("source: {source}\nprinted: {printed}\nerror: {error:?}"));
    assert!(
        original.syntax_eq(&reparsed),
        "source: {source}\nprinted: {printed}\noriginal: {original:?}\nreparsed: {reparsed:?}"
    );
    original
}

fn query_roundtrip(source: &str) -> Query {
    let original = parse_query(source).unwrap();
    let printed = print_query(&original).unwrap();
    let reparsed = parse_query(&printed)
        .unwrap_or_else(|error| panic!("source: {source}\nprinted: {printed}\nerror: {error:?}"));
    assert!(
        original.syntax_eq(&reparsed),
        "source: {source}\nprinted: {printed}\noriginal: {original:?}\nreparsed: {reparsed:?}"
    );
    original
}

fn table(query: &Query) -> &JsonTable {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("not SELECT")
    };
    let Some(Relation::JsonTable(table)) = &select.from else {
        panic!("not JSON_TABLE")
    };
    table
}

#[test]
fn every_existing_json_coverage_sample_roundtrips_through_the_typed_printer() {
    let rules = [
        "jsonTableColumn",
        "jsonTableSpecificPlan",
        "jsonTablePathName",
        "planPrimary",
        "jsonTableDefaultPlan",
        "jsonPathInvocation",
        "jsonValueExpression",
        "jsonRepresentation",
        "jsonArgument",
        "jsonExistsErrorBehavior",
        "jsonValueBehavior",
        "jsonQueryWrapperBehavior",
        "jsonQueryBehavior",
        "jsonSerializeOnErrorBehavior",
        "jsonObjectMember",
    ];
    let labels = [
        "jsonConstructor",
        "jsonExists",
        "jsonValue",
        "jsonQuery",
        "jsonSerialize",
        "jsonObject",
        "jsonArray",
        "jsonTable",
    ];
    let mut checked = 0;
    for line in include_str!("grammar-coverage.tsv")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let fields: Vec<_> = line.split('\t').collect();
        if !matches!(fields[0], "alternative" | "optional")
            || !(rules.contains(&fields[1]) || labels.contains(&fields[3]))
        {
            continue;
        }
        assert!(!fields[5].is_empty(), "unfilled JSON sample: {line}");
        query_roundtrip(&super::coverage::sample(fields[5]));
        checked += 1;
    }
    assert_eq!(checked, 104, "the bound JSON sample obligations changed");
}

#[test]
fn json_formats_quotes_and_defaults_keep_their_optional_facts() {
    let original = expression_roundtrip(
        r#"JSON_QUERY(x FORMAT JSON ENCODING UTF16, 'lax $."中''k"' AS "Path" PASSING a AS plain, b FORMAT JSON AS "encoded", c FORMAT JSON ENCODING UTF8 AS bytes RETURNING VARBINARY FORMAT JSON ENCODING UTF32 WITH CONDITIONAL ARRAY WRAPPER KEEP QUOTES ON SCALAR STRING EMPTY ARRAY ON EMPTY ERROR ON ERROR)"#,
    );
    let Expr::JsonQuery(value) = original else {
        panic!("not JSON_QUERY")
    };
    assert_eq!(value.path_invocation.input_format, JsonFormat::Utf16);
    assert_eq!(value.path_invocation.json_path.value, "lax $.\"中'k\"");
    assert!(value.path_invocation.path_name.as_ref().unwrap().quoted);
    let formats: Vec<_> = value
        .path_invocation
        .path_parameters
        .iter()
        .map(|parameter| parameter.format)
        .collect();
    assert_eq!(
        formats,
        [None, Some(JsonFormat::Json), Some(JsonFormat::Utf8)]
    );
    assert_eq!(value.output_format, Some(JsonFormat::Utf32));
    assert_eq!(value.quotes_behavior, Some(JsonQueryQuotesBehavior::Keep));
    assert_eq!(
        value.wrapper_behavior,
        JsonQueryWrapperBehavior::Conditional
    );
    assert_eq!(value.empty_behavior, JsonQueryBehavior::EmptyArray);
    assert_eq!(value.error_behavior, JsonQueryBehavior::Error);

    let Expr::JsonQuery(absent) = expression_roundtrip("JSON_QUERY(x, '$')") else {
        panic!("not JSON_QUERY")
    };
    assert!(absent.returned_type.is_none());
    assert!(absent.output_format.is_none());
    assert!(absent.quotes_behavior.is_none());
    let Expr::JsonQuery(explicit) = expression_roundtrip("JSON_QUERY(x, '$' KEEP QUOTES)") else {
        panic!("not JSON_QUERY")
    };
    assert_eq!(
        explicit.quotes_behavior,
        Some(JsonQueryQuotesBehavior::Keep)
    );
    assert!(!absent.syntax_eq(&explicit));

    let Expr::JsonValue(value) = expression_roundtrip(
        "JSON_VALUE(x, '$' RETURNING BIGINT DEFAULT CAST(NULL AS BIGINT) ON EMPTY DEFAULT NULL ON ERROR)",
    ) else {
        panic!("not JSON_VALUE")
    };
    assert_eq!(value.empty_behavior, JsonValueBehavior::Default);
    let Expr::Cast(default) = value.empty_default.as_ref().unwrap() else {
        panic!("not typed default")
    };
    assert!(matches!(
        &default.expression,
        Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        })
    ));
    assert_eq!(value.error_behavior, JsonValueBehavior::Default);
    assert!(matches!(
        value.error_default.as_ref().unwrap(),
        Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        })
    ));

    let Expr::JsonObject(object) = expression_roundtrip(
        "JSON_OBJECT('a': x, 'b': y FORMAT JSON ABSENT ON NULL WITH UNIQUE KEYS)",
    ) else {
        panic!("not JSON_OBJECT")
    };
    assert_eq!(object.members[0].format, None);
    assert_eq!(object.members[1].format, Some(JsonFormat::Json));
    assert!(!object.null_on_null);
    assert!(object.unique_keys);
    let Expr::JsonArray(array) = expression_roundtrip("JSON_ARRAY(x, y FORMAT JSON NULL ON NULL)")
    else {
        panic!("not JSON_ARRAY")
    };
    assert_eq!(array.elements[0].format, None);
    assert_eq!(array.elements[1].format, Some(JsonFormat::Json));
    assert!(array.null_on_null);
    expression_roundtrip("JSON_ARRAY(RETURNING)");
    expression_roundtrip("JSON_OBJECT(RETURNING VALUE 1)");
    expression_roundtrip("JSON_OBJECT(RETURNING VARCHAR FORMAT JSON)");
    expression_roundtrip("JSON_ARRAY(RETURNING VARBINARY FORMAT JSON ENCODING UTF8)");
    for source in [
        "\"JSON\"(x)",
        "\"JSON_QUERY\"(x, '$')",
        "JSON()",
        "JSON(x, y)",
    ] {
        assert!(matches!(
            expression_roundtrip(source),
            Expr::FunctionCall(_)
        ));
    }
}

#[test]
fn json_table_preserves_inherited_errors_typed_defaults_and_nested_plan_boundaries() {
    let original = query_roundtrip(
        "SELECT * FROM JSON_TABLE(x, '$' AS root COLUMNS (ord FOR ORDINALITY, a BIGINT, b BIGINT NULL ON ERROR, c VARCHAR FORMAT JSON, d VARCHAR FORMAT JSON NULL ON ERROR, NESTED '$.a' AS a COLUMNS (v BIGINT DEFAULT CAST(NULL AS BIGINT) ON EMPTY DEFAULT NULL ON ERROR), NESTED '$.b' AS b COLUMNS (v INTEGER), NESTED '$.c' AS c COLUMNS (v INTEGER)) PLAN (root OUTER (a UNION (b UNION c))))",
    );
    let value = table(&original);
    assert!(value.error_behavior.is_none());
    let JsonTableColumn::Value(inherited) = &value.columns[1] else {
        panic!("not value column")
    };
    assert!(inherited.error_behavior.is_none());
    let JsonTableColumn::Value(explicit) = &value.columns[2] else {
        panic!("not value column")
    };
    assert_eq!(explicit.error_behavior, Some(JsonValueBehavior::Null));
    let JsonTableColumn::Query(inherited) = &value.columns[3] else {
        panic!("not query column")
    };
    assert!(inherited.error_behavior.is_none());
    let JsonTableColumn::Query(explicit) = &value.columns[4] else {
        panic!("not query column")
    };
    assert_eq!(explicit.error_behavior, Some(JsonQueryBehavior::Null));
    let JsonTableColumn::Nested(nested) = &value.columns[5] else {
        panic!("not nested columns")
    };
    let JsonTableColumn::Value(default) = &nested.columns[0] else {
        panic!("not child value")
    };
    assert!(matches!(
        default.empty_default.as_ref().unwrap(),
        Expr::Cast(_)
    ));
    assert!(matches!(
        default.error_default.as_ref().unwrap(),
        Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        })
    ));
    let Some(JsonTablePlan::Specific(JsonTableSpecificPlan::ParentChild(parent))) = &value.plan
    else {
        panic!("not parent-child plan")
    };
    assert_eq!(parent.parent.name.value, "root");
    let JsonTableSpecificPlan::Siblings(children) = parent.child.as_ref() else {
        panic!("not first UNION")
    };
    assert_eq!(children.siblings.len(), 2);
    assert!(
        matches!(&children.siblings[1], JsonTableSpecificPlan::Siblings(nested) if nested.plan_type == JsonTableSiblingsPlanType::Union && nested.siblings.len() == 2)
    );
    for (source, parent, siblings) in [
        (
            "OUTER,UNION",
            JsonTableParentChildPlanType::Outer,
            JsonTableSiblingsPlanType::Union,
        ),
        (
            "OUTER,CROSS",
            JsonTableParentChildPlanType::Outer,
            JsonTableSiblingsPlanType::Cross,
        ),
        (
            "INNER,UNION",
            JsonTableParentChildPlanType::Inner,
            JsonTableSiblingsPlanType::Union,
        ),
        (
            "INNER,CROSS",
            JsonTableParentChildPlanType::Inner,
            JsonTableSiblingsPlanType::Cross,
        ),
    ] {
        let query = query_roundtrip(&format!(
            "SELECT * FROM JSON_TABLE(x, '$' COLUMNS (v INTEGER) PLAN DEFAULT ({source}) EMPTY ON ERROR)"
        ));
        let value = table(&query);
        assert_eq!(value.error_behavior, Some(JsonTableErrorBehavior::Empty));
        let Some(JsonTablePlan::Default(plan)) = &value.plan else {
            panic!("not default plan")
        };
        assert_eq!(plan.parent_child, parent);
        assert_eq!(plan.siblings, siblings);
    }
}

#[test]
fn empty_json_constructors_refuse_unrepresentable_member_list_policies() {
    let mut object = parse_expression("JSON_OBJECT()").unwrap();
    let Expr::JsonObject(value) = &mut object else {
        panic!("not JSON_OBJECT")
    };
    value.unique_keys = true;
    assert!(matches!(
        print_expression(&object),
        Err(novarocks_parser::trino_printer::PrintError::Unrepresentable { .. })
    ));
    let Expr::JsonObject(value) = &mut object else {
        unreachable!()
    };
    value.unique_keys = false;
    value.null_on_null = false;
    assert!(matches!(
        print_expression(&object),
        Err(novarocks_parser::trino_printer::PrintError::Unrepresentable { .. })
    ));
    let mut array = parse_expression("JSON_ARRAY()").unwrap();
    let Expr::JsonArray(value) = &mut array else {
        panic!("not JSON_ARRAY")
    };
    value.null_on_null = true;
    assert!(matches!(
        print_expression(&array),
        Err(novarocks_parser::trino_printer::PrintError::Unrepresentable { .. })
    ));
}
