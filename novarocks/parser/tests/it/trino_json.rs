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

use novarocks_parser::ast::*;
use novarocks_parser::parser::{parse_expression, parse_query};
use novarocks_parser::{ParseError, ParserError, Span};

fn parameter(expression: &Expr) -> &Parameter {
    let Expr::Parameter(value) = expression else {
        panic!("not parameter: {expression:?}")
    };
    value
}
fn object(expression: &Expr) -> &JsonObject {
    let Expr::JsonObject(value) = expression else {
        panic!("not object: {expression:?}")
    };
    value
}
fn array(expression: &Expr) -> &JsonArray {
    let Expr::JsonArray(value) = expression else {
        panic!("not array: {expression:?}")
    };
    value
}
fn query(expression: &Expr) -> &JsonQuery {
    let Expr::JsonQuery(value) = expression else {
        panic!("not JSON query: {expression:?}")
    };
    value
}
fn generic(data_type: &TypeName) -> &GenericDataType {
    let TypeKind::Generic(value) = &data_type.kind else {
        panic!("not generic: {data_type:?}")
    };
    value
}
fn table(source: &str) -> JsonTable {
    let query = parse_query(&format!("SELECT * FROM {source}")).unwrap();
    let SetExpr::Select(mut select) = query.body.into_inner() else {
        panic!("not select")
    };
    let Some(Relation::JsonTable(table)) = select.from.take() else {
        panic!("not JSON_TABLE")
    };
    table.into_inner()
}

#[test]
fn every_json_expression_root_has_the_exact_default_profile() {
    let expression = parse_expression("JSON(?)").unwrap();
    let Expr::JsonConstructor(value) = expression else {
        panic!("not constructor")
    };
    assert_eq!(value.format, JsonFormat::Json);
    assert_eq!(parameter(&value.expression).position, 0);
    let expression = parse_expression("JSON_EXISTS(?, '$')").unwrap();
    let Expr::JsonExists(value) = expression else {
        panic!("not exists")
    };
    assert_eq!(value.error_behavior, JsonExistsErrorBehavior::False);
    let expression = parse_expression("JSON_VALUE(?, '$')").unwrap();
    let Expr::JsonValue(value) = expression else {
        panic!("not value")
    };
    assert!(value.returned_type.is_none());
    assert_eq!(value.empty_behavior, JsonValueBehavior::Null);
    assert_eq!(value.error_behavior, JsonValueBehavior::Null);
    assert!(value.empty_default.is_none() && value.error_default.is_none());
    let expression = parse_expression("JSON_QUERY(?, '$')").unwrap();
    let value = query(&expression);
    assert_eq!(value.path_invocation.input_format, JsonFormat::Json);
    assert_eq!(value.path_invocation.json_path.value, "$");
    assert!(
        value.path_invocation.path_name.is_none()
            && value.path_invocation.path_parameters.is_empty()
    );
    assert!(value.returned_type.is_none() && value.output_format.is_none());
    assert_eq!(value.wrapper_behavior, JsonQueryWrapperBehavior::Without);
    assert!(value.quotes_behavior.is_none());
    assert_eq!(value.empty_behavior, JsonQueryBehavior::Null);
    assert_eq!(value.error_behavior, JsonQueryBehavior::Null);
    let expression = parse_expression("JSON_SERIALIZE(?)").unwrap();
    let Expr::JsonSerialize(value) = expression else {
        panic!("not serialize")
    };
    assert_eq!(value.input_format, JsonFormat::Json);
    assert_eq!(value.error_behavior, JsonSerializeErrorBehavior::Error);
    assert!(value.returned_type.is_none() && value.output_format.is_none());
    let expression = parse_expression("JSON_OBJECT()").unwrap();
    assert!(object(&expression).members.is_empty());
    assert!(object(&expression).null_on_null);
    assert!(!object(&expression).unique_keys);
    let expression = parse_expression("JSON_ARRAY()").unwrap();
    assert!(array(&expression).elements.is_empty());
    assert!(!array(&expression).null_on_null);
}

#[test]
fn json_query_retains_all_formats_path_labels_passing_and_behavior_facts() {
    let source = r#"JSON_QUERY(? FORMAT JSON ENCODING UTF16, U&'$.\0061' AS "Root" PASSING ? FORMAT JSON AS "P", ? FORMAT JSON ENCODING UTF32 AS q RETURNING VARCHAR FORMAT JSON ENCODING UTF8 WITH CONDITIONAL ARRAY WRAPPER OMIT QUOTES ON SCALAR STRING EMPTY ARRAY ON EMPTY EMPTY OBJECT ON ERROR)"#;
    let expression = parse_expression(source).unwrap();
    let value = query(&expression);
    assert_eq!(value.span, Span::new(0, source.len()));
    let invocation = &value.path_invocation;
    assert_eq!(invocation.input_format, JsonFormat::Utf16);
    assert_eq!(parameter(&invocation.input_expression).position, 0);
    assert_eq!(invocation.json_path.value, "$.a");
    let path_start = source.find(r"U&'$.\0061'").unwrap();
    assert_eq!(
        invocation.json_path.span,
        Span::new(path_start, path_start + r"U&'$.\0061'".len())
    );
    assert_eq!(invocation.path_name.as_ref().unwrap().value, "Root");
    assert!(invocation.path_name.as_ref().unwrap().quoted);
    assert_eq!(invocation.path_parameters.len(), 2);
    assert_eq!(invocation.path_parameters[0].format, Some(JsonFormat::Json));
    assert_eq!(
        invocation.path_parameters[1].format,
        Some(JsonFormat::Utf32)
    );
    assert_eq!(
        parameter(&invocation.path_parameters[0].parameter).position,
        1
    );
    assert_eq!(
        parameter(&invocation.path_parameters[1].parameter).position,
        2
    );
    assert_eq!(invocation.path_parameters[0].name.value, "P");
    assert!(invocation.path_parameters[0].name.quoted);
    assert_eq!(invocation.path_parameters[1].name.value, "q");
    assert_eq!(
        generic(value.returned_type.as_ref().unwrap()).name.value,
        "VARCHAR"
    );
    assert_eq!(value.output_format, Some(JsonFormat::Utf8));
    assert_eq!(
        value.wrapper_behavior,
        JsonQueryWrapperBehavior::Conditional
    );
    assert_eq!(value.quotes_behavior, Some(JsonQueryQuotesBehavior::Omit));
    assert_eq!(value.empty_behavior, JsonQueryBehavior::EmptyArray);
    assert_eq!(value.error_behavior, JsonQueryBehavior::EmptyObject);
}

#[test]
fn exists_serialize_and_value_defaults_are_separate_from_default_expressions() {
    for (source_behavior, expected) in [
        ("FALSE", JsonExistsErrorBehavior::False),
        ("TRUE", JsonExistsErrorBehavior::True),
        ("UNKNOWN", JsonExistsErrorBehavior::Unknown),
        ("ERROR", JsonExistsErrorBehavior::Error),
    ] {
        let expression =
            parse_expression(&format!("JSON_EXISTS(?, '$' {source_behavior} ON ERROR)")).unwrap();
        let Expr::JsonExists(value) = expression else {
            panic!("not exists")
        };
        assert_eq!(value.error_behavior, expected);
    }
    let expression = parse_expression("JSON_SERIALIZE(? FORMAT JSON ENCODING UTF8 RETURNING VARBINARY FORMAT JSON ENCODING UTF32 NULL ON ERROR)").unwrap();
    let Expr::JsonSerialize(value) = expression else {
        panic!("not serialize")
    };
    assert_eq!(value.input_format, JsonFormat::Utf8);
    assert_eq!(value.output_format, Some(JsonFormat::Utf32));
    assert_eq!(value.error_behavior, JsonSerializeErrorBehavior::Null);
    assert_eq!(
        generic(value.returned_type.as_ref().unwrap()).name.value,
        "VARBINARY"
    );
    let expression = parse_expression(
        "JSON_VALUE(?, '$' RETURNING BIGINT DEFAULT NULL ON EMPTY DEFAULT ? ON ERROR)",
    )
    .unwrap();
    let Expr::JsonValue(value) = expression else {
        panic!("not value")
    };
    assert_eq!(value.empty_behavior, JsonValueBehavior::Default);
    assert!(matches!(
        &value.empty_default,
        Some(Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        }))
    ));
    assert_eq!(value.error_behavior, JsonValueBehavior::Default);
    assert_eq!(parameter(value.error_default.as_ref().unwrap()).position, 1);
    assert_eq!(
        generic(value.returned_type.as_ref().unwrap()).name.value,
        "BIGINT"
    );
    let expression = parse_expression("JSON_VALUE(?, '$' PASSING f(?) FILTER (WHERE ?) AS p DEFAULT ? ON EMPTY DEFAULT ? ON ERROR)").unwrap();
    let Expr::JsonValue(value) = expression else {
        panic!("not value")
    };
    let Expr::FunctionCall(passing) = &value.path_invocation.path_parameters[0].parameter else {
        panic!("not the real passing call")
    };
    assert_eq!(
        parameter(&value.path_invocation.input_expression).position,
        0
    );
    assert_eq!(parameter(passing.filter.as_ref().unwrap()).position, 1);
    assert_eq!(parameter(&passing.arguments[0].value).position, 2);
    assert_eq!(parameter(value.empty_default.as_ref().unwrap()).position, 3);
    assert_eq!(parameter(value.error_default.as_ref().unwrap()).position, 4);
}

#[test]
fn wrapper_quotes_and_query_behaviors_cover_the_complete_enum_choices() {
    for (clause, expected) in [
        ("WITHOUT WRAPPER", JsonQueryWrapperBehavior::Without),
        ("WITHOUT ARRAY WRAPPER", JsonQueryWrapperBehavior::Without),
        ("WITH WRAPPER", JsonQueryWrapperBehavior::Unconditional),
        (
            "WITH ARRAY WRAPPER",
            JsonQueryWrapperBehavior::Unconditional,
        ),
        (
            "WITH UNCONDITIONAL WRAPPER",
            JsonQueryWrapperBehavior::Unconditional,
        ),
        (
            "WITH CONDITIONAL ARRAY WRAPPER",
            JsonQueryWrapperBehavior::Conditional,
        ),
    ] {
        let expression = parse_expression(&format!("JSON_QUERY(?, '$' {clause})")).unwrap();
        assert_eq!(query(&expression).wrapper_behavior, expected);
    }
    for (clause, expected) in [
        ("KEEP QUOTES", JsonQueryQuotesBehavior::Keep),
        (
            "OMIT QUOTES ON SCALAR STRING",
            JsonQueryQuotesBehavior::Omit,
        ),
    ] {
        let expression = parse_expression(&format!("JSON_QUERY(?, '$' {clause})")).unwrap();
        assert_eq!(query(&expression).quotes_behavior, Some(expected));
    }
    for (behavior, expected) in [
        ("NULL", JsonQueryBehavior::Null),
        ("ERROR", JsonQueryBehavior::Error),
        ("EMPTY ARRAY", JsonQueryBehavior::EmptyArray),
        ("EMPTY OBJECT", JsonQueryBehavior::EmptyObject),
    ] {
        let expression = parse_expression(&format!(
            "JSON_QUERY(?, '$' {behavior} ON EMPTY {behavior} ON ERROR)"
        ))
        .unwrap();
        assert_eq!(query(&expression).empty_behavior, expected);
        assert_eq!(query(&expression).error_behavior, expected);
    }
}

#[test]
fn object_member_alternatives_preserve_children_formats_and_uncommitted_event_rollback() {
    let source = "JSON_OBJECT(KEY ? VALUE ? FORMAT JSON, ? : ? ABSENT ON NULL WITH UNIQUE KEYS RETURNING VARCHAR FORMAT JSON ENCODING UTF16)";
    let expression = parse_expression(source).unwrap();
    let value = object(&expression);
    assert_eq!(value.members.len(), 2);
    assert_eq!(parameter(&value.members[0].key).position, 0);
    assert_eq!(parameter(&value.members[0].value).position, 1);
    assert_eq!(parameter(&value.members[1].key).position, 2);
    assert_eq!(parameter(&value.members[1].value).position, 3);
    assert_eq!(value.members[0].format, Some(JsonFormat::Json));
    assert_eq!(value.members[1].format, None);
    assert!(!value.null_on_null && value.unique_keys);
    assert_eq!(value.output_format, Some(JsonFormat::Utf16));
    assert_eq!(
        generic(value.returned_type.as_ref().unwrap()).name.value,
        "VARCHAR"
    );
    assert_eq!(value.span, Span::new(0, source.len()));
    for source in ["JSON_OBJECT(KEY VALUE ?)", "JSON_OBJECT(KEY: ?)"] {
        let expression = parse_expression(source).unwrap();
        let value = object(&expression);
        let Expr::Identifier(key) = &value.members[0].key else {
            panic!("KEY must be the expression identifier")
        };
        assert_eq!(key.value, "KEY");
        assert_eq!(parameter(&value.members[0].value).position, 0);
    }
    let expression = parse_expression("JSON_OBJECT(KEY(?) FILTER (WHERE ?) VALUE ?)").unwrap();
    let value = object(&expression);
    let Expr::FunctionCall(key) = &value.members[0].key else {
        panic!("KEY belongs to the expression call here")
    };
    assert_eq!(parameter(key.filter.as_ref().unwrap()).position, 0);
    assert_eq!(parameter(&key.arguments[0].value).position, 1);
    assert_eq!(parameter(&value.members[0].value).position, 2);
    // Duplicate literal keys and passing names are facts for the semantic owner.
    let expression = parse_expression("JSON_OBJECT('x': 1, 'x': 2 WITH UNIQUE)").unwrap();
    assert_eq!(object(&expression).members.len(), 2);
    assert!(object(&expression).unique_keys);
    let expression = parse_expression("JSON_EXISTS(?, '$' PASSING ? AS p, ? AS p)").unwrap();
    let Expr::JsonExists(value) = expression else {
        panic!("not exists")
    };
    assert_eq!(value.path_invocation.path_parameters.len(), 2);
    assert_eq!(
        parameter(&value.path_invocation.path_parameters[1].parameter).position,
        2
    );
}

#[test]
fn arrays_preserve_nested_json_and_distinguish_absent_format_from_explicit_json() {
    let expression = parse_expression("JSON_ARRAY(? FORMAT JSON ENCODING UTF8, JSON_OBJECT('k': ?) NULL ON NULL RETURNING VARBINARY FORMAT JSON)").unwrap();
    let value = array(&expression);
    assert!(value.null_on_null);
    assert_eq!(value.elements.len(), 2);
    assert_eq!(value.elements[0].format, Some(JsonFormat::Utf8));
    assert_eq!(parameter(&value.elements[0].value).position, 0);
    assert!(value.elements[1].format.is_none());
    assert_eq!(
        parameter(&object(&value.elements[1].value).members[0].value).position,
        1
    );
    assert_eq!(
        generic(value.returned_type.as_ref().unwrap()).name.value,
        "VARBINARY"
    );
    assert_eq!(value.output_format, Some(JsonFormat::Json));
    let expression = parse_expression("JSON_ARRAY(1 ABSENT ON NULL)").unwrap();
    assert!(!array(&expression).null_on_null);
    let expression = parse_expression("JSON(? FORMAT JSON ENCODING UTF32)").unwrap();
    let Expr::JsonConstructor(value) = expression else {
        panic!("not constructor")
    };
    assert_eq!(value.format, JsonFormat::Utf32);
}

#[test]
fn returning_keyword_can_be_an_element_or_key_without_losing_its_grammar_alternative() {
    let expression = parse_expression("JSON_ARRAY(RETURNING VARCHAR FORMAT JSON)").unwrap();
    assert!(array(&expression).elements.is_empty());
    assert_eq!(
        generic(array(&expression).returned_type.as_ref().unwrap())
            .name
            .value,
        "VARCHAR"
    );
    for (source, null_on_null, format) in [
        ("JSON_ARRAY(RETURNING)", false, None),
        ("JSON_ARRAY(RETURNING NULL ON NULL)", true, None),
        (
            "JSON_ARRAY(RETURNING FORMAT JSON)",
            false,
            Some(JsonFormat::Json),
        ),
    ] {
        let expression = parse_expression(source).unwrap();
        let value = array(&expression);
        assert!(value.returned_type.is_none());
        let Expr::Identifier(name) = &value.elements[0].value else {
            panic!("not RETURNING expression")
        };
        assert_eq!(name.value, "RETURNING");
        assert_eq!(value.null_on_null, null_on_null);
        assert_eq!(value.elements[0].format, format);
    }
    for source in [
        "JSON_OBJECT(RETURNING VALUE ?)",
        "JSON_OBJECT(RETURNING: ?)",
    ] {
        let expression = parse_expression(source).unwrap();
        let value = object(&expression);
        assert!(value.returned_type.is_none());
        let Expr::Identifier(name) = &value.members[0].key else {
            panic!("not RETURNING key")
        };
        assert_eq!(name.value, "RETURNING");
        assert_eq!(parameter(&value.members[0].value).position, 0);
    }
}

#[test]
fn table_columns_keep_inherited_errors_distinct_from_explicit_null_and_nested_defaults() {
    let source = "JSON_TABLE(?, '$' COLUMNS (ord FOR ORDINALITY, a BIGINT, b BIGINT NULL ON ERROR, c VARCHAR FORMAT JSON, d VARCHAR FORMAT JSON NULL ON ERROR, NESTED PATH '$.n' AS n COLUMNS (v INTEGER DEFAULT ? ON EMPTY DEFAULT NULL ON ERROR)) EMPTY ON ERROR)";
    let value = table(source);
    assert_eq!(value.span, Span::new(14, 14 + source.len()));
    assert_eq!(value.columns.len(), 6);
    assert_eq!(value.error_behavior, Some(JsonTableErrorBehavior::Empty));
    assert!(value.plan.is_none());
    let JsonTableColumn::Ordinality(ordinal) = &value.columns[0] else {
        panic!("not ordinality")
    };
    assert_eq!(ordinal.name.value, "ord");
    let JsonTableColumn::Value(inherited) = &value.columns[1] else {
        panic!("not value")
    };
    assert_eq!(inherited.empty_behavior, JsonValueBehavior::Null);
    assert!(inherited.json_path.is_none());
    assert!(inherited.error_behavior.is_none());
    let JsonTableColumn::Value(explicit) = &value.columns[2] else {
        panic!("not value")
    };
    assert_eq!(explicit.error_behavior, Some(JsonValueBehavior::Null));
    let JsonTableColumn::Query(inherited) = &value.columns[3] else {
        panic!("not query")
    };
    assert_eq!(inherited.format, JsonFormat::Json);
    assert!(inherited.error_behavior.is_none());
    let JsonTableColumn::Query(explicit) = &value.columns[4] else {
        panic!("not query")
    };
    assert_eq!(explicit.error_behavior, Some(JsonQueryBehavior::Null));
    let JsonTableColumn::Nested(nested) = &value.columns[5] else {
        panic!("not nested")
    };
    assert_eq!(nested.json_path.value, "$.n");
    assert_eq!(nested.path_name.as_ref().unwrap().value, "n");
    let JsonTableColumn::Value(child) = &nested.columns[0] else {
        panic!("not child value")
    };
    assert_eq!(parameter(child.empty_default.as_ref().unwrap()).position, 1);
    assert_eq!(child.error_behavior, Some(JsonValueBehavior::Default));
    assert!(matches!(
        &child.error_default,
        Some(Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        }))
    ));
    let value = table("JSON_TABLE(?, '$' COLUMNS (NESTED PATH))");
    let JsonTableColumn::Value(column) = &value.columns[0] else {
        panic!("NESTED is the column name here")
    };
    assert_eq!(column.name.value, "NESTED");
    assert_eq!(generic(&column.data_type).name.value, "PATH");
}

#[test]
fn table_query_column_options_and_plan_defaults_cover_both_clause_orders() {
    let value = table(
        "JSON_TABLE(? FORMAT JSON ENCODING UTF8, '$' COLUMNS (x VARCHAR FORMAT JSON ENCODING UTF16 PATH '$.x' WITH UNCONDITIONAL WRAPPER KEEP QUOTES ON SCALAR STRING EMPTY OBJECT ON EMPTY ERROR ON ERROR))",
    );
    assert_eq!(value.path_invocation.input_format, JsonFormat::Utf8);
    let JsonTableColumn::Query(column) = &value.columns[0] else {
        panic!("not query")
    };
    assert_eq!(column.format, JsonFormat::Utf16);
    assert_eq!(column.json_path.as_ref().unwrap().value, "$.x");
    assert_eq!(
        column.wrapper_behavior,
        JsonQueryWrapperBehavior::Unconditional
    );
    assert_eq!(column.quotes_behavior, Some(JsonQueryQuotesBehavior::Keep));
    assert_eq!(column.empty_behavior, JsonQueryBehavior::EmptyObject);
    assert_eq!(column.error_behavior, Some(JsonQueryBehavior::Error));
    for (clause, parent, siblings) in [
        (
            "INNER",
            JsonTableParentChildPlanType::Inner,
            JsonTableSiblingsPlanType::Union,
        ),
        (
            "OUTER",
            JsonTableParentChildPlanType::Outer,
            JsonTableSiblingsPlanType::Union,
        ),
        (
            "CROSS",
            JsonTableParentChildPlanType::Outer,
            JsonTableSiblingsPlanType::Cross,
        ),
        (
            "UNION",
            JsonTableParentChildPlanType::Outer,
            JsonTableSiblingsPlanType::Union,
        ),
        (
            "INNER,CROSS",
            JsonTableParentChildPlanType::Inner,
            JsonTableSiblingsPlanType::Cross,
        ),
        (
            "CROSS,INNER",
            JsonTableParentChildPlanType::Inner,
            JsonTableSiblingsPlanType::Cross,
        ),
        (
            "OUTER,UNION",
            JsonTableParentChildPlanType::Outer,
            JsonTableSiblingsPlanType::Union,
        ),
        (
            "UNION,OUTER",
            JsonTableParentChildPlanType::Outer,
            JsonTableSiblingsPlanType::Union,
        ),
    ] {
        let value = table(&format!(
            "JSON_TABLE(?, '$' COLUMNS (x BIGINT) PLAN DEFAULT ({clause}))"
        ));
        let Some(JsonTablePlan::Default(plan)) = value.plan else {
            panic!("not default plan")
        };
        assert_eq!(plan.parent_child, parent);
        assert_eq!(plan.siblings, siblings);
    }
}

#[test]
fn table_specific_plan_keeps_parent_and_nested_sibling_order_without_flattening() {
    let source = "JSON_TABLE(?, '$' AS root COLUMNS (NESTED '$.a' AS a COLUMNS (v INTEGER), NESTED '$.b' AS b COLUMNS (v INTEGER), NESTED '$.c' AS c COLUMNS (v INTEGER)) PLAN (root OUTER (a UNION (b CROSS c))))";
    let value = table(source);
    let Some(JsonTablePlan::Specific(JsonTableSpecificPlan::ParentChild(plan))) = value.plan else {
        panic!("not parent-child")
    };
    assert_eq!(plan.plan_type, JsonTableParentChildPlanType::Outer);
    assert_eq!(plan.parent.name.value, "root");
    let parent_start = 14 + source.find("root OUTER").unwrap();
    assert_eq!(plan.parent.span, Span::new(parent_start, parent_start + 4));
    let JsonTableSpecificPlan::Siblings(children) = plan.child.as_ref() else {
        panic!("not union siblings")
    };
    assert_eq!(children.plan_type, JsonTableSiblingsPlanType::Union);
    assert_eq!(children.siblings.len(), 2);
    let JsonTableSpecificPlan::Leaf(first) = &children.siblings[0] else {
        panic!("not first child")
    };
    assert_eq!(first.name.value, "a");
    let JsonTableSpecificPlan::Siblings(second) = &children.siblings[1] else {
        panic!("not nested cross")
    };
    assert_eq!(second.plan_type, JsonTableSiblingsPlanType::Cross);
    let names: Vec<_> = second
        .siblings
        .iter()
        .map(|plan| {
            let JsonTableSpecificPlan::Leaf(leaf) = plan else {
                panic!("not leaf")
            };
            leaf.name.value.as_str()
        })
        .collect();
    assert_eq!(names, ["b", "c"]);
    let value = table("JSON_TABLE(?, '$' COLUMNS (x BIGINT) PLAN ((a) UNION (b)))");
    let Some(JsonTablePlan::Specific(JsonTableSpecificPlan::Siblings(plan))) = value.plan else {
        panic!("not siblings")
    };
    assert_eq!(plan.siblings.len(), 2);
}

#[test]
fn malformed_json_syntax_never_fabricates_paths_or_drops_duplicate_clauses() {
    for source in [
        "JSON_QUERY(?, ?)",
        "JSON_QUERY(?, NULL)",
        "JSON_QUERY(?, $$path$$)",
        "JSON_QUERY(?, '$' FORMAT JSON)",
        "JSON_QUERY(?, '$' RETURNING VARCHAR FORMAT JSON FORMAT JSON)",
        "JSON_QUERY(?, '$' WITH WRAPPER WITHOUT WRAPPER)",
        "JSON_VALUE(?, '$' NULL ON ERROR NULL ON EMPTY)",
        "JSON_VALUE(?, '$' DEFAULT ON EMPTY)",
        "JSON_EXISTS(?, '$' NULL ON ERROR)",
        "JSON_OBJECT(WITH UNIQUE KEYS)",
        "JSON_OBJECT('k' 1)",
        "JSON_ARRAY(NULL ON NULL)",
        "JSON_SERIALIZE(? EMPTY ON ERROR)",
    ] {
        assert!(
            matches!(parse_expression(source), Err(ParserError::Parse(_))),
            "{source}"
        );
    }
    for source in [
        "JSON_TABLE(?, '$' COLUMNS ())",
        "JSON_TABLE(?, '$' COLUMNS (NESTED '$.a' COLUMNS ()))",
        "JSON_TABLE(?, '$' COLUMNS (q VARCHAR FORMAT JSON DEFAULT ? ON EMPTY))",
        "JSON_TABLE(?, '$' COLUMNS (x BIGINT) PLAN ((root)))",
        "JSON_TABLE(?, '$' COLUMNS (x BIGINT) PLAN (root UNION a CROSS b))",
        "JSON_TABLE(?, '$' COLUMNS (x BIGINT) PLAN DEFAULT (INNER,OUTER))",
    ] {
        assert!(
            matches!(
                parse_query(&format!("SELECT * FROM {source}")),
                Err(ParserError::Parse(_))
            ),
            "{source}"
        );
    }
    let source = "JSON_OBJECT(KEY CAST(? AS DECIMAL(9223372036854775808)) VALUE ?)";
    let ParserError::Parse(ParseError::UnexpectedToken { span, .. }) =
        parse_expression(source).unwrap_err()
    else {
        panic!("not the numeric constructor error")
    };
    let start = source.find("9223372036854775808").unwrap();
    assert_eq!(span, Span::new(start, start + 19));
}

#[test]
fn every_existing_json_grammar_coverage_sample_reaches_the_new_query_parser() {
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
        assert!(
            !fields[5].is_empty(),
            "unfilled JSON grammar sample: {line}"
        );
        parse_query(&super::coverage::sample(fields[5])).unwrap_or_else(|error| {
            panic!(
                "{} alternative {} clause {}: {}: {error:?}",
                fields[1], fields[2], fields[4], fields[5]
            )
        });
    }
}

#[test]
fn json_constructor_and_nonreserved_function_alternatives_keep_their_own_facts() {
    use novarocks_parser::ast::*;
    use novarocks_parser::parser::parse_expression;
    assert!(matches!(
        parse_expression("JSON(?)").unwrap(),
        Expr::JsonConstructor(_)
    ));
    assert!(matches!(
        parse_expression("JSON '{}' ").unwrap(),
        Expr::Literal(Literal {
            kind: LiteralKind::Generic(_),
            ..
        })
    ));
    assert!(matches!(
        parse_expression("JSON").unwrap(),
        Expr::Identifier(_)
    ));
    let Expr::FunctionCall(empty) = parse_expression("JSON()").unwrap() else {
        panic!("ordinary empty call")
    };
    assert!(empty.arguments.is_empty());
    let Expr::FunctionCall(pair) = parse_expression("JSON(?, ?)").unwrap() else {
        panic!("ordinary two-argument call")
    };
    assert_eq!(pair.arguments.len(), 2);
    assert!(matches!(&pair.arguments[0].value, Expr::Parameter(p) if p.position == 0));
    assert!(matches!(&pair.arguments[1].value, Expr::Parameter(p) if p.position == 1));
    let Expr::FunctionCall(distinct) = parse_expression("JSON(DISTINCT ?)").unwrap() else {
        panic!("ordinary distinct call")
    };
    assert!(distinct.distinct);
    let Expr::FunctionCall(named) = parse_expression("JSON(value => ?)").unwrap() else {
        panic!("ordinary named call")
    };
    assert_eq!(named.arguments[0].name.as_ref().unwrap().value, "value");
    let Expr::FunctionCall(quoted) = parse_expression("\"JSON\"(?)").unwrap() else {
        panic!("quoted ordinary call")
    };
    assert!(quoted.name.parts[0].quoted);
}

#[test]
fn json_constructor_trial_restores_events_before_a_complete_function_follow() {
    use novarocks_parser::ast::*;
    use novarocks_parser::parser::parse_expression;
    let Expr::FunctionCall(filtered) = parse_expression("JSON(?) FILTER (WHERE ?)").unwrap() else {
        panic!("ordinary filtered call")
    };
    assert!(matches!(filtered.filter.as_ref().unwrap(), Expr::Parameter(p) if p.position == 0));
    assert!(matches!(&filtered.arguments[0].value, Expr::Parameter(p) if p.position == 1));
    let Expr::FunctionCall(call) =
        parse_expression("JSON(IF(?, ? ORDER BY ?)) OVER (PARTITION BY ?)").unwrap()
    else {
        panic!("ordinary window call")
    };
    let Window::Specification(window) = call.window.as_ref().unwrap() else {
        panic!("window")
    };
    assert!(matches!(&window.partition_by[0], Expr::Parameter(p) if p.position == 0));
    let Expr::If(control) = &call.arguments[0].value else {
        panic!("IF")
    };
    // Position 1 belongs to the successfully visited, discarded IF ORDER BY.
    assert!(matches!(&control.condition, Expr::Parameter(p) if p.position == 2));
    assert!(matches!(&control.true_value, Expr::Parameter(p) if p.position == 3));
}
