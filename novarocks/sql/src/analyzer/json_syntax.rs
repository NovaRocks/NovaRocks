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

use novarocks_parser::ast;

use crate::analysis::TypedExpr;
use crate::analyze_error::AnalyzeError;

use super::AnalyzerContext;
use super::resolve_expr::resolved_scalar_call_at;
use super::scope::AnalyzerScope;

/// The pinned AstBuilder facts for JSON_QUERY(input, 'path') without clauses.
/// Discovery and analysis share this decision before visiting either operand.
pub(crate) fn is_plain_json_query(node: &ast::JsonQuery) -> bool {
    let invocation = &node.path_invocation;
    invocation.input_format == ast::JsonFormat::Json
        && invocation.path_name.is_none()
        && invocation.path_parameters.is_empty()
        && node.returned_type.is_none()
        && node.output_format.is_none()
        && node.wrapper_behavior == ast::JsonQueryWrapperBehavior::Without
        && node.quotes_behavior.is_none()
        && node.empty_behavior == ast::JsonQueryBehavior::Null
        && node.error_behavior == ast::JsonQueryBehavior::Null
}

/// D22 preserves the existing object's NULL-value and duplicate-key semantics.
/// This admits a syntax profile to the original binder, not an arity guarantee.
pub(crate) fn is_supported_json_object_profile(node: &ast::JsonObject) -> bool {
    node.null_on_null
        && !node.unique_keys
        && node.returned_type.is_none()
        && node.output_format.is_none()
        && node.members.iter().all(|member| member.format.is_none())
}

/// Only profiles expressible without filtering or reinterpreting elements may
/// reach the original JSON_ARRAY binder. Its unavailable implementation remains
/// unavailable; this predicate does not install or promise an execution kernel.
pub(crate) fn is_supported_json_array_profile(node: &ast::JsonArray) -> bool {
    node.returned_type.is_none()
        && node.output_format.is_none()
        && node.elements.iter().all(|element| element.format.is_none())
        && if node.elements.is_empty() {
            !node.null_on_null
        } else {
            node.null_on_null
        }
}

impl AnalyzerContext<'_> {
    pub(super) fn analyze_json_object_syntax(
        &self,
        node: &ast::JsonObject,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        if !is_supported_json_object_profile(node) {
            return Err(AnalyzeError::unsupported_expression(
                "JSON_OBJECT only supports NULL ON NULL without UNIQUE KEYS, RETURNING, or FORMAT",
                node.span,
            ));
        }
        let arguments = node
            .members
            .iter()
            .flat_map(|member| [&member.key, &member.value])
            .collect::<Vec<_>>();
        self.analyze_json_constructor_call("json_object", &arguments, node.span, scope)
    }

    pub(super) fn analyze_json_array_syntax(
        &self,
        node: &ast::JsonArray,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        if !is_supported_json_array_profile(node) {
            return Err(AnalyzeError::unsupported_expression(
                "JSON_ARRAY only supports its empty default profile or NULL ON NULL without RETURNING or FORMAT",
                node.span,
            ));
        }
        let arguments = node
            .elements
            .iter()
            .map(|element| &element.value)
            .collect::<Vec<_>>();
        self.analyze_json_constructor_call("json_array", &arguments, node.span, scope)
    }

    /// Enter the original scalar-function analysis owner, including its arity,
    /// type validation, catalog availability, binding, and value-domain rules.
    /// Child syntax keeps its actual original occurrence, including the pointer
    /// identity used by source constants and signed write assignments. No SQL
    /// text or selected binding is manufactured by this typed lowering.
    fn analyze_json_constructor_call(
        &self,
        name: &'static str,
        arguments: &[&ast::Expr],
        span: novarocks_parser::Span,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let mut expression = novarocks_parser::build::function(
            ast::ObjectName {
                parts: vec![novarocks_parser::build::ident_at(name, false, span)],
                span,
            },
            Vec::new(),
        );
        let ast::Expr::FunctionCall(call) = &mut expression else {
            unreachable!("the function builder produces a FunctionCall");
        };
        call.span = span;
        self.analyze_function_with_original_arguments(call, arguments, scope)
    }

    /// Preserve the original two-argument JSON accessor's binding owner.
    pub(super) fn analyze_json_query_syntax(
        &self,
        node: &ast::JsonQuery,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        if !is_plain_json_query(node) {
            return Err(AnalyzeError::unsupported_expression(
                "JSON_QUERY only supports its plain two-argument profile",
                node.span,
            ));
        }

        let invocation = &node.path_invocation;
        let input = self.analyze_expr(&invocation.input_expression, scope)?;
        // JsonPathString already owns the original StringLiteral's decoded
        // payload and location. Reuse the ordinary literal analyzer unchanged.
        let path = ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::String(invocation.json_path.value.clone()),
            span: invocation.json_path.span,
        });
        let path = self.analyze_expr(&path, scope)?;
        resolved_scalar_call_at(
            self.function_catalog,
            "json_query",
            vec![input, path],
            invocation.json_path.span,
            self.sql_semantics.sql_mode().decimal_overflow_policy(),
            self.constant_policy,
            self.control,
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_functions::{FunctionArgumentType, FunctionResultType};
    use novarocks_parser::{Span, build};
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

    use super::*;
    use crate::analysis::{ExprKind, LiteralValue};
    use crate::analyze_error::AnalyzeErrorKind;
    use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
    use crate::compiler::SqlCompileControl;

    struct NoTables;
    impl PlannerTableProvider for NoTables {
        fn resolve_table_for_analysis(
            &self,
            _: Option<&str>,
            _: &str,
            _: &str,
        ) -> Result<ResolvedAnalyzerTable, String> {
            panic!("expression fixture must not discover a table")
        }
    }
    static NO_TABLES: NoTables = NoTables;

    // These original component fixtures intentionally use unscoped metadata.
    // They prove binding/type behavior, not full compiler or native admission.
    // Presence-scoped refusals and the declaration shortcut are guarded below.
    fn context(control: &SqlCompileControl) -> AnalyzerContext<'_> {
        AnalyzerContext {
            grouping_scope: Default::default(),
            view_stack: Default::default(),
            control,
            constant_policy: crate::constant::test_constant_policy(),
            materialized_values: None,
            catalog: &NO_TABLES,
            current_database: "default",
            function_catalog: crate::functions::builtin_sql_function_catalog(),
            sql_semantics: Default::default(),
            factory: std::rc::Rc::new(std::cell::RefCell::new(
                crate::column_id::ColumnRefFactory::new(),
            )),
            ctes: Default::default(),
            pending_ctes: Default::default(),
            next_subquery_id: Default::default(),
            next_lambda_slot_id: Default::default(),
            collected_subqueries: Default::default(),
            cte_registry: std::cell::RefCell::new(crate::analysis::cte::CTERegistry::new()),
        }
    }

    fn plain(input: ast::Expr, path: &str) -> ast::JsonQuery {
        ast::JsonQuery::plain(
            ast::JsonPathInvocation::plain(
                input,
                ast::JsonPathString::new(path.to_owned(), Span::new(41, 80)),
                Span::new(11, 80),
            ),
            Span::new(0, 81),
        )
    }

    fn ordinary_call(node: &ast::JsonQuery) -> ast::Expr {
        build::function(
            build::object_name(vec![build::ident("json_query", false)]).unwrap(),
            vec![
                node.path_invocation.input_expression.clone(),
                ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::String(node.path_invocation.json_path.value.clone()),
                    span: node.path_invocation.json_path.span,
                }),
            ],
        )
    }

    // Compare complete binding facts and recursively preserved argument kinds,
    // rather than comparing display names or reconstructing selected metadata.
    fn assert_same_typed(actual: &TypedExpr, expected: &TypedExpr) {
        assert_eq!(actual.value_type, expected.value_type);
        match (&actual.kind, &expected.kind) {
            (
                ExprKind::FunctionCall {
                    name: actual_name,
                    args: actual_args,
                    distinct: actual_distinct,
                    binding: actual_binding,
                    volatility: actual_volatility,
                },
                ExprKind::FunctionCall {
                    name: expected_name,
                    args: expected_args,
                    distinct: expected_distinct,
                    binding: expected_binding,
                    volatility: expected_volatility,
                },
            ) => {
                assert_eq!(actual_name, expected_name);
                assert_eq!(actual_binding, expected_binding);
                assert_eq!(actual_distinct, expected_distinct);
                assert_eq!(actual_volatility, expected_volatility);
                assert_eq!(actual_args.len(), expected_args.len());
                for (actual, expected) in actual_args.iter().zip(expected_args) {
                    assert_same_typed(actual, expected);
                }
            }
            (
                ExprKind::ColumnRef {
                    column_id: actual_id,
                    qualifier: actual_qualifier,
                    column: actual_column,
                },
                ExprKind::ColumnRef {
                    column_id: expected_id,
                    qualifier: expected_qualifier,
                    column: expected_column,
                },
            ) => {
                assert_eq!(actual_id, expected_id);
                assert_eq!(actual_qualifier, expected_qualifier);
                assert_eq!(actual_column, expected_column);
            }
            (ExprKind::Literal(actual), ExprKind::Literal(expected)) => {
                assert_eq!(actual, expected);
            }
            (ExprKind::Constant(actual), ExprKind::Constant(expected)) => {
                assert_eq!(
                    actual.pool().backing_identity(),
                    expected.pool().backing_identity()
                );
                assert_eq!(actual.ordinal(), expected.ordinal());
                assert_eq!(actual.field(), expected.field());
                assert_eq!(actual.value_type(), expected.value_type());
            }
            (
                ExprKind::Cast {
                    array_failure_mode: None,
                    expr: actual,
                    target: actual_target,
                    decimal_overflow_policy: actual_policy,
                },
                ExprKind::Cast {
                    array_failure_mode: None,
                    expr: expected,
                    target: expected_target,
                    decimal_overflow_policy: expected_policy,
                },
            ) => {
                assert_eq!(actual_target, expected_target);
                assert_eq!(actual_policy, expected_policy);
                assert_same_typed(actual, expected);
            }
            _ => panic!("unexpected argument kinds: {actual:?} versus {expected:?}"),
        }
    }

    #[test]
    fn plain_query_keeps_canonical_binding_full_types_and_original_column_identity() {
        for logical in [ValueLogicalType::Physical, ValueLogicalType::Json] {
            for nullable in [false, true] {
                for mode in ["", "ERROR_IF_OVERFLOW"] {
                    let control = SqlCompileControl::unbounded();
                    let mut ctx = context(&control);
                    ctx.sql_semantics = ctx
                        .sql_semantics
                        .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(mode));
                    let mut scope = ctx.new_scope();
                    let input_type =
                        FunctionValueType::try_with_logical_type(DataType::Utf8, nullable, logical)
                            .unwrap();
                    let input_id = scope.add_column(None, "document", input_type.clone());
                    let node = plain(
                        ast::Expr::Identifier(build::ident_at(
                            "document",
                            false,
                            Span::new(11, 19),
                        )),
                        "$.a.b",
                    );
                    let actual = ctx.analyze_json_query_syntax(&node, &scope).unwrap();
                    let expected = ctx.analyze_expr(&ordinary_call(&node), &scope).unwrap();
                    assert_same_typed(&actual, &expected);
                    let ExprKind::FunctionCall { binding, args, .. } = &actual.kind else {
                        panic!("canonical scalar call");
                    };
                    assert_eq!(binding.function_id.as_str(), "builtin.scalar/json_query/v1");
                    assert_eq!(
                        binding.decimal_overflow_policy(),
                        ctx.sql_semantics.sql_mode().decimal_overflow_policy()
                    );
                    assert_eq!(args.len(), 2);
                    assert_eq!(args[0].value_type, input_type);
                    assert!(
                        matches!(args[0].kind, ExprKind::ColumnRef { column_id, .. } if column_id == input_id)
                    );
                    assert_eq!(
                        args[1].value_type,
                        FunctionValueType::new(DataType::Utf8, false)
                    );
                    assert!(
                        matches!(&args[1].kind, ExprKind::Literal(LiteralValue::String(value)) if value == "$.a.b")
                    );
                    for (arg, selected) in args.iter().zip(&binding.selected.argument_types) {
                        assert_eq!(
                            selected,
                            &FunctionArgumentType::Value(arg.value_type.clone())
                        );
                    }
                    assert_eq!(actual.value_type.data_type(), &DataType::Utf8);
                    assert_eq!(actual.value_type.logical_type(), ValueLogicalType::Json);
                    assert_eq!(
                        binding.selected.result_type,
                        FunctionResultType::Scalar(actual.value_type.clone())
                    );
                }
            }
        }
    }

    #[test]
    fn plain_query_preserves_nested_input_binding_and_decoded_path_bytes() {
        let control = SqlCompileControl::unbounded();
        let ctx = context(&control);
        let scope = ctx.new_scope();
        let input = build::function(
            build::object_name(vec![build::ident("parse_json", false)]).unwrap(),
            vec![build::string("{\"雪'\\\\name\":1}")],
        );
        let path = "lax $.\"雪'\\\\name\"";
        let node = plain(input, path);
        let input_before = ctx
            .analyze_expr(&node.path_invocation.input_expression, &scope)
            .unwrap();
        let actual = ctx.analyze_json_query_syntax(&node, &scope).unwrap();
        let expected = ctx.analyze_expr(&ordinary_call(&node), &scope).unwrap();
        assert_same_typed(&actual, &expected);
        let ExprKind::FunctionCall { args, .. } = &actual.kind else {
            panic!("scalar call")
        };
        assert_same_typed(&args[0], &input_before);
        assert!(
            matches!(&args[1].kind, ExprKind::Literal(LiteralValue::String(value)) if value == path)
        );
    }

    fn non_plain_profiles() -> Vec<(&'static str, ast::JsonQuery)> {
        let base = plain(build::string("{}"), "$[");
        let mut profiles = Vec::new();
        macro_rules! changed {
            ($name:literal, $change:expr) => {{
                let mut node = base.clone();
                ($change)(&mut node);
                profiles.push(($name, node));
            }};
        }
        changed!("FORMAT UTF8", |n: &mut ast::JsonQuery| n
            .path_invocation
            .input_format =
            ast::JsonFormat::Utf8);
        changed!("FORMAT UTF16", |n: &mut ast::JsonQuery| n
            .path_invocation
            .input_format =
            ast::JsonFormat::Utf16);
        changed!("FORMAT UTF32", |n: &mut ast::JsonQuery| n
            .path_invocation
            .input_format =
            ast::JsonFormat::Utf32);
        changed!("path name", |n: &mut ast::JsonQuery| n
            .path_invocation
            .path_name =
            Some(build::ident("path_name", false)));
        changed!("PASSING", |n: &mut ast::JsonQuery| n
            .path_invocation
            .path_parameters
            .push(ast::JsonPathParameter {
                name: build::ident("parameter", false),
                parameter: ast::Expr::Identifier(build::ident("unknown_parameter", false)),
                format: None,
                span: Span::new(83, 90),
            }));
        changed!("RETURNING", |n: &mut ast::JsonQuery| n.returned_type =
            Some(build::simple_type("VARCHAR")));
        changed!("output FORMAT JSON", |n: &mut ast::JsonQuery| n
            .output_format =
            Some(ast::JsonFormat::Json));
        changed!("CONDITIONAL", |n: &mut ast::JsonQuery| n.wrapper_behavior =
            ast::JsonQueryWrapperBehavior::Conditional);
        changed!("UNCONDITIONAL", |n: &mut ast::JsonQuery| n
            .wrapper_behavior =
            ast::JsonQueryWrapperBehavior::Unconditional);
        changed!("explicit KEEP", |n: &mut ast::JsonQuery| n
            .quotes_behavior =
            Some(ast::JsonQueryQuotesBehavior::Keep));
        changed!("explicit OMIT", |n: &mut ast::JsonQuery| n
            .quotes_behavior =
            Some(ast::JsonQueryQuotesBehavior::Omit));
        for behavior in [
            ast::JsonQueryBehavior::Error,
            ast::JsonQueryBehavior::EmptyArray,
            ast::JsonQueryBehavior::EmptyObject,
        ] {
            let mut empty = base.clone();
            empty.empty_behavior = behavior;
            profiles.push(("ON EMPTY", empty));
            let mut error = base.clone();
            error.error_behavior = behavior;
            profiles.push(("ON ERROR", error));
        }
        profiles
    }

    #[test]
    fn every_non_plain_fact_is_rejected_before_unknown_input_or_malformed_path() {
        let control = SqlCompileControl::unbounded();
        let ctx = context(&control);
        let scope = ctx.new_scope();
        let unknown =
            ast::Expr::Identifier(build::ident_at("unknown_input", false, Span::new(11, 24)));
        assert_eq!(
            ctx.analyze_expr(&unknown, &scope).unwrap_err().kind(),
            AnalyzeErrorKind::UnknownColumn
        );
        for (name, node) in non_plain_profiles() {
            assert!(!is_plain_json_query(&node), "{name}");
            // Malformed decoded JSON path is deliberately not parsed or bound
            // on this unsupported profile, even with an otherwise valid input.
            assert_eq!(node.path_invocation.json_path.value, "$[");
            let error = ctx.analyze_json_query_syntax(&node, &scope).unwrap_err();
            assert_eq!(
                error.kind(),
                AnalyzeErrorKind::UnsupportedExpression,
                "{name}"
            );
            assert_eq!(error.span(), Some(node.span), "{name}");
            let mut unknown_node = node;
            unknown_node.path_invocation.input_expression = unknown.clone();
            let error = ctx
                .analyze_json_query_syntax(&unknown_node, &scope)
                .unwrap_err();
            assert_eq!(
                error.kind(),
                AnalyzeErrorKind::UnsupportedExpression,
                "{name}"
            );
            assert_eq!(error.span(), Some(unknown_node.span), "{name}");
        }
    }

    #[test]
    fn exact_plain_profile_keeps_operand_failures_and_ignores_source_location() {
        let control = SqlCompileControl::unbounded();
        let ctx = context(&control);
        let scope = ctx.new_scope();
        let unknown =
            ast::Expr::Identifier(build::ident_at("unknown_input", false, Span::new(11, 24)));
        let mut node = plain(unknown, "$.a");
        assert!(is_plain_json_query(&node));
        let error = ctx.analyze_json_query_syntax(&node, &scope).unwrap_err();
        assert_eq!(error.kind(), AnalyzeErrorKind::UnknownColumn);
        assert_eq!(error.span(), Some(Span::new(11, 24)));
        node.span = Span::new(300, 381);
        node.path_invocation.span = Span::new(311, 380);
        node.path_invocation.json_path.span = Span::new(341, 380);
        assert!(is_plain_json_query(&node));
    }

    fn projection_query(expression: ast::Expr, from: Vec<ast::Relation>) -> ast::Query {
        build::query(ast::SetExpr::Select(ast::AstBox::new(
            build::select(
                vec![novarocks_parser::build::item(expression, None)],
                from.into_iter()
                    .reduce(|left, right| build::join(left, ast::JoinType::Implicit, right, None)),
                None,
                None,
            )
            .unwrap(),
        )))
    }

    fn missing_table(name: &str) -> ast::Relation {
        build::table(
            build::object_name(vec![build::ident(name, false)]).unwrap(),
            None,
            None,
        )
    }

    fn first_projection(query: &crate::analysis::ResolvedQuery) -> &TypedExpr {
        let crate::analysis::QueryBody::Select(select) = &query.body else {
            panic!("SELECT fixture");
        };
        assert_eq!(select.projection.len(), 1);
        &select.projection[0].expr
    }

    #[test]
    fn whole_query_entry_preserves_plain_binding_arguments_and_full_value_types() {
        for input in [
            build::string("{\"a\":1}"),
            build::function(
                build::object_name(vec![build::ident("parse_json", false)]).unwrap(),
                vec![build::string("{\"a\":1}")],
            ),
        ] {
            let node = plain(input, "$.a");
            let actual_query =
                projection_query(ast::Expr::JsonQuery(ast::AstBox::new(node.clone())), vec![]);
            let expected_query = projection_query(ordinary_call(&node), vec![]);
            let (actual, _, _) = crate::analyzer::analyze(&actual_query, &NO_TABLES, "default")
                .expect("plain typed JSON_QUERY must enter the original scalar binder");
            let (expected, _, _) = crate::analyzer::analyze(&expected_query, &NO_TABLES, "default")
                .expect("ordinary json_query comparison must analyze");
            assert_same_typed(first_projection(&actual), first_projection(&expected));
            assert_eq!(
                first_projection(&actual).value_type.logical_type(),
                ValueLogicalType::Json
            );
        }
    }

    #[test]
    fn whole_query_rejects_non_plain_json_before_any_from_table_discovery() {
        for (name, mut node) in non_plain_profiles() {
            node.path_invocation.input_expression =
                ast::Expr::Identifier(build::ident_at("unknown_input", false, Span::new(11, 24)));
            let span = node.span;
            let query = projection_query(
                ast::Expr::JsonQuery(ast::AstBox::new(node)),
                vec![missing_table("missing_from_table")],
            );
            // NoTables panics on any lookup. This exercises the public analyzer
            // entry and its prepass ordering, not only the local consumer guard.
            let error = crate::analyzer::analyze(&query, &NO_TABLES, "default").unwrap_err();
            assert_eq!(
                error.kind(),
                AnalyzeErrorKind::UnsupportedExpression,
                "{name}"
            );
            assert_eq!(error.span(), Some(span), "{name}");
            assert!(error.message().contains("JSON_QUERY"), "{name}");
        }
    }

    #[test]
    fn whole_query_rejects_json_table_in_unused_cte_before_outer_or_inner_lookup() {
        let factor_span = Span::new(200, 290);
        let table = ast::JsonTable::new(
            ast::JsonPathInvocation::plain(
                ast::Expr::Identifier(build::ident("unknown_json_input", false)),
                ast::JsonPathString::new("$[".into(), Span::new(220, 225)),
                Span::new(210, 225),
            ),
            vec![ast::JsonTableColumn::Ordinality(
                ast::JsonTableOrdinalityColumn {
                    name: build::ident("ordinal", false),
                    span: Span::new(230, 240),
                },
            )],
            None,
            None,
            Span::new(205, 285),
        )
        .unwrap();
        let cte_query = projection_query(
            build::integer(1),
            vec![
                missing_table("missing_inner_table"),
                ast::Relation::JsonTable(ast::AstBox::new({
                    let mut table = table;
                    table.span = factor_span;
                    table
                })),
            ],
        );
        let mut query = projection_query(
            build::integer(1),
            vec![missing_table("missing_outer_table")],
        );
        query.with = Some(ast::With {
            recursive: false,
            ctes: vec![ast::Cte {
                name: build::ident("unused_json", false),
                columns: None,
                query: ast::AstBox::new(cte_query),
                span: Span::new(150, 295),
            }]
            .into(),
            span: Span::new(130, 295),
        });
        let error = crate::analyzer::analyze(&query, &NO_TABLES, "default").unwrap_err();
        assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
        assert_eq!(error.span(), Some(factor_span));
        assert!(error.message().contains("JSON_TABLE"));
    }

    fn object(members: Vec<(ast::Expr, ast::Expr)>) -> ast::JsonObject {
        ast::JsonObject {
            members: members
                .into_iter()
                .map(|(key, value)| ast::JsonObjectMember {
                    key,
                    value,
                    format: None,
                    span: Span::new(20, 90),
                })
                .collect(),
            null_on_null: true,
            unique_keys: false,
            returned_type: None,
            output_format: None,
            span: Span::new(10, 100),
        }
    }

    fn array(values: Vec<ast::Expr>, null_on_null: bool) -> ast::JsonArray {
        ast::JsonArray {
            elements: values
                .into_iter()
                .map(|value| ast::JsonArrayElement {
                    value,
                    format: None,
                    span: Span::new(120, 150),
                })
                .collect(),
            null_on_null,
            returned_type: None,
            output_format: None,
            span: Span::new(110, 160),
        }
    }

    fn ordinary_constructor(name: &str, arguments: Vec<ast::Expr>, span: Span) -> ast::Expr {
        let mut expression = build::function(
            build::object_name(vec![build::ident(name, false)]).unwrap(),
            arguments,
        );
        let ast::Expr::FunctionCall(call) = &mut expression else {
            panic!("function fixture")
        };
        call.span = span;
        expression
    }

    fn assert_same_refusal(actual: &AnalyzeError, expected: &AnalyzeError) {
        assert_eq!(actual.kind(), expected.kind());
        assert_eq!(actual.code(), expected.code());
        assert_eq!(actual.span(), expected.span());
        assert_eq!(actual.message(), expected.message());
        assert_eq!(actual.control_error(), expected.control_error());
        assert_eq!(
            std::error::Error::source(actual).map(ToString::to_string),
            std::error::Error::source(expected).map(ToString::to_string),
        );
    }

    #[test]
    fn object_whole_query_preserves_null_key_value_duplicates_and_nested_json_owner() {
        let nested = object(vec![(build::string("inner"), build::integer(3))]);
        let node = object(vec![
            (build::string("z"), build::null()),
            (build::null(), build::integer(7)),
            (build::string("duplicate"), build::string("{\"raw\":1}")),
            (
                build::string("duplicate"),
                ast::Expr::JsonObject(Box::new(nested)),
            ),
        ]);
        assert!(is_supported_json_object_profile(&node));
        let original = node.clone();
        let ordinary = ordinary_constructor(
            "json_object",
            vec![
                build::string("z"),
                build::null(),
                build::null(),
                build::integer(7),
                build::string("duplicate"),
                build::string("{\"raw\":1}"),
                build::string("duplicate"),
                ordinary_constructor(
                    "json_object",
                    vec![build::string("inner"), build::integer(3)],
                    Span::new(10, 100),
                ),
            ],
            node.span,
        );
        let actual_query = projection_query(ast::Expr::JsonObject(Box::new(node)), vec![]);
        let expected_query = projection_query(ordinary, vec![]);
        let (actual, _, _) =
            crate::analyzer::analyze(&actual_query, &NO_TABLES, "default").unwrap();
        let (expected, _, _) =
            crate::analyzer::analyze(&expected_query, &NO_TABLES, "default").unwrap();
        let actual = first_projection(&actual);
        assert_same_typed(actual, first_projection(&expected));
        let ExprKind::FunctionCall { binding, args, .. } = &actual.kind else {
            panic!("scalar")
        };
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/json_object/v1"
        );
        assert_eq!(
            binding.semantics.argument_evaluation,
            novarocks_functions::FunctionArgumentEvaluation::Eager
        );
        assert_eq!(actual.value_type.logical_type(), ValueLogicalType::Json);
        assert_eq!(args.len(), 8);
        assert!(matches!(
            args[1].kind,
            ExprKind::Literal(LiteralValue::Null)
        ));
        assert!(matches!(
            args[2].kind,
            ExprKind::Literal(LiteralValue::Null)
        ));
        assert!(
            matches!(&args[4].kind, ExprKind::Literal(LiteralValue::String(value)) if value == "duplicate")
        );
        assert!(
            matches!(&args[6].kind, ExprKind::Literal(LiteralValue::String(value)) if value == "duplicate")
        );
        assert_eq!(args[7].value_type.logical_type(), ValueLogicalType::Json);
        let ast::SetExpr::Select(select) = actual_query.body.as_ref() else {
            panic!("select")
        };
        let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
            panic!("object")
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let ast::Expr::JsonObject(after) = &projection_column.expression else {
            panic!("object")
        };
        assert_eq!(after.as_ref(), &original);
        // The original owner decides SQL-NULL keys, JSON text embedding and
        // numeric encoding. This syntax consumer preserves its exact arguments.
    }

    #[test]
    fn empty_object_keeps_the_original_zero_argument_binding() {
        let control = SqlCompileControl::unbounded();
        let ctx = context(&control);
        let scope = ctx.new_scope();
        let node = object(vec![]);
        assert!(is_supported_json_object_profile(&node));
        let actual = ctx.analyze_json_object_syntax(&node, &scope).unwrap();
        let expected = ctx
            .analyze_expr(
                &ordinary_constructor("json_object", vec![], node.span),
                &scope,
            )
            .unwrap();
        assert_same_typed(&actual, &expected);
        let ExprKind::FunctionCall { args, binding, .. } = &actual.kind else {
            panic!("canonical function")
        };
        assert!(args.is_empty());
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/json_object/v1"
        );
    }

    #[test]
    fn array_expressible_profiles_preserve_original_arity_or_unavailable_result() {
        let control = SqlCompileControl::unbounded();
        let ctx = context(&control);
        let scope = ctx.new_scope();
        for node in [
            array(vec![], false),
            array(
                vec![
                    build::null(),
                    build::integer(2),
                    build::string("{\"raw\":1}"),
                ],
                true,
            ),
            array(
                vec![ast::Expr::JsonObject(Box::new(object(vec![(
                    build::string("k"),
                    build::integer(1),
                )])))],
                true,
            ),
        ] {
            assert!(is_supported_json_array_profile(&node));
            let ordinary = ordinary_constructor(
                "json_array",
                node.elements
                    .iter()
                    .map(|element| element.value.clone())
                    .collect(),
                node.span,
            );
            let actual = ctx.analyze_json_array_syntax(&node, &scope).unwrap_err();
            let expected = ctx.analyze_expr(&ordinary, &scope).unwrap_err();
            assert_same_refusal(&actual, &expected);
            if !node.elements.is_empty() {
                assert_eq!(actual.kind(), AnalyzeErrorKind::UnsupportedExpression);
                assert!(
                    actual
                        .message()
                        .contains("no admitted selected scalar implementation")
                );
            }
        }
    }

    #[test]
    fn array_original_operands_are_analyzed_in_order_before_catalog_availability() {
        let control = SqlCompileControl::unbounded();
        let ctx = context(&control);
        let scope = ctx.new_scope();
        let first =
            ast::Expr::Identifier(build::ident_at("first_missing", false, Span::new(120, 130)));
        let second = ast::Expr::Identifier(build::ident_at(
            "second_missing",
            false,
            Span::new(140, 150),
        ));
        for (arguments, error_span) in [
            (vec![first.clone(), second.clone()], first.span()),
            (vec![second, first], Span::new(140, 150)),
        ] {
            let node = array(arguments.clone(), true);
            let actual = ctx.analyze_json_array_syntax(&node, &scope).unwrap_err();
            let expected = ctx
                .analyze_expr(
                    &ordinary_constructor("json_array", arguments, node.span),
                    &scope,
                )
                .unwrap_err();
            assert_same_refusal(&actual, &expected);
            assert_eq!(actual.kind(), AnalyzeErrorKind::UnknownColumn);
            assert_eq!(actual.span(), Some(error_span));
        }
    }

    #[test]
    fn nondefault_object_and_array_profiles_reject_before_operands_or_discovery() {
        let unknown = ast::Expr::Identifier(build::ident("unknown_constructor_operand", false));
        let base = object(vec![(unknown.clone(), unknown.clone())]);
        let mut objects = Vec::new();
        let mut node = base.clone();
        node.null_on_null = false;
        objects.push(node);
        let mut node = base.clone();
        node.unique_keys = true;
        objects.push(node);
        let mut node = base.clone();
        node.returned_type = Some(build::simple_type("VARCHAR"));
        objects.push(node);
        let mut node = base.clone();
        node.output_format = Some(ast::JsonFormat::Json);
        objects.push(node);
        for format in [ast::JsonFormat::Json, ast::JsonFormat::Utf16] {
            let mut node = base.clone();
            node.members[0].format = Some(format);
            objects.push(node);
        }
        let control = SqlCompileControl::unbounded();
        let ctx = context(&control);
        let scope = ctx.new_scope();
        for node in objects {
            assert!(!is_supported_json_object_profile(&node));
            let error = ctx.analyze_json_object_syntax(&node, &scope).unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
            assert_eq!(error.span(), Some(node.span));
            let span = node.span;
            let query = projection_query(
                ast::Expr::JsonObject(Box::new(node)),
                vec![missing_table("must_not_discover_object")],
            );
            let error = crate::analyzer::analyze(&query, &NO_TABLES, "default").unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
            assert_eq!(error.span(), Some(span));
        }
        let base = array(vec![unknown], true);
        let mut arrays = Vec::new();
        let mut node = base.clone();
        node.null_on_null = false;
        arrays.push(node);
        let mut node = base.clone();
        node.returned_type = Some(build::simple_type("VARCHAR"));
        arrays.push(node);
        let mut node = base.clone();
        node.output_format = Some(ast::JsonFormat::Json);
        arrays.push(node);
        for format in [ast::JsonFormat::Json, ast::JsonFormat::Utf32] {
            let mut node = base.clone();
            node.elements[0].format = Some(format);
            arrays.push(node);
        }
        arrays.push(array(vec![], true));
        for node in arrays {
            assert!(!is_supported_json_array_profile(&node));
            let error = ctx.analyze_json_array_syntax(&node, &scope).unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
            assert_eq!(error.span(), Some(node.span));
            let span = node.span;
            let query = projection_query(
                ast::Expr::JsonArray(Box::new(node)),
                vec![missing_table("must_not_discover_array")],
            );
            let error = crate::analyzer::analyze(&query, &NO_TABLES, "default").unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
            assert_eq!(error.span(), Some(span));
        }
    }

    fn projection_syntax(query: &ast::Query) -> &ast::Expr {
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("SELECT fixture")
        };
        let [ast::SelectItem::SingleColumn(projection_column)] = select.projection.as_slice()
        else {
            panic!("one projection fixture")
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let expression = &projection_column.expression;
        expression
    }

    #[test]
    fn object_original_children_keep_constant_backing_ordinal_and_write_assignment_loan() {
        use crate::compiler::SqlWriteAssignmentSite;
        use crate::compiler::materialized_values::MaterializedSourceBorrow;
        use arrow::array::{Array, StringArray};
        use novarocks_functions::ConstantPool;
        use novarocks_type_contract::CompilePhase;
        use std::{borrow::Cow, sync::Arc};

        let control = SqlCompileControl::unbounded();
        let policy = crate::constant::test_constant_policy();
        let constant_type = FunctionValueType::new(DataType::Utf8, false);
        let field = Arc::new(arrow::datatypes::Field::new(
            "original_json_input",
            DataType::Utf8,
            false,
        ));
        let pool = ConstantPool::try_new(
            field.clone(),
            constant_type,
            StringArray::from(vec!["unused ordinal", "{\"retained\":true}"]).to_data(),
            policy,
            CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let value = pool.value(1).unwrap();
        let placeholder = ast::Expr::Identifier(build::ident_at(
            "only_materialized_source_can_resolve",
            false,
            Span::new(40, 49),
        ));
        let assigned = ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Long {
                spelling: "1".into(),
                parsed_value: 1,
            },
            span: Span::new(65, 66),
        });
        let node = object(vec![
            (build::string("materialized"), placeholder.clone()),
            (build::string("assigned"), assigned.clone()),
        ]);
        let object_span = node.span;
        let actual_query = projection_query(ast::Expr::JsonObject(Box::new(node)), vec![]);
        let expected_query = projection_query(
            ordinary_constructor(
                "json_object",
                vec![
                    build::string("materialized"),
                    placeholder,
                    build::string("assigned"),
                    assigned,
                ],
                object_span,
            ),
            vec![],
        );
        let ast::Expr::JsonObject(actual_node) = projection_syntax(&actual_query) else {
            panic!("object fixture")
        };
        let ast::Expr::FunctionCall(expected_node) = projection_syntax(&expected_query) else {
            panic!("ordinary fixture")
        };
        let assignment = SqlWriteAssignmentSite::try_new(
            0,
            FunctionValueType::new(DataType::Int64, false),
            None,
            &control,
        )
        .unwrap()
        .with_source_span(Span::new(65, 66));
        let mut actual_source = MaterializedSourceBorrow::empty(&actual_query);
        actual_source
            .insert_context_constant(&actual_node.members[0].value, value.clone(), &control)
            .unwrap();
        actual_source
            .insert_write_assignment(
                &actual_node.members[1].value,
                Cow::Owned(assignment.clone()),
                &control,
            )
            .unwrap();
        let mut expected_source = MaterializedSourceBorrow::empty(&expected_query);
        expected_source
            .insert_context_constant(&expected_node.arguments[1].value, value.clone(), &control)
            .unwrap();
        expected_source
            .insert_write_assignment(
                &expected_node.arguments[3].value,
                Cow::Owned(assignment),
                &control,
            )
            .unwrap();

        let mut actual_context = context(&control);
        actual_context.materialized_values = Some(actual_source.view());
        let actual_scope = actual_context.new_scope();
        let actual = actual_context
            .analyze_expr(projection_syntax(&actual_query), &actual_scope)
            .unwrap();
        let mut expected_context = context(&control);
        expected_context.materialized_values = Some(expected_source.view());
        let expected_scope = expected_context.new_scope();
        let expected = expected_context
            .analyze_expr(projection_syntax(&expected_query), &expected_scope)
            .unwrap();
        assert_same_typed(&actual, &expected);
        let ExprKind::FunctionCall { args, .. } = &actual.kind else {
            panic!("scalar call")
        };
        let ExprKind::Constant(retained) = &args[1].kind else {
            panic!("original constant must survive")
        };
        assert_eq!(retained.pool().backing_identity(), pool.backing_identity());
        assert_eq!(retained.ordinal(), 1);
        assert_eq!(retained.field(), field.as_ref());
        assert_eq!(retained.value_type(), value.value_type());
        assert_eq!(args[3].value_type.data_type(), &DataType::Int64);
        assert!(matches!(args[3].kind, ExprKind::Cast { .. }));
        // Equal source text at the same span is deliberately not this loan.
        assert!(
            actual_source
                .view()
                .constant_at(&actual_node.members[0].value.clone())
                .is_none()
        );

        // An unavailable array implementation must not hide a lost original
        // constant loan behind an earlier UnknownColumn from a cloned child.
        let array = array(
            vec![ast::Expr::Identifier(build::ident(
                "array_materialized_input",
                false,
            ))],
            true,
        );
        let array_span = array.span;
        let array_query = projection_query(ast::Expr::JsonArray(Box::new(array)), vec![]);
        let ordinary_query = projection_query(
            ordinary_constructor(
                "json_array",
                vec![ast::Expr::Identifier(build::ident(
                    "array_materialized_input",
                    false,
                ))],
                array_span,
            ),
            vec![],
        );
        let ast::Expr::JsonArray(array) = projection_syntax(&array_query) else {
            panic!("array fixture")
        };
        let ast::Expr::FunctionCall(ordinary) = projection_syntax(&ordinary_query) else {
            panic!("ordinary fixture")
        };
        let mut array_source = MaterializedSourceBorrow::empty(&array_query);
        array_source
            .insert_context_constant(&array.elements[0].value, value.clone(), &control)
            .unwrap();
        let mut ordinary_source = MaterializedSourceBorrow::empty(&ordinary_query);
        ordinary_source
            .insert_context_constant(&ordinary.arguments[0].value, value, &control)
            .unwrap();
        let mut array_context = context(&control);
        array_context.materialized_values = Some(array_source.view());
        let array_scope = array_context.new_scope();
        let actual = array_context
            .analyze_expr(projection_syntax(&array_query), &array_scope)
            .unwrap_err();
        let mut ordinary_context = context(&control);
        ordinary_context.materialized_values = Some(ordinary_source.view());
        let ordinary_scope = ordinary_context.new_scope();
        let expected = ordinary_context
            .analyze_expr(projection_syntax(&ordinary_query), &ordinary_scope)
            .unwrap_err();
        assert_same_refusal(&actual, &expected);
        assert_eq!(actual.kind(), AnalyzeErrorKind::UnsupportedExpression);
        assert!(
            actual
                .message()
                .contains("no admitted selected scalar implementation")
        );
    }

    #[test]
    fn constructor_dispatch_keeps_the_original_request_cancellation() {
        struct Cancelled;
        impl crate::compiler::SqlCancellationObservation for Cancelled {
            fn is_cancelled(&self) -> bool {
                true
            }
        }
        let control = SqlCompileControl::new(None, std::sync::Arc::new(Cancelled));
        let ctx = context(&control);
        let scope = ctx.new_scope();
        let object = object(vec![(build::string("k"), build::null())]);
        let array = array(vec![build::null()], true);
        for (actual, ordinary) in [
            (
                ast::Expr::JsonObject(Box::new(object.clone())),
                ordinary_constructor(
                    "json_object",
                    vec![build::string("k"), build::null()],
                    object.span,
                ),
            ),
            (
                ast::Expr::JsonArray(Box::new(array.clone())),
                ordinary_constructor("json_array", vec![build::null()], array.span),
            ),
        ] {
            let actual = ctx.analyze_expr(&actual, &scope).unwrap_err();
            let expected = ctx.analyze_expr(&ordinary, &scope).unwrap_err();
            assert_same_refusal(&actual, &expected);
            assert_eq!(
                actual.control_error(),
                Some(novarocks_type_contract::CompileControlError::Cancelled)
            );
        }
    }
    #[test]
    fn typed_json_typeof_keeps_the_original_logical_hint_and_clause_refusals() {
        let control = SqlCompileControl::unbounded();
        let ctx = context(&control);
        let scope = ctx.new_scope();
        let unknown = || build::identifier(build::ident("must_not_resolve", false));
        let object_node = object(vec![(build::string("k"), unknown())]);
        let array_node = array(vec![unknown()], true);
        for (typed, ordinary) in [
            (
                ast::Expr::JsonObject(Box::new(object_node.clone())),
                ordinary_constructor(
                    "json_object",
                    vec![build::string("k"), unknown()],
                    object_node.span,
                ),
            ),
            (
                ast::Expr::JsonArray(Box::new(array_node.clone())),
                ordinary_constructor("json_array", vec![unknown()], array_node.span),
            ),
        ] {
            let actual = ctx
                .analyze_expr(
                    &ordinary_constructor("typeof", vec![typed], Span::new(0, 90)),
                    &scope,
                )
                .unwrap();
            let expected = ctx
                .analyze_expr(
                    &ordinary_constructor("typeof", vec![ordinary], Span::new(0, 90)),
                    &scope,
                )
                .unwrap();
            assert_same_typed(&actual, &expected);
            assert!(
                matches!(actual.kind, ExprKind::Literal(LiteralValue::String(ref value)) if value == "json")
            );
        }
        let mut unsupported_object = object_node;
        unsupported_object.unique_keys = true;
        let unsupported_array = array(vec![unknown()], false);
        for typed in [
            ast::Expr::JsonObject(Box::new(unsupported_object)),
            ast::Expr::JsonArray(Box::new(unsupported_array)),
        ] {
            let error = ctx
                .analyze_expr(
                    &ordinary_constructor("typeof", vec![typed], Span::new(0, 90)),
                    &scope,
                )
                .unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
        }
    }

    fn opaque_dynamic_json_call(quoted: bool) -> ast::Expr {
        let input = ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Generic(ast::GenericLiteral {
                type_name: "JSON".to_string(),
                value: "{\"a\":1}".to_string(),
            }),
            span: novarocks_parser::SYNTHETIC,
        });
        let path = ordinary_constructor(
            "concat",
            vec![build::string("$."), build::string("a")],
            novarocks_parser::SYNTHETIC,
        );
        let mut call =
            ordinary_constructor("json_query", vec![input, path], novarocks_parser::SYNTHETIC);
        let ast::Expr::FunctionCall(function) = &mut call else {
            panic!("ordinary call");
        };
        function.name.parts[0].quoted = quoted;
        call
    }

    #[derive(Clone, Debug)]
    struct ObservedPresenceCatalog {
        original: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
        scalar_resolutions: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl crate::compiler::SqlFunctionCatalog for ObservedPresenceCatalog {
        fn snapshot(&self) -> std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog> {
            std::sync::Arc::new(self.clone())
        }
        fn resolve_scalar_signature(
            &self,
            name: &str,
            args: &[DataType],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionSignature,
            novarocks_functions::FunctionResolutionError,
        > {
            self.scalar_resolutions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.original.resolve_scalar_signature(name, args, control)
        }
        fn resolve_scalar_binding(
            &self,
            name: &str,
            args: &[novarocks_functions::FunctionArgument],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionBinding,
            novarocks_functions::FunctionBindingError,
        > {
            self.scalar_resolutions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.original.resolve_scalar_binding(name, args, control)
        }
        fn contains_aggregate(&self, name: &str) -> bool {
            self.original.contains_aggregate(name)
        }
        fn resolve_aggregate_signature(
            &self,
            name: &str,
            args: &[DataType],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedAggregateSignature,
            novarocks_functions::FunctionResolutionError,
        > {
            self.original
                .resolve_aggregate_signature(name, args, control)
        }
        fn resolve_aggregate_trusted(
            &self,
            name: &str,
            args: &[DataType],
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedAggregateSignature,
            novarocks_functions::FunctionResolutionError,
        > {
            self.original.resolve_aggregate_trusted(name, args, control)
        }
        fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility {
            self.original.volatility(name)
        }
    }

    #[test]
    fn typeof_cast_declaration_keeps_opaque_ordinary_and_quoted_children_unbound() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let control = SqlCompileControl::unbounded();
        let resolutions = Arc::new(AtomicUsize::new(0));
        let functions = ObservedPresenceCatalog {
            original: crate::functions::builtin_sql_function_catalog()
                .snapshot_for_scalar_presence(),
            scalar_resolutions: resolutions.clone(),
        };
        let mut ctx = context(&control);
        ctx.function_catalog = &functions;
        let scope = ctx.new_scope();
        for quoted in [false, true] {
            let child = opaque_dynamic_json_call(quoted);
            let statements =
                novarocks_parser::parse("SELECT typeof(CAST(NULL AS INTEGER))").unwrap();
            let [ast::Statement::Query(query)] = statements.as_slice() else {
                panic!("query");
            };
            let ast::SetExpr::Select(select) = query.body.as_ref() else {
                panic!("select");
            };
            let ast::SelectItem::SingleColumn(projection_column) = select.projection[0].clone()
            else {
                panic!("typeof");
            };
            assert!(projection_column.alias.is_none(), "anonymous projection");
            let mut expression = projection_column.into_inner().expression;
            let ast::Expr::FunctionCall(call) = &mut expression else {
                panic!("typeof call");
            };
            let ast::Expr::Cast(cast) = &mut call.arguments[0].value else {
                panic!("declared cast");
            };
            cast.expression = child.clone();
            resolutions.store(0, Ordering::SeqCst);
            let actual = ctx.analyze_expr(&expression, &scope).unwrap();
            assert_eq!(
                actual.value_type,
                FunctionValueType::new(DataType::Utf8, false)
            );
            assert!(
                matches!(&actual.kind, ExprKind::Literal(LiteralValue::String(value)) if value == "integer")
            );
            assert_eq!(
                resolutions.load(Ordering::SeqCst),
                0,
                "the original declaration shortcut must not bind its opaque child"
            );
            let direct = ctx.analyze_expr(&child, &scope).unwrap_err();
            assert!(
                resolutions.load(Ordering::SeqCst) > 0,
                "the observation must see actual child resolution"
            );
            if !quoted {
                assert_eq!(direct.kind(), AnalyzeErrorKind::UnavailableImplementation);
            }
        }
    }

    #[test]
    fn full_compiler_typeof_json_keeps_original_scalar_presence_refusal() {
        use crate::compiler::{
            SessionOptimizerSettings, SqlAnalyzeRequest, SqlCompileError, SqlCompileIntent,
            SqlCompiler, SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext,
            SqlStatementInput,
        };
        let span = novarocks_parser::SYNTHETIC;
        let literal_query =
            ast::Expr::JsonQuery(ast::AstBox::new(plain(build::string("{\"a\":1}"), "$.a")));
        let mut ordinary_literal_query = opaque_dynamic_json_call(false);
        let ast::Expr::FunctionCall(call) = &mut ordinary_literal_query else {
            panic!("ordinary call");
        };
        call.arguments[1].value = build::string("$.a");
        for child in [
            opaque_dynamic_json_call(false),
            ordinary_literal_query,
            literal_query,
        ] {
            let query = projection_query(ordinary_constructor("typeof", vec![child], span), vec![]);
            let catalog = SqlPlannerTableSnapshot::new(&NO_TABLES);
            let functions = crate::functions::builtin_sql_function_catalog();
            let result = SqlCompiler::analyze(SqlAnalyzeRequest::new(
                SqlStatementInput::parsed_query(Box::new(query)),
                SqlCompileIntent::AnalyzeOnly,
                SqlSessionContext {
                    current_catalog: None,
                    current_database: "default".to_string(),
                    optimizer_settings: SessionOptimizerSettings::default(),
                    sql_semantics: Default::default(),
                },
                SqlPlanningEnvironment::Distributed,
                &catalog,
                functions,
                crate::compiler::noop_constant_evaluator(),
                None,
                crate::constant::test_constant_policy(),
                SqlCompileControl::unbounded(),
            ));
            let error = match result {
                Err(SqlCompileError::Analyze(error)) => error,
                _ => panic!("full scalar presence must refuse the unavailable JSON query owner"),
            };
            assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
            assert_eq!(
                error.code().as_str(),
                "sql.analyze.unavailable_implementation"
            );
            assert!(error.message().contains("json_query"), "{error}");
        }
    }
}
