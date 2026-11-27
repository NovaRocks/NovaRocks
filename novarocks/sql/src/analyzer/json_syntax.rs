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

use arrow::datatypes::DataType;
use novarocks_expression::typing::NodeShape;
use novarocks_functions::builtin::json_sql::{JsonEncoding, JsonFunction, is_json, is_text};
use novarocks_parser::ast;
use novarocks_type_contract::{FunctionValueType, TrinoCastMode, ValueKind};

use crate::analysis::{ExprKind, TypedExpr};
use crate::analyze_error::AnalyzeError;

use super::AnalyzerContext;
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

impl AnalyzerContext<'_> {
    pub(super) fn analyze_json_constructor_syntax(
        &self,
        node: &ast::JsonConstructor,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let source = self.analyze_expr(&node.expression, scope)?;
        self.read_json_input(source, node.format, false, node.expression.span())
    }

    pub(super) fn analyze_json_serialize_syntax(
        &self,
        node: &ast::JsonSerialize,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let source = self.analyze_expr(&node.expression, scope)?;
        let source = self.read_json_input(
            source,
            node.input_format,
            node.error_behavior == ast::JsonSerializeErrorBehavior::Null,
            node.expression.span(),
        )?;
        self.write_json_output(
            source,
            node.returned_type.as_ref(),
            node.output_format,
            false,
            node.span,
        )
    }

    pub(super) fn analyze_json_object_syntax(
        &self,
        node: &ast::JsonObject,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for member in &node.members {
            self.check_control()?;
            let key = self.analyze_expr(&member.key, scope)?;
            if !is_text(&key.value_type) {
                return Err(AnalyzeError::invalid_function_argument(
                    "JSON_OBJECT key must be a character string",
                    member.key.span(),
                ));
            }
            keys.push(key);
            values.push(self.analyze_json_member(&member.value, member.format, scope)?);
        }
        // Trino evaluates the key row before the value row. Preserve
        // each group's original order when lowering to selected arguments.
        keys.extend(values);
        let source = self.bind_json_call(
            JsonFunction::Object {
                null_on_null: node.null_on_null,
                unique: node.unique_keys,
            },
            keys,
            node.span,
        )?;
        self.write_json_output(
            source,
            node.returned_type.as_ref(),
            node.output_format,
            true,
            node.span,
        )
    }

    pub(super) fn analyze_json_array_syntax(
        &self,
        node: &ast::JsonArray,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let mut args = Vec::new();
        for element in &node.elements {
            self.check_control()?;
            args.push(self.analyze_json_member(&element.value, element.format, scope)?);
        }
        let source = self.bind_json_call(
            JsonFunction::Array {
                null_on_null: node.null_on_null,
            },
            args,
            node.span,
        )?;
        self.write_json_output(
            source,
            node.returned_type.as_ref(),
            node.output_format,
            true,
            node.span,
        )
    }

    fn analyze_json_member(
        &self,
        expr: &ast::Expr,
        format: Option<ast::JsonFormat>,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        let mut source = self.analyze_expr(expr, scope)?;
        if let Some(format) = format.or_else(|| {
            (is_json(&source.value_type)
                || matches!(
                    expr,
                    ast::Expr::JsonObject(_) | ast::Expr::JsonArray(_) | ast::Expr::JsonQuery(_)
                ))
            .then_some(ast::JsonFormat::Json)
        }) {
            return self.read_json_input(source, format, false, expr.span());
        }
        if !is_text(&source.value_type)
            && !matches!(
                source.value_type.value_type().kind(),
                ValueKind::Scalar(
                    DataType::Null
                        | DataType::Boolean
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Float32
                        | DataType::Float64
                        | DataType::Decimal128(..)
                )
            )
        {
            // The constructor has its own implicit text conversion for
            // nonnumeric scalars, including UUID and datetime.
            if matches!(source.value_type.value_type().kind(), ValueKind::Binary)
                || !matches!(
                    source.value_type.value_type().kind(),
                    ValueKind::Nominal(novarocks_type_contract::ValueLogicalType::Uuid)
                        | ValueKind::Temporal(_)
                        | ValueKind::Scalar(DataType::Date32)
                )
            {
                return Err(AnalyzeError::unsupported_expression(
                    "Unsupported type of SQL/JSON constructor member",
                    expr.span(),
                ));
            }
            source = self.cast_with_trino_mode(
                source,
                FunctionValueType::new(DataType::Utf8, true),
                TrinoCastMode::Explicit,
                expr.span(),
            )?;
        }
        self.bind_json_call(JsonFunction::Scalar, vec![source], expr.span())
    }

    fn read_json_input(
        &self,
        source: TypedExpr,
        format: ast::JsonFormat,
        null_on_error: bool,
        span: novarocks_parser::Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let ty = &source.value_type;
        let operation = if format == ast::JsonFormat::Json && is_json(ty) {
            return Ok(source);
        } else if format == ast::JsonFormat::Json
            && (is_text(ty) || ty.data_type() == &DataType::Null)
        {
            JsonFunction::ReadText { null_on_error }
        } else if (ty.value_type().kind() == &ValueKind::Binary
            && ty.data_type() == &DataType::Binary)
            || ty.data_type() == &DataType::Null
        {
            JsonFunction::ReadBinary {
                encoding: json_encoding(format),
                null_on_error,
            }
        } else {
            return Err(AnalyzeError::type_mismatch(
                format!(
                    "Cannot read input of type {:?} with JSON format {format:?}",
                    ty.value_type().kind()
                ),
                span,
            ));
        };
        self.bind_json_call(operation, vec![source], span)
    }

    fn write_json_output(
        &self,
        source: TypedExpr,
        declaration: Option<&ast::TypeName>,
        format: Option<ast::JsonFormat>,
        allows_json: bool,
        span: novarocks_parser::Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let target = declaration
            .map(|decl| {
                super::helpers::sql_type_to_value_type(
                    decl,
                    source.value_type.nullable(),
                    self.control,
                )
            })
            .transpose()?
            .unwrap_or_else(|| {
                FunctionValueType::new(DataType::Utf8, source.value_type.nullable())
            });
        let format = format.unwrap_or(ast::JsonFormat::Json);
        if allows_json && format == ast::JsonFormat::Json && is_json(&target) {
            return Ok(source);
        }
        if target.value_type().kind() == &ValueKind::Binary
            && target.data_type() == &DataType::Binary
        {
            return self.bind_json_call(
                JsonFunction::SerializeBinary(json_encoding(format)),
                vec![source],
                span,
            );
        }
        if format != ast::JsonFormat::Json || !is_text(&target) {
            return Err(AnalyzeError::type_mismatch(
                format!(
                    "Cannot output JSON as {:?} with format {format:?}",
                    target.value_type().kind()
                ),
                span,
            ));
        }
        let text = self.bind_json_call(JsonFunction::Format, vec![source], span)?;
        if text.value_type == target {
            return Ok(text);
        }
        self.cast_with_trino_mode(text, target, TrinoCastMode::Explicit, span)
    }

    fn bind_json_call(
        &self,
        operation: JsonFunction,
        arguments: Vec<TypedExpr>,
        span: novarocks_parser::Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let call = self.type_scalar_call(
            NodeShape::TrustedCall {
                function: &operation.function_id(),
            },
            arguments,
            span,
        )?;
        Ok(TypedExpr {
            value_type: call.value_type().clone(),
            kind: ExprKind::FunctionCall {
                name: operation.name().to_string(),
                args: call.args.into(),
                volatility: call.binding.semantics.volatility,
                binding: call.binding,
                distinct: false,
            },
        })
    }

    /// Enter the original scalar-function analysis owner, including its arity,
    /// type validation, catalog availability, binding, and value-domain rules.
    /// Child syntax keeps its actual original occurrence, including the pointer
    /// identity used by source constants and signed write assignments. No SQL
    /// text or selected binding is manufactured by this typed lowering.
    fn analyze_json_scalar_call(
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
        // JsonPathString already owns the original StringLiteral's decoded
        // payload and location. Reuse the ordinary literal analyzer unchanged.
        let path = ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::String(invocation.json_path.value.clone()),
            span: invocation.json_path.span,
        });
        self.analyze_json_scalar_call(
            "json_query",
            &[&invocation.input_expression, &path],
            invocation.json_path.span,
            scope,
        )
    }
}

fn json_encoding(format: ast::JsonFormat) -> JsonEncoding {
    match format {
        ast::JsonFormat::Json | ast::JsonFormat::Utf8 => JsonEncoding::Utf8,
        ast::JsonFormat::Utf16 => JsonEncoding::Utf16,
        ast::JsonFormat::Utf32 => JsonEncoding::Utf32,
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
            purpose: crate::analyzer::QueryAnalysisPurpose::Ordinary,
            effects: Default::default(),
            grouping_scope: Default::default(),
            view_stack: Default::default(),
            control,
            constant_policy: crate::constant::test_constant_policy(),
            materialized_values: None,
            statement_relation: None,
            prepared_parameters: None,
            table_sequence: None,
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
                    cast_mode: actual_mode,
                    expr: actual,
                    target: actual_target,
                    decimal_overflow_policy: actual_policy,
                },
                ExprKind::Cast {
                    cast_mode: expected_mode,
                    expr: expected,
                    target: expected_target,
                    decimal_overflow_policy: expected_policy,
                },
            ) => {
                assert_eq!(actual_mode, expected_mode);
                assert_eq!(actual_target, expected_target);
                assert_eq!(actual_policy, expected_policy);
                assert_same_typed(actual, expected);
            }
            _ => panic!("unexpected argument kinds: {actual:?} versus {expected:?}"),
        }
    }

    fn assert_bound_json_path(argument: &TypedExpr, path: &str) {
        use novarocks_type_contract::{CharacterKind, CharacterType, TrinoCastMode, ValueKind};
        let ExprKind::Cast {
            cast_mode,
            expr,
            target,
            ..
        } = &argument.kind
        else {
            panic!("expected the selected VARCHAR parameter conversion");
        };
        assert_eq!(*cast_mode, Some(TrinoCastMode::Implicit));
        assert_eq!(target, &DataType::Utf8);
        assert_eq!(
            argument.value_type,
            FunctionValueType::new(DataType::Utf8, false)
        );
        assert_eq!(
            expr.value_type.value_type().kind(),
            &ValueKind::BoundedCharacter(
                CharacterType::new(CharacterKind::Varchar, path.chars().count() as u32).unwrap()
            )
        );
        assert!(
            matches!(&expr.kind, ExprKind::Literal(LiteralValue::String(value)) if value == path)
        );
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
                    let input_type = FunctionValueType::try_with_logical_type(
                        if logical == ValueLogicalType::Json {
                            DataType::LargeBinary
                        } else {
                            DataType::Utf8
                        },
                        nullable,
                        logical,
                    )
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
                    assert_bound_json_path(&args[1], "$.a.b");
                    for (arg, selected) in args.iter().zip(&binding.selected.argument_types) {
                        assert_eq!(
                            selected,
                            &FunctionArgumentType::Value(arg.value_type.clone())
                        );
                    }
                    assert_eq!(actual.value_type.data_type(), &DataType::LargeBinary);
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
            build::object_name(vec![build::ident("json_parse", false)]).unwrap(),
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
        assert_bound_json_path(&args[1], path);
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
                build::object_name(vec![build::ident("json_parse", false)]).unwrap(),
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

    #[test]
    fn constructor_clauses_bind_each_original_source_and_exact_output() {
        let control = SqlCompileControl::unbounded();
        let ctx = context(&control);
        let scope = ctx.new_scope();
        let span = novarocks_parser::SYNTHETIC;
        let object = ast::JsonObject {
            members: vec![ast::JsonObjectMember {
                key: build::string("k"),
                value: build::string("[1]"),
                format: Some(ast::JsonFormat::Json),
                span,
            }],
            null_on_null: false,
            unique_keys: true,
            returned_type: Some(build::simple_type("JSON")),
            output_format: None,
            span,
        };
        let value = ctx.analyze_json_object_syntax(&object, &scope).unwrap();
        assert!(is_json(&value.value_type));
        let ExprKind::FunctionCall { binding, args, .. } = &value.kind else {
            panic!("Expected bound SQL/JSON object call");
        };
        assert_eq!(
            binding.function_id,
            JsonFunction::Object {
                null_on_null: false,
                unique: true
            }
            .function_id()
        );
        let ExprKind::FunctionCall {
            binding,
            args: input_args,
            ..
        } = &args[1].kind
        else {
            panic!("Expected selected FORMAT JSON input call");
        };
        assert_eq!(
            binding.function_id,
            JsonFunction::ReadText {
                null_on_error: false
            }
            .function_id()
        );
        assert!(
            matches!(&input_args[0].kind, ExprKind::Literal(LiteralValue::String(value)) if value == "[1]")
        );

        let array = ast::JsonArray {
            elements: vec![ast::JsonArrayElement {
                value: build::null(),
                format: None,
                span,
            }],
            null_on_null: false,
            returned_type: None,
            output_format: None,
            span,
        };
        let result = ctx.analyze_json_array_syntax(&array, &scope).unwrap();
        assert_eq!(result.value_type.data_type(), &DataType::Utf8);
        let ExprKind::FunctionCall { binding, args, .. } = &result.kind else {
            panic!("Expected text output wrapper");
        };
        assert_eq!(binding.function_id, JsonFunction::Format.function_id());
        assert!(is_json(&args[0].value_type));
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
        let array = ast::Expr::JsonArray(ast::AstBox::new(ast::JsonArray {
            elements: Vec::new(),
            null_on_null: false,
            returned_type: None,
            output_format: None,
            span: novarocks_parser::SYNTHETIC,
        }));
        let actual = ctx.analyze_expr(&array, &scope).unwrap_err();
        assert_eq!(
            actual.control_error(),
            Some(novarocks_type_contract::CompileControlError::Cancelled)
        );
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
        fn finish_scalar_binding(
            &self,
            function: &novarocks_functions::FunctionId,
            overload: &novarocks_functions::FunctionOverloadId,
            request: novarocks_functions::FunctionBindingRequest<'_>,
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<
            novarocks_functions::ResolvedFunctionBinding,
            novarocks_functions::FunctionBindingError,
        > {
            self.scalar_resolutions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.original
                .finish_scalar_binding(function, overload, request, control)
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
    fn typeof_cast_preserves_ordinary_and_quoted_child_resolution() {
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
            let query =
                &novarocks_parser::parser::parse_query("SELECT typeof(CAST(NULL AS INTEGER))")
                    .unwrap();
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
            let actual = ctx.analyze_expr(&expression, &scope).unwrap_err();
            assert!(
                resolutions.load(Ordering::SeqCst) > 0,
                "typeof must resolve its child"
            );
            let direct = ctx.analyze_expr(&child, &scope).unwrap_err();
            assert_eq!(actual.kind(), direct.kind());
            assert_eq!(actual.message(), direct.message());
            if !quoted {
                assert_eq!(
                    direct.kind(),
                    AnalyzeErrorKind::UnavailableImplementation,
                    "{direct}"
                );
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
                novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
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
