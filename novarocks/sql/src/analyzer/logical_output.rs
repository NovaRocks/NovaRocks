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

//! SQL semantic output provenance for physically ambiguous scalar values.

use arrow::datatypes::{DataType, Field};
use novarocks_parser::{Span, ast};
use novarocks_types::logical::{LogicalType, field_with_logical_type};
use novarocks_types::schema::SqlType;

use super::{AnalyzerContext, scope::AnalyzerScope};
use crate::analysis::{ExprKind, TypedExpr};
use crate::analyze_error::AnalyzeError;

impl AnalyzerContext<'_> {
    pub(super) fn logical_output_type(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        // A declared target cannot validate arbitrary Utf8. Only an already
        // proven JSON operand keeps its domain through a same-domain cast.
        // Public string-to-JSON conversion belongs to its cast owner.
        if let Some(ast::Expr::Cast(cast)) = source {
            let json_target = cast.data_type.name.parts.last().is_some_and(|part| {
                matches!(part.value.to_ascii_lowercase().as_str(), "json" | "jsonb")
            });
            return match (&expression.kind, json_target) {
                (ExprKind::Cast { expr: inner, .. }, true) => {
                    self.logical_output_type(Some(&cast.expr), inner, scope)
                }
                _ => None,
            };
        }
        let source_binding = match source {
            Some(ast::Expr::Identifier(ident)) => scope.resolve(None, &ident.value).ok(),
            Some(ast::Expr::CompoundIdentifier(parts)) if parts.parts.len() >= 2 => {
                let n = parts.parts.len();
                scope
                    .resolve(Some(&parts.parts[n - 2].value), &parts.parts[n - 1].value)
                    .ok()
            }
            _ => None,
        };
        if let Some((column_id, _, _)) = source_binding {
            return self.factory.borrow().logical_type(column_id);
        }
        match &expression.kind {
            ExprKind::Cast { expr: inner, .. } => self.logical_output_type(source, inner, scope),
            ExprKind::ColumnRef { .. } => scope
                .logical_type_of_expr(expression)
                .filter(|logical| matches!(logical, SqlType::Json)),
            ExprKind::FunctionCall { binding, .. } => {
                crate::functions::scalar_output_logical_type(binding)
            }
            ExprKind::Case {
                when_then,
                else_expr,
                ..
            } => {
                let Some(ast::Expr::Case(case)) = source else {
                    return None;
                };
                let mut saw_json = false;
                for (source, value) in case
                    .results
                    .iter()
                    .zip(when_then.iter().map(|(_, value)| value))
                    .chain(case.else_result.as_deref().zip(else_expr.as_deref()))
                {
                    if matches!(
                        source,
                        ast::Expr::Literal(ast::Literal {
                            kind: ast::LiteralKind::Null,
                            ..
                        })
                    ) {
                        continue;
                    }
                    if self.logical_output_type(Some(source), value, scope) != Some(SqlType::Json) {
                        return None;
                    }
                    saw_json = true;
                }
                saw_json.then_some(SqlType::Json)
            }
            ExprKind::Nested(inner) => self.logical_output_type(
                source.and_then(|source| match source {
                    ast::Expr::Nested(nested) => Some(nested.expression.as_ref()),
                    _ => None,
                }),
                inner,
                scope,
            ),
            _ => None,
        }
    }

    /// Value provenance is separate from a public CAST target's type spelling.
    /// Only catalog JSON, exact JSON producers, and their preserving operators
    /// establish this fact. Derived/CTE columns carry it by ColumnId.
    pub(super) fn json_list_provenance(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> bool {
        if let Some(ast::Expr::Cast(cast)) = source {
            let Some(ast::TypeNameArgument::Type(item)) = cast.data_type.arguments.first() else {
                return false;
            };
            let array_json = cast
                .data_type
                .name
                .parts
                .last()
                .is_some_and(|part| part.value.eq_ignore_ascii_case("array"))
                && item.name.parts.last().is_some_and(|part| {
                    matches!(part.value.to_ascii_lowercase().as_str(), "json" | "jsonb")
                });
            return array_json
                && matches!(&expression.kind,
                ExprKind::Cast { expr: inner, .. } if self.json_list_provenance(Some(&cast.expr), inner, scope));
        }
        let source_id = match source {
            Some(ast::Expr::Identifier(ident)) => scope.resolve(None, &ident.value).ok(),
            Some(ast::Expr::CompoundIdentifier(parts)) if parts.parts.len() >= 2 => {
                let n = parts.parts.len();
                scope
                    .resolve(Some(&parts.parts[n - 2].value), &parts.parts[n - 1].value)
                    .ok()
            }
            _ => None,
        };
        if let Some((column_id, _, _)) = source_id {
            return self.factory.borrow().has_json_list_provenance(column_id);
        }
        match &expression.kind {
            // Internal output adapters and materialized binding coercions do
            // not establish proof. The original source must establish it.
            ExprKind::Cast { expr: inner, .. } => self.json_list_provenance(source, inner, scope),
            ExprKind::ColumnRef { column_id, .. } => {
                self.factory.borrow().has_json_list_provenance(*column_id)
            }
            ExprKind::Nested(inner) => self.json_list_provenance(
                source.and_then(|source| match source {
                    ast::Expr::Nested(nested) => Some(nested.expression.as_ref()),
                    _ => None,
                }),
                inner,
                scope,
            ),
            ExprKind::FunctionCall { binding, args, .. }
                if binding.function_id.as_str() == "builtin.scalar/__array_literal/v1" =>
            {
                let Some(ast::Expr::Array(array)) = source else {
                    return false;
                };
                self.json_array_elements_provenance(array, args, scope)
            }
            ExprKind::FunctionCall { binding, args, .. }
                if binding.function_id.as_str() == "builtin.scalar/array_sortby/v1" =>
            {
                let Some(ast::Expr::FunctionCall(function)) = source else {
                    return false;
                };
                function
                    .arguments
                    .first()
                    .zip(args.first())
                    .is_some_and(|(source, value)| {
                        self.json_list_provenance(Some(source), value, scope)
                    })
            }
            ExprKind::AggregateCall { resolved, args, .. }
                if resolved.function_id.as_str() == "builtin.aggregate/array_agg/v1" =>
            {
                let Some(ast::Expr::FunctionCall(function)) = source else {
                    return false;
                };
                function
                    .arguments
                    .first()
                    .zip(args.first())
                    .is_some_and(|(source, value)| {
                        self.logical_output_type(Some(source), value, scope) == Some(SqlType::Json)
                    })
            }
            ExprKind::WindowCall {
                aggregate_binding: Some(binding),
                args,
                ..
            } if binding.function_id.as_str() == "builtin.aggregate/array_agg/v1" => {
                let Some(ast::Expr::FunctionCall(function)) = source else {
                    return false;
                };
                function
                    .arguments
                    .first()
                    .zip(args.first())
                    .is_some_and(|(source, value)| {
                        self.logical_output_type(Some(source), value, scope) == Some(SqlType::Json)
                    })
            }
            ExprKind::Case {
                when_then,
                else_expr,
                ..
            } => {
                let Some(ast::Expr::Case(case)) = source else {
                    return false;
                };
                let mut saw_json = false;
                for (source, value) in case
                    .results
                    .iter()
                    .zip(when_then.iter().map(|(_, value)| value))
                    .chain(case.else_result.as_deref().zip(else_expr.as_deref()))
                {
                    if matches!(
                        source,
                        ast::Expr::Literal(ast::Literal {
                            kind: ast::LiteralKind::Null,
                            ..
                        })
                    ) {
                        continue;
                    }
                    if !self.json_list_provenance(Some(source), value, scope) {
                        return false;
                    }
                    saw_json = true;
                }
                saw_json
            }
            _ => false,
        }
    }

    pub(super) fn json_array_elements_provenance(
        &self,
        array: &ast::ArrayExpr,
        args: &[TypedExpr],
        scope: &AnalyzerScope,
    ) -> bool {
        if array.element_type.as_ref().is_some_and(|target| {
            !target.name.parts.last().is_some_and(|part| {
                matches!(part.value.to_ascii_lowercase().as_str(), "json" | "jsonb")
            })
        }) {
            return false;
        }
        if array.elements.len() != args.len() {
            return false;
        }
        let mut saw_json = false;
        let all_json = array.elements.iter().zip(args).all(|(source, value)| {
            if matches!(
                source,
                ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::Null,
                    ..
                })
            ) {
                return true;
            }
            let json = self.logical_output_type(Some(source), value, scope) == Some(SqlType::Json);
            saw_json |= json;
            json
        });
        all_json && (saw_json || array.element_type.is_some())
    }

    /// Keep scalar/aggregate bindings exact. The ordinary output CAST adapts
    /// the List item's semantic field metadata. An explicitly typed empty
    /// JSON literal also materializes its Utf8 item carrier without any values.
    pub(super) fn adapt_json_list_output(
        &self,
        expression: TypedExpr,
        json_input: bool,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        if !json_input {
            return Ok(expression);
        }
        let binding = match &expression.kind {
            ExprKind::AggregateCall { resolved, .. } => Some(resolved),
            ExprKind::FunctionCall { binding, .. } => Some(binding),
            ExprKind::WindowCall {
                aggregate_binding, ..
            } => aggregate_binding.as_ref(),
            _ => None,
        };
        if !binding.is_some_and(|binding| {
            matches!(
                binding.function_id.as_str(),
                "builtin.aggregate/array_agg/v1"
                    | "builtin.scalar/__array_literal/v1"
                    | "builtin.scalar/array_sortby/v1"
            )
        }) {
            return Ok(expression);
        }
        let DataType::List(item) = &expression.data_type else {
            return Err(AnalyzeError::type_mismatch(
                "JSON list producer binding must declare a List result",
                span,
            ));
        };
        let empty_json_literal = matches!(&expression.kind,
            ExprKind::FunctionCall { binding, args, .. }
                if binding.function_id.as_str() == "builtin.scalar/__array_literal/v1" && args.is_empty());
        if item.data_type() != &DataType::Utf8
            && !(empty_json_literal && item.data_type() == &DataType::Null)
        {
            return Err(AnalyzeError::type_mismatch(
                "JSON list elements must use their declared Utf8 physical carrier",
                span,
            ));
        }
        let target = DataType::List(std::sync::Arc::new(field_with_logical_type(
            Field::new(item.name(), DataType::Utf8, item.is_nullable()),
            LogicalType::Json,
        )));
        let nullable = expression.nullable;
        Ok(TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(expression),
                target: target.clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            data_type: target,
            nullable,
        })
    }
}
