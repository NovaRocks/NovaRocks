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

//! Closed owner-bound wrapper validation. Selected complete bindings author
//! execution outputs; display provenance cannot alter them.

use super::*;
use novarocks_functions::{FunctionKind, ResolvedFunctionBinding};

fn supported(binding: &ResolvedFunctionBinding) -> bool {
    let id = binding.function_id.as_str();
    if id.starts_with("builtin.window/") && binding.kind != FunctionKind::Window {
        return false;
    }
    matches!(
        id,
        "builtin.scalar/__array_literal/v1"
            | "builtin.aggregate/array_agg/v1"
            | "builtin.aggregate/array_agg_distinct/v1"
            | "builtin.aggregate/map_agg/v1"
            | "builtin.scalar/map/v1"
            | "builtin.scalar/row/v1"
            | "builtin.scalar/named_struct/v1"
            | "builtin.scalar/__struct_subfield/v1"
            | "builtin.scalar/__row_field/v1"
            | "builtin.scalar/__array_element_at/v1"
            | "builtin.scalar/__map_element_at/v1"
            | "builtin.scalar/__map_subscript/v1"
            | "builtin.scalar/element_at/v1"
            | "builtin.scalar/array_min/v1"
            | "builtin.scalar/array_max/v1"
            | "builtin.scalar/map_keys/v1"
            | "builtin.scalar/map_values/v1"
            | "builtin.scalar/array_sort/v1"
            | "builtin.scalar/array_distinct/v1"
            | "builtin.scalar/slice/v1"
            | "builtin.scalar/flatten/v1"
            | "builtin.scalar/repeat/v1"
            | "builtin.scalar/zip/v1"
            | "builtin.scalar/__array_struct_subfield/v1"
            | "builtin.window/first_value/v1"
            | "builtin.window/last_value/v1"
            | "builtin.window/lead/v1"
            | "builtin.window/lag/v1"
    )
}

impl AnalyzerContext<'_> {
    /// The selected complete binding is the only output author. Display
    /// provenance cannot establish a nominal child or alter a selected result.
    pub(in crate::analyzer) fn adapt_bound_output_domains(
        &self,
        expression: TypedExpr,
        _source: Option<&ast::Expr>,
        _scope: &AnalyzerScope,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Validate)
            .map_err(AnalyzeError::control)?;
        let mut original = &expression;
        while let ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) = &original.kind {
            validate_transparent_output_source(original, &mut work, self, span)?;
            original = expr;
        }
        let (binding, args, expected_kind) = match &original.kind {
            ExprKind::FunctionCall { binding, args, .. } => {
                (binding.resolved(), args.as_slice(), FunctionKind::Scalar)
            }
            ExprKind::AggregateCall { resolved, args, .. } => (
                resolved.resolved(),
                args.as_slice(),
                FunctionKind::Aggregate,
            ),
            ExprKind::WindowCall {
                aggregate_binding: Some(binding),
                args,
                ..
            } => (binding.resolved(), args.as_slice(), FunctionKind::Aggregate),
            ExprKind::WindowCall {
                binding,
                args,
                aggregate_binding: None,
                ..
            } => (binding.resolved(), args.as_slice(), FunctionKind::Window),
            _ => {
                work.finish().map_err(AnalyzeError::control)?;
                return Ok(expression);
            }
        };
        if !supported(binding) {
            work.finish().map_err(AnalyzeError::control)?;
            return Ok(expression);
        }
        if binding.kind != expected_kind {
            return Err(AnalyzeError::type_mismatch(
                "wrapper kind differs from its selected callable role",
                span,
            ));
        }
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            return Err(AnalyzeError::type_mismatch(
                "wrapper binding must select a scalar result",
                span,
            ));
        };
        if binding.logical_argument_count != args.len()
            || binding.selected.argument_types.len() != args.len()
            || !result.exactly_equals_checked_observed(&original.value_type, || {
                work.step().map_err(AnalyzeError::control)
            })?
        {
            return Err(AnalyzeError::type_mismatch(
                "wrapper result differs from its complete selected binding",
                span,
            ));
        }
        for (actual, formal) in args.iter().zip(binding.selected.argument_types.iter()) {
            let novarocks_functions::FunctionArgumentType::Value(expected) = formal else {
                return Err(AnalyzeError::type_mismatch(
                    "wrapper argument differs from its selected callable role",
                    span,
                ));
            };
            if !actual
                .value_type
                .fits_physical_binding_observed(expected, || {
                    work.step().map_err(AnalyzeError::control)
                })?
            {
                return Err(AnalyzeError::type_mismatch(
                    "wrapper argument differs from its complete selected binding",
                    span,
                ));
            }
            let mut current = actual;
            while let ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) = &current.kind {
                validate_transparent_output_source(current, &mut work, self, span)?;
                current = expr;
            }
        }
        work.finish().map_err(AnalyzeError::control)?;
        Ok(expression)
    }
}

// Only a physically transparent projection requires domain preservation.
// Explicit conversions change physical ports or enter the selected conversion
// function owner before a carrier Cast. NULL has its own checked value author.
fn validate_transparent_output_source(
    expression: &TypedExpr,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    context: &AnalyzerContext<'_>,
    span: Span,
) -> Result<(), AnalyzeError> {
    let inner = match &expression.kind {
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => expr,
        _ => return Ok(()),
    };
    if expression.value_type.data_type() != inner.value_type.data_type() {
        return Ok(());
    }
    work.flush().map_err(AnalyzeError::control)?;
    if super::super::value_conversion::constant_is_null(
        inner,
        context.constant_policy,
        context.control,
    )? {
        return Ok(());
    }
    if !novarocks_type_contract::preserves_value_domains_observed(
        inner.value_type.value_type(),
        expression.value_type.value_type(),
        |units| {
            for _ in 0..units {
                work.step().map_err(AnalyzeError::control)?;
            }
            Ok::<(), AnalyzeError>(())
        },
    )? {
        return Err(AnalyzeError::type_mismatch(
            "transparent wrapper source changes its original nominal domain",
            span,
        ));
    }
    Ok(())
}
