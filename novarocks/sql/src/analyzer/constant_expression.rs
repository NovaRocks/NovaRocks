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

//! Empty-scope required constants use the ordinary typed and physical authors.

use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    rc::Rc,
};

use arrow::datatypes::DataType;
use novarocks_functions::FunctionVolatility;
use novarocks_parser::ast::Visit;
use novarocks_parser::{Span, ast};
use novarocks_type_contract::{CompilePhase, FunctionValueType, PureCompileControl, ValueKind};

use crate::compiler::{
    PreparedRequiredConstant, RequiredConstantInput, RequiredConstantRequest,
    RequiredConstantTarget, SqlAuthoredScalarUnit, SqlCompileError,
};
use crate::planner::distributed::build::lower_required_constant_unit;
use crate::{
    analysis::{ExprKind, TypedExpr},
    analyze_error::AnalyzeError,
};

fn scalar_context<'a>(
    catalog: &'a dyn crate::compiler::SqlCatalogSnapshot,
    current_database: &'a str,
    functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    sql_semantics: &crate::sql_mode::SqlSemanticSettings,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &'a crate::compiler::SqlCompileControl,
) -> super::AnalyzerContext<'a> {
    super::AnalyzerContext {
        purpose: crate::analyzer::QueryAnalysisPurpose::Ordinary,
        grouping_scope: RefCell::new(None),
        view_stack: Default::default(),
        control,
        constant_policy,
        materialized_values: None,
        catalog: catalog.planner_table_provider(),
        current_database,
        function_catalog: functions,
        sql_semantics: sql_semantics.clone(),
        factory: Rc::new(RefCell::new(crate::column_id::ColumnRefFactory::new())),
        ctes: HashMap::new(),
        pending_ctes: HashSet::new(),
        next_subquery_id: Cell::new(0),
        next_lambda_slot_id: Cell::new(0),
        collected_subqueries: RefCell::new(Vec::new()),
        cte_registry: RefCell::new(crate::analysis::cte::CTERegistry::new()),
    }
}

#[cfg(novarocks_dev_controls)]
pub(crate) fn author_standalone(
    request: crate::compiler::SqlStandaloneScalarRequest<'_>,
) -> Result<SqlAuthoredScalarUnit, SqlCompileError> {
    use crate::compiler::SqlStandaloneScalarRequest;
    use crate::planner::distributed::build::lower_standalone_scalar_unit;

    let SqlStandaloneScalarRequest {
        text,
        columns,
        catalog,
        current_database,
        functions,
        sealed_functions,
        sql_semantics,
        constant_policy,
        control,
    } = request;
    control.check()?;
    let parsed = novarocks_parser::parser::parse_expression(text)
        .map_err(|cause| SqlCompileError::InvalidRequest(cause.to_string()))?;
    reject_subqueries(
        &parsed,
        control,
        "standalone scalar expressions cannot contain subqueries",
    )?;
    let functions = functions.snapshot_for_scalar_presence();
    let context = scalar_context(
        catalog,
        current_database,
        functions.as_ref(),
        sql_semantics,
        constant_policy,
        control,
    );
    let mut scope = context.new_scope();
    let mut inputs = Vec::new();
    let mut names = HashSet::new();
    let mut values = HashSet::new();
    for column in columns {
        control.checkpoint(CompilePhase::Validate, 1)?;
        if !names.insert(column.name.to_ascii_lowercase()) || !values.insert(column.value) {
            return Err(SqlCompileError::InvalidRequest(
                "standalone scalar layout contains a duplicate column name or value".into(),
            ));
        }
        super::helpers::validate_value_type(&column.value_type, control)?;
        let column_id = scope.add_column(None, column.name, column.value_type.clone());
        inputs.push((column_id, column.value, column.value_type.clone()));
    }
    let expression = context.analyze_expr(&parsed, &scope)?;
    drop(context);
    let source = SqlAuthoredScalarUnit::from_emission(
        lower_standalone_scalar_unit(
            &expression,
            &inputs,
            functions,
            sealed_functions,
            sql_semantics.sql_mode().allow_throw_exception(),
            constant_policy,
            control,
        )
        .map_err(required_lowering_error)?,
    );
    control.check()?;
    Ok(source)
}

pub(crate) fn prepare(
    request: RequiredConstantRequest<'_>,
) -> Result<PreparedRequiredConstant, SqlCompileError> {
    let RequiredConstantRequest {
        input,
        target,
        catalog,
        current_database,
        functions,
        sealed_functions,
        sql_semantics,
        constant_policy,
        control,
    } = request;
    control.check()?;
    let context = scalar_context(
        catalog,
        current_database,
        functions.as_ref(),
        sql_semantics,
        constant_policy,
        control,
    );
    let (expression, span) = match input {
        RequiredConstantInput::Expression(expression) => {
            reject_subqueries(
                expression,
                control,
                "required constants cannot contain subqueries",
            )?;
            (
                context.analyze_expr(expression, &context.new_scope())?,
                expression.span(),
            )
        }
        RequiredConstantInput::Materialized(value) => {
            super::helpers::validate_value_type(value.value_type(), control)?;
            (
                TypedExpr {
                    value_type: value.value_type().clone(),
                    kind: ExprKind::Constant(value),
                },
                Span::new(0, 0),
            )
        }
    };
    validate_scalar(&expression, span, control)?;
    validate_implicit_target(&expression.value_type, target, span)?;
    // The target admits NULL until the application checks the actual result.
    // This does not erase a possible NULL produced by an ordinary CAST.
    let result_type = required_target_value_type(&expression.value_type, target, control)?;
    let expression = super::value_conversion::cast_to_value_type(
        expression,
        result_type,
        span,
        functions.as_ref(),
        sql_semantics,
        constant_policy,
        control,
    )?;
    let original_value = match &expression.kind {
        ExprKind::Constant(value) => Some(value.clone()),
        _ => None,
    };
    drop(context);
    let source = SqlAuthoredScalarUnit::from_emission(
        lower_required_constant_unit(
            &expression,
            functions,
            sealed_functions,
            sql_semantics.sql_mode().allow_throw_exception(),
            constant_policy,
            control,
        )
        .map_err(required_lowering_error)?,
    );
    // This is the actual completed scalar source. Required preparation never
    // enters optional folding or constructs a physical plan for observation.
    if let Some(observer) = control.fold_dependency_observer() {
        observer.observe_required_scalar_source_observed(&source, control)?;
    }
    let value_type = source.value_type().clone();
    control.check()?;
    Ok(PreparedRequiredConstant {
        source,
        value_type,
        span,
        policy: constant_policy,
        control: control.clone(),
        original_value,
    })
}

fn required_lowering_error(
    error: crate::planner::distributed::build::ContractLoweringError,
) -> SqlCompileError {
    use crate::planner::distributed::build::ContractLoweringError as E;
    match error {
        E::Control(cause) => cause.into(),
        E::Unit(cause) => SqlCompileError::CompilationSource(
            novarocks_failure_contract::SharedFailureCause::new(cause),
        ),
        E::LogicalCallSource(crate::binding::LogicalCallArgumentCaptureError::Control(cause)) => {
            cause.into()
        }
        E::LogicalCallSource(crate::binding::LogicalCallArgumentCaptureError::Binding(cause)) => {
            cause.into()
        }
        other => SqlCompileError::CompilationSource(
            novarocks_failure_contract::SharedFailureCause::new(RequiredLoweringFailure(other)),
        ),
    }
}

#[derive(Debug)]
struct RequiredLoweringFailure(crate::planner::distributed::build::ContractLoweringError);
impl std::fmt::Display for RequiredLoweringFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RequiredLoweringFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use crate::planner::distributed::build::ContractLoweringError as E;
        Some(match &self.0 {
            E::ComparisonCoercion(cause) => cause,
            E::ConstantSource(cause) => cause,
            E::CallRequests(cause) => cause,
            E::Unit(cause) => cause,
            E::Control(cause) => cause,
            E::LogicalCallSource(cause) => cause,
            E::AggregateSource(cause) => cause,
            E::UnsupportedWaitCycle(cause) => cause,
            other => other,
        })
    }
}
impl novarocks_failure_contract::FailureProjection for RequiredLoweringFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::compiler::PackageSemanticsError;
        use crate::planner::distributed::build::ContractLoweringError as E;
        match &self.0 {
            E::Control(cause) => cause.failure_fact(),
            E::FunctionBinding(cause) => cause.failure_fact(),
            E::Semantics(cause) => cause.failure_fact(),
            E::ComparisonCoercion(cause) => cause.failure_fact(),
            E::ConstantSource(cause) => cause.failure_fact(),
            E::CallRequests(cause) => cause.failure_fact(),
            E::Unit(cause) => cause.failure_fact(),
            E::UnsupportedWaitCycle(cause) => cause.failure_fact(),
            E::UnsupportedExpression { .. } | E::UnsupportedNode { .. } => {
                PackageSemanticsError::UnsupportedCapability(String::new()).failure_fact()
            }
            _ => PackageSemanticsError::InvalidSource("required expression lowering invariant")
                .failure_fact(),
        }
    }
}

fn reject_subqueries(
    expression: &ast::Expr,
    control: &crate::compiler::SqlCompileControl,
    message: &'static str,
) -> Result<(), AnalyzeError> {
    struct Gate<'a> {
        error: Option<AnalyzeError>,
        control: &'a crate::compiler::SqlCompileControl,
        message: &'static str,
    }
    impl Visit for Gate<'_> {
        fn visit_expr(&mut self, expression: &ast::Expr) {
            if self.error.is_some() {
                return;
            }
            if let Err(cause) = self.control.checkpoint(CompilePhase::Validate, 1) {
                self.error = Some(AnalyzeError::control(cause));
                return;
            }
            if matches!(expression, ast::Expr::Subquery(_) | ast::Expr::Exists(_)) {
                self.error = Some(AnalyzeError::unsupported_expression(
                    self.message,
                    expression.span(),
                ));
                return;
            }
            ast::walk_expr(self, expression);
        }
    }
    let mut gate = Gate {
        error: None,
        control,
        message,
    };
    gate.visit_expr(expression);
    gate.error.map_or(Ok(()), Err)
}

fn validate_implicit_target(
    source: &FunctionValueType,
    target: RequiredConstantTarget,
    span: Span,
) -> Result<(), AnalyzeError> {
    use DataType::*;
    if target == RequiredConstantTarget::TimeZone {
        let ty = source.value_type();
        if matches!(ty.kind(), ValueKind::String)
            || matches!(ty.temporal(), Some(novarocks_type_contract::TemporalType::Interval(qualifier)) if !qualifier.is_year_month())
        {
            return Ok(());
        }
        return Err(AnalyzeError::type_mismatch(
            "Expected an expression of varchar or interval day-time type",
            span,
        ));
    }
    // The semantic value kind owns this decision. A dictionary layout, a
    // nominal Utf8 value, and a physical SQL string are not interchangeable.
    let allowed = if target == RequiredConstantTarget::Timestamp {
        source.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
            && match source.value_type().kind() {
                ValueKind::Scalar(ty @ Timestamp(..)) => source.data_type() == ty,
                ValueKind::Scalar(Null) => source.nullable() && source.data_type() == &Null,
                _ => false,
            }
    } else if target == RequiredConstantTarget::ArrayVarchar {
        match source.value_type().kind() {
            ValueKind::Scalar(Null) => source.nullable(),
            ValueKind::List(child) => match child.kind() {
                ValueKind::String => true,
                ValueKind::Scalar(Null) => child.nullable(),
                _ => false,
            },
            _ => false,
        }
    } else {
        match source.value_type().kind() {
            ValueKind::String => target == RequiredConstantTarget::Varchar,
            ValueKind::Scalar(data_type) => match data_type {
                Null => true,
                Boolean => target == RequiredConstantTarget::Boolean,
                Int8 | Int16 | Int32 => matches!(
                    target,
                    RequiredConstantTarget::Integer
                        | RequiredConstantTarget::Bigint
                        | RequiredConstantTarget::Double
                ),
                Int64 => matches!(
                    target,
                    RequiredConstantTarget::Bigint | RequiredConstantTarget::Double
                ),
                Float32 | Float64 | Decimal32(..) | Decimal64(..) | Decimal128(..)
                | Decimal256(..) => target == RequiredConstantTarget::Double,
                _ => false,
            },
            _ => false,
        }
    };
    if allowed {
        Ok(())
    } else {
        let message = if target == RequiredConstantTarget::Timestamp {
            "required constant for an absolute time must have a Physical TIMESTAMP type"
        } else {
            "required constant cannot be implicitly converted to the declared property type"
        };
        Err(AnalyzeError::unsupported_expression(message, span))
    }
}

fn required_target_value_type(
    source: &FunctionValueType,
    target: RequiredConstantTarget,
    control: &dyn PureCompileControl,
) -> Result<FunctionValueType, AnalyzeError> {
    use novarocks_type_contract::{
        ArrayCommonTypeFailure, ValueConstructionRequest, ValueLogicalType,
        ValueTypeStorageLifetime,
    };
    match target {
        RequiredConstantTarget::Boolean
        | RequiredConstantTarget::Integer
        | RequiredConstantTarget::Bigint
        | RequiredConstantTarget::Double
        | RequiredConstantTarget::Varchar => {
            let data_type = target.scalar_data_type().ok_or_else(|| {
                AnalyzeError::internal("scalar required target has no scalar declaration")
            })?;
            Ok(FunctionValueType::new(
                data_type,
                source.nullable()
                    || matches!(
                        source.value_type().kind(),
                        ValueKind::Scalar(DataType::Null)
                    ),
            ))
        }
        RequiredConstantTarget::Timestamp | RequiredConstantTarget::TimeZone => source
            .clone_observed(|| control.checkpoint(CompilePhase::Validate, 1))
            .map_err(AnalyzeError::control),
        RequiredConstantTarget::ArrayVarchar => {
            if matches!(source.value_type().kind(), ValueKind::List(child) if matches!(child.kind(), ValueKind::String))
            {
                // Exact identity keeps the original complete semantic/layout
                // binding and allows the admitted CV's pool/ordinal to survive.
                return source
                    .clone_observed(|| control.checkpoint(CompilePhase::Validate, 1))
                    .map_err(AnalyzeError::control);
            }
            let unknown = matches!(source.value_type().kind(), ValueKind::Scalar(DataType::Null) if source.nullable())
                || matches!(source.value_type().kind(), ValueKind::List(child) if child.nullable() && matches!(child.kind(), ValueKind::Scalar(DataType::Null)));
            if !unknown {
                return Err(AnalyzeError::internal(
                    "required ArrayVarchar declaration received an unadmitted source",
                ));
            }
            let unknown_source = match source.value_type().kind() {
                ValueKind::Scalar(DataType::Null) => source.nullable(),
                ValueKind::List(child) => {
                    child.nullable() && matches!(child.kind(), ValueKind::Scalar(DataType::Null))
                }
                _ => false,
            };
            if !unknown_source {
                return Err(AnalyzeError::internal(
                    "required ArrayVarchar declaration lacks its validated UNKNOWN source",
                ));
            }
            // These are pure declaration-construction observations, not a
            // runtime storage grant. Exact/opaque events still check control.
            let mut observe = |request| {
                let mut remaining = match request {
                    ValueConstructionRequest::Work(units) => units,
                    ValueConstructionRequest::Exact(_)
                    | ValueConstructionRequest::OpaqueUpper(_) => 0,
                };
                while remaining > u32::MAX as usize {
                    control.checkpoint(CompilePhase::Validate, u32::MAX)?;
                    remaining -= u32::MAX as usize;
                }
                control.checkpoint(CompilePhase::Validate, remaining as u32)
            };
            let storage = ValueTypeStorageLifetime::pure();
            let child = FunctionValueType::try_with_logical_type_with_construction(
                DataType::Utf8,
                true,
                ValueLogicalType::Physical,
                storage.clone(),
                Some(&mut observe),
            )
            .map_err(|cause| {
                required_array_type_failure(ArrayCommonTypeFailure::Construction(cause))
            })?;
            novarocks_type_contract::list_value_type_with_construction(
                &child,
                source.nullable()
                    || matches!(
                        source.value_type().kind(),
                        ValueKind::Scalar(DataType::Null)
                    ),
                storage,
                Some(&mut observe),
            )
            .map_err(required_array_type_failure)
        }
    }
}

fn required_array_type_failure(
    failure: novarocks_type_contract::ArrayCommonTypeFailure<
        novarocks_type_contract::CompileControlError,
    >,
) -> AnalyzeError {
    use novarocks_failure_contract::SharedFailureCause;
    use novarocks_type_contract::{ArrayCommonTypeFailure, ValueConstructionFailure};
    match failure {
        ArrayCommonTypeFailure::Construction(cause) => match cause {
            ValueConstructionFailure::Observer(cause) => AnalyzeError::control(cause),
            ValueConstructionFailure::ValueType(cause) => {
                AnalyzeError::shared_domain_source(SharedFailureCause::new(cause))
            }
            ValueConstructionFailure::Layout(cause) => {
                AnalyzeError::shared_domain_source(SharedFailureCause::new(cause))
            }
            ValueConstructionFailure::Vec(
                novarocks_type_contract::ControlResourceError::Control(cause),
            ) => AnalyzeError::control(cause),
            ValueConstructionFailure::Vec(cause) => {
                AnalyzeError::shared_domain_source(SharedFailureCause::new(cause))
            }
            ValueConstructionFailure::HashMap(cause) => {
                AnalyzeError::shared_domain_source(SharedFailureCause::new(cause))
            }
            ValueConstructionFailure::BTree(cause) => {
                AnalyzeError::shared_domain_source(SharedFailureCause::new(cause))
            }
            ValueConstructionFailure::Rule(detail) => AnalyzeError::internal(detail),
        },
        ArrayCommonTypeFailure::Rule(reason) => AnalyzeError::internal(format!(
            "required List declaration unexpectedly applied ARRAY common rules: {reason:?}"
        )),
    }
}

fn validate_scalar(
    expression: &TypedExpr,
    span: Span,
    control: &crate::compiler::SqlCompileControl,
) -> Result<(), AnalyzeError> {
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        control
            .checkpoint(CompilePhase::Validate, 1)
            .map_err(AnalyzeError::control)?;
        match &expression.kind {
            ExprKind::ColumnRef { .. }
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::AggregateCall { .. }
            | ExprKind::WindowCall { .. }
            | ExprKind::SubqueryPlaceholder { .. }
            | ExprKind::Lambda { .. }
            | ExprKind::LambdaFunction { .. } => {
                return Err(AnalyzeError::unsupported_expression(
                    "required constants cannot depend on columns, aggregates, windows or lambda inputs",
                    span,
                ));
            }
            ExprKind::Literal(_) | ExprKind::Constant(_) => {}
            ExprKind::FunctionCall {
                args, volatility, ..
            } => {
                if *volatility == FunctionVolatility::Volatile {
                    return Err(AnalyzeError::unsupported_expression(
                        "volatile functions are not supported in required constants",
                        span,
                    ));
                }
                pending.extend(args);
            }
            ExprKind::BinaryOp { left, right, .. } => {
                pending.push(right);
                pending.push(left);
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr) => pending.push(expr),
            ExprKind::InList { expr, list, .. } => {
                pending.extend(list);
                pending.push(expr);
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                pending.push(high);
                pending.push(low);
                pending.push(expr);
            }
            ExprKind::Like { expr, pattern, .. } => {
                pending.push(pattern);
                pending.push(expr);
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(expr) = else_expr {
                    pending.push(expr);
                }
                for (condition, result) in when_then {
                    pending.push(result);
                    pending.push(condition);
                }
                if let Some(expr) = operand {
                    pending.push(expr);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
