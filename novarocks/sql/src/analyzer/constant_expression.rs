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
use novarocks_physical_plan::PipelineDopDomain;
use novarocks_type_contract::{CompilePhase, FunctionValueType, PureCompileControl, ValueKind};

use crate::compiler::{
    PreparedRequiredConstant, RequiredConstantInput, RequiredConstantRequest,
    RequiredConstantTarget, SqlCompileError,
};
use crate::planner::{
    distributed::build::{SqlPublicationError, lower_final_physical_plan_with_root_semantics},
    payload::{PlanProjectNode, PlanValuesNode},
    physical::{PhysicalPlanKind, PhysicalPlanNode, PhysicalPlanStats, PlannerConfidence},
};
use crate::{
    analysis::{ExprKind, ProjectItem, TypedExpr},
    analyze_error::AnalyzeError,
    common::OutputColumn,
};

pub(crate) fn prepare(
    request: RequiredConstantRequest<'_>,
) -> Result<PreparedRequiredConstant, SqlCompileError> {
    let RequiredConstantRequest {
        input,
        target,
        catalog,
        current_database,
        functions,
        sql_semantics,
        constant_policy,
        control,
        version,
    } = request;
    control.check()?;
    let factory = Rc::new(RefCell::new(crate::column_id::ColumnRefFactory::new()));
    let context = super::AnalyzerContext {
        grouping_scope: RefCell::new(None),
        view_stack: Default::default(),
        control,
        constant_policy,
        materialized_values: None,
        catalog: catalog.planner_table_provider(),
        current_database,
        function_catalog: functions.as_ref(),
        sql_semantics: sql_semantics.clone(),
        factory: factory.clone(),
        ctes: HashMap::new(),
        pending_ctes: HashSet::new(),
        next_subquery_id: Cell::new(0),
        next_lambda_slot_id: Cell::new(0),
        collected_subqueries: RefCell::new(Vec::new()),
        cte_registry: RefCell::new(crate::analysis::cte::CTERegistry::new()),
    };
    let (expression, span) = match input {
        RequiredConstantInput::Expression(expression) => {
            reject_subqueries(expression, control)?;
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
    let result_type = FunctionValueType::new(
        target.data_type(),
        expression.value_type.nullable()
            || matches!(
                expression.value_type.value_type().kind(),
                ValueKind::Scalar(DataType::Null)
            ),
    );
    let expression = super::value_conversion::cast_to_value_type(
        expression,
        result_type,
        span,
        functions.as_ref(),
        sql_semantics,
        constant_policy,
        control,
    )?;
    let value_type = expression.value_type.clone();
    let original_value = match &expression.kind {
        ExprKind::Constant(value) => Some(value.clone()),
        _ => None,
    };
    let column_id = context.alloc_column_id(None, "value".into(), value_type.clone());
    let column = OutputColumn {
        column_id,
        name: "value".into(),
        value_type: value_type.clone(),
        is_internal: false,
    };
    let stats = || PhysicalPlanStats {
        output_row_count: 1.0,
        row_count_confidence: PlannerConfidence::Exact,
        column_statistics: HashMap::new(),
        cost_estimate: None,
        broadcast_decision: None,
    };
    let input = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![vec![]],
            columns: vec![],
        }),
        children: vec![],
        output_columns: vec![],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let plan = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![ProjectItem {
                expr: expression,
                output_name: "value".into(),
                output_column_id: column_id,
            }],
            output_qualifier: None,
        }),
        children: vec![input],
        output_columns: vec![column],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let semantics = crate::compiler::root_output::RootOutputSemantics::from_declared_names(
        &plan.output_columns,
        Box::from([Some("value".into())]),
    )
    .map_err(SqlCompileError::Compilation)?
    .with_analysis_span(span);
    drop(context);
    // This is a local one-row expression domain, not a backend topology fact.
    let draft = lower_final_physical_plan_with_root_semantics(
        &plan,
        version,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        None,
        semantics,
        functions,
        sql_semantics.sql_mode().allow_throw_exception(),
        constant_policy,
        control,
    )
    .map_err(required_lowering_error)?;
    let source = draft
        .finish_with_dependency_observer_observed(control)
        .map_err(|error| match error {
            SqlPublicationError::Construction(
                novarocks_physical_plan::PlanConstructionError::Constants(
                    novarocks_physical_plan::ConstantReferenceError::Control(cause),
                ),
            ) => SqlCompileError::from(cause),
            SqlPublicationError::Construction(error) => SqlCompileError::CompilationSource(
                novarocks_failure_contract::SharedFailureCause::new(RequiredConstructionFailure(
                    error,
                )),
            ),
            SqlPublicationError::Support(error) => error.into_compile_error(),
        })?;
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
            E::ComparisonCoercion(cause) => cause.failure_fact(),
            E::ConstantSource(cause) => cause.failure_fact(),
            E::CallRequests(cause) => cause.failure_fact(),
            E::UnsupportedWaitCycle(cause) => cause.failure_fact(),
            E::UnsupportedExpression { .. } | E::UnsupportedNode { .. } => {
                PackageSemanticsError::UnsupportedCapability(String::new()).failure_fact()
            }
            _ => PackageSemanticsError::InvalidSource("required expression lowering invariant")
                .failure_fact(),
        }
    }
}
#[derive(Debug)]
struct RequiredConstructionFailure(novarocks_physical_plan::PlanConstructionError);
impl std::fmt::Display for RequiredConstructionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RequiredConstructionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match &self.0 {
            novarocks_physical_plan::PlanConstructionError::Requests(cause) => cause,
            novarocks_physical_plan::PlanConstructionError::Constants(cause) => cause,
            novarocks_physical_plan::PlanConstructionError::Structure(cause) => cause,
        })
    }
}
impl novarocks_failure_contract::FailureProjection for RequiredConstructionFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match &self.0 {
            novarocks_physical_plan::PlanConstructionError::Requests(cause) => cause.failure_fact(),
            novarocks_physical_plan::PlanConstructionError::Constants(cause) => {
                cause.failure_fact()
            }
            novarocks_physical_plan::PlanConstructionError::Structure(cause) => {
                cause.failure_fact()
            }
        }
    }
}

fn reject_subqueries(
    expression: &ast::Expr,
    control: &crate::compiler::SqlCompileControl,
) -> Result<(), AnalyzeError> {
    struct Gate<'a> {
        error: Option<AnalyzeError>,
        control: &'a crate::compiler::SqlCompileControl,
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
            if matches!(
                expression,
                ast::Expr::Subquery(_) | ast::Expr::InSubquery(_) | ast::Expr::Exists(_)
            ) {
                self.error = Some(AnalyzeError::unsupported_expression(
                    "required constants cannot contain subqueries",
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
    // The semantic value kind owns this decision. A dictionary layout, a
    // nominal Utf8 value, and a physical SQL string are not interchangeable.
    let allowed = match source.value_type().kind() {
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
            Float32 | Float64 | Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..) => {
                target == RequiredConstantTarget::Double
            }
            _ => false,
        },
        _ => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(AnalyzeError::unsupported_expression(
            "required constant cannot be implicitly converted to the declared property type",
            span,
        ))
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
