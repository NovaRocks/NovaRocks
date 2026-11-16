// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Explicit source ownership for one already-materialized VALUES relation.
//! No parser token, literal content, alias or provider attribute is an identity.
use super::SqlCompileError;
use novarocks_parser::ast;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

fn invalid(detail: &'static str) -> SqlCompileError {
    SqlCompileError::InvalidRequest(detail.to_owned())
}
/// Structural edges of the producer's existing eligible query body. These
/// names describe owned AST positions, never SQL text or wire identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlConstantQueryStep {
    NestedQuery,
    SetOperand { index: usize },
    SelectFromDerived { from: usize },
}
/// Exact expression edges below a producer-authored projection or VALUES cell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlConstantExpressionStep {
    Nested,
    LogicalTerm(usize),
    PredicatedValue,
    ComparisonRight,
    BetweenMin,
    BetweenMax,
    LikePattern,
    InValueList,
    InListValue(usize),
    CastExpression,
    ExtractInput,
    RowFieldExpression(usize),
    DereferenceBase,
    SubscriptBase,
    SubscriptIndex,
    FunctionArgumentValue(usize),
    IfCondition,
    IfTrue,
    IfFalse,
    NullIfFirst,
    NullIfSecond,
    CoalesceOperand(usize),
    TryInner,
    FormatArgument(usize),
    FunctionOrderByKey(usize),
    WindowOrderByKey(usize),
    CaseOperand,
    CaseWhenOperand(usize),
    CaseResult(usize),
    CaseDefault,
    LambdaBody,
    ExistsSubquery,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlConstantExpressionSite {
    ValuesCell { row: usize, column: usize },
    SelectProjection { column: usize },
    QueryOrderByKey { index: usize },
    SelectOrderByKey { index: usize },
}
#[derive(Clone, Debug)]
pub struct SqlExpressionConstant {
    body_path: Box<[SqlConstantQueryStep]>,
    site: SqlConstantExpressionSite,
    expression_path: Box<[SqlConstantExpressionStep]>,
    value: novarocks_functions::ConstantValue,
}
impl SqlExpressionConstant {
    /// The source producer supplies the path after its final AST wrapper.
    pub fn new(
        body_path: Vec<SqlConstantQueryStep>,
        site: SqlConstantExpressionSite,
        value: novarocks_functions::ConstantValue,
    ) -> Self {
        Self {
            body_path: body_path.into_boxed_slice(),
            site,
            expression_path: Box::default(),
            value,
        }
    }
    /// Preserve the actual nested syntax callbacks outside this exact source.
    pub fn with_expression_path(mut self, path: Vec<SqlConstantExpressionStep>) -> Self {
        self.expression_path = path.into_boxed_slice();
        self
    }
    pub fn value(&self) -> &novarocks_functions::ConstantValue {
        &self.value
    }
    fn resolve<'a>(
        &self,
        query: &'a ast::Query,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a ast::Expr, SqlCompileError> {
        resolve_expression_site(
            query,
            &self.body_path,
            self.site,
            &self.expression_path,
            false,
            work,
        )
    }
}

pub(crate) fn resolve_expression_site<'a>(
    query: &'a ast::Query,
    body_path: &[SqlConstantQueryStep],
    site: SqlConstantExpressionSite,
    expression_path: &[SqlConstantExpressionStep],
    allow_set_operations: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a ast::Expr, SqlCompileError> {
    let mut query_owner = Some(query);
    let mut body = query.body.as_ref();
    for step in body_path {
        work.step()?;
        body = match (step, body) {
            (SqlConstantQueryStep::NestedQuery, ast::SetExpr::Query(query)) => {
                query_owner = Some(query);
                query.body.as_ref()
            }
            (SqlConstantQueryStep::SetOperand { index }, ast::SetExpr::SetOperation(op))
                if op.corresponding.is_none()
                    && (allow_set_operations
                        || (op.operator == ast::SetOperator::Union
                            && op.quantifier == ast::SetQuantifier::All)) =>
            {
                query_owner = None;
                op.inputs.get(*index).ok_or_else(|| {
                    invalid("constant source set operand index differs from its owned query")
                })?
            }
            (SqlConstantQueryStep::SelectFromDerived { from }, ast::SetExpr::Select(select)) => {
                let Some(ast::TableFactor::Derived { subquery, .. }) =
                    select.from.get(*from).map(|from| &from.relation)
                else {
                    return Err(invalid(
                        "constant source path differs from its owned derived relation",
                    ));
                };
                query_owner = Some(subquery);
                subquery.body.as_ref()
            }
            _ => {
                return Err(invalid(
                    "constant source path differs from its owned query body",
                ));
            }
        };
    }
    work.step()?;
    let mut expression = match (site, body) {
        (SqlConstantExpressionSite::QueryOrderByKey { index }, _) => query_owner
            .and_then(|owner| owner.modifiers.order_by.as_ref())
            .and_then(|order| order.items.get(index))
            .map(|item| &item.sort_key),
        (SqlConstantExpressionSite::SelectOrderByKey { index }, ast::SetExpr::Select(select)) => {
            select
                .modifiers
                .order_by
                .as_ref()
                .and_then(|order| order.items.get(index))
                .map(|item| &item.sort_key)
        }
        (SqlConstantExpressionSite::ValuesCell { row, column }, ast::SetExpr::Values(values)) => {
            values.rows.get(row).and_then(|row| row.get(column))
        }
        (SqlConstantExpressionSite::SelectProjection { column }, ast::SetExpr::Select(select)) => {
            select.projection.get(column).and_then(|item| match item {
                ast::SelectItem::UnnamedExpr(expr)
                | ast::SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                _ => None,
            })
        }
        _ => None,
    }
    .ok_or_else(|| invalid("constant source expression is outside its owned query"))?;
    for step in expression_path {
        work.step()?;
        expression = match (step, expression) {
            (SqlConstantExpressionStep::Nested, ast::Expr::Nested(nested)) => &nested.expression,
            (SqlConstantExpressionStep::LogicalTerm(index), ast::Expr::Logical(logical)) => {
                logical.terms.get(*index).ok_or_else(|| {
                    invalid("constant source logical term index differs from its owned syntax")
                })?
            }
            (SqlConstantExpressionStep::PredicatedValue, ast::Expr::Predicated(node)) => {
                &node.value
            }
            (SqlConstantExpressionStep::ComparisonRight, ast::Expr::Predicated(node)) => {
                match &node.predicate {
                    ast::Predicate::Comparison(p) => &p.right,
                    ast::Predicate::DistinctFrom(p) => &p.right,
                    _ => {
                        return Err(invalid(
                            "constant source comparison edge differs from its owned syntax",
                        ));
                    }
                }
            }
            (SqlConstantExpressionStep::BetweenMin, ast::Expr::Predicated(node)) => {
                let ast::Predicate::Between(p) = &node.predicate else {
                    return Err(invalid(
                        "constant source BETWEEN edge differs from its owned syntax",
                    ));
                };
                &p.min
            }
            (SqlConstantExpressionStep::BetweenMax, ast::Expr::Predicated(node)) => {
                let ast::Predicate::Between(p) = &node.predicate else {
                    return Err(invalid(
                        "constant source BETWEEN edge differs from its owned syntax",
                    ));
                };
                &p.max
            }
            (SqlConstantExpressionStep::LikePattern, ast::Expr::Predicated(node)) => {
                let ast::Predicate::Like(p) = &node.predicate else {
                    return Err(invalid(
                        "constant source LIKE edge differs from its owned syntax",
                    ));
                };
                &p.pattern
            }
            (SqlConstantExpressionStep::InValueList, ast::Expr::Predicated(node)) => {
                let ast::Predicate::In(p) = &node.predicate else {
                    return Err(invalid(
                        "constant source IN edge differs from its owned syntax",
                    ));
                };
                &p.value_list
            }
            (SqlConstantExpressionStep::InListValue(index), ast::Expr::InList(list)) => {
                list.values.get(*index).ok_or_else(|| {
                    invalid("constant source IN value index differs from its owned syntax")
                })?
            }
            (SqlConstantExpressionStep::CastExpression, ast::Expr::Cast(cast)) => &cast.expression,
            (SqlConstantExpressionStep::DereferenceBase, ast::Expr::Dereference(node)) => {
                &node.base
            }
            (SqlConstantExpressionStep::SubscriptBase, ast::Expr::Subscript(node)) => &node.base,
            (SqlConstantExpressionStep::SubscriptIndex, ast::Expr::Subscript(node)) => &node.index,
            (
                SqlConstantExpressionStep::FunctionArgumentValue(index),
                ast::Expr::FunctionCall(call),
            ) => {
                &call
                    .arguments
                    .get(*index)
                    .ok_or_else(|| {
                        invalid(
                            "constant source function argument index differs from its owned syntax",
                        )
                    })?
                    .value
            }
            (
                SqlConstantExpressionStep::FunctionOrderByKey(index),
                ast::Expr::FunctionCall(call),
            ) => {
                &call
                    .order_by
                    .as_ref()
                    .and_then(|order| order.items.get(*index))
                    .ok_or_else(|| {
                        invalid(
                            "constant source function ORDER BY key differs from its owned syntax",
                        )
                    })?
                    .sort_key
            }
            (SqlConstantExpressionStep::WindowOrderByKey(index), ast::Expr::FunctionCall(call)) => {
                &call
                    .window
                    .as_ref()
                    .and_then(|window| match window {
                        ast::Window::Specification(specification) => {
                            specification.order_by.as_ref()
                        }
                        ast::Window::Reference(_) => None,
                    })
                    .and_then(|order| order.items.get(*index))
                    .ok_or_else(|| {
                        invalid("constant source window ORDER BY key differs from its owned syntax")
                    })?
                    .sort_key
            }
            (SqlConstantExpressionStep::ExtractInput, ast::Expr::Extract(node)) => &node.expression,
            (SqlConstantExpressionStep::RowFieldExpression(index), ast::Expr::Row(row)) => {
                &row.fields
                    .get(*index)
                    .ok_or_else(|| {
                        invalid("constant source ROW field differs from its owned syntax")
                    })?
                    .expression
            }
            (SqlConstantExpressionStep::IfCondition, ast::Expr::If(node)) => &node.condition,
            (SqlConstantExpressionStep::IfTrue, ast::Expr::If(node)) => &node.true_value,
            (SqlConstantExpressionStep::IfFalse, ast::Expr::If(node)) => {
                node.false_value.as_ref().ok_or_else(|| {
                    invalid("constant source IF false value is absent from its owned syntax")
                })?
            }
            (SqlConstantExpressionStep::NullIfFirst, ast::Expr::NullIf(node)) => &node.first,
            (SqlConstantExpressionStep::NullIfSecond, ast::Expr::NullIf(node)) => &node.second,
            (SqlConstantExpressionStep::CoalesceOperand(index), ast::Expr::Coalesce(node)) => {
                node.operands.get(*index).ok_or_else(|| {
                    invalid("constant source COALESCE operand differs from its owned syntax")
                })?
            }
            (SqlConstantExpressionStep::TryInner, ast::Expr::Try(node)) => &node.inner,
            (SqlConstantExpressionStep::FormatArgument(index), ast::Expr::Format(node)) => {
                node.arguments.get(*index).ok_or_else(|| {
                    invalid("constant source FORMAT argument differs from its owned syntax")
                })?
            }
            (SqlConstantExpressionStep::CaseOperand, ast::Expr::SimpleCase(case)) => &case.operand,
            (SqlConstantExpressionStep::CaseWhenOperand(index), ast::Expr::SearchedCase(case)) => {
                case_when_operand(&case.when_clauses, *index)?
            }
            (SqlConstantExpressionStep::CaseWhenOperand(index), ast::Expr::SimpleCase(case)) => {
                case_when_operand(&case.when_clauses, *index)?
            }
            (SqlConstantExpressionStep::CaseResult(index), ast::Expr::SearchedCase(case)) => {
                &case
                    .when_clauses
                    .get(*index)
                    .ok_or_else(|| {
                        invalid("constant source CASE branch index differs from its owned syntax")
                    })?
                    .result
            }
            (SqlConstantExpressionStep::CaseResult(index), ast::Expr::SimpleCase(case)) => {
                &case
                    .when_clauses
                    .get(*index)
                    .ok_or_else(|| {
                        invalid("constant source CASE branch index differs from its owned syntax")
                    })?
                    .result
            }
            (SqlConstantExpressionStep::CaseDefault, ast::Expr::SearchedCase(case)) => {
                case.default_value.as_ref().ok_or_else(|| {
                    invalid("constant source CASE default is absent from its owned syntax")
                })?
            }
            (SqlConstantExpressionStep::CaseDefault, ast::Expr::SimpleCase(case)) => {
                case.default_value.as_ref().ok_or_else(|| {
                    invalid("constant source CASE default is absent from its owned syntax")
                })?
            }
            (SqlConstantExpressionStep::LambdaBody, ast::Expr::Lambda(lambda)) => &lambda.body,
            (SqlConstantExpressionStep::ExistsSubquery, ast::Expr::Exists(exists)) => {
                &exists.subquery
            }
            _ => {
                return Err(invalid(
                    "constant source expression path differs from its owned syntax",
                ));
            }
        };
    }
    Ok(expression)
}

fn case_when_operand(
    clauses: &[ast::WhenClause],
    index: usize,
) -> Result<&ast::Expr, SqlCompileError> {
    let clause = clauses.get(index).ok_or_else(|| {
        invalid("constant source CASE branch index differs from its owned syntax")
    })?;
    let ast::WhenMatch::Operand(value) = &clause.match_value else {
        return Err(invalid("constant source CASE match is a partial predicate"));
    };
    Ok(value)
}

pub type SqlExpressionConstants = std::sync::Arc<[SqlExpressionConstant]>;

/// Each constant retains its actual immutable pool and complete value type.
#[derive(Clone, Debug)]
pub(crate) enum MaterializedValues {
    Expressions { constants: SqlExpressionConstants },
}
/// The lookup is local to one actual analysis/query borrow. It stores no
/// process-global pointer identity, query owner, provider or capability.
pub(crate) struct MaterializedSourceBorrow<'a> {
    // Expression addresses belong to this exact query borrow.
    _query: std::marker::PhantomData<&'a ast::Query>,
    expressions: std::collections::BTreeMap<usize, novarocks_functions::ConstantValue>,
    write_assignments:
        std::collections::BTreeMap<usize, std::borrow::Cow<'a, super::SqlWriteAssignmentSite>>,
}

impl<'a> MaterializedSourceBorrow<'a> {
    pub(crate) fn empty(_query: &'a ast::Query) -> Self {
        Self {
            _query: std::marker::PhantomData,
            expressions: std::collections::BTreeMap::new(),
            write_assignments: std::collections::BTreeMap::new(),
        }
    }
    pub(crate) fn insert_write_assignment(
        &mut self,
        expression: &'a ast::Expr,
        assignment: std::borrow::Cow<'a, super::SqlWriteAssignmentSite>,
        control: &super::SqlCompileControl,
    ) -> Result<(), SqlCompileError> {
        control.checkpoint(CompilePhase::Validate, 1)?;
        if self
            .write_assignments
            .insert(std::ptr::from_ref(expression) as usize, assignment)
            .is_some()
        {
            return Err(invalid(
                "write source assignment repeats an original occurrence",
            ));
        }
        Ok(())
    }

    pub(crate) fn has_expression(&self, expression: &ast::Expr) -> bool {
        self.expressions
            .contains_key(&(std::ptr::from_ref(expression) as usize))
    }
    pub(crate) fn insert_context_constant(
        &mut self,
        expression: &'a ast::Expr,
        value: novarocks_functions::ConstantValue,
        control: &super::SqlCompileControl,
    ) -> Result<(), SqlCompileError> {
        control.checkpoint(CompilePhase::Validate, 1)?;
        if self
            .expressions
            .insert(std::ptr::from_ref(expression) as usize, value)
            .is_some()
        {
            return Err(invalid(
                "write literal context repeats an original constant occurrence",
            ));
        }
        Ok(())
    }
}

impl MaterializedSourceBorrow<'_> {
    pub(crate) fn view(&self) -> MaterializedValuesLoan<'_> {
        MaterializedValuesLoan {
            expressions: &self.expressions,
            write_assignments: &self.write_assignments,
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) struct MaterializedValuesLoan<'a> {
    expressions: &'a std::collections::BTreeMap<usize, novarocks_functions::ConstantValue>,
    write_assignments:
        &'a std::collections::BTreeMap<usize, std::borrow::Cow<'a, super::SqlWriteAssignmentSite>>,
}
impl<'a> MaterializedValuesLoan<'a> {
    pub(crate) fn write_assignment_at(
        self,
        expression: &ast::Expr,
    ) -> Option<&'a super::SqlWriteAssignmentSite> {
        self.write_assignments
            .get(&(std::ptr::from_ref(expression) as usize))
            .map(std::borrow::Cow::as_ref)
    }

    pub(crate) fn constant_at(
        self,
        expr: &ast::Expr,
    ) -> Option<&'a novarocks_functions::ConstantValue> {
        self.expressions.get(&(std::ptr::from_ref(expr) as usize))
    }
}
impl MaterializedValues {
    pub(crate) fn try_new_expressions(
        query: &ast::Query,
        constants: SqlExpressionConstants,
        control: &dyn PureCompileControl,
    ) -> Result<Self, SqlCompileError> {
        let source = Self::Expressions { constants };
        source.loan(query, control)?;
        Ok(source)
    }
    pub(crate) fn loan<'a>(
        &'a self,
        query: &'a ast::Query,
        control: &dyn PureCompileControl,
    ) -> Result<MaterializedSourceBorrow<'a>, SqlCompileError> {
        let Self::Expressions { constants } = self;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        work.step()?;
        if constants.is_empty() {
            return Err(invalid("constant source has no authored expressions"));
        }
        let mut expressions = std::collections::BTreeMap::new();
        for constant in constants.iter() {
            let expr = constant.resolve(query, &mut work)?;
            novarocks_type_contract::visit_value_type_observed(
                constant.value().value_type().value_type(),
                |_| work.step().map_err(SqlCompileError::from),
            )?;
            if expressions
                .insert(std::ptr::from_ref(expr) as usize, constant.value().clone())
                .is_some()
            {
                return Err(invalid(
                    "constant source repeats its owned expression occurrence",
                ));
            }
        }
        work.finish()?;
        Ok(MaterializedSourceBorrow {
            _query: std::marker::PhantomData,
            expressions,
            write_assignments: std::collections::BTreeMap::new(),
        })
    }
}
#[cfg(test)]
#[path = "materialized_values_tests.rs"]
mod tests;
