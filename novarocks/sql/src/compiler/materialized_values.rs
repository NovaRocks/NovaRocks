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
    UnionLeft,
    UnionRight,
    SelectFromDerived { from: usize },
}
/// Exact expression edges below a producer-authored projection or VALUES cell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlConstantExpressionStep {
    Nested,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlConstantExpressionSite {
    ValuesCell { row: usize, column: usize },
    SelectProjection { column: usize },
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
        let mut body = query.body.as_ref();
        for step in &self.body_path {
            work.step()?;
            body = match (step, body) {
                (SqlConstantQueryStep::NestedQuery, ast::SetExpr::Query(query)) => {
                    query.body.as_ref()
                }
                (SqlConstantQueryStep::UnionLeft, ast::SetExpr::SetOperation(op))
                    if matches!(op.operator, ast::SetOperator::Union)
                        && matches!(op.quantifier, ast::SetQuantifier::All) =>
                {
                    op.left.as_ref()
                }
                (SqlConstantQueryStep::UnionRight, ast::SetExpr::SetOperation(op))
                    if matches!(op.operator, ast::SetOperator::Union)
                        && matches!(op.quantifier, ast::SetQuantifier::All) =>
                {
                    op.right.as_ref()
                }
                (
                    SqlConstantQueryStep::SelectFromDerived { from },
                    ast::SetExpr::Select(select),
                ) => {
                    let Some(ast::TableFactor::Derived { subquery, .. }) =
                        select.from.get(*from).map(|from| &from.relation)
                    else {
                        return Err(invalid(
                            "constant source path differs from its owned derived relation",
                        ));
                    };
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
        let mut expression = match (self.site, body) {
            (
                SqlConstantExpressionSite::ValuesCell { row, column },
                ast::SetExpr::Values(values),
            ) => values.rows.get(row).and_then(|row| row.get(column)),
            (
                SqlConstantExpressionSite::SelectProjection { column },
                ast::SetExpr::Select(select),
            ) => select.projection.get(column).and_then(|item| match item {
                ast::SelectItem::UnnamedExpr(expr)
                | ast::SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                _ => None,
            }),
            _ => None,
        }
        .ok_or_else(|| invalid("constant source expression is outside its owned query"))?;
        for step in &self.expression_path {
            work.step()?;
            expression = match (step, expression) {
                (SqlConstantExpressionStep::Nested, ast::Expr::Nested(nested)) => {
                    &nested.expression
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
}
impl MaterializedSourceBorrow<'_> {
    pub(crate) fn view(&self) -> MaterializedValuesLoan<'_> {
        MaterializedValuesLoan {
            expressions: &self.expressions,
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) struct MaterializedValuesLoan<'a> {
    expressions: &'a std::collections::BTreeMap<usize, novarocks_functions::ConstantValue>,
}
impl<'a> MaterializedValuesLoan<'a> {
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
        })
    }
}
#[cfg(test)]
#[path = "materialized_values_tests.rs"]
mod tests;
