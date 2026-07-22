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

//! Semantic analyzer: converts `sqlparser::ast::Query` into `ResolvedQuery`.
//!
//! This module performs name resolution, type inference, and scope management
//! without producing any physical plan concepts (tuple_id, slot_id, etc.).

mod functions;
mod helpers;
mod literal_coercion;
#[cfg(test)]
mod load_op_column;
mod resolve_expr;
mod resolve_from;
mod scope;
mod subquery_rewrite;

pub mod alter_iceberg_ref;
pub mod iceberg_metadata;
pub mod iceberg_ref;
pub(crate) mod mv_lineage;

use arrow::datatypes::DataType;
use sqlparser::ast as sqlast;

use crate::sql::catalog::PlannerTableProvider;
use crate::sql::column_id::ColumnId;

use crate::sql::analysis::{
    ExprKind, JoinKind, JoinRelation, OutputColumn, ProjectItem, QueryBody, Relation,
    ResolvedQuery, ResolvedSelect, ResolvedSetOp, ResolvedValues, SetOpKind, SortItem,
    SubqueryInfo, TypedExpr,
};
use novarocks_types::wider_type;

use helpers::{expr_display_name, extract_limit, extract_offset};
use scope::AnalyzerScope;

#[derive(Clone, Debug)]
struct RepeatGroupBySpec {
    grouping_sets: Vec<Vec<sqlast::Expr>>,
    all_group_by_exprs: Vec<sqlast::Expr>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Analyze a parsed SQL query and produce a fully resolved query IR,
/// along with a registry of all non-recursive CTE definitions.
pub(crate) fn analyze(
    query: &sqlast::Query,
    catalog: &dyn PlannerTableProvider,
    current_database: &str,
) -> Result<
    (
        ResolvedQuery,
        crate::sql::analysis::cte::CTERegistry,
        crate::sql::column_id::ColumnRefFactory,
    ),
    String,
> {
    analyze_with_factory(
        query,
        catalog,
        current_database,
        crate::sql::column_id::ColumnRefFactory::new(),
    )
}

/// Like [`analyze`], but threads an existing [`ColumnRefFactory`] so that
/// ColumnIds allocated by this analysis never collide with ids the caller
/// already minted (used by MV rewrite candidate preparation, which analyzes
/// the MV defining SQL inside an already-planned user query).
pub(crate) fn analyze_with_factory(
    query: &sqlast::Query,
    catalog: &dyn PlannerTableProvider,
    current_database: &str,
    factory: crate::sql::column_id::ColumnRefFactory,
) -> Result<
    (
        ResolvedQuery,
        crate::sql::analysis::cte::CTERegistry,
        crate::sql::column_id::ColumnRefFactory,
    ),
    String,
> {
    let factory = std::rc::Rc::new(std::cell::RefCell::new(factory));
    let ctx = AnalyzerContext {
        catalog,
        current_database,
        factory: factory.clone(),
        ctes: std::collections::HashMap::new(),
        pending_ctes: std::collections::HashSet::new(),
        next_subquery_id: std::cell::Cell::new(0),
        next_lambda_slot_id: std::cell::Cell::new(0),
        collected_subqueries: std::cell::RefCell::new(Vec::new()),
        cte_registry: std::cell::RefCell::new(crate::sql::analysis::cte::CTERegistry::new()),
    };
    let resolved = ctx.analyze_query(query)?;
    let registry = ctx.cte_registry.into_inner();
    let col_factory = std::rc::Rc::try_unwrap(factory)
        .map(|cell| cell.into_inner())
        .unwrap_or_else(|rc| rc.borrow().clone());
    Ok((resolved, registry, col_factory))
}

// ---------------------------------------------------------------------------
// Analyzer context
// ---------------------------------------------------------------------------

pub(super) struct AnalyzerContext<'a> {
    pub(super) catalog: &'a dyn PlannerTableProvider,
    pub(super) current_database: &'a str,
    /// Shared factory for allocating globally unique ColumnIds.
    pub(super) factory: std::rc::Rc<std::cell::RefCell<crate::sql::column_id::ColumnRefFactory>>,
    /// Currently visible CTE definitions from outer scopes or earlier entries
    /// in the same WITH clause, keyed by lowercase name.
    pub(super) ctes: std::collections::HashMap<String, crate::sql::analysis::cte::CteId>,
    /// Names declared by the current WITH clause but not yet visible because
    /// their definitions have not been analyzed.
    pub(super) pending_ctes: std::collections::HashSet<String>,
    /// Counter for generating unique subquery placeholder IDs.
    pub(super) next_subquery_id: std::cell::Cell<usize>,
    /// Counter for generating synthetic slot ids used only inside lambda expressions.
    pub(super) next_lambda_slot_id: std::cell::Cell<i32>,
    /// Subqueries collected during expression analysis.
    /// Populated by `resolve_expr.rs`, consumed by `subquery_rewrite.rs`.
    pub(super) collected_subqueries: std::cell::RefCell<Vec<SubqueryInfo>>,
    /// Accumulated CTE registry for the current query analysis.
    pub(super) cte_registry: std::cell::RefCell<crate::sql::analysis::cte::CTERegistry>,
}

impl<'a> AnalyzerContext<'a> {
    /// Create a new empty AnalyzerScope sharing this context's factory.
    pub(super) fn new_scope(&self) -> scope::AnalyzerScope {
        scope::AnalyzerScope::new(self.factory.clone())
    }

    /// Allocate a fresh ColumnId from the shared factory.
    pub(super) fn alloc_column_id(
        &self,
        qualifier: Option<String>,
        name: String,
        data_type: arrow::datatypes::DataType,
        nullable: bool,
    ) -> crate::sql::column_id::ColumnId {
        self.factory
            .borrow_mut()
            .create(qualifier, name, data_type, nullable)
    }

    /// Allocate a unique subquery placeholder ID.
    pub(super) fn alloc_subquery_id(&self) -> usize {
        let id = self.next_subquery_id.get();
        self.next_subquery_id.set(id + 1);
        id
    }

    pub(super) fn alloc_lambda_slot_id(&self) -> i32 {
        const LAMBDA_SLOT_ID_BASE: i32 = 1_900_000_000;
        let offset = self.next_lambda_slot_id.get();
        self.next_lambda_slot_id.set(offset + 1);
        LAMBDA_SLOT_ID_BASE - offset
    }

    fn build_with_clause_context(
        &self,
        with_clause: &sqlast::With,
    ) -> Result<(AnalyzerContext<'a>, Vec<crate::sql::analysis::cte::CteId>), String> {
        let mut pending_ctes = self.pending_ctes.clone();
        pending_ctes.extend(
            with_clause
                .cte_tables
                .iter()
                .map(|cte| cte.alias.name.value.to_lowercase()),
        );

        let mut child_ctx = AnalyzerContext {
            catalog: self.catalog,
            current_database: self.current_database,
            factory: self.factory.clone(),
            ctes: self.ctes.clone(),
            pending_ctes: pending_ctes.clone(),
            next_subquery_id: std::cell::Cell::new(self.next_subquery_id.get()),
            next_lambda_slot_id: std::cell::Cell::new(self.next_lambda_slot_id.get()),
            collected_subqueries: std::cell::RefCell::new(Vec::new()),
            cte_registry: std::cell::RefCell::new(self.cte_registry.borrow().clone()),
        };
        let mut local_cte_ids = Vec::with_capacity(with_clause.cte_tables.len());

        for cte in &with_clause.cte_tables {
            let name = cte.alias.name.value.to_lowercase();
            pending_ctes.remove(&name);
            child_ctx.pending_ctes = pending_ctes.clone();

            let col_aliases: Vec<String> = cte
                .alias
                .columns
                .iter()
                .map(|ident| ident.name.value.clone())
                .collect();

            let mut resolved_cte = child_ctx.analyze_query(&cte.query)?;
            if !col_aliases.is_empty() {
                for (idx, alias_name) in col_aliases.iter().enumerate() {
                    if let Some(col) = resolved_cte.output_columns.get_mut(idx) {
                        col.name = alias_name.clone();
                    }
                }
            }

            let output_columns = resolved_cte.output_columns.clone();
            let cte_id = child_ctx.cte_registry.borrow_mut().register(
                name.clone(),
                resolved_cte,
                output_columns,
            );

            child_ctx.ctes.insert(name, cte_id);
            local_cte_ids.push(cte_id);
        }

        Ok((child_ctx, local_cte_ids))
    }

    /// Top-level query analysis.
    fn analyze_query(&self, query: &sqlast::Query) -> Result<ResolvedQuery, String> {
        let (maybe_child_ctx, local_cte_ids) = if let Some(ref with_clause) = query.with {
            let (child_ctx, local_cte_ids) = self.build_with_clause_context(with_clause)?;
            (Some(child_ctx), local_cte_ids)
        } else {
            (None, Vec::new())
        };
        let ctx = maybe_child_ctx.as_ref().unwrap_or(self);

        // Analyze body (SELECT / SetOperation / VALUES)
        let (body, body_output) = ctx.analyze_set_expr(query.body.as_ref())?;

        // Analyze ORDER BY
        let order_by = ctx.analyze_order_by(query, &body_output, &body)?;

        // Extract LIMIT / OFFSET
        let limit = extract_limit(query)?;
        let offset = extract_offset(query)?;

        if let Some(child_ctx) = maybe_child_ctx {
            *self.cte_registry.borrow_mut() = child_ctx.cte_registry.borrow().clone();
        }

        // Build output columns from the body
        let output_columns = body_output;

        Ok(ResolvedQuery {
            body,
            order_by,
            limit,
            offset,
            output_columns,
            local_cte_ids,
        })
    }

    /// Analyze a SetExpr and return (QueryBody, output_columns).
    fn analyze_set_expr(
        &self,
        set_expr: &sqlast::SetExpr,
    ) -> Result<(QueryBody, Vec<OutputColumn>), String> {
        match set_expr {
            sqlast::SetExpr::Select(s) => {
                // Check if GROUP BY contains ROLLUP/CUBE/GROUPING SETS.
                if let Some(repeat_spec) = self.extract_repeat_from_group_by(s) {
                    return self.resolve_repeat_group_by(s, &repeat_spec);
                }
                let (sel, cols) = self.analyze_select(s)?;
                Ok((QueryBody::Select(sel), cols))
            }
            sqlast::SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let left_query = self.analyze_set_operand(left)?;
                let right_query = self.analyze_set_operand(right)?;
                let left_cols = left_query.output_columns.clone();
                let right_cols = right_query.output_columns.clone();

                // Validate column count
                if left_cols.len() != right_cols.len() {
                    return Err(format!(
                        "set operation column count mismatch: left has {}, right has {}",
                        left_cols.len(),
                        right_cols.len()
                    ));
                }

                // Widen types
                let mut output_cols = Vec::with_capacity(left_cols.len());
                for (lc, rc) in left_cols.iter().zip(right_cols.iter()) {
                    let dt = wider_type(&lc.data_type, &rc.data_type);
                    let column_id = self.alloc_column_id(
                        None,
                        lc.name.clone(),
                        dt.clone(),
                        lc.nullable || rc.nullable,
                    );
                    output_cols.push(OutputColumn {
                        column_id,
                        name: lc.name.clone(),
                        data_type: dt,
                        nullable: lc.nullable || rc.nullable,
                        is_internal: false,
                    });
                }

                let kind = match op {
                    sqlast::SetOperator::Union => SetOpKind::Union,
                    sqlast::SetOperator::Intersect => SetOpKind::Intersect,
                    sqlast::SetOperator::Except | sqlast::SetOperator::Minus => SetOpKind::Except,
                };
                let all = matches!(
                    set_quantifier,
                    sqlast::SetQuantifier::All | sqlast::SetQuantifier::AllByName
                );

                Ok((
                    QueryBody::SetOperation(ResolvedSetOp {
                        kind,
                        all,
                        left: Box::new(left_query),
                        right: Box::new(right_query),
                    }),
                    output_cols,
                ))
            }
            sqlast::SetExpr::Values(values) => {
                let (resolved_values, cols) = self.analyze_values(values)?;
                Ok((QueryBody::Values(resolved_values), cols))
            }
            sqlast::SetExpr::Query(q) => {
                let resolved = self.analyze_query(q)?;
                let cols = resolved.output_columns.clone();
                Ok((resolved.body, cols))
            }
            other => Err(format!("unsupported set expression: {other}")),
        }
    }

    fn analyze_set_operand(&self, set_expr: &sqlast::SetExpr) -> Result<ResolvedQuery, String> {
        match set_expr {
            sqlast::SetExpr::Query(q) => self.analyze_query(q),
            _ => {
                let (body, output_columns) = self.analyze_set_expr(set_expr)?;
                Ok(ResolvedQuery {
                    body,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    output_columns,
                    local_cte_ids: vec![],
                })
            }
        }
    }

    /// Analyze a VALUES clause.
    fn analyze_values(
        &self,
        values: &sqlast::Values,
    ) -> Result<(ResolvedValues, Vec<OutputColumn>), String> {
        let scope = self.new_scope(); // VALUES has no table scope
        let mut resolved_rows = Vec::with_capacity(values.rows.len());
        let mut column_types: Vec<DataType> = Vec::new();

        for row in &values.rows {
            let mut resolved_row = Vec::with_capacity(row.len());
            for (col_idx, expr) in row.iter().enumerate() {
                let typed = self.analyze_expr(expr, &scope)?;
                if col_idx < column_types.len() {
                    column_types[col_idx] = wider_type(&column_types[col_idx], &typed.data_type);
                } else {
                    column_types.push(typed.data_type.clone());
                }
                resolved_row.push(typed);
            }
            resolved_rows.push(resolved_row);
        }

        let output_cols: Vec<OutputColumn> = column_types
            .iter()
            .enumerate()
            .map(|(i, dt)| {
                let name = format!("column_{i}");
                let column_id = self.alloc_column_id(None, name.clone(), dt.clone(), true);
                OutputColumn {
                    column_id,
                    name,
                    data_type: dt.clone(),
                    nullable: true,
                    is_internal: false,
                }
            })
            .collect();

        Ok((
            ResolvedValues {
                rows: resolved_rows,
                output_columns: output_cols.clone(),
            },
            output_cols,
        ))
    }

    /// Analyze a SELECT statement.
    fn analyze_select(
        &self,
        select: &sqlast::Select,
    ) -> Result<(ResolvedSelect, Vec<OutputColumn>), String> {
        // --- FROM clause ---
        let (from, scope) = if select.from.is_empty() {
            // SELECT without FROM (dual)
            (None, self.new_scope())
        } else if select.from.len() == 1 {
            let (rel, scope) = self.analyze_from(&select.from[0])?;
            (Some(rel), scope)
        } else {
            // Multiple comma-separated FROM items → implicit CROSS JOIN.
            // Subsequent entries see the accumulated left-hand scope so
            // table-valued functions (e.g. `unnest(t.arr)`) can reference
            // earlier sibling columns (StarRocks implicit-lateral semantics).
            let mut iter = select.from.iter();
            let first = iter.next().unwrap();
            let (mut current_rel, mut current_scope) = self.analyze_from(first)?;
            for twj in iter {
                let (right_rel, right_scope) =
                    self.analyze_from_with_outer(twj, Some(&current_scope))?;
                current_scope.merge(&right_scope);
                current_rel = Relation::Join(Box::new(JoinRelation {
                    left: current_rel,
                    right: right_rel,
                    join_type: JoinKind::Cross,
                    condition: None,
                }));
            }
            (Some(current_rel), current_scope)
        };

        // --- WHERE clause ---
        let filter = match &select.selection {
            Some(expr) => Some(super::analyzer::subquery_rewrite::coerce_where_to_bool(
                self.analyze_expr(expr, &scope)?,
            )),
            None => None,
        };

        // --- SELECT list (before GROUP BY so aliases are available) ---
        let (projection, mut output_columns) =
            self.analyze_projection(&select.projection, &scope)?;

        // --- GROUP BY (with SELECT alias fallback) ---
        let group_by_exprs = match &select.group_by {
            sqlast::GroupByExpr::Expressions(exprs, _) => exprs.clone(),
            sqlast::GroupByExpr::All(_) => {
                return Err("GROUP BY ALL is not supported".into());
            }
        };
        let mut group_by = Vec::with_capacity(group_by_exprs.len());
        for gb_expr in &group_by_exprs {
            if let sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::Number(n, _),
                ..
            }) = gb_expr
            {
                let pos = n
                    .parse::<usize>()
                    .map_err(|e| format!("invalid GROUP BY position: {e}"))?;
                if pos == 0 || pos > projection.len() {
                    return Err(format!(
                        "GROUP BY position {pos} is out of range (1..{})",
                        projection.len()
                    ));
                }
                let select_item = select
                    .projection
                    .get(pos - 1)
                    .ok_or_else(|| format!("GROUP BY position {pos} is out of range"))?;
                let select_expr = match select_item {
                    sqlast::SelectItem::UnnamedExpr(expr)
                    | sqlast::SelectItem::ExprWithAlias { expr, .. } => expr,
                    _ => {
                        return Err(format!(
                            "GROUP BY position {pos} must reference a select expression"
                        ));
                    }
                };
                if self.expr_contains_aggregate(select_expr) {
                    return Err(format!(
                        "GROUP BY position {pos} cannot reference an aggregate expression"
                    ));
                }
                if contains_subquery_placeholder(&projection[pos - 1].expr) {
                    return Err("subquery is not supported in GROUP BY".to_string());
                }
                group_by.push(projection[pos - 1].expr.clone());
                continue;
            }
            match self.analyze_expr(gb_expr, &scope) {
                Ok(typed) => {
                    if contains_subquery_placeholder(&typed) {
                        return Err("subquery is not supported in GROUP BY".to_string());
                    }
                    group_by.push(typed);
                }
                Err(_) => {
                    // Try SELECT aliases: GROUP BY alias_name
                    let mut alias_scope = scope.clone();
                    for item in &projection {
                        alias_scope.add_column(
                            None,
                            &item.output_name,
                            item.expr.data_type.clone(),
                            item.expr.nullable,
                        );
                    }
                    let typed = self.analyze_expr(gb_expr, &alias_scope)?;
                    // Substitute alias ref with original expression
                    let typed = self.substitute_select_aliases(typed, &projection);
                    if contains_subquery_placeholder(&typed) {
                        return Err("subquery is not supported in GROUP BY".to_string());
                    }
                    group_by.push(typed);
                }
            }
        }

        // BITMAP / HLL columns cannot participate in GROUP BY because they
        // have no comparable scalar identity. Reject upfront so the user
        // sees a clear error before lowering / codegen.
        for gb in &group_by {
            if let Some(logical) = scope.logical_type_of_expr(gb).filter(is_bitmap_or_hll_type) {
                return Err(format!(
                    "BITMAP/HLL columns cannot appear in GROUP BY (column has type {logical:?})"
                ));
            }
        }

        // --- Detect aggregation ---
        let has_agg_in_select = self.select_has_aggregate_functions(&select.projection);
        let has_aggregation = !group_by.is_empty() || has_agg_in_select;

        // --- HAVING ---
        // Resolve against the FROM scope. If a HAVING reference matches a SELECT
        // alias, substitute with the aliased expression so the emitter sees the
        // real aggregate call (e.g. `total` → `sum(v)`).
        let having = match &select.having {
            Some(expr) => {
                let analyzed = self.analyze_expr(expr, &scope);
                match analyzed {
                    Ok(h) => Some(h),
                    Err(_) => {
                        // Maybe references a SELECT alias — build alias scope
                        let mut alias_scope = scope.clone();
                        for item in &projection {
                            alias_scope.add_column(
                                None,
                                &item.output_name,
                                item.expr.data_type.clone(),
                                item.expr.nullable,
                            );
                        }
                        let h = self.analyze_expr(expr, &alias_scope)?;
                        // Substitute alias refs with real expressions
                        Some(self.substitute_select_aliases(h, &projection))
                    }
                }
            }
            None => None,
        };

        // --- DISTINCT ---
        let distinct = matches!(select.distinct, Some(sqlast::Distinct::Distinct));

        let mut resolved_select = ResolvedSelect {
            from,
            filter,
            group_by,
            having,
            projection,
            has_aggregation,
            distinct,
            repeat: None,
            apply_specs: Vec::new(),
            predicate_apply_specs: Vec::new(),
        };

        // --- Subquery rewriting ---
        // If the WHERE or HAVING clause contained subqueries (recorded as
        // SubqueryPlaceholder nodes), rewrite them into JOINs now.
        let has_subqueries = !self.collected_subqueries.borrow().is_empty();
        if has_subqueries {
            let mut mutable_scope = scope;
            self.rewrite_subqueries(&mut resolved_select, &mut mutable_scope)?;
            sync_output_columns_from_projection(&mut output_columns, &resolved_select.projection);
        }

        Ok((resolved_select, output_columns))
    }

    /// Replace ColumnRef nodes that match SELECT aliases with the aliased expression.
    fn substitute_select_aliases(&self, expr: TypedExpr, projection: &[ProjectItem]) -> TypedExpr {
        self.substitute_select_aliases_inner(expr, projection, false)
    }

    /// Like [`substitute_select_aliases`] but only rewrites a `ColumnRef`
    /// when the name does NOT also resolve as a column from the FROM scope.
    /// SELECT items reference table columns far more often than they
    /// reference earlier aliases, so suppress the rewrite whenever the
    /// FROM column is in scope to avoid breaking semantics for queries
    /// like `SELECT n + 1 AS n, (n + 1) * (n + 1) AS sq FROM t`.
    fn substitute_select_aliases_for_select(
        &self,
        expr: TypedExpr,
        projection: &[ProjectItem],
        from_scope: &AnalyzerScope,
    ) -> TypedExpr {
        self.substitute_select_aliases_for_select_inner(expr, projection, from_scope, false)
    }

    fn substitute_select_aliases_for_select_inner(
        &self,
        expr: TypedExpr,
        projection: &[ProjectItem],
        from_scope: &AnalyzerScope,
        inside_agg: bool,
    ) -> TypedExpr {
        match expr.kind {
            ExprKind::ColumnRef {
                ref qualifier,
                ref column,
                ..
            } if qualifier.is_none() && !inside_agg => {
                if from_scope.resolve(None, column).is_ok() {
                    // The FROM clause already binds this column; do not
                    // shadow it with an earlier SELECT alias.
                    return expr;
                }
                let col_lower = column.to_lowercase();
                for item in projection {
                    if item.output_name.to_lowercase() == col_lower {
                        return item.expr.clone();
                    }
                }
                expr
            }
            ExprKind::BinaryOp { left, op, right } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::BinaryOp {
                    left: Box::new(self.substitute_select_aliases_for_select_inner(
                        *left, projection, from_scope, inside_agg,
                    )),
                    op,
                    right: Box::new(self.substitute_select_aliases_for_select_inner(
                        *right, projection, from_scope, inside_agg,
                    )),
                },
            },
            ExprKind::UnaryOp { op, expr: inner } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::UnaryOp {
                    op,
                    expr: Box::new(self.substitute_select_aliases_for_select_inner(
                        *inner, projection, from_scope, inside_agg,
                    )),
                },
            },
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
            } => {
                let is_agg = crate::sql::analyzer::functions::is_aggregate_function(&name);
                TypedExpr {
                    data_type: expr.data_type,
                    nullable: expr.nullable,
                    kind: ExprKind::FunctionCall {
                        name,
                        args: args
                            .into_iter()
                            .map(|arg| {
                                self.substitute_select_aliases_for_select_inner(
                                    arg,
                                    projection,
                                    from_scope,
                                    inside_agg || is_agg,
                                )
                            })
                            .collect(),
                        distinct,
                    },
                }
            }
            ExprKind::LambdaFunction { params, body } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::LambdaFunction {
                    params,
                    body: Box::new(self.substitute_select_aliases_for_select_inner(
                        *body, projection, from_scope, inside_agg,
                    )),
                },
            },
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::AggregateCall {
                    name,
                    args: args
                        .into_iter()
                        .map(|arg| {
                            self.substitute_select_aliases_for_select_inner(
                                arg, projection, from_scope, true,
                            )
                        })
                        .collect(),
                    distinct,
                    order_by: order_by
                        .into_iter()
                        .map(|item| SortItem {
                            expr: self.substitute_select_aliases_for_select_inner(
                                item.expr, projection, from_scope, true,
                            ),
                            ..item
                        })
                        .collect(),
                },
            },
            ExprKind::Cast {
                expr: inner,
                target,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::Cast {
                    expr: Box::new(self.substitute_select_aliases_for_select_inner(
                        *inner, projection, from_scope, inside_agg,
                    )),
                    target,
                },
            },
            ExprKind::Nested(inner) => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::Nested(Box::new(self.substitute_select_aliases_for_select_inner(
                    *inner, projection, from_scope, inside_agg,
                ))),
            },
            ExprKind::IsNull {
                expr: inner,
                negated,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::IsNull {
                    expr: Box::new(self.substitute_select_aliases_for_select_inner(
                        *inner, projection, from_scope, inside_agg,
                    )),
                    negated,
                },
            },
            ExprKind::IsTruthValue {
                expr: inner,
                value,
                negated,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::IsTruthValue {
                    expr: Box::new(self.substitute_select_aliases_for_select_inner(
                        *inner, projection, from_scope, inside_agg,
                    )),
                    value,
                    negated,
                },
            },
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::Case {
                    operand: operand.map(|expr| {
                        Box::new(self.substitute_select_aliases_for_select_inner(
                            *expr, projection, from_scope, inside_agg,
                        ))
                    }),
                    when_then: when_then
                        .into_iter()
                        .map(|(when, then)| {
                            (
                                self.substitute_select_aliases_for_select_inner(
                                    when, projection, from_scope, inside_agg,
                                ),
                                self.substitute_select_aliases_for_select_inner(
                                    then, projection, from_scope, inside_agg,
                                ),
                            )
                        })
                        .collect(),
                    else_expr: else_expr.map(|expr| {
                        Box::new(self.substitute_select_aliases_for_select_inner(
                            *expr, projection, from_scope, inside_agg,
                        ))
                    }),
                },
            },
            _ => expr,
        }
    }

    fn substitute_select_aliases_inner(
        &self,
        expr: TypedExpr,
        projection: &[ProjectItem],
        inside_agg: bool,
    ) -> TypedExpr {
        match expr.kind {
            ExprKind::ColumnRef {
                ref qualifier,
                ref column,
                ..
            } if qualifier.is_none() && !inside_agg => {
                // Check if this column name matches a SELECT alias
                let col_lower = column.to_lowercase();
                for item in projection {
                    if item.output_name.to_lowercase() == col_lower {
                        return item.expr.clone();
                    }
                }
                expr
            }
            ExprKind::BinaryOp { left, op, right } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::BinaryOp {
                    left: Box::new(
                        self.substitute_select_aliases_inner(*left, projection, inside_agg),
                    ),
                    op,
                    right: Box::new(
                        self.substitute_select_aliases_inner(*right, projection, inside_agg),
                    ),
                },
            },
            ExprKind::UnaryOp { op, expr: inner } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::UnaryOp {
                    op,
                    expr: Box::new(
                        self.substitute_select_aliases_inner(*inner, projection, inside_agg),
                    ),
                },
            },
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::FunctionCall {
                    name,
                    args: args
                        .into_iter()
                        .map(|arg| {
                            self.substitute_select_aliases_inner(arg, projection, inside_agg)
                        })
                        .collect(),
                    distinct,
                },
            },
            ExprKind::LambdaFunction { params, body } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::LambdaFunction {
                    params,
                    body: Box::new(
                        self.substitute_select_aliases_inner(*body, projection, inside_agg),
                    ),
                },
            },
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::AggregateCall {
                    name,
                    args: args
                        .into_iter()
                        .map(|arg| self.substitute_select_aliases_inner(arg, projection, true))
                        .collect(),
                    distinct,
                    order_by: order_by
                        .into_iter()
                        .map(|item| SortItem {
                            expr: self.substitute_select_aliases_inner(item.expr, projection, true),
                            ..item
                        })
                        .collect(),
                },
            },
            ExprKind::Cast {
                expr: inner,
                target,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::Cast {
                    expr: Box::new(
                        self.substitute_select_aliases_inner(*inner, projection, inside_agg),
                    ),
                    target,
                },
            },
            ExprKind::Nested(inner) => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::Nested(Box::new(
                    self.substitute_select_aliases_inner(*inner, projection, inside_agg),
                )),
            },
            ExprKind::IsNull {
                expr: inner,
                negated,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::IsNull {
                    expr: Box::new(
                        self.substitute_select_aliases_inner(*inner, projection, inside_agg),
                    ),
                    negated,
                },
            },
            ExprKind::IsTruthValue {
                expr: inner,
                value,
                negated,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::IsTruthValue {
                    expr: Box::new(
                        self.substitute_select_aliases_inner(*inner, projection, inside_agg),
                    ),
                    value,
                    negated,
                },
            },
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::Case {
                    operand: operand.map(|expr| {
                        Box::new(
                            self.substitute_select_aliases_inner(*expr, projection, inside_agg),
                        )
                    }),
                    when_then: when_then
                        .into_iter()
                        .map(|(when, then)| {
                            (
                                self.substitute_select_aliases_inner(when, projection, inside_agg),
                                self.substitute_select_aliases_inner(then, projection, inside_agg),
                            )
                        })
                        .collect(),
                    else_expr: else_expr.map(|expr| {
                        Box::new(
                            self.substitute_select_aliases_inner(*expr, projection, inside_agg),
                        )
                    }),
                },
            },
            // For other node types, return as-is
            _ => expr,
        }
    }

    /// Check if a SELECT's GROUP BY clause contains ROLLUP/CUBE/GROUPING SETS.
    /// Returns the explicit grouping-set levels plus the full GROUP BY key list.
    fn extract_repeat_from_group_by(&self, select: &sqlast::Select) -> Option<RepeatGroupBySpec> {
        let (exprs, modifiers) = match &select.group_by {
            sqlast::GroupByExpr::Expressions(exprs, modifiers) => (exprs.as_slice(), modifiers),
            sqlast::GroupByExpr::All(modifiers) => (&[][..], modifiers),
        };

        for expr in exprs {
            match expr {
                sqlast::Expr::Rollup(groups) => {
                    return Some(RepeatGroupBySpec {
                        grouping_sets: rollup_grouping_sets(groups),
                        all_group_by_exprs: flatten_grouping_groups(groups),
                    });
                }
                sqlast::Expr::Cube(groups) => {
                    return Some(RepeatGroupBySpec {
                        grouping_sets: cube_grouping_sets(groups),
                        all_group_by_exprs: flatten_grouping_groups(groups),
                    });
                }
                sqlast::Expr::GroupingSets(sets) => {
                    return Some(RepeatGroupBySpec {
                        grouping_sets: sets.clone(),
                        all_group_by_exprs: unique_exprs_in_order(
                            sets.iter().flat_map(|set| set.iter().cloned()),
                        ),
                    });
                }
                sqlast::Expr::Function(func) => {
                    let func_name = func.name.to_string().to_lowercase();
                    let sqlast::FunctionArguments::List(arg_list) = &func.args else {
                        continue;
                    };
                    let groups: Vec<Vec<sqlast::Expr>> = arg_list
                        .args
                        .iter()
                        .filter_map(|arg| match arg {
                            sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) => {
                                Some(vec![e.clone()])
                            }
                            _ => None,
                        })
                        .collect();
                    if groups.is_empty() {
                        continue;
                    }
                    match func_name.as_str() {
                        "rollup" => {
                            return Some(RepeatGroupBySpec {
                                grouping_sets: rollup_grouping_sets(&groups),
                                all_group_by_exprs: flatten_grouping_groups(&groups),
                            });
                        }
                        "cube" => {
                            return Some(RepeatGroupBySpec {
                                grouping_sets: cube_grouping_sets(&groups),
                                all_group_by_exprs: flatten_grouping_groups(&groups),
                            });
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        if exprs.is_empty() {
            for modifier in modifiers {
                match modifier {
                    sqlast::GroupByWithModifier::Rollup => {
                        return Some(RepeatGroupBySpec {
                            grouping_sets: rollup_grouping_sets(&[]),
                            all_group_by_exprs: vec![],
                        });
                    }
                    sqlast::GroupByWithModifier::Cube => {
                        return Some(RepeatGroupBySpec {
                            grouping_sets: cube_grouping_sets(&[]),
                            all_group_by_exprs: vec![],
                        });
                    }
                    sqlast::GroupByWithModifier::GroupingSets(sqlast::Expr::GroupingSets(sets)) => {
                        return Some(RepeatGroupBySpec {
                            grouping_sets: sets.clone(),
                            all_group_by_exprs: unique_exprs_in_order(
                                sets.iter().flat_map(|set| set.iter().cloned()),
                            ),
                        });
                    }
                    _ => {}
                }
            }
        } else {
            let singleton_groups: Vec<Vec<sqlast::Expr>> =
                exprs.iter().cloned().map(|expr| vec![expr]).collect();
            for modifier in modifiers {
                match modifier {
                    sqlast::GroupByWithModifier::Rollup => {
                        return Some(RepeatGroupBySpec {
                            grouping_sets: rollup_grouping_sets(&singleton_groups),
                            all_group_by_exprs: exprs.to_vec(),
                        });
                    }
                    sqlast::GroupByWithModifier::Cube => {
                        return Some(RepeatGroupBySpec {
                            grouping_sets: cube_grouping_sets(&singleton_groups),
                            all_group_by_exprs: exprs.to_vec(),
                        });
                    }
                    sqlast::GroupByWithModifier::GroupingSets(sqlast::Expr::GroupingSets(sets)) => {
                        return Some(RepeatGroupBySpec {
                            grouping_sets: sets.clone(),
                            all_group_by_exprs: unique_exprs_in_order(
                                sets.iter().flat_map(|set| set.iter().cloned()),
                            ),
                        });
                    }
                    _ => {}
                }
            }
        }
        None
    }

    /// Expand `GROUP BY ROLLUP(a, b, ...)` into a UNION ALL of GROUP BY variants.
    ///
    /// `ROLLUP(a, b)` expands to:
    ///   SELECT a, b, agg(...) ... GROUP BY a, b
    ///   UNION ALL
    ///   SELECT a, NULL, agg(...) ... GROUP BY a
    ///   UNION ALL
    ///   SELECT NULL, NULL, agg(...) ... (no GROUP BY, full aggregation)
    ///
    /// NOTE: This method is superseded by `resolve_rollup` which produces a
    /// single-pass RepeatInfo instead. Kept temporarily for reference and will
    /// be removed in a later cleanup pass.
    #[allow(dead_code)]
    fn expand_rollup(
        &self,
        select: &sqlast::Select,
        rollup_groups: &[Vec<sqlast::Expr>],
    ) -> Result<(QueryBody, Vec<OutputColumn>), String> {
        // Flatten the rollup groups: each inner Vec is a "composite key"
        // (usually single element). For ROLLUP(a, b), groups = [[a], [b]].
        let n = rollup_groups.len();

        // Build n+1 levels: level i has the first (n-i) groups.
        // Level 0: all groups (a, b)  →  GROUP BY a, b
        // Level 1: first (n-1) groups (a) → GROUP BY a, select b as NULL
        // Level n: no groups → select a as NULL, b as NULL
        let mut bodies: Vec<(QueryBody, Vec<OutputColumn>)> = Vec::new();

        for level in 0..=n {
            let active_count = n - level; // number of active rollup groups

            // Build a modified GROUP BY expressions list:
            // - Keep first `active_count` groups as real GROUP BY keys
            // - The remaining groups are NULLed out in the projection
            let mut modified_gb_exprs: Vec<sqlast::Expr> = Vec::new();
            for group in rollup_groups.iter().take(active_count) {
                for expr in group {
                    modified_gb_exprs.push(expr.clone());
                }
            }

            // Build the set of NULLed column names (from inactive rollup groups)
            let mut nulled_exprs: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for group in rollup_groups.iter().skip(active_count) {
                for expr in group {
                    nulled_exprs.insert(format!("{expr}").to_lowercase());
                }
            }

            // Build modified SELECT with the adjusted GROUP BY
            // We need to reconstruct the AST Select with modified group_by and projection
            let mut modified_select = select.clone();

            // Replace GROUP BY with the active keys only
            modified_select.group_by = sqlast::GroupByExpr::Expressions(modified_gb_exprs, vec![]);

            // Modify projection: replace NULLed columns with NULL literals,
            // and replace GROUPING(col) calls with literal 0 or 1.
            let mut modified_projection = Vec::new();
            for item in &select.projection {
                let (expr_part, alias_part) = match item {
                    sqlast::SelectItem::ExprWithAlias { expr, alias } => {
                        (expr, Some(alias.clone()))
                    }
                    sqlast::SelectItem::UnnamedExpr(expr) => (expr, None),
                    other => {
                        modified_projection.push(other.clone());
                        continue;
                    }
                };

                // Check if this projection item is one of the rollup keys
                // that should be NULLed at this level
                let expr_str = format!("{expr_part}").to_lowercase();
                if nulled_exprs.contains(&expr_str) {
                    let null_expr = sqlast::Expr::Value(sqlast::Value::Null.into());
                    if let Some(alias) = alias_part {
                        modified_projection.push(sqlast::SelectItem::ExprWithAlias {
                            expr: null_expr,
                            alias,
                        });
                    } else {
                        // Preserve the original name by adding an alias
                        let name = expr_display_name(expr_part);
                        modified_projection.push(sqlast::SelectItem::ExprWithAlias {
                            expr: null_expr,
                            alias: sqlast::Ident::new(name),
                        });
                    }
                } else {
                    // Replace GROUPING(col) calls with 0 or 1
                    let rewritten = replace_grouping_calls(expr_part, &nulled_exprs);
                    if let Some(alias) = alias_part {
                        modified_projection.push(sqlast::SelectItem::ExprWithAlias {
                            expr: rewritten,
                            alias,
                        });
                    } else {
                        modified_projection.push(sqlast::SelectItem::UnnamedExpr(rewritten));
                    }
                }
            }
            modified_select.projection = modified_projection;

            let (sel, cols) = self.analyze_select(&modified_select)?;
            bodies.push((QueryBody::Select(sel), cols));
        }

        // Build UNION ALL chain from right to left
        let (mut result_body, result_cols) = bodies.remove(0);
        for (body, cols) in bodies {
            result_body = QueryBody::SetOperation(ResolvedSetOp {
                kind: SetOpKind::Union,
                all: true,
                left: Box::new(ResolvedQuery {
                    body: result_body,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    output_columns: result_cols.clone(),
                    local_cte_ids: vec![],
                }),
                right: Box::new(ResolvedQuery {
                    body,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    output_columns: cols,
                    local_cte_ids: vec![],
                }),
            });
        }

        Ok((result_body, result_cols))
    }

    /// Resolve `GROUP BY ROLLUP/CUBE/GROUPING SETS` into a single SELECT with
    /// RepeatInfo instead of rewriting to UNION ALL branches.
    ///
    /// The SELECT is analyzed once with the union of all grouping keys in the
    /// GROUP BY. RepeatInfo records the per-level null patterns and grouping_id
    /// bitmaps so the Repeat operator can replay the grouping-set semantics.
    fn resolve_repeat_group_by(
        &self,
        select: &sqlast::Select,
        repeat_spec: &RepeatGroupBySpec,
    ) -> Result<(QueryBody, Vec<OutputColumn>), String> {
        use crate::sql::analysis::RepeatInfo;

        let all_rollup_columns: Vec<String> = repeat_spec
            .all_group_by_exprs
            .iter()
            .map(|expr| format!("{expr}").to_lowercase())
            .collect();

        // StarRocks uses the last grouping argument as the least-significant
        // bit, so for GROUPING_ID(a, b) nulling b yields 0b01, not 0b10.
        let total_grouping_columns = all_rollup_columns.len();
        let mut repeat_column_ref_list: Vec<Vec<String>> =
            Vec::with_capacity(repeat_spec.grouping_sets.len());
        let mut grouping_ids: Vec<u64> = Vec::with_capacity(repeat_spec.grouping_sets.len());

        for grouping_set in &repeat_spec.grouping_sets {
            let non_null_cols: Vec<String> = grouping_set
                .iter()
                .map(|expr| format!("{expr}").to_lowercase())
                .collect();
            let active_cols: std::collections::HashSet<String> =
                non_null_cols.iter().cloned().collect();
            repeat_column_ref_list.push(non_null_cols);

            let mut bitmap: u64 = 0;
            for (idx, col_name) in all_rollup_columns.iter().enumerate() {
                if !active_cols.contains(col_name) {
                    let bit_pos = total_grouping_columns - 1 - idx;
                    bitmap |= 1u64 << bit_pos;
                }
            }
            grouping_ids.push(bitmap);
        }

        // Scan ALL GROUPING() calls (top-level and nested) in projection,
        // assign each a unique marker value, and record metadata for RepeatInfo.
        // Use negative marker values (-9000, -9001, ...) so they're distinguishable
        // from real literals after analysis.
        let mut grouping_fn_args: Vec<(String, Vec<String>)> = Vec::new();
        let mut next_marker: i64 = -9000;

        let replace_grouping_with_marker = |expr: &sqlast::Expr,
                                            args: &mut Vec<(String, Vec<String>)>,
                                            marker: &mut i64|
         -> sqlast::Expr {
            replace_grouping_calls_with_markers(expr, args, marker)
        };

        let mut modified_projection = Vec::new();
        for item in &select.projection {
            match item {
                sqlast::SelectItem::ExprWithAlias { expr, alias } => {
                    let rewritten =
                        replace_grouping_with_marker(expr, &mut grouping_fn_args, &mut next_marker);
                    modified_projection.push(sqlast::SelectItem::ExprWithAlias {
                        expr: rewritten,
                        alias: alias.clone(),
                    });
                }
                sqlast::SelectItem::UnnamedExpr(expr) => {
                    let rewritten =
                        replace_grouping_with_marker(expr, &mut grouping_fn_args, &mut next_marker);
                    modified_projection.push(sqlast::SelectItem::UnnamedExpr(rewritten));
                }
                other => modified_projection.push(other.clone()),
            }
        }

        // Build modified SELECT with the union of all grouping keys in GROUP BY.
        let mut modified_select = select.clone();
        modified_select.group_by =
            sqlast::GroupByExpr::Expressions(repeat_spec.all_group_by_exprs.clone(), vec![]);
        modified_select.projection = modified_projection;

        // Replace GROUPING() in HAVING too.
        if let Some(ref having_expr) = modified_select.having {
            modified_select.having = Some(replace_grouping_calls_with_markers(
                having_expr,
                &mut grouping_fn_args,
                &mut next_marker,
            ));
        }
        let emitted_grouping_marker_count = grouping_fn_args.len();

        // Analyze the SELECT once with all GROUP BY keys active.
        let (mut sel, cols) = self.analyze_select(&modified_select)?;

        // When no GROUPING() calls exist, synthesize one for the first rollup
        // column so that __grouping_fn_0 is always in the GROUP BY.  This
        // ensures ROLLUP levels are distinguishable even when grouped columns
        // happen to have the same values (e.g., all NULLs).
        if grouping_fn_args.is_empty() && !all_rollup_columns.is_empty() {
            let virtual_name = "__grouping_fn_0".to_string();
            grouping_fn_args.push((virtual_name, all_rollup_columns.clone()));
        }
        // Also add each GROUPING() virtual column as a GROUP BY key so it
        // passes through the Aggregate operator.
        let mut grouping_fn_ids: Vec<(String, ColumnId)> =
            Vec::with_capacity(grouping_fn_args.len());
        for (fn_name, _) in &grouping_fn_args {
            let column_id = self.alloc_column_id(None, fn_name.clone(), DataType::Int64, false);
            grouping_fn_ids.push((fn_name.clone(), column_id));
            sel.group_by.push(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id,
                    qualifier: None,
                    column: fn_name.clone(),
                },
                data_type: DataType::Int64,
                nullable: false,
            });
        }

        // Post-analysis fixup: replace GROUPING() marker literals in the
        // resolved projection with ColumnRef to the virtual slot names.
        // Markers are Literal(Int(-9000)), Literal(Int(-9001)), etc.
        // Each maps to grouping_fn_args[marker - (-9000)].
        for item in &mut sel.projection {
            item.expr = replace_grouping_markers_in_typed_expr(
                &item.expr,
                &grouping_fn_args,
                &grouping_fn_ids,
                emitted_grouping_marker_count,
            );
        }
        if let Some(having) = sel.having.as_mut() {
            *having = replace_grouping_markers_in_typed_expr(
                having,
                &grouping_fn_args,
                &grouping_fn_ids,
                emitted_grouping_marker_count,
            );
        }
        if let Some(from) = sel.from.as_mut() {
            replace_grouping_markers_in_relation(
                from,
                &grouping_fn_args,
                &grouping_fn_ids,
                emitted_grouping_marker_count,
            );
        }
        for spec in &mut sel.predicate_apply_specs {
            if let Some(in_lhs) = spec.in_lhs.as_mut() {
                *in_lhs = replace_grouping_markers_in_typed_expr(
                    in_lhs,
                    &grouping_fn_args,
                    &grouping_fn_ids,
                    emitted_grouping_marker_count,
                );
            }
        }

        // Attach RepeatInfo to the resolved SELECT.
        sel.repeat = Some(RepeatInfo {
            repeat_column_ref_list,
            repeat_column_ref_ids: Vec::new(),
            grouping_ids,
            all_rollup_columns,
            all_rollup_column_ids: Vec::new(),
            grouping_fn_args,
            grouping_fn_arg_ids: Vec::new(),
            grouping_fn_ids,
        });

        Ok((QueryBody::Select(sel), cols))
    }

    /// Analyze the SELECT projection list.
    fn analyze_projection(
        &self,
        items: &[sqlast::SelectItem],
        scope: &AnalyzerScope,
    ) -> Result<(Vec<ProjectItem>, Vec<OutputColumn>), String> {
        let mut projection: Vec<ProjectItem> = Vec::new();
        let mut output_columns = Vec::new();
        // StarRocks allows later SELECT items to reference earlier item
        // aliases by name. Track those aliases in a parallel scope so name
        // resolution succeeds; afterwards we rewrite every `ColumnRef` that
        // matches an earlier alias to the alias's already-analyzed
        // expression, so the codegen sees inlined expressions instead of
        // unbound projection slot references.
        let mut effective_scope = scope.clone();

        for item in items {
            match item {
                sqlast::SelectItem::UnnamedExpr(expr) => {
                    let typed = self.analyze_expr(expr, &effective_scope)?;
                    let typed =
                        self.substitute_select_aliases_for_select(typed, &projection, scope);
                    let (name, column_id) = match &typed.kind {
                        ExprKind::ColumnRef {
                            column_id, column, ..
                        } => (column.clone(), *column_id),
                        _ => {
                            let n = expr_display_name(expr);
                            let id = self.alloc_column_id(
                                None,
                                n.clone(),
                                typed.data_type.clone(),
                                typed.nullable,
                            );
                            (n, id)
                        }
                    };
                    output_columns.push(OutputColumn {
                        column_id,
                        name: name.clone(),
                        data_type: typed.data_type.clone(),
                        nullable: typed.nullable,
                        is_internal: false,
                    });
                    projection.push(ProjectItem {
                        expr: typed,
                        output_name: name,
                        output_column_id: column_id,
                    });
                }
                sqlast::SelectItem::ExprWithAlias { expr, alias } => {
                    let typed = self.analyze_expr(expr, &effective_scope)?;
                    let typed =
                        self.substitute_select_aliases_for_select(typed, &projection, scope);
                    let name = alias.value.clone();
                    let column_id = match &typed.kind {
                        ExprKind::ColumnRef { column_id, .. } => *column_id,
                        _ => self.alloc_column_id(
                            None,
                            name.clone(),
                            typed.data_type.clone(),
                            typed.nullable,
                        ),
                    };
                    output_columns.push(OutputColumn {
                        column_id,
                        name: name.clone(),
                        data_type: typed.data_type.clone(),
                        nullable: typed.nullable,
                        is_internal: false,
                    });
                    // Make the alias visible to later items in the same
                    // projection list, but only if it does not already
                    // collide with a column from the FROM scope; otherwise
                    // the alias would shadow the underlying column and
                    // future items would resolve to the alias expression
                    // instead of the FROM column.
                    if scope.resolve(None, &name).is_err() {
                        effective_scope.add_column_with_id(
                            None,
                            &name,
                            column_id,
                            typed.data_type.clone(),
                            typed.nullable,
                        );
                    }
                    projection.push(ProjectItem {
                        expr: typed,
                        output_name: name,
                        output_column_id: column_id,
                    });
                }
                sqlast::SelectItem::Wildcard(_) => {
                    for (qualifier, col_name, col_id, data_type, nullable) in scope.iter_columns() {
                        // FULL OUTER USING columns are exposed as a synthetic
                        // `COALESCE(left.col, right.col)` expression. SELECT *
                        // expansion must use that expression instead of the
                        // raw left-side ColumnRef so right-only rows show
                        // the right-side value.
                        let typed = if let Some(expr) = scope.computed_column_for(col_name) {
                            expr.clone()
                        } else {
                            TypedExpr {
                                kind: ExprKind::ColumnRef {
                                    column_id: *col_id,
                                    qualifier: qualifier.clone(),
                                    column: col_name.clone(),
                                },
                                data_type: data_type.clone(),
                                nullable: *nullable,
                            }
                        };
                        output_columns.push(OutputColumn {
                            column_id: *col_id,
                            name: col_name.clone(),
                            data_type: data_type.clone(),
                            nullable: *nullable,
                            is_internal: false,
                        });
                        projection.push(ProjectItem {
                            expr: typed,
                            output_name: col_name.clone(),
                            output_column_id: *col_id,
                        });
                    }
                }
                sqlast::SelectItem::QualifiedWildcard(kind, _) => {
                    let qualifier_str = match kind {
                        sqlast::SelectItemQualifiedWildcardKind::ObjectName(obj_name) => {
                            obj_name.to_string()
                        }
                        _ => return Err("unsupported qualified wildcard expression".into()),
                    };
                    // Tables are registered under their alias (or bare table
                    // name) in scope, but users can address them with the
                    // multi-part name they typed (e.g. `db.t0.*`). Fall back
                    // to the last identifier segment when the full string
                    // does not match anything, so `<db>.<tbl>.*` finds the
                    // same columns as `<tbl>.*`.
                    let fallback_qualifier = qualifier_str
                        .rsplit('.')
                        .next()
                        .unwrap_or(&qualifier_str)
                        .to_string();
                    let mut found = false;
                    for (qualifier, col_name, col_id, data_type, nullable) in
                        scope.iter_qualified_columns(&qualifier_str).chain(
                            if fallback_qualifier != qualifier_str {
                                Some(scope.iter_qualified_columns(&fallback_qualifier))
                            } else {
                                None
                            }
                            .into_iter()
                            .flatten(),
                        )
                    {
                        found = true;
                        let typed = TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: *col_id,
                                qualifier: qualifier.clone(),
                                column: col_name.clone(),
                            },
                            data_type: data_type.clone(),
                            nullable: *nullable,
                        };
                        output_columns.push(OutputColumn {
                            column_id: *col_id,
                            name: col_name.clone(),
                            data_type: data_type.clone(),
                            nullable: *nullable,
                            is_internal: false,
                        });
                        projection.push(ProjectItem {
                            expr: typed,
                            output_name: col_name.clone(),
                            output_column_id: *col_id,
                        });
                    }
                    if !found {
                        return Err(format!("no columns found for qualifier `{qualifier_str}`"));
                    }
                }
            }
        }

        Ok((projection, output_columns))
    }

    /// Like `analyze_projection` but uses `wildcard_scope` for `SELECT *`
    /// expansion and `expr_scope` for all other expression resolution.
    /// This prevents outer scope columns from leaking into wildcard expansion
    /// inside correlated subqueries.
    fn analyze_projection_with_wildcard_scope(
        &self,
        items: &[sqlast::SelectItem],
        expr_scope: &AnalyzerScope,
        wildcard_scope: &AnalyzerScope,
    ) -> Result<(Vec<ProjectItem>, Vec<OutputColumn>), String> {
        let mut projection = Vec::new();
        let mut output_columns = Vec::new();

        for item in items {
            match item {
                sqlast::SelectItem::Wildcard(_) => {
                    for (qualifier, col_name, col_id, data_type, nullable) in
                        wildcard_scope.iter_columns()
                    {
                        let typed = TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: *col_id,
                                qualifier: qualifier.clone(),
                                column: col_name.clone(),
                            },
                            data_type: data_type.clone(),
                            nullable: *nullable,
                        };
                        output_columns.push(OutputColumn {
                            column_id: *col_id,
                            name: col_name.clone(),
                            data_type: data_type.clone(),
                            nullable: *nullable,
                            is_internal: false,
                        });
                        projection.push(ProjectItem {
                            expr: typed,
                            output_name: col_name.clone(),
                            output_column_id: *col_id,
                        });
                    }
                }
                // All other items use the full scope (including outer for correlation)
                _ => {
                    let (mut p, mut o) =
                        self.analyze_projection(std::slice::from_ref(item), expr_scope)?;
                    projection.append(&mut p);
                    output_columns.append(&mut o);
                }
            }
        }

        Ok((projection, output_columns))
    }

    /// Rebuild the FROM scope from an already-resolved Relation tree.
    /// Used by ORDER BY fallback when the expression doesn't match projection columns.
    fn rebuild_from_scope(&self, relation: &Relation) -> Result<((), AnalyzerScope), String> {
        let mut scope = self.new_scope();
        self.collect_relation_scope(relation, &mut scope)?;
        Ok(((), scope))
    }

    fn collect_relation_scope(
        &self,
        relation: &Relation,
        scope: &mut AnalyzerScope,
    ) -> Result<(), String> {
        match relation {
            Relation::Scan(scan) => {
                let qualifier = scan.alias.as_deref().unwrap_or(&scan.table.name);
                let base_len = scan.table.columns.len();
                let meta_len = scan.table.iceberg_row_lineage_metadata_columns.len();
                if scan.column_ids.len() >= base_len {
                    scope.add_table_with_ids(
                        Some(qualifier),
                        &scan.table.columns,
                        &scan.column_ids[..base_len],
                    );
                } else {
                    scope.add_table(Some(qualifier), &scan.table.columns);
                }
                if !scan.table.iceberg_row_lineage_metadata_columns.is_empty() {
                    if scan.column_ids.len() == base_len + meta_len {
                        scope.add_iceberg_metadata_columns_with_ids(
                            qualifier,
                            &scan.table.iceberg_row_lineage_metadata_columns,
                            &scan.column_ids[base_len..],
                        );
                    } else {
                        scope.add_iceberg_metadata_columns(
                            qualifier,
                            &scan.table.iceberg_row_lineage_metadata_columns,
                        );
                    }
                }
                Ok(())
            }
            Relation::Subquery {
                alias,
                output_columns,
                ..
            } => {
                for col in output_columns {
                    scope.add_column_with_id(
                        Some(alias.as_str()),
                        &col.name,
                        col.column_id,
                        col.data_type.clone(),
                        col.nullable,
                    );
                }
                Ok(())
            }
            Relation::Join(join_rel) => {
                self.collect_relation_scope(&join_rel.left, scope)?;
                self.collect_relation_scope(&join_rel.right, scope)?;
                Ok(())
            }
            Relation::GenerateSeries(gs) => {
                let qualifier = gs.alias.as_deref().unwrap_or("generate_series");
                scope.add_column_with_id(
                    Some(qualifier),
                    &gs.column_name,
                    gs.output_column_id,
                    DataType::Int64,
                    false,
                );
                Ok(())
            }
            Relation::Unnest(unnest) => {
                let qualifier = unnest.alias.as_deref().unwrap_or("unnest");
                for col in &unnest.output_columns {
                    scope.add_column(
                        Some(qualifier),
                        &col.name,
                        col.data_type.clone(),
                        col.nullable,
                    );
                }
                Ok(())
            }
            Relation::CTEConsume {
                alias,
                output_columns,
                ..
            } => {
                for col in output_columns {
                    scope.add_column_with_id(
                        Some(alias.as_str()),
                        &col.name,
                        col.column_id,
                        col.data_type.clone(),
                        col.nullable,
                    );
                }
                Ok(())
            }
            Relation::IcebergMetadataScan(rel) => {
                let cols = crate::sql::analyzer::iceberg_metadata::metadata_table_schema(
                    rel.metadata_table_type.clone(),
                );
                let qualifier = rel.alias.as_deref().unwrap_or(&rel.table.name);
                for col in &cols {
                    scope.add_column(
                        Some(qualifier),
                        &col.name,
                        col.data_type.clone(),
                        col.nullable,
                    );
                }
                Ok(())
            }
            Relation::IcebergDeltaScan(rel) => {
                // Mirror Scan: expose base columns + row-lineage metadata
                // columns under the alias (or table name).
                let qualifier = rel.alias.as_deref().unwrap_or(&rel.table.name);
                let base_len = rel.table.columns.len();
                let meta_len = rel.table.iceberg_row_lineage_metadata_columns.len();
                if rel.column_ids.len() >= base_len {
                    scope.add_table_with_ids(
                        Some(qualifier),
                        &rel.table.columns,
                        &rel.column_ids[..base_len],
                    );
                } else {
                    scope.add_table(Some(qualifier), &rel.table.columns);
                }
                if rel.column_ids.len() == base_len + meta_len {
                    scope.add_iceberg_metadata_columns_with_ids(
                        qualifier,
                        &rel.table.iceberg_row_lineage_metadata_columns,
                        &rel.column_ids[base_len..],
                    );
                } else {
                    scope.add_iceberg_metadata_columns(
                        qualifier,
                        &rel.table.iceberg_row_lineage_metadata_columns,
                    );
                }
                Ok(())
            }
        }
    }

    /// Analyze ORDER BY clause.
    fn analyze_order_by(
        &self,
        query: &sqlast::Query,
        body_output: &[OutputColumn],
        body: &QueryBody,
    ) -> Result<Vec<SortItem>, String> {
        let order_by_exprs = match &query.order_by {
            Some(sqlast::OrderBy {
                kind: sqlast::OrderByKind::Expressions(exprs),
                ..
            }) => exprs,
            Some(sqlast::OrderBy {
                kind: sqlast::OrderByKind::All(_),
                ..
            }) => return Err("ORDER BY ALL is not supported".into()),
            None => return Ok(vec![]),
        };

        // Build a projection scope from body output columns for ORDER BY resolution.
        let mut projection_scope = self.new_scope();
        for col in body_output {
            projection_scope.add_column_with_id(
                None,
                &col.name,
                col.column_id,
                col.data_type.clone(),
                col.nullable,
            );
        }
        // Also register qualified column refs from projection items
        // so ORDER BY a.id works when SELECT has a.id
        if let QueryBody::Select(sel) = body {
            for item in &sel.projection {
                if let ExprKind::ColumnRef {
                    column_id,
                    qualifier: Some(ref q),
                    ref column,
                } = item.expr.kind
                {
                    projection_scope.add_column_with_id(
                        Some(q),
                        column,
                        column_id,
                        item.expr.data_type.clone(),
                        item.expr.nullable,
                    );
                }
            }
        }

        // Pre-aggregation (FROM) scope used to rebind aggregate arguments
        // below. An ORDER BY aggregate's argument must reference a base column,
        // not a projection alias of the same name. Built once; rebuilding it
        // from base-table scans reuses their ColumnIds (no minting), so it has
        // no effect on non-aggregate ORDER BY items.
        let order_by_from_scope: Option<AnalyzerScope> = match body {
            QueryBody::Select(sel) => sel
                .from
                .as_ref()
                .and_then(|rel| self.rebuild_from_scope(rel).ok().map(|(_, scope)| scope)),
            _ => None,
        };

        let mut sort_items = Vec::with_capacity(order_by_exprs.len());
        for ob in order_by_exprs {
            // Try resolving against the projection scope first, then fall back
            // to a numeric literal reference (ORDER BY 1, 2, ...)
            let typed = match &ob.expr {
                sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number(n, _),
                    ..
                }) => {
                    // Positional reference: ORDER BY 1
                    let pos: usize = n
                        .parse::<usize>()
                        .map_err(|e| format!("invalid ORDER BY position: {e}"))?;
                    if pos == 0 || pos > body_output.len() {
                        return Err(format!(
                            "ORDER BY position {pos} is out of range (1..{})",
                            body_output.len()
                        ));
                    }
                    let col = &body_output[pos - 1];
                    TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: col.column_id,
                            qualifier: None,
                            column: col.name.clone(),
                        },
                        data_type: col.data_type.clone(),
                        nullable: col.nullable,
                    }
                }
                _ => {
                    // First: check if the ORDER BY expression textually matches
                    // a SELECT list expression. If so, resolve as a reference to
                    // the output alias. This handles ORDER BY count(x) matching
                    // SELECT count(x) as alias.
                    let ob_text = format!("{}", ob.expr).to_lowercase();
                    let mut matched_alias = None;
                    // Match ORDER BY expression text against SELECT item
                    // expressions (not aliases). This handles ORDER BY
                    // count(distinct x) matching SELECT count(distinct x) as y.
                    if let QueryBody::Select(sel) = body
                        && let sqlast::SetExpr::Select(ast_sel) = query.body.as_ref()
                    {
                        // Bare-identifier ORDER BY (`ORDER BY k1`) must prefer
                        // the projection's output alias over the SELECT item's
                        // underlying analyzed expression: the SELECT item may
                        // analyse to a synthetic expression that's no longer
                        // valid in the post-Project scope (FULL OUTER USING
                        // expands `k1` into `coalesce(t1.k1, t2.k1)`, which
                        // references columns the SORT operator can no longer
                        // see). Promote the alias-match path to fire first
                        // for plain identifiers; AST-text matching is still
                        // useful for `ORDER BY a.c` echoing `SELECT a.c`,
                        // where preserving qualifiers matters.
                        let ob_is_bare_ident = matches!(ob.expr, sqlast::Expr::Identifier(_));
                        for (ast_item, ir_item) in
                            ast_sel.projection.iter().zip(sel.projection.iter())
                        {
                            if ob_is_bare_ident && ir_item.output_name.to_lowercase() == ob_text {
                                let col_id = select_item_output_column_id(
                                    ir_item,
                                    self,
                                    &ir_item.output_name,
                                );
                                matched_alias = Some(TypedExpr {
                                    kind: ExprKind::ColumnRef {
                                        column_id: col_id,
                                        qualifier: None,
                                        column: ir_item.output_name.clone(),
                                    },
                                    data_type: ir_item.expr.data_type.clone(),
                                    nullable: ir_item.expr.nullable,
                                });
                                break;
                            }
                            let ast_expr_text = match ast_item {
                                sqlast::SelectItem::ExprWithAlias { expr, .. }
                                | sqlast::SelectItem::UnnamedExpr(expr) => {
                                    format!("{expr}").to_lowercase()
                                }
                                _ => continue,
                            };
                            // Exact AST match means the user wrote the
                            // same expression as the SELECT item: reuse the
                            // analyzed expression directly so qualifiers and
                            // sub-expressions are preserved. Without this,
                            // `SELECT a.c, b.c ... ORDER BY a.c, b.c` would
                            // produce two unqualified `c` refs and collapse
                            // both keys onto the first projection slot.
                            if ast_expr_text == ob_text {
                                matched_alias = Some(ir_item.expr.clone());
                                break;
                            }
                            // Output-name (alias) match: the user is
                            // referring to the projection's *output* column
                            // by name. Use a synthetic unqualified ColumnRef
                            // so post-aggregation contexts (GROUP BY,
                            // HAVING) can resolve the alias against the
                            // projection-output scope rather than against
                            // the pre-aggregation FROM-scope.
                            if ir_item.output_name.to_lowercase() == ob_text {
                                let col_id = select_item_output_column_id(
                                    ir_item,
                                    self,
                                    &ir_item.output_name,
                                );
                                matched_alias = Some(TypedExpr {
                                    kind: ExprKind::ColumnRef {
                                        column_id: col_id,
                                        qualifier: None,
                                        column: ir_item.output_name.clone(),
                                    },
                                    data_type: ir_item.expr.data_type.clone(),
                                    nullable: ir_item.expr.nullable,
                                });
                                break;
                            }
                        }
                    }
                    if let Some(alias_ref) = matched_alias {
                        alias_ref
                    } else {
                        // Try projection scope first, then fall back to FROM scope
                        match self.analyze_expr(&ob.expr, &projection_scope) {
                            Ok(typed) => {
                                if let QueryBody::Select(sel) = body {
                                    self.substitute_select_aliases(typed, &sel.projection)
                                } else {
                                    typed
                                }
                            }
                            Err(proj_err) => {
                                if let QueryBody::Select(sel) = body {
                                    if let Some(ref from_rel) = sel.from {
                                        let (_, from_scope) = self.rebuild_from_scope(from_rel)?;
                                        match self.analyze_expr(&ob.expr, &from_scope) {
                                            Ok(typed) => self
                                                .substitute_select_aliases(typed, &sel.projection),
                                            Err(_) => {
                                                let mut alias_scope = from_scope.clone();
                                                for item in &sel.projection {
                                                    let col_id = select_item_output_column_id(
                                                        item,
                                                        self,
                                                        &item.output_name,
                                                    );
                                                    alias_scope.add_column_with_id(
                                                        None,
                                                        &item.output_name,
                                                        col_id,
                                                        item.expr.data_type.clone(),
                                                        item.expr.nullable,
                                                    );
                                                }
                                                match self.analyze_expr(&ob.expr, &alias_scope) {
                                                    Ok(typed) => self.substitute_select_aliases(
                                                        typed,
                                                        &sel.projection,
                                                    ),
                                                    Err(_) => return Err(proj_err),
                                                }
                                            }
                                        }
                                    } else {
                                        return Err(proj_err);
                                    }
                                } else {
                                    return Err(proj_err);
                                }
                            }
                        }
                    }
                }
            };

            // Rebind columns inside aggregate arguments to the base FROM scope.
            // The projection-scope resolution above can mis-bind a column
            // *inside* an aggregate (e.g. `v1` in `ORDER BY abs(min(v1)) +
            // abs(v1)` where `min(v1) AS v1` shadows the base column), minting a
            // phantom ColumnId the aggregate's child scope never produces. Only
            // aggregate args are touched; top-level references are unchanged.
            let typed = match &order_by_from_scope {
                Some(from_scope) => self.rebind_order_by_agg_args(typed, from_scope, false),
                None => typed,
            };
            if contains_subquery_placeholder(&typed) {
                return Err("subquery is not supported in ORDER BY".to_string());
            }

            let asc = ob.options.asc.unwrap_or(true);
            let nulls_first = ob.options.nulls_first.unwrap_or(asc);

            sort_items.push(SortItem {
                expr: typed,
                asc,
                nulls_first,
            });
        }

        // BITMAP / HLL columns cannot participate in ORDER BY because they
        // are opaque blobs with no ordering. Check the projection scope for
        // alias references, then the FROM scope for direct column refs.
        let from_scope_for_check: Option<AnalyzerScope> = if let QueryBody::Select(sel) = body {
            sel.from
                .as_ref()
                .and_then(|rel| self.rebuild_from_scope(rel).ok().map(|(_, s)| s))
        } else {
            None
        };
        for item in &sort_items {
            let logical = projection_scope
                .logical_type_of_expr(&item.expr)
                .filter(is_bitmap_or_hll_type)
                .or_else(|| {
                    from_scope_for_check
                        .as_ref()
                        .and_then(|s| s.logical_type_of_expr(&item.expr))
                        .filter(is_bitmap_or_hll_type)
                });
            if let Some(logical) = logical {
                return Err(format!(
                    "BITMAP/HLL columns cannot appear in ORDER BY (column has type {logical:?})"
                ));
            }
        }

        Ok(sort_items)
    }

    /// Rebind `ColumnRef`s that occur inside an aggregate argument to the
    /// pre-aggregation `from_scope`, so an ORDER BY aggregate binds its
    /// argument to the base column rather than a SELECT alias of the same
    /// name. Aggregates cannot nest, so once `inside_agg` is set every
    /// `ColumnRef` reached is an aggregate argument and is re-resolved by name
    /// against the FROM scope (kept as-is when the name is not a base column).
    /// Top-level references (`inside_agg == false`) are returned unchanged —
    /// they may legitimately point at projection outputs (for example a FULL
    /// OUTER USING coalesce column that is only valid above the Project).
    fn rebind_order_by_agg_args(
        &self,
        expr: TypedExpr,
        from_scope: &AnalyzerScope,
        inside_agg: bool,
    ) -> TypedExpr {
        let TypedExpr {
            kind,
            data_type,
            nullable,
        } = expr;
        let kind = match kind {
            ExprKind::ColumnRef {
                column_id,
                qualifier,
                column,
            } => {
                let column_id = if inside_agg {
                    match from_scope.resolve(qualifier.as_deref(), &column) {
                        Ok((base_id, _, _)) => base_id,
                        Err(_) => column_id,
                    }
                } else {
                    column_id
                };
                ExprKind::ColumnRef {
                    column_id,
                    qualifier,
                    column,
                }
            }
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
            } => ExprKind::AggregateCall {
                name,
                args: args
                    .into_iter()
                    .map(|arg| self.rebind_order_by_agg_args(arg, from_scope, true))
                    .collect(),
                distinct,
                order_by: order_by
                    .into_iter()
                    .map(|item| SortItem {
                        expr: self.rebind_order_by_agg_args(item.expr, from_scope, true),
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                    .collect(),
            },
            ExprKind::BinaryOp { left, op, right } => ExprKind::BinaryOp {
                left: Box::new(self.rebind_order_by_agg_args(*left, from_scope, inside_agg)),
                op,
                right: Box::new(self.rebind_order_by_agg_args(*right, from_scope, inside_agg)),
            },
            ExprKind::UnaryOp { op, expr: inner } => ExprKind::UnaryOp {
                op,
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
            },
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
            } => ExprKind::FunctionCall {
                name,
                args: args
                    .into_iter()
                    .map(|arg| self.rebind_order_by_agg_args(arg, from_scope, inside_agg))
                    .collect(),
                distinct,
            },
            ExprKind::Cast {
                expr: inner,
                target,
            } => ExprKind::Cast {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                target,
            },
            ExprKind::IsNull {
                expr: inner,
                negated,
            } => ExprKind::IsNull {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                negated,
            },
            ExprKind::IsTruthValue {
                expr: inner,
                value,
                negated,
            } => ExprKind::IsTruthValue {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                value,
                negated,
            },
            ExprKind::Nested(inner) => ExprKind::Nested(Box::new(
                self.rebind_order_by_agg_args(*inner, from_scope, inside_agg),
            )),
            ExprKind::InList {
                expr: inner,
                list,
                negated,
            } => ExprKind::InList {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                list: list
                    .into_iter()
                    .map(|item| self.rebind_order_by_agg_args(item, from_scope, inside_agg))
                    .collect(),
                negated,
            },
            ExprKind::Between {
                expr: inner,
                low,
                high,
                negated,
            } => ExprKind::Between {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                low: Box::new(self.rebind_order_by_agg_args(*low, from_scope, inside_agg)),
                high: Box::new(self.rebind_order_by_agg_args(*high, from_scope, inside_agg)),
                negated,
            },
            ExprKind::Like {
                expr: inner,
                pattern,
                negated,
            } => ExprKind::Like {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                pattern: Box::new(self.rebind_order_by_agg_args(*pattern, from_scope, inside_agg)),
                negated,
            },
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => ExprKind::Case {
                operand: operand
                    .map(|e| Box::new(self.rebind_order_by_agg_args(*e, from_scope, inside_agg))),
                when_then: when_then
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            self.rebind_order_by_agg_args(when, from_scope, inside_agg),
                            self.rebind_order_by_agg_args(then, from_scope, inside_agg),
                        )
                    })
                    .collect(),
                else_expr: else_expr
                    .map(|e| Box::new(self.rebind_order_by_agg_args(*e, from_scope, inside_agg))),
            },
            // Leaves and node kinds that cannot carry an aggregate argument
            // needing a base-column rebind (Literal, LambdaParamRef,
            // SubqueryPlaceholder, WindowCall, LambdaFunction, Lambda): keep
            // as-is. This mirrors substitute_select_aliases_inner's coverage.
            other => other,
        };
        TypedExpr {
            kind,
            data_type,
            nullable,
        }
    }
}

fn contains_subquery_placeholder(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::SubqueryPlaceholder { .. } => true,
        ExprKind::BinaryOp { left, right, .. } => {
            contains_subquery_placeholder(left) || contains_subquery_placeholder(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => contains_subquery_placeholder(expr),
        ExprKind::FunctionCall { args, .. } => args.iter().any(contains_subquery_placeholder),
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            contains_subquery_placeholder(body)
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            args.iter().any(contains_subquery_placeholder)
                || order_by
                    .iter()
                    .any(|item| contains_subquery_placeholder(&item.expr))
        }
        ExprKind::InList { expr, list, .. } => {
            contains_subquery_placeholder(expr) || list.iter().any(contains_subquery_placeholder)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            contains_subquery_placeholder(expr)
                || contains_subquery_placeholder(low)
                || contains_subquery_placeholder(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            contains_subquery_placeholder(expr) || contains_subquery_placeholder(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| contains_subquery_placeholder(expr))
                || when_then.iter().any(|(when, then)| {
                    contains_subquery_placeholder(when) || contains_subquery_placeholder(then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| contains_subquery_placeholder(expr))
        }
        ExprKind::Nested(inner) => contains_subquery_placeholder(inner),
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(contains_subquery_placeholder)
                || partition_by.iter().any(contains_subquery_placeholder)
                || order_by
                    .iter()
                    .any(|item| contains_subquery_placeholder(&item.expr))
        }
        ExprKind::ColumnRef { .. } | ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {
            false
        }
    }
}

fn select_item_output_column_id(
    item: &ProjectItem,
    ctx: &AnalyzerContext<'_>,
    fallback_name: &str,
) -> crate::sql::column_id::ColumnId {
    if item.output_column_id != crate::sql::column_id::ColumnId::UNSET {
        return item.output_column_id;
    }
    match &item.expr.kind {
        ExprKind::ColumnRef { column_id, .. } => *column_id,
        _ => ctx.alloc_column_id(
            None,
            fallback_name.to_string(),
            item.expr.data_type.clone(),
            item.expr.nullable,
        ),
    }
}

/// Replace GROUPING(col) function calls in a sqlparser AST expression with
/// integer literal 0 (column is active) or 1 (column is NULLed) based on the
/// current ROLLUP expansion level.
fn replace_grouping_calls(
    expr: &sqlast::Expr,
    nulled_exprs: &std::collections::HashSet<String>,
) -> sqlast::Expr {
    // Replace references to NULLed columns with NULL (needed for window
    // PARTITION BY / ORDER BY expressions that reference rollup keys).
    let expr_str = format!("{expr}").to_lowercase();
    if nulled_exprs.contains(&expr_str) {
        return sqlast::Expr::Value(sqlast::Value::Null.into());
    }
    match expr {
        sqlast::Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();
            if name == "grouping"
                && let sqlast::FunctionArguments::List(ref list) = func.args
            {
                // Extract the argument column name
                if let Some(sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(arg_expr))) =
                    list.args.first()
                {
                    let arg_str = format!("{arg_expr}").to_lowercase();
                    let value = if nulled_exprs.contains(&arg_str) {
                        1i64
                    } else {
                        0i64
                    };
                    return sqlast::Expr::Value(
                        sqlast::Value::Number(value.to_string(), false).into(),
                    );
                }
            }
            // Not a GROUPING() call — recurse into arguments
            let new_args = match &func.args {
                sqlast::FunctionArguments::List(list) => {
                    let new_list_args: Vec<_> = list
                        .args
                        .iter()
                        .map(|arg| match arg {
                            sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) => {
                                sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(
                                    replace_grouping_calls(e, nulled_exprs),
                                ))
                            }
                            other => other.clone(),
                        })
                        .collect();
                    sqlast::FunctionArguments::List(sqlast::FunctionArgumentList {
                        args: new_list_args,
                        ..list.clone()
                    })
                }
                other => other.clone(),
            };
            let mut new_func = func.clone();
            new_func.args = new_args;
            // Recurse into OVER clause for window functions
            if let Some(ref window) = func.over {
                new_func.over = Some(replace_grouping_in_window(window, nulled_exprs));
            }
            sqlast::Expr::Function(new_func)
        }
        sqlast::Expr::BinaryOp { left, op, right } => sqlast::Expr::BinaryOp {
            left: Box::new(replace_grouping_calls(left, nulled_exprs)),
            op: op.clone(),
            right: Box::new(replace_grouping_calls(right, nulled_exprs)),
        },
        sqlast::Expr::UnaryOp { op, expr: inner } => sqlast::Expr::UnaryOp {
            op: *op,
            expr: Box::new(replace_grouping_calls(inner, nulled_exprs)),
        },
        sqlast::Expr::Nested(inner) => {
            sqlast::Expr::Nested(Box::new(replace_grouping_calls(inner, nulled_exprs)))
        }
        sqlast::Expr::Case {
            case_token,
            end_token,
            operand,
            conditions,
            else_result,
        } => sqlast::Expr::Case {
            case_token: case_token.clone(),
            end_token: end_token.clone(),
            operand: operand
                .as_ref()
                .map(|o| Box::new(replace_grouping_calls(o, nulled_exprs))),
            conditions: conditions
                .iter()
                .map(|cw| sqlast::CaseWhen {
                    condition: replace_grouping_calls(&cw.condition, nulled_exprs),
                    result: replace_grouping_calls(&cw.result, nulled_exprs),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(replace_grouping_calls(e, nulled_exprs))),
        },
        other => other.clone(),
    }
}

/// Recurse into window specifications to replace GROUPING() calls in PARTITION BY / ORDER BY.
fn replace_grouping_in_window(
    window: &sqlast::WindowType,
    nulled_exprs: &std::collections::HashSet<String>,
) -> sqlast::WindowType {
    match window {
        sqlast::WindowType::WindowSpec(spec) => {
            let partition_by = spec
                .partition_by
                .iter()
                .map(|e| replace_grouping_calls(e, nulled_exprs))
                .collect();
            let order_by = spec
                .order_by
                .iter()
                .map(|ob| sqlast::OrderByExpr {
                    expr: replace_grouping_calls(&ob.expr, nulled_exprs),
                    ..ob.clone()
                })
                .collect();
            sqlast::WindowType::WindowSpec(sqlast::WindowSpec {
                partition_by,
                order_by,
                ..spec.clone()
            })
        }
        other => other.clone(),
    }
}

/// Replace GROUPING/GROUPING_ID calls in an AST expression with unique marker
/// literals (-9000, -9001, ...). Each call is recorded in `args` as
/// (virtual_name, [column_args]) for the RepeatInfo.
fn replace_grouping_markers_in_function_arg_expr(
    arg_expr: &sqlast::FunctionArgExpr,
    args: &mut Vec<(String, Vec<String>)>,
    next_marker: &mut i64,
) -> sqlast::FunctionArgExpr {
    match arg_expr {
        sqlast::FunctionArgExpr::Expr(expr) => sqlast::FunctionArgExpr::Expr(
            replace_grouping_calls_with_markers(expr, args, next_marker),
        ),
        other => other.clone(),
    }
}

fn replace_grouping_markers_in_function_arg(
    arg: &sqlast::FunctionArg,
    args: &mut Vec<(String, Vec<String>)>,
    next_marker: &mut i64,
) -> sqlast::FunctionArg {
    match arg {
        sqlast::FunctionArg::Named {
            name,
            arg,
            operator,
        } => sqlast::FunctionArg::Named {
            name: name.clone(),
            arg: replace_grouping_markers_in_function_arg_expr(arg, args, next_marker),
            operator: operator.clone(),
        },
        sqlast::FunctionArg::ExprNamed {
            name,
            arg,
            operator,
        } => sqlast::FunctionArg::ExprNamed {
            name: replace_grouping_calls_with_markers(name, args, next_marker),
            arg: replace_grouping_markers_in_function_arg_expr(arg, args, next_marker),
            operator: operator.clone(),
        },
        sqlast::FunctionArg::Unnamed(arg) => sqlast::FunctionArg::Unnamed(
            replace_grouping_markers_in_function_arg_expr(arg, args, next_marker),
        ),
    }
}

fn replace_grouping_markers_in_order_by_expr(
    order_by: &sqlast::OrderByExpr,
    args: &mut Vec<(String, Vec<String>)>,
    next_marker: &mut i64,
) -> sqlast::OrderByExpr {
    sqlast::OrderByExpr {
        expr: replace_grouping_calls_with_markers(&order_by.expr, args, next_marker),
        with_fill: order_by
            .with_fill
            .as_ref()
            .map(|with_fill| sqlast::WithFill {
                from: with_fill
                    .from
                    .as_ref()
                    .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker)),
                to: with_fill
                    .to
                    .as_ref()
                    .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker)),
                step: with_fill
                    .step
                    .as_ref()
                    .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker)),
            }),
        ..order_by.clone()
    }
}

fn replace_grouping_markers_in_function_argument_clause(
    clause: &sqlast::FunctionArgumentClause,
    args: &mut Vec<(String, Vec<String>)>,
    next_marker: &mut i64,
) -> sqlast::FunctionArgumentClause {
    match clause {
        sqlast::FunctionArgumentClause::OrderBy(order_by) => {
            sqlast::FunctionArgumentClause::OrderBy(
                order_by
                    .iter()
                    .map(|item| replace_grouping_markers_in_order_by_expr(item, args, next_marker))
                    .collect(),
            )
        }
        sqlast::FunctionArgumentClause::Limit(expr) => sqlast::FunctionArgumentClause::Limit(
            replace_grouping_calls_with_markers(expr, args, next_marker),
        ),
        sqlast::FunctionArgumentClause::Having(bound) => {
            sqlast::FunctionArgumentClause::Having(sqlast::HavingBound(
                bound.0,
                replace_grouping_calls_with_markers(&bound.1, args, next_marker),
            ))
        }
        other => other.clone(),
    }
}

fn replace_grouping_markers_in_function_arguments(
    arguments: &sqlast::FunctionArguments,
    args: &mut Vec<(String, Vec<String>)>,
    next_marker: &mut i64,
) -> sqlast::FunctionArguments {
    match arguments {
        sqlast::FunctionArguments::List(list) => {
            sqlast::FunctionArguments::List(sqlast::FunctionArgumentList {
                args: list
                    .args
                    .iter()
                    .map(|arg| replace_grouping_markers_in_function_arg(arg, args, next_marker))
                    .collect(),
                clauses: list
                    .clauses
                    .iter()
                    .map(|clause| {
                        replace_grouping_markers_in_function_argument_clause(
                            clause,
                            args,
                            next_marker,
                        )
                    })
                    .collect(),
                ..list.clone()
            })
        }
        sqlast::FunctionArguments::Subquery(_) | sqlast::FunctionArguments::None => {
            arguments.clone()
        }
    }
}

fn replace_grouping_markers_in_subscript(
    subscript: &sqlast::Subscript,
    args: &mut Vec<(String, Vec<String>)>,
    next_marker: &mut i64,
) -> sqlast::Subscript {
    match subscript {
        sqlast::Subscript::Index { index } => sqlast::Subscript::Index {
            index: replace_grouping_calls_with_markers(index, args, next_marker),
        },
        sqlast::Subscript::Slice {
            lower_bound,
            upper_bound,
            stride,
        } => sqlast::Subscript::Slice {
            lower_bound: lower_bound
                .as_ref()
                .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker)),
            upper_bound: upper_bound
                .as_ref()
                .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker)),
            stride: stride
                .as_ref()
                .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker)),
        },
    }
}

fn replace_grouping_markers_in_access_expr(
    access_expr: &sqlast::AccessExpr,
    args: &mut Vec<(String, Vec<String>)>,
    next_marker: &mut i64,
) -> sqlast::AccessExpr {
    match access_expr {
        sqlast::AccessExpr::Dot(expr) => {
            sqlast::AccessExpr::Dot(replace_grouping_calls_with_markers(expr, args, next_marker))
        }
        sqlast::AccessExpr::Subscript(subscript) => sqlast::AccessExpr::Subscript(
            replace_grouping_markers_in_subscript(subscript, args, next_marker),
        ),
    }
}

fn replace_grouping_markers_in_json_path(
    path: &sqlast::JsonPath,
    args: &mut Vec<(String, Vec<String>)>,
    next_marker: &mut i64,
) -> sqlast::JsonPath {
    sqlast::JsonPath {
        path: path
            .path
            .iter()
            .map(|elem| match elem {
                sqlast::JsonPathElem::Bracket { key } => sqlast::JsonPathElem::Bracket {
                    key: replace_grouping_calls_with_markers(key, args, next_marker),
                },
                other => other.clone(),
            })
            .collect(),
    }
}

fn replace_grouping_calls_with_markers(
    expr: &sqlast::Expr,
    args: &mut Vec<(String, Vec<String>)>,
    next_marker: &mut i64,
) -> sqlast::Expr {
    match expr {
        sqlast::Expr::CompoundFieldAccess { root, access_chain } => {
            sqlast::Expr::CompoundFieldAccess {
                root: Box::new(replace_grouping_calls_with_markers(root, args, next_marker)),
                access_chain: access_chain
                    .iter()
                    .map(|access| {
                        replace_grouping_markers_in_access_expr(access, args, next_marker)
                    })
                    .collect(),
            }
        }
        sqlast::Expr::JsonAccess { value, path } => sqlast::Expr::JsonAccess {
            value: Box::new(replace_grouping_calls_with_markers(
                value,
                args,
                next_marker,
            )),
            path: replace_grouping_markers_in_json_path(path, args, next_marker),
        },
        sqlast::Expr::IsFalse(inner) => sqlast::Expr::IsFalse(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::IsNotFalse(inner) => sqlast::Expr::IsNotFalse(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::IsTrue(inner) => sqlast::Expr::IsTrue(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::IsNotTrue(inner) => sqlast::Expr::IsNotTrue(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::IsNull(inner) => sqlast::Expr::IsNull(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::IsNotNull(inner) => sqlast::Expr::IsNotNull(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::IsUnknown(inner) => sqlast::Expr::IsUnknown(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::IsNotUnknown(inner) => sqlast::Expr::IsNotUnknown(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::IsDistinctFrom(left, right) => sqlast::Expr::IsDistinctFrom(
            Box::new(replace_grouping_calls_with_markers(left, args, next_marker)),
            Box::new(replace_grouping_calls_with_markers(
                right,
                args,
                next_marker,
            )),
        ),
        sqlast::Expr::IsNotDistinctFrom(left, right) => sqlast::Expr::IsNotDistinctFrom(
            Box::new(replace_grouping_calls_with_markers(left, args, next_marker)),
            Box::new(replace_grouping_calls_with_markers(
                right,
                args,
                next_marker,
            )),
        ),
        sqlast::Expr::IsNormalized {
            expr: inner,
            form,
            negated,
        } => sqlast::Expr::IsNormalized {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            form: form.clone(),
            negated: *negated,
        },
        sqlast::Expr::InList {
            expr: inner,
            list,
            negated,
        } => sqlast::Expr::InList {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            list: list
                .iter()
                .map(|item| replace_grouping_calls_with_markers(item, args, next_marker))
                .collect(),
            negated: *negated,
        },
        sqlast::Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => sqlast::Expr::InSubquery {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            subquery: subquery.clone(),
            negated: *negated,
        },
        sqlast::Expr::InUnnest {
            expr: inner,
            array_expr,
            negated,
        } => sqlast::Expr::InUnnest {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            array_expr: Box::new(replace_grouping_calls_with_markers(
                array_expr,
                args,
                next_marker,
            )),
            negated: *negated,
        },
        sqlast::Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => sqlast::Expr::Between {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            negated: *negated,
            low: Box::new(replace_grouping_calls_with_markers(low, args, next_marker)),
            high: Box::new(replace_grouping_calls_with_markers(high, args, next_marker)),
        },
        sqlast::Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();
            if matches!(name.as_str(), "grouping" | "grouping_id")
                && let sqlast::FunctionArguments::List(ref list) = func.args
            {
                let arg_cols: Vec<String> = list
                    .args
                    .iter()
                    .filter_map(|a| match a {
                        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) => {
                            Some(format!("{e}").to_lowercase())
                        }
                        _ => None,
                    })
                    .collect();
                let idx = args.len();
                let virtual_name = format!("__grouping_fn_{idx}");
                args.push((virtual_name, arg_cols));
                let marker = *next_marker;
                *next_marker -= 1;
                return sqlast::Expr::Value(
                    sqlast::Value::Number(marker.to_string(), false).into(),
                );
            }
            let mut new_func = func.clone();
            new_func.parameters =
                replace_grouping_markers_in_function_arguments(&func.parameters, args, next_marker);
            new_func.args =
                replace_grouping_markers_in_function_arguments(&func.args, args, next_marker);
            new_func.filter = func.filter.as_ref().map(|filter| {
                Box::new(replace_grouping_calls_with_markers(
                    filter,
                    args,
                    next_marker,
                ))
            });
            new_func.over = func
                .over
                .as_ref()
                .map(|window| replace_grouping_markers_in_window(window, args, next_marker));
            new_func.within_group = func
                .within_group
                .iter()
                .map(|item| replace_grouping_markers_in_order_by_expr(item, args, next_marker))
                .collect();
            sqlast::Expr::Function(new_func)
        }
        sqlast::Expr::BinaryOp { left, op, right } => sqlast::Expr::BinaryOp {
            left: Box::new(replace_grouping_calls_with_markers(left, args, next_marker)),
            op: op.clone(),
            right: Box::new(replace_grouping_calls_with_markers(
                right,
                args,
                next_marker,
            )),
        },
        sqlast::Expr::UnaryOp { op, expr: inner } => sqlast::Expr::UnaryOp {
            op: *op,
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
        },
        sqlast::Expr::Like {
            negated,
            any,
            expr: inner,
            pattern,
            escape_char,
        } => sqlast::Expr::Like {
            negated: *negated,
            any: *any,
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            pattern: Box::new(replace_grouping_calls_with_markers(
                pattern,
                args,
                next_marker,
            )),
            escape_char: escape_char.clone(),
        },
        sqlast::Expr::ILike {
            negated,
            any,
            expr: inner,
            pattern,
            escape_char,
        } => sqlast::Expr::ILike {
            negated: *negated,
            any: *any,
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            pattern: Box::new(replace_grouping_calls_with_markers(
                pattern,
                args,
                next_marker,
            )),
            escape_char: escape_char.clone(),
        },
        sqlast::Expr::SimilarTo {
            negated,
            expr: inner,
            pattern,
            escape_char,
        } => sqlast::Expr::SimilarTo {
            negated: *negated,
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            pattern: Box::new(replace_grouping_calls_with_markers(
                pattern,
                args,
                next_marker,
            )),
            escape_char: escape_char.clone(),
        },
        sqlast::Expr::RLike {
            negated,
            expr: inner,
            pattern,
            regexp,
        } => sqlast::Expr::RLike {
            negated: *negated,
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            pattern: Box::new(replace_grouping_calls_with_markers(
                pattern,
                args,
                next_marker,
            )),
            regexp: *regexp,
        },
        sqlast::Expr::AnyOp {
            left,
            compare_op,
            right,
            is_some,
        } => sqlast::Expr::AnyOp {
            left: Box::new(replace_grouping_calls_with_markers(left, args, next_marker)),
            compare_op: compare_op.clone(),
            right: Box::new(replace_grouping_calls_with_markers(
                right,
                args,
                next_marker,
            )),
            is_some: *is_some,
        },
        sqlast::Expr::AllOp {
            left,
            compare_op,
            right,
        } => sqlast::Expr::AllOp {
            left: Box::new(replace_grouping_calls_with_markers(left, args, next_marker)),
            compare_op: compare_op.clone(),
            right: Box::new(replace_grouping_calls_with_markers(
                right,
                args,
                next_marker,
            )),
        },
        sqlast::Expr::Convert {
            is_try,
            expr: inner,
            data_type,
            charset,
            target_before_value,
            styles,
        } => sqlast::Expr::Convert {
            is_try: *is_try,
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            data_type: data_type.clone(),
            charset: charset.clone(),
            target_before_value: *target_before_value,
            styles: styles
                .iter()
                .map(|style| replace_grouping_calls_with_markers(style, args, next_marker))
                .collect(),
        },
        sqlast::Expr::Cast {
            kind,
            expr: inner,
            data_type,
            array,
            format,
        } => sqlast::Expr::Cast {
            kind: kind.clone(),
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            data_type: data_type.clone(),
            array: *array,
            format: format.clone(),
        },
        sqlast::Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => sqlast::Expr::AtTimeZone {
            timestamp: Box::new(replace_grouping_calls_with_markers(
                timestamp,
                args,
                next_marker,
            )),
            time_zone: Box::new(replace_grouping_calls_with_markers(
                time_zone,
                args,
                next_marker,
            )),
        },
        sqlast::Expr::Extract {
            field,
            syntax,
            expr: inner,
        } => sqlast::Expr::Extract {
            field: field.clone(),
            syntax: syntax.clone(),
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
        },
        sqlast::Expr::Ceil { expr: inner, field } => sqlast::Expr::Ceil {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            field: field.clone(),
        },
        sqlast::Expr::Floor { expr: inner, field } => sqlast::Expr::Floor {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            field: field.clone(),
        },
        sqlast::Expr::Position { expr: inner, r#in } => sqlast::Expr::Position {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            r#in: Box::new(replace_grouping_calls_with_markers(r#in, args, next_marker)),
        },
        sqlast::Expr::Substring {
            expr: inner,
            substring_from,
            substring_for,
            special,
            shorthand,
        } => sqlast::Expr::Substring {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            substring_from: substring_from
                .as_ref()
                .map(|expr| Box::new(replace_grouping_calls_with_markers(expr, args, next_marker))),
            substring_for: substring_for
                .as_ref()
                .map(|expr| Box::new(replace_grouping_calls_with_markers(expr, args, next_marker))),
            special: *special,
            shorthand: *shorthand,
        },
        sqlast::Expr::Trim {
            expr: inner,
            trim_where,
            trim_what,
            trim_characters,
        } => sqlast::Expr::Trim {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            trim_where: trim_where.clone(),
            trim_what: trim_what
                .as_ref()
                .map(|expr| Box::new(replace_grouping_calls_with_markers(expr, args, next_marker))),
            trim_characters: trim_characters.as_ref().map(|exprs| {
                exprs
                    .iter()
                    .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker))
                    .collect()
            }),
        },
        sqlast::Expr::Overlay {
            expr: inner,
            overlay_what,
            overlay_from,
            overlay_for,
        } => sqlast::Expr::Overlay {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            overlay_what: Box::new(replace_grouping_calls_with_markers(
                overlay_what,
                args,
                next_marker,
            )),
            overlay_from: Box::new(replace_grouping_calls_with_markers(
                overlay_from,
                args,
                next_marker,
            )),
            overlay_for: overlay_for
                .as_ref()
                .map(|expr| Box::new(replace_grouping_calls_with_markers(expr, args, next_marker))),
        },
        sqlast::Expr::Collate {
            expr: inner,
            collation,
        } => sqlast::Expr::Collate {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            collation: collation.clone(),
        },
        sqlast::Expr::Nested(inner) => sqlast::Expr::Nested(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::Prefixed { prefix, value } => sqlast::Expr::Prefixed {
            prefix: prefix.clone(),
            value: Box::new(replace_grouping_calls_with_markers(
                value,
                args,
                next_marker,
            )),
        },
        sqlast::Expr::Case {
            case_token,
            end_token,
            operand,
            conditions,
            else_result,
        } => sqlast::Expr::Case {
            case_token: case_token.clone(),
            end_token: end_token.clone(),
            operand: operand
                .as_ref()
                .map(|o| Box::new(replace_grouping_calls_with_markers(o, args, next_marker))),
            conditions: conditions
                .iter()
                .map(|cw| sqlast::CaseWhen {
                    condition: replace_grouping_calls_with_markers(
                        &cw.condition,
                        args,
                        next_marker,
                    ),
                    result: replace_grouping_calls_with_markers(&cw.result, args, next_marker),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(replace_grouping_calls_with_markers(e, args, next_marker))),
        },
        sqlast::Expr::GroupingSets(groups) => sqlast::Expr::GroupingSets(
            groups
                .iter()
                .map(|group| {
                    group
                        .iter()
                        .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker))
                        .collect()
                })
                .collect(),
        ),
        sqlast::Expr::Cube(groups) => sqlast::Expr::Cube(
            groups
                .iter()
                .map(|group| {
                    group
                        .iter()
                        .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker))
                        .collect()
                })
                .collect(),
        ),
        sqlast::Expr::Rollup(groups) => sqlast::Expr::Rollup(
            groups
                .iter()
                .map(|group| {
                    group
                        .iter()
                        .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker))
                        .collect()
                })
                .collect(),
        ),
        sqlast::Expr::Tuple(exprs) => sqlast::Expr::Tuple(
            exprs
                .iter()
                .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker))
                .collect(),
        ),
        sqlast::Expr::Struct { values, fields } => sqlast::Expr::Struct {
            values: values
                .iter()
                .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker))
                .collect(),
            fields: fields.clone(),
        },
        sqlast::Expr::Named { expr: inner, name } => sqlast::Expr::Named {
            expr: Box::new(replace_grouping_calls_with_markers(
                inner,
                args,
                next_marker,
            )),
            name: name.clone(),
        },
        sqlast::Expr::Dictionary(fields) => sqlast::Expr::Dictionary(
            fields
                .iter()
                .map(|field| sqlast::DictionaryField {
                    key: field.key.clone(),
                    value: Box::new(replace_grouping_calls_with_markers(
                        &field.value,
                        args,
                        next_marker,
                    )),
                })
                .collect(),
        ),
        sqlast::Expr::Map(map) => sqlast::Expr::Map(sqlast::Map {
            entries: map
                .entries
                .iter()
                .map(|entry| sqlast::MapEntry {
                    key: Box::new(replace_grouping_calls_with_markers(
                        &entry.key,
                        args,
                        next_marker,
                    )),
                    value: Box::new(replace_grouping_calls_with_markers(
                        &entry.value,
                        args,
                        next_marker,
                    )),
                })
                .collect(),
        }),
        sqlast::Expr::Array(array) => sqlast::Expr::Array(sqlast::Array {
            elem: array
                .elem
                .iter()
                .map(|expr| replace_grouping_calls_with_markers(expr, args, next_marker))
                .collect(),
            named: array.named,
        }),
        sqlast::Expr::Interval(interval) => sqlast::Expr::Interval(sqlast::Interval {
            value: Box::new(replace_grouping_calls_with_markers(
                &interval.value,
                args,
                next_marker,
            )),
            leading_field: interval.leading_field.clone(),
            leading_precision: interval.leading_precision,
            last_field: interval.last_field.clone(),
            fractional_seconds_precision: interval.fractional_seconds_precision,
        }),
        sqlast::Expr::OuterJoin(inner) => sqlast::Expr::OuterJoin(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::Prior(inner) => sqlast::Expr::Prior(Box::new(
            replace_grouping_calls_with_markers(inner, args, next_marker),
        )),
        sqlast::Expr::Lambda(lambda) => sqlast::Expr::Lambda(sqlast::LambdaFunction {
            params: lambda.params.clone(),
            body: Box::new(replace_grouping_calls_with_markers(
                &lambda.body,
                args,
                next_marker,
            )),
            syntax: lambda.syntax,
        }),
        sqlast::Expr::MemberOf(member_of) => sqlast::Expr::MemberOf(sqlast::MemberOf {
            value: Box::new(replace_grouping_calls_with_markers(
                &member_of.value,
                args,
                next_marker,
            )),
            array: Box::new(replace_grouping_calls_with_markers(
                &member_of.array,
                args,
                next_marker,
            )),
        }),
        other => other.clone(),
    }
}

fn replace_grouping_markers_in_window(
    window: &sqlast::WindowType,
    args: &mut Vec<(String, Vec<String>)>,
    next_marker: &mut i64,
) -> sqlast::WindowType {
    match window {
        sqlast::WindowType::WindowSpec(spec) => {
            let partition_by = spec
                .partition_by
                .iter()
                .map(|e| replace_grouping_calls_with_markers(e, args, next_marker))
                .collect();
            let order_by = spec
                .order_by
                .iter()
                .map(|ob| replace_grouping_markers_in_order_by_expr(ob, args, next_marker))
                .collect();
            sqlast::WindowType::WindowSpec(sqlast::WindowSpec {
                partition_by,
                order_by,
                ..spec.clone()
            })
        }
        other => other.clone(),
    }
}

fn flatten_grouping_groups(groups: &[Vec<sqlast::Expr>]) -> Vec<sqlast::Expr> {
    groups
        .iter()
        .flat_map(|group| group.iter().cloned())
        .collect()
}

fn unique_exprs_in_order<I>(exprs: I) -> Vec<sqlast::Expr>
where
    I: IntoIterator<Item = sqlast::Expr>,
{
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();
    for expr in exprs {
        let key = format!("{expr}").to_lowercase();
        if seen.insert(key) {
            result.push(expr);
        }
    }
    result
}

fn rollup_grouping_sets(groups: &[Vec<sqlast::Expr>]) -> Vec<Vec<sqlast::Expr>> {
    let mut grouping_sets = Vec::with_capacity(groups.len() + 1);
    for active_count in (0..=groups.len()).rev() {
        let grouping_set = groups
            .iter()
            .take(active_count)
            .flat_map(|group| group.iter().cloned())
            .collect();
        grouping_sets.push(grouping_set);
    }
    grouping_sets
}

fn cube_grouping_sets(groups: &[Vec<sqlast::Expr>]) -> Vec<Vec<sqlast::Expr>> {
    let group_count = groups.len();
    let total = 1usize.checked_shl(group_count as u32).unwrap_or(0);
    let mut grouping_sets = Vec::with_capacity(total);
    for mask in (0..total).rev() {
        let grouping_set = groups
            .iter()
            .enumerate()
            .filter(|(idx, _)| mask & (1usize << idx) != 0)
            .flat_map(|(_, group)| group.iter().cloned())
            .collect();
        grouping_sets.push(grouping_set);
    }
    grouping_sets
}

/// Walk a resolved TypedExpr tree and replace marker literals (Int(-9000), Int(-9001), ...)
/// with ColumnRef to the corresponding GROUPING/GROUPING_ID virtual slot name.
fn replace_grouping_markers_in_sort_item(
    item: &SortItem,
    grouping_fn_args: &[(String, Vec<String>)],
    grouping_fn_ids: &[(String, ColumnId)],
    emitted_marker_count: usize,
) -> SortItem {
    SortItem {
        expr: replace_grouping_markers_in_typed_expr(
            &item.expr,
            grouping_fn_args,
            grouping_fn_ids,
            emitted_marker_count,
        ),
        asc: item.asc,
        nulls_first: item.nulls_first,
    }
}

fn grouping_marker_index(value: i64, emitted_marker_count: usize) -> Option<usize> {
    let offset = (-9000i128).checked_sub(value as i128)?;
    if offset < 0 {
        return None;
    }
    let idx = usize::try_from(offset).ok()?;
    (idx < emitted_marker_count).then_some(idx)
}

fn replace_grouping_markers_in_typed_expr(
    expr: &TypedExpr,
    grouping_fn_args: &[(String, Vec<String>)],
    grouping_fn_ids: &[(String, ColumnId)],
    emitted_marker_count: usize,
) -> TypedExpr {
    match &expr.kind {
        ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(v)) => {
            if let Some(idx) = grouping_marker_index(*v, emitted_marker_count)
                && let (Some((fn_name, _)), Some((_, column_id))) =
                    (grouping_fn_args.get(idx), grouping_fn_ids.get(idx))
            {
                return TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: *column_id,
                        qualifier: None,
                        column: fn_name.clone(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                };
            }
            expr.clone()
        }
        ExprKind::BinaryOp { left, op, right } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(replace_grouping_markers_in_typed_expr(
                    left,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
                op: *op,
                right: Box::new(replace_grouping_markers_in_typed_expr(
                    right,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
            },
        },
        ExprKind::UnaryOp { op, expr: inner } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::UnaryOp {
                op: *op,
                expr: Box::new(replace_grouping_markers_in_typed_expr(
                    inner,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
            },
        },
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::FunctionCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| {
                        replace_grouping_markers_in_typed_expr(
                            arg,
                            grouping_fn_args,
                            grouping_fn_ids,
                            emitted_marker_count,
                        )
                    })
                    .collect(),
                distinct: *distinct,
            },
        },
        ExprKind::LambdaFunction { params, body } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::LambdaFunction {
                params: params.clone(),
                body: Box::new(replace_grouping_markers_in_typed_expr(
                    body,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
            },
        },
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::AggregateCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| {
                        replace_grouping_markers_in_typed_expr(
                            arg,
                            grouping_fn_args,
                            grouping_fn_ids,
                            emitted_marker_count,
                        )
                    })
                    .collect(),
                distinct: *distinct,
                order_by: order_by
                    .iter()
                    .map(|item| {
                        replace_grouping_markers_in_sort_item(
                            item,
                            grouping_fn_args,
                            grouping_fn_ids,
                            emitted_marker_count,
                        )
                    })
                    .collect(),
            },
        },
        ExprKind::Cast {
            expr: inner,
            target,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Cast {
                expr: Box::new(replace_grouping_markers_in_typed_expr(
                    inner,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
                target: target.clone(),
            },
        },
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::IsNull {
                expr: Box::new(replace_grouping_markers_in_typed_expr(
                    inner,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
                negated: *negated,
            },
        },
        ExprKind::InList {
            expr: inner,
            list,
            negated,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::InList {
                expr: Box::new(replace_grouping_markers_in_typed_expr(
                    inner,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
                list: list
                    .iter()
                    .map(|item| {
                        replace_grouping_markers_in_typed_expr(
                            item,
                            grouping_fn_args,
                            grouping_fn_ids,
                            emitted_marker_count,
                        )
                    })
                    .collect(),
                negated: *negated,
            },
        },
        ExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Between {
                expr: Box::new(replace_grouping_markers_in_typed_expr(
                    inner,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
                low: Box::new(replace_grouping_markers_in_typed_expr(
                    low,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
                high: Box::new(replace_grouping_markers_in_typed_expr(
                    high,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
                negated: *negated,
            },
        },
        ExprKind::Like {
            expr: inner,
            pattern,
            negated,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Like {
                expr: Box::new(replace_grouping_markers_in_typed_expr(
                    inner,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
                pattern: Box::new(replace_grouping_markers_in_typed_expr(
                    pattern,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
                negated: *negated,
            },
        },
        ExprKind::Nested(inner) => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Nested(Box::new(replace_grouping_markers_in_typed_expr(
                inner,
                grouping_fn_args,
                grouping_fn_ids,
                emitted_marker_count,
            ))),
        },
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::WindowCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| {
                        replace_grouping_markers_in_typed_expr(
                            a,
                            grouping_fn_args,
                            grouping_fn_ids,
                            emitted_marker_count,
                        )
                    })
                    .collect(),
                distinct: *distinct,
                partition_by: partition_by
                    .iter()
                    .map(|p| {
                        replace_grouping_markers_in_typed_expr(
                            p,
                            grouping_fn_args,
                            grouping_fn_ids,
                            emitted_marker_count,
                        )
                    })
                    .collect(),
                order_by: order_by
                    .iter()
                    .map(|ob| {
                        replace_grouping_markers_in_sort_item(
                            ob,
                            grouping_fn_args,
                            grouping_fn_ids,
                            emitted_marker_count,
                        )
                    })
                    .collect(),
                window_frame: window_frame.clone(),
                ignore_nulls: *ignore_nulls,
            },
        },
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Case {
                operand: operand.as_ref().map(|o| {
                    Box::new(replace_grouping_markers_in_typed_expr(
                        o,
                        grouping_fn_args,
                        grouping_fn_ids,
                        emitted_marker_count,
                    ))
                }),
                when_then: when_then
                    .iter()
                    .map(|(w, t)| {
                        (
                            replace_grouping_markers_in_typed_expr(
                                w,
                                grouping_fn_args,
                                grouping_fn_ids,
                                emitted_marker_count,
                            ),
                            replace_grouping_markers_in_typed_expr(
                                t,
                                grouping_fn_args,
                                grouping_fn_ids,
                                emitted_marker_count,
                            ),
                        )
                    })
                    .collect(),
                else_expr: else_expr.as_ref().map(|e| {
                    Box::new(replace_grouping_markers_in_typed_expr(
                        e,
                        grouping_fn_args,
                        grouping_fn_ids,
                        emitted_marker_count,
                    ))
                }),
            },
        },
        ExprKind::IsTruthValue {
            expr: inner,
            value,
            negated,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::IsTruthValue {
                expr: Box::new(replace_grouping_markers_in_typed_expr(
                    inner,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
                value: *value,
                negated: *negated,
            },
        },
        ExprKind::Lambda { params, body } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Lambda {
                params: params.clone(),
                body: Box::new(replace_grouping_markers_in_typed_expr(
                    body,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                )),
            },
        },
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => expr.clone(),
    }
}

fn replace_grouping_markers_in_relation(
    rel: &mut Relation,
    grouping_fn_args: &[(String, Vec<String>)],
    grouping_fn_ids: &[(String, ColumnId)],
    emitted_marker_count: usize,
) {
    match rel {
        Relation::Join(join) => {
            replace_grouping_markers_in_relation(
                &mut join.left,
                grouping_fn_args,
                grouping_fn_ids,
                emitted_marker_count,
            );
            replace_grouping_markers_in_relation(
                &mut join.right,
                grouping_fn_args,
                grouping_fn_ids,
                emitted_marker_count,
            );
            if let Some(condition) = join.condition.as_mut() {
                *condition = replace_grouping_markers_in_typed_expr(
                    condition,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                );
            }
        }
        Relation::Unnest(unnest) => {
            for arg in &mut unnest.args {
                *arg = replace_grouping_markers_in_typed_expr(
                    arg,
                    grouping_fn_args,
                    grouping_fn_ids,
                    emitted_marker_count,
                );
            }
        }
        Relation::Scan(_)
        | Relation::IcebergMetadataScan(_)
        | Relation::IcebergDeltaScan(_)
        | Relation::Subquery { .. }
        | Relation::GenerateSeries(_)
        | Relation::CTEConsume { .. } => {}
    }
}

fn is_bitmap_or_hll_type(sql_type: &novarocks_catalog::schema::SqlType) -> bool {
    matches!(
        sql_type,
        novarocks_catalog::schema::SqlType::Bitmap | novarocks_catalog::schema::SqlType::Hll
    )
}

fn sync_output_columns_from_projection(
    output_columns: &mut [crate::sql::analysis::OutputColumn],
    projection: &[crate::sql::analysis::ProjectItem],
) {
    for (output, item) in output_columns.iter_mut().zip(projection.iter()) {
        output.data_type = item.expr.data_type.clone();
        output.nullable = item.expr.nullable;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::IcebergMetadataTableType;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::sql::analysis::{
        ApplyClause, ApplyPredicateSpec, ApplyScalarSpec, BinOp, ExprKind, LiteralValue, Relation,
        SubqueryKind,
    };
    use crate::sql::catalog::IcebergMetadataTableProvider;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    struct TestCatalog;

    fn test_iceberg_table_info_for(
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: format!("file:///tmp/{catalog}/{namespace}/{table}"),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: Some(
                serde_json::to_string(
                    &crate::sql::analyzer::iceberg_ref::test_utils::metadata_empty(),
                )
                .expect("serialize test iceberg metadata"),
            ),
            serialized_metadata_rows: None,
        }
    }

    fn test_iceberg_table_info() -> IcebergTableInfo {
        test_iceberg_table_info_for("test_catalog", "test_db", "test_table")
    }

    impl TestCatalog {
        fn get_table(&self, _db: &str, table: &str) -> Result<TableDef, String> {
            match table {
                "t1" | "t2" | "t3" => {
                    let value_col = match table {
                        "t1" => "v1",
                        "t2" => "v2",
                        _ => "v3",
                    };
                    Ok(TableDef {
                        name: table.to_string(),
                        columns: vec![
                            ColumnDef {
                                name: "k1".to_string(),
                                data_type: arrow::datatypes::DataType::Int64,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                            ColumnDef {
                                name: "k2".to_string(),
                                data_type: arrow::datatypes::DataType::Int64,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                            ColumnDef {
                                name: value_col.to_string(),
                                data_type: arrow::datatypes::DataType::Utf8,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                        ],
                        iceberg_row_lineage_metadata_columns: vec![],
                        source: ScanSource::StarRocks {
                            db_id: 0,
                            table_id: 0,
                        },
                    })
                }
                "array_test" => Ok(TableDef {
                    name: "array_test".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "s_1".to_string(),
                            data_type: arrow::datatypes::DataType::List(
                                arrow::datatypes::Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Utf8,
                                    true,
                                )
                                .into(),
                            ),
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "i_1".to_string(),
                            data_type: arrow::datatypes::DataType::List(
                                arrow::datatypes::Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Int64,
                                    true,
                                )
                                .into(),
                            ),
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "d_1".to_string(),
                            data_type: arrow::datatypes::DataType::List(
                                arrow::datatypes::Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Decimal128(26, 2),
                                    true,
                                )
                                .into(),
                            ),
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                }),
                "orders" => Ok(TableDef {
                    name: "orders".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "o_orderkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_orderstatus".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_totalprice".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_orderdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_orderpriority".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                }),
                "lineitem" => Ok(TableDef {
                    name: "lineitem".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "l_orderkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_partkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_suppkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_quantity".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_extendedprice".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_discount".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_commitdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_receiptdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_shipdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                }),
                "supplier" => Ok(TableDef {
                    name: "supplier".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "s_suppkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "s_name".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "s_comment".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                }),
                "part" => Ok(TableDef {
                    name: "part".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "p_partkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "p_name".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "p_brand".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                }),
                "partsupp" => Ok(TableDef {
                    name: "partsupp".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "ps_partkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "ps_suppkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "ps_supplycost".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "ps_availqty".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                }),
                "customer" => Ok(TableDef {
                    name: "customer".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "c_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "c_acctbal".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "c_phone".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                }),
                "nation" => Ok(TableDef {
                    name: "nation".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "n_nationkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "n_name".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                }),
                // IVM-A1 v3-row-lineage fixture: an iceberg-backed base
                // table exposing the row-lineage metadata pseudo-columns
                // (_row_id, _last_updated_sequence_number) that
                // `__nr_ivm_delta` requires.
                "iv_orders" => Ok(TableDef {
                    name: "iv_orders".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "o_orderkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![
                        ColumnDef {
                            name: "_row_id".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "_last_updated_sequence_number".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info(),
                        files: vec![],
                        cloud_properties: Default::default(),
                        binding:
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
                    },
                }),
                _ => Err(format!("table not found: {table}")),
            }
        }
    }

    impl crate::sql::catalog::PlannerTableProvider for TestCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::sql::catalog::ResolvedAnalyzerTable, String> {
            let planner = self.get_table(database, table)?;
            Ok(crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }

        fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
            Some(self)
        }
    }

    impl IcebergMetadataTableProvider for TestCatalog {
        fn get_iceberg_metadata_table(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
            _metadata_table_type: IcebergMetadataTableType,
        ) -> Result<TableDef, String> {
            let mut table_def = self.get_table(database, table)?;
            table_def.source = ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info_for(
                    catalog.unwrap_or("default_catalog"),
                    database,
                    table,
                ),
                files: vec![],
                cloud_properties: Default::default(),
                binding:
                    crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
            };
            Ok(table_def)
        }
    }

    fn parse_and_analyze(sql: &str) -> Result<ResolvedQuery, String> {
        let dialect = sqlparser::dialect::GenericDialect {};
        let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql)
            .map_err(|e| format!("parse error: {e}"))?;
        let stmt = stmts.into_iter().next().ok_or("empty SQL")?;
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => return Err("expected a query".into()),
        };
        let (resolved, _registry, _factory) = analyze(&query, &TestCatalog, "default")?;
        Ok(resolved)
    }

    fn parse_and_analyze_with_registry(
        sql: &str,
    ) -> Result<(ResolvedQuery, crate::sql::analysis::cte::CTERegistry), String> {
        let dialect = crate::sql::parser::dialect::StarRocksDialect;
        let mut ast =
            sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
        let stmt = ast
            .pop()
            .ok_or_else(|| "expected a statement".to_string())?;
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => return Err("expected a query".into()),
        };
        let (resolved, registry, _factory) = analyze(&query, &TestCatalog, "default")?;
        Ok((resolved, registry))
    }

    fn parse_raw_and_analyze(sql: &str) -> Result<ResolvedQuery, String> {
        let stmt = crate::sql::parser::parse_sql_raw(sql)?;
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => return Err("expected a query".into()),
        };
        let (resolved, _registry, _factory) = analyze(&query, &TestCatalog, "default")?;
        Ok(resolved)
    }

    #[test]
    fn decimal_literal_over_decimal128_precision_infers_decimal256() {
        let resolved =
            parse_raw_and_analyze("select 123456789012345678901234567890.123456789 as d")
                .expect("wide decimal literal should analyze");

        assert_eq!(
            resolved.output_columns[0].data_type,
            DataType::Decimal256(39, 9)
        );
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.projection[0].expr.data_type,
            DataType::Decimal256(39, 9)
        );
    }

    #[test]
    fn decimal_literal_above_decimal256_precision_is_rejected() {
        let err = parse_raw_and_analyze(
            "select 12345678901234567890123456789012345678901234567890123456789012345678901234567.1",
        )
        .expect_err("decimal literal above Decimal256 precision should fail during analysis");

        assert!(
            err.contains("decimal literal precision 78 exceeds maximum precision 76"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn array_agg_window_decimal_literal_preserves_decimal256_item() {
        let (resolved, _) = parse_and_analyze_with_registry(
            "select array_agg(123456789012345678901234567890.123456789) \
         over(partition by o_orderstatus order by o_orderkey) as arr from orders",
        )
        .expect("array_agg window over wide decimal literal should analyze");

        let expected = DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Decimal256(39, 9),
            true,
        )));
        assert_eq!(resolved.output_columns[0].data_type, expected);
    }

    #[test]
    fn hex_string_literal_analyzes_as_binary() {
        let resolved = parse_raw_and_analyze("SELECT X'AB01'").expect("analysis should succeed");
        assert_eq!(resolved.output_columns[0].data_type, DataType::Binary);
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        let ExprKind::Literal(LiteralValue::Binary(bytes)) = &select.projection[0].expr.kind else {
            panic!("expected binary literal");
        };
        assert_eq!(bytes, &[0xab, 0x01]);
    }

    #[test]
    fn derived_values_column_alias_is_visible() {
        let sql = "SELECT col1 FROM (VALUES (1)) AS tmp(col1)";
        let (resolved, _) = parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        assert_eq!(resolved.output_columns[0].name, "col1");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        let Relation::Subquery {
            query,
            output_columns,
            ..
        } = select.from.as_ref().expect("expected FROM relation")
        else {
            panic!("expected derived subquery");
        };
        assert_eq!(query.output_columns[0].name, "column_0");
        assert_eq!(output_columns[0].name, "col1");
        let ExprKind::ColumnRef {
            qualifier, column, ..
        } = &select.projection[0].expr.kind
        else {
            panic!("expected column ref projection");
        };
        assert_eq!(qualifier.as_deref(), None);
        assert_eq!(column, "col1");
    }

    #[test]
    fn derived_table_column_alias_count_must_match() {
        let sql = "SELECT a FROM (VALUES (1, 2)) AS tmp(a)";
        let err = parse_and_analyze_with_registry(sql).expect_err("analysis must fail");
        assert!(
            err.contains("has 1 column aliases but subquery produces 2 columns"),
            "err={err}"
        );
    }

    fn expr_has_qualified_column(expr: &TypedExpr, qualifier: &str, column: &str) -> bool {
        match &expr.kind {
            ExprKind::ColumnRef {
                qualifier: Some(q),
                column: c,
                ..
            } => q.eq_ignore_ascii_case(qualifier) && c.eq_ignore_ascii_case(column),
            ExprKind::BinaryOp { left, right, .. } => {
                expr_has_qualified_column(left, qualifier, column)
                    || expr_has_qualified_column(right, qualifier, column)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::Nested(expr) => expr_has_qualified_column(expr, qualifier, column),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => args
                .iter()
                .any(|arg| expr_has_qualified_column(arg, qualifier, column)),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| expr_has_qualified_column(expr, qualifier, column))
                    || when_then.iter().any(|(when, then)| {
                        expr_has_qualified_column(when, qualifier, column)
                            || expr_has_qualified_column(then, qualifier, column)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|expr| expr_has_qualified_column(expr, qualifier, column))
            }
            ExprKind::InList { expr, list, .. } => {
                expr_has_qualified_column(expr, qualifier, column)
                    || list
                        .iter()
                        .any(|item| expr_has_qualified_column(item, qualifier, column))
            }
            _ => false,
        }
    }

    fn expr_has_unqualified_column(expr: &TypedExpr, column: &str) -> bool {
        match &expr.kind {
            ExprKind::ColumnRef {
                qualifier: None,
                column: c,
                ..
            } => c.eq_ignore_ascii_case(column),
            ExprKind::BinaryOp { left, right, .. } => {
                expr_has_unqualified_column(left, column)
                    || expr_has_unqualified_column(right, column)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::Nested(expr) => expr_has_unqualified_column(expr, column),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => args
                .iter()
                .any(|arg| expr_has_unqualified_column(arg, column)),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| expr_has_unqualified_column(expr, column))
                    || when_then.iter().any(|(when, then)| {
                        expr_has_unqualified_column(when, column)
                            || expr_has_unqualified_column(then, column)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|expr| expr_has_unqualified_column(expr, column))
            }
            ExprKind::InList { expr, list, .. } => {
                expr_has_unqualified_column(expr, column)
                    || list
                        .iter()
                        .any(|item| expr_has_unqualified_column(item, column))
            }
            _ => false,
        }
    }

    fn select_body(resolved: &ResolvedQuery) -> &ResolvedSelect {
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        sel
    }

    fn only_predicate_spec(sel: &ResolvedSelect) -> &ApplyPredicateSpec {
        assert_eq!(
            sel.predicate_apply_specs.len(),
            1,
            "expected one predicate Apply spec: {:?}",
            sel.predicate_apply_specs
        );
        &sel.predicate_apply_specs[0]
    }

    fn only_scalar_spec(sel: &ResolvedSelect) -> &ApplyScalarSpec {
        assert_eq!(
            sel.apply_specs.len(),
            1,
            "expected one scalar Apply spec: {:?}",
            sel.apply_specs
        );
        &sel.apply_specs[0]
    }

    fn apply_inner_filter(query: &ResolvedQuery) -> &TypedExpr {
        let QueryBody::Select(sel) = &query.body else {
            panic!("expected Select body in Apply inner query");
        };
        sel.filter
            .as_ref()
            .expect("expected Apply inner query filter")
    }

    #[test]
    fn exists_subquery_rewrites_to_left_semi_join() {
        let sql = "SELECT o_orderpriority, count(*) FROM orders \
                    WHERE exists (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey) \
                    GROUP BY o_orderpriority";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(spec.kind, SubqueryKind::Exists { negated: false }));
        assert_eq!(spec.clause, ApplyClause::Where);
        assert!(spec.use_semi_anti);
        assert!(
            sel.filter.is_none() || !filter_has_placeholder(&sel.filter),
            "filter should not contain SubqueryPlaceholder"
        );
    }

    #[test]
    fn correlated_exists_self_join_apply_inner_keeps_outer_qualifier_for_ambiguous_column() {
        let sql = "SELECT l1.l_orderkey FROM lineitem l1 \
                   WHERE EXISTS (SELECT 1 FROM lineitem l2 \
                                 WHERE l2.l_partkey = l1.l_suppkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        let cond = apply_inner_filter(&spec.inner);

        assert!(
            expr_has_qualified_column(cond, "l1", "l_suppkey"),
            "outer self-join column must keep its qualifier in correlated EXISTS filter: {cond:?}"
        );
        let ExprKind::BinaryOp { left, op, right } = &cond.kind else {
            panic!("expected binary correlation condition, got: {cond:?}");
        };
        assert_eq!(*op, BinOp::Eq);
        assert!(
            expr_has_qualified_column(left, "l2", "l_partkey"),
            "inner column should stay on the original SQL left side in Apply inner filter: {cond:?}"
        );
        assert!(
            expr_has_qualified_column(right, "l1", "l_suppkey"),
            "outer column should stay on the original SQL right side in Apply inner filter: {cond:?}"
        );
    }

    #[test]
    fn exists_multi_table_subquery_rewrites_to_semi_join() {
        // When the EXISTS subquery has multiple tables and a mix of correlation
        // and inner-join predicates, the subquery rewriter produces a LEFT SEMI
        // JOIN. The right side is the flattened inner FROM clause.
        //
        // Known limitation: ideally `l_suppkey = s_suppkey` and `s_name = 'test'`
        // should stay inside the subquery as inner predicates (preventing the
        // inner side from degenerating into a CROSS JOIN). The current rewriter
        // hoists all predicates to the SEMI condition and flattens the FROM
        // clause as a bare multi-table join. Predicate pushdown in the rewrite/CBO
        // phases later pushes them back down, so the runtime plan is
        // functionally correct (TPC-DS 98/99 pass), but the analyzer output is
        // suboptimal for this specific pattern. This test documents the current
        // behavior; a proper fix would be to partition predicates into
        // correlation vs inner during subquery rewriting.
        let sql = "SELECT o_orderkey FROM orders \
                    WHERE EXISTS (SELECT * FROM lineitem, supplier \
                                  WHERE l_orderkey = o_orderkey \
                                  AND l_suppkey = s_suppkey \
                                  AND s_name = 'test')";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(spec.kind, SubqueryKind::Exists { negated: false }));
        assert!(
            apply_inner_filter(&spec.inner).data_type == arrow::datatypes::DataType::Boolean,
            "inner EXISTS filter should retain correlation and residual predicates"
        );
    }

    #[test]
    fn not_exists_subquery_rewrites_to_left_anti_join() {
        let sql = "SELECT o_orderpriority FROM orders \
                    WHERE not exists (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(spec.kind, SubqueryKind::Exists { negated: true }));
        assert_eq!(spec.clause, ApplyClause::Where);
    }

    #[test]
    fn in_subquery_rewrites_to_left_semi_join() {
        let sql = "SELECT o_orderkey FROM orders \
                    WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(
            spec.kind,
            SubqueryKind::InSubquery { negated: false }
        ));
        assert!(spec.in_lhs.is_some());
    }

    #[test]
    fn not_in_subquery_rewrites_to_left_anti_join() {
        let sql = "SELECT s_suppkey FROM supplier \
                    WHERE s_suppkey NOT IN (SELECT ps_suppkey FROM partsupp)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(
            spec.kind,
            SubqueryKind::InSubquery { negated: true }
        ));
        assert!(spec.in_lhs.is_some());
    }

    #[test]
    fn correlated_not_in_nullable_key_rewrites_to_null_aware_left_anti() {
        let sql = "SELECT l1.l_orderkey FROM lineitem l1 \
                   WHERE l1.l_shipdate NOT IN ( \
                       SELECT l2.l_shipdate FROM lineitem l2 \
                       WHERE l2.l_suppkey = l1.l_suppkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);

        assert!(
            matches!(spec.kind, SubqueryKind::InSubquery { negated: true }),
            "correlated nullable NOT IN should be routed as a negated IN Apply spec"
        );
        assert!(spec.in_lhs.as_ref().is_some_and(|lhs| lhs.nullable));
    }

    #[test]
    fn correlated_not_in_nullable_filter_stays_residual_for_null_aware_anti() {
        let sql = "SELECT l1.l_orderkey FROM lineitem l1 \
                   WHERE l1.l_shipdate NOT IN ( \
                       SELECT l2.l_shipdate FROM lineitem l2 \
                       WHERE l2.l_receiptdate = l1.l_receiptdate)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        let filter = apply_inner_filter(&spec.inner);

        assert!(
            expr_has_qualified_column(filter, "l1", "l_receiptdate")
                && expr_has_qualified_column(filter, "l2", "l_receiptdate"),
            "correlated NOT IN filter should stay in Apply inner query, got: {filter:?}"
        );
    }

    #[test]
    fn subquery_unqualified_name_prefers_inner_over_outer_using_canonical() {
        let sql = "SELECT k1, k2, v1, v2 \
                   FROM t1 FULL OUTER JOIN t2 USING(k1, k2) \
                   WHERE EXISTS (SELECT 1 FROM t3 WHERE t3.k1 = k1 AND t3.k2 = k2)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        let cond = apply_inner_filter(&spec.inner);

        assert!(
            !expr_has_qualified_column(cond, "t1", "k1")
                && !expr_has_qualified_column(cond, "t2", "k1"),
            "inner subquery k1 must not inherit outer USING canonical qualifier: {cond:?}"
        );
        assert!(
            expr_has_qualified_column(cond, "t3", "k1")
                && expr_has_qualified_column(cond, "t3", "k2"),
            "subquery-local names should resolve to t3 columns: {cond:?}"
        );
        assert!(
            !expr_has_unqualified_column(cond, "k1") && !expr_has_unqualified_column(cond, "k2"),
            "subquery-local shadowing columns must stay qualified for codegen: {cond:?}"
        );
    }

    #[test]
    fn correlated_exists_apply_inner_preserves_original_compare_sides() {
        let sql = "SELECT s.s_1 FROM array_test s \
                   WHERE EXISTS (SELECT 1 FROM array_test t WHERE t.s_1 = s.d_1)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        let cond = apply_inner_filter(&spec.inner);

        let ExprKind::BinaryOp { left, op, right } = &cond.kind else {
            panic!("expected binary correlation condition, got: {cond:?}");
        };
        assert_eq!(*op, BinOp::Eq);
        assert!(expr_has_qualified_column(left, "t", "s_1"));
        assert!(expr_has_qualified_column(right, "s", "d_1"));
    }

    #[test]
    fn uncorrelated_scalar_subquery_rewrites_to_cross_join() {
        let sql = "SELECT c_custkey FROM customer \
                    WHERE c_acctbal > (SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_scalar_spec(sel);
        assert_eq!(spec.clause, ApplyClause::Where);
        assert!(spec.correlation_column_ids.is_empty());
        assert!(
            sel.filter.is_some(),
            "filter should still contain the comparison"
        );
        assert!(
            !filter_has_placeholder(&sel.filter),
            "filter should not contain SubqueryPlaceholder"
        );
    }

    #[test]
    fn correlated_scalar_subquery_rewrites_to_left_join() {
        let sql = "SELECT l_orderkey FROM lineitem, part \
                    WHERE p_partkey = l_partkey \
                    AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_scalar_spec(sel);
        assert_eq!(spec.clause, ApplyClause::Where);
        assert!(
            !spec.correlation_column_ids.is_empty(),
            "correlated scalar subquery should record outer correlation columns"
        );
    }

    #[test]
    fn scalar_subquery_in_having_rewrites_to_cross_join() {
        let sql = "SELECT ps_partkey, sum(ps_supplycost) as value FROM partsupp \
                    GROUP BY ps_partkey \
                    HAVING sum(ps_supplycost) > (SELECT sum(ps_supplycost) * 0.0001 FROM partsupp)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_scalar_spec(sel);
        assert_eq!(spec.clause, ApplyClause::Having);
        assert!(
            sel.having.is_some() && !filter_has_placeholder(&sel.having),
            "HAVING should retain comparison with scalar Apply output and no placeholder"
        );
    }

    #[test]
    fn scalar_subquery_in_projection_without_outer_from_synthesizes_dummy() {
        // Mirrors the iceberg_in_list_predicate failure pattern: outer SELECT
        // has no FROM clause; the projection contains scalar subqueries.
        // Without a synthetic single-row FROM, the subquery rewriter would
        // bail out with `scalar subquery rewrite requires a FROM clause`.
        let sql = "SELECT COALESCE((SELECT count(*) FROM customer WHERE c_acctbal > 0), 0) AS n_pos, \
             COALESCE((SELECT count(*) FROM customer WHERE c_acctbal < 0), 0) AS n_neg";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        assert!(
            sel.from.is_none(),
            "analyzer should not synthesize FROM for Apply specs"
        );
        assert_eq!(sel.apply_specs.len(), 2);
        assert!(
            sel.apply_specs
                .iter()
                .all(|spec| spec.clause == ApplyClause::Projection),
            "projection scalar subqueries should be routed to projection Apply specs"
        );
        assert!(
            sel.projection
                .iter()
                .all(|item| !expr_has_placeholder(&item.expr)),
            "projection should not contain SubqueryPlaceholder"
        );
    }

    #[test]
    fn multiple_subqueries_exists_and_not_exists() {
        // q21 pattern: EXISTS + NOT EXISTS in the same WHERE
        let sql = "SELECT s_name FROM supplier, lineitem l1, orders, nation \
                    WHERE s_suppkey = l1.l_suppkey \
                    AND o_orderkey = l1.l_orderkey \
                    AND exists (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) \
                    AND not exists (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        assert_eq!(sel.predicate_apply_specs.len(), 2);
        assert!(
            sel.predicate_apply_specs
                .iter()
                .any(|spec| matches!(spec.kind, SubqueryKind::Exists { negated: false })),
            "EXISTS should produce a predicate Apply spec"
        );
        assert!(
            sel.predicate_apply_specs
                .iter()
                .any(|spec| matches!(spec.kind, SubqueryKind::Exists { negated: true })),
            "NOT EXISTS should produce a predicate Apply spec"
        );
        assert!(
            !filter_has_placeholder(&sel.filter),
            "filter should not contain SubqueryPlaceholder"
        );
    }

    #[test]
    fn subquery_in_from_derived_table() {
        // q22 pattern: subquery inside a derived table in FROM
        let sql = "SELECT cntrycode FROM \
                    (SELECT substring(c_phone, 1, 2) as cntrycode, c_acctbal FROM customer \
                     WHERE c_acctbal > (SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0.00)) as custsale";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        // The subquery in the derived table should be rewritten to a CROSS JOIN
        // within the derived table's ResolvedQuery
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn in_subquery_with_group_by_having() {
        // q18 pattern: IN subquery with GROUP BY and HAVING
        let sql = "SELECT o_orderkey FROM orders \
                    WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 315)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(
            spec.kind,
            SubqueryKind::InSubquery { negated: false }
        ));
        assert!(spec.in_lhs.is_some());
    }

    fn filter_has_placeholder(filter: &Option<TypedExpr>) -> bool {
        match filter {
            Some(expr) => expr_has_placeholder(expr),
            None => false,
        }
    }

    fn expr_has_placeholder(expr: &TypedExpr) -> bool {
        match &expr.kind {
            ExprKind::SubqueryPlaceholder { .. } => true,
            ExprKind::BinaryOp { left, right, .. } => {
                expr_has_placeholder(left) || expr_has_placeholder(right)
            }
            ExprKind::UnaryOp { expr, .. } => expr_has_placeholder(expr),
            ExprKind::Nested(inner) => expr_has_placeholder(inner),
            _ => false,
        }
    }

    /// Deep check for SubqueryPlaceholder in any expression node.
    fn expr_has_placeholder_deep(expr: &TypedExpr) -> bool {
        match &expr.kind {
            ExprKind::SubqueryPlaceholder { .. } => true,
            ExprKind::BinaryOp { left, right, .. } => {
                expr_has_placeholder_deep(left) || expr_has_placeholder_deep(right)
            }
            ExprKind::UnaryOp { expr, .. } => expr_has_placeholder_deep(expr),
            ExprKind::Nested(inner) => expr_has_placeholder_deep(inner),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                args.iter().any(expr_has_placeholder_deep)
            }
            ExprKind::Cast { expr, .. } | ExprKind::IsNull { expr, .. } => {
                expr_has_placeholder_deep(expr)
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|o| expr_has_placeholder_deep(o))
                    || when_then
                        .iter()
                        .any(|(w, t)| expr_has_placeholder_deep(w) || expr_has_placeholder_deep(t))
                    || else_expr
                        .as_ref()
                        .is_some_and(|e| expr_has_placeholder_deep(e))
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                expr_has_placeholder_deep(expr)
                    || expr_has_placeholder_deep(low)
                    || expr_has_placeholder_deep(high)
            }
            ExprKind::Like { expr, pattern, .. } => {
                expr_has_placeholder_deep(expr) || expr_has_placeholder_deep(pattern)
            }
            ExprKind::InList { expr, list, .. } => {
                expr_has_placeholder_deep(expr) || list.iter().any(expr_has_placeholder_deep)
            }
            ExprKind::IsTruthValue { expr, .. } => expr_has_placeholder_deep(expr),
            ExprKind::WindowCall { args, .. } => args.iter().any(expr_has_placeholder_deep),
            _ => false,
        }
    }

    #[test]
    fn test_table_alias_qualified_reference() {
        // Simplified q3 pattern: table alias with qualified reference
        let sql = "SELECT o.o_orderkey FROM orders o WHERE o.o_custkey > 100";
        let resolved = parse_and_analyze(sql).expect("table alias qualified ref should work");
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn test_cte_with_alias() {
        // Simplified q1 pattern: CTE with alias
        let sql = "WITH order_totals AS (SELECT o_orderkey as ok, o_totalprice as total FROM orders) \
                   SELECT t1.ok FROM order_totals t1 WHERE t1.total > 100";
        let resolved = parse_and_analyze(sql).expect("CTE with alias should work");
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn test_cte_qualified_projection_uses_base_column_names() {
        let sql = "WITH joined AS ( \
                     SELECT o1.o_orderkey, o2.o_custkey \
                     FROM orders o1 JOIN orders o2 ON o1.o_orderkey = o2.o_orderkey \
                   ) \
                   SELECT o_orderkey, o_custkey FROM joined";
        let resolved =
            parse_and_analyze(sql).expect("qualified CTE projection should expose base names");
        assert_eq!(resolved.output_columns.len(), 2);
        assert_eq!(resolved.output_columns[0].name, "o_orderkey");
        assert_eq!(resolved.output_columns[1].name, "o_custkey");
    }

    #[test]
    fn test_single_use_cte_is_still_registered() {
        let sql = "WITH order_totals AS (SELECT o_orderkey AS ok FROM orders) \
                   SELECT ok FROM order_totals";
        let (resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        assert_eq!(registry.entries.len(), 1);
        assert_eq!(registry.entries[0].name, "order_totals");
        match &resolved.body {
            QueryBody::Select(sel) => match sel.from.as_ref().expect("should have FROM") {
                Relation::CTEConsume { cte_id, .. } => {
                    let entry = registry
                        .entries
                        .iter()
                        .find(|entry| entry.id == *cte_id)
                        .expect("cte id should exist in registry");
                    assert_eq!(entry.name, "order_totals");
                }
                _ => panic!("expected direct CTEConsume for single-use top-level CTE"),
            },
            _ => panic!("expected select body"),
        }
    }

    #[test]
    fn cte_consume_records_producer_column_ids_parallel_to_consumer_outputs() {
        let sql = "WITH c AS (SELECT o_orderkey AS k, o_custkey AS c FROM orders) \
                   SELECT a.k, a.c FROM c a";
        let (resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        let entry = registry
            .entries
            .iter()
            .find(|entry| entry.name == "c")
            .expect("cte entry");
        let producer_ids: Vec<_> = entry.output_columns.iter().map(|c| c.column_id).collect();

        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        let Relation::CTEConsume {
            output_columns,
            producer_column_ids,
            ..
        } = select.from.as_ref().expect("from relation")
        else {
            panic!("expected direct CTEConsume");
        };

        let consumer_ids: Vec<_> = output_columns.iter().map(|c| c.column_id).collect();
        assert_eq!(producer_column_ids, &producer_ids);
        assert_eq!(producer_column_ids.len(), output_columns.len());
        assert_ne!(
            consumer_ids, producer_ids,
            "consumer aliases must use fresh ColumnIds"
        );
    }

    #[test]
    fn test_forward_cte_reference_is_rejected() {
        let sql = "WITH a AS (SELECT 1 AS x) \
                   SELECT * FROM (\
                     WITH b AS (SELECT * FROM a), a AS (SELECT o_orderkey FROM orders) \
                     SELECT * FROM b\
                   ) s";
        let err = parse_and_analyze_with_registry(sql).expect_err("forward reference must fail");
        assert!(
            err.contains("forward CTE reference is not supported"),
            "err={err}"
        );
    }

    #[test]
    fn test_nested_with_inherits_enclosing_pending_ctes() {
        let sql = "WITH early AS (\
                     WITH nested AS (SELECT * FROM orders) \
                     SELECT * FROM nested\
                   ), orders AS (SELECT 1 AS x) \
                   SELECT * FROM early";
        let err = parse_and_analyze_with_registry(sql).expect_err("forward reference must fail");
        assert!(
            err.contains("forward CTE reference is not supported: orders"),
            "err={err}"
        );
    }

    #[test]
    fn test_inner_cte_shadows_outer_cte() {
        let sql = "WITH t AS (SELECT 1 AS x) \
                   SELECT * FROM (WITH t AS (SELECT 2 AS x) SELECT x FROM t) s";
        let (resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        let cte_id = match &resolved.body {
            QueryBody::Select(sel) => match sel.from.as_ref().expect("should have FROM") {
                Relation::Subquery { query, .. } => match &query.body {
                    QueryBody::Select(inner_sel) => match inner_sel.from.as_ref() {
                        Some(Relation::CTEConsume { cte_id, .. }) => *cte_id,
                        _ => panic!("expected CTEConsume inside shadowing subquery"),
                    },
                    _ => panic!("expected select inside subquery"),
                },
                _ => panic!("expected derived subquery"),
            },
            _ => panic!("expected select body"),
        };
        let entry = registry
            .entries
            .iter()
            .find(|entry| entry.id == cte_id)
            .expect("cte id should exist in registry");
        let inner_value = match &entry.resolved_query.body {
            QueryBody::Select(inner_sel) => match &inner_sel.projection[0].expr.kind {
                ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(v)) => Some(*v),
                _ => None,
            },
            _ => None,
        };
        assert_eq!(inner_value, Some(2));
    }

    #[test]
    fn test_inner_with_does_not_leak_to_sibling_scope() {
        let sql = "WITH t AS (SELECT 1 AS x) \
                   SELECT * FROM (WITH t AS (SELECT 2 AS x) SELECT 1) d, t";
        let (resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        let sibling_cte_id = match &resolved.body {
            QueryBody::Select(sel) => match sel.from.as_ref().expect("should have FROM") {
                Relation::Join(join) => match &join.right {
                    Relation::CTEConsume { cte_id, .. } => *cte_id,
                    _ => panic!("expected sibling CTEConsume"),
                },
                _ => panic!("expected join from comma-separated FROM items"),
            },
            _ => panic!("expected select body"),
        };
        let entry = registry
            .entries
            .iter()
            .find(|entry| entry.id == sibling_cte_id)
            .expect("cte id should exist in registry");
        let outer_value = match &entry.resolved_query.body {
            QueryBody::Select(inner_sel) => match &inner_sel.projection[0].expr.kind {
                ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(v)) => Some(*v),
                _ => None,
            },
            _ => None,
        };
        assert_eq!(outer_value, Some(1));
    }

    #[test]
    fn test_cte_with_comma_join_and_correlated_subquery() {
        // Closer to q1 pattern: CTE with comma-join and correlated subquery
        let sql = "WITH order_totals AS (\
                     SELECT o_orderkey as ok, o_custkey as ck, o_totalprice as total FROM orders\
                   ) \
                   SELECT t1.ok FROM order_totals t1, customer \
                   WHERE t1.total > (\
                     SELECT avg(t2.total) FROM order_totals t2 WHERE t1.ck = t2.ck\
                   ) AND t1.ck = c_custkey";
        let resolved = parse_and_analyze(sql)
            .expect("CTE with comma-join and correlated subquery should work");
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn test_correlated_subquery_with_inner_with_registers_cte() {
        let sql = "SELECT o_orderkey FROM orders o \
                   WHERE EXISTS (\
                     WITH filtered AS (SELECT l_orderkey FROM lineitem) \
                     SELECT 1 FROM filtered WHERE filtered.l_orderkey = o.o_orderkey\
                   )";
        let (_resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        assert_eq!(registry.entries.len(), 1);
        assert_eq!(registry.entries[0].name, "filtered");
    }

    #[test]
    fn test_comma_join_multiple_aliases() {
        // Simplified q3 pattern: comma-join with multiple table aliases
        let sql = "SELECT o.o_orderkey, l.l_partkey \
                   FROM orders o, lineitem l \
                   WHERE o.o_orderkey = l.l_orderkey \
                   GROUP BY o.o_orderkey, l.l_partkey";
        let resolved =
            parse_and_analyze(sql).expect("comma-join with multiple aliases should work");
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn test_scalar_subquery_in_projection() {
        // q9 pattern: scalar subqueries in projection (SELECT list), not in WHERE
        let sql = "SELECT (SELECT count(*) FROM orders) as total_orders FROM lineitem \
                   WHERE l_orderkey = 1";
        let resolved = parse_and_analyze(sql).expect("scalar subquery in projection should work");
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn test_case_with_scalar_subqueries_in_projection() {
        // q9 pattern: CASE WHEN with scalar subqueries in projection
        let sql = "SELECT CASE WHEN (SELECT count(*) FROM orders) > 100 \
                          THEN (SELECT avg(o_totalprice) FROM orders) \
                          ELSE (SELECT avg(o_totalprice) FROM orders WHERE o_totalprice > 0) \
                   END as bucket1 \
                   FROM lineitem WHERE l_orderkey = 1";
        let resolved =
            parse_and_analyze(sql).expect("CASE with scalar subqueries in projection should work");
        assert!(!resolved.output_columns.is_empty());
        assert_eq!(
            resolved.output_columns[0].data_type,
            arrow::datatypes::DataType::Float64,
            "rewritten CASE scalar-subquery projection should expose the final branch type"
        );
        // Verify that no SubqueryPlaceholder remains in the projection
        if let QueryBody::Select(sel) = &resolved.body {
            for item in &sel.projection {
                assert_eq!(
                    item.expr.data_type,
                    arrow::datatypes::DataType::Float64,
                    "projection expression type should be recomputed after placeholder replacement"
                );
                assert!(
                    !expr_has_placeholder_deep(&item.expr),
                    "projection should not contain SubqueryPlaceholder after rewriting: {:?}",
                    item.expr
                );
            }
        }
    }

    #[test]
    fn test_percentile_approx_with_array_literal_returns_array_type() {
        let resolved = parse_raw_and_analyze(
            "SELECT percentile_approx(o_totalprice, array<double>[0.25, 0.5]) FROM orders",
        )
        .expect("analysis should succeed");
        assert_eq!(resolved.output_columns.len(), 1);
        assert!(matches!(
            resolved.output_columns[0].data_type,
            arrow::datatypes::DataType::List(_)
        ));
    }

    #[test]
    fn test_percentile_approx_weighted_rejects_null_weight_in_analyzer() {
        let err = parse_raw_and_analyze("SELECT percentile_approx_weighted(1, NULL, 0.9)")
            .expect_err("analysis should reject NULL weight");
        assert!(err.contains(
            "percentile_approx_weighted requires the second parameter (weight) to be numeric type, but got: NULL_TYPE."
        ));
    }

    #[test]
    fn test_percentile_approx_weighted_rejects_negative_scalar_percentile_in_analyzer() {
        let err = parse_raw_and_analyze("SELECT percentile_approx_weighted(1, 1, -0.1)")
            .expect_err("analysis should reject negative percentile");
        assert!(err.contains(
            "Type check failed. percentile parameter must be between 0 and 1 in percentile_approx_weighted, but got: -0.1"
        ));
    }

    #[test]
    fn test_percentile_approx_weighted_rejects_negative_array_percentile_in_analyzer() {
        let err = parse_raw_and_analyze("SELECT percentile_approx_weighted(1, 1, [-0.1, 0.5])")
            .expect_err("analysis should reject negative percentile array item");
        assert!(err.contains(
            "Type check failed. percentile array element[0] must be between 0 and 1 in percentile_approx_weighted, but got: -0.1"
        ));
    }

    #[test]
    fn test_group_concat_rejects_negative_order_by_position() {
        let err = parse_raw_and_analyze(
            "SELECT group_concat(distinct 3.1323, o_orderstatus order by 1, 2, -20) FROM orders GROUP BY o_orderkey",
        )
        .expect_err("negative group_concat ORDER BY position should be rejected");
        assert!(
            err.contains("ORDER BY position -20 is not in group_concat output list."),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_group_concat_rejects_empty_input() {
        let err = parse_raw_and_analyze("SELECT group_concat()")
            .expect_err("group_concat without input should be rejected");
        assert!(
            err.contains("group_concat should have at least one input."),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_group_concat_rejects_non_string_separator() {
        let err = parse_raw_and_analyze(
            "SELECT group_concat(\"中国\" ORDER BY \"第一\" SEPARATOR 1) FROM orders",
        )
        .expect_err("group_concat should reject non-string separator");
        assert!(
            err.contains(
                "group_concat requires separator to be of getType() STRING: group_concat('中国', 1)."
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_group_concat_rejects_array_input() {
        let err = parse_raw_and_analyze("SELECT group_concat([1,2]) FROM orders")
            .expect_err("group_concat should reject array input");
        assert!(
            err.contains(
                "No matching function with signature: group_concat(array<tinyint(4)>, varchar)."
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_group_concat_rejects_map_input() {
        let err = parse_raw_and_analyze("SELECT group_concat(map(2,3)) FROM orders")
            .expect_err("group_concat should reject map input");
        assert!(
            err.contains(
                "No matching function with signature: group_concat(map<tinyint(4),tinyint(4)>, varchar)."
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_array_map_lambda_cast_param_resolves_lambda_scope() {
        let resolved = parse_raw_and_analyze(
            "SELECT array_map(x -> CAST(x AS STRING), array_generate(1, 3, 1))",
        )
        .expect("array_map lambda parameter should resolve in lambda scope");
        assert_eq!(resolved.output_columns.len(), 1);
        match &resolved.output_columns[0].data_type {
            arrow::datatypes::DataType::List(item) => {
                assert!(matches!(item.data_type(), arrow::datatypes::DataType::Utf8));
            }
            other => panic!("expected ARRAY<VARCHAR>, got {other:?}"),
        }
    }

    #[test]
    fn test_array_map_multi_param_lambda_analyzes() {
        let resolved = parse_raw_and_analyze("SELECT array_map((x, y) -> x + y, [1,2], [3,4])")
            .expect("array_map should analyze multi-parameter lambda");
        assert_eq!(resolved.output_columns.len(), 1);
        match &resolved.output_columns[0].data_type {
            arrow::datatypes::DataType::List(item) => {
                // StarRocks narrows array literal element types to the
                // smallest signed integer width (TINYINT for `[1, 2]`),
                // so `x + y` widens to SMALLINT under the arithmetic
                // promotion rules.
                assert!(
                    matches!(
                        item.data_type(),
                        arrow::datatypes::DataType::Int8
                            | arrow::datatypes::DataType::Int16
                            | arrow::datatypes::DataType::Int32
                            | arrow::datatypes::DataType::Int64
                    ),
                    "expected integer element type, got {:?}",
                    item.data_type()
                );
            }
            other => panic!("expected ARRAY<integer>, got {other:?}"),
        }
    }

    #[test]
    fn test_array_ordering_comparison_is_rejected() {
        let err = parse_raw_and_analyze("SELECT i_1 > [1, 2] FROM array_test")
            .expect_err("array ordering comparison should be rejected");
        assert!(
            err.contains("does not support binary predicate operation on ARRAY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn in_predicate_apply_rejects_scalar_vs_array_key() {
        let err = parse_raw_and_analyze(
            "SELECT 1 FROM array_test s WHERE 1 NOT IN (SELECT i_1 FROM array_test t)",
        )
        .expect_err("scalar-vs-array IN subquery should be rejected before planning");
        assert!(
            err.contains("does not support binary predicate operation between"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_time_slice_interval_literal_analyzes() {
        let resolved = parse_raw_and_analyze(
            "SELECT time_slice('9999-12-31 23:59:59', interval 5 year, ceil)",
        )
        .expect("time_slice interval literal should analyze");
        assert_eq!(resolved.output_columns.len(), 1);
    }

    #[test]
    fn test_any_match_lambda_analyzes_as_boolean() {
        let resolved = parse_raw_and_analyze("SELECT any_match(x -> x < 10, [1,2])")
            .expect("any_match should analyze lambda predicate");
        assert_eq!(resolved.output_columns.len(), 1);
        assert!(matches!(
            resolved.output_columns[0].data_type,
            arrow::datatypes::DataType::Boolean
        ));
    }

    #[test]
    fn select_alias_is_visible_to_later_projection_expression() {
        let resolved = parse_raw_and_analyze("SELECT 1 AS l, l + 1")
            .expect("later projection item should resolve earlier alias");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        assert_eq!(sel.projection.len(), 2);
        let ExprKind::BinaryOp { left, .. } = &sel.projection[1].expr.kind else {
            panic!("expected binary op in second projection");
        };
        let ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(value)) = &left.kind else {
            panic!("expected alias to be substituted with original literal");
        };
        assert_eq!(*value, 1);
    }

    #[test]
    fn select_alias_is_visible_inside_lambda_body() {
        parse_raw_and_analyze("SELECT 'x' AS l, array_map(arg -> concat(arg, l), ['a'])")
            .expect("lambda body should resolve earlier select alias");
    }

    #[test]
    fn select_alias_without_as_is_visible_inside_lambda_body() {
        parse_raw_and_analyze(
            "SELECT cast(if (1 > rand(), '[]', '') as array<string>) l, \
             array_map(x -> concat(x, l), ['a'])",
        )
        .expect("lambda body should resolve earlier select alias without AS");
    }

    #[test]
    fn select_alias_inside_lambda_body_is_fully_substituted() {
        fn contains_unresolved_l(expr: &TypedExpr) -> bool {
            match &expr.kind {
                ExprKind::ColumnRef {
                    qualifier, column, ..
                } => qualifier.is_none() && column.eq_ignore_ascii_case("l"),
                ExprKind::BinaryOp { left, right, .. } => {
                    contains_unresolved_l(left) || contains_unresolved_l(right)
                }
                ExprKind::UnaryOp { expr, .. }
                | ExprKind::Cast { expr, .. }
                | ExprKind::Nested(expr)
                | ExprKind::IsNull { expr, .. }
                | ExprKind::IsTruthValue { expr, .. } => contains_unresolved_l(expr),
                ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                    args.iter().any(contains_unresolved_l)
                }
                ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
                    contains_unresolved_l(body)
                }
                ExprKind::Case {
                    operand,
                    when_then,
                    else_expr,
                } => {
                    operand
                        .as_ref()
                        .is_some_and(|expr| contains_unresolved_l(expr))
                        || when_then.iter().any(|(when, then)| {
                            contains_unresolved_l(when) || contains_unresolved_l(then)
                        })
                        || else_expr
                            .as_ref()
                            .is_some_and(|expr| contains_unresolved_l(expr))
                }
                _ => false,
            }
        }

        let resolved = parse_raw_and_analyze(
            "SELECT cast(if (1 > rand(), '[]', '') as array<string>) l, \
             array_map(x -> concat(x, l), ['a'])",
        )
        .expect("analysis should succeed");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        assert_eq!(sel.projection.len(), 2);
        assert!(
            !contains_unresolved_l(&sel.projection[1].expr),
            "projection still contains unresolved alias reference: {:?}",
            sel.projection[1].expr
        );
    }

    #[test]
    fn test_group_by_rollup() {
        // q5 pattern: GROUP BY ROLLUP(a, b)
        let sql = "SELECT o_orderstatus, o_orderpriority, count(*) as cnt \
                   FROM orders \
                   GROUP BY ROLLUP(o_orderstatus, o_orderpriority)";
        let resolved = parse_and_analyze(sql).expect("GROUP BY ROLLUP should work");
        assert!(!resolved.output_columns.is_empty());
        // ROLLUP(a, b) should produce a single Select with RepeatInfo containing
        // 3 levels: (a,b), (a), ()
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("ROLLUP should produce RepeatInfo");
            // 3 levels for 2 rollup dimensions
            assert_eq!(
                repeat.repeat_column_ref_list.len(),
                3,
                "ROLLUP(a,b) should have 3 levels"
            );
            // Level 0: both active
            assert_eq!(repeat.repeat_column_ref_list[0].len(), 2);
            // Level 1: only first active
            assert_eq!(repeat.repeat_column_ref_list[1].len(), 1);
            // Level 2: none active
            assert_eq!(repeat.repeat_column_ref_list[2].len(), 0);

            // Grouping IDs
            assert_eq!(repeat.grouping_ids[0], 0b00); // both active
            assert_eq!(repeat.grouping_ids[1], 0b01); // b NULLed (least-significant bit)
            assert_eq!(repeat.grouping_ids[2], 0b11); // both NULLed

            // All rollup columns
            assert_eq!(repeat.all_rollup_columns.len(), 2);
        } else {
            panic!(
                "ROLLUP should produce a Select with RepeatInfo, got: {:?}",
                std::mem::discriminant(&resolved.body)
            );
        }
    }

    #[test]
    fn test_group_by_rollup_output_columns() {
        // Verify ROLLUP preserves output column structure
        let sql = "SELECT o_orderstatus as status, count(*) as cnt \
                   FROM orders \
                   GROUP BY ROLLUP(o_orderstatus)";
        let resolved = parse_and_analyze(sql).expect("GROUP BY ROLLUP should work");
        assert_eq!(resolved.output_columns.len(), 2);
        assert_eq!(resolved.output_columns[0].name, "status");
        assert_eq!(resolved.output_columns[1].name, "cnt");
        // Also verify RepeatInfo is present
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("ROLLUP should produce RepeatInfo");
            assert_eq!(
                repeat.repeat_column_ref_list.len(),
                2,
                "ROLLUP(a) should have 2 levels"
            );
            assert_eq!(repeat.grouping_ids[0], 0b0); // active
            assert_eq!(repeat.grouping_ids[1], 0b1); // NULLed
        } else {
            panic!("expected Select body with RepeatInfo");
        }
    }

    #[test]
    fn test_group_by_rollup_with_grouping() {
        // Verify GROUPING() function calls are captured in RepeatInfo
        let sql = "SELECT o_orderstatus, grouping(o_orderstatus) as g_status, count(*) as cnt \
                   FROM orders \
                   GROUP BY ROLLUP(o_orderstatus)";
        let resolved = parse_and_analyze(sql).expect("GROUP BY ROLLUP with GROUPING should work");
        assert_eq!(resolved.output_columns.len(), 3);
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("ROLLUP should produce RepeatInfo");
            // GROUPING(o_orderstatus) should be recorded. The name is an internal
            // placeholder (__grouping_fn_N) rather than the user alias — the alias
            // mapping is handled later by the planner when constructing PlanRepeatNode.
            assert_eq!(repeat.grouping_fn_args.len(), 1);
            assert_eq!(repeat.grouping_fn_args[0].0, "__grouping_fn_0");
            assert_eq!(repeat.grouping_fn_args[0].1, vec!["o_orderstatus"]);
        } else {
            panic!("expected Select body with RepeatInfo");
        }
    }

    fn grouping_fn_ids(
        resolved: &ResolvedQuery,
    ) -> (
        crate::sql::column_id::ColumnId,
        crate::sql::column_id::ColumnId,
    ) {
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };

        let proj_id = sel
            .projection
            .iter()
            .find_map(|item| match &item.expr.kind {
                ExprKind::ColumnRef {
                    column_id, column, ..
                } if column == "__grouping_fn_0" => Some(*column_id),
                _ => None,
            })
            .expect("expected projection grouping ColumnRef");
        let gb_id = sel
            .group_by
            .iter()
            .find_map(|expr| match &expr.kind {
                ExprKind::ColumnRef {
                    column_id, column, ..
                } if column == "__grouping_fn_0" => Some(*column_id),
                _ => None,
            })
            .expect("expected group-by grouping ColumnRef");

        (proj_id, gb_id)
    }

    fn grouping_fn_group_by_id(
        sel: &ResolvedSelect,
        name: &str,
    ) -> crate::sql::column_id::ColumnId {
        sel.group_by
            .iter()
            .find_map(|expr| match &expr.kind {
                ExprKind::ColumnRef {
                    column_id, column, ..
                } if column == name => Some(*column_id),
                _ => None,
            })
            .expect("expected group-by grouping ColumnRef")
    }

    fn find_grouping_ref_id(
        expr: &TypedExpr,
        name: &str,
    ) -> Option<crate::sql::column_id::ColumnId> {
        match &expr.kind {
            ExprKind::ColumnRef {
                column_id, column, ..
            } if column == name => Some(*column_id),
            ExprKind::BinaryOp { left, right, .. } => {
                find_grouping_ref_id(left, name).or_else(|| find_grouping_ref_id(right, name))
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr)
            | ExprKind::LambdaFunction { body: expr, .. }
            | ExprKind::Lambda { body: expr, .. } => find_grouping_ref_id(expr, name),
            ExprKind::FunctionCall { args, .. } => {
                args.iter().find_map(|arg| find_grouping_ref_id(arg, name))
            }
            ExprKind::AggregateCall { args, order_by, .. } => args
                .iter()
                .find_map(|arg| find_grouping_ref_id(arg, name))
                .or_else(|| {
                    order_by
                        .iter()
                        .find_map(|item| find_grouping_ref_id(&item.expr, name))
                }),
            ExprKind::InList { expr, list, .. } => find_grouping_ref_id(expr, name).or_else(|| {
                list.iter()
                    .find_map(|item| find_grouping_ref_id(item, name))
            }),
            ExprKind::Between {
                expr, low, high, ..
            } => find_grouping_ref_id(expr, name)
                .or_else(|| find_grouping_ref_id(low, name))
                .or_else(|| find_grouping_ref_id(high, name)),
            ExprKind::Like { expr, pattern, .. } => {
                find_grouping_ref_id(expr, name).or_else(|| find_grouping_ref_id(pattern, name))
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => operand
                .as_ref()
                .and_then(|expr| find_grouping_ref_id(expr, name))
                .or_else(|| {
                    when_then.iter().find_map(|(when, then)| {
                        find_grouping_ref_id(when, name)
                            .or_else(|| find_grouping_ref_id(then, name))
                    })
                })
                .or_else(|| {
                    else_expr
                        .as_ref()
                        .and_then(|expr| find_grouping_ref_id(expr, name))
                }),
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => args
                .iter()
                .find_map(|arg| find_grouping_ref_id(arg, name))
                .or_else(|| {
                    partition_by
                        .iter()
                        .find_map(|expr| find_grouping_ref_id(expr, name))
                })
                .or_else(|| {
                    order_by
                        .iter()
                        .find_map(|item| find_grouping_ref_id(&item.expr, name))
                }),
            ExprKind::ColumnRef { .. }
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::SubqueryPlaceholder { .. } => None,
        }
    }

    fn expr_contains_column_id(expr: &TypedExpr, target: crate::sql::column_id::ColumnId) -> bool {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id == target,
            ExprKind::BinaryOp { left, right, .. } => {
                expr_contains_column_id(left, target) || expr_contains_column_id(right, target)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr)
            | ExprKind::LambdaFunction { body: expr, .. }
            | ExprKind::Lambda { body: expr, .. } => expr_contains_column_id(expr, target),
            ExprKind::FunctionCall { args, .. } => {
                args.iter().any(|arg| expr_contains_column_id(arg, target))
            }
            ExprKind::AggregateCall { args, order_by, .. } => {
                args.iter().any(|arg| expr_contains_column_id(arg, target))
                    || order_by
                        .iter()
                        .any(|item| expr_contains_column_id(&item.expr, target))
            }
            ExprKind::InList { expr, list, .. } => {
                expr_contains_column_id(expr, target)
                    || list
                        .iter()
                        .any(|item| expr_contains_column_id(item, target))
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                expr_contains_column_id(expr, target)
                    || expr_contains_column_id(low, target)
                    || expr_contains_column_id(high, target)
            }
            ExprKind::Like { expr, pattern, .. } => {
                expr_contains_column_id(expr, target) || expr_contains_column_id(pattern, target)
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| expr_contains_column_id(expr, target))
                    || when_then.iter().any(|(when, then)| {
                        expr_contains_column_id(when, target)
                            || expr_contains_column_id(then, target)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|expr| expr_contains_column_id(expr, target))
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                args.iter().any(|arg| expr_contains_column_id(arg, target))
                    || partition_by
                        .iter()
                        .any(|expr| expr_contains_column_id(expr, target))
                    || order_by
                        .iter()
                        .any(|item| expr_contains_column_id(&item.expr, target))
            }
            ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::SubqueryPlaceholder { .. } => false,
        }
    }

    fn contains_grouping_marker_literal(expr: &TypedExpr) -> bool {
        match &expr.kind {
            ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(v)) if *v <= -9000 => true,
            ExprKind::BinaryOp { left, right, .. } => {
                contains_grouping_marker_literal(left) || contains_grouping_marker_literal(right)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr)
            | ExprKind::LambdaFunction { body: expr, .. }
            | ExprKind::Lambda { body: expr, .. } => contains_grouping_marker_literal(expr),
            ExprKind::FunctionCall { args, .. } => {
                args.iter().any(contains_grouping_marker_literal)
            }
            ExprKind::AggregateCall { args, order_by, .. } => {
                args.iter().any(contains_grouping_marker_literal)
                    || order_by
                        .iter()
                        .any(|item| contains_grouping_marker_literal(&item.expr))
            }
            ExprKind::InList { expr, list, .. } => {
                contains_grouping_marker_literal(expr)
                    || list.iter().any(contains_grouping_marker_literal)
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                contains_grouping_marker_literal(expr)
                    || contains_grouping_marker_literal(low)
                    || contains_grouping_marker_literal(high)
            }
            ExprKind::Like { expr, pattern, .. } => {
                contains_grouping_marker_literal(expr) || contains_grouping_marker_literal(pattern)
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| contains_grouping_marker_literal(expr))
                    || when_then.iter().any(|(when, then)| {
                        contains_grouping_marker_literal(when)
                            || contains_grouping_marker_literal(then)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|expr| contains_grouping_marker_literal(expr))
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                args.iter().any(contains_grouping_marker_literal)
                    || partition_by.iter().any(contains_grouping_marker_literal)
                    || order_by
                        .iter()
                        .any(|item| contains_grouping_marker_literal(&item.expr))
            }
            ExprKind::ColumnRef { .. }
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::SubqueryPlaceholder { .. } => false,
        }
    }

    fn relation_contains_grouping_marker_literal(rel: &Relation) -> bool {
        match rel {
            Relation::Join(join) => {
                join.condition
                    .as_ref()
                    .is_some_and(contains_grouping_marker_literal)
                    || relation_contains_grouping_marker_literal(&join.left)
                    || relation_contains_grouping_marker_literal(&join.right)
            }
            Relation::Unnest(unnest) => unnest.args.iter().any(contains_grouping_marker_literal),
            Relation::Scan(_)
            | Relation::IcebergMetadataScan(_)
            | Relation::IcebergDeltaScan(_)
            | Relation::Subquery { .. }
            | Relation::GenerateSeries(_)
            | Relation::CTEConsume { .. } => false,
        }
    }

    fn outer_select_contains_grouping_marker_literal(sel: &ResolvedSelect) -> bool {
        sel.projection
            .iter()
            .any(|item| contains_grouping_marker_literal(&item.expr))
            || sel
                .filter
                .as_ref()
                .is_some_and(contains_grouping_marker_literal)
            || sel
                .having
                .as_ref()
                .is_some_and(contains_grouping_marker_literal)
            || sel
                .from
                .as_ref()
                .is_some_and(relation_contains_grouping_marker_literal)
    }

    #[test]
    fn p1_grouping_marker_carries_group_by_id() {
        let resolved =
            parse_and_analyze("SELECT k1, grouping(k1) AS g FROM t1 GROUP BY ROLLUP(k1)")
                .expect("GROUP BY ROLLUP with GROUPING should work");
        let (proj_id, gb_id) = grouping_fn_ids(&resolved);
        assert_ne!(proj_id, crate::sql::column_id::ColumnId::UNSET);
        assert_eq!(
            proj_id, gb_id,
            "projection grouping ref must reuse the group-by key id"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_in_having() {
        let resolved = parse_and_analyze(
            "SELECT k1, count(*) AS c FROM t1 GROUP BY ROLLUP(k1) HAVING grouping(k1) = 0",
        )
        .expect("GROUP BY ROLLUP with GROUPING in HAVING should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let having = sel.having.as_ref().expect("expected HAVING expression");
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let having_id = find_grouping_ref_id(having, "__grouping_fn_0")
            .expect("expected HAVING grouping ColumnRef");
        assert_eq!(
            having_id, gb_id,
            "HAVING grouping ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(having),
            "HAVING must not retain grouping marker literal: {having:?}"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_inside_nested_projection() {
        let resolved =
            parse_and_analyze("SELECT abs(grouping(k1)) AS g FROM t1 GROUP BY ROLLUP(k1)")
                .expect("nested GROUPING projection should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let projection = &sel.projection[0].expr;
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let proj_id = find_grouping_ref_id(projection, "__grouping_fn_0")
            .expect("expected nested projection grouping ColumnRef");
        assert_eq!(
            proj_id, gb_id,
            "nested projection grouping ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(projection),
            "nested projection must not retain grouping marker literal: {projection:?}"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_inside_cast_projection() {
        let resolved = parse_and_analyze(
            "SELECT CAST(grouping(k1) AS BIGINT) AS g FROM t1 GROUP BY ROLLUP(k1)",
        )
        .expect("CAST around GROUPING projection should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let projection = &sel.projection[0].expr;
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let proj_id = find_grouping_ref_id(projection, "__grouping_fn_0")
            .expect("expected cast projection grouping ColumnRef");
        assert_eq!(
            proj_id, gb_id,
            "cast projection grouping ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(projection),
            "cast projection must not retain grouping marker literal: {projection:?}"
        );
    }

    #[test]
    fn p1_grouping_id_marker_replaced_inside_nested_projection() {
        let resolved = parse_and_analyze(
            "SELECT abs(grouping_id(k1, k2)) AS gid \
             FROM t1 \
             GROUP BY GROUPING SETS ((k1, k2), ())",
        )
        .expect("nested GROUPING_ID projection should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let projection = &sel.projection[0].expr;
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let proj_id = find_grouping_ref_id(projection, "__grouping_fn_0")
            .expect("expected nested projection grouping_id ColumnRef");
        assert_eq!(
            proj_id, gb_id,
            "nested projection grouping_id ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(projection),
            "nested grouping_id projection must not retain grouping marker literal: {projection:?}"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_in_having_in_list() {
        let resolved =
            parse_and_analyze("SELECT k1 FROM t1 GROUP BY ROLLUP(k1) HAVING grouping(k1) IN (0)")
                .expect("GROUPING in HAVING IN list should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let having = sel.having.as_ref().expect("expected HAVING expression");
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let having_id = find_grouping_ref_id(having, "__grouping_fn_0")
            .expect("expected HAVING IN grouping ColumnRef");
        assert_eq!(
            having_id, gb_id,
            "HAVING IN grouping ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(having),
            "HAVING IN must not retain grouping marker literal: {having:?}"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_in_aggregate_order_by() {
        let resolved = parse_and_analyze(
            "SELECT sum(v1 ORDER BY grouping(k1)) AS g \
             FROM t1 GROUP BY ROLLUP(k1)",
        )
        .expect("GROUPING in aggregate ORDER BY should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let projection = &sel.projection[0].expr;
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let proj_id = find_grouping_ref_id(projection, "__grouping_fn_0")
            .expect("expected aggregate ORDER BY grouping ColumnRef");
        assert_eq!(
            proj_id, gb_id,
            "aggregate ORDER BY grouping ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(projection),
            "aggregate ORDER BY must not retain grouping marker literal: {projection:?}"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_in_having_in_subquery_join_condition() {
        let resolved = parse_and_analyze(
            "SELECT k1 FROM t1 \
             GROUP BY ROLLUP(k1) \
             HAVING grouping(k1) IN (SELECT k2 FROM t2)",
        )
        .expect("GROUPING in HAVING IN subquery should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let spec = only_predicate_spec(sel);
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let lhs = spec
            .in_lhs
            .as_ref()
            .expect("HAVING IN Apply spec should retain analyzed LHS");
        assert!(
            expr_contains_column_id(lhs, gb_id),
            "Apply IN LHS grouping ref must reuse the group-by key id"
        );
        assert!(
            !outer_select_contains_grouping_marker_literal(sel),
            "outer select must not retain grouping marker literal: {sel:?}"
        );
    }

    #[test]
    fn p1_synthetic_grouping_does_not_replace_real_marker_literal() {
        let resolved = parse_and_analyze("SELECT -9000 AS marker, k1 FROM t1 GROUP BY ROLLUP(k1)")
            .expect("ROLLUP with real -9000 literal should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        assert!(
            sel.repeat
                .as_ref()
                .expect("ROLLUP should produce RepeatInfo")
                .grouping_fn_args
                .iter()
                .any(|(name, _)| name == "__grouping_fn_0"),
            "ROLLUP without explicit GROUPING should still synthesize grouping_fn"
        );
        assert!(
            find_grouping_ref_id(&sel.projection[0].expr, "__grouping_fn_0").is_none(),
            "real -9000 literal must not be rewritten to synthetic grouping ColumnRef"
        );
        match &sel.projection[0].expr.kind {
            ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(v)) => assert_eq!(*v, -9000),
            ExprKind::UnaryOp { expr, .. } => match &expr.kind {
                ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(v)) => {
                    assert_eq!(*v, 9000)
                }
                other => panic!("expected numeric literal under unary op, got {other:?}"),
            },
            other => panic!("expected real -9000 literal to remain literal, got {other:?}"),
        }
    }

    #[test]
    fn p1_typed_marker_replacement_ignores_synthetic_grouping_entry() {
        let expr = TypedExpr {
            kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(-9000)),
            data_type: DataType::Int64,
            nullable: false,
        };
        let grouping_fn_args = vec![("__grouping_fn_0".to_string(), vec!["k1".to_string()])];
        let grouping_fn_ids = vec![(
            "__grouping_fn_0".to_string(),
            crate::sql::column_id::ColumnId::new_for_test(99),
        )];

        let replaced =
            replace_grouping_markers_in_typed_expr(&expr, &grouping_fn_args, &grouping_fn_ids, 0);

        match replaced.kind {
            ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(v)) => assert_eq!(v, -9000),
            other => panic!("real -9000 literal must remain literal, got {other:?}"),
        }
    }

    #[test]
    fn test_group_by_grouping_sets_single_column_resolves() {
        // Reduced from sql-tests/aggregate/agg_grouping_sets_v1 query 5:
        //     GROUP BY GROUPING SETS((), (k1))
        // Failed in standalone server with "Column 'k1' cannot be resolved.".
        let sql = "SELECT o_orderstatus, count(*) AS cnt \
                   FROM orders \
                   GROUP BY GROUPING SETS((), (o_orderstatus))";
        let resolved =
            parse_and_analyze(sql).expect("GROUPING SETS with single-column set should analyze");
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("GROUPING SETS should produce RepeatInfo");
            assert_eq!(repeat.repeat_column_ref_list.len(), 2);
        } else {
            panic!("expected Select body with RepeatInfo");
        }
    }

    #[test]
    fn test_group_by_grouping_sets_single_column_via_starrocks_dialect() {
        // Same as the test above but uses the StarRocks dialect via
        // parse_and_analyze_with_registry — the production parse path.
        let sql = "SELECT o_orderstatus, count(*) AS cnt \
                   FROM orders \
                   GROUP BY GROUPING SETS((), (o_orderstatus))";
        let (resolved, _) = parse_and_analyze_with_registry(sql)
            .expect("GROUPING SETS via StarRocks dialect should analyze");
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("GROUPING SETS should produce RepeatInfo");
            assert_eq!(repeat.repeat_column_ref_list.len(), 2);
        } else {
            panic!("expected Select body with RepeatInfo");
        }
    }

    #[test]
    fn test_group_by_grouping_sets_with_grouping_id() {
        let sql = "SELECT o_orderstatus, o_orderpriority, \
                          grouping_id(o_orderstatus, o_orderpriority) as gid, \
                          grouping(o_orderstatus, o_orderpriority) as grp, \
                          count(*) as cnt \
                   FROM orders \
                   GROUP BY GROUPING SETS ((), (o_orderstatus, o_orderpriority))";
        let resolved = parse_and_analyze(sql).expect("GROUPING SETS should work");
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("GROUPING SETS should produce RepeatInfo");
            assert_eq!(repeat.repeat_column_ref_list.len(), 2);
            assert_eq!(repeat.grouping_ids, vec![0b11, 0b00]);
            assert_eq!(
                repeat.all_rollup_columns,
                vec!["o_orderstatus".to_string(), "o_orderpriority".to_string()]
            );
            assert_eq!(repeat.grouping_fn_args.len(), 2);
            assert_eq!(
                repeat.grouping_fn_args[0].1,
                vec!["o_orderstatus".to_string(), "o_orderpriority".to_string()]
            );
            assert_eq!(
                repeat.grouping_fn_args[1].1,
                vec!["o_orderstatus".to_string(), "o_orderpriority".to_string()]
            );
        } else {
            panic!("expected Select body with RepeatInfo");
        }
    }

    #[test]
    fn array_agg_without_arguments_reports_starrocks_error() {
        let err = parse_raw_and_analyze("select array_agg()")
            .expect_err("array_agg() should fail during analysis");
        assert_eq!(err, "array_agg should have at least one input.");
    }

    #[test]
    fn array_agg_with_multiple_arguments_keeps_order_syntax_error() {
        let err = parse_raw_and_analyze("select array_agg(1, 2 order by 1)")
            .expect_err("array_agg with multiple arguments should fail");
        assert_eq!(
            err,
            "Unexpected input 'order', the most similar input is {',', ')'}."
        );
    }

    #[test]
    fn bool_or_window_coerces_non_boolean_arguments() {
        let resolved = parse_and_analyze(
            "select bool_or(o_orderkey) over (partition by o_custkey order by o_orderkey) as v \
             from orders",
        )
        .expect("bool_or window should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let expr = &sel.projection[0].expr;
        assert_eq!(expr.data_type, arrow::datatypes::DataType::Boolean);
        let ExprKind::WindowCall { name, args, .. } = &expr.kind else {
            panic!("expected WindowCall, got {:?}", expr.kind);
        };
        assert_eq!(name, "bool_or");
        assert_eq!(args.len(), 1);
        let ExprKind::Cast { target, .. } = &args[0].kind else {
            panic!(
                "expected bool_or window argument cast, got {:?}",
                args[0].kind
            );
        };
        assert_eq!(target, &arrow::datatypes::DataType::Boolean);
        assert_eq!(args[0].data_type, arrow::datatypes::DataType::Boolean);
    }

    #[test]
    fn sum_avg_cast_varchar_arguments_to_double_in_analyzer_ir() {
        let resolved =
            parse_and_analyze("select sum(o_orderstatus), avg(o_orderstatus) from orders")
                .expect("string aggregate arguments should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };

        for expr in [&sel.projection[0].expr, &sel.projection[1].expr] {
            assert_eq!(expr.data_type, arrow::datatypes::DataType::Float64);
            let ExprKind::AggregateCall { name, args, .. } = &expr.kind else {
                panic!("expected AggregateCall, got {:?}", expr.kind);
            };
            assert!(matches!(name.as_str(), "sum" | "avg"));
            assert_eq!(args.len(), 1);
            let ExprKind::Cast {
                target,
                expr: inner,
            } = &args[0].kind
            else {
                panic!("expected aggregate argument cast, got {:?}", args[0].kind);
            };
            assert_eq!(target, &arrow::datatypes::DataType::Float64);
            assert_eq!(args[0].data_type, arrow::datatypes::DataType::Float64);
            assert_eq!(inner.data_type, arrow::datatypes::DataType::Utf8);
        }
    }

    #[test]
    fn length_casts_numeric_argument_to_varchar() {
        let resolved = parse_and_analyze("select length(o_orderkey) from orders")
            .expect("length should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let ExprKind::FunctionCall { name, args, .. } = &sel.projection[0].expr.kind else {
            panic!(
                "expected FunctionCall, got {:?}",
                sel.projection[0].expr.kind
            );
        };
        assert_eq!(name, "length");
        let ExprKind::Cast { target, expr } = &args[0].kind else {
            panic!("expected length argument cast, got {:?}", args[0].kind);
        };
        assert_eq!(target, &arrow::datatypes::DataType::Utf8);
        assert_eq!(args[0].data_type, arrow::datatypes::DataType::Utf8);
        assert_eq!(expr.data_type, arrow::datatypes::DataType::Int64);
    }

    #[test]
    fn left_casts_value_argument_but_preserves_length_type() {
        let resolved = parse_and_analyze("select left(o_totalprice, 6) from orders")
            .expect("left should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let ExprKind::FunctionCall { name, args, .. } = &sel.projection[0].expr.kind else {
            panic!(
                "expected FunctionCall, got {:?}",
                sel.projection[0].expr.kind
            );
        };
        assert_eq!(name, "left");
        let ExprKind::Cast { target, expr } = &args[0].kind else {
            panic!("expected left value argument cast, got {:?}", args[0].kind);
        };
        assert_eq!(target, &arrow::datatypes::DataType::Utf8);
        assert_eq!(expr.data_type, arrow::datatypes::DataType::Float64);
        assert_eq!(args[1].data_type, arrow::datatypes::DataType::Int64);
    }

    #[test]
    fn date_trunc_return_type_comes_from_value_argument() {
        let resolved = parse_and_analyze(
            "select date_trunc('week', o_orderstatus), date_trunc('week', o_orderdate) from orders",
        )
        .expect("date_trunc should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        assert_eq!(
            sel.projection[0].expr.data_type,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            sel.projection[1].expr.data_type,
            arrow::datatypes::DataType::Date32
        );
    }

    #[test]
    fn date_trunc_casts_numeric_value_argument_to_datetime() {
        let resolved = parse_and_analyze("select date_trunc('week', o_orderkey) from orders")
            .expect("date_trunc should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let ExprKind::FunctionCall { args, .. } = &sel.projection[0].expr.kind else {
            panic!(
                "expected FunctionCall, got {:?}",
                sel.projection[0].expr.kind
            );
        };
        let ExprKind::Cast { target, expr } = &args[1].kind else {
            panic!(
                "expected date_trunc value argument cast, got {:?}",
                args[1].kind
            );
        };
        assert_eq!(
            target,
            &arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        assert_eq!(expr.data_type, arrow::datatypes::DataType::Int64);
    }

    #[test]
    fn split_returns_varchar_array_and_casts_arguments() {
        let resolved = parse_and_analyze("select split(o_orderkey, 1) from orders")
            .expect("split should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let arrow::datatypes::DataType::List(item) = &sel.projection[0].expr.data_type else {
            panic!("expected ARRAY return type");
        };
        assert_eq!(item.data_type(), &arrow::datatypes::DataType::Utf8);
        let ExprKind::FunctionCall { args, .. } = &sel.projection[0].expr.kind else {
            panic!(
                "expected FunctionCall, got {:?}",
                sel.projection[0].expr.kind
            );
        };
        assert!(matches!(args[0].kind, ExprKind::Cast { .. }));
        assert!(matches!(args[1].kind, ExprKind::Cast { .. }));
        assert_eq!(args[0].data_type, arrow::datatypes::DataType::Utf8);
        assert_eq!(args[1].data_type, arrow::datatypes::DataType::Utf8);
    }

    #[test]
    fn ceil_expression_analyzes_as_scalar_function() {
        let resolved = parse_and_analyze("select ceil(sum(o_totalprice)) from orders")
            .expect("ceil should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let ExprKind::FunctionCall { name, args, .. } = &sel.projection[0].expr.kind else {
            panic!(
                "expected FunctionCall, got {:?}",
                sel.projection[0].expr.kind
            );
        };
        assert_eq!(name, "ceil");
        assert_eq!(args.len(), 1);
        assert!(matches!(args[0].kind, ExprKind::AggregateCall { .. }));
    }

    struct CatalogAwareTestCatalog;

    impl CatalogAwareTestCatalog {
        fn starrocks_table_def(catalog: Option<&str>, database: &str, table: &str) -> TableDef {
            let catalog_name = catalog.unwrap_or("default_catalog");
            TableDef {
                name: format!("{catalog_name}_{database}_{table}"),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: if catalog == Some("ice") { 100 } else { 1 },
                    table_id: 2,
                },
            }
        }

        fn iceberg_table_def(catalog: Option<&str>, database: &str, table: &str) -> TableDef {
            let mut table_def = Self::starrocks_table_def(catalog, database, table);
            table_def.source = ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info_for(
                    catalog.unwrap_or("default_catalog"),
                    database,
                    table,
                ),
                files: vec![],
                cloud_properties: Default::default(),
                binding:
                    crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
            };
            table_def
        }
    }

    impl crate::sql::catalog::PlannerTableProvider for CatalogAwareTestCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::sql::catalog::ResolvedAnalyzerTable, String> {
            Ok(crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                catalog,
                database,
                Self::starrocks_table_def(catalog, database, table),
            ))
        }

        fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
            Some(self)
        }
    }

    impl IcebergMetadataTableProvider for CatalogAwareTestCatalog {
        fn get_iceberg_metadata_table(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
            _metadata_table_type: IcebergMetadataTableType,
        ) -> Result<TableDef, String> {
            Ok(Self::iceberg_table_def(catalog, database, table))
        }
    }

    #[test]
    fn analyzer_passes_three_part_catalog_to_catalog_provider() {
        let stmt =
            crate::sql::parser::parse_sql_raw("SELECT id FROM ice.db.orders").expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };

        let (resolved, _, _) =
            analyze(&query, &CatalogAwareTestCatalog, "default").expect("analyze");

        let QueryBody::Select(select) = resolved.body else {
            panic!("expected select");
        };
        let Some(Relation::Scan(scan)) = select.from else {
            panic!("expected scan");
        };
        assert_eq!(scan.database, "db");
        assert_eq!(scan.table.name, "ice_db_orders");
    }

    #[test]
    fn analyzer_passes_uppercase_three_part_catalog_to_catalog_provider() {
        let stmt =
            crate::sql::parser::parse_sql_raw("SELECT id FROM ICE.DB.ORDERS").expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };

        let (resolved, _, _) =
            analyze(&query, &CatalogAwareTestCatalog, "default").expect("analyze");

        let QueryBody::Select(select) = resolved.body else {
            panic!("expected select");
        };
        let Some(Relation::Scan(scan)) = select.from else {
            panic!("expected scan");
        };
        assert_eq!(scan.database, "db");
        assert_eq!(scan.table.name, "ice_db_orders");
    }

    #[test]
    fn analyzer_uses_metadata_provider_for_partitions_table() {
        struct MetadataModeCatalog(std::cell::Cell<bool>);
        impl crate::sql::catalog::PlannerTableProvider for MetadataModeCatalog {
            fn resolve_table_for_analysis(
                &self,
                catalog: Option<&str>,
                database: &str,
                table: &str,
            ) -> Result<crate::sql::catalog::ResolvedAnalyzerTable, String> {
                CatalogAwareTestCatalog.resolve_table_for_analysis(catalog, database, table)
            }

            fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
                Some(self)
            }
        }

        impl IcebergMetadataTableProvider for MetadataModeCatalog {
            fn get_iceberg_metadata_table(
                &self,
                catalog: Option<&str>,
                database: &str,
                table: &str,
                metadata_table_type: IcebergMetadataTableType,
            ) -> Result<TableDef, String> {
                self.0
                    .set(metadata_table_type == IcebergMetadataTableType::Partitions);
                CatalogAwareTestCatalog.get_iceberg_metadata_table(
                    catalog,
                    database,
                    table,
                    metadata_table_type,
                )
            }
        }

        let catalog = MetadataModeCatalog(std::cell::Cell::new(false));
        let stmt =
            crate::sql::parser::parse_sql_raw("SELECT record_count FROM ice.db.orders$partitions")
                .expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };

        let _ = analyze(&query, &catalog, "default").expect("analyze");
        assert!(
            catalog.0.get(),
            "partitions metadata provider was not requested"
        );
    }

    #[test]
    fn analyzer_uses_lowercase_catalog_for_metadata_provider() {
        struct LowercaseMetadataModeCatalog(std::cell::Cell<bool>);
        impl crate::sql::catalog::PlannerTableProvider for LowercaseMetadataModeCatalog {
            fn resolve_table_for_analysis(
                &self,
                catalog: Option<&str>,
                database: &str,
                table: &str,
            ) -> Result<crate::sql::catalog::ResolvedAnalyzerTable, String> {
                CatalogAwareTestCatalog.resolve_table_for_analysis(catalog, database, table)
            }

            fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
                Some(self)
            }
        }

        impl IcebergMetadataTableProvider for LowercaseMetadataModeCatalog {
            fn get_iceberg_metadata_table(
                &self,
                catalog: Option<&str>,
                database: &str,
                table: &str,
                metadata_table_type: IcebergMetadataTableType,
            ) -> Result<TableDef, String> {
                assert_eq!(catalog, Some("ice"));
                assert_eq!(database, "db");
                assert_eq!(table, "orders");
                self.0
                    .set(metadata_table_type == IcebergMetadataTableType::Partitions);
                CatalogAwareTestCatalog.get_iceberg_metadata_table(
                    catalog,
                    database,
                    table,
                    metadata_table_type,
                )
            }
        }

        let catalog = LowercaseMetadataModeCatalog(std::cell::Cell::new(false));
        let stmt =
            crate::sql::parser::parse_sql_raw("SELECT record_count FROM ICE.DB.ORDERS$partitions")
                .expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };

        let _ = analyze(&query, &catalog, "default").expect("analyze");
        assert!(
            catalog.0.get(),
            "partitions metadata provider was not requested"
        );
    }

    #[test]
    fn analyzer_resolves_t_dollar_snapshots_to_metadata_scan() {
        use crate::connector::iceberg::IcebergMetadataTableType;

        // The parser rewrites `orders$snapshots` -> `orders.__nr_meta_snapshots__`
        // so we go through `parse_raw_and_analyze` to exercise the full pipeline.
        let resolved = parse_raw_and_analyze("SELECT snapshot_id FROM orders$snapshots")
            .expect("analyze should succeed");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let from = sel.from.as_ref().expect("FROM clause should be present");
        match from {
            Relation::IcebergMetadataScan(rel) => {
                assert_eq!(rel.metadata_table_type, IcebergMetadataTableType::Snapshots);
                assert_eq!(rel.table.name, "orders");
                assert_eq!(rel.database, "default");
                assert!(rel.alias.is_none());
            }
            other => panic!("expected IcebergMetadataScan, got {other:?}"),
        }

        // The output column `snapshot_id` should resolve through the synthetic
        // metadata schema (Int64, NOT NULL).
        assert_eq!(resolved.output_columns.len(), 1);
        let col = &resolved.output_columns[0];
        assert_eq!(col.name, "snapshot_id");
        assert_eq!(col.data_type, arrow::datatypes::DataType::Int64);
        assert!(!col.nullable);
    }

    #[test]
    fn analyzer_rejects_branch_combined_with_metadata_suffix() {
        // `orders.branch_dev$snapshots` -> `orders.branch_dev.__nr_meta_snapshots__`
        // after parser rewrite. The base_parts ends in `branch_dev`, which is
        // illegal in combination with a metadata-table suffix.
        let err = parse_raw_and_analyze("SELECT * FROM orders.branch_dev$snapshots")
            .expect_err("must fail");
        assert!(
            err.contains("cannot be combined with branch/tag suffix"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn analyzer_recognizes_nr_ivm_delta_table_function() {
        // The TestCatalog's `iv_orders` table carries the v3 row-lineage
        // metadata columns required by __nr_ivm_delta. Note: the TestCatalog
        // returns the same table for any database name, so we can pass any
        // catalog/namespace strings in the three-part identifier.
        let resolved = parse_raw_and_analyze(
            "SELECT _row_id FROM __nr_ivm_delta('cat.ns.iv_orders', 100, 200) AS t",
        )
        .expect("analysis should succeed");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let from = sel.from.as_ref().expect("FROM should be present");
        match from {
            Relation::IcebergDeltaScan(rel) => {
                assert_eq!(rel.catalog, "cat");
                assert_eq!(rel.namespace, "ns");
                assert_eq!(rel.table_name, "iv_orders");
                assert_eq!(rel.from_snapshot_id, 100);
                assert_eq!(rel.to_snapshot_id, 200);
                assert_eq!(rel.alias.as_deref(), Some("t"));
                assert!(
                    rel.table
                        .iceberg_row_lineage_metadata_columns
                        .iter()
                        .any(|c| c.name == "_row_id"),
                    "expected _row_id in row-lineage metadata columns"
                );
            }
            other => panic!("expected IcebergDeltaScan, got {other:?}"),
        }
        // Output column `_row_id` should resolve through the row-lineage
        // metadata columns the analyzer registered into the scope.
        assert_eq!(resolved.output_columns.len(), 1);
        assert_eq!(resolved.output_columns[0].name, "_row_id");
        assert_eq!(
            resolved.output_columns[0].data_type,
            arrow::datatypes::DataType::Int64
        );
    }

    #[test]
    fn analyzer_preserves_row_lineage_metadata_column_ids_for_base_scan() {
        let resolved = parse_raw_and_analyze("SELECT _row_id FROM iv_orders AS t")
            .expect("analysis should succeed");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let from = sel.from.as_ref().expect("FROM should be present");
        let Relation::Scan(scan) = from else {
            panic!("expected base Scan, got {from:?}");
        };

        let base_len = scan.table.columns.len();
        let meta_len = scan.table.iceberg_row_lineage_metadata_columns.len();
        assert_eq!(
            scan.column_ids.len(),
            base_len + meta_len,
            "base scan must carry base column ids followed by row-lineage metadata ids"
        );
        let row_id = scan.column_ids[base_len];
        assert_ne!(row_id, ColumnId::UNSET);

        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &sel.projection[0].expr.kind
        else {
            panic!("expected projection to resolve _row_id as ColumnRef");
        };
        assert_eq!(column, "_row_id");
        assert_eq!(
            *column_id, row_id,
            "projection must reference the metadata ColumnId from the base Scan relation"
        );
        assert_eq!(
            resolved.output_columns[0].column_id, row_id,
            "visible output must preserve the same metadata ColumnId"
        );
    }

    #[test]
    fn analyzer_rejects_nr_ivm_delta_with_negative_snapshot() {
        let err =
            parse_raw_and_analyze("SELECT * FROM __nr_ivm_delta('cat.ns.iv_orders', -1, 200) AS t")
                .expect_err("must fail");
        assert!(err.contains("non-negative"), "unexpected error: {err}");
    }

    #[test]
    fn analyzer_rejects_nr_ivm_delta_on_non_v3_table() {
        // `orders` is registered without row-lineage metadata columns.
        let err =
            parse_raw_and_analyze("SELECT * FROM __nr_ivm_delta('cat.ns.orders', 100, 200) AS t")
                .expect_err("must fail");
        assert!(
            err.contains("write.row-lineage") || err.contains("row-lineage metadata"),
            "expected row-lineage rebuild diagnostic, got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // output_column_id on ProjectItem
    // -----------------------------------------------------------------------

    /// A computed SELECT item (`a + b AS c`) must receive a non-UNSET
    /// `output_column_id` that matches the corresponding entry in
    /// `output_columns`.  A passthrough ColumnRef item reuses the
    /// source column's id.
    #[test]
    fn computed_project_item_gets_non_unset_output_column_id() {
        // `orders` has o_orderkey (Int64) and o_custkey (Int64).
        let sql = "SELECT o_orderkey + o_custkey AS sum_keys, o_orderkey FROM orders";
        let resolved = parse_raw_and_analyze(sql).expect("analysis should succeed");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected Select body");
        };

        assert_eq!(select.projection.len(), 2, "expected two projection items");
        assert_eq!(
            resolved.output_columns.len(),
            2,
            "expected two output columns"
        );

        // First item: computed expression (o_orderkey + o_custkey).
        let computed_item = &select.projection[0];
        let computed_out_col = &resolved.output_columns[0];
        assert_ne!(
            computed_item.output_column_id,
            crate::sql::column_id::ColumnId::UNSET,
            "computed item must not have UNSET output_column_id"
        );
        assert_eq!(
            computed_item.output_column_id, computed_out_col.column_id,
            "computed item output_column_id must match the output_columns entry"
        );

        // Second item: passthrough ColumnRef (o_orderkey).
        let passthrough_item = &select.projection[1];
        let passthrough_out_col = &resolved.output_columns[1];
        assert_ne!(
            passthrough_item.output_column_id,
            crate::sql::column_id::ColumnId::UNSET,
            "passthrough item must not have UNSET output_column_id"
        );
        assert_eq!(
            passthrough_item.output_column_id, passthrough_out_col.column_id,
            "passthrough item output_column_id must match the output_columns entry"
        );

        // The two items must have distinct ids.
        assert_ne!(
            computed_item.output_column_id, passthrough_item.output_column_id,
            "each projection item must receive a distinct output_column_id"
        );
    }

    #[test]
    fn p2_using_reference_keeps_analyzer_selected_id() {
        let resolved =
            parse_and_analyze("SELECT k1 FROM t1 JOIN t2 USING(k1)").expect("analysis succeeds");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected Select body");
        };
        let item = &select.projection[0];
        let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } = &item.expr.kind
        else {
            panic!(
                "expected USING projection to resolve as ColumnRef, got {:?}",
                item.expr.kind
            );
        };
        assert_eq!(column, "k1");
        assert!(
            qualifier.is_none(),
            "USING projection must not rely on canonical qualifier steering: {:?}",
            item.expr.kind
        );
        assert_ne!(
            *column_id,
            crate::sql::column_id::ColumnId::UNSET,
            "USING projection must carry a real ColumnId"
        );
        assert_eq!(
            *column_id, item.output_column_id,
            "USING projection output must reuse the analyzer-selected source ColumnId"
        );
        assert_eq!(
            *column_id, resolved.output_columns[0].column_id,
            "query output must expose the same USING source ColumnId"
        );
    }

    #[test]
    fn p2_full_outer_using_coalesce_has_project_output_id() {
        let resolved = parse_and_analyze("SELECT k1 FROM t1 FULL OUTER JOIN t2 USING(k1)")
            .expect("analysis succeeds");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected Select body");
        };
        let merged_item = &select.projection[0];
        assert_ne!(
            merged_item.output_column_id,
            crate::sql::column_id::ColumnId::UNSET,
            "FULL OUTER USING merged projection must have a real output ColumnId"
        );
        assert_eq!(
            merged_item.output_column_id, resolved.output_columns[0].column_id,
            "FULL OUTER USING merged query output must expose the project output id"
        );
        let ExprKind::FunctionCall { name, args, .. } = &merged_item.expr.kind else {
            panic!(
                "expected FULL OUTER USING merged column to be COALESCE, got {:?}",
                merged_item.expr.kind
            );
        };
        assert_eq!(name, "coalesce");
        assert_eq!(args.len(), 2);
        assert!(
            args.iter()
                .all(|arg| matches!(arg.kind, ExprKind::ColumnRef { .. })),
            "FULL OUTER USING merged expression should coalesce the two side ColumnRefs"
        );
    }

    #[test]
    fn analyze_with_factory_threads_column_ids() {
        // Pre-seed the factory with 3 ids so threaded analysis must start at 4.
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        for i in 0..3_u32 {
            factory.create(
                None,
                format!("seed{i}"),
                arrow::datatypes::DataType::Int64,
                false,
            );
        }
        assert_eq!(factory.peek_next_id(), 4);

        let stmt = crate::sql::parser::parse_sql_raw("SELECT 1 + 1 AS x").expect("parse");
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => panic!("not a query"),
        };
        let (_resolved, _ctes, out_factory) =
            analyze_with_factory(&query, &TestCatalog, "db", factory).expect("analyze");
        // The analysis must have allocated its ids on top of the seeded ones.
        assert!(out_factory.peek_next_id() > 4);
        assert_eq!(
            out_factory.get(crate::sql::column_id::ColumnId(1)).name,
            "seed0"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 3 — Apply framework scalar subquery routing tests
    // ---------------------------------------------------------------------------

    const CORRELATED_SCALAR_SQL: &str =
        "SELECT k1 FROM t1 WHERE k1 = (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1)";

    fn parse_and_analyze_for_apply_specs(sql: &str) -> Result<ResolvedQuery, String> {
        let dialect = sqlparser::dialect::GenericDialect {};
        let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql)
            .map_err(|e| format!("parse error: {e}"))?;
        let stmt = stmts.into_iter().next().ok_or("empty SQL")?;
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => return Err("expected a query".into()),
        };
        let (resolved, _cte, _factory) = analyze(&query, &TestCatalog, "default")?;
        Ok(resolved)
    }

    /// In the Apply framework a correlated scalar WHERE subquery must be recorded as an
    /// ApplyScalarSpec instead of being rewritten into a join.
    #[test]
    fn current_route_records_where_scalar_subquery_apply_spec() {
        use crate::sql::analysis::{ApplyClause, QueryBody};

        let dialect = sqlparser::dialect::GenericDialect {};
        let stmts = sqlparser::parser::Parser::parse_sql(&dialect, CORRELATED_SCALAR_SQL).unwrap();
        let query = match stmts.into_iter().next().unwrap() {
            sqlparser::ast::Statement::Query(q) => q,
            _ => panic!("expected query"),
        };
        let (resolved, _cte, _factory) =
            analyze(&query, &TestCatalog, "default").expect("analyze with apply framework");

        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.apply_specs.len(),
            1,
            "one scalar apply spec expected"
        );
        let spec = &select.apply_specs[0];
        assert_eq!(spec.clause, ApplyClause::Where);
        assert!(spec.need_check_max_rows);
        assert!(
            !spec.correlation_column_ids.is_empty(),
            "correlated subquery must record outer column ids"
        );
        // The placeholder must be gone from the WHERE predicate (replaced by a
        // ColumnRef to the spec's output column). Verify no SubqueryPlaceholder
        // remains in the WHERE filter.
        if let Some(ref filter) = select.filter {
            assert!(
                !expr_has_subquery_placeholder(filter),
                "WHERE filter must not contain a SubqueryPlaceholder after apply routing"
            );
        }
        // In the Apply framework the FROM should NOT have grown a join.
        let from_is_join = matches!(select.from, Some(crate::sql::analysis::Relation::Join(_)));
        assert!(
            !from_is_join,
            "Apply framework must NOT rewrite the scalar subquery into a join"
        );
    }

    #[test]
    fn scalar_apply_factors_common_correlation_from_or_filter() {
        use crate::sql::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 \
             WHERE (SELECT count(*) FROM t2 \
                    WHERE (t2.k1 = t1.k1 AND t2.k2 = 1) \
                       OR (t2.k1 = t1.k1 AND t2.k2 = 2)) > 0",
        )
        .expect("analyze scalar Apply with OR correlation");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.apply_specs.len(), 1);

        let QueryBody::Select(inner_select) = &select.apply_specs[0].inner.body else {
            panic!("expected scalar Apply inner select");
        };
        let filter = inner_select
            .filter
            .as_ref()
            .expect("expected scalar Apply inner filter");
        let ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } = &filter.kind
        else {
            panic!("expected common correlation to be factored into top-level AND: {filter:?}");
        };
        assert!(
            matches!(&left.kind, ExprKind::BinaryOp { op: BinOp::Eq, .. }),
            "left conjunct should be the common equality correlation: {left:?}"
        );
        assert!(
            matches!(&right.kind, ExprKind::BinaryOp { op: BinOp::Or, .. }),
            "right conjunct should retain the residual OR predicate: {right:?}"
        );
    }

    #[test]
    fn correlated_avg_scalar_subquery_comparison_preserves_float_type() {
        use crate::sql::analysis::QueryBody;
        use arrow::datatypes::DataType;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 \
             WHERE k1 < (SELECT avg(k2) FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect("analyze correlated AVG scalar subquery");

        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.apply_specs.len(), 1);
        let spec = &select.apply_specs[0];
        assert_eq!(
            spec.output_column.data_type,
            DataType::Float64,
            "AVG scalar subquery output must remain Float64"
        );

        let filter = select.filter.as_ref().expect("expected WHERE filter");
        let ExprKind::BinaryOp {
            left,
            op: BinOp::Lt,
            right,
        } = &filter.kind
        else {
            panic!("expected k1 < scalar subquery comparison, got {filter:?}");
        };

        assert_eq!(
            left.data_type,
            DataType::Float64,
            "left INT operand should be widened to Float64 for AVG comparison"
        );
        assert_eq!(
            right.data_type,
            DataType::Float64,
            "scalar subquery RHS should stay Float64, not cast back to INT"
        );
        assert!(
            !expr_casts_column_to_type(right, spec.output_column.column_id, &DataType::Int64)
                && !expr_casts_column_to_type(
                    right,
                    spec.output_column.column_id,
                    &DataType::Int32
                ),
            "RHS must not contain CAST(scalar_avg AS integer): {right:?}"
        );
    }

    fn expr_casts_column_to_type(
        expr: &TypedExpr,
        column_id: crate::sql::column_id::ColumnId,
        target: &arrow::datatypes::DataType,
    ) -> bool {
        match &expr.kind {
            ExprKind::Cast {
                expr: inner,
                target: cast_target,
            } => {
                (cast_target == target && expr_refs_column(inner, column_id))
                    || expr_casts_column_to_type(inner, column_id, target)
            }
            ExprKind::BinaryOp { left, right, .. } => {
                expr_casts_column_to_type(left, column_id, target)
                    || expr_casts_column_to_type(right, column_id, target)
            }
            ExprKind::Nested(inner)
            | ExprKind::UnaryOp { expr: inner, .. }
            | ExprKind::IsNull { expr: inner, .. } => {
                expr_casts_column_to_type(inner, column_id, target)
            }
            _ => false,
        }
    }

    fn expr_refs_column(expr: &TypedExpr, column_id: crate::sql::column_id::ColumnId) -> bool {
        match &expr.kind {
            ExprKind::ColumnRef { column_id: id, .. } => *id == column_id,
            ExprKind::Cast { expr: inner, .. }
            | ExprKind::Nested(inner)
            | ExprKind::UnaryOp { expr: inner, .. }
            | ExprKind::IsNull { expr: inner, .. } => expr_refs_column(inner, column_id),
            ExprKind::BinaryOp { left, right, .. } => {
                expr_refs_column(left, column_id) || expr_refs_column(right, column_id)
            }
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                args.iter().any(|arg| expr_refs_column(arg, column_id))
            }
            _ => false,
        }
    }

    #[test]
    fn in_subquery_derived_table_internal_refs_are_not_outer_refs() {
        use crate::sql::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 \
             WHERE k1 IN ( \
                 SELECT k1 \
                 FROM (SELECT k1 AS k1, k2 AS ranking FROM t2) tmp1 \
                 WHERE ranking <= 5 \
             )",
        )
        .expect("derived-table internals should not be treated as correlated outer refs");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        assert!(
            select.predicate_apply_specs[0]
                .correlation_column_ids
                .is_empty(),
            "uncorrelated IN subquery must not record correlation ids"
        );
    }

    /// Helper: returns true if `expr` or any descendant is a SubqueryPlaceholder.
    fn expr_has_subquery_placeholder(expr: &TypedExpr) -> bool {
        match &expr.kind {
            ExprKind::SubqueryPlaceholder { .. } => true,
            ExprKind::BinaryOp { left, right, .. } => {
                expr_has_subquery_placeholder(left) || expr_has_subquery_placeholder(right)
            }
            ExprKind::UnaryOp { expr: inner, .. } => expr_has_subquery_placeholder(inner),
            ExprKind::IsNull { expr: inner, .. } => expr_has_subquery_placeholder(inner),
            ExprKind::Cast { expr: inner, .. } => expr_has_subquery_placeholder(inner),
            ExprKind::Nested(inner) => expr_has_subquery_placeholder(inner),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                args.iter().any(expr_has_subquery_placeholder)
            }
            _ => false,
        }
    }

    #[test]
    fn exists_correlated_where_records_predicate_spec() {
        use crate::sql::analysis::{QueryBody, SubqueryKind};

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        let spec = &select.predicate_apply_specs[0];
        assert!(matches!(spec.kind, SubqueryKind::Exists { negated: false }));
        assert!(spec.use_semi_anti);
        assert_eq!(spec.correlation_column_ids.len(), 1);
        assert!(spec.in_lhs.is_none());
        if let Some(filter) = &select.filter {
            assert!(
                !expr_has_subquery_placeholder(filter),
                "WHERE filter must not contain a SubqueryPlaceholder after apply routing"
            );
        }
    }

    #[test]
    fn not_exists_sets_negated() {
        use crate::sql::analysis::{QueryBody, SubqueryKind};

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        assert!(matches!(
            select.predicate_apply_specs[0].kind,
            SubqueryKind::Exists { negated: true }
        ));
    }

    #[test]
    fn in_uncorrelated_records_spec_with_lhs() {
        use crate::sql::analysis::{QueryBody, SubqueryKind};

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE t1.k1 IN (SELECT t2.k2 FROM t2)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        let spec = &select.predicate_apply_specs[0];
        assert!(matches!(
            spec.kind,
            SubqueryKind::InSubquery { negated: false }
        ));
        assert!(spec.in_lhs.is_some());
        assert!(
            spec.correlation_column_ids.is_empty(),
            "uncorrelated IN must not record correlation column ids"
        );
    }

    #[test]
    fn not_in_sets_negated() {
        use crate::sql::analysis::{QueryBody, SubqueryKind};

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE t1.k1 NOT IN (SELECT t2.k2 FROM t2)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        assert!(matches!(
            select.predicate_apply_specs[0].kind,
            SubqueryKind::InSubquery { negated: true }
        ));
    }

    #[test]
    fn exists_inside_or_uses_explicit_value_form_rewrite() {
        use crate::sql::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE k1 = 1 OR EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = 1)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert!(select.predicate_apply_specs.is_empty());
        assert!(
            matches!(select.from, Some(crate::sql::analysis::Relation::Join(_))),
            "inside-OR EXISTS should use the explicit value-form rewrite"
        );
    }

    #[test]
    fn not_in_value_form_marker_relations_are_nullable() {
        use crate::sql::analysis::{QueryBody, Relation};

        fn collect_marker_outputs(rel: &Relation, out: &mut Vec<(String, bool)>) {
            match rel {
                Relation::Subquery {
                    alias,
                    output_columns,
                    ..
                } => {
                    if alias.starts_with("__sq_null_") || alias.starts_with("__sq_any_") {
                        for column in output_columns {
                            if column.name.starts_with("__has_") {
                                out.push((column.name.clone(), column.nullable));
                            }
                        }
                    }
                }
                Relation::Join(join) => {
                    collect_marker_outputs(&join.left, out);
                    collect_marker_outputs(&join.right, out);
                }
                _ => {}
            }
        }

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE k2 NOT IN (SELECT t2.k2 FROM t2) OR false",
        )
        .expect("analyze value-form NOT IN");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        let mut markers = Vec::new();
        if let Some(from) = &select.from {
            collect_marker_outputs(from, &mut markers);
        }
        assert_eq!(
            markers.len(),
            2,
            "nullable NOT IN should create null and nonempty marker relations"
        );
        for (name, nullable) in markers {
            assert!(nullable, "{name} marker output must be nullable");
        }
    }

    #[test]
    fn correlated_exists_inside_or_is_rejected_without_fallback() {
        let err = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE k1 = 1 OR EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect_err("correlated EXISTS value-form should not fall back");
        assert!(
            err.contains("correlated EXISTS subquery in value-form expression is not supported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn correlated_in_inside_or_is_rejected_without_fallback() {
        let err = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE k1 = 1 OR k2 IN (SELECT t2.k2 FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect_err("correlated IN value-form should not fall back");
        assert!(
            err.contains("correlated IN subquery in value-form expression is not supported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn exists_in_having_records_predicate_spec() {
        use crate::sql::analysis::{ApplyClause, QueryBody};

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1, count(*) FROM t1 GROUP BY k1 HAVING EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        assert_eq!(select.predicate_apply_specs[0].clause, ApplyClause::Having);
    }

    #[test]
    fn multi_column_in_rewrites_without_apply_spec() {
        use crate::sql::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE (k1, k2) IN (SELECT t2.k1, t2.k2 FROM t2)",
        )
        .expect("multi-column IN should use the local join rewrite");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert!(
            select.predicate_apply_specs.is_empty(),
            "multi-column IN is represented by the local join rewrite, not Apply"
        );
    }

    #[test]
    fn mixed_eq_and_non_eq_outer_ref_records_exists_spec() {
        use crate::sql::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.v2 = t1.v1 AND t2.k2 > t1.k2)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "mixed equality and non-equality correlation should record an EXISTS predicate spec"
        );
    }

    #[test]
    fn pure_between_outer_ref_records_exists_spec() {
        use crate::sql::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t1.k1 BETWEEN t2.k1 AND t2.k2)",
        )
        .expect("BETWEEN correlation should route through predicate Apply");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "BETWEEN correlation should record an EXISTS predicate spec"
        );
    }

    #[test]
    fn pure_non_eq_outer_ref_records_exists_spec() {
        use crate::sql::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k2 > t1.k2)",
        )
        .expect("analyze pure non-EQ correlation in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "pure non-equality correlation should record an EXISTS predicate spec"
        );
    }

    #[test]
    fn in_pure_non_eq_outer_ref_records_predicate_spec() {
        use crate::sql::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE t1.k1 IN (SELECT t2.k1 FROM t2 WHERE t2.k2 > t1.k2)",
        )
        .expect("analyze pure non-EQ IN correlation in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "pure non-equality correlation should record an IN predicate spec"
        );
    }

    #[test]
    fn is_true_outer_ref_records_exists_spec() {
        use crate::sql::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE (t1.k1 = 1) IS TRUE)",
        )
        .expect("IS TRUE outer ref should route through predicate Apply");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "IS TRUE correlation should record an EXISTS predicate spec"
        );
    }

    #[test]
    fn in_rhs_projection_outer_ref_is_rejected_without_fallback() {
        let err = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE t1.k1 IN (SELECT t1.k2 FROM t2)",
        )
        .expect_err("outer refs outside the inner filter should not fall back");
        assert!(
            err.contains("correlated EXISTS/IN subquery must use comparison predicates"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn correlated_exists_in_projection_is_rejected_clearly() {
        let err = parse_and_analyze("SELECT EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1) FROM t1")
            .expect_err("correlated EXISTS in SELECT list should be rejected");
        assert!(
            err.contains("correlated EXISTS subquery in SELECT list is not supported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn group_by_subquery_is_rejected_clearly() {
        let err = parse_and_analyze("SELECT k1 FROM t1 GROUP BY (SELECT max(k2) FROM t2)")
            .expect_err("subquery in GROUP BY should be rejected");
        assert!(
            err.contains("subquery is not supported in GROUP BY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn order_by_subquery_is_rejected_clearly() {
        let err = parse_and_analyze("SELECT k1 FROM t1 ORDER BY (SELECT max(k2) FROM t2)")
            .expect_err("subquery in ORDER BY should be rejected");
        assert!(
            err.contains("subquery is not supported in ORDER BY"),
            "unexpected error: {err}"
        );
    }
}
