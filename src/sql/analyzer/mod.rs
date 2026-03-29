//! Semantic analyzer: converts `sqlparser::ast::Query` into `ResolvedQuery`.
//!
//! This module performs name resolution, type inference, and scope management
//! without producing any physical plan concepts (tuple_id, slot_id, etc.).

mod functions;
mod helpers;
mod resolve_expr;
mod resolve_from;
mod scope;
mod subquery_rewrite;

use arrow::datatypes::DataType;
use sqlparser::ast as sqlast;

use crate::sql::catalog::CatalogProvider;

use crate::sql::ir::{
    ExprKind, JoinKind, JoinRelation, OutputColumn, ProjectItem, QueryBody, Relation,
    ResolvedQuery, ResolvedSelect, ResolvedSetOp, ResolvedValues, SetOpKind, SortItem,
    SubqueryInfo, TypedExpr,
};
use crate::sql::types::wider_type;

use helpers::{expr_display_name, extract_limit, extract_offset};
use scope::AnalyzerScope;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Analyze a parsed SQL query and produce a fully resolved query IR.
pub(crate) fn analyze(
    query: &sqlast::Query,
    catalog: &impl CatalogProvider,
    current_database: &str,
) -> Result<ResolvedQuery, String> {
    let ctx = AnalyzerContext {
        catalog,
        current_database,
        ctes: std::collections::HashMap::new(),
        next_subquery_id: std::cell::Cell::new(0),
        collected_subqueries: std::cell::RefCell::new(Vec::new()),
    };
    ctx.analyze_query(query)
}

// ---------------------------------------------------------------------------
// Analyzer context
// ---------------------------------------------------------------------------

pub(super) struct AnalyzerContext<'a> {
    pub(super) catalog: &'a dyn CatalogProvider,
    pub(super) current_database: &'a str,
    /// CTE definitions from WITH clause, keyed by lowercase name.
    /// Value is (query, optional column aliases).
    pub(super) ctes: std::collections::HashMap<String, (sqlast::Query, Vec<String>)>,
    /// Counter for generating unique subquery placeholder IDs.
    pub(super) next_subquery_id: std::cell::Cell<usize>,
    /// Subqueries collected during expression analysis.
    /// Populated by `resolve_expr.rs`, consumed by `subquery_rewrite.rs`.
    pub(super) collected_subqueries: std::cell::RefCell<Vec<SubqueryInfo>>,
}

impl<'a> AnalyzerContext<'a> {
    /// Allocate a unique subquery placeholder ID.
    pub(super) fn alloc_subquery_id(&self) -> usize {
        let id = self.next_subquery_id.get();
        self.next_subquery_id.set(id + 1);
        id
    }

    /// Top-level query analysis.
    fn analyze_query(&self, query: &sqlast::Query) -> Result<ResolvedQuery, String> {
        // Register CTEs from WITH clause (if any) into a child context.
        let child_ctx;
        let ctx = if let Some(ref with_clause) = query.with {
            let mut ctes = self.ctes.clone();
            for cte in &with_clause.cte_tables {
                let name = cte.alias.name.value.to_lowercase();
                let col_aliases: Vec<String> = cte
                    .alias
                    .columns
                    .iter()
                    .map(|c| c.name.value.to_lowercase())
                    .collect();
                ctes.insert(name, (*cte.query.clone(), col_aliases));
            }
            child_ctx = AnalyzerContext {
                catalog: self.catalog,
                current_database: self.current_database,
                ctes,
                next_subquery_id: std::cell::Cell::new(self.next_subquery_id.get()),
                collected_subqueries: std::cell::RefCell::new(Vec::new()),
            };
            &child_ctx
        } else {
            self
        };

        // Analyze body (SELECT / SetOperation / VALUES)
        let (body, body_output) = ctx.analyze_set_expr(query.body.as_ref())?;

        // Analyze ORDER BY
        let order_by = ctx.analyze_order_by(query, &body_output, &body)?;

        // Extract LIMIT / OFFSET
        let limit = extract_limit(query)?;
        let offset = extract_offset(query)?;

        // Build output columns from the body
        let output_columns = body_output;

        Ok(ResolvedQuery {
            body,
            order_by,
            limit,
            offset,
            output_columns,
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
                // If so, expand into a UNION ALL of GROUP BY variants.
                if let Some(rollup_exprs) = self.extract_rollup_from_group_by(s) {
                    return self.expand_rollup(s, &rollup_exprs);
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
                let (left_body, left_cols) = self.analyze_set_expr(left)?;
                let (right_body, right_cols) = self.analyze_set_expr(right)?;

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
                    output_cols.push(OutputColumn {
                        name: lc.name.clone(),
                        data_type: dt,
                        nullable: lc.nullable || rc.nullable,
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
                        left: Box::new(left_body),
                        right: Box::new(right_body),
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

    /// Analyze a VALUES clause.
    fn analyze_values(
        &self,
        values: &sqlast::Values,
    ) -> Result<(ResolvedValues, Vec<OutputColumn>), String> {
        let scope = AnalyzerScope::new(); // VALUES has no table scope
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
            .map(|(i, dt)| OutputColumn {
                name: format!("column{}", i),
                data_type: dt.clone(),
                nullable: true,
            })
            .collect();

        Ok((
            ResolvedValues {
                rows: resolved_rows,
                column_types,
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
            (None, AnalyzerScope::new())
        } else if select.from.len() == 1 {
            let (rel, scope) = self.analyze_from(&select.from[0])?;
            (Some(rel), scope)
        } else {
            // Multiple comma-separated FROM items → implicit CROSS JOIN
            let mut iter = select.from.iter();
            let first = iter.next().unwrap();
            let (mut current_rel, mut current_scope) = self.analyze_from(first)?;
            for twj in iter {
                let (right_rel, right_scope) = self.analyze_from(twj)?;
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
            Some(expr) => Some(self.analyze_expr(expr, &scope)?),
            None => None,
        };

        // --- SELECT list (before GROUP BY so aliases are available) ---
        let (projection, output_columns) = self.analyze_projection(&select.projection, &scope)?;

        // --- GROUP BY (with SELECT alias fallback) ---
        let group_by_exprs = match &select.group_by {
            sqlast::GroupByExpr::Expressions(exprs, _) => exprs.clone(),
            sqlast::GroupByExpr::All(_) => {
                return Err("GROUP BY ALL is not supported".into());
            }
        };
        let mut group_by = Vec::with_capacity(group_by_exprs.len());
        for gb_expr in &group_by_exprs {
            match self.analyze_expr(gb_expr, &scope) {
                Ok(typed) => group_by.push(typed),
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
                    group_by.push(self.substitute_select_aliases(typed, &projection));
                }
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
        };

        // --- Subquery rewriting ---
        // If the WHERE or HAVING clause contained subqueries (recorded as
        // SubqueryPlaceholder nodes), rewrite them into JOINs now.
        let has_subqueries = !self.collected_subqueries.borrow().is_empty();
        if has_subqueries {
            let mut mutable_scope = scope;
            self.rewrite_subqueries(&mut resolved_select, &mut mutable_scope)?;
        }

        Ok((resolved_select, output_columns))
    }

    /// Replace ColumnRef nodes that match SELECT aliases with the aliased expression.
    fn substitute_select_aliases(&self, expr: TypedExpr, projection: &[ProjectItem]) -> TypedExpr {
        match expr.kind {
            ExprKind::ColumnRef {
                ref qualifier,
                ref column,
            } if qualifier.is_none() => {
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
                    left: Box::new(self.substitute_select_aliases(*left, projection)),
                    op,
                    right: Box::new(self.substitute_select_aliases(*right, projection)),
                },
            },
            ExprKind::UnaryOp { op, expr: inner } => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::UnaryOp {
                    op,
                    expr: Box::new(self.substitute_select_aliases(*inner, projection)),
                },
            },
            ExprKind::Nested(inner) => TypedExpr {
                data_type: expr.data_type,
                nullable: expr.nullable,
                kind: ExprKind::Nested(Box::new(
                    self.substitute_select_aliases(*inner, projection),
                )),
            },
            // For other node types, return as-is
            _ => expr,
        }
    }

    /// Check if a SELECT's GROUP BY clause contains a ROLLUP expression.
    /// Returns the flattened rollup column expressions if found, None otherwise.
    fn extract_rollup_from_group_by(
        &self,
        select: &sqlast::Select,
    ) -> Option<Vec<Vec<sqlast::Expr>>> {
        let exprs = match &select.group_by {
            sqlast::GroupByExpr::Expressions(exprs, _) => exprs,
            _ => return None,
        };
        // Look for a single Rollup expression in the GROUP BY list
        for expr in exprs {
            if let sqlast::Expr::Rollup(groups) = expr {
                return Some(groups.clone());
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

            // Modify projection: replace NULLed columns with NULL literals
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
                    modified_projection.push(item.clone());
                }
            }
            modified_select.projection = modified_projection;

            let (sel, cols) = self.analyze_select(&modified_select)?;
            bodies.push((QueryBody::Select(sel), cols));
        }

        // Build UNION ALL chain from right to left
        let (mut result_body, result_cols) = bodies.remove(0);
        for (body, _cols) in bodies {
            result_body = QueryBody::SetOperation(ResolvedSetOp {
                kind: SetOpKind::Union,
                all: true,
                left: Box::new(result_body),
                right: Box::new(body),
            });
        }

        Ok((result_body, result_cols))
    }

    /// Analyze the SELECT projection list.
    fn analyze_projection(
        &self,
        items: &[sqlast::SelectItem],
        scope: &AnalyzerScope,
    ) -> Result<(Vec<ProjectItem>, Vec<OutputColumn>), String> {
        let mut projection = Vec::new();
        let mut output_columns = Vec::new();

        for item in items {
            match item {
                sqlast::SelectItem::UnnamedExpr(expr) => {
                    let typed = self.analyze_expr(expr, scope)?;
                    let name = expr_display_name(expr);
                    output_columns.push(OutputColumn {
                        name: name.clone(),
                        data_type: typed.data_type.clone(),
                        nullable: typed.nullable,
                    });
                    projection.push(ProjectItem {
                        expr: typed,
                        output_name: name,
                    });
                }
                sqlast::SelectItem::ExprWithAlias { expr, alias } => {
                    let typed = self.analyze_expr(expr, scope)?;
                    let name = alias.value.clone();
                    output_columns.push(OutputColumn {
                        name: name.clone(),
                        data_type: typed.data_type.clone(),
                        nullable: typed.nullable,
                    });
                    projection.push(ProjectItem {
                        expr: typed,
                        output_name: name,
                    });
                }
                sqlast::SelectItem::Wildcard(_) => {
                    for (qualifier, col_name, data_type, nullable) in scope.iter_columns() {
                        let typed = TypedExpr {
                            kind: ExprKind::ColumnRef {
                                qualifier: qualifier.clone(),
                                column: col_name.clone(),
                            },
                            data_type: data_type.clone(),
                            nullable: *nullable,
                        };
                        output_columns.push(OutputColumn {
                            name: col_name.clone(),
                            data_type: data_type.clone(),
                            nullable: *nullable,
                        });
                        projection.push(ProjectItem {
                            expr: typed,
                            output_name: col_name.clone(),
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
                    let mut found = false;
                    for (qualifier, col_name, data_type, nullable) in
                        scope.iter_qualified_columns(&qualifier_str)
                    {
                        found = true;
                        let typed = TypedExpr {
                            kind: ExprKind::ColumnRef {
                                qualifier: qualifier.clone(),
                                column: col_name.clone(),
                            },
                            data_type: data_type.clone(),
                            nullable: *nullable,
                        };
                        output_columns.push(OutputColumn {
                            name: col_name.clone(),
                            data_type: data_type.clone(),
                            nullable: *nullable,
                        });
                        projection.push(ProjectItem {
                            expr: typed,
                            output_name: col_name.clone(),
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

    /// Rebuild the FROM scope from an already-resolved Relation tree.
    /// Used by ORDER BY fallback when the expression doesn't match projection columns.
    fn rebuild_from_scope(&self, relation: &Relation) -> Result<((), AnalyzerScope), String> {
        let mut scope = AnalyzerScope::new();
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
                scope.add_table(Some(qualifier), &scan.table.columns);
                Ok(())
            }
            Relation::Subquery { query, alias } => {
                for col in &query.output_columns {
                    scope.add_column(
                        Some(alias.as_str()),
                        &col.name,
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
                scope.add_column(Some(qualifier), &gs.column_name, DataType::Int64, false);
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
        let mut projection_scope = AnalyzerScope::new();
        for col in body_output {
            projection_scope.add_column(None, &col.name, col.data_type.clone(), col.nullable);
        }
        // Also register qualified column refs from projection items
        // so ORDER BY a.id works when SELECT has a.id
        if let QueryBody::Select(sel) = body {
            for item in &sel.projection {
                if let ExprKind::ColumnRef {
                    qualifier: Some(ref q),
                    ref column,
                } = item.expr.kind
                {
                    projection_scope.add_column(
                        Some(q),
                        column,
                        item.expr.data_type.clone(),
                        item.expr.nullable,
                    );
                }
            }
        }

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
                            qualifier: None,
                            column: col.name.clone(),
                        },
                        data_type: col.data_type.clone(),
                        nullable: col.nullable,
                    }
                }
                _ => {
                    // Try projection scope first, then fall back to FROM scope
                    match self.analyze_expr(&ob.expr, &projection_scope) {
                        Ok(typed) => typed,
                        Err(proj_err) => {
                            // Try resolving against the FROM scope (for ORDER BY
                            // on columns not in the SELECT list).
                            if let QueryBody::Select(sel) = body {
                                if let Some(ref from_rel) = sel.from {
                                    let (_, from_scope) = self.rebuild_from_scope(from_rel)?;
                                    match self.analyze_expr(&ob.expr, &from_scope) {
                                        Ok(typed) => typed,
                                        Err(_) => return Err(proj_err),
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
            };

            let asc = ob.options.asc.unwrap_or(true);
            let nulls_first = ob.options.nulls_first.unwrap_or(!asc);

            sort_items.push(SortItem {
                expr: typed,
                asc,
                nulls_first,
            });
        }

        Ok(sort_items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::{ExprKind, JoinKind, Relation};

    struct TestCatalog;
    impl crate::sql::catalog::CatalogProvider for TestCatalog {
        fn get_table(&self, _db: &str, table: &str) -> Result<TableDef, String> {
            match table {
                "orders" => Ok(TableDef {
                    name: "orders".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "o_orderkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "o_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "o_orderstatus".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "o_totalprice".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "o_orderdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "o_orderpriority".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                        },
                    ],
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/orders.parquet"),
                    },
                }),
                "lineitem" => Ok(TableDef {
                    name: "lineitem".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "l_orderkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "l_partkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "l_suppkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "l_quantity".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "l_extendedprice".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "l_discount".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "l_commitdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "l_receiptdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "l_shipdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                        },
                    ],
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/lineitem.parquet"),
                    },
                }),
                "supplier" => Ok(TableDef {
                    name: "supplier".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "s_suppkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "s_name".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "s_comment".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                        },
                    ],
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/supplier.parquet"),
                    },
                }),
                "part" => Ok(TableDef {
                    name: "part".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "p_partkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "p_name".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "p_brand".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                        },
                    ],
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/part.parquet"),
                    },
                }),
                "partsupp" => Ok(TableDef {
                    name: "partsupp".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "ps_partkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "ps_suppkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "ps_supplycost".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "ps_availqty".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: true,
                        },
                    ],
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/partsupp.parquet"),
                    },
                }),
                "customer" => Ok(TableDef {
                    name: "customer".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "c_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "c_acctbal".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                        },
                        ColumnDef {
                            name: "c_phone".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                        },
                    ],
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/customer.parquet"),
                    },
                }),
                "nation" => Ok(TableDef {
                    name: "nation".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "n_nationkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "n_name".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                        },
                    ],
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/nation.parquet"),
                    },
                }),
                _ => Err(format!("table not found: {table}")),
            }
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
        analyze(&query, &TestCatalog, "default")
    }

    /// Helper to check that a Relation tree contains a JOIN of a given kind.
    fn has_join_kind(rel: &Relation, kind: JoinKind) -> bool {
        match rel {
            Relation::Join(jr) => {
                jr.join_type == kind
                    || has_join_kind(&jr.left, kind)
                    || has_join_kind(&jr.right, kind)
            }
            _ => false,
        }
    }

    #[test]
    fn exists_subquery_rewrites_to_left_semi_join() {
        let sql = "SELECT o_orderpriority, count(*) FROM orders \
                    WHERE exists (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey) \
                    GROUP BY o_orderpriority";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        if let QueryBody::Select(sel) = &resolved.body {
            let from = sel.from.as_ref().expect("should have FROM");
            assert!(
                has_join_kind(from, JoinKind::LeftSemi),
                "EXISTS should be rewritten to LEFT SEMI JOIN, got: {from:?}"
            );
            // The placeholder should be removed from the filter
            assert!(
                sel.filter.is_none() || !filter_has_placeholder(&sel.filter),
                "filter should not contain SubqueryPlaceholder"
            );
        } else {
            panic!("expected Select body");
        }
    }

    #[test]
    fn not_exists_subquery_rewrites_to_left_anti_join() {
        let sql = "SELECT o_orderpriority FROM orders \
                    WHERE not exists (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        if let QueryBody::Select(sel) = &resolved.body {
            let from = sel.from.as_ref().expect("should have FROM");
            assert!(
                has_join_kind(from, JoinKind::LeftAnti),
                "NOT EXISTS should be rewritten to LEFT ANTI JOIN"
            );
        } else {
            panic!("expected Select body");
        }
    }

    #[test]
    fn in_subquery_rewrites_to_left_semi_join() {
        let sql = "SELECT o_orderkey FROM orders \
                    WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        if let QueryBody::Select(sel) = &resolved.body {
            let from = sel.from.as_ref().expect("should have FROM");
            assert!(
                has_join_kind(from, JoinKind::LeftSemi),
                "IN should be rewritten to LEFT SEMI JOIN"
            );
        } else {
            panic!("expected Select body");
        }
    }

    #[test]
    fn not_in_subquery_rewrites_to_left_anti_join() {
        let sql = "SELECT s_suppkey FROM supplier \
                    WHERE s_suppkey NOT IN (SELECT ps_suppkey FROM partsupp)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        if let QueryBody::Select(sel) = &resolved.body {
            let from = sel.from.as_ref().expect("should have FROM");
            assert!(
                has_join_kind(from, JoinKind::LeftAnti),
                "NOT IN should be rewritten to LEFT ANTI JOIN"
            );
        } else {
            panic!("expected Select body");
        }
    }

    #[test]
    fn uncorrelated_scalar_subquery_rewrites_to_cross_join() {
        let sql = "SELECT c_custkey FROM customer \
                    WHERE c_acctbal > (SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        if let QueryBody::Select(sel) = &resolved.body {
            let from = sel.from.as_ref().expect("should have FROM");
            assert!(
                has_join_kind(from, JoinKind::Cross),
                "uncorrelated scalar subquery should be rewritten to CROSS JOIN, got: {from:?}"
            );
            // The filter should still exist (the comparison) but no placeholder
            assert!(
                sel.filter.is_some(),
                "filter should still contain the comparison"
            );
            assert!(
                !filter_has_placeholder(&sel.filter),
                "filter should not contain SubqueryPlaceholder"
            );
        } else {
            panic!("expected Select body");
        }
    }

    #[test]
    fn correlated_scalar_subquery_rewrites_to_left_join() {
        let sql = "SELECT l_orderkey FROM lineitem, part \
                    WHERE p_partkey = l_partkey \
                    AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        if let QueryBody::Select(sel) = &resolved.body {
            let from = sel.from.as_ref().expect("should have FROM");
            assert!(
                has_join_kind(from, JoinKind::LeftOuter),
                "correlated scalar subquery should be rewritten to LEFT OUTER JOIN, got: {from:?}"
            );
        } else {
            panic!("expected Select body");
        }
    }

    #[test]
    fn scalar_subquery_in_having_rewrites_to_cross_join() {
        let sql = "SELECT ps_partkey, sum(ps_supplycost) as value FROM partsupp \
                    GROUP BY ps_partkey \
                    HAVING sum(ps_supplycost) > (SELECT sum(ps_supplycost) * 0.0001 FROM partsupp)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        if let QueryBody::Select(sel) = &resolved.body {
            let from = sel.from.as_ref().expect("should have FROM");
            assert!(
                has_join_kind(from, JoinKind::Cross),
                "scalar subquery in HAVING should be rewritten to CROSS JOIN"
            );
        } else {
            panic!("expected Select body");
        }
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
        if let QueryBody::Select(sel) = &resolved.body {
            let from = sel.from.as_ref().expect("should have FROM");
            assert!(
                has_join_kind(from, JoinKind::LeftSemi),
                "EXISTS should produce LEFT SEMI JOIN"
            );
            assert!(
                has_join_kind(from, JoinKind::LeftAnti),
                "NOT EXISTS should produce LEFT ANTI JOIN"
            );
            assert!(
                !filter_has_placeholder(&sel.filter),
                "filter should not contain SubqueryPlaceholder"
            );
        } else {
            panic!("expected Select body");
        }
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
        if let QueryBody::Select(sel) = &resolved.body {
            let from = sel.from.as_ref().expect("should have FROM");
            assert!(
                has_join_kind(from, JoinKind::LeftSemi),
                "IN subquery should be rewritten to LEFT SEMI JOIN"
            );
        } else {
            panic!("expected Select body");
        }
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
                    .map_or(false, |o| expr_has_placeholder_deep(o))
                    || when_then
                        .iter()
                        .any(|(w, t)| expr_has_placeholder_deep(w) || expr_has_placeholder_deep(t))
                    || else_expr
                        .as_ref()
                        .map_or(false, |e| expr_has_placeholder_deep(e))
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
        // Verify that no SubqueryPlaceholder remains in the projection
        if let QueryBody::Select(sel) = &resolved.body {
            for item in &sel.projection {
                assert!(
                    !expr_has_placeholder_deep(&item.expr),
                    "projection should not contain SubqueryPlaceholder after rewriting: {:?}",
                    item.expr
                );
            }
        }
    }

    #[test]
    fn test_group_by_rollup() {
        // q5 pattern: GROUP BY ROLLUP(a, b)
        let sql = "SELECT o_orderstatus, o_orderpriority, count(*) as cnt \
                   FROM orders \
                   GROUP BY ROLLUP(o_orderstatus, o_orderpriority)";
        let resolved = parse_and_analyze(sql).expect("GROUP BY ROLLUP should work");
        assert!(!resolved.output_columns.is_empty());
        // ROLLUP(a, b) should produce a UNION ALL of 3 levels:
        // Level 0: GROUP BY a, b
        // Level 1: GROUP BY a (b is NULLed)
        // Level 2: no GROUP BY (both NULLed)
        assert!(
            matches!(resolved.body, QueryBody::SetOperation(_)),
            "ROLLUP should be expanded into a UNION ALL set operation, got: {:?}",
            std::mem::discriminant(&resolved.body)
        );
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
    }
}
