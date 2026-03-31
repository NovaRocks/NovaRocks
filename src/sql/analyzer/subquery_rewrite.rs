//! Subquery-to-join rewriting pass.
//!
//! After the analyzer produces `SubqueryPlaceholder` nodes in WHERE/HAVING
//! expressions, this module rewrites them into equivalent JOINs:
//!
//! - Scalar subqueries → CROSS JOIN (uncorrelated) or LEFT JOIN (correlated)
//! - EXISTS / NOT EXISTS → LEFT SEMI / LEFT ANTI JOIN
//! - IN / NOT IN → LEFT SEMI / LEFT ANTI JOIN
//!
//! The rewriting happens at the `ResolvedSelect` level before the planner sees it.

use arrow::datatypes::DataType;

use crate::sql::ir::*;

use super::AnalyzerContext;
use super::scope::AnalyzerScope;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

impl<'a> AnalyzerContext<'a> {
    /// Rewrite subquery placeholders in a ResolvedSelect into JOINs.
    /// This must be called after `analyze_select` has finished and the
    /// subquery placeholders have been collected.
    pub(super) fn rewrite_subqueries(
        &self,
        select: &mut ResolvedSelect,
        scope: &mut AnalyzerScope,
    ) -> Result<(), String> {
        let subqueries: Vec<SubqueryInfo> =
            self.collected_subqueries.borrow_mut().drain(..).collect();
        if subqueries.is_empty() {
            return Ok(());
        }

        for sq_info in subqueries {
            self.rewrite_single_subquery(select, scope, sq_info)?;
        }

        Ok(())
    }

    /// Rewrite a single subquery into a JOIN.
    fn rewrite_single_subquery(
        &self,
        select: &mut ResolvedSelect,
        scope: &mut AnalyzerScope,
        sq_info: SubqueryInfo,
    ) -> Result<(), String> {
        match &sq_info.kind {
            SubqueryKind::Exists { negated } => {
                let negated = *negated;
                self.rewrite_exists(select, scope, sq_info, negated)
            }
            SubqueryKind::InSubquery { negated } => {
                let negated = *negated;
                self.rewrite_in_subquery(select, scope, sq_info, negated)
            }
            SubqueryKind::Scalar => self.rewrite_scalar_subquery(select, scope, sq_info),
        }
    }

    // -----------------------------------------------------------------------
    // EXISTS / NOT EXISTS → LEFT SEMI / LEFT ANTI JOIN
    // -----------------------------------------------------------------------

    fn rewrite_exists(
        &self,
        select: &mut ResolvedSelect,
        scope: &mut AnalyzerScope,
        sq_info: SubqueryInfo,
        negated: bool,
    ) -> Result<(), String> {
        let (resolved, inner_scope) =
            self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;

        let join_type = if negated {
            JoinKind::LeftAnti
        } else {
            JoinKind::LeftSemi
        };

        // For EXISTS, the subquery FROM becomes the right side of the join.
        // The subquery WHERE is split into:
        //   - correlation predicates → join ON condition
        //   - remaining inner predicates → kept in the subquery WHERE
        let (sub_from, sub_filter) = match resolved.body {
            QueryBody::Select(sel) => (sel.from, sel.filter),
            _ => return Err("EXISTS subquery must be a SELECT".into()),
        };

        let sub_rel = sub_from.ok_or("EXISTS subquery must have a FROM clause")?;

        // Extract correlation predicates from the subquery WHERE
        let join_condition = if let Some(ref filter) = sub_filter {
            let corr_preds = extract_correlation_predicates(filter, &inner_scope, scope);
            if corr_preds.is_empty() {
                // No correlation — use full filter as join condition
                sub_filter
            } else {
                // Build join condition from correlation predicates
                let mut cond = TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: false,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(corr_preds[0].outer_col.clone()),
                        op: corr_preds[0].op,
                        right: Box::new(corr_preds[0].inner_col.clone()),
                    },
                };
                for pred in &corr_preds[1..] {
                    cond = TypedExpr {
                        data_type: DataType::Boolean,
                        nullable: false,
                        kind: ExprKind::BinaryOp {
                            left: Box::new(cond),
                            op: BinOp::And,
                            right: Box::new(TypedExpr {
                                data_type: DataType::Boolean,
                                nullable: false,
                                kind: ExprKind::BinaryOp {
                                    left: Box::new(pred.outer_col.clone()),
                                    op: pred.op,
                                    right: Box::new(pred.inner_col.clone()),
                                },
                            }),
                        },
                    };
                }
                // Also include non-correlation predicates from the filter
                let remaining = remove_correlation_preds_from_expr(filter, &corr_preds);
                if let Some(remaining) = remaining {
                    cond = TypedExpr {
                        data_type: DataType::Boolean,
                        nullable: false,
                        kind: ExprKind::BinaryOp {
                            left: Box::new(cond),
                            op: BinOp::And,
                            right: Box::new(remaining),
                        },
                    };
                }
                Some(cond)
            }
        } else {
            None
        };

        let current_from = select
            .from
            .take()
            .ok_or("EXISTS subquery rewrite requires a FROM clause")?;

        select.from = Some(Relation::Join(Box::new(JoinRelation {
            left: current_from,
            right: sub_rel,
            join_type,
            condition: join_condition,
        })));

        Self::remove_placeholder_from_filter(&mut select.filter, sq_info.id);
        Self::remove_placeholder_from_filter(&mut select.having, sq_info.id);

        Ok(())
    }

    // -----------------------------------------------------------------------
    // IN / NOT IN → LEFT SEMI / LEFT ANTI JOIN
    // -----------------------------------------------------------------------

    fn rewrite_in_subquery(
        &self,
        select: &mut ResolvedSelect,
        scope: &mut AnalyzerScope,
        sq_info: SubqueryInfo,
        negated: bool,
    ) -> Result<(), String> {
        let in_expr_ast = sq_info
            .in_expr
            .as_ref()
            .ok_or("IN subquery rewrite: missing left-hand expression")?;

        let lhs_typed = self.analyze_expr(in_expr_ast, scope)?;

        let resolved_sub = self.analyze_query_in_scope(&sq_info.subquery, scope)?;

        if resolved_sub.output_columns.is_empty() {
            return Err("IN subquery must produce at least one column".into());
        }
        let sub_output_col = resolved_sub.output_columns[0].clone();

        let join_type = if negated {
            JoinKind::LeftAnti
        } else {
            JoinKind::LeftSemi
        };

        let sq_alias = format!("__sq_{}", sq_info.id);
        let sub_rel = Relation::Subquery {
            query: Box::new(resolved_sub),
            alias: sq_alias.clone(),
        };

        // Use unqualified column ref for the right side of the join condition.
        // The physical planner resolves the right side against the subquery's
        // own scope (which contains the original table columns, not the __sq_N
        // alias), so a qualified reference like __sq_0.col would fail.
        let eq_cond = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(lhs_typed),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: None,
                        column: sub_output_col.name.clone(),
                    },
                    data_type: sub_output_col.data_type.clone(),
                    nullable: sub_output_col.nullable,
                }),
            },
        };

        scope.add_column(
            Some(&sq_alias),
            &sub_output_col.name,
            sub_output_col.data_type.clone(),
            sub_output_col.nullable,
        );

        let current_from = select
            .from
            .take()
            .ok_or("IN subquery rewrite requires a FROM clause")?;

        select.from = Some(Relation::Join(Box::new(JoinRelation {
            left: current_from,
            right: sub_rel,
            join_type,
            condition: Some(eq_cond),
        })));

        Self::remove_placeholder_from_filter(&mut select.filter, sq_info.id);
        Self::remove_placeholder_from_filter(&mut select.having, sq_info.id);

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Scalar subquery → CROSS JOIN (uncorrelated) or LEFT JOIN (correlated)
    // -----------------------------------------------------------------------

    fn rewrite_scalar_subquery(
        &self,
        select: &mut ResolvedSelect,
        scope: &mut AnalyzerScope,
        sq_info: SubqueryInfo,
    ) -> Result<(), String> {
        let sq_alias = format!("__sq_{}", sq_info.id);

        // Analyze the subquery. We get back (resolved, inner_scope) where
        // inner_scope is the scope derived from the subquery's own FROM clause.
        let (resolved_sub, inner_scope) =
            self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;

        if resolved_sub.output_columns.is_empty() {
            return Err("scalar subquery must produce at least one output column".into());
        }

        // Detect correlation by examining the subquery's WHERE for predicates
        // that reference columns present in the outer scope but NOT in the inner scope.
        let corr_preds = if let QueryBody::Select(ref sel) = resolved_sub.body {
            if let Some(ref filter) = sel.filter {
                extract_correlation_predicates(filter, &inner_scope, scope)
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let is_correlated = !corr_preds.is_empty();

        if is_correlated {
            let (modified_sub, corr_join_conds) = self.build_correlated_scalar_subquery(
                &sq_info.subquery,
                scope,
                &sq_alias,
                &corr_preds,
            )?;

            let scalar_output_name = modified_sub.output_columns[0].name.clone();
            let scalar_data_type = modified_sub.output_columns[0].data_type.clone();
            let scalar_nullable = true;

            let sub_rel = Relation::Subquery {
                query: Box::new(modified_sub),
                alias: sq_alias.clone(),
            };

            scope.add_column(
                Some(&sq_alias),
                &scalar_output_name,
                scalar_data_type.clone(),
                scalar_nullable,
            );

            let current_from = select
                .from
                .take()
                .ok_or("scalar subquery rewrite requires a FROM clause")?;

            select.from = Some(Relation::Join(Box::new(JoinRelation {
                left: current_from,
                right: sub_rel,
                join_type: JoinKind::LeftOuter,
                condition: corr_join_conds,
            })));

            // Use unqualified column ref for the replacement expression.
            // The physical planner's scope for the joined subquery uses the
            // original table names, not the __sq_N alias.
            let replacement = TypedExpr {
                kind: ExprKind::ColumnRef {
                    qualifier: None,
                    column: scalar_output_name,
                },
                data_type: scalar_data_type,
                nullable: scalar_nullable,
            };
            Self::replace_placeholder_in_filter(&mut select.filter, sq_info.id, &replacement);
            Self::replace_placeholder_in_filter(&mut select.having, sq_info.id, &replacement);
            Self::replace_placeholder_in_projection(
                &mut select.projection,
                sq_info.id,
                &replacement,
            );
        } else {
            let scalar_col = resolved_sub.output_columns[0].clone();
            let sub_rel = Relation::Subquery {
                query: Box::new(resolved_sub),
                alias: sq_alias.clone(),
            };

            scope.add_column(
                Some(&sq_alias),
                &scalar_col.name,
                scalar_col.data_type.clone(),
                scalar_col.nullable,
            );

            let current_from = select
                .from
                .take()
                .ok_or("scalar subquery rewrite requires a FROM clause")?;

            select.from = Some(Relation::Join(Box::new(JoinRelation {
                left: current_from,
                right: sub_rel,
                join_type: JoinKind::Cross,
                condition: None,
            })));

            // Use unqualified column ref for the replacement expression.
            let replacement = TypedExpr {
                kind: ExprKind::ColumnRef {
                    qualifier: None,
                    column: scalar_col.name.clone(),
                },
                data_type: scalar_col.data_type.clone(),
                nullable: scalar_col.nullable,
            };
            Self::replace_placeholder_in_filter(&mut select.filter, sq_info.id, &replacement);
            Self::replace_placeholder_in_filter(&mut select.having, sq_info.id, &replacement);
            Self::replace_placeholder_in_projection(
                &mut select.projection,
                sq_info.id,
                &replacement,
            );
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Subquery analysis helpers
    // -----------------------------------------------------------------------

    /// Analyze a query in the context of an outer scope, allowing correlated references.
    fn analyze_query_in_scope(
        &self,
        query: &sqlparser::ast::Query,
        outer_scope: &AnalyzerScope,
    ) -> Result<ResolvedQuery, String> {
        let (resolved, _inner_scope) =
            self.analyze_query_in_scope_with_inner(query, outer_scope)?;
        Ok(resolved)
    }

    /// Analyze a query with outer scope, also returning the inner scope.
    fn analyze_query_in_scope_with_inner(
        &self,
        query: &sqlparser::ast::Query,
        outer_scope: &AnalyzerScope,
    ) -> Result<(ResolvedQuery, AnalyzerScope), String> {
        let child_ctx = AnalyzerContext {
            catalog: self.catalog,
            current_database: self.current_database,
            ctes: self.ctes.clone(),
            next_subquery_id: std::cell::Cell::new(self.next_subquery_id.get()),
            collected_subqueries: std::cell::RefCell::new(Vec::new()),
            shared_cte_ids: self.shared_cte_ids.clone(),
            cte_registry: std::cell::RefCell::new(self.cte_registry.borrow().clone()),
        };

        let result = child_ctx.analyze_query_with_outer_scope_inner(query, outer_scope)?;

        self.next_subquery_id.set(child_ctx.next_subquery_id.get());

        let nested_sqs: Vec<SubqueryInfo> = child_ctx
            .collected_subqueries
            .borrow_mut()
            .drain(..)
            .collect();
        if !nested_sqs.is_empty() {
            let resolved = self.rewrite_nested_subqueries(result.0, nested_sqs, outer_scope)?;
            return Ok((resolved, result.1));
        }

        Ok(result)
    }

    /// Analyze a query that can reference columns from an outer scope.
    /// Returns (ResolvedQuery, inner_scope_from_FROM_clause).
    fn analyze_query_with_outer_scope_inner(
        &self,
        query: &sqlparser::ast::Query,
        outer_scope: &AnalyzerScope,
    ) -> Result<(ResolvedQuery, AnalyzerScope), String> {
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
                shared_cte_ids: self.shared_cte_ids.clone(),
                cte_registry: std::cell::RefCell::new(self.cte_registry.borrow().clone()),
            };
            &child_ctx
        } else {
            self
        };

        let body = query.body.as_ref();
        match body {
            sqlparser::ast::SetExpr::Select(s) => {
                let (sel, cols, inner_scope) =
                    ctx.analyze_select_with_outer_scope(s, outer_scope)?;
                let body = QueryBody::Select(sel);

                let order_by = ctx.analyze_order_by(query, &cols, &body)?;
                let limit = super::helpers::extract_limit(query)?;
                let offset = super::helpers::extract_offset(query)?;

                Ok((
                    ResolvedQuery {
                        body,
                        order_by,
                        limit,
                        offset,
                        output_columns: cols,
                    },
                    inner_scope,
                ))
            }
            _ => {
                let resolved = ctx.analyze_query(query)?;
                Ok((resolved, AnalyzerScope::new()))
            }
        }
    }

    /// Analyze a SELECT that can reference outer scope columns for correlation.
    /// Returns (ResolvedSelect, output_columns, inner_scope).
    fn analyze_select_with_outer_scope(
        &self,
        select: &sqlparser::ast::Select,
        outer_scope: &AnalyzerScope,
    ) -> Result<(ResolvedSelect, Vec<OutputColumn>, AnalyzerScope), String> {
        use sqlparser::ast as sqlast;

        // --- FROM clause ---
        let (from, inner_scope) = if select.from.is_empty() {
            (None, AnalyzerScope::new())
        } else if select.from.len() == 1 {
            let (rel, scope) = self.analyze_from(&select.from[0])?;
            (Some(rel), scope)
        } else {
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

        // Merged scope: inner tables first (higher priority), then outer scope for fallback
        let mut merged_scope = inner_scope.clone();
        merged_scope.merge(outer_scope);

        // --- WHERE clause ---
        let filter = match &select.selection {
            Some(expr) => Some(self.analyze_expr(expr, &merged_scope)?),
            None => None,
        };

        // --- SELECT list ---
        let (projection, output_columns) =
            self.analyze_projection(&select.projection, &merged_scope)?;

        // --- GROUP BY ---
        let group_by_exprs = match &select.group_by {
            sqlast::GroupByExpr::Expressions(exprs, _) => exprs.clone(),
            sqlast::GroupByExpr::All(_) => {
                return Err("GROUP BY ALL is not supported".into());
            }
        };
        let mut group_by = Vec::with_capacity(group_by_exprs.len());
        for gb_expr in &group_by_exprs {
            match self.analyze_expr(gb_expr, &merged_scope) {
                Ok(typed) => group_by.push(typed),
                Err(_) => {
                    let mut alias_scope = merged_scope.clone();
                    for item in &projection {
                        alias_scope.add_column(
                            None,
                            &item.output_name,
                            item.expr.data_type.clone(),
                            item.expr.nullable,
                        );
                    }
                    let typed = self.analyze_expr(gb_expr, &alias_scope)?;
                    group_by.push(self.substitute_select_aliases(typed, &projection));
                }
            }
        }

        // --- Detect aggregation ---
        let has_agg_in_select = self.select_has_aggregate_functions(&select.projection);
        let has_aggregation = !group_by.is_empty() || has_agg_in_select;

        // --- HAVING ---
        let having = match &select.having {
            Some(expr) => {
                let analyzed = self.analyze_expr(expr, &merged_scope);
                match analyzed {
                    Ok(h) => Some(h),
                    Err(_) => {
                        let mut alias_scope = merged_scope.clone();
                        for item in &projection {
                            alias_scope.add_column(
                                None,
                                &item.output_name,
                                item.expr.data_type.clone(),
                                item.expr.nullable,
                            );
                        }
                        let h = self.analyze_expr(expr, &alias_scope)?;
                        Some(self.substitute_select_aliases(h, &projection))
                    }
                }
            }
            None => None,
        };

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
        };

        // Rewrite nested subqueries within this SELECT if any were collected
        let nested_sqs: Vec<SubqueryInfo> =
            self.collected_subqueries.borrow_mut().drain(..).collect();
        if !nested_sqs.is_empty() {
            let mut mutable_inner = inner_scope.clone();
            for sq_info in nested_sqs {
                self.rewrite_single_subquery(&mut resolved_select, &mut mutable_inner, sq_info)?;
            }
        }

        Ok((resolved_select, output_columns, inner_scope))
    }

    /// Build a correlated scalar subquery as a derived table with GROUP BY
    /// on the correlation keys.
    fn build_correlated_scalar_subquery(
        &self,
        subquery: &sqlparser::ast::Query,
        outer_scope: &AnalyzerScope,
        sq_alias: &str,
        correlated_cols: &[CorrelationPred],
    ) -> Result<(ResolvedQuery, Option<TypedExpr>), String> {
        let resolved = self.analyze_query_in_scope(subquery, outer_scope)?;

        let mut join_conds: Vec<TypedExpr> = Vec::new();
        let mut extra_group_by: Vec<TypedExpr> = Vec::new();
        let mut extra_output: Vec<OutputColumn> = Vec::new();
        let mut extra_projection: Vec<ProjectItem> = Vec::new();

        for (idx, pred) in correlated_cols.iter().enumerate() {
            let inner_col = &pred.inner_col;
            let outer_col = &pred.outer_col;

            extra_group_by.push(inner_col.clone());

            let col_name = match &inner_col.kind {
                ExprKind::ColumnRef { column, .. } => column.clone(),
                _ => format!("__corr_key_{}", idx),
            };
            extra_output.push(OutputColumn {
                name: col_name.clone(),
                data_type: inner_col.data_type.clone(),
                nullable: inner_col.nullable,
            });
            extra_projection.push(ProjectItem {
                expr: inner_col.clone(),
                output_name: col_name.clone(),
            });

            // Use unqualified column ref for the right side of the join condition.
            // The physical planner resolves the right side against the subquery's
            // own scope, which uses the original table names, not __sq_N.
            join_conds.push(TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    left: Box::new(outer_col.clone()),
                    op: pred.op,
                    right: Box::new(TypedExpr {
                        kind: ExprKind::ColumnRef {
                            qualifier: None,
                            column: col_name,
                        },
                        data_type: inner_col.data_type.clone(),
                        nullable: inner_col.nullable,
                    }),
                },
            });
        }

        let mut modified = resolved;
        if let QueryBody::Select(ref mut sel) = modified.body {
            for gb in &extra_group_by {
                sel.group_by.push(gb.clone());
            }
            sel.has_aggregation = true;

            for proj in &extra_projection {
                sel.projection.push(proj.clone());
            }

            if let Some(ref filter) = sel.filter {
                let remaining = remove_correlation_preds_from_expr(filter, correlated_cols);
                sel.filter = remaining;
            }
        }
        for out_col in &extra_output {
            modified.output_columns.push(out_col.clone());
        }

        let join_cond = if join_conds.is_empty() {
            None
        } else {
            Some(conjoin(join_conds))
        };

        Ok((modified, join_cond))
    }

    /// Rewrite nested subqueries within an already-resolved query.
    fn rewrite_nested_subqueries(
        &self,
        mut resolved: ResolvedQuery,
        nested_sqs: Vec<SubqueryInfo>,
        outer_scope: &AnalyzerScope,
    ) -> Result<ResolvedQuery, String> {
        if let QueryBody::Select(ref mut sel) = resolved.body {
            let mut scope = AnalyzerScope::new();
            if let Some(ref from_rel) = sel.from {
                self.collect_relation_scope(from_rel, &mut scope)?;
            }
            scope.merge(outer_scope);

            for sq_info in nested_sqs {
                self.rewrite_single_subquery(sel, &mut scope, sq_info)?;
            }
        }
        Ok(resolved)
    }

    // -----------------------------------------------------------------------
    // Placeholder manipulation in expression trees
    // -----------------------------------------------------------------------

    fn remove_placeholder_from_filter(filter: &mut Option<TypedExpr>, placeholder_id: usize) {
        let should_clear = if let Some(expr) = filter.as_ref() {
            is_placeholder(expr, placeholder_id)
        } else {
            false
        };
        if should_clear {
            *filter = None;
            return;
        }
        if let Some(expr) = filter.as_ref() {
            let new_expr = remove_placeholder_from_expr(expr, placeholder_id);
            *filter = Some(new_expr);
        }
    }

    fn replace_placeholder_in_filter(
        filter: &mut Option<TypedExpr>,
        placeholder_id: usize,
        replacement: &TypedExpr,
    ) {
        if let Some(expr) = filter.as_ref() {
            let new_expr = replace_placeholder_in_expr(expr, placeholder_id, replacement);
            *filter = Some(new_expr);
        }
    }

    /// Replace subquery placeholders in projection items (SELECT list).
    /// This handles scalar subqueries that appear in the SELECT list
    /// (e.g., TPC-DS q9: CASE WHEN (SELECT ...) > N THEN (SELECT ...) ELSE (SELECT ...) END).
    fn replace_placeholder_in_projection(
        projection: &mut [ProjectItem],
        placeholder_id: usize,
        replacement: &TypedExpr,
    ) {
        for item in projection.iter_mut() {
            item.expr = replace_placeholder_in_expr(&item.expr, placeholder_id, replacement);
        }
    }
}

// ---------------------------------------------------------------------------
// Correlation predicate detection
// ---------------------------------------------------------------------------

/// Represents a detected correlation between outer and inner query columns.
#[derive(Clone, Debug)]
pub(super) struct CorrelationPred {
    /// The outer column reference (belongs to outer scope only).
    pub outer_col: TypedExpr,
    /// The inner column reference (belongs to subquery inner scope).
    pub inner_col: TypedExpr,
    /// The comparison operator.
    pub op: BinOp,
    /// The full expression (for structural equality matching during removal).
    pub full_expr: TypedExpr,
}

/// Extract correlation predicates from an expression.
/// A correlation predicate is an equality (or comparison) where one side
/// references an outer-scope column (resolves in outer_scope but NOT in inner_scope)
/// and the other side references an inner-scope column.
fn extract_correlation_predicates(
    expr: &TypedExpr,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> Vec<CorrelationPred> {
    let mut result = Vec::new();
    extract_corr_preds_inner(expr, inner_scope, outer_scope, &mut result);
    result
}

fn extract_corr_preds_inner(
    expr: &TypedExpr,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
    out: &mut Vec<CorrelationPred>,
) {
    match &expr.kind {
        ExprKind::BinaryOp { left, op, right } => match op {
            BinOp::And | BinOp::Or => {
                extract_corr_preds_inner(left, inner_scope, outer_scope, out);
                extract_corr_preds_inner(right, inner_scope, outer_scope, out);
            }
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                let left_outer_only = is_outer_only_ref(left, inner_scope, outer_scope);
                let right_outer_only = is_outer_only_ref(right, inner_scope, outer_scope);

                if left_outer_only && !right_outer_only {
                    out.push(CorrelationPred {
                        outer_col: *left.clone(),
                        inner_col: *right.clone(),
                        op: *op,
                        full_expr: expr.clone(),
                    });
                } else if !left_outer_only && right_outer_only {
                    let rev_op = match op {
                        BinOp::Eq => BinOp::Eq,
                        BinOp::Ne => BinOp::Ne,
                        BinOp::Lt => BinOp::Gt,
                        BinOp::Le => BinOp::Ge,
                        BinOp::Gt => BinOp::Lt,
                        BinOp::Ge => BinOp::Le,
                        _ => *op,
                    };
                    out.push(CorrelationPred {
                        outer_col: *right.clone(),
                        inner_col: *left.clone(),
                        op: rev_op,
                        full_expr: expr.clone(),
                    });
                }
            }
            _ => {}
        },
        ExprKind::Nested(inner) => {
            extract_corr_preds_inner(inner, inner_scope, outer_scope, out);
        }
        _ => {}
    }
}

/// Check if an expression is a reference to a column that exists in the outer scope
/// but NOT in the inner scope. This identifies true correlation references.
fn is_outer_only_ref(
    expr: &TypedExpr,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> bool {
    match &expr.kind {
        ExprKind::ColumnRef { qualifier, column } => {
            let in_inner = inner_scope.resolve(qualifier.as_deref(), column).is_ok();
            let in_outer = outer_scope.resolve(qualifier.as_deref(), column).is_ok();
            // Outer-only: in outer but not in inner
            !in_inner && in_outer
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Expression tree manipulation
// ---------------------------------------------------------------------------

fn is_placeholder(expr: &TypedExpr, id: usize) -> bool {
    matches!(&expr.kind, ExprKind::SubqueryPlaceholder { id: pid, .. } if *pid == id)
}

fn remove_placeholder_from_expr(expr: &TypedExpr, placeholder_id: usize) -> TypedExpr {
    match &expr.kind {
        ExprKind::BinaryOp { left, op, right } if matches!(op, BinOp::And | BinOp::Or) => {
            let identity = matches!(op, BinOp::And); // AND identity = true, OR identity = false
            let left_is = is_placeholder(left, placeholder_id);
            let right_is = is_placeholder(right, placeholder_id);
            if left_is && right_is {
                TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Bool(identity)),
                    data_type: DataType::Boolean,
                    nullable: false,
                }
            } else if left_is {
                remove_placeholder_from_expr(right, placeholder_id)
            } else if right_is {
                remove_placeholder_from_expr(left, placeholder_id)
            } else {
                let new_left = remove_placeholder_from_expr(left, placeholder_id);
                let new_right = remove_placeholder_from_expr(right, placeholder_id);
                TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: false,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(new_left),
                        op: *op,
                        right: Box::new(new_right),
                    },
                }
            }
        }
        ExprKind::Nested(inner) => {
            if is_placeholder(inner, placeholder_id) {
                TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Bool(true)),
                    data_type: DataType::Boolean,
                    nullable: false,
                }
            } else {
                let new_inner = remove_placeholder_from_expr(inner, placeholder_id);
                TypedExpr {
                    data_type: expr.data_type.clone(),
                    nullable: expr.nullable,
                    kind: ExprKind::Nested(Box::new(new_inner)),
                }
            }
        }
        _ => expr.clone(),
    }
}

fn replace_placeholder_in_expr(
    expr: &TypedExpr,
    placeholder_id: usize,
    replacement: &TypedExpr,
) -> TypedExpr {
    if is_placeholder(expr, placeholder_id) {
        return replacement.clone();
    }

    match &expr.kind {
        ExprKind::BinaryOp { left, op, right } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(replace_placeholder_in_expr(
                    left,
                    placeholder_id,
                    replacement,
                )),
                op: *op,
                right: Box::new(replace_placeholder_in_expr(
                    right,
                    placeholder_id,
                    replacement,
                )),
            },
        },
        ExprKind::UnaryOp { op, expr: inner } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::UnaryOp {
                op: *op,
                expr: Box::new(replace_placeholder_in_expr(
                    inner,
                    placeholder_id,
                    replacement,
                )),
            },
        },
        ExprKind::Nested(inner) => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Nested(Box::new(replace_placeholder_in_expr(
                inner,
                placeholder_id,
                replacement,
            ))),
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
                    .map(|a| replace_placeholder_in_expr(a, placeholder_id, replacement))
                    .collect(),
                distinct: *distinct,
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
                    .map(|a| replace_placeholder_in_expr(a, placeholder_id, replacement))
                    .collect(),
                distinct: *distinct,
                order_by: order_by.clone(),
            },
        },
        ExprKind::Cast {
            expr: inner,
            target,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Cast {
                expr: Box::new(replace_placeholder_in_expr(
                    inner,
                    placeholder_id,
                    replacement,
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
                expr: Box::new(replace_placeholder_in_expr(
                    inner,
                    placeholder_id,
                    replacement,
                )),
                negated: *negated,
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
                operand: operand
                    .as_ref()
                    .map(|o| Box::new(replace_placeholder_in_expr(o, placeholder_id, replacement))),
                when_then: when_then
                    .iter()
                    .map(|(w, t)| {
                        (
                            replace_placeholder_in_expr(w, placeholder_id, replacement),
                            replace_placeholder_in_expr(t, placeholder_id, replacement),
                        )
                    })
                    .collect(),
                else_expr: else_expr
                    .as_ref()
                    .map(|e| Box::new(replace_placeholder_in_expr(e, placeholder_id, replacement))),
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
                expr: Box::new(replace_placeholder_in_expr(
                    inner,
                    placeholder_id,
                    replacement,
                )),
                low: Box::new(replace_placeholder_in_expr(
                    low,
                    placeholder_id,
                    replacement,
                )),
                high: Box::new(replace_placeholder_in_expr(
                    high,
                    placeholder_id,
                    replacement,
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
                expr: Box::new(replace_placeholder_in_expr(
                    inner,
                    placeholder_id,
                    replacement,
                )),
                pattern: Box::new(replace_placeholder_in_expr(
                    pattern,
                    placeholder_id,
                    replacement,
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
                expr: Box::new(replace_placeholder_in_expr(
                    inner,
                    placeholder_id,
                    replacement,
                )),
                list: list
                    .iter()
                    .map(|a| replace_placeholder_in_expr(a, placeholder_id, replacement))
                    .collect(),
                negated: *negated,
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
                expr: Box::new(replace_placeholder_in_expr(
                    inner,
                    placeholder_id,
                    replacement,
                )),
                value: *value,
                negated: *negated,
            },
        },
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
        } => TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::WindowCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| replace_placeholder_in_expr(a, placeholder_id, replacement))
                    .collect(),
                distinct: *distinct,
                partition_by: partition_by
                    .iter()
                    .map(|p| replace_placeholder_in_expr(p, placeholder_id, replacement))
                    .collect(),
                order_by: order_by.clone(),
                window_frame: window_frame.clone(),
            },
        },
        _ => expr.clone(),
    }
}

/// Remove correlation predicates from an expression, returning the remaining parts.
fn remove_correlation_preds_from_expr(
    expr: &TypedExpr,
    corr_preds: &[CorrelationPred],
) -> Option<TypedExpr> {
    for pred in corr_preds {
        if exprs_structurally_equal(expr, &pred.full_expr) {
            return None;
        }
    }

    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let left_remaining = remove_correlation_preds_from_expr(left, corr_preds);
            let right_remaining = remove_correlation_preds_from_expr(right, corr_preds);
            match (left_remaining, right_remaining) {
                (Some(l), Some(r)) => Some(TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: false,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(l),
                        op: BinOp::And,
                        right: Box::new(r),
                    },
                }),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        _ => Some(expr.clone()),
    }
}

fn exprs_structurally_equal(a: &TypedExpr, b: &TypedExpr) -> bool {
    format!("{:?}", a.kind) == format!("{:?}", b.kind)
}

fn conjoin(mut exprs: Vec<TypedExpr>) -> TypedExpr {
    assert!(!exprs.is_empty());
    if exprs.len() == 1 {
        return exprs.pop().unwrap();
    }
    let first = exprs.remove(0);
    exprs.into_iter().fold(first, |acc, e| TypedExpr {
        data_type: DataType::Boolean,
        nullable: false,
        kind: ExprKind::BinaryOp {
            left: Box::new(acc),
            op: BinOp::And,
            right: Box::new(e),
        },
    })
}
