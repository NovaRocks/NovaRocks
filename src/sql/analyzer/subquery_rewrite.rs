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

use crate::sql::analysis::*;

use super::AnalyzerContext;
use super::scope::AnalyzerScope;

/// Take the outer SELECT's FROM relation, or synthesize a single-row
/// "dummy" relation when the SELECT has no FROM clause.
///
/// A SELECT whose only sources are scalar subqueries (e.g.
/// `SELECT (SELECT count(*) FROM t1) AS a, (SELECT max(x) FROM t2) AS b`)
/// arrives here with `from = None`. The rewriter normally turns each
/// scalar subquery into a CROSS / LEFT OUTER JOIN against the existing
/// outer FROM; without an outer FROM the join has no left child. Since
/// SQL semantics for a from-less SELECT are "evaluate the projection
/// over a single virtual row", we synthesize that single row as
/// `generate_series(1, 1)` so the join below has a valid left side.
/// `GenerateSeries` is already in the analyzer's `Relation` vocabulary
/// and lowers to a simple 1-row source operator.
fn take_from_or_synthesize_single_row(from: &mut Option<Relation>) -> Relation {
    from.take().unwrap_or_else(|| {
        Relation::GenerateSeries(GenerateSeriesRelation {
            start: 1,
            end: 1,
            step: 1,
            column_name: "__nr_subquery_join_dummy".to_string(),
            alias: None,
        })
    })
}

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
            // Subqueries can appear in three locations:
            //   1. WHERE / HAVING (the original path)
            //   2. JOIN ... ON clauses inside `select.from`
            //   3. Inside projection items (rare, handled by the scalar
            //      rewrite which also touches projection)
            //
            // The existing rewrite functions (`rewrite_exists` /
            // `rewrite_in_subquery` / `rewrite_scalar_subquery`) all assume
            // the placeholder lives in `select.filter` / `select.having`.
            // For JOIN-ON placeholders we need to do the rewrite locally on
            // the containing JoinRelation: pre-compute the subquery as a
            // derived table, attach it as a LEFT OUTER JOIN to the host
            // join's left input, and replace the placeholder with a
            // match-indicator expression. We handle the JOIN-ON case
            // first; if no JOIN-ON match is found, fall through to the
            // original WHERE/HAVING path.
            let in_filter = select
                .filter
                .as_ref()
                .map(|f| expr_contains_placeholder(f, sq_info.id))
                .unwrap_or(false);
            let in_having = select
                .having
                .as_ref()
                .map(|f| expr_contains_placeholder(f, sq_info.id))
                .unwrap_or(false);
            if !in_filter && !in_having && let Some(from) = select.from.as_mut() {
                let id = sq_info.id;
                if self.rewrite_subquery_in_relation(from, scope, &sq_info)? {
                    // Placeholder dispatched to JOIN-ON rewrite.
                    debug_assert!(!expr_contains_placeholder_in_relation(from, id));
                    continue;
                }
            }
            self.rewrite_single_subquery(select, scope, sq_info)?;
        }

        Ok(())
    }

    /// Walk a Relation tree looking for a JoinRelation whose `condition`
    /// contains the subquery placeholder. If found, rewrite it in place
    /// (wrapping the join's left input with a LEFT OUTER JOIN against the
    /// subquery, and replacing the placeholder with a match-indicator
    /// expression). Returns Ok(true) if the placeholder was found and
    /// rewritten.
    fn rewrite_subquery_in_relation(
        &self,
        rel: &mut Relation,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
    ) -> Result<bool, String> {
        match rel {
            Relation::Join(join_box) => {
                if self.rewrite_subquery_in_relation(&mut join_box.left, scope, sq_info)? {
                    return Ok(true);
                }
                if self.rewrite_subquery_in_relation(&mut join_box.right, scope, sq_info)? {
                    return Ok(true);
                }
                let has_placeholder = join_box
                    .condition
                    .as_ref()
                    .map(|c| expr_contains_placeholder(c, sq_info.id))
                    .unwrap_or(false);
                if !has_placeholder {
                    return Ok(false);
                }
                self.rewrite_join_on_subquery(join_box, scope, sq_info)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Rewrite a single subquery placeholder living inside a JoinRelation's
    /// ON clause. The placeholder is replaced with either:
    /// - For uncorrelated IN: `__sq_alias.match IS NOT NULL` (or `IS NULL`
    ///   for NOT IN), backed by a LEFT OUTER JOIN against `SELECT DISTINCT
    ///   col FROM subquery` added to the host join's left input.
    /// - For uncorrelated EXISTS: a constant boolean (or for NOT EXISTS).
    ///   We add a LEFT OUTER JOIN against the subquery limited to one row
    ///   and use `match IS NOT NULL`.
    /// - For uncorrelated scalar: a CROSS JOIN exposing the scalar as a
    ///   single-row column, plus a ColumnRef replacement.
    ///
    /// Correlated JOIN-ON subqueries are not yet supported and surface as
    /// the original "unexpected SubqueryPlaceholder" codegen error.
    fn rewrite_join_on_subquery(
        &self,
        join: &mut JoinRelation,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
    ) -> Result<(), String> {
        let (resolved_sub, inner_scope) =
            self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;

        let is_correlated = match resolved_sub.body {
            QueryBody::Select(ref sel) => sel
                .filter
                .as_ref()
                .map(|f| !extract_correlation_predicates(f, &inner_scope, scope).is_empty())
                .unwrap_or(false),
            _ => false,
        };

        let sq_alias = format!("__sq_on_{}", sq_info.id);

        if is_correlated {
            return match &sq_info.kind {
                SubqueryKind::InSubquery { negated } => self
                    .rewrite_join_on_in_subquery_correlated(
                        join,
                        scope,
                        sq_info,
                        resolved_sub,
                        sq_alias,
                        *negated,
                    ),
                SubqueryKind::Exists { negated } => self.rewrite_join_on_exists_correlated(
                    join,
                    scope,
                    sq_info,
                    resolved_sub,
                    sq_alias,
                    *negated,
                ),
                SubqueryKind::Scalar => self.rewrite_join_on_scalar_correlated(
                    join,
                    scope,
                    sq_info,
                    resolved_sub,
                    sq_alias,
                ),
            };
        }

        match &sq_info.kind {
            SubqueryKind::InSubquery { negated } => self.rewrite_join_on_in_subquery(
                join,
                scope,
                sq_info,
                resolved_sub,
                sq_alias,
                *negated,
            ),
            SubqueryKind::Exists { negated } => self.rewrite_join_on_exists(
                join,
                scope,
                sq_info,
                resolved_sub,
                sq_alias,
                *negated,
            ),
            SubqueryKind::Scalar => {
                self.rewrite_join_on_scalar(join, scope, sq_info, resolved_sub, sq_alias)
            }
        }
    }

    /// Correlated IN inside a JOIN ON clause. Extract the subquery's FROM
    /// and lift the WHERE (which contains the correlation predicate) up
    /// into the auxiliary LEFT OUTER JOIN's ON clause. The match-indicator
    /// is a non-null literal projected by the subquery, so the placeholder
    /// becomes `__match IS [NOT] NULL`.
    fn rewrite_join_on_in_subquery_correlated(
        &self,
        join: &mut JoinRelation,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
        resolved_sub: ResolvedQuery,
        sq_alias: String,
        negated: bool,
    ) -> Result<(), String> {
        let in_expr_ast = sq_info
            .in_expr
            .as_ref()
            .ok_or("IN subquery rewrite (JOIN ON, correlated): missing left-hand expression")?;
        let lhs_typed = self.analyze_expr(in_expr_ast, scope)?;

        let (sub_from, sub_filter) = match resolved_sub.body {
            QueryBody::Select(sel) => (sel.from, sel.filter),
            _ => return Err("correlated IN subquery must be a SELECT".into()),
        };
        let sub_first_col = resolved_sub
            .output_columns
            .first()
            .ok_or("IN subquery must produce at least one column")?
            .clone();
        let sub_rel = sub_from
            .ok_or("correlated IN subquery must have a FROM clause".to_string())?;

        // Build the equality condition plus the lifted WHERE.
        let eq_cond = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(lhs_typed.clone()),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: None,
                        column: sub_first_col.name.clone(),
                    },
                    data_type: sub_first_col.data_type.clone(),
                    nullable: sub_first_col.nullable,
                }),
            },
        };
        let join_cond = match sub_filter.clone() {
            Some(f) => Some(TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    left: Box::new(eq_cond),
                    op: BinOp::And,
                    right: Box::new(f),
                },
            }),
            None => Some(eq_cond),
        };

        // Choose which side of the host join to attach the auxiliary
        // join to, based on which side carries the correlation column.
        let mut corr_exprs: Vec<TypedExpr> = vec![lhs_typed];
        if let Some(f) = sub_filter.as_ref() {
            corr_exprs.push(f.clone());
        }
        let side = choose_aux_join_side(join, &corr_exprs);
        attach_aux_join(join, side, sub_rel, join_cond);

        // The placeholder evaluates by checking the subquery's first column
        // (now exposed on the auxiliary join's output via LEFT OUTER JOIN).
        let replacement = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: None,
                        column: sub_first_col.name.clone(),
                    },
                    data_type: sub_first_col.data_type.clone(),
                    nullable: true,
                }),
                negated: !negated,
            },
        };
        if let Some(cond) = join.condition.as_ref() {
            join.condition = Some(replace_placeholder_in_expr(cond, sq_info.id, &replacement));
        }

        // Expose subquery columns in the outer scope so downstream
        // references (e.g. ORDER BY on subquery column, though uncommon)
        // resolve.
        let _ = sq_alias; // sq_alias unused for unwrapped FROM
        Ok(())
    }

    /// Correlated EXISTS inside JOIN ON. Lift sub-FROM and sub-WHERE
    /// (which has correlation) into the auxiliary LEFT OUTER JOIN ON.
    /// Placeholder becomes `<inner_col> IS [NOT] NULL`.
    fn rewrite_join_on_exists_correlated(
        &self,
        join: &mut JoinRelation,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
        resolved_sub: ResolvedQuery,
        sq_alias: String,
        negated: bool,
    ) -> Result<(), String> {
        let (sub_from, sub_filter) = match resolved_sub.body {
            QueryBody::Select(sel) => (sel.from, sel.filter),
            _ => return Err("correlated EXISTS subquery must be a SELECT".into()),
        };
        // Pick the first projection column as the match indicator. For
        // EXISTS with arbitrary projection this is fine — we just need a
        // non-null indicator when a matching row exists.
        let indicator = resolved_sub
            .output_columns
            .first()
            .ok_or("EXISTS subquery must produce at least one column")?
            .clone();
        let sub_rel = sub_from
            .ok_or("correlated EXISTS subquery must have a FROM clause".to_string())?;

        let side = match sub_filter.as_ref() {
            Some(f) => choose_aux_join_side(join, &[f.clone()]),
            None => AuxJoinSide::Left,
        };
        attach_aux_join(join, side, sub_rel, sub_filter);

        let replacement = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: None,
                        column: indicator.name.clone(),
                    },
                    data_type: indicator.data_type.clone(),
                    nullable: true,
                }),
                negated: !negated,
            },
        };
        if let Some(cond) = join.condition.as_ref() {
            join.condition = Some(replace_placeholder_in_expr(cond, sq_info.id, &replacement));
        }
        let _ = sq_alias;
        Ok(())
    }

    /// Correlated scalar subquery inside JOIN ON. The subquery returns one
    /// value per outer row; if the subquery is an aggregate (e.g.
    /// `(SELECT count(*) FROM t WHERE pred(outer))`) we still emit a LEFT
    /// OUTER JOIN against its FROM with the correlation predicate hoisted
    /// into ON, then the placeholder becomes a reference to the aggregated
    /// projection column.
    fn rewrite_join_on_scalar_correlated(
        &self,
        join: &mut JoinRelation,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
        resolved_sub: ResolvedQuery,
        sq_alias: String,
    ) -> Result<(), String> {
        // For correlated scalar (typically `SELECT agg(...) FROM t WHERE
        // <correlated>`), we re-wrap the subquery as a Subquery relation
        // but pre-extract the correlation predicate up into a LEFT OUTER
        // JOIN's ON, similar to the WHERE-clause path
        // (`build_correlated_scalar_subquery_from_resolved`). That helper
        // builds a per-correlation-key aggregate, which is what we want.
        // Reuse it.
        if resolved_sub.output_columns.is_empty() {
            return Err("correlated scalar subquery must produce at least one column".into());
        }
        let inner_scope_filter = match resolved_sub.body {
            QueryBody::Select(ref s) => s.filter.clone(),
            _ => None,
        };
        let inner_scope = match resolved_sub.body {
            QueryBody::Select(_) => {
                // Re-derive the inner scope from the subquery's analyzed FROM.
                // For simplicity, recompute via `analyze_query_in_scope_with_inner`.
                let (_, scope) = self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;
                scope
            }
            _ => return Err("correlated scalar subquery must be a SELECT".into()),
        };
        let corr_preds = match (&inner_scope_filter, &resolved_sub.body) {
            (Some(filter), QueryBody::Select(_)) => {
                extract_correlation_predicates(filter, &inner_scope, scope)
            }
            _ => vec![],
        };
        let outer_corr_exprs: Vec<TypedExpr> =
            corr_preds.iter().map(|p| p.outer_col.clone()).collect();
        let (modified_sub, corr_join_conds) = self
            .build_correlated_scalar_subquery_from_resolved(
                resolved_sub,
                scope,
                &sq_alias,
                &corr_preds,
            )?;
        let scalar_output = modified_sub.output_columns[0].clone();
        let output_columns = modified_sub.output_columns.clone();
        let sub_rel = Relation::Subquery {
            query: Box::new(modified_sub),
            alias: sq_alias.clone(),
            output_columns,
        };
        scope.add_column(
            Some(&sq_alias),
            &scalar_output.name,
            scalar_output.data_type.clone(),
            true,
        );

        let side = choose_aux_join_side(join, &outer_corr_exprs);
        attach_aux_join(join, side, sub_rel, corr_join_conds);

        let replacement = TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: Some(sq_alias),
                column: scalar_output.name,
            },
            data_type: scalar_output.data_type,
            nullable: true,
        };
        if let Some(cond) = join.condition.as_ref() {
            join.condition = Some(replace_placeholder_in_expr(cond, sq_info.id, &replacement));
        }
        Ok(())
    }

    fn rewrite_join_on_in_subquery(
        &self,
        join: &mut JoinRelation,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
        resolved_sub: ResolvedQuery,
        sq_alias: String,
        negated: bool,
    ) -> Result<(), String> {
        let in_expr_ast = sq_info
            .in_expr
            .as_ref()
            .ok_or("IN subquery rewrite (JOIN ON): missing left-hand expression")?;
        let lhs_typed = self.analyze_expr(in_expr_ast, scope)?;
        if resolved_sub.output_columns.is_empty() {
            return Err("IN subquery must produce at least one column".into());
        }
        let sub_col = resolved_sub.output_columns[0].clone();
        let match_col = format!("__match_{}", sq_info.id);

        // Augment the subquery: DISTINCT + match-indicator column equal to
        // the IN target. After LEFT OUTER JOIN, the match column is NULL
        // for non-matching outer rows and non-NULL for matches.
        let mut modified_sub = resolved_sub;
        if let QueryBody::Select(ref mut sel) = modified_sub.body {
            sel.distinct = true;
            sel.projection.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: None,
                        column: sub_col.name.clone(),
                    },
                    data_type: sub_col.data_type.clone(),
                    nullable: sub_col.nullable,
                },
                output_name: match_col.clone(),
            });
        }
        modified_sub.output_columns.push(OutputColumn {
            name: match_col.clone(),
            data_type: sub_col.data_type.clone(),
            nullable: true,
        });
        let output_columns = modified_sub.output_columns.clone();
        let sub_rel = Relation::Subquery {
            query: Box::new(modified_sub),
            alias: sq_alias.clone(),
            output_columns,
        };

        // Expose the subquery alias in the outer scope so the rewritten
        // ON expression can reference `<sq_alias>.<match>`.
        scope.add_column(
            Some(&sq_alias),
            &sub_col.name,
            sub_col.data_type.clone(),
            true,
        );
        scope.add_column(
            Some(&sq_alias),
            &match_col,
            sub_col.data_type.clone(),
            true,
        );

        let eq_cond = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(lhs_typed.clone()),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some(sq_alias.clone()),
                        column: sub_col.name.clone(),
                    },
                    data_type: sub_col.data_type.clone(),
                    nullable: true,
                }),
            },
        };

        // Attach the aux LEFT OUTER JOIN to whichever side of the host join
        // exposes the LHS column(s); otherwise default to LEFT.
        let side = choose_aux_join_side(join, std::slice::from_ref(&lhs_typed));
        attach_aux_join(join, side, sub_rel, Some(eq_cond));

        let replacement = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some(sq_alias),
                        column: match_col,
                    },
                    data_type: sub_col.data_type.clone(),
                    nullable: true,
                }),
                negated: !negated, // IN → IS NOT NULL; NOT IN → IS NULL
            },
        };
        if let Some(cond) = join.condition.as_ref() {
            join.condition = Some(replace_placeholder_in_expr(cond, sq_info.id, &replacement));
        }
        Ok(())
    }

    fn rewrite_join_on_exists(
        &self,
        join: &mut JoinRelation,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
        resolved_sub: ResolvedQuery,
        sq_alias: String,
        negated: bool,
    ) -> Result<(), String> {
        let match_col = format!("__exists_{}", sq_info.id);
        // Project a single non-null indicator so LEFT OUTER JOIN against
        // `__sq_alias` yields a row with `__exists IS NOT NULL` iff the
        // subquery has any rows.
        let mut modified_sub = resolved_sub;
        if let QueryBody::Select(ref mut sel) = modified_sub.body {
            sel.distinct = false;
            sel.projection.clear();
            sel.projection.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: match_col.clone(),
            });
            sel.has_aggregation = false;
        }
        modified_sub.output_columns = vec![OutputColumn {
            name: match_col.clone(),
            data_type: DataType::Int64,
            nullable: true,
        }];
        modified_sub.limit = Some(1);
        let output_columns = modified_sub.output_columns.clone();
        let sub_rel = Relation::Subquery {
            query: Box::new(modified_sub),
            alias: sq_alias.clone(),
            output_columns,
        };

        scope.add_column(Some(&sq_alias), &match_col, DataType::Int64, true);

        let placeholder = std::mem::replace(&mut join.left, dummy_relation());
        join.left = Relation::Join(Box::new(JoinRelation {
            left: placeholder,
            right: sub_rel,
            join_type: JoinKind::LeftOuter,
            condition: Some(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Bool(true)),
                data_type: DataType::Boolean,
                nullable: false,
            }),
        }));

        let replacement = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some(sq_alias),
                        column: match_col,
                    },
                    data_type: DataType::Int64,
                    nullable: true,
                }),
                negated: !negated, // EXISTS → IS NOT NULL; NOT EXISTS → IS NULL
            },
        };
        if let Some(cond) = join.condition.as_ref() {
            join.condition = Some(replace_placeholder_in_expr(cond, sq_info.id, &replacement));
        }
        Ok(())
    }

    fn rewrite_join_on_scalar(
        &self,
        join: &mut JoinRelation,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
        resolved_sub: ResolvedQuery,
        sq_alias: String,
    ) -> Result<(), String> {
        if resolved_sub.output_columns.is_empty() {
            return Err("scalar subquery must produce at least one column".into());
        }
        let scalar_col = resolved_sub.output_columns[0].clone();
        let output_columns = resolved_sub.output_columns.clone();
        let sub_rel = Relation::Subquery {
            query: Box::new(resolved_sub),
            alias: sq_alias.clone(),
            output_columns,
        };
        scope.add_column(
            Some(&sq_alias),
            &scalar_col.name,
            scalar_col.data_type.clone(),
            true,
        );

        let placeholder = std::mem::replace(&mut join.left, dummy_relation());
        join.left = Relation::Join(Box::new(JoinRelation {
            left: placeholder,
            right: sub_rel,
            join_type: JoinKind::Cross,
            condition: None,
        }));

        let replacement = TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: Some(sq_alias),
                column: scalar_col.name,
            },
            data_type: scalar_col.data_type,
            nullable: true,
        };
        if let Some(cond) = join.condition.as_ref() {
            join.condition = Some(replace_placeholder_in_expr(cond, sq_info.id, &replacement));
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

        // For EXISTS, the subquery becomes the right side of a SEMI/ANTI JOIN.
        // The subquery WHERE is split into:
        //   - correlation predicates → SEMI JOIN ON condition
        //   - remaining inner predicates → kept inside the subquery WHERE
        //
        // This ensures the subquery's internal joins (e.g. store_sales JOIN
        // date_dim ON ss_sold_date_sk = d_date_sk) are preserved as proper
        // joins within the subquery, rather than being hoisted into the
        // semi-join condition which would leave a CROSS JOIN on the inner side.

        // Extract correlation predicates from the subquery WHERE.
        let corr_preds = if let QueryBody::Select(ref sel) = resolved.body {
            if let Some(ref filter) = sel.filter {
                extract_correlation_predicates(filter, &inner_scope, scope)
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let (sub_rel, join_condition) = {
            // Destructure subquery: use FROM as right side, full WHERE as
            // join condition (including both correlation and inner predicates).
            // The optimizer's join reorder and cost model will handle turning
            // inner predicates into proper hash joins.
            let (sub_from, sub_filter) = match resolved.body {
                QueryBody::Select(sel) => (sel.from, sel.filter),
                _ => return Err("EXISTS subquery must be a SELECT".into()),
            };

            let sub_rel = sub_from.ok_or("EXISTS subquery must have a FROM clause")?;

            // Build join condition: correlation predicates + remaining filter.
            // For correlated EXISTS, extract correlation preds as equi-join keys
            // and keep remaining predicates as other conditions.
            let join_cond = if corr_preds.is_empty() {
                sub_filter
            } else {
                // Build combined condition: correlation + non-correlation predicates.
                // Use unqualified column refs so the physical layer can resolve
                // them against either join side without requiring specific aliases.
                // For correlation conditions, unqualify column refs to help
                // the physical layer resolve them. BUT for self-joins (same
                // bare column name on both sides), keep qualifiers to avoid
                // producing tautologies like `col = col`.
                let maybe_unqualify = |expr: &TypedExpr| -> TypedExpr {
                    match &expr.kind {
                        ExprKind::BinaryOp { left, op, right } => {
                            let l_name = match &left.kind {
                                ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
                                _ => None,
                            };
                            let r_name = match &right.kind {
                                ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
                                _ => None,
                            };
                            let same_bare_name = l_name.is_some() && l_name == r_name;

                            let unq = |col: &TypedExpr| -> TypedExpr {
                                if same_bare_name {
                                    col.clone() // Keep qualifier for self-join
                                } else if let ExprKind::ColumnRef { column, .. } = &col.kind {
                                    TypedExpr {
                                        kind: ExprKind::ColumnRef {
                                            qualifier: None,
                                            column: column.clone(),
                                        },
                                        data_type: col.data_type.clone(),
                                        nullable: col.nullable,
                                    }
                                } else {
                                    col.clone()
                                }
                            };
                            TypedExpr {
                                data_type: expr.data_type.clone(),
                                nullable: expr.nullable,
                                kind: ExprKind::BinaryOp {
                                    left: Box::new(unq(left)),
                                    op: *op,
                                    right: Box::new(unq(right)),
                                },
                            }
                        }
                        _ => expr.clone(),
                    }
                };
                let corr_cond = {
                    let mut c = maybe_unqualify(&corr_preds[0].full_expr);
                    for pred in &corr_preds[1..] {
                        c = TypedExpr {
                            data_type: DataType::Boolean,
                            nullable: false,
                            kind: ExprKind::BinaryOp {
                                left: Box::new(c),
                                op: BinOp::And,
                                right: Box::new(maybe_unqualify(&pred.full_expr)),
                            },
                        };
                    }
                    c
                };
                // Remaining non-correlation predicates
                let remaining = sub_filter
                    .as_ref()
                    .and_then(|f| remove_correlation_preds_from_expr(f, &corr_preds));
                match remaining {
                    Some(rem) => Some(TypedExpr {
                        data_type: DataType::Boolean,
                        nullable: false,
                        kind: ExprKind::BinaryOp {
                            left: Box::new(corr_cond),
                            op: BinOp::And,
                            right: Box::new(rem),
                        },
                    }),
                    None => Some(corr_cond),
                }
            };

            (sub_rel, join_cond)
        };

        let current_from = take_from_or_synthesize_single_row(&mut select.from);

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

        // Multi-column LHS: `(a, b) IN (SELECT c, d FROM ...)`. sqlparser
        // emits the LHS as `Expr::Tuple(items)` (possibly wrapped in
        // `Expr::Nested`). Analyze each component separately and pair
        // them with the subquery's output columns one-to-one.
        let lhs_items_ast: Vec<&sqlparser::ast::Expr> = match in_expr_ast.as_ref() {
            sqlparser::ast::Expr::Tuple(items) => items.iter().collect(),
            sqlparser::ast::Expr::Nested(inner) => match inner.as_ref() {
                sqlparser::ast::Expr::Tuple(items) => items.iter().collect(),
                other => vec![other],
            },
            other => vec![other],
        };
        let lhs_typed_list: Vec<TypedExpr> = lhs_items_ast
            .iter()
            .map(|e| self.analyze_expr(e, scope))
            .collect::<Result<Vec<_>, _>>()?;

        let (resolved_sub, inner_scope) =
            self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;

        if resolved_sub.output_columns.len() != lhs_typed_list.len() {
            return Err(format!(
                "IN subquery column count mismatch: LHS has {} expression(s) but subquery produces {} column(s)",
                lhs_typed_list.len(),
                resolved_sub.output_columns.len()
            ));
        }
        let lhs_typed = lhs_typed_list[0].clone();
        let sub_output_col = resolved_sub.output_columns[0].clone();

        // Check if the IN placeholder is inside an OR expression.
        // If so, SEMI JOIN semantics are wrong — we need LEFT OUTER JOIN
        // + IS [NOT] NULL replacement (matching StarRocks FE approach).
        let inside_or = select
            .filter
            .as_ref()
            .map(|f| is_placeholder_inside_or(f, sq_info.id))
            .unwrap_or(false);

        // Correlated subquery: if any predicate in the subquery WHERE references
        // an outer-scope column (e.g. `WHERE t.x = outer.y`), the wrapped
        // `Relation::Subquery` would isolate the inner SELECT and the outer
        // reference would no longer resolve. We must lift the subquery's WHERE
        // up into the SEMI/ANTI join's ON condition — same pattern as EXISTS.
        let is_correlated = match resolved_sub.body {
            QueryBody::Select(ref sel) => sel
                .filter
                .as_ref()
                .map(|f| !extract_correlation_predicates(f, &inner_scope, scope).is_empty())
                .unwrap_or(false),
            _ => false,
        };

        if is_correlated && !inside_or {
            return self.rewrite_correlated_in_subquery(
                select,
                lhs_typed,
                resolved_sub,
                sq_info.id,
                negated,
            );
        }

        let sq_alias = format!("__sq_{}", sq_info.id);

        if inside_or && lhs_typed_list.len() > 1 {
            return Err(
                "multi-column IN subquery inside OR is not yet supported".to_string(),
            );
        }

        // Build per-column equality conjuncts. For a single-column IN this
        // collapses to the original behaviour; for `(a, b) IN (SELECT c, d
        // ...)` we get `a = c AND b = d` (or the null-aware variant for
        // NOT IN).
        let mut eq_conjuncts: Vec<TypedExpr> = Vec::with_capacity(lhs_typed_list.len());
        for (idx, lhs_i) in lhs_typed_list.iter().enumerate() {
            let sub_col = &resolved_sub.output_columns[idx];
            let lhs_name_lower = match &lhs_i.kind {
                ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
                _ => None,
            };
            let rhs_needs_qualifier =
                lhs_name_lower.as_deref() == Some(&sub_col.name.to_lowercase());
            let rhs_ref = TypedExpr {
                kind: ExprKind::ColumnRef {
                    qualifier: if rhs_needs_qualifier {
                        Some(sq_alias.clone())
                    } else {
                        None
                    },
                    column: sub_col.name.clone(),
                },
                data_type: sub_col.data_type.clone(),
                nullable: sub_col.nullable,
            };
            // For IN (semi), plain equality is correct: NULLs never satisfy
            // `=` so they're already excluded. For NOT IN (anti) we need
            // null-aware equality so the LEFT ANTI join matches whenever
            // either operand is NULL, matching SQL's
            // "x NOT IN S returns UNKNOWN if x is NULL or S contains NULL"
            // semantics.
            let eq = if negated && !inside_or {
                null_aware_eq(lhs_i.clone(), rhs_ref)
            } else {
                TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: false,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(lhs_i.clone()),
                        op: BinOp::Eq,
                        right: Box::new(rhs_ref),
                    },
                }
            };
            eq_conjuncts.push(eq);
        }
        let eq_cond = {
            let mut iter = eq_conjuncts.into_iter();
            let mut acc = iter.next().expect("at least one IN column");
            for next in iter {
                acc = TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: false,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(acc),
                        op: BinOp::And,
                        right: Box::new(next),
                    },
                };
            }
            acc
        };

        // Expose every subquery output column under `__sq_<id>` so
        // explicit references (e.g. in IN-inside-OR's match-indicator
        // wrapping below) can resolve.
        for sub_col in &resolved_sub.output_columns {
            scope.add_column(
                Some(&sq_alias),
                &sub_col.name,
                sub_col.data_type.clone(),
                true, // nullable for LEFT OUTER JOIN
            );
        }

        let current_from = take_from_or_synthesize_single_row(&mut select.from);

        if inside_or {
            // IN-inside-OR: use LEFT OUTER JOIN, replace placeholder with
            // IS [NOT] NULL on the join key column (unqualified, which will
            // resolve to the right side's column after JOIN scope merge).
            // We use the right-side column name directly; after the LEFT
            // OUTER JOIN, non-matching rows have NULL in the right column.
            let match_col_name = format!("__in_match_{}", sq_info.id);
            scope.add_column(
                Some(&sq_alias),
                &match_col_name,
                sub_output_col.data_type.clone(),
                true,
            );

            // Wrap the subquery to add a match-indicator column.
            // Also mark as DISTINCT to prevent duplicate matches from
            // multiplying left-side rows via the LEFT OUTER JOIN.
            let mut modified_sub = resolved_sub;
            if let QueryBody::Select(ref mut sel) = modified_sub.body {
                sel.distinct = true;
            }
            modified_sub.output_columns.push(OutputColumn {
                name: match_col_name.clone(),
                data_type: sub_output_col.data_type.clone(),
                nullable: true,
            });
            if let QueryBody::Select(ref mut sel) = modified_sub.body {
                sel.projection.push(ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            qualifier: None,
                            column: sub_output_col.name.clone(),
                        },
                        data_type: sub_output_col.data_type.clone(),
                        nullable: sub_output_col.nullable,
                    },
                    output_name: match_col_name.clone(),
                });
            }

            let output_columns = modified_sub.output_columns.clone();
            let sub_rel = Relation::Subquery {
                query: Box::new(modified_sub),
                alias: sq_alias.clone(),
                output_columns,
            };

            select.from = Some(Relation::Join(Box::new(JoinRelation {
                left: current_from,
                right: sub_rel,
                join_type: JoinKind::LeftOuter,
                condition: Some(eq_cond),
            })));

            // Replace the SubqueryPlaceholder with `match_col IS [NOT] NULL`
            let is_null_expr = TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::IsNull {
                    expr: Box::new(TypedExpr {
                        kind: ExprKind::ColumnRef {
                            qualifier: None,
                            column: match_col_name,
                        },
                        data_type: sub_output_col.data_type.clone(),
                        nullable: true,
                    }),
                    negated: !negated, // IN → IS NOT NULL; NOT IN → IS NULL
                },
            };
            Self::replace_placeholder_in_filter(&mut select.filter, sq_info.id, &is_null_expr);
            Self::replace_placeholder_in_filter(&mut select.having, sq_info.id, &is_null_expr);
        } else {
            // Standard case: SEMI / ANTI JOIN. NULL handling for NOT IN is
            // baked into `eq_cond` above (null-aware equality), so the
            // subquery is wrapped as-is.
            let join_type = if negated {
                JoinKind::LeftAnti
            } else {
                JoinKind::LeftSemi
            };

            let output_columns = resolved_sub.output_columns.clone();
            let sub_rel = Relation::Subquery {
                query: Box::new(resolved_sub),
                alias: sq_alias.clone(),
                output_columns,
            };
            select.from = Some(Relation::Join(Box::new(JoinRelation {
                left: current_from,
                right: sub_rel,
                join_type,
                condition: Some(eq_cond),
            })));
            Self::remove_placeholder_from_filter(&mut select.filter, sq_info.id);
            Self::remove_placeholder_from_filter(&mut select.having, sq_info.id);
        }

        Ok(())
    }

    /// Rewrite a correlated `IN (...)` / `NOT IN (...)` subquery into a
    /// SEMI / ANTI JOIN, hoisting the subquery's WHERE clause (which contains
    /// the correlation predicates) up into the JOIN ON condition.
    ///
    /// Unlike the uncorrelated path, we cannot leave the subquery wrapped as
    /// a `Relation::Subquery` because outer-scope column references in the
    /// inner WHERE would no longer resolve. Instead, we mirror the EXISTS
    /// path: take the subquery's FROM as the join's right side, and place
    /// the subquery's full WHERE plus the eq_cond into the join condition.
    fn rewrite_correlated_in_subquery(
        &self,
        select: &mut ResolvedSelect,
        lhs_typed: TypedExpr,
        resolved_sub: ResolvedQuery,
        sq_id: usize,
        negated: bool,
    ) -> Result<(), String> {
        let (sub_from, sub_filter, sub_projection) = match resolved_sub.body {
            QueryBody::Select(sel) => (sel.from, sel.filter, sel.projection),
            _ => return Err("correlated IN subquery must be a SELECT".into()),
        };

        if sub_projection.is_empty() {
            return Err("IN subquery must produce a column".into());
        }
        let rhs_expr = sub_projection[0].expr.clone();
        let sub_rel = sub_from.ok_or("IN subquery must have a FROM clause".to_string())?;

        // For correlated NOT IN, use null-aware equality so the LEFT ANTI join
        // matches whenever either side is NULL within the correlation group —
        // the row is then excluded from the outer result, matching SQL's
        // "x NOT IN S returns UNKNOWN when x is NULL or S contains NULL"
        // semantics. For IN (semi), a plain equality is correct: NULLs never
        // satisfy IN and the row stays out.
        let key_cond = if negated {
            null_aware_eq(lhs_typed, rhs_expr)
        } else {
            TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    left: Box::new(lhs_typed),
                    op: BinOp::Eq,
                    right: Box::new(rhs_expr),
                },
            }
        };

        let join_cond = match sub_filter {
            Some(f) => Some(TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    left: Box::new(key_cond),
                    op: BinOp::And,
                    right: Box::new(f),
                },
            }),
            None => Some(key_cond),
        };

        let join_type = if negated {
            JoinKind::LeftAnti
        } else {
            JoinKind::LeftSemi
        };

        let current_from = take_from_or_synthesize_single_row(&mut select.from);
        select.from = Some(Relation::Join(Box::new(JoinRelation {
            left: current_from,
            right: sub_rel,
            join_type,
            condition: join_cond,
        })));

        Self::remove_placeholder_from_filter(&mut select.filter, sq_id);
        Self::remove_placeholder_from_filter(&mut select.having, sq_id);
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
        let (mut resolved_sub, inner_scope) =
            self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;

        if resolved_sub.output_columns.is_empty() {
            return Err("scalar subquery must produce at least one output column".into());
        }

        // Factor out common correlation predicates from OR branches before
        // extraction.  E.g. `(corr AND X) OR (corr AND Y)` → `corr AND (X OR Y)`
        // so the correlation predicate lands at the top-level AND and can be
        // extracted normally (matching StarRocks FE behaviour).
        if let QueryBody::Select(ref mut sel) = resolved_sub.body
            && let Some(ref filter) = sel.filter
        {
            sel.filter = Some(factor_common_correlation_from_or(
                filter,
                &inner_scope,
                scope,
            ));
        }

        // Detect correlation by examining the subquery's WHERE for predicates
        // that reference columns present in the outer scope but NOT in the inner scope.
        let corr_preds = if let QueryBody::Select(ref sel) = resolved_sub.body {
            if let Some(ref filter) = sel.filter {
                let mut preds = extract_correlation_predicates(filter, &inner_scope, scope);
                // Deduplicate: OR branches may yield the same correlation
                // predicate multiple times.
                preds.dedup_by(|a, b| exprs_structurally_equal(&a.full_expr, &b.full_expr));
                preds
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let is_correlated = !corr_preds.is_empty();

        if is_correlated {
            let (modified_sub, corr_join_conds) = self
                .build_correlated_scalar_subquery_from_resolved(
                    resolved_sub,
                    scope,
                    &sq_alias,
                    &corr_preds,
                )?;

            let scalar_output_name = modified_sub.output_columns[0].name.clone();
            let scalar_data_type = modified_sub.output_columns[0].data_type.clone();
            let scalar_nullable = true;

            let output_columns = modified_sub.output_columns.clone();
            let sub_rel = Relation::Subquery {
                query: Box::new(modified_sub),
                alias: sq_alias.clone(),
                output_columns,
            };

            scope.add_column(
                Some(&sq_alias),
                &scalar_output_name,
                scalar_data_type.clone(),
                scalar_nullable,
            );

            let current_from = take_from_or_synthesize_single_row(&mut select.from);

            select.from = Some(Relation::Join(Box::new(JoinRelation {
                left: current_from,
                right: sub_rel,
                join_type: JoinKind::LeftOuter,
                condition: corr_join_conds,
            })));

            // Use qualified column ref so that multiple scalar subqueries
            // producing columns with the same name resolve to distinct bindings.
            let replacement = TypedExpr {
                kind: ExprKind::ColumnRef {
                    qualifier: Some(sq_alias.clone()),
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
            let output_columns = resolved_sub.output_columns.clone();
            let sub_rel = Relation::Subquery {
                query: Box::new(resolved_sub),
                alias: sq_alias.clone(),
                output_columns,
            };

            scope.add_column(
                Some(&sq_alias),
                &scalar_col.name,
                scalar_col.data_type.clone(),
                scalar_col.nullable,
            );

            let current_from = take_from_or_synthesize_single_row(&mut select.from);

            select.from = Some(Relation::Join(Box::new(JoinRelation {
                left: current_from,
                right: sub_rel,
                join_type: JoinKind::Cross,
                condition: None,
            })));

            // Use qualified column ref so that multiple scalar subqueries
            // producing columns with the same name resolve to distinct bindings.
            let replacement = TypedExpr {
                kind: ExprKind::ColumnRef {
                    qualifier: Some(sq_alias.clone()),
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
            pending_ctes: self.pending_ctes.clone(),
            next_subquery_id: std::cell::Cell::new(self.next_subquery_id.get()),
            next_lambda_slot_id: std::cell::Cell::new(self.next_lambda_slot_id.get()),
            collected_subqueries: std::cell::RefCell::new(Vec::new()),
            cte_registry: std::cell::RefCell::new(self.cte_registry.borrow().clone()),
        };

        let result = child_ctx.analyze_query_with_outer_scope_inner(query, outer_scope)?;

        self.next_subquery_id.set(child_ctx.next_subquery_id.get());

        let nested_sqs: Vec<SubqueryInfo> = child_ctx
            .collected_subqueries
            .borrow_mut()
            .drain(..)
            .collect();

        self.cte_registry
            .borrow_mut()
            .clone_from(&child_ctx.cte_registry.borrow());
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
        let (maybe_child_ctx, local_cte_ids) = if let Some(ref with_clause) = query.with {
            let (child_ctx, local_cte_ids) = self.build_with_clause_context(with_clause)?;
            (Some(child_ctx), local_cte_ids)
        } else {
            (None, Vec::new())
        };
        let ctx = maybe_child_ctx.as_ref().unwrap_or(self);

        let body = query.body.as_ref();
        let result = match body {
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
                        local_cte_ids,
                    },
                    inner_scope,
                ))
            }
            _ => {
                let (body, cols) = ctx.analyze_set_expr(body)?;
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
                        local_cte_ids,
                    },
                    AnalyzerScope::new(),
                ))
            }
        };

        if let Some(child_ctx) = maybe_child_ctx {
            self.next_subquery_id.set(child_ctx.next_subquery_id.get());
            *self.cte_registry.borrow_mut() = child_ctx.cte_registry.borrow().clone();
        }

        result
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
                // Comma-separated FROM entries are implicit CROSS JOINs.
                // Expose the accumulated left-hand scope so that table-valued
                // functions like `unnest(...)` can reference earlier sibling
                // columns (StarRocks implicit-lateral semantics).
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

        // Merged scope: inner tables first (higher priority), then outer scope for fallback
        let mut merged_scope = inner_scope.clone();
        merged_scope.merge(outer_scope);

        // --- WHERE clause ---
        let filter = match &select.selection {
            Some(expr) => Some(self.analyze_expr(expr, &merged_scope)?),
            None => None,
        };

        // --- SELECT list ---
        // Use inner_scope for wildcard expansion (SELECT * should only produce
        // the subquery's own columns, not outer scope columns) but use
        // merged_scope for column/expression resolution so that correlated
        // references can resolve against the outer scope.
        let (projection, output_columns) = self.analyze_projection_with_wildcard_scope(
            &select.projection,
            &merged_scope,
            &inner_scope,
        )?;

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

    /// Build a correlated scalar subquery from an already-analyzed ResolvedQuery.
    /// Uses the pre-analyzed (and potentially OR-factored) query instead of
    /// re-analyzing from the raw AST, which would lose the OR factoring.
    fn build_correlated_scalar_subquery_from_resolved(
        &self,
        resolved: ResolvedQuery,
        _outer_scope: &AnalyzerScope,
        _sq_alias: &str,
        correlated_cols: &[CorrelationPred],
    ) -> Result<(ResolvedQuery, Option<TypedExpr>), String> {
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
/// Build `(lhs = rhs) OR (lhs IS NULL) OR (rhs IS NULL)`. Used as the join
/// key condition for null-aware `NOT IN`: the LEFT ANTI JOIN must match
/// (and thus exclude the outer row) whenever either operand is NULL,
/// because SQL's NOT IN returns UNKNOWN under those conditions.
fn null_aware_eq(lhs: TypedExpr, rhs: TypedExpr) -> TypedExpr {
    let lhs_clone = lhs.clone();
    let rhs_clone = rhs.clone();
    let eq = TypedExpr {
        data_type: DataType::Boolean,
        nullable: false,
        kind: ExprKind::BinaryOp {
            left: Box::new(lhs),
            op: BinOp::Eq,
            right: Box::new(rhs),
        },
    };
    let lhs_is_null = TypedExpr {
        data_type: DataType::Boolean,
        nullable: false,
        kind: ExprKind::IsNull {
            expr: Box::new(lhs_clone),
            negated: false,
        },
    };
    let rhs_is_null = TypedExpr {
        data_type: DataType::Boolean,
        nullable: false,
        kind: ExprKind::IsNull {
            expr: Box::new(rhs_clone),
            negated: false,
        },
    };
    let or1 = TypedExpr {
        data_type: DataType::Boolean,
        nullable: false,
        kind: ExprKind::BinaryOp {
            left: Box::new(eq),
            op: BinOp::Or,
            right: Box::new(lhs_is_null),
        },
    };
    TypedExpr {
        data_type: DataType::Boolean,
        nullable: false,
        kind: ExprKind::BinaryOp {
            left: Box::new(or1),
            op: BinOp::Or,
            right: Box::new(rhs_is_null),
        },
    }
}

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

/// Synthetic placeholder Relation used with `std::mem::replace` while we
/// shuffle a JoinRelation's left input. The value is immediately
/// overwritten before any consumer sees it.
fn dummy_relation() -> Relation {
    Relation::GenerateSeries(GenerateSeriesRelation {
        start: 0,
        end: -1,
        step: 1,
        column_name: "__nr_dummy".to_string(),
        alias: None,
    })
}

/// Wrap `join.left` (or `join.right`) with a LEFT OUTER JOIN against the
/// given subquery side relation. Used by the JOIN-ON subquery rewrite
/// path to attach the auxiliary subquery to whichever side carries the
/// correlation column.
fn attach_aux_join(
    join: &mut JoinRelation,
    side: AuxJoinSide,
    sub_rel: Relation,
    condition: Option<TypedExpr>,
) {
    let host_side = match side {
        AuxJoinSide::Left => &mut join.left,
        AuxJoinSide::Right => &mut join.right,
    };
    let placeholder = std::mem::replace(host_side, dummy_relation());
    *host_side = Relation::Join(Box::new(JoinRelation {
        left: placeholder,
        right: sub_rel,
        join_type: JoinKind::LeftOuter,
        condition,
    }));
}

/// Recursively walk a TypedExpr looking for any `SubqueryPlaceholder` whose
/// id matches `placeholder_id`.
fn expr_contains_placeholder(expr: &TypedExpr, placeholder_id: usize) -> bool {
    if is_placeholder(expr, placeholder_id) {
        return true;
    }
    match &expr.kind {
        ExprKind::BinaryOp { left, right, .. } => {
            expr_contains_placeholder(left, placeholder_id)
                || expr_contains_placeholder(right, placeholder_id)
        }
        ExprKind::UnaryOp { expr: inner, .. } => {
            expr_contains_placeholder(inner, placeholder_id)
        }
        ExprKind::IsNull { expr: inner, .. } => {
            expr_contains_placeholder(inner, placeholder_id)
        }
        ExprKind::Cast { expr: inner, .. } => {
            expr_contains_placeholder(inner, placeholder_id)
        }
        ExprKind::Nested(inner) => expr_contains_placeholder(inner, placeholder_id),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            args.iter().any(|a| expr_contains_placeholder(a, placeholder_id))
        }
        ExprKind::InList { expr: inner, list, .. } => {
            expr_contains_placeholder(inner, placeholder_id)
                || list.iter().any(|i| expr_contains_placeholder(i, placeholder_id))
        }
        ExprKind::Between { expr: inner, low, high, .. } => {
            expr_contains_placeholder(inner, placeholder_id)
                || expr_contains_placeholder(low, placeholder_id)
                || expr_contains_placeholder(high, placeholder_id)
        }
        ExprKind::Like { expr: inner, pattern, .. } => {
            expr_contains_placeholder(inner, placeholder_id)
                || expr_contains_placeholder(pattern, placeholder_id)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                if expr_contains_placeholder(op, placeholder_id) {
                    return true;
                }
            }
            for (when, then) in when_then {
                if expr_contains_placeholder(when, placeholder_id)
                    || expr_contains_placeholder(then, placeholder_id)
                {
                    return true;
                }
            }
            if let Some(else_) = else_expr {
                if expr_contains_placeholder(else_, placeholder_id) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

/// Return true if the relation tree exposes a column with the given
/// (lowercased) qualifier. Used to pick which side of a JoinRelation
/// should host an auxiliary correlated subquery join.
fn relation_exposes_qualifier(rel: &Relation, qual_lower: &str) -> bool {
    match rel {
        Relation::Scan(s) => {
            let name = s.alias.as_deref().unwrap_or(&s.table.name);
            name.eq_ignore_ascii_case(qual_lower)
        }
        Relation::IcebergMetadataScan(s) => {
            let name = s.alias.as_deref().unwrap_or(&s.table.name);
            name.eq_ignore_ascii_case(qual_lower)
        }
        Relation::Subquery { alias, .. } => alias.eq_ignore_ascii_case(qual_lower),
        Relation::CTEConsume { alias, .. } => alias.eq_ignore_ascii_case(qual_lower),
        Relation::GenerateSeries(g) => g
            .alias
            .as_deref()
            .map(|n| n.eq_ignore_ascii_case(qual_lower))
            .unwrap_or(false),
        Relation::Unnest(u) => u
            .alias
            .as_deref()
            .map(|n| n.eq_ignore_ascii_case(qual_lower))
            .unwrap_or(false),
        Relation::Join(j) => {
            relation_exposes_qualifier(&j.left, qual_lower)
                || relation_exposes_qualifier(&j.right, qual_lower)
        }
    }
}

/// Return true if the relation tree exposes an unqualified column with
/// the given (lowercased) name. Used to disambiguate aux-join placement
/// when the rewritten expression carries unqualified ColumnRefs.
fn relation_exposes_column(rel: &Relation, col_lower: &str) -> bool {
    match rel {
        Relation::Scan(s) => s
            .table
            .columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(col_lower)),
        Relation::IcebergMetadataScan(s) => s
            .table
            .columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(col_lower)),
        Relation::Subquery {
            output_columns, ..
        } => output_columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(col_lower)),
        Relation::CTEConsume {
            output_columns, ..
        } => output_columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(col_lower)),
        Relation::GenerateSeries(g) => g.column_name.eq_ignore_ascii_case(col_lower),
        Relation::Unnest(u) => u
            .output_columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(col_lower)),
        Relation::Join(j) => {
            relation_exposes_column(&j.left, col_lower)
                || relation_exposes_column(&j.right, col_lower)
        }
    }
}

/// Collect every distinct ColumnRef referenced by `expr`, returned as
/// `(qualifier_lower_or_none, column_name_lower)` pairs. Used to decide
/// whether a correlated subquery's auxiliary join should attach to the
/// host join's LEFT input, RIGHT input, or above.
fn collect_column_refs(expr: &TypedExpr, out: &mut Vec<(Option<String>, String)>) {
    match &expr.kind {
        ExprKind::ColumnRef { qualifier, column } => {
            let entry = (
                qualifier.as_ref().map(|q| q.to_lowercase()),
                column.to_lowercase(),
            );
            if !out.contains(&entry) {
                out.push(entry);
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_column_refs(left, out);
            collect_column_refs(right, out);
        }
        ExprKind::UnaryOp { expr: inner, .. } => collect_column_refs(inner, out),
        ExprKind::IsNull { expr: inner, .. } => collect_column_refs(inner, out),
        ExprKind::Cast { expr: inner, .. } => collect_column_refs(inner, out),
        ExprKind::Nested(inner) => collect_column_refs(inner, out),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            for a in args {
                collect_column_refs(a, out);
            }
        }
        ExprKind::InList {
            expr: inner, list, ..
        } => {
            collect_column_refs(inner, out);
            for i in list {
                collect_column_refs(i, out);
            }
        }
        ExprKind::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            collect_column_refs(inner, out);
            collect_column_refs(low, out);
            collect_column_refs(high, out);
        }
        ExprKind::Like {
            expr: inner,
            pattern,
            ..
        } => {
            collect_column_refs(inner, out);
            collect_column_refs(pattern, out);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_column_refs(op, out);
            }
            for (w, t) in when_then {
                collect_column_refs(w, out);
                collect_column_refs(t, out);
            }
            if let Some(e) = else_expr {
                collect_column_refs(e, out);
            }
        }
        _ => {}
    }
}

/// Decide which side of a JoinRelation should host an auxiliary
/// subquery join (or whether the placement is ambiguous). Returns
/// `Side::Left` if `corr_exprs` only references columns reachable from
/// `join.left`, `Side::Right` if only from `join.right`, and `None` if
/// neither or both (ambiguous; falls back to LEFT).
#[derive(Clone, Copy, Debug)]
enum AuxJoinSide {
    Left,
    Right,
}

fn choose_aux_join_side(join: &JoinRelation, corr_exprs: &[TypedExpr]) -> AuxJoinSide {
    let mut refs: Vec<(Option<String>, String)> = Vec::new();
    for e in corr_exprs {
        collect_column_refs(e, &mut refs);
    }
    // Probe each ref against the immediate left/right children of the host
    // join. A ref reaches a side if either:
    //   - its qualifier matches a relation alias on that side, OR
    //   - it is unqualified and its column name is exposed there.
    let on_side = |rel: &Relation, (q, c): &(Option<String>, String)| -> bool {
        match q {
            Some(qual) => relation_exposes_qualifier(rel, qual),
            None => relation_exposes_column(rel, c),
        }
    };
    let any_right = refs.iter().any(|r| on_side(&join.right, r));
    let any_left = refs.iter().any(|r| on_side(&join.left, r));
    if any_right && !any_left {
        AuxJoinSide::Right
    } else {
        AuxJoinSide::Left
    }
}

/// Walk a Relation tree (joins only — base scans / subqueries cannot
/// carry placeholders themselves) looking for any JoinRelation whose
/// `condition` references the given placeholder.
fn expr_contains_placeholder_in_relation(rel: &Relation, placeholder_id: usize) -> bool {
    match rel {
        Relation::Join(j) => {
            j.condition
                .as_ref()
                .map(|c| expr_contains_placeholder(c, placeholder_id))
                .unwrap_or(false)
                || expr_contains_placeholder_in_relation(&j.left, placeholder_id)
                || expr_contains_placeholder_in_relation(&j.right, placeholder_id)
        }
        _ => false,
    }
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
            ignore_nulls,
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
                ignore_nulls: *ignore_nulls,
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

/// Check if a SubqueryPlaceholder with the given id appears under an OR node.
fn is_placeholder_inside_or(expr: &TypedExpr, id: usize) -> bool {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => has_placeholder(left, id) || has_placeholder(right, id),
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => is_placeholder_inside_or(left, id) || is_placeholder_inside_or(right, id),
        ExprKind::Nested(inner) => is_placeholder_inside_or(inner, id),
        _ => false,
    }
}

/// Check if an expression contains a SubqueryPlaceholder with the given id.
fn has_placeholder(expr: &TypedExpr, id: usize) -> bool {
    match &expr.kind {
        ExprKind::SubqueryPlaceholder { id: pid, .. } => *pid == id,
        ExprKind::BinaryOp { left, right, .. } => {
            has_placeholder(left, id) || has_placeholder(right, id)
        }
        ExprKind::Nested(inner) => has_placeholder(inner, id),
        ExprKind::UnaryOp { expr, .. } => has_placeholder(expr, id),
        ExprKind::IsNull { expr, .. } => has_placeholder(expr, id),
        _ => false,
    }
}

fn exprs_structurally_equal(a: &TypedExpr, b: &TypedExpr) -> bool {
    format!("{:?}", a.kind) == format!("{:?}", b.kind)
}

/// Factor out correlation predicates that appear in ALL branches of an OR.
/// `(corr AND X) OR (corr AND Y)` → `corr AND (X OR Y)`
///
/// This matches StarRocks FE's subquery unnesting behavior: the common
/// correlation key is lifted to a top-level AND so the normal correlation
/// extraction can process it.
fn factor_common_correlation_from_or(
    expr: &TypedExpr,
    inner_scope: &super::scope::AnalyzerScope,
    outer_scope: &super::scope::AnalyzerScope,
) -> TypedExpr {
    // Only act on top-level OR
    let branches = split_or(expr);
    if branches.len() < 2 {
        return expr.clone();
    }

    // Collect AND conjuncts for each OR branch, identify correlation predicates
    let branch_conjuncts: Vec<Vec<&TypedExpr>> = branches.iter().map(|b| split_and(b)).collect();

    // Find correlation predicates (inner = outer) common to ALL branches
    let mut common_corr: Vec<TypedExpr> = Vec::new();
    if let Some(first_conjs) = branch_conjuncts.first() {
        for candidate in first_conjs {
            if !is_correlation_eq(candidate, inner_scope, outer_scope) {
                continue;
            }
            let found_in_all = branch_conjuncts[1..]
                .iter()
                .all(|conjs| conjs.iter().any(|c| exprs_structurally_equal(c, candidate)));
            if found_in_all {
                common_corr.push((*candidate).clone());
            }
        }
    }

    if common_corr.is_empty() {
        return expr.clone();
    }

    // Remove common correlation preds from each branch, rebuild OR
    let mut new_branches: Vec<TypedExpr> = Vec::new();
    for branch_conjs in &branch_conjuncts {
        let remaining: Vec<TypedExpr> = branch_conjs
            .iter()
            .filter(|c| !common_corr.iter().any(|cc| exprs_structurally_equal(c, cc)))
            .map(|c| (*c).clone())
            .collect();
        if remaining.is_empty() {
            // Branch was only the correlation pred — becomes TRUE
            new_branches.push(TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Bool(true)),
            });
        } else {
            new_branches.push(conjoin(remaining));
        }
    }

    // Build: common_corr AND (remaining_branch1 OR remaining_branch2 OR ...)
    let or_part = disjoin(new_branches);
    let mut result_parts = common_corr;
    result_parts.push(or_part);
    conjoin(result_parts)
}

/// Check if an expression is a correlation equality: `inner_col = outer_col`.
fn is_correlation_eq(
    expr: &TypedExpr,
    inner_scope: &super::scope::AnalyzerScope,
    outer_scope: &super::scope::AnalyzerScope,
) -> bool {
    if let ExprKind::BinaryOp {
        left,
        op: BinOp::Eq,
        right,
    } = &expr.kind
    {
        let l_outer = is_outer_only_ref(left, inner_scope, outer_scope);
        let r_outer = is_outer_only_ref(right, inner_scope, outer_scope);
        (l_outer && !r_outer) || (!l_outer && r_outer)
    } else {
        false
    }
}

/// Split an expression on AND into a flat list of conjuncts.
fn split_and(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut v = split_and(left);
            v.extend(split_and(right));
            v
        }
        ExprKind::Nested(inner) => split_and(inner),
        _ => vec![expr],
    }
}

/// Split an expression on OR into a flat list of disjuncts.
fn split_or(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let mut v = split_or(left);
            v.extend(split_or(right));
            v
        }
        ExprKind::Nested(inner) => split_or(inner),
        _ => vec![expr],
    }
}

fn disjoin(mut exprs: Vec<TypedExpr>) -> TypedExpr {
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
            op: BinOp::Or,
            right: Box::new(e),
        },
    })
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
