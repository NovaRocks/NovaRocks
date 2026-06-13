//! Subquery routing pass.
//!
//! After the analyzer produces `SubqueryPlaceholder` nodes in WHERE/HAVING
//! expressions, this module routes them through one of the explicit supported
//! implementations:
//!
//! - Scalar subqueries → Apply specs consumed by the planner/rewrite pipeline
//! - WHERE/HAVING EXISTS / IN predicates → predicate Apply specs
//! - JOIN-ON and value-form predicates → local marker-join rewrites
//!
//! Unsupported shapes fail here instead of being sent through an implicit
//! alternate rewrite path.

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
fn take_from_or_synthesize_single_row(
    from: &mut Option<Relation>,
    scope: &AnalyzerScope,
) -> Relation {
    from.take().unwrap_or_else(|| {
        let column_name = "__nr_subquery_join_dummy".to_string();
        let output_column_id = scope.factory().borrow_mut().create(
            Some("generate_series".to_string()),
            column_name.clone(),
            DataType::Int64,
            false,
        );
        Relation::GenerateSeries(GenerateSeriesRelation {
            start: 1,
            end: 1,
            step: 1,
            column_name,
            alias: None,
            output_column_id,
        })
    })
}

fn bool_literal(value: bool) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Bool(value)),
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn null_bool_literal() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        data_type: DataType::Boolean,
        nullable: true,
    }
}

fn is_null_expr(expr: TypedExpr, negated: bool) -> TypedExpr {
    TypedExpr {
        data_type: DataType::Boolean,
        nullable: false,
        kind: ExprKind::IsNull {
            expr: Box::new(expr),
            negated,
        },
    }
}

fn value_form_marker_query(
    source: ResolvedQuery,
    source_alias: String,
    marker_col_id: crate::sql::column_id::ColumnId,
    marker_col_name: String,
    filter: Option<TypedExpr>,
) -> ResolvedQuery {
    let marker_dtype = DataType::Int32;
    let marker_output = OutputColumn {
        column_id: marker_col_id,
        name: marker_col_name.clone(),
        data_type: marker_dtype.clone(),
        nullable: false,
        is_internal: false,
    };
    let source_outputs = source.output_columns.clone();
    ResolvedQuery {
        body: QueryBody::Select(ResolvedSelect {
            from: Some(Relation::Subquery {
                query: Box::new(source),
                alias: source_alias,
                output_columns: source_outputs,
            }),
            filter,
            group_by: Vec::new(),
            having: None,
            projection: vec![ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: marker_dtype.clone(),
                    nullable: false,
                },
                output_name: marker_col_name.clone(),
                output_column_id: marker_col_id,
            }],
            has_aggregation: false,
            distinct: false,
            repeat: None,
            apply_specs: Vec::new(),
            predicate_apply_specs: Vec::new(),
        }),
        order_by: Vec::new(),
        limit: Some(1),
        offset: None,
        output_columns: vec![marker_output],
        local_cte_ids: Vec::new(),
    }
}

fn value_form_nonempty_marker_query(
    source: ResolvedQuery,
    source_alias: String,
    marker_col_id: crate::sql::column_id::ColumnId,
    marker_col_name: String,
) -> ResolvedQuery {
    value_form_marker_query(source, source_alias, marker_col_id, marker_col_name, None)
}

fn value_form_null_marker_query(
    source: ResolvedQuery,
    source_col: &OutputColumn,
    source_alias: String,
    marker_col_id: crate::sql::column_id::ColumnId,
    marker_col_name: String,
) -> ResolvedQuery {
    let filter = is_null_expr(
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: source_col.column_id,
                qualifier: Some(source_alias.clone()),
                column: source_col.name.clone(),
            },
            data_type: source_col.data_type.clone(),
            nullable: source_col.nullable,
        },
        false,
    );
    value_form_marker_query(
        source,
        source_alias,
        marker_col_id,
        marker_col_name,
        Some(filter),
    )
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

impl<'a> AnalyzerContext<'a> {
    /// Route subquery placeholders in a ResolvedSelect.
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
            //   1. WHERE / HAVING / projection clauses that can be represented
            //      as Apply specs.
            //   2. JOIN ... ON clauses that still require relation-local
            //      rewriting because the planner has no JoinOn Apply insertion
            //      point yet.
            //   3. Predicate value-form contexts (projection and OR operands)
            //      where SQL three-valued logic needs explicit marker joins.
            // Every branch below is an explicit supported route. A rejected
            // Apply collection must become a clear unsupported-shape error.
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
            if !in_filter
                && !in_having
                && let Some(from) = select.from.as_mut()
            {
                let id = sq_info.id;
                if self.rewrite_subquery_in_relation(from, scope, &sq_info)? {
                    // Placeholder dispatched to JOIN-ON rewrite.
                    debug_assert!(!expr_contains_placeholder_in_relation(from, id));
                    continue;
                }
            }

            let routed = match &sq_info.kind {
                SubqueryKind::Scalar => self.collect_scalar_apply_spec(select, scope, &sq_info),
                SubqueryKind::Exists { .. } | SubqueryKind::InSubquery { .. } => {
                    self.collect_predicate_apply_spec(select, scope, &sq_info)
                }
            }?;
            if routed {
                continue;
            }

            if matches!(
                sq_info.kind,
                SubqueryKind::Exists { .. } | SubqueryKind::InSubquery { .. }
            ) && predicate_placeholder_is_value_form(select, sq_info.id)
            {
                self.rewrite_single_subquery(select, scope, sq_info)?;
                continue;
            }

            return Err(format!(
                "subquery shape is not supported by Apply rewrite: {}",
                sq_info.subquery
            ));
        }

        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Apply-spec scalar subquery routing
    // ---------------------------------------------------------------------------

    /// Apply-spec handler for a scalar subquery. Returns Ok(true) if an
    /// ApplyScalarSpec was recorded and the placeholder replaced. Returns
    /// Ok(false) only when the placeholder is not in a clause represented by
    /// ApplyScalarSpec; the caller must either dispatch it to an explicit
    /// non-Apply route or report an unsupported shape.
    fn collect_scalar_apply_spec(
        &self,
        select: &mut ResolvedSelect,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
    ) -> Result<bool, String> {
        use crate::sql::analysis::{ApplyScalarSpec, OutputColumn};

        // 1. Determine which clause the placeholder lives in.
        let clause = match locate_scalar_placeholder_clause(select, sq_info.id) {
            Some(c) => c,
            None => return Ok(false),
        };

        // 2. Analyze the inner subquery with the merged outer scope. Outer refs
        //    inside it now carry outer ColumnIds.
        let (mut resolved_sub, inner_scope) =
            self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;
        if resolved_sub.output_columns.len() != 1 {
            return Err("scalar subquery must produce exactly one output column".to_string());
        }

        if let QueryBody::Select(ref mut sel) = resolved_sub.body
            && let Some(ref filter) = sel.filter
        {
            sel.filter = Some(factor_common_correlation_from_or(
                filter,
                &inner_scope,
                scope,
            ));
        }

        // 3. Collect correlation column ids after semantic-preserving filter
        // normalization.
        let corr_ids = collect_correlation_column_ids(&resolved_sub, &inner_scope, scope);

        // 4. Mint an output column representing the subquery's scalar value in
        //    the outer expressions. Always nullable (the inner may produce NULL
        //    for non-matching rows).
        let inner_out = &resolved_sub.output_columns[0];
        let output_name = format!("__scalar_sq_{}", sq_info.id);
        let output_id =
            self.alloc_column_id(None, output_name.clone(), inner_out.data_type.clone(), true);
        let output_column = OutputColumn {
            column_id: output_id,
            name: output_name.clone(),
            data_type: inner_out.data_type.clone(),
            nullable: true,
            is_internal: true,
        };

        // 5. Replace the SubqueryPlaceholder in filter/having/projection with a
        //    ColumnRef to the minted output column. Reuses the same static
        //    helpers rewrite_scalar_subquery uses (replace_placeholder_in_filter,
        //    replace_placeholder_in_projection, replace_placeholder_in_expr).
        let replacement = crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                column_id: output_id,
                qualifier: None,
                column: output_name.clone(),
            },
            data_type: inner_out.data_type.clone(),
            nullable: true,
        };
        Self::replace_placeholder_in_filter(&mut select.filter, sq_info.id, &replacement);
        Self::replace_placeholder_in_filter(&mut select.having, sq_info.id, &replacement);
        Self::replace_placeholder_in_projection(&mut select.projection, sq_info.id, &replacement);

        // 6. Record the spec. The inner query is left INTACT — correlation
        //    predicates remain in its WHERE; M1b's PushDownApplyFilter rule
        //    extracts them into the Apply node's correlation_conjuncts.
        select.apply_specs.push(ApplyScalarSpec {
            subquery_id: sq_info.id,
            clause,
            output_column,
            inner: resolved_sub,
            correlation_column_ids: corr_ids,
            need_check_max_rows: true,
            subquery_text: sq_info.subquery.to_string(),
        });
        Ok(true)
    }

    /// Apply-spec handler for an EXISTS / NOT EXISTS / IN / NOT IN subquery.
    /// Returns Ok(true) if an ApplyPredicateSpec was recorded and the placeholder
    /// conjunct removed. Returns Ok(false) only for explicit value-form routes
    /// that are handled outside Apply in this module.
    fn collect_predicate_apply_spec(
        &self,
        select: &mut ResolvedSelect,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
    ) -> Result<bool, String> {
        use crate::sql::analysis::{ApplyClause, ApplyPredicateSpec, OutputColumn};

        let top_level_where_conjunct = select
            .filter
            .as_ref()
            .map(|f| is_placeholder_top_level_and_conjunct(f, sq_info.id))
            .unwrap_or(false);
        let inside_or = select
            .filter
            .as_ref()
            .map(|f| is_placeholder_inside_or(f, sq_info.id))
            .unwrap_or(false)
            || select
                .having
                .as_ref()
                .map(|f| is_placeholder_inside_or(f, sq_info.id))
                .unwrap_or(false);
        if inside_or {
            return Ok(false);
        }
        let top_level_having_conjunct = select
            .having
            .as_ref()
            .map(|f| is_placeholder_top_level_and_conjunct(f, sq_info.id))
            .unwrap_or(false);
        let clause = match (top_level_where_conjunct, top_level_having_conjunct) {
            (true, _) => ApplyClause::Where,
            (false, true) => ApplyClause::Having,
            (false, false) => return Ok(false),
        };

        let (resolved_sub, inner_scope) =
            self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;

        let in_lhs = match &sq_info.kind {
            SubqueryKind::InSubquery { .. } => {
                let in_expr = sq_info
                    .in_expr
                    .as_ref()
                    .ok_or_else(|| "IN subquery missing LHS expression".to_string())?;
                if matches!(in_expr.as_ref(), sqlparser::ast::Expr::Tuple(_))
                    || matches!(
                        in_expr.as_ref(),
                        sqlparser::ast::Expr::Nested(inner)
                            if matches!(inner.as_ref(), sqlparser::ast::Expr::Tuple(_))
                    )
                {
                    let SubqueryKind::InSubquery { negated } = &sq_info.kind else {
                        unreachable!("tuple LHS only exists for IN subqueries");
                    };
                    self.rewrite_in_subquery(select, scope, sq_info.clone(), *negated)?;
                    return Ok(true);
                }
                if resolved_sub.output_columns.len() != 1 {
                    return Err("IN subquery must produce exactly one output column".to_string());
                }
                let lhs = self.analyze_expr(in_expr, scope)?;
                let inner_col = &resolved_sub.output_columns[0];
                if let Some(reason) = super::resolve_expr::incompatible_complex_compare_pub(
                    &lhs.data_type,
                    &inner_col.data_type,
                ) {
                    let op_sym = match &sq_info.kind {
                        SubqueryKind::InSubquery { negated: true } => "NOT IN",
                        _ => "IN",
                    };
                    return Err(format!(
                        "comparison operator `{op_sym}` does not support binary predicate operation between {reason}"
                    ));
                }
                Some(lhs)
            }
            SubqueryKind::Exists { .. } => None,
            SubqueryKind::Scalar => return Ok(false),
        };

        let corr_ids =
            collect_predicate_correlation_column_ids_for_apply(&resolved_sub, &inner_scope, scope);
        let outer_refs = collect_subquery_outer_ref_usage(&resolved_sub, &inner_scope, scope);
        if outer_refs.outside_filter || (outer_refs.filter && corr_ids.is_empty()) {
            return Err(
                "correlated EXISTS/IN subquery must use comparison predicates in the subquery filter"
                    .to_string(),
            );
        }

        let output_name = format!("__pred_sq_{}", sq_info.id);
        let output_id = self.alloc_column_id(None, output_name.clone(), DataType::Boolean, false);
        let output_column = OutputColumn {
            column_id: output_id,
            name: output_name,
            data_type: DataType::Boolean,
            nullable: false,
            is_internal: true,
        };

        match clause {
            ApplyClause::Where => {
                Self::remove_placeholder_from_filter(&mut select.filter, sq_info.id);
            }
            ApplyClause::Having => {
                Self::remove_placeholder_from_filter(&mut select.having, sq_info.id);
            }
            ApplyClause::Projection => {
                return Err("predicate subquery in SELECT list requires value-form rewrite".into());
            }
        }

        select.predicate_apply_specs.push(ApplyPredicateSpec {
            subquery_id: sq_info.id,
            kind: sq_info.kind.clone(),
            clause,
            output_column,
            inner: resolved_sub,
            correlation_column_ids: corr_ids,
            in_lhs,
            use_semi_anti: true,
            subquery_text: sq_info.subquery.to_string(),
        });
        Ok(true)
    }

    fn build_value_form_marker_relation(
        &self,
        scope: &mut AnalyzerScope,
        source: ResolvedQuery,
        relation_alias: String,
        source_alias: String,
        marker_col_name: String,
        null_source_col: Option<&OutputColumn>,
    ) -> (Relation, TypedExpr) {
        let marker_col_id = self.alloc_column_id(
            Some(relation_alias.clone()),
            marker_col_name.clone(),
            DataType::Int32,
            true,
        );
        let marker_query = match null_source_col {
            Some(source_col) => value_form_null_marker_query(
                source,
                source_col,
                source_alias,
                marker_col_id,
                marker_col_name.clone(),
            ),
            None => value_form_nonempty_marker_query(
                source,
                source_alias,
                marker_col_id,
                marker_col_name.clone(),
            ),
        };
        scope.add_column_with_id(
            Some(&relation_alias),
            &marker_col_name,
            marker_col_id,
            DataType::Int32,
            true,
        );
        let exists = is_null_expr(
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: marker_col_id,
                    qualifier: Some(relation_alias.clone()),
                    column: marker_col_name.clone(),
                },
                data_type: DataType::Int32,
                nullable: true,
            },
            true,
        );
        let relation = Relation::Subquery {
            query: Box::new(marker_query),
            alias: relation_alias,
            output_columns: vec![OutputColumn {
                column_id: marker_col_id,
                name: marker_col_name,
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
        };
        (relation, exists)
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
    /// Correlated JOIN-ON subqueries use dedicated relation-local rewrite paths.
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
            SubqueryKind::Exists { negated } => {
                self.rewrite_join_on_exists(join, scope, sq_info, resolved_sub, sq_alias, *negated)
            }
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
        let sub_rel =
            sub_from.ok_or("correlated IN subquery must have a FROM clause".to_string())?;

        // Build the equality condition plus the lifted WHERE.
        let eq_cond = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(lhs_typed.clone()),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: sub_first_col.column_id,
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
                        column_id: sub_first_col.column_id,
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
        _scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
        resolved_sub: ResolvedQuery,
        sq_alias: String,
        negated: bool,
    ) -> Result<(), String> {
        let (sub_from, sub_filter) = match resolved_sub.body {
            QueryBody::Select(sel) => (sel.from, sel.filter),
            _ => return Err("correlated EXISTS subquery must be a SELECT".into()),
        };
        let sub_rel =
            sub_from.ok_or("correlated EXISTS subquery must have a FROM clause".to_string())?;

        // Pick the first output column of the FROM relation as the match indicator.
        //
        // We intentionally do NOT use `resolved_sub.output_columns.first()` here.
        // When the EXISTS subquery is `SELECT 1 FROM rel WHERE <corr>`, the
        // first projection output is the literal `1` — a freshly-allocated
        // ColumnId that is only present in the SELECT projection, which is
        // *discarded* when we deconstruct the subquery into `(FROM, WHERE)`.
        // Referencing that ColumnId in the IS NOT NULL replacement expression
        // would produce "Column '1' cannot be resolved" at codegen time because
        // the column never materialises in the physical plan.
        //
        // Instead, use the first column of `sub_rel` (the FROM relation). This
        // column IS in the plan (it is attached via `attach_aux_join`) and its
        // value is non-NULL whenever the correlated subquery matches a row,
        // which is exactly the semantics we need for the EXISTS IS NOT NULL check.
        let indicator = relation_first_output_column(&sub_rel).ok_or(
            "correlated EXISTS subquery: FROM relation has no output column for indicator",
        )?;

        let side = match sub_filter.as_ref() {
            Some(f) => choose_aux_join_side(join, std::slice::from_ref(f)),
            None => AuxJoinSide::Left,
        };
        attach_aux_join(join, side, sub_rel, sub_filter);

        let replacement = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: indicator.column_id,
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
                let (_, scope) =
                    self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;
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
        let (modified_sub, corr_join_conds) = self.build_correlated_scalar_subquery_from_resolved(
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
        scope.add_column_with_id(
            Some(&sq_alias),
            &scalar_output.name,
            scalar_output.column_id,
            scalar_output.data_type.clone(),
            true,
        );

        let side = choose_aux_join_side(join, &outer_corr_exprs);
        attach_aux_join(join, side, sub_rel, corr_join_conds);

        let replacement = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: scalar_output.column_id,
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
        let source_sub = resolved_sub.clone();

        // Augment the subquery: DISTINCT + match-indicator column. After the
        // LEFT OUTER JOIN, this column is NULL for non-matching outer rows
        // and non-NULL for matches.
        //
        // Use a constant `1` as the indicator value rather than a ColumnRef
        // to the user-visible subquery output column. The original output
        // column may be a derived expression (e.g. `max(v12) - 501`) whose
        // codegen-side display name lives in the post-Project scope only;
        // a ColumnRef to that name fails to resolve when the second project
        // item compiles in the same Project's input scope ("Column
        // '(max(v12)) - 501' cannot be resolved"). A literal sidesteps the
        // lookup entirely and produces the same boolean indicator semantics.
        let mut modified_sub = resolved_sub;
        let indicator_dtype = DataType::Int32;
        let match_col_id = self.alloc_column_id(
            Some(sq_alias.clone()),
            match_col.clone(),
            indicator_dtype.clone(),
            true,
        );
        if let QueryBody::Select(ref mut sel) = modified_sub.body {
            sel.distinct = true;
            sel.projection.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: indicator_dtype.clone(),
                    nullable: false,
                },
                output_name: match_col.clone(),
                output_column_id: match_col_id,
            });
        }
        modified_sub.output_columns.push(OutputColumn {
            column_id: match_col_id,
            name: match_col.clone(),
            data_type: indicator_dtype.clone(),
            nullable: true,
            is_internal: false,
        });
        let output_columns = modified_sub.output_columns.clone();
        let sub_rel = Relation::Subquery {
            query: Box::new(modified_sub),
            alias: sq_alias.clone(),
            output_columns,
        };

        // Expose the subquery alias in the outer scope so the rewritten
        // ON expression can reference `<sq_alias>.<match>`.
        scope.add_column_with_id(
            Some(&sq_alias),
            &sub_col.name,
            sub_col.column_id,
            sub_col.data_type.clone(),
            true,
        );
        scope.add_column_with_id(
            Some(&sq_alias),
            &match_col,
            match_col_id,
            indicator_dtype.clone(),
            true,
        );
        let null_marker = if sub_col.nullable {
            Some(self.build_value_form_marker_relation(
                scope,
                source_sub.clone(),
                format!("__sq_on_null_{}", sq_info.id),
                format!("__sq_on_null_src_{}", sq_info.id),
                format!("__on_has_null_{}", sq_info.id),
                Some(&sub_col),
            ))
        } else {
            None
        };
        let nonempty_marker = if lhs_typed.nullable {
            Some(self.build_value_form_marker_relation(
                scope,
                source_sub,
                format!("__sq_on_any_{}", sq_info.id),
                format!("__sq_on_any_src_{}", sq_info.id),
                format!("__on_has_row_{}", sq_info.id),
                None,
            ))
        } else {
            None
        };

        let eq_cond = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(lhs_typed.clone()),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: sub_col.column_id,
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
        if let Some((null_rel, _)) = null_marker.as_ref() {
            attach_aux_join(join, side, null_rel.clone(), Some(bool_literal(true)));
        }
        if let Some((any_rel, _)) = nonempty_marker.as_ref() {
            attach_aux_join(join, side, any_rel.clone(), Some(bool_literal(true)));
        }

        let match_exists = is_null_expr(
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: match_col_id,
                    qualifier: Some(sq_alias),
                    column: match_col,
                },
                data_type: indicator_dtype,
                nullable: true,
            },
            true,
        );
        let null_exists = null_marker.map(|(_, null_exists)| null_exists);
        let nonempty_exists = nonempty_marker.map(|(_, any_exists)| any_exists);
        let nullable_result = null_exists.is_some() || nonempty_exists.is_some();
        let mut when_then = Vec::new();
        when_then.push((match_exists, bool_literal(!negated)));
        if lhs_typed.nullable {
            let lhs_null_unknown = match nonempty_exists {
                Some(any_exists) => TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: false,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(is_null_expr(lhs_typed.clone(), false)),
                        op: BinOp::And,
                        right: Box::new(any_exists),
                    },
                },
                None => is_null_expr(lhs_typed.clone(), false),
            };
            when_then.push((lhs_null_unknown, null_bool_literal()));
        }
        if let Some(null_exists) = null_exists {
            when_then.push((null_exists, null_bool_literal()));
        }
        let replacement = TypedExpr {
            data_type: DataType::Boolean,
            nullable: nullable_result,
            kind: ExprKind::Case {
                operand: None,
                when_then,
                else_expr: Some(Box::new(bool_literal(negated))),
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
        let exists_col_id = self.alloc_column_id(
            Some(sq_alias.clone()),
            match_col.clone(),
            DataType::Int64,
            true,
        );
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
                output_column_id: exists_col_id,
            });
            sel.has_aggregation = false;
        }
        modified_sub.output_columns = vec![OutputColumn {
            column_id: exists_col_id,
            name: match_col.clone(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }];
        modified_sub.limit = Some(1);
        let output_columns = modified_sub.output_columns.clone();
        let sub_rel = Relation::Subquery {
            query: Box::new(modified_sub),
            alias: sq_alias.clone(),
            output_columns,
        };

        scope.add_column_with_id(
            Some(&sq_alias),
            &match_col,
            exists_col_id,
            DataType::Int64,
            true,
        );

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
                        column_id: exists_col_id,
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
        scope.add_column_with_id(
            Some(&sq_alias),
            &scalar_col.name,
            scalar_col.column_id,
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
                column_id: scalar_col.column_id,
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

        let is_correlated = match &resolved.body {
            QueryBody::Select(sel) => sel
                .filter
                .as_ref()
                .map(|f| {
                    !extract_correlation_predicates(f, &inner_scope, scope).is_empty()
                        || expr_references_outer_scope(f, &inner_scope, scope)
                })
                .unwrap_or(false),
            _ => false,
        };
        if !is_correlated {
            return self.rewrite_uncorrelated_exists(select, scope, resolved, sq_info.id, negated);
        }
        if select
            .projection
            .iter()
            .any(|item| expr_contains_placeholder(&item.expr, sq_info.id))
        {
            return Err("correlated EXISTS subquery in SELECT list is not supported".to_string());
        }
        if predicate_placeholder_is_value_form(select, sq_info.id) {
            return Err(
                "correlated EXISTS subquery in value-form expression is not supported".to_string(),
            );
        }

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
                let maybe_unqualify_col = |col: &TypedExpr, same_bare_name: bool| -> TypedExpr {
                    if same_bare_name {
                        col.clone()
                    } else if let ExprKind::ColumnRef {
                        column_id,
                        qualifier,
                        column,
                    } = &col.kind
                    {
                        let ambiguous_between_scopes = qualifier.is_some()
                            && inner_scope.resolve(None, column).is_ok()
                            && scope.resolve(None, column).is_ok();
                        if ambiguous_between_scopes {
                            return col.clone();
                        }
                        TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: *column_id,
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
                let build_corr_cond = |pred: &CorrelationPred| -> TypedExpr {
                    let outer_name = match &pred.outer_col.kind {
                        ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
                        _ => None,
                    };
                    let inner_name = match &pred.inner_col.kind {
                        ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
                        _ => None,
                    };
                    let same_bare_name = outer_name.is_some() && outer_name == inner_name;

                    // Put the outer column on the left and the inner column
                    // on the right. Hash join key extraction is input-side
                    // sensitive; preserving the original inner=outer textual
                    // order can reverse SEMI/ANTI keys.
                    let original_left_is_inner = match &pred.full_expr.kind {
                        ExprKind::BinaryOp { left, .. } => {
                            exprs_structurally_equal(left, &pred.inner_col)
                        }
                        _ => false,
                    };
                    let mut outer_expr = maybe_unqualify_col(&pred.outer_col, same_bare_name);
                    if original_left_is_inner && outer_expr.data_type != pred.inner_col.data_type {
                        outer_expr = TypedExpr {
                            data_type: pred.inner_col.data_type.clone(),
                            nullable: outer_expr.nullable,
                            kind: ExprKind::Cast {
                                expr: Box::new(outer_expr),
                                target: pred.inner_col.data_type.clone(),
                            },
                        };
                    }
                    TypedExpr {
                        data_type: pred.full_expr.data_type.clone(),
                        nullable: pred.full_expr.nullable,
                        kind: ExprKind::BinaryOp {
                            left: Box::new(outer_expr),
                            op: pred.op,
                            right: Box::new(maybe_unqualify_col(&pred.inner_col, same_bare_name)),
                        },
                    }
                };
                let corr_cond = {
                    let mut c = build_corr_cond(&corr_preds[0]);
                    for pred in &corr_preds[1..] {
                        c = TypedExpr {
                            data_type: DataType::Boolean,
                            nullable: false,
                            kind: ExprKind::BinaryOp {
                                left: Box::new(c),
                                op: BinOp::And,
                                right: Box::new(build_corr_cond(pred)),
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

        let current_from = take_from_or_synthesize_single_row(&mut select.from, scope);

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

    fn rewrite_uncorrelated_exists(
        &self,
        select: &mut ResolvedSelect,
        scope: &mut AnalyzerScope,
        mut resolved_sub: ResolvedQuery,
        sq_id: usize,
        negated: bool,
    ) -> Result<(), String> {
        let sq_alias = format!("__sq_{}", sq_id);
        let match_col = format!("__exists_{}", sq_id);
        let exists_col_id = self.alloc_column_id(
            Some(sq_alias.clone()),
            match_col.clone(),
            DataType::Int64,
            true,
        );

        if let QueryBody::Select(ref mut sel) = resolved_sub.body {
            sel.distinct = false;
            sel.projection.clear();
            sel.projection.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: match_col.clone(),
                output_column_id: exists_col_id,
            });
            sel.has_aggregation = false;
        }
        resolved_sub.output_columns = vec![OutputColumn {
            column_id: exists_col_id,
            name: match_col.clone(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }];
        resolved_sub.limit = Some(1);

        let sub_rel = Relation::Subquery {
            query: Box::new(resolved_sub),
            alias: sq_alias.clone(),
            output_columns: vec![OutputColumn {
                column_id: exists_col_id,
                name: match_col.clone(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
        };

        scope.add_column_with_id(
            Some(&sq_alias),
            &match_col,
            exists_col_id,
            DataType::Int64,
            true,
        );
        let current_from = take_from_or_synthesize_single_row(&mut select.from, scope);
        select.from = Some(Relation::Join(Box::new(JoinRelation {
            left: current_from,
            right: sub_rel,
            join_type: JoinKind::LeftOuter,
            condition: Some(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Bool(true)),
                data_type: DataType::Boolean,
                nullable: false,
            }),
        })));

        let replacement = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: exists_col_id,
                        qualifier: Some(sq_alias),
                        column: match_col,
                    },
                    data_type: DataType::Int64,
                    nullable: true,
                }),
                negated: !negated,
            },
        };

        Self::replace_placeholder_in_filter(&mut select.filter, sq_id, &replacement);
        Self::replace_placeholder_in_filter(&mut select.having, sq_id, &replacement);
        Self::replace_placeholder_in_projection(&mut select.projection, sq_id, &replacement);

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
        // Per-pair shape check: `x IN (SELECT y …)` is rewritten into an
        // EQ-join on (x, y). For composite types the two sides must have a
        // compatible shape — same outer kind, recursively compatible fields
        // / element types. The downstream `BinaryOp::Eq` we build here
        // does NOT run through the analyzer's compare-type guard, so we
        // duplicate the check up front and surface the standard
        // "does not support binary predicate operation" diagnostic
        // (matching the bare-`=` rejection path).
        for (lhs_i, sub_col) in lhs_typed_list
            .iter()
            .zip(resolved_sub.output_columns.iter())
        {
            if let Some(reason) = super::resolve_expr::incompatible_complex_compare_pub(
                &lhs_i.data_type,
                &sub_col.data_type,
            ) {
                let op_sym = if negated { "NOT IN" } else { "IN" };
                return Err(format!(
                    "comparison operator `{op_sym}` does not support binary predicate operation between {reason}"
                ));
            }
        }
        let lhs_typed = lhs_typed_list[0].clone();
        let sub_output_col = resolved_sub.output_columns[0].clone();

        // Value-form contexts (OR operands, SELECT projection, nested HAVING)
        // cannot use SEMI/ANTI joins because they must preserve the outer row
        // and evaluate to TRUE/FALSE/NULL per row.
        let value_form = predicate_placeholder_is_value_form(select, sq_info.id);

        // Correlated subquery: if any predicate in the subquery WHERE references
        // an outer-scope column (e.g. `WHERE t.x = outer.y`), the wrapped
        // `Relation::Subquery` would isolate the inner SELECT and the outer
        // reference would no longer resolve. We must lift the subquery's WHERE
        // up into the SEMI/ANTI join's ON condition — same pattern as EXISTS.
        //
        // Detect correlation broadly: extract_correlation_predicates only
        // recognises comparison-shaped predicates (e.g. `outer.x = inner.y`),
        // but a bare-column WHERE like `WHERE outer.flag` (implicit `!= 0`)
        // or any expression that references an outer column also has to
        // route through the correlated path so the outer reference stays
        // visible at codegen time. Use a generic "does the subquery filter
        // mention any outer-scope column" check as the broad detector.
        let is_correlated = match resolved_sub.body {
            QueryBody::Select(ref sel) => sel
                .filter
                .as_ref()
                .map(|f| {
                    !extract_correlation_predicates(f, &inner_scope, scope).is_empty()
                        || expr_references_outer_scope(f, &inner_scope, scope)
                })
                .unwrap_or(false),
            _ => false,
        };

        if is_correlated && value_form {
            return Err(
                "correlated IN subquery in value-form expression is not supported".to_string(),
            );
        }

        if is_correlated {
            return self.rewrite_correlated_in_subquery(
                select,
                scope,
                lhs_typed,
                resolved_sub,
                sq_info.id,
                negated,
            );
        }

        let sq_alias = format!("__sq_{}", sq_info.id);

        if value_form && lhs_typed_list.len() > 1 {
            return Err(
                "multi-column IN subquery in value-form expression is not supported".to_string(),
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
                    column_id: sub_col.column_id,
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
            // Always emit plain `Eq` as the join condition. For NOT IN, SQL's
            // "any NULL anywhere → UNKNOWN" semantics is encoded by selecting
            // `JoinKind::NullAwareLeftAnti` below when either operand could be
            // NULL, rather than by wrapping the condition in IS-NULL ORs (the
            // old form). Keeping the condition a bare `Eq` lets the Cascades
            // implement phase extract it as a real hash-join key — without
            // that, `c0 NOT IN (subq)` on nullable columns degraded to a
            // NestLoopJoin and timed out on 60K×40K-scale inputs.
            let eq = TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    left: Box::new(lhs_i.clone()),
                    op: BinOp::Eq,
                    right: Box::new(rhs_ref),
                },
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
            scope.add_column_with_id(
                Some(&sq_alias),
                &sub_col.name,
                sub_col.column_id,
                sub_col.data_type.clone(),
                true, // nullable for LEFT OUTER JOIN
            );
        }

        let current_from = take_from_or_synthesize_single_row(&mut select.from, scope);

        if value_form {
            // IN value-form: use LEFT OUTER JOIN, replace placeholder with
            // a CASE over the match indicator plus an optional build-NULL
            // marker. SQL IN/NOT IN value-form preserves three-valued logic:
            // non-matching rows become UNKNOWN when the build side contains
            // NULL, even if the probe side is non-NULL.
            let match_col_name = format!("__in_match_{}", sq_info.id);
            let in_match_col_id = self.alloc_column_id(
                Some(sq_alias.clone()),
                match_col_name.clone(),
                sub_output_col.data_type.clone(),
                true,
            );
            scope.add_column_with_id(
                Some(&sq_alias),
                &match_col_name,
                in_match_col_id,
                sub_output_col.data_type.clone(),
                true,
            );

            let null_marker = if sub_output_col.nullable {
                Some(self.build_value_form_marker_relation(
                    scope,
                    resolved_sub.clone(),
                    format!("__sq_null_{}", sq_info.id),
                    format!("__sq_null_src_{}", sq_info.id),
                    format!("__has_null_{}", sq_info.id),
                    Some(&sub_output_col),
                ))
            } else {
                None
            };
            let nonempty_marker = if lhs_typed.nullable {
                Some(self.build_value_form_marker_relation(
                    scope,
                    resolved_sub.clone(),
                    format!("__sq_any_{}", sq_info.id),
                    format!("__sq_any_src_{}", sq_info.id),
                    format!("__has_row_{}", sq_info.id),
                    None,
                ))
            } else {
                None
            };

            // Wrap the subquery to add a match-indicator column.
            // Also mark as DISTINCT to prevent duplicate matches from
            // multiplying left-side rows via the LEFT OUTER JOIN.
            let mut modified_sub = resolved_sub;
            if let QueryBody::Select(ref mut sel) = modified_sub.body {
                sel.distinct = true;
            }
            modified_sub.output_columns.push(OutputColumn {
                column_id: in_match_col_id,
                name: match_col_name.clone(),
                data_type: sub_output_col.data_type.clone(),
                nullable: true,
                is_internal: false,
            });
            if let QueryBody::Select(ref mut sel) = modified_sub.body {
                sel.projection.push(ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: sub_output_col.column_id,
                            qualifier: None,
                            column: sub_output_col.name.clone(),
                        },
                        data_type: sub_output_col.data_type.clone(),
                        nullable: sub_output_col.nullable,
                    },
                    output_name: match_col_name.clone(),
                    output_column_id: in_match_col_id,
                });
            }

            let output_columns = modified_sub.output_columns.clone();
            let sub_rel = Relation::Subquery {
                query: Box::new(modified_sub),
                alias: sq_alias.clone(),
                output_columns,
            };

            let match_join = Relation::Join(Box::new(JoinRelation {
                left: current_from,
                right: sub_rel,
                join_type: JoinKind::LeftOuter,
                condition: Some(eq_cond),
            }));
            let (joined_from, null_exists) = match null_marker {
                Some((null_rel, null_exists)) => (
                    Relation::Join(Box::new(JoinRelation {
                        left: match_join,
                        right: null_rel,
                        join_type: JoinKind::LeftOuter,
                        condition: Some(bool_literal(true)),
                    })),
                    Some(null_exists),
                ),
                None => (match_join, None),
            };
            let (joined_from, nonempty_exists) = match nonempty_marker {
                Some((any_rel, any_exists)) => (
                    Relation::Join(Box::new(JoinRelation {
                        left: joined_from,
                        right: any_rel,
                        join_type: JoinKind::LeftOuter,
                        condition: Some(bool_literal(true)),
                    })),
                    Some(any_exists),
                ),
                None => (joined_from, None),
            };
            select.from = Some(joined_from);

            let match_exists = is_null_expr(
                TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: in_match_col_id,
                        qualifier: None,
                        column: match_col_name,
                    },
                    data_type: sub_output_col.data_type.clone(),
                    nullable: true,
                },
                true,
            );
            let nullable_result = null_exists.is_some() || nonempty_exists.is_some();
            let mut when_then = Vec::new();
            when_then.push((match_exists, bool_literal(!negated)));
            if lhs_typed.nullable {
                let lhs_null_unknown = match nonempty_exists {
                    Some(any_exists) => TypedExpr {
                        data_type: DataType::Boolean,
                        nullable: false,
                        kind: ExprKind::BinaryOp {
                            left: Box::new(is_null_expr(lhs_typed.clone(), false)),
                            op: BinOp::And,
                            right: Box::new(any_exists),
                        },
                    },
                    None => is_null_expr(lhs_typed.clone(), false),
                };
                when_then.push((lhs_null_unknown, null_bool_literal()));
            }
            if let Some(null_exists) = null_exists {
                when_then.push((null_exists, null_bool_literal()));
            }
            let replacement = TypedExpr {
                data_type: DataType::Boolean,
                nullable: nullable_result,
                kind: ExprKind::Case {
                    operand: None,
                    when_then,
                    else_expr: Some(Box::new(bool_literal(negated))),
                },
            };
            Self::replace_placeholder_in_filter(&mut select.filter, sq_info.id, &replacement);
            Self::replace_placeholder_in_filter(&mut select.having, sq_info.id, &replacement);
            Self::replace_placeholder_in_projection(
                &mut select.projection,
                sq_info.id,
                &replacement,
            );
        } else {
            // Standard case: SEMI / ANTI JOIN. For NOT IN, NULL handling now
            // lives in the JoinKind itself — pick `NullAwareLeftAnti` when
            // either side could carry NULLs so the exec layer's null-aware
            // anti-join logic kicks in (drops every probe row if the build
            // side has any NULL key; drops probe rows whose key is NULL).
            // For statically non-nullable operands the regular `LeftAnti`
            // already matches SQL semantics.
            let either_nullable = lhs_typed_list
                .iter()
                .zip(resolved_sub.output_columns.iter())
                .any(|(lhs_i, sub_col)| lhs_i.nullable || sub_col.nullable);
            let join_type = if negated {
                if either_nullable {
                    JoinKind::NullAwareLeftAnti
                } else {
                    JoinKind::LeftAnti
                }
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
        scope: &AnalyzerScope,
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

        // Keep the key condition as a plain equality so the optimizer can
        // implement it as a hash join key. For nullable NOT IN semantics,
        // the join type carries the null-aware anti behavior and evaluates
        // residual correlation predicates against matching/null-key build
        // rows.
        let either_nullable = lhs_typed.nullable || rhs_expr.nullable;
        let key_cond = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(lhs_typed),
                op: BinOp::Eq,
                right: Box::new(rhs_expr),
            },
        };
        let sub_filter = sub_filter.map(|filter| {
            if negated && either_nullable && filter.nullable {
                TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: false,
                    kind: ExprKind::FunctionCall {
                        name: "coalesce".to_string(),
                        args: vec![
                            filter,
                            TypedExpr {
                                kind: ExprKind::Literal(LiteralValue::Bool(false)),
                                data_type: DataType::Boolean,
                                nullable: false,
                            },
                        ],
                        distinct: false,
                    },
                }
            } else {
                filter
            }
        });

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
            if either_nullable {
                JoinKind::NullAwareLeftAnti
            } else {
                JoinKind::LeftAnti
            }
        } else {
            JoinKind::LeftSemi
        };

        let current_from = take_from_or_synthesize_single_row(&mut select.from, scope);
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

            let scalar_output_id = modified_sub.output_columns[0].column_id;
            let scalar_output_name = modified_sub.output_columns[0].name.clone();
            let scalar_data_type = modified_sub.output_columns[0].data_type.clone();
            let scalar_nullable = true;

            let output_columns = modified_sub.output_columns.clone();
            let sub_rel = Relation::Subquery {
                query: Box::new(modified_sub),
                alias: sq_alias.clone(),
                output_columns,
            };

            scope.add_column_with_id(
                Some(&sq_alias),
                &scalar_output_name,
                scalar_output_id,
                scalar_data_type.clone(),
                scalar_nullable,
            );

            let current_from = take_from_or_synthesize_single_row(&mut select.from, scope);

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
                    column_id: scalar_output_id,
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

            scope.add_column_with_id(
                Some(&sq_alias),
                &scalar_col.name,
                scalar_col.column_id,
                scalar_col.data_type.clone(),
                scalar_col.nullable,
            );

            let current_from = take_from_or_synthesize_single_row(&mut select.from, scope);

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
                    column_id: scalar_col.column_id,
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
            factory: self.factory.clone(),
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
                    self.new_scope(),
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
            (None, self.new_scope())
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

        // Merged scope: inner tables first (higher priority), then outer scope lookup.
        let mut merged_scope = inner_scope.clone();
        merged_scope.merge(outer_scope);

        // --- WHERE clause ---
        let filter = match &select.selection {
            Some(expr) => Some(qualify_inner_shadowing_column_refs(
                coerce_where_to_bool(self.analyze_expr(expr, &merged_scope)?),
                &inner_scope,
                outer_scope,
            )),
            None => None,
        };

        // --- SELECT list ---
        // Use inner_scope for wildcard expansion (SELECT * should only produce
        // the subquery's own columns, not outer scope columns) but use
        // merged_scope for column/expression resolution so that correlated
        // references can resolve against the outer scope.
        let (mut projection, output_columns) = self.analyze_projection_with_wildcard_scope(
            &select.projection,
            &merged_scope,
            &inner_scope,
        )?;
        for item in &mut projection {
            item.expr =
                qualify_inner_shadowing_column_refs(item.expr.clone(), &inner_scope, outer_scope);
        }

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
                Ok(typed) => group_by.push(qualify_inner_shadowing_column_refs(
                    typed,
                    &inner_scope,
                    outer_scope,
                )),
                Err(_) => {
                    let mut alias_scope = merged_scope.clone();
                    for item in &projection {
                        alias_scope.add_column_with_id(
                            None,
                            &item.output_name,
                            item.output_column_id,
                            item.expr.data_type.clone(),
                            item.expr.nullable,
                        );
                    }
                    let typed = self.analyze_expr(gb_expr, &alias_scope)?;
                    group_by.push(qualify_inner_shadowing_column_refs(
                        self.substitute_select_aliases(typed, &projection),
                        &inner_scope,
                        outer_scope,
                    ));
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
                    Ok(h) => Some(qualify_inner_shadowing_column_refs(
                        h,
                        &inner_scope,
                        outer_scope,
                    )),
                    Err(_) => {
                        let mut alias_scope = merged_scope.clone();
                        for item in &projection {
                            alias_scope.add_column_with_id(
                                None,
                                &item.output_name,
                                item.output_column_id,
                                item.expr.data_type.clone(),
                                item.expr.nullable,
                            );
                        }
                        let h = self.analyze_expr(expr, &alias_scope)?;
                        Some(qualify_inner_shadowing_column_refs(
                            self.substitute_select_aliases(h, &projection),
                            &inner_scope,
                            outer_scope,
                        ))
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
            apply_specs: Vec::new(),
            predicate_apply_specs: Vec::new(),
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
            let corr_col_id = match &inner_col.kind {
                ExprKind::ColumnRef { column_id, .. } => *column_id,
                _ => self.alloc_column_id(
                    None,
                    col_name.clone(),
                    inner_col.data_type.clone(),
                    inner_col.nullable,
                ),
            };
            extra_output.push(OutputColumn {
                column_id: corr_col_id,
                name: col_name.clone(),
                data_type: inner_col.data_type.clone(),
                nullable: inner_col.nullable,
                is_internal: false,
            });
            extra_projection.push(ProjectItem {
                expr: inner_col.clone(),
                output_name: col_name.clone(),
                output_column_id: corr_col_id,
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
                            column_id: corr_col_id,
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
            let mut scope = self.new_scope();
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
/// Coerce a WHERE / ON / HAVING expression to BOOLEAN when its analysed
/// type is not already boolean. MySQL/StarRocks accept `WHERE int_col`
/// (truthy when non-zero, FALSE when zero, NULL when NULL); our analyzer
/// hands such an expression off to downstream AND-conjuncts that
/// require boolean operands ("AND right operand must be boolean"). Wrap
/// non-boolean truthy filters as `expr != 0` (numeric) so the
/// three-valued logic matches.
pub(crate) fn coerce_where_to_bool(expr: TypedExpr) -> TypedExpr {
    use arrow::datatypes::DataType;
    if matches!(expr.data_type, DataType::Boolean) {
        return expr;
    }
    let nullable = expr.nullable;
    let zero = TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(0)),
        data_type: DataType::Int64,
        nullable: false,
    };
    TypedExpr {
        data_type: DataType::Boolean,
        nullable,
        kind: ExprKind::BinaryOp {
            left: Box::new(expr),
            op: BinOp::Ne,
            right: Box::new(zero),
        },
    }
}

fn qualify_inner_shadowing_column_refs(
    expr: TypedExpr,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> TypedExpr {
    let data_type = expr.data_type.clone();
    let nullable = expr.nullable;
    match expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier: None,
            column,
        } => {
            let qualifier = inner_scope
                .resolve(None, &column)
                .ok()
                .filter(|(inner_id, _, _)| *inner_id == column_id)
                .filter(|_| outer_scope.resolve(None, &column).is_ok())
                .and_then(|_| inner_scope.qualifier_for_binding(&column, column_id));
            TypedExpr {
                data_type,
                nullable,
                kind: ExprKind::ColumnRef {
                    column_id,
                    qualifier,
                    column,
                },
            }
        }
        ExprKind::BinaryOp { left, op, right } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(qualify_inner_shadowing_column_refs(
                    *left,
                    inner_scope,
                    outer_scope,
                )),
                op,
                right: Box::new(qualify_inner_shadowing_column_refs(
                    *right,
                    inner_scope,
                    outer_scope,
                )),
            },
        },
        ExprKind::UnaryOp { op, expr: inner } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::UnaryOp {
                op,
                expr: Box::new(qualify_inner_shadowing_column_refs(
                    *inner,
                    inner_scope,
                    outer_scope,
                )),
            },
        },
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::FunctionCall {
                name,
                args: args
                    .into_iter()
                    .map(|arg| qualify_inner_shadowing_column_refs(arg, inner_scope, outer_scope))
                    .collect(),
                distinct,
            },
        },
        ExprKind::LambdaFunction { params, body } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::LambdaFunction {
                params,
                body: Box::new(qualify_inner_shadowing_column_refs(
                    *body,
                    inner_scope,
                    outer_scope,
                )),
            },
        },
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::AggregateCall {
                name,
                args: args
                    .into_iter()
                    .map(|arg| qualify_inner_shadowing_column_refs(arg, inner_scope, outer_scope))
                    .collect(),
                distinct,
                order_by: qualify_inner_shadowing_sort_items(order_by, inner_scope, outer_scope),
            },
        },
        ExprKind::Cast {
            expr: inner,
            target,
        } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::Cast {
                expr: Box::new(qualify_inner_shadowing_column_refs(
                    *inner,
                    inner_scope,
                    outer_scope,
                )),
                target,
            },
        },
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::IsNull {
                expr: Box::new(qualify_inner_shadowing_column_refs(
                    *inner,
                    inner_scope,
                    outer_scope,
                )),
                negated,
            },
        },
        ExprKind::InList {
            expr: inner,
            list,
            negated,
        } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::InList {
                expr: Box::new(qualify_inner_shadowing_column_refs(
                    *inner,
                    inner_scope,
                    outer_scope,
                )),
                list: list
                    .into_iter()
                    .map(|item| qualify_inner_shadowing_column_refs(item, inner_scope, outer_scope))
                    .collect(),
                negated,
            },
        },
        ExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::Between {
                expr: Box::new(qualify_inner_shadowing_column_refs(
                    *inner,
                    inner_scope,
                    outer_scope,
                )),
                low: Box::new(qualify_inner_shadowing_column_refs(
                    *low,
                    inner_scope,
                    outer_scope,
                )),
                high: Box::new(qualify_inner_shadowing_column_refs(
                    *high,
                    inner_scope,
                    outer_scope,
                )),
                negated,
            },
        },
        ExprKind::Like {
            expr: inner,
            pattern,
            negated,
        } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::Like {
                expr: Box::new(qualify_inner_shadowing_column_refs(
                    *inner,
                    inner_scope,
                    outer_scope,
                )),
                pattern: Box::new(qualify_inner_shadowing_column_refs(
                    *pattern,
                    inner_scope,
                    outer_scope,
                )),
                negated,
            },
        },
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::Case {
                operand: operand.map(|operand| {
                    Box::new(qualify_inner_shadowing_column_refs(
                        *operand,
                        inner_scope,
                        outer_scope,
                    ))
                }),
                when_then: when_then
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            qualify_inner_shadowing_column_refs(when, inner_scope, outer_scope),
                            qualify_inner_shadowing_column_refs(then, inner_scope, outer_scope),
                        )
                    })
                    .collect(),
                else_expr: else_expr.map(|else_expr| {
                    Box::new(qualify_inner_shadowing_column_refs(
                        *else_expr,
                        inner_scope,
                        outer_scope,
                    ))
                }),
            },
        },
        ExprKind::IsTruthValue {
            expr: inner,
            value,
            negated,
        } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::IsTruthValue {
                expr: Box::new(qualify_inner_shadowing_column_refs(
                    *inner,
                    inner_scope,
                    outer_scope,
                )),
                value,
                negated,
            },
        },
        ExprKind::Nested(inner) => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::Nested(Box::new(qualify_inner_shadowing_column_refs(
                *inner,
                inner_scope,
                outer_scope,
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
            data_type,
            nullable,
            kind: ExprKind::WindowCall {
                name,
                args: args
                    .into_iter()
                    .map(|arg| qualify_inner_shadowing_column_refs(arg, inner_scope, outer_scope))
                    .collect(),
                distinct,
                partition_by: partition_by
                    .into_iter()
                    .map(|item| qualify_inner_shadowing_column_refs(item, inner_scope, outer_scope))
                    .collect(),
                order_by: qualify_inner_shadowing_sort_items(order_by, inner_scope, outer_scope),
                window_frame,
                ignore_nulls,
            },
        },
        ExprKind::Lambda { params, body } => TypedExpr {
            data_type,
            nullable,
            kind: ExprKind::Lambda {
                params,
                body: Box::new(qualify_inner_shadowing_column_refs(
                    *body,
                    inner_scope,
                    outer_scope,
                )),
            },
        },
        kind @ (ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. }) => TypedExpr {
            data_type,
            nullable,
            kind,
        },
    }
}

fn qualify_inner_shadowing_sort_items(
    items: Vec<SortItem>,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> Vec<SortItem> {
    items
        .into_iter()
        .map(|item| SortItem {
            expr: qualify_inner_shadowing_column_refs(item.expr, inner_scope, outer_scope),
            asc: item.asc,
            nulls_first: item.nulls_first,
        })
        .collect()
}

/// `true` when `expr` contains at least one column reference that resolves
/// only in `outer_scope` and not in `inner_scope`. Broader than
/// `extract_correlation_predicates` (which looks for comparison-shaped
/// predicates) — needed so a bare WHERE clause like `WHERE outer.flag`
/// (implicit `!= 0` boolean) still routes the IN/NOT IN rewrite through
/// the correlated path that lifts the outer reference up into the join
/// ON condition.
fn expr_references_outer_scope(
    expr: &TypedExpr,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> bool {
    let mut saw_outer = false;
    walk_for_outer_ref(expr, inner_scope, outer_scope, &mut saw_outer);
    saw_outer
}

fn collect_outer_ref_column_ids(
    expr: &TypedExpr,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
    out: &mut Vec<crate::sql::column_id::ColumnId>,
) {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            let inner_has = inner_scope.contains_column_id(*column_id);
            let outer_has = outer_scope.contains_column_id(*column_id);
            if !inner_has && outer_has {
                out.push(*column_id);
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_outer_ref_column_ids(left, inner_scope, outer_scope, out);
            collect_outer_ref_column_ids(right, inner_scope, outer_scope, out);
        }
        ExprKind::UnaryOp { expr: inner, .. }
        | ExprKind::IsNull { expr: inner, .. }
        | ExprKind::IsTruthValue { expr: inner, .. }
        | ExprKind::Cast { expr: inner, .. }
        | ExprKind::Nested(inner)
        | ExprKind::LambdaFunction { body: inner, .. }
        | ExprKind::Lambda { body: inner, .. } => {
            collect_outer_ref_column_ids(inner, inner_scope, outer_scope, out);
        }
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_outer_ref_column_ids(arg, inner_scope, outer_scope, out);
            }
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_outer_ref_column_ids(arg, inner_scope, outer_scope, out);
            }
            for item in order_by {
                collect_outer_ref_column_ids(&item.expr, inner_scope, outer_scope, out);
            }
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_outer_ref_column_ids(operand, inner_scope, outer_scope, out);
            }
            for (when, then) in when_then {
                collect_outer_ref_column_ids(when, inner_scope, outer_scope, out);
                collect_outer_ref_column_ids(then, inner_scope, outer_scope, out);
            }
            if let Some(else_expr) = else_expr {
                collect_outer_ref_column_ids(else_expr, inner_scope, outer_scope, out);
            }
        }
        ExprKind::InList {
            expr: inner, list, ..
        } => {
            collect_outer_ref_column_ids(inner, inner_scope, outer_scope, out);
            for item in list {
                collect_outer_ref_column_ids(item, inner_scope, outer_scope, out);
            }
        }
        ExprKind::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            collect_outer_ref_column_ids(inner, inner_scope, outer_scope, out);
            collect_outer_ref_column_ids(low, inner_scope, outer_scope, out);
            collect_outer_ref_column_ids(high, inner_scope, outer_scope, out);
        }
        ExprKind::Like {
            expr: inner,
            pattern,
            ..
        } => {
            collect_outer_ref_column_ids(inner, inner_scope, outer_scope, out);
            collect_outer_ref_column_ids(pattern, inner_scope, outer_scope, out);
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_outer_ref_column_ids(arg, inner_scope, outer_scope, out);
            }
            for partition in partition_by {
                collect_outer_ref_column_ids(partition, inner_scope, outer_scope, out);
            }
            for item in order_by {
                collect_outer_ref_column_ids(&item.expr, inner_scope, outer_scope, out);
            }
        }
        ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. }
        | ExprKind::LambdaParamRef { .. } => {}
    }
}

#[derive(Default)]
struct SubqueryOuterRefUsage {
    filter: bool,
    outside_filter: bool,
}

fn collect_subquery_outer_ref_usage(
    resolved_sub: &crate::sql::analysis::ResolvedQuery,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> SubqueryOuterRefUsage {
    let mut usage = SubqueryOuterRefUsage::default();
    if sort_items_reference_outer_scope(&resolved_sub.order_by, inner_scope, outer_scope) {
        usage.outside_filter = true;
    }

    match &resolved_sub.body {
        QueryBody::Select(sel) => {
            if let Some(f) = &sel.filter {
                usage.filter = expr_references_outer_scope(f, inner_scope, outer_scope);
            }
            if relation_references_outer_scope(&sel.from, inner_scope, outer_scope)
                || sel
                    .projection
                    .iter()
                    .any(|p| expr_references_outer_scope(&p.expr, inner_scope, outer_scope))
                || sel
                    .group_by
                    .iter()
                    .any(|g| expr_references_outer_scope(g, inner_scope, outer_scope))
                || sel
                    .having
                    .as_ref()
                    .is_some_and(|h| expr_references_outer_scope(h, inner_scope, outer_scope))
            {
                usage.outside_filter = true;
            }
        }
        QueryBody::SetOperation(set) => {
            if query_references_outer_scope(&set.left, inner_scope, outer_scope)
                || query_references_outer_scope(&set.right, inner_scope, outer_scope)
            {
                usage.outside_filter = true;
            }
        }
        QueryBody::Values(values) => {
            if values
                .rows
                .iter()
                .flatten()
                .any(|expr| expr_references_outer_scope(expr, inner_scope, outer_scope))
            {
                usage.outside_filter = true;
            }
        }
    }
    usage
}

fn query_references_outer_scope(
    query: &crate::sql::analysis::ResolvedQuery,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> bool {
    let usage = collect_subquery_outer_ref_usage(query, inner_scope, outer_scope);
    usage.filter || usage.outside_filter
}

fn relation_references_outer_scope(
    relation: &Option<Relation>,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> bool {
    relation
        .as_ref()
        .is_some_and(|rel| relation_node_references_outer_scope(rel, inner_scope, outer_scope))
}

fn relation_node_references_outer_scope(
    relation: &Relation,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> bool {
    match relation {
        Relation::Join(join) => {
            relation_node_references_outer_scope(&join.left, inner_scope, outer_scope)
                || relation_node_references_outer_scope(&join.right, inner_scope, outer_scope)
                || join
                    .condition
                    .as_ref()
                    .is_some_and(|cond| expr_references_outer_scope(cond, inner_scope, outer_scope))
        }
        Relation::Subquery { query, .. } => {
            query_references_outer_scope(query, inner_scope, outer_scope)
        }
        Relation::Unnest(unnest) => unnest
            .args
            .iter()
            .any(|arg| expr_references_outer_scope(arg, inner_scope, outer_scope)),
        Relation::Scan(_)
        | Relation::IcebergMetadataScan(_)
        | Relation::IcebergDeltaScan(_)
        | Relation::GenerateSeries(_)
        | Relation::CTEConsume { .. } => false,
    }
}

fn sort_items_reference_outer_scope(
    items: &[SortItem],
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> bool {
    items
        .iter()
        .any(|item| expr_references_outer_scope(&item.expr, inner_scope, outer_scope))
}

fn is_placeholder_top_level_and_conjunct(expr: &TypedExpr, id: usize) -> bool {
    if is_placeholder(expr, id) {
        return true;
    }
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            is_placeholder_top_level_and_conjunct(left, id)
                || is_placeholder_top_level_and_conjunct(right, id)
        }
        ExprKind::Nested(inner) => is_placeholder_top_level_and_conjunct(inner, id),
        _ => false,
    }
}

fn walk_for_outer_ref(
    expr: &TypedExpr,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
    saw_outer: &mut bool,
) {
    if *saw_outer {
        return;
    }
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            let inner_has = inner_scope.contains_column_id(*column_id);
            let outer_has = outer_scope.contains_column_id(*column_id);
            if !inner_has && outer_has {
                *saw_outer = true;
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            walk_for_outer_ref(left, inner_scope, outer_scope, saw_outer);
            walk_for_outer_ref(right, inner_scope, outer_scope, saw_outer);
        }
        ExprKind::UnaryOp { expr: inner, .. }
        | ExprKind::IsNull { expr: inner, .. }
        | ExprKind::IsTruthValue { expr: inner, .. }
        | ExprKind::Cast { expr: inner, .. }
        | ExprKind::Nested(inner)
        | ExprKind::LambdaFunction { body: inner, .. }
        | ExprKind::Lambda { body: inner, .. } => {
            walk_for_outer_ref(inner, inner_scope, outer_scope, saw_outer);
        }
        ExprKind::FunctionCall { args, .. } => {
            for a in args {
                walk_for_outer_ref(a, inner_scope, outer_scope, saw_outer);
            }
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            for a in args {
                walk_for_outer_ref(a, inner_scope, outer_scope, saw_outer);
            }
            for item in order_by {
                walk_for_outer_ref(&item.expr, inner_scope, outer_scope, saw_outer);
            }
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                walk_for_outer_ref(op, inner_scope, outer_scope, saw_outer);
            }
            for (w, t) in when_then {
                walk_for_outer_ref(w, inner_scope, outer_scope, saw_outer);
                walk_for_outer_ref(t, inner_scope, outer_scope, saw_outer);
            }
            if let Some(e) = else_expr {
                walk_for_outer_ref(e, inner_scope, outer_scope, saw_outer);
            }
        }
        ExprKind::InList {
            expr: inner, list, ..
        } => {
            walk_for_outer_ref(inner, inner_scope, outer_scope, saw_outer);
            for v in list {
                walk_for_outer_ref(v, inner_scope, outer_scope, saw_outer);
            }
        }
        ExprKind::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            walk_for_outer_ref(inner, inner_scope, outer_scope, saw_outer);
            walk_for_outer_ref(low, inner_scope, outer_scope, saw_outer);
            walk_for_outer_ref(high, inner_scope, outer_scope, saw_outer);
        }
        ExprKind::Like {
            expr: inner,
            pattern,
            ..
        } => {
            walk_for_outer_ref(inner, inner_scope, outer_scope, saw_outer);
            walk_for_outer_ref(pattern, inner_scope, outer_scope, saw_outer);
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for a in args {
                walk_for_outer_ref(a, inner_scope, outer_scope, saw_outer);
            }
            for p in partition_by {
                walk_for_outer_ref(p, inner_scope, outer_scope, saw_outer);
            }
            for item in order_by {
                walk_for_outer_ref(&item.expr, inner_scope, outer_scope, saw_outer);
            }
        }
        // Literal / LambdaParamRef / SubqueryPlaceholder / etc. — no
        // sub-expressions that could carry outer references in the
        // contexts this helper covers.
        _ => {}
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
            BinOp::Eq
            | BinOp::EqForNull
            | BinOp::Ne
            | BinOp::Lt
            | BinOp::Le
            | BinOp::Gt
            | BinOp::Ge => {
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
                        BinOp::EqForNull => BinOp::EqForNull,
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

/// Check if an expression is "outer-only" from the subquery's point of view:
/// every column reference in the expression resolves in the outer scope and
/// does NOT resolve in the inner scope, AND the expression itself contains
/// at least one column reference (i.e. it's not a pure constant).
///
/// This is the test used by `extract_correlation_predicates` to decide
/// which side of a comparison is the outer ("correlation") side. We must
/// recurse into function calls / arithmetic / nested expressions so a
/// correlation hidden behind a wrapper like `r.k3 = coalesce(l.k3, 2)`
/// is still recognised — otherwise the inner query is analysed without
/// outer-column visibility and the `l.k3` reference fails to resolve.
fn is_outer_only_ref(
    expr: &TypedExpr,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
) -> bool {
    let mut saw_column = false;
    let mut all_outer_only = true;
    collect_column_outer_status(
        expr,
        inner_scope,
        outer_scope,
        &mut saw_column,
        &mut all_outer_only,
    );
    saw_column && all_outer_only
}

fn collect_column_outer_status(
    expr: &TypedExpr,
    inner_scope: &AnalyzerScope,
    outer_scope: &AnalyzerScope,
    saw_column: &mut bool,
    all_outer_only: &mut bool,
) {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            *saw_column = true;
            let in_inner = inner_scope.contains_column_id(*column_id);
            let in_outer = outer_scope.contains_column_id(*column_id);
            if !(in_outer && !in_inner) {
                *all_outer_only = false;
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_column_outer_status(left, inner_scope, outer_scope, saw_column, all_outer_only);
            collect_column_outer_status(
                right,
                inner_scope,
                outer_scope,
                saw_column,
                all_outer_only,
            );
        }
        ExprKind::UnaryOp { expr: inner, .. } => {
            collect_column_outer_status(
                inner,
                inner_scope,
                outer_scope,
                saw_column,
                all_outer_only,
            );
        }
        ExprKind::IsNull { expr: inner, .. } => {
            collect_column_outer_status(
                inner,
                inner_scope,
                outer_scope,
                saw_column,
                all_outer_only,
            );
        }
        ExprKind::Cast { expr: inner, .. } => {
            collect_column_outer_status(
                inner,
                inner_scope,
                outer_scope,
                saw_column,
                all_outer_only,
            );
        }
        ExprKind::Nested(inner) => {
            collect_column_outer_status(
                inner,
                inner_scope,
                outer_scope,
                saw_column,
                all_outer_only,
            );
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            for a in args {
                collect_column_outer_status(
                    a,
                    inner_scope,
                    outer_scope,
                    saw_column,
                    all_outer_only,
                );
            }
        }
        ExprKind::InList {
            expr: inner, list, ..
        } => {
            collect_column_outer_status(
                inner,
                inner_scope,
                outer_scope,
                saw_column,
                all_outer_only,
            );
            for item in list {
                collect_column_outer_status(
                    item,
                    inner_scope,
                    outer_scope,
                    saw_column,
                    all_outer_only,
                );
            }
        }
        ExprKind::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            collect_column_outer_status(
                inner,
                inner_scope,
                outer_scope,
                saw_column,
                all_outer_only,
            );
            collect_column_outer_status(low, inner_scope, outer_scope, saw_column, all_outer_only);
            collect_column_outer_status(high, inner_scope, outer_scope, saw_column, all_outer_only);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_column_outer_status(
                    op,
                    inner_scope,
                    outer_scope,
                    saw_column,
                    all_outer_only,
                );
            }
            for (when, then) in when_then {
                collect_column_outer_status(
                    when,
                    inner_scope,
                    outer_scope,
                    saw_column,
                    all_outer_only,
                );
                collect_column_outer_status(
                    then,
                    inner_scope,
                    outer_scope,
                    saw_column,
                    all_outer_only,
                );
            }
            if let Some(else_) = else_expr {
                collect_column_outer_status(
                    else_,
                    inner_scope,
                    outer_scope,
                    saw_column,
                    all_outer_only,
                );
            }
        }
        // Literals / placeholders / lambda params: no column refs, leave
        // saw_column / all_outer_only unchanged.
        _ => {}
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
        output_column_id: crate::sql::column_id::ColumnId::UNSET,
    })
}

/// Return the first output column from `rel` that is actually produced by the
/// physical plan when `rel` is attached as an auxiliary join.
///
/// Used by `rewrite_join_on_exists_correlated` to pick an indicator column
/// for the `IS NOT NULL` check.  The indicator MUST come from the relation
/// that is attached to the plan (i.e. `sub_rel`), NOT from the subquery's
/// SELECT projection — the projection is discarded when the EXISTS subquery
/// is deconstructed into `(FROM, WHERE)` and the `SELECT <projection>` is
/// not materialised.
///
/// For `CTEConsume` and `Subquery` relations the output_columns are
/// authoritative.  For `Scan`-family relations we pair the first table column
/// with its analyzer-allocated ColumnId.  For `Join` we recurse left.  If no
/// column can be determined, return `None` so the caller can raise a clear
/// shape-specific error.
fn relation_first_output_column(rel: &Relation) -> Option<OutputColumn> {
    match rel {
        Relation::CTEConsume { output_columns, .. } => output_columns.first().cloned(),
        Relation::Subquery { output_columns, .. } => output_columns.first().cloned(),
        Relation::Unnest(u) => u.output_columns.first().cloned(),
        Relation::Scan(s) => {
            // Pair the first table column definition with its analyzer ColumnId.
            let col_def = s.table.columns.first()?;
            let col_id = s.column_ids.first().copied()?;
            Some(OutputColumn {
                column_id: col_id,
                name: col_def.name.clone(),
                data_type: col_def.data_type.clone(),
                nullable: col_def.nullable,
                is_internal: false,
            })
        }
        Relation::IcebergMetadataScan(s) => {
            let col_def = s.table.columns.first()?;
            let col_id = s.column_ids.first().copied()?;
            Some(OutputColumn {
                column_id: col_id,
                name: col_def.name.clone(),
                data_type: col_def.data_type.clone(),
                nullable: col_def.nullable,
                is_internal: false,
            })
        }
        Relation::IcebergDeltaScan(s) => {
            let col_def = s.table.columns.first()?;
            let col_id = s.column_ids.first().copied()?;
            Some(OutputColumn {
                column_id: col_id,
                name: col_def.name.clone(),
                data_type: col_def.data_type.clone(),
                nullable: col_def.nullable,
                is_internal: false,
            })
        }
        Relation::Join(j) => relation_first_output_column(&j.left),
        Relation::GenerateSeries(g) => {
            if g.output_column_id == crate::sql::column_id::ColumnId::UNSET {
                None
            } else {
                Some(OutputColumn {
                    column_id: g.output_column_id,
                    name: g.column_name.clone(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                })
            }
        }
    }
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

// ---------------------------------------------------------------------------
// Apply helper free functions
// ---------------------------------------------------------------------------

/// Determine which clause of the SELECT contains the scalar subquery
/// placeholder. Checks WHERE first, then HAVING, then the projection list.
/// Returns None if the placeholder is in JOIN-ON or another non-Apply route.
fn locate_scalar_placeholder_clause(
    select: &ResolvedSelect,
    placeholder_id: usize,
) -> Option<crate::sql::analysis::ApplyClause> {
    if select
        .filter
        .as_ref()
        .map(|f| expr_contains_placeholder(f, placeholder_id))
        .unwrap_or(false)
    {
        return Some(crate::sql::analysis::ApplyClause::Where);
    }
    if select
        .having
        .as_ref()
        .map(|f| expr_contains_placeholder(f, placeholder_id))
        .unwrap_or(false)
    {
        return Some(crate::sql::analysis::ApplyClause::Having);
    }
    if select
        .projection
        .iter()
        .any(|p| expr_contains_placeholder(&p.expr, placeholder_id))
    {
        return Some(crate::sql::analysis::ApplyClause::Projection);
    }
    None
}

fn predicate_placeholder_is_value_form(select: &ResolvedSelect, placeholder_id: usize) -> bool {
    select
        .projection
        .iter()
        .any(|p| expr_contains_placeholder(&p.expr, placeholder_id))
        || select
            .filter
            .as_ref()
            .map(|f| {
                expr_contains_placeholder(f, placeholder_id)
                    && !is_placeholder_top_level_and_conjunct(f, placeholder_id)
            })
            .unwrap_or(false)
        || select
            .having
            .as_ref()
            .map(|f| {
                expr_contains_placeholder(f, placeholder_id)
                    && !is_placeholder_top_level_and_conjunct(f, placeholder_id)
            })
            .unwrap_or(false)
}

/// Collect the outer ColumnIds referenced by the correlation predicates of an
/// analyzed inner subquery. For each CorrelationPred, walks the `outer_col`
/// expression and collects every ColumnRef id (the outer side can be a wrapped
/// expression like `coalesce(l.k, 2)`, not just a bare column ref). Returns a
/// deduplicated Vec. Returns empty if the inner query is not correlated.
fn collect_correlation_column_ids(
    resolved_sub: &crate::sql::analysis::ResolvedQuery,
    inner_scope: &super::scope::AnalyzerScope,
    outer_scope: &super::scope::AnalyzerScope,
) -> Vec<crate::sql::column_id::ColumnId> {
    use crate::sql::analysis::QueryBody;
    let filter = match &resolved_sub.body {
        QueryBody::Select(sel) => match &sel.filter {
            Some(f) => f,
            None => return Vec::new(),
        },
        _ => return Vec::new(),
    };
    let preds = extract_correlation_predicates(filter, inner_scope, outer_scope);
    let mut ids: Vec<crate::sql::column_id::ColumnId> = Vec::new();
    for pred in &preds {
        collect_column_ids_in_expr(&pred.outer_col, &mut ids);
    }
    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(*id));
    ids
}

/// Collect filter outer-reference column ids for predicate Apply routing.
///
/// Predicate Apply-to-join rules lift the whole correlated inner filter into a
/// join ON predicate, so the correlation anchor may be a comparison, IF/CASE,
/// BETWEEN, IS TRUE, or another boolean expression. The ids only need to tell
/// the planner which outer columns are legal while the Apply is alive.
fn collect_predicate_correlation_column_ids_for_apply(
    resolved_sub: &crate::sql::analysis::ResolvedQuery,
    inner_scope: &super::scope::AnalyzerScope,
    outer_scope: &super::scope::AnalyzerScope,
) -> Vec<crate::sql::column_id::ColumnId> {
    use crate::sql::analysis::QueryBody;
    let filter = match &resolved_sub.body {
        QueryBody::Select(sel) => match &sel.filter {
            Some(f) => f,
            None => return Vec::new(),
        },
        _ => return Vec::new(),
    };
    let mut ids: Vec<crate::sql::column_id::ColumnId> = Vec::new();
    collect_outer_ref_column_ids(filter, inner_scope, outer_scope, &mut ids);
    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(*id));
    ids
}

/// Recursively collect every ColumnRef column_id appearing in `expr`.
fn collect_column_ids_in_expr(expr: &TypedExpr, out: &mut Vec<crate::sql::column_id::ColumnId>) {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            out.push(*column_id);
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_column_ids_in_expr(left, out);
            collect_column_ids_in_expr(right, out);
        }
        ExprKind::UnaryOp { expr: inner, .. } => {
            collect_column_ids_in_expr(inner, out);
        }
        ExprKind::IsNull { expr: inner, .. } => {
            collect_column_ids_in_expr(inner, out);
        }
        ExprKind::Cast { expr: inner, .. } => {
            collect_column_ids_in_expr(inner, out);
        }
        ExprKind::Nested(inner) => {
            collect_column_ids_in_expr(inner, out);
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            for a in args {
                collect_column_ids_in_expr(a, out);
            }
        }
        // Intentionally limited to the expression kinds a correlation predicate
        // can wrap (refs, binary ops, casts, function args). Correlation hidden
        // inside other shapes (CASE/IN/BETWEEN) is not collected — acceptable
        // for the EQ-comparison correlation M1 supports.
        _ => {}
    }
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
        ExprKind::UnaryOp { expr: inner, .. } => expr_contains_placeholder(inner, placeholder_id),
        ExprKind::IsNull { expr: inner, .. } => expr_contains_placeholder(inner, placeholder_id),
        ExprKind::Cast { expr: inner, .. } => expr_contains_placeholder(inner, placeholder_id),
        ExprKind::Nested(inner) => expr_contains_placeholder(inner, placeholder_id),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => args
            .iter()
            .any(|a| expr_contains_placeholder(a, placeholder_id)),
        ExprKind::InList {
            expr: inner, list, ..
        } => {
            expr_contains_placeholder(inner, placeholder_id)
                || list
                    .iter()
                    .any(|i| expr_contains_placeholder(i, placeholder_id))
        }
        ExprKind::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            expr_contains_placeholder(inner, placeholder_id)
                || expr_contains_placeholder(low, placeholder_id)
                || expr_contains_placeholder(high, placeholder_id)
        }
        ExprKind::Like {
            expr: inner,
            pattern,
            ..
        } => {
            expr_contains_placeholder(inner, placeholder_id)
                || expr_contains_placeholder(pattern, placeholder_id)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand
                && expr_contains_placeholder(op, placeholder_id)
            {
                return true;
            }
            for (when, then) in when_then {
                if expr_contains_placeholder(when, placeholder_id)
                    || expr_contains_placeholder(then, placeholder_id)
                {
                    return true;
                }
            }
            if let Some(else_) = else_expr
                && expr_contains_placeholder(else_, placeholder_id)
            {
                return true;
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
        Relation::IcebergDeltaScan(s) => {
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
        Relation::IcebergDeltaScan(s) => {
            s.table
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(col_lower))
                || s.table
                    .iceberg_row_lineage_metadata_columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(col_lower))
        }
        Relation::Subquery { output_columns, .. } => output_columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(col_lower)),
        Relation::CTEConsume { output_columns, .. } => output_columns
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
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => {
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

fn recompute_case_result_type(
    when_then: &[(TypedExpr, TypedExpr)],
    else_expr: Option<&TypedExpr>,
) -> DataType {
    let mut result_type = DataType::Null;
    for (_, then_expr) in when_then {
        if result_type == DataType::Null {
            result_type = then_expr.data_type.clone();
        } else {
            result_type = crate::sql::types::wider_type(&result_type, &then_expr.data_type);
        }
    }
    if let Some(expr) = else_expr {
        if result_type == DataType::Null {
            result_type = expr.data_type.clone();
        } else {
            result_type = crate::sql::types::wider_type(&result_type, &expr.data_type);
        }
    }
    if result_type == DataType::Null {
        DataType::Utf8
    } else {
        result_type
    }
}

fn cast_case_branch_if_needed(expr: TypedExpr, target: &DataType) -> TypedExpr {
    if &expr.data_type != target && expr.data_type != DataType::Null {
        TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(expr),
                target: target.clone(),
            },
            data_type: target.clone(),
            nullable: true,
        }
    } else {
        expr
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
        } => {
            let operand = operand
                .as_ref()
                .map(|o| Box::new(replace_placeholder_in_expr(o, placeholder_id, replacement)));
            let mut rewritten_when_then: Vec<(TypedExpr, TypedExpr)> = when_then
                .iter()
                .map(|(w, t)| {
                    (
                        replace_placeholder_in_expr(w, placeholder_id, replacement),
                        replace_placeholder_in_expr(t, placeholder_id, replacement),
                    )
                })
                .collect();
            let mut else_expr = else_expr
                .as_ref()
                .map(|e| Box::new(replace_placeholder_in_expr(e, placeholder_id, replacement)));

            let result_type =
                recompute_case_result_type(&rewritten_when_then, else_expr.as_deref());
            for (_, then_expr) in &mut rewritten_when_then {
                *then_expr = cast_case_branch_if_needed(then_expr.clone(), &result_type);
            }
            if let Some(expr) = else_expr.take() {
                else_expr = Some(Box::new(cast_case_branch_if_needed(*expr, &result_type)));
            }

            TypedExpr {
                data_type: result_type,
                nullable: true,
                kind: ExprKind::Case {
                    operand,
                    when_then: rewritten_when_then,
                    else_expr,
                },
            }
        }
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
