//! Semantic analyzer: converts `sqlparser::ast::Query` into `ResolvedQuery`.
//!
//! This module performs name resolution, type inference, and scope management
//! without producing any physical plan concepts (tuple_id, slot_id, etc.).

mod scope;
mod functions;
mod helpers;
mod resolve_from;
mod resolve_expr;

use arrow::datatypes::DataType;
use sqlparser::ast as sqlast;

use crate::sql::catalog::CatalogProvider;

use crate::sql::ir::{
    ExprKind, JoinKind, JoinRelation, OutputColumn,
    ProjectItem, QueryBody, Relation, ResolvedQuery, ResolvedSelect, ResolvedSetOp,
    ResolvedValues, SetOpKind, SortItem, TypedExpr,
};
use crate::sql::types::wider_type;

use scope::AnalyzerScope;
use helpers::{extract_limit, extract_offset, expr_display_name};

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
}

impl<'a> AnalyzerContext<'a> {
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
        let (projection, output_columns) =
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

        Ok((
            ResolvedSelect {
                from,
                filter,
                group_by,
                having,
                projection,
                has_aggregation,
                distinct,
            },
            output_columns,
        ))
    }

    /// Replace ColumnRef nodes that match SELECT aliases with the aliased expression.
    fn substitute_select_aliases(
        &self,
        expr: TypedExpr,
        projection: &[ProjectItem],
    ) -> TypedExpr {
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
                        return Err(format!(
                            "no columns found for qualifier `{qualifier_str}`"
                        ));
                    }
                }
            }
        }

        Ok((projection, output_columns))
    }

    /// Rebuild the FROM scope from an already-resolved Relation tree.
    /// Used by ORDER BY fallback when the expression doesn't match projection columns.
    fn rebuild_from_scope(
        &self,
        relation: &Relation,
    ) -> Result<((), AnalyzerScope), String> {
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
