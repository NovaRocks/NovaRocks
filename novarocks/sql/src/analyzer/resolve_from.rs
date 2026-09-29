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

use arrow::datatypes::DataType;
use novarocks_parser::ast;
use novarocks_parser::printer::print_expr;

use crate::analysis::*;
use crate::analyze_error::AnalyzeError;
use crate::column_id::ColumnId;

use super::scope::AnalyzerScope;

impl<'a> super::AnalyzerContext<'a> {
    /// Analyze a FROM clause (TableWithJoins).
    pub(super) fn analyze_from(
        &self,
        twj: &ast::TableWithJoins,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        self.analyze_from_with_outer(twj, None)
    }

    /// Analyze a FROM clause with an optional outer scope visible to the
    /// first relation. Used when comma-separated FROM entries (each parsed as
    /// its own TableWithJoins) need to see earlier sibling scopes so that
    /// table-valued functions like `unnest(...)` can reference outer columns.
    pub(super) fn analyze_from_with_outer(
        &self,
        twj: &ast::TableWithJoins,
        outer_scope: Option<&AnalyzerScope>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        let (mut current_rel, mut current_scope) =
            self.analyze_table_factor_with_outer(&twj.relation, outer_scope)?;

        for join in &twj.joins {
            let (right_rel, mut right_scope) =
                self.analyze_table_factor_with_outer(&join.relation, Some(&current_scope))?;

            let (join_kind, constraint) = parse_join_operator(join.operator, &join.constraint)?;

            let condition = match constraint {
                Some(ast::JoinConstraint::On(on_expr)) => {
                    // Build a merged scope for analyzing the ON condition
                    let mut merged = self.new_scope();
                    merged.merge(&current_scope);
                    merged.merge(&right_scope);
                    Some(self.analyze_expr(on_expr, &merged)?)
                }
                Some(ast::JoinConstraint::Using { columns, .. }) => {
                    // Convert USING(col1, col2) to ON left.col1 = right.col1 AND ...
                    //
                    // Each USING column resolves to *different* ColumnIds on
                    // each side of the join (the LEFT child's binding and the
                    // RIGHT child's binding). We build the BinaryOp with
                    // distinct left/right ColumnRefs so that downstream
                    // optimizer passes (in particular `shuffle_join_column_ids`
                    // and HashPartitioned distribution derivation) see both
                    // ids as the join key on its respective side. Using the
                    // same merged id on both sides causes the optimizer to
                    // produce a distribution spec keyed on only one id, which
                    // fragment-builder cannot resolve against the other
                    // child's scope.
                    let mut conds = Vec::new();
                    for col_obj in columns {
                        let col_name = col_obj.value.clone();
                        let (left_id, left_dt, left_nullable) = current_scope
                            .resolve(None, &col_name)
                            .unwrap_or((crate::column_id::ColumnId::UNSET, DataType::Utf8, true));
                        let (right_id, right_dt, right_nullable) = right_scope
                            .resolve(None, &col_name)
                            .unwrap_or((crate::column_id::ColumnId::UNSET, DataType::Utf8, true));
                        // After a previous FULL OUTER USING the left side
                        // carries a synthetic COALESCE for this column —
                        // `coalesce(prev_left.id, prev_right.id)` — because
                        // either side may be NULL-padded. The bare
                        // `left.id` reference returned by
                        // `current_scope.resolve` only sees ONE specific
                        // side's binding, so a row whose id originated on
                        // the other side compares as `NULL = right.id`
                        // and never joins. Use the computed column when it
                        // exists so the chained join matches on the merged
                        // value (`coalesce(coalesce(t1.id, t2.id), t3.id)`,
                        // and so on).
                        let left_ref =
                            if let Some(expr) = current_scope.computed_column_for(&col_name) {
                                expr.clone()
                            } else {
                                TypedExpr {
                                    kind: ExprKind::ColumnRef {
                                        column_id: left_id,
                                        qualifier: None,
                                        column: col_name.clone(),
                                    },
                                    data_type: left_dt,
                                    nullable: left_nullable,
                                }
                            };
                        let right_ref = TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: right_id,
                                qualifier: None,
                                column: col_name,
                            },
                            data_type: right_dt,
                            nullable: right_nullable,
                        };
                        conds.push(TypedExpr {
                            data_type: DataType::Boolean,
                            nullable: false,
                            kind: ExprKind::BinaryOp {
                                left: Box::new(left_ref),
                                op: BinOp::Eq,
                                right: Box::new(right_ref),
                            },
                        });
                    }
                    if conds.is_empty() {
                        None
                    } else {
                        let mut result = conds.pop().unwrap();
                        while let Some(prev) = conds.pop() {
                            result = TypedExpr {
                                data_type: DataType::Boolean,
                                nullable: false,
                                kind: ExprKind::BinaryOp {
                                    left: Box::new(prev),
                                    op: BinOp::And,
                                    right: Box::new(result),
                                },
                            };
                        }
                        Some(result)
                    }
                }
                Some(ast::JoinConstraint::Natural(span)) => {
                    return Err(AnalyzeError::unsupported_query_shape(
                        "NATURAL JOIN is not yet supported",
                        *span,
                    ));
                }
                Some(ast::JoinConstraint::None) | None => None,
            };

            // SEMI / ANTI joins only expose the surviving side's columns to
            // the outer scope — the other side is consumed by the join itself
            // and is not visible to WHERE/SELECT or downstream joins. The ON
            // condition above was already analyzed against the merged scope.
            // This must match the physical join output scope so that
            // analyzer-emitted projections agree with fragment materialization.
            match join_kind {
                JoinKind::LeftSemi | JoinKind::LeftAnti => {
                    // outer scope = left scope unchanged. USING-clause
                    // reordering still applies: even though right columns
                    // are not exposed, the surviving USING columns should
                    // sit at the front of the SELECT * column list.
                    if let Some(ast::JoinConstraint::Using {
                        columns: using_cols_ast,
                        ..
                    }) = constraint
                    {
                        let using_names: Vec<String> =
                            using_cols_ast.iter().map(|c| c.value.clone()).collect();
                        current_scope.apply_using_layout(&using_names, false);
                    }
                }
                JoinKind::RightSemi | JoinKind::RightAnti => {
                    current_scope = right_scope;
                    if let Some(ast::JoinConstraint::Using {
                        columns: using_cols_ast,
                        ..
                    }) = constraint
                    {
                        let using_names: Vec<String> =
                            using_cols_ast.iter().map(|c| c.value.clone()).collect();
                        current_scope.apply_using_layout(&using_names, false);
                    }
                }
                _ => {
                    // For FULL OUTER USING, the joined column is the merge
                    // of both sides (`COALESCE(left.col, right.col)`).
                    // Capture the per-side qualifiers before
                    // `apply_using_layout` deduplicates `ordered`.
                    let coalesce_quals: Option<Vec<(String, String, String)>> =
                        if matches!(join_kind, JoinKind::FullOuter)
                            && let Some(ast::JoinConstraint::Using {
                                columns: using_cols_ast,
                                ..
                            }) = constraint
                        {
                            let mut out = Vec::new();
                            for c in using_cols_ast {
                                let name = c.value.clone();
                                let name_lower = name.to_lowercase();
                                let left_q = current_scope
                                    .iter_columns()
                                    .find(|(_, n, _, _, _)| n.to_lowercase() == name_lower)
                                    .and_then(|(q, _, _, _, _)| q.clone());
                                let right_q = right_scope
                                    .iter_columns()
                                    .find(|(_, n, _, _, _)| n.to_lowercase() == name_lower)
                                    .and_then(|(q, _, _, _, _)| q.clone());
                                match (left_q, right_q) {
                                    (Some(l), Some(r)) => out.push((name, l, r)),
                                    _ => {
                                        return Err(AnalyzeError::invalid_query_shape(
                                            format!(
                                                "USING column `{name}` must exist on both sides"
                                            ),
                                            join.span,
                                        ));
                                    }
                                }
                            }
                            Some(out)
                        } else {
                            None
                        };

                    // An outer join pads the side that found no partner, so
                    // every column that side exposes can arrive as NULL even
                    // when the table declares it NOT NULL. Widen the padded
                    // side's output scope before merging; the ON condition was
                    // analyzed above against the unwidened scopes, which is
                    // where the source's own nullability is still the truth.
                    match join_kind {
                        JoinKind::LeftOuter => right_scope.mark_all_nullable(),
                        JoinKind::RightOuter => current_scope.mark_all_nullable(),
                        JoinKind::FullOuter => {
                            current_scope.mark_all_nullable();
                            right_scope.mark_all_nullable();
                        }
                        _ => {}
                    }
                    current_scope.merge(&right_scope);
                    // USING-clause column hiding: each USING column appears
                    // once in SELECT * and at the head of the column list.
                    // For RIGHT joins the preserved side is right, so the
                    // surviving column resolves to the right binding. For
                    // FULL OUTER, both sides can be NULL-padded so we
                    // additionally register a `COALESCE(left.col,
                    // right.col)` computed column so that unqualified
                    // references and `SELECT *` see the merged value.
                    if let Some(ast::JoinConstraint::Using {
                        columns: using_cols_ast,
                        ..
                    }) = constraint
                    {
                        let using_names: Vec<String> =
                            using_cols_ast.iter().map(|c| c.value.clone()).collect();
                        let prefer_right = matches!(join_kind, JoinKind::RightOuter);
                        current_scope.apply_using_layout(&using_names, prefer_right);
                        if let Some(quals) = coalesce_quals {
                            for (col, l_q, r_q) in &quals {
                                current_scope
                                    .register_full_outer_using_coalesce(
                                        self.function_catalog,
                                        std::slice::from_ref(col),
                                        l_q,
                                        r_q,
                                    )
                                    .map_err(|message| {
                                        AnalyzeError::type_mismatch(message, join.span)
                                    })?;
                            }
                        } else if matches!(join_kind, JoinKind::RightOuter) {
                            // RIGHT JOIN USING after a previous FULL OUTER
                            // USING: the right side now owns the merged
                            // column, so the prior left-side COALESCE
                            // chain no longer reflects reality. Drop the
                            // computed_column so unqualified resolution
                            // falls back to the right-side binding.
                            // LEFT / INNER joins keep the COALESCE
                            // unchanged — they preserve the left side or
                            // require equality, so the chained value is
                            // still correct.
                            for c in using_cols_ast {
                                current_scope.clear_computed_column(&c.value);
                            }
                        }
                    }
                }
            }
            current_rel = Relation::Join(Box::new(JoinRelation {
                left: current_rel,
                right: right_rel,
                join_type: join_kind,
                condition,
            }));
        }

        Ok((current_rel, current_scope))
    }

    fn analyze_table_factor_with_outer(
        &self,
        factor: &ast::TableFactor,
        outer_scope: Option<&AnalyzerScope>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        match factor {
            ast::TableFactor::Table {
                name,
                metadata,
                alias,
                ..
            } => {
                let parts: Vec<String> = name.parts.iter().map(|part| part.value.clone()).collect();

                // `$<metatype>` is a parser-owned relation field. Capability
                // admission stays in analysis, after catalog resolution.
                if let Some(metadata) = metadata {
                    let metadata_ty =
                        crate::planner::table::SqlMetadataTableKind::parse(&metadata.value)
                            .map_err(|_| {
                                AnalyzeError::unsupported_query_shape(
                                    format!(
                                        "unsupported iceberg metadata table type: {}",
                                        metadata.value
                                    ),
                                    metadata.span,
                                )
                            })?;
                    // Reject branch/tag combo: `t.branch_dev$snapshots` is meaningless.
                    if let Some(last) = parts.last()
                        && (last.starts_with("branch_") || last.starts_with("tag_"))
                    {
                        return Err(AnalyzeError::invalid_query_shape(
                            format!(
                                "iceberg metadata table cannot be combined with branch/tag suffix: {parts:?}"
                            ),
                            name.span,
                        ));
                    }

                    let (catalog_override, db_lower, tbl_lower) = match parts.as_slice() {
                        [tbl] => (
                            None,
                            self.current_database.to_lowercase(),
                            tbl.to_lowercase(),
                        ),
                        [db, tbl] => (None, db.to_lowercase(), tbl.to_lowercase()),
                        [cat, db, tbl] => (
                            Some(cat.to_lowercase()),
                            db.to_lowercase(),
                            tbl.to_lowercase(),
                        ),
                        _ => {
                            return Err(AnalyzeError::invalid_query_shape(
                                format!(
                                    "iceberg metadata table requires <tbl> | <db>.<tbl> | <cat>.<db>.<tbl>, got: {parts:?}"
                                ),
                                name.span,
                            ));
                        }
                    };

                    let metadata_provider =
                        self.catalog.iceberg_metadata_provider().ok_or_else(|| {
                            AnalyzeError::unsupported_query_shape(
                                "iceberg metadata table lookup is not supported by this catalog",
                                name.span,
                            )
                        })?;
                    let table_def = metadata_provider
                        .get_iceberg_metadata_table(
                            catalog_override.as_deref(),
                            &db_lower,
                            &tbl_lower,
                            metadata_ty,
                        )
                        .map_err(|error| AnalyzeError::unknown_table(error, name.span))?
                        .planner;
                    let alias_name = alias.as_ref().map(|a| a.name.value.clone());

                    // Metadata aliases are materialized by the application
                    // catalog boundary. Their complete schema is already a
                    // SQL table fact here; analysis must not reopen a
                    // provider source to rebuild it.
                    let cols = &table_def.columns;
                    let mut scope = self.new_scope();
                    let qualifier = alias_name.as_deref().unwrap_or(&table_def.name);
                    // Collect analyzer-allocated ColumnIds so the planner can reuse
                    // them on the scan's output_columns (keeping ColumnRef ids in the
                    // rest of the plan consistent with the scan's output — same pattern
                    // as `Relation::Scan`).
                    let column_ids: Vec<crate::column_id::ColumnId> = cols
                        .iter()
                        .map(|col| scope.add_table_column(Some(qualifier), col))
                        .collect();

                    let relation = Relation::IcebergMetadataScan(IcebergMetadataScanRelation {
                        database: db_lower,
                        table: table_def,
                        metadata_table_type: metadata_ty,
                        alias: alias_name,
                        column_ids,
                    });
                    return Ok((relation, scope));
                }

                let (catalog_override, db, tbl) = match parts.len() {
                    1 => (None, self.current_database.to_string(), parts[0].clone()),
                    2 => (None, parts[0].clone(), parts[1].clone()),
                    3 => (
                        Some(parts[0].to_lowercase()),
                        parts[1].clone(),
                        parts[2].clone(),
                    ),
                    _ => {
                        return Err(AnalyzeError::invalid_query_shape(
                            format!("unsupported table name: {name:?}"),
                            name.span,
                        ));
                    }
                };
                let db_lower = db.to_lowercase();
                let tbl_lower = tbl.to_lowercase();

                if parts.len() == 1 {
                    if self.pending_ctes.contains(&tbl_lower) {
                        return Err(AnalyzeError::invalid_query_shape(
                            format!("forward CTE reference is not supported: {tbl_lower}"),
                            name.span,
                        ));
                    }

                    if let Some(&cte_id) = self.ctes.get(&tbl_lower) {
                        let (producer_columns, entry_id) = {
                            let registry = self.cte_registry.borrow();
                            let entry = registry.get(cte_id).ok_or_else(|| {
                                AnalyzeError::internal(format!("unknown CTE id: {cte_id}"))
                            })?;
                            (entry.output_columns.clone(), entry.id)
                        };
                        let alias_name = alias
                            .as_ref()
                            .map(|a| a.name.value.clone())
                            .unwrap_or_else(|| tbl.clone());
                        // Each CTE consume must mint fresh ColumnIds. The
                        // producer's ColumnIds are owned by the body of the
                        // WITH definition; if multiple consumes shared them,
                        // downstream operators could not tell aliases apart
                        // (e.g. `cte a, cte b WHERE a.x=1 AND b.x=2`).
                        let producer_column_ids: Vec<ColumnId> =
                            producer_columns.iter().map(|col| col.column_id).collect();
                        let output_columns: Vec<OutputColumn> = producer_columns
                            .into_iter()
                            .map(|col| {
                                let logical_type =
                                    self.factory.borrow().logical_type(col.column_id);
                                let json_list = self
                                    .factory
                                    .borrow()
                                    .has_json_list_provenance(col.column_id);
                                let new_id = self.alloc_column_id(
                                    Some(alias_name.clone()),
                                    col.name.clone(),
                                    col.data_type.clone(),
                                    col.nullable,
                                );
                                self.factory
                                    .borrow_mut()
                                    .set_logical_type(new_id, logical_type);
                                self.factory
                                    .borrow_mut()
                                    .set_json_list_provenance(new_id, json_list);
                                OutputColumn {
                                    column_id: new_id,
                                    name: col.name,
                                    data_type: col.data_type,
                                    nullable: col.nullable,
                                    is_internal: false,
                                }
                            })
                            .collect();
                        let mut scope = self.new_scope();
                        for col in &output_columns {
                            scope.add_column_with_id(
                                Some(&alias_name),
                                &col.name,
                                col.column_id,
                                col.data_type.clone(),
                                col.nullable,
                            );
                        }
                        return Ok((
                            Relation::CTEConsume {
                                cte_id: entry_id,
                                alias: alias_name,
                                output_columns,
                                producer_column_ids,
                            },
                            scope,
                        ));
                    }
                }

                let resolved_table = self
                    .catalog
                    .resolve_table_for_analysis(catalog_override.as_deref(), &db_lower, &tbl_lower)
                    .map_err(|error| AnalyzeError::unknown_table(error, name.span))?;
                let table_def = resolved_table.planner;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());

                // Build scope
                let mut scope = self.new_scope();
                let qualifier = alias_name.as_deref().unwrap_or(&table_def.name);
                let mut column_ids =
                    scope.add_table(Some(qualifier), &resolved_table.catalog.columns);
                // If alias differs from table name, also register with table name
                if let Some(ref a) = alias_name
                    && !a.eq_ignore_ascii_case(&table_def.name)
                {
                    scope
                        .add_table_qualified_only(&table_def.name, &resolved_table.catalog.columns);
                }
                // Register Iceberg V3 row-lineage pseudo-columns (_row_id,
                // _last_updated_sequence_number) when the table carries them.
                // These are hidden from SELECT * but resolvable by explicit name.
                if !resolved_table.catalog.hidden_columns.is_empty() {
                    let meta_ids = scope.add_iceberg_metadata_columns(
                        qualifier,
                        &resolved_table.catalog.hidden_columns,
                    );
                    column_ids.extend(meta_ids);
                }

                let relation = Relation::Scan(ScanRelation {
                    database: db_lower,
                    table: table_def,
                    alias: alias_name,
                    column_ids,
                });

                Ok((relation, scope))
            }
            ast::TableFactor::Derived {
                subquery,
                alias,
                span,
                ..
            } => {
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .ok_or_else(|| {
                        AnalyzeError::invalid_query_shape(
                            "subquery in FROM requires an alias",
                            *span,
                        )
                    })?;

                let resolved_query = self.analyze_query(subquery)?;
                let output_columns =
                    derived_table_output_columns(&resolved_query.output_columns, alias.as_ref())?;

                // Build scope from subquery output columns.
                // Reuse the inner query's ColumnId so that distribution /
                // equivalence specs remain valid across the alias boundary.
                let mut scope = self.new_scope();
                for col in &output_columns {
                    scope.add_column_with_id(
                        Some(&alias_name),
                        &col.name,
                        col.column_id,
                        col.data_type.clone(),
                        col.nullable,
                    );
                }

                let relation = Relation::Subquery {
                    query: Box::new(resolved_query),
                    alias: alias_name,
                    output_columns,
                };

                Ok((relation, scope))
            }
            ast::TableFactor::TableFunction { expr, alias, .. } => {
                self.analyze_table_function(expr, alias.as_ref())
            }
            ast::TableFactor::Unnest {
                alias,
                array_exprs,
                with_offset,
                span,
                ..
            } => self.analyze_unnest(
                array_exprs,
                alias.as_ref(),
                *with_offset,
                *span,
                outer_scope,
            ),
            ast::TableFactor::NestedJoin {
                table_with_joins,
                alias,
                span,
                ..
            } => {
                if alias.is_some() {
                    return Err(AnalyzeError::unsupported_query_shape(
                        "alias on parenthesized JOIN is not yet supported",
                        *span,
                    ));
                }
                self.analyze_from(table_with_joins)
            }
        }
    }

    fn analyze_unnest(
        &self,
        array_exprs: &[ast::Expr],
        alias: Option<&ast::TableAlias>,
        with_offset: bool,
        span: novarocks_parser::Span,
        outer_scope: Option<&AnalyzerScope>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        if with_offset {
            return Err(AnalyzeError::unsupported_query_shape(
                "UNNEST WITH OFFSET/ORDINALITY is not yet supported",
                span,
            ));
        }
        if array_exprs.is_empty() {
            return Err(AnalyzeError::invalid_query_shape(
                "UNNEST requires at least one ARRAY expression",
                span,
            ));
        }
        let Some(outer_scope) = outer_scope else {
            return Err(AnalyzeError::unsupported_query_shape(
                "UNNEST is currently supported only in LATERAL JOIN",
                span,
            ));
        };

        let alias_columns = alias
            .map(|a| {
                a.columns
                    .iter()
                    .map(|c| c.value.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !alias_columns.is_empty() && alias_columns.len() != array_exprs.len() {
            return Err(AnalyzeError::invalid_query_shape(
                format!(
                    "UNNEST alias has {} columns but produces {} columns",
                    alias_columns.len(),
                    array_exprs.len()
                ),
                alias.map(|alias| alias.span).unwrap_or(span),
            ));
        }

        let alias_name = alias.map(|a| a.name.value.clone());
        let qualifier = alias_name.as_deref().unwrap_or("unnest");
        let mut args = Vec::with_capacity(array_exprs.len());
        let mut output_columns = Vec::with_capacity(array_exprs.len());
        let mut scope = self.new_scope();

        for (idx, expr) in array_exprs.iter().enumerate() {
            let typed = self.analyze_expr(expr, outer_scope)?;
            let DataType::List(item_field) = &typed.data_type else {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "UNNEST argument {} must be ARRAY, got {:?}",
                        idx + 1,
                        typed.data_type
                    ),
                    expr.span(),
                ));
            };
            let col_name = alias_columns.get(idx).cloned().unwrap_or_else(|| {
                if array_exprs.len() == 1 {
                    "unnest".to_string()
                } else {
                    format!("unnest_{}", idx + 1)
                }
            });
            let data_type = item_field.data_type().clone();
            let nullable = true;
            let column_id =
                scope.add_column(Some(qualifier), &col_name, data_type.clone(), nullable);
            output_columns.push(OutputColumn {
                column_id,
                name: col_name,
                data_type,
                nullable,
                is_internal: false,
            });
            args.push(typed);
        }

        let function_arguments = args
            .iter()
            .map(crate::analysis::function_argument)
            .collect::<Vec<_>>();
        let binding = self
            .function_catalog
            .resolve_table_binding("unnest", &function_arguments)
            .map_err(|error| {
                AnalyzeError::invalid_argument(format!("failed to bind UNNEST: {error}"), span)
            })?;
        let novarocks_functions::FunctionResultType::Relation(result_columns) =
            &binding.selected.result_type
        else {
            return Err(AnalyzeError::invalid_argument(
                "UNNEST binding must produce a relation",
                span,
            ));
        };
        if result_columns.len() != output_columns.len()
            || result_columns
                .iter()
                .zip(&output_columns)
                .any(|(bound, output)| {
                    bound.data_type != output.data_type || bound.nullable != output.nullable
                })
        {
            return Err(AnalyzeError::invalid_argument(
                "UNNEST binding result differs from analyzed output columns",
                span,
            ));
        }

        Ok((
            Relation::Unnest(UnnestRelation {
                args,
                binding: binding.into(),
                output_columns,
                alias: alias_name,
            }),
            scope,
        ))
    }

    /// Analyze a TABLE(...) table function reference.
    fn analyze_table_function(
        &self,
        expr: &ast::Expr,
        alias: Option<&ast::TableAlias>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        let ast::Expr::FunctionCall(function) = expr else {
            return Err(AnalyzeError::invalid_query_shape(
                format!("TABLE() requires a function call, got: {expr:?}"),
                expr.span(),
            ));
        };
        let func_name = function
            .name
            .parts
            .last()
            .map(|part| part.value.to_ascii_lowercase())
            .unwrap_or_default();
        if func_name == "__nr_ivm_delta" {
            // `TABLE(__nr_ivm_delta(...))` (explicit TABLE wrapper) also
            // routes here in addition to the bare-call table-function syntax.
            return self.analyze_iceberg_delta_table_function(function, alias);
        }
        if func_name != "generate_series" {
            return Err(AnalyzeError::unsupported_query_shape(
                format!("unsupported table function: {func_name}"),
                function.name.span,
            ));
        }

        // Detect whether the call uses named args (start=>2, end=>5, ...).
        // The typed parser represents both `=>` and StarRocks's accepted
        // `=` spelling as a binary syntax node; their semantic distinction is
        // local to this function's argument grammar.
        let arguments = &function.arguments;
        // Mixing named and positional is disallowed; StarRocks's FE rejects
        // the first positional token after a named one as `Unexpected input
        // '<token>'`, which the SQL test suite asserts against verbatim.
        let any_named = arguments.iter().any(generate_series_named_arg_is_some);
        let any_positional = arguments
            .iter()
            .any(|arg| !generate_series_named_arg_is_some(arg));
        if any_named && any_positional {
            // Surface the first stray positional token. An
            // `Identifier -> value` expression in a function-arg slot is
            // not a valid named-arg operator in StarRocks; report it with
            // the legacy `No viable statement for input` wording the FE
            // uses. Other stray positional tokens get the canonical
            // `Unexpected input '<token>'` form.
            if let Some(arg) = arguments
                .iter()
                .find(|arg| !generate_series_named_arg_is_some(arg))
            {
                if matches!(arg, ast::Expr::Lambda(_)) {
                    return Err(AnalyzeError::invalid_argument(
                        "No viable statement for input",
                        arg.span(),
                    ));
                }
                return Err(AnalyzeError::invalid_argument(
                    format!("Unexpected input '{}'.", print_expr(arg)),
                    arg.span(),
                ));
            }
            return Err(AnalyzeError::invalid_argument(
                "Unknown table function: generate_series",
                function.span,
            ));
        }

        let (start, end, step) = if any_named {
            let mut start_v: Option<Option<i64>> = None;
            let mut end_v: Option<Option<i64>> = None;
            let mut step_v: Option<Option<i64>> = None;
            for arg in arguments {
                let Some((name, expr)) = generate_series_named_arg(arg) else {
                    return Err(AnalyzeError::invalid_argument(
                        "Unknown table function: generate_series",
                        arg.span(),
                    ));
                };
                let key = name.value.to_ascii_lowercase();
                let value = if is_null_literal(expr) {
                    None
                } else {
                    Some(eval_const_i64(expr)?)
                };
                let slot = match key.as_str() {
                    "start" => &mut start_v,
                    "end" => &mut end_v,
                    "step" => &mut step_v,
                    _ => {
                        return Err(AnalyzeError::invalid_argument(
                            format!("Unknown table function: generate_series ({key})"),
                            name.span,
                        ));
                    }
                };
                if slot.is_some() {
                    return Err(AnalyzeError::invalid_argument(
                        "Unknown table function: generate_series",
                        name.span,
                    ));
                }
                *slot = Some(value);
            }
            let start = start_v.ok_or_else(|| {
                AnalyzeError::invalid_argument(
                    "Unknown table function: generate_series",
                    function.span,
                )
            })?;
            let end = end_v.ok_or_else(|| {
                AnalyzeError::invalid_argument(
                    "Unknown table function: generate_series",
                    function.span,
                )
            })?;
            // Named args do not allow NULL values for any parameter.
            if start.is_none() || end.is_none() || matches!(step_v, Some(None)) {
                return Err(AnalyzeError::invalid_argument(
                    "table function not support null parameter",
                    function.span,
                ));
            }
            let step = step_v.flatten().unwrap_or(1);
            if step == 0 {
                return Err(AnalyzeError::invalid_argument(
                    "generate_series step must not be zero",
                    function.span,
                ));
            }
            (start.unwrap(), end.unwrap(), step)
        } else {
            let values: Vec<i64> = arguments
                .iter()
                .map(eval_const_i64)
                .collect::<Result<_, _>>()?;
            match values.as_slice() {
                [s, e] => (*s, *e, 1i64),
                [s, e, st] => {
                    if *st == 0 {
                        return Err(AnalyzeError::invalid_argument(
                            "generate_series step must not be zero",
                            function.span,
                        ));
                    }
                    (*s, *e, *st)
                }
                _ => {
                    return Err(AnalyzeError::invalid_argument(
                        "Unknown table function: generate_series",
                        function.span,
                    ));
                }
            }
        };

        // Determine output column name from alias or default
        let column_name = alias
            .and_then(|a| a.columns.first().map(|c| c.value.clone()))
            .unwrap_or_else(|| "generate_series".to_string());
        let alias_name = alias.map(|a| a.name.value.clone());
        let qualifier = alias_name.as_deref().unwrap_or("generate_series");

        let mut scope = self.new_scope();
        let output_column_id =
            scope.add_column(Some(qualifier), &column_name, DataType::Int64, false);

        let relation = Relation::GenerateSeries(GenerateSeriesRelation {
            start,
            end,
            step,
            column_name,
            alias: alias_name,
            output_column_id,
        });
        Ok((relation, scope))
    }

    /// Analyze the IVM-A1 internal table function
    /// `__nr_ivm_delta('cat.ns.tbl', from_snapshot_id, to_snapshot_id)`.
    /// This function is produced by the IVM refresh driver (and exercised
    /// directly in tests) to scan the snapshot-range delta of an Iceberg
    /// base table.
    fn analyze_iceberg_delta_table_function(
        &self,
        function: &ast::FunctionCall,
        alias: Option<&ast::TableAlias>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        if function.arguments.len() != 3 {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "__nr_ivm_delta requires 3 positional arguments \
                     (catalog.namespace.table, from_snapshot_id, to_snapshot_id), got {}",
                    function.arguments.len()
                ),
                function.span,
            ));
        }

        // Argument 0: three-part identifier as a string literal.
        let three_part = &function.arguments[0];
        let three_part = match three_part {
            ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::String(s),
                ..
            }) => s.clone(),
            _ => {
                return Err(AnalyzeError::invalid_literal(
                    format!(
                        "__nr_ivm_delta argument 0 must be a string literal \
                         (catalog.namespace.table), got {three_part:?}"
                    ),
                    three_part.span(),
                ));
            }
        };
        let parts: Vec<&str> = three_part.split('.').collect();
        if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "__nr_ivm_delta argument 0 must be a three-part identifier \
                     'catalog.namespace.table', got '{three_part}'"
                ),
                function.arguments[0].span(),
            ));
        }
        let catalog = parts[0].to_string();
        let namespace = parts[1].to_string();
        let table_name = parts[2].to_string();

        // Argument 1 / 2: from_snapshot_id, to_snapshot_id (non-negative i64).
        let from_expr = &function.arguments[1];
        let to_expr = &function.arguments[2];
        let from_snapshot_id = eval_const_i64(from_expr).map_err(|error| {
            AnalyzeError::invalid_literal(
                format!("__nr_ivm_delta from_snapshot_id: {error}"),
                from_expr.span(),
            )
        })?;
        let to_snapshot_id = eval_const_i64(to_expr).map_err(|error| {
            AnalyzeError::invalid_literal(
                format!("__nr_ivm_delta to_snapshot_id: {error}"),
                to_expr.span(),
            )
        })?;
        if from_snapshot_id < 0 || to_snapshot_id < 0 {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "__nr_ivm_delta requires non-negative snapshot ids; \
                     got from={from_snapshot_id}, to={to_snapshot_id}"
                ),
                function.span,
            ));
        }

        // Look up the base table. `__nr_ivm_delta` requires that the table
        // exposes Iceberg v3 row-lineage metadata columns — without them
        // we cannot recover row identity across snapshots.
        let resolved_table = self
            .catalog
            .resolve_table_for_analysis(Some(&catalog), &namespace, &table_name)
            .map_err(|error| AnalyzeError::unknown_table(error, function.arguments[0].span()))?;
        let table_def = resolved_table.planner;
        if resolved_table.catalog.hidden_columns.is_empty() {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "__nr_ivm_delta requires base table '{three_part}' to expose Iceberg v3 \
                     row-lineage metadata columns; rebuild the table with \
                     `write.row-lineage = true` (Iceberg v3)"
                ),
                function.arguments[0].span(),
            ));
        }

        // Output schema = base table columns + row-lineage metadata columns.
        // Both are exposed as resolvable columns under the alias / table name.
        let alias_name = alias.map(|a| a.name.value.clone());
        let mut scope = self.new_scope();
        let qualifier = alias_name.as_deref().unwrap_or(&table_def.name);
        // Collect analyzer-allocated ColumnIds so the planner can reuse them on
        // the scan's output_columns, keeping ColumnRef ids consistent throughout
        // the plan (same pattern as `Relation::Scan`).
        let mut column_ids = scope.add_table(Some(qualifier), &resolved_table.catalog.columns);
        let meta_ids =
            scope.add_iceberg_metadata_columns(qualifier, &resolved_table.catalog.hidden_columns);
        column_ids.extend(meta_ids);

        let relation = Relation::IcebergDeltaScan(IcebergDeltaScanRelation {
            catalog,
            namespace,
            table_name,
            table: table_def,
            from_snapshot_id,
            to_snapshot_id,
            alias: alias_name,
            column_ids,
        });
        Ok((relation, scope))
    }
}

fn parse_join_operator(
    operator: ast::JoinOperator,
    constraint: &ast::JoinConstraint,
) -> Result<(JoinKind, Option<&ast::JoinConstraint>), AnalyzeError> {
    use ast::JoinOperator as Operator;

    let kind = match operator {
        Operator::Inner | Operator::InnerExplicit => JoinKind::Inner,
        Operator::LeftOuter | Operator::LeftOuterExplicit => JoinKind::LeftOuter,
        Operator::RightOuter | Operator::RightOuterExplicit => JoinKind::RightOuter,
        Operator::FullOuter | Operator::FullOuterExplicit => JoinKind::FullOuter,
        Operator::Cross => return Ok((JoinKind::Cross, None)),
        Operator::LeftSemi => JoinKind::LeftSemi,
        Operator::RightSemi => JoinKind::RightSemi,
        Operator::LeftAnti => JoinKind::LeftAnti,
        Operator::RightAnti => JoinKind::RightAnti,
    };
    Ok((kind, Some(constraint)))
}

fn generate_series_named_arg_is_some(expr: &ast::Expr) -> bool {
    generate_series_named_arg(expr).is_some()
}

fn generate_series_named_arg(expr: &ast::Expr) -> Option<(&ast::Ident, &ast::Expr)> {
    let ast::Expr::Binary(binary) = expr else {
        return None;
    };
    if !matches!(
        binary.operator,
        ast::BinaryOperator::NamedArgument | ast::BinaryOperator::Equal
    ) {
        return None;
    }
    let ast::Expr::Identifier(name) = binary.left.as_ref() else {
        return None;
    };
    Some((name, binary.right.as_ref()))
}

fn eval_const_i64(expr: &ast::Expr) -> Result<i64, AnalyzeError> {
    match expr {
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(number),
            ..
        }) => number.parse::<i64>().map_err(|error| {
            AnalyzeError::invalid_literal(
                format!("cannot parse integer literal `{number}`: {error}"),
                expr.span(),
            )
        }),
        ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Minus) => {
            Ok(-eval_const_i64(&unary.expression)?)
        }
        ast::Expr::Binary(binary) => {
            let left = eval_const_i64(&binary.left)?;
            let right = eval_const_i64(&binary.right)?;
            match binary.operator {
                ast::BinaryOperator::Add => Ok(left + right),
                ast::BinaryOperator::Subtract => Ok(left - right),
                ast::BinaryOperator::Multiply => Ok(left * right),
                ast::BinaryOperator::Divide if right != 0 => Ok(left / right),
                ast::BinaryOperator::Modulo if right != 0 => Ok(left % right),
                operator => Err(AnalyzeError::invalid_argument(
                    format!("unsupported operator in constant expression: {operator:?}"),
                    expr.span(),
                )),
            }
        }
        ast::Expr::Nested(nested) => eval_const_i64(&nested.expression),
        _ => Err(AnalyzeError::invalid_literal(
            format!("expected constant integer expression, got: {expr:?}"),
            expr.span(),
        )),
    }
}

fn is_null_literal(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        })
    )
}

fn derived_table_output_columns(
    columns: &[OutputColumn],
    alias: Option<&ast::TableAlias>,
) -> Result<Vec<OutputColumn>, AnalyzeError> {
    let Some(alias) = alias else {
        return Ok(columns.to_vec());
    };
    if alias.columns.is_empty() {
        return Ok(columns.to_vec());
    }
    if alias.columns.len() != columns.len() {
        return Err(AnalyzeError::invalid_query_shape(
            format!(
                "derived table alias '{}' has {} column aliases but subquery produces {} columns",
                alias.name.value,
                alias.columns.len(),
                columns.len()
            ),
            alias.span,
        ));
    }
    Ok(columns
        .iter()
        .zip(alias.columns.iter())
        .map(|(col, alias_col)| OutputColumn {
            column_id: col.column_id,
            name: alias_col.value.clone(),
            data_type: col.data_type.clone(),
            nullable: col.nullable,
            is_internal: false,
        })
        .collect())
}
