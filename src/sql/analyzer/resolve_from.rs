use arrow::datatypes::DataType;
use sqlparser::ast as sqlast;

use crate::sql::analysis::*;

use super::helpers::eval_const_i64;
use super::iceberg_metadata::{metadata_table_schema, split_metadata_suffix};
use super::scope::AnalyzerScope;

impl<'a> super::AnalyzerContext<'a> {
    /// Analyze a FROM clause (TableWithJoins).
    pub(super) fn analyze_from(
        &self,
        twj: &sqlast::TableWithJoins,
    ) -> Result<(Relation, AnalyzerScope), String> {
        self.analyze_from_with_outer(twj, None)
    }

    /// Analyze a FROM clause with an optional outer scope visible to the
    /// first relation. Used when comma-separated FROM entries (each parsed as
    /// its own TableWithJoins) need to see earlier sibling scopes so that
    /// table-valued functions like `unnest(...)` can reference outer columns.
    pub(super) fn analyze_from_with_outer(
        &self,
        twj: &sqlast::TableWithJoins,
        outer_scope: Option<&AnalyzerScope>,
    ) -> Result<(Relation, AnalyzerScope), String> {
        let (mut current_rel, mut current_scope) =
            self.analyze_table_factor_with_outer(&twj.relation, outer_scope)?;

        for join in &twj.joins {
            let (right_rel, right_scope) =
                self.analyze_table_factor_with_outer(&join.relation, Some(&current_scope))?;

            let (join_kind, constraint) = super::helpers::parse_join_operator(&join.join_operator)?;

            let condition = match constraint {
                Some(sqlast::JoinConstraint::On(on_expr)) => {
                    // Build a merged scope for analyzing the ON condition
                    let mut merged = AnalyzerScope::new();
                    merged.merge(&current_scope);
                    merged.merge(&right_scope);
                    Some(self.analyze_expr(on_expr, &merged)?)
                }
                Some(sqlast::JoinConstraint::Using(columns)) => {
                    // Convert USING(col1, col2) to ON left.col1 = right.col1 AND ...
                    let mut merged = AnalyzerScope::new();
                    merged.merge(&current_scope);
                    merged.merge(&right_scope);
                    let mut conds = Vec::new();
                    for col_obj in columns {
                        let col_name = col_obj.to_string();
                        let (dt, nullable) = merged
                            .resolve(None, &col_name)
                            .unwrap_or((DataType::Utf8, true));
                        let col_ref = TypedExpr {
                            kind: ExprKind::ColumnRef {
                                qualifier: None,
                                column: col_name,
                            },
                            data_type: dt,
                            nullable,
                        };
                        // The eq is intentionally unqualified on both sides.
                        // The codegen compiles each operand against the
                        // matching child's scope (left operand → left child,
                        // right operand → right child), so the resolution
                        // there produces the cross-side `left.col = right.col`
                        // physical equality, not a tautology.
                        conds.push(TypedExpr {
                            data_type: DataType::Boolean,
                            nullable: false,
                            kind: ExprKind::BinaryOp {
                                left: Box::new(col_ref.clone()),
                                op: BinOp::Eq,
                                right: Box::new(col_ref),
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
                Some(sqlast::JoinConstraint::Natural) => {
                    return Err("NATURAL JOIN is not yet supported".into());
                }
                Some(sqlast::JoinConstraint::None) | None => None,
            };

            // SEMI / ANTI joins only expose the surviving side's columns to
            // the outer scope — the other side is consumed by the join itself
            // and is not visible to WHERE/SELECT or downstream joins. The ON
            // condition above was already analyzed against the merged scope.
            // This must match `fragment_builder::merged_scope` so that
            // analyzer-emitted projections agree with codegen scope.
            match join_kind {
                JoinKind::LeftSemi | JoinKind::LeftAnti => {
                    // outer scope = left scope unchanged. USING-clause
                    // reordering still applies: even though right columns
                    // are not exposed, the surviving USING columns should
                    // sit at the front of the SELECT * column list.
                    if let Some(sqlast::JoinConstraint::Using(using_cols_ast)) = constraint {
                        let using_names: Vec<String> =
                            using_cols_ast.iter().map(|c| c.to_string()).collect();
                        current_scope.apply_using_layout(&using_names, false);
                    }
                }
                JoinKind::RightSemi | JoinKind::RightAnti => {
                    current_scope = right_scope;
                    if let Some(sqlast::JoinConstraint::Using(using_cols_ast)) = constraint {
                        let using_names: Vec<String> =
                            using_cols_ast.iter().map(|c| c.to_string()).collect();
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
                            && let Some(sqlast::JoinConstraint::Using(using_cols_ast)) = constraint
                        {
                            let mut out = Vec::new();
                            for c in using_cols_ast {
                                let name = c.to_string();
                                let name_lower = name.to_lowercase();
                                let left_q = current_scope
                                    .iter_columns()
                                    .find(|(_, n, _, _)| n.to_lowercase() == name_lower)
                                    .and_then(|(q, _, _, _)| q.clone());
                                let right_q = right_scope
                                    .iter_columns()
                                    .find(|(_, n, _, _)| n.to_lowercase() == name_lower)
                                    .and_then(|(q, _, _, _)| q.clone());
                                match (left_q, right_q) {
                                    (Some(l), Some(r)) => out.push((name, l, r)),
                                    _ => return Err(format!(
                                        "USING column `{name}` must exist on both sides"
                                    )),
                                }
                            }
                            Some(out)
                        } else {
                            None
                        };

                    current_scope.merge(&right_scope);
                    // USING-clause column hiding: each USING column appears
                    // once in SELECT * and at the head of the column list.
                    // For RIGHT joins the preserved side is right, so the
                    // surviving column resolves to the right binding. For
                    // FULL OUTER, both sides can be NULL-padded so we
                    // additionally register a `COALESCE(left.col,
                    // right.col)` computed column so that unqualified
                    // references and `SELECT *` see the merged value.
                    if let Some(sqlast::JoinConstraint::Using(using_cols_ast)) = constraint {
                        let using_names: Vec<String> =
                            using_cols_ast.iter().map(|c| c.to_string()).collect();
                        let prefer_right = matches!(join_kind, JoinKind::RightOuter);
                        current_scope.apply_using_layout(&using_names, prefer_right);
                        if let Some(quals) = coalesce_quals {
                            for (col, l_q, r_q) in &quals {
                                current_scope
                                    .register_full_outer_using_coalesce(
                                        &[col.clone()],
                                        l_q,
                                        r_q,
                                    );
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
                                current_scope
                                    .clear_computed_column(&c.to_string());
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
        factor: &sqlast::TableFactor,
        outer_scope: Option<&AnalyzerScope>,
    ) -> Result<(Relation, AnalyzerScope), String> {
        match factor {
            sqlast::TableFactor::Table {
                name, alias, args, ..
            } => {
                let parts: Vec<String> = name
                    .0
                    .iter()
                    .filter_map(|part| match part {
                        sqlast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                        _ => None,
                    })
                    .collect();

                // StarRocks dialect allows `FROM t, unnest(arr_expr) [AS u(cols)]`
                // as an implicit lateral table function. The standard sqlparser
                // dialect we use does not recognize UNNEST as a keyword, so the
                // parser produces a TableFactor::Table with name "unnest" and a
                // populated args list. Detect that here and route to the unnest
                // analyzer using the outer scope from the preceding comma-join.
                if parts.len() == 1
                    && parts[0].eq_ignore_ascii_case("unnest")
                    && let Some(table_function_args) = args
                {
                    let array_exprs = table_function_args
                        .args
                        .iter()
                        .map(|arg| match arg {
                            sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(expr)) => {
                                Ok(expr.clone())
                            }
                            other => Err(format!(
                                "UNNEST expects positional expression args, got {other}"
                            )),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    return self.analyze_unnest(
                        &array_exprs,
                        alias.as_ref(),
                        false,
                        None,
                        false,
                        outer_scope,
                    );
                }

                // Iceberg metadata-table dispatch: parser pre-rewrites
                // `<tbl>$<metatype>` into `<tbl>.__nr_meta_<metatype>__` so we
                // detect the trailing `__nr_meta_*__` segment here, resolve
                // the base table, and emit a typed `IcebergMetadataScan`.
                let (base_parts, metadata_suffix) = split_metadata_suffix(&parts);
                if let Some(metadata_ty) = metadata_suffix {
                    // Reject branch/tag combo: `t.branch_dev$snapshots` is meaningless.
                    if let Some(last) = base_parts.last() {
                        if last.starts_with("branch_") || last.starts_with("tag_") {
                            return Err(format!(
                                "iceberg metadata table cannot be combined with branch/tag suffix: {parts:?}"
                            ));
                        }
                    }

                    let (db_lower, tbl_lower) = match base_parts.as_slice() {
                        [tbl] => (self.current_database.to_lowercase(), tbl.to_lowercase()),
                        [db, tbl] => (db.to_lowercase(), tbl.to_lowercase()),
                        [_cat, db, tbl] => (db.to_lowercase(), tbl.to_lowercase()),
                        _ => {
                            return Err(format!(
                                "iceberg metadata table requires <tbl> | <db>.<tbl> | <cat>.<db>.<tbl>, got: {parts:?}"
                            ));
                        }
                    };

                    let table_def = self.catalog.get_table(&db_lower, &tbl_lower)?;
                    let alias_name = alias.as_ref().map(|a| a.name.value.clone());

                    // Build scope from the fixed metadata schema.
                    let cols = metadata_table_schema(metadata_ty.clone());
                    let mut scope = AnalyzerScope::new();
                    let qualifier = alias_name.as_deref().unwrap_or(&table_def.name);
                    for col in &cols {
                        scope.add_column(
                            Some(qualifier),
                            &col.name,
                            col.data_type.clone(),
                            col.nullable,
                        );
                    }

                    let relation = Relation::IcebergMetadataScan(IcebergMetadataScanRelation {
                        database: db_lower,
                        table: table_def,
                        metadata_table_type: metadata_ty,
                        alias: alias_name,
                    });
                    return Ok((relation, scope));
                }

                let (db, tbl) = match parts.len() {
                    1 => (self.current_database.to_string(), parts[0].clone()),
                    2 => (parts[0].clone(), parts[1].clone()),
                    _ => return Err(format!("unsupported table name: {name}")),
                };
                let db_lower = db.to_lowercase();
                let tbl_lower = tbl.to_lowercase();

                if parts.len() == 1 {
                    if self.pending_ctes.contains(&tbl_lower) {
                        return Err(format!(
                            "forward CTE reference is not supported: {tbl_lower}"
                        ));
                    }

                    if let Some(&cte_id) = self.ctes.get(&tbl_lower) {
                        let registry = self.cte_registry.borrow();
                        let entry = registry
                            .get(cte_id)
                            .ok_or_else(|| format!("unknown CTE id: {cte_id}"))?;
                        let alias_name = alias
                            .as_ref()
                            .map(|a| a.name.value.clone())
                            .unwrap_or_else(|| tbl.clone());
                        let output_columns = entry.output_columns.clone();
                        let mut scope = AnalyzerScope::new();
                        for col in &output_columns {
                            scope.add_column(
                                Some(&alias_name),
                                &col.name,
                                col.data_type.clone(),
                                col.nullable,
                            );
                        }
                        return Ok((
                            Relation::CTEConsume {
                                cte_id: entry.id,
                                alias: alias_name,
                                output_columns,
                            },
                            scope,
                        ));
                    }
                }

                let table_def = self.catalog.get_table(&db_lower, &tbl_lower)?;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());

                // Build scope
                let mut scope = AnalyzerScope::new();
                let qualifier = alias_name.as_deref().unwrap_or(&table_def.name);
                scope.add_table(Some(qualifier), &table_def.columns);
                // If alias differs from table name, also register with table name
                if let Some(ref a) = alias_name
                    && !a.eq_ignore_ascii_case(&table_def.name)
                {
                    scope.add_table_qualified_only(&table_def.name, &table_def.columns);
                }
                // Register Iceberg V3 row-lineage pseudo-columns (_row_id,
                // _last_updated_sequence_number) when the table carries them.
                // These are hidden from SELECT * but resolvable by explicit name.
                if !table_def.iceberg_row_lineage_metadata_columns.is_empty() {
                    scope.add_iceberg_metadata_columns(
                        qualifier,
                        &table_def.iceberg_row_lineage_metadata_columns,
                    );
                }

                let relation = Relation::Scan(ScanRelation {
                    database: db_lower,
                    table: table_def,
                    alias: alias_name,
                });

                Ok((relation, scope))
            }
            sqlast::TableFactor::Derived {
                subquery, alias, ..
            } => {
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .ok_or("subquery in FROM requires an alias")?;

                let resolved_query = self.analyze_query(subquery)?;
                let output_columns =
                    derived_table_output_columns(&resolved_query.output_columns, alias.as_ref())?;

                // Build scope from subquery output columns
                let mut scope = AnalyzerScope::new();
                for col in &output_columns {
                    scope.add_column(
                        Some(&alias_name),
                        &col.name,
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
            sqlast::TableFactor::TableFunction { expr, alias } => {
                self.analyze_table_function(expr, alias.as_ref())
            }
            sqlast::TableFactor::Function {
                lateral,
                name,
                args,
                alias,
            } => {
                let func_name = name
                    .0
                    .last()
                    .map(|p| p.to_string().to_ascii_lowercase())
                    .unwrap_or_default();
                if func_name != "unnest" {
                    return Err(format!("unsupported table function: {func_name}"));
                }
                if !*lateral {
                    return Err("UNNEST is currently supported only in LATERAL JOIN".into());
                }
                let array_exprs = args
                    .iter()
                    .map(|arg| match arg {
                        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(expr)) => {
                            Ok(expr.clone())
                        }
                        other => Err(format!(
                            "UNNEST expects positional expression args, got {other}"
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.analyze_unnest(
                    &array_exprs,
                    alias.as_ref(),
                    false,
                    None,
                    false,
                    outer_scope,
                )
            }
            sqlast::TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset,
                with_offset_alias,
                with_ordinality,
            } => self.analyze_unnest(
                array_exprs,
                alias.as_ref(),
                *with_offset,
                with_offset_alias.as_ref(),
                *with_ordinality,
                outer_scope,
            ),
            sqlast::TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                if alias.is_some() {
                    return Err("alias on parenthesized JOIN is not yet supported".into());
                }
                self.analyze_from(table_with_joins)
            }
            other => Err(format!("unsupported table factor: {other}")),
        }
    }

    fn analyze_unnest(
        &self,
        array_exprs: &[sqlast::Expr],
        alias: Option<&sqlast::TableAlias>,
        with_offset: bool,
        with_offset_alias: Option<&sqlast::Ident>,
        with_ordinality: bool,
        outer_scope: Option<&AnalyzerScope>,
    ) -> Result<(Relation, AnalyzerScope), String> {
        if with_offset || with_offset_alias.is_some() || with_ordinality {
            return Err("UNNEST WITH OFFSET/ORDINALITY is not yet supported".into());
        }
        if array_exprs.is_empty() {
            return Err("UNNEST requires at least one ARRAY expression".into());
        }
        let Some(outer_scope) = outer_scope else {
            return Err("UNNEST is currently supported only in LATERAL JOIN".into());
        };

        let alias_columns = alias
            .map(|a| {
                a.columns
                    .iter()
                    .map(|c| c.name.value.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !alias_columns.is_empty() && alias_columns.len() != array_exprs.len() {
            return Err(format!(
                "UNNEST alias has {} columns but produces {} columns",
                alias_columns.len(),
                array_exprs.len()
            ));
        }

        let alias_name = alias.map(|a| a.name.value.clone());
        let qualifier = alias_name.as_deref().unwrap_or("unnest");
        let mut args = Vec::with_capacity(array_exprs.len());
        let mut output_columns = Vec::with_capacity(array_exprs.len());
        let mut scope = AnalyzerScope::new();

        for (idx, expr) in array_exprs.iter().enumerate() {
            let typed = self.analyze_expr(expr, outer_scope)?;
            let DataType::List(item_field) = &typed.data_type else {
                return Err(format!(
                    "UNNEST argument {} must be ARRAY, got {:?}",
                    idx + 1,
                    typed.data_type
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
            scope.add_column(Some(qualifier), &col_name, data_type.clone(), nullable);
            output_columns.push(OutputColumn {
                name: col_name,
                data_type,
                nullable,
            });
            args.push(typed);
        }

        Ok((
            Relation::Unnest(UnnestRelation {
                args,
                output_columns,
                alias: alias_name,
            }),
            scope,
        ))
    }

    /// Analyze a TABLE(...) table function reference.
    fn analyze_table_function(
        &self,
        expr: &sqlast::Expr,
        alias: Option<&sqlast::TableAlias>,
    ) -> Result<(Relation, AnalyzerScope), String> {
        let sqlast::Expr::Function(function) = expr else {
            return Err(format!("TABLE() requires a function call, got: {expr}"));
        };
        let func_name = function
            .name
            .0
            .last()
            .map(|p| p.to_string().to_ascii_lowercase())
            .unwrap_or_default();
        if func_name != "generate_series" {
            return Err(format!("unsupported table function: {func_name}"));
        }

        let sqlast::FunctionArguments::List(ref arg_list) = function.args else {
            return Err("generate_series requires parenthesized arguments".into());
        };

        // StarRocks allows `name = value` for named function arguments. Our
        // dialect parses `=` as a binary comparison instead, so reinterpret a
        // positional `Identifier = expr` here as a named argument before the
        // mixed-mode check.
        let normalized_args: Vec<sqlast::FunctionArg> = arg_list
            .args
            .iter()
            .map(|arg| {
                if let sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(
                    sqlast::Expr::BinaryOp { left, op, right },
                )) = arg
                    && matches!(op, sqlast::BinaryOperator::Eq)
                    && let sqlast::Expr::Identifier(ident) = left.as_ref()
                {
                    return sqlast::FunctionArg::Named {
                        name: ident.clone(),
                        arg: sqlast::FunctionArgExpr::Expr(right.as_ref().clone()),
                        operator: sqlast::FunctionArgOperator::Equals,
                    };
                }
                arg.clone()
            })
            .collect();

        // Detect whether the call uses named args (start=>2, end=>5, ...).
        // Mixing named and positional is disallowed; StarRocks's FE rejects
        // the first positional token after a named one as `Unexpected input
        // '<token>'`, which the SQL test suite asserts against verbatim.
        let any_named = normalized_args
            .iter()
            .any(|a| matches!(a, sqlast::FunctionArg::Named { .. }));
        let any_positional = normalized_args
            .iter()
            .any(|a| matches!(a, sqlast::FunctionArg::Unnamed(_)));
        if any_named && any_positional {
            // Surface the first stray positional token in the canonical
            // `Unexpected input '<token>'` form.
            if let Some(sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e))) =
                normalized_args
                    .iter()
                    .find(|a| matches!(a, sqlast::FunctionArg::Unnamed(_)))
            {
                return Err(format!("Unexpected input '{e}'."));
            }
            return Err("Unknown table function: generate_series".into());
        }

        let (start, end, step) = if any_named {
            let mut start_v: Option<Option<i64>> = None;
            let mut end_v: Option<Option<i64>> = None;
            let mut step_v: Option<Option<i64>> = None;
            for arg in &normalized_args {
                let sqlast::FunctionArg::Named {
                    name,
                    arg: arg_expr,
                    operator: _,
                } = arg
                else {
                    return Err("Unknown table function: generate_series".into());
                };
                let key = name.value.to_ascii_lowercase();
                let expr = match arg_expr {
                    sqlast::FunctionArgExpr::Expr(e) => e,
                    _ => return Err("Unknown table function: generate_series".into()),
                };
                let value = if is_null_literal(expr) {
                    None
                } else {
                    Some(eval_const_i64(expr)?)
                };
                let slot = match key.as_str() {
                    "start" => &mut start_v,
                    "end" => &mut end_v,
                    "step" => &mut step_v,
                    _ => return Err(format!("Unknown table function: generate_series ({key})")),
                };
                if slot.is_some() {
                    return Err("Unknown table function: generate_series".into());
                }
                *slot = Some(value);
            }
            let start =
                start_v.ok_or_else(|| "Unknown table function: generate_series".to_string())?;
            let end = end_v.ok_or_else(|| "Unknown table function: generate_series".to_string())?;
            // Named args do not allow NULL values for any parameter.
            if start.is_none() || end.is_none() || matches!(step_v, Some(None)) {
                return Err("table function not support null parameter".into());
            }
            let step = step_v.flatten().unwrap_or(1);
            if step == 0 {
                return Err("generate_series step must not be zero".into());
            }
            (start.unwrap(), end.unwrap(), step)
        } else {
            let values: Vec<i64> = normalized_args
                .iter()
                .map(|arg| match arg {
                    sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) => {
                        eval_const_i64(e)
                    }
                    other => Err(format!(
                        "generate_series expects positional args, got {other}"
                    )),
                })
                .collect::<Result<_, _>>()?;
            match values.as_slice() {
                [s, e] => (*s, *e, 1i64),
                [s, e, st] => {
                    if *st == 0 {
                        return Err("generate_series step must not be zero".into());
                    }
                    (*s, *e, *st)
                }
                _ => return Err("Unknown table function: generate_series".into()),
            }
        };

        // Determine output column name from alias or default
        let column_name = alias
            .and_then(|a| a.columns.first().map(|c| c.name.value.clone()))
            .unwrap_or_else(|| "generate_series".to_string());
        let alias_name = alias.map(|a| a.name.value.clone());
        let qualifier = alias_name.as_deref().unwrap_or("generate_series");

        let mut scope = AnalyzerScope::new();
        scope.add_column(Some(qualifier), &column_name, DataType::Int64, false);

        let relation = Relation::GenerateSeries(GenerateSeriesRelation {
            start,
            end,
            step,
            column_name,
            alias: alias_name,
        });
        Ok((relation, scope))
    }
}

fn is_null_literal(expr: &sqlast::Expr) -> bool {
    matches!(
        expr,
        sqlast::Expr::Value(v) if matches!(v.value, sqlast::Value::Null)
    )
}

fn derived_table_output_columns(
    columns: &[OutputColumn],
    alias: Option<&sqlast::TableAlias>,
) -> Result<Vec<OutputColumn>, String> {
    let Some(alias) = alias else {
        return Ok(columns.to_vec());
    };
    if alias.columns.is_empty() {
        return Ok(columns.to_vec());
    }
    if alias.columns.len() != columns.len() {
        return Err(format!(
            "derived table alias '{}' has {} column aliases but subquery produces {} columns",
            alias.name.value,
            alias.columns.len(),
            columns.len()
        ));
    }
    Ok(columns
        .iter()
        .zip(alias.columns.iter())
        .map(|(col, alias_col)| OutputColumn {
            name: alias_col.name.value.clone(),
            data_type: col.data_type.clone(),
            nullable: col.nullable,
        })
        .collect())
}
