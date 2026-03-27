use arrow::datatypes::DataType;
use sqlparser::ast as sqlast;

use crate::sql::ir::*;

use super::helpers::eval_const_i64;
use super::scope::AnalyzerScope;

impl<'a> super::AnalyzerContext<'a> {
    /// Analyze a FROM clause (TableWithJoins).
    pub(super) fn analyze_from(
        &self,
        twj: &sqlast::TableWithJoins,
    ) -> Result<(Relation, AnalyzerScope), String> {
        let (mut current_rel, mut current_scope) = self.analyze_table_factor(&twj.relation)?;

        for join in &twj.joins {
            let (right_rel, right_scope) = self.analyze_table_factor(&join.relation)?;

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
                        // USING produces a single equality on the same unqualified column
                        // Both sides resolve through the merged scope
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

            current_scope.merge(&right_scope);
            current_rel = Relation::Join(Box::new(JoinRelation {
                left: current_rel,
                right: right_rel,
                join_type: join_kind,
                condition,
            }));
        }

        Ok((current_rel, current_scope))
    }

    /// Analyze a single table factor (table reference, subquery, etc.).
    pub(super) fn analyze_table_factor(
        &self,
        factor: &sqlast::TableFactor,
    ) -> Result<(Relation, AnalyzerScope), String> {
        match factor {
            sqlast::TableFactor::Table { name, alias, .. } => {
                let parts: Vec<String> = name
                    .0
                    .iter()
                    .filter_map(|part| match part {
                        sqlast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                        _ => None,
                    })
                    .collect();
                let (db, tbl) = match parts.len() {
                    1 => (self.current_database.to_string(), parts[0].clone()),
                    2 => (parts[0].clone(), parts[1].clone()),
                    _ => return Err(format!("unsupported table name: {name}")),
                };
                let db_lower = db.to_lowercase();
                let tbl_lower = tbl.to_lowercase();

                // Check if this table name refers to a CTE first.
                if parts.len() == 1 {
                    if let Some((cte_query, cte_col_aliases)) = self.ctes.get(&tbl_lower) {
                        // Inline-expand the CTE as a subquery.
                        // Remove self from CTE map to prevent infinite recursion
                        // for recursive CTEs (not supported — will error gracefully).
                        let mut child_ctes = self.ctes.clone();
                        child_ctes.remove(&tbl_lower);
                        let child_ctx = super::AnalyzerContext {
                            catalog: self.catalog,
                            current_database: self.current_database,
                            ctes: child_ctes,
                        };
                        let mut resolved_cte = child_ctx.analyze_query(cte_query)?;
                        // Apply CTE column aliases if present: WITH t(a, b) AS (...)
                        if !cte_col_aliases.is_empty() {
                            for (i, alias_name) in cte_col_aliases.iter().enumerate() {
                                if let Some(col) = resolved_cte.output_columns.get_mut(i) {
                                    col.name = alias_name.clone();
                                }
                            }
                        }
                        let alias_name = alias
                            .as_ref()
                            .map(|a| a.name.value.clone())
                            .unwrap_or_else(|| tbl.clone());
                        let mut scope = AnalyzerScope::new();
                        for col in &resolved_cte.output_columns {
                            scope.add_column(
                                Some(&alias_name),
                                &col.name,
                                col.data_type.clone(),
                                col.nullable,
                            );
                        }
                        let relation = Relation::Subquery {
                            query: Box::new(resolved_cte),
                            alias: alias_name,
                        };
                        return Ok((relation, scope));
                    }
                }

                let table_def = self.catalog.get_table(&db_lower, &tbl_lower)?;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());

                // Build scope
                let mut scope = AnalyzerScope::new();
                let qualifier = alias_name.as_deref().unwrap_or(&table_def.name);
                scope.add_table(Some(qualifier), &table_def.columns);
                // If alias differs from table name, also register with table name
                if let Some(ref a) = alias_name {
                    if !a.eq_ignore_ascii_case(&table_def.name) {
                        scope.add_table_qualified_only(&table_def.name, &table_def.columns);
                    }
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

                // Build scope from subquery output columns
                let mut scope = AnalyzerScope::new();
                for col in &resolved_query.output_columns {
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
                };

                Ok((relation, scope))
            }
            sqlast::TableFactor::TableFunction { expr, alias } => {
                self.analyze_table_function(expr, alias.as_ref())
            }
            other => Err(format!("unsupported table factor: {other}")),
        }
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
        let values: Vec<i64> = arg_list
            .args
            .iter()
            .map(|arg| match arg {
                sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) => eval_const_i64(e),
                other => Err(format!(
                    "generate_series expects positional args, got {other}"
                )),
            })
            .collect::<Result<_, _>>()?;
        let (start, end, step) = match values.as_slice() {
            [s, e] => (*s, *e, 1i64),
            [s, e, st] => {
                if *st == 0 {
                    return Err("generate_series step must not be zero".into());
                }
                (*s, *e, *st)
            }
            _ => return Err("generate_series expects 2 or 3 arguments".into()),
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
