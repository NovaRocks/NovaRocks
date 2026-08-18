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

//! SQL rewrite helper for Iceberg MV `CompatibleSafeWithRebind` decisions.
//!
//! Rewrites identifiers at column-reference positions (projection, WHERE,
//! JOIN ON, GROUP BY, HAVING, ORDER BY, function arguments) so that base
//! columns are referenced by their current name rather than the
//! `name_at_create` captured in the schema contract. Table names, aliases,
//! and string literals are left alone.
//!
//! For multi-base refresh (join family), qualified identifiers must
//! resolve to a unique base table through the SELECT's FROM / JOIN
//! qualifiers; unqualified identifiers must be unambiguous across the
//! rebind set. Ambiguity is a fail-fast error rather than a best-effort
//! rewrite.

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RebindColumn {
    pub(crate) base_table_fqn: String,
    pub(crate) field_id: i32,
    pub(crate) name_at_create: String,
    pub(crate) current_name: String,
}

pub(crate) fn rewrite_select_sql_for_rebind(
    stored_sql: &str,
    rebound_columns: &[RebindColumn],
) -> Result<String, String> {
    if rebound_columns.is_empty() {
        return Ok(stored_sql.to_string());
    }
    let normalized = novarocks_sql::syntax::normalize_for_raw_parse(stored_sql)
        .map_err(|e| format!("rebind rewrite: normalize_for_raw_parse: {e}"))?;
    let mut stmt = novarocks_sql::syntax::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("rebind rewrite: parse: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("rebind rewrite: expected SELECT query".to_string());
    };
    let order_by = query.order_by.as_mut();
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rebind rewrite: expected SELECT body".to_string());
    };
    let ctx = RebindRewriteContext::new(select, rebound_columns)?;
    rewrite_select(select, order_by, &ctx)?;
    Ok(stmt.to_string())
}

#[derive(Clone, Debug)]
struct RebindRule {
    base_table_fqn: String,
    current_name: String,
}

#[derive(Debug)]
struct RebindRewriteContext {
    rules_by_old_name: std::collections::HashMap<String, Vec<RebindRule>>,
    qualifier_to_base: std::collections::HashMap<String, String>,
}

impl RebindRewriteContext {
    fn new(
        select: &sqlparser::ast::Select,
        rebound_columns: &[RebindColumn],
    ) -> Result<Self, String> {
        let mut rules_by_old_name: std::collections::HashMap<String, Vec<RebindRule>> =
            std::collections::HashMap::new();
        for col in rebound_columns {
            rules_by_old_name
                .entry(col.name_at_create.to_ascii_lowercase())
                .or_default()
                .push(RebindRule {
                    base_table_fqn: col.base_table_fqn.to_ascii_lowercase(),
                    current_name: col.current_name.clone(),
                });
        }
        let qualifier_to_base = collect_select_qualifiers(select);
        Ok(Self {
            rules_by_old_name,
            qualifier_to_base,
        })
    }

    fn rewrite_unqualified(&self, ident: &mut sqlparser::ast::Ident) -> Result<(), String> {
        let key = ident.value.to_ascii_lowercase();
        let Some(rules) = self.rules_by_old_name.get(&key) else {
            return Ok(());
        };
        if rules.len() != 1 {
            return Err(format!(
                "rebind rewrite: ambiguous unqualified column {} matches {} base tables; qualify the MV SELECT",
                ident.value,
                rules.len()
            ));
        }
        ident.value = rules[0].current_name.clone();
        Ok(())
    }

    fn rewrite_qualified(&self, parts: &mut [sqlparser::ast::Ident]) -> Result<(), String> {
        if parts.len() < 2 {
            if let Some(last) = parts.last_mut() {
                return self.rewrite_unqualified(last);
            }
            return Ok(());
        }
        let last_idx = parts.len() - 1;
        let old_name = parts[last_idx].value.to_ascii_lowercase();
        let Some(rules) = self.rules_by_old_name.get(&old_name) else {
            return Ok(());
        };
        let qualifier_full = parts[..last_idx]
            .iter()
            .map(|p| p.value.as_str())
            .collect::<Vec<_>>()
            .join(".")
            .to_ascii_lowercase();
        let qualifier_tail = parts[last_idx - 1].value.to_ascii_lowercase();
        let resolved_base = self
            .qualifier_to_base
            .get(&qualifier_full)
            .or_else(|| self.qualifier_to_base.get(&qualifier_tail))
            .cloned();
        let matches: Vec<&RebindRule> = rules
            .iter()
            .filter(|rule| {
                resolved_base
                    .as_ref()
                    .is_some_and(|base| base == &rule.base_table_fqn)
                    || rule.base_table_fqn == qualifier_full
            })
            .collect();
        if matches.len() == 1 {
            parts[last_idx].value = matches[0].current_name.clone();
            return Ok(());
        }
        Err(format!(
            "rebind rewrite: qualifier {qualifier_full} for column {old_name} does not uniquely match a renamed base column",
        ))
    }
}

fn top_level_column_output_name(expr: &sqlparser::ast::Expr) -> Option<String> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => Some(ident.value.clone()),
        sqlparser::ast::Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
        _ => None,
    }
}

fn collect_select_qualifiers(
    select: &sqlparser::ast::Select,
) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    for table_with_joins in &select.from {
        collect_table_factor_qualifier(&table_with_joins.relation, &mut out);
        for join in &table_with_joins.joins {
            collect_table_factor_qualifier(&join.relation, &mut out);
        }
    }
    out
}

fn collect_table_factor_qualifier(
    relation: &sqlparser::ast::TableFactor,
    out: &mut std::collections::HashMap<String, String>,
) {
    if let sqlparser::ast::TableFactor::Table { name, alias, .. } = relation {
        let fqn = name.to_string().to_ascii_lowercase();
        out.insert(fqn.clone(), fqn.clone());
        if let Some(last) = name.0.last().and_then(|p| p.as_ident()) {
            out.insert(last.value.to_ascii_lowercase(), fqn.clone());
        }
        if let Some(alias) = alias {
            out.insert(alias.name.value.to_ascii_lowercase(), fqn);
        }
    }
}

fn rewrite_select(
    select: &mut sqlparser::ast::Select,
    order_by: Option<&mut sqlparser::ast::OrderBy>,
    ctx: &RebindRewriteContext,
) -> Result<(), String> {
    for item in &mut select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => {
                // Preserve the original output column name when a top-level
                // unaliased column reference is rewritten by rebind. SQL
                // implicitly uses the last identifier as the output column
                // name; rewriting `region` -> `area` would silently change
                // the MV's output column name, breaking the contract with
                // the frozen target table schema.
                let original_output_name = top_level_column_output_name(e);
                rewrite_expr_idents(e, ctx)?;
                if let Some(original) = original_output_name {
                    let current = top_level_column_output_name(e);
                    if current.as_deref() != Some(original.as_str()) {
                        let new_expr = std::mem::replace(
                            e,
                            sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("")),
                        );
                        *item = sqlparser::ast::SelectItem::ExprWithAlias {
                            expr: new_expr,
                            alias: sqlparser::ast::Ident::new(original),
                        };
                    }
                }
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                rewrite_expr_idents(e, ctx)?;
            }
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => {}
        }
    }
    if let Some(filter) = &mut select.selection {
        rewrite_expr_idents(filter, ctx)?;
    }
    for table_with_joins in &mut select.from {
        for join in &mut table_with_joins.joins {
            rewrite_join_constraint(&mut join.join_operator, ctx)?;
        }
    }
    match &mut select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                rewrite_expr_idents(expr, ctx)?;
            }
        }
        sqlparser::ast::GroupByExpr::All(_) => {}
    }
    if let Some(having) = &mut select.having {
        rewrite_expr_idents(having, ctx)?;
    }
    if let Some(order_by) = order_by
        && let sqlparser::ast::OrderByKind::Expressions(exprs) = &mut order_by.kind
    {
        for item in exprs {
            rewrite_expr_idents(&mut item.expr, ctx)?;
        }
    }
    Ok(())
}

fn rewrite_join_constraint(
    op: &mut sqlparser::ast::JoinOperator,
    ctx: &RebindRewriteContext,
) -> Result<(), String> {
    use sqlparser::ast::JoinConstraint;
    let constraint = match op {
        sqlparser::ast::JoinOperator::Join(c)
        | sqlparser::ast::JoinOperator::Inner(c)
        | sqlparser::ast::JoinOperator::Left(c)
        | sqlparser::ast::JoinOperator::LeftOuter(c)
        | sqlparser::ast::JoinOperator::Right(c)
        | sqlparser::ast::JoinOperator::RightOuter(c)
        | sqlparser::ast::JoinOperator::FullOuter(c)
        | sqlparser::ast::JoinOperator::CrossJoin(c)
        | sqlparser::ast::JoinOperator::Semi(c)
        | sqlparser::ast::JoinOperator::LeftSemi(c)
        | sqlparser::ast::JoinOperator::RightSemi(c)
        | sqlparser::ast::JoinOperator::Anti(c)
        | sqlparser::ast::JoinOperator::LeftAnti(c)
        | sqlparser::ast::JoinOperator::RightAnti(c)
        | sqlparser::ast::JoinOperator::StraightJoin(c) => c,
        sqlparser::ast::JoinOperator::AsOf {
            match_condition,
            constraint,
        } => {
            rewrite_expr_idents(match_condition, ctx)?;
            if let JoinConstraint::On(expr) = constraint {
                rewrite_expr_idents(expr, ctx)?;
            }
            return Ok(());
        }
        sqlparser::ast::JoinOperator::CrossApply | sqlparser::ast::JoinOperator::OuterApply => {
            return Ok(());
        }
    };
    if let JoinConstraint::On(expr) = constraint {
        rewrite_expr_idents(expr, ctx)?;
    }
    Ok(())
}

fn rewrite_expr_idents(
    expr: &mut sqlparser::ast::Expr,
    ctx: &RebindRewriteContext,
) -> Result<(), String> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(ident) => ctx.rewrite_unqualified(ident)?,
        Expr::CompoundIdentifier(parts) => ctx.rewrite_qualified(parts)?,
        Expr::BinaryOp { left, right, .. } => {
            rewrite_expr_idents(left, ctx)?;
            rewrite_expr_idents(right, ctx)?;
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::Nested(expr) => {
            rewrite_expr_idents(expr, ctx)?;
        }
        Expr::Function(func) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &mut func.args {
                for arg in &mut list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) = arg
                    {
                        rewrite_expr_idents(inner, ctx)?;
                    }
                }
                for clause in &mut list.clauses {
                    if let sqlparser::ast::FunctionArgumentClause::OrderBy(order_by) = clause {
                        for item in order_by {
                            rewrite_expr_idents(&mut item.expr, ctx)?;
                        }
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                rewrite_expr_idents(op, ctx)?;
            }
            for c in conditions {
                rewrite_expr_idents(&mut c.condition, ctx)?;
                rewrite_expr_idents(&mut c.result, ctx)?;
            }
            if let Some(e) = else_result {
                rewrite_expr_idents(e, ctx)?;
            }
        }
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner) => rewrite_expr_idents(inner, ctx)?,
        Expr::InList { expr, list, .. } => {
            rewrite_expr_idents(expr, ctx)?;
            for e in list {
                rewrite_expr_idents(e, ctx)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_expr_idents(expr, ctx)?;
            rewrite_expr_idents(low, ctx)?;
            rewrite_expr_idents(high, ctx)?;
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            rewrite_expr_idents(expr, ctx)?;
            rewrite_expr_idents(pattern, ctx)?;
        }
        Expr::Subquery(_) | Expr::Exists { .. } => {
            return Err(
                "rebind rewrite: subqueries are not supported in Iceberg MV definitions"
                    .to_string(),
            );
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single(old: &str, new: &str) -> Vec<RebindColumn> {
        vec![RebindColumn {
            base_table_fqn: "ice.db.orders".to_string(),
            field_id: 2,
            name_at_create: old.to_string(),
            current_name: new.to_string(),
        }]
    }

    fn join_rebinds() -> Vec<RebindColumn> {
        vec![
            RebindColumn {
                base_table_fqn: "ice.db.fact".to_string(),
                field_id: 2,
                name_at_create: "dim_id".to_string(),
                current_name: "new_dim_id".to_string(),
            },
            RebindColumn {
                base_table_fqn: "ice.db.dim".to_string(),
                field_id: 1,
                name_at_create: "id".to_string(),
                current_name: "new_id".to_string(),
            },
            RebindColumn {
                base_table_fqn: "ice.db.dim".to_string(),
                field_id: 3,
                name_at_create: "region".to_string(),
                current_name: "area".to_string(),
            },
        ]
    }

    #[test]
    fn rewrites_group_by_and_having_for_single_aggregate() {
        let sql = "SELECT region, COUNT(*) AS c FROM ice.db.orders GROUP BY region HAVING region IS NOT NULL ORDER BY region";
        let rewritten = rewrite_select_sql_for_rebind(sql, &single("region", "area")).unwrap();
        // SELECT projection preserves the original output column name as an
        // alias so the MV target schema contract remains stable.
        assert!(
            rewritten.contains("area AS region"),
            "rewritten={rewritten}"
        );
        // GROUP BY / HAVING / ORDER BY reference the base column directly and
        // do not need an alias.
        assert!(rewritten.contains("GROUP BY area"), "rewritten={rewritten}");
        assert!(
            rewritten.contains("HAVING area IS NOT NULL"),
            "rewritten={rewritten}"
        );
        assert!(rewritten.contains("ORDER BY area"), "rewritten={rewritten}");
    }

    #[test]
    fn rewrites_aggregate_function_argument() {
        let sql = "SELECT region, SUM(amount) AS total_amount FROM ice.db.orders GROUP BY region";
        let rewritten =
            rewrite_select_sql_for_rebind(sql, &single("amount", "gross_amount")).unwrap();
        assert!(rewritten.contains("gross_amount"), "rewritten={rewritten}");
        assert!(
            rewritten.contains("total_amount"),
            "alias must stay unchanged: {rewritten}"
        );
    }

    #[test]
    fn rewrites_join_on_and_group_key_with_qualifiers() {
        let sql = "SELECT d.region, COUNT(*) AS c FROM ice.db.fact AS f JOIN ice.db.dim AS d ON f.dim_id = d.id GROUP BY d.region ORDER BY d.region";
        let rewritten = rewrite_select_sql_for_rebind(sql, &join_rebinds()).unwrap();
        assert!(rewritten.contains("f.new_dim_id"), "rewritten={rewritten}");
        assert!(rewritten.contains("d.new_id"), "rewritten={rewritten}");
        // SELECT projection: rewritten reference + original output name alias.
        assert!(
            rewritten.contains("d.area AS region"),
            "rewritten={rewritten}"
        );
        // GROUP BY / ORDER BY: no alias needed; rewrites cleanly.
        assert!(
            rewritten.contains("GROUP BY d.area"),
            "rewritten={rewritten}"
        );
        assert!(
            rewritten.contains("ORDER BY d.area"),
            "rewritten={rewritten}"
        );
        assert!(!rewritten.contains("f.dim_id"), "rewritten={rewritten}");
    }

    #[test]
    fn join_projection_qualified_rename_keeps_output_alias() {
        let sql = "SELECT d.region FROM ice.db.fact AS f JOIN ice.db.dim AS d ON f.dim_id = d.id";
        let rebound = vec![RebindColumn {
            base_table_fqn: "ice.db.dim".to_string(),
            field_id: 3,
            name_at_create: "region".to_string(),
            current_name: "area".to_string(),
        }];
        let rewritten = rewrite_select_sql_for_rebind(sql, &rebound).unwrap();
        assert!(
            rewritten.contains("d.area AS region"),
            "rewritten={rewritten}"
        );
    }

    #[test]
    fn preserves_string_literals_and_aliases() {
        let sql = "SELECT region AS region_label FROM ice.db.orders WHERE region = 'region'";
        let rewritten = rewrite_select_sql_for_rebind(sql, &single("region", "area")).unwrap();
        assert!(rewritten.contains("area"), "rewritten={rewritten}");
        assert!(
            rewritten.contains("region_label"),
            "alias must stay unchanged: {rewritten}"
        );
        assert!(
            rewritten.contains("'region'"),
            "string literal must stay unchanged: {rewritten}"
        );
    }

    #[test]
    fn rejects_ambiguous_unqualified_join_rebind() {
        let sql = "SELECT id FROM ice.db.fact AS f JOIN ice.db.dim AS d ON f.id = d.id";
        let err = rewrite_select_sql_for_rebind(
            sql,
            &[
                RebindColumn {
                    base_table_fqn: "ice.db.fact".to_string(),
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    current_name: "fact_id".to_string(),
                },
                RebindColumn {
                    base_table_fqn: "ice.db.dim".to_string(),
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    current_name: "dim_id".to_string(),
                },
            ],
        )
        .expect_err("ambiguous unqualified id rejected");
        assert!(err.contains("ambiguous unqualified column"), "err={err}");
    }

    #[test]
    fn no_rebind_returns_input_unchanged() {
        let sql = "SELECT id, region FROM ice.db.orders WHERE region = 'US'";
        let rewritten = rewrite_select_sql_for_rebind(sql, &[]).unwrap();
        assert_eq!(rewritten, sql);
    }
}
