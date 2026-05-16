#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotWindow {
    pub(crate) from: i64,
    pub(crate) to: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BranchSide {
    Delta(SnapshotWindow),
    Snapshot(i64),
}

pub(crate) const JOIN_LEFT_ROW_ID_COLUMN: &str = "__nova_left_row_id";
pub(crate) const JOIN_RIGHT_ROW_ID_COLUMN: &str = "__nova_right_row_id";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinDeltaBranchPlan {
    pub(crate) left_base: crate::connector::starrocks::managed::model::IcebergTableRef,
    pub(crate) right_base: crate::connector::starrocks::managed::model::IcebergTableRef,
    pub(crate) left: BranchSide,
    pub(crate) right: BranchSide,
}

pub(crate) fn plan_join_delta_branches(
    left_base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    right_base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    left_window: SnapshotWindow,
    right_window: SnapshotWindow,
    left_has_changes: bool,
    right_has_changes: bool,
) -> Vec<JoinDeltaBranchPlan> {
    let mut plans = Vec::new();
    if left_has_changes {
        plans.push(JoinDeltaBranchPlan {
            left_base: left_base.clone(),
            right_base: right_base.clone(),
            left: BranchSide::Delta(left_window),
            right: BranchSide::Snapshot(right_window.from),
        });
    }
    if right_has_changes {
        plans.push(JoinDeltaBranchPlan {
            left_base: left_base.clone(),
            right_base: right_base.clone(),
            left: BranchSide::Snapshot(left_window.to),
            right: BranchSide::Delta(right_window),
        });
    }
    plans
}

pub(crate) fn rewrite_join_branch_query(
    query: &sqlparser::ast::Query,
    plan: &JoinDeltaBranchPlan,
    left_alias: &str,
    right_alias: &str,
) -> Result<sqlparser::ast::Query, String> {
    let mut query = query.clone();
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("join branch rewrite requires SELECT body".to_string());
    };
    let [from] = select.from.as_mut_slice() else {
        return Err("join branch rewrite requires one FROM item".to_string());
    };
    let [join] = from.joins.as_mut_slice() else {
        return Err("join branch rewrite requires one JOIN".to_string());
    };
    let effective_left_alias =
        rewrite_branch_factor(&mut from.relation, &plan.left_base, plan.left, left_alias)?;
    let effective_right_alias = rewrite_branch_factor(
        &mut join.relation,
        &plan.right_base,
        plan.right,
        right_alias,
    )?;
    append_join_hidden_projection(select, &effective_left_alias, &effective_right_alias);
    Ok(query)
}

fn rewrite_branch_factor(
    factor: &mut sqlparser::ast::TableFactor,
    base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    side: BranchSide,
    alias: &str,
) -> Result<String, String> {
    match side {
        BranchSide::Delta(window) => {
            let effective_alias = table_factor_alias_name(factor)
                .ok_or_else(|| "join branch delta side must be a table".to_string())?
                .unwrap_or_else(|| alias.to_string());
            *factor = build_nr_ivm_delta_table_factor_for_join(base, window, &effective_alias);
            Ok(effective_alias)
        }
        BranchSide::Snapshot(snapshot_id) => {
            let sqlparser::ast::TableFactor::Table {
                name,
                version,
                alias: factor_alias,
                ..
            } = factor
            else {
                return Err("join branch snapshot side must be a table".to_string());
            };
            *name = snapshot_table_object_name(base, snapshot_id);
            *version = None;
            let effective_alias = factor_alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_else(|| alias.to_string());
            if factor_alias.is_none() {
                *factor_alias = Some(sqlparser::ast::TableAlias {
                    explicit: true,
                    name: sqlparser::ast::Ident::new(&effective_alias),
                    columns: Vec::new(),
                });
            }
            Ok(effective_alias)
        }
    }
}

fn table_factor_alias_name(factor: &sqlparser::ast::TableFactor) -> Option<Option<String>> {
    let sqlparser::ast::TableFactor::Table { alias, .. } = factor else {
        return None;
    };
    Some(alias.as_ref().map(|alias| alias.name.value.clone()))
}

fn snapshot_table_object_name(
    base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    snapshot_id: i64,
) -> sqlparser::ast::ObjectName {
    sqlparser::ast::ObjectName(vec![
        sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(&base.namespace)),
        sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(format!(
            "{}__at_{}",
            base.table, snapshot_id
        ))),
    ])
}

fn build_nr_ivm_delta_table_factor_for_join(
    base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    window: SnapshotWindow,
    alias: &str,
) -> sqlparser::ast::TableFactor {
    use sqlparser::ast as sqlast;
    let make_string_arg = |s: String| -> sqlast::FunctionArg {
        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(sqlast::Expr::Value(
            sqlast::Value::SingleQuotedString(s).into(),
        )))
    };
    let make_number_arg = |n: i64| -> sqlast::FunctionArg {
        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(sqlast::Expr::Value(
            sqlast::Value::Number(n.to_string(), false).into(),
        )))
    };
    sqlast::TableFactor::Table {
        name: sqlast::ObjectName(vec![sqlast::ObjectNamePart::Identifier(
            sqlast::Ident::new("__nr_ivm_delta"),
        )]),
        alias: Some(sqlast::TableAlias {
            explicit: true,
            name: sqlast::Ident::new(alias),
            columns: Vec::new(),
        }),
        args: Some(sqlast::TableFunctionArgs {
            args: vec![
                make_string_arg(base.fqn()),
                make_number_arg(window.from),
                make_number_arg(window.to),
            ],
            settings: None,
        }),
        with_hints: Vec::new(),
        version: None,
        with_ordinality: false,
        partitions: Vec::new(),
        json_path: None,
        sample: None,
        index_hints: Vec::new(),
    }
}

fn append_join_hidden_projection(
    select: &mut sqlparser::ast::Select,
    left_alias: &str,
    right_alias: &str,
) {
    select
        .projection
        .push(sqlparser::ast::SelectItem::UnnamedExpr(
            sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new(
                crate::exec::change_op::CHANGE_OP_COLUMN,
            )),
        ));
    select
        .projection
        .push(row_id_alias(left_alias, JOIN_LEFT_ROW_ID_COLUMN));
    select
        .projection
        .push(row_id_alias(right_alias, JOIN_RIGHT_ROW_ID_COLUMN));
}

fn row_id_alias(alias: &str, output: &str) -> sqlparser::ast::SelectItem {
    sqlparser::ast::SelectItem::ExprWithAlias {
        expr: sqlparser::ast::Expr::CompoundIdentifier(vec![
            sqlparser::ast::Ident::new(alias),
            sqlparser::ast::Ident::new("_row_id"),
        ]),
        alias: sqlparser::ast::Ident::new(output),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(name: &str) -> crate::connector::starrocks::managed::model::IcebergTableRef {
        crate::connector::starrocks::managed::model::IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: name.to_string(),
        }
    }

    #[test]
    fn both_changed_uses_telescoping_order() {
        let left = base("left");
        let right = base("right");
        let plans = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 21 },
            true,
            true,
        );
        assert_eq!(plans.len(), 2);
        assert_eq!(
            plans[0].left,
            BranchSide::Delta(SnapshotWindow { from: 10, to: 11 })
        );
        assert_eq!(plans[0].right, BranchSide::Snapshot(20));
        assert_eq!(plans[1].left, BranchSide::Snapshot(11));
        assert_eq!(
            plans[1].right,
            BranchSide::Delta(SnapshotWindow { from: 20, to: 21 })
        );
    }

    #[test]
    fn only_left_changed_has_one_branch() {
        let left = base("left");
        let right = base("right");
        let plans = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 20 },
            true,
            false,
        );
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].left,
            BranchSide::Delta(SnapshotWindow { from: 10, to: 11 })
        );
        assert_eq!(plans[0].right, BranchSide::Snapshot(20));
    }

    #[test]
    fn branch_rewrite_delta_left_snapshot_right() {
        let query = parse_query(
            "select l.id, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
        );
        let left = base("left");
        let right = base("right");
        let plan = JoinDeltaBranchPlan {
            left_base: left,
            right_base: right,
            left: BranchSide::Delta(SnapshotWindow { from: 10, to: 11 }),
            right: BranchSide::Snapshot(20),
        };
        let rewritten = rewrite_join_branch_query(&query, &plan, "l", "r").expect("rewrite");
        let rendered = rewritten.to_string();
        assert!(rendered.contains("__nr_ivm_delta"), "sql={rendered}");
        assert!(rendered.contains("right__at_20"), "sql={rendered}");
        assert!(rendered.contains("__nova_left_row_id"), "sql={rendered}");
        assert!(rendered.contains("__nova_right_row_id"), "sql={rendered}");
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        *query
    }
}
