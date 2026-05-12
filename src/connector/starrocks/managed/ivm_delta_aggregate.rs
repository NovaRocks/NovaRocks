use crate::connector::starrocks::managed::mv_agg_state::{
    AGG_RETRACTION_COUNT_STATE_COLUMN, aggregate_shape_needs_retraction_count_state,
    sanitize_state_column_name,
};
use crate::connector::starrocks::managed::mv_shape::{
    AggregateFunctionKind, AggregateInput, AggregateMvShape, VisibleAggregateOutput,
};
use crate::exec::change_op::CHANGE_OP_COLUMN;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, CaseWhen, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, SelectItem, SetExpr, Statement, Value,
};

pub(crate) fn rewrite_select_sql_for_signed_delta_state(
    select_sql: &str,
    shape: &AggregateMvShape,
) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("rewrite_select_sql_for_signed_delta_state normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("rewrite_select_sql_for_signed_delta_state parse error: {e}"))?;

    let Statement::Query(query) = &mut stmt else {
        return Err(
            "rewrite_select_sql_for_signed_delta_state: expected Query statement".to_string(),
        );
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rewrite_select_sql_for_signed_delta_state: expected SELECT body".to_string());
    };

    if shape.aggregates.iter().any(|agg| {
        matches!(
            agg.function,
            AggregateFunctionKind::Min | AggregateFunctionKind::Max
        )
    }) {
        return Err(
            "MIN/MAX aggregate outputs are not reversible: delete-bearing signed delta state cannot be consumed incrementally"
                .to_string(),
        );
    }

    let mut projection = Vec::with_capacity(shape.visible_outputs.len() + shape.aggregates.len());
    for output in &shape.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let group_key = shape.group_keys.get(*group_key_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_signed_delta_state: group key index {group_key_index} out of range"
                    )
                })?;
                projection.push(SelectItem::ExprWithAlias {
                    expr: group_key.expr.clone(),
                    alias: Ident::new(group_key.output_name.clone()),
                });
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let aggregate = shape.aggregates.get(*aggregate_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_signed_delta_state: aggregate index {aggregate_index} out of range"
                    )
                })?;
                match aggregate.function {
                    AggregateFunctionKind::Count => match &aggregate.input {
                        AggregateInput::Star => projection.push(make_aggregate_select_item(
                            "SUM",
                            change_op_expr(),
                            &aggregate.output_name,
                        )),
                        AggregateInput::Expr(expr) => projection.push(make_aggregate_select_item(
                            "SUM",
                            count_expr_signed_delta_arg(expr.as_ref().clone()),
                            &aggregate.output_name,
                        )),
                    },
                    AggregateFunctionKind::Sum => {
                        let AggregateInput::Expr(expr) = &aggregate.input else {
                            return Err(
                                "rewrite_select_sql_for_signed_delta_state: SUM requires an expression input"
                                    .to_string(),
                            );
                        };
                        projection.push(make_aggregate_select_item(
                            "SUM",
                            signed_value_expr(expr.as_ref().clone()),
                            &aggregate.output_name,
                        ));
                    }
                    AggregateFunctionKind::Avg => {
                        let AggregateInput::Expr(expr) = &aggregate.input else {
                            return Err(
                                "rewrite_select_sql_for_signed_delta_state: AVG requires an expression input"
                                    .to_string(),
                            );
                        };
                        let sanitized = sanitize_state_column_name(&aggregate.output_name);
                        let sum_alias = format!("__agg_state_{sanitized}__sum");
                        let count_alias = format!("__agg_state_{sanitized}__count");
                        projection.push(make_aggregate_select_item(
                            "SUM",
                            signed_value_expr(expr.as_ref().clone()),
                            &sum_alias,
                        ));
                        projection.push(make_aggregate_select_item(
                            "SUM",
                            count_expr_signed_delta_arg(expr.as_ref().clone()),
                            &count_alias,
                        ));
                    }
                    AggregateFunctionKind::Min | AggregateFunctionKind::Max => unreachable!(
                        "MIN/MAX aggregate functions are rejected before projection rewrite"
                    ),
                }
            }
        }
    }
    if aggregate_shape_needs_retraction_count_state(shape) {
        projection.push(make_aggregate_select_item(
            "SUM",
            change_op_expr(),
            AGG_RETRACTION_COUNT_STATE_COLUMN,
        ));
    }
    select.projection = projection;

    Ok(stmt.to_string())
}

fn change_op_expr() -> Expr {
    Expr::Identifier(Ident::new(CHANGE_OP_COLUMN))
}

fn signed_value_expr(expr: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(expr),
        op: BinaryOperator::Multiply,
        right: Box::new(change_op_expr()),
    }
}

fn count_expr_signed_delta_arg(expr: Expr) -> Expr {
    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![CaseWhen {
            condition: Expr::IsNotNull(Box::new(expr)),
            result: change_op_expr(),
        }],
        else_result: Some(Box::new(Expr::Value(
            Value::Number("0".to_string(), false).into(),
        ))),
    }
}

fn make_aggregate_select_item(func_name: &str, arg: Expr, alias: &str) -> SelectItem {
    let function = Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(func_name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    };
    SelectItem::ExprWithAlias {
        expr: Expr::Function(function),
        alias: Ident::new(alias),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::managed::mv_shape::{AggregateMvShape, IncrementalMvShape};

    fn parse_aggregate_shape(sql: &str) -> AggregateMvShape {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        match crate::connector::starrocks::managed::mv_shape::classify_incremental_mv_query(&query)
            .expect("classify")
        {
            IncrementalMvShape::Aggregate(shape) => shape,
            _ => panic!("expected aggregate shape"),
        }
    }

    #[test]
    fn signed_delta_rewrite_turns_sum_into_sum_times_change_op() {
        let sql = "select k1, sum(v2) as s from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state(sql, &shape).expect("rewrite signed delta");
        let upper = rewritten.to_uppercase();

        assert!(upper.contains("K1 AS K1"), "got: {rewritten}");
        assert!(
            upper.contains("SUM(V2 * __CHANGE_OP)") || upper.contains("SUM((V2 * __CHANGE_OP))"),
            "got: {rewritten}"
        );
        assert!(upper.contains("AS S"), "got: {rewritten}");
        assert!(
            upper.contains("SUM(__CHANGE_OP) AS __AGG_STATE___IVM_ROW_COUNT"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn signed_delta_rewrite_turns_count_star_into_sum_change_op() {
        let sql = "select k1, count(*) as c from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state(sql, &shape).expect("rewrite signed delta");
        let upper = rewritten.to_uppercase();

        assert!(upper.contains("SUM(__CHANGE_OP)"), "got: {rewritten}");
        assert!(upper.contains("AS C"), "got: {rewritten}");
        assert!(!upper.contains("COUNT(*)"), "got: {rewritten}");
    }

    #[test]
    fn signed_delta_rewrite_expands_avg_to_signed_sum_and_count() {
        let sql = "select k1, avg(v2) as a from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state(sql, &shape).expect("rewrite signed delta");
        let upper = rewritten.to_uppercase();

        assert!(
            upper.contains("SUM(V2 * __CHANGE_OP)") || upper.contains("SUM((V2 * __CHANGE_OP))"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("CASE WHEN V2 IS NOT NULL THEN __CHANGE_OP ELSE 0 END"),
            "got: {rewritten}"
        );
        assert!(rewritten.contains("__agg_state_a__sum"), "got: {rewritten}");
        assert!(
            rewritten.contains("__agg_state_a__count"),
            "got: {rewritten}"
        );
        assert!(!upper.contains("AVG(V2)"), "got: {rewritten}");
    }

    #[test]
    fn signed_delta_rewrite_rejects_min_max() {
        let sql = "select k1, min(v2) as mn, max(v2) as mx from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let err = rewrite_select_sql_for_signed_delta_state(sql, &shape).expect_err("reject");

        assert!(err.contains("MIN/MAX"), "err={err}");
        assert!(
            err.contains("delete-bearing signed delta state"),
            "err={err}"
        );
        assert!(err.contains("incrementally"), "err={err}");
    }
}
