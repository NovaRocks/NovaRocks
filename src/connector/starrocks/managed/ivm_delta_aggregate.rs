use crate::connector::starrocks::managed::mv_agg_state::{
    AGG_RETRACTION_COUNT_STATE_COLUMN, aggregate_shape_needs_retraction_count_state,
    sanitize_state_column_name,
};
use crate::connector::starrocks::managed::mv_shape::{
    AggregateCallShape, AggregateFunctionKind, AggregateInput, AggregateMvShape,
    VisibleAggregateOutput,
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
    rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(select_sql, shape, None)
}

pub(crate) fn rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(
    select_sql: &str,
    shape: &AggregateMvShape,
    change_op_qualifier: Option<&str>,
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

    let change_op = ChangeOpExpr::new(change_op_qualifier);
    select.projection = signed_delta_projection(shape, &change_op)?;

    Ok(stmt.to_string())
}

struct ChangeOpExpr {
    qualifier: Option<String>,
}

impl ChangeOpExpr {
    fn new(qualifier: Option<&str>) -> Self {
        Self {
            qualifier: qualifier.map(ToString::to_string),
        }
    }

    fn expr(&self) -> Expr {
        match &self.qualifier {
            Some(qualifier) => {
                Expr::CompoundIdentifier(vec![Ident::new(qualifier), Ident::new(CHANGE_OP_COLUMN)])
            }
            None => Expr::Identifier(Ident::new(CHANGE_OP_COLUMN)),
        }
    }
}

fn signed_delta_projection(
    shape: &AggregateMvShape,
    change_op: &ChangeOpExpr,
) -> Result<Vec<SelectItem>, String> {
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
                    alias: select_alias_ident(&group_key.output_name),
                });
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let aggregate = shape.aggregates.get(*aggregate_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_signed_delta_state: aggregate index {aggregate_index} out of range"
                    )
                })?;
                push_signed_aggregate_state_projection(&mut projection, aggregate, change_op)?;
            }
        }
    }
    if aggregate_shape_needs_retraction_count_state(shape) {
        projection.push(make_aggregate_select_item(
            "SUM",
            change_op.expr(),
            AGG_RETRACTION_COUNT_STATE_COLUMN,
        ));
    }
    Ok(projection)
}

fn push_signed_aggregate_state_projection(
    projection: &mut Vec<SelectItem>,
    aggregate: &AggregateCallShape,
    change_op: &ChangeOpExpr,
) -> Result<(), String> {
    match aggregate.function {
        AggregateFunctionKind::Count => match &aggregate.input {
            AggregateInput::Star => projection.push(make_aggregate_select_item(
                "SUM",
                change_op.expr(),
                &aggregate.output_name,
            )),
            AggregateInput::Expr(expr) => projection.push(make_aggregate_select_item(
                "SUM",
                count_expr_signed_delta_arg(expr.as_ref().clone(), change_op),
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
                signed_value_expr(expr.as_ref().clone(), change_op),
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
                signed_value_expr(expr.as_ref().clone(), change_op),
                &sum_alias,
            ));
            projection.push(make_aggregate_select_item(
                "SUM",
                count_expr_signed_delta_arg(expr.as_ref().clone(), change_op),
                &count_alias,
            ));
        }
        AggregateFunctionKind::Min
        | AggregateFunctionKind::Max
        | AggregateFunctionKind::BoolOr
        | AggregateFunctionKind::BoolAnd => {
            // IVM-P5 (Phase 3): MIN/MAX delta state is a per-group
            // `Map<value, Int64>` detail map. In the signed-delta path the
            // map values are signed by `__change_op` (+1 for INSERT, -1 for
            // DELETE), so we emit `map_value_count_signed(arg, change_op)`
            // as the state-column projection. The visible MIN/MAX value is
            // never sourced from the delta in this path — Phase 4 will
            // re-derive it from the merged detail map at write time — so we
            // do not emit a visible projection here. (Compare with COUNT/
            // SUM, where the same expression doubles as both visible and
            // state value.)
            //
            // IVM-BoolAgg (2026-05-23): BOOL_OR / BOOL_AND share the exact
            // same `Map<Boolean, Int64>` detail-state contract; the only
            // difference vs MIN/MAX is the visible-derivation logic in
            // `update_visible_values_from_state` (Phase 4 hook).
            let aggregate_label = match aggregate.function {
                AggregateFunctionKind::Min | AggregateFunctionKind::Max => "MIN/MAX",
                AggregateFunctionKind::BoolOr | AggregateFunctionKind::BoolAnd => {
                    "BOOL_OR/BOOL_AND"
                }
                _ => unreachable!(),
            };
            let AggregateInput::Expr(expr) = &aggregate.input else {
                return Err(format!(
                    "rewrite_select_sql_for_signed_delta_state: {aggregate_label} requires an expression input"
                ));
            };
            let sanitized = sanitize_state_column_name(&aggregate.output_name);
            let state_alias = format!("__agg_state_{sanitized}");
            projection.push(make_two_arg_aggregate_select_item(
                "map_value_count_signed",
                expr.as_ref().clone(),
                change_op.expr(),
                &state_alias,
            ));
        }
    }
    Ok(())
}

fn signed_value_expr(expr: Expr, change_op: &ChangeOpExpr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(expr),
        op: BinaryOperator::Multiply,
        right: Box::new(change_op.expr()),
    }
}

fn count_expr_signed_delta_arg(expr: Expr, change_op: &ChangeOpExpr) -> Expr {
    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![CaseWhen {
            condition: Expr::IsNotNull(Box::new(expr)),
            result: change_op.expr(),
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
        alias: select_alias_ident(alias),
    }
}

fn make_two_arg_aggregate_select_item(
    func_name: &str,
    arg1: Expr,
    arg2: Expr,
    alias: &str,
) -> SelectItem {
    let function = Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(func_name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(arg1)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(arg2)),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    };
    SelectItem::ExprWithAlias {
        expr: Expr::Function(function),
        alias: select_alias_ident(alias),
    }
}

fn select_alias_ident(alias: &str) -> Ident {
    if is_plain_identifier(alias) {
        Ident::new(alias)
    } else {
        Ident::with_quote('`', alias)
    }
}

fn is_plain_identifier(alias: &str) -> bool {
    let mut chars = alias.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::managed::mv_shape::{AggregateMvShape, IncrementalMvShape};

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        *query
    }

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
    fn join_signed_delta_rewrite_qualifies_change_op_to_delta_alias() {
        let sql = "select d.region, count(*) as c, sum(f.amount) as s \
               from ice.ns.fact f join ice.ns.dim d on f.dim_id = d.id \
               group by d.region";
        let shape = match crate::connector::starrocks::managed::mv_shape::classify_incremental_mv_query(
            &parse_query(sql),
        )
        .expect("classify")
        {
            crate::connector::starrocks::managed::mv_shape::IncrementalMvShape::JoinAggregate(shape) => {
                shape
            }
            other => panic!("expected join aggregate, got {other:?}"),
        };

        let rewritten = rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(
            sql,
            &shape.as_aggregate_shape_for_layout(),
            Some("f"),
        )
        .expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(
            upper.contains("SUM(F.__CHANGE_OP) AS C"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("SUM(F.AMOUNT * F.__CHANGE_OP)")
                || upper.contains("SUM((F.AMOUNT * F.__CHANGE_OP))"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn single_signed_delta_rewrite_keeps_unqualified_change_op() {
        let sql = "select k1, count(*) as c from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(sql, &shape, None)
                .expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(upper.contains("SUM(__CHANGE_OP) AS C"), "got: {rewritten}");
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
    fn signed_delta_rewrite_accepts_min_max_with_map_value_count_signed() {
        let sql = "select k1, min(v2) as mn, max(v2) as mx from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state(sql, &shape).expect("rewrite signed delta");
        let upper = rewritten.to_uppercase();

        // MIN/MAX state columns become `map_value_count_signed(arg, __change_op)`.
        assert!(
            upper.contains("MAP_VALUE_COUNT_SIGNED(V2, __CHANGE_OP)"),
            "got: {rewritten}"
        );
        // Two state projections, one per MIN/MAX, with sanitized aliases.
        assert!(
            rewritten.contains("__agg_state_mn"),
            "missing mn state alias; got: {rewritten}"
        );
        assert!(
            rewritten.contains("__agg_state_mx"),
            "missing mx state alias; got: {rewritten}"
        );
        // Visible MIN/MAX scalars are NOT emitted by the signed-delta rewriter;
        // Phase 4 will re-derive the visible value from the merged detail map.
        assert!(
            !upper.contains("MIN(V2)") && !upper.contains("MAX(V2)"),
            "signed-delta rewrite should not project visible MIN/MAX; got: {rewritten}"
        );
        // Retraction count row column must still be present.
        assert!(
            upper.contains("SUM(__CHANGE_OP) AS __AGG_STATE___IVM_ROW_COUNT"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn signed_delta_rewrite_combined_min_max_and_others() {
        let sql = "select k1, count(*) as c, sum(v2) as s, min(v2) as mn, max(v3) as mx \
                   from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state(sql, &shape).expect("rewrite signed delta");
        let upper = rewritten.to_uppercase();

        // COUNT(*) -> SUM(__change_op) AS c
        assert!(upper.contains("SUM(__CHANGE_OP) AS C"), "got: {rewritten}");
        // SUM(v2) -> SUM(v2 * __change_op) AS s
        assert!(
            upper.contains("SUM(V2 * __CHANGE_OP)") || upper.contains("SUM((V2 * __CHANGE_OP))"),
            "got: {rewritten}"
        );
        assert!(upper.contains("AS S"), "got: {rewritten}");
        // MIN(v2) -> map_value_count_signed(v2, __change_op) AS __agg_state_mn
        assert!(
            upper.contains("MAP_VALUE_COUNT_SIGNED(V2, __CHANGE_OP)"),
            "got: {rewritten}"
        );
        assert!(
            rewritten.contains("__agg_state_mn"),
            "missing mn state alias; got: {rewritten}"
        );
        // MAX(v3) -> map_value_count_signed(v3, __change_op) AS __agg_state_mx
        assert!(
            upper.contains("MAP_VALUE_COUNT_SIGNED(V3, __CHANGE_OP)"),
            "got: {rewritten}"
        );
        assert!(
            rewritten.contains("__agg_state_mx"),
            "missing mx state alias; got: {rewritten}"
        );
        // No literal MIN(...) / MAX(...) projection in signed-delta path.
        assert!(
            !upper.contains("MIN(V2)") && !upper.contains("MAX(V3)"),
            "signed-delta rewrite must not project visible MIN/MAX; got: {rewritten}"
        );
    }
}
