use crate::connector::starrocks::managed::mv_agg_state::{
    AGG_RETRACTION_COUNT_STATE_COLUMN, aggregate_shape_needs_retraction_count_state,
    sanitize_state_column_name,
};
use crate::connector::starrocks::managed::mv_shape::{
    AggregateCallShape, AggregateFunctionKind, AggregateInput, AggregateMvShape,
    VisibleAggregateOutput,
};
use crate::exec::change_op::CHANGE_OP_COLUMN;
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    ObjectName, ObjectNamePart, SelectItem, SetExpr, Statement, Value,
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
    let func_name = combinator_name_for_kind(aggregate.function, true);
    let state_alias = aggregate_state_alias(&aggregate.output_name);
    let input = signed_state_input_expr(aggregate)?;
    projection.push(make_two_arg_aggregate_select_item(
        func_name,
        input,
        change_op.expr(),
        &state_alias,
    ));
    Ok(())
}

fn signed_state_input_expr(aggregate: &AggregateCallShape) -> Result<Expr, String> {
    match &aggregate.input {
        AggregateInput::Star => {
            if aggregate.function == AggregateFunctionKind::Count {
                Ok(Expr::Value(Value::Number("1".to_string(), false).into()))
            } else {
                Err(format!(
                    "rewrite_select_sql_for_signed_delta_state: {} requires an expression input",
                    aggregate_function_label(aggregate.function)
                ))
            }
        }
        AggregateInput::Expr(expr) => Ok(expr.as_ref().clone()),
    }
}

fn aggregate_state_alias(output_name: &str) -> String {
    let sanitized = sanitize_state_column_name(output_name);
    format!("__agg_state_{sanitized}")
}

fn aggregate_function_label(kind: AggregateFunctionKind) -> &'static str {
    match kind {
        AggregateFunctionKind::Count => "COUNT",
        AggregateFunctionKind::Sum => "SUM",
        AggregateFunctionKind::Avg => "AVG",
        AggregateFunctionKind::Min => "MIN",
        AggregateFunctionKind::Max => "MAX",
        AggregateFunctionKind::BoolOr => "BOOL_OR",
        AggregateFunctionKind::BoolAnd => "BOOL_AND",
        AggregateFunctionKind::CountDistinct => "COUNT_DISTINCT",
        AggregateFunctionKind::ApproxCountDistinct => "APPROX_COUNT_DISTINCT",
    }
}

fn combinator_name_for_kind(kind: AggregateFunctionKind, signed: bool) -> &'static str {
    match (kind, signed) {
        (AggregateFunctionKind::Count, false) => "count_state",
        (AggregateFunctionKind::Count, true) => "count_state_signed",
        (AggregateFunctionKind::Sum, false) => "sum_state",
        (AggregateFunctionKind::Sum, true) => "sum_state_signed",
        (AggregateFunctionKind::Avg, false) => "avg_state",
        (AggregateFunctionKind::Avg, true) => "avg_state_signed",
        (AggregateFunctionKind::Min, false) => "min_state",
        (AggregateFunctionKind::Min, true) => "min_state_signed",
        (AggregateFunctionKind::Max, false) => "max_state",
        (AggregateFunctionKind::Max, true) => "max_state_signed",
        (AggregateFunctionKind::BoolOr, false) => "bool_or_state",
        (AggregateFunctionKind::BoolOr, true) => "bool_or_state_signed",
        (AggregateFunctionKind::BoolAnd, false) => "bool_and_state",
        (AggregateFunctionKind::BoolAnd, true) => "bool_and_state_signed",
        (AggregateFunctionKind::CountDistinct, false) => "count_distinct_state",
        (AggregateFunctionKind::CountDistinct, true) => "count_distinct_state_signed",
        (AggregateFunctionKind::ApproxCountDistinct, false) => "approx_count_distinct_state",
        (AggregateFunctionKind::ApproxCountDistinct, true) => "approx_count_distinct_state_signed",
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
            upper.contains("COUNT_STATE_SIGNED(1, F.__CHANGE_OP) AS __AGG_STATE_C"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("SUM_STATE_SIGNED(F.AMOUNT, F.__CHANGE_OP) AS __AGG_STATE_S"),
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

        assert!(
            upper.contains("COUNT_STATE_SIGNED(1, __CHANGE_OP) AS __AGG_STATE_C"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn signed_delta_rewrite_turns_sum_into_sum_state_signed() {
        let sql = "select k1, sum(v2) as s from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state(sql, &shape).expect("rewrite signed delta");
        let upper = rewritten.to_uppercase();

        assert!(upper.contains("K1 AS K1"), "got: {rewritten}");
        assert!(
            upper.contains("SUM_STATE_SIGNED(V2, __CHANGE_OP) AS __AGG_STATE_S"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("SUM(__CHANGE_OP) AS __AGG_STATE___IVM_ROW_COUNT"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn signed_delta_rewrite_turns_count_star_into_count_state_signed() {
        let sql = "select k1, count(*) as c from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state(sql, &shape).expect("rewrite signed delta");
        let upper = rewritten.to_uppercase();

        assert!(
            upper.contains("COUNT_STATE_SIGNED(1, __CHANGE_OP) AS __AGG_STATE_C"),
            "got: {rewritten}"
        );
        assert!(!upper.contains("COUNT(*)"), "got: {rewritten}");
    }

    #[test]
    fn signed_delta_rewrite_turns_avg_into_avg_state_signed() {
        let sql = "select k1, avg(v2) as a from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state(sql, &shape).expect("rewrite signed delta");
        let upper = rewritten.to_uppercase();

        assert!(
            upper.contains("AVG_STATE_SIGNED(V2, __CHANGE_OP) AS __AGG_STATE_A"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("SUM(__CHANGE_OP) AS __AGG_STATE___IVM_ROW_COUNT"),
            "got: {rewritten}"
        );
        assert!(!upper.contains("AVG(V2)"), "got: {rewritten}");
    }

    #[test]
    fn signed_delta_rewrite_accepts_min_max_with_state_signed() {
        let sql = "select k1, min(v2) as mn, max(v2) as mx from ice.ns.orders group by k1";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state(sql, &shape).expect("rewrite signed delta");
        let upper = rewritten.to_uppercase();

        assert!(
            upper.contains("MIN_STATE_SIGNED(V2, __CHANGE_OP) AS __AGG_STATE_MN"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("MAX_STATE_SIGNED(V2, __CHANGE_OP) AS __AGG_STATE_MX"),
            "got: {rewritten}"
        );
        assert!(
            !upper.contains("MAP_VALUE_COUNT_SIGNED"),
            "legacy combinator must be replaced; got: {rewritten}"
        );
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
    fn signed_delta_projection_emits_per_kind_combinator() {
        let sql = "select region, count(distinct user_id) as u, \
                   approx_count_distinct(session_id) as s, bool_or(flag) as f \
                   from ice.ns.events group by region";
        let shape = parse_aggregate_shape(sql);
        let rewritten =
            rewrite_select_sql_for_signed_delta_state(sql, &shape).expect("rewrite signed delta");
        let upper = rewritten.to_uppercase();

        assert!(
            upper.contains("COUNT_DISTINCT_STATE_SIGNED(USER_ID, __CHANGE_OP)"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("APPROX_COUNT_DISTINCT_STATE_SIGNED(SESSION_ID, __CHANGE_OP)"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("BOOL_OR_STATE_SIGNED(FLAG, __CHANGE_OP)"),
            "got: {rewritten}"
        );
        assert!(
            !upper.contains("MAP_VALUE_COUNT_SIGNED"),
            "legacy combinator must be replaced; got: {rewritten}"
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

        assert!(
            upper.contains("COUNT_STATE_SIGNED(1, __CHANGE_OP) AS __AGG_STATE_C"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("SUM_STATE_SIGNED(V2, __CHANGE_OP) AS __AGG_STATE_S"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("MIN_STATE_SIGNED(V2, __CHANGE_OP) AS __AGG_STATE_MN"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("MAX_STATE_SIGNED(V3, __CHANGE_OP) AS __AGG_STATE_MX"),
            "got: {rewritten}"
        );
        assert!(
            !upper.contains("MAP_VALUE_COUNT_SIGNED"),
            "legacy combinator must be replaced; got: {rewritten}"
        );
        assert!(
            !upper.contains("MIN(V2)") && !upper.contains("MAX(V3)"),
            "signed-delta rewrite must not project visible MIN/MAX; got: {rewritten}"
        );
    }
}
