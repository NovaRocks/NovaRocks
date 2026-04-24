#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IncrementalMvShape {
    pub(crate) base_table: sqlparser::ast::ObjectName,
}

pub(crate) fn classify_incremental_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<IncrementalMvShape, String> {
    reject_unsupported_query_clauses(query)?;

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(projection_filter_error());
    };
    reject_unsupported_select_clauses(select)?;
    reject_match_against_before_from_shape_check(select)?;

    let [from] = select.from.as_slice() else {
        return Err(single_base_table_error());
    };
    if !from.joins.is_empty() {
        return Err(single_base_table_error());
    }

    let sqlparser::ast::TableFactor::Table {
        name,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = &from.relation
    else {
        return Err(projection_filter_error());
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(single_base_table_error());
    }
    if !is_three_part_object_name(name) {
        return Err(single_base_table_error());
    }

    reject_unsupported_projection_filter_exprs(select)?;

    Ok(IncrementalMvShape {
        base_table: name.clone(),
    })
}

fn reject_unsupported_query_clauses(query: &sqlparser::ast::Query) -> Result<(), String> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn reject_unsupported_select_clauses(select: &sqlparser::ast::Select) -> Result<(), String> {
    if select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !is_empty_group_by(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn reject_unsupported_projection_filter_exprs(
    select: &sqlparser::ast::Select,
) -> Result<(), String> {
    for item in &select.projection {
        reject_unsupported_select_item_expr(item)?;
    }
    if let Some(selection) = &select.selection {
        reject_unsupported_expr(selection)?;
    }
    Ok(())
}

fn reject_unsupported_select_item_expr(item: &sqlparser::ast::SelectItem) -> Result<(), String> {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => reject_unsupported_expr(expr),
        sqlparser::ast::SelectItem::QualifiedWildcard(kind, _) => {
            if let sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr) = kind {
                reject_unsupported_expr(expr)?;
            }
            Ok(())
        }
        sqlparser::ast::SelectItem::Wildcard(_) => Ok(()),
    }
}

fn reject_match_against_before_from_shape_check(
    select: &sqlparser::ast::Select,
) -> Result<(), String> {
    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                if contains_match_against(expr) {
                    return Err(projection_filter_error());
                }
            }
            sqlparser::ast::SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                _,
            ) => {
                if contains_match_against(expr) {
                    return Err(projection_filter_error());
                }
            }
            sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
            | sqlparser::ast::SelectItem::Wildcard(_) => {}
        }
    }
    if let Some(selection) = &select.selection
        && contains_match_against(selection)
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn contains_match_against(expr: &sqlparser::ast::Expr) -> bool {
    matches!(expr, sqlparser::ast::Expr::MatchAgainst { .. })
        || matches!(
            expr,
            sqlparser::ast::Expr::Function(function)
                if function.name.to_string().eq_ignore_ascii_case("match")
        )
}

fn reject_unsupported_expr(expr: &sqlparser::ast::Expr) -> Result<(), String> {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::GroupingSets(_)
        | Expr::Cube(_)
        | Expr::Rollup(_)
        | Expr::MatchAgainst { .. } => return Err(projection_filter_error()),
        Expr::Function(function) => reject_unsupported_function(function)?,
        Expr::CompoundFieldAccess { root, access_chain } => {
            reject_unsupported_expr(root)?;
            for access in access_chain {
                reject_unsupported_access_expr(access)?;
            }
        }
        Expr::JsonAccess { value, .. }
        | Expr::IsFalse(value)
        | Expr::IsNotFalse(value)
        | Expr::IsTrue(value)
        | Expr::IsNotTrue(value)
        | Expr::IsNull(value)
        | Expr::IsNotNull(value)
        | Expr::IsUnknown(value)
        | Expr::IsNotUnknown(value)
        | Expr::Nested(value)
        | Expr::OuterJoin(value)
        | Expr::Prior(value) => {
            reject_unsupported_expr(value)?;
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            reject_unsupported_expr(left)?;
            reject_unsupported_expr(right)?;
        }
        Expr::IsNormalized { expr, .. } | Expr::UnaryOp { expr, .. } => {
            reject_unsupported_expr(expr)?;
        }
        Expr::InList { expr, list, .. } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_exprs(list)?;
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_expr(array_expr)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_expr(low)?;
            reject_unsupported_expr(high)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            reject_unsupported_expr(left)?;
            reject_unsupported_expr(right)?;
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_expr(pattern)?;
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            reject_unsupported_expr(left)?;
            reject_unsupported_expr(right)?;
        }
        Expr::Convert { expr, styles, .. } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_exprs(styles)?;
        }
        Expr::Cast { expr, .. } => reject_unsupported_expr(expr)?,
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            reject_unsupported_expr(timestamp)?;
            reject_unsupported_expr(time_zone)?;
        }
        Expr::Extract { expr, .. } => reject_unsupported_expr(expr)?,
        Expr::Ceil { expr, .. } | Expr::Floor { expr, .. } => reject_unsupported_expr(expr)?,
        Expr::Position { expr, r#in } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_expr(r#in)?;
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            reject_unsupported_expr(expr)?;
            if let Some(substring_from) = substring_from {
                reject_unsupported_expr(substring_from)?;
            }
            if let Some(substring_for) = substring_for {
                reject_unsupported_expr(substring_for)?;
            }
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            reject_unsupported_expr(expr)?;
            if let Some(trim_what) = trim_what {
                reject_unsupported_expr(trim_what)?;
            }
            if let Some(trim_characters) = trim_characters {
                reject_unsupported_exprs(trim_characters)?;
            }
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            reject_unsupported_expr(expr)?;
            reject_unsupported_expr(overlay_what)?;
            reject_unsupported_expr(overlay_from)?;
            if let Some(overlay_for) = overlay_for {
                reject_unsupported_expr(overlay_for)?;
            }
        }
        Expr::Collate { expr, .. } | Expr::Prefixed { value: expr, .. } => {
            reject_unsupported_expr(expr)?;
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                reject_unsupported_expr(operand)?;
            }
            for condition in conditions {
                reject_unsupported_expr(&condition.condition)?;
                reject_unsupported_expr(&condition.result)?;
            }
            if let Some(else_result) = else_result {
                reject_unsupported_expr(else_result)?;
            }
        }
        Expr::Tuple(values) | Expr::Array(sqlparser::ast::Array { elem: values, .. }) => {
            reject_unsupported_exprs(values)?;
        }
        Expr::Struct { values, .. } => reject_unsupported_exprs(values)?,
        Expr::Named { expr, .. } => reject_unsupported_expr(expr)?,
        Expr::Dictionary(fields) => {
            for field in fields {
                reject_unsupported_expr(&field.value)?;
            }
        }
        Expr::Map(map) => {
            for entry in &map.entries {
                reject_unsupported_expr(&entry.key)?;
                reject_unsupported_expr(&entry.value)?;
            }
        }
        Expr::Interval(interval) => reject_unsupported_expr(&interval.value)?,
        Expr::Lambda(lambda) => reject_unsupported_expr(&lambda.body)?,
        Expr::MemberOf(member_of) => {
            reject_unsupported_expr(&member_of.value)?;
            reject_unsupported_expr(&member_of.array)?;
        }
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::TypedString(_)
        | Expr::Wildcard(_)
        | Expr::QualifiedWildcard(_, _) => {}
    }
    Ok(())
}

fn reject_unsupported_exprs(exprs: &[sqlparser::ast::Expr]) -> Result<(), String> {
    for expr in exprs {
        reject_unsupported_expr(expr)?;
    }
    Ok(())
}

fn reject_unsupported_access_expr(access: &sqlparser::ast::AccessExpr) -> Result<(), String> {
    match access {
        sqlparser::ast::AccessExpr::Dot(expr) => reject_unsupported_expr(expr),
        sqlparser::ast::AccessExpr::Subscript(subscript) => match subscript {
            sqlparser::ast::Subscript::Index { index } => reject_unsupported_expr(index),
            sqlparser::ast::Subscript::Slice {
                lower_bound,
                upper_bound,
                stride,
            } => {
                if let Some(lower_bound) = lower_bound {
                    reject_unsupported_expr(lower_bound)?;
                }
                if let Some(upper_bound) = upper_bound {
                    reject_unsupported_expr(upper_bound)?;
                }
                if let Some(stride) = stride {
                    reject_unsupported_expr(stride)?;
                }
                Ok(())
            }
        },
    }
}

fn reject_unsupported_function(function: &sqlparser::ast::Function) -> Result<(), String> {
    let function_name = function.name.to_string().to_ascii_lowercase();
    if is_non_deterministic_function(&function_name, &function.args) {
        return Err(
            "incremental MV projection/filter query contains non-deterministic function"
                .to_string(),
        );
    }
    if is_aggregate_function(&function_name)
        || is_window_only_function(&function_name)
        || is_grouping_function(&function_name)
        || is_unsafe_scalar_function(&function_name)
        || function.uses_odbc_syntax
        || function.null_treatment.is_some()
        || function.over.is_some()
    {
        return Err(projection_filter_error());
    }
    if function.within_group.is_empty()
        && function.filter.is_none()
        && matches!(function.parameters, sqlparser::ast::FunctionArguments::None)
    {
        reject_unsupported_function_arguments(&function.args)?;
        return Ok(());
    }

    if let Some(filter) = &function.filter {
        reject_unsupported_expr(filter)?;
    }
    for order_by in &function.within_group {
        reject_unsupported_expr(&order_by.expr)?;
    }
    reject_unsupported_function_arguments(&function.parameters)?;
    reject_unsupported_function_arguments(&function.args)
}

fn reject_unsupported_function_arguments(
    args: &sqlparser::ast::FunctionArguments,
) -> Result<(), String> {
    match args {
        sqlparser::ast::FunctionArguments::None => Ok(()),
        sqlparser::ast::FunctionArguments::Subquery(_) => Err(projection_filter_error()),
        sqlparser::ast::FunctionArguments::List(list) => {
            if list.duplicate_treatment.is_some() {
                return Err(projection_filter_error());
            }
            if !list.clauses.is_empty() {
                return Err(projection_filter_error());
            }
            for arg in &list.args {
                reject_unsupported_function_arg(arg)?;
            }
            Ok(())
        }
    }
}

fn reject_unsupported_function_arg(arg: &sqlparser::ast::FunctionArg) -> Result<(), String> {
    match arg {
        sqlparser::ast::FunctionArg::Named { arg, .. }
        | sqlparser::ast::FunctionArg::ExprNamed { arg, .. }
        | sqlparser::ast::FunctionArg::Unnamed(arg) => match arg {
            sqlparser::ast::FunctionArgExpr::Expr(expr) => reject_unsupported_expr(expr),
            sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_)
            | sqlparser::ast::FunctionArgExpr::Wildcard => Ok(()),
        },
    }
}

fn is_non_deterministic_function(name: &str, args: &sqlparser::ast::FunctionArguments) -> bool {
    matches!(
        name,
        "now"
            | "current_timestamp"
            | "localtime"
            | "localtimestamp"
            | "utc_timestamp"
            | "current_date"
            | "curdate"
            | "current_time"
            | "curtime"
            | "utc_time"
            | "random"
            | "rand"
            | "uuid"
    ) || (name == "unix_timestamp" && function_argument_count(args) == Some(0))
}

fn function_argument_count(args: &sqlparser::ast::FunctionArguments) -> Option<usize> {
    match args {
        sqlparser::ast::FunctionArguments::None => Some(0),
        sqlparser::ast::FunctionArguments::List(list) => Some(list.args.len()),
        sqlparser::ast::FunctionArguments::Subquery(_) => None,
    }
}

fn is_window_only_function(name: &str) -> bool {
    // Keep in sync with sql::analyzer::functions::is_window_only_function.
    matches!(
        name,
        "row_number"
            | "rank"
            | "dense_rank"
            | "cume_dist"
            | "percent_rank"
            | "ntile"
            | "lag"
            | "lead"
            | "first_value"
            | "last_value"
            | "session_number"
    )
}

fn is_grouping_function(name: &str) -> bool {
    matches!(name, "grouping" | "grouping_id")
}

fn is_unsafe_scalar_function(name: &str) -> bool {
    matches!(
        name,
        "sleep" | "version" | "database" | "current_user" | "user"
    )
}

fn is_aggregate_function(name: &str) -> bool {
    // Keep in sync with sql::analyzer::functions::is_aggregate_function and
    // exec::expr::agg::functions::resolve_by_func aliases.
    matches!(
        name,
        "sum"
            | "count"
            | "count_distinct"
            | "avg"
            | "min"
            | "max"
            | "count_if"
            | "any_value"
            | "array_agg"
            | "group_concat"
            | "string_agg"
            | "bitmap_agg"
            | "bitmap_union"
            | "bitmap_union_count"
            | "bitmap_union_int"
            | "multi_distinct_count"
            | "array_agg_distinct"
            | "array_unique_agg"
            | "sum_map"
            | "map_agg"
            | "percentile_approx"
            | "percentile_approx_weighted"
            | "percentile_cont"
            | "percentile_disc"
            | "percentile_disc_lc"
            | "percentile_union"
            | "approx_count_distinct"
            | "approx_count_distinct_hll_sketch"
            | "approx_top_k"
            | "ds_hll_accumulate"
            | "ds_hll_combine"
            | "ds_hll_estimate"
            | "ds_hll_count_distinct"
            | "ds_hll_count_distinct_union"
            | "ds_hll_count_distinct_merge"
            | "hll_union"
            | "hll_union_agg"
            | "hll_raw_agg"
            | "hll_raw"
            | "hll_cardinality"
            | "ndv"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "variance"
            | "variance_samp"
            | "variance_pop"
            | "var_samp"
            | "var_pop"
            | "std"
            | "covar_samp"
            | "covar_pop"
            | "corr"
            | "max_by"
            | "min_by"
            | "max_by_v2"
            | "min_by_v2"
            | "multi_distinct_sum"
            | "retention"
            | "window_funnel"
            | "histogram"
            | "histogram_hll_ndv"
            | "mann_whitney_u_test"
            | "dict_merge"
            | "ds_theta_count_distinct"
            | "bool_or"
            | "bool_and"
            | "boolor_agg"
            | "booland_agg"
            | "every"
            | "min_n"
            | "max_n"
    )
}

fn is_empty_group_by(group_by: &sqlparser::ast::GroupByExpr) -> bool {
    match group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, modifiers) => {
            exprs.is_empty() && modifiers.is_empty()
        }
        sqlparser::ast::GroupByExpr::All(_) => false,
    }
}

fn is_three_part_object_name(name: &sqlparser::ast::ObjectName) -> bool {
    name.0.len() == 3
        && name
            .0
            .iter()
            .all(|part| matches!(part, sqlparser::ast::ObjectNamePart::Identifier(_)))
}

fn single_base_table_error() -> String {
    "incremental MV query must reference a single Iceberg base table".to_string()
}

fn projection_filter_error() -> String {
    "incremental MV query must be a projection/filter SELECT".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("not a query: {stmt:?}");
        };
        *query
    }

    fn classify_sql(sql: &str) -> Result<IncrementalMvShape, String> {
        let query = parse_query(sql);
        classify_incremental_mv_query(&query)
    }

    fn assert_rejects_with(sql: &str, needle: &str) {
        let err = classify_sql(sql).expect_err("query should be rejected");
        assert!(
            err.contains(needle),
            "expected error to contain `{needle}` for `{sql}`, got `{err}`"
        );
    }

    #[test]
    fn accepts_single_table_projection_filter() {
        let shape = classify_sql("select k1, v2 + 1 as v3 from ice.ns.orders where v2 > 10")
            .expect("query should be accepted");
        assert_eq!(shape.base_table.to_string(), "ice.ns.orders");
    }

    #[test]
    fn accepts_projection_filter_string_literals_containing_keywords() {
        classify_sql("select 'select' from ice.ns.orders").expect("query should be accepted");
        classify_sql("select k1 from ice.ns.orders where k1 = 'over'")
            .expect("query should be accepted");
    }

    #[test]
    fn rejects_multi_table_join() {
        assert_rejects_with(
            "select o.k1 from ice.ns.orders o join ice.ns.items i on o.k1 = i.k1",
            "single Iceberg base table",
        );
    }

    #[test]
    fn rejects_aggregation() {
        assert_rejects_with(
            "select k1, sum(v2) from ice.ns.orders group by k1",
            "projection/filter",
        );
        assert_rejects_with("select stddev(v2) from ice.ns.orders", "projection/filter");
        assert_rejects_with(
            "select array_agg(k1) from ice.ns.orders",
            "projection/filter",
        );
        for sql in [
            "select approx_count_distinct(k1) from ice.ns.orders",
            "select bitmap_union(k1) from ice.ns.orders",
            "select count_distinct(k1) from ice.ns.orders",
            "select hll_union(k1) from ice.ns.orders",
            "select percentile_approx(v2, 0.5) from ice.ns.orders",
            "select max_by_v2(k1, v2) from ice.ns.orders",
            "select multi_distinct_sum(v2) from ice.ns.orders",
        ] {
            assert_rejects_with(sql, "projection/filter");
        }
    }

    #[test]
    fn rejects_group_by_all() {
        assert_rejects_with(
            "select k1 from ice.ns.orders group by all",
            "projection/filter",
        );
    }

    #[test]
    fn rejects_distinct_window_limit_and_subquery() {
        assert_rejects_with("select distinct k1 from ice.ns.orders", "projection/filter");
        assert_rejects_with(
            "select k1, row_number() over (partition by k1) from ice.ns.orders",
            "projection/filter",
        );
        for sql in [
            "select row_number() from ice.ns.orders",
            "select rank() from ice.ns.orders",
            "select dense_rank() from ice.ns.orders",
            "select cume_dist() from ice.ns.orders",
            "select percent_rank() from ice.ns.orders",
            "select ntile(4) from ice.ns.orders",
            "select lag(k1) from ice.ns.orders",
            "select lead(k1) from ice.ns.orders",
            "select first_value(k1) from ice.ns.orders",
            "select last_value(k1) from ice.ns.orders",
            "select session_number() from ice.ns.orders",
        ] {
            assert_rejects_with(sql, "projection/filter");
        }
        assert_rejects_with("select k1 from ice.ns.orders limit 1", "projection/filter");
        assert_rejects_with(
            "select k1 from (select k1 from ice.ns.orders) t",
            "projection/filter",
        );
    }

    #[test]
    fn rejects_grouping_functions() {
        assert_rejects_with(
            "select grouping(k1) from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select grouping_id(k1) from ice.ns.orders",
            "projection/filter",
        );
    }

    #[test]
    fn rejects_unsafe_scalar_functions() {
        for sql in [
            "select sleep(1) from ice.ns.orders",
            "select current_user() from ice.ns.orders",
            "select database() from ice.ns.orders",
            "select version() from ice.ns.orders",
            "select user() from ice.ns.orders",
        ] {
            assert_rejects_with(sql, "projection/filter");
        }
    }

    #[test]
    fn rejects_unsupported_function_arguments_and_match_against() {
        assert_rejects_with(
            "select abs(distinct v2) from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select abs(k1) ignore nulls from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select {fn abs(k1)} from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select lower(k1 order by v2) from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select lower(k1 limit 1) from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with(
            "select match(k1) against ('x') from ice.ns.orders",
            "projection/filter",
        );
    }

    #[test]
    fn rejects_non_deterministic_now() {
        assert_rejects_with("select k1, now() from ice.ns.orders", "non-deterministic");
        assert_rejects_with(
            "select k1, current_timestamp from ice.ns.orders",
            "non-deterministic",
        );
        for sql in [
            "select current_date from ice.ns.orders",
            "select current_time from ice.ns.orders",
            "select curtime() from ice.ns.orders",
            "select localtime from ice.ns.orders",
            "select localtimestamp from ice.ns.orders",
            "select utc_time() from ice.ns.orders",
            "select utc_timestamp() from ice.ns.orders",
            "select unix_timestamp() from ice.ns.orders",
        ] {
            assert_rejects_with(sql, "non-deterministic");
        }
    }

    #[test]
    fn rejects_non_deterministic_is_distinct_from_rhs() {
        assert_rejects_with(
            "select k1 from ice.ns.orders where k1 is distinct from now()",
            "non-deterministic",
        );
        assert_rejects_with(
            "select k1 from ice.ns.orders where k1 is not distinct from current_timestamp",
            "non-deterministic",
        );
    }

    #[test]
    fn accepts_unix_timestamp_with_argument() {
        classify_sql("select unix_timestamp(k1) from ice.ns.orders")
            .expect("query should be accepted");
    }
}
