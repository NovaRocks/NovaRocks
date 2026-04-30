#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IncrementalMvShape {
    ProjectionFilter(ProjectionFilterMvShape),
    Aggregate(AggregateMvShape),
}

impl IncrementalMvShape {
    pub(crate) fn base_table(&self) -> &sqlparser::ast::ObjectName {
        match self {
            IncrementalMvShape::ProjectionFilter(shape) => &shape.base_table,
            IncrementalMvShape::Aggregate(shape) => &shape.base_table,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionFilterMvShape {
    pub(crate) base_table: sqlparser::ast::ObjectName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateMvShape {
    pub(crate) base_table: sqlparser::ast::ObjectName,
    pub(crate) group_keys: Vec<GroupKeyShape>,
    pub(crate) aggregates: Vec<AggregateCallShape>,
    pub(crate) visible_outputs: Vec<VisibleAggregateOutput>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupKeyShape {
    pub(crate) output_name: String,
    pub(crate) expr: sqlparser::ast::Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateCallShape {
    pub(crate) output_name: String,
    pub(crate) function: AggregateFunctionKind,
    pub(crate) input: AggregateInput,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateFunctionKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AggregateInput {
    Star,
    Expr(Box<sqlparser::ast::Expr>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum VisibleAggregateOutput {
    GroupKey(usize),
    Aggregate(usize),
}

pub(crate) fn classify_incremental_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<IncrementalMvShape, String> {
    match classify_aggregate_mv_query(query) {
        Ok(shape) => return Ok(IncrementalMvShape::Aggregate(shape)),
        Err(err) if is_probably_aggregate_query(query) => return Err(err),
        Err(_) => {}
    }

    classify_projection_filter_mv_query(query).map(IncrementalMvShape::ProjectionFilter)
}

fn classify_projection_filter_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<ProjectionFilterMvShape, String> {
    reject_unsupported_query_clauses(query)?;

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(projection_filter_error());
    };
    reject_unsupported_select_clauses(select)?;
    reject_match_against_before_from_shape_check(select)?;

    let base_table =
        extract_single_base_table(select, projection_filter_error, single_base_table_error)?;
    reject_unsupported_projection_filter_exprs(select)?;

    Ok(ProjectionFilterMvShape { base_table })
}

fn classify_aggregate_mv_query(query: &sqlparser::ast::Query) -> Result<AggregateMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| aggregate_error())?;

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(aggregate_error());
    };
    reject_unsupported_aggregate_select_clauses(select)?;

    let base_table = extract_single_base_table(select, aggregate_error, aggregate_error)?;
    if let Some(selection) = &select.selection {
        reject_unsupported_expr(selection).map_err(aggregate_expr_error)?;
    }

    let group_by_exprs = aggregate_group_by_exprs(&select.group_by)?;
    for expr in group_by_exprs {
        reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    }

    let mut group_keys = group_by_exprs
        .iter()
        .cloned()
        .map(|expr| GroupKeyShape {
            output_name: String::new(),
            expr,
        })
        .collect::<Vec<_>>();
    let mut aggregates = Vec::new();
    let mut visible_outputs = Vec::with_capacity(select.projection.len());
    let mut projected_group_keys = vec![false; group_keys.len()];

    for item in &select.projection {
        let (expr, output_name) = projection_expr_and_output_name(item)?;
        if let Some(group_key_index) = group_keys
            .iter()
            .position(|group_key| group_key.expr == *expr)
        {
            if group_keys[group_key_index].output_name.is_empty() {
                group_keys[group_key_index].output_name = output_name;
            }
            projected_group_keys[group_key_index] = true;
            visible_outputs.push(VisibleAggregateOutput::GroupKey(group_key_index));
            continue;
        }

        let aggregate = classify_aggregate_call(expr, output_name)?;
        let aggregate_index = aggregates.len();
        aggregates.push(aggregate);
        visible_outputs.push(VisibleAggregateOutput::Aggregate(aggregate_index));
    }

    if projected_group_keys.iter().any(|projected| !projected) {
        return Err(
            "incremental aggregate MV projection must include every GROUP BY key".to_string(),
        );
    }
    if aggregates.is_empty() {
        return Err("incremental aggregate MV requires at least one aggregate output".to_string());
    }

    Ok(AggregateMvShape {
        base_table,
        group_keys,
        aggregates,
        visible_outputs,
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

fn reject_unsupported_aggregate_select_clauses(
    select: &sqlparser::ast::Select,
) -> Result<(), String> {
    if select.optimizer_hint.is_some()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(aggregate_error());
    }
    Ok(())
}

fn extract_single_base_table(
    select: &sqlparser::ast::Select,
    shape_error: fn() -> String,
    single_table_error: fn() -> String,
) -> Result<sqlparser::ast::ObjectName, String> {
    let [from] = select.from.as_slice() else {
        return Err(single_table_error());
    };
    if !from.joins.is_empty() {
        return Err(single_table_error());
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
        return Err(shape_error());
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
        return Err(single_table_error());
    }
    if !is_three_part_object_name(name) {
        return Err(single_table_error());
    }
    Ok(name.clone())
}

fn aggregate_group_by_exprs(
    group_by: &sqlparser::ast::GroupByExpr,
) -> Result<&[sqlparser::ast::Expr], String> {
    match group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, modifiers) => {
            if exprs.is_empty() {
                return Err("incremental aggregate MV requires a non-empty GROUP BY".to_string());
            }
            if !modifiers.is_empty() {
                return Err("incremental aggregate MV does not support GROUP BY modifiers".to_string());
            }
            Ok(exprs)
        }
        sqlparser::ast::GroupByExpr::All(_) => Err(
            "incremental aggregate MV requires an explicit non-empty GROUP BY; GROUP BY ALL is unsupported"
                .to_string(),
        ),
    }
}

fn projection_expr_and_output_name(
    item: &sqlparser::ast::SelectItem,
) -> Result<(&sqlparser::ast::Expr, String), String> {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr) => Ok((expr, expr.to_string())),
        sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
            Ok((expr, alias.value.clone()))
        }
        sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
        | sqlparser::ast::SelectItem::Wildcard(_) => Err(
            "incremental aggregate MV projection can only contain expressions or aliases"
                .to_string(),
        ),
    }
}

fn classify_aggregate_call(
    expr: &sqlparser::ast::Expr,
    output_name: String,
) -> Result<AggregateCallShape, String> {
    let sqlparser::ast::Expr::Function(function) = expr else {
        return Err(
            "incremental aggregate MV scalar projection must be a GROUP BY key or aggregate call"
                .to_string(),
        );
    };
    if function.name.0.len() != 1
        || !matches!(
            function.name.0.first(),
            Some(sqlparser::ast::ObjectNamePart::Identifier(_))
        )
        || function.uses_odbc_syntax
        || function.null_treatment.is_some()
        || function.over.is_some()
        || function.filter.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, sqlparser::ast::FunctionArguments::None)
    {
        return Err(aggregate_error());
    }

    let sqlparser::ast::FunctionArguments::List(args) = &function.args else {
        return Err(aggregate_error());
    };
    if args.duplicate_treatment.is_some() || !args.clauses.is_empty() {
        return Err(aggregate_error());
    }

    let function_name = function.name.to_string().to_ascii_lowercase();
    let (function, input) = match function_name.as_str() {
        "count" => classify_count_input(&args.args)?,
        "sum" => (AggregateFunctionKind::Sum, classify_sum_input(&args.args)?),
        "avg" => (AggregateFunctionKind::Avg, classify_avg_input(&args.args)?),
        "min" => (
            AggregateFunctionKind::Min,
            classify_min_max_input(&args.args)?,
        ),
        "max" => (
            AggregateFunctionKind::Max,
            classify_min_max_input(&args.args)?,
        ),
        _ => return Err(aggregate_error()),
    };

    Ok(AggregateCallShape {
        output_name,
        function,
        input,
    })
}

fn classify_count_input(
    args: &[sqlparser::ast::FunctionArg],
) -> Result<(AggregateFunctionKind, AggregateInput), String> {
    let [arg] = args else {
        return Err(aggregate_error());
    };
    match simple_aggregate_arg_expr(arg)? {
        sqlparser::ast::FunctionArgExpr::Wildcard => {
            Ok((AggregateFunctionKind::Count, AggregateInput::Star))
        }
        sqlparser::ast::FunctionArgExpr::Expr(expr) => {
            reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
            Ok((
                AggregateFunctionKind::Count,
                AggregateInput::Expr(Box::new(expr.clone())),
            ))
        }
        sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => Err(aggregate_error()),
    }
}

fn classify_sum_input(args: &[sqlparser::ast::FunctionArg]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err(aggregate_error());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err(aggregate_error());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}

fn classify_avg_input(args: &[sqlparser::ast::FunctionArg]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err("AVG aggregate requires a column expression argument".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("AVG aggregate requires a column expression argument".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}

fn classify_min_max_input(args: &[sqlparser::ast::FunctionArg]) -> Result<AggregateInput, String> {
    let [arg] = args else {
        return Err("MIN/MAX aggregate requires a column expression argument".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("MIN/MAX aggregate requires a column expression argument".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}

fn simple_aggregate_arg_expr(
    arg: &sqlparser::ast::FunctionArg,
) -> Result<&sqlparser::ast::FunctionArgExpr, String> {
    match arg {
        sqlparser::ast::FunctionArg::Unnamed(arg) => Ok(arg),
        sqlparser::ast::FunctionArg::Named { .. }
        | sqlparser::ast::FunctionArg::ExprNamed { .. } => Err(aggregate_error()),
    }
}

fn is_probably_aggregate_query(query: &sqlparser::ast::Query) -> bool {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    !is_empty_group_by(&select.group_by)
        || select.having.is_some()
        || select
            .projection
            .iter()
            .any(select_item_contains_aggregate_function)
}

fn select_item_contains_aggregate_function(item: &sqlparser::ast::SelectItem) -> bool {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
            expr_contains_aggregate_function(expr)
        }
        sqlparser::ast::SelectItem::QualifiedWildcard(
            sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
            _,
        ) => expr_contains_aggregate_function(expr),
        sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
        | sqlparser::ast::SelectItem::Wildcard(_) => false,
    }
}

fn expr_contains_aggregate_function(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Function(function) => {
            let name = function.name.to_string().to_ascii_lowercase();
            is_aggregate_function(&name)
                || function_args_contain_aggregate_function(&function.parameters)
                || function_args_contain_aggregate_function(&function.args)
                || function
                    .filter
                    .as_ref()
                    .is_some_and(|filter| expr_contains_aggregate_function(filter))
                || function
                    .within_group
                    .iter()
                    .any(|order_by| expr_contains_aggregate_function(&order_by.expr))
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::AnyOp { left, right, .. }
        | Expr::AllOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_aggregate_function(left) || expr_contains_aggregate_function(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNormalized { expr, .. }
        | Expr::Nested(expr)
        | Expr::OuterJoin(expr)
        | Expr::Prior(expr)
        | Expr::Cast { expr, .. }
        | Expr::Extract { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::Prefixed { value: expr, .. }
        | Expr::Named { expr, .. } => expr_contains_aggregate_function(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate_function(expr)
                || list.iter().any(expr_contains_aggregate_function)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate_function(expr)
                || expr_contains_aggregate_function(low)
                || expr_contains_aggregate_function(high)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|operand| expr_contains_aggregate_function(operand))
                || conditions.iter().any(|condition| {
                    expr_contains_aggregate_function(&condition.condition)
                        || expr_contains_aggregate_function(&condition.result)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|else_result| expr_contains_aggregate_function(else_result))
        }
        Expr::Tuple(values)
        | Expr::Array(sqlparser::ast::Array { elem: values, .. })
        | Expr::Struct { values, .. } => values.iter().any(expr_contains_aggregate_function),
        _ => false,
    }
}

fn function_args_contain_aggregate_function(args: &sqlparser::ast::FunctionArguments) -> bool {
    match args {
        sqlparser::ast::FunctionArguments::None
        | sqlparser::ast::FunctionArguments::Subquery(_) => false,
        sqlparser::ast::FunctionArguments::List(list) => list.args.iter().any(|arg| match arg {
            sqlparser::ast::FunctionArg::Named { arg, .. }
            | sqlparser::ast::FunctionArg::ExprNamed { arg, .. }
            | sqlparser::ast::FunctionArg::Unnamed(arg) => match arg {
                sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                    expr_contains_aggregate_function(expr)
                }
                sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_)
                | sqlparser::ast::FunctionArgExpr::Wildcard => false,
            },
        }),
    }
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

fn aggregate_error() -> String {
    "incremental aggregate MV query must be a single-table SELECT with non-empty GROUP BY and only count/sum/avg/min/max aggregate outputs".to_string()
}

fn aggregate_expr_error(_err: String) -> String {
    "incremental aggregate MV query contains an unsupported expression".to_string()
}

/// Rewrite a MV SELECT SQL so that AVG aggregates are replaced by their SUM and COUNT
/// sub-states, producing a SELECT whose output columns map directly to the layout's state
/// columns rather than the user-visible aggregate results.
///
/// For each `AVG(expr) AS alias` projection item:
/// - Replace with `SUM(expr) AS __agg_state_<sanitized(alias)>__sum`
/// - Followed by  `COUNT(expr) AS __agg_state_<sanitized(alias)>__count`
///
/// COUNT, SUM, MIN, MAX projections are passed through unchanged (their visible == state).
/// Group keys and WHERE/HAVING clauses are unchanged.
///
/// The returned SQL string can be fed directly to the executor to produce a state-shaped
/// Arrow batch that `materialize_aggregate_result_chunks` can consume.
pub(crate) fn rewrite_select_sql_for_state(
    select_sql: &str,
    shape: &AggregateMvShape,
) -> Result<String, String> {
    use sqlparser::ast::{SelectItem, SetExpr, Statement};

    // Check if there are any AVG aggregates; if not, return the SQL unchanged.
    if shape
        .aggregates
        .iter()
        .all(|agg| agg.function != AggregateFunctionKind::Avg)
    {
        return Ok(select_sql.to_string());
    }

    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("rewrite_select_sql_for_state normalize error: {e}"))?;
    let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("rewrite_select_sql_for_state parse error: {e}"))?;
    let mut stmt = stmt;

    let Statement::Query(query) = &mut stmt else {
        return Err("rewrite_select_sql_for_state: expected Query statement".to_string());
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rewrite_select_sql_for_state: expected SELECT body".to_string());
    };

    let mut new_projection: Vec<SelectItem> = Vec::with_capacity(select.projection.len() + 4);
    for item in std::mem::take(&mut select.projection) {
        match extract_avg_expr_and_alias(&item) {
            Some((arg_expr, alias)) => {
                let sanitized =
                    crate::connector::starrocks::managed::mv_agg_state::sanitize_state_column_name(
                        &alias,
                    );
                let sum_alias = format!("__agg_state_{sanitized}__sum");
                let count_alias = format!("__agg_state_{sanitized}__count");

                new_projection.push(make_aggregate_select_item(
                    "SUM",
                    arg_expr.clone(),
                    &sum_alias,
                ));
                new_projection.push(make_aggregate_select_item("COUNT", arg_expr, &count_alias));
            }
            None => {
                new_projection.push(item);
            }
        }
    }
    select.projection = new_projection;

    Ok(stmt.to_string())
}

/// Returns `(arg_expr, alias)` if the select item is `AVG(expr) AS alias` or `AVG(expr)`.
/// Returns `None` for all other items.
fn extract_avg_expr_and_alias(
    item: &sqlparser::ast::SelectItem,
) -> Option<(sqlparser::ast::Expr, String)> {
    use sqlparser::ast::{Expr, SelectItem};

    let (expr, alias) = match item {
        SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
        SelectItem::UnnamedExpr(expr) => (expr, expr.to_string()),
        _ => return None,
    };

    let Expr::Function(func) = expr else {
        return None;
    };

    // Check that this is a plain `avg` call (no ODBC syntax, no window, etc.)
    if !is_plain_avg_function(func) {
        return None;
    }

    let arg_expr = extract_single_expr_arg(func)?;
    Some((arg_expr, alias))
}

fn is_plain_avg_function(func: &sqlparser::ast::Function) -> bool {
    let name = func.name.to_string().to_ascii_lowercase();
    name == "avg"
        && !func.uses_odbc_syntax
        && func.null_treatment.is_none()
        && func.over.is_none()
        && func.filter.is_none()
        && func.within_group.is_empty()
        && matches!(func.parameters, sqlparser::ast::FunctionArguments::None)
}

fn extract_single_expr_arg(func: &sqlparser::ast::Function) -> Option<sqlparser::ast::Expr> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };
    let [arg] = list.args.as_slice() else {
        return None;
    };
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr.clone()),
        _ => None,
    }
}

fn make_aggregate_select_item(
    func_name: &str,
    arg: sqlparser::ast::Expr,
    alias: &str,
) -> sqlparser::ast::SelectItem {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName, ObjectNamePart, SelectItem,
    };
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
        expr: sqlparser::ast::Expr::Function(function),
        alias: Ident::new(alias),
    }
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
        assert_eq!(shape.base_table().to_string(), "ice.ns.orders");
        let IncrementalMvShape::ProjectionFilter(shape) = shape else {
            panic!("expected projection/filter shape");
        };
        assert_eq!(shape.base_table.to_string(), "ice.ns.orders");
    }

    #[test]
    fn accepts_single_table_count_sum_group_by() {
        let shape = classify_sql(
            "select k1, count(*) as c, count(v2) as cv, sum(v2) as s \
             from ice.ns.orders where v2 > 0 group by k1",
        )
        .expect("query should be accepted");
        assert_eq!(shape.base_table().to_string(), "ice.ns.orders");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        assert_eq!(shape.base_table.to_string(), "ice.ns.orders");
        assert_eq!(shape.group_keys.len(), 1);
        assert_eq!(shape.group_keys[0].output_name, "k1");
        assert_eq!(shape.group_keys[0].expr.to_string(), "k1");
        assert_eq!(shape.aggregates.len(), 3);
        assert_eq!(shape.aggregates[0].output_name, "c");
        assert_eq!(shape.aggregates[0].function, AggregateFunctionKind::Count);
        assert_eq!(shape.aggregates[0].input, AggregateInput::Star);
        assert_eq!(shape.aggregates[1].output_name, "cv");
        assert_eq!(shape.aggregates[1].function, AggregateFunctionKind::Count);
        assert_eq!(
            shape.aggregates[1].input,
            AggregateInput::Expr(Box::new(sqlparser::ast::Expr::Identifier("v2".into())))
        );
        assert_eq!(shape.aggregates[2].output_name, "s");
        assert_eq!(shape.aggregates[2].function, AggregateFunctionKind::Sum);
        assert_eq!(
            shape.aggregates[2].input,
            AggregateInput::Expr(Box::new(sqlparser::ast::Expr::Identifier("v2".into())))
        );
        assert_eq!(
            shape.visible_outputs,
            vec![
                VisibleAggregateOutput::GroupKey(0),
                VisibleAggregateOutput::Aggregate(0),
                VisibleAggregateOutput::Aggregate(1),
                VisibleAggregateOutput::Aggregate(2),
            ]
        );
    }

    #[test]
    fn rejects_scalar_aggregate_without_group_by() {
        assert_rejects_with(
            "select count(*) as c from ice.ns.orders",
            "non-empty GROUP BY",
        );
    }

    #[test]
    fn rejects_unsupported_aggregate_functions() {
        for sql in [
            "select k1, count(distinct v2) from ice.ns.orders group by k1",
            "select k1, sum(v2) filter (where v2 > 0) from ice.ns.orders group by k1",
            "select k1, sum(v2 order by k1) from ice.ns.orders group by k1",
            "select k1, sum(v2) over (partition by k1) from ice.ns.orders group by k1",
        ] {
            assert_rejects_with(sql, "incremental aggregate MV");
        }
    }

    #[test]
    fn accepts_min_max_aggregates() {
        let shape =
            classify_sql("select k1, min(v2) as mn, max(v2) as mx from ice.ns.orders group by k1")
                .expect("query should be accepted");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        assert_eq!(shape.aggregates.len(), 2);
        assert_eq!(shape.aggregates[0].function, AggregateFunctionKind::Min);
        assert_eq!(shape.aggregates[1].function, AggregateFunctionKind::Max);
    }

    #[test]
    fn rejects_min_max_star() {
        assert_rejects_with(
            "select k1, min(*) from ice.ns.orders group by k1",
            "MIN/MAX aggregate requires a column expression argument",
        );
        assert_rejects_with(
            "select k1, max(*) from ice.ns.orders group by k1",
            "MIN/MAX aggregate requires a column expression argument",
        );
    }

    #[test]
    fn accepts_avg_aggregate() {
        let shape = classify_sql("select k1, avg(v2) as a from ice.ns.orders group by k1")
            .expect("query should be accepted");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        assert_eq!(shape.aggregates.len(), 1);
        assert_eq!(shape.aggregates[0].output_name, "a");
        assert_eq!(shape.aggregates[0].function, AggregateFunctionKind::Avg);
        assert_eq!(
            shape.aggregates[0].input,
            AggregateInput::Expr(Box::new(sqlparser::ast::Expr::Identifier("v2".into())))
        );
    }

    #[test]
    fn rejects_avg_star_and_avg_distinct() {
        assert_rejects_with(
            "select k1, avg(*) from ice.ns.orders group by k1",
            "AVG aggregate requires a column expression argument",
        );
        assert_rejects_with(
            "select k1, avg(distinct v2) from ice.ns.orders group by k1",
            "incremental aggregate MV",
        );
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
            "select stddev(v2) from ice.ns.orders",
            "incremental aggregate MV",
        );
        assert_rejects_with(
            "select array_agg(k1) from ice.ns.orders",
            "incremental aggregate MV",
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
            assert_rejects_with(sql, "incremental aggregate MV");
        }
    }

    #[test]
    fn rejects_group_by_all() {
        assert_rejects_with(
            "select k1 from ice.ns.orders group by all",
            "non-empty GROUP BY",
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

    fn as_aggregate_shape(shape: IncrementalMvShape) -> AggregateMvShape {
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        shape
    }

    #[test]
    fn rewrite_select_sql_avg_to_sum_count() {
        let original = "SELECT k1, COUNT(*) AS c, AVG(v2) AS a FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        // Must contain SUM and COUNT for the AVG column; exact spacing is flexible.
        assert!(
            rewritten.to_uppercase().contains("SUM(V2)"),
            "got: {rewritten}"
        );
        assert!(
            rewritten.to_uppercase().contains("COUNT(V2)"),
            "got: {rewritten}"
        );
        // Original AVG must be gone.
        assert!(
            !rewritten.to_uppercase().contains("AVG(V2)"),
            "got: {rewritten}"
        );
        // COUNT(*) for the original COUNT aggregate should be preserved.
        assert!(
            rewritten.to_uppercase().contains("COUNT(*)"),
            "got: {rewritten}"
        );
        // State aliases should be present.
        assert!(rewritten.contains("__agg_state_a__sum"), "got: {rewritten}");
        assert!(
            rewritten.contains("__agg_state_a__count"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_no_avg_is_passthrough() {
        let original = "SELECT k1, COUNT(*) AS c, SUM(v2) AS s FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        // No AVG → original SQL returned unchanged.
        assert_eq!(
            rewritten, original,
            "no-AVG case should return original SQL"
        );
    }

    #[test]
    fn rewrite_select_sql_avg_only() {
        let original = "SELECT k1, AVG(v2) AS a FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        assert!(
            rewritten.to_uppercase().contains("SUM(V2)"),
            "got: {rewritten}"
        );
        assert!(
            rewritten.to_uppercase().contains("COUNT(V2)"),
            "got: {rewritten}"
        );
        assert!(
            !rewritten.to_uppercase().contains("AVG"),
            "got: {rewritten}"
        );
        // Rewritten SQL must re-parse as a valid aggregate query.
        let re_shape = classify_sql(&rewritten).expect("re-classify rewritten");
        let IncrementalMvShape::Aggregate(_) = re_shape else {
            panic!("rewritten SQL should be aggregate shape");
        };
    }

    #[test]
    fn rewrite_select_sql_multiple_avg() {
        let original = "SELECT k1, AVG(v2) AS a1, AVG(v3) AS a2 FROM ice.ns.orders GROUP BY k1";
        let shape = as_aggregate_shape(classify_sql(original).expect("classify"));
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        // Both AVGs replaced.
        assert!(
            !rewritten.to_uppercase().contains("AVG"),
            "got: {rewritten}"
        );
        assert!(
            rewritten.contains("__agg_state_a1__sum"),
            "got: {rewritten}"
        );
        assert!(
            rewritten.contains("__agg_state_a2__sum"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_avg_without_alias() {
        let original = "SELECT k1, AVG(v2) FROM ice.ns.orders GROUP BY k1";
        let shape = match classify_sql(original).expect("classify") {
            IncrementalMvShape::Aggregate(s) => s,
            _ => panic!("expected aggregate shape"),
        };
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        assert!(upper.contains("SUM(V2)"), "got: {rewritten}");
        assert!(upper.contains("COUNT(V2)"), "got: {rewritten}");
        assert!(!upper.contains("AVG(V2)"), "got: {rewritten}");
        // For an unaliased AVG(v2), the alias is derived from expr.to_string() which
        // sqlparser renders as "AVG(v2)". After sanitize_state_column_name that becomes
        // "avg_v2_" (parentheses are replaced with underscores, letters lowercased).
        assert!(
            rewritten.contains("__agg_state_avg_v2___sum"),
            "state sum alias not found; got: {rewritten}"
        );
        assert!(
            rewritten.contains("__agg_state_avg_v2___count"),
            "state count alias not found; got: {rewritten}"
        );
    }

    #[test]
    fn rewrite_select_sql_avg_with_complex_argument() {
        let original = "SELECT k1, AVG(v2 + 1) AS a FROM ice.ns.orders GROUP BY k1";
        let shape = match classify_sql(original).expect("classify") {
            IncrementalMvShape::Aggregate(s) => s,
            _ => panic!("expected aggregate shape"),
        };
        let rewritten = rewrite_select_sql_for_state(original, &shape).expect("rewrite");
        let upper = rewritten.to_uppercase();
        // The complex argument must be preserved inside both SUM and COUNT.
        assert!(
            upper.contains("SUM(V2 + 1)") || upper.contains("SUM(V2+1)"),
            "got: {rewritten}"
        );
        assert!(
            upper.contains("COUNT(V2 + 1)") || upper.contains("COUNT(V2+1)"),
            "got: {rewritten}"
        );
        assert!(!upper.contains("AVG(V2 + 1)"), "got: {rewritten}");
    }
}
