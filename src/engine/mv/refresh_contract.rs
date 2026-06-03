use crate::connector::starrocks::table::model::IcebergTableRef;
use crate::sql::analysis::{
    BinOp, ExprKind, JoinKind, QueryBody, Relation, ResolvedQuery, ResolvedSelect, ResolvedSetOp,
    SetOpKind, TypedExpr,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshStrategy {
    ProjectionFilter,
    JoinProjectionFilter,
    UnionProjectionFilter,
    SingleAggregate,
    FanInAggregate,
    JoinAggregate,
    UnsupportedBranchUnionAggregate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RewriteEvidence {
    None,
    Aggregate,
    JoinAggregate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ApplyKeyContract {
    pub(crate) column_name: &'static str,
    pub(crate) value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType,
    pub(crate) rewrite_evidence: RewriteEvidence,
    pub(crate) allow_full_rebuild_on_policy_full_refresh: bool,
    pub(crate) preload_locator_for_change_stream_deletes: bool,
}

impl ApplyKeyContract {
    pub(crate) fn projection_filter() -> Self {
        Self {
            column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
            value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Int64,
            rewrite_evidence: RewriteEvidence::None,
            allow_full_rebuild_on_policy_full_refresh: true,
            preload_locator_for_change_stream_deletes: false,
        }
    }

    pub(crate) fn union_projection_filter() -> Self {
        Self {
            column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
            value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::BranchInt64,
            rewrite_evidence: RewriteEvidence::None,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: false,
        }
    }

    pub(crate) fn join_projection_filter() -> Self {
        Self {
            column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
            value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Utf8,
            rewrite_evidence: RewriteEvidence::None,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: false,
        }
    }

    pub(crate) fn aggregate_group_row() -> Self {
        Self {
            column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
            value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Utf8,
            rewrite_evidence: RewriteEvidence::Aggregate,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: true,
        }
    }

    pub(crate) fn join_aggregate_group_row() -> Self {
        Self {
            column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
            value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Utf8,
            rewrite_evidence: RewriteEvidence::JoinAggregate,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvRefreshContract {
    pub(crate) strategy: RefreshStrategy,
    pub(crate) base_refs: Vec<IcebergTableRef>,
    pub(crate) apply_key: ApplyKeyContract,
    pub(crate) aggregate: Option<AggregateRefreshContract>,
    pub(crate) join: Option<JoinRefreshContract>,
    pub(crate) branch: Option<BranchRefreshContract>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AggregateRefreshContract {
    pub(crate) group_key_count: usize,
    pub(crate) aggregate_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JoinRefreshContract {
    pub(crate) join_key_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BranchRefreshContract {
    pub(crate) branch_count: usize,
}

pub(crate) fn derive_imv_refresh_contract(
    analysis: &crate::connector::starrocks::table::mv_ddl::MvAnalysis,
) -> Result<ImvRefreshContract, String> {
    let base_refs = analysis
        .resolved_refs
        .iter()
        .map(iceberg_ref_from_resolved)
        .collect::<Result<Vec<_>, _>>()?;
    let derived = derive_from_query(&analysis.resolved_query)?;
    validate_base_ref_contract(&derived, &base_refs)?;
    Ok(derived.into_contract(base_refs))
}

fn validate_base_ref_contract(
    derived: &DerivedStructure,
    base_refs: &[IcebergTableRef],
) -> Result<(), String> {
    let expected = match derived {
        DerivedStructure::ProjectionFilter | DerivedStructure::SingleAggregate { .. } => 1,
        DerivedStructure::JoinProjection { .. } | DerivedStructure::JoinAggregate { .. } => 2,
        DerivedStructure::UnionProjection { branch_count }
        | DerivedStructure::FanInAggregate { branch_count, .. }
        | DerivedStructure::BranchUnionAggregate { branch_count, .. } => *branch_count,
    };
    if base_refs.len() != expected {
        return Err(format!(
            "Iceberg IMV refresh contract requires {expected} distinct Iceberg base table refs for {derived:?}, got {}",
            base_refs.len()
        ));
    }
    Ok(())
}

fn iceberg_ref_from_resolved(
    resolved: &crate::connector::starrocks::table::mv_ddl::ResolvedTableRef,
) -> Result<IcebergTableRef, String> {
    match resolved {
        crate::connector::starrocks::table::mv_ddl::ResolvedTableRef::Iceberg {
            catalog,
            namespace,
            table,
        } => Ok(IcebergTableRef {
            catalog: catalog.clone(),
            namespace: namespace.clone(),
            table: table.clone(),
        }),
        crate::connector::starrocks::table::mv_ddl::ResolvedTableRef::StarRocks {
            database,
            table,
        } => Err(format!(
            "Iceberg IMV refresh contract requires Iceberg base tables, got StarRocks table {database}.{table}"
        )),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DerivedStructure {
    ProjectionFilter,
    JoinProjection {
        join_key_count: usize,
    },
    UnionProjection {
        branch_count: usize,
    },
    SingleAggregate {
        group_key_count: usize,
        aggregate_count: usize,
    },
    FanInAggregate {
        branch_count: usize,
        group_key_count: usize,
        aggregate_count: usize,
    },
    JoinAggregate {
        join_key_count: usize,
        group_key_count: usize,
        aggregate_count: usize,
    },
    BranchUnionAggregate {
        branch_count: usize,
        group_key_count: usize,
        aggregate_count: usize,
    },
}

impl DerivedStructure {
    fn into_contract(self, base_refs: Vec<IcebergTableRef>) -> ImvRefreshContract {
        match self {
            Self::ProjectionFilter => ImvRefreshContract {
                strategy: RefreshStrategy::ProjectionFilter,
                base_refs,
                apply_key: ApplyKeyContract::projection_filter(),
                aggregate: None,
                join: None,
                branch: None,
            },
            Self::JoinProjection { join_key_count } => ImvRefreshContract {
                strategy: RefreshStrategy::JoinProjectionFilter,
                base_refs,
                apply_key: ApplyKeyContract::join_projection_filter(),
                aggregate: None,
                join: Some(JoinRefreshContract { join_key_count }),
                branch: None,
            },
            Self::UnionProjection { branch_count } => ImvRefreshContract {
                strategy: RefreshStrategy::UnionProjectionFilter,
                base_refs,
                apply_key: ApplyKeyContract::union_projection_filter(),
                aggregate: None,
                join: None,
                branch: Some(BranchRefreshContract { branch_count }),
            },
            Self::SingleAggregate {
                group_key_count,
                aggregate_count,
            } => ImvRefreshContract {
                strategy: RefreshStrategy::SingleAggregate,
                base_refs,
                apply_key: ApplyKeyContract::aggregate_group_row(),
                aggregate: Some(AggregateRefreshContract {
                    group_key_count,
                    aggregate_count,
                }),
                join: None,
                branch: None,
            },
            Self::FanInAggregate {
                branch_count,
                group_key_count,
                aggregate_count,
            } => ImvRefreshContract {
                strategy: RefreshStrategy::FanInAggregate,
                base_refs,
                apply_key: ApplyKeyContract::aggregate_group_row(),
                aggregate: Some(AggregateRefreshContract {
                    group_key_count,
                    aggregate_count,
                }),
                join: None,
                branch: Some(BranchRefreshContract { branch_count }),
            },
            Self::JoinAggregate {
                join_key_count,
                group_key_count,
                aggregate_count,
            } => ImvRefreshContract {
                strategy: RefreshStrategy::JoinAggregate,
                base_refs,
                apply_key: ApplyKeyContract::join_aggregate_group_row(),
                aggregate: Some(AggregateRefreshContract {
                    group_key_count,
                    aggregate_count,
                }),
                join: Some(JoinRefreshContract { join_key_count }),
                branch: None,
            },
            Self::BranchUnionAggregate {
                branch_count,
                group_key_count,
                aggregate_count,
            } => ImvRefreshContract {
                strategy: RefreshStrategy::UnsupportedBranchUnionAggregate,
                base_refs,
                apply_key: ApplyKeyContract::aggregate_group_row(),
                aggregate: Some(AggregateRefreshContract {
                    group_key_count,
                    aggregate_count,
                }),
                join: None,
                branch: Some(BranchRefreshContract { branch_count }),
            },
        }
    }
}

fn derive_from_query(query: &ResolvedQuery) -> Result<DerivedStructure, String> {
    validate_query_wrapper(query)?;
    derive_from_query_body(&query.body)
}

fn validate_query_wrapper(query: &ResolvedQuery) -> Result<(), String> {
    if !query.local_cte_ids.is_empty() {
        return Err("Iceberg IMV refresh contract does not support WITH queries".to_string());
    }
    if !query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some() {
        return Err(
            "Iceberg IMV refresh contract does not support ORDER BY, LIMIT, or OFFSET".to_string(),
        );
    }
    Ok(())
}

fn derive_from_query_body(body: &QueryBody) -> Result<DerivedStructure, String> {
    match body {
        QueryBody::Select(select) => derive_from_select(select),
        QueryBody::SetOperation(set_op) => derive_from_set_operation(set_op),
        QueryBody::Values(_) => {
            Err("Iceberg IMV refresh contract does not support VALUES queries".to_string())
        }
    }
}

fn derive_from_select(select: &ResolvedSelect) -> Result<DerivedStructure, String> {
    if select.distinct {
        return Err("Iceberg IMV refresh contract does not support SELECT DISTINCT".to_string());
    }
    if select.having.is_some() || select.repeat.is_some() {
        return Err(
            "Iceberg IMV refresh contract does not support HAVING, ROLLUP, CUBE, or GROUPING SETS"
                .to_string(),
        );
    }

    let has_aggregate = select.has_aggregation || !select.group_by.is_empty();
    if has_aggregate {
        let group_key_count = select.group_by.len();
        if group_key_count == 0 {
            return Err(
                "Iceberg IMV refresh contract requires aggregate queries to use a non-empty GROUP BY"
                    .to_string(),
            );
        }
        if let Some(filter) = &select.filter {
            validate_projection_filter_expr(filter)?;
        }
        for group_key in &select.group_by {
            validate_projection_filter_expr(group_key)?;
        }
        let aggregate_count = count_aggregate_projection_outputs(select)?;
        if aggregate_count == 0 {
            return Err(
                "Iceberg IMV refresh contract requires at least one aggregate output".to_string(),
            );
        }
        let input = derive_from_optional_relation(select.from.as_ref())?;
        match input {
            DerivedStructure::ProjectionFilter => Ok(DerivedStructure::SingleAggregate {
                group_key_count,
                aggregate_count,
            }),
            DerivedStructure::UnionProjection { branch_count } => {
                Ok(DerivedStructure::FanInAggregate {
                    branch_count,
                    group_key_count,
                    aggregate_count,
                })
            }
            DerivedStructure::JoinProjection { join_key_count } => {
                Ok(DerivedStructure::JoinAggregate {
                    join_key_count,
                    group_key_count,
                    aggregate_count,
                })
            }
            other => Err(format!(
                "Iceberg IMV refresh contract does not support aggregate over {other:?}"
            )),
        }
    } else {
        validate_projection_filter_exprs(select)?;
        match derive_from_optional_relation(select.from.as_ref())? {
            DerivedStructure::SingleAggregate { .. }
            | DerivedStructure::FanInAggregate { .. }
            | DerivedStructure::JoinAggregate { .. }
            | DerivedStructure::BranchUnionAggregate { .. } => Err(
                "Iceberg IMV refresh contract does not support projection/filter over aggregate subqueries"
                    .to_string(),
            ),
            structure => Ok(structure),
        }
    }
}

fn derive_from_optional_relation(relation: Option<&Relation>) -> Result<DerivedStructure, String> {
    let Some(relation) = relation else {
        return Err(
            "Iceberg IMV refresh contract requires a SELECT with at least one base relation"
                .to_string(),
        );
    };
    derive_from_relation(relation)
}

fn derive_from_relation(relation: &Relation) -> Result<DerivedStructure, String> {
    match relation {
        Relation::Scan(_) => Ok(DerivedStructure::ProjectionFilter),
        Relation::Subquery { query, .. } => derive_from_query(query),
        Relation::Join(join) => {
            if join.join_type != JoinKind::Inner {
                return Err(
                    "Iceberg IMV refresh contract supports only two-table inner equi-join shapes"
                        .to_string(),
                );
            }
            let condition = join.condition.as_ref().ok_or_else(|| {
                "Iceberg IMV refresh contract requires JOIN ... ON equi-join predicates".to_string()
            })?;
            let left_qualifiers = relation_qualifiers(&join.left)?;
            let right_qualifiers = relation_qualifiers(&join.right)?;
            let join_key_count =
                count_equality_join_keys(condition, &left_qualifiers, &right_qualifiers)?;
            if join_key_count == 0 {
                return Err(
                    "Iceberg IMV refresh contract requires at least one equi-join predicate"
                        .to_string(),
                );
            }
            Ok(DerivedStructure::JoinProjection { join_key_count })
        }
        Relation::IcebergMetadataScan(_)
        | Relation::IcebergDeltaScan(_)
        | Relation::GenerateSeries(_)
        | Relation::Unnest(_)
        | Relation::CTEConsume { .. } => Err(format!(
            "Iceberg IMV refresh contract does not support relation {relation:?}"
        )),
    }
}

fn derive_from_set_operation(set_op: &ResolvedSetOp) -> Result<DerivedStructure, String> {
    let mut branches = Vec::new();
    collect_union_all_branches(set_op, &mut branches)?;
    if branches.len() < 2 {
        return Err(
            "Iceberg IMV refresh contract requires UNION ALL with at least two branches"
                .to_string(),
        );
    }
    let derived = branches
        .iter()
        .map(|query| derive_from_query(query))
        .collect::<Result<Vec<_>, _>>()?;
    let branch_count = derived.len();

    let first = derived
        .first()
        .expect("UNION ALL branch list was checked as non-empty");
    match first {
        DerivedStructure::ProjectionFilter
            if derived
                .iter()
                .all(|d| matches!(d, DerivedStructure::ProjectionFilter)) =>
        {
            Ok(DerivedStructure::UnionProjection { branch_count })
        }
        DerivedStructure::SingleAggregate {
            group_key_count,
            aggregate_count,
        } if derived
            .iter()
            .all(|d| matches!(d, DerivedStructure::SingleAggregate { .. })) =>
        {
            for branch in &derived[1..] {
                let DerivedStructure::SingleAggregate {
                    group_key_count: other_group_key_count,
                    aggregate_count: other_aggregate_count,
                } = branch
                else {
                    unreachable!("branch shape checked above");
                };
                if other_group_key_count != group_key_count
                    || other_aggregate_count != aggregate_count
                {
                    return Err(
                        "Iceberg IMV refresh contract requires compatible aggregate branch contracts"
                            .to_string(),
                    );
                }
            }
            Ok(DerivedStructure::BranchUnionAggregate {
                branch_count,
                group_key_count: *group_key_count,
                aggregate_count: *aggregate_count,
            })
        }
        _ => Err(
            "Iceberg IMV refresh contract only supports UNION ALL of projection/filter branches or aggregate branches"
                .to_string(),
        ),
    }
}

fn collect_union_all_branches<'a>(
    set_op: &'a ResolvedSetOp,
    out: &mut Vec<&'a ResolvedQuery>,
) -> Result<(), String> {
    if set_op.kind != SetOpKind::Union || !set_op.all {
        return Err(
            "Iceberg IMV refresh contract only supports UNION ALL set operations".to_string(),
        );
    }
    collect_union_all_query(&set_op.left, out)?;
    collect_union_all_query(&set_op.right, out)
}

fn collect_union_all_query<'a>(
    query: &'a ResolvedQuery,
    out: &mut Vec<&'a ResolvedQuery>,
) -> Result<(), String> {
    validate_query_wrapper(query)?;
    match &query.body {
        QueryBody::SetOperation(set_op) => collect_union_all_branches(set_op, out),
        _ => {
            out.push(query);
            Ok(())
        }
    }
}

fn count_aggregate_projection_outputs(select: &ResolvedSelect) -> Result<usize, String> {
    let mut aggregate_count = 0;
    let mut projected_group_keys = vec![false; select.group_by.len()];
    for item in &select.projection {
        if let Some(index) = select
            .group_by
            .iter()
            .position(|group_key| typed_expr_eq(group_key, &item.expr))
        {
            projected_group_keys[index] = true;
            continue;
        }

        match &item.expr.kind {
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
                ..
            } => {
                validate_supported_aggregate_call(name, args.len(), *distinct, order_by)?;
                validate_aggregate_argument_exprs(args)?;
                aggregate_count += 1;
                continue;
            }
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
            } if is_legacy_unresolved_aggregate_function_name(name) => {
                validate_supported_aggregate_call(name, args.len(), *distinct, &[])?;
                validate_aggregate_argument_exprs(args)?;
                aggregate_count += 1;
                continue;
            }
            _ => {}
        }

        validate_non_contract_aggregate_projection_expr(&item.expr)?;
        return Err(
            "Iceberg IMV refresh contract aggregate projections must be GROUP BY keys or direct aggregate calls"
                .to_string(),
        );
    }
    if projected_group_keys.iter().any(|projected| !projected) {
        return Err(
            "Iceberg IMV refresh contract aggregate projection must include every GROUP BY key"
                .to_string(),
        );
    }
    Ok(aggregate_count)
}

fn validate_non_contract_aggregate_projection_expr(expr: &TypedExpr) -> Result<(), String> {
    match &expr.kind {
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            ..
        } => {
            validate_supported_aggregate_call(name, args.len(), *distinct, order_by)?;
            validate_aggregate_argument_exprs(args)
        }
        ExprKind::WindowCall { .. } => Err(
            "Iceberg IMV refresh contract does not support aggregate or window expressions outside direct aggregate outputs"
                .to_string(),
        ),
        ExprKind::BinaryOp { left, right, .. } => {
            validate_non_contract_aggregate_projection_expr(left)?;
            validate_non_contract_aggregate_projection_expr(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)
        }
        ExprKind::Nested(expr) => validate_non_contract_aggregate_projection_expr(expr),
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => {
            if is_legacy_unresolved_aggregate_function_name(name) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support aggregate function `{name}` outside direct aggregate outputs"
                ));
            }
            if *distinct {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support DISTINCT scalar function `{name}`"
                ));
            }
            if is_unsupported_contract_scalar_function(name, args.len()) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support non-deterministic or unsafe scalar function `{name}`"
                ));
            }
            args.iter()
                .try_for_each(validate_non_contract_aggregate_projection_expr)
        }
        ExprKind::LambdaFunction { body, .. } => validate_non_contract_aggregate_projection_expr(body),
        ExprKind::InList { expr, list, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            list.iter()
                .try_for_each(validate_non_contract_aggregate_projection_expr)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            validate_non_contract_aggregate_projection_expr(low)?;
            validate_non_contract_aggregate_projection_expr(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            validate_non_contract_aggregate_projection_expr(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_non_contract_aggregate_projection_expr(operand)?;
            }
            for (when, then) in when_then {
                validate_non_contract_aggregate_projection_expr(when)?;
                validate_non_contract_aggregate_projection_expr(then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_non_contract_aggregate_projection_expr(else_expr)?;
            }
            Ok(())
        }
        ExprKind::Lambda { body, .. } => validate_non_contract_aggregate_projection_expr(body),
        ExprKind::SubqueryPlaceholder { .. } => Err(
            "Iceberg IMV refresh contract does not support subquery expressions in aggregate projections"
                .to_string(),
        ),
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_) => Ok(()),
    }
}

fn validate_supported_aggregate_call(
    name: &str,
    arg_count: usize,
    distinct: bool,
    order_by: &[crate::sql::analysis::SortItem],
) -> Result<(), String> {
    if !order_by.is_empty() {
        return Err("Iceberg IMV refresh contract does not support aggregate ORDER BY".to_string());
    }
    let normalized = name.to_ascii_lowercase();
    let supported = matches!(
        normalized.as_str(),
        "count"
            | "count_distinct"
            | "multi_distinct_count"
            | "approx_count_distinct"
            | "ndv"
            | "hll_ndv"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "bool_or"
            | "boolor_agg"
            | "bool_and"
            | "booland_agg"
    );
    if !supported {
        return Err(format!(
            "Iceberg IMV refresh contract does not support aggregate function `{name}`"
        ));
    }
    if distinct && normalized != "count" {
        return Err(format!(
            "Iceberg IMV refresh contract does not support DISTINCT aggregate `{name}`"
        ));
    }
    if normalized == "count" {
        if (distinct && arg_count != 1) || (!distinct && arg_count > 1) {
            return Err(format!(
                "Iceberg IMV refresh contract supports only zero or one argument for aggregate function `{name}`"
            ));
        }
    } else if arg_count != 1 {
        return Err(format!(
            "Iceberg IMV refresh contract requires exactly one argument for aggregate function `{name}`"
        ));
    }
    Ok(())
}

fn validate_aggregate_argument_exprs(args: &[TypedExpr]) -> Result<(), String> {
    args.iter().try_for_each(validate_projection_filter_expr)
}

fn is_legacy_unresolved_aggregate_function_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count_distinct" | "hll_ndv"
    )
}

fn typed_expr_eq(left: &TypedExpr, right: &TypedExpr) -> bool {
    left.data_type == right.data_type
        && left.nullable == right.nullable
        && expr_kind_eq(&left.kind, &right.kind)
}

fn typed_exprs_eq(left: &[TypedExpr], right: &[TypedExpr]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| typed_expr_eq(left, right))
}

fn expr_kind_eq(left: &ExprKind, right: &ExprKind) -> bool {
    match (left, right) {
        (
            ExprKind::ColumnRef {
                column_id: left_id,
                qualifier: left_qualifier,
                column: left_column,
            },
            ExprKind::ColumnRef {
                column_id: right_id,
                qualifier: right_qualifier,
                column: right_column,
            },
        ) => {
            left_id == right_id
                && left_qualifier == right_qualifier
                && left_column.eq_ignore_ascii_case(right_column)
        }
        (
            ExprKind::LambdaParamRef {
                name: left_name,
                slot_id: left_slot,
            },
            ExprKind::LambdaParamRef {
                name: right_name,
                slot_id: right_slot,
            },
        ) => left_name == right_name && left_slot == right_slot,
        (ExprKind::Literal(left), ExprKind::Literal(right)) => left == right,
        (
            ExprKind::BinaryOp {
                left: left_left,
                op: left_op,
                right: left_right,
            },
            ExprKind::BinaryOp {
                left: right_left,
                op: right_op,
                right: right_right,
            },
        ) => {
            left_op == right_op
                && typed_expr_eq(left_left, right_left)
                && typed_expr_eq(left_right, right_right)
        }
        (
            ExprKind::UnaryOp {
                op: left_op,
                expr: left_expr,
            },
            ExprKind::UnaryOp {
                op: right_op,
                expr: right_expr,
            },
        ) => left_op == right_op && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::FunctionCall {
                name: left_name,
                args: left_args,
                distinct: left_distinct,
            },
            ExprKind::FunctionCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && typed_exprs_eq(left_args, right_args)
        }
        (
            ExprKind::Cast {
                expr: left_expr,
                target: left_target,
            },
            ExprKind::Cast {
                expr: right_expr,
                target: right_target,
            },
        ) => left_target == right_target && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::IsNull {
                expr: left_expr,
                negated: left_negated,
            },
            ExprKind::IsNull {
                expr: right_expr,
                negated: right_negated,
            },
        ) => left_negated == right_negated && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::InList {
                expr: left_expr,
                list: left_list,
                negated: left_negated,
            },
            ExprKind::InList {
                expr: right_expr,
                list: right_list,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_exprs_eq(left_list, right_list)
        }
        (
            ExprKind::Between {
                expr: left_expr,
                low: left_low,
                high: left_high,
                negated: left_negated,
            },
            ExprKind::Between {
                expr: right_expr,
                low: right_low,
                high: right_high,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_expr_eq(left_low, right_low)
                && typed_expr_eq(left_high, right_high)
        }
        (
            ExprKind::Like {
                expr: left_expr,
                pattern: left_pattern,
                negated: left_negated,
            },
            ExprKind::Like {
                expr: right_expr,
                pattern: right_pattern,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_expr_eq(left_pattern, right_pattern)
        }
        (
            ExprKind::Case {
                operand: left_operand,
                when_then: left_when_then,
                else_expr: left_else,
            },
            ExprKind::Case {
                operand: right_operand,
                when_then: right_when_then,
                else_expr: right_else,
            },
        ) => {
            option_typed_expr_eq(left_operand.as_deref(), right_operand.as_deref())
                && left_when_then.len() == right_when_then.len()
                && left_when_then.iter().zip(right_when_then.iter()).all(
                    |((left_when, left_then), (right_when, right_then))| {
                        typed_expr_eq(left_when, right_when) && typed_expr_eq(left_then, right_then)
                    },
                )
                && option_typed_expr_eq(left_else.as_deref(), right_else.as_deref())
        }
        (
            ExprKind::IsTruthValue {
                expr: left_expr,
                value: left_value,
                negated: left_negated,
            },
            ExprKind::IsTruthValue {
                expr: right_expr,
                value: right_value,
                negated: right_negated,
            },
        ) => {
            left_value == right_value
                && left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
        }
        (ExprKind::Nested(left), ExprKind::Nested(right)) => typed_expr_eq(left, right),
        _ => false,
    }
}

fn option_typed_expr_eq(left: Option<&TypedExpr>, right: Option<&TypedExpr>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => typed_expr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn validate_projection_filter_exprs(select: &ResolvedSelect) -> Result<(), String> {
    for item in &select.projection {
        validate_projection_filter_expr(&item.expr)?;
    }
    if let Some(filter) = &select.filter {
        validate_projection_filter_expr(filter)?;
    }
    Ok(())
}

fn validate_projection_filter_expr(expr: &TypedExpr) -> Result<(), String> {
    match &expr.kind {
        ExprKind::AggregateCall { .. } | ExprKind::WindowCall { .. } => {
            Err("Iceberg IMV refresh contract does not support aggregate or window expressions in projection/filter shapes".to_string())
        }
        ExprKind::SubqueryPlaceholder { .. } => Err(
            "Iceberg IMV refresh contract does not support subquery expressions in projection/filter shapes"
                .to_string(),
        ),
        ExprKind::BinaryOp { left, right, .. } => {
            validate_projection_filter_expr(left)?;
            validate_projection_filter_expr(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::LambdaFunction { body: expr, .. }
        | ExprKind::Lambda { body: expr, .. } => validate_projection_filter_expr(expr),
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => {
            if is_legacy_unresolved_aggregate_function_name(name) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support aggregate function `{name}` in projection/filter shapes"
                ));
            }
            if *distinct {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support DISTINCT scalar function `{name}`"
                ));
            }
            if is_unsupported_contract_scalar_function(name, args.len()) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support non-deterministic or unsafe scalar function `{name}`"
                ));
            }
            for arg in args {
                validate_projection_filter_expr(arg)?;
            }
            Ok(())
        }
        ExprKind::InList { expr, list, .. } => {
            validate_projection_filter_expr(expr)?;
            for item in list {
                validate_projection_filter_expr(item)?;
            }
            Ok(())
        }
        ExprKind::Between { expr, low, high, .. } => {
            validate_projection_filter_expr(expr)?;
            validate_projection_filter_expr(low)?;
            validate_projection_filter_expr(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_projection_filter_expr(expr)?;
            validate_projection_filter_expr(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_projection_filter_expr(operand)?;
            }
            for (when, then) in when_then {
                validate_projection_filter_expr(when)?;
                validate_projection_filter_expr(then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_projection_filter_expr(else_expr)?;
            }
            Ok(())
        }
        ExprKind::ColumnRef { .. } | ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {
            Ok(())
        }
    }
}

fn is_unsupported_contract_scalar_function(name: &str, arg_count: usize) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
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
            | "sleep"
            | "version"
            | "database"
            | "current_user"
            | "user"
            | "grouping"
            | "grouping_id"
    ) || (name.eq_ignore_ascii_case("unix_timestamp") && arg_count == 0)
}

fn relation_qualifiers(relation: &Relation) -> Result<Vec<String>, String> {
    match relation {
        Relation::Scan(scan) => Ok(vec![
            scan.alias
                .clone()
                .unwrap_or_else(|| scan.table.name.clone())
                .to_ascii_lowercase(),
        ]),
        _ => Err(
            "Iceberg IMV refresh contract supports join keys only over direct scan inputs"
                .to_string(),
        ),
    }
}

fn count_equality_join_keys(
    expr: &TypedExpr,
    left_qualifiers: &[String],
    right_qualifiers: &[String],
) -> Result<usize, String> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => Ok(
            count_equality_join_keys(left, left_qualifiers, right_qualifiers)?
                + count_equality_join_keys(right, left_qualifiers, right_qualifiers)?,
        ),
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let left_side = join_key_side(left, left_qualifiers, right_qualifiers)?;
            let right_side = join_key_side(right, left_qualifiers, right_qualifiers)?;
            if left_side == right_side {
                return Err(
                    "Iceberg IMV refresh contract equi-join predicates must compare left and right join inputs"
                        .to_string(),
                );
            }
            Ok(1)
        }
        ExprKind::Nested(expr) => count_equality_join_keys(expr, left_qualifiers, right_qualifiers),
        _ => Err(
            "Iceberg IMV refresh contract supports only AND-combined equi-join predicates"
                .to_string(),
        ),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinKeySide {
    Left,
    Right,
}

fn join_key_side(
    expr: &TypedExpr,
    left_qualifiers: &[String],
    right_qualifiers: &[String],
) -> Result<JoinKeySide, String> {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier: Some(qualifier),
            ..
        } => {
            let qualifier = qualifier.to_ascii_lowercase();
            if left_qualifiers.iter().any(|left| left == &qualifier) {
                Ok(JoinKeySide::Left)
            } else if right_qualifiers.iter().any(|right| right == &qualifier) {
                Ok(JoinKeySide::Right)
            } else {
                Err(format!(
                    "Iceberg IMV refresh contract join key qualifier `{qualifier}` does not match either join input"
                ))
            }
        }
        ExprKind::Nested(expr) => join_key_side(expr, left_qualifiers, right_qualifiers),
        _ => Err(
            "Iceberg IMV refresh contract join keys must be qualified column references"
                .to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::table::mv_ddl::{MvAnalysis, ResolvedTableRef};
    use crate::sql::analysis::{LiteralValue, QueryBody, SortItem, SubqueryKind};
    use crate::sql::catalog::{
        CatalogProvider, ColumnDef, IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo,
        ScanSource, TableDef,
    };
    use arrow::datatypes::DataType;

    struct TestIcebergCatalog;

    impl CatalogProvider for TestIcebergCatalog {
        fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
            Ok(TableDef {
                name: table.to_string(),
                columns: vec![
                    column("id", DataType::Int64, false),
                    column("region", DataType::Utf8, true),
                    column("amount", DataType::Int64, true),
                    column("flag", DataType::Boolean, true),
                ],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: iceberg_table_info(database, table),
                    files: Vec::new(),
                    cloud_properties: Default::default(),
                    binding: IcebergDataFileBinding::CurrentSnapshot,
                },
            })
        }
    }

    fn column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_table_info(database: &str, table: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: database.to_string(),
            table: table.to_string(),
            table_uuid: Some(format!("uuid-{table}")),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: format!("file:///tmp/{database}/{table}"),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
        }
    }

    fn parse_and_analyze_mv_query(sql: &str, table_refs: &[&str]) -> MvAnalysis {
        let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse query");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        let (resolved_query, _, _) =
            crate::sql::analyzer::analyze(&query, &TestIcebergCatalog, "sales")
                .expect("analyze query");
        MvAnalysis {
            resolved_refs: table_refs
                .iter()
                .map(|table| ResolvedTableRef::Iceberg {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: (*table).to_string(),
                })
                .collect(),
            output_columns: resolved_query.output_columns.clone(),
            resolved_query,
        }
    }

    fn base_refs(contract: &ImvRefreshContract) -> Vec<String> {
        contract
            .base_refs
            .iter()
            .map(IcebergTableRef::fqn)
            .collect()
    }

    fn int_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn distinct_abs_expr() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "abs".to_string(),
                args: vec![int_literal(1)],
                distinct: true,
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    #[test]
    fn derives_projection_filter_contract_from_analyzed_query() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, amount + 1 AS adjusted_amount
             FROM fact_east
             WHERE amount > 0",
            &["fact_east"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(contract.strategy, RefreshStrategy::ProjectionFilter);
        assert_eq!(base_refs(&contract), vec!["ice.sales.fact_east"]);
        assert_eq!(contract.apply_key, ApplyKeyContract::projection_filter());
        assert_eq!(contract.aggregate, None);
        assert_eq!(contract.join, None);
        assert_eq!(contract.branch, None);
    }

    #[test]
    fn derives_union_projection_filter_contract_from_analyzed_query() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, amount FROM fact_east
             UNION ALL
             SELECT region, amount FROM fact_west",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(contract.strategy, RefreshStrategy::UnionProjectionFilter);
        assert_eq!(
            base_refs(&contract),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
        assert_eq!(
            contract.apply_key,
            ApplyKeyContract::union_projection_filter()
        );
        assert_eq!(
            contract.branch,
            Some(BranchRefreshContract { branch_count: 2 })
        );
        assert_eq!(contract.aggregate, None);
        assert_eq!(contract.join, None);
    }

    #[test]
    fn rejects_top_level_order_limit_offset_contracts() {
        for sql in [
            "SELECT region FROM fact_east ORDER BY region",
            "SELECT region FROM fact_east LIMIT 10",
            "SELECT region FROM fact_east OFFSET 1",
        ] {
            let analysis = parse_and_analyze_mv_query(sql, &["fact_east"]);

            let err = derive_imv_refresh_contract(&analysis)
                .expect_err("top-level ORDER BY/LIMIT/OFFSET are unsupported");

            assert!(
                err.contains("ORDER BY, LIMIT, or OFFSET"),
                "unexpected error for {sql}: {err}"
            );
        }
    }

    #[test]
    fn rejects_nested_union_wrapper_limit_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "(SELECT region FROM fact_east
              UNION ALL
              SELECT region FROM fact_west
              LIMIT 1)
             UNION ALL
             SELECT region FROM fact_extra",
            &["fact_east", "fact_west", "fact_extra"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("nested UNION wrapper LIMIT is unsupported");

        assert!(
            err.contains("ORDER BY, LIMIT, or OFFSET"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_select_distinct_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT DISTINCT region
             FROM fact_east",
            &["fact_east"],
        );

        let err =
            derive_imv_refresh_contract(&analysis).expect_err("SELECT DISTINCT is unsupported");

        assert!(err.contains("SELECT DISTINCT"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_having_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c
             FROM fact_east
             GROUP BY region
             HAVING count(*) > 0",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis).expect_err("HAVING is unsupported");

        assert!(err.contains("HAVING"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_non_iceberg_base_refs() {
        let mut analysis = parse_and_analyze_mv_query(
            "SELECT region
             FROM fact_east",
            &["fact_east"],
        );
        analysis.resolved_refs[0] = ResolvedTableRef::StarRocks {
            database: "sales".to_string(),
            table: "fact_east".to_string(),
        };

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("StarRocks base refs are unsupported");

        assert!(
            err.contains("requires Iceberg base tables"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn derives_single_aggregate_contract_from_analyzed_query() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c, sum(amount) AS s FROM fact GROUP BY region",
            &["fact"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(contract.strategy, RefreshStrategy::SingleAggregate);
        assert_eq!(base_refs(&contract), vec!["ice.sales.fact"]);
        assert_eq!(contract.apply_key, ApplyKeyContract::aggregate_group_row());
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(contract.join, None);
        assert_eq!(contract.branch, None);
    }

    #[test]
    fn derives_join_aggregate_contract_from_analyzed_query() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, count(*) AS c, sum(r.amount) AS s
             FROM fact_east l JOIN fact_west r ON l.id = r.id
             GROUP BY l.region",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(contract.strategy, RefreshStrategy::JoinAggregate);
        assert_eq!(
            contract.apply_key,
            ApplyKeyContract::join_aggregate_group_row()
        );
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(
            contract.join,
            Some(JoinRefreshContract { join_key_count: 1 })
        );
    }

    #[test]
    fn rejects_self_join_contracts_with_deduplicated_base_refs() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact l JOIN fact r ON l.id = r.id",
            &["fact"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("self-join has one distinct base ref for a two-side contract");

        assert!(
            err.contains("requires 2 distinct Iceberg base table refs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn derives_fan_in_aggregate_contract_from_aggregate_over_union() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM (
                 SELECT region, amount FROM fact_east
                 UNION ALL
                 SELECT region, amount FROM fact_west
             ) u
             GROUP BY region",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(contract.strategy, RefreshStrategy::FanInAggregate);
        assert_eq!(
            base_refs(&contract),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
        assert_eq!(contract.apply_key, ApplyKeyContract::aggregate_group_row());
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(
            contract.branch,
            Some(BranchRefreshContract { branch_count: 2 })
        );
        assert_eq!(contract.join, None);
    }

    #[test]
    fn rejects_duplicate_base_fan_in_aggregate_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM (
                 SELECT region, amount FROM fact
                 UNION ALL
                 SELECT region, amount FROM fact
             ) u
             GROUP BY region",
            &["fact"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("fan-in duplicate base refs are unsupported");

        assert!(
            err.contains("requires 2 distinct Iceberg base table refs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn recognizes_b_family_but_keeps_it_unsupported() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM fact_east
             GROUP BY region
             UNION ALL
             SELECT region, count(*) AS c, sum(amount) AS s
             FROM fact_west
             GROUP BY region",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(
            contract.strategy,
            RefreshStrategy::UnsupportedBranchUnionAggregate
        );
        assert_eq!(
            base_refs(&contract),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
        assert_eq!(contract.apply_key, ApplyKeyContract::aggregate_group_row());
        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 2,
            })
        );
        assert_eq!(
            contract.branch,
            Some(BranchRefreshContract { branch_count: 2 })
        );
        assert_eq!(contract.join, None);
    }

    #[test]
    fn derives_join_projection_contract_from_inner_equi_join() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact_east l JOIN fact_west r ON l.id = r.id",
            &["fact_east", "fact_west"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(contract.strategy, RefreshStrategy::JoinProjectionFilter);
        assert_eq!(
            contract.apply_key.column_name,
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN
        );
        assert_eq!(
            contract.apply_key.value_type,
            crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Utf8
        );
        assert!(!contract.apply_key.allow_full_rebuild_on_policy_full_refresh);
        assert_eq!(contract.apply_key.rewrite_evidence, RewriteEvidence::None);
        assert_eq!(
            contract.join,
            Some(JoinRefreshContract { join_key_count: 1 })
        );
    }

    #[test]
    fn rejects_outer_join_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact_east l LEFT JOIN fact_west r ON l.id = r.id",
            &["fact_east", "fact_west"],
        );

        let err = derive_imv_refresh_contract(&analysis).expect_err("outer join is unsupported");

        assert!(err.contains("inner equi-join"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_cross_join_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact_east l CROSS JOIN fact_west r",
            &["fact_east", "fact_west"],
        );

        let err = derive_imv_refresh_contract(&analysis).expect_err("cross join is unsupported");

        assert!(err.contains("inner equi-join"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_non_equi_join_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact_east l JOIN fact_west r ON l.id > r.id",
            &["fact_east", "fact_west"],
        );

        let err = derive_imv_refresh_contract(&analysis).expect_err("non-equi join is unsupported");

        assert!(err.contains("equi-join"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_same_side_join_key_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM fact_east l JOIN fact_west r ON l.id = l.amount",
            &["fact_east", "fact_west"],
        );

        let err =
            derive_imv_refresh_contract(&analysis).expect_err("same-side join key is unsupported");

        assert!(
            err.contains("left and right join inputs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_join_subquery_side_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT l.region, r.amount
             FROM (
                 SELECT id, region
                 FROM fact_east
                 WHERE amount > 0
             ) l
             JOIN fact_west r ON l.id = r.id",
            &["fact_east", "fact_west"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("join branch refresh requires direct scan inputs");

        assert!(
            err.contains("direct scan inputs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_aggregate_without_group_keys() {
        let analysis =
            parse_and_analyze_mv_query("SELECT count(*) AS c FROM fact_east", &["fact_east"]);

        let err =
            derive_imv_refresh_contract(&analysis).expect_err("global aggregate is unsupported");

        assert!(
            err.contains("non-empty GROUP BY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_group_by_without_aggregate_outputs() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region FROM fact_east GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis).expect_err("aggregate output is required");

        assert!(
            err.contains("requires at least one aggregate output"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_top_level_with_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "WITH unused AS (SELECT id FROM fact_extra)
             SELECT region, amount
             FROM fact_east",
            &["fact_extra", "fact_east"],
        );

        let err =
            derive_imv_refresh_contract(&analysis).expect_err("top-level WITH is unsupported");

        assert!(err.contains("WITH"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_sql_aggregate_filter_at_parser_boundary() {
        let err = crate::sql::parser::parse_sql_raw(
            "SELECT region, sum(amount) FILTER (WHERE flag) AS total
             FROM fact_east
             GROUP BY region",
        )
        .expect_err("aggregate FILTER should be rejected instead of being dropped");

        assert!(err.contains("syntax error"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_aggregate_contracts_missing_projected_group_keys() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT count(*) AS c
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("aggregate contract must project every group key");

        assert!(
            err.contains("include every GROUP BY key"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_nondeterministic_aggregate_filter_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count(*) AS c
             FROM fact_east
             WHERE rand() > 0.5
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("non-deterministic aggregate filter is unsupported");

        assert!(
            err.contains("non-deterministic or unsafe"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_nondeterministic_group_key_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT rand() AS r, count(*) AS c
             FROM fact_east
             GROUP BY rand()",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("non-deterministic group key is unsupported");

        assert!(
            err.contains("non-deterministic or unsafe"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_invalid_aggregate_filter_expression_contracts() {
        for (filter, expected) in [
            (
                TypedExpr {
                    kind: ExprKind::AggregateCall {
                        name: "sum".to_string(),
                        args: vec![int_literal(1)],
                        distinct: false,
                        order_by: Vec::new(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                "aggregate or window expressions",
            ),
            (
                TypedExpr {
                    kind: ExprKind::WindowCall {
                        name: "row_number".to_string(),
                        args: Vec::new(),
                        distinct: false,
                        partition_by: Vec::new(),
                        order_by: Vec::new(),
                        window_frame: None,
                        ignore_nulls: false,
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                "aggregate or window expressions",
            ),
            (
                TypedExpr {
                    kind: ExprKind::SubqueryPlaceholder {
                        id: 1,
                        kind: SubqueryKind::Scalar,
                        data_type: DataType::Int64,
                    },
                    data_type: DataType::Int64,
                    nullable: true,
                },
                "subquery expressions",
            ),
        ] {
            let mut analysis = parse_and_analyze_mv_query(
                "SELECT region, count(*) AS c
                 FROM fact_east
                 GROUP BY region",
                &["fact_east"],
            );
            let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
                panic!("expected select");
            };
            select.filter = Some(filter);

            let err = derive_imv_refresh_contract(&analysis)
                .expect_err("aggregate filter expression is unsupported");

            assert!(
                err.contains(expected),
                "expected {expected}, unexpected error: {err}"
            );
        }
    }

    #[test]
    fn rejects_unsupported_aggregate_function_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, count_if(flag) AS c
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("count_if is unsupported by the Iceberg IMV rewrite contract");

        assert!(
            err.contains("does not support aggregate function"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn derives_supported_distinct_state_aggregate_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region,
                    count(DISTINCT id) AS exact_distinct,
                    count_distinct(id) AS count_distinct_alias,
                    multi_distinct_count(id) AS multi_distinct,
                    approx_count_distinct(id) AS approx_distinct,
                    ndv(id) AS ndv_alias,
                    hll_ndv(id) AS hll_ndv_alias
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 6,
            })
        );
    }

    #[test]
    fn rejects_hll_sketch_distinct_aggregate_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, approx_count_distinct_hll_sketch(id) AS c
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("HLL sketch aggregate is unsupported by the Iceberg IMV rewrite contract");

        assert!(
            err.contains("does not support aggregate function"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn derives_supported_aggregate_alias_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region,
                    count(*) AS c0,
                    count(id) AS c1,
                    sum(amount) AS s,
                    avg(amount) AS a,
                    min(amount) AS mn,
                    max(amount) AS mx,
                    bool_or(flag) AS b0,
                    boolor_agg(flag) AS b1,
                    bool_and(flag) AS b2,
                    booland_agg(flag) AS b3
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let contract = derive_imv_refresh_contract(&analysis).expect("derive contract");

        assert_eq!(
            contract.aggregate,
            Some(AggregateRefreshContract {
                group_key_count: 1,
                aggregate_count: 10,
            })
        );
    }

    #[test]
    fn rejects_unsupported_distinct_aggregate_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(DISTINCT amount) AS c
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("sum DISTINCT is unsupported by the logical IMV rewrite");

        assert!(err.contains("DISTINCT"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_multi_argument_aggregate_contracts() {
        let mut analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(amount) AS s
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );
        let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
            panic!("expected select");
        };
        let ExprKind::AggregateCall { args, .. } = &mut select.projection[1].expr.kind else {
            panic!("expected aggregate projection");
        };
        args.push(TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        });

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("multi-argument aggregate is unsupported by the logical IMV rewrite");

        assert!(
            err.contains("exactly one argument") || err.contains("zero or one argument"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_aggregate_order_by_contracts() {
        let mut analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(amount) AS s
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );
        let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
            panic!("expected select");
        };
        let ExprKind::AggregateCall { order_by, .. } = &mut select.projection[1].expr.kind else {
            panic!("expected aggregate projection");
        };
        order_by.push(SortItem {
            expr: int_literal(1),
            asc: true,
            nulls_first: false,
        });

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("aggregate ORDER BY is unsupported by the logical IMV rewrite");

        assert!(
            err.contains("aggregate ORDER BY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_nondeterministic_aggregate_argument_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(rand()) AS s
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("non-deterministic aggregate argument is unsupported");

        assert!(
            err.contains("non-deterministic or unsafe"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_distinct_scalar_aggregate_argument_contracts() {
        let mut analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(amount) AS s
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );
        let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
            panic!("expected select");
        };
        let ExprKind::AggregateCall { args, .. } = &mut select.projection[1].expr.kind else {
            panic!("expected aggregate projection");
        };
        args[0] = distinct_abs_expr();

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("distinct scalar aggregate argument is unsupported");

        assert!(
            err.contains("DISTINCT scalar function"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_scalar_wrapped_aggregate_projection_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, sum(amount) + 1 AS adjusted_sum
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("scalar-wrapped aggregate output is not represented in the contract");

        assert!(
            err.contains("GROUP BY keys or direct aggregate calls"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_outer_projection_over_aggregate_subquery_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, c + 1 AS adjusted_count
             FROM (
                 SELECT region, count(*) AS c
                 FROM fact_east
                 GROUP BY region
             ) s",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("outer projection over aggregate subquery is unsupported");

        assert!(
            err.contains("aggregate subqueries"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_nondeterministic_projection_filter_contracts() {
        for sql in [
            "SELECT region, rand() AS r FROM fact_east",
            "SELECT region FROM fact_east WHERE sleep(1)",
            "SELECT region, current_timestamp() AS ts FROM fact_east",
        ] {
            let analysis = parse_and_analyze_mv_query(sql, &["fact_east"]);

            let err = derive_imv_refresh_contract(&analysis)
                .expect_err("non-deterministic projection/filter expression is unsupported");

            assert!(
                err.contains("non-deterministic or unsafe"),
                "unexpected error for {sql}: {err}"
            );
        }
    }

    #[test]
    fn rejects_grouping_pseudo_functions_in_projection_filter_contracts() {
        for function_name in ["grouping", "grouping_id"] {
            let mut analysis = parse_and_analyze_mv_query(
                "SELECT region, abs(amount) AS pseudo
                 FROM fact_east",
                &["fact_east"],
            );
            let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
                panic!("expected select");
            };
            select.projection[1].expr = TypedExpr {
                kind: ExprKind::FunctionCall {
                    name: function_name.to_string(),
                    args: vec![int_literal(1)],
                    distinct: false,
                },
                data_type: DataType::Int64,
                nullable: false,
            };

            let err = derive_imv_refresh_contract(&analysis)
                .expect_err("grouping pseudo function is unsupported");

            assert!(
                err.contains("non-deterministic or unsafe"),
                "unexpected error for {function_name}: {err}"
            );
        }
    }

    #[test]
    fn rejects_distinct_scalar_projection_filter_contracts() {
        let mut analysis = parse_and_analyze_mv_query(
            "SELECT region, abs(amount) AS abs_amount
             FROM fact_east",
            &["fact_east"],
        );
        let QueryBody::Select(select) = &mut analysis.resolved_query.body else {
            panic!("expected select");
        };
        select.projection[1].expr = distinct_abs_expr();

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("distinct scalar projection expression is unsupported");

        assert!(
            err.contains("DISTINCT scalar function"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_aggregate_window_projection_contracts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, row_number() OVER (ORDER BY region) AS rn
             FROM fact_east
             GROUP BY region",
            &["fact_east"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("window output is not represented in the aggregate contract");

        assert!(
            err.contains("aggregate or window expressions"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_projection_filter_window_expressions() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, row_number() OVER (ORDER BY id) AS rn FROM fact_east",
            &["fact_east"],
        );

        let err =
            derive_imv_refresh_contract(&analysis).expect_err("window expression is unsupported");

        assert!(
            err.contains("aggregate or window expressions"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_branch_union_aggregate_with_incompatible_counts() {
        let analysis = parse_and_analyze_mv_query(
            "SELECT region, amount, count(*) AS c
             FROM fact_east
             GROUP BY region, amount
             UNION ALL
             SELECT region, count(*) AS c, sum(amount) AS s
             FROM fact_west
             GROUP BY region",
            &["fact_east", "fact_west"],
        );

        let err = derive_imv_refresh_contract(&analysis)
            .expect_err("branch aggregate contracts must be compatible");

        assert!(
            err.contains("compatible aggregate branch contracts"),
            "unexpected error: {err}"
        );
    }
}
