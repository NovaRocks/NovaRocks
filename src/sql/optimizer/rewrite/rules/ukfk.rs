//! UK/FK-based logical rewrites for standalone Iceberg table properties.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, ProjectItem, TypedExpr};
use crate::sql::optimizer::options::current_session_optimizer_settings;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule as RewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_column_id_refs, collect_output_ids, collect_qualified_column_refs,
    collect_qualified_output_columns, combine_and,
};
use crate::sql::planner::plan::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ForeignKeyConstraint {
    local_columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
}

pub(crate) struct PruneUkFkJoin;

impl RewriteRule for PruneUkFkJoin {
    fn name(&self) -> &'static str {
        "PruneUkFkJoin"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Project(ProjectNode { input, .. })
                if matches!(input.as_ref(), LogicalPlan::Join(_))
        )
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let settings = current_session_optimizer_settings();
        let table_prune_enabled = settings.enable_query_rewrite_table_prune
            || settings.enable_cbo_table_prune
            || settings.enable_table_prune_on_update;
        if !table_prune_enabled && !settings.enable_ukfk_opt {
            return None;
        }

        let LogicalPlan::Project(project) = plan else {
            return None;
        };
        let LogicalPlan::Join(join) = *project.input else {
            return None;
        };

        let retained_side = project_referenced_side(&project.items, &join.left, &join.right)?;
        let eq_pairs = join_equality_pairs(&join)?;
        let left_cols: Vec<String> = eq_pairs.iter().map(|(left, _)| left.clone()).collect();
        let right_cols: Vec<String> = eq_pairs.iter().map(|(_, right)| right.clone()).collect();
        let left_scan = root_scan(&join.left)?;
        let right_scan = root_scan(&join.right)?;

        let retained = match (join.join_type, retained_side) {
            (JoinKind::LeftOuter, Side::Left)
                if table_prune_enabled && table_has_unique_key(right_scan, &right_cols) =>
            {
                Some(join.left.as_ref().clone())
            }
            (JoinKind::RightOuter, Side::Right)
                if table_prune_enabled && table_has_unique_key(left_scan, &left_cols) =>
            {
                Some(join.right.as_ref().clone())
            }
            (JoinKind::Inner, Side::Left)
                if settings.enable_ukfk_opt
                    && foreign_key_matches(left_scan, right_scan, &left_cols, &right_cols) =>
            {
                Some(add_not_null_filter(
                    join.left.as_ref().clone(),
                    left_scan,
                    &left_cols,
                ))
            }
            (JoinKind::Inner, Side::Right)
                if settings.enable_ukfk_opt
                    && foreign_key_matches(right_scan, left_scan, &right_cols, &left_cols) =>
            {
                Some(add_not_null_filter(
                    join.right.as_ref().clone(),
                    right_scan,
                    &right_cols,
                ))
            }
            _ => None,
        }?;

        Some(LogicalPlan::Project(ProjectNode {
            input: Box::new(retained),
            items: project.items,
            required_output_columns: project.required_output_columns,
        }))
    }
}

pub(crate) struct EliminateUniqueAggregate;

impl RewriteRule for EliminateUniqueAggregate {
    fn name(&self) -> &'static str {
        "EliminateUniqueAggregate"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Project(ProjectNode { input, .. })
                if matches!(input.as_ref(), LogicalPlan::Aggregate(_))
        )
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let settings = current_session_optimizer_settings();
        if !settings.enable_eliminate_agg {
            return None;
        }

        let LogicalPlan::Project(project) = plan else {
            return None;
        };
        let LogicalPlan::Aggregate(aggregate) = *project.input else {
            return None;
        };
        let scan = root_scan(&aggregate.input)?;
        let group_columns = group_by_columns(&aggregate.group_by)?;
        if group_columns.is_empty() || !table_has_unique_key(scan, &group_columns) {
            return None;
        }
        if aggregate.aggregates.is_empty() || !aggregate.aggregates.iter().all(is_eliminable_count)
        {
            return None;
        }
        let items = project
            .items
            .into_iter()
            .map(rewrite_eliminated_aggregate_project_item)
            .collect::<Option<Vec<_>>>()?;

        Some(LogicalPlan::Project(ProjectNode {
            input: aggregate.input,
            items,
            required_output_columns: project.required_output_columns,
        }))
    }
}

fn root_scan(plan: &LogicalPlan) -> Option<&ScanNode> {
    match plan {
        LogicalPlan::Scan(scan) => Some(scan),
        LogicalPlan::Filter(filter) => root_scan(&filter.input),
        _ => None,
    }
}

fn project_referenced_side(
    items: &[ProjectItem],
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Option<Side> {
    let left_ids = collect_output_ids(left);
    let right_ids = collect_output_ids(right);
    let left_cols = collect_qualified_output_columns(left);
    let right_cols = collect_qualified_output_columns(right);
    let mut side = None;
    for item in items {
        if collect_column_id_refs(&item.expr).is_empty()
            && collect_qualified_column_refs(&item.expr).is_empty()
        {
            continue;
        }
        let reference_side =
            referenced_side(&item.expr, &left_ids, &right_ids, &left_cols, &right_cols)?;
        if let Some(existing) = side {
            if existing != reference_side {
                return None;
            }
        } else {
            side = Some(reference_side);
        }
    }
    side
}

fn join_equality_pairs(join: &JoinNode) -> Option<Vec<(String, String)>> {
    let condition = join.condition.as_ref()?;
    let left_ids = collect_output_ids(&join.left);
    let right_ids = collect_output_ids(&join.right);
    let left_cols = collect_qualified_output_columns(&join.left);
    let right_cols = collect_qualified_output_columns(&join.right);
    let mut pairs = Vec::new();
    collect_join_equality_pairs(
        condition,
        &left_ids,
        &right_ids,
        &left_cols,
        &right_cols,
        &mut pairs,
    )?;
    (!pairs.is_empty()).then_some(pairs)
}

fn collect_join_equality_pairs(
    expr: &TypedExpr,
    left_ids: &HashSet<crate::sql::column_id::ColumnId>,
    right_ids: &HashSet<crate::sql::column_id::ColumnId>,
    left_cols: &HashSet<(Option<String>, String)>,
    right_cols: &HashSet<(Option<String>, String)>,
    pairs: &mut Vec<(String, String)>,
) -> Option<()> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_join_equality_pairs(left, left_ids, right_ids, left_cols, right_cols, pairs)?;
            collect_join_equality_pairs(right, left_ids, right_ids, left_cols, right_cols, pairs)
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let left_ref = classify_column_ref(left, left_ids, right_ids, left_cols, right_cols)?;
            let right_ref = classify_column_ref(right, left_ids, right_ids, left_cols, right_cols)?;
            match (left_ref, right_ref) {
                ((Side::Left, left_col), (Side::Right, right_col)) => {
                    pairs.push((left_col, right_col));
                    Some(())
                }
                ((Side::Right, right_col), (Side::Left, left_col)) => {
                    pairs.push((left_col, right_col));
                    Some(())
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn referenced_side(
    expr: &TypedExpr,
    left_ids: &HashSet<crate::sql::column_id::ColumnId>,
    right_ids: &HashSet<crate::sql::column_id::ColumnId>,
    left_cols: &HashSet<(Option<String>, String)>,
    right_cols: &HashSet<(Option<String>, String)>,
) -> Option<Side> {
    let id_refs = collect_column_id_refs(expr);
    let qualified_refs = collect_qualified_column_refs(expr);
    if !id_refs.is_empty() && id_refs.len() == qualified_refs.len() {
        let mut side = None;
        let mut classified_all = true;
        for id in id_refs {
            let reference_side = match (left_ids.contains(&id), right_ids.contains(&id)) {
                (true, false) => Side::Left,
                (false, true) => Side::Right,
                _ => {
                    classified_all = false;
                    break;
                }
            };
            if let Some(existing) = side {
                if existing != reference_side {
                    return None;
                }
            } else {
                side = Some(reference_side);
            }
        }
        if classified_all {
            return side;
        }
    }

    let mut side = None;
    for reference in qualified_refs {
        let reference_side = match (
            left_cols.contains(&reference),
            right_cols.contains(&reference),
        ) {
            (true, false) => Side::Left,
            (false, true) => Side::Right,
            _ => return None,
        };
        if let Some(existing) = side {
            if existing != reference_side {
                return None;
            }
        } else {
            side = Some(reference_side);
        }
    }
    side
}

fn classify_column_ref(
    expr: &TypedExpr,
    left_ids: &HashSet<crate::sql::column_id::ColumnId>,
    right_ids: &HashSet<crate::sql::column_id::ColumnId>,
    left_cols: &HashSet<(Option<String>, String)>,
    right_cols: &HashSet<(Option<String>, String)>,
) -> Option<(Side, String)> {
    match &expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } => {
            if *column_id != crate::sql::column_id::ColumnId::UNSET {
                match (left_ids.contains(column_id), right_ids.contains(column_id)) {
                    (true, false) => return Some((Side::Left, normalize_identifier(column))),
                    (false, true) => return Some((Side::Right, normalize_identifier(column))),
                    _ => {}
                }
            }

            let reference = (
                qualifier.as_ref().map(|q| q.to_ascii_lowercase()),
                column.to_ascii_lowercase(),
            );
            match (
                left_cols.contains(&reference),
                right_cols.contains(&reference),
            ) {
                (true, false) => Some((Side::Left, normalize_identifier(column))),
                (false, true) => Some((Side::Right, normalize_identifier(column))),
                _ => None,
            }
        }
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => {
            classify_column_ref(expr, left_ids, right_ids, left_cols, right_cols)
        }
        _ => None,
    }
}

fn group_by_columns(group_by: &[TypedExpr]) -> Option<Vec<String>> {
    group_by
        .iter()
        .map(|expr| match &expr.kind {
            ExprKind::ColumnRef { column, .. } => Some(normalize_identifier(column)),
            _ => None,
        })
        .collect()
}

fn table_has_unique_key(scan: &ScanNode, columns: &[String]) -> bool {
    unique_constraints(scan)
        .into_iter()
        .any(|constraint| same_columns(&constraint, columns))
}

fn foreign_key_matches(
    local_scan: &ScanNode,
    referenced_scan: &ScanNode,
    local_columns: &[String],
    referenced_columns: &[String],
) -> bool {
    if !table_has_unique_key(referenced_scan, referenced_columns) {
        return false;
    }
    foreign_key_constraints(local_scan).into_iter().any(|fk| {
        same_columns(&fk.local_columns, local_columns)
            && table_name_matches(referenced_scan, &fk.referenced_table)
            && same_columns(&fk.referenced_columns, referenced_columns)
    })
}

fn unique_constraints(scan: &ScanNode) -> Vec<Vec<String>> {
    let Some(value) = table_properties(scan).remove("unique_constraints") else {
        return Vec::new();
    };
    value.split(';').filter_map(parse_column_list).collect()
}

fn foreign_key_constraints(scan: &ScanNode) -> Vec<ForeignKeyConstraint> {
    let Some(value) = table_properties(scan).remove("foreign_key_constraints") else {
        return Vec::new();
    };
    value
        .split(';')
        .filter_map(parse_foreign_key_constraint)
        .collect()
}

fn table_properties(scan: &ScanNode) -> HashMap<String, String> {
    let Some(serialized_metadata) =
        iceberg_table_info(&scan.table.source).and_then(|table| table.serialized_metadata.as_ref())
    else {
        return HashMap::new();
    };
    let Ok(metadata) = serde_json::from_str::<iceberg::spec::TableMetadata>(serialized_metadata)
    else {
        return HashMap::new();
    };
    metadata
        .properties()
        .iter()
        .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
        .collect()
}

fn iceberg_table_info(
    source: &crate::sql::catalog::ScanSource,
) -> Option<&crate::sql::catalog::IcebergTableInfo> {
    match source {
        crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. }
        | crate::sql::catalog::ScanSource::IcebergMetadataTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergDeltaTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergVersionTable { table, .. } => Some(table),
        crate::sql::catalog::ScanSource::StarRocks { .. }
        | crate::sql::catalog::ScanSource::IcebergMvTargetState { .. } => None,
    }
}

fn parse_foreign_key_constraint(raw: &str) -> Option<ForeignKeyConstraint> {
    let raw = raw.trim().trim_end_matches(';').trim();
    if raw.is_empty() {
        return None;
    }
    let references_idx = raw.to_ascii_lowercase().find("references")?;
    let left = raw[..references_idx].trim();
    let right = raw[references_idx + "references".len()..].trim();
    let local_columns = parse_column_list(left)?;
    let open = right.find('(')?;
    let referenced_table = normalize_table_name(&right[..open]);
    let referenced_columns = parse_column_list(right)?;
    if referenced_table.is_empty() || local_columns.is_empty() || referenced_columns.is_empty() {
        return None;
    }
    Some(ForeignKeyConstraint {
        local_columns,
        referenced_table,
        referenced_columns,
    })
}

fn parse_column_list(raw: &str) -> Option<Vec<String>> {
    let segment = if let Some(open) = raw.find('(') {
        let close = raw[open + 1..].find(')')? + open + 1;
        &raw[open + 1..close]
    } else {
        raw
    };
    let columns = segment
        .split(',')
        .map(normalize_identifier)
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>();
    (!columns.is_empty()).then_some(columns)
}

fn same_columns(left: &[String], right: &[String]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let left: HashSet<&str> = left.iter().map(String::as_str).collect();
    right.iter().all(|column| left.contains(column.as_str()))
}

fn table_name_matches(scan: &ScanNode, raw_table: &str) -> bool {
    let table = normalize_table_name(raw_table);
    if table.eq_ignore_ascii_case(&scan.table.name) {
        return true;
    }
    scan.alias
        .as_ref()
        .is_some_and(|alias| table.eq_ignore_ascii_case(alias))
}

fn normalize_identifier(raw: &str) -> String {
    let trimmed = raw
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'');
    let leaf = trimmed.rsplit('.').next().unwrap_or(trimmed);
    leaf.trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .to_ascii_lowercase()
}

fn normalize_table_name(raw: &str) -> String {
    normalize_identifier(raw)
}

fn add_not_null_filter(plan: LogicalPlan, scan: &ScanNode, columns: &[String]) -> LogicalPlan {
    let qualifier = scan
        .alias
        .clone()
        .unwrap_or_else(|| scan.table.name.clone());
    let predicates = columns
        .iter()
        .filter_map(|column| {
            scan.columns
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
                .map(|output| TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: false,
                    kind: ExprKind::IsNull {
                        expr: Box::new(TypedExpr {
                            data_type: output.data_type.clone(),
                            nullable: output.nullable,
                            kind: ExprKind::ColumnRef {
                                column_id: crate::sql::column_id::ColumnId::UNSET,
                                qualifier: Some(qualifier.clone()),
                                column: output.name.clone(),
                            },
                        }),
                        negated: true,
                    },
                })
        })
        .collect::<Vec<_>>();
    if predicates.is_empty() {
        return plan;
    }
    LogicalPlan::Filter(FilterNode {
        input: Box::new(plan),
        predicate: combine_and(predicates),
        required_output_columns: None,
    })
}

fn is_eliminable_count(aggregate: &AggregateCall) -> bool {
    aggregate.name.eq_ignore_ascii_case("count")
        && !aggregate.distinct
        && aggregate.order_by.is_empty()
        && aggregate.args.iter().all(|arg| {
            matches!(
                arg.kind,
                ExprKind::Literal(LiteralValue::Int(_)) | ExprKind::Literal(LiteralValue::Null)
            )
        })
}

fn rewrite_eliminated_aggregate_project_item(item: ProjectItem) -> Option<ProjectItem> {
    let expr = rewrite_eliminated_aggregate_expr(item.expr)?;
    Some(ProjectItem { expr, ..item })
}

fn rewrite_eliminated_aggregate_expr(expr: TypedExpr) -> Option<TypedExpr> {
    match expr.kind {
        ExprKind::AggregateCall {
            name,
            distinct,
            order_by,
            ..
        } if name.eq_ignore_ascii_case("count") && !distinct && order_by.is_empty() => {
            Some(TypedExpr {
                data_type: expr.data_type,
                nullable: false,
                kind: ExprKind::Literal(LiteralValue::Int(1)),
            })
        }
        _ if !contains_aggregate(&expr) => Some(expr),
        _ => None,
    }
}

fn contains_aggregate(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::AggregateCall { .. } => true,
        ExprKind::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Nested(expr) => contains_aggregate(expr),
        ExprKind::FunctionCall { args, .. } => args.iter().any(contains_aggregate),
        ExprKind::LambdaFunction { body, .. } => contains_aggregate(body),
        ExprKind::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        ExprKind::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| contains_aggregate(expr))
                || when_then
                    .iter()
                    .any(|(when, then)| contains_aggregate(when) || contains_aggregate(then))
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| contains_aggregate(expr))
        }
        ExprKind::IsTruthValue { expr, .. } => contains_aggregate(expr),
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(contains_aggregate)
                || partition_by.iter().any(contains_aggregate)
                || order_by.iter().any(|item| contains_aggregate(&item.expr))
        }
        ExprKind::Lambda { body, .. } => contains_aggregate(body),
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => false,
    }
}
