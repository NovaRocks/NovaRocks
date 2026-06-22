//! Common-subexpression elimination (CSE v1): a post-CBO physical-tree pass that
//! detects repeated non-trivial scalar subexpressions within an operator's
//! expression set and materializes each as a Project output column computed once,
//! rewriting consumers to reference it by ColumnId.
//!
//! See docs/design/specs/2026-06-21-optimizer-cse-v1-design.md.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::common::OutputColumn;
use crate::sql::optimizer::operator::{Operator, ScalarProjectItem};
use crate::sql::optimizer::options::OptimizerOptions;
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::scalar_expr;

/// Stable rule name for `SET disable_optimizer_rules`.
pub(crate) const CSE_RULE: &str = "CommonSubexpressionReuse";

/// Entry point: rewrite the physical tree in place. Gated by `CSE_RULE`.
pub(crate) fn rewrite(
    root: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
    options: &OptimizerOptions,
) {
    if !options.is_enabled(CSE_RULE) {
        return;
    }
    rewrite_node(root, scalars, factory);
}

/// Post-order walk. Per-operator drivers are added in later tasks.
fn rewrite_node(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    for child in &mut node.children {
        rewrite_node(child, scalars, factory);
    }
    match &node.op {
        Operator::PhysicalProject(_) => rewrite_project(node, scalars, factory),
        Operator::PhysicalFilter(_) => rewrite_filter(node, scalars, factory),
        Operator::PhysicalHashAggregate(_) => rewrite_aggregate(node, scalars, factory),
        Operator::PhysicalHashJoin(_) | Operator::PhysicalNestLoopJoin(_) => {
            rewrite_join(node, scalars, factory)
        }
        Operator::PhysicalSort(_) => rewrite_sort(node, scalars, factory),
        Operator::PhysicalTopN(_) => rewrite_topn(node, scalars, factory),
        Operator::PhysicalWindow(_) => rewrite_window(node, scalars, factory),
        _ => {}
    }
}

fn child_ids(scalars: &ScalarArena, id: ScalarId) -> Vec<ScalarId> {
    match scalars.node(id) {
        ScalarNode::BinaryOp { left, right, .. } => vec![*left, *right],
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => vec![*child],
        ScalarNode::FunctionCall { args, .. } => args.clone(),
        ScalarNode::AggregateCall { args, order_by, .. } => {
            let mut children = Vec::with_capacity(args.len() + order_by.len());
            children.extend(args.iter().copied());
            children.extend(order_by.iter().map(|key| key.expr));
            children
        }
        ScalarNode::InList { child, list, .. } => {
            let mut children = Vec::with_capacity(1 + list.len());
            children.push(*child);
            children.extend(list.iter().copied());
            children
        }
        ScalarNode::Between {
            child, low, high, ..
        } => vec![*child, *low, *high],
        ScalarNode::Like { child, pattern, .. } => vec![*child, *pattern],
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut children = Vec::with_capacity(operand.iter().count() + when_then.len() * 2 + 1);
            children.extend(operand.iter().copied());
            for (when, then) in when_then {
                children.push(*when);
                children.push(*then);
            }
            children.extend(else_expr.iter().copied());
            children
        }
        ScalarNode::ColumnRef(_)
        | ScalarNode::Literal(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::WindowCall { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::LambdaFunction { .. } => Vec::new(),
    }
}

fn count_subexprs(scalars: &ScalarArena, roots: &[ScalarId]) -> HashMap<ScalarId, usize> {
    let mut counts = HashMap::new();
    for &root in roots {
        count_subexprs_inner(scalars, root, &mut counts);
    }
    counts
}

fn count_subexprs_inner(
    scalars: &ScalarArena,
    id: ScalarId,
    counts: &mut HashMap<ScalarId, usize>,
) {
    *counts.entry(id).or_default() += 1;
    for child in child_ids(scalars, id) {
        count_subexprs_inner(scalars, child, counts);
    }
}

fn subtree_size(scalars: &ScalarArena, id: ScalarId) -> usize {
    1 + child_ids(scalars, id)
        .into_iter()
        .map(|child| subtree_size(scalars, child))
        .sum::<usize>()
}

fn eligible(scalars: &ScalarArena, id: ScalarId) -> bool {
    match scalars.node(id) {
        ScalarNode::ColumnRef(_)
        | ScalarNode::Literal(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::WindowCall { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::LambdaFunction { .. } => false,
        ScalarNode::Cast { child, .. } => {
            !matches!(scalars.node(*child), ScalarNode::ColumnRef(_))
                && !scalar_expr::contains_non_deterministic_function(scalars, id)
        }
        _ => !scalar_expr::contains_non_deterministic_function(scalars, id),
    }
}

fn cse_semantic_guard_ids(scalars: &ScalarArena, roots: &[ScalarId]) -> HashSet<ScalarId> {
    let mut guards = HashSet::new();
    for &root in roots {
        collect_cse_semantic_guard_ids(scalars, root, &mut guards);
    }
    guards
}

fn is_complex_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    )
}

fn collect_cse_semantic_guard_ids(
    scalars: &ScalarArena,
    id: ScalarId,
    guards: &mut HashSet<ScalarId>,
) {
    if let ScalarNode::FunctionCall { name, args, .. } = scalars.node(id)
        && name == "time_to_sec"
    {
        for &arg in args {
            guard_sec_to_time_source_path(scalars, arg, guards);
        }
    }
    if let ScalarNode::InList { child, .. } = scalars.node(id)
        && is_complex_data_type(scalars.data_type(*child))
        && !scalar_contains_column_ref(scalars, *child)
    {
        guard_subtree(scalars, *child, guards);
    }
    for child in child_ids(scalars, id) {
        collect_cse_semantic_guard_ids(scalars, child, guards);
    }
}

fn scalar_contains_column_ref(scalars: &ScalarArena, id: ScalarId) -> bool {
    match scalars.node(id) {
        ScalarNode::ColumnRef(_) => true,
        _ => child_ids(scalars, id)
            .into_iter()
            .any(|child| scalar_contains_column_ref(scalars, child)),
    }
}

fn guard_subtree(scalars: &ScalarArena, id: ScalarId, guards: &mut HashSet<ScalarId>) {
    if !guards.insert(id) {
        return;
    }
    for child in child_ids(scalars, id) {
        guard_subtree(scalars, child, guards);
    }
}

fn guard_sec_to_time_source_path(
    scalars: &ScalarArena,
    id: ScalarId,
    guards: &mut HashSet<ScalarId>,
) {
    match scalars.node(id) {
        ScalarNode::FunctionCall { name, .. } if name == "sec_to_time" => {
            guards.insert(id);
        }
        ScalarNode::Cast { child, .. } | ScalarNode::Nested(child) => {
            guards.insert(id);
            guard_sec_to_time_source_path(scalars, *child, guards);
        }
        _ => {}
    }
}

fn first_seen_order(scalars: &ScalarArena, roots: &[ScalarId]) -> HashMap<ScalarId, usize> {
    let mut order = HashMap::new();
    let mut next = 0;
    for &root in roots {
        record_first_seen(scalars, root, &mut order, &mut next);
    }
    order
}

fn pick_commons(scalars: &ScalarArena, roots: &[ScalarId]) -> Vec<ScalarId> {
    let counts = count_subexprs(scalars, roots);
    let first_seen = first_seen_order(scalars, roots);
    let semantic_guards = cse_semantic_guard_ids(scalars, roots);
    let mut candidates = counts
        .into_iter()
        .filter_map(|(id, count)| {
            if count >= 2 && !semantic_guards.contains(&id) && eligible(scalars, id) {
                Some(id)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|&id| {
        (
            subtree_size(scalars, id),
            first_seen.get(&id).copied().unwrap_or(usize::MAX),
        )
    });
    candidates
}

fn record_first_seen(
    scalars: &ScalarArena,
    id: ScalarId,
    order: &mut HashMap<ScalarId, usize>,
    next: &mut usize,
) {
    if order.contains_key(&id) {
        return;
    }
    order.insert(id, *next);
    *next += 1;
    for child in child_ids(scalars, id) {
        record_first_seen(scalars, child, order, next);
    }
}

fn collect_column_refs(
    scalars: &ScalarArena,
    roots: &[ScalarId],
) -> Vec<(ColumnId, DataType, bool)> {
    let mut seen = HashSet::new();
    let mut refs = Vec::new();
    for &root in roots {
        collect_column_refs_inner(scalars, root, &mut seen, &mut refs);
    }
    refs
}

fn collect_column_refs_inner(
    scalars: &ScalarArena,
    id: ScalarId,
    seen: &mut HashSet<ColumnId>,
    refs: &mut Vec<(ColumnId, DataType, bool)>,
) {
    match scalars.node(id) {
        ScalarNode::ColumnRef(column_id) => {
            if seen.insert(*column_id) {
                refs.push((
                    *column_id,
                    scalars.data_type(id).clone(),
                    scalars.nullable(id),
                ));
            }
        }
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_column_refs_inner(scalars, *left, seen, refs);
            collect_column_refs_inner(scalars, *right, seen, refs);
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => collect_column_refs_inner(scalars, *child, seen, refs),
        ScalarNode::FunctionCall { args, .. } => {
            for &arg in args {
                collect_column_refs_inner(scalars, arg, seen, refs);
            }
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            for &arg in args {
                collect_column_refs_inner(scalars, arg, seen, refs);
            }
            for key in order_by {
                collect_column_refs_inner(scalars, key.expr, seen, refs);
            }
        }
        ScalarNode::InList { child, list, .. } => {
            collect_column_refs_inner(scalars, *child, seen, refs);
            for &item in list {
                collect_column_refs_inner(scalars, item, seen, refs);
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            collect_column_refs_inner(scalars, *child, seen, refs);
            collect_column_refs_inner(scalars, *low, seen, refs);
            collect_column_refs_inner(scalars, *high, seen, refs);
        }
        ScalarNode::Like { child, pattern, .. } => {
            collect_column_refs_inner(scalars, *child, seen, refs);
            collect_column_refs_inner(scalars, *pattern, seen, refs);
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_column_refs_inner(scalars, *operand, seen, refs);
            }
            for (when, then) in when_then {
                collect_column_refs_inner(scalars, *when, seen, refs);
                collect_column_refs_inner(scalars, *then, seen, refs);
            }
            if let Some(else_expr) = else_expr {
                collect_column_refs_inner(scalars, *else_expr, seen, refs);
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for &arg in args {
                collect_column_refs_inner(scalars, arg, seen, refs);
            }
            for &partition in partition_by {
                collect_column_refs_inner(scalars, partition, seen, refs);
            }
            for key in order_by {
                collect_column_refs_inner(scalars, key.expr, seen, refs);
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            collect_column_refs_inner(scalars, *body, seen, refs);
        }
        ScalarNode::Literal(_) | ScalarNode::LambdaParamRef { .. } => {}
    }
}

fn substitute(
    scalars: &mut ScalarArena,
    id: ScalarId,
    subst: &HashMap<ScalarId, ScalarId>,
) -> ScalarId {
    if let Some(&replacement) = subst.get(&id) {
        return replacement;
    }

    let node = scalars.node(id).clone();
    let data_type = scalars.data_type(id).clone();
    let nullable = scalars.nullable(id);
    let rewritten = match node {
        ScalarNode::BinaryOp { op, left, right } => ScalarNode::BinaryOp {
            op,
            left: substitute(scalars, left, subst),
            right: substitute(scalars, right, subst),
        },
        ScalarNode::UnaryOp { op, child } => ScalarNode::UnaryOp {
            op,
            child: substitute(scalars, child, subst),
        },
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
        } => ScalarNode::FunctionCall {
            name,
            args: args
                .into_iter()
                .map(|arg| substitute(scalars, arg, subst))
                .collect(),
            distinct,
        },
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => ScalarNode::AggregateCall {
            name,
            args: args
                .into_iter()
                .map(|arg| substitute(scalars, arg, subst))
                .collect(),
            distinct,
            order_by: order_by
                .into_iter()
                .map(|mut key| {
                    key.expr = substitute(scalars, key.expr, subst);
                    key
                })
                .collect(),
        },
        ScalarNode::Cast { child, target } => ScalarNode::Cast {
            child: substitute(scalars, child, subst),
            target,
        },
        ScalarNode::IsNull { child, negated } => ScalarNode::IsNull {
            child: substitute(scalars, child, subst),
            negated,
        },
        ScalarNode::InList {
            child,
            list,
            negated,
        } => ScalarNode::InList {
            child: substitute(scalars, child, subst),
            list: list
                .into_iter()
                .map(|item| substitute(scalars, item, subst))
                .collect(),
            negated,
        },
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => ScalarNode::Between {
            child: substitute(scalars, child, subst),
            low: substitute(scalars, low, subst),
            high: substitute(scalars, high, subst),
            negated,
        },
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => ScalarNode::Like {
            child: substitute(scalars, child, subst),
            pattern: substitute(scalars, pattern, subst),
            negated,
        },
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => ScalarNode::Case {
            operand: operand.map(|operand| substitute(scalars, operand, subst)),
            when_then: when_then
                .into_iter()
                .map(|(when, then)| {
                    (
                        substitute(scalars, when, subst),
                        substitute(scalars, then, subst),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|else_expr| substitute(scalars, else_expr, subst)),
        },
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => ScalarNode::IsTruthValue {
            child: substitute(scalars, child, subst),
            value,
            negated,
        },
        ScalarNode::Nested(child) => ScalarNode::Nested(substitute(scalars, child, subst)),
        ScalarNode::ColumnRef(_)
        | ScalarNode::Literal(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::WindowCall { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::LambdaFunction { .. } => return id,
    };

    scalars.intern(rewritten, data_type, nullable)
}

fn build_commons(
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
    commons: &[ScalarId],
) -> (Vec<ScalarProjectItem>, HashMap<ScalarId, ScalarId>) {
    let mut items = Vec::with_capacity(commons.len());
    let mut subst = HashMap::new();

    for &common in commons {
        let expr = common;
        let data_type = scalars.data_type(common).clone();
        let nullable = scalars.nullable(common);
        let output_name = format!("__cse_{}", items.len());
        let output_column_id =
            factory.create(None, output_name.clone(), data_type.clone(), nullable);
        scalars.remember_project_output_display(output_column_id, None, output_name.clone());
        items.push(ScalarProjectItem {
            expr,
            output_name,
            output_column_id,
            expr_display: None,
        });

        let replacement =
            scalars.intern(ScalarNode::ColumnRef(output_column_id), data_type, nullable);
        subst.insert(common, replacement);
    }

    (items, subst)
}

fn output_column_for_project_item(scalars: &ScalarArena, item: &ScalarProjectItem) -> OutputColumn {
    OutputColumn {
        column_id: item.output_column_id,
        name: item.output_name.clone(),
        data_type: scalars.data_type(item.expr).clone(),
        nullable: scalars.nullable(item.expr),
        is_internal: true,
    }
}

fn repeat_virtual_output_columns(node: &PhysicalPlanNode) -> Vec<OutputColumn> {
    let Operator::PhysicalRepeat(repeat) = &node.op else {
        return Vec::new();
    };
    repeat
        .grouping_fn_ids
        .iter()
        .map(|(name, column_id)| OutputColumn {
            column_id: *column_id,
            name: name.clone(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        })
        .collect()
}

fn available_output_ids(node: &PhysicalPlanNode) -> HashSet<ColumnId> {
    match &node.op {
        Operator::PhysicalScan(scan) => {
            let required = scan
                .required_columns
                .as_ref()
                .map(|columns| columns.iter().map(String::as_str).collect::<HashSet<_>>());
            scan.columns
                .iter()
                .filter(|column| match &required {
                    Some(required) => required.contains(column.name.as_str()),
                    None => true,
                })
                .map(|column| column.column_id)
                .collect()
        }
        Operator::PhysicalValues(values) => values
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
        Operator::PhysicalGenerateSeries(generate_series) => {
            HashSet::from([generate_series.output_column_id])
        }
        Operator::PhysicalProject(project) => project
            .items
            .iter()
            .map(|item| item.output_column_id)
            .collect(),
        Operator::PhysicalFilter(_)
        | Operator::PhysicalSort(_)
        | Operator::PhysicalLimit(_)
        | Operator::PhysicalTopN(_)
        | Operator::PhysicalDistribution(_)
        | Operator::PhysicalAssertOneRow(_) => node
            .children
            .first()
            .map(available_output_ids)
            .unwrap_or_default(),
        Operator::PhysicalHashAggregate(aggregate) => aggregate
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
        Operator::PhysicalWindow(window) => window
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
        Operator::PhysicalHashJoin(_) | Operator::PhysicalNestLoopJoin(_) => {
            let child_ids = node
                .children
                .iter()
                .flat_map(available_output_ids)
                .collect::<HashSet<_>>();
            let declared = node
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .filter(|column_id| child_ids.contains(column_id))
                .collect::<HashSet<_>>();
            if declared.is_empty() {
                child_ids
            } else {
                declared
            }
        }
        Operator::PhysicalTableFunction(table_function) => {
            let mut ids = node
                .children
                .first()
                .map(available_output_ids)
                .unwrap_or_default();
            ids.extend(
                table_function
                    .output_columns
                    .iter()
                    .map(|column| column.column_id),
            );
            ids
        }
        Operator::PhysicalRepeat(repeat) => {
            let mut ids = node
                .children
                .first()
                .map(available_output_ids)
                .unwrap_or_default();
            ids.extend(
                repeat
                    .grouping_fn_ids
                    .iter()
                    .map(|(_, column_id)| *column_id),
            );
            ids
        }
        Operator::PhysicalDecode(decode) => decode
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
        Operator::PhysicalUnion(union) => union
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
        Operator::PhysicalIntersect(intersect) => intersect
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
        Operator::PhysicalExcept(except) => except
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
        Operator::PhysicalCTEProduce(produce) => {
            if produce.output_columns.is_empty() {
                node.children
                    .first()
                    .map(available_output_ids)
                    .unwrap_or_default()
            } else {
                produce
                    .output_columns
                    .iter()
                    .map(|column| column.column_id)
                    .collect()
            }
        }
        Operator::PhysicalCTEConsume(consume) => consume
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
        Operator::PhysicalCTEAnchor(_) => node
            .children
            .get(1)
            .map(available_output_ids)
            .unwrap_or_default(),
        Operator::PhysicalAggregateStateMerge(merge) => merge
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
        _ => node
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect(),
    }
}

fn prelude_binds_to_outputs(
    scalars: &ScalarArena,
    prelude: &[ScalarProjectItem],
    output_columns: &[OutputColumn],
) -> bool {
    let available = output_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<HashSet<_>>();
    let roots = prelude.iter().map(|item| item.expr).collect::<Vec<_>>();
    collect_column_refs(scalars, &roots)
        .into_iter()
        .all(|(column_id, _, _)| available.contains(&column_id))
}

fn wrap_project_around_child(
    child: &mut PhysicalPlanNode,
    prelude: Vec<ScalarProjectItem>,
    scalars: &mut ScalarArena,
) {
    let original = child.clone();
    let available = available_output_ids(&original);
    let mut passthrough_columns = original
        .output_columns
        .iter()
        .filter(|column| available.contains(&column.column_id))
        .cloned()
        .collect::<Vec<_>>();
    let mut seen_passthrough = passthrough_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<HashSet<_>>();
    for column in repeat_virtual_output_columns(&original) {
        if available.contains(&column.column_id) && seen_passthrough.insert(column.column_id) {
            passthrough_columns.push(column);
        }
    }
    let mut items = Vec::with_capacity(passthrough_columns.len() + prelude.len());
    for column in &passthrough_columns {
        let expr = scalars.intern(
            ScalarNode::ColumnRef(column.column_id),
            column.data_type.clone(),
            column.nullable,
        );
        items.push(ScalarProjectItem {
            expr,
            output_name: column.name.clone(),
            output_column_id: column.column_id,
            expr_display: None,
        });
    }
    items.extend(prelude.iter().cloned());

    let mut output_columns = passthrough_columns;
    output_columns.extend(
        prelude
            .iter()
            .map(|item| output_column_for_project_item(scalars, item)),
    );

    *child = PhysicalPlanNode {
        op: Operator::PhysicalProject(crate::sql::optimizer::operator::ProjectOp {
            items,
            output_qualifier: None,
        }),
        stats: original.stats.clone(),
        output_columns,
        execution_props: original.execution_props.clone(),
        children: vec![original],
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
    };
}

fn insert_or_reuse_project_below(
    child: &mut PhysicalPlanNode,
    prelude: Vec<ScalarProjectItem>,
    scalars: &mut ScalarArena,
) {
    if prelude.is_empty() {
        return;
    }

    let can_reuse_project = match &child.op {
        Operator::PhysicalProject(_) if child.children.len() == 1 => {
            prelude_binds_to_outputs(scalars, &prelude, &child.children[0].output_columns)
        }
        _ => false,
    };
    if can_reuse_project {
        if let Operator::PhysicalProject(project) = &mut child.op {
            child.output_columns.extend(
                prelude
                    .iter()
                    .map(|item| output_column_for_project_item(scalars, item)),
            );
            project.items.extend(prelude);
            return;
        }
    }

    wrap_project_around_child(child, prelude, scalars);
}

fn output_column_set(node: &PhysicalPlanNode) -> HashSet<ColumnId> {
    node.output_columns
        .iter()
        .map(|column| column.column_id)
        .collect()
}

fn side_subset(scalars: &ScalarArena, id: ScalarId, side_columns: &HashSet<ColumnId>) -> bool {
    let Some(refs) = scalar_expr::collect_column_ids_strict(scalars, id) else {
        return false;
    };
    !refs.is_empty()
        && refs
            .iter()
            .all(|column_id| side_columns.contains(column_id))
}

fn rewrite_project(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    let Operator::PhysicalProject(project) = &node.op else {
        return;
    };
    let roots = project
        .items
        .iter()
        .map(|item| item.expr)
        .collect::<Vec<_>>();
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() {
        return;
    }
    if node.children.len() != 1 {
        return;
    }

    let (prelude, subst) = build_commons(scalars, factory, &commons);
    let Operator::PhysicalProject(project) = &mut node.op else {
        unreachable!("checked project operator above");
    };
    for item in &mut project.items {
        item.expr = substitute(scalars, item.expr, &subst);
    }

    let input_refs = collect_column_refs(scalars, &roots);
    let child = node.children.remove(0);
    let mut child_project_items = input_refs
        .iter()
        .map(|&(column_id, ref data_type, nullable)| {
            let expr = scalars.intern(
                ScalarNode::ColumnRef(column_id),
                data_type.clone(),
                nullable,
            );
            let child_column = child
                .output_columns
                .iter()
                .find(|column| column.column_id == column_id);
            ScalarProjectItem {
                expr,
                output_name: child_column
                    .map(|column| column.name.clone())
                    .unwrap_or_else(|| column_id.to_string()),
                output_column_id: column_id,
                expr_display: None,
            }
        })
        .collect::<Vec<_>>();
    let mut child_project_output_columns = input_refs
        .iter()
        .map(|&(column_id, ref data_type, nullable)| {
            let child_column = child
                .output_columns
                .iter()
                .find(|column| column.column_id == column_id);
            OutputColumn {
                column_id,
                name: child_column
                    .map(|column| column.name.clone())
                    .unwrap_or_else(|| column_id.to_string()),
                data_type: child_column
                    .map(|column| column.data_type.clone())
                    .unwrap_or_else(|| data_type.clone()),
                nullable: child_column
                    .map(|column| column.nullable)
                    .unwrap_or(nullable),
                is_internal: child_column
                    .map(|column| column.is_internal)
                    .unwrap_or(false),
            }
        })
        .collect::<Vec<_>>();
    child_project_output_columns.extend(prelude.iter().map(|item| OutputColumn {
        column_id: item.output_column_id,
        name: item.output_name.clone(),
        data_type: scalars.data_type(item.expr).clone(),
        nullable: scalars.nullable(item.expr),
        is_internal: true,
    }));
    child_project_items.extend(prelude);

    let cse_project = PhysicalPlanNode {
        op: Operator::PhysicalProject(crate::sql::optimizer::operator::ProjectOp {
            items: child_project_items,
            output_qualifier: None,
        }),
        stats: child.stats.clone(),
        output_columns: child_project_output_columns,
        execution_props: child.execution_props.clone(),
        children: vec![child],
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
    };
    node.children.push(cse_project);
}

fn rewrite_filter(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    let Operator::PhysicalFilter(filter) = &node.op else {
        return;
    };
    let roots = [filter.predicate];
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() {
        return;
    }
    if node.children.len() != 1 {
        return;
    }

    let (prelude, subst) = build_commons(scalars, factory, &commons);
    let Operator::PhysicalFilter(filter) = &mut node.op else {
        unreachable!("checked filter operator above");
    };
    filter.predicate = substitute(scalars, filter.predicate, &subst);
    insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
}

fn rewrite_aggregate(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    if node.children.len() != 1 {
        return;
    }
    let Operator::PhysicalHashAggregate(aggregate) = &node.op else {
        return;
    };
    let mut roots = aggregate.group_by.clone();
    for (index, spec) in aggregate.aggregates.iter().enumerate() {
        if aggregate.is_merge.get(index).copied().unwrap_or(false) {
            continue;
        }
        roots.extend(spec.args.iter().copied());
        roots.extend(spec.order_by.iter().map(|key| key.expr));
    }
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() {
        return;
    }

    let (prelude, subst) = build_commons(scalars, factory, &commons);
    let Operator::PhysicalHashAggregate(aggregate) = &mut node.op else {
        unreachable!("checked aggregate operator above");
    };
    for group_by in &mut aggregate.group_by {
        *group_by = substitute(scalars, *group_by, &subst);
    }
    let is_merge = aggregate.is_merge.clone();
    for (index, spec) in aggregate.aggregates.iter_mut().enumerate() {
        if is_merge.get(index).copied().unwrap_or(false) {
            continue;
        }
        for arg in &mut spec.args {
            *arg = substitute(scalars, *arg, &subst);
        }
        for key in &mut spec.order_by {
            key.expr = substitute(scalars, key.expr, &subst);
        }
    }
    insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
}

fn rewrite_join(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    if node.children.len() != 2 {
        return;
    }
    let condition = match &node.op {
        Operator::PhysicalHashJoin(join) => join.other_condition,
        Operator::PhysicalNestLoopJoin(join) => join.condition,
        _ => return,
    };
    let Some(condition) = condition else {
        return;
    };

    let left_columns = output_column_set(&node.children[0]);
    let right_columns = output_column_set(&node.children[1]);
    let commons = pick_commons(scalars, &[condition]);
    let mut left_commons = Vec::new();
    let mut right_commons = Vec::new();
    for common in commons {
        match (
            side_subset(scalars, common, &left_columns),
            side_subset(scalars, common, &right_columns),
        ) {
            (true, false) => left_commons.push(common),
            (false, true) => right_commons.push(common),
            _ => {}
        }
    }
    if left_commons.is_empty() && right_commons.is_empty() {
        return;
    }

    let mut subst = HashMap::new();
    if !left_commons.is_empty() {
        let (prelude, side_subst) = build_commons(scalars, factory, &left_commons);
        subst.extend(side_subst);
        insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
    }
    if !right_commons.is_empty() {
        let (prelude, side_subst) = build_commons(scalars, factory, &right_commons);
        subst.extend(side_subst);
        insert_or_reuse_project_below(&mut node.children[1], prelude, scalars);
    }
    let new_condition = substitute(scalars, condition, &subst);
    match &mut node.op {
        Operator::PhysicalHashJoin(join) => join.other_condition = Some(new_condition),
        Operator::PhysicalNestLoopJoin(join) => join.condition = Some(new_condition),
        _ => unreachable!("checked join operator above"),
    }
}

fn rewrite_sort(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    if node.children.len() != 1 {
        return;
    }
    let Operator::PhysicalSort(sort) = &node.op else {
        return;
    };
    let mut roots = sort.items.iter().map(|key| key.expr).collect::<Vec<_>>();
    roots.extend(sort.analytic_partition_exprs.iter().copied());
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() {
        return;
    }

    let (prelude, subst) = build_commons(scalars, factory, &commons);
    let Operator::PhysicalSort(sort) = &mut node.op else {
        unreachable!("checked sort operator above");
    };
    for key in &mut sort.items {
        key.expr = substitute(scalars, key.expr, &subst);
    }
    for expr in &mut sort.analytic_partition_exprs {
        *expr = substitute(scalars, *expr, &subst);
    }
    insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
}

fn rewrite_topn(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    if node.children.len() != 1 {
        return;
    }
    let Operator::PhysicalTopN(topn) = &node.op else {
        return;
    };
    let roots = topn.items.iter().map(|key| key.expr).collect::<Vec<_>>();
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() {
        return;
    }

    let (prelude, subst) = build_commons(scalars, factory, &commons);
    let Operator::PhysicalTopN(topn) = &mut node.op else {
        unreachable!("checked topn operator above");
    };
    for key in &mut topn.items {
        key.expr = substitute(scalars, key.expr, &subst);
    }
    insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
}

fn rewrite_window(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    if node.children.len() != 1 {
        return;
    }
    let Operator::PhysicalWindow(window) = &node.op else {
        return;
    };
    let mut roots = Vec::new();
    for spec in &window.window_exprs {
        roots.extend(spec.args.iter().copied());
        roots.extend(spec.partition_by.iter().copied());
        roots.extend(spec.order_by.iter().map(|key| key.expr));
    }
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() {
        return;
    }

    let (prelude, subst) = build_commons(scalars, factory, &commons);
    let Operator::PhysicalWindow(window) = &mut node.op else {
        unreachable!("checked window operator above");
    };
    for spec in &mut window.window_exprs {
        for arg in &mut spec.args {
            *arg = substitute(scalars, *arg, &subst);
        }
        for partition in &mut spec.partition_by {
            *partition = substitute(scalars, *partition, &subst);
        }
        for key in &mut spec.order_by {
            key.expr = substitute(scalars, key.expr, &subst);
        }
    }
    insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Fields};
    use std::sync::Arc;

    use crate::sql::column_id::ColumnId;
    use crate::sql::common::OutputColumn;
    use crate::sql::common::{BinOp, JoinKind, LiteralValue};
    use crate::sql::optimizer::operator::{
        AggMode, FilterOp, JoinDistribution, Operator, PhysicalHashAggregateOp,
        PhysicalHashJoinEqCondition, PhysicalHashJoinOp, PhysicalNestLoopJoinOp, ProjectOp,
        RepeatOp, ScalarAggregateSpec, ScalarProjectItem, ScalarWindowSpec, SortOp, TopNOp,
        TopNPhase, ValuesOp, WindowOp,
    };
    use crate::sql::optimizer::physical_plan::{PhysicalPlanNode, PlanExecutionProps};
    use crate::sql::optimizer::scalar::{
        HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey,
    };
    use crate::sql::optimizer::statistics::Statistics;

    use super::pick_commons;

    fn col(arena: &mut ScalarArena, id: u32) -> ScalarId {
        arena.intern(ScalarNode::ColumnRef(ColumnId(id)), DataType::Int64, true)
    }

    fn add(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Add,
                left,
                right,
            },
            DataType::Int64,
            true,
        )
    }

    fn mul(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Mul,
                left,
                right,
            },
            DataType::Int64,
            true,
        )
    }

    fn gt(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Gt,
                left,
                right,
            },
            DataType::Boolean,
            true,
        )
    }

    fn lt(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Lt,
                left,
                right,
            },
            DataType::Boolean,
            true,
        )
    }

    fn and(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left,
                right,
            },
            DataType::Boolean,
            true,
        )
    }

    fn int_lit(arena: &mut ScalarArena, value: i64) -> ScalarId {
        arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
            DataType::Int64,
            false,
        )
    }

    fn call(arena: &mut ScalarArena, name: &str, args: Vec<ScalarId>) -> ScalarId {
        arena.intern(
            ScalarNode::FunctionCall {
                name: name.to_string(),
                args,
                distinct: false,
            },
            DataType::Int64,
            true,
        )
    }

    fn cast(arena: &mut ScalarArena, child: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::Cast {
                child,
                target: DataType::Int64,
            },
            DataType::Int64,
            true,
        )
    }

    fn project_item(expr: ScalarId, output_column_id: u32, output_name: &str) -> ScalarProjectItem {
        ScalarProjectItem {
            expr,
            output_name: output_name.to_string(),
            output_column_id: ColumnId(output_column_id),
            expr_display: None,
        }
    }

    fn output_column(column_id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(column_id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn values_node(columns: Vec<OutputColumn>) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: columns.clone(),
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    fn sort_key(expr: ScalarId) -> SortKey {
        SortKey {
            expr,
            asc: true,
            nulls_first: true,
            display: None,
        }
    }

    #[test]
    fn repeated_binary_op_is_common_candidate() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let a_plus_b = add(&mut arena, a, b);
        let root = add(&mut arena, a_plus_b, a);

        assert_eq!(pick_commons(&arena, &[a_plus_b, root]), vec![a_plus_b]);
    }

    #[test]
    fn repeated_columns_are_not_common_candidates() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let root = add(&mut arena, a, b);

        assert_eq!(pick_commons(&arena, &[a, root]), Vec::<ScalarId>::new());
    }

    #[test]
    fn volatile_functions_are_not_common_candidates() {
        let mut arena = ScalarArena::new();
        let rand = arena.intern(
            ScalarNode::FunctionCall {
                name: "rand".to_string(),
                args: vec![],
                distinct: false,
            },
            DataType::Float64,
            false,
        );

        assert_eq!(pick_commons(&arena, &[rand, rand]), Vec::<ScalarId>::new());
    }

    #[test]
    fn repeated_current_timestamp_is_not_common_candidate() {
        let mut arena = ScalarArena::new();
        let current_timestamp = call(&mut arena, "current_timestamp", vec![]);

        assert_eq!(
            pick_commons(&arena, &[current_timestamp, current_timestamp]),
            Vec::<ScalarId>::new()
        );
    }

    #[test]
    fn sec_to_time_source_for_time_to_sec_is_not_common_candidate() {
        let mut arena = ScalarArena::new();
        let minus_one = int_lit(&mut arena, -1);
        let sec_to_time = call(&mut arena, "sec_to_time", vec![minus_one]);
        let time_to_sec = call(&mut arena, "time_to_sec", vec![sec_to_time]);

        assert_eq!(
            pick_commons(&arena, &[sec_to_time, time_to_sec]),
            Vec::<ScalarId>::new()
        );
    }

    #[test]
    fn complex_literal_in_list_lhs_is_not_common_candidate() {
        let mut arena = ScalarArena::new();
        let null = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Null)),
            DataType::Null,
            true,
        );
        let null_array_type = DataType::List(Arc::new(Field::new("item", DataType::Null, true)));
        let null_array = arena.intern(
            ScalarNode::FunctionCall {
                name: "__array_literal".to_string(),
                args: vec![null],
                distinct: false,
            },
            null_array_type,
            false,
        );
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int32, true)),
                    Arc::new(Field::new("value", DataType::Int32, true)),
                ])),
                false,
            )),
            false,
        );
        let array_map_type = DataType::List(Arc::new(Field::new("item", map_type, true)));
        let cast_null_array = arena.intern(
            ScalarNode::Cast {
                child: null_array,
                target: array_map_type.clone(),
            },
            array_map_type.clone(),
            false,
        );
        let candidate = arena.intern(
            ScalarNode::ColumnRef(ColumnId(10)),
            array_map_type.clone(),
            true,
        );
        let in_list = arena.intern(
            ScalarNode::InList {
                child: cast_null_array,
                list: vec![candidate],
                negated: false,
            },
            DataType::Boolean,
            true,
        );
        let not_in_list = arena.intern(
            ScalarNode::InList {
                child: cast_null_array,
                list: vec![candidate],
                negated: true,
            },
            DataType::Boolean,
            true,
        );

        assert_eq!(
            pick_commons(&arena, &[in_list, not_in_list]),
            Vec::<ScalarId>::new()
        );
    }

    #[test]
    fn nested_non_deterministic_expression_is_not_common_candidate() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let rand = call(&mut arena, "rand", vec![]);
        let rand_plus_a = add(&mut arena, rand, a);

        assert_eq!(
            pick_commons(&arena, &[rand_plus_a, rand_plus_a]),
            Vec::<ScalarId>::new()
        );
    }

    #[test]
    fn repeated_cast_column_ref_is_not_common_candidate() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let cast_a = cast(&mut arena, a);

        assert_eq!(
            pick_commons(&arena, &[cast_a, cast_a]),
            Vec::<ScalarId>::new()
        );
    }

    #[test]
    fn equal_size_candidates_follow_first_seen_order() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let c = col(&mut arena, 3);
        let d = col(&mut arena, 4);
        let c_plus_d = add(&mut arena, c, d);
        let a_plus_b = add(&mut arena, a, b);

        assert_eq!(
            pick_commons(&arena, &[c_plus_d, a_plus_b, c_plus_d, a_plus_b]),
            vec![c_plus_d, a_plus_b]
        );
    }

    #[test]
    fn substitute_replaces_common_and_reinterns() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let a_plus_b = add(&mut arena, a, b);
        let cse_ref = arena.intern(ScalarNode::ColumnRef(ColumnId(99)), DataType::Int64, true);
        let mut subst = std::collections::HashMap::new();
        subst.insert(a_plus_b, cse_ref);

        let root = add(&mut arena, a_plus_b, a);
        let rewritten = super::substitute(&mut arena, root, &subst);

        match arena.node(rewritten) {
            ScalarNode::BinaryOp { left, right, .. } => {
                assert!(matches!(
                    arena.node(*left),
                    ScalarNode::ColumnRef(ColumnId(99))
                ));
                assert!(matches!(
                    arena.node(*right),
                    ScalarNode::ColumnRef(ColumnId(1))
                ));
            }
            other => panic!("unexpected node: {other:?}"),
        }
    }

    #[test]
    fn build_commons_keeps_prelude_items_independent() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let a_plus_b = add(&mut arena, a, b);
        let doubled = add(&mut arena, a_plus_b, a_plus_b);

        let (items, subst) = super::build_commons(&mut arena, &mut factory, &[a_plus_b, doubled]);

        assert_eq!(items.len(), 2);
        let first_cse = items[0].output_column_id;
        assert!(matches!(
            arena.node(items[1].expr),
            ScalarNode::BinaryOp { left, right, .. }
                if !matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == first_cse)
                    && !matches!(arena.node(*right), ScalarNode::ColumnRef(column_id) if *column_id == first_cse)
        ));
        assert!(matches!(
            arena.node(*subst.get(&doubled).expect("doubled replacement")),
            ScalarNode::ColumnRef(column_id) if *column_id == items[1].output_column_id
        ));
    }

    #[test]
    fn collect_column_refs_includes_lambda_captures() {
        let mut arena = ScalarArena::new();
        let captured = col(&mut arena, 1);
        let lambda_param = arena.intern(
            ScalarNode::LambdaParamRef {
                name: "x".to_string(),
                slot_id: 7,
            },
            DataType::Int64,
            true,
        );
        let body = add(&mut arena, lambda_param, captured);
        let lambda = arena.intern(
            ScalarNode::Lambda {
                params: vec!["x".to_string()],
                body,
            },
            DataType::Int64,
            true,
        );

        let refs = super::collect_column_refs(&arena, &[lambda]);

        assert_eq!(
            refs.into_iter()
                .map(|(column_id, _, _)| column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1)]
        );
    }

    #[test]
    fn rewrite_project_factors_repeated_subexpr() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_plus_b = add(&mut arena, a, b);
        let doubled = add(&mut arena, a_plus_b, a_plus_b);
        let child = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: 42.0,
                ..Statistics::default()
            },
            output_columns: vec![
                output_column(101, "a"),
                output_column(102, "b"),
                OutputColumn {
                    is_internal: true,
                    ..output_column(199, "__stale_internal")
                },
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![
                    project_item(a_plus_b, 110, "x"),
                    project_item(doubled, 111, "y"),
                ],
                output_qualifier: None,
            }),
            children: vec![child],
            stats: Statistics {
                output_row_count: 7.0,
                ..Statistics::default()
            },
            output_columns: vec![output_column(110, "x"), output_column(111, "y")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(project) = &node.op else {
            panic!("expected physical project");
        };
        assert_eq!(project.items.len(), 2);
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"]
        );
        let Operator::PhysicalProject(cse_project) = &node.children[0].op else {
            panic!("expected inserted CSE project");
        };
        assert_eq!(cse_project.items[2].output_name, "__cse_0");
        let common_col = cse_project.items[2].output_column_id;
        assert!(matches!(
            arena.node(cse_project.items[2].expr),
            ScalarNode::BinaryOp { .. }
        ));
        assert!(matches!(
            arena.node(project.items[0].expr),
            ScalarNode::ColumnRef(column_id) if *column_id == common_col
        ));
        assert!(matches!(
            arena.node(project.items[1].expr),
            ScalarNode::BinaryOp { left, right, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == common_col)
                    && matches!(arena.node(*right), ScalarNode::ColumnRef(column_id) if *column_id == common_col)
        ));
        assert_eq!(
            node.output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"],
            "Project node output_columns remains the visible result contract"
        );
        assert_eq!(
            node.children[0]
                .output_columns
                .iter()
                .map(|column| (column.name.as_str(), column.is_internal))
                .collect::<Vec<_>>(),
            vec![("a", false), ("b", false), ("__cse_0", true)]
        );
        assert_eq!(node.children[0].stats.output_row_count, 42.0);
    }

    #[test]
    fn rewrite_project_preserves_lambda_capture_input() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let c = col(&mut arena, 103);
        let b_plus_c = add(&mut arena, b, c);
        let doubled = add(&mut arena, b_plus_c, b_plus_c);
        let lambda_param = arena.intern(
            ScalarNode::LambdaParamRef {
                name: "x".to_string(),
                slot_id: 7,
            },
            DataType::Int64,
            true,
        );
        let lambda_body = add(&mut arena, lambda_param, a);
        let lambda = arena.intern(
            ScalarNode::Lambda {
                params: vec!["x".to_string()],
                body: lambda_body,
            },
            DataType::Int64,
            true,
        );
        let child = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![
                    output_column(101, "a"),
                    output_column(102, "b"),
                    output_column(103, "c"),
                ],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![
                output_column(101, "a"),
                output_column(102, "b"),
                output_column(103, "c"),
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![
                    project_item(b_plus_c, 110, "x"),
                    project_item(doubled, 111, "y"),
                    project_item(lambda, 112, "lambda_capture"),
                ],
                output_qualifier: None,
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![
                output_column(110, "x"),
                output_column(111, "y"),
                output_column(112, "lambda_capture"),
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        assert_eq!(
            node.children[0]
                .output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c", "a", "__cse_0"]
        );
    }

    #[test]
    fn insert_or_reuse_project_below_wraps_non_project_child() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_plus_b = add(&mut arena, a, b);
        let (prelude, _) = super::build_commons(&mut arena, &mut factory, &[a_plus_b]);
        let child = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: 42.0,
                ..Statistics::default()
            },
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut parent = PhysicalPlanNode {
            op: Operator::PhysicalFilter(FilterOp {
                predicate: gt(&mut arena, a, b),
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::insert_or_reuse_project_below(&mut parent.children[0], prelude, &mut arena);

        let Operator::PhysicalProject(project) = &parent.children[0].op else {
            panic!("expected inserted physical project");
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "__cse_0"]
        );
        assert_eq!(
            parent.children[0]
                .output_columns
                .iter()
                .map(|column| (column.name.as_str(), column.is_internal))
                .collect::<Vec<_>>(),
            vec![("a", false), ("b", false), ("__cse_0", true)]
        );
        assert_eq!(parent.children[0].children.len(), 1);
        assert_eq!(parent.children[0].stats.output_row_count, 42.0);
    }

    #[test]
    fn insert_or_reuse_project_below_drops_stale_passthrough_metadata() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_plus_b = add(&mut arena, a, b);
        let (prelude, _) = super::build_commons(&mut arena, &mut factory, &[a_plus_b]);
        let values = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut stale_filter = PhysicalPlanNode {
            op: Operator::PhysicalFilter(FilterOp {
                predicate: gt(&mut arena, a, b),
            }),
            children: vec![values],
            stats: Statistics::default(),
            output_columns: vec![
                output_column(101, "a"),
                output_column(102, "b"),
                output_column(999, "stale_not_in_child_scope"),
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::insert_or_reuse_project_below(&mut stale_filter, prelude, &mut arena);

        let Operator::PhysicalProject(project) = &stale_filter.op else {
            panic!("expected inserted physical project");
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "__cse_0"]
        );
        assert_eq!(
            stale_filter
                .output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "__cse_0"]
        );
    }

    #[test]
    fn insert_or_reuse_project_below_preserves_repeat_grouping_outputs() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_plus_b = add(&mut arena, a, b);
        let (prelude, _) = super::build_commons(&mut arena, &mut factory, &[a_plus_b]);
        let values = values_node(vec![output_column(101, "a"), output_column(102, "b")]);
        let mut repeat = PhysicalPlanNode {
            op: Operator::PhysicalRepeat(RepeatOp {
                repeat_column_ref_list: vec![vec!["a".to_string()]],
                repeat_column_ref_ids: vec![vec![ColumnId(101)]],
                grouping_ids: vec![0, 1],
                all_rollup_columns: vec!["a".to_string()],
                all_rollup_column_ids: vec![ColumnId(101)],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![("__grouping_fn_0".to_string(), vec!["a".to_string()])],
                grouping_fn_arg_ids: vec![vec![ColumnId(101)]],
                grouping_fn_ids: vec![("__grouping_fn_0".to_string(), ColumnId(109))],
            }),
            children: vec![values],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::insert_or_reuse_project_below(&mut repeat, prelude, &mut arena);

        let Operator::PhysicalProject(project) = &repeat.op else {
            panic!("expected inserted project above repeat");
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "__grouping_fn_0", "__cse_0"]
        );
        assert_eq!(
            repeat
                .output_columns
                .iter()
                .map(|column| {
                    (
                        column.column_id,
                        column.name.as_str(),
                        column.data_type.clone(),
                        column.nullable,
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (ColumnId(101), "a", DataType::Int64, true),
                (ColumnId(102), "b", DataType::Int64, true),
                (ColumnId(109), "__grouping_fn_0", DataType::Int64, false),
                (
                    project.items[3].output_column_id,
                    "__cse_0",
                    DataType::Int64,
                    true
                ),
            ]
        );
    }

    #[test]
    fn insert_or_reuse_project_below_wraps_project_when_producer_refs_project_outputs() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let x = col(&mut arena, 201);
        let y = col(&mut arena, 202);
        let x_plus_y = add(&mut arena, x, y);
        let (prelude, _) = super::build_commons(&mut arena, &mut factory, &[x_plus_y]);
        let values = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut child_project = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![project_item(a, 201, "x"), project_item(b, 202, "y")],
                output_qualifier: None,
            }),
            children: vec![values],
            stats: Statistics::default(),
            output_columns: vec![output_column(201, "x"), output_column(202, "y")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::insert_or_reuse_project_below(&mut child_project, prelude, &mut arena);

        let Operator::PhysicalProject(outer_project) = &child_project.op else {
            panic!("expected outer wrapper project");
        };
        assert_eq!(
            outer_project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y", "__cse_0"]
        );
        assert_eq!(
            child_project
                .output_columns
                .iter()
                .map(|column| (column.name.as_str(), column.is_internal))
                .collect::<Vec<_>>(),
            vec![("x", false), ("y", false), ("__cse_0", true)]
        );
        let Operator::PhysicalProject(inner_project) = &child_project.children[0].op else {
            panic!("expected original inner project");
        };
        assert_eq!(
            inner_project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"]
        );
    }

    #[test]
    fn insert_or_reuse_project_below_reuses_passthrough_project_when_producer_refs_input() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_plus_b = add(&mut arena, a, b);
        let (prelude, _) = super::build_commons(&mut arena, &mut factory, &[a_plus_b]);
        let values = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut child_project = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![project_item(a, 101, "a"), project_item(b, 102, "b")],
                output_qualifier: None,
            }),
            children: vec![values],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::insert_or_reuse_project_below(&mut child_project, prelude, &mut arena);

        let Operator::PhysicalProject(project) = &child_project.op else {
            panic!("expected reused physical project");
        };
        assert!(matches!(
            child_project.children[0].op,
            Operator::PhysicalValues(_)
        ));
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "__cse_0"]
        );
        assert_eq!(
            child_project
                .output_columns
                .iter()
                .map(|column| (column.name.as_str(), column.is_internal))
                .collect::<Vec<_>>(),
            vec![("a", false), ("b", false), ("__cse_0", true)]
        );
        let cse_expr = project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item")
            .expr;
        assert!(matches!(
            arena.node(cse_expr),
            ScalarNode::BinaryOp { left, right, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(ColumnId(101)))
                    && matches!(arena.node(*right), ScalarNode::ColumnRef(ColumnId(102)))
        ));
    }

    #[test]
    fn rewrite_filter_factors_repeated_predicate_subexpr() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_plus_b = add(&mut arena, a, b);
        let ten = int_lit(&mut arena, 10);
        let twenty = int_lit(&mut arena, 20);
        let lower = gt(&mut arena, a_plus_b, ten);
        let upper = lt(&mut arena, a_plus_b, twenty);
        let predicate = and(&mut arena, lower, upper);
        let child = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalFilter(FilterOp { predicate }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(cse_project) = &node.children[0].op else {
            panic!("expected inserted CSE project");
        };
        let cse_item = cse_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item");
        let cse_column = cse_item.output_column_id;
        assert!(
            node.children[0]
                .output_columns
                .iter()
                .any(|column| column.column_id == cse_column
                    && column.name == "__cse_0"
                    && column.is_internal)
        );
        let Operator::PhysicalFilter(filter) = &node.op else {
            panic!("expected physical filter");
        };
        let ScalarNode::BinaryOp { left, right, .. } = arena.node(filter.predicate) else {
            panic!("expected conjunction");
        };
        assert!(matches!(
            arena.node(*left),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
        assert!(matches!(
            arena.node(*right),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
        assert_eq!(
            node.output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn rewrite_filter_wraps_existing_project_when_predicate_refs_project_outputs() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let x = col(&mut arena, 201);
        let y = col(&mut arena, 202);
        let x_plus_y = add(&mut arena, x, y);
        let ten = int_lit(&mut arena, 10);
        let twenty = int_lit(&mut arena, 20);
        let lower = gt(&mut arena, x_plus_y, ten);
        let upper = lt(&mut arena, x_plus_y, twenty);
        let predicate = and(&mut arena, lower, upper);
        let values = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(101, "a"), output_column(102, "b")],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let project = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![project_item(a, 201, "x"), project_item(b, 202, "y")],
                output_qualifier: None,
            }),
            children: vec![values],
            stats: Statistics::default(),
            output_columns: vec![output_column(201, "x"), output_column(202, "y")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalFilter(FilterOp { predicate }),
            children: vec![project],
            stats: Statistics::default(),
            output_columns: vec![output_column(201, "x"), output_column(202, "y")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(outer_project) = &node.children[0].op else {
            panic!("expected outer CSE project");
        };
        assert_eq!(
            outer_project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y", "__cse_0"]
        );
        let cse_item = outer_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item");
        let cse_column = cse_item.output_column_id;
        assert_eq!(
            node.children[0]
                .output_columns
                .iter()
                .map(|column| (column.name.as_str(), column.is_internal))
                .collect::<Vec<_>>(),
            vec![("x", false), ("y", false), ("__cse_0", true)]
        );
        let Operator::PhysicalProject(inner_project) = &node.children[0].children[0].op else {
            panic!("expected original inner project");
        };
        assert_eq!(
            inner_project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"]
        );
        let Operator::PhysicalFilter(filter) = &node.op else {
            panic!("expected physical filter");
        };
        let ScalarNode::BinaryOp { left, right, .. } = arena.node(filter.predicate) else {
            panic!("expected conjunction");
        };
        assert!(matches!(
            arena.node(*left),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
        assert!(matches!(
            arena.node(*right),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
        assert_eq!(
            node.output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"]
        );
    }

    #[test]
    fn rewrite_aggregate_factors_repeated_aggregate_args() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_mul_b = mul(&mut arena, a, b);
        let child = values_node(vec![output_column(101, "a"), output_column(102, "b")]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: vec![],
                aggregates: vec![
                    ScalarAggregateSpec {
                        name: "sum".to_string(),
                        args: vec![a_mul_b],
                        distinct: false,
                        order_by: vec![],
                    },
                    ScalarAggregateSpec {
                        name: "avg".to_string(),
                        args: vec![a_mul_b],
                        distinct: false,
                        order_by: vec![],
                    },
                ],
                output_columns: vec![output_column(201, "sum_ab"), output_column(202, "avg_ab")],
                is_merge: vec![false, false],
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![output_column(201, "sum_ab"), output_column(202, "avg_ab")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(cse_project) = &node.children[0].op else {
            panic!("expected inserted CSE project below aggregate");
        };
        let cse_item = cse_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item");
        assert!(matches!(
            arena.node(cse_item.expr),
            ScalarNode::BinaryOp { op: BinOp::Mul, .. }
        ));
        let cse_column = cse_item.output_column_id;
        let Operator::PhysicalHashAggregate(aggregate) = &node.op else {
            panic!("expected physical aggregate");
        };
        for spec in &aggregate.aggregates {
            assert!(matches!(
                arena.node(spec.args[0]),
                ScalarNode::ColumnRef(column_id) if *column_id == cse_column
            ));
        }
    }

    #[test]
    fn rewrite_aggregate_does_not_factor_merge_aggregate_args() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_mul_b = mul(&mut arena, a, b);
        let child = values_node(vec![
            output_column(301, "sum_state"),
            output_column(302, "avg_state"),
        ]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Global,
                group_by: vec![],
                aggregates: vec![
                    ScalarAggregateSpec {
                        name: "sum".to_string(),
                        args: vec![a_mul_b],
                        distinct: false,
                        order_by: vec![],
                    },
                    ScalarAggregateSpec {
                        name: "avg".to_string(),
                        args: vec![a_mul_b],
                        distinct: false,
                        order_by: vec![],
                    },
                ],
                output_columns: vec![output_column(201, "sum_ab"), output_column(202, "avg_ab")],
                is_merge: vec![true, true],
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![output_column(201, "sum_ab"), output_column(202, "avg_ab")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        assert!(matches!(node.children[0].op, Operator::PhysicalValues(_)));
        let Operator::PhysicalHashAggregate(aggregate) = &node.op else {
            panic!("expected physical aggregate");
        };
        for spec in &aggregate.aggregates {
            assert_eq!(spec.args[0], a_mul_b);
        }
    }

    #[test]
    fn rewrite_aggregate_factors_repeated_order_by_exprs_without_rewriting_args() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_mul_b = mul(&mut arena, a, b);
        let child = values_node(vec![output_column(101, "a"), output_column(102, "b")]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: vec![],
                aggregates: vec![
                    ScalarAggregateSpec {
                        name: "array_agg".to_string(),
                        args: vec![a],
                        distinct: false,
                        order_by: vec![sort_key(a_mul_b)],
                    },
                    ScalarAggregateSpec {
                        name: "array_agg".to_string(),
                        args: vec![b],
                        distinct: false,
                        order_by: vec![sort_key(a_mul_b)],
                    },
                ],
                output_columns: vec![
                    output_column(201, "ordered_a"),
                    output_column(202, "ordered_b"),
                ],
                is_merge: vec![false, false],
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![
                output_column(201, "ordered_a"),
                output_column(202, "ordered_b"),
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(cse_project) = &node.children[0].op else {
            panic!("expected inserted CSE project below aggregate");
        };
        let cse_column = cse_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item")
            .output_column_id;
        let Operator::PhysicalHashAggregate(aggregate) = &node.op else {
            panic!("expected physical aggregate");
        };
        assert_eq!(aggregate.aggregates[0].args[0], a);
        assert_eq!(aggregate.aggregates[1].args[0], b);
        for spec in &aggregate.aggregates {
            assert!(matches!(
                arena.node(spec.order_by[0].expr),
                ScalarNode::ColumnRef(column_id) if *column_id == cse_column
            ));
        }
    }

    #[test]
    fn rewrite_sort_factors_items_and_analytic_partition_exprs() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_mul_b = mul(&mut arena, a, b);
        let child = values_node(vec![output_column(101, "a"), output_column(102, "b")]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalSort(SortOp {
                items: vec![sort_key(a_mul_b)],
                analytic_partition_exprs: vec![a_mul_b],
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(cse_project) = &node.children[0].op else {
            panic!("expected inserted CSE project below sort");
        };
        let cse_column = cse_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item")
            .output_column_id;
        let Operator::PhysicalSort(sort) = &node.op else {
            panic!("expected physical sort");
        };
        assert!(matches!(
            arena.node(sort.items[0].expr),
            ScalarNode::ColumnRef(column_id) if *column_id == cse_column
        ));
        assert!(matches!(
            arena.node(sort.analytic_partition_exprs[0]),
            ScalarNode::ColumnRef(column_id) if *column_id == cse_column
        ));
    }

    #[test]
    fn rewrite_topn_factors_repeated_sort_items() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_mul_b = mul(&mut arena, a, b);
        let child = values_node(vec![output_column(101, "a"), output_column(102, "b")]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalTopN(TopNOp {
                items: vec![sort_key(a_mul_b), sort_key(a_mul_b)],
                limit: Some(10),
                offset: None,
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "a"), output_column(102, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(cse_project) = &node.children[0].op else {
            panic!("expected inserted CSE project below topn");
        };
        let cse_column = cse_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item")
            .output_column_id;
        let Operator::PhysicalTopN(topn) = &node.op else {
            panic!("expected physical topn");
        };
        for item in &topn.items {
            assert!(matches!(
                arena.node(item.expr),
                ScalarNode::ColumnRef(column_id) if *column_id == cse_column
            ));
        }
    }

    #[test]
    fn rewrite_window_factors_args_partition_and_order_by() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let a = col(&mut arena, 101);
        let b = col(&mut arena, 102);
        let a_mul_b = mul(&mut arena, a, b);
        let child = values_node(vec![output_column(101, "a"), output_column(102, "b")]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalWindow(WindowOp {
                window_exprs: vec![ScalarWindowSpec {
                    name: "sum".to_string(),
                    args: vec![a_mul_b],
                    distinct: false,
                    partition_by: vec![a_mul_b],
                    order_by: vec![sort_key(a_mul_b)],
                    window_frame: None,
                    ignore_nulls: false,
                }],
                output_columns: vec![output_column(201, "win_sum")],
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![
                output_column(101, "a"),
                output_column(102, "b"),
                output_column(201, "win_sum"),
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(cse_project) = &node.children[0].op else {
            panic!("expected inserted CSE project below window");
        };
        let cse_column = cse_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("CSE project item")
            .output_column_id;
        let Operator::PhysicalWindow(window) = &node.op else {
            panic!("expected physical window");
        };
        let spec = &window.window_exprs[0];
        assert!(matches!(
            arena.node(spec.args[0]),
            ScalarNode::ColumnRef(column_id) if *column_id == cse_column
        ));
        assert!(matches!(
            arena.node(spec.partition_by[0]),
            ScalarNode::ColumnRef(column_id) if *column_id == cse_column
        ));
        assert!(matches!(
            arena.node(spec.order_by[0].expr),
            ScalarNode::ColumnRef(column_id) if *column_id == cse_column
        ));
    }

    #[test]
    fn rewrite_join_factors_left_only_condition_expr_to_left_child() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let left_a = col(&mut arena, 101);
        let right_b = col(&mut arena, 201);
        let two = int_lit(&mut arena, 2);
        let ten = int_lit(&mut arena, 10);
        let left_a_times_two = mul(&mut arena, left_a, two);
        let lower = gt(&mut arena, left_a_times_two, right_b);
        let upper_bound = add(&mut arena, right_b, ten);
        let upper = lt(&mut arena, left_a_times_two, upper_bound);
        let condition = and(&mut arena, lower, upper);
        let left = values_node(vec![output_column(101, "left_a")]);
        let right = values_node(vec![output_column(201, "right_b")]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left, right],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "left_a"), output_column(201, "right_b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        let Operator::PhysicalProject(left_project) = &node.children[0].op else {
            panic!("expected CSE project on left child");
        };
        assert!(matches!(node.children[1].op, Operator::PhysicalValues(_)));
        let cse_column = left_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("left CSE item")
            .output_column_id;
        let Operator::PhysicalNestLoopJoin(join) = &node.op else {
            panic!("expected nested loop join");
        };
        let ScalarNode::BinaryOp { left, right, .. } =
            arena.node(join.condition.expect("join condition"))
        else {
            panic!("expected conjunction");
        };
        assert!(matches!(
            arena.node(*left),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
        assert!(matches!(
            arena.node(*right),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
    }

    #[test]
    fn rewrite_join_factors_right_only_hash_join_condition_expr_to_right_child() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let left_a = col(&mut arena, 101);
        let right_b = col(&mut arena, 201);
        let right_key = col(&mut arena, 202);
        let two = int_lit(&mut arena, 2);
        let ten = int_lit(&mut arena, 10);
        let right_b_times_two = mul(&mut arena, right_b, two);
        let lower = gt(&mut arena, right_b_times_two, left_a);
        let upper_bound = add(&mut arena, left_a, ten);
        let upper = lt(&mut arena, right_b_times_two, upper_bound);
        let condition = and(&mut arena, lower, upper);
        let left = values_node(vec![output_column(101, "left_a")]);
        let right = values_node(vec![
            output_column(201, "right_b"),
            output_column(202, "right_k"),
        ]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_a,
                    right: right_key,
                    null_safe: false,
                }],
                other_condition: Some(condition),
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
            stats: Statistics::default(),
            output_columns: vec![
                output_column(101, "left_a"),
                output_column(201, "right_b"),
                output_column(202, "right_k"),
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        assert!(matches!(node.children[0].op, Operator::PhysicalValues(_)));
        let Operator::PhysicalProject(right_project) = &node.children[1].op else {
            panic!("expected CSE project on right child");
        };
        let cse_column = right_project
            .items
            .iter()
            .find(|item| item.output_name == "__cse_0")
            .expect("right CSE item")
            .output_column_id;
        let Operator::PhysicalHashJoin(join) = &node.op else {
            panic!("expected hash join");
        };
        let ScalarNode::BinaryOp { left, right, .. } =
            arena.node(join.other_condition.expect("join other condition"))
        else {
            panic!("expected conjunction");
        };
        assert!(matches!(
            arena.node(*left),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
        assert!(matches!(
            arena.node(*right),
            ScalarNode::BinaryOp { left, .. }
                if matches!(arena.node(*left), ScalarNode::ColumnRef(column_id) if *column_id == cse_column)
        ));
    }

    #[test]
    fn rewrite_join_does_not_factor_cross_input_condition_expr() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let left_a = col(&mut arena, 101);
        let right_b = col(&mut arena, 201);
        let ten = int_lit(&mut arena, 10);
        let twenty = int_lit(&mut arena, 20);
        let left_times_right = mul(&mut arena, left_a, right_b);
        let lower = gt(&mut arena, left_times_right, ten);
        let upper = lt(&mut arena, left_times_right, twenty);
        let condition = and(&mut arena, lower, upper);
        let left = values_node(vec![output_column(101, "left_a")]);
        let right = values_node(vec![output_column(201, "right_b")]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left, right],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "left_a"), output_column(201, "right_b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        assert!(matches!(node.children[0].op, Operator::PhysicalValues(_)));
        assert!(matches!(node.children[1].op, Operator::PhysicalValues(_)));
        let Operator::PhysicalNestLoopJoin(join) = &node.op else {
            panic!("expected nested loop join");
        };
        assert_eq!(join.condition, Some(condition));
    }

    #[test]
    fn rewrite_join_does_not_factor_ambiguous_side_expr() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let shared = col(&mut arena, 101);
        let one = int_lit(&mut arena, 1);
        let ten = int_lit(&mut arena, 10);
        let twenty = int_lit(&mut arena, 20);
        let shared_plus_one = add(&mut arena, shared, one);
        let lower = gt(&mut arena, shared_plus_one, ten);
        let upper = lt(&mut arena, shared_plus_one, twenty);
        let condition = and(&mut arena, lower, upper);
        let left = values_node(vec![output_column(101, "shared_left")]);
        let right = values_node(vec![output_column(101, "shared_right")]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left, right],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "shared")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        assert!(matches!(node.children[0].op, Operator::PhysicalValues(_)));
        assert!(matches!(node.children[1].op, Operator::PhysicalValues(_)));
        let Operator::PhysicalNestLoopJoin(join) = &node.op else {
            panic!("expected nested loop join");
        };
        assert_eq!(join.condition, Some(condition));
    }

    #[test]
    fn rewrite_join_does_not_misclassify_lambda_capture_as_single_side() {
        let mut arena = ScalarArena::new();
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        let left_a = col(&mut arena, 101);
        let right_b = col(&mut arena, 201);
        let ten = int_lit(&mut arena, 10);
        let twenty = int_lit(&mut arena, 20);
        let lambda_param = arena.intern(
            ScalarNode::LambdaParamRef {
                name: "x".to_string(),
                slot_id: 7,
            },
            DataType::Int64,
            true,
        );
        let lambda_body = add(&mut arena, lambda_param, right_b);
        let lambda = arena.intern(
            ScalarNode::Lambda {
                params: vec!["x".to_string()],
                body: lambda_body,
            },
            DataType::Int64,
            true,
        );
        let captures_right = arena.intern(
            ScalarNode::FunctionCall {
                name: "test_lambda_wrapper".to_string(),
                args: vec![left_a, lambda],
                distinct: false,
            },
            DataType::Int64,
            true,
        );
        let lower = gt(&mut arena, captures_right, ten);
        let upper = lt(&mut arena, captures_right, twenty);
        let condition = and(&mut arena, lower, upper);
        let left = values_node(vec![output_column(101, "left_a")]);
        let right = values_node(vec![output_column(201, "right_b")]);
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left, right],
            stats: Statistics::default(),
            output_columns: vec![output_column(101, "left_a"), output_column(201, "right_b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        super::rewrite_node(&mut node, &mut arena, &mut factory);

        assert!(matches!(node.children[0].op, Operator::PhysicalValues(_)));
        assert!(matches!(node.children[1].op, Operator::PhysicalValues(_)));
        let Operator::PhysicalNestLoopJoin(join) = &node.op else {
            panic!("expected nested loop join");
        };
        assert_eq!(join.condition, Some(condition));
    }
}
