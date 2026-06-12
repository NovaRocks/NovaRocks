use crate::sql::analysis::{ExprKind, OutputColumn, SortItem};
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalTopNOp, Operator, TopNPhase};
use crate::sql::optimizer::property::typed_expr_to_column_id;
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::optimizer::topn_proof::{
    ScanTopNCapability, TopNWindow, default_scan_topn_capability, ordering_covers,
    remap_sort_items_through_project, sort_items_to_keys, sort_keys_equivalent,
};

pub(crate) struct MergeConsecutiveTopN;

impl Rule for MergeConsecutiveTopN {
    fn name(&self) -> &str {
        "MergeConsecutiveTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        merge_consecutive_topn(expr, memo)
    }
}

pub(crate) struct RemoveRedundantSortUnderTopN;

impl Rule for RemoveRedundantSortUnderTopN {
    fn name(&self) -> &str {
        "RemoveRedundantSortUnderTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        remove_redundant_sort_under_topn(expr, memo)
    }
}

pub(crate) struct PushTopNThroughProject;

impl Rule for PushTopNThroughProject {
    fn name(&self) -> &str {
        "PushTopNThroughProject"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        push_topn_through_project(expr, memo)
    }
}

pub(crate) struct PushTopNIntoScan;

impl Rule for PushTopNIntoScan {
    fn name(&self) -> &str {
        "PushTopNIntoScan"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        push_topn_into_scan(expr, memo)
    }
}

pub(crate) struct PushTopNThroughJoin;

impl Rule for PushTopNThroughJoin {
    fn name(&self) -> &str {
        stringify!(PushTopNThroughJoin)
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        push_topn_through_join(expr, memo)
    }
}

pub(crate) struct PushTopNThroughAggregate;

impl Rule for PushTopNThroughAggregate {
    fn name(&self) -> &str {
        stringify!(PushTopNThroughAggregate)
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        push_topn_through_aggregate(expr, memo)
    }
}

pub(crate) struct PushTopNThroughSetOp;

impl Rule for PushTopNThroughSetOp {
    fn name(&self) -> &str {
        stringify!(PushTopNThroughSetOp)
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        push_topn_through_setop(expr, memo)
    }
}

fn merge_consecutive_topn(expr: &MExpr, memo: &Memo) -> Vec<NewExpr> {
    let Operator::LogicalTopN(outer) = &expr.op else {
        return vec![];
    };
    if expr.children.len() != 1 {
        return vec![];
    }

    let child_group_id = expr.children[0];
    let Some(child_group) = memo.groups.get(child_group_id) else {
        return vec![];
    };

    let mut results = Vec::new();
    for child_expr in child_group.logical_exprs.iter() {
        let Operator::LogicalTopN(inner) = &child_expr.op else {
            continue;
        };
        if child_expr.children.len() != 1 {
            continue;
        }
        if !topn_phase_can_merge(outer, inner) {
            continue;
        }
        if inner.offset.unwrap_or(0) != 0 {
            continue;
        }
        let Some(outer_window) = TopNWindow::from_limit_offset(outer.limit, outer.offset) else {
            continue;
        };
        let Some(inner_window) = TopNWindow::from_limit_offset(inner.limit, inner.offset) else {
            continue;
        };
        if !inner_window.covers(outer_window) {
            continue;
        }

        let Some(outer_keys) = sort_items_to_keys(&outer.items) else {
            continue;
        };
        let Some(inner_keys) = sort_items_to_keys(&inner.items) else {
            continue;
        };
        let inner_child_group_id = child_expr.children[0];
        let equivalences = memo
            .groups
            .get(inner_child_group_id)
            .and_then(|group| group.logical_props.as_ref())
            .map(|props| &props.equivalence_classes);
        if !sort_keys_equivalent(&outer_keys, &inner_keys, equivalences) {
            continue;
        }

        results.push(NewExpr {
            op: Operator::LogicalTopN(outer.clone()),
            children: vec![inner_child_group_id],
        });
    }
    results
}

fn remove_redundant_sort_under_topn(expr: &MExpr, memo: &Memo) -> Vec<NewExpr> {
    let Operator::LogicalTopN(topn) = &expr.op else {
        return vec![];
    };
    if expr.children.len() != 1 {
        return vec![];
    }
    let Some(topn_keys) = sort_items_to_keys(&topn.items) else {
        return vec![];
    };
    let Some(child_group) = memo.groups.get(expr.children[0]) else {
        return vec![];
    };
    let equivalences = child_group
        .logical_props
        .as_ref()
        .map(|props| &props.equivalence_classes);

    let mut results = Vec::new();
    for child_expr in child_group.logical_exprs.iter() {
        let Operator::LogicalSort(sort) = &child_expr.op else {
            continue;
        };
        if !sort.analytic_partition_exprs.is_empty() {
            continue;
        }
        if child_expr.children.len() != 1 {
            continue;
        }
        let Some(sort_keys) = sort_items_to_keys(&sort.items) else {
            continue;
        };
        if !ordering_covers(&sort_keys, &topn_keys, equivalences) {
            continue;
        }

        results.push(NewExpr {
            op: Operator::LogicalTopN(topn.clone()),
            children: child_expr.children.clone(),
        });
    }
    results
}

fn push_topn_through_project(expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
    let Operator::LogicalTopN(topn) = &expr.op else {
        return vec![];
    };
    // Split TopN codegen expects the final TopN to read directly from its partial root.
    if topn.phase != TopNPhase::Final || topn.is_split {
        return vec![];
    }
    if expr.children.len() != 1 {
        return vec![];
    }
    let Some(project_group) = memo.groups.get(expr.children[0]).cloned() else {
        return vec![];
    };

    let mut results = Vec::new();
    for project_expr in project_group.logical_exprs.iter() {
        let Operator::LogicalProject(project) = &project_expr.op else {
            continue;
        };
        if project_expr.children.len() != 1 {
            continue;
        }
        let Some(remapped_items) = remap_sort_items_through_project(&topn.items, &project.items)
        else {
            continue;
        };

        let pushed_op = Operator::LogicalTopN(LogicalTopNOp {
            items: remapped_items,
            limit: topn.limit,
            offset: topn.offset,
            phase: topn.phase,
            is_split: topn.is_split,
        });
        let pushed_group = find_existing_logical_group(memo, &pushed_op, &project_expr.children)
            .unwrap_or_else(|| {
                let pushed_id = memo.next_expr_id();
                memo.new_group(MExpr {
                    id: pushed_id,
                    op: pushed_op,
                    children: project_expr.children.clone(),
                })
            });
        results.push(NewExpr {
            op: Operator::LogicalProject(project.clone()),
            children: vec![pushed_group],
        });
    }
    results
}

fn push_topn_into_scan(expr: &MExpr, memo: &Memo) -> Vec<NewExpr> {
    let Operator::LogicalTopN(topn) = &expr.op else {
        return vec![];
    };
    if TopNWindow::from_limit_offset(topn.limit, topn.offset).is_none() {
        return vec![];
    }
    if expr.children.len() != 1 {
        return vec![];
    }
    let Some(child_group) = memo.groups.get(expr.children[0]) else {
        return vec![];
    };
    let has_scan = child_group
        .logical_exprs
        .iter()
        .any(|child| matches!(child.op, Operator::LogicalScan(_)));
    if !has_scan {
        return vec![];
    }

    match default_scan_topn_capability() {
        ScanTopNCapability::NoOrdering => vec![],
        ScanTopNCapability::OrderedTopK => vec![NewExpr {
            op: expr.op.clone(),
            children: expr.children.clone(),
        }],
    }
}

fn push_topn_through_join(_expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
    Vec::new()
}

fn push_topn_through_aggregate(_expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
    Vec::new()
}

fn push_topn_through_setop(expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
    let Operator::LogicalTopN(topn) = &expr.op else {
        return vec![];
    };
    if !matches!(topn.phase, TopNPhase::Final) || topn.is_split {
        return vec![];
    }
    let Some(window) = TopNWindow::from_limit_offset(topn.limit, topn.offset) else {
        return vec![];
    };
    let Some(branch_limit) = window.end_exclusive() else {
        return vec![];
    };
    if expr.children.len() != 1 {
        return vec![];
    }
    let Some(union_group) = memo.groups.get(expr.children[0]).cloned() else {
        return vec![];
    };

    let mut results = Vec::new();
    for union_expr in union_group.logical_exprs.iter() {
        let Operator::LogicalUnion(union) = &union_expr.op else {
            continue;
        };
        if !union.all || union_expr.children.is_empty() {
            continue;
        }
        let Some(branch_topn_ops) =
            build_union_branch_topn_ops(topn, branch_limit, union, &union_expr.children, memo)
        else {
            continue;
        };
        if union_expr
            .children
            .iter()
            .zip(&branch_topn_ops)
            .all(|(group, op)| {
                group_starts_with_logical_op(memo, *group, &Operator::LogicalTopN(op.clone()))
            })
        {
            continue;
        }

        let mut pushed_branch_groups = Vec::with_capacity(union_expr.children.len());
        for (branch_group, branch_topn_op) in union_expr.children.iter().zip(branch_topn_ops) {
            let pushed_op = Operator::LogicalTopN(branch_topn_op);
            let pushed_children = vec![*branch_group];
            let pushed_group = find_existing_logical_group(memo, &pushed_op, &pushed_children)
                .unwrap_or_else(|| {
                    let pushed_id = memo.next_expr_id();
                    memo.new_group(MExpr {
                        id: pushed_id,
                        op: pushed_op,
                        children: pushed_children,
                    })
                });
            pushed_branch_groups.push(pushed_group);
        }

        let pushed_union_op = Operator::LogicalUnion(union.clone());
        let pushed_union_group =
            find_existing_logical_group(memo, &pushed_union_op, &pushed_branch_groups)
                .unwrap_or_else(|| {
                    let pushed_id = memo.next_expr_id();
                    memo.new_group(MExpr {
                        id: pushed_id,
                        op: pushed_union_op,
                        children: pushed_branch_groups.clone(),
                    })
                });
        results.push(NewExpr {
            op: Operator::LogicalTopN(topn.clone()),
            children: vec![pushed_union_group],
        });
    }
    results
}

fn build_union_branch_topn_ops(
    topn: &LogicalTopNOp,
    branch_limit: i64,
    union: &crate::sql::optimizer::operator::LogicalUnionOp,
    branch_groups: &[usize],
    memo: &Memo,
) -> Option<Vec<LogicalTopNOp>> {
    branch_groups
        .iter()
        .map(|branch_group| {
            let branch_outputs = memo
                .groups
                .get(*branch_group)?
                .logical_props
                .as_ref()?
                .output_columns
                .as_slice();
            let items =
                remap_sort_items_through_union(&topn.items, &union.output_columns, branch_outputs)?;
            Some(LogicalTopNOp {
                items,
                limit: Some(branch_limit),
                offset: Some(0),
                phase: topn.phase,
                is_split: topn.is_split,
            })
        })
        .collect()
}

fn remap_sort_items_through_union(
    items: &[SortItem],
    union_outputs: &[OutputColumn],
    branch_outputs: &[OutputColumn],
) -> Option<Vec<SortItem>> {
    items
        .iter()
        .map(|item| {
            let union_column_id = typed_expr_to_column_id(&item.expr)?;
            let output_position = union_outputs
                .iter()
                .position(|column| column.column_id == union_column_id)?;
            let union_output = union_outputs.get(output_position)?;
            let branch_output = branch_outputs.get(output_position)?;
            if item.expr.data_type != union_output.data_type
                || item.expr.nullable != union_output.nullable
                || branch_output.data_type != union_output.data_type
                || branch_output.nullable != union_output.nullable
            {
                return None;
            }

            let mut remapped = item.clone();
            remapped.expr.kind = ExprKind::ColumnRef {
                column_id: branch_output.column_id,
                qualifier: None,
                column: branch_output.name.clone(),
            };
            remapped.expr.data_type = branch_output.data_type.clone();
            remapped.expr.nullable = branch_output.nullable;
            Some(remapped)
        })
        .collect()
}

fn group_starts_with_logical_op(memo: &Memo, group_id: usize, op: &Operator) -> bool {
    let op_debug = format!("{op:?}");
    memo.groups
        .get(group_id)
        .and_then(|group| group.logical_exprs.first())
        .is_some_and(|expr| expr.children.len() == 1 && format!("{:?}", expr.op) == op_debug)
}

fn find_existing_logical_group(memo: &Memo, op: &Operator, children: &[usize]) -> Option<usize> {
    let op_debug = format!("{op:?}");
    memo.groups.iter().position(|group| {
        group
            .logical_exprs
            .iter()
            .any(|expr| expr.children == children && format!("{:?}", expr.op) == op_debug)
    })
}

fn topn_phase_can_merge(outer: &LogicalTopNOp, inner: &LogicalTopNOp) -> bool {
    matches!(
        (outer.phase, inner.phase, outer.is_split, inner.is_split),
        (TopNPhase::Final, TopNPhase::Final, false, false)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{
        ExprKind, JoinKind, LiteralValue, ProjectItem, SortItem, TypedExpr,
    };
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
    use crate::sql::optimizer::operator::{
        LogicalAggregateOp, LogicalJoinOp, LogicalProjectOp, LogicalScanOp, LogicalSortOp,
        LogicalTopNOp, LogicalUnionOp, LogicalValuesOp, TopNPhase,
    };
    use crate::sql::optimizer::rule::NewExpr;
    use crate::sql::planner::plan::AggregateCall;
    use arrow::datatypes::DataType;

    fn col(id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn sort_item(id: u32) -> SortItem {
        SortItem {
            expr: col(id),
            asc: true,
            nulls_first: false,
        }
    }

    fn literal_expr(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan_group(memo: &mut Memo) -> usize {
        let scan = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        };
        memo.new_group(scan)
    }

    fn output_column(id: u32, name: &str) -> crate::sql::analysis::OutputColumn {
        output_column_with(id, name, DataType::Int64, true)
    }

    fn output_column_with(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> crate::sql::analysis::OutputColumn {
        crate::sql::analysis::OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn values_group(memo: &mut Memo, column_ids: &[u32]) -> usize {
        let columns = column_ids
            .iter()
            .map(|id| output_column(*id, &format!("c{id}")))
            .collect::<Vec<_>>();
        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: columns.clone(),
            }),
            children: vec![],
        });
        memo.groups[group].logical_props = Some(LogicalProperties::new(columns, 0.0));
        group
    }

    fn join_group(memo: &mut Memo, left_group: usize, right_group: usize) -> usize {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            children: vec![left_group, right_group],
        })
    }

    fn aggregate_group(memo: &mut Memo, child_group: usize) -> usize {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![col(1)],
                vec![AggregateCall {
                    name: "array_agg".to_string(),
                    args: vec![col(2)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![sort_item(2)],
                    output_column_id: ColumnId(10),
                }],
                vec![output_column(1, "c1"), output_column(10, "array_agg")],
            )),
            children: vec![child_group],
        })
    }

    fn union_group(memo: &mut Memo, all: bool, inputs: Vec<usize>) -> usize {
        union_group_with_outputs(memo, all, inputs, vec![output_column(1, "c1")])
    }

    fn union_group_with_outputs(
        memo: &mut Memo,
        all: bool,
        inputs: Vec<usize>,
        output_columns: Vec<crate::sql::analysis::OutputColumn>,
    ) -> usize {
        let child_output_columns = inputs
            .iter()
            .map(|_| output_columns.clone())
            .collect::<Vec<_>>();
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalUnion(LogicalUnionOp {
                all,
                output_columns,
                child_output_columns,
            }),
            children: inputs,
        })
    }

    fn topn_with_item(
        memo: &Memo,
        item: SortItem,
        limit: i64,
        offset: i64,
        phase: TopNPhase,
        is_split: bool,
        child_group: usize,
    ) -> MExpr {
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![item],
                limit: Some(limit),
                offset: Some(offset),
                phase,
                is_split,
            }),
            children: vec![child_group],
        }
    }

    fn topn(
        memo: &Memo,
        limit: i64,
        offset: i64,
        phase: TopNPhase,
        is_split: bool,
        child_group: usize,
    ) -> MExpr {
        topn_with_item(
            memo,
            sort_item(1),
            limit,
            offset,
            phase,
            is_split,
            child_group,
        )
    }

    fn sort_with_items(memo: &Memo, items: Vec<SortItem>, child_group: usize) -> MExpr {
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalSort(LogicalSortOp {
                items,
                analytic_partition_exprs: Vec::new(),
            }),
            children: vec![child_group],
        }
    }

    fn project_with_items(memo: &Memo, items: Vec<ProjectItem>, child_group: usize) -> MExpr {
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalProject(LogicalProjectOp {
                items,
                output_qualifier: None,
            }),
            children: vec![child_group],
        }
    }

    fn project_item(expr: TypedExpr, output_id: u32, output_name: &str) -> ProjectItem {
        ProjectItem {
            expr,
            output_name: output_name.to_string(),
            output_column_id: ColumnId(output_id),
        }
    }

    fn project_pushdown_rule() -> Box<dyn Rule> {
        rule_by_name("PushTopNThroughProject")
    }

    fn rule_by_name(name: &str) -> Box<dyn Rule> {
        crate::sql::optimizer::cascades_rules::all_transformation_rules()
            .into_iter()
            .find(|rule| rule.name() == name)
            .unwrap_or_else(|| panic!("{name} should be registered"))
    }

    fn add_new_expr_to_group(memo: &mut Memo, group_id: usize, new_expr: NewExpr) {
        memo.add_expr_to_group(
            group_id,
            MExpr {
                id: memo.next_expr_id(),
                op: new_expr.op,
                children: new_expr.children,
            },
        );
    }

    fn assert_single_branch_topn(
        memo: &Memo,
        branch_group: usize,
        expected_child_group: usize,
        expected_limit: i64,
        expected_offset: i64,
        expected_sort_column_id: u32,
    ) {
        let branch_expr = memo.groups[branch_group]
            .logical_exprs
            .iter()
            .find(|expr| matches!(expr.op, Operator::LogicalTopN(_)))
            .expect("branch group should contain pushed LogicalTopN");
        assert_eq!(
            branch_expr.children,
            vec![expected_child_group],
            "branch TopN should point at the original UNION branch"
        );
        match &branch_expr.op {
            Operator::LogicalTopN(branch_topn) => {
                assert_eq!(branch_topn.limit, Some(expected_limit));
                assert_eq!(branch_topn.offset, Some(expected_offset));
                assert_eq!(branch_topn.items.len(), 1);
                match &branch_topn.items[0].expr.kind {
                    ExprKind::ColumnRef { column_id, .. } => {
                        assert_eq!(*column_id, ColumnId(expected_sort_column_id));
                    }
                    other => panic!("expected branch TopN ColumnRef sort item, got {other:?}"),
                }
            }
            other => panic!("expected branch LogicalTopN, got {other:?}"),
        }
    }

    fn analytic_sort_with_items(memo: &Memo, items: Vec<SortItem>, child_group: usize) -> MExpr {
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalSort(LogicalSortOp {
                items,
                analytic_partition_exprs: vec![col(2)],
            }),
            children: vec![child_group],
        }
    }

    #[test]
    fn merges_consecutive_topn_when_inner_window_covers_outer() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 20, 0, TopNPhase::Final, false, scan_group));
        let outer = topn(&memo, 5, 10, TopNPhase::Final, false, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert_eq!(out.len(), 1, "expected one merged TopN alternative");
        match &out[0].op {
            Operator::LogicalTopN(topn) => {
                assert_eq!(topn.limit, Some(5));
                assert_eq!(topn.offset, Some(10));
                assert_eq!(topn.phase, TopNPhase::Final);
                assert!(!topn.is_split);
            }
            other => panic!("expected LogicalTopN, got {other:?}"),
        }
        assert_eq!(
            out[0].children,
            vec![scan_group],
            "merged TopN should bypass the inner TopN"
        );
    }

    #[test]
    fn does_not_merge_when_inner_window_is_too_small() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 12, 0, TopNPhase::Final, false, scan_group));
        let outer = topn(&memo, 5, 10, TopNPhase::Final, false, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert!(
            out.is_empty(),
            "inner TopN that ends before the outer window must not be removed"
        );
    }

    #[test]
    fn does_not_merge_when_inner_offset_is_non_zero() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 20, 3, TopNPhase::Final, false, scan_group));
        let outer = topn(&memo, 5, 10, TopNPhase::Final, false, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert!(
            out.is_empty(),
            "inner offset must be preserved instead of dropping the inner TopN"
        );
    }

    #[test]
    fn does_not_merge_split_final_over_partial_topn() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 20, 0, TopNPhase::Partial, false, scan_group));
        let outer = topn(&memo, 5, 0, TopNPhase::Final, true, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert!(
            out.is_empty(),
            "split final TopN must keep its partial TopN child"
        );
    }

    #[test]
    fn does_not_merge_unsplit_final_over_partial_topn() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 20, 0, TopNPhase::Partial, false, scan_group));
        let outer = topn(&memo, 5, 0, TopNPhase::Final, false, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert!(
            out.is_empty(),
            "unsplit final TopN must not merge over partial TopN"
        );
    }

    #[test]
    fn merges_when_child_equivalence_classes_prove_sort_keys_equivalent() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let mut props = LogicalProperties::new(vec![], 100.0);
        props
            .equivalence_classes
            .merge_pair(ColumnId(1), ColumnId(2));
        memo.groups[scan_group].logical_props = Some(props);
        let inner_group = memo.new_group(topn_with_item(
            &memo,
            sort_item(2),
            20,
            0,
            TopNPhase::Final,
            false,
            scan_group,
        ));
        let outer = topn(&memo, 5, 0, TopNPhase::Final, false, inner_group);

        let out = MergeConsecutiveTopN.apply(&outer, &mut memo);

        assert_eq!(out.len(), 1, "equivalent sort keys should allow merge");
        assert_eq!(out[0].children, vec![scan_group]);
    }

    #[test]
    fn removes_plain_sort_under_matching_topn() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let sort_group = memo.new_group(sort_with_items(&memo, vec![sort_item(1)], scan_group));
        let topn = topn(&memo, 10, 0, TopNPhase::Final, false, sort_group);

        let out = RemoveRedundantSortUnderTopN.apply(&topn, &mut memo);

        assert_eq!(out.len(), 1, "matching plain Sort should be elided");
        match &out[0].op {
            Operator::LogicalTopN(rewritten) => {
                assert_eq!(rewritten.limit, Some(10));
                assert_eq!(rewritten.offset, Some(0));
                assert_eq!(rewritten.phase, TopNPhase::Final);
                assert!(!rewritten.is_split);
            }
            other => panic!("expected LogicalTopN, got {other:?}"),
        }
        assert_eq!(
            out[0].children,
            vec![scan_group],
            "rewritten TopN should bypass the redundant Sort"
        );
    }

    #[test]
    fn does_not_remove_analytic_partition_sort_under_topn() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let sort_group = memo.new_group(analytic_sort_with_items(
            &memo,
            vec![sort_item(1)],
            scan_group,
        ));
        let topn = topn(&memo, 10, 0, TopNPhase::Final, false, sort_group);

        let out = RemoveRedundantSortUnderTopN.apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "analytic partition sort carries distribution semantics and must not be elided"
        );
    }

    #[test]
    fn does_not_remove_sort_when_ordering_does_not_cover_topn() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let sort_group = memo.new_group(sort_with_items(&memo, vec![sort_item(2)], scan_group));
        let topn = topn(&memo, 10, 0, TopNPhase::Final, false, sort_group);

        let out = RemoveRedundantSortUnderTopN.apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "Sort ordering must cover TopN ordering before the Sort can be elided"
        );
    }

    #[test]
    fn project_pushdown_remaps_column_ref_sort_keys() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let project_items = vec![
            project_item(col(1), 10, "alias_c1"),
            project_item(col(2), 20, "alias_c2"),
        ];
        let project_group =
            memo.new_group(project_with_items(&memo, project_items.clone(), scan_group));
        let topn = topn_with_item(
            &memo,
            sort_item(10),
            7,
            2,
            TopNPhase::Final,
            false,
            project_group,
        );

        let out = project_pushdown_rule().apply(&topn, &mut memo);

        assert_eq!(out.len(), 1, "project alias should allow TopN pushdown");
        match &out[0].op {
            Operator::LogicalProject(project) => {
                assert_eq!(project.items.len(), project_items.len());
                assert_eq!(project.items[0].output_column_id, ColumnId(10));
            }
            other => panic!("expected LogicalProject, got {other:?}"),
        }
        let pushed_group = out[0].children[0];
        let pushed_expr = memo.groups[pushed_group]
            .logical_exprs
            .first()
            .expect("pushed TopN group should contain a logical expression");
        assert_eq!(
            pushed_expr.children,
            vec![scan_group],
            "pushed TopN should point at the original Project child"
        );
        match &pushed_expr.op {
            Operator::LogicalTopN(pushed) => {
                assert_eq!(pushed.limit, Some(7));
                assert_eq!(pushed.offset, Some(2));
                assert_eq!(pushed.phase, TopNPhase::Final);
                assert!(!pushed.is_split);
                assert_eq!(pushed.items.len(), 1);
                match &pushed.items[0].expr.kind {
                    ExprKind::ColumnRef { column_id, .. } => {
                        assert_eq!(*column_id, ColumnId(1));
                    }
                    other => panic!("expected remapped ColumnRef sort key, got {other:?}"),
                }
            }
            other => panic!("expected pushed LogicalTopN, got {other:?}"),
        }
    }

    #[test]
    fn project_pushdown_reuses_existing_pushed_topn_group() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let project_group = memo.new_group(project_with_items(
            &memo,
            vec![project_item(col(1), 10, "alias_c1")],
            scan_group,
        ));
        let topn = topn_with_item(
            &memo,
            sort_item(10),
            7,
            0,
            TopNPhase::Final,
            false,
            project_group,
        );
        let rule = project_pushdown_rule();

        let first = rule.apply(&topn, &mut memo);
        assert_eq!(first.len(), 1, "first apply should push TopN");
        let first_pushed_group = first[0].children[0];
        let group_count_after_first = memo.groups.len();

        let second = rule.apply(&topn, &mut memo);

        assert_eq!(
            second.len(),
            1,
            "second apply should still return the candidate"
        );
        assert_eq!(
            second[0].children[0], first_pushed_group,
            "second apply should reuse the existing pushed TopN group"
        );
        assert_eq!(
            memo.groups.len(),
            group_count_after_first,
            "second apply must not create another equivalent pushed TopN group"
        );
    }

    #[test]
    fn project_pushdown_merge_survives_physical_implementation_dedup() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let inner_group = memo.new_group(topn(&memo, 3, 0, TopNPhase::Final, false, scan_group));
        let project_group = memo.new_group(project_with_items(
            &memo,
            vec![project_item(col(1), 10, "alias_c1")],
            inner_group,
        ));
        let outer_group = memo.new_group(topn_with_item(
            &memo,
            sort_item(10),
            2,
            0,
            TopNPhase::Final,
            false,
            project_group,
        ));

        let outer_expr = memo.groups[outer_group].logical_exprs[0].clone();
        let pushed = project_pushdown_rule().apply(&outer_expr, &mut memo);
        assert_eq!(
            pushed.len(),
            1,
            "project pushdown should expose nested TopN"
        );
        let pushed_group = pushed[0].children[0];
        add_new_expr_to_group(&mut memo, outer_group, pushed.into_iter().next().unwrap());

        let pushed_expr = memo.groups[pushed_group].logical_exprs[0].clone();
        let merged = MergeConsecutiveTopN.apply(&pushed_expr, &mut memo);
        assert_eq!(merged.len(), 1, "exposed adjacent TopNs should merge");
        add_new_expr_to_group(&mut memo, pushed_group, merged.into_iter().next().unwrap());

        crate::sql::optimizer::implement(
            &mut memo,
            &crate::sql::optimizer::cascades_rules::all_implementation_rules(),
            &crate::sql::optimizer::options::OptimizerOptions::default_settings(),
        );

        assert!(
            memo.groups[pushed_group]
                .physical_exprs
                .iter()
                .any(|expr| matches!(expr.op, Operator::PhysicalTopN(_))
                    && expr.children == vec![scan_group]),
            "implementation must keep the merged TopN alternative that bypasses the inner TopN"
        );
    }

    #[test]
    fn project_pushdown_rejects_computed_sort_keys() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let project_group = memo.new_group(project_with_items(
            &memo,
            vec![project_item(literal_expr(42), 10, "computed")],
            scan_group,
        ));
        let topn = topn_with_item(
            &memo,
            sort_item(10),
            7,
            0,
            TopNPhase::Final,
            false,
            project_group,
        );

        let out = project_pushdown_rule().apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "computed Project expressions must fail closed"
        );
    }

    #[test]
    fn project_pushdown_fails_closed_for_partial_topn() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let project_group = memo.new_group(project_with_items(
            &memo,
            vec![project_item(col(1), 10, "alias_c1")],
            scan_group,
        ));
        let topn = topn_with_item(
            &memo,
            sort_item(10),
            7,
            0,
            TopNPhase::Partial,
            false,
            project_group,
        );

        let out = project_pushdown_rule().apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "Project pushdown must not move PARTIAL TopN away from split-final shape"
        );
    }

    #[test]
    fn project_pushdown_fails_closed_for_split_final_topn() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let project_group = memo.new_group(project_with_items(
            &memo,
            vec![project_item(col(1), 10, "alias_c1")],
            scan_group,
        ));
        let topn = topn_with_item(
            &memo,
            sort_item(10),
            7,
            0,
            TopNPhase::Final,
            true,
            project_group,
        );

        let out = project_pushdown_rule().apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "Project pushdown must not wrap an already split-final TopN"
        );
    }

    #[test]
    fn scan_pushdown_fails_closed_with_default_capability() {
        let mut memo = Memo::new();
        let scan_group = scan_group(&mut memo);
        let topn = topn(&memo, 10, 0, TopNPhase::Final, false, scan_group);

        let out = PushTopNIntoScan.apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "default scan capability must not produce TopN pushdown candidates"
        );
    }

    #[test]
    fn join_pushdown_fails_closed_for_inner_join_without_multiplicity_proof() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[2]);
        let join_group = join_group(&mut memo, left_group, right_group);
        let topn = topn(&memo, 10, 0, TopNPhase::Final, false, join_group);

        let out = rule_by_name("PushTopNThroughJoin").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "join TopN pushdown must fail closed until multiplicity is proven"
        );
    }

    #[test]
    fn aggregate_pushdown_fails_closed_for_aggregate_function_order() {
        let mut memo = Memo::new();
        let input_group = values_group(&mut memo, &[1, 2]);
        let aggregate_group = aggregate_group(&mut memo, input_group);
        let topn = topn_with_item(
            &memo,
            sort_item(10),
            10,
            0,
            TopNPhase::Final,
            false,
            aggregate_group,
        );

        let out = rule_by_name("PushTopNThroughAggregate").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "aggregate TopN pushdown must fail closed for ordered aggregate functions"
        );
    }

    #[test]
    fn setop_pushdown_fails_closed_for_union_distinct() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[1]);
        let union_group = union_group(&mut memo, false, vec![left_group, right_group]);
        let topn = topn(&memo, 10, 0, TopNPhase::Final, false, union_group);

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "set-op TopN pushdown must fail closed for UNION DISTINCT"
        );
    }

    #[test]
    fn setop_pushdown_adds_branch_topn_for_union_all_and_keeps_final_topn() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[1]);
        let union_group = union_group(&mut memo, true, vec![left_group, right_group]);
        let topn = topn(&memo, 10, 0, TopNPhase::Final, false, union_group);
        let rule = rule_by_name("PushTopNThroughSetOp");

        let first = rule.apply(&topn, &mut memo);

        assert_eq!(first.len(), 1, "UNION ALL should allow branch pruning");
        match &first[0].op {
            Operator::LogicalTopN(final_topn) => {
                assert_eq!(final_topn.limit, Some(10));
                assert_eq!(final_topn.offset, Some(0));
            }
            other => panic!("expected final LogicalTopN, got {other:?}"),
        }
        assert_eq!(
            first[0].children.len(),
            1,
            "final TopN must have the pushed UNION as its child"
        );
        let pushed_union_group = first[0].children[0];
        assert_ne!(
            pushed_union_group, union_group,
            "final TopN should wrap the pushed UNION, not the original UNION"
        );
        let pushed_union_expr = memo.groups[pushed_union_group]
            .logical_exprs
            .first()
            .expect("pushed union group should contain a logical expression");
        match &pushed_union_expr.op {
            Operator::LogicalUnion(pushed_union) => {
                assert!(pushed_union.all, "pushed UNION must preserve UNION ALL");
            }
            other => panic!("expected pushed LogicalUnion, got {other:?}"),
        }
        assert_eq!(
            pushed_union_expr.children.len(),
            2,
            "pushed UNION should keep both branches"
        );
        assert_single_branch_topn(&memo, pushed_union_expr.children[0], left_group, 10, 0, 1);
        assert_single_branch_topn(&memo, pushed_union_expr.children[1], right_group, 10, 0, 1);

        let group_count_after_first = memo.groups.len();
        let second = rule.apply(&topn, &mut memo);

        assert_eq!(
            second.len(),
            1,
            "repeated apply should still produce one candidate"
        );
        assert_eq!(
            second[0].children[0], pushed_union_group,
            "repeated apply should reuse the pushed UNION group"
        );
        assert_eq!(
            memo.groups.len(),
            group_count_after_first,
            "repeated apply must not create duplicate branch TopN or UNION groups"
        );
    }

    #[test]
    fn setop_pushdown_uses_branch_window_for_offset() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[1]);
        let union_group = union_group(&mut memo, true, vec![left_group, right_group]);
        let topn = topn(&memo, 2, 3, TopNPhase::Final, false, union_group);

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert_eq!(out.len(), 1, "UNION ALL should allow offset-safe pruning");
        match &out[0].op {
            Operator::LogicalTopN(final_topn) => {
                assert_eq!(final_topn.limit, Some(2));
                assert_eq!(final_topn.offset, Some(3));
            }
            other => panic!("expected final LogicalTopN, got {other:?}"),
        }
        let pushed_union_group = out[0].children[0];
        let pushed_union_expr = memo.groups[pushed_union_group]
            .logical_exprs
            .first()
            .expect("pushed union group should contain a logical expression");
        assert_single_branch_topn(&memo, pushed_union_expr.children[0], left_group, 5, 0, 1);
        assert_single_branch_topn(&memo, pushed_union_expr.children[1], right_group, 5, 0, 1);
    }

    #[test]
    fn setop_pushdown_remaps_union_output_sort_key_to_branch_outputs() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[2]);
        memo.groups[left_group].logical_props = Some(LogicalProperties::new(
            vec![output_column(1, "left_c")],
            10.0,
        ));
        memo.groups[right_group].logical_props = Some(LogicalProperties::new(
            vec![output_column(2, "right_c")],
            10.0,
        ));
        let union_group = union_group_with_outputs(
            &mut memo,
            true,
            vec![left_group, right_group],
            vec![output_column(10, "union_c")],
        );
        let topn = topn_with_item(
            &memo,
            sort_item(10),
            10,
            0,
            TopNPhase::Final,
            false,
            union_group,
        );

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert_eq!(
            out.len(),
            1,
            "UNION ALL should remap parent output sort key into branch output keys"
        );
        let pushed_union_group = out[0].children[0];
        let pushed_union_expr = memo.groups[pushed_union_group]
            .logical_exprs
            .first()
            .expect("pushed union group should contain a logical expression");
        assert_single_branch_topn(&memo, pushed_union_expr.children[0], left_group, 10, 0, 1);
        assert_single_branch_topn(&memo, pushed_union_expr.children[1], right_group, 10, 0, 2);
    }

    #[test]
    fn setop_pushdown_fails_closed_for_non_column_ref_sort_item() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[1]);
        let union_group = union_group(&mut memo, true, vec![left_group, right_group]);
        let topn = topn_with_item(
            &memo,
            SortItem {
                expr: literal_expr(1),
                asc: true,
                nulls_first: false,
            },
            10,
            0,
            TopNPhase::Final,
            false,
            union_group,
        );

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "UNION ALL TopN pushdown must fail closed for non-ColumnRef sort items"
        );
    }

    #[test]
    fn setop_pushdown_fails_closed_when_sort_column_is_not_union_output() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[2]);
        let union_group = union_group_with_outputs(
            &mut memo,
            true,
            vec![left_group, right_group],
            vec![output_column(10, "union_c")],
        );
        let topn = topn_with_item(
            &memo,
            sort_item(99),
            10,
            0,
            TopNPhase::Final,
            false,
            union_group,
        );

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "UNION ALL TopN pushdown must fail closed when sort key is not a UNION output"
        );
    }

    #[test]
    fn setop_pushdown_fails_closed_when_branch_lacks_output_position() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1, 3]);
        let right_group = values_group(&mut memo, &[2]);
        let union_group = union_group_with_outputs(
            &mut memo,
            true,
            vec![left_group, right_group],
            vec![output_column(10, "union_c1"), output_column(11, "union_c2")],
        );
        let topn = topn_with_item(
            &memo,
            sort_item(11),
            10,
            0,
            TopNPhase::Final,
            false,
            union_group,
        );

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "UNION ALL TopN pushdown must fail closed when any branch lacks the mapped position"
        );
    }

    #[test]
    fn setop_pushdown_fails_closed_when_branch_output_type_mismatches_union_output() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[2]);
        memo.groups[right_group].logical_props = Some(LogicalProperties::new(
            vec![output_column_with(2, "right_c", DataType::Utf8, true)],
            10.0,
        ));
        let union_group = union_group_with_outputs(
            &mut memo,
            true,
            vec![left_group, right_group],
            vec![output_column(10, "union_c")],
        );
        let topn = topn_with_item(
            &memo,
            sort_item(10),
            10,
            0,
            TopNPhase::Final,
            false,
            union_group,
        );

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "UNION ALL TopN pushdown must fail closed on branch output type mismatch"
        );
    }

    #[test]
    fn setop_pushdown_fails_closed_when_branch_output_nullability_mismatches_union_output() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[2]);
        memo.groups[right_group].logical_props = Some(LogicalProperties::new(
            vec![output_column_with(2, "right_c", DataType::Int64, false)],
            10.0,
        ));
        let union_group = union_group_with_outputs(
            &mut memo,
            true,
            vec![left_group, right_group],
            vec![output_column(10, "union_c")],
        );
        let topn = topn_with_item(
            &memo,
            sort_item(10),
            10,
            0,
            TopNPhase::Final,
            false,
            union_group,
        );

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "UNION ALL TopN pushdown must fail closed on branch output nullability mismatch"
        );
    }

    #[test]
    fn setop_pushdown_is_idempotent_after_candidate_is_added_to_group() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[1]);
        let union_group = union_group(&mut memo, true, vec![left_group, right_group]);
        let topn_group = memo.new_group(topn(&memo, 10, 0, TopNPhase::Final, false, union_group));
        let topn_expr = memo.groups[topn_group].logical_exprs[0].clone();
        let rule = rule_by_name("PushTopNThroughSetOp");

        let mut first = rule.apply(&topn_expr, &mut memo);
        assert_eq!(first.len(), 1, "first apply should produce branch pruning");
        add_new_expr_to_group(&mut memo, topn_group, first.pop().unwrap());
        let pushed_expr = memo.groups[topn_group]
            .logical_exprs
            .last()
            .expect("candidate should be added back to the original group")
            .clone();
        let group_count_after_add = memo.groups.len();

        let second = rule.apply(&pushed_expr, &mut memo);

        assert!(
            second.is_empty(),
            "already-pushed UNION ALL branch pruning must not chain"
        );
        assert_eq!(
            memo.groups.len(),
            group_count_after_add,
            "idempotent apply must not create additional memo groups"
        );
    }

    #[test]
    fn setop_pushdown_fails_closed_for_partial_topn() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[1]);
        let union_group = union_group(&mut memo, true, vec![left_group, right_group]);
        let topn = topn(&memo, 10, 0, TopNPhase::Partial, false, union_group);

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "set-op TopN pruning only supports unsplit final TopN"
        );
    }

    #[test]
    fn setop_pushdown_fails_closed_for_split_topn() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[1]);
        let union_group = union_group(&mut memo, true, vec![left_group, right_group]);
        let topn = topn(&memo, 10, 0, TopNPhase::Final, true, union_group);

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "set-op TopN pruning only supports unsplit final TopN"
        );
    }

    #[test]
    fn setop_pushdown_fails_closed_when_branch_window_overflows() {
        let mut memo = Memo::new();
        let left_group = values_group(&mut memo, &[1]);
        let right_group = values_group(&mut memo, &[1]);
        let union_group = union_group(&mut memo, true, vec![left_group, right_group]);
        let topn = topn(&memo, 1, i64::MAX, TopNPhase::Final, false, union_group);

        let out = rule_by_name("PushTopNThroughSetOp").apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "branch pruning must fail closed when offset + limit overflows"
        );
    }
}
