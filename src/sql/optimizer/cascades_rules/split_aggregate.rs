use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::codegen::helpers::{
    agg_call_display_name, agg_call_display_name_without_qualifiers, typed_expr_display_name,
};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp, Operator};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::planner::plan::AggregateCall;

pub(crate) struct SplitAggregateRule;

impl Rule for SplitAggregateRule {
    fn name(&self) -> &str {
        "SplitAggregateRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(agg) = &expr.op else {
            return Vec::new();
        };
        if !is_eligible(agg) {
            return Vec::new();
        }

        let local_output_columns = local_output_columns(agg);
        let local_group_by =
            aggregate_group_key_output_ref(&local_output_columns, agg.group_by.len());
        let local = LogicalAggregateOp::staged(
            AggStage::Local,
            agg.group_by.clone(),
            agg.aggregates.clone(),
            local_output_columns,
            vec![false; agg.aggregates.len()],
            true,
        );
        let local_op = Operator::LogicalAggregate(local);
        let local_group = find_existing_logical_group(memo, &local_op, &expr.children)
            .unwrap_or_else(|| {
                let local_id = memo.next_expr_id();
                memo.new_group(MExpr {
                    id: local_id,
                    op: local_op,
                    children: expr.children.clone(),
                })
            });
        let global = LogicalAggregateOp::staged(
            AggStage::Global,
            local_group_by,
            agg.aggregates.clone(),
            agg.output_columns.clone(),
            vec![true; agg.aggregates.len()],
            true,
        );

        vec![NewExpr {
            op: Operator::LogicalAggregate(global),
            children: vec![local_group],
        }]
    }
}

fn is_eligible(agg: &LogicalAggregateOp) -> bool {
    agg.stage == AggStage::Single
        && !agg.is_split
        && agg.is_merge.iter().all(|flag| !*flag)
        && (!agg.aggregates.is_empty() || !agg.group_by.is_empty())
        && agg.aggregates.iter().all(is_splittable_aggregate)
}

fn is_splittable_aggregate(call: &AggregateCall) -> bool {
    use crate::sql::agg_mergeability::{AggMergeability, aggregate_mergeability};
    aggregate_mergeability(call) == AggMergeability::TwoPhase
}

fn local_output_columns(agg: &LogicalAggregateOp) -> Vec<OutputColumn> {
    let mut columns = Vec::with_capacity(agg.group_by.len() + agg.aggregates.len());
    columns.extend(agg.group_by.iter().enumerate().map(|(idx, expr)| {
        let name = typed_expr_display_name(expr);
        // Non-ColumnRef group keys (constant/alias/expression, e.g. `'a' as g`)
        // reuse the original aggregate's group output id *by position* (the
        // first group_by.len() output columns are the group keys, in order).
        // The previous by-name lookup returned ColumnId::UNSET when the output
        // name was a SELECT alias rather than the expression's display name,
        // which the id-binding verifier rejects once the aggregate is split.
        let column_id = match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id,
            _ => agg
                .output_columns
                .get(idx)
                .map(|output| output.column_id)
                .filter(|id| *id != ColumnId::UNSET)
                .unwrap_or_else(|| group_key_output_column_id(expr, &name, &agg.output_columns)),
        };
        OutputColumn {
            column_id,
            name,
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            is_internal: false,
        }
    }));
    columns.extend(agg.aggregates.iter().map(|call| {
        let name = agg_call_display_name(call);
        OutputColumn {
            column_id: aggregate_output_column_id(call, &name, &agg.output_columns),
            name,
            data_type: call.result_type.clone(),
            nullable: true,
            is_internal: true,
        }
    }));
    columns
}

pub(crate) fn group_key_output_column_id(
    expr: &TypedExpr,
    display_name: &str,
    existing_outputs: &[OutputColumn],
) -> ColumnId {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => *column_id,
        _ => existing_outputs
            .iter()
            .find(|output| output.name == display_name)
            .map(|output| output.column_id)
            .unwrap_or(ColumnId::UNSET),
    }
}

fn aggregate_output_column_id(
    call: &AggregateCall,
    display_name: &str,
    existing_outputs: &[OutputColumn],
) -> ColumnId {
    if call.output_column_id != ColumnId::UNSET {
        return call.output_column_id;
    }
    let unqualified_display_name = agg_call_display_name_without_qualifiers(call);
    existing_outputs
        .iter()
        .find(|output| output.name == display_name || output.name == unqualified_display_name)
        .map(|output| output.column_id)
        .unwrap_or(ColumnId::UNSET)
}

pub(crate) fn aggregate_group_key_output_ref(
    local_output_columns: &[OutputColumn],
    group_by_len: usize,
) -> Vec<TypedExpr> {
    local_output_columns
        .iter()
        .take(group_by_len)
        .map(|output| TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: output.column_id,
                qualifier: None,
                column: output.name.clone(),
            },
            data_type: output.data_type.clone(),
            nullable: output.nullable,
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp, LogicalValuesOp};
    use crate::sql::planner::plan::AggregateCall;
    use arrow::datatypes::DataType;

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_ref(id: u32, name: &str) -> TypedExpr {
        nullable_col_ref(id, name, false)
    }

    fn nullable_col_ref(id: u32, name: &str, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable,
        }
    }

    fn count_call(distinct: bool) -> AggregateCall {
        AggregateCall {
            name: "count".to_string(),
            args: vec![col_ref(2, "v")],
            distinct,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: ColumnId::UNSET,
        }
    }

    fn values_group(memo: &mut Memo) -> usize {
        let id = memo.next_expr_id();
        memo.new_group(MExpr {
            id,
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        })
    }

    fn single_grouped_expr(memo: &mut Memo) -> MExpr {
        let child = values_group(memo);
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![nullable_col_ref(1, "k", true)],
                vec![count_call(false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![child],
        }
    }

    fn select_order_grouped_expr(memo: &mut Memo) -> MExpr {
        let child = values_group(memo);
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![col_ref(1, "k")],
                vec![count_call(false)],
                vec![output_column(3, "count(v)"), output_column(1, "k")],
            )),
            children: vec![child],
        }
    }

    fn single_scalar_expr(memo: &mut Memo) -> MExpr {
        let child = values_group(memo);
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![],
                vec![count_call(false)],
                vec![output_column(3, "count(v)")],
            )),
            children: vec![child],
        }
    }

    #[test]
    fn splits_grouped_aggregate_into_global_over_local() {
        let mut memo = Memo::new();
        let expr = single_grouped_expr(&mut memo);
        let out = SplitAggregateRule.apply(&expr, &mut memo);
        assert_eq!(out.len(), 1);
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.stage, AggStage::Global);
        assert_eq!(global.is_merge, vec![true]);
        assert!(global.is_split);
        assert_eq!(global.group_by.len(), 1);
        assert!(global.group_by[0].nullable);
        assert_eq!(out[0].children.len(), 1);
        let local_group_id = out[0].children[0];
        let local_group = &memo.groups[local_group_id];
        assert_eq!(local_group.logical_exprs.len(), 1);
        let Operator::LogicalAggregate(local) = &local_group.logical_exprs[0].op else {
            panic!("expected local aggregate child");
        };
        assert_eq!(local.stage, AggStage::Local);
        assert_eq!(local.is_merge, vec![false]);
        assert!(local.is_split);
        assert_eq!(
            local.output_columns[local.group_by.len()].column_id,
            ColumnId::new_for_test(3)
        );
    }

    #[test]
    fn split_global_group_by_uses_local_group_key_layout_not_select_order_output() {
        let mut memo = Memo::new();
        let expr = select_order_grouped_expr(&mut memo);
        let out = SplitAggregateRule.apply(&expr, &mut memo);
        assert_eq!(out.len(), 1);
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.group_by.len(), 1);
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &global.group_by[0].kind
        else {
            panic!("expected global group key column ref");
        };
        assert_eq!(*column_id, ColumnId::new_for_test(1));
        assert_eq!(column, "t.k");
    }

    #[test]
    fn repeated_apply_reuses_existing_local_group() {
        let mut memo = Memo::new();
        let expr = single_grouped_expr(&mut memo);
        let first = SplitAggregateRule.apply(&expr, &mut memo);
        assert_eq!(first.len(), 1);
        let first_local_group = first[0].children[0];
        let group_count_after_first = memo.groups.len();

        let second = SplitAggregateRule.apply(&expr, &mut memo);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].children[0], first_local_group);
        assert_eq!(memo.groups.len(), group_count_after_first);
    }

    #[test]
    fn splits_scalar_aggregate() {
        let mut memo = Memo::new();
        let expr = single_scalar_expr(&mut memo);
        let out = SplitAggregateRule.apply(&expr, &mut memo);
        assert_eq!(out.len(), 1);
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.stage, AggStage::Global);
        assert!(global.group_by.is_empty());
        let local_group_id = out[0].children[0];
        let local_group = &memo.groups[local_group_id];
        let Operator::LogicalAggregate(local) = &local_group.logical_exprs[0].op else {
            panic!("expected local aggregate child");
        };
        assert_eq!(local.stage, AggStage::Local);
        assert!(local.group_by.is_empty());
        assert_eq!(local.output_columns[0].column_id, ColumnId::new_for_test(3));
    }

    fn avg_call() -> AggregateCall {
        AggregateCall {
            name: "avg".to_string(),
            args: vec![col_ref(2, "v")],
            distinct: false,
            result_type: arrow::datatypes::DataType::Float64,
            order_by: vec![],
            output_column_id: ColumnId::UNSET,
        }
    }

    #[test]
    fn splits_grouped_avg_aggregate() {
        let mut memo = Memo::new();
        let child = values_group(&mut memo);
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![nullable_col_ref(1, "k", true)],
                vec![avg_call()],
                vec![output_column(1, "k"), output_column(3, "avg(v)")],
            )),
            children: vec![child],
        };
        let out = SplitAggregateRule.apply(&expr, &mut memo);
        assert_eq!(out.len(), 1, "avg must now produce a split alternative");
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.stage, AggStage::Global);
        assert_eq!(global.is_merge, vec![true]);
    }

    #[test]
    fn rejects_distinct_and_already_split_aggregate() {
        let mut memo = Memo::new();
        let child = values_group(&mut memo);
        let distinct = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![col_ref(1, "k")],
                vec![count_call(true)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![child],
        };
        assert!(SplitAggregateRule.apply(&distinct, &mut memo).is_empty());

        let already_split = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Local,
                vec![col_ref(1, "k")],
                vec![count_call(false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
                vec![false],
                true,
            )),
            children: vec![child],
        };
        assert!(
            SplitAggregateRule
                .apply(&already_split, &mut memo)
                .is_empty()
        );
    }
}
