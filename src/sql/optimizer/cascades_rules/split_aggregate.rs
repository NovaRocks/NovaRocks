use crate::sql::column_id::ColumnId;
use crate::sql::common::OutputColumn;
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{
    AggStage, LogicalAggregateOp, Operator, ScalarAggregateSpec,
};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::scalar_expr;

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

        let local_output_columns = local_output_columns(agg, &memo.scalars);
        remember_group_key_output_displays(&mut memo.scalars, &agg.group_by, &local_output_columns);
        let local_group_by = aggregate_group_key_output_ref(
            &mut memo.scalars,
            &local_output_columns,
            agg.group_by.len(),
        );
        remember_group_key_output_displays(&mut memo.scalars, &local_group_by, &agg.output_columns);
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

fn remember_group_key_output_displays(
    scalars: &mut ScalarArena,
    group_by: &[ScalarId],
    output_columns: &[OutputColumn],
) {
    for (scalar_id, output) in group_by.iter().zip(output_columns.iter()) {
        scalars.remember_column_display_from_scalar(output.column_id, *scalar_id);
    }
}

fn is_eligible(agg: &LogicalAggregateOp) -> bool {
    agg.stage == AggStage::Single
        && !agg.is_split
        && agg.is_merge.iter().all(|flag| !*flag)
        && (!agg.aggregates.is_empty() || !agg.group_by.is_empty())
        && agg.aggregates.iter().all(is_splittable_aggregate)
}

fn is_splittable_aggregate(call: &ScalarAggregateSpec) -> bool {
    use crate::sql::agg_mergeability::{AggMergeability, scalar_aggregate_mergeability};
    scalar_aggregate_mergeability(call) == AggMergeability::TwoPhase
}

fn local_output_columns(agg: &LogicalAggregateOp, arena: &ScalarArena) -> Vec<OutputColumn> {
    let mut columns = Vec::with_capacity(agg.group_by.len() + agg.aggregates.len());
    columns.extend(agg.group_by.iter().enumerate().map(|(idx, expr)| {
        let name = scalar_expr::scalar_display_name(arena, *expr);
        // Non-ColumnRef group keys (constant/alias/expression, e.g. `'a' as g`)
        // reuse the original aggregate's group output id *by position* (the
        // first group_by.len() output columns are the group keys, in order).
        // The previous by-name lookup returned ColumnId::UNSET when the output
        // name was a SELECT alias rather than the expression's display name,
        // which the id-binding verifier rejects once the aggregate is split.
        let column_id = match arena.node(*expr) {
            ScalarNode::ColumnRef(column_id) => *column_id,
            _ => agg
                .output_columns
                .get(idx)
                .map(|output| output.column_id)
                .filter(|id| *id != ColumnId::UNSET)
                .unwrap_or_else(|| {
                    group_key_output_column_id(arena, *expr, &name, &agg.output_columns)
                }),
        };
        OutputColumn {
            column_id,
            name,
            data_type: arena.data_type(*expr).clone(),
            nullable: arena.nullable(*expr),
            is_internal: false,
        }
    }));
    columns.extend(agg.aggregates.iter().enumerate().map(|(idx, call)| {
        let name = scalar_expr::aggregate_display_name(
            arena,
            &call.name,
            &call.args,
            call.distinct,
            &call.order_by,
        );
        let source_output = aggregate_output_column(agg, idx);
        OutputColumn {
            column_id: aggregate_output_column_id(&name, source_output),
            name,
            data_type: source_output
                .map(|output| output.data_type.clone())
                .unwrap_or_else(|| arrow::datatypes::DataType::Null),
            nullable: true,
            is_internal: true,
        }
    }));
    columns
}

pub(crate) fn group_key_output_column_id(
    arena: &ScalarArena,
    expr: ScalarId,
    display_name: &str,
    existing_outputs: &[OutputColumn],
) -> ColumnId {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => *column_id,
        _ => existing_outputs
            .iter()
            .find(|output| output.name == display_name)
            .map(|output| output.column_id)
            .unwrap_or(ColumnId::UNSET),
    }
}

fn aggregate_output_column_id(
    display_name: &str,
    source_output: Option<&OutputColumn>,
) -> ColumnId {
    source_output
        .filter(|output| output.name == display_name || output.column_id != ColumnId::UNSET)
        .map(|output| output.column_id)
        .unwrap_or(ColumnId::UNSET)
}

fn aggregate_output_column(
    agg: &LogicalAggregateOp,
    aggregate_idx: usize,
) -> Option<&OutputColumn> {
    agg.output_columns.get(agg.group_by.len() + aggregate_idx)
}

pub(crate) fn aggregate_group_key_output_ref(
    arena: &mut ScalarArena,
    local_output_columns: &[OutputColumn],
    group_by_len: usize,
) -> Vec<ScalarId> {
    local_output_columns
        .iter()
        .take(group_by_len)
        .map(|output| {
            arena.remember_project_output_display(output.column_id, None, output.name.clone());
            arena.intern(
                ScalarNode::ColumnRef(output.column_id),
                output.data_type.clone(),
                output.nullable,
            )
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
    use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp, ValuesOp};
    use crate::sql::planner::optimizer_bridge::scalar::materialize;
    use crate::sql::planner::optimizer_bridge::scalar::{intern_aggregate_calls, intern_exprs};
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

    fn single_agg(
        memo: &mut Memo,
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        output_columns: Vec<OutputColumn>,
    ) -> LogicalAggregateOp {
        let group_by = intern_exprs(&mut memo.scalars, &group_by);
        let aggregates = intern_aggregate_calls(&mut memo.scalars, &aggregates);
        LogicalAggregateOp::single(group_by, aggregates, output_columns)
    }

    fn staged_agg(
        memo: &mut Memo,
        stage: AggStage,
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        output_columns: Vec<OutputColumn>,
        is_merge: Vec<bool>,
        is_split: bool,
    ) -> LogicalAggregateOp {
        let group_by = intern_exprs(&mut memo.scalars, &group_by);
        let aggregates = intern_aggregate_calls(&mut memo.scalars, &aggregates);
        LogicalAggregateOp::staged(
            stage,
            group_by,
            aggregates,
            output_columns,
            is_merge,
            is_split,
        )
    }

    fn values_group(memo: &mut Memo) -> usize {
        let id = memo.next_expr_id();
        memo.new_group(MExpr {
            id,
            op: Operator::LogicalValues(ValuesOp {
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
            op: Operator::LogicalAggregate(single_agg(
                memo,
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
            op: Operator::LogicalAggregate(single_agg(
                memo,
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
            op: Operator::LogicalAggregate(single_agg(
                memo,
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
        assert!(materialize(&memo.scalars, global.group_by[0]).nullable);
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
        let group_by = materialize(&memo.scalars, global.group_by[0]);
        let ExprKind::ColumnRef { column_id, .. } = &group_by.kind else {
            panic!("expected global group key column ref");
        };
        assert_eq!(*column_id, ColumnId::new_for_test(1));
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
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
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
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![col_ref(1, "k")],
                vec![count_call(true)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![child],
        };
        assert!(SplitAggregateRule.apply(&distinct, &mut memo).is_empty());

        let already_split = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(staged_agg(
                &mut memo,
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
