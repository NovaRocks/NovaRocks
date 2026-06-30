//! Shared optimizer structural-match vocabulary for memo and tree binders.
//!
//! `Pattern` matches on operator kind plus structural shape only; field
//! predicates stay in the caller's rule predicate or bound-apply code.
//! `Pattern::MultiLeaf` is variadic only when it is the whole root pattern or
//! the tail child of `Pattern::Op`. Non-tail `MultiLeaf` is outside the grammar
//! and binders reject it instead of treating it as a single leaf.

use crate::sql::optimizer::operator::Operator;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub(crate) enum OpKind {
    Join,
    Limit,
    Sort,
    TopN,
    Project,
    Scan,
    Union,
    Filter,
    Aggregate,
    Window,
    Intersect,
    Except,
    Values,
    GenerateSeries,
    TableFunction,
    Repeat,
    CTEAnchor,
    CTEProduce,
    CTEConsume,
    Decode,
    AggregateStateMerge,
    AssertOneRow,
    Apply,
}

/// Structural match pattern. `Op` matches operator KIND only (not fields).
/// `Leaf` = opaque single child group/subtree (captured, not descended).
/// `MultiLeaf` = variable-arity trailing tail of opaque children.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Pattern {
    Op {
        kind: OpKind,
        children: Vec<Pattern>,
    },
    Leaf,
    MultiLeaf,
}

/// Logical operator -> its `OpKind`, or `None` for physical and marker
/// operators that are intentionally outside the pattern vocabulary.
pub(crate) fn op_kind(op: &Operator) -> Option<OpKind> {
    match op {
        Operator::LogicalFilter(_) => Some(OpKind::Filter),
        Operator::LogicalAggregate(_) => Some(OpKind::Aggregate),
        Operator::LogicalWindow(_) => Some(OpKind::Window),
        Operator::LogicalIntersect(_) => Some(OpKind::Intersect),
        Operator::LogicalExcept(_) => Some(OpKind::Except),
        Operator::LogicalValues(_) => Some(OpKind::Values),
        Operator::LogicalGenerateSeries(_) => Some(OpKind::GenerateSeries),
        Operator::LogicalTableFunction(_) => Some(OpKind::TableFunction),
        Operator::LogicalRepeat(_) => Some(OpKind::Repeat),
        Operator::LogicalCTEAnchor(_) => Some(OpKind::CTEAnchor),
        Operator::LogicalCTEProduce(_) => Some(OpKind::CTEProduce),
        Operator::LogicalCTEConsume(_) => Some(OpKind::CTEConsume),
        Operator::LogicalDecode(_) => Some(OpKind::Decode),
        Operator::LogicalAggregateStateMerge(_) => Some(OpKind::AggregateStateMerge),
        Operator::LogicalAssertOneRow(_) => Some(OpKind::AssertOneRow),
        Operator::LogicalApply(_) => Some(OpKind::Apply),
        Operator::LogicalJoin(_) => Some(OpKind::Join),
        Operator::LogicalLimit(_) => Some(OpKind::Limit),
        Operator::LogicalSort(_) => Some(OpKind::Sort),
        Operator::LogicalTopN(_) => Some(OpKind::TopN),
        Operator::LogicalProject(_) => Some(OpKind::Project),
        Operator::LogicalScan(_) => Some(OpKind::Scan),
        Operator::LogicalUnion(_) => Some(OpKind::Union),
        _ => None,
    }
}

/// Cheap root gate used by explore/implement before constructing a Binder:
/// equivalent to today's `rule.matches(&op)` on the root variant.
///
/// Used by tests and retained as a utility for future callers.
#[allow(dead_code)]
pub(crate) fn pattern_root_matches(p: &Pattern, op: &Operator) -> bool {
    match p {
        Pattern::Leaf | Pattern::MultiLeaf => true,
        Pattern::Op { kind, .. } => op_kind(op) == Some(*kind),
    }
}

#[cfg(test)]
mod tests {
    use super::{OpKind, Pattern, op_kind, pattern_root_matches};
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::{ApplyKind, ImvVersionRef, JoinKind, LiteralValue, OutputColumn};
    use crate::sql::optimizer::operator::{
        AggregateStateMergeOp, ApplyOp, AssertOneRowOp, CTEAnchorOp, CTEConsumeOp, CTEProduceOp,
        DecodeOp, ExceptOp, FilterOp, GenerateSeriesOp, ImvDeltaOp, ImvVersionOp, IntersectOp,
        LimitOp, LogicalAggregateOp, LogicalJoinOp, Operator, ProjectOp, RepeatOp, ScanOp, SortOp,
        TableFunctionOp, TopNOp, TopNPhase, UnionOp, ValuesOp, WindowOp,
    };
    use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};

    fn bool_literal_scalar(arena: &mut ScalarArena) -> ScalarId {
        arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(true))),
            DataType::Boolean,
            false,
        )
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn scan_op() -> ScanOp {
        ScanOp {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            stats_ref: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }
    }

    #[test]
    fn op_kind_covers_rbo_logical_variants() {
        let mut arena = ScalarArena::new();
        let scalar = bool_literal_scalar(&mut arena);
        let cases = vec![
            (
                Operator::LogicalFilter(FilterOp { predicate: scalar }),
                OpKind::Filter,
            ),
            (
                Operator::LogicalAggregate(LogicalAggregateOp::single(vec![], vec![], vec![])),
                OpKind::Aggregate,
            ),
            (
                Operator::LogicalWindow(WindowOp {
                    window_exprs: vec![],
                    output_columns: vec![],
                }),
                OpKind::Window,
            ),
            (
                Operator::LogicalIntersect(IntersectOp {
                    output_columns: vec![],
                    child_output_columns: vec![],
                }),
                OpKind::Intersect,
            ),
            (
                Operator::LogicalExcept(ExceptOp {
                    output_columns: vec![],
                    child_output_columns: vec![],
                }),
                OpKind::Except,
            ),
            (
                Operator::LogicalValues(ValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                OpKind::Values,
            ),
            (
                Operator::LogicalGenerateSeries(GenerateSeriesOp {
                    start: 1,
                    end: 3,
                    step: 1,
                    column_name: "x".to_string(),
                    alias: None,
                    output_column_id: ColumnId::new_for_test(1),
                }),
                OpKind::GenerateSeries,
            ),
            (
                Operator::LogicalTableFunction(TableFunctionOp {
                    function_name: "unnest".to_string(),
                    args: vec![],
                    output_columns: vec![],
                    alias: None,
                    is_left_join: false,
                }),
                OpKind::TableFunction,
            ),
            (
                Operator::LogicalRepeat(RepeatOp {
                    repeat_column_ref_list: vec![],
                    repeat_column_ref_ids: vec![],
                    grouping_ids: vec![],
                    all_rollup_columns: vec![],
                    all_rollup_column_ids: vec![],
                    grouping_key_aliases: vec![],
                    grouping_fn_args: vec![],
                    grouping_fn_arg_ids: vec![],
                    grouping_fn_ids: vec![],
                }),
                OpKind::Repeat,
            ),
            (
                Operator::LogicalCTEAnchor(CTEAnchorOp { cte_id: 1 }),
                OpKind::CTEAnchor,
            ),
            (
                Operator::LogicalCTEProduce(CTEProduceOp {
                    cte_id: 1,
                    output_columns: vec![],
                }),
                OpKind::CTEProduce,
            ),
            (
                Operator::LogicalCTEConsume(CTEConsumeOp {
                    cte_id: 1,
                    alias: "c".to_string(),
                    output_columns: vec![],
                    producer_column_ids: vec![],
                }),
                OpKind::CTEConsume,
            ),
            (
                Operator::LogicalDecode(DecodeOp {
                    mappings: vec![],
                    output_columns: vec![],
                }),
                OpKind::Decode,
            ),
            (
                Operator::LogicalAggregateStateMerge(AggregateStateMergeOp {
                    group_key_names: vec![],
                    aggregate_state_names: vec![],
                    change_op_column: "__op".to_string(),
                    output_columns: vec![],
                }),
                OpKind::AggregateStateMerge,
            ),
            (
                Operator::LogicalAssertOneRow(AssertOneRowOp {
                    subquery_text: "select 1".to_string(),
                }),
                OpKind::AssertOneRow,
            ),
            (
                Operator::LogicalApply(ApplyOp {
                    kind: ApplyKind::Scalar,
                    subquery_expr: scalar,
                    output_column: output_column(2, "subquery"),
                    inner_output_column_id: ColumnId::new_for_test(3),
                    correlation_column_ids: vec![],
                    correlation_conjuncts: vec![],
                    residual_predicate: None,
                    need_check_max_rows: false,
                    use_semi_anti: false,
                    uncorrelated_outer_predicate_columns: HashSet::new(),
                }),
                OpKind::Apply,
            ),
        ];

        for (op, expected) in cases {
            assert_eq!(op_kind(&op), Some(expected));
        }
    }

    #[test]
    fn maps_initial_a2_logical_operator_kinds() {
        let cases = vec![
            (
                Operator::LogicalJoin(LogicalJoinOp {
                    join_type: JoinKind::Inner,
                    condition: None,
                }),
                OpKind::Join,
            ),
            (
                Operator::LogicalLimit(LimitOp {
                    limit: Some(10),
                    offset: None,
                }),
                OpKind::Limit,
            ),
            (
                Operator::LogicalSort(SortOp {
                    items: vec![],
                    analytic_partition_exprs: vec![],
                    partition_limit: None,
                    topn_type: None,
                }),
                OpKind::Sort,
            ),
            (
                Operator::LogicalTopN(TopNOp {
                    items: vec![],
                    limit: Some(10),
                    offset: None,
                    phase: TopNPhase::Final,
                    is_split: false,
                }),
                OpKind::TopN,
            ),
            (
                Operator::LogicalProject(ProjectOp {
                    items: vec![],
                    output_qualifier: None,
                }),
                OpKind::Project,
            ),
            (Operator::LogicalScan(scan_op()), OpKind::Scan),
            (
                Operator::LogicalUnion(UnionOp {
                    all: true,
                    output_columns: vec![],
                    child_output_columns: vec![],
                }),
                OpKind::Union,
            ),
        ];

        for (op, expected) in cases {
            assert_eq!(op_kind(&op), Some(expected));
        }
    }

    #[test]
    fn pattern_root_matches_kind_only_not_fields() {
        let left = Operator::LogicalJoin(LogicalJoinOp {
            join_type: JoinKind::LeftOuter,
            condition: None,
        });
        let pattern = Pattern::Op {
            kind: OpKind::Join,
            children: vec![Pattern::Leaf, Pattern::Leaf],
        };
        assert!(pattern_root_matches(&pattern, &left));
    }

    #[test]
    fn unhandled_operator_kinds_return_none() {
        assert_eq!(op_kind(&Operator::PhysicalScan(scan_op())), None);
        assert_eq!(
            op_kind(&Operator::LogicalImvDelta(ImvDeltaOp {
                is_root: false,
                action_column: None,
                branch_scope: None,
            })),
            None
        );
        assert_eq!(
            op_kind(&Operator::LogicalImvVersion(ImvVersionOp {
                version_ref: ImvVersionRef::default(),
            })),
            None
        );
    }
}
