//! Transformation rule: LogicalLimit(LogicalSort(x)) -> LogicalTopN(x).
//!
//! Produces an equivalent LogicalTopN expression in the Limit's group.
//! The Limit group's children are replaced: where Limit had [sort_group],
//! TopN has [grandchild_group].

use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{Operator, TopNOp, TopNPhase};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};

pub(crate) struct SortLimitToTopN;

impl Rule for SortLimitToTopN {
    fn name(&self) -> &str {
        "SortLimitToTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalLimit(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalLimit(limit_op) = &expr.op else {
            return vec![];
        };
        // A LogicalTopN without a limit is just a Sort -- don't rewrite that case,
        // let the plain Sort path handle it.
        if limit_op.limit.is_none() {
            return vec![];
        }
        // LogicalLimit has exactly one child.
        if expr.children.len() != 1 {
            return vec![];
        }
        let child_group_id = expr.children[0];

        // Look for any LogicalSort MExpr in the child group.
        let child_group = match memo.groups.get(child_group_id) {
            Some(g) => g,
            None => return vec![],
        };

        let mut results = Vec::new();
        for child_mexpr in child_group.logical_exprs.iter() {
            let Operator::LogicalSort(sort_op) = &child_mexpr.op else {
                continue;
            };
            // A partition-topn Sort carries per-partition truncation semantics
            // (partition_limit / topn_type set by RankingWindowPredicatePushdown).
            // Converting it to a plain LogicalTopN would silently discard the
            // partition_limit, producing wrong results. Skip such sorts; the
            // partition-topn path handles them independently.
            if sort_op.partition_limit.is_some() {
                continue;
            }
            if child_mexpr.children.len() != 1 {
                continue;
            }
            let grandchild_group_id = child_mexpr.children[0];
            results.push(NewExpr {
                op: Operator::LogicalTopN(TopNOp {
                    items: sort_op.items.clone(),
                    limit: limit_op.limit,
                    offset: limit_op.offset,
                    phase: TopNPhase::Final,
                    is_split: false,
                }),
                children: vec![grandchild_group_id],
            });
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::memo::Memo;
    use crate::sql::optimizer::operator::{LimitOp, ScanOp, SortOp};
    use crate::sql::optimizer::scalar::intern_typed;

    fn mk_scan_mexpr(memo: &mut Memo) -> MExpr {
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(ScanOp {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::catalog::ScanSource::StarRocks {
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
        }
    }

    #[test]
    fn fires_when_limit_has_sort_child() {
        let mut memo = Memo::new();
        let scan_mexpr = mk_scan_mexpr(&mut memo);
        let scan_group = memo.new_group(scan_mexpr);

        let sort_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalSort(SortOp {
                items: vec![],
                analytic_partition_exprs: Vec::new(),
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![scan_group],
        };
        let sort_group = memo.new_group(sort_mexpr);

        let limit_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalLimit(LimitOp {
                limit: Some(100),
                offset: None,
            }),
            children: vec![sort_group],
        };

        let rule = SortLimitToTopN;
        let out = rule.apply(&limit_mexpr, &mut memo);
        assert_eq!(out.len(), 1, "expected one TopN alternative");
        match &out[0].op {
            Operator::LogicalTopN(t) => {
                assert_eq!(t.limit, Some(100));
                assert_eq!(t.offset, None);
            }
            other => panic!("expected LogicalTopN, got {:?}", other),
        }
        // Children must point to the scan group, skipping the sort.
        assert_eq!(out[0].children, vec![scan_group]);
    }

    #[test]
    fn does_not_fire_when_limit_has_non_sort_child() {
        let mut memo = Memo::new();
        let scan_mexpr = mk_scan_mexpr(&mut memo);
        let scan_group = memo.new_group(scan_mexpr);

        let limit_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalLimit(LimitOp {
                limit: Some(10),
                offset: None,
            }),
            children: vec![scan_group],
        };

        let rule = SortLimitToTopN;
        let out = rule.apply(&limit_mexpr, &mut memo);
        assert!(
            out.is_empty(),
            "expected no alternatives without a Sort child"
        );
    }

    #[test]
    fn does_not_fire_when_limit_is_none() {
        // Edge case: LIMIT clause can be absent (OFFSET-only). Don't rewrite
        // because a TopN without a limit is just a Sort.
        let mut memo = Memo::new();
        let scan_mexpr = mk_scan_mexpr(&mut memo);
        let scan_group = memo.new_group(scan_mexpr);

        let sort_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalSort(SortOp {
                items: vec![],
                analytic_partition_exprs: Vec::new(),
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![scan_group],
        };
        let sort_group = memo.new_group(sort_mexpr);

        let limit_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalLimit(LimitOp {
                limit: None,
                offset: Some(5),
            }),
            children: vec![sort_group],
        };

        let rule = SortLimitToTopN;
        let out = rule.apply(&limit_mexpr, &mut memo);
        assert!(out.is_empty(), "expected no rewrite when limit is None");
    }

    #[test]
    fn does_not_fire_when_sort_has_partition_limit() {
        // Regression: a Sort with partition_limit.is_some() is a partition-topn
        // Sort placed by RankingWindowPredicatePushdown.  Converting it to a plain
        // LogicalTopN would silently discard the per-partition truncation semantics.
        // The rule must skip such a Sort and return no alternatives.
        let mut memo = Memo::new();
        let scan_mexpr = mk_scan_mexpr(&mut memo);
        let scan_group = memo.new_group(scan_mexpr);
        let partition_expr = crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                column_id: crate::sql::column_id::ColumnId(1),
                qualifier: None,
                column: "p".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
        };
        let partition_expr = intern_typed(&mut memo.scalars, &partition_expr);

        let sort_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalSort(SortOp {
                items: vec![],
                // non-empty: partition-topn Sort always carries these
                analytic_partition_exprs: vec![partition_expr],
                partition_limit: Some(2),
                topn_type: Some(crate::exec::node::sort::SortTopNType::Rank),
            }),
            children: vec![scan_group],
        };
        let sort_group = memo.new_group(sort_mexpr);

        let limit_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalLimit(LimitOp {
                limit: Some(10),
                offset: None,
            }),
            children: vec![sort_group],
        };

        let rule = SortLimitToTopN;
        let out = rule.apply(&limit_mexpr, &mut memo);
        assert!(
            out.is_empty(),
            "SortLimitToTopN must not rewrite a partition-topn Sort (partition_limit.is_some())"
        );
    }
}
