//! Transformation rule: LogicalLimit(LogicalSort(x)) -> LogicalTopN(x).
//!
//! Produces an equivalent LogicalTopN expression in the Limit's group.
//! The Limit group's children are replaced: where Limit had [sort_group],
//! TopN has [grandchild_group].

use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalTopNOp, Operator, TopNPhase};
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
            if child_mexpr.children.len() != 1 {
                continue;
            }
            let grandchild_group_id = child_mexpr.children[0];
            results.push(NewExpr {
                op: Operator::LogicalTopN(LogicalTopNOp {
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
    use crate::sql::optimizer::operator::{LogicalLimitOp, LogicalScanOp, LogicalSortOp};

    fn mk_scan_mexpr(memo: &mut Memo) -> MExpr {
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
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
            op: Operator::LogicalSort(LogicalSortOp { items: vec![] }),
            children: vec![scan_group],
        };
        let sort_group = memo.new_group(sort_mexpr);

        let limit_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalLimit(LogicalLimitOp {
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
            op: Operator::LogicalLimit(LogicalLimitOp {
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
            op: Operator::LogicalSort(LogicalSortOp { items: vec![] }),
            children: vec![scan_group],
        };
        let sort_group = memo.new_group(sort_mexpr);

        let limit_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalLimit(LogicalLimitOp {
                limit: None,
                offset: Some(5),
            }),
            children: vec![sort_group],
        };

        let rule = SortLimitToTopN;
        let out = rule.apply(&limit_mexpr, &mut memo);
        assert!(out.is_empty(), "expected no rewrite when limit is None");
    }
}
