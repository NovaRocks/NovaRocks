//! Transformation rule: LogicalTopN(FINAL, !split) -> LogicalTopN(FINAL, split=true) over LogicalTopN(PARTIAL).
//!
//! Mirrors StarRocks's SplitTopNRule.java. Cost search picks between the
//! single-stage TopN (original) and this two-stage alternative.

use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalTopNOp, Operator, TopNPhase};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};

pub(crate) struct SplitTopN;

impl Rule for SplitTopN {
    fn name(&self) -> &str {
        "SplitTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalTopN(t) if t.phase == TopNPhase::Final && !t.is_split
        )
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalTopN(src) = &expr.op else {
            return vec![];
        };
        // Finite limit required; plain ORDER BY without LIMIT is out of scope.
        let limit = match src.limit {
            Some(l) if l >= 0 => l,
            _ => return vec![],
        };
        let offset = src.offset.unwrap_or(0).max(0);
        if offset > 0 {
            // Standalone execution already preserves LIMIT/OFFSET correctly for
            // single-stage TopN. Split TopN with a non-zero final offset still
            // needs tighter parity work in the merging exchange path, so keep
            // the conservative single-stage plan for semantic correctness.
            return vec![];
        }
        // Saturating add: if L+O would overflow, cap at i64::MAX (effectively
        // means "partial passes everything through"; cost search will prefer
        // single-stage in that corner case).
        let partial_limit = limit.saturating_add(offset);

        // PARTIAL child: same sort items, larger limit, zero offset.
        let partial_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: src.items.clone(),
                limit: Some(partial_limit),
                offset: Some(0),
                phase: TopNPhase::Partial,
                is_split: false,
            }),
            children: expr.children.clone(),
        };
        let partial_group = memo.new_group(partial_mexpr);

        // FINAL with split flag, original limit/offset. Child = new partial group.
        let final_expr = NewExpr {
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: src.items.clone(),
                limit: src.limit,
                offset: src.offset,
                phase: TopNPhase::Final,
                is_split: true,
            }),
            children: vec![partial_group],
        };

        vec![final_expr]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::memo::Memo;
    use crate::sql::optimizer::operator::{LogicalScanOp, LogicalTopNOp};

    fn mk_scan_group(memo: &mut Memo) -> usize {
        let m = MExpr {
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
        };
        memo.new_group(m)
    }

    #[test]
    fn fires_on_final_unsplit_with_limit() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: Some(100),
                offset: Some(0),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN.apply(&topn_mexpr, &mut memo);
        assert_eq!(out.len(), 1, "expected one split alternative");
        match &out[0].op {
            Operator::LogicalTopN(t) => {
                assert_eq!(t.phase, TopNPhase::Final);
                assert!(t.is_split);
                assert_eq!(t.limit, Some(100));
                assert_eq!(t.offset, Some(0));
            }
            other => panic!("expected LogicalTopN final+split, got {:?}", other),
        }
        // FINAL's child is a new group containing the PARTIAL TopN.
        assert_eq!(out[0].children.len(), 1);
        let partial_group = &memo.groups[out[0].children[0]];
        match &partial_group.logical_exprs[0].op {
            Operator::LogicalTopN(t) => {
                assert_eq!(t.phase, TopNPhase::Partial);
                assert!(!t.is_split);
                assert_eq!(t.limit, Some(100), "partial limit must be L+O = 100+0");
                assert_eq!(t.offset, Some(0));
            }
            other => panic!("expected LogicalTopN partial, got {:?}", other),
        }
        assert_eq!(partial_group.logical_exprs[0].children, vec![scan_group]);
    }

    #[test]
    fn does_not_fire_with_non_zero_offset() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: Some(100),
                offset: Some(10),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN.apply(&topn_mexpr, &mut memo);
        assert!(
            out.is_empty(),
            "non-zero offset should stay single-stage for now"
        );
    }

    #[test]
    fn does_not_fire_on_partial() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: Some(100),
                offset: None,
                phase: TopNPhase::Partial,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        assert!(
            !SplitTopN.matches(&topn_mexpr.op),
            "rule must not match on PARTIAL phase"
        );
    }

    #[test]
    fn does_not_fire_when_already_split() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: Some(100),
                offset: None,
                phase: TopNPhase::Final,
                is_split: true,
            }),
            children: vec![scan_group],
        };
        assert!(
            !SplitTopN.matches(&topn_mexpr.op),
            "rule must not match when already split"
        );
    }

    #[test]
    fn does_not_fire_without_limit() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: None,
                offset: Some(5),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN.apply(&topn_mexpr, &mut memo);
        assert!(out.is_empty(), "no limit => out of scope");
    }
}
