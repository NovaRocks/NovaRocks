//! RBO rule trait. Operates directly on a LogicalPlan tree (no Memo).
//!
//! The driver (see `rbo::driver`) owns traversal; rules are pure local
//! rewrites. A rule's `apply` MUST NOT recurse into its children. The
//! driver visits children bottom-up, then applies all enabled rules at
//! each node to a node-level fixed-point.

use crate::sql::planner::plan::LogicalPlan;

/// A logical-plan rewrite rule applied during the RBO phase.
pub(crate) trait RewriteRule: Send + Sync {
    /// Stable rule name used for `OptimizerOptions::is_enabled` lookups
    /// and trace logging. Must be unique across both `RewriteRule` (RBO)
    /// and `Rule` (CBO) namespaces.
    fn name(&self) -> &'static str;

    /// Cheap discriminant precheck. If false, `apply` is not called.
    /// Implementations should match on the operator kind without deep
    /// inspection or recursion.
    fn matches(&self, plan: &LogicalPlan) -> bool;

    /// Try to rewrite the plan rooted at this node.
    /// Return `Some(new_plan)` when the rule fires and produces a different
    /// shape; `None` when the rule is a no-op at this node.
    /// MUST NOT recurse into children.
    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::planner::plan::ScanNode;

    /// Test-only helper: a no-op rule that never fires.
    struct NoopRule;
    impl RewriteRule for NoopRule {
        fn name(&self) -> &'static str {
            "TestNoop"
        }
        fn matches(&self, _plan: &LogicalPlan) -> bool {
            false
        }
        fn apply(&self, _plan: LogicalPlan) -> Option<LogicalPlan> {
            None
        }
    }

    /// Test-only helper: a rule that fires once on Scan and turns it into
    /// itself (returns Some with structurally-identical input). Used to
    /// verify the driver detects "no observable change" via Option semantics.
    struct AlwaysFireOnScan;
    impl RewriteRule for AlwaysFireOnScan {
        fn name(&self) -> &'static str {
            "TestAlwaysFireOnScan"
        }
        fn matches(&self, plan: &LogicalPlan) -> bool {
            matches!(plan, LogicalPlan::Scan(_))
        }
        fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
            // Return Some(plan) — driver treats Some as "changed".
            Some(plan)
        }
    }

    fn dummy_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
        })
    }

    #[test]
    fn noop_rule_does_not_match_or_apply() {
        let rule = NoopRule;
        let plan = dummy_scan();
        assert_eq!(rule.name(), "TestNoop");
        assert!(!rule.matches(&plan));
        assert!(rule.apply(plan).is_none());
    }

    #[test]
    fn always_fire_rule_matches_scan_only() {
        let rule = AlwaysFireOnScan;
        let scan = dummy_scan();
        assert!(rule.matches(&scan));
        assert!(rule.apply(scan).is_some());
    }
}
