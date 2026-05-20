//! RBO rule registry.

use std::collections::HashMap;
use std::sync::Arc;

use super::rule::RewriteRule;
use crate::sql::optimizer::statistics::TableStatistics;

pub(crate) mod aggregate_pushdown;
pub(crate) mod column_pruning;
pub(crate) mod join_reorder;
pub(crate) mod predicate_pushdown;
pub(crate) mod ukfk;

pub(crate) fn column_pruning_rules() -> Vec<Box<dyn RewriteRule>> {
    vec![
        Box::new(column_pruning::PruneColumns),
        Box::new(ukfk::PruneUkFkJoin),
        Box::new(ukfk::EliminateUniqueAggregate),
    ]
}

/// Predicate pushdown rules only (no column pruning). Used in the
/// push → reorder → push pattern. Column pruning runs as a separate
/// final pass AFTER all pushdown and reorder passes are complete —
/// matching the legacy pipeline where prune_columns was always last.
/// Mixing PruneColumns with PushDownPredicate in a fixed-point loop
/// causes the needed-column set to shrink across iterations as
/// predicates get reshuffled, incorrectly dropping join-key or
/// select-list columns from scan required_columns.
pub(crate) fn predicate_pushdown_rbo_rules() -> Vec<Box<dyn RewriteRule>> {
    predicate_pushdown::predicate_pushdown_rules()
}

/// Join reorder rule only. Called as a SEPARATE pass between two
/// structural_rbo_rules passes (the "push, reorder, push" pattern).
/// Do NOT mix with structural rules in a single fixed-point — pushdown
/// and reorder oscillate and either time out or produce column-scope errors.
#[allow(dead_code)]
pub(crate) fn join_reorder_rules(
    table_stats: &HashMap<String, TableStatistics>,
) -> Vec<Box<dyn RewriteRule>> {
    vec![Box::new(join_reorder::JoinReorderRule::new(Arc::new(
        table_stats.clone(),
    )))]
}

/// All RBO rules including join reorder. For registry test only;
/// production code calls predicate_pushdown_rbo_rules(), join_reorder,
/// and column_pruning_rules() separately per the four-pass pattern.
#[allow(dead_code)]
pub(crate) fn all_rbo_rules(
    table_stats: &HashMap<String, TableStatistics>,
) -> Vec<Box<dyn RewriteRule>> {
    let mut all = Vec::new();
    all.extend(predicate_pushdown_rbo_rules());
    all.extend(column_pruning_rules());
    all.extend(join_reorder_rules(table_stats));
    all.extend(aggregate_pushdown::aggregate_pushdown_rules(table_stats));
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_expected_rules() {
        let rules = all_rbo_rules(&HashMap::new());
        assert_eq!(rules.len(), 10);
        let mut names: Vec<&str> = rules.iter().map(|r| r.name()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "AggregatePushdown",
                "EliminateUniqueAggregate",
                "JoinReorder",
                "PruneColumns",
                "PruneUkFkJoin",
                "PushDownPredicateAggregate",
                "PushDownPredicateJoin",
                "PushDownPredicateProject",
                "PushDownPredicateScan",
                "PushSemiAntiRightOnlyCondition",
            ]
        );
    }
}
