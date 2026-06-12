//! Query logical rewrite rule registry.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::statistics::TableStatistics;

pub(crate) mod aggregate_pushdown;
pub(crate) mod column_pruning;
pub(crate) mod derive_join_not_null;
pub(crate) mod join_reorder;
pub(crate) mod low_cardinality_dict;
pub(crate) mod predicate_pushdown;
pub(crate) mod subquery;
pub(crate) mod ukfk;
pub(crate) mod utils;
pub(crate) mod variant_path_pushdown;

pub(crate) fn low_cardinality_dictionary_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(
        low_cardinality_dict::LowCardinalityDictionaryRewriteRule,
    )]
}

pub(crate) fn column_pruning_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    let mut rules = column_pruning::all_rules();
    rules.push(Box::new(ukfk::PruneUkFkJoin));
    rules.push(Box::new(ukfk::EliminateUniqueAggregate));
    rules
}

/// Reusable predicate pushdown rules only (no column pruning). Query rewrite
/// stages decide where to run these rules; column pruning stays in its own pass
/// after predicate placement has stabilized.
/// Mixing PruneColumns with PushDownPredicate in a fixed-point loop
/// causes the needed-column set to shrink across iterations as
/// predicates get reshuffled, incorrectly dropping join-key or
/// select-list columns from scan required_columns.
pub(crate) fn predicate_pushdown_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    predicate_pushdown::predicate_pushdown_rules()
}

pub(crate) fn predicate_move_around_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    predicate_pushdown::predicate_move_around_rules()
}

pub(crate) fn variant_path_pushdown_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(variant_path_pushdown::VariantPathPushdownRule)]
}

/// Join reorder rule only. Called as a SEPARATE pass between two
/// predicate pushdown passes (the "push, reorder, push" pattern).
/// Do NOT mix with structural rules in a single fixed-point — pushdown
/// and reorder oscillate and either time out or produce column-scope errors.
#[allow(dead_code)]
pub(crate) fn join_reorder_rules(
    table_stats: &HashMap<String, TableStatistics>,
) -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(join_reorder::JoinReorderRule::new(Arc::new(
        table_stats.clone(),
    )))]
}

/// All known query rewrite rules for registry and rule-name validation.
/// Production ordering is defined by query_rewrite_pipeline(), not this set.
#[allow(dead_code)]
pub(crate) fn all_query_rewrite_rules(
    table_stats: &HashMap<String, TableStatistics>,
) -> Vec<Box<dyn LogicalRewriteRule>> {
    let mut all = Vec::new();
    all.extend(predicate_pushdown_rules());
    all.extend(predicate_move_around_rules());
    all.extend(column_pruning_rules());
    all.extend(join_reorder_rules(table_stats));
    all.extend(variant_path_pushdown_rules());
    all.extend(aggregate_pushdown::aggregate_pushdown_rules(table_stats));
    all.extend(low_cardinality_dictionary_rules());
    all.push(Box::new(derive_join_not_null::DeriveJoinNotNullPredicate));
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_expected_rules() {
        let rules = all_query_rewrite_rules(&HashMap::new());
        // 17 v2 pruning rules + 2 ukfk + 1 JoinReorder + 1 VariantPathPushdown
        // + 1 AggregatePushdown
        // + 1 LowCardinalityDictionaryRewrite + 5 predicate pushdown rules
        // + 1 predicate move-around rule + 1 DeriveJoinNotNullPredicate = 30
        assert_eq!(rules.len(), 30);
        let mut names: Vec<&str> = rules.iter().map(|r| r.name()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "AggregatePushdown",
                "DeriveJoinNotNullPredicate",
                "EliminateUniqueAggregate",
                "JoinPredicateMoveAround",
                "JoinReorder",
                "LowCardinalityDictionaryRewrite",
                "PruneAggregateColumns",
                "PruneCTEAnchorColumns",
                "PruneCTEConsumeColumns",
                "PruneCTEProduceColumns",
                "PruneDecodeColumns",
                "PruneExceptColumns",
                "PruneFilterColumns",
                "PruneIntersectColumns",
                "PruneJoinColumns",
                "PruneLimitColumns",
                "PruneProjectColumns",
                "PruneRepeatColumns",
                "PruneScanColumns",
                "PruneSortColumns",
                "PruneTableFunctionColumns",
                "PruneUkFkJoin",
                "PruneUnionColumns",
                "PruneWindowColumns",
                "PushDownPredicateAggregate",
                "PushDownPredicateJoin",
                "PushDownPredicateProject",
                "PushDownPredicateScan",
                "PushSemiAntiRightOnlyCondition",
                "VariantPathPushdown",
            ]
        );
    }
}
