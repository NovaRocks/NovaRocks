// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Query logical rewrite rule registry.

use crate::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) mod aggregate_pushdown;
pub(crate) mod column_pruning;
pub(crate) mod derive_join_not_null;
pub(crate) mod fold_constant;
pub(crate) mod predicate_pushdown;
pub(crate) mod ranking_window_predicate_pushdown;
pub(crate) mod subquery;
pub(crate) mod ukfk;
pub(crate) mod union_distinct_to_aggregate;
pub(crate) mod utils;
pub(crate) mod variant_path_pushdown;

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

/// Constant folding. Runs in `LogicalNormalize` before any structural
/// rewrite so a `Cast(Literal)` has already collapsed into a bare literal by
/// the time predicate pushdown extracts static predicates.
pub(crate) fn fold_constant_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(fold_constant::FoldConstant)]
}

pub(crate) fn union_distinct_normalize_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(
        union_distinct_to_aggregate::UnionDistinctToAggregate,
    )]
}

/// All known query rewrite rules for registry and rule-name validation.
/// Production ordering is defined by query_rewrite_pipeline(), not this set.
#[allow(dead_code)]
pub(crate) fn all_query_rewrite_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    let mut all = Vec::new();
    all.extend(union_distinct_normalize_rules());
    all.extend(fold_constant_rules());
    all.extend(predicate_pushdown_rules());
    all.extend(predicate_move_around_rules());
    all.extend(column_pruning_rules());
    all.extend(variant_path_pushdown_rules());
    all.extend(aggregate_pushdown::aggregate_pushdown_rules());
    all.push(Box::new(derive_join_not_null::DeriveJoinNotNullPredicate));
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_expected_rules() {
        let rules = all_query_rewrite_rules();
        let mut names: Vec<&str> = rules.iter().map(|r| r.name()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "AggregatePushdown",
                "DeriveJoinNotNullPredicate",
                "EliminateUniqueAggregate",
                "FoldConstant",
                "JoinPredicateMoveAround",
                "PruneAggregateColumns",
                "PruneCTEAnchorColumns",
                "PruneCTEConsumeColumns",
                "PruneCTEProduceColumns",
                "PruneExceptColumns",
                "PruneFilterColumns",
                "PruneIntersectColumns",
                "PruneJoinColumns",
                "PruneLimitColumns",
                "PruneProjectColumns",
                "PruneRepeatColumns",
                "PruneScanColumns",
                "PruneSortColumns",
                "PruneSystemTableColumns",
                "PruneTableFunctionColumns",
                "PruneUkFkJoin",
                "PruneUnionColumns",
                "PruneValuesColumns",
                "PruneWindowColumns",
                "PushDownPredicateAggregate",
                "PushDownPredicateJoin",
                "PushDownPredicateProject",
                "PushDownPredicateScan",
                "PushSemiAntiRightOnlyCondition",
                "UnionDistinctToAggregate",
                "VariantPathPushdown",
            ]
        );
    }
}
