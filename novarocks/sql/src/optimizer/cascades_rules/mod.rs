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

//! Memo/Cascades rule registration.

pub(crate) mod equivalence_predicate;
pub(crate) mod implement;
pub(crate) mod join_associativity;
pub(crate) mod join_commutativity;
pub(crate) mod multi_join_reorder;
pub(crate) mod mv_rewrite;
pub(crate) mod push_topn_through_join;
pub(crate) mod push_topn_to_preagg;
pub(crate) mod sort_limit_to_top_n;
pub(crate) mod split_aggregate;
pub(crate) mod split_distinct_agg;
pub(crate) mod split_top_n;
pub(crate) mod topn_compactness;

use super::rule::Rule;

/// Returns all implementation rules (logical -> physical).
pub(crate) fn all_implementation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(implement::ScanToPhysical),
        Box::new(implement::FilterToPhysical),
        Box::new(implement::ProjectToPhysical),
        Box::new(implement::JoinToHashJoin),
        Box::new(implement::JoinToNestLoop),
        Box::new(implement::AggToHashAgg),
        Box::new(implement::SortToPhysical),
        Box::new(implement::LimitToPhysical),
        Box::new(implement::AssertOneRowToPhysical),
        Box::new(implement::TopNToPhysical), // NEW
        Box::new(implement::WindowToPhysical),
        Box::new(implement::CTEAnchorToPhysical),
        Box::new(implement::CTEProduceToPhysical),
        Box::new(implement::CTEConsumeToPhysical),
        Box::new(implement::RepeatToPhysical),
        Box::new(implement::ChangeEventExpandToPhysical),
        Box::new(implement::UnionToPhysical),
        Box::new(implement::IntersectToPhysical),
        Box::new(implement::ExceptToPhysical),
        Box::new(implement::ValuesToPhysical),
        Box::new(implement::SystemTableToPhysical),
        Box::new(implement::GenerateSeriesToPhysical),
        Box::new(implement::TableFunctionToPhysical),
        Box::new(split_distinct_agg::SplitDistinctAgg),
    ]
}

/// Returns all transformation rules (logical -> logical).
pub(crate) fn all_transformation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
        Box::new(equivalence_predicate::InnerJoinEquivalencePredicateRule),
        Box::new(sort_limit_to_top_n::SortLimitToTopN),
        Box::new(split_aggregate::SplitAggregateRule),
        Box::new(split_top_n::SplitTopN),
        // The compiled path owns GroupedStates before complete aggregation.
        Box::new(push_topn_to_preagg::PushDownTopNToPreAgg),
        Box::new(topn_compactness::MergeConsecutiveTopN),
        Box::new(topn_compactness::RemoveRedundantSortUnderTopN),
        Box::new(topn_compactness::PushTopNIntoScan),
        Box::new(topn_compactness::PushTopNThroughProject),
        Box::new(push_topn_through_join::PushTopNThroughJoin),
        Box::new(topn_compactness::PushTopNThroughSetOp),
    ]
}
