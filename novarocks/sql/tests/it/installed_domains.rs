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

//! Full SQL-authored final-plan checks for optional optimization domains.
use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_physical_plan::{NodeKind, RuntimeFilterDomain, TopNReduction};
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use novarocks_sql_testkit::{sql_source, sql_source_with_declared_fields};
use std::sync::Arc;

fn grouped_state_topns(source: &SqlAuthoredPhysicalPlan) -> usize {
    source
        .plan()
        .fragments()
        .values()
        .flat_map(|fragment| fragment.nodes().values())
        .filter(|node| {
            matches!(
                &node.kind,
                NodeKind::TopN {
                    reduction: TopNReduction::GroupedStates { .. },
                    ..
                }
            )
        })
        .count()
}
fn ordered_filters(source: &SqlAuthoredPhysicalPlan) -> usize {
    source
        .plan()
        .runtime_filters()
        .values()
        .filter(|filter| matches!(&filter.domain, RuntimeFilterDomain::Ordered { .. }))
        .count()
}
fn membership_filters(source: &SqlAuthoredPhysicalPlan) -> usize {
    source
        .plan()
        .runtime_filters()
        .values()
        .filter(|filter| matches!(&filter.domain, RuntimeFilterDomain::Membership { .. }))
        .count()
}

#[test]
fn grouped_topn_completed_plan_uses_only_installed_keys() {
    let sql = "SELECT k, COUNT(*) AS n FROM fixture.source \
               GROUP BY k ORDER BY k LIMIT 1";
    let accepted = sql_source(sql, DataType::Int64);
    let rejected = sql_source(sql, DataType::Float64);
    // Positive selection must be observed; an unselected optimization is not
    // evidence for this pair. The source fixture has Missing statistics.
    assert!(
        grouped_state_topns(&accepted) > 0,
        "the admitted key must select GroupedStates"
    );
    assert_eq!(grouped_state_topns(&rejected), 0);
}

#[test]
fn ordered_filter_completed_plan_omits_nested_scan_payload() {
    let sql = "SELECT k, COUNT(a) AS n FROM fixture.source \
               GROUP BY k ORDER BY k LIMIT 1";
    let compile = |payload| {
        sql_source_with_declared_fields(
            sql,
            &[
                Field::new("k", DataType::Utf8, false),
                Field::new("a", payload, true),
            ],
        )
    };
    // Utf8 is admitted by both Global and direct-Partial placement, so this
    // pair need not force a specific choice between those two valid shapes.
    let flat = compile(DataType::Int64);
    let nested = compile(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Int64,
        true,
    ))));
    assert!(
        ordered_filters(&flat) > 0,
        "the flat source must retain an ordered filter"
    );
    assert_eq!(ordered_filters(&nested), 0);
}

#[test]
fn ordered_filter_completed_plan_requires_an_installed_timestamp_key() {
    // Zero-call grouping is deduplication: PushDownTopNToPreAgg rejects it.
    // The ordinary Global ordered-filter route therefore remains eligible.
    let sql = "SELECT k FROM fixture.source GROUP BY k ORDER BY k LIMIT 1";
    let accepted = sql_source(sql, DataType::Timestamp(TimeUnit::Microsecond, None));
    let rejected = sql_source(
        sql,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    );
    assert!(
        ordered_filters(&accepted) > 0,
        "the timezone-free key must retain its filter"
    );
    assert_eq!(ordered_filters(&rejected), 0);
}

#[test]
fn membership_completed_plan_requires_a_canonical_key_schema() {
    // A one-row build makes the default broadcast/RF size gates meaningful on
    // the fixture's Missing-statistics source without changing session policy.
    let sql = "SELECT p.k FROM fixture.probe AS p \
               JOIN (SELECT k FROM fixture.build LIMIT 1) AS b ON p.k = b.k";
    let accepted = sql_source(sql, DataType::Int64);
    let rejected = sql_source(sql, DataType::Binary);
    assert!(
        membership_filters(&accepted) > 0,
        "the admitted key must retain membership RF"
    );
    assert_eq!(membership_filters(&rejected), 0);
}
