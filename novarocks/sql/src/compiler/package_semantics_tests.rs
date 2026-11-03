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

//! Production package semantics authored from real SQL statements.

use std::collections::BTreeMap;

use novarocks_physical_plan::{
    FragmentPackageAdmission, NodeKind, PhysicalCallSite, PlanLimits,
    PropertyProofProjectionLimits, extract_fragment_packages,
};

use super::SqlCompileIntent;
use super::tests::complete_with_exact_statistics;
use crate::compiler::{
    PackageSemanticsError, SqlAuthoredPhysicalPlan, SqlCompileControl,
    author_fragment_package_semantics,
};

// A table-free statement completes on its first step: it has no catalog,
// statistics or provider observation need.
fn table_free_owner(sql: &str) -> SqlAuthoredPhysicalPlan {
    crate::compiler::SqlCompiler::start(
        super::tests::request(sql, SqlCompileIntent::Query)
            .try_into_completion()
            .expect("completion seed"),
        &SqlCompileControl::unbounded(),
    )
    .expect("table-free statement")
    .into_complete()
    .expect("no observation need")
    .into_parts()
    .0
}

fn owner(sql: &str) -> SqlAuthoredPhysicalPlan {
    complete_with_exact_statistics(sql, SqlCompileIntent::Query, 13)
        .into_parts()
        .0
}

#[test]
fn aggregate_scan_statement_authors_every_fragment_and_covers_each_aggregate_call() {
    let owner = owner("SELECT MIN(order_key) AS lo FROM orders");
    let semantics = author_fragment_package_semantics(
        &owner,
        crate::constant::test_constant_policy(),
        &SqlCompileControl::unbounded(),
    )
    .expect("package semantics");
    assert_eq!(
        semantics.keys().collect::<Vec<_>>(),
        owner.plan().fragments().keys().collect::<Vec<_>>()
    );
    let mut aggregate_calls = 0;
    for (id, fragment) in owner.plan().fragments() {
        let authored = &semantics[id];
        assert_eq!(authored.pruning.fragment(), *id);
        for node in fragment.nodes().values() {
            if let NodeKind::Aggregate { calls, .. } = &node.kind {
                for ordinal in 0..calls.len() {
                    let site = PhysicalCallSite::Aggregate {
                        node: node.id,
                        call: ordinal as u32,
                    };
                    assert!(
                        authored.calls.entries().contains_key(&site),
                        "aggregate call {site:?} is frozen"
                    );
                    aggregate_calls += 1;
                }
            }
        }
    }
    assert!(aggregate_calls >= 1);
}

#[test]
fn statement_policy_must_equal_every_captured_call_policy() {
    let owner = owner("SELECT MIN(order_key) AS lo FROM orders");
    let mut other = crate::constant::test_constant_policy();
    other.max_rows -= 1;
    let error = author_fragment_package_semantics(&owner, other, &SqlCompileControl::unbounded())
        .expect_err("a different statement policy is refused");
    assert!(
        matches!(error, PackageSemanticsError::PolicyMismatch(_)),
        "{error}"
    );
}

// A provider-free statement publishes complete checked packages through the
// original extraction law with the authored uses, calls and pruning.
#[test]
fn provider_free_statement_publishes_checked_packages_through_extraction() {
    let owner = table_free_owner("SELECT 1 + 2 AS x, abs(-5) AS y");
    let control = SqlCompileControl::unbounded();
    let semantics = author_fragment_package_semantics(
        &owner,
        crate::constant::test_constant_policy(),
        &control,
    )
    .expect("package semantics");
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (id, authored) in semantics {
        uses.insert(id, authored.expression_uses);
        calls.insert(id, authored.calls);
        pruning.insert(id, authored.pruning);
        admissions.insert(
            id,
            FragmentPackageAdmission {
                plan_limits: PlanLimits::FROZEN,
                source_retained_bytes: 2 << 30,
                property_projection_limits: PropertyProofProjectionLimits {
                    max_request_bytes: 512 << 20,
                    max_coexisting_bytes: 4 << 30,
                    max_projection_work: usize::MAX / 4,
                },
            },
        );
    }
    let packages = extract_fragment_packages(
        owner.plan(),
        &BTreeMap::new(),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &control,
    )
    .expect("checked packages");
    assert_eq!(packages.len(), owner.plan().fragments().len());
    for (id, package) in &packages {
        assert_eq!(package.fragment().id(), *id);
    }
}

#[test]
fn json_in_subquery_authors_a_null_aware_hash_mark_with_canonical_keys() {
    use novarocks_physical_plan::{JoinDistribution, JoinKind, ValueOrigin};
    let owner = table_free_owner(
        "SELECT JSON '{\"a\":1,\"b\":2}' IN (SELECT JSON '{\"b\":2,\"a\":1}') AS present",
    );
    let mut count = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::HashJoin {
                kind: JoinKind::LeftMark,
                distribution,
                condition,
                ..
            } = &node.kind
            {
                count += 1;
                assert!(matches!(
                    distribution,
                    JoinDistribution::Singleton | JoinDistribution::BroadcastBuild
                ));
                let mark = node.output.columns.iter().filter_map(|id| fragment.values().get(id)).find(|value| matches!(value.origin, ValueOrigin::JoinMark { node: id } if id == node.id)).expect("authored mark output");
                assert!(mark.ty.nullable());
                assert_eq!(mark.ty.data_type(), &arrow::datatypes::DataType::Boolean);
                let novarocks_physical_plan::HashJoinCondition::NullAwareEqual {
                    value_equal_keys,
                    scope_keys,
                    eligibility,
                } = condition
                else {
                    panic!("mark must preserve null-aware equality")
                };
                assert_eq!(value_equal_keys.len(), 1);
                assert!(scope_keys.is_empty() && eligibility.is_none());
                assert!(!value_equal_keys[0].null_safe);
                for key in [value_equal_keys[0].left, value_equal_keys[0].right] {
                    let ty = &fragment.expressions().get(key).unwrap().ty;
                    assert_eq!(ty.data_type(), &arrow::datatypes::DataType::Utf8);
                    assert_eq!(
                        ty.logical_type(),
                        novarocks_type_contract::ValueLogicalType::Physical
                    );
                }
            }
        }
    }
    assert_eq!(count, 1);
    let control = SqlCompileControl::unbounded();
    let contract = crate::explain::completed::render_completed_plan(
        owner.plan(),
        &[],
        crate::explain::ExplainLevel::Contract,
        None,
        crate::explain::completed::ExplainRenderBudget::default(),
        &control,
    )
    .expect("completed mark contract EXPLAIN");
    let tree = crate::explain::completed_tree::render_completed_plan_tree(
        owner.plan(),
        crate::explain::ExplainLevel::Normal,
        &control,
    )
    .expect("completed mark tree EXPLAIN");
    for lines in [contract, tree] {
        assert!(
            lines
                .iter()
                .any(|line| line.contains("NULL AWARE LEFT MARK"))
        );
    }
    author_fragment_package_semantics(
        &owner,
        crate::constant::test_constant_policy(),
        &SqlCompileControl::unbounded(),
    )
    .expect("JSON normalization calls and hash key roots are frozen");
}
