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

use super::*;
use crate::iceberg::spec::{
    FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, SnapshotReference,
    SortOrder, Summary, TableMetadataBuilder, Type,
};
use std::collections::HashMap;
use std::sync::Arc;

fn graph(
    properties: HashMap<String, String>,
    refs: Vec<(&str, i64, SnapshotRetention)>,
) -> TableMetadata {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let mut builder = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://retention/table".into(),
        FormatVersion::V2,
        properties,
    )
    .unwrap();
    let mut historical = serde_json::to_value(builder.build().unwrap().metadata).unwrap();
    historical["last-updated-ms"] = serde_json::json!(0);
    let base: TableMetadata = serde_json::from_value(historical).unwrap();
    builder = base.into_builder(None);
    for (ordinal, (id, parent, time)) in [
        (1, None, 1000),
        (2, Some(1), 2000),
        (3, Some(2), 3000),
        (4, Some(1), 4000),
        (5, None, 9500),
        (6, None, 1500),
    ]
    .into_iter()
    .enumerate()
    {
        builder = builder
            .add_snapshot(
                Snapshot::builder()
                    .with_snapshot_id(id)
                    .with_parent_snapshot_id(parent)
                    .with_sequence_number(ordinal as i64 + 1)
                    .with_timestamp_ms(time)
                    .with_manifest_list(format!("memory://retention/{id}.avro"))
                    .with_schema_id(0)
                    .with_summary(Summary {
                        operation: Operation::Append,
                        additional_properties: HashMap::new(),
                    })
                    .build(),
            )
            .unwrap();
    }
    for (name, id, retention) in refs {
        builder = builder
            .set_ref(name, SnapshotReference::new(id, retention))
            .unwrap();
    }
    builder.build().unwrap().metadata
}
fn branch() -> SnapshotRetention {
    SnapshotRetention::branch(None, None, None)
}
fn removed(plan: &RetentionPlan) -> Vec<i64> {
    plan.removed_snapshots
        .iter()
        .map(Snapshot::snapshot_id)
        .collect()
}

#[test]
fn explicit_cutoff_retains_the_full_main_chain_when_count_covers_it() {
    let metadata = graph(HashMap::new(), vec![("main", 3, branch())]);
    let plan = RetentionPlan::compute(&metadata, 10000, Some(i64::MAX), Some(20)).unwrap();
    assert_eq!(removed(&plan), vec![4, 5, 6]);
    assert_eq!(
        plan.protected_snapshots,
        std::collections::BTreeSet::from([1, 2, 3])
    );
}

#[test]
fn no_refs_under_an_explicit_cutoff_has_no_implicit_main_protection() {
    let metadata = graph(HashMap::new(), vec![]);
    let plan = RetentionPlan::compute(&metadata, 10000, Some(i64::MAX), Some(20)).unwrap();
    assert_eq!(removed(&plan), vec![1, 2, 3, 4, 5, 6]);
    assert!(plan.protected_snapshots.is_empty());
    assert!(plan.requirements.is_empty());
}

#[test]
fn default_age_keeps_recent_history_instead_of_only_the_head() {
    let metadata = graph(HashMap::new(), vec![("main", 3, branch())]);
    let plan = RetentionPlan::compute(&metadata, 10000, None, None).unwrap();
    assert!(plan.is_empty());
    assert_eq!(plan.protected_snapshots.len(), 6);
}
#[test]
fn each_branch_override_and_young_unreferenced_snapshot_protect_independently() {
    let metadata = graph(
        HashMap::new(),
        vec![
            ("main", 3, branch()),
            ("dev", 4, SnapshotRetention::branch(Some(2), Some(1), None)),
        ],
    );
    let plan = RetentionPlan::compute(&metadata, 10000, Some(9000), Some(1)).unwrap();
    assert_eq!(removed(&plan), vec![2, 6]);
    assert!(plan.protected_snapshots.contains(&1));
    assert!(plan.protected_snapshots.contains(&5));
    assert_eq!(plan.requirements.len(), 2);
}
#[test]
fn main_never_ages_out_and_expired_tag_ref_only_is_still_a_change() {
    let metadata = graph(
        HashMap::new(),
        vec![
            ("main", 3, SnapshotRetention::branch(None, None, Some(1))),
            (
                "old_tag",
                5,
                SnapshotRetention::Tag {
                    max_ref_age_ms: Some(1),
                },
            ),
        ],
    );
    let plan = RetentionPlan::compute(&metadata, 10000, Some(0), Some(1)).unwrap();
    assert_eq!(plan.removed_refs, BTreeSet::from(["old_tag".to_owned()]));
    assert!(plan.removed_snapshots.is_empty());
    assert!(!plan.is_empty());
    assert!(
        matches!(&plan.updates()[0], TableUpdate::RemoveSnapshotRef { ref_name } if ref_name == "old_tag")
    );
    assert_eq!(plan.requirements.len(), 2);
}
#[test]
fn table_defaults_and_call_overrides_are_evaluated_at_the_fixed_time() {
    let metadata = graph(
        HashMap::from([
            ("history.expire.max-snapshot-age-ms".into(), "2000".into()),
            ("history.expire.min-snapshots-to-keep".into(), "2".into()),
        ]),
        vec![("main", 3, branch())],
    );
    let plan = RetentionPlan::compute(&metadata, 10000, None, None).unwrap();
    assert_eq!(removed(&plan), vec![1, 4, 6]);
    let overridden = RetentionPlan::compute(&metadata, 10000, Some(9000), Some(3)).unwrap();
    assert_eq!(removed(&overridden), vec![4, 6]);
}
#[test]
fn disabled_gc_and_invalid_policy_are_rejected_without_any_io() {
    let metadata = graph(
        HashMap::from([("gc.enabled".into(), "false".into())]),
        vec![("main", 3, branch())],
    );
    assert_eq!(
        RetentionPlan::compute(&metadata, 10000, None, None)
            .unwrap_err()
            .kind(),
        ErrorKind::FeatureUnsupported
    );
    let metadata = graph(HashMap::new(), vec![("main", 3, branch())]);
    assert!(RetentionPlan::compute(&metadata, 10000, None, Some(0)).is_err());
    assert!(RetentionPlan::compute(&metadata, 10000, Some(-1), Some(1)).is_err());
    let metadata = graph(
        HashMap::new(),
        vec![("main", 3, SnapshotRetention::branch(Some(0), None, None))],
    );
    assert!(RetentionPlan::compute(&metadata, 10000, None, None).is_err());
}
#[test]
fn dev_min_three_protects_head_and_two_ancestors_despite_call_keep_one() {
    let metadata = graph(
        HashMap::new(),
        vec![
            ("main", 4, branch()),
            ("dev", 3, SnapshotRetention::branch(Some(3), Some(1), None)),
        ],
    );
    let result = RetentionPlan::compute(&metadata, 10000, Some(9000), Some(1)).unwrap();
    assert_eq!(removed(&result), vec![6]);
    assert_eq!(result.protected_snapshots, BTreeSet::from([1, 2, 3, 4, 5]));
}
#[test]
fn branch_age_override_protects_ancestors_after_its_minimum_count_is_met() {
    let metadata = graph(
        HashMap::new(),
        vec![
            ("main", 4, branch()),
            (
                "dev",
                3,
                SnapshotRetention::branch(Some(1), Some(8500), None),
            ),
        ],
    );
    let result = RetentionPlan::compute(&metadata, 10000, Some(9000), Some(1)).unwrap();
    assert_eq!(removed(&result), vec![1, 6]);
    assert_eq!(result.protected_snapshots, BTreeSet::from([2, 3, 4, 5]));
}

#[test]
fn execute_keeps_branch_ancestry_out_of_global_fallback_after_prefix_stops() {
    let metadata = graph(
        HashMap::new(),
        vec![
            (
                "main",
                3,
                SnapshotRetention::branch(Some(1), Some(7500), Some(1)),
            ),
            (
                "stable_tag",
                4,
                SnapshotRetention::Tag {
                    max_ref_age_ms: Some(9000),
                },
            ),
            (
                "expired_tag",
                6,
                SnapshotRetention::Tag {
                    max_ref_age_ms: Some(3),
                },
            ),
        ],
    );
    let plan = RetentionPlan::compute_execute(&metadata, 10000, 500, 1).unwrap();
    assert_eq!(removed(&plan), vec![1, 2]);
    assert_eq!(
        plan.removed_refs,
        BTreeSet::from(["expired_tag".to_owned()])
    );
    assert_eq!(plan.protected_snapshots, BTreeSet::from([3, 4, 5, 6]));
}

#[test]
fn execute_branch_count_overrides_frozen_default() {
    let metadata = graph(
        HashMap::new(),
        vec![
            ("main", 4, branch()),
            ("dev", 3, SnapshotRetention::branch(Some(3), Some(1), None)),
        ],
    );
    let plan = RetentionPlan::compute_execute(&metadata, 10000, 9000, 1).unwrap();
    assert_eq!(removed(&plan), vec![6]);
    assert_eq!(plan.protected_snapshots, BTreeSet::from([1, 2, 3, 4, 5]));
}

#[test]
fn execute_stops_at_first_expirable_ancestor_even_if_older_ancestor_timestamp_is_newer() {
    let base = graph(
        HashMap::new(),
        vec![(
            "main",
            3,
            SnapshotRetention::branch(Some(1), Some(7500), None),
        )],
    );
    // Across external writers, ancestry sequence can remain valid even when
    // clocks go backward between successive snapshots.
    let mut raw = serde_json::to_value(base).unwrap();
    let ancestor = raw["snapshots"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|item| item["snapshot-id"] == 1)
        .unwrap();
    ancestor["timestamp-ms"] = 2900.into();
    let metadata = serde_json::from_value(raw).unwrap();
    let explicit = RetentionPlan::compute_execute(&metadata, 10_000, 500, 1).unwrap();
    assert_eq!(removed(&explicit), vec![1, 2]);
    let legacy = RetentionPlan::compute(&metadata, 10_000, Some(500), Some(1)).unwrap();
    assert_eq!(removed(&legacy), vec![2]);
}
