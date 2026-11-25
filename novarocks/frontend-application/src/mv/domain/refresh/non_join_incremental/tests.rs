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
use novarocks_spi::connector::{
    ConnectorChangeWindowNetZeroBasis, ConnectorChangeWindowPartitionImpact,
    ConnectorChangeWindowVolume,
};

const NET_ZERO: ConnectorChangeWindowFacts =
    ConnectorChangeWindowFacts::NetZero(ConnectorChangeWindowNetZeroBasis::EqualEndpoints);

fn fact(
    base_fqn: &str,
    current_snapshot_id: i64,
    window_facts: ConnectorChangeWindowFacts,
) -> PlannedFact {
    PlannedFact {
        occurrence_id: SqlMvRelationOccurrenceId::new(current_snapshot_id as u32),
        base_fqn: base_fqn.to_string(),
        current_snapshot_id,
        current_table_object_id: ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(
            format!("object-{base_fqn}").as_bytes(),
        ))
        .expect("valid test object ID"),
        window_facts,
    }
}

fn difference(negative_weight: ConnectorChangeWindowNegativeWeight) -> ConnectorChangeWindowFacts {
    ConnectorChangeWindowFacts::Difference {
        negative_weight,
        partition_impact: ConnectorChangeWindowPartitionImpact::Unavailable,
        volume: ConnectorChangeWindowVolume {
            source_event_rows_upper_bound: 3,
            source_data_file_bytes: 128,
        },
    }
}

#[test]
fn empty_changes_are_rejected() {
    let err = reduce_non_join_incremental_facts(Vec::new()).expect_err("empty changes");
    assert!(err.contains("at least one base change"), "{err}");
}

#[test]
fn repeated_table_occurrences_remain_distinct() {
    let plan = reduce_non_join_incremental_facts(vec![
        fact("c.db.t", 2, NET_ZERO),
        fact("c.db.t", 3, NET_ZERO),
    ])
    .expect("separate relation occurrences may name the same table");
    let NonJoinIncrementalChangePlan::MetadataOnly(lineage) = plan else {
        panic!("expected metadata-only plan");
    };
    assert_eq!(lineage.snapshots.len(), 2);
    assert_eq!(lineage.snapshots[&SqlMvRelationOccurrenceId::new(2)], 2);
    assert_eq!(lineage.snapshots[&SqlMvRelationOccurrenceId::new(3)], 3);
}

#[test]
fn duplicate_relation_occurrence_is_rejected() {
    let mut duplicate = fact("c.db.t", 3, NET_ZERO);
    duplicate.occurrence_id = SqlMvRelationOccurrenceId::new(2);
    let err = reduce_non_join_incremental_facts(vec![fact("c.db.t", 2, NET_ZERO), duplicate])
        .expect_err("duplicate occurrence");
    assert!(err.contains("duplicate relation occurrence 2"), "{err}");
}

#[test]
fn all_empty_batches_are_metadata_only() {
    let plan = reduce_non_join_incremental_facts(vec![fact("c.db.t", 2, NET_ZERO)])
        .expect("metadata only");
    assert!(matches!(
        plan,
        NonJoinIncrementalChangePlan::MetadataOnly(_)
    ));
}

#[test]
fn insert_only_batch_is_delete_free_change_stream() {
    let plan = reduce_non_join_incremental_facts(vec![fact(
        "c.db.t",
        2,
        difference(ConnectorChangeWindowNegativeWeight::Absent),
    )])
    .expect("change stream");
    assert!(matches!(
        plan,
        NonJoinIncrementalChangePlan::ChangeStream {
            negative_weight: ConnectorChangeWindowNegativeWeight::Absent,
            ..
        }
    ));
}

#[test]
fn delete_admission_marks_change_stream_as_delete_capable() {
    let plan = reduce_non_join_incremental_facts(vec![fact(
        "c.db.t",
        2,
        difference(ConnectorChangeWindowNegativeWeight::Possible),
    )])
    .expect("delete change stream");
    assert!(matches!(
        plan,
        NonJoinIncrementalChangePlan::ChangeStream {
            negative_weight: ConnectorChangeWindowNegativeWeight::Possible,
            ..
        }
    ));
}

#[test]
fn lineage_and_unsafe_replace_request_typed_full_rebuild() {
    for reason in [
        ConnectorChangeWindowNotIncrementalReason::SnapshotAncestryBroken {
            from_snapshot_id: 1,
        },
        ConnectorChangeWindowNotIncrementalReason::SchemaChanged { snapshot_id: 2 },
    ] {
        let plan = reduce_non_join_incremental_facts(vec![fact(
            "c.db.t",
            2,
            ConnectorChangeWindowFacts::NotIncremental(reason),
        )])
        .expect("typed full rebuild");
        let NonJoinIncrementalChangePlan::FullRebuild { reasons, .. } = plan else {
            panic!("expected full rebuild");
        };
        assert_eq!(reasons.len(), 1);
        assert_eq!(reasons[0].reason, reason);
    }
}

#[test]
fn multiple_full_rebuild_reasons_are_stable_across_input_order() {
    let lineage_broken = fact(
        "z.db.lineage",
        2,
        ConnectorChangeWindowFacts::NotIncremental(
            ConnectorChangeWindowNotIncrementalReason::SnapshotAncestryBroken {
                from_snapshot_id: 1,
            },
        ),
    );
    let schema_changed = fact(
        "a.db.schema",
        3,
        ConnectorChangeWindowFacts::NotIncremental(
            ConnectorChangeWindowNotIncrementalReason::SchemaChanged { snapshot_id: 3 },
        ),
    );

    let forward =
        reduce_non_join_incremental_facts(vec![lineage_broken.clone(), schema_changed.clone()])
            .expect("forward full rebuild");
    let reverse = reduce_non_join_incremental_facts(vec![schema_changed, lineage_broken])
        .expect("reverse full rebuild");

    assert_eq!(forward, reverse);
    let NonJoinIncrementalChangePlan::FullRebuild { reasons, .. } = forward else {
        panic!("expected full rebuild");
    };
    assert_eq!(
        reasons
            .iter()
            .map(NonJoinRebuildReason::log_message)
            .collect::<Vec<_>>(),
        [
            "occurrence 2 (z.db.lineage): previous snapshot 1 is not reachable",
            "occurrence 3 (a.db.schema): snapshot 3 changes the schema beyond field renames",
        ]
    );
}

#[test]
fn one_non_incremental_occurrence_takes_precedence_and_names_its_schema_change() {
    let plan = reduce_non_join_incremental_facts(vec![
        fact(
            "c.db.t",
            2,
            difference(ConnectorChangeWindowNegativeWeight::Absent),
        ),
        fact(
            "c.db.schema",
            3,
            ConnectorChangeWindowFacts::NotIncremental(
                ConnectorChangeWindowNotIncrementalReason::SchemaChanged { snapshot_id: 3 },
            ),
        ),
        fact("c.db.t", 4, NET_ZERO),
    ])
    .expect("schema change requires full rebuild");
    let NonJoinIncrementalChangePlan::FullRebuild { lineage, reasons } = plan else {
        panic!("expected full rebuild");
    };
    assert_eq!(lineage.snapshots.len(), 3);
    let [reason] = reasons.as_slice() else {
        panic!("expected a single reason");
    };
    assert_eq!(
        reason.log_message(),
        "occurrence 3 (c.db.schema): snapshot 3 changes the schema beyond field renames"
    );
}

#[test]
fn possible_negative_weight_from_one_occurrence_governs_the_complete_change_stream() {
    use ConnectorChangeWindowNegativeWeight::{Absent, Possible};
    let plan = reduce_non_join_incremental_facts(vec![
        fact("c.db.changed", 2, difference(Possible)),
        fact("c.db.unchanged", 3, NET_ZERO),
        fact("c.db.append", 4, difference(Absent)),
    ])
    .expect("multi-source change stream");
    assert!(matches!(
        plan,
        NonJoinIncrementalChangePlan::ChangeStream {
            negative_weight: Possible,
            ..
        }
    ));
}

#[test]
fn lineage_maps_are_complete_and_sorted_by_occurrence() {
    let plan = reduce_non_join_incremental_facts(vec![
        fact("z.db.t", 3, NET_ZERO),
        fact("a.db.t", 2, NET_ZERO),
    ])
    .expect("metadata only");
    let NonJoinIncrementalChangePlan::MetadataOnly(lineage) = plan else {
        panic!("expected metadata only");
    };
    assert_eq!(
        lineage.snapshots.keys().cloned().collect::<Vec<_>>(),
        [
            SqlMvRelationOccurrenceId::new(2),
            SqlMvRelationOccurrenceId::new(3)
        ]
    );
    assert_eq!(lineage.snapshots[&SqlMvRelationOccurrenceId::new(2)], 2);
    assert_eq!(
        lineage.table_object_ids[&SqlMvRelationOccurrenceId::new(3)]
            .as_bytes()
            .as_ref(),
        b"object-z.db.t"
    );
}
