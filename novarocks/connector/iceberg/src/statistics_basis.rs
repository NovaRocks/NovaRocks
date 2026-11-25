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

//! Derives how a statistic's basis row set relates to the row set of the
//! queried snapshot.
//!
//! This answers only "what happened to the rows between these two snapshots",
//! never "is the number accurate" — those are separate axes of
//! `StatisticsEvidence` and must not encode each other.
//!
//! The walk is deliberately one-directional: it starts at the queried snapshot
//! and follows `parent_snapshot_id` backwards looking for the basis. A basis
//! that is not an ancestor of the queried snapshot is not comparable, and
//! neither is a lineage this cannot fully classify. Guessing would let a stale
//! NDV be presented as describing rows it never saw.

use novarocks_spi::connector::StatisticsBasisRelation;

use crate::iceberg::spec::TableMetadata;
use crate::snapshot_history::{SnapshotDataEffect, SnapshotHistory};

/// Upper bound on how far back the walk looks before giving up. A basis older
/// than this is reported as incomparable rather than costing an unbounded
/// metadata traversal on a table with a long snapshot history.
const MAX_BASIS_LINEAGE_STEPS: usize = 1024;

/// Classifies `basis_snapshot` against `queried_snapshot`.
///
/// Returns `Incomparable` whenever the relationship cannot be proven: the basis
/// is not an ancestor, the lineage is longer than `MAX_BASIS_LINEAGE_STEPS`, a
/// snapshot on the path is absent from `metadata`, or the path mixes operations
/// whose combined effect on the row set is not one-directional.
pub fn basis_relation(
    metadata: &TableMetadata,
    basis_snapshot: i64,
    queried_snapshot: i64,
) -> StatisticsBasisRelation {
    if basis_snapshot == queried_snapshot {
        return StatisticsBasisRelation::Identical;
    }

    let mut only_added_rows = false;
    let mut only_removed_rows = false;
    let mut history = SnapshotHistory::new(metadata, queried_snapshot);

    for _ in 0..MAX_BASIS_LINEAGE_STEPS {
        if history.next_id() == Some(basis_snapshot) {
            return match (only_added_rows, only_removed_rows) {
                // Only rewrites/compactions: the logical row set is unchanged.
                (false, false) => StatisticsBasisRelation::Identical,
                (true, false) => StatisticsBasisRelation::BasisIsSubset,
                (false, true) => StatisticsBasisRelation::BasisIsSuperset,
                // Rows were both added and removed; neither containment holds.
                (true, true) => StatisticsBasisRelation::Incomparable,
            };
        }
        let Some(step) = history.next() else {
            return StatisticsBasisRelation::Incomparable;
        };
        match step.data_effect {
            SnapshotDataEffect::Adds => only_added_rows = true,
            SnapshotDataEffect::Removes => only_removed_rows = true,
            SnapshotDataEffect::Preserves => {}
            SnapshotDataEffect::MayAddAndRemove => {
                return StatisticsBasisRelation::Incomparable;
            }
        }
    }

    StatisticsBasisRelation::Incomparable
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SortOrder, Summary, TableMetadataBuilder, Type,
    };

    use super::*;

    fn snapshot(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        operation: Operation,
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(1_700_000_000_000 + snapshot_id)
            .with_manifest_list(format!("file:///tmp/manifest-list-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build()
    }

    fn metadata_with_snapshots(snapshots: Vec<Snapshot>) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let mut builder = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "/tmp/statistics-basis-test".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder");
        for snapshot in snapshots {
            builder = builder.add_snapshot(snapshot).expect("add snapshot");
        }
        builder.build().expect("metadata").metadata
    }

    /// Builds a linear chain 1..=n where snapshot `i + 1` carries `ops[i]`.
    fn linear_chain(ops: &[Operation]) -> TableMetadata {
        let mut snapshots = vec![snapshot(1, None, Operation::Append)];
        for (index, operation) in ops.iter().enumerate() {
            let id = index as i64 + 2;
            snapshots.push(snapshot(id, Some(id - 1), operation.clone()));
        }
        metadata_with_snapshots(snapshots)
    }

    #[test]
    fn the_same_snapshot_is_identical() {
        let metadata = linear_chain(&[]);
        assert_eq!(
            basis_relation(&metadata, 1, 1),
            StatisticsBasisRelation::Identical
        );
    }

    #[test]
    fn a_path_of_only_rewrites_leaves_the_row_set_identical() {
        let metadata = linear_chain(&[Operation::Replace, Operation::Replace]);
        assert_eq!(
            basis_relation(&metadata, 1, 3),
            StatisticsBasisRelation::Identical
        );
    }

    #[test]
    fn a_path_of_only_appends_makes_the_basis_a_subset() {
        let metadata = linear_chain(&[Operation::Append, Operation::Append]);
        assert_eq!(
            basis_relation(&metadata, 1, 3),
            StatisticsBasisRelation::BasisIsSubset
        );
    }

    #[test]
    fn a_path_of_only_deletes_makes_the_basis_a_superset() {
        let metadata = linear_chain(&[Operation::Delete, Operation::Delete]);
        assert_eq!(
            basis_relation(&metadata, 1, 3),
            StatisticsBasisRelation::BasisIsSuperset
        );
    }

    #[test]
    fn rewrites_do_not_disturb_a_one_directional_path() {
        let metadata = linear_chain(&[Operation::Append, Operation::Replace, Operation::Append]);
        assert_eq!(
            basis_relation(&metadata, 1, 4),
            StatisticsBasisRelation::BasisIsSubset
        );
    }

    #[test]
    fn mixing_appends_and_deletes_is_not_provable_in_either_direction() {
        let metadata = linear_chain(&[Operation::Append, Operation::Delete]);
        assert_eq!(
            basis_relation(&metadata, 1, 3),
            StatisticsBasisRelation::Incomparable
        );
    }

    #[test]
    fn an_overwrite_on_the_path_is_not_provable() {
        let metadata = linear_chain(&[Operation::Overwrite]);
        assert_eq!(
            basis_relation(&metadata, 1, 2),
            StatisticsBasisRelation::Incomparable
        );
    }

    #[test]
    fn a_basis_that_is_not_an_ancestor_is_not_comparable() {
        // Two siblings both parented on snapshot 1; neither can reach the other.
        let metadata = metadata_with_snapshots(vec![
            snapshot(1, None, Operation::Append),
            snapshot(2, Some(1), Operation::Append),
            snapshot(3, Some(1), Operation::Append),
        ]);
        assert_eq!(
            basis_relation(&metadata, 2, 3),
            StatisticsBasisRelation::Incomparable
        );
        // A descendant is not a valid basis for its own ancestor either.
        assert_eq!(
            basis_relation(&metadata, 3, 1),
            StatisticsBasisRelation::Incomparable
        );
    }

    #[test]
    fn a_snapshot_missing_from_metadata_is_not_comparable() {
        let metadata = metadata_with_snapshots(vec![snapshot(1, None, Operation::Append)]);
        assert_eq!(
            basis_relation(&metadata, 7, 1),
            StatisticsBasisRelation::Incomparable
        );
    }

    #[test]
    fn an_expired_basis_body_can_still_be_reached_by_parent_id() {
        let metadata = linear_chain(&[Operation::Replace, Operation::Replace])
            .into_builder(None)
            .remove_snapshots(&[1])
            .build()
            .unwrap()
            .metadata;
        assert!(metadata.snapshot_by_id(1).is_none());
        assert_eq!(
            basis_relation(&metadata, 1, 3),
            StatisticsBasisRelation::Identical
        );
        assert_eq!(
            basis_relation(&metadata, 0, 3),
            StatisticsBasisRelation::Incomparable
        );
    }

    #[test]
    fn the_basis_identity_must_be_seen_before_the_last_step_is_consumed() {
        let metadata = linear_chain(&vec![Operation::Replace; MAX_BASIS_LINEAGE_STEPS]);
        let head = MAX_BASIS_LINEAGE_STEPS as i64 + 1;
        assert_eq!(
            basis_relation(&metadata, 2, head),
            StatisticsBasisRelation::Identical
        );
        assert_eq!(
            basis_relation(&metadata, 1, head),
            StatisticsBasisRelation::Incomparable
        );
    }
}
