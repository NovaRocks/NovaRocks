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

use std::collections::{BTreeMap, BTreeSet};

use crate::connector::iceberg::changes::{
    ChangeError, IcebergChangePolicySignal, plan_changes, policy_signal_from_change_error,
};
use novarocks_catalog::identifier::TableIdentity;

pub(crate) struct NonJoinBaseChange<'a> {
    pub base_ref: &'a TableIdentity,
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub base_table: &'a iceberg::table::Table,
    pub current_table_uuid: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum NonJoinIncrementalChangePlan {
    MetadataOnly(NonJoinLineage),
    FullRebuild {
        lineage: NonJoinLineage,
        reason: String,
    },
    ChangeStream {
        lineage: NonJoinLineage,
        has_delete_changes: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NonJoinLineage {
    pub snapshots: BTreeMap<String, i64>,
    pub table_uuids: BTreeMap<String, String>,
}

#[derive(Clone)]
struct PlannedFact {
    base_fqn: String,
    current_snapshot_id: i64,
    current_table_uuid: String,
    result: Result<BatchFact, ChangeError>,
}

#[derive(Clone, Copy)]
struct BatchFact {
    current_snapshot_id: i64,
    has_inserts: bool,
    has_position_deletes: bool,
    has_equality_deletes: bool,
    has_deleted_data_files: bool,
}

pub(crate) fn plan_non_join_incremental_changes(
    changes: &[NonJoinBaseChange<'_>],
) -> Result<NonJoinIncrementalChangePlan, String> {
    if changes.is_empty() {
        return Err("iceberg MV incremental refresh requires at least one base change".to_string());
    }

    let mut seen = BTreeSet::new();
    for change in changes {
        let base_fqn = change.base_ref.fqn();
        if !seen.insert(base_fqn.clone()) {
            return Err(format!(
                "iceberg MV incremental refresh has duplicate base {base_fqn}"
            ));
        }
    }

    let facts = changes
        .iter()
        .map(|change| {
            let result = plan_changes(
                change.base_table,
                change.previous_snapshot_id,
                Some(change.current_snapshot_id),
                &[],
            )
            .map(|batch| BatchFact {
                current_snapshot_id: batch.current_snapshot_id,
                has_inserts: !batch.inserts.is_empty(),
                has_position_deletes: !batch.deletes.is_empty(),
                has_equality_deletes: !batch.equality_deletes.is_empty(),
                has_deleted_data_files: !batch.deleted_data_files.is_empty(),
            });
            PlannedFact {
                base_fqn: change.base_ref.fqn(),
                current_snapshot_id: change.current_snapshot_id,
                current_table_uuid: change.current_table_uuid.to_string(),
                result,
            }
        })
        .collect();
    reduce_non_join_incremental_facts(facts)
}

fn reduce_non_join_incremental_facts(
    facts: Vec<PlannedFact>,
) -> Result<NonJoinIncrementalChangePlan, String> {
    if facts.is_empty() {
        return Err("iceberg MV incremental refresh requires at least one base change".to_string());
    }

    let mut snapshots = BTreeMap::new();
    let mut table_uuids = BTreeMap::new();
    for fact in &facts {
        if snapshots
            .insert(fact.base_fqn.clone(), fact.current_snapshot_id)
            .is_some()
        {
            return Err(format!(
                "iceberg MV incremental refresh has duplicate base {}",
                fact.base_fqn
            ));
        }
        table_uuids.insert(fact.base_fqn.clone(), fact.current_table_uuid.clone());
    }
    let lineage = NonJoinLineage {
        snapshots,
        table_uuids,
    };

    let mut has_insert_changes = false;
    let mut has_delete_changes = false;
    let mut hard_errors = BTreeMap::new();
    let mut full_rebuild_reasons = BTreeMap::new();
    for fact in facts {
        let batch = match fact.result {
            Ok(batch) => batch,
            Err(err) => match policy_signal_from_change_error(&err) {
                IcebergChangePolicySignal::FullRefresh { reason } => {
                    full_rebuild_reasons.insert(fact.base_fqn, reason);
                    continue;
                }
                IcebergChangePolicySignal::Unsupported { reason } => {
                    hard_errors.insert(
                        fact.base_fqn,
                        format!("iceberg-stored materialized view refresh unsupported: {reason}"),
                    );
                    continue;
                }
                IcebergChangePolicySignal::Incremental => {
                    hard_errors.insert(
                        fact.base_fqn,
                        "iceberg-stored materialized view refresh produced invalid incremental policy from change planner"
                            .to_string(),
                    );
                    continue;
                }
            },
        };
        if batch.current_snapshot_id != fact.current_snapshot_id {
            hard_errors.insert(
                fact.base_fqn.clone(),
                format!(
                    "iceberg mv incremental refresh: change batch snapshot mismatch for {} (expected {}, got {})",
                    fact.base_fqn, fact.current_snapshot_id, batch.current_snapshot_id
                ),
            );
            continue;
        }
        has_insert_changes |= batch.has_inserts;
        has_delete_changes |= batch.has_position_deletes
            || batch.has_equality_deletes
            || batch.has_deleted_data_files;
    }

    if let Some((_, err)) = hard_errors.into_iter().next() {
        return Err(err);
    }
    if !full_rebuild_reasons.is_empty() {
        let reason = if full_rebuild_reasons.len() == 1 {
            full_rebuild_reasons
                .into_values()
                .next()
                .expect("one full-rebuild reason")
        } else {
            full_rebuild_reasons
                .into_iter()
                .map(|(base_fqn, reason)| format!("{base_fqn}: {reason}"))
                .collect::<Vec<_>>()
                .join("; ")
        };
        return Ok(NonJoinIncrementalChangePlan::FullRebuild { lineage, reason });
    }

    if !has_insert_changes && !has_delete_changes {
        Ok(NonJoinIncrementalChangePlan::MetadataOnly(lineage))
    } else {
        Ok(NonJoinIncrementalChangePlan::ChangeStream {
            lineage,
            has_delete_changes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(current_snapshot_id: i64) -> BatchFact {
        BatchFact {
            current_snapshot_id,
            has_inserts: false,
            has_position_deletes: false,
            has_equality_deletes: false,
            has_deleted_data_files: false,
        }
    }

    fn fact(
        base_fqn: &str,
        current_snapshot_id: i64,
        result: Result<BatchFact, ChangeError>,
    ) -> PlannedFact {
        PlannedFact {
            base_fqn: base_fqn.to_string(),
            current_snapshot_id,
            current_table_uuid: format!("uuid-{base_fqn}"),
            result,
        }
    }

    #[test]
    fn empty_changes_are_rejected() {
        let err = reduce_non_join_incremental_facts(Vec::new()).expect_err("empty changes");
        assert!(err.contains("at least one base change"), "{err}");
    }

    #[test]
    fn duplicate_base_fqn_is_rejected() {
        let err = reduce_non_join_incremental_facts(vec![
            fact("c.db.t", 2, Ok(batch(2))),
            fact("c.db.t", 3, Ok(batch(3))),
        ])
        .expect_err("duplicate base");
        assert!(err.contains("duplicate base c.db.t"), "{err}");
    }

    #[test]
    fn exact_endpoint_mismatch_is_rejected() {
        let err = reduce_non_join_incremental_facts(vec![fact("c.db.t", 3, Ok(batch(2)))])
            .expect_err("endpoint mismatch");
        assert!(err.contains("expected 3, got 2"), "{err}");
    }

    #[test]
    fn all_empty_batches_are_metadata_only() {
        let plan = reduce_non_join_incremental_facts(vec![fact("c.db.t", 2, Ok(batch(2)))])
            .expect("metadata only");
        assert!(matches!(
            plan,
            NonJoinIncrementalChangePlan::MetadataOnly(_)
        ));
    }

    #[test]
    fn insert_only_batch_is_delete_free_change_stream() {
        let mut planned = batch(2);
        planned.has_inserts = true;
        let plan = reduce_non_join_incremental_facts(vec![fact("c.db.t", 2, Ok(planned))])
            .expect("change stream");
        assert!(matches!(
            plan,
            NonJoinIncrementalChangePlan::ChangeStream {
                has_delete_changes: false,
                ..
            }
        ));
    }

    #[test]
    fn every_delete_category_marks_change_stream_as_delete_capable() {
        for mutate in [
            |fact: &mut BatchFact| fact.has_position_deletes = true,
            |fact: &mut BatchFact| fact.has_equality_deletes = true,
            |fact: &mut BatchFact| fact.has_deleted_data_files = true,
        ] {
            let mut planned = batch(2);
            mutate(&mut planned);
            let plan = reduce_non_join_incremental_facts(vec![fact("c.db.t", 2, Ok(planned))])
                .expect("delete change stream");
            assert!(matches!(
                plan,
                NonJoinIncrementalChangePlan::ChangeStream {
                    has_delete_changes: true,
                    ..
                }
            ));
        }
    }

    #[test]
    fn lineage_and_unsafe_replace_request_typed_full_rebuild() {
        for error in [
            ChangeError::LineageBroken {
                previous_snapshot: 1,
            },
            ChangeError::ReplaceValidationFailed {
                snapshot_id: 2,
                reason: "records changed".to_string(),
            },
        ] {
            let plan = reduce_non_join_incremental_facts(vec![fact("c.db.t", 2, Err(error))])
                .expect("typed full rebuild");
            assert!(matches!(
                plan,
                NonJoinIncrementalChangePlan::FullRebuild { .. }
            ));
        }
    }

    #[test]
    fn multiple_full_rebuild_reasons_are_stable_across_input_order() {
        let lineage_broken = fact(
            "z.db.lineage",
            2,
            Err(ChangeError::LineageBroken {
                previous_snapshot: 1,
            }),
        );
        let unsafe_replace = fact(
            "a.db.replace",
            3,
            Err(ChangeError::ReplaceValidationFailed {
                snapshot_id: 3,
                reason: "records changed".to_string(),
            }),
        );

        let forward =
            reduce_non_join_incremental_facts(vec![lineage_broken.clone(), unsafe_replace.clone()])
                .expect("forward full rebuild");
        let reverse = reduce_non_join_incremental_facts(vec![unsafe_replace, lineage_broken])
            .expect("reverse full rebuild");

        assert_eq!(forward, reverse);
        let NonJoinIncrementalChangePlan::FullRebuild { reason, .. } = forward else {
            panic!("expected full rebuild");
        };
        assert!(
            reason.contains("a.db.replace: replace snapshot"),
            "{reason}"
        );
        assert!(
            reason.contains("z.db.lineage: previous snapshot"),
            "{reason}"
        );
    }

    #[test]
    fn schema_and_unsupported_errors_remain_explicit_errors() {
        for error in [
            ChangeError::SchemaEvolutionUnsupported {
                detail: "column type changed".to_string(),
            },
            ChangeError::UnsupportedOperation {
                snapshot_id: 2,
                op: "vendor-op".to_string(),
            },
        ] {
            let err = reduce_non_join_incremental_facts(vec![fact("c.db.t", 2, Err(error))])
                .expect_err("unsupported change");
            assert!(err.contains("refresh unsupported"), "{err}");
        }
    }

    #[test]
    fn hard_errors_are_not_masked_by_full_rebuild_in_either_input_order() {
        for (label, hard_fact, expected) in [
            (
                "unsupported change",
                fact(
                    "c.db.unsupported",
                    3,
                    Err(ChangeError::SchemaEvolutionUnsupported {
                        detail: "column type changed".to_string(),
                    }),
                ),
                "refresh unsupported",
            ),
            (
                "endpoint mismatch",
                fact("c.db.mismatch", 3, Ok(batch(2))),
                "expected 3, got 2",
            ),
        ] {
            for full_rebuild_first in [true, false] {
                let full_rebuild_fact = fact(
                    "c.db.full_rebuild",
                    4,
                    Err(ChangeError::LineageBroken {
                        previous_snapshot: 1,
                    }),
                );
                let facts = if full_rebuild_first {
                    vec![full_rebuild_fact, hard_fact.clone()]
                } else {
                    vec![hard_fact.clone(), full_rebuild_fact]
                };
                let err = reduce_non_join_incremental_facts(facts).expect_err(label);
                assert!(
                    err.contains(expected),
                    "{label} must win regardless of input order: {err}"
                );
            }
        }
    }

    #[test]
    fn lineage_maps_are_complete_and_sorted_by_fqn() {
        let plan = reduce_non_join_incremental_facts(vec![
            fact("z.db.t", 3, Ok(batch(3))),
            fact("a.db.t", 2, Ok(batch(2))),
        ])
        .expect("metadata only");
        let NonJoinIncrementalChangePlan::MetadataOnly(lineage) = plan else {
            panic!("expected metadata only");
        };
        assert_eq!(
            lineage.snapshots.keys().cloned().collect::<Vec<_>>(),
            ["a.db.t", "z.db.t"]
        );
        assert_eq!(lineage.snapshots["a.db.t"], 2);
        assert_eq!(lineage.table_uuids["z.db.t"], "uuid-z.db.t");
    }
}
