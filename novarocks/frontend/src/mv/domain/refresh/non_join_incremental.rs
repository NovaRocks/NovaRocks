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

use novarocks_catalog::identifier::TableIdentity;
use novarocks_spi::connector::{
    ConnectorChangeWindowAdmission, ConnectorChangeWindowFullRebuildReason,
    ConnectorChangeWindowReplaceFailure,
};

pub struct NonJoinBaseChange<'a> {
    pub base_ref: &'a TableIdentity,
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub current_table_uuid: &'a str,
    pub admission: ConnectorChangeWindowAdmission,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NonJoinIncrementalChangePlan {
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
pub struct NonJoinLineage {
    pub snapshots: BTreeMap<String, i64>,
    pub table_uuids: BTreeMap<String, String>,
}

#[derive(Clone)]
struct PlannedFact {
    base_fqn: String,
    current_snapshot_id: i64,
    current_table_uuid: String,
    admission: ConnectorChangeWindowAdmission,
}

pub fn plan_non_join_incremental_changes(
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
        .map(|change| PlannedFact {
            base_fqn: change.base_ref.fqn(),
            current_snapshot_id: change.current_snapshot_id,
            current_table_uuid: change.current_table_uuid.to_string(),
            admission: change.admission.clone(),
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
    let mut full_rebuild_reasons = BTreeMap::new();
    for fact in facts {
        match fact.admission {
            ConnectorChangeWindowAdmission::MetadataOnly => {}
            ConnectorChangeWindowAdmission::Incremental {
                has_inserts,
                has_deletes,
                ..
            } => {
                has_insert_changes |= has_inserts;
                has_delete_changes |= has_deletes;
            }
            ConnectorChangeWindowAdmission::FullRebuild(reason) => {
                full_rebuild_reasons.insert(fact.base_fqn, full_rebuild_reason_message(reason));
            }
        }
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

pub(crate) fn full_rebuild_reason_message(
    reason: ConnectorChangeWindowFullRebuildReason,
) -> String {
    match reason {
        ConnectorChangeWindowFullRebuildReason::LineageBroken { from_snapshot_id } => {
            format!("previous snapshot {from_snapshot_id} is not reachable")
        }
        ConnectorChangeWindowFullRebuildReason::UnprovenReplace {
            snapshot_id,
            failure,
        } => {
            let failure = match failure {
                ConnectorChangeWindowReplaceFailure::MissingParent => "missing parent",
                ConnectorChangeWindowReplaceFailure::RecordCountChanged => "record count changed",
                ConnectorChangeWindowReplaceFailure::MissingOrInvalidSummary => {
                    "missing or invalid summary"
                }
                ConnectorChangeWindowReplaceFailure::InvalidDataFileCounts => {
                    "invalid data-file counts"
                }
                ConnectorChangeWindowReplaceFailure::SchemaChanged => "schema changed",
            };
            format!("replace snapshot {snapshot_id} is unproven: {failure}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fact(
        base_fqn: &str,
        current_snapshot_id: i64,
        admission: ConnectorChangeWindowAdmission,
    ) -> PlannedFact {
        PlannedFact {
            base_fqn: base_fqn.to_string(),
            current_snapshot_id,
            current_table_uuid: format!("uuid-{base_fqn}"),
            admission,
        }
    }

    fn incremental(has_inserts: bool, has_deletes: bool) -> ConnectorChangeWindowAdmission {
        ConnectorChangeWindowAdmission::Incremental {
            has_inserts,
            has_deletes,
            partition_impact:
                novarocks_spi::connector::ConnectorChangeWindowPartitionImpact::Unavailable,
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
            fact("c.db.t", 2, ConnectorChangeWindowAdmission::MetadataOnly),
            fact("c.db.t", 3, ConnectorChangeWindowAdmission::MetadataOnly),
        ])
        .expect_err("duplicate base");
        assert!(err.contains("duplicate base c.db.t"), "{err}");
    }

    #[test]
    fn all_empty_batches_are_metadata_only() {
        let plan = reduce_non_join_incremental_facts(vec![fact(
            "c.db.t",
            2,
            ConnectorChangeWindowAdmission::MetadataOnly,
        )])
        .expect("metadata only");
        assert!(matches!(
            plan,
            NonJoinIncrementalChangePlan::MetadataOnly(_)
        ));
    }

    #[test]
    fn insert_only_batch_is_delete_free_change_stream() {
        let plan =
            reduce_non_join_incremental_facts(vec![fact("c.db.t", 2, incremental(true, false))])
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
    fn delete_admission_marks_change_stream_as_delete_capable() {
        let plan =
            reduce_non_join_incremental_facts(vec![fact("c.db.t", 2, incremental(false, true))])
                .expect("delete change stream");
        assert!(matches!(
            plan,
            NonJoinIncrementalChangePlan::ChangeStream {
                has_delete_changes: true,
                ..
            }
        ));
    }

    #[test]
    fn lineage_and_unsafe_replace_request_typed_full_rebuild() {
        for admission in [
            ConnectorChangeWindowAdmission::FullRebuild(
                ConnectorChangeWindowFullRebuildReason::LineageBroken {
                    from_snapshot_id: 1,
                },
            ),
            ConnectorChangeWindowAdmission::FullRebuild(
                ConnectorChangeWindowFullRebuildReason::UnprovenReplace {
                    snapshot_id: 2,
                    failure: ConnectorChangeWindowReplaceFailure::RecordCountChanged,
                },
            ),
        ] {
            let plan = reduce_non_join_incremental_facts(vec![fact("c.db.t", 2, admission)])
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
            ConnectorChangeWindowAdmission::FullRebuild(
                ConnectorChangeWindowFullRebuildReason::LineageBroken {
                    from_snapshot_id: 1,
                },
            ),
        );
        let unsafe_replace = fact(
            "a.db.replace",
            3,
            ConnectorChangeWindowAdmission::FullRebuild(
                ConnectorChangeWindowFullRebuildReason::UnprovenReplace {
                    snapshot_id: 3,
                    failure: ConnectorChangeWindowReplaceFailure::RecordCountChanged,
                },
            ),
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
    fn lineage_maps_are_complete_and_sorted_by_fqn() {
        let plan = reduce_non_join_incremental_facts(vec![
            fact("z.db.t", 3, ConnectorChangeWindowAdmission::MetadataOnly),
            fact("a.db.t", 2, ConnectorChangeWindowAdmission::MetadataOnly),
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
