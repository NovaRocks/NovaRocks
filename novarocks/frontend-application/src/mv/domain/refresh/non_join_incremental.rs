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

use std::collections::BTreeMap;

use novarocks_spi::connector::{
    ConnectorChangeWindowFacts, ConnectorChangeWindowNegativeWeight,
    ConnectorChangeWindowNotIncrementalReason, ConnectorTableObjectId,
};
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
use novarocks_types::naming::TableIdentity;

use super::rewrite_context::{OccurrenceWindow, occurrence_window};

pub struct NonJoinBaseChange<'a> {
    pub occurrence_id: SqlMvRelationOccurrenceId,
    pub base_ref: &'a TableIdentity,
    pub current_snapshot_id: i64,
    pub current_table_object_id: &'a ConnectorTableObjectId,
    pub window_facts: ConnectorChangeWindowFacts,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NonJoinIncrementalChangePlan {
    MetadataOnly(NonJoinLineage),
    FullRebuild {
        lineage: NonJoinLineage,
        reasons: Vec<NonJoinRebuildReason>,
    },
    ChangeStream {
        lineage: NonJoinLineage,
        negative_weight: ConnectorChangeWindowNegativeWeight,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NonJoinRebuildReason {
    occurrence_id: SqlMvRelationOccurrenceId,
    base_fqn: String,
    reason: ConnectorChangeWindowNotIncrementalReason,
}

impl NonJoinRebuildReason {
    pub(crate) fn log_message(&self) -> String {
        format!(
            "occurrence {} ({}): {}",
            self.occurrence_id.get(),
            self.base_fqn,
            not_incremental_reason_message(self.reason),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NonJoinLineage {
    pub snapshots: BTreeMap<SqlMvRelationOccurrenceId, i64>,
    pub table_object_ids: BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
}

#[derive(Clone)]
struct PlannedFact {
    occurrence_id: SqlMvRelationOccurrenceId,
    base_fqn: String,
    current_snapshot_id: i64,
    current_table_object_id: ConnectorTableObjectId,
    window_facts: ConnectorChangeWindowFacts,
}

pub fn plan_non_join_incremental_changes(
    changes: &[NonJoinBaseChange<'_>],
) -> Result<NonJoinIncrementalChangePlan, String> {
    let facts = changes
        .iter()
        .map(|change| PlannedFact {
            occurrence_id: change.occurrence_id,
            base_fqn: change.base_ref.fqn(),
            current_snapshot_id: change.current_snapshot_id,
            current_table_object_id: change.current_table_object_id.clone(),
            window_facts: change.window_facts.clone(),
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
    let mut table_object_ids = BTreeMap::new();
    for fact in &facts {
        if snapshots
            .insert(fact.occurrence_id, fact.current_snapshot_id)
            .is_some()
        {
            return Err(format!(
                "iceberg MV incremental refresh has duplicate relation occurrence {}",
                fact.occurrence_id.get()
            ));
        }
        table_object_ids.insert(fact.occurrence_id, fact.current_table_object_id.clone());
    }
    let lineage = NonJoinLineage {
        snapshots,
        table_object_ids,
    };

    let mut has_difference = false;
    let mut negative_weight = ConnectorChangeWindowNegativeWeight::Absent;
    let mut reasons = Vec::new();
    for fact in facts {
        match occurrence_window(&fact.window_facts) {
            Ok(OccurrenceWindow::Unchanged) => {}
            Ok(OccurrenceWindow::Changed {
                negative_weight: weight,
            }) => {
                has_difference = true;
                if weight == ConnectorChangeWindowNegativeWeight::Possible {
                    negative_weight = weight;
                }
            }
            Err(reason) => {
                reasons.push(NonJoinRebuildReason {
                    occurrence_id: fact.occurrence_id,
                    base_fqn: fact.base_fqn,
                    reason,
                });
            }
        }
    }
    if !reasons.is_empty() {
        reasons.sort_unstable_by_key(|reason| reason.occurrence_id);
        return Ok(NonJoinIncrementalChangePlan::FullRebuild { lineage, reasons });
    }

    if has_difference {
        Ok(NonJoinIncrementalChangePlan::ChangeStream {
            lineage,
            negative_weight,
        })
    } else {
        Ok(NonJoinIncrementalChangePlan::MetadataOnly(lineage))
    }
}

pub(crate) fn not_incremental_reason_message(
    reason: ConnectorChangeWindowNotIncrementalReason,
) -> String {
    match reason {
        ConnectorChangeWindowNotIncrementalReason::SnapshotAncestryBroken { from_snapshot_id } => {
            format!("previous snapshot {from_snapshot_id} is not reachable")
        }
        ConnectorChangeWindowNotIncrementalReason::SchemaChanged { snapshot_id } => {
            format!("snapshot {snapshot_id} changes the schema beyond field renames")
        }
    }
}

#[cfg(test)]
mod tests;
