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

use crate::mv::domain::model::{AffectedTargetPartitions, MvStorageEngine, RefreshMode};
use crate::mv::domain::refresh::snapshot::{
    BaseSnapshotPolicy, BaseSnapshotStatus, ExecutableRefreshDecision, decide_refresh,
};
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorExactSemanticRevision, ConnectorRequestContext,
};
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;
use novarocks_types::naming::TableIdentity;

pub(crate) struct RefreshPlanningInput<'a> {
    pub(crate) snapshot_policy: BaseSnapshotPolicy,
    pub(crate) base_snapshots: &'a [BaseSnapshotStatus],
    pub(crate) label: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshPlanningDecision {
    pub(crate) refresh: ExecutableRefreshDecision,
}

impl RefreshPlanningDecision {
    pub(crate) fn mode(&self) -> RefreshMode {
        self.refresh.mode()
    }
}

pub(crate) fn decide_refresh_plan(
    input: &RefreshPlanningInput<'_>,
) -> Result<RefreshPlanningDecision, String> {
    let refresh = ExecutableRefreshDecision::from_refresh_decision(decide_refresh(
        input.snapshot_policy,
        input.base_snapshots,
        input.label,
    ))?;
    Ok(RefreshPlanningDecision { refresh })
}

pub(crate) fn decide_requested_refresh_plan(
    input: &RefreshPlanningInput<'_>,
    explicit_full: bool,
) -> Result<RefreshPlanningDecision, String> {
    if explicit_full {
        for base in input.base_snapshots {
            if base.current_snapshot_id_before_pin.is_none() {
                return Err(format!(
                    "{} FULL refresh requires a current snapshot for {}",
                    input.label, base.fqn
                ));
            }
        }
        if input.base_snapshots.is_empty() {
            return Err(format!(
                "{} FULL refresh has no base snapshot status",
                input.label
            ));
        }
        return Ok(RefreshPlanningDecision {
            refresh: ExecutableRefreshDecision::FirstRefresh,
        });
    }
    decide_refresh_plan(input)
}

/// One base relation this refresh reads, as the occurrence it is.
///
/// The table name is what two mentions of one table share, so it cannot be
/// what tells them apart; the occurrence id, minted by the CREATE documents,
/// is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshBaseRelationOccurrence {
    pub occurrence_id: SqlMvRelationOccurrenceId,
    pub table: TableIdentity,
}

impl RefreshBaseRelationOccurrence {
    /// How this relation is named to an operator: the table, and which mention
    /// of it. Both, because either alone is ambiguous in a self-join.
    pub(crate) fn display(&self) -> String {
        occurrence_display(self.occurrence_id, &self.table)
    }
}

pub(crate) fn occurrence_display(
    occurrence_id: SqlMvRelationOccurrenceId,
    table: &TableIdentity,
) -> String {
    format!("{} (occurrence {})", table.fqn(), occurrence_id.get())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshStateBaselineSource {
    pub(crate) occurrence_id: SqlMvRelationOccurrenceId,
    pub(crate) table: TableIdentity,
    pub(crate) semantic_revision: ConnectorExactSemanticRevision,
}

impl RefreshStateBaselineSource {
    pub(crate) fn table_object_id(&self) -> Result<ConnectorTableObjectId, String> {
        ConnectorTableObjectId::try_new(self.semantic_revision.object_identity().value().clone())
            .map_err(|error| {
                format!(
                    "MV refresh baseline has invalid object identity for {}: {error}",
                    occurrence_display(self.occurrence_id, &self.table),
                )
            })
    }
}

pub(crate) fn baseline_revision_for_occurrence<'a>(
    baseline: &'a RefreshStateBaseline,
    occurrence: &RefreshBaseRelationOccurrence,
) -> Result<&'a ConnectorExactSemanticRevision, String> {
    let RefreshStateBaseline::SnapshotBacked {
        previous_sources, ..
    } = baseline;
    let mut matches = previous_sources.iter().filter(|source| {
        source.occurrence_id == occurrence.occurrence_id && source.table == occurrence.table
    });
    let source = matches.next().ok_or_else(|| {
        format!(
            "MV refresh baseline has no exact revision for {}",
            occurrence.display()
        )
    })?;
    if matches.next().is_some() {
        return Err(format!(
            "MV refresh baseline repeats the exact revision for {}",
            occurrence.display()
        ));
    }
    Ok(&source.semantic_revision)
}

/// What each baseline source names, keyed by the occurrence it belongs to.
///
/// Occurrences, not table names: a definition may read one table twice, and
/// the two mentions were pinned separately. Keying by name would make the
/// second overwrite the first, which is why this used to refuse such a
/// baseline outright.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BaselinePredecessors {
    pub(crate) snapshots: BTreeMap<SqlMvRelationOccurrenceId, i64>,
    pub(crate) table_object_ids: BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshStateBaseline {
    SnapshotBacked {
        /// Ordered P inputs. Provider-native data versions remain opaque; a
        /// consumer that needs a typed selector must ask the provider rather
        /// than reconstructing one from these bytes.
        previous_sources: Vec<RefreshStateBaselineSource>,
        target_snapshot_id: Option<i64>,
        target_table_uuid: String,
        definition_fingerprint: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshPlanContract {
    pub mv_id: Option<i64>,
    pub target: MvTarget,
    pub(crate) storage_engine: MvStorageEngine,
    pub decision: ExecutableRefreshDecision,
    pub state_baseline: RefreshStateBaseline,
    /// Ordered, one entry per base scan, in the definition's own canonical
    /// relation order. Two entries may name one table.
    pub base_refs: Vec<RefreshBaseRelationOccurrence>,
    /// Keyed by occurrence, so a definition reading one table twice pins each
    /// mention at the revision it was read at.
    pub snapshot_pins: BTreeMap<SqlMvRelationOccurrenceId, Option<i64>>,
    pub(crate) affected_partitions: AffectedTargetPartitions,
}

impl RefreshPlanContract {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn mode(&self) -> RefreshMode {
        self.decision.mode()
    }
}

pub(crate) async fn baseline_predecessors_original(
    io: &crate::query_execution::mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationIo,
    previous_sources: &[RefreshStateBaselineSource],
    connector_control: &dyn ConnectorControlResolver,
    connector_context: &ConnectorRequestContext,
) -> Result<
    BaselinePredecessors,
    crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError,
> {
    let mut predecessors = BaselinePredecessors::default();
    for source in previous_sources {
        let named = occurrence_display(source.occurrence_id, &source.table);
        if predecessors.snapshots.contains_key(&source.occurrence_id) {
            return Err(format!(
                "MV refresh baseline names {named} more than once; one occurrence is pinned at \
                 one source"
            )
            .into());
        }
        let snapshot_id = io
            .snapshot_from_revision(
                connector_control,
                &source.table,
                &source.semantic_revision,
                connector_context,
            )
            .await?;
        predecessors
            .snapshots
            .insert(source.occurrence_id, snapshot_id);
        predecessors
            .table_object_ids
            .insert(source.occurrence_id, source.table_object_id()?);
    }
    Ok(predecessors)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::mv::domain::model::{AffectedTargetPartitions, MvStorageEngine, RefreshMode};
    use crate::mv::domain::refresh::snapshot::{
        BaseSnapshotPolicy, BaseSnapshotStatus, ExecutableRefreshDecision,
    };
    use novarocks_spi::connector::{
        ConnectorExactSemanticRevision, ConnectorProviderId, ConnectorTableObjectId,
    };
    use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
    use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;
    use novarocks_types::naming::TableIdentity;

    fn object_id(value: &str) -> ConnectorTableObjectId {
        ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(value.as_bytes()))
            .expect("test object ID")
    }

    const LABEL: &str = "iceberg MV test";

    fn decide(
        snapshot_policy: BaseSnapshotPolicy,
        base_snapshots: &[BaseSnapshotStatus],
    ) -> Result<RefreshPlanningDecision, String> {
        decide_refresh_plan(&RefreshPlanningInput {
            snapshot_policy,
            base_snapshots,
            label: LABEL,
        })
    }

    #[test]
    fn planning_preserves_skip_empty_vs_metadata_only() {
        let skip = decide(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.left", None, None)],
        )
        .unwrap();
        let metadata = decide(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.left", Some(10), Some(10))],
        )
        .unwrap();

        assert_eq!(skip.refresh, ExecutableRefreshDecision::SkipEmpty);
        assert_eq!(skip.mode(), RefreshMode::Noop);
        assert_eq!(metadata.refresh, ExecutableRefreshDecision::MetadataOnly);
        assert_eq!(metadata.mode(), RefreshMode::Noop);
    }

    #[test]
    fn planning_projects_first_and_incremental_modes() {
        let first = decide(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.left", None, Some(10))],
        )
        .unwrap();
        let incremental = decide(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.left", Some(10), Some(11))],
        )
        .unwrap();

        assert_eq!(first.refresh, ExecutableRefreshDecision::FirstRefresh);
        assert_eq!(first.mode(), RefreshMode::Full);
        assert_eq!(incremental.refresh, ExecutableRefreshDecision::Incremental);
        assert_eq!(incremental.mode(), RefreshMode::Incremental);
    }

    #[test]
    fn explicit_full_rebuilds_an_unchanged_published_source() {
        let statuses = [BaseSnapshotStatus::new("ice.db.left", Some(10), Some(10))];
        let decision = decide_requested_refresh_plan(
            &RefreshPlanningInput {
                snapshot_policy: BaseSnapshotPolicy::SingleBase,
                base_snapshots: &statuses,
                label: LABEL,
            },
            true,
        )
        .unwrap();
        assert_eq!(decision.refresh, ExecutableRefreshDecision::FirstRefresh);
        assert_eq!(decision.mode(), RefreshMode::Full);

        let missing = [BaseSnapshotStatus::new("ice.db.left", Some(10), None)];
        assert!(
            decide_requested_refresh_plan(
                &RefreshPlanningInput {
                    snapshot_policy: BaseSnapshotPolicy::SingleBase,
                    base_snapshots: &missing,
                    label: LABEL,
                },
                true,
            )
            .unwrap_err()
            .contains("requires a current snapshot")
        );
    }

    #[test]
    fn planning_preserves_fail_fast_reason() {
        let error = decide(
            BaseSnapshotPolicy::SingleBase,
            &[BaseSnapshotStatus::new("ice.db.left", Some(10), None)],
        )
        .unwrap_err();

        assert_eq!(
            error,
            "iceberg MV test refresh cannot continue: previously-refreshed base snapshot for ice.db.left is no longer reachable"
        );
    }

    #[test]
    fn planning_uses_explicit_snapshot_policy() {
        let statuses = [
            BaseSnapshotStatus::new("ice.db.left", None, Some(10)),
            BaseSnapshotStatus::new("ice.db.right", None, None),
        ];

        assert_eq!(
            decide(BaseSnapshotPolicy::AllBasesRequired, &statuses).unwrap_err(),
            "iceberg MV test refresh cannot run first refresh because only some bases have current snapshots; load all bases or recreate the MV"
        );
        let partial = decide(BaseSnapshotPolicy::JoinPairPartialInitialSkip, &statuses).unwrap();
        assert_eq!(partial.refresh, ExecutableRefreshDecision::SkipEmpty);
        assert_eq!(partial.mode(), RefreshMode::Noop);
    }

    #[test]
    fn plan_contract_preserves_common_fields() {
        let target = MvTarget {
            catalog: Some("ice".to_string()),
            database: "db".to_string(),
            name: "mv".to_string(),
        };
        let base_refs = vec![
            RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(0),
                table: TableIdentity::new("ice", "db", "left"),
            },
            RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(1),
                table: TableIdentity::new("ice", "db", "right"),
            },
        ];
        let snapshot_pins = BTreeMap::from([
            (SqlMvRelationOccurrenceId::new(0), Some(10)),
            (SqlMvRelationOccurrenceId::new(1), Some(20)),
        ]);
        let affected_partitions = AffectedTargetPartitions::not_derived("join planning");
        let previous_object = object_id("object-left");
        let state_baseline = RefreshStateBaseline::SnapshotBacked {
            previous_sources: vec![RefreshStateBaselineSource {
                occurrence_id: SqlMvRelationOccurrenceId::new(7),
                table: base_refs[0].table.clone(),
                semantic_revision:
                    ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                        ConnectorProviderId::parse("iceberg").unwrap(),
                        &previous_object,
                        Some(9),
                    )
                    .unwrap(),
            }],
            target_snapshot_id: Some(30),
            target_table_uuid: "uuid-target".to_string(),
            definition_fingerprint: "definition-v1".to_string(),
        };

        let contract = RefreshPlanContract {
            mv_id: Some(42),
            target: target.clone(),
            storage_engine: MvStorageEngine::Iceberg,
            decision: ExecutableRefreshDecision::Incremental,
            state_baseline: state_baseline.clone(),
            base_refs: base_refs.clone(),
            snapshot_pins: snapshot_pins.clone(),
            affected_partitions: affected_partitions.clone(),
        };

        assert_eq!(contract.mv_id, Some(42));
        assert_eq!(contract.target, target);
        assert_eq!(contract.storage_engine, MvStorageEngine::Iceberg);
        assert_eq!(contract.mode(), RefreshMode::Incremental);
        assert_eq!(contract.state_baseline, state_baseline);
        assert_eq!(contract.base_refs, base_refs);
        assert_eq!(contract.snapshot_pins, snapshot_pins);
        assert_eq!(contract.affected_partitions, affected_partitions);
    }
}
