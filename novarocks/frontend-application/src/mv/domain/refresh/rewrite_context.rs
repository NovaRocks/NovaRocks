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

//! Frozen refresh rewrite inputs shared by domain planning and query assembly.

use std::sync::Arc;

use crate::mv::domain::refresh::pin::RefreshSnapshotPin;
use crate::mv::domain::refresh::planning::{RefreshStateBaseline, RefreshStateBaselineSource};
use crate::mv::domain::refresh::target_binding::MvTargetBinding;
use crate::mv::domain::rewrite::analysis::{MvRewriteAnalysisInput, freeze_rewrite_analysis_facts};
use crate::mv::domain::rewrite::context::{
    IcebergMvRewriteContext, MvRewriteAnalysisFacts, MvRewriteSourceSnapshot,
};
use crate::mv::domain::storage_observation::MvSchemaValidationObservation;
use novarocks_mv_application::persistence::runtime_bindings::MvRuntimeBindings;
use novarocks_mv_application::persistence::{
    projection::StoredMvProjection, runtime_bindings::MvExactTargetSchemaFacts,
};
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{
    ConnectorChangeWindowAdmission, ConnectorControlRegistry, ConnectorExactSemanticRevision,
    ConnectorReadSelector, ConnectorRequestContext, ConnectorScanAdmission,
    ConnectorTableResolution,
};
use novarocks_sql::planning::mv::SqlMvAggregateCalls;
use novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout;
use novarocks_types::naming::TableIdentity;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdmittedChangeFacts {
    pub has_inserts: bool,
    pub has_deletes: bool,
}

pub fn admitted_change_facts(
    admission: &ConnectorChangeWindowAdmission,
) -> Result<AdmittedChangeFacts, String> {
    match admission {
        ConnectorChangeWindowAdmission::MetadataOnly => Ok(AdmittedChangeFacts::default()),
        ConnectorChangeWindowAdmission::Incremental {
            has_inserts,
            has_deletes,
            ..
        } => Ok(AdmittedChangeFacts {
            has_inserts: *has_inserts,
            has_deletes: *has_deletes,
        }),
        ConnectorChangeWindowAdmission::FullRebuild(reason) => Err(
            crate::mv::domain::refresh::non_join_incremental::full_rebuild_reason_message(*reason),
        ),
    }
}

/// Assemble the one canonical rewrite value from already frozen D/L/P/C and
/// provider facts. Runtime handles and a second Current observation are not
/// accepted at this boundary.
pub fn build_neutral_refresh_rewrite_context(
    projection: Arc<StoredMvProjection>,
    current: Vec<MvRewriteSourceSnapshot>,
    previous: Vec<MvRewriteSourceSnapshot>,
    target_table_uuid: String,
    target_arrow_schema: arrow::datatypes::SchemaRef,
    exact_target: MvExactTargetSchemaFacts,
    analysis: MvRewriteAnalysisFacts,
) -> Result<Arc<IcebergMvRewriteContext>, String> {
    IcebergMvRewriteContext::from_parts(
        projection,
        current,
        previous,
        target_table_uuid,
        target_arrow_schema,
        exact_target,
        analysis,
    )
    .map(Arc::new)
}

/// Everything one attempt already froze, assembled into the single rewrite
/// value. The target binding and its schema observation must be the same
/// generation; no second Current observation is taken here.
pub(crate) struct RefreshRewriteInputs<'a> {
    pub connector_control: &'a dyn ConnectorControlRegistry,
    pub connector_context: &'a ConnectorRequestContext,
    pub projection: Arc<StoredMvProjection>,
    pub pin: &'a RefreshSnapshotPin,
    pub state_baseline: &'a RefreshStateBaseline,
    pub target_binding: &'a MvTargetBinding,
    pub target_observation: &'a MvSchemaValidationObservation,
    pub runtime_bindings: &'a MvRuntimeBindings,
    /// D's explicit direct-join analysis from its effective query.
    pub join_analysis: novarocks_sql::planning::mv::SqlMvRefreshJoinAnalysis,
    pub aggregate: Option<(SqlMvAggregateCalls, SqlMvAggregatePhysicalLayout)>,
}

/// Freeze this attempt's rewrite context from already-observed facts.
pub(crate) fn freeze_refresh_rewrite_context(
    inputs: RefreshRewriteInputs<'_>,
) -> Result<Arc<IcebergMvRewriteContext>, String> {
    let (previous_sources, baseline_target_uuid, baseline_target_snapshot_id) =
        match inputs.state_baseline {
            RefreshStateBaseline::SnapshotBacked {
                previous_sources,
                target_snapshot_id,
                target_table_uuid,
                ..
            } => (
                previous_sources.as_slice(),
                Some(target_table_uuid.as_str()),
                *target_snapshot_id,
            ),
            RefreshStateBaseline::Pinless => (&[][..], None, None),
        };
    if let Some(expected_uuid) = baseline_target_uuid {
        if inputs.target_binding.table_uuid() != expected_uuid {
            return Err("MV refresh target UUID drifted after planning".to_string());
        }
        if inputs.target_binding.current_snapshot_id() != baseline_target_snapshot_id {
            return Err("MV refresh target snapshot drifted after planning".to_string());
        }
    }
    if inputs.target_observation.table() != inputs.target_binding.identity() {
        return Err(
            "MV refresh target binding and schema observation name different tables".to_string(),
        );
    }
    let analysis = freeze_rewrite_analysis_facts(MvRewriteAnalysisInput {
        projection: inputs.projection.as_ref(),
        runtime_bindings: inputs.runtime_bindings,
        observed_target_partition: inputs.target_binding.partition(),
        join_analysis: inputs.join_analysis.clone(),
        aggregate: inputs.aggregate,
    })?;
    build_neutral_refresh_rewrite_context(
        Arc::clone(&inputs.projection),
        rewrite_current_sources(inputs.pin),
        rewrite_history_sources(
            previous_sources,
            inputs.connector_control,
            inputs.connector_context,
        )?,
        inputs.target_binding.table_uuid().to_string(),
        inputs.target_binding.physical_write_schema()?,
        inputs.target_observation.exact_schema().clone(),
        analysis,
    )
}

/// The current rewrite sources are exactly this attempt's frozen pin. No
/// second observation and no FQN keying: one entry per D occurrence.
pub(crate) fn rewrite_current_sources(pin: &RefreshSnapshotPin) -> Vec<MvRewriteSourceSnapshot> {
    pin.occurrences()
        .iter()
        .map(|occurrence| MvRewriteSourceSnapshot {
            occurrence_id: occurrence.occurrence_id(),
            snapshot_id: occurrence.snapshot_id(),
            table_object_id: occurrence.table_object_id().clone(),
            semantic_revision: occurrence.semantic_revision().clone(),
        })
        .collect()
}

/// The published baseline records each source as an exact semantic revision,
/// and the change window this rewrite plans needs a read point.
///
/// The provider validates and interprets each opaque revision on a retained
/// exact table generation. The later scan still admits the change window.
pub(crate) fn rewrite_history_sources(
    previous_sources: &[RefreshStateBaselineSource],
    connector_control: &dyn ConnectorControlRegistry,
    connector_context: &ConnectorRequestContext,
) -> Result<Vec<MvRewriteSourceSnapshot>, String> {
    previous_sources
        .iter()
        .map(|source| {
            let snapshot_id =
                crate::mv::domain::refresh_io::snapshot_from_exact_revision_with_ports(
                    connector_control,
                    &source.table,
                    &source.semantic_revision,
                    connector_context,
                )?;
            Ok(MvRewriteSourceSnapshot {
                occurrence_id: source.occurrence_id,
                snapshot_id,
                table_object_id: source.table_object_id()?,
                semantic_revision: source.semantic_revision.clone(),
            })
        })
        .collect()
}

pub(crate) fn observe_and_admit_change_window_for_table(
    connector_control: &dyn ConnectorControlRegistry,
    storage_observation: &dyn MvStorageObservationPort,
    table: &TableIdentity,
    from_revision: &ConnectorExactSemanticRevision,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    connector_context: &ConnectorRequestContext,
) -> Result<
    (
        ConnectorChangeWindowAdmission,
        MvSchemaValidationObservation,
    ),
    String,
> {
    let exact_lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &table.catalog)?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    let provider = exact_lease.binding().metadata();
    let to_revision = provider
        .exact_semantic_revision(&metadata.table, ConnectorReadSelector::Current)
        .map_err(|error| {
            format!(
                "observe exact change-window end for {}: {error}",
                table.fqn()
            )
        })?;
    let window = provider
        .change_window_from_exact_revisions(&metadata.table, from_revision, &to_revision)
        .map_err(|error| {
            format!(
                "admit exact change-window revisions for {}: {error}",
                table.fqn()
            )
        })?;
    if window.from_exclusive() != from_snapshot_id || window.to_inclusive() != to_snapshot_id {
        return Err(format!(
            "MV change-window revision and frozen numeric pin disagree for {}",
            table.fqn()
        ));
    }
    let scan = crate::connector::scan_admission::admit_connector_change_window(
        &metadata.table,
        &metadata.schema,
        &exact_lease,
        connector_context.clone(),
        window,
    )?;
    let ConnectorScanAdmission::ChangeWindow(admission) = scan.admission() else {
        return Err("connector returned a snapshot admission for a change-window scan".to_string());
    };
    let observation = crate::mv::domain::storage_observation::observe_schema_validation(
        storage_observation,
        &exact_lease,
        &metadata,
        connector_context.clone(),
    )
    .map_err(|error| {
        format!(
            "observe MV schema validation facts for {}: {error}",
            table.fqn()
        )
    })?;
    Ok((admission.clone(), observation))
}
