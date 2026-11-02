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
use crate::mv::domain::storage_observation::{
    MvSchemaValidationObservation, MvSourceSchemaObservation,
};
use novarocks_mv_application::persistence::runtime_bindings::MvRuntimeBindings;
use novarocks_mv_application::persistence::{
    keyed_documents::StoredKeyedMvProjection, runtime_bindings::MvExactTargetSchemaFacts,
};
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{
    ConnectorChangeWindowAdmission, ConnectorControlRegistry, ConnectorExactSemanticRevision,
    ConnectorRequestContext,
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
    projection: Arc<StoredKeyedMvProjection>,
    current: Vec<MvRewriteSourceSnapshot>,
    previous: Vec<MvRewriteSourceSnapshot>,
    target_table_uuid: String,
    target_arrow_schema: arrow::datatypes::SchemaRef,
    target_write_fields: Arc<[novarocks_spi::connector::ConnectorWriteFieldRequest]>,
    exact_target: MvExactTargetSchemaFacts,
    analysis: MvRewriteAnalysisFacts,
) -> Result<Arc<IcebergMvRewriteContext>, String> {
    IcebergMvRewriteContext::from_parts(
        projection,
        current,
        previous,
        target_table_uuid,
        target_arrow_schema,
        target_write_fields,
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
    pub projection: Arc<StoredKeyedMvProjection>,
    pub pin: &'a RefreshSnapshotPin,
    pub state_baseline: &'a RefreshStateBaseline,
    pub target_binding: &'a MvTargetBinding,
    pub target_observation: &'a MvSchemaValidationObservation,
    pub runtime_bindings: &'a MvRuntimeBindings,
    /// D's explicit direct-join analysis from its effective query.
    pub join_analysis: novarocks_sql::planning::mv::SqlMvRefreshJoinAnalysis,
    pub aggregate: Option<(SqlMvAggregateCalls, SqlMvAggregatePhysicalLayout)>,
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

pub(crate) async fn freeze_refresh_rewrite_context_original(
    io: &crate::query_execution::mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationIo,
    inputs: RefreshRewriteInputs<'_>,
) -> Result<
    Arc<IcebergMvRewriteContext>,
    crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError,
> {
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
        };
    if let Some(expected_uuid) = baseline_target_uuid {
        if inputs.target_binding.table_uuid() != expected_uuid {
            return Err("MV refresh target UUID drifted after planning".into());
        }
        if inputs.target_binding.current_snapshot_id() != baseline_target_snapshot_id {
            return Err("MV refresh target snapshot drifted after planning".into());
        }
    }
    if inputs.target_observation.table() != inputs.target_binding.identity() {
        return Err(
            "MV refresh target binding and schema observation name different tables".into(),
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
        rewrite_history_sources_original(
            io,
            previous_sources,
            inputs.connector_control,
            inputs.connector_context,
        )
        .await?,
        inputs.target_binding.table_uuid().to_string(),
        inputs.target_binding.physical_write_schema()?,
        inputs.target_binding.write_input_fields()?,
        inputs.target_observation.exact_schema().clone(),
        analysis,
    )
    .map_err(Into::into)
}

pub(crate) async fn rewrite_history_sources_original(
    io: &crate::query_execution::mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationIo,
    previous_sources: &[RefreshStateBaselineSource],
    connector_control: &dyn ConnectorControlRegistry,
    connector_context: &ConnectorRequestContext,
) -> Result<
    Vec<MvRewriteSourceSnapshot>,
    crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError,
> {
    let mut result = Vec::with_capacity(previous_sources.len());
    for source in previous_sources {
        let snapshot_id = io
            .snapshot_from_revision(
                connector_control,
                &source.table,
                &source.semantic_revision,
                connector_context,
            )
            .await?;
        result.push(MvRewriteSourceSnapshot {
            occurrence_id: source.occurrence_id,
            snapshot_id,
            table_object_id: source.table_object_id()?,
            semantic_revision: source.semantic_revision.clone(),
        });
    }
    Ok(result)
}

/// Project policy and source schema from the one completed Root preparation.
/// This function cannot start catalog or endpoint discovery.
pub(crate) fn project_prepared_change_window_for_table_original(
    io: &crate::query_execution::mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationIo,
    storage_observation: &dyn MvStorageObservationPort,
    table: &TableIdentity,
    from_revision: &ConnectorExactSemanticRevision,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    connector_context: &ConnectorRequestContext,
) -> Result<
    (ConnectorChangeWindowAdmission, MvSourceSchemaObservation),
    crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError,
> {
    use crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError;
    let captured = io.captured_sources().require(table)?;
    if captured.snapshot != Some(to_snapshot_id) {
        return Err("MV window To differs from its original pin".into());
    }
    let from = from_revision.canonical_read_point();
    if from
        != Some(
            novarocks_spi::connector::ConnectorCanonicalReadPoint::Snapshot(Some(from_snapshot_id)),
        )
    {
        return Err("MV window From differs from its exact revision".into());
    }
    let prepared = captured.receipt(Some(from_revision)).ok_or_else(|| {
        PreparationError::user("MV change window has no completed original typed preparation")
    })?;
    let admission = prepared
        .admission()
        .ok_or("MV change-window source has no lineage admission")?
        .clone();
    let observation = crate::mv::domain::storage_observation::observe_source_schema(
        storage_observation,
        &captured.lease,
        prepared.to_metadata(),
        connector_context.clone(),
    )?;
    Ok((admission, observation))
}
