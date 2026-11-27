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

use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use novarocks_sql::compiler::SqlImvBaseFieldFacts;

use crate::mv::domain::refresh::pin::RefreshSnapshotPin;
use crate::mv::domain::refresh::planning::{RefreshStateBaseline, RefreshStateBaselineSource};
use crate::mv::domain::refresh::target_binding::MvTargetBinding;
use crate::mv::domain::rewrite::analysis::{MvRewriteAnalysisInput, freeze_rewrite_analysis_facts};
use crate::mv::domain::rewrite::context::{
    IcebergMvRewriteContext, MvRewriteAnalysisFacts, MvRewriteRebuildLayout,
    MvRewriteSourceSnapshot, occurrence_table,
};
use crate::mv::domain::storage_observation::{
    MvSchemaValidationObservation, MvSourceSchemaObservation,
};
use novarocks_mv_application::persistence::keyed_documents::{
    MvStrictRecoveryPublication, StoredKeyedMvProjection,
};
use novarocks_mv_application::persistence::runtime_bindings::MvRuntimeBindings;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{
    ConnectorChangeWindowFacts, ConnectorChangeWindowNegativeWeight,
    ConnectorChangeWindowNotIncrementalReason, ConnectorControlRegistry,
    ConnectorExactSemanticRevision, ConnectorRequestContext,
};
use novarocks_sql::planning::mv::SqlMvAggregateCalls;
use novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout;
use novarocks_types::naming::TableIdentity;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OccurrenceWindow {
    Unchanged,
    Changed {
        negative_weight: ConnectorChangeWindowNegativeWeight,
    },
}

impl OccurrenceWindow {
    pub(crate) const fn negative_weight_possible(self) -> bool {
        matches!(
            self,
            Self::Changed {
                negative_weight: ConnectorChangeWindowNegativeWeight::Possible,
            }
        )
    }
}

pub(crate) fn occurrence_window(
    facts: &ConnectorChangeWindowFacts,
) -> Result<OccurrenceWindow, ConnectorChangeWindowNotIncrementalReason> {
    match facts {
        ConnectorChangeWindowFacts::NetZero(_) => Ok(OccurrenceWindow::Unchanged),
        ConnectorChangeWindowFacts::Difference {
            negative_weight, ..
        } => Ok(OccurrenceWindow::Changed {
            negative_weight: *negative_weight,
        }),
        ConnectorChangeWindowFacts::NotIncremental(reason) => Err(*reason),
    }
}

/// Assemble the one canonical rewrite value from already frozen D/L/P/C and
/// provider facts. The caller lends its original target and schema observation.
pub(crate) fn build_neutral_refresh_rewrite_context(
    projection: Arc<StoredKeyedMvProjection>,
    rebuild_layout: Option<Arc<MvRewriteRebuildLayout>>,
    recovery_publication: Option<Arc<MvStrictRecoveryPublication>>,
    current: Vec<MvRewriteSourceSnapshot>,
    previous: Vec<MvRewriteSourceSnapshot>,
    target_binding: &MvTargetBinding,
    target_observation: &MvSchemaValidationObservation,
    analysis: MvRewriteAnalysisFacts,
) -> Result<IcebergMvRewriteContext, String> {
    IcebergMvRewriteContext::from_parts(
        projection,
        rebuild_layout,
        recovery_publication,
        current,
        previous,
        target_binding,
        target_observation,
        analysis,
    )
}

/// Everything one attempt already froze, assembled into the single rewrite
/// value. The target binding and its schema observation must be the same
/// generation; no second Current observation is taken here.
pub(crate) struct RefreshRewriteInputs<'a> {
    pub connector_control: &'a dyn ConnectorControlRegistry,
    pub storage_observation: &'a dyn MvStorageObservationPort,
    pub connector_context: &'a ConnectorRequestContext,
    pub projection: Arc<StoredKeyedMvProjection>,
    pub rebuild_layout: Option<Arc<MvRewriteRebuildLayout>>,
    pub recovery_publication: Option<Arc<MvStrictRecoveryPublication>>,
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
    let facts = &inputs.projection.facts;
    let (binding, interpretation) = inputs.rebuild_layout.as_deref().map_or_else(
        || (facts.binding(), facts.interpretation()),
        |layout| (&layout.binding, &layout.interpretation),
    );
    let captured = io.captured_sources();
    let analysis = freeze_rewrite_analysis_facts(MvRewriteAnalysisInput {
        definition_revision: facts.source_revision().definition_revision,
        binding,
        interpretation,
        runtime_bindings: inputs.runtime_bindings,
        observed_target_partition: inputs.target_binding.partition(),
        join_analysis: inputs.join_analysis.clone(),
        aggregate: inputs.aggregate,
        sql_source_fields: freeze_sql_source_fields(
            &captured,
            binding,
            inputs.storage_observation,
            inputs.connector_context,
        )?,
        direct_output_sources: captured
            .direct_output_sources()
            .cloned()
            .ok_or("MV rewrite lacks the original analyzed output lineage")?,
    })?;
    let mut rewrite = build_neutral_refresh_rewrite_context(
        Arc::clone(&inputs.projection),
        inputs.rebuild_layout,
        inputs.recovery_publication,
        rewrite_current_sources(inputs.pin),
        rewrite_history_sources_original(
            io,
            previous_sources,
            inputs.connector_control,
            inputs.connector_context,
        )
        .await?,
        inputs.target_binding,
        inputs.target_observation,
        analysis,
    )?;
    rewrite.retain_equivalent_source_contexts(&io.captured_sources())?;
    Ok(Arc::new(rewrite))
}

fn freeze_sql_source_fields(
    sources: &crate::query_execution::mv_assembly::refresh_preparation::captured_sources::CapturedRefreshSources,
    binding: &novarocks_mv_application::persistence::codec::BindingDocument,
    storage_observation: &dyn MvStorageObservationPort,
    context: &ConnectorRequestContext,
) -> Result<
    BTreeMap<u32, Vec<SqlImvBaseFieldFacts>>,
    crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError,
> {
    let analyzed = sources
        .analyzed_fields()
        .ok_or("MV rewrite lacks the original analyzed source fields")?;
    binding
        .occurrences
        .iter()
        .map(|occurrence| {
            let projected = sources.require(&occurrence_table(occurrence))?;
            let metadata = projected.sql_metadata()?;
            let observed = crate::mv::domain::storage_observation::observe_source_schema(
                storage_observation,
                &projected.planning_pin,
                &metadata,
                context.clone(),
            )?;
            crate::mv::domain::refresh::observation::validate_observed_schema_locator(
                observed.table(),
                &metadata.identity,
            )?;
            if observed.table_object_id() != &projected.object
                || observed.selected_snapshot_id() != projected.snapshot
            {
                return Err("MV SQL source schema differs from its original pin".into());
            }
            super::schema_contract::validate_relation_occurrence_schema(occurrence, &observed)?;
            let fields = analyzed
                .get(&occurrence.occurrence_id)
                .ok_or("MV rewrite lacks an analyzed source occurrence")?;
            let fields = fields
                .iter()
                .map(|field| {
                    let exact = observed
                        .fields()
                        .iter()
                        .find(|value| value.field_id == field.field_id)
                        .ok_or("MV prepared SQL source lost an admitted field identity")?;
                    let ordinal = usize::try_from(exact.ordinal)
                        .map_err(|_| "MV prepared SQL field ordinal is out of range")?;
                    let arrow = metadata
                        .schema()
                        .fields()
                        .get(ordinal)
                        .ok_or("MV prepared SQL source has no field at its Provider ordinal")?;
                    let declaration = metadata
                        .execution_schema()
                        .binding(ordinal)
                        .ok_or("MV prepared SQL source lacks its exact value declaration")?;
                    if exact.name != field.name
                        || arrow.name() != &exact.name
                        || exact.nullable != field.nullable
                        || declaration.nullable() != exact.nullable
                    {
                        return Err(
                            "MV prepared SQL source changed the admitted field name or nullability"
                                .to_string(),
                        );
                    }
                    SqlImvBaseFieldFacts::try_new(
                        Bytes::copy_from_slice(exact.field_id.as_bytes()),
                        exact.name.clone(),
                        declaration.data_type().clone(),
                        declaration.nullable(),
                    )
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok((occurrence.occurrence_id, fields))
        })
        .collect()
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
    (ConnectorChangeWindowFacts, MvSourceSchemaObservation),
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
    let facts = prepared
        .window_facts()
        .ok_or("MV change-window source has no prepared window facts")?
        .clone();
    let observation = crate::mv::domain::storage_observation::observe_source_schema(
        storage_observation,
        &captured.planning_pin,
        prepared.to_metadata(),
        connector_context.clone(),
    )?;
    Ok((facts, observation))
}
