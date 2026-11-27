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

//! Re-admit only the original content application when its target baseline is equivalent.

use super::{pending_fence::PendingFence, target_binding::MvTargetBinding};
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::query_execution::mv_assembly::refresh_preparation::{
    async_facts::PreparationError, io_gate::MvRefreshPreparationIo,
};
use novarocks_mv_application::persistence::{
    codec::Marker,
    exact_revision::restore_exact_query_revision,
    keyed_documents::{
        MvHistoricalPublication, MvKeyedObservedCurrentDocuments,
        resolve_historical_keyed_publication,
    },
};
use novarocks_spi::connector::read_stack::{
    ConnectorReadChangeWindow, ConnectorReadSourcePreparationRequest, SchemaTableName,
};
use novarocks_spi::connector::{
    ConnectorChangeWindowFacts, ConnectorExactSemanticRevision, ConnectorRequestContext,
};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct StrictRecovery {
    pub(crate) fence: Arc<PendingFence>,
    pub(crate) baseline: MvHistoricalPublication,
    pub(crate) targets: BTreeMap<u32, ConnectorExactSemanticRevision>,
}

// Design: ADR-0278 (docs/adr/ADR-0278-mv-maintains-visible-tuples-and-replays-verification.md)
pub(crate) async fn prove_strict_recovery(
    io: &MvRefreshPreparationIo,
    source: &IcebergMvCorePorts,
    observed: &MvKeyedObservedCurrentDocuments,
    target: &MvTargetBinding,
    context: &ConnectorRequestContext,
) -> Result<StrictRecovery, PreparationError> {
    let eligibility = observed.eligibility();
    if !eligibility.strict_recovery_required || observed.interpretation.is_aggregate() {
        return Err("MV strict recovery has no eligible Pending marker".into());
    }
    let mut obligations = eligibility.active.iter().filter_map(|marker| match marker {
        Marker::Pending {
            publication,
            evidence,
        } => Some((publication, evidence)),
        _ => None,
    });
    let (publication, evidence) = obligations
        .next()
        .ok_or("MV strict recovery lost its original Pending marker")?;
    if obligations.next().is_some() {
        return Err(
            "MV has multiple unresolved Pending markers and requires an explicit rebuild".into(),
        );
    }
    let storage_uuid = uuid::Uuid::parse_str(target.table_uuid())
        .map_err(|error| format!("MV strict recovery target UUID is not canonical: {error}"))?;
    if storage_uuid.as_bytes() != &evidence.storage_uuid
        || target.current_snapshot_id() != observed.view.current_snapshot
        || evidence.verification_requirement.estimate_bytes()
            > evidence.verification_requirement.budget()
        || i64::try_from(evidence.verification_requirement.budget()).is_err()
    {
        return Err("MV strict recovery lost its original target or execution budget".into());
    }
    let current_snapshot = target
        .current_snapshot_id()
        .ok_or("MV strict recovery target has no readable current snapshot")?;
    let pin = target.planning_pin();
    let documents = pin.derive_document_storage_lease()?;
    let request = crate::mv::domain::staged_create::keyed_request(
        &documents,
        observed.view.table.clone(),
        observed.view.object_id.clone(),
        context.clone(),
    );
    let full = io.observe_full(&documents, request).await?;
    if full.current.object_id != observed.view.object_id
        || full.current.metadata_version != observed.view.metadata_version
        || full.current.current_records != observed.view.current_records
        || full.current.markers != observed.view.markers
        || full.current.roots != observed.view.roots
        || full.current.slots != observed.view.slots
    {
        return Err("MV Current changed during strict recovery observation".into());
    }
    let baseline = resolve_historical_keyed_publication(&full, evidence.baseline_snapshot)
        .map_err(|error| format!("MV strict recovery old semantics are unavailable: {error}"))?;
    if baseline.publication.publication_id != *publication
        || baseline.versions.mapping.identity().to_bytes() != *evidence.mapping_revision.as_bytes()
        || baseline.versions != observed.versions
        || baseline.interpretation.is_aggregate()
    {
        return Err("MV strict recovery cannot replay its original D, L, B and M".into());
    }
    // The target's Current write observation can carry only snapshot refs.
    // Complete the same observation before asking 10a to prove its window.
    let scoped = crate::connector::context_for_planning_pin_typed(pin, context.clone())?;
    let prepared = io
        .complete_table_observation(pin, target.metadata(), scoped.clone())
        .await?;
    let metadata = crate::connector::schema_creation::with_schema_creation(&scoped, |creation| {
        pin.binding()
            .metadata()
            .create_prepared_table(prepared, creation)
    })
    .map_err(crate::connector::schema_creation::connector_schema_error)?;
    let complete = crate::mv::domain::storage_observation::observe_refresh_target(
        source.storage_observation(),
        pin,
        &metadata,
        scoped.clone(),
    )?;
    let exact = crate::mv::domain::storage_observation::observe_schema_validation(
        source.storage_observation(),
        pin,
        &metadata,
        scoped,
    )?;
    if exact.exact_schema().metadata_version != observed.view.metadata_version
        || metadata.identity != target.metadata().identity
        || metadata.version != target.metadata().version
        || complete.table_uuid() != target.table_uuid()
        || complete.current_snapshot_id() != Some(current_snapshot)
    {
        return Err("MV strict recovery completed a different Current observation".into());
    }
    let source_facts = metadata
        .source_read_facts()
        .cloned()
        .ok_or("MV strict recovery target has no complete access provenance")?;
    let endpoint = |snapshot| {
        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            pin.binding().descriptor().provider_id.clone(),
            &observed.view.object_id,
            Some(snapshot),
        )
    };
    let window = ConnectorReadSourcePreparationRequest::change_window(
        SchemaTableName::try_new(
            observed.view.table.namespace.as_ref(),
            observed.view.table.table.as_ref(),
        )?,
        metadata.table.clone(),
        source_facts,
        observed.view.object_id.clone(),
        ConnectorReadChangeWindow::new(evidence.baseline_snapshot, current_snapshot),
        endpoint(evidence.baseline_snapshot)?,
        endpoint(current_snapshot)?,
    );
    let baseline_window = io.source_read(source, pin, window, context.clone()).await?;
    if !matches!(
        baseline_window.window_facts(),
        Some(ConnectorChangeWindowFacts::NetZero(_))
    ) {
        return Err(format!(
            "MV strict recovery baseline {} is not equivalent to current snapshot {current_snapshot}; explicit rebuild is required",
            evidence.baseline_snapshot,
        ).into());
    }
    if evidence.source_windows.len() != baseline.publication.inputs.len() {
        return Err("MV strict recovery Pending does not cover every original occurrence".into());
    }
    let mut targets = BTreeMap::new();
    for (window, input) in evidence
        .source_windows
        .iter()
        .zip(&baseline.publication.inputs)
    {
        if window.occurrence != input.relation_occurrence_id
            || window.from != input.native_data_version
        {
            return Err(
                "MV strict recovery Pending differs from the original input watermark".into(),
            );
        }
        if targets
            .insert(
                window.occurrence,
                restore_exact_query_revision(&input.object_id, &window.to)?,
            )
            .is_some()
        {
            return Err("MV strict recovery repeats an original occurrence".into());
        }
    }
    let publication_proof = baseline
        .for_strict_recovery(observed, publication, evidence)
        .map_err(|error| error.to_string())?;
    let fence = Arc::new(PendingFence::recover(
        observed,
        publication.clone(),
        evidence.clone(),
        baseline.publication.logical_result_rows,
        baseline.record.clone(),
        publication_proof,
    )?);
    Ok(StrictRecovery {
        fence,
        baseline,
        targets,
    })
}
