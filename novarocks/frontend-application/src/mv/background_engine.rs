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

//! Frontend adapter bound to the MV background runtime.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use novarocks_mv_application::maintenance::{
    MvBackgroundEngineError, MvBackgroundEngineErrorKind, MvMaintenanceFacts,
};
use novarocks_mv_application::persistence::codec::{PublicationInput, RelationOccurrence};
use novarocks_mv_application::persistence::exact_revision::persist_exact_connector_revision;
use novarocks_mv_application::persistence::identity::{NativeDataVersion, ObjectIdentity};
use novarocks_spi::connector::ConnectorRequestContext;

type SourceRevisionKey = (String, String, String, Vec<u8>);
type SourceRevision = (ObjectIdentity, NativeDataVersion);

pub(crate) fn maintenance_facts_from_stats(
    stats: crate::mv::domain::maintenance::stats::TableMaintenanceStats,
) -> MvMaintenanceFacts {
    MvMaintenanceFacts {
        current_snapshot_id: stats.current_snapshot_id,
        total_data_files: stats.total_data_files.map(|value| value as i64),
        max_compactable_data_files: stats.max_compactable_data_files.map(|value| value as i64),
        total_delete_files: stats.total_delete_files.map(|value| value as i64),
        total_files_size_bytes: stats.total_files_size_bytes.map(|value| value as i64),
        oldest_snapshot_timestamp_ms: stats
            .snapshots
            .iter()
            .map(|snapshot| snapshot.timestamp_ms)
            .min(),
        snapshot_count: stats.snapshots.len(),
        non_default_reference_count: stats.non_default_reference_count,
        downstream_floor_ts_ms: stats.downstream_floor_ts_ms,
        downstream_floor_unknown: stats.downstream_floor_unknown,
        maintenance_enabled: stats.maintenance_enabled,
        expire_max_snapshot_age_ms: stats.expire_max_snapshot_age_ms,
        expire_min_snapshots_to_keep: stats.expire_min_snapshots_to_keep,
        target_file_size_bytes: stats.target_file_size_bytes,
    }
}

/// Same occurrence/revision author as the legacy method, with actual I/O in
/// one management pass and the selected Provider's original CPU capture.
pub(crate) async fn current_source_inputs_in(
    ports: &IcebergMvCorePorts,
    occurrences: &[RelationOccurrence],
    context: &ConnectorRequestContext,
    factory: &crate::mv::domain::staged_create::CurrentProjectionMetadataCpu,
    capture: &Arc<crate::mv::domain::staged_create::CurrentMetadataCapture>,
) -> Result<Vec<PublicationInput>, MvBackgroundEngineError> {
    use novarocks_spi::connector::{
        ConnectorControlResolver, ConnectorInstanceId, ConnectorReadSelector,
        ConnectorTableIdentity, ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector,
        ConnectorTableResolution,
    };
    let provider_error = |error: novarocks_spi::connector::ConnectorError| {
        let cause = crate::mv::domain::readiness::connector_failure_cause(error);
        MvBackgroundEngineError::new(
            MvBackgroundEngineErrorKind::TransientUnavailable,
            cause.detail(),
        )
        .with_original_failure(cause)
    };
    let mut revisions: BTreeMap<SourceRevisionKey, SourceRevision> = BTreeMap::new();
    let mut inputs = Vec::with_capacity(occurrences.len());
    for occurrence in occurrences {
        let table = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse(&occurrence.catalog_at_binding).map_err(
                |error| {
                    MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::InvalidDefinition,
                        error.to_string(),
                    )
                },
            )?,
            namespace: occurrence.namespace_at_binding.clone().into(),
            table: occurrence.relation_at_binding.clone().into(),
        };
        let key = (
            occurrence.catalog_at_binding.clone(),
            occurrence.namespace_at_binding.clone(),
            occurrence.relation_at_binding.clone(),
            occurrence.object_id.as_bytes().to_vec(),
        );
        let (object_id, native_data_version) = if let Some(revision) = revisions.get(&key) {
            revision.clone()
        } else {
            let lease = ConnectorControlResolver::acquire_current(
                ports.connector_control(),
                &table.instance_id,
            )
            .map_err(provider_error)?;
            let binding = capture
                .capture(
                    &lease,
                    factory,
                    ConnectorTableObjectCaptureRequest {
                        table: table.clone(),
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        selector: ConnectorTableObjectSelector::Current,
                        context: context.clone(),
                    },
                )
                .await
                .map_err(|failure| {
                    let error = failure.into_application();
                    let mut projected = MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::TransientUnavailable,
                        error.message(),
                    );
                    if let Some(cause) = error.original_failure() {
                        projected = projected.with_original_failure(cause.clone());
                    }
                    projected
                })?;
            let metadata = &binding.metadata;
            let observed = crate::mv::domain::storage_observation::observe_refresh_base(
                ports.storage_observation(),
                &lease,
                metadata,
                context.clone(),
            )
            .map_err(provider_error)?;
            if observed.table() != &metadata.identity {
                return Err(MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TransientUnavailable,
                    "MV scheduler source observation identity differs from its captured metadata",
                ));
            }
            let revision = lease
                .binding()
                .metadata()
                .exact_semantic_revision(&metadata.table, ConnectorReadSelector::Current)
                .map_err(provider_error)?;
            if let Some(snapshot) = observed.current_snapshot_id()
                && lease
                    .binding()
                    .metadata()
                    .read_selector_from_exact_revision(&metadata.table, &revision)
                    .map_err(provider_error)?
                    != ConnectorReadSelector::SnapshotId(snapshot)
            {
                return Err(MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TransientUnavailable,
                    "MV scheduler source observation and exact revision disagree",
                ));
            }
            let revision = persist_exact_connector_revision(&revision).map_err(|error| {
                MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::InvalidDefinition,
                    error.to_string(),
                )
            })?;
            revisions.insert(key, revision.clone());
            revision
        };
        if object_id != occurrence.object_id {
            return Err(MvBackgroundEngineError::new(
                MvBackgroundEngineErrorKind::InvalidDefinition,
                format!(
                    "MV source occurrence {} belongs to a replaced object",
                    occurrence.occurrence_id
                ),
            ));
        }
        inputs.push(PublicationInput {
            relation_occurrence_id: occurrence.occurrence_id,
            object_id,
            native_data_version,
        });
    }
    Ok(inputs)
}
