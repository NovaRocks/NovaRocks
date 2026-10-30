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

//! Collects per-table maintenance facts through the neutral connector surface:
//! snapshot list, current-snapshot summary counters, typed maintenance policy
//! facts, references, the provider-signed compaction count, and the
//! downstream-consumer floor that protects incremental MV lineage.
//!
//! Nothing here interprets a storage format. The provider signs every fact; the
//! frontend owns every policy decision made from them.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorExactSemanticRevision, ConnectorInstanceId, ConnectorReadSelector,
    ConnectorTableIdentity, ConnectorTableResolution,
};

#[cfg(test)]
use novarocks_mv_application::persistence::exact_revision::persist_exact_connector_revision;
use novarocks_mv_application::persistence::exact_revision::restore_exact_query_revision;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

/// Provider facts only. Frontend owns every policy decision and retry state.
#[derive(Clone, Debug, Default)]
pub struct TableMaintenanceStats {
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Vec<SnapshotInfo>,
    pub total_data_files: Option<u64>,
    pub max_compactable_data_files: Option<u64>,
    pub total_files_size_bytes: Option<u64>,
    pub total_delete_files: Option<u64>,
    /// Typed maintenance policy facts declared by the table. `None` means the
    /// table declares no usable value for that key; defaults and clamping are
    /// policy and belong to the frontend, never to this fact layer.
    pub maintenance_enabled: Option<bool>,
    pub expire_max_snapshot_age_ms: Option<i64>,
    pub expire_min_snapshots_to_keep: Option<u32>,
    pub target_file_size_bytes: Option<i64>,
    pub non_default_reference_count: usize,
    pub downstream_floor_ts_ms: Option<i64>,
    pub downstream_floor_unknown: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DownstreamFloor {
    pub(crate) floor_ts_ms: Option<i64>,
    pub(crate) unknown: bool,
}

/// Minimum published consumed-version timestamp across every durable relation
/// occurrence that reads this exact table object. A name match never revives a
/// dependency on a replaced object, while an unresolved version of the current
/// object blocks expiration for safety.
pub(crate) fn downstream_floor(
    projections: &[StoredKeyedMvProjection],
    catalog: &str,
    namespace: &str,
    table: &str,
    current_revision: &ConnectorExactSemanticRevision,
    timestamp_by_revision: &BTreeMap<ConnectorExactSemanticRevision, i64>,
) -> DownstreamFloor {
    let mut floor_ts: Option<i64> = None;
    let mut unknown = false;
    for projection in projections {
        let Some(publication) = projection.facts.publication() else {
            continue;
        };
        let inputs = publication
            .inputs
            .iter()
            .map(|input| (input.relation_occurrence_id, input))
            .collect::<BTreeMap<_, _>>();
        for occurrence in projection
            .facts
            .definition()
            .relation_occurrences
            .iter()
            .filter(|occurrence| {
                occurrence.catalog_at_binding == catalog
                    && occurrence.namespace_at_binding == namespace
                    && occurrence.relation_at_binding == table
            })
        {
            let Some(input) = inputs.get(&occurrence.occurrence_id) else {
                unknown = true;
                continue;
            };
            let Ok(revision) =
                restore_exact_query_revision(&input.object_id, &input.native_data_version)
            else {
                unknown = true;
                continue;
            };
            if revision.object_identity() == current_revision.object_identity() {
                match timestamp_by_revision.get(&revision) {
                    Some(ts) => floor_ts = Some(floor_ts.map_or(*ts, |floor| floor.min(*ts))),
                    None => unknown = true,
                }
            }
        }
    }
    DownstreamFloor {
        floor_ts_ms: floor_ts,
        unknown,
    }
}

/// Stable fields of one actual cadence observation. Capture owns its Provider
/// loader/CPU/footer before any foreign poll; the observation owns the same
/// generation's compaction F, including after a borrowed wait disappears.
pub(crate) struct OriginalMaintenanceFacts {
    capture: Arc<super::super::staged_create::CurrentMetadataCapture>,
    lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    table: ConnectorTableIdentity,
    context: novarocks_spi::connector::ConnectorRequestContext,
    first_metadata: Option<novarocks_spi::connector::ConnectorTableObjectBinding>,
    observation: Option<novarocks_spi::connector::ConnectorMaxCompactableDataFilesExecution>,
    compactable: Option<Result<Option<u64>, novarocks_spi::connector::ConnectorError>>,
    observation_closed: bool,
    current_metadata: Option<novarocks_spi::connector::ConnectorTableObjectBinding>,
    raw: Option<Result<TableMaintenanceStats, crate::mv::domain::application::MvApplicationError>>,
}
impl OriginalMaintenanceFacts {
    pub(crate) fn try_new(
        controls: &dyn novarocks_spi::connector::ConnectorControlRegistry,
        target: &novarocks_table_maintenance::MaintenanceTarget,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Self, novarocks_spi::connector::ConnectorError> {
        use novarocks_spi::connector::{
            ConnectorControlResolver, ConnectorError, ConnectorErrorKind,
        };
        let instance = ConnectorInstanceId::parse(&target.catalog).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
        })?;
        let lease = ConnectorControlResolver::acquire_current(controls, &instance)?;
        Ok(Self {
            capture: super::super::staged_create::CurrentMetadataCapture::new(),
            lease,
            table: ConnectorTableIdentity {
                instance_id: instance,
                namespace: target.namespace.clone().into(),
                table: target.table.clone().into(),
            },
            context,
            first_metadata: None,
            observation: None,
            compactable: None,
            observation_closed: false,
            current_metadata: None,
            raw: None,
        })
    }
    pub(crate) async fn observe_in(
        &mut self,
        controls: &dyn novarocks_spi::connector::ConnectorControlRegistry,
        storage: &dyn novarocks_spi::connector::MvStorageObservationPort,
        factory: &super::super::staged_create::CurrentProjectionMetadataCpu,
        projections: &[StoredKeyedMvProjection],
    ) {
        use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
        use novarocks_spi::connector::{
            ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector,
        };
        fn provider(error: novarocks_spi::connector::ConnectorError) -> MvApplicationError {
            MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
        }
        let result = async {
            if self.first_metadata.is_none() {
                let metadata = self.capture.capture(&self.lease, factory, ConnectorTableObjectCaptureRequest {
                    table: self.table.clone(), resolution: ConnectorTableResolution::StrictBaseTable,
                    selector: ConnectorTableObjectSelector::Current, context: self.context.clone(),
                }).await.map_err(|failure| failure.into_application())?;
                self.first_metadata = Some(metadata);
            }
            if self.observation.is_none() {
                let cpu = factory.port_for(&self.lease).map_err(|error| {
                    let cause = novarocks_failure_contract::SharedFailureCause::new(error);
                    MvApplicationError::new(MvApplicationErrorKind::Engine, cause.detail()).with_original_failure(cause)
                })?;
                self.observation = Some(crate::connector::metadata_maintenance::begin_captured_max_compactable_data_files(
                    controls, &self.lease, &self.first_metadata.as_ref().unwrap().metadata, self.context.clone(), cpu,
                ).map_err(provider)?);
            }
            if self.compactable.is_none() {
                let raw = std::future::poll_fn(|cx| {
                    let original = self.observation.as_mut().unwrap();
                    let polled = original.poll_result(cx);
                    if polled.is_pending()
                        && let Some(cause) = original.local_failure() {
                            std::panic::panic_any(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("MV compaction facts original CPU exit is unproven", cause));
                        }
                    polled
                }).await;
                // Scalar/error reaches this stable original field before its
                // Provider/CPU footer can be retired, including refusal paths.
                self.compactable = Some(raw.map(|value| value.value()));
            }
            if !self.observation_closed {
                let closed = std::future::poll_fn(|cx| {
                    let original = self.observation.as_mut().unwrap();
                    let polled = original.poll_closed(cx);
                    if let Some(cause) = original.local_failure() {
                        std::panic::panic_any(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("MV compaction facts original CPU exit is unproven", cause));
                    }
                    polled
                }).await;
                if let Some(cause) = self.observation.as_ref().unwrap().local_failure() {
                    std::panic::panic_any(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("MV compaction facts original CPU exit is unproven", cause));
                }
                if let Err(error) = closed {
                    std::panic::panic_any(novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("MV compaction facts original close is unproven", Arc::new(error)));
                }
                self.observation_closed = true;
            }
            let compactable = self.compactable.as_ref().unwrap().clone().map_err(provider)?;
            // Same ordering as the existing author: compaction forces the
            // Provider refresh before the maintenance metadata observation.
            if self.current_metadata.is_none() {
                let metadata = self.capture.capture(&self.lease, factory, ConnectorTableObjectCaptureRequest {
                    table: self.table.clone(), resolution: ConnectorTableResolution::StrictBaseTable,
                    selector: ConnectorTableObjectSelector::Current, context: self.context.clone(),
                }).await.map_err(|failure| failure.into_application())?;
                self.current_metadata = Some(metadata);
            }
            let metadata = &self.current_metadata.as_ref().unwrap().metadata;
            let observed = crate::mv::domain::storage_observation::observe_maintenance_metadata(storage, &self.lease, metadata, self.context.clone()).map_err(provider)?;
            finish_table_stats_original(&self.lease, metadata, &observed, self.table.instance_id.as_str(), &self.table.namespace, &self.table.table, projections, compactable)
                .map_err(|(_context, error)| provider(error))
        }.await;
        assert!(self.raw.is_none(), "one original maintenance facts outcome");
        self.raw = Some(result);
    }
    pub(crate) fn take_result_in(
        &mut self,
        target: &mut Option<
            Result<TableMaintenanceStats, crate::mv::domain::application::MvApplicationError>,
        >,
    ) {
        assert!(
            target.is_none(),
            "original maintenance facts destination is occupied"
        );
        *target = self.raw.take();
    }
    pub(crate) fn capture_observer(
        &self,
    ) -> Arc<super::super::staged_create::CurrentMetadataCapture> {
        self.capture.clone()
    }
    pub(crate) fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.capture.local_health_failure()
    }
    pub(crate) fn poll_closed(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        self.capture.poll_closed(cx)
    }
}

fn finish_table_stats_original(
    exact_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    observed: &crate::mv::domain::storage_observation::MvMaintenanceMetadataObservation,
    catalog: &str,
    namespace: &str,
    table: &str,
    projections: &[StoredKeyedMvProjection],
    max_compactable_data_files: Option<u64>,
) -> Result<TableMaintenanceStats, (String, novarocks_spi::connector::ConnectorError)> {
    let snapshots: Vec<SnapshotInfo> = observed
        .snapshots()
        .iter()
        .map(|snapshot| SnapshotInfo {
            snapshot_id: snapshot.snapshot_id,
            timestamp_ms: snapshot.timestamp_ms,
        })
        .collect();
    let current_revision = exact_lease
        .binding()
        .metadata()
        .exact_semantic_revision(&metadata.table, ConnectorReadSelector::Current)
        .map_err(|error| {
            (
                format!("observe exact {catalog}.{namespace}.{table} maintenance identity"),
                error,
            )
        })?;
    let timestamp_by_revision = snapshots
        .iter()
        .map(|snapshot| {
            exact_lease
                .binding()
                .metadata()
                .exact_semantic_revision(
                    &metadata.table,
                    ConnectorReadSelector::SnapshotId(snapshot.snapshot_id),
                )
                .map(|revision| (revision, snapshot.timestamp_ms))
                .map_err(|error| {
                    (
                        format!(
                            "observe exact {catalog}.{namespace}.{table} snapshot {}",
                            snapshot.snapshot_id
                        ),
                        error,
                    )
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    let floor = downstream_floor(
        projections,
        catalog,
        namespace,
        table,
        &current_revision,
        &timestamp_by_revision,
    );
    let policy = *observed.policy();

    Ok(TableMaintenanceStats {
        current_snapshot_id: observed.current_snapshot_id(),
        snapshots,
        total_data_files: observed.total_data_files(),
        max_compactable_data_files,
        total_files_size_bytes: observed.total_files_size_bytes(),
        total_delete_files: observed.total_delete_files(),
        maintenance_enabled: policy.maintenance_enabled,
        expire_max_snapshot_age_ms: policy.expire_max_snapshot_age_ms,
        expire_min_snapshots_to_keep: policy.expire_min_snapshots_to_keep,
        target_file_size_bytes: policy.target_file_size_bytes,
        non_default_reference_count: observed.non_default_reference_count(),
        downstream_floor_ts_ms: floor.floor_ts_ms,
        downstream_floor_unknown: floor.unknown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use bytes::Bytes;
    use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
    use novarocks_mv_application::product::MvTarget;
    use novarocks_spi::connector::{ConnectorProviderId, ConnectorTableObjectId};

    fn exact(object: &'static [u8], snapshot_id: i64) -> ConnectorExactSemanticRevision {
        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            ConnectorProviderId::parse("iceberg").unwrap(),
            &ConnectorTableObjectId::try_new(Bytes::from_static(object)).unwrap(),
            Some(snapshot_id),
        )
        .unwrap()
    }

    fn projection(
        mv_id: i64,
        relation: &str,
        object: &'static [u8],
        snapshots: [i64; 2],
    ) -> StoredKeyedMvProjection {
        let target_name = format!("mv_{mv_id}");
        let mut fixture = ProjectionFixture::new(
            MvTarget::from_parts(Some("ice"), "analytics", &target_name),
            Some(11),
        );
        for (index, occurrence) in fixture
            .definition
            .relation_occurrences
            .iter_mut()
            .enumerate()
        {
            occurrence.relation_at_binding = relation.to_string();
            let revision = exact(object, snapshots[index]);
            let (object_id, data_version) = persist_exact_connector_revision(&revision).unwrap();
            occurrence.object_id = object_id.clone();
            let input = fixture
                .publication
                .as_mut()
                .unwrap()
                .inputs
                .iter_mut()
                .find(|input| input.relation_occurrence_id == occurrence.occurrence_id)
                .unwrap();
            input.object_id = object_id;
            input.native_data_version = data_version;
        }
        StoredKeyedMvProjection {
            mv_id,
            facts: fixture.build().unwrap(),
        }
    }

    #[test]
    fn floor_is_min_consumed_version_timestamp_across_occurrences() {
        let current = exact(b"source", 30);
        let timestamp_by_revision =
            BTreeMap::from([(exact(b"source", 10), 1_000), (exact(b"source", 20), 2_000)]);
        let projections = vec![projection(1, "orders", b"source", [20, 10])];
        let floor = downstream_floor(
            &projections,
            "ice",
            "sales",
            "orders",
            &current,
            &timestamp_by_revision,
        );
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }

    #[test]
    fn floor_is_none_without_consumers() {
        let current = exact(b"source", 30);
        let projections = vec![projection(1, "other", b"source", [10, 10])];
        let floor = downstream_floor(
            &projections,
            "ice",
            "sales",
            "orders",
            &current,
            &BTreeMap::new(),
        );
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: None,
                unknown: false
            }
        );
    }

    #[test]
    fn floor_unknown_when_current_object_consumed_version_is_not_retained() {
        let current = exact(b"source", 30);
        let projections = vec![projection(1, "orders", b"source", [99, 99])];
        let floor = downstream_floor(
            &projections,
            "ice",
            "sales",
            "orders",
            &current,
            &BTreeMap::new(),
        );
        assert!(floor.unknown);
    }

    #[test]
    fn replaced_object_does_not_hold_the_new_objects_snapshots() {
        let current = exact(b"new-source", 30);
        let projections = vec![projection(1, "orders", b"old-source", [10, 10])];
        let floor = downstream_floor(
            &projections,
            "ice",
            "sales",
            "orders",
            &current,
            &BTreeMap::new(),
        );
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: None,
                unknown: false,
            }
        );
    }
}
