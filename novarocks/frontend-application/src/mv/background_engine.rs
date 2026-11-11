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

use std::sync::Arc;

use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use novarocks_mv_application::maintenance::{
    MvBackgroundEngineError, MvBackgroundEngineErrorKind, MvMaintenanceFacts,
};
use novarocks_mv_application::persistence::codec::{PublicationInput, RelationOccurrence};
use novarocks_spi::connector::ConnectorRequestContext;

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
    let source = BackgroundCapture {
        ports,
        factory,
        capture,
    };
    let observations =
        crate::mv::domain::freshness::observe_current_sources(&source, occurrences, context)
            .await
            .map_err(provider_error)?;
    scheduler_inputs(occurrences, observations)
}

fn provider_error(
    error: crate::mv::domain::freshness::CurrentSourceObservationError,
) -> MvBackgroundEngineError {
    let kind = if matches!(
        &error,
        crate::mv::domain::freshness::CurrentSourceObservationError::Definition(_)
    ) {
        MvBackgroundEngineErrorKind::InvalidDefinition
    } else {
        MvBackgroundEngineErrorKind::TransientUnavailable
    };
    let error = error.into_connector();
    let cause = crate::mv::domain::readiness::connector_failure_cause(error);
    MvBackgroundEngineError::new(kind, cause.detail()).with_original_failure(cause)
}

struct BackgroundCapture<'a> {
    ports: &'a IcebergMvCorePorts,
    factory: &'a crate::mv::domain::staged_create::CurrentProjectionMetadataCpu,
    capture: &'a Arc<crate::mv::domain::staged_create::CurrentMetadataCapture>,
}
#[async_trait::async_trait]
impl crate::mv::domain::freshness::CurrentSourceCapture for BackgroundCapture<'_> {
    async fn observe(
        &self,
        table: novarocks_spi::connector::ConnectorTableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<
        novarocks_mv_application::freshness::MvSourceState,
        crate::mv::domain::freshness::CurrentSourceObservationError,
    > {
        let pin = novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            self.ports.connector_control(),
            &table.instance_id,
        )?;
        crate::mv::domain::freshness::observe_current_source_with_pin(
            &pin,
            self.ports.storage_observation(),
            self.factory,
            self.capture,
            table,
            context,
        )
        .await
    }
}

pub(crate) fn scheduler_inputs(
    occurrences: &[RelationOccurrence],
    observations: Vec<novarocks_mv_application::freshness::MvSourceObservation>,
) -> Result<Vec<PublicationInput>, MvBackgroundEngineError> {
    use novarocks_mv_application::freshness::MvSourceState;
    let by_id = occurrences
        .iter()
        .map(|item| (item.occurrence_id, item))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut inputs = Vec::with_capacity(observations.len());
    for observation in observations {
        let occurrence = by_id
            .get(&observation.relation_occurrence_id)
            .ok_or_else(|| {
                MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::InvalidDefinition,
                    "MV source observation names an unknown occurrence",
                )
            })?;
        let (object_id, native_data_version) = match observation.state {
            MvSourceState::Missing { cause } => {
                return Err(MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TransientUnavailable,
                    cause.detail(),
                )
                .with_original_failure(cause));
            }
            MvSourceState::Present {
                object_id,
                native_data_version,
            } => (object_id, native_data_version),
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

#[cfg(test)]
mod freshness_tests {
    use super::*;
    use novarocks_failure_contract::SharedFailureCause;
    use novarocks_mv_application::freshness::{MvSourceObservation, MvSourceState};
    use novarocks_mv_application::persistence::identity::{
        NativeDataVersion, ObjectIdentity, SchemaVersion,
    };
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    fn occurrence() -> RelationOccurrence {
        RelationOccurrence {
            occurrence_id: 1,
            catalog_at_binding: "ice".into(),
            namespace_at_binding: "db".into(),
            relation_at_binding: "t".into(),
            qualifier_at_binding: "t".into(),
            object_id: ObjectIdentity::try_new(vec![1]).unwrap(),
            schema_version: SchemaVersion::try_new(vec![1]).unwrap(),
            fields: vec![],
        }
    }
    #[test]
    fn scheduler_adapter_keeps_missing_and_replaced_classification() {
        let original = ConnectorError::new(ConnectorErrorKind::NotFound, "original missing source");
        let cause = SharedFailureCause::new(original.clone());
        let error = scheduler_inputs(
            &[occurrence()],
            vec![MvSourceObservation {
                relation_occurrence_id: 1,
                state: MvSourceState::Missing {
                    cause: cause.clone(),
                },
            }],
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            MvBackgroundEngineErrorKind::TransientUnavailable
        );
        assert_eq!(error.original_failure(), Some(&cause));
        let error = scheduler_inputs(
            &[occurrence()],
            vec![MvSourceObservation {
                relation_occurrence_id: 1,
                state: MvSourceState::Present {
                    object_id: ObjectIdentity::try_new(vec![2]).unwrap(),
                    native_data_version: NativeDataVersion::try_new(vec![1]).unwrap(),
                },
            }],
        )
        .unwrap_err();
        assert_eq!(error.kind(), MvBackgroundEngineErrorKind::InvalidDefinition);
        assert_eq!(
            error.message(),
            "MV source occurrence 1 belongs to a replaced object"
        );
    }
}
