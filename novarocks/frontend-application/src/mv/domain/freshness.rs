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

//! Current observations shared by statement and background capture sources.

use super::staged_create::{CaptureFailure, CurrentMetadataCapture, CurrentProjectionMetadataCpu};
use async_trait::async_trait;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_mv_application::freshness::{MvSourceObservation, MvSourceState};
use novarocks_mv_application::persistence::codec::RelationOccurrence;
use novarocks_mv_application::persistence::exact_revision::persist_exact_connector_revision;
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorReadSelector, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector, ConnectorTableResolution,
    MvStorageObservationPort,
};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Stage remains typed so the scheduler preserves its original definition
/// classification without the query consumer inheriting scheduling legality.
#[derive(Debug)]
pub(crate) enum CurrentSourceObservationError {
    Provider(ConnectorError),
    Definition(ConnectorError),
}
impl CurrentSourceObservationError {
    pub(crate) fn into_connector(self) -> ConnectorError {
        match self {
            Self::Provider(error) | Self::Definition(error) => error,
        }
    }
}
impl From<ConnectorError> for CurrentSourceObservationError {
    fn from(error: ConnectorError) -> Self {
        Self::Provider(error)
    }
}

/// Implemented by the original statement source or the original background
/// source. It owns pin selection and capture custody, never a global lookup.
#[async_trait]
pub(crate) trait CurrentSourceCapture: Send + Sync {
    async fn observe(
        &self,
        table: ConnectorTableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<MvSourceState, CurrentSourceObservationError>;
}

/// Deduplicate only identical bound-source keys; every occurrence keeps a fact.
/// Replaced identities are Present and belong to the consumer's comparison.
pub(crate) async fn observe_current_sources(
    source: &dyn CurrentSourceCapture,
    occurrences: &[RelationOccurrence],
    context: &ConnectorRequestContext,
) -> Result<Vec<MvSourceObservation>, CurrentSourceObservationError> {
    let mut revisions: BTreeMap<(String, String, String, Vec<u8>), MvSourceState> = BTreeMap::new();
    let mut observations = Vec::with_capacity(occurrences.len());
    for occurrence in occurrences {
        let key = (
            occurrence.catalog_at_binding.clone(),
            occurrence.namespace_at_binding.clone(),
            occurrence.relation_at_binding.clone(),
            occurrence.object_id.as_bytes().to_vec(),
        );
        let state = if let Some(state) = revisions.get(&key) {
            state.clone()
        } else {
            let state = source
                .observe(
                    ConnectorTableIdentity {
                        instance_id: ConnectorInstanceId::parse(&occurrence.catalog_at_binding)
                            .map_err(|error| {
                                CurrentSourceObservationError::Definition(error.into())
                            })?,
                        namespace: occurrence.namespace_at_binding.clone().into(),
                        table: occurrence.relation_at_binding.clone().into(),
                    },
                    context,
                )
                .await?;
            revisions.insert(key, state.clone());
            state
        };
        observations.push(MvSourceObservation {
            relation_occurrence_id: occurrence.occurrence_id,
            state,
        });
    }
    Ok(observations)
}

fn capture_failure(
    failure: CaptureFailure,
) -> Result<MvSourceState, CurrentSourceObservationError> {
    match failure {
        CaptureFailure::Provider(error) if error.kind() == ConnectorErrorKind::NotFound => {
            Ok(MvSourceState::Missing {
                cause: std::error::Error::source(&error)
                    .and_then(|source| source.downcast_ref::<SharedFailureCause>())
                    .cloned()
                    .unwrap_or_else(|| SharedFailureCause::new(error)),
            })
        }
        CaptureFailure::Provider(error) => Err(error.into()),
        CaptureFailure::Admission(error) => {
            Err(ConnectorError::from_domain(ConnectorErrorKind::Internal, error).into())
        }
    }
}

/// Call only with the source's original selected pin and stable capture. Pin
/// selection failures are returned before this function and never mean Missing.
pub(crate) async fn observe_current_source_with_pin(
    pin: &ConnectorControlPlanningPin,
    observer: &dyn MvStorageObservationPort,
    factory: &CurrentProjectionMetadataCpu,
    capture: &Arc<CurrentMetadataCapture>,
    table: ConnectorTableIdentity,
    context: &ConnectorRequestContext,
) -> Result<MvSourceState, CurrentSourceObservationError> {
    let binding = match capture
        .capture(
            pin,
            factory,
            ConnectorTableObjectCaptureRequest {
                table,
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: context.clone(),
            },
        )
        .await
    {
        Ok(binding) => binding,
        Err(failure) => return capture_failure(failure),
    };
    let metadata = &binding.metadata;
    let observed =
        super::storage_observation::observe_refresh_base(observer, pin, metadata, context.clone())?;
    if observed.table() != &metadata.identity {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "MV scheduler source observation identity differs from its captured metadata",
        )
        .into());
    }
    let revision = pin
        .binding()
        .metadata()
        .exact_semantic_revision(&metadata.table, ConnectorReadSelector::Current)?;
    if let Some(snapshot) = observed.current_snapshot_id()
        && pin
            .binding()
            .metadata()
            .read_selector_from_exact_revision(&metadata.table, &revision)?
            != ConnectorReadSelector::SnapshotId(snapshot)
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "MV scheduler source observation and exact revision disagree",
        )
        .into());
    }
    let (object_id, native_data_version) = persist_exact_connector_revision(&revision)
        .map_err(CurrentSourceObservationError::Definition)?;
    Ok(MvSourceState::Present {
        object_id,
        native_data_version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::persistence::identity::{
        NativeDataVersion, ObjectIdentity, SchemaVersion,
    };
    use novarocks_spi::connector::{
        ConnectorStopOwner, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn only_typed_table_capture_not_found_becomes_missing() {
        let original = ConnectorError::new(ConnectorErrorKind::NotFound, "original table missing");
        let MvSourceState::Missing { cause } =
            capture_failure(CaptureFailure::Provider(original.clone())).unwrap()
        else {
            panic!("missing");
        };
        assert_eq!(cause.downcast_ref::<ConnectorError>(), Some(&original));
        let shared = SharedFailureCause::new(original);
        let wrapped = ConnectorError::from_domain(ConnectorErrorKind::NotFound, shared.clone());
        let MvSourceState::Missing { cause } =
            capture_failure(CaptureFailure::Provider(wrapped)).unwrap()
        else {
            panic!("missing");
        };
        assert_eq!(cause, shared);

        for kind in [
            ConnectorErrorKind::Unavailable,
            ConnectorErrorKind::Cancelled,
            ConnectorErrorKind::CorruptData,
        ] {
            let original = ConnectorError::new(kind, "original provider failure");
            assert_eq!(
                capture_failure(CaptureFailure::Provider(original.clone()))
                    .unwrap_err()
                    .into_connector(),
                original
            );
        }
    }
    struct Source {
        calls: AtomicUsize,
        fail: bool,
    }
    #[async_trait]
    impl CurrentSourceCapture for Source {
        async fn observe(
            &self,
            _table: ConnectorTableIdentity,
            _context: &ConnectorRequestContext,
        ) -> Result<MvSourceState, CurrentSourceObservationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "original I/O failure",
                )
                .into());
            }
            Ok(MvSourceState::Present {
                object_id: ObjectIdentity::try_new(vec![2]).unwrap(),
                native_data_version: NativeDataVersion::try_new(vec![3]).unwrap(),
            })
        }
    }
    #[tokio::test]
    async fn observations_deduplicate_sources_and_keep_replaced_objects_present() {
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        let a = RelationOccurrence {
            occurrence_id: 1,
            catalog_at_binding: "ice".into(),
            namespace_at_binding: "db".into(),
            relation_at_binding: "t".into(),
            qualifier_at_binding: "a".into(),
            object_id: ObjectIdentity::try_new(vec![1]).unwrap(),
            schema_version: SchemaVersion::try_new(vec![1]).unwrap(),
            fields: vec![],
        };
        let mut b = a.clone();
        b.occurrence_id = 2;
        let source = Source {
            calls: AtomicUsize::new(0),
            fail: false,
        };
        let observed = observe_current_sources(&source, &[a.clone(), b.clone()], &context)
            .await
            .unwrap();
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            observed
                .iter()
                .map(|item| item.relation_occurrence_id)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(
            matches!(&observed[0].state,MvSourceState::Present{object_id,..} if object_id!=&a.object_id)
        );
        b.object_id = ObjectIdentity::try_new(vec![9]).unwrap();
        observe_current_sources(&source, &[a.clone(), b], &context)
            .await
            .unwrap();
        assert_eq!(source.calls.load(Ordering::SeqCst), 3);
        let failing = Source {
            calls: AtomicUsize::new(0),
            fail: true,
        };
        assert_eq!(
            observe_current_sources(&failing, &[a], &context)
                .await
                .unwrap_err()
                .into_connector()
                .kind(),
            ConnectorErrorKind::Unavailable
        );
    }
}
