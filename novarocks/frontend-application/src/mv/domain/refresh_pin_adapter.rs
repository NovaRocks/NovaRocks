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

use crate::mv::domain::refresh::pin::RefreshSnapshotPinOccurrence;
use novarocks_mv_application::persistence::exact_revision::persist_exact_connector_revision;
use novarocks_spi::connector::ConnectorExactSemanticRevision;
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
use novarocks_types::naming::TableIdentity;

pub(crate) fn finish_pin_occurrence(
    occurrence_id: u32,
    expected_object: &novarocks_mv_application::persistence::identity::ObjectIdentity,
    base_ref: TableIdentity,
    observed: crate::mv::domain::storage_observation::MvRefreshBaseObservation,
    exact_revision: ConnectorExactSemanticRevision,
) -> Result<RefreshSnapshotPinOccurrence, String> {
    let snapshot_id = observed.current_snapshot_id().ok_or_else(|| {
        format!(
            "iceberg base table {} has no current snapshot; cannot freeze refresh pin",
            base_ref.fqn()
        )
    })?;
    if exact_revision.object_identity().value().as_ref() != observed.object_id().as_bytes().as_ref()
    {
        return Err(format!(
            "MV refresh occurrence {} observation and exact revision identify different objects",
            occurrence_id,
        ));
    }
    let (persisted_object, _) = persist_exact_connector_revision(&exact_revision)
        .map_err(|error| format!("persist MV refresh source identity: {error}"))?;
    if &persisted_object != expected_object {
        return Err(format!(
            "MV refresh occurrence {} source object changed since D was persisted",
            occurrence_id,
        ));
    }
    RefreshSnapshotPinOccurrence::try_new(
        SqlMvRelationOccurrenceId::new(occurrence_id),
        base_ref,
        snapshot_id,
        observed.object_id().clone(),
        exact_revision,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::application::MvApplicationErrorKind;
    use crate::mv::domain::refresh::pin::RefreshSnapshotPin;
    use crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError;
    use novarocks_spi::connector::ConnectorRequestContext;

    fn observation(
        object: &[u8],
        snapshot: Option<i64>,
    ) -> (
        crate::mv::domain::storage_observation::MvRefreshBaseObservation,
        ConnectorExactSemanticRevision,
    ) {
        use novarocks_spi::connector::{
            ConnectorProviderId, ConnectorTableIdentity, ConnectorTableObjectId,
        };
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1,
            1,
        )
        .unwrap();
        let object =
            ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(object)).unwrap();
        let revision = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            ConnectorProviderId::parse("iceberg").unwrap(),
            &object,
            snapshot,
        )
        .unwrap();
        let observation =
            crate::mv::domain::storage_observation::MvRefreshBaseObservation::try_new(
                ConnectorTableIdentity {
                    instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(
                        "iceberg.rest",
                    )
                    .unwrap(),
                    namespace: "db".into(),
                    table: "base".into(),
                },
                object,
                snapshot,
                &context,
            )
            .unwrap();
        (observation, revision)
    }

    fn base_ref() -> TableIdentity {
        TableIdentity {
            catalog: "ice".into(),
            namespace: "db".into(),
            table: "base".into(),
        }
    }

    #[test]
    fn common_pin_author_preserves_order_and_exact_revision_after_observation_changes() {
        let (observed, revision) = observation(b"original", Some(17));
        let (persisted, _) = persist_exact_connector_revision(&revision).unwrap();
        let mut entries = Vec::new();
        for occurrence_id in [9, 2] {
            entries.push(
                finish_pin_occurrence(
                    occurrence_id,
                    &persisted,
                    base_ref(),
                    observed.clone(),
                    revision.clone(),
                )
                .unwrap(),
            );
        }
        let pin = RefreshSnapshotPin::try_from_occurrences(entries).unwrap();
        assert!(RefreshSnapshotPin::try_from_occurrences(Vec::new()).is_err());
        assert!(
            RefreshSnapshotPin::try_from_occurrences(vec![
                pin.occurrences()[0].clone(),
                pin.occurrences()[0].clone(),
            ])
            .is_err()
        );
        let (_later_observation, _later_revision) = observation(b"replacement", Some(18));
        assert_eq!(
            pin.occurrences()
                .iter()
                .map(|entry| entry.occurrence_id())
                .collect::<Vec<_>>(),
            vec![
                SqlMvRelationOccurrenceId::new(9),
                SqlMvRelationOccurrenceId::new(2)
            ]
        );
        for entry in pin.occurrences() {
            assert_eq!(entry.snapshot_id(), 17);
            assert_eq!(entry.table_object_id(), observed.object_id());
            assert_eq!(entry.semantic_revision(), &revision);
        }
    }

    #[test]
    fn common_pin_author_keeps_persisted_object_and_snapshot_domain_validation() {
        let (observed, revision) = observation(b"original", Some(17));
        let (_, replacement) = observation(b"replacement", Some(18));
        let (persisted, _) = persist_exact_connector_revision(&revision).unwrap();
        let (foreign_persisted, _) = persist_exact_connector_revision(&replacement).unwrap();
        let mismatch =
            finish_pin_occurrence(9, &persisted, base_ref(), observed.clone(), replacement)
                .unwrap_err();
        assert!(mismatch.contains("observation and exact revision identify different objects"));
        let changed = finish_pin_occurrence(9, &foreign_persisted, base_ref(), observed, revision)
            .unwrap_err();
        assert!(changed.contains("source object changed since D was persisted"));
        let (empty, empty_revision) = observation(b"original", None);
        let missing =
            finish_pin_occurrence(9, &persisted, base_ref(), empty, empty_revision).unwrap_err();
        assert!(missing.contains("has no current snapshot"));
        for message in [mismatch, changed, missing] {
            let error = PreparationError::from(message.clone()).0;
            assert_eq!(error.kind(), MvApplicationErrorKind::InvalidRequest);
            assert_eq!(error.message(), message);
            assert_eq!(error.original_failure(), None);
            assert_eq!(error.compile_control_error(), None);
        }
    }
}
