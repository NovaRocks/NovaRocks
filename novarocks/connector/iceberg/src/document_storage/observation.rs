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

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorDocumentDiscoveryPage, ConnectorDocumentDiscoveryRequest,
    ConnectorDocumentStorageObservation, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorTableObjectId, ProviderBindingEpoch,
};
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CommittedVersionV1 {
    version: u16,
    #[expect(
        dead_code,
        reason = "The versioned observation payload keeps the table UUID as an identity fact for downstream decoders."
    )]
    table_uuid: String,
    metadata_location: Option<String>,
    #[expect(
        dead_code,
        reason = "The versioned observation payload keeps the metadata update time as a committed-version fact."
    )]
    last_updated_ms: i64,
    current_snapshot_id: Option<i64>,
}

pub(crate) fn table_object_id(
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<ConnectorTableObjectId, ConnectorError> {
    ConnectorTableObjectId::try_new(Bytes::from(metadata.uuid().to_string()))
}

pub(crate) fn committed_version(
    table: &crate::iceberg::table::Table,
) -> Result<ConnectorCommittedVersion, ConnectorError> {
    committed_version_from_metadata(table.metadata(), table.metadata_location())
}

/// The metadata and catalog pointer must come from one frozen provider read.
pub(crate) fn committed_version_from_metadata(
    metadata: &crate::iceberg::spec::TableMetadata,
    metadata_location: Option<&str>,
) -> Result<ConnectorCommittedVersion, ConnectorError> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "table_uuid": metadata.uuid().to_string(),
        "metadata_location": metadata_location,
        "last_updated_ms": metadata.last_updated_ms(),
        "current_snapshot_id": metadata.current_snapshot_id(),
    }))
    .map(Bytes::from)
    .map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("encode Iceberg committed metadata version: {error}"),
        )
    })?;
    ConnectorCommittedVersion::try_new(payload, metadata.current_snapshot_id())
}

pub(crate) fn validate_expected_object(
    expected: &ConnectorTableObjectId,
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<(), ConnectorError> {
    if &table_object_id(metadata)? != expected {
        return Err(ConnectorError::table_object_binding(
            novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced,
            "Iceberg table name now resolves to a different table UUID",
        ));
    }
    Ok(())
}

impl ConnectorDocumentStorageObservation for super::create::IcebergDocumentStorage {
    fn observe_keyed_documents(
        &self,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<
        novarocks_spi::connector::KeyedDocumentView,
        novarocks_spi::connector::KeyedDocumentReadError,
    > {
        let table = load_keyed_table(self, &request, crate::catalog::SnapshotLoading::Refs)?;
        super::keyed::project_view(table.metadata(), &request, committed_version(&table)?)
    }

    fn observe_keyed_documents_async(
        &self,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        novarocks_spi::connector::KeyedDocumentView,
                        novarocks_spi::connector::KeyedDocumentReadError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let table =
                load_keyed_table_async(self, &request, crate::catalog::SnapshotLoading::Refs)
                    .await?;
            super::keyed::project_view(table.metadata(), &request, committed_version(&table)?)
        })
    }

    fn observe_full_keyed_documents(
        &self,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<
        novarocks_spi::connector::FullKeyedDocumentView,
        novarocks_spi::connector::KeyedDocumentReadError,
    > {
        let table = load_keyed_table(self, &request, crate::catalog::SnapshotLoading::All)?;
        super::keyed::project_full_view(table.metadata(), &request, committed_version(&table)?)
    }

    fn observe_full_keyed_documents_async(
        &self,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        novarocks_spi::connector::FullKeyedDocumentView,
                        novarocks_spi::connector::KeyedDocumentReadError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let table =
                load_keyed_table_async(self, &request, crate::catalog::SnapshotLoading::All)
                    .await?;
            super::keyed::project_full_view(table.metadata(), &request, committed_version(&table)?)
        })
    }

    fn decode_keyed_slot_change_evidence(
        &self,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
        evidence: novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<novarocks_spi::connector::UnknownCommitEvidence, ConnectorError> {
        decode_keyed_slot_evidence(self, &request, &evidence)
    }

    fn resolve_keyed_commit(
        &self,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
        evidence: novarocks_spi::connector::UnknownCommitEvidence,
    ) -> Result<novarocks_spi::connector::CommitConclusion, ConnectorError> {
        use super::keyed::resolution::{self, Resolution};
        let refs = self
            .observe_keyed_documents(request.clone())
            .map_err(keyed_read_error)?;
        match resolution::resolve(&refs, None, &evidence)? {
            Resolution::Conclusion(conclusion) => Ok(conclusion),
            Resolution::NeedFull => {
                let full = self
                    .observe_full_keyed_documents(request)
                    .map_err(keyed_read_error)?;
                match resolution::resolve(&refs, Some(&full), &evidence)? {
                    Resolution::Conclusion(conclusion) => Ok(conclusion),
                    Resolution::NeedFull => Err(ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        "complete keyed commit observation requested another full load",
                    )),
                }
            }
        }
    }

    fn resolve_keyed_commit_async(
        &self,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
        evidence: novarocks_spi::connector::UnknownCommitEvidence,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<novarocks_spi::connector::CommitConclusion, ConnectorError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            use super::keyed::resolution::{self, Resolution};
            let refs = self
                .observe_keyed_documents_async(request.clone())
                .await
                .map_err(keyed_read_error)?;
            match resolution::resolve(&refs, None, &evidence)? {
                Resolution::Conclusion(conclusion) => Ok(conclusion),
                Resolution::NeedFull => {
                    let full = self
                        .observe_full_keyed_documents_async(request)
                        .await
                        .map_err(keyed_read_error)?;
                    match resolution::resolve(&refs, Some(&full), &evidence)? {
                        Resolution::Conclusion(conclusion) => Ok(conclusion),
                        Resolution::NeedFull => Err(ConnectorError::new(
                            ConnectorErrorKind::Internal,
                            "complete keyed commit observation requested another full load",
                        )),
                    }
                }
            }
        })
    }

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation()
    }

    fn discover_documents(
        &self,
        request: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        super::discovery::discover(self, &request)
    }
}

fn decode_keyed_slot_evidence(
    storage: &super::create::IcebergDocumentStorage,
    request: &novarocks_spi::connector::KeyedDocumentObservationRequest,
    evidence: &novarocks_spi::connector::ExternalMutationEvidence,
) -> Result<novarocks_spi::connector::UnknownCommitEvidence, ConnectorError> {
    use novarocks_spi::connector::{
        ConnectorSlotName, UnknownCommitEvidence, UnknownCommitIdentity,
    };
    request.validate()?;
    let invalid = |message| ConnectorError::new(ConnectorErrorKind::InvalidRequest, message);
    if evidence.descriptor() != storage.descriptor()
        || evidence.incarnation() != storage.incarnation()
        || request.owner.instance_id != storage.descriptor().instance_id
        || request.owner.incarnation != storage.incarnation()
        || evidence.schema_version() != crate::reconcile_payload::ICEBERG_MUTATION_EVIDENCE_VERSION
        || evidence.operation_kind() != "keyed-documents"
    {
        return Err(invalid(
            "slot evidence belongs to a different provider or operation kind",
        ));
    }
    let payload =
        crate::reconcile_payload::decode_mutation_evidence(evidence.provider_payload())
            .map_err(|message| ConnectorError::new(ConnectorErrorKind::CorruptData, message))?;
    let crate::reconcile_payload::IcebergMutationEvidenceTarget::KeyedSlotChange {
        namespace,
        table,
        table_uuid,
        operation_id,
        asserted_parent,
        preselected_snapshot,
        slots,
    } = payload.target
    else {
        return Err(invalid("evidence does not describe a slot change"));
    };
    if namespace != request.target.namespace.as_ref()
        || table != request.target.table.as_ref()
        || table_uuid.as_bytes() != request.object_id.as_bytes().as_ref()
        || operation_id != evidence.operation_id().to_bytes()
        || slots.is_empty()
    {
        return Err(invalid(
            "slot evidence does not match its exact table object and operation",
        ));
    }
    let slots: Vec<_> = slots
        .into_iter()
        .map(|(owner, name)| ConnectorSlotName::try_new(owner, name))
        .collect::<Result<_, _>>()?;
    if slots.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("slot evidence names are not strictly sorted"));
    }
    Ok(UnknownCommitEvidence {
        asserted_parent,
        preselected_snapshot,
        identity: UnknownCommitIdentity::SlotWriter {
            slot: slots[0].clone(),
        },
    })
}

pub(crate) fn metadata_location(
    version: &ConnectorCommittedVersion,
) -> Result<String, ConnectorError> {
    let decoded: CommittedVersionV1 =
        serde_json::from_slice(version.payload()).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode keyed observation version: {error}"),
            )
        })?;
    if decoded.version != 1 || decoded.current_snapshot_id != version.snapshot_id() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "invalid keyed observation version",
        ));
    }
    decoded.metadata_location.ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "full document observation has no metadata location",
        )
    })
}

pub(crate) fn keyed_read_error(
    error: novarocks_spi::connector::KeyedDocumentReadError,
) -> ConnectorError {
    match error {
        novarocks_spi::connector::KeyedDocumentReadError::Invalid(error) => error,
        legacy @ novarocks_spi::connector::KeyedDocumentReadError::LegacyFormat { .. } => {
            ConnectorError::new(ConnectorErrorKind::Unsupported, legacy.to_string())
        }
    }
}

fn load_keyed_table(
    storage: &super::create::IcebergDocumentStorage,
    request: &novarocks_spi::connector::KeyedDocumentObservationRequest,
    mode: crate::catalog::SnapshotLoading,
) -> Result<crate::iceberg::table::Table, ConnectorError> {
    request.validate()?;
    if request.owner.instance_id != storage.descriptor().instance_id
        || request.owner.incarnation != storage.incarnation()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "keyed observation belongs to a different provider generation",
        ));
    }
    super::context::check_context(&request.context)?;
    // Both scopes are fresh observations. A cached All load cannot adjudicate
    // an unknown commit, and a partial load must never enter that cache.
    let table = storage
        .runtime()
        .observe_table_classified_for_request_with_snapshots(
            &request.target.namespace,
            &request.target.table,
            &request.context,
            mode,
        )
        .map_err(|(kind, message)| ConnectorError::new(kind, message))?
        .into_table();
    validate_expected_object(&request.object_id, table.metadata())?;
    Ok(table)
}

async fn load_keyed_table_async(
    storage: &super::create::IcebergDocumentStorage,
    request: &novarocks_spi::connector::KeyedDocumentObservationRequest,
    mode: crate::catalog::SnapshotLoading,
) -> Result<crate::iceberg::table::Table, ConnectorError> {
    request.validate()?;
    if request.owner.instance_id != storage.descriptor().instance_id
        || request.owner.incarnation != storage.incarnation()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "keyed observation belongs to a different provider generation",
        ));
    }
    super::context::check_context(&request.context)?;
    // Both scopes are fresh observations. A cached All load cannot adjudicate
    // an unknown commit, and a partial load must never enter that cache.
    let table = storage
        .runtime()
        .observe_table_classified_for_request_with_snapshots_async(
            &request.target.namespace,
            &request.target.table,
            &request.context,
            mode,
        )
        .await
        .map_err(|(kind, message)| ConnectorError::new(kind, message))?
        .into_table();
    validate_expected_object(&request.object_id, table.metadata())?;
    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorTableObjectBindingFailure;
    use std::collections::HashMap;
    fn metadata(properties: HashMap<String, String>) -> crate::iceberg::spec::TableMetadata {
        let schema = crate::iceberg::spec::Schema::builder()
            .with_fields(vec![std::sync::Arc::new(
                crate::iceberg::spec::NestedField::required(
                    1,
                    "id",
                    crate::iceberg::spec::Type::Primitive(
                        crate::iceberg::spec::PrimitiveType::Long,
                    ),
                ),
            )])
            .build()
            .unwrap();
        crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://warehouse/table".to_string(),
            crate::iceberg::spec::FormatVersion::V2,
            properties,
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn exact_object_validation_reports_a_typed_replacement() {
        let metadata = metadata(HashMap::new());
        let wrong = ConnectorTableObjectId::try_new(Bytes::from_static(b"replaced-table")).unwrap();
        let error = validate_expected_object(&wrong, &metadata).unwrap_err();
        assert_eq!(
            error.table_object_binding_failure(),
            Some(ConnectorTableObjectBindingFailure::Replaced)
        );
    }
}
