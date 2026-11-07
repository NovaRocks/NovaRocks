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

use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorDocumentManagementAdmissionRequest, ConnectorDocumentManagementOperation,
    ConnectorDocumentStorageManagement, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ProviderBindingEpoch,
};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct IcebergDocumentStorage {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    runtime: Arc<crate::metadata_context::IcebergMetadataContext>,
}

impl IcebergDocumentStorage {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        runtime: Arc<crate::metadata_context::IcebergMetadataContext>,
    ) -> Self {
        Self {
            descriptor,
            incarnation,
            runtime,
        }
    }

    pub(crate) fn runtime(&self) -> &Arc<crate::metadata_context::IcebergMetadataContext> {
        &self.runtime
    }

    pub(crate) fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub(crate) const fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionTokenV1 {
    version: u16,
    operation: String,
    operation_id: [u8; 16],
    namespace: String,
    table: String,
    expected_object_id: Option<Vec<u8>>,
}

impl ConnectorDocumentStorageManagement for IcebergDocumentStorage {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    fn admit_management(
        &self,
        request: ConnectorDocumentManagementAdmissionRequest,
    ) -> Result<Bytes, ConnectorError> {
        super::context::check_context(request.context())?;
        use crate::catalog::admission::{
            CatalogAdmissionRequest, CatalogOperation, connector_unsupported,
        };
        let operation = match request.operation() {
            ConnectorDocumentManagementOperation::Create => CatalogOperation::CreateDocuments,
            ConnectorDocumentManagementOperation::SingleTargetUpdate => {
                CatalogOperation::UpdateDocuments
            }
            ConnectorDocumentManagementOperation::Publication => CatalogOperation::PublishDocuments,
            ConnectorDocumentManagementOperation::Drop => CatalogOperation::DropDocuments,
        };
        self.runtime
            .novarocks_catalog()
            .admit(&CatalogAdmissionRequest::new(
                operation,
                crate::catalog::CatalogTableName::new(
                    Arc::clone(&request.target().namespace),
                    Arc::clone(&request.target().table),
                ),
                request.context().initiation(),
            ))
            .map_err(connector_unsupported)?;
        let operation = match request.operation() {
            ConnectorDocumentManagementOperation::Create => "create",
            ConnectorDocumentManagementOperation::SingleTargetUpdate => "single-target-update",
            ConnectorDocumentManagementOperation::Publication => "publication",
            ConnectorDocumentManagementOperation::Drop => "drop",
        };
        let token = AdmissionTokenV1 {
            version: 1,
            operation: operation.to_string(),
            operation_id: request.operation_id().to_bytes(),
            namespace: request.target().namespace.to_string(),
            table: request.target().table.to_string(),
            expected_object_id: request
                .expected_object_id()
                .map(|object| object.as_bytes().to_vec()),
        };
        serde_json::to_vec(&token)
            .map(Bytes::from)
            .map_err(|error| internal(format!("encode Iceberg document admission: {error}")))
    }
}

pub(crate) fn validate_management_admission(
    admission: &novarocks_spi::connector::ConnectorDocumentManagementAdmission,
) -> Result<(), ConnectorError> {
    let token: AdmissionTokenV1 =
        serde_json::from_slice(admission.provider_token()).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode Iceberg document admission: {error}"),
            )
        })?;
    validate_admission_token(&token, admission)
}

fn validate_admission_token(
    token: &AdmissionTokenV1,
    admission: &novarocks_spi::connector::ConnectorDocumentManagementAdmission,
) -> Result<(), ConnectorError> {
    let operation = match admission.operation() {
        ConnectorDocumentManagementOperation::Create => "create",
        ConnectorDocumentManagementOperation::SingleTargetUpdate => "single-target-update",
        ConnectorDocumentManagementOperation::Publication => "publication",
        ConnectorDocumentManagementOperation::Drop => "drop",
    };
    if token.version != 1
        || token.operation != operation
        || token.operation_id != admission.operation_id().to_bytes()
        || token.namespace != admission.target().namespace.as_ref()
        || token.table != admission.target().table.as_ref()
        || token.expected_object_id.as_deref()
            != admission
                .expected_object_id()
                .map(|object| object.as_bytes().as_ref())
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg document admission token does not match the exact operation",
        ));
    }
    Ok(())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorDocumentManagementAdmissionRequest,
        ConnectorDocumentStorageManagement, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderBindingKey, ConnectorProviderId, ConnectorRequestContext,
        ConnectorTableIdentity, ConnectorTableObjectId, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::super::admission_support::{AdmissionCatalogSpy, AdmissionFileIoSpy};
    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog::CatalogCreateIntent;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;

    #[test]
    fn native_hadoop_rejects_every_document_management_operation_before_catalog_io() {
        assert_native_management_refused_before_io("hadoop");
    }

    #[test]
    fn native_hive_rejects_every_document_management_operation_before_catalog_io() {
        assert_native_management_refused_before_io("hive");
    }

    fn assert_native_management_refused_before_io(catalog_kind: &str) {
        let tokio = tokio::runtime::Runtime::new().unwrap();
        let handle = tokio.handle().clone();
        let warehouse = tempfile::tempdir().unwrap();
        // No metastore is needed: the native catalog must refuse document
        // management without contacting this reserved loopback endpoint.
        let metastore = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        metastore.set_nonblocking(true).unwrap();
        let mut properties = vec![
            ("iceberg.catalog.type".to_string(), catalog_kind.to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            ),
        ];
        if catalog_kind == "hive" {
            properties.push((
                "hive.metastore.uris".to_string(),
                format!("thrift://{}", metastore.local_addr().unwrap()),
            ));
        }
        let configuration =
            crate::catalog_config::parse_catalog_configuration("ice", &properties).unwrap();
        let file_io = Arc::new(AdmissionFileIoSpy::default());
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            file_io.clone(),
            novarocks_fs::TokioRefreshExecutor::shared(handle.clone()),
            file_io.clone(),
        );
        let state = IcebergCatalogControlState::new(configuration);
        let resources = IcebergMetadataResources::new(binding, handle);
        let native_runtime = crate::metadata_context::IcebergMetadataContext::try_new(
            state.clone(),
            resources.clone(),
        )
        .unwrap();
        let empty_admission = native_runtime.novarocks_catalog().admit(
            &crate::catalog::admission::CatalogAdmissionRequest::statement(
                crate::catalog::admission::CatalogOperation::CreateTable(
                    CatalogCreateIntent::EmptyTable,
                ),
                crate::catalog::CatalogTableName::new("db", "t"),
            ),
        );
        assert_eq!(empty_admission.is_ok(), catalog_kind == "hadoop");
        assert!(
            native_runtime
                .novarocks_catalog()
                .admit(
                    &crate::catalog::admission::CatalogAdmissionRequest::statement(
                        crate::catalog::admission::CatalogOperation::CreateTable(
                            CatalogCreateIntent::CreateTableAsSelect
                        ),
                        crate::catalog::CatalogTableName::new("db", "t"),
                    )
                )
                .is_err()
        );
        let catalog = Arc::new(AdmissionCatalogSpy::new(Arc::clone(
            native_runtime.novarocks_catalog(),
        )));
        let runtime = Arc::new(
            crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
                state,
                resources,
                catalog.clone(),
            ),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
        };
        let epoch = ProviderBindingEpoch::new();
        let storage = IcebergDocumentStorage::new(descriptor.clone(), epoch, runtime);
        let owner = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: epoch,
        };
        let catalog_handle = CatalogHandle::new(
            descriptor.instance_id.clone(),
            CatalogVersion::from_bytes([3; 32]),
        );
        let target = ConnectorTableIdentity {
            instance_id: descriptor.instance_id,
            namespace: Arc::from("db"),
            table: Arc::from("mv"),
        };
        let context = || {
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .unwrap()
        };
        for (operation, object_id) in [
            (ConnectorDocumentManagementOperation::Create, None),
            (
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                Some(ConnectorTableObjectId::try_new(Bytes::from_static(b"uuid")).unwrap()),
            ),
            (
                ConnectorDocumentManagementOperation::Publication,
                Some(ConnectorTableObjectId::try_new(Bytes::from_static(b"uuid")).unwrap()),
            ),
            (
                ConnectorDocumentManagementOperation::Drop,
                Some(ConnectorTableObjectId::try_new(Bytes::from_static(b"uuid")).unwrap()),
            ),
        ] {
            let request = ConnectorDocumentManagementAdmissionRequest::try_new(
                owner.clone(),
                catalog_handle.clone(),
                ConnectorMutationOperationId::new(),
                target.clone(),
                object_id,
                operation,
                context(),
            )
            .unwrap();
            let error = storage.admit_management(request).unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
            catalog.assert_no_io();
            file_io.assert_no_dispatch();
            assert!(warehouse.path().read_dir().unwrap().next().is_none());
            assert_eq!(
                metastore.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock,
                "document admission must not contact the native metastore",
            );
        }
    }
}
