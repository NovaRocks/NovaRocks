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

//! Mint a real document management admission for tests.
//!
//! An admission can only come from a provider, so these tests stand up a
//! minimal one rather than fabricating the value. That keeps the admission's
//! own invariants — owner, catalog generation, operation and exact object —
//! under test instead of bypassed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentManagementAdmission, ConnectorDocumentManagementAdmissionRequest,
    ConnectorDocumentManagementOperation, ConnectorDocumentStorageBinding,
    ConnectorDocumentStorageManagement,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorError, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorMutationOperationId, ConnectorProviderId, ConnectorRequestContext,
    ConnectorTableIdentity, ConnectorTableObjectId, LakePublicationId, ProviderBindingEpoch,
};

pub(super) fn catalog_properties(
    instance_id: &ConnectorInstanceId,
) -> novarocks_spi::connector::CatalogProperties {
    novarocks_spi::connector::CatalogProperties::new(
        novarocks_spi::connector::CatalogHandle::new(
            instance_id.clone(),
            novarocks_spi::connector::CatalogVersion::from_bytes([3; 32]),
        ),
        ConnectorProviderId::parse("iceberg").expect("test provider"),
        1,
        Vec::new(),
        Vec::new(),
    )
    .expect("test catalog properties")
}

pub(super) struct AdmittingManagement {
    pub(super) descriptor: ConnectorInstanceDescriptor,
    pub(super) incarnation: ProviderBindingEpoch,
}

impl ConnectorDocumentStorageManagement for AdmittingManagement {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    fn admit_management(
        &self,
        _request: ConnectorDocumentManagementAdmissionRequest,
    ) -> Result<Bytes, ConnectorError> {
        Ok(Bytes::from_static(b"test-admission-token"))
    }
}

/// A publication admission for `target`, pinned to `publication_id` and the
/// exact object the publication will commit against.
pub(crate) fn publication_admission(
    catalog: &str,
    namespace: &str,
    table: &str,
    publication_id: LakePublicationId,
    object_id: &ConnectorTableObjectId,
) -> ConnectorDocumentManagementAdmission {
    let instance_id = ConnectorInstanceId::parse(catalog).expect("test instance ID");
    let incarnation = ProviderBindingEpoch::from_bytes([8; 16]);
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").expect("test provider"),
        instance_id: instance_id.clone(),
    };
    let documents = ConnectorDocumentStorageBinding::try_new(
        descriptor.clone(),
        incarnation,
        None,
        Some(Arc::new(AdmittingManagement {
            descriptor,
            incarnation,
        })),
    )
    .expect("test document storage binding");
    let binding = novarocks_catalog_application::test_support::test_control_binding_for(
        instance_id.clone(),
        8,
    )
    .with_catalog_properties(catalog_properties(&instance_id))
    .and_then(|binding| binding.try_with_document_storage(Some(documents)))
    .expect("test control binding");
    let lease = ConnectorControlPlanningPin::new(Arc::new(binding), || {})
        .derive_document_storage_lease()
        .expect("test document storage lease");
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        4096,
        64 * 1024,
    )
    .expect("test request context");
    lease
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                lease.owner().clone(),
                lease.catalog_handle().clone(),
                ConnectorMutationOperationId::from_bytes(publication_id.to_bytes()),
                ConnectorTableIdentity {
                    instance_id,
                    namespace: Arc::from(namespace),
                    table: Arc::from(table),
                },
                Some(object_id.clone()),
                ConnectorDocumentManagementOperation::Publication,
                context,
            )
            .expect("test admission request"),
        )
        .expect("test publication admission")
}

pub(crate) use novarocks_mv_application::persistence::test_support::{
    KeyedProjectionFixture, sample_keyed_projection,
};

pub(crate) async fn keyed_repository()
-> Arc<novarocks_mv_application::state_store_repository::StateStoreMvRepository> {
    novarocks_mv_application::state_store_repository::StateStoreMvRepository::open(
        Arc::new(novarocks_state_store_testkit::testing::InMemoryStateStore::new("fe-keyed-tests")),
        Default::default(),
    )
    .await
    .expect("test keyed repository")
}
pub(crate) fn management_entrance() -> Arc<novarocks_mv_application::management::ManagementEntrance>
{
    Arc::new(
        novarocks_mv_application::management::ManagementEntrance::new(
            novarocks_mv_application::management::DeploymentOwner::parse("test-deployment")
                .unwrap(),
            novarocks_mv_application::management::ProcessIncarnation::parse("test-process")
                .unwrap(),
        ),
    )
}
pub(crate) async fn keyed_product() -> (
    novarocks_mv_application::service::MvKeyedProductService,
    Arc<novarocks_mv_application::state_store_repository::StateStoreMvRepository>,
) {
    let repository = keyed_repository().await;
    let product=novarocks_mv_application::service::MvKeyedProductService::new_with_keyed_management_continuation(Default::default(),repository.clone(),Default::default(),management_entrance(),Default::default());
    (product, repository)
}

/// Each test gets an independent MV state owner; only the Tokio executor is shared.
pub(crate) fn test_readiness() -> Arc<crate::mv::domain::readiness::MvReadinessPort> {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test MV executor")
    });
    let (product, _) = std::thread::scope(|scope| {
        scope
            .spawn(|| runtime.block_on(keyed_product()))
            .join()
            .expect("construct test MV owner")
    });
    Arc::new(crate::mv::domain::readiness::MvReadinessPort::from_product(
        product.keyed_readiness_service().unwrap(),
        runtime.handle().clone(),
        product.management_entrance().unwrap(),
    ))
}
