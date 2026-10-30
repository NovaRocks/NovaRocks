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

//! Publication preparation has no dispatch state. The journal consumes the
//! resulting mechanism and is the sole production owner of its verdict.

use super::*;
use crate::catalog::publication::CatalogEvidencePolicy;
use crate::catalog::publication_dispatch::{
    PreparationFailure, PreparationResult, PreparedPublication,
};
use crate::commit::publication_descriptor::PublicationDescriptor;
use novarocks_spi::connector::{
    ConnectorOperationControl, ConnectorRequestContext, ProviderBindingEpoch,
};
use std::sync::Arc;

#[async_trait]
pub(crate) trait PublicationPreparer: Send + Sync + 'static {
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase>;
    async fn load_target_with_snapshots(
        &self,
        attempt: &IcebergCommitAttempt,
        mode: crate::catalog::SnapshotLoading,
    ) -> Result<StagingBase> {
        if mode != crate::catalog::SnapshotLoading::All {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication preparer has no refs-scoped metadata load",
            ));
        }
        self.load_target(attempt).await
    }
    fn pinned_expiry_base(&self) -> Option<StagingBase> {
        None
    }
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()>;
    /// Borrow the original request slot. A scoped producer may move it only
    /// into its stable original Source while the same CPU receipt is awaited.
    /// Other existing families keep their original pure preflight author until
    /// their P06 interface retirement; no network or refused CPU fallback.
    fn preflight_recovery_in<'a>(
        &'a self,
        request: &'a mut Option<crate::commit::model::FrozenRequest>,
        operation: &'a IcebergCommitOperation,
    ) -> crate::commit::attempt::RecoveryPreflightFuture<'a> {
        crate::commit::attempt::RecoveryPreflightFuture::ready(
            request
                .as_ref()
                .ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Original frozen request is absent")
                })
                .and_then(|request| self.preflight_recovery(request, operation)),
        )
    }
    fn evidence_policy(&self) -> CatalogEvidencePolicy;
    async fn prepare_dispatch(
        self: Arc<Self>,
        request: FrozenRequest,
        descriptor: PublicationDescriptor,
        generation: ProviderBindingEpoch,
        preparation: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication>;
}

pub(crate) struct CatalogPublisher {
    pub catalog_operation: crate::catalog::admission::CatalogOperation,
    pub catalog: Arc<dyn crate::catalog::NovaRocksCatalog>,
    pub ident: crate::iceberg::TableIdent,
    pub target_ref: String,
    pub operation: super::super::model::OperationToken,
    pub recovery_preflight: RecoveryPreflight,
}
impl CatalogPublisher {
    fn admit(&self) -> PreparationResult<()> {
        self.catalog
            .admit_operation(
                &self.catalog_operation,
                &crate::catalog::CatalogTableName::from_identifier(&self.ident).into(),
            )
            .map_err(|error| {
                PreparationFailure(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unsupported,
                    error.message(),
                ))
            })
    }
}
#[async_trait]
impl PublicationPreparer for CatalogPublisher {
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
        self.load_target_with_snapshots(attempt, crate::catalog::SnapshotLoading::All)
            .await
    }
    async fn load_target_with_snapshots(
        &self,
        attempt: &IcebergCommitAttempt,
        mode: crate::catalog::SnapshotLoading,
    ) -> Result<StagingBase> {
        attempt.check_active()?;
        self.admit()
            .map_err(|failure| Error::new(ErrorKind::FeatureUnsupported, failure.0.message()))?;
        let loaded = self
            .catalog
            .load_commit_base_for_request(
                crate::catalog::CatalogTableName::from_identifier(&self.ident),
                attempt.file_io().clone(),
                mode,
                attempt.read_binding(),
            )
            .await;
        attempt.check_active()?;
        let base = loaded?;
        Ok(StagingBase::Existing {
            metadata: base.metadata,
            metadata_location: base.metadata_location,
        })
    }
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()> {
        if operation.token() != self.operation
            || request.artifacts().attempt().operation() != self.operation
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Frozen publication differs from its admitted operation",
            ));
        }
        (self.recovery_preflight)(request, operation)
    }
    fn evidence_policy(&self) -> CatalogEvidencePolicy {
        self.catalog.evidence_policy()
    }
    async fn prepare_dispatch(
        self: Arc<Self>,
        request: FrozenRequest,
        descriptor: PublicationDescriptor,
        generation: ProviderBindingEpoch,
        preparation: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        preparation.check_active()?;
        self.admit()?;
        if request.identifier() != &self.ident
            || request.target_ref() != self.target_ref
            || request.artifacts().attempt().operation() != self.operation
            || descriptor.operation_id != self.operation.to_bytes()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Frozen publication differs from its exact admitted operation or target",
            )
            .into());
        }
        descriptor.validate_frozen(&request)?;
        self.catalog
            .prepare_frozen_publication(
                request,
                self.catalog_operation,
                None,
                descriptor,
                generation,
                preparation,
            )
            .await
    }
}

/// Expiry freezes the full observation used to select snapshots and documents.
/// Its dispatch still belongs to the ordinary prepared publication and journal.
pub(crate) struct PinnedExpiryPreparer {
    pub inner: Arc<dyn PublicationPreparer>,
    pub base: StagingBase,
}

#[async_trait]
impl PublicationPreparer for PinnedExpiryPreparer {
    async fn load_target(&self, _attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
        Err(Error::new(
            ErrorKind::DataInvalid,
            "Pinned expiry cannot reload its base",
        ))
    }
    fn pinned_expiry_base(&self) -> Option<StagingBase> {
        Some(self.base.clone())
    }
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()> {
        self.inner.preflight_recovery(request, operation)
    }
    fn preflight_recovery_in<'a>(
        &'a self,
        request: &'a mut Option<crate::commit::model::FrozenRequest>,
        operation: &'a IcebergCommitOperation,
    ) -> crate::commit::attempt::RecoveryPreflightFuture<'a> {
        self.inner.preflight_recovery_in(request, operation)
    }
    fn evidence_policy(&self) -> CatalogEvidencePolicy {
        self.inner.evidence_policy()
    }
    async fn prepare_dispatch(
        self: Arc<Self>,
        request: FrozenRequest,
        descriptor: PublicationDescriptor,
        generation: ProviderBindingEpoch,
        preparation: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        Arc::clone(&self.inner)
            .prepare_dispatch(request, descriptor, generation, preparation)
            .await
    }
}
