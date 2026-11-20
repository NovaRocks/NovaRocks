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

//! Exact provider lease and governed source for explicit orphan cleanup.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use uuid::Uuid;

use super::{
    ConnectorControlRuntimeId, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorMaintenanceRetention, ConnectorMetadata,
    ConnectorProviderBindingKey, ConnectorRequestContext, LakePublicationDisposition,
    ProviderBindingEpoch,
};

/// Process-local statement identity selected by the frontend operation owner.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorCleanupOperationId(Uuid);

impl ConnectorCleanupOperationId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl Default for ConnectorCleanupOperationId {
    fn default() -> Self {
        Self::new()
    }
}

pub trait ConnectorCleanupMaintenance: Send + Sync {
    /// Exact concrete author of this selected provider generation; no CPU grant.
    fn metadata_cpu_source_type_id(&self) -> Result<std::any::TypeId, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "cleanup has no governed metadata CPU author",
        ))
    }
    /// Silent construction only. The original application installs this source
    /// before polling; dropping a borrowed wait never drops its physical F.
    fn begin_async_in(
        &self,
        _start: &mut Option<ConnectorCleanupAsyncStart>,
        _metadata: Arc<dyn ConnectorMetadata>,
        _cpu: Arc<dyn super::ProviderMetadataCpuPort>,
        _target: &mut Option<ConnectorCleanupAsyncExecution>,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "provider has no governed cleanup async source",
        ))
    }
    /// Initial routes selected by THIS original cleanup factory. This is
    /// neither a byte allowance nor an I/O/concurrency grant.
    fn initial_dependencies(&self) -> Result<Vec<super::SharedDependency>, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "cleanup factory has no original selected route facts",
        ))
    }

    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn binding_key(&self) -> &ConnectorProviderBindingKey;
}

pub(crate) fn validate_cleanup_maintenance_owner(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorProviderBindingKey,
    cleanup: &dyn ConnectorCleanupMaintenance,
) -> Result<(), ConnectorError> {
    if cleanup.descriptor() != descriptor || cleanup.binding_key() != key {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "cleanup capability owner does not match control binding",
        ));
    }
    Ok(())
}

pub trait ConnectorCleanupMaintenanceResolver: Send + Sync {
    fn acquire_current_cleanup_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCleanupMaintenanceSession, ConnectorError>;
    fn acquire_exact_cleanup_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorCleanupMaintenanceSession, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorCleanupMaintenanceSession {
    descriptor: ConnectorInstanceDescriptor,
    control_runtime_id: ConnectorControlRuntimeId,
    provider_binding_key: ConnectorProviderBindingKey,
    metadata: Arc<dyn ConnectorMetadata>,
    cleanup: Arc<dyn ConnectorCleanupMaintenance>,
    _release: Arc<CleanupRelease>,
}
struct CleanupRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorCleanupMaintenanceSession {
    pub fn metadata_cpu_source_type_id(&self) -> Result<std::any::TypeId, ConnectorError> {
        let selected = self.cleanup.metadata_cpu_source_type_id()?;
        let (key, actual) = self
            .metadata
            .provider_metadata_cpu_author()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "selected cleanup metadata CPU author is unsupported",
                )
            })?;
        if key != self.provider_binding_key || actual != selected {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup metadata CPU author belongs to another exact generation",
            ));
        }
        Ok(selected)
    }
    pub const fn incarnation(&self) -> ProviderBindingEpoch {
        self.provider_binding_key.incarnation
    }
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        control_runtime_id: ConnectorControlRuntimeId,
        provider_incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        cleanup: Arc<dyn ConnectorCleanupMaintenance>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let provider_binding_key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: provider_incarnation,
        };
        if metadata.instance_id() != &descriptor.instance_id
            || cleanup.descriptor() != &descriptor
            || cleanup.binding_key() != &provider_binding_key
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup capabilities do not match lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            control_runtime_id,
            provider_binding_key,
            metadata,
            cleanup,
            _release: Arc::new(CleanupRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }
    pub fn initial_dependencies(&self) -> Result<Vec<super::SharedDependency>, ConnectorError> {
        if self.cleanup.descriptor() != &self.descriptor
            || self.cleanup.binding_key() != &self.provider_binding_key
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup route author belongs to another exact generation",
            ));
        }
        self.cleanup.initial_dependencies()
    }
    pub fn begin_async_in(
        &self,
        start: &mut Option<ConnectorCleanupAsyncStart>,
        cpu: Arc<dyn super::ProviderMetadataCpuPort>,
        target: &mut Option<ConnectorCleanupAsyncExecution>,
    ) -> Result<(), ConnectorError> {
        let input = start.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup original start is absent",
            )
        })?;
        if target.is_some() || input.table.instance_id != self.descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup original receiving or exact owner differs",
            ));
        }
        let operation = input.operation_id;
        self.cleanup
            .begin_async_in(start, self.metadata.clone(), cpu, target)?;
        let execution = target.as_mut().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "cleanup provider did not install its original source",
            )
        })?;
        execution.expected = Some((self.provider_binding_key.clone(), operation));
        Ok(())
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub fn metadata(&self) -> &Arc<dyn ConnectorMetadata> {
        &self.metadata
    }

    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }
}

impl fmt::Debug for ConnectorCleanupMaintenanceSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorCleanupMaintenanceSession")
            .field("descriptor", &self.descriptor)
            .field("control_runtime_id", &self.control_runtime_id)
            .finish_non_exhaustive()
    }
}

impl Drop for CleanupRelease {
    fn drop(&mut self) {
        if let Some(release) = self
            .release
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            release();
        }
    }
}

/// Frozen request from the statement's original authority.
pub struct ConnectorCleanupAsyncStart {
    pub original: super::ConnectorOriginalResultScope,
    pub table: super::ConnectorTableIdentity,
    pub operation_id: ConnectorCleanupOperationId,
    pub older_than_ms: i64,
    pub retention: ConnectorMaintenanceRetention,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorOrphanCleanupMetrics {
    pub processed_manifests: u64,
    pub active_files: u64,
    pub scanned_files: u64,
    pub deleted_files: u64,
    pub deleted_bytes: u64,
}

pub struct ConnectorCleanupAsyncOutput {
    pub owner: ConnectorProviderBindingKey,
    pub operation_id: ConnectorCleanupOperationId,
    pub metrics: Option<ConnectorOrphanCleanupMetrics>,
}

/// The provider source owns the physical future, terminal facts and actual exit.
pub trait ConnectorCleanupAsyncSource: Send + Sync {
    fn poll_output(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ConnectorCleanupAsyncOutput, ConnectorError>>;
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
    fn publication_fact(&self) -> (Option<LakePublicationDisposition>, bool);
}

/// Read-only facts from the same source. Observing does not drive effects.
#[derive(Clone)]
pub struct ConnectorCleanupAsyncObservation {
    original: Arc<dyn ConnectorCleanupAsyncSource>,
}

impl ConnectorCleanupAsyncObservation {
    pub fn publication_disposition(&self) -> Option<LakePublicationDisposition> {
        self.original.publication_fact().0
    }
}

pub struct ConnectorCleanupAsyncExecution {
    original: Arc<dyn ConnectorCleanupAsyncSource>,
    expected: Option<(ConnectorProviderBindingKey, ConnectorCleanupOperationId)>,
}

impl ConnectorCleanupAsyncExecution {
    pub fn from_original(original: Arc<dyn ConnectorCleanupAsyncSource>) -> Self {
        Self {
            original,
            expected: None,
        }
    }

    pub fn observation(&self) -> ConnectorCleanupAsyncObservation {
        ConnectorCleanupAsyncObservation {
            original: self.original.clone(),
        }
    }

    pub fn poll_output(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ConnectorCleanupAsyncOutput, ConnectorError>> {
        let value = std::task::ready!(self.original.poll_output(cx))?;
        if let Some((owner, operation)) = &self.expected
            && (&value.owner != owner || value.operation_id != *operation)
        {
            return Poll::Ready(Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup output differs from its original owner or operation",
            )));
        }
        Poll::Ready(Ok(value))
    }

    pub fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.original.poll_closed(cx)
    }

    pub fn publication_fact(&self) -> (Option<LakePublicationDisposition>, bool) {
        self.original.publication_fact()
    }
}
