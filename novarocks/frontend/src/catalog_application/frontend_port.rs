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

//! Frontend implementation of the catalog application boundary.
//!
//! The StateStore attachment is committed before a local control generation is
//! registered. A registration failure therefore leaves durable truth intact
//! and is reported as `Unavailable`; reconciliation can retry installation.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::{
    CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind, CatalogApplicationPort,
    CatalogCreateCommand, CatalogDropCommand, CatalogRuntimeObservation,
    CatalogRuntimePublisherSink,
};
use crate::mv::domain::repository::{MvRepositoryError, MvRepositoryErrorKind};
use novarocks_spi::connector::{
    ConnectorControlFactoryRequest, ConnectorControlFactoryResolver, ConnectorControlResolver,
    ConnectorInstanceId, ConnectorProviderId,
};
use tokio::runtime::{Handle, RuntimeFlavor};
use uuid::Uuid;

use crate::catalog_attachment::{
    CatalogAttachment, CatalogAttachmentError, CatalogAttachmentErrorKind,
    CatalogAttachmentRepository,
};
use crate::connector::ConnectorControlHost;
use crate::mv::repository::CatalogAttachmentObservationSource;

#[derive(Clone, Debug, Eq, PartialEq)]
enum LocalProjection {
    Unavailable {
        attachment_id: Uuid,
        provider_id: ConnectorProviderId,
        reason: String,
    },
    Ready {
        attachment_id: Uuid,
        provider_id: ConnectorProviderId,
        generation: u64,
    },
}

impl LocalProjection {
    fn attachment_id(&self) -> Uuid {
        match self {
            Self::Unavailable { attachment_id, .. } | Self::Ready { attachment_id, .. } => {
                *attachment_id
            }
        }
    }

    fn ready_generation(&self) -> Option<u64> {
        match self {
            Self::Ready { generation, .. } => Some(*generation),
            Self::Unavailable { .. } => None,
        }
    }
}

/// Owns durable attachment mutation and the local Connector control projection.
// Design: ADR-0066 (docs/adr/ADR-0066-state-store-catalog-attachment-authority.md)
pub struct FrontendCatalogApplicationPort {
    repository: Option<CatalogAttachmentRepository>,
    control: Arc<ConnectorControlHost>,
    runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
    runtime: Handle,
    projections: Mutex<BTreeMap<ConnectorInstanceId, LocalProjection>>,
    next_generation: AtomicU64,
}

impl FrontendCatalogApplicationPort {
    pub fn unavailable(
        control: Arc<ConnectorControlHost>,
        runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
        runtime: Handle,
    ) -> Self {
        Self {
            repository: None,
            control,
            runtime_publisher,
            runtime,
            projections: Mutex::new(BTreeMap::new()),
            next_generation: AtomicU64::new(1),
        }
    }

    pub fn new(
        repository: CatalogAttachmentRepository,
        control: Arc<ConnectorControlHost>,
        runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
        runtime: Handle,
    ) -> Self {
        Self {
            repository: Some(repository),
            control,
            runtime_publisher,
            runtime,
            projections: Mutex::new(BTreeMap::new()),
            next_generation: AtomicU64::new(1),
        }
    }

    fn repository(&self) -> Result<&CatalogAttachmentRepository, CatalogApplicationError> {
        self.repository.as_ref().ok_or_else(|| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "catalog attachments require a configured Frontend StateStore",
            )
        })
    }

    fn block_on<T>(
        &self,
        future: impl Future<Output = Result<T, CatalogAttachmentError>>,
    ) -> Result<T, CatalogApplicationError> {
        let result = match Handle::try_current() {
            Ok(_) if self.runtime.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                return Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Unavailable,
                    "catalog attachment StateStore access is unavailable on a current-thread Tokio runtime",
                ));
            }
            Ok(_) => tokio::task::block_in_place(|| self.runtime.block_on(future)),
            Err(_) => self.runtime.block_on(future),
        };
        result.map_err(repository_error)
    }

    fn next_projection_generation(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::Relaxed)
    }

    fn observation(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission {
        let projection = match self.projections.lock() {
            Ok(projections) => projections.get(instance_id).cloned(),
            Err(_) => {
                return CatalogAdmission::Unavailable {
                    reason: "catalog projection lock is poisoned".to_string(),
                };
            }
        };
        let Some(projection) = projection else {
            return CatalogAdmission::Absent;
        };
        match projection {
            LocalProjection::Unavailable { reason, .. } => CatalogAdmission::Unavailable { reason },
            LocalProjection::Ready {
                attachment_id,
                provider_id,
                generation,
            } => match self.control.observe_current_binding(instance_id) {
                Ok(_) => CatalogAdmission::Ready(CatalogRuntimeObservation {
                    attachment_id,
                    instance_id: instance_id.clone(),
                    provider_id,
                    generation,
                }),
                Err(error) => CatalogAdmission::Unavailable {
                    reason: error.to_string(),
                },
            },
        }
    }

    fn mark_unavailable(
        &self,
        instance_id: &ConnectorInstanceId,
        attachment_id: Uuid,
        provider_id: &ConnectorProviderId,
        reason: impl Into<String>,
    ) {
        let previous = self.projections.lock().ok().and_then(|mut projections| {
            projections.insert(
                instance_id.clone(),
                LocalProjection::Unavailable {
                    attachment_id,
                    provider_id: provider_id.clone(),
                    reason: reason.into(),
                },
            )
        });
        if let Some(generation) = previous
            .as_ref()
            .and_then(LocalProjection::ready_generation)
            && let Err(error) = self
                .runtime_publisher
                .unpublish_catalog_runtime(instance_id, generation)
        {
            tracing::warn!(%error, catalog = instance_id.as_str(), "catalog runtime unpublish failed while marking projection unavailable");
        }
        if previous.is_some()
            && let Err(error) = self.control.retire_current(instance_id)
        {
            tracing::debug!(%error, catalog = instance_id.as_str(), "catalog runtime was not locally active while marking projection unavailable");
        }
    }

    fn install_created(
        &self,
        attachment: &CatalogAttachment,
        binding: novarocks_spi::connector::ConnectorControlBinding,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        self.control.register(binding).map_err(connector_error)?;
        let generation = self.next_projection_generation();
        let observation = CatalogRuntimeObservation {
            attachment_id: attachment.attachment_id,
            instance_id: attachment.instance_id.clone(),
            provider_id: attachment.provider_id.clone(),
            generation,
        };
        if let Err(error) = self
            .runtime_publisher
            .publish_catalog_runtime(observation.clone())
        {
            let _ = self.control.retire_current(&attachment.instance_id);
            return Err(error);
        }
        let projection = LocalProjection::Ready {
            attachment_id: attachment.attachment_id,
            provider_id: attachment.provider_id.clone(),
            generation,
        };
        let publish_result = self
            .projections
            .lock()
            .map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog projection lock is poisoned",
                )
            })
            .and_then(
                |mut projections| match projections.get(&attachment.instance_id) {
                    Some(LocalProjection::Unavailable {
                        attachment_id,
                        provider_id,
                        ..
                    }) if *attachment_id == attachment.attachment_id
                        && *provider_id == attachment.provider_id =>
                    {
                        projections.insert(attachment.instance_id.clone(), projection);
                        Ok(())
                    }
                    _ => Err(CatalogApplicationError::new(
                        CatalogApplicationErrorKind::Conflict,
                        "catalog projection changed before its runtime became ready",
                    )),
                },
            );
        if let Err(error) = publish_result {
            let _ = self
                .runtime_publisher
                .unpublish_catalog_runtime(&attachment.instance_id, generation);
            let _ = self.control.retire_current(&attachment.instance_id);
            if let Ok(mut projections) = self.projections.lock()
                && projections
                    .get(&attachment.instance_id)
                    .is_some_and(|projection| {
                        projection.attachment_id() == attachment.attachment_id
                    })
            {
                projections.insert(
                    attachment.instance_id.clone(),
                    LocalProjection::Unavailable {
                        attachment_id: attachment.attachment_id,
                        provider_id: attachment.provider_id.clone(),
                        reason: error.to_string(),
                    },
                );
            }
            return Err(error);
        }
        Ok(observation)
    }

    /// Rebuild this process's control projection from the authoritative
    /// attachment scan. A change hint never carries attachment state; callers
    /// always invoke this method after rereading StateStore. Factory and
    /// registration work is bounded because provider materialization can
    /// synchronously perform remote validation.
    pub(crate) async fn reconcile_with_page_size(
        self: &Arc<Self>,
        page_size: usize,
        worker_count: usize,
    ) -> Result<(), CatalogApplicationError> {
        if worker_count == 0 {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::InvalidRequest,
                "catalog projection worker count must be positive",
            ));
        }
        let repository = self.repository()?;
        let attachments = repository
            .list_with_page_size(page_size)
            .await
            .map_err(repository_error)?;
        let desired = attachments
            .iter()
            .map(|versioned| {
                (
                    versioned.attachment.instance_id.clone(),
                    versioned.attachment.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let stale = self
            .projections
            .lock()
            .map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog projection lock is poisoned",
                )
            })?
            .iter()
            .filter(|(instance_id, _)| !desired.contains_key(*instance_id))
            .map(|(instance_id, projection)| (instance_id.clone(), projection.attachment_id()))
            .collect::<Vec<_>>();
        // A projection missing from the scan is not proof that its attachment is
        // gone: `create_catalog` commits the attachment and only then installs
        // the projection, so a catalog created after `list` began is present
        // locally and absent from `desired`. Retiring on that alone made the
        // statement right after CREATE EXTERNAL CATALOG fail with
        // "unknown catalog" whenever a reconcile cycle straddled it.
        //
        // Re-reading each candidate closes the window rather than narrowing it:
        // the projection can only exist because the attachment was already
        // committed, so a read issued after observing the projection sees it.
        for (instance_id, attachment_id) in stale {
            match repository.get(&instance_id).await {
                Ok(Some(versioned)) if versioned.attachment.attachment_id == attachment_id => {}
                Ok(_) => self.retire_projection(&instance_id),
                // Keep serving and retry next cycle: the read failed, so nothing
                // was proven about the attachment either way.
                Err(error) => tracing::warn!(
                    %error,
                    catalog = instance_id.as_str(),
                    "catalog attachment re-read failed while retiring a projection absent from the scan",
                ),
            }
        }

        let mut workers = tokio::task::JoinSet::new();
        for attachment in desired.into_values() {
            if workers.len() >= worker_count {
                let completed = workers.join_next().await.ok_or_else(|| {
                    CatalogApplicationError::new(
                        CatalogApplicationErrorKind::Internal,
                        "catalog projection worker exited unexpectedly",
                    )
                })?;
                completed.map_err(|error| {
                    CatalogApplicationError::new(
                        CatalogApplicationErrorKind::Internal,
                        format!("catalog projection worker failed: {error}"),
                    )
                })?;
            }
            let projection = Arc::clone(self);
            workers.spawn_blocking(move || projection.reconcile_attachment(attachment));
        }
        while let Some(completed) = workers.join_next().await {
            completed.map_err(|error| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    format!("catalog projection worker failed: {error}"),
                )
            })?;
        }
        Ok(())
    }

    fn reconcile_attachment(&self, attachment: CatalogAttachment) {
        let installed = self
            .projections
            .lock()
            .map(|projections| {
                projections
                    .get(&attachment.instance_id)
                    .is_some_and(|projection| {
                        matches!(
                            projection,
                            LocalProjection::Ready { attachment_id, .. }
                                if *attachment_id == attachment.attachment_id
                        )
                    })
            })
            .unwrap_or(false)
            && self
                .control
                .observe_current_binding(&attachment.instance_id)
                .is_ok();
        if installed {
            return;
        }

        self.mark_unavailable(
            &attachment.instance_id,
            attachment.attachment_id,
            &attachment.provider_id,
            "catalog attachment runtime is being materialized",
        );
        let installed = (|| {
            let request = ConnectorControlFactoryRequest::try_new(
                attachment.provider_id.clone(),
                attachment.instance_id.clone(),
                attachment.durable_properties.clone(),
            )
            .map_err(connector_error)?;
            let creation = self
                .control
                .create_control(request)
                .map_err(connector_error)?;
            let (binding, _) = creation.into_parts();
            self.install_created(&attachment, binding).map(|_| ())
        })();
        if let Err(error) = installed {
            self.mark_unavailable(
                &attachment.instance_id,
                attachment.attachment_id,
                &attachment.provider_id,
                error.to_string(),
            );
            // A single provider failure must not make durable truth disappear
            // or prevent unrelated catalog projections. Its admission remains
            // Unavailable until a later resync works.
            tracing::warn!(%error, catalog = attachment.instance_id.as_str(), "catalog attachment remains unavailable after projection attempt");
        }
    }

    /// Stops all local admission before retiring existing leases. Durable
    /// attachments remain unchanged, so a later authoritative reconcile can
    /// construct fresh generations after a freshness outage.
    pub(crate) fn unpublish_all(&self) {
        let attachments = self
            .projections
            .lock()
            .map(|projections| {
                projections
                    .iter()
                    .map(|(instance_id, projection)| match projection {
                        LocalProjection::Unavailable {
                            attachment_id,
                            provider_id,
                            ..
                        }
                        | LocalProjection::Ready {
                            attachment_id,
                            provider_id,
                            ..
                        } => (instance_id.clone(), *attachment_id, provider_id.clone()),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for (instance_id, attachment_id, provider_id) in attachments {
            self.mark_unavailable(
                &instance_id,
                attachment_id,
                &provider_id,
                "catalog attachment projection freshness expired",
            );
        }
    }

    pub(crate) fn projection_count(&self) -> usize {
        self.projections
            .lock()
            .map(|projections| {
                projections
                    .values()
                    .filter(|projection| matches!(projection, LocalProjection::Ready { .. }))
                    .count()
            })
            .unwrap_or_default()
    }

    fn retire_projection(&self, instance_id: &ConnectorInstanceId) {
        let projection = self
            .projections
            .lock()
            .ok()
            .and_then(|mut projections| projections.remove(instance_id));
        if let Some(generation) = projection
            .as_ref()
            .and_then(LocalProjection::ready_generation)
            && let Err(error) = self
                .runtime_publisher
                .unpublish_catalog_runtime(instance_id, generation)
        {
            tracing::warn!(%error, catalog = instance_id.as_str(), "catalog runtime unpublish failed during retirement");
        }
        if let Err(error) = self.control.retire_current(instance_id) {
            tracing::debug!(%error, catalog = instance_id.as_str(), "catalog runtime was not locally active during retirement");
        }
    }
}

impl CatalogApplicationPort for FrontendCatalogApplicationPort {
    fn create_catalog(
        &self,
        command: CatalogCreateCommand,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        let repository = self.repository()?;
        if self
            .block_on(repository.get(&command.instance_id))?
            .is_some()
        {
            if !command.if_not_exists {
                return Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::AlreadyExists,
                    "catalog attachment already exists",
                ));
            }
            return self
                .admit_catalog(&command.instance_id)
                .require_ready(&command.instance_id);
        }

        let provider_id = provider_id_from_properties(&command.properties)?;
        let request = ConnectorControlFactoryRequest::try_new(
            provider_id.clone(),
            command.instance_id.clone(),
            command.properties,
        )
        .map_err(connector_error)?;
        // The factory may validate provider configuration, but it does not
        // become live until after the attachment CAS succeeds below.
        let creation = self
            .control
            .create_control(request)
            .map_err(connector_error)?;
        let (binding, mut durable_properties) = creation.into_parts();
        durable_properties.sort_by(|left, right| left.0.cmp(&right.0));
        let attachment = CatalogAttachment {
            attachment_id: Uuid::now_v7(),
            instance_id: command.instance_id,
            provider_id,
            display_name: command.display_name,
            durable_properties,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let created = self.block_on(repository.create(attachment))?;
        self.mark_unavailable(
            &created.attachment.instance_id,
            created.attachment.attachment_id,
            &created.attachment.provider_id,
            "catalog attachment runtime is being installed",
        );
        self.install_created(&created.attachment, binding)
    }

    fn drop_catalog(&self, command: CatalogDropCommand) -> Result<(), CatalogApplicationError> {
        let repository = self.repository()?;
        let Some(existing) = self.block_on(repository.get(&command.instance_id))? else {
            return if command.if_exists {
                Ok(())
            } else {
                Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::NotFound,
                    "catalog attachment was not found",
                ))
            };
        };
        self.block_on(repository.drop_exact_fenced_by_materialized_views(existing, 256))?;
        self.retire_projection(&command.instance_id);
        // Durable deletion is authoritative. A local generation can be absent
        // or already retiring; either case converges through reconciliation.
        Ok(())
    }

    fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission {
        if self.repository.is_none() {
            return CatalogAdmission::Unavailable {
                reason: "catalog attachments require a configured Frontend StateStore".to_string(),
            };
        }
        self.observation(instance_id)
    }
}

impl CatalogAttachmentObservationSource for FrontendCatalogApplicationPort {
    fn capture(
        &self,
        catalogs: &std::collections::BTreeSet<String>,
    ) -> Result<Vec<crate::catalog_attachment::CatalogAttachmentVersioned>, MvRepositoryError> {
        let repository = self.repository().map_err(mv_repository_error)?;
        let mut observations = Vec::with_capacity(catalogs.len());
        for catalog in catalogs {
            let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| {
                MvRepositoryError::new(MvRepositoryErrorKind::InvalidRequest, error.to_string())
            })?;
            match self.admit_catalog(&instance_id) {
                CatalogAdmission::Ready(observation) => {
                    let versioned = self
                        .block_on(repository.get(&instance_id))
                        .map_err(mv_repository_error)?
                        .ok_or_else(|| {
                            MvRepositoryError::new(
                                MvRepositoryErrorKind::Conflict,
                                "catalog attachment disappeared during MV admission",
                            )
                        })?;
                    if versioned.attachment.attachment_id != observation.attachment_id
                        || versioned.attachment.provider_id != observation.provider_id
                    {
                        return Err(MvRepositoryError::new(
                            MvRepositoryErrorKind::Conflict,
                            "catalog attachment changed during MV admission",
                        ));
                    }
                    observations.push(versioned);
                }
                CatalogAdmission::Absent => {
                    return Err(MvRepositoryError::new(
                        MvRepositoryErrorKind::Conflict,
                        "materialized view references a catalog attachment that is absent",
                    ));
                }
                CatalogAdmission::Unavailable { reason } => {
                    return Err(MvRepositoryError::new(
                        MvRepositoryErrorKind::Unavailable,
                        format!("materialized view catalog admission is unavailable: {reason}"),
                    ));
                }
            }
        }
        Ok(observations)
    }
}

fn provider_id_from_properties(
    properties: &[(String, String)],
) -> Result<ConnectorProviderId, CatalogApplicationError> {
    let mut providers = properties
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case("type"))
        .map(|(_, value)| value.as_str());
    let Some(provider) = providers.next() else {
        return Err(CatalogApplicationError::new(
            CatalogApplicationErrorKind::InvalidRequest,
            "CREATE CATALOG requires exactly one type property",
        ));
    };
    if providers.next().is_some() {
        return Err(CatalogApplicationError::new(
            CatalogApplicationErrorKind::InvalidRequest,
            "CREATE CATALOG requires exactly one type property",
        ));
    }
    ConnectorProviderId::parse(provider).map_err(|error| {
        CatalogApplicationError::new(
            CatalogApplicationErrorKind::InvalidRequest,
            error.to_string(),
        )
    })
}

fn repository_error(error: CatalogAttachmentError) -> CatalogApplicationError {
    let kind = match error.kind() {
        CatalogAttachmentErrorKind::InvalidRequest => CatalogApplicationErrorKind::InvalidRequest,
        CatalogAttachmentErrorKind::NotFound => CatalogApplicationErrorKind::NotFound,
        CatalogAttachmentErrorKind::AlreadyExists => CatalogApplicationErrorKind::AlreadyExists,
        CatalogAttachmentErrorKind::Conflict => CatalogApplicationErrorKind::Conflict,
        CatalogAttachmentErrorKind::Unavailable | CatalogAttachmentErrorKind::CommitUnknown => {
            CatalogApplicationErrorKind::Unavailable
        }
        CatalogAttachmentErrorKind::Corruption => CatalogApplicationErrorKind::Internal,
    };
    CatalogApplicationError::new(kind, error.to_string())
}

fn connector_error(error: novarocks_spi::connector::ConnectorError) -> CatalogApplicationError {
    use novarocks_spi::connector::ConnectorErrorKind;

    let kind = match error.kind() {
        ConnectorErrorKind::InvalidRequest => CatalogApplicationErrorKind::InvalidRequest,
        ConnectorErrorKind::NotFound => CatalogApplicationErrorKind::Unavailable,
        ConnectorErrorKind::Unavailable
        | ConnectorErrorKind::ResourceExhausted
        | ConnectorErrorKind::DeadlineExceeded
        | ConnectorErrorKind::Cancelled => CatalogApplicationErrorKind::Unavailable,
        ConnectorErrorKind::PermissionDenied
        | ConnectorErrorKind::Unsupported
        | ConnectorErrorKind::CorruptData
        | ConnectorErrorKind::Internal => CatalogApplicationErrorKind::Internal,
    };
    CatalogApplicationError::new(kind, error.to_string())
}

fn mv_repository_error(error: CatalogApplicationError) -> MvRepositoryError {
    let kind = match error.kind() {
        CatalogApplicationErrorKind::InvalidRequest => MvRepositoryErrorKind::InvalidRequest,
        CatalogApplicationErrorKind::NotFound
        | CatalogApplicationErrorKind::AlreadyExists
        | CatalogApplicationErrorKind::Conflict => MvRepositoryErrorKind::Conflict,
        CatalogApplicationErrorKind::Unavailable => MvRepositoryErrorKind::Unavailable,
        CatalogApplicationErrorKind::Internal => MvRepositoryErrorKind::Corruption,
    };
    MvRepositoryError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn create_catalog_requires_one_type_property() {
        assert_eq!(
            provider_id_from_properties(&[])
                .expect_err("missing type must fail")
                .kind(),
            CatalogApplicationErrorKind::InvalidRequest
        );
        assert_eq!(
            provider_id_from_properties(&[
                ("type".to_string(), "iceberg".to_string()),
                ("TYPE".to_string(), "starrocks".to_string()),
            ])
            .expect_err("duplicate type must fail")
            .kind(),
            CatalogApplicationErrorKind::InvalidRequest
        );
        assert_eq!(
            provider_id_from_properties(&[("type".to_string(), "iceberg".to_string())])
                .expect("one type")
                .as_str(),
            "iceberg"
        );
    }

    #[tokio::test]
    async fn mv_observation_source_rejects_catalogs_without_a_durable_frontend_attachment() {
        let port = FrontendCatalogApplicationPort::unavailable(
            Arc::new(ConnectorControlHost::new()),
            CatalogRuntimeProjection::new().publisher(),
            tokio::runtime::Handle::current(),
        );
        let error = CatalogAttachmentObservationSource::capture(
            &port,
            &BTreeSet::from(["catalog.analytics".to_string()]),
        )
        .expect_err("an unavailable attachment repository cannot freeze an MV dependency");
        assert_eq!(error.kind(), MvRepositoryErrorKind::Unavailable);
    }
}
