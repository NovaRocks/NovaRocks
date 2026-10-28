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
//! Every reconcile runs one path: enumerate a complete
//! [`CatalogDesiredStateSnapshot`] from the selected source, validate it, then
//! materialize each located entry on its own. The two failure scopes that path
//! produces are carried by the error type rather than by which call happened to
//! propagate — see [`CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete`]
//! for the global one and [`CatalogApplicationService::materialize_entry`]
//! for the per-catalog one.
//!
//! Desired state is committed before a local control generation is registered.
//! A registration failure therefore leaves the source's truth intact and is
//! reported as `Unavailable`; reconciliation can retry installation.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{
    CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind, CatalogApplicationPort,
    CatalogAttachment, CatalogAttachmentError, CatalogAttachmentErrorKind,
    CatalogAttachmentRepository, CatalogAttachmentWakeupSignal, CatalogCreateCommand,
    CatalogDesiredStateEntry, CatalogDesiredStateSnapshot, CatalogDesiredStateSource,
    CatalogDesiredStateSourceMode, CatalogDropCommand, CatalogReferenceReader,
    CatalogRuntimeObservation, CatalogRuntimePublisherSink,
};
#[cfg(test)]
use futures::future::BoxFuture;
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
    ConnectorControlResolver, ConnectorInstanceId, ConnectorProviderId, CredentialConsumerRole,
    NodeLocalCredentialReference, StaticCredentialReference,
    canonicalize_catalog_credential_bindings,
};
use novarocks_spi::connector::{
    ConnectorMaterializationRetryDisposition, MaterializationContext, NormalizedCatalogProperties,
};
use tokio::runtime::{Handle, RuntimeFlavor};
use uuid::Uuid;

use crate::ConnectorControlHost;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogMaterializationConfig {
    pub attempt_timeout: Duration,
    pub retry_initial_backoff: Duration,
    pub retry_max_backoff: Duration,
    pub max_inflight: usize,
}

impl CatalogMaterializationConfig {
    pub fn try_new(
        attempt_timeout: Duration,
        retry_initial_backoff: Duration,
        retry_max_backoff: Duration,
        max_inflight: usize,
    ) -> Result<Self, CatalogApplicationError> {
        if attempt_timeout.is_zero()
            || retry_initial_backoff.is_zero()
            || retry_max_backoff.is_zero()
            || retry_initial_backoff > retry_max_backoff
            || max_inflight == 0
        {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::InvalidRequest,
                "catalog materialization bounds must be nonzero and retry initial must not exceed retry max",
            ));
        }
        Ok(Self {
            attempt_timeout,
            retry_initial_backoff,
            retry_max_backoff,
            max_inflight,
        })
    }
}

impl Default for CatalogMaterializationConfig {
    fn default() -> Self {
        Self::try_new(
            Duration::from_secs(10),
            Duration::from_millis(100),
            Duration::from_secs(5),
            64,
        )
        .expect("default catalog materialization bounds are valid")
    }
}

/// Per-exact-key state owned by the FE projection loop. The entry deliberately
/// stores no provider binding: a binding is published only after the attempt
/// completes and its token is still current.
struct ProjectionAttempt {
    instance_id: ConnectorInstanceId,
    attachment_id: Uuid,
    token: u64,
    context: MaterializationContext,
    completion_waiters: Vec<
        tokio::sync::oneshot::Sender<Result<CatalogRuntimeObservation, CatalogApplicationError>>,
    >,
}

struct CatalogMaterializer {
    identity: novarocks_spi::connector::CatalogScopeIdentity,
    factory: Arc<dyn novarocks_spi::connector::ProviderCatalogControlFactory>,
    reader: Option<Arc<dyn novarocks_spi::connector::CatalogActivityReader>>,
}

#[derive(Default)]
struct CatalogMaterializers {
    current: BTreeMap<ConnectorInstanceId, CatalogMaterializer>,
    draining: Vec<(
        novarocks_spi::connector::CatalogScopeIdentity,
        std::sync::Weak<dyn novarocks_spi::connector::CatalogActivityReader>,
    )>,
}
impl CatalogMaterializers {
    fn retain_draining(&mut self, owner: &CatalogMaterializer) {
        self.draining
            .retain(|(_, reader)| reader.strong_count() > 0);
        if let Some(reader) = &owner.reader {
            self.draining
                .push((owner.identity.clone(), Arc::downgrade(reader)));
        }
    }
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.current.is_empty()
    }
}

struct MaterializationSubmission {
    entry: CatalogDesiredStateEntry,
    provider_id: ConnectorProviderId,
    properties: NormalizedCatalogProperties,
    factory: Arc<dyn novarocks_spi::connector::ProviderCatalogControlFactory>,
    key: CatalogHandle,
    token: u64,
    context: MaterializationContext,
}

struct ProjectionScheduler {
    attempts: Mutex<BTreeMap<CatalogHandle, ProjectionAttempt>>,
    next_token: AtomicU64,
    permits: Arc<tokio::sync::Semaphore>,
    config: CatalogMaterializationConfig,
}

impl ProjectionScheduler {
    fn new(config: CatalogMaterializationConfig) -> Self {
        Self {
            attempts: Mutex::new(BTreeMap::new()),
            next_token: AtomicU64::new(1),
            permits: Arc::new(tokio::sync::Semaphore::new(config.max_inflight)),
            config,
        }
    }
}

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

/// Aggregate local materialization result for one exact desired-state snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CatalogProjectionCounts {
    pub ready: usize,
    pub unavailable: usize,
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

/// Owns catalog desired-state mutation and the local Connector control
/// projection.
///
/// The source is taken by value at construction and never replaced, so which
/// authority owns catalog desired state is a composition-time fact of this
/// process. `None` means this frontend was composed without any source at all
/// — a role that never serves external catalogs — which is a different thing
/// from a source that exists and is failing.
// Design: ADR-0115 (docs/adr/ADR-0115-catalog-desired-state-source-modes.md)
pub struct CatalogApplicationService {
    source: Option<CatalogDesiredStateSource>,
    control: Arc<ConnectorControlHost>,
    runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
    runtime: Handle,
    projections: Mutex<BTreeMap<ConnectorInstanceId, LocalProjection>>,
    /// Desired-entry lifetime, independent of current/retiring generations.
    catalog_materializers: Mutex<CatalogMaterializers>,
    complete_reachable_catalogs: Mutex<Option<BTreeSet<CatalogHandle>>>,
    next_generation: AtomicU64,
    scheduler: ProjectionScheduler,
    reference_reader: Arc<dyn CatalogReferenceReader>,
}

impl CatalogApplicationService {
    pub fn unavailable(
        control: Arc<ConnectorControlHost>,
        runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
        runtime: Handle,
        reference_reader: Arc<dyn CatalogReferenceReader>,
    ) -> Self {
        Self {
            source: None,
            control,
            runtime_publisher,
            runtime,
            projections: Mutex::new(BTreeMap::new()),
            catalog_materializers: Mutex::new(CatalogMaterializers::default()),
            complete_reachable_catalogs: Mutex::new(None),
            next_generation: AtomicU64::new(1),
            scheduler: ProjectionScheduler::new(CatalogMaterializationConfig::default()),
            reference_reader,
        }
    }

    pub fn new(
        source: CatalogDesiredStateSource,
        control: Arc<ConnectorControlHost>,
        runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
        runtime: Handle,
        reference_reader: Arc<dyn CatalogReferenceReader>,
    ) -> Self {
        Self::new_with_materialization_config(
            source,
            control,
            runtime_publisher,
            runtime,
            CatalogMaterializationConfig::default(),
            reference_reader,
        )
    }

    pub fn new_with_materialization_config(
        source: CatalogDesiredStateSource,
        control: Arc<ConnectorControlHost>,
        runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
        runtime: Handle,
        materialization_config: CatalogMaterializationConfig,
        reference_reader: Arc<dyn CatalogReferenceReader>,
    ) -> Self {
        Self {
            source: Some(source),
            control,
            runtime_publisher,
            runtime,
            projections: Mutex::new(BTreeMap::new()),
            catalog_materializers: Mutex::new(CatalogMaterializers::default()),
            complete_reachable_catalogs: Mutex::new(None),
            next_generation: AtomicU64::new(1),
            scheduler: ProjectionScheduler::new(materialization_config),
            reference_reader,
        }
    }

    fn source(&self) -> Result<&CatalogDesiredStateSource, CatalogApplicationError> {
        self.source.as_ref().ok_or_else(|| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "this frontend has no configured catalog desired-state source",
            )
        })
    }

    /// A signal that fires when this process commits a desired-state write.
    ///
    /// `None` when the configured source has no writes of ours to announce
    /// (see [`CatalogDesiredStateSource::attachment_wakeup_signal`]) or when
    /// this frontend has no source at all. The reconciler that consumes it
    /// owns a periodic sweep either way, so `None` costs latency, never
    /// correctness.
    pub fn attachment_wakeup_signal(&self) -> Option<CatalogAttachmentWakeupSignal> {
        self.source.as_ref()?.attachment_wakeup_signal()
    }

    /// A complete desired-state projection plus every still-draining local
    /// control generation. `None` means this frontend has not completed a
    /// source enumeration, so pruning must skip the round.
    pub fn reachable_catalog_handles(&self) -> Option<BTreeSet<CatalogHandle>> {
        let mut reachable = self.complete_reachable_catalogs.lock().ok()?.clone()?;
        reachable.extend(self.control.reachable_catalog_handles().ok()?);
        Some(reachable)
    }

    /// The authority a SQL `CREATE`/`DROP CATALOG` writes through.
    ///
    /// Admission is a function of the selected source mode, so a deployment
    /// whose desired state comes from a file or a controller never reaches a
    /// repository here: it is refused with
    /// [`CatalogApplicationErrorKind::UnsupportedSourceMode`] instead, which is
    /// what keeps one truth from having two writers.
    fn sql_mutation_authority(
        &self,
    ) -> Result<&CatalogAttachmentRepository, CatalogApplicationError> {
        self.source()?.sql_mutation_authority()
    }

    fn block_on_catalog<T>(
        &self,
        future: impl Future<Output = Result<T, CatalogApplicationError>>,
    ) -> Result<T, CatalogApplicationError> {
        match Handle::try_current() {
            Ok(_) if self.runtime.runtime_flavor() == RuntimeFlavor::CurrentThread => {
                Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Unavailable,
                    "catalog materialization is unavailable on a current-thread Tokio runtime",
                ))
            }
            Ok(_) => tokio::task::block_in_place(|| self.runtime.block_on(future)),
            Err(_) => self.runtime.block_on(future),
        }
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
        entry: &CatalogDesiredStateEntry,
        binding: novarocks_spi::connector::ConnectorControlRoleBinding,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        let attachment_id = entry.identity().as_uuid();
        let instance_id = entry.config().instance_id();
        let provider_id = entry.config().provider_id();
        self.control
            .register_role_binding(binding)
            .map_err(connector_error)?;
        let generation = self.next_projection_generation();
        let observation = CatalogRuntimeObservation {
            attachment_id,
            instance_id: instance_id.clone(),
            provider_id: provider_id.clone(),
            generation,
        };
        if let Err(error) = self
            .runtime_publisher
            .publish_catalog_runtime(observation.clone())
        {
            let _ = self.control.retire_current(instance_id);
            return Err(error);
        }
        let projection = LocalProjection::Ready {
            attachment_id,
            provider_id: provider_id.clone(),
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
            .and_then(|mut projections| match projections.get(instance_id) {
                Some(LocalProjection::Unavailable {
                    attachment_id: installed_id,
                    provider_id: installed_provider,
                    ..
                }) if *installed_id == attachment_id && installed_provider == provider_id => {
                    projections.insert(instance_id.clone(), projection);
                    Ok(())
                }
                _ => Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Conflict,
                    "catalog projection changed before its runtime became ready",
                )),
            });
        if publish_result.is_ok() {
            self.runtime_publisher.catalog_runtime_admitted(instance_id);
        }
        if let Err(error) = publish_result {
            let _ = self
                .runtime_publisher
                .unpublish_catalog_runtime(instance_id, generation);
            let _ = self.control.retire_current(instance_id);
            if let Ok(mut projections) = self.projections.lock()
                && projections
                    .get(instance_id)
                    .is_some_and(|projection| projection.attachment_id() == attachment_id)
            {
                projections.insert(
                    instance_id.clone(),
                    LocalProjection::Unavailable {
                        attachment_id,
                        provider_id: provider_id.clone(),
                        reason: error.to_string(),
                    },
                );
            }
            return Err(error);
        }
        Ok(observation)
    }

    /// Rebuilds this process's control projection from the selected source's
    /// desired state, as `enumerate -> validate -> per-catalog materialize`.
    ///
    /// A change hint never carries desired state; callers always invoke this
    /// method after rereading the source. Factory and registration work is
    /// bounded because provider materialization can synchronously perform
    /// remote validation.
    ///
    /// The two failure scopes are expressed by the type of what fails, not by
    /// which statement happens to use `?`:
    ///
    /// * `enumerate` returns a whole snapshot or
    ///   [`CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete`], so
    ///   an incomplete enumeration propagates and fails frontend bootstrap. It
    ///   cannot arrive here as a valid snapshot holding fewer catalogs, which
    ///   would retire the ones it lost.
    /// * [`CatalogApplicationService::materialize_entry`] returns `()`, so
    ///   one catalog's provider failure cannot reach this function's `Result`
    ///   at all — it marks that catalog `Unavailable` and leaves the rest
    ///   serving.
    pub async fn reconcile_with_page_size(
        self: &Arc<Self>,
        page_size: usize,
        worker_count: usize,
    ) -> Result<(), CatalogApplicationError> {
        self.reconcile_snapshot_with_page_size(page_size, worker_count)
            .await
            .map(|_| ())
    }

    /// Reconciles one complete source snapshot and returns its exact identity
    /// together with this process's materialization counts at the submission
    /// boundary. The source is enumerated exactly once; callers must not
    /// reread it merely to publish bootstrap observability.
    pub async fn reconcile_snapshot_with_page_size(
        self: &Arc<Self>,
        page_size: usize,
        worker_count: usize,
    ) -> Result<(CatalogDesiredStateSnapshot, CatalogProjectionCounts), CatalogApplicationError>
    {
        self.reconcile_snapshot_with_execution(page_size, worker_count, None)
            .await
    }

    pub async fn reconcile_snapshot_in(
        self: &Arc<Self>,
        page_size: usize,
        worker_count: usize,
        execution: &dyn novarocks_state_store_api::StateStoreReadExecution,
    ) -> Result<(CatalogDesiredStateSnapshot, CatalogProjectionCounts), CatalogApplicationError>
    {
        self.reconcile_snapshot_with_execution(page_size, worker_count, Some(execution))
            .await
    }

    async fn reconcile_snapshot_with_execution(
        self: &Arc<Self>,
        page_size: usize,
        worker_count: usize,
        execution: Option<&dyn novarocks_state_store_api::StateStoreReadExecution>,
    ) -> Result<(CatalogDesiredStateSnapshot, CatalogProjectionCounts), CatalogApplicationError>
    {
        if worker_count == 0 {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::InvalidRequest,
                "catalog projection worker count must be positive",
            ));
        }
        let source = self.source()?;
        let snapshot = match execution {
            Some(execution) => {
                let transaction = execution.read_transaction_context().map_err(|cause| {
                    CatalogApplicationError::from_domain(
                        CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete,
                        cause,
                    )
                })?;
                source.enumerate_in(page_size, &transaction).await
            }
            None => source.enumerate(page_size).await,
        }?;
        let reachable = snapshot
            .catalog_properties()?
            .into_iter()
            .map(|properties| properties.handle().clone())
            .collect::<BTreeSet<_>>();
        tracing::debug!(
            source_mode = snapshot.mode().as_str(),
            snapshot = snapshot.identity().short_digest(),
            catalogs = snapshot.identity().catalog_count(),
            "catalog desired-state snapshot enumerated"
        );
        *self.complete_reachable_catalogs.lock().map_err(|_| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                "catalog reachable snapshot lock is poisoned",
            )
        })? = Some(reachable);
        self.retire_projections_absent_from(source, &snapshot, execution)
            .await?;

        // Design: ADR-0131 (docs/adr/ADR-0131-server-composes-provider-role-bindings.md)
        // `worker_count` bounds the local submission fanout only. Each task
        // returns as soon as its exact key is owned by the scheduler; provider
        // materialization and any completion waiter remain background work.
        let mut submissions = tokio::task::JoinSet::new();
        let mode = snapshot.mode();
        for entry in snapshot.clone().into_entries() {
            if submissions.len() >= worker_count {
                Self::join_submission(&mut submissions).await?;
            }
            let projection = Arc::clone(self);
            submissions.spawn(async move {
                projection.materialize_entry(entry, mode);
            });
        }
        while !submissions.is_empty() {
            Self::join_submission(&mut submissions).await?;
        }
        Ok((snapshot, self.projection_counts()))
    }

    async fn join_submission(
        submissions: &mut tokio::task::JoinSet<()>,
    ) -> Result<(), CatalogApplicationError> {
        let completed = submissions.join_next().await.ok_or_else(|| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                "catalog projection submission task exited unexpectedly",
            )
        })?;
        completed.map_err(|error| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                format!("catalog projection submission task failed: {error}"),
            )
        })
    }

    /// Retires every local projection the snapshot no longer declares.
    ///
    /// This is the step that makes a snapshot total truth rather than additive
    /// seeds: a catalog absent from the source is not unmentioned, it is not
    /// wanted, so its projection has to go.
    ///
    /// A projection missing from the snapshot is not proof that its catalog is
    /// gone, though: `create_catalog` commits desired state and only then
    /// installs the projection, so a catalog created after the enumeration
    /// began is present locally and absent from the snapshot. Retiring on that
    /// alone made the statement right after CREATE EXTERNAL CATALOG fail with
    /// "unknown catalog" whenever a reconcile cycle straddled it — after a
    /// create that reported success.
    ///
    /// Re-reading each candidate closes the window rather than narrowing it:
    /// the projection can only exist because desired state was already
    /// committed, so a read issued after observing the projection sees it.
    async fn retire_projections_absent_from(
        &self,
        source: &CatalogDesiredStateSource,
        snapshot: &CatalogDesiredStateSnapshot,
        execution: Option<&dyn novarocks_state_store_api::StateStoreReadExecution>,
    ) -> Result<(), CatalogApplicationError> {
        let candidates = self
            .projections
            .lock()
            .map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog projection lock is poisoned",
                )
            })?
            .iter()
            .filter(|(instance_id, _)| !snapshot.wants(instance_id))
            .map(|(instance_id, projection)| (instance_id.clone(), projection.attachment_id()))
            .collect::<Vec<_>>();
        for (instance_id, attachment_id) in candidates {
            // Each independent re-read starts one transaction. Its Get/Range
            // operations keep this context; the prior enumeration cannot
            // consume this transaction's selected-provider time budget.
            let located = match execution {
                Some(execution) => match execution.read_transaction_context() {
                    Ok(transaction) => {
                        source
                            .locate_with_execution(&instance_id, Some(&transaction))
                            .await
                    }
                    Err(cause) => Err(CatalogApplicationError::from_domain(
                        CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete,
                        cause,
                    )),
                },
                None => source.locate_with_execution(&instance_id, None).await,
            };
            match located {
                Ok(Some(entry)) if entry.identity().as_uuid() == attachment_id => {}
                Ok(_) => self.retire_projection(&instance_id),
                // Keep serving and retry next cycle: the read failed, so
                // nothing was proven about desired state either way.
                Err(error) => tracing::warn!(
                    %error,
                    catalog = instance_id.as_str(),
                    "catalog desired-state re-read failed while retiring a projection absent from the snapshot",
                ),
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn enqueue_materialization(
        self: &Arc<Self>,
        entry: CatalogDesiredStateEntry,
        mode: CatalogDesiredStateSourceMode,
    ) -> Result<bool, CatalogApplicationError> {
        let (submitted, work) = self.submit_materialization(entry, mode, None)?;
        if let Some(work) = work {
            let projection = Arc::clone(self);
            self.runtime.spawn(async move {
                projection.run_materialization(work).await;
            });
        }
        Ok(submitted)
    }

    /// Submits an exact desired-state key to the one scheduler. A caller that
    /// must preserve CREATE's synchronous success contract receives a ticket
    /// for this same attempt; it never constructs a separate provider path.
    fn submit_materialization(
        &self,
        entry: CatalogDesiredStateEntry,
        mode: CatalogDesiredStateSourceMode,
        completion: Option<
            tokio::sync::oneshot::Sender<
                Result<CatalogRuntimeObservation, CatalogApplicationError>,
            >,
        >,
    ) -> Result<(bool, Option<MaterializationSubmission>), CatalogApplicationError> {
        let attachment_id = entry.identity().as_uuid();
        let instance_id = entry.config().instance_id().clone();
        let provider_id = entry.config().provider_id().clone();
        let raw_properties = entry.catalog_properties(mode)?;
        let factory = match self.control.role_factory(&provider_id) {
            Ok(factory) => factory,
            Err(error)
                if error.kind() == novarocks_spi::connector::ConnectorErrorKind::NotFound =>
            {
                return Ok((false, None));
            }
            Err(error) => return Err(connector_error(error)),
        };
        let properties = factory
            .normalize_and_validate(raw_properties)
            .map_err(|error| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::InvalidRequest,
                    error.to_string(),
                )
            })?;
        let identity = novarocks_spi::connector::CatalogScopeIdentity::new(
            provider_id.clone(),
            instance_id.clone(),
            attachment_id,
        );
        let key = properties.handle().clone();
        let context =
            MaterializationContext::new(Instant::now() + self.scheduler.config.attempt_timeout);
        let token = self.scheduler.next_token.fetch_add(1, Ordering::Relaxed);
        let factory = {
            let mut attempts = self.scheduler.attempts.lock().map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog projection scheduler lock is poisoned",
                )
            })?;
            // Reconcile checks the installed projection before entering this
            // scheduler, but CREATE can finish and clear its attempt between
            // that check and this submission. Recheck while holding the key's
            // scheduler lock so a stale reconcile submission cannot withdraw
            // the runtime CREATE just published.
            if completion.is_none()
                && self
                    .projections
                    .lock()
                    .map_err(|_| {
                        CatalogApplicationError::new(
                            CatalogApplicationErrorKind::Internal,
                            "catalog projection lock is poisoned",
                        )
                    })?
                    .get(&instance_id)
                    .is_some_and(|projection| {
                        matches!(
                            projection,
                            LocalProjection::Ready { attachment_id: installed_id, .. }
                                if *installed_id == attachment_id
                        )
                    })
                && self.control.has_current_definition(&key).unwrap_or(false)
            {
                return Ok((true, None));
            }
            if attempts.get(&key).is_some_and(|attempt| {
                attempt.attachment_id == attachment_id && attempt.instance_id == instance_id
            }) {
                if let Some(completion) = completion
                    && let Some(attempt) = attempts.get_mut(&key)
                {
                    attempt.completion_waiters.push(completion);
                }
                return Ok((true, None));
            }
            for attempt in attempts.values() {
                if attempt.instance_id == instance_id {
                    attempt.context.cancel();
                }
            }
            attempts
                .retain(|existing, _| existing.catalog_name() != &instance_id || existing == &key);
            attempts.insert(
                key.clone(),
                ProjectionAttempt {
                    instance_id: instance_id.clone(),
                    attachment_id,
                    token,
                    context: context.clone(),
                    completion_waiters: completion.into_iter().collect(),
                },
            );
            let mut owners = self.catalog_materializers.lock().map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog materialization owner lock is poisoned",
                )
            })?;
            if owners
                .current
                .get(&instance_id)
                .is_some_and(|owner| owner.identity != identity)
            {
                let old = owners.current.remove(&instance_id).expect("existing owner");
                owners.retain_draining(&old);
            }
            let owner = owners
                .current
                .entry(instance_id.clone())
                .or_insert_with(|| {
                    let opened = factory.open_catalog(identity.clone());
                    let reader = opened.activity_reader();
                    CatalogMaterializer {
                        identity,
                        factory: opened,
                        reader,
                    }
                });
            Arc::clone(&owner.factory)
        };
        self.mark_unavailable(
            &instance_id,
            attachment_id,
            &provider_id,
            "catalog desired-state runtime is being materialized",
        );
        Ok((
            true,
            Some(MaterializationSubmission {
                entry,
                provider_id,
                properties,
                factory,
                key,
                token,
                context,
            }),
        ))
    }

    fn token_is_current(&self, key: &CatalogHandle, token: u64) -> bool {
        self.scheduler
            .attempts
            .lock()
            .ok()
            .and_then(|attempts| attempts.get(key).map(|attempt| attempt.token == token))
            .unwrap_or(false)
    }

    fn replace_attempt_context(
        &self,
        key: &CatalogHandle,
        token: u64,
    ) -> Option<MaterializationContext> {
        let mut attempts = self.scheduler.attempts.lock().ok()?;
        let attempt = attempts.get_mut(key)?;
        if attempt.token != token {
            return None;
        }
        let context =
            MaterializationContext::new(Instant::now() + self.scheduler.config.attempt_timeout);
        attempt.context = context.clone();
        Some(context)
    }

    fn take_completion_waiters(
        &self,
        key: &CatalogHandle,
        token: u64,
    ) -> Vec<tokio::sync::oneshot::Sender<Result<CatalogRuntimeObservation, CatalogApplicationError>>>
    {
        if let Ok(mut attempts) = self.scheduler.attempts.lock()
            && attempts
                .get(key)
                .is_some_and(|attempt| attempt.token == token)
        {
            return attempts
                .get_mut(key)
                .map(|attempt| std::mem::take(&mut attempt.completion_waiters))
                .unwrap_or_default();
        }
        Vec::new()
    }

    fn clear_attempt(&self, key: &CatalogHandle, token: u64) {
        if let Ok(mut attempts) = self.scheduler.attempts.lock()
            && attempts
                .get(key)
                .is_some_and(|attempt| attempt.token == token)
        {
            attempts.remove(key);
        }
    }

    async fn run_materialization(&self, work: MaterializationSubmission) {
        let MaterializationSubmission {
            entry,
            provider_id,
            properties,
            factory,
            key,
            token,
            mut context,
        } = work;
        let mut backoff = self.scheduler.config.retry_initial_backoff;
        let mut attempts = 0_u64;
        loop {
            if !self.token_is_current(&key, token) || context.check_active().is_err() {
                return;
            }
            let Ok(permit) = Arc::clone(&self.scheduler.permits).acquire_owned().await else {
                return;
            };
            let result = tokio::time::timeout_at(
                tokio::time::Instant::from_std(context.deadline()),
                factory.materialize(properties.clone(), context.clone()),
            )
            .await
            .unwrap_or_else(|_| {
                Err(
                    novarocks_spi::connector::ConnectorMaterializationError::new(
                        novarocks_spi::connector::ConnectorMaterializationErrorClass::Timeout,
                        ConnectorMaterializationRetryDisposition::Transient,
                        "connector materialization deadline elapsed",
                    ),
                )
            });
            drop(permit);
            if !self.token_is_current(&key, token) {
                return;
            }
            match result {
                Ok(binding) => {
                    if self.token_is_current(&key, token) {
                        let completion = if let Err(error) = self.install_created(&entry, binding) {
                            self.mark_unavailable(
                                entry.config().instance_id(),
                                entry.identity().as_uuid(),
                                &provider_id,
                                error.to_string(),
                            );
                            Err(error)
                        } else {
                            self.admit_catalog(entry.config().instance_id())
                                .require_ready(entry.config().instance_id())
                        };
                        for waiter in self.take_completion_waiters(&key, token) {
                            let _ = waiter.send(completion.clone());
                        }
                        self.clear_attempt(&key, token);
                    }
                    return;
                }
                Err(error)
                    if error.disposition()
                        == ConnectorMaterializationRetryDisposition::UntilDefinitionChanges =>
                {
                    self.mark_unavailable(
                        entry.config().instance_id(),
                        entry.identity().as_uuid(),
                        &provider_id,
                        error.to_string(),
                    );
                    let completion = Err(materialization_error(&error));
                    for waiter in self.take_completion_waiters(&key, token) {
                        let _ = waiter.send(completion.clone());
                    }
                    // Keep the exact key as the suppression marker. A new
                    // desired definition has a new key and wakes immediately.
                    return;
                }
                Err(error) => {
                    self.mark_unavailable(
                        entry.config().instance_id(),
                        entry.identity().as_uuid(),
                        &provider_id,
                        error.to_string(),
                    );
                    let completion = Err(materialization_error(&error));
                    for waiter in self.take_completion_waiters(&key, token) {
                        let _ = waiter.send(completion.clone());
                    }
                    attempts = attempts.saturating_add(1);
                    let jitter = Duration::from_millis((token.wrapping_add(attempts) % 17) + 1);
                    tokio::time::sleep(backoff.saturating_add(jitter)).await;
                    if !self.token_is_current(&key, token) {
                        return;
                    }
                    backoff =
                        (backoff.saturating_mul(2)).min(self.scheduler.config.retry_max_backoff);
                    let Some(next) = self.replace_attempt_context(&key, token) else {
                        return;
                    };
                    context = next;
                }
            }
        }
    }

    /// Submits one located entry to the keyed scheduler without waiting for
    /// provider I/O. A complete desired-state snapshot is the bootstrap
    /// authority; individual materialization outcomes update projections
    /// asynchronously and cannot hold the frontend serving barrier closed.
    fn materialize_entry(
        self: &Arc<Self>,
        entry: CatalogDesiredStateEntry,
        mode: CatalogDesiredStateSourceMode,
    ) {
        let attachment_id = entry.identity().as_uuid();
        let instance_id = entry.config().instance_id().clone();
        let provider_id = entry.config().provider_id().clone();
        let installed = self
            .projections
            .lock()
            .map(|projections| {
                projections.get(&instance_id).is_some_and(|projection| {
                    matches!(
                        projection,
                        LocalProjection::Ready { attachment_id: installed_id, .. }
                            if *installed_id == attachment_id
                    )
                })
            })
            .unwrap_or(false)
            && entry
                .catalog_properties(mode)
                .ok()
                .is_some_and(|properties| {
                    self.control
                        .has_current_definition(properties.handle())
                        .unwrap_or(false)
                });
        if installed {
            return;
        }

        let (submitted, work) = match self.submit_materialization(entry.clone(), mode, None) {
            Ok(result) => result,
            Err(error) => {
                self.mark_unavailable(&instance_id, attachment_id, &provider_id, error.to_string());
                tracing::warn!(%error, catalog = instance_id.as_str(), "catalog remains unavailable after projection submission");
                return;
            }
        };
        if !submitted {
            self.mark_unavailable(
                &instance_id,
                attachment_id,
                &provider_id,
                "connector control role factory is not installed",
            );
            return;
        }
        if let Some(work) = work {
            let projection = Arc::clone(self);
            self.runtime.spawn(async move {
                projection.run_materialization(work).await;
            });
        }
    }

    /// Stops all local admission before retiring existing leases. Durable
    /// attachments remain unchanged, so a later authoritative reconcile can
    /// construct fresh generations after a freshness outage.
    pub fn unpublish_all(&self) {
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

    pub fn projection_count(&self) -> usize {
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

    pub fn projection_counts(&self) -> CatalogProjectionCounts {
        self.projections
            .lock()
            .map(|projections| {
                projections.values().fold(
                    CatalogProjectionCounts::default(),
                    |mut counts, projection| {
                        match projection {
                            LocalProjection::Ready { .. } => counts.ready += 1,
                            LocalProjection::Unavailable { .. } => counts.unavailable += 1,
                        }
                        counts
                    },
                )
            })
            .unwrap_or_default()
    }

    fn retire_projection(&self, instance_id: &ConnectorInstanceId) {
        if let Ok(mut attempts) = self.scheduler.attempts.lock() {
            if let Ok(mut owners) = self.catalog_materializers.lock()
                && let Some(old) = owners.current.remove(instance_id)
            {
                owners.retain_draining(&old);
            }
            attempts.retain(|_, attempt| {
                if &attempt.instance_id == instance_id {
                    attempt.context.cancel();
                    false
                } else {
                    true
                }
            });
        }
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

impl CatalogApplicationPort for CatalogApplicationService {
    fn require_catalog_mutation_admission(&self) -> Result<(), CatalogApplicationError> {
        self.sql_mutation_authority().map(|_| ())
    }

    fn catalog_activity_sources(
        &self,
    ) -> Result<Vec<novarocks_spi::connector::CatalogActivitySource>, CatalogApplicationError> {
        let mut owners = self.catalog_materializers.lock().map_err(|_| {
            CatalogApplicationError::new(
                CatalogApplicationErrorKind::Internal,
                "catalog activity owner lock is poisoned",
            )
        })?;
        owners
            .draining
            .retain(|(_, reader)| reader.strong_count() > 0);
        let mut sources = owners
            .current
            .values()
            .map(|owner| novarocks_spi::connector::CatalogActivitySource {
                identity: owner.identity.clone(),
                current: true,
                reader: owner.reader.clone(),
            })
            .collect::<Vec<_>>();
        sources.extend(owners.draining.iter().filter_map(|(identity, weak)| {
            weak.upgrade()
                .map(|reader| novarocks_spi::connector::CatalogActivitySource {
                    identity: identity.clone(),
                    current: false,
                    reader: Some(reader),
                })
        }));
        Ok(sources)
    }

    fn create_catalog(
        &self,
        command: CatalogCreateCommand,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        self.block_on_catalog(self.create_catalog_with_execution(command, None, None))
    }

    fn drop_catalog(&self, command: CatalogDropCommand) -> Result<(), CatalogApplicationError> {
        self.block_on_catalog(self.drop_catalog_with_execution(command, None, None))
    }

    fn create_catalog_in<'a>(
        &'a self,
        command: CatalogCreateCommand,
        execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
        receipt: &'a crate::CatalogMutationReceipt,
    ) -> futures::future::BoxFuture<'a, Result<CatalogRuntimeObservation, CatalogApplicationError>>
    {
        Box::pin(self.create_catalog_with_execution(command, Some(execution), Some(receipt)))
    }

    fn drop_catalog_in<'a>(
        &'a self,
        command: CatalogDropCommand,
        execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
        receipt: &'a crate::CatalogMutationReceipt,
    ) -> futures::future::BoxFuture<'a, Result<(), CatalogApplicationError>> {
        Box::pin(self.drop_catalog_with_execution(command, Some(execution), Some(receipt)))
    }

    fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission {
        if self.source.is_none() {
            return CatalogAdmission::Unavailable {
                reason: "this frontend has no configured catalog desired-state source".to_string(),
            };
        }
        self.observation(instance_id)
    }
}

impl CatalogApplicationService {
    async fn create_catalog_with_execution(
        &self,
        command: CatalogCreateCommand,
        execution: Option<&novarocks_state_store_api::StateStoreExecutionContext>,
        receipt: Option<&crate::CatalogMutationReceipt>,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        let repository = self.sql_mutation_authority()?;
        let (credential_bindings, provider_properties) =
            extract_catalog_credential_bindings(command.properties)?;
        if match execution {
            Some(execution) => repository.get_in(&command.instance_id, execution).await,
            None => repository.get(&command.instance_id).await,
        }
        .map_err(repository_error)?
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

        let provider_id = provider_id_from_properties(&provider_properties)?;
        let mut durable_properties = provider_properties;
        durable_properties.sort_by(|left, right| left.0.cmp(&right.0));
        let attachment = CatalogAttachment {
            attachment_id: Uuid::now_v7(),
            instance_id: command.instance_id,
            provider_id: provider_id.clone(),
            display_name: command.display_name,
            durable_properties,
            credential_bindings,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let candidate_entry = CatalogDesiredStateEntry::from_attachment(&attachment)?;
        let candidate_properties =
            candidate_entry.catalog_properties(CatalogDesiredStateSourceMode::DynamicStateStore)?;
        self.control
            .role_factory(&provider_id)
            .map_err(connector_error)?
            .normalize_and_validate(candidate_properties)
            .map_err(|error| materialization_error(&error))?;
        let created = match execution {
            Some(execution) => match receipt {
                Some(receipt) => {
                    repository
                        .create_in_receiving(attachment, execution, receipt)
                        .await
                }
                None => repository.create_in(attachment, execution).await,
            },
            None => repository.create(attachment).await,
        }
        .map_err(repository_error)?;
        // CREATE uses the exact same keyed scheduler as reconcile. The ticket
        // observes that attempt's first completion and does not create a
        // second materialization policy or provider side effect path.
        let entry = CatalogDesiredStateEntry::from_attachment(&created.attachment)?;
        let (completion, ticket) = tokio::sync::oneshot::channel();
        let (submitted, work) = self.submit_materialization(
            entry,
            CatalogDesiredStateSourceMode::DynamicStateStore,
            Some(completion),
        )?;
        if !submitted {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "connector control role factory is unavailable for this provider",
            ));
        }
        async {
            if let Some(work) = work {
                self.run_materialization(work).await;
            }
            ticket.await.map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Unavailable,
                    "catalog materialization attempt stopped before completion",
                )
            })?
        }
        .await
    }

    async fn drop_catalog_with_execution(
        &self,
        command: CatalogDropCommand,
        execution: Option<&novarocks_state_store_api::StateStoreExecutionContext>,
        receipt: Option<&crate::CatalogMutationReceipt>,
    ) -> Result<(), CatalogApplicationError> {
        let repository = self.sql_mutation_authority()?;
        let Some(existing) = match execution {
            Some(execution) => repository.get_in(&command.instance_id, execution).await,
            None => repository.get(&command.instance_id).await,
        }
        .map_err(repository_error)?
        else {
            return if command.if_exists {
                Ok(())
            } else {
                Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::NotFound,
                    "catalog attachment was not found",
                ))
            };
        };
        // Ordering, not atomicity: the reference check runs here, before the
        // delete and outside it, and the delete below is a single-family
        // transaction on the catalog attachment record alone.
        //
        // The check used to be a scan inside that transaction, which read as a
        // cross-family serializability fence against MV DDL. It is now an
        // operational check that can miss — a wiped or unreadable MV
        // Accelerator observes nothing, and MV DDL elsewhere can land right
        // after the observation. What escapes it is bounded to an MV whose
        // catalog is gone, which the MV side already refuses through its
        // unavailable/fail-closed paths rather than publishing anything wrong
        // to the lake.
        match execution {
            Some(execution) => {
                repository
                    .observe_references_before_drop_in(
                        self.reference_reader.as_ref(),
                        &command.instance_id,
                        256,
                        execution,
                    )
                    .await
            }
            None => {
                repository
                    .observe_references_before_drop(
                        self.reference_reader.as_ref(),
                        &command.instance_id,
                        256,
                    )
                    .await
            }
        }
        .map_err(repository_error)?;
        match execution {
            Some(execution) => match receipt {
                Some(receipt) => {
                    repository
                        .drop_exact_in_receiving(existing.clone(), execution, receipt)
                        .await
                }
                None => repository.drop_exact_in(existing.clone(), execution).await,
            },
            None => repository.drop_exact(existing.clone()).await,
        }
        .map_err(repository_error)?;
        self.retire_projection(&command.instance_id);
        // Durable deletion is authoritative. A local generation can be absent
        // or already retiring; either case converges through reconciliation.
        Ok(())
    }
}

type CatalogCredentialBindingExtraction = (Vec<CatalogCredentialBinding>, Vec<(String, String)>);

fn extract_catalog_credential_bindings(
    properties: Vec<(String, String)>,
) -> Result<CatalogCredentialBindingExtraction, CatalogApplicationError> {
    let mut credential_fields = BTreeMap::new();
    let mut provider_properties = Vec::with_capacity(properties.len());
    for (key, value) in properties {
        let normalized = key.to_ascii_lowercase();
        let recognized = matches!(
            normalized.as_str(),
            "credential.catalog-control.consumer-role"
                | "credential.catalog-control.mode"
                | "credential.catalog-control.name"
                | "credential.catalog-control.generation"
                | "credential.object-store-data.consumer-role"
                | "credential.object-store-data.mode"
                | "credential.object-store-data.name"
                | "credential.object-store-data.generation"
                | "credential.object-store-metadata.consumer-role"
                | "credential.object-store-metadata.mode"
                | "credential.object-store-metadata.name"
                | "credential.object-store-metadata.generation"
                | "credential.data-credential-vending.consumer-role"
                | "credential.data-credential-vending.mode"
                | "credential.data-credential-vending.name"
                | "credential.data-credential-vending.generation"
        );
        if recognized {
            if credential_fields.insert(normalized, value).is_some() {
                return Err(invalid_credential_property(format!(
                    "duplicate catalog credential property: {key}"
                )));
            }
        } else if normalized.starts_with("credential.") {
            return Err(invalid_credential_property(format!(
                "unknown catalog credential property: {key}"
            )));
        } else {
            provider_properties.push((key, value));
        }
    }

    let mut bindings = Vec::new();
    if let Some(binding) = take_credential_binding(
        &mut credential_fields,
        "credential.catalog-control",
        CatalogCredentialPurpose::CatalogControl,
    )? {
        bindings.push(binding);
    }
    if let Some(binding) = take_credential_binding(
        &mut credential_fields,
        "credential.object-store-data",
        CatalogCredentialPurpose::ObjectStoreData,
    )? {
        bindings.push(binding);
    }
    if let Some(binding) = take_credential_binding(
        &mut credential_fields,
        "credential.object-store-metadata",
        CatalogCredentialPurpose::ObjectStoreMetadata,
    )? {
        bindings.push(binding);
    }
    // The execution node's own catalog identity (CAD-1 D1). Declared like any
    // other binding: the property names a reference, and the material lives in
    // that role's local registry.
    if let Some(binding) = take_credential_binding(
        &mut credential_fields,
        "credential.data-credential-vending",
        CatalogCredentialPurpose::DataCredentialVending,
    )? {
        bindings.push(binding);
    }
    debug_assert!(credential_fields.is_empty());
    let bindings = canonicalize_catalog_credential_bindings(bindings)
        .map_err(|error| invalid_credential_property(error.to_string()))?;
    Ok((bindings, provider_properties))
}

fn take_credential_binding(
    fields: &mut BTreeMap<String, String>,
    prefix: &str,
    purpose: CatalogCredentialPurpose,
) -> Result<Option<CatalogCredentialBinding>, CatalogApplicationError> {
    let role = fields.remove(&format!("{prefix}.consumer-role"));
    let mode = fields.remove(&format!("{prefix}.mode"));
    let name = fields.remove(&format!("{prefix}.name"));
    let generation = fields.remove(&format!("{prefix}.generation"));
    if role.is_none() && mode.is_none() && name.is_none() && generation.is_none() {
        return Ok(None);
    }
    let role = match role.as_deref() {
        Some("frontend") => CredentialConsumerRole::Frontend,
        Some("backend") => CredentialConsumerRole::Backend,
        Some("frontend-and-backend") => CredentialConsumerRole::FrontendAndBackend,
        Some(_) => {
            return Err(invalid_credential_property(
                "unknown catalog credential consumer role",
            ));
        }
        None => {
            return Err(invalid_credential_property(
                "catalog credential binding requires consumer-role",
            ));
        }
    };
    let mode = match mode.as_deref() {
        Some("static") => {
            let name = name.as_deref().ok_or_else(|| {
                invalid_credential_property("static catalog credential binding requires name")
            })?;
            let generation = generation.as_deref().ok_or_else(|| {
                invalid_credential_property("static catalog credential binding requires generation")
            })?;
            CatalogCredentialMode::Static(
                StaticCredentialReference::try_new(name, generation)
                    .map_err(|error| invalid_credential_property(error.to_string()))?,
            )
        }
        Some("node-local") => {
            let name = name.as_deref().ok_or_else(|| {
                invalid_credential_property("node-local catalog credential binding requires name")
            })?;
            let generation = generation.as_deref().ok_or_else(|| {
                invalid_credential_property(
                    "node-local catalog credential binding requires generation",
                )
            })?;
            CatalogCredentialMode::NodeLocal(
                NodeLocalCredentialReference::try_new(name, generation)
                    .map_err(|error| invalid_credential_property(error.to_string()))?,
            )
        }
        Some("vended") => {
            if name.is_some() || generation.is_some() {
                return Err(invalid_credential_property(
                    "vended catalog credential binding forbids name and generation",
                ));
            }
            CatalogCredentialMode::Vended
        }
        Some(_) => {
            return Err(invalid_credential_property(
                "unknown catalog credential mode",
            ));
        }
        None => {
            return Err(invalid_credential_property(
                "catalog credential binding requires mode",
            ));
        }
    };
    CatalogCredentialBinding::try_new(purpose, role, mode)
        .map(Some)
        .map_err(|error| invalid_credential_property(error.to_string()))
}

fn invalid_credential_property(message: impl Into<String>) -> CatalogApplicationError {
    CatalogApplicationError::new(CatalogApplicationErrorKind::InvalidRequest, message)
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
    CatalogApplicationError::from_domain(kind, error)
}

fn connector_error(error: novarocks_spi::connector::ConnectorError) -> CatalogApplicationError {
    use novarocks_spi::connector::ConnectorErrorKind;

    let kind = match error.kind() {
        ConnectorErrorKind::Interrupted => CatalogApplicationErrorKind::Interrupted,
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
    CatalogApplicationError::from_domain(kind, error.clone())
}

fn materialization_error(
    error: &novarocks_spi::connector::ConnectorMaterializationError,
) -> CatalogApplicationError {
    use novarocks_spi::connector::ConnectorMaterializationErrorClass;

    let kind = match error.class() {
        ConnectorMaterializationErrorClass::Interrupted => CatalogApplicationErrorKind::Interrupted,
        ConnectorMaterializationErrorClass::InvalidDefinition => {
            CatalogApplicationErrorKind::InvalidRequest
        }
        ConnectorMaterializationErrorClass::Authentication
        | ConnectorMaterializationErrorClass::Unavailable
        | ConnectorMaterializationErrorClass::Timeout
        | ConnectorMaterializationErrorClass::ResourceExhausted
        | ConnectorMaterializationErrorClass::Cancelled => CatalogApplicationErrorKind::Unavailable,
        ConnectorMaterializationErrorClass::Internal => CatalogApplicationErrorKind::Internal,
    };
    CatalogApplicationError::from_domain(kind, error.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_catalog_extracts_node_local_sources_without_retyping_static() {
        let properties = [
            ("credential.object-store-data.consumer-role", "backend"),
            ("credential.object-store-data.mode", "node-local"),
            ("credential.object-store-data.name", "worker-source"),
            ("credential.object-store-data.generation", "blue"),
            ("warehouse", "s3://warehouse/"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        let (bindings, provider) = extract_catalog_credential_bindings(properties).unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(
            bindings[0].node_local_reference().unwrap().name(),
            "worker-source"
        );
        assert!(bindings[0].static_reference().is_none());
        assert_eq!(
            provider,
            vec![("warehouse".to_string(), "s3://warehouse/".to_string())]
        );
    }

    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn reconcile_borrows_one_context_for_enumeration_and_each_independent_reread() {
        use novarocks_state_store_api::{StateStoreExecutionContext, StateStoreReadExecution};
        struct ReadRequests {
            base: StateStoreExecutionContext,
            issued: Mutex<Vec<Instant>>,
        }
        impl StateStoreReadExecution for ReadRequests {
            fn read_transaction_context(
                &self,
            ) -> Result<
                StateStoreExecutionContext,
                novarocks_state_store_api::StateStoreExecutionFailure,
            > {
                let mut issued = self.issued.lock().unwrap();
                let fixed = self.base.with_deadline(
                    self.base.deadline() - Duration::from_secs(10 - issued.len() as u64),
                );
                issued.push(fixed.deadline());
                Ok(fixed)
            }
        }
        let (store, _port, absolute, reads, _) =
            CatalogAttachmentRepository::original_read_fixture();
        let repository = CatalogAttachmentRepository::open_in(
            store,
            novarocks_state_store_runtime::StateStoreRunPolicy::default(),
            &absolute,
        )
        .await
        .unwrap();
        reads.lock().unwrap().clear();
        let control = Arc::new(
            ConnectorControlHost::with_role_factories(vec![Arc::new(FailingRoleFactory {
                disposition: ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                calls: Arc::new(AtomicUsize::new(0)),
            })])
            .unwrap(),
        );
        let service = Arc::new(CatalogApplicationService::new(
            CatalogDesiredStateSource::dynamic_state_store(repository),
            control,
            Arc::new(TestRuntimePublisher),
            Handle::current(),
            Arc::new(TestReferenceReader),
        ));
        for name in ["absent-a", "absent-b"] {
            service.projections.lock().unwrap().insert(
                ConnectorInstanceId::parse(name).unwrap(),
                LocalProjection::Unavailable {
                    attachment_id: Uuid::now_v7(),
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    reason: "original candidate must be reread".into(),
                },
            );
        }
        let request = ReadRequests {
            base: absolute,
            issued: Mutex::new(Vec::new()),
        };
        service.reconcile_snapshot_in(1, 1, &request).await.unwrap();
        let issued = request.issued.lock().unwrap();
        assert_eq!(
            issued.len(),
            3,
            "one enumeration and two independent read transactions"
        );
        assert_eq!(*reads.lock().unwrap(), *issued);
        assert!(issued.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(service.projections.lock().unwrap().is_empty());
    }

    #[test]
    fn catalog_boundary_retains_connector_author_facts_and_occurrence() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_spi::connector::{ConnectorError, ConnectorIdentityError};
        let raw = ConnectorIdentityError::InvalidWriteTargetOrdinal;
        let expected = raw.failure_fact();
        let connector = ConnectorError::from(raw);
        let error = connector_error(connector.clone());
        assert_eq!(error.failure_fact(), expected);
        let source = error.source_failure().expect("original domain source");
        assert_eq!(source.failure_fact(), expected);
        assert_eq!(source.downcast_ref::<ConnectorError>(), Some(&connector));
        let clone = error.clone();
        assert_eq!(clone.source_failure(), Some(source));
    }

    fn vending_properties() -> Vec<(String, String)> {
        [
            ("credential.catalog-control.consumer-role", "frontend"),
            ("credential.catalog-control.mode", "static"),
            ("credential.catalog-control.name", "coordinator"),
            ("credential.catalog-control.generation", "v1"),
            (
                "credential.data-credential-vending.consumer-role",
                "backend",
            ),
            ("credential.data-credential-vending.mode", "static"),
            ("credential.data-credential-vending.name", "executor"),
            ("credential.data-credential-vending.generation", "v1"),
            ("warehouse", "s3://warehouse"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
    }

    #[test]
    fn an_execution_node_vending_binding_is_declared_like_any_other() {
        // CAD-1 D1 reaches a deployment through catalog properties: the property
        // names a reference, and the material stays in that role's own registry.
        let (bindings, provider) =
            extract_catalog_credential_bindings(vending_properties()).expect("bindings");
        assert_eq!(bindings.len(), 2);
        assert!(bindings.iter().any(|binding| binding.purpose()
            == CatalogCredentialPurpose::DataCredentialVending
            && binding.consumer_role() == CredentialConsumerRole::Backend));
        assert_eq!(
            provider,
            vec![("warehouse".to_string(), "s3://warehouse".to_string())]
        );
    }

    #[test]
    fn the_two_catalog_identities_must_not_name_one_credential() {
        // CAD-1 D9's residual check, reached through the property surface an
        // operator actually types. Without it the identity split degrades at
        // deployment time with nothing to notice it.
        let mut properties = vending_properties();
        for (key, value) in properties.iter_mut() {
            if key == "credential.data-credential-vending.name" {
                *value = "coordinator".to_string();
            }
        }
        assert!(extract_catalog_credential_bindings(properties).is_err());
    }

    #[test]
    fn an_unknown_credential_property_is_still_refused() {
        let mut properties = vending_properties();
        properties.push((
            "credential.data-credential-vending.principal".to_string(),
            "x".to_string(),
        ));
        assert!(extract_catalog_credential_bindings(properties).is_err());
    }

    use futures::FutureExt;

    struct TestRuntimePublisher;

    impl CatalogRuntimePublisherSink for TestRuntimePublisher {
        fn publish_catalog_runtime(
            &self,
            _observation: CatalogRuntimeObservation,
        ) -> Result<(), CatalogApplicationError> {
            Ok(())
        }

        fn unpublish_catalog_runtime(
            &self,
            _instance_id: &ConnectorInstanceId,
            _generation: u64,
        ) -> Result<(), CatalogApplicationError> {
            Ok(())
        }
    }

    struct TestReferenceReader;

    impl CatalogReferenceReader for TestReferenceReader {
        fn observe_references<'a>(
            &'a self,
            _store: &'a dyn novarocks_state_store_api::StateStore,
            _instance_id: &'a ConnectorInstanceId,
            _page_size: usize,
        ) -> BoxFuture<'a, Result<Option<&'static str>, String>> {
            Box::pin(async { Ok(None) })
        }
    }

    #[derive(Clone)]
    struct FailingRoleFactory {
        disposition: ConnectorMaterializationRetryDisposition,
        calls: Arc<AtomicUsize>,
    }

    impl novarocks_spi::connector::ConnectorControlRoleBindingFactory for FailingRoleFactory {
        fn provider_id(&self) -> novarocks_spi::connector::ConnectorProviderId {
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static provider ID")
        }

        fn normalize_and_validate(
            &self,
            properties: novarocks_spi::connector::CatalogProperties,
        ) -> Result<
            NormalizedCatalogProperties,
            novarocks_spi::connector::ConnectorMaterializationError,
        > {
            NormalizedCatalogProperties::try_new(properties).map_err(|detail| {
                novarocks_spi::connector::ConnectorMaterializationError::new(
                    novarocks_spi::connector::ConnectorMaterializationErrorClass::InvalidDefinition,
                    ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    detail,
                )
            })
        }

        fn open_catalog(
            &self,
            _identity: novarocks_spi::connector::CatalogScopeIdentity,
        ) -> Arc<dyn novarocks_spi::connector::ProviderCatalogControlFactory> {
            Arc::new(self.clone())
        }
    }

    impl novarocks_spi::connector::ProviderCatalogControlFactory for FailingRoleFactory {
        fn materialize(
            &self,
            _properties: NormalizedCatalogProperties,
            _context: MaterializationContext,
        ) -> futures::future::BoxFuture<
            'static,
            Result<
                novarocks_spi::connector::ConnectorControlRoleBinding,
                novarocks_spi::connector::ConnectorMaterializationError,
            >,
        > {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let disposition = self.disposition;
            async move {
                Err(
                    novarocks_spi::connector::ConnectorMaterializationError::new(
                        novarocks_spi::connector::ConnectorMaterializationErrorClass::Unavailable,
                        disposition,
                        "injected projection failure",
                    ),
                )
            }
            .boxed()
        }
    }

    fn scheduler_entry(instance: &str) -> CatalogDesiredStateEntry {
        CatalogDesiredStateEntry::from_attachment(&CatalogAttachment {
            attachment_id: Uuid::now_v7(),
            instance_id: ConnectorInstanceId::parse(instance).expect("instance id"),
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
            display_name: instance.to_owned(),
            durable_properties: vec![("type".to_owned(), "iceberg".to_owned())],
            credential_bindings: Vec::new(),
            created_at_ms: 1,
        })
        .expect("desired-state entry")
    }

    fn scheduler_port(factory: Arc<FailingRoleFactory>) -> Arc<CatalogApplicationService> {
        let control = Arc::new(
            ConnectorControlHost::with_role_factories(vec![factory]).expect("role factory host"),
        );
        Arc::new(CatalogApplicationService::unavailable(
            control,
            Arc::new(TestRuntimePublisher),
            tokio::runtime::Handle::current(),
            Arc::new(TestReferenceReader),
        ))
    }

    #[tokio::test]
    async fn scheduler_single_flight_retries_transient_and_drop_clears_exact_token() {
        let calls = Arc::new(AtomicUsize::new(0));
        let port = scheduler_port(Arc::new(FailingRoleFactory {
            disposition: ConnectorMaterializationRetryDisposition::Transient,
            calls: Arc::clone(&calls),
        }));
        let entry = scheduler_entry("catalog.scheduler");
        assert!(
            port.enqueue_materialization(
                entry.clone(),
                CatalogDesiredStateSourceMode::DynamicStateStore
            )
            .expect("first enqueue")
        );
        assert!(
            port.enqueue_materialization(
                entry.clone(),
                CatalogDesiredStateSourceMode::DynamicStateStore
            )
            .expect("same key enqueue")
        );
        tokio::time::sleep(Duration::from_millis(260)).await;
        assert!(
            calls.load(Ordering::SeqCst) >= 2,
            "transient failure retries"
        );
        port.retire_projection(entry.config().instance_id());
        let key = entry
            .catalog_properties(CatalogDesiredStateSourceMode::DynamicStateStore)
            .expect("properties")
            .handle()
            .clone();
        assert!(!port.token_is_current(&key, 1), "drop clears old token");
    }

    #[tokio::test]
    async fn scheduler_suppresses_permanent_failure_until_exact_definition_changes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let port = scheduler_port(Arc::new(FailingRoleFactory {
            disposition: ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
            calls: Arc::clone(&calls),
        }));
        let entry = scheduler_entry("catalog.permanent");
        port.enqueue_materialization(
            entry.clone(),
            CatalogDesiredStateSourceMode::DynamicStateStore,
        )
        .expect("enqueue");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        port.enqueue_materialization(entry, CatalogDesiredStateSourceMode::DynamicStateStore)
            .expect("same permanent key remains suppressed");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// Counts materializations and hands back a live control binding.
    #[derive(Clone)]
    struct CountingRoleFactory {
        calls: Arc<AtomicUsize>,
    }

    impl novarocks_spi::connector::ConnectorControlRoleBindingFactory for CountingRoleFactory {
        fn provider_id(&self) -> novarocks_spi::connector::ConnectorProviderId {
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static provider ID")
        }

        fn normalize_and_validate(
            &self,
            properties: novarocks_spi::connector::CatalogProperties,
        ) -> Result<
            NormalizedCatalogProperties,
            novarocks_spi::connector::ConnectorMaterializationError,
        > {
            NormalizedCatalogProperties::try_new(properties).map_err(|detail| {
                novarocks_spi::connector::ConnectorMaterializationError::new(
                    novarocks_spi::connector::ConnectorMaterializationErrorClass::InvalidDefinition,
                    ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    detail,
                )
            })
        }

        fn open_catalog(
            &self,
            _identity: novarocks_spi::connector::CatalogScopeIdentity,
        ) -> Arc<dyn novarocks_spi::connector::ProviderCatalogControlFactory> {
            Arc::new(self.clone())
        }
    }

    impl novarocks_spi::connector::ProviderCatalogControlFactory for CountingRoleFactory {
        fn materialize(
            &self,
            properties: NormalizedCatalogProperties,
            _context: MaterializationContext,
        ) -> futures::future::BoxFuture<
            'static,
            Result<
                novarocks_spi::connector::ConnectorControlRoleBinding,
                novarocks_spi::connector::ConnectorMaterializationError,
            >,
        > {
            use futures::FutureExt;

            // Distinct per call: reusing an incarnation would trip the
            // retired-generation guard and hide a resubmission behind an
            // unrelated failure.
            let incarnation = u8::try_from(self.calls.fetch_add(1, Ordering::SeqCst) % 250)
                .expect("incarnation")
                + 1;
            async move {
                let control = crate::test_support::test_control_binding_for(
                    properties.handle().catalog_name().clone(),
                    incarnation,
                )
                .with_catalog_properties(properties.as_catalog_properties().clone())
                .map_err(novarocks_spi::connector::ConnectorMaterializationError::from)?;
                novarocks_spi::connector::ConnectorControlRoleBinding::try_new(
                    properties,
                    Arc::new(control),
                    None,
                    None,
                )
                .map_err(novarocks_spi::connector::ConnectorMaterializationError::from)
            }
            .boxed()
        }
    }

    /// A reconcile that finds the catalog it already serves must do nothing.
    ///
    /// Every reconcile round is a complete enumeration now, and the periodic
    /// sweep runs one whether or not anything changed. Without this short
    /// circuit each sweep would resubmit every catalog to its provider and
    /// mint a fresh control generation for it — a provider call per catalog
    /// per round, for no change, and a generation churn that briefly makes
    /// healthy catalogs unavailable.
    ///
    /// The guard has to stay conditional, so the second half asserts the case
    /// it must *not* suppress: once the projection is gone, the same entry is
    /// materialized again.
    #[tokio::test]
    async fn re_materializing_an_already_serving_catalog_is_a_no_op() {
        let calls = Arc::new(AtomicUsize::new(0));
        let control = Arc::new(
            ConnectorControlHost::with_role_factories(vec![Arc::new(CountingRoleFactory {
                calls: Arc::clone(&calls),
            })])
            .expect("role factory host"),
        );
        let port = Arc::new(CatalogApplicationService::unavailable(
            Arc::clone(&control),
            Arc::new(TestRuntimePublisher),
            tokio::runtime::Handle::current(),
            Arc::new(TestReferenceReader),
        ));
        let entry = scheduler_entry("catalog.steady");
        let instance_id = entry.config().instance_id().clone();

        port.materialize_entry(
            entry.clone(),
            CatalogDesiredStateSourceMode::DynamicStateStore,
        );
        // `observation` rather than `admit_catalog`: the latter refuses first
        // on "this frontend has no configured desired-state source", which is
        // a property of this source-free fixture and not of the projection
        // state the suppression guard actually reads.
        tokio::time::timeout(Duration::from_secs(2), async {
            while !matches!(port.observation(&instance_id), CatalogAdmission::Ready(_)) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the first materialization installs a Ready projection");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // A reconcile that observed NotReady before CREATE completed can
        // arrive at the scheduler after CREATE installed Ready and cleared its
        // attempt. The scheduler must reject that stale submission itself.
        let (submitted, work) = port
            .submit_materialization(
                entry.clone(),
                CatalogDesiredStateSourceMode::DynamicStateStore,
                None,
            )
            .expect("submit the stale reconcile observation");
        assert!(submitted);
        assert!(work.is_none());
        assert!(matches!(
            port.observation(&instance_id),
            CatalogAdmission::Ready(_)
        ));

        for _ in 0..5 {
            port.materialize_entry(
                entry.clone(),
                CatalogDesiredStateSourceMode::DynamicStateStore,
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "an unchanged, already-serving catalog must not be resubmitted once per round"
        );
        assert!(
            matches!(port.observation(&instance_id), CatalogAdmission::Ready(_)),
            "and it must still be serving"
        );

        // The suppression is a function of what is installed, not of what has
        // been seen: with the projection retired, the same entry materializes.
        port.retire_projection(&instance_id);
        port.materialize_entry(entry, CatalogDesiredStateSourceMode::DynamicStateStore);
        tokio::time::timeout(Duration::from_secs(2), async {
            while calls.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a retired projection must be materialized again");
    }

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

    #[test]
    fn create_catalog_extracts_closed_typed_credential_properties() {
        let (bindings, properties) = extract_catalog_credential_bindings(vec![
            ("type".to_string(), "iceberg".to_string()),
            (
                "credential.object-store-data.generation".to_string(),
                "blue".to_string(),
            ),
            (
                "credential.catalog-control.consumer-role".to_string(),
                "frontend".to_string(),
            ),
            (
                "credential.object-store-data.consumer-role".to_string(),
                "backend".to_string(),
            ),
            (
                "credential.catalog-control.mode".to_string(),
                "static".to_string(),
            ),
            (
                "credential.object-store-data.mode".to_string(),
                "static".to_string(),
            ),
            (
                "credential.catalog-control.name".to_string(),
                "rest-control".to_string(),
            ),
            (
                "credential.object-store-data.name".to_string(),
                "warehouse-data".to_string(),
            ),
            (
                "credential.catalog-control.generation".to_string(),
                "blue".to_string(),
            ),
            (
                "credential.object-store-metadata.consumer-role".to_string(),
                "frontend".to_string(),
            ),
            (
                "credential.object-store-metadata.mode".to_string(),
                "static".to_string(),
            ),
            (
                "credential.object-store-metadata.name".to_string(),
                "warehouse-metadata".to_string(),
            ),
            (
                "credential.object-store-metadata.generation".to_string(),
                "blue".to_string(),
            ),
        ])
        .expect("typed credential bindings");
        assert_eq!(
            properties,
            vec![("type".to_string(), "iceberg".to_string())]
        );
        assert_eq!(bindings.len(), 3);
        assert_eq!(
            bindings[0].purpose(),
            CatalogCredentialPurpose::CatalogControl
        );
        assert_eq!(
            bindings[1].purpose(),
            CatalogCredentialPurpose::ObjectStoreData
        );
        assert_eq!(bindings[1].consumer_role(), CredentialConsumerRole::Backend);
        assert_eq!(
            bindings[2].purpose(),
            CatalogCredentialPurpose::ObjectStoreMetadata
        );
        assert_eq!(
            bindings[2].consumer_role(),
            CredentialConsumerRole::Frontend
        );
    }

    #[test]
    fn create_catalog_rejects_unknown_partial_and_conflicting_credential_properties() {
        for properties in [
            vec![(
                "credential.object-store-data.typo".to_string(),
                "static".to_string(),
            )],
            vec![(
                "credential.object-store-data.mode".to_string(),
                "static".to_string(),
            )],
            vec![
                (
                    "credential.object-store-data.consumer-role".to_string(),
                    "frontend-and-backend".to_string(),
                ),
                (
                    "credential.object-store-data.mode".to_string(),
                    "static".to_string(),
                ),
                (
                    "credential.object-store-data.name".to_string(),
                    "warehouse-data".to_string(),
                ),
                (
                    "credential.object-store-data.generation".to_string(),
                    "blue".to_string(),
                ),
            ],
            vec![
                (
                    "credential.object-store-data.consumer-role".to_string(),
                    "frontend-and-backend".to_string(),
                ),
                (
                    "credential.object-store-data.mode".to_string(),
                    "vended".to_string(),
                ),
                (
                    "credential.object-store-data.name".to_string(),
                    "forbidden".to_string(),
                ),
            ],
            vec![
                (
                    "credential.object-store-data.consumer-role".to_string(),
                    "frontend-and-backend".to_string(),
                ),
                (
                    "CREDENTIAL.OBJECT-STORE-DATA.CONSUMER-ROLE".to_string(),
                    "frontend-and-backend".to_string(),
                ),
                (
                    "credential.object-store-data.mode".to_string(),
                    "vended".to_string(),
                ),
            ],
            vec![
                (
                    "credential.object-store-metadata.consumer-role".to_string(),
                    "frontend".to_string(),
                ),
                (
                    "credential.object-store-metadata.mode".to_string(),
                    "vended".to_string(),
                ),
            ],
        ] {
            assert_eq!(
                extract_catalog_credential_bindings(properties)
                    .expect_err("invalid credential properties must fail admission")
                    .kind(),
                CatalogApplicationErrorKind::InvalidRequest
            );
        }
    }

    #[test]
    fn create_catalog_allows_an_explicitly_binding_free_local_definition() {
        let (bindings, properties) =
            extract_catalog_credential_bindings(vec![("type".to_string(), "local".to_string())])
                .expect("local catalog definition");
        assert!(bindings.is_empty());
        assert_eq!(properties, vec![("type".to_string(), "local".to_string())]);
    }
    struct DrainingControlResolver {
        entered: tokio::sync::Notify,
        gate: (std::sync::Mutex<bool>, std::sync::Condvar),
        old_calls: std::sync::atomic::AtomicUsize,
        new_calls: std::sync::atomic::AtomicUsize,
    }
    impl DrainingControlResolver {
        fn release(&self) {
            *self.gate.0.lock().unwrap() = true;
            self.gate.1.notify_all();
        }
    }
    struct ReleaseDrainingControl(Arc<DrainingControlResolver>);
    impl Drop for ReleaseDrainingControl {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    impl novarocks_connector_iceberg::access_binding::IcebergCredentialResolver
        for DrainingControlResolver
    {
        fn resolve_object_store_static(
            &self,
            _: &StaticCredentialReference,
        ) -> Result<novarocks_fs::ObjectStoreSecretMaterial, novarocks_spi::connector::ConnectorError>
        {
            Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "test owns only catalog-control identity",
            ))
        }
        fn resolve_catalog_control(
            &self,
            reference: &StaticCredentialReference,
        ) -> Result<
            novarocks_connector_iceberg::access_binding::IcebergRestAuthMaterial,
            novarocks_spi::connector::ConnectorError,
        > {
            use std::sync::atomic::Ordering::SeqCst;
            assert_eq!(reference.name(), "control");
            match reference.generation() {
                "old" => {
                    self.old_calls.fetch_add(1, SeqCst);
                    self.entered.notify_one();
                    drop(
                        self.gate
                            .1
                            .wait_timeout_while(
                                self.gate.0.lock().unwrap(),
                                Duration::from_secs(20),
                                |released| !*released,
                            )
                            .unwrap(),
                    );
                }
                "new" => {
                    self.new_calls.fetch_add(1, SeqCst);
                }
                _ => panic!("unexpected test identity generation"),
            }
            Ok(
                novarocks_connector_iceberg::access_binding::IcebergRestAuthMaterial::Bearer {
                    token: novarocks_fs::SecretValue::new("local-test-token"),
                },
            )
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn activity_draining_scope_survives_wrapper_deadline_until_actual_worker_exit() {
        use novarocks_connector_iceberg::{
            IcebergControlRoleBindingFactory, access_binding::IcebergReadBinding,
            resources::IcebergMetadataResources,
        };
        use novarocks_spi::connector::CatalogActivityPhase as P;
        use std::sync::atomic::Ordering::SeqCst;
        let resolver = Arc::new(DrainingControlResolver {
            entered: tokio::sync::Notify::new(),
            gate: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
            old_calls: std::sync::atomic::AtomicUsize::new(0),
            new_calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let _release = ReleaseDrainingControl(resolver.clone());
        let handle = tokio::runtime::Handle::current();
        let fs = novarocks_fs::FsAccessResources::new(
            Arc::new(novarocks_fs::ObjectStoreProviderPool::new(Default::default()).unwrap()),
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle.clone())),
        );
        let access = IcebergReadBinding::with_metadata_credential_resolver(fs, resolver.clone());
        let role = Arc::new(IcebergControlRoleBindingFactory::new(
            IcebergMetadataResources::new(access, handle.clone()),
            std::num::NonZeroUsize::new(2).unwrap(),
        ));
        let control = Arc::new(ConnectorControlHost::with_role_factories(vec![role]).unwrap());
        let mut service = CatalogApplicationService::unavailable(
            control.clone(),
            Arc::new(TestRuntimePublisher),
            handle,
            Arc::new(TestReferenceReader),
        );
        service.scheduler = ProjectionScheduler::new(
            CatalogMaterializationConfig::try_new(
                Duration::from_secs(2),
                Duration::from_millis(1),
                Duration::from_millis(1),
                2,
            )
            .unwrap(),
        );
        let port = Arc::new(service);
        let instance = ConnectorInstanceId::parse("catalog.draining").unwrap();
        let old_id = Uuid::now_v7();
        let new_id = Uuid::now_v7();
        assert_ne!(old_id, new_id);
        let entry = |identity, generation: &str| {
            CatalogDesiredStateEntry::new(
                crate::CatalogSourceEntryIdentity::new(identity),
                crate::CatalogLogicalConfig::try_new(
                    instance.clone(),
                    ConnectorProviderId::parse("iceberg").unwrap(),
                    "Draining",
                    vec![
                        ("iceberg.catalog.type".into(), "rest".into()),
                        ("uri".into(), "http://127.0.0.1:1".into()),
                    ],
                    vec![
                        CatalogCredentialBinding::try_new(
                            CatalogCredentialPurpose::CatalogControl,
                            CredentialConsumerRole::Frontend,
                            CatalogCredentialMode::Static(
                                StaticCredentialReference::try_new("control", generation).unwrap(),
                            ),
                        )
                        .unwrap(),
                    ],
                    1,
                )
                .unwrap(),
            )
        };
        let mode = CatalogDesiredStateSourceMode::DynamicStateStore;
        let (_, work) = port
            .submit_materialization(entry(old_id, "old"), mode, None)
            .unwrap();
        let task_port = port.clone();
        let old_wrapper =
            tokio::spawn(async move { task_port.run_materialization(work.unwrap()).await });
        tokio::time::timeout(Duration::from_secs(5), resolver.entered.notified())
            .await
            .expect("old real worker entered");
        let (_, work) = port
            .submit_materialization(entry(new_id, "new"), mode, None)
            .unwrap();
        port.run_materialization(work.unwrap()).await;
        let new_epoch = {
            let lease = control
                .acquire_current(&instance)
                .expect("new scope progresses while old worker blocks");
            lease.binding().incarnation()
        };
        // Replacement cancels the old context, but the existing wrapper waits
        // for its original deadline. It is not manually aborted in this test.
        tokio::time::timeout(Duration::from_secs(4), old_wrapper)
            .await
            .unwrap()
            .unwrap();
        fn snapshots(
            port: &CatalogApplicationService,
        ) -> Vec<(
            Uuid,
            bool,
            novarocks_spi::connector::CatalogActivitySnapshot,
        )> {
            port.catalog_activity_sources()
                .unwrap()
                .into_iter()
                .map(|source| {
                    let snapshot = source.reader.as_ref().unwrap().snapshot();
                    (source.identity.source_entry_id(), source.current, snapshot)
                })
                .collect()
        }
        let rows = snapshots(&port);
        assert_eq!(rows.len(), 2);
        let old = rows.iter().find(|row| row.0 == old_id).unwrap();
        assert!(!old.1);
        assert_eq!(old.2.phases[P::MaterializationWorker as usize].inflight, 1);
        assert_eq!(old.2.phases[P::Bridge as usize].inflight, 1);
        assert_eq!(old.2.generations.len(), 1);
        assert_eq!(old.2.generations[0].generation, None);
        let new = rows.iter().find(|row| row.0 == new_id).unwrap();
        assert!(new.1);
        assert!(new.2.phases.iter().all(|phase| phase.inflight == 0));
        assert_eq!(
            new.2.generations[0].generation.unwrap().as_bytes(),
            &new_epoch.to_bytes()
        );
        assert_eq!(resolver.old_calls.load(SeqCst), 1);
        assert_eq!(resolver.new_calls.load(SeqCst), 1);
        drop(rows);
        resolver.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let rows = snapshots(&port);
                if rows.len() == 1 && rows[0].0 == new_id && rows[0].1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("old actual exit removes the draining weak reader");
        assert_eq!(
            control
                .acquire_current(&instance)
                .unwrap()
                .binding()
                .incarnation(),
            new_epoch
        );
    }

    // Lifecycle integration uses the production Iceberg role factory and real
    // generation leases. Local Hadoop construction performs no remote I/O;
    // test-owned runtimes and warehouses are released after every lease exits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalog_owner_spans_real_iceberg_generations_and_freshness_outage() {
        use novarocks_connector_iceberg::IcebergControlRoleBindingFactory;
        use novarocks_connector_iceberg::access_binding::IcebergReadBinding;
        use novarocks_connector_iceberg::resources::IcebergMetadataResources;
        use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

        let warehouse = tempfile::tempdir().expect("warehouse");
        let runtime = tokio::runtime::Handle::current();
        let access = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.clone())),
        );
        let factory = Arc::new(IcebergControlRoleBindingFactory::new(
            IcebergMetadataResources::new(access, runtime.clone()),
            std::num::NonZeroUsize::new(1).unwrap(),
        ));
        let control = Arc::new(ConnectorControlHost::with_role_factories(vec![factory]).unwrap());
        let port = Arc::new(CatalogApplicationService::unavailable(
            Arc::clone(&control),
            Arc::new(TestRuntimePublisher),
            runtime,
            Arc::new(TestReferenceReader),
        ));
        let instance = ConnectorInstanceId::parse("catalog.lifecycle").unwrap();
        let identity = Uuid::now_v7();
        let entry = |identity, credential_generation| {
            CatalogDesiredStateEntry::new(
                crate::CatalogSourceEntryIdentity::new(identity),
                crate::CatalogLogicalConfig::try_new(
                    instance.clone(),
                    ConnectorProviderId::parse("iceberg").unwrap(),
                    "Lifecycle",
                    vec![(
                        "iceberg.catalog.warehouse".to_string(),
                        warehouse.path().display().to_string(),
                    )],
                    vec![
                        CatalogCredentialBinding::try_new(
                            CatalogCredentialPurpose::CatalogControl,
                            CredentialConsumerRole::Frontend,
                            CatalogCredentialMode::Static(
                                StaticCredentialReference::try_new(
                                    "control",
                                    credential_generation,
                                )
                                .unwrap(),
                            ),
                        )
                        .unwrap(),
                    ],
                    1,
                )
                .unwrap(),
            )
        };
        let first_entry = entry(identity, "v1");
        let mode = CatalogDesiredStateSourceMode::DynamicStateStore;
        let (_, first_work) = port
            .submit_materialization(first_entry.clone(), mode, None)
            .unwrap();
        let first_work = first_work.expect("G1 submission");
        let scoped_owner = Arc::clone(&first_work.factory);
        port.run_materialization(first_work).await;
        let first = control.acquire_current(&instance).expect("real G1 lease");
        let first_runtime = first.binding().control_runtime_id();
        let sources = port.catalog_activity_sources().unwrap();
        assert_eq!(sources.len(), 1);
        let first_activity = sources[0]
            .reader
            .as_ref()
            .expect("Iceberg reader")
            .snapshot();
        assert_eq!(first_activity.source_entry_id, identity);
        assert_eq!(first_activity.generations.len(), 1);
        assert_eq!(
            first_activity.generations[0].generation.unwrap().as_bytes(),
            &first.binding().incarnation().to_bytes()
        );
        drop(sources);

        let first_properties = first.binding().catalog_properties().unwrap().clone();
        assert_eq!(
            first_properties.credential_bindings(),
            first_entry.config().credential_bindings()
        );

        // A temporary loss of freshness retires G1, but does not retire the
        // logical domain that G1's already-admitted operation still needs.
        port.unpublish_all();
        assert!(control.acquire_current(&instance).is_err());
        let (_, second_work) = port
            .submit_materialization(entry(identity, "v2"), mode, None)
            .unwrap();
        let second_work = second_work.expect("G2 submission");
        assert!(Arc::ptr_eq(&scoped_owner, &second_work.factory));
        assert_ne!(
            first_properties,
            *second_work.properties.as_catalog_properties()
        );
        port.run_materialization(second_work).await;
        let second = control.acquire_current(&instance).expect("real G2 lease");
        assert_ne!(first_runtime, second.binding().control_runtime_id());
        let sources = port.catalog_activity_sources().unwrap();
        assert_eq!(sources.len(), 1);
        let both = sources[0].reader.as_ref().unwrap().snapshot();
        assert_eq!(both.generations.len(), 2);
        for lease in [&first, &second] {
            assert!(
                both.generations
                    .iter()
                    .any(|row| row.generation.unwrap().as_bytes()
                        == &lease.binding().incarnation().to_bytes())
            );
        }
        drop(sources);

        assert_eq!(
            first.binding().catalog_properties().unwrap(),
            &first_properties
        );
        assert_ne!(
            first
                .binding()
                .catalog_properties()
                .unwrap()
                .credential_bindings(),
            second
                .binding()
                .catalog_properties()
                .unwrap()
                .credential_bindings()
        );

        // Configuration/credential replacement alone also changes the runtime
        // while retaining the same catalog scope, without requiring an outage.
        let (_, third_work) = port
            .submit_materialization(entry(identity, "v3"), mode, None)
            .unwrap();
        let third_work = third_work.expect("changed definition requires G3");
        assert!(Arc::ptr_eq(&scoped_owner, &third_work.factory));
        port.run_materialization(third_work).await;
        assert_eq!(first.binding().control_runtime_id(), first_runtime);

        // A genuinely new attachment separates domains even with the same
        // name and a still-live exact lease from the old attachment.
        let (_, replacement) = port
            .submit_materialization(entry(Uuid::now_v7(), "v3"), mode, None)
            .unwrap();
        let replacement = replacement.expect("new attachment");
        assert!(!Arc::ptr_eq(&scoped_owner, &replacement.factory));
        port.run_materialization(replacement).await;
        let sources = port.catalog_activity_sources().unwrap();
        assert_eq!(
            sources.len(),
            2,
            "replacement cannot hide the retained old scope"
        );
        assert_eq!(sources.iter().filter(|s| s.current).count(), 1);
        assert!(
            sources
                .iter()
                .any(|s| !s.current && s.identity.source_entry_id() == identity)
        );
        let current = sources.iter().find(|s| s.current).unwrap();
        assert_ne!(current.identity.source_entry_id(), identity);
        assert_eq!(
            current
                .reader
                .as_ref()
                .unwrap()
                .snapshot()
                .generations
                .len(),
            1
        );
        drop(sources);
        drop(first);
        drop(second);
        port.retire_projection(&instance);
        assert!(port.catalog_materializers.lock().unwrap().is_empty());
    }

    mod generation_expiry_ref {
        use super::*;
        use novarocks_connector_iceberg::access_binding::{
            IcebergCredentialResolver, IcebergReadBinding, IcebergRestAuthMaterial,
        };
        use novarocks_connector_iceberg::{IcebergControlRoleBindingFactory, iceberg};
        use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
        use novarocks_spi::connector::{
            CatalogActivityPhase, ConnectorCatalogMutationLease, ConnectorCatalogMutationOperation,
            ConnectorCatalogMutationResolver, ConnectorError, ConnectorErrorKind,
            ConnectorMetadataMaintenanceExecuteRequest, ConnectorMetadataMaintenanceOperation,
            ConnectorMetadataMaintenanceResolver, ConnectorMutationFailureKind,
            ConnectorMutationOperationId, ConnectorRefAction, ConnectorRefKind,
            ConnectorRequestContext, ConnectorStopOwner, ConnectorTableHandle,
            ConnectorTableIdentity, CreateOrReplacePolicy, ExternalMutationEffect,
            ExternalMutationOutcome,
        };
        use serde_json::{Value, json};
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        use std::sync::{Condvar, atomic::AtomicBool};

        struct Identity;
        impl IcebergCredentialResolver for Identity {
            fn resolve_object_store_static(
                &self,
                _: &StaticCredentialReference,
            ) -> Result<novarocks_fs::ObjectStoreSecretMaterial, ConnectorError> {
                Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "No object-store identity in this local fixture",
                ))
            }
            fn resolve_catalog_control(
                &self,
                reference: &StaticCredentialReference,
            ) -> Result<IcebergRestAuthMaterial, ConnectorError> {
                Ok(IcebergRestAuthMaterial::Bearer {
                    token: novarocks_fs::SecretValue::new(format!(
                        "generation-{}",
                        reference.generation()
                    )),
                })
            }
        }
        #[derive(Clone)]
        struct Request {
            method: String,
            path: String,
            authorization: String,
            body: Value,
        }
        struct State {
            tables: BTreeMap<String, Value>,
            records: Vec<Request>,
            expiry_entered: bool,
        }
        struct Server {
            address: std::net::SocketAddr,
            state: Arc<Mutex<State>>,
            release: Arc<(Mutex<bool>, Condvar)>,
            stop: Arc<AtomicBool>,
            acceptor: Option<std::thread::JoinHandle<()>>,
            connections: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
        }
        impl Server {
            fn start(tables: BTreeMap<String, Value>) -> Self {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                listener.set_nonblocking(true).unwrap();
                let address = listener.local_addr().unwrap();
                let state = Arc::new(Mutex::new(State {
                    tables,
                    records: vec![],
                    expiry_entered: false,
                }));
                let release = Arc::new((Mutex::new(false), Condvar::new()));
                let stop = Arc::new(AtomicBool::new(false));
                let connections = Arc::new(Mutex::new(vec![]));
                let (s, r, done, joins) = (
                    state.clone(),
                    release.clone(),
                    stop.clone(),
                    connections.clone(),
                );
                let acceptor = std::thread::spawn(move || {
                    while !done.load(Ordering::SeqCst) {
                        match listener.accept() {
                            Ok((stream, _)) => {
                                let (s, r) = (s.clone(), r.clone());
                                joins
                                    .lock()
                                    .unwrap()
                                    .push(std::thread::spawn(move || Self::respond(stream, s, r)));
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                std::thread::sleep(Duration::from_millis(1))
                            }
                            Err(error) => panic!("fixture accept failed: {error}"),
                        }
                    }
                });
                Self {
                    address,
                    state,
                    release,
                    stop,
                    acceptor: Some(acceptor),
                    connections,
                }
            }
            fn respond(
                mut stream: TcpStream,
                state: Arc<Mutex<State>>,
                release: Arc<(Mutex<bool>, Condvar)>,
            ) {
                // Accepted sockets can inherit the listener's nonblocking mode
                // on macOS. Each owned connection uses blocking, bounded I/O.
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = vec![];
                let head_end = loop {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).unwrap();
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        break index + 4;
                    }
                    assert!(bytes.len() < 65536);
                };
                let head = String::from_utf8(bytes[..head_end].to_vec()).unwrap();
                let mut first = head.lines().next().unwrap().split_whitespace();
                let method = first.next().unwrap().to_owned();
                let path = first.next().unwrap().split('?').next().unwrap().to_owned();
                let headers: BTreeMap<_, _> = head
                    .lines()
                    .skip(1)
                    .filter_map(|line| line.split_once(':'))
                    .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_owned()))
                    .collect();
                let length: usize = headers
                    .get("content-length")
                    .map(|v| v.parse().unwrap())
                    .unwrap_or(0);
                while bytes.len() - head_end < length {
                    let mut buffer = [0; 4096];
                    let count = stream.read(&mut buffer).unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buffer[..count]);
                }
                let body = if length == 0 {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes[head_end..head_end + length]).unwrap()
                };
                let request = Request {
                    method: method.clone(),
                    path: path.clone(),
                    authorization: headers.get("authorization").cloned().unwrap_or_default(),
                    body,
                };
                let expiry = method == "POST"
                    && request.body["updates"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|update| update["action"] == "remove-snapshot-ref");
                {
                    let mut state = state.lock().unwrap();
                    state.records.push(request.clone());
                    if expiry {
                        state.expiry_entered = true;
                    }
                }
                if expiry {
                    let (lock, changed) = &*release;
                    let mut released = lock.lock().unwrap();
                    while !*released {
                        released = changed.wait(released).unwrap();
                    }
                }
                let response = if path == "/v1/config" {
                    json!({"defaults":{},"overrides":{}})
                } else {
                    let name = path.rsplit('/').next().unwrap();
                    let mut state = state.lock().unwrap();
                    let response = state
                        .tables
                        .get_mut(name)
                        .expect("only declared tables are requested");
                    if method == "POST" {
                        let previous = response["metadata-location"].as_str().unwrap().to_owned();
                        let previous_time =
                            response["metadata"]["last-updated-ms"].as_i64().unwrap();
                        let version = response["metadata"]["metadata-log"]
                            .as_array()
                            .unwrap()
                            .len()
                            + 2;
                        for update in request.body["updates"].as_array().unwrap() {
                            match update["action"].as_str().unwrap() {
                                "remove-snapshot-ref" => {
                                    response["metadata"]["refs"]
                                        .as_object_mut()
                                        .unwrap()
                                        .remove(update["ref-name"].as_str().unwrap());
                                }
                                "set-snapshot-ref" => {
                                    response["metadata"]["refs"]
                                        [update["ref-name"].as_str().unwrap()] = json!({"snapshot-id":update["snapshot-id"],"type":update["type"]});
                                }
                                "set-properties" => {
                                    for (key, value) in update["updates"].as_object().unwrap() {
                                        response["metadata"]["properties"][key] = value.clone();
                                    }
                                }
                                other => panic!("unexpected publication update {other}"),
                            }
                        }
                        if expiry {
                            // A refreshed success may include a later writer's head.
                            // The waiting G2 must use 3, never its pre-lock head 2.
                            let mut snapshot = response["metadata"]["snapshots"][1].clone();
                            snapshot["snapshot-id"] = json!(3);
                            snapshot["parent-snapshot-id"] = json!(2);
                            snapshot["sequence-number"] = json!(3);
                            snapshot["manifest-list"] = json!(format!(
                                "{}/list-3.avro",
                                response["metadata"]["location"].as_str().unwrap()
                            ));
                            response["metadata"]["snapshots"]
                                .as_array_mut()
                                .unwrap()
                                .push(snapshot);
                            response["metadata"]["current-snapshot-id"] = json!(3);
                            response["metadata"]["last-sequence-number"] = json!(3);
                            response["metadata"]["refs"]["main"]["snapshot-id"] = json!(3);
                        }
                        response["metadata"]["metadata-log"]
                            .as_array_mut()
                            .unwrap()
                            .push(json!({"timestamp-ms":previous_time,"metadata-file":previous}));
                        response["metadata"]["last-updated-ms"] = json!(previous_time + 1);
                        let next = format!(
                            "{}/v{version}.metadata.json",
                            response["metadata"]["location"].as_str().unwrap()
                        );
                        std::fs::write(&next, serde_json::to_vec(&response["metadata"]).unwrap())
                            .unwrap();
                        response["metadata-location"] = json!(next);
                    }
                    response.clone()
                };
                let body = serde_json::to_vec(&response).unwrap();
                let _ = write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).and_then(|_| stream.write_all(&body));
            }
            fn release(&self) {
                let (lock, changed) = &*self.release;
                *lock.lock().unwrap() = true;
                changed.notify_all();
            }
            fn posts(&self, name: &str) -> Vec<Request> {
                self.state
                    .lock()
                    .unwrap()
                    .records
                    .iter()
                    .filter(|r| r.method == "POST" && r.path.ends_with(&format!("/tables/{name}")))
                    .cloned()
                    .collect()
            }
        }
        impl Drop for Server {
            fn drop(&mut self) {
                self.release();
                self.stop.store(true, Ordering::SeqCst);
                self.acceptor.take().unwrap().join().unwrap();
                let mut panicked = false;
                for connection in self.connections.lock().unwrap().drain(..) {
                    panicked |= connection.join().is_err();
                }
                if !std::thread::panicking() {
                    assert!(!panicked, "fixture connection thread panicked");
                }
            }
        }
        fn context(stop: &ConnectorStopOwner, budget: Duration) -> ConnectorRequestContext {
            ConnectorRequestContext::try_new(
                Instant::now() + budget,
                stop.view(),
                64 * 1024,
                1024 * 1024,
            )
            .unwrap()
        }
        async fn until(mut predicate: impl FnMut() -> bool) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !predicate() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("controlled stage did not arrive");
        }
        fn tag(
            lease: ConnectorCatalogMutationLease,
            instance: ConnectorInstanceId,
            table: &str,
            name: &str,
            context: ConnectorRequestContext,
        ) -> impl FnOnce() -> Result<
            ExternalMutationOutcome<novarocks_spi::connector::ConnectorCatalogMutationReceipt>,
            ConnectorError,
        > + Send
        + 'static {
            let operation = ConnectorCatalogMutationOperation::AlterRef {
                table: ConnectorTableIdentity {
                    instance_id: instance,
                    namespace: "db".into(),
                    table: table.into(),
                },
                action: ConnectorRefAction::Create {
                    kind: ConnectorRefKind::Tag,
                    name: name.into(),
                    snapshot_id: None,
                    policy: CreateOrReplacePolicy::FailIfExists,
                    expected_table_uuid: None,
                },
            };
            move || lease.execute_operation(ConnectorMutationOperationId::new(), operation, context)
        }
        fn assert_stopped(
            outcome: Result<
                ExternalMutationOutcome<novarocks_spi::connector::ConnectorCatalogMutationReceipt>,
                ConnectorError,
            >,
            expected: ConnectorMutationFailureKind,
        ) {
            let ExternalMutationOutcome::KnownUncommitted { failure, .. } = outcome.unwrap() else {
                panic!("waiting mutation must be definitely undispatched")
            };
            assert_eq!(failure.kind(), expected);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn real_service_generations_serialize_expiry_and_refs_with_fresh_authenticated_observations()
         {
            let warehouse = tempfile::tempdir().unwrap();
            let io = iceberg::io::FileIO::new_with_fs();
            let mut tables = BTreeMap::new();
            for (name, uuid) in [("t", Uuid::from_u128(1)), ("other", Uuid::from_u128(2))] {
                let location = warehouse.path().canonicalize().unwrap().join(name);
                std::fs::create_dir_all(&location).unwrap();
                for id in 1..=3 {
                    iceberg::spec::ManifestListWriter::v2(
                        io.new_output(location.join(format!("list-{id}.avro")).to_str().unwrap())
                            .unwrap()
                            .writer()
                            .await
                            .unwrap(),
                        id,
                        (id > 1).then_some(id - 1),
                        id,
                    )
                    .close()
                    .await
                    .unwrap();
                }
                let schema = iceberg::spec::Schema::builder()
                    .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                        1,
                        "id",
                        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                    ))])
                    .build()
                    .unwrap();
                let metadata = iceberg::spec::TableMetadataBuilder::new(
                    schema,
                    iceberg::spec::PartitionSpec::unpartition_spec(),
                    iceberg::spec::SortOrder::unsorted_order(),
                    location.display().to_string(),
                    iceberg::spec::FormatVersion::V2,
                    Default::default(),
                )
                .unwrap()
                .build()
                .unwrap()
                .metadata;
                let mut metadata = serde_json::to_value(metadata).unwrap();
                metadata["table-uuid"] = json!(uuid);
                metadata["last-updated-ms"] = json!(1700000000000_i64);
                metadata["last-sequence-number"] = json!(2);
                metadata["current-snapshot-id"] = json!(2);
                metadata["snapshots"] = json!([
                    {"snapshot-id":1,"sequence-number":1,"timestamp-ms":1,"manifest-list":location.join("list-1.avro"),"schema-id":0,"summary":{"operation":"append"}},
                    {"snapshot-id":2,"parent-snapshot-id":1,"sequence-number":2,"timestamp-ms":2,"manifest-list":location.join("list-2.avro"),"schema-id":0,"summary":{"operation":"append"}}
                ]);
                metadata["refs"] = json!({"main":{"snapshot-id":2,"type":"branch"},"expired":{"snapshot-id":1,"type":"tag","max-ref-age-ms":1}});
                metadata["metadata-log"] = json!([]);
                let _: iceberg::spec::TableMetadata =
                    serde_json::from_value(metadata.clone()).unwrap();
                let path = location.join("v1.metadata.json");
                std::fs::write(&path, serde_json::to_vec(&metadata).unwrap()).unwrap();
                tables.insert(
                    name.to_owned(),
                    json!({"metadata-location":path,"metadata":metadata}),
                );
            }
            let server = Server::start(tables);
            let runtime = tokio::runtime::Handle::current();
            let fs = novarocks_fs::FsAccessResources::new(
                Arc::new(novarocks_fs::ObjectStoreProviderPool::new(Default::default()).unwrap()),
                FsAccessResolver::new(),
                Arc::new(TokioFileIoRuntime::new(runtime.clone())),
                Arc::new(TokioFileTaskSpawner::new(runtime.clone())),
            );
            let access =
                IcebergReadBinding::with_metadata_credential_resolver(fs, Arc::new(Identity));
            let factory = Arc::new(IcebergControlRoleBindingFactory::new(
                novarocks_connector_iceberg::resources::IcebergMetadataResources::new(
                    access,
                    runtime.clone(),
                ),
                std::num::NonZeroUsize::new(1).unwrap(),
            ));
            let control =
                Arc::new(ConnectorControlHost::with_role_factories(vec![factory]).unwrap());
            let port = Arc::new(CatalogApplicationService::unavailable(
                control.clone(),
                Arc::new(TestRuntimePublisher),
                runtime,
                Arc::new(TestReferenceReader),
            ));
            let instance = ConnectorInstanceId::parse("catalog.generation-race").unwrap();
            let identity = Uuid::now_v7();
            let entry = |version| {
                CatalogDesiredStateEntry::new(
                    crate::CatalogSourceEntryIdentity::new(identity),
                    crate::CatalogLogicalConfig::try_new(
                        instance.clone(),
                        ConnectorProviderId::parse("iceberg").unwrap(),
                        "Generation race",
                        vec![
                            ("iceberg.catalog.type".into(), "rest".into()),
                            ("uri".into(), format!("http://{}", server.address)),
                            (
                                "iceberg.catalog.warehouse".into(),
                                warehouse.path().display().to_string(),
                            ),
                        ],
                        vec![
                            CatalogCredentialBinding::try_new(
                                CatalogCredentialPurpose::CatalogControl,
                                CredentialConsumerRole::Frontend,
                                CatalogCredentialMode::Static(
                                    StaticCredentialReference::try_new("control", version).unwrap(),
                                ),
                            )
                            .unwrap(),
                        ],
                        1,
                    )
                    .unwrap(),
                )
            };
            let mode = CatalogDesiredStateSourceMode::DynamicStateStore;
            let (_, work) = port
                .submit_materialization(entry("v1"), mode, None)
                .unwrap();
            port.run_materialization(work.unwrap()).await;
            let first = control
                .acquire_current_metadata_maintenance(&instance)
                .unwrap();
            // This request-only handle freezes names, never a guessed UUID/base.
            // The real provider plans and executes against its fresh catalog facts.
            let handle = ConnectorTableHandle::try_new(instance.clone(), bytes::Bytes::from_static(br#"{"namespace":"db","table":"t","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#)).unwrap();
            let stop = ConnectorStopOwner::new();
            let planning = context(&stop, Duration::from_secs(20));
            let plan = first
                .plan_operation(
                    ConnectorMutationOperationId::new(),
                    ConnectorMetadataMaintenanceOperation::expire_table_versions(
                        handle,
                        Some(i64::MAX),
                        Some(2),
                    )
                    .unwrap(),
                    planning.clone(),
                )
                .await
                .unwrap();
            let first_id = first.control_runtime_id();
            let mut execution = first
                .begin_execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, planning).unwrap(),
                )
                .unwrap();
            drop(first);
            let expiry = tokio::spawn(async move { execution.wait().await });
            until(|| server.state.lock().unwrap().expiry_entered).await;
            assert_eq!(server.posts("t").len(), 1);
            port.unpublish_all();
            let (_, work) = port
                .submit_materialization(entry("v2"), mode, None)
                .unwrap();
            port.run_materialization(work.unwrap()).await;
            let second_generation = control.acquire_current(&instance).unwrap();
            assert_ne!(second_generation.binding().control_runtime_id(), first_id);
            let second_epoch =
                Uuid::from_bytes(second_generation.binding().incarnation().to_bytes());
            let second = control.acquire_current_mutation(&instance).unwrap();
            let waiting_count = || {
                port.catalog_activity_sources()
                    .unwrap()
                    .iter()
                    .filter_map(|source| source.reader.as_ref())
                    .flat_map(|reader| reader.snapshot().generations)
                    .filter(|generation| generation.generation == Some(second_epoch))
                    .map(|generation| {
                        generation
                            .phases
                            .iter()
                            .find(|phase| phase.phase == CatalogActivityPhase::TableCoordination)
                            .unwrap()
                            .inflight
                    })
                    .sum::<u64>()
            };
            let waiting = tokio::task::spawn_blocking(tag(
                second.clone(),
                instance.clone(),
                "t",
                "after",
                context(&stop, Duration::from_secs(20)),
            ));
            until(|| waiting_count() == 1).await;
            assert!(!waiting.is_finished());
            assert_eq!(
                server.posts("t").len(),
                1,
                "G2 must not dispatch through G1's live lock"
            );
            let other = tokio::task::spawn_blocking(tag(
                second.clone(),
                instance.clone(),
                "other",
                "parallel",
                context(&stop, Duration::from_secs(10)),
            ));
            assert!(matches!(
                other.await.unwrap().unwrap(),
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    ..
                }
            ));
            assert_eq!(server.posts("other").len(), 1);
            assert_eq!(server.posts("t").len(), 1);
            let cancelled = ConnectorStopOwner::new();
            let cancel_task = tokio::task::spawn_blocking(tag(
                second.clone(),
                instance.clone(),
                "t",
                "cancelled",
                context(&cancelled, Duration::from_secs(10)),
            ));
            until(|| waiting_count() == 2).await;
            cancelled.request_stop();
            assert_stopped(
                cancel_task.await.unwrap(),
                ConnectorMutationFailureKind::Cancelled,
            );
            let deadline_task = tokio::task::spawn_blocking(tag(
                second,
                instance.clone(),
                "t",
                "expired-waiter",
                context(&stop, Duration::from_millis(200)),
            ));
            until(|| waiting_count() == 2).await;
            assert_stopped(
                deadline_task.await.unwrap(),
                ConnectorMutationFailureKind::DeadlineExceeded,
            );
            assert_eq!(waiting_count(), 1);
            assert_eq!(server.posts("t").len(), 1);
            let release_index = server.state.lock().unwrap().records.len();
            server.release();
            assert!(matches!(
                expiry.await.unwrap().unwrap(),
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    ..
                }
            ));
            assert!(matches!(
                waiting.await.unwrap().unwrap(),
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    ..
                }
            ));
            assert_eq!(waiting_count(), 0);
            let posts = server.posts("t");
            assert_eq!(posts.len(), 2);
            assert_eq!(posts[0].authorization, "Bearer generation-v1");
            assert_eq!(posts[1].authorization, "Bearer generation-v2");
            let set_ref = posts[1].body["updates"]
                .as_array()
                .unwrap()
                .iter()
                .find(|update| update["action"] == "set-snapshot-ref")
                .unwrap();
            assert_eq!(set_ref["ref-name"], "after");
            assert_eq!(
                set_ref["snapshot-id"], 3,
                "G2 must lower from the post-lock fresh head"
            );
            let state = server.state.lock().unwrap();
            assert!(
                state.records[release_index..]
                    .iter()
                    .any(|r| r.method == "GET"
                        && r.path.ends_with("/tables/t")
                        && r.authorization == "Bearer generation-v2")
            );
            assert_eq!(
                state.tables["t"]["metadata"]["refs"]["after"]["snapshot-id"],
                3
            );
            assert!(
                state.tables["t"]["metadata"]["refs"]
                    .get("cancelled")
                    .is_none()
            );
            assert!(
                state.tables["t"]["metadata"]["refs"]
                    .get("expired-waiter")
                    .is_none()
            );
            drop(state);
            // Server Drop releases any gate and joins every actual HTTP thread.
            drop(server);
        }
    }
}
