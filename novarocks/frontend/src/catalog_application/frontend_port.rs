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
//! for the global one and [`FrontendCatalogApplicationPort::materialize_entry`]
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

use super::desired_state::{
    CatalogDesiredStateEntry, CatalogDesiredStateSnapshot, CatalogDesiredStateSource,
    CatalogDesiredStateSourceMode,
};
use super::{
    CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind, CatalogApplicationPort,
    CatalogCreateCommand, CatalogDropCommand, CatalogRuntimeObservation,
    CatalogRuntimePublisherSink,
};
use crate::mv::domain::repository::{MvRepositoryError, MvRepositoryErrorKind};
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
    ConnectorControlResolver, ConnectorInstanceId, ConnectorProviderId, CredentialConsumerRole,
    StaticCredentialReference, canonicalize_catalog_credential_bindings,
};
use novarocks_spi::connector::{
    ConnectorMaterializationRetryDisposition, MaterializationContext, NormalizedCatalogProperties,
};
use tokio::runtime::{Handle, RuntimeFlavor};
use uuid::Uuid;

use crate::catalog_attachment::{
    CatalogAttachment, CatalogAttachmentError, CatalogAttachmentErrorKind,
    CatalogAttachmentRepository, CatalogAttachmentWakeupSignal,
};
use crate::connector::ConnectorControlHost;

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

struct MaterializationSubmission {
    entry: CatalogDesiredStateEntry,
    provider_id: ConnectorProviderId,
    properties: NormalizedCatalogProperties,
    factory: Arc<dyn novarocks_spi::connector::ConnectorControlRoleBindingFactory>,
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
pub(crate) struct CatalogProjectionCounts {
    pub(crate) ready: usize,
    pub(crate) unavailable: usize,
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
pub struct FrontendCatalogApplicationPort {
    source: Option<CatalogDesiredStateSource>,
    control: Arc<ConnectorControlHost>,
    runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
    runtime: Handle,
    projections: Mutex<BTreeMap<ConnectorInstanceId, LocalProjection>>,
    complete_reachable_catalogs: Mutex<Option<BTreeSet<CatalogHandle>>>,
    next_generation: AtomicU64,
    scheduler: ProjectionScheduler,
}

impl FrontendCatalogApplicationPort {
    pub fn unavailable(
        control: Arc<ConnectorControlHost>,
        runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
        runtime: Handle,
    ) -> Self {
        Self {
            source: None,
            control,
            runtime_publisher,
            runtime,
            projections: Mutex::new(BTreeMap::new()),
            complete_reachable_catalogs: Mutex::new(None),
            next_generation: AtomicU64::new(1),
            scheduler: ProjectionScheduler::new(CatalogMaterializationConfig::default()),
        }
    }

    pub fn new(
        source: CatalogDesiredStateSource,
        control: Arc<ConnectorControlHost>,
        runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
        runtime: Handle,
    ) -> Self {
        Self::new_with_materialization_config(
            source,
            control,
            runtime_publisher,
            runtime,
            CatalogMaterializationConfig::default(),
        )
    }

    pub fn new_with_materialization_config(
        source: CatalogDesiredStateSource,
        control: Arc<ConnectorControlHost>,
        runtime_publisher: Arc<dyn CatalogRuntimePublisherSink>,
        runtime: Handle,
        materialization_config: CatalogMaterializationConfig,
    ) -> Self {
        Self {
            source: Some(source),
            control,
            runtime_publisher,
            runtime,
            projections: Mutex::new(BTreeMap::new()),
            complete_reachable_catalogs: Mutex::new(None),
            next_generation: AtomicU64::new(1),
            scheduler: ProjectionScheduler::new(materialization_config),
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
    pub(crate) fn attachment_wakeup_signal(&self) -> Option<CatalogAttachmentWakeupSignal> {
        self.source.as_ref()?.attachment_wakeup_signal()
    }

    /// A complete desired-state projection plus every still-draining local
    /// control generation. `None` means this frontend has not completed a
    /// source enumeration, so pruning must skip the round.
    pub(crate) fn reachable_catalog_handles(&self) -> Option<BTreeSet<CatalogHandle>> {
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
    /// * [`FrontendCatalogApplicationPort::materialize_entry`] returns `()`, so
    ///   one catalog's provider failure cannot reach this function's `Result`
    ///   at all — it marks that catalog `Unavailable` and leaves the rest
    ///   serving.
    pub(crate) async fn reconcile_with_page_size(
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
    pub(crate) async fn reconcile_snapshot_with_page_size(
        self: &Arc<Self>,
        page_size: usize,
        worker_count: usize,
    ) -> Result<(CatalogDesiredStateSnapshot, CatalogProjectionCounts), CatalogApplicationError>
    {
        if worker_count == 0 {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::InvalidRequest,
                "catalog projection worker count must be positive",
            ));
        }
        let source = self.source()?;
        let snapshot = source.enumerate(page_size).await?;
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
        self.retire_projections_absent_from(source, &snapshot)
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
            match source.locate(&instance_id).await {
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
        let key = properties.handle().clone();
        let context =
            MaterializationContext::new(Instant::now() + self.scheduler.config.attempt_timeout);
        let token = self.scheduler.next_token.fetch_add(1, Ordering::Relaxed);
        {
            let mut attempts = self.scheduler.attempts.lock().map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Internal,
                    "catalog projection scheduler lock is poisoned",
                )
            })?;
            if attempts.get(&key).is_some_and(|attempt| {
                attempt.attachment_id == attachment_id && attempt.instance_id == instance_id
            }) {
                if let Some(completion) = completion {
                    if let Some(attempt) = attempts.get_mut(&key) {
                        attempt.completion_waiters.push(completion);
                    }
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
        }
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
            && self.control.observe_current_binding(&instance_id).is_ok();
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

    pub(crate) fn projection_counts(&self) -> CatalogProjectionCounts {
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

impl CatalogApplicationPort for FrontendCatalogApplicationPort {
    fn create_catalog(
        &self,
        command: CatalogCreateCommand,
    ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
        let repository = self.sql_mutation_authority()?;
        let (credential_bindings, provider_properties) =
            extract_catalog_credential_bindings(command.properties)?;
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
        let created = self.block_on(repository.create(attachment))?;
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
        self.block_on_catalog(async {
            if let Some(work) = work {
                self.run_materialization(work).await;
            }
            ticket.await.map_err(|_| {
                CatalogApplicationError::new(
                    CatalogApplicationErrorKind::Unavailable,
                    "catalog materialization attempt stopped before completion",
                )
            })?
        })
    }

    fn drop_catalog(&self, command: CatalogDropCommand) -> Result<(), CatalogApplicationError> {
        let repository = self.sql_mutation_authority()?;
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
        self.block_on(repository.observe_materialized_view_references(&command.instance_id, 256))?;
        self.block_on(repository.drop_exact(existing))?;
        self.retire_projection(&command.instance_id);
        // Durable deletion is authoritative. A local generation can be absent
        // or already retiring; either case converges through reconciliation.
        Ok(())
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

fn extract_catalog_credential_bindings(
    properties: Vec<(String, String)>,
) -> Result<(Vec<CatalogCredentialBinding>, Vec<(String, String)>), CatalogApplicationError> {
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

fn materialization_error(
    error: &novarocks_spi::connector::ConnectorMaterializationError,
) -> CatalogApplicationError {
    use novarocks_spi::connector::ConnectorMaterializationErrorClass;

    let kind = match error.class() {
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
    CatalogApplicationError::new(kind, error.to_string())
}

fn mv_repository_error(error: CatalogApplicationError) -> MvRepositoryError {
    let kind = match error.kind() {
        // A source mode that forbids the operation refuses it permanently, so
        // it is a request-level rejection rather than an outage to retry.
        CatalogApplicationErrorKind::InvalidRequest
        | CatalogApplicationErrorKind::UnsupportedSourceMode => {
            MvRepositoryErrorKind::InvalidRequest
        }
        CatalogApplicationErrorKind::NotFound
        | CatalogApplicationErrorKind::AlreadyExists
        | CatalogApplicationErrorKind::Conflict => MvRepositoryErrorKind::Conflict,
        // The source could not be read completely; a later attempt may succeed,
        // and nothing about desired state was proven either way.
        CatalogApplicationErrorKind::Unavailable
        | CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete => {
            MvRepositoryErrorKind::Unavailable
        }
        CatalogApplicationErrorKind::Internal => MvRepositoryErrorKind::Corruption,
    };
    MvRepositoryError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    use futures::FutureExt;

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

    fn scheduler_port(factory: Arc<FailingRoleFactory>) -> Arc<FrontendCatalogApplicationPort> {
        let control = Arc::new(
            ConnectorControlHost::with_role_factories(vec![factory]).expect("role factory host"),
        );
        Arc::new(FrontendCatalogApplicationPort::unavailable(
            control,
            crate::catalog_application::CatalogRuntimeProjection::new().publisher(),
            tokio::runtime::Handle::current(),
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
                let control = crate::connector::control_host::tests::test_control_binding_for(
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
        let port = Arc::new(FrontendCatalogApplicationPort::unavailable(
            Arc::clone(&control),
            crate::catalog_application::CatalogRuntimeProjection::new().publisher(),
            tokio::runtime::Handle::current(),
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
                "frontend-and-backend".to_string(),
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
        ])
        .expect("typed credential bindings");
        assert_eq!(
            properties,
            vec![("type".to_string(), "iceberg".to_string())]
        );
        assert_eq!(bindings.len(), 2);
        assert_eq!(
            bindings[0].purpose(),
            CatalogCredentialPurpose::CatalogControl
        );
        assert_eq!(
            bindings[1].purpose(),
            CatalogCredentialPurpose::ObjectStoreData
        );
        assert_eq!(
            bindings[1].consumer_role(),
            CredentialConsumerRole::FrontendAndBackend
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
                    "backend".to_string(),
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
}
