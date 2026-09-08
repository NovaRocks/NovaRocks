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

//! Backend-local catalog materialization and retention state.
//!
//! This module deliberately has no RPC, provider discovery, or query-lifecycle
//! ownership.  Its caller supplies the immutable `CatalogProperties` frozen by
//! the frontend, and its only liveness input is the set of admitted query
//! execution ids.  A later integration layer owns converting provider-specific
//! materialization failures into protocol responses.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use novarocks_spi::connector::{CatalogHandle, CatalogProperties, ConnectorProviderId};
use novarocks_spi::connector::{
    ConnectorExecutionRoleBinding, ConnectorExecutionRoleBindingFactory,
    ConnectorMaterializationError, ConnectorMaterializationErrorClass,
    ConnectorMaterializationRetryDisposition, NormalizedCatalogProperties,
};
use novarocks_types::QueryExecutionId;

/// The bounded number of unleased materialized catalogs retained by default.
pub const DEFAULT_MAX_RETAINED_CATALOGS: usize = 64;
pub const DEFAULT_MAX_FAILED_CATALOGS: usize = 64;
const DEFAULT_FAILED_RETENTION: Duration = Duration::from_secs(60);
const DEFAULT_TRANSIENT_RETRY_COOLDOWN: Duration = Duration::from_secs(1);
const DEFAULT_PROVIDER_MAX_CONCURRENT_BINDS: usize = 4;

/// Startup-sealed execution-role factories keyed by the closed catalog family.
/// Each selected factory constructs the entire immutable BE capability binding
/// in one local, bounded operation.
#[derive(Clone)]
pub struct ConnectorExecutionRoleBindingFactorySet {
    factories: Arc<BTreeMap<ConnectorProviderId, Arc<dyn ConnectorExecutionRoleBindingFactory>>>,
}

impl ConnectorExecutionRoleBindingFactorySet {
    pub fn try_new(
        factories: impl IntoIterator<Item = Arc<dyn ConnectorExecutionRoleBindingFactory>>,
    ) -> Result<Self, CatalogManagerError> {
        let mut sealed = BTreeMap::new();
        for factory in factories {
            let provider_id = factory.provider_id();
            if sealed.insert(provider_id, factory).is_some() {
                return Err(CatalogManagerError::InvalidConfiguration(
                    "duplicate connector execution role binding factory provider kind",
                ));
            }
        }
        Ok(Self {
            factories: Arc::new(sealed),
        })
    }

    pub fn bind(
        &self,
        properties: &CatalogProperties,
    ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
        let normalized =
            NormalizedCatalogProperties::try_new(properties.clone()).map_err(|detail| {
                ConnectorMaterializationError::new(
                    ConnectorMaterializationErrorClass::InvalidDefinition,
                    ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    detail,
                )
            })?;
        let Some(factory) = self.factories.get(normalized.provider_id()) else {
            return Err(ConnectorMaterializationError::new(
                ConnectorMaterializationErrorClass::InvalidDefinition,
                ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                "connector execution role provider is not installed",
            ));
        };
        let binding = factory.bind(&normalized)?;
        if crate::config::debug_emit_catalog_materialization_marker() {
            println!(
                "NOVAROCKS_CATALOG_RUNTIME_MATERIALIZED catalog={:?}",
                properties.handle()
            );
        }
        Ok(binding)
    }
}

/// Catalog-manager configuration that is independent of any provider runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogManagerConfig {
    pub max_retained_catalogs: usize,
    pub max_failed_catalogs: usize,
    pub failed_retention: Duration,
    pub transient_retry_cooldown: Duration,
    pub provider_max_concurrent_binds: usize,
    pub provider_min_bind_interval: Duration,
}

impl Default for CatalogManagerConfig {
    fn default() -> Self {
        Self {
            max_retained_catalogs: DEFAULT_MAX_RETAINED_CATALOGS,
            max_failed_catalogs: DEFAULT_MAX_FAILED_CATALOGS,
            failed_retention: DEFAULT_FAILED_RETENTION,
            transient_retry_cooldown: DEFAULT_TRANSIENT_RETRY_COOLDOWN,
            provider_max_concurrent_binds: DEFAULT_PROVIDER_MAX_CONCURRENT_BINDS,
            provider_min_bind_interval: Duration::ZERO,
        }
    }
}

/// A stable, provider-neutral failure returned by the materialization owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogManagerError {
    InvalidConfiguration(&'static str),
    ConflictingProperties {
        handle: CatalogHandle,
    },
    MaterializationFailed {
        class: ConnectorMaterializationErrorClass,
        disposition: ConnectorMaterializationRetryDisposition,
        detail: Arc<str>,
    },
}

impl CatalogManagerError {
    pub fn materialization_failed(message: impl Into<Arc<str>>) -> Self {
        Self::MaterializationFailed {
            class: ConnectorMaterializationErrorClass::Internal,
            disposition: ConnectorMaterializationRetryDisposition::Transient,
            detail: message.into(),
        }
    }

    pub fn from_materialization(error: ConnectorMaterializationError) -> Self {
        Self::MaterializationFailed {
            class: error.class(),
            disposition: error.disposition(),
            detail: Arc::from(error.detail()),
        }
    }

    pub const fn class(&self) -> Option<ConnectorMaterializationErrorClass> {
        match self {
            Self::MaterializationFailed { class, .. } => Some(*class),
            Self::InvalidConfiguration(_) | Self::ConflictingProperties { .. } => None,
        }
    }

    pub const fn disposition(&self) -> Option<ConnectorMaterializationRetryDisposition> {
        match self {
            Self::MaterializationFailed { disposition, .. } => Some(*disposition),
            Self::InvalidConfiguration(_) | Self::ConflictingProperties { .. } => None,
        }
    }
}

impl fmt::Display for CatalogManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => formatter.write_str(message),
            Self::ConflictingProperties { handle } => write!(
                formatter,
                "catalog handle {} has conflicting materialization properties",
                handle.catalog_name().as_str()
            ),
            Self::MaterializationFailed { class, detail, .. } => {
                write!(
                    formatter,
                    "catalog materialization failed ({class:?}): {detail}"
                )
            }
        }
    }
}

impl std::error::Error for CatalogManagerError {}

/// Result of reconciling backend retention against a frontend reachability
/// snapshot.  A rejection is atomic: no catalog was evicted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogPruneResult {
    Pruned {
        handles: BTreeSet<CatalogHandle>,
    },
    Rejected {
        missing_live_handles: BTreeSet<CatalogHandle>,
    },
}

impl CatalogPruneResult {
    pub fn pruned_handles(&self) -> Option<&BTreeSet<CatalogHandle>> {
        match self {
            Self::Pruned { handles } => Some(handles),
            Self::Rejected { .. } => None,
        }
    }

    pub fn missing_live_handles(&self) -> Option<&BTreeSet<CatalogHandle>> {
        match self {
            Self::Pruned { .. } => None,
            Self::Rejected {
                missing_live_handles,
            } => Some(missing_live_handles),
        }
    }
}

/// Handle-keyed BE-local materializations.  Multiple versions of one catalog
/// name coexist; only an exact `CatalogHandle` can resolve a runtime.
#[derive(Clone)]
// Design: ADR-0130 (docs/adr/ADR-0130-connector-role-binding-generation-ownership.md)
pub struct CatalogManager<T> {
    state: Arc<Mutex<CatalogManagerState<T>>>,
    config: CatalogManagerConfig,
    provider_changed: Arc<Condvar>,
}

/// A bounded, provider-neutral view of catalog leases held by admitted
/// queries. Retained warm runtimes are deliberately excluded: cache retention
/// is not an execution authority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CatalogLeaseSnapshot {
    pub query_leases: usize,
    pub handle_leases: usize,
}

struct CatalogManagerState<T> {
    entries: BTreeMap<CatalogHandle, Arc<CatalogCell<T>>>,
    query_reachability: BTreeMap<QueryExecutionId, BTreeSet<CatalogHandle>>,
    next_registration_token: u64,
    provider_binds: BTreeMap<ConnectorProviderId, ProviderBindState>,
}

#[derive(Default)]
struct ProviderBindState {
    active: usize,
    last_started: Option<Instant>,
}

struct CatalogCell<T> {
    properties: CatalogProperties,
    state: Mutex<CatalogCellState<T>>,
    changed: Condvar,
}

enum CatalogCellState<T> {
    Materializing,
    Ready {
        runtime: Arc<T>,
        registration_token: RegistrationToken,
    },
    FailedSuppressed {
        error: CatalogManagerError,
        attempts: u32,
        retry_after: Option<Instant>,
        last_used: Instant,
    },
}

/// BE-private registration identity.  It is never supplied by FE input or
/// carried on the wire: it only proves an eviction still targets the exact
/// local installation that was selected earlier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegistrationToken(u64);

#[derive(Clone)]
struct ReadyCandidate<T> {
    handle: CatalogHandle,
    cell: Arc<CatalogCell<T>>,
    registration_token: RegistrationToken,
}

impl<T> CatalogCell<T> {
    fn materializing(properties: CatalogProperties) -> Self {
        Self {
            properties,
            state: Mutex::new(CatalogCellState::Materializing),
            changed: Condvar::new(),
        }
    }

    fn wait_for_result(&self, active: &impl Fn() -> bool) -> Result<Arc<T>, CatalogManagerError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        loop {
            match &*state {
                CatalogCellState::Materializing => {
                    if !active() {
                        return Err(cancelled_catalog_install_error());
                    }
                    let (next, _) = self
                        .changed
                        .wait_timeout(state, Duration::from_millis(10))
                        .unwrap_or_else(|error| error.into_inner());
                    state = next;
                }
                CatalogCellState::Ready { runtime, .. } => return Ok(Arc::clone(runtime)),
                CatalogCellState::FailedSuppressed { error, .. } => return Err(error.clone()),
            }
        }
    }

    fn complete(
        &self,
        result: Result<T, CatalogManagerError>,
        registration_token: Option<RegistrationToken>,
        attempts: u32,
        retry_after: Option<Instant>,
    ) -> Result<Arc<T>, CatalogManagerError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let result = match result {
            Ok(runtime) => {
                let registration_token = registration_token
                    .expect("successful catalog materialization requires a registration token");
                let runtime = Arc::new(runtime);
                *state = CatalogCellState::Ready {
                    runtime: Arc::clone(&runtime),
                    registration_token,
                };
                Ok(runtime)
            }
            Err(error) => {
                *state = CatalogCellState::FailedSuppressed {
                    error: error.clone(),
                    attempts,
                    retry_after,
                    last_used: Instant::now(),
                };
                Err(error)
            }
        };
        self.changed.notify_all();
        result
    }

    fn ready_candidate(self: &Arc<Self>, handle: &CatalogHandle) -> Option<ReadyCandidate<T>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let CatalogCellState::Ready {
            registration_token, ..
        } = &*state
        else {
            return None;
        };
        Some(ReadyCandidate {
            handle: handle.clone(),
            cell: Arc::clone(self),
            registration_token: *registration_token,
        })
    }
}

impl<T> CatalogManager<T> {
    pub fn try_new(config: CatalogManagerConfig) -> Result<Self, CatalogManagerError> {
        if config.max_retained_catalogs == 0 {
            return Err(CatalogManagerError::InvalidConfiguration(
                "catalog manager must retain at least one catalog",
            ));
        }
        if config.max_failed_catalogs == 0 {
            return Err(CatalogManagerError::InvalidConfiguration(
                "catalog manager must retain at least one suppressed failure",
            ));
        }
        if config.failed_retention.is_zero() || config.provider_max_concurrent_binds == 0 {
            return Err(CatalogManagerError::InvalidConfiguration(
                "catalog manager suppression and provider bind limits must be positive",
            ));
        }
        Ok(Self {
            state: Arc::new(Mutex::new(CatalogManagerState {
                entries: BTreeMap::new(),
                query_reachability: BTreeMap::new(),
                next_registration_token: 0,
                provider_binds: BTreeMap::new(),
            })),
            config,
            provider_changed: Arc::new(Condvar::new()),
        })
    }

    /// Materialize one exact catalog, lease it to `query`, and return the
    /// shared local runtime.  Pending installation and query reachability are
    /// intentionally separate: only a completed Ready binding acquires a
    /// query lease.  A suppressed failure therefore cannot become a decode
    /// authority while it protects a provider from an Init storm.
    pub fn ensure(
        &self,
        query: QueryExecutionId,
        properties: CatalogProperties,
        materialize: impl FnOnce(&CatalogProperties) -> Result<T, CatalogManagerError>,
    ) -> Result<Arc<T>, CatalogManagerError> {
        self.ensure_while(query, properties, || true, materialize)
    }

    pub fn ensure_while(
        &self,
        query: QueryExecutionId,
        properties: CatalogProperties,
        active: impl Fn() -> bool,
        materialize: impl FnOnce(&CatalogProperties) -> Result<T, CatalogManagerError>,
    ) -> Result<Arc<T>, CatalogManagerError> {
        let handle = properties.handle().clone();
        let (cell, installer, attempts) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(cell) = state.entries.get(&handle).cloned() {
                if cell.properties != properties {
                    return Err(CatalogManagerError::ConflictingProperties { handle });
                }
                let mut cell_state = cell.state.lock().unwrap_or_else(|error| error.into_inner());
                match &mut *cell_state {
                    CatalogCellState::Ready { runtime, .. } => {
                        let runtime = Arc::clone(runtime);
                        drop(cell_state);
                        state
                            .query_reachability
                            .entry(query)
                            .or_default()
                            .insert(handle);
                        return Ok(runtime);
                    }
                    CatalogCellState::Materializing => {
                        drop(cell_state);
                        (cell, false, 0)
                    }
                    CatalogCellState::FailedSuppressed {
                        error,
                        attempts,
                        retry_after,
                        last_used,
                    } => {
                        *last_used = Instant::now();
                        let retryable = error.disposition()
                            == Some(ConnectorMaterializationRetryDisposition::Transient)
                            && retry_after.is_some_and(|deadline| Instant::now() >= deadline);
                        if !retryable {
                            return Err(error.clone());
                        }
                        let next_attempt = attempts.saturating_add(1);
                        *cell_state = CatalogCellState::Materializing;
                        drop(cell_state);
                        (cell, true, next_attempt)
                    }
                }
            } else {
                let cell = Arc::new(CatalogCell::materializing(properties));
                state.entries.insert(handle.clone(), Arc::clone(&cell));
                (cell, true, 1)
            }
        };

        if !installer {
            let runtime = cell.wait_for_result(&active)?;
            if !active() {
                return Err(cancelled_catalog_install_error());
            }
            self.lease_ready(query, &handle, &cell)?;
            return Ok(runtime);
        }

        let provider = cell.properties.provider_id().clone();
        let materialized = match self.acquire_provider_bind(provider.clone(), &active) {
            Ok(()) => {
                let result = if active() {
                    materialize(&cell.properties)
                } else {
                    Err(cancelled_catalog_install_error())
                };
                self.release_provider_bind(provider);
                result
            }
            // The cell is already visible to other queries. Complete it even
            // when cancellation wins while waiting for the provider limiter,
            // otherwise followers can wait forever on Materializing.
            Err(error) => Err(error),
        };
        let materialized = if active() {
            materialized
        } else {
            Err(cancelled_catalog_install_error())
        };
        let registration_token = materialized
            .as_ref()
            .ok()
            .map(|_| self.allocate_registration_token());
        let retry_after = materialized.as_ref().err().and_then(|error| {
            (error.disposition() == Some(ConnectorMaterializationRetryDisposition::Transient))
                .then(|| Instant::now() + self.config.transient_retry_cooldown)
        });
        let result = cell.complete(materialized, registration_token, attempts, retry_after);
        if let Ok(runtime) = result {
            if !active() {
                return Err(cancelled_catalog_install_error());
            }
            self.lease_ready(query, &handle, &cell)?;
            return Ok(runtime);
        }
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            trim_failed_entries(
                &mut state,
                self.config.max_failed_catalogs,
                self.config.failed_retention,
                Instant::now(),
            );
        }
        result
    }

    /// Atomically lease a complete catalog set when every exact runtime is
    /// already Ready.  A caller that receives `false` must use `ensure` for
    /// the ordinary cold/single-flight path; it receives no partial lease.
    pub fn try_acquire_ready_catalogs(
        &self,
        query: QueryExecutionId,
        catalogs: &[CatalogProperties],
    ) -> Result<bool, CatalogManagerError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut cells = Vec::with_capacity(catalogs.len());
        for properties in catalogs {
            let handle = properties.handle().clone();
            let Some(cell) = state.entries.get(&handle).cloned() else {
                return Ok(false);
            };
            if cell.properties != *properties {
                return Err(CatalogManagerError::ConflictingProperties { handle });
            }
            cells.push(cell);
        }
        if cells.iter().any(|cell| {
            !matches!(
                &*cell.state.lock().unwrap_or_else(|error| error.into_inner()),
                CatalogCellState::Ready { .. }
            )
        }) {
            return Ok(false);
        }
        state.query_reachability.entry(query).or_default().extend(
            catalogs
                .iter()
                .map(|properties| properties.handle().clone()),
        );
        Ok(true)
    }

    /// Release every catalog lease held by a terminal query.  The configured
    /// retention bound is then enforced without evicting a still-reachable
    /// catalog or a materialization that is still in progress.
    pub fn release_query(&self, query: QueryExecutionId) -> BTreeSet<CatalogHandle> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.query_reachability.remove(&query);
        let removed = trim_unreachable_ready_entries(
            &mut state,
            self.config.max_retained_catalogs,
            &BTreeSet::new(),
        );
        trim_failed_entries(
            &mut state,
            self.config.max_failed_catalogs,
            self.config.failed_retention,
            Instant::now(),
        );
        removed
    }

    /// Reconcile retained entries with the frontend's complete view of live
    /// query reachability.  Omitting a BE-live handle rejects the whole prune
    /// request before mutation; this prevents stale control traffic from
    /// tearing down a runtime needed by an admitted query.
    pub fn prune_unreachable(&self, reachable: &BTreeSet<CatalogHandle>) -> CatalogPruneResult {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let locally_live = all_query_handles(&state.query_reachability);
        let missing_live_handles = locally_live
            .difference(reachable)
            .cloned()
            .collect::<BTreeSet<_>>();
        if !missing_live_handles.is_empty() {
            return CatalogPruneResult::Rejected {
                missing_live_handles,
            };
        }
        remove_unreachable_failed_entries(&mut state, reachable);
        let handles = trim_unreachable_ready_entries(&mut state, 0, reachable);
        CatalogPruneResult::Pruned { handles }
    }

    pub fn resolve(&self, handle: &CatalogHandle) -> Option<Arc<T>> {
        let cell = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .get(handle)
            .cloned()?;
        let state = cell.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            CatalogCellState::Ready { runtime, .. } => Some(Arc::clone(runtime)),
            CatalogCellState::Materializing | CatalogCellState::FailedSuppressed { .. } => None,
        }
    }

    /// Resolve a runtime only when the exact handle remains leased by the
    /// admitted query. Decode uses this rather than the retained-cache lookup,
    /// so retention can never become an authority path.
    pub fn resolve_for_query(
        &self,
        query: QueryExecutionId,
        handle: &CatalogHandle,
    ) -> Option<Arc<T>> {
        let cell = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state
                .query_reachability
                .get(&query)
                .filter(|handles| handles.contains(handle))
                .and_then(|_| state.entries.get(handle))
                .cloned()
        }?;
        let state = cell.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            CatalogCellState::Ready { runtime, .. } => Some(Arc::clone(runtime)),
            CatalogCellState::Materializing | CatalogCellState::FailedSuppressed { .. } => None,
        }
    }

    pub fn retained_handles(&self) -> BTreeSet<CatalogHandle> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .keys()
            .cloned()
            .collect()
    }

    pub fn query_handles(&self, query: QueryExecutionId) -> BTreeSet<CatalogHandle> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .query_reachability
            .get(&query)
            .cloned()
            .unwrap_or_default()
    }

    /// Return lease cardinalities without exposing the query-to-handle map.
    /// Metrics consume this snapshot after the manager lock is released.
    pub fn lease_snapshot(&self) -> CatalogLeaseSnapshot {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        CatalogLeaseSnapshot {
            query_leases: state.query_reachability.len(),
            handle_leases: state.query_reachability.values().map(BTreeSet::len).sum(),
        }
    }

    fn lease_ready(
        &self,
        query: QueryExecutionId,
        handle: &CatalogHandle,
        cell: &Arc<CatalogCell<T>>,
    ) -> Result<(), CatalogManagerError> {
        let cell_state = cell.state.lock().unwrap_or_else(|error| error.into_inner());
        if !matches!(&*cell_state, CatalogCellState::Ready { .. }) {
            return Err(CatalogManagerError::materialization_failed(
                "catalog install did not complete Ready",
            ));
        }
        drop(cell_state);
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .query_reachability
            .entry(query)
            .or_default()
            .insert(handle.clone());
        Ok(())
    }

    fn acquire_provider_bind(
        &self,
        provider: ConnectorProviderId,
        active: &impl Fn() -> bool,
    ) -> Result<(), CatalogManagerError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        loop {
            if !active() {
                return Err(cancelled_catalog_install_error());
            }
            let provider_state = state.provider_binds.entry(provider.clone()).or_default();
            let rate_ready = provider_state
                .last_started
                .is_none_or(|last| last.elapsed() >= self.config.provider_min_bind_interval);
            if provider_state.active < self.config.provider_max_concurrent_binds && rate_ready {
                provider_state.active += 1;
                provider_state.last_started = Some(Instant::now());
                return Ok(());
            }
            let (next, _) = self
                .provider_changed
                .wait_timeout(state, Duration::from_millis(10))
                .unwrap_or_else(|error| error.into_inner());
            state = next;
        }
    }

    fn release_provider_bind(&self, provider: ConnectorProviderId) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let provider_state = state
            .provider_binds
            .get_mut(&provider)
            .expect("acquired provider bind state must exist");
        provider_state.active = provider_state.active.saturating_sub(1);
        self.provider_changed.notify_all();
    }

    fn allocate_registration_token(&self) -> RegistrationToken {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.next_registration_token = state
            .next_registration_token
            .checked_add(1)
            .expect("catalog registration token exhausted");
        RegistrationToken(state.next_registration_token)
    }
}

impl<T> Default for CatalogManager<T> {
    fn default() -> Self {
        Self::try_new(CatalogManagerConfig::default()).expect("default catalog manager is valid")
    }
}

fn all_query_handles(
    query_reachability: &BTreeMap<QueryExecutionId, BTreeSet<CatalogHandle>>,
) -> BTreeSet<CatalogHandle> {
    query_reachability
        .values()
        .flat_map(|handles| handles.iter().cloned())
        .collect()
}

fn cancelled_catalog_install_error() -> CatalogManagerError {
    CatalogManagerError::from_materialization(ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::Cancelled,
        ConnectorMaterializationRetryDisposition::Transient,
        "catalog installation was cancelled before Ready",
    ))
}

fn trim_failed_entries<T>(
    state: &mut CatalogManagerState<T>,
    retain_at_most: usize,
    retention: Duration,
    now: Instant,
) {
    let mut failed = state
        .entries
        .iter()
        .filter_map(|(handle, cell)| {
            let cell_state = cell.state.lock().unwrap_or_else(|error| error.into_inner());
            let CatalogCellState::FailedSuppressed { last_used, .. } = &*cell_state else {
                return None;
            };
            Some((handle.clone(), *last_used))
        })
        .collect::<Vec<_>>();
    failed.sort_by_key(|(_, last_used)| *last_used);
    let expired = failed
        .iter()
        .filter(|(_, last_used)| now.duration_since(*last_used) >= retention)
        .map(|(handle, _)| handle.clone())
        .collect::<BTreeSet<_>>();
    let overflow = failed.len().saturating_sub(retain_at_most);
    let evicted = failed
        .into_iter()
        .take(overflow)
        .map(|(handle, _)| handle)
        .collect::<BTreeSet<_>>();
    for handle in expired.union(&evicted) {
        let removable = state.entries.get(handle).is_some_and(|cell| {
            matches!(
                &*cell.state.lock().unwrap_or_else(|error| error.into_inner()),
                CatalogCellState::FailedSuppressed { .. }
            )
        });
        if removable {
            state.entries.remove(handle);
        }
    }
}

fn remove_unreachable_failed_entries<T>(
    state: &mut CatalogManagerState<T>,
    reachable: &BTreeSet<CatalogHandle>,
) {
    state.entries.retain(|handle, cell| {
        reachable.contains(handle)
            || !matches!(
                &*cell.state.lock().unwrap_or_else(|error| error.into_inner()),
                CatalogCellState::FailedSuppressed { .. }
            )
    });
}

fn trim_unreachable_ready_entries<T>(
    state: &mut CatalogManagerState<T>,
    retain_at_most: usize,
    protected: &BTreeSet<CatalogHandle>,
) -> BTreeSet<CatalogHandle> {
    let live = all_query_handles(&state.query_reachability);
    let candidates = state
        .entries
        .iter()
        .filter_map(|(handle, cell)| {
            if live.contains(handle) || protected.contains(handle) {
                return None;
            }
            cell.ready_candidate(handle)
        })
        .collect::<Vec<_>>();
    let excess = state.entries.len().saturating_sub(retain_at_most);
    let remove_count = if retain_at_most == 0 {
        candidates.len()
    } else {
        candidates.len().min(excess)
    };
    let candidates = candidates
        .into_iter()
        .take(remove_count)
        .collect::<Vec<_>>();
    let mut removed = BTreeSet::new();
    for candidate in candidates {
        if remove_ready_candidate_if_current(state, &candidate) {
            removed.insert(candidate.handle);
        }
    }
    removed
}

/// A stale candidate is harmless: the current map entry must still be the
/// same allocation and must retain the registration token observed at
/// candidate creation.  This closes an ABA gap when a handle is pruned and
/// subsequently materialized again before a delayed cleanup runs.
fn remove_ready_candidate_if_current<T>(
    state: &mut CatalogManagerState<T>,
    candidate: &ReadyCandidate<T>,
) -> bool {
    let Some(current) = state.entries.get(&candidate.handle) else {
        return false;
    };
    if !Arc::ptr_eq(current, &candidate.cell) {
        return false;
    }
    let current_state = current
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let is_current_registration = matches!(
        &*current_state,
        CatalogCellState::Ready {
            registration_token,
            ..
        } if *registration_token == candidate.registration_token
    );
    drop(current_state);
    if !is_current_registration {
        return false;
    }
    state.entries.remove(&candidate.handle);
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogVersion, ConnectorInstanceId, ConnectorProviderId,
    };
    use novarocks_spi::connector::{
        ConnectorExecutionRoleBinding, ConnectorExecutionRoleBindingFactory,
        ConnectorMaterializationError, ConnectorMaterializationErrorClass,
        ConnectorMaterializationRetryDisposition, NormalizedCatalogProperties,
    };
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};

    use super::{
        CatalogManager, CatalogManagerConfig, CatalogManagerError, CatalogPruneResult,
        ConnectorExecutionRoleBindingFactorySet, remove_ready_candidate_if_current,
    };

    fn query(value: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(7, value), AttemptId::new(1).expect("attempt"))
            .expect("query execution id")
    }

    fn properties(version: u8) -> CatalogProperties {
        let handle = CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("catalog.analytics").expect("catalog id"),
            CatalogVersion::from_bytes([version; 32]),
        );
        CatalogProperties::new(
            handle,
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![],
            vec![],
        )
        .expect("catalog properties")
    }

    struct UnsupportedFactory {
        provider_id: ConnectorProviderId,
    }

    impl ConnectorExecutionRoleBindingFactory for UnsupportedFactory {
        fn provider_id(&self) -> ConnectorProviderId {
            self.provider_id.clone()
        }

        fn bind(
            &self,
            properties: &NormalizedCatalogProperties,
        ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
            ConnectorExecutionRoleBinding::try_new(properties.clone(), None, None)
                .map_err(Into::into)
        }
    }

    #[test]
    fn execution_role_factory_set_rejects_duplicate_provider_id_before_admission() {
        assert!(matches!(
            ConnectorExecutionRoleBindingFactorySet::try_new([
                Arc::new(UnsupportedFactory {
                    provider_id: ConnectorProviderId::parse("iceberg").expect("static provider ID"),
                }) as Arc<dyn ConnectorExecutionRoleBindingFactory>,
                Arc::new(UnsupportedFactory {
                    provider_id: ConnectorProviderId::parse("iceberg").expect("static provider ID"),
                }) as Arc<dyn ConnectorExecutionRoleBindingFactory>,
            ]),
            Err(CatalogManagerError::InvalidConfiguration(
                "duplicate connector execution role binding factory provider kind"
            ))
        ));
    }

    #[test]
    fn concurrent_ensure_deduplicates_materialization_and_shares_runtime() {
        let manager = Arc::new(CatalogManager::<usize>::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let first_manager = Arc::clone(&manager);
        let first_calls = Arc::clone(&calls);
        let first_entered = Arc::clone(&entered);
        let first_release = Arc::clone(&release);
        let first = thread::spawn(move || {
            first_manager.ensure(query(1), properties(1), |_| {
                first_calls.fetch_add(1, Ordering::SeqCst);
                first_entered.wait();
                first_release.wait();
                Ok(42)
            })
        });
        entered.wait();
        let second_manager = Arc::clone(&manager);
        let second_calls = Arc::clone(&calls);
        let second = thread::spawn(move || {
            second_manager.ensure(query(2), properties(1), |_| {
                second_calls.fetch_add(1, Ordering::SeqCst);
                Ok(7)
            })
        });
        release.wait();
        assert_eq!(*first.join().expect("first ensure").expect("runtime"), 42);
        assert_eq!(*second.join().expect("second ensure").expect("runtime"), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn failed_materialization_is_suppressed_but_never_query_reachable() {
        let manager = CatalogManager::<usize>::default();
        let catalog = properties(3);
        let handle = catalog.handle().clone();
        let error = manager
            .ensure(query(1), catalog, |_| {
                Err(CatalogManagerError::materialization_failed(
                    "credentials unavailable",
                ))
            })
            .expect_err("materialization must fail");
        assert_eq!(
            error,
            CatalogManagerError::materialization_failed("credentials unavailable")
        );
        assert!(manager.resolve(&handle).is_none());
        assert!(manager.retained_handles().contains(&handle));
        assert!(manager.query_handles(query(1)).is_empty());
    }

    #[test]
    fn retained_runtime_does_not_bypass_the_query_catalog_lease() {
        let manager = CatalogManager::<usize>::default();
        let catalog = properties(1);
        let handle = catalog.handle().clone();
        manager
            .ensure(query(1), catalog, |_| Ok(7))
            .expect("materialize runtime");
        assert_eq!(
            *manager
                .resolve_for_query(query(1), &handle)
                .expect("query owns exact handle"),
            7
        );
        manager.release_query(query(1));
        assert!(
            manager.resolve(&handle).is_some(),
            "retention keeps the runtime"
        );
        assert!(
            manager.resolve_for_query(query(1), &handle).is_none(),
            "retention must not grant decode authority after terminal release"
        );
    }

    #[test]
    fn cooldown_suppresses_repeated_transient_bind_attempts() {
        let manager = CatalogManager::<usize>::try_new(CatalogManagerConfig {
            transient_retry_cooldown: Duration::from_secs(60),
            ..CatalogManagerConfig::default()
        })
        .expect("valid manager");
        let catalog = properties(3);
        let calls = AtomicUsize::new(0);
        manager
            .ensure(query(1), catalog.clone(), |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(CatalogManagerError::materialization_failed(
                    "transient failure",
                ))
            })
            .expect_err("first materialization fails");
        assert!(
            manager
                .ensure(query(2), catalog.clone(), |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(9)
                })
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(manager.resolve(catalog.handle()).is_none());
    }

    #[test]
    fn expired_transient_suppression_allows_one_fresh_installer_and_success_clears_failure() {
        let manager = CatalogManager::<usize>::try_new(CatalogManagerConfig {
            transient_retry_cooldown: Duration::ZERO,
            ..CatalogManagerConfig::default()
        })
        .expect("valid manager");
        let catalog = properties(3);
        manager
            .ensure(query(1), catalog.clone(), |_| {
                Err(CatalogManagerError::materialization_failed(
                    "transient failure",
                ))
            })
            .expect_err("first bind fails");
        assert_eq!(
            *manager
                .ensure(query(2), catalog.clone(), |_| Ok(9))
                .expect("expired suppression permits one fresh bind"),
            9
        );
        assert_eq!(
            *manager.resolve(catalog.handle()).expect("Ready runtime"),
            9
        );
        assert_eq!(manager.query_handles(query(1)).len(), 0);
        assert_eq!(manager.query_handles(query(2)).len(), 1);
    }

    #[test]
    fn permanent_failure_waits_for_a_new_exact_handle() {
        let manager = CatalogManager::<usize>::default();
        let calls = AtomicUsize::new(0);
        let permanent =
            CatalogManagerError::from_materialization(ConnectorMaterializationError::new(
                ConnectorMaterializationErrorClass::InvalidDefinition,
                ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                "invalid catalog definition",
            ));
        manager
            .ensure(query(1), properties(3), |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(permanent.clone())
            })
            .expect_err("permanent bind fails");
        assert!(
            manager
                .ensure(query(2), properties(3), |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(9)
                })
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *manager
                .ensure(query(3), properties(4), |_| Ok(11))
                .expect("new exact handle is independent"),
            11
        );
    }

    #[test]
    fn provider_limiter_serializes_many_exact_keys() {
        let manager = Arc::new(
            CatalogManager::<usize>::try_new(CatalogManagerConfig {
                provider_max_concurrent_binds: 1,
                ..CatalogManagerConfig::default()
            })
            .expect("valid manager"),
        );
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let run = |query, catalog: CatalogProperties| {
            let manager = Arc::clone(&manager);
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            thread::spawn(move || {
                manager.ensure(query, catalog, |_| {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(25));
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(1)
                })
            })
        };
        let first = run(query(1), properties(1));
        let second = run(query(2), properties(2));
        first.join().expect("first bind").expect("first Ready");
        second.join().expect("second bind").expect("second Ready");
        assert_eq!(peak.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cancelled_provider_limiter_waiter_completes_cell_for_a_later_retry() {
        let manager = Arc::new(
            CatalogManager::<usize>::try_new(CatalogManagerConfig {
                provider_max_concurrent_binds: 1,
                transient_retry_cooldown: Duration::ZERO,
                ..CatalogManagerConfig::default()
            })
            .expect("valid manager"),
        );
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let first_manager = Arc::clone(&manager);
        let first_entered = Arc::clone(&entered);
        let first_release = Arc::clone(&release);
        let first = thread::spawn(move || {
            first_manager.ensure(query(1), properties(1), |_| {
                first_entered.wait();
                first_release.wait();
                Ok(1)
            })
        });
        entered.wait();

        let cancelled = manager.ensure_while(query(2), properties(2), || false, |_| Ok(2));
        assert!(cancelled.is_err(), "cancelled limiter waiter must fail");
        let retry_manager = Arc::clone(&manager);
        let retry = thread::spawn(move || retry_manager.ensure(query(3), properties(2), |_| Ok(3)));
        release.wait();
        first
            .join()
            .expect("first limited bind thread")
            .expect("first limited bind completes");
        assert_eq!(
            *retry
                .join()
                .expect("retry bind thread")
                .expect("the cancelled cell must be retryable"),
            3
        );
    }

    #[test]
    fn failed_suppression_capacity_evicts_lru_without_creating_query_leases() {
        let manager = CatalogManager::<usize>::try_new(CatalogManagerConfig {
            max_failed_catalogs: 1,
            transient_retry_cooldown: Duration::from_secs(60),
            ..CatalogManagerConfig::default()
        })
        .expect("valid manager");
        for (query_id, catalog) in [(1, properties(1)), (2, properties(2))] {
            manager
                .ensure(query(query_id), catalog, |_| {
                    Err(CatalogManagerError::materialization_failed(
                        "transient failure",
                    ))
                })
                .expect_err("suppressed failure");
        }
        assert_eq!(manager.retained_handles().len(), 1);
        assert!(manager.query_handles(query(1)).is_empty());
        assert!(manager.query_handles(query(2)).is_empty());
    }

    #[test]
    fn failed_suppression_ttl_expires_without_turning_failure_into_a_lease() {
        let manager = CatalogManager::<usize>::try_new(CatalogManagerConfig {
            failed_retention: Duration::from_millis(1),
            transient_retry_cooldown: Duration::from_secs(60),
            ..CatalogManagerConfig::default()
        })
        .expect("valid manager");
        let catalog = properties(1);
        let handle = catalog.handle().clone();
        manager
            .ensure(query(1), catalog, |_| {
                Err(CatalogManagerError::materialization_failed(
                    "transient failure",
                ))
            })
            .expect_err("suppressed failure");
        thread::sleep(Duration::from_millis(2));
        manager.release_query(query(1));
        assert!(!manager.retained_handles().contains(&handle));
        assert!(manager.query_handles(query(1)).is_empty());
    }

    #[test]
    fn prune_retires_failed_suppression_for_an_absent_exact_handle() {
        let manager = CatalogManager::<usize>::default();
        let catalog = properties(1);
        let handle = catalog.handle().clone();
        manager
            .ensure(query(1), catalog, |_| {
                Err(CatalogManagerError::materialization_failed(
                    "transient failure",
                ))
            })
            .expect_err("suppressed failure");
        assert!(manager.retained_handles().contains(&handle));
        assert_eq!(
            manager.prune_unreachable(&BTreeSet::new()),
            CatalogPruneResult::Pruned {
                handles: BTreeSet::new(),
            }
        );
        assert!(!manager.retained_handles().contains(&handle));
    }

    #[test]
    fn cancelled_install_never_calls_provider_or_leases_pending_handle() {
        let manager = CatalogManager::<usize>::default();
        let calls = AtomicUsize::new(0);
        let catalog = properties(1);
        let handle = catalog.handle().clone();
        assert!(
            manager
                .ensure_while(
                    query(1),
                    catalog,
                    || false,
                    |_| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(1)
                    }
                )
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(manager.query_handles(query(1)).is_empty());
        assert!(manager.resolve(&handle).is_none());
    }

    #[test]
    fn stale_prune_candidate_cannot_remove_a_reinstalled_handle() {
        let manager = CatalogManager::<usize>::default();
        let catalog = properties(4);
        let handle = catalog.handle().clone();
        manager
            .ensure(query(1), catalog.clone(), |_| Ok(1))
            .expect("first runtime");
        let stale_candidate = {
            let state = manager.state.lock().expect("manager state");
            state
                .entries
                .get(&handle)
                .expect("first entry")
                .ready_candidate(&handle)
                .expect("first entry is registered")
        };
        manager.release_query(query(1));
        manager.prune_unreachable(&BTreeSet::new());
        assert!(manager.resolve(&handle).is_none());

        manager
            .ensure(query(2), catalog, |_| Ok(2))
            .expect("second runtime");
        let removed = {
            let mut state = manager.state.lock().expect("manager state");
            remove_ready_candidate_if_current(&mut state, &stale_candidate)
        };
        assert!(!removed);
        assert_eq!(*manager.resolve(&handle).expect("new runtime remains"), 2);
    }

    #[test]
    fn old_and_new_catalog_versions_coexist_while_queries_hold_leases() {
        let manager = CatalogManager::<usize>::default();
        let old = properties(1);
        let new = properties(2);
        manager
            .ensure(query(1), old.clone(), |_| Ok(1))
            .expect("old runtime");
        manager
            .ensure(query(2), new.clone(), |_| Ok(2))
            .expect("new runtime");
        assert_eq!(*manager.resolve(old.handle()).expect("old resolves"), 1);
        assert_eq!(*manager.resolve(new.handle()).expect("new resolves"), 2);
        assert_eq!(manager.release_query(query(1)), BTreeSet::new());
        assert!(manager.resolve(old.handle()).is_some());
        assert!(manager.resolve(new.handle()).is_some());
    }

    #[test]
    fn lease_snapshot_counts_queries_and_exact_catalog_handles() {
        let manager = CatalogManager::<usize>::default();
        let first = properties(1);
        let second = properties(2);

        manager
            .ensure(query(1), first, |_| Ok(1))
            .expect("first catalog is leased to query one");
        manager
            .ensure(query(1), second.clone(), |_| Ok(2))
            .expect("second catalog is leased to query one");
        manager
            .ensure(query(2), second, |_| Ok(2))
            .expect("second catalog is leased to query two");

        assert_eq!(
            manager.lease_snapshot(),
            super::CatalogLeaseSnapshot {
                query_leases: 2,
                handle_leases: 3,
            }
        );

        manager.release_query(query(1));
        assert_eq!(
            manager.lease_snapshot(),
            super::CatalogLeaseSnapshot {
                query_leases: 1,
                handle_leases: 1,
            }
        );
    }

    #[test]
    fn prune_rejects_stale_reachability_without_evicting_live_catalogs() {
        let manager = CatalogManager::<usize>::default();
        let catalog = properties(1);
        let handle = catalog.handle().clone();
        manager
            .ensure(query(1), catalog, |_| Ok(1))
            .expect("runtime");
        let result = manager.prune_unreachable(&BTreeSet::new());
        assert_eq!(
            result,
            CatalogPruneResult::Rejected {
                missing_live_handles: BTreeSet::from([handle.clone()]),
            }
        );
        assert_eq!(*manager.resolve(&handle).expect("still retained"), 1);
    }

    #[test]
    fn prune_removes_unreachable_ready_entries_but_not_live_or_in_progress_entries() {
        let manager = Arc::new(CatalogManager::<usize>::default());
        let ready = properties(1);
        let ready_handle = ready.handle().clone();
        manager
            .ensure(query(1), ready, |_| Ok(1))
            .expect("ready runtime");
        manager.release_query(query(1));

        let loading = properties(2);
        let loading_handle = loading.handle().clone();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let loader_manager = Arc::clone(&manager);
        let loader_entered = Arc::clone(&entered);
        let loader_release = Arc::clone(&release);
        let thread = thread::spawn(move || {
            loader_manager.ensure(query(2), loading, |_| {
                loader_entered.wait();
                loader_release.wait();
                Ok(2)
            })
        });
        entered.wait();
        let result = manager.prune_unreachable(&BTreeSet::from([loading_handle.clone()]));
        assert_eq!(
            result,
            CatalogPruneResult::Pruned {
                handles: BTreeSet::from([ready_handle.clone()]),
            }
        );
        assert!(manager.resolve(&ready_handle).is_none());
        assert!(manager.retained_handles().contains(&loading_handle));
        release.wait();
        thread
            .join()
            .expect("loading ensure")
            .expect("loaded runtime");
    }

    #[test]
    fn release_enforces_bounded_retention_after_queries_finish() {
        let manager = CatalogManager::<usize>::try_new(CatalogManagerConfig {
            max_retained_catalogs: 1,
            ..CatalogManagerConfig::default()
        })
        .expect("valid manager");
        let first = properties(1);
        let second = properties(2);
        manager
            .ensure(query(1), first.clone(), |_| Ok(1))
            .expect("first runtime");
        manager
            .ensure(query(2), second.clone(), |_| Ok(2))
            .expect("second runtime");
        manager.release_query(query(1));
        assert!(manager.resolve(first.handle()).is_none());
        assert!(manager.resolve(second.handle()).is_some());
        manager.release_query(query(2));
        assert_eq!(manager.retained_handles().len(), 1);
        assert!(manager.resolve(first.handle()).is_none());
        assert_eq!(
            *manager
                .resolve(second.handle())
                .expect("newest stays by key order"),
            2
        );
    }

    #[test]
    fn zero_retention_limit_is_rejected() {
        assert!(matches!(
            CatalogManager::<()>::try_new(CatalogManagerConfig {
                max_retained_catalogs: 0,
                ..CatalogManagerConfig::default()
            }),
            Err(CatalogManagerError::InvalidConfiguration(
                "catalog manager must retain at least one catalog"
            ))
        ));
    }
}
