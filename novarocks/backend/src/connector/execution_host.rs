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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use novarocks_proto::lifecycle::QueryExecutionId;
use novarocks_proto::provider::{
    EnsureConnectorExecutionBindingRejection, EnsureConnectorExecutionBindingRejectionReason,
    EnsureConnectorExecutionBindingResult, RetireConnectorExecutionBindingOutcome,
    RetireConnectorExecutionBindingResult,
};
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorError, ConnectorErrorKind, ConnectorExecutionBinding,
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorExecutionInstaller,
    ConnectorExecutionProviderKind, ConnectorExecutionResolver, ConnectorRequestContext,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

use super::binding_decode::AdmittedConnectorExecutionDeclaration;

const CONNECTOR_BINDING_ACTIVATION_TIMEOUT: Duration = Duration::from_secs(30);

// Design: ADR-0104 (docs/adr/ADR-0104-typed-connector-execution-binding-declaration.md)
/// Process-scoped owner of BE-only execution bindings. Its built-in installer
/// set is sealed before readiness, and fragments resolve only query-leased
/// exact generations.
#[derive(Clone)]
pub struct ConnectorExecutionHost {
    state: Arc<Mutex<ExecutionHostState>>,
}

struct ExecutionHostState {
    installers: BTreeMap<ConnectorExecutionProviderKind, Arc<dyn ConnectorExecutionInstaller>>,
    bindings: BTreeMap<ConnectorExecutionBindingKey, Arc<BindingCell>>,
    retiring: BTreeSet<ConnectorExecutionBindingKey>,
    query_leases: BTreeMap<QueryExecutionId, BTreeSet<ConnectorExecutionBindingKey>>,
    shutting_down: bool,
}

struct BindingCell {
    state: Mutex<BindingCellState>,
    changed: Condvar,
}

#[derive(Clone)]
enum Completion {
    Ready(Arc<ConnectorExecutionBinding>),
    Failed(EnsureConnectorExecutionBindingRejection),
}

enum BindingCellState {
    Installing {
        digest: [u8; 32],
        epoch: u64,
        completions: BTreeMap<u64, Completion>,
    },
    Ready {
        digest: [u8; 32],
        binding: Arc<ConnectorExecutionBinding>,
        completions: BTreeMap<u64, Completion>,
    },
    RetryableFailed {
        digest: [u8; 32],
        epoch: u64,
        completions: BTreeMap<u64, Completion>,
    },
    TerminalFailed {
        digest: [u8; 32],
        rejection: EnsureConnectorExecutionBindingRejection,
        completions: BTreeMap<u64, Completion>,
    },
}

enum CellAction {
    Install(u64),
    Complete(Completion),
}

impl BindingCell {
    fn installing(digest: [u8; 32]) -> Self {
        Self {
            state: Mutex::new(BindingCellState::Installing {
                digest,
                epoch: 1,
                completions: BTreeMap::new(),
            }),
            changed: Condvar::new(),
        }
    }

    fn acquire(&self, digest: [u8; 32]) -> Result<CellAction, ConnectorError> {
        let mut state = self.state.lock().map_err(cell_lock_error)?;
        loop {
            if digest_for(&state) != digest {
                return Ok(CellAction::Complete(Completion::Failed(
                    conflict_rejection(),
                )));
            }
            match &mut *state {
                BindingCellState::Installing { epoch, .. } => {
                    let observed_epoch = *epoch;
                    state = self.changed.wait(state).map_err(cell_lock_error)?;
                    if let Some(completion) = completions_for(&state).get(&observed_epoch) {
                        return Ok(CellAction::Complete(completion.clone()));
                    }
                }
                BindingCellState::Ready { binding, .. } => {
                    return Ok(CellAction::Complete(Completion::Ready(Arc::clone(binding))));
                }
                BindingCellState::TerminalFailed { rejection, .. } => {
                    return Ok(CellAction::Complete(Completion::Failed(rejection.clone())));
                }
                BindingCellState::RetryableFailed {
                    epoch, completions, ..
                } => {
                    let next_epoch = epoch.saturating_add(1);
                    let retained = std::mem::take(completions);
                    *state = BindingCellState::Installing {
                        digest,
                        epoch: next_epoch,
                        completions: retained,
                    };
                    return Ok(CellAction::Install(next_epoch));
                }
            }
        }
    }

    fn complete(
        &self,
        expected_digest: [u8; 32],
        expected_epoch: u64,
        completion: Completion,
    ) -> Result<(), ConnectorError> {
        let mut state = self.state.lock().map_err(cell_lock_error)?;
        let BindingCellState::Installing {
            digest,
            epoch,
            completions,
        } = &mut *state
        else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector execution installation completed after its cell changed state",
            ));
        };
        if *digest != expected_digest || *epoch != expected_epoch {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector execution installation completed for a stale epoch",
            ));
        }
        completions.insert(expected_epoch, completion.clone());
        let retained = std::mem::take(completions);
        *state = match completion {
            Completion::Ready(binding) => BindingCellState::Ready {
                digest: expected_digest,
                binding,
                completions: retained,
            },
            Completion::Failed(rejection) if rejection.retryable_before_progress() => {
                BindingCellState::RetryableFailed {
                    digest: expected_digest,
                    epoch: expected_epoch,
                    completions: retained,
                }
            }
            Completion::Failed(rejection) => BindingCellState::TerminalFailed {
                digest: expected_digest,
                rejection,
                completions: retained,
            },
        };
        self.changed.notify_all();
        Ok(())
    }
}

fn completions_for(state: &BindingCellState) -> &BTreeMap<u64, Completion> {
    match state {
        BindingCellState::Installing { completions, .. }
        | BindingCellState::Ready { completions, .. }
        | BindingCellState::RetryableFailed { completions, .. }
        | BindingCellState::TerminalFailed { completions, .. } => completions,
    }
}

fn digest_for(state: &BindingCellState) -> [u8; 32] {
    match state {
        BindingCellState::Installing { digest, .. }
        | BindingCellState::Ready { digest, .. }
        | BindingCellState::RetryableFailed { digest, .. }
        | BindingCellState::TerminalFailed { digest, .. } => *digest,
    }
}

impl ConnectorExecutionHost {
    /// Validates and seals exactly the built-in installer set before the BE is
    /// ready. There is intentionally no runtime registration operation.
    pub fn try_new(
        installers: impl IntoIterator<Item = Arc<dyn ConnectorExecutionInstaller>>,
    ) -> Result<Self, ConnectorError> {
        let mut sealed_installers = BTreeMap::new();
        for installer in installers {
            let kind = installer.provider_kind();
            if sealed_installers.insert(kind, installer).is_some() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("startup installer set contains duplicate {kind:?} installer"),
                ));
            }
        }
        for kind in ConnectorExecutionProviderKind::ALL {
            if !sealed_installers.contains_key(&kind) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("startup installer set is missing {kind:?} installer"),
                ));
            }
        }
        Ok(Self {
            state: Arc::new(Mutex::new(ExecutionHostState {
                installers: sealed_installers,
                bindings: BTreeMap::new(),
                retiring: BTreeSet::new(),
                query_leases: BTreeMap::new(),
                shutting_down: false,
            })),
        })
    }

    #[cfg(test)]
    pub(crate) fn empty_for_tests() -> Self {
        Self {
            state: Arc::new(Mutex::new(ExecutionHostState {
                installers: BTreeMap::new(),
                bindings: BTreeMap::new(),
                retiring: BTreeSet::new(),
                query_leases: BTreeMap::new(),
                shutting_down: false,
            })),
        }
    }

    pub fn publish_resource_snapshot(&self) {
        let (query_leases, binding_leases) = match self.state.lock() {
            Ok(state) => (
                state.query_leases.len(),
                state.query_leases.values().map(BTreeSet::len).sum(),
            ),
            Err(_) => return,
        };
        crate::metrics::publish_backend_query_execution_resource(
            "connector_query_leases",
            query_leases,
        );
        crate::metrics::publish_backend_query_execution_resource(
            "connector_binding_leases",
            binding_leases,
        );
    }

    /// Ensures a binding and leases its exact generation to the supplied query.
    /// A same-key wave has one activation owner; waiters see that owner's result
    /// before a later retry can begin.
    pub fn ensure(
        &self,
        query: QueryExecutionId,
        admitted: &AdmittedConnectorExecutionDeclaration,
    ) -> EnsureConnectorExecutionBindingResult {
        let declaration = admitted.declaration();
        let key = declaration.binding_key().clone();
        let digest = admitted.digest();
        let (installer, cell, first_activation) = match self.state.lock() {
            Ok(mut state) => {
                if let Err(rejection) = ensure_admissible(&state, query, &key) {
                    return rejected(rejection);
                }
                let Some(installer) = state.installers.get(&declaration.provider_kind()).cloned()
                else {
                    return rejected(host_unavailable(
                        "connector execution provider is not installed",
                    ));
                };
                let (cell, first_activation) = match state.bindings.get(&key) {
                    Some(cell) => (Arc::clone(cell), false),
                    None => {
                        let cell = Arc::new(BindingCell::installing(digest));
                        state.bindings.insert(key.clone(), Arc::clone(&cell));
                        (cell, true)
                    }
                };
                (installer, cell, first_activation)
            }
            Err(_) => return rejected(host_unavailable("connector execution host is unavailable")),
        };
        let action = if first_activation {
            CellAction::Install(1)
        } else {
            match cell.acquire(digest) {
                Ok(action) => action,
                Err(error) => return rejected(internal_failure(&error)),
            }
        };

        let completion = match action {
            CellAction::Complete(completion) => completion,
            CellAction::Install(epoch) => {
                let completion = self.activate(installer.as_ref(), declaration, &key);
                if let Err(error) = cell.complete(digest, epoch, completion.clone()) {
                    return rejected(internal_failure(&error));
                }
                completion
            }
        };

        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return rejected(host_unavailable("connector execution host is unavailable")),
        };
        if let Err(rejection) = ensure_admissible(&state, query, &key) {
            return rejected(rejection);
        }
        match completion {
            Completion::Ready(_) => {
                state.query_leases.entry(query).or_default().insert(key);
                drop(state);
                self.publish_resource_snapshot();
                EnsureConnectorExecutionBindingResult::ensured()
            }
            Completion::Failed(rejection) => rejected(rejection),
        }
    }

    fn activate(
        &self,
        installer: &dyn ConnectorExecutionInstaller,
        declaration: &ConnectorExecutionDeclaration,
        key: &ConnectorExecutionBindingKey,
    ) -> Completion {
        let deadline = Instant::now() + CONNECTOR_BINDING_ACTIVATION_TIMEOUT;
        if Instant::now() >= deadline {
            return Completion::Failed(deadline_exceeded());
        }
        let context = match ConnectorRequestContext::try_new(
            deadline,
            Arc::new(HostActivationCancellation),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        ) {
            Ok(context) => context,
            Err(error) => return Completion::Failed(internal_failure(&error)),
        };
        let installed = installer.install(declaration, &context);
        if Instant::now() >= deadline {
            return Completion::Failed(deadline_exceeded());
        }
        match installed {
            Ok(binding)
                if binding.key() == key
                    && binding.provider_id().as_str() == declaration.provider_id() =>
            {
                Completion::Ready(Arc::new(binding))
            }
            Ok(_) => Completion::Failed(internal_text(
                "connector execution installer returned a mismatched binding",
            )),
            Err(error) => Completion::Failed(rejection_from_install_error(&error)),
        }
    }

    pub fn retire(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> RetireConnectorExecutionBindingResult {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return RetireConnectorExecutionBindingResult::new(
                    RetireConnectorExecutionBindingOutcome::Internal,
                );
            }
        };
        if state.shutting_down {
            return RetireConnectorExecutionBindingResult::new(
                RetireConnectorExecutionBindingOutcome::Unavailable,
            );
        }
        state.retiring.insert(key.clone());
        RetireConnectorExecutionBindingResult::new(RetireConnectorExecutionBindingOutcome::Accepted)
    }

    pub fn resolver_for(&self, query: QueryExecutionId) -> ConnectorExecutionQueryResolver {
        ConnectorExecutionQueryResolver {
            host: Arc::downgrade(&self.state),
            query,
        }
    }

    pub fn query_lease(&self, query: QueryExecutionId) -> ConnectorExecutionLease {
        ConnectorExecutionLease {
            host: Arc::downgrade(&self.state),
            query: Some(query),
        }
    }

    pub fn release_query(&self, query: QueryExecutionId) -> Result<(), ConnectorError> {
        release_query(&self.state, query)
    }

    /// Shutdown clears query leases only. Cells and retiring keys remain until
    /// the process-scoped Host itself drops.
    pub fn shutdown(&self) -> Result<(), ConnectorError> {
        let mut state = self.state.lock().map_err(host_lock_error)?;
        state.shutting_down = true;
        state.query_leases.clear();
        drop(state);
        self.publish_resource_snapshot();
        Ok(())
    }
}

struct HostActivationCancellation;

impl ConnectorCancellation for HostActivationCancellation {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn ensure_admissible(
    state: &ExecutionHostState,
    query: QueryExecutionId,
    key: &ConnectorExecutionBindingKey,
) -> Result<(), EnsureConnectorExecutionBindingRejection> {
    if state.shutting_down {
        return Err(host_unavailable(
            "connector execution host is shutting down",
        ));
    }
    if state.retiring.contains(key) {
        return Err(rejection(
            EnsureConnectorExecutionBindingRejectionReason::Retiring,
            false,
            "connector execution binding is retiring",
        ));
    }
    if state.query_leases.get(&query).is_some_and(|keys| {
        keys.iter().any(|existing| {
            existing.instance_id == key.instance_id && existing.incarnation != key.incarnation
        })
    }) {
        return Err(rejection(
            EnsureConnectorExecutionBindingRejectionReason::QueryIncarnationConflict,
            false,
            "one query cannot lease multiple connector incarnations for the same instance",
        ));
    }
    Ok(())
}

fn rejection_from_install_error(
    error: &ConnectorError,
) -> EnsureConnectorExecutionBindingRejection {
    match error.kind() {
        ConnectorErrorKind::InvalidRequest
        | ConnectorErrorKind::NotFound
        | ConnectorErrorKind::PermissionDenied
        | ConnectorErrorKind::Unsupported
        | ConnectorErrorKind::CorruptData => rejection(
            EnsureConnectorExecutionBindingRejectionReason::InvalidDeclaration,
            false,
            error.message(),
        ),
        ConnectorErrorKind::Unavailable => rejection(
            EnsureConnectorExecutionBindingRejectionReason::ActivationUnavailable,
            error.retryable_before_progress(),
            error.message(),
        ),
        ConnectorErrorKind::DeadlineExceeded => deadline_exceeded(),
        ConnectorErrorKind::ResourceExhausted => rejection(
            EnsureConnectorExecutionBindingRejectionReason::ResourceExhausted,
            error.retryable_before_progress(),
            error.message(),
        ),
        ConnectorErrorKind::Cancelled => {
            internal_text("connector execution activation was cancelled")
        }
        ConnectorErrorKind::Internal => rejection(
            EnsureConnectorExecutionBindingRejectionReason::InternalFailure,
            error.retryable_before_progress(),
            error.message(),
        ),
    }
}

fn conflict_rejection() -> EnsureConnectorExecutionBindingRejection {
    rejection(
        EnsureConnectorExecutionBindingRejectionReason::ConflictingDeclaration,
        false,
        "connector execution binding received a conflicting declaration",
    )
}

fn host_unavailable(detail: &str) -> EnsureConnectorExecutionBindingRejection {
    rejection(
        EnsureConnectorExecutionBindingRejectionReason::HostUnavailable,
        false,
        detail,
    )
}

fn deadline_exceeded() -> EnsureConnectorExecutionBindingRejection {
    rejection(
        EnsureConnectorExecutionBindingRejectionReason::DeadlineExceeded,
        true,
        "connector execution activation deadline exceeded",
    )
}

fn internal_failure(error: &ConnectorError) -> EnsureConnectorExecutionBindingRejection {
    internal_text(error.message())
}

fn internal_text(detail: &str) -> EnsureConnectorExecutionBindingRejection {
    rejection(
        EnsureConnectorExecutionBindingRejectionReason::InternalFailure,
        false,
        detail,
    )
}

fn rejection(
    reason: EnsureConnectorExecutionBindingRejectionReason,
    retryable_before_progress: bool,
    detail: &str,
) -> EnsureConnectorExecutionBindingRejection {
    EnsureConnectorExecutionBindingRejection::try_new(
        reason,
        retryable_before_progress,
        truncate_safe_detail(detail),
        None,
    )
    .expect("host rejection reason matrix and safe detail bounds are fixed")
}

fn truncate_safe_detail(value: &str) -> String {
    let mut end = value.len().min(512);
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

fn rejected(
    rejection: EnsureConnectorExecutionBindingRejection,
) -> EnsureConnectorExecutionBindingResult {
    EnsureConnectorExecutionBindingResult::rejected(rejection)
}

fn resolve_for_query(
    state: &Arc<Mutex<ExecutionHostState>>,
    query: QueryExecutionId,
    key: &ConnectorExecutionBindingKey,
) -> Result<Arc<ConnectorExecutionBinding>, ConnectorError> {
    let cell = {
        let state = state.lock().map_err(host_lock_error)?;
        if !state
            .query_leases
            .get(&query)
            .is_some_and(|keys| keys.contains(key))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "connector execution binding is not leased to this query",
            ));
        }
        state.bindings.get(key).cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "connector execution binding is not installed",
            )
        })?
    };
    let state = cell.state.lock().map_err(cell_lock_error)?;
    match &*state {
        BindingCellState::Ready { binding, .. } => Ok(Arc::clone(binding)),
        BindingCellState::Installing { .. }
        | BindingCellState::RetryableFailed { .. }
        | BindingCellState::TerminalFailed { .. } => Err(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "connector execution binding is not ready",
        )),
    }
}

fn release_query(
    state: &Arc<Mutex<ExecutionHostState>>,
    query: QueryExecutionId,
) -> Result<(), ConnectorError> {
    let mut guard = state.lock().map_err(host_lock_error)?;
    guard.query_leases.remove(&query);
    let (query_leases, binding_leases) = (
        guard.query_leases.len(),
        guard
            .query_leases
            .values()
            .map(BTreeSet::len)
            .sum::<usize>(),
    );
    drop(guard);
    crate::metrics::publish_backend_query_execution_resource(
        "connector_query_leases",
        query_leases,
    );
    crate::metrics::publish_backend_query_execution_resource(
        "connector_binding_leases",
        binding_leases,
    );
    Ok(())
}

fn host_lock_error<T>(_error: std::sync::PoisonError<T>) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "connector execution host lock poisoned",
    )
}

fn cell_lock_error<T>(_error: std::sync::PoisonError<T>) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "connector execution install cell lock poisoned",
    )
}

/// Resolver passed to one native fragment/query decode context.
pub struct ConnectorExecutionQueryResolver {
    host: Weak<Mutex<ExecutionHostState>>,
    query: QueryExecutionId,
}

impl ConnectorExecutionResolver for ConnectorExecutionQueryResolver {
    fn resolve(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<Arc<ConnectorExecutionBinding>, ConnectorError> {
        let host = self.host.upgrade().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "connector execution host is no longer available",
            )
        })?;
        resolve_for_query(&host, self.query, key)
    }
}

/// Cleanup token attached to the existing query lifecycle. It carries no
/// control-plane authority and cannot be sent as a fragment capability.
pub struct ConnectorExecutionLease {
    host: Weak<Mutex<ExecutionHostState>>,
    query: Option<QueryExecutionId>,
}

impl ConnectorExecutionLease {
    pub fn release(mut self) -> Result<(), ConnectorError> {
        self.release_inner()
    }

    fn release_inner(&mut self) -> Result<(), ConnectorError> {
        let Some(query) = self.query.take() else {
            return Ok(());
        };
        let Some(host) = self.host.upgrade() else {
            return Ok(());
        };
        release_query(&host, query)
    }
}

impl Drop for ConnectorExecutionLease {
    fn drop(&mut self) {
        let _ = self.release_inner();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use novarocks_proto::lifecycle::AttemptId;
    use novarocks_proto::provider::EnsureConnectorExecutionBindingOutcome;
    use novarocks_spi::connector::{
        ConnectorExecutionBindingKey, ConnectorOpenReaderRequest, ConnectorPrepareSplitRequest,
        ConnectorPreparedScanUnit, ConnectorPreparedScanUnitDescriptor,
        ConnectorPreparedScanUnitSet, ConnectorProviderId, ConnectorReadExecution,
        ConnectorScanUnitDomainFacts, ConnectorScanUnitFactsMissingReason,
    };
    use novarocks_types::QueryId;

    use super::*;

    struct TestRead {
        key: ConnectorExecutionBindingKey,
    }

    impl ConnectorReadExecution for TestRead {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }
        fn prepare_split(
            &self,
            split: &novarocks_spi::connector::ConnectorSplit,
            request: ConnectorPrepareSplitRequest,
        ) -> Result<ConnectorPreparedScanUnitSet, ConnectorError> {
            ConnectorPreparedScanUnitSet::try_new(
                self.key.clone(),
                split,
                Bytes::new(),
                vec![ConnectorPreparedScanUnitDescriptor::try_new(
                    Bytes::from_static(b"test-unit"),
                    split.estimated_bytes(),
                    ConnectorScanUnitDomainFacts::missing(
                        ConnectorScanUnitFactsMissingReason::ProviderUnsupported,
                    ),
                )?],
                &request,
            )
        }
        fn open_unit_reader(
            &self,
            _unit: &ConnectorPreparedScanUnit,
            _request: ConnectorOpenReaderRequest,
        ) -> Result<Box<dyn novarocks_spi::connector::ConnectorBatchReader>, ConnectorError>
        {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "test read does not open batches",
            ))
        }
    }

    struct TestInstaller {
        provider_kind: ConnectorExecutionProviderKind,
        provider_id: ConnectorProviderId,
        installs: Arc<AtomicUsize>,
        error: Option<ConnectorError>,
    }

    impl ConnectorExecutionInstaller for TestInstaller {
        fn provider_kind(&self) -> ConnectorExecutionProviderKind {
            self.provider_kind
        }
        fn install(
            &self,
            declaration: &ConnectorExecutionDeclaration,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorExecutionBinding, ConnectorError> {
            self.installs.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            let key = ConnectorExecutionBindingKey::from(declaration);
            ConnectorExecutionBinding::try_new(
                self.provider_id.clone(),
                key.clone(),
                Arc::new(TestRead { key }),
            )
        }
    }

    fn installer(
        provider_kind: ConnectorExecutionProviderKind,
        installs: Arc<AtomicUsize>,
        error: Option<ConnectorError>,
    ) -> Arc<dyn ConnectorExecutionInstaller> {
        Arc::new(TestInstaller {
            provider_kind,
            provider_id: ConnectorProviderId::parse(provider_kind.provider_id())
                .expect("provider ID"),
            installs,
            error,
        })
    }

    fn host(installs: Arc<AtomicUsize>, error: Option<ConnectorError>) -> ConnectorExecutionHost {
        ConnectorExecutionHost::try_new([
            installer(ConnectorExecutionProviderKind::Iceberg, installs, error),
            installer(
                ConnectorExecutionProviderKind::StarRocks,
                Arc::new(AtomicUsize::new(0)),
                None,
            ),
        ])
        .expect("complete installer set")
    }

    fn declaration(incarnation: u8, binding: &str) -> ConnectorExecutionDeclaration {
        ConnectorExecutionDeclaration::iceberg("catalog.analytics", [incarnation; 16], binding)
            .expect("declaration")
    }
    fn admitted(
        declaration: ConnectorExecutionDeclaration,
    ) -> AdmittedConnectorExecutionDeclaration {
        let mut digest = [0; 32];
        digest[0] = declaration.binding_key().incarnation.to_bytes()[0];
        digest[1] = declaration
            .iceberg_access_binding()
            .unwrap_or_default()
            .bytes()
            .fold(0, u8::wrapping_add);
        super::super::binding_decode::admitted_for_tests(declaration, digest)
    }
    fn query(value: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(1, value), AttemptId::new(1).expect("attempt"))
            .expect("query")
    }
    fn rejection(
        result: EnsureConnectorExecutionBindingResult,
    ) -> EnsureConnectorExecutionBindingRejection {
        match result.outcome() {
            EnsureConnectorExecutionBindingOutcome::Rejected(rejection) => rejection.clone(),
            EnsureConnectorExecutionBindingOutcome::Ensured => panic!("expected rejection"),
        }
    }

    #[test]
    fn sealed_set_rejects_missing_and_duplicate_installers() {
        let installs = Arc::new(AtomicUsize::new(0));
        assert!(
            ConnectorExecutionHost::try_new([installer(
                ConnectorExecutionProviderKind::Iceberg,
                Arc::clone(&installs),
                None,
            )])
            .is_err()
        );
        assert!(
            ConnectorExecutionHost::try_new([
                installer(
                    ConnectorExecutionProviderKind::Iceberg,
                    Arc::clone(&installs),
                    None
                ),
                installer(ConnectorExecutionProviderKind::Iceberg, installs, None),
            ])
            .is_err()
        );
    }

    #[test]
    fn digest_lock_and_terminal_replay_are_stable() {
        let installs = Arc::new(AtomicUsize::new(0));
        let execution_host = host(Arc::clone(&installs), None);
        let first_declaration = declaration(7, "default");
        assert!(matches!(
            execution_host
                .ensure(query(1), &admitted(first_declaration.clone()))
                .outcome(),
            EnsureConnectorExecutionBindingOutcome::Ensured
        ));
        assert!(matches!(
            execution_host
                .ensure(query(2), &admitted(first_declaration.clone()))
                .outcome(),
            EnsureConnectorExecutionBindingOutcome::Ensured
        ));
        assert_eq!(installs.load(Ordering::SeqCst), 1);
        assert_eq!(
            rejection(execution_host.ensure(query(3), &admitted(declaration(7, "other")))).reason(),
            EnsureConnectorExecutionBindingRejectionReason::ConflictingDeclaration,
        );

        let installs = Arc::new(AtomicUsize::new(0));
        let terminal_host = host(
            Arc::clone(&installs),
            Some(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "bad",
            )),
        );
        let terminal_declaration = declaration(8, "default");
        assert_eq!(
            rejection(terminal_host.ensure(query(4), &admitted(terminal_declaration.clone())))
                .reason(),
            EnsureConnectorExecutionBindingRejectionReason::InvalidDeclaration
        );
        assert_eq!(
            rejection(terminal_host.ensure(query(5), &admitted(terminal_declaration.clone())))
                .reason(),
            EnsureConnectorExecutionBindingRejectionReason::InvalidDeclaration
        );
        assert_eq!(installs.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn retryable_failure_retries_and_retirement_keeps_old_lease() {
        let installs = Arc::new(AtomicUsize::new(0));
        let retry_host = host(
            Arc::clone(&installs),
            Some(
                ConnectorError::new(ConnectorErrorKind::Unavailable, "temporary")
                    .with_retryable_before_progress(),
            ),
        );
        let declaration = declaration(7, "default");
        for value in [1, 2] {
            let error = rejection(retry_host.ensure(query(value), &admitted(declaration.clone())));
            assert_eq!(
                error.reason(),
                EnsureConnectorExecutionBindingRejectionReason::ActivationUnavailable
            );
            assert!(error.retryable_before_progress());
        }
        assert_eq!(installs.load(Ordering::SeqCst), 2);

        let ready_host = host(Arc::new(AtomicUsize::new(0)), None);
        let first = query(3);
        assert!(matches!(
            ready_host
                .ensure(first, &admitted(declaration.clone()))
                .outcome(),
            EnsureConnectorExecutionBindingOutcome::Ensured
        ));
        let key = ConnectorExecutionBindingKey::from(&declaration);
        assert_eq!(
            ready_host.retire(&key).outcome(),
            RetireConnectorExecutionBindingOutcome::Accepted
        );
        assert!(ready_host.resolver_for(first).resolve(&key).is_ok());
        assert_eq!(
            rejection(ready_host.ensure(query(4), &admitted(declaration))).reason(),
            EnsureConnectorExecutionBindingRejectionReason::Retiring
        );
    }
}
