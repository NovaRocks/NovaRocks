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

use novarocks::query_execution::lifecycle::QueryExecutionId;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBinding, ConnectorExecutionBindingKey,
    ConnectorExecutionDeclaration, ConnectorExecutionInstaller, ConnectorExecutionResolver,
    ConnectorProviderId, ConnectorRequestContext,
};

/// Process-scoped owner of BE-only connector execution bindings. It is not a
/// catalog registry: installers are startup composed, declarations are opaque,
/// and fragments can only resolve generations leased to their query.
#[derive(Clone, Default)]
pub struct ConnectorExecutionHost {
    state: Arc<Mutex<ExecutionHostState>>,
}

#[derive(Default)]
struct ExecutionHostState {
    installers: BTreeMap<ConnectorProviderId, Arc<dyn ConnectorExecutionInstaller>>,
    bindings: BTreeMap<ConnectorExecutionBindingKey, Arc<BindingCell>>,
    retiring: BTreeSet<ConnectorExecutionBindingKey>,
    query_leases: BTreeMap<QueryExecutionId, BTreeSet<ConnectorExecutionBindingKey>>,
    shutting_down: bool,
}

struct BindingCell {
    state: Mutex<BindingCellState>,
    changed: Condvar,
}

enum BindingCellState {
    Empty,
    Installing,
    Ready {
        digest: [u8; 32],
        binding: Arc<ConnectorExecutionBinding>,
    },
}

impl Default for BindingCell {
    fn default() -> Self {
        Self {
            state: Mutex::new(BindingCellState::Empty),
            changed: Condvar::new(),
        }
    }
}

impl ConnectorExecutionHost {
    pub fn publish_resource_snapshot(&self) {
        let (query_leases, binding_leases) = {
            let state = self.state.lock().expect("connector execution host lock");
            (
                state.query_leases.len(),
                state.query_leases.values().map(BTreeSet::len).sum(),
            )
        };
        novarocks::service::publish_backend_query_execution_resource(
            "connector_query_leases",
            query_leases,
        );
        novarocks::service::publish_backend_query_execution_resource(
            "connector_binding_leases",
            binding_leases,
        );
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_installer(
        &self,
        installer: Arc<dyn ConnectorExecutionInstaller>,
    ) -> Result<(), ConnectorError> {
        let mut state = self.lock_state()?;
        if state.shutting_down {
            return Err(unavailable("connector execution host is shutting down"));
        }
        let provider_id = installer.provider_id().clone();
        if state.installers.contains_key(&provider_id) {
            return Err(invalid(format!(
                "connector execution installer `{}` is already registered",
                provider_id.as_str()
            )));
        }
        state.installers.insert(provider_id, installer);
        Ok(())
    }

    /// Ensures a binding is installed and leases its exact generation to the
    /// supplied query. Installation happens outside the host lock; concurrent
    /// ensures for the same key share one per-key cell.
    pub fn ensure(
        &self,
        query: QueryExecutionId,
        declaration: &ConnectorExecutionDeclaration,
        context: &ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        let key = declaration.binding_key();
        let (installer, cell) = {
            let mut state = self.lock_state()?;
            ensure_admissible(&state, query, &key)?;
            let installer = state
                .installers
                .get(&declaration.descriptor().provider_id)
                .cloned()
                .ok_or_else(|| {
                    invalid(format!(
                        "no startup installer is registered for connector provider `{}`",
                        declaration.descriptor().provider_id.as_str()
                    ))
                })?;
            let cell = state
                .bindings
                .entry(key.clone())
                .or_insert_with(|| Arc::new(BindingCell::default()))
                .clone();
            (installer, cell)
        };

        let binding = cell.ensure(installer.as_ref(), declaration, context)?;
        if binding.provider_id() != &declaration.descriptor().provider_id || binding.key() != &key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector execution installer returned a mismatched binding",
            ));
        }

        let mut state = self.lock_state()?;
        ensure_admissible(&state, query, &key)?;
        state.query_leases.entry(query).or_default().insert(key);
        drop(state);
        self.publish_resource_snapshot();
        Ok(())
    }

    pub fn retire(&self, key: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        let mut state = self.lock_state()?;
        if state.shutting_down {
            return Err(unavailable("connector execution host is shutting down"));
        }
        if !state.bindings.contains_key(key) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                format!(
                    "connector execution binding `{}` is not installed",
                    key.instance_id.as_str()
                ),
            ));
        }
        state.retiring.insert(key.clone());
        Ok(())
    }

    pub fn resolver_for(&self, query: QueryExecutionId) -> ConnectorExecutionQueryResolver {
        ConnectorExecutionQueryResolver {
            host: Arc::downgrade(&self.state),
            query,
        }
    }

    /// Returns a query-lifecycle cleanup lease. The lifecycle is responsible
    /// for closing it on Finalize, Abort, deadline, KILL QUERY, and shutdown;
    /// Drop is only the safety net for failed setup.
    pub fn query_lease(&self, query: QueryExecutionId) -> ConnectorExecutionLease {
        ConnectorExecutionLease {
            host: Arc::downgrade(&self.state),
            query: Some(query),
        }
    }

    pub fn release_query(&self, query: QueryExecutionId) -> Result<(), ConnectorError> {
        release_query(&self.state, query)
    }

    pub fn shutdown(&self) -> Result<(), ConnectorError> {
        let mut state = self.lock_state()?;
        state.shutting_down = true;
        state.query_leases.clear();
        drop(state);
        self.publish_resource_snapshot();
        Ok(())
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ExecutionHostState>, ConnectorError> {
        self.state.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector execution host lock poisoned",
            )
        })
    }
}

impl BindingCell {
    fn ensure(
        &self,
        installer: &dyn ConnectorExecutionInstaller,
        declaration: &ConnectorExecutionDeclaration,
        context: &ConnectorRequestContext,
    ) -> Result<Arc<ConnectorExecutionBinding>, ConnectorError> {
        let digest = declaration.digest();
        let mut state = self.state.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector execution install cell lock poisoned",
            )
        })?;
        loop {
            match &*state {
                BindingCellState::Ready {
                    digest: existing,
                    binding,
                } if *existing == digest => return Ok(Arc::clone(binding)),
                BindingCellState::Ready { .. } => {
                    return Err(invalid(
                        "connector execution binding received a conflicting declaration",
                    ));
                }
                BindingCellState::Installing => {
                    state = self.changed.wait(state).map_err(|_| {
                        ConnectorError::new(
                            ConnectorErrorKind::Internal,
                            "connector execution install cell lock poisoned",
                        )
                    })?;
                }
                BindingCellState::Empty => {
                    *state = BindingCellState::Installing;
                    break;
                }
            }
        }
        drop(state);

        let installed = installer.install(declaration, context).map(Arc::new);
        let mut state = self.state.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector execution install cell lock poisoned",
            )
        })?;
        match installed {
            Ok(binding) => {
                *state = BindingCellState::Ready {
                    digest,
                    binding: Arc::clone(&binding),
                };
                self.changed.notify_all();
                Ok(binding)
            }
            Err(error) => {
                *state = BindingCellState::Empty;
                self.changed.notify_all();
                Err(error)
            }
        }
    }
}

fn ensure_admissible(
    state: &ExecutionHostState,
    query: QueryExecutionId,
    key: &ConnectorExecutionBindingKey,
) -> Result<(), ConnectorError> {
    if state.shutting_down {
        return Err(unavailable("connector execution host is shutting down"));
    }
    if state.retiring.contains(key) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            format!(
                "connector execution binding `{}` is draining",
                key.instance_id.as_str()
            ),
        ));
    }
    if state.query_leases.get(&query).is_some_and(|keys| {
        keys.iter().any(|existing| {
            existing.instance_id == key.instance_id && existing.incarnation != key.incarnation
        })
    }) {
        return Err(invalid(
            "one query cannot lease multiple connector incarnations for the same instance",
        ));
    }
    Ok(())
}

fn resolve_for_query(
    state: &Arc<Mutex<ExecutionHostState>>,
    query: QueryExecutionId,
    key: &ConnectorExecutionBindingKey,
) -> Result<Arc<ConnectorExecutionBinding>, ConnectorError> {
    let cell = {
        let state = state.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector execution host lock poisoned",
            )
        })?;
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
    let state = cell.state.lock().map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            "connector execution install cell lock poisoned",
        )
    })?;
    match &*state {
        BindingCellState::Ready { binding, .. } => Ok(Arc::clone(binding)),
        BindingCellState::Empty | BindingCellState::Installing => Err(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "connector execution binding is not ready",
        )),
    }
}

fn release_query(
    state: &Arc<Mutex<ExecutionHostState>>,
    query: QueryExecutionId,
) -> Result<(), ConnectorError> {
    let mut guard = state.lock().map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            "connector execution host lock poisoned",
        )
    })?;
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
    novarocks::service::publish_backend_query_execution_resource(
        "connector_query_leases",
        query_leases,
    );
    novarocks::service::publish_backend_query_execution_resource(
        "connector_binding_leases",
        binding_leases,
    );
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
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
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks::query_execution::lifecycle::AttemptId;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorOpenReaderRequest,
        ConnectorProviderId, ConnectorReadExecution,
    };
    use novarocks_types::QueryId;

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct TestRead {
        key: ConnectorExecutionBindingKey,
    }

    impl ConnectorReadExecution for TestRead {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn open_reader(
            &self,
            _split: &novarocks_spi::connector::ConnectorSplit,
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
        provider_id: ConnectorProviderId,
        installs: Arc<AtomicUsize>,
    }

    impl ConnectorExecutionInstaller for TestInstaller {
        fn provider_id(&self) -> &ConnectorProviderId {
            &self.provider_id
        }

        fn install(
            &self,
            declaration: &ConnectorExecutionDeclaration,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorExecutionBinding, ConnectorError> {
            self.installs.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(10));
            let key = declaration.binding_key();
            ConnectorExecutionBinding::try_new(
                self.provider_id.clone(),
                key.clone(),
                Arc::new(TestRead { key }),
            )
        }
    }

    fn descriptor() -> ConnectorInstanceDescriptor {
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("catalog.analytics").expect("instance ID"),
        }
    }

    fn declaration(incarnation: u8) -> ConnectorExecutionDeclaration {
        ConnectorExecutionDeclaration::try_new(
            descriptor(),
            ConnectorInstanceIncarnation::from_bytes([incarnation; 16]),
            Bytes::from_static(b"binding=default"),
        )
        .expect("declaration")
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("context")
    }

    fn query(value: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(1, value), AttemptId::new(1).expect("attempt"))
            .expect("query")
    }

    fn host(installs: Arc<AtomicUsize>) -> Arc<ConnectorExecutionHost> {
        let host = Arc::new(ConnectorExecutionHost::new());
        host.register_installer(Arc::new(TestInstaller {
            provider_id: descriptor().provider_id,
            installs,
        }))
        .expect("installer");
        host
    }

    #[test]
    fn concurrent_ensure_constructs_one_binding_and_leases_each_query() {
        let installs = Arc::new(AtomicUsize::new(0));
        let host = host(Arc::clone(&installs));
        let declaration = declaration(7);
        let mut joins = Vec::new();
        for value in 1..=4 {
            let host = Arc::clone(&host);
            let declaration = declaration.clone();
            joins.push(std::thread::spawn(move || {
                host.ensure(query(value), &declaration, &context())
            }));
        }
        for join in joins {
            join.join().expect("thread").expect("ensure");
        }
        assert_eq!(installs.load(Ordering::SeqCst), 1);
        assert!(
            host.resolver_for(query(1))
                .resolve(&declaration.binding_key())
                .is_ok()
        );
    }

    #[test]
    fn draining_generation_rejects_new_leases_but_old_query_can_resolve() {
        let host = host(Arc::new(AtomicUsize::new(0)));
        let old = declaration(7);
        let new = declaration(8);
        let first = query(1);
        let second = query(2);
        host.ensure(first, &old, &context()).expect("old ensure");
        host.retire(&old.binding_key()).expect("retire old");

        assert!(host.resolver_for(first).resolve(&old.binding_key()).is_ok());
        assert_eq!(
            host.ensure(second, &old, &context())
                .expect_err("draining generation must reject new query")
                .kind(),
            ConnectorErrorKind::Unavailable
        );
        host.ensure(second, &new, &context()).expect("new ensure");
        assert!(
            host.resolver_for(second)
                .resolve(&new.binding_key())
                .is_ok()
        );
        assert_eq!(
            host.ensure(second, &old, &context())
                .expect_err("one query cannot mix incarnations")
                .kind(),
            ConnectorErrorKind::Unavailable
        );

        host.release_query(first).expect("release old query");
        let released = host.resolver_for(first).resolve(&old.binding_key());
        assert_eq!(
            released
                .err()
                .expect("released query cannot resolve")
                .kind(),
            ConnectorErrorKind::NotFound
        );
    }
}
