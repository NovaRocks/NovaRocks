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

//! Frontend-owned connector instance installation barrier.
//!
//! A declaration is control-plane state, never fragment-carrier data.  The
//! compiler derives one declaration set for each BE that will host a connector
//! scan and requires all installs to ACK before native submissions exist.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorInstanceId,
};

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::lifecycle::QueryExecutionId;
use crate::query_execution::preparation::PreparedFragmentSet;
use crate::query_execution::schedule::SchedulingPlan;
use crate::query_execution::write_plan::ConnectorWritePlanAttachment;

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

/// One BE's deduplicated connector declarations.  This is intentionally a
/// control-plane DTO: no scan handle, split, credential, or client is present.
pub struct ConnectorBindingBackendInstallPlan {
    backend_idx: usize,
    endpoint: SocketAddr,
    declarations: Vec<ConnectorExecutionDeclaration>,
}

impl ConnectorBindingBackendInstallPlan {
    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub fn declarations(&self) -> &[ConnectorExecutionDeclaration] {
        &self.declarations
    }
}

/// Complete install barrier input for one scheduled query.
pub struct ConnectorBindingInstallPlan {
    backends: Vec<ConnectorBindingBackendInstallPlan>,
}

impl ConnectorBindingInstallPlan {
    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    pub fn declaration_count(&self) -> usize {
        self.backends
            .iter()
            .map(|backend| backend.declarations.len())
            .sum()
    }

    pub fn backends(&self) -> &[ConnectorBindingBackendInstallPlan] {
        &self.backends
    }
}

/// Transport-only installation port.  The frontend owns deduplication and
/// ordering; the implementation only sends the supplied declaration to the
/// supplied already-selected BE.
pub trait ConnectorBindingDispatcher: Send + Sync + 'static {
    fn install(
        &self,
        execution_id: QueryExecutionId,
        backend_idx: usize,
        endpoint: SocketAddr,
        declaration: &ConnectorExecutionDeclaration,
    ) -> Result<(), String>;

    /// Best-effort process binding retirement. Query lease release remains
    /// solely owned by the established query terminal lifecycle.
    fn retire(
        &self,
        endpoint: SocketAddr,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<(), String>;
}

/// Frontend-local observer for successful ensure acknowledgements.  It is
/// deliberately separate from the transport port: core owns neither the
/// control-generation registry nor any catalog lifecycle state.
pub trait ConnectorBindingInstallObserver: Send + Sync + 'static {
    fn installed(
        &self,
        endpoint: SocketAddr,
        declaration: &ConnectorExecutionDeclaration,
    ) -> Result<(), String>;
}

#[derive(Default)]
pub struct NoopConnectorBindingInstallObserver;

impl ConnectorBindingInstallObserver for NoopConnectorBindingInstallObserver {
    fn installed(&self, _: SocketAddr, _: &ConnectorExecutionDeclaration) -> Result<(), String> {
        Ok(())
    }
}

/// Build the production gRPC control adapter for an immutable backend
/// snapshot.  It shares the native fragment dispatch endpoints but does not
/// share fragment payloads or fallback registries.
pub fn new_grpc_connector_binding_dispatcher(
    backends: &[(usize, SocketAddr)],
) -> Result<Arc<dyn ConnectorBindingDispatcher>, String> {
    Ok(Arc::new(
        crate::service::grpc_fragment_dispatcher::GrpcConnectorBindingControl::new(backends)?,
    ))
}

/// Linear evidence that the connector install barrier completed.  Connector
/// instances are process-scoped and deliberately remain installed after one
/// query; consuming this lease only records terminal query ownership.
#[must_use = "connector installs must be consumed with native execution cleanup"]
#[derive(Debug)]
pub struct ConnectorBindingInstallLease;

impl ConnectorBindingInstallLease {
    pub fn release(self) {}

    pub fn abort_preserving(self, primary_error: String) -> String {
        primary_error
    }
}

/// Frontend-owned all-BE install/ACK barrier.
pub trait ConnectorBindingInstallBarrier: Send + Sync + 'static {
    fn install_all(
        &self,
        execution_id: QueryExecutionId,
        plan: ConnectorBindingInstallPlan,
    ) -> Result<ConnectorBindingInstallLease, DistributedQueryError>;
}

/// Derive the only permitted instance distribution plan from prepared reads
/// and placement-frozen writers at their actual BE placements. A scan node
/// having an empty assigned split set is still installed: an empty source can
/// be opened by the fragment and must resolve its real instance without a
/// query-local fallback. Write-only fragments install the exact declaration
/// retained by their planning lease, never a declaration for a later current
/// incarnation.
pub(crate) fn compile_install_plan(
    prepared: &PreparedFragmentSet,
    schedule: &SchedulingPlan,
    connector_write_plan: Option<&ConnectorWritePlanAttachment>,
) -> Result<ConnectorBindingInstallPlan, DistributedQueryError> {
    let mut by_backend: BTreeMap<
        usize,
        (
            SocketAddr,
            BTreeMap<ConnectorInstanceId, ConnectorExecutionDeclaration>,
        ),
    > = BTreeMap::new();

    for (&fragment_id, placements) in &schedule.by_fragment {
        for placement in placements {
            let endpoint = placement_socket_addr(&placement.endpoint)?;
            let entry = by_backend
                .entry(placement.backend_idx)
                .or_insert_with(|| (endpoint, BTreeMap::new()));
            if entry.0 != endpoint {
                return Err(contract_error(format!(
                    "connector binding schedule assigns backend {} to conflicting endpoints {} and {}",
                    placement.backend_idx, entry.0, endpoint
                )));
            }
            for &node_id in placement.connector_splits.keys() {
                let read = prepared
                    .scan_bindings()
                    .connector_read(fragment_id, node_id)
                    .ok_or_else(|| {
                        contract_error(format!(
                            "scheduled connector split has no prepared read for fragment {fragment_id} node {node_id}"
                        ))
                    })?;
                if read.planning_lease.is_none() {
                    return Err(contract_error(format!(
                        "connector read fragment_id={fragment_id} node_id={node_id} is missing its control planning lease"
                    )));
                }
                let instance_id = read.declaration.descriptor().instance_id.clone();
                match entry.1.get(&instance_id) {
                    Some(existing) if existing != &read.declaration => {
                        return Err(contract_error(format!(
                            "connector instance '{}' has conflicting declarations for backend {}",
                            instance_id.as_str(),
                            placement.backend_idx
                        )));
                    }
                    Some(_) => {}
                    None => {
                        entry.1.insert(instance_id, read.declaration.clone());
                    }
                }
            }
        }
    }

    if let Some(attachment) = connector_write_plan {
        let declaration = attachment.execution_declaration();
        let instance_id = declaration.descriptor().instance_id.clone();
        for writer in attachment.manifest().writers() {
            let fragment_id = u32::try_from(writer.fragment_id()).map_err(|_| {
                contract_error("connector writer manifest contains a negative fragment ID")
            })?;
            let placements = schedule.by_fragment.get(&fragment_id).ok_or_else(|| {
                contract_error(format!(
                    "connector writer manifest references absent fragment {fragment_id}"
                ))
            })?;
            let placement = placements
                .iter()
                .find(|placement| {
                    i32::try_from(placement.instance_index).ok() == Some(writer.backend_num())
                        && writer.fragment_instance_id()
                            == connector_writer_fragment_instance_bytes(placement.finst_id)
                })
                .ok_or_else(|| {
                    contract_error(format!(
                        "connector writer manifest does not match a scheduled placement for fragment {fragment_id} backend {}",
                        writer.backend_num()
                    ))
                })?;
            let endpoint = placement_socket_addr(&placement.endpoint)?;
            let entry = by_backend
                .entry(placement.backend_idx)
                .or_insert_with(|| (endpoint, BTreeMap::new()));
            if entry.0 != endpoint {
                return Err(contract_error(format!(
                    "connector binding writer schedule assigns backend {} to conflicting endpoints {} and {}",
                    placement.backend_idx, entry.0, endpoint
                )));
            }
            match entry.1.get(&instance_id) {
                Some(existing) if existing != declaration => {
                    return Err(contract_error(format!(
                        "connector instance '{}' has conflicting read/write declarations for backend {}",
                        instance_id.as_str(),
                        placement.backend_idx
                    )));
                }
                Some(_) => {}
                None => {
                    entry.1.insert(instance_id.clone(), declaration.clone());
                }
            }
        }
    }

    Ok(ConnectorBindingInstallPlan {
        backends: by_backend
            .into_iter()
            .map(
                |(backend_idx, (endpoint, declarations))| ConnectorBindingBackendInstallPlan {
                    backend_idx,
                    endpoint,
                    declarations: declarations.into_values().collect(),
                },
            )
            .collect(),
    })
}

fn connector_writer_fragment_instance_bytes(value: crate::common::types::UniqueId) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&value.high().to_be_bytes());
    bytes[8..].copy_from_slice(&value.low().to_be_bytes());
    bytes
}

fn placement_socket_addr(
    endpoint: &crate::runtime::endpoint::RuntimeEndpoint,
) -> Result<SocketAddr, DistributedQueryError> {
    let ip = endpoint.host().parse::<IpAddr>().map_err(|_| {
        contract_error(format!(
            "connector binding requires an IP-native scheduled endpoint, got '{}'",
            endpoint.host()
        ))
    })?;
    let port = u16::try_from(endpoint.port()).map_err(|_| {
        contract_error(format!(
            "connector binding scheduled endpoint '{}' has invalid port {}",
            endpoint.host(),
            endpoint.port()
        ))
    })?;
    Ok(SocketAddr::new(ip, port))
}

/// Serial barrier implementation shared by production and focused tests.
/// Installation is idempotent at the BE host, so a retry after a transport
/// ambiguity is safe and does not manufacture a second registry entry.
pub struct DispatchingConnectorBindingBarrier {
    dispatcher: Arc<dyn ConnectorBindingDispatcher>,
    observer: Arc<dyn ConnectorBindingInstallObserver>,
}

impl DispatchingConnectorBindingBarrier {
    pub fn new(dispatcher: Arc<dyn ConnectorBindingDispatcher>) -> Self {
        Self {
            dispatcher,
            observer: Arc::new(NoopConnectorBindingInstallObserver),
        }
    }

    pub fn with_observer(
        dispatcher: Arc<dyn ConnectorBindingDispatcher>,
        observer: Arc<dyn ConnectorBindingInstallObserver>,
    ) -> Self {
        Self {
            dispatcher,
            observer,
        }
    }
}

impl ConnectorBindingInstallBarrier for DispatchingConnectorBindingBarrier {
    fn install_all(
        &self,
        execution_id: QueryExecutionId,
        plan: ConnectorBindingInstallPlan,
    ) -> Result<ConnectorBindingInstallLease, DistributedQueryError> {
        for backend in plan.backends() {
            for declaration in backend.declarations() {
                self.dispatcher
                    .install(
                        execution_id,
                        backend.backend_idx(),
                        backend.endpoint(),
                        declaration,
                    )
                    .map_err(|error| {
                        failed(format!(
                            "connector instance '{}' installation on BE[{}] ({}) failed: {error}",
                            declaration.descriptor().instance_id.as_str(),
                            backend.backend_idx(),
                            backend.endpoint()
                        ))
                    })?;
                self.observer
                    .installed(backend.endpoint(), declaration)
                    .map_err(|error| {
                        failed(format!(
                            "connector instance '{}' installation acknowledgement could not be recorded for BE[{}] ({}): {error}",
                            declaration.descriptor().instance_id.as_str(),
                            backend.backend_idx(),
                            backend.endpoint()
                        ))
                    })?;
            }
        }
        Ok(ConnectorBindingInstallLease)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::{AttemptId, QueryExecutionId};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceIncarnation, ConnectorProviderId,
    };

    use super::*;

    fn declaration(instance_id: &str) -> ConnectorExecutionDeclaration {
        ConnectorExecutionDeclaration::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse(instance_id).unwrap(),
            },
            ConnectorInstanceIncarnation::from_bytes([7; 16]),
            Bytes::from_static(b"binding=default"),
        )
        .unwrap()
    }

    struct RecordingDispatcher {
        installs: Mutex<Vec<(usize, SocketAddr, String)>>,
        fail_on: Option<usize>,
    }

    impl ConnectorBindingDispatcher for RecordingDispatcher {
        fn install(
            &self,
            _execution_id: QueryExecutionId,
            backend_idx: usize,
            endpoint: SocketAddr,
            declaration: &ConnectorExecutionDeclaration,
        ) -> Result<(), String> {
            let mut installs = self.installs.lock().unwrap();
            if self.fail_on == Some(installs.len()) {
                return Err("injected install failure".to_string());
            }
            installs.push((
                backend_idx,
                endpoint,
                declaration.descriptor().instance_id.as_str().to_string(),
            ));
            Ok(())
        }

        fn retire(
            &self,
            _endpoint: SocketAddr,
            _key: &ConnectorExecutionBindingKey,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn dispatch_barrier_installs_each_backend_declaration_once() {
        let dispatcher = Arc::new(RecordingDispatcher {
            installs: Mutex::new(Vec::new()),
            fail_on: None,
        });
        let barrier = DispatchingConnectorBindingBarrier::new(dispatcher.clone());
        let plan = ConnectorBindingInstallPlan {
            backends: vec![
                ConnectorBindingBackendInstallPlan {
                    backend_idx: 3,
                    endpoint: "127.0.0.1:19033".parse().unwrap(),
                    declarations: vec![declaration("catalog.one"), declaration("catalog.two")],
                },
                ConnectorBindingBackendInstallPlan {
                    backend_idx: 9,
                    endpoint: "127.0.0.1:19039".parse().unwrap(),
                    declarations: vec![declaration("catalog.one")],
                },
            ],
        };

        barrier
            .install_all(execution_id(), plan)
            .expect("all installs ACK")
            .release();
        assert_eq!(dispatcher.installs.lock().unwrap().len(), 3);
    }

    #[test]
    fn dispatch_barrier_stops_before_any_following_install_after_failure() {
        let dispatcher = Arc::new(RecordingDispatcher {
            installs: Mutex::new(Vec::new()),
            fail_on: Some(1),
        });
        let barrier = DispatchingConnectorBindingBarrier::new(dispatcher.clone());
        let plan = ConnectorBindingInstallPlan {
            backends: vec![ConnectorBindingBackendInstallPlan {
                backend_idx: 3,
                endpoint: "127.0.0.1:19033".parse().unwrap(),
                declarations: vec![declaration("catalog.one"), declaration("catalog.two")],
            }],
        };

        let error = barrier
            .install_all(execution_id(), plan)
            .expect_err("second install fails");
        assert_eq!(error.kind(), DistributedQueryErrorKind::Failed);
        assert_eq!(dispatcher.installs.lock().unwrap().len(), 1);
    }

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(7, 9),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("valid execution id")
    }
}
