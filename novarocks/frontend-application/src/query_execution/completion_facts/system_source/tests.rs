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

//! Custody regressions use the real Source receiving and close algorithm.
//! This layer proves borrower/actual-exit behavior; connector route and keyed
//! StateStore integration are exercised separately through their real ports.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
struct Cursor {
    gate: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
    error: ConnectorError,
}
impl Drop for Cursor {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}
#[async_trait]
impl MaterializedViewInventory for Cursor {
    async fn next(&mut self) -> Result<Option<MaterializedViewFacts>, SystemTableError> {
        self.gate.notified().await;
        Err(self.error.clone().into())
    }
}
async fn source(
    blocking: ConnectorBlockingIoSupervisor,
    capacity: QueryResultCapacityBinding,
) -> Arc<FrontendSystemTableFacts> {
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(10),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let scope = StatementFactScope::binding_only(
        Arc::new(QueryTableBindingStore::try_new().unwrap()),
        context,
        Some("default_catalog"),
        capacity.scope(),
        capacity.clone(),
    )
    .unwrap();
    source_in(blocking, scope).await
}
async fn source_in(
    blocking: ConnectorBlockingIoSupervisor,
    scope: StatementFactScope,
) -> Arc<FrontendSystemTableFacts> {
    source_in_registry(
        blocking,
        scope,
        Arc::new(
            novarocks_query_application::system_tables::SystemTableRegistry::try_new([]).unwrap(),
        ),
    )
    .await
}
async fn source_in_registry(
    blocking: ConnectorBlockingIoSupervisor,
    scope: StatementFactScope,
    registry: Arc<novarocks_query_application::system_tables::SystemTableRegistry>,
) -> Arc<FrontendSystemTableFacts> {
    let (product, _repository) = crate::mv::domain::test_admission::keyed_product().await;
    let readiness = Arc::new(crate::mv::domain::readiness::MvReadinessPort::from_product(
        product.keyed_readiness_service().unwrap(),
        tokio::runtime::Handle::current(),
        product.management_entrance().unwrap(),
    ));
    let owners = CompletionFactOwners::new(
        Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
        None,
        Arc::new(ConnectorControlHost::new()),
        Arc::new(UnifiedStatisticsResolver::default()),
        readiness.candidate_reader(),
        Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
        blocking,
        Arc::new(novarocks_query_application::view::QueryViewService::new()),
        registry,
        readiness,
    );
    FrontendSystemTableFacts::new(owners, scope).unwrap()
}
mod listing;
#[tokio::test]
async fn cancelled_inventory_borrow_and_cancelled_close_keep_the_same_cursor_until_actual_exit() {
    let (mut executors, supervisor) = crate::task_execution::blocking_io::tests::original_executors(
        tokio::runtime::Handle::current(),
    );
    let (_control, _root, capacity, _capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let source = source(supervisor.clone(), capacity).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    source.io.lock().unwrap().inventory = Some(Box::new(Cursor {
        gate: gate.clone(),
        dropped: dropped.clone(),
        error: ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "original held inventory error",
        ),
    }));
    let mut borrowed = Box::pin(source.next_inventory());
    assert!(futures::poll!(borrowed.as_mut()).is_pending());
    drop(borrowed);
    assert!(!dropped.load(Ordering::Acquire));
    let mut close = Box::pin(source.close());
    assert!(futures::poll!(close.as_mut()).is_pending());
    drop(close);
    assert!(!dropped.load(Ordering::Acquire));
    assert!(!source.io.lock().unwrap().joined);
    gate.notify_one();
    source.close().await.unwrap();
    assert!(source.io.lock().unwrap().joined);
    assert!(dropped.load(Ordering::Acquire));
    drop(source);
    drop(supervisor);
    executors.shutdown().await;
}
#[tokio::test]
async fn raw_provider_failure_and_cursor_remain_when_independent_health_is_unproven() {
    let (mut executors, supervisor) = crate::task_execution::blocking_io::tests::original_executors(
        tokio::runtime::Handle::current(),
    );
    let (_control, _root, capacity, _capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let source = source(supervisor.clone(), capacity).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let original = ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        "original raw inventory source",
    );
    source.io.lock().unwrap().inventory = Some(Box::new(Cursor {
        gate: gate.clone(),
        dropped: dropped.clone(),
        error: original.clone(),
    }));
    let mut borrowed = Box::pin(source.next_inventory());
    assert!(futures::poll!(borrowed.as_mut()).is_pending());
    drop(borrowed);
    let health = StatementNativeHealthFailure::actual_exit_unproven(
        "independent original health",
        Arc::new(std::io::Error::other("held health cause")),
    );
    source.io.lock().unwrap().health = Some(health.clone());
    let mut close = Box::pin(source.close());
    assert!(futures::poll!(close.as_mut()).is_pending());
    gate.notify_one();
    let received = close.await.unwrap_err();
    assert!(std::ptr::addr_eq(received.cause(), health.cause()));
    {
        let state = source.io.lock().unwrap();
        assert!(!state.joined);
        assert!(state.exited);
        assert!(
            matches!(state.response, Some(Response::Next(_, Err(SystemTableError::Provider(ref error)))) if *error == original)
        );
    }
    assert!(!dropped.load(Ordering::Acquire));
    drop(source);
    assert!(dropped.load(Ordering::Acquire));
    drop(supervisor);
    executors.shutdown().await;
}
