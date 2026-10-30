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

//! Pure receiver custody tests; physical queue/exit is the enclosing owner's test.
use super::*;
use crate::StateStoreProviderRegistration;
use novarocks_state_store_api::{
    InstanceScope, StateStoreBlockingOriginal, StateStoreBlockingSlot, StateStoreExecutionPort,
    StateStoreLimits,
};
use novarocks_state_store_testkit::testing::InMemoryStateStore;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

const ID: StateStoreProviderId = StateStoreProviderId::new("prepared-host-test");
const DESCRIPTOR: StateStoreProviderDescriptor =
    StateStoreProviderDescriptor::new(ID, novarocks_state_store_api::MAX_KEY_BYTES);
fn vocabulary() -> StateStoreBlockingSourceVocabulary {
    StateStoreBlockingSourceVocabulary::for_provider::<
        NoPhysicalSource,
        NoPhysicalSource,
        NoPhysicalSource,
    >(ID)
}
// A vocabulary witness only. These receiver tests execute no physical phase.
struct NoPhysicalSource;
impl StateStoreBlockingOriginal for NoPhysicalSource {
    fn origin(&self) -> novarocks_state_store_api::StateStoreBlockingOrigin {
        unreachable!("pure receiver has no physical origin")
    }
    fn requested(&self) -> Option<novarocks_state_store_api::StateStoreBlockingRequest> {
        unreachable!("pure receiver has no physical request")
    }
    fn publication_refusal(
        &self,
        _: novarocks_state_store_api::StateStoreBlockingRequest,
    ) -> Option<StateStoreExecutionFailure> {
        unreachable!("pure receiver has no physical publication")
    }
    fn run_original_phase(&self, _: novarocks_state_store_api::StateStoreBlockingRequest) {
        unreachable!("pure receiver has no physical work")
    }
    fn before_publish(
        &self,
        _: novarocks_state_store_api::StateStoreBlockingRequest,
    ) -> Result<(), StateStoreExecutionFailure> {
        unreachable!("pure receiver has no physical publication")
    }
    fn receive_result(
        &self,
        _: novarocks_state_store_api::StateStoreBlockingRequest,
        _: Result<(), StateStoreExecutionFailure>,
    ) {
        unreachable!("pure receiver has no physical result")
    }
    fn receive_exit(
        &self,
        _: novarocks_state_store_api::StateStoreBlockingRequest,
        _: Result<novarocks_state_store_api::StateStoreBlockingExit, StateStoreExecutionFailure>,
    ) {
        unreachable!("pure receiver has no physical footer")
    }
    fn is_complete(&self) -> bool {
        unreachable!("pure receiver asserts no physical exit")
    }
    fn local_failure(&self) -> Option<StateStoreExecutionFailure> {
        unreachable!("pure receiver has no physical health")
    }
    fn request_close(&self) {
        unreachable!("pure receiver has no physical close")
    }
    fn request_interrupt(&self) {
        unreachable!("pure receiver has no physical interrupt")
    }
}
fn input() -> StateStoreHostInput {
    StateStoreHostInput {
        cluster_id: "prepared-host".into(),
        provider_id: ID,
        limits: StateStoreLimits::default(),
        run_policy: StateStoreRunPolicy::default(),
    }
}
struct Instance {
    dependencies: Option<Vec<novarocks_state_store_api::SharedDependency>>,
    descriptor: StateStoreProviderDescriptor,
    scope: InstanceScope,
    store: Arc<dyn StateStore>,
    drops: Arc<AtomicUsize>,
}
impl Drop for Instance {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl StateStoreProviderInstance for Instance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }
    fn blocking_dependency_facts(
        &self,
    ) -> Option<Vec<novarocks_state_store_api::SharedDependency>> {
        self.dependencies.clone()
    }
    fn blocking_source_vocabulary(&self) -> Option<StateStoreBlockingSourceVocabulary> {
        Some(vocabulary())
    }
    fn blocking_instance_scope(&self) -> Option<InstanceScope> {
        Some(self.scope)
    }
    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        StateStoreProviderLifecycle::Ready
    }
    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        Some(self.store.clone())
    }
    async fn shutdown(&mut self, _: Instant) -> Result<(), StateStoreError> {
        panic!("prepared opening must not call legacy shutdown")
    }
}
struct Original {
    instance: Option<Box<dyn StateStoreProviderInstance>>,
}
impl StateStoreOriginalOpen for Original {
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), StateStoreExecutionFailure>> {
        Poll::Ready(Ok(()))
    }
    fn take_instance_in(
        &mut self,
        target: &mut Option<Box<dyn StateStoreProviderInstance>>,
    ) -> Result<(), StateStoreExecutionFailure> {
        assert!(target.is_none());
        *target = self.instance.take();
        Ok(())
    }
}
struct Factory {
    dependencies: Option<Vec<novarocks_state_store_api::SharedDependency>>,
    original: Mutex<Option<Original>>,
}
#[async_trait::async_trait]
impl StateStoreProviderFactory for Factory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &DESCRIPTOR
    }
    fn blocking_dependency_facts(
        &self,
    ) -> Option<Vec<novarocks_state_store_api::SharedDependency>> {
        self.dependencies.clone()
    }
    fn blocking_source_vocabulary(&self) -> Option<StateStoreBlockingSourceVocabulary> {
        Some(vocabulary())
    }
    fn prepare_open_in(
        self: Box<Self>,
        _: StateStoreOpenRequest,
        _: &StateStoreExecutionContext,
        target: &mut Option<Box<dyn StateStoreOriginalOpen>>,
    ) -> Result<(), StateStoreExecutionFailure> {
        assert!(target.is_none());
        *target = self
            .original
            .into_inner()
            .unwrap()
            .map(|v| Box::new(v) as _);
        Ok(())
    }
    async fn open(
        self: Box<Self>,
        _: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        panic!("no legacy opening fallback")
    }
}
struct Port;
impl StateStoreExecutionPort for Port {
    fn bind_original_in(
        &self,
        _: &Arc<dyn StateStoreBlockingOriginal>,
        _: Instant,
        _: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), StateStoreExecutionFailure> {
        panic!("pure fixture has no physical work")
    }
    fn request_progress(&self) {}
}
fn prepared(mismatch: bool, drops: Arc<AtomicUsize>) -> PreparedStateStoreHost {
    prepared_with_facts(mismatch, drops, Some(Vec::new()), Some(Vec::new())).unwrap()
}
fn prepared_with_facts(
    mismatch: bool,
    drops: Arc<AtomicUsize>,
    factory_facts: Option<Vec<novarocks_state_store_api::SharedDependency>>,
    instance_facts: Option<Vec<novarocks_state_store_api::SharedDependency>>,
) -> Result<PreparedStateStoreHost, StateStoreHostError> {
    let instance = Box::new(Instance {
        dependencies: instance_facts,
        descriptor: if mismatch {
            StateStoreProviderDescriptor::new(
                StateStoreProviderId::new("wrong-instance"),
                DESCRIPTOR.max_key_bytes,
            )
        } else {
            DESCRIPTOR
        },
        scope: InstanceScope::new(),
        store: Arc::new(InMemoryStateStore::new("prepared-host")),
        drops,
    });
    let original = Arc::new(Mutex::new(Some(Original {
        instance: Some(instance),
    })));
    let mut registry = StateStoreProviderRegistry::new();
    registry
        .register(StateStoreProviderRegistration::new(DESCRIPTOR, move |_| {
            Ok(Box::new(Factory {
                dependencies: factory_facts.clone(),
                original: Mutex::new(original.lock().unwrap().take()),
            }))
        }))
        .unwrap();
    StateStoreHost::prepare_original_open(
        &registry,
        input(),
        Instant::now() + Duration::from_secs(1),
    )
}
fn start(value: &mut PreparedStateStoreHost) -> Arc<dyn StateStoreExecutionPort> {
    let port: Arc<dyn StateStoreExecutionPort> = Arc::new(Port);
    value
        .start_in(&StateStoreExecutionContext::new(&port, value.deadline()))
        .unwrap();
    port
}
fn ready(value: &mut PreparedStateStoreHost) {
    let waker = std::task::Waker::noop();
    assert!(matches!(
        value.poll_ready(&mut Context::from_waker(waker)),
        Poll::Ready(Ok(()))
    ));
}
struct NoOriginalFactory;
#[async_trait::async_trait]
impl StateStoreProviderFactory for NoOriginalFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &DESCRIPTOR
    }
    async fn open(
        self: Box<Self>,
        _: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        panic!("missing Original capability must not call legacy open")
    }
}
#[test]
fn absent_original_capability_refuses_without_legacy_open() {
    let mut registry = StateStoreProviderRegistry::new();
    registry
        .register(StateStoreProviderRegistration::new(DESCRIPTOR, |_| {
            Ok(Box::new(NoOriginalFactory))
        }))
        .unwrap();
    let error = StateStoreHost::prepare_original_open(
        &registry,
        input(),
        Instant::now() + Duration::from_secs(1),
    )
    .err()
    .unwrap();
    assert!(
        error
            .original_failure()
            .unwrap()
            .downcast_ref::<StateStoreOriginalUnsupported>()
            .is_some()
    );
}
#[test]
fn handoff_requires_ready_and_preserves_occupied_receiver() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut value = prepared(false, drops.clone());
    let _port = start(&mut value);
    let mut target = None;
    assert!(value.install_ready_in(&mut target).is_err());
    assert!(!value.has_retained_instance());
    ready(&mut value);
    value.install_ready_in(&mut target).unwrap();
    assert_eq!(
        target
            .as_ref()
            .unwrap()
            .original_blocking_sources()
            .unwrap()
            .0,
        vocabulary()
    );
    assert_eq!(value.original_blocking_dependency_facts(), &[]);
    assert_eq!(
        target
            .as_ref()
            .unwrap()
            .original_blocking_dependency_facts(),
        Some([].as_slice())
    );
    assert!(value.install_ready_in(&mut target).is_err());
    value.request_close();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(target);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn validation_failure_retains_instance_and_same_cause_through_close_request() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut value = prepared(true, drops.clone());
    let _port = start(&mut value);
    ready(&mut value);
    let mut target = None;
    let error = value.install_ready_in(&mut target).unwrap_err();
    assert_eq!(error.kind(), StateStoreHostErrorKind::DescriptorMismatch);
    assert!(target.is_none());
    assert!(value.has_retained_instance());
    assert_eq!(error.original_failure(), value.original_failure());
    value.request_close();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        value
            .install_ready_in(&mut target)
            .unwrap_err()
            .original_failure(),
        error.original_failure()
    );
    drop(value);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn original_vocabulary_without_selected_facts_refuses_before_start() {
    let drops = Arc::new(AtomicUsize::new(0));
    let error = prepared_with_facts(false, drops.clone(), None, Some(Vec::new()))
        .err()
        .expect("missing selected facts are a contract error");
    assert_eq!(error.kind(), StateStoreHostErrorKind::DescriptorMismatch);
    assert!(error.original_failure().is_some());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "unstarted pure input has no physical tail"
    );
}
#[test]
fn changed_instance_facts_preserve_received_instance_and_original_cause() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut value = prepared_with_facts(false, drops.clone(), Some(Vec::new()), None).unwrap();
    let _port = start(&mut value);
    ready(&mut value);
    let mut target = None;
    let first = value.install_ready_in(&mut target).unwrap_err();
    assert_eq!(first.kind(), StateStoreHostErrorKind::DescriptorMismatch);
    assert!(target.is_none());
    assert!(value.has_retained_instance());
    value.request_close();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        value
            .install_ready_in(&mut target)
            .unwrap_err()
            .original_failure(),
        first.original_failure()
    );
    drop(value);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
