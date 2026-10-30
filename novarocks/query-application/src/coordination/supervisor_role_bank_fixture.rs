// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Included only inside the original supervisor unit-test module. This is an
// explicit neutral test policy, never a complete Native production profile.
use novarocks_resource_contract::SharedDependency;
use novarocks_workload_control::joint_admission::{
    JointAdmissionHandle, JointProgress, JointProgressDriver, ResponsibilityConfig,
};
use novarocks_workload_control::{ControlStorageConfig, PollRoleGeometry};

type TestRoleFactory<F> = fn(WorkOwner, LogicalRoleInput) -> F;
type TestRoleBank<F> =
    RolePollBank<LogicalRoleInput, F, TestRoleFactory<F>, Arc<OriginalLogicalExit>>;
struct TestGovernance<F: Future> {
    control: Option<WorkloadControl>,
    statement_source_slots: NonZeroUsize,
    // A metadata-only Option move transfers this SAME bank into the root.
    bank: std::sync::Mutex<Option<TestRoleBank<F>>>,
    admission: JointAdmissionHandle,
    progress: Option<TestProgressOwner>,
    finished: bool,
}
impl<F: Future> std::ops::Deref for TestGovernance<F> {
    type Target = WorkloadControl;
    fn deref(&self) -> &Self::Target {
        self.control.as_ref().expect("original test authority")
    }
}
impl<F: Future> TestGovernance<F> {
    async fn begin_root(&self) -> RootWork {
        self.begin_root_request(WorkRequest::new(WorkClass::Query))
            .await
    }
    async fn begin_root_request(&self, request: WorkRequest) -> RootWork {
        let root = self.try_begin_root(request).unwrap();
        let mut proposal = self.admission.begin_paid_proposal(&root.owner).unwrap();
        proposal
            .begin_first_bind(root.owner.control_claim_view().unwrap())
            .unwrap();
        // Every Native port used here is an in-process fake. A production
        // Network action must retain its actual Provider/network dependency.
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        let mut request = proposal.submit().unwrap();
        let batch = tokio::time::timeout(Duration::from_secs(5), &mut request)
            .await
            .expect("original test FirstBind did not complete")
            .unwrap();
        assert!(batch.new_claims.is_empty());
        batch
            .adjustment
            .unwrap()
            .install_control(&root.owner, &[])
            .unwrap();
        root
    }

    async fn finish_for_process_exit(&mut self) {
        // This test deliberately proved unresolved Native custody. Stop and
        // observe only the real fixture service; retain the actual unproven
        // ledger/backing to process exit without fabricating WC.drained.
        self.close_admission();
        self.progress.as_mut().unwrap().finish().await;
        std::mem::forget(self.progress.take());
        std::mem::forget(self.control.take());
        let bank = { self.bank.lock().unwrap().take() };
        std::mem::forget(bank);
        self.finished = true;
    }

    async fn finish(&mut self) {
        // Caller returns this owner from an inner test block: every ordinary
        // local View/Bytes/window/source alias has already actually dropped.
        let bank = { self.bank.lock().unwrap().take() };
        drop(bank);
        self.close_admission();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let revision = self.progress_revision();
            let progress = tokio::time::timeout_at(deadline, self.wait_progress(revision))
                .await
                .expect("original test authority did not actually drain");
            if matches!(
                progress,
                novarocks_workload_control::WorkloadProgress::Drained(_)
            ) {
                break;
            }
            assert!(
                !matches!(
                    progress,
                    novarocks_workload_control::WorkloadProgress::ControlReady(_)
                ),
                "test must acknowledge its real controls before fixture finish"
            );
        }
        self.progress.as_mut().unwrap().finish().await;
        drop(self.progress.take());
        let owner = self.control.take().unwrap();
        match owner.shutdown() {
            Ok(_) => {}
            Err(failure) => {
                let (error, owner) = failure.into_parts();
                self.control = Some(owner);
                panic!("original fixed test authority refused shutdown: {error}");
            }
        }
        self.finished = true;
    }
}
impl<F: Future> Drop for TestGovernance<F> {
    fn drop(&mut self) {
        if !self.finished {
            // Preserve original finite owners on a failed assertion. This is
            // NOT cleanup success, abort or a substitute for explicit finish.
            std::mem::forget(self.progress.take());
        }
    }
}

fn governance_empty() -> TestGovernance<impl Future<Output = Result<(), QueryExecutionError>> + Send>
{
    governance_with_client_compute(false)
}
fn governance_with_client_compute(
    client: bool,
) -> TestGovernance<impl Future<Output = Result<(), QueryExecutionError>> + Send> {
    governance_with_query_limit(client, None)
}
fn governance_with_query_limit(
    client: bool,
    query_limit: Option<usize>,
) -> TestGovernance<impl Future<Output = Result<(), QueryExecutionError>> + Send> {
    governance_with_result_positions(client, query_limit, None)
}
fn governance_with_result_positions(
    client: bool,
    query_limit: Option<usize>,
    internal_positions: Option<usize>,
) -> TestGovernance<impl Future<Output = Result<(), QueryExecutionError>> + Send> {
    // This function pointer names the SAME actual opaque async factory. Quote
    // its real M too (a pointer here, not the production zero-sized item).
    let factory: TestRoleFactory<_> = make_logical_role;
    let roles = [
        PollRoleGeometry::for_factory_with_context::<_, _, _, Arc<OriginalLogicalExit>>(&factory)
            .unwrap(),
    ];
    // The same original global scope inventory and actual role factory.
    // Work classes identify owners; they do not partition these records.
    let count = 20;
    let storage = ControlStorageConfig::new(count, &roles);
    let config = ResponsibilityConfig {
        work_per_turn: NonZeroUsize::new(7).unwrap(),
    };
    // Client cut tests deliberately have one real computation position.
    let query_concurrency_limit = query_limit.unwrap_or(if client { 1 } else { 8 });
    let mut control = WorkloadControl::try_new(
        WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        },
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 12,
            per_scope_bytes: (1 << 20) - (1 << 12),
        },
    )
    .unwrap();
    let mut result = novarocks_workload_control::ResultCapacityConfig::V1;
    if client {
        result.client_compute_positions = 1;
    }
    if let Some(positions) = internal_positions {
        result.positions[2] = positions;
    }
    control.configure_result_capacity(result).unwrap();
    let (admission, driver) = control.configure_responsibility(config).unwrap();
    control.configure_control_storage(storage).unwrap();
    let bank = RolePollBank::<_, _, _, Arc<OriginalLogicalExit>>::install_with_context(
        &mut control,
        0,
        factory,
    )
    .unwrap();
    control.mark_ready().unwrap();
    let progress = TestProgressOwner::start(driver, control.deadline_expiry_handle());
    TestGovernance {
        control: Some(control),
        statement_source_slots: NonZeroUsize::new(count).unwrap(),
        bank: std::sync::Mutex::new(Some(bank)),
        admission,
        progress: Some(progress),
        finished: false,
    }
}
async fn governance() -> (
    TestGovernance<impl Future<Output = Result<(), QueryExecutionError>> + Send>,
    RootWork,
) {
    let control = governance_empty();
    let root = control.begin_root().await;
    (control, root)
}
fn test_supervisor_with_clock<F>(
    control: &TestGovernance<F>,
    runtime: Handle,
    native: Arc<dyn LogicalExecutionNativePort>,
    namespace: QueryProcessNamespace,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    clock: Arc<dyn MonotonicClock>,
) -> (LogicalExecutionSupervisor, QueryExecutionClient)
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
{
    let bank = {
        control
            .bank
            .lock()
            .unwrap()
            .take()
            .expect("transfer original installed bank exactly once")
    };
    LogicalExecutionSupervisor::start_installed_bank(
        bank,
        NonZeroUsize::new(7).unwrap(),
        runtime,
        native,
        namespace,
        frontend_process_id,
        config,
        clock,
    )
    .unwrap()
}
fn test_supervisor<F>(
    control: &TestGovernance<F>,
    runtime: Handle,
    native: Arc<dyn LogicalExecutionNativePort>,
    namespace: QueryProcessNamespace,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
) -> (LogicalExecutionSupervisor, QueryExecutionClient)
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
{
    test_supervisor_with_clock(
        control,
        runtime,
        native,
        namespace,
        frontend_process_id,
        config,
        Arc::new(super::super::ProcessMonotonicClock::new()),
    )
}

struct TestDriverCustody(std::sync::Mutex<Option<Box<JointProgressDriver>>>);
struct TestDriverLease<'a>(&'a TestDriverCustody, Option<Box<JointProgressDriver>>);
impl Drop for TestDriverLease<'_> {
    fn drop(&mut self) {
        let previous = self.0.0.lock().unwrap().replace(self.1.take().unwrap());
        assert!(previous.is_none());
    }
}
impl TestDriverCustody {
    fn with<R>(&self, f: impl FnOnce(&mut JointProgressDriver) -> R) -> R {
        let driver = self.0.lock().unwrap().take().unwrap();
        let mut lease = TestDriverLease(self, Some(driver));
        f(lease.1.as_deref_mut().unwrap())
    }
}
struct TestProgressOwner {
    custody: Option<Arc<TestDriverCustody>>,
    stop: Option<watch::Sender<bool>>,
    join: Option<JoinHandle<()>>,
    failure: Option<tokio::task::JoinError>,
    joined: bool,
}
impl TestProgressOwner {
    fn start(
        driver: JointProgressDriver,
        deadlines: novarocks_workload_control::DeadlineExpiryHandle,
    ) -> Self {
        let custody = Arc::new(TestDriverCustody(std::sync::Mutex::new(Some(Box::new(
            driver,
        )))));
        let (stop, mut stopped) = watch::channel(false);
        let mut owner = Self {
            custody: Some(Arc::clone(&custody)),
            stop: Some(stop),
            join: None,
            failure: None,
            joined: false,
        };
        owner.join = Some(Handle::current().spawn(async move {
            let interval = Duration::from_millis(1);
            let mut recheck = tokio::time::Instant::now() + interval;
            let mut timer = Box::pin(tokio::time::sleep_until(recheck));
            loop {
                if *stopped.borrow() {
                    return;
                }
                deadlines.expire_deadlines();
                let now = tokio::time::Instant::now();
                if now >= recheck {
                    custody.with(JointProgressDriver::recheck_retired_backing);
                    recheck = now + interval;
                }
                match custody.with(JointProgressDriver::step) {
                    JointProgress::More => tokio::task::yield_now().await,
                    JointProgress::Idle(marker) => {
                        timer.as_mut().reset(
                            deadlines
                                .next_deadline()
                                .map_or(recheck, |d| d.min(recheck)),
                        );
                        tokio::select! {
                            _ = stopped.changed() => {},
                            _ = deadlines.wait_state_change(marker) => {},
                            _ = timer.as_mut() => {},
                        }
                    }
                }
            }
        }));
        owner
    }
    async fn finish(&mut self) {
        self.stop.as_ref().unwrap().send_replace(true);
        let result = self.join.as_mut().expect("same fixture driver join").await;
        if let Err(error) = result {
            self.failure = Some(error);
        }
        self.join.take();
        if self.failure.is_some() {
            panic!("original fixture progress task failed: {:?}", self.failure);
        }
        self.joined = true;
    }
}
impl Drop for TestProgressOwner {
    fn drop(&mut self) {
        if !self.joined {
            std::mem::forget(self.join.take());
            std::mem::forget(self.custody.take());
            std::mem::forget(self.failure.take());
            std::mem::forget(self.stop.take());
        }
    }
}

// A real producer can be paused before its original root starts polling. The
// test still consumes the production driver and actual factory, not a mailbox.
fn unpolled_test_driver<F>(
    control: &TestGovernance<F>,
    native: Arc<dyn LogicalExecutionNativePort>,
) -> (
    TestRoleBank<F>,
    watch::Sender<bool>,
    LogicalExecutionRuntimeRegistry,
    QueryExecutionClient,
)
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
{
    let bank = { control.bank.lock().unwrap().take().unwrap() };
    let ingress = bank.ingress().unwrap();
    let registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let (shutdown, receiver) = watch::channel(false);
    let driver = QueryExecutionDriverHandle {
        ingress,
        shutdown: receiver,
        query_ids: Arc::new(ProcessQueryIdAllocator::new(QueryProcessNamespace::new(
            0x50,
        ))),
        native,
        frontend_process_id: FrontendProcessId::new_v7(),
        config: supervisor_config(),
        clock: Arc::new(super::super::ProcessMonotonicClock::new()),
        registry: registry.handle(),
    };
    (bank, shutdown, registry, QueryExecutionClient::new(driver))
}
