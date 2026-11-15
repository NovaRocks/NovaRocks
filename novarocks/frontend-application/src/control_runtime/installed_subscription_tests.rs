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

//! The production attempt assembler with its original Sources and no poller pool.

use super::*;
use crate::{
    native::{
        fragment_encoder::frozen::tests::frozen_package,
        task_result_transport::{NativeTaskResultTransport, TaskReadGrace},
        task_transport::{NativeTaskOperationSink, tests::Loopback},
    },
    query_execution::{
        artifact::{
            NativePlacementAssignment, TaskManifestBinding, ValidatedNativeSubmission,
            task_key_for_contract_test,
        },
        lifecycle_plan::QueryCredentialLeases,
        native_execution_adapter::{LogicalAbortRouter, source_cohort},
        schedule::{SchedulingPlan, TaskPlacement},
    },
    task_execution::{
        clock::ManualClock,
        host::{
            install::InstallAttemptHost,
            transport::{FrozenSubscriptionStageInputs, stage_subscription_endpoints},
        },
        lease::RequestedLeaseDurations,
        manifest_projection::{AttemptPlanPolicy, ManifestAttemptInputs},
        sources::{AttemptEstablishFacts, SubmissionTaskPlans},
    },
};
use novarocks_attempt_protocol::{config::CoordinationBudgets, ids::IdSeed};
use novarocks_coordination_contract::ExecutionEffect;
use novarocks_execution_contract::{BackendProcessDescriptor, RuntimeEndpoint};
use novarocks_physical_plan::PipelineDopDomain;
use novarocks_proto_models::{
    catalog::CatalogSet,
    novarocks::{QueryOptions, RuntimeFilterContribution},
};
use novarocks_query_application::{
    api::{BackendProcessObservation, BackendTopologyError},
    coordination::LogicalExecutionActorConfig,
    test_support::LogicalExecutionTestHarness,
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use novarocks_workload_control::{
    ControlStorageConfig, PollRoleSourceBacking, ResultCapacityConfig, ResultWindowClass, Stage,
    WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    joint_admission::{JointProgressDriver, ResponsibilityConfig},
};
use std::{
    alloc::Layout,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

struct CurrentPeer(BackendProcessDescriptor);
impl BackendProcessObservationPort for CurrentPeer {
    fn observe_process_at_endpoint(
        &self,
        process: BackendProcessId,
        endpoint: &RuntimeEndpoint,
    ) -> Result<BackendProcessObservation, BackendTopologyError> {
        assert_eq!(process, self.0.process_id());
        assert_eq!(endpoint, self.0.endpoint());
        Ok(BackendProcessObservation::Current)
    }
}

// This is the normal production process owner without its separately-owned
// poller and membership service. The test gives the same queues finite turns.
fn manual_process(
    data: &FrontendDataRuntime,
    clock: Arc<dyn MonotonicClock>,
    peer: BackendProcessDescriptor,
    history: Option<novarocks_control_runtime::history::HistoryCapture>,
) -> FrontendAttemptRuntime {
    let layout = data.transport_capacity().clone();
    let clock: Arc<dyn Clock> = Arc::new(FrontendClock(clock));
    let observation = RuntimeObservation::new(Some(clock.clone()));
    let limit = NonZeroUsize::new(8).unwrap();
    let budget = ObservationBudget::for_task_attempt();
    let limits = layout
        .process_limits()
        .checked_add(&budget.byte_windows(limit).unwrap())
        .unwrap()
        .checked_add(&ObservationBackingQuote::process_limit(budget, limit).unwrap())
        .unwrap();
    let ledger = RuntimeLedger::with_keyed_families_and_observation(
        limits,
        layout.keyed_families(),
        [],
        history.clone(),
        observation.clone(),
    )
    .unwrap();
    let runtime = ActorRuntime::with_observation(history, observation.clone());
    let host_observation = Arc::new(FrontendHostObservations::new(clock.clone()));
    let host = Arc::new(HostRegistry::new(
        runtime.clone(),
        ledger.clone(),
        host_observation.clone(),
    ));
    FrontendAttemptRuntime {
        registration_open: Mutex::new(true),
        runtime,
        ledger,
        layout,
        host,
        members: Arc::new(MembershipRegistry::new()),
        backend_processes: Arc::new(CurrentPeer(peer)),
        clock,
        observation,
        host_observation,
        unresolved_replacements: Mutex::new(BTreeMap::new()),
    }
}

// The original finite queue turns are driven even while the external native
// future is pending. No Core input, source callback or exit fact is synthesized.
async fn drive_until<F: Future>(
    process: &FrontendAttemptRuntime,
    joint: &mut JointProgressDriver,
    future: F,
    until: Instant,
) -> Option<F::Output> {
    let mut future = Box::pin(future);
    loop {
        source_cohort::tests::drive(joint);
        let now = process.clock.now();
        process.runtime.advance_time(now, 64).unwrap();
        for _ in 0..64 {
            let core = process
                .runtime
                .poll_one_with_clock(process.clock.as_ref())
                .unwrap();
            let host_claimed = if let Some(turn) = process.runtime.claim_host_actor() {
                process.host.drive(process.clock.now(), turn);
                true
            } else {
                false
            };
            if !core && !host_claimed {
                break;
            }
        }
        if let Poll::Ready(value) = futures::poll!(future.as_mut()) {
            return Some(value);
        }
        if Instant::now() >= until {
            return None;
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn installed_unstarted_single_participation_closes_all_original_sources() {
    let wait = Duration::from_secs(5);
    let handle = tokio::runtime::Handle::current();
    let mut data = FrontendDataRuntime::new(handle.clone());
    let mut peer = Loopback::start().await;
    let backend = BackendProcessId::new_v7();
    let descriptor = peer.bounded_root_descriptor(backend);
    let backends: Arc<[BackendProcessDescriptor]> = Arc::from([descriptor.clone()]);
    let execution =
        QueryExecutionId::new(QueryId::new(711, 19), AttemptId::new(1).unwrap()).unwrap();
    let frontend = FrontendProcessId::new_v7();
    let fragment_id = 1;
    let task_key = task_key_for_contract_test(execution.query_id(), fragment_id, 0);
    let fragment = frozen_package(
        fragment_id,
        100,
        [9; 16],
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        None,
        0,
    );
    let schedule = SchedulingPlan {
        root_fragment_id: fragment_id,
        by_fragment: [(
            fragment_id,
            vec![TaskPlacement {
                fragment_id,
                instance_index: 0,
                task_key,
                backend_idx: 0,
                endpoint: descriptor.endpoint().clone(),
                scan_ranges: BTreeMap::new(),
            }],
        )]
        .into(),
        root_task_key: task_key,
        root_backend_idx: 0,
    };
    let manifest = TaskManifestBinding::protocol_fixture_with_topology_for_test(
        execution,
        frontend,
        &schedule,
        &[],
        &[(0, backend)].into(),
        [(fragment_id, fragment.clone())].into(),
        &[(0, descriptor.clone())].into(),
    )
    .unwrap();
    let participations = manifest
        .participations()
        .iter()
        .map(|member| member.participation())
        .collect::<Vec<_>>();
    let submission = ValidatedNativeSubmission::new(
        0,
        task_key,
        execution,
        fragment,
        NativePlacementAssignment::new(0, NonZeroUsize::MIN, BTreeMap::new()),
    );
    let mut plans = SubmissionTaskPlans::index_manifest(vec![submission], &manifest).unwrap();
    let mut prepared = ManifestAttemptInputs::freeze(
        &manifest,
        &mut plans,
        descriptor.native_compatibility_id(),
        TransportBudget::DEFAULT,
    )
    .unwrap()
    .prepare_publications()
    .unwrap();
    let statuses = prepared.publications.root_status.take().unwrap();
    let protocol_facts = prepared.publications.protocol_facts.take().unwrap();

    // One original root pays QA, the result window, all Native actions and
    // the actual subscription child. No second root or quota wallet is used.
    let count = 8;
    let send_geometry =
        NativeTaskOperationSink::proposal_wait_source_geometry(TransportBudget::DEFAULT).unwrap();
    let send_backing = Layout::array::<u8>(send_geometry.resident_bytes().unwrap()).unwrap();
    let subscription_backing = Layout::from_size_align(
        crate::native::NativeSubscriptionOwner::resident_storage_bytes(count).unwrap(),
        8,
    )
    .unwrap();
    let storage = ControlStorageConfig::new(count, &[]).with_process_source_backing(
        PollRoleSourceBacking::from_layouts(&[subscription_backing], &[send_backing]).unwrap(),
    );
    let workload = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_, mut joint) = workload
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    workload.configure_control_storage(storage).unwrap();
    let capacity = workload
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    workload.mark_ready().unwrap();
    let original = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = original.owner.scope();
    let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
    let mut binding = scope.begin_control_binding().unwrap();
    for endpoint in [descriptor.endpoint(), descriptor.control_endpoint()] {
        binding
            .push_selected_network(
                transport
                    .selected_network_origin(endpoint.native_endpoint())
                    .unwrap(),
            )
            .unwrap();
    }
    binding.end_change().unwrap();
    let mut admission = binding.submit().unwrap();
    source_cohort::tests::drive(&mut joint);
    let Poll::Ready(Ok(mut granted)) =
        Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("the original selected native binding must grant");
    };
    granted
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(&scope)
        .unwrap()
        .notify();
    drop((granted, admission));
    source_cohort::tests::drive(&mut joint);
    let window = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let authority = window.retain_alias().native_action_authority().unwrap();
    let cohort = source_cohort::tests::cohort(&scope, 1, &mut joint);
    let stage = scope.try_acquire(Stage::Execution).unwrap();
    let business = original.business;
    let router = LogicalAbortRouter::new();
    let config = LogicalExecutionActorConfig::single_attempt_completion(
        execution,
        ExecutionEffect::None,
        NonZeroUsize::new(8).unwrap(),
        participations,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        original.owner,
        stage,
    )
    .unwrap()
    .with_abort_participation_effect_port(router.clone(), NonZeroUsize::MIN);
    let mut logical = LogicalExecutionTestHarness::install(
        handle,
        config,
        execution,
        manifest
            .participations()
            .iter()
            .map(|member| member.participation())
            .collect(),
    )
    .unwrap();
    logical.activate_initial().await.unwrap();
    let drive = logical.native_attempt_drive();
    let route = Arc::new(router.reserve_route(execution).unwrap());
    let clock = Arc::new(ManualClock::new());
    let process = manual_process(&data, clock.clone(), descriptor.clone(), None);
    let staged = stage_subscription_endpoints(
        &[crate::control_runtime::membership::FrozenMember {
            participation: manifest.participations()[0].participation(),
            endpoint: descriptor.endpoint().clone(),
        }],
        FrozenSubscriptionStageInputs {
            backends: &backends,
            compatibility: descriptor.native_compatibility_id(),
            data: &data,
            scope: &scope,
            cohort: &cohort,
            execution,
        },
    )
    .unwrap();
    let establish = AttemptEstablishFacts::freeze(
        CatalogSet::default(),
        [(backend, RuntimeFilterContribution::default())],
        QueryOptions::default(),
        &QueryCredentialLeases::empty(),
    )
    .unwrap();
    let lease = RequestedLeaseDurations::DEFAULT;
    let mut split = None;
    let installed = InstallAttemptHost {
        process: &process,
        data: &data,
        clock,
        drive: &drive,
        prepared,
        establish: &establish,
        replacements: None,
        subscriptions: staged,
        authority,
        route: route.clone(),
        results: Arc::new(
            NativeTaskResultTransport::new(
                backends,
                data.clone(),
                TaskReadGrace::new(Duration::from_secs(1)),
            )
            .unwrap(),
        ),
        profile: None,
        dynamic: None,
        split: &mut split,
        policy: AttemptPlanPolicy {
            coordination: CoordinationBudgets::DEFAULT,
            retry: Default::default(),
            initial_lease: lease.initial(),
            steady_lease: lease.steady(),
        },
        ids: IdSeed::new(1, 100),
        retain_diagnostics: false,
        observe_dev_root: false,
        observe_lifecycle: false,
        #[cfg(novarocks_dev_controls)]
        root_observer: None,
    }
    .install()
    .unwrap();

    // Close the exact registered owner BEFORE its first Host/Core turn.
    // This first slice proves only unused/staged production-source cleanup.
    // No Open, Task execution, P convergence or live RPC exit is claimed.
    process.host.close_registration();
    // Retain the original execution observer through the real Host close.
    // No asynchronous bridge run or convergence was started in this slice.
    drop((statuses, protocol_facts, cohort, window));
    let host_exit = drive_until(
        &process,
        &mut joint,
        async {
            while !process.host.is_empty() {
                tokio::task::yield_now().await;
            }
        },
        Instant::now() + wait,
    )
    .await;
    let no_native_dispatch =
        peer.subscription_requests().is_empty() && peer.applied_requests().is_empty();
    drop((installed, drive, route, router, manifest, capacity));
    let qa_exit = logical.finish_until(Instant::now() + wait).await;
    business.release();
    source_cohort::tests::drive(&mut joint);
    let scope_released = tokio::time::timeout(wait, scope.wait_released()).await;
    drop(scope);
    workload.close_admission();
    source_cohort::tests::drive(&mut joint);
    let workload_exit = workload.shutdown();
    let subscriptions_exit = data
        .drain_original_subscriptions_until(Instant::now() + wait)
        .await;
    let apply_exit = data.drain_original_apply_until(Instant::now() + wait).await;
    let outgoing_exit = data
        .drain_native_outgoing_until(Instant::now() + wait)
        .await;
    let peer_exit = peer.settle().await;
    let clean = process.host.is_empty()
        && process.runtime.is_empty()
        && process.ledger.is_empty()
        && process.members.is_empty();

    // Assertions follow actual host/Source/QA/Workload/channel/peer cleanup.
    assert!(clean);
    assert!(qa_exit.is_ok() && scope_released.is_ok() && workload_exit.is_ok());
    assert!(subscriptions_exit.is_ok() && apply_exit.is_ok() && outgoing_exit.is_ok() && peer_exit);
    assert!(host_exit.is_some() && no_native_dispatch);
}

#[tokio::test]
async fn installed_live_single_root_subscription_normally_converges_and_really_closes() {
    run_installed_live(false).await;
}

#[tokio::test]
async fn installed_reopen_retires_original_final_phase_before_reusing_paid_source() {
    run_installed_live(true).await;
}

async fn run_installed_live(controlled_reopen: bool) {
    let wait = Duration::from_secs(5);
    let handle = tokio::runtime::Handle::current();
    let mut data = FrontendDataRuntime::new(handle.clone());
    let mut peer = Loopback::start().await;
    let backend = BackendProcessId::new_v7();
    let descriptor = peer.bounded_root_descriptor(backend);
    let backends: Arc<[BackendProcessDescriptor]> = Arc::from([descriptor.clone()]);
    let execution =
        QueryExecutionId::new(QueryId::new(712, 19), AttemptId::new(1).unwrap()).unwrap();
    let frontend = FrontendProcessId::new_v7();
    let fragment_id = 1;
    let task_key = task_key_for_contract_test(execution.query_id(), fragment_id, 0);
    let fragment = frozen_package(
        fragment_id,
        100,
        [9; 16],
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        None,
        0,
    );
    let schedule = SchedulingPlan {
        root_fragment_id: fragment_id,
        by_fragment: [(
            fragment_id,
            vec![TaskPlacement {
                fragment_id,
                instance_index: 0,
                task_key,
                backend_idx: 0,
                endpoint: descriptor.endpoint().clone(),
                scan_ranges: BTreeMap::new(),
            }],
        )]
        .into(),
        root_task_key: task_key,
        root_backend_idx: 0,
    };
    let manifest = TaskManifestBinding::protocol_fixture_with_topology_for_test(
        execution,
        frontend,
        &schedule,
        &[],
        &[(0, backend)].into(),
        [(fragment_id, fragment.clone())].into(),
        &[(0, descriptor.clone())].into(),
    )
    .unwrap();
    // The fixture purpose is completion without row payloads. This explicit
    // declaration is not inferred from the package's Binary carrier.
    let kind = novarocks_result_contract::RootOutputKind::CountOnly;
    let root_task = manifest.root();
    if controlled_reopen {
        peer.enable_single_root_first_subscription_eof_before_create(root_task, kind);
    } else {
        peer.enable_single_root_lifecycle(root_task, kind);
    }
    let participations = manifest
        .participations()
        .iter()
        .map(|member| member.participation())
        .collect::<Vec<_>>();
    let submission = ValidatedNativeSubmission::new(
        0,
        task_key,
        execution,
        fragment,
        NativePlacementAssignment::new(0, NonZeroUsize::MIN, BTreeMap::new()),
    );
    let mut plans = SubmissionTaskPlans::index_manifest(vec![submission], &manifest).unwrap();
    let mut prepared = ManifestAttemptInputs::freeze(
        &manifest,
        &mut plans,
        descriptor.native_compatibility_id(),
        TransportBudget::DEFAULT,
    )
    .unwrap()
    .prepare_publications()
    .unwrap();
    let statuses = prepared.publications.root_status.take().unwrap();
    let protocol_facts = prepared.publications.protocol_facts.take().unwrap();

    // One original root pays QA, the result window, all Native actions and
    // the actual subscription child. No second root or quota wallet is used.
    let count = 8;
    let send_geometry =
        NativeTaskOperationSink::proposal_wait_source_geometry(TransportBudget::DEFAULT).unwrap();
    let send_backing = Layout::array::<u8>(send_geometry.resident_bytes().unwrap()).unwrap();
    let subscription_backing = Layout::from_size_align(
        crate::native::NativeSubscriptionOwner::resident_storage_bytes(count).unwrap(),
        8,
    )
    .unwrap();
    let storage = ControlStorageConfig::new(count, &[]).with_process_source_backing(
        PollRoleSourceBacking::from_layouts(&[subscription_backing], &[send_backing]).unwrap(),
    );
    let workload = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_, mut joint) = workload
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    workload.configure_control_storage(storage).unwrap();
    let capacity = workload
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    workload.mark_ready().unwrap();
    let original = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = original.owner.scope();
    let cancellation = scope.cancellation().unwrap();
    let requester = original.owner.cancellation_requester();
    let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
    let mut binding = scope.begin_control_binding().unwrap();
    for endpoint in [descriptor.endpoint(), descriptor.control_endpoint()] {
        binding
            .push_selected_network(
                transport
                    .selected_network_origin(endpoint.native_endpoint())
                    .unwrap(),
            )
            .unwrap();
    }
    binding.end_change().unwrap();
    let mut admission = binding.submit().unwrap();
    source_cohort::tests::drive(&mut joint);
    let Poll::Ready(Ok(mut granted)) =
        Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("the original selected native binding must grant");
    };
    granted
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(&scope)
        .unwrap()
        .notify();
    drop((granted, admission));
    source_cohort::tests::drive(&mut joint);
    let window = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let authority = window.retain_alias().native_action_authority().unwrap();
    let cohort = source_cohort::tests::cohort(&scope, 1, &mut joint);
    let stage = scope.try_acquire(Stage::Execution).unwrap();
    let business = original.business;
    let router = LogicalAbortRouter::new();
    let expected_participations = participations.clone();
    let config = LogicalExecutionActorConfig::single_attempt_completion(
        execution,
        ExecutionEffect::None,
        NonZeroUsize::new(8).unwrap(),
        participations,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        original.owner,
        stage,
    )
    .unwrap()
    .with_abort_participation_effect_port(router.clone(), NonZeroUsize::MIN);
    let mut logical = LogicalExecutionTestHarness::install(
        handle,
        config,
        execution,
        manifest
            .participations()
            .iter()
            .map(|member| member.participation())
            .collect(),
    )
    .unwrap();
    logical.activate_initial().await.unwrap();
    let drive = logical.native_attempt_drive();
    let route = Arc::new(router.reserve_route(execution).unwrap());
    let clock = Arc::new(ManualClock::new());
    let history = novarocks_control_runtime::history::HistoryCapture::new(32_768);
    let process = manual_process(
        &data,
        clock.clone(),
        descriptor.clone(),
        controlled_reopen.then(|| history.clone()),
    );
    let staged = stage_subscription_endpoints(
        &[crate::control_runtime::membership::FrozenMember {
            participation: manifest.participations()[0].participation(),
            endpoint: descriptor.endpoint().clone(),
        }],
        FrozenSubscriptionStageInputs {
            backends: &backends,
            compatibility: descriptor.native_compatibility_id(),
            data: &data,
            scope: &scope,
            cohort: &cohort,
            execution,
        },
    )
    .unwrap();
    let establish = AttemptEstablishFacts::freeze(
        CatalogSet::default(),
        [(backend, RuntimeFilterContribution::default())],
        QueryOptions::default(),
        &QueryCredentialLeases::empty(),
    )
    .unwrap();
    let lease = RequestedLeaseDurations::DEFAULT;
    let mut split = None;
    let results = Arc::new(
        NativeTaskResultTransport::new(
            backends.clone(),
            data.clone(),
            TaskReadGrace::new(Duration::from_secs(1)),
        )
        .unwrap(),
    );
    let root_port =
        crate::native::task_result_transport::NativeBoundedRootReadPort::new(results.clone());
    let mut installed = InstallAttemptHost {
        process: &process,
        data: &data,
        clock: clock.clone(),
        drive: &drive,
        prepared,
        establish: &establish,
        replacements: None,
        subscriptions: staged,
        authority,
        route: route.clone(),
        results: results.clone(),
        profile: None,
        dynamic: None,
        split: &mut split,
        policy: AttemptPlanPolicy {
            coordination: CoordinationBudgets::DEFAULT,
            retry: Default::default(),
            initial_lease: lease.initial(),
            steady_lease: lease.steady(),
        },
        ids: IdSeed::new(1, 100),
        retain_diagnostics: false,
        observe_dev_root: false,
        observe_lifecycle: false,
        #[cfg(novarocks_dev_controls)]
        root_observer: None,
    }
    .install()
    .unwrap();

    // Drive the original Core until it creates the task and opens its real
    // loopback subscription. The consumer below obtains exact empty EOS and
    // ACK through the native codec and original retained result window before
    // requesting the serialized owner's success decision.
    let (actor_id, subscription_source) = installed.fixture_sources;
    let (allow_consumer, consumer_allowed) = tokio::sync::oneshot::channel::<()>();
    let mut allow_consumer = Some(allow_consumer);
    if !controlled_reopen {
        let _ = allow_consumer.take().unwrap().send(());
    }
    let mut normal_future = Box::pin(async {
        let consume_and_seal = async {
            consumer_allowed
                .await
                .map_err(|_| "fixture consumer gate withdrawn".to_owned())?;
            use novarocks_execution_contract::root_result::RootResultRead;
            use novarocks_query_application::api::BoundedRootReadPort;
            use novarocks_query_application::coordination::{RootRelayFrontier, RootRelayStep};
            use novarocks_result_contract::RootProfileId;

            let mut frontier = RootRelayFrontier::new(root_task, RootProfileId::V1, kind, None)
                .map_err(|error| error.to_string())?;
            while let Some(read) = frontier.next_read() {
                let request = RootResultRead::try_new(
                    root_task,
                    RootProfileId::V1,
                    kind,
                    read.wanted,
                    read.consumed,
                    Duration::from_millis(10),
                )
                .map_err(|error| error.to_string())?;
                frontier.sent(read);
                let reply = root_port
                    .read(request, window.retain_alias())
                    .await
                    .map_err(|error| format!("fixed empty-root read failed: {error:?}"))?;
                let step = frontier
                    .accept(reply.response(), None)
                    .map_err(|error| error.to_string())?;
                drop(reply);
                match step {
                    RootRelayStep::EndKnown(_) | RootRelayStep::Acknowledged => {}
                    RootRelayStep::Deferred(_) | RootRelayStep::NotReady => {
                        tokio::task::yield_now().await;
                    }
                    _ => {
                        return Err(
                            "fixed CountOnly root returned an unexpected outcome".to_owned()
                        );
                    }
                }
            }
            let end = frontier
                .end_consumed()
                .ok_or_else(|| "fixed CountOnly root did not consume exact EOS".to_owned())?;
            // next_read=None with an empty root is reached only after the
            // original frontier checked its ACK-only accepted-consumed reply.
            let seal = statuses
                .begin_success_seal_request()
                .map_err(|error| error.to_string())?;
            seal.await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(end)
        };
        tokio::join!(installed.bridge.run(cancellation.clone()), consume_and_seal)
    });
    use novarocks_control_runtime::actor_turn::{ActorTurnError, InputPhase};
    use novarocks_control_runtime_core::{history::HistoryFact, mailbox::InputClass};
    let mut early_normal = None;
    let mut setup = None;
    let mut body_exit = false;
    let mut blocked = None;
    let mut second_subscribe = !controlled_reopen;
    let mut captured_history = Vec::new();
    if controlled_reopen {
        let mut first_open = None;
        let deadline = Instant::now() + wait;
        // Stop Core before handing off its first real Open. No phase or
        // input is assigned: the Host and original transport still progress.
        while Instant::now() < deadline {
            source_cohort::tests::drive(&mut joint);
            process
                .runtime
                .advance_time(process.clock.now(), 64)
                .unwrap();
            if first_open.is_none() {
                process
                    .runtime
                    .poll_one_with_clock(process.clock.as_ref())
                    .unwrap();
                first_open = fixture_pending_open(&process, actor_id, None);
            }
            if let Some(turn) = process.runtime.claim_host_actor() {
                process.host.drive(process.clock.now(), turn);
            }
            if early_normal.is_none()
                && let Poll::Ready(value) = futures::poll!(normal_future.as_mut())
            {
                early_normal = Some(value);
            }
            if first_open.is_some() && peer.single_root_first_subscription_waiting() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let inputs = process
            .runtime
            .input_inventory(actor_id, None, 4096)
            .unwrap();
        let phases = inputs
            .iter()
            .filter(|row| {
                row.source == subscription_source
                    && row.class == InputClass::Lifecycle
                    && row.phase == InputPhase::Filled
            })
            .copied()
            .collect::<Vec<_>>();
        if let (Some(open), [phase]) = (first_open, phases.as_slice()) {
            let authors = process
                .runtime
                .input_header_authors(actor_id, phase.input, 4)
                .unwrap();
            if let [work] = authors.as_slice() {
                setup = Some((open, *phase, *work));
            }
        }
        peer.release_single_root_first_subscription();
        let deadline = Instant::now() + wait;
        while Instant::now() < deadline {
            source_cohort::tests::drive(&mut joint);
            if let Some(turn) = process.runtime.claim_host_actor() {
                process.host.drive(process.clock.now(), turn);
            }
            captured_history.extend(history.drain(4096).entries);
            if let Some((_, _, original)) = setup {
                body_exit = captured_history.iter().any(|e| matches!(e.fact,
                    HistoryFact::BodyExited { scope, work } if (scope, work) == (original.scope, original.work)))
                    && captured_history.iter().any(|e| matches!(e.fact,
                    HistoryFact::CapturesExited { scope, work } if (scope, work) == (original.scope, original.work)));
                if body_exit {
                    // The result was published before these actual reports;
                    // drain its original Host without polling the paused Core.
                    for _ in 0..8 {
                        if let Some(turn) = process.runtime.claim_host_actor() {
                            process.host.drive(process.clock.now(), turn);
                        }
                        tokio::task::yield_now().await;
                    }
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
        // The actual EOF was received at the frozen original clock reading.
        // Let its first retry (100 ms with at most 20% jitter in
        // CoveredRetryBudget) become due while Core still has queued work.
        // This advances only the injected clock, never an input or phase.
        if body_exit {
            clock.advance(Duration::from_millis(120));
        }
        // Never require the old early Exit publication. A fixed Source can
        // hold that original result until the final phase actually retires.
        let deadline = Instant::now() + wait;
        let mut advanced_retry = false;
        while Instant::now() < deadline {
            source_cohort::tests::drive(&mut joint);
            process
                .runtime
                .advance_time(process.clock.now(), 64)
                .unwrap();
            let core_ran = process
                .runtime
                .poll_one_with_clock(process.clock.as_ref())
                .unwrap();
            let mut host_ran = false;
            if let Some(turn) = process.runtime.claim_host_actor() {
                host_ran = true;
                if let Some((first, phase, _)) = setup
                    && let Some(open) = fixture_pending_open(&process, actor_id, Some(first.3))
                {
                    let inputs = process
                        .runtime
                        .input_inventory(actor_id, None, 4096)
                        .unwrap();
                    if let Some(last) = inputs.iter().find(|row| {
                        row.source == subscription_source
                            && row.standing_input_id == phase.standing_input_id
                            && row.input != phase.input
                            && row.phase == InputPhase::Filled
                    }) && process.runtime.pending_timer_input(actor_id).unwrap() == Some(open.0)
                        && process
                            .runtime
                            .effect_ready_for(&turn, open.0, open.1)
                            .unwrap_or(false)
                        && process.runtime.input_retirement_status(actor_id, open.0)
                            == Err(ActorTurnError::EffectsRemain)
                    {
                        blocked = Some((open, last.input));
                    }
                }
                process.host.drive(process.clock.now(), turn);
            }
            captured_history.extend(history.drain(4096).entries);
            if early_normal.is_none()
                && let Poll::Ready(value) = futures::poll!(normal_future.as_mut())
            {
                early_normal = Some(value);
            }
            if peer.subscription_requests().len() >= 2 {
                second_subscribe = true;
                break;
            }
            // Advance only an original indexed future deadline, once, after
            // actual queued Core/Host work ran out. No Timer fact is authored.
            if !core_ran
                && !host_ran
                && !advanced_retry
                && let Some(next) = process.runtime.next_deadline()
                && next > process.clock.now()
            {
                clock.advance(Duration::from_nanos(next.0 - process.clock.now().0));
                advanced_retry = true;
            }
            tokio::task::yield_now().await;
        }
        peer.release_single_root_first_subscription();
        peer.release_single_root_create();
        let _ = allow_consumer.take().unwrap().send(());
    }
    let normal_first = if early_normal.is_some() {
        early_normal
    } else {
        drive_until(
            &process,
            &mut joint,
            normal_future.as_mut(),
            Instant::now() + wait,
        )
        .await
    };
    let normal_within_watchdog = normal_first.is_some();
    let normal = if normal_first.is_some() {
        normal_first
    } else {
        // Keep this exact run/consumer/seal future pinned. Cancellation is
        // submitted to its original Work owner; no seal or run is recreated.
        let _ = requester.request(novarocks_workload_control::CancellationReason::Requested);
        drive_until(
            &process,
            &mut joint,
            normal_future.as_mut(),
            Instant::now() + wait,
        )
        .await
    };
    drop(normal_future);
    let original_terminal = installed.bridge.original_terminal_debug_for_test();
    // Match the production completion supervisor: consume the original
    // running permit only for the terminal this Native owner actually returned.
    // None/timeout is not Completed; an original failure preserves its error.
    let qa_completion = match &normal {
        Some((novarocks_query_application::api::NativeAttemptTerminal::Completed, _)) => {
            Some(tokio::time::timeout(wait, logical.complete_original_running_attempt()).await)
        }
        Some((novarocks_query_application::api::NativeAttemptTerminal::Failed(failure), _)) => {
            Some(
                tokio::time::timeout(
                    wait,
                    logical.fail_original_running_attempt(failure.error().clone()),
                )
                .await,
            )
        }
        _ => None,
    };
    let read_closed = {
        use novarocks_execution_contract::root_lifetime::RootReadConvergence;
        use novarocks_query_application::api::{BoundedRootReadPort, NativeAttemptTerminal};
        let mut read = RootReadConvergence::new(root_task);
        match &normal {
            Some((NativeAttemptTerminal::Completed, Ok(end))) => {
                let end_checked = read.consume_end(*end).is_ok();
                // Completed is the original serialized terminal, not a test
                // TaskStatus assignment or a root ACK masquerading as success.
                read.observe_root_finished();
                end_checked
                    && read
                        .seal_normal()
                        .is_ok_and(|sealed| root_port.seal(sealed).is_ok())
            }
            Some((NativeAttemptTerminal::Failed(_), _)) => {
                root_port.seal(read.seal_terminated()).is_ok()
            }
            _ if cancellation.reason().is_some() => root_port.seal(read.seal_terminated()).is_ok(),
            _ => false,
        }
    };

    // The same borrowed convergence future owns its original frozen remote
    // deadline and caller-position return closure across bounded observations.
    let convergence_deadline = Instant::now() + wait;
    let mut convergence_future = Box::pin(installed.bridge.converge(
        novarocks_query_application::api::NativeActiveConvergenceRequest::new(
            cancellation.clone(),
            convergence_deadline,
        ),
        false,
        move || {
            drop((statuses, protocol_facts));
            Ok(())
        },
    ));
    let closure_first = drive_until(
        &process,
        &mut joint,
        convergence_future.as_mut(),
        convergence_deadline,
    )
    .await;
    let convergence_within_watchdog = closure_first.is_some();
    let closure = if closure_first.is_some() {
        closure_first
    } else {
        let _ = requester.request(novarocks_workload_control::CancellationReason::Requested);
        drive_until(
            &process,
            &mut joint,
            convergence_future.as_mut(),
            Instant::now() + wait,
        )
        .await
    };
    drop(convergence_future);
    // Return the actual Native closure to the original QA registry. Host
    // emptiness, a local timeout or Release acknowledgement is not this fact.
    let qa_convergence = tokio::time::timeout(wait, async {
        use novarocks_query_application::api::{
            NativeActiveConvergenceClosure, NativeAttemptConvergence,
            NativeParticipationConvergence, NativeParticipationConvergenceKind,
        };
        let expected = expected_participations
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if expected.len() != expected_participations.len() {
            return Err("original scheduled participation list contains duplicates".to_owned());
        }
        let facts = match &closure {
            Some(Ok(NativeActiveConvergenceClosure::Evidence(
                NativeAttemptConvergence::Participations(facts),
            ))) => facts.to_vec(),
            Some(Ok(NativeActiveConvergenceClosure::Evidence(
                NativeAttemptConvergence::AllWorkersStoppedAndParticipationsFenced,
            ))) => {
                // This is the original owner's typed whole-attempt evidence,
                // projected exactly as the production Supervisor does.
                expected
                    .iter()
                    .copied()
                    .map(NativeParticipationConvergence::worker_stopped_and_participation_fenced)
                    .collect()
            }
            _ => return Err("no positive Native participation convergence was returned".to_owned()),
        };
        let observed = facts
            .iter()
            .map(|fact| fact.participation())
            .collect::<std::collections::BTreeSet<_>>();
        if observed.len() != facts.len() || observed != expected {
            return Err(
                "Native closure omitted, duplicated or changed a scheduled participation"
                    .to_owned(),
            );
        }
        for fact in facts {
            let result = match fact.kind() {
                NativeParticipationConvergenceKind::NeverEstablished => {
                    logical
                        .observe_participation_never_established(fact.participation())
                        .await
                }
                NativeParticipationConvergenceKind::WorkerStoppedAndParticipationFenced => {
                    logical
                        .observe_worker_stopped_and_participation_fenced(fact.participation())
                        .await
                }
                NativeParticipationConvergenceKind::WorkerProcessReplaced => {
                    logical
                        .observe_worker_process_replaced(fact.participation())
                        .await
                }
            };
            result.map_err(|error| error.to_string())?;
        }
        Ok::<_, String>(())
    })
    .await;
    // A failed observation is never local exit. Ask the existing Host owner
    // to close its entries and separately retain its actual bounded outcome.
    process.host.close_registration();
    let host_exit = drive_until(
        &process,
        &mut joint,
        async {
            while !process.host.is_empty() {
                tokio::task::yield_now().await;
            }
        },
        Instant::now() + wait,
    )
    .await;
    let subscribed = !peer.subscription_requests().is_empty();
    let released = peer.single_root_release_observed();
    drop((
        installed,
        drive,
        route,
        router,
        manifest,
        cohort,
        window,
        root_port,
        results,
        requester,
        cancellation,
        capacity,
    ));
    let qa_exit = logical.finish_until(Instant::now() + wait).await;
    business.release();
    source_cohort::tests::drive(&mut joint);
    let scope_released = tokio::time::timeout(wait, scope.wait_released()).await;
    drop(scope);
    workload.close_admission();
    source_cohort::tests::drive(&mut joint);
    let workload_exit = workload.shutdown();
    let subscriptions_exit = data
        .drain_original_subscriptions_until(Instant::now() + wait)
        .await;
    let apply_exit = data.drain_original_apply_until(Instant::now() + wait).await;
    let outgoing_exit = data
        .drain_native_outgoing_until(Instant::now() + wait)
        .await;
    let peer_exit = peer.settle().await;
    let clean = process.host.is_empty()
        && process.runtime.is_empty()
        && process.ledger.is_empty()
        && process.members.is_empty();

    // Assertions follow actual host/Source/QA/Workload/channel/peer cleanup.
    assert!(
        clean,
        "original host cleanup failed: setup={setup:?}, body_exit={body_exit}, blocked={blocked:?}, second_subscribe={second_subscribe}, normal={normal:?}, original_terminal={original_terminal:?}, closure={closure:?}, host_exit={host_exit:?}, qa={qa_exit:?}, scope={scope_released:?}, workload_ok={}, subscriptions={subscriptions_exit:?}, apply={apply_exit:?}, outgoing={outgoing_exit:?}, peer={peer_exit}",
        workload_exit.is_ok()
    );
    if controlled_reopen {
        assert!(history.is_complete());
        assert!(setup.is_some() && body_exit);
        assert!(
            second_subscribe,
            "next original Open was stranded: {blocked:?}"
        );
    }
    assert!(
        qa_exit.is_ok() && scope_released.is_ok() && workload_exit.is_ok(),
        "saved live fixture outcomes: qa={qa_exit:?}, scope={scope_released:?}, workload_ok={},, normal={normal:?}, original_terminal={original_terminal:?}, convergence={closure:?}, normal_first={normal_within_watchdog}, convergence_first={convergence_within_watchdog}, host_exit={host_exit:?}, subscribed={subscribed}, released={released}, read_closed={read_closed}, subscriptions={subscriptions_exit:?}, apply={apply_exit:?}, outgoing={outgoing_exit:?}, peer={peer_exit}, qa_completion={qa_completion:?}, qa_convergence={qa_convergence:?}, setup={setup:?}, body_exit={body_exit}, blocked={blocked:?}, second_subscribe={second_subscribe}",
        workload_exit.is_ok()
    );
    assert!(subscriptions_exit.is_ok() && apply_exit.is_ok() && outgoing_exit.is_ok() && peer_exit);
    assert!(host_exit.is_some() && subscribed && released && read_closed);
    assert!(normal_within_watchdog && convergence_within_watchdog);
    assert!(matches!(
        qa_completion,
        Some(Ok(Ok(
            novarocks_query_application::coordination::LogicalConclusion::Succeeded,
        )))
    ));
    assert!(matches!(qa_convergence, Ok(Ok(()))));
    assert!(matches!(
        normal,
        Some((
            novarocks_query_application::api::NativeAttemptTerminal::Completed,
            Ok(_)
        ))
    ));
    assert!(matches!(
        closure,
        Some(Ok(
            novarocks_query_application::api::NativeActiveConvergenceClosure::Evidence(_)
        ))
    ));
}

fn fixture_pending_open(
    process: &FrontendAttemptRuntime,
    actor: novarocks_control_runtime_core::identity::ActorId,
    after: Option<std::num::NonZeroU64>,
) -> Option<(
    novarocks_control_runtime_core::identity::InputId,
    novarocks_control_runtime_core::identity::ActionId,
    novarocks_execution_contract::ParticipationRef,
    std::num::NonZeroU64,
)> {
    use crate::control_runtime::actor::FrontendEffect;
    use novarocks_attempt_protocol::effect::EffectBody;
    use novarocks_control_runtime_core::effect::EffectState;
    for (_, input) in process.runtime.committed_batches(actor, None, 256).unwrap() {
        for (descriptor, state) in process
            .runtime
            .effect_inventory(actor, input, None, 32)
            .unwrap()
        {
            if state != EffectState::Pending {
                continue;
            }
            let Some(FrontendEffect::Attempt(effect)) = process
                .runtime
                .pending_effect_metadata(actor, input, descriptor.id)
                .unwrap()
            else {
                continue;
            };
            if let EffectBody::OpenSubscription(request) = &effect.protocol.body
                && after.is_none_or(|old| request.generation > old)
            {
                return Some((
                    input,
                    descriptor.id,
                    request.participation,
                    request.generation,
                ));
            }
        }
    }
    None
}
